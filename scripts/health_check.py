#!/usr/bin/env python3
"""Watchdog for the internship pollers, run every 5 min by a systemd timer.

1. Every watcher service is running (restarts any that stopped).
2. Every source finished a pass recently (by its own schedule), with no crash,
   out-of-memory stop, or pass where every company failed.
3. canada_internships.csv was written in the last 15 min.
4. Every new Canadian software internship in canada_internships.csv is pushed to your phone.

Alerts and new internships go to ntfy.sh/<topic> (topic in engineering-ats/.ntfy_topic;
subscribe to it in the ntfy app). A problem is re-sent at most once an hour while it lasts,
and a "recovered" note is sent when it clears. Every run is logged to engineering-ats/health.log.

    .venv/bin/python scripts/health_check.py            # one check
    .venv/bin/python scripts/health_check.py --test     # send a test notification
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import secrets
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "engineering-ats"
WATCH_LOG = OUT / "watch.log"
CSV_PATH = OUT / "canada_internships.csv"
STATE = OUT / ".health_state.json"
TOPIC_FILE = OUT / ".ntfy_topic"
HEALTH_LOG = OUT / "health.log"
GROUPS = ("big_ats", "bigtech", "small_ats", "canada")
# Sources that run less often than their group (internship_core.MIN_GAP), in minutes.
MIN_GAP = {"interndock": 30, "cgi": 30, "linkedin": 15, "workopolis": 30, "eluta": 60, "eightfold": 180, "wellfound": 30}
CSV_STALE_MIN = 15
REALERT_SECONDS = 3600
LOG_LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) (\S+)\s+(.*)$")
CA_WORDS = re.compile(r"canada|ontario|qu[eé]bec|british columbia|alberta|manitoba|saskatchewan|"
                      r"nova scotia|new brunswick|newfoundland|prince edward|toronto|montr[eé]al|"
                      r"vancouver|ottawa|waterloo|kitchener|calgary|edmonton|winnipeg|halifax", re.I)
CA_CODES = re.compile(r"(,|\s)\s*(ON|QC|BC|AB|MB|SK|NS|NB|NL|PE)\b")
NOT_CA = re.compile(r"\b(IA|Iowa|WA|Washington|NY|New York|Ohio|OH|US|USA|United States)\b")


def log(msg: str) -> None:
    with HEALTH_LOG.open("a") as fh:
        fh.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}\n")


def topic() -> str:
    if not TOPIC_FILE.exists():
        TOPIC_FILE.write_text(f"internships-{secrets.token_hex(8)}\n")
    return TOPIC_FILE.read_text().strip()


def push(title: str, body: str, *, click: str = "", priority: str = "default", tags: str = "") -> None:
    headers = {"Title": title.encode("utf-8"), "Priority": priority}
    if click:
        headers["Click"] = click
    if tags:
        headers["Tags"] = tags
    try:
        httpx.post(f"https://ntfy.sh/{topic()}", content=body.encode("utf-8"), headers=headers, timeout=15)
    except httpx.HTTPError as exc:
        log(f"push failed: {exc}")


def load_state() -> dict:
    try:
        return json.loads(STATE.read_text())
    except (OSError, ValueError):
        return {}


def save_state(state: dict) -> None:
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE)


def group_sources() -> dict[str, tuple[list[str], int]]:
    """{group: (sources, interval minutes)} read from scripts/watch_<group>.py."""
    out = {}
    for g in GROUPS:
        tree = ast.parse((REPO / "scripts" / f"watch_{g}.py").read_text())
        sources, interval = [], 10
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "SOURCES" for t in node.targets):
                sources = ast.literal_eval(node.value)
            if isinstance(node, ast.keyword) and node.arg == "interval_minutes":
                interval = ast.literal_eval(node.value)
        out[g] = (sources, interval)
    return out


def tail_lines(path: Path, max_bytes: int = 3_000_000) -> list[str]:
    with path.open("rb") as fh:
        fh.seek(0, 2)
        fh.seek(max(0, fh.tell() - max_bytes))
        return fh.read().decode("utf-8", "replace").splitlines()


def is_canadian(location: str) -> bool:
    if re.search(r"canada", location, re.I):
        return True
    # "Vancouver, WA", "Waterloo, IA", "London, ON" vs "London, KY": a US marker vetoes the rest.
    return bool(CA_WORDS.search(location) or CA_CODES.search(location)) and not NOT_CA.search(location)


def check(state: dict) -> list[str]:
    problems: list[str] = []
    now = datetime.now()

    # 1. Services.
    for g in GROUPS:
        unit = f"internship-watch@{g}"
        active = subprocess.run(["systemctl", "--user", "is-active", unit], capture_output=True, text=True).stdout.strip()
        if active != "active":
            subprocess.run(["systemctl", "--user", "restart", unit])
            problems.append(f"{unit} was {active or 'unknown'}; restarted it")

    # 2. Sources: last completed pass, crashes, every-company-failed passes.
    lines = tail_lines(WATCH_LOG)
    last: dict[str, datetime] = {}
    since = now - timedelta(minutes=10)
    for line in lines:
        m = LOG_LINE.match(line)
        if not m:
            continue
        when, source, rest = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"), m.group(2), m.group(3)
        if "checked," in rest:
            last[source] = when
        if when >= since:
            if "FAILED" in rest or "STOPPED" in rest:
                problems.append(f"{source}: {rest[:160]}")
            c = re.search(r"(\d+)/(\d+) checked.*?(\d+) errors", rest)
            if c and int(c.group(2)) >= 5 and int(c.group(1)) == 0:
                problems.append(f"{source}: every company failed this pass ({rest[:120]})")
    started = {}  # a group that just restarted hasn't had time to finish a pass
    for g in GROUPS:
        out_file = OUT / f"{g}.out"
        for line in reversed(tail_lines(out_file, 20_000)) if out_file.exists() else []:
            if "] started:" in line:
                started[g] = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
                break
    for g, (sources, interval) in group_sources().items():
        for s in sources:
            allowed = timedelta(minutes=3 * max(interval, MIN_GAP.get(s, 0)) + 10)
            ref = max(last.get(s, datetime.min), started.get(g, datetime.min))
            if now - ref > allowed:
                seen = last[s].strftime("%H:%M") if s in last else "never"
                problems.append(f"{s} ({g}) hasn't finished a pass since {seen}")

    # 3. The CSV.
    if not CSV_PATH.exists():
        problems.append(f"{CSV_PATH.name} hasn't been written yet")
        return problems
    age = (time.time() - CSV_PATH.stat().st_mtime) / 60
    if age > CSV_STALE_MIN:
        problems.append(f"canada_internships.csv not updated for {age:.0f} min")
    return problems


def notify_new_internships(state: dict) -> int:
    if not CSV_PATH.exists():
        return 0
    with CSV_PATH.open(newline="", encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if is_canadian(r.get("location") or "")]
    seen = set(state.get("notified", []))
    if not state.get("notified_baseline"):
        # First run: everything already in the file counts as seen, so no flood of old postings.
        state["notified"] = sorted({r["link"] for r in rows})
        state["notified_baseline"] = True
        return 0
    new = [r for r in rows if r["link"] not in seen]
    for r in new[:10]:
        push(f"🇨🇦 {r['company']}: {r['title']}"[:250],
             f"{r['location']}\nPosted {r['posted']} · via {r['source']}\n{r['link']}",
             click=r["link"], priority="high", tags="briefcase")
    if len(new) > 10:
        push(f"{len(new) - 10} more new Canadian internships",
             "\n".join(f"{r['company']}: {r['title']}" for r in new[10:40]), tags="briefcase")
    # Never push the same link twice, even if it drops out of the file and comes back.
    state["notified"] = sorted(seen | {r["link"] for r in new})[-50000:]
    return len(new)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true")
    args = ap.parse_args()
    if args.test:
        push("Internship watchdog connected", "You'll get pollers' problems and new Canadian internships here.",
             tags="white_check_mark")
        print(f"sent a test to ntfy.sh/{topic()}")
        return
    state = load_state()
    problems = check(state)
    open_before = state.get("problems", {})
    now = time.time()
    still = {}
    for p in problems:
        key = re.sub(r"\d", "#", p)  # same issue with a different count/time is the same alert
        first = open_before.get(key, {}).get("sent", 0)
        if now - first >= REALERT_SECONDS:
            push("⚠️ Poller problem", p, priority="high", tags="warning")
            first = now
        still[key] = {"sent": first, "text": p}
    for key, info in open_before.items():
        if key not in still:
            push("✅ Recovered", info["text"], tags="white_check_mark")
    state["problems"] = still
    sent = notify_new_internships(state)
    save_state(state)
    log(f"{len(problems)} problems, {sent} new Canadian internships pushed"
        + (": " + " | ".join(problems) if problems else ""))


if __name__ == "__main__":
    main()
