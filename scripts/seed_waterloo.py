#!/usr/bin/env python3
"""Add Waterloo-hiring startups to the Canada poller.

Sources:
  - University of Waterloo Engineering startup list (founders / spin-offs)
  - Velocity incubator company directory
  - A short extra list of well-known Waterloo co-op tech employers

Matched ATS boards go on the 3-minute Canada watcher. Everyone else with a
public website goes on the careers poller. Does not invent intern history —
outreach still only lists companies that actually hired software interns.

    .venv/bin/python scripts/seed_waterloo.py
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

UW_STARTUPS = "https://uwaterloo.ca/engineering/startup-list"
VELOCITY = "https://velocity.uwaterloo.ca/companies/"
WATERLOO_SEED_REFRESH = 7 * 24 * 60 * 60
# Names Waterloo CS/SE kids actually intern at that the two pages miss or spell oddly.
EXTRA = [
    ("A Thinking Ape", "https://www.athinkingape.com/"),
    ("Faire", "https://www.faire.com/careers"),
    ("FreshBooks", "https://freshbooks.bamboohr.com/careers"),
    ("Wealthsimple", "https://jobs.ashbyhq.com/wealthsimple"),
    ("Jobber", "https://jobs.ashbyhq.com/jobber"),
    ("KOHO", "https://jobs.ashbyhq.com/koho"),
    ("Thinkific", "https://job-boards.greenhouse.io/thinkific"),
    ("Wave", "https://www.waveapps.com/careers"),
    ("League", "https://job-boards.greenhouse.io/leagueinc"),
    ("Vidyard", "https://www.vidyard.com/careers"),
    ("Mappedin", "https://www.mappedin.com/careers"),
    ("ApplyBoard", "https://www.applyboard.com/careers"),
    ("TextNow", "https://www.textnow.com/careers"),
    ("D2L", "https://www.d2l.com/careers/"),
    ("Magnet Forensics", "https://www.magnetforensics.com/careers/"),
    ("Arctic Wolf", "https://arcticwolf.com/careers/"),
    ("OpenText", "https://www.opentext.com/about/careers"),
    ("BlackBerry", "https://www.blackberry.com/us/en/careers"),
    ("SAP", "https://jobs.sap.com/"),
    ("PointClickCare", "https://pointclickcare.com/company/careers/"),
    ("Maplesoft", "https://www.maplesoft.com/careers/"),
    ("Clearpath Robotics", "https://clearpathrobotics.com/careers/"),
    ("OTTO Motors", "https://ottomotors.com/careers/"),
    ("eSentire", "https://www.esentire.com/about/careers"),
    ("PlayStation", "https://www.playstation.com/en-ca/corporate/careers/"),
    ("Intuit", "https://www.intuit.com/careers/"),
    ("Veeva", "https://www.veeva.com/careers/"),
    ("Miovision", "https://miovision.com/careers/"),
    ("Auvik", "https://www.auvik.com/careers/"),
    ("Top Hat", "https://tophat.com/careers/"),
    ("Nulogy", "https://nulogy.com/careers/"),
    ("Kik", "https://www.kik.com/"),
    ("North", "https://www.bynorth.com/"),
    ("Drop", "https://www.joindrop.com/careers"),
    ("Flipp", "https://www.flipp.com/careers"),
    ("Wattpad", "https://www.wattpad.com/careers/"),
    ("Ritual", "https://ritual.co/careers"),
    ("Solink", "https://solink.com/careers/"),
    ("Sortable", "https://www.sortable.com/"),
    ("SSIMWAVE", "https://www.ssimwave.com/"),
    ("DarwinAI", "https://darwinai.com/"),
    ("Ethoca", "https://www.ethoca.com/careers"),
    ("Tulip Retail", "https://tulip.com/careers/"),
    ("OpenPhone", "https://www.openphone.com/careers"),
    ("Bridgit", "https://www.gobridgit.com/careers"),
    ("Float", "https://www.floatcard.com/careers"),
    ("Ground News", "https://ground.news/careers"),
    ("FluidAI", "https://fluidai.com/careers"),
    ("Avidbots", "https://www.avidbots.com/careers"),
    ("Nicoya", "https://nicoyalife.com/careers/"),
    ("Tailscale", "https://tailscale.com/careers"),
    ("OpsLevel", "https://www.opslevel.com/careers"),
    ("Citylitics", "https://citylitics.com/careers/"),
    ("RideCo", "https://rideco.com/careers"),
    ("Clearco", "https://clearco.com/careers"),
    ("Crowdriff", "https://crowdriff.com/careers/"),
    ("PagerDuty", "https://www.pagerduty.com/careers/"),
    ("Nymi", "https://www.nymi.com/careers"),
    ("Kindred AI", "https://www.kindred.ai/careers"),
    ("Sandbox", "https://www.sandboxvr.com/careers"),
    ("Teledyne DALSA", "https://www.teledyne.com/careers"),
    ("Christie Digital", "https://www.christiedigital.com/about/careers/"),
    ("Descartes", "https://www.descartes.com/careers"),
    ("Kinaxis", "https://www.kinaxis.com/en/company/careers"),
]
_SKIP_NAME = re.compile(
    r"vodka|landscap|architect|flower|ice cream|construction limited|construction ltd|"
    r"food market|scarves|cosmetics|golf apparel|prizm golf|mechanical inc|biofilter|"
    r"gardening|associates ltd|dean of engineering|coop interview|winery|brewery|"
    r"bladetech hockey",
    re.I,
)
_SKIP_HOST = re.compile(r"(^|\.)(linkedin|facebook|instagram|twitter|x|youtube|uwaterloo)\.com$")
# One-word names that collide with a bigger unrelated ATS tenant.
_SKIP_ATS = {
    "j", "alchemy", "athena", "athos", "aurelius", "avra", "canopy", "cocoon",
    "concord", "doppel", "float", "flow", "glow", "glowinc", "heartwood", "hitch",
    "imbue", "indigo", "inspire", "nook", "origin", "perch", "presto", "sandbox",
    "sesame", "snowball", "stealth", "lumos", "kojo", "monogram", "c3group",
    "sap", "intuit",
}


def _clean_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name or "").strip()
    name = re.sub(r"\s*\((acquired by|now|previously) [^)]*\)", "", name, flags=re.I).strip()
    return name.replace("&amp;", "&")


def _site_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    host = (urlparse(url).netloc or "").lower().removeprefix("www.")
    if not host or _SKIP_HOST.search(host):
        return ""
    return url


def parse_uw(html: str) -> list[tuple[str, str]]:
    marker = '<a href="https://www.athinkingape.com/"'
    pos = html.find(marker)
    end = html.find(">Zumio<")
    if pos < 0 or end < 0:
        return []
    start = html.rfind("<li>", 0, pos)
    chunk = html[start if start >= 0 else pos:end + 80]
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for url, name in re.findall(r'<li>\s*<a href="([^"]+)"[^>]*>([^<]+)</a>', chunk):
        name = _clean_name(name)
        if not name or len(name) < 3 or _SKIP_NAME.search(name):
            continue
        key = core.norm_text(name)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, _site_url(url)))
    for name in re.findall(r"<li>([^<]{2,80})</li>", chunk):
        name = _clean_name(name)
        if not name or len(name) < 3 or _SKIP_NAME.search(name):
            continue
        key = core.norm_text(name)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, ""))
    return out


def parse_velocity(html: str) -> list[tuple[str, str]]:
    names = [_clean_name(n) for n in re.findall(r'fs-cmsfilter-field="company"[^>]*>([^<]+)<', html)]
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name in names:
        if not name or _SKIP_NAME.search(name):
            continue
        key = core.norm_text(name)
        if key in seen:
            continue
        seen.add(key)
        out.append((name, ""))
    return out


def _fetch(url: str) -> str:
    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            r = client.get(url)
            r.raise_for_status()
            return r.text
    except Exception:
        return subprocess.check_output(
            ["curl", "-fsSL", "-k", "--max-time", "60", "-A", "Mozilla/5.0", url],
            text=True,
        )


def fetch_pages() -> tuple[str, str]:
    return _fetch(UW_STARTUPS), _fetch(VELOCITY)


def _ats_by_name() -> dict[str, tuple[str, str, dict[str, str]]]:
    """norm_text(name) → (ats, board_key, row)."""
    skip = set(core.SINGLE_SOURCES) | set(core.LIST_SOURCES) | set(core.MIN_GAP) | {"canada", "careers"}
    by_name: dict[str, tuple[str, str, dict[str, str]]] = {}
    for src in sorted(s for s in core.CONFIGS if s not in skip and core.CONFIGS[s].get("csv")
                      and (core.REPO / core.CONFIGS[s]["csv"]).exists()):
        for r in core.company_list(src, None):
            by_name.setdefault(core.norm_text(r.get("name") or ""), (src, core._board_key(r), r))
    by_name.pop("", None)
    return by_name


def seed_waterloo_employers(db, companies: list[tuple[str, str]]) -> dict[str, int]:
    """Insert Waterloo employers into discovered + careers_cache. Returns counts."""
    core._discover_tables(db)
    core._careers_cache(db)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    ats_names = _ats_by_name()
    stats = {"total": 0, "listed": 0, "careers": 0, "queued": 0, "already": 0, "skip": 0}
    with db:
        db.execute("DELETE FROM discovered WHERE source='waterloo' AND length(name_key) < 3")
        for name, url in companies:
            key = core.norm_text(name)
            if not key or len(key) < 3:
                stats["skip"] += 1
                if key:
                    db.execute("DELETE FROM discovered WHERE name_key=? AND source='waterloo'", (key,))
                continue
            stats["total"] += 1
            row = db.execute("SELECT status, ats, slug, url FROM discovered WHERE name_key=?",
                             (key,)).fetchone()
            listed_ok = row and row[0] in ("listed", "careers") and key not in _SKIP_ATS
            if listed_ok:
                stats["already"] += 1
                continue
            if key in ats_names and key not in _SKIP_ATS:
                src, board_key, rec = ats_names[key]
                page = rec.get("url") or url
                if row:
                    db.execute(
                        """UPDATE discovered SET last_seen=?, status='listed', ats=?, slug=?,
                           url=COALESCE(NULLIF(url,''), ?) WHERE name_key=?""",
                        (now, src, board_key, page, key))
                else:
                    db.execute(
                        "INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (key, name, "", "waterloo", now, now, "listed", src, board_key, page, now))
                stats["listed"] += 1
                continue
            if url:
                if row:
                    db.execute(
                        """UPDATE discovered SET last_seen=?, status='careers', ats=NULL, slug=NULL, url=?
                           WHERE name_key=?""",
                        (now, url, key))
                else:
                    db.execute(
                        "INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (key, name, "", "waterloo", now, now, "careers", None, None, url, now))
                db.execute("INSERT OR REPLACE INTO careers_cache VALUES (?,?,?,?,?)",
                           (f"discover:{key}", url, None, None, now))
                stats["careers"] += 1
            elif row:
                db.execute(
                    """UPDATE discovered SET last_seen=?, status='new', ats=NULL, slug=NULL, url=NULL
                       WHERE name_key=?""",
                    (now, key))
                stats["queued"] += 1
            else:
                db.execute(
                    "INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (key, name, "", "waterloo", now, now, "new", None, None, None, now))
                stats["queued"] += 1
        if stats["total"]:
            db.execute("INSERT OR REPLACE INTO discover_state VALUES ('waterloo_seed_at', ?)",
                       (str(datetime.now(UTC).timestamp()),))
    return stats


def collect_companies(uw_html: str, velocity_html: str) -> list[tuple[str, str]]:
    by_key: dict[str, tuple[str, str]] = {}
    for name, url in parse_uw(uw_html) + parse_velocity(velocity_html) + EXTRA:
        key = core.norm_text(name)
        if not key:
            continue
        prev = by_key.get(key)
        if prev is None or (url and not prev[1]):
            by_key[key] = (name, url)
    return list(by_key.values())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true",
                        help="use /tmp/uw-startups.html and /tmp/velocity.html if present")
    args = parser.parse_args()
    uw_path, vel_path = Path("/tmp/uw-startups.html"), Path("/tmp/velocity.html")
    if args.offline and uw_path.exists() and vel_path.exists():
        uw_html, vel_html = uw_path.read_text(errors="replace"), vel_path.read_text(errors="replace")
    else:
        uw_html, vel_html = fetch_pages()
    companies = collect_companies(uw_html, vel_html)
    db = core.connect()
    stats = seed_waterloo_employers(db, companies)
    db.close()
    n = core.write_ca_companies()
    print(f"{stats['total']} Waterloo employers "
          f"({stats['listed']} ATS boards, {stats['careers']} careers pages, "
          f"{stats['queued']} queued to find a board, {stats['already']} already polled)")
    print(f"canada_software / discovered lists refreshed ({n} Canadian software employers)")


if __name__ == "__main__":
    main()
