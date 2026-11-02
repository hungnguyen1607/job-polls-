#!/usr/bin/env python3
"""24/7 outreach poller: keep the company list correct from intern hiring history.

Does not scrape the 5k career pages. Rebuilds as soon as internships or the 5k
company list change (and at least once a minute). Every hour it also refreshes
GitHub intern-list history (live lists + season snapshots). Canada: Winter 2027
first; dedicated Summer 2027 SWE intern postings are dropped. US: every software
intern employer seen in the US unless they say no-sponsor/citizen/clearance.

    python scripts/watch_outreach.py            # run forever
    python scripts/watch_outreach.py --once     # one rebuild, then exit
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

# health_check.group_sources reads these.
SOURCES = ["outreach"]
INTERVAL_MINUTES = 1          # never go longer than this between rebuilds
POLL_SECONDS = 5              # how often we look for new internships
MIN_GAP = 15                  # seconds; don't stack rebuilds
HISTORY_EVERY = 60 * 60       # GitHub intern-list refresh
HISTORY_TIMEOUT = 180


def _counts() -> tuple[int, int, int]:
    winter = list(csv.DictReader(core.WINTER_CSV.open())) if core.WINTER_CSV.exists() else []
    us = list(csv.DictReader(core.US_SPONSOR_CSV.open())) if core.US_SPONSOR_CSV.exists() else []
    return len(winter), sum(1 for r in winter if r.get("winter_2027") == "yes"), len(us)


def _inputs_mtime() -> float:
    """Newest mtime of internships / 5k list / GitHub snapshot the join reads."""
    paths = [
        core.CA_CSV_PATH, core.ALL_CSV_PATH, core.US_CSV_PATH, core.CA_COMPANIES_CSV,
        core.DISCOVERED_CSV, core.RECENT_CSV, core.ALREADY_2027_CSV, core.DB_PATH,
        Path(str(core.DB_PATH) + "-wal"),
    ]
    latest = 0.0
    for path in paths:
        try:
            latest = max(latest, path.stat().st_mtime)
        except OSError:
            continue
    return latest


def _refresh_github_history() -> None:
    script = Path(__file__).resolve().parent / "find_recent_intern_companies.py"
    subprocess.run(
        [sys.executable, str(script)], cwd=core.REPO, timeout=HISTORY_TIMEOUT, check=False)
    try:
        core.refresh_swe_intern_history(force=True, snapshots=True)
    except Exception as exc:  # noqa: BLE001 — CSV rebuild still runs
        core.log_line(f"{'intern_hist':16} {exc}")


def _pass(*, history: bool) -> None:
    started = time.monotonic()
    if history:
        try:
            _refresh_github_history()
        except subprocess.TimeoutExpired:
            core.log_line(f"{'outreach':16} github history timed out; rebuilding from last snapshot")
            core.write_winter_companies()
        except Exception as exc:  # noqa: BLE001 — live intern join still runs
            core.log_line(f"{'outreach':16} github history: {exc}; rebuilding from last snapshot")
            core.write_winter_companies()
    else:
        core.write_winter_companies()
    n, winter, us_n = _counts()
    elapsed = (time.monotonic() - started) / 60
    employers = len(core._ca_software_employers())
    core.log_line(
        f"{'outreach':16} {employers}/{employers} checked, {n} Canada, "
        f"{winter} Winter 2027, {us_n} US intern employers, 0 errors | {elapsed:.1f} min")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="one rebuild, then exit")
    parser.add_argument("--interval", type=float, default=INTERVAL_MINUTES,
                        help=f"max minutes between rebuilds (default {INTERVAL_MINUTES})")
    args = parser.parse_args()
    core.connect().close()
    core.log_line(f"[outreach] started: 1 sources every {args.interval:g} min"
                  + (" (one pass)" if args.once else "")
                  + ", rebuilds as soon as internships change")
    last_history = 0.0
    last_rebuild = 0.0
    last_seen_mtime = -1.0
    try:
        while True:
            now = time.time()
            history = (now - last_history) >= HISTORY_EVERY
            changed = _inputs_mtime() > last_seen_mtime
            stale = (now - last_rebuild) >= args.interval * 60
            ready = (now - last_rebuild) >= MIN_GAP
            if args.once or history or (ready and (changed or stale)):
                try:
                    _pass(history=history)
                    if history:
                        last_history = time.time()
                except Exception as exc:  # noqa: BLE001 — next pass still runs
                    core.log_line(f"{'outreach':16} FAILED {type(exc).__name__}: {exc}")
                last_rebuild = time.time()
                last_seen_mtime = _inputs_mtime()
                if args.once:
                    break
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        pass
    core.log_line("[outreach] stopped")


if __name__ == "__main__":
    main()
