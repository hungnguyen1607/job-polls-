#!/usr/bin/env python3
"""Smaller ATSes (and rate-limited Paycom / Workable / ADP), every 30 min.

Writes into engineering-ats/US_internships.csv and canada_internships.csv (shared
with the other watch_*.py scripts). See scripts/internship_core.py for the filter and output format.

Usage:
    python scripts/watch_small_ats.py              # run forever
    python scripts/watch_small_ats.py --once       # one pass, then exit
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import run_group  # noqa: E402

SOURCES = [
    "recruitee", "pinpoint", "breezy", "cornerstone", "dayforce", "ukg", "phenom",
    "eightfold", "jobvite", "avature", "taleo", "paylocity", "pageup", "paycom", "workable", "adp",
    "careers_search",  # web-searches dead boards the careers-page guesses couldn't place
    "discover",
    "jobbankca",  # Job Bank Canada: few internships, but many small Canadian software employers
]

if __name__ == "__main__":
    run_group("small_ats", SOURCES, interval_minutes=30, description=__doc__)
