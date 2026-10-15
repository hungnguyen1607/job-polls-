#!/usr/bin/env python3
"""Big tech careers sites, every 6 min.

Writes into engineering-ats/US_internships.csv and canada_internships.csv (shared
with the other watch_*.py scripts). See scripts/internship_core.py for the filter and output format.

Usage:
    python scripts/watch_bigtech.py              # run forever
    python scripts/watch_bigtech.py --once       # one pass, then exit
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import run_group  # noqa: E402

SOURCES = [
    "cgi", "shopify", "janestreet", "linkedin", "bytedance", "google", "amazon", "apple", "meta", "tiktok", "uber", "tesla",
]

if __name__ == "__main__":
    run_group("bigtech", SOURCES, interval_minutes=6, description=__doc__)
