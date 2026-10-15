#!/usr/bin/env python3
"""Companies hiring software people in Canada, every 3 min (or back to back when a pass runs longer).

Polls only the boards of companies in canada_software_companies.csv and discovered.csv
(see internship_core.canada_targets), so a Canadian internship shows up within minutes
instead of when its company's turn comes in the full ATS lists (25-60 min).

Writes into engineering-ats/US_internships.csv and canada_internships.csv (shared
with the other watch_*.py scripts). See scripts/internship_core.py for the filter and output format.

Usage:
    python scripts/watch_canada.py              # run forever
    python scripts/watch_canada.py --once       # one pass, then exit
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import run_group  # noqa: E402

SOURCES = [
    "canada",
    "wellfound",  # startups in Canadian cities: internships, and new companies for discovery (every 30 min)
]

if __name__ == "__main__":
    run_group("canada", SOURCES, interval_minutes=3, description=__doc__)
