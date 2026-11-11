#!/usr/bin/env python3
"""High-yield internship lists, every 3 min.

These are cheap GitHub/JSON lists that surface a lot of postings per pass. They used to
share big_ats's child-process slots with slow browser/ATS passes, so a 10-30 min careers,
ADP, or Paycom pass could delay a new posting by a full cycle. Running them in their own
watcher gives them dedicated slots and a 3-minute cadence.

Writes into engineering-ats/US_internships.csv and canada_internships.csv (shared with the
other watch_*.py scripts). See scripts/internship_core.py for the filter and output format.

Usage:
    python scripts/watch_fast.py              # run forever
    python scripts/watch_fast.py --once       # one pass, then exit
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import run_group  # noqa: E402

SOURCES = [
    "simplify", "jobright", "zshah101", "earlycareerradar", "applyguy", "vanshb03",
    "speedyapply", "negarprh", "hansonchen", "remoterocketship",
]

if __name__ == "__main__":
    run_group("fast", SOURCES, interval_minutes=3, description=__doc__)
