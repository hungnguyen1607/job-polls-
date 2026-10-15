#!/usr/bin/env python3
"""Big ATSes: every company on the major applicant tracking systems, every 10 min.

Writes into engineering-ats/US_internships.csv and canada_internships.csv (shared
with the other watch_*.py scripts). See scripts/internship_core.py for the filter and output format.

Usage:
    python scripts/watch_big_ats.py              # run forever
    python scripts/watch_big_ats.py --once       # one pass, then exit
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import run_group  # noqa: E402

SOURCES = [
    "greenhouse", "workday", "ashby", "lever", "smartrecruiters", "icims", "oracle",
    "successfactors", "rippling", "bamboohr", "jazzhr", "gem",
    # Backup sources: community internship lists catch unlisted postings (Jane Street,
    # SpaceX) and companies missing from ats-companies/. Duplicates of ATS rows are dropped.
    "simplify", "speedyapply", "jobright", "zshah101", "earlycareerradar", "applyguy", "ycombinator", "interndock", "negarprh", "hansonchen", "vanshb03", "careers", "remoterocketship",
    # Canadian job boards: Workopolis carries Indeed Canada's postings; Eluta crawls employers' own sites.
    "workopolis", "eluta",
]

if __name__ == "__main__":
    run_group("big_ats", SOURCES, interval_minutes=10, description=__doc__)
