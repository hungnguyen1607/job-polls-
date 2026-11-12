#!/usr/bin/env python3
"""Coverage scoreboard: are our own pollers at least as good as Simplify?

Read-only on engineering-ats/.internships.sqlite. For open postings first seen in the
last --days days:

  simplify recall  % of Simplify's postings that one of our own pollers also found
                   (same normalized URL, or same company with >=60% of title words in
                   common, since Simplify rewrites titles)
  lead time        median minutes our first sighting came before Simplify's
                   (positive = we were earlier)
  our-only         postings our pollers found that Simplify doesn't list
  per source       open postings per source, so a source that silently drops shows up

"Our own pollers" excludes every community list (LIST_SOURCES, simplifyjobs), so the
number measures the ATS/careers polling, not other people's lists.

Appends one row to engineering-ats/coverage.csv and prints a summary. With --alert,
exits 1 when recall fell more than --max-drop points below the 7-day median.

Usage:
    python scripts/coverage_report.py
    python scripts/coverage_report.py --days 14 --alert
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import statistics
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internship_core import (  # noqa: E402
    DB_PATH, LIST_SOURCES, OUT_DIR, company_key, norm_url,
)

SIMPLIFY = {"simplify", "simplifyjobs"}
AGGREGATORS = set(LIST_SOURCES) | SIMPLIFY
COVERAGE_CSV = OUT_DIR / "coverage.csv"
FIELDS = ["at", "days", "simplify_total", "simplify_matched", "recall_pct",
          "lead_median_min", "our_total", "our_only", "simplify_company_polled_pct", "per_source"]


GENERIC_WORDS = {
    "intern", "interns", "internship", "internships", "co", "op", "coop", "summer", "fall",
    "winter", "spring", "2025", "2026", "2027", "2028", "student", "the", "and", "of", "a", "in",
    "for", "to", "program", "early", "career", "talent", "undergraduate", "graduate",
    "university", "new", "grad", "i", "ii",
}
SIMILARITY = 0.6  # Jaccard on title words; 0.5-1.0 moved recall only 31% -> 25% on 2026-11-12 data


def title_words(title: str | None) -> frozenset:
    return frozenset(w for w in re.findall(r"[a-z0-9+#]+", (title or "").lower()) if w not in GENERIC_WORDS)


def similar(a: frozenset, b: frozenset) -> bool:
    return bool(a and b) and len(a & b) / len(a | b) >= SIMILARITY


def _when(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def measure(days: int) -> dict:
    db = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
    cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
    rows = db.execute(
        "SELECT source, company, title, location, url, first_seen FROM postings "
        "WHERE open=1 AND first_seen >= ?", (cutoff,)).fetchall()
    db.close()

    # Simplify rewrites titles ("Internship, Data Engineer, Energy (Winter 2027)" on Tesla's
    # board is "Data Engineer Intern - Energy" there) and many of its links point at
    # simplify.jobs, so match on URL first, else on title words within the same company.
    ours_by_url: dict[str, datetime | None] = {}
    ours_by_company: dict[str, list[tuple[frozenset, datetime | None]]] = {}
    simplify: list[tuple[str, str, frozenset, datetime | None]] = []
    per_source: dict[str, int] = {}
    for source, company, title, location, url, first_seen in rows:
        per_source[source] = per_source.get(source, 0) + 1
        co = company_key(company or "")
        words = title_words(title)
        u = norm_url(url or "")
        when = _when(first_seen)
        if source in SIMPLIFY:
            simplify.append((u, co, words, when))
        elif source not in AGGREGATORS:
            if u and (u not in ours_by_url or (when and (ours_by_url[u] is None or when < ours_by_url[u]))):
                ours_by_url[u] = when
            ours_by_company.setdefault(co, []).append((words, when))

    matched = 0
    leads: list[float] = []
    simplify_by_company: dict[str, list[frozenset]] = {}
    for u, co, words, when in simplify:
        simplify_by_company.setdefault(co, []).append(words)
        if u in ours_by_url:
            ours: datetime | None | str = ours_by_url[u]
        else:
            hits = [w for t, w in ours_by_company.get(co, []) if similar(words, t)]
            ours = min((h for h in hits if h), default=None) if hits else "missing"
        if ours == "missing":
            continue
        matched += 1
        if ours and when:
            leads.append((when - ours).total_seconds() / 60)

    our_total = sum(len(v) for v in ours_by_company.values())
    our_only = sum(1 for co, items in ours_by_company.items() for t, _ in items
                   if not any(similar(t, s) for s in simplify_by_company.get(co, [])))
    polled = sum(1 for _, co, _, _ in simplify if co in ours_by_company)
    return {
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "days": days,
        "simplify_total": len(simplify),
        "simplify_matched": matched,
        "recall_pct": round(100 * matched / len(simplify), 1) if simplify else 0.0,
        "lead_median_min": round(statistics.median(leads), 1) if leads else "",
        "our_total": our_total,
        "our_only": our_only,
        "simplify_company_polled_pct": round(100 * polled / len(simplify), 1) if simplify else 0.0,
        "per_source": ";".join(f"{s}={n}" for s, n in sorted(per_source.items(), key=lambda x: -x[1])),
    }


def history_recalls() -> list[float]:
    if not COVERAGE_CSV.exists():
        return []
    week_ago = datetime.now(UTC) - timedelta(days=7)
    out = []
    with COVERAGE_CSV.open() as fh:
        for row in csv.DictReader(fh):
            at = _when(row.get("at"))
            if at and at >= week_ago and row.get("recall_pct"):
                out.append(float(row["recall_pct"]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--alert", action="store_true", help="exit 1 on a recall drop")
    ap.add_argument("--max-drop", type=float, default=5.0, help="allowed recall drop (points)")
    ap.add_argument("--no-write", action="store_true", help="print only, don't append coverage.csv")
    args = ap.parse_args()

    history = history_recalls()
    result = measure(args.days)
    if not args.no_write:
        new = not COVERAGE_CSV.exists()
        with COVERAGE_CSV.open("a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            if new:
                w.writeheader()
            w.writerow(result)

    print(f"last {args.days} days, open postings")
    print(f"  simplify recall : {result['recall_pct']}%  "
          f"({result['simplify_matched']}/{result['simplify_total']} Simplify postings also found by our pollers)")
    print(f"  lead time       : {result['lead_median_min']} min median (positive = we saw it first)")
    print(f"  company polled  : {result['simplify_company_polled_pct']}% of Simplify's postings are at companies we poll at all")
    print(f"  our-only        : {result['our_only']} of {result['our_total']} postings Simplify doesn't list")
    print(f"  top sources     : {', '.join(result['per_source'].split(';')[:10])}")

    if args.alert and history:
        baseline = statistics.median(history)
        if result["recall_pct"] < baseline - args.max_drop:
            print(f"ALERT: recall {result['recall_pct']}% is below the 7-day median {baseline}% "
                  f"by more than {args.max_drop} points")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
