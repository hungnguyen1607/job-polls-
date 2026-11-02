#!/usr/bin/env python3
"""Find Canadian companies with a software intern/co-op hire in Winter 2027 (ranked first) or
the previous terms -- EXCLUDING anyone with Summer 2027 evidence (open or closed).

Merges Simplify/vanshb03 GitHub lists (live + end-of-season snapshots) with the live pollers'
`postings` table, so currently-open Winter 2027 co-ops show up even when the crowd lists
haven't picked them up yet.

    .venv/bin/python scripts/find_recent_intern_companies.py
        -> engineering-ats/recent_intern_companies.csv
        -> engineering-ats/winter_2027_companies.csv
"""
from __future__ import annotations

import asyncio
import csv
import sys
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
import internship_core as core  # noqa: E402

OUT_CSV = core.RECENT_CSV
ALREADY_2027_CSV = core.ALREADY_2027_CSV

# The repo's numeric ID is stable across renames (api.github.com/repos/<owner>/<name> 301s to
# api.github.com/repositories/<id> once renamed) -- that's how commit history survives the rename.
REPO_IDS = {"simplify2026": 266417772, "vanshb03_2026": 794545597}
REPO_NAMES = {"simplify2026": "SimplifyJobs/Summer2026-Internships",
              "vanshb03_2026": "vanshb03/Summer2026-Internships"}
# End of each past season -- late enough to have caught that season's postings, before the next
# season's churn starts overwriting them in the live file.
SNAPSHOT_DATES = ["2025-08-31", "2025-09-30", "2026-01-31", "2026-04-30"]


async def nearest_commit_sha(client: httpx.AsyncClient, repo_id: int, until_date: str) -> str | None:
    try:
        r = await client.get(f"https://api.github.com/repositories/{repo_id}/commits",
                              params={"per_page": 1, "until": f"{until_date}T23:59:59Z"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data[0]["sha"] if data else None
    except Exception as exc:  # noqa: BLE001 — one missed snapshot shouldn't stop the others
        print(f"commit lookup failed (repo {repo_id} @ {until_date}): {exc}", file=sys.stderr)
        return None


async def fetch_items(client: httpx.AsyncClient) -> list[dict]:
    items: list[dict] = []
    seen_ids: set = set()

    def add_all(new_items: list[dict]) -> None:
        for x in new_items:
            item_id = x.get("id") or (x.get("company_name"), x.get("title"), x.get("url"))
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            items.append(x)

    for source, url in core.INTERN_HISTORY_LISTS.items():  # current live list
        try:
            r = await client.get(url, timeout=120)
            r.raise_for_status()
            add_all(r.json())
        except Exception as exc:  # noqa: BLE001 — one list failing shouldn't stop the other
            print(f"{source}: {exc}", file=sys.stderr)

    for source, repo_id in REPO_IDS.items():  # + past-season snapshots the live list already lost
        for date in SNAPSHOT_DATES:
            sha = await nearest_commit_sha(client, repo_id, date)
            if not sha:
                continue
            try:
                r = await client.get(f"https://raw.githubusercontent.com/{REPO_NAMES[source]}/"
                                      f"{sha}/.github/scripts/listings.json", timeout=120)
                r.raise_for_status()
                add_all(r.json())
            except Exception as exc:  # noqa: BLE001
                print(f"{source} snapshot @ {date}: {exc}", file=sys.stderr)
    return items


def main() -> None:
    async def run() -> list[dict]:
        async with httpx.AsyncClient() as client:
            return await fetch_items(client)

    items = asyncio.run(run())
    rows, already_2027, skipped = core.collect_coop_companies(github_items=items)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = ["company", "terms_matched", "term_count", "example_job",
              "example_link", "careers_page", "ats", "linkedin"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    with ALREADY_2027_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["company_key", "company"])
        w.writerows(sorted(already_2027.items()))

    core.write_winter_companies()

    from collections import Counter
    winter = sum(1 for r in rows if r.get("winter_2027"))
    counts = Counter(r["term_count"] for r in rows)
    print(f"{len(rows)} companies matched -> {OUT_CSV} ({winter} with Winter 2027)")
    for n in sorted(counts, reverse=True):
        print(f"  {counts[n]:3d} companies matched {n} term(s)")
    print(f"  Winter 2027-first list -> {core.WINTER_CSV}")
    if skipped:
        print(f"  (excluded {skipped} companies that already have Summer 2027 posting evidence)")


if __name__ == "__main__":
    main()
