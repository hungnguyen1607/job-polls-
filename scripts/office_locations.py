#!/usr/bin/env python3
"""Every major US/Canada city each company has an office in, from all of its job postings.

Our pollers only store intern postings, so a company's intern cities miss most of its offices.
The upstream ats-scrapers dataset (storage.stapply.ai/jobhive, ~5.8M jobs across 64 ATSes,
every role) lists where each company is hiring right now. Reading only the company + location
columns of its parquet snapshot (HTTP range reads, no 2.5 GB download) and mapping locations to
metros gives a close proxy for "where their offices are".

Cached to engineering-ats/office_locations.json ({dedup key: {metro: job count}}). The outreach
page only reads the cache (a refresh takes several minutes), so rerun this weekly.

    .venv/bin/python scripts/office_locations.py      # refresh the cache, print a summary
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402
from metros import metros_in  # noqa: E402

SNAPSHOT = "https://storage.stapply.ai/jobhive/v1/all.parquet"
CACHE = core.OUT_DIR / "office_locations.json"
REFRESH = 7 * 24 * 60 * 60


def fetch(source: str = SNAPSHOT) -> dict[str, dict[str, int]]:
    import polars as pl

    lf = pl.scan_parquet(source)
    if "n" in lf.collect_schema().names():  # already aggregated (company, location, n)
        pairs = lf.select(["company", "location", "n"]).collect()
    else:
        pairs = (lf.select(["company", "location", "country_iso"])
                 .filter(pl.col("country_iso").is_in(["US", "CA"])
                         | pl.col("location").str.contains("(?i)canada|united states|usa"))
                 .group_by(["company", "location"]).agg(pl.len().alias("n"))
                 .collect())
    out: dict[str, dict[str, int]] = {}
    loc_cache: dict[str, list[str]] = {}
    key_cache: dict[str, str] = {}
    for company, loc, n in pairs.iter_rows():
        metros = loc_cache.get(loc)
        if metros is None:
            metros = loc_cache[loc] = metros_in(loc or "")
        if not metros:
            continue
        k = key_cache.get(company)
        if k is None:
            k = key_cache[company] = core.dedup_key(core.outreach_company_name(company or ""))
        if not k:
            continue
        per = out.setdefault(k, {})
        for m in metros:
            per[m] = per.get(m, 0) + int(n)
    return out


def load(refresh: bool = False) -> dict[str, dict[str, int]]:
    """Cached map; refetches only when asked and the cache is over a week old."""
    cached: dict[str, dict[str, int]] = {}
    if CACHE.exists():
        try:
            cached = json.loads(CACHE.read_text())
        except (OSError, json.JSONDecodeError):
            cached = {}
    fresh = CACHE.exists() and time.time() - CACHE.stat().st_mtime < REFRESH
    if (cached and fresh) or not refresh:
        return cached
    try:
        data = fetch()
    except Exception:  # noqa: BLE001 - network/parquet errors: keep the old cache
        return cached
    if not data:
        return cached
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_name(f".{CACHE.name}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.replace(CACHE)
    return data


def main() -> None:
    src = sys.argv[1] if len(sys.argv) > 1 else None
    if src:  # build from a local parquet (e.g. an already-downloaded snapshot slice)
        data = fetch(src)
        CACHE.write_text(json.dumps(data, ensure_ascii=False))
    else:
        data = load(refresh=True)
    print(f"{len(data)} companies with US/Canada office cities -> {CACHE}")


if __name__ == "__main__":
    main()
