#!/usr/bin/env python3
"""Companies + intern reviews from interndb.io (Canadian-student internship review site).

interndb's public API lists every company it tracks (~1.3k) and, for companies with reviews,
each review's office location, start date and role. That's direct proof a company hired an
intern in a given city, including small shops (e.g. Demonware, Vancouver) that never show up
in our intern-posting pollers.

Cached to engineering-ats/interndb.json and refreshed weekly, so the outreach page builds offline.
Reviews are fetched one company at a time with a short pause, only for companies that have any.

    .venv/bin/python scripts/interndb.py            # refresh the cache, print a summary
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

API = "https://api.interndb.io/api/v1"
CACHE = core.OUT_DIR / "interndb.json"
REFRESH = 7 * 24 * 60 * 60
PAUSE = 0.5  # seconds between review requests


def _term(start: str) -> str:
    """'2025-09' -> 'Fall 2025' (Jan-Apr Winter, May-Aug Summer, Sep-Dec Fall)."""
    try:
        y, m = start.split("-")[:2]
        m = int(m)
    except (AttributeError, ValueError):
        return ""
    season = "Winter" if m <= 4 else "Summer" if m <= 8 else "Fall"
    return f"{season} {y}"


def _slug(name: str) -> str:
    """interndb's page slug: 'J.P. Morgan' -> 'jp-morgan', 'Johnson & Johnson' -> 'johnson-johnson'."""
    s = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    return re.sub(r"[\s-]+", "-", s).strip("-")


def fetch() -> dict[str, dict]:
    """{company name: {reviews, canada_reviews, us_reviews, locations, terms, last_start, url}}."""
    out: dict[str, dict] = {}
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        r = client.get(f"{API}/companies", params={"q": "", "sortBy": "mostReviews"})
        r.raise_for_status()
        for c in r.json():
            name = (c.get("name") or "").strip()
            if not name:
                continue
            stats = c.get("countryStats") or {}
            rec = {
                "reviews": int(c.get("reviewCount") or 0),
                "canada_reviews": int((stats.get("Canada") or {}).get("reviewCount") or 0),
                "us_reviews": int((stats.get("US") or {}).get("reviewCount") or 0),
                "locations": {}, "terms": [], "last_start": "",
                "url": f"https://www.interndb.io/companies/{_slug(name)}",
            }
            if rec["reviews"]:
                time.sleep(PAUSE)
                rr = client.get(f"{API}/reviews/company/{c['companyId']}",
                                params={"sortBy": "qualityScore"})
                reviews = rr.json() if rr.status_code == 200 else []
                terms = set()
                for rv in reviews if isinstance(reviews, list) else []:
                    loc = (rv.get("Location") or "").strip()
                    if loc:
                        rec["locations"][loc] = rec["locations"].get(loc, 0) + 1
                    start = rv.get("StartDate") or ""
                    if t := _term(start):
                        terms.add(t)
                    rec["last_start"] = max(rec["last_start"], start)
                rec["terms"] = sorted(terms)
            out[name] = rec
    return out


def load(refresh: bool = False) -> dict[str, dict]:
    """Cached companies; refetches weekly (or when asked), keeps the old cache if offline."""
    cached: dict[str, dict] = {}
    if CACHE.exists():
        try:
            cached = json.loads(CACHE.read_text())
        except (OSError, json.JSONDecodeError):
            cached = {}
    fresh = CACHE.exists() and time.time() - CACHE.stat().st_mtime < REFRESH
    if cached and fresh and not refresh:
        return cached
    try:
        data = fetch()
    except (httpx.HTTPError, ValueError):
        return cached
    if not data:
        return cached
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_name(f".{CACHE.name}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1))
    tmp.replace(CACHE)
    return data


def main() -> None:
    data = load(refresh=True)
    reviewed = sum(1 for r in data.values() if r["reviews"])
    ca = sum(1 for r in data.values() if r["canada_reviews"])
    print(f"{len(data)} interndb companies -> {CACHE} ({reviewed} with reviews, {ca} with Canadian ones)")


if __name__ == "__main__":
    main()
