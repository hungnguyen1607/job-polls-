#!/usr/bin/env python3
"""Co-op employers from student-run class profiles (UW CS Club, csclub.uwaterloo.ca/classprofile).

Each graduating class surveys where its students did co-op. That lists the small startups that
hire students without ever posting publicly (WaterlooWorks / referral shops), with proof a student
actually worked there. The pages are Next.js apps, so the employer word clouds live in the page's
JS chunk as {text|category: "Name", value: N} arrays.

Cached to engineering-ats/class_profile_employers.json and refreshed weekly, so outreach builds
offline.

    .venv/bin/python scripts/class_profiles.py           # refresh the cache, print a summary
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

CACHE = core.OUT_DIR / "class_profile_employers.json"
REFRESH = 7 * 24 * 60 * 60
BASE = "https://csclub.uwaterloo.ca/classprofile/{year}/coop"
# 2022 is a pages-router build, 2023+ app-router; the chunk regex covers both.
YEARS = range(2022, 2031)
_CHUNK = re.compile(r'(/classprofile/\d+/_next/)?(static/chunks/(?:app/coop/page|pages/coop)-[a-f0-9]+\.js)')
_ARRAY = re.compile(r'\[(?:\{(?:text|category):"[^"]*",value:\d+\},?)+\]')
_ITEM = re.compile(r'\{(?:text|category):"([^"]*)",value:(\d+)\}')
# An employer word cloud always has at least one of these; location/salary/yes-no arrays don't.
_ANCHORS = {"google", "amazon", "meta", "meta/facebook", "facebook", "rbc", "shopify", "microsoft",
            "td bank", "cibc", "sap", "d2l", "opentext", "aws", "wealthsimple"}
_JUNK = re.compile(
    r"^(a startup|ai startup|canada startup company|faang|not going to say|n/?a|none|other|"
    r"redacted|research group.*|.*\buniversity\b.*|.*\bcollege\b.*|.*\bschool\b.*|"
    r"government.*|.*\bministry\b.*|canada revenue agency|statistics canada|"
    r"department of national defen[cs]e|.*\bmunicipality\b.*|toronto transit commission|"
    r"canadian security intelligence service|cse|wsib|lcbo|olg|uhn|mpac)$",
    re.I,
)


def _fetch(client: httpx.Client, url: str) -> str:
    r = client.get(url)
    return r.text if r.status_code == 200 else ""


def parse_employers(js: str) -> dict[str, int]:
    """Employer name -> mentions, from every word-cloud array that looks like employers."""
    out: dict[str, int] = {}
    for m in _ARRAY.finditer(js):
        items = _ITEM.findall(m.group(0))
        if len(items) < 5 or not ({n.strip().lower() for n, _ in items} & _ANCHORS):
            continue
        for name, value in items:
            name = re.sub(r"\s+", " ", name).strip()
            if name and not _JUNK.match(name):
                out[name] = out.get(name, 0) + int(value)
    return out


def fetch_profiles() -> dict[str, dict]:
    """{company: {"years": [...], "mentions": n, "source": url}} across every published year."""
    merged: dict[str, dict] = {}
    by_key: dict[str, str] = {}
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for year in YEARS:
            page_url = BASE.format(year=year)
            html = _fetch(client, page_url)
            m = _CHUNK.search(html)
            if not m:
                continue
            js = _fetch(client, f"https://csclub.uwaterloo.ca/classprofile/{year}/_next/{m.group(2)}")
            for name, n in parse_employers(js).items():
                key = core.dedup_key(core.outreach_company_name(name))
                if not key:
                    continue
                canon = by_key.setdefault(key, name)
                rec = merged.setdefault(canon, {"years": [], "mentions": 0, "source": page_url})
                if year not in rec["years"]:
                    rec["years"].append(year)
                rec["mentions"] += n
                rec["source"] = page_url  # latest year that lists them
    return merged


def load(refresh: bool = False) -> dict[str, dict]:
    """Cached employers; refetches weekly (or when asked), keeps the old cache if offline."""
    cached: dict[str, dict] = {}
    if CACHE.exists():
        try:
            cached = json.loads(CACHE.read_text())
        except (OSError, json.JSONDecodeError):
            cached = {}
    fresh = CACHE.exists() and time.time() - CACHE.stat().st_mtime < REFRESH
    if cached.get("employers") and fresh and not refresh:
        return cached["employers"]
    try:
        employers = fetch_profiles()
    except httpx.HTTPError:
        employers = {}
    if not employers:
        return cached.get("employers") or {}
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE.with_name(f".{CACHE.name}.tmp")
    tmp.write_text(json.dumps({"employers": employers}, indent=1, ensure_ascii=False))
    tmp.replace(CACHE)
    return employers


def main() -> None:
    employers = load(refresh=True)
    years = sorted({y for r in employers.values() for y in r["years"]})
    print(f"{len(employers)} co-op employers from UW CS class profiles {years} -> {CACHE}")


if __name__ == "__main__":
    main()
