#!/usr/bin/env python3
"""MCP search over intern-employer companies (Canada + US).

Cursor: .cursor/mcp.json → intern-companies
CLI:    .venv/bin/python scripts/company_search_mcp.py --search "payfacto"
        .venv/bin/python scripts/company_search_mcp.py   # stdio MCP server
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402
import outreach_contacts  # noqa: E402
from mcp.server.mcpserver import MCPServer  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
WINTER = core.OUT_DIR / "winter_2027_companies.csv"
US_CSV = core.OUT_DIR / "us_sponsor_companies.csv"
CA_SOFTWARE = core.CA_COMPANIES_CSV
mcp = MCPServer("intern-companies", version="0.1.0",
                instructions="Search Hung's intern-employer list plus Canadian software shops "
                "(Rippling/Ashby/etc that hire SWE in Canada but may not have a live intern req — "
                "Ampliwork class). Use search_companies, then company_people. When you confirm who "
                "to ping (LinkedIn, published email only), call update_outreach_info so /outreach "
                "shows it. Never invent emails.")

_CACHE: tuple[float, list[dict]] | None = None


def _company_people(linkedin: str, keywords: str) -> str:
    m = re.search(r"linkedin\.com/company/([^/?#]+)", linkedin or "", re.I)
    if not m:
        return ""
    slug = m.group(1).rstrip("/")
    return f"https://www.linkedin.com/company/{slug}/people/?keywords={quote(keywords)}"


def _li_people(company: str, kind: str, linkedin: str = "") -> str:
    if kind == "recruiters":
        kw = "university recruiter OR campus recruiter OR early talent OR early careers"
        fallback = (f'"{company}" ("university recruiter" OR "campus recruiter" OR '
                    '"early talent" OR "early careers")')
    else:
        kw = "software engineer OR software developer"
        fallback = f'"{company}" ("software engineer" OR "software developer")'
    return (_company_people(linkedin, kw)
            or f"https://www.linkedin.com/search/results/people/?keywords={quote(fallback)}")


def _company_page(company: str, linkedin: str) -> str:
    if linkedin:
        return linkedin
    return f"https://www.linkedin.com/search/results/companies/?keywords={quote(company)}"


def _slug_map() -> dict[str, str]:
    out: dict[str, str] = {}
    db = core.connect()
    try:
        for name, slug in db.execute(
                "SELECT name, li_slug FROM discovered WHERE li_slug IS NOT NULL AND li_slug != ''"):
            key = core.dedup_key(core.outreach_company_name(name or ""))
            slug = (slug or "").strip().strip("/")
            if key and slug and key not in out:
                out[key] = f"https://www.linkedin.com/company/{slug}/"
    finally:
        db.close()
    return out


def _row(company: str, region: str, extra: dict[str, str], slugs: dict[str, str]) -> dict:
    company = core.outreach_company_name(company).strip()
    key = core.dedup_key(company)
    linkedin = (extra.get("linkedin") or "").strip() or slugs.get(key, "")
    row = {
        "company": company,
        "region": region,
        "open_now": extra.get("open_now") or "",
        "intern_count": int(extra.get("intern_count") or 0),
        "california_count": int(extra.get("california_count") or 0),
        "term": extra.get("term") or extra.get("terms_matched") or "",
        "job": extra.get("example_job") or extra.get("title") or "",
        "job_link": extra.get("example_link") or extra.get("url") or extra.get("link") or "",
        "careers": extra.get("careers_page") or "",
        "ats": extra.get("ats") or "",
        "kind": extra.get("kind") or "intern",
        "linkedin": linkedin,
        "company_page": _company_page(company, linkedin),
        "recruiters": _li_people(company, "recruiters", linkedin),
        "engineers": _li_people(company, "engineers", linkedin),
        "_key": key,
        "_blob": f"{company} {extra.get('term') or extra.get('terms_matched') or ''} "
                 f"{extra.get('example_job') or extra.get('title') or ''} "
                 f"{extra.get('location') or extra.get('evidence') or ''} "
                 f"{extra.get('ats') or ''} {extra.get('kind') or ''}".lower(),
    }
    outreach_contacts.attach(row)
    row["_blob"] += f" {row.get('ping_name') or ''} {row.get('ping_note') or ''}".lower()
    return row


def _mtime() -> float:
    latest = 0.0
    for path in (WINTER, US_CSV, CA_SOFTWARE, Path(str(core.DB_PATH)),
                 Path(str(core.DB_PATH) + "-wal")):
        try:
            latest = max(latest, path.stat().st_mtime)
        except OSError:
            continue
    return latest


def load_companies() -> list[dict]:
    global _CACHE
    stamp = _mtime()
    if _CACHE and _CACHE[0] == stamp:
        return _CACHE[1]
    slugs = _slug_map()
    by_key: dict[str, dict] = {}

    def add(company: str, region: str, extra: dict[str, str]) -> None:
        if not (company or "").strip():
            return
        row = _row(company, region, extra, slugs)
        key = row["_key"]
        if not key:
            return
        hit = by_key.get(key)
        if not hit:
            by_key[key] = row
            return
        if hit["region"] != region:
            hit["region"] = "Canada + US"
        for field in ("linkedin", "careers", "job", "job_link", "term"):
            if not hit[field] and row[field]:
                hit[field] = row[field]
        hit["intern_count"] = max(int(hit.get("intern_count") or 0), int(row.get("intern_count") or 0))
        hit["california_count"] = max(int(hit.get("california_count") or 0),
                                      int(row.get("california_count") or 0))
        if row["open_now"] == "yes":
            hit["open_now"] = "yes"
        if row["linkedin"]:
            hit["company_page"] = _company_page(hit["company"], row["linkedin"])

    if WINTER.exists():
        with WINTER.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                extra = dict(r)
                extra["kind"] = "intern"
                add(r.get("company") or "", "Canada", extra)
    if US_CSV.exists():
        with US_CSV.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                extra = dict(r)
                extra["kind"] = "intern"
                add(r.get("company") or "", "US", extra)
    if CA_SOFTWARE.exists():
        with CA_SOFTWARE.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                key = core.dedup_key(core.outreach_company_name(r.get("company") or ""))
                if not key or key in by_key:
                    continue
                extra = dict(r)
                extra["kind"] = "software"
                extra["terms_matched"] = r.get("evidence") or "Canada software hiring"
                add(r.get("company") or "", "Canada", extra)

    db = core.connect()
    try:
        for company, title, location, url in db.execute(
                "SELECT company, title, location, url FROM postings WHERE open=1"):
            if not core.is_software_internship(title or "", software_employer=True):
                continue
            countries = core.us_canada(location or "")
            if "Canada" in countries:
                region = "Canada"
            elif "US" in countries:
                region = "US"
            else:
                continue
            add(company or "", region, {
                "kind": "intern",
                "open_now": "yes",
                "example_job": f"{title} ({location})" if location else title,
                "example_link": url or "",
                "terms_matched": ", ".join(core.posting_terms(title or "", url or "")),
                "intern_count": "1",
            })
    finally:
        db.close()

    rows = list(by_key.values())
    _CACHE = (stamp, rows)
    return rows


def _score(row: dict, query: str) -> int | None:
    q = query.strip().lower()
    if not q:
        return 50
    name = row["company"].lower()
    if name == q:
        return 0
    if name.startswith(q):
        return 1
    if q in name:
        return 2
    tokens = [t for t in re.split(r"\s+", q) if t]
    if tokens and all(t in name for t in tokens):
        return 3
    blob = row["_blob"]
    if tokens and all(t in blob for t in tokens):
        return 4
    return None


def search_companies(query: str, region: str = "all", limit: int = 20) -> list[dict]:
    """Find intern-employer companies. query matches name, intern title, or location."""
    region = (region or "all").strip().lower()
    limit = max(1, min(int(limit or 20), 50))
    hits: list[tuple[int, dict]] = []
    for row in load_companies():
        if region == "canada" and "Canada" not in row["region"]:
            continue
        if region == "us" and "US" not in row["region"]:
            continue
        rank = _score(row, query)
        if rank is None:
            continue
        public = {k: v for k, v in row.items() if not k.startswith("_")}
        hits.append((rank, public))
    hits.sort(key=lambda x: (
        x[0],
        -core.intern_priority(x[1].get("intern_count") or 0, x[1].get("california_count") or 0),
        x[1]["company"].lower(),
    ))
    return [row for _rank, row in hits[:limit]]


@mcp.tool(name="search_companies")
def search_companies_tool(query: str, region: str = "all", limit: int = 20) -> list[dict]:
    """Search intern employers plus Canadian software shops with no live SWE intern req.

    Use this to find a company by name, intern title, or location (e.g. "ampliwork",
    "montreal", "payfacto"). kind=intern is on the winter/US intern lists; kind=software
    is a Canada SWE employer (Rippling/Ashby/etc) that hasn't posted a software intern.
    region is all, canada, or us.
    """
    return search_companies(query, region, limit)


@mcp.tool()
def update_outreach_info(company: str, ping_name: str = "", ping_linkedin: str = "",
                         ping_title: str = "", email: str = "", job: str = "",
                         job_link: str = "", job_id: str = "", city: str = "",
                         note: str = "", channel: str = "") -> dict:
    """Save who to ping onto the /outreach page. LinkedIn preferred.

    Only pass email if the company published it (careers page, Play listing).
    Leave email blank otherwise — no RocketReach, no guessed @company.com.
    Empty fields are ignored (existing values stay).
    """
    kwargs = {k: v for k, v in {
        "ping_name": ping_name, "ping_linkedin": ping_linkedin, "ping_title": ping_title,
        "email": email, "job": job, "job_link": job_link, "job_id": job_id,
        "city": city, "note": note, "channel": channel,
    }.items() if v}
    return outreach_contacts.update(company, **kwargs)


@mcp.tool()
def get_outreach_info(company: str) -> dict:
    """Named ping target currently shown on /outreach for this company."""
    return outreach_contacts.get(company) or {"company": company, "ping_name": ""}


@mcp.tool()
def list_outreach_info() -> list[dict]:
    """Every company with a saved /outreach ping target."""
    return outreach_contacts.list_all()


@mcp.tool()
def company_people(company: str) -> dict:
    """LinkedIn recruiter / engineer / company links for one intern employer."""
    hits = search_companies(company, "all", 5)
    if hits and hits[0]["company"].lower() == company.strip().lower():
        row = hits[0]
    elif hits:
        row = hits[0]
    else:
        name = company.strip()
        row = {
            "company": name,
            "region": "",
            "company_page": _company_page(name, ""),
            "recruiters": _li_people(name, "recruiters"),
            "engineers": _li_people(name, "engineers"),
        }
    outreach_contacts.attach(row)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--search", help="run a search on stdout instead of the MCP server")
    parser.add_argument("--region", default="all", help="all, canada, or us")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if args.search is not None:
        print(json.dumps(search_companies(args.search, args.region, args.limit), indent=2))
        return
    mcp.run()


if __name__ == "__main__":
    main()
