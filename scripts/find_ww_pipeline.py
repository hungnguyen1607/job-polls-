#!/usr/bin/env python3
"""Find Toronto/Montreal companies that hire SWE co-ops off WaterlooWorks
(or only show up as intern alumni), not a public intern req.

Does not log into LinkedIn or WaterlooWorks. Sources: known WW-gated shops,
our intern-history DB (hired students, no live public req), and clickable
LinkedIn/Google alumni searches.

    .venv/bin/python scripts/find_ww_pipeline.py
    .venv/bin/python scripts/find_ww_pipeline.py --city Toronto
    .venv/bin/python scripts/ww_pipeline_mcp.py   # MCP server

Writes engineering-ats/ww_pipeline_toronto_montreal.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote, quote_plus

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
import find_contacts as fc  # noqa: E402
import internship_core as core  # noqa: E402

OUT_CSV = core.OUT_DIR / "ww_pipeline_toronto_montreal.csv"
FIELDS = ["company", "city", "signal", "public_intern", "people_intern_url",
          "evidence", "source_url"]

CITIES = ("Toronto", "Montreal", "San Francisco")
SCHOOLS = {
    "waterloo": "University of Waterloo",
    "mcgill": "McGill",
    "concordia": "Concordia",
    "polytechnique": "Polytechnique Montréal",
    "ets": "ÉTS",
}
# Montreal shops lean on McGill / Concordia / Poly / ÉTS the way Toronto ones lean on WaterlooWorks.
MTL_SCHOOLS = ("mcgill", "concordia", "polytechnique", "ets")
# Job boards, staffing and consulting resellers that show up as "employers" on ATS feeds.
NOT_A_SHOP = re.compile(
    r"\b(eluta|mthree|job wrapping|tehora|hikinex|fastwater|jay analytix|1pertinent|"
    r"sopra steria|appnovation|artefact|vo2 group|cmhc|cdpq|caisse de d[ée]p[ôo]t|"
    r"domtar|nbcuniversal|ubisoft|okta|rogers|fitch|opentable|kaseya|system canada technologies|"
    r"careers inc|levio|konrad ?group|\bgft\b|act digital|r&d partners|human agency)\b",
    re.I,
)
# Verified this cycle: they take SWE co-ops via school boards / alumni, not a
# public intern req (or the public page just says "apply on WaterlooWorks").
KNOWN = [
    {"company": "Mantle", "city": "Toronto", "signal": "waterlooworks",
     "evidence": "withmantle.com/waterloo-coop-program — apply WW or recruiting@withmantle.com",
     "source_url": "https://withmantle.com/waterloo-coop-program"},
    {"company": "Mash", "city": "Toronto", "signal": "school boards",
     "evidence": "Careers page: apply through University of Waterloo and UofT co-op",
     "source_url": "https://mash.com/careers/software-engineer-co-op"},
    {"company": "Spellbook", "city": "Toronto", "signal": "intern alumni",
     "evidence": "UW SWE co-ops exist; Ashby has no intern req",
     "source_url": "https://jobs.ashbyhq.com/spellbook.com"},
    {"company": "CoinWa", "city": "Toronto", "signal": "uw exclusive",
     "evidence": "Public post: hiring UW co-op students exclusively",
     "source_url": "https://www.linkedin.com/posts/naimah-venezia-54098b1b1_now-hiring-university-of-waterloo-co-op-students-activity-7495553626585620481-jSG5"},
    {"company": "Modular", "city": "Toronto", "signal": "waterlooworks",
     "evidence": "W26 Cloud Inference co-op posted as apply on WaterlooWorks",
     "source_url": "https://www.linkedin.com/posts/dhillondeep_were-hiring-university-of-waterloo-winter-activity-7372017056856199168-4Bvc"},
    {"company": "FreshBooks", "city": "Toronto", "signal": "intern alumni",
     "evidence": "Fullstack SWE intern/co-op hires; BambooHR often has no intern req",
     "source_url": "https://freshbooks.bamboohr.com/careers"},
]
LARGE = re.compile(
    r"\b(cibc|rbc|royal bank|td bank|scotiabank|bmo|manulife|sun life|amazon|"
    r"google|microsoft|ibm|cgi|accenture|deloitte|ericsson|huawei|qualcomm|"
    r"adobe|intel|nvidia|bombardier|autodesk|celestica|ciena|ge aerospace|"
    r"ge vernova|abbvie|danaher|garmin|allegion)\b",
    re.I,
)
JOINING_RE = re.compile(
    r"(?:joining|joined|incoming(?:\s+\w+)?(?:\s+at)?)\s+"
    r"([A-Z][\w.&'’-]{1,40}(?:\s+[A-Z][\w.&'’-]{1,30}){0,4})",
)
AT_RE = re.compile(
    r"(?:Software Engineering Co-op|Software Engineer Intern|SWE Intern|"
    r"Software Developer Co-op|Co-op Software Developer)\s+(?:at|@)\s+"
    r"([A-Z][\w.&'’-]{1,40}(?:\s+[A-Z][\w.&'’-]{1,30}){0,4})",
    re.I,
)
DASH_RE = re.compile(
    r"[-–|]\s*(Software Engineering Co-op|SWE Intern|Software Engineer Intern)"
    r"\s+(?:at|@)\s+([^|-]+)",
    re.I,
)
SKIP_COMPANY = re.compile(
    r"university of waterloo|uwaterloo|concordia|mcgill|polytechnique|"
    r"university of toronto|\buoft\b|york university|linkedin|google|meta|"
    r"facebook|amazon|microsoft|apple|netflix|openai",
    re.I,
)


def _city_in(text: str) -> str:
    t = (text or "").lower()
    if re.search(r"montr[eé]al", t):
        return "Montreal"
    if re.search(r"toronto", t):
        return "Toronto"
    if re.search(r"san francisco|\bsf\b|bay area|palo alto|mountain view", t):
        return "San Francisco"
    return ""


def _in_city(text: str, city: str) -> bool:
    got = _city_in(text)
    return got.lower() == city.lower() if city else bool(got)


def _clean_company(name: str) -> str:
    name = re.sub(r"\s+", " ", name or "").strip(" .,;:-–|")
    name = re.sub(r"\s+(as a|as an|for the|this|the team)\b.*$", "", name, flags=re.I)
    if len(name) < 2 or len(name) > 50:
        return ""
    if SKIP_COMPANY.search(name):
        return ""
    return core.outreach_company_name(name)


def extract_company(title: str, snippet: str) -> str:
    text = f"{title} {snippet}"
    for rx in (AT_RE, DASH_RE, JOINING_RE):
        m = rx.search(text)
        if m:
            company = _clean_company(m.group(m.lastindex or 1))
            if company:
                return company
    return ""


def _school_for(city: str, school: str) -> str:
    """Montreal defaults to its own schools instead of Waterloo."""
    school = (school or "").lower()
    if (city or "").lower().startswith("montr") and school in ("", "waterloo", "all"):
        return "montreal"
    return school or "waterloo"


def people_url(company: str, city: str = "", school: str = "waterloo") -> str:
    school = _school_for(city, school)
    if school == "montreal":
        school_name = "(" + " OR ".join(SCHOOLS[s] for s in MTL_SCHOOLS) + ")"
    else:
        school_name = SCHOOLS.get(school, SCHOOLS["waterloo"])
    bits = [f'"{company}"', "intern OR co-op", school_name]
    if city:
        bits.append(city)
    return "https://www.linkedin.com/search/results/people/?keywords=" + quote(" ".join(bits))


def google_alumni_query(city: str, school: str = "waterloo") -> str:
    school_name = SCHOOLS.get((school or "waterloo").lower(), SCHOOLS["waterloo"])
    q = f'site:linkedin.com/in "{school_name}" "{_alumni_role(city)}" {city}'
    return "https://www.google.com/search?q=" + quote_plus(q)


def _alumni_role(city: str) -> str:
    if "francisco" in (city or "").lower() or city.lower() == "sf":
        return "Software Engineering Intern"
    return "Software Engineering Co-op"


def linkedin_alumni_query(city: str, school: str = "waterloo") -> str:
    school_name = SCHOOLS.get((school or "waterloo").lower(), SCHOOLS["waterloo"])
    q = f'"{school_name}" "{_alumni_role(city)}" {city}'
    return "https://www.linkedin.com/search/results/people/?keywords=" + quote(q)


def alumni_search_urls(city: str = "", school: str = "waterloo") -> list[dict[str, str]]:
    cities = (city,) if city else CITIES
    out = []
    for c in cities:
        pick = _school_for(c, school)
        if pick == "montreal":
            schools = MTL_SCHOOLS
        elif pick == "all":
            schools = tuple(SCHOOLS)
        else:
            schools = (pick,) if pick in SCHOOLS else ("waterloo",)
        for s in schools:
            out.append({
                "city": c,
                "school": SCHOOLS[s],
                "linkedin_people": linkedin_alumni_query(c, s),
                "google": google_alumni_query(c, s),
                "how": (f'LinkedIn People → school = {SCHOOLS[s]} → '
                        f'keywords "Software Engineering Co-op" {c}. '
                        "Company name is the current/past employer. "
                        "If careers has no intern req, it's the hidden class."),
            })
    return out


def public_intern_names() -> set[str]:
    names: set[str] = set()
    for path in (core.CA_CSV_PATH, core.ALL_CSV_PATH):
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                n = core.dedup_key(core.outreach_company_name(row.get("company") or ""))
                if n:
                    names.add(n)
    return names


def _slug_map() -> dict[str, str]:
    out: dict[str, str] = {}
    db = core.connect()
    try:
        for name, slug in db.execute(
                "SELECT name, li_slug FROM discovered WHERE li_slug IS NOT NULL AND li_slug != ''"):
            key = core.dedup_key(core.outreach_company_name(name or ""))
            slug = (slug or "").strip().strip("/")
            if key and slug and key not in out:
                out[key] = slug
    except Exception:  # noqa: BLE001 — empty/old DBs still work
        return out
    finally:
        db.close()
    return out


def company_intern_alumni(company: str, city: str = "Toronto",
                          school: str = "waterloo") -> dict:
    name = core.outreach_company_name(company.strip())
    key = core.dedup_key(name)
    slug = _slug_map().get(key, "")
    people_tab = (f"https://www.linkedin.com/company/{slug}/people/?keywords="
                  + quote("intern OR co-op") if slug else "")
    return {
        "company": name,
        "city": city,
        "school": SCHOOLS.get((school or "waterloo").lower(), SCHOOLS["waterloo"]),
        "people_tab_interns": people_tab,
        "alumni_search": people_url(name, city, school),
        "ask": ("Hi — Concordia SWE intern (not Waterloo). Do you hire SWE intern/"
                "co-ops from outside WaterlooWorks, or is it UW-only?"),
    }


def _history_hidden(city: str, public: set[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    db = core.connect()
    try:
        core._intern_history_table(db)
        core._swe_intern_history_table(db)
        q = list(db.execute(
            "SELECT company, title, location, url, term FROM ca_intern_history"))
        q += list(db.execute(
            "SELECT company, title, location, url, term FROM swe_intern_history "
            "WHERE region='Canada'"))
    except Exception:  # noqa: BLE001
        db.close()
        return rows
    db.close()
    for company, title, location, url, term in q:
        if not _in_city(f"{location or ''} {title or ''}", city):
            continue
        if not core.is_software_internship(title or ""):
            continue
        name = core.outreach_company_name(company or "")
        key = core.dedup_key(name)
        if not key or key in seen or LARGE.search(name):
            continue
        seen.add(key)
        live = key in public
        if live:
            continue
        rows.append({
            "company": name,
            "city": city,
            "signal": "hired interns, no live public req",
            "public_intern": "no",
            "people_intern_url": people_url(name, city),
            "evidence": f"{title or ''} ({term or ''}) {location or ''}".strip(),
            "source_url": (url or "").split("?")[0],
        })
    return rows


def _hires_devs_no_intern(city: str, public: set[str]) -> list[dict[str, str]]:
    """Startups polled on their own ATS with a live software job in the city but no intern
    req ever seen (Botpress class). Most-hiring first; they're who to cold-ask about interns."""
    db = core.connect()
    try:
        q = list(db.execute(
            "SELECT company, board_url, jobs, example_title, example_location, example_url "
            "FROM ca_software_hiring"))
    except Exception:  # noqa: BLE001 — empty/old DBs still work
        return []
    finally:
        db.close()
    best: dict[str, dict[str, str]] = {}
    jobs_of: dict[str, int] = {}
    key_of_board: dict[str, str] = {}  # one company per board ("Broadsign" / "Carrières Broadsign")
    for company, board, jobs, title, location, url in q:
        if not _in_city(location or "", city):
            continue
        name = core.outreach_company_name(company or "")
        key = key_of_board.setdefault((board or "").lower().rstrip("/") or name, core.dedup_key(name))
        if (not key or key in public or LARGE.search(name) or NOT_A_SHOP.search(name)
                or core._AGENCY_RE.search(name)):
            continue
        n = int(jobs or 0)
        if key in best and jobs_of[key] >= n:
            continue
        jobs_of[key] = n
        best[key] = {
            "company": name,
            "city": city,
            "signal": "hires devs, no intern req",
            "public_intern": "no",
            "people_intern_url": people_url(name, city),
            "evidence": f"{n} software job{'s' if n != 1 else ''}, e.g. {title or ''} ({location or ''})",
            "source_url": board or (url or "").split("?")[0],
        }
    return sorted(best.values(), key=lambda r: (-jobs_of[core.dedup_key(r["company"])],
                                                r["company"].lower()))


def _search(query: str) -> list[dict[str, str]]:
    brave = os.environ.get("BRAVE_API_KEY")
    if brave:
        r = httpx.get("https://api.search.brave.com/res/v1/web/search",
                      params={"q": query, "count": 10},
                      headers={"X-Subscription-Token": brave, "Accept": "application/json"},
                      timeout=20)
        if r.status_code == 200:
            return [{"title": x.get("title", ""), "url": x.get("url", ""),
                     "snippet": x.get("description", "")}
                    for x in (r.json().get("web") or {}).get("results", [])]
    return fc.ddg_search(query)


def _live_harvest(city: str, public: set[str]) -> list[dict[str, str]]:
    if _school_for(city, "") == "montreal":
        queries = [f'site:linkedin.com/in "{SCHOOLS[s]}" "Software" (stagiaire OR intern OR co-op) {city}'
                   for s in MTL_SCHOOLS]
        queries.append(f'"McGill" OR "Concordia" "software" (stage OR intern OR co-op) {city} startup')
    else:
        school = SCHOOLS["waterloo"]
        queries = [
            f'site:linkedin.com/in "{school}" "Software Engineering Co-op" {city}',
            f'"WaterlooWorks" "software" (co-op OR intern) {city}',
            f'"hiring University of Waterloo" (co-op OR intern) {city}',
        ]
    by_key: dict[str, dict[str, str]] = {}
    for q in queries:
        try:
            results = _search(q)
        except SystemExit:
            break
        except Exception:  # noqa: BLE001
            continue
        for r in results:
            title, snippet, url = r.get("title") or "", r.get("snippet") or "", r.get("url") or ""
            company = extract_company(title, snippet)
            if not company:
                continue
            key = core.dedup_key(company)
            if not key or key in by_key or LARGE.search(company):
                continue
            loc = _city_in(f"{title} {snippet} {city}") or city
            signal = "waterlooworks" if "waterlooworks" in f"{title} {snippet}".lower() else "intern alumni"
            by_key[key] = {
                "company": company,
                "city": loc,
                "signal": signal,
                "public_intern": "yes" if key in public else "no",
                "people_intern_url": people_url(company, loc),
                "evidence": re.sub(r"\s+", " ", f"{title} {snippet}")[:240],
                "source_url": url.split("?")[0],
            }
    return list(by_key.values())


def find_hidden_intern_shops(city: str = "Toronto", school: str = "waterloo",
                             live_search: bool = False, limit: int = 80,
                             include_no_intern: bool = True) -> dict:
    """WaterlooWorks / school-board intern shops in Toronto or Montreal, then startups that
    hire devs there but never posted an intern req (cold-ask list)."""
    city = "Montreal" if (city or "").lower().startswith("montr") else "Toronto"
    school = (school or "waterloo").lower()
    if school not in SCHOOLS and school != "all":
        school = "waterloo"
    public = public_intern_names()
    by_key: dict[str, dict[str, str]] = {}

    def add(row: dict[str, str]) -> None:
        key = core.dedup_key(row.get("company") or "")
        if not key or key in by_key:
            return
        by_key[key] = row

    for row in KNOWN:
        if row["city"] == city:
            add({**row, "public_intern": "yes" if core.dedup_key(row["company"]) in public else "no",
                 "people_intern_url": people_url(row["company"], city, school)})
    for row in _history_hidden(city, public):
        add(row)
    if live_search:
        for row in _live_harvest(city, public):
            add(row)
    known = list(by_key.values())
    rank = {"waterlooworks": 0, "uw exclusive": 1, "school boards": 2, "intern alumni": 3}
    known.sort(key=lambda r: (rank.get(r["signal"], 9), r["company"].lower()))
    # Already sorted most-hiring first; kept after every intern-evidence row.
    devs = [r for r in _hires_devs_no_intern(city, public)
            if core.dedup_key(r["company"]) not in by_key] if include_no_intern else []

    companies = known + devs
    limit = max(1, min(int(limit or 80), 1000))
    return {
        "city": city,
        "school": school,
        "note": ("WaterlooWorks is not public. Open alumni_search_urls in LinkedIn, "
                 "copy company names from intern profiles, then ping shops with "
                 "no intern req. Montreal uses McGill / Concordia / Poly / ÉTS alumni. "
                 "'hires devs, no intern req' rows have no intern evidence at all: "
                 "they're a cold ask, not a known intern shop."),
        "total_found": len(companies),
        "alumni_search_urls": alumni_search_urls(city, school),
        "companies": companies[:limit],
        "count": min(len(companies), limit),
    }


def search_urls() -> list[tuple[str, str]]:
    out = []
    for city in CITIES:
        for school in SCHOOLS:
            out.append((city, google_alumni_query(city, school)))
    return out


def harvest(_client: httpx.Client | None = None) -> list[dict[str, str]]:
    rows = []
    for city in CITIES:
        rows.extend(find_hidden_intern_shops(city, live_search=True)["companies"])
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--urls-only", action="store_true")
    parser.add_argument("--city", choices=["Toronto", "Montreal", "toronto", "montreal"])
    parser.add_argument("--school", default="waterloo",
                        help="waterloo, mcgill, concordia, or all")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.urls_only:
        for row in alumni_search_urls(args.city or "", args.school):
            print(f"[{row['city']} / {row['school']}] {row['linkedin_people']}")
        return
    cities = (args.city.title(),) if args.city else CITIES
    payload = [find_hidden_intern_shops(c, args.school, live_search=False, limit=1000) for c in cities]
    if args.json:
        print(json.dumps(payload if args.city else payload, indent=2))
        return
    core.OUT_DIR.mkdir(parents=True, exist_ok=True)
    # find_hidden_intern_shops maps San Francisco onto Toronto, so drop the repeat rows.
    seen_rows: set[tuple[str, str]] = set()
    flat = []
    for block in payload:
        for co in block["companies"]:
            k = (co["city"], core.dedup_key(co["company"]))
            if k not in seen_rows:
                seen_rows.add(k)
                flat.append(co)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(flat)
    print(f"{len(flat)} companies -> {OUT_CSV}")
    for block in payload:
        print(f"\n{block['city']}")
        for row in block["alumni_search_urls"][:2]:
            print(f"  {row['linkedin_people']}")
        for co in block["companies"][:15]:
            print(f"  {co['company']}  ({co['signal']})")


if __name__ == "__main__":
    main()
