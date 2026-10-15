#!/usr/bin/env python3
"""Find recruiters and engineers for internships in engineering-ats/canada_internships.csv
(or US_internships.csv with --us).

For each selected internship (company + location), finds public LinkedIn
profiles through a search engine (never LinkedIn itself, no login):
  - recruiters: technical / university / campus recruiters, talent acquisition,
                talent partners, sourcers, early-careers (and French titles)
  - engineers:  software engineers / developers at that company in that city
and adds a work-email guess from the company's email format.

Writes engineering-ats/contacts.csv (next to the internship CSVs). It only finds
people; it never contacts anyone.

Search works with no key (DuckDuckGo, throttled). Optional keys, used when set:
  BRAVE_API_KEY  or  GOOGLE_API_KEY + GOOGLE_CSE_ID     faster search
  HUNTER_API_KEY                                         real email format per company

Usage:
  python scripts/find_contacts.py --company Robinhood
  python scripts/find_contacts.py --company Robinhood --location Toronto
  python scripts/find_contacts.py --new 10                  # 10 newest internships not looked up yet
  python scripts/find_contacts.py --company Nakisa --location Montreal --direct
                                                            # any company, no internship needed
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "engineering-ats"
CA_INTERNSHIPS_CSV = OUT_DIR / "canada_internships.csv"
US_INTERNSHIPS_CSV = OUT_DIR / "US_internships.csv"
CONTACTS_CSV = OUT_DIR / "contacts.csv"
FIELDS = ["found_at", "company", "job_title", "job_location", "job_link", "role_type", "name",
          "headline", "linkedin_url", "email", "email_confidence"]

# One search per role (all title variants OR-ed together), so the free 100 Google searches
# a day cover ~25-50 companies; page 2 is fetched only when page 1 came up short.
RECRUITER_TERMS = ('recruiter OR "technical recruiter" OR "university recruiter" OR "campus recruiter" OR '
                   '"talent acquisition" OR "talent partner" OR "early talent" OR "early careers" OR sourcer '
                   'OR "recruiting coordinator"')
RECRUITER_TERMS_FR = ' OR recruteur OR recruteuse OR "acquisition de talents"'
ENGINEER_TERMS = '"software engineer" OR "software developer" OR "full stack developer"'
ENGINEER_TERMS_FR = ' OR "développeur logiciel" OR "développeuse logiciel"'
MAX_PAGES = 2
RECRUITER_RE = re.compile(r"recruit|talent|university|campus|early career|sourc|people partner|"
                          r"recrut|acquisition de talents", re.I)
ENGINEER_RE = re.compile(r"engineer|developer|swe\b|software|d[ée]veloppeu|ing[ée]nieur", re.I)
QUEBEC_RE = re.compile(r"montr[eé]al|qu[eé]bec|\bQC\b|laval|longueuil|sherbrooke|gatineau", re.I)
# Job-board ATS hosts: not the company's own email domain.
ATS_HOSTS = re.compile(r"(greenhouse|lever|ashbyhq|myworkdayjobs|workday|icims|smartrecruiters|"
                       r"jazzhr|applytojob|bamboohr|rippling|gem\.com|oraclecloud|successfactors|"
                       r"sapsf|taleo|jobvite|recruitee|breezy|pinpoint|teamtailor|linkedin)\.", re.I)


# --- search -------------------------------------------------------------------

DDG_GAP_SECONDS = 8.0  # DuckDuckGo refuses (HTTP 202) at a faster pace
_last_ddg = 0.0


def search(client: httpx.Client, query: str, count: int = 10, page: int = 0) -> list[dict[str, str]]:
    """Google Custom Search, else Brave, else DuckDuckGo (no key; it blocks LinkedIn people
    searches quickly, so a key is the reliable option). `page` 0 = results 1-10."""
    key, cx = os.environ.get("GOOGLE_API_KEY"), os.environ.get("GOOGLE_CSE_ID")
    if key and cx:
        r = client.get("https://www.googleapis.com/customsearch/v1",
                       params={"key": key, "cx": cx, "q": query, "num": min(count, 10), "start": 1 + 10 * page})
        r.raise_for_status()
        return [{"title": x.get("title", ""), "url": x.get("link", ""), "snippet": x.get("snippet", "")}
                for x in r.json().get("items", [])]
    brave = os.environ.get("BRAVE_API_KEY")
    if brave:
        r = client.get("https://api.search.brave.com/res/v1/web/search",
                       params={"q": query, "count": count, "offset": page},
                       headers={"X-Subscription-Token": brave, "Accept": "application/json"})
        r.raise_for_status()
        return [{"title": x.get("title", ""), "url": x.get("url", ""), "snippet": x.get("description", "")}
                for x in (r.json().get("web") or {}).get("results", [])]
    return ddg_search(query) if page == 0 else []


def ddg_search(query: str) -> list[dict[str, str]]:
    """DuckDuckGo's HTML results, no key. httpcloak's browser-like connection is needed:
    DuckDuckGo answers plain HTTP clients with a 202 bot check."""
    import html as _html
    from urllib.parse import parse_qs, quote_plus, unquote

    import httpcloak

    global _last_ddg
    time.sleep(max(0.0, DDG_GAP_SECONDS - (time.monotonic() - _last_ddg)))
    _last_ddg = time.monotonic()
    r = httpcloak.get(f"https://html.duckduckgo.com/html/?q={quote_plus(query)}", timeout=20_000)
    if r.status_code != 200 or "anomaly" in r.text[:5000].lower():
        raise SystemExit(f"DuckDuckGo refused the search (HTTP {r.status_code}); wait a few minutes and rerun.")
    out = []
    for block in re.findall(r'<div class="result results_links.*?</div>\s*</div>', r.text, re.S):
        a = re.search(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', block, re.S)
        if not a:
            continue
        href = _html.unescape(a[1])
        if "uddg=" in href:
            href = unquote(parse_qs(urlparse(href).query).get("uddg", [href])[0])
        sn = re.search(r'class="result__snippet"[^>]*>(.*?)</a>', block, re.S)
        out.append({"title": _html.unescape(clean_html(a[2])), "url": href,
                    "snippet": _html.unescape(clean_html(sn[1])) if sn else ""})
    return out


def clean_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").replace("&amp;", "&").strip()


def parse_profile(result: dict[str, str], company: str) -> dict[str, str] | None:
    """'Jane Doe - University Recruiter - Robinhood | LinkedIn' → name + headline."""
    url = result["url"]
    if not re.search(r"linkedin\.com/in/", url):
        return None
    title = clean_html(result["title"])
    snippet = clean_html(result["snippet"])
    text = f"{title} {snippet}"
    if company_key(company) not in company_key(text):
        return None  # profile doesn't mention this company
    title = re.sub(r"\s*\|\s*LinkedIn.*$", "", title)
    parts = [p.strip() for p in re.split(r"\s+[-–|]\s+", title) if p.strip()]
    if not parts:
        return None
    name = re.sub(r",.*$", "", parts[0]).strip()  # drop ", MBA" etc.
    if len(name.split()) < 2 or len(name) > 60:
        return None
    headline = " - ".join(parts[1:]) or snippet[:120]
    return {"name": name, "headline": headline, "linkedin_url": url.split("?")[0], "text": text}


def company_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", re.sub(r"\b(inc|llc|ltd|corp|corporation|co)\b", "",
                                            (text or "").lower()))


# --- email ------------------------------------------------------------------------

def company_domain(client: httpx.Client, company: str, job_link: str) -> str | None:
    host = (urlparse(job_link).hostname or "").lower()
    if host and not ATS_HOSTS.search(host):
        parts = host.split(".")
        return ".".join(parts[-2:])  # careers.amtrak.com → amtrak.com
    hunter = os.environ.get("HUNTER_API_KEY")
    if hunter:
        r = client.get("https://api.hunter.io/v2/domain-search",
                       params={"company": company, "api_key": hunter, "limit": 1})
        if r.status_code == 200 and (r.json().get("data") or {}).get("domain"):
            return r.json()["data"]["domain"]
    # Fallback: the first search result that looks like the company's own site.
    social = re.compile(r"(linkedin|wikipedia|facebook|twitter|x\.com|instagram|glassdoor|indeed|"
                        r"crunchbase|bloomberg|youtube|reddit|zoominfo)\.", re.I)
    for result in search(client, f'"{company}" official website', count=5):
        host = (urlparse(result["url"]).hostname or "").lower()
        if host and not social.search(host) and not ATS_HOSTS.search(host):
            return ".".join(host.split(".")[-2:])
    return None


_pattern_cache: dict[str, str | None] = {}


def email_pattern(client: httpx.Client, domain: str) -> str | None:
    """Hunter.io pattern like '{first}.{last}', or None."""
    if domain in _pattern_cache:
        return _pattern_cache[domain]
    pattern = None
    hunter = os.environ.get("HUNTER_API_KEY")
    if hunter:
        r = client.get("https://api.hunter.io/v2/domain-search",
                       params={"domain": domain, "api_key": hunter, "limit": 1})
        if r.status_code == 200:
            pattern = (r.json().get("data") or {}).get("pattern")
    _pattern_cache[domain] = pattern
    return pattern


def has_mail_server(client: httpx.Client, domain: str) -> bool:
    try:
        r = client.get("https://dns.google/resolve", params={"name": domain, "type": "MX"})
        return bool(r.json().get("Answer"))
    except (httpx.HTTPError, ValueError):
        return False


def make_email(name: str, domain: str, pattern: str | None) -> str:
    bits = [re.sub(r"[^a-z]", "", p) for p in name.lower().split()]
    first, last = bits[0], bits[-1]
    pattern = pattern or "{first}.{last}"
    return (pattern.replace("{first}", first).replace("{last}", last)
            .replace("{f}", first[:1]).replace("{l}", last[:1])) + "@" + domain


# --- main ---------------------------------------------------------------------------

def city_of(location: str) -> str:
    loc = re.sub(r"\(.*?\)", "", location or "")
    first = re.split(r"[,|/;]", loc)[0].strip()
    return "" if re.search(r"remote|united states|canada|usa|multiple", first, re.I) else first


def load_done() -> set[str]:
    if not CONTACTS_CSV.exists():
        return set()
    with CONTACTS_CSV.open(newline="", encoding="utf-8") as fh:
        return {r["job_link"] for r in csv.DictReader(fh)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--company", help="only internships at this company (substring match)")
    parser.add_argument("--location", help="only internships whose location contains this")
    parser.add_argument("--new", type=int, metavar="N",
                        help="the N newest internships not looked up yet")
    parser.add_argument("--recruiters", type=int, default=5, help="max recruiters per internship")
    parser.add_argument("--engineers", type=int, default=5, help="max engineers per internship")
    parser.add_argument("--all", action="store_true",
                        help="pick from all_open_internships.csv instead of the last-7-days internship CSV")
    parser.add_argument("--us", action="store_true",
                        help="pick from US_internships.csv instead of canada_internships.csv")
    parser.add_argument("--direct", action="store_true",
                        help="look up --company at --location even if it has no internship posted")
    args = parser.parse_args()
    if not (args.company or args.new):
        parser.error("pick internships with --company NAME and/or --new N")

    if args.direct:
        if not args.company:
            parser.error("--direct needs --company")
        jobs = [{"company": args.company, "title": "(no posting: direct lookup)",
                 "location": args.location or "", "link": f"direct:{args.company}:{args.location or ''}"}]
    else:
        src = (OUT_DIR / "all_open_internships.csv" if args.all
               else US_INTERNSHIPS_CSV if args.us else CA_INTERNSHIPS_CSV)
        with src.open(newline="", encoding="utf-8") as fh:
            jobs = list(csv.DictReader(fh))
    if args.company and not args.direct:
        jobs = [j for j in jobs if args.company.lower() in j["company"].lower()]
    if args.location and not args.direct:
        jobs = [j for j in jobs if args.location.lower() in j["location"].lower()]
    done = load_done()
    jobs = [j for j in jobs if j["link"] not in done]
    if args.new:
        jobs = jobs[: args.new]  # the internship CSVs are newest-first
    if not jobs:
        print("No matching internships left to look up.")
        return

    # One lookup per company+city: several internships at the same place share contacts.
    groups: dict[tuple[str, str], list[dict[str, str]]] = {}
    for j in jobs:
        groups.setdefault((j["company"], city_of(j["location"])), []).append(j)

    new_file = not CONTACTS_CSV.exists()
    total = 0
    with httpx.Client(timeout=20, follow_redirects=True) as client, \
            CONTACTS_CSV.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if new_file:
            writer.writeheader()
        for (company, city), group in groups.items():
            where = f'"{city}"' if city else ""
            quebec = bool(QUEBEC_RE.search(f"{city} {group[0]['location']}"))
            people: list[tuple[str, dict[str, str]]] = []
            seen_profiles: set[str] = set()
            for role_type, terms, role_re, limit in (
                ("recruiter", RECRUITER_TERMS + (RECRUITER_TERMS_FR if quebec else ""),
                 RECRUITER_RE, args.recruiters),
                ("engineer", ENGINEER_TERMS + (ENGINEER_TERMS_FR if quebec else ""),
                 ENGINEER_RE, args.engineers),
            ):
                found = 0
                query = f'site:linkedin.com/in "{company}" ({terms}) {where}'.strip()
                for page in range(MAX_PAGES):
                    if found >= limit:
                        break
                    results = search(client, query, page=page)
                    if not results:
                        break  # no more pages; don't spend a search on an empty one
                    for result in results:
                        p = parse_profile(result, company)
                        if p and p["linkedin_url"] not in seen_profiles and found < limit \
                                and role_re.search(p["headline"] or p["text"]):
                            # People in the job's city first; others only fill leftover slots.
                            p["in_city"] = bool(city and city.lower() in p["text"].lower())
                            seen_profiles.add(p["linkedin_url"])
                            people.append((role_type, p))
                            found += 1
                    if os.environ.get("BRAVE_API_KEY") or os.environ.get("GOOGLE_API_KEY"):
                        time.sleep(1.1)  # API free tiers; DuckDuckGo spaces itself out
            people.sort(key=lambda rp: (rp[0], not rp[1].get("in_city")))
            domain = company_domain(client, company, group[0]["link"])
            pattern = email_pattern(client, domain) if domain else None
            mx_ok = has_mail_server(client, domain) if domain else False
            stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            for job in group:
                for role_type, p in people:
                    email, conf = "", ""
                    if domain and mx_ok:
                        email = make_email(p["name"], domain, pattern)
                        conf = "pattern from Hunter" if pattern else "guess (first.last)"
                    writer.writerow({"found_at": stamp, "company": company, "job_title": job["title"],
                                     "job_location": job["location"], "job_link": job["link"],
                                     "role_type": role_type, "name": p["name"], "headline": p["headline"],
                                     "linkedin_url": p["linkedin_url"], "email": email,
                                     "email_confidence": conf})
                    total += 1
            fh.flush()
            n_rec = sum(1 for r, _ in people if r == "recruiter")
            print(f"{company} ({city or 'any location'}): {n_rec} recruiters, {len(people) - n_rec} engineers"
                  f"{', email domain ' + domain if domain else ', no email domain found'}")
    print(f"\nAdded {total} rows to {CONTACTS_CSV.relative_to(REPO)}")


if __name__ == "__main__":
    try:
        main()
    except httpx.HTTPStatusError as exc:
        sys.exit(f"Search/email API error: {exc.response.status_code} {exc.response.text[:200]}")
