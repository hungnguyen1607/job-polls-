#!/usr/bin/env python3
"""Small startups that may take interns without posting widely, from public directories.

Sources (all public JSON, no login):
  yc   YC company directory (yc-oss mirror): active, hiring, <= 200 people, US / Canada / remote
  getro  Canadian VC / university portfolio boards on Getro (Inovia, Real Ventures, Georgian,
         Portage, U of T Entrepreneurship, Ironspring, Techstars): every portfolio company located
         in Canada, with head count and stage
  clera  Clera's public startup map (getclera.com/map): every startup it lists in Canadian cities,
         with city, one-liner, funding and founding year (honest user-agent, 1 page/second)
  dmz  DMZ's "Oh Canada" directory of Canadian-built tech companies
  latka  GetLatka's SaaS company list (its 3 public sitemap files only): .ca companies, and
         websites for companies stuck in the web-search queue whose name matches a domain
  a16z   a16z's portfolio board (jobs.a16z.com/companies, one page): US / Canada companies
  startupmap  StartupMap.ca: every Canadian startup in its sitemap (profile JSON-LD, 1 page/second,
         only new profiles fetched)
  accelerators  Montreal / Toronto / Ottawa accelerators: Centech, FounderFuel, District 3,
         CDL Toronto + Montreal, L-SPARK, Femtech Canada (profile pages fetched once, 1/second)
  hn   Hacker News "Ask HN: Who is hiring?" threads (last 2 months) via the Algolia API:
       posts that mention intern/co-op/new grad, or Canada/Toronto/Montreal/Vancouver/remote

Every company is stored in the `startup_sources` table (size, stage, website, where, note) and,
when no list already polls it, inserted into `discovered` as status 'new' so the discovery pass
(internship_core.resolve_discovered) finds its ATS board and the pollers watch it for internships.

    .venv/bin/python scripts/startup_sources.py              # fetch all sources
    .venv/bin/python scripts/startup_sources.py --place 50   # also place 50 new companies now
"""
from __future__ import annotations

import argparse
import asyncio
import html
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

YC_URL = "https://yc-oss.github.io/api/companies/all.json"
GETRO_API = "https://api.getro.com/api/v2/collections/{}/search/companies"
# collection id -> (network, text query to narrow huge global networks, keep only Canadian locations?)
GETRO_NETWORKS = {
    1201: ("Inovia", "", True), 166: ("Real Ventures", "", True), 14282: ("Georgian", "", True),
    144: ("Portage", "", True), 1187: ("U of T Entrepreneurship", "", False),
    3755: ("Ironspring", "", True), 89: ("Techstars", "Canada", True),
    # US VCs: keep portfolio companies in the US or Canada ("na").
    246: ("Insight Partners", "", "na"), 222: ("General Catalyst", "", "na"), 8672: ("Accel", "", "na"),
    2105: ("Thrive Capital", "", "na"), 764: ("GGV Capital", "", "na"),
}
NA_RE = re.compile(r"united states|\busa?\b|canada|, ?(CA|NY|MA|WA|TX|IL|CO|GA|FL|NC|PA|OR|UT|VA|DC)\b|"
                   r"san francisco|new york|boston|seattle|austin|los angeles|chicago|toronto|montr[eé]al|vancouver",
                   re.I)
A16Z_URL = "https://jobs.a16z.com/companies"
STARTUPMAP = "https://startupmap.ca"
GETRO_MAX_PAGES = 400  # 12 companies a page
HN_SEARCH = "https://hn.algolia.com/api/v1/search_by_date?tags=story,author_whoishiring&hitsPerPage=8"
HN_ITEM = "https://hn.algolia.com/api/v1/items/{}"
YC_MAX_TEAM = 200
YC_REGIONS = {"America / Canada", "United States of America", "Canada", "Remote", "Fully Remote"}
HN_WHERE = re.compile(r"\b(canada|toronto|montr[eé]al|vancouver|waterloo|ottawa|calgary|remote)\b", re.I)
HN_INTERN = re.compile(r"\bintern(s|ship)?\b|\bco-?op\b|new grad|university grad|\bstudents?\b", re.I)
HN_SOFTWARE = re.compile(r"engineer|developer|software|full[- ]?stack|backend|frontend|swe\b", re.I)
HN_VISA = re.compile(r"\bvisa\b|sponsor", re.I)
CANADA_RE = re.compile(r"canada|toronto|montr[eé]al|vancouver|waterloo|ottawa|calgary|\b(ON|QC|BC|AB)\b")


def _table(db) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS startup_sources (name_key TEXT PRIMARY KEY, name TEXT,
        via TEXT, website TEXT, linkedin TEXT, team_size TEXT, stage TEXT, location TEXT, note TEXT,
        first_seen TEXT, last_seen TEXT)""")


def _size_bucket(n) -> str:
    """YC gives a head count; store it in the same buckets simplify.jobs uses."""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return ""
    for top, label in ((10, "1-10"), (50, "11-50"), (200, "51-200"), (500, "201-500"), (1000, "501-1,000"),
                       (5000, "1,001-5,000"), (10000, "5,001-10,000")):
        if n <= top:
            return label
    return "10,001+"


def fetch_yc(client: httpx.Client) -> list[dict]:
    out = []
    for c in client.get(YC_URL, timeout=120).json():
        regions = set(c.get("regions") or [])
        if not (c.get("isHiring") and c.get("status") == "Active" and regions & YC_REGIONS):
            continue
        if (c.get("team_size") or 0) > YC_MAX_TEAM:
            continue
        loc = c.get("all_locations") or ""
        out.append({"name": c["name"].strip(), "via": "yc", "website": c.get("website") or "",
                    "linkedin": "", "team_size": _size_bucket(c.get("team_size")),
                    "stage": f"YC {c.get('batch') or ''} ({c.get('stage') or ''})".strip(),
                    "location": loc, "note": (c.get("one_liner") or "")[:200]})
    return out


def fetch_getro(client: httpx.Client) -> list[dict]:
    import time
    out = []
    for cid, (network, query, ca_only) in GETRO_NETWORKS.items():
        seen = 0
        for page in range(GETRO_MAX_PAGES):
            r = client.post(GETRO_API.format(cid), timeout=60, headers={"Accept": "application/json"},
                            json={"hitsPerPage": 12, "page": page, "filters": {}, "query": query})
            r.raise_for_status()
            res = r.json().get("results") or {}
            comps = res.get("companies") or []
            for c in comps:
                locs = " | ".join(c.get("locations") or [])
                if ca_only == "na":
                    if locs and not NA_RE.search(locs):
                        continue
                elif ca_only and not CANADA_RE.search(locs):
                    continue
                out.append({"name": (c.get("name") or "").strip(), "via": "getro",
                            "website": f"https://{c['domain']}" if c.get("domain") else "", "linkedin": "",
                            "team_size": _size_bucket(c.get("head_count")),
                            "stage": f"{network} portfolio" + (f" ({c['stage']})" if c.get("stage") not in (None, "other") else ""),
                            "location": locs[:160],
                            "note": f"{network}; {c.get('active_jobs_count') or 0} open jobs; "
                                    f"{(c.get('description') or '')[:140]}"})
            seen += len(comps)
            if not comps or seen >= (res.get("count") or 0):
                break
            time.sleep(0.4)
    return [r for r in out if r["name"]]


CLERA_CITY = "https://www.getclera.com/map/city/{}/all?page={}"
CLERA_CITIES = ("toronto", "montreal", "ottawa", "waterloo", "vancouver", "calgary", "kitchener",
                "quebec-city", "edmonton")
CLERA_UA = "ats-scrapers/startup-sources (personal internship tracker)"
CLERA_MAX_PAGES = 40


def _clera_blob(text: str) -> str:
    import json
    return "".join(json.loads('"' + c + '"')
                   for c in re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', text, re.S))


def fetch_clera(client: httpx.Client) -> list[dict]:
    import time
    out: dict[str, dict] = {}
    for city in CLERA_CITIES:
        page, last = 1, 1
        while page <= min(last, CLERA_MAX_PAGES):
            url = CLERA_CITY.format(city, page) if page > 1 else CLERA_CITY.split("?")[0].format(city)
            r = client.get(url, timeout=60, headers={"User-Agent": CLERA_UA})  # ?page=1 renders empty
            if r.status_code == 404:
                break
            r.raise_for_status()
            blob = _clera_blob(r.text)
            m = re.search(r"[Pp]age \d+ of (\d+)", blob)
            last = int(m.group(1)) if m else page
            got = 0
            for part in blob.split('["$","li","')[1:]:
                slug = part.split('"', 1)[0]
                if f'"/companies/{slug}"' not in part[:400]:
                    continue
                name = re.search(r'\{"name":"((?:[^"\\]|\\.)*)","logoUrl"', part)
                if not name:
                    continue
                texts = re.findall(r'"className":"[^"]*","children":"((?:[^"\\]|\\.)*)"', part)
                tags = [a.lstrip("$") if a else f"est. {b}" for a, b in
                        re.findall(r'"children":(?:"(\$\$[^"]+)"|\["Est\. ",(\d+)\])', part)]
                industry = texts[3] if len(texts) > 3 else ""
                got += 1
                out.setdefault(slug, {"name": name.group(1), "via": "clera",
                                      "website": f"https://www.getclera.com/companies/{slug}", "linkedin": "",
                                      "team_size": "", "stage": ", ".join(t for t in tags if t.startswith("$")),
                                      "location": f"{texts[1] if len(texts) > 1 else ''} (Clera {city})",
                                      "note": f"{(texts[2] if len(texts) > 2 else '')[:150]} "
                                              f"[{', '.join([industry, *tags]).strip(', ')}]"})
            if not got and page == 1:
                break  # unknown city or the page changed: nothing to parse
            page += 1
            time.sleep(1.0)
    return list(out.values())


DMZ_URL = "https://dmzlegacy.torontomu.ca/oh-canadas-tech-directory"
_DMZ_CARD = re.compile(r'<a[^>]*href="(https?://[^"]+)"[^>]*>\s*<div class="card-defaultProfile.*?<h4>(.*?)</h4>'
                       r'(.*?)</p>', re.S)


def fetch_dmz(client: httpx.Client) -> list[dict]:
    r = client.get(DMZ_URL, timeout=60, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/130"})
    r.raise_for_status()
    out = {}
    for url, name, rest in _DMZ_CARD.findall(r.text):
        name = html.unescape(re.sub(r"<[^>]+>", "", name)).strip()
        desc = html.unescape(re.sub(r"<[^>]+>", " ", rest)).strip()
        if name and name.lower() not in out:
            out[name.lower()] = {"name": name, "via": "dmz", "website": url, "linkedin": "", "team_size": "",
                                 "stage": "DMZ Oh Canada directory", "location": "Canada",
                                 "note": re.sub(r"\s+", " ", desc)[:200]}
    return list(out.values())


def _hn_company(text: str) -> tuple[str, str]:
    """'Acme (YC W24) | Senior Engineer | Remote | https://acme.dev' -> ('Acme', 'https://acme.dev')."""
    first = re.split(r"\s*\|\s*", text, maxsplit=1)[0]
    name = re.sub(r"\s*\(.*?\)\s*", " ", first).strip(" -–:")
    url = re.search(r"https?://[^\s|<>\"]+", text)
    return (name if 1 < len(name) <= 60 and len(name.split()) <= 6 else ""), (url.group(0) if url else "")


def fetch_hn(client: httpx.Client, months: int = 2) -> list[dict]:
    stories = [h for h in client.get(HN_SEARCH, timeout=60).json().get("hits", [])
               if "who is hiring" in (h.get("title") or "").lower()][:months]
    out = []
    for story in stories:
        item = client.get(HN_ITEM.format(story["objectID"]), timeout=120).json()
        for child in item.get("children") or []:
            raw = child.get("text") or ""
            text = html.unescape(re.sub(r"<[^>]+>", " ", raw.replace("<p>", "\n")))
            head = text.split("\n", 1)[0]
            intern = bool(HN_INTERN.search(text))
            if not HN_SOFTWARE.search(text) or not (intern or HN_WHERE.search(head)):
                continue
            name, url = _hn_company(head)
            if not name:
                continue
            tags = [t for t, ok in (("intern", intern), ("visa", HN_VISA.search(text)),
                                    ("canada", CANADA_RE.search(head))) if ok]
            out.append({"name": name, "via": "hn", "website": url, "linkedin": "", "team_size": "",
                        "stage": "", "location": head[:160],
                        "note": f"[{', '.join(tags)}] HN {story.get('title', '')[-15:-1]}: "
                                f"https://news.ycombinator.com/item?id={child.get('id')}"})
    return out


def fetch_a16z(client: httpx.Client) -> list[dict]:
    """a16z's portfolio board (its own site, not Getro): the companies page embeds every company
    (name, domain, location, head count, stage). One request; keeps US / Canada."""
    import json
    r = client.get(A16Z_URL, timeout=60, headers={"User-Agent": CLERA_UA})
    r.raise_for_status()
    blob = _clera_blob(r.text)
    out: dict[str, dict] = {}
    for m in re.finditer(r'\{"id":"[a-z0-9]+","slug":"[^"]+","name":', blob):
        depth, i = 0, m.start()
        for j in range(i, min(len(blob), i + 6000)):
            depth += {"{": 1, "}": -1}.get(blob[j], 0)
            if depth == 0:
                break
        try:
            c = json.loads(blob[i:j + 1])
        except ValueError:
            continue
        loc = c.get("location") or ""
        if loc and not NA_RE.search(loc):
            continue
        name = (c.get("name") or "").strip()
        if name and name not in out:
            out[name] = {"name": name, "via": "a16z", "website": f"https://{c['domain']}" if c.get("domain") else "",
                         "linkedin": "", "team_size": _size_bucket(c.get("headcount")),
                         "stage": "a16z portfolio" + (f" ({c['stage']})" if c.get("stage") else ""),
                         "location": loc[:160],
                         "note": f"a16z; {c.get('jobCount') or 0} open jobs; {(c.get('description') or '')[:140]}"}
    return list(out.values())


STARTUPMAP_PER_RUN = 3000  # new profile pages fetched per run, 1 a second


def fetch_startupmap(client: httpx.Client) -> list[dict]:
    """StartupMap.ca: every startup in its public sitemap; each profile page's JSON-LD gives the
    name, website and city. Only profiles not fetched before are requested (honest user-agent,
    1 page a second); earlier ones come from the cache table."""
    import json
    db = core.connect()
    db.execute("""CREATE TABLE IF NOT EXISTS startupmap_pages (slug TEXT PRIMARY KEY, name TEXT, website TEXT,
                  city TEXT, region TEXT, description TEXT, fetched_at TEXT)""")
    sm = client.get(f"{STARTUPMAP}/sitemap.xml", timeout=60, headers={"User-Agent": CLERA_UA})
    sm.raise_for_status()
    slugs = re.findall(r"<loc>https://startupmap\.ca/startup/([^<]+)</loc>", sm.text)
    have = {r[0] for r in db.execute("SELECT slug FROM startupmap_pages")}
    todo = [x for x in slugs if x not in have][:STARTUPMAP_PER_RUN]
    print(f"startupmap: {len(slugs)} listed, fetching {len(todo)} new profiles")
    for n, slug in enumerate(todo, 1):
        try:
            r = client.get(f"{STARTUPMAP}/startup/{slug}", timeout=30, headers={"User-Agent": CLERA_UA})
        except httpx.HTTPError:
            time.sleep(1)
            continue
        if r.status_code in (403, 429):
            print(f"startupmap: HTTP {r.status_code}, stopping; the rest is fetched next run")
            break
        org = {}
        for m in re.finditer(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', r.text, re.S):
            try:
                d = json.loads(m.group(1))
            except ValueError:
                continue
            if d.get("@type") == "Organization":
                org = d
                break
        addr = org.get("address") or {}
        with db:
            db.execute("INSERT OR REPLACE INTO startupmap_pages VALUES (?,?,?,?,?,?,?)",
                       (slug, (org.get("name") or "").strip(), org.get("url") or "", addr.get("addressLocality") or "",
                        addr.get("addressRegion") or "", (org.get("description") or "")[:200],
                        datetime.now(UTC).isoformat(timespec="seconds")))
        if n % 200 == 0:
            print(f"startupmap: {n}/{len(todo)}")
        time.sleep(1)
    rows = [{"name": name, "via": "startupmap", "website": site or "", "linkedin": "", "team_size": "",
             "stage": "", "location": ", ".join(x for x in (city, region, "Canada") if x), "note": desc or ""}
            for name, site, city, region, desc in
            db.execute("SELECT name, website, city, region, description FROM startupmap_pages WHERE name <> ''")]
    db.close()
    return rows


# Accelerators / incubators in Montreal, Toronto and Ottawa: their portfolio companies.
# "profiles": a sitemap of company profile pages; each page links out to the company's website.
# "list": one page (paginated) that links straight to company websites.
ACCELERATORS = [
    {"name": "Centech", "city": "Montreal, QC", "kind": "profiles", "url": "https://centech.co/startups-sitemap.xml",
     "skip": r"etsmtl|centech"},
    {"name": "FounderFuel", "city": "Montreal, QC", "kind": "profiles", "url": "https://founderfuel.com/company-sitemap.xml",
     "skip": r"founderfuel|realventures"},
    {"name": "Femtech Canada", "city": "Canada", "kind": "profiles", "url": "https://femtech.ca/company-sitemap.xml",
     "skip": r"femtech|innovationfactory|cognitoforms"},
    {"name": "Creative Destruction Lab", "city": "", "kind": "profiles",
     "url": ["https://creativedestructionlab.com/companies-sitemap.xml",
             "https://creativedestructionlab.com/companies-sitemap2.xml"],
     "skip": r"creativedestructionlab|rotman|sauder|haskayne|ucalgary|utoronto|ubc\.ca|mcgill|hec\.ca|oxford|"
             r"wisc|gatech|stern|nyu|bocconi|hec\.edu|ie\.edu|insead|unimelb|uq\.edu|iisc|isb",
     "site_only": r"Site:\s*\|?\s*CDL-(Toronto|Montreal)"},
    {"name": "District 3", "city": "Montreal, QC", "kind": "list", "url": "https://www.district3.co/startups",
     "page_param": "37e06a8e_page", "skip": r"district3|website-files|mentorconnect|goo\.gl"},
    {"name": "L-SPARK", "city": "Ottawa, ON", "kind": "list", "url": "https://www.l-spark.com/meet-our-companies/",
     "skip": r"l-spark"},
]
_ACCEL_SKIP = (r"facebook|twitter|x\.com|linkedin|instagram|youtube|google|apple\.com|tiktok|vimeo|wordpress|"
               r"gravatar|w3\.org|schema\.org|cloudflare|hubspot|jsdelivr|gstatic|wp\.com|yoast|elementor|"
               r"cookie|gmpg\.org|calendly|typeform|mailchimp|eventbrite|medium\.com|github\.com|wix\.com")


def _accel_website(html_text: str, skip: str) -> str:
    for h in dict.fromkeys(re.findall(r'href="(https?://[^"]+)"', html_text)):
        if not re.search(_ACCEL_SKIP, h, re.I) and not re.search(skip, h, re.I):
            return h
    return ""


def fetch_accelerators(client: httpx.Client) -> list[dict]:
    """Portfolio companies of Montreal / Toronto / Ottawa accelerators (Centech, FounderFuel, District 3,
    CDL Toronto + Montreal, L-SPARK, Femtech Canada). Profile pages are fetched once (cached in
    accel_pages), 1 a second, honest user-agent."""
    db = core.connect()
    db.execute("""CREATE TABLE IF NOT EXISTS accel_pages (url TEXT PRIMARY KEY, accelerator TEXT, name TEXT,
                  website TEXT, city TEXT, keep INTEGER, fetched_at TEXT)""")
    hdr = {"User-Agent": CLERA_UA}
    for acc in ACCELERATORS:
        try:
            if acc["kind"] == "list":
                page, seen_any = 1, True
                while seen_any and page <= 30:
                    url = acc["url"] + (f"?{acc['page_param']}={page}" if page > 1 and acc.get("page_param") else "")
                    r = client.get(url, headers=hdr, timeout=30)
                    if r.status_code != 200:
                        break
                    seen_any = False
                    for h in dict.fromkeys(re.findall(r'href="(https?://[^"]+)"', r.text)):
                        if re.search(_ACCEL_SKIP, h, re.I) or re.search(acc["skip"], h, re.I):
                            continue
                        host = urlparse_host(h)
                        if not host:
                            continue
                        name = host.split(".")[0].replace("-", " ").title()
                        cur = db.execute("INSERT OR IGNORE INTO accel_pages VALUES (?,?,?,?,?,1,?)",
                                         (f"{acc['name']}:{host}", acc["name"], name, h, acc["city"],
                                          datetime.now(UTC).isoformat(timespec="seconds")))
                        seen_any = seen_any or cur.rowcount > 0
                    db.commit()
                    if not acc.get("page_param"):
                        break
                    page += 1
                    time.sleep(1)
                continue
            sitemaps = acc["url"] if isinstance(acc["url"], list) else [acc["url"]]
            pages: list[str] = []
            for sm in sitemaps:
                pages += re.findall(r"<loc>([^<]+)</loc>", client.get(sm, headers=hdr, timeout=60).text)
            have = {r[0] for r in db.execute("SELECT url FROM accel_pages WHERE accelerator=?", (acc["name"],))}
            todo = [u for u in pages if u not in have and u.rstrip("/").count("/") >= 4]
            print(f"accelerators: {acc['name']}: {len(pages)} profiles, fetching {len(todo)} new")
            for u in todo:
                try:
                    r = client.get(u, headers=hdr, timeout=30)
                except httpx.HTTPError:
                    continue
                if r.status_code in (403, 429):
                    print(f"accelerators: {acc['name']} HTTP {r.status_code}, stopping; resumes next run")
                    break
                if r.status_code != 200:
                    continue
                text = r.text
                m = re.search(r'<meta property="og:title" content="([^"]+)"', text) or re.search(r"<h1[^>]*>(.*?)</h1>", text, re.S)
                name = html.unescape(re.sub(r"<[^>]+>", "", m.group(1))).split(" - ")[0].split(" | ")[0].split(" – ")[0].strip() if m else ""
                plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " | ", text))
                keep = 1
                city = acc["city"]
                if acc.get("site_only"):
                    sm_ = re.search(acc["site_only"], plain)
                    keep = 1 if sm_ else 0
                    city = {"Toronto": "Toronto, ON", "Montreal": "Montreal, QC"}.get(sm_.group(1), "") if sm_ else ""
                db.execute("INSERT OR REPLACE INTO accel_pages VALUES (?,?,?,?,?,?,?)",
                           (u, acc["name"], name, _accel_website(text, acc["skip"]), city, keep,
                            datetime.now(UTC).isoformat(timespec="seconds")))
                db.commit()
                time.sleep(1)
        except Exception as exc:  # noqa: BLE001 — one accelerator failing never stops the others
            print(f"accelerators: {acc['name']} failed ({type(exc).__name__}: {exc})")
    rows = [{"name": name, "via": "accelerator", "website": site or "", "linkedin": "", "team_size": "",
             "stage": f"{acc} portfolio", "location": ", ".join(x for x in (city, "Canada") if x),
             "note": f"{acc} accelerator/incubator company"}
            for acc, name, site, city in db.execute(
                "SELECT accelerator, name, website, city FROM accel_pages WHERE keep=1 AND name <> ''")]
    db.close()
    return rows


def urlparse_host(url: str) -> str:
    from urllib.parse import urlparse
    host = urlparse(url).netloc.lower().removeprefix("www.")
    return host if "." in host else ""


LATKA_SITEMAPS = [f"https://getlatka.com/sitemaps/companies-{i}.xml" for i in range(3)]
LATKA_DOMAIN = re.compile(r"[a-z0-9-]+(\.[a-z0-9-]+)+")


def _compact(name: str) -> str:
    """'Episode Six, Inc.' -> 'episodesix'. Only legal suffixes are dropped (not 'Group' or
    'Technologies'), so 'Chronos Group' never matches chronos.ai."""
    words = re.findall(r"[a-z0-9]+", (name or "").lower())
    while words and words[-1] in {"inc", "ltd", "llc", "corp", "corporation", "limited", "co"}:
        words.pop()
    return "".join(words)


# Domains a US / Canadian company would use; skips look-alikes such as rona.at for RONA.
LATKA_TLDS = {"com", "ca", "ai", "io", "co", "app", "dev", "tech", "so", "xyz", "us", "net", "org", "inc"}


def fetch_latka(client: httpx.Client) -> list[dict]:
    """GetLatka's SaaS company list, from its 3 public sitemap files only (3 requests; no company
    pages). Most slugs are the company's domain, which is all we need: the website pipeline then
    finds its careers page. Returns the Canadian (.ca) companies as new rows, and gives websites to
    companies stuck in the web-search queue whose name matches a domain."""
    db = core.connect()
    db.execute("CREATE TABLE IF NOT EXISTS latka_companies (slug TEXT PRIMARY KEY, domain TEXT, label TEXT)")
    slugs: list[str] = []
    for url in LATKA_SITEMAPS:
        r = client.get(url, headers={"User-Agent": CLERA_UA}, timeout=60)
        r.raise_for_status()
        slugs += re.findall(r"<loc>https://getlatka\.com/companies/([^<]+)</loc>", r.text)
        time.sleep(1)
    rows = []
    for slug in slugs:
        dom = slug.removeprefix("app.").removeprefix("www.") if LATKA_DOMAIN.fullmatch(slug) else ""
        label = (dom.split(".")[0] if dom else slug).replace("-", "")
        rows.append((slug, dom, label))
    with db:
        db.executemany("INSERT OR REPLACE INTO latka_companies VALUES (?,?,?)", rows)
    by_label: dict[str, set[str]] = {}
    for _, dom, label in rows:
        if dom and dom.rsplit(".", 1)[1] in LATKA_TLDS:
            by_label.setdefault(label, set()).add(dom)
    out: list[dict] = []
    # 1. Companies stuck waiting for a web search: a unique domain whose label is their name.
    stuck = db.execute("""SELECT d.name, MAX(s.website) FROM discovered d LEFT JOIN startup_sources s
                          ON s.name_key=d.name_key WHERE d.status='searching' GROUP BY d.name_key""").fetchall()
    matched = 0
    for name, site in stuck:
        if core.clean_website(site):
            continue  # already has a real website
        doms = by_label.get(_compact(name) or "-") or set()
        if len(doms) == 1 and len(_compact(name)) >= 4:
            out.append({"name": name, "via": "latka", "website": "https://" + next(iter(doms)), "linkedin": "",
                        "team_size": "", "stage": "", "location": "", "note": "website from GetLatka"})
            matched += 1
    # 2. Canadian SaaS companies (.ca domain) as new startups.
    ca = 0
    for _, dom, label in rows:
        if dom.endswith(".ca"):
            out.append({"name": dom.split(".")[0].replace("-", " ").title(), "via": "latka",
                        "website": "https://" + dom, "linkedin": "", "team_size": "", "stage": "",
                        "location": "Canada", "note": "GetLatka SaaS (.ca)"})
            ca += 1
    db.close()
    print(f"latka: {len(slugs)} companies listed, {matched} stuck companies matched to a website, {ca} .ca companies")
    return out


def save(rows: list[dict]) -> tuple[int, int]:
    """Upsert into startup_sources; queue companies no list polls yet into discovered. -> (stored, queued)."""
    db = core.connect()
    _table(db)
    core._discover_tables(db)
    known = core._known_companies()
    now = datetime.now(UTC).isoformat(timespec="seconds")
    queued = 0
    with db:
        for r in rows:
            key = core.norm_text(r["name"])
            if not key:
                continue
            cur = db.execute("""UPDATE startup_sources SET last_seen=?, website=COALESCE(NULLIF(?, ''), website),
                team_size=COALESCE(NULLIF(?, ''), team_size), stage=COALESCE(NULLIF(?, ''), stage),
                location=COALESCE(NULLIF(?, ''), location), note=? WHERE name_key=?""",
                             (now, r["website"], r["team_size"], r["stage"], r["location"], r["note"], key))
            if cur.rowcount == 0:
                db.execute("INSERT INTO startup_sources VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                           (key, r["name"], r["via"], r["website"], r["linkedin"], r["team_size"],
                            r["stage"], r["location"], r["note"], now, now))
            if key in known:
                continue
            if db.execute("UPDATE discovered SET last_seen=? WHERE name_key=?", (now, key)).rowcount == 0:
                db.execute("INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                           (key, r["name"], "", r["via"], now, now, "new", None, None, None, None))
                queued += 1
    db.close()
    return len(rows), queued


async def place(n: int) -> dict[str, int]:
    """Find boards for up to n queued startup-source companies right away (same logic as discovery)."""
    db = core.connect()
    core._discover_tables(db)
    core._careers_cache(db)
    core._search_queue(db)
    todo = db.execute("""SELECT name_key, name, li_slug, source FROM discovered
                         WHERE status='new' AND source IN ('yc', 'hn', 'getro', 'dmz', 'clera', 'pey', 'waterlooworks', 'latka', 'a16z', 'startupmap', 'accelerator') ORDER BY first_seen LIMIT ?""",
                      (n,)).fetchall()
    known = core._known_companies()
    counts: dict[str, int] = {}
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient(timeout=30, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/128"}) as client:
        async def one(row):
            async with sem:
                try:
                    res = await core.resolve_discovered(db, client, known, row[0], row[1], row[2] or "", row[3])
                except Exception as exc:  # noqa: BLE001 — one company never stops the batch
                    core.log_line(f"{'startup_sources':16} {row[1]}: {exc}")
                    res = "error"
                counts[res] = counts.get(res, 0) + 1
        await asyncio.gather(*(one(r) for r in todo))
    db.close()
    return counts


async def retry_searching(n: int) -> dict[str, int]:
    """Companies stuck waiting for a web search that a source gave a website for: find their board
    or careers page from that website instead (no search engine needed)."""
    db = core.connect()
    core._discover_tables(db)
    core._careers_cache(db)
    core._search_queue(db)
    todo = db.execute("""SELECT d.name_key, d.name, d.li_slug, d.source, MAX(s.website) FROM discovered d
                         JOIN startup_sources s ON s.name_key = d.name_key
                         WHERE d.status='searching' AND s.website <> ''
                         GROUP BY d.name_key ORDER BY d.checked_at""").fetchall()
    todo = [(*r[:4], core.clean_website(r[4])) for r in todo]
    todo = [r for r in todo if r[4]][:n]
    counts: dict[str, int] = {}
    sem = asyncio.Semaphore(8)
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        async def one(row):
            async with sem:
                res = await core.place_from_website(db, client, row[0], row[1], row[2] or "", "website", row[4])
                if not res:
                    stamp = core.datetime.now(core.UTC).isoformat(timespec="seconds")
                    with db:
                        db.execute("UPDATE discovered SET checked_at=?, url=COALESCE(NULLIF(url,''), ?) "
                                   "WHERE name_key=?", (stamp, row[4], row[0]))
                    res = "still searching"
                counts[res] = counts.get(res, 0) + 1
        await asyncio.gather(*(one(r) for r in todo))
    db.close()
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", choices=("yc", "hn", "getro", "dmz", "clera", "latka", "a16z", "startupmap", "accelerators"), help="fetch one source")
    ap.add_argument("--place", type=int, default=0, metavar="N", help="also place N queued companies now")
    ap.add_argument("--retry-searching", type=int, default=0, metavar="N",
                    help="only: place N companies stuck in the web-search queue from their own website")
    args = ap.parse_args()
    if args.retry_searching:
        print("website retry:", asyncio.run(retry_searching(args.retry_searching)))
        return
    rows: list[dict] = []
    with httpx.Client(follow_redirects=True, headers={"User-Agent": "ats-scrapers/startup-sources"}) as client:
        for name, fn in (("yc", fetch_yc), ("hn", fetch_hn), ("getro", fetch_getro), ("dmz", fetch_dmz), ("clera", fetch_clera),
                         ("latka", fetch_latka), ("a16z", fetch_a16z), ("startupmap", fetch_startupmap),
                         ("accelerators", fetch_accelerators)):
            if args.only and args.only != name:
                continue
            try:
                got = fn(client)
                print(f"{name}: {len(got)} companies")
                rows += got
            except Exception as exc:  # noqa: BLE001 — one source failing never stops the others
                print(f"{name}: failed ({type(exc).__name__}: {exc})")
                core.log_line(f"{'startup_sources':16} {name}: {exc}")
    stored, queued = save(rows)
    print(f"stored {stored}, queued {queued} new companies for board discovery")
    if args.place:
        print("placed:", asyncio.run(place(args.place)))


if __name__ == "__main__":
    main()
