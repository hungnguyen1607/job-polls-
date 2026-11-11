#!/usr/bin/env python3
"""US intern outreach list — separate from the Canada winter co-op list.

Every company our pollers have seen hire a software intern in the US (Simplify,
Greenhouse, Workday, LinkedIn, …). Summer 2027 stays on this list — that's the
US intern cycle.

Every company crowd intern lists or our pollers have seen hire a US software
intern (open or closed). J-1 High/Med is a small extra overlay, not a filter.

    .venv/bin/python scripts/us_sponsor_outreach.py
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

# High = official careers/FAQ or posting sentence. Med = official but silent, or
# consistent student reports. Do not invent policy; notes paraphrase the research.
# Excluded on purpose: Intel (no J-1 undergrad), JPMorgan (no-sponsor intern
# programs), Airbnb (tracker no-sponsor), Bitrig (US university only), Snowflake
# (aggregator: J-1 no), Amazon ADC / clearance reqs, Google EMEA intern, Meta /
# NVIDIA / Two Sigma / Datadog / Goldman (Low or CPT-shaped only).
COMPANIES: list[dict[str, object]] = [
    {
        "company": "Microsoft",
        "aliases": ("microsoft",),
        "confidence": "High",
        "visa_note": "Official university FAQ: visa sponsorship yes. US eligibility does not require preexisting work rights. Must return to school 1 term after (May 2028 → Summer 2027). Confirm J-1 Intern / DS-2019.",
        "careers_page": "https://careers.microsoft.com/v2/global/en/universityinternship",
        "linkedin": "https://www.linkedin.com/company/microsoft/",
        "skip_title": r"cleared|\bctj\b|secret clearance|ts/?sci",
    },
    {
        "company": "Palantir",
        "aliases": ("palantir",),
        "confidence": "High",
        "visa_note": "Official early-talent FAQ: visa sponsorship yes. J-1 intern hosting is in writing on country-specific pipelines. 2028-grad / final-internship rule maps to Summer 2027. Confirm Concordia is in the general US intern pipeline.",
        "careers_page": "https://www.palantir.com/careers/students-and-early-talent/",
        "linkedin": "https://www.linkedin.com/company/palantir-technologies/",
    },
    {
        "company": "SIG (Susquehanna)",
        "aliases": ("susquehanna", "susquehannainternationalgroup"),
        "confidence": "High",
        "visa_note": "2027 intern posting: “Visa sponsorship is available for this position.” Confirm the SWE/engineering intern has the same sentence, and that it is J-1 (not only H-1B).",
        "careers_page": "https://careers.sig.com/",
        "linkedin": "https://www.linkedin.com/company/susquehanna-international-group-llp/",
    },
    {
        "company": "Jane Street",
        "aliases": ("janestreet",),
        "confidence": "High",
        "visa_note": "Official: internationals considered; visa sponsorship changes by office and role. 2025 intern class from 24 countries. Ask NY specifically for J-1 Intern.",
        "careers_page": "https://www.janestreet.com/join-jane-street/internships/",
        "linkedin": "https://www.linkedin.com/company/jane-street/",
        "contact": "jobs@janestreet.com",
    },
    {
        "company": "Google",
        "aliases": ("google",),
        "confidence": "Med",
        "visa_note": "US SWE intern posting requires being in the US and does not ban immigration sponsorship (EMEA intern postings do). Confirm J-1 in writing. Apply to the US req, not EMEA. Prefer BS intern over PhD.",
        "careers_page": "https://www.google.com/about/careers/applications/jobs/results/?q=%22Software%20Engineering%20Intern%22",
        "linkedin": "https://www.linkedin.com/company/google/",
        "exact": True,
    },
    {
        "company": "Amazon",
        "aliases": ("amazon",),
        "confidence": "Med",
        "visa_note": "General SDE intern is visa-silent — apply that req only. Skip ADC (US citizen + TS/SCI) and intern reqs that refuse sponsorship. Confirm J-1 in writing.",
        "careers_page": "https://www.amazon.jobs/en/jobs/10552937/software-development-engineer-intern-summer-2027-usa",
        "linkedin": "https://www.linkedin.com/company/amazon/",
        "skip_company": ("dedicatedcloud", "developmentcenter", "developmentcentre"),
        "skip_title": r"\badc\b|dedicated cloud|ts/?sci|clearance|us citizen",
    },
    {
        "company": "Stripe",
        "aliases": ("stripe",),
        "confidence": "Med",
        "visa_note": "Intern application asks if you need visa sponsorship now or later; it does not refuse. Confirm J-1 vs F-1 CPT.",
        "careers_page": "https://stripe.com/careers/emerging-talent",
        "linkedin": "https://www.linkedin.com/company/stripe/",
    },
    {
        "company": "Hudson River Trading",
        "aliases": ("hudsonrivertrading",),
        "confidence": "Med",
        "visa_note": "Campus recruiter offered visa sponsorship for Singapore internships. US intern J-1 is not published — apply and ask.",
        "careers_page": "https://www.hudsonrivertrading.com/careers/",
        "linkedin": "https://www.linkedin.com/company/hudson-river-trading/",
    },
    {
        "company": "Citadel",
        "aliases": ("citadel",),
        "confidence": "Med",
        "visa_note": "Application asks if you need sponsorship; intern JD has no “will not sponsor” clause. Confirm J-1 vs F-1. Apply Citadel and Citadel Securities separately.",
        "careers_page": "https://www.citadel.com/careers/internships/",
        "linkedin": "https://www.linkedin.com/company/citadel-llc/",
        "skip_company": ("citadelsecurities",),
    },
    {
        "company": "Citadel Securities",
        "aliases": ("citadelsecurities",),
        "confidence": "Med",
        "visa_note": "Same campus pipeline as Citadel — apply this firm separately. Confirm J-1 vs F-1.",
        "careers_page": "https://www.citadelsecurities.com/careers/",
        "linkedin": "https://www.linkedin.com/company/citadel-securities/",
    },
    {
        "company": "Optiver",
        "aliases": ("optiver",),
        "confidence": "Med",
        "visa_note": "Intern class from 40+ countries; tracker tags H-1B. Ask J-1 for a software / quant-dev intern if posted. H-1B is full-time conversion, not intern visa.",
        "careers_page": "https://optiver.com/working-at-optiver/career-opportunities/",
        "linkedin": "https://www.linkedin.com/company/optiver/",
    },
    {
        "company": "Salesforce",
        "aliases": ("salesforce",),
        "confidence": "Med",
        "visa_note": "Official: internship program is open to university students around the world. Not a named J-1 policy — confirm in writing.",
        "careers_page": "https://www.salesforce.com/company/careers/university/internships/",
        "linkedin": "https://www.linkedin.com/company/salesforce/",
    },
    {
        "company": "Cloudflare",
        "aliases": ("cloudflare",),
        "confidence": "Med",
        "visa_note": "Austin SWE intern JD has no immigration sentence. Export-control clause is separate from J-1. Confirm they will host J-1.",
        "careers_page": "https://www.cloudflare.com/careers/early-talent/",
        "linkedin": "https://www.linkedin.com/company/cloudflare/",
    },
    {
        "company": "Apple",
        "aliases": ("apple",),
        "confidence": "Med",
        "visa_note": "Waterloo-student reports they host J-1; no official intern visa page. Crowd CPT lists often mark Apple as F-1-only. Confirm J-1 in writing before spending recruiter capital.",
        "careers_page": "https://jobs.apple.com/en-us/search?team=internships-STDNT-INTRN",
        "linkedin": "https://www.linkedin.com/company/apple/",
        "exact": True,
        "skip_company": ("applebank",),
    },
]

HEADER = ["company", "confidence", "visa_note", "open_now", "blocked", "terms_matched", "term_count",
          "intern_count", "california_count",
          "example_job", "example_link", "careers_page", "linkedin", "contact",
          "work_auth", "company_size", "funding_stage", "smb", "ca_interns", "ca_intern_profiles"]
# Read from the full simplify.jobs JD (best status across the company's US intern postings).
WORK_AUTH_RANK = {"sponsors": 0, "silent": 1, "authorized": 2}
SMB_SIZES = {"1-10", "11-50", "51-200", "201-500"}
SMB_STAGES = re.compile(r"seed|series[ _]?[abc]\b|angel|pre[- _]?seed|early", re.I)
CA_INTERN_CACHE = core.OUT_DIR / ".ca_intern_evidence.json"
# US intern cycle: Summer 2027 is the target, not an exclusion (unlike Canada outreach).
US_TERMS = [
    "Summer 2027", "Fall 2026", "Summer 2026", "Winter 2027", "Spring 2027",
    "Winter 2026", "Fall 2025", "Summer 2025", "Open",
]
US_TERM_RANK = {t: i for i, t in enumerate(US_TERMS)}
_CONF_RANK = {"High": 0, "Med": 1}
VISA_CACHE = core.OUT_DIR / ".us_intern_visa.json"
VISA_CACHE_TTL = 60 * 60
_BLOCK_NOTE = {
    "no-sponsor": "Intern list or posting says they do not offer visa sponsorship.",
    "citizen": "Intern list or posting requires US citizenship.",
    "clearance": "Intern posting requires a security clearance.",
    "us-school": "Intern posting requires enrollment at a US school.",
}
_WORK_AUTH_BLOCK = {"citizen": "citizen", "clearance": "clearance", "no_sponsor": "no-sponsor",
                    "us_school": "us-school"}
_NO_SPONSOR_TEXT = re.compile(
    r"does not (?:offer|provide)(?: visa)? sponsor|"
    r"do not (?:offer|provide)(?: visa)? sponsor|"
    r"will not sponsor|won't sponsor|"
    r"not (?:able|going) to (?:provide |offer )?sponsor|"
    r"unable to sponsor|no visa sponsor|"
    r"without (?:visa |immigration )?sponsor|"
    r"sponsor(?:ship)? (?:is )?not available|"
    r"not offer(?:ing)? (?:visa )?sponsor",
    re.I,
)
_CITIZEN_TEXT = re.compile(
    r"\b(?:us|u\.s\.|united states) citizens?(?:hip)?\b|"
    r"must be an? (?:us|u\.s\.|united states) citizen|"
    r"citizenship (?:is )?required|"
    r"green card holders? only",
    re.I,
)
_CLEARANCE_TEXT = re.compile(
    r"\bts/?sci\b|\btop secret\b|\bsecret clearance\b|\bsecurity clearance\b",
    re.I,
)
_SPONSOR_BLOCK = {
    "does-not-offer-sponsorship": "no-sponsor",
    "does-not-sponsor": "no-sponsor",
    "no-sponsorship": "no-sponsor",
    "no-sponsor": "no-sponsor",
    "u.s.-citizenship-is-required": "citizen",
    "us-citizenship-is-required": "citizen",
    "citizens-only": "citizen",
    "citizenship-required": "citizen",
    "us-citizen": "citizen",
}


def _matches(company: str, entry: dict) -> bool:
    key = core.dedup_key(company)
    if not key:
        return False
    for skip in entry.get("skip_company") or ():
        if skip in key:
            return False
    aliases = entry["aliases"]
    if key in aliases:
        return True
    if entry.get("exact"):
        return False
    # "sig" must not steal Signifyd / Signal. Prefix only for a real company stem.
    return any(len(alias) >= 5 and key.startswith(alias) for alias in aliases)


def _j1_overlay(company: str) -> dict[str, object] | None:
    hits = [e for e in COMPANIES if _matches(company, e)]
    if not hits:
        return None
    hits.sort(key=lambda e: _CONF_RANK.get(str(e["confidence"]), 9))
    return hits[0]


def us_for_outreach(location: str | None, title: str = "", url: str = "") -> bool:
    """True when a software intern posting should count as US for the US outreach list."""
    loc = location or ""
    countries = core.us_canada(loc)
    if "US" in countries:
        return True
    if "Canada" in countries:
        return False
    unclear = (not loc) or loc.strip().lower() in {"location not listed"} or "country unclear" in loc.lower()
    if not unclear:
        return False
    if any(core._FOREIGN_PLACE.search(s) or core._poll.FOREIGN_COUNTRY_TAIL_RE.search(s)
           for s in (loc, title) if s):
        return False
    return True


def _us_terms(title: str, url: str, posted_at: str | None, is_open: bool) -> list[str]:
    named = [t for t in core.posting_terms(title, url) if t in US_TERM_RANK]
    if named:
        return named
    inferred = core.coop_term_from_posted(posted_at)
    if inferred and inferred in US_TERM_RANK:
        return [inferred]
    return ["Open"] if is_open else []


def _title_blocked(title: str, pattern: str | None) -> bool:
    return bool(pattern and re.search(pattern, title or "", re.I))


def _norm_url(url: str) -> str:
    return (url or "").strip().rstrip("/").split("?")[0].lower()


def _sponsorship_reason(raw: str | None) -> str | None:
    s = re.sub(r"[_\s]+", "-", (raw or "").strip().lower())
    s = s.replace("–", "-").replace("—", "-")
    return _SPONSOR_BLOCK.get(s)


def visa_block_reason(title: str, url: str, sponsorship: str | None = None) -> str | None:
    """Why this intern posting is a dead end for a Concordia Canadian. None = silent."""
    tagged = _sponsorship_reason(sponsorship)
    if tagged:
        return tagged
    blob = f"{title or ''} {url or ''}"
    if _CITIZEN_TEXT.search(blob):
        return "citizen"
    if _CLEARANCE_TEXT.search(blob):
        return "clearance"
    if _NO_SPONSOR_TEXT.search(blob):
        return "no-sponsor"
    return None


# --- work authorization read from the full job description ------------------------------
# Order matters: the first match wins. citizen/clearance/no_sponsor/us_school are dead ends for a
# Concordia student on a J-1; "authorized" is ambiguous (J-1 holders are authorized once the DS-2019
# is issued), so it stays on the list flagged "ask the recruiter"; "silent" is the target.
WORK_AUTH_BLOCKED = ("citizen", "clearance", "no_sponsor", "us_school")
_JD_CITIZEN = re.compile(
    r"must be an? (?:us|u\.s\.|united states) citizen|"
    r"(?:us|u\.s\.|united states) citizenship (?:is )?(?:generally |typically )?(?:required|needed|mandatory)(?!\s*:?\s*no\b)|"
    r"(?:required|requirements?|qualifications?)\s*:?[^.]{0,25}\b(?:us|u\.s\.) citizen(?:ship)?\b|"
    r"citizenship\s*:\s*(?:this position )?requires? (?:us|u\.s\.)|"
    r"requires? (?:us|u\.s\.|united states) citizenship|"
    r"\b(?:us|u\.s\.|united states) persons?\b(?! [a-z]+ under)|\bitar\b|"
    r"only (?:us|u\.s\.|united states) citizens|(?:us|u\.s\.) citizenship desired|"
    r"green card holders? only|citizens? (?:or|and) (?:lawful )?permanent residents? only",
    re.I)
_JD_CLEARANCE = re.compile(r"\bts/?sci\b|\btop secret\b|\b(?:interim )?secret (?:security )?clearance\b|"
                           r"(?:active|obtain|maintain|eligib\w+ for) (?:a |an )?(?:us |u\.s\. )?(?:government )?"
                           r"security clearance", re.I)
_JD_NO_SPONSOR = re.compile(
    _NO_SPONSOR_TEXT.pattern + r"|"
    r"not eligible for (?:visa |immigration |employment )?sponsor|"
    r"(?:cannot|can ?not|can't) (?:offer |provide |support )?(?:visa |immigration )?sponsor|"
    r"(?:will not|won't|do not|does not|not) (?:now or in the future )?require (?:visa |immigration |employer )?sponsor|"
    r"without (?:the need for |requiring |needing )?(?:current or future |now or in the future )?"
    r"(?:visa |immigration |employer |employment )?sponsor|"
    r"sponsorship (?:will not|cannot|is not) be (?:provided|offered|available)|"
    r"not (?:in a position|able) to (?:offer|provide|support) (?:visa |immigration )?sponsor|"
    r"without [^.]{0,40}sponsorship|"
    r"\bnot\b[^.]{0,80}\brequire[^.]{0,40}sponsorship|"
    r"not (?:offer|provide|pursue|support) [^.]{0,30}sponsorship|"
    r"will not [a-z]+ (?:visa |immigration )?sponsorship|"
    r"not available for any [^.]{0,20}sponsorship|"
    r"sponsorship\s*:\s*no\b|"
    r"(?:unable|not able) to consider [^.]{0,40}(?:student )?visa|"
    r"not eligible for [^.]{0,20}\b(?:f-?1|j-?1)\b|"
    r"authori[sz]ed to work for any employer|"
    r"(?:not|no) (?:provide |offer )?[^.]{0,40}immigration[- ](?:related )?(?:sponsorship|support)|"
    r"(?:authori[sz]ed|eligible) to work [^.]{0,60}(?:now and|now or|both now and) in the future|"
    r"\b(?:cpt|opt)\b[^.]{0,40}\b(?:required|only|eligib)|"
    r"must (?:be eligible for|have) (?:cpt|opt)\b",
    re.I)
_JD_US_SCHOOL = re.compile(
    r"enrolled (?:full[- ]time )?(?:in|at) an? (?:accredited )?(?:us|u\.s\.|united states)[- ]based "
    r"(?:college|university|school|institution)|"
    r"enrolled (?:full[- ]time )?(?:in|at) an? (?:accredited )?(?:us|u\.s\.|united states) "
    r"(?:college|university|school|institution)|"
    r"attending an? (?:us|u\.s\.|united states) (?:college|university)", re.I)
_JD_SPONSORS = re.compile(
    r"(?:visa|immigration) sponsorship (?:is |may be )?(?:available|offered|provided)|"
    r"(?:will|can|may|are able to|happy to|open to) (?:provide |offer |consider )?(?:visa |immigration )?sponsor(?:ing|ship)?\b|"
    r"\bj-?1 (?:visa )?(?:sponsorship|support|program)|sponsor[^.]{0,30}\bj-?1\b|handle your j-?1", re.I)
_JD_AUTHORIZED = re.compile(
    r"(?:legally |currently )?(?:authori[sz]ed|eligible|permitted|able) to work (?:in|within|for) "
    r"(?:the )?(?:us|u\.s\.|united states|usa)|"
    r"(?:us|u\.s\.) work authori[sz]ation|work authori[sz]ation (?:in|for) the (?:us|u\.s\.|united states)",
    re.I)


def work_auth_status(text: str, sponsors_h1b: object = None) -> str:
    """citizen | clearance | no_sponsor | us_school | sponsors | authorized | silent."""
    t = re.sub(r"\s+", " ", text or "")
    if _JD_CITIZEN.search(t):
        return "citizen"
    if _JD_CLEARANCE.search(t):
        return "clearance"
    if _JD_NO_SPONSOR.search(t):
        return "no_sponsor"
    if _JD_US_SCHOOL.search(t):
        return "us_school"
    if _JD_SPONSORS.search(t) or sponsors_h1b is True:
        return "sponsors"
    if _JD_AUTHORIZED.search(t):
        return "authorized"
    return "silent"


def _strip_html(s: str) -> str:
    import html as _html
    return _html.unescape(re.sub(r"<[^>]+>", " ", s or ""))


def _simplify_detail(client, job_id: str) -> dict[str, object] | None:
    """Fetch one simplify.jobs posting page and pull the JD + company fields out of __NEXT_DATA__."""
    r = client.get(f"https://simplify.jobs/p/{job_id}")
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not m:
        return None
    jp = (json.loads(m.group(1)).get("props") or {}).get("pageProps", {}).get("jobPosting") or {}
    company = (jp.get("job") or {}).get("company") or {}
    reqs = [*(jp.get("requirements") or []), *(jp.get("additional_requirements") or []),
            *(jp.get("desirable") or [])]
    return {"description": _strip_html(jp.get("description") or ""),
            "requirements": "\n".join(str(x) for x in reqs),
            "sponsors_h1b": jp.get("sponsors_h1b"), "company_sponsors_h1b": company.get("sponsors_h1b"),
            "company_linkedin": company.get("linkedin") or "", "company_url": company.get("url") or ""}


def enrich_simplify_details(limit: int = 400, gap: float = 1.0) -> int:
    """Fetch JD text for simplify_jobs rows never enriched (open US rows first) and classify work auth."""
    import httpx

    db = core.connect()
    core._simplify_db(db)
    todo = db.execute("""SELECT id FROM simplify_jobs WHERE detail_at IS NULL AND open=1
                         ORDER BY country='US' DESC, first_seen DESC LIMIT ?""", (limit,)).fetchall()
    done = 0
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36"
    with httpx.Client(timeout=30, follow_redirects=True, headers={"User-Agent": ua}) as client:
        for (job_id,) in todo:
            try:
                d = _simplify_detail(client, job_id)
            except Exception as exc:  # noqa: BLE001 — retry this job next run
                core.log_line(f"{'simplify_detail':16} {job_id}: {exc}")
                time.sleep(gap * 5)
                continue
            if d is None:
                continue
            stamp = datetime.now(UTC).isoformat(timespec="seconds")
            status = work_auth_status(f"{d.get('description', '')}\n{d.get('requirements', '')}",
                                      d.get("sponsors_h1b")) if d else ""
            with db:
                db.execute("""UPDATE simplify_jobs SET detail_at=?, description=?, requirements=?,
                    sponsors_h1b=?, company_sponsors_h1b=?, company_linkedin=?, company_url=?, work_auth=?
                    WHERE id=?""",
                           (stamp, d.get("description", ""), d.get("requirements", ""),
                            json.dumps(d.get("sponsors_h1b")), json.dumps(d.get("company_sponsors_h1b")),
                            d.get("company_linkedin", ""), d.get("company_url", ""), status, job_id))
            done += 1
            time.sleep(gap)
    db.close()
    return done


def reclassify_simplify() -> int:
    """Re-run work_auth_status over stored JDs (after editing the regexes)."""
    db = core.connect()
    core._simplify_db(db)
    rows = db.execute("SELECT id, description, requirements, sponsors_h1b FROM simplify_jobs "
                      "WHERE detail_at IS NOT NULL AND description IS NOT NULL").fetchall()
    with db:
        for job_id, desc, reqs, h1b in rows:
            db.execute("UPDATE simplify_jobs SET work_auth=? WHERE id=?",
                       (work_auth_status(f"{desc}\n{reqs}", json.loads(h1b or "null")), job_id))
    db.close()
    return len(rows)


def is_smb(size: str, stage: str) -> bool:
    """Small/early company: <= 500 people, or seed through Series C when size is unknown."""
    if size:
        return size in SMB_SIZES
    return bool(stage and SMB_STAGES.search(stage))


def simplify_by_url() -> dict[str, dict]:
    """simplify.jobs/p/<id> -> snapshot row (incl. work_auth read from the JD)."""
    db = core.connect()
    core._simplify_db(db)
    cols = ("id", "company", "title", "locations", "country", "company_size", "funding_stage",
            "company_linkedin", "company_url", "work_auth", "open")
    out = {}
    for row in db.execute(f"SELECT {', '.join(cols)} FROM simplify_jobs"):
        d = dict(zip(cols, row))
        out[_norm_url(f"https://simplify.jobs/p/{d['id']}")] = d
    db.close()
    return out


def load_ca_intern_evidence() -> dict[str, dict]:
    """company key -> {count, profiles, checked_at} written by find_contacts.py --canadian-interns."""
    try:
        return json.loads(CA_INTERN_CACHE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _fetch_json(url: str) -> object:
    req = urllib.request.Request(url, headers={"User-Agent": "ats-scrapers/us-outreach"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def _fetch_csv_rows(url: str) -> list[dict[str, str]]:
    req = urllib.request.Request(url, headers={"User-Agent": "ats-scrapers/us-outreach"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        text = resp.read().decode()
    return list(csv.DictReader(text.splitlines()))


def intern_list_visa_by_url() -> dict[str, str]:
    """url → no-sponsor|citizen from Simplify / vanshb03 / zshah intern lists (hourly cache)."""
    if VISA_CACHE.exists() and time.time() - VISA_CACHE.stat().st_mtime < VISA_CACHE_TTL:
        try:
            data = json.loads(VISA_CACHE.read_text(encoding="utf-8"))
            urls = data.get("urls") if isinstance(data, dict) else data
            if isinstance(urls, dict):
                return {str(k): str(v) for k, v in urls.items() if v}
        except json.JSONDecodeError:
            pass
    out: dict[str, str] = {}

    def add(url: str, raw: str | None) -> None:
        reason = _sponsorship_reason(raw)
        key = _norm_url(url)
        if reason and key and key not in out:
            out[key] = reason

    try:
        for source in ("simplify", "vanshb03"):
            for x in _fetch_json(core.LIST_SOURCES[source]) or []:
                if not isinstance(x, dict):
                    continue
                add(str(x.get("url") or ""), x.get("sponsorship"))
        for row in _fetch_csv_rows(core.LIST_SOURCES["zshah101"]):
            add(row.get("url") or "", row.get("sponsorship"))
    except Exception as exc:  # noqa: BLE001 — live sqlite list still builds
        core.log_line(f"{'us_outreach':16} intern-list visa tags: {exc}")
        if VISA_CACHE.exists():
            try:
                data = json.loads(VISA_CACHE.read_text(encoding="utf-8"))
                urls = data.get("urls") if isinstance(data, dict) else data
                return {str(k): str(v) for k, v in (urls or {}).items() if v}
            except json.JSONDecodeError:
                return out
        return out
    core.OUT_DIR.mkdir(exist_ok=True)
    VISA_CACHE.write_text(json.dumps({"urls": out}), encoding="utf-8")
    return out


def collect_us_sponsor_companies() -> list[dict[str, str]]:
    """US software-intern employers. Silent visa language stays; explicit no-sponsor drops."""
    by_company: dict[str, dict] = {}
    visa_urls = intern_list_visa_by_url()
    simplify = simplify_by_url()

    def note(company: str, title: str, location: str, url: str, terms: list[str],
             is_open: bool) -> None:
        company = core.outreach_company_name(company)
        key = core.dedup_key(company)
        if not key or core._AGENCY_RE.search(company or ""):
            return
        overlay = _j1_overlay(company)
        if overlay and _title_blocked(title, overlay.get("skip_title")):
            return
        if overlay:
            company = str(overlay["company"])
            key = core.dedup_key(company)
        sj = simplify.get(_norm_url(url))
        blocked = None if overlay else (_WORK_AUTH_BLOCK.get((sj or {}).get("work_auth") or "")
                                        or visa_block_reason(title, url, visa_urls.get(_norm_url(url))))
        slot = by_company.setdefault(key, {
            "name": company, "terms": {}, "open": False,
            "open_job": None, "any_job": None,
            "clean": False, "blocked": "", "auth": set(),
        })
        if sj and sj.get("work_auth") and not blocked:
            slot["auth"].add(sj["work_auth"])
        job = (title, location, url)
        keep = [t for t in terms if t in US_TERM_RANK]
        if blocked:
            if slot["clean"]:
                return
            slot["blocked"] = blocked
            slot["any_job"] = slot["any_job"] or job
            for term in keep:
                slot["terms"].setdefault(term, job)
            if is_open:
                slot["open"] = True
                slot["open_job"] = slot["open_job"] or job
            return
        if not slot["clean"]:
            slot["terms"] = {}
            slot["open"] = False
            slot["open_job"] = None
            slot["any_job"] = job
        slot["clean"] = True
        slot["blocked"] = ""
        slot["any_job"] = slot["any_job"] or job
        if not keep and not is_open:
            slot["terms"].setdefault("", job)
        for term in keep:
            slot["terms"].setdefault(term, job)
        if is_open:
            slot["open"] = True
            slot["open_job"] = slot["open_job"] or job

    def note_live(company: str, title: str, location: str, url: str,
                  posted_at: str | None, is_open: bool) -> None:
        if not company or not core.is_software_internship(title, software_employer=True):
            return
        if not us_for_outreach(location, title, url):
            return
        note(company, title, location, url, _us_terms(title, url, posted_at, is_open), is_open)

    db = core.connect()
    for company, title, location, url, posted_at, is_open in db.execute(
            "SELECT company, title, location, url, posted_at, open FROM postings"):
        note_live(company or "", title or "", location or "", url or "", posted_at, bool(is_open))
    db.close()

    def note_csv(path: Path) -> None:
        if not path.exists():
            return
        with path.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                note_live(row.get("company") or "", row.get("title") or "",
                          row.get("location") or "", row.get("link") or "", None, True)

    note_csv(core.US_CSV_PATH)
    note_csv(core.ALL_CSV_PATH)

    # Every open US software internship on simplify.jobs, straight from the snapshot table.
    for u_url, sj in simplify.items():
        if sj["country"] == "US" and sj["open"]:
            note_live(sj["company"], sj["title"], sj["locations"] or "United States", u_url, None, True)
    sj_company: dict[str, dict] = {}
    for sj in simplify.values():
        k = core.dedup_key(core.outreach_company_name(sj["company"]))
        cur = sj_company.setdefault(k, {})
        for f in ("company_size", "funding_stage", "company_linkedin", "company_url"):
            if sj.get(f) and not cur.get(f):
                cur[f] = sj[f]
    ca_interns = load_ca_intern_evidence()

    db = core.connect()
    core._swe_intern_history_table(db)
    core.seed_known_us_intern_employers(db)
    for company, title, location, url, posted_at in db.execute(
            "SELECT company, title, location, url, posted_at FROM swe_intern_history WHERE region='US'"):
        note_live(company or "", title or "", location or "", url or "", posted_at, False)
    db.close()

    known_us = {core.dedup_key(core.outreach_company_name(e["company"])): e
                for e in core.KNOWN_US_INTERN_EMPLOYERS}
    for entry in core.KNOWN_US_INTERN_EMPLOYERS:
        note_live(entry["company"], entry["title"], entry["location"], entry["url"], None, True)

    linkedin: dict[str, str] = {}
    careers: dict[str, str] = {}
    db = core.connect()
    core._discover_tables(db)
    for name, slug, url in db.execute(
            "SELECT name, li_slug, url FROM discovered"):
        key = core.dedup_key(core.outreach_company_name(name or ""))
        if not key:
            continue
        if slug and key not in linkedin:
            linkedin[key] = f"https://www.linkedin.com/company/{slug.strip().strip('/')}/"
        if url and str(url).startswith("http") and key not in careers:
            careers[key] = url
    db.close()
    for key, entry in known_us.items():
        if entry.get("linkedin") and key not in linkedin:
            linkedin[key] = entry["linkedin"]
        if entry.get("careers_page") and key not in careers:
            careers[key] = entry["careers_page"]

    stats = core.intern_hiring_stats()
    rows: list[dict[str, str]] = []
    seen_j1: set[str] = set()
    for key, slot in by_company.items():
        company = slot["name"]
        overlay = _j1_overlay(company)
        matched = [t for t in slot["terms"] if t in US_TERM_RANK]
        matched.sort(key=lambda t: US_TERM_RANK[t])
        job = slot["open_job"] or (slot["terms"].get(matched[0]) if matched else None) or slot["any_job"]
        title, loc, url = job or ("", "", "")
        if overlay:
            seen_j1.add(str(overlay["company"]))
            company = str(overlay["company"])
        blocked = "" if overlay or slot["clean"] else (slot.get("blocked") or "")
        visa_note = str(overlay["visa_note"]) if overlay else (_BLOCK_NOTE.get(blocked) or "")
        meta = sj_company.get(key) or sj_company.get(core.dedup_key(company)) or {}
        auth = min(slot.get("auth") or (), key=lambda a: WORK_AUTH_RANK.get(a, 9), default="")
        ev = ca_interns.get(core.dedup_key(company)) or {}
        intern_n, intern_ca = core.intern_volume_for(company, stats, region="US")
        if core.is_california_place(loc, title):
            intern_ca = max(intern_ca, 1)
        rows.append({
            "company": company,
            "confidence": str(overlay["confidence"]) if overlay else "",
            "visa_note": visa_note,
            "open_now": "yes" if slot["open"] else "",
            "blocked": blocked,
            "terms_matched": ", ".join(matched),
            "term_count": str(len(matched)) if matched else "",
            "intern_count": str(intern_n),
            "california_count": str(intern_ca),
            "example_job": f"{title} ({loc})" if title and loc else title,
            "example_link": url or "",
            "careers_page": ((str(overlay.get("careers_page") or "") if overlay else "") or careers.get(key, "")
                             or meta.get("company_url", "")),
            "linkedin": ((str(overlay.get("linkedin") or "") if overlay else "") or linkedin.get(key, "")
                         or meta.get("company_linkedin", "")),
            "contact": str(overlay.get("contact") or "") if overlay else "",
            "work_auth": "" if blocked else auth,
            "company_size": meta.get("company_size", ""),
            "funding_stage": meta.get("funding_stage", ""),
            "smb": "yes" if is_smb(meta.get("company_size", ""), meta.get("funding_stage", "")) else "",
            "ca_interns": str(ev.get("count", "")) if ev else "",
            "ca_intern_profiles": " ".join(ev.get("profiles", [])[:3]) if ev else "",
        })

    # Curated J-1 firms with no US intern posting in sqlite still belong on the list.
    for entry in COMPANIES:
        if str(entry["company"]) in seen_j1:
            continue
        intern_n, intern_ca = core.intern_volume_for(str(entry["company"]), stats, region="US")
        rows.append({
            "company": str(entry["company"]),
            "confidence": str(entry["confidence"]),
            "visa_note": str(entry["visa_note"]),
            "open_now": "",
            "blocked": "",
            "terms_matched": "",
            "term_count": "",
            "intern_count": str(intern_n),
            "california_count": str(intern_ca),
            "example_job": "",
            "example_link": "",
            "careers_page": str(entry.get("careers_page") or ""),
            "linkedin": str(entry.get("linkedin") or ""),
            "contact": str(entry.get("contact") or ""),
            "work_auth": "", "company_size": "", "funding_stage": "", "smb": "",
            "ca_interns": "", "ca_intern_profiles": "",
        })

    rows.sort(key=lambda r: (
        -core.intern_priority(int(r["intern_count"] or 0), int(r["california_count"] or 0)),
        0 if r["open_now"] else 1,
        r["company"].lower(),
    ))
    return rows


def write_us_sponsor_companies() -> int:
    core.OUT_DIR.mkdir(exist_ok=True)
    rows = collect_us_sponsor_companies()
    core._write_rows(core.US_SPONSOR_CSV, HEADER, [[r[h] for h in HEADER] for r in rows])
    return len(rows)


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--enrich", type=int, default=0, metavar="N",
                    help="first read the JD of up to N new simplify.jobs postings (work-auth status)")
    ap.add_argument("--reclassify", action="store_true", help="re-run the work-auth regexes on stored JDs")
    args = ap.parse_args()
    if args.reclassify:
        print(f"reclassified {reclassify_simplify()} postings")
    if args.enrich:
        print(f"read {enrich_simplify_details(limit=args.enrich)} new JDs")
    rows = collect_us_sponsor_companies()
    core.OUT_DIR.mkdir(exist_ok=True)
    core._write_rows(core.US_SPONSOR_CSV, HEADER, [[r[h] for h in HEADER] for r in rows])
    live = sum(1 for r in rows if r["open_now"] and not r["blocked"])
    blocked = sum(1 for r in rows if r["blocked"])
    keep = len(rows) - blocked
    print(f"{keep} US intern employers ({live} open now, {blocked} dropped: said no-sponsor/citizen/clearance)")


if __name__ == "__main__":
    main()
