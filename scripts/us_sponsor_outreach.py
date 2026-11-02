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
          "example_job", "example_link", "careers_page", "linkedin", "contact"]
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
}
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
        blocked = None if overlay else visa_block_reason(
            title, url, visa_urls.get(_norm_url(url)))
        slot = by_company.setdefault(key, {
            "name": company, "terms": {}, "open": False,
            "open_job": None, "any_job": None,
            "clean": False, "blocked": "",
        })
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
            "careers_page": (str(overlay.get("careers_page") or "") if overlay else "") or careers.get(key, ""),
            "linkedin": (str(overlay.get("linkedin") or "") if overlay else "") or linkedin.get(key, ""),
            "contact": str(overlay.get("contact") or "") if overlay else "",
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
    rows = collect_us_sponsor_companies()
    core.OUT_DIR.mkdir(exist_ok=True)
    core._write_rows(core.US_SPONSOR_CSV, HEADER, [[r[h] for h in HEADER] for r in rows])
    live = sum(1 for r in rows if r["open_now"] and not r["blocked"])
    blocked = sum(1 for r in rows if r["blocked"])
    keep = len(rows) - blocked
    print(f"{keep} US intern employers ({live} open now, {blocked} dropped: said no-sponsor/citizen/clearance)")


if __name__ == "__main__":
    main()
