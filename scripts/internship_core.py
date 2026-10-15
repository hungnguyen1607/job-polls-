#!/usr/bin/env python3
"""Shared engine for the internship watcher scripts.

The ``watch_*.py`` scripts each poll one group of sources on their own
interval. All of them write into the same place:

    engineering-ats/US_internships.csv      company,title,location,posted,link,source
    engineering-ats/canada_internships.csv  open software internships in the US / in Canada,
                                             de-duplicated, newest first (postings in both
                                             countries or with no clear country are in both)
    engineering-ats/.internships.sqlite   shared store (safe for concurrent scripts)
    engineering-ats/watch.log         one line per pass (time, counts, peak memory)

Filter: title has an internship word (intern, co-op, stagiaire…) AND a
software-side role (software, developer, engineer, IT, data, ML/AI, DevOps,
cloud, security…; not mechanical/civil/electrical/sales/recruiting), AND the
job is in the US or Canada (remote with no country is kept and tagged).

Memory: every pass of every source runs in a fresh child process that exits
when the pass ends, so nothing accumulates. A child that exceeds the memory
cap is stopped and logged; the next pass still runs.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import contextvars
import importlib.util
import json
import os
import secrets
import re
import subprocess
import sqlite3
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from ats_scrapers.scrapers.workday import URL_PATTERN  # noqa: E402
from scripts.run_pipeline import CONFIGS  # noqa: E402

_spec = importlib.util.spec_from_file_location("poll_engineering", REPO / "scripts" / "poll_engineering.py")
_poll = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_poll)
us_canada = _poll.us_canada

OUT_DIR = REPO / "engineering-ats"
US_CSV_PATH = OUT_DIR / "US_internships.csv"
CA_CSV_PATH = OUT_DIR / "canada_internships.csv"
ALL_CSV_PATH = OUT_DIR / "all_open_internships.csv"
MOVED_CSV = OUT_DIR / "moved_boards.csv"
WATCH_LOG = OUT_DIR / "watch.log"
DB_PATH = OUT_DIR / ".internships.sqlite"
EASTERN = ZoneInfo("America/New_York")
MIN = 60

# Sources that are a whole site (not a list of companies in ats-companies/).
SINGLE_SOURCES = {"cgi", "shopify", "janestreet", "linkedin", "careers_search", "discover", "bytedance", "google", "amazon", "apple", "meta", "tiktok", "uber", "tesla",
                  "jobbankca", "builtin", "ycombinator", "wellfound", "remoterocketship", "workopolis", "eluta"}
LIST_SOURCES = {
    "simplify": "https://raw.githubusercontent.com/SimplifyJobs/Summer2027-Internships/dev/.github/scripts/listings.json",
    "vanshb03": "https://raw.githubusercontent.com/vanshb03/Summer2027-Internships/dev/.github/scripts/listings.json",
    "jobright": "https://raw.githubusercontent.com/jobright-ai/2026-Software-Engineer-Internship/master/README.md",
    "speedyapply": "https://raw.githubusercontent.com/speedyapply/2027-SWE-College-Jobs/main/README.md",
    "zshah101": "https://raw.githubusercontent.com/zshah101/Automated-List-Of-Summer-2027-and-Fall-2026-Tech-Internships/main/data/internships.csv",
    "earlycareerradar": "https://earlycareerradar.com/summer-internships",
    "applyguy": "https://raw.githubusercontent.com/ApplyGuy/2027-Internships/main/data/internships.json",
    # Public guide page (robots.txt allows all) listing every role added in the last 30 days.
    "interndock": "https://www.interndock.com/tracker/guides/new-summer-2027-internships-last-30-days",
    # Canada-focused lists.
    "negarprh": "https://raw.githubusercontent.com/negarprh/Canadian-Tech-Internships-2027/main/README.md",
    "hansonchen": "https://raw.githubusercontent.com/HansonChen06/canada-tech-internships/main/data/jobs.json",
}
CONCURRENCY = {"workday": 32, "greenhouse": 24, "lever": 24, "ashby": 24, "smartrecruiters": 24,
               "workable": 4, "recruitee": 4,  # both 429 above ~4 req/s
               "eightfold": 2,  # see MIN_GAP
               "canada": 32}  # mixed hosts; each source inside it keeps its own cap (run_pass)
DEFAULT_CONCURRENCY = 16
# Companies per child process. Heavy scrapers get smaller batches so a process never
# reaches the memory cap (SuccessFactors pages are large).
BATCH_SIZE = {"successfactors": 100, "lever": 250, "careers": 200, "canada": 420}
# Batch processes run side by side for these sources (others run one batch at a time).
# Careers: 3 at once; 8 (each rendering pages in Chrome) pushed load past 100 on 16 cores
# and starved the other sources (ADP / Paycom passes hit the 45-min cap, 2026-10-08).
# Canada: all ~4 batches at once (API calls, little rendering) so a round takes ~3-4 min, not ~13.
PARALLEL_BATCHES = {"careers": 3, "canada": 4}
CAREERS_PAGE_CAP = 90  # seconds per careers page: fetch, maybe render, read linked boards
# A few SuccessFactors tenants are huge on their own; give that source more headroom
# (only one of its processes runs at a time).
MEM_CAP_MB = {"successfactors": 3000}
SLOW_DELAY = {"workable": 1.0}  # seconds between companies
NEEDS_DETAIL = {"taleo"}  # listings carry no location without the detail page


# --- filter -----------------------------------------------------------------------

INTERN_RE = re.compile(
    r"\b(intern|interns|internship|internships|co-?op|coop|stagiaire|apprentice(ship)?|"
    r"summer (student|associate|analyst)|working student|werkstudent|student|stagiaires?|stagaires?)\b"
    r"|^\s*stage\b|\bstage\s*[:\-–]",
    re.I,
)
# A: the role must be software-side (user-confirmed: no mechanical/civil/etc.).
ROLE_RE = re.compile(
    # Strong on their own: clearly software / engineering / IT roles.
    r"(?i:\b(software|swe|sde|sdet|developer|programmer|programming language\w*|coder|full[- ]?stack|"
    r"front[- ]?end|back[- ]?end|frontend|backend|devops|devsecops|mlops|sre|site reliability|"
    r"d[eé]veloppe(ur|use)\w*|information technology|"
    r"computer science|computer engineering|machine learning|deep learning|"
    r"artificial intelligence|intelligence artificielle|cyber\s?security|cyber|cybers[eé]curit[eé]|"
    r"information security|infosec|donn[eé]es|r[eé]seaux?|infonuagique|network services|"
    r"intelligence d.affaires|business intelligence|"
    r"robotics software|firmware|embedded)\b"
    # Broad words only count in a software / engineering context.
    r"|\b((software|data|ml|ai|machine learning|platform|cloud|infrastructure|systems?|"
    r"network\w*|security|cyber\w*|devops|reliability|backend|back[- ]?end|frontend|front[- ]?end|"
    r"full[- ]?stack|web|mobile|ios|android|game|graphics|compiler|firmware|embedded|"
    r"automation|test|qa|quality|application\w*|solutions?|research|product|ux|"
    r"computer|it|digital|integration|database|analytics|desktop|support|devsecops|mlops) (engineer\w*|engineering)|"
    r"engineer\w* \w+ (software|data|ml|ai|cloud|platform|infrastructure|security)|"
    r"(computer|software|electrical (and|&) computer) engineering|"
    r"data (scien\w*|engineer\w*|analy\w*|infrastructure|platform)|"
    r"(software|web|mobile|app\w*|game|full[- ]?stack|front[- ]?end|back[- ]?end|"
    r"platform|systems) development|logiciel|informatique|"
    r"(qa|quality assurance|test|testing) (automation|engineer\w*|analyst|developer)|"
    r"test automation|software (qa|quality|testing)|"
    r"(network|system|systems|database|cloud|it) (administrat\w*|admin|analyst|support|operations)|"
    r"cloud (computing|infrastructure|solutions?)|security (analyst|operations|research)|"
    r"(mobile|ios|android|web) (app\w*|developer|development)|technology analyst)\b)"
    r"|\b(IT|AI|ML|LLM|NLP|SW)\b|\bS/W\b"  # case-sensitive: "IT" not "it", "SW" = software
)
SOFTWARE_SIDE_RE = re.compile(
    r"(?i:\b(software|firmware|embedded|data|machine learning|devops|cloud|computer|"
    r"programm\w*|developer|full[- ]?stack|web|mobile|cyber\w*|network|site reliability|sre|"
    r"qa|quality assurance analyst|test automation|information technology)\b)|\b(IT|AI|ML|SW)\b|\bS/W\b"
)
NON_SOFTWARE_RE = re.compile(
    r"\b(mechanical|civil|chemical|structural|manufacturing|industrial|process|electrical|"
    r"hardware|aerospace|materials?|nuclear|biomedical|environmental|geotechnical|hvac|"
    r"construction|mining|petroleum|packaging|plant|facilities|maintenance|field service|"
    r"rf|optical|mechatronics|power systems?|quality|operations|sustainability|tooling|"
    r"equipment|transmission|asset|production|controls?|automation & controls|design verification|"
    r"digital design|asic|silicon|analog|pcb|layout|validation|reliability|safety|"
    r"customer|field|sales|applications? engineer|industriel\w*|pratiques|facility|"
    r"building|energy|utility|utilities|substation|distribution|water|wastewater|"
    r"transportation|traffic|survey\w*|estimat\w*|project engineer|process|packaging|"
    r"biomedical|clinical|quality assurance|roadway|highway|bridge|electrification|"
    r"transit|railway|hydraulic|stormwater|geospatial survey)\b",
    re.I,
)
NOT_TECH_ROLE_RE = re.compile(
    r"\b(recruit\w*|talent acquisition|sales|account executive|account manager|"
    r"business development|marketing|trainer|tutor|teacher|data entry|human resources|hr|"
    r"buyer|acheteur|procurement|purchasing|finance|financial|accounting|audit\w*|tax|"
    r"project management|program management|strategy|market analysis|people analytics|"
    r"numerical control|cnc|machinist|legal|compliance|communications)\b",
    re.I,
)
# Titles that are software roles no matter what else they mention ("Financial Software Engineer").
CLEARLY_SOFTWARE_RE = re.compile(
    r"\b(software|developer|programmer|swe|sde|sdet|full[- ]?stack|devops|firmware|embedded|"
    r"machine learning|data scien\w*|data engineer\w*|ml engineer\w*|ai engineer\w*|"
    r"front[- ]?end|back[- ]?end|frontend|backend)\b",
    re.I,
)
REMOTE_ONLY_RE = re.compile(r"^\W*(fully |100% )?(remote|anywhere|virtual|work from home|wfh)"
                            r"(\W+(remote|hybrid|flexible|first|friendly))*\W*$", re.I)


def location_label(location: str | None, country_iso: str | None = None) -> str | None:
    """Return the location to show, or None when the job isn't in the US/Canada."""
    if us_canada(location, country_iso):
        return location or {"US": "United States", "CA": "Canada"}.get((country_iso or "").upper(), "")
    if not country_iso and location and REMOTE_ONLY_RE.match(location.strip()):
        return f"{location.strip()} (country unclear)"
    return None


def _software_title(title: str) -> bool:
    if not ROLE_RE.search(title):
        return False
    if NOT_TECH_ROLE_RE.search(title) and not CLEARLY_SOFTWARE_RE.search(title):
        return False
    if NON_SOFTWARE_RE.search(title) and not SOFTWARE_SIDE_RE.search(title):
        return False
    return True


def is_software_internship(title: str) -> bool:
    """Title has an internship word AND a software-side role word (see ROLE_RE)."""
    title = (title or "").replace("\xa0", " ").strip()
    return bool(INTERN_RE.search(title)) and _software_title(title)


def is_software_fulltime(title: str) -> bool:
    """A software-side role with no internship word: a company hiring these in Canada is
    likely to recruit software interns too (canada_software_companies.csv)."""
    title = (title or "").replace("\xa0", " ").strip()
    return not INTERN_RE.search(title) and _software_title(title)


# Set per company by run_pass; the checkers add every Canadian full-time software job they see.
_CA_HIRING: contextvars.ContextVar[list[tuple[str, str, str]] | None] = contextvars.ContextVar(
    "ca_hiring", default=None)


def note_ca_software(title: str | None, location: str | None, country_iso: str | None = None,
                     url: str | None = "") -> None:
    seen = _CA_HIRING.get()
    if seen is None or not title or not is_software_fulltime(title):
        return
    # One place at a time: "Vancouver, WA, United States" names both countries but is the US.
    parts = [""] if country_iso else re.split(r"\s*[|;/]\s*|\n", location or "")
    if any(us_canada(p, country_iso) == {"Canada"} for p in parts if p or country_iso):
        seen.append((title.strip(), location or "Canada", url or ""))


def keep(title: str, location: str | None, country_iso: str | None = None) -> str | None:
    if not is_software_internship(title):
        return None
    return location_label(location, country_iso)


# --- posting age --------------------------------------------------------------------

def iso(dt: Any) -> str | None:
    if isinstance(dt, datetime):
        return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC).isoformat(timespec="seconds")
    if isinstance(dt, str) and dt:
        try:
            return iso(datetime.fromisoformat(dt.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def workday_posted(text: str | None) -> str | None:
    """'Posted Today' / 'Posted Yesterday' / 'Posted 3 Days Ago' / 'Posted 30+ Days Ago' → ISO date."""
    if not text:
        return None
    t = text.lower()
    now = datetime.now(EASTERN)
    if "today" in t:
        days = 0
    elif "yesterday" in t:
        days = 1
    else:
        m = re.search(r"(\d+)\+?\s*day", t)
        if not m:
            return None
        days = int(m.group(1))
    day = (now - timedelta(days=days)).date()
    return datetime(day.year, day.month, day.day, tzinfo=UTC).isoformat(timespec="seconds")


def age_text(posted_at: str | None, first_seen: str, workday_text: str | None) -> str:
    if workday_text and "30+" in workday_text:
        return "30+ days ago"
    stamp = posted_at or first_seen
    try:
        dt = datetime.fromisoformat(stamp)
    except ValueError:
        return ""
    secs = max(0, (datetime.now(UTC) - dt).total_seconds())
    if workday_text:  # Workday only gives whole days
        days = max(0, (datetime.now(EASTERN).date() - dt.astimezone(UTC).date()).days)
        return "today" if days == 0 else "1 day ago" if days == 1 else f"{days} days ago"
    label = ago(secs)
    return label if posted_at else f"first seen {label}"


def ago(secs: float) -> str:
    """'just now', '1 min ago', '7 min ago', '3 h ago', '2 days ago'."""
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{int(secs // 60)} min ago"
    if secs < 86400:
        return f"{int(secs // 3600)} h ago"
    days = int(secs // 86400)
    return "1 day ago" if days == 1 else f"{days} days ago"


# --- storage (shared by all poller processes) ---------------------------------------

def connect() -> sqlite3.Connection:
    OUT_DIR.mkdir(exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE IF NOT EXISTS postings (
        key TEXT PRIMARY KEY, source TEXT, company_key TEXT, company TEXT, title TEXT,
        location TEXT, posted_at TEXT, posted_text TEXT, url TEXT,
        first_seen TEXT, last_seen TEXT, open INTEGER, origin TEXT, baseline INTEGER)""")
    # A source's first full pass is its baseline: undated postings found then have no
    # real "posted" time (we only know they already existed), so they're kept out of
    # the 7-day view. Undated postings found on later passes date from first_seen.
    db.execute("CREATE TABLE IF NOT EXISTS sources (source TEXT PRIMARY KEY, baseline_done INTEGER)")
    # Companies whose board failed on the latest pass (dead, closed, moved, blocked).
    db.execute("""CREATE TABLE IF NOT EXISTS failures (source TEXT, company_key TEXT, name TEXT,
        url TEXT, error TEXT, last_seen TEXT, PRIMARY KEY (source, company_key))""")
    db.commit()
    return db


def baseline_done(db: sqlite3.Connection, source: str) -> bool:
    row = db.execute("SELECT baseline_done FROM sources WHERE source=?", (source,)).fetchone()
    return bool(row and row[0])


def mark_baseline_done(source: str) -> None:
    db = connect()
    with db:
        db.execute("INSERT OR REPLACE INTO sources VALUES (?, 1)", (source,))
    db.close()


def save_company(db: sqlite3.Connection, source: str, company_key: str,
                 found: list[dict[str, Any]], baseline: bool = False) -> int:
    """Upsert one company's postings; close its postings that disappeared. Returns # new."""
    now = datetime.now(UTC).isoformat(timespec="seconds")
    new = 0
    with db:
        keys = set()
        for p in found:
            key = f"{source}|{p['url']}"
            keys.add(key)
            row = db.execute("SELECT 1 FROM postings WHERE key=?", (key,)).fetchone()
            if row is None:
                new += 1
                db.execute("INSERT INTO postings VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
                           (key, source, company_key, p["company"], p["title"], p["location"],
                            p.get("posted_at"), p.get("posted_text"), p["url"], now, now,
                            p.get("source") or source, int(baseline)))
            else:
                db.execute("""UPDATE postings SET company_key=?, company=?, title=?, location=?,
                              posted_at=COALESCE(?, posted_at), posted_text=?, last_seen=?, open=1
                              WHERE key=?""",
                           (company_key, p["company"], p["title"], p["location"], p.get("posted_at"),
                            p.get("posted_text"), now, key))
        if source == "careers":
            # Rows from when the whole discovery list was one company ("careers"): each moves to
            # its own company when seen again; close the ones no company has shown for a day.
            cutoff = (datetime.now(UTC) - timedelta(days=1)).isoformat(timespec="seconds")
            db.execute("UPDATE postings SET open=0 WHERE source='careers' AND company_key='careers' "
                       "AND open=1 AND last_seen<?", (cutoff,))
        if source in PARTIAL_SOURCES:
            # Each pass only reads the newest postings, so absence means nothing: close by age.
            cutoff = (datetime.now(UTC) - timedelta(days=WINDOW_DAYS)).isoformat(timespec="seconds")
            db.execute("UPDATE postings SET open=0 WHERE source=? AND company_key=? AND open=1 AND last_seen<?",
                       (source, company_key, cutoff))
            return new
        for (key,) in db.execute("SELECT key FROM postings WHERE source=? AND company_key=? AND open=1",
                                 (source, company_key)).fetchall():
            if key not in keys:
                db.execute("UPDATE postings SET open=0 WHERE key=?", (key,))
    return new


PARTIAL_SOURCES = {"linkedin", "workopolis", "eluta", "wellfound"}  # sources whose pass returns new postings only, not the full list


def save_ca_hiring(db: sqlite3.Connection, source: str, company_key: str, company: str, board: str,
                   jobs: list[tuple[str, str, str]]) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS ca_software_hiring (source TEXT, company_key TEXT, company TEXT,
        board_url TEXT, jobs INTEGER, example_title TEXT, example_location TEXT, example_url TEXT,
        first_seen TEXT, last_seen TEXT, PRIMARY KEY (source, company_key))""")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    title, location, url = jobs[0]
    with db:
        db.execute("""INSERT INTO ca_software_hiring VALUES (?,?,?,?,?,?,?,?,?,?)
                      ON CONFLICT (source, company_key) DO UPDATE SET company=excluded.company,
                      board_url=excluded.board_url, jobs=excluded.jobs, example_title=excluded.example_title,
                      example_location=excluded.example_location, example_url=excluded.example_url,
                      last_seen=excluded.last_seen""",
                   (source, company_key, company, board, len(jobs), title, location, url, now, now))


_DROP_PARAMS = re.compile(r"^(utm_\w+|nl|fr|src|source|ref|referrer|lever-source\w*|gh_src)$", re.I)
_COMPANY_SUFFIX = re.compile(r"\b(inc|llc|ltd|corp|corporation|co|company|technologies|technology|"
                             r"group|holdings|plc|gmbh|the)\b", re.I)


def norm_url(url: str) -> str:
    from urllib.parse import parse_qsl, urlencode, urlparse
    try:
        u = urlparse(url.strip())
    except ValueError:
        return url
    host = (u.hostname or "").lower().removeprefix("www.")
    path = u.path.rstrip("/").lower()
    params = dict(parse_qsl(u.query))
    # Same job, different link styles (board vs. embed vs. company site, /application suffix):
    # key on the ATS's own job id so every variant collapses to one row.
    m = re.search(r"/jobs/(\d{5,})", path)
    if "greenhouse.io" in host and m:
        return f"greenhouse:{m.group(1)}"
    if params.get("gh_jid", "").isdigit():
        return f"greenhouse:{params['gh_jid']}"
    m = re.search(r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", path)
    if m and ("lever.co" in host or "ashbyhq.com" in host):
        return f"{host.split('.')[-2]}:{m.group(1)}"
    if "myworkdayjobs.com" in host:
        path = re.sub(r"^/[a-z]{2}-[a-z]{2}(?=/)", "", path)  # drop /en-US locale prefix
    query = urlencode(sorted((k, v) for k, v in params.items() if not _DROP_PARAMS.match(k)))
    return f"{host}{path}?{query}"


def norm_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _COMPANY_SUFFIX.sub("", (text or "").lower()))


LIST_ORIGINS = set(LIST_SOURCES) | {"linkedin", "remoterocketship", "workopolis", "eluta", "jobbankca"}  # ATS rows win duplicates


def _city_key(location: str | None) -> str:
    """First place in a location ("Montréal, QC, Canada" -> "montreal"), accents dropped."""
    import unicodedata

    first = re.split(r"[,;|/(]| - ", location or "")[0]
    return re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", first.lower()).encode("ascii", "ignore").decode())


WINDOW_DAYS = 7  # US_/canada_internships.csv show postings from the last N days (run_group --days)


def _write(path: Path, rows: list[tuple]) -> None:
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["company", "title", "location", "posted_on", "posted", "found", "link", "source"])
        for when, company, title, location, posted_at, posted_text, first_seen, url, origin in rows:
            if when is None:
                posted_on, posted = "", "unknown (open before watcher started)"
            else:
                # Day-precision dates are stored as midnight UTC of the calendar day itself.
                posted_on = (when.astimezone(UTC).strftime("%Y-%m-%d") if _date_only(when, posted_text)
                             else when.astimezone(EASTERN).strftime("%Y-%m-%d %H:%M ET"))
                posted = age_text(posted_at, first_seen, posted_text)
            # When this poller first saw the posting (exact even when the source only gives a date).
            found = ago((datetime.now(UTC) - datetime.fromisoformat(first_seen)).total_seconds())
            w.writerow([company, title, location, posted_on, posted, found, url, origin])
    tmp.replace(path)


def _date_only(when: datetime, posted_text: str | None) -> bool:
    """Day-precision posting. Some sources send a bare date as midnight UTC with no posted_text;
    shown in Eastern time that would read as 8 pm the day before."""
    if posted_text:
        return True
    w = when.astimezone(UTC)
    return (w.hour, w.minute, w.second, w.microsecond) == (0, 0, 0, 0)


def posted_day(row: tuple) -> date:
    """The calendar day shown in posted_on, so the CSV order matches the dates people read."""
    when = row[0]
    return (when.astimezone(UTC) if _date_only(when, row[5]) else when.astimezone(EASTERN)).date()


def sort_time(row: tuple) -> datetime:
    """Sort key: the posting time, but for date-only postings (stored as midnight of that day)
    use the moment we first saw it when that falls on the posting day, so fresh finds rank on top."""
    when, posted_text, first_seen = row[0], row[5], row[6]
    if when is not None and _date_only(when, posted_text):
        seen = datetime.fromisoformat(first_seen)
        if timedelta(0) <= seen - when < timedelta(days=1):
            return seen
    return when or datetime.min.replace(tzinfo=UTC)


def write_csv() -> int:
    """US_internships.csv and canada_internships.csv = software internships posted in the last
    WINDOW_DAYS, newest first. Returns how many internships are in either file."""
    db = connect()
    rows = db.execute("""SELECT company, title, location, posted_at, posted_text, first_seen, url,
                                origin, baseline
                         FROM postings WHERE open=1""").fetchall()
    db.close()
    dated: dict[tuple[str, str, str], str] = {}
    for r in rows:
        if r[3]:
            k = (norm_text(r[0]), norm_text(r[1]), _city_key(r[2]))
            dated[k] = min(dated.get(k, r[3]), r[3])
    rows = [r if r[3] or not r[8] else
            (*r[:3], dated.get((norm_text(r[0]), norm_text(r[1]), _city_key(r[2]))), *r[4:]) for r in rows]
    # 1. One row per link: the ATS row when there is one (its date is the real posting date).
    rows.sort(key=lambda r: (r[7] in LIST_ORIGINS, r[5]))
    by_url: dict[str, tuple] = {}
    for r in rows:
        by_url.setdefault(norm_url(r[6]), r)
    # 2. Same company, title and city under different links (a repost, or another req with the
    #    same title): keep the newest, so a fresh posting is never hidden behind an old one
    #    (Dow Jones reposts each intern role; EA posts identical titles as separate reqs).
    rows = sorted(by_url.values(), key=lambda r: -datetime.fromisoformat(r[3] or r[5]).timestamp())
    seen_urls: set[str] = set()
    seen_jobs: set[tuple[str, str]] = set()
    cutoff = datetime.now(UTC) - timedelta(days=WINDOW_DAYS)
    out = []
    all_rows = []
    for company, title, location, posted_at, posted_text, first_seen, url, origin, baseline in rows:
        if not is_software_internship(title):
            continue
        # Same title in another city is another internship (DRW's Montreal and Chicago
        # "Software Developer Intern"), so the city is part of the key.
        url_key, job_key = norm_url(url), (norm_text(company), norm_text(title), _city_key(location))
        if url_key in seen_urls or job_key in seen_jobs:
            continue
        seen_urls.add(url_key)
        seen_jobs.add(job_key)
        if posted_at:
            when = datetime.fromisoformat(posted_at)
        elif not baseline:
            when = datetime.fromisoformat(first_seen)  # undated, but we saw it appear
        else:
            # Undated and already open before we started watching: age unknown.
            all_rows.append((None, company, title, location, posted_at, posted_text, first_seen, url, origin))
            continue
        all_rows.append((when, company, title, location, posted_at, posted_text, first_seen, url, origin))
        if when < cutoff:
            continue
        out.append((when, company, title, location, posted_at, posted_text, first_seen, url, origin))
    out.sort(key=lambda r: (posted_day(r), sort_time(r)), reverse=True)  # newest day at the top
    countries = [us_canada(r[3]) for r in out]
    # A posting with no clear country ("location not listed", "Remote (country unclear)") goes in both.
    _write(US_CSV_PATH, [r for r, c in zip(out, countries) if "US" in c or not c])
    _write(CA_CSV_PATH, [r for r, c in zip(out, countries) if "Canada" in c or not c])
    # Every open internship, newest first; undated ones (age unknown) at the bottom.
    all_rows.sort(key=lambda r: (r[0] is not None, posted_day(r) if r[0] else date.min, sort_time(r)),
                  reverse=True)
    _write(ALL_CSV_PATH, all_rows)
    write_ca_companies()
    return len(out)


CA_COMPANIES_CSV = OUT_DIR / "canada_software_companies.csv"
CA_HIRING_DAYS = 30  # a company stays on the list this long after its last Canadian software posting
_AGENCY_RE = re.compile(r"\b(staffing|recruit\w*|talent|placement|personnel|headhunt\w*|"
                        r"employment agency|executive search)\b", re.I)


def write_ca_companies() -> int:
    """canada_software_companies.csv: every company seen hiring for full-time software roles in
    Canada, with the careers page or ATS board we poll for its internships. Two sources: ATS
    boards with a Canadian software posting (ca_software_hiring) and companies `discover`
    found in LinkedIn's Canadian software searches. Staffing agencies are left out.

    discovered.csv: companies likely to hire software interns in Canada for summer 2027 that
    haven't posted one yet: they hired software interns in Canada before, or are hiring
    full-time software roles there now."""
    db = connect()
    _discover_tables(db)
    _careers_cache(db)
    db.execute("""CREATE TABLE IF NOT EXISTS ca_software_hiring (source TEXT, company_key TEXT, company TEXT,
        board_url TEXT, jobs INTEGER, example_title TEXT, example_location TEXT, example_url TEXT,
        first_seen TEXT, last_seen TEXT, PRIMARY KEY (source, company_key))""")
    cutoff = (datetime.now(UTC) - timedelta(days=CA_HIRING_DAYS)).isoformat(timespec="seconds")
    rows: dict[str, list[str]] = {}
    for source, key, company, board, jobs, title, loc, url, first, last in db.execute(
            "SELECT * FROM ca_software_hiring WHERE last_seen >= ? ORDER BY jobs DESC", (cutoff,)):
        name = company or key
        k = norm_text(name)
        if not k or _AGENCY_RE.search(name) or k in rows:
            continue
        board = board or (key if key.startswith("http") else "")
        rows[k] = [name, board, source, "polled on its ATS",
                   f"{jobs} Canadian software job{'s' * (jobs != 1)} on its board", f"{title} ({loc})", url,
                   first[:10], last[:10]]
    status_text = {"listed": "polled on its ATS", "careers": "careers page polled",
                   "searching": "finding its careers page", "new": "finding its careers page"}
    evidence = {"linkedin": "LinkedIn: hiring full-time software roles in Canada",
                "workopolis": "Indeed/Workopolis: hiring full-time software roles in Canada",
                "jobbank": "Job Bank: hiring full-time software roles in Canada",
                "newgrad": "New-grad lists: hiring new-grad software roles in Canada",
                "wellfound": "Wellfound: startup hiring full-time software roles in Canada"}
    placed: dict[str, tuple[str, str, str]] = {}  # every discovered company: (page, ats, polling status)
    for name, name_key, ats, slug, url, first, last, status, careers_url, source in db.execute(
            """SELECT d.name, d.name_key, d.ats, d.slug, d.url, d.first_seen, d.last_seen, d.status, c.careers_url,
                      d.source
               FROM discovered d LEFT JOIN careers_cache c ON c.company_key = 'discover:' || d.name_key"""):
        k = norm_text(name or "")
        if not k or _AGENCY_RE.search(name or ""):
            continue
        page = careers_url or url or (slug if (slug or "").startswith("http") else "")
        label = status_text.get(status or "", status or "")
        if status in ("searching", "new") and page:
            label = "guessed page polled; web search looking for a better one"
        placed.setdefault(k, (page, ats or "", label))
        # "lists" and "history" companies came from internship lists, not full-time software hiring.
        if source not in evidence:
            continue
        if k in rows:
            if not rows[k][1]:
                rows[k][1] = page
            continue
        rows[k] = [name, page, ats or "", label, evidence[source], "", "", (first or "")[:10], (last or "")[:10]]

    # Canadian software internships: a summer 2027 one takes the company off discovered.csv;
    # any other (last season's, a fall/winter co-op) shows it hires interns in Canada.
    _intern_history_table(db)
    hired: dict[str, tuple[str, str, str, str, str]] = {
        k: (company, title, location, url, term) for k, company, title, location, url, term in db.execute(
            "SELECT company_key, company, title, location, url, term FROM ca_intern_history")}
    with_intern: set[str] = set()
    for company, title, location, url, posted_at, is_open in db.execute(
            "SELECT company, title, location, url, posted_at, open FROM postings"):
        if not is_software_internship(title) or "Canada" not in us_canada(location):
            continue
        k = norm_text(company)
        if is_open and _summer_2027(title, url, posted_at):
            with_intern.add(k)
        else:
            hired.setdefault(k, (company, title, location, url, ""))
    db.close()

    out = sorted(rows.values(), key=lambda r: (not r[1], r[0].lower()))
    header = ["company", "careers_page", "ats", "status", "evidence", "example_job", "example_link",
              "first_seen", "last_seen"]
    _write_rows(CA_COMPANIES_CSV, header, out)

    waiting: list[list[str]] = []
    for k in (set(rows) | set(hired)) - with_intern:
        if k in hired and _AGENCY_RE.search(hired[k][0]):
            continue
        why = []
        if k in hired:
            term = hired[k][4]
            why.append(f"hired software interns in Canada{f' ({term})' if term else ''}")
        if k in rows:
            why.append("hiring full-time software roles in Canada now")
            name, page, ats, status = rows[k][:4]
        else:
            name = hired[k][0]
            page, ats, status = placed.get(k, ("", "", "polled through an ATS list or internship lists"))
        example_job, example_link = ((f"{hired[k][1]} ({hired[k][2]})", hired[k][3]) if k in hired
                                     else (rows[k][5], rows[k][6]))
        waiting.append([name, "; ".join(why), page, ats, status, example_job, example_link])
    waiting.sort(key=lambda r: (not r[1].startswith("hired"), r[0].lower()))
    _migrate_discover_log()
    _write_rows(DISCOVERED_CSV, ["company", "why", "careers_page", "ats", "status", "example_job", "example_link"],
                waiting)
    return len(out)


INTERN_HISTORY_LISTS = {  # last season's lists keep every posting, closed ones included
    "simplify2026": "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/.github/scripts/listings.json",
    "vanshb03_2026": "https://raw.githubusercontent.com/vanshb03/Summer2026-Internships/dev/.github/scripts/listings.json",
}
INTERN_HISTORY_REFRESH = 24 * 60 * 60  # seconds


def _intern_history_table(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS ca_intern_history (company_key TEXT PRIMARY KEY, company TEXT,
        title TEXT, location TEXT, url TEXT, term TEXT, posted_at TEXT, source TEXT)""")


async def refresh_intern_history(db: sqlite3.Connection, client: httpx.AsyncClient) -> int:
    """Companies that posted software internships in Canada last season (once a day)."""
    _intern_history_table(db)
    _discover_tables(db)
    last = db.execute("SELECT v FROM discover_state WHERE k='intern_history_at'").fetchone()
    if last and time.time() - float(last[0]) < INTERN_HISTORY_REFRESH:
        return 0
    found: dict[str, tuple[str, str, str, str, str, str | None, str]] = {}
    for source, url in INTERN_HISTORY_LISTS.items():
        try:
            r = await client.get(url, timeout=120)
            r.raise_for_status()
            items = r.json()
        except Exception as exc:  # noqa: BLE001 — one list failing never stops discovery
            log_line(f"{'discover':16} intern history {source}: {exc}")
            continue
        for x in items:
            company, title = (x.get("company_name") or "").strip(), (x.get("title") or "").strip()
            locations = [loc for loc in x.get("locations") or [] if "Canada" in us_canada(loc)]
            key = norm_text(company)
            if not key or not locations or not is_software_internship(title):
                continue
            ts = x.get("date_posted")
            posted = iso(datetime.fromtimestamp(ts, UTC)) if isinstance(ts, (int, float)) else None
            term = ", ".join(x.get("terms") or []) or (x.get("season") or "")
            if key not in found or (posted or "") > (found[key][5] or ""):
                found[key] = (company, title, locations[0], x.get("url") or "", term, posted, source)
    if not found:
        return 0
    with db:
        for key, row in found.items():
            db.execute("INSERT OR REPLACE INTO ca_intern_history VALUES (?,?,?,?,?,?,?,?)", (key, *row))
        db.execute("INSERT OR REPLACE INTO discover_state VALUES ('intern_history_at', ?)", (str(time.time()),))
    return len(found)


_OTHER_YEAR = re.compile(r"\b20(1\d|2[0-68-9])\b")
_SUMMER = re.compile(r"\b(summer|[eé]t[eé])\b", re.I)
_OTHER_TERM = re.compile(r"\b(fall|autumn|winter|spring|automne|hiver|printemps)\b", re.I)


SUMMER_2027_POSTED_FROM = "2026-05-01"  # earlier postings belong to an earlier hiring cycle


def _summer_2027(title: str, url: str | None = "", posted_at: str | None = None) -> bool:
    """Title or link says summer or 2027, names no other year, and isn't only another term
    ("Winter 2027"). Untermed titles don't count: Canadian co-ops posted in the fall are
    usually for winter, and lists keep old postings open."""
    from urllib.parse import unquote

    if posted_at and posted_at < SUMMER_2027_POSTED_FROM:
        return False
    text = f"{title} {unquote(url or '')}".replace("-", " ").replace("_", " ")
    if _OTHER_YEAR.search(text) and "2027" not in text:
        return False
    summer = bool(_SUMMER.search(text))
    return summer or ("2027" in text and not _OTHER_TERM.search(text))


def _write_rows(path: Path, header: list[str], rows: list[list[str]]) -> None:
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    tmp.replace(path)


def log_line(text: str) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}"
    print(line, flush=True)
    with WATCH_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


# --- per-company checks ---------------------------------------------------------------

def company_list(ats: str, max_tenants: int | None) -> list[dict[str, str]]:
    if ats in ("careers", "canada"):
        rows = careers_targets() if ats == "careers" else canada_targets()
        return rows[:max_tenants] if max_tenants else rows
    path = REPO / CONFIGS[ats]["csv"]
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    gone = _moved_away(ats)
    if gone:  # dead boards whose company moved to a board on another list (dead_boards.csv)
        rows = [r for r in rows if (r.get("url") or "").rstrip("/").lower() not in gone]
    if max_tenants:
        return rows[:max_tenants]
    # Plus boards that find_moved_boards.py discovered for companies whose listed board failed.
    if MOVED_CSV.exists():
        known = {(r.get("url") or "").rstrip("/").lower() for r in rows}
        with MOVED_CSV.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if r.get("new_ats") == ats and (r.get("url") or "").rstrip("/").lower() not in known:
                    rows.append({"name": r["name"], "slug": r.get("slug", ""), "url": r["url"]})
    return rows


_NO_PLACE = re.compile(r"^\s*(in[- ]?office|hybrid|on[- ]?site|remote|flexible|multiple locations|various|"
                       r"locations?|tbd|n/?a)?\s*$", re.I)


async def _greenhouse_offices(client: httpx.AsyncClient, slug: str, job_id: Any) -> str:
    try:
        r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}")
        r.raise_for_status()
    except httpx.HTTPError:
        return ""
    return " | ".join(o.get("location") or o.get("name") or "" for o in r.json().get("offices") or [])


async def check_greenhouse(client: httpx.AsyncClient, row: dict[str, str]) -> list[dict[str, Any]]:
    slug = CONFIGS["greenhouse"]["slug"](row)
    r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    r.raise_for_status()
    out = []
    for j in r.json().get("jobs", []):
        note_ca_software(j.get("title"), (j.get("location") or {}).get("name"), url=j.get("absolute_url"))
        loc = keep(j.get("title") or "", (j.get("location") or {}).get("name"))
        if (loc is None and is_software_internship(j.get("title") or "")
                and _NO_PLACE.match((j.get("location") or {}).get("name") or "")):
            # Some boards put the work mode ("In-Office", "Hybrid") where the city goes (Cloudflare);
            # the job's offices say where it is.
            offices = await _greenhouse_offices(client, slug, j.get("id"))
            if offices:
                loc = keep(j.get("title") or "", offices)
        if loc is not None:
            out.append({"title": j["title"].strip(), "location": loc, "url": j.get("absolute_url") or "",
                        "posted_at": iso(j.get("first_published") or j.get("updated_at"))})
    return out


WORKDAY_SEARCH_WORDS = ("intern", "co-op", "stagiaire", "student")  # fallback when no intern filter
WORKDAY_EXTRA_WORDS = ("software intern",)                          # catches mis-tagged internships
WORKDAY_KEYWORD_MAX = 200     # results read per keyword search
WORKDAY_FACET_MAX = 600       # results read through an internship filter
INTERN_FACET_RE = re.compile(r"intern|co-?op|student|stagi|apprenti|trainee|work term|summer", re.I)
FACET_PARAMS = ("workerSubType", "jobFamilyGroup", "timeType", "workerType", "jobType", "employeeType")


def _intern_facets(facets: list[dict[str, Any]]) -> dict[str, list[str]]:
    """{facetParameter: [value ids]} for facet values that mean internship / co-op / student."""
    found: dict[str, list[str]] = {}

    def walk(items: list[dict[str, Any]], param: str | None) -> None:
        for f in items or []:
            p = f.get("facetParameter") or param
            for v in f.get("values") or []:
                if v.get("values"):
                    walk([v], p)
                elif p in FACET_PARAMS and INTERN_FACET_RE.search(v.get("descriptor") or ""):
                    found.setdefault(p, []).append(v["id"])
    walk(facets, None)
    return found


async def check_workday(client: httpx.AsyncClient, row: dict[str, str]) -> list[dict[str, Any]]:
    m = URL_PATTERN.match((row.get("url") or "").strip().rstrip("/"))
    if not m:
        raise ValueError("unrecognized Workday URL")
    co, inst, site = m.group("company"), m.group("instance"), m.group("site")
    base = f"https://{co}.{inst}.myworkdayjobs.com"
    api = f"{base}/wday/cxs/{co}/{site}"
    out: dict[str, dict[str, Any]] = {}

    async def page(text: str, facets: dict[str, list[str]], offset: int) -> dict[str, Any]:
        for attempt in range(4):
            r = await client.post(f"{api}/jobs", json={"appliedFacets": facets, "limit": 20,
                                                      "offset": offset, "searchText": text})
            if r.status_code not in (403, 429, 500, 502, 503, 504):
                break
            await asyncio.sleep(2 * (attempt + 1) + secrets.randbelow(3))  # back off, jittered
        r.raise_for_status()
        return r.json()

    async def absorb(postings: list[dict[str, Any]]) -> None:
        for p in postings:
            path, title = p.get("externalPath") or "", (p.get("title") or "").strip()
            if not path or path in out or not is_software_internship(title):
                continue
            loc = p.get("locationsText") or ""
            if re.match(r"^\d+ Locations?$", loc.strip(), re.I):
                d = await client.get(api + path)
                if d.status_code == 200:
                    info = d.json().get("jobPostingInfo") or {}
                    loc = " | ".join(x for x in [info.get("location") or "",
                                                 *(info.get("additionalLocations") or [])] if x)
            label = location_label(loc)
            if label is not None:
                out[path] = {"title": title, "location": label, "url": f"{base}/{site}{path}",
                             "posted_at": workday_posted(p.get("postedOn")),
                             "posted_text": p.get("postedOn")}

    async def read_all(text: str, facets: dict[str, list[str]], cap: int) -> None:
        offset, total = 0, 20
        while offset < min(total, cap):
            payload = await page(text, facets, offset)
            total = int(payload.get("total") or 0)
            postings = payload.get("jobPostings") or []
            await absorb(postings)
            if not postings:
                break
            offset += 20

    first = await page("", {}, 0)
    for p in first.get("jobPostings") or []:  # first 20 jobs only: the rest are read by intern search
        note_ca_software(p.get("title"), p.get("locationsText"), url=f"{base}/{site}{p.get('externalPath') or ''}")
    if int(first.get("total") or 0) <= 20:
        await absorb(first.get("jobPostings") or [])  # small board: this page is every job
        return list(out.values())

    intern_facets = _intern_facets(first.get("facets") or [])
    if intern_facets:
        # Workday ANDs different facet parameters, so query each one separately.
        for param, ids in intern_facets.items():
            await read_all("", {param: ids}, WORKDAY_FACET_MAX)
        words = WORKDAY_EXTRA_WORDS
    else:
        words = WORKDAY_SEARCH_WORDS + WORKDAY_EXTRA_WORDS
    for text in words:
        await read_all(text, {}, WORKDAY_KEYWORD_MAX)
    return list(out.values())

    intern_facets = _intern_facets(first.get("facets") or [])
    if intern_facets:
        # Workday ANDs different facet parameters, so query each one separately.
        for param, ids in intern_facets.items():
            await read_all("", {param: ids}, WORKDAY_FACET_MAX)
        words = WORKDAY_EXTRA_WORDS
    else:
        words = WORKDAY_SEARCH_WORDS + WORKDAY_EXTRA_WORDS
    for text in words:
        await read_all(text, {}, WORKDAY_KEYWORD_MAX)
    return list(out.values())


async def check_scraper(ats: str, row: dict[str, str], timeout: float) -> list[dict[str, Any]]:
    cfg = CONFIGS[ats]
    slug = cfg["slug"](row)
    if not slug:
        raise ValueError("no usable slug")
    kwargs = cfg["kwargs"](row) if cfg.get("kwargs") else {}
    scraper = cfg["scraper"](slug, timeout=timeout, **kwargs)
    scraper.include_descriptions = ats in NEEDS_DETAIL
    jobs = await scraper.afetch()
    # Some boards (iCIMS for many tenants) list jobs without a location; it's only on the
    # detail page. Fetch details just for software-internship titles that lack one.
    need = [j for j in jobs if not j.location and not j.country_iso and is_software_internship(j.title)]
    if need and hasattr(scraper, "_enrich_detail") and hasattr(scraper, "make_fetcher"):
        async with scraper.make_fetcher() as fetch:
            sem = asyncio.Semaphore(4)
            await asyncio.gather(*(scraper._enrich_detail(fetch, sem, j) for j in need),
                                 return_exceptions=True)
    return _from_jobs(jobs)


_MD_LINK = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")
_HTML_LINK = re.compile(r'<a href="([^"]+)"')
_TAGS = re.compile(r"<[^>]+>|\*\*")


def _cell_text(cell: str) -> str:
    cell = _MD_LINK.sub(lambda m: m.group(1), cell)
    return _TAGS.sub("", cell).strip()


def _cell_link(cell: str) -> str | None:
    # Badges look like [![Apply](https://img.shields.io/...)](https://real-job-link): take the
    # outer link, never an image URL.
    links = [u for _t, u in _MD_LINK.findall(cell)]
    links += re.findall(r"\]\((https?://[^)\s]+)\)", cell)
    links = [u for u in links if not re.search(r"img\.shields\.io|\.(svg|png|gif)(\?|$)", u, re.I)]
    if links:
        return links[-1]
    h = _HTML_LINK.search(cell)
    return h.group(1) if h else None


def _relative_date(text: str) -> str | None:
    """'Oct 05' (current year) or '2d' / '5h' / '1w' / '0d' → ISO timestamp."""
    text = (text or "").strip()
    now = datetime.now(UTC)
    m = re.fullmatch(r"(\d+)\s*([mhdw])", text, re.I)
    if m:
        n, unit = int(m.group(1)), m.group(2).lower()
        delta = {"m": timedelta(minutes=n), "h": timedelta(hours=n), "d": timedelta(days=n),
                 "w": timedelta(weeks=n)}[unit]
        return (now - delta).isoformat(timespec="seconds")
    for fmt, value in (("%b %d, %Y", text), ("%b %d %Y", f"{text} {now.year}")):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=UTC)
            break
        except ValueError:
            continue
    else:
        return None
    if dt > now + timedelta(days=1):  # "Dec 30" seen in January → last year
        dt = dt.replace(year=now.year - 1)
    return dt.isoformat(timespec="seconds")


async def check_list(source: str, timeout: float) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=max(timeout, 60), follow_redirects=True) as client:
        r = await client.get(LIST_SOURCES[source])
        r.raise_for_status()
    out: list[dict[str, Any]] = []
    if source == "zshah101":
        for x in csv.DictReader(r.text.splitlines()):
            loc = keep(x.get("title") or "", x.get("location") or "")
            if loc is None or not x.get("url"):
                continue
            out.append({"company": x.get("company") or "", "title": x["title"].strip(), "location": loc,
                        "url": x["url"], "posted_at": iso(x.get("posted_at") or x.get("first_seen_at"))})
        return out
    if source == "hansonchen":
        data = r.json()
        items = data if isinstance(data, list) else next((v for v in data.values() if isinstance(v, list)), [])
        for x in items:
            loc = keep(x.get("title") or "", x.get("location") or "")
            if loc is None or not x.get("url"):
                continue
            out.append({"company": x.get("company") or "", "title": x["title"].strip(), "location": loc,
                        "url": x["url"], "posted_at": iso(x.get("posted_date") or x.get("first_seen")),
                        "posted_text": "day precision"})
        return out
    if source == "negarprh":
        # | Company | Role | Location | Apply | Date Posted |  ("↳" = same company as above)
        last = ""
        for line in r.text.splitlines():
            if not line.startswith("|") or set(line.replace("|", "").strip()) <= set("-: "):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 5 or _cell_text(cells[0]).lower() == "company":
                continue
            company = _cell_text(cells[0]).strip("* ")
            company = last if company in ("", "↳") else company
            last = company
            title, location, link = _cell_text(cells[1]), _cell_text(cells[2]), _cell_link(cells[3])
            loc = keep(title, location)
            if loc is None or not link:
                continue
            out.append({"company": company, "title": title, "location": loc, "url": link,
                        "posted_at": _relative_date(_cell_text(cells[4])), "posted_text": "day precision"})
        return out
    if source == "interndock":
        import html as _html
        # <h3>Company</h3><ul><li>Title — [<strong>Just posted</strong> —] <a href=URL>Apply</a>
        #   — Location <em>(added Sep 24)</em></li>...</ul>
        for company, block in re.findall(r"<h3>(.*?)</h3><ul>(.*?)</ul>", r.text, re.S):
            company = _html.unescape(re.sub(r"<[^>]+>", "", company)).strip()
            for li in re.findall(r"<li>(.*?)</li>", block, re.S):
                link = re.search(r'<a href="([^"]+)"', li)
                if not link:
                    continue
                text = _html.unescape(re.sub(r"<[^>]+>", "", li))
                parts = [x.strip() for x in text.split(" — ")]
                title = parts[0]
                location = parts[-1] if len(parts) >= 3 else ""
                added = re.search(r"\(added ([A-Z][a-z]{2} \d{1,2})\)", text)
                location = re.sub(r"\s*\(added [^)]*\)\s*$", "", location)
                loc = keep(title, location)
                if loc is None:
                    continue
                posted = (datetime.now(UTC).isoformat(timespec="seconds") if "Just posted" in text
                          else _relative_date(added.group(1)) if added else None)
                out.append({"company": company, "title": title, "location": loc,
                            "url": _html.unescape(link.group(1)), "posted_at": posted,
                            "posted_text": "day precision"})
        return out
    if source == "applyguy":
        data = r.json()
        items = data if isinstance(data, list) else next((v for v in data.values() if isinstance(v, list)), [])
        for x in items:
            loc = keep(x.get("title") or "", x.get("location") or "")
            link = x.get("listingUrl") or x.get("url")  # direct employer link, not their apply redirect
            if loc is None or not link:
                continue
            out.append({"company": x.get("company") or "", "title": x["title"].strip(), "location": loc,
                        "url": link, "posted_at": iso(x.get("posted")), "posted_text": "day precision"})
        return out
    if source == "earlycareerradar":
        # The page embeds its whole list as JSON inside a Next.js payload string.
        for chunk in re.findall(r'self\.__next_f\.push\(\[1,("(?:[^"\\]|\\.)*")\]\)', r.text):
            text = json.loads(chunk)
            i = text.find('"initialJobs":')
            if i < 0:
                continue
            jobs, _ = json.JSONDecoder().raw_decode(text[i + len('"initialJobs":'):])
            for x in jobs:
                loc = keep(x.get("title") or "", x.get("location") or "")
                if loc is None or not x.get("applyUrl"):
                    continue
                out.append({"company": x.get("company") or "", "title": x["title"].strip(), "location": loc,
                            "url": x["applyUrl"], "posted_at": iso(x.get("postedAt")),
                            "posted_text": "day precision"})
            break
        return out
    if source in ("simplify", "vanshb03"):
        for x in r.json():
            if not x.get("active") or x.get("is_visible") is False:
                continue
            loc = keep(x.get("title") or "", " | ".join(x.get("locations") or []))
            if loc is None or not x.get("url"):
                continue
            posted = x.get("date_posted")
            out.append({"company": x.get("company_name") or "", "title": x["title"].strip(),
                        "location": loc, "url": x["url"],
                        "posted_at": datetime.fromtimestamp(posted, UTC).isoformat(timespec="seconds")
                        if isinstance(posted, (int, float)) else None})
        return out
    # Markdown tables: jobright (Company | Job Title | Location | Work Model | Date Posted)
    # and speedyapply (Company | Position | Location | Salary | Posting | Age).
    last_company = ""
    for line in r.text.splitlines():
        if not line.startswith("|") or set(line.replace("|", "").strip()) <= set("-: "):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4 or _cell_text(cells[0]).lower() == "company":
            continue
        company = _cell_text(cells[0])
        if company in ("↳", ""):
            company = last_company
        last_company = company
        title, location = _cell_text(cells[1]), _cell_text(cells[2])
        if source == "jobright":
            link, posted = _cell_link(cells[1]), _relative_date(_cell_text(cells[-1]))
        else:
            link, posted = _cell_link(cells[4]) if len(cells) > 4 else None, _relative_date(_cell_text(cells[-1]))
        loc = keep(title, location)
        if loc is not None and link:
            # These lists give whole days only ("Oct 05", "2d"): show ages in days.
            out.append({"company": company, "title": title, "location": loc, "url": link,
                        "posted_at": posted, "posted_text": "day precision"})
    return out


JOBBANK_TERMS = ("intern", "co-op", "software", "stagiaire")


async def jobbank_search(term: str, timeout: float, max_pages: int = 10) -> list[Any]:
    """Job Bank Canada jobs matching one search term, newest first."""
    from ats_scrapers.scrapers.jobbankca import SEARCH_URL, JobBankCAScraper

    class _Search(JobBankCAScraper):
        async def _fetch_page(self, client, sem, page):  # noqa: ANN001
            async with sem:
                r = await client.get(SEARCH_URL, params={"searchstring": term, "sort": "M", "page": page})
            r.raise_for_status()
            return r.text

    return await _Search("jobbankca", timeout=timeout, include_descriptions=False, max_pages=max_pages).afetch()


async def check_jobbank(timeout: float) -> list[dict[str, Any]]:
    """Job Bank Canada has ~60k jobs; search only intern/co-op/software terms instead."""
    results = await asyncio.gather(*(jobbank_search(t, timeout) for t in JOBBANK_TERMS), return_exceptions=True)
    jobs = {str(j.url): j for res in results if isinstance(res, list) for j in res}
    if not jobs and all(isinstance(r, Exception) for r in results):
        raise results[0]
    return _from_jobs(list(jobs.values()), with_company=True)


REMOTEROCKETSHIP = "https://www.remoterocketship.com/remote-jobs/"
# Each page shows the newest 20 matches (it has no paging without a login), so several narrow
# searches together cover everything new between two passes.
REMOTEROCKETSHIP_SEARCHES = (
    {"employmentType": "internship", "locations": "Canada"},
    {"employmentType": "internship", "locations": "United States"},
    {"employmentType": "internship"},
    {"jobTitle": "software engineer intern"}, {"jobTitle": "software developer intern"},
    {"jobTitle": "co-op"}, {"jobTitle": "data intern"}, {"jobTitle": "machine learning intern"},
)


async def check_remoterocketship(timeout: float) -> list[dict[str, Any]]:
    """Remote Rocketship: newest internships, read from the page's embedded Next.js data."""
    out: dict[str, dict[str, Any]] = {}
    failed = 0
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/128"}) as client:
        for params in REMOTEROCKETSHIP_SEARCHES:
            r = await client.get(REMOTEROCKETSHIP, params=params)
            await asyncio.sleep(2)
            m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
            if r.status_code != 200 or not m:
                failed += 1
                continue
            for x in json.loads(m.group(1))["props"]["pageProps"].get("initialJobOpenings") or []:
                title, url = (x.get("roleTitle") or "").strip(), x.get("url") or ""
                place = ", ".join(v for v in (x.get("locationCity"), x.get("location")) if v)
                loc = keep(title, place)
                if loc is None or not url or x.get("dateDeleted"):
                    continue
                out[url] = {"company": ((x.get("company") or {}).get("name") or "").strip(), "title": title,
                            "location": loc, "url": url, "posted_at": iso(x.get("created_at"))}
    if failed == len(REMOTEROCKETSHIP_SEARCHES):
        raise RuntimeError("remoterocketship: no page had job data; the site probably changed")
    return list(out.values())


def _ca_board_location(location: str) -> str:
    """Canada-only boards give "Ottawa ON" / "Calgary, AB" / "Work Remotely": name the country."""
    location = re.sub(r"\s+", " ", location or "").strip()
    if not location or re.fullmatch(r"(work )?remote(ly)?", location, re.I):
        return "Remote, Canada"
    location = re.sub(r"(?<!,)\s+(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)$", r", \1", location)
    return location if re.search(r"canada", location, re.I) else f"{location}, Canada"


def _ca_board_item(company: str, title: str, location: str, url: str, posted_at: str | None) -> dict[str, Any] | None:
    location = _ca_board_location(location)
    note_ca_software(title, location, "CA", url)
    loc = keep(title, location, "CA")
    if loc is None:
        return None
    return {"company": company, "title": title, "location": loc, "url": url, "posted_at": posted_at}


# Workopolis is Indeed's Canadian site: the same postings (Indeed job keys) without Indeed's
# Cloudflare wall. httpcloak's browser-like TLS gets the server-rendered page; plain httpx is 403.
# /search params: q, l, t (posted within N days), jt (job type key), cursor (from pageCursors).
WORKOPOLIS_SEARCH = "https://www.workopolis.com/search"
WORKOPOLIS_INTERN_TYPE = "VDTG7"  # "Internship / Co-op"
WORKOPOLIS_DAYS = "3"  # covers a laptop asleep overnight; repeats are deduplicated by URL
WORKOPOLIS_SEARCHES = (
    *(({"q": q, "jt": WORKOPOLIS_INTERN_TYPE}) for q in (
        "software", "developer", "engineer", "data", "computer", "IT", "machine learning",
        "web", "cloud", "security", "QA", "informatique", "développeur", "logiciel")),
    # Co-ops are often typed Full-time or Temporary: catch them by title too.
    *(({"q": q}) for q in ("software intern", "software co-op", "developer co-op", "developer intern",
                           "data intern", "engineering co-op", "stagiaire informatique",
                           "stagiaire développeur", "stage logiciel")),
)
WORKOPOLIS_MAX_PAGES = 20  # 25 jobs a page


async def workopolis_pages(search: dict[str, str], timeout: float):  # noqa: ANN201 — async generator
    """Each result page's job list for one Workopolis search, following its page cursors.
    Yields None once when the first page has no job data (blocked or the page changed)."""
    from urllib.parse import urlencode

    import httpcloak

    def page_props(params: dict[str, str]) -> dict[str, Any] | None:
        r = httpcloak.get(f"{WORKOPOLIS_SEARCH}?{urlencode(params)}", timeout=int(timeout * 1000))
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
        if r.status_code != 200 or not m:
            return None
        return json.loads(m.group(1)).get("props", {}).get("pageProps")

    params = dict(search)
    for page in range(1, WORKOPOLIS_MAX_PAGES + 1):
        props = await asyncio.to_thread(page_props, params)
        await asyncio.sleep(3.0)
        if props is None or "jobs" not in props:
            if page == 1:
                yield None
            return
        yield props["jobs"]
        cursor = (props.get("pageCursors") or {}).get(str(page + 1))
        if not cursor:
            return
        params = {**search, "cursor": cursor}


async def check_workopolis(timeout: float) -> list[dict[str, Any]]:
    """Workopolis (Indeed Canada inventory): internships posted in the last few days, every page."""
    import html as _html

    out: dict[str, dict[str, Any]] = {}
    failed = 0
    for search in WORKOPOLIS_SEARCHES:
        async for jobs in workopolis_pages({**search, "l": "canada", "t": WORKOPOLIS_DAYS}, timeout):
            if jobs is None:
                failed += 1
                break
            for j in jobs:
                title, key = _html.unescape(j.get("title") or "").strip(), j.get("jobKey")
                if not title or not key:
                    continue
                posted = j.get("dateOnIndeed")
                url = f"https://www.workopolis.com/jobsearch/viewjob/{key}"
                item = _ca_board_item(_html.unescape(j.get("company") or "").strip(), title,
                                      j.get("location") or "", url,
                                      iso(datetime.fromtimestamp(posted / 1000, UTC)) if posted else None)
                if item:
                    out[url] = item
    if failed == len(WORKOPOLIS_SEARCHES):
        raise RuntimeError("workopolis: no search returned job data; blocked or the page changed")
    return list(out.values())


# Eluta crawls Canadian employers' own career pages (big and small), newest first with sort=post.
# ~50 requests in 3 min sends the IP to a reCAPTCHA page (2026-10-08): few searches, slow pages,
# hourly (MIN_GAP), and a pass stops at the first challenge, keeping what it read.
ELUTA_SEARCH = "https://www.eluta.ca/search"
ELUTA_SEARCHES = ("software intern", "software co-op", "developer co-op", "data intern", "IT co-op",
                  "stagiaire informatique")
ELUTA_MAX_PAGES = 5  # 10 jobs a page
ELUTA_PAGE_GAP = 6.0
ELUTA_MAX_AGE = timedelta(days=2)
_ELUTA_TITLE = re.compile(r'lk-job-title"\s+data-url="([^"?]+)[^"]*"[^>]*title="([^"]*)"')
_ELUTA_EMPLOYER = re.compile(r'class="employer[^"]*"[^>]*title="See all jobs at ([^"]*)"')
_ELUTA_LOCATION = re.compile(r'<span class="location">(.*?)</span>\s*(?:<span class="description"|$)', re.S)
_ELUTA_AGE = re.compile(r'class="lk lastseen"[^>]*>\s*(\d+)\s*(minute|hour|day|week|month)s?\s+ago', re.I)


async def check_eluta(timeout: float) -> list[dict[str, Any]]:
    """Eluta.ca: Canadian internships indexed in the last few days, newest first."""
    import html as _html

    out: dict[str, dict[str, Any]] = {}
    now = datetime.now(UTC)
    blocked = False
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/129.0 Safari/537.36"}) as client:
        for term in ELUTA_SEARCHES:
            for page in range(1, ELUTA_MAX_PAGES + 1):
                r = await client.get(ELUTA_SEARCH, params={"q": term, "sort": "post", "pg": page})
                await asyncio.sleep(ELUTA_PAGE_GAP)
                if "/sandbox" in str(r.url) or r.status_code != 200:
                    blocked = True
                    break
                cards = r.text.split('class="organic-job')[1:]
                if not cards:
                    break
                too_old = False
                for card in cards:
                    card = re.sub(r"<!--.*?-->", "", card, flags=re.S)
                    t, emp, loc, age = (_ELUTA_TITLE.search(card), _ELUTA_EMPLOYER.search(card),
                                        _ELUTA_LOCATION.search(card), _ELUTA_AGE.search(card))
                    if not t:
                        continue
                    posted = None
                    if age:
                        n, unit = int(age.group(1)), age.group(2).lower()
                        delta = {"minute": timedelta(minutes=n), "hour": timedelta(hours=n), "day": timedelta(days=n),
                                 "week": timedelta(weeks=n), "month": timedelta(days=30 * n)}[unit]
                        too_old = too_old or delta > ELUTA_MAX_AGE
                        posted = (now - delta).isoformat(timespec="seconds")
                    url = f"https://www.eluta.ca/{t.group(1)}"
                    item = _ca_board_item(_html.unescape(emp.group(1)).strip() if emp else "",
                                          _html.unescape(t.group(2)).strip(),
                                          _html.unescape(re.sub(r"<[^>]+>", " ", loc.group(1))) if loc else "",
                                          url, posted)
                    if item:
                        out[url] = item
                if too_old or len(cards) < 10:
                    break
            if blocked:
                break
    if blocked and not out:
        raise RuntimeError("eluta: sent to its reCAPTCHA page; the next hourly pass retries")
    if blocked:
        log_line(f"{'eluta':16} sent to its reCAPTCHA page; keeping the {len(out)} read so far")
    return list(out.values())


CGI_LIST = "https://cgi.njoyn.com/CORP/xweb/xweb.asp?NTKN=c&clid=21001&Page=JobListing&lang=1&pn={page}"
CGI_JOB = "https://cgi.njoyn.com/CORP/xweb/xweb.asp?NTKN=c&clid=21001&Page=JobDetails&Jobid={jid}&BRID={brid}&lang=1"
_CGI_ROW = re.compile(
    r"Jobid=([^&'\"]+)&amp;BRID=(\d+)&amp;lang=1'>[^<]*</a></td><td>([^<]*)</td><td>([^<]*)</td>"
    r"<td>([^<]*)</td><td name='CountryCell'>([^<]*)</td>")


async def check_cgi(timeout: float) -> list[dict[str, Any]]:
    """CGI posts on Njoyn behind Radware bot protection; httpcloak's browser-like TLS gets
    through. Read only the listing pages (50 jobs each), gently, not every job's detail page."""
    import html as _html

    import httpcloak

    def get(page: int) -> str:
        r = httpcloak.get(CGI_LIST.format(page=page), timeout=int(timeout * 1000))
        if r.status_code != 200 or "perfdrive" in r.text[:20000]:
            raise RuntimeError(f"CGI blocked or unavailable (HTTP {r.status_code})")
        return r.text

    first = await asyncio.to_thread(get, 1)
    total = int((re.search(r"Search Results \((\d+)\)", first) or [0, 0])[1])
    if not total:
        raise RuntimeError("cgi: 0 jobs total; scraper is probably broken")
    pages = [first]
    for page in range(2, -(-total // 50) + 1):
        await asyncio.sleep(1.0)  # one page per second keeps Radware calm
        pages.append(await asyncio.to_thread(get, page))
    out, seen = [], set()
    for text in pages:
        for jid, brid, title, _category, city, country in _CGI_ROW.findall(text):
            if jid in seen:
                continue
            seen.add(jid)
            title = _html.unescape(title).strip()
            location = ", ".join(x for x in (_html.unescape(city).strip(), country.strip()) if x)
            loc = keep(title, location)
            if loc is not None:
                out.append({"company": "CGI", "title": title, "location": loc,
                            "url": CGI_JOB.format(jid=jid, brid=brid), "posted_at": None})
    return out


SHOPIFY_CAREERS = "https://www.shopify.com/careers"
# React Router turbo-stream sentinels (hole, NaN, -inf, -0, null, +inf, undefined).
_TURBO_SPECIAL = {-1: None, -2: None, -3: None, -4: 0, -5: None, -6: None, -7: None}


def _turbo_decode(values: list[Any]) -> Any:
    """Rebuild the object graph from React Router's flat turbo-stream array: objects are
    {"_<key index>": <value index>}, arrays are lists of value indexes."""
    memo: dict[int, Any] = {}

    def hyd(i: Any) -> Any:
        if not isinstance(i, int) or isinstance(i, bool):
            return i
        if i < 0:
            return _TURBO_SPECIAL.get(i)
        if i in memo:
            return memo[i]
        v = values[i]
        if isinstance(v, list):
            if v and isinstance(v[0], str) and len(v[0]) == 1:  # typed value (date, promise, ...)
                memo[i] = v
                return v
            out: list[Any] = []
            memo[i] = out
            out.extend(hyd(x) for x in v)
            return out
        if isinstance(v, dict):
            obj: dict[str, Any] = {}
            memo[i] = obj
            for k, x in v.items():
                obj[values[int(k[1:])]] = hyd(x)
            return obj
        memo[i] = v
        return v

    return hyd(0)


def _shopify_location(title: str, location: str) -> str:
    """Shopify is remote-first: most roles say just "Americas", so take the country from
    the title ("US Software Engineering Internships") when it names one."""
    if location.lower() != "americas":
        return location
    if re.search(r"\b(US|U\.S\.|USA|United States)\b", title):
        return "Remote - United States"
    if re.search(r"\b(Canada|Canadian)\b", title, re.I):
        return "Remote - Canada"
    return "Remote, Canada / United States"


async def check_shopify(timeout: float) -> list[dict[str, Any]]:
    """Shopify runs Ashby privately (no public job board), so read the job list that
    shopify.com/careers embeds in its page as a React Router turbo-stream."""
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as client:
        r = await client.get(SHOPIFY_CAREERS)
        r.raise_for_status()
    m = re.search(r'streamController\.enqueue\(("(?:[^"\\]|\\.)*")\)', r.text)
    if not m:
        raise RuntimeError("Shopify careers page has no embedded job data")
    root = _turbo_decode(json.loads(json.loads(m.group(1))))
    loader = next((v for k, v in (root.get("loaderData") or {}).items()
                   if k.endswith("careers") and isinstance(v, dict) and "jobPostingsWithJobs" in v), None)
    if loader is None:
        raise RuntimeError("Shopify careers data moved: no jobPostingsWithJobs")
    if not loader["jobPostingsWithJobs"]:
        raise RuntimeError("shopify: 0 jobs total; scraper is probably broken")
    out = []
    for entry in loader["jobPostingsWithJobs"]:
        p = entry.get("jobPosting") or {}
        title = (p.get("title") or "").strip()
        loc = keep(title, _shopify_location(title, (p.get("locationName") or "").strip()))
        if loc is not None:
            out.append({"company": "Shopify", "title": title, "location": loc,
                        "url": p.get("externalLink") or SHOPIFY_CAREERS,
                        "posted_at": iso(p.get("publishedDate"))})
    return out


LINKEDIN_SEARCH = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
# LinkedIn 429s an IP after ~60 guest requests in a row (discover shares that budget).
# The guest search ignores sortBy and returns relevance order, so a new posting can sit
# pages deep. Instead each search asks only for postings since the previous good pass
# (f_TPR=r<seconds>), which is small enough to read to the end: ~1 request per search.
# Keywords without an internship word get LinkedIn's Internship experience filter (f_E=1);
# the rest don't, since co-ops are often tagged Entry level or Full-time.
_CA, _US = "Canada", "United States"
LINKEDIN_SEARCHES = [
    (_CA, "software intern"), (_US, "software engineer intern"),
    (_CA, "software engineer intern"), (_CA, "developer co-op"), (_CA, "software co-op"),
    (_CA, "stagiaire développeur"), (_CA, "stage informatique"), (_CA, "stagiaire logiciel"),
    (_CA, "machine learning intern"), (_CA, "AI intern"), (_CA, "data intern"),
    (_CA, "data science intern"), (_CA, "data engineer intern"), (_CA, "QA intern"),
    (_CA, "devops intern"), (_CA, "cloud intern"), (_CA, "full stack intern"),
    (_CA, "frontend intern"), (_CA, "backend intern"), (_CA, "web developer intern"),
    (_CA, "mobile developer intern"), (_CA, "embedded software intern"), (_CA, "security intern"),
    ("Montreal, Quebec, Canada", "software"), ("Montreal, Quebec, Canada", "développeur"),
    ("Toronto, Ontario, Canada", "software"), ("Vancouver, British Columbia, Canada", "software"),
    ("Ottawa, Ontario, Canada", "software"), ("Waterloo, Ontario, Canada", "software"),
    ("Calgary, Alberta, Canada", "software"), ("Quebec City, Quebec, Canada", "logiciel"),
    ("Edmonton, Alberta, Canada", "software"),
    (_US, "software intern"), (_US, "machine learning intern"), (_US, "software co-op"),
]
LINKEDIN_MAX_PAGES = 5  # 10 cards per page; only a long gap (laptop asleep) needs more than 2
LINKEDIN_OVERLAP = 10 * 60  # seconds re-read before the previous pass, so nothing falls between
LINKEDIN_MIN_WINDOW, LINKEDIN_MAX_WINDOW = 30 * 60, 24 * 60 * 60
_LI_INTERN_WORD = re.compile(r"\b(intern|internship|co-?op|stagiaire|stage)\b", re.I)
_LI_CARD = re.compile(r"<li>(.*?)</li>", re.S)
_LI_TITLE = re.compile(r'base-search-card__title">\s*(.*?)\s*<', re.S)
_LI_COMPANY = re.compile(r'base-search-card__subtitle">.*?>\s*(.*?)\s*<', re.S)
_LI_LOCATION = re.compile(r'job-search-card__location">\s*(.*?)\s*<', re.S)
_LI_DATE = re.compile(r'datetime="([^"]+)"')
_LI_URL = re.compile(r'href="(https://[a-z.]*linkedin\.com/jobs/view/[^"?]+)')


async def check_linkedin(timeout: float) -> list[dict[str, Any]]:
    """LinkedIn's logged-out job search (no account), Canada + US, posted since the last
    good pass. Returns only what this pass saw: LinkedIn rows are closed by age, not absence
    (see save_company), so a pass cut short by a block still saves what it read."""
    import html as _html

    db = connect()
    db.execute("CREATE TABLE IF NOT EXISTS discover_state (k TEXT PRIMARY KEY, v TEXT)")
    row = db.execute("SELECT v FROM discover_state WHERE k='linkedin_last_ok'").fetchone()
    started = datetime.now(UTC)
    since = (started - datetime.fromisoformat(row[0])).total_seconds() if row else LINKEDIN_MAX_WINDOW
    window = int(min(max(since + LINKEDIN_OVERLAP, LINKEDIN_MIN_WINDOW), LINKEDIN_MAX_WINDOW))

    out: dict[str, dict[str, Any]] = {}
    total_cards = requests = 0
    blocked = None
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for location, term in LINKEDIN_SEARCHES:
            params = {"keywords": term, "location": location, "f_TPR": f"r{window}"}
            if not _LI_INTERN_WORD.search(term):
                params["f_E"] = "1"
            for page in range(LINKEDIN_MAX_PAGES):
                try:
                    text = await linkedin_get(client, LINKEDIN_SEARCH, {**params, "start": page * 10}, tries=3)
                except RuntimeError as exc:
                    blocked = exc
                    break
                requests += 1
                await asyncio.sleep(2.0)
                cards = _LI_CARD.findall(text or "")
                total_cards += len(cards)
                for card in cards:
                    title, url = _LI_TITLE.search(card), _LI_URL.search(card)
                    if not title or not url:
                        continue
                    link = re.sub(r"^https://[a-z]+\.linkedin", "https://www.linkedin", url.group(1))
                    title_text = _html.unescape(title.group(1)).strip()
                    loc_m, co_m, date_m = _LI_LOCATION.search(card), _LI_COMPANY.search(card), _LI_DATE.search(card)
                    loc = keep(title_text, _html.unescape(loc_m.group(1)).strip() if loc_m else None)
                    if loc is None:
                        continue
                    # LinkedIn gives only the day; posted_text marks the date as day-precision.
                    out[link] = {"company": _html.unescape(co_m.group(1)).strip() if co_m else "",
                                 "title": title_text, "location": loc, "url": link,
                                 "posted_at": iso(date_m.group(1)) if date_m else None,
                                 "posted_text": date_m.group(1) if date_m else None}
                if len(cards) < 10:
                    break
            if blocked:
                break
    if not blocked and (total_cards or window < 3 * 60 * 60):
        # A blocked pass leaves the mark where it was, so the next window covers its gap.
        with db:
            db.execute("INSERT OR REPLACE INTO discover_state VALUES ('linkedin_last_ok', ?)",
                       (started.isoformat(timespec="seconds"),))
    db.close()
    if blocked and not out:
        raise RuntimeError(f"linkedin: 429 blocked after {requests} requests ({blocked})")
    if blocked:
        log_line(f"{'linkedin':16} blocked after {requests} requests; keeping the {len(out)} read so far")
    elif not total_cards and window >= 3 * 60 * 60:
        raise RuntimeError("linkedin: 0 jobs across every search; page layout probably changed")
    return list(out.values())


JANESTREET_FEED = "https://www.janestreet.com/jobs/main.json"
JANESTREET_CITIES = {"NYC": "New York, NY, United States", "LDN": "London, United Kingdom",
                     "HKG": "Hong Kong", "SGP": "Singapore", "AMS": "Amsterdam, Netherlands"}
# Some Jane Street titles use Lisu letters that look Latin ("ꓟachine ꓡearning"); map them back.
_LISU = str.maketrans("ꓐꓑꓒꓓꓔꓖꓗꓙꓚꓜꓝꓟꓠꓡꓢꓣꓦꓧꓪꓫꓬꓮꓰꓲꓳꓴ", "BPdDTGKJCZFMNLSRVHWXYAEIOU")


async def check_janestreet(timeout: float) -> list[dict[str, Any]]:
    """Jane Street's careers feed. Internship titles carry no intern word ("Data Engineer");
    the internship is in a separate `availability` field, so add it to the title."""
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as client:
        r = await client.get(JANESTREET_FEED)
        r.raise_for_status()
    jobs = r.json()
    if not jobs:
        raise RuntimeError("janestreet: 0 jobs total; feed probably changed")
    out = []
    for j in jobs:
        avail = j.get("availability") or ""
        title = (j.get("position") or "").translate(_LISU).strip()
        if not re.search(r"intern|co-?op", avail, re.I):
            continue
        full = f"{title} ({avail})"
        loc = keep(full, JANESTREET_CITIES.get(j.get("city", ""), j.get("city", "")))
        if loc is not None:
            out.append({"company": "Jane Street", "title": full, "location": loc, "posted_at": None,
                        "url": f"https://www.janestreet.com/join-jane-street/position/{j['id']}/"})
    return out


# Wellfound (startups) sits behind Akamai; the stealth browser gets through. Its pages aren't
# paginated and ignore job-type filters, so read role x Canadian-city pages, a slice per pass.
WELLFOUND_CITIES = ("toronto", "vancouver", "montreal", "ottawa", "waterloo", "kitchener", "calgary",
                    "edmonton", "mississauga", "halifax", "victoria", "winnipeg")
WELLFOUND_ROLES = ("software-engineer", "full-stack-engineer", "backend-engineer", "frontend-engineer",
                   "machine-learning-engineer", "data-engineer", "data-scientist", "devops-engineer",
                   "mobile-engineer")
WELLFOUND_PAGES = tuple(f"https://wellfound.com/location/{c}" for c in WELLFOUND_CITIES) + tuple(
    f"https://wellfound.com/role/l/{r}/{c}" for c in WELLFOUND_CITIES for r in WELLFOUND_ROLES)
WELLFOUND_PER_PASS = 10
_NEXT_DATA = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)


async def check_wellfound() -> list[dict[str, Any]]:
    """Software internships on Wellfound in the US/Canada, and every startup hiring full-time
    software roles in a Canadian city goes into `discovered` (source 'wellfound') to be placed."""
    db = connect()
    _discover_tables(db)
    i = int((db.execute("SELECT v FROM discover_state WHERE k='wellfound_cursor'").fetchone() or ["0"])[0])
    with db:
        db.execute("INSERT OR REPLACE INTO discover_state VALUES ('wellfound_cursor', ?)",
                   (str((i + WELLFOUND_PER_PASS) % len(WELLFOUND_PAGES)),))
    pages = [WELLFOUND_PAGES[(i + j) % len(WELLFOUND_PAGES)] for j in range(WELLFOUND_PER_PASS)]
    known = _known_companies()
    now = datetime.now(UTC).isoformat(timespec="seconds")
    out: dict[str, dict[str, Any]] = {}
    read = 0
    for page in pages:
        m = _NEXT_DATA.search(await _browser_html(page) or "")
        if not m:
            continue
        read += 1
        data = json.loads(m.group(1)).get("props", {}).get("pageProps", {}).get("apolloState", {}).get("data", {})
        owner = {ref["__ref"].split(":", 1)[1]: v.get("name") or ""
                 for k, v in data.items() if k.startswith("StartupResult")
                 for ref in v.get("highlightedJobListings") or [] if isinstance(ref, dict) and "__ref" in ref}
        for k, v in data.items():
            if not k.startswith("JobListingSearchResult"):
                continue
            name, title = owner.get(str(v.get("id")), "").strip(), (v.get("title") or "").strip()
            locs = v.get("locationNames") or []
            if not name or _AGENCY_RE.search(name):
                continue
            if any("Canada" in us_canada(loc) for loc in locs) and is_software_fulltime(title):
                key = norm_text(name)
                if key and key not in known:
                    with db:
                        if not db.execute("UPDATE discovered SET last_seen=? WHERE name_key=?", (now, key)).rowcount:
                            db.execute("INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                       (key, name, "", "wellfound", now, now, "new", None, None, None, None))
            loc = keep(title, "; ".join(locs) or ("Remote" if v.get("remote") else None))
            if loc is not None:
                url = f"https://wellfound.com/jobs/{v.get('id')}-{v.get('slug') or ''}"
                started = v.get("liveStartAt")
                out[url] = {"title": title, "location": loc, "url": url, "company": name,
                            "posted_at": iso(datetime.fromtimestamp(started, UTC)) if started else None}
    db.close()
    if not read:
        raise RuntimeError("Wellfound pages didn't load (challenge or layout change)")
    return list(out.values())


async def check_single(source: str, timeout: float) -> list[dict[str, Any]]:
    if source == "wellfound":
        return await check_wellfound()
    if source == "cgi":
        return await check_cgi(timeout)
    if source == "shopify":
        return await check_shopify(timeout)
    if source == "janestreet":
        return await check_janestreet(timeout)
    if source == "linkedin":
        return await check_linkedin(timeout)
    if source == "careers_search":
        return await run_careers_search(timeout)
    if source == "discover":
        return await run_discover(timeout)
    if source in LIST_SOURCES:
        return await check_list(source, timeout)
    if source == "jobbankca":
        return await check_jobbank(timeout)
    if source == "remoterocketship":
        return await check_remoterocketship(timeout)
    if source == "workopolis":
        return await check_workopolis(timeout)
    if source == "eluta":
        return await check_eluta(timeout)
    scraper = CONFIGS[source]["scraper"](source, timeout=timeout)
    scraper.include_descriptions = False
    jobs = await scraper.afetch()
    if not jobs:
        # Big companies always have open jobs: zero means the site changed and the
        # scraper broke silently (Meta renamed its GraphQL field in 2026-10).
        raise RuntimeError(f"{source}: 0 jobs total; scraper is probably broken")
    if source == "google":
        # Google's listing has no locations; fetch the detail page only for internship titles.
        need = [j for j in jobs if not j.location and is_software_internship(j.title)]
        if need:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
                sem = asyncio.Semaphore(8)
                await asyncio.gather(*(scraper._enrich_detail(client, sem, j) for j in need))
    return _from_jobs(jobs, with_company=True)


def _from_jobs(jobs: list[Any], *, with_company: bool = False) -> list[dict[str, Any]]:
    out = []
    for j in jobs:
        note_ca_software(j.title, j.location, j.country_iso, str(j.url))
        loc = keep(j.title, j.location, j.country_iso)
        if loc is not None:
            item = {"title": j.title.strip(), "location": loc, "url": str(j.url), "posted_at": iso(j.posted_at)}
            if with_company:
                item["company"] = j.company
            out.append(item)
    return out


# --- dead ATS board → company careers page ---------------------------------------------
# When a company's board is gone (404 / not found), read its own careers page instead:
# a working ATS board linked from it (polled through that board from then on), job data
# embedded in the page, or job-looking links. Careers URLs are cached in SQLite.

CAREERS_OVERRIDES = REPO / "ats-companies" / "careers_pages.csv"
CAREERS_RETRY_SECONDS = 24 * 60 * 60  # retry finding a careers page once a day
_ATS_HOSTS = re.compile(
    r"(greenhouse\.io|lever\.co|ashbyhq\.com|myworkdayjobs\.com|myworkdaysite\.com|icims\.com|"
    r"smartrecruiters\.com|workable\.com|bamboohr\.com|rippling\.com|breezy\.hr|recruitee\.com|"
    r"applytojob\.com|pinpointhq\.com|jobvite\.com|eightfold\.ai|avature\.net|oraclecloud\.com)", re.I)
_JOB_WORDS = re.compile(r"career|job|opening|position|join (us|our team)|we.re hiring|emploi", re.I)
# A challenge page, not a page that merely loads a CAPTCHA script for a contact form
# (reCAPTCHA/hCaptcha tags, Squarespace "captchaSettings", Wix module lists).
_CHALLENGE = re.compile(r"human verification|verify (that )?you are (a )?human|are you a robot|awswaf|"
                        r"cf-chl-|challenge-platform|just a moment\.\.\.|attention required|px-captcha|"
                        r"unusual traffic", re.I)
_CAPTCHA_WORD = re.compile(r"captcha", re.I)


def _is_challenge(page: str) -> bool:
    head = page[:20000]
    if _CHALLENGE.search(head):
        return True
    # A bare "captcha" counts only on a near-empty page (the challenge is all there is).
    text = re.sub(r"<script.*?</script>|<style.*?</style>|<[^>]+>", " ", page, flags=re.S | re.I)
    return bool(_CAPTCHA_WORD.search(head)) and len(" ".join(text.split())) < 300
_HREF = re.compile(r"""(?:href|src|data-url)\s*=\s*["']([^"'#\s]+)""", re.I)
_ANCHOR = re.compile(r"<a\b[^>]*href\s*=\s*[\"']([^\"'#\s]+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
_GH_EMBED = re.compile(r"greenhouse\.io/embed/job_board(?:/js)?\?for=([A-Za-z0-9_-]+)")
_LD_JSON = re.compile(r"<script[^>]+application/ld\+json[^>]*>(.*?)</script>", re.I | re.S)
_NEXT_DATA = re.compile(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_URL_KEYS = ("absolute_url", "externalLink", "applyLink", "applyUrl", "hostedUrl", "jobUrl", "url")
_LOC_KEYS = ("locationName", "location", "locations", "city", "jobLocation")
_careers_browsers = asyncio.Semaphore(3)  # pages rendered at once, all in one shared browser (~300 MB)
_browser_lock = asyncio.Lock()
_shared_browser: Any = None


def _dead_board(exc: BaseException) -> bool:
    if type(exc).__name__ == "CompanyNotFoundError":
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        # Workday answers 422 when the board's site id no longer exists.
        return exc.response.status_code in (404, 410) or (
            exc.response.status_code == 422 and "myworkdayjobs" in str(exc.request.url))
    return bool(re.search(r"\bnot found\b|\b404\b|\b410\b", str(exc), re.I))


def _blocked_board(exc: BaseException) -> bool:
    """The ATS refuses us (rate limit, WAF, bot check): read the careers page instead."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (403, 429)
    return bool(re.search(r"\b(403|429)\b|captcha|\bwaf\b|rate.?limit", str(exc), re.I))


def _careers_cache(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS careers_cache (company_key TEXT PRIMARY KEY,
        careers_url TEXT, ats TEXT, ats_row TEXT, checked_at TEXT)""")


def _careers_overrides() -> dict[str, str]:
    if not CAREERS_OVERRIDES.exists():
        return {}
    with CAREERS_OVERRIDES.open(newline="", encoding="utf-8") as fh:
        return {(r.get("name") or "").strip().lower(): (r.get("careers_url") or "").strip()
                for r in csv.DictReader(fh) if r.get("careers_url")}


def _name_slug(name: str) -> str:
    n = re.sub(r"\b(inc|llc|ltd|limited|corp|corporation|co|company|group|technologies|"
               r"technology|labs?|hq|the)\b", "", (name or "").lower())
    return re.sub(r"[^a-z0-9]", "", n)


async def _get_page(client: httpx.AsyncClient, url: str) -> httpx.Response | None:
    try:
        r = await client.get(url, timeout=10, follow_redirects=True,
                             headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/128",
                                      "Accept": "text/html,application/xhtml+xml"})
    except (httpx.HTTPError, ValueError):
        return None
    return r if r.status_code == 200 and "html" in r.headers.get("content-type", "") else None


async def find_careers_page(client: httpx.AsyncClient, target: dict[str, str]) -> str | None:
    """Manual override, then wherever the dead board redirects, then common URL guesses."""
    name = (target.get("name") or "").strip()
    override = _careers_overrides().get(name.lower())
    if override:
        return override
    board = (target.get("url") or "").strip()
    if board.startswith("http"):
        r = await _get_page(client, board)
        if r is not None and not _ATS_HOSTS.search(r.url.host or "") and _JOB_WORDS.search(r.text):
            return str(r.url)
    slugs = [s for s in dict.fromkeys([_name_slug(name), _name_slug(target.get("slug") or "")]) if len(s) >= 3]
    for s in slugs:
        for url in (f"https://www.{s}.com/careers", f"https://careers.{s}.com", f"https://www.{s}.com/jobs",
                    f"https://www.{s}.ca/careers", f"https://www.{s}.io/careers", f"https://www.{s}.ai/careers"):
            r = await _get_page(client, url)
            # The page must stay on the company's own domain (not a parked/redirected one).
            if r is not None and s in (r.url.host or "") and _JOB_WORDS.search(r.text):
                return str(r.url)
    return None


async def _browser_html(url: str) -> str | None:
    """Render a JavaScript-only careers page (cloakbrowser, same as the Eightfold fallback)."""
    try:
        from cloakbrowser import launch_async
    except ImportError:
        return None
    global _shared_browser
    async with _careers_browsers:
        async with _browser_lock:
            if _shared_browser is None or not _shared_browser.is_connected():
                _shared_browser = await launch_async(headless=True, humanize=True)
        page = None
        try:
            page = await _shared_browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(4_000)
            return await page.content()
        except Exception:  # noqa: BLE001 — a page that won't render just yields nothing
            return None
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:  # noqa: BLE001
                    pass


async def close_browser() -> None:
    global _shared_browser
    if _shared_browser is not None:
        try:
            await _shared_browser.close()
        except Exception:  # noqa: BLE001
            pass
        _shared_browser = None


def _ats_rows_from_page(page_html: str, base: str) -> list[tuple[str, dict[str, str]]]:
    """Every ATS board the page links to or embeds, as (ats, list-row) pairs."""
    from urllib.parse import urljoin

    from ats_scrapers.resolve import resolve_careers_url

    rows: dict[tuple[str, str], dict[str, str]] = {}
    page_html = f'href="{base}" ' + page_html  # the URL itself may be an ATS job/board (web search)
    for m in _GH_EMBED.finditer(page_html):
        rows[("greenhouse", m[1].lower())] = {"slug": m[1].lower(), "url": f"https://job-boards.greenhouse.io/{m[1]}"}
    for raw in _HREF.findall(page_html):
        url = urljoin(base, raw.replace("&amp;", "&"))
        if not _ATS_HOSTS.search(url):
            continue
        wd = URL_PATTERN.match(url)
        if wd:
            site = wd["site"] if not re.fullmatch(r"[a-z]{2}-[A-Z]{2}", wd["site"]) else url.split("/")[4]
            rows[("workday", f"{wd['company']}/{site}".lower())] = {
                "url": f"https://{wd['company']}.{wd['instance']}.myworkdayjobs.com/{site}"}
            continue
        try:
            res = resolve_careers_url(url)
        except Exception:  # noqa: BLE001
            res = None
        if res is not None and res.ats.value in CONFIGS:
            rows[(res.ats.value, res.slug.lower())] = {"slug": res.slug, "url": url}
    # A SuccessFactors career site on the company's own domain (careers.kinectrics.com).
    if not rows and _SF_SITE.search(page_html):
        host = urlparse(base).netloc.lower()
        labels = [x for x in host.split(".") if x not in _SUBDOMAIN_WORDS]
        rows[("successfactors", host)] = {"slug": labels[0] if labels else host, "url": f"https://{host}"}
    return [(a, r) for (a, _), r in rows.items()]


_SF_SITE = re.compile(r"rmkcdn\.successfactors\.com|jobs2web\.com", re.I)
_SUBDOMAIN_WORDS = {"www", "careers", "career", "jobs", "job", "emplois", "carrieres", "talent", "recruiting",
                    "apply", "work", "join"}


def _job_site_links(page_html: str, base: str) -> list[str]:
    """Links to the company's own job site on another host of its domain (careers.example.com),
    which a careers page often sends applicants to."""
    from urllib.parse import urljoin
    host = urlparse(base).netloc.lower()
    site = ".".join(host.split(".")[-2:])
    out: list[str] = []
    for raw in _HREF.findall(page_html):
        url = urljoin(base, raw.replace("&amp;", "&"))
        h = urlparse(url).netloc.lower()
        if h != host and h.endswith("." + site) and h.split(".")[0] in _SUBDOMAIN_WORDS - {"www"} and url not in out:
            out.append(url)
    return out[:2]


async def _poll_ats_row(client: httpx.AsyncClient, ats: str, row: dict[str, str],
                        timeout: float) -> list[dict[str, Any]]:
    if ats == "greenhouse":
        return await check_greenhouse(client, row)
    if ats == "workday":
        return await check_workday(client, row)
    return await check_scraper(ats, row, timeout)


def _walk_job_objects(obj: Any, out: list[dict[str, Any]], depth: int = 0) -> None:
    """Find job-shaped dicts (a title plus a location or link) anywhere in embedded app data."""
    if depth > 12:
        return
    if isinstance(obj, dict):
        title = obj.get("title") or obj.get("name") if obj.get("@type") == "JobPosting" else obj.get("title")
        if isinstance(title, str) and (any(k in obj for k in _LOC_KEYS) or any(k in obj for k in _URL_KEYS)):
            out.append(obj)
        for v in obj.values():
            _walk_job_objects(v, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj:
            _walk_job_objects(v, out, depth + 1)


def _loc_text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return " | ".join(filter(None, (_loc_text(x) for x in v)))
    if isinstance(v, dict):
        a = v.get("address") if isinstance(v.get("address"), dict) else v
        parts = [a.get(k) for k in ("addressLocality", "addressRegion", "addressCountry", "name", "city",
                                    "region", "country") if isinstance(a.get(k), str)]
        return ", ".join(dict.fromkeys(parts))
    return ""


def _jobs_from_page(page_html: str, base: str) -> list[dict[str, Any]]:
    """Jobs embedded in the page (JSON-LD, Next.js data, React Router stream), else job links."""
    import html as _html
    from urllib.parse import urljoin

    blobs: list[Any] = []
    for raw in _LD_JSON.findall(page_html):
        try:
            blobs.append(json.loads(raw.strip()))
        except ValueError:
            pass
    m = _NEXT_DATA.search(page_html)
    if m:
        try:
            blobs.append(json.loads(m[1]))
        except ValueError:
            pass
    m = re.search(r'streamController\.enqueue\(("(?:[^"\\]|\\.)*")\)', page_html)
    if m:
        try:
            blobs.append(_turbo_decode(json.loads(json.loads(m[1]))))
        except (ValueError, IndexError, KeyError, TypeError):
            pass
    found: list[dict[str, Any]] = []
    for b in blobs:
        _walk_job_objects(b, found)
    out: dict[str, dict[str, Any]] = {}
    for o in found:
        title = (o.get("title") or o.get("name") or "").strip()
        url = next((o[k] for k in _URL_KEYS if isinstance(o.get(k), str) and o[k]), "") or base
        loc = next((_loc_text(o[k]) for k in _LOC_KEYS if o.get(k)), "")
        label = keep(title, loc or None)
        if label is not None:
            out[urljoin(base, url) + "#" + title] = {"title": title, "location": label, "url": urljoin(base, url),
                                                    "posted_at": iso(o.get("datePosted") or o.get("publishedDate"))}
    if out:
        return list(out.values())
    # Last resort: links whose text reads like a software internship. No location on the
    # link itself, so it's marked for a look rather than dropped.
    for href, text in _ANCHOR.findall(page_html):
        title = _html.unescape(re.sub(r"<[^>]+>|\s+", " ", text)).strip()
        if 6 < len(title) < 160 and is_software_internship(title):
            url = urljoin(base, href.replace("&amp;", "&"))
            out[url] = {"title": title, "location": "location not listed", "url": url, "posted_at": None}
    return list(out.values())


# Web search for dead boards the URL guesses couldn't place. Searches are slow and
# throttled, so passes only queue the company; the `careers_search` source works the queue.
# It searches for an actual internship posting first ("Acme software engineer intern
# summer 2027"): the result's link shows which ATS board the company really uses.
SEARCH_QUERIES = ("{name} software engineer jobs", "{name} careers")  # its full-time roles lead to its board
SEARCH_GAP_SECONDS = 10.0  # between the two searches for one company
SEARCH_COMPANY_SECONDS = {"ddg": 60.0, "yahoo": 30.0}  # per company per engine (DuckDuckGo refuses faster)
SEARCH_ENGINES = tuple(SEARCH_COMPANY_SECONDS)  # each works through the queue at its own pace
# Tried 2026-10-08: Bing serves bots unrelated results, Qwant sits behind DataDome, Brave 429s,
# Startpage returns no results without JS, DuckDuckGo lite shares DuckDuckGo's limit.
SEARCH_BUDGET = 27 * 60  # seconds per careers_search pass, inside the 30-min cycle
SEARCH_BATCH = int(sum(SEARCH_BUDGET / s for s in SEARCH_COMPANY_SECONDS.values()))  # companies per pass
SEARCH_RECHECK_DAYS = 7  # search again for a company that still has no board
_DDG_RESULT = re.compile(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S)
_SEARCH_SKIP = re.compile(
    r"(^|\.)(indeed|linkedin|glassdoor|ziprecruiter|wellfound|angel|builtin|simplify|crunchbase|wikipedia|"
    r"facebook|twitter|x|instagram|youtube|tiktok|startup|levels|comparably|teamblind|theladders|monster|"
    r"careerbuilder|jooble|talent|workopolis|eluta|jobbank|bebee|ycombinator|zoominfo|rocketreach|tracxn|"
    r"pitchbook|bloomberg|reddit|medium|github|duckduckgo|google|bing|yelp|dnb|owler|apollo|signalhire|"
    r"lusha|jobillico|careerbeacon|adzuna|simplyhired|dice|handshake|jobright|interndock|intern-list)\.", re.I)


# Only US/Canada jobs matter: skip sites on other countries' domains (often a namesake company).
_FOREIGN_TLD = re.compile(r"\.(eu|uk|de|fr|nl|es|it|se|no|dk|fi|pl|ch|at|be|ie|pt|cz|in|au|nz|sg|jp|kr|cn|"
                          r"hk|tw|br|mx|ar|za|ae|il|ru|tr)$")


def _domain_is(host: str, slug: str) -> bool:
    """The domain is the company's name: mirego.com, careers.stay22.com, deductive.ai (name + TLD).
    Rejects lookalikes such as atomic.com for "Atomic Semi" or thehiive.ca for "Hiive"."""
    parts = host.removeprefix("www.").split(".")
    if len(parts) < 2:
        return False
    label, tld = parts[-2].replace("-", ""), parts[-1]
    return label == slug or label.startswith(slug) or label + tld == slug


def _search_queue(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS careers_search_queue (company_key TEXT PRIMARY KEY,
        name TEXT, slug TEXT, board_url TEXT, queued_at TEXT, searched_at TEXT, result TEXT)""")


def _host(url: str) -> str:
    from urllib.parse import urlparse

    try:
        return (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""


_YAHOO_RESULT = re.compile(r'<a[^>]+href="(https://r\.search\.yahoo\.com/[^"]+)"[^>]*>(.*?)</a>', re.S)


async def yahoo_search(query: str) -> list[tuple[str, str]]:
    """(url, title) results from Yahoo's HTML page (Bing's index); each link wraps the real URL in /RU=.../."""
    import html as _html
    from urllib.parse import quote_plus, unquote

    import httpcloak

    r = await asyncio.to_thread(httpcloak.get, f"https://search.yahoo.com/search?p={quote_plus(query)}",
                                timeout=20_000)
    if r.status_code != 200:
        raise RuntimeError(f"Yahoo refused the search (HTTP {r.status_code})")
    out = []
    for href, title in _YAHOO_RESULT.findall(r.text):
        ru = re.search(r"/RU=([^/]+)/", _html.unescape(href))
        if not ru:
            continue
        url = unquote(ru[1])
        if re.match(r"https?://[^/]*\.(yahoo|bing)\.com", url):
            continue
        title = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", _html.unescape(title))).strip()
        if title:
            out.append((url, title))
    return out


async def web_search(client: httpx.AsyncClient, query: str, engine: str = "ddg") -> list[tuple[str, str]]:
    """(url, title) results: Google or Brave when their keys are set, else DuckDuckGo's HTML page
    (or Yahoo's, for engine="yahoo")."""
    import html as _html
    from urllib.parse import parse_qs, unquote, urlparse

    if engine == "yahoo":
        return await yahoo_search(query)
    if os.environ.get("GOOGLE_API_KEY") and os.environ.get("GOOGLE_CSE_ID"):
        r = await client.get("https://www.googleapis.com/customsearch/v1", params={
            "key": os.environ["GOOGLE_API_KEY"], "cx": os.environ["GOOGLE_CSE_ID"], "q": query, "num": 10})
        r.raise_for_status()
        return [(i["link"], i.get("title", "")) for i in r.json().get("items", [])]
    if os.environ.get("BRAVE_API_KEY"):
        r = await client.get("https://api.search.brave.com/res/v1/web/search", params={"q": query, "count": 10},
                             headers={"X-Subscription-Token": os.environ["BRAVE_API_KEY"],
                                      "Accept": "application/json"})
        r.raise_for_status()
        return [(i["url"], i.get("title", "")) for i in (r.json().get("web") or {}).get("results", [])]
    # DuckDuckGo answers plain httpx with a 202 bot check; httpcloak's browser-like
    # connection gets normal results (same trick as check_cgi).
    import httpcloak
    from urllib.parse import quote_plus

    r = await asyncio.to_thread(httpcloak.get, f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
                                timeout=20_000)
    if r.status_code != 200 or "anomaly" in r.text[:5000].lower():
        raise RuntimeError(f"DuckDuckGo refused the search (HTTP {r.status_code})")
    out = []
    for href, title in _DDG_RESULT.findall(r.text):
        href = _html.unescape(href)
        if "uddg=" in href:
            href = unquote(parse_qs(urlparse(href).query).get("uddg", [href])[0])
        if href.startswith("http") and "duckduckgo.com/y.js" not in href:  # skip ads
            out.append((href, _html.unescape(re.sub(r"<[^>]+>", "", title)).strip()))
    return out


def _board_id(url: str) -> tuple[str, str]:
    """(host, first path segment) with any locale segment dropped: identifies an ATS board."""
    from urllib.parse import urlparse

    try:
        p = urlparse(url)
    except ValueError:
        return ("", "")
    segs = [s for s in p.path.split("/") if s and not re.fullmatch(r"[a-z]{2}(-[A-Za-z]{2})?", s)]
    return ((p.hostname or "").lower().removeprefix("www."), segs[0].lower() if segs else "")


def _pick_search_result(results: list[tuple[str, str]], name: str, dead_board: str = "") -> str | None:
    """The company's ATS board/job, or a page on its own site, never an aggregator. The result
    must clearly be this company: its name in the URL or in the result title. Results on the
    dead board itself (search engines keep old job pages indexed) are skipped."""
    slug = _name_slug(name)
    dead = _board_id(dead_board) if dead_board else None
    for url, title in results:
        host = _host(url)
        if not host or _SEARCH_SKIP.search(host) or _FOREIGN_TLD.search(host):
            continue
        if dead and _board_id(url) == dead:
            continue
        if _ATS_HOSTS.search(host):
            # An ATS job/board: the company must be in the board itself (subdomain or board
            # name), not just anywhere in the link (a job's city can match), or in the title.
            board = "".join(_board_id(url)).replace("-", "").replace("_", "")
            if (slug[:6] and slug[:6] in board) or (name and name.lower() in title.lower()):
                return url
        elif slug and _domain_is(host, slug):
            return url  # the company's own site
    return None


async def run_careers_search(timeout: float) -> list[dict[str, Any]]:
    """Work through the queue of dead-board companies the URL guesses couldn't place."""
    from urllib.parse import urljoin

    db = connect()
    _careers_cache(db)
    _search_queue(db)
    queue = db.execute("SELECT company_key, name, slug, board_url FROM careers_search_queue WHERE searched_at IS NULL "
                       "ORDER BY company_key LIKE 'discover:%' DESC, queued_at LIMIT ?", (SEARCH_BATCH,)).fetchall()
    found = searched = 0
    pending = list(queue)
    deadline = time.monotonic() + SEARCH_BUDGET
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        async def worker(engine: str) -> None:
            nonlocal found, searched
            pace = SEARCH_COMPANY_SECONDS[engine]
            while pending and time.monotonic() + pace <= deadline:
                company_key, name, slug, board_url = pending.pop(0)
                started = time.monotonic()
                name = name or slug or ""
                url = None
                try:
                    for q in SEARCH_QUERIES:
                        results = await web_search(client, q.format(name=name), engine)
                        await asyncio.sleep(SEARCH_GAP_SECONDS)
                        url = _pick_search_result(results, name, board_url or company_key)
                        if url:
                            break
                except Exception as exc:  # noqa: BLE001 — refused/rate-limited: this engine stops, resumes next pass
                    pending.insert(0, (company_key, name, slug, board_url))
                    log_line(f"{'careers_search':16} {engine} stopped early: {exc}")
                    return
                searched += 1
                if url and not _ATS_HOSTS.search(_host(url)) and \
                        not re.search(r"career|job|join|emploi|work-with|opening", url, re.I):
                    # Landed on a homepage: its /careers page is the better target when it exists.
                    r = await _get_page(client, urljoin(url, "/careers"))
                    if r is not None:
                        url = str(r.url)
                now = datetime.now(UTC).isoformat(timespec="seconds")
                with db:
                    db.execute("UPDATE careers_search_queue SET searched_at=?, result=? WHERE company_key=?",
                               (now, url, company_key))
                    if url:
                        found += 1
                        if not company_key.startswith("discover:"):
                            _log_discovered([now, name, "", "dead board", "careers page (web search)", "", url])
                        # An ATS job/board URL resolves to its board on the next pass (self-heal);
                        # a company page is read like any careers page.
                        db.execute("INSERT OR REPLACE INTO careers_cache VALUES (?,?,?,?,?)",
                                   (company_key, url, None, None, now))
                await asyncio.sleep(max(0.0, pace - (time.monotonic() - started)))

        await asyncio.gather(*(worker(e) for e in SEARCH_ENGINES))
    db.close()
    log_line(f"{'careers_search':16} {searched} searched, {found} boards/careers pages found, "
             f"{len(pending)} left in this batch")
    return []


async def careers_fallback(db: sqlite3.Connection, client: httpx.AsyncClient, target: dict[str, str],
                           company_key: str, timeout: float) -> list[dict[str, Any]]:
    """The company's ATS board is dead: poll its careers page (or the board it links to)."""
    _careers_cache(db)
    row = db.execute("SELECT careers_url, ats, ats_row, checked_at FROM careers_cache WHERE company_key=?",
                     (company_key,)).fetchone()
    now = datetime.now(UTC)
    # Already found a working board through the careers page: poll it directly.
    if row and row[1]:
        try:
            return await _poll_ats_row(client, row[1], json.loads(row[2]), timeout)
        except Exception as exc:  # noqa: BLE001
            if not _dead_board(exc):
                raise
            row = (row[0], None, None, row[3])  # that board died too; re-read the careers page
    careers_url = row[0] if row else None
    if not careers_url:
        if row and (now - datetime.fromisoformat(row[3])).total_seconds() < CAREERS_RETRY_SECONDS:
            raise RuntimeError("dead board; no careers page found (retrying daily)")
        careers_url = await find_careers_page(client, target)
        if not careers_url:
            careers_url = await _careers_via_linkedin(client, (target.get("name") or "").strip())
        if careers_url and not company_key.startswith(("discover:", "careers:")):
            _log_discovered([now.isoformat(timespec="seconds"), (target.get("name") or "").strip(), "",
                             "dead board", "careers page", "", careers_url])
        with db:
            db.execute("INSERT OR REPLACE INTO careers_cache VALUES (?,?,?,?,?)",
                       (company_key, careers_url, None, None, now.isoformat(timespec="seconds")))
            if not careers_url:
                # URL guesses missed: queue a web search (re-queued weekly if that found nothing too).
                _search_queue(db)
                q = db.execute("SELECT searched_at FROM careers_search_queue WHERE company_key=?",
                               (company_key,)).fetchone()
                stale = q and q[0] and (now - datetime.fromisoformat(q[0])).days >= SEARCH_RECHECK_DAYS
                if q is None or stale:
                    db.execute("INSERT OR REPLACE INTO careers_search_queue VALUES (?,?,?,?,?,NULL,NULL)",
                               (company_key, (target.get("name") or "").strip(), (target.get("slug") or "").strip(),
                                (target.get("url") or "").strip(), now.isoformat(timespec="seconds")))
        if not careers_url:
            raise RuntimeError("dead board; no careers page found (queued for web search)")
    r = await _get_page(client, careers_url)
    page_html, base = (r.text, str(r.url)) if r is not None else ("", careers_url)
    candidates = _ats_rows_from_page(page_html, base)
    challenged = not candidates and _is_challenge(page_html)
    jobs = [] if candidates or challenged else _jobs_from_page(page_html, base)
    if not candidates and not jobs:
        # JavaScript-only page (or a challenge the browser may pass): render it and look again.
        rendered = await _browser_html(careers_url)
        if rendered and not _is_challenge(rendered):
            challenged = False
            page_html = rendered
            candidates = _ats_rows_from_page(page_html, base)
            jobs = [] if candidates else _jobs_from_page(page_html, base)
        if challenged:
            raise RuntimeError(f"dead board; careers page {careers_url} is behind a CAPTCHA")
    if not candidates and not jobs:
        # The page sends applicants to the company's own job site (careers.example.com): look there.
        for link in _job_site_links(page_html, base):
            sub = await _get_page(client, link)
            if sub is not None:
                candidates = _ats_rows_from_page(sub.text, str(sub.url))
                if candidates:
                    break
    # ATS boards first: a search result can be an ATS job link that won't load as a web page.
    for ats, ats_row in candidates[:3]:
        ats_row = {"name": target.get("name") or "", **ats_row}
        try:
            items = await _poll_ats_row(client, ats, ats_row, timeout)
        except Exception:  # noqa: BLE001 — try the next linked board
            continue
        with db:  # self-heal: poll this board directly from the next pass on
            db.execute("UPDATE careers_cache SET ats=?, ats_row=? WHERE company_key=?",
                       (ats, json.dumps(ats_row), company_key))
        if not company_key.startswith(("discover:", "careers:")):
            _log_discovered([datetime.now(UTC).isoformat(timespec="seconds"), ats_row["name"], "",
                             "dead board", "board found via careers page", ats,
                             ats_row.get("slug") or ats_row.get("url") or ""])
        return items
    if candidates and not jobs:
        jobs = _jobs_from_page(page_html, base)
    if candidates and not jobs:
        with db:  # the page points back at the dead board: look for the company's current one
            _search_queue(db)
            q = db.execute("SELECT searched_at FROM careers_search_queue WHERE company_key=?",
                           (company_key,)).fetchone()
            if q is None or (q[0] and (now - datetime.fromisoformat(q[0])).days >= SEARCH_RECHECK_DAYS):
                db.execute("INSERT OR REPLACE INTO careers_search_queue VALUES (?,?,?,?,?,NULL,NULL)",
                           (company_key, (target.get("name") or "").strip(), (target.get("slug") or "").strip(),
                            (target.get("url") or "").strip(), now.isoformat(timespec="seconds")))
        raise RuntimeError(f"dead board; boards linked from {careers_url} don't work either (queued for web search)")
    if r is None and not page_html:
        raise RuntimeError(f"dead board; careers page {careers_url} did not load")
    return jobs


# --- discovery: every company hiring for software roles → its ATS board -----------------
# Companies that hire developers usually hire interns too, so watch them before they post
# an internship. `discover` reads LinkedIn's logged-out search for software/tech roles in
# Canada (plus companies the community lists surface), finds each new company's ATS board
# by name, and adds it to that ATS list; companies it can't place go to the web-search
# queue, and `careers` polls whatever careers page or board that finds.

DISCOVERED_CSV = OUT_DIR / "discovered.csv"  # written by write_ca_companies
DISCOVER_LOG_CSV = OUT_DIR / "discover_log.csv"  # every company discovery placed, and where
DISCOVER_KEYWORDS = ("software engineer", "software developer", "full stack developer", "backend developer",
                     "frontend developer", "data engineer", "machine learning engineer", "devops engineer",
                     "site reliability engineer", "mobile developer", "embedded software engineer",
                     "QA automation engineer", "cloud engineer", "platform engineer", "AI engineer", "game developer",
                     "web developer", "programmer", "data scientist", "security engineer", "développeur logiciel",
                     "programmeur", "firmware engineer", "software engineer in test", "solutions engineer",
                     "salesforce developer", "business intelligence developer", "network engineer",
                     "systems engineer", "IT developer", "analyste programmeur", "ingénieur logiciel",
                     "new grad software engineer", "junior developer")
DISCOVER_LOCATIONS = ("Canada", "Toronto, Ontario, Canada", "Montreal, Quebec, Canada",
                      "Vancouver, British Columbia, Canada", "Ottawa, Ontario, Canada", "Waterloo, Ontario, Canada",
                      "Kitchener, Ontario, Canada", "Mississauga, Ontario, Canada", "Markham, Ontario, Canada",
                      "Calgary, Alberta, Canada", "Edmonton, Alberta, Canada", "Winnipeg, Manitoba, Canada",
                      "Quebec City, Quebec, Canada", "Halifax, Nova Scotia, Canada",
                      "Victoria, British Columbia, Canada", "Burnaby, British Columbia, Canada",
                      "Richmond Hill, Ontario, Canada", "Vaughan, Ontario, Canada", "Oakville, Ontario, Canada",
                      "Burlington, Ontario, Canada", "Hamilton, Ontario, Canada", "London, Ontario, Canada",
                      "Guelph, Ontario, Canada", "Brampton, Ontario, Canada", "Laval, Quebec, Canada",
                      "Gatineau, Quebec, Canada", "Sherbrooke, Quebec, Canada", "Surrey, British Columbia, Canada",
                      "Richmond, British Columbia, Canada", "Kelowna, British Columbia, Canada",
                      "Saskatoon, Saskatchewan, Canada", "Regina, Saskatchewan, Canada",
                      "Fredericton, New Brunswick, Canada", "Moncton, New Brunswick, Canada",
                      "St. John's, Newfoundland and Labrador, Canada", "Charlottetown, Prince Edward Island, Canada")
DISCOVER_SEARCHES = [(loc, kw) for loc in DISCOVER_LOCATIONS for kw in DISCOVER_KEYWORDS]
# Indeed Canada (through Workopolis) and Job Bank list employers LinkedIn misses, mostly small ones.
# Each pass reads a slice of these searches in full; a cursor rotates through them.
DISCOVER_WORKOPOLIS_KEYWORDS = DISCOVER_KEYWORDS + ("IT analyst", "systems analyst", "analyste informatique",
                                                    "développeur", "data analyst")
DISCOVER_WORKOPOLIS_PER_PASS = 4  # keywords per pass, up to 20 pages each
DISCOVER_JOBBANK_TERMS = ("software", "developer", "programmer", "informatique", "web developer",
                          "data", "computer", "développeur", "engineer", "analyst", "IT", "programmeur")
DISCOVER_JOBBANK_PER_PASS = 4  # terms per pass
DISCOVER_JOBBANK_PAGES = 10  # 25 jobs a page, newest first
# New-grad lists: companies hiring new-grad software people in Canada hire interns too.
DISCOVER_NEWGRAD_LISTS = (
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json",
)
DISCOVER_MAX_PAGE = 10  # 10 cards a page
DISCOVER_REQUESTS = 60  # LinkedIn pages per pass; blocks are waited out (linkedin_get)
DISCOVER_RESOLVE = 150  # new companies placed per pass
_LI_COMPANY_SLUG = re.compile(r"linkedin\.com/company/([^?\"/]+)")
_CA_LOCATION = re.compile(r"canada|\b(ON|QC|BC|AB|MB|NS|NB|SK)\b|ontario|qu[eé]bec|toronto|montr[eé]al|"
                          r"ottawa|vancouver|waterloo|calgary|edmonton", re.I)


def _discover_tables(db: sqlite3.Connection) -> None:
    db.execute("""CREATE TABLE IF NOT EXISTS discovered (name_key TEXT PRIMARY KEY, name TEXT,
        li_slug TEXT, source TEXT, first_seen TEXT, last_seen TEXT, status TEXT, ats TEXT, slug TEXT,
        url TEXT, checked_at TEXT)""")
    db.execute("CREATE TABLE IF NOT EXISTS discover_state (k TEXT PRIMARY KEY, v TEXT)")


def _known_companies() -> set[str]:
    """Normalised names and slugs of every company already in a list."""
    known: set[str] = set()
    for path in (REPO / "ats-companies").glob("*.csv"):
        with path.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                for v in (r.get("name"), r.get("slug")):
                    if v:
                        known.add(norm_text(v.split("/")[0]))
    known.discard("")
    return known


async def _probe_board(client: httpx.AsyncClient, ats: str, slug: str, name: str) -> int:
    """Open jobs on that ATS board when it exists and is this company's; else 0."""
    try:
        if ats == "greenhouse":
            r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
            if r.status_code != 200 or not _same_company(r.json().get("name", ""), name):
                return 0
            r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            return len(r.json().get("jobs", [])) if r.status_code == 200 else 0
        if ats == "ashby":
            r = await client.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            return len(r.json().get("jobs", [])) if r.status_code == 200 else 0
        if ats == "lever":
            r = await client.get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
            return len(r.json()) if r.status_code == 200 and isinstance(r.json(), list) else 0
        if ats == "smartrecruiters":
            r = await client.get(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1")
            if r.status_code != 200:
                return 0
            d = r.json()
            first = (d.get("content") or [{}])[0]
            if not _same_company((first.get("company") or {}).get("name", ""), name):
                return 0
            return int(d.get("totalFound") or 0)
    except (httpx.HTTPError, ValueError):
        return 0
    return 0


def _same_company(board_name: str, name: str) -> bool:
    a, b = norm_text(board_name), norm_text(name)
    return bool(a and b and (a == b or a.startswith(b) or b.startswith(a)))


async def _place_company(client: httpx.AsyncClient, name: str, li_slug: str) -> tuple[str, str] | None:
    """(ats, slug) of the company's live board on a free-API ATS, by its LinkedIn handle or name.
    Ashby/Lever boards carry no company name, so only an exact slug match counts there."""
    slugs = list(dict.fromkeys(s for s in (
        (li_slug or "").lower(), _name_slug(name), re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-"),
        re.sub(r"-(inc|ltd|llc|corp|group|canada|ca|technologies|tech|labs?|hq)$", "", (li_slug or "").lower()),
    ) if s and len(s) >= 3))
    for slug in slugs:
        for ats in ("greenhouse", "ashby", "lever", "smartrecruiters"):
            if ats in ("ashby", "lever") and slug.replace("-", "") != _name_slug(name) and slug != (li_slug or "").lower():
                continue
            if await _probe_board(client, ats, slug, name) > 0:
                return ats, slug
    return None


_ATS_ROW_URL = {"greenhouse": "https://job-boards.greenhouse.io/{}", "ashby": "https://jobs.ashbyhq.com/{}",
                "lever": "https://jobs.lever.co/{}", "smartrecruiters": "https://careers.smartrecruiters.com/{}"}


DEAD_BOARDS_CSV = OUT_DIR / "dead_boards.csv"  # source,name,url,moved_to,new_slug
BOARD_RECHECK_DAYS = 7  # look again for a dead board's new home weekly
BOARD_REPAIR_PER_PASS = 150
# Lists whose board URL ends in the company's own handle.
_SLUG_BOARDS = {"greenhouse", "ashby", "lever", "smartrecruiters", "workable", "recruitee", "breezy",
                "rippling", "jazzhr", "bamboohr", "pinpoint", "gem"}


def _moved_away(ats: str) -> set[str]:
    if not DEAD_BOARDS_CSV.exists():
        return set()
    with DEAD_BOARDS_CSV.open(newline="", encoding="utf-8") as fh:
        return {(r.get("url") or "").rstrip("/").lower() for r in csv.DictReader(fh)
                if r.get("source") == ats and r.get("moved_to")}


async def repair_dead_boards(db: sqlite3.Connection, client: httpx.AsyncClient) -> tuple[int, int]:
    """Boards that 404 (the company left that ATS or renamed its board): look for the company's
    live board on Greenhouse / Ashby / Lever / SmartRecruiters. A find is added to that list and
    logged in dead_boards.csv, which takes the dead board off its own list (company_list).
    Returns (checked, moved)."""
    db.execute("""CREATE TABLE IF NOT EXISTS board_repair (source TEXT, company_key TEXT, checked_at TEXT,
        PRIMARY KEY (source, company_key))""")
    now = datetime.now(UTC)
    rows = db.execute(
        """SELECT f.source, f.company_key, f.name, f.url FROM failures f
           LEFT JOIN board_repair b ON b.source = f.source AND b.company_key = f.company_key
           WHERE f.error LIKE '%dead board%' AND f.source NOT IN ('careers', 'canada') AND f.last_seen >= ?
             AND (b.checked_at IS NULL OR b.checked_at < ?) ORDER BY b.checked_at IS NOT NULL, f.last_seen DESC
           LIMIT ?""",
        ((now - timedelta(days=1)).isoformat(timespec="seconds"),
         (now - timedelta(days=BOARD_RECHECK_DAYS)).isoformat(timespec="seconds"), BOARD_REPAIR_PER_PASS)).fetchall()
    listed: dict[str, set[str]] = {}
    moved = 0
    sem = asyncio.Semaphore(8)

    async def one(source: str, key: str, name: str, url: str) -> None:
        nonlocal moved
        slug = key.rstrip("/").rsplit("/", 1)[-1] if key.startswith("http") else key
        # Workday / iCIMS / Oracle URLs end in words like "External": match those by name only.
        hint = slug if source in _SLUG_BOARDS else ""
        async with sem:
            got = await _place_company(client, name or slug, hint)
        with db:
            db.execute("INSERT OR REPLACE INTO board_repair VALUES (?,?,?)",
                       (source, key, now.isoformat(timespec="seconds")))
        if not got or not url:
            return
        ats, new_slug = got
        if ats == source and new_slug.lower() == slug.lower():
            return  # the same board answers again: it wasn't dead after all
        if ats not in listed:
            listed[ats] = {(r.get("slug") or "").lower() for r in company_list(ats, None)}
        if new_slug.lower() not in listed[ats]:
            _append_list_row(ats, name or new_slug, new_slug)
            listed[ats].add(new_slug.lower())
        new_file = not DEAD_BOARDS_CSV.exists()
        with DEAD_BOARDS_CSV.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            if new_file:
                w.writerow(["source", "name", "url", "moved_to", "new_slug"])
            w.writerow([source, name, url, ats, new_slug])
        moved += 1

    await asyncio.gather(*(one(*r) for r in rows))
    return len(rows), moved


def _append_list_row(ats: str, name: str, slug: str) -> None:
    path = REPO / CONFIGS[ats]["csv"]
    with path.open(newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    raw = path.read_bytes()
    with path.open("a", newline="", encoding="utf-8") as fh:
        if raw and not raw.endswith(b"\n"):
            fh.write("\n")
        csv.DictWriter(fh, fieldnames=header, extrasaction="ignore", lineterminator="\n").writerow(
            {"name": name, "slug": slug, "url": _ATS_ROW_URL[ats].format(slug)})


def _migrate_discover_log() -> None:
    """discovered.csv used to be this log; keep its history under the log's name."""
    if DISCOVER_LOG_CSV.exists() or not DISCOVERED_CSV.exists():
        return
    with DISCOVERED_CSV.open(encoding="utf-8") as fh:
        is_log = fh.readline().startswith("found_at,")
    if is_log:
        DISCOVERED_CSV.replace(DISCOVER_LOG_CSV)


def _log_discovered(row: list[str]) -> None:
    _migrate_discover_log()
    new = not DISCOVER_LOG_CSV.exists()
    with DISCOVER_LOG_CSV.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["found_at", "company", "linkedin", "seen_via", "result", "ats", "slug_or_url"])
        w.writerow(row)


_LI_WEBSITE = re.compile(r'linkedin\.com/redir/redirect\?url=([^&"]+)[^"]*trk=about_website')
_CAREERS_LINK = re.compile(r"career|carri[eè]re|\bjobs?\b|join[- ]?(us|the[- ]team|our[- ]team)|emploi|"
                           r"work[- ](with|at)[- ]us|we.re[- ]hiring|open[- ]positions|opportunit", re.I)
_CAREERS_PATHS = ("/careers", "/jobs", "/en/careers", "/careers/", "/carrieres", "/fr/carrieres",
                  "/join-us", "/company/careers", "/about/careers", "/join")
LINKEDIN_UAS = ("Mozilla/5.0 (X11; Linux x86_64) Chrome/128",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15")


async def linkedin_get(client: httpx.AsyncClient, url: str, params: dict | None = None,
                       tries: int = 5) -> str | None:
    """A LinkedIn guest page. When LinkedIn blocks (429/999/403): retry through httpcloak's
    browser-like connection and other user agents, backing off 30 s, 60 s, 120 s... between
    tries. None for a page that doesn't exist; raises only if every try stayed blocked."""
    full = str(httpx.URL(url, params=params or {}))
    delay = 30.0
    for attempt in range(tries):
        try:
            r = await client.get(full, headers={"User-Agent": LINKEDIN_UAS[attempt % len(LINKEDIN_UAS)]})
            if r.status_code == 200:
                return r.text
            if r.status_code not in (429, 999, 403):
                return None
        except httpx.HTTPError:
            pass
        try:
            import httpcloak

            rc = await asyncio.to_thread(httpcloak.get, full, timeout=20_000)
            if rc.status_code == 200:
                return rc.text
        except Exception:  # noqa: BLE001 — fall through to the back-off
            pass
        if attempt < tries - 1:
            await asyncio.sleep(delay)
            delay *= 2
    raise RuntimeError("LinkedIn kept blocking after back-off")


def _site_of(host: str) -> str:
    parts = (host or "").lower().removeprefix("www.").split(".")
    return ".".join(parts[-3:] if len(parts) > 2 and parts[-2] in ("co", "com", "qc", "on") else parts[-2:])


async def careers_from_site(client: httpx.AsyncClient, site: str) -> str | None:
    """The careers page of a company website: a careers link on the homepage, else common paths."""
    import html as _html
    from urllib.parse import urljoin, urlparse

    r = await _get_page(client, site)
    if r is None:
        return None
    base, home = str(r.url), _site_of(r.url.host or "")
    links = []
    for href, label in _ANCHOR.findall(r.text):
        url = urljoin(base, _html.unescape(href))
        host = _host(url)
        if not (_ATS_HOSTS.search(host) or _site_of(host) == home):
            continue
        label = re.sub(r"<[^>]+>", " ", label)
        if _CAREERS_LINK.search(label):
            links.insert(0, url)  # the link's own text says careers: best
        elif _CAREERS_LINK.search(urlparse(url).path.replace("-", " ")):
            links.append(url)
    for url in dict.fromkeys(links):
        if _ATS_HOSTS.search(_host(url)):
            return url
        page = await _get_page(client, url)
        if page is not None and _JOB_WORDS.search(page.text):
            return str(page.url)
    for path in _CAREERS_PATHS:
        page = await _get_page(client, urljoin(base, path))
        if page is not None and _site_of(page.url.host or "") == home and _JOB_WORDS.search(page.text):
            return str(page.url)
    return None


async def _careers_via_linkedin(client: httpx.AsyncClient, name: str) -> str | None:
    """Careers page through the company's LinkedIn page, its handle guessed from the name.
    One try only: this runs inside an ATS pass, so a LinkedIn block is not waited out here."""
    from urllib.parse import unquote

    for slug in dict.fromkeys([re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-"), _name_slug(name)]):
        if len(slug) < 3:
            continue
        try:
            text = await linkedin_get(client, f"https://www.linkedin.com/company/{slug}", tries=1)
        except RuntimeError:
            return None
        title = re.search(r"<title>\s*(.*?)\s*\|", text or "")
        m = _LI_WEBSITE.search(text or "")
        if m and title and _same_company(title.group(1), name):
            site = unquote(m.group(1))
            if not _SEARCH_SKIP.search(_host(site)):
                return await careers_from_site(client, site)
    return None


async def company_careers(client: httpx.AsyncClient, name: str, li_slug: str) -> str | None:
    """Careers page via the website on the company's LinkedIn page, else URL guesses."""
    from urllib.parse import unquote

    if li_slug:
        try:
            text = await linkedin_get(client, f"https://www.linkedin.com/company/{li_slug}", tries=3)
        except RuntimeError:  # still blocked: the URL guesses below, then the web search
            text = None
        m = _LI_WEBSITE.search(text or "")
        if m:
            site = unquote(m.group(1))
            if not _SEARCH_SKIP.search(_host(site)):
                found = await careers_from_site(client, site)
                if found:
                    return found
    return await find_careers_page(client, {"name": name, "slug": li_slug})


async def resolve_discovered(db: sqlite3.Connection, client: httpx.AsyncClient, known: set[str],
                             key: str, name: str, li_slug: str, via: str) -> str:
    """Put one discovered company where a poller reads it: its ATS list when it has a board on a
    free-API ATS, else its careers page (the `careers` source polls those every 10 min), else the
    web-search queue. Returns 'listed', 'careers' or 'searching'."""
    hit = await _place_company(client, name, li_slug)
    careers = None
    if not hit:
        try:
            careers = await company_careers(client, name, li_slug)
        except Exception:  # noqa: BLE001 — LinkedIn blocked: the web search still runs
            careers = None
    stamp = datetime.now(UTC).isoformat(timespec="seconds")
    with db:
        if hit:
            ats, slug = hit
            if norm_text(slug) not in known:
                _append_list_row(ats, name, slug)
                known.add(norm_text(slug))
            db.execute("UPDATE discovered SET status='listed', ats=?, slug=?, checked_at=? WHERE name_key=?",
                       (ats, slug, stamp, key))
            _log_discovered([stamp, name, li_slug, via, "added to list", ats, slug])
            return "listed"
        if careers:
            db.execute("INSERT OR REPLACE INTO careers_cache VALUES (?,?,?,?,?)",
                       (f"discover:{key}", careers, None, None, stamp))
            db.execute("UPDATE discovered SET status='careers', url=?, checked_at=? WHERE name_key=?",
                       (careers, stamp, key))
            db.execute("DELETE FROM careers_search_queue WHERE company_key=?", (f"discover:{key}",))
            _log_discovered([stamp, name, li_slug, via, "careers page", "", careers])
            return "careers"
        # Unknown board: the web search finds its board or careers page, and the
        # `careers` source polls that.
        db.execute("INSERT OR IGNORE INTO careers_search_queue VALUES (?,?,?,?,?,NULL,NULL)",
                   (f"discover:{key}", name, li_slug, "", stamp))
        if via != "retry":
            db.execute("UPDATE discovered SET status='searching', checked_at=? WHERE name_key=?", (stamp, key))
            _log_discovered([stamp, name, li_slug, via, "queued for web search", "", ""])
        return "searching"


DISCOVER_HARVEST = 40  # unknown boards (from list/LinkedIn job links) tested per pass


def _known_boards() -> set[tuple[str, str]]:
    """(ats, board id) for every board already in a list; Workday ids are "tenant/site"."""
    boards: set[tuple[str, str]] = set()
    for ats, cfg in CONFIGS.items():
        path = REPO / cfg.get("csv", "")
        if not cfg.get("csv") or not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                url = (r.get("url") or "").strip()
                wd = URL_PATTERN.match(url) if ats == "workday" else None
                if wd:
                    boards.add(("workday", f"{wd['company']}/{wd['site']}".lower()))
                if r.get("slug"):
                    boards.add((ats, r["slug"].strip().lower()))
    return boards


def _board_id_of(ats: str, row: dict[str, str]) -> str:
    if ats == "workday":
        wd = URL_PATTERN.match(row.get("url", ""))
        return f"{wd['company']}/{wd['site']}".lower() if wd else ""
    return (row.get("slug") or "").lower()


async def harvest_boards(db: sqlite3.Connection, client: httpx.AsyncClient, timeout: float) -> int:
    """Internships the lists and LinkedIn found link straight to the company's ATS board.
    Any such board not in our lists yet (a company's second, campus or intern board,
    or a board on another ATS) is tested and added, so its next postings come straight
    from the source instead of hours later through a list."""
    known = _known_boards()
    tried = {r[0] for r in db.execute("SELECT k FROM discover_state WHERE k LIKE 'board:%'")}
    candidates: dict[tuple[str, str], tuple[str, dict[str, str]]] = {}
    for company, url in db.execute("SELECT DISTINCT company, url FROM postings WHERE origin IN (%s)"
                                   % ",".join("?" * len(LIST_ORIGINS)), tuple(LIST_ORIGINS)):
        for ats, row in _ats_rows_from_page("", url or ""):
            bid = _board_id_of(ats, row)
            if bid and (ats, bid) not in known and f"board:{ats}:{bid}" not in tried:
                candidates.setdefault((ats, bid), (company, row))
    added = 0
    for (ats, bid), (company, row) in list(candidates.items())[:DISCOVER_HARVEST]:
        row = {"name": company, **row}
        if ats == "workday" and not row.get("slug"):
            row["slug"] = bid
        try:
            await asyncio.wait_for(_poll_ats_row(client, ats, row, timeout), 120)
            ok = True
        except Exception:  # noqa: BLE001 — dead/blocked board: remember it and move on
            ok = False
        stamp = datetime.now(UTC).isoformat(timespec="seconds")
        with db:
            db.execute("INSERT OR REPLACE INTO discover_state VALUES (?, ?)",
                       (f"board:{ats}:{bid}", "added" if ok else "failed"))
        if ok:
            _append_list_row_raw(ats, company, row)
            _log_discovered([stamp, company, "", "job link", "board added to list", ats, bid])
            added += 1
    return added


def _append_list_row_raw(ats: str, name: str, row: dict[str, str]) -> None:
    path = REPO / CONFIGS[ats]["csv"]
    with path.open(newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    raw = path.read_bytes()
    with path.open("a", newline="", encoding="utf-8") as fh:
        if raw and not raw.endswith(b"\n"):
            fh.write("\n")
        csv.DictWriter(fh, fieldnames=header, extrasaction="ignore", lineterminator="\n").writerow(
            {"name": name, "slug": row.get("slug", ""), "url": row.get("url", ""), "company_name": name})


async def run_discover(timeout: float) -> list[dict[str, Any]]:
    import html as _html

    db = connect()
    _discover_tables(db)
    _careers_cache(db)
    _search_queue(db)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    known = _known_companies()
    seen_new = 0

    def note(name: str, li_slug: str, via: str) -> None:
        nonlocal seen_new
        key = norm_text(name)
        if not key or key in known or norm_text(li_slug) in known:
            return
        with db:
            cur = db.execute("UPDATE discovered SET last_seen=? WHERE name_key=?", (now, key))
            if cur.rowcount == 0:
                db.execute("INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                           (key, name, li_slug, via, now, now, "new", None, None, None, None))
                seen_new += 1

    # 1. Collect: companies hiring for software roles on LinkedIn (a slice of the searches
    #    per pass; a cursor resumes where the last pass stopped).
    cur = db.execute("SELECT v FROM discover_state WHERE k='cursor'").fetchone()
    search_i, page = map(int, (cur[0] if cur else "0,0").split(","))
    requests = 0
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/128"}) as client:
        while requests < DISCOVER_REQUESTS:
            location, keyword = DISCOVER_SEARCHES[search_i % len(DISCOVER_SEARCHES)]
            try:  # a block is retried with back-off and a browser-like connection
                text = await linkedin_get(client, LINKEDIN_SEARCH, {"keywords": keyword, "location": location,
                                                                    "f_JT": "F", "f_TPR": "r604800",
                                                                    "start": page * 10}, tries=3)
            except RuntimeError:
                log_line(f"{'discover':16} LinkedIn still blocking after back-off; resuming next pass")
                break
            requests += 1
            await asyncio.sleep(2.5)
            cards = _LI_CARD.findall(text or "")
            for card in cards:
                co = _LI_COMPANY.search(card)
                if co:
                    li = _LI_COMPANY_SLUG.search(card)
                    note(_html.unescape(co.group(1)).strip(), li.group(1) if li else "", "linkedin")
            page += 1
            if not cards or page >= DISCOVER_MAX_PAGE:
                search_i, page = (search_i + 1) % len(DISCOVER_SEARCHES), 0
        with db:
            db.execute("INSERT OR REPLACE INTO discover_state VALUES ('cursor', ?)", (f"{search_i},{page}",))

        # Employers with full-time software postings on Indeed Canada (Workopolis) and Job Bank.
        board_seen = {"workopolis": 0, "jobbank": 0}

        def note_employer(name: str, title: str, via: str) -> None:
            name = _html.unescape(name or "").strip()
            if name and is_software_fulltime(title) and not _AGENCY_RE.search(name):
                board_seen[via] += 1
                note(name, "", via)

        def rotate(k: str, items: tuple[str, ...], n: int) -> list[str]:
            i = int((db.execute("SELECT v FROM discover_state WHERE k=?", (k,)).fetchone() or ["0"])[0])
            with db:
                db.execute("INSERT OR REPLACE INTO discover_state VALUES (?, ?)", (k, str((i + n) % len(items))))
            return [items[(i + j) % len(items)] for j in range(n)]

        for kw in rotate("workopolis_cursor", DISCOVER_WORKOPOLIS_KEYWORDS, DISCOVER_WORKOPOLIS_PER_PASS):
            try:
                async for jobs in workopolis_pages({"q": kw, "l": "canada", "t": "7"}, timeout):
                    for j in jobs or []:
                        note_employer(j.get("company") or "", j.get("title") or "", "workopolis")
            except Exception as exc:  # noqa: BLE001 — a board failing never stops discovery
                log_line(f"{'discover':16} workopolis '{kw}': {exc}")
        for term in rotate("jobbank_cursor", DISCOVER_JOBBANK_TERMS, DISCOVER_JOBBANK_PER_PASS):
            try:
                for j in await jobbank_search(term, timeout, DISCOVER_JOBBANK_PAGES):
                    note_employer(j.company or "", j.title or "", "jobbank")
            except Exception as exc:  # noqa: BLE001
                log_line(f"{'discover':16} jobbank '{term}': {exc}")
        newgrad_seen = 0
        for url in DISCOVER_NEWGRAD_LISTS:
            try:
                r = await client.get(url, timeout=60)
                r.raise_for_status()
                for x in r.json():
                    name = (x.get("company_name") or "").strip()
                    if (x.get("active") and name and not _AGENCY_RE.search(name)
                            and is_software_fulltime(x.get("title") or "")
                            and any("Canada" in us_canada(loc) for loc in x.get("locations") or [])):
                        newgrad_seen += 1
                        note(name, "", "newgrad")
            except Exception as exc:  # noqa: BLE001
                log_line(f"{'discover':16} new-grad list {url}: {exc}")

        # Companies that hired software interns in Canada last season: polled for this season's.
        await refresh_intern_history(db, client)
        for (company,) in db.execute("SELECT company FROM ca_intern_history").fetchall():
            if not _AGENCY_RE.search(company):
                note(company, "", "history")

        # Companies the community lists / LinkedIn internship search showed in Canada.
        for (company, loc) in db.execute("SELECT DISTINCT company, location FROM postings WHERE origin IN (%s)"
                                         % ",".join("?" * len(LIST_ORIGINS)), tuple(LIST_ORIGINS)).fetchall():
            if company and loc and _CA_LOCATION.search(loc):
                note(company, "", "lists")

        # 2. Place: find each new company's board; the rest go to the web search.
        todo = db.execute("SELECT name_key, name, li_slug, source FROM discovered WHERE status='new' "
                          "ORDER BY first_seen LIMIT ?", (DISCOVER_RESOLVE,)).fetchall()
        sem = asyncio.Semaphore(4)  # each one may read a LinkedIn company page
        placed = queued = careers = 0

        async def place(key: str, name: str, li_slug: str, via: str) -> None:
            nonlocal placed, queued, careers
            async with sem:
                got = await resolve_discovered(db, client, known, key, name, li_slug, via)
            placed += got == "listed"
            careers += got == "careers"
            queued += got == "searching"

        await asyncio.gather(*(place(*t) for t in todo))

        # 3. Harvest: boards that list/LinkedIn job links point to but we don't poll.
        harvested = await harvest_boards(db, client, timeout)

        # 4. Repair: find where companies with dead (404) boards moved.
        repaired, moved = await repair_dead_boards(db, client)
        if repaired:
            log_line(f"{'discover':16} {repaired} dead boards checked, {moved} moved to a board we now poll")
    db.close()
    log_line(f"{'discover':16} {requests} LinkedIn pages, {board_seen['workopolis']} Workopolis, "
             f"{board_seen['jobbank']} Job Bank and {newgrad_seen} new-grad list software jobs, {seen_new} new companies; "
             f"{placed} added to ATS lists, {careers} careers pages found, {queued} queued for web search; "
             f"{harvested} boards added from job links")
    return []


def careers_targets() -> list[dict[str, str]]:
    """The discovery list, polled like an ATS list: every discovered company with a careers page
    or board (found, or guessed while its web search is pending) plus manual overrides. Each row's
    `key` is its careers_cache key; sorted so every batch process slices the same list."""
    db = connect()
    _careers_cache(db)
    _discover_tables(db)
    now = datetime.now(UTC).isoformat(timespec="seconds")
    with db:  # a guessed page is polled while the web search looks for a better one
        db.execute("""INSERT OR IGNORE INTO careers_cache
                      SELECT 'discover:' || name_key, url, NULL, NULL, ? FROM discovered
                      WHERE status IN ('new', 'searching') AND url LIKE 'http%'""", (now,))
    targets: dict[str, dict[str, str]] = {}
    if CAREERS_OVERRIDES.exists():
        with CAREERS_OVERRIDES.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                name = (r.get("name") or "").strip()
                if name and r.get("careers_url"):
                    targets[f"careers:{name.lower()}"] = {"key": f"careers:{name.lower()}", "name": name,
                                                         "url": r["careers_url"].strip()}
    for key, name, url in db.execute("SELECT c.company_key, d.name, c.careers_url FROM careers_cache c JOIN discovered d "
                                     "ON c.company_key = 'discover:' || d.name_key WHERE c.careers_url IS NOT NULL"):
        targets[key] = {"key": key, "name": name, "url": url}
    db.close()
    return [targets[k] for k in sorted(targets)]


def _board_key(row: dict[str, str]) -> str:
    return (row.get("url") or row.get("slug") or row.get("name") or "").strip()


def canada_targets() -> list[dict[str, Any]]:
    """The `canada` watcher's list: every company in canada_software_companies.csv or discovered.csv
    with a board we can poll through an API, so it's checked every few minutes instead of waiting
    its turn in the full ATS lists (25-60 min). Each row names the source the board belongs to;
    postings are saved under that source, so they're the same rows the ATS passes keep.
    Careers pages that need a browser are left to the `careers` pass (it only polls these companies)."""
    names: set[str] = set()
    for path in (CA_COMPANIES_CSV, DISCOVERED_CSV):
        if path.exists():
            with path.open(newline="", encoding="utf-8") as fh:
                names.update(norm_text(r.get("company") or "") for r in csv.DictReader(fh))
    names.discard("")
    skip = set(SINGLE_SOURCES) | set(LIST_SOURCES) | set(MIN_GAP)  # polled on their own schedules already
    ats_sources = sorted(s for s in CONFIGS if s not in skip and CONFIGS[s].get("csv")
                         and (REPO / CONFIGS[s]["csv"]).exists())
    careers = {t["key"]: t for t in careers_targets()}
    db = connect()
    db.execute("""CREATE TABLE IF NOT EXISTS ca_software_hiring (source TEXT, company_key TEXT, company TEXT,
        board_url TEXT, jobs INTEGER, example_title TEXT, example_location TEXT, example_url TEXT,
        first_seen TEXT, last_seen TEXT, PRIMARY KEY (source, company_key))""")
    cutoff = (datetime.now(UTC) - timedelta(days=CA_HIRING_DAYS)).isoformat(timespec="seconds")
    hiring = db.execute("SELECT source, company_key, company FROM ca_software_hiring WHERE last_seen >= ?",
                        (cutoff,)).fetchall()
    # Careers pages that led to a working board: polling them is one API call, no browser.
    resolved = {k for (k,) in db.execute("SELECT company_key FROM careers_cache WHERE ats IS NOT NULL")}
    db.close()

    by_key: dict[str, dict[str, dict[str, str]]] = {}
    by_name: dict[str, tuple[str, str, dict[str, str]]] = {}
    for src in ats_sources:
        rows = company_list(src, None)
        by_key[src] = {_board_key(r): r for r in rows}
        for r in rows:
            by_name.setdefault(norm_text(r.get("name") or ""), (src, _board_key(r), r))
    by_name.pop("", None)

    targets: dict[str, dict[str, Any]] = {}

    def add(src: str, key: str, name: str, row: dict[str, str]) -> None:
        targets.setdefault(f"{src}|{key}", {"key": f"{src}|{key}", "src": src, "company_key": key,
                                            "name": name, "row": row})

    # 1. Boards that had a Canadian software posting in the last 30 days.
    for src, key, company in hiring:
        if _AGENCY_RE.search(company or ""):
            continue
        if src == "careers":
            if key in careers and key in resolved:
                add(src, key, careers[key]["name"], careers[key])
        elif key in by_key.get(src, {}):
            add(src, key, company or by_key[src][key].get("name") or key, by_key[src][key])
    # 2. Discovered companies whose careers page led to a board.
    for key, t in careers.items():
        if key in resolved and norm_text(t["name"]) in names:
            add("careers", key, t["name"], t)
    # 3. The rest of both lists (past Canadian intern employers, LinkedIn finds) found on an ATS list by name.
    covered = {norm_text(t["name"]) for t in targets.values()}
    for n in names - covered:
        if n in by_name:
            src, key, row = by_name[n]
            add(src, key, row.get("name") or key, row)
    return [targets[k] for k in sorted(targets)]


# --- one poller (runs in its own process) ---------------------------------------------



# --- one pass of one source (runs inside a short-lived child process) ----------------



PASS_WALL_MAX = 45 * MIN  # kill a pass still running after this (the longest normal one is ~30 min)

# Sources that update slowly: read them at most this often (seconds), even on faster cycles.
MIN_GAP = {"interndock": 30 * MIN, "cgi": 30 * MIN, "linkedin": 15 * MIN, "workopolis": 30 * MIN,
           "eluta": 60 * MIN,  # reCAPTCHA after a burst; see ELUTA_SEARCHES
           "eightfold": 180 * MIN,  # Eightfold's AWS WAF flags an IP that polls it often
           "wellfound": 30 * MIN}  # 10 browser-rendered pages a pass


async def run_pass(source: str, max_tenants: int | None, timeout: float, tenant_cap: float,
                   offset: int = 0, limit: int | None = None) -> None:
    import logging
    logging.disable(logging.WARNING)
    if source in MIN_GAP:
        stamp = OUT_DIR / ".status" / f"{source}.last"
        # 60 s slack: _supervise schedules these sources exactly MIN_GAP apart.
        if stamp.exists() and time.time() - stamp.stat().st_mtime < MIN_GAP[source] - 60:
            return  # read recently; skip this cycle
        stamp.parent.mkdir(parents=True, exist_ok=True)
        stamp.touch()
    started = time.monotonic()
    single = source in SINGLE_SOURCES or source in LIST_SOURCES
    concurrency = 1 if single else CONCURRENCY.get(source, DEFAULT_CONCURRENCY)
    if not single and isinstance(CONFIGS.get(source, {}).get("max_concurrency"), int):
        concurrency = min(concurrency, CONFIGS[source]["max_concurrency"])
    targets: list[Any] = [source] if single else company_list(source, max_tenants)
    if not single:
        targets = targets[offset:offset + limit] if limit else targets[offset:]
    delay = SLOW_DELAY.get(source, 0)
    sem = asyncio.Semaphore(concurrency)
    db = connect()
    is_baseline = not baseline_done(db, source)
    stats = {"checked": 0, "found": 0, "new": 0, "errors": 0, "blocked": 0, "careers": 0}
    # `canada` targets belong to other sources: keep each one's own rate limit, at half its
    # usual concurrency since that source's own pass may be hitting the same host.
    src_sems: dict[str, asyncio.Semaphore] = {}

    async def check_board(client: httpx.AsyncClient, src: str, target: dict[str, str],
                          company_key: str) -> list[dict[str, Any]]:
        if src == "careers":
            return await asyncio.wait_for(
                careers_fallback(db, client, target, company_key, timeout), CAREERS_PAGE_CAP)
        if src == "greenhouse":
            coro = check_greenhouse(client, target)
        elif src == "workday":
            coro = check_workday(client, target)
        else:
            coro = check_scraper(src, target, timeout)
        try:
            return await asyncio.wait_for(coro, tenant_cap)
        except Exception as exc:  # noqa: BLE001
            if not (_dead_board(exc) or _blocked_board(exc)):
                raise
            # Board is gone or refuses us: read the company's careers page instead.
            items = await asyncio.wait_for(
                careers_fallback(db, client, target, company_key, timeout), tenant_cap * 2)
            stats["careers"] += 1
            return items

    def src_sem(target: Any) -> Any:
        if source != "canada":
            return contextlib.nullcontext()
        src = target["src"]
        if src not in src_sems:
            src_sems[src] = asyncio.Semaphore(max(1, CONCURRENCY.get(src, DEFAULT_CONCURRENCY) // 2))
        return src_sems[src]

    async def one(target: Any, client: httpx.AsyncClient) -> None:
        async with src_sem(target), sem:
            hiring: list[tuple[str, str, str]] = []
            _CA_HIRING.set(hiring)
            save_as, baseline, pause = source, is_baseline, delay
            if source == "canada":
                save_as = target["src"]
                baseline, pause = not baseline_done(db, save_as), SLOW_DELAY.get(save_as, 0)
            try:
                if single:
                    company_key = source
                    cap = 1800 if source in ("careers_search", "discover", "careers") else 900 if source in (
                        "cgi", "linkedin", "workopolis", "eluta", "ycombinator", "google", "wellfound") else tenant_cap * 4  # CGI reads ~59 pages slowly
                    items = await asyncio.wait_for(check_single(source, timeout), cap)
                    for it in items:
                        it["company"] = it.get("company") or source
                else:
                    if source == "canada":
                        row, company_key, company = target["row"], target["company_key"], target["name"]
                    elif source == "careers":
                        row, company_key, company = target, target["key"], target["name"]
                    else:
                        row, company_key = target, _board_key(target)
                        company = (target.get("name") or "").strip() or company_key
                    items = await check_board(client, save_as, row, company_key)
                    for it in items:
                        it["company"] = company
                stats["new"] += save_company(db, save_as, company_key, items, baseline)
                stats["found"] += len(items)
                stats["checked"] += 1
                if hiring:
                    board = row.get("url") if not single else ""
                    save_ca_hiring(db, save_as, company_key, company if not single else source,
                                   board or "", hiring)
                if not single:
                    with db:
                        db.execute("DELETE FROM failures WHERE source=? AND company_key=?", (save_as, company_key))
            except Exception as exc:  # noqa: BLE001 — one company never stops a pass
                stats["errors"] += 1
                if not single:
                    row = target["row"] if source == "canada" else target
                    key = (target["company_key"] if source == "canada"
                           else target.get("key") or _board_key(target))
                    with db:
                        db.execute("INSERT OR REPLACE INTO failures VALUES (?,?,?,?,?,?)",
                                   (save_as, key, (target.get("name") or "").strip(), row.get("url") or "",
                                    f"{type(exc).__name__}: {str(exc)[:200]}",
                                    datetime.now(UTC).isoformat(timespec="seconds")))
                if re.search(r"\b(403|429)\b", str(exc)):
                    stats["blocked"] += 1
            finally:
                if pause:
                    await asyncio.sleep(pause)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
        limits=httpx.Limits(max_connections=concurrency * 2),
        follow_redirects=True,
    ) as client:
        await asyncio.gather(*(one(t, client) for t in targets))
    await close_browser()
    db.close()
    elapsed = time.monotonic() - started
    peak_mb = _peak_rss_mb()
    batch = f" [{offset + 1}-{offset + len(targets)}]" if (offset or limit) and not single else ""
    log_line(f"{source:16}{batch} {stats['checked']}/{len(targets)} checked, {stats['found']} open internships, "
             f"{stats['new']} new, {stats['errors']} errors"
             + (f" ({stats['blocked']} rate-limited)" if stats["blocked"] else "")
             + (f", {stats['careers']} via careers page" if stats["careers"] else "")
             + f" | {elapsed / 60:.1f} min, peak {peak_mb:.0f} MB")


def _peak_rss_mb() -> float:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


def _rss_mb(pid: int) -> float:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except OSError:
        pass
    return 0.0


# --- a watcher script: one group of sources, each on its own timer -----------------

async def _run_child(source: str, args, offset: int, limit: int | None) -> None:
    status_dir = OUT_DIR / ".status"
    status_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(Path(__file__).resolve()), "--pass", source,
           "--timeout", str(args.timeout), "--tenant-cap", str(args.tenant_cap),
           "--offset", str(offset)]
    if limit:
        cmd += ["--limit", str(limit)]
    if args.max_tenants:
        cmd += ["--max-tenants", str(args.max_tenants)]
    with (status_dir / f"{source}.log").open("a") as fh:
        # Own process group, so a kill also takes the headless browsers a pass started.
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=fh, stderr=asyncio.subprocess.STDOUT,
                                                    cwd=str(REPO), start_new_session=True)
        killed = hung = False
        started = time.monotonic()
        while proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                over_mem = _rss_mb(proc.pid) > MEM_CAP_MB.get(source, args.mem_cap_mb)
                # A pass stuck on a browser that never answers held Lever for 2 h (2026-10-07).
                over_time = time.monotonic() - started > PASS_WALL_MAX
                if over_mem or over_time:
                    import signal

                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    killed, hung = over_mem, not over_mem
                    await proc.wait()
    label = f"{source} [{offset + 1}-{offset + limit}]" if limit else source
    if hung:
        log_line(f"{label:16} STOPPED: still running after {PASS_WALL_MAX // MIN} min (saved so far; next pass will run)")
    elif killed:
        log_line(f"{label:16} STOPPED: went over {MEM_CAP_MB.get(source, args.mem_cap_mb)} MB "
                 f"(saved so far; next pass will run)")
    elif proc.returncode:
        log_line(f"{label:16} FAILED (exit {proc.returncode}); see engineering-ats/.status/{source}.log")


async def _supervise(source: str, interval: float, args, stop_after_one: bool) -> None:
    single = source in SINGLE_SOURCES or source in LIST_SOURCES
    interval = max(interval, MIN_GAP.get(source, 0))  # slow sources: exactly MIN_GAP apart
    while True:
        started = time.monotonic()
        if single:
            await _run_child(source, args, 0, None)
        else:
            # Big lists are split into batches, each in its own short-lived process,
            # so memory per process stays small no matter how many companies an ATS has.
            total = len(company_list(source, args.max_tenants))
            batch = min(args.batch, BATCH_SIZE.get(source, args.batch))
            side_by_side = asyncio.Semaphore(PARALLEL_BATCHES.get(source, 1))

            async def run_batch(offset: int) -> None:
                async with side_by_side:
                    await _run_child(source, args, offset, batch)
                    write_csv()
            await asyncio.gather(*(run_batch(o) for o in range(0, total, batch)))
        mark_baseline_done(source)
        write_csv()
        if stop_after_one:
            return
        # Next pass `interval` after this one started; right away if the pass ran longer.
        await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))


def run_group(group: str, sources: list[str], interval_minutes: float, description: str) -> None:
    """Entry point used by the watch_*.py scripts."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--once", action="store_true", help="one pass of every source, then exit")
    parser.add_argument("--only", help="comma-separated subset of this script's sources")
    parser.add_argument("--max-tenants", type=int, help="only the first N companies per ATS (testing)")
    parser.add_argument("--interval", type=float, default=interval_minutes,
                        help=f"minutes between passes (default {interval_minutes})")
    parser.add_argument("--timeout", type=float, default=15, help="per-request timeout (s)")
    parser.add_argument("--tenant-cap", type=float, default=60, help="max seconds per company")
    parser.add_argument("--mem-cap-mb", type=int, default=1500, help="stop a process above this memory (MB)")
    parser.add_argument("--batch", type=int, default=500, help="companies per process (default 500)")
    parser.add_argument("--days", type=int, default=7,
                        help="US_/canada_internships.csv show postings from the last N days (default 7)")
    args = parser.parse_args()
    global WINDOW_DAYS
    WINDOW_DAYS = args.days

    selected = sources
    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        unknown = wanted - set(sources)
        if unknown:
            raise SystemExit(f"not in this script: {sorted(unknown)}; choose from {', '.join(sources)}")
        selected = [s for s in sources if s in wanted]
    available = [s for s in selected
                 if s in SINGLE_SOURCES or s in LIST_SOURCES or s in ("careers", "canada")
                 or (s in CONFIGS and (REPO / CONFIGS[s]["csv"]).exists())]
    skipped = sorted(set(selected) - set(available))
    connect().close()
    log_line(f"[{group}] started: {len(available)} sources every {args.interval:g} min"
             + (" (one pass)" if args.once else "")
             + (f"; skipped (no company list): {', '.join(skipped)}" if skipped else ""))

    async def main() -> None:
        await asyncio.gather(*(_supervise(s, args.interval * MIN, args, args.once) for s in available))

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    total = write_csv()
    log_line(f"[{group}] {'done' if args.once else 'stopped'}: {total} open internships in "
             f"{US_CSV_PATH.relative_to(REPO)} and {CA_CSV_PATH.relative_to(REPO)}")


if __name__ == "__main__":
    # Child-process mode: run exactly one pass of one source, then exit.
    _p = argparse.ArgumentParser()
    _p.add_argument("--pass", dest="source", required=True)
    _p.add_argument("--max-tenants", type=int)
    _p.add_argument("--timeout", type=float, default=15)
    _p.add_argument("--tenant-cap", type=float, default=60)
    _p.add_argument("--offset", type=int, default=0)
    _p.add_argument("--limit", type=int)
    _a = _p.parse_args()
    asyncio.run(run_pass(_a.source, _a.max_tenants, _a.timeout, _a.tenant_cap, _a.offset, _a.limit))
