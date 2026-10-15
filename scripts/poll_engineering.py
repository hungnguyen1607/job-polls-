#!/usr/bin/env python3
"""Poll every ATS and keep companies hiring for software / IT / data roles.

Each ATS list in ``ats-companies/`` (read-only) is polled by its own worker
process, and all workers run at the same time. A worker runs the repo's
existing scraper for its ATS (listing only, no per-job description calls) on
every company; a company is kept when at least one open posting looks like
software / IT / data work (title or department).

Workday is special-cased: instead of scraping whole boards (hours), it asks
each site's search API for a few engineering keywords and stops at the first
engineering title — one hit is enough.

Output (``engineering-ats/``):
    engineering_companies.csv   ats,name,slug,url — every kept company from every
                                ATS, values copied unchanged from ats-companies/
    _errors.csv                 companies that failed or timed out (re-poll these)
While running, each worker writes ``engineering-ats/.parts/<ats>.*``; the parts
are merged into the two files above when all workers finish.

Usage:
    python scripts/poll_engineering.py                       # all ATSes
    python scripts/poll_engineering.py --max-tenants 5       # quick test, 5 companies per ATS
    python scripts/poll_engineering.py --ats greenhouse,lever,ashby
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from ats_scrapers.scrapers.workday import URL_PATTERN  # noqa: E402
from scripts.run_pipeline import CONFIGS  # noqa: E402

OUT_DIR = REPO / "engineering-ats"
PARTS_DIR = OUT_DIR / ".parts"
OUT_FIELDS = ["name", "slug", "url", "country"]
ERROR_FIELDS = ["name", "url", "error"]
# Not company lists: Mercor is a single marketplace seed row; SEEK rows are
# whole country job boards (JobStreet, JobsDB, SEEK AU/NZ).
SKIP_ATS = {"mercor"}
NEEDS_DETAIL_FOR_LOCATION = {"taleo"}

# --- engineering classifier -------------------------------------------------

_TECH = (
    r"software|data|machine learning|ml|ai|platform|cloud|devops|dev ops|"
    r"site reliability|reliability|qa|quality assurance|test|automation|security|"
    r"network|systems?|back[- ]?end|front[- ]?end|full[- ]?stack|mobile|ios|"
    r"android|firmware|embedded|infrastructure|web|application|integration|"
    r"database|solutions?|salesforce|sap|analytics|computer vision|robotics software"
)
ENG_TITLE_RE = re.compile(
    r"\bsoftware\b|\bdevelop(er|ers|ment engineer)\b|\bprogrammer\b|\bcoder\b|"
    r"\b(swe|sde|sdet|sre)\b|\bdevops\b|\bdev ops\b|\bdevsecops\b|\bmlops\b|"
    r"\bfull[- ]?stack\b|\b(front|back)[- ]?end (developer|engineer|software|web|lead)\b|"
    r"\b(frontend|backend)\b(?! loader)|\bweb developer\b|"
    r"\b(ios|android|mobile) (developer|engineer)\b|"
    rf"\b({_TECH})\s+(engineer|engineering|architect|developer)\b|"
    rf"\b(engineer|engineering|architect|developer)[^,()]*\b({_TECH})\b|"
    r"\bdata (scientist|analyst|engineer|architect)\b|\bmachine learning\b|"
    r"\b(ai|ml) (engineer|researcher|scientist|developer)\b|"
    r"\bcyber ?security\b|\binformation security\b|\bsecurity analyst\b|"
    r"\b(it|i\.t\.)\b|\binformation technology\b|\binformatique\b|\binformatik\b|"
    r"\binform[aá]tic[oa]s?\b|\b(analista|ingenier[oa]|engenheir[oa]|t[eé]cnic[oa]) (de |em )?(sistemas|ti|software)\b|"
    r"\bsystems? administrator\b|\bsysadmin\b|\bnetwork (administrator|engineer|analyst)\b|"
    r"\bhelp ?desk\b|\bservice desk\b|\btech(nical)? support\b|"
    r"\bcloud (engineer|architect|administrator)\b|\bdatabase administrator\b|\bdba\b|"
    r"\bentwickler\b|\bdéveloppeur\b|\bdeveloppeur\b|\bdesarrollador\b|\bdesenvolvedor\b|"
    r"\bstack\b.*\b(engineer|developer)\b|\bscrum master\b|\btech lead\b|\bcto\b|"
    r"\bmember of (the )?technical staff\b|\bmts\b",
    re.I,
)
ENG_DEPT_RE = re.compile(
    r"\b(engineering|software|r&d|research (and|&) development|technology|tech|"
    r"information technology|it|data|product development|digital|devops|infrastructure)\b",
    re.I,
)
DEPT_ROLE_RE = re.compile(
    r"\b(engineer\w*|developer|programmer|architect|scientist|analyst|administrator|"
    r"intern\w*|co-?op|coop|devops|qa|tester|researcher|product manager)\b",
    re.I,
)
NON_SOFTWARE_ENG_RE = re.compile(
    r"\b(mechanical|civil|chemical|structural|process|manufacturing|industrial|"
    r"field service|sales|electrical|hvac|piping|geotechnical|environmental|"
    r"maintenance|facilities|plant|construction|mining|petroleum|biomedical|"
    r"packaging|quality engineer(?! software))\b",
    re.I,
)



# Non-engineering roles that mention tech words ("IT Recruiter", "AI Trainer").
NOT_ENG_ROLE_RE = re.compile(
    r"\b(recruit\w*|talent acquisition|sourcer|trainer|tutor|sales|account executive|"
    r"account manager|business development|marketing|teacher|instructor)\b",
    re.I,
)


def is_engineering(title: str, department: str | None = None) -> bool:
    title = title or ""
    if NOT_ENG_ROLE_RE.search(title):
        return False
    if ENG_TITLE_RE.search(title):
        # "Electrical Engineer" etc. only count if they also name a software area.
        if NON_SOFTWARE_ENG_RE.search(title) and not re.search(
            r"\b(software|data|devops|cloud|it|firmware|embedded|systems?)\b", title, re.I
        ):
            return False
        return True
    # A tech department only counts when the title is also a technical role
    # ("Summer Intern" / "Analyst" in Engineering yes; "Lab Technician" in R&D no).
    if (department and ENG_DEPT_RE.search(department)
            and DEPT_ROLE_RE.search(title) and not NON_SOFTWARE_ENG_RE.search(title)):
        return True
    return False


# --- US / Canada location check ----------------------------------------------

_US_STATES = (
    "alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|"
    "georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|"
    "massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|"
    "new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|"
    "oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|"
    "texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming|"
    "district of columbia"
)
_US_CITIES = (
    "nyc|san francisco|seattle|austin|boston|chicago|los angeles|atlanta|denver|dallas|"
    "houston|san jose|san diego|palo alto|mountain view|sunnyvale|menlo park|redmond|"
    "bellevue|cambridge, ma|pittsburgh|philadelphia|phoenix|miami|raleigh|salt lake city|"
    "minneapolis|detroit|nashville|portland, or|brooklyn|manhattan|silicon valley|bay area"
)
US_TEXT_RE = re.compile(
    rf"\b(united states|u\.s\.a?\.?|{_US_STATES}|{_US_CITIES})\b|\bamerica\b(?! latina)", re.I
)
# Case-sensitive codes. IN/DE are skipped (India/Germany use them too).
US_CODE_RE = re.compile(
    r"\b(USA|US)\b|^US-|, ?(AL|AK|AZ|AR|CA|CO|CT|FL|GA|HI|ID|IL|IA|KS|KY|LA|ME|MD|MA|MI|"
    r"MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|"
    r"WY|DC)\b"
)
CA_TEXT_RE = re.compile(
    r"\b(canada|canadian|ontario|qu[eé]bec|british columbia|alberta|manitoba|"
    r"saskatchewan|nova scotia|new brunswick|newfoundland|prince edward island|yukon|"
    r"toronto|ottawa|vancouver|montr[eé]al|waterloo|kitchener|calgary|edmonton|"
    r"mississauga|halifax|winnipeg|burnaby|markham|oakville|brampton|hamilton, on|"
    r"london, on|victoria, bc|surrey, bc|richmond, bc|gatineau|laval|longueuil|"
    r"saskatoon|regina|fredericton|moncton|st\.? john'?s|kelowna|guelph|vaughan)\b",
    re.I,
)
# Case-sensitive. NL/PE are skipped (Netherlands/Peru use them too).
CA_CODE_RE = re.compile(r"^CA-|^CAN\b|\bCAN\b|, ?(ON|BC|QC|AB|MB|SK|NS|NB|PEI|YT|NT|NU)\b")


def us_canada(location: str | None, country_iso: str | None = None) -> set[str]:
    """Return which of {"US", "Canada"} a job's location points to (may be empty)."""
    if country_iso:
        code = country_iso.upper()
        if code == "US":
            return {"US"}
        if code == "CA":
            return {"Canada"}
        if len(code) == 2:
            return set()  # structured country says somewhere else
    found: set[str] = set()
    for part in re.split(r"\s*[|;/]\s*|\n", location or ""):
        part = part.strip()
        if not part:
            continue
        if CA_CODE_RE.search(part) or CA_TEXT_RE.search(part):
            found.add("Canada")
        # "CA-AB-Calgary" is Canada; ", CA" (California) is US.
        if US_CODE_RE.search(part) or US_TEXT_RE.search(part):
            if not part.startswith("CA-"):
                found.add("US")
    return found


def country_label(found: set[str]) -> str:
    if found >= {"US", "Canada"}:
        return "US+Canada"
    return next(iter(found), "")


# --- generic per-ATS poller (existing scrapers) ----------------------------

async def poll_company_generic(ats: str, row: dict[str, str], args) -> set[str]:
    cfg = CONFIGS[ats]
    slug = cfg["slug"](row)
    if not slug:
        raise ValueError("no usable slug in CSV row")
    kwargs = cfg["kwargs"](row) if cfg.get("kwargs") else {}
    scraper = cfg["scraper"](slug, timeout=args.timeout, **kwargs)
    # Taleo listings carry no location; only the detail pages do.
    scraper.include_descriptions = ats in NEEDS_DETAIL_FOR_LOCATION
    jobs = await asyncio.wait_for(scraper.afetch(), timeout=args.tenant_cap)
    found: set[str] = set()
    for j in jobs:
        if is_engineering(j.title, j.department or j.team):
            found |= us_canada(j.location, j.country_iso)
            if len(found) == 2:
                break
    return found


# --- Workday fast path -------------------------------------------------------

WORKDAY_KEYWORDS = ["software", "developer", "engineer", "IT", "data"]


async def poll_company_workday(
    client: httpx.AsyncClient, row: dict[str, str], args
) -> set[str]:
    url = (row.get("url") or "").strip().rstrip("/")
    m = URL_PATTERN.match(url)
    if not m:
        raise ValueError(f"unrecognized Workday URL {url!r}")
    co, inst, site = m.group("company"), m.group("instance"), m.group("site")
    base = f"https://{co}.{inst}.myworkdayjobs.com/wday/cxs/{co}/{site}"
    retryable = (403, 429, 500, 502, 503, 504)

    async def call(method: str, endpoint: str, body: Any = None) -> dict[str, Any]:
        for attempt in range(2):
            r = await client.request(method, endpoint, json=body)
            if r.status_code == 200:
                return r.json()
            if r.status_code not in retryable or attempt:
                raise RuntimeError(f"Workday HTTP {r.status_code}")
            await asyncio.sleep(2)
        raise RuntimeError("unreachable")

    async def search(text: str) -> list[dict[str, Any]]:
        body = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": text}
        return (await call("POST", f"{base}/jobs", body)).get("jobPostings") or []

    rollup = re.compile(r"^\d+ Locations?$", re.I)
    details_left = 5  # cap detail lookups for "N Locations" rollups per company

    async def where(posting: dict[str, Any]) -> set[str]:
        nonlocal details_left
        text = posting.get("locationsText") or ""
        if rollup.match(text.strip()) and posting.get("externalPath") and details_left > 0:
            details_left -= 1
            info = (await call("GET", base + posting["externalPath"])).get("jobPostingInfo") or {}
            locs = [info.get("location") or ""] + list(info.get("additionalLocations") or [])
            country = (info.get("country") or {}).get("descriptor") or ""
            return us_canada(" | ".join(str(x) for x in locs + [country] if x))
        return us_canada(text)

    async def run() -> set[str]:
        found: set[str] = set()
        seen: set[str] = set()
        for text in ["", *WORKDAY_KEYWORDS]:
            for p in await search(text):
                key = p.get("externalPath") or p.get("title") or ""
                if key in seen or not is_engineering(p.get("title") or ""):
                    continue
                seen.add(key)
                found |= await where(p)
                if len(found) == 2:
                    return found
        return found

    return await asyncio.wait_for(run(), timeout=args.tenant_cap)


# --- orchestration -----------------------------------------------------------

# --- worker: one ATS per process ----------------------------------------------

def load_rows(ats: str) -> list[dict[str, str]]:
    with (REPO / CONFIGS[ats]["csv"]).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_rows(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(path)


async def run_worker(ats: str, args) -> None:
    rows = load_rows(ats)
    if args.max_tenants:
        rows = rows[: args.max_tenants]
    cfg = CONFIGS[ats]
    limit = args.concurrency
    if isinstance(cfg.get("max_concurrency"), int):
        limit = min(limit, cfg["max_concurrency"])
    sem = asyncio.Semaphore(max(1, limit))
    delay = float(cfg.get("tenant_delay_seconds", 0))
    kept: list[tuple[int, dict[str, str], str]] = []
    errors: list[dict[str, str]] = []
    state = {"ats": ats, "total": len(rows), "done": 0, "kept": 0, "errors": 0}
    progress_path = PARTS_DIR / f"{ats}.progress.json"
    started = time.monotonic()

    def save_progress() -> None:
        state["elapsed"] = round(time.monotonic() - started)
        tmp = progress_path.with_name(progress_path.name + ".tmp")
        tmp.write_text(json.dumps(state))
        tmp.replace(progress_path)

    async def heartbeat() -> None:
        while True:
            save_progress()
            await asyncio.sleep(10)

    async def one(index: int, row: dict[str, str], client: httpx.AsyncClient) -> None:
        found: set[str] = set()
        async with sem:
            try:
                if ats == "workday":
                    found = await poll_company_workday(client, row, args)
                else:
                    found = await poll_company_generic(ats, row, args)
            except Exception as exc:  # noqa: BLE001 — one bad company must not stop the ATS
                state["errors"] += 1
                reason = ("timeout" if isinstance(exc, asyncio.TimeoutError)
                          else f"{type(exc).__name__}: {str(exc)[:150]}")
                errors.append({"name": row.get("name") or "",
                               "url": row.get("url") or "", "error": reason})
            finally:
                state["done"] += 1
                if delay:
                    await asyncio.sleep(delay)
        if found:
            state["kept"] += 1
            kept.append((index, row, country_label(found)))

    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(
        timeout=args.timeout,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        limits=httpx.Limits(max_connections=max(8, limit * 2)),
    ) as client:
        beat = asyncio.create_task(heartbeat())
        await asyncio.gather(*(one(i, row, client) for i, row in enumerate(rows)))
        beat.cancel()

    out = [
        {"name": row.get("name", ""), "slug": row.get("slug", ""), "url": row.get("url", ""),
         "country": country}
        for _i, row, country in sorted(kept, key=lambda item: item[0])
    ]
    write_rows(PARTS_DIR / f"{ats}.csv", OUT_FIELDS, out)
    write_rows(PARTS_DIR / f"{ats}.errors.csv", ERROR_FIELDS, errors)
    state["finished"] = True
    save_progress()


# --- launcher: one worker process per ATS, then merge -------------------------

def read_progress(ats: str) -> dict[str, Any]:
    try:
        return json.loads((PARTS_DIR / f"{ats}.progress.json").read_text())
    except (OSError, ValueError):
        return {"ats": ats, "total": 0, "done": 0, "kept": 0, "errors": 0}


def merge(selected: list[str]) -> tuple[int, int]:
    kept: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    for ats in selected:
        for name, bucket in ((f"{ats}.csv", kept), (f"{ats}.errors.csv", errors)):
            path = PARTS_DIR / name
            if path.exists():
                with path.open(newline="", encoding="utf-8") as fh:
                    bucket.extend(csv.DictReader(fh))
    write_rows(OUT_DIR / "engineering_companies.csv", OUT_FIELDS, kept)
    write_rows(OUT_DIR / "_errors.csv", ERROR_FIELDS, errors)
    return len(kept), len(errors)


async def launch(selected: list[str], args) -> None:
    started = time.monotonic()
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    passthrough = ["--timeout", str(args.timeout), "--tenant-cap", str(args.tenant_cap),
                   "--concurrency", str(args.concurrency)]
    if args.max_tenants:
        passthrough += ["--max-tenants", str(args.max_tenants)]

    procs: dict[str, asyncio.subprocess.Process] = {}
    for ats in selected:
        log = (PARTS_DIR / f"{ats}.log").open("w")
        procs[ats] = await asyncio.create_subprocess_exec(
            sys.executable, str(Path(__file__).resolve()), "--worker", ats, *passthrough,
            stdout=log, stderr=asyncio.subprocess.STDOUT, cwd=str(REPO),
        )
    print(f"Started {len(procs)} pollers (one process per ATS) → "
          f"{OUT_DIR.relative_to(REPO)}/engineering_companies.csv", flush=True)

    async def wait_one(ats: str) -> None:
        code = await procs[ats].wait()
        p = read_progress(ats)
        status = "done" if code == 0 else f"CRASHED (exit {code}, see .parts/{ats}.log)"
        print(f"  {ats:16} {status}: kept {p['kept']}/{p['total']}, errors {p['errors']}, "
              f"{p.get('elapsed', 0) / 60:.1f} min", flush=True)

    async def report() -> None:
        while True:
            await asyncio.sleep(args.progress_every)
            ps = [read_progress(a) for a in selected]
            running = [p for p, a in zip(ps, selected) if procs[a].returncode is None]
            print(f"[{(time.monotonic() - started) / 60:5.1f} min] "
                  f"{sum(p['done'] for p in ps)}/{sum(p['total'] for p in ps)} companies, "
                  f"kept {sum(p['kept'] for p in ps)}, errors {sum(p['errors'] for p in ps)}; "
                  f"still running: " + ", ".join(f"{p['ats']} {p['done']}/{p['total']}" for p in running),
                  flush=True)

    reporter = asyncio.create_task(report())
    await asyncio.gather(*(wait_one(a) for a in selected))
    reporter.cancel()

    kept, errors = merge(selected)
    crashed = [a for a in selected if procs[a].returncode != 0]
    total = sum(read_progress(a)["total"] for a in selected)
    elapsed = time.monotonic() - started
    print(f"\nDone in {elapsed / 60:.1f} min. Kept {kept} of {total} companies; {errors} errors.")
    print(f"  {OUT_DIR.relative_to(REPO)}/engineering_companies.csv")
    print(f"  {OUT_DIR.relative_to(REPO)}/_errors.csv")
    if crashed:
        print(f"  Crashed pollers (results missing, kept .parts/ for debugging): {', '.join(crashed)}")
    else:
        for path in PARTS_DIR.iterdir():
            path.unlink()
        PARTS_DIR.rmdir()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ats", help="comma-separated ATS names (default: all company lists)")
    parser.add_argument("--max-tenants", type=int, help="only poll the first N companies per ATS")
    parser.add_argument("--concurrency", type=int, default=16, help="companies at once per ATS (capped by the ATS's own limit)")
    parser.add_argument("--timeout", type=float, default=20, help="per-request timeout (s)")
    parser.add_argument("--tenant-cap", type=float, default=120, help="max seconds per company")
    parser.add_argument("--progress-every", type=float, default=60, help="seconds between progress lines")
    parser.add_argument("--worker", help=argparse.SUPPRESS)  # internal: poll one ATS
    args = parser.parse_args()

    import logging
    logging.disable(logging.WARNING)

    if args.worker:
        asyncio.run(run_worker(args.worker, args))
        return

    available = sorted(a for a, c in CONFIGS.items()
                       if not c.get("singleton") and a not in SKIP_ATS and (REPO / c["csv"]).exists())
    selected = [a.strip() for a in args.ats.split(",")] if args.ats else available
    unknown = [a for a in selected if a not in available]
    if unknown:
        parser.error(f"unknown ATS {unknown}; available: {', '.join(available)}")
    asyncio.run(launch(selected, args))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
