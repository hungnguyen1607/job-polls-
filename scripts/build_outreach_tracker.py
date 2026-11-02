#!/usr/bin/env python3
"""Build applications/outreach_tracker.xlsx from internships the live watchers already polled.

Joins intern hiring *history* (GitHub intern lists + snapshots, closed/open intern postings)
onto every company in canada_software_companies.csv (~5k). Does not scrape those career
pages looking for Summer 2027. Winter 2027 first — a currently-open Canadian intern posting
counts even if the title doesn't say "Winter 2027". Older terms (Fall 2026, Summer 2026, …)
are kept. Anyone with a dedicated Summer 2027 SWE intern posting is dropped.

The internship watchers call this whenever those CSVs change, so /outreach stays live. This
script is not a second board crawler.

Any company that already has Summer 2027 evidence (open or closed, anywhere -- the live postings
table, the historical snapshots, or a manual override for postings our title-text check can't see)
is excluded entirely: the point of this list is cold-messaging companies *before* they post for
Summer 2027, so one that already has is a miss, not a win.

Ranks rows by how many of the 6 cycles each company matched (then recency), with tier-1 verified
rows always ranked above tier-2 unverified ones. Tier 3 is the backdoor list: companies that hire
students without a public intern req, labeled by evidence (WaterlooWorks-only, UW CS class
profile co-op employer, intern alumni), then "cold ask" startups hiring devs in Montreal/Toronto.

Re-running this keeps any Status / Date Messaged / Notes you already entered on the page, matched
by company name.

    .venv/bin/python scripts/build_outreach_tracker.py
"""
from __future__ import annotations

import csv
import fcntl
import hashlib
import os
import re
import sys
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
import internship_core as core  # noqa: E402

DISCOVERED_CSV = REPO / "engineering-ats" / "discovered.csv"
RECENT_CSV = REPO / "engineering-ats" / "recent_intern_companies.csv"
ALREADY_2027_CSV = REPO / "engineering-ats" / "already_summer2027.csv"
OUT_XLSX = REPO / "applications" / "outreach_tracker.xlsx"
OUT_LOCK = OUT_XLSX.parent / ".outreach.lock"
OUT_HASH = REPO / "engineering-ats" / ".outreach.hash"
HEADER = ["Company", "Last Internship Hired", "Terms Matched", "Example Internship Posting",
          "Example Link", "Careers Page", "ATS", "Status", "Date Messaged", "Notes"]

WHY_TERM_RE = re.compile(r"hired software interns in Canada(?: \(([^)]*)\))?")
TERM_RANK = {  # Winter 2027 first; older terms kept but ranked below.
    "Winter 2027": 0, "Spring 2027": 0.5, "Fall 2026": 1, "Summer 2026": 2, "Winter 2026": 3,
    "Fall 2025": 4, "Summer 2025": 5,
}  # "Summer 2027" isn't here on purpose: it's an exclusion trigger, never a term to rank or show.


def title_terms(example_job: str) -> list[str]:
    """Fallback when discovered.csv's `why` field has no term: the example posting's own title
    often names one."""
    return core.posting_terms(example_job or "", "")


def terms_for(why: str, example_job: str) -> list[str]:
    m = WHY_TERM_RE.search(why)
    raw = m.group(1) if m else None
    if raw and raw != "N/A":
        return [t.strip() for t in raw.split(",")]
    return title_terms(example_job)


def best_term_rank(terms: list[str]) -> float:
    return min((TERM_RANK.get(t, 4) for t in terms), default=5)

# Confirmed Summer 2027 postings our automated checks miss: "Summer 2027" has to appear literally
# in a posting's title/URL (see internship_core._summer_2027), but some companies run "rolling,
# year-round" postings with a generic title (e.g. Microsoft's "Research Intern - Machine Learning"
# in Montreal -- confirmed Summer 2027 via a deadline-tracking aggregator, nothing in the title
# says so) where the real term only shows up in the application deadline, not the title text.
MANUAL_EXCLUDE = set(core.SUMMER_2027_MANUAL)

dedup_key = core.dedup_key  # shared with find_recent_intern_companies.py (see internship_core.py)


def already_summer2027() -> set[str]:
    """Every company_key with Summer 2027 posting evidence (open or closed) anywhere in the
    Simplify/vanshb03 lists -- from find_recent_intern_companies.py's companion output, which
    scans the *full* lists."""
    if not ALREADY_2027_CSV.exists():
        return set()
    return {row["company_key"] for row in csv.DictReader(ALREADY_2027_CSV.open())}


def live_open_summer2027() -> set[str]:
    """name_key of every company with a currently-OPEN Canadian software internship posting for
    Summer 2027 right now, straight from the live pollers' own postings table -- the most
    up-to-date signal, catching postings that opened since recent_intern_companies.csv was last
    generated."""
    db = core.connect()
    out = set()
    for company, title, location, url, posted_at, is_open in db.execute(
            "SELECT company, title, location, url, posted_at, open FROM postings"):
        if not core.is_software_internship(title) or "Canada" not in core.us_canada(location):
            continue
        if is_open and core._summer_2027(title, url, posted_at):
            out.add(core.norm_text(company))
    db.close()
    return out


def xlsx_ok(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 2048:
        return False
    try:
        load_workbook(path, read_only=True)
        return True
    except Exception:  # noqa: BLE001 — truncated/corrupt xlsx from a mid-write crash
        return False


def load_existing_notes() -> dict[str, tuple[str, str, str]]:
    """key -> (status, date_messaged, notes) from the tracker already on disk, so rebuilding
    doesn't wipe out anything you've entered on the /outreach page (works across schema changes:
    looks columns up by header name, not position)."""
    if not xlsx_ok(OUT_XLSX):
        return {}
    ws = load_workbook(OUT_XLSX, read_only=True).active
    rows = ws.iter_rows(values_only=True)
    header = [str(c or "") for c in next(rows)]
    idx = {name: header.index(name) for name in ("Status", "Date Messaged", "Notes") if name in header}
    out = {}
    for row in rows:
        if row and row[0]:
            out[dedup_key(row[0])] = tuple(
                (row[idx[name]] if name in idx and idx[name] < len(row) else default) or default
                for name, default in (("Status", "Not contacted"), ("Date Messaged", ""), ("Notes", "")))
    return out


HIDDEN_CITIES = ("Montreal", "Toronto")  # Montreal first: that's where the user studies


US_CSV = REPO / "engineering-ats" / "us_sponsor_companies.csv"
CLASS_PROFILE = "uw class profile"
# How we know a company hires students off the public boards. Label shown in the
# "Last Internship Hired" column, and the rank within tier 3.
BACKDOOR = {
    "waterlooworks": ("backdoor: WaterlooWorks only", 0),
    "uw exclusive": ("backdoor: WaterlooWorks only", 0),
    "school boards": ("backdoor: school co-op board", 0),
    CLASS_PROFILE: ("backdoor: UW class profile", 1),
    "intern alumni": ("backdoor: intern alumni", 1),
    "hired interns, no live public req": ("backdoor: hired interns, no public req", 2),
    "hires devs, no intern req": ("cold ask: hiring devs, no intern seen", 3),
}


def _public_posters() -> set[str]:
    """Companies that post intern reqs publicly (open now in Canada, or on the US list):
    a class-profile hit for them isn't a backdoor shop."""
    keys: set[str] = set()
    try:
        import find_ww_pipeline as ww
        keys |= ww.public_intern_names()
    except Exception:  # noqa: BLE001
        pass
    if US_CSV.exists():
        for row in csv.DictReader(US_CSV.open(newline="", encoding="utf-8")):
            keys.add(dedup_key(core.outreach_company_name(row.get("company") or "")))
    db = core.connect()
    try:  # every public intern req we or the crowd lists ever saw, any region
        for table in ("swe_intern_history", "ca_intern_history"):
            for (company,) in db.execute(f"SELECT DISTINCT company FROM {table}"):
                keys.add(dedup_key(core.outreach_company_name(company or "")))
    except Exception:  # noqa: BLE001 — old DBs without the history tables
        pass
    finally:
        db.close()
    # Big names whose public intern programs hide behind another spelling (Meta, Amazon).
    keys |= {dedup_key(n) for n in ("Facebook", "Instagram", "AWS", "Amazon Web Services", "Uber ATG",
                                    "Google Deepmind", "Microsoft Research", "SAP Labs", "IBM Extreme Blue")}
    return keys


# Class-profile employers big enough to run public intern programs (so not a backdoor shop),
# even when our history has them under another spelling.
BIG_PUBLIC = re.compile(
    r"\b(opentext|adp|coursera|td|ncr|hrt|hudson river|twitter|xero|infor|wish|noom|keeptruckin|"
    r"uber|lyft|bloomberg|jane street|citadel|databricks|snowflake|datadog|stripe|coinbase|robinhood|"
    r"roblox|riot games|palantir|tesla|netflix|apple|nvidia|intel|amd|qualcomm|huawei|cisco|"
    r"salesforce|linkedin|spotify|pinterest|snap|dropbox|slack|asana|mongo ?db|cloudflare|splunk|"
    r"autodesk|ansys|bentley|capital one|american express|morgan stanley|bmo|scotiabank|"
    r"cpp investments?|omers|hoopp|optrust|kpmg|pwc|bdo|capgemini|ford|honeywell|siemens|nokia|"
    r"bell|rogers|telus|shopify|wealthsimple|faire|1password|arctic wolf|blackberry|d2l|geotab|"
    r"playstation|electronic arts|ea|ubisoft|meta|amazon|aws|tiktok|bytedance|doordash|instacart|"
    r"rippling|imc|akuna|drw|optiver|two sigma|balyasny|marvell|oliver wyman|marsh|desjardins|"
    r"co-?operators|equitable life|economical|definity|empire life|manulife|loblaws?|canada life|"
    r"oanda|ss&c|genesys|dayforce|cerebras|cohere|clio|geotab|index exchange|oliver|carta|census)\b",
    re.I,
)


def _class_profile_rows() -> list[dict[str, str]]:
    try:
        import class_profiles
        import find_ww_pipeline as ww
        employers = class_profiles.load()
    except Exception:  # noqa: BLE001 — offline/no cache: outreach still builds
        return []
    public = _public_posters()
    rows = []
    for name, rec in sorted(employers.items(), key=lambda kv: (-len(kv[1]["years"]), -kv[1]["mentions"])):
        if (dedup_key(core.outreach_company_name(name)) in public or ww.LARGE.search(name)
                or BIG_PUBLIC.search(name)):
            continue
        years = ", ".join(str(y) for y in sorted(rec["years"]))
        rows.append({"company": name, "city": "", "signal": CLASS_PROFILE,
                     "evidence": f"UW CS co-op employer ({years} class profile)",
                     "source_url": rec["source"]})
    return rows


def hidden_shops() -> list[dict[str, str]]:
    """Tier 3: companies that hire students without a public intern req (WaterlooWorks shops,
    UW class-profile co-op employers, past hires) and startups hiring devs in Montreal/Toronto
    that never posted one (Botpress). Ranked by how strong the backdoor evidence is."""
    rows = []
    try:
        import find_ww_pipeline as ww
        for city in HIDDEN_CITIES:
            rows += ww.find_hidden_intern_shops(city, "all", limit=1000)["companies"]
    except Exception:  # noqa: BLE001 — outreach still builds without it
        pass
    return rows + _class_profile_rows()


def _inputs_hash(hidden: list[dict[str, str]]) -> str:
    h = hashlib.md5()
    for path in (core.WINTER_CSV, ALREADY_2027_CSV, DISCOVERED_CSV):
        if path.exists():
            h.update(path.read_bytes())
    h.update(repr([(r["company"], r["signal"], r["evidence"]) for r in hidden]).encode())
    return h.hexdigest()


def write_xlsx(*, force: bool = False) -> int:
    """Rebuild outreach_tracker.xlsx from winter_2027_companies.csv + discovered.csv.
    Does not recrawl boards. Returns how many companies were written, or -1 if unchanged."""
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    OUT_LOCK.touch(exist_ok=True)
    with OUT_LOCK.open("w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        hidden = hidden_shops()
        digest = _inputs_hash(hidden)
        if (not force and OUT_HASH.exists() and xlsx_ok(OUT_XLSX)
                and OUT_HASH.read_text().strip() == digest):
            return -1
        n = _write_xlsx_locked(hidden)
        OUT_HASH.parent.mkdir(parents=True, exist_ok=True)
        OUT_HASH.write_text(digest)
        return n


def _write_xlsx_locked(hidden: list[dict[str, str]] | None = None) -> int:
    intern_rows = list(csv.DictReader(core.WINTER_CSV.open())) if core.WINTER_CSV.exists() else []
    already_live = already_summer2027()
    discovered = list(csv.DictReader(DISCOVERED_CSV.open())) if DISCOVERED_CSV.exists() else []
    strong = [r for r in discovered if r["why"].startswith("hired software interns in Canada")]
    saved = load_existing_notes()
    exclude_keys = live_open_summer2027() | already_live | MANUAL_EXCLUDE

    already_names = {k: k for k in exclude_keys}
    if ALREADY_2027_CSV.exists():
        for row in csv.DictReader(ALREADY_2027_CSV.open()):
            k = (row.get("company_key") or "").strip()
            if k:
                already_names[k] = row.get("company") or k
    for k in MANUAL_EXCLUDE:
        already_names.setdefault(k, k)

    def already_posting_su27(key: str, company: str = "") -> bool:
        return core.already_excludes(key, company or key, already_names)

    skipped = 0
    built: dict[str, list] = {}  # dedup_key -> row (pre-dedup of near-duplicate names)
    ranks: dict[str, tuple] = {}

    def add(company: str, terms: list[str], term_count: str, example_job: str, example_link: str,
            careers_page: str, ats: str, rank: tuple) -> None:
        company = core.outreach_company_name(company)
        dk = dedup_key(company)
        if dk in built:
            alt = f'Also found as "{company}"' if company != built[dk][0] else ""
            if rank < ranks[dk]:  # the better-ranked name/row wins; keep the other as a note
                old_company, old_notes = built[dk][0], built[dk][9]
                built[dk] = [company, ", ".join(terms) or "unspecified", term_count, example_job,
                             example_link, careers_page, ats, "Not contacted", "", old_notes]
                if old_company != company:
                    note = f'Also found as "{old_company}"'
                    built[dk][9] = f"{built[dk][9]}; {note}" if built[dk][9] else note
                ranks[dk] = rank
            elif alt:
                built[dk][9] = f"{built[dk][9]}; {alt}" if built[dk][9] else alt
            return
        built[dk] = [company, ", ".join(terms) or "unspecified", term_count, example_job,
                      example_link, careers_page, ats, "Not contacted", "", ""]
        ranks[dk] = rank

    # Tier 1: internships we already polled (Winter 2027 first, including open CA co-ops
    # whose title doesn't name a term).
    intern_keys = set()
    for rec in intern_rows:
        key = dedup_key(core.outreach_company_name(rec["company"]))
        intern_keys.add(key)
        job = rec.get("example_job") or ""
        loc = job[job.rfind(" (") + 2:-1] if job.endswith(")") and " (" in job else ""
        if (already_posting_su27(key, rec["company"])
                or not core.canada_for_outreach(loc, job, rec.get("example_link") or "", rec["company"])):
            skipped += 1
            continue
        terms = [t for t in (rec.get("terms_matched") or "").split(", ") if t]
        add(rec["company"], terms, rec.get("term_count") or "", rec.get("example_job") or "",
            rec.get("example_link") or "", rec.get("careers_page") or "", rec.get("ats") or "",
            (0, 0 if rec.get("winter_2027") else 1, -int(rec.get("term_count") or 0)))

    # Tier 2: discovered.csv's own single-signal flag, for companies tier 1 didn't independently
    # confirm -- real volume the internships CSVs don't cover, but no verified term count, so these
    # rank below every tier-1 row (rank[0] = 1, vs. 0 for tier 1) and sort to the bottom.
    for r in strong:
        key = dedup_key(core.outreach_company_name(r["company"]))
        if key in intern_keys or already_posting_su27(key, r["company"]):
            if key not in intern_keys:
                skipped += 1
            continue
        terms = terms_for(r["why"], r["example_job"])
        if "Summer 2027" in terms and "Winter 2027" not in terms:
            skipped += 1
            continue
        job = r.get("example_job") or ""
        loc = job[job.rfind(" (") + 2:-1] if job.endswith(")") and " (" in job else ""
        if not core.canada_for_outreach(loc, job, r.get("example_link") or "", r["company"]):
            skipped += 1
            continue
        add(r["company"], terms, "", r["example_job"], r["example_link"], r["careers_page"],
            r["ats"], (1, best_term_rank(terms)))

    # Tier 3: no public intern req, labeled by how we know they hire students. Strongest
    # evidence first; within a label, Montreal, then Toronto, then the rest, then source order.
    for i, r in enumerate(hidden or []):
        key = dedup_key(core.outreach_company_name(r["company"]))
        if key in built or already_posting_su27(key, r["company"]):
            continue
        label, strength = BACKDOOR.get(r["signal"], (f"backdoor: {r['signal']}", 2))
        city = HIDDEN_CITIES.index(r["city"]) if r["city"] in HIDDEN_CITIES else len(HIDDEN_CITIES)
        add(r["company"], [label], "", r["evidence"], r["source_url"], r["source_url"], "",
            (2, strength, city, i))
        if r["city"] and not built[key][9]:
            built[key][9] = r["city"]

    out_rows = []
    for dk, row in built.items():
        status, date_messaged, notes = saved.get(dk, (row[7], row[8], row[9]))
        if dk in saved and row[9] and row[9] not in notes:
            notes = f"{notes}; {row[9]}" if notes else row[9]
        out_rows.append((ranks[dk], row[0].lower(), [row[0], row[1], row[2], row[3], row[4],
                                                       row[5], row[6], status, date_messaged, notes]))
    out_rows.sort(key=lambda t: (t[0], t[1]))

    wb = Workbook()
    ws = wb.active
    ws.title = "Outreach"
    ws.append(HEADER)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for _, _, row in out_rows:
        ws.append(row)

    widths = [28, 34, 12, 42, 46, 36, 14, 14, 14, 30]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    n = len(out_rows)
    if n:
        tab = Table(displayName="Outreach", ref=f"A1:{get_column_letter(len(HEADER))}{n + 1}")
        tab.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
        ws.add_table(tab)
    ws.freeze_panes = "A2"
    tmp = OUT_XLSX.with_name(f".{OUT_XLSX.name}.{os.getpid()}.tmp")
    wb.save(tmp)
    tmp.replace(OUT_XLSX)
    with_count = sum(1 for _, _, r in out_rows if r[2])
    print(f"{n} companies written to {OUT_XLSX} ({with_count} with a verified term count, "
          f"{n - with_count} single-signal only -- unspecified term, lower confidence)")
    if skipped:
        print(f"  (excluded {skipped} companies that already have a Summer 2027 posting -- apply "
              "to those directly instead of cold-messaging)")
    return n


def main() -> None:
    n = core.write_winter_companies()
    print(f"{n} companies -> {core.WINTER_CSV}")


if __name__ == "__main__":
    main()
