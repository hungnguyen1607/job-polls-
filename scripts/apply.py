#!/usr/bin/env python3
"""Pre-fill a job application in a real browser; you review and click Submit.

Supported now: Greenhouse. Fills everything it can from profile.json, uploads
your résumé, highlights unanswered questions in yellow, and NEVER submits.
When you submit and Greenhouse shows its confirmation page, the job is logged
to engineering-ats/applied.csv.

Usage:
    python scripts/apply.py "https://job-boards.greenhouse.io/robinhood/jobs/8199744"
    python scripts/apply.py --row 3        # row 3 of engineering-ats/canada_internships.csv
    python scripts/apply.py --row 3 --us   # row 3 of engineering-ats/US_internships.csv
    python scripts/apply.py URL --headless --screenshot out.png   # test without a window
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx

REPO = Path(__file__).resolve().parent.parent
PROFILE = REPO / "profile.json"
CA_INTERNSHIPS = REPO / "engineering-ats" / "canada_internships.csv"
US_INTERNSHIPS = REPO / "engineering-ats" / "US_internships.csv"
APPLIED = REPO / "engineering-ats" / "applied.csv"


# --- answers -----------------------------------------------------------------------

def pick_option(options: list[str], answer: str) -> str | None:
    """Best option label for a Yes/No/free answer."""
    a = answer.strip().lower()
    for o in options:  # exact
        if o.strip().lower() == a:
            return o
    for o in options:  # starts with ("Yes, I ..." / "No, I ...")
        if o.strip().lower().startswith(a):
            return o
    for o in options:  # contains
        if a and a in o.lower():
            return o
    return None


def decide(label: str, options: list[str], p: dict, job_location: str) -> str | None:
    """Return the answer (an option label for selects) or None to leave it for the user."""
    l = " ".join(label.lower().split())
    loc = job_location.lower()
    in_canada = bool(re.search(r"canada|toronto|montr|vancouver|ottawa|waterloo|calgary|, on\b|, qc\b|, bc\b", loc))

    def opt(ans: str | None) -> str | None:
        if not ans:
            return None
        return pick_option(options, ans) if options else ans

    if re.search(r"if you answered|please (explain|provide|specify)|additional information", l):
        return None
    if "linkedin" in l:
        return opt(p["linkedin"])
    if "github" in l:
        return opt(p["github"])
    if re.search(r"website|portfolio|personal site", l):
        return opt(p["website"])
    if re.search(r"sponsor|visa", l):
        return opt(p["needs_sponsorship_canada"] if in_canada or "canada" in l else p["needs_sponsorship_us"])
    if re.search(r"authori[sz]ed|legally (able|permitted|eligible)|right to work|eligible to work|work permit", l):
        if re.search(r"united states|\bu\.?s\.?\b|\busa\b", l):
            return opt(p["authorized_us"])
        if "canada" in l:
            return opt(p["authorized_canada"])
        return opt(p["authorized_canada"] if in_canada else p["authorized_us"] if loc else None)
    if "international student" in l:
        return opt(p["international_student"])
    if re.search(r"(18|eighteen) years|of legal age", l):
        return opt("Yes")
    if re.search(r"(ever|previously|currently).{0,40}(work|employ)|former employee", l):
        if options:  # e.g. options only list the ways you DID work there
            return next((o for o in options if re.search(r"\bno\b|never|have not|haven.t", o, re.I)), None)
        return p["previously_worked_here"]
    if re.search(r"relative|relationship|family member|related to", l):
        return opt(p["relatives_at_company"])
    if re.search(r"government official|politically exposed|public official", l):
        return opt(p["government_official"])
    if re.search(r"privacy|acknowledge|consent|i agree|certify|attest", l):
        return next((o for o in options if re.search(r"acknowledge|agree|yes|consent|accept", o, re.I)), None) if options else None
    if re.search(r"how did you (hear|find|learn)|source", l):
        if options:
            return (pick_option(options, p["how_did_you_hear"]) or pick_option(options, "website")
                    or pick_option(options, "career") or pick_option(options, "other"))
        return p["how_did_you_hear"]
    if re.search(r"\bschool\b|university|college|institution", l):
        return opt(p["school"])
    if re.search(r"\bdegree\b", l):
        return opt(p["degree"])
    if re.search(r"discipline|major|field of study|program of study", l):
        return opt(p["discipline"])
    if re.search(r"graduat", l):
        if "year" in l:
            return opt(p["graduation_year"])
        if "month" in l:
            return opt(p["graduation_month"])
        return opt(f'{p["graduation_month"]} {p["graduation_year"]}')
    if re.search(r"where are you (located|based)|current (city|location)|city", l):
        return opt(f'{p["city"]}, {p["province_state"]}, {p["country"]}')
    if re.search(r"race|ethnic", l) and p.get("ethnicity"):
        return opt(p["ethnicity"])
    if re.search(r"gender|veteran|disabilit|pronoun|sexual orientation|transgender|hispanic|latino", l):
        key = "gender" if "gender" in l else "veteran" if "veteran" in l else "disability" if "disab" in l else ""
        return opt(p.get(key) or None)
    return None


# --- Greenhouse --------------------------------------------------------------------

def greenhouse_ids(url: str) -> tuple[str, str] | None:
    u = urlparse(url)
    q = parse_qs(u.query)
    if "for" in q and "token" in q:  # embed: boards.greenhouse.io/embed/job_app?for=x&token=123
        return q["for"][0], q["token"][0]
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url)
    if m:
        return m.group(1), m.group(2)
    if "gh_jid" in q:  # company careers page with ?gh_jid=123: board slug unknown
        return None
    return None


async def fill_greenhouse(page, url: str, p: dict, location: str) -> tuple[int, list[str]]:
    ids = greenhouse_ids(url)
    if not ids:
        raise SystemExit(f"Can't read Greenhouse board/job id from {url}")
    board, job = ids
    data = httpx.get(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{job}",
                     params={"questions": "true"}, timeout=20).json()
    location = location or (data.get("location") or {}).get("name", "")
    await page.goto(f"https://job-boards.greenhouse.io/{board}/jobs/{job}", wait_until="domcontentloaded")
    await page.wait_for_selector("#first_name", timeout=30000)

    filled, todo = 0, []
    resume = (REPO / p["resume_pdf"]).resolve()
    for q in data.get("questions", []):
        label = q.get("label", "")
        for f in q.get("fields", []):
            name, ftype = f["name"], f["type"]
            options = [v["label"] for v in f.get("values", [])]
            sel = f"[id='{name}']"
            try:
                if name in ("first_name", "last_name", "email", "phone"):
                    await page.fill(sel, p[name]); filled += 1; continue
                if ftype == "input_file":
                    if name == "resume" and resume.exists():
                        await page.set_input_files("input[type=file][id='resume']", str(resume)); filled += 1
                    elif name == "resume":
                        todo.append(f"{label} (résumé file not found: {resume})")
                    continue
                if name == "resume_text" or (ftype == "textarea" and not q.get("required")):
                    continue
                answer = decide(label, options, p, location)
                if answer is None:
                    if q.get("required"):
                        todo.append(label.splitlines()[0][:90])
                        await highlight(page, sel)
                    continue
                if ftype in ("input_text", "textarea"):
                    await page.fill(sel, answer)
                elif ftype == "multi_value_single_select":
                    await choose_react_select(page, sel, answer)
                elif ftype == "multi_value_multi_select":
                    await page.get_by_label(answer, exact=False).first.check()
                filled += 1
            except Exception as exc:  # noqa: BLE001 — leave that field for the user
                todo.append(f"{label.splitlines()[0][:80]} (couldn't fill: {type(exc).__name__})")
                await highlight(page, sel)
    # Built-in sections that aren't in the question list (country, city, education).
    builtin = [
        ("country", "Canada", r"^canada"),
        ("candidate-location", p["city"], p["city"][:5]),
        ("school--0", p["school"], re.escape(p["school"])),
        ("degree--0", "Bachelor", r"bachelor"),
        ("end-month--0", p["graduation_month"], "^" + p["graduation_month"]),
    ]
    for fid, typed, pattern in builtin:
        if not await page.locator(f"[id='{fid}']").count():
            continue
        try:
            await choose_typed(page, f"[id='{fid}']", typed, pattern)
            filled += 1
        except Exception:  # noqa: BLE001
            todo.append(f"{fid.split('--')[0].replace('-', ' ').title()} (pick it yourself)")
            await highlight(page, f"[id='{fid}']")
    if await page.locator("[id='end-year--0']").count():
        await page.fill("[id='end-year--0']", p["graduation_year"]); filled += 1
    # Picking the country re-renders part of the form; refill anything that got cleared.
    for name in ("first_name", "last_name", "email", "phone"):
        box = page.locator(f"[id='{name}']")
        if await box.count() and not (await box.input_value()).strip():
            await box.fill(p[name])
    return filled, todo


async def choose_typed(page, sel: str, typed: str, pattern: str) -> None:
    """Type into a searchable dropdown and click the first option matching `pattern`."""
    box = page.locator(sel)
    await box.scroll_into_view_if_needed()
    await box.click()
    await box.fill("")
    await box.type(typed, delay=30)  # real keystrokes: some dropdowns only search on keypress
    rx = re.compile(pattern, re.I)
    opts = page.get_by_role("option")
    for _ in range(40):  # options load asynchronously (school/city search): wait up to ~10 s
        texts = await opts.all_inner_texts()
        for i, t in enumerate(texts):
            if rx.search(t):
                await opts.nth(i).click()
                return
        await page.wait_for_timeout(250)
    raise LookupError(f"no option matching {pattern!r}")


async def choose_react_select(page, sel: str, option: str) -> None:
    await choose_typed(page, sel, option[:40], re.escape(option[:60]))


async def highlight(page, sel: str) -> None:
    await page.evaluate(
        """sel => { const el = document.querySelector(sel); if (!el) return;
                    const box = el.closest('.field-wrapper, .select__container, .input-wrapper, div') || el;
                    box.style.outline = '3px solid #f5c400'; box.style.background = '#fff7cc'; }""", sel)


# --- main ---------------------------------------------------------------------------

def from_row(n: int, us: bool = False) -> tuple[str, str, str, str]:
    with (US_INTERNSHIPS if us else CA_INTERNSHIPS).open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    r = rows[n - 1]
    return r["link"], r["company"], r["title"], r["location"]


def log_applied(company: str, title: str, url: str) -> None:
    new = not APPLIED.exists()
    with APPLIED.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new:
            w.writerow(["applied_at", "company", "title", "link"])
        w.writerow([datetime.now().strftime("%Y-%m-%d %H:%M"), company, title, url])


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url", nargs="?")
    ap.add_argument("--row", type=int, help="use row N of engineering-ats/canada_internships.csv")
    ap.add_argument("--us", action="store_true", help="--row reads US_internships.csv instead")
    ap.add_argument("--headless", action="store_true", help="no window (testing)")
    ap.add_argument("--screenshot", help="save a screenshot after filling (testing)")
    args = ap.parse_args()
    company = title = location = ""
    url = args.url
    if args.row:
        url, company, title, location = from_row(args.row, args.us)
    if not url:
        ap.error("give a job URL or --row N")
    if "greenhouse" not in url and "gh_jid" not in url:
        sys.exit("Only Greenhouse is supported so far (Lever and Ashby are next).")
    p = json.loads(PROFILE.read_text())

    from cloakbrowser import launch_async
    browser = await launch_async(headless=args.headless)
    page = await browser.new_page()
    filled, todo = await fill_greenhouse(page, url, p, location)
    print(f"Filled {filled} fields. Not submitted.")
    if todo:
        print("Answer these yourself (highlighted in yellow):")
        for t in todo:
            print(f"  - {t}")
    if args.screenshot:
        await page.screenshot(path=args.screenshot, full_page=True)
    if args.headless:
        await browser.close()
        return
    print("\nReview everything, then click Submit in the browser. Close the window when done.")
    submitted = False
    try:
        while not page.is_closed():
            if re.search(r"confirmation|thank", page.url, re.I):
                submitted = True
            await asyncio.sleep(1)
    except Exception:  # noqa: BLE001 — window closed
        pass
    if submitted:
        log_applied(company, title, url)
        print("Logged to engineering-ats/applied.csv")
    await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
