#!/usr/bin/env python3
"""Collect public co-op-hiring signals for UBC, McMaster and McGill.

McGill and McMaster publish no full employer list (myFuture / OSCARplus are login-gated),
and UBC Engineering blocks plain HTTP. This script takes the public signals that do exist
(award winners, testimonials, employer-of-the-year posts) and, when BRAVE_API_KEY is set,
adds LinkedIn alumni evidence (students from the school who interned at the company).

Writes engineering-ats/school_signals.json:
    {company_key: {"company": name, "schools": [...], "evidence": [...]}}

    python scripts/school_programs.py
"""
from __future__ import annotations

import html
import json
import os
import re
import sys
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "engineering-ats" / "school_signals.json"
sys.path.insert(0, str(REPO / "scripts"))
try:
    import internship_core as core
except Exception:  # noqa: BLE001
    core = None
BAD = re.compile(
    r"university|college|hospital|institute|research|government|department|school|"
    r"centre|center|faculty|ministry|agency|laborator|foundation|society|council|"
    r"board of|health|clinic|provincial|federal|municipal|accessibility|biobank|"
    r"\blab\b|regulator|commission|award winners|employer of the year|\bubc\b|"
    r"risk management|energy and atomic",
    re.I,
)
COMPANY_HINT = re.compile(
    r"inc\b|ltd\b|corp\b|technolog|systems|labs?\b|software|digital|data|ai\b|"
    r"group\b|solutions|services|consult|media|games|studios|bank|capital|energy|"
    r"shopify|amazon|google|microsoft|meta|apple|tesla|nvidia|ibm|sap\b|oracle|"
    r"hootsuite|visier|clearly|rbc|salesforce|adobe|cisco|intel|amd|qualcomm",
    re.I,
)


def _clean(text: str) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", "", text or ""))
    return re.sub(r"\s+", " ", text).strip(" .;-–—\"'")


def _key(name: str) -> str:
    if core is not None:
        return core.dedup_key(core.outreach_company_name(name))
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _add(out: dict, name: str, school: str, url: str) -> None:
    name = _clean(name)
    if not name or len(name) < 2 or len(name) > 90 or BAD.search(name):
        return
    key = _key(name)
    if not key:
        return
    rec = out.setdefault(key, {"company": name, "schools": [], "evidence": []})
    if school not in rec["schools"]:
        rec["schools"].append(school)
    if url and url not in rec["evidence"]:
        rec["evidence"].append(url)


def _get(url: str) -> str:
    r = httpx.get(url, timeout=30, follow_redirects=True,
                  headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/128"})
    r.raise_for_status()
    return r.text


def ubc(out: dict) -> None:
    """UBC Science Co-op award winners + UBC Co-op testimonials (public)."""
    url = "https://sciencecoop.ubc.ca/employers/award"
    try:
        text = _get(url)
    except httpx.HTTPError as e:
        print(f"UBC awards failed: {e}")
        text = ""
    for m in re.finditer(r"<li[^>]*>(.*?)</li>", text, re.S | re.I):
        item = _clean(m.group(1))
        if not item or len(item) > 90:
            continue
        # Supervisor entries look like "Jane Doe, Company"; employer awards are just company.
        name = item.rsplit(",", 1)[1].strip() if "," in item else item
        if "," in item and not COMPANY_HINT.search(name) and not re.search(
                r"\b(inc|ltd|corp|technolog|systems|labs?|group|software|solutions|services)\b",
                name, re.I):
            continue
        _add(out, name, "ubc", url)

    url2 = "https://coop.ubc.ca/employers/testimonials"
    try:
        text2 = _get(url2)
    except httpx.HTTPError as e:
        print(f"UBC testimonials failed: {e}")
        text2 = ""
    for m in re.finditer(r"<strong[^>]*>(.*?)</strong>", text2, re.S | re.I):
        name = _clean(m.group(1))
        if name and not re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", name):
            _add(out, name, "ubc", url2)
    for name in ("Clearly", "Visier", "Hootsuite", "RBC Ventures", "SAP",
                 "Agriculture and Agri-Food Canada", "Optimal Performance Clinic"):
        _add(out, name, "ubc", url2)


def mcmaster(out: dict) -> None:
    """McMaster Science Employer of the Year posts (public); OSCARplus is gated."""
    urls = (
        "https://careers.science.mcmaster.ca/?post_type=post&p=4245",
        "https://careers.science.mcmaster.ca/employers/co-op-employer-of-the-year-award/",
    )
    for url in urls:
        try:
            text = _get(url)
        except httpx.HTTPError as e:
            print(f"McMaster failed ({url}): {e}")
            continue
        for m in re.finditer(r"<h[1-4][^>]*>(.*?)</h[1-4]>|<li[^>]*>(.*?)</li>", text, re.S | re.I):
            item = _clean(m.group(1) or m.group(2) or "")
            if not item or len(item) > 90:
                continue
            if COMPANY_HINT.search(item) or re.search(
                    r"\b(award|winner|employer of the year)\b", item, re.I):
                _add(out, item, "mcmaster", url)


def brave_alumni(out: dict, school: str, label: str) -> None:
    """Optional: students from the school who interned at a company (LinkedIn snippets)."""
    if not os.environ.get("BRAVE_API_KEY"):
        return
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        import find_contacts as fc
        import httpx as hx

        queries = [
            f'site:linkedin.com/in "{label}" ("software engineer intern" OR "software developer intern" OR co-op)',
            f'site:linkedin.com/in "{label}" (intern OR co-op) software Toronto',
            f'site:linkedin.com/in "{label}" (intern OR co-op) software Montreal',
            f'site:linkedin.com/in "{label}" (intern OR co-op) software Vancouver',
        ]
        with hx.Client(timeout=20, follow_redirects=True) as client:
            for query in queries:
                try:
                    results = fc.search(client, query, count=10)
                except Exception as exc:  # noqa: BLE001
                    print(f"{school}: search failed ({exc})")
                    continue
                for r in results:
                    title = r.get("title") or ""
                    parts = [p.strip() for p in re.split(r"\s[-–|]\s", title) if p.strip()]
                    if len(parts) >= 2:
                        _add(out, parts[-1].replace("| LinkedIn", "").strip(), school, "brave")
    except Exception as exc:  # noqa: BLE001
        print(f"{school}: Brave alumni search failed ({exc})")


def main() -> None:
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        import find_contacts as fc
        fc._load_dotenv()
    except Exception:  # noqa: BLE001
        pass
    out: dict[str, dict] = {}
    ubc(out)
    mcmaster(out)
    for school, label in (("mcgill", "McGill"), ("mcmaster", "McMaster"), ("ubc", "UBC")):
        brave_alumni(out, school, label)
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    from collections import Counter
    print(f"wrote {len(out)} companies -> {OUT.relative_to(REPO)}")
    print("schools:", dict(Counter(s for v in out.values() for s in v["schools"])))


if __name__ == "__main__":
    main()
