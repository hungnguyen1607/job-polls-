#!/usr/bin/env python3
"""MCP: find Toronto/Montreal SWE intern shops that don't post publicly
(WaterlooWorks / school-board / intern-alumni class: Mantle, Spellbook).

Cursor: .cursor/mcp.json → ww-pipeline
CLI:    .venv/bin/python scripts/ww_pipeline_mcp.py --city Toronto
        .venv/bin/python scripts/ww_pipeline_mcp.py   # stdio MCP server
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import find_ww_pipeline as ww  # noqa: E402
import outreach_contacts  # noqa: E402
from mcp.server.mcpserver import MCPServer  # noqa: E402

mcp = MCPServer(
    "ww-pipeline",
    version="0.1.0",
    instructions=(
        "Find SWE intern/co-op employers in Toronto and Montreal that hire through "
        "WaterlooWorks (Toronto) or McGill / Concordia / Poly / ÉTS (Montreal) and often "
        "have no public intern req (Mantle, Spellbook, Botpress class). Results list intern "
        "evidence first, then 'hires devs, no intern req' startups to cold-ask. "
        "Use find_hidden_intern_shops first. Then alumni_search_urls "
        "or company_intern_alumni. Do not invent emails. Concordia students should ask "
        "if they hire outside Waterloo, not pretend to be UW. After you confirm a ping "
        "target, call update_outreach_info so /outreach shows it. Never invent emails."
    ),
)


@mcp.tool()
def find_hidden_intern_shops(city: str = "Toronto", school: str = "waterloo",
                             live_search: bool = False, limit: int = 80,
                             include_no_intern: bool = True) -> dict:
    """Companies that take SWE interns/co-ops without a public intern posting
    (WaterlooWorks / school boards / intern alumni; SF is intern alumni), then
    startups hiring devs in the city that never posted an intern req (signal
    'hires devs, no intern req', most-hiring first, e.g. Botpress in Montreal).

    city: Toronto or Montreal. school: waterloo (default), mcgill, concordia,
    polytechnique, ets, or all. Montreal defaults to McGill/Concordia/Poly/ÉTS alumni links.
    limit: up to 1000; total_found says how many exist. include_no_intern=false keeps
    only rows with intern evidence. live_search tries DuckDuckGo/Brave (often blocked).
    Each company includes a LinkedIn intern-alumni people search URL.
    """
    return ww.find_hidden_intern_shops(city, school, live_search, limit, include_no_intern)


@mcp.tool()
def alumni_search_urls(city: str = "Toronto", school: str = "waterloo") -> list[dict]:
    """Google + LinkedIn People search URLs for SWE co-op alumni in a city.

    Open linkedin_people logged in. School filter + 'Software Engineering Co-op'
    {city} (SF uses 'Software Engineering Intern'). Current employer = hidden shop.
    """
    return ww.alumni_search_urls(city, school)


@mcp.tool()
def company_intern_alumni(company: str, city: str = "Toronto",
                          school: str = "waterloo") -> dict:
    """LinkedIn intern/co-op alumni links for one company, plus the outside-Waterloo ask."""
    return ww.company_intern_alumni(company, city, school)


@mcp.tool()
def update_outreach_info(company: str, ping_name: str = "", ping_linkedin: str = "",
                         ping_title: str = "", email: str = "", job: str = "",
                         job_link: str = "", job_id: str = "", city: str = "",
                         note: str = "", channel: str = "") -> dict:
    """Save who to ping onto the /outreach page. LinkedIn preferred; no guessed emails."""
    kwargs = {k: v for k, v in {
        "ping_name": ping_name, "ping_linkedin": ping_linkedin, "ping_title": ping_title,
        "email": email, "job": job, "job_link": job_link, "job_id": job_id,
        "city": city, "note": note, "channel": channel,
    }.items() if v}
    return outreach_contacts.update(company, **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--city", default="", help="Toronto or Montreal (CLI search)")
    parser.add_argument("--school", default="waterloo")
    parser.add_argument("--company", help="company_intern_alumni instead of the list")
    args = parser.parse_args()
    if args.company:
        print(json.dumps(ww.company_intern_alumni(args.company, args.city or "Toronto",
                                                   args.school), indent=2))
        return
    if args.city:
        print(json.dumps(ww.find_hidden_intern_shops(args.city, args.school), indent=2))
        return
    mcp.run()


if __name__ == "__main__":
    main()
