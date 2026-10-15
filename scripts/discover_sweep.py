"""One full sweep of LinkedIn for companies hiring full-time software roles in Canada.

Every company not already in a list is placed right away (internship_core.resolve_discovered):
onto its ATS list, or its careers page into the careers cache that the `careers` source polls
every 10 min, or into the web-search queue. Before the sweep, every company still waiting for a
web search is retried the same way. LinkedIn blocks are waited out (back-off), never a reason to stop.
The cursor is saved after every page, so a restarted sweep resumes where it stopped.

    .venv/bin/python scripts/discover_sweep.py            # resume / run the sweep
    .venv/bin/python scripts/discover_sweep.py --restart  # start over from the first search
    .venv/bin/python scripts/discover_sweep.py --forever  # resume, then start a new sweep after each one
    (runs as the internship-discover-sweep systemd user service: --forever --gap 6)
"""
from __future__ import annotations

import argparse
import asyncio
import html
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import internship_core as core  # noqa: E402

SEARCHES = core.DISCOVER_SEARCHES  # same keywords x Canadian locations as the every-30-min `discover`
MAX_PAGE = 100  # LinkedIn's guest search stops at ~1000 results
PAGE_GAP = 2.0


async def main(restart: bool) -> None:
    db = core.connect()
    core._discover_tables(db)
    core._careers_cache(db)
    core._search_queue(db)
    known = core._known_companies()
    cur = None if restart else db.execute("SELECT v FROM discover_state WHERE k='sweep_cursor'").fetchone()
    search_i, page = map(int, (cur[0] if cur else "0,0").split(","))
    counts = {"new": 0, "listed": 0, "careers": 0, "searching": 0}
    sem = asyncio.Semaphore(4)
    pending: set[asyncio.Task] = set()

    async def resolve(key: str, name: str, li_slug: str, via: str) -> None:
        async with sem:
            try:
                got = await core.resolve_discovered(db, client, known, key, name, li_slug, via)
            except Exception as exc:  # noqa: BLE001 — one company never stops the sweep
                core.log_line(f"{'discover_sweep':16} {name}: {exc}")
                return
        counts[got] += 1

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        if (search_i, page) == (0, 0):
            # A fresh sweep first retries companies still waiting for the web search through their
            # LinkedIn website (one LinkedIn request each, so not on every resume).
            waiting = db.execute("SELECT name_key, name, li_slug FROM discovered WHERE status IN ('new', 'searching')"
                                 ).fetchall()
            await asyncio.gather(*(resolve(k, n, s or "", "retry") for k, n, s in waiting))
            core.log_line(f"{'discover_sweep':16} retried {len(waiting)} waiting companies: "
                          f"{counts['listed']} on ATS lists, {counts['careers']} careers pages")
            counts.update(listed=0, careers=0, searching=0)

        while search_i < len(SEARCHES):
            location, keyword = SEARCHES[search_i]
            try:
                text = await core.linkedin_get(client, core.LINKEDIN_SEARCH, {
                    "keywords": keyword, "location": location, "f_JT": "F", "f_TPR": "r2592000",
                    "start": page * 10}, tries=8)
            except RuntimeError:
                core.log_line(f"{'discover_sweep':16} LinkedIn blocked for an hour straight; pausing 15 min")
                await asyncio.sleep(900)
                continue
            cards = core._LI_CARD.findall(text or "")
            now = datetime.now(UTC).isoformat(timespec="seconds")
            for card in cards:
                co = core._LI_COMPANY.search(card)
                if not co:
                    continue
                name = html.unescape(co.group(1)).strip()
                li = core._LI_COMPANY_SLUG.search(card)
                li_slug = li.group(1) if li else ""
                key = core.norm_text(name)
                if not key or key in known or core.norm_text(li_slug) in known:
                    continue
                with db:
                    if db.execute("UPDATE discovered SET last_seen=? WHERE name_key=?", (now, key)).rowcount:
                        continue
                    db.execute("INSERT INTO discovered VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                               (key, name, li_slug, "linkedin", now, now, "new", None, None, None, None))
                counts["new"] += 1
                t = asyncio.create_task(resolve(key, name, li_slug, "linkedin"))
                pending.add(t)
                t.add_done_callback(pending.discard)
            page += 1
            if not cards or page >= MAX_PAGE:
                core.log_line(f"{'discover_sweep':16} done '{keyword}' in {location} ({page} pages); so far "
                              f"{counts['new']} new companies: {counts['listed']} on ATS lists, "
                              f"{counts['careers']} careers pages, {counts['searching']} queued for web search")
                search_i, page = search_i + 1, 0
            with db:
                db.execute("INSERT OR REPLACE INTO discover_state VALUES ('sweep_cursor', ?)", (f"{search_i},{page}",))
            await asyncio.sleep(PAGE_GAP)
        await asyncio.gather(*pending)

    db.close()
    core.log_line(f"{'discover_sweep':16} finished: {counts['new']} new companies; {counts['listed']} on ATS lists, "
                  f"{counts['careers']} careers pages, {counts['searching']} queued for web search")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--forever", action="store_true", help="start a new sweep whenever one finishes")
    ap.add_argument("--gap", type=float, default=PAGE_GAP,
                    help=f"seconds between LinkedIn pages (default {PAGE_GAP:g}); slower leaves LinkedIn "
                         "room for the internship pollers")
    args = ap.parse_args()
    PAGE_GAP = args.gap
    restart = args.restart
    while True:
        asyncio.run(main(restart))
        if not args.forever:
            break
        restart = True
