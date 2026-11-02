# Job Polls

Live tracker of **US and Canada software internships**. Pollers hit company career pages and ATS APIs on a loop, keep a de-duplicated SQLite store, and serve a feed the web UI refreshes every minute.

Built for catching a posting when it goes up — not when an aggregator indexes it hours later.

## How it works

```
ATS / careers / intern lists
        │
        ▼
watch_*.py  (asyncio, one group per interval)
        │
        ▼
engineering-ats/.internships.sqlite
        │
        ▼
US_internships.csv · canada_internships.csv
        │
        ▼
JSON feed  →  web UI (US / Canada tabs)
```

Each source runs in a **child process** that exits when the pass finishes, so a leaky scraper cannot grow forever. Titles must look like an internship **and** a software-side role (engineer, developer, data, ML, intern, co-op, stagiaire, …). US and Canada only; remote with no country is kept.

Company names are normalized (CGI Group Inc. / CGI, Capital One - CA / Capital One) so the same job from two sources is one row. Closed postings stay in SQLite as `open=0` instead of disappearing.

## Pollers

| Script | Interval | What it covers |
|---|---|---|
| `scripts/watch_canada.py` | 3 min | Canadian software employers + Wellfound |
| `scripts/watch_bigtech.py` | 6 min | Google, Meta, Amazon, Apple, Tesla, Uber, Shopify, … |
| `scripts/watch_big_ats.py` | 10 min | Greenhouse, Workday, Ashby, Lever, iCIMS, intern lists, Built In, simplify.jobs |
| `scripts/watch_small_ats.py` | 30 min | Workable, ADP, Taleo, Dayforce, Job Bank, dead-board fallbacks |

~49k company career pages live under `ats-companies/`. ~60 sources in total, including community intern lists and careers-page / web-search fallbacks when a board URL is dead.

**simplify.jobs** carries many postings that never reach Simplify's GitHub list. Its page is rendered in JavaScript, so `check_simplifyjobs` loads it in headless Chromium (Playwright), sorted by most recently updated, and keeps US/Canada software internships. It uses the real posting date, so old postings Simplify re-bumps don't show up as new. Needs a Playwright headless shell: `playwright install chromium-headless-shell`.

`scripts/health_check.py` runs every 5 min from a systemd timer and sends ntfy pushes when a poller stalls or a new Canadian internship appears.

## Web UI

`web/index.html` — intern list with search, US/Canada tabs, posted vs found times.

Local (reads the CSVs on this machine, localhost only):

```bash
python scripts/view_local.py   # http://localhost:8090
```

Token-gated feed for a hosted frontend:

```bash
FEED_TOKEN=... python scripts/serve_feed.py   # GET /feed.json
```

## Run a poller

Python 3.11+. From the repo root:

```bash
uv sync --extra scrapers
python scripts/watch_canada.py --once    # one pass
python scripts/watch_canada.py           # loop
```

Outputs land in `engineering-ats/`. Shared engine: `scripts/internship_core.py`.

On a server, `deploy/setup_server.sh` installs user systemd units (`internship-watch@canada`, `@bigtech`, `@big_ats`, `@small_ats`, plus the feed).

## Outreach and company finding

Besides live postings, a few scripts build lists of companies worth contacting directly, mostly Canadian employers that hire co-ops without a public posting:

| Script | What it does |
|---|---|
| `scripts/find_recent_intern_companies.py` | Canadian companies with a software intern/co-op hire in recent terms but no Summer 2027 posting yet |
| `scripts/find_ww_pipeline.py`, `scripts/ww_pipeline_mcp.py` | Toronto/Montreal companies that hire through WaterlooWorks / school boards instead of public postings |
| `scripts/class_profiles.py`, `scripts/seed_waterloo.py` | Co-op employers from UW class profiles; adds Waterloo-hiring startups to the Canada poller |
| `scripts/us_sponsor_outreach.py` | US intern outreach list, separate from the Canada co-op list |
| `scripts/watch_outreach.py` | Keeps the outreach company list current from intern hiring history |
| `scripts/build_outreach_tracker.py` | Builds an outreach spreadsheet from what the pollers have seen |
| `scripts/company_search_mcp.py` | MCP server for searching intern-hiring companies |
| `scripts/email_tracker.py` | Application tracker built from Gmail (confirmations, rejections, interviews, offers) |
| `web/outreach.html`, `web/tracker.html` | Pages for the outreach list and application tracker |

Contact details live in `scripts/outreach_contacts.py`, which is kept out of the repo (it holds people's names and emails). `view_local.py` and `company_search_mcp.py` import it, so add your own copy to run them.

## Layout

| Path | Role |
|---|---|
| `scripts/watch_*.py` | Poller entrypoints |
| `scripts/internship_core.py` | Filter, dedup, SQLite, CSV export |
| `src/ats_scrapers/` | Per-ATS scrapers (Greenhouse, Workday, …) |
| `ats-companies/` | Company lists, one CSV per ATS |
| `engineering-ats/` | Live intern CSVs + sqlite |
| `web/` | Frontend |
| `deploy/` | systemd + rsync setup |

Scrapers come from [ats-scrapers](https://github.com/kalil0321/ats-scrapers) (Kalil Bouzigues). This repo adds the intern filter, concurrent watchers, feed, and UI.

## License

MIT. See `LICENSE`.
