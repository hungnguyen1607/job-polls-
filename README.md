# Job Polls

Live tracker of US and Canada software internships. Polls ~49k company career pages across ~60 sources on a loop, de-dupes into SQLite, and serves a feed that refreshes every minute — so a posting shows up when it goes live, not when an aggregator indexes it hours later.

## How it works

clone → install → run → view → deploy

1. **clone** — `git clone https://github.com/hungnguyen1607/job-polls-.git && cd job-polls-` — gets the code.
2. **install** — `uv sync --extra scrapers` — Python 3.11+. For the simplify.jobs browser source: `playwright install chromium-headless-shell`.
3. **run** — `python scripts/watch_canada.py` — polls Canadian software employers every 3 min (`--once` for a single pass). Others: `watch_fast.py` (3 min), `watch_bigtech.py` (6 min), `watch_big_ats.py` (10 min), `watch_small_ats.py` (30 min).
4. **view** — `python scripts/view_local.py` → http://localhost:8090 — local UI with search and US/Canada tabs. Hosted feed: `FEED_TOKEN=... python scripts/serve_feed.py` → `GET /feed.json`.
5. **deploy** — `bash deploy/setup_server.sh` — installs the systemd watchers (`internship-watch@canada`, `@bigtech`, `@big_ats`, `@small_ats`) plus the feed.

Outputs land in `engineering-ats/` (`US_internships.csv`, `canada_internships.csv`, `.internships.sqlite`). `scripts/health_check.py` runs every 5 min and pushes ntfy alerts when a poller stalls or a new Canadian internship appears.

Beyond live postings, `scripts/watch_outreach.py` + `scripts/build_outreach_tracker.py` build a cold-outreach list of Canadian employers that hire co-ops without a public posting.

## Distributed ingestion (C#/C++)

`distributed/` is a separate ingestion backend that consumes the Python poller's SQLite DB:

- **C# Publisher** tails `engineering-ats/.internships.sqlite` and publishes new postings to **Redis Streams**.
- **C# Consumers** pull consumer groups, call the native dedup core, and write deduped rows back to SQLite.
- **C++ core** does allocation-light SimHash near-duplicate detection behind a POD-only C ABI, using a per-batch arena (measured ~1.06M rows/s, 0.06 MB arena for 2,000 rows).
- **C# API** serves `/api/jobs`, `/api/stats`, `/health`, and an SSE dashboard.

See [`distributed/README.md`](distributed/README.md) for build/run steps and [`distributed/RESULTS.md`](distributed/RESULTS.md) for the measured numbers.

Scrapers come from [ats-scrapers](https://github.com/kalil0321/ats-scrapers) (Kalil Bouzigues); this repo adds the intern filter, concurrent watchers, feed, and UI. MIT licensed.
