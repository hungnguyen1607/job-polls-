# Results

Measured on this machine (Linux x86_64, .NET 10, g++ 15.2, Redis 7 on localhost).
Numbers are from the included demo + microbenchmark; re-run to reproduce.

## Native core (C++)

```
make -C native bench
native dedup: 2000 rows in 1.9 ms (1058774 rows/s), duplicates=1996, arena=0.06 MB
```

- ~1.06M rows/s for SimHash + near-dup over short titles.
- **0.06 MB** arena for a 2,000-row batch (freed once with `jp_arena_reset()`).
- No per-item malloc/free and no GC-visible allocation in the hot path.

> The 1,996 "duplicates" is because the synthetic titles differ only by a trailing
> token, so SimHash puts them within 3 Hamming bits. That is a threshold-tuning
> property, not a bug — see the note in `README.md`.

## End-to-end pipeline

6 postings published to `jobs:demo`:

```
publisher: published 6 (6 total)
consumer[c1]: +5 stored, 1 intra-batch dup, total=5
consumer done: stored=5 duplicates=1 errors=0 arena=0B
```

- Native dedup removed 1 duplicate ("Software Engineer Intern" vs "software engineer intern!").
- 0 errors; 6 events → 5 stored rows.

API (`/api/stats`):

```json
{"total":5,"open":5,"canada":3,"us":2,"streamLength":6,"consumerEvents":5,"duplicates":1,"errors":0}
```

Live push (`/api/events`, SSE) after a new posting was published:

```
data: {"Key":"ats|7","Company":"NewCo","Title":"Frontend Engineer Intern","Location":"Ottawa, ON","EventType":"new",...}
```

## Tests

```
dotnet test JobPolls.slnx   -> Passed: 10, Failed: 0
make -C native test         -> native tests ok
```

## Not yet measured

- Throughput vs consumer count (1/4/8) at steady state.
- End-to-end p50/p95 latency (first_seen → dashboard) on a real poller dataset.
- Dedup precision/recall on labeled duplicates.
- Postgres vs SQLite store.
