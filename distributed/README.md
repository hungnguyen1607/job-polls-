# Job Polls Distributed

A distributed ingestion backend for the [job-polls](https://github.com/hungnguyen1607/job-polls-) scraper. C# services move postings through **Redis Streams**; a **C++ core** does allocation-light SimHash dedup behind a C ABI, called from C# via `[LibraryImport]`.

## How it works

build native → start redis → publish → consume → serve

1. **build** — `make -C native` — builds `native/build/libjobpolls_native.so` (per-batch arena, normalize, SimHash, Hamming, dedup).
2. **start** — `docker compose up -d redis` (or `redis-server`) — the event bus.
3. **publish** — `dotnet run --project src/JobPolls.Publisher -- --db /path/to/.internships.sqlite --backfill` — tails new rows into the `jobs:events` stream.
4. **consume** — `dotnet run --project src/JobPolls.Consumer -- --store jobpolls.consumer.db` — a consumer group pulls events, calls the native dedup batch, upserts SQLite, and ACKs.
5. **serve** — `dotnet run --project src/JobPolls.Api` → <http://localhost:5000> — live dashboard (SSE), `/api/jobs`, `/api/stats`, `/health`.

Scale by running more consumers with different `--name`, or `docker compose up --scale consumer=8`. Same host or many — they all share the Redis consumer group and ACK pending work.

## The C++/C# seam

- `native/include/jobpolls.h` is **POD-only** (`extern "C"`): caller allocates inputs and outputs, native never returns heap pointers.
- `JobPolls.Native` wraps it with `[LibraryImport]` and `unsafe` spans. Buffers come from `NativeMemory`/`Marshal`, batch work uses the native **thread-local arena**, then `jp_arena_reset()` frees it in one shot.
- C# owns the distributed parts (Redis, consumer groups, HTTP/SSE); C++ owns the memory-sensitive per-batch work (normalize, SimHash, Hamming, near-dup).

Threshold note: SimHash on short titles is aggressive; `--threshold 3` is a starting point. Tune per dataset (see `RESULTS.md`).

## Tests & numbers

```bash
make -C native test && make -C native bench
dotnet test JobPolls.slnx
```

See `RESULTS.md` for measured numbers.

## Layout

```
src/JobPolls.Contracts/   events + store models + region
src/JobPolls.Native/      LibraryImport wrapper over the C ABI
src/JobPolls.Publisher/   SQLite tail -> Redis Streams
src/JobPolls.Consumer/    consumer group -> native dedup -> SQLite
src/JobPolls.Api/         minimal API + SSE dashboard
native/                   C++ arena + SimHash + dedup (Makefile)
tests/JobPolls.Tests/     xUnit + native equivalence
scripts/demo.sh           end-to-end local demo
```

MIT.
