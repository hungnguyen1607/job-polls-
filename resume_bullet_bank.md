# Resume Bullet Bank

Pick 3–4 per role. One line ≈ 113 characters max at the current font. Tags: [fullstack] [backend] [systems] [ai] [payments] [concurrency] [perf].
✅ = on a resume now · 🆕 = new, not on any resume yet · ⚠️ = verify/run before using

## Rakuna — Software Engineer Intern (Sept. 2026 – Present, Remote)
- ✅ Built an internal React & Rails event-ops tool with updates, cutting review time 75% across 400+ monthly events *(old, "with updates" is vague)*
- 🆕 Built a React & Rails ops tool pushing live statuses via Action Cable, cutting review time 75% for 400+ events/mo [fullstack] *(if it wraps: "React/Rails")*
- ✅ Built a React/Rails event ops tool with live updates via Action Cable, cutting review time 75% for 400+ events/mo [fullstack] *(Main, Agent)*
- ✅ Wrote 150+ RSpec regression tests across Rails controllers/models/services, reducing production bugs by 25% *(Main, Agent)*
- ✅ Shipped 3 scheduled Rails Slack-webhook reports tracking dropped/bounced/missing emails, saving 25 hrs/week [backend]
- ✅ Eliminated 20% of irrelevant search results by mapping 192 free-text majors to canonical IDs via indexed SQL joins [backend]
- 🆕 Mapped 192 free-text majors to canonical IDs via indexed SQL joins, removing 20% of irrelevant search results [backend]
- ✅⚠️ Wrote 150+ RSpec regression tests across Rails controllers and models, reducing production bugs by 25% *(be ready to explain how 25% was measured)*
- 🆕⚠️ Wrote 150+ RSpec regression tests across Rails controllers and models, gating every deploy in CI *(only if tests block deploys)*

## Rakuna — Software Engineer Intern (May 2026 – Aug. 2026, Hanoi)
- ✅ Migrated Workday REST → SOAP for 100+ enterprise clients, merging into 1 request, cutting export time 95% [backend]
- 🆕 Consolidated Workday exports from REST into 1 batched SOAP request for 100+ clients, cutting export time 95% [backend]
- ✅ Accelerated resume downloads 13× via Sidekiq-orchestrated ECS Fargate workers, handling 20K+ resumes/mo [systems]
- 🆕 Cut bulk resume downloads 10 min → 45s via Sidekiq + ECS Fargate workers, handling 20K+ resumes/mo [systems][perf] *(on Agent)*
- ✅ Built Sidekiq + SendGrid jobs processing 50K+ emails/mo, reducing recruiter email setup 15 min → 3s [backend]
- 🆕 Built Sidekiq + SendGrid jobs sending 50K+ emails/mo, cutting recruiter email setup from 15 min → 3s [backend]
- ✅ Cut bulk resume downloads 10 min → 45s via Sidekiq-orchestrated Fargate workers, handling 20K+ resumes/mo [systems][perf] *(all 3)*
- ✅ Refactored repeated JSX into 5 shared React components across 10+ pages, cutting new-page build time ~80% [fullstack] *(all 3; ~5 days → ~1 day)*
- 🆕 Refactored repeated JSX into 5 shared React components across 10+ pages, removing 500+ redundant lines *(alternative)*
- ✅ Refactored repeated JSX into 5 reusable React components across 10+ pages, removing ~500 duplicated lines [fullstack] *(weakest; first to cut)*

## Innova Museum — Software Engineer (Mar. 2026 – May 2026, Vancouver)
Schema / data layer
- ✅ Migrated 17 services from Supabase client to Postgres, rebuilding a 43-table canonical schema with RLS *(Main)*
- 🆕 Migrated 17 services off the Supabase client onto a rebuilt 43-table Postgres schema with 89 indexes and RLS [backend]
- ✅ Migrated 17 services onto a rebuilt 43-table multi-tenant Postgres schema with 89 indexes and RLS tenant isolation *(Systems)*
- ✅ Rebuilt the Postgres schema into a 43-table canonical baseline with 89 indexes, retiring the legacy schema *(Agent/Systems)*
- 🆕 Rebuilt a 43-table multi-tenant Postgres schema with 89 indexes and RLS for tenant isolation [backend]
- 🆕 Rebuilt a 43-table Postgres schema with 89 indexes and RLS, consolidating legacy migrations into 1 baseline [backend]

Payments
- ✅ Architected a Stripe checkout flow with atomic orders, per-ticket HMAC-signed QRs, and refund-time revocation [payments]
- 🆕 Built a Stripe webhook pipeline routing 31 event types through a Postgres inbox, with replay/backfill after outages [payments][systems]
- 🆕 Architected Stripe checkout with idempotency keys and a replayable webhook inbox, with 0 duplicate orders on replay [payments]
- 🆕 Architected Stripe checkout, refunds, and chargebacks, passing 10/10 payment edge-case scenarios incl. webhook replay [payments]
- 🆕⚠️ Architected Stripe checkout with HMAC-signed QR tickets, signing X/s with 0 collisions across 500K tokens *(run `npx tsx benchmarks/qr/bench.ts`)*

Concurrency
- ✅ Made ticket holds oversell-proof with row-locked reservations + an expiry cron; 0 oversold at 50 concurrent buyers [concurrency] *(Main; the version that fits on one line)*
- 🆕 Engineered oversell-proof ticket holds with row-locked atomic reservations and an expiry cron; 0 oversold at 50 concurrent buyers [concurrency]
- 🆕⚠️ Prevented ticket overselling with atomic conditional UPDATEs; 0 oversold across 200 concurrent buys of 100 tickets [concurrency] *(run benchmarks/ticket_purchase/run.sh)*
- 🆕⚠️ Made ticket reservation oversell-proof with atomic UPDATEs, sustaining X TPS at p95 Y ms with 0 oversold *(fill from run.sh scenario_b)*
- 🆕 Built a Postgres email job queue using SKIP LOCKED claims with stale-job recovery, so crashed workers never drop tickets [systems] *(not on Systems resume, overlaps the Apartment queue)*

Performance / security
- ✅ Designed a revenue aggregation API that computes per-event totals in a single query, cutting latency 1.97s → 182ms [perf]
- 🆕 Designed a revenue API aggregating per-event totals in one CTE + GROUP BY query, cutting latency 1.97s → 182ms [perf]
- ✅ Built a revenue analytics API for organizer dashboards in one CTE query, cutting latency 1.97s → 182ms [perf] *(Agent; dashboard/reporting roles)*
- ✅ Engineered rate limiting on auth, OTP, and analytics endpoints, capping each IP at 20 auth attempts/15min
- 🆕 Wrote token-bucket rate-limit middleware for auth/OTP/analytics, capping each IP at 20 auth attempts/15min *(in-memory per instance; don't call it distributed)*

## Agentic Apartment Search SaaS
- ✅ Saved $11,000/year in LLM costs by designing a cache-stable AI agent for automated outreach emails [ai]
- 🆕⚠️ Cut LLM costs $11K/year by keeping agent prompt prefixes stable for prompt-cache hits on outreach emails [ai] *(confirm mechanism)*
- ✅ Engineered a multi-agent pipeline raising verified filter coverage 45% → 87%, used in 500+ successful searches [ai] *(Main, Agent; 500+ = successful apartment searches on the site)*
- ✅ Built a multi-tenant MCP server consolidating agent tools into one process, cutting memory 400MB → 134MB [ai][systems]
- 🆕 Consolidated agent tools into one multi-tenant MCP server, cutting memory 400MB → 134MB [ai]
- ✅ *(Systems, 2 lines)* Built an AI-agent fact pipeline with Java as the trust boundary: MCP proposes facts, a validator rejects bad fields/types/sources, and two-agent consensus verifies them; Kafka + DLQ, p95 127ms
- 🆕 Validated AI-proposed facts in Java with two-agent consensus and Kafka + DLQ retries at p95 127ms [systems]
- ✅ *(Systems, 2 lines)* Built a versioned building/availability read model: Spring Batch imports a 149K-building / 856K-unit roll into Postgres in 34-56s, with O(1) unit/availability reads and trigger-based cache invalidation
- 🆕 Loaded 149K buildings / 856K units into Postgres in <60s via Spring Batch, with O(1) availability reads [systems]
- ✅ *(Systems, 2 lines)* Built a lease-based Postgres work queue with atomic FOR UPDATE SKIP LOCKED claiming, leases, expired-lease recovery, retry budgets, and job dedupe; exactly-once verified under 20 concurrent claims
- 🆕 Built a Postgres job queue with SKIP LOCKED leases and retries; zero double-claims across 20 workers [systems][concurrency]

- ✅ Served 500+ successful searches; cut p95 7.2s → 8ms by moving SQLite → Postgres and fixing an N+1 query [backend][perf] *(Main)*
- ✅ Built a Kafka pipeline ingesting 975 listings from 31 scraper sources into Postgres at p95 43ms scrape-to-save [backend] *(Main)*
- ✅ Designed a gRPC validation service that saves AI-sourced facts only when 2 agents agree, 30% faster than REST [backend][ai] *(Main)*
- 🆕 (long) Built a full-stack apartment search platform (React/TypeScript, Node, Java/Spring Boot, Postgres), live in production with blue/green deploys and Prometheus/Grafana monitoring. Migrating from SQLite to Postgres and fixing an N+1 query took p95 at 200 req/s from 7.2 s to 8 ms, and it sustains 400+ req/s with 0 errors.
- 🆕 (long) Built a Kafka ingestion pipeline (Node scrapers → Kafka → Spring Boot consumer → Postgres) that pulls 975 listings from 31 sources at p95 43 ms from scrape to save; sources paused by errors now retry on a backoff, which brought all 31 back after an outage.
- 🆕 (long) Built an MCP agent pipeline where AI agents submit 7,293 apartment facts over gRPC to a Java validator, which accepts a fact only after two agents agree; gRPC/protobuf cut agent write latency 30% and payload size 42% compared with REST.
- ✅ *(Systems, 2 lines, Java-heavy for Autodesk)* Built an apartment search platform on Java/Spring Boot + React/Postgres; migrating SQLite → Postgres and fixing an N+1 query cut p95 7.2s → 8ms at 200 req/s, sustaining 400+ req/s with 0 errors
- ✅ *(Systems, 2 lines)* Built a Kafka ingestion pipeline (Node scrapers → Kafka → Spring Boot consumer → Postgres) ingesting 975 listings from 31 sources at p95 43ms scrape-to-save; backoff retries recovered all 31 after an outage
- ✅ *(Systems, 2 lines)* Built a gRPC pipeline where AI agents submit 7,293 facts to a Java validator that saves only when 2 agents agree; protobuf cut agent write latency 30% and payload size 42% vs REST

## Android Automation Platform
- ✅ Built and sold a full-stack Android automation SaaS running 24/7, reaching 120 paying users and $10,000 in revenue *(consider naming what it automated)*
- ✅ Scaled Android containers via Redis-backed Celery workers, cutting event-to-action latency 11s → 3.5s [systems]
- ✅ Migrated AWS VMs to Waydroid (Android) containers on Hetzner, lowering per-user infrastructure cost 72% [systems]

## Self Hosted AI Agent
- ✅ Built a sandboxed multi-agent AI runtime in Rust/Tokio, exposed as an MCP server and Telegram bot [ai][systems]
- 🆕 Developed a sandboxed multi-agent runtime in Rust/Tokio, exposed as an MCP server and Telegram bot
- ✅ Isolated agent tool calls with Linux namespaces, seccomp and Firecracker microVMs for per-agent isolation [systems]
- 🆕 Isolated each agent's tool calls with Linux namespaces, seccomp, and Firecracker microVMs
- ✅ Built a cassette-replay eval harness for 8 agent tasks, replaying recorded model calls deterministically at $0 [ai]

## AI Self Study Tool
- ✅ Built hybrid RAG search (BM25 + vector, rank fusion) over 4,339 course pages; caching cut vector-load p95 ~98% [ai][backend] *(Main; measured: p95 240.7 → 5.6 ms, p50 136.6 → 1.5 ms, 50 queries × 5 runs; end-to-end search showed no measurable change)*
- 🆕 Built hybrid RAG search (BM25 + vector, rank fusion) over 4,339 course pages, scored on a 50-question eval *(previous)*
- ✅ Built a multi-user FastAPI backend (REST + GraphQL) serving 42 students at p95 2ms, with per-user isolation [backend] *(Main; 2ms = server-side API p95)*
- ✅ Shipped an OAuth MCP server for Claude and Moodle deadline sync, deployed via GitHub Actions at 99.7% uptime [ai][backend] *(Main)*
- 🆕 Built a multi-user FastAPI backend (REST + GraphQL) with per-user isolation, rate limits, and quotas; 71 tests *(previous)*
- 🆕 Shipped an OAuth MCP server for Claude, auto-syncing 38 Moodle deadlines, deployed via GitHub Actions *(previous)*
- 🆕 Built hybrid RAG search (BM25 + vector, rank fusion) over 4,339 pages of course material, used by 42 students *(swap in to show real users)*
- ⚠️ TODO measure: Recall@5 hybrid vs keyword-only on the 50-question eval; GraphQL study page calls N → 1 and p95; deploy time / VPS uptime
- 🆕 (long) Built a hybrid RAG search pipeline that indexes 4,339 pages (8,149 chunks) of course material across 2 courses, using BM25 + vector search merged with reciprocal rank fusion (optional pgvector/HNSW), with a 50-question retrieval eval set measuring Recall@k and MRR.
- 🆕 (long) Built a multi-user FastAPI backend with REST + GraphQL APIs, per-user data isolation, rate limiting, storage quotas and a bring-your-own-API-key option, backed by 71 automated tests covering auth, isolation, rate limits and budgets; it has produced 127 problem-pattern guides and 59 verified practice problems.
- 🆕 (long) Shipped a remote MCP server with OAuth so Claude can use the tutor's tools, plus Moodle deadline sync tracking 38 deadlines; push-to-deploy via GitHub Actions and nightly systemd-timer backups.
- *(below: on Agent)*
- ✅ Built an AI study tool used by 42 students that indexes Moodle course files and cites slides via agentic RAG [ai]
- ✅ Reduced RAG latency 87% by caching query embeddings over a pgvector index for fast semantic retrieval [ai][perf] *(Agent; note: 2026-11-12 benchmark showed the gain in vector loading, not end-to-end search)*
- 🆕 Cut vector-loading p95 ~98% by caching course embeddings for pgvector semantic search *(measured 240.7 → 5.6 ms)*
- ✅ Built a GraphQL BFF (Strawberry + FastAPI) merging REST calls into one, cutting DB latency 98% *(inaccurate: say "request latency")*
- 🆕 Merged REST calls into one GraphQL BFF query (Strawberry + FastAPI), cutting request latency 98% [perf]

## Distributed Job Polling Engine (this repo)
- ✅ *(Systems, 2 lines)* Built a C#/C++ ingestion pipeline over Redis Streams with XACK consumer groups and horizontal workers; a native C ABI dedup core (arena + SimHash) sustains ~1M rows/s at 0.06 MB/batch
- 🆕 Built a C#/C++ ingestion pipeline on Redis Streams consumer groups; native SimHash dedup hits ~1M rows/s [systems][perf]
- ✅ *(Systems, 2 lines)* Architected one-process-per-source pollers with asyncio semaphores, per-ATS concurrency caps, retries/backoff, and systemd cgroup memory limits, sustaining ~50K ATS boards without OOMing the host
- 🆕 Polled ~50K ATS boards with per-source async pollers, concurrency caps, backoff, and systemd memory limits [systems]
- ✅ *(Systems, 2 lines)* Designed a WAL SQLite store with atomic os.replace writes, normalized cross-source dedup (URL + company + title/city), and a 5-min health/alert loop, keeping 43K+ postings consistent
- 🆕 Deduped 43K+ postings across sources in a WAL-mode SQLite store with atomic writes and 5-min health checks [systems]
