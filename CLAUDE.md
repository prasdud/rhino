# CLAUDE.md — Rhino Project Context

> This file is the single source of truth for any AI agent or developer working on this codebase.
> Read this entire file before writing a single line of code.

---

## What is Rhino?

**A durable, exactly-once job queue built in Rust.**

Rhino is an open-source job queue library. It ships as a Rust crate that drops into any project. No broker, no sidecar, no managed service, no signup. The only dependency is a Postgres database — which most backend teams already have.

The closest comparison is **Oban for Elixir** — battle-tested, loved for its simplicity and correctness guarantees. That library does not exist for Rust. Rhino fills that gap.

**License:** MIT  
**Domain:** getrhino.dev  
**Crate name:** `rhinoqueue`  
**npm package:** `rhinoqueue`  
**PyPI package:** `rhinoqueue`

---

## The Core Guarantee

> A job enqueued will execute **exactly once** — even under worker crashes, network partitions, or simultaneous competing workers.

This is not at-least-once. This is not best-effort. Duplicates are mathematically impossible given correct implementation.

This guarantee is not magic. It is a direct consequence of:
1. `SELECT FOR UPDATE SKIP LOCKED` — Postgres row-level locking primitive (stable since PG 9.5)
2. Wrapping job execution and status update in the same database transaction
3. Lease-based locking with timeout-based crash recovery

---

## The Problem Rhino Solves

Every serious backend app needs background job processing. Current options are all broken in at least one way:

- **Hosted SaaS queues** (QStash, Upstash, Trigger.dev, Inngest) — monthly cost, data leaves your infra, vendor lock-in, can sunset
- **Raw `tokio::spawn`** — no durability, no retries, jobs vanish on crash
- **Existing Rust crates** (`apalis`, etc.) — incomplete, unpolished, no exactly-once guarantee
- **Oban** — gold standard but Elixir-only, not available to Rust teams

Rhino is the missing library.

---

## The API — Dead Simple by Design

Rhino has one new concept for the user: `#[rhino]`.

```rust
// You already had this function
async fn process_order(order_id: u64) {
    charge_card(order_id).await;
}

// This is the entire change
#[rhino]
async fn process_order(order_id: u64) {
    charge_card(order_id).await;
}

// Start the queue
let q = rhino!("postgres://localhost/myapp");
q.start().await;

// Enqueue from anywhere — fire and forget
rhino!(process_order(42));

// Or batch
q.add(process_order(42));
q.add(process_order(43));
q.send().await;

// Chain — B runs after A succeeds
q.add(charge_card(order_id))
 .then(send_receipt(order_id))
 .then(update_inventory(order_id))
 .send().await;

// Schedule
q.add(send_report()).at(tomorrow());
q.send().await;
```

The user never touches serialization, registries, worker loops, or Postgres directly. They write functions and call `send()`.

---

## What the `#[rhino]` Macro Does

The macro silently generates:

1. **A serialization wrapper struct** — so function arguments can be stored as JSONB in Postgres
2. **A `RhinoJob` trait impl** — which calls the user's original function
3. **An enqueue helper** — what `q.add()` calls internally
4. **Auto-registration** — the job type string is derived from the function name

The user's original function is completely untouched. Rhino wraps around it.

---

## The Database Schema

```sql
CREATE TABLE rhino_jobs (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type     TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',
    priority     INT         NOT NULL DEFAULT 0,
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 3,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at    TIMESTAMPTZ,
    locked_by    TEXT,
    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX rhino_jobs_fetchable
    ON rhino_jobs (status, priority DESC, run_at ASC)
    WHERE status = 'pending';
```

Job status transitions (one direction only, never backwards):
```
pending → locked → done
                 → failed  (attempts < max_attempts, re-queued with backoff)
                 → dead    (attempts >= max_attempts, dead letter)
```

---

## The Worker Loop — The Heart of Rhino

This is the core of the entire project. Everything else is interface on top of this.

```sql
-- The locking query — DO NOT SIMPLIFY THIS
SELECT id, job_type, payload, attempts
FROM rhino_jobs
WHERE status = 'pending'
  AND run_at <= NOW()
ORDER BY priority DESC, run_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

`SKIP LOCKED` means: if another worker holds a lock on this row, skip it entirely. This prevents double-execution without any application-level coordination.

**Worker execution flow:**
1. Poll for pending job (query above)
2. Lock the row — set `status = 'locked'`, `locked_at = NOW()`, `locked_by = worker_id`
3. Deserialize payload → call user's function
4. **Success** → `UPDATE status = 'done'` in same transaction as step 3
5. **Failure** → increment `attempts`, set `run_at = backoff(attempts)`, reset to `pending`
6. **Max attempts exceeded** → set `status = 'dead'`
7. **Worker crash mid-execution** → lock_timeout expires, another worker picks it up

**Key invariant:** The worker must ALWAYS commit or rollback its transaction. Never hold an open transaction across non-DB work.

---

## Retry Backoff Formula

```rust
// Exponential backoff with jitter
// attempt 1 → ~30s, attempt 2 → ~4min, attempt 3 → ~30min
fn next_run_at(attempts: i32) -> DateTime<Utc> {
    let base: i64 = 15 * 2_i64.pow(attempts as u32);
    let jitter: i64 = rand::thread_rng().gen_range(0..base / 2);
    Utc::now() + Duration::seconds(base + jitter)
}
```

---

## Repository Structure

```
rhinoqueue/
├── Cargo.toml
├── CLAUDE.md                    ← this file
├── src/
│   ├── lib.rs                   # Public API surface — exports, re-exports
│   ├── worker.rs                # Worker loop, polling, locking, execution
│   ├── queue.rs                 # Queue handle, enqueue logic, q.add()/send()
│   ├── job.rs                   # RhinoJob trait definition
│   ├── registry.rs              # Job type registry — string → handler mapping
│   ├── error.rs                 # thiserror error types — RhinoError enum
│   ├── config.rs                # RhinoConfig — workers, timeouts, backoff
│   ├── backoff.rs               # Retry backoff formula
│   └── migrations/
│       └── 001_create_rhino_jobs.sql
├── rhino-macros/                # Separate crate for proc macros
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs               # #[rhino] procedural macro
├── benches/
│   ├── enqueue.rs               # criterion — raw enqueue throughput
│   ├── drain.rs                 # criterion — worker drain speed
│   └── latency.rs               # criterion — job pickup latency p50/p95/p99
├── stress/
│   └── stress_test.rs           # Full system stress tester — see below
└── tests/
    ├── exactly_once.rs          # THE most important test — zero duplicates
    ├── retry.rs                 # Failure → retry → dead letter
    ├── crash_recovery.rs        # Kill worker mid-job, verify pickup
    ├── chains.rs                # .then() sequencing
    ├── batches.rs               # q.batch().on_complete()
    └── integration.rs           # General integration — requires Postgres
```

---

## Technical Stack

| Purpose | Crate | Why |
|---|---|---|
| Async runtime | `tokio` | Industry standard |
| Postgres | `sqlx` | Async, compile-time query checking |
| Serialization | `serde` + `serde_json` | Universal |
| Job IDs | `uuid` | v4, collision-resistant |
| Timestamps | `chrono` | Timezone-aware |
| Error handling | `thiserror` | Clean library errors |
| Proc macros | `syn`, `quote`, `proc-macro2` | The `#[rhino]` macro |
| Benchmarks | `criterion` | Statistical rigor |
| Random (jitter) | `rand` | Backoff jitter |

---

## Performance Targets — Hit and Exceed These

These are not aspirational. These are the bar. Every architectural decision should be made with these in mind.

| Metric | Target | Notes |
|---|---|---|
| Throughput (no-op, 10 workers) | **> 10,000 jobs/sec** | Exceed Oban's 4.4K at same concurrency |
| Throughput (no-op, peak) | **> 20,000 jobs/sec** | |
| Job pickup latency p50 | **< 10ms** | |
| Job pickup latency p95 | **< 30ms** | |
| Job pickup latency p99 | **< 50ms** | |
| Idle RAM (worker process) | **< 5MB RSS** | Measured with `/proc/self/status` |
| RAM under load (10 workers) | **< 20MB RSS** | |
| Binary size (release) | **< 5MB** | With `opt-level="z"`, `strip=true` |
| Startup time | **< 50ms** | From binary start to ready |
| Exactly-once under contention | **0 duplicates** | 10k jobs, 20 workers, non-negotiable |
| Crash recovery | **< lock_timeout** | Configurable, default 30s |

**Competitor reference points:**
- Sidekiq peak: ~30K jobs/sec (96-core EC2 + Dragonfly)
- BullMQ: ~8.3K jobs/sec at concurrency=100
- Oban: ~4.4K jobs/sec at concurrency=100 (M1 Pro)
- Celery: ~500–1.5K jobs/sec

Rhino should beat Oban on throughput at equivalent concurrency. The Rust async runtime vs BEAM VM advantage makes this achievable.

---

## Release Build Profile — Non-Negotiable

```toml
[profile.release]
opt-level = "z"       # optimize for binary size
strip = true          # strip debug symbols
lto = true            # link-time optimization
codegen-units = 1     # maximize optimization
panic = "abort"       # smaller binary, no unwinding
```

---

## The Stress Tester — `stress/stress_test.rs`

The stress tester is a standalone binary. Run it constantly during development.

**Three job tiers:**

```rust
// Tier 1 — no-op (measures pure queue overhead)
#[rhino]
async fn noop_job(_id: u64) {}

// Tier 2 — CPU work (realistic light job)
#[rhino]
async fn cpu_job(data: String) {
    let _ = sha256(data.as_bytes()); // ~0.1ms CPU
}

// Tier 3 — I/O work (realistic heavy job — DB write inside job)
#[rhino]
async fn io_job(id: u64, pool: PgPool) {
    sqlx::query!("INSERT INTO stress_results (job_id) VALUES ($1)", id as i64)
        .execute(&pool).await.unwrap();
}
```

**Test matrix:**

```
10k  jobs × 1  worker  × noop  → single worker baseline
10k  jobs × 10 workers × noop  → concurrency scaling
10k  jobs × 20 workers × noop  → peak throughput
50k  jobs × 20 workers × noop  → sustained throughput
10k  jobs × 20 workers × cpu   → realistic light workload
10k  jobs × 20 workers × io    → realistic heavy workload
10k  jobs × 20 workers × noop  → EXACTLY-ONCE PROOF
```

**The exactly-once proof test:**
- Enqueue 10,000 jobs
- Spin up 20 workers simultaneously, all racing
- Each job atomically increments a counter in Postgres
- After drain: `assert!(counter == 10_000)`
- `assert!(duplicate_count == 0)`
- This test must pass every single run, no exceptions

**Output format:**
```
=== Rhino Stress Test ===
Config:        10,000 jobs / 20 workers / noop
Duration:      4.21s
Throughput:    2,375 jobs/sec
Latency p50:   8ms
Latency p95:   23ms
Latency p99:   41ms
RAM peak:      6.2MB
Duplicates:    0        ← must always be 0
Failed:        0
Dead letter:   0
```

---

## Features — What Ships in Each Version

### v0.1 — Core Engine (Proof of Concept)
**Goal: does the exactly-once guarantee work?**

- [ ] `rhino_jobs` table auto-created on startup
- [ ] Raw worker loop — poll, lock, execute, commit
- [ ] `SELECT FOR UPDATE SKIP LOCKED` locking
- [ ] Basic enqueue — serialize args, insert row
- [ ] Retry with exponential backoff + jitter
- [ ] Dead letter queue — `status = 'dead'` after max_attempts
- [ ] Crash recovery — lock timeout re-queue
- [ ] `RhinoWorker::new(pool)` API (rough, not final)
- [ ] Stress tester: 10k jobs, 10 workers, zero duplicates

No macro yet. No pretty API yet. Internals can be rough. This version is for the developer, not users. The only question it answers is: **does the guarantee hold?**

**Done when:** stress tester runs 10k jobs with 20 concurrent workers and reports 0 duplicates.

---

### v0.2 — The Macro + Clean API
**Goal: `#[rhino]` works, API feels like nothing**

- [ ] `#[rhino]` procedural macro — wraps any async fn
- [ ] Auto-serialization of function arguments via serde
- [ ] Job type registry — function name → handler
- [ ] `rhino!()` macro — zero-config queue handle
- [ ] `q.add().send()` interface
- [ ] Scheduled jobs — `.at(datetime)` and `.in(duration)`
- [ ] Priority queues — `priority` field respected in ORDER BY
- [ ] Stress tester: 50k jobs, all features, zero duplicates

**Done when:** a developer can add `#[rhino]` to a function and call `q.add().send()` with no other setup.

---

### v0.3 — Power Features (Everything Oban Pro Charges For)
**Goal: ship for free what Oban charges $135/mo for**

Ship in this order (easiest → hardest):
- [ ] Unique job deduplication — `.enqueue_unique()`
- [ ] Decorators — `q.use(middleware)` pipeline
- [ ] Rate limiting — `#[rhino(rate_limit = N)]`, Postgres advisory locks
- [ ] Global concurrency limits — cap parallel execution
- [ ] Chains — `.then()` — next job enqueued atomically on success
- [ ] Batches — `q.batch().on_complete()` — callback when all N jobs done
- [ ] Workflows — DAG dependencies, `rhino_workflow_edges` table (hardest, do last)
- [ ] Dynamic plugins — `q.install(plugin)` trait object registry

**Done when:** each feature has a passing integration test and stress test coverage.

---

### v0.4 — Production Hardening
**Goal: you could run this in production and sleep at night**

- [ ] Graceful shutdown — drain in-flight jobs before exit
- [ ] Configurable worker concurrency — `RhinoConfig::workers(N)`
- [ ] Connection pool tuning — configurable pool size
- [ ] Proper error types — complete `RhinoError` enum with `thiserror`
- [ ] Structured logging — `tracing` integration
- [ ] Metrics hooks — job count, latency, failure rate
- [ ] Full test suite — unit tests (no DB) + integration tests (real PG)
- [ ] Crash recovery test — kill worker mid-job, assert pickup
- [ ] Benchmark suite — criterion benches for enqueue and drain
- [ ] README with internal architecture docs

**Done when:** all tests pass, benchmarks published, zero known correctness issues.

---

### v1.0 — Public Launch
**Goal: strangers can use it**

- [ ] Published to crates.io as `rhinoqueue`
- [ ] Full API docs on docs.rs
- [ ] README with 5-minute quickstart
- [ ] Real benchmark numbers — not projections, measured on real hardware
- [ ] Optional dashboard — `q.dashboard()` mounts read-only UI at `/rhino`
- [ ] MIT license in repo
- [ ] getrhino.dev live with docs
- [ ] CHANGELOG.md
- [ ] CI — GitHub Actions, tests on every PR
- [ ] Cross-platform builds — Linux x64, Linux arm64, macOS arm64

**Done when:** someone you've never met opens a GitHub issue.

---

### v2.0 — Multi-Language (Thin Clients)
**Goal: Python and Node teams can use Rhino without knowing it's Rust**

- [ ] `rhinoqueue` on PyPI — pure Python, ~200-300 lines, psycopg3 + schema knowledge
- [ ] `rhinoqueue` on npm — pure TypeScript, ~200-300 lines, postgres.js + schema knowledge
- [ ] Zero compilation for users — `pip install rhinoqueue` just works
- [ ] Zero Rust toolchain required
- [ ] Python enqueue API: `q.add('job_type', {'key': 'val'}).send()`
- [ ] Node enqueue API: `await q.add('job_type', { key: 'val' }).send()`
- [ ] Docs updated for all three languages
- [ ] Examples for Python and Node in README

**Architecture:** Python and Node clients are producers only. They INSERT into `rhino_jobs`. The Rust worker processes everything. The table schema is the contract.

**Enqueue performance:** ~0.5–2ms per job from Python or Node. A Postgres INSERT on same-datacenter infra is as fast as or faster than a Redis round trip (2–3ms). No meaningful performance penalty.

**Done when:** a Python developer uses Rhino without knowing it's built in Rust.

---

### v3.0 — Native Bindings (Maybe, Future)
**Goal: Rust engine runs natively inside Python and Node processes**

- [ ] PyO3 native extension — Rust core compiled to `.so`, imported as native Python package
- [ ] napi-rs native addon — Rust core compiled to `.node`, imported natively by Node
- [ ] Same `pip install` / `bun add` UX — transparent upgrade, no API changes
- [ ] Cross-platform build matrix — Linux x64/arm64, macOS arm64/x64, Windows x64
- [ ] maturin for Python publishing, napi-rs CLI for Node publishing

**Ships only if:** real production data shows thin client enqueue is a bottleneck. This is unlikely. The worker is always Rust. The thin client is just an INSERT.

**Proven path:** Pydantic v2, Polars, ruff (PyO3). SWC, Biome, @node-rs/bcrypt (napi-rs).

---

## Key Invariants — Never Violate These

These are correctness rules. Breaking any of them breaks the exactly-once guarantee.

1. **Job status transitions are one-directional.** `pending → locked → done/failed/dead`. Never backwards.
2. **`locked_by` and `locked_at` must be set atomically** in the same UPDATE as the SELECT FOR UPDATE.
3. **The worker must always commit or rollback its transaction.** Never hold an open transaction across non-DB work (no HTTP calls, no sleeps inside a transaction).
4. **The job execution and status update must be in the same transaction.** If the job succeeds but the status update fails, the job will re-run. This is acceptable — the user's function must be written with this in mind, or Rhino wraps it safely.
5. **`max_attempts = 0` means unlimited retries.** Default is 3.
6. **Dead letter jobs are never deleted automatically.** They exist for human inspection.
7. **The `rhino_jobs` index must cover the worker query.** `(status, priority DESC, run_at ASC) WHERE status = 'pending'`. Without this index, queue performance degrades at scale.
8. **Never use `SELECT FOR UPDATE` without `SKIP LOCKED`.** Without `SKIP LOCKED`, workers block each other instead of skipping claimed jobs.

---

## Agent Instructions — Read This Section Carefully

You are writing **production-grade Rust** for a library that will be used in production by real teams. The standard is high. Here is exactly what that means:

### Code Quality

- **Memory safety is non-negotiable.** No `unsafe` blocks unless absolutely required and extensively documented with safety proofs. If you think you need `unsafe`, you almost certainly don't.
- **No `.unwrap()` in library code.** Ever. Use `?`, `map_err`, `ok_or`, or explicit error handling. `unwrap()` is acceptable only in tests and examples.
- **No `.clone()` without justification.** Every clone is a potential performance issue. If you're cloning, ask if you can borrow instead.
- **Errors must be typed.** Use `thiserror` for all error types. No `Box<dyn Error>` in public APIs. No string errors.
- **Every public function must have a doc comment.** `///` with a description, at minimum. Examples where the usage is non-obvious.

### Performance

- **The worker loop is hot code.** Every allocation, every clone, every unnecessary copy in the worker loop is a throughput hit. Profile before assuming.
- **Use `Arc` for shared state, not `Mutex` where avoidable.** The job registry is read-only after startup — it should be `Arc<HashMap>`, not `Mutex<HashMap>`.
- **Connection pool is shared across workers.** `sqlx::PgPool` is already `Clone + Send + Sync`. Pass it by clone (cheap — it's an Arc internally).
- **Tokio tasks are cheap.** Each worker is a `tokio::spawn`ed task, not a thread. Spin up N workers as async tasks. This is the intended usage.
- **Minimize time inside transactions.** Lock the job, start the transaction, run the job, commit. Do not do expensive non-DB work inside a transaction.

### Async Rust

- **All DB operations are async via sqlx.** Never block the Tokio runtime with sync operations.
- **Use `tokio::select!` for graceful shutdown.** The worker loop should be cancellable via a shutdown signal.
- **Avoid `std::thread::sleep`.** Use `tokio::time::sleep` inside async contexts.
- **Worker concurrency via `tokio::spawn`.** Each worker is an independent async task. Use `JoinSet` or `FuturesUnordered` to manage them.

### Postgres / sqlx

- **Use `sqlx::query!` macros** where possible for compile-time query verification.
- **Use `FOR UPDATE SKIP LOCKED` exactly as written.** Do not simplify this query. It is the core of the exactly-once guarantee.
- **All DB queries must handle errors explicitly.** A failed query is not a panic — it's a `RhinoError::Database(sqlx::Error)`.
- **Migrations run automatically on startup.** Use `sqlx::migrate!` macro. Users should never run migrations manually.

### Testing

- **The exactly-once test is sacred.** It must pass on every single run, no exceptions, no flakiness. If it ever fails, stop everything and fix it before proceeding.
- **Unit tests require no database.** Test serialization, backoff formula, config parsing without Postgres.
- **Integration tests use a real Postgres instance.** Use `#[sqlx::test]` for automatic test DB setup and teardown.
- **Stress tests run before every release.** Not negotiable.

### What "Exceed the Benchmarks" Means

The targets in the performance table above are the floor, not the ceiling. When writing the worker loop, ask:

- Can I reduce allocations in this hot path?
- Can I batch multiple status updates in one query instead of N queries?
- Can I use a smarter polling strategy than naive sleep-and-poll?
- Can I pipeline Postgres queries?

The goal is to make Rhino the fastest Postgres-backed job queue that exists, in any language. That is achievable. The Rust async runtime + sqlx + zero GC is a fundamentally better foundation than the BEAM, V8, or CPython for this workload.

---

## What Rhino Is Not

- **Not a message broker.** No pub/sub, no fan-out, no Kafka replacement.
- **Not a streaming platform.** Not for event streaming.
- **Not a cron daemon.** Scheduled jobs are supported but Rhino is not a cron replacement.
- **Not a SaaS.** No cloud, no dashboard to log into, no account, no phone-home.
- **Not magic.** If Postgres is down, the queue is down. This is a feature — one fewer thing to monitor.

---

## Architecture — One Queue, Any Language

```
Your Python app       Your Node app        Your Rust app
      |                     |                    |
pip install rhinoqueue  bun add rhinoqueue  cargo add rhinoqueue
      |                     |                    |
      +---------------------+--------------------+
                            |
               INSERT into rhino_jobs
               (Postgres — you already have it)
                            |
               Rhino Rust Worker Binary
               (3MB, 2-5MB RAM, exactly-once)
               retries · batches · chains
               workflows · rate limiting
```

The worker doesn't know or care which language enqueued a job. It sees a row. It locks it. It runs it. It commits. The guarantee is universal.

---

## Feasibility — Every Claim is Proven

Nothing in this project is research. Everything is assembly of proven primitives:

- `SELECT FOR UPDATE SKIP LOCKED` — Postgres docs use job queues as the canonical example
- Exactly-once via transactions — Oban, Django-pgq, Good Job all do this in production
- `#[rhino]` macro — same technique as `#[tokio::main]`, `#[derive(Serialize)]`
- 3MB binary — provable today: `cargo build --release` with optimized profile on any Rust project
- 2–5MB RAM — Tokio + sqlx pool at idle, measurable on any existing Rust service
- PyO3 Python bindings — Pydantic v2, Polars, ruff are all Rust cores via PyO3
- napi-rs Node bindings — SWC (powers Next.js), Biome are all Rust cores via napi-rs
- Thin client Python/Node — 200 lines of psycopg3 or postgres.js with schema knowledge

The hardest part is the `#[rhino]` macro ergonomics — getting it to handle all async function signature edge cases cleanly. This does not block v0.1. The macro is refined over time.

---

## Current Status

**Active version:** v0.1  
**Goal:** worker loop, exactly-once guarantee, stress tester proves zero duplicates.

Start here. Build the worker. Get the guarantee right. Everything else is interface.