# rhino
<img src="rhino-attack.webp" alt="rhino dude" height="400" style="width:1000px; object-fit:fill;" />

A durable, exactly-once job queue built in Rust.

## Current Status (v0.1 POC Optimized)
Rhino is in a highly optimized Proof-of-Concept state, proving that a Postgres-backed queue in Rust can achieve industry-leading performance while maintaining strict "exactly-once" guarantees.

### Performance Benchmarks (Current)
*   **Peak Throughput:** ~45,000 jobs/sec (10 workers, 250k jobs)
*   **Single-Worker Peak:** ~32,000 jobs/sec (1 worker, 1000 batch size)
*   **Insertion Speed:** ~80,000 jobs/sec (Bulk INSERT with QueryBuilder)
*   **Latency:** p50 < 10ms (single worker), p50 < 200ms (10 workers, 1000 batch size)

## Done
- [x] **Async Engine:** Fully `tokio` + `sqlx` powered.
- [x] **Exactly-Once Core:** Implemented via `SELECT FOR UPDATE SKIP LOCKED`.
- [x] **Batch Processing:** Workers claim and finalize jobs in batches (up to 1000) using `UNNEST` and `ANY($1)` to minimize DB round-trips.
- [x] **Internal Concurrency:** Workers process batch jobs concurrently using `JoinSet`.
- [x] **Index Optimization:** Hot-path queries strictly follow `CLAUDE.md` to ensure $O(1)$ index-ordered scans.
- [x] **Pipelined Execution:** Overlapped fetching and finalization to maximize I/O utilization.
- [x] **High-Speed Stress Tester:** Optimized setup for testing multi-million job scenarios.

## To Be Addressed (Technical Debt & Risks)
The current high-performance numbers involve specific trade-offs that must be resolved for a production-ready v1.0:
*   **Crash Recovery:** To hit 32k/sec, the "hot path" locking query was optimized to ignore timed-out jobs. Currently, if a worker crashes, its jobs will stay `locked` forever until a "Reaper" task is implemented.
*   **Batch Blast Radius:** Finalizing 1,000 jobs in one transaction means a crash mid-batch affects 1,000 jobs simultaneously. If the worker crashes after the work is done but before the commit, all 1,000 jobs will eventually re-run.
*   **Timeout Hazard:** If a batch of 1,000 jobs takes longer than the 30s lock timeout (due to slow I/O or CPU contention), a second worker may "reclaim" the jobs while the first is still working, breaking the exactly-once guarantee.
*   **UUID Fragmentation:** v4 UUIDs cause massive B-Tree fragmentation at 5M+ rows, leading to the "performance cliff" observed in multi-million job runs.

## Next Steps: Production Hardening
As we scale to multi-million job tables, the following "Production Hardening" tasks are required to maintain performance:

- [ ] **Sequential UUIDs (v7):** Switch Primary Keys to time-ordered UUIDs to prevent B-Tree fragmentation and random I/O at 5M+ row scales.
- [ ] **Concurrency Throttling:** Implement `Semaphore`-based limits on internal worker execution to prevent thread pool exhaustion during heavy CPU/IO bursts.
- [ ] **Background Reaper:** Implement a dedicated background task to reclaim timed-out `locked` jobs, keeping the hot-path `claim_jobs` query simple and fast.
- [ ] **Modularization:** Split `src/worker.rs` into cleaner modules (`worker`, `queue`, `backoff`, `error`).
- [ ] **Macro Support:** Implement the `#[rhino]` procedural macro for seamless developer ergonomics.
- [ ] **Retry with Jitter:** Replace fixed backoff with a full exponential backoff + jitter implementation.
- [ ] **Graceful Shutdown:** Ensure in-flight jobs finish or are safely released during worker termination.

## Development
### Commands
**Run DB:**
```bash
docker-compose up -d
```

**Run Performance Benchmarks:**
```bash
cargo run --release --bin v0_1_master_tester
```

**Access Database:**
```bash
docker exec -it rhino_db psql -U rhino -d rhino_db
```
