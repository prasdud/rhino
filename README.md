# rhino
<img src="rhino-attack.webp" alt="rhino dude" height="400" style="width:1000px; object-fit:fill;" />

A durable, exactly-once job queue built in Rust.

# to-do (v0.1)

## Done
- [x] move from sync postgres client to `tokio` + `sqlx`
- [x] async `main` with Tokio runtime
- [x] create `rhino_jobs` table on startup
- [x] add worker fetch index for pending jobs
- [x] `SELECT ... FOR UPDATE SKIP LOCKED` query wired
- [x] sleep when no pending jobs (avoid hammering Postgres)

## Next
- [ ] finish real job execution hook (replace placeholder `is_task_done = true`)
- [ ] implement exponential backoff + jitter (currently fixed 30s)
- [ ] finalize retry/dead-letter behavior and edge cases (`max_attempts = 0` unlimited)
- [ ] add lock timeout recovery path for crashed workers
- [ ] tighten transaction boundaries around execute + final status update
- [ ] add enqueue path (`job_type`, `payload`) + smoke test

## After POC
- [ ] split `main.rs` into modules (`worker`, `queue`, `backoff`, `error`, `config`)
- [ ] add minimal abstraction + typed errors