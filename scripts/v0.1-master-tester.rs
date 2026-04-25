use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Utc;
use rhino::worker::{init_db, DB_URL};
use serde::Serialize;
use sqlx::{PgPool, Row};
use tokio::time::sleep;

const LOCK_TIMEOUT_SECS: i32 = 30;

#[derive(Clone, Copy)]
struct Scenario {
    jobs: i32,
    workers: usize,
}

#[derive(Serialize)]
struct ScenarioReport {
    run_id: String,
    started_at_utc: String,
    scenario_jobs: i32,
    scenario_workers: usize,
    insert_duration_secs: f64,
    drain_duration_secs: f64,
    insert_throughput_jobs_per_sec: f64,
    drain_throughput_jobs_per_sec: f64,
    counter: i32,
    jobs_done: i64,
    jobs_dead: i64,
    jobs_pending: i64,
    jobs_locked: i64,
    job_results_rows: i64,
    queue_wait_p50_ms: f64,
    queue_wait_p95_ms: f64,
    queue_wait_p99_ms: f64,
    processing_p50_ms: f64,
    processing_p95_ms: f64,
    processing_p99_ms: f64,
    exactly_once_ok: bool,
}

#[derive(Serialize)]
struct OutputReport {
    generated_at_utc: String,
    db_url: String,
    lock_timeout_secs: i32,
    scenarios: Vec<ScenarioReport>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_ts = Utc::now();
    let run_id = run_ts.format("%Y%m%dT%H%M%SZ").to_string();

    let pool = init_db().await?;
    ensure_stress_schema(&pool).await?;

    // Gauge v0.1 as-is across multiple loads.
    let scenarios = vec![
        Scenario { jobs: 1_000_000, workers: 10 },
        Scenario { jobs: 5_000_000, workers: 10 },
        Scenario { jobs: 10_000_000, workers: 10 },
    ];

    // Previous scenarios:
    // Scenario { jobs: 250_000, workers: 10 },
    // Scenario { jobs: 500_000, workers: 10 },
    // Scenario { jobs: 1_000_000, workers: 10 },
    // Scenario { jobs: 5_000_000, workers: 10 },

    println!("=== Rhino v0.1 Master Tester ===");
    println!("Run ID: {}", run_id);
    println!("Scenarios: {}", scenarios.len());

    let mut reports = Vec::with_capacity(scenarios.len());

    for (idx, scenario) in scenarios.iter().enumerate() {
        println!("\n--- Scenario {}/{} ---", idx + 1, scenarios.len());
        println!("jobs={} workers={}", scenario.jobs, scenario.workers);

        let report = run_scenario(&pool, run_id.clone(), run_ts.to_rfc3339(), *scenario).await?;
        println!(
            "result: done={} counter={} pending={} locked={} throughput={:.0} jobs/s queue_wait(p50/p95/p99)={:.2}/{:.2}/{:.2}ms processing(p50/p95/p99)={:.2}/{:.2}/{:.2}ms exactly_once={}",
            report.jobs_done,
            report.counter,
            report.jobs_pending,
            report.jobs_locked,
            report.drain_throughput_jobs_per_sec,
            report.queue_wait_p50_ms,
            report.queue_wait_p95_ms,
            report.queue_wait_p99_ms,
            report.processing_p50_ms,
            report.processing_p95_ms,
            report.processing_p99_ms,
            report.exactly_once_ok
        );

        reports.push(report);
    }

    let output = OutputReport {
        generated_at_utc: Utc::now().to_rfc3339(),
        db_url: DB_URL.to_string(),
        lock_timeout_secs: LOCK_TIMEOUT_SECS,
        scenarios: reports,
    };

    let output_dir = Path::new("outputs");
    if !output_dir.exists() {
        fs::create_dir_all(output_dir)?;
    }

    let output_path = format!("outputs/output-{}.json", run_id);
    let json = serde_json::to_string_pretty(&output)?;
    fs::write(&output_path, json)?;

    println!("\nSaved report: {}", output_path);
    Ok(())
}

async fn run_scenario(
    pool: &PgPool,
    run_id: String,
    started_at_utc: String,
    scenario: Scenario,
) -> Result<ScenarioReport, Box<dyn std::error::Error>> {
    reset_data(pool).await?;

    let insert_start = Instant::now();
    let chunk_size = 5000;
    for i in (0..scenario.jobs).step_by(chunk_size) {
        let current_chunk_size = (scenario.jobs - i).min(chunk_size as i32);
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> =
            sqlx::QueryBuilder::new("INSERT INTO rhino_jobs (job_type, payload, status) ");

        query_builder.push_values(0..current_chunk_size, |mut b, _| {
            b.push_bind("stress_random")
                .push_bind(serde_json::json!({}))
                .push_bind("pending");
        });

        let query = query_builder.build();
        query.execute(pool).await?;
    }
    let insert_duration = insert_start.elapsed();

    let drain_start = Instant::now();
    let mut handles = Vec::with_capacity(scenario.workers);
    for worker_id in 0..scenario.workers {
        let pool_clone = pool.clone();
        handles.push(tokio::spawn(async move {
            worker_loop(&pool_clone, worker_id).await;
        }));
    }

    loop {
        let pending_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'pending'")
                .fetch_one(pool)
                .await?;
        let locked_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'locked'")
                .fetch_one(pool)
                .await?;
        let done_live: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'done'")
                .fetch_one(pool)
                .await?;

        println!(
            "[live] done={} pending={} locked={} elapsed={:.1}s",
            done_live,
            pending_live,
            locked_live,
            drain_start.elapsed().as_secs_f64()
        );

        if handles.iter().all(|h| h.is_finished()) {
            break;
        }

        sleep(Duration::from_secs(1)).await;
    }

    for h in handles {
        let _ = h.await;
    }
    let drain_duration = drain_start.elapsed();

    let jobs_done: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'done'")
            .fetch_one(pool)
            .await?;
    let jobs_dead: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'dead'")
            .fetch_one(pool)
            .await?;
    let jobs_pending: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'pending'")
            .fetch_one(pool)
            .await?;
    let jobs_locked: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'locked'")
            .fetch_one(pool)
            .await?;
    let job_results_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM stress_job_results")
            .fetch_one(pool)
            .await?;

    let counter: i32 = job_results_rows as i32;

    let exactly_once_ok = jobs_done == scenario.jobs as i64 && job_results_rows == scenario.jobs as i64;

    let queue_wait_row = sqlx::query(
        "SELECT
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY (locked_at - inserted_at))) * 1000)::double precision, 0::double precision) AS p50_ms,
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY (locked_at - inserted_at))) * 1000)::double precision, 0::double precision) AS p95_ms,
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY (locked_at - inserted_at))) * 1000)::double precision, 0::double precision) AS p99_ms
         FROM rhino_jobs
         WHERE status = 'done' AND locked_at IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;

    let processing_row = sqlx::query(
        "SELECT
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.50) WITHIN GROUP (ORDER BY (done_at - locked_at))) * 1000)::double precision, 0::double precision) AS p50_ms,
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.95) WITHIN GROUP (ORDER BY (done_at - locked_at))) * 1000)::double precision, 0::double precision) AS p95_ms,
            COALESCE((EXTRACT(EPOCH FROM percentile_cont(0.99) WITHIN GROUP (ORDER BY (done_at - locked_at))) * 1000)::double precision, 0::double precision) AS p99_ms
         FROM rhino_jobs
         WHERE status = 'done' AND done_at IS NOT NULL AND locked_at IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;

    let queue_wait_p50_ms: f64 = queue_wait_row.get("p50_ms");
    let queue_wait_p95_ms: f64 = queue_wait_row.get("p95_ms");
    let queue_wait_p99_ms: f64 = queue_wait_row.get("p99_ms");
    let processing_p50_ms: f64 = processing_row.get("p50_ms");
    let processing_p95_ms: f64 = processing_row.get("p95_ms");
    let processing_p99_ms: f64 = processing_row.get("p99_ms");

    Ok(ScenarioReport {
        run_id,
        started_at_utc,
        scenario_jobs: scenario.jobs,
        scenario_workers: scenario.workers,
        insert_duration_secs: insert_duration.as_secs_f64(),
        drain_duration_secs: drain_duration.as_secs_f64(),
        insert_throughput_jobs_per_sec: scenario.jobs as f64 / insert_duration.as_secs_f64(),
        drain_throughput_jobs_per_sec: scenario.jobs as f64 / drain_duration.as_secs_f64(),
        counter,
        jobs_done,
        jobs_dead,
        jobs_pending,
        jobs_locked,
        job_results_rows,
        queue_wait_p50_ms,
        queue_wait_p95_ms,
        queue_wait_p99_ms,
        processing_p50_ms,
        processing_p95_ms,
        processing_p99_ms,
        exactly_once_ok,
    })
}

async fn worker_loop(pool: &PgPool, worker_id: usize) {
    use rhino::worker::daemon_pipelined;
    let mut prev_results = None;

    loop {
        match daemon_pipelined(pool, &format!("worker-{worker_id}"), prev_results).await {
            Ok((0, None)) => {
                let remaining = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM rhino_jobs WHERE status IN ('pending', 'locked')",
                )
                .fetch_one(pool)
                .await;

                match remaining {
                    Ok(0) => break,
                    Ok(_) => {
                        prev_results = None;
                        sleep(Duration::from_millis(5)).await;
                    }
                    Err(_) => break,
                }
            }
            Ok((_, current_results)) => {
                prev_results = current_results;
            }
            Err(_) => break,
        }
    }
}

async fn ensure_stress_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS stress_job_results (
            job_id UUID PRIMARY KEY,
            op_kind TEXT NOT NULL,
            input_bytes INT NOT NULL,
            output_bytes INT NOT NULL,
            output_digest TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "ALTER TABLE stress_job_results
         ALTER COLUMN job_id TYPE UUID
         USING job_id::uuid",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn reset_data(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("TRUNCATE TABLE rhino_jobs").execute(pool).await?;
    sqlx::query("TRUNCATE TABLE stress_job_results").execute(pool).await?;
    Ok(())
}
