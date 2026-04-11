use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use rand::Rng;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub const DB_URL: &str = "postgresql://rhino:rhino@localhost:5445/rhino_db";
const CLAIM_BATCH_SIZE: i64 = 50;

struct ClaimedJob {
    id: Uuid,
    job_type: String,
    payload: Value,
    attempts: i32,
    max_attempts: i32,
}

struct JobExecution {
    success: bool,
    stress_result: Option<(String, i32, i32, String)>,
}

pub async fn init_db() -> Result<PgPool, sqlx::Error> {
    let pool = PgPool::connect(DB_URL).await?;

    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rhino_jobs (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            job_type     TEXT        NOT NULL,
            payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
            status       TEXT        NOT NULL DEFAULT 'pending',
            priority     INT         NOT NULL DEFAULT 0,
            attempts     INT         NOT NULL DEFAULT 0,
            max_attempts INT         NOT NULL DEFAULT 3,
            run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            locked_at    TIMESTAMPTZ,
            done_at      TIMESTAMPTZ,
            locked_by    TEXT,
            inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(&pool)
    .await?;

    sqlx::query("ALTER TABLE rhino_jobs ADD COLUMN IF NOT EXISTS done_at TIMESTAMPTZ")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS rhino_jobs_fetchable
         ON rhino_jobs (status, priority DESC, run_at ASC)
         WHERE status = 'pending'",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS rhino_jobs_reclaimable
         ON rhino_jobs (status, locked_at)
         WHERE status = 'locked'",
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

pub async fn daemon(pool: &PgPool, worker_id: &str) -> Result<usize, sqlx::Error> {
    let claimed_jobs = claim_jobs(pool, worker_id, CLAIM_BATCH_SIZE).await?;
    if claimed_jobs.is_empty() {
        return Ok(0);
    }

    let processed_count = claimed_jobs.len();

    for job in claimed_jobs {
        let execution = execute_job(&job.job_type, &job.payload).await?;
        finalize_job(pool, &job, execution).await?;
    }

    Ok(processed_count)
}

async fn execute_job(
    job_type: &str,
    _payload: &Value,
) -> Result<JobExecution, sqlx::Error> {
    match job_type {
        "stress_random" => {
            let (op_kind, input_bytes, output_bytes, output_digest) = tokio::task::spawn_blocking(run_random_realistic_job)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("spawn_blocking join error: {e}").into()))?
                .map_err(|e| sqlx::Error::Protocol(e.into()))?;

            Ok(JobExecution {
                success: true,
                stress_result: Some((op_kind, input_bytes, output_bytes, output_digest)),
            })
        }
        "stress_noop" => {
            Ok(JobExecution {
                success: true,
                stress_result: None,
            })
        }
        _ => {
            let result = some_job(10, 20).await;
            Ok(JobExecution {
                success: result > 0,
                stress_result: None,
            })
        }
    }
}

async fn claim_jobs(pool: &PgPool, worker_id: &str, batch_size: i64) -> Result<Vec<ClaimedJob>, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let rows = sqlx::query(
        "WITH candidates AS (
            SELECT id, job_type, payload, attempts, max_attempts
            FROM rhino_jobs
            WHERE (
                    status = 'pending'
                AND run_at <= NOW()
            ) OR (
                    status = 'locked'
                AND locked_at IS NOT NULL
                AND locked_at < NOW() - INTERVAL '30 seconds'
            )
            ORDER BY priority DESC, run_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE rhino_jobs j
        SET status = 'locked', locked_at = clock_timestamp(), locked_by = $1
        FROM candidates c
        WHERE j.id = c.id
        RETURNING j.id, c.job_type, c.payload, c.attempts, c.max_attempts",
    )
    .bind(worker_id)
    .bind(batch_size)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    let claimed = rows
        .into_iter()
        .map(|row| ClaimedJob {
            id: row.get("id"),
            job_type: row.get("job_type"),
            payload: row.get("payload"),
            attempts: row.get("attempts"),
            max_attempts: row.get("max_attempts"),
        })
        .collect();

    Ok(claimed)
}

async fn finalize_job(pool: &PgPool, job: &ClaimedJob, execution: JobExecution) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    if let Some((op_kind, input_bytes, output_bytes, output_digest)) = execution.stress_result {
        sqlx::query(
            "INSERT INTO stress_job_results (job_id, op_kind, input_bytes, output_bytes, output_digest)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (job_id) DO NOTHING",
        )
        .bind(job.id)
        .bind(op_kind)
        .bind(input_bytes)
        .bind(output_bytes)
        .bind(output_digest)
        .execute(&mut *tx)
        .await?;
    }

    if execution.success {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'done', done_at = clock_timestamp()
             WHERE id = $1",
        )
        .bind(job.id)
        .execute(&mut *tx)
        .await?;
    } else {
        let next_attempt = job.attempts + 1;

        if job.max_attempts > 0 && next_attempt >= job.max_attempts {
            sqlx::query(
                "UPDATE rhino_jobs
                 SET status = 'dead', attempts = $2
                 WHERE id = $1",
            )
            .bind(job.id)
            .bind(next_attempt)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "UPDATE rhino_jobs
                 SET status = 'pending', attempts = $2, run_at = NOW() + INTERVAL '30 seconds', locked_at = NULL, locked_by = NULL
                 WHERE id = $1",
            )
            .bind(job.id)
            .bind(next_attempt)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

async fn some_job(first_number: i16, second_number: i16) -> i16 {
    if first_number < 0 || second_number < 0 {
        -1
    } else {
        first_number + second_number
    }
}

fn run_random_realistic_job() -> Result<(String, i32, i32, String), String> {
    let mut rng = rand::thread_rng();
    let input_size: usize = rng.gen_range(2048..8192);

    let mut input = vec![0u8; input_size];
    rng.fill(input.as_mut_slice());

    if rng.gen_bool(0.5) {
        let digest = Sha256::digest(&input);
        let digest_hex = format!("{:x}", digest);
        Ok(("hash".to_string(), input_size as i32, 32, digest_hex))
    } else {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&input)
            .map_err(|e| format!("gzip write failed: {e}"))?;
        let compressed = encoder
            .finish()
            .map_err(|e| format!("gzip finish failed: {e}"))?;
        let digest = Sha256::digest(&compressed);
        let digest_hex = format!("{:x}", digest);
        Ok((
            "compression".to_string(),
            input_size as i32,
            compressed.len() as i32,
            digest_hex,
        ))
    }
}
