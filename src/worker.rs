use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use rand::Rng;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

pub const DB_URL: &str = "postgresql://rhino:rhino@localhost:5445/rhino_db";

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

    Ok(pool)
}

pub async fn daemon(pool: &PgPool, worker_id: &str) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let pending_job = sqlx::query(
        "SELECT id::text AS id, job_type, payload, attempts, max_attempts
         FROM rhino_jobs
         WHERE status = 'pending'
           AND (locked_at IS NULL OR locked_at < NOW() - INTERVAL '30 seconds')
           AND run_at <= NOW()
         ORDER BY priority DESC, run_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED",
    )
    .fetch_optional(&mut *tx)
    .await?;

    let Some(job) = pending_job else {
        tx.commit().await?;
        return Ok(());
    };

    let id: String = job.get("id");
    let job_type: String = job.get("job_type");
    let payload: Value = job.get("payload");
    let attempts: i32 = job.get("attempts");
    let max_attempts: i32 = job.get("max_attempts");

    sqlx::query(
        "UPDATE rhino_jobs
         SET status = 'locked', locked_at = clock_timestamp(), locked_by = $2
         WHERE id::text = $1",
    )
    .bind(&id)
    .bind(worker_id)
    .execute(&mut *tx)
    .await?;

    let is_task_done = execute_job(&mut tx, &id, &job_type, &payload).await?;

    if is_task_done {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'done', done_at = clock_timestamp()
             WHERE id::text = $1",
        )
        .bind(&id)
        .execute(&mut *tx)
        .await?;
    } else {
        let next_attempt = attempts + 1;

        if max_attempts > 0 && next_attempt >= max_attempts {
            sqlx::query(
                "UPDATE rhino_jobs
                 SET status = 'dead', attempts = $2
                 WHERE id::text = $1",
            )
            .bind(&id)
            .bind(next_attempt)
            .execute(&mut *tx)
            .await?;
        } else {
            sqlx::query(
                "UPDATE rhino_jobs
                 SET status = 'pending', attempts = $2, run_at = NOW() + INTERVAL '30 seconds', locked_at = NULL, locked_by = NULL
                 WHERE id::text = $1",
            )
            .bind(&id)
            .bind(next_attempt)
            .execute(&mut *tx)
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

async fn execute_job(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    job_id: &str,
    job_type: &str,
    _payload: &Value,
) -> Result<bool, sqlx::Error> {
    match job_type {
        "stress_random" => {
            let (op_kind, input_bytes, output_bytes, output_digest) = tokio::task::spawn_blocking(run_random_realistic_job)
                .await
                .map_err(|e| sqlx::Error::Protocol(format!("spawn_blocking join error: {e}").into()))?
                .map_err(|e| sqlx::Error::Protocol(e.into()))?;

            sqlx::query(
                "INSERT INTO stress_job_results (job_id, op_kind, input_bytes, output_bytes, output_digest)
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(job_id)
            .bind(op_kind)
            .bind(input_bytes)
            .bind(output_bytes)
            .bind(output_digest)
            .execute(&mut **tx)
            .await?;

            sqlx::query("UPDATE stress_results SET counter = counter + 1")
                .execute(&mut **tx)
                .await?;

            Ok(true)
        }
        "stress_noop" => {
            sqlx::query("UPDATE stress_results SET counter = counter + 1")
                .execute(&mut **tx)
                .await?;
            Ok(true)
        }
        _ => {
            let result = some_job(10, 20).await;
            Ok(result > 0)
        }
    }
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
