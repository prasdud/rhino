/*
 * This file is part of Rhino.
 *
 * Rhino is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Rhino is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Rhino.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use rand::Rng;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub const DB_URL: &str = "postgresql://rhino:rhino@localhost:5445/rhino_db";
const CLAIM_BATCH_SIZE: i64 = 1000;

pub struct ClaimedJob {
    pub id: Uuid,
    pub job_type: String,
    pub payload: Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

pub struct JobExecution {
    pub success: bool,
    pub stress_result: Option<(String, i32, i32, String)>,
}

pub async fn init_db() -> Result<PgPool, sqlx::Error> {
    let pool = PgPool::connect(DB_URL).await?;

    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .execute(&pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rhino_jobs (
            id           UUID        PRIMARY KEY DEFAULT uuidv7(),
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
    let results = execute_batch(claimed_jobs).await?;
    finalize_jobs_batch(pool, results).await?;

    Ok(processed_count)
}

pub async fn daemon_pipelined(
    pool: &PgPool,
    worker_id: &str,
    prev_results: Option<Vec<(ClaimedJob, JobExecution)>>,
) -> Result<(usize, Option<Vec<(ClaimedJob, JobExecution)>>), sqlx::Error> {
    let (claimed_res, finalize_res) = tokio::join!(
        claim_jobs(pool, worker_id, CLAIM_BATCH_SIZE),
        async {
            if let Some(res) = prev_results {
                finalize_jobs_batch(pool, res).await?;
            }
            Ok::<(), sqlx::Error>(())
        }
    );

    finalize_res?;
    let claimed_jobs = claimed_res?;

    if claimed_jobs.is_empty() {
        return Ok((0, None));
    }

    let processed_count = claimed_jobs.len();
    let results = execute_batch(claimed_jobs).await?;

    Ok((processed_count, Some(results)))
}

async fn execute_batch(claimed_jobs: Vec<ClaimedJob>) -> Result<Vec<(ClaimedJob, JobExecution)>, sqlx::Error> {
    let processed_count = claimed_jobs.len();
    let mut set = tokio::task::JoinSet::new();

    for job in claimed_jobs {
        set.spawn(async move {
            let execution = execute_job(&job.job_type, &job.payload).await;
            (job, execution)
        });
    }

    let mut results = Vec::with_capacity(processed_count);
    while let Some(res) = set.join_next().await {
        let (job, execution_res) = res.map_err(|e| sqlx::Error::Protocol(format!("Join error: {e}").into()))?;
        results.push((job, execution_res?));
    }

    Ok(results)
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
    let rows = sqlx::query(
        "WITH candidates AS (
            SELECT id, job_type, payload, attempts, max_attempts
            FROM rhino_jobs
            WHERE status = 'pending'
              AND run_at <= NOW()
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
    .fetch_all(pool)
    .await?;

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

async fn finalize_jobs_batch(pool: &PgPool, results: Vec<(ClaimedJob, JobExecution)>) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let mut success_ids = Vec::new();
    let mut dead_ids = Vec::new();
    let mut dead_attempts = Vec::new();
    let mut retry_ids = Vec::new();
    let mut retry_attempts = Vec::new();

    let mut stress_job_ids = Vec::new();
    let mut stress_op_kinds = Vec::new();
    let mut stress_inputs = Vec::new();
    let mut stress_outputs = Vec::new();
    let mut stress_digests = Vec::new();

    for (job, execution) in results {
        if let Some((op_kind, input_bytes, output_bytes, output_digest)) = execution.stress_result {
            stress_job_ids.push(job.id);
            stress_op_kinds.push(op_kind);
            stress_inputs.push(input_bytes);
            stress_outputs.push(output_bytes);
            stress_digests.push(output_digest);
        }

        if execution.success {
            success_ids.push(job.id);
        } else {
            let next_attempt = job.attempts + 1;
            if job.max_attempts > 0 && next_attempt >= job.max_attempts {
                dead_ids.push(job.id);
                dead_attempts.push(next_attempt);
            } else {
                retry_ids.push(job.id);
                retry_attempts.push(next_attempt);
            }
        }
    }

    if !stress_job_ids.is_empty() {
        sqlx::query(
            "INSERT INTO stress_job_results (job_id, op_kind, input_bytes, output_bytes, output_digest)
             SELECT * FROM UNNEST($1, $2, $3, $4, $5)
             ON CONFLICT (job_id) DO NOTHING",
        )
        .bind(&stress_job_ids)
        .bind(&stress_op_kinds)
        .bind(&stress_inputs)
        .bind(&stress_outputs)
        .bind(&stress_digests)
        .execute(&mut *tx)
        .await?;
    }

    if !success_ids.is_empty() {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'done', done_at = clock_timestamp()
             WHERE id = ANY($1)",
        )
        .bind(&success_ids)
        .execute(&mut *tx)
        .await?;
    }

    if !dead_ids.is_empty() {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'dead', attempts = u.next_attempt
             FROM UNNEST($1, $2) AS u(id, next_attempt)
             WHERE rhino_jobs.id = u.id",
        )
        .bind(&dead_ids)
        .bind(&dead_attempts)
        .execute(&mut *tx)
        .await?;
    }

    if !retry_ids.is_empty() {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'pending', attempts = u.next_attempt, run_at = NOW() + INTERVAL '30 seconds', locked_at = NULL, locked_by = NULL
             FROM UNNEST($1, $2) AS u(id, next_attempt)
             WHERE rhino_jobs.id = u.id",
        )
        .bind(&retry_ids)
        .bind(&retry_attempts)
        .execute(&mut *tx)
        .await?;
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
