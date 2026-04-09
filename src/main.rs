use std::time::Duration;

use sqlx::{PgPool, Row};
use tokio::time::sleep;

/**
 * jobs table holds job to be processed. it also has a status column
 * a writer, it inserts a row with status = 'pending'
 * a daemon, a loop that asks 'any pending jobs?' and runs them
 * 
 * pesudocode
 * loop forever:
    row = SELECT * FROM rhino_jobs 
          WHERE status = 'pending' 
          FOR UPDATE SKIP LOCKED
          LIMIT 1

    if no row:
        sleep 500ms
        continue

    run(row.job_type, row.payload)

    if success:
        UPDATE status = 'done'
    if failure:
        UPDATE attempts = attempts + 1
        UPDATE run_at = now + backoff
        UPDATE status = 'pending'
    if attempts >= max_attempts:
        UPDATE status = 'dead'
 */ 

// constants
const DB_URL: &str = "postgresql://rhino:rhino@localhost:5445/rhino_db";

#[tokio::main]
async fn main() {
    let pool = match init_db().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Database initialization failed: {}", e);
            return;
        }
    };

    println!("Rhino Daemon is running...");

    loop {
        if let Err(e) = daemon(&pool).await {
            eprintln!("Daemon error: {}", e);
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn daemon(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let pending_job = sqlx::query(
        "SELECT id::text AS id, job_type, payload, attempts, max_attempts
         FROM rhino_jobs
         WHERE status = 'pending'
           AND run_at <= NOW()
         ORDER BY priority DESC, run_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED",
    )
    .fetch_optional(&mut *tx)
    .await?;

    let Some(job) = pending_job else {
        tx.commit().await?;
        println!("No pending jobs found.");
        return Ok(());
    };

    let id: String = job.get("id");
    let attempts: i32 = job.get("attempts");
    let max_attempts: i32 = job.get("max_attempts");

    sqlx::query(
        "UPDATE rhino_jobs
         SET status = 'locked', locked_at = NOW(), locked_by = $2
         WHERE id::text = $1",
    )
    .bind(&id)
    .bind("worker-1")
    .execute(&mut *tx)
    .await?;

    // Placeholder execution result for MVP wiring.
    let is_task_done = true;

    if is_task_done {
        sqlx::query(
            "UPDATE rhino_jobs
             SET status = 'done'
             WHERE id::text = $1",
        )
        .bind(&id)
        .execute(&mut *tx)
        .await?;

        println!("task is done");
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

async fn init_db() -> Result<PgPool, sqlx::Error> {
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
            locked_by    TEXT,
            inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
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

