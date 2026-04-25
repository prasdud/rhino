use std::time::{Duration, Instant};
use sqlx::{PgPool, Row};
use tokio::time::sleep;

const DB_URL: &str = "postgresql://rhino:rhino@localhost:5445/rhino_db";
const NUM_JOBS: i32 = 10_000;
const NUM_WORKERS: usize = 20;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(DB_URL).await?;

    println!("=== Rhino Stress Test ===");
    println!("Config: {} jobs / {} workers", NUM_JOBS, NUM_WORKERS);

    // Setup: create stress_results table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS stress_results (
            counter INT NOT NULL DEFAULT 0
        )",
    )
    .execute(&pool)
    .await?;

    // Reset counter
    sqlx::query("DELETE FROM stress_results").execute(&pool).await?;
    sqlx::query("INSERT INTO stress_results (counter) VALUES (0)")
        .execute(&pool)
        .await?;

    // Clear any leftover jobs
    sqlx::query("TRUNCATE TABLE rhino_jobs").execute(&pool).await?;

    // Insert 10k jobs
    println!("Inserting {} jobs...", NUM_JOBS);
    let start_insert = Instant::now();
    for _ in 0..NUM_JOBS {
        sqlx::query(
            "INSERT INTO rhino_jobs (job_type, payload, status)
             VALUES ('stress_noop', '{}', 'pending')",
        )
        .execute(&pool)
        .await?;
    }
    let insert_time = start_insert.elapsed();
    println!("Inserted in {:.2}s", insert_time.as_secs_f64());

    // Spawn workers
    println!("Spawning {} workers...", NUM_WORKERS);
    let start_drain = Instant::now();

    let mut handles = vec![];
    for worker_id in 0..NUM_WORKERS {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            worker_loop(&pool_clone, worker_id).await
        });
        handles.push(handle);
    }

    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.await;
    }

    let drain_time = start_drain.elapsed();
    println!("Drained in {:.2}s", drain_time.as_secs_f64());

    // Check results
    let result_row = sqlx::query("SELECT counter FROM stress_results")
        .fetch_one(&pool)
        .await?;
    let counter: i32 = result_row.get("counter");

    let done_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'done'")
        .fetch_one(&pool)
        .await?;

    let dead_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'dead'")
        .fetch_one(&pool)
        .await?;

    let pending_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rhino_jobs WHERE status = 'pending'")
        .fetch_one(&pool)
        .await?;

    println!("\n=== Results ===");
    println!("Counter (atomic increments): {}", counter);
    println!("Jobs done: {}", done_count);
    println!("Jobs dead: {}", dead_count);
    println!("Jobs pending: {}", pending_count);
    println!("Throughput: {:.0} jobs/sec", NUM_JOBS as f64 / drain_time.as_secs_f64());

    // Verify exactly-once
    if counter == NUM_JOBS && done_count == NUM_JOBS as i64 {
        println!("\n✓ SUCCESS: Exactly-once guarantee verified (0 duplicates)");
        Ok(())
    } else {
        println!("\n✗ FAILURE: Guarantee broken!");
        println!("  Expected counter == {}, got {}", NUM_JOBS, counter);
        println!("  Expected done == {}, got {}", NUM_JOBS, done_count);
        Err("Stress test failed".into())
    }
}

async fn worker_loop(pool: &PgPool, worker_id: usize) {
    loop {
        if let Err(e) = worker_tick(pool, worker_id).await {
            eprintln!("Worker {} error: {}", worker_id, e);
            break;
        }

        // Check if all jobs are done
        if let Ok(pending) = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM rhino_jobs WHERE status IN ('pending', 'locked')",
        )
        .fetch_one(pool)
        .await
        {
            if pending == 0 {
                break;
            }
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn worker_tick(pool: &PgPool, worker_id: usize) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let pending_job = sqlx::query(
        "SELECT id::text AS id
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

    // Lock the job
    sqlx::query(
        "UPDATE rhino_jobs
         SET status = 'locked', locked_at = NOW(), locked_by = $2
         WHERE id::text = $1",
    )
    .bind(&id)
    .bind(format!("worker-{}", worker_id))
    .execute(&mut *tx)
    .await?;

    // Simulate job execution — increment counter
    sqlx::query("UPDATE stress_results SET counter = counter + 1")
        .execute(&mut *tx)
        .await?;

    // Mark as done
    sqlx::query(
        "UPDATE rhino_jobs
         SET status = 'done'
         WHERE id::text = $1",
    )
    .bind(&id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}
