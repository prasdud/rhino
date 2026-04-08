use sqlx::PgPool;

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

async fn main() {
    let pool = PgPool::connect(DB_URL)
    .await
    .unwrap();
    let mut client = match init_db() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Database initialization failed: {}", e);
            return;
        }
    };
    println!("Rhino Daemon is running...");
    loop {
        if let Err(e) = daemon(&mut client) {
            eprintln!("Daemon error: {}", e);
            break;
        }
    }
    // Here you would add your job processing loop
}


fn daemon(client: &mut Client) -> Result<(), Error> {
    println!("Starting Rhino Daemon...");
    let pending_job = client.query(
        "SELECT * FROM jobs 
        WHERE status = 'pending' 
        ORDER BY id 
        LIMIT 1
        FOR UPDATE SKIP LOCKED",
        &[]
    )?;
    Ok(
        if pending_job.is_empty() {
            println!("No pending jobs found.");
        } else {
            let job = &pending_job[0];
            println!("Processing job: {:?}", job);
            // Here you would add your job processing logic
        }
    )
}

fn init_db() -> Result<Client, Error> {
    let mut client = Client::connect("postgresql://rhino:rhino@localhost:5445/rhino_db", NoTls)?;
    client.batch_execute("
        CREATE TABLE IF NOT EXISTS jobs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_type        TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending'
        )
    ")?;
    Ok(client)
}

