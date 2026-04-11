use tokio::time::{sleep, Duration};

use rhino::worker::{daemon, init_db};

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
        match daemon(&pool, "worker-1").await {
            Ok(0) => sleep(Duration::from_millis(5)).await,
            Ok(_) => {}
            Err(e) => {
                eprintln!("Daemon error: {}", e);
                sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

