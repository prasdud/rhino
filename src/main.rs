use postgres::{Client, NoTls, Error};

fn main() -> Result<(), Error> {
    let mut client = Client::connect("postgresql://rhino:rhino@localhost:5445/rhino_db", NoTls)?;
    
    let res = client.batch_execute("
        CREATE TABLE IF NOT EXISTS jobs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            job_type        TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'pending'
        )
    ");
    match res {
        Ok(_) => {
            let mut i = 0;
            loop {
                i += 1;
                println!("Waiting for jobs... {}", i);
            }
        },
        Err(e) => {
            eprintln!("Database error: {}", e);
            return Err(e);
        }
    }
}