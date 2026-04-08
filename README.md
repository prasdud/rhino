# rhino
<img src="rhino-attack.webp" alt="rhino dude" height="400" style="width:1000px; object-fit:fill;" />

A durable, exactly-once job queue built in Rust.

# to-do
- move from postgres to sqlx for async
- enhance schema
worker stuff
    - status update after processing
    - payload column for job
    - retry logic, backoff with dlq
    - sleep when no pending jobs, rn hammers postgres
    - transaction integrity