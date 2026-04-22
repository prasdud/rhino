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

