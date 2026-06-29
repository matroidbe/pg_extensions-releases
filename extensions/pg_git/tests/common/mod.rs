//! Common test utilities for pg_git integration tests.

use std::time::Duration;
use tokio_postgres::{Client, NoTls};

/// Skip test if pg_git integration test database is not running.
macro_rules! skip_if_not_running {
    ($port:expr) => {{
        let url = format!(
            "host=localhost port={} user=postgres dbname=pg_git_test",
            $port
        );
        match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
            Ok(_) => {}
            Err(_) => {
                eprintln!(
                    "Skipping: pg_git test database not running on port {}",
                    $port
                );
                return;
            }
        }
    }};
}

pub(crate) use skip_if_not_running;

/// Connect to the test database.
pub async fn connect(port: u16) -> Client {
    let url = format!(
        "host=localhost port={} user=postgres dbname=pg_git_test",
        port
    );
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("Failed to connect to test database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    client
}

/// Poll until a condition is true or timeout.
pub async fn poll_until<F, Fut>(mut check: F, timeout: Duration, interval: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if check().await {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(interval).await;
    }
}
