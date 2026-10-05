//! Common test utilities for pg_git integration tests.

use std::time::Duration;
use tokio_postgres::{Client, NoTls};

/// Skip test if pg_git integration test database is not running.
macro_rules! skip_if_not_running {
    ($port:expr) => {{
        let url = format!("host=localhost port={} dbname=pg_git_test", $port);
        match tokio_postgres::connect(&url, tokio_postgres::NoTls).await {
            Ok(_) => {}
            Err(_) => {
                eprintln!(
                    "Skipping: pg_git test database not running on port {}",
                    $port
                );
                $crate::common::require_server();
                return;
            }
        }
    }};
}

pub(crate) use skip_if_not_running;

/// Port of the pgrx-managed PostgreSQL (`PG_PORT`, default pgrx's pg18 port).
pub fn pg_port() -> u16 {
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28818)
}

/// Connect to the test database.
pub async fn connect(port: u16) -> Client {
    let url = format!("host=localhost port={} dbname=pg_git_test", port);
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

/// Under `test.sh` (`PG_TESTS_REQUIRE_SERVER=1`) a missing server fails the
/// test instead of skipping it: a server that died must not turn the suite
/// green.
pub fn require_server() {
    if std::env::var_os("PG_TESTS_REQUIRE_SERVER").is_some() {
        panic!(
            "server not running, but PG_TESTS_REQUIRE_SERVER is set (see the SKIPPED reason above)"
        );
    }
}
