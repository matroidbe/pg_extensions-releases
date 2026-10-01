//! Test harness utilities for pg_s3 integration tests
//!
//! Provides server readiness detection, graceful skipping, and SQL/HTTP execution helpers.

// Compiled separately into every test binary, so a helper used by only one of
// them reads as dead code while building the others.
#![allow(dead_code)]

use std::net::TcpStream;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

pub const S3_HOST: &str = "127.0.0.1";

/// The S3 server port, from `PG_S3_PORT` — the same variable `test.sh`
/// exports after choosing it and writing it into `postgresql.conf`.
///
/// Resolved at RUNTIME rather than fixed at 19100 because the port is global
/// to the machine: a test that dials a port another process owns does not
/// fail to connect, it connects to the WRONG SERVER and reports nonsense.
///
/// The default keeps a bare `cargo test` working against a hand-started PG.
pub fn s3_port() -> u16 {
    std::env::var("PG_S3_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(19100)
}

/// `host:port` for the server, resolved once per test binary.
pub fn s3_addr() -> &'static str {
    static ADDR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ADDR.get_or_init(|| format!("{}:{}", S3_HOST, s3_port()))
}

/// Default PostgreSQL connection parameters for pgrx-managed instance
pub const PG_HOST: &str = "localhost";
pub fn pg_port() -> u16 {
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28816)
}
pub const PG_DB: &str = "pg_s3";

/// Wait for TCP server to accept connections
pub fn wait_for_server(addr: &str, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!("Server at {} not ready after {:?}", addr, timeout))
}

/// Check if server is currently accepting connections
pub fn is_server_running(addr: &str) -> bool {
    TcpStream::connect(addr).is_ok()
}

/// Create a tokio runtime for async operations
fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

/// Execute SQL against the pgrx-managed PostgreSQL instance and return first column of first row
pub fn run_sql(sql: &str) -> Result<String, String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        if rows.is_empty() {
            Ok(String::new())
        } else {
            let row = &rows[0];
            let val: String = if let Ok(v) = row.try_get::<_, i64>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, i32>(0) {
                v.to_string()
            } else {
                row.try_get::<_, String>(0).unwrap_or_default()
            };
            Ok(val)
        }
    })
}

/// Execute SQL without returning results
pub fn execute_sql(sql: &str) -> Result<(), String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        client
            .execute(sql, &[])
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        Ok(())
    })
}

/// Base URL for the S3 HTTP server
pub fn s3_base_url() -> String {
    format!("http://{}", s3_addr())
}

/// Macro to skip test if server is not running
#[macro_export]
macro_rules! skip_if_no_server {
    ($addr:expr) => {
        if !$crate::common::is_server_running($addr) {
            eprintln!("SKIPPED: pg_s3 server not running at {}", $addr);
            return;
        }
    };
}
