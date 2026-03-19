//! Test harness utilities for pg_kafka integration tests
//!
//! Provides server readiness detection, graceful skipping, and SQL execution helpers.

use std::net::TcpStream;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

/// Default Kafka server address
pub const KAFKA_ADDR: &str = "127.0.0.1:9092";

/// Default PostgreSQL connection parameters for pgrx-managed instance
pub const PG_HOST: &str = "localhost";
pub const PG_PORT: u16 = 28816;
pub const PG_DB: &str = "pg_kafka";

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

/// Execute SQL against the pgrx-managed PostgreSQL instance
pub fn run_sql(sql: &str) -> Result<String, String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, PG_PORT, PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        // Spawn the connection handler
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
            // Return first column of first row as string
            // Try different types since PostgreSQL may return int, bigint, or text
            let row = &rows[0];
            let val: String = if let Ok(v) = row.try_get::<_, i64>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, i32>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, String>(0) {
                v
            } else {
                String::new()
            };
            Ok(val)
        }
    })
}

/// Execute SQL without returning results
pub fn execute_sql(sql: &str) -> Result<(), String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, PG_PORT, PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        // Spawn the connection handler
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

/// Ensure a topic exists and is writable, creating it if necessary
pub fn ensure_topic(topic_name: &str) -> Result<(), String> {
    // Try to create the topic (ignore error if already exists)
    let create_sql = format!("SELECT pgkafka.create_topic('{}');", topic_name);
    let _ = run_sql(&create_sql);

    // Ensure the topic is writable for tests
    let writable_sql = format!(
        "UPDATE pgkafka.topics SET writable = true WHERE name = '{}';",
        topic_name
    );
    let _ = execute_sql(&writable_sql);

    Ok(())
}

/// Macro to skip test if server is not running
#[macro_export]
macro_rules! skip_if_no_server {
    ($addr:expr) => {
        if !$crate::common::is_server_running($addr) {
            eprintln!("SKIPPED: pg_kafka server not running at {}", $addr);
            return;
        }
    };
}
