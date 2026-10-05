//! Test harness utilities for pg_streaming integration tests
//!
//! Provides PostgreSQL connection helpers, SQL execution, and wait-for-processing
//! utilities. Requires pg_streaming + pg_kafka extensions installed via test.sh.

#![allow(dead_code)]

pub mod modbus_mock;

use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

/// Default PostgreSQL connection parameters for pgrx-managed instance
pub const PG_HOST: &str = "localhost";
pub fn pg_port() -> u16 {
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28818)
}
pub const PG_DB: &str = "pg_streaming";

/// Create a tokio runtime for async operations
fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

/// Check if the pg_streaming database is accessible
pub fn is_pg_running() -> bool {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
        tokio_postgres::connect(&conn_str, NoTls).await.is_ok()
    })
}

/// Execute SQL and return the first column of the first row as String
#[allow(clippy::manual_ok_err)]
pub fn query_one(sql: &str) -> Result<Option<String>, String> {
    runtime().block_on(async {
        let (client, connection) = connect().await?;
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
            return Ok(None);
        }

        let row = &rows[0];
        let val: Option<String> = if let Ok(v) = row.try_get::<_, i64>(0) {
            Some(v.to_string())
        } else if let Ok(v) = row.try_get::<_, i32>(0) {
            Some(v.to_string())
        } else if let Ok(v) = row.try_get::<_, f64>(0) {
            Some(v.to_string())
        } else if let Ok(v) = row.try_get::<_, bool>(0) {
            Some(v.to_string())
        } else if let Ok(v) = row.try_get::<_, String>(0) {
            Some(v)
        } else {
            None
        };
        Ok(val)
    })
}

/// Execute SQL and return all rows, each as a Vec<String> of column values
pub fn query_all(sql: &str) -> Result<Vec<Vec<String>>, String> {
    runtime().block_on(async {
        let (client, connection) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        let mut result = Vec::new();
        for row in &rows {
            let mut cols = Vec::new();
            for i in 0..row.len() {
                let val = if let Ok(v) = row.try_get::<_, i64>(i) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<_, i32>(i) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<_, f64>(i) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<_, bool>(i) {
                    v.to_string()
                } else if let Ok(v) = row.try_get::<_, String>(i) {
                    v
                } else {
                    "NULL".to_string()
                };
                cols.push(val);
            }
            result.push(cols);
        }
        Ok(result)
    })
}

/// Execute SQL without returning results
pub fn execute(sql: &str) -> Result<(), String> {
    runtime().block_on(async {
        let (client, connection) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        client.batch_execute(sql).await.map_err(format_sql_error)
    })
}

/// tokio_postgres' `Display` for a server error is just "db error" — the
/// actual message hangs off the `DbError`. Surface it so tests can assert on
/// what PostgreSQL actually said.
fn format_sql_error(e: tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => format!("SQL error: {}", db.message()),
        None => format!("SQL error: {}", e),
    }
}

/// Wait for a condition to become true, polling at intervals.
/// Returns Ok(()) if condition met within timeout, Err with description otherwise.
pub fn wait_for(
    description: &str,
    check_sql: &str,
    expected: &str,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);

    while start.elapsed() < timeout {
        if let Ok(Some(val)) = query_one(check_sql) {
            if val == expected {
                return Ok(());
            }
        }
        std::thread::sleep(poll_interval);
    }

    // Final check with actual value for error message
    let actual = query_one(check_sql)
        .unwrap_or(None)
        .unwrap_or_else(|| "NULL".to_string());
    Err(format!(
        "Timeout waiting for {}: expected '{}', got '{}' after {:?}",
        description, expected, actual, timeout
    ))
}

/// Wait for a row count to reach at least `min_count`
pub fn wait_for_row_count(table: &str, min_count: i64, timeout: Duration) -> Result<(), String> {
    let sql = format!("SELECT count(*)::bigint FROM {}", table);
    let start = Instant::now();
    let poll_interval = Duration::from_millis(500);

    while start.elapsed() < timeout {
        if let Ok(Some(val)) = query_one(&sql) {
            if let Ok(count) = val.parse::<i64>() {
                if count >= min_count {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(poll_interval);
    }

    let actual = query_one(&sql)
        .unwrap_or(None)
        .unwrap_or_else(|| "0".to_string());
    Err(format!(
        "Timeout waiting for {} to have >= {} rows, got {} after {:?}",
        table, min_count, actual, timeout
    ))
}

/// Cleanup helper: stop pipeline, drop pipeline, ignore errors
pub fn cleanup_pipeline(name: &str) {
    let _ = execute(&format!("SELECT pgstreams.stop('{}')", name));
    let _ = execute(&format!("SELECT pgstreams.drop_pipeline('{}')", name));
}

/// Ensure a plain (non source-backed) Kafka topic exists.
///
/// Lifecycle tests assert `state = 'running'` right after `pgstreams.start()`.
/// That races the engine: a pipeline whose Kafka topic does not exist fails to
/// initialize, and the executor legitimately flips it to 'failed' a second or
/// two later. Creating the topic first removes the race instead of papering
/// over it with a sleep.
pub fn ensure_topic(name: &str) {
    let _ = execute(&format!("SELECT pgkafka.create_topic('{}')", name));
}

/// Cleanup helper: drop a Kafka typed topic, ignore errors
pub fn cleanup_topic(name: &str) {
    let _ = execute(&format!("SELECT pgkafka.drop_topic('{}')", name));
    let _ = execute(&format!("DROP TABLE IF EXISTS {} CASCADE", name));
}

/// Cleanup helper: drop a table, ignore errors
pub fn cleanup_table(name: &str) {
    let _ = execute(&format!("DROP TABLE IF EXISTS {} CASCADE", name));
}

async fn connect() -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    String,
> {
    let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
    tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))
}

/// Macro to skip test if pg_streaming database is not running
#[macro_export]
macro_rules! skip_if_not_running {
    () => {
        if !$crate::common::is_pg_running() {
            eprintln!(
                "SKIPPED: pg_streaming database not running at {}:{}",
                $crate::common::PG_HOST,
                $crate::common::pg_port()
            );
            $crate::common::require_server();
            return;
        }
    };
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
