//! Test harness utilities for pg_ortools integration tests
//!
//! Provides PostgreSQL connection helpers, SQL execution, and wait-for-processing
//! utilities. Requires pg_ortools extension installed via test.sh.

#![allow(dead_code)]

use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

/// Default PostgreSQL connection parameters for pgrx-managed instance
pub const PG_HOST: &str = "localhost";
pub fn pg_port() -> u16 {
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28816)
}
pub const PG_DB: &str = "pg_ortools";

/// Create a tokio runtime for async operations
fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

/// Check if the pg_ortools database is accessible
pub fn is_pg_running() -> bool {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
        tokio_postgres::connect(&conn_str, NoTls).await.is_ok()
    })
}

/// Execute SQL and return the first column of the first row as String
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

        client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))
    })
}

/// Wait for a condition to become true, polling at intervals.
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

    let actual = query_one(check_sql)
        .unwrap_or(None)
        .unwrap_or_else(|| "NULL".to_string());
    Err(format!(
        "Timeout waiting for {}: expected '{}', got '{}' after {:?}",
        description, expected, actual, timeout
    ))
}

/// Cleanup helper: drop a problem, ignore errors
pub fn cleanup_problem(name: &str) {
    let _ = execute(&format!("SELECT pgortools.drop_problem('{}')", name));
}

/// Cleanup helper: delete solve jobs for a problem
pub fn cleanup_jobs(problem_name: &str) {
    let _ = execute(&format!(
        "DELETE FROM pgortools.solve_jobs WHERE problem_name = '{}'",
        problem_name
    ));
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

/// Macro to skip test if pg_ortools database is not running
#[macro_export]
macro_rules! skip_if_not_running {
    () => {
        if !$crate::common::is_pg_running() {
            eprintln!(
                "SKIPPED: pg_ortools database not running at {}:{}",
                $crate::common::PG_HOST,
                $crate::common::pg_port()
            );
            return;
        }
    };
}
