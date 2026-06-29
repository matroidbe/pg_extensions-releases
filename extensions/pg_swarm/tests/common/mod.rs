//! Test harness utilities for pg_swarm integration tests

#![allow(dead_code)]

use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

pub const PG_HOST: &str = "localhost";
pub fn pg_port() -> u16 {
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(28816)
}
pub const PG_DB: &str = "postgres";

fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

pub fn is_pg_running() -> bool {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), PG_DB);
        tokio_postgres::connect(&conn_str, NoTls).await.is_ok()
    })
}

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

/// Cleanup: delete tasks, jobs, watches for an executor, then unregister
pub fn cleanup_executor(name: &str) {
    // Delete in FK order: tasks → jobs → watches → executor
    let _ = execute(&format!(
        "DELETE FROM pgswarm.tasks WHERE job_id IN (SELECT id FROM pgswarm.jobs WHERE executor_name = '{}')",
        name
    ));
    let _ = execute(&format!(
        "DELETE FROM pgswarm.jobs WHERE executor_name = '{}'",
        name
    ));
    let _ = execute(&format!(
        "DELETE FROM pgswarm.watches WHERE executor_name = '{}'",
        name
    ));
    let _ = execute(&format!(
        "DELETE FROM pgswarm.executors WHERE name = '{}'",
        name
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

#[macro_export]
macro_rules! skip_if_not_running {
    () => {
        if !$crate::common::is_pg_running() {
            eprintln!(
                "SKIPPED: pg_swarm database not running at {}:{}",
                $crate::common::PG_HOST,
                $crate::common::pg_port()
            );
            return;
        }
    };
}
