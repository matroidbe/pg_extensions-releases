//! Task scheduler - claims and executes tasks using SKIP LOCKED
//!
//! Each scheduler instance runs as a background worker, polling for
//! pending tasks, executing them via their registered executor function,
//! and recording results.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::{GUC_ENABLED, GUC_POLL_INTERVAL};

/// Main scheduler worker loop.
///
/// Claims pending tasks via SKIP LOCKED, calls the executor function,
/// and stores the result. Runs until SIGTERM is received.
///
/// **Always-connected guarantee**: If a database operation fails, the worker
/// stops immediately. We do not retry on error - if the database is
/// unreachable, we are no longer part of the swarm. The background worker
/// restart mechanism (set_restart_time) handles reconnection.
pub fn scheduler_main() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    if !GUC_ENABLED.get() {
        log!("pg_swarm scheduler disabled via GUC, exiting");
        return;
    }

    // Wait for the node manager to register our node, then look up our node_id.
    // The node manager runs in the same postgres instance, registered by PID.
    let node_id = wait_for_node_id();

    log!("pg_swarm scheduler started for node_id={}", node_id);

    loop {
        if BackgroundWorker::sigterm_received() {
            log!("pg_swarm scheduler shutting down");
            break;
        }

        let poll_secs = GUC_POLL_INTERVAL.get() as u64;
        if !BackgroundWorker::wait_latch(Some(Duration::from_secs(poll_secs))) {
            continue;
        }

        BackgroundWorker::transaction(|| {
            claim_and_execute_task(node_id);
        });
    }
}

/// Wait for the node manager to register this node, then return the node_id.
/// Retries every second for up to 30 seconds.
fn wait_for_node_id() -> i64 {
    for _ in 0..30 {
        let result = BackgroundWorker::transaction(|| {
            Spi::get_one::<i64>(
                "SELECT id FROM pgswarm.nodes WHERE status = 'active' ORDER BY registered_at DESC LIMIT 1",
            )
        });

        if let Ok(Some(id)) = result {
            return id;
        }

        if BackgroundWorker::sigterm_received() {
            pgrx::error!("pg_swarm scheduler: shutting down before node registered");
        }

        BackgroundWorker::wait_latch(Some(Duration::from_secs(1)));
    }

    pgrx::error!("pg_swarm scheduler: timed out waiting for node registration");
}

/// Attempt to claim one pending task and execute it.
///
/// Supports two scheduling strategies:
/// - **greedy** (default): any worker grabs the next available task via SKIP LOCKED
/// - **round_robin**: workers only claim tasks assigned to their node
fn claim_and_execute_task(node_id: i64) {
    // Try round-robin first (tasks assigned to this node), then greedy (any unassigned task)
    let claimed = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.tasks
         SET status = 'running',
             node_id = $1,
             started_at = now()
         WHERE id = (
             SELECT t.id FROM pgswarm.tasks t
             JOIN pgswarm.jobs j ON j.id = t.job_id
             JOIN pgswarm.executors e ON e.name = j.executor_name
             WHERE t.status = 'pending'
               AND e.health_status != 'disabled'
               AND (t.assigned_node_id = $1 OR t.assigned_node_id IS NULL)
             ORDER BY
               CASE WHEN t.assigned_node_id = $1 THEN 0 ELSE 1 END,
               t.priority DESC, t.created_at ASC
             FOR UPDATE OF t SKIP LOCKED
             LIMIT 1
         )
         RETURNING id",
        &[node_id.into()],
    );

    let task_id = match claimed {
        Ok(Some(id)) => id,
        _ => return, // No pending tasks
    };

    // Update parent job to 'running' if it's still 'pending'
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.jobs
         SET status = 'running', started_at = COALESCE(started_at, now())
         WHERE id = (SELECT job_id FROM pgswarm.tasks WHERE id = $1)
           AND status = 'pending'",
        &[task_id.into()],
    );

    // Get task details + executor function name + executor name + result_table
    let task_info = Spi::connect(|client| {
        let row = client.select(
            "SELECT t.id, t.job_id, t.chunk_index, t.chunk_id, t.payload::text,
                    j.chunk_count, j.result_table, e.function_name, e.name AS executor_name
             FROM pgswarm.tasks t
             JOIN pgswarm.jobs j ON j.id = t.job_id
             JOIN pgswarm.executors e ON e.name = j.executor_name
             WHERE t.id = $1",
            None,
            &[task_id.into()],
        );

        if let Ok(table) = row {
            if let Some(row) = table.into_iter().next() {
                let chunk_index: i32 = row.get_by_name("chunk_index").unwrap().unwrap_or(0);
                let chunk_id: Option<String> = row.get_by_name("chunk_id").unwrap();
                let payload_str: String = row
                    .get_by_name("payload")
                    .unwrap()
                    .unwrap_or_else(|| "{}".to_string());
                let chunk_count: i32 = row.get_by_name("chunk_count").unwrap().unwrap_or(1);
                let function_name: String = row
                    .get_by_name("function_name")
                    .unwrap()
                    .unwrap_or_default();
                let executor_name: String = row
                    .get_by_name("executor_name")
                    .unwrap()
                    .unwrap_or_default();
                let result_table: Option<String> = row.get_by_name("result_table").unwrap();
                let job_id: i64 = row.get_by_name("job_id").unwrap().unwrap_or(0);
                return Some((
                    chunk_index,
                    chunk_id,
                    payload_str,
                    chunk_count,
                    function_name,
                    executor_name,
                    result_table,
                    job_id,
                ));
            }
        }
        None
    });

    let (
        chunk_index,
        chunk_id,
        payload_str,
        chunk_count,
        function_name,
        executor_name,
        result_table,
        job_id,
    ) = match task_info {
        Some(info) => info,
        None => {
            warning!(
                "pg_swarm: claimed task {} but could not read details",
                task_id
            );
            return;
        }
    };

    // Execute the task by calling the registered executor function
    let result = execute_task(
        task_id,
        &function_name,
        &payload_str,
        chunk_index,
        chunk_count,
    );

    match result {
        Ok(result_json) => {
            // Mark task completed with result
            let _ = Spi::run_with_args(
                "UPDATE pgswarm.tasks
                 SET status = 'completed',
                     result = $2::jsonb,
                     completed_at = now()
                 WHERE id = $1",
                &[task_id.into(), result_json.clone().into()],
            );

            // Auto-insert result into result_table if configured
            if let Some(ref rt) = result_table {
                let insert_query = format!(
                    "INSERT INTO {} (job_id, task_id, chunk_index, chunk_id, result, node_id)
                     VALUES ($1, $2, $3, $4, $5::jsonb, $6)",
                    rt
                );
                let _ = Spi::run_with_args(
                    &insert_query,
                    &[
                        job_id.into(),
                        task_id.into(),
                        chunk_index.into(),
                        chunk_id.clone().into(),
                        result_json.clone().into(),
                        node_id.into(),
                    ],
                );
            }

            // Update executor health: success resets consecutive failures
            let _ = Spi::run_with_args(
                "UPDATE pgswarm.executors
                 SET total_runs = total_runs + 1,
                     consecutive_failures = 0,
                     health_status = CASE
                         WHEN health_status = 'degraded' THEN 'healthy'
                         ELSE health_status
                     END
                 WHERE name = $1",
                &[executor_name.clone().into()],
            );
        }
        Err(error_msg) => {
            // Check retry count vs max_retries
            let _ = Spi::run_with_args(
                "UPDATE pgswarm.tasks
                 SET status = CASE
                         WHEN retry_count + 1 < max_retries THEN 'pending'
                         ELSE 'failed'
                     END,
                     retry_count = retry_count + 1,
                     node_id = NULL,
                     started_at = NULL,
                     error_message = $2,
                     completed_at = CASE
                         WHEN retry_count + 1 >= max_retries THEN now()
                         ELSE NULL
                     END
                 WHERE id = $1",
                &[task_id.into(), error_msg.clone().into()],
            );

            // Update executor health: track failure
            let _ = Spi::run_with_args(
                "UPDATE pgswarm.executors
                 SET total_runs = total_runs + 1,
                     total_failures = total_failures + 1,
                     consecutive_failures = consecutive_failures + 1,
                     last_failure_at = now(),
                     last_error = $2,
                     health_status = CASE
                         WHEN consecutive_failures + 1 >= max_consecutive_failures THEN 'disabled'
                         WHEN consecutive_failures + 1 >= max_consecutive_failures / 2 THEN 'degraded'
                         ELSE health_status
                     END
                 WHERE name = $1",
                &[
                    executor_name.clone().into(),
                    error_msg.into(),
                ],
            );
        }
    }

    // Update job status based on task states
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.jobs j
         SET status = CASE
                 WHEN NOT EXISTS (
                     SELECT 1 FROM pgswarm.tasks t
                     WHERE t.job_id = j.id AND t.status IN ('pending', 'running')
                 ) THEN
                     CASE
                         WHEN EXISTS (
                             SELECT 1 FROM pgswarm.tasks t
                             WHERE t.job_id = j.id AND t.status = 'failed'
                         ) THEN 'failed'
                         ELSE 'completed'
                     END
                 ELSE 'running'
             END,
             completed_at = CASE
                 WHEN NOT EXISTS (
                     SELECT 1 FROM pgswarm.tasks t
                     WHERE t.job_id = j.id AND t.status IN ('pending', 'running')
                 ) THEN now()
                 ELSE NULL
             END
         WHERE j.id = (SELECT job_id FROM pgswarm.tasks WHERE id = $1)",
        &[task_id.into()],
    );
}

/// Execute a task by calling its registered SQL executor function.
///
/// The executor function must accept (task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
/// and return JSONB.
fn execute_task(
    task_id: i64,
    function_name: &str,
    payload_str: &str,
    chunk_index: i32,
    chunk_count: i32,
) -> Result<String, String> {
    // Validate function name to prevent SQL injection
    if !is_valid_sql_identifier(function_name) {
        return Err(format!("Invalid executor function name: {}", function_name));
    }

    // Build the query to call the executor function
    // We use a parameterized approach for values, but function name must be an identifier
    let query = format!("SELECT {}($1, $2::jsonb, $3, $4)::text", function_name);

    let result = Spi::get_one_with_args::<String>(
        &query,
        &[
            task_id.into(),
            payload_str.to_string().into(),
            chunk_index.into(),
            chunk_count.into(),
        ],
    );

    match result {
        Ok(Some(json_str)) => Ok(json_str),
        Ok(None) => Ok("null".to_string()),
        Err(e) => Err(format!("{}", e)),
    }
}

/// Validate that a string is a safe SQL identifier (schema.function or just function).
/// Allows alphanumeric, underscore, and dot (for schema-qualified names).
fn is_valid_sql_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    // Allow schema-qualified names like "pgswarm.my_func"
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() > 2 {
        return false;
    }
    parts.iter().all(|part| {
        !part.is_empty()
            && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && part
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_sql_identifiers() {
        assert!(is_valid_sql_identifier("my_func"));
        assert!(is_valid_sql_identifier("pgswarm.my_func"));
        assert!(is_valid_sql_identifier("_private_func"));
        assert!(is_valid_sql_identifier("func123"));
    }

    #[test]
    fn test_invalid_sql_identifiers() {
        assert!(!is_valid_sql_identifier(""));
        assert!(!is_valid_sql_identifier("123func")); // starts with number
        assert!(!is_valid_sql_identifier("my-func")); // hyphen
        assert!(!is_valid_sql_identifier("my func")); // space
        assert!(!is_valid_sql_identifier("a.b.c")); // too many dots
        assert!(!is_valid_sql_identifier("func; DROP TABLE")); // injection
        assert!(!is_valid_sql_identifier(".func")); // leading dot
        assert!(!is_valid_sql_identifier("func.")); // trailing dot
    }
}
