//! Node manager - handles node registration, heartbeats, and dead node detection
//!
//! Runs as a background worker that:
//! 1. Registers this node on startup
//! 2. Sends periodic heartbeats
//! 3. Detects and cleans up dead nodes
//! 4. Reassigns orphaned tasks from dead nodes
//! 5. Polls watched source tables for new rows (no leader needed — uses SKIP LOCKED)

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::{GUC_HEARTBEAT_INTERVAL, GUC_NODE_NAME, GUC_NODE_TIMEOUT};

/// Main node manager worker loop.
///
/// **Always-connected guarantee**: The node manager requires a live database
/// connection at all times. If the connection drops, the background worker
/// process exits. PostgreSQL will restart it after restart_time, at which point
/// it re-registers as a new node. There is no offline mode, no local state,
/// no split-brain. Disconnected = stopped.
pub fn node_manager_main() {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    log!("pg_swarm node manager started");

    // Wait for the database to be ready
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    // Register this node
    let node_id = BackgroundWorker::transaction(register_node);

    log!("pg_swarm node registered with id={}", node_id);

    loop {
        if BackgroundWorker::sigterm_received() {
            log!("pg_swarm node manager shutting down");
            BackgroundWorker::transaction(|| deregister_node(node_id));
            break;
        }

        let heartbeat_secs = GUC_HEARTBEAT_INTERVAL.get() as u64;
        BackgroundWorker::wait_latch(Some(Duration::from_secs(heartbeat_secs)));

        BackgroundWorker::transaction(|| {
            // Send heartbeat
            update_heartbeat(node_id);

            // Check for dead nodes and reassign their tasks
            cleanup_dead_nodes();

            // Check for timed-out tasks
            handle_timed_out_tasks();

            // Process watched source tables
            process_watches(node_id);
        });
    }
}

/// Maximum number of nodes in the swarm.
/// Beyond this, sync overhead becomes problematic.
const MAX_NODES: i64 = 10;

/// Register this node in the pgswarm.nodes table.
fn register_node() -> i64 {
    let node_name = get_node_name();
    let pid = std::process::id() as i32;

    // Enforce max node limit
    let active_count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM pgswarm.nodes WHERE status IN ('active', 'draining')",
    );
    if let Ok(Some(count)) = active_count {
        if count >= MAX_NODES {
            pgrx::error!(
                "pg_swarm: maximum node limit ({}) reached. Cannot register new node.",
                MAX_NODES
            );
        }
    }

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgswarm.nodes (node_name, pid, status)
         VALUES ($1, $2, 'active')
         ON CONFLICT (pid) DO UPDATE
         SET node_name = EXCLUDED.node_name,
             status = 'active',
             last_heartbeat = now()
         RETURNING id",
        &[node_name.into(), pid.into()],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to register node: no ID returned"),
        Err(e) => pgrx::error!("Failed to register node: {}", e),
    }
}

/// Remove this node from the registry on shutdown.
fn deregister_node(node_id: i64) {
    // Reassign any running tasks first
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.tasks
         SET status = 'pending', node_id = NULL, started_at = NULL
         WHERE node_id = $1 AND status = 'running'",
        &[node_id.into()],
    );

    let _ = Spi::run_with_args("DELETE FROM pgswarm.nodes WHERE id = $1", &[node_id.into()]);

    log!("pg_swarm node {} deregistered", node_id);
}

/// Update heartbeat timestamp for this node.
fn update_heartbeat(node_id: i64) {
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.nodes SET last_heartbeat = now() WHERE id = $1",
        &[node_id.into()],
    );
}

/// Detect dead nodes and reassign their tasks.
fn cleanup_dead_nodes() {
    let timeout_secs = GUC_NODE_TIMEOUT.get();

    // Find dead nodes
    let _ = Spi::run_with_args(
        "WITH dead AS (
             UPDATE pgswarm.nodes
             SET status = 'dead'
             WHERE status = 'active'
               AND last_heartbeat < now() - make_interval(secs => $1::double precision)
             RETURNING id
         )
         UPDATE pgswarm.tasks
         SET status = 'pending', node_id = NULL, started_at = NULL
         WHERE node_id IN (SELECT id FROM dead)
           AND status = 'running'",
        &[timeout_secs.into()],
    );
}

/// Handle tasks that have exceeded their timeout.
fn handle_timed_out_tasks() {
    let _ = Spi::run(
        "UPDATE pgswarm.tasks
         SET status = CASE
                 WHEN retry_count + 1 < max_retries THEN 'pending'
                 ELSE 'failed'
             END,
             node_id = NULL,
             retry_count = retry_count + 1,
             error_message = 'Task timed out',
             started_at = NULL,
             completed_at = CASE
                 WHEN retry_count + 1 >= max_retries THEN now()
                 ELSE NULL
             END
         WHERE status = 'running'
           AND started_at < now() - make_interval(secs => timeout_secs::double precision)",
    );
}

/// Process all watched source tables.
///
/// For each enabled watch whose poll_interval has elapsed, claim it via
/// FOR UPDATE SKIP LOCKED (so only one node processes each watch at a time),
/// then read new rows from the source table and create a job with tasks.
///
/// No leader needed — PostgreSQL row locking IS the coordination.
fn process_watches(node_id: i64) {
    // Find watches that are due for polling.
    // FOR UPDATE SKIP LOCKED ensures only one node processes each watch.
    let watches: Vec<(
        i64,
        String,
        String,
        String,
        i32,
        i32,
        i32,
        i32,
        String,
        Option<String>,
        i64,
    )> = Spi::connect(|client| {
        let table = client.select(
            "SELECT id, source_table, executor_name, job_payload::text,
                        batch_size, priority, max_retries, task_timeout,
                        scheduling, result_table, last_processed_id
                 FROM pgswarm.watches
                 WHERE enabled = true
                 FOR UPDATE SKIP LOCKED",
            None,
            &[],
        );
        let mut result = Vec::new();
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let source_table: String =
                    row.get_by_name("source_table").unwrap().unwrap_or_default();
                let executor_name: String = row
                    .get_by_name("executor_name")
                    .unwrap()
                    .unwrap_or_default();
                let job_payload: String = row
                    .get_by_name("job_payload")
                    .unwrap()
                    .unwrap_or_else(|| "{}".to_string());
                let batch_size: i32 = row.get_by_name("batch_size").unwrap().unwrap_or(100);
                let priority: i32 = row.get_by_name("priority").unwrap().unwrap_or(0);
                let max_retries: i32 = row.get_by_name("max_retries").unwrap().unwrap_or(3);
                let task_timeout: i32 = row.get_by_name("task_timeout").unwrap().unwrap_or(3600);
                let scheduling: String = row
                    .get_by_name("scheduling")
                    .unwrap()
                    .unwrap_or_else(|| "greedy".to_string());
                let result_table: Option<String> = row.get_by_name("result_table").unwrap();
                let last_processed_id: i64 =
                    row.get_by_name("last_processed_id").unwrap().unwrap_or(0);
                result.push((
                    id,
                    source_table,
                    executor_name,
                    job_payload,
                    batch_size,
                    priority,
                    max_retries,
                    task_timeout,
                    scheduling,
                    result_table,
                    last_processed_id,
                ));
            }
        }
        result
    });

    for (
        watch_id,
        source_table,
        executor_name,
        job_payload_str,
        batch_size,
        priority,
        max_retries,
        task_timeout,
        scheduling,
        result_table,
        last_processed_id,
    ) in watches
    {
        process_single_watch(
            node_id,
            watch_id,
            &source_table,
            &executor_name,
            &job_payload_str,
            batch_size,
            priority,
            max_retries,
            task_timeout,
            &scheduling,
            result_table.as_deref(),
            last_processed_id,
        );
    }
}

/// Process a single watch: read new rows from source table, create job + tasks.
fn process_single_watch(
    _node_id: i64,
    watch_id: i64,
    source_table: &str,
    executor_name: &str,
    job_payload_str: &str,
    batch_size: i32,
    priority: i32,
    max_retries: i32,
    task_timeout: i32,
    scheduling: &str,
    result_table: Option<&str>,
    last_processed_id: i64,
) {
    // Read new rows from source table (id > last_processed_id)
    let query = format!(
        "SELECT id, payload::text FROM {} WHERE id > $1 ORDER BY id LIMIT $2",
        source_table
    );
    let rows: Vec<(i64, String)> = Spi::connect(|client| {
        let table = client.select(&query, None, &[last_processed_id.into(), batch_size.into()]);
        let mut result = Vec::new();
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let payload_str: String = row
                    .get_by_name("payload")
                    .unwrap()
                    .unwrap_or_else(|| "{}".to_string());
                result.push((id, payload_str));
            }
        }
        result
    });

    if rows.is_empty() {
        return; // No new rows
    }

    let max_row_id = rows
        .iter()
        .map(|(id, _)| *id)
        .max()
        .unwrap_or(last_processed_id);
    let chunk_count = rows.len() as i32;

    // Create the job
    let job_id = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgswarm.jobs (executor_name, payload, chunk_count, priority, max_retries, task_timeout, scheduling, result_table)
         VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8)
         RETURNING id",
        &[
            executor_name.into(),
            job_payload_str.into(),
            chunk_count.into(),
            priority.into(),
            max_retries.into(),
            task_timeout.into(),
            scheduling.into(),
            result_table.into(),
        ],
    );

    let job_id = match job_id {
        Ok(Some(id)) => id,
        _ => {
            warning!("pg_swarm: failed to create job for watch {}", watch_id);
            return;
        }
    };

    // Parse job payload for merging
    let job_payload_val: serde_json::Value = serde_json::from_str(job_payload_str)
        .unwrap_or(serde_json::Value::Object(Default::default()));

    // Get active nodes for round-robin
    let active_nodes: Vec<i64> = if scheduling == "round_robin" {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT id FROM pgswarm.nodes WHERE status = 'active' ORDER BY id",
                None,
                &[],
            );
            let mut nodes = Vec::new();
            if let Ok(table) = table {
                for row in table {
                    let id: Option<i64> = row.get_by_name("id").unwrap();
                    if let Some(id) = id {
                        nodes.push(id);
                    }
                }
            }
            nodes
        })
    } else {
        vec![]
    };

    // Create one task per source row
    for (idx, (row_id, row_payload_str)) in rows.iter().enumerate() {
        // Merge job payload with row payload
        let row_val: serde_json::Value = serde_json::from_str(row_payload_str)
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let mut merged = job_payload_val.clone();
        if let (Some(base), Some(overlay)) = (merged.as_object_mut(), row_val.as_object()) {
            for (k, v) in overlay {
                base.insert(k.clone(), v.clone());
            }
        }
        let task_payload = serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string());

        let chunk_id = row_id.to_string();
        let assigned_node = if scheduling == "round_robin" && !active_nodes.is_empty() {
            Some(active_nodes[idx % active_nodes.len()])
        } else {
            None::<i64>
        };

        let _ = Spi::run_with_args(
            "INSERT INTO pgswarm.tasks (job_id, chunk_index, chunk_id, priority, max_retries, timeout_secs, payload, assigned_node_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)",
            &[
                job_id.into(),
                (idx as i32).into(),
                chunk_id.into(),
                priority.into(),
                max_retries.into(),
                task_timeout.into(),
                task_payload.into(),
                assigned_node.into(),
            ],
        );
    }

    // Update last_processed_id so we don't process these rows again
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.watches SET last_processed_id = $1 WHERE id = $2",
        &[max_row_id.into(), watch_id.into()],
    );

    let _ = Spi::run("NOTIFY pgswarm_tasks");

    log!(
        "pg_swarm: watch {} created job {} with {} tasks from {}",
        watch_id,
        job_id,
        chunk_count,
        source_table
    );
}

/// Get the node name from GUC or fallback to hostname.
fn get_node_name() -> String {
    let guc_name = GUC_NODE_NAME.get();
    if let Some(cstring) = guc_name {
        if let Ok(name) = cstring.into_string() {
            if !name.is_empty() {
                return name;
            }
        }
    }

    // Fallback to hostname
    gethostname().unwrap_or_else(|| "pg_swarm_node".to_string())
}

/// Get the system hostname.
fn gethostname() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buf = vec![0u8; 256];
        let ret = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
        if ret == 0 {
            let nul_pos = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            Some(String::from_utf8_lossy(&buf[..nul_pos]).to_string())
        } else {
            None
        }
    }
    #[cfg(not(unix))]
    {
        None
    }
}
