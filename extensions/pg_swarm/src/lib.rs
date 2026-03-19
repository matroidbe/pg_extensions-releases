//! pg_swarm - Distributed task processing swarm for PostgreSQL
//!
//! Brings Docker Swarm-style distributed task processing to PostgreSQL.
//! Large workloads are divided into tasks, distributed across worker nodes
//! using SKIP LOCKED, and coordinated entirely through standard PostgreSQL
//! tables and primitives.
//!
//! ## Architecture: Always Connected, Always Atomic
//!
//! pg_swarm is database-driven, not network-driven. Every operation is an
//! ACID transaction against the shared PostgreSQL database.
//!
//! - **No eventual consistency**: All state is transactional
//! - **No split-brain**: Disconnected nodes stop immediately (fail-stop)
//! - **No master**: Every connected node is an equal peer
//! - **No offline mode**: If you can't reach the database, you're not in the swarm
//!
//! This is a deliberate design choice. The database IS the swarm.

#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::ffi::CString;
use std::time::Duration;

mod node;
mod scheduler;
#[allow(dead_code)]
mod types;

pgrx::pg_module_magic!();

// ─── GUC Settings ───────────────────────────────────────────────────────────

static GUC_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);
static GUC_WORKERS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(2);
static GUC_HEARTBEAT_INTERVAL: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(10);
static GUC_NODE_TIMEOUT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(30);
static GUC_TASK_TIMEOUT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(3600);
static GUC_POLL_INTERVAL: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1);
static GUC_MAX_RETRIES: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(3);
static GUC_NODE_NAME: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

// ─── Schema Bootstrap ───────────────────────────────────────────────────────

pgrx::extension_sql!(
    r#"
-- Executor registry with health monitoring
CREATE TABLE pgswarm.executors (
    id                    BIGSERIAL PRIMARY KEY,
    name                  TEXT NOT NULL UNIQUE,
    function_name         TEXT NOT NULL,
    description           TEXT,
    total_runs            BIGINT NOT NULL DEFAULT 0,
    total_failures        BIGINT NOT NULL DEFAULT 0,
    consecutive_failures  INT NOT NULL DEFAULT 0,
    max_consecutive_failures INT NOT NULL DEFAULT 5,
    last_failure_at       TIMESTAMPTZ,
    last_error            TEXT,
    health_status         TEXT NOT NULL DEFAULT 'healthy'
                          CHECK (health_status IN ('healthy', 'degraded', 'disabled')),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Node registry
CREATE TABLE pgswarm.nodes (
    id              BIGSERIAL PRIMARY KEY,
    node_name       TEXT NOT NULL,
    pid             INT NOT NULL,
    workers_count   INT NOT NULL DEFAULT 2,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'draining', 'dead')),
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata        JSONB DEFAULT '{}'
);

CREATE INDEX idx_nodes_heartbeat ON pgswarm.nodes (last_heartbeat);
CREATE UNIQUE INDEX idx_nodes_pid ON pgswarm.nodes (pid);

-- Jobs: high-level units of work
CREATE TABLE pgswarm.jobs (
    id              BIGSERIAL PRIMARY KEY,
    executor_name   TEXT NOT NULL REFERENCES pgswarm.executors(name),
    payload         JSONB NOT NULL DEFAULT '{}',
    chunk_count     INT NOT NULL DEFAULT 1,
    priority        INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    scheduling      TEXT NOT NULL DEFAULT 'greedy'
                    CHECK (scheduling IN ('greedy', 'round_robin')),
    result_table    TEXT,
    max_retries     INT NOT NULL DEFAULT 3,
    task_timeout    INT NOT NULL DEFAULT 3600,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_by      TEXT NOT NULL DEFAULT current_user,
    metadata        JSONB DEFAULT '{}'
);

CREATE INDEX idx_jobs_status ON pgswarm.jobs (status);
CREATE INDEX idx_jobs_created ON pgswarm.jobs (created_at);

-- Tasks: individual units of work (one per chunk per job)
CREATE TABLE pgswarm.tasks (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES pgswarm.jobs(id) ON DELETE CASCADE,
    chunk_index     INT NOT NULL,
    chunk_id        TEXT,
    priority        INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    node_id         BIGINT REFERENCES pgswarm.nodes(id) ON DELETE SET NULL,
    assigned_node_id BIGINT REFERENCES pgswarm.nodes(id) ON DELETE SET NULL,
    retry_count     INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    timeout_secs    INT NOT NULL DEFAULT 3600,
    payload         JSONB NOT NULL DEFAULT '{}',
    result          JSONB,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_status ON pgswarm.tasks (status);
CREATE INDEX idx_tasks_job_id ON pgswarm.tasks (job_id);
CREATE INDEX idx_tasks_node_id ON pgswarm.tasks (node_id) WHERE node_id IS NOT NULL;
CREATE INDEX idx_tasks_pending ON pgswarm.tasks (priority DESC, created_at ASC)
    WHERE status = 'pending';

-- Aggressive autovacuum for high-churn task table
ALTER TABLE pgswarm.tasks SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

-- Watches: continuous source table monitoring
-- Links a source table to an executor for automatic task creation.
-- The node manager polls watched tables and creates jobs from new rows.
CREATE TABLE pgswarm.watches (
    id              BIGSERIAL PRIMARY KEY,
    source_table    TEXT NOT NULL,
    executor_name   TEXT NOT NULL REFERENCES pgswarm.executors(name),
    job_payload     JSONB NOT NULL DEFAULT '{}',
    batch_size      INT NOT NULL DEFAULT 100,
    poll_interval   INT NOT NULL DEFAULT 5,
    priority        INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    task_timeout    INT NOT NULL DEFAULT 3600,
    scheduling      TEXT NOT NULL DEFAULT 'greedy'
                    CHECK (scheduling IN ('greedy', 'round_robin')),
    result_table    TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    last_processed_id BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(source_table, executor_name)
);

-- Job progress view
CREATE VIEW pgswarm.job_progress AS
SELECT
    j.id AS job_id,
    j.executor_name,
    j.status AS job_status,
    j.chunk_count AS total_tasks,
    COUNT(*) FILTER (WHERE t.status = 'completed') AS completed,
    COUNT(*) FILTER (WHERE t.status = 'failed') AS failed,
    COUNT(*) FILTER (WHERE t.status = 'running') AS running,
    COUNT(*) FILTER (WHERE t.status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE t.status = 'cancelled') AS cancelled,
    j.created_at,
    j.started_at,
    j.completed_at,
    CASE WHEN j.completed_at IS NOT NULL
         THEN j.completed_at - j.started_at
         WHEN j.started_at IS NOT NULL
         THEN now() - j.started_at
         ELSE NULL
    END AS duration
FROM pgswarm.jobs j
LEFT JOIN pgswarm.tasks t ON t.job_id = j.id
GROUP BY j.id;

-- Node status view
CREATE VIEW pgswarm.node_status AS
SELECT
    n.id AS node_id,
    n.node_name,
    n.pid,
    n.status,
    n.workers_count,
    n.last_heartbeat,
    now() - n.last_heartbeat AS time_since_heartbeat,
    COUNT(t.id) FILTER (WHERE t.status = 'running') AS active_tasks,
    COUNT(t.id) FILTER (WHERE t.status = 'completed') AS completed_tasks,
    COUNT(t.id) FILTER (WHERE t.status = 'failed') AS failed_tasks
FROM pgswarm.nodes n
LEFT JOIN pgswarm.tasks t ON t.node_id = n.id
GROUP BY n.id;

-- Executor health view
CREATE VIEW pgswarm.executor_health AS
SELECT
    e.id,
    e.name,
    e.function_name,
    e.health_status,
    e.total_runs,
    e.total_failures,
    e.consecutive_failures,
    e.max_consecutive_failures,
    CASE WHEN e.total_runs > 0
         THEN round((e.total_failures::numeric / e.total_runs::numeric) * 100, 1)
         ELSE 0
    END AS failure_rate_pct,
    e.last_failure_at,
    e.last_error,
    e.created_at
FROM pgswarm.executors e
ORDER BY e.consecutive_failures DESC;
"#,
    name = "bootstrap_schema",
    bootstrap
);

// ─── Extension Init ─────────────────────────────────────────────────────────

/// Extension initialization - register GUCs and background workers.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_bool_guc(
        c"pg_swarm.enabled",
        c"Enable pg_swarm background workers",
        c"When false, no background workers are started",
        &GUC_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.workers",
        c"Number of scheduler workers per node",
        c"Each worker can process one task at a time",
        &GUC_WORKERS,
        1,
        32,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.heartbeat_interval",
        c"Heartbeat interval in seconds",
        c"How often the node manager sends heartbeats",
        &GUC_HEARTBEAT_INTERVAL,
        1,
        300,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.node_timeout",
        c"Node timeout in seconds",
        c"Seconds without heartbeat before a node is considered dead",
        &GUC_NODE_TIMEOUT,
        5,
        600,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.task_timeout",
        c"Default task timeout in seconds",
        c"Maximum time a task can run before being considered timed out",
        &GUC_TASK_TIMEOUT,
        10,
        86400,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.poll_interval",
        c"Task poll interval in seconds",
        c"How often scheduler workers check for new tasks",
        &GUC_POLL_INTERVAL,
        1,
        60,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_swarm.max_retries",
        c"Default max retries for failed tasks",
        c"Number of times a failed task will be retried before being marked as permanently failed",
        &GUC_MAX_RETRIES,
        0,
        100,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_swarm.node_name",
        c"Human-readable node name",
        c"If empty, the system hostname is used",
        &GUC_NODE_NAME,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Register the node manager background worker
    BackgroundWorkerBuilder::new("pg_swarm node manager")
        .set_function("pg_swarm_node_manager")
        .set_library("pg_swarm")
        .set_argument(0i32.into_datum())
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();

    // Register the scheduler background worker
    BackgroundWorkerBuilder::new("pg_swarm scheduler")
        .set_function("pg_swarm_scheduler")
        .set_library("pg_swarm")
        .set_argument(0i32.into_datum())
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Node manager background worker entry point.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_swarm_node_manager(_arg: pg_sys::Datum) {
    node::node_manager_main();
}

/// Scheduler background worker entry point.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_swarm_scheduler(_arg: pg_sys::Datum) {
    scheduler::scheduler_main();
}

// ─── Executor Management ────────────────────────────────────────────────────

/// Register a SQL function as a task executor.
///
/// The function must accept (task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
/// and return JSONB.
#[pg_extern]
fn register_executor(
    name: &str,
    function_name: &str,
    description: default!(Option<&str>, "NULL"),
) -> i64 {
    if name.is_empty() {
        pgrx::error!("Executor name cannot be empty");
    }
    if function_name.is_empty() {
        pgrx::error!("Function name cannot be empty");
    }

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgswarm.executors (name, function_name, description)
         VALUES ($1, $2, $3)
         RETURNING id",
        &[name.into(), function_name.into(), description.into()],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to register executor: no ID returned"),
        Err(e) => pgrx::error!("Failed to register executor: {}", e),
    }
}

/// Unregister an executor by name.
#[pg_extern]
fn unregister_executor(name: &str) -> bool {
    let result = Spi::get_one_with_args::<i64>(
        "DELETE FROM pgswarm.executors WHERE name = $1 RETURNING id",
        &[name.into()],
    );

    matches!(result, Ok(Some(_)))
}

/// List all registered executors.
#[pg_extern]
fn list_executors() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(name, String),
        name!(function_name, String),
        name!(description, Option<String>),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, name, function_name, description FROM pgswarm.executors ORDER BY name",
            None,
            &[],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let name: String = row.get_by_name("name").unwrap().unwrap_or_default();
                let function_name: String = row
                    .get_by_name("function_name")
                    .unwrap()
                    .unwrap_or_default();
                let description: Option<String> = row.get_by_name("description").unwrap();
                results.push((id, name, function_name, description));
            }
        }
    });

    TableIterator::new(results)
}

// ─── Job Management ─────────────────────────────────────────────────────────

/// Submit a new job for distributed processing.
///
/// Creates the job and `chunk_count` tasks in a single transaction.
/// Each task receives the same payload plus its chunk_index.
///
/// scheduling: 'greedy' (default, SKIP LOCKED) or 'round_robin' (distribute evenly across nodes)
#[pg_extern]
fn submit_job(
    executor_name: &str,
    payload: default!(pgrx::JsonB, "'{}'"),
    chunk_count: default!(i32, 1),
    priority: default!(i32, 0),
    max_retries: default!(i32, 3),
    task_timeout: default!(i32, 3600),
    scheduling: default!(&str, "'greedy'"),
    result_table: default!(Option<&str>, "NULL"),
) -> i64 {
    if chunk_count < 1 {
        pgrx::error!("chunk_count must be at least 1");
    }
    if scheduling != "greedy" && scheduling != "round_robin" {
        pgrx::error!("scheduling must be 'greedy' or 'round_robin'");
    }

    validate_executor(executor_name);

    // Validate result_table name if provided
    if let Some(rt) = result_table {
        validate_table_name(rt);
    }

    let payload_str = serde_json::to_string(&payload.0).unwrap_or_else(|_| "{}".to_string());

    let job_id = insert_job(
        executor_name,
        &payload_str,
        chunk_count,
        priority,
        max_retries,
        task_timeout,
        scheduling,
        result_table,
    );

    // Create result table if specified
    if let Some(rt) = result_table {
        ensure_result_table(rt);
    }

    // Get active nodes for round-robin assignment
    let active_nodes = if scheduling == "round_robin" {
        get_active_node_ids()
    } else {
        vec![]
    };

    if scheduling == "round_robin" && active_nodes.is_empty() {
        pgrx::warning!("No active nodes for round_robin scheduling, tasks will be unassigned");
    }

    // Create tasks for each chunk
    for chunk_index in 0..chunk_count {
        let assigned_node = if scheduling == "round_robin" && !active_nodes.is_empty() {
            Some(active_nodes[chunk_index as usize % active_nodes.len()])
        } else {
            None
        };

        insert_task(
            job_id,
            chunk_index,
            None, // no chunk_id for simple submit
            priority,
            max_retries,
            task_timeout,
            &payload_str,
            assigned_node,
        );
    }

    let _ = Spi::run("NOTIFY pgswarm_tasks");
    job_id
}

/// Submit a job with explicit per-chunk payloads.
///
/// Each element in `chunks` is a JSONB object with:
/// - "chunk_id" (optional TEXT): human-readable identifier (e.g., "frames_0_100")
/// - "payload" (optional JSONB): chunk-specific payload merged with job payload
///
/// Example:
/// ```sql
/// SELECT pgswarm.submit_job_with_chunks(
///     'process_video',
///     '[
///         {"chunk_id": "frames_0_999",   "payload": {"start_frame": 0,    "end_frame": 999}},
///         {"chunk_id": "frames_1000_1999", "payload": {"start_frame": 1000, "end_frame": 1999}},
///         {"chunk_id": "frames_2000_2999", "payload": {"start_frame": 2000, "end_frame": 2999}}
///     ]'::jsonb,
///     '{"video": "input.mp4"}'::jsonb
/// );
/// ```
#[pg_extern]
fn submit_job_with_chunks(
    executor_name: &str,
    chunks: pgrx::JsonB,
    job_payload: default!(pgrx::JsonB, "'{}'"),
    priority: default!(i32, 0),
    max_retries: default!(i32, 3),
    task_timeout: default!(i32, 3600),
    scheduling: default!(&str, "'greedy'"),
    result_table: default!(Option<&str>, "NULL"),
) -> i64 {
    if scheduling != "greedy" && scheduling != "round_robin" {
        pgrx::error!("scheduling must be 'greedy' or 'round_robin'");
    }

    let chunk_array = match chunks.0.as_array() {
        Some(arr) => arr.clone(),
        None => pgrx::error!("chunks must be a JSON array"),
    };

    if chunk_array.is_empty() {
        pgrx::error!("chunks array must not be empty");
    }

    validate_executor(executor_name);

    // Validate result_table name if provided
    if let Some(rt) = result_table {
        validate_table_name(rt);
    }

    let job_payload_str =
        serde_json::to_string(&job_payload.0).unwrap_or_else(|_| "{}".to_string());
    let chunk_count = chunk_array.len() as i32;

    let job_id = insert_job(
        executor_name,
        &job_payload_str,
        chunk_count,
        priority,
        max_retries,
        task_timeout,
        scheduling,
        result_table,
    );

    // Create result table if specified
    if let Some(rt) = result_table {
        ensure_result_table(rt);
    }

    let active_nodes = if scheduling == "round_robin" {
        get_active_node_ids()
    } else {
        vec![]
    };

    if scheduling == "round_robin" && active_nodes.is_empty() {
        pgrx::warning!("No active nodes for round_robin scheduling, tasks will be unassigned");
    }

    for (idx, chunk_spec) in chunk_array.iter().enumerate() {
        let chunk_id = chunk_spec
            .get("chunk_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Merge job payload with chunk-specific payload
        let task_payload = if let Some(chunk_payload) = chunk_spec.get("payload") {
            let mut merged = job_payload.0.clone();
            if let (Some(base), Some(overlay)) = (merged.as_object_mut(), chunk_payload.as_object())
            {
                for (k, v) in overlay {
                    base.insert(k.clone(), v.clone());
                }
            }
            serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string())
        } else {
            job_payload_str.clone()
        };

        let assigned_node = if scheduling == "round_robin" && !active_nodes.is_empty() {
            Some(active_nodes[idx % active_nodes.len()])
        } else {
            None
        };

        insert_task(
            job_id,
            idx as i32,
            chunk_id.as_deref(),
            priority,
            max_retries,
            task_timeout,
            &task_payload,
            assigned_node,
        );
    }

    let _ = Spi::run("NOTIFY pgswarm_tasks");
    job_id
}

/// Submit a job that reads input from a source table.
///
/// Each row in the source table becomes one task. The row's `payload` column
/// is merged with the job-level payload. The row's `id` is used as the chunk_id.
///
/// This is the full pipeline: swarm_insert → source_table → submit_job_from_table → tasks → result_table
///
/// Example:
/// ```sql
/// -- 1. Create and populate source table
/// SELECT pgswarm.create_source_table('pgswarm.video_segments');
/// SELECT pgswarm.swarm_insert('pgswarm.video_segments', '{"start": 0, "end": 60}'::jsonb);
/// SELECT pgswarm.swarm_insert('pgswarm.video_segments', '{"start": 60, "end": 120}'::jsonb);
///
/// -- 2. Submit job from source table
/// SELECT pgswarm.submit_job_from_table(
///     'transcode_video',
///     'pgswarm.video_segments',
///     job_payload := '{"format": "h264"}'::jsonb,
///     result_table := 'pgswarm.transcode_results'
/// );
/// ```
#[pg_extern]
fn submit_job_from_table(
    executor_name: &str,
    source_table: &str,
    job_payload: default!(pgrx::JsonB, "'{}'"),
    priority: default!(i32, 0),
    max_retries: default!(i32, 3),
    task_timeout: default!(i32, 3600),
    scheduling: default!(&str, "'greedy'"),
    result_table: default!(Option<&str>, "NULL"),
) -> i64 {
    if scheduling != "greedy" && scheduling != "round_robin" {
        pgrx::error!("scheduling must be 'greedy' or 'round_robin'");
    }

    validate_executor(executor_name);
    validate_table_name(source_table);

    if let Some(rt) = result_table {
        validate_table_name(rt);
    }

    // Read all rows from source table
    let query = format!("SELECT id, payload::text FROM {} ORDER BY id", source_table);
    let rows: Vec<(i64, String)> = Spi::connect(|client| {
        let table = client.select(&query, None, &[]);
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
        pgrx::error!("Source table '{}' is empty", source_table);
    }

    let chunk_count = rows.len() as i32;
    let job_payload_str =
        serde_json::to_string(&job_payload.0).unwrap_or_else(|_| "{}".to_string());

    let job_id = insert_job(
        executor_name,
        &job_payload_str,
        chunk_count,
        priority,
        max_retries,
        task_timeout,
        scheduling,
        result_table,
    );

    // Create result table if specified
    if let Some(rt) = result_table {
        ensure_result_table(rt);
    }

    let active_nodes = if scheduling == "round_robin" {
        get_active_node_ids()
    } else {
        vec![]
    };

    if scheduling == "round_robin" && active_nodes.is_empty() {
        pgrx::warning!("No active nodes for round_robin scheduling, tasks will be unassigned");
    }

    // Create one task per source row
    for (idx, (row_id, row_payload_str)) in rows.iter().enumerate() {
        // Merge job payload with row payload
        let task_payload = {
            let row_val: serde_json::Value = serde_json::from_str(row_payload_str)
                .unwrap_or(serde_json::Value::Object(Default::default()));
            let mut merged = job_payload.0.clone();
            if let (Some(base), Some(overlay)) = (merged.as_object_mut(), row_val.as_object()) {
                for (k, v) in overlay {
                    base.insert(k.clone(), v.clone());
                }
            }
            serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string())
        };

        let chunk_id = Some(row_id.to_string());
        let assigned_node = if scheduling == "round_robin" && !active_nodes.is_empty() {
            Some(active_nodes[idx % active_nodes.len()])
        } else {
            None
        };

        insert_task(
            job_id,
            idx as i32,
            chunk_id.as_deref(),
            priority,
            max_retries,
            task_timeout,
            &task_payload,
            assigned_node,
        );
    }

    let _ = Spi::run("NOTIFY pgswarm_tasks");
    job_id
}

// ─── Internal Helpers ───────────────────────────────────────────────────────

/// Validate that an executor exists and is healthy.
fn validate_executor(executor_name: &str) {
    let health = Spi::get_one_with_args::<String>(
        "SELECT health_status FROM pgswarm.executors WHERE name = $1",
        &[executor_name.into()],
    );

    match health {
        Ok(Some(status)) => {
            if status == "disabled" {
                pgrx::error!(
                    "Executor '{}' is disabled due to repeated failures. Use pgswarm.reset_executor() to re-enable.",
                    executor_name
                );
            }
        }
        _ => pgrx::error!(
            "Executor '{}' not found. Register it first with pgswarm.register_executor()",
            executor_name
        ),
    }
}

/// Insert a job row and return its ID.
fn insert_job(
    executor_name: &str,
    payload_str: &str,
    chunk_count: i32,
    priority: i32,
    max_retries: i32,
    task_timeout: i32,
    scheduling: &str,
    result_table: Option<&str>,
) -> i64 {
    let job_id = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgswarm.jobs (executor_name, payload, chunk_count, priority, max_retries, task_timeout, scheduling, result_table)
         VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8)
         RETURNING id",
        &[
            executor_name.into(),
            payload_str.into(),
            chunk_count.into(),
            priority.into(),
            max_retries.into(),
            task_timeout.into(),
            scheduling.into(),
            result_table.into(),
        ],
    );

    match job_id {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create job: no ID returned"),
        Err(e) => pgrx::error!("Failed to create job: {}", e),
    }
}

/// Insert a task row.
fn insert_task(
    job_id: i64,
    chunk_index: i32,
    chunk_id: Option<&str>,
    priority: i32,
    max_retries: i32,
    task_timeout: i32,
    payload_str: &str,
    assigned_node_id: Option<i64>,
) {
    let _ = Spi::run_with_args(
        "INSERT INTO pgswarm.tasks (job_id, chunk_index, chunk_id, priority, max_retries, timeout_secs, payload, assigned_node_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)",
        &[
            job_id.into(),
            chunk_index.into(),
            chunk_id.into(),
            priority.into(),
            max_retries.into(),
            task_timeout.into(),
            payload_str.into(),
            assigned_node_id.into(),
        ],
    );
}

/// Get active node IDs for round-robin assignment.
fn get_active_node_ids() -> Vec<i64> {
    let mut nodes = Vec::new();
    Spi::connect(|client| {
        let table = client.select(
            "SELECT id FROM pgswarm.nodes WHERE status = 'active' ORDER BY id",
            None,
            &[],
        );
        if let Ok(table) = table {
            for row in table {
                let id: Option<i64> = row.get_by_name("id").unwrap();
                if let Some(id) = id {
                    nodes.push(id);
                }
            }
        }
    });
    nodes
}

/// Validate that a table name is a safe SQL identifier.
/// Allows schema-qualified names like "myschema.my_table".
fn validate_table_name(name: &str) {
    if name.is_empty() {
        pgrx::error!("table name cannot be empty");
    }
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() > 2 {
        pgrx::error!("table name has too many dots: {}", name);
    }
    for part in &parts {
        if part.is_empty() {
            pgrx::error!("table name has empty component: {}", name);
        }
        if !part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            pgrx::error!("table name contains invalid characters: {}", name);
        }
        if !part
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            pgrx::error!(
                "table name component must start with letter or underscore: {}",
                name
            );
        }
    }
}

/// Create the result table if it doesn't already exist.
///
/// Result tables have a standard schema:
/// - job_id BIGINT
/// - task_id BIGINT
/// - chunk_index INT
/// - chunk_id TEXT
/// - result JSONB
/// - node_id BIGINT
/// - created_at TIMESTAMPTZ
fn ensure_result_table(table_name: &str) {
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            job_id      BIGINT NOT NULL,
            task_id     BIGINT NOT NULL,
            chunk_index INT NOT NULL,
            chunk_id    TEXT,
            result      JSONB,
            node_id     BIGINT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        table_name
    );
    let _ = Spi::run(&query);
}

/// Create a result table with pgswarm's standard schema for collecting distributed task output.
///
/// The table is created in the current database, visible to all nodes in the swarm.
/// Use this when you want results from distributed jobs (e.g., YOLO detections) to land
/// in a shared table that all nodes can query.
#[pg_extern]
fn create_result_table(table_name: &str) -> bool {
    validate_table_name(table_name);
    ensure_result_table(table_name);
    true
}

// ─── Source Tables ──────────────────────────────────────────────────────────

/// Create a source table for distributed input data.
///
/// Source tables have a standard schema that pg_swarm understands:
/// - id BIGSERIAL (auto-assigned row ID)
/// - payload JSONB (the data for each work item)
/// - created_at TIMESTAMPTZ
///
/// Since all nodes share the same PostgreSQL database, any row inserted
/// is immediately visible to the entire swarm.
#[pg_extern]
fn create_source_table(table_name: &str) -> bool {
    validate_table_name(table_name);
    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id          BIGSERIAL PRIMARY KEY,
            payload     JSONB NOT NULL DEFAULT '{{}}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        table_name
    );
    let _ = Spi::run(&query);
    true
}

/// Insert a single row into a source table and return its ID.
///
/// This is the standard way to populate a distributed input table.
/// All nodes in the swarm see the insert immediately (same database).
#[pg_extern]
fn swarm_insert(table_name: &str, payload: pgrx::JsonB) -> i64 {
    validate_table_name(table_name);
    let payload_str = serde_json::to_string(&payload.0).unwrap_or_else(|_| "{}".to_string());
    let query = format!(
        "INSERT INTO {} (payload) VALUES ($1::jsonb) RETURNING id",
        table_name
    );
    let result = Spi::get_one_with_args::<i64>(&query, &[payload_str.into()]);
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("swarm_insert: no ID returned"),
        Err(e) => pgrx::error!("swarm_insert failed: {}", e),
    }
}

/// Insert multiple rows into a source table from a JSON array.
///
/// Each element in the array becomes one row's payload.
/// Returns the number of rows inserted.
#[pg_extern]
fn swarm_insert_batch(table_name: &str, payloads: pgrx::JsonB) -> i64 {
    validate_table_name(table_name);
    let arr = match payloads.0.as_array() {
        Some(arr) => arr.clone(),
        None => pgrx::error!("payloads must be a JSON array"),
    };
    if arr.is_empty() {
        return 0;
    }
    let query = format!("INSERT INTO {} (payload) VALUES ($1::jsonb)", table_name);
    let mut count: i64 = 0;
    for item in &arr {
        let payload_str = serde_json::to_string(item).unwrap_or_else(|_| "{}".to_string());
        let _ = Spi::run_with_args(&query, &[payload_str.into()]);
        count += 1;
    }
    count
}

/// Count rows in a source table.
#[pg_extern]
fn swarm_count(table_name: &str) -> i64 {
    validate_table_name(table_name);
    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    Spi::get_one::<i64>(&query).unwrap_or(Some(0)).unwrap_or(0)
}

// ─── Source Table Watches ────────────────────────────────────────────────────

/// Watch a source table for new rows and automatically create jobs.
///
/// The node manager polls watched tables every `poll_interval` seconds.
/// New rows (id > last_processed_id) are batched into jobs and distributed
/// across the swarm. Coordination uses FOR UPDATE SKIP LOCKED — no leader needed.
///
/// Example:
/// ```sql
/// SELECT pgswarm.watch_source_table(
///     'pgswarm.video_frames',
///     'yolo_detect',
///     job_payload := '{"model": "yolov8"}'::jsonb,
///     result_table := 'pgswarm.detections',
///     batch_size := 50,
///     poll_interval := 5
/// );
/// ```
#[pg_extern]
fn watch_source_table(
    source_table: &str,
    executor_name: &str,
    job_payload: default!(pgrx::JsonB, "'{}'"),
    batch_size: default!(i32, 100),
    poll_interval: default!(i32, 5),
    priority: default!(i32, 0),
    max_retries: default!(i32, 3),
    task_timeout: default!(i32, 3600),
    scheduling: default!(&str, "'greedy'"),
    result_table: default!(Option<&str>, "NULL"),
) -> i64 {
    validate_table_name(source_table);
    validate_executor(executor_name);
    if let Some(rt) = result_table {
        validate_table_name(rt);
    }
    if batch_size < 1 {
        pgrx::error!("batch_size must be at least 1");
    }
    if poll_interval < 1 {
        pgrx::error!("poll_interval must be at least 1 second");
    }

    let payload_str = serde_json::to_string(&job_payload.0).unwrap_or_else(|_| "{}".to_string());

    // Create result table if specified
    if let Some(rt) = result_table {
        ensure_result_table(rt);
    }

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgswarm.watches
             (source_table, executor_name, job_payload, batch_size, poll_interval,
              priority, max_retries, task_timeout, scheduling, result_table)
         VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (source_table, executor_name) DO UPDATE
         SET job_payload = EXCLUDED.job_payload,
             batch_size = EXCLUDED.batch_size,
             poll_interval = EXCLUDED.poll_interval,
             priority = EXCLUDED.priority,
             max_retries = EXCLUDED.max_retries,
             task_timeout = EXCLUDED.task_timeout,
             scheduling = EXCLUDED.scheduling,
             result_table = EXCLUDED.result_table,
             enabled = true
         RETURNING id",
        &[
            source_table.into(),
            executor_name.into(),
            payload_str.into(),
            batch_size.into(),
            poll_interval.into(),
            priority.into(),
            max_retries.into(),
            task_timeout.into(),
            scheduling.into(),
            result_table.into(),
        ],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create watch"),
        Err(e) => pgrx::error!("Failed to create watch: {}", e),
    }
}

/// Stop watching a source table.
#[pg_extern]
fn unwatch_source_table(source_table: &str, executor_name: &str) -> bool {
    let result = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.watches SET enabled = false
         WHERE source_table = $1 AND executor_name = $2 AND enabled = true
         RETURNING id",
        &[source_table.into(), executor_name.into()],
    );
    matches!(result, Ok(Some(_)))
}

/// List all active watches.
#[pg_extern]
fn list_watches() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(source_table, String),
        name!(executor_name, String),
        name!(batch_size, i32),
        name!(poll_interval, i32),
        name!(result_table, Option<String>),
        name!(last_processed_id, i64),
        name!(enabled, bool),
    ),
> {
    let mut results = Vec::new();
    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, source_table, executor_name, batch_size, poll_interval,
                    result_table, last_processed_id, enabled
             FROM pgswarm.watches ORDER BY id",
            None,
            &[],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let source_table: String =
                    row.get_by_name("source_table").unwrap().unwrap_or_default();
                let executor_name: String = row
                    .get_by_name("executor_name")
                    .unwrap()
                    .unwrap_or_default();
                let batch_size: i32 = row.get_by_name("batch_size").unwrap().unwrap_or(100);
                let poll_interval: i32 = row.get_by_name("poll_interval").unwrap().unwrap_or(5);
                let result_table: Option<String> = row.get_by_name("result_table").unwrap();
                let last_processed_id: i64 =
                    row.get_by_name("last_processed_id").unwrap().unwrap_or(0);
                let enabled: bool = row.get_by_name("enabled").unwrap().unwrap_or(false);
                results.push((
                    id,
                    source_table,
                    executor_name,
                    batch_size,
                    poll_interval,
                    result_table,
                    last_processed_id,
                    enabled,
                ));
            }
        }
    });
    TableIterator::new(results)
}

/// Cancel a job and all its pending/running tasks.
#[pg_extern]
fn cancel_job(job_id: i64) -> bool {
    // Cancel pending and running tasks
    let _ = Spi::run_with_args(
        "UPDATE pgswarm.tasks
         SET status = 'cancelled', completed_at = now()
         WHERE job_id = $1 AND status IN ('pending', 'running')",
        &[job_id.into()],
    );

    // Update job status
    let result = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.jobs
         SET status = 'cancelled', completed_at = now()
         WHERE id = $1 AND status IN ('pending', 'running')
         RETURNING id",
        &[job_id.into()],
    );

    matches!(result, Ok(Some(_)))
}

/// Re-queue all failed tasks in a job.
#[pg_extern]
fn retry_job(job_id: i64) -> i64 {
    let result = Spi::get_one_with_args::<i64>(
        "WITH retried AS (
             UPDATE pgswarm.tasks
             SET status = 'pending',
                 node_id = NULL,
                 started_at = NULL,
                 completed_at = NULL,
                 error_message = NULL,
                 retry_count = 0
             WHERE job_id = $1 AND status = 'failed'
             RETURNING id
         )
         SELECT COUNT(*) FROM retried",
        &[job_id.into()],
    );

    let count = result.unwrap_or(Some(0)).unwrap_or(0);

    if count > 0 {
        // Reset job status to running
        let _ = Spi::run_with_args(
            "UPDATE pgswarm.jobs
             SET status = 'running', completed_at = NULL
             WHERE id = $1",
            &[job_id.into()],
        );
        let _ = Spi::run("NOTIFY pgswarm_tasks");
    }

    count
}

/// Get details about a specific job.
#[pg_extern]
fn get_job(
    job_id: i64,
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(executor_name, String),
        name!(payload, pgrx::JsonB),
        name!(chunk_count, i32),
        name!(status, String),
        name!(priority, i32),
        name!(created_at, TimestampWithTimeZone),
        name!(started_at, Option<TimestampWithTimeZone>),
        name!(completed_at, Option<TimestampWithTimeZone>),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, executor_name, payload::text, chunk_count, status,
                    priority, created_at, started_at, completed_at
             FROM pgswarm.jobs WHERE id = $1",
            None,
            &[job_id.into()],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let executor_name: String = row
                    .get_by_name("executor_name")
                    .unwrap()
                    .unwrap_or_default();
                let payload_str: String = row
                    .get_by_name("payload")
                    .unwrap()
                    .unwrap_or_else(|| "{}".to_string());
                let payload_json: serde_json::Value = serde_json::from_str(&payload_str)
                    .unwrap_or(serde_json::Value::Object(Default::default()));
                let chunk_count: i32 = row.get_by_name("chunk_count").unwrap().unwrap_or(1);
                let status: String = row.get_by_name("status").unwrap().unwrap_or_default();
                let priority: i32 = row.get_by_name("priority").unwrap().unwrap_or(0);
                let created_at: TimestampWithTimeZone =
                    row.get_by_name("created_at").unwrap().unwrap();
                let started_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("started_at").unwrap();
                let completed_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("completed_at").unwrap();

                results.push((
                    id,
                    executor_name,
                    pgrx::JsonB(payload_json),
                    chunk_count,
                    status,
                    priority,
                    created_at,
                    started_at,
                    completed_at,
                ));
            }
        }
    });

    TableIterator::new(results)
}

/// List all jobs, optionally filtered by status.
#[pg_extern]
fn list_jobs(
    status_filter: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(executor_name, String),
        name!(status, String),
        name!(chunk_count, i32),
        name!(priority, i32),
        name!(created_at, TimestampWithTimeZone),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, executor_name, status, chunk_count, priority, created_at
             FROM pgswarm.jobs
             WHERE ($1::text IS NULL OR status = $1)
             ORDER BY created_at DESC",
            None,
            &[status_filter.into()],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let executor_name: String = row
                    .get_by_name("executor_name")
                    .unwrap()
                    .unwrap_or_default();
                let status: String = row.get_by_name("status").unwrap().unwrap_or_default();
                let chunk_count: i32 = row.get_by_name("chunk_count").unwrap().unwrap_or(1);
                let priority: i32 = row.get_by_name("priority").unwrap().unwrap_or(0);
                let created_at: TimestampWithTimeZone =
                    row.get_by_name("created_at").unwrap().unwrap();
                results.push((id, executor_name, status, chunk_count, priority, created_at));
            }
        }
    });

    TableIterator::new(results)
}

// ─── Task Queries ───────────────────────────────────────────────────────────

/// Get tasks for a specific job.
#[pg_extern]
fn get_tasks(
    job_id: i64,
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(chunk_index, i32),
        name!(chunk_id, Option<String>),
        name!(status, String),
        name!(node_id, Option<i64>),
        name!(assigned_node_id, Option<i64>),
        name!(retry_count, i32),
        name!(result, Option<pgrx::JsonB>),
        name!(error_message, Option<String>),
        name!(started_at, Option<TimestampWithTimeZone>),
        name!(completed_at, Option<TimestampWithTimeZone>),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, chunk_index, chunk_id, status, node_id, assigned_node_id,
                    retry_count, result::text, error_message, started_at, completed_at
             FROM pgswarm.tasks
             WHERE job_id = $1
             ORDER BY chunk_index",
            None,
            &[job_id.into()],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let chunk_index: i32 = row.get_by_name("chunk_index").unwrap().unwrap_or(0);
                let chunk_id: Option<String> = row.get_by_name("chunk_id").unwrap();
                let status: String = row.get_by_name("status").unwrap().unwrap_or_default();
                let node_id: Option<i64> = row.get_by_name("node_id").unwrap();
                let assigned_node_id: Option<i64> = row.get_by_name("assigned_node_id").unwrap();
                let retry_count: i32 = row.get_by_name("retry_count").unwrap().unwrap_or(0);
                let result_str: Option<String> = row.get_by_name("result").unwrap();
                let result_json =
                    result_str.and_then(|s| serde_json::from_str(&s).ok().map(pgrx::JsonB));
                let error_message: Option<String> = row.get_by_name("error_message").unwrap();
                let started_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("started_at").unwrap();
                let completed_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("completed_at").unwrap();

                results.push((
                    id,
                    chunk_index,
                    chunk_id,
                    status,
                    node_id,
                    assigned_node_id,
                    retry_count,
                    result_json,
                    error_message,
                    started_at,
                    completed_at,
                ));
            }
        }
    });

    TableIterator::new(results)
}

/// Get only completed task results for a job.
#[pg_extern]
fn get_task_results(
    job_id: i64,
) -> TableIterator<
    'static,
    (
        name!(chunk_index, i32),
        name!(result, Option<pgrx::JsonB>),
        name!(completed_at, Option<TimestampWithTimeZone>),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT chunk_index, result::text, completed_at
             FROM pgswarm.tasks
             WHERE job_id = $1 AND status = 'completed'
             ORDER BY chunk_index",
            None,
            &[job_id.into()],
        );
        if let Ok(table) = table {
            for row in table {
                let chunk_index: i32 = row.get_by_name("chunk_index").unwrap().unwrap_or(0);
                let result_str: Option<String> = row.get_by_name("result").unwrap();
                let result_json =
                    result_str.and_then(|s| serde_json::from_str(&s).ok().map(pgrx::JsonB));
                let completed_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("completed_at").unwrap();

                results.push((chunk_index, result_json, completed_at));
            }
        }
    });

    TableIterator::new(results)
}

// ─── Node Management ────────────────────────────────────────────────────────

/// List all registered nodes with their status.
#[pg_extern]
fn list_nodes() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(node_name, String),
        name!(pid, i32),
        name!(status, String),
        name!(workers_count, i32),
        name!(last_heartbeat, TimestampWithTimeZone),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, node_name, pid, status, workers_count, last_heartbeat
             FROM pgswarm.nodes
             ORDER BY id",
            None,
            &[],
        );
        if let Ok(table) = table {
            for row in table {
                let id: i64 = row.get_by_name("id").unwrap().unwrap_or(0);
                let node_name: String = row.get_by_name("node_name").unwrap().unwrap_or_default();
                let pid: i32 = row.get_by_name("pid").unwrap().unwrap_or(0);
                let status: String = row.get_by_name("status").unwrap().unwrap_or_default();
                let workers_count: i32 = row.get_by_name("workers_count").unwrap().unwrap_or(0);
                let last_heartbeat: TimestampWithTimeZone =
                    row.get_by_name("last_heartbeat").unwrap().unwrap();
                results.push((id, node_name, pid, status, workers_count, last_heartbeat));
            }
        }
    });

    TableIterator::new(results)
}

/// Drain a node - stop accepting new tasks, finish current ones.
#[pg_extern]
fn drain_node(node_id: i64) -> bool {
    let result = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.nodes SET status = 'draining'
         WHERE id = $1 AND status = 'active'
         RETURNING id",
        &[node_id.into()],
    );
    matches!(result, Ok(Some(_)))
}

/// Reactivate a drained node.
#[pg_extern]
fn activate_node(node_id: i64) -> bool {
    let result = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.nodes SET status = 'active'
         WHERE id = $1 AND status = 'draining'
         RETURNING id",
        &[node_id.into()],
    );
    matches!(result, Ok(Some(_)))
}

// ─── Executor Health ────────────────────────────────────────────────────

/// Reset an executor's health status back to healthy.
/// Use this after fixing the underlying issue that caused failures.
#[pg_extern]
fn reset_executor(name: &str) -> bool {
    let result = Spi::get_one_with_args::<i64>(
        "UPDATE pgswarm.executors
         SET health_status = 'healthy',
             consecutive_failures = 0,
             last_error = NULL,
             last_failure_at = NULL
         WHERE name = $1
         RETURNING id",
        &[name.into()],
    );
    matches!(result, Ok(Some(_)))
}

/// Get health stats for a specific executor.
#[pg_extern]
fn executor_stats(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(health_status, String),
        name!(total_runs, i64),
        name!(total_failures, i64),
        name!(consecutive_failures, i32),
        name!(failure_rate_pct, f64),
        name!(last_error, Option<String>),
        name!(last_failure_at, Option<TimestampWithTimeZone>),
    ),
> {
    let mut results = Vec::new();

    Spi::connect(|client| {
        let table = client.select(
            "SELECT name, health_status, total_runs, total_failures,
                    consecutive_failures,
                    CASE WHEN total_runs > 0
                         THEN (total_failures::float8 / total_runs::float8) * 100.0
                         ELSE 0.0
                    END AS failure_rate_pct,
                    last_error, last_failure_at
             FROM pgswarm.executors WHERE name = $1",
            None,
            &[name.into()],
        );
        if let Ok(table) = table {
            for row in table {
                let name: String = row.get_by_name("name").unwrap().unwrap_or_default();
                let health_status: String = row
                    .get_by_name("health_status")
                    .unwrap()
                    .unwrap_or_default();
                let total_runs: i64 = row.get_by_name("total_runs").unwrap().unwrap_or(0);
                let total_failures: i64 = row.get_by_name("total_failures").unwrap().unwrap_or(0);
                let consecutive_failures: i32 = row
                    .get_by_name("consecutive_failures")
                    .unwrap()
                    .unwrap_or(0);
                let failure_rate_pct: f64 =
                    row.get_by_name("failure_rate_pct").unwrap().unwrap_or(0.0);
                let last_error: Option<String> = row.get_by_name("last_error").unwrap();
                let last_failure_at: Option<TimestampWithTimeZone> =
                    row.get_by_name("last_failure_at").unwrap();
                results.push((
                    name,
                    health_status,
                    total_runs,
                    total_failures,
                    consecutive_failures,
                    failure_rate_pct,
                    last_error,
                    last_failure_at,
                ));
            }
        }
    });

    TableIterator::new(results)
}

// ─── Monitoring ─────────────────────────────────────────────────────────────

/// Get overall swarm status as JSON.
#[pg_extern]
fn status() -> pgrx::JsonB {
    let result = Spi::get_one::<String>(
        "SELECT json_build_object(
             'active_nodes', (SELECT COUNT(*) FROM pgswarm.nodes WHERE status = 'active'),
             'total_workers', (SELECT COALESCE(SUM(workers_count), 0) FROM pgswarm.nodes WHERE status = 'active'),
             'pending_tasks', (SELECT COUNT(*) FROM pgswarm.tasks WHERE status = 'pending'),
             'running_tasks', (SELECT COUNT(*) FROM pgswarm.tasks WHERE status = 'running'),
             'completed_jobs_24h', (SELECT COUNT(*) FROM pgswarm.jobs WHERE status = 'completed' AND completed_at > now() - interval '24 hours'),
             'failed_jobs_24h', (SELECT COUNT(*) FROM pgswarm.jobs WHERE status = 'failed' AND completed_at > now() - interval '24 hours')
         )::text",
    );

    let json_str = result
        .unwrap_or(Some("{}".to_string()))
        .unwrap_or_else(|| "{}".to_string());
    let json_val: serde_json::Value =
        serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Object(Default::default()));
    pgrx::JsonB(json_val)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use super::types::{HealthStatus, JobStatus, NodeStatus, SchedulingStrategy, TaskStatus};

    #[test]
    fn test_task_status_strings_match_sql_check() {
        let expected = ["pending", "running", "completed", "failed", "cancelled"];
        let actual = [
            TaskStatus::Pending.as_str(),
            TaskStatus::Running.as_str(),
            TaskStatus::Completed.as_str(),
            TaskStatus::Failed.as_str(),
            TaskStatus::Cancelled.as_str(),
        ];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_job_status_strings_match_sql_check() {
        let expected = ["pending", "running", "completed", "failed", "cancelled"];
        let actual = [
            JobStatus::Pending.as_str(),
            JobStatus::Running.as_str(),
            JobStatus::Completed.as_str(),
            JobStatus::Failed.as_str(),
            JobStatus::Cancelled.as_str(),
        ];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_node_status_strings_match_sql_check() {
        let expected = ["active", "draining", "dead"];
        let actual = [
            NodeStatus::Active.as_str(),
            NodeStatus::Draining.as_str(),
            NodeStatus::Dead.as_str(),
        ];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_scheduling_strings_match_sql_check() {
        let expected = ["greedy", "round_robin"];
        let actual = [
            SchedulingStrategy::Greedy.as_str(),
            SchedulingStrategy::RoundRobin.as_str(),
        ];
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_health_status_strings_match_sql_check() {
        let expected = ["healthy", "degraded", "disabled"];
        let actual = [
            HealthStatus::Healthy.as_str(),
            HealthStatus::Degraded.as_str(),
            HealthStatus::Disabled.as_str(),
        ];
        assert_eq!(expected, actual);
    }

    #[pg_test]
    fn test_register_and_list_executor() {
        // Create a dummy executor function
        Spi::run(
            "CREATE FUNCTION test_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{\"ok\": true}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();

        let id = crate::register_executor("test_executor", "test_exec", None);
        assert!(id > 0);

        // Verify it appears in list
        let results: Vec<_> = crate::list_executors().collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, "test_executor");
        assert_eq!(results[0].2, "test_exec");
    }

    #[pg_test]
    fn test_unregister_executor() {
        Spi::run(
            "CREATE FUNCTION unreg_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();

        crate::register_executor("unreg_test", "unreg_exec", None);
        let removed = crate::unregister_executor("unreg_test");
        assert!(removed);

        let removed_again = crate::unregister_executor("unreg_test");
        assert!(!removed_again);
    }

    #[pg_test]
    fn test_submit_job_creates_tasks() {
        // Register executor
        Spi::run(
            "CREATE FUNCTION job_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{\"done\": true}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("job_executor", "job_exec", None);

        // Submit job with 5 chunks
        let payload = pgrx::JsonB(serde_json::json!({"video": "test.mp4"}));
        let job_id = crate::submit_job("job_executor", payload, 5, 0, 3, 3600, "greedy", None);
        assert!(job_id > 0);

        // Verify 5 tasks were created
        let task_count = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgswarm.tasks WHERE job_id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(task_count, 5);

        // Verify chunk indices are 0..4
        let max_chunk = Spi::get_one_with_args::<i32>(
            "SELECT MAX(chunk_index) FROM pgswarm.tasks WHERE job_id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(max_chunk, 4);

        // Verify job status is pending
        let status = Spi::get_one_with_args::<String>(
            "SELECT status FROM pgswarm.jobs WHERE id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "pending");
    }

    #[pg_test]
    #[should_panic(expected = "Executor 'nonexistent' not found")]
    fn test_submit_job_missing_executor() {
        let payload = pgrx::JsonB(serde_json::json!({}));
        crate::submit_job("nonexistent", payload, 1, 0, 3, 3600, "greedy", None);
    }

    #[pg_test]
    #[should_panic(expected = "chunk_count must be at least 1")]
    fn test_submit_job_invalid_chunk_count() {
        Spi::run(
            "CREATE FUNCTION bad_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("bad_executor", "bad_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        crate::submit_job("bad_executor", payload, 0, 0, 3, 3600, "greedy", None);
    }

    #[pg_test]
    fn test_cancel_job() {
        Spi::run(
            "CREATE FUNCTION cancel_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("cancel_executor", "cancel_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        let job_id = crate::submit_job("cancel_executor", payload, 3, 0, 3, 3600, "greedy", None);

        let cancelled = crate::cancel_job(job_id);
        assert!(cancelled);

        // Verify job is cancelled
        let status = Spi::get_one_with_args::<String>(
            "SELECT status FROM pgswarm.jobs WHERE id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "cancelled");

        // Verify all tasks are cancelled
        let pending_count = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgswarm.tasks WHERE job_id = $1 AND status != 'cancelled'",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(pending_count, 0);
    }

    #[pg_test]
    fn test_job_progress_view() {
        Spi::run(
            "CREATE FUNCTION progress_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("progress_executor", "progress_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        let job_id = crate::submit_job("progress_executor", payload, 4, 0, 3, 3600, "greedy", None);

        // Simulate some tasks completing
        Spi::run_with_args(
            "UPDATE pgswarm.tasks SET status = 'completed', completed_at = now()
             WHERE job_id = $1 AND chunk_index < 2",
            &[job_id.into()],
        )
        .unwrap();

        // Check progress view
        let completed = Spi::get_one_with_args::<i64>(
            "SELECT completed FROM pgswarm.job_progress WHERE job_id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(completed, 2);

        let pending = Spi::get_one_with_args::<i64>(
            "SELECT pending FROM pgswarm.job_progress WHERE job_id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(pending, 2);
    }

    #[pg_test]
    fn test_status_returns_json() {
        let status = crate::status();
        let obj = status.0.as_object().expect("status should return object");
        assert!(obj.contains_key("active_nodes"));
        assert!(obj.contains_key("pending_tasks"));
        assert!(obj.contains_key("running_tasks"));
    }

    #[pg_test]
    fn test_drain_and_activate_node() {
        // Insert a test node
        let node_id = Spi::get_one::<i64>(
            "INSERT INTO pgswarm.nodes (node_name, pid, status)
             VALUES ('test_node', 99999, 'active')
             RETURNING id",
        )
        .unwrap()
        .unwrap();

        // Drain it
        let drained = crate::drain_node(node_id);
        assert!(drained);

        let status = Spi::get_one_with_args::<String>(
            "SELECT status FROM pgswarm.nodes WHERE id = $1",
            &[node_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "draining");

        // Reactivate
        let activated = crate::activate_node(node_id);
        assert!(activated);

        let status = Spi::get_one_with_args::<String>(
            "SELECT status FROM pgswarm.nodes WHERE id = $1",
            &[node_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "active");
    }

    #[pg_test]
    fn test_get_tasks_returns_correct_data() {
        Spi::run(
            "CREATE FUNCTION tasks_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("tasks_executor", "tasks_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({"key": "value"}));
        let job_id = crate::submit_job("tasks_executor", payload, 3, 0, 3, 3600, "greedy", None);

        let tasks: Vec<_> = crate::get_tasks(job_id).collect();
        assert_eq!(tasks.len(), 3);

        // Tasks should be ordered by chunk_index
        assert_eq!(tasks[0].1, 0); // chunk_index
        assert_eq!(tasks[1].1, 1);
        assert_eq!(tasks[2].1, 2);

        // All should be pending
        assert_eq!(tasks[0].3, "pending"); // status (shifted by chunk_id)
        assert_eq!(tasks[1].3, "pending");
        assert_eq!(tasks[2].3, "pending");
    }

    #[pg_test]
    fn test_retry_job_requeues_failed_tasks() {
        Spi::run(
            "CREATE FUNCTION retry_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("retry_executor", "retry_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        let job_id = crate::submit_job("retry_executor", payload, 4, 0, 3, 3600, "greedy", None);

        // Mark 2 tasks as failed
        Spi::run_with_args(
            "UPDATE pgswarm.tasks SET status = 'failed', error_message = 'boom'
             WHERE job_id = $1 AND chunk_index < 2",
            &[job_id.into()],
        )
        .unwrap();

        // Mark others as completed
        Spi::run_with_args(
            "UPDATE pgswarm.tasks SET status = 'completed', completed_at = now()
             WHERE job_id = $1 AND chunk_index >= 2",
            &[job_id.into()],
        )
        .unwrap();

        // Retry
        let retried = crate::retry_job(job_id);
        assert_eq!(retried, 2);

        // Verify failed tasks are now pending
        let pending_count = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgswarm.tasks WHERE job_id = $1 AND status = 'pending'",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(pending_count, 2);
    }

    #[pg_test]
    fn test_submit_job_with_chunks() {
        Spi::run(
            "CREATE FUNCTION chunk_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT payload; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("chunk_executor", "chunk_exec", None);

        let job_payload = pgrx::JsonB(serde_json::json!({"video": "input.mp4"}));
        let chunks = pgrx::JsonB(serde_json::json!([
            {"chunk_id": "frames_0_99",   "payload": {"start_frame": 0,   "end_frame": 99}},
            {"chunk_id": "frames_100_199", "payload": {"start_frame": 100, "end_frame": 199}},
            {"chunk_id": "frames_200_299", "payload": {"start_frame": 200, "end_frame": 299}}
        ]));

        let job_id = crate::submit_job_with_chunks(
            "chunk_executor",
            chunks,
            job_payload,
            0,
            3,
            3600,
            "greedy",
            None,
        );
        assert!(job_id > 0);

        let tasks: Vec<_> = crate::get_tasks(job_id).collect();
        assert_eq!(tasks.len(), 3);

        // Verify chunk_ids are set
        assert_eq!(tasks[0].2, Some("frames_0_99".to_string()));
        assert_eq!(tasks[1].2, Some("frames_100_199".to_string()));
        assert_eq!(tasks[2].2, Some("frames_200_299".to_string()));

        // Verify payloads are merged (check via SQL since we can't easily access JSONB from task iterator)
        let payload_str = Spi::get_one_with_args::<String>(
            "SELECT payload::text FROM pgswarm.tasks WHERE job_id = $1 AND chunk_index = 0",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        let payload_val: serde_json::Value = serde_json::from_str(&payload_str).unwrap();
        // Should have both job-level and chunk-level fields
        assert_eq!(payload_val["video"], "input.mp4");
        assert_eq!(payload_val["start_frame"], 0);
        assert_eq!(payload_val["end_frame"], 99);
    }

    #[pg_test]
    fn test_round_robin_scheduling() {
        Spi::run(
            "CREATE FUNCTION rr_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("rr_executor", "rr_exec", None);

        // Create 3 fake nodes
        Spi::run(
            "INSERT INTO pgswarm.nodes (node_name, pid, status)
             VALUES ('node_a', 10001, 'active'),
                    ('node_b', 10002, 'active'),
                    ('node_c', 10003, 'active')",
        )
        .unwrap();

        let payload = pgrx::JsonB(serde_json::json!({}));
        let job_id = crate::submit_job("rr_executor", payload, 6, 0, 3, 3600, "round_robin", None);

        // Verify tasks are assigned round-robin across 3 nodes
        let assigned: Vec<Option<i64>> = Spi::connect(|client| {
            let table = client
                .select(
                    "SELECT assigned_node_id FROM pgswarm.tasks WHERE job_id = $1 ORDER BY chunk_index",
                    None,
                    &[job_id.into()],
                )
                .unwrap();
            table
                .into_iter()
                .map(|row| {
                    let v: Option<i64> = row.get_by_name("assigned_node_id").unwrap();
                    v
                })
                .collect()
        });

        assert_eq!(assigned.len(), 6);
        // With 3 nodes and 6 tasks: A, B, C, A, B, C
        assert_eq!(assigned[0], assigned[3]); // same node for chunk 0 and 3
        assert_eq!(assigned[1], assigned[4]); // same node for chunk 1 and 4
        assert_eq!(assigned[2], assigned[5]); // same node for chunk 2 and 5
                                              // All different nodes for first 3
        assert_ne!(assigned[0], assigned[1]);
        assert_ne!(assigned[1], assigned[2]);
    }

    #[pg_test]
    fn test_executor_health_tracking() {
        Spi::run(
            "CREATE FUNCTION health_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("health_executor", "health_exec", None);

        // Verify initial health
        let stats: Vec<_> = crate::executor_stats("health_executor").collect();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1, "healthy"); // health_status
        assert_eq!(stats[0].2, 0); // total_runs
        assert_eq!(stats[0].3, 0); // total_failures

        // Simulate failures by directly updating
        Spi::run(
            "UPDATE pgswarm.executors
             SET total_runs = 10, total_failures = 6, consecutive_failures = 6,
                 health_status = 'disabled', last_error = 'connection refused'
             WHERE name = 'health_executor'",
        )
        .unwrap();

        // Verify disabled state
        let stats: Vec<_> = crate::executor_stats("health_executor").collect();
        assert_eq!(stats[0].1, "disabled");
        assert_eq!(stats[0].4, 6); // consecutive_failures
        assert!(stats[0].5 > 50.0); // failure_rate_pct > 50%

        // Reset executor
        let reset = crate::reset_executor("health_executor");
        assert!(reset);

        let stats: Vec<_> = crate::executor_stats("health_executor").collect();
        assert_eq!(stats[0].1, "healthy");
        assert_eq!(stats[0].4, 0); // consecutive_failures reset
    }

    #[pg_test]
    #[should_panic(expected = "disabled due to repeated failures")]
    fn test_cannot_submit_to_disabled_executor() {
        Spi::run(
            "CREATE FUNCTION disabled_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("disabled_executor", "disabled_exec", None);

        // Disable the executor
        Spi::run(
            "UPDATE pgswarm.executors SET health_status = 'disabled'
             WHERE name = 'disabled_executor'",
        )
        .unwrap();

        // This should fail
        let payload = pgrx::JsonB(serde_json::json!({}));
        crate::submit_job("disabled_executor", payload, 1, 0, 3, 3600, "greedy", None);
    }

    #[pg_test]
    fn test_executor_health_view() {
        Spi::run(
            "CREATE FUNCTION view_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("view_executor", "view_exec", None);

        // Check the view works
        let health_status = Spi::get_one::<String>(
            "SELECT health_status FROM pgswarm.executor_health WHERE name = 'view_executor'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(health_status, "healthy");
    }

    #[pg_test]
    #[should_panic(expected = "scheduling must be")]
    fn test_invalid_scheduling_strategy() {
        Spi::run(
            "CREATE FUNCTION sched_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("sched_executor", "sched_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        crate::submit_job("sched_executor", payload, 1, 0, 3, 3600, "invalid", None);
    }

    #[pg_test]
    fn test_create_result_table() {
        let created = crate::create_result_table("pgswarm.yolo_detections");
        assert!(created);

        // Verify the table exists with correct columns
        let col_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = 'pgswarm' AND table_name = 'yolo_detections'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(col_count, 7); // job_id, task_id, chunk_index, chunk_id, result, node_id, created_at

        // Creating again should not error (IF NOT EXISTS)
        let created_again = crate::create_result_table("pgswarm.yolo_detections");
        assert!(created_again);
    }

    #[pg_test]
    fn test_submit_job_with_result_table() {
        Spi::run(
            "CREATE FUNCTION rt_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{\"detection\": \"cat\"}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("rt_executor", "rt_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({"video": "test.mp4"}));
        let job_id = crate::submit_job(
            "rt_executor",
            payload,
            2,
            0,
            3,
            3600,
            "greedy",
            Some("pgswarm.test_results"),
        );
        assert!(job_id > 0);

        // Verify result_table is stored on the job
        let rt = Spi::get_one_with_args::<String>(
            "SELECT result_table FROM pgswarm.jobs WHERE id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(rt, "pgswarm.test_results");

        // Verify result table was created
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'pgswarm' AND table_name = 'test_results'
            )",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    #[should_panic(expected = "invalid characters")]
    fn test_result_table_sql_injection() {
        Spi::run(
            "CREATE FUNCTION inj_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("inj_executor", "inj_exec", None);

        let payload = pgrx::JsonB(serde_json::json!({}));
        crate::submit_job(
            "inj_executor",
            payload,
            1,
            0,
            3,
            3600,
            "greedy",
            Some("results; DROP TABLE pgswarm.tasks"),
        );
    }

    #[pg_test]
    fn test_source_table_workflow() {
        // 1. Create executor
        Spi::run(
            "CREATE FUNCTION src_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT payload; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("src_executor", "src_exec", None);

        // 2. Create source table
        let created = crate::create_source_table("pgswarm.video_segments");
        assert!(created);

        // 3. Insert data via swarm_insert
        let id1 = crate::swarm_insert(
            "pgswarm.video_segments",
            pgrx::JsonB(serde_json::json!({"start_sec": 0, "end_sec": 60})),
        );
        let id2 = crate::swarm_insert(
            "pgswarm.video_segments",
            pgrx::JsonB(serde_json::json!({"start_sec": 60, "end_sec": 120})),
        );
        let id3 = crate::swarm_insert(
            "pgswarm.video_segments",
            pgrx::JsonB(serde_json::json!({"start_sec": 120, "end_sec": 180})),
        );
        assert!(id1 > 0);
        assert!(id2 > 0);
        assert!(id3 > 0);

        // 4. Verify count
        let count = crate::swarm_count("pgswarm.video_segments");
        assert_eq!(count, 3);

        // 5. Submit job from source table
        let job_id = crate::submit_job_from_table(
            "src_executor",
            "pgswarm.video_segments",
            pgrx::JsonB(serde_json::json!({"format": "h264"})),
            0,
            3,
            3600,
            "greedy",
            None,
        );
        assert!(job_id > 0);

        // 6. Verify tasks were created from source rows
        let tasks: Vec<_> = crate::get_tasks(job_id).collect();
        assert_eq!(tasks.len(), 3);

        // chunk_ids should be the source row IDs
        assert_eq!(tasks[0].2, Some(id1.to_string()));
        assert_eq!(tasks[1].2, Some(id2.to_string()));
        assert_eq!(tasks[2].2, Some(id3.to_string()));

        // Task payloads should be merged (job + row)
        let payload_str = Spi::get_one_with_args::<String>(
            "SELECT payload::text FROM pgswarm.tasks WHERE job_id = $1 AND chunk_index = 0",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        let payload_val: serde_json::Value = serde_json::from_str(&payload_str).unwrap();
        assert_eq!(payload_val["format"], "h264"); // from job payload
        assert_eq!(payload_val["start_sec"], 0); // from source row
        assert_eq!(payload_val["end_sec"], 60); // from source row
    }

    #[pg_test]
    fn test_swarm_insert_batch() {
        crate::create_source_table("pgswarm.batch_test");

        let payloads = pgrx::JsonB(serde_json::json!([
            {"frame": 1},
            {"frame": 2},
            {"frame": 3},
            {"frame": 4}
        ]));
        let count = crate::swarm_insert_batch("pgswarm.batch_test", payloads);
        assert_eq!(count, 4);

        let total = crate::swarm_count("pgswarm.batch_test");
        assert_eq!(total, 4);
    }

    #[pg_test]
    fn test_full_pipeline_source_to_result() {
        // Full pipeline: source_table → submit_job_from_table → result_table
        Spi::run(
            "CREATE FUNCTION pipe_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{\"detected\": true}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("pipe_executor", "pipe_exec", None);

        // Create source and result tables
        crate::create_source_table("pgswarm.pipe_input");
        crate::create_result_table("pgswarm.pipe_output");

        // Populate source
        crate::swarm_insert(
            "pgswarm.pipe_input",
            pgrx::JsonB(serde_json::json!({"item": "a"})),
        );
        crate::swarm_insert(
            "pgswarm.pipe_input",
            pgrx::JsonB(serde_json::json!({"item": "b"})),
        );

        // Submit job with both source and result tables
        let job_id = crate::submit_job_from_table(
            "pipe_executor",
            "pgswarm.pipe_input",
            pgrx::JsonB(serde_json::json!({})),
            0,
            3,
            3600,
            "greedy",
            Some("pgswarm.pipe_output"),
        );
        assert!(job_id > 0);

        // Verify result_table is set on job
        let rt = Spi::get_one_with_args::<String>(
            "SELECT result_table FROM pgswarm.jobs WHERE id = $1",
            &[job_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(rt, "pgswarm.pipe_output");

        // Verify both tables exist
        let input_exists = Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgswarm' AND table_name = 'pipe_input')",
        ).unwrap().unwrap();
        let output_exists = Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgswarm' AND table_name = 'pipe_output')",
        ).unwrap().unwrap();
        assert!(input_exists);
        assert!(output_exists);
    }

    #[pg_test]
    fn test_watch_source_table() {
        Spi::run(
            "CREATE FUNCTION watch_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{\"ok\": true}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("watch_executor", "watch_exec", None);

        // Create source table
        crate::create_source_table("pgswarm.watch_input");

        // Set up watch
        let watch_id = crate::watch_source_table(
            "pgswarm.watch_input",
            "watch_executor",
            pgrx::JsonB(serde_json::json!({"model": "yolov8"})),
            50,   // batch_size
            5,    // poll_interval
            0,    // priority
            3,    // max_retries
            3600, // task_timeout
            "greedy",
            Some("pgswarm.watch_output"),
        );
        assert!(watch_id > 0);

        // Verify watch is listed
        let watches: Vec<_> = crate::list_watches().collect();
        assert_eq!(watches.len(), 1);
        assert_eq!(watches[0].1, "pgswarm.watch_input");
        assert_eq!(watches[0].2, "watch_executor");
        assert_eq!(watches[0].3, 50); // batch_size
        assert_eq!(watches[0].7, true); // enabled
    }

    #[pg_test]
    fn test_unwatch_source_table() {
        Spi::run(
            "CREATE FUNCTION unwatch_exec(task_id BIGINT, payload JSONB, chunk_index INT, chunk_count INT)
             RETURNS JSONB AS $$ SELECT '{}'::jsonb; $$ LANGUAGE SQL",
        )
        .unwrap();
        crate::register_executor("unwatch_executor", "unwatch_exec", None);

        crate::create_source_table("pgswarm.unwatch_input");
        crate::watch_source_table(
            "pgswarm.unwatch_input",
            "unwatch_executor",
            pgrx::JsonB(serde_json::json!({})),
            100,
            5,
            0,
            3,
            3600,
            "greedy",
            None,
        );

        // Unwatch
        let removed = crate::unwatch_source_table("pgswarm.unwatch_input", "unwatch_executor");
        assert!(removed);

        // Verify disabled
        let enabled = Spi::get_one::<bool>(
            "SELECT enabled FROM pgswarm.watches
             WHERE source_table = 'pgswarm.unwatch_input' AND executor_name = 'unwatch_executor'",
        )
        .unwrap()
        .unwrap();
        assert!(!enabled);

        // Unwatching again returns false
        let removed_again =
            crate::unwatch_source_table("pgswarm.unwatch_input", "unwatch_executor");
        assert!(!removed_again);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
