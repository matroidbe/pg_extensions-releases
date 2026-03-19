//! pg_streaming - Declarative stream processing engine inside PostgreSQL
//!
//! This extension provides a declarative pipeline DSL for building stream
//! processing pipelines that run as PostgreSQL background workers. Pipelines
//! read from input connectors (Kafka topics, tables, MQTT), apply processor
//! chains (filter, map, aggregate, window, join, CEP), and write to output
//! connectors. All state lives in PostgreSQL tables — queryable, durable,
//! and transactional.

#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]

use pgrx::prelude::*;

mod config;
mod connector;
mod dsl;
mod engine;
mod pipeline;
mod processor;
mod record;
mod worker;

// Re-export worker entry points so they appear as dynamic symbols
pub use worker::{
    pg_streaming_coordinator_main, pg_streaming_executor_main, pg_streaming_timer_main,
};

// =============================================================================
// SQL Functions — Pipeline CRUD and Lifecycle
// =============================================================================

/// Create a new streaming pipeline from a JSON definition
#[pg_extern]
fn create_pipeline(name: &str, definition: pgrx::JsonB) -> i32 {
    pipeline::crud::create_pipeline_impl(name, definition)
}

/// Update a pipeline definition (restarts if running)
#[pg_extern]
fn update_pipeline(name: &str, definition: pgrx::JsonB) {
    pipeline::crud::update_pipeline_impl(name, definition);
}

/// Drop a pipeline by name
#[pg_extern]
fn drop_pipeline(name: &str) {
    pipeline::crud::drop_pipeline_impl(name);
}

/// Start a pipeline
#[pg_extern]
fn start(name: &str) {
    pipeline::lifecycle::start_pipeline_impl(name);
}

/// Stop a running pipeline
#[pg_extern]
fn stop(name: &str) {
    pipeline::lifecycle::stop_pipeline_impl(name);
}

/// Restart a pipeline (stop + start)
#[pg_extern]
fn restart(name: &str) {
    pipeline::lifecycle::restart_pipeline_impl(name);
}

// =============================================================================
// SQL Functions — Observability
// =============================================================================

/// Show status of all pipelines
#[pg_extern]
#[allow(clippy::type_complexity)]
fn status() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(state, String),
        name!(worker_id, Option<i32>),
        name!(error, Option<String>),
        name!(started_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(stopped_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(uptime, Option<String>),
    ),
> {
    TableIterator::new(pipeline::observability::status_impl())
}

/// Show recent errors for a pipeline
#[pg_extern]
#[allow(clippy::type_complexity)]
fn errors(
    name: &str,
    limit: default!(i32, 10),
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(pipeline, String),
        name!(processor, Option<String>),
        name!(error, String),
        name!(record, Option<pgrx::JsonB>),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(pipeline::observability::errors_impl(name, limit))
}

/// Show recent late events for a pipeline
#[pg_extern]
#[allow(clippy::type_complexity)]
fn late_events(
    name: &str,
    limit: default!(i32, 10),
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(pipeline, String),
        name!(processor, String),
        name!(event_time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(window_start, pgrx::datum::TimestampWithTimeZone),
        name!(window_end, pgrx::datum::TimestampWithTimeZone),
        name!(watermark, pgrx::datum::TimestampWithTimeZone),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(pipeline::observability::late_events_impl(name, limit))
}

/// Show recent metrics for a pipeline
#[pg_extern]
fn metrics(
    name: &str,
    limit: default!(i32, 50),
) -> TableIterator<
    'static,
    (
        name!(pipeline, String),
        name!(metric, String),
        name!(value, f64),
        name!(measured_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(pipeline::observability::metrics_impl(name, limit))
}

/// Show connector offset lag for all pipelines
#[pg_extern]
fn lag() -> TableIterator<
    'static,
    (
        name!(pipeline, String),
        name!(connector, String),
        name!(committed_offset, i64),
        name!(updated_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(pipeline::observability::lag_impl())
}

/// Quick trace of recent error records for debugging
#[pg_extern]
fn trace(
    name: &str,
    limit: default!(i32, 5),
) -> TableIterator<
    'static,
    (
        name!(record, Option<pgrx::JsonB>),
        name!(error, String),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(pipeline::observability::trace_impl(name, limit))
}

pgrx::pg_module_magic!();

// =============================================================================
// Bootstrap SQL — creates pgstreams schema tables
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Pipeline definitions
CREATE TABLE pgstreams.pipelines (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    definition  JSONB NOT NULL,
    state       TEXT NOT NULL DEFAULT 'created'
                CHECK (state IN ('created', 'running', 'stopped', 'failed')),
    worker_id   INT,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at  TIMESTAMPTZ,
    stopped_at  TIMESTAMPTZ
);

-- Pipeline version history
CREATE TABLE pgstreams.pipeline_versions (
    id          SERIAL PRIMARY KEY,
    pipeline_id INT NOT NULL REFERENCES pgstreams.pipelines(id) ON DELETE CASCADE,
    version     INT NOT NULL,
    definition  JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version)
);

-- Registry of auto-created state tables
CREATE TABLE pgstreams.state_tables (
    id          SERIAL PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    processor   TEXT NOT NULL,
    table_name  TEXT NOT NULL UNIQUE,
    table_type  TEXT NOT NULL
                CHECK (table_type IN ('aggregate', 'window', 'join_buffer', 'cep', 'dedupe')),
    definition  JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reusable processor/input/output definitions
CREATE TABLE pgstreams.resources (
    id          SERIAL PRIMARY KEY,
    type        TEXT NOT NULL CHECK (type IN ('processor', 'input', 'output')),
    name        TEXT NOT NULL,
    definition  JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (type, name)
);

-- Offset tracking for non-kafka input connectors
CREATE TABLE pgstreams.connector_offsets (
    id            SERIAL PRIMARY KEY,
    pipeline      TEXT NOT NULL,
    connector     TEXT NOT NULL,
    offset_value  BIGINT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline, connector)
);

-- Error log / dead letter records
CREATE TABLE pgstreams.error_log (
    id          BIGSERIAL PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    processor   TEXT,
    error       TEXT NOT NULL,
    record      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_error_log_pipeline ON pgstreams.error_log (pipeline, created_at);

-- Late events log for window processors
CREATE TABLE pgstreams.late_events (
    id            BIGSERIAL PRIMARY KEY,
    pipeline      TEXT NOT NULL,
    processor     TEXT NOT NULL,
    record        JSONB NOT NULL,
    event_time    TIMESTAMPTZ,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    watermark     TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_late_events_pipeline ON pgstreams.late_events (pipeline, created_at);

-- Pipeline metrics snapshots
CREATE TABLE pgstreams.metrics (
    id          BIGSERIAL PRIMARY KEY,
    pipeline    TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_metrics_pipeline ON pgstreams.metrics (pipeline, measured_at);
"#,
    name = "bootstrap_tables",
    bootstrap
);

// =============================================================================
// Extension initialization
// =============================================================================

use crate::config::{
    PG_STREAMING_BATCH_SIZE, PG_STREAMING_CHECKPOINT_INTERVAL_MS, PG_STREAMING_DATABASE,
    PG_STREAMING_ENABLED, PG_STREAMING_METRICS_ENABLED, PG_STREAMING_POLL_INTERVAL_MS,
    PG_STREAMING_WORKER_COUNT,
};
use pgrx::bgworkers::*;
use std::time::Duration;

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_bool_guc(
        c"pg_streaming.enabled",
        c"Enable the stream processing engine",
        c"When true, pg_streaming workers start automatically with PostgreSQL",
        &PG_STREAMING_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_streaming.worker_count",
        c"Number of executor workers",
        c"Each executor processes assigned pipelines. Requires restart to take effect.",
        &PG_STREAMING_WORKER_COUNT,
        1,
        32,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_streaming.batch_size",
        c"Records per poll batch",
        c"Maximum number of records to fetch from input in a single poll",
        &PG_STREAMING_BATCH_SIZE,
        1,
        100000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_streaming.poll_interval_ms",
        c"Poll interval in milliseconds",
        c"How often executor workers poll for new input records",
        &PG_STREAMING_POLL_INTERVAL_MS,
        1,
        60000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_streaming.checkpoint_interval_ms",
        c"Checkpoint interval in milliseconds",
        c"How often to commit offsets and flush state",
        &PG_STREAMING_CHECKPOINT_INTERVAL_MS,
        100,
        300000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_streaming.metrics_enabled",
        c"Enable metrics collection",
        c"When true, collects pipeline metrics into pgstreams.metrics table",
        &PG_STREAMING_METRICS_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_streaming.database",
        c"Database for pg_streaming to connect to",
        c"The database where pg_streaming extension is installed. Defaults to 'postgres'.",
        &PG_STREAMING_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Register coordinator worker (1)
    BackgroundWorkerBuilder::new("pg_streaming coordinator")
        .set_function("pg_streaming_coordinator_main")
        .set_library("pg_streaming")
        .set_argument(Some(pg_sys::Datum::from(0i64)))
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();

    // Register executor workers (N)
    let worker_count = PG_STREAMING_WORKER_COUNT.get();
    for i in 0..worker_count {
        BackgroundWorkerBuilder::new(&format!("pg_streaming executor {}", i))
            .set_function("pg_streaming_executor_main")
            .set_library("pg_streaming")
            .set_argument(Some(pg_sys::Datum::from(i as i64)))
            .enable_shmem_access(None)
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }

    // Register timer worker (1)
    BackgroundWorkerBuilder::new("pg_streaming timer")
        .set_function("pg_streaming_timer_main")
        .set_library("pg_streaming")
        .set_argument(Some(pg_sys::Datum::from(0i64)))
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        // Verify the pipelines table exists and is queryable
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.pipelines");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_pipeline_versions_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.pipeline_versions");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_state_tables_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.state_tables");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_resources_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.resources");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_connector_offsets_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.connector_offsets");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_error_log_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.error_log");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_late_events_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.late_events");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_metrics_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.metrics");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_pipeline_state_constraint() {
        // Valid states should work
        Spi::run(
            "INSERT INTO pgstreams.pipelines (name, definition, state) VALUES ('test', '{}', 'created')"
        ).unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgstreams.pipelines WHERE name = 'test'",
        );
        assert_eq!(count, Ok(Some(1)));

        // Verify the CHECK constraint exists
        let has_constraint = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conname = 'pipelines_state_check' AND contype = 'c')"
        );
        assert_eq!(has_constraint, Ok(Some(true)));
    }

    #[pg_test]
    fn test_pipeline_name_unique() {
        Spi::run("INSERT INTO pgstreams.pipelines (name, definition) VALUES ('unique-test', '{}')")
            .unwrap();

        // Verify the unique constraint exists
        let has_constraint = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE tablename = 'pipelines' AND indexname = 'pipelines_name_key')"
        );
        assert_eq!(has_constraint, Ok(Some(true)));
    }

    // =========================================================================
    // Phase 2: Pipeline CRUD + Lifecycle tests
    // =========================================================================

    #[pg_test]
    fn test_create_pipeline() {
        let id = Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('test-pipe', '{
                \"input\": {\"kafka\": {\"topic\": \"orders\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"out\"}}
            }'::jsonb)",
        );
        assert!(id.unwrap().unwrap() > 0);

        // Verify it's in the table
        let state = Spi::get_one::<String>(
            "SELECT state FROM pgstreams.pipelines WHERE name = 'test-pipe'",
        );
        assert_eq!(state, Ok(Some("created".to_string())));
    }

    #[pg_test]
    fn test_create_pipeline_with_filter() {
        let id = Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('filter-pipe', '{
                \"input\": {\"kafka\": {\"topic\": \"orders\"}},
                \"pipeline\": {\"processors\": [
                    {\"filter\": \"value_json->>''region'' = ''US''\"}
                ]},
                \"output\": {\"kafka\": {\"topic\": \"us-orders\"}}
            }'::jsonb)",
        );
        assert!(id.unwrap().unwrap() > 0);
    }

    #[pg_test]
    fn test_create_pipeline_with_mapping() {
        let id = Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('mapping-pipe', '{
                \"input\": {\"kafka\": {\"topic\": \"orders\"}},
                \"pipeline\": {\"processors\": [
                    {\"mapping\": {\"order_id\": \"value_json->>''id''\", \"total\": \"(value_json->>''amount'')::numeric\"}}
                ]},
                \"output\": {\"kafka\": {\"topic\": \"mapped\"}}
            }'::jsonb)"
        );
        assert!(id.unwrap().unwrap() > 0);
    }

    #[pg_test]
    fn test_create_pipeline_version_created() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('versioned', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        let version = Spi::get_one::<i32>(
            "SELECT version FROM pgstreams.pipeline_versions pv \
             JOIN pgstreams.pipelines p ON p.id = pv.pipeline_id \
             WHERE p.name = 'versioned'",
        );
        assert_eq!(version, Ok(Some(1)));
    }

    #[pg_test]
    fn test_drop_pipeline() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('to-drop', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        Spi::run("SELECT pgstreams.drop_pipeline('to-drop')").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgstreams.pipelines WHERE name = 'to-drop'",
        );
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_start_stop_pipeline() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('lifecycle', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        // Start
        Spi::run("SELECT pgstreams.start('lifecycle')").unwrap();
        let state = Spi::get_one::<String>(
            "SELECT state FROM pgstreams.pipelines WHERE name = 'lifecycle'",
        );
        assert_eq!(state, Ok(Some("running".to_string())));

        // Stop
        Spi::run("SELECT pgstreams.stop('lifecycle')").unwrap();
        let state = Spi::get_one::<String>(
            "SELECT state FROM pgstreams.pipelines WHERE name = 'lifecycle'",
        );
        assert_eq!(state, Ok(Some("stopped".to_string())));
    }

    #[pg_test]
    fn test_restart_pipeline() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('restart-test', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        Spi::run("SELECT pgstreams.start('restart-test')").unwrap();
        Spi::run("SELECT pgstreams.restart('restart-test')").unwrap();

        let state = Spi::get_one::<String>(
            "SELECT state FROM pgstreams.pipelines WHERE name = 'restart-test'",
        );
        assert_eq!(state, Ok(Some("running".to_string())));
    }

    #[pg_test]
    fn test_start_from_failed_state() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('failed-pipe', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        // Manually set to failed state
        Spi::run(
            "UPDATE pgstreams.pipelines SET state = 'failed', error = 'test error' WHERE name = 'failed-pipe'"
        ).unwrap();

        // Should be able to start from failed state
        Spi::run("SELECT pgstreams.start('failed-pipe')").unwrap();
        let state = Spi::get_one::<String>(
            "SELECT state FROM pgstreams.pipelines WHERE name = 'failed-pipe'",
        );
        assert_eq!(state, Ok(Some("running".to_string())));

        // Error should be cleared
        let error = Spi::get_one::<String>(
            "SELECT error FROM pgstreams.pipelines WHERE name = 'failed-pipe'",
        );
        assert_eq!(error, Ok(None));
    }

    #[pg_test]
    fn test_drop_output() {
        let id = Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('drop-test', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": [{\"filter\": \"true\"}]},
                \"output\": {\"drop\": {}}
            }'::jsonb)",
        );
        assert!(id.unwrap().unwrap() > 0);
    }

    // =========================================================================
    // Observability function tests
    // =========================================================================

    #[pg_test]
    fn test_status_empty() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.status()");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_status_with_pipeline() {
        Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('status-test', '{
                \"input\": {\"kafka\": {\"topic\": \"t\"}},
                \"pipeline\": {\"processors\": []},
                \"output\": {\"kafka\": {\"topic\": \"o\"}}
            }'::jsonb)",
        )
        .unwrap();

        let row = Spi::get_two::<String, String>(
            "SELECT name, state FROM pgstreams.status() WHERE name = 'status-test'",
        );
        let (name, state) = row.unwrap();
        assert_eq!(name, Some("status-test".to_string()));
        assert_eq!(state, Some("created".to_string()));
    }

    #[pg_test]
    fn test_errors_empty() {
        let count =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.errors('nonexistent')");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_errors_with_data() {
        Spi::run(
            "INSERT INTO pgstreams.error_log (pipeline, processor, error, record) \
             VALUES ('err-pipe', 'filter', 'bad expression', '{\"key\": 1}'::jsonb)",
        )
        .unwrap();

        let row = Spi::get_two::<String, String>(
            "SELECT pipeline, error FROM pgstreams.errors('err-pipe', 10)",
        );
        let (pipeline, error) = row.unwrap();
        assert_eq!(pipeline, Some("err-pipe".to_string()));
        assert_eq!(error, Some("bad expression".to_string()));
    }

    #[pg_test]
    fn test_lag_empty() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.lag()");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_trace_empty() {
        let count =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.trace('nonexistent')");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_unnest_pipeline() {
        let id = Spi::get_one::<i32>(
            "SELECT pgstreams.create_pipeline('unnest-test', '{
                \"input\": {\"kafka\": {\"topic\": \"orders\"}},
                \"pipeline\": {\"processors\": [
                    {\"unnest\": {\"array\": \"value_json->''items''\", \"as\": \"item\"}}
                ]},
                \"output\": {\"drop\": {}}
            }'::jsonb)",
        );
        assert!(id.unwrap().unwrap() > 0);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
