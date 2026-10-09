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
// `connector`, `processor`, `record` are public to allow companion
// extensions (e.g., `pg_streaming_xarray`) to register custom
// sinks/sources/processors via the SDK.
pub mod connector;
mod dsl;
mod engine;
mod pipeline;
pub mod processor;
pub mod record;
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

/// Rewind a stopped pipeline's source: re-read everything (`after => NULL`) or
/// everything after a path (`after => 'source/arrival_date=2026-10-01'`).
/// Start the pipeline afterwards to run the replay.
#[pg_extern]
fn replay(name: &str, after: default!(Option<&str>, "NULL")) {
    pipeline::lifecycle::replay_pipeline_impl(name, after);
}

// =============================================================================
// SQL Functions — Secrets management
// =============================================================================

/// Create or replace a secret referenced by connector configs as `${secret:NAME}`.
#[pg_extern]
fn set_secret(name: &str, value: &str, description: default!(Option<&str>, "NULL")) {
    connector::secrets::set_secret_impl(name, value, description);
}

/// Drop a secret. Returns true if it existed.
#[pg_extern]
fn drop_secret(name: &str) -> bool {
    connector::secrets::drop_secret_impl(name)
}

/// List secret names (never values). Returns (name, description, created_at).
#[pg_extern]
fn list_secrets() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(description, Option<String>),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(connector::secrets::list_secrets_impl())
}

// =============================================================================
// SQL Functions — Registry introspection
//
// Diagnostic view of what custom connectors are registered in the
// process-global SDK registries. Useful for verifying that a
// companion extension's _PG_init (e.g., pg_streaming's xarray
// feature) actually registered its plugins.
// =============================================================================

/// List custom input source names registered via the SDK.
#[pg_extern]
fn list_custom_sources() -> TableIterator<'static, (name!(name, String),)> {
    TableIterator::new(
        connector::registry::list_sources()
            .into_iter()
            .map(|n| (n,)),
    )
}

/// List custom output sink names registered via the SDK
/// (covers both async and sync sinks).
#[pg_extern]
fn list_custom_sinks() -> TableIterator<'static, (name!(name, String),)> {
    TableIterator::new(connector::registry::list_sinks().into_iter().map(|n| (n,)))
}

/// List custom processor names registered via the SDK.
#[pg_extern]
fn list_custom_processors() -> TableIterator<'static, (name!(name, String),)> {
    TableIterator::new(
        connector::registry::list_processors()
            .into_iter()
            .map(|n| (n,)),
    )
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

-- Generic cursor state for non-Kafka connectors (opendal, http_paginated,
-- webhook, ldes, custom). Per pipeline + connector role.
CREATE TABLE pgstreams.connector_state (
    pipeline       TEXT NOT NULL,
    connector_role TEXT NOT NULL,
    cursor         JSONB NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, connector_role)
);

-- Named secrets referenced by connector configs as ${secret:NAME}.
-- Values returned ONLY to engine code; never via observability functions.
CREATE TABLE pgstreams.secrets (
    name        TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- LDES member-IRI dedupe (used by the LDES source connector).
CREATE TABLE pgstreams.ldes_seen_members (
    stream_url    TEXT NOT NULL,
    member_iri    TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_url, member_iri)
);

-- Per-record isolation for the `call` output connector.
--
-- Runs one record's function call inside a subtransaction: the PL/pgSQL
-- EXCEPTION block is exactly BeginInternalSubTransaction /
-- RollbackAndReleaseCurrentSubTransaction. Returns NULL on success, or the
-- SQLSTATE and message on a raise, so the caller can route that one record to
-- the dead-letter output and keep going with the rest of the batch.
--
-- Caveat: non-transactional side effects inside the called function (dblink,
-- the http extension, NOTIFY — anything reaching outside the database) are NOT
-- rolled back by the subtransaction.
-- `assume_role` is applied HERE rather than around the call site, so it wraps
-- only the user function. Setting it outside would leave the connector calling
-- this very function as the application role, which has no rights on the
-- pgstreams schema — the pipeline then fails with `permission denied for schema
-- pgstreams` before the user function is ever reached.
--
-- On error the subtransaction rollback restores the role for free; on success
-- it is reset explicitly.
CREATE FUNCTION pgstreams.call_guarded(query TEXT, payload JSONB, assume_role TEXT DEFAULT NULL)
RETURNS JSONB
LANGUAGE plpgsql
AS $call_guarded$
BEGIN
    IF assume_role IS NOT NULL THEN
        EXECUTE format('SET LOCAL ROLE %I', assume_role);
    END IF;
    EXECUTE query USING payload;
    IF assume_role IS NOT NULL THEN
        RESET ROLE;
    END IF;
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object('sqlstate', SQLSTATE, 'message', SQLERRM);
END;
$call_guarded$;

COMMENT ON FUNCTION pgstreams.call_guarded(TEXT, JSONB, TEXT) IS
    'Runs one `call` output-connector invocation inside a subtransaction, optionally as `assume_role`; returns NULL on success or {sqlstate, message} on error.';
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
    worker::SUPERVISOR.init();

    // Built-in xarray connectors (only when `--features xarray`). These
    // would normally live in a separate pgrx extension but two pgrx
    // cdylibs can't statically link each other (Pg_magic_func / _PG_init
    // collide), so we fold them in here behind a feature flag. Pipelines
    // reference them in the DSL as { "custom": { "name": "..." } }.
    #[cfg(feature = "xarray")]
    {
        connector::registry::register_sync_sink(
            "xarray_index",
            connector::output::xarray_index::factory,
        );
        connector::registry::register_processor("xarray_header", processor::xarray_header::factory);
    }

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

    // =========================================================================
    // Phase 1: Connector framework — state + secrets infrastructure
    // =========================================================================

    #[pg_test]
    fn test_connector_state_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.connector_state");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_secrets_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.secrets");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_ldes_seen_members_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgstreams.ldes_seen_members");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_set_secret_and_list() {
        Spi::run("SELECT pgstreams.set_secret('api_token', 'sek-42', 'test token')").unwrap();

        let row = Spi::get_two::<String, String>(
            "SELECT name, description FROM pgstreams.list_secrets() WHERE name = 'api_token'",
        );
        let (name, desc) = row.unwrap();
        assert_eq!(name, Some("api_token".to_string()));
        assert_eq!(desc, Some("test token".to_string()));

        // list_secrets must NOT expose the value.
        let value =
            Spi::get_one::<String>("SELECT value FROM pgstreams.secrets WHERE name = 'api_token'");
        assert_eq!(value, Ok(Some("sek-42".to_string())));
    }

    #[pg_test]
    fn test_set_secret_overwrites() {
        Spi::run("SELECT pgstreams.set_secret('rotate_me', 'v1', NULL)").unwrap();
        Spi::run("SELECT pgstreams.set_secret('rotate_me', 'v2', NULL)").unwrap();

        let value =
            Spi::get_one::<String>("SELECT value FROM pgstreams.secrets WHERE name = 'rotate_me'");
        assert_eq!(value, Ok(Some("v2".to_string())));
    }

    #[pg_test]
    fn test_drop_secret_returns_true_if_existed() {
        Spi::run("SELECT pgstreams.set_secret('temp', 'x', NULL)").unwrap();
        let existed = Spi::get_one::<bool>("SELECT pgstreams.drop_secret('temp')").unwrap();
        assert_eq!(existed, Some(true));

        let count = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgstreams.secrets WHERE name = 'temp'",
        );
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_drop_secret_returns_false_if_missing() {
        let existed =
            Spi::get_one::<bool>("SELECT pgstreams.drop_secret('nonexistent_xyz')").unwrap();
        assert_eq!(existed, Some(false));
    }

    fn replay_fixture() {
        Spi::run(
            "SELECT pgstreams.create_pipeline('rz', '{\"input\": {\"opendal\": \
             {\"service\": \"fs\", \"path\": \"x/*.parquet\", \"parse_as\": \"parquet\", \
              \"order\": \"lexicographic\"}}, \"pipeline\": {\"processors\": []}, \
             \"output\": {\"drop\": {}}}'::jsonb)",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO pgstreams.connector_state (pipeline, connector_role, cursor) \
             VALUES ('rz', 'input', '{\"after\": \"x/2026-10-08/09.parquet\"}'::jsonb)",
        )
        .unwrap();
    }

    #[pg_test]
    fn test_replay_from_path_rewinds_the_cursor() {
        replay_fixture();
        Spi::run("SELECT pgstreams.replay('rz', 'x/2026-10-01')").unwrap();
        let cursor = Spi::get_one::<pgrx::JsonB>(
            "SELECT cursor FROM pgstreams.connector_state WHERE pipeline = 'rz'",
        )
        .unwrap();
        assert_eq!(
            cursor.unwrap().0,
            serde_json::json!({"after": "x/2026-10-01"})
        );
    }

    #[pg_test]
    fn test_replay_without_path_forgets_the_cursor() {
        replay_fixture();
        Spi::run("SELECT pgstreams.replay('rz')").unwrap();
        let n = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgstreams.connector_state WHERE pipeline = 'rz'",
        );
        assert_eq!(n, Ok(Some(0)));
    }

    #[pg_test(error = "Pipeline 'rz' is running; stop it first: SELECT pgstreams.stop('rz')")]
    fn test_replay_refuses_a_running_pipeline() {
        replay_fixture();
        Spi::run("UPDATE pgstreams.pipelines SET state = 'running' WHERE name = 'rz'").unwrap();
        Spi::run("SELECT pgstreams.replay('rz')").unwrap();
    }

    #[pg_test]
    fn test_connector_state_upsert() {
        Spi::run(
            "INSERT INTO pgstreams.connector_state (pipeline, connector_role, cursor) \
             VALUES ('p1', 'input', '42'::jsonb)",
        )
        .unwrap();

        Spi::run(
            "INSERT INTO pgstreams.connector_state (pipeline, connector_role, cursor) \
             VALUES ('p1', 'input', '100'::jsonb) \
             ON CONFLICT (pipeline, connector_role) \
             DO UPDATE SET cursor = EXCLUDED.cursor",
        )
        .unwrap();

        let cursor = Spi::get_one::<pgrx::JsonB>(
            "SELECT cursor FROM pgstreams.connector_state \
             WHERE pipeline = 'p1' AND connector_role = 'input'",
        )
        .unwrap();
        assert_eq!(cursor.unwrap().0, serde_json::json!(100));
    }

    // =========================================================================
    // call output connector — pgstreams.call_guarded (per-record isolation)
    // =========================================================================

    /// Create a function that raises on every third record, plus the sink
    /// table it writes to. Mirrors the design doc's first test case.
    fn setup_call_target() {
        Spi::run(
            r#"
            CREATE TABLE landed (payload jsonb);
            CREATE FUNCTION ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
            BEGIN
                IF (rec->>'n')::int % 3 = 0 THEN
                    RAISE EXCEPTION 'poison record %', rec->>'n' USING ERRCODE = '22000';
                END IF;
                INSERT INTO landed VALUES (rec);
            END;
            $fn$;
            "#,
        )
        .unwrap();
    }

    /// The SQL the call connector builds for `args: ["record"]`.
    const CALL_SQL: &str = "SELECT ingest(r) FROM jsonb_array_elements($1) AS r";

    #[pg_test]
    fn test_call_guarded_returns_null_on_success() {
        setup_call_target();

        let err = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT pgstreams.call_guarded($1, $2)",
            &[
                CALL_SQL.into(),
                pgrx::JsonB(serde_json::json!([{"n": 1}])).into(),
            ],
        )
        .unwrap();
        assert!(err.is_none(), "successful call should return NULL");

        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM landed").unwrap();
        assert_eq!(count, Some(1));
    }

    #[pg_test]
    fn test_call_guarded_returns_sqlstate_and_message_on_raise() {
        setup_call_target();

        let err = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT pgstreams.call_guarded($1, $2)",
            &[
                CALL_SQL.into(),
                pgrx::JsonB(serde_json::json!([{"n": 3}])).into(),
            ],
        )
        .unwrap()
        .expect("failed call should return an error object");

        assert_eq!(err.0["sqlstate"], "22000");
        assert!(err.0["message"]
            .as_str()
            .unwrap()
            .contains("poison record 3"));
    }

    /// The whole point of the subtransaction: one poison record must not take
    /// the good records with it, and the caller can keep going afterwards.
    #[pg_test]
    fn test_call_guarded_isolates_failures_per_record() {
        setup_call_target();

        let mut failed = 0;
        for n in 1..=9 {
            let err = Spi::get_one_with_args::<pgrx::JsonB>(
                "SELECT pgstreams.call_guarded($1, $2)",
                &[
                    CALL_SQL.into(),
                    pgrx::JsonB(serde_json::json!([{"n": n}])).into(),
                ],
            )
            .unwrap();
            if err.is_some() {
                failed += 1;
            }
        }

        // 3, 6, 9 raise; 1, 2, 4, 5, 7, 8 land. 2/3 through, 1/3 rejected.
        assert_eq!(failed, 3);
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM landed").unwrap();
        assert_eq!(count, Some(6), "good records must survive the poison ones");
    }

    /// A rolled-back record must not leave partial writes behind.
    #[pg_test]
    fn test_call_guarded_rolls_back_partial_writes() {
        Spi::run(
            r#"
            CREATE TABLE landed (payload jsonb);
            CREATE FUNCTION ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
            BEGIN
                INSERT INTO landed VALUES (rec);
                RAISE EXCEPTION 'after the insert';
            END;
            $fn$;
            "#,
        )
        .unwrap();

        let err = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT pgstreams.call_guarded($1, $2)",
            &[
                CALL_SQL.into(),
                pgrx::JsonB(serde_json::json!([{"n": 1}])).into(),
            ],
        )
        .unwrap();
        assert!(err.is_some());

        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM landed").unwrap();
        assert_eq!(count, Some(0), "the insert must roll back with the record");
    }

    /// set_config is applied outside the subtransaction, so a rolled-back
    /// record must not strip the setting from the record that follows.
    #[pg_test]
    fn test_set_config_survives_a_rolled_back_record() {
        Spi::run(
            r#"
            CREATE TABLE landed (role text);
            CREATE FUNCTION ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
            BEGIN
                IF (rec->>'n')::int = 1 THEN RAISE EXCEPTION 'boom'; END IF;
                INSERT INTO landed VALUES (current_setting('app.user_role'));
            END;
            $fn$;
            "#,
        )
        .unwrap();

        Spi::run("SELECT set_config('app.user_role', 'ingest_service', true)").unwrap();

        // Record 1 raises and rolls back.
        let err = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT pgstreams.call_guarded($1, $2)",
            &[
                CALL_SQL.into(),
                pgrx::JsonB(serde_json::json!([{"n": 1}])).into(),
            ],
        )
        .unwrap();
        assert!(err.is_some());

        // Record 2 must still see the setting.
        let err = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT pgstreams.call_guarded($1, $2)",
            &[
                CALL_SQL.into(),
                pgrx::JsonB(serde_json::json!([{"n": 2}])).into(),
            ],
        )
        .unwrap();
        assert!(err.is_none(), "unexpected error: {:?}", err.map(|e| e.0));

        let role = Spi::get_one::<String>("SELECT role FROM landed").unwrap();
        assert_eq!(role.as_deref(), Some("ingest_service"));
    }

    // =========================================================================
    // call output connector — compile-time function resolution
    // =========================================================================

    fn call_output(function: &str, args: &[&str]) -> crate::dsl::types::CallOutputConfig {
        crate::dsl::types::CallOutputConfig {
            function: function.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            set_config: Default::default(),
            set_role: None,
            on_record_error: crate::dsl::types::OnRecordError::DeadLetter,
            batch: false,
        }
    }

    fn verify(function: &str, args: &[&str]) -> Result<(), String> {
        crate::connector::output::call::CallOutput::new(
            &call_output(function, args),
            crate::record::TopicShape::Messages.batch_cte().as_str(),
            "test_pipeline",
            None,
        )
        .verify_function_exists()
    }

    #[pg_test]
    fn test_verify_function_exists_qualified() {
        Spi::run(
            "CREATE SCHEMA app; \
             CREATE FUNCTION app.ingest(rec jsonb) RETURNS void LANGUAGE sql AS $fn$ SELECT $fn$;",
        )
        .unwrap();
        assert!(verify("app.ingest", &["record"]).is_ok());
    }

    #[pg_test]
    fn test_verify_function_missing_is_rejected() {
        let err = verify("app.no_such_function", &["record"]).unwrap_err();
        assert!(err.contains("does not exist"), "got: {}", err);
    }

    #[pg_test]
    fn test_verify_function_arity_mismatch_is_rejected() {
        Spi::run(
            "CREATE FUNCTION ingest(rec jsonb) RETURNS void LANGUAGE sql AS $fn$ SELECT $fn$;",
        )
        .unwrap();
        let err = verify("ingest", &["record", "record"]).unwrap_err();
        assert!(err.contains("2 argument(s)"), "got: {}", err);
    }

    #[pg_test]
    fn test_verify_function_unqualified_uses_search_path() {
        Spi::run(
            "CREATE FUNCTION ingest(rec jsonb) RETURNS void LANGUAGE sql AS $fn$ SELECT $fn$;",
        )
        .unwrap();
        assert!(verify("ingest", &["record"]).is_ok());
    }

    // =========================================================================
    // "not found" lookups must say so
    //
    // `Spi::get_one` on a query returning ZERO rows fails with
    // "SpiTupleTable positioned before the start" rather than yielding
    // Ok(None). Any lookup written as `SELECT col FROM t WHERE ...` therefore
    // reports a plain miss as an internal SPI error, and whatever nice message
    // the Ok(None) arm carried is dead code. Both call sites below now use a
    // scalar subquery so a miss is one row of NULL.
    // =========================================================================

    #[pg_test]
    fn test_missing_secret_reports_unknown_secret() {
        let err = crate::connector::secrets::resolve(&serde_json::json!({
            "token": "${secret:no_such_secret}"
        }))
        .expect_err("an undefined secret must not resolve");

        assert!(
            err.contains("Unknown secret"),
            "expected 'Unknown secret', got: {}",
            err
        );
        assert!(
            !err.contains("positioned before the start"),
            "SPI internals leaked into the message: {}",
            err
        );
    }

    #[pg_test]
    fn test_existing_secret_still_resolves() {
        Spi::run("SELECT pgstreams.set_secret('sx_token', 'sekrit')").unwrap();
        let resolved = crate::connector::secrets::resolve(&serde_json::json!({
            "token": "${secret:sx_token}"
        }))
        .unwrap();
        assert_eq!(resolved["token"], "sekrit");
    }

    /// Defaulted parameters may absorb the arity difference.
    #[pg_test]
    fn test_verify_function_accepts_defaulted_args() {
        Spi::run(
            "CREATE FUNCTION ingest(rec jsonb, src text DEFAULT 'stream') \
             RETURNS void LANGUAGE sql AS $fn$ SELECT $fn$;",
        )
        .unwrap();
        assert!(verify("ingest", &["record"]).is_ok());
        assert!(verify("ingest", &["record", "'kafka'"]).is_ok());
        assert!(verify("ingest", &["record", "'a'", "'b'"]).is_err());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}

/// The settings this extension's bottle ships (pgbrew.toml) are the code's
/// defaults, so `pgx install --configure` writes nothing surprising
/// (design/bgworker-config).
#[cfg(test)]
mod pgbrew_manifest_tests {
    #[test]
    fn declared_settings_match_code_defaults() {
        let manifest: toml::Table = include_str!("../pgbrew.toml").parse().expect("pgbrew.toml");
        let declared = manifest["postgresql"]
            .get("settings")
            .and_then(|s| s.as_table())
            .expect("pgbrew.toml declares [postgresql.settings]");
        let expected: Vec<(&str, String)> = vec![(
            "pg_streaming.database",
            pg_bgworker::DEFAULT_DATABASE.to_string(),
        )];
        let mut keys: Vec<&str> = declared.keys().map(String::as_str).collect();
        keys.sort_unstable();
        let mut want: Vec<&str> = expected.iter().map(|(k, _)| *k).collect();
        want.sort_unstable();
        assert_eq!(keys, want, "settings declared in pgbrew.toml");
        for (key, default) in expected {
            assert_eq!(declared[key].as_str(), Some(default.as_str()), "{key}");
        }
    }
}
