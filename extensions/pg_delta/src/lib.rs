//! pg_delta - Delta Lake streaming integration for PostgreSQL
//!
//! Stream data between PostgreSQL and Delta Lake tables. Enables Postgres
//! to participate in lakehouse architectures as both a source and sink
//! for analytical data.

#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::ffi::CString;
use std::time::Duration;

mod storage;
mod streams;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings - Storage Configuration
// =============================================================================

// Worker settings
static PG_DELTA_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);
static PG_DELTA_WORKER_LOG_LEVEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

// Default stream settings
static PG_DELTA_POLL_INTERVAL_MS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(10000);
static PG_DELTA_BATCH_SIZE: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(10000);
static PG_DELTA_FLUSH_INTERVAL_MS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(60000);

// AWS S3 Configuration
static PG_DELTA_AWS_REGION: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AWS_ACCESS_KEY_ID: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AWS_SECRET_ACCESS_KEY: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AWS_SESSION_TOKEN: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AWS_ENDPOINT: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AWS_USE_INSTANCE_PROFILE: pgrx::GucSetting<bool> =
    pgrx::GucSetting::<bool>::new(false);
static PG_DELTA_AWS_ALLOW_HTTP: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(false);

// Azure Configuration
static PG_DELTA_AZURE_STORAGE_ACCOUNT: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AZURE_STORAGE_KEY: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AZURE_SAS_TOKEN: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_AZURE_USE_MANAGED_IDENTITY: pgrx::GucSetting<bool> =
    pgrx::GucSetting::<bool>::new(false);

// GCS Configuration
static PG_DELTA_GCS_SERVICE_ACCOUNT_PATH: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_GCS_SERVICE_ACCOUNT_KEY: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);
static PG_DELTA_GCS_USE_DEFAULT_CREDENTIALS: pgrx::GucSetting<bool> =
    pgrx::GucSetting::<bool>::new(false);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUCs
    register_gucs();

    // Register background worker
    BackgroundWorkerBuilder::new("pg_delta stream manager")
        .set_function("pg_delta_worker_main")
        .set_library("pg_delta")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

fn register_gucs() {
    // Worker settings
    pgrx::GucRegistry::define_bool_guc(
        c"delta.enabled",
        c"Enable the Delta Lake stream manager",
        c"When true, pg_delta starts the background worker to manage streams",
        &PG_DELTA_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.worker_log_level",
        c"Log level for the stream manager worker",
        c"One of: debug, info, warn, error",
        &PG_DELTA_WORKER_LOG_LEVEL,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Default stream settings
    pgrx::GucRegistry::define_int_guc(
        c"delta.poll_interval_ms",
        c"Default poll interval for ingest streams in milliseconds",
        c"How often to check Delta tables for new versions",
        &PG_DELTA_POLL_INTERVAL_MS,
        100,    // min: 100ms
        600000, // max: 10 minutes
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"delta.batch_size",
        c"Default batch size for export streams",
        c"Number of rows to buffer before writing to Delta",
        &PG_DELTA_BATCH_SIZE,
        1,       // min
        1000000, // max: 1M rows
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"delta.flush_interval_ms",
        c"Default flush interval for export streams in milliseconds",
        c"Maximum time to buffer data before writing to Delta",
        &PG_DELTA_FLUSH_INTERVAL_MS,
        1000,    // min: 1 second
        3600000, // max: 1 hour
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // AWS S3 Configuration
    pgrx::GucRegistry::define_string_guc(
        c"delta.aws_region",
        c"AWS region for S3 access",
        c"e.g., us-east-1, eu-west-1",
        &PG_DELTA_AWS_REGION,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.aws_access_key_id",
        c"AWS access key ID",
        c"Static credential for S3 access",
        &PG_DELTA_AWS_ACCESS_KEY_ID,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.aws_secret_access_key",
        c"AWS secret access key",
        c"Static credential for S3 access",
        &PG_DELTA_AWS_SECRET_ACCESS_KEY,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.aws_session_token",
        c"AWS session token",
        c"For temporary credentials",
        &PG_DELTA_AWS_SESSION_TOKEN,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.aws_endpoint",
        c"Custom S3 endpoint URL",
        c"For S3-compatible storage like MinIO",
        &PG_DELTA_AWS_ENDPOINT,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"delta.aws_use_instance_profile",
        c"Use EC2 instance profile for AWS credentials",
        c"Recommended for production on EC2/ECS",
        &PG_DELTA_AWS_USE_INSTANCE_PROFILE,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"delta.aws_allow_http",
        c"Allow HTTP (non-HTTPS) connections to S3",
        c"Required for local S3-compatible storage",
        &PG_DELTA_AWS_ALLOW_HTTP,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    // Azure Configuration
    pgrx::GucRegistry::define_string_guc(
        c"delta.azure_storage_account",
        c"Azure storage account name",
        c"Account name for Azure Blob Storage",
        &PG_DELTA_AZURE_STORAGE_ACCOUNT,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.azure_storage_key",
        c"Azure storage account key",
        c"Access key for Azure Blob Storage",
        &PG_DELTA_AZURE_STORAGE_KEY,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.azure_sas_token",
        c"Azure SAS token",
        c"Shared Access Signature for Azure Blob Storage",
        &PG_DELTA_AZURE_SAS_TOKEN,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"delta.azure_use_managed_identity",
        c"Use Azure Managed Identity for credentials",
        c"Recommended for production on Azure VMs",
        &PG_DELTA_AZURE_USE_MANAGED_IDENTITY,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    // GCS Configuration
    pgrx::GucRegistry::define_string_guc(
        c"delta.gcs_service_account_path",
        c"Path to GCS service account JSON file",
        c"Credentials file for Google Cloud Storage",
        &PG_DELTA_GCS_SERVICE_ACCOUNT_PATH,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"delta.gcs_service_account_key",
        c"GCS service account JSON key (inline)",
        c"Full JSON key content for Google Cloud Storage",
        &PG_DELTA_GCS_SERVICE_ACCOUNT_KEY,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"delta.gcs_use_default_credentials",
        c"Use GCS Application Default Credentials",
        c"Uses GOOGLE_APPLICATION_CREDENTIALS or GCE metadata",
        &PG_DELTA_GCS_USE_DEFAULT_CREDENTIALS,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );
}

// =============================================================================
// Bootstrap SQL - Schema and Tables
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Stream configuration table
CREATE TABLE delta.streams (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('ingest', 'export')),
    uri TEXT NOT NULL,
    pg_table TEXT NOT NULL,
    mode TEXT NOT NULL,
    poll_interval_ms INTEGER,
    batch_size INTEGER,
    flush_interval_ms INTEGER,
    start_version BIGINT,
    tracking_column TEXT,
    key_columns TEXT[],
    storage_options JSONB,
    enabled BOOLEAN NOT NULL DEFAULT true,
    status TEXT NOT NULL DEFAULT 'stopped' CHECK (status IN ('running', 'stopped', 'error')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream progress tracking (table-level)
CREATE TABLE delta.stream_progress (
    stream_id INTEGER PRIMARY KEY REFERENCES delta.streams(id) ON DELETE CASCADE,
    delta_version BIGINT,
    pg_lsn PG_LSN,
    last_value JSONB,
    rows_synced BIGINT NOT NULL DEFAULT 0,
    bytes_synced BIGINT NOT NULL DEFAULT 0,
    last_sync_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    error_count INTEGER NOT NULL DEFAULT 0
);

-- Partition-level progress tracking for efficient incremental sync
CREATE TABLE delta.partition_progress (
    id BIGSERIAL PRIMARY KEY,
    stream_id INTEGER NOT NULL REFERENCES delta.streams(id) ON DELETE CASCADE,
    partition_values JSONB NOT NULL,        -- {"year": 2024, "month": 1}
    partition_path TEXT NOT NULL,            -- "year=2024/month=01"
    delta_version BIGINT NOT NULL,           -- Version when this partition was last synced
    files_processed TEXT[],                  -- List of file paths already processed
    rows_synced BIGINT NOT NULL DEFAULT 0,
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(stream_id, partition_path)
);

-- Index for efficient partition lookup
CREATE INDEX idx_partition_progress_stream_path
    ON delta.partition_progress(stream_id, partition_path);

-- Index for finding stale partitions
CREATE INDEX idx_partition_progress_stream_version
    ON delta.partition_progress(stream_id, delta_version);

-- Error log for debugging
CREATE TABLE delta.stream_errors (
    id BIGSERIAL PRIMARY KEY,
    stream_id INTEGER REFERENCES delta.streams(id) ON DELETE CASCADE,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    context JSONB,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for efficient error lookup
CREATE INDEX idx_stream_errors_stream_occurred
    ON delta.stream_errors(stream_id, occurred_at DESC);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION delta.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER streams_updated_at
    BEFORE UPDATE ON delta.streams
    FOR EACH ROW EXECUTE FUNCTION delta.update_timestamp();

-- Managed Delta tables - tracks tables created via create_table()
CREATE TABLE delta.tables (
    id SERIAL PRIMARY KEY,
    pg_table TEXT UNIQUE NOT NULL,               -- Target Postgres table (schema.table)
    delta_uri TEXT NOT NULL,                     -- Delta Lake URI (s3://, file://, etc.)
    partition_columns TEXT[],                    -- Partition columns from Delta metadata
    delta_version BIGINT NOT NULL DEFAULT 0,     -- Last synced Delta version
    storage_options JSONB,                       -- Per-table storage credentials override
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refresh_at TIMESTAMPTZ
);

-- Track partition sync state for managed tables
CREATE TABLE delta.table_partitions (
    id BIGSERIAL PRIMARY KEY,
    table_id INTEGER NOT NULL REFERENCES delta.tables(id) ON DELETE CASCADE,
    partition_path TEXT NOT NULL,                 -- "year=2024/month=01"
    partition_values JSONB NOT NULL,              -- {"year": 2024, "month": 1}
    delta_version BIGINT NOT NULL,                -- Version when partition was last synced
    files_processed TEXT[],                       -- Files already synced
    rows_synced BIGINT NOT NULL DEFAULT 0,
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(table_id, partition_path)
);

-- Index for partition lookup
CREATE INDEX idx_table_partitions_table_path
    ON delta.table_partitions(table_id, partition_path);

CREATE TRIGGER tables_updated_at
    BEFORE UPDATE ON delta.tables
    FOR EACH ROW EXECUTE FUNCTION delta.update_timestamp();

-- Export tables - tracks Postgres tables exported to Delta via export_table()
CREATE TABLE delta.export_tables (
    id SERIAL PRIMARY KEY,
    pg_table TEXT UNIQUE NOT NULL,               -- Source Postgres table (schema.table)
    delta_uri TEXT NOT NULL,                     -- Delta Lake URI (s3://, file://, etc.)
    partition_columns TEXT[],                    -- Columns to partition by in Delta
    tracking_column TEXT,                        -- Column to detect changes (e.g., updated_at)
    last_tracking_value TEXT,                    -- Last value of tracking column exported
    delta_version BIGINT NOT NULL DEFAULT 0,     -- Current Delta table version
    storage_options JSONB,                       -- Per-table storage credentials override
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_export_at TIMESTAMPTZ
);

-- Track partition export state
CREATE TABLE delta.export_partitions (
    id BIGSERIAL PRIMARY KEY,
    export_id INTEGER NOT NULL REFERENCES delta.export_tables(id) ON DELETE CASCADE,
    partition_path TEXT NOT NULL,                 -- "year=2024/month=01"
    partition_values JSONB NOT NULL,              -- {"year": 2024, "month": 1}
    last_tracking_value TEXT,                     -- Last tracking value for this partition
    rows_exported BIGINT NOT NULL DEFAULT 0,
    last_export_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(export_id, partition_path)
);

-- Index for partition lookup
CREATE INDEX idx_export_partitions_export_path
    ON delta.export_partitions(export_id, partition_path);

CREATE TRIGGER export_tables_updated_at
    BEFORE UPDATE ON delta.export_tables
    FOR EACH ROW EXECUTE FUNCTION delta.update_timestamp();
"#,
    name = "bootstrap_schema",
    bootstrap
);

// =============================================================================
// Background Worker
// =============================================================================

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_delta_worker_main(_arg: pg_sys::Datum) {
    // Attach signal handlers FIRST - this is critical for clean shutdown
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    log!("pg_delta: stream manager starting");

    // Check for immediate shutdown request
    if BackgroundWorker::sigterm_received() {
        log!("pg_delta: received SIGTERM during startup, exiting immediately");
        return;
    }

    if !PG_DELTA_ENABLED.get() {
        log!("pg_delta: disabled via GUC, exiting");
        return;
    }

    // Connect to SPI only after checking for shutdown
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!("pg_delta: stream manager started, running main loop");

    // Run the stream manager with proper error handling
    match streams::run_stream_manager() {
        Ok(()) => log!("pg_delta: stream manager exited cleanly"),
        Err(e) => log!("pg_delta: stream manager error: {}", e),
    }

    log!("pg_delta: stream manager shutdown complete");
}

// =============================================================================
// SQL Functions - Stream Management
// =============================================================================

/// Get extension status as JSON.
#[pg_extern]
fn status() -> pgrx::JsonB {
    let enabled = PG_DELTA_ENABLED.get();
    let poll_interval_ms = PG_DELTA_POLL_INTERVAL_MS.get();
    let batch_size = PG_DELTA_BATCH_SIZE.get();

    pgrx::JsonB(serde_json::json!({
        "enabled": enabled,
        "poll_interval_ms": poll_interval_ms,
        "batch_size": batch_size,
        "aws_configured": PG_DELTA_AWS_REGION.get().is_some() || PG_DELTA_AWS_USE_INSTANCE_PROFILE.get(),
        "azure_configured": PG_DELTA_AZURE_STORAGE_ACCOUNT.get().is_some(),
        "gcs_configured": PG_DELTA_GCS_SERVICE_ACCOUNT_PATH.get().is_some() || PG_DELTA_GCS_USE_DEFAULT_CREDENTIALS.get(),
    }))
}

/// Create a new stream.
///
/// # Arguments
/// * `name` - Unique stream identifier
/// * `uri` - Delta table URI (s3://, az://, gs://, file://)
/// * `pg_table` - Postgres table (schema.table)
/// * `direction` - 'ingest' (Delta → Postgres) or 'export' (Postgres → Delta)
/// * `mode` - Sync mode: append, upsert, replace (ingest) or cdc, incremental, snapshot (export)
/// * `poll_interval_ms` - Check interval for ingest streams
/// * `batch_size` - Rows per batch for export streams
/// * `flush_interval_ms` - Max time between flushes for export
/// * `start_version` - Delta version to start from (NULL = latest)
/// * `tracking_column` - Column for incremental export mode
/// * `key_columns` - Primary key columns for upsert mode
/// * `storage_options` - Per-stream storage credentials override
/// * `enabled` - Start immediately
#[pg_extern]
fn stream_create(
    name: &str,
    uri: &str,
    pg_table: &str,
    direction: &str,
    mode: default!(&str, "'append'"),
    poll_interval_ms: default!(Option<i32>, "NULL"),
    batch_size: default!(Option<i32>, "NULL"),
    flush_interval_ms: default!(Option<i32>, "NULL"),
    start_version: default!(Option<i64>, "NULL"),
    tracking_column: default!(Option<&str>, "NULL"),
    key_columns: default!(Option<Vec<String>>, "NULL"),
    storage_options: default!(Option<pgrx::JsonB>, "NULL"),
    enabled: default!(bool, "true"),
) -> i32 {
    // Validate direction
    if direction != "ingest" && direction != "export" {
        pgrx::error!(
            "direction must be 'ingest' or 'export', got '{}'",
            direction
        );
    }

    // Validate mode based on direction
    let valid_modes = match direction {
        "ingest" => vec!["append", "upsert", "replace"],
        "export" => vec!["cdc", "incremental", "snapshot"],
        _ => unreachable!(),
    };
    if !valid_modes.contains(&mode) {
        pgrx::error!(
            "mode must be one of {:?} for direction '{}', got '{}'",
            valid_modes,
            direction,
            mode
        );
    }

    // Validate URI scheme
    let uri_lower = uri.to_lowercase();
    if !uri_lower.starts_with("s3://")
        && !uri_lower.starts_with("az://")
        && !uri_lower.starts_with("azure://")
        && !uri_lower.starts_with("gs://")
        && !uri_lower.starts_with("file://")
    {
        pgrx::error!(
            "uri must start with s3://, az://, azure://, gs://, or file://, got '{}'",
            uri
        );
    }

    // Convert key_columns to array literal for SQL
    let key_columns_sql = key_columns
        .as_ref()
        .map(|cols| {
            format!(
                "ARRAY[{}]",
                cols.iter()
                    .map(|c| format!("'{}'", c))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .unwrap_or_else(|| "NULL".to_string());

    let storage_options_sql = storage_options
        .as_ref()
        .map(|j| format!("'{}'::jsonb", j.0))
        .unwrap_or_else(|| "NULL".to_string());

    let query = format!(
        r#"
        INSERT INTO delta.streams (
            name, direction, uri, pg_table, mode,
            poll_interval_ms, batch_size, flush_interval_ms,
            start_version, tracking_column, key_columns,
            storage_options, enabled
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8,
            $9, $10, {},
            {}, $11
        ) RETURNING id
        "#,
        key_columns_sql, storage_options_sql
    );

    Spi::get_one_with_args::<i32>(
        &query,
        &[
            name.into(),
            direction.into(),
            uri.into(),
            pg_table.into(),
            mode.into(),
            poll_interval_ms.into(),
            batch_size.into(),
            flush_interval_ms.into(),
            start_version.into(),
            tracking_column.into(),
            enabled.into(),
        ],
    )
    .expect("Failed to create stream")
    .expect("No ID returned from INSERT")
}

/// Start a stream.
#[pg_extern]
fn stream_start(name: &str) -> bool {
    // First check if stream exists and isn't already running
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM delta.streams WHERE name = $1 AND status != 'running')",
        &[name.into()],
    );

    match exists {
        Ok(Some(true)) => {
            // Stream exists and is not running - update it
            if let Err(e) = Spi::run_with_args(
                "UPDATE delta.streams SET status = 'running', enabled = true, error_message = NULL
                 WHERE name = $1 AND status != 'running'",
                &[name.into()],
            ) {
                pgrx::error!("Failed to start stream '{}': {}", name, e);
            }
            log!("pg_delta: stream '{}' started", name);
            true
        }
        Ok(Some(false)) | Ok(None) => {
            pgrx::warning!("Stream '{}' not found or already running", name);
            false
        }
        Err(e) => {
            pgrx::error!("Failed to start stream '{}': {}", name, e);
        }
    }
}

/// Stop a stream gracefully.
#[pg_extern]
fn stream_stop(name: &str) -> bool {
    // First check if stream exists and is running
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM delta.streams WHERE name = $1 AND status = 'running')",
        &[name.into()],
    );

    match exists {
        Ok(Some(true)) => {
            // Stream exists and is running - stop it
            if let Err(e) = Spi::run_with_args(
                "UPDATE delta.streams SET status = 'stopped', enabled = false
                 WHERE name = $1 AND status = 'running'",
                &[name.into()],
            ) {
                pgrx::error!("Failed to stop stream '{}': {}", name, e);
            }
            log!("pg_delta: stream '{}' stopped", name);
            true
        }
        Ok(Some(false)) | Ok(None) => {
            pgrx::warning!("Stream '{}' not found or not running", name);
            false
        }
        Err(e) => {
            pgrx::error!("Failed to stop stream '{}': {}", name, e);
        }
    }
}

/// Drop a stream and optionally its progress.
#[pg_extern]
fn stream_drop(name: &str, force: default!(bool, "false")) -> bool {
    // First stop if running
    if !force {
        let _ = stream_stop(name);
    }

    // Check if stream exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM delta.streams WHERE name = $1)",
        &[name.into()],
    );

    match exists {
        Ok(Some(true)) => {
            // Stream exists - delete it
            if let Err(e) =
                Spi::run_with_args("DELETE FROM delta.streams WHERE name = $1", &[name.into()])
            {
                pgrx::error!("Failed to drop stream '{}': {}", name, e);
            }
            log!("pg_delta: stream '{}' dropped", name);
            true
        }
        Ok(Some(false)) | Ok(None) => {
            pgrx::warning!("Stream '{}' not found", name);
            false
        }
        Err(e) => {
            pgrx::error!("Failed to drop stream '{}': {}", name, e);
        }
    }
}

/// Reset stream progress (re-sync from beginning or specific version).
#[pg_extern]
fn stream_reset(name: &str, version: default!(Option<i64>, "NULL")) -> bool {
    // Stop the stream first
    let _ = stream_stop(name);

    // Reset progress
    let result = Spi::run_with_args(
        r#"
        UPDATE delta.stream_progress
        SET delta_version = $2, pg_lsn = NULL, last_value = NULL,
            rows_synced = 0, bytes_synced = 0, last_sync_at = NULL
        WHERE stream_id = (SELECT id FROM delta.streams WHERE name = $1)
        "#,
        &[name.into(), version.into()],
    );

    match result {
        Ok(_) => {
            log!("pg_delta: stream '{}' reset to version {:?}", name, version);
            true
        }
        Err(e) => {
            pgrx::error!("Failed to reset stream '{}': {}", name, e);
        }
    }
}

// =============================================================================
// SQL Functions - One-Shot Operations
// =============================================================================

/// Read a Delta table into a Postgres table (one-time).
#[pg_extern]
fn read(
    uri: &str,
    pg_table: &str,
    mode: default!(&str, "'replace'"),
    version: default!(Option<i64>, "NULL"),
    timestamp: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> i64 {
    storage::read_delta_to_postgres(uri, pg_table, mode, version, timestamp)
}

/// Write a Postgres query result to Delta (one-time).
#[pg_extern]
fn write(
    uri: &str,
    query: default!(Option<&str>, "NULL"),
    pg_table: default!(Option<&str>, "NULL"),
    mode: default!(&str, "'append'"),
    partition_by: default!(Option<Vec<String>>, "NULL"),
) -> i64 {
    if query.is_none() && pg_table.is_none() {
        pgrx::error!("Either query or pg_table must be specified");
    }
    if query.is_some() && pg_table.is_some() {
        pgrx::error!("Only one of query or pg_table can be specified");
    }

    let actual_query = query.unwrap_or_else(|| {
        pg_table.expect("pg_table guaranteed Some when query is None (validated at lines 732-736)")
    });

    storage::write_postgres_to_delta(uri, actual_query, query.is_some(), mode, partition_by)
}

/// Get metadata about a Delta table.
#[pg_extern]
fn info(uri: &str) -> pgrx::JsonB {
    storage::get_delta_info(uri)
}

/// Get the schema of a Delta table.
#[pg_extern]
fn schema(uri: &str) -> pgrx::JsonB {
    storage::get_delta_schema(uri)
}

/// Get transaction history of a Delta table.
#[pg_extern]
fn history(uri: &str, limit: default!(Option<i32>, "10")) -> pgrx::JsonB {
    storage::get_delta_history(uri, limit.unwrap_or(10))
}

/// Test storage connectivity.
#[pg_extern]
fn test_storage(uri: &str) -> pgrx::JsonB {
    storage::test_storage_connectivity(uri)
}

// =============================================================================
// SQL Functions - Managed Tables (create_table/refresh pattern)
// =============================================================================

/// Create a Postgres table from a Delta Lake table.
///
/// This performs an initial full load of the Delta table into Postgres and
/// registers it for incremental refresh. Similar to creating a materialized view.
///
/// # Arguments
/// * `uri` - Delta table URI (s3://, az://, gs://, file://)
/// * `pg_table` - Target Postgres table name (will be created)
/// * `storage_options` - Optional per-table storage credentials override
///
/// # Returns
/// Number of rows loaded
///
/// # Example
/// ```sql
/// SELECT delta.create_table('s3://bucket/sales', 'public.sales');
/// SELECT delta.refresh('public.sales'); -- Later, to sync changes
/// ```
#[pg_extern]
fn create_table(
    uri: &str,
    pg_table: &str,
    storage_options: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    storage::create_managed_table(uri, pg_table, storage_options)
}

/// Refresh a managed Delta table, syncing only changed partitions.
///
/// This incrementally syncs the Postgres table with the Delta source,
/// only processing partitions that have new or modified files.
///
/// # Arguments
/// * `pg_table` - Postgres table name (must have been created with create_table)
/// * `full_refresh` - If true, re-sync all data (default: false)
///
/// # Returns
/// Number of rows synced
///
/// # Example
/// ```sql
/// SELECT delta.refresh('public.sales');           -- Incremental
/// SELECT delta.refresh('public.sales', true);    -- Full refresh
/// ```
#[pg_extern]
fn refresh(pg_table: &str, full_refresh: default!(bool, "false")) -> i64 {
    storage::refresh_managed_table(pg_table, full_refresh)
}

/// Drop a managed Delta table registration.
///
/// This removes the table from delta.tables tracking. Optionally drops the
/// Postgres table as well.
///
/// # Arguments
/// * `pg_table` - Postgres table name
/// * `drop_table` - If true, also DROP the Postgres table (default: false)
///
/// # Returns
/// true if successfully unregistered
#[pg_extern]
fn drop_table(pg_table: &str, drop_table: default!(bool, "false")) -> bool {
    storage::drop_managed_table(pg_table, drop_table)
}

/// List all managed Delta tables.
#[pg_extern]
fn list_tables() -> pgrx::JsonB {
    let tables = Spi::connect(|client| {
        let result = client
            .select(
                r#"
            SELECT pg_table, delta_uri, partition_columns, delta_version,
                   created_at, last_refresh_at
            FROM delta.tables
            ORDER BY pg_table
            "#,
                None,
                &[],
            )
            .map_err(|e| format!("Query failed: {:?}", e))?;

        let mut tables = Vec::new();
        for row in result {
            let pg_table: String = row
                .get::<String>(1)
                .map_err(|e| format!("Failed to get pg_table: {}", e))?
                .unwrap_or_default();
            let delta_uri: String = row
                .get::<String>(2)
                .map_err(|e| format!("Failed to get delta_uri: {}", e))?
                .unwrap_or_default();
            let partition_columns: Option<Vec<String>> = row
                .get::<Vec<String>>(3)
                .map_err(|e| format!("Failed to get partition_columns: {}", e))?;
            let delta_version: i64 = row
                .get::<i64>(4)
                .map_err(|e| format!("Failed to get delta_version: {}", e))?
                .unwrap_or(0);

            tables.push(serde_json::json!({
                "pg_table": pg_table,
                "delta_uri": delta_uri,
                "partition_columns": partition_columns,
                "delta_version": delta_version,
            }));
        }

        Ok::<Vec<serde_json::Value>, String>(tables)
    });

    match tables {
        Ok(t) => pgrx::JsonB(serde_json::json!({"tables": t})),
        Err(e) => {
            pgrx::error!("Failed to list tables: {}", e);
        }
    }
}

// =============================================================================
// SQL Functions - Export Tables (Postgres → Delta)
// =============================================================================

/// Export a Postgres table to Delta Lake.
///
/// This performs an initial full export of the Postgres table to Delta and
/// registers it for incremental export. Similar to `create_table` but in reverse.
///
/// # Arguments
/// * `pg_table` - Source Postgres table (schema.table)
/// * `uri` - Delta table URI (s3://, az://, gs://, file://)
/// * `partition_by` - Columns to partition by in Delta (e.g., ARRAY['year', 'month'])
/// * `tracking_column` - Column to detect changes (e.g., 'updated_at')
/// * `storage_options` - Optional per-table storage credentials override
///
/// # Returns
/// Number of rows exported
///
/// # Example
/// ```sql
/// SELECT delta.export_table('public.sales', 's3://bucket/sales',
///     partition_by => ARRAY['year', 'month'],
///     tracking_column => 'updated_at'
/// );
/// SELECT delta.export('public.sales'); -- Later, to sync changes
/// ```
#[pg_extern]
fn export_table(
    pg_table: &str,
    uri: &str,
    partition_by: default!(Option<Vec<String>>, "NULL"),
    tracking_column: default!(Option<&str>, "NULL"),
    storage_options: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    storage::create_export_table(
        pg_table,
        uri,
        partition_by,
        tracking_column,
        storage_options,
    )
}

/// Export changed rows from a managed Postgres table to Delta.
///
/// This incrementally exports only rows that have changed since the last export,
/// writing only the affected partitions to Delta.
///
/// # Arguments
/// * `pg_table` - Postgres table name (must have been registered with export_table)
/// * `full_export` - If true, re-export all data (default: false)
///
/// # Returns
/// Number of rows exported
///
/// # Example
/// ```sql
/// SELECT delta.export('public.sales');           -- Incremental
/// SELECT delta.export('public.sales', true);    -- Full export
/// ```
#[pg_extern]
fn export(pg_table: &str, full_export: default!(bool, "false")) -> i64 {
    storage::export_managed_table(pg_table, full_export)
}

/// Drop an export table registration.
///
/// This removes the table from delta.export_tables tracking.
/// Does NOT delete the Delta table or Postgres table.
///
/// # Arguments
/// * `pg_table` - Postgres table name
///
/// # Returns
/// true if successfully unregistered
#[pg_extern]
fn drop_export(pg_table: &str) -> bool {
    storage::drop_export_table(pg_table)
}

/// List all export table registrations.
#[pg_extern]
fn list_exports() -> pgrx::JsonB {
    let exports = Spi::connect(|client| {
        let result = client
            .select(
                r#"
            SELECT pg_table, delta_uri, partition_columns, tracking_column,
                   delta_version, last_export_at
            FROM delta.export_tables
            ORDER BY pg_table
            "#,
                None,
                &[],
            )
            .map_err(|e| format!("Query failed: {:?}", e))?;

        let mut exports = Vec::new();
        for row in result {
            let pg_table: String = row
                .get::<String>(1)
                .map_err(|e| format!("Failed to get pg_table: {}", e))?
                .unwrap_or_default();
            let delta_uri: String = row
                .get::<String>(2)
                .map_err(|e| format!("Failed to get delta_uri: {}", e))?
                .unwrap_or_default();
            let partition_columns: Option<Vec<String>> = row
                .get::<Vec<String>>(3)
                .map_err(|e| format!("Failed to get partition_columns: {}", e))?;
            let tracking_column: Option<String> = row
                .get::<String>(4)
                .map_err(|e| format!("Failed to get tracking_column: {}", e))?;
            let delta_version: i64 = row
                .get::<i64>(5)
                .map_err(|e| format!("Failed to get delta_version: {}", e))?
                .unwrap_or(0);

            exports.push(serde_json::json!({
                "pg_table": pg_table,
                "delta_uri": delta_uri,
                "partition_columns": partition_columns,
                "tracking_column": tracking_column,
                "delta_version": delta_version,
            }));
        }

        Ok::<Vec<serde_json::Value>, String>(exports)
    });

    match exports {
        Ok(e) => pgrx::JsonB(serde_json::json!({"exports": e})),
        Err(e) => {
            pgrx::error!("Failed to list exports: {}", e);
        }
    }
}

// =============================================================================
// Documentation
// =============================================================================

#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_status() {
        let status = crate::status();
        assert!(status.0.is_object());
        assert!(status.0.get("enabled").is_some());
    }

    #[pg_test]
    fn test_stream_create_ingest() {
        // Create an ingest stream
        let id = crate::stream_create(
            "test_ingest",
            "file:///tmp/test_delta",
            "public.test_table",
            "ingest",
            "append",
            Some(5000),
            None,
            None,
            None,
            None,
            None,
            None,
            false, // don't start
        );
        assert!(id > 0);

        // Verify it exists
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM delta.streams WHERE name = 'test_ingest')",
        )
        .expect("Query failed");
        assert_eq!(exists, Some(true));

        // Cleanup
        crate::stream_drop("test_ingest", false);
    }

    #[pg_test]
    fn test_stream_create_export() {
        // Create an export stream
        let id = crate::stream_create(
            "test_export",
            "file:///tmp/test_delta_out",
            "public.test_table",
            "export",
            "incremental",
            None,
            Some(1000),
            Some(30000),
            None,
            Some("updated_at"),
            None,
            None,
            false,
        );
        assert!(id > 0);

        // Cleanup
        crate::stream_drop("test_export", true);
    }

    #[pg_test]
    fn test_stream_create_invalid_direction() {
        // This should fail
        let result = std::panic::catch_unwind(|| {
            crate::stream_create(
                "test_invalid",
                "file:///tmp/test",
                "public.test",
                "invalid", // bad direction
                "append",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            );
        });
        assert!(result.is_err());
    }

    #[pg_test]
    fn test_stream_create_invalid_mode() {
        // Export with ingest mode should fail
        let result = std::panic::catch_unwind(|| {
            crate::stream_create(
                "test_invalid_mode",
                "file:///tmp/test",
                "public.test",
                "export",
                "append", // append is for ingest, not export
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                false,
            );
        });
        assert!(result.is_err());
    }

    #[pg_test]
    fn test_stream_start_stop() {
        // Create a stream
        let _id = crate::stream_create(
            "test_startstop",
            "file:///tmp/test_delta",
            "public.test_table",
            "ingest",
            "append",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );

        // Start it
        let started = crate::stream_start("test_startstop");
        assert!(started);

        // Verify status
        let status = Spi::get_one::<String>(
            "SELECT status FROM delta.streams WHERE name = 'test_startstop'",
        )
        .expect("Query failed");
        assert_eq!(status, Some("running".to_string()));

        // Stop it
        let stopped = crate::stream_stop("test_startstop");
        assert!(stopped);

        // Verify status
        let status = Spi::get_one::<String>(
            "SELECT status FROM delta.streams WHERE name = 'test_startstop'",
        )
        .expect("Query failed");
        assert_eq!(status, Some("stopped".to_string()));

        // Cleanup
        crate::stream_drop("test_startstop", true);
    }

    #[pg_test]
    fn test_stream_drop() {
        // Create and drop
        let _id = crate::stream_create(
            "test_drop",
            "file:///tmp/test_delta",
            "public.test_table",
            "ingest",
            "append",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
        );

        let dropped = crate::stream_drop("test_drop", false);
        assert!(dropped);

        // Verify it's gone
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM delta.streams WHERE name = 'test_drop')",
        )
        .expect("Query failed");
        assert_eq!(exists, Some(false));
    }

    #[pg_test]
    fn test_stream_drop_nonexistent() {
        let dropped = crate::stream_drop("nonexistent_stream", false);
        assert!(!dropped);
    }
}

/// Test configuration module required by pgrx-tests framework.
/// The `#[pg_test]` macro generates code that references `crate::pg_test::setup`
/// and `crate::pg_test::postgresql_conf_options`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Setup code for tests
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
