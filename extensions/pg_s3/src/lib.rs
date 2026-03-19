//! pg_s3 - S3-compatible object storage in PostgreSQL
//!
//! This extension implements the S3 REST API, storing object metadata
//! in PostgreSQL tables and binary content on the local filesystem.
//!
//! Buckets are tables. Objects are rows with pointers to files on disk.
//! Standard S3 clients (aws cli, boto3, SDKs) connect without modification.

#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

pub mod s3;
mod server;
pub mod storage;

pgrx::pg_module_magic!();

// GUC settings
static PG_S3_PORT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(9100);
static PG_S3_HOST: pgrx::GucSetting<Option<std::ffi::CString>> =
    pgrx::GucSetting::<Option<std::ffi::CString>>::new(None);
static PG_S3_DATA_DIR: pgrx::GucSetting<Option<std::ffi::CString>> =
    pgrx::GucSetting::<Option<std::ffi::CString>>::new(None);
static PG_S3_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);
static PG_S3_WORKER_COUNT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1);
static PG_S3_DATABASE: pgrx::GucSetting<Option<std::ffi::CString>> =
    pgrx::GucSetting::<Option<std::ffi::CString>>::new(None);
static PG_S3_MAX_OBJECT_SIZE_MB: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(5120);

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_DATA_DIR: &str = "pgs3_data";
const DEFAULT_DATABASE: &str = "postgres";

// Bootstrap SQL — creates metadata tables
pgrx::extension_sql!(
    r#"
-- Bucket registry
CREATE TABLE pgs3.buckets (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_buckets_name ON pgs3.buckets (name);

-- Object metadata
CREATE TABLE pgs3.objects (
    id BIGSERIAL PRIMARY KEY,
    bucket_id BIGINT NOT NULL REFERENCES pgs3.buckets(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    content_hash TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    storage_path TEXT NOT NULL,
    custom_metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_modified TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_bucket_key UNIQUE (bucket_id, key)
);

-- Fast lookup by bucket + key (primary access pattern)
CREATE UNIQUE INDEX idx_objects_bucket_key ON pgs3.objects (bucket_id, key);

-- Prefix listing (LIST operations)
CREATE INDEX idx_objects_bucket_key_prefix ON pgs3.objects (bucket_id, key text_pattern_ops);

-- Content dedup reference check
CREATE INDEX idx_objects_content_hash ON pgs3.objects (content_hash);

-- Aggressive autovacuum for high-write objects table
ALTER TABLE pgs3.objects SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 10
);
"#,
    name = "bootstrap_tables",
    bootstrap
);

/// Extension initialization — register background workers and GUCs
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_int_guc(
        c"pg_s3.port",
        c"Port for the S3 HTTP server",
        c"HTTP port that pg_s3 listens on for S3 API requests",
        &PG_S3_PORT,
        1,
        65535,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_s3.host",
        c"Host address for the S3 HTTP server",
        c"IP address or hostname that pg_s3 binds to",
        &PG_S3_HOST,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_s3.data_directory",
        c"Directory for binary object storage",
        c"Path for storing object content files. Relative to $PGDATA or absolute.",
        &PG_S3_DATA_DIR,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_s3.enabled",
        c"Enable the S3 HTTP server",
        c"When true, pg_s3 starts automatically with PostgreSQL",
        &PG_S3_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_s3.worker_count",
        c"Number of S3 background workers",
        c"Number of background workers handling S3 HTTP connections",
        &PG_S3_WORKER_COUNT,
        1,
        16,
        pgrx::GucContext::Postmaster,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_s3.database",
        c"Database for pg_s3 to connect to",
        c"The database where pg_s3 extension is installed. Defaults to 'postgres'.",
        &PG_S3_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_s3.max_object_size_mb",
        c"Maximum object size in megabytes",
        c"Maximum size of a single object upload in megabytes",
        &PG_S3_MAX_OBJECT_SIZE_MB,
        1,
        i32::MAX,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Register background workers
    let worker_count = PG_S3_WORKER_COUNT.get();
    pgrx::log!("pg_s3: registering {} background worker(s)", worker_count);

    for i in 0..worker_count {
        BackgroundWorkerBuilder::new(&format!("pg_s3 worker {}", i))
            .set_function("pg_s3_worker_main")
            .set_library("pg_s3")
            .set_argument(Some(pg_sys::Datum::from(i as i64)))
            .enable_shmem_access(None)
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }
}

/// Background worker main function — runs the HTTP server
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_s3_worker_main(arg: pg_sys::Datum) {
    let worker_id = arg.value() as i32;

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_setting = PG_S3_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    log!(
        "pg_s3 worker {}: started, pid={}",
        worker_id,
        std::process::id()
    );

    if !PG_S3_ENABLED.get() {
        log!(
            "pg_s3 worker {}: disabled via pg_s3.enabled=false",
            worker_id
        );
        return;
    }

    let port = PG_S3_PORT.get() as u16;
    let host_setting = PG_S3_HOST.get();
    let host = host_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_HOST);
    let data_dir_setting = PG_S3_DATA_DIR.get();
    let data_dir = data_dir_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATA_DIR);

    log!(
        "pg_s3 worker {}: starting HTTP server on {}:{}",
        worker_id,
        host,
        port
    );
    log!("pg_s3 worker {}: data directory: {}", worker_id, data_dir);

    if let Err(e) = server::run_server(host, port, data_dir, worker_id) {
        log!("pg_s3 worker {}: server error: {}", worker_id, e);
    }

    log!("pg_s3 worker {}: shutting down", worker_id);
}

// ── SQL Functions ──────────────────────────────────────────────────

fn get_data_dir() -> String {
    PG_S3_DATA_DIR
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATA_DIR)
        .to_string()
}

/// Get server status as JSON
#[pg_extern]
fn status() -> pgrx::JsonB {
    let enabled = PG_S3_ENABLED.get();
    let port = PG_S3_PORT.get();
    let host = PG_S3_HOST
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_HOST)
        .to_string();
    let data_dir = get_data_dir();

    pgrx::JsonB(serde_json::json!({
        "enabled": enabled,
        "port": port,
        "host": host,
        "data_directory": data_dir,
    }))
}

/// Validate a bucket name following S3 naming rules
fn validate_bucket_name(name: &str) -> Result<(), String> {
    if name.len() < 3 || name.len() > 63 {
        return Err("Bucket name must be between 3 and 63 characters".to_string());
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(
            "Bucket name must contain only lowercase letters, numbers, and hyphens".to_string(),
        );
    }

    if !name.starts_with(|c: char| c.is_ascii_alphanumeric())
        || !name.ends_with(|c: char| c.is_ascii_alphanumeric())
    {
        return Err("Bucket name must start and end with a letter or number".to_string());
    }

    if name.contains("--") {
        return Err("Bucket name must not contain consecutive hyphens".to_string());
    }

    // Check not IP address format
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return Err("Bucket name must not be formatted as an IP address".to_string());
    }

    Ok(())
}

/// Create a new bucket
#[pg_extern(sql = "
CREATE FUNCTION create_bucket(name TEXT) RETURNS BIGINT
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'create_bucket_wrapper';
")]
fn create_bucket(name: &str) -> i64 {
    if let Err(e) = validate_bucket_name(name) {
        pgrx::error!("Invalid bucket name '{}': {}", name, e);
    }

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgs3.buckets (name) VALUES ($1) RETURNING id",
        &[name.into()],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create bucket"),
        Err(e) => pgrx::error!("Failed to create bucket '{}': {}", name, e),
    }
}

/// Delete a bucket
#[pg_extern(sql = "
CREATE FUNCTION delete_bucket(name TEXT, force BOOLEAN DEFAULT FALSE) RETURNS BOOLEAN
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'delete_bucket_wrapper';
")]
fn delete_bucket(name: &str, force: default!(bool, false)) -> bool {
    let bucket_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgs3.buckets WHERE name = $1",
        &[name.into()],
    );

    let bucket_id = match bucket_id {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Bucket '{}' does not exist", name),
        Err(e) => pgrx::error!("Failed to check bucket: {}", e),
    };

    if !force {
        let has_objects = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pgs3.objects WHERE bucket_id = $1)",
            &[bucket_id.into()],
        );

        match has_objects {
            Ok(Some(true)) => {
                pgrx::error!(
                    "Bucket '{}' is not empty. Use force => true to delete all objects.",
                    name
                );
            }
            Ok(Some(false)) | Ok(None) => {}
            Err(e) => pgrx::error!("Failed to check bucket contents: {}", e),
        }
    }

    match Spi::run_with_args(
        "DELETE FROM pgs3.buckets WHERE id = $1",
        &[bucket_id.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to delete bucket: {}", e),
    }
}

/// Store an object via SQL
#[pg_extern(sql = "
CREATE FUNCTION put_object(
    bucket TEXT,
    key TEXT,
    content BYTEA,
    content_type TEXT DEFAULT 'application/octet-stream',
    metadata JSONB DEFAULT NULL
) RETURNS TEXT
VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'put_object_wrapper';
")]
fn put_object(
    bucket: &str,
    key: &str,
    content: Vec<u8>,
    content_type: default!(&str, "'application/octet-stream'"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
) -> String {
    if key.is_empty() || key.len() > 1024 {
        pgrx::error!("Object key must be between 1 and 1024 characters");
    }

    let bucket_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgs3.buckets WHERE name = $1",
        &[bucket.into()],
    );

    let bucket_id = match bucket_id {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Bucket '{}' does not exist", bucket),
        Err(e) => pgrx::error!("Failed to look up bucket: {}", e),
    };

    let data_dir = get_data_dir();

    let content_hash = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&content);
        hex::encode(hasher.finalize())
    };

    let size_bytes = content.len() as i64;
    let prefix = &content_hash[..2];
    let storage_path = format!("{}/{}/{}", bucket, prefix, content_hash);

    let content_dir = std::path::Path::new(&data_dir).join(bucket).join(prefix);
    let final_path = content_dir.join(&content_hash);

    if !final_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&content_dir) {
            pgrx::error!("Failed to create content directory: {}", e);
        }
        if let Err(e) = std::fs::write(&final_path, &content) {
            pgrx::error!("Failed to write content file: {}", e);
        }
    }

    let old_hash = Spi::get_one_with_args::<String>(
        "INSERT INTO pgs3.objects (bucket_id, key, size_bytes, content_hash, content_type, storage_path, custom_metadata, last_modified)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now())
         ON CONFLICT (bucket_id, key) DO UPDATE SET
            size_bytes = EXCLUDED.size_bytes,
            content_hash = EXCLUDED.content_hash,
            content_type = EXCLUDED.content_type,
            storage_path = EXCLUDED.storage_path,
            custom_metadata = EXCLUDED.custom_metadata,
            last_modified = now()
         RETURNING (SELECT o.content_hash FROM pgs3.objects o WHERE o.bucket_id = $1 AND o.key = $2 AND o.content_hash != $4)",
        &[
            bucket_id.into(),
            key.into(),
            size_bytes.into(),
            content_hash.clone().into(),
            content_type.into(),
            storage_path.clone().into(),
            metadata.into(),
        ],
    );

    if let Ok(Some(old_hash)) = old_hash {
        let old_ref_count = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgs3.objects WHERE content_hash = $1",
            &[old_hash.clone().into()],
        );

        if let Ok(Some(0)) = old_ref_count {
            let old_prefix = &old_hash[..2];
            let old_path = std::path::Path::new(&data_dir)
                .join(bucket)
                .join(old_prefix)
                .join(&old_hash);
            let _ = std::fs::remove_file(old_path);
        }
    }

    content_hash
}

/// Retrieve object content via SQL
#[pg_extern(sql = "
CREATE FUNCTION get_object(bucket TEXT, key TEXT) RETURNS BYTEA
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'get_object_wrapper';
")]
fn get_object(bucket: &str, key: &str) -> Vec<u8> {
    let storage_path = Spi::get_one_with_args::<String>(
        "SELECT o.storage_path FROM pgs3.objects o
         JOIN pgs3.buckets b ON o.bucket_id = b.id
         WHERE b.name = $1 AND o.key = $2",
        &[bucket.into(), key.into()],
    );

    let storage_path = match storage_path {
        Ok(Some(path)) => path,
        Ok(None) => pgrx::error!("Object '{}/{}' not found", bucket, key),
        Err(e) => pgrx::error!("Failed to look up object: {}", e),
    };

    let data_dir = get_data_dir();
    let full_path = std::path::Path::new(&data_dir).join(&storage_path);

    match std::fs::read(&full_path) {
        Ok(content) => content,
        Err(e) => pgrx::error!("Failed to read object content: {}", e),
    }
}

/// Delete an object
#[pg_extern(sql = "
CREATE FUNCTION delete_object(bucket TEXT, key TEXT) RETURNS BOOLEAN
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'delete_object_wrapper';
")]
fn delete_object(bucket: &str, key: &str) -> bool {
    let info: Option<String> = Spi::connect_mut(|client| {
        let table = client
            .update(
                "DELETE FROM pgs3.objects
                 WHERE bucket_id = (SELECT id FROM pgs3.buckets WHERE name = $1)
                   AND key = $2
                 RETURNING content_hash || '|' || storage_path",
                None,
                &[bucket.into(), key.into()],
            )
            .map_err(|e| pgrx::error!("Failed to delete object: {}", e))
            .unwrap();

        table.first().get::<String>(1).ok().flatten()
    });

    match info {
        Some(info) => {
            let parts: Vec<&str> = info.splitn(2, '|').collect();
            if parts.len() == 2 {
                let content_hash = parts[0];
                let storage_path = parts[1];

                let ref_count = Spi::get_one_with_args::<i64>(
                    "SELECT COUNT(*) FROM pgs3.objects WHERE content_hash = $1",
                    &[content_hash.into()],
                );

                if let Ok(Some(0)) = ref_count {
                    let data_dir = get_data_dir();
                    let full_path = std::path::Path::new(&data_dir).join(storage_path);
                    let _ = std::fs::remove_file(full_path);
                }
            }
            true
        }
        None => false,
    }
}

/// Get object metadata without content
#[pg_extern(sql = "
CREATE FUNCTION head_object(bucket TEXT, key TEXT) RETURNS JSONB
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'head_object_wrapper';
")]
fn head_object(bucket: &str, key: &str) -> pgrx::JsonB {
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_build_object(
            'key', o.key,
            'size_bytes', o.size_bytes,
            'content_type', o.content_type,
            'etag', o.content_hash,
            'last_modified', o.last_modified,
            'custom_metadata', o.custom_metadata
         )
         FROM pgs3.objects o
         JOIN pgs3.buckets b ON o.bucket_id = b.id
         WHERE b.name = $1 AND o.key = $2",
        &[bucket.into(), key.into()],
    );

    match result {
        Ok(Some(json)) => json,
        Ok(None) => pgrx::error!("Object '{}/{}' not found", bucket, key),
        Err(e) => pgrx::error!("Failed to get object metadata: {}", e),
    }
}

// ── New SQL Functions ──────────────────────────────────────────────

/// List all buckets with object counts and sizes
#[pg_extern]
fn list_buckets() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(object_count, i64),
        name!(total_size_bytes, i64),
        name!(created_at, String),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT b.name, COUNT(o.id)::bigint, COALESCE(SUM(o.size_bytes), 0)::bigint, b.created_at::text
             FROM pgs3.buckets b
             LEFT JOIN pgs3.objects o ON b.id = o.bucket_id
             GROUP BY b.id, b.name, b.created_at
             ORDER BY b.name",
                None,
                &[],
            )
            .unwrap();

        for row in table {
            let name: String = row.get::<String>(1).unwrap().unwrap_or_default();
            let count: i64 = row.get::<i64>(2).unwrap().unwrap_or(0);
            let size: i64 = row.get::<i64>(3).unwrap().unwrap_or(0);
            let created: String = row.get::<String>(4).unwrap().unwrap_or_default();
            rows.push((name, count, size, created));
        }
    });

    TableIterator::new(rows)
}

/// List objects in a bucket with optional prefix/delimiter filtering
#[pg_extern(sql = "
CREATE FUNCTION list_objects(
    bucket TEXT,
    prefix TEXT DEFAULT '',
    delimiter TEXT DEFAULT '',
    max_keys INT DEFAULT 1000,
    start_after TEXT DEFAULT ''
) RETURNS TABLE(key TEXT, size_bytes BIGINT, content_type TEXT, etag TEXT, last_modified TEXT, is_prefix BOOLEAN)
VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'list_objects_wrapper';
")]
fn list_objects(
    bucket: &str,
    prefix: default!(&str, "''"),
    delimiter: default!(&str, "''"),
    max_keys: default!(i32, 1000),
    start_after: default!(&str, "''"),
) -> TableIterator<
    'static,
    (
        name!(key, String),
        name!(size_bytes, i64),
        name!(content_type, String),
        name!(etag, String),
        name!(last_modified, String),
        name!(is_prefix, bool),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let like_pattern = format!("{}%", prefix.replace('%', "\\%"));

        let table = client
            .select(
                "SELECT o.key, o.size_bytes, o.content_type, o.content_hash, o.last_modified::text
             FROM pgs3.objects o
             JOIN pgs3.buckets b ON o.bucket_id = b.id
             WHERE b.name = $1 AND o.key LIKE $2 AND o.key > $3
             ORDER BY o.key",
                None,
                &[bucket.into(), like_pattern.into(), start_after.into()],
            )
            .unwrap();

        let mut all_objects = Vec::new();
        for row in table {
            let key: String = row.get::<String>(1).unwrap().unwrap_or_default();
            let size: i64 = row.get::<i64>(2).unwrap().unwrap_or(0);
            let ct: String = row.get::<String>(3).unwrap().unwrap_or_default();
            let hash: String = row.get::<String>(4).unwrap().unwrap_or_default();
            let modified: String = row.get::<String>(5).unwrap().unwrap_or_default();
            all_objects.push((key, size, ct, hash, modified));
        }

        if delimiter.is_empty() {
            for (key, size, ct, hash, modified) in all_objects.into_iter().take(max_keys as usize) {
                rows.push((key, size, ct, hash, modified, false));
            }
        } else {
            let mut seen_prefixes = std::collections::HashSet::new();
            let mut count = 0;

            for (key, size, ct, hash, modified) in &all_objects {
                if count >= max_keys as usize {
                    break;
                }

                let after_prefix = &key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter) {
                    let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                    if seen_prefixes.insert(cp.clone()) {
                        rows.push((cp, 0, String::new(), String::new(), String::new(), true));
                        count += 1;
                    }
                } else {
                    rows.push((
                        key.clone(),
                        *size,
                        ct.clone(),
                        hash.clone(),
                        modified.clone(),
                        false,
                    ));
                    count += 1;
                }
            }
        }
    });

    TableIterator::new(rows)
}

/// Get statistics for a specific bucket
#[pg_extern(sql = "
CREATE FUNCTION bucket_stats(bucket TEXT) RETURNS JSONB
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'bucket_stats_wrapper';
")]
fn bucket_stats(bucket: &str) -> pgrx::JsonB {
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_build_object(
            'name', b.name,
            'object_count', COUNT(o.id),
            'total_size_bytes', COALESCE(SUM(o.size_bytes), 0),
            'avg_object_size', COALESCE(AVG(o.size_bytes), 0),
            'largest_object_bytes', COALESCE(MAX(o.size_bytes), 0),
            'created_at', b.created_at
         )
         FROM pgs3.buckets b
         LEFT JOIN pgs3.objects o ON b.id = o.bucket_id
         WHERE b.name = $1
         GROUP BY b.id",
        &[bucket.into()],
    );

    match result {
        Ok(Some(json)) => json,
        Ok(None) => pgrx::error!("Bucket '{}' does not exist", bucket),
        Err(e) => pgrx::error!("Failed to get bucket stats: {}", e),
    }
}

/// Get global storage statistics
#[pg_extern]
fn storage_stats() -> pgrx::JsonB {
    let result = Spi::get_one::<pgrx::JsonB>(
        "SELECT jsonb_build_object(
            'total_buckets', (SELECT COUNT(*) FROM pgs3.buckets),
            'total_objects', (SELECT COUNT(*) FROM pgs3.objects),
            'total_content_bytes', (SELECT COALESCE(SUM(size_bytes), 0) FROM pgs3.objects),
            'unique_content_files', (SELECT COUNT(DISTINCT content_hash) FROM pgs3.objects),
            'data_directory', current_setting('pg_s3.data_directory', true)
        )",
    );

    match result {
        Ok(Some(json)) => json,
        Ok(None) => pgrx::JsonB(serde_json::json!({})),
        Err(e) => pgrx::error!("Failed to get storage stats: {}", e),
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_status() {
        let status = crate::status();
        assert!(status.0.is_object());
        assert_eq!(status.0["port"], 9100);
    }

    #[pg_test]
    fn test_validate_bucket_name() {
        assert!(crate::validate_bucket_name("my-bucket").is_ok());
        assert!(crate::validate_bucket_name("bucket123").is_ok());
        assert!(crate::validate_bucket_name("abc").is_ok());
        assert!(crate::validate_bucket_name("ab").is_err());
        let long_name = "a".repeat(64);
        assert!(crate::validate_bucket_name(&long_name).is_err());
        assert!(crate::validate_bucket_name("MyBucket").is_err());
        assert!(crate::validate_bucket_name("-bucket").is_err());
        assert!(crate::validate_bucket_name("bucket-").is_err());
        assert!(crate::validate_bucket_name("my--bucket").is_err());
        assert!(crate::validate_bucket_name("my_bucket").is_err());
        assert!(crate::validate_bucket_name("my.bucket").is_err());
    }

    #[pg_test]
    fn test_create_and_delete_bucket() {
        let bucket_id = crate::create_bucket("test-bucket-1");
        assert!(bucket_id > 0);
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pgs3.buckets WHERE name = 'test-bucket-1'");
        assert_eq!(count.unwrap(), Some(1));
        let deleted = crate::delete_bucket("test-bucket-1", false);
        assert!(deleted);
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pgs3.buckets WHERE name = 'test-bucket-1'");
        assert_eq!(count.unwrap(), Some(0));
    }

    #[pg_test]
    fn test_create_duplicate_bucket_fails() {
        crate::create_bucket("test-dup-bucket");
        let result = std::panic::catch_unwind(|| {
            crate::create_bucket("test-dup-bucket");
        });
        assert!(result.is_err());
        crate::delete_bucket("test-dup-bucket", false);
    }

    #[pg_test]
    fn test_delete_nonempty_bucket_fails() {
        crate::create_bucket("test-nonempty");
        Spi::run(
            "INSERT INTO pgs3.objects (bucket_id, key, size_bytes, content_hash, content_type, storage_path)
             SELECT id, 'test.txt', 5, 'abc123', 'text/plain', 'test-nonempty/ab/abc123'
             FROM pgs3.buckets WHERE name = 'test-nonempty'",
        ).expect("Failed to insert test object");
        let result = std::panic::catch_unwind(|| {
            crate::delete_bucket("test-nonempty", false);
        });
        assert!(result.is_err());
        let deleted = crate::delete_bucket("test-nonempty", true);
        assert!(deleted);
    }

    #[pg_test]
    fn test_put_and_get_object() {
        crate::create_bucket("test-put-get");
        let content = b"Hello, pg_s3!".to_vec();
        let etag = crate::put_object(
            "test-put-get",
            "greeting.txt",
            content.clone(),
            "text/plain",
            None,
        );
        assert!(!etag.is_empty());
        let retrieved = crate::get_object("test-put-get", "greeting.txt");
        assert_eq!(retrieved, content);
        crate::delete_object("test-put-get", "greeting.txt");
        crate::delete_bucket("test-put-get", true);
    }

    #[pg_test]
    fn test_put_object_overwrite() {
        crate::create_bucket("test-overwrite");
        let etag1 = crate::put_object(
            "test-overwrite",
            "data.bin",
            b"version 1".to_vec(),
            "application/octet-stream",
            None,
        );
        let etag2 = crate::put_object(
            "test-overwrite",
            "data.bin",
            b"version 2".to_vec(),
            "application/octet-stream",
            None,
        );
        assert_ne!(etag1, etag2);
        let content = crate::get_object("test-overwrite", "data.bin");
        assert_eq!(content, b"version 2");
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = 'test-overwrite' AND o.key = 'data.bin'",
        );
        assert_eq!(count.unwrap(), Some(1));
        crate::delete_bucket("test-overwrite", true);
    }

    #[pg_test]
    fn test_head_object() {
        crate::create_bucket("test-head");
        crate::put_object(
            "test-head",
            "info.json",
            br#"{"key": "value"}"#.to_vec(),
            "application/json",
            None,
        );
        let meta = crate::head_object("test-head", "info.json");
        assert_eq!(meta.0["key"], "info.json");
        assert_eq!(meta.0["content_type"], "application/json");
        assert_eq!(meta.0["size_bytes"], 16);
        crate::delete_bucket("test-head", true);
    }

    #[pg_test]
    fn test_delete_object() {
        crate::create_bucket("test-delete-obj");
        crate::put_object(
            "test-delete-obj",
            "temp.txt",
            b"temporary".to_vec(),
            "text/plain",
            None,
        );
        let deleted = crate::delete_object("test-delete-obj", "temp.txt");
        assert!(deleted);
        let result = std::panic::catch_unwind(|| {
            crate::get_object("test-delete-obj", "temp.txt");
        });
        assert!(result.is_err());
        let deleted = crate::delete_object("test-delete-obj", "nonexistent.txt");
        assert!(!deleted);
        crate::delete_bucket("test-delete-obj", false);
    }

    #[pg_test]
    fn test_put_object_with_metadata() {
        crate::create_bucket("test-metadata");
        let meta = serde_json::json!({"department": "engineering", "project": "pg_s3"});
        crate::put_object(
            "test-metadata",
            "doc.pdf",
            b"fake pdf".to_vec(),
            "application/pdf",
            Some(pgrx::JsonB(meta)),
        );
        let head = crate::head_object("test-metadata", "doc.pdf");
        assert_eq!(head.0["custom_metadata"]["department"], "engineering");
        assert_eq!(head.0["custom_metadata"]["project"], "pg_s3");
        crate::delete_bucket("test-metadata", true);
    }

    #[pg_test]
    fn test_get_nonexistent_object_fails() {
        crate::create_bucket("test-404");
        let result = std::panic::catch_unwind(|| {
            crate::get_object("test-404", "nonexistent.txt");
        });
        assert!(result.is_err());
        crate::delete_bucket("test-404", false);
    }

    #[pg_test]
    fn test_sql_queryable_metadata() {
        crate::create_bucket("test-query");
        crate::put_object(
            "test-query",
            "images/cat.jpg",
            b"cat".to_vec(),
            "image/jpeg",
            None,
        );
        crate::put_object(
            "test-query",
            "images/dog.jpg",
            b"dog".to_vec(),
            "image/jpeg",
            None,
        );
        crate::put_object(
            "test-query",
            "docs/readme.md",
            b"readme".to_vec(),
            "text/markdown",
            None,
        );
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = 'test-query' AND o.content_type = 'image/jpeg'",
        );
        assert_eq!(count.unwrap(), Some(2));
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = 'test-query' AND o.key LIKE 'images/%'",
        );
        assert_eq!(count.unwrap(), Some(2));
        crate::delete_bucket("test-query", true);
    }

    // ── New SQL function tests ──────────────────────────────────────

    #[pg_test]
    fn test_list_buckets() {
        crate::create_bucket("test-list-a");
        crate::create_bucket("test-list-b");
        crate::put_object(
            "test-list-a",
            "file.txt",
            b"hello".to_vec(),
            "text/plain",
            None,
        );

        let result: Vec<_> = crate::list_buckets().collect();
        let a = result.iter().find(|r| r.0 == "test-list-a").unwrap();
        let b = result.iter().find(|r| r.0 == "test-list-b").unwrap();
        assert_eq!(a.1, 1);
        assert_eq!(a.2, 5);
        assert_eq!(b.1, 0);
        assert_eq!(b.2, 0);

        crate::delete_bucket("test-list-a", true);
        crate::delete_bucket("test-list-b", true);
    }

    #[pg_test]
    fn test_list_objects_basic() {
        crate::create_bucket("test-list-obj");
        crate::put_object(
            "test-list-obj",
            "a.txt",
            b"aaa".to_vec(),
            "text/plain",
            None,
        );
        crate::put_object(
            "test-list-obj",
            "b.txt",
            b"bbb".to_vec(),
            "text/plain",
            None,
        );

        let result: Vec<_> = crate::list_objects("test-list-obj", "", "", 1000, "").collect();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "a.txt");
        assert_eq!(result[1].0, "b.txt");
        assert!(!result[0].5);

        crate::delete_bucket("test-list-obj", true);
    }

    #[pg_test]
    fn test_list_objects_with_delimiter() {
        crate::create_bucket("test-list-delim");
        crate::put_object(
            "test-list-delim",
            "images/cat.jpg",
            b"c".to_vec(),
            "image/jpeg",
            None,
        );
        crate::put_object(
            "test-list-delim",
            "images/dog.jpg",
            b"d".to_vec(),
            "image/jpeg",
            None,
        );
        crate::put_object(
            "test-list-delim",
            "docs/readme.md",
            b"r".to_vec(),
            "text/plain",
            None,
        );
        crate::put_object(
            "test-list-delim",
            "root.txt",
            b"x".to_vec(),
            "text/plain",
            None,
        );

        let result: Vec<_> = crate::list_objects("test-list-delim", "", "/", 1000, "").collect();
        let prefixes: Vec<_> = result
            .iter()
            .filter(|r| r.5)
            .map(|r| r.0.as_str())
            .collect();
        let objects: Vec<_> = result
            .iter()
            .filter(|r| !r.5)
            .map(|r| r.0.as_str())
            .collect();

        assert!(prefixes.contains(&"images/"));
        assert!(prefixes.contains(&"docs/"));
        assert!(objects.contains(&"root.txt"));

        crate::delete_bucket("test-list-delim", true);
    }

    #[pg_test]
    fn test_bucket_stats() {
        crate::create_bucket("test-bstats");
        crate::put_object(
            "test-bstats",
            "a.txt",
            b"hello".to_vec(),
            "text/plain",
            None,
        );
        crate::put_object(
            "test-bstats",
            "b.txt",
            b"world!!".to_vec(),
            "text/plain",
            None,
        );

        let stats = crate::bucket_stats("test-bstats");
        assert_eq!(stats.0["name"], "test-bstats");
        assert_eq!(stats.0["object_count"], 2);
        assert_eq!(stats.0["total_size_bytes"], 12);

        crate::delete_bucket("test-bstats", true);
    }

    #[pg_test]
    fn test_storage_stats() {
        crate::create_bucket("test-sstats");
        crate::put_object("test-sstats", "f.txt", b"data".to_vec(), "text/plain", None);

        let stats = crate::storage_stats();
        assert!(stats.0["total_buckets"].as_i64().unwrap() >= 1);
        assert!(stats.0["total_objects"].as_i64().unwrap() >= 1);

        crate::delete_bucket("test-sstats", true);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_s3'",
            "max_worker_processes = 16",
            "pg_s3.enabled = false",
        ]
    }
}
