//! Ingest stream - Delta Lake to PostgreSQL.
//!
//! Supports partition-aware tracking for efficient incremental sync.
//! Tracks progress at both table-level (version) and partition-level (files).
//!
//! # Idempotency and VACUUM Handling
//!
//! Delta Lake can compact (OPTIMIZE) and vacuum files, which changes the physical
//! file layout without changing the logical data. This creates challenges for
//! incremental sync:
//!
//! - **Compaction**: Files A + B → File C (same data, different file)
//! - **VACUUM**: Removes old files that are no longer referenced
//!
//! ## Mode Selection for Idempotent Loading
//!
//! | Mode | Behavior | When to Use |
//! |------|----------|-------------|
//! | `append` | INSERT only | Append-only tables with no compaction |
//! | `upsert` | INSERT ... ON CONFLICT UPDATE | Tables with primary keys (recommended) |
//! | `replace` | TRUNCATE + INSERT | Full refresh each time |
//!
//! **For idempotent loading with compaction, use `upsert` mode with `key_columns`.**
//! This ensures that re-ingesting compacted data updates existing rows rather than
//! creating duplicates.
//!
//! ## File Tracking
//!
//! We track processed files in `delta.partition_progress`. When files are removed
//! by VACUUM, we detect this and prune our tracking. New files (including compacted
//! files) are processed as new data.

use super::manager::StreamConfig;
use crate::storage::merge_storage_options;
use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema, TimeUnit};
use deltalake::kernel::DataType as DeltaDataType;
use pgrx::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Convert Delta Lake data type to Arrow data type.
fn delta_type_to_arrow(dt: &DeltaDataType) -> ArrowDataType {
    use deltalake::kernel::PrimitiveType;
    match dt {
        DeltaDataType::Primitive(p) => match p {
            PrimitiveType::String => ArrowDataType::Utf8,
            PrimitiveType::Long => ArrowDataType::Int64,
            PrimitiveType::Integer => ArrowDataType::Int32,
            PrimitiveType::Short => ArrowDataType::Int16,
            PrimitiveType::Byte => ArrowDataType::Int8,
            PrimitiveType::Float => ArrowDataType::Float32,
            PrimitiveType::Double => ArrowDataType::Float64,
            PrimitiveType::Boolean => ArrowDataType::Boolean,
            PrimitiveType::Binary => ArrowDataType::Binary,
            PrimitiveType::Date => ArrowDataType::Date32,
            PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => {
                // Always use UTC timezone to avoid Delta Lake writer v7+ feature requirements
                ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::Decimal(_, _) => ArrowDataType::Float64, // Simplified
        },
        _ => ArrowDataType::Utf8, // Complex types as string fallback
    }
}

/// Generate a stable lock ID for ingest stream advisory locks.
fn stream_to_lock_id(stream_name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    "delta_ingest_stream".hash(&mut hasher);
    stream_name.hash(&mut hasher);
    (hasher.finish() as i64).abs()
}

/// Partition progress information.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields used for tracking state
struct PartitionProgress {
    partition_path: String,
    partition_values: serde_json::Value,
    delta_version: i64,
    files_processed: HashSet<String>,
    rows_synced: i64,
}

/// Ingest stream that polls a Delta table and writes to Postgres.
pub struct IngestStream {
    config: StreamConfig,
}

impl IngestStream {
    pub fn new(config: StreamConfig) -> Self {
        Self { config }
    }

    /// Poll for new data and ingest into Postgres with partition-aware tracking.
    ///
    /// Uses advisory locks to ensure only one poll runs at a time per stream.
    /// This prevents overlapping micro-batches when the background worker
    /// processes faster than the ingest can complete.
    pub async fn poll(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let lock_id = stream_to_lock_id(&self.config.name);

        // Try to acquire advisory lock (non-blocking)
        let acquired = Spi::get_one::<bool>(&format!("SELECT pg_try_advisory_lock({})", lock_id))
            .unwrap_or(Some(false))
            .unwrap_or(false);

        if !acquired {
            log!(
                "pg_delta: ingest stream '{}' already running (lock_id={}), skipping",
                self.config.name,
                lock_id
            );
            return Ok(0);
        }

        // Execute the poll with lock held
        let result = self.poll_inner().await;

        // Always release the lock
        let _ = Spi::run(&format!("SELECT pg_advisory_unlock({})", lock_id));

        result
    }

    /// Inner implementation of poll (without locking).
    async fn poll_inner(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // Register storage handlers
        register_storage_handlers(&self.config.uri);

        // Build storage options
        let storage_options =
            merge_storage_options(&self.config.uri, self.config.storage_options.as_ref());

        // Open the Delta table
        let table =
            deltalake::open_table_with_storage_options(&self.config.uri, storage_options.clone())
                .await?;

        let latest_version = table.version();

        // Get current table-level progress
        let current_version = get_current_version(&self.config.name)?;
        let start_version =
            current_version.unwrap_or_else(|| self.config.start_version.unwrap_or(0));

        if latest_version <= start_version {
            // No new data at table level
            return Ok(0);
        }

        log!(
            "pg_delta: ingest '{}' - processing versions {} to {}",
            self.config.name,
            start_version + 1,
            latest_version
        );

        // Get the schema
        let delta_schema = table.snapshot()?.schema();
        let arrow_schema: ArrowSchema = ArrowSchema::new(
            delta_schema
                .fields()
                .map(|f| {
                    arrow_schema::Field::new(
                        f.name(),
                        delta_type_to_arrow(f.data_type()),
                        f.is_nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        );

        // Get partition columns from metadata
        let partition_columns = table.metadata()?.partition_columns.clone();
        let is_partitioned = !partition_columns.is_empty();

        // Get all files with their partition info
        let files: Vec<_> = table.get_file_uris()?.collect();

        let total_rows: i64 = if is_partitioned {
            // Partition-aware processing
            self.process_partitioned(&files, &arrow_schema, &partition_columns, latest_version)
                .await?
        } else {
            // Simple non-partitioned processing
            self.process_non_partitioned(&files, &arrow_schema, latest_version)
                .await?
        };

        // Update table-level progress
        update_version_progress(&self.config.name, latest_version, total_rows)?;

        Ok(total_rows)
    }

    /// Process a partitioned Delta table.
    async fn process_partitioned(
        &self,
        files: &[String],
        schema: &ArrowSchema,
        partition_columns: &[String],
        current_version: i64,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // Group files by partition
        let mut partition_files: HashMap<String, Vec<String>> = HashMap::new();

        for file_uri in files {
            let partition_path = extract_partition_path(file_uri, partition_columns);
            partition_files
                .entry(partition_path)
                .or_default()
                .push(file_uri.clone());
        }

        // Load existing partition progress
        let stream_id = get_stream_id(&self.config.name)?;
        let existing_progress = load_partition_progress(stream_id)?;

        let mut total_rows: i64 = 0;

        for (partition_path, files) in partition_files {
            let partition_values = parse_partition_path(&partition_path);

            // Get or create progress for this partition
            let progress = existing_progress.get(&partition_path);

            // Determine which files need processing
            let files_to_process: Vec<&String> = if let Some(p) = progress {
                // Only process files not already processed
                files
                    .iter()
                    .filter(|f| !p.files_processed.contains(*f))
                    .collect()
            } else {
                // New partition - process all files
                files.iter().collect()
            };

            if files_to_process.is_empty() {
                continue;
            }

            log!(
                "pg_delta: partition '{}' - processing {} new files",
                partition_path,
                files_to_process.len()
            );

            let mut partition_rows: i64 = 0;
            let mut processed_files: Vec<String> = Vec::new();

            for file_uri in files_to_process {
                match self.process_file(file_uri, schema).await {
                    Ok(rows) => {
                        partition_rows += rows;
                        processed_files.push(file_uri.clone());
                    }
                    Err(e) => {
                        log!("pg_delta: error processing file '{}': {}", file_uri, e);
                        // Continue with other files, but don't mark this one as processed
                    }
                }
            }

            // Update partition progress
            update_partition_progress(
                stream_id,
                &partition_path,
                &partition_values,
                current_version,
                &processed_files,
                partition_rows,
            )?;

            total_rows += partition_rows;
        }

        Ok(total_rows)
    }

    /// Process a non-partitioned Delta table.
    /// Uses file-level tracking to only read files added since last sync.
    ///
    /// VACUUM handling: After VACUUM, some files we've tracked may be removed
    /// and replaced by compacted files. We handle this by:
    /// 1. Only processing files that are NEW (not in our stored list)
    /// 2. Updating our stored list to match current table state (pruning removed files)
    ///
    /// Note: This means after compaction, we will re-read compacted data. This is
    /// safe for append-only tables but may cause duplicates for tables with updates.
    /// For tables with updates/deletes, use upsert mode with key_columns.
    async fn process_non_partitioned(
        &self,
        files: &[String],
        schema: &ArrowSchema,
        current_version: i64,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // Use "__unpartitioned__" as the partition path for tracking non-partitioned tables
        const UNPARTITIONED_KEY: &str = "__unpartitioned__";

        // Get stream ID and load existing progress
        let stream_id = get_stream_id(&self.config.name)?;
        let existing_progress = load_partition_progress(stream_id)?;

        // Convert current files to a set for efficient lookup
        let current_files_set: HashSet<&String> = files.iter().collect();

        // Determine which files need processing
        let progress = existing_progress.get(UNPARTITIONED_KEY);
        let (files_to_process, previously_processed): (Vec<&String>, HashSet<String>) =
            if let Some(p) = progress {
                // Filter to only files that:
                // 1. Are currently in the table (not VACUUM'd)
                // 2. AND were previously processed
                let still_valid: HashSet<String> = p
                    .files_processed
                    .iter()
                    .filter(|f| current_files_set.contains(f))
                    .cloned()
                    .collect();

                // Check if any files were removed by VACUUM
                let vacuumed_count = p.files_processed.len() - still_valid.len();
                if vacuumed_count > 0 {
                    log!(
                        "pg_delta: ingest '{}' - {} files removed by VACUUM/compaction",
                        self.config.name,
                        vacuumed_count
                    );
                }

                // New files = current files - previously processed (that still exist)
                let new_files: Vec<&String> =
                    files.iter().filter(|f| !still_valid.contains(*f)).collect();

                (new_files, still_valid)
            } else {
                // First sync - process all files
                (files.iter().collect(), HashSet::new())
            };

        if files_to_process.is_empty() {
            // Even if no new files, we should update progress to prune VACUUM'd files
            if progress.is_some() {
                let current_files_vec: Vec<String> = previously_processed.into_iter().collect();
                replace_partition_progress(
                    stream_id,
                    UNPARTITIONED_KEY,
                    &serde_json::json!({}),
                    current_version,
                    &current_files_vec,
                )?;
            }
            log!(
                "pg_delta: ingest '{}' - no new files to process",
                self.config.name
            );
            return Ok(0);
        }

        log!(
            "pg_delta: ingest '{}' - processing {} new files (out of {} total)",
            self.config.name,
            files_to_process.len(),
            files.len()
        );

        let mut total_rows: i64 = 0;
        let mut successfully_processed: Vec<String> = previously_processed.into_iter().collect();

        for file_uri in files_to_process {
            match self.process_file(file_uri, schema).await {
                Ok(rows) => {
                    total_rows += rows;
                    successfully_processed.push(file_uri.clone());
                }
                Err(e) => {
                    log!("pg_delta: error processing file '{}': {}", file_uri, e);
                    // Continue with other files, but don't mark this one as processed
                }
            }
        }

        // Replace progress with current state (not append)
        // This ensures we don't accumulate stale file references
        replace_partition_progress(
            stream_id,
            UNPARTITIONED_KEY,
            &serde_json::json!({}),
            current_version,
            &successfully_processed,
        )?;

        Ok(total_rows)
    }

    /// Process a single Parquet file.
    async fn process_file(
        &self,
        file_uri: &str,
        schema: &ArrowSchema,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        // For now, only support local files
        if !file_uri.starts_with("file://") && !file_uri.starts_with("/") {
            // TODO: Use object_store for remote files
            return Err(format!("Remote file reading not yet implemented: {}", file_uri).into());
        }

        let path = file_uri.strip_prefix("file://").unwrap_or(file_uri);
        let file = File::open(path)?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut total_rows: i64 = 0;

        for batch_result in reader {
            let batch = batch_result?;
            let rows = self.insert_batch(&batch, schema)?;
            total_rows += rows;
        }

        Ok(total_rows)
    }

    /// Insert a RecordBatch into Postgres based on mode.
    fn insert_batch(
        &self,
        batch: &RecordBatch,
        _schema: &ArrowSchema,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        match self.config.mode.as_str() {
            "append" => self.insert_batch_append(batch),
            "upsert" => self.insert_batch_upsert(batch),
            "replace" => self.insert_batch_replace(batch),
            _ => Err(format!("Unknown ingest mode: {}", self.config.mode).into()),
        }
    }

    /// Append mode - simple INSERT.
    fn insert_batch_append(
        &self,
        batch: &RecordBatch,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let num_rows = batch.num_rows();

        // Build column names
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();

        // Insert rows in batches of 100 for efficiency
        let batch_size = 100;

        for chunk_start in (0..num_rows).step_by(batch_size) {
            let chunk_end = (chunk_start + batch_size).min(num_rows);

            let values: Vec<String> = (chunk_start..chunk_end)
                .map(|row_idx| build_row_values(batch, row_idx))
                .collect();

            let insert_sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                self.config.pg_table,
                columns.join(", "),
                values.join(", ")
            );

            Spi::run(&insert_sql)?;
        }

        Ok(num_rows as i64)
    }

    /// Upsert mode - INSERT ON CONFLICT UPDATE.
    fn insert_batch_upsert(
        &self,
        batch: &RecordBatch,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let num_rows = batch.num_rows();

        let key_columns = self
            .config
            .key_columns
            .as_ref()
            .ok_or_else(|| "key_columns required for upsert mode".to_string())?;

        // Build column names
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();

        // Build SET clause for non-key columns using EXCLUDED
        let update_columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .filter(|f| !key_columns.contains(&f.name().to_string()))
            .map(|f| format!("\"{}\" = EXCLUDED.\"{}\"", f.name(), f.name()))
            .collect();

        let conflict_cols = key_columns
            .iter()
            .map(|k| format!("\"{}\"", k))
            .collect::<Vec<_>>()
            .join(", ");

        // Insert rows one at a time for upsert (can't batch upserts easily)
        for row_idx in 0..num_rows {
            let values = build_row_values(batch, row_idx);

            let upsert_sql = format!(
                "INSERT INTO {} ({}) VALUES {} ON CONFLICT ({}) DO UPDATE SET {}",
                self.config.pg_table,
                columns.join(", "),
                values,
                conflict_cols,
                update_columns.join(", ")
            );

            Spi::run(&upsert_sql)?;
        }

        Ok(num_rows as i64)
    }

    /// Replace mode - TRUNCATE then INSERT.
    fn insert_batch_replace(
        &self,
        batch: &RecordBatch,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // Note: This truncates on every batch, which is not ideal
        // A better implementation would truncate once at the start
        // For now, this is a simplified version
        Spi::run(&format!("TRUNCATE TABLE {}", self.config.pg_table))?;
        self.insert_batch_append(batch)
    }
}

// =============================================================================
// Partition Helper Functions
// =============================================================================

/// Extract partition path from a file URI.
/// e.g., "s3://bucket/table/year=2024/month=01/file.parquet" -> "year=2024/month=01"
fn extract_partition_path(file_uri: &str, partition_columns: &[String]) -> String {
    // Find partition segments in the path
    let parts: Vec<&str> = file_uri.split('/').collect();

    let partition_parts: Vec<&str> = parts
        .iter()
        .filter(|part| {
            // Check if this part is a partition segment (contains '=')
            if let Some(eq_pos) = part.find('=') {
                let col_name = &part[..eq_pos];
                partition_columns.iter().any(|c| c == col_name)
            } else {
                false
            }
        })
        .copied()
        .collect();

    if partition_parts.is_empty() {
        "__unpartitioned__".to_string()
    } else {
        partition_parts.join("/")
    }
}

/// Parse partition path into JSON values.
/// e.g., "year=2024/month=01" -> {"year": "2024", "month": "01"}
fn parse_partition_path(partition_path: &str) -> serde_json::Value {
    if partition_path == "__unpartitioned__" {
        return serde_json::json!({});
    }

    let mut values = serde_json::Map::new();

    for part in partition_path.split('/') {
        if let Some(eq_pos) = part.find('=') {
            let key = &part[..eq_pos];
            let value = &part[eq_pos + 1..];
            // Try to parse as number, fallback to string
            if let Ok(num) = value.parse::<i64>() {
                values.insert(key.to_string(), serde_json::json!(num));
            } else if let Ok(num) = value.parse::<f64>() {
                values.insert(key.to_string(), serde_json::json!(num));
            } else {
                values.insert(key.to_string(), serde_json::json!(value));
            }
        }
    }

    serde_json::Value::Object(values)
}

// =============================================================================
// Database Helper Functions
// =============================================================================

/// Register storage handlers based on URI scheme.
fn register_storage_handlers(uri: &str) {
    let uri_lower = uri.to_lowercase();

    if uri_lower.starts_with("s3://") {
        deltalake_aws::register_handlers(None);
    } else if uri_lower.starts_with("az://") || uri_lower.starts_with("azure://") {
        deltalake_azure::register_handlers(None);
    } else if uri_lower.starts_with("gs://") {
        deltalake_gcp::register_handlers(None);
    }
}

/// Get stream ID by name.
fn get_stream_id(stream_name: &str) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM delta.streams WHERE name = $1",
        &[stream_name.into()],
    )?
    .ok_or_else(|| format!("Stream '{}' not found", stream_name))?;

    Ok(id)
}

/// Get current version from progress table.
fn get_current_version(
    stream_name: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error + Send + Sync>> {
    let version = Spi::get_one_with_args::<i64>(
        r#"
        SELECT p.delta_version
        FROM delta.stream_progress p
        JOIN delta.streams s ON s.id = p.stream_id
        WHERE s.name = $1
        "#,
        &[stream_name.into()],
    )?;

    Ok(version)
}

/// Update table-level version progress in database.
fn update_version_progress(
    stream_name: &str,
    version: i64,
    rows_synced: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Spi::run_with_args(
        r#"
        INSERT INTO delta.stream_progress (stream_id, delta_version, rows_synced, last_sync_at)
        SELECT id, $2, $3, now()
        FROM delta.streams WHERE name = $1
        ON CONFLICT (stream_id) DO UPDATE SET
            delta_version = $2,
            rows_synced = delta.stream_progress.rows_synced + $3,
            last_sync_at = now()
        "#,
        &[stream_name.into(), version.into(), rows_synced.into()],
    )?;

    Ok(())
}

/// Load partition progress for a stream.
fn load_partition_progress(
    stream_id: i32,
) -> Result<HashMap<String, PartitionProgress>, Box<dyn std::error::Error + Send + Sync>> {
    let mut progress_map = HashMap::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT partition_path, partition_values, delta_version, files_processed, rows_synced
                FROM delta.partition_progress
                WHERE stream_id = $1
                "#,
                None,
                &[stream_id.into()],
            )
            .map_err(|e| format!("Failed to load partition progress: {:?}", e))?;

        for row in result {
            let partition_path: String = row
                .get::<String>(1)
                .map_err(|e| format!("Failed to get partition_path: {}", e))?
                .ok_or("partition_path is null")?;

            let partition_values: pgrx::JsonB = row
                .get::<pgrx::JsonB>(2)
                .map_err(|e| format!("Failed to get partition_values: {}", e))?
                .ok_or("partition_values is null")?;

            let delta_version: i64 = row
                .get::<i64>(3)
                .map_err(|e| format!("Failed to get delta_version: {}", e))?
                .ok_or("delta_version is null")?;

            let files_processed: Option<Vec<String>> = row
                .get::<Vec<String>>(4)
                .map_err(|e| format!("Failed to get files_processed: {}", e))?;

            let rows_synced: i64 = row
                .get::<i64>(5)
                .map_err(|e| format!("Failed to get rows_synced: {}", e))?
                .unwrap_or(0);

            let files_set: HashSet<String> =
                files_processed.unwrap_or_default().into_iter().collect();

            progress_map.insert(
                partition_path.clone(),
                PartitionProgress {
                    partition_path,
                    partition_values: partition_values.0,
                    delta_version,
                    files_processed: files_set,
                    rows_synced,
                },
            );
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    Ok(progress_map)
}

/// Update partition progress in database.
fn update_partition_progress(
    stream_id: i32,
    partition_path: &str,
    partition_values: &serde_json::Value,
    delta_version: i64,
    new_files: &[String],
    rows_synced: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let partition_values_str = partition_values.to_string();

    // Build the array literal for new files
    let files_array = if new_files.is_empty() {
        "ARRAY[]::text[]".to_string()
    } else {
        format!(
            "ARRAY[{}]",
            new_files
                .iter()
                .map(|f| format!("'{}'", f.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    let query = format!(
        r#"
        INSERT INTO delta.partition_progress
            (stream_id, partition_path, partition_values, delta_version, files_processed, rows_synced, last_sync_at)
        VALUES
            ($1, $2, $3::jsonb, $4, {}, $5, now())
        ON CONFLICT (stream_id, partition_path) DO UPDATE SET
            delta_version = $4,
            files_processed = delta.partition_progress.files_processed || {},
            rows_synced = delta.partition_progress.rows_synced + $5,
            last_sync_at = now()
        "#,
        files_array, files_array
    );

    Spi::run_with_args(
        &query,
        &[
            stream_id.into(),
            partition_path.into(),
            partition_values_str.as_str().into(),
            delta_version.into(),
            rows_synced.into(),
        ],
    )?;

    Ok(())
}

/// Replace partition progress in database (overwrites files_processed instead of appending).
/// This is used after VACUUM to ensure we don't keep stale file references.
fn replace_partition_progress(
    stream_id: i32,
    partition_path: &str,
    partition_values: &serde_json::Value,
    delta_version: i64,
    all_files: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let partition_values_str = partition_values.to_string();

    // Build the array literal for all files
    let files_array = if all_files.is_empty() {
        "ARRAY[]::text[]".to_string()
    } else {
        format!(
            "ARRAY[{}]",
            all_files
                .iter()
                .map(|f| format!("'{}'", f.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    let query = format!(
        r#"
        INSERT INTO delta.partition_progress
            (stream_id, partition_path, partition_values, delta_version, files_processed, rows_synced, last_sync_at)
        VALUES
            ($1, $2, $3::jsonb, $4, {}, 0, now())
        ON CONFLICT (stream_id, partition_path) DO UPDATE SET
            delta_version = $4,
            files_processed = {},
            last_sync_at = now()
        "#,
        files_array, files_array
    );

    Spi::run_with_args(
        &query,
        &[
            stream_id.into(),
            partition_path.into(),
            partition_values_str.as_str().into(),
            delta_version.into(),
        ],
    )?;

    Ok(())
}

// =============================================================================
// SQL Value Formatting
// =============================================================================

/// Format a single value from an Arrow array as SQL literal.
fn format_arrow_value_as_sql(array: &arrow_array::ArrayRef, index: usize) -> String {
    use arrow_array::*;
    use arrow_schema::{DataType, TimeUnit};

    if array.is_null(index) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("downcast guaranteed by DataType::Boolean match");
            if arr.value(index) { "true" } else { "false" }.to_string()
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("downcast guaranteed by DataType::Int16 match");
            arr.value(index).to_string()
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("downcast guaranteed by DataType::Int32 match");
            arr.value(index).to_string()
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("downcast guaranteed by DataType::Int64 match");
            arr.value(index).to_string()
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("downcast guaranteed by DataType::Float32 match");
            arr.value(index).to_string()
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("downcast guaranteed by DataType::Float64 match");
            arr.value(index).to_string()
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("downcast guaranteed by DataType::Utf8 match");
            let val = arr.value(index);
            // Escape single quotes
            format!("'{}'", val.replace('\'', "''"))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("downcast guaranteed by DataType::LargeUtf8 match");
            let val = arr.value(index);
            format!("'{}'", val.replace('\'', "''"))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("downcast guaranteed by DataType::Binary match");
            let val = arr.value(index);
            format!("'\\x{}'", hex::encode(val))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("downcast guaranteed by DataType::Date32 match");
            let days = arr.value(index);
            // Convert days since Unix epoch to date
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(
                days + 719163, // Days from CE to Unix epoch
            )
            .unwrap_or_default();
            format!("'{}'::date", date.format("%Y-%m-%d"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("downcast guaranteed by DataType::Timestamp match");
            let micros = arr.value(index);
            // Convert microseconds to timestamp string
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
            format!("'{}'::timestamptz", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        _ => {
            // Fallback to NULL for unsupported types
            "NULL".to_string()
        }
    }
}

/// Build VALUES clause for a row from RecordBatch.
fn build_row_values(batch: &RecordBatch, row_idx: usize) -> String {
    let values: Vec<String> = (0..batch.num_columns())
        .map(|col_idx| format_arrow_value_as_sql(batch.column(col_idx), row_idx))
        .collect();

    format!("({})", values.join(", "))
}
