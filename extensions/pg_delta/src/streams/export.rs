//! Export stream - PostgreSQL to Delta Lake.

use super::manager::StreamConfig;
use crate::storage::merge_storage_options;
use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema, TimeUnit};
use pgrx::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Generate a stable lock ID for export stream advisory locks.
fn stream_to_lock_id(stream_name: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    "delta_export_stream".hash(&mut hasher);
    stream_name.hash(&mut hasher);
    (hasher.finish() as i64).abs()
}

/// Export stream that reads from Postgres and writes to Delta Lake.
pub struct ExportStream {
    config: StreamConfig,
}

impl ExportStream {
    pub fn new(config: StreamConfig) -> Self {
        Self { config }
    }

    /// Poll for new data and export to Delta.
    ///
    /// Uses advisory locks to ensure only one poll runs at a time per stream.
    /// This prevents overlapping micro-batches when the background worker
    /// processes faster than the export can complete.
    pub async fn poll(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let lock_id = stream_to_lock_id(&self.config.name);

        // Try to acquire advisory lock (non-blocking)
        let acquired = Spi::get_one::<bool>(&format!("SELECT pg_try_advisory_lock({})", lock_id))
            .unwrap_or(Some(false))
            .unwrap_or(false);

        if !acquired {
            log!(
                "pg_delta: export stream '{}' already running (lock_id={}), skipping",
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
        match self.config.mode.as_str() {
            "incremental" => self.poll_incremental().await,
            "snapshot" => self.poll_snapshot().await,
            "cdc" => {
                // CDC requires logical replication setup - placeholder for now
                log!(
                    "pg_delta: CDC mode not yet implemented for stream '{}'",
                    self.config.name
                );
                Ok(0)
            }
            _ => Err(format!("Unknown export mode: {}", self.config.mode).into()),
        }
    }

    /// Incremental export - poll based on tracking column.
    async fn poll_incremental(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let tracking_column = self
            .config
            .tracking_column
            .as_ref()
            .ok_or_else(|| "tracking_column required for incremental mode".to_string())?;

        // Get last exported value
        let last_value = get_last_value(&self.config.name)?;

        // Build query for new rows
        let query = if let Some(ref last) = last_value {
            format!(
                "SELECT * FROM {} WHERE {} > {} ORDER BY {} LIMIT {}",
                self.config.pg_table,
                tracking_column,
                last,
                tracking_column,
                self.config.batch_size
            )
        } else {
            format!(
                "SELECT * FROM {} ORDER BY {} LIMIT {}",
                self.config.pg_table, tracking_column, self.config.batch_size
            )
        };

        // Execute query and convert to Arrow
        let (batches, new_last_value) = query_to_arrow_batches(&query, tracking_column)?;

        if batches.is_empty() {
            return Ok(0);
        }

        let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        // Write to Delta
        self.write_to_delta(batches).await?;

        // Update progress
        if let Some(last) = new_last_value {
            update_last_value(&self.config.name, &last)?;
        }

        Ok(total_rows)
    }

    /// Snapshot export - full table dump.
    async fn poll_snapshot(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        // For snapshot mode, we export the entire table
        // This should typically be run on a schedule, not continuously
        let query = format!("SELECT * FROM {}", self.config.pg_table);

        let (batches, _) = query_to_arrow_batches(&query, "")?;

        if batches.is_empty() {
            return Ok(0);
        }

        let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        // Write to Delta with overwrite mode
        self.write_to_delta_overwrite(batches).await?;

        Ok(total_rows)
    }

    /// Write batches to Delta Lake (append mode).
    async fn write_to_delta(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        register_storage_handlers(&self.config.uri);

        let storage_options =
            merge_storage_options(&self.config.uri, self.config.storage_options.as_ref());

        // Try to open existing table or create new one
        let table = match deltalake::open_table_with_storage_options(
            &self.config.uri,
            storage_options.clone(),
        )
        .await
        {
            Ok(t) => t,
            Err(_) => {
                // Create new table
                let ops = deltalake::DeltaOps::try_from_uri_with_storage_options(
                    &self.config.uri,
                    storage_options,
                )
                .await?;

                ops.create()
                    .with_columns(schema_to_delta_columns(batches[0].schema().as_ref()))
                    .await?
            }
        };

        // Write with append mode
        deltalake::DeltaOps(table)
            .write(batches)
            .with_save_mode(deltalake::protocol::SaveMode::Append)
            .await?;

        Ok(())
    }

    /// Write batches to Delta Lake (overwrite mode for snapshots).
    async fn write_to_delta_overwrite(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        register_storage_handlers(&self.config.uri);

        let storage_options =
            merge_storage_options(&self.config.uri, self.config.storage_options.as_ref());

        // Try to open existing table or create new one
        let table = match deltalake::open_table_with_storage_options(
            &self.config.uri,
            storage_options.clone(),
        )
        .await
        {
            Ok(t) => t,
            Err(_) => {
                // Create new table
                let ops = deltalake::DeltaOps::try_from_uri_with_storage_options(
                    &self.config.uri,
                    storage_options,
                )
                .await?;

                ops.create()
                    .with_columns(schema_to_delta_columns(batches[0].schema().as_ref()))
                    .await?
            }
        };

        // Write with overwrite mode
        deltalake::DeltaOps(table)
            .write(batches)
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
            .await?;

        Ok(())
    }
}

// =============================================================================
// Helper Functions
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

/// Get last exported value from progress table.
fn get_last_value(
    stream_name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    let value = Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT p.last_value
        FROM delta.stream_progress p
        JOIN delta.streams s ON s.id = p.stream_id
        WHERE s.name = $1
        "#,
        &[stream_name.into()],
    )?;

    Ok(value.and_then(|v| v.0.as_str().map(|s| s.to_string())))
}

/// Update last exported value in progress table.
fn update_last_value(
    stream_name: &str,
    last_value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json_value = format!("\"{}\"", last_value);
    Spi::run_with_args(
        r#"
        INSERT INTO delta.stream_progress (stream_id, last_value, last_sync_at)
        SELECT id, $2::jsonb, now()
        FROM delta.streams WHERE name = $1
        ON CONFLICT (stream_id) DO UPDATE SET
            last_value = $2::jsonb,
            last_sync_at = now()
        "#,
        &[stream_name.into(), json_value.as_str().into()],
    )?;

    Ok(())
}

/// Execute query and convert to Arrow RecordBatches.
/// Note: This is a simplified implementation. A full implementation would
/// need to query pg_catalog for column types and build proper Arrow arrays.
fn query_to_arrow_batches(
    _query: &str,
    _tracking_column: &str,
) -> Result<(Vec<RecordBatch>, Option<String>), Box<dyn std::error::Error + Send + Sync>> {
    // TODO: Implement proper query-to-Arrow conversion
    // This requires querying pg_catalog for column metadata and proper type conversion
    // The pgrx 0.16 SPI API doesn't expose column metadata in a way that's easy to use
    // for dynamic schema discovery. This would need to use pg_catalog queries to get
    // column types, then build Arrow arrays accordingly.
    Err("Export stream query_to_arrow_batches not yet fully implemented".into())
}

/// Infer Arrow type from Postgres type OID.
#[allow(dead_code)] // Will be used when query_to_arrow_batches is fully implemented
fn infer_arrow_type_from_pg(type_oid: pgrx::pg_sys::Oid) -> DataType {
    match type_oid.to_u32() {
        16 => DataType::Boolean,                                         // BOOLOID
        21 => DataType::Int16,                                           // INT2OID
        23 => DataType::Int32,                                           // INT4OID
        20 => DataType::Int64,                                           // INT8OID
        700 => DataType::Float32,                                        // FLOAT4OID
        701 => DataType::Float64,                                        // FLOAT8OID
        25 | 1043 | 1042 => DataType::Utf8,                              // TEXT, VARCHAR, BPCHAR
        17 => DataType::Binary,                                          // BYTEAOID
        1082 => DataType::Date32,                                        // DATEOID
        1114 | 1184 => DataType::Timestamp(TimeUnit::Microsecond, None), // TIMESTAMP, TIMESTAMPTZ
        1700 => DataType::Float64,                                       // NUMERICOID
        114 | 3802 => DataType::Utf8,                                    // JSON, JSONB
        2950 => DataType::Utf8,                                          // UUIDOID
        _ => DataType::Utf8,                                             // Default to text
    }
}

/// Build Arrow arrays from string row data.
#[allow(dead_code)] // Will be used when query_to_arrow_batches is fully implemented
fn build_arrow_arrays(
    column_types: &[DataType],
    rows: &[Vec<Option<String>>],
) -> Result<Vec<ArrayRef>, Box<dyn std::error::Error + Send + Sync>> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_types.len());

    for (col_idx, col_type) in column_types.iter().enumerate() {
        let values: Vec<Option<&str>> = rows
            .iter()
            .map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
            .collect();

        let array: ArrayRef = match col_type {
            DataType::Boolean => {
                let arr: BooleanArray = values
                    .iter()
                    .map(|v| v.map(|s| s == "t" || s == "true" || s == "1"))
                    .collect();
                Arc::new(arr)
            }
            DataType::Int16 => {
                let arr: Int16Array = values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(arr)
            }
            DataType::Int32 => {
                let arr: Int32Array = values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(arr)
            }
            DataType::Int64 => {
                let arr: Int64Array = values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(arr)
            }
            DataType::Float32 => {
                let arr: Float32Array = values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(arr)
            }
            DataType::Float64 => {
                let arr: Float64Array = values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                Arc::new(arr)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                // Parse timestamp strings to microseconds since epoch
                let arr: TimestampMicrosecondArray = values
                    .iter()
                    .map(|v| {
                        v.and_then(|_| {
                            // Simplified parsing - would need proper timestamp parsing
                            None::<i64>
                        })
                    })
                    .collect();
                Arc::new(arr)
            }
            _ => {
                // Default: string array
                let arr: StringArray = values.iter().copied().collect();
                Arc::new(arr)
            }
        };

        arrays.push(array);
    }

    Ok(arrays)
}

/// Convert Arrow schema to Delta column definitions.
fn schema_to_delta_columns(schema: &ArrowSchema) -> Vec<deltalake::kernel::StructField> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let delta_type = arrow_type_to_delta_type(f.data_type());
            deltalake::kernel::StructField::new(f.name(), delta_type, f.is_nullable())
        })
        .collect()
}

/// Convert Arrow DataType to Delta DataType.
fn arrow_type_to_delta_type(dt: &DataType) -> deltalake::kernel::DataType {
    use deltalake::kernel::DataType as DeltaType;
    use deltalake::kernel::PrimitiveType;

    match dt {
        DataType::Boolean => DeltaType::Primitive(PrimitiveType::Boolean),
        DataType::Int8 | DataType::Int16 => DeltaType::Primitive(PrimitiveType::Short),
        DataType::Int32 => DeltaType::Primitive(PrimitiveType::Integer),
        DataType::Int64 => DeltaType::Primitive(PrimitiveType::Long),
        DataType::Float32 => DeltaType::Primitive(PrimitiveType::Float),
        DataType::Float64 => DeltaType::Primitive(PrimitiveType::Double),
        DataType::Utf8 | DataType::LargeUtf8 => DeltaType::Primitive(PrimitiveType::String),
        DataType::Binary | DataType::LargeBinary => DeltaType::Primitive(PrimitiveType::Binary),
        DataType::Date32 | DataType::Date64 => DeltaType::Primitive(PrimitiveType::Date),
        DataType::Timestamp(_, _) => DeltaType::Primitive(PrimitiveType::Timestamp),
        _ => DeltaType::Primitive(PrimitiveType::String), // Fallback
    }
}
