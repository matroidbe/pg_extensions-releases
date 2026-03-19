//! Delta Lake operations - read, write, metadata queries.

use super::config::build_storage_options;
use super::convert::generate_create_table_sql;
use arrow_array::RecordBatch;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use deltalake::kernel::DataType as DeltaDataType;
use pgrx::prelude::*;

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

/// Convert Delta schema to Arrow schema.
fn delta_schema_to_arrow(delta_schema: &deltalake::kernel::StructType) -> ArrowSchema {
    ArrowSchema::new(
        delta_schema
            .fields()
            .map(|f| {
                ArrowField::new(
                    f.name(),
                    delta_type_to_arrow(f.data_type()),
                    f.is_nullable(),
                )
            })
            .collect::<Vec<_>>(),
    )
}

/// Read a Delta table into a Postgres table.
pub fn read_delta_to_postgres(
    uri: &str,
    pg_table: &str,
    mode: &str,
    version: Option<i64>,
    _timestamp: Option<pgrx::datum::TimestampWithTimeZone>,
) -> i64 {
    // Validate mode
    if mode != "append" && mode != "replace" {
        pgrx::error!("mode must be 'append' or 'replace', got '{}'", mode);
    }

    // Register storage handlers
    register_storage_handlers(uri);

    let storage_options = build_storage_options(uri);

    // Create tokio runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let rows_inserted = runtime.block_on(async {
        // Open the Delta table
        let table = if let Some(v) = version {
            deltalake::open_table_with_version(uri, v)
                .await
                .map_err(|e| format!("Failed to open Delta table at version {}: {}", v, e))?
        } else {
            deltalake::open_table_with_storage_options(uri, storage_options)
                .await
                .map_err(|e| format!("Failed to open Delta table: {}", e))?
        };

        // Get schema
        let delta_schema = table
            .snapshot()
            .map_err(|e| format!("Failed to get snapshot: {}", e))?
            .schema();
        let arrow_schema = delta_schema_to_arrow(delta_schema);

        // Create or truncate the target table
        prepare_target_table(pg_table, &arrow_schema, mode)?;

        // Read all files
        let files: Vec<_> = table
            .get_file_uris()
            .map_err(|e| format!("Failed to get file URIs: {}", e))?
            .collect();

        let mut total_rows: i64 = 0;

        for file_uri in files {
            let rows = read_parquet_file_to_postgres(&file_uri, pg_table, &arrow_schema).await?;
            total_rows += rows;
        }

        Ok::<i64, String>(total_rows)
    });

    match rows_inserted {
        Ok(count) => {
            log!(
                "pg_delta: read {} rows from {} into {}",
                count,
                uri,
                pg_table
            );
            count
        }
        Err(e) => {
            pgrx::error!("Failed to read Delta table: {}", e);
        }
    }
}

/// Write Postgres data to a Delta table.
pub fn write_postgres_to_delta(
    uri: &str,
    query_or_table: &str,
    is_query: bool,
    mode: &str,
    _partition_by: Option<Vec<String>>,
) -> i64 {
    // Validate mode
    if mode != "append" && mode != "overwrite" {
        pgrx::error!("mode must be 'append' or 'overwrite', got '{}'", mode);
    }

    // Register storage handlers
    register_storage_handlers(uri);

    let storage_options = build_storage_options(uri);

    // Build the query
    let query = if is_query {
        query_or_table.to_string()
    } else {
        format!("SELECT * FROM {}", query_or_table)
    };

    // Create tokio runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    // Try to get existing table schema (for proper column naming when appending)
    let existing_schema: Option<ArrowSchema> = runtime.block_on(async {
        match deltalake::open_table_with_storage_options(uri, storage_options.clone()).await {
            Ok(table) => {
                // Convert Delta schema to Arrow schema
                let delta_schema = table.get_schema().ok()?;
                Some(delta_schema_to_arrow(delta_schema))
            }
            Err(_) => None,
        }
    });

    // Get table name hint for schema lookup
    // If we have an existing table, use its schema; otherwise try to get from pg_table param
    let table_name = if existing_schema.is_some() {
        // We'll use the existing Delta schema
        None
    } else if !is_query {
        Some(query_or_table.to_string())
    } else {
        None
    };

    // Execute query and convert to Arrow
    let (batches, schema) =
        match table_to_arrow_batches_with_schema(&query, table_name.as_deref(), existing_schema) {
            Ok(result) => result,
            Err(e) => {
                pgrx::error!("Failed to convert query to Arrow: {}", e);
            }
        };

    if batches.is_empty() {
        log!("pg_delta: no data to write to {}", uri);
        return 0;
    }

    let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    let result = runtime.block_on(async {
        // Try to open existing table or create new one
        let table = match deltalake::open_table_with_storage_options(uri, storage_options.clone())
            .await
        {
            Ok(t) => t,
            Err(_) => {
                // Create new table
                let ops =
                    deltalake::DeltaOps::try_from_uri_with_storage_options(uri, storage_options)
                        .await
                        .map_err(|e| format!("Failed to create DeltaOps: {}", e))?;

                ops.create()
                    .with_columns(schema_to_delta_columns(&schema))
                    .await
                    .map_err(|e| format!("Failed to create Delta table: {}", e))?
            }
        };

        // Write batches
        let save_mode = match mode {
            "overwrite" => deltalake::protocol::SaveMode::Overwrite,
            _ => deltalake::protocol::SaveMode::Append,
        };

        deltalake::DeltaOps(table)
            .write(batches)
            .with_save_mode(save_mode)
            .await
            .map_err(|e| format!("Failed to write to Delta table: {}", e))?;

        Ok::<(), String>(())
    });

    match result {
        Ok(_) => {
            log!("pg_delta: wrote {} rows to {}", total_rows, uri);
            total_rows
        }
        Err(e) => {
            pgrx::error!("Failed to write to Delta table: {}", e);
        }
    }
}

/// Get metadata about a Delta table.
pub fn get_delta_info(uri: &str) -> pgrx::JsonB {
    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let info = runtime.block_on(async {
        let table = deltalake::open_table_with_storage_options(uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| format!("Failed to get snapshot: {}", e))?;

        let version = table.version();
        let files: Vec<_> = table
            .get_file_uris()
            .map_err(|e| format!("Failed to get file URIs: {}", e))?
            .collect();

        let num_files = files.len();

        // Get metadata
        let metadata = snapshot.metadata();

        Ok::<serde_json::Value, String>(serde_json::json!({
            "version": version,
            "num_files": num_files,
            "name": metadata.name,
            "description": metadata.description,
            "partition_columns": metadata.partition_columns,
            "created_at": metadata.created_time,
        }))
    });

    match info {
        Ok(v) => pgrx::JsonB(v),
        Err(e) => {
            pgrx::error!("Failed to get Delta table info: {}", e);
        }
    }
}

/// Get the schema of a Delta table.
pub fn get_delta_schema(uri: &str) -> pgrx::JsonB {
    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let schema_json = runtime.block_on(async {
        let table = deltalake::open_table_with_storage_options(uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        let snapshot = table
            .snapshot()
            .map_err(|e| format!("Failed to get snapshot: {}", e))?;

        let schema = snapshot.schema();

        let columns: Vec<serde_json::Value> = schema
            .fields()
            .map(|f| {
                serde_json::json!({
                    "name": f.name(),
                    "type": format!("{:?}", f.data_type()),
                    "nullable": f.is_nullable(),
                    "metadata": f.metadata(),
                })
            })
            .collect();

        Ok::<serde_json::Value, String>(serde_json::json!({
            "columns": columns
        }))
    });

    match schema_json {
        Ok(v) => pgrx::JsonB(v),
        Err(e) => {
            pgrx::error!("Failed to get Delta table schema: {}", e);
        }
    }
}

/// Get transaction history of a Delta table.
pub fn get_delta_history(uri: &str, limit: i32) -> pgrx::JsonB {
    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let history_json = runtime.block_on(async {
        let table = deltalake::open_table_with_storage_options(uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        let history = table
            .history(Some(limit as usize))
            .await
            .map_err(|e| format!("Failed to get history: {}", e))?;

        // Get current table version to calculate version numbers
        let current_version = table.version();
        let entries: Vec<serde_json::Value> = history
            .iter()
            .enumerate()
            .map(|(idx, h)| {
                // History is returned in reverse order (newest first)
                let version = current_version - idx as i64;
                serde_json::json!({
                    "version": version,
                    "timestamp": h.timestamp,
                    "operation": h.operation,
                    "parameters": h.operation_parameters,
                })
            })
            .collect();

        Ok::<serde_json::Value, String>(serde_json::json!({
            "history": entries
        }))
    });

    match history_json {
        Ok(v) => pgrx::JsonB(v),
        Err(e) => {
            pgrx::error!("Failed to get Delta table history: {}", e);
        }
    }
}

/// Test storage connectivity.
pub fn test_storage_connectivity(uri: &str) -> pgrx::JsonB {
    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        let mut can_list = false;
        let mut can_read = false;
        let mut error_message: Option<String> = None;

        // Try to open the table
        match deltalake::open_table_with_storage_options(uri, storage_options).await {
            Ok(table) => {
                can_list = true;

                // Try to read files
                match table.get_file_uris() {
                    Ok(_) => {
                        can_read = true;
                    }
                    Err(e) => {
                        error_message = Some(format!("Cannot read files: {}", e));
                    }
                }
            }
            Err(e) => {
                // Check if it's a "not found" error (table doesn't exist yet)
                let err_str = e.to_string();
                if err_str.contains("not found") || err_str.contains("NotFound") {
                    can_list = true; // We can list, but table doesn't exist
                    error_message = Some("Table does not exist (can be created)".to_string());
                } else {
                    error_message = Some(format!("Cannot access storage: {}", e));
                }
            }
        }

        serde_json::json!({
            "can_list": can_list,
            "can_read": can_read,
            "error_message": error_message,
        })
    });

    pgrx::JsonB(result)
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

/// Prepare target Postgres table (create or truncate).
fn prepare_target_table(pg_table: &str, schema: &ArrowSchema, mode: &str) -> Result<(), String> {
    // Create table if not exists
    let create_sql = generate_create_table_sql(pg_table, schema);
    Spi::run(&create_sql).map_err(|e| format!("Failed to create table: {}", e))?;

    // Truncate if replace mode
    if mode == "replace" {
        let truncate_sql = format!("TRUNCATE TABLE {}", pg_table);
        Spi::run(&truncate_sql).map_err(|e| format!("Failed to truncate table: {}", e))?;
    }

    Ok(())
}

/// Read a Parquet file and insert into Postgres table.
async fn read_parquet_file_to_postgres(
    file_uri: &str,
    pg_table: &str,
    _schema: &ArrowSchema,
) -> Result<i64, String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    // For now, only support local files
    // TODO: Support remote files via object_store
    if !file_uri.starts_with("file://") && !file_uri.starts_with("/") {
        return Err(format!(
            "Remote file reading not yet implemented: {}",
            file_uri
        ));
    }

    let path = file_uri.strip_prefix("file://").unwrap_or(file_uri);

    let file = File::open(path).map_err(|e| format!("Failed to open file {}: {}", path, e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to create Parquet reader: {}", e))?;

    let reader = builder
        .build()
        .map_err(|e| format!("Failed to build Parquet reader: {}", e))?;

    let mut total_rows: i64 = 0;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("Failed to read batch: {}", e))?;
        let rows = insert_batch_to_postgres(&batch, pg_table)?;
        total_rows += rows;
    }

    Ok(total_rows)
}

/// Insert an Arrow RecordBatch into Postgres.
fn insert_batch_to_postgres(batch: &RecordBatch, pg_table: &str) -> Result<i64, String> {
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
            .map(|row_idx| format_row_values(batch, row_idx))
            .collect();

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            pg_table,
            columns.join(", "),
            values.join(", ")
        );

        Spi::run(&insert_sql).map_err(|e| format!("Failed to insert row: {}", e))?;
    }

    Ok(num_rows as i64)
}

/// Format a single value from an Arrow array as SQL literal.
fn format_arrow_value_sql(array: &arrow_array::ArrayRef, index: usize) -> String {
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
            let date =
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
            format!("'{}'::date", date.format("%Y-%m-%d"))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("downcast guaranteed by DataType::Timestamp match");
            let micros = arr.value(index);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
            format!("'{}'::timestamptz", dt.format("%Y-%m-%d %H:%M:%S%.6f"))
        }
        _ => "NULL".to_string(),
    }
}

/// Format a row as VALUES clause.
fn format_row_values(batch: &RecordBatch, row_idx: usize) -> String {
    let values: Vec<String> = (0..batch.num_columns())
        .map(|col_idx| format_arrow_value_sql(batch.column(col_idx), row_idx))
        .collect();
    format!("({})", values.join(", "))
}

/// Execute a query and convert results to Arrow RecordBatches.
#[allow(dead_code)]
fn query_to_arrow(query: &str) -> Result<(Vec<RecordBatch>, ArrowSchema), String> {
    // Use the table-aware implementation without table name hint
    table_to_arrow_batches(query, None)
}

/// Execute a query and convert results to Arrow RecordBatches.
/// When table_name is provided, uses information_schema to get proper column names and types.
fn table_to_arrow_batches(
    query: &str,
    table_name: Option<&str>,
) -> Result<(Vec<RecordBatch>, ArrowSchema), String> {
    table_to_arrow_batches_with_schema(query, table_name, None)
}

/// Execute a query and convert results to Arrow RecordBatches.
/// When existing_schema is provided, uses it for column names and types.
/// Otherwise, when table_name is provided, uses information_schema.
fn table_to_arrow_batches_with_schema(
    query: &str,
    table_name: Option<&str>,
    existing_schema: Option<ArrowSchema>,
) -> Result<(Vec<RecordBatch>, ArrowSchema), String> {
    use super::convert::build_arrow_schema_from_pg_table;
    use arrow_array::*;
    use std::sync::Arc;

    // Determine schema to use:
    // 1. If existing_schema is provided (from existing Delta table), use it
    // 2. Otherwise, if we have a table name, get from information_schema
    // 3. Otherwise, fall back to generic column names
    let schema = if let Some(s) = existing_schema {
        Some(s)
    } else if let Some(tbl) = table_name {
        match build_arrow_schema_from_pg_table(tbl) {
            Ok(s) => Some(s),
            Err(e) => {
                log!(
                    "pg_delta: failed to get schema for {}, falling back: {}",
                    tbl,
                    e
                );
                None
            }
        }
    } else {
        None
    };

    // Execute query and collect data
    let mut columns_data: Vec<(String, ArrowDataType, bool, Vec<Option<String>>)> = Vec::new();
    let mut row_count = 0;

    Spi::connect(|client| {
        let result = client
            .select(query, None, &[])
            .map_err(|e| format!("Query failed: {:?}", e))?;

        // Initialize columns from schema or from query result
        let mut initialized = false;

        for row in result {
            if !initialized {
                if let Some(ref s) = schema {
                    // Use pre-fetched schema
                    for field in s.fields() {
                        columns_data.push((
                            field.name().clone(),
                            field.data_type().clone(),
                            field.is_nullable(),
                            Vec::new(),
                        ));
                    }
                } else {
                    // Fallback: infer from query result (uses generic column names)
                    let num_cols = row.columns();
                    for col_idx in 1..=num_cols {
                        let col_name = format!("column_{}", col_idx);
                        columns_data.push((col_name, ArrowDataType::Utf8, true, Vec::new()));
                    }
                }
                initialized = true;
            }

            row_count += 1;

            // Extract values from each column as strings
            for (col_idx, (_name, dtype, _nullable, values)) in columns_data.iter_mut().enumerate()
            {
                // Try to get the value as the appropriate type and format it
                let val = get_column_value_as_string(&row, col_idx + 1, dtype);
                values.push(val);
            }
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    if columns_data.is_empty() || row_count == 0 {
        return Ok((Vec::new(), ArrowSchema::empty()));
    }

    // Build Arrow arrays with proper types
    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

    for (name, dtype, nullable, values) in columns_data {
        fields.push(arrow_schema::Field::new(&name, dtype.clone(), nullable));
        let array = build_arrow_array(&dtype, values)?;
        arrays.push(array);
    }

    let final_schema = ArrowSchema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(final_schema.clone()), arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

    Ok((vec![batch], final_schema))
}

/// Get a column value as a string representation.
fn get_column_value_as_string(
    row: &pgrx::spi::SpiHeapTupleData,
    col_idx: usize,
    dtype: &ArrowDataType,
) -> Option<String> {
    // Try to get the value based on the expected type
    // The SPI interface returns values that can be converted to string
    match dtype {
        ArrowDataType::Boolean => row
            .get::<bool>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        ArrowDataType::Int16 => row
            .get::<i16>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        ArrowDataType::Int32 => row
            .get::<i32>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        ArrowDataType::Int64 => row
            .get::<i64>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string()),
        ArrowDataType::Float32 => row
            .get::<f32>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string())
            .or_else(|| {
                // Postgres NUMERIC type must be read as AnyNumeric, not f32
                row.get::<pgrx::AnyNumeric>(col_idx)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            }),
        ArrowDataType::Float64 => row
            .get::<f64>(col_idx)
            .ok()
            .flatten()
            .map(|v| v.to_string())
            .or_else(|| {
                // Postgres NUMERIC type must be read as AnyNumeric, not f64
                row.get::<pgrx::AnyNumeric>(col_idx)
                    .ok()
                    .flatten()
                    .map(|v| v.to_string())
            }),
        ArrowDataType::Date32 => {
            // Get date as i32 days since epoch
            row.get::<pgrx::datum::Date>(col_idx)
                .ok()
                .flatten()
                .map(|v| {
                    // Convert pgrx Date to days since 2000-01-01, then adjust to Unix epoch
                    let days_since_2000 = v.to_pg_epoch_days();
                    // PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01
                    // Difference is 10957 days
                    (days_since_2000 + 10957).to_string()
                })
        }
        ArrowDataType::Timestamp(_, _) => {
            // Get timestamp and convert to microseconds since Unix epoch
            row.get::<pgrx::datum::Timestamp>(col_idx)
                .ok()
                .flatten()
                .map(|v| {
                    // pgrx Timestamp.into_inner() returns microseconds since Postgres epoch (2000-01-01)
                    // We need microseconds since Unix epoch (1970-01-01)
                    // Difference: 30 years = 946684800 seconds = 946684800000000 microseconds
                    const PG_EPOCH_TO_UNIX_EPOCH_MICROS: i64 = 946_684_800_000_000;
                    let pg_micros = v.into_inner();
                    let unix_micros = pg_micros + PG_EPOCH_TO_UNIX_EPOCH_MICROS;
                    unix_micros.to_string()
                })
        }
        ArrowDataType::Time64(_) | ArrowDataType::Time32(_) => {
            // Get time as string and convert to microseconds since midnight
            row.get::<pgrx::datum::Time>(col_idx)
                .ok()
                .flatten()
                .map(|v| {
                    // pgrx Time stores time as microseconds since midnight
                    // Access the inner value using 0 index (tuple struct)
                    let time_val: i64 = unsafe { std::mem::transmute::<pgrx::datum::Time, i64>(v) };
                    time_val.to_string()
                })
        }
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            // For string types, try various Postgres types that map to string
            // First try String (covers TEXT, VARCHAR, CHAR)
            if let Some(s) = row.get::<String>(col_idx).ok().flatten() {
                return Some(s);
            }
            // Try UUID (stored as Utf8 in Arrow/Delta)
            if let Some(u) = row.get::<pgrx::datum::Uuid>(col_idx).ok().flatten() {
                return Some(u.to_string());
            }
            // Try JsonB (stored as string in Delta)
            if let Some(j) = row.get::<pgrx::JsonB>(col_idx).ok().flatten() {
                return Some(j.0.to_string());
            }
            // Try Time (stored as string in Delta since Delta doesn't support Time)
            if let Some(t) = row.get::<pgrx::datum::Time>(col_idx).ok().flatten() {
                // Format as HH:MM:SS
                return Some(format!("{}", t));
            }
            None
        }
        _ => {
            // Default fallback: try to get as String
            row.get::<String>(col_idx).ok().flatten()
        }
    }
}

/// Build an Arrow array from string values based on the target data type.
fn build_arrow_array(
    dtype: &ArrowDataType,
    values: Vec<Option<String>>,
) -> Result<std::sync::Arc<dyn arrow_array::Array>, String> {
    use arrow_array::*;
    use std::sync::Arc;

    match dtype {
        ArrowDataType::Boolean => {
            let array: BooleanArray = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<bool>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Int16 => {
            let array: Int16Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i16>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Int32 => {
            let array: Int32Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Int64 => {
            let array: Int64Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Float32 => {
            let array: Float32Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Float64 => {
            let array: Float64Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Date32 => {
            let array: Date32Array = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Timestamp(_unit, tz) => {
            // For timestamps, try to parse as microseconds
            let base_array: TimestampMicrosecondArray = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                .collect();
            // Apply timezone if specified in the schema
            let array = if let Some(tz_str) = tz {
                base_array.with_timezone(tz_str.to_string())
            } else {
                base_array
            };
            Ok(Arc::new(array))
        }
        ArrowDataType::Time64(_) => {
            // For time, parse as microseconds since midnight
            let array: Time64MicrosecondArray = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        ArrowDataType::Time32(_) => {
            // For time32, parse as milliseconds since midnight
            let array: Time32MillisecondArray = values
                .into_iter()
                .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                .collect();
            Ok(Arc::new(array))
        }
        _ => {
            // Default: store as strings
            let array: StringArray = values.into_iter().collect();
            Ok(Arc::new(array))
        }
    }
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
fn arrow_type_to_delta_type(dt: &ArrowDataType) -> deltalake::kernel::DataType {
    use deltalake::kernel::DataType as DeltaType;
    use deltalake::kernel::PrimitiveType;

    match dt {
        ArrowDataType::Boolean => DeltaType::Primitive(PrimitiveType::Boolean),
        ArrowDataType::Int8 | ArrowDataType::Int16 => DeltaType::Primitive(PrimitiveType::Short),
        ArrowDataType::Int32 => DeltaType::Primitive(PrimitiveType::Integer),
        ArrowDataType::Int64 => DeltaType::Primitive(PrimitiveType::Long),
        ArrowDataType::Float32 => DeltaType::Primitive(PrimitiveType::Float),
        ArrowDataType::Float64 => DeltaType::Primitive(PrimitiveType::Double),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
            DeltaType::Primitive(PrimitiveType::String)
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            DeltaType::Primitive(PrimitiveType::Binary)
        }
        ArrowDataType::Date32 | ArrowDataType::Date64 => DeltaType::Primitive(PrimitiveType::Date),
        ArrowDataType::Timestamp(_, _) => DeltaType::Primitive(PrimitiveType::Timestamp),
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => {
            // Delta doesn't have a native Time type, store as string
            DeltaType::Primitive(PrimitiveType::String)
        }
        _ => DeltaType::Primitive(PrimitiveType::String), // Fallback
    }
}

// =============================================================================
// Managed Table Functions (create_table/refresh pattern)
// =============================================================================

/// Create a managed Postgres table from a Delta Lake table.
pub fn create_managed_table(
    uri: &str,
    pg_table: &str,
    storage_options_override: Option<pgrx::JsonB>,
) -> i64 {
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

    // Register storage handlers
    register_storage_handlers(uri);

    let storage_options = build_storage_options(uri);

    // Create tokio runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        // Open the Delta table
        let table = deltalake::open_table_with_storage_options(uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        let delta_version = table.version();

        // Get schema
        let snapshot = table
            .snapshot()
            .map_err(|e| format!("Failed to get snapshot: {}", e))?;
        let delta_schema = snapshot.schema();
        let arrow_schema = delta_schema_to_arrow(delta_schema);

        // Get partition columns
        let metadata = table
            .metadata()
            .map_err(|e| format!("Failed to get metadata: {}", e))?;
        let partition_columns = metadata.partition_columns.clone();

        // Create the Postgres table
        prepare_target_table(pg_table, &arrow_schema, "replace")?;

        // Register in delta.tables
        register_managed_table(uri, pg_table, &partition_columns, &storage_options_override)?;

        // Get all files
        let files: Vec<_> = table
            .get_file_uris()
            .map_err(|e| format!("Failed to get file URIs: {}", e))?
            .collect();

        // Load all data and track partition progress
        let total_rows = if !partition_columns.is_empty() {
            load_partitioned_data(
                pg_table,
                &files,
                &arrow_schema,
                &partition_columns,
                delta_version,
            )
            .await?
        } else {
            load_non_partitioned_data(pg_table, &files, &arrow_schema, delta_version).await?
        };

        // Update the tracked version
        update_managed_table_version(pg_table, delta_version)?;

        Ok::<i64, String>(total_rows)
    });

    match result {
        Ok(rows) => {
            log!(
                "pg_delta: created table '{}' from '{}' with {} rows",
                pg_table,
                uri,
                rows
            );
            rows
        }
        Err(e) => {
            pgrx::error!("Failed to create table: {}", e);
        }
    }
}

/// Refresh a managed Delta table, syncing only changed partitions.
///
/// Uses advisory locks to ensure only one refresh runs at a time per table.
/// If another refresh is already running, returns 0 immediately without blocking.
pub fn refresh_managed_table(pg_table: &str, full_refresh: bool) -> i64 {
    // Generate a unique lock ID for this refresh
    let lock_id = table_to_lock_id(pg_table, "delta_refresh");

    // Try to acquire advisory lock (non-blocking)
    let acquired = Spi::get_one::<bool>(&format!("SELECT pg_try_advisory_lock({})", lock_id))
        .unwrap_or(Some(false))
        .unwrap_or(false);

    if !acquired {
        log!(
            "pg_delta: refresh '{}' already running (lock_id={}), skipping",
            pg_table,
            lock_id
        );
        return 0;
    }

    // Ensure lock is released even on error
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        refresh_managed_table_inner(pg_table, full_refresh)
    }));

    // Always release the lock
    let _ = Spi::run(&format!("SELECT pg_advisory_unlock({})", lock_id));

    match result {
        Ok(rows) => rows,
        Err(e) => {
            // Re-panic after releasing lock
            std::panic::resume_unwind(e);
        }
    }
}

/// Inner implementation of refresh_managed_table (without locking).
fn refresh_managed_table_inner(pg_table: &str, full_refresh: bool) -> i64 {
    // Get table info from delta.tables
    let table_info = get_managed_table_info(pg_table);
    let (table_id, uri, partition_columns, current_version, _storage_options_json) =
        match table_info {
            Ok(info) => info,
            Err(e) => {
                pgrx::error!("Table '{}' not found: {}", pg_table, e);
            }
        };

    // Register storage handlers
    register_storage_handlers(&uri);

    let storage_options = build_storage_options(&uri);

    // Create tokio runtime for async operations
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        // Open the Delta table
        let table = deltalake::open_table_with_storage_options(&uri, storage_options)
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        let latest_version = table.version();

        // Check if there are changes
        if !full_refresh && latest_version <= current_version {
            log!(
                "pg_delta: table '{}' is already at version {}, no changes",
                pg_table,
                current_version
            );
            return Ok(0);
        }

        // Get schema
        let snapshot = table
            .snapshot()
            .map_err(|e| format!("Failed to get snapshot: {}", e))?;
        let delta_schema = snapshot.schema();
        let arrow_schema = delta_schema_to_arrow(delta_schema);

        // Get all files
        let files: Vec<_> = table
            .get_file_uris()
            .map_err(|e| format!("Failed to get file URIs: {}", e))?
            .collect();

        let total_rows = if full_refresh {
            // Full refresh - truncate and reload
            Spi::run(&format!("TRUNCATE TABLE {}", pg_table))
                .map_err(|e| format!("Failed to truncate table: {}", e))?;

            // Clear partition progress
            clear_partition_progress(table_id)?;

            // Reload all data
            if !partition_columns.is_empty() {
                load_partitioned_data(
                    pg_table,
                    &files,
                    &arrow_schema,
                    &partition_columns,
                    latest_version,
                )
                .await?
            } else {
                load_non_partitioned_data(pg_table, &files, &arrow_schema, latest_version).await?
            }
        } else {
            // Incremental refresh - only sync changed partitions
            if !partition_columns.is_empty() {
                refresh_partitioned_data(
                    table_id,
                    pg_table,
                    &files,
                    &arrow_schema,
                    &partition_columns,
                    latest_version,
                )
                .await?
            } else {
                // Non-partitioned tables always reload all data
                Spi::run(&format!("TRUNCATE TABLE {}", pg_table))
                    .map_err(|e| format!("Failed to truncate table: {}", e))?;
                load_non_partitioned_data(pg_table, &files, &arrow_schema, latest_version).await?
            }
        };

        // Update the tracked version
        update_managed_table_version(pg_table, latest_version)?;

        Ok::<i64, String>(total_rows)
    });

    match result {
        Ok(rows) => {
            log!(
                "pg_delta: refreshed table '{}' with {} rows",
                pg_table,
                rows
            );
            rows
        }
        Err(e) => {
            pgrx::error!("Failed to refresh table: {}", e);
        }
    }
}

/// Drop a managed Delta table registration.
pub fn drop_managed_table(pg_table: &str, drop_pg_table: bool) -> bool {
    // Delete from delta.tables (cascades to delta.table_partitions)
    let deleted = Spi::get_one_with_args::<i64>(
        "DELETE FROM delta.tables WHERE pg_table = $1 RETURNING id",
        &[pg_table.into()],
    );

    match deleted {
        Ok(Some(_)) => {
            // Optionally drop the Postgres table
            if drop_pg_table {
                if let Err(e) = Spi::run(&format!("DROP TABLE IF EXISTS {}", pg_table)) {
                    pgrx::warning!("Failed to drop table '{}': {}", pg_table, e);
                }
            }
            log!("pg_delta: dropped managed table '{}'", pg_table);
            true
        }
        Ok(None) => {
            pgrx::warning!("Managed table '{}' not found", pg_table);
            false
        }
        Err(e) => {
            pgrx::error!("Failed to drop managed table '{}': {}", pg_table, e);
        }
    }
}

// =============================================================================
// Managed Table Helper Functions
// =============================================================================

/// Register a new managed table in delta.tables.
fn register_managed_table(
    uri: &str,
    pg_table: &str,
    partition_columns: &[String],
    storage_options: &Option<pgrx::JsonB>,
) -> Result<(), String> {
    let partition_cols_sql = if partition_columns.is_empty() {
        "NULL".to_string()
    } else {
        format!(
            "ARRAY[{}]",
            partition_columns
                .iter()
                .map(|c| format!("'{}'", c))
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    let storage_opts_sql = storage_options
        .as_ref()
        .map(|j| format!("'{}'::jsonb", j.0))
        .unwrap_or_else(|| "NULL".to_string());

    let query = format!(
        r#"
        INSERT INTO delta.tables (pg_table, delta_uri, partition_columns, storage_options)
        VALUES ($1, $2, {}, {})
        ON CONFLICT (pg_table) DO UPDATE SET
            delta_uri = $2,
            partition_columns = {},
            storage_options = {}
        "#,
        partition_cols_sql, storage_opts_sql, partition_cols_sql, storage_opts_sql
    );

    Spi::run_with_args(&query, &[pg_table.into(), uri.into()])
        .map_err(|e| format!("Failed to register managed table: {}", e))?;

    Ok(())
}

/// Get managed table info from delta.tables.
#[allow(clippy::type_complexity)]
fn get_managed_table_info(
    pg_table: &str,
) -> Result<(i32, String, Vec<String>, i64, Option<pgrx::JsonB>), String> {
    let mut found = false;
    let mut id: i32 = 0;
    let mut delta_uri = String::new();
    let mut partition_columns: Vec<String> = Vec::new();
    let mut delta_version: i64 = 0;
    let mut storage_options: Option<pgrx::JsonB> = None;

    Spi::connect(|client| {
        let mut result = client
            .select(
                r#"
                SELECT id, delta_uri, partition_columns, delta_version, storage_options
                FROM delta.tables
                WHERE pg_table = $1
                "#,
                None,
                &[pg_table.into()],
            )
            .map_err(|e| format!("Query failed: {:?}", e))?;

        if let Some(row) = result.next() {
            found = true;
            id = row
                .get::<i32>(1)
                .map_err(|e| format!("Failed to get id: {}", e))?
                .ok_or_else(|| "id is null".to_string())?;

            delta_uri = row
                .get::<String>(2)
                .map_err(|e| format!("Failed to get delta_uri: {}", e))?
                .ok_or_else(|| "delta_uri is null".to_string())?;

            partition_columns = row
                .get::<Vec<String>>(3)
                .map_err(|e| format!("Failed to get partition_columns: {}", e))?
                .unwrap_or_default();

            delta_version = row
                .get::<i64>(4)
                .map_err(|e| format!("Failed to get delta_version: {}", e))?
                .unwrap_or(0);

            storage_options = row
                .get::<pgrx::JsonB>(5)
                .map_err(|e| format!("Failed to get storage_options: {}", e))?;
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    if !found {
        return Err(format!("Table '{}' not found in delta.tables", pg_table));
    }

    Ok((
        id,
        delta_uri,
        partition_columns,
        delta_version,
        storage_options,
    ))
}

/// Update the delta_version for a managed table.
fn update_managed_table_version(pg_table: &str, version: i64) -> Result<(), String> {
    Spi::run_with_args(
        "UPDATE delta.tables SET delta_version = $2, last_refresh_at = now() WHERE pg_table = $1",
        &[pg_table.into(), version.into()],
    )
    .map_err(|e| format!("Failed to update version: {}", e))?;

    Ok(())
}

/// Clear partition progress for a table (for full refresh).
fn clear_partition_progress(table_id: i32) -> Result<(), String> {
    Spi::run_with_args(
        "DELETE FROM delta.table_partitions WHERE table_id = $1",
        &[table_id.into()],
    )
    .map_err(|e| format!("Failed to clear partition progress: {}", e))?;

    Ok(())
}

/// Load all data from a partitioned Delta table.
async fn load_partitioned_data(
    pg_table: &str,
    files: &[String],
    schema: &ArrowSchema,
    partition_columns: &[String],
    delta_version: i64,
) -> Result<i64, String> {
    // Get table_id
    let table_id = get_table_id(pg_table)?;

    // Group files by partition
    let mut partition_files: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for file_uri in files {
        let partition_path = extract_partition_path(file_uri, partition_columns);
        partition_files
            .entry(partition_path)
            .or_default()
            .push(file_uri.clone());
    }

    let mut total_rows: i64 = 0;

    for (partition_path, files) in partition_files {
        let partition_values = parse_partition_path(&partition_path);

        let mut partition_rows: i64 = 0;
        let mut processed_files: Vec<String> = Vec::new();

        for file_uri in &files {
            match read_parquet_file_to_postgres(file_uri, pg_table, schema).await {
                Ok(rows) => {
                    partition_rows += rows;
                    processed_files.push(file_uri.clone());
                }
                Err(e) => {
                    log!("pg_delta: error processing file '{}': {}", file_uri, e);
                }
            }
        }

        // Record partition progress
        save_partition_progress(
            table_id,
            &partition_path,
            &partition_values,
            delta_version,
            &processed_files,
            partition_rows,
        )?;

        total_rows += partition_rows;
    }

    Ok(total_rows)
}

/// Load all data from a non-partitioned Delta table.
async fn load_non_partitioned_data(
    pg_table: &str,
    files: &[String],
    schema: &ArrowSchema,
    _delta_version: i64,
) -> Result<i64, String> {
    let mut total_rows: i64 = 0;

    for file_uri in files {
        let rows = read_parquet_file_to_postgres(file_uri, pg_table, schema).await?;
        total_rows += rows;
    }

    Ok(total_rows)
}

/// Incrementally refresh a partitioned Delta table.
async fn refresh_partitioned_data(
    table_id: i32,
    pg_table: &str,
    files: &[String],
    schema: &ArrowSchema,
    partition_columns: &[String],
    delta_version: i64,
) -> Result<i64, String> {
    // Group files by partition
    let mut partition_files: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for file_uri in files {
        let partition_path = extract_partition_path(file_uri, partition_columns);
        partition_files
            .entry(partition_path)
            .or_default()
            .push(file_uri.clone());
    }

    // Load existing partition progress
    let existing_progress = load_table_partition_progress(table_id)?;

    let mut total_rows: i64 = 0;

    for (partition_path, files) in partition_files {
        let partition_values = parse_partition_path(&partition_path);

        // Get existing progress for this partition
        let progress = existing_progress.get(&partition_path);

        // Determine which files need processing
        let files_to_process: Vec<&String> = if let Some(p) = progress {
            files.iter().filter(|f| !p.contains(*f)).collect()
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
            match read_parquet_file_to_postgres(file_uri, pg_table, schema).await {
                Ok(rows) => {
                    partition_rows += rows;
                    processed_files.push(file_uri.clone());
                }
                Err(e) => {
                    log!("pg_delta: error processing file '{}': {}", file_uri, e);
                }
            }
        }

        // Update partition progress
        save_partition_progress(
            table_id,
            &partition_path,
            &partition_values,
            delta_version,
            &processed_files,
            partition_rows,
        )?;

        total_rows += partition_rows;
    }

    Ok(total_rows)
}

/// Get table_id from delta.tables.
fn get_table_id(pg_table: &str) -> Result<i32, String> {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM delta.tables WHERE pg_table = $1",
        &[pg_table.into()],
    )
    .map_err(|e| format!("Query failed: {}", e))?
    .ok_or_else(|| format!("Table '{}' not found in delta.tables", pg_table))
}

/// Save partition progress to delta.table_partitions.
fn save_partition_progress(
    table_id: i32,
    partition_path: &str,
    partition_values: &serde_json::Value,
    delta_version: i64,
    new_files: &[String],
    rows_synced: i64,
) -> Result<(), String> {
    let partition_values_str = partition_values.to_string();

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
        INSERT INTO delta.table_partitions
            (table_id, partition_path, partition_values, delta_version, files_processed, rows_synced, last_sync_at)
        VALUES
            ($1, $2, $3::jsonb, $4, {}, $5, now())
        ON CONFLICT (table_id, partition_path) DO UPDATE SET
            delta_version = $4,
            files_processed = delta.table_partitions.files_processed || {},
            rows_synced = delta.table_partitions.rows_synced + $5,
            last_sync_at = now()
        "#,
        files_array, files_array
    );

    Spi::run_with_args(
        &query,
        &[
            table_id.into(),
            partition_path.into(),
            partition_values_str.as_str().into(),
            delta_version.into(),
            rows_synced.into(),
        ],
    )
    .map_err(|e| format!("Failed to save partition progress: {}", e))?;

    Ok(())
}

/// Load partition progress for a managed table.
fn load_table_partition_progress(
    table_id: i32,
) -> Result<std::collections::HashMap<String, std::collections::HashSet<String>>, String> {
    let mut progress_map = std::collections::HashMap::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT partition_path, files_processed
                FROM delta.table_partitions
                WHERE table_id = $1
                "#,
                None,
                &[table_id.into()],
            )
            .map_err(|e| format!("Failed to load partition progress: {:?}", e))?;

        for row in result {
            let partition_path: String = row
                .get::<String>(1)
                .map_err(|e| format!("Failed to get partition_path: {}", e))?
                .ok_or("partition_path is null")?;

            let files_processed: Option<Vec<String>> = row
                .get::<Vec<String>>(2)
                .map_err(|e| format!("Failed to get files_processed: {}", e))?;

            let files_set: std::collections::HashSet<String> =
                files_processed.unwrap_or_default().into_iter().collect();

            progress_map.insert(partition_path, files_set);
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    Ok(progress_map)
}

// =============================================================================
// Partition Extraction Helpers
// =============================================================================

/// Extract partition path from a file URI.
/// e.g., "s3://bucket/table/year=2024/month=01/file.parquet" -> "year=2024/month=01"
fn extract_partition_path(file_uri: &str, partition_columns: &[String]) -> String {
    let parts: Vec<&str> = file_uri.split('/').collect();

    let partition_parts: Vec<&str> = parts
        .iter()
        .filter(|part| {
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
// Export Table Functions (Postgres → Delta)
// =============================================================================

/// Create an export table registration and perform initial full export.
pub fn create_export_table(
    pg_table: &str,
    uri: &str,
    partition_by: Option<Vec<String>>,
    tracking_column: Option<&str>,
    storage_options_override: Option<pgrx::JsonB>,
) -> i64 {
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

    // Verify source table exists
    let table_exists = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema || '.' || table_name = '{}')",
        pg_table
    )).unwrap_or(Some(false)).unwrap_or(false);

    if !table_exists {
        // Try without schema prefix
        let parts: Vec<&str> = pg_table.split('.').collect();
        let (schema, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            ("public", pg_table)
        };

        let exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}')",
            schema, table
        )).unwrap_or(Some(false)).unwrap_or(false);

        if !exists {
            pgrx::error!("Source table '{}' does not exist", pg_table);
        }
    }

    // Register in delta.export_tables
    register_export_table(
        pg_table,
        uri,
        partition_by.as_deref(),
        tracking_column,
        &storage_options_override,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to register export table: {}", e));

    // Perform initial full export
    let rows = do_export(
        pg_table,
        uri,
        partition_by.as_deref(),
        tracking_column,
        true,
    );

    log!(
        "pg_delta: created export '{}' -> '{}' with {} rows",
        pg_table,
        uri,
        rows
    );

    rows
}

/// Generate a stable lock ID from a table name for advisory locks.
/// Uses a simple hash to create a unique i64 lock ID per table.
fn table_to_lock_id(pg_table: &str, prefix: &str) -> i64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    prefix.hash(&mut hasher);
    pg_table.hash(&mut hasher);
    // Ensure positive value for clarity in logs
    (hasher.finish() as i64).abs()
}

/// Export changed rows from a managed Postgres table to Delta.
///
/// Uses advisory locks to ensure only one export runs at a time per table.
/// If another export is already running, returns 0 immediately without blocking.
pub fn export_managed_table(pg_table: &str, full_export: bool) -> i64 {
    // Generate a unique lock ID for this export
    let lock_id = table_to_lock_id(pg_table, "delta_export");

    // Try to acquire advisory lock (non-blocking)
    let acquired = Spi::get_one::<bool>(&format!("SELECT pg_try_advisory_lock({})", lock_id))
        .unwrap_or(Some(false))
        .unwrap_or(false);

    if !acquired {
        log!(
            "pg_delta: export '{}' already running (lock_id={}), skipping",
            pg_table,
            lock_id
        );
        return 0;
    }

    // Ensure lock is released even on error
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        export_managed_table_inner(pg_table, full_export)
    }));

    // Always release the lock
    let _ = Spi::run(&format!("SELECT pg_advisory_unlock({})", lock_id));

    match result {
        Ok(rows) => rows,
        Err(e) => {
            // Re-panic after releasing lock
            std::panic::resume_unwind(e);
        }
    }
}

/// Inner implementation of export_managed_table (without locking).
fn export_managed_table_inner(pg_table: &str, full_export: bool) -> i64 {
    // Get export info
    let export_info = get_export_table_info(pg_table);
    let (_export_id, uri, partition_columns, tracking_column, last_tracking_value, _storage_opts) =
        match export_info {
            Ok(info) => info,
            Err(e) => {
                pgrx::error!("Export '{}' not found: {}", pg_table, e);
            }
        };

    let partition_cols: Option<Vec<String>> = if partition_columns.is_empty() {
        None
    } else {
        Some(partition_columns)
    };

    let rows = if full_export {
        do_export(
            pg_table,
            &uri,
            partition_cols.as_deref(),
            tracking_column.as_deref(),
            true,
        )
    } else {
        do_incremental_export(
            pg_table,
            &uri,
            partition_cols.as_deref(),
            tracking_column.as_deref(),
            last_tracking_value.as_deref(),
        )
    };

    log!("pg_delta: exported {} rows from '{}'", rows, pg_table);
    rows
}

/// Drop an export table registration.
pub fn drop_export_table(pg_table: &str) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        "DELETE FROM delta.export_tables WHERE pg_table = $1 RETURNING id",
        &[pg_table.into()],
    );

    match deleted {
        Ok(Some(_)) => {
            log!("pg_delta: dropped export '{}'", pg_table);
            true
        }
        Ok(None) => {
            pgrx::warning!("Export '{}' not found", pg_table);
            false
        }
        Err(e) => {
            pgrx::error!("Failed to drop export '{}': {}", pg_table, e);
        }
    }
}

// =============================================================================
// Export Helper Functions
// =============================================================================

/// Register a new export table in delta.export_tables.
fn register_export_table(
    pg_table: &str,
    uri: &str,
    partition_columns: Option<&[String]>,
    tracking_column: Option<&str>,
    storage_options: &Option<pgrx::JsonB>,
) -> Result<(), String> {
    let partition_cols_sql = partition_columns
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

    let tracking_col_sql = tracking_column
        .map(|c| format!("'{}'", c))
        .unwrap_or_else(|| "NULL".to_string());

    let storage_opts_sql = storage_options
        .as_ref()
        .map(|j| format!("'{}'::jsonb", j.0))
        .unwrap_or_else(|| "NULL".to_string());

    let query = format!(
        r#"
        INSERT INTO delta.export_tables (pg_table, delta_uri, partition_columns, tracking_column, storage_options)
        VALUES ($1, $2, {}, {}, {})
        ON CONFLICT (pg_table) DO UPDATE SET
            delta_uri = $2,
            partition_columns = {},
            tracking_column = {},
            storage_options = {}
        "#,
        partition_cols_sql,
        tracking_col_sql,
        storage_opts_sql,
        partition_cols_sql,
        tracking_col_sql,
        storage_opts_sql
    );

    Spi::run_with_args(&query, &[pg_table.into(), uri.into()])
        .map_err(|e| format!("Failed to register export table: {}", e))?;

    Ok(())
}

/// Get export table info from delta.export_tables.
#[allow(clippy::type_complexity)]
fn get_export_table_info(
    pg_table: &str,
) -> Result<
    (
        i32,
        String,
        Vec<String>,
        Option<String>,
        Option<String>,
        Option<pgrx::JsonB>,
    ),
    String,
> {
    let mut found = false;
    let mut id: i32 = 0;
    let mut delta_uri = String::new();
    let mut partition_columns: Vec<String> = Vec::new();
    let mut tracking_column: Option<String> = None;
    let mut last_tracking_value: Option<String> = None;
    let mut storage_options: Option<pgrx::JsonB> = None;

    Spi::connect(|client| {
        let mut result = client
            .select(
                r#"
                SELECT id, delta_uri, partition_columns, tracking_column, last_tracking_value, storage_options
                FROM delta.export_tables
                WHERE pg_table = $1
                "#,
                None,
                &[pg_table.into()],
            )
            .map_err(|e| format!("Query failed: {:?}", e))?;

        if let Some(row) = result.next() {
            found = true;
            id = row
                .get::<i32>(1)
                .map_err(|e| format!("Failed to get id: {}", e))?
                .ok_or_else(|| "id is null".to_string())?;

            delta_uri = row
                .get::<String>(2)
                .map_err(|e| format!("Failed to get delta_uri: {}", e))?
                .ok_or_else(|| "delta_uri is null".to_string())?;

            partition_columns = row
                .get::<Vec<String>>(3)
                .map_err(|e| format!("Failed to get partition_columns: {}", e))?
                .unwrap_or_default();

            tracking_column = row
                .get::<String>(4)
                .map_err(|e| format!("Failed to get tracking_column: {}", e))?;

            last_tracking_value = row
                .get::<String>(5)
                .map_err(|e| format!("Failed to get last_tracking_value: {}", e))?;

            storage_options = row
                .get::<pgrx::JsonB>(6)
                .map_err(|e| format!("Failed to get storage_options: {}", e))?;
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    if !found {
        return Err(format!(
            "Export '{}' not found in delta.export_tables",
            pg_table
        ));
    }

    Ok((
        id,
        delta_uri,
        partition_columns,
        tracking_column,
        last_tracking_value,
        storage_options,
    ))
}

/// Perform a full export from Postgres to Delta.
fn do_export(
    pg_table: &str,
    uri: &str,
    partition_columns: Option<&[String]>,
    tracking_column: Option<&str>,
    _is_full: bool,
) -> i64 {
    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        // Build query to get data
        let query = format!("SELECT * FROM {}", pg_table);

        // Execute query and build Arrow batches
        let (batches, _schema, max_tracking_value) =
            query_to_arrow_batches(&query, tracking_column)?;

        if batches.is_empty() {
            return Ok(0);
        }

        let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        // Create or open the Delta table
        let ops = deltalake::DeltaOps::try_from_uri_with_storage_options(uri, storage_options)
            .await
            .map_err(|e| format!("Failed to create DeltaOps: {}", e))?;

        // Write with partition columns if specified
        let write_builder = ops
            .write(batches)
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite);

        let write_builder = if let Some(partition_cols) = partition_columns {
            write_builder.with_partition_columns(partition_cols.to_vec())
        } else {
            write_builder
        };

        let table = write_builder
            .await
            .map_err(|e| format!("Failed to write to Delta: {}", e))?;

        let new_version = table.version();

        // Update tracking info
        update_export_tracking(pg_table, new_version, max_tracking_value.as_deref())?;

        Ok::<i64, String>(total_rows)
    });

    match result {
        Ok(rows) => rows,
        Err(e) => {
            pgrx::error!("Export failed: {}", e);
        }
    }
}

/// Perform an incremental export - only changed rows.
fn do_incremental_export(
    pg_table: &str,
    uri: &str,
    partition_columns: Option<&[String]>,
    tracking_column: Option<&str>,
    last_tracking_value: Option<&str>,
) -> i64 {
    // If no tracking column, fall back to full export
    let tracking_col = match tracking_column {
        Some(col) => col,
        None => {
            log!(
                "pg_delta: no tracking column for '{}', doing full export",
                pg_table
            );
            return do_export(pg_table, uri, partition_columns, None, true);
        }
    };

    register_storage_handlers(uri);
    let storage_options = build_storage_options(uri);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let result = runtime.block_on(async {
        // Build query for changed rows only
        let query = if let Some(last_val) = last_tracking_value {
            format!(
                "SELECT * FROM {} WHERE {} > '{}'",
                pg_table, tracking_col, last_val
            )
        } else {
            format!("SELECT * FROM {}", pg_table)
        };

        // Execute query and build Arrow batches
        let (batches, _schema, max_tracking_value) =
            query_to_arrow_batches(&query, Some(tracking_col))?;

        if batches.is_empty() {
            log!("pg_delta: no changes to export for '{}'", pg_table);
            return Ok(0);
        }

        let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

        // Open existing Delta table
        let table = deltalake::open_table_with_storage_options(uri, storage_options.clone())
            .await
            .map_err(|e| format!("Failed to open Delta table: {}", e))?;

        // Append new data
        let write_builder = deltalake::DeltaOps(table)
            .write(batches)
            .with_save_mode(deltalake::protocol::SaveMode::Append);

        let write_builder = if let Some(partition_cols) = partition_columns {
            write_builder.with_partition_columns(partition_cols.to_vec())
        } else {
            write_builder
        };

        let table = write_builder
            .await
            .map_err(|e| format!("Failed to write to Delta: {}", e))?;

        let new_version = table.version();

        // Update tracking info
        update_export_tracking(pg_table, new_version, max_tracking_value.as_deref())?;

        Ok::<i64, String>(total_rows)
    });

    match result {
        Ok(rows) => rows,
        Err(e) => {
            pgrx::error!("Incremental export failed: {}", e);
        }
    }
}

/// Update export tracking information after successful export.
fn update_export_tracking(
    pg_table: &str,
    delta_version: i64,
    max_tracking_value: Option<&str>,
) -> Result<(), String> {
    let tracking_sql = max_tracking_value
        .map(|v| format!("'{}'", v))
        .unwrap_or_else(|| "last_tracking_value".to_string());

    let query = format!(
        r#"
        UPDATE delta.export_tables
        SET delta_version = $2,
            last_tracking_value = {},
            last_export_at = now()
        WHERE pg_table = $1
        "#,
        tracking_sql
    );

    Spi::run_with_args(&query, &[pg_table.into(), delta_version.into()])
        .map_err(|e| format!("Failed to update export tracking: {}", e))?;

    Ok(())
}

/// Execute a query and convert results to Arrow RecordBatches.
/// Returns (batches, schema, max_tracking_value).
fn query_to_arrow_batches(
    query: &str,
    tracking_column: Option<&str>,
) -> Result<(Vec<RecordBatch>, ArrowSchema, Option<String>), String> {
    use arrow_array::*;
    use std::sync::Arc;

    let mut columns_data: Vec<(String, ArrowDataType, bool, Vec<Option<String>>)> = Vec::new();
    let mut max_tracking_value: Option<String> = None;
    let mut row_count = 0;

    Spi::connect(|client| {
        let result = client
            .select(query, None, &[])
            .map_err(|e| format!("Query failed: {:?}", e))?;

        // Get column info from first row
        let mut initialized = false;

        for row in result {
            if !initialized {
                // Initialize columns from the row's column info
                // For now, we'll infer types from the first row's values
                let num_cols = row.columns();
                for col_idx in 1..=num_cols {
                    let col_name = format!("col_{}", col_idx);
                    // Default to Utf8, we'll cast everything to string for simplicity
                    columns_data.push((col_name, ArrowDataType::Utf8, true, Vec::new()));
                }
                initialized = true;
            }

            row_count += 1;

            // Extract values from each column
            for (col_idx, (_name, _dtype, _nullable, values)) in columns_data.iter_mut().enumerate()
            {
                // Try to get as string - this is a simplification
                let val: Option<String> = row.get::<String>(col_idx + 1).ok().flatten();

                // Track max value for tracking column
                if let Some(ref tc) = tracking_column {
                    if _name.ends_with(tc) || col_idx == 0 {
                        // Simplified: assume first col might be tracking
                        if let Some(ref v) = val {
                            if max_tracking_value.is_none()
                                || v.as_str() > max_tracking_value.as_deref().unwrap_or("")
                            {
                                max_tracking_value = Some(v.clone());
                            }
                        }
                    }
                }

                values.push(val);
            }
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    if columns_data.is_empty() || row_count == 0 {
        return Ok((Vec::new(), ArrowSchema::empty(), None));
    }

    // Build Arrow arrays
    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

    for (name, dtype, nullable, values) in columns_data {
        fields.push(arrow_schema::Field::new(&name, dtype, nullable));
        let array: StringArray = values.into_iter().collect();
        arrays.push(Arc::new(array));
    }

    let schema = ArrowSchema::new(fields);
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

    Ok((vec![batch], schema, max_tracking_value))
}
