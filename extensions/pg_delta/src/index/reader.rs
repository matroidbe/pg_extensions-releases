//! Parquet reader for the index path.
//!
//! Given the files that survived tier-1 (catalog) pruning, fetch each one via
//! the Delta table's `object_store`, apply tier-2 (parquet row-group) pruning,
//! decode the surviving rows, merge in the file's partition values, apply the
//! exact row filter, project, and emit one `serde_json::Value` object per row.

use crate::index::catalog::FileRow;
use crate::index::filter;
use crate::storage::{merge_storage_options, register_storage_handlers};
use arrow_schema::{DataType, TimeUnit};
use object_store::path::Path as ObjPath;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::statistics::Statistics;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};

/// Inputs for a read.
pub struct ReadRequest<'a> {
    /// Delta table root URI (object_store is rooted here).
    pub location: &'a str,
    /// Per-table storage credential override.
    pub storage_options: Option<&'a Value>,
    /// Files that survived tier-1 pruning.
    pub files: &'a [FileRow],
    /// The exact filter (applied per-row and for tier-2 pruning).
    pub filter: &'a Value,
    /// Projected columns (`None` = all).
    pub columns: Option<&'a [String]>,
    /// Hard cap on rows returned (avoids unbounded materialization).
    pub limit: usize,
}

/// Read and filter the requested files into JSON rows.
pub fn read_files(req: ReadRequest) -> Result<Vec<Value>, String> {
    if req.files.is_empty() || req.limit == 0 {
        return Ok(Vec::new());
    }

    register_storage_handlers(req.location);
    let storage_options = merge_storage_options(req.location, req.storage_options);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {}", e))?;

    runtime.block_on(async {
        let table = deltalake::open_table_with_storage_options(req.location, storage_options)
            .await
            .map_err(|e| format!("failed to open Delta table: {}", e))?;
        let store = table.object_store();

        let mut rows: Vec<Value> = Vec::new();
        for file in req.files {
            if rows.len() >= req.limit {
                break;
            }
            let path = ObjPath::from(file.path.as_str());
            let bytes = store
                .get(&path)
                .await
                .map_err(|e| format!("failed to fetch '{}': {}", file.path, e))?
                .bytes()
                .await
                .map_err(|e| format!("failed to read bytes for '{}': {}", file.path, e))?;

            read_one_file(bytes, file, &req, &mut rows)?;
        }
        Ok(rows)
    })
}

/// Decode a single parquet file's surviving rows into `rows`.
fn read_one_file(
    bytes: bytes::Bytes,
    file: &FileRow,
    req: &ReadRequest,
    rows: &mut Vec<Value>,
) -> Result<(), String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| format!("parquet open '{}': {}", file.path, e))?;

    let arrow_schema = builder.schema().clone();

    // Tier-2: prune row groups by footer stats.
    let surviving_rgs: Vec<usize> = builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter_map(|(i, rg)| {
            let stats = rowgroup_col_stats(rg);
            if filter::row_group_survives(req.filter, &stats) {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    if surviving_rgs.is_empty() {
        return Ok(());
    }

    // Projection: read (requested ∪ filter) columns that exist in the parquet
    // schema. When no columns are requested, read everything.
    let mut builder = builder.with_row_groups(surviving_rgs);
    if let Some(requested) = req.columns {
        let mut needed: HashSet<&str> = requested.iter().map(|s| s.as_str()).collect();
        if let Value::Object(obj) = req.filter {
            for k in obj.keys() {
                needed.insert(k.as_str());
            }
        }
        let mut indices: Vec<usize> = Vec::new();
        for (i, f) in arrow_schema.fields().iter().enumerate() {
            if needed.contains(f.name().as_str()) {
                indices.push(i);
            }
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .build()
        .map_err(|e| format!("parquet build '{}': {}", file.path, e))?;

    // Per-file partition values become constant columns on every row.
    let base: Map<String, Value> = match &file.partition_values {
        Some(Value::Object(m)) => m.clone(),
        _ => Map::new(),
    };

    for batch in reader {
        let batch = batch.map_err(|e| format!("parquet decode '{}': {}", file.path, e))?;
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            if rows.len() >= req.limit {
                return Ok(());
            }
            let mut obj = base.clone();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                obj.insert(
                    field.name().clone(),
                    arrow_value_to_json(batch.column(col_idx), row_idx),
                );
            }
            let row_json = Value::Object(obj);
            if !filter::eval_row(req.filter, &row_json) {
                continue;
            }
            rows.push(project(row_json, req.columns));
        }
    }
    Ok(())
}

/// Restrict a row object to the requested columns (missing → null).
fn project(row: Value, columns: Option<&[String]>) -> Value {
    match columns {
        None => row,
        Some(cols) => {
            let mut m = Map::new();
            for c in cols {
                m.insert(c.clone(), row.get(c).cloned().unwrap_or(Value::Null));
            }
            Value::Object(m)
        }
    }
}

/// Build a `column name → (min, max)` map from a row group's column stats.
fn rowgroup_col_stats(
    rg: &parquet::file::metadata::RowGroupMetaData,
) -> HashMap<String, (Option<Value>, Option<Value>)> {
    let mut map = HashMap::new();
    for col in rg.columns() {
        if let Some(stats) = col.statistics() {
            let name = col.column_descr().name().to_string();
            map.insert(name, stat_min_max(stats));
        }
    }
    map
}

/// Extract `(min, max)` as JSON scalars from parquet statistics. Returns
/// `(None, None)` for types we don't compare on (Int96, fixed-len bytes, etc.).
fn stat_min_max(stats: &Statistics) -> (Option<Value>, Option<Value>) {
    match stats {
        Statistics::Boolean(v) => (
            v.min_opt().map(|b| Value::Bool(*b)),
            v.max_opt().map(|b| Value::Bool(*b)),
        ),
        Statistics::Int32(v) => (
            v.min_opt().map(|n| json!(*n)),
            v.max_opt().map(|n| json!(*n)),
        ),
        Statistics::Int64(v) => (
            v.min_opt().map(|n| json!(*n)),
            v.max_opt().map(|n| json!(*n)),
        ),
        Statistics::Float(v) => (
            v.min_opt().map(|n| json!(*n)),
            v.max_opt().map(|n| json!(*n)),
        ),
        Statistics::Double(v) => (
            v.min_opt().map(|n| json!(*n)),
            v.max_opt().map(|n| json!(*n)),
        ),
        Statistics::ByteArray(v) => (
            v.min_opt()
                .and_then(|b| b.as_utf8().ok().map(|s| Value::String(s.to_string()))),
            v.max_opt()
                .and_then(|b| b.as_utf8().ok().map(|s| Value::String(s.to_string()))),
        ),
        _ => (None, None),
    }
}

/// Convert one Arrow value to JSON. Timestamps/dates render as ISO strings so
/// they compare correctly against ISO-string filter values.
fn arrow_value_to_json(array: &arrow_array::ArrayRef, idx: usize) -> Value {
    use arrow_array::*;

    if array.is_null(idx) {
        return Value::Null;
    }

    macro_rules! num {
        ($t:ty) => {{
            let a = array.as_any().downcast_ref::<$t>().unwrap();
            json!(a.value(idx))
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(idx))
        }
        DataType::Int8 => num!(Int8Array),
        DataType::Int16 => num!(Int16Array),
        DataType::Int32 => num!(Int32Array),
        DataType::Int64 => num!(Int64Array),
        DataType::UInt8 => num!(UInt8Array),
        DataType::UInt16 => num!(UInt16Array),
        DataType::UInt32 => num!(UInt32Array),
        DataType::UInt64 => num!(UInt64Array),
        DataType::Float32 => num!(Float32Array),
        DataType::Float64 => num!(Float64Array),
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(a.value(idx).to_string())
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Value::String(format!("\\x{}", hex::encode(a.value(idx))))
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(a.value(idx) + 719_163)
                .unwrap_or_default();
            Value::String(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            timestamp_micros_to_json(a.value(idx))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            timestamp_micros_to_json(a.value(idx) * 1000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            timestamp_micros_to_json(a.value(idx) / 1000)
        }
        _ => Value::Null,
    }
}

fn timestamp_micros_to_json(micros: i64) -> Value {
    let secs = micros.div_euclid(1_000_000);
    let nsecs = (micros.rem_euclid(1_000_000) * 1000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        None => Value::Null,
    }
}
