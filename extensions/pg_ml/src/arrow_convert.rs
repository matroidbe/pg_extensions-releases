//! Arrow-based zero-copy data transfer between PostgreSQL and Python
//!
//! This module provides efficient data transfer using Apache Arrow's C Data Interface,
//! avoiding JSON serialization overhead for large datasets.

// Allow dead code since this module is used internally and via tests
#![allow(dead_code)]

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pgrx::prelude::*;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use std::sync::Arc;

use crate::datasets::{quote_identifier, quote_literal};
use crate::PgMlError;

// =============================================================================
// Type Mapping: PostgreSQL → Arrow
// =============================================================================

/// Check if a PostgreSQL type needs to be cast to another type for extraction
///
/// Some PostgreSQL types cannot be directly extracted as their target Rust type.
/// This function returns the SQL cast expression if needed.
///
/// Returns:
/// - `Some("DOUBLE PRECISION")` for NUMERIC/DECIMAL types
/// - `Some("TEXT")` for timestamp/date/time/interval types (pgrx can't extract to String directly)
/// - `None` for types that can be extracted directly
pub fn pg_type_needs_cast(pg_type: &str) -> Option<&'static str> {
    match pg_type.to_uppercase().as_str() {
        "NUMERIC" | "DECIMAL" => Some("DOUBLE PRECISION"),
        "TIMESTAMP WITHOUT TIME ZONE"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "DATE"
        | "TIME WITHOUT TIME ZONE"
        | "TIME WITH TIME ZONE"
        | "TIME"
        | "TIMETZ"
        | "INTERVAL" => Some("TEXT"),
        _ => None,
    }
}

/// Maps PostgreSQL type name to Arrow DataType
pub fn pg_type_to_arrow_type(pg_type: &str) -> DataType {
    let normalized = pg_type.to_uppercase();
    match normalized.as_str() {
        // Integer types
        "BIGINT" | "INT8" => DataType::Int64,
        "INTEGER" | "INT4" | "INT" => DataType::Int32,
        "SMALLINT" | "INT2" => DataType::Int16,

        // Float types
        "DOUBLE PRECISION" | "FLOAT8" => DataType::Float64,
        "REAL" | "FLOAT4" => DataType::Float32,
        "NUMERIC" | "DECIMAL" => DataType::Float64, // Lossy but practical

        // Boolean
        "BOOLEAN" | "BOOL" => DataType::Boolean,

        // Text types - all map to Utf8 (Arrow's string type)
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "NAME" => DataType::Utf8,

        // Default to Utf8 for unknown types
        _ => DataType::Utf8,
    }
}

// =============================================================================
// Column Builders
// =============================================================================

/// Enum to hold different Arrow array builders
enum ColumnBuilder {
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, 32)),
            _ => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, 32)),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColumnBuilder::Int16(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(mut b) => Arc::new(b.finish()),
            ColumnBuilder::Utf8(mut b) => Arc::new(b.finish()),
        }
    }
}

// =============================================================================
// SPI to Arrow Conversion
// =============================================================================

/// Column metadata from information_schema
#[derive(Debug)]
struct ColumnMeta {
    name: String,
    pg_type: String,
    arrow_type: DataType,
}

/// Get all column names for a table
///
/// Returns column names in ordinal position order.
pub fn get_column_names(schema_name: &str, table_name: &str) -> Result<Vec<String>, PgMlError> {
    let columns = get_column_metadata(schema_name, table_name)?;
    Ok(columns.into_iter().map(|c| c.name).collect())
}

/// Query column metadata from information_schema
fn get_column_metadata(schema_name: &str, table_name: &str) -> Result<Vec<ColumnMeta>, PgMlError> {
    let sql = format!(
        "SELECT column_name::TEXT, data_type::TEXT FROM information_schema.columns
         WHERE table_schema = {} AND table_name = {}
         ORDER BY ordinal_position",
        quote_literal(schema_name),
        quote_literal(table_name)
    );

    let mut columns = Vec::new();
    Spi::connect(|client| {
        let result = client.select(&sql, None, &[])?;
        for row in result {
            let name: String = row.get(1)?.unwrap_or_default();
            let pg_type: String = row.get(2)?.unwrap_or_default();
            let arrow_type = pg_type_to_arrow_type(&pg_type);
            columns.push(ColumnMeta {
                name,
                pg_type,
                arrow_type,
            });
        }
        Ok::<_, pgrx::spi::SpiError>(())
    })
    .map_err(|e| PgMlError::SpiError(format!("Failed to get column metadata: {}", e)))?;

    Ok(columns)
}

/// Get primary key column names for a table
///
/// Queries PostgreSQL system catalogs to find primary key columns,
/// which should be automatically excluded from ML features.
pub fn get_primary_key_columns(
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>, PgMlError> {
    let sql = format!(
        "SELECT a.attname::TEXT
         FROM pg_index i
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = (
             SELECT c.oid FROM pg_class c
             JOIN pg_namespace n ON c.relnamespace = n.oid
             WHERE n.nspname = {} AND c.relname = {}
         )
         AND i.indisprimary",
        quote_literal(schema_name),
        quote_literal(table_name)
    );

    let mut pk_columns = Vec::new();
    Spi::connect(|client| {
        let result = client.select(&sql, None, &[])?;
        for row in result {
            if let Some(name) = row.get::<String>(1)? {
                pk_columns.push(name);
            }
        }
        Ok::<_, pgrx::spi::SpiError>(())
    })
    .map_err(|e| PgMlError::SpiError(format!("Failed to get primary key columns: {}", e)))?;

    Ok(pk_columns)
}

/// Convert a PostgreSQL table to an Arrow RecordBatch
pub fn table_to_arrow(
    schema_name: &str,
    table_name: &str,
    columns: Option<&[&str]>,
    limit: Option<i64>,
) -> Result<RecordBatch, PgMlError> {
    // Get column metadata
    let all_columns = get_column_metadata(schema_name, table_name)?;

    // Filter columns if specified
    let selected_columns: Vec<&ColumnMeta> = match columns {
        Some(cols) => all_columns
            .iter()
            .filter(|c| cols.contains(&c.name.as_str()))
            .collect(),
        None => all_columns.iter().collect(),
    };

    if selected_columns.is_empty() {
        return Err(PgMlError::SpiError("No columns found".to_string()));
    }

    // Build Arrow schema
    let fields: Vec<Field> = selected_columns
        .iter()
        .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Build SELECT query with casts for types that need them (e.g., NUMERIC -> DOUBLE PRECISION)
    let col_list: String = selected_columns
        .iter()
        .map(|c| {
            let quoted_name = quote_identifier(&c.name);
            match pg_type_needs_cast(&c.pg_type) {
                Some(cast_to) => format!("{}::{} AS {}", quoted_name, cast_to, quoted_name),
                None => quoted_name,
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    let qualified_table = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );

    let limit_clause = match limit {
        Some(n) => format!(" LIMIT {}", n),
        None => String::new(),
    };

    let sql = format!(
        "SELECT {} FROM {}{}",
        col_list, qualified_table, limit_clause
    );

    // Query and build arrays
    let mut builders: Vec<ColumnBuilder> = selected_columns
        .iter()
        .map(|c| ColumnBuilder::new(&c.arrow_type, 1000))
        .collect();

    Spi::connect(|client| {
        let result = client.select(&sql, None, &[])?;

        for row in result {
            for (i, col_meta) in selected_columns.iter().enumerate() {
                match &mut builders[i] {
                    ColumnBuilder::Int16(b) => {
                        let val: Option<i16> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Int32(b) => {
                        let val: Option<i32> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Int64(b) => {
                        let val: Option<i64> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Float32(b) => {
                        let val: Option<f32> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Float64(b) => {
                        let val: Option<f64> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Boolean(b) => {
                        let val: Option<bool> = row.get(i + 1)?;
                        b.append_option(val);
                    }
                    ColumnBuilder::Utf8(b) => {
                        // Try to get as String, handle various types
                        let val: Option<String> = match col_meta.pg_type.to_uppercase().as_str() {
                            "BIGINT" | "INT8" => row.get::<i64>(i + 1)?.map(|v| v.to_string()),
                            "INTEGER" | "INT4" | "INT" => {
                                row.get::<i32>(i + 1)?.map(|v| v.to_string())
                            }
                            "DOUBLE PRECISION" | "FLOAT8" => {
                                row.get::<f64>(i + 1)?.map(|v| v.to_string())
                            }
                            "REAL" | "FLOAT4" => row.get::<f32>(i + 1)?.map(|v| v.to_string()),
                            _ => row.get::<String>(i + 1)?,
                        };
                        b.append_option(val);
                    }
                }
            }
        }
        Ok::<_, pgrx::spi::SpiError>(())
    })
    .map_err(|e| PgMlError::SpiError(format!("Failed to query table: {}", e)))?;

    // Finish building arrays
    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();

    // Create RecordBatch
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| PgMlError::SpiError(format!("Failed to create RecordBatch: {}", e)))
}

// =============================================================================
// Arrow to Python/Pandas Conversion
// =============================================================================

/// Convert Arrow RecordBatch to PyArrow Table and then to pandas DataFrame
/// Uses zero-copy transfer via Arrow C Data Interface
pub fn arrow_to_pandas<'py>(
    py: Python<'py>,
    batch: RecordBatch,
) -> Result<Bound<'py, PyAny>, PgMlError> {
    // Convert to pyo3-arrow wrapper
    let py_batch = PyRecordBatch::new(batch);

    // Export to PyArrow via C Data Interface (zero-copy)
    let pa_batch = py_batch
        .to_pyarrow(py)
        .map_err(|e| PgMlError::PythonError(format!("Failed to export to PyArrow: {}", e)))?;

    // Convert PyArrow RecordBatch to Table (needed for to_pandas)
    let pa = py
        .import("pyarrow")
        .map_err(|e| PgMlError::PythonError(format!("Failed to import pyarrow: {}", e)))?;

    // Create a list containing the batch, then call Table.from_batches
    let batches_list = pyo3::types::PyList::new(py, [&pa_batch])
        .map_err(|e| PgMlError::PythonError(format!("Failed to create list: {}", e)))?;

    let table_class = pa
        .getattr("Table")
        .map_err(|e| PgMlError::PythonError(format!("Failed to get Table class: {}", e)))?;

    let table = table_class
        .call_method1("from_batches", (batches_list,))
        .map_err(|e| PgMlError::PythonError(format!("Failed to create Table: {}", e)))?;

    // Convert to pandas with Arrow-backed dtypes for zero-copy
    let pd = py
        .import("pandas")
        .map_err(|e| PgMlError::PythonError(format!("Failed to import pandas: {}", e)))?;

    let arrow_dtype = pd
        .getattr("ArrowDtype")
        .map_err(|e| PgMlError::PythonError(format!("Failed to get ArrowDtype: {}", e)))?;

    let kwargs = pyo3::types::PyDict::new(py);
    kwargs
        .set_item("types_mapper", arrow_dtype)
        .map_err(|e| PgMlError::PythonError(format!("Failed to set types_mapper: {}", e)))?;

    table
        .call_method("to_pandas", (), Some(&kwargs))
        .map_err(|e| PgMlError::PythonError(format!("Failed to convert to pandas: {}", e)))
}

/// Full pipeline: PostgreSQL table → pandas DataFrame (zero-copy via Arrow)
pub fn table_to_dataframe_arrow<'py>(
    py: Python<'py>,
    schema_name: &str,
    table_name: &str,
    columns: Option<&[&str]>,
    limit: Option<i64>,
) -> Result<Bound<'py, PyAny>, PgMlError> {
    let batch = table_to_arrow(schema_name, table_name, columns, limit)?;
    arrow_to_pandas(py, batch)
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_to_arrow_type_integers() {
        assert!(matches!(pg_type_to_arrow_type("BIGINT"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow_type("bigint"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow_type("INT8"), DataType::Int64));
        assert!(matches!(pg_type_to_arrow_type("INTEGER"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow_type("INT4"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow_type("INT"), DataType::Int32));
        assert!(matches!(pg_type_to_arrow_type("SMALLINT"), DataType::Int16));
        assert!(matches!(pg_type_to_arrow_type("INT2"), DataType::Int16));
    }

    #[test]
    fn test_pg_type_to_arrow_type_floats() {
        assert!(matches!(
            pg_type_to_arrow_type("DOUBLE PRECISION"),
            DataType::Float64
        ));
        assert!(matches!(pg_type_to_arrow_type("FLOAT8"), DataType::Float64));
        assert!(matches!(pg_type_to_arrow_type("REAL"), DataType::Float32));
        assert!(matches!(pg_type_to_arrow_type("FLOAT4"), DataType::Float32));
        assert!(matches!(
            pg_type_to_arrow_type("NUMERIC"),
            DataType::Float64
        ));
    }

    #[test]
    fn test_pg_type_to_arrow_type_boolean() {
        assert!(matches!(
            pg_type_to_arrow_type("BOOLEAN"),
            DataType::Boolean
        ));
        assert!(matches!(pg_type_to_arrow_type("bool"), DataType::Boolean));
    }

    #[test]
    fn test_pg_type_to_arrow_type_text() {
        assert!(matches!(pg_type_to_arrow_type("TEXT"), DataType::Utf8));
        assert!(matches!(pg_type_to_arrow_type("VARCHAR"), DataType::Utf8));
        assert!(matches!(
            pg_type_to_arrow_type("CHARACTER VARYING"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_pg_type_to_arrow_type_unknown() {
        // Unknown types should default to Utf8
        assert!(matches!(
            pg_type_to_arrow_type("UNKNOWN_TYPE"),
            DataType::Utf8
        ));
    }

    #[test]
    fn test_pg_type_needs_cast_numeric() {
        // NUMERIC and DECIMAL need to be cast to DOUBLE PRECISION
        assert_eq!(pg_type_needs_cast("NUMERIC"), Some("DOUBLE PRECISION"));
        assert_eq!(pg_type_needs_cast("numeric"), Some("DOUBLE PRECISION"));
        assert_eq!(pg_type_needs_cast("DECIMAL"), Some("DOUBLE PRECISION"));
        assert_eq!(pg_type_needs_cast("decimal"), Some("DOUBLE PRECISION"));
    }

    #[test]
    fn test_pg_type_needs_cast_no_cast() {
        // Types that don't need casting
        assert_eq!(pg_type_needs_cast("INTEGER"), None);
        assert_eq!(pg_type_needs_cast("BIGINT"), None);
        assert_eq!(pg_type_needs_cast("DOUBLE PRECISION"), None);
        assert_eq!(pg_type_needs_cast("REAL"), None);
        assert_eq!(pg_type_needs_cast("TEXT"), None);
        assert_eq!(pg_type_needs_cast("BOOLEAN"), None);
        assert_eq!(pg_type_needs_cast("VARCHAR"), None);
    }

    #[test]
    fn test_pg_type_needs_cast_timestamp() {
        // Timestamp/date/time types need TEXT cast for pgrx extraction
        assert_eq!(
            pg_type_needs_cast("timestamp without time zone"),
            Some("TEXT")
        );
        assert_eq!(pg_type_needs_cast("timestamp with time zone"), Some("TEXT"));
        assert_eq!(pg_type_needs_cast("TIMESTAMP"), Some("TEXT"));
        assert_eq!(pg_type_needs_cast("TIMESTAMPTZ"), Some("TEXT"));
        assert_eq!(pg_type_needs_cast("DATE"), Some("TEXT"));
        assert_eq!(pg_type_needs_cast("time without time zone"), Some("TEXT"));
        assert_eq!(pg_type_needs_cast("INTERVAL"), Some("TEXT"));
    }
}
