//! Dataset loading functionality for pg_ml
//!
//! Provides functions to:
//! - Load sample datasets from PyCaret into PostgreSQL tables
//! - Convert PostgreSQL tables to pandas DataFrames for ML training
//! - Bidirectional type mapping between pandas and PostgreSQL

use crate::PgMlError;
use pgrx::prelude::*;
use pyo3::prelude::*;
use serde::Deserialize;

// =============================================================================
// Data Structures
// =============================================================================

/// Column information extracted from pandas DataFrame
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: String,
}

/// Result of fetching a dataset from Python
#[derive(Debug, Deserialize)]
pub struct DatasetResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<serde_json::Value>,
    #[allow(dead_code)] // Used for logging, may be used in future
    pub row_count: i64,
}

/// Metadata about a loaded dataset
#[derive(Debug)]
pub struct DatasetMetadata {
    pub schema_name: String,
    pub table_name: String,
    pub row_count: i64,
    pub columns: Vec<String>,
}

/// Dataset index entry from PyCaret
#[derive(Debug, Deserialize)]
pub struct DatasetIndexEntry {
    #[serde(rename = "Dataset")]
    pub name: String,
    #[serde(rename = "Default Task")]
    pub task: Option<String>,
    #[serde(rename = "# Instances")]
    pub rows: Option<i64>,
    #[serde(rename = "# Attributes")]
    pub features: Option<i64>,
}

// =============================================================================
// Type Mapping Functions
// =============================================================================

/// Maps pandas dtype string to PostgreSQL type name
pub fn pandas_dtype_to_pg_type(dtype: &str) -> &'static str {
    match dtype {
        // Integer types
        "int64" | "Int64" => "BIGINT",
        "int32" | "Int32" => "INTEGER",
        "int16" | "int8" | "Int16" | "Int8" => "SMALLINT",
        "uint64" | "UInt64" => "BIGINT", // PostgreSQL has no unsigned
        "uint32" | "UInt32" => "BIGINT", // Upcast to avoid overflow
        "uint16" | "uint8" | "UInt16" | "UInt8" => "INTEGER",

        // Float types
        "float64" | "Float64" => "DOUBLE PRECISION",
        "float32" | "Float32" => "REAL",

        // Boolean
        "bool" | "boolean" => "BOOLEAN",

        // Date/Time - match prefix since timezone info can vary
        dt if dt.starts_with("datetime64") => "TIMESTAMPTZ",
        dt if dt.starts_with("timedelta") => "INTERVAL",

        // Default to TEXT for object, category, and unknown types
        _ => "TEXT",
    }
}

/// Maps PostgreSQL type name to pandas dtype string
/// Used internally by table_to_dataframe_impl for future train()/automl() functions
#[allow(dead_code)]
pub fn pg_type_to_pandas_dtype(pg_type: &str) -> &'static str {
    let normalized = pg_type.to_uppercase();
    match normalized.as_str() {
        // Integer types
        "BIGINT" | "INT8" => "int64",
        "INTEGER" | "INT4" | "INT" => "int32",
        "SMALLINT" | "INT2" => "int16",

        // Float types
        "DOUBLE PRECISION" | "FLOAT8" => "float64",
        "REAL" | "FLOAT4" => "float32",
        "NUMERIC" | "DECIMAL" => "float64",

        // Boolean
        "BOOLEAN" | "BOOL" => "bool",

        // Date/Time
        "TIMESTAMP"
        | "TIMESTAMPTZ"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITHOUT TIME ZONE" => "datetime64[ns]",
        "DATE" => "datetime64[ns]",
        "INTERVAL" => "timedelta64[ns]",

        // Text types - all map to object (Python str)
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" => "object",

        // Default to object for unknown types
        _ => "object",
    }
}

/// Sanitize identifier to prevent SQL injection
/// Removes any characters that aren't alphanumeric or underscore
pub fn sanitize_identifier(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Quote an identifier for safe use in SQL (double-quote style)
pub fn quote_identifier(name: &str) -> String {
    let sanitized = sanitize_identifier(name);
    format!("\"{}\"", sanitized.replace('"', "\"\""))
}

/// Quote a literal string value for safe use in SQL (single-quote style)
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

// =============================================================================
// Embedded Python Code
// =============================================================================

/// Python code for dataset operations (kept for documentation/reference)
#[allow(dead_code)]
pub const DATASET_PYTHON_CODE: &str = r#"
import json

def fetch_dataset(name, folder=None):
    """
    Fetch dataset from PyCaret and return as JSON.

    Args:
        name: Dataset name (e.g., 'iris', 'boston')
        folder: Optional folder for time series datasets

    Returns:
        JSON string with columns info, rows data, and row count
    """
    from pycaret.datasets import get_data

    try:
        if folder:
            df = get_data(name, folder=folder, verbose=False)
        else:
            df = get_data(name, verbose=False)
    except Exception as e:
        raise ValueError(f"Failed to fetch dataset '{name}': {e}")

    # Extract column info with dtypes
    columns = []
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        columns.append({
            'name': str(col),
            'dtype': dtype_str
        })

    # Convert DataFrame to JSON, handling NaN -> null
    rows_json = df.to_json(orient='records', date_format='iso')

    return json.dumps({
        'columns': columns,
        'rows': json.loads(rows_json),
        'row_count': len(df)
    })


def list_datasets():
    """
    Get list of available datasets.

    Returns:
        JSON string with dataset index
    """
    from pycaret.datasets import get_data

    try:
        index = get_data('index', verbose=False)
        return index.to_json(orient='records')
    except Exception as e:
        raise ValueError(f"Failed to fetch dataset index: {e}")


def table_to_dataframe(json_data, dtypes=None):
    """
    Convert JSON rows to pandas DataFrame with optional type hints.

    Args:
        json_data: JSON string with array of row objects
        dtypes: Optional dict mapping column names to pandas dtypes

    Returns:
        pandas DataFrame
    """
    import pandas as pd

    df = pd.read_json(json_data, orient='records')
    if dtypes:
        for col, dtype in dtypes.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except (ValueError, TypeError):
                    pass  # Keep original type if conversion fails
    return df
"#;

// =============================================================================
// Python Integration
// =============================================================================

/// Execute the dataset Python module code and get access to functions
fn get_python_module(py: Python<'_>) -> PyResult<Bound<'_, pyo3::types::PyModule>> {
    pyo3::types::PyModule::from_code(
        py,
        c"
import json

def fetch_dataset(name, folder=None):
    from pycaret.datasets import get_data
    try:
        if folder:
            df = get_data(name, folder=folder, verbose=False)
        else:
            df = get_data(name, verbose=False)
    except Exception as e:
        raise ValueError(f\"Failed to fetch dataset '{name}': {e}\")
    columns = []
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        columns.append({'name': str(col), 'dtype': dtype_str})
    rows_json = df.to_json(orient='records', date_format='iso')
    return json.dumps({'columns': columns, 'rows': json.loads(rows_json), 'row_count': len(df)})

def list_datasets():
    from pycaret.datasets import get_data
    try:
        index = get_data('index', verbose=False)
        return index.to_json(orient='records')
    except Exception as e:
        raise ValueError(f\"Failed to fetch dataset index: {e}\")

def table_to_dataframe(json_data, dtypes=None):
    import pandas as pd
    df = pd.read_json(json_data, orient='records')
    if dtypes:
        for col, dtype in dtypes.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except (ValueError, TypeError):
                    pass
    return df
",
        c"pgml_datasets",
        c"pgml_datasets",
    )
}

/// Fetch a dataset from PyCaret via Python
pub fn fetch_dataset_from_pycaret(name: &str) -> Result<DatasetResult, PgMlError> {
    // Ensure Python is initialized
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_python_module(py).map_err(PgMlError::from)?;
        let fetch_fn = module.getattr("fetch_dataset").map_err(PgMlError::from)?;
        let result: String = fetch_fn
            .call1((name,))
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let dataset: DatasetResult = serde_json::from_str(&result)
            .map_err(|e| PgMlError::JsonError(format!("Failed to parse dataset result: {}", e)))?;

        Ok(dataset)
    })
}

/// Fetch the dataset index from PyCaret
pub fn fetch_dataset_index() -> Result<Vec<DatasetIndexEntry>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_python_module(py).map_err(PgMlError::from)?;
        let list_fn = module.getattr("list_datasets").map_err(PgMlError::from)?;
        let result: String = list_fn
            .call0()
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let index: Vec<DatasetIndexEntry> = serde_json::from_str(&result)
            .map_err(|e| PgMlError::JsonError(format!("Failed to parse dataset index: {}", e)))?;

        Ok(index)
    })
}

/// Convert JSON data to a pandas DataFrame (returns PyObject for use with PyCaret)
/// Used internally by table_to_dataframe_impl for future train()/automl() functions
#[allow(dead_code)]
pub fn json_to_dataframe<'py>(
    py: Python<'py>,
    json_data: &str,
    dtypes: Option<&std::collections::HashMap<String, String>>,
) -> PyResult<Bound<'py, pyo3::PyAny>> {
    let module = get_python_module(py)?;
    let convert_fn = module.getattr("table_to_dataframe")?;

    let dtypes_py = match dtypes {
        Some(dt) => {
            let dict = pyo3::types::PyDict::new(py);
            for (k, v) in dt {
                dict.set_item(k, v)?;
            }
            dict.into_any()
        }
        None => py.None().into_bound(py),
    };

    convert_fn.call1((json_data, dtypes_py))
}

// =============================================================================
// SPI Operations
// =============================================================================

/// Ensure the samples schema exists
pub fn ensure_schema_exists(schema_name: &str) -> Result<(), PgMlError> {
    let quoted_schema = quote_identifier(schema_name);
    let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quoted_schema);

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to create schema: {}", e)))?;
    Ok(())
}

/// Check if a table exists in the given schema
pub fn table_exists(schema_name: &str, table_name: &str) -> Result<bool, PgMlError> {
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = {} AND table_name = {})",
        quote_literal(schema_name),
        quote_literal(table_name)
    );

    let exists = Spi::get_one::<bool>(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to check table existence: {}", e)))?;

    Ok(exists.unwrap_or(false))
}

/// Drop a table if it exists
pub fn drop_table(schema_name: &str, table_name: &str) -> Result<(), PgMlError> {
    let qualified_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );
    let sql = format!("DROP TABLE IF EXISTS {}", qualified_name);

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to drop table: {}", e)))?;
    Ok(())
}

/// Create a table with the given columns
/// Automatically adds an 'id' column as SERIAL PRIMARY KEY for reproducibility tracking
pub fn create_table(
    schema_name: &str,
    table_name: &str,
    columns: &[ColumnInfo],
) -> Result<(), PgMlError> {
    let qualified_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );

    // Start with id column as primary key for reproducibility tracking
    let mut column_defs: Vec<String> = vec!["id SERIAL PRIMARY KEY".to_string()];

    // Add the dataset columns
    column_defs.extend(columns.iter().map(|col| {
        let pg_type = pandas_dtype_to_pg_type(&col.dtype);
        format!("{} {}", quote_identifier(&col.name), pg_type)
    }));

    let sql = format!(
        "CREATE TABLE {} ({})",
        qualified_name,
        column_defs.join(", ")
    );

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to create table: {}", e)))?;
    Ok(())
}

/// Insert data into a table from JSON rows
pub fn insert_data(
    schema_name: &str,
    table_name: &str,
    columns: &[ColumnInfo],
    rows: &[serde_json::Value],
) -> Result<i64, PgMlError> {
    if rows.is_empty() {
        return Ok(0);
    }

    let qualified_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );

    let column_names: Vec<String> = columns.iter().map(|c| quote_identifier(&c.name)).collect();
    let column_list = column_names.join(", ");

    // Insert in batches for efficiency
    const BATCH_SIZE: usize = 100;
    let mut total_inserted = 0i64;

    for batch in rows.chunks(BATCH_SIZE) {
        let values_clauses: Vec<String> = batch
            .iter()
            .map(|row| {
                let values: Vec<String> = columns
                    .iter()
                    .map(|col| match row.get(&col.name) {
                        Some(serde_json::Value::Null) | None => "NULL".to_string(),
                        Some(serde_json::Value::Number(n)) => n.to_string(),
                        Some(serde_json::Value::Bool(b)) => b.to_string(),
                        Some(serde_json::Value::String(s)) => quote_literal(s),
                        Some(v) => quote_literal(&v.to_string()),
                    })
                    .collect();
                format!("({})", values.join(", "))
            })
            .collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            qualified_name,
            column_list,
            values_clauses.join(", ")
        );

        Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to insert data: {}", e)))?;
        total_inserted += batch.len() as i64;
    }

    Ok(total_inserted)
}

/// Select all data from a table as JSON string (for DataFrame conversion)
pub fn select_table_as_json(
    schema_name: &str,
    table_name: &str,
    columns: Option<&[&str]>,
    limit: Option<i64>,
) -> Result<(String, Vec<(String, String)>), PgMlError> {
    let qualified_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );

    let select_cols = match columns {
        Some(cols) => cols
            .iter()
            .map(|c| quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", "),
        None => "*".to_string(),
    };

    let limit_clause = match limit {
        Some(n) => format!(" LIMIT {}", n),
        None => String::new(),
    };

    // First get column types from information_schema
    // Cast to TEXT because information_schema uses special types (sql_identifier, character_data)
    let type_sql = format!(
        "SELECT column_name::TEXT, data_type::TEXT FROM information_schema.columns
         WHERE table_schema = {} AND table_name = {}
         ORDER BY ordinal_position",
        quote_literal(schema_name),
        quote_literal(table_name)
    );

    let mut col_types: Vec<(String, String)> = Vec::new();
    Spi::connect(|client| {
        let result = client.select(&type_sql, None, &[])?;
        for row in result {
            let col_name: String = row.get(1)?.unwrap_or_default();
            let data_type: String = row.get(2)?.unwrap_or_default();
            col_types.push((col_name, data_type));
        }
        Ok::<_, pgrx::spi::SpiError>(())
    })
    .map_err(|e| PgMlError::SpiError(format!("Failed to get column types: {}", e)))?;

    // Use PostgreSQL's row_to_json and array aggregation
    let sql = format!(
        "SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text FROM (SELECT {} FROM {}{}) t",
        select_cols, qualified_name, limit_clause
    );

    let json_result: String = Spi::get_one(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to select as JSON: {}", e)))?
        .unwrap_or_else(|| "[]".to_string());

    Ok((json_result, col_types))
}

// =============================================================================
// Public API Implementation
// =============================================================================

/// Load a dataset from PyCaret into a PostgreSQL table
pub fn load_dataset_impl(
    name: &str,
    schema_name: &str,
    table_name: &str,
    if_exists: &str,
) -> Result<DatasetMetadata, PgMlError> {
    // Check if table already exists
    let exists = table_exists(schema_name, table_name)?;

    if exists {
        match if_exists {
            "error" => {
                return Err(PgMlError::TableExists(
                    schema_name.to_string(),
                    table_name.to_string(),
                ));
            }
            "skip" => {
                // Return metadata for existing table
                let (_, col_types) = select_table_as_json(schema_name, table_name, None, Some(0))?;
                let columns: Vec<String> = col_types.into_iter().map(|(name, _)| name).collect();

                // Get row count
                let qualified_name = format!(
                    "{}.{}",
                    quote_identifier(schema_name),
                    quote_identifier(table_name)
                );
                let count_sql = format!("SELECT COUNT(*) FROM {}", qualified_name);
                let row_count: i64 = Spi::get_one(&count_sql)
                    .map_err(|e| PgMlError::SpiError(format!("Failed to count rows: {}", e)))?
                    .unwrap_or(0);

                return Ok(DatasetMetadata {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    row_count,
                    columns,
                });
            }
            "replace" => {
                drop_table(schema_name, table_name)?;
            }
            _ => {
                return Err(PgMlError::InvalidParameter(format!(
                    "if_exists must be 'error', 'replace', or 'skip', got '{}'",
                    if_exists
                )));
            }
        }
    }

    // Fetch dataset from PyCaret
    pgrx::info!("pg_ml: Fetching dataset '{}' from PyCaret...", name);
    let dataset = fetch_dataset_from_pycaret(name)?;

    // Ensure schema exists
    ensure_schema_exists(schema_name)?;

    // Create table
    create_table(schema_name, table_name, &dataset.columns)?;

    // Insert data
    let row_count = insert_data(schema_name, table_name, &dataset.columns, &dataset.rows)?;
    pgrx::info!(
        "pg_ml: Loaded {} rows into {}.{}",
        row_count,
        schema_name,
        table_name
    );

    // Build columns list with 'id' first (matches table structure)
    let mut columns: Vec<String> = vec!["id".to_string()];
    columns.extend(dataset.columns.into_iter().map(|c| c.name));

    Ok(DatasetMetadata {
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        row_count,
        columns,
    })
}

/// Load a PostgreSQL table into a pandas DataFrame
/// This is an internal function used by future train() and automl() functions
#[allow(dead_code)]
pub fn table_to_dataframe_impl<'py>(
    py: Python<'py>,
    schema_name: &str,
    table_name: &str,
    columns: Option<&[&str]>,
    limit: Option<i64>,
) -> Result<Bound<'py, pyo3::PyAny>, PgMlError> {
    // Get table data as JSON
    let (json_data, col_types) = select_table_as_json(schema_name, table_name, columns, limit)?;

    // Build dtype hints from PostgreSQL types
    let dtypes: std::collections::HashMap<String, String> = col_types
        .into_iter()
        .map(|(name, pg_type)| {
            let pandas_dtype = pg_type_to_pandas_dtype(&pg_type);
            (name, pandas_dtype.to_string())
        })
        .collect();

    // Convert to DataFrame
    json_to_dataframe(py, &json_data, Some(&dtypes)).map_err(PgMlError::from)
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Type Mapping Tests (Pandas -> PostgreSQL)
    // -------------------------------------------------------------------------

    #[test]
    fn test_pandas_dtype_to_pg_type_integers() {
        assert_eq!(pandas_dtype_to_pg_type("int64"), "BIGINT");
        assert_eq!(pandas_dtype_to_pg_type("Int64"), "BIGINT");
        assert_eq!(pandas_dtype_to_pg_type("int32"), "INTEGER");
        assert_eq!(pandas_dtype_to_pg_type("Int32"), "INTEGER");
        assert_eq!(pandas_dtype_to_pg_type("int16"), "SMALLINT");
        assert_eq!(pandas_dtype_to_pg_type("int8"), "SMALLINT");
    }

    #[test]
    fn test_pandas_dtype_to_pg_type_unsigned() {
        // Unsigned integers need careful mapping to avoid overflow
        assert_eq!(pandas_dtype_to_pg_type("uint64"), "BIGINT");
        assert_eq!(pandas_dtype_to_pg_type("uint32"), "BIGINT"); // Upcasted
        assert_eq!(pandas_dtype_to_pg_type("uint16"), "INTEGER");
        assert_eq!(pandas_dtype_to_pg_type("uint8"), "INTEGER");
    }

    #[test]
    fn test_pandas_dtype_to_pg_type_floats() {
        assert_eq!(pandas_dtype_to_pg_type("float64"), "DOUBLE PRECISION");
        assert_eq!(pandas_dtype_to_pg_type("Float64"), "DOUBLE PRECISION");
        assert_eq!(pandas_dtype_to_pg_type("float32"), "REAL");
        assert_eq!(pandas_dtype_to_pg_type("Float32"), "REAL");
    }

    #[test]
    fn test_pandas_dtype_to_pg_type_boolean() {
        assert_eq!(pandas_dtype_to_pg_type("bool"), "BOOLEAN");
        assert_eq!(pandas_dtype_to_pg_type("boolean"), "BOOLEAN");
    }

    #[test]
    fn test_pandas_dtype_to_pg_type_datetime() {
        assert_eq!(pandas_dtype_to_pg_type("datetime64[ns]"), "TIMESTAMPTZ");
        assert_eq!(
            pandas_dtype_to_pg_type("datetime64[ns, UTC]"),
            "TIMESTAMPTZ"
        );
        assert_eq!(pandas_dtype_to_pg_type("timedelta64[ns]"), "INTERVAL");
    }

    #[test]
    fn test_pandas_dtype_to_pg_type_default() {
        assert_eq!(pandas_dtype_to_pg_type("object"), "TEXT");
        assert_eq!(pandas_dtype_to_pg_type("category"), "TEXT");
        assert_eq!(pandas_dtype_to_pg_type("unknown_type"), "TEXT");
    }

    // -------------------------------------------------------------------------
    // Type Mapping Tests (PostgreSQL -> Pandas)
    // -------------------------------------------------------------------------

    #[test]
    fn test_pg_type_to_pandas_dtype_integers() {
        assert_eq!(pg_type_to_pandas_dtype("BIGINT"), "int64");
        assert_eq!(pg_type_to_pandas_dtype("INT8"), "int64");
        assert_eq!(pg_type_to_pandas_dtype("INTEGER"), "int32");
        assert_eq!(pg_type_to_pandas_dtype("INT4"), "int32");
        assert_eq!(pg_type_to_pandas_dtype("INT"), "int32");
        assert_eq!(pg_type_to_pandas_dtype("SMALLINT"), "int16");
        assert_eq!(pg_type_to_pandas_dtype("INT2"), "int16");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_floats() {
        assert_eq!(pg_type_to_pandas_dtype("DOUBLE PRECISION"), "float64");
        assert_eq!(pg_type_to_pandas_dtype("FLOAT8"), "float64");
        assert_eq!(pg_type_to_pandas_dtype("REAL"), "float32");
        assert_eq!(pg_type_to_pandas_dtype("FLOAT4"), "float32");
        assert_eq!(pg_type_to_pandas_dtype("NUMERIC"), "float64");
        assert_eq!(pg_type_to_pandas_dtype("DECIMAL"), "float64");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_boolean() {
        assert_eq!(pg_type_to_pandas_dtype("BOOLEAN"), "bool");
        assert_eq!(pg_type_to_pandas_dtype("BOOL"), "bool");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_datetime() {
        assert_eq!(pg_type_to_pandas_dtype("TIMESTAMP"), "datetime64[ns]");
        assert_eq!(pg_type_to_pandas_dtype("TIMESTAMPTZ"), "datetime64[ns]");
        assert_eq!(pg_type_to_pandas_dtype("DATE"), "datetime64[ns]");
        assert_eq!(pg_type_to_pandas_dtype("INTERVAL"), "timedelta64[ns]");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_text() {
        assert_eq!(pg_type_to_pandas_dtype("TEXT"), "object");
        assert_eq!(pg_type_to_pandas_dtype("VARCHAR"), "object");
        assert_eq!(pg_type_to_pandas_dtype("CHAR"), "object");
        assert_eq!(pg_type_to_pandas_dtype("CHARACTER VARYING"), "object");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_case_insensitive() {
        // Should work regardless of case
        assert_eq!(pg_type_to_pandas_dtype("bigint"), "int64");
        assert_eq!(pg_type_to_pandas_dtype("BigInt"), "int64");
        assert_eq!(pg_type_to_pandas_dtype("text"), "object");
    }

    #[test]
    fn test_pg_type_to_pandas_dtype_unknown() {
        assert_eq!(pg_type_to_pandas_dtype("JSONB"), "object");
        assert_eq!(pg_type_to_pandas_dtype("UUID"), "object");
        assert_eq!(pg_type_to_pandas_dtype("unknown_type"), "object");
    }

    // -------------------------------------------------------------------------
    // Identifier Sanitization Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_sanitize_identifier_valid() {
        assert_eq!(sanitize_identifier("valid_name"), "valid_name");
        assert_eq!(sanitize_identifier("name123"), "name123");
        assert_eq!(sanitize_identifier("CamelCase"), "CamelCase");
        assert_eq!(sanitize_identifier("_underscore"), "_underscore");
    }

    #[test]
    fn test_sanitize_identifier_removes_special_chars() {
        assert_eq!(sanitize_identifier("bad;name"), "badname");
        assert_eq!(sanitize_identifier("drop--table"), "droptable");
        assert_eq!(sanitize_identifier("name with spaces"), "namewithspaces");
        assert_eq!(sanitize_identifier("name.with.dots"), "namewithdots");
    }

    #[test]
    fn test_sanitize_identifier_sql_injection() {
        let malicious = "users; DROP TABLE users;--";
        let sanitized = sanitize_identifier(malicious);
        assert!(!sanitized.contains(';'));
        assert!(!sanitized.contains('-'));
        assert!(!sanitized.contains(' '));
        assert_eq!(sanitized, "usersDROPTABLEusers");
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("with space"), "\"withspace\"");
        // Double quotes in identifiers are doubled
        assert_eq!(quote_identifier("with\"quote"), "\"withquote\"");
    }

    #[test]
    fn test_quote_literal() {
        assert_eq!(quote_literal("simple"), "'simple'");
        assert_eq!(quote_literal("with'quote"), "'with''quote'");
        assert_eq!(quote_literal(""), "''");
    }

    // -------------------------------------------------------------------------
    // Data Structure Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_column_info_deserialize() {
        let json = r#"{"name": "age", "dtype": "int64"}"#;
        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert_eq!(col.name, "age");
        assert_eq!(col.dtype, "int64");
    }

    #[test]
    fn test_dataset_result_deserialize() {
        let json = r#"{
            "columns": [{"name": "id", "dtype": "int64"}, {"name": "name", "dtype": "object"}],
            "rows": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            "row_count": 2
        }"#;
        let result: DatasetResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.row_count, 2);
    }
}
