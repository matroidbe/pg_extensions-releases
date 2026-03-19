//! SPI execution logic for processing bridge requests
//!
//! This module contains the code that runs in PostgreSQL context to execute
//! SPI queries. It must only be called from the background worker's main thread.

use crate::bridge::SpiRequest;
use crate::types::{ColumnType, SpiError, SpiParam, SpiResult, SpiRow, SpiValue};
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

/// Execute a single SPI request
///
/// This function must be called from the background worker's main thread
/// where SPI access is valid. It wraps the SPI call in a transaction context.
///
/// # Safety
///
/// This function uses PostgreSQL's SPI interface and must only be called from
/// the background worker's main thread. Calling from any other thread will
/// cause undefined behavior.
pub fn execute_spi_request(request: SpiRequest) {
    // Use BackgroundWorker::transaction to properly set up transaction context
    // This sets the statement start timestamp, starts a transaction, pushes a snapshot,
    // and commits when done.
    let result = BackgroundWorker::transaction(|| {
        execute_query(&request.query, &request.params, &request.column_types)
    });

    // Ignore send errors - the receiver may have been dropped
    let _ = request.response_tx.send(result);
}

/// Execute a query using SPI
///
/// **IMPORTANT**: This function MUST only be called from the background worker's
/// main thread. Calling from any other thread will cause a panic.
fn execute_query(
    query: &str,
    params: &[SpiParam],
    column_types: &[ColumnType],
) -> Result<SpiResult, SpiError> {
    // Determine if this is a mutating query (INSERT, UPDATE, DELETE, CREATE, DROP, ALTER)
    let is_mutating = {
        let upper = query.trim().to_uppercase();
        upper.starts_with("INSERT")
            || upper.starts_with("UPDATE")
            || upper.starts_with("DELETE")
            || upper.starts_with("CREATE")
            || upper.starts_with("DROP")
            || upper.starts_with("ALTER")
    };

    // Use connect_mut for all queries since we might need to run mutating queries
    Spi::connect_mut(|client| {
        // Build the query with parameters substituted
        let query_str = if params.is_empty() {
            query.to_string()
        } else {
            // IMPORTANT: Process parameters in REVERSE order (highest index first)
            // to avoid $24 matching the start of $240, $241, etc.
            let mut query_str = query.to_string();
            for (i, param) in params.iter().enumerate().rev() {
                let placeholder = format!("${}", i + 1);
                let value_str = param_to_sql(param);
                query_str = query_str.replace(&placeholder, &value_str);
            }
            query_str
        };

        // Use update() for mutating queries, select() for read-only
        let table = if is_mutating {
            client
                .update(&query_str, None, &[])
                .map_err(|e| SpiError::QueryFailed(e.to_string()))?
        } else {
            client
                .select(&query_str, None, &[])
                .map_err(|e| SpiError::QueryFailed(e.to_string()))?
        };

        let mut rows = Vec::new();

        for row in table {
            let mut columns = Vec::new();

            // Read each column according to its expected type
            for (i, col_type) in column_types.iter().enumerate() {
                let ordinal = i + 1; // SPI uses 1-based indexing
                let value = read_column(&row, ordinal, *col_type);
                columns.push(value);
            }

            rows.push(SpiRow { columns });
        }

        Ok(SpiResult { rows })
    })
}

/// Convert an SpiParam to its SQL string representation
fn param_to_sql(param: &SpiParam) -> String {
    match param {
        SpiParam::Text(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        SpiParam::Text(None) => "NULL".to_string(),
        SpiParam::Int4(Some(i)) => i.to_string(),
        SpiParam::Int4(None) => "NULL".to_string(),
        SpiParam::Int8(Some(i)) => i.to_string(),
        SpiParam::Int8(None) => "NULL".to_string(),
        SpiParam::Float8(Some(f)) => f.to_string(),
        SpiParam::Float8(None) => "NULL".to_string(),
        SpiParam::Bool(Some(b)) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SpiParam::Bool(None) => "NULL".to_string(),
        SpiParam::Bytea(Some(b)) => format!("'\\x{}'::bytea", hex::encode(b)),
        SpiParam::Bytea(None) => "NULL".to_string(),
        SpiParam::Json(Some(j)) => format!("'{}'::jsonb", j.to_string().replace('\'', "''")),
        SpiParam::Json(None) => "NULL".to_string(),
    }
}

/// Read a single column value from an SPI row
fn read_column(
    row: &pgrx::spi::SpiHeapTupleData,
    ordinal: usize,
    col_type: ColumnType,
) -> SpiValue {
    match col_type {
        ColumnType::Int32 => match row.get::<i32>(ordinal) {
            Ok(Some(v)) => SpiValue::Int32(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Int64 => match row.get::<i64>(ordinal) {
            Ok(Some(v)) => SpiValue::Int64(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Float64 => match row.get::<f64>(ordinal) {
            Ok(Some(v)) => SpiValue::Float64(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Text => match row.get::<String>(ordinal) {
            Ok(Some(v)) => SpiValue::Text(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Bytea => match row.get::<Vec<u8>>(ordinal) {
            Ok(Some(v)) => SpiValue::Bytes(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Bool => match row.get::<bool>(ordinal) {
            Ok(Some(v)) => SpiValue::Bool(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Json => match row.get::<pgrx::JsonB>(ordinal) {
            Ok(Some(v)) => SpiValue::Json(v.0),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_to_sql_text() {
        assert_eq!(
            param_to_sql(&SpiParam::Text(Some("hello".to_string()))),
            "'hello'"
        );
        assert_eq!(param_to_sql(&SpiParam::Text(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_text_escaping() {
        assert_eq!(
            param_to_sql(&SpiParam::Text(Some("it's".to_string()))),
            "'it''s'"
        );
    }

    #[test]
    fn test_param_to_sql_int4() {
        assert_eq!(param_to_sql(&SpiParam::Int4(Some(42))), "42");
        assert_eq!(param_to_sql(&SpiParam::Int4(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_int8() {
        assert_eq!(
            param_to_sql(&SpiParam::Int8(Some(12345678901234))),
            "12345678901234"
        );
        assert_eq!(param_to_sql(&SpiParam::Int8(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_float8() {
        assert_eq!(param_to_sql(&SpiParam::Float8(Some(3.14))), "3.14");
        assert_eq!(param_to_sql(&SpiParam::Float8(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_bool() {
        assert_eq!(param_to_sql(&SpiParam::Bool(Some(true))), "TRUE");
        assert_eq!(param_to_sql(&SpiParam::Bool(Some(false))), "FALSE");
        assert_eq!(param_to_sql(&SpiParam::Bool(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_bytea() {
        assert_eq!(
            param_to_sql(&SpiParam::Bytea(Some(vec![0xde, 0xad, 0xbe, 0xef]))),
            "'\\xdeadbeef'::bytea"
        );
        assert_eq!(param_to_sql(&SpiParam::Bytea(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_json() {
        let json = serde_json::json!({"key": "value"});
        assert_eq!(
            param_to_sql(&SpiParam::Json(Some(json))),
            "'{\"key\":\"value\"}'::jsonb"
        );
        assert_eq!(param_to_sql(&SpiParam::Json(None)), "NULL");
    }

    #[test]
    fn test_param_to_sql_json_escaping() {
        let json = serde_json::json!({"key": "it's a value"});
        // JSON serialization escapes the apostrophe differently, then we escape for SQL
        let result = param_to_sql(&SpiParam::Json(Some(json)));
        assert!(result.starts_with("'"));
        assert!(result.ends_with("'::jsonb"));
    }
}
