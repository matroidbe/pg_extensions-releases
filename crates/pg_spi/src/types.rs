//! Core types for SPI bridge communication
//!
//! This module provides unified types that support all variants needed
//! across pg_kafka, pg_mqtt, pg_forms, and pg_mcp extensions.

use thiserror::Error;

/// Error type for SPI bridge operations
#[derive(Debug, Clone, Error)]
pub enum SpiError {
    /// Query execution failed
    #[error("query failed: {0}")]
    QueryFailed(String),

    /// Channel communication failed
    #[error("SPI channel closed")]
    ChannelClosed,

    /// No rows returned when expected
    #[error("no rows returned")]
    NoRows,

    /// Type conversion error
    #[error("type conversion error: {0}")]
    TypeConversion(String),
}

/// Query parameter with type information (superset of all extensions)
///
/// Supports all parameter types used across pg_kafka, pg_mqtt, pg_forms, and pg_mcp.
#[derive(Debug, Clone)]
pub enum SpiParam {
    /// Text/VARCHAR parameter
    Text(Option<String>),
    /// 32-bit integer parameter
    Int4(Option<i32>),
    /// 64-bit integer parameter
    Int8(Option<i64>),
    /// Double precision float parameter
    Float8(Option<f64>),
    /// Boolean parameter
    Bool(Option<bool>),
    /// Binary data parameter
    Bytea(Option<Vec<u8>>),
    /// JSONB parameter (stored as serde_json::Value)
    Json(Option<serde_json::Value>),
}

/// Owned value types that can be stored and sent across threads
///
/// Supports all value types returned by SPI queries across all extensions.
#[derive(Debug, Clone)]
pub enum SpiValue {
    /// NULL value
    Null,
    /// 32-bit integer
    Int32(i32),
    /// 64-bit integer
    Int64(i64),
    /// Double precision float
    Float64(f64),
    /// Text/VARCHAR
    Text(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Boolean
    Bool(bool),
    /// JSON/JSONB
    Json(serde_json::Value),
}

/// Column type hint for reading results
///
/// Determines how to read each column from the SPI result set.
#[derive(Debug, Clone, Copy)]
pub enum ColumnType {
    /// Read as 32-bit integer
    Int32,
    /// Read as 64-bit integer
    Int64,
    /// Read as double precision float
    Float64,
    /// Read as text/VARCHAR
    Text,
    /// Read as binary data
    Bytea,
    /// Read as boolean
    Bool,
    /// Read as JSON/JSONB
    Json,
}

/// A row from a query result - stores data as owned values
#[derive(Debug, Clone)]
pub struct SpiRow {
    /// Column values stored as owned types
    pub columns: Vec<SpiValue>,
}

impl SpiRow {
    /// Get a column value as i32
    pub fn get_i32(&self, index: usize) -> Result<i32, SpiError> {
        match self.columns.get(index) {
            Some(SpiValue::Int32(v)) => Ok(*v),
            Some(SpiValue::Int64(v)) => Ok(*v as i32),
            Some(SpiValue::Null) => Err(SpiError::NoRows),
            Some(other) => Err(SpiError::TypeConversion(format!(
                "expected i32, got {:?}",
                other
            ))),
            None => Err(SpiError::NoRows),
        }
    }

    /// Get a column value as i64
    pub fn get_i64(&self, index: usize) -> Result<i64, SpiError> {
        match self.columns.get(index) {
            Some(SpiValue::Int64(v)) => Ok(*v),
            Some(SpiValue::Int32(v)) => Ok(*v as i64),
            Some(SpiValue::Null) => Err(SpiError::NoRows),
            Some(other) => Err(SpiError::TypeConversion(format!(
                "expected i64, got {:?}",
                other
            ))),
            None => Err(SpiError::NoRows),
        }
    }

    /// Get a column value as optional String
    pub fn get_string(&self, index: usize) -> Option<String> {
        match self.columns.get(index) {
            Some(SpiValue::Text(s)) => Some(s.clone()),
            Some(SpiValue::Null) => None,
            _ => None,
        }
    }

    /// Get a column value as bytes
    pub fn get_bytes(&self, index: usize) -> Option<Vec<u8>> {
        match self.columns.get(index) {
            Some(SpiValue::Bytes(b)) => Some(b.clone()),
            Some(SpiValue::Null) => None,
            _ => None,
        }
    }

    /// Get a column value as bool
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        match self.columns.get(index) {
            Some(SpiValue::Bool(b)) => Some(*b),
            Some(SpiValue::Null) => None,
            _ => None,
        }
    }

    /// Get a column value as JSON
    pub fn get_json(&self, index: usize) -> Option<serde_json::Value> {
        match self.columns.get(index) {
            Some(SpiValue::Json(j)) => Some(j.clone()),
            Some(SpiValue::Null) => None,
            _ => None,
        }
    }
}

/// Query result from SPI
#[derive(Debug, Clone)]
pub struct SpiResult {
    /// Result rows
    pub rows: Vec<SpiRow>,
}

impl SpiResult {
    /// Create a new empty result
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Get the first row
    pub fn first(&self) -> Option<&SpiRow> {
        self.rows.first()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = &SpiRow> {
        self.rows.iter()
    }
}

impl Default for SpiResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spi_row_get_i64() {
        let row = SpiRow {
            columns: vec![SpiValue::Int64(12345678901234)],
        };
        assert_eq!(row.get_i64(0).unwrap(), 12345678901234i64);
    }

    #[test]
    fn test_spi_row_get_i64_from_i32() {
        let row = SpiRow {
            columns: vec![SpiValue::Int32(42)],
        };
        assert_eq!(row.get_i64(0).unwrap(), 42i64);
    }

    #[test]
    fn test_spi_row_get_i32() {
        let row = SpiRow {
            columns: vec![SpiValue::Int32(42)],
        };
        assert_eq!(row.get_i32(0).unwrap(), 42i32);
    }

    #[test]
    fn test_spi_row_get_string() {
        let row = SpiRow {
            columns: vec![SpiValue::Text("hello".to_string())],
        };
        assert_eq!(row.get_string(0), Some("hello".to_string()));
    }

    #[test]
    fn test_spi_row_get_string_null() {
        let row = SpiRow {
            columns: vec![SpiValue::Null],
        };
        assert_eq!(row.get_string(0), None);
    }

    #[test]
    fn test_spi_row_get_bytes() {
        let row = SpiRow {
            columns: vec![SpiValue::Bytes(vec![1, 2, 3])],
        };
        assert_eq!(row.get_bytes(0), Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_spi_row_get_bool() {
        let row = SpiRow {
            columns: vec![SpiValue::Bool(true)],
        };
        assert_eq!(row.get_bool(0), Some(true));
    }

    #[test]
    fn test_spi_row_get_json() {
        let row = SpiRow {
            columns: vec![SpiValue::Json(serde_json::json!({"key": "value"}))],
        };
        assert_eq!(row.get_json(0), Some(serde_json::json!({"key": "value"})));
    }

    #[test]
    fn test_spi_result_first() {
        let result = SpiResult {
            rows: vec![
                SpiRow {
                    columns: vec![SpiValue::Text("first".to_string())],
                },
                SpiRow {
                    columns: vec![SpiValue::Text("second".to_string())],
                },
            ],
        };
        assert_eq!(
            result.first().unwrap().get_string(0),
            Some("first".to_string())
        );
        assert_eq!(result.len(), 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_spi_result_empty() {
        let result = SpiResult { rows: vec![] };
        assert!(result.first().is_none());
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_spi_result_default() {
        let result = SpiResult::default();
        assert!(result.is_empty());
    }

    #[test]
    fn test_spi_error_display() {
        assert_eq!(
            SpiError::QueryFailed("test error".to_string()).to_string(),
            "query failed: test error"
        );
        assert_eq!(SpiError::ChannelClosed.to_string(), "SPI channel closed");
        assert_eq!(SpiError::NoRows.to_string(), "no rows returned");
        assert_eq!(
            SpiError::TypeConversion("bad type".to_string()).to_string(),
            "type conversion error: bad type"
        );
    }
}
