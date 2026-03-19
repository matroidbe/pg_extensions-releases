//! SPI bridge re-exports from shared crate
//!
//! This module provides backward-compatible re-exports of the shared pg_spi crate.
//! The actual implementation is in `crates/pg_spi/`.

// Allow unused imports - these are public API for other modules that may use them
#[allow(unused_imports)]
pub use pg_spi::{
    create_bridge, execute_spi_request, ColumnType, SpiBridge, SpiError, SpiParam, SpiReceiver,
    SpiRequest, SpiResult, SpiRow, SpiValue,
};

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
    fn test_spi_row_get_string() {
        let row = SpiRow {
            columns: vec![SpiValue::Text("hello".to_string())],
        };
        assert_eq!(row.get_string(0), Some("hello".to_string()));
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
}
