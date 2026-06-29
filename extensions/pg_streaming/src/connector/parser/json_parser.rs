//! JSON parser — parses a complete payload as a single JSON value.
//!
//! If the value is an array, each element becomes one record. Otherwise
//! the payload becomes a single record.

use crate::connector::sdk::{ParseContext, Parser};
use bytes::Bytes;
use serde_json::Value;

pub struct JsonParser;

impl JsonParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for JsonParser {
    fn parse(&self, bytes: Bytes, _context: &ParseContext) -> Result<Vec<Value>, String> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_slice(bytes.as_ref())
            .map_err(|e| format!("JSON parse failed: {}", e))?;
        Ok(match value {
            Value::Array(arr) => arr,
            other => vec![other],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_object() {
        let p = JsonParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(Bytes::from_static(br#"{"a": 1, "b": "x"}"#), &ctx)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["a"], 1);
        assert_eq!(records[0]["b"], "x");
    }

    #[test]
    fn parses_array_into_multiple_records() {
        let p = JsonParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(
                Bytes::from_static(br#"[{"id": 1}, {"id": 2}, {"id": 3}]"#),
                &ctx,
            )
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["id"], 1);
        assert_eq!(records[2]["id"], 3);
    }

    #[test]
    fn empty_payload_yields_zero_records() {
        let p = JsonParser::new();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::new(), &ctx).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn invalid_json_errors() {
        let p = JsonParser::new();
        let ctx = ParseContext::default();
        let err = p.parse(Bytes::from_static(b"not json"), &ctx).unwrap_err();
        assert!(err.contains("JSON parse failed"));
    }

    #[test]
    fn primitive_payload_becomes_one_record() {
        let p = JsonParser::new();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::from_static(b"42"), &ctx).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], 42);
    }
}
