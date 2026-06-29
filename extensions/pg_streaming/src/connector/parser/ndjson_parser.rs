//! NDJSON (newline-delimited JSON) parser.
//!
//! Each non-empty line is parsed as one JSON value. Empty/blank lines are
//! skipped. Errors on any malformed line.

use crate::connector::sdk::{ParseContext, Parser};
use bytes::Bytes;
use serde_json::Value;

pub struct NdjsonParser;

impl NdjsonParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NdjsonParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for NdjsonParser {
    fn parse(&self, bytes: Bytes, _context: &ParseContext) -> Result<Vec<Value>, String> {
        let text =
            std::str::from_utf8(bytes.as_ref()).map_err(|e| format!("NDJSON not UTF-8: {}", e))?;
        let mut records = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(trimmed)
                .map_err(|e| format!("NDJSON parse failed on line {}: {}", i + 1, e))?;
            records.push(value);
        }
        Ok(records)
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiple_lines() {
        let p = NdjsonParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(
                Bytes::from_static(b"{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n"),
                &ctx,
            )
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["id"], 1);
        assert_eq!(records[2]["id"], 3);
    }

    #[test]
    fn skips_empty_lines() {
        let p = NdjsonParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(
                Bytes::from_static(b"{\"id\": 1}\n\n   \n{\"id\": 2}\n"),
                &ctx,
            )
            .unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn handles_missing_final_newline() {
        let p = NdjsonParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(Bytes::from_static(b"{\"id\": 1}\n{\"id\": 2}"), &ctx)
            .unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn empty_payload_zero_records() {
        let p = NdjsonParser::new();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::new(), &ctx).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn malformed_line_errors_with_line_number() {
        let p = NdjsonParser::new();
        let ctx = ParseContext::default();
        let err = p
            .parse(
                Bytes::from_static(b"{\"id\": 1}\nnot json\n{\"id\": 3}"),
                &ctx,
            )
            .unwrap_err();
        assert!(err.contains("line 2"));
    }

    #[test]
    fn supports_streaming_returns_true() {
        let p = NdjsonParser::new();
        assert!(p.supports_streaming());
    }
}
