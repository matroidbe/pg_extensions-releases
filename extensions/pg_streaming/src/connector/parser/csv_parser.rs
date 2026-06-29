//! CSV parser using the `csv` crate.
//!
//! Configuration (from `parser_config` JSON):
//!
//! | Field | Type | Default | Description |
//! |---|---|---|---|
//! | `header` | bool | `true` | First row is column names |
//! | `delimiter` | string | `","` | Field separator (single char) |
//! | `quote` | string | `"\""` | Quote character (single char) |
//! | `null_values` | array<string> | `["", "NULL", "null"]` | Strings to interpret as JSON null |
//! | `columns` | array<string> | `null` | Column names if `header: false` |
//!
//! Rows are emitted as `{col1: val1, col2: val2, …}`. If `header: false`
//! and `columns` is omitted, columns are named `_c0, _c1, …`.

use crate::connector::sdk::{ParseContext, Parser};
use bytes::Bytes;
#[cfg(test)]
use serde_json::json;
use serde_json::Value;

#[derive(Debug)]
pub struct CsvParser {
    header: bool,
    delimiter: u8,
    quote: u8,
    null_values: Vec<String>,
    columns_override: Option<Vec<String>>,
}

impl CsvParser {
    pub fn new() -> Self {
        Self {
            header: true,
            delimiter: b',',
            quote: b'"',
            null_values: vec!["".into(), "NULL".into(), "null".into()],
            columns_override: None,
        }
    }

    pub fn from_config(config: &Value) -> Result<Self, String> {
        let mut p = Self::new();
        if config.is_null() {
            return Ok(p);
        }
        let obj = config
            .as_object()
            .ok_or_else(|| "csv parser_config must be a JSON object".to_string())?;

        if let Some(h) = obj.get("header") {
            p.header = h
                .as_bool()
                .ok_or_else(|| "csv.header must be boolean".to_string())?;
        }
        if let Some(d) = obj.get("delimiter") {
            let s = d
                .as_str()
                .ok_or_else(|| "csv.delimiter must be a string".to_string())?;
            if s.len() != 1 {
                return Err(format!("csv.delimiter must be a single byte; got {:?}", s));
            }
            p.delimiter = s.as_bytes()[0];
        }
        if let Some(q) = obj.get("quote") {
            let s = q
                .as_str()
                .ok_or_else(|| "csv.quote must be a string".to_string())?;
            if s.len() != 1 {
                return Err(format!("csv.quote must be a single byte; got {:?}", s));
            }
            p.quote = s.as_bytes()[0];
        }
        if let Some(nulls) = obj.get("null_values") {
            let arr = nulls
                .as_array()
                .ok_or_else(|| "csv.null_values must be an array of strings".to_string())?;
            p.null_values = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .ok_or_else(|| "csv.null_values entries must be strings".to_string())
                })
                .collect::<Result<_, _>>()?;
        }
        if let Some(cols) = obj.get("columns") {
            let arr = cols
                .as_array()
                .ok_or_else(|| "csv.columns must be an array of strings".to_string())?;
            let names: Vec<String> = arr
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .ok_or_else(|| "csv.columns entries must be strings".to_string())
                })
                .collect::<Result<_, _>>()?;
            p.columns_override = Some(names);
        }
        Ok(p)
    }

    fn cell_to_value(&self, s: &str) -> Value {
        if self.null_values.iter().any(|n| n == s) {
            Value::Null
        } else {
            Value::String(s.to_string())
        }
    }
}

impl Default for CsvParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for CsvParser {
    fn parse(&self, bytes: Bytes, _context: &ParseContext) -> Result<Vec<Value>, String> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(self.header)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .flexible(true)
            .from_reader(bytes.as_ref());

        // Determine column names.
        let columns: Vec<String> = if self.header {
            let headers = reader
                .headers()
                .map_err(|e| format!("csv: failed to read header row: {}", e))?;
            headers.iter().map(String::from).collect()
        } else if let Some(ref cols) = self.columns_override {
            cols.clone()
        } else {
            // Lazily generated below per row width.
            Vec::new()
        };

        let mut records = Vec::new();
        for (row_index, result) in reader.records().enumerate() {
            let row = result.map_err(|e| format!("csv: row {} parse error: {}", row_index, e))?;
            let mut obj = serde_json::Map::new();
            for (i, cell) in row.iter().enumerate() {
                let col_name = if i < columns.len() {
                    columns[i].clone()
                } else {
                    format!("_c{}", i)
                };
                obj.insert(col_name, self.cell_to_value(cell));
            }
            records.push(Value::Object(obj));
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
    fn parses_with_header() {
        let p = CsvParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(
                Bytes::from_static(b"id,name,amount\n1,Alice,10.5\n2,Bob,99\n"),
                &ctx,
            )
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["name"], "Alice");
        assert_eq!(records[0]["amount"], "10.5");
        assert_eq!(records[1]["name"], "Bob");
    }

    #[test]
    fn parses_with_pipe_delimiter() {
        let cfg = json!({"delimiter": "|"});
        let p = CsvParser::from_config(&cfg).unwrap();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::from_static(b"a|b\n1|2\n"), &ctx).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["a"], "1");
        assert_eq!(records[0]["b"], "2");
    }

    #[test]
    fn parses_without_header_with_columns() {
        let cfg = json!({"header": false, "columns": ["a", "b", "c"]});
        let p = CsvParser::from_config(&cfg).unwrap();
        let ctx = ParseContext::default();
        let records = p
            .parse(Bytes::from_static(b"1,2,3\n4,5,6\n"), &ctx)
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["a"], "1");
        assert_eq!(records[1]["c"], "6");
    }

    #[test]
    fn parses_without_header_uses_default_names() {
        let cfg = json!({"header": false});
        let p = CsvParser::from_config(&cfg).unwrap();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::from_static(b"1,2,3\n"), &ctx).unwrap();
        assert_eq!(records[0]["_c0"], "1");
        assert_eq!(records[0]["_c1"], "2");
        assert_eq!(records[0]["_c2"], "3");
    }

    #[test]
    fn empty_strings_become_null_by_default() {
        let p = CsvParser::new();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::from_static(b"a,b\n1,\n,2\n"), &ctx).unwrap();
        assert_eq!(records[0]["a"], "1");
        assert_eq!(records[0]["b"], serde_json::Value::Null);
        assert_eq!(records[1]["a"], serde_json::Value::Null);
        assert_eq!(records[1]["b"], "2");
    }

    #[test]
    fn quoted_fields_with_commas() {
        let p = CsvParser::new();
        let ctx = ParseContext::default();
        let records = p
            .parse(
                Bytes::from_static(b"id,address\n1,\"123 Main St, Apt 4\"\n"),
                &ctx,
            )
            .unwrap();
        assert_eq!(records[0]["address"], "123 Main St, Apt 4");
    }

    #[test]
    fn empty_payload_zero_records() {
        let p = CsvParser::new();
        let ctx = ParseContext::default();
        let records = p.parse(Bytes::new(), &ctx).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn invalid_delimiter_errors() {
        let cfg = json!({"delimiter": ",,"});
        let err = CsvParser::from_config(&cfg).unwrap_err();
        assert!(err.contains("single byte"));
    }

    #[test]
    fn non_object_config_errors() {
        let cfg = json!("string config");
        let err = CsvParser::from_config(&cfg).unwrap_err();
        assert!(err.contains("must be a JSON object"));
    }

    #[test]
    fn custom_null_values() {
        let cfg = json!({"null_values": ["NA", "-"]});
        let p = CsvParser::from_config(&cfg).unwrap();
        let ctx = ParseContext::default();
        let records = p
            .parse(Bytes::from_static(b"a,b,c\nNA,-,5\n"), &ctx)
            .unwrap();
        assert_eq!(records[0]["a"], serde_json::Value::Null);
        assert_eq!(records[0]["b"], serde_json::Value::Null);
        assert_eq!(records[0]["c"], "5");
        // empty string is no longer treated as null
        let records2 = p.parse(Bytes::from_static(b"a,b\n1,\n"), &ctx).unwrap();
        assert_eq!(records2[0]["b"], "");
    }

    #[test]
    fn supports_streaming() {
        let p = CsvParser::new();
        assert!(p.supports_streaming());
    }
}
