//! Parser layer — bytes → `Vec<serde_json::Value>` for ELT pipelines.
//!
//! Parsers are configured by name in the DSL via `"parse_as"` and an
//! optional `"parser_config"` blob. A parser is constructed via
//! [`parser_from_config`] and then applied to each fetched payload.
//!
//! See `design/pg_streaming/connectors.md` for the spec.

use crate::connector::sdk::{Codec, ParseContext, Parser};
use bytes::Bytes;
use serde_json::Value;

pub mod bytes_pass;
pub mod codec;
pub mod csv_parser;
pub mod json_parser;
pub mod ndjson_parser;

pub use bytes_pass::BytesParser;
pub use csv_parser::CsvParser;
pub use json_parser::JsonParser;
pub use ndjson_parser::NdjsonParser;

/// Build a `Parser` by name. `config` is the optional `parser_config`
/// JSON blob from the DSL (may be `Value::Null` if absent).
pub fn parser_from_config(name: &str, config: &Value) -> Result<Box<dyn Parser>, String> {
    match name {
        "csv" => Ok(Box::new(CsvParser::from_config(config)?)),
        "json" => Ok(Box::new(JsonParser::new())),
        "ndjson" => Ok(Box::new(NdjsonParser::new())),
        "bytes" => Ok(Box::new(BytesParser::new())),
        other => Err(format!(
            "Unknown parser '{}'. Supported: csv, json, ndjson, bytes",
            other
        )),
    }
}

/// Decode `bytes` using the given codec, then parse with `parser`.
/// Returns the parsed records.
pub fn decode_and_parse(
    bytes: Bytes,
    codec: Codec,
    parser: &dyn Parser,
    context: &ParseContext,
) -> Result<Vec<Value>, String> {
    let decoded = codec::decode(bytes, codec)?;
    parser.parse(decoded, context)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_from_config_recognizes_all_supported() {
        assert!(parser_from_config("csv", &Value::Null).is_ok());
        assert!(parser_from_config("json", &Value::Null).is_ok());
        assert!(parser_from_config("ndjson", &Value::Null).is_ok());
        assert!(parser_from_config("bytes", &Value::Null).is_ok());
    }

    #[test]
    fn parser_from_config_rejects_unknown() {
        match parser_from_config("xml", &Value::Null) {
            Err(e) => {
                assert!(e.contains("Unknown parser"));
                assert!(e.contains("xml"));
            }
            Ok(_) => panic!("expected error for unknown parser 'xml'"),
        }
    }

    #[test]
    fn decode_and_parse_chains_codec_then_parser() {
        let parser = JsonParser::new();
        let ctx = ParseContext::default();
        let bytes = Bytes::from_static(br#"{"a": 1}"#);
        let result = decode_and_parse(bytes, Codec::None, &parser, &ctx).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["a"], 1);
    }
}
