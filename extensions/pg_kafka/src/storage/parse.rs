//! Payload parsing utilities for pg_kafka
//!
//! Provides functions to detect and parse UTF-8 text and JSON payloads
//! from Kafka message keys and values.

use serde_json::Value;

/// Parse bytes into text and JSON representations.
///
/// Returns `(text, json)` where:
/// - `text` is `Some` if bytes are valid UTF-8
/// - `json` is `Some` if bytes are valid JSON
///
/// # Examples
///
/// ```ignore
/// // Valid UTF-8, not JSON
/// let (text, json) = parse_bytes(b"hello world");
/// assert_eq!(text, Some("hello world".to_string()));
/// assert!(json.is_none());
///
/// // Valid JSON object
/// let (text, json) = parse_bytes(br#"{"name":"test"}"#);
/// assert!(text.is_some());
/// assert!(json.is_some());
/// ```
pub fn parse_bytes(bytes: &[u8]) -> (Option<String>, Option<Value>) {
    let text = std::str::from_utf8(bytes).ok().map(|s| s.to_string());
    let json = text.as_ref().and_then(|s| serde_json::from_str(s).ok());
    (text, json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_utf8_non_json() {
        let (text, json) = parse_bytes(b"hello world");
        assert_eq!(text, Some("hello world".to_string()));
        assert!(json.is_none());
    }

    #[test]
    fn test_parse_valid_json_object() {
        let (text, json) = parse_bytes(br#"{"name":"test"}"#);
        assert_eq!(text, Some(r#"{"name":"test"}"#.to_string()));
        assert!(json.is_some());
        assert_eq!(json.unwrap()["name"], "test");
    }

    #[test]
    fn test_parse_valid_json_array() {
        let (text, json) = parse_bytes(b"[1,2,3]");
        assert!(text.is_some());
        assert!(json.is_some());
        let arr = json.unwrap();
        assert!(arr.is_array());
        assert_eq!(arr.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_parse_invalid_utf8() {
        let (text, json) = parse_bytes(&[0xff, 0xfe, 0x00, 0x01]);
        assert!(text.is_none());
        assert!(json.is_none());
    }

    #[test]
    fn test_parse_empty_bytes() {
        let (text, json) = parse_bytes(b"");
        assert_eq!(text, Some("".to_string()));
        assert!(json.is_none()); // empty string is not valid JSON
    }

    #[test]
    fn test_parse_json_string_literal() {
        let (text, json) = parse_bytes(br#""just a string""#);
        assert!(text.is_some());
        assert!(json.is_some()); // JSON string literals are valid JSON
        assert_eq!(json.unwrap(), "just a string");
    }

    #[test]
    fn test_parse_json_number() {
        let (text, json) = parse_bytes(b"42");
        assert_eq!(text, Some("42".to_string()));
        assert!(json.is_some()); // numbers are valid JSON
        assert_eq!(json.unwrap(), 42);
    }

    #[test]
    fn test_parse_json_boolean_true() {
        let (text, json) = parse_bytes(b"true");
        assert_eq!(text, Some("true".to_string()));
        assert!(json.is_some());
        assert_eq!(json.unwrap(), true);
    }

    #[test]
    fn test_parse_json_boolean_false() {
        let (text, json) = parse_bytes(b"false");
        assert_eq!(text, Some("false".to_string()));
        assert!(json.is_some());
        assert_eq!(json.unwrap(), false);
    }

    #[test]
    fn test_parse_json_null() {
        let (text, json) = parse_bytes(b"null");
        assert_eq!(text, Some("null".to_string()));
        assert!(json.is_some());
        assert!(json.unwrap().is_null());
    }

    #[test]
    fn test_parse_nested_json() {
        let (text, json) = parse_bytes(br#"{"user":{"id":123,"name":"test"}}"#);
        assert!(text.is_some());
        assert!(json.is_some());
        let obj = json.unwrap();
        assert_eq!(obj["user"]["id"], 123);
        assert_eq!(obj["user"]["name"], "test");
    }

    #[test]
    fn test_parse_json_with_whitespace() {
        let (text, json) = parse_bytes(b"  { \"key\" : \"value\" }  ");
        assert!(text.is_some());
        assert!(json.is_some());
    }

    #[test]
    fn test_parse_invalid_json_valid_utf8() {
        // Valid UTF-8 but not valid JSON
        let (text, json) = parse_bytes(b"{not valid json}");
        assert!(text.is_some());
        assert!(json.is_none());
    }

    #[test]
    fn test_parse_typical_kafka_key() {
        // Common pattern: simple string key like "user-123"
        let (text, json) = parse_bytes(b"user-123");
        assert_eq!(text, Some("user-123".to_string()));
        assert!(json.is_none()); // not valid JSON
    }

    #[test]
    fn test_parse_compound_key() {
        // Common pattern: compound key like "order:456"
        let (text, json) = parse_bytes(b"order:456");
        assert_eq!(text, Some("order:456".to_string()));
        assert!(json.is_none());
    }

    #[test]
    fn test_parse_json_key() {
        // JSON key pattern
        let (text, json) = parse_bytes(br#"{"type":"user","id":123}"#);
        assert!(text.is_some());
        assert!(json.is_some());
        let obj = json.unwrap();
        assert_eq!(obj["type"], "user");
        assert_eq!(obj["id"], 123);
    }
}
