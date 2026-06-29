//! Pass-through parser — emits one record per payload with the raw bytes
//! base64-encoded plus origin metadata. Useful for cases where the
//! downstream pipeline does its own parsing in SQL.

use crate::connector::sdk::{ParseContext, Parser};
use bytes::Bytes;
use serde_json::{json, Value};

/// One record per payload: `{ filename, source_uri, bytes_base64, byte_len }`.
pub struct BytesParser;

impl BytesParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BytesParser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser for BytesParser {
    fn parse(&self, bytes: Bytes, context: &ParseContext) -> Result<Vec<Value>, String> {
        let byte_len = bytes.len();
        let encoded = base64_encode(bytes.as_ref());
        Ok(vec![json!({
            "filename":     context.filename,
            "source_uri":   context.source_uri,
            "byte_len":     byte_len,
            "bytes_base64": encoded,
        })])
    }
}

/// Minimal base64 encoder (RFC 4648 standard alphabet, with padding).
/// Avoids pulling in the `base64` crate just for this one function.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= input.len() {
        let b0 = input[i];
        let b1 = input[i + 1];
        let b2 = input[i + 2];
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(TABLE[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize] as char);
        out.push(TABLE[(b2 & 0x3F) as usize] as char);
        i += 3;
    }
    match input.len() - i {
        0 => {}
        1 => {
            let b0 = input[i];
            out.push(TABLE[(b0 >> 2) as usize] as char);
            out.push(TABLE[((b0 & 0x03) << 4) as usize] as char);
            out.push('=');
            out.push('=');
        }
        2 => {
            let b0 = input[i];
            let b1 = input[i + 1];
            out.push(TABLE[(b0 >> 2) as usize] as char);
            out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
            out.push(TABLE[((b1 & 0x0F) << 2) as usize] as char);
            out.push('=');
        }
        _ => unreachable!(),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_to_single_record() {
        let p = BytesParser::new();
        let ctx = ParseContext {
            filename: Some("x.bin".into()),
            source_uri: Some("fs:///tmp/x.bin".into()),
        };
        let result = p.parse(Bytes::from_static(b"hello"), &ctx).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["filename"], "x.bin");
        assert_eq!(result[0]["source_uri"], "fs:///tmp/x.bin");
        assert_eq!(result[0]["byte_len"], 5);
        assert_eq!(result[0]["bytes_base64"], "aGVsbG8=");
    }

    #[test]
    fn handles_empty_payload() {
        let p = BytesParser::new();
        let ctx = ParseContext::default();
        let result = p.parse(Bytes::new(), &ctx).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["byte_len"], 0);
        assert_eq!(result[0]["bytes_base64"], "");
    }

    #[test]
    fn base64_encode_known_vectors() {
        // RFC 4648 test vectors
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }
}
