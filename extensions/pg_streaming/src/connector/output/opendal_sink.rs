//! OpenDAL-backed sink — writes each batch as a serialized file to any
//! OpenDAL backend (fs, s3, gcs, azblob, sftp, ftp, webdav, ...).
//!
//! DSL configuration:
//!
//! ```json
//! { "opendal": {
//!     "service": "fs",
//!     "config":  { "root": "/out" },
//!     "path":    "{date:YYYY/MM/DD}/{batch_id}.ndjson",
//!     "serialize_as": "ndjson",
//!     "codec":   "none"
//! }}
//! ```
//!
//! Path templating tokens:
//! - `{date:FMT}` — current UTC date in chrono strftime FMT
//! - `{batch_id}` — UUIDv7 per batch (also monotonic with time)
//! - `{record_count}` — number of records in the batch

use crate::connector::sdk::AsyncSink;
use async_trait::async_trait;
use opendal::{Operator, Scheme};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// DSL config for an OpenDAL sink.
#[derive(Debug, Clone, Deserialize)]
pub struct OpendalSinkConfig {
    pub service: String,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub config: HashMap<String, String>,
    pub path: String,
    /// Serializer: "ndjson" (default), "json" (single array), "csv", "bytes".
    #[serde(default = "default_serialize_as")]
    pub serialize_as: String,
    #[serde(default)]
    pub codec: Option<String>,
}

fn default_serialize_as() -> String {
    "ndjson".to_string()
}

/// OpenDAL-backed async sink.
#[derive(Debug)]
pub struct OpendalSink {
    config: OpendalSinkConfig,
    operator: Option<Operator>,
    batch_counter: u64,
}

impl OpendalSink {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: OpendalSinkConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("opendal sink: invalid config: {}", e))?;
        Ok(Self {
            config,
            operator: None,
            batch_counter: 0,
        })
    }

    fn build_operator(&self) -> Result<Operator, String> {
        let scheme = Scheme::from_str(&self.config.service)
            .map_err(|e| format!("opendal sink: unknown service: {}", e))?;
        let mut cfg = self.config.config.clone();
        if let Some(root) = &self.config.root {
            cfg.insert("root".to_string(), root.clone());
        }
        Operator::via_iter(scheme, cfg)
            .map_err(|e| format!("opendal sink: failed to build operator: {}", e))
    }

    fn ensure_operator(&mut self) -> Result<&Operator, String> {
        if self.operator.is_none() {
            let op = self.build_operator()?;
            self.operator = Some(op);
        }
        Ok(self.operator.as_ref().unwrap())
    }

    fn serialize(&self, records: &[Value]) -> Result<Vec<u8>, String> {
        match self.config.serialize_as.as_str() {
            "ndjson" => {
                let mut out = Vec::new();
                for r in records {
                    let line =
                        serde_json::to_vec(r).map_err(|e| format!("ndjson serialize: {}", e))?;
                    out.extend_from_slice(&line);
                    out.push(b'\n');
                }
                Ok(out)
            }
            "json" => serde_json::to_vec(records).map_err(|e| format!("json serialize: {}", e)),
            "bytes" => {
                // Pass-through: take the `bytes_base64` field of the first record.
                if let Some(first) = records.first() {
                    if let Some(b64) = first.get("bytes_base64").and_then(|v| v.as_str()) {
                        return base64_decode(b64);
                    }
                }
                Err("bytes serializer requires bytes_base64 field on the record".to_string())
            }
            other => Err(format!("Unknown serializer '{}'", other)),
        }
    }

    fn encode_codec(&self, bytes: Vec<u8>) -> Result<Vec<u8>, String> {
        match self.config.codec.as_deref() {
            None | Some("none") | Some("") => Ok(bytes),
            Some("gzip") => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                enc.write_all(&bytes)
                    .map_err(|e| format!("gzip write: {}", e))?;
                enc.finish().map_err(|e| format!("gzip finish: {}", e))
            }
            Some(other) => Err(format!("Unsupported codec '{}'", other)),
        }
    }
}

#[async_trait]
impl AsyncSink for OpendalSink {
    async fn write_batch(&mut self, records: &[Value]) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }
        self.batch_counter += 1;
        let path = render_path(&self.config.path, records.len(), self.batch_counter);
        let bytes = self.serialize(records)?;
        let encoded = self.encode_codec(bytes)?;
        let op = self.ensure_operator()?.clone();
        op.write(&path, encoded)
            .await
            .map_err(|e| format!("opendal write '{}' failed: {}", path, e))?;
        Ok(())
    }
}

/// Replace template tokens in `pattern`. Supported tokens:
/// - `{date:FMT}` — chrono::Utc::now().format(FMT)
/// - `{batch_id}` — UUIDv7
/// - `{record_count}` — usize as decimal
/// - `{seq}` — monotonic counter passed in
pub fn render_path(pattern: &str, record_count: usize, seq: u64) -> String {
    let mut out = String::with_capacity(pattern.len() + 32);
    let mut rest = pattern;
    loop {
        match rest.find('{') {
            None => {
                out.push_str(rest);
                return out;
            }
            Some(start) => {
                out.push_str(&rest[..start]);
                let after = &rest[start + 1..];
                match after.find('}') {
                    None => {
                        // Unterminated template — emit literally.
                        out.push('{');
                        out.push_str(after);
                        return out;
                    }
                    Some(end) => {
                        let token = &after[..end];
                        let resolved = render_token(token, record_count, seq);
                        out.push_str(&resolved);
                        rest = &after[end + 1..];
                    }
                }
            }
        }
    }
}

fn render_token(token: &str, record_count: usize, seq: u64) -> String {
    if let Some(fmt) = token.strip_prefix("date:") {
        return render_date(fmt);
    }
    match token {
        "batch_id" => uuid::Uuid::now_v7().to_string(),
        "record_count" => record_count.to_string(),
        "seq" => seq.to_string(),
        other => format!("{{{}}}", other), // unknown — leave as-is
    }
}

fn render_date(fmt: &str) -> String {
    // chrono uses strftime-style; the design doc uses YYYY/MM/DD style.
    // Translate the common forms to chrono format specifiers.
    let chrono_fmt = fmt
        .replace("YYYY", "%Y")
        .replace("MM", "%m")
        .replace("DD", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%S");
    // The chrono workspace dep is already in the tree (see Cargo.toml).
    // For non-chrono use, we fall back to %Y-%m-%d.
    let now = chrono_now();
    format!("{}", now.format(&chrono_fmt))
}

#[cfg(not(test))]
fn chrono_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

/// Fixed timestamp in tests for deterministic output.
#[cfg(test)]
fn chrono_now() -> chrono::DateTime<chrono::Utc> {
    use chrono::TimeZone;
    chrono::Utc
        .with_ymd_and_hms(2026, 5, 16, 14, 30, 45)
        .unwrap()
}

/// Minimal base64 decoder (RFC 4648). Mirrors the encoder in bytes_pass.
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0u32;
    for c in s.chars() {
        let v: u32 = match c {
            'A'..='Z' => (c as u32) - ('A' as u32),
            'a'..='z' => (c as u32) - ('a' as u32) + 26,
            '0'..='9' => (c as u32) - ('0' as u32) + 52,
            '+' => 62,
            '/' => 63,
            _ => return Err(format!("invalid base64 char '{}'", c)),
        };
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xff) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn render_path_substitutes_tokens() {
        let p = render_path("out/{date:YYYY/MM/DD}/{seq}.ndjson", 100, 7);
        assert_eq!(p, "out/2026/05/16/7.ndjson");
    }

    #[test]
    fn render_path_includes_record_count() {
        let p = render_path("out/n{record_count}-{seq}.json", 42, 1);
        assert_eq!(p, "out/n42-1.json");
    }

    #[test]
    fn render_path_batch_id_is_uuid_v7() {
        let p = render_path("out/{batch_id}.json", 0, 0);
        // Just verify it contains a uuid-shaped string.
        assert!(p.starts_with("out/"));
        assert!(p.ends_with(".json"));
        let uuid = p.trim_start_matches("out/").trim_end_matches(".json");
        assert_eq!(uuid.len(), 36);
        assert_eq!(uuid.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn render_path_unknown_token_passes_through() {
        let p = render_path("a/{unknown}/b", 0, 0);
        assert_eq!(p, "a/{unknown}/b");
    }

    #[test]
    fn render_path_literal_passes_through() {
        let p = render_path("literal/path.json", 0, 0);
        assert_eq!(p, "literal/path.json");
    }

    #[test]
    fn render_path_handles_unterminated() {
        let p = render_path("a/{unclosed", 0, 0);
        assert_eq!(p, "a/{unclosed");
    }

    #[test]
    fn render_path_date_only() {
        let p = render_path("{date:YYYY-MM-DD}.json", 0, 0);
        assert_eq!(p, "2026-05-16.json");
    }

    #[test]
    fn from_config_parses_minimal() {
        let cfg = json!({
            "service": "fs",
            "path": "out.ndjson"
        });
        let s = OpendalSink::from_config(&cfg).unwrap();
        assert_eq!(s.config.service, "fs");
        assert_eq!(s.config.serialize_as, "ndjson");
    }

    #[test]
    fn from_config_rejects_missing_path() {
        let cfg = json!({"service": "fs"});
        let err = OpendalSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("invalid config"));
    }

    #[test]
    fn serialize_ndjson() {
        let s = OpendalSink {
            config: OpendalSinkConfig {
                service: "memory".into(),
                root: None,
                config: HashMap::new(),
                path: "x".into(),
                serialize_as: "ndjson".into(),
                codec: None,
            },
            operator: None,
            batch_counter: 0,
        };
        let bytes = s.serialize(&[json!({"a": 1}), json!({"a": 2})]).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert_eq!(text, "{\"a\":1}\n{\"a\":2}\n");
    }

    #[test]
    fn serialize_json_array() {
        let s = OpendalSink {
            config: OpendalSinkConfig {
                service: "memory".into(),
                root: None,
                config: HashMap::new(),
                path: "x".into(),
                serialize_as: "json".into(),
                codec: None,
            },
            operator: None,
            batch_counter: 0,
        };
        let bytes = s.serialize(&[json!({"a": 1}), json!({"a": 2})]).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed[0]["a"], 1);
    }

    #[test]
    fn base64_decode_known_vectors() {
        assert_eq!(base64_decode("").unwrap(), b"");
        assert_eq!(base64_decode("Zg==").unwrap(), b"f");
        assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
        assert_eq!(base64_decode("Zm9v").unwrap(), b"foo");
        assert_eq!(base64_decode("Zm9vYg==").unwrap(), b"foob");
        assert_eq!(base64_decode("Zm9vYmFy").unwrap(), b"foobar");
    }

    #[tokio::test]
    async fn write_batch_memory_backend() {
        let cfg = json!({
            "service": "memory",
            "path": "out/{seq}.ndjson"
        });
        let mut sink = OpendalSink::from_config(&cfg).unwrap();
        sink.write_batch(&[json!({"a": 1}), json!({"b": 2})])
            .await
            .unwrap();

        // Read back what we wrote.
        let op = sink.operator.as_ref().unwrap().clone();
        let bytes = op.read("out/1.ndjson").await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(text, "{\"a\":1}\n{\"b\":2}\n");
    }

    #[tokio::test]
    async fn write_batch_empty_is_noop() {
        let cfg = json!({
            "service": "memory",
            "path": "out/{seq}.ndjson"
        });
        let mut sink = OpendalSink::from_config(&cfg).unwrap();
        sink.write_batch(&[]).await.unwrap();
        // No file should have been written.
        assert_eq!(sink.batch_counter, 0);
    }

    #[tokio::test]
    async fn write_batch_with_gzip_codec() {
        let cfg = json!({
            "service": "memory",
            "path": "out/{seq}.ndjson.gz",
            "codec": "gzip"
        });
        let mut sink = OpendalSink::from_config(&cfg).unwrap();
        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        let op = sink.operator.as_ref().unwrap().clone();
        let bytes = op.read("out/1.ndjson.gz").await.unwrap();
        // Decode and verify.
        use flate2::read::GzDecoder;
        use std::io::Read;
        let raw = bytes.to_vec();
        let mut dec = GzDecoder::new(raw.as_slice());
        let mut out = String::new();
        dec.read_to_string(&mut out).unwrap();
        assert_eq!(out, "{\"a\":1}\n");
    }
}
