//! Connector SDK — traits and types for async source/sink connectors.
//!
//! New connectors (OpenDAL, paginated REST, LDES, webhook, custom plugins) implement
//! the async traits here and are bridged to the engine's sync `InputConnector` /
//! `OutputConnector` via [`crate::connector::bridge`].
//!
//! See `design/pg_streaming/connectors.md` for the architecture overview.

// All items are public SDK surface; clippy can't see external callers.
#![allow(dead_code)]

use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// Per-item cursor advance emitted with each record. Persisted to
/// `pgstreams.connector_state` on every successful commit. On
/// `initialize`, the source reads the persisted cursor and resumes from it.
///
/// Cursors are stored as JSONB. The variants reflect common patterns:
///
/// - `Numeric` — monotonic id, mtime, page number
/// - `String` — opaque next-page token, ETag, IRI
/// - `Composite` — structured cursor (e.g. `{file, mtime, row}`)
/// - `None` — for one-shot sources that don't resume
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Cursor {
    Numeric(i64),
    String(String),
    Composite(Value),
    #[default]
    None,
}

impl Cursor {
    /// Returns `true` if this cursor carries no state (a no-op resume).
    pub fn is_none(&self) -> bool {
        matches!(self, Cursor::None)
    }

    /// Convert to JSONB-friendly `Value` for SPI persistence.
    pub fn to_json(&self) -> Value {
        match self {
            Cursor::Numeric(n) => Value::from(*n),
            Cursor::String(s) => Value::from(s.clone()),
            Cursor::Composite(v) => v.clone(),
            Cursor::None => Value::Null,
        }
    }

    /// Parse from a JSONB Value (inverse of `to_json`).
    pub fn from_json(value: &Value) -> Self {
        match value {
            Value::Null => Cursor::None,
            Value::Number(n) => n
                .as_i64()
                .map(Cursor::Numeric)
                .unwrap_or_else(|| Cursor::Composite(value.clone())),
            Value::String(s) => Cursor::String(s.clone()),
            _ => Cursor::Composite(value.clone()),
        }
    }
}

/// A single item produced by an async source: a record plus the cursor
/// that should be persisted after this record is successfully written
/// downstream.
#[derive(Debug, Clone)]
pub struct SourceItem {
    pub record: Value,
    pub cursor_advance: Cursor,
}

impl SourceItem {
    pub fn new(record: Value, cursor: Cursor) -> Self {
        Self {
            record,
            cursor_advance: cursor,
        }
    }

    /// Construct a SourceItem with no cursor advance (one-shot sources).
    pub fn one_shot(record: Value) -> Self {
        Self {
            record,
            cursor_advance: Cursor::None,
        }
    }
}

/// Async streaming source connector. Implementations live in
/// `connector/input/<name>.rs` and are wrapped by `AsyncSourceBridge`
/// to fit the sync `InputConnector` trait.
#[async_trait::async_trait]
pub trait AsyncSource: Send + 'static {
    /// Open a stream of source items, resuming from `last_cursor`.
    ///
    /// The stream ends when:
    /// - The source is exhausted (one-shot mode), or
    /// - The connector hits a transient end (watch mode) and should be reopened
    ///   after `poll_interval()`.
    ///
    /// Errors yielded from the stream propagate to the engine; the source
    /// can choose to continue (skip-and-emit-error) or end.
    async fn open(
        &mut self,
        last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String>;

    /// True if this source should reopen with a delay when its current
    /// stream ends. False for one-shot sources that should terminate.
    fn is_continuous(&self) -> bool {
        false
    }

    /// Delay between reopen attempts for continuous sources.
    fn poll_interval(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// Async streaming sink. Implementations live in `connector/output/<name>.rs`
/// and are wrapped by `AsyncSinkBridge` to fit the sync `OutputConnector` trait.
#[async_trait::async_trait]
pub trait AsyncSink: Send + 'static {
    /// Write a batch of records. Implementations may buffer internally —
    /// `flush` will be called at safe points.
    async fn write_batch(&mut self, records: &[Value]) -> Result<(), String>;

    /// Flush any buffered output. Called periodically by the bridge
    /// (e.g., on batch boundaries, before commit).
    async fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Compression codec applied to/from a byte stream.
///
/// `Codec` is intentionally a small enum; new codecs are added centrally.
/// Implementations live in `connector/parser/codec.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    #[default]
    None,
    Gzip,
    Zstd,
}

impl Codec {
    /// File-extension hint to guess codec from filename. Returns `None`
    /// if the codec can't be inferred.
    pub fn from_filename(name: &str) -> Option<Codec> {
        let lower = name.to_lowercase();
        if lower.ends_with(".gz") || lower.ends_with(".gzip") {
            Some(Codec::Gzip)
        } else if lower.ends_with(".zst") || lower.ends_with(".zstd") {
            Some(Codec::Zstd)
        } else {
            None
        }
    }
}

/// Context passed to parsers — origin metadata that may end up in
/// emitted records (`_filename`, `_source_uri`, etc.).
#[derive(Debug, Default, Clone)]
pub struct ParseContext {
    pub filename: Option<String>,
    pub source_uri: Option<String>,
}

/// Bytes → records parser. Format-specific. Stateless (constructed
/// fresh per file/payload via `parser_from_config`).
pub trait Parser: Send + Sync {
    /// Parse a complete in-memory payload (one file, one HTTP response).
    fn parse(&self, bytes: Bytes, context: &ParseContext) -> Result<Vec<Value>, String>;

    /// True if the parser supports streaming row-by-row. Streaming-capable
    /// parsers should also implement `parse_stream` (added in Phase 2).
    fn supports_streaming(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cursor_numeric_roundtrip() {
        let c = Cursor::Numeric(42);
        let v = c.to_json();
        assert_eq!(v, json!(42));
        let back = Cursor::from_json(&v);
        assert!(matches!(back, Cursor::Numeric(42)));
    }

    #[test]
    fn cursor_string_roundtrip() {
        let c = Cursor::String("next_page_token".into());
        let v = c.to_json();
        assert_eq!(v, json!("next_page_token"));
        let back = Cursor::from_json(&v);
        assert!(matches!(back, Cursor::String(s) if s == "next_page_token"));
    }

    #[test]
    fn cursor_composite_roundtrip() {
        let c = Cursor::Composite(json!({"file": "x.csv", "row": 100}));
        let v = c.to_json();
        assert_eq!(v["file"], "x.csv");
        let back = Cursor::from_json(&v);
        match back {
            Cursor::Composite(v) => assert_eq!(v["row"], 100),
            _ => panic!("expected composite"),
        }
    }

    #[test]
    fn cursor_none_roundtrip() {
        let c = Cursor::None;
        assert!(c.is_none());
        assert_eq!(c.to_json(), Value::Null);
        let back = Cursor::from_json(&Value::Null);
        assert!(back.is_none());
    }

    #[test]
    fn cursor_default_is_none() {
        let c = Cursor::default();
        assert!(c.is_none());
    }

    #[test]
    fn source_item_one_shot_has_none_cursor() {
        let item = SourceItem::one_shot(json!({"id": 1}));
        assert!(item.cursor_advance.is_none());
        assert_eq!(item.record["id"], 1);
    }

    #[test]
    fn codec_from_filename_detects_gzip() {
        assert_eq!(Codec::from_filename("orders.csv.gz"), Some(Codec::Gzip));
        assert_eq!(Codec::from_filename("data.gzip"), Some(Codec::Gzip));
        assert_eq!(Codec::from_filename("ORDERS.CSV.GZ"), Some(Codec::Gzip));
    }

    #[test]
    fn codec_from_filename_detects_zstd() {
        assert_eq!(Codec::from_filename("orders.csv.zst"), Some(Codec::Zstd));
        assert_eq!(Codec::from_filename("data.zstd"), Some(Codec::Zstd));
    }

    #[test]
    fn codec_from_filename_returns_none_for_plain() {
        assert_eq!(Codec::from_filename("orders.csv"), None);
        assert_eq!(Codec::from_filename("data.json"), None);
    }

    #[test]
    fn codec_default_is_none() {
        assert_eq!(Codec::default(), Codec::None);
    }

    #[test]
    fn codec_serde() {
        let json = serde_json::to_string(&Codec::Gzip).unwrap();
        assert_eq!(json, "\"gzip\"");
        let back: Codec = serde_json::from_str("\"zstd\"").unwrap();
        assert_eq!(back, Codec::Zstd);
        let none: Codec = serde_json::from_str("\"none\"").unwrap();
        assert_eq!(none, Codec::None);
    }
}
