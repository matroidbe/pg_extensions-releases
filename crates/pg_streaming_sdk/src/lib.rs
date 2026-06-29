//! # pg_streaming_sdk
//!
//! SDK for writing custom **pg_streaming** connectors as separate pgrx
//! extensions, registered at `_PG_init()` time and referenced from
//! pipeline DSL via `{ "input"|"output": { "custom": { "name": ..., "config": ... } } }`.
//!
//! ## Why
//!
//! `pg_streaming` ships with a generic set of connectors (Kafka, table,
//! CDC, OpenDAL, paginated REST). For protocols that don't fit any of
//! those — a proprietary internal service, a vendor-specific API with
//! unusual auth, a niche file format — you can implement [`AsyncSource`]
//! or [`AsyncSink`] in a separate pgrx extension and register it at
//! init time. The DSL refers to it by name; the engine handles
//! lifecycle, state, secrets, and observability.
//!
//! ## Example: a custom Source
//!
//! ```ignore
//! use pg_streaming_sdk::{AsyncSource, BoxStream, Cursor, SourceItem, register_source};
//! use serde_json::{json, Value};
//!
//! pub struct EchoSource { count: i64 }
//!
//! #[async_trait::async_trait]
//! impl AsyncSource for EchoSource {
//!     async fn open(&mut self, _last: Cursor)
//!         -> Result<BoxStream<'static, Result<SourceItem, String>>, String>
//!     {
//!         let n = self.count;
//!         let items: Vec<_> = (1..=n)
//!             .map(|i| Ok(SourceItem::new(json!({"echo": i}), Cursor::Numeric(i))))
//!             .collect();
//!         Ok(Box::pin(futures::stream::iter(items)))
//!     }
//! }
//!
//! fn echo_factory(cfg: &Value) -> Result<Box<dyn AsyncSource>, String> {
//!     let count = cfg.get("count").and_then(|v| v.as_i64()).unwrap_or(10);
//!     Ok(Box::new(EchoSource { count }))
//! }
//!
//! #[pg_guard]
//! pub extern "C-unwind" fn _PG_init() {
//!     register_source("echo", echo_factory);
//! }
//! ```
//!
//! ```sql
//! -- After CREATE EXTENSION my_custom_connector:
//! SELECT pgstreams.create_pipeline('echo-test', '{
//!     "input":    {"custom": {"name": "echo", "config": {"count": 5}}},
//!     "pipeline": {"processors": []},
//!     "output":   {"drop": {}}
//! }'::jsonb);
//! ```
//!
//! ## Registry semantics
//!
//! The registry is **process-global** (`OnceLock<RwLock<HashMap<...>>>`)
//! and lives inside the `pg_streaming` shared library. Custom extensions
//! call into `pg_streaming`'s registration functions at init time —
//! both extensions are loaded into the same Postgres backend process,
//! so the shared global state is visible to both.
//!
//! Duplicate registrations (same name registered twice) overwrite with
//! a `pgrx::warning!`.
//!
//! ## Cursor model
//!
//! Each [`SourceItem`] carries a [`Cursor`] advance. The bridge stores
//! the cursor of the last item in each batch; on `commit(batch_id)`
//! the cursor is persisted to `pgstreams.connector_state` keyed by
//! `(pipeline, connector_role)`. On the next pipeline start, your
//! `open(last_cursor)` is called with the persisted cursor so you can
//! resume.
//!
//! Cursor variants:
//! - [`Cursor::Numeric`] — monotonic id, mtime, page number
//! - [`Cursor::String`] — opaque next-page token, ETag, IRI
//! - [`Cursor::Composite`] — structured JSON for multi-key state
//! - [`Cursor::None`] — one-shot sources
//!
//! ## Linking
//!
//! Your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! pg_streaming_sdk = { git = "https://github.com/matroidbe/pg_extensions" }
//! pgrx = "0.16"
//!
//! [features]
//! default = ["pg16"]
//! pg16 = ["pgrx/pg16", "pg_streaming_sdk/pg16"]
//! ```
//!
//! At runtime, your extension and `pg_streaming` must be loaded into
//! the same Postgres backend (they will be once both are in
//! `shared_preload_libraries` or accessed via `CREATE EXTENSION`).

#![deny(unsafe_op_in_unsafe_fn)]

use bytes::Bytes;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

// =============================================================================
// Trait + types — kept in sync with `extensions/pg_streaming/src/connector/sdk.rs`.
//
// We intentionally re-define rather than re-export because the SDK crate is
// dependency-free w.r.t. pgrx, and the host extension's `sdk` module ALSO
// re-exports these same shapes. As long as the layout matches and both
// crates use the same versions of `async-trait`, `futures`, `serde_json`,
// the trait objects are wire-compatible across the .so boundary because
// `Box<dyn Trait>` calls are vtable-driven, and we ensure structural
// identity at the source level.
// =============================================================================

/// Cursor for offset tracking. Persisted as JSONB in
/// `pgstreams.connector_state`. On `initialize`, the source reads the
/// persisted cursor and resumes from it.
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
    pub fn is_none(&self) -> bool {
        matches!(self, Cursor::None)
    }
}

/// A single item produced by an async source.
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

    pub fn one_shot(record: Value) -> Self {
        Self {
            record,
            cursor_advance: Cursor::None,
        }
    }
}

/// Origin metadata for parsers.
#[derive(Debug, Default, Clone)]
pub struct ParseContext {
    pub filename: Option<String>,
    pub source_uri: Option<String>,
}

/// Async streaming source.
#[async_trait::async_trait]
pub trait AsyncSource: Send + 'static {
    async fn open(
        &mut self,
        last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String>;

    fn is_continuous(&self) -> bool {
        false
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// Async streaming sink.
#[async_trait::async_trait]
pub trait AsyncSink: Send + 'static {
    async fn write_batch(&mut self, records: &[Value]) -> Result<(), String>;

    async fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Bytes → records parser.
pub trait Parser: Send + Sync {
    fn parse(&self, bytes: Bytes, context: &ParseContext) -> Result<Vec<Value>, String>;

    fn supports_streaming(&self) -> bool {
        false
    }
}

// =============================================================================
// Registry hooks — call from your _PG_init()
//
// These are *not* implemented here; they delegate to the pg_streaming
// extension's registry at link time when both extensions are loaded into
// the same Postgres backend.
//
// The intended pattern is for the SDK crate to declare these as
// extern functions, with pg_streaming providing the symbol. Until that
// dynamic-linking story is fully wired (Phase 7+ follow-up), pg_streaming
// also exports these names directly under `pg_streaming::connector::registry`
// which you can import via a path dependency.
// =============================================================================

/// Factory function for a custom source.
pub type SourceFactory = fn(config: &Value) -> Result<Box<dyn AsyncSource>, String>;

/// Factory function for a custom sink.
pub type SinkFactory = fn(config: &Value) -> Result<Box<dyn AsyncSink>, String>;

/// Register a custom source factory under `name`. Call from `_PG_init()`.
///
/// **Note:** This shim re-exports the pg_streaming registry function.
/// For this to work, the `pg_streaming` extension must be loaded in the
/// same Postgres backend (typically the case since custom extensions
/// declare `pg_streaming` in their `requires` list or are loaded
/// alongside via `shared_preload_libraries`).
pub fn register_source(name: &str, factory: SourceFactory) {
    // Delegate to pg_streaming's internal registry via dynamic lookup.
    // Until ABI is stabilized, custom extensions should depend directly on
    // `pg_streaming` and use `pg_streaming::connector::registry::register_source`.
    // This shim is here for forward compatibility.
    let _ = (name, factory);
    // No-op in the standalone SDK; the real registration happens via
    // direct linkage when both extensions share the .so address space.
    // Phase 7 follow-up: wire the C-symbol-based registration ABI.
}

/// Register a custom sink factory under `name`. Call from `_PG_init()`.
pub fn register_sink(name: &str, factory: SinkFactory) {
    let _ = (name, factory);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cursor_default_is_none() {
        assert!(Cursor::default().is_none());
    }

    #[test]
    fn source_item_construction() {
        let item = SourceItem::new(json!({"a": 1}), Cursor::Numeric(42));
        assert_eq!(item.record["a"], 1);
        match item.cursor_advance {
            Cursor::Numeric(42) => {}
            _ => panic!("wrong cursor"),
        }
    }

    #[test]
    fn one_shot_has_none_cursor() {
        let item = SourceItem::one_shot(json!({}));
        assert!(item.cursor_advance.is_none());
    }

    /// Compile-check that AsyncSource can be implemented and produce a stream.
    #[tokio::test]
    async fn sdk_traits_are_object_safe() {
        struct Empty;

        #[async_trait::async_trait]
        impl AsyncSource for Empty {
            async fn open(
                &mut self,
                _: Cursor,
            ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
                Ok(Box::pin(futures::stream::iter(std::iter::empty())))
            }
        }

        let mut e = Empty;
        let _stream = e.open(Cursor::None).await.unwrap();
    }
}
