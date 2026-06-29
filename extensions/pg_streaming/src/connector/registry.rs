//! Process-global registry for custom connectors.
//!
//! Populated at `_PG_init()` by custom pgrx extensions. Looked up at
//! pipeline-compile time when the DSL refers to
//! `{ "input": { "custom": { "name": "...", "config": {...} } } }`.

// All registry functions are public SDK surface used by separate
// pgrx extensions; clippy can't see those callers.
#![allow(dead_code)]

use crate::connector::sdk::{AsyncSink, AsyncSource};
use crate::connector::OutputConnector;
use crate::processor::Processor;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Factory function for an async source. Receives the connector's
/// `config` JSONB (after secret interpolation) and returns the source.
pub type SourceFactory = fn(config: &Value) -> Result<Box<dyn AsyncSource>, String>;

/// Factory function for an async sink.
pub type SinkFactory = fn(config: &Value) -> Result<Box<dyn AsyncSink>, String>;

/// Factory function for a SYNC sink — needed when the sink must call
/// SPI (which is bound to the PG worker thread and incompatible with
/// async runtimes). Returned `OutputConnector` runs directly in the
/// engine's process_batch context.
pub type SyncSinkFactory = fn(config: &Value) -> Result<Box<dyn OutputConnector>, String>;

/// Factory function for a custom processor. The returned `Processor`
/// is sync (`process(batch) -> RecordBatch`); async I/O inside is fine
/// via an internal tokio runtime + `block_on`.
pub type ProcessorFactory = fn(config: &Value) -> Result<Box<dyn Processor>, String>;

static SOURCE_REGISTRY: OnceLock<RwLock<HashMap<String, SourceFactory>>> = OnceLock::new();
static SINK_REGISTRY: OnceLock<RwLock<HashMap<String, SinkFactory>>> = OnceLock::new();
static SYNC_SINK_REGISTRY: OnceLock<RwLock<HashMap<String, SyncSinkFactory>>> = OnceLock::new();
static PROCESSOR_REGISTRY: OnceLock<RwLock<HashMap<String, ProcessorFactory>>> = OnceLock::new();

fn sources() -> &'static RwLock<HashMap<String, SourceFactory>> {
    SOURCE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn sinks() -> &'static RwLock<HashMap<String, SinkFactory>> {
    SINK_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn sync_sinks() -> &'static RwLock<HashMap<String, SyncSinkFactory>> {
    SYNC_SINK_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

fn processors() -> &'static RwLock<HashMap<String, ProcessorFactory>> {
    PROCESSOR_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register a custom source factory. Call from `_PG_init()` of a
/// dependent pgrx extension. Duplicate names overwrite (with a warning).
pub fn register_source(name: &str, factory: SourceFactory) {
    let mut map = sources().write().expect("source registry poisoned");
    if map.insert(name.to_string(), factory).is_some() {
        pgrx::warning!(
            "pg_streaming: custom source '{}' re-registered (overwriting previous)",
            name
        );
    }
}

/// Register a custom sink factory.
pub fn register_sink(name: &str, factory: SinkFactory) {
    let mut map = sinks().write().expect("sink registry poisoned");
    if map.insert(name.to_string(), factory).is_some() {
        pgrx::warning!(
            "pg_streaming: custom sink '{}' re-registered (overwriting previous)",
            name
        );
    }
}

/// Register a custom SYNC sink factory. Use this when the sink needs
/// to call SPI (e.g., writes to PG tables via `pgrx::Spi`).
pub fn register_sync_sink(name: &str, factory: SyncSinkFactory) {
    let mut map = sync_sinks().write().expect("sync sink registry poisoned");
    if map.insert(name.to_string(), factory).is_some() {
        pgrx::warning!(
            "pg_streaming: custom sync sink '{}' re-registered (overwriting previous)",
            name
        );
    }
}

/// Register a custom processor factory. Referenced from pipeline DSL as
/// `{ "custom": { "name": "...", "config": {...} } }` in the processors list.
pub fn register_processor(name: &str, factory: ProcessorFactory) {
    let mut map = processors().write().expect("processor registry poisoned");
    if map.insert(name.to_string(), factory).is_some() {
        pgrx::warning!(
            "pg_streaming: custom processor '{}' re-registered (overwriting previous)",
            name
        );
    }
}

/// Look up a source factory. Returns `None` if no source by this name has
/// been registered.
pub fn lookup_source(name: &str) -> Option<SourceFactory> {
    sources().read().ok().and_then(|map| map.get(name).copied())
}

/// Look up a sink factory.
pub fn lookup_sink(name: &str) -> Option<SinkFactory> {
    sinks().read().ok().and_then(|map| map.get(name).copied())
}

/// Look up a sync sink factory.
pub fn lookup_sync_sink(name: &str) -> Option<SyncSinkFactory> {
    sync_sinks()
        .read()
        .ok()
        .and_then(|map| map.get(name).copied())
}

/// Look up a processor factory.
pub fn lookup_processor(name: &str) -> Option<ProcessorFactory> {
    processors()
        .read()
        .ok()
        .and_then(|map| map.get(name).copied())
}

/// List names of all registered custom processors.
pub fn list_processors() -> Vec<String> {
    processors()
        .read()
        .ok()
        .map(|map| {
            let mut v: Vec<String> = map.keys().cloned().collect();
            v.sort();
            v
        })
        .unwrap_or_default()
}

/// List names of all registered custom sources (used in observability).
pub fn list_sources() -> Vec<String> {
    sources()
        .read()
        .ok()
        .map(|map| {
            let mut v: Vec<String> = map.keys().cloned().collect();
            v.sort();
            v
        })
        .unwrap_or_default()
}

/// List names of all registered custom sinks (async + sync, deduplicated).
pub fn list_sinks() -> Vec<String> {
    let mut all: Vec<String> = sinks()
        .read()
        .ok()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default();
    if let Ok(sync_map) = sync_sinks().read() {
        for k in sync_map.keys() {
            if !all.contains(k) {
                all.push(k.clone());
            }
        }
    }
    all.sort();
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::sdk::{AsyncSource, Cursor, SourceItem};
    use futures::stream::{self, BoxStream};
    use serde_json::json;

    struct DummySource;

    #[async_trait::async_trait]
    impl AsyncSource for DummySource {
        async fn open(
            &mut self,
            _last_cursor: Cursor,
        ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
            Ok(Box::pin(stream::iter(vec![Ok(SourceItem::one_shot(
                json!({"hello": "world"}),
            ))])))
        }
    }

    fn dummy_factory(_cfg: &serde_json::Value) -> Result<Box<dyn AsyncSource>, String> {
        Ok(Box::new(DummySource))
    }

    #[test]
    fn register_and_lookup_source() {
        // Use a unique name to avoid colliding with other tests that share the
        // process-global registry.
        let unique = format!("test_reg_{}", std::process::id());
        register_source(&unique, dummy_factory);
        let f = lookup_source(&unique).expect("registered source should be found");
        let _src = f(&json!({})).unwrap();
        assert!(list_sources().contains(&unique));
    }

    #[test]
    fn lookup_unknown_returns_none() {
        let result = lookup_source("nonexistent_source_xyz_42");
        assert!(result.is_none());
    }

    #[test]
    fn list_sources_is_sorted() {
        // Just sanity-check the ordering invariant; some entries may exist
        // from concurrent tests.
        let names = list_sources();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }
}
