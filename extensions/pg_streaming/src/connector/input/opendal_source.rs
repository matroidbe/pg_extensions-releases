//! OpenDAL-backed source — one connector covering many storage backends.
//!
//! Backends enabled at build time via Cargo features:
//! - `fs`, `memory`, `http` (always on)
//! - `opendal-s3`, `opendal-gcs`, `opendal-azblob`, `opendal-sftp`,
//!   `opendal-ftp`, `opendal-webdav` (opt-in)
//!
//! DSL configuration:
//!
//! ```json
//! { "opendal": {
//!     "service": "fs",
//!     "root":    "/incoming",
//!     "config":  { "key1": "val1", ... },
//!     "path":    "orders/*.csv.gz",
//!     "parse_as": "csv",
//!     "parser_config": { "header": true },
//!     "codec":   "gzip",
//!     "mode":    "one_shot"
//! }}
//! ```

use crate::connector::parser::{decode_and_parse, parser_from_config};
use crate::connector::sdk::{AsyncSource, Codec, Cursor, ParseContext, Parser, SourceItem};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use opendal::{Operator, Scheme};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// DSL config for an OpenDAL source.
#[derive(Debug, Clone, Deserialize)]
pub struct OpendalSourceConfig {
    /// OpenDAL service name: "fs", "memory", "http", "s3", "gcs", "azblob",
    /// "sftp", "ftp", "webdav".
    pub service: String,
    /// Optional root for the operator (e.g., a bucket root).
    #[serde(default)]
    pub root: Option<String>,
    /// Backend-specific configuration (passed to `Operator::via_iter`).
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// File path or glob (e.g. `"orders/*.csv.gz"`, `"path/to/file.json"`).
    pub path: String,
    /// Parser name: "csv", "json", "ndjson", "bytes".
    pub parse_as: String,
    /// Parser-specific options (passed to parser_from_config).
    #[serde(default)]
    pub parser_config: Value,
    /// Compression codec: "none" (default), "gzip", "zstd". Auto-detected
    /// from filename when absent.
    #[serde(default)]
    pub codec: Option<String>,
    /// Mode: "one_shot" (default) or "watch".
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn default_mode() -> String {
    "one_shot".to_string()
}

/// OpenDAL-backed async source.
#[derive(Debug)]
pub struct OpendalSource {
    config: OpendalSourceConfig,
}

impl OpendalSource {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: OpendalSourceConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("opendal: invalid config: {}", e))?;
        Ok(Self { config })
    }

    fn build_operator(&self) -> Result<Operator, String> {
        let scheme = Scheme::from_str(&self.config.service)
            .map_err(|e| format!("opendal: unknown service '{}': {}", self.config.service, e))?;
        let mut config = self.config.config.clone();
        if let Some(root) = &self.config.root {
            config.insert("root".to_string(), root.clone());
        }
        Operator::via_iter(scheme, config)
            .map_err(|e| format!("opendal: failed to build operator: {}", e))
    }

    // Codec selection happens inline in `read_chunk`'s per-file
    // closure (see below). A standalone helper was removed because
    // the closure captures fields the helper can't reach without
    // cloning, and clippy flagged it as dead.
}

#[async_trait]
impl AsyncSource for OpendalSource {
    async fn open(
        &mut self,
        last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
        let op = self.build_operator()?;
        let path = self.config.path.clone();
        let parser_name = self.config.parse_as.clone();
        let parser_config = self.config.parser_config.clone();
        let codec_override = self.config.codec.clone();
        let service = self.config.service.clone();
        let root = self.config.root.clone();

        // Determine the set of files already processed (for watch-mode resume).
        let processed: Vec<String> = match &last_cursor {
            Cursor::Composite(v) => v
                .get("processed")
                .and_then(|p| p.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|e| e.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        };

        // Resolve the paths to read. If `path` contains a glob, expand via lister.
        // Otherwise treat it as a single literal path.
        let paths = expand_paths(&op, &path).await?;

        // Build the parser once (stateless).
        let parser = parser_from_config(&parser_name, &parser_config)?;

        // Stream of (filename, bytes) → parse → SourceItems.
        let items_stream = stream::iter(paths).then(move |file_path| {
            let op = op.clone();
            let processed = processed.clone();
            let parser_ref: &dyn Parser = parser.as_ref();
            // We need to recreate the parser inside each block since it's a
            // trait object that can't be captured by the closure.
            let parser_name = parser_name.clone();
            let parser_config = parser_config.clone();
            let codec_override = codec_override.clone();
            let service = service.clone();
            let root = root.clone();
            async move {
                let _ = parser_ref; // silence unused for code reviewer

                if processed.contains(&file_path) {
                    return Ok::<Vec<SourceItem>, String>(Vec::new());
                }

                let bytes_buf = match op.read(&file_path).await {
                    Ok(b) => b,
                    Err(e) => return Err(format!("opendal: read '{}' failed: {}", file_path, e)),
                };
                let bytes = Bytes::from(bytes_buf.to_vec());

                let codec = match codec_override.as_deref() {
                    Some("gzip") => Codec::Gzip,
                    Some("zstd") => Codec::Zstd,
                    Some("none") | Some("") => Codec::None,
                    None => Codec::from_filename(&file_path).unwrap_or(Codec::None),
                    Some(_) => Codec::None,
                };

                let parser = parser_from_config(&parser_name, &parser_config)?;
                let ctx = ParseContext {
                    filename: Some(file_path.clone()),
                    source_uri: Some(make_uri(&service, root.as_deref(), &file_path)),
                };

                let records = decode_and_parse(bytes, codec, parser.as_ref(), &ctx)?;

                // Build cursor: composite { processed: [file_path] }.
                let mut new_processed = processed.clone();
                new_processed.push(file_path.clone());
                let cursor = Cursor::Composite(serde_json::json!({
                    "processed": new_processed,
                    "last_file": file_path,
                }));

                // Wrap each parsed record in the standard "Messages" shape so
                // engine SQL processors can use `value_json->>'field'` consistently
                // with Kafka inputs. _original holds the parsed record verbatim.
                let source_topic = make_uri(&service, root.as_deref(), &file_path);
                let items: Vec<SourceItem> = records
                    .into_iter()
                    .map(|r| {
                        let wrapped = serde_json::json!({
                            "key_text":     serde_json::Value::Null,
                            "key_json":     serde_json::Value::Null,
                            "value_text":   serde_json::to_string(&r).unwrap_or_default(),
                            "value_json":   r,
                            "headers":      serde_json::json!({}),
                            "offset_id":    0,
                            "created_at":   chrono::Utc::now().to_rfc3339(),
                            "source_topic": source_topic.clone(),
                        });
                        SourceItem::new(wrapped, cursor.clone())
                    })
                    .collect();
                Ok(items)
            }
        });

        // Flatten Result<Vec<SourceItem>, String> → Stream<Result<SourceItem, String>>.
        let flat = items_stream.flat_map(|res| match res {
            Ok(items) => {
                let iter: Box<dyn Iterator<Item = Result<SourceItem, String>> + Send> =
                    Box::new(items.into_iter().map(Ok));
                stream::iter(iter).boxed()
            }
            Err(e) => stream::iter(vec![Err(e)]).boxed(),
        });

        Ok(flat.boxed())
    }

    fn is_continuous(&self) -> bool {
        self.config.mode == "watch"
    }

    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(30)
    }
}

fn make_uri(service: &str, root: Option<&str>, path: &str) -> String {
    let prefix = match root {
        Some(r) if !r.is_empty() => format!("{}://{}", service, r.trim_end_matches('/')),
        _ => format!("{}://", service),
    };
    format!("{}/{}", prefix, path.trim_start_matches('/'))
}

/// Expand `pattern` to a list of file paths. If `pattern` contains glob
/// metacharacters (`*`, `?`, `[`), the operator's lister is used to
/// enumerate files and match against the glob. Otherwise the literal
/// path is returned.
async fn expand_paths(op: &Operator, pattern: &str) -> Result<Vec<String>, String> {
    let has_glob = pattern.contains('*') || pattern.contains('?') || pattern.contains('[');
    if !has_glob {
        return Ok(vec![pattern.to_string()]);
    }

    // Find the parent directory (everything before the first metachar segment).
    let parent = parent_of_glob(pattern);
    let lister = op
        .lister_with(&parent)
        .recursive(true)
        .await
        .map_err(|e| format!("opendal: list '{}' failed: {}", parent, e))?;

    let matcher =
        glob::Pattern::new(pattern).map_err(|e| format!("opendal: invalid glob: {}", e))?;
    let mut paths = Vec::new();
    futures::pin_mut!(lister);
    while let Some(entry) = lister.next().await {
        let entry = entry.map_err(|e| format!("opendal: list iteration failed: {}", e))?;
        let p = entry.path().to_string();
        if entry.metadata().mode().is_file() && matcher.matches(&p) {
            paths.push(p);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Find the longest prefix of `pattern` that has no glob metacharacters,
/// ending at a `/`. Used to scope `op.lister_with`.
fn parent_of_glob(pattern: &str) -> String {
    let mut parent = String::new();
    for segment in pattern.split('/') {
        if segment.contains('*') || segment.contains('?') || segment.contains('[') {
            break;
        }
        if !parent.is_empty() {
            parent.push('/');
        }
        parent.push_str(segment);
    }
    if parent.is_empty() {
        ".".to_string()
    } else {
        parent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_config_parses_minimal() {
        let cfg = serde_json::json!({
            "service": "fs",
            "path": "/tmp/data.json",
            "parse_as": "json"
        });
        let s = OpendalSource::from_config(&cfg).unwrap();
        assert_eq!(s.config.service, "fs");
        assert_eq!(s.config.path, "/tmp/data.json");
        assert_eq!(s.config.parse_as, "json");
        assert_eq!(s.config.mode, "one_shot");
    }

    #[test]
    fn from_config_rejects_invalid() {
        let cfg = serde_json::json!({"service": "fs"});
        let err = OpendalSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("invalid config"));
    }

    #[test]
    fn parent_of_glob_finds_literal_prefix() {
        assert_eq!(parent_of_glob("a/b/c/*.csv"), "a/b/c");
        assert_eq!(parent_of_glob("a/b/c.csv"), "a/b/c.csv");
        assert_eq!(parent_of_glob("*.csv"), ".");
        assert_eq!(parent_of_glob("a/*/b.csv"), "a");
        assert_eq!(parent_of_glob("a/b[12]/x.csv"), "a");
    }

    #[test]
    fn make_uri_handles_root() {
        assert_eq!(
            make_uri("fs", Some("/tmp/data"), "x.csv"),
            "fs:///tmp/data/x.csv"
        );
        assert_eq!(
            make_uri("s3", None, "bucket/key.csv"),
            "s3:///bucket/key.csv"
        );
        assert_eq!(make_uri("fs", Some(""), "x.csv"), "fs:///x.csv");
    }

    /// Build a unique temp directory under /tmp for an isolated fs-backed test.
    fn fresh_tempdir(test_name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pgstreams_opendal_{}_{}_{}",
            test_name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Integration test against the fs backend with a real temp dir.
    /// Verifies the full open → read → parse → emit pipeline.
    #[tokio::test]
    async fn end_to_end_fs_backend_json() {
        let dir = fresh_tempdir("json");
        std::fs::write(dir.join("data.json"), br#"[{"id": 1}, {"id": 2}]"#).unwrap();

        let cfg = serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": "data.json",
            "parse_as": "json"
        });
        let mut source = OpendalSource::from_config(&cfg).unwrap();

        let mut stream = source.open(Cursor::None).await.unwrap();
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        assert_eq!(items.len(), 2);
        // Records are wrapped in the standard Messages shape (`value_json`)
        // so engine SQL processors can use `value_json->>'field'`.
        assert_eq!(items[0].record["value_json"]["id"], 1);
        assert_eq!(items[1].record["value_json"]["id"], 2);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn end_to_end_fs_backend_ndjson_glob() {
        let dir = fresh_tempdir("ndjson");
        std::fs::create_dir_all(dir.join("d")).unwrap();
        std::fs::write(dir.join("d/a.ndjson"), b"{\"x\":1}\n{\"x\":2}\n").unwrap();
        std::fs::write(dir.join("d/b.ndjson"), b"{\"x\":3}\n").unwrap();

        let cfg = serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": "d/*.ndjson",
            "parse_as": "ndjson"
        });
        let mut source = OpendalSource::from_config(&cfg).unwrap();
        let mut stream = source.open(Cursor::None).await.unwrap();
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        assert_eq!(items.len(), 3);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn cursor_resume_skips_processed_files() {
        let dir = fresh_tempdir("resume");
        std::fs::create_dir_all(dir.join("d")).unwrap();
        std::fs::write(dir.join("d/a.json"), br#"{"x":1}"#).unwrap();
        std::fs::write(dir.join("d/b.json"), br#"{"x":2}"#).unwrap();

        let cfg = serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": "d/*.json",
            "parse_as": "json"
        });
        let mut source = OpendalSource::from_config(&cfg).unwrap();

        let cursor = Cursor::Composite(serde_json::json!({
            "processed": ["d/a.json"]
        }));
        let mut stream = source.open(cursor).await.unwrap();
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        // Only b.json should be processed.
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].record["value_json"]["x"], 2);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn end_to_end_fs_backend_csv_gzipped() {
        let dir = fresh_tempdir("csvgz");
        // gzip-compressed CSV
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"id,name\n1,Alice\n2,Bob\n").unwrap();
        let compressed = encoder.finish().unwrap();
        std::fs::write(dir.join("orders.csv.gz"), &compressed).unwrap();

        let cfg = serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": "orders.csv.gz",
            "parse_as": "csv",
            "codec": "gzip"
        });
        let mut source = OpendalSource::from_config(&cfg).unwrap();
        let mut stream = source.open(Cursor::None).await.unwrap();
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].record["value_json"]["id"], "1");
        assert_eq!(items[0].record["value_json"]["name"], "Alice");
        assert_eq!(items[1].record["value_json"]["name"], "Bob");
        cleanup(&dir);
    }
}
