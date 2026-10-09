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
//!     "mode":    "one_shot",
//!     "order":   "processed_set"
//! }}
//! ```
//!
//! ## Cursor orders
//!
//! - `processed_set` (default): the cursor lists every file already read. Any
//!   naming scheme works, but the cursor grows with the number of files.
//! - `lexicographic`: the cursor is the last fully-read path plus, while a file
//!   is in flight, the last row emitted from it — `{after, file, row}`. Files
//!   are read in path order and only paths after `after` are considered, so the
//!   cursor stays constant-size and a restart resumes mid-file without
//!   re-emitting rows. Listing pushes `start_after` down to backends that
//!   support it (S3). This is the order for the **raw zone**, whose contract
//!   requires that new files sort after old ones (arrival-date partitions with
//!   time-ordered file names, e.g. UUIDv7). A file that lands *before* the
//!   cursor is never read; replay with `pgstreams.replay` to pick it up.
//!
//! Every record carries `source_file` and `source_row` next to `value_json`,
//! so a target command can record exactly where a row came from.
//!
//! ## `parse_as: "listing"` (lexicographic only)
//!
//! Emits one record per new file **without reading it**: `{path, uri}`. This is
//! the raw-zone lane for N-D arrays (Zarr, NetCDF, COG), which pg_xarray
//! indexes in place rather than copying into rows — feed it to the
//! `xarray_header` processor and the `xarray_index` sink. A Zarr v3 store is a
//! directory, so glob its root metadata and strip it back to the store:
//!
//! ```json
//! { "path": "radar/**/*.zarr/zarr.json", "parse_as": "listing",
//!   "order": "lexicographic",
//!   "parser_config": { "uri_prefix": "s3://raw", "strip_suffix": "/zarr.json" } }
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
    /// Cursor order: "processed_set" (default) or "lexicographic". See the
    /// module docs.
    #[serde(default = "default_order")]
    pub order: String,
}

fn default_mode() -> String {
    "one_shot".to_string()
}

fn default_order() -> String {
    "processed_set".to_string()
}

/// Resume position for `order: lexicographic`.
#[derive(Debug, Clone, Default, PartialEq)]
struct LexCursor {
    /// Last file read to the end. Only paths sorting after it are listed.
    after: Option<String>,
    /// File in flight and the last row index emitted from it.
    in_flight: Option<(String, u64)>,
}

impl LexCursor {
    fn from_cursor(cursor: &Cursor) -> Self {
        let Cursor::Composite(v) = cursor else {
            return Self::default();
        };
        let after = v.get("after").and_then(|a| a.as_str()).map(String::from);
        let in_flight = match (
            v.get("file").and_then(|f| f.as_str()),
            v.get("row").and_then(|r| r.as_u64()),
        ) {
            (Some(f), Some(r)) => Some((f.to_string(), r)),
            _ => None,
        };
        Self { after, in_flight }
    }

    /// Cursor after emitting `row` of `file` (of `rows` total), given the last
    /// fully-read file before it.
    fn advance(after: Option<&str>, file: &str, row: u64, rows: u64) -> Cursor {
        if row + 1 >= rows {
            Cursor::Composite(serde_json::json!({ "after": file }))
        } else {
            Cursor::Composite(serde_json::json!({ "after": after, "file": file, "row": row }))
        }
    }
}

/// Wrap a parsed record in the standard "Messages" shape so engine SQL
/// processors can use `value_json->>'field'` consistently with Kafka inputs.
fn wrap_record(record: Value, source_topic: &str, file: &str, row: u64) -> Value {
    serde_json::json!({
        "key_text":     Value::Null,
        "key_json":     Value::Null,
        "value_text":   serde_json::to_string(&record).unwrap_or_default(),
        "value_json":   record,
        "headers":      serde_json::json!({}),
        "offset_id":    0,
        "created_at":   chrono::Utc::now().to_rfc3339(),
        "source_topic": source_topic,
        "source_file":  file,
        "source_row":   row,
    })
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
        if config.parse_as == "listing" && config.order != "lexicographic" {
            return Err("opendal: parse_as 'listing' needs order 'lexicographic'".to_string());
        }
        if !matches!(config.order.as_str(), "processed_set" | "lexicographic") {
            return Err(format!(
                "opendal: unknown order '{}' (processed_set, lexicographic)",
                config.order
            ));
        }
        Ok(Self { config })
    }

    fn codec_for(&self, file_path: &str) -> Codec {
        match self.config.codec.as_deref() {
            Some("gzip") => Codec::Gzip,
            Some("zstd") => Codec::Zstd,
            Some("none") | Some("") => Codec::None,
            None => Codec::from_filename(file_path).unwrap_or(Codec::None),
            Some(_) => Codec::None,
        }
    }

    /// `order: lexicographic` — read files in path order after the cursor,
    /// resuming a half-read file at the row after the last one emitted.
    async fn open_lexicographic(
        &self,
        last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
        let op = self.build_operator()?;
        let cursor = LexCursor::from_cursor(&last_cursor);

        // List from the in-flight file if there is one (it must be re-read),
        // otherwise from strictly after the last completed file.
        let mut paths = expand_paths_after(&op, &self.config.path, cursor.after.as_deref()).await?;
        if let Some((file, _)) = &cursor.in_flight {
            if !paths.contains(file) {
                // Listed paths exclude `after` itself; the in-flight file sorts
                // after it, so it is only missing if it was deleted.
                paths.retain(|p| p > file);
            }
        }

        if self.config.parse_as == "listing" {
            return listing_stream(paths, &cursor, &self.config.parser_config);
        }
        let parser = parser_from_config(&self.config.parse_as, &self.config.parser_config)?;
        let service = self.config.service.clone();
        let root = self.config.root.clone();
        let codecs: Vec<Codec> = paths.iter().map(|p| self.codec_for(p)).collect();

        // Lazy: one file is fetched and decoded at a time, so a replay over a
        // long history never holds more than a single arrival in memory.
        let s = async_stream::stream! {
            let mut after = cursor.after.clone();
            for (file_path, codec) in paths.into_iter().zip(codecs) {
                let skip_through = match &cursor.in_flight {
                    Some((f, row)) if *f == file_path => Some(*row),
                    _ => None,
                };
                let buf = match op.read(&file_path).await {
                    Ok(b) => b,
                    Err(e) => {
                        yield Err(format!("opendal: read '{}' failed: {}", file_path, e));
                        return;
                    }
                };
                let topic = make_uri(&service, root.as_deref(), &file_path);
                let ctx = ParseContext {
                    filename: Some(file_path.clone()),
                    source_uri: Some(topic.clone()),
                };
                let records = match decode_and_parse(
                    Bytes::from(buf.to_vec()),
                    codec,
                    parser.as_ref(),
                    &ctx,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        // Stop here: later files must not overtake an unreadable
                        // one, or the cursor would move past it and lose it.
                        yield Err(e);
                        return;
                    }
                };
                let rows = records.len() as u64;
                for (i, record) in records.into_iter().enumerate() {
                    let row = i as u64;
                    if skip_through.is_some_and(|s| row <= s) {
                        continue;
                    }
                    let cursor = LexCursor::advance(after.as_deref(), &file_path, row, rows);
                    yield Ok(SourceItem::new(
                        wrap_record(record, &topic, &file_path, row),
                        cursor,
                    ));
                }
                after = Some(file_path);
            }
        };
        Ok(s.boxed())
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
        if self.config.order == "lexicographic" {
            return self.open_lexicographic(last_cursor).await;
        }
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

                let source_topic = make_uri(&service, root.as_deref(), &file_path);
                let items: Vec<SourceItem> = records
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| {
                        SourceItem::new(
                            wrap_record(r, &source_topic, &file_path, i as u64),
                            cursor.clone(),
                        )
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

/// `parse_as: "listing"`: one `{path, uri}` record per file, nothing read.
fn listing_stream(
    paths: Vec<String>,
    cursor: &LexCursor,
    parser_config: &Value,
) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
    let prefix = parser_config
        .get("uri_prefix")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            "opendal: parse_as 'listing' needs parser_config.uri_prefix (e.g. \"s3://raw\")"
                .to_string()
        })?
        .trim_end_matches('/')
        .to_string();
    let strip = parser_config
        .get("strip_suffix")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let in_flight = cursor.in_flight.as_ref().map(|(f, _)| f.clone());
    let items: Vec<Result<SourceItem, String>> = paths
        .into_iter()
        // A listed file is one row; an in-flight one was already emitted.
        .filter(|p| in_flight.as_ref() != Some(p))
        .map(|path| {
            let target = path.strip_suffix(strip.as_str()).unwrap_or(&path);
            let uri = format!("{}/{}", prefix, target.trim_start_matches('/'));
            let record = serde_json::json!({ "path": path, "uri": uri });
            let topic = format!("{}/{}", prefix, path.trim_start_matches('/'));
            Ok(SourceItem::new(
                wrap_record(record, &topic, &path, 0),
                LexCursor::advance(None, &path, 0, 1),
            ))
        })
        .collect();
    Ok(stream::iter(items).boxed())
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

/// Like [`expand_paths`], but only paths sorting strictly after `after`, in
/// path order. A literal (non-glob) pattern is returned if it sorts after the
/// cursor. Uses the backend's `start_after` when it has one (S3 ListObjectsV2),
/// and always filters client-side as well, so backends without it are correct.
async fn expand_paths_after(
    op: &Operator,
    pattern: &str,
    after: Option<&str>,
) -> Result<Vec<String>, String> {
    let has_glob = pattern.contains('*') || pattern.contains('?') || pattern.contains('[');
    if !has_glob {
        return Ok(match after {
            Some(a) if pattern <= a => Vec::new(),
            _ => vec![pattern.to_string()],
        });
    }
    let parent = parent_of_glob(pattern);
    let mut lister = op.lister_with(&parent).recursive(true);
    if let Some(a) = after {
        if op.info().full_capability().list_with_start_after {
            lister = lister.start_after(a);
        }
    }
    let lister = lister
        .await
        .map_err(|e| format!("opendal: list '{}' failed: {}", parent, e))?;
    let matcher =
        glob::Pattern::new(pattern).map_err(|e| format!("opendal: invalid glob: {}", e))?;
    let mut paths = Vec::new();
    futures::pin_mut!(lister);
    while let Some(entry) = lister.next().await {
        let entry = entry.map_err(|e| format!("opendal: list iteration failed: {}", e))?;
        let p = entry.path().to_string();
        if entry.metadata().mode().is_file()
            && matcher.matches(&p)
            && after.is_none_or(|a| p.as_str() > a)
        {
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

    // ---- order: lexicographic (raw zone) ----------------------------------

    fn lex_cfg(dir: &std::path::Path, path: &str, parse_as: &str) -> serde_json::Value {
        serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": path,
            "parse_as": parse_as,
            "order": "lexicographic"
        })
    }

    async fn drain(source: &mut OpendalSource, cursor: Cursor) -> Vec<Result<SourceItem, String>> {
        let mut stream = source.open(cursor).await.unwrap();
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item);
        }
        out
    }

    /// Three arrivals in one partition, as the raw-zone contract lays them out.
    fn raw_zone(test: &str) -> std::path::PathBuf {
        let dir = fresh_tempdir(test);
        let part = dir.join("hic/arrival_date=2026-10-08");
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("01.ndjson"), b"{\"x\":1}\n{\"x\":2}\n").unwrap();
        std::fs::write(part.join("02.ndjson"), b"{\"x\":3}\n{\"x\":4}\n{\"x\":5}\n").unwrap();
        std::fs::write(part.join("03.ndjson"), b"{\"x\":6}\n").unwrap();
        dir
    }

    const F1: &str = "hic/arrival_date=2026-10-08/01.ndjson";
    const F2: &str = "hic/arrival_date=2026-10-08/02.ndjson";
    const F3: &str = "hic/arrival_date=2026-10-08/03.ndjson";

    #[test]
    fn unknown_order_is_rejected() {
        let cfg = serde_json::json!({
            "service": "fs", "path": "x", "parse_as": "json", "order": "newest"
        });
        assert!(OpendalSource::from_config(&cfg)
            .unwrap_err()
            .contains("unknown order"));
    }

    #[test]
    fn lex_cursor_is_row_precise_until_the_file_is_done() {
        let mid = LexCursor::advance(Some(F1), F2, 0, 3).to_json();
        assert_eq!(mid, serde_json::json!({"after": F1, "file": F2, "row": 0}));
        let done = LexCursor::advance(Some(F1), F2, 2, 3).to_json();
        assert_eq!(done, serde_json::json!({"after": F2}));
        let parsed = LexCursor::from_cursor(&Cursor::Composite(mid));
        assert_eq!(parsed.after.as_deref(), Some(F1));
        assert_eq!(parsed.in_flight, Some((F2.to_string(), 0)));
    }

    #[tokio::test]
    async fn lexicographic_reads_in_path_order_with_provenance() {
        let dir = raw_zone("lex_order");
        let mut source =
            OpendalSource::from_config(&lex_cfg(&dir, "hic/**/*.ndjson", "ndjson")).unwrap();
        let items: Vec<SourceItem> = drain(&mut source, Cursor::None)
            .await
            .into_iter()
            .map(|i| i.unwrap())
            .collect();

        let xs: Vec<i64> = items
            .iter()
            .map(|i| i.record["value_json"]["x"].as_i64().unwrap())
            .collect();
        assert_eq!(xs, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(items[3].record["source_file"], F2);
        assert_eq!(items[3].record["source_row"], 1);
        // The final cursor is constant-size: just the last completed file.
        assert_eq!(
            items.last().unwrap().cursor_advance.to_json(),
            serde_json::json!({"after": F3})
        );
        cleanup(&dir);
    }

    #[tokio::test]
    async fn lexicographic_resumes_mid_file_without_repeating_rows() {
        let dir = raw_zone("lex_resume");
        let mut source =
            OpendalSource::from_config(&lex_cfg(&dir, "hic/**/*.ndjson", "ndjson")).unwrap();
        // Crashed after emitting row 0 of 02.ndjson.
        let cursor = Cursor::Composite(serde_json::json!({"after": F1, "file": F2, "row": 0}));
        let xs: Vec<i64> = drain(&mut source, cursor)
            .await
            .into_iter()
            .map(|i| i.unwrap().record["value_json"]["x"].as_i64().unwrap())
            .collect();
        assert_eq!(xs, vec![4, 5, 6]);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn lexicographic_picks_up_new_arrivals_only() {
        let dir = raw_zone("lex_new");
        let mut source =
            OpendalSource::from_config(&lex_cfg(&dir, "hic/**/*.ndjson", "ndjson")).unwrap();
        let first = drain(&mut source, Cursor::None).await;
        let cursor = first
            .last()
            .unwrap()
            .as_ref()
            .unwrap()
            .cursor_advance
            .clone();

        // A new partition arrives (sorts after), and a late file lands in the
        // old partition (sorts before the cursor — skipped until a replay).
        let next = dir.join("hic/arrival_date=2026-10-09");
        std::fs::create_dir_all(&next).unwrap();
        std::fs::write(next.join("01.ndjson"), b"{\"x\":7}\n").unwrap();
        std::fs::write(
            dir.join("hic/arrival_date=2026-10-08/00.ndjson"),
            b"{\"x\":0}\n",
        )
        .unwrap();

        let xs: Vec<i64> = drain(&mut source, cursor)
            .await
            .into_iter()
            .map(|i| i.unwrap().record["value_json"]["x"].as_i64().unwrap())
            .collect();
        assert_eq!(xs, vec![7]);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn lexicographic_stops_at_an_unreadable_file() {
        let dir = raw_zone("lex_poison");
        std::fs::write(dir.join(F2), b"{not json\n").unwrap();
        let mut source =
            OpendalSource::from_config(&lex_cfg(&dir, "hic/**/*.ndjson", "ndjson")).unwrap();
        let items = drain(&mut source, Cursor::None).await;
        // 01 is emitted, 02 errors, and 03 must not overtake it.
        assert_eq!(items.len(), 3);
        assert!(items[0].is_ok() && items[1].is_ok());
        assert!(items[2].as_ref().unwrap_err().contains("line 1"));
        cleanup(&dir);
    }

    #[tokio::test]
    async fn lexicographic_parquet_end_to_end() {
        use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = fresh_tempdir("lex_parquet");
        let part = dir.join("hic-waterlevels/arrival_date=2026-10-08");
        std::fs::create_dir_all(&part).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("gauge", DataType::Utf8, false),
            Field::new("level_m", DataType::Float64, false),
        ]));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["DEM-DIE", "DEM-AAR"])),
            Arc::new(Float64Array::from(vec![3.41, 2.87])),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let file = std::fs::File::create(part.join("0192f0a1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let mut source =
            OpendalSource::from_config(&lex_cfg(&dir, "hic-waterlevels/**/*.parquet", "parquet"))
                .unwrap();
        let items: Vec<SourceItem> = drain(&mut source, Cursor::None)
            .await
            .into_iter()
            .map(|i| i.unwrap())
            .collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[1].record["value_json"]["gauge"], "DEM-AAR");
        assert_eq!(items[1].record["value_json"]["level_m"], 2.87);
        assert_eq!(items[1].record["source_row"], 1);
        cleanup(&dir);
    }

    #[tokio::test]
    async fn listing_emits_zarr_store_uris_without_reading() {
        let dir = fresh_tempdir("lex_listing");
        for day in ["2026-10-07", "2026-10-08"] {
            let store = dir.join(format!("radar/arrival_date={day}/kmi.zarr"));
            std::fs::create_dir_all(store.join("precip/c/0")).unwrap();
            // Unparseable on purpose: listing must never read the bytes.
            std::fs::write(store.join("zarr.json"), b"\x00not json").unwrap();
            std::fs::write(store.join("precip/c/0/0"), b"chunk").unwrap();
        }
        let cfg = serde_json::json!({
            "service": "fs",
            "root": dir.to_str().unwrap(),
            "path": "radar/**/*.zarr/zarr.json",
            "parse_as": "listing",
            "order": "lexicographic",
            "parser_config": {"uri_prefix": "s3://raw/", "strip_suffix": "/zarr.json"}
        });
        let mut source = OpendalSource::from_config(&cfg).unwrap();
        let items: Vec<SourceItem> = drain(&mut source, Cursor::None)
            .await
            .into_iter()
            .map(|i| i.unwrap())
            .collect();
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].record["value_json"]["uri"],
            "s3://raw/radar/arrival_date=2026-10-07/kmi.zarr"
        );
        // Resume: nothing new after the last store.
        let cursor = items[1].cursor_advance.clone();
        assert!(drain(&mut source, cursor).await.is_empty());
        cleanup(&dir);
    }

    #[test]
    fn listing_requires_lexicographic_order() {
        let cfg = serde_json::json!({
            "service": "fs", "path": "x/*", "parse_as": "listing"
        });
        assert!(OpendalSource::from_config(&cfg)
            .unwrap_err()
            .contains("lexicographic"));
    }
}
