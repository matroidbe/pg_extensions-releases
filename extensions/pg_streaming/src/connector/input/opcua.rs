//! OPC UA source — opens one session to an OPC UA server via the pure-Rust
//! `async-opcua` client, creates one subscription over the configured nodes
//! and emits one record per DataChange notification.
//!
//! Cursor is `Cursor::None`: OPC UA has no replayable offset (same as the
//! Modbus and S7 sources). Error policy follows them too: connect inside
//! the stream, end the stream cleanly on a transient error (connect
//! failure, connection lost) so the bridge's reopen loop acts as
//! reconnect-with-delay, and fail the pipeline only after
//! `max_consecutive_failures` (`0` = retry forever). Each reopen is a fresh
//! session, so the namespace index and browse paths are resolved again —
//! a restarted server may hand out different indexes. Configuration
//! errors found at connect time (unknown namespace, unresolvable browse
//! path, a node the server rejects) fail the pipeline immediately.
//!
//! The library's own reconnect loop is disabled (`session_retry_limit(0)`):
//! one stream = one session. With `emit_link_records`, the stream yields a
//! `link: up` record once the subscription is live and `link: lost` when
//! that session drops.
//!
//! DSL configuration: see `design/pg_streaming/connectors.md`.

use crate::connector::input::table::parse_poll_interval_ms;
use crate::connector::sdk::{AsyncSource, Cursor, SourceItem};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use opcua::client::{
    ClientBuilder, DataChangeCallback, IdentityToken, MonitoredItem, Session, SessionPollResult,
};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    BrowsePath, DataValue, DateTime, EndpointDescription, Identifier, MessageSecurityMode,
    MonitoredItemCreateRequest, MonitoringMode, MonitoringParameters, NodeId, ObjectId,
    QualifiedName, ReadValueId, ReferenceTypeId, RelativePath, RelativePathElement, StatusCode,
    TimestampsToReturn, UserTokenPolicy, Variant,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const MAX_INTERVAL_MS: u64 = 60_000;

// =============================================================================
// Configuration
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpcuaSourceConfig {
    /// `opc.tcp://host:port[/path]`.
    pub endpoint: String,
    #[serde(default)]
    pub security_mode: SecurityMode,
    #[serde(default)]
    pub security_policy: SecurityPolicyName,
    #[serde(default)]
    pub identity: Identity,
    /// Namespace that unprefixed node ids and browse-path segments live in.
    /// Resolved to an index at connect time. Omitted = namespace 0.
    #[serde(default)]
    pub namespace_uri: Option<String>,
    #[serde(default = "default_publishing_interval")]
    pub publishing_interval: String,
    #[serde(default = "default_sampling_interval")]
    pub sampling_interval: String,
    /// Server-side queue per monitored item, so samples taken between two
    /// publishes are not dropped.
    #[serde(default = "default_queue_size")]
    pub queue_size: u32,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Bounds every service call (read, translate, create subscription).
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Delay between a lost/failed session and the next attempt.
    #[serde(default = "default_reconnect_interval")]
    pub reconnect_interval: String,
    /// `0` = retry forever.
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
    #[serde(default = "default_true")]
    pub emit_link_records: bool,
    /// Client PKI directory. Unused while `security_mode` is `none`, but the
    /// client still creates it. Default: `<tmp>/pg_streaming_opcua/pki`.
    #[serde(default)]
    pub pki_dir: Option<String>,
    pub nodes: Vec<NodeEntry>,
}

fn default_publishing_interval() -> String {
    "1s".to_string()
}
fn default_sampling_interval() -> String {
    "250ms".to_string()
}
fn default_queue_size() -> u32 {
    10
}
fn default_connect_timeout_ms() -> u64 {
    5000
}
fn default_request_timeout_ms() -> u64 {
    5000
}
fn default_reconnect_interval() -> String {
    "5s".to_string()
}
fn default_max_consecutive_failures() -> u32 {
    5
}
fn default_true() -> bool {
    true
}

/// Message security mode. Only `none` is implemented; the other variants
/// parse so configs can name them, and validation rejects them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityMode {
    #[default]
    None,
    Sign,
    SignAndEncrypt,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityPolicyName {
    #[default]
    None,
    #[serde(rename = "basic256sha256")]
    Basic256Sha256,
    #[serde(rename = "aes128_sha256_rsa_oaep")]
    Aes128Sha256RsaOaep,
    #[serde(rename = "aes256_sha256_rsa_pss")]
    Aes256Sha256RsaPss,
}

/// User identity. Only `anonymous` is implemented; `username` parses (and
/// its password may be a `${secret:...}`) but validation rejects it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Identity {
    #[default]
    Anonymous,
    Username {
        username: String,
        password: String,
    },
}

/// One configured node: a bare string (NodeId or `/`-prefixed browse
/// path) or an object with an explicit kind and an optional `name`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum NodeEntry {
    Short(String),
    NodeId {
        node_id: String,
        #[serde(default)]
        name: Option<String>,
    },
    BrowsePath {
        browse_path: String,
        #[serde(default)]
        name: Option<String>,
    },
}

/// Namespace reference inside a node id or browse-path segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NsRef {
    /// No prefix: the configured `namespace_uri` (or 0 when unset).
    Default,
    Index(u16),
    Uri(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathSegment {
    pub ns: NsRef,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum NodeRef {
    Id { ns: NsRef, identifier: Identifier },
    Path(Vec<PathSegment>),
}

/// A validated node: what to subscribe to, and the `name` it is reported as.
#[derive(Debug, Clone)]
pub(crate) struct NodeSpec {
    pub name: String,
    pub node: NodeRef,
}

/// Parse an OPC UA string NodeId: `[ns=<index>;|nsu=<uri>;](i|s|g|b)=<value>`.
pub(crate) fn parse_node_id(s: &str) -> Result<NodeRef, String> {
    let s = s.trim();
    let (ns, rest) = if let Some(rest) = s.strip_prefix("nsu=") {
        let (uri, id) = rest
            .split_once(';')
            .ok_or_else(|| format!("node id '{}': 'nsu=' needs a ';' before the identifier", s))?;
        if uri.is_empty() {
            return Err(format!("node id '{}': empty namespace uri", s));
        }
        (NsRef::Uri(uri.to_string()), id)
    } else if let Some(rest) = s.strip_prefix("ns=") {
        let (idx, id) = rest
            .split_once(';')
            .ok_or_else(|| format!("node id '{}': 'ns=' needs a ';' before the identifier", s))?;
        let idx: u16 = idx
            .parse()
            .map_err(|_| format!("node id '{}': namespace index must be 0-65535", s))?;
        (NsRef::Index(idx), id)
    } else {
        (NsRef::Default, s)
    };
    let identifier = Identifier::from_str(rest).map_err(|_| {
        format!(
            "node id '{}': identifier must be i=<number>, s=<string>, g=<guid> or b=<base64>",
            s
        )
    })?;
    Ok(NodeRef::Id { ns, identifier })
}

/// Parse a browse path relative to the Objects folder:
/// `/Segment/Segment/...`, each segment `[<ns index>:]<browse name>`,
/// following hierarchical references (OPC UA's `/` in Part 4 A.2).
pub(crate) fn parse_browse_path(s: &str) -> Result<NodeRef, String> {
    let body = s.trim().strip_prefix('/').ok_or_else(|| {
        format!(
            "browse path '{}': must start with '/' (relative to Objects)",
            s
        )
    })?;
    let mut segments = Vec::new();
    for raw in body.split('/') {
        if raw.is_empty() {
            return Err(format!("browse path '{}': empty segment", s));
        }
        let segment = match raw.split_once(':') {
            Some((idx, name)) if !idx.is_empty() && idx.bytes().all(|b| b.is_ascii_digit()) => {
                let idx: u16 = idx
                    .parse()
                    .map_err(|_| format!("browse path '{}': namespace index must be 0-65535", s))?;
                if name.is_empty() {
                    return Err(format!("browse path '{}': empty segment", s));
                }
                PathSegment {
                    ns: NsRef::Index(idx),
                    name: name.to_string(),
                }
            }
            _ => PathSegment {
                ns: NsRef::Default,
                name: raw.to_string(),
            },
        };
        segments.push(segment);
    }
    Ok(NodeRef::Path(segments))
}

impl NodeEntry {
    fn to_spec(&self) -> Result<NodeSpec, String> {
        let (raw, node, name) = match self {
            NodeEntry::Short(s) if s.trim_start().starts_with('/') => {
                (s, parse_browse_path(s)?, None)
            }
            NodeEntry::Short(s) => (s, parse_node_id(s)?, None),
            NodeEntry::NodeId { node_id, name } => (node_id, parse_node_id(node_id)?, name.clone()),
            NodeEntry::BrowsePath { browse_path, name } => {
                (browse_path, parse_browse_path(browse_path)?, name.clone())
            }
        };
        let name = name.unwrap_or_else(|| raw.trim().to_string());
        if name.is_empty() {
            return Err("node names must not be empty".to_string());
        }
        Ok(NodeSpec { name, node })
    }
}

fn parse_interval(field: &str, s: &str) -> Result<u64, String> {
    let ms = parse_poll_interval_ms(s).map_err(|e| format!("opcua: invalid {}: {}", field, e))?;
    if ms > MAX_INTERVAL_MS {
        return Err(format!("opcua: {} must be <= 60s (got {}ms)", field, ms));
    }
    Ok(ms)
}

// =============================================================================
// Value mapping
// =============================================================================

pub(crate) fn quality(status: StatusCode) -> &'static str {
    if status.is_good() {
        "good"
    } else if status.is_uncertain() {
        "uncertain"
    } else {
        "bad"
    }
}

fn timestamp_json(ts: &Option<DateTime>) -> Value {
    match ts {
        Some(t) if !t.is_null() => Value::from(t.to_rfc3339()),
        _ => Value::Null,
    }
}

/// OPC UA Variant → JSON. Numbers stay numbers (f32 via its shortest
/// decimal form, so 0.1f32 is 0.1 not 0.10000000149), NaN/Inf become
/// null, byte strings are base64 (OPC UA JSON encoding), arrays map
/// element-wise, and anything structured falls back to its debug text.
pub(crate) fn variant_to_json(v: &Variant) -> Value {
    match v {
        Variant::Empty => Value::Null,
        Variant::Boolean(b) => Value::from(*b),
        Variant::SByte(n) => Value::from(*n),
        Variant::Byte(n) => Value::from(*n),
        Variant::Int16(n) => Value::from(*n),
        Variant::UInt16(n) => Value::from(*n),
        Variant::Int32(n) => Value::from(*n),
        Variant::UInt32(n) => Value::from(*n),
        Variant::Int64(n) => Value::from(*n),
        Variant::UInt64(n) => Value::from(*n),
        Variant::Float(f) => f
            .to_string()
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or(Value::Null),
        Variant::Double(f) => Value::from(*f),
        Variant::String(s) => s.value().clone().map(Value::from).unwrap_or(Value::Null),
        Variant::DateTime(t) => Value::from(t.to_rfc3339()),
        Variant::Guid(g) => Value::from(g.to_string()),
        Variant::StatusCode(c) => Value::from(c.sub_code().name()),
        Variant::ByteString(b) => {
            if b.is_null() {
                Value::Null
            } else {
                Value::from(b.as_base64())
            }
        }
        Variant::XmlElement(x) => Value::from(x.to_string()),
        Variant::QualifiedName(q) => Value::from(q.name.value().clone().unwrap_or_default()),
        Variant::LocalizedText(t) => t
            .text
            .value()
            .clone()
            .map(Value::from)
            .unwrap_or(Value::Null),
        Variant::NodeId(n) => Value::from(n.to_string()),
        Variant::ExpandedNodeId(n) => Value::from(n.to_string()),
        Variant::Variant(inner) => variant_to_json(inner),
        Variant::DataValue(dv) => dv
            .value
            .as_ref()
            .map(variant_to_json)
            .unwrap_or(Value::Null),
        Variant::Array(a) => Value::Array(a.values.iter().map(variant_to_json).collect()),
        other => Value::from(format!("{:?}", other)),
    }
}

/// Identity of a monitored node as reported in records.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedNode {
    pub name: String,
    /// `node_id` without the namespace index (`s=hbw.state`): together
    /// with `namespace_uri` it is stable across server restarts.
    pub identifier: String,
    pub namespace_uri: Option<String>,
}

/// Wrap a value in the Messages shape every bridged source emits.
fn wrap_message(value: Value, key: Option<&str>, source_topic: &str) -> Value {
    let now = chrono::Utc::now().to_rfc3339();
    serde_json::json!({
        "key_text":     key,
        "key_json":     Value::Null,
        "value_text":   serde_json::to_string(&value).unwrap_or_default(),
        "value_json":   value,
        "headers":      serde_json::json!({}),
        "offset_id":    0,
        "created_at":   now,
        "source_topic": source_topic,
    })
}

/// One DataChange notification → one record.
pub(crate) fn data_record(node: &ResolvedNode, dv: &DataValue, endpoint: &str) -> Value {
    let status = dv.status.unwrap_or(StatusCode::Good);
    let value = serde_json::json!({
        "kind":          "data",
        "name":          node.name,
        "node_id":       node.identifier,
        "namespace_uri": node.namespace_uri,
        "value":         dv.value.as_ref().map(variant_to_json).unwrap_or(Value::Null),
        "status":        quality(status),
        "status_code":   status.sub_code().name(),
        "source_ts":     timestamp_json(&dv.source_timestamp),
        "server_ts":     timestamp_json(&dv.server_timestamp),
    });
    wrap_message(value, Some(&node.name), endpoint)
}

/// A link-state record (`up` once subscribed, `lost` when the session drops).
pub(crate) fn link_record(up: bool, reason: Option<String>, endpoint: &str) -> Value {
    let value = serde_json::json!({
        "kind":     "link",
        "link":     if up { "up" } else { "lost" },
        "endpoint": endpoint,
        "ts":       chrono::Utc::now().to_rfc3339(),
        "reason":   reason,
    });
    wrap_message(value, None, endpoint)
}

// =============================================================================
// Source
// =============================================================================

/// OPC UA subscription source.
#[derive(Debug)]
pub struct OpcuaSource {
    config: OpcuaSourceConfig,
    nodes: Vec<NodeSpec>,
    publishing_ms: u64,
    sampling_ms: u64,
    reconnect_ms: u64,
    consecutive_failures: Arc<AtomicU32>,
}

impl OpcuaSource {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: OpcuaSourceConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("opcua: invalid config: {}", e))?;

        let endpoint = config.endpoint.trim();
        if !endpoint.starts_with("opc.tcp://") || endpoint.len() <= "opc.tcp://".len() {
            return Err(format!(
                "opcua: endpoint must be opc.tcp://host:port, got '{}'",
                config.endpoint
            ));
        }
        if config.security_mode != SecurityMode::None
            || config.security_policy != SecurityPolicyName::None
        {
            return Err(
                "opcua: only security_mode 'none' / security_policy 'none' is supported so far"
                    .to_string(),
            );
        }
        if let Identity::Username { .. } = config.identity {
            return Err(
                "opcua: identity 'username' is not supported yet (requires a secured channel); use 'anonymous'"
                    .to_string(),
            );
        }
        if let Some(ns) = &config.namespace_uri {
            if ns.trim().is_empty() {
                return Err("opcua: namespace_uri must not be empty".to_string());
            }
        }
        if config.nodes.is_empty() {
            return Err("opcua: at least one entry in 'nodes' is required".to_string());
        }
        let mut nodes = Vec::with_capacity(config.nodes.len());
        let mut seen = std::collections::HashSet::new();
        for entry in &config.nodes {
            let spec = entry.to_spec().map_err(|e| format!("opcua: {}", e))?;
            if !seen.insert(spec.name.clone()) {
                return Err(format!("opcua: duplicate node name '{}'", spec.name));
            }
            nodes.push(spec);
        }

        let publishing_ms = parse_interval("publishing_interval", &config.publishing_interval)?;
        let sampling_ms = parse_interval("sampling_interval", &config.sampling_interval)?;
        let reconnect_ms = parse_interval("reconnect_interval", &config.reconnect_interval)?;
        if config.queue_size == 0 {
            return Err("opcua: queue_size must be >= 1".to_string());
        }
        if config.connect_timeout_ms == 0 || config.request_timeout_ms == 0 {
            return Err("opcua: connect_timeout_ms and request_timeout_ms must be > 0".to_string());
        }

        Ok(Self {
            config,
            nodes,
            publishing_ms,
            sampling_ms,
            reconnect_ms,
            consecutive_failures: Arc::new(AtomicU32::new(0)),
        })
    }
}

/// Why a session attempt ended.
enum SessionEnd {
    /// Connect failure or connection loss: retry after `reconnect_interval`.
    Transient(String),
    /// Configuration the server rejects: fail the pipeline.
    Permanent(String),
}

/// What the event-loop task reports about the session.
enum LinkEvent {
    Up,
    Down(String),
}

/// Aborts the event-loop task when the stream is dropped (pipeline stop,
/// reopen), which closes the TCP connection.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A live session with its subscription created.
struct Live {
    _session: Arc<Session>,
    _event_loop: AbortOnDrop,
    events: mpsc::UnboundedReceiver<LinkEvent>,
    data: mpsc::UnboundedReceiver<(NodeId, DataValue)>,
    nodes: HashMap<NodeId, ResolvedNode>,
}

fn resolve_ns(ns: &NsRef, default_ns: u16, uris: &HashMap<String, u16>) -> Result<u16, String> {
    match ns {
        NsRef::Default => Ok(default_ns),
        NsRef::Index(i) => Ok(*i),
        NsRef::Uri(u) => uris
            .get(u)
            .copied()
            .ok_or_else(|| format!("namespace '{}' not in the server's namespace array", u)),
    }
}

/// Translate configured nodes into server NodeIds. Node ids map directly;
/// browse paths go through one TranslateBrowsePathsToNodeIds call.
async fn resolve_nodes(
    session: &Session,
    specs: &[NodeSpec],
    default_ns: u16,
    uris: &HashMap<String, u16>,
) -> Result<Vec<(String, NodeId)>, SessionEnd> {
    let mut resolved: Vec<Option<NodeId>> = vec![None; specs.len()];
    let mut paths = Vec::new();
    let mut path_idx = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        match &spec.node {
            NodeRef::Id { ns, identifier } => {
                let ns = resolve_ns(ns, default_ns, uris)
                    .map_err(|e| SessionEnd::Permanent(format!("node '{}': {}", spec.name, e)))?;
                resolved[i] = Some(NodeId::new(ns, identifier.clone()));
            }
            NodeRef::Path(segments) => {
                let mut elements = Vec::with_capacity(segments.len());
                for seg in segments {
                    let ns = resolve_ns(&seg.ns, default_ns, uris).map_err(|e| {
                        SessionEnd::Permanent(format!("node '{}': {}", spec.name, e))
                    })?;
                    elements.push(RelativePathElement {
                        reference_type_id: ReferenceTypeId::HierarchicalReferences.into(),
                        is_inverse: false,
                        include_subtypes: true,
                        target_name: QualifiedName::new(ns, seg.name.as_str()),
                    });
                }
                paths.push(BrowsePath {
                    starting_node: ObjectId::ObjectsFolder.into(),
                    relative_path: RelativePath {
                        elements: Some(elements),
                    },
                });
                path_idx.push(i);
            }
        }
    }
    if !paths.is_empty() {
        let results = session
            .translate_browse_paths_to_node_ids(&paths)
            .await
            .map_err(|e| SessionEnd::Transient(format!("translate browse paths: {}", e)))?;
        if results.len() != paths.len() {
            return Err(SessionEnd::Transient(format!(
                "translate browse paths: expected {} results, got {}",
                paths.len(),
                results.len()
            )));
        }
        for (r, &i) in results.iter().zip(&path_idx) {
            let target = r
                .targets
                .as_ref()
                .and_then(|t| t.first())
                .filter(|_| r.status_code.is_good());
            match target {
                Some(t) => resolved[i] = Some(t.target_id.node_id.clone()),
                None => {
                    return Err(SessionEnd::Permanent(format!(
                        "node '{}': browse path not found ({})",
                        specs[i].name, r.status_code
                    )))
                }
            }
        }
    }
    Ok(specs
        .iter()
        .zip(resolved)
        .map(|(s, n)| (s.name.clone(), n.expect("every node resolved")))
        .collect())
}

impl OpcuaSource {
    async fn connect(&self) -> Result<Live, SessionEnd> {
        let cfg = &self.config;
        let endpoint_url = cfg.endpoint.trim();
        let pki_dir = cfg
            .pki_dir
            .clone()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("pg_streaming_opcua").join("pki"));
        let mut client = ClientBuilder::new()
            .application_name("pg_streaming")
            .application_uri("urn:pg_streaming:opcua")
            .product_uri("urn:pg_streaming")
            .pki_dir(pki_dir)
            .create_sample_keypair(false)
            .trust_server_certs(false)
            .session_retry_limit(0)
            .request_timeout(Duration::from_millis(cfg.request_timeout_ms))
            .client()
            .map_err(|e| SessionEnd::Permanent(format!("client config: {}", e.join("; "))))?;

        let endpoint: EndpointDescription = (
            endpoint_url,
            SecurityPolicy::None.to_str(),
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();
        let (session, event_loop) = client
            .connect_to_endpoint_directly(endpoint, IdentityToken::Anonymous)
            .map_err(|e| SessionEnd::Permanent(format!("{}: {}", endpoint_url, e)))?;

        // Drive the session on its own task and report link changes. With
        // retries disabled the loop ends on the first failure or loss.
        let (ev_tx, mut events) = mpsc::unbounded_channel::<LinkEvent>();
        let task = tokio::spawn(async move {
            let stream = event_loop.enter();
            tokio::pin!(stream);
            while let Some(event) = stream.next().await {
                match event {
                    Ok(SessionPollResult::Reconnected(_)) => {
                        let _ = ev_tx.send(LinkEvent::Up);
                    }
                    Ok(SessionPollResult::ConnectionLost(code))
                    | Ok(SessionPollResult::ReconnectFailed(code)) => {
                        let _ = ev_tx.send(LinkEvent::Down(code.to_string()));
                        return;
                    }
                    Ok(_) => {}
                    Err(code) => {
                        let _ = ev_tx.send(LinkEvent::Down(code.to_string()));
                        return;
                    }
                }
            }
            let _ = ev_tx.send(LinkEvent::Down("session ended".to_string()));
        });
        let event_loop = AbortOnDrop(task);

        let connected =
            tokio::time::timeout(Duration::from_millis(cfg.connect_timeout_ms), events.recv())
                .await;
        match connected {
            Ok(Some(LinkEvent::Up)) => {}
            Ok(Some(LinkEvent::Down(reason))) => {
                return Err(SessionEnd::Transient(format!(
                    "connect {}: {}",
                    endpoint_url, reason
                )))
            }
            Ok(None) => {
                return Err(SessionEnd::Transient(format!(
                    "connect {}: session ended",
                    endpoint_url
                )))
            }
            Err(_) => {
                return Err(SessionEnd::Transient(format!(
                    "connect {}: timed out after {}ms",
                    endpoint_url, cfg.connect_timeout_ms
                )))
            }
        }

        let ns_map = session
            .read_namespace_array()
            .await
            .map_err(|e| SessionEnd::Transient(format!("read namespace array: {}", e)))?;
        let uris: HashMap<String, u16> = ns_map
            .known_namespaces()
            .iter()
            .map(|(u, i)| (u.clone(), *i))
            .collect();
        let by_index: HashMap<u16, String> = uris.iter().map(|(u, i)| (*i, u.clone())).collect();
        let default_ns = match &cfg.namespace_uri {
            Some(uri) => *uris.get(uri).ok_or_else(|| {
                SessionEnd::Permanent(format!(
                    "namespace_uri '{}' not in the server's namespace array",
                    uri
                ))
            })?,
            None => 0,
        };

        let resolved = resolve_nodes(&session, &self.nodes, default_ns, &uris).await?;

        let (data_tx, data) = mpsc::unbounded_channel::<(NodeId, DataValue)>();
        let sub = session
            .create_subscription(
                Duration::from_millis(self.publishing_ms),
                60,
                20,
                0,
                0,
                true,
                DataChangeCallback::new(move |dv, item: &MonitoredItem| {
                    let _ = data_tx.send((item.item_to_monitor().node_id.clone(), dv));
                }),
            )
            .await
            .map_err(|e| SessionEnd::Transient(format!("create subscription: {}", e)))?;

        let items: Vec<MonitoredItemCreateRequest> = resolved
            .iter()
            .map(|(_, node_id)| {
                MonitoredItemCreateRequest::new(
                    ReadValueId::from(node_id.clone()),
                    MonitoringMode::Reporting,
                    MonitoringParameters {
                        sampling_interval: self.sampling_ms as f64,
                        queue_size: cfg.queue_size,
                        discard_oldest: true,
                        ..Default::default()
                    },
                )
            })
            .collect();
        let created = session
            .create_monitored_items(sub, TimestampsToReturn::Both, items)
            .await
            .map_err(|e| SessionEnd::Transient(format!("create monitored items: {}", e)))?;
        for ((name, node_id), c) in resolved.iter().zip(&created) {
            if !c.result.status_code.is_good() {
                return Err(SessionEnd::Permanent(format!(
                    "node '{}' ({}): server rejected the monitored item: {}",
                    name, node_id, c.result.status_code
                )));
            }
        }

        let nodes = resolved
            .into_iter()
            .map(|(name, node_id)| {
                let resolved = ResolvedNode {
                    name,
                    identifier: node_id.identifier.to_string(),
                    namespace_uri: by_index.get(&node_id.namespace).cloned(),
                };
                (node_id, resolved)
            })
            .collect();

        Ok(Live {
            _session: session,
            _event_loop: event_loop,
            events,
            data,
            nodes,
        })
    }
}

#[async_trait]
impl AsyncSource for OpcuaSource {
    async fn open(
        &mut self,
        _last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
        let source = OpcuaSource {
            config: self.config.clone(),
            nodes: self.nodes.clone(),
            publishing_ms: self.publishing_ms,
            sampling_ms: self.sampling_ms,
            reconnect_ms: self.reconnect_ms,
            consecutive_failures: Arc::clone(&self.consecutive_failures),
        };
        let endpoint = self.config.endpoint.trim().to_string();
        let emit_link = self.config.emit_link_records;
        let max_failures = self.config.max_consecutive_failures;
        let failures = Arc::clone(&self.consecutive_failures);

        let stream = async_stream::stream! {
            // Connect inside the stream: transient errors must not fail the
            // pipeline, they end the stream and the bridge reopens it.
            let mut live = match source.connect().await {
                Ok(live) => live,
                Err(SessionEnd::Permanent(e)) => {
                    yield Err(format!("opcua: {}", e));
                    return;
                }
                Err(SessionEnd::Transient(e)) => {
                    let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if max_failures != 0 && n >= max_failures {
                        yield Err(format!("opcua: {} consecutive failures, last: {}", n, e));
                    }
                    return;
                }
            };
            failures.store(0, Ordering::SeqCst);
            if emit_link {
                yield Ok(SourceItem::one_shot(link_record(true, None, &endpoint)));
            }

            loop {
                tokio::select! {
                    // Data first: notifications that arrived before a loss
                    // are still delivered.
                    biased;
                    Some((node_id, dv)) = live.data.recv() => {
                        if let Some(node) = live.nodes.get(&node_id) {
                            yield Ok(SourceItem::one_shot(data_record(node, &dv, &endpoint)));
                        }
                    }
                    event = live.events.recv() => {
                        match event {
                            Some(LinkEvent::Up) => continue,
                            Some(LinkEvent::Down(reason)) => {
                                if emit_link {
                                    yield Ok(SourceItem::one_shot(link_record(false, Some(reason), &endpoint)));
                                }
                            }
                            None => {
                                if emit_link {
                                    yield Ok(SourceItem::one_shot(link_record(false, Some("session ended".to_string()), &endpoint)));
                                }
                            }
                        }
                        // The session is gone: count it, end the stream, reopen.
                        failures.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn is_continuous(&self) -> bool {
        true
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.reconnect_ms)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::{ByteString, LocalizedText, UAString};
    use serde_json::json;

    fn base() -> Value {
        json!({
            "endpoint": "opc.tcp://127.0.0.1:4840",
            "namespace_uri": "urn:eidos:lab:training:training_factory",
            "nodes": ["s=hbw.state"]
        })
    }

    fn with(patch: Value) -> Value {
        let mut v = base();
        for (k, val) in patch.as_object().unwrap() {
            v[k] = val.clone();
        }
        v
    }

    fn err(cfg: Value) -> String {
        OpcuaSource::from_config(&cfg).unwrap_err()
    }

    // -------------------------------------------------------------------------
    // Config validation
    // -------------------------------------------------------------------------

    #[test]
    fn minimal_config_gets_defaults() {
        let src = OpcuaSource::from_config(&base()).unwrap();
        assert_eq!(src.publishing_ms, 1000);
        assert_eq!(src.sampling_ms, 250);
        assert_eq!(src.reconnect_ms, 5000);
        assert_eq!(src.config.queue_size, 10);
        assert_eq!(src.config.max_consecutive_failures, 5);
        assert!(src.config.emit_link_records);
        assert_eq!(src.config.security_mode, SecurityMode::None);
        assert!(matches!(src.config.identity, Identity::Anonymous));
        assert!(src.is_continuous());
        assert_eq!(src.poll_interval(), Duration::from_secs(5));
    }

    #[test]
    fn intervals_are_parsed() {
        let src = OpcuaSource::from_config(&with(json!({
            "publishing_interval": "100ms",
            "sampling_interval": "50ms",
            "reconnect_interval": "2s"
        })))
        .unwrap();
        assert_eq!(src.publishing_ms, 100);
        assert_eq!(src.sampling_ms, 50);
        assert_eq!(src.poll_interval(), Duration::from_secs(2));
    }

    #[test]
    fn rejects_bad_endpoint() {
        assert!(err(with(json!({"endpoint": "http://h:4840"}))).contains("opc.tcp://"));
        assert!(err(with(json!({"endpoint": "opc.tcp://"}))).contains("opc.tcp://"));
    }

    #[test]
    fn rejects_missing_or_empty_nodes() {
        let mut cfg = base();
        cfg.as_object_mut().unwrap().remove("nodes");
        assert!(err(cfg).contains("nodes"));
        assert!(err(with(json!({"nodes": []}))).contains("at least one"));
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(err(with(json!({"polling": "1s"}))).contains("unknown field"));
    }

    #[test]
    fn rejects_security_other_than_none() {
        assert!(err(with(json!({"security_mode": "sign_and_encrypt"}))).contains("security_mode"));
        assert!(err(with(json!({"security_policy": "basic256sha256"}))).contains("security_mode"));
        assert!(
            err(with(json!({"security_policy": "aes256_sha256_rsa_pss"})))
                .contains("security_mode")
        );
        assert!(err(with(json!({"security_mode": "bogus"}))).contains("invalid config"));
    }

    #[test]
    fn username_identity_parses_but_is_rejected() {
        let e = err(with(json!({
            "identity": {"type": "username", "username": "u", "password": "p"}
        })));
        assert!(e.contains("not supported yet"), "{e}");
        OpcuaSource::from_config(&with(json!({"identity": {"type": "anonymous"}}))).unwrap();
    }

    #[test]
    fn rejects_bad_intervals_and_limits() {
        assert!(err(with(json!({"publishing_interval": "0ms"}))).contains("publishing_interval"));
        assert!(err(with(json!({"sampling_interval": "fast"}))).contains("sampling_interval"));
        assert!(err(with(json!({"reconnect_interval": "2m"}))).contains("<= 60s"));
        assert!(err(with(json!({"queue_size": 0}))).contains("queue_size"));
        assert!(err(with(json!({"connect_timeout_ms": 0}))).contains("connect_timeout_ms"));
        assert!(err(with(json!({"namespace_uri": " "}))).contains("namespace_uri"));
    }

    #[test]
    fn max_consecutive_failures_zero_means_forever() {
        let src = OpcuaSource::from_config(&with(json!({"max_consecutive_failures": 0}))).unwrap();
        assert_eq!(src.config.max_consecutive_failures, 0);
    }

    #[test]
    fn rejects_bad_node_ids() {
        assert!(err(with(json!({"nodes": ["hbw.state"]}))).contains("identifier must be"));
        assert!(err(with(json!({"nodes": ["ns=x;s=a"]}))).contains("namespace index"));
        assert!(err(with(json!({"nodes": ["ns=2"]}))).contains("';'"));
        assert!(err(with(json!({"nodes": ["nsu=;s=a"]}))).contains("empty namespace uri"));
        assert!(err(with(json!({"nodes": ["i=abc"]}))).contains("identifier must be"));
    }

    #[test]
    fn rejects_bad_browse_paths() {
        assert!(err(with(json!({"nodes": ["//a"]}))).contains("empty segment"));
        assert!(err(with(json!({"nodes": ["/a/"]}))).contains("empty segment"));
        assert!(
            err(with(json!({"nodes": [{"browse_path": "a/b"}]}))).contains("must start with '/'")
        );
        assert!(err(with(json!({"nodes": ["/2:"]}))).contains("empty segment"));
    }

    #[test]
    fn rejects_duplicate_names() {
        let e = err(with(json!({"nodes": ["s=a", "s=a"]})));
        assert!(e.contains("duplicate node name 's=a'"), "{e}");
        let e = err(with(json!({"nodes": [
            {"node_id": "s=a", "name": "x"},
            {"browse_path": "/f/b", "name": "x"}
        ]})));
        assert!(e.contains("duplicate"), "{e}");
    }

    #[test]
    fn node_entries_and_names() {
        let src = OpcuaSource::from_config(&with(json!({"nodes": [
            "s=hbw.state",
            " /training_factory/hbw/state ",
            {"node_id": "ns=0;i=2258", "name": "server_time"},
            {"browse_path": "/0:Server/0:ServerStatus"}
        ]})))
        .unwrap();
        let names: Vec<&str> = src.nodes.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "s=hbw.state",
                "/training_factory/hbw/state",
                "server_time",
                "/0:Server/0:ServerStatus"
            ]
        );
        assert!(matches!(src.nodes[1].node, NodeRef::Path(_)));
        assert!(matches!(
            src.nodes[2].node,
            NodeRef::Id {
                ns: NsRef::Index(0),
                ..
            }
        ));
    }

    #[test]
    fn parse_node_id_forms() {
        assert_eq!(
            parse_node_id("s=hbw.state").unwrap(),
            NodeRef::Id {
                ns: NsRef::Default,
                identifier: Identifier::from("hbw.state")
            }
        );
        assert_eq!(
            parse_node_id("ns=3;i=1001").unwrap(),
            NodeRef::Id {
                ns: NsRef::Index(3),
                identifier: Identifier::Numeric(1001)
            }
        );
        // The uri may contain ':' — only the first ';' separates it.
        assert_eq!(
            parse_node_id("nsu=urn:a:b;s=x;y").unwrap(),
            NodeRef::Id {
                ns: NsRef::Uri("urn:a:b".into()),
                identifier: Identifier::from("x;y")
            }
        );
    }

    #[test]
    fn parse_browse_path_segments() {
        let NodeRef::Path(segs) = parse_browse_path("/training_factory/2:hbw/urn:x").unwrap()
        else {
            panic!("expected path");
        };
        assert_eq!(
            segs,
            vec![
                PathSegment {
                    ns: NsRef::Default,
                    name: "training_factory".into()
                },
                PathSegment {
                    ns: NsRef::Index(2),
                    name: "hbw".into()
                },
                // A non-numeric prefix is part of the name.
                PathSegment {
                    ns: NsRef::Default,
                    name: "urn:x".into()
                },
            ]
        );
    }

    #[test]
    fn resolve_ns_lookup() {
        let uris: HashMap<String, u16> = [("urn:a".to_string(), 4u16)].into_iter().collect();
        assert_eq!(resolve_ns(&NsRef::Default, 2, &uris).unwrap(), 2);
        assert_eq!(resolve_ns(&NsRef::Index(7), 2, &uris).unwrap(), 7);
        assert_eq!(
            resolve_ns(&NsRef::Uri("urn:a".into()), 2, &uris).unwrap(),
            4
        );
        assert!(resolve_ns(&NsRef::Uri("urn:b".into()), 2, &uris).is_err());
    }

    #[test]
    fn validate_dsl_shape() {
        // The DSL wraps the config as {"opcua": {...}}.
        let def: crate::dsl::types::PipelineDefinition = serde_json::from_value(json!({
            "input": {"opcua": base()},
            "pipeline": {"processors": []},
            "output": {"drop": {}}
        }))
        .unwrap();
        assert!(matches!(
            def.input,
            crate::dsl::types::InputConfig::Opcua(_)
        ));
    }

    // -------------------------------------------------------------------------
    // Record mapping
    // -------------------------------------------------------------------------

    fn node() -> ResolvedNode {
        ResolvedNode {
            name: "hbw.state".into(),
            identifier: "s=hbw.state".into(),
            namespace_uri: Some("urn:eidos:lab:training:training_factory".into()),
        }
    }

    #[test]
    fn data_record_shape() {
        let ts = DateTime::from(
            chrono::DateTime::parse_from_rfc3339("2026-10-30T10:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        );
        let dv = DataValue {
            value: Some(Variant::Int32(6)),
            status: Some(StatusCode::Good),
            source_timestamp: Some(ts),
            server_timestamp: None,
            ..Default::default()
        };
        let r = data_record(&node(), &dv, "opc.tcp://127.0.0.1:4840");
        let v = &r["value_json"];
        assert_eq!(v["kind"], "data");
        assert_eq!(v["name"], "hbw.state");
        assert_eq!(v["node_id"], "s=hbw.state");
        assert_eq!(
            v["namespace_uri"],
            "urn:eidos:lab:training:training_factory"
        );
        assert_eq!(v["value"], 6);
        assert_eq!(v["status"], "good");
        assert_eq!(v["status_code"], "Good");
        assert!(v["source_ts"]
            .as_str()
            .unwrap()
            .starts_with("2026-10-30T10:00:00"));
        assert_eq!(v["server_ts"], Value::Null);
        assert_eq!(r["key_text"], "hbw.state");
        assert_eq!(r["source_topic"], "opc.tcp://127.0.0.1:4840");
        assert_eq!(r["offset_id"], 0);
        let text: Value = serde_json::from_str(r["value_text"].as_str().unwrap()).unwrap();
        assert_eq!(&text, v);
    }

    #[test]
    fn data_record_missing_status_is_good_and_bad_is_bad() {
        let dv = DataValue {
            value: Some(Variant::Double(1.5)),
            ..Default::default()
        };
        assert_eq!(
            data_record(&node(), &dv, "e")["value_json"]["status"],
            "good"
        );

        let dv = DataValue {
            value: None,
            status: Some(StatusCode::BadCommunicationError),
            ..Default::default()
        };
        let v = &data_record(&node(), &dv, "e")["value_json"];
        assert_eq!(v["status"], "bad");
        assert_eq!(v["status_code"], "BadCommunicationError");
        assert_eq!(v["value"], Value::Null);

        let dv = DataValue {
            value: Some(Variant::Boolean(true)),
            status: Some(StatusCode::UncertainLastUsableValue),
            ..Default::default()
        };
        assert_eq!(
            data_record(&node(), &dv, "e")["value_json"]["status"],
            "uncertain"
        );
    }

    #[test]
    fn link_record_shape() {
        let up = link_record(true, None, "opc.tcp://h:4840");
        assert_eq!(up["value_json"]["kind"], "link");
        assert_eq!(up["value_json"]["link"], "up");
        assert_eq!(up["value_json"]["endpoint"], "opc.tcp://h:4840");
        assert_eq!(up["value_json"]["reason"], Value::Null);
        assert_eq!(up["key_text"], Value::Null);
        let lost = link_record(
            false,
            Some("BadConnectionClosed".into()),
            "opc.tcp://h:4840",
        );
        assert_eq!(lost["value_json"]["link"], "lost");
        assert_eq!(lost["value_json"]["reason"], "BadConnectionClosed");
    }

    #[test]
    fn variant_mapping() {
        assert_eq!(variant_to_json(&Variant::Empty), Value::Null);
        assert_eq!(variant_to_json(&Variant::Boolean(true)), json!(true));
        assert_eq!(variant_to_json(&Variant::SByte(-3)), json!(-3));
        assert_eq!(variant_to_json(&Variant::UInt16(7)), json!(7));
        assert_eq!(
            variant_to_json(&Variant::Int64(-9_000_000_000)),
            json!(-9_000_000_000i64)
        );
        assert_eq!(variant_to_json(&Variant::UInt64(u64::MAX)), json!(u64::MAX));
        assert_eq!(variant_to_json(&Variant::Float(0.1)), json!(0.1));
        assert_eq!(variant_to_json(&Variant::Double(21.5)), json!(21.5));
        assert_eq!(variant_to_json(&Variant::Double(f64::NAN)), Value::Null);
        assert_eq!(variant_to_json(&Variant::Float(f32::INFINITY)), Value::Null);
        assert_eq!(
            variant_to_json(&Variant::String(UAString::from("wp-1"))),
            json!("wp-1")
        );
        assert_eq!(
            variant_to_json(&Variant::String(UAString::null())),
            Value::Null
        );
        assert_eq!(
            variant_to_json(&Variant::ByteString(ByteString::from(vec![1u8, 2, 3]))),
            json!("AQID")
        );
        assert_eq!(
            variant_to_json(&Variant::StatusCode(StatusCode::BadNodeIdUnknown)),
            json!("BadNodeIdUnknown")
        );
        assert_eq!(
            variant_to_json(&Variant::LocalizedText(Box::new(LocalizedText::new(
                "en", "Idle"
            )))),
            json!("Idle")
        );
        assert_eq!(
            variant_to_json(&Variant::NodeId(Box::new(NodeId::new(2, "hbw")))),
            json!("ns=2;s=hbw")
        );
        assert_eq!(
            variant_to_json(&Variant::from(vec![1i32, 2, 3])),
            json!([1, 2, 3])
        );
        let dt = variant_to_json(&Variant::DateTime(Box::new(DateTime::now())));
        assert!(dt.as_str().unwrap().contains('T'));
    }

    #[test]
    fn quality_buckets() {
        assert_eq!(quality(StatusCode::Good), "good");
        assert_eq!(quality(StatusCode::UncertainInitialValue), "uncertain");
        assert_eq!(quality(StatusCode::BadOutOfService), "bad");
    }

    // -------------------------------------------------------------------------
    // Stream behaviour without a server
    // -------------------------------------------------------------------------

    /// Nothing listens: the first attempt ends the stream cleanly (transient),
    /// and with max_consecutive_failures = 1 the next yields the error.
    #[tokio::test]
    async fn connect_failure_ends_stream_then_fails_at_cap() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut src = OpcuaSource::from_config(&with(json!({
            "endpoint": format!("opc.tcp://127.0.0.1:{}", port),
            "max_consecutive_failures": 2,
            "connect_timeout_ms": 2000
        })))
        .unwrap();

        let items: Vec<_> = src.open(Cursor::None).await.unwrap().collect().await;
        assert!(
            items.is_empty(),
            "first failure ends the stream cleanly: {:?}",
            items.len()
        );

        let items: Vec<_> = src.open(Cursor::None).await.unwrap().collect().await;
        assert_eq!(items.len(), 1);
        let e = items.into_iter().next().unwrap().unwrap_err();
        assert!(e.contains("2 consecutive failures"), "{e}");
    }
}
