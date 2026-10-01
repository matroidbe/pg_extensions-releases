//! Modbus TCP source — polls a PLC/device on a fixed interval and emits
//! one record per poll cycle (a coherent snapshot of all configured tags).
//!
//! Cursor semantics: `Cursor::None` — live telemetry has no replayable
//! offset, so there is no resume across restarts (at-least-once within a
//! running pipeline).
//!
//! Error policy: the TCP connection is established inside the stream
//! body, never in `open()` — an `Err` from `open()` or the stream is
//! fatal to the pipeline, so transient failures instead end the stream
//! cleanly and rely on `is_continuous()` + `poll_interval()` for the
//! bridge to reopen (reconnect-with-delay). Only after
//! `max_consecutive_failures` does the source yield an `Err` and fail
//! the pipeline.
//!
//! DSL configuration: see `design/pg_streaming/connectors.md`.

use crate::connector::input::table::parse_poll_interval_ms;
use crate::connector::sdk::{AsyncSource, Cursor, SourceItem};
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_modbus::client::{Context, Reader};
use tokio_modbus::Slave;

/// Modbus register read requests are capped at 125 registers / 2000 bits
/// by the protocol.
const MAX_REGISTERS_PER_READ: u32 = 125;
const MAX_BITS_PER_READ: u32 = 2000;

/// Poll intervals above this bound the worst-case pipeline stop latency
/// (the in-stream sleep is not cancellable by bridge teardown).
const MAX_POLL_MS: u64 = 60_000;

#[derive(Debug, Clone, Deserialize)]
pub struct ModbusSourceConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_unit")]
    pub unit: u8,
    #[serde(default = "default_poll")]
    pub poll: String,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
    pub reads: Vec<ReadSpec>,
}

pub(crate) fn default_port() -> u16 {
    502
}
pub(crate) fn default_unit() -> u8 {
    1
}
fn default_poll() -> String {
    "1s".to_string()
}
pub(crate) fn default_connect_timeout_ms() -> u64 {
    5000
}
pub(crate) fn default_read_timeout_ms() -> u64 {
    3000
}
fn default_max_consecutive_failures() -> u32 {
    5
}
fn default_count() -> u16 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadSpec {
    /// Tag name — key in the emitted snapshot. Unique, not "ts".
    pub name: String,
    pub kind: RegisterKind,
    /// 0-based register/coil address.
    pub address: u16,
    /// Only valid for register kinds (holding/input); coils and discrete
    /// inputs always decode as booleans.
    #[serde(default)]
    pub data_type: Option<DataType>,
    #[serde(default)]
    pub word_order: Option<WordOrder>,
    /// Number of values (not registers) to read; > 1 emits a JSON array.
    #[serde(default = "default_count")]
    pub count: u16,
}

impl ReadSpec {
    pub fn data_type(&self) -> DataType {
        self.data_type.unwrap_or_default()
    }

    pub fn word_order(&self) -> WordOrder {
        self.word_order.unwrap_or_default()
    }

    fn is_bit_kind(&self) -> bool {
        matches!(self.kind, RegisterKind::Coil | RegisterKind::Discrete)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegisterKind {
    Coil,
    Discrete,
    Holding,
    Input,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    U16,
    I16,
    U32,
    I32,
    F32,
}

impl DataType {
    /// Number of 16-bit registers holding one value of this type.
    pub fn words(self) -> u16 {
        match self {
            DataType::U16 | DataType::I16 => 1,
            DataType::U32 | DataType::I32 | DataType::F32 => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WordOrder {
    /// ABCD — high word first (Modbus default).
    #[default]
    Big,
    /// CDAB — low word first.
    Little,
}

/// Validate the parts of a Modbus config shared between source and sink:
/// host and the per-entry tag/field names.
pub(crate) fn validate_names<'a>(
    connector: &str,
    names: impl Iterator<Item = &'a str>,
) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for name in names {
        if name.is_empty() {
            return Err(format!("{}: read/write names must not be empty", connector));
        }
        if name == "ts" {
            return Err(format!(
                "{}: the name 'ts' is reserved for the snapshot timestamp",
                connector
            ));
        }
        if !seen.insert(name) {
            return Err(format!("{}: duplicate name '{}'", connector, name));
        }
    }
    Ok(())
}

/// Decode one value from `words` (already sliced to the right length).
pub fn decode_value(words: &[u16], data_type: DataType, word_order: WordOrder) -> Value {
    match data_type {
        DataType::U16 => Value::from(words[0]),
        DataType::I16 => Value::from(words[0] as i16),
        DataType::U32 | DataType::I32 | DataType::F32 => {
            let (hi, lo) = match word_order {
                WordOrder::Big => (words[0], words[1]),
                WordOrder::Little => (words[1], words[0]),
            };
            let bits = ((hi as u32) << 16) | (lo as u32);
            match data_type {
                DataType::U32 => Value::from(bits),
                DataType::I32 => Value::from(bits as i32),
                DataType::F32 => Value::from(f32::from_bits(bits)),
                _ => unreachable!(),
            }
        }
    }
}

/// Encode one value into registers — the exact inverse of [`decode_value`].
/// Used by the Modbus sink.
pub fn encode_value(
    value: &Value,
    data_type: DataType,
    word_order: WordOrder,
) -> Result<Vec<u16>, String> {
    fn int_from(value: &Value) -> Result<i64, String> {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
            .ok_or_else(|| format!("expected an integer, got {}", value))
    }

    let bits: u32 = match data_type {
        DataType::U16 => {
            let n = int_from(value)?;
            let w = u16::try_from(n).map_err(|_| format!("{} out of range for u16", n))?;
            return Ok(vec![w]);
        }
        DataType::I16 => {
            let n = int_from(value)?;
            let w = i16::try_from(n).map_err(|_| format!("{} out of range for i16", n))?;
            return Ok(vec![w as u16]);
        }
        DataType::U32 => {
            let n = int_from(value)?;
            u32::try_from(n).map_err(|_| format!("{} out of range for u32", n))?
        }
        DataType::I32 => {
            let n = int_from(value)?;
            i32::try_from(n).map_err(|_| format!("{} out of range for i32", n))? as u32
        }
        DataType::F32 => {
            let f = value
                .as_f64()
                .ok_or_else(|| format!("expected a number, got {}", value))?;
            (f as f32).to_bits()
        }
    };
    let hi = (bits >> 16) as u16;
    let lo = (bits & 0xFFFF) as u16;
    Ok(match word_order {
        WordOrder::Big => vec![hi, lo],
        WordOrder::Little => vec![lo, hi],
    })
}

/// Wrap a snapshot of tag values in the standard Messages shape so engine
/// SQL `value_json->>'field'` works. Adds the `ts` timestamp field.
pub(crate) fn wrap_snapshot(tags: serde_json::Map<String, Value>, source_topic: &str) -> Value {
    let now = chrono::Utc::now().to_rfc3339();
    let mut value = serde_json::Map::with_capacity(tags.len() + 1);
    value.insert("ts".to_string(), Value::from(now.clone()));
    value.extend(tags);
    let value = Value::Object(value);
    serde_json::json!({
        "key_text":     Value::Null,
        "key_json":     Value::Null,
        "value_text":   serde_json::to_string(&value).unwrap_or_default(),
        "value_json":   value,
        "headers":      serde_json::json!({}),
        "offset_id":    0,
        "created_at":   now,
        "source_topic": source_topic,
    })
}

/// Resolve `host:port` and connect with a bounded timeout.
pub(crate) async fn connect(
    host: &str,
    port: u16,
    unit: u8,
    connect_timeout_ms: u64,
) -> Result<Context, String> {
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("modbus: resolve {}:{}: {}", host, port, e))?;
    let addr = addrs
        .next()
        .ok_or_else(|| format!("modbus: no address for {}:{}", host, port))?;
    tokio::time::timeout(
        Duration::from_millis(connect_timeout_ms),
        tokio_modbus::client::tcp::connect_slave(addr, Slave(unit)),
    )
    .await
    .map_err(|_| format!("modbus: connect to {}:{} timed out", host, port))?
    .map_err(|e| format!("modbus: connect to {}:{}: {}", host, port, e))
}

/// Flatten tokio-modbus' nested `Result<Result<T, ExceptionCode>, Error>`
/// under a read timeout.
async fn bounded<T>(
    timeout_ms: u64,
    what: &str,
    fut: impl std::future::Future<Output = tokio_modbus::Result<T>>,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| format!("modbus: {} timed out", what))?
        .map_err(|e| format!("modbus: {}: {}", what, e))?
        .map_err(|e| format!("modbus: {}: device exception {}", what, e))
}

/// Execute one read spec against an open connection.
async fn read_one(
    ctx: &mut Context,
    spec: &ReadSpec,
    read_timeout_ms: u64,
) -> Result<Value, String> {
    if spec.is_bit_kind() {
        let bits = match spec.kind {
            RegisterKind::Coil => {
                bounded(
                    read_timeout_ms,
                    &format!("read coil '{}'", spec.name),
                    ctx.read_coils(spec.address, spec.count),
                )
                .await?
            }
            RegisterKind::Discrete => {
                bounded(
                    read_timeout_ms,
                    &format!("read discrete '{}'", spec.name),
                    ctx.read_discrete_inputs(spec.address, spec.count),
                )
                .await?
            }
            _ => unreachable!(),
        };
        if bits.len() < spec.count as usize {
            return Err(format!(
                "modbus: read '{}' returned {} bits, expected {}",
                spec.name,
                bits.len(),
                spec.count
            ));
        }
        return Ok(if spec.count == 1 {
            Value::from(bits[0])
        } else {
            Value::from(bits[..spec.count as usize].to_vec())
        });
    }

    let words_per_value = spec.data_type().words();
    let quantity = spec.count * words_per_value;
    let words = match spec.kind {
        RegisterKind::Holding => {
            bounded(
                read_timeout_ms,
                &format!("read holding '{}'", spec.name),
                ctx.read_holding_registers(spec.address, quantity),
            )
            .await?
        }
        RegisterKind::Input => {
            bounded(
                read_timeout_ms,
                &format!("read input '{}'", spec.name),
                ctx.read_input_registers(spec.address, quantity),
            )
            .await?
        }
        _ => unreachable!(),
    };
    if words.len() < quantity as usize {
        return Err(format!(
            "modbus: read '{}' returned {} registers, expected {}",
            spec.name,
            words.len(),
            quantity
        ));
    }
    let values: Vec<Value> = words
        .chunks(words_per_value as usize)
        .take(spec.count as usize)
        .map(|chunk| decode_value(chunk, spec.data_type(), spec.word_order()))
        .collect();
    Ok(if spec.count == 1 {
        values.into_iter().next().unwrap()
    } else {
        Value::from(values)
    })
}

/// Read every configured tag once. Any failure discards the whole cycle
/// (no partial snapshots).
async fn read_cycle(
    ctx: &mut Context,
    config: &ModbusSourceConfig,
) -> Result<serde_json::Map<String, Value>, String> {
    let mut tags = serde_json::Map::with_capacity(config.reads.len());
    for spec in &config.reads {
        let value = read_one(ctx, spec, config.read_timeout_ms).await?;
        tags.insert(spec.name.clone(), value);
    }
    Ok(tags)
}

/// Modbus TCP polling source.
#[derive(Debug)]
pub struct ModbusSource {
    config: ModbusSourceConfig,
    poll_ms: u64,
    /// Failures across stream reopens (open() takes &mut self but the
    /// stream is 'static, so the counter is shared).
    consecutive_failures: Arc<AtomicU32>,
}

impl ModbusSource {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: ModbusSourceConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("modbus: invalid config: {}", e))?;

        if config.host.trim().is_empty() {
            return Err("modbus: host must not be empty".to_string());
        }
        if config.reads.is_empty() {
            return Err("modbus: at least one entry in 'reads' is required".to_string());
        }
        validate_names("modbus", config.reads.iter().map(|r| r.name.as_str()))?;
        for spec in &config.reads {
            if spec.count == 0 {
                return Err(format!("modbus: read '{}': count must be >= 1", spec.name));
            }
            if spec.is_bit_kind() {
                if spec.data_type.is_some() || spec.word_order.is_some() {
                    return Err(format!(
                        "modbus: read '{}': data_type/word_order do not apply to {:?} (always boolean)",
                        spec.name, spec.kind
                    ));
                }
                if spec.count as u32 > MAX_BITS_PER_READ {
                    return Err(format!(
                        "modbus: read '{}': count {} exceeds the protocol limit of {} bits per read",
                        spec.name, spec.count, MAX_BITS_PER_READ
                    ));
                }
            } else {
                let quantity = spec.count as u32 * spec.data_type().words() as u32;
                if quantity > MAX_REGISTERS_PER_READ {
                    return Err(format!(
                        "modbus: read '{}': {} registers exceeds the protocol limit of {} per read",
                        spec.name, quantity, MAX_REGISTERS_PER_READ
                    ));
                }
            }
        }

        let poll_ms = parse_poll_interval_ms(&config.poll)
            .map_err(|e| format!("modbus: invalid poll interval: {}", e))?;
        if poll_ms > MAX_POLL_MS {
            return Err(format!(
                "modbus: poll interval must be <= 60s (got {}ms) — it bounds pipeline stop latency",
                poll_ms
            ));
        }
        if config.max_consecutive_failures == 0 {
            return Err("modbus: max_consecutive_failures must be >= 1".to_string());
        }

        Ok(Self {
            config,
            poll_ms,
            consecutive_failures: Arc::new(AtomicU32::new(0)),
        })
    }

    fn source_topic(&self) -> String {
        format!(
            "modbus://{}:{}/{}",
            self.config.host, self.config.port, self.config.unit
        )
    }
}

#[async_trait]
impl AsyncSource for ModbusSource {
    async fn open(
        &mut self,
        _last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
        let config = self.config.clone();
        let poll_ms = self.poll_ms;
        let topic = self.source_topic();
        let failures = Arc::clone(&self.consecutive_failures);
        let max_failures = config.max_consecutive_failures;

        let stream = async_stream::stream! {
            // Connect inside the stream: connection errors are transient,
            // and an Err from open() would be fatal to the pipeline.
            let mut ctx = match connect(
                &config.host,
                config.port,
                config.unit,
                config.connect_timeout_ms,
            )
            .await
            {
                Ok(ctx) => ctx,
                Err(e) => {
                    let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if n >= max_failures {
                        yield Err(format!("modbus: {} consecutive failures, last: {}", n, e));
                    }
                    return; // clean end — the bridge reopens after poll_interval()
                }
            };

            loop {
                match read_cycle(&mut ctx, &config).await {
                    Ok(tags) => {
                        failures.store(0, Ordering::SeqCst);
                        yield Ok(SourceItem::one_shot(wrap_snapshot(tags, &topic)));
                    }
                    Err(e) => {
                        let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                        if n >= max_failures {
                            yield Err(format!("modbus: {} consecutive failures, last: {}", n, e));
                        }
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
        };

        Ok(Box::pin(stream))
    }

    fn is_continuous(&self) -> bool {
        true
    }

    fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_ms)
    }
}

#[cfg(test)]
pub(crate) mod mock_server {
    //! In-process Modbus TCP server for tests — a canned register map
    //! that answers reads and records writes.

    use std::collections::HashMap;
    use std::future;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tokio_modbus::server::tcp::{accept_tcp_connection, Server};
    use tokio_modbus::server::Service;
    use tokio_modbus::{ExceptionCode, Request, Response};

    #[derive(Debug, Clone, Default)]
    pub struct MockDevice {
        pub holding: Arc<Mutex<HashMap<u16, u16>>>,
        pub input: Arc<Mutex<HashMap<u16, u16>>>,
        pub coils: Arc<Mutex<HashMap<u16, bool>>>,
        pub discrete: Arc<Mutex<HashMap<u16, bool>>>,
    }

    impl MockDevice {
        pub fn set_holding(&self, addr: u16, words: &[u16]) {
            let mut map = self.holding.lock().unwrap();
            for (i, w) in words.iter().enumerate() {
                map.insert(addr + i as u16, *w);
            }
        }

        pub fn set_input(&self, addr: u16, words: &[u16]) {
            let mut map = self.input.lock().unwrap();
            for (i, w) in words.iter().enumerate() {
                map.insert(addr + i as u16, *w);
            }
        }

        pub fn set_coil(&self, addr: u16, value: bool) {
            self.coils.lock().unwrap().insert(addr, value);
        }

        pub fn set_discrete(&self, addr: u16, value: bool) {
            self.discrete.lock().unwrap().insert(addr, value);
        }

        pub fn holding_at(&self, addr: u16) -> u16 {
            *self.holding.lock().unwrap().get(&addr).unwrap_or(&0)
        }

        pub fn coil_at(&self, addr: u16) -> bool {
            *self.coils.lock().unwrap().get(&addr).unwrap_or(&false)
        }

        fn read_words(map: &Mutex<HashMap<u16, u16>>, addr: u16, cnt: u16) -> Vec<u16> {
            let map = map.lock().unwrap();
            (0..cnt)
                .map(|i| *map.get(&(addr + i)).unwrap_or(&0))
                .collect()
        }

        fn read_bits(map: &Mutex<HashMap<u16, bool>>, addr: u16, cnt: u16) -> Vec<bool> {
            let map = map.lock().unwrap();
            (0..cnt)
                .map(|i| *map.get(&(addr + i)).unwrap_or(&false))
                .collect()
        }
    }

    impl Service for MockDevice {
        type Request = Request<'static>;
        type Response = Response;
        type Exception = ExceptionCode;
        type Future = future::Ready<Result<Self::Response, Self::Exception>>;

        fn call(&self, req: Self::Request) -> Self::Future {
            let response = match req {
                Request::ReadHoldingRegisters(addr, cnt) => Ok(Response::ReadHoldingRegisters(
                    Self::read_words(&self.holding, addr, cnt),
                )),
                Request::ReadInputRegisters(addr, cnt) => Ok(Response::ReadInputRegisters(
                    Self::read_words(&self.input, addr, cnt),
                )),
                Request::ReadCoils(addr, cnt) => {
                    Ok(Response::ReadCoils(Self::read_bits(&self.coils, addr, cnt)))
                }
                Request::ReadDiscreteInputs(addr, cnt) => Ok(Response::ReadDiscreteInputs(
                    Self::read_bits(&self.discrete, addr, cnt),
                )),
                Request::WriteSingleCoil(addr, value) => {
                    self.coils.lock().unwrap().insert(addr, value);
                    Ok(Response::WriteSingleCoil(addr, value))
                }
                Request::WriteSingleRegister(addr, word) => {
                    self.holding.lock().unwrap().insert(addr, word);
                    Ok(Response::WriteSingleRegister(addr, word))
                }
                Request::WriteMultipleRegisters(addr, words) => {
                    let cnt = words.len() as u16;
                    self.set_holding(addr, &words);
                    Ok(Response::WriteMultipleRegisters(addr, cnt))
                }
                _ => Err(ExceptionCode::IllegalFunction),
            };
            future::ready(response)
        }
    }

    /// Spawn the mock server on an ephemeral localhost port. The server
    /// task lives until the test's runtime shuts down.
    pub async fn spawn(device: MockDevice) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Server::new(listener);
        tokio::spawn(async move {
            let on_connected = |stream, socket_addr| {
                let device = device.clone();
                async move {
                    accept_tcp_connection(stream, socket_addr, move |_| Ok(Some(device.clone())))
                }
            };
            let _ = server.serve(&on_connected, |_err| {}).await;
        });
        addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use serde_json::json;

    // =========================================================================
    // Config parsing + validation
    // =========================================================================

    fn minimal_config() -> Value {
        json!({
            "host": "10.0.0.5",
            "reads": [
                { "name": "temperature", "kind": "holding", "address": 100 }
            ]
        })
    }

    #[test]
    fn from_config_minimal_applies_defaults() {
        let src = ModbusSource::from_config(&minimal_config()).unwrap();
        assert_eq!(src.config.port, 502);
        assert_eq!(src.config.unit, 1);
        assert_eq!(src.config.poll, "1s");
        assert_eq!(src.poll_ms, 1000);
        assert_eq!(src.config.connect_timeout_ms, 5000);
        assert_eq!(src.config.read_timeout_ms, 3000);
        assert_eq!(src.config.max_consecutive_failures, 5);
        let spec = &src.config.reads[0];
        assert_eq!(spec.data_type(), DataType::U16);
        assert_eq!(spec.word_order(), WordOrder::Big);
        assert_eq!(spec.count, 1);
    }

    #[test]
    fn from_config_full() {
        let cfg = json!({
            "host": "plc.local",
            "port": 1502,
            "unit": 3,
            "poll": "500ms",
            "reads": [
                { "name": "f", "kind": "holding", "address": 0, "data_type": "f32", "word_order": "little" },
                { "name": "running", "kind": "coil", "address": 12 },
                { "name": "arr", "kind": "input", "address": 10, "data_type": "u16", "count": 4 }
            ]
        });
        let src = ModbusSource::from_config(&cfg).unwrap();
        assert_eq!(src.config.port, 1502);
        assert_eq!(src.config.unit, 3);
        assert_eq!(src.poll_ms, 500);
        assert_eq!(src.config.reads[0].data_type(), DataType::F32);
        assert_eq!(src.config.reads[0].word_order(), WordOrder::Little);
        assert_eq!(src.config.reads[1].kind, RegisterKind::Coil);
        assert_eq!(src.config.reads[2].count, 4);
        assert_eq!(src.source_topic(), "modbus://plc.local:1502/3");
    }

    #[test]
    fn from_config_rejects_empty_reads() {
        let cfg = json!({ "host": "x", "reads": [] });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("at least one"));
    }

    #[test]
    fn from_config_rejects_empty_host() {
        let cfg = json!({ "host": " ", "reads": [{ "name": "a", "kind": "coil", "address": 0 }] });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("host"));
    }

    #[test]
    fn from_config_rejects_duplicate_names() {
        let cfg = json!({
            "host": "x",
            "reads": [
                { "name": "a", "kind": "coil", "address": 0 },
                { "name": "a", "kind": "coil", "address": 1 }
            ]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("duplicate name 'a'"));
    }

    #[test]
    fn from_config_rejects_reserved_ts_name() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "ts", "kind": "coil", "address": 0 }]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("reserved"));
    }

    #[test]
    fn from_config_rejects_unknown_kind() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "kind": "flux_capacitor", "address": 0 }]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("invalid config"));
    }

    #[test]
    fn from_config_rejects_zero_count() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "kind": "holding", "address": 0, "count": 0 }]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("count must be >= 1"));
    }

    #[test]
    fn from_config_rejects_data_type_on_coil() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "kind": "coil", "address": 0, "data_type": "f32" }]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("do not apply"));
    }

    #[test]
    fn from_config_rejects_oversized_register_read() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "kind": "holding", "address": 0, "data_type": "f32", "count": 63 }]
        });
        // 63 * 2 = 126 registers > 125.
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("protocol limit"));
    }

    #[test]
    fn from_config_rejects_poll_above_60s() {
        let cfg = json!({
            "host": "x",
            "poll": "2m",
            "reads": [{ "name": "a", "kind": "coil", "address": 0 }]
        });
        let err = ModbusSource::from_config(&cfg).unwrap_err();
        assert!(err.contains("<= 60s"));
    }

    // =========================================================================
    // decode_value / encode_value
    // =========================================================================

    #[test]
    fn decode_u16_and_i16() {
        assert_eq!(
            decode_value(&[513], DataType::U16, WordOrder::Big),
            json!(513)
        );
        assert_eq!(
            decode_value(&[0xFFFF], DataType::I16, WordOrder::Big),
            json!(-1)
        );
        assert_eq!(
            decode_value(&[0xFFFF], DataType::U16, WordOrder::Big),
            json!(65535)
        );
    }

    #[test]
    fn decode_u32_word_orders() {
        // 0x0001_0002 = 65538
        assert_eq!(
            decode_value(&[0x0001, 0x0002], DataType::U32, WordOrder::Big),
            json!(65538)
        );
        assert_eq!(
            decode_value(&[0x0002, 0x0001], DataType::U32, WordOrder::Little),
            json!(65538)
        );
    }

    #[test]
    fn decode_i32_negative() {
        // -2 = 0xFFFF_FFFE
        assert_eq!(
            decode_value(&[0xFFFF, 0xFFFE], DataType::I32, WordOrder::Big),
            json!(-2)
        );
    }

    #[test]
    fn decode_f32_known_vector() {
        // 21.5f32 = 0x41AC_0000 → ABCD words [0x41AC, 0x0000]
        assert_eq!(
            decode_value(&[0x41AC, 0x0000], DataType::F32, WordOrder::Big),
            json!(21.5)
        );
        // CDAB layout for the same value
        assert_eq!(
            decode_value(&[0x0000, 0x41AC], DataType::F32, WordOrder::Little),
            json!(21.5)
        );
    }

    #[test]
    fn encode_decode_roundtrip() {
        let cases = [
            (json!(513), DataType::U16, WordOrder::Big),
            (json!(-7), DataType::I16, WordOrder::Big),
            (json!(65538), DataType::U32, WordOrder::Big),
            (json!(65538), DataType::U32, WordOrder::Little),
            (json!(-123456), DataType::I32, WordOrder::Big),
            (json!(-123456), DataType::I32, WordOrder::Little),
            (json!(21.5), DataType::F32, WordOrder::Big),
            (json!(21.5), DataType::F32, WordOrder::Little),
            (json!(-0.15625), DataType::F32, WordOrder::Big),
        ];
        for (value, dt, wo) in cases {
            let words = encode_value(&value, dt, wo).unwrap();
            assert_eq!(words.len(), dt.words() as usize);
            assert_eq!(decode_value(&words, dt, wo), value, "{:?} {:?}", dt, wo);
        }
    }

    #[test]
    fn encode_rejects_wrong_types_and_ranges() {
        assert!(encode_value(&json!("nope"), DataType::U16, WordOrder::Big).is_err());
        assert!(encode_value(&json!(70000), DataType::U16, WordOrder::Big).is_err());
        assert!(encode_value(&json!(-1), DataType::U32, WordOrder::Big).is_err());
        assert!(encode_value(&json!(true), DataType::F32, WordOrder::Big).is_err());
    }

    // =========================================================================
    // Snapshot wrapping
    // =========================================================================

    #[test]
    fn wrap_snapshot_shape() {
        let mut tags = serde_json::Map::new();
        tags.insert("temperature".into(), json!(21.5));
        tags.insert("running".into(), json!(true));
        let record = wrap_snapshot(tags, "modbus://h:502/1");

        assert_eq!(record["source_topic"], "modbus://h:502/1");
        assert_eq!(record["value_json"]["temperature"], 21.5);
        assert_eq!(record["value_json"]["running"], true);
        assert!(record["value_json"]["ts"].is_string());
        assert_eq!(record["key_text"], Value::Null);
        assert_eq!(record["offset_id"], 0);
        // value_text is the serialized value_json
        let text: Value = serde_json::from_str(record["value_text"].as_str().unwrap()).unwrap();
        assert_eq!(text, record["value_json"]);
    }

    // =========================================================================
    // Mock-server stream tests
    // =========================================================================

    #[tokio::test]
    async fn open_emits_decoded_snapshot() {
        let device = mock_server::MockDevice::default();
        device.set_holding(100, &[0x41AC, 0x0000]); // f32 21.5 ABCD
        device.set_input(200, &[1, 2, 3, 4]);
        device.set_coil(12, true);
        device.set_discrete(3, true);
        let addr = mock_server::spawn(device).await;

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "poll": "50ms",
            "reads": [
                { "name": "temperature", "kind": "holding", "address": 100, "data_type": "f32" },
                { "name": "running", "kind": "coil", "address": 12 },
                { "name": "alarm", "kind": "discrete", "address": 3 },
                { "name": "raw", "kind": "input", "address": 200, "count": 4 }
            ]
        });
        let mut source = ModbusSource::from_config(&cfg).unwrap();
        let mut stream = source.open(Cursor::None).await.unwrap();

        let item = stream.next().await.unwrap().unwrap();
        assert!(item.cursor_advance.is_none());
        let v = &item.record["value_json"];
        assert_eq!(v["temperature"], 21.5);
        assert_eq!(v["running"], true);
        assert_eq!(v["alarm"], true);
        assert_eq!(v["raw"], json!([1, 2, 3, 4]));

        // Second cycle arrives after the poll interval.
        let item2 = stream.next().await.unwrap().unwrap();
        assert_eq!(item2.record["value_json"]["temperature"], 21.5);
    }

    #[tokio::test]
    async fn connect_failure_ends_stream_cleanly_until_cap() {
        // Nothing listens on this port: bind-then-drop to reserve a dead addr.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "poll": "50ms",
            "connect_timeout_ms": 500,
            "max_consecutive_failures": 2,
            "reads": [{ "name": "a", "kind": "coil", "address": 0 }]
        });
        let mut source = ModbusSource::from_config(&cfg).unwrap();

        // First open: below the cap — clean end, no items, no error.
        let mut stream = source.open(Cursor::None).await.unwrap();
        assert!(stream.next().await.is_none());

        // Second open: reaches the cap — yields Err.
        let mut stream = source.open(Cursor::None).await.unwrap();
        let item = stream.next().await.unwrap();
        assert!(item.is_err());
        assert!(item.unwrap_err().contains("consecutive failures"));
    }

    #[tokio::test]
    async fn success_resets_failure_counter() {
        let device = mock_server::MockDevice::default();
        device.set_holding(0, &[7]);
        let addr = mock_server::spawn(device).await;

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "poll": "50ms",
            "max_consecutive_failures": 2,
            "reads": [{ "name": "a", "kind": "holding", "address": 0 }]
        });
        let mut source = ModbusSource::from_config(&cfg).unwrap();
        source.consecutive_failures.store(1, Ordering::SeqCst);

        let mut stream = source.open(Cursor::None).await.unwrap();
        let item = stream.next().await.unwrap().unwrap();
        assert_eq!(item.record["value_json"]["a"], 7);
        assert_eq!(source.consecutive_failures.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn source_is_continuous_with_configured_interval() {
        let src = ModbusSource::from_config(&minimal_config()).unwrap();
        assert!(src.is_continuous());
        assert_eq!(src.poll_interval(), Duration::from_millis(1000));
    }
}
