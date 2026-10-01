//! Siemens S7 source — polls an S7-300/1200/1500 PLC over ISO-on-TCP
//! (S7comm) via the pure-Rust `rust7` client and emits one record per
//! poll cycle (a coherent snapshot of all configured fields).
//!
//! Same cursor (`Cursor::None`) and error policy as the Modbus source:
//! connect inside the stream, end the stream cleanly on transient
//! errors so the bridge's reopen loop acts as reconnect-with-delay, and
//! fail the pipeline only after `max_consecutive_failures`.
//!
//! rust7 is a sync/blocking client, so every connect and poll cycle
//! runs under `tokio::task::spawn_blocking` — the bridge's runtime
//! thread stays responsive for teardown, and `set_timeout` (mandatory)
//! bounds every blocking call.
//!
//! DSL configuration: see `design/pg_streaming/connectors.md`.

use crate::connector::input::modbus::{
    default_connect_timeout_ms, default_read_timeout_ms, wrap_snapshot,
};
use crate::connector::input::table::parse_poll_interval_ms;
use crate::connector::sdk::{AsyncSource, Cursor, SourceItem};
use async_trait::async_trait;
use futures::stream::BoxStream;
use rust7::{S7Client, S7_AREA_DB, S7_AREA_MK, S7_AREA_PA, S7_AREA_PE, S7_WL_BYTE};
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MAX_POLL_MS: u64 = 60_000;

// =============================================================================
// Configuration
// =============================================================================

/// Connection settings shared by the S7 source and sink.
#[derive(Debug, Clone, Deserialize)]
pub struct S7ConnConfig {
    pub host: String,
    #[serde(default = "default_port_s7")]
    pub port: u16,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub rack: Option<u16>,
    #[serde(default)]
    pub slot: Option<u16>,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms")]
    pub read_timeout_ms: u64,
}

fn default_port_s7() -> u16 {
    102
}
fn default_model() -> String {
    "s7-1200".to_string()
}
fn default_poll() -> String {
    "1s".to_string()
}
fn default_max_consecutive_failures() -> u32 {
    5
}

impl S7ConnConfig {
    pub(crate) fn validate(&self, connector: &str) -> Result<(), String> {
        if self.host.trim().is_empty() {
            return Err(format!("{}: host must not be empty", connector));
        }
        match (self.rack, self.slot) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(format!(
                    "{}: rack and slot must be set together (or both omitted)",
                    connector
                ));
            }
        }
        if self.rack.is_none() && !matches!(self.model.as_str(), "s7-300" | "s7-1200" | "s7-1500") {
            return Err(format!(
                "{}: unsupported model '{}'. Supported: s7-300, s7-1200, s7-1500 (or set rack/slot)",
                connector, self.model
            ));
        }
        Ok(())
    }

    pub(crate) fn source_topic(&self) -> String {
        format!("s7://{}:{}", self.host, self.port)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S7SourceConfig {
    #[serde(flatten)]
    pub conn: S7ConnConfig,
    #[serde(default = "default_poll")]
    pub poll: String,
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
    pub reads: Vec<S7FieldSpec>,
}

/// One field to read (source) or write (sink). `name`/`field` is the
/// snapshot key or record key respectively — the serde alias lets the
/// same spec type serve both.
#[derive(Debug, Clone, Deserialize)]
pub struct S7FieldSpec {
    #[serde(alias = "field")]
    pub name: String,
    pub area: S7Area,
    #[serde(default)]
    pub db: Option<u16>,
    /// Byte offset within the area.
    pub offset: u16,
    #[serde(rename = "type")]
    pub field_type: S7Type,
    /// Bit index 0-7, required for (and only valid for) `bool`.
    #[serde(default)]
    pub bit: Option<u8>,
    /// Byte count, required for (and only valid for) `bytes`.
    #[serde(default)]
    pub length: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S7Area {
    Db,
    Inputs,
    Outputs,
    Markers,
}

impl S7Area {
    pub(crate) fn code(self) -> u8 {
        match self {
            S7Area::Db => S7_AREA_DB,
            S7Area::Inputs => S7_AREA_PE,
            S7Area::Outputs => S7_AREA_PA,
            S7Area::Markers => S7_AREA_MK,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S7Type {
    Bool,
    Byte,
    Int,
    Dint,
    Real,
    Bytes,
}

impl S7FieldSpec {
    /// Byte length of the area read/write backing this field
    /// (bool is bit-addressed and handled separately).
    pub(crate) fn byte_len(&self) -> usize {
        match self.field_type {
            S7Type::Bool => 1,
            S7Type::Byte => 1,
            S7Type::Int => 2,
            S7Type::Dint | S7Type::Real => 4,
            S7Type::Bytes => self.length.unwrap_or(0) as usize,
        }
    }

    pub(crate) fn db_number(&self) -> u16 {
        self.db.unwrap_or(0)
    }

    /// Structural validation shared by source (`reads`) and sink (`writes`).
    pub(crate) fn validate(&self, connector: &str) -> Result<(), String> {
        match self.area {
            S7Area::Db => {
                if self.db.is_none() {
                    return Err(format!(
                        "{}: field '{}': area 'db' requires a 'db' number",
                        connector, self.name
                    ));
                }
            }
            _ => {
                if self.db.is_some() {
                    return Err(format!(
                        "{}: field '{}': 'db' only applies to area 'db'",
                        connector, self.name
                    ));
                }
            }
        }
        match self.field_type {
            S7Type::Bool => match self.bit {
                Some(b) if b <= 7 => {}
                Some(b) => {
                    return Err(format!(
                        "{}: field '{}': bit {} out of range 0-7",
                        connector, self.name, b
                    ));
                }
                None => {
                    return Err(format!(
                        "{}: field '{}': type 'bool' requires 'bit' (0-7)",
                        connector, self.name
                    ));
                }
            },
            _ => {
                if self.bit.is_some() {
                    return Err(format!(
                        "{}: field '{}': 'bit' only applies to type 'bool'",
                        connector, self.name
                    ));
                }
            }
        }
        match self.field_type {
            S7Type::Bytes => match self.length {
                Some(l) if l >= 1 => {}
                _ => {
                    return Err(format!(
                        "{}: field '{}': type 'bytes' requires 'length' >= 1",
                        connector, self.name
                    ));
                }
            },
            _ => {
                if self.length.is_some() {
                    return Err(format!(
                        "{}: field '{}': 'length' only applies to type 'bytes'",
                        connector, self.name
                    ));
                }
            }
        }
        Ok(())
    }
}

// =============================================================================
// Client abstraction — lets cycle logic be unit-tested without a PLC
// =============================================================================

/// The subset of the rust7 client the connectors use. Implemented by
/// `S7Client` and by test fakes.
pub(crate) trait S7Io: Send {
    fn read_area(&mut self, area: u8, db: u16, start: u16, buf: &mut [u8]) -> Result<(), String>;
    fn read_bit(&mut self, area: u8, db: u16, byte_num: u16, bit: u8) -> Result<bool, String>;
    fn write_area(&mut self, area: u8, db: u16, start: u16, buf: &[u8]) -> Result<(), String>;
    fn write_bit(
        &mut self,
        area: u8,
        db: u16,
        byte_num: u16,
        bit: u8,
        value: bool,
    ) -> Result<(), String>;
}

impl S7Io for S7Client {
    fn read_area(&mut self, area: u8, db: u16, start: u16, buf: &mut [u8]) -> Result<(), String> {
        S7Client::read_area(self, area, db, start, S7_WL_BYTE, buf)
            .map_err(|e| format!("s7: read_area: {}", e))
    }

    fn read_bit(&mut self, area: u8, db: u16, byte_num: u16, bit: u8) -> Result<bool, String> {
        S7Client::read_bit(self, area, db, byte_num, bit)
            .map_err(|e| format!("s7: read_bit: {}", e))
    }

    fn write_area(&mut self, area: u8, db: u16, start: u16, buf: &[u8]) -> Result<(), String> {
        S7Client::write_area(self, area, db, start, S7_WL_BYTE, buf)
            .map_err(|e| format!("s7: write_area: {}", e))
    }

    fn write_bit(
        &mut self,
        area: u8,
        db: u16,
        byte_num: u16,
        bit: u8,
        value: bool,
    ) -> Result<(), String> {
        S7Client::write_bit(self, area, db, byte_num, bit, value)
            .map_err(|e| format!("s7: write_bit: {}", e))
    }
}

/// Build and connect a rust7 client per the connection config. Blocking —
/// call from `spawn_blocking`. `set_timeout` is mandatory so a dead PLC
/// can never block pipeline shutdown beyond one timeout.
pub(crate) fn connect_client(conn: &S7ConnConfig) -> Result<S7Client, String> {
    let mut client = S7Client::new();
    client
        .set_timeout(
            conn.connect_timeout_ms,
            conn.read_timeout_ms,
            conn.read_timeout_ms,
        )
        .map_err(|e| format!("s7: set_timeout: {}", e))?;
    if conn.port != 102 {
        client
            .set_connection_port(conn.port)
            .map_err(|e| format!("s7: set_connection_port: {}", e))?;
    }
    let result = match (conn.rack, conn.slot) {
        (Some(rack), Some(slot)) => client.connect_rack_slot(&conn.host, rack, slot),
        _ => match conn.model.as_str() {
            "s7-300" => client.connect_s7300(&conn.host),
            _ => client.connect_s71200_1500(&conn.host),
        },
    };
    result.map_err(|e| format!("s7: connect {}:{}: {}", conn.host, conn.port, e))?;
    Ok(client)
}

// =============================================================================
// Field decode/encode (big-endian per S7)
// =============================================================================

/// Decode one non-bool field from its byte buffer.
pub(crate) fn decode_field(buf: &[u8], field_type: S7Type) -> Value {
    match field_type {
        S7Type::Bool => Value::from(buf[0] != 0),
        S7Type::Byte => Value::from(buf[0]),
        S7Type::Int => Value::from(i16::from_be_bytes([buf[0], buf[1]])),
        S7Type::Dint => Value::from(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])),
        S7Type::Real => Value::from(f32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])),
        S7Type::Bytes => Value::from(hex::encode(buf)),
    }
}

/// Encode one non-bool field into bytes — the exact inverse of
/// [`decode_field`]. Used by the S7 sink.
pub(crate) fn encode_field(value: &Value, field_type: S7Type) -> Result<Vec<u8>, String> {
    fn int_from(value: &Value) -> Result<i64, String> {
        value
            .as_i64()
            .ok_or_else(|| format!("expected an integer, got {}", value))
    }

    Ok(match field_type {
        S7Type::Bool => {
            return Err("bool fields are bit-addressed; use write_bit".to_string());
        }
        S7Type::Byte => {
            let n = int_from(value)?;
            let b = u8::try_from(n).map_err(|_| format!("{} out of range for byte", n))?;
            vec![b]
        }
        S7Type::Int => {
            let n = int_from(value)?;
            let v = i16::try_from(n).map_err(|_| format!("{} out of range for int", n))?;
            v.to_be_bytes().to_vec()
        }
        S7Type::Dint => {
            let n = int_from(value)?;
            let v = i32::try_from(n).map_err(|_| format!("{} out of range for dint", n))?;
            v.to_be_bytes().to_vec()
        }
        S7Type::Real => {
            let f = value
                .as_f64()
                .ok_or_else(|| format!("expected a number, got {}", value))?;
            (f as f32).to_be_bytes().to_vec()
        }
        S7Type::Bytes => {
            let s = value
                .as_str()
                .ok_or_else(|| format!("expected a hex string, got {}", value))?;
            hex::decode(s).map_err(|e| format!("invalid hex string: {}", e))?
        }
    })
}

// =============================================================================
// Poll cycle
// =============================================================================

/// Read every configured field once. Pure sync — runs inside
/// `spawn_blocking` in production, against a fake in tests. Any failure
/// discards the whole cycle (no partial snapshots).
pub(crate) fn read_cycle(
    io: &mut dyn S7Io,
    reads: &[S7FieldSpec],
) -> Result<serde_json::Map<String, Value>, String> {
    let mut tags = serde_json::Map::with_capacity(reads.len());
    for spec in reads {
        let value = match spec.field_type {
            S7Type::Bool => {
                let bit = io
                    .read_bit(
                        spec.area.code(),
                        spec.db_number(),
                        spec.offset,
                        spec.bit.unwrap_or(0),
                    )
                    .map_err(|e| format!("{} (field '{}')", e, spec.name))?;
                Value::from(bit)
            }
            _ => {
                let mut buf = vec![0u8; spec.byte_len()];
                io.read_area(spec.area.code(), spec.db_number(), spec.offset, &mut buf)
                    .map_err(|e| format!("{} (field '{}')", e, spec.name))?;
                decode_field(&buf, spec.field_type)
            }
        };
        tags.insert(spec.name.clone(), value);
    }
    Ok(tags)
}

// =============================================================================
// Source
// =============================================================================

/// Siemens S7 polling source.
#[derive(Debug)]
pub struct S7Source {
    config: S7SourceConfig,
    poll_ms: u64,
    consecutive_failures: Arc<AtomicU32>,
}

impl S7Source {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: S7SourceConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("s7: invalid config: {}", e))?;

        config.conn.validate("s7")?;
        if config.reads.is_empty() {
            return Err("s7: at least one entry in 'reads' is required".to_string());
        }
        crate::connector::input::modbus::validate_names(
            "s7",
            config.reads.iter().map(|r| r.name.as_str()),
        )?;
        for spec in &config.reads {
            spec.validate("s7")?;
        }

        let poll_ms = parse_poll_interval_ms(&config.poll)
            .map_err(|e| format!("s7: invalid poll interval: {}", e))?;
        if poll_ms > MAX_POLL_MS {
            return Err(format!(
                "s7: poll interval must be <= 60s (got {}ms) — it bounds pipeline stop latency",
                poll_ms
            ));
        }
        if config.max_consecutive_failures == 0 {
            return Err("s7: max_consecutive_failures must be >= 1".to_string());
        }

        Ok(Self {
            config,
            poll_ms,
            consecutive_failures: Arc::new(AtomicU32::new(0)),
        })
    }
}

#[async_trait]
impl AsyncSource for S7Source {
    async fn open(
        &mut self,
        _last_cursor: Cursor,
    ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
        let config = self.config.clone();
        let poll_ms = self.poll_ms;
        let topic = self.config.conn.source_topic();
        let failures = Arc::clone(&self.consecutive_failures);
        let max_failures = config.max_consecutive_failures;

        let stream = async_stream::stream! {
            // Connect inside the stream (transient errors must not fail the
            // pipeline) and on the blocking pool (rust7 is sync).
            let conn = config.conn.clone();
            let connected = tokio::task::spawn_blocking(move || connect_client(&conn))
                .await
                .map_err(|e| format!("s7: connect task panicked: {}", e))
                .and_then(|r| r);
            let mut client = match connected {
                Ok(client) => client,
                Err(e) => {
                    let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                    if n >= max_failures {
                        yield Err(format!("s7: {} consecutive failures, last: {}", n, e));
                    }
                    return; // clean end — the bridge reopens after poll_interval()
                }
            };

            let reads = Arc::new(config.reads.clone());
            loop {
                let reads_for_cycle = Arc::clone(&reads);
                let cycle = tokio::task::spawn_blocking(move || {
                    let result = read_cycle(&mut client, &reads_for_cycle);
                    (client, result)
                })
                .await;
                let (returned_client, result) = match cycle {
                    Ok(pair) => pair,
                    Err(e) => {
                        let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                        let msg = format!("s7: poll task panicked: {}", e);
                        if n >= max_failures {
                            yield Err(format!("s7: {} consecutive failures, last: {}", n, msg));
                        }
                        return;
                    }
                };
                client = returned_client;
                match result {
                    Ok(tags) => {
                        failures.store(0, Ordering::SeqCst);
                        yield Ok(SourceItem::one_shot(wrap_snapshot(tags, &topic)));
                    }
                    Err(e) => {
                        let n = failures.fetch_add(1, Ordering::SeqCst) + 1;
                        if n >= max_failures {
                            yield Err(format!("s7: {} consecutive failures, last: {}", n, e));
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
pub(crate) mod fake {
    //! In-memory fake of the S7 client for cycle-logic tests.

    use super::S7Io;
    use std::collections::HashMap;

    #[derive(Debug, Default)]
    pub struct FakeS7 {
        /// (area, db) -> 256-byte memory image.
        pub memory: HashMap<(u8, u16), Vec<u8>>,
        /// Force every operation to fail with this message.
        pub fail_with: Option<String>,
    }

    impl FakeS7 {
        pub fn with_area(mut self, area: u8, db: u16, offset: usize, bytes: &[u8]) -> Self {
            let image = self
                .memory
                .entry((area, db))
                .or_insert_with(|| vec![0; 256]);
            image[offset..offset + bytes.len()].copy_from_slice(bytes);
            self
        }

        pub fn area_bytes(&self, area: u8, db: u16, offset: usize, len: usize) -> Vec<u8> {
            self.memory
                .get(&(area, db))
                .map(|image| image[offset..offset + len].to_vec())
                .unwrap_or_else(|| vec![0; len])
        }

        fn check(&self) -> Result<(), String> {
            match &self.fail_with {
                Some(message) => Err(message.clone()),
                None => Ok(()),
            }
        }
    }

    impl S7Io for FakeS7 {
        fn read_area(
            &mut self,
            area: u8,
            db: u16,
            start: u16,
            buf: &mut [u8],
        ) -> Result<(), String> {
            self.check()?;
            let bytes = self.area_bytes(area, db, start as usize, buf.len());
            buf.copy_from_slice(&bytes);
            Ok(())
        }

        fn read_bit(&mut self, area: u8, db: u16, byte_num: u16, bit: u8) -> Result<bool, String> {
            self.check()?;
            let byte = self.area_bytes(area, db, byte_num as usize, 1)[0];
            Ok((byte >> bit) & 1 == 1)
        }

        fn write_area(&mut self, area: u8, db: u16, start: u16, buf: &[u8]) -> Result<(), String> {
            self.check()?;
            let image = self
                .memory
                .entry((area, db))
                .or_insert_with(|| vec![0; 256]);
            image[start as usize..start as usize + buf.len()].copy_from_slice(buf);
            Ok(())
        }

        fn write_bit(
            &mut self,
            area: u8,
            db: u16,
            byte_num: u16,
            bit: u8,
            value: bool,
        ) -> Result<(), String> {
            self.check()?;
            let image = self
                .memory
                .entry((area, db))
                .or_insert_with(|| vec![0; 256]);
            if value {
                image[byte_num as usize] |= 1 << bit;
            } else {
                image[byte_num as usize] &= !(1 << bit);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal_config() -> Value {
        json!({
            "host": "192.168.0.100",
            "reads": [
                { "name": "speed", "area": "db", "db": 100, "offset": 2, "type": "int" }
            ]
        })
    }

    // =========================================================================
    // Config parsing + validation
    // =========================================================================

    #[test]
    fn from_config_minimal_applies_defaults() {
        let src = S7Source::from_config(&minimal_config()).unwrap();
        assert_eq!(src.config.conn.port, 102);
        assert_eq!(src.config.conn.model, "s7-1200");
        assert_eq!(src.config.poll, "1s");
        assert_eq!(src.poll_ms, 1000);
        assert_eq!(src.config.conn.connect_timeout_ms, 5000);
        assert_eq!(src.config.conn.read_timeout_ms, 3000);
        assert_eq!(src.config.max_consecutive_failures, 5);
        assert_eq!(src.config.conn.source_topic(), "s7://192.168.0.100:102");
    }

    #[test]
    fn from_config_full() {
        let cfg = json!({
            "host": "plc.local",
            "port": 1102,
            "model": "s7-300",
            "poll": "500ms",
            "reads": [
                { "name": "motor_on", "area": "db", "db": 100, "offset": 0, "type": "bool", "bit": 3 },
                { "name": "temp", "area": "db", "db": 100, "offset": 8, "type": "real" },
                { "name": "flags", "area": "markers", "offset": 0, "type": "bytes", "length": 8 }
            ]
        });
        let src = S7Source::from_config(&cfg).unwrap();
        assert_eq!(src.config.conn.model, "s7-300");
        assert_eq!(src.poll_ms, 500);
        assert_eq!(src.config.reads[0].bit, Some(3));
        assert_eq!(src.config.reads[2].area, S7Area::Markers);
        assert_eq!(src.config.reads[2].length, Some(8));
    }

    #[test]
    fn from_config_rejects_db_area_without_db() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "db", "offset": 0, "type": "int" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("requires a 'db' number"));
    }

    #[test]
    fn from_config_rejects_db_on_markers() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "markers", "db": 1, "offset": 0, "type": "int" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("'db' only applies"));
    }

    #[test]
    fn from_config_rejects_bool_without_bit() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "bool" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("requires 'bit'"));
    }

    #[test]
    fn from_config_rejects_bit_out_of_range() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "bool", "bit": 8 }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("out of range"));
    }

    #[test]
    fn from_config_rejects_bit_on_non_bool() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "int", "bit": 1 }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("'bit' only applies"));
    }

    #[test]
    fn from_config_rejects_bytes_without_length() {
        let cfg = json!({
            "host": "x",
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "bytes" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("requires 'length'"));
    }

    #[test]
    fn from_config_rejects_rack_without_slot() {
        let cfg = json!({
            "host": "x",
            "rack": 0,
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "int" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("rack and slot"));
    }

    #[test]
    fn from_config_rejects_unknown_model() {
        let cfg = json!({
            "host": "x",
            "model": "s5-95u",
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "int" }]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("unsupported model"));
    }

    #[test]
    fn from_config_rack_slot_skips_model_check() {
        let cfg = json!({
            "host": "x",
            "model": "whatever",
            "rack": 0,
            "slot": 2,
            "reads": [{ "name": "a", "area": "markers", "offset": 0, "type": "int" }]
        });
        assert!(S7Source::from_config(&cfg).is_ok());
    }

    #[test]
    fn from_config_rejects_duplicate_names() {
        let cfg = json!({
            "host": "x",
            "reads": [
                { "name": "a", "area": "markers", "offset": 0, "type": "int" },
                { "name": "a", "area": "markers", "offset": 2, "type": "int" }
            ]
        });
        let err = S7Source::from_config(&cfg).unwrap_err();
        assert!(err.contains("duplicate name"));
    }

    #[test]
    fn source_is_continuous_with_configured_interval() {
        let src = S7Source::from_config(&minimal_config()).unwrap();
        assert!(src.is_continuous());
        assert_eq!(src.poll_interval(), Duration::from_millis(1000));
    }

    // =========================================================================
    // decode_field / encode_field
    // =========================================================================

    #[test]
    fn decode_known_vectors() {
        // real 21.5 = 0x41AC0000 big-endian
        assert_eq!(
            decode_field(&[0x41, 0xAC, 0x00, 0x00], S7Type::Real),
            json!(21.5)
        );
        assert_eq!(decode_field(&[0xFF, 0xFF], S7Type::Int), json!(-1));
        assert_eq!(decode_field(&[0x01, 0x02], S7Type::Int), json!(258));
        assert_eq!(
            decode_field(&[0xFF, 0xFF, 0xFF, 0xFE], S7Type::Dint),
            json!(-2)
        );
        assert_eq!(decode_field(&[0xAB], S7Type::Byte), json!(171));
        assert_eq!(
            decode_field(&[0xDE, 0xAD, 0xBE, 0xEF], S7Type::Bytes),
            json!("deadbeef")
        );
    }

    #[test]
    fn encode_decode_roundtrip() {
        let cases = [
            (json!(171), S7Type::Byte),
            (json!(-1), S7Type::Int),
            (json!(258), S7Type::Int),
            (json!(-123456), S7Type::Dint),
            (json!(21.5), S7Type::Real),
            (json!("deadbeef"), S7Type::Bytes),
        ];
        for (value, t) in cases {
            let bytes = encode_field(&value, t).unwrap();
            assert_eq!(decode_field(&bytes, t), value, "{:?}", t);
        }
    }

    #[test]
    fn encode_rejects_wrong_types_and_ranges() {
        assert!(encode_field(&json!("x"), S7Type::Int).is_err());
        assert!(encode_field(&json!(70000), S7Type::Int).is_err());
        assert!(encode_field(&json!(256), S7Type::Byte).is_err());
        assert!(encode_field(&json!("not-hex"), S7Type::Bytes).is_err());
        assert!(encode_field(&json!(true), S7Type::Bool).is_err());
    }

    // =========================================================================
    // read_cycle against the fake client
    // =========================================================================

    #[test]
    fn read_cycle_decodes_all_field_types() {
        let mut io = fake::FakeS7::default()
            // DB100: byte 0 = flags (bit 3 set), bytes 2-3 = int 258,
            // bytes 4-7 = dint -2, bytes 8-11 = real 21.5
            .with_area(S7_AREA_DB, 100, 0, &[0b0000_1000])
            .with_area(S7_AREA_DB, 100, 2, &[0x01, 0x02])
            .with_area(S7_AREA_DB, 100, 4, &[0xFF, 0xFF, 0xFF, 0xFE])
            .with_area(S7_AREA_DB, 100, 8, &[0x41, 0xAC, 0x00, 0x00])
            .with_area(S7_AREA_MK, 0, 0, &[0xDE, 0xAD]);

        let reads: Vec<S7FieldSpec> = serde_json::from_value(json!([
            { "name": "motor_on", "area": "db", "db": 100, "offset": 0, "type": "bool", "bit": 3 },
            { "name": "motor_off", "area": "db", "db": 100, "offset": 0, "type": "bool", "bit": 4 },
            { "name": "speed", "area": "db", "db": 100, "offset": 2, "type": "int" },
            { "name": "total", "area": "db", "db": 100, "offset": 4, "type": "dint" },
            { "name": "temp", "area": "db", "db": 100, "offset": 8, "type": "real" },
            { "name": "flags", "area": "markers", "offset": 0, "type": "bytes", "length": 2 }
        ]))
        .unwrap();

        let tags = read_cycle(&mut io, &reads).unwrap();
        assert_eq!(tags["motor_on"], json!(true));
        assert_eq!(tags["motor_off"], json!(false));
        assert_eq!(tags["speed"], json!(258));
        assert_eq!(tags["total"], json!(-2));
        assert_eq!(tags["temp"], json!(21.5));
        assert_eq!(tags["flags"], json!("dead"));
    }

    #[test]
    fn read_cycle_fails_whole_cycle_on_error() {
        let mut io = fake::FakeS7 {
            fail_with: Some("CPU not responding".to_string()),
            ..Default::default()
        };
        let reads: Vec<S7FieldSpec> = serde_json::from_value(json!([
            { "name": "speed", "area": "db", "db": 100, "offset": 2, "type": "int" }
        ]))
        .unwrap();
        let err = read_cycle(&mut io, &reads).unwrap_err();
        assert!(err.contains("CPU not responding"));
        assert!(err.contains("field 'speed'"));
    }
}
