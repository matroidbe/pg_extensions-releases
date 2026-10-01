//! Modbus TCP write sink — maps record fields to coil/holding-register
//! writes. One write command per mapped field per record, issued in
//! `writes[]` order (deterministic actuation order).
//!
//! Delivery is at-least-once: a batch retried by the engine re-issues
//! its writes. Safe for setpoint-style values (idempotent); do not
//! drive counters or toggle-on-change semantics through a retried
//! pipeline.
//!
//! Error policy is deliberately stricter than the sources: encoding
//! errors and persistent write failures (after one reconnect attempt
//! per batch) propagate and fail the pipeline — silently dropping
//! actuation commands is worse than stopping.
//!
//! DSL configuration: see `design/pg_streaming/connectors.md`.

use crate::connector::input::modbus::{
    connect, default_connect_timeout_ms, default_port, default_read_timeout_ms, default_unit,
    encode_value, validate_names, DataType, RegisterKind, WordOrder,
};
use crate::connector::sdk::AsyncSink;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio_modbus::client::{Context, Writer};

#[derive(Debug, Clone, Deserialize)]
pub struct ModbusSinkConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_unit")]
    pub unit: u8,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_read_timeout_ms", alias = "write_timeout_ms")]
    pub write_timeout_ms: u64,
    pub writes: Vec<WriteSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WriteSpec {
    /// Record field to write. A record missing the field skips this
    /// target (partial updates allowed).
    pub field: String,
    pub kind: RegisterKind,
    pub address: u16,
    #[serde(default)]
    pub data_type: Option<DataType>,
    #[serde(default)]
    pub word_order: Option<WordOrder>,
}

impl WriteSpec {
    fn data_type(&self) -> DataType {
        self.data_type.unwrap_or_default()
    }

    fn word_order(&self) -> WordOrder {
        self.word_order.unwrap_or_default()
    }
}

/// Look up a mapped field: top-level first (processed/mapped records),
/// then inside `value_json` (unprocessed Messages-shaped records).
pub(crate) fn extract_field<'a>(record: &'a Value, field: &str) -> Option<&'a Value> {
    record
        .get(field)
        .or_else(|| record.get("value_json").and_then(|v| v.get(field)))
}

/// Modbus TCP write sink.
pub struct ModbusSink {
    config: ModbusSinkConfig,
    ctx: Option<Context>,
}

impl std::fmt::Debug for ModbusSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModbusSink")
            .field("config", &self.config)
            .field("connected", &self.ctx.is_some())
            .finish()
    }
}

impl ModbusSink {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: ModbusSinkConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("modbus sink: invalid config: {}", e))?;

        if config.host.trim().is_empty() {
            return Err("modbus sink: host must not be empty".to_string());
        }
        if config.writes.is_empty() {
            return Err("modbus sink: at least one entry in 'writes' is required".to_string());
        }
        validate_names(
            "modbus sink",
            config.writes.iter().map(|w| w.field.as_str()),
        )?;
        for spec in &config.writes {
            match spec.kind {
                RegisterKind::Coil => {
                    if spec.data_type.is_some() || spec.word_order.is_some() {
                        return Err(format!(
                            "modbus sink: write '{}': data_type/word_order do not apply to coils (always boolean)",
                            spec.field
                        ));
                    }
                }
                RegisterKind::Holding => {}
                RegisterKind::Discrete | RegisterKind::Input => {
                    return Err(format!(
                        "modbus sink: write '{}': {:?} is a read-only area (use coil or holding)",
                        spec.field, spec.kind
                    ));
                }
            }
        }
        Ok(Self { config, ctx: None })
    }

    async fn ensure_connected(&mut self) -> Result<(), String> {
        if self.ctx.is_none() {
            let ctx = connect(
                &self.config.host,
                self.config.port,
                self.config.unit,
                self.config.connect_timeout_ms,
            )
            .await?;
            self.ctx = Some(ctx);
        }
        Ok(())
    }

    /// Issue all mapped writes for one record, in `writes[]` order.
    async fn write_record(&mut self, record: &Value) -> Result<(), String> {
        for spec in &self.config.writes {
            let Some(value) = extract_field(record, &spec.field) else {
                continue; // missing field skips this target
            };
            let timeout = Duration::from_millis(self.config.write_timeout_ms);
            let ctx = self.ctx.as_mut().expect("connected");
            match spec.kind {
                RegisterKind::Coil => {
                    let state = value.as_bool().ok_or_else(|| {
                        format!(
                            "modbus sink: write '{}': expected a boolean, got {}",
                            spec.field, value
                        )
                    })?;
                    flatten(
                        timeout,
                        &spec.field,
                        ctx.write_single_coil(spec.address, state),
                    )
                    .await?;
                }
                RegisterKind::Holding => {
                    let words = encode_value(value, spec.data_type(), spec.word_order())
                        .map_err(|e| format!("modbus sink: write '{}': {}", spec.field, e))?;
                    if words.len() == 1 {
                        flatten(
                            timeout,
                            &spec.field,
                            ctx.write_single_register(spec.address, words[0]),
                        )
                        .await?;
                    } else {
                        flatten(
                            timeout,
                            &spec.field,
                            ctx.write_multiple_registers(spec.address, &words),
                        )
                        .await?;
                    }
                }
                _ => unreachable!("rejected at from_config"),
            }
        }
        Ok(())
    }

    async fn write_all(&mut self, records: &[Value]) -> Result<(), String> {
        self.ensure_connected().await?;
        for record in records {
            self.write_record(record).await?;
        }
        Ok(())
    }
}

/// Flatten tokio-modbus' nested result under a write timeout.
async fn flatten(
    timeout: Duration,
    field: &str,
    fut: impl std::future::Future<Output = tokio_modbus::Result<()>>,
) -> Result<(), String> {
    tokio::time::timeout(timeout, fut)
        .await
        .map_err(|_| format!("modbus sink: write '{}' timed out", field))?
        .map_err(|e| format!("modbus sink: write '{}': {}", field, e))?
        .map_err(|e| format!("modbus sink: write '{}': device exception {}", field, e))
}

#[async_trait]
impl AsyncSink for ModbusSink {
    async fn write_batch(&mut self, records: &[Value]) -> Result<(), String> {
        match self.write_all(records).await {
            Ok(()) => Ok(()),
            Err(first_err) => {
                // One reconnect attempt per batch, then fail the pipeline.
                // Note: at-least-once — writes issued before the error are
                // re-issued on the retry.
                self.ctx = None;
                self.write_all(records)
                    .await
                    .map_err(|e| format!("{} (after reconnect; first error: {})", e, first_err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::input::modbus::mock_server;
    use serde_json::json;

    fn minimal_config() -> Value {
        json!({
            "host": "10.0.0.5",
            "writes": [
                { "field": "setpoint", "kind": "holding", "address": 40, "data_type": "f32" }
            ]
        })
    }

    // =========================================================================
    // Config parsing + validation
    // =========================================================================

    #[test]
    fn from_config_minimal_applies_defaults() {
        let sink = ModbusSink::from_config(&minimal_config()).unwrap();
        assert_eq!(sink.config.port, 502);
        assert_eq!(sink.config.unit, 1);
        assert_eq!(sink.config.connect_timeout_ms, 5000);
        assert_eq!(sink.config.write_timeout_ms, 3000);
    }

    #[test]
    fn from_config_rejects_empty_writes() {
        let cfg = json!({ "host": "x", "writes": [] });
        let err = ModbusSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("at least one"));
    }

    #[test]
    fn from_config_rejects_read_only_kinds() {
        for kind in ["discrete", "input"] {
            let cfg = json!({
                "host": "x",
                "writes": [{ "field": "a", "kind": kind, "address": 0 }]
            });
            let err = ModbusSink::from_config(&cfg).unwrap_err();
            assert!(err.contains("read-only"), "kind {}: {}", kind, err);
        }
    }

    #[test]
    fn from_config_rejects_data_type_on_coil() {
        let cfg = json!({
            "host": "x",
            "writes": [{ "field": "a", "kind": "coil", "address": 0, "data_type": "f32" }]
        });
        let err = ModbusSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("do not apply"));
    }

    #[test]
    fn from_config_rejects_duplicate_fields() {
        let cfg = json!({
            "host": "x",
            "writes": [
                { "field": "a", "kind": "coil", "address": 0 },
                { "field": "a", "kind": "coil", "address": 1 }
            ]
        });
        let err = ModbusSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("duplicate name"));
    }

    // =========================================================================
    // Field extraction
    // =========================================================================

    #[test]
    fn extract_field_prefers_top_level_then_value_json() {
        let record = json!({
            "setpoint": 1.5,
            "value_json": { "setpoint": 9.9, "enable": true }
        });
        assert_eq!(extract_field(&record, "setpoint"), Some(&json!(1.5)));
        assert_eq!(extract_field(&record, "enable"), Some(&json!(true)));
        assert_eq!(extract_field(&record, "missing"), None);
    }

    // =========================================================================
    // Mock-server write tests
    // =========================================================================

    #[tokio::test]
    async fn write_batch_writes_registers_and_coils() {
        let device = mock_server::MockDevice::default();
        let addr = mock_server::spawn(device.clone()).await;

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "writes": [
                { "field": "setpoint", "kind": "holding", "address": 40, "data_type": "f32" },
                { "field": "count", "kind": "holding", "address": 50 },
                { "field": "enable", "kind": "coil", "address": 12 }
            ]
        });
        let mut sink = ModbusSink::from_config(&cfg).unwrap();

        sink.write_batch(&[
            json!({ "setpoint": 21.5, "count": 7, "enable": true }),
            json!({ "count": 9 }), // partial update: only count
        ])
        .await
        .unwrap();

        // f32 21.5 = 0x41AC0000 → ABCD [0x41AC, 0x0000]
        assert_eq!(device.holding_at(40), 0x41AC);
        assert_eq!(device.holding_at(41), 0x0000);
        assert_eq!(device.holding_at(50), 9); // second record overwrote 7
        assert!(device.coil_at(12));
    }

    #[tokio::test]
    async fn write_batch_fails_on_bad_value_type() {
        let device = mock_server::MockDevice::default();
        let addr = mock_server::spawn(device).await;

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "writes": [{ "field": "enable", "kind": "coil", "address": 0 }]
        });
        let mut sink = ModbusSink::from_config(&cfg).unwrap();
        let err = sink
            .write_batch(&[json!({ "enable": "not a bool" })])
            .await
            .unwrap_err();
        assert!(err.contains("expected a boolean"));
    }

    #[tokio::test]
    async fn write_batch_fails_when_device_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let cfg = json!({
            "host": addr.ip().to_string(),
            "port": addr.port(),
            "connect_timeout_ms": 500,
            "writes": [{ "field": "enable", "kind": "coil", "address": 0 }]
        });
        let mut sink = ModbusSink::from_config(&cfg).unwrap();
        let err = sink
            .write_batch(&[json!({ "enable": true })])
            .await
            .unwrap_err();
        assert!(err.contains("after reconnect"));
    }
}
