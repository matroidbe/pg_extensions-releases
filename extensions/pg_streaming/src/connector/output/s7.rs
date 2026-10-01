//! Siemens S7 write sink — maps record fields to PLC memory writes
//! (DB/outputs/markers; inputs are read-only). One `write_area` or
//! `write_bit` call per mapped field per record, in `writes[]` order.
//!
//! Same at-least-once / fail-stop semantics as the Modbus sink: a batch
//! retried by the engine re-issues its writes, and persistent failures
//! (after one reconnect attempt per batch) fail the pipeline.
//!
//! rust7 is a sync client, so each batch runs under
//! `tokio::task::spawn_blocking`, moving the client in and out.
//!
//! DSL configuration: see `design/pg_streaming/connectors.md`.

use crate::connector::input::modbus::validate_names;
use crate::connector::input::s7::{
    connect_client, encode_field, S7Area, S7ConnConfig, S7FieldSpec, S7Io, S7Type,
};
use crate::connector::output::modbus::extract_field;
use crate::connector::sdk::AsyncSink;
use async_trait::async_trait;
use rust7::S7Client;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct S7SinkConfig {
    #[serde(flatten)]
    pub conn: S7ConnConfig,
    pub writes: Vec<S7FieldSpec>,
}

/// Issue all mapped writes for a batch of records. Pure sync — runs
/// inside `spawn_blocking` in production, against a fake in tests.
pub(crate) fn write_cycle(
    io: &mut dyn S7Io,
    writes: &[S7FieldSpec],
    records: &[Value],
) -> Result<(), String> {
    for record in records {
        for spec in writes {
            let Some(value) = extract_field(record, &spec.name) else {
                continue; // missing field skips this target
            };
            match spec.field_type {
                S7Type::Bool => {
                    let state = value.as_bool().ok_or_else(|| {
                        format!(
                            "s7 sink: write '{}': expected a boolean, got {}",
                            spec.name, value
                        )
                    })?;
                    io.write_bit(
                        spec.area.code(),
                        spec.db_number(),
                        spec.offset,
                        spec.bit.unwrap_or(0),
                        state,
                    )
                    .map_err(|e| format!("{} (field '{}')", e, spec.name))?;
                }
                _ => {
                    let bytes = encode_field(value, spec.field_type)
                        .map_err(|e| format!("s7 sink: write '{}': {}", spec.name, e))?;
                    if spec.field_type == S7Type::Bytes && bytes.len() != spec.byte_len() {
                        return Err(format!(
                            "s7 sink: write '{}': hex value is {} bytes, config says {}",
                            spec.name,
                            bytes.len(),
                            spec.byte_len()
                        ));
                    }
                    io.write_area(spec.area.code(), spec.db_number(), spec.offset, &bytes)
                        .map_err(|e| format!("{} (field '{}')", e, spec.name))?;
                }
            }
        }
    }
    Ok(())
}

/// Siemens S7 write sink.
pub struct S7Sink {
    config: S7SinkConfig,
    client: Option<S7Client>,
}

impl std::fmt::Debug for S7Sink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S7Sink")
            .field("config", &self.config)
            .field("connected", &self.client.is_some())
            .finish()
    }
}

impl S7Sink {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: S7SinkConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("s7 sink: invalid config: {}", e))?;

        config.conn.validate("s7 sink")?;
        if config.writes.is_empty() {
            return Err("s7 sink: at least one entry in 'writes' is required".to_string());
        }
        validate_names("s7 sink", config.writes.iter().map(|w| w.name.as_str()))?;
        for spec in &config.writes {
            spec.validate("s7 sink")?;
            if spec.area == S7Area::Inputs {
                return Err(format!(
                    "s7 sink: write '{}': area 'inputs' is read-only (use db, outputs or markers)",
                    spec.name
                ));
            }
        }
        Ok(Self {
            config,
            client: None,
        })
    }

    /// Connect if needed and run one batch on the blocking pool, moving
    /// the client in and out.
    async fn write_all(&mut self, records: Vec<Value>) -> Result<(), String> {
        let conn = self.config.conn.clone();
        let writes = self.config.writes.clone();
        let existing = self.client.take();

        let (client, result) = tokio::task::spawn_blocking(move || {
            let mut client = match existing {
                Some(client) => client,
                None => match connect_client(&conn) {
                    Ok(client) => client,
                    Err(e) => return (None, Err(e)),
                },
            };
            let result = write_cycle(&mut client, &writes, &records);
            (Some(client), result)
        })
        .await
        .map_err(|e| format!("s7 sink: write task panicked: {}", e))?;

        self.client = client;
        result
    }
}

#[async_trait]
impl AsyncSink for S7Sink {
    async fn write_batch(&mut self, records: &[Value]) -> Result<(), String> {
        match self.write_all(records.to_vec()).await {
            Ok(()) => Ok(()),
            Err(first_err) => {
                // One reconnect attempt per batch, then fail the pipeline.
                // At-least-once: writes issued before the error re-run.
                self.client = None;
                self.write_all(records.to_vec())
                    .await
                    .map_err(|e| format!("{} (after reconnect; first error: {})", e, first_err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::input::s7::fake::FakeS7;
    use rust7::{S7_AREA_DB, S7_AREA_PA};
    use serde_json::json;

    fn specs(v: Value) -> Vec<S7FieldSpec> {
        serde_json::from_value(v).unwrap()
    }

    // =========================================================================
    // Config parsing + validation
    // =========================================================================

    #[test]
    fn from_config_minimal_applies_defaults() {
        let cfg = json!({
            "host": "192.168.0.100",
            "writes": [
                { "field": "setpoint", "area": "db", "db": 100, "offset": 8, "type": "real" }
            ]
        });
        let sink = S7Sink::from_config(&cfg).unwrap();
        assert_eq!(sink.config.conn.port, 102);
        assert_eq!(sink.config.conn.model, "s7-1200");
        assert_eq!(sink.config.writes[0].name, "setpoint");
    }

    #[test]
    fn from_config_rejects_empty_writes() {
        let cfg = json!({ "host": "x", "writes": [] });
        let err = S7Sink::from_config(&cfg).unwrap_err();
        assert!(err.contains("at least one"));
    }

    #[test]
    fn from_config_rejects_inputs_area() {
        let cfg = json!({
            "host": "x",
            "writes": [{ "field": "a", "area": "inputs", "offset": 0, "type": "int" }]
        });
        let err = S7Sink::from_config(&cfg).unwrap_err();
        assert!(err.contains("read-only"));
    }

    #[test]
    fn from_config_runs_field_spec_validation() {
        let cfg = json!({
            "host": "x",
            "writes": [{ "field": "a", "area": "db", "offset": 0, "type": "int" }]
        });
        let err = S7Sink::from_config(&cfg).unwrap_err();
        assert!(err.contains("requires a 'db' number"));
    }

    // =========================================================================
    // write_cycle against the fake client
    // =========================================================================

    #[test]
    fn write_cycle_encodes_all_field_types() {
        let mut io = FakeS7::default();
        let writes = specs(json!([
            { "field": "setpoint", "area": "db", "db": 100, "offset": 8, "type": "real" },
            { "field": "speed", "area": "db", "db": 100, "offset": 2, "type": "int" },
            { "field": "enable", "area": "db", "db": 100, "offset": 0, "type": "bool", "bit": 3 },
            { "field": "mask", "area": "outputs", "offset": 4, "type": "bytes", "length": 2 }
        ]));
        let records = vec![json!({
            "setpoint": 21.5,
            "speed": 258,
            "enable": true,
            "mask": "dead"
        })];

        write_cycle(&mut io, &writes, &records).unwrap();

        assert_eq!(
            io.area_bytes(S7_AREA_DB, 100, 8, 4),
            vec![0x41, 0xAC, 0x00, 0x00]
        );
        assert_eq!(io.area_bytes(S7_AREA_DB, 100, 2, 2), vec![0x01, 0x02]);
        assert_eq!(io.area_bytes(S7_AREA_DB, 100, 0, 1), vec![0b0000_1000]);
        assert_eq!(io.area_bytes(S7_AREA_PA, 0, 4, 2), vec![0xDE, 0xAD]);
    }

    #[test]
    fn write_cycle_bit_write_leaves_neighbors() {
        let mut io = FakeS7::default().with_area(S7_AREA_DB, 1, 0, &[0b1010_0001]);
        let writes = specs(json!([
            { "field": "flag", "area": "db", "db": 1, "offset": 0, "type": "bool", "bit": 1 }
        ]));
        write_cycle(&mut io, &writes, &[json!({ "flag": true })]).unwrap();
        assert_eq!(io.area_bytes(S7_AREA_DB, 1, 0, 1), vec![0b1010_0011]);
    }

    #[test]
    fn write_cycle_skips_missing_fields() {
        let mut io = FakeS7::default().with_area(S7_AREA_DB, 1, 0, &[7, 7]);
        let writes = specs(json!([
            { "field": "absent", "area": "db", "db": 1, "offset": 0, "type": "int" }
        ]));
        write_cycle(&mut io, &writes, &[json!({ "other": 1 })]).unwrap();
        // untouched
        assert_eq!(io.area_bytes(S7_AREA_DB, 1, 0, 2), vec![7, 7]);
    }

    #[test]
    fn write_cycle_rejects_wrong_value_type() {
        let mut io = FakeS7::default();
        let writes = specs(json!([
            { "field": "speed", "area": "db", "db": 1, "offset": 0, "type": "int" }
        ]));
        let err = write_cycle(&mut io, &writes, &[json!({ "speed": "fast" })]).unwrap_err();
        assert!(err.contains("expected an integer"));
    }

    #[test]
    fn write_cycle_rejects_hex_length_mismatch() {
        let mut io = FakeS7::default();
        let writes = specs(json!([
            { "field": "mask", "area": "markers", "offset": 0, "type": "bytes", "length": 4 }
        ]));
        let err = write_cycle(&mut io, &writes, &[json!({ "mask": "dead" })]).unwrap_err();
        assert!(err.contains("2 bytes, config says 4"));
    }

    #[test]
    fn write_cycle_propagates_io_errors() {
        let mut io = FakeS7 {
            fail_with: Some("connection lost".to_string()),
            ..Default::default()
        };
        let writes = specs(json!([
            { "field": "speed", "area": "db", "db": 1, "offset": 0, "type": "int" }
        ]));
        let err = write_cycle(&mut io, &writes, &[json!({ "speed": 1 })]).unwrap_err();
        assert!(err.contains("connection lost"));
        assert!(err.contains("field 'speed'"));
    }
}
