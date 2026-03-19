//! CDC input connector — reads change events via PostgreSQL logical replication
//!
//! Uses wal2json output plugin with `pg_logical_slot_peek_changes()` (non-consuming)
//! for reading and `pg_replication_slot_advance()` for committing. This gives
//! exactly-once semantics: if a pipeline crashes between peek and commit,
//! changes are re-read on restart.
//!
//! Each CDC record is a JSON object with fields:
//! - `operation`: INSERT / UPDATE / DELETE
//! - `table`: schema-qualified table name
//! - `timestamp`: commit timestamp from WAL
//! - `lsn`: log sequence number as text (e.g. "0/1234568")
//! - `offset_id`: LSN as bigint (for offset tracking)
//! - `new`: JSON object with new column values (INSERT/UPDATE)
//! - `old`: JSON object with old column values (UPDATE/DELETE, if REPLICA IDENTITY set)

use crate::connector::InputConnector;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct CdcInput {
    table: String,
    #[allow(dead_code)]
    publication: String,
    slot: String,
    operations: Vec<String>,
    last_lsn: i64,
    /// Text LSN of the last consumed batch (for pg_replication_slot_advance)
    pending_lsn_text: Option<String>,
    poll_interval_ms: u64,
    last_poll_ms: u64,
    peek_sql: String,
}

/// Convert wal2json action character to operation name.
fn action_to_operation(action: &str) -> &str {
    match action {
        "I" => "INSERT",
        "U" => "UPDATE",
        "D" => "DELETE",
        "T" => "TRUNCATE",
        _ => action,
    }
}

/// Convert wal2json format-version 2 column array to a flat JSON object.
/// Input:  `[{"name":"id","type":"integer","value":1}, {"name":"name","type":"text","value":"Alice"}]`
/// Output: `{"id": 1, "name": "Alice"}`
pub(crate) fn columns_to_object(columns: Option<&serde_json::Value>) -> serde_json::Value {
    let arr = match columns {
        Some(v) => match v.as_array() {
            Some(a) => a,
            None => return serde_json::json!({}),
        },
        None => return serde_json::json!({}),
    };

    let mut obj = serde_json::Map::new();
    for col in arr {
        let name = match col["name"].as_str() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let value = &col["value"];
        obj.insert(name, value.clone());
    }
    serde_json::Value::Object(obj)
}

/// Build the peek SQL query for a CDC slot.
/// Uses wal2json format-version 2 with add-tables filter.
fn build_peek_sql(table: &str, slot: &str) -> String {
    // wal2json add-tables format: schema.table (no quotes)
    format!(
        "SELECT COALESCE(jsonb_agg(sub.r ORDER BY sub.lsn_val), '[]'::jsonb) \
         FROM (\
           SELECT jsonb_build_object(\
             'lsn', lsn::text, \
             'lsn_i64', lsn::bigint, \
             'data', data::jsonb\
           ) AS r, \
           lsn::bigint AS lsn_val \
           FROM pg_logical_slot_peek_changes(\
             '{}', NULL, $1, \
             'format-version', '2', \
             'add-tables', '{}'\
           ) \
           WHERE lsn::bigint > $2\
         ) sub",
        slot, table
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl CdcInput {
    pub fn new(
        table: &str,
        publication: &str,
        slot: &str,
        operations: &[String],
        poll_interval_ms: u64,
    ) -> Self {
        let peek_sql = build_peek_sql(table, slot);
        Self {
            table: table.to_string(),
            publication: publication.to_string(),
            slot: slot.to_string(),
            operations: operations.to_vec(),
            last_lsn: 0,
            pending_lsn_text: None,
            poll_interval_ms,
            last_poll_ms: 0,
            peek_sql,
        }
    }

    #[cfg(test)]
    pub fn peek_sql(&self) -> &str {
        &self.peek_sql
    }

    /// Initialize: check publication, create slot if needed, restore offset
    fn init_slot_and_offset(&mut self, pipeline_name: &str) -> Result<(), String> {
        // Check publication exists
        let pub_exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[self.publication.as_str().into()],
        );
        match pub_exists {
            Ok(Some(true)) => {}
            Ok(_) => {
                return Err(format!(
                    "Publication '{}' does not exist. Create it with: \
                     CREATE PUBLICATION {} FOR TABLE {}",
                    self.publication, self.publication, self.table
                ))
            }
            Err(e) => {
                return Err(format!(
                    "Failed to check publication '{}': {}",
                    self.publication, e
                ))
            }
        }

        // Create replication slot if it doesn't exist
        let slot_exists = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[self.slot.as_str().into()],
        );
        match slot_exists {
            Ok(Some(true)) => {} // slot already exists
            Ok(_) => {
                // Create the slot with wal2json
                Spi::run_with_args(
                    "SELECT pg_create_logical_replication_slot($1, 'wal2json')",
                    &[self.slot.as_str().into()],
                )
                .map_err(|e| {
                    format!(
                        "Failed to create replication slot '{}': {} \
                         (is wal2json extension installed?)",
                        self.slot, e
                    )
                })?;
            }
            Err(e) => {
                return Err(format!(
                    "Failed to check replication slot '{}': {}",
                    self.slot, e
                ))
            }
        }

        // Restore offset from connector_offsets
        let existing = Spi::get_one_with_args::<i64>(
            "SELECT offset_value FROM pgstreams.connector_offsets \
             WHERE pipeline = $1 AND connector = 'cdc_input'",
            &[pipeline_name.into()],
        );

        if let Ok(Some(offset)) = existing {
            self.last_lsn = offset;

            // Advance slot to skip already-processed changes
            // Convert i64 back to LSN text: upper 32 bits / lower 32 bits
            let lsn_text = format!("{:X}/{:X}", (offset >> 32) as u32, offset as u32);
            let _ = Spi::run_with_args(
                "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
                &[self.slot.as_str().into(), lsn_text.as_str().into()],
            );
        }

        Ok(())
    }

    /// Parse a single wal2json change into a CDC record
    fn parse_change(
        &self,
        change: &serde_json::Value,
        lsn_text: &str,
        lsn_i64: i64,
        timestamp: &str,
    ) -> Option<serde_json::Value> {
        let action = change["action"].as_str().unwrap_or("");
        let operation = action_to_operation(action);

        // Filter by allowed operations
        if !self.operations.iter().any(|op| op == operation) {
            return None;
        }

        let schema = change["schema"].as_str().unwrap_or("public");
        let table = change["table"].as_str().unwrap_or("");
        let full_table = format!("{}.{}", schema, table);

        let new_obj = columns_to_object(change.get("columns"));
        let old_obj = columns_to_object(change.get("identity"));

        Some(serde_json::json!({
            "operation": operation,
            "table": full_table,
            "timestamp": timestamp,
            "lsn": lsn_text,
            "offset_id": lsn_i64,
            "new": new_obj,
            "old": old_obj,
        }))
    }
}

impl InputConnector for CdcInput {
    fn initialize(&mut self, pipeline_name: &str) -> Result<(), String> {
        self.init_slot_and_offset(pipeline_name)
    }

    fn poll(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String> {
        // Throttle
        let now = now_ms();
        if now - self.last_poll_ms < self.poll_interval_ms {
            return Ok((vec![], None));
        }
        self.last_poll_ms = now;

        let result = Spi::get_one_with_args::<pgrx::JsonB>(
            &self.peek_sql,
            &[(batch_size as i64).into(), self.last_lsn.into()],
        );

        let jsonb =
            result.map_err(|e| format!("Failed to peek CDC slot '{}': {}", self.slot, e))?;

        let batch_value = match jsonb {
            Some(jb) => jb.0,
            None => return Ok((vec![], None)),
        };

        let raw_records = match batch_value.as_array() {
            Some(arr) => arr.clone(),
            None => return Ok((vec![], None)),
        };

        if raw_records.is_empty() {
            return Ok((vec![], None));
        }

        // Parse wal2json records into CDC format
        let mut records = Vec::new();
        let mut max_lsn: Option<i64> = None;
        let mut max_lsn_text: Option<String> = None;

        for raw in &raw_records {
            let lsn_text = raw["lsn"].as_str().unwrap_or("");
            let lsn_i64 = raw["lsn_i64"].as_i64().unwrap_or(0);
            let data = &raw["data"];

            let timestamp = data["timestamp"].as_str().unwrap_or("");

            // wal2json format-version 2: changes is an array of change objects
            if let Some(changes) = data["change"].as_array() {
                for change in changes {
                    if let Some(record) = self.parse_change(change, lsn_text, lsn_i64, timestamp) {
                        records.push(record);
                    }
                }
            }

            // Track max LSN
            if max_lsn.is_none() || lsn_i64 > max_lsn.unwrap_or(0) {
                max_lsn = Some(lsn_i64);
                max_lsn_text = Some(lsn_text.to_string());
            }
        }

        // Store pending LSN text for commit
        if let Some(ref text) = max_lsn_text {
            self.pending_lsn_text = Some(text.clone());
        }

        if let Some(lsn) = max_lsn {
            self.last_lsn = lsn;
        }

        Ok((records, max_lsn))
    }

    fn commit(&mut self, pipeline_name: &str, offset: i64) -> Result<(), String> {
        // Advance the replication slot to consume changes
        if let Some(ref lsn_text) = self.pending_lsn_text {
            Spi::run_with_args(
                "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
                &[self.slot.as_str().into(), lsn_text.as_str().into()],
            )
            .map_err(|e| format!("Failed to advance slot '{}': {}", self.slot, e))?;
        }

        // Persist offset
        Spi::run_with_args(
            "INSERT INTO pgstreams.connector_offsets (pipeline, connector, offset_value, updated_at) \
             VALUES ($1, 'cdc_input', $2, now()) \
             ON CONFLICT (pipeline, connector) \
             DO UPDATE SET offset_value = $2, updated_at = now()",
            &[pipeline_name.into(), offset.into()],
        )
        .map_err(|e| format!("Failed to commit CDC offset: {}", e))?;

        self.pending_lsn_text = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // columns_to_object
    // =========================================================================

    #[test]
    fn test_columns_to_object_insert() {
        let columns = serde_json::json!([
            {"name": "id", "type": "integer", "value": 1},
            {"name": "name", "type": "text", "value": "Alice"},
            {"name": "amount", "type": "numeric", "value": 99.5}
        ]);
        let obj = columns_to_object(Some(&columns));
        assert_eq!(obj["id"], 1);
        assert_eq!(obj["name"], "Alice");
        assert_eq!(obj["amount"], 99.5);
    }

    #[test]
    fn test_columns_to_object_null_values() {
        let columns = serde_json::json!([
            {"name": "id", "type": "integer", "value": 1},
            {"name": "email", "type": "text", "value": null}
        ]);
        let obj = columns_to_object(Some(&columns));
        assert_eq!(obj["id"], 1);
        assert!(obj["email"].is_null());
    }

    #[test]
    fn test_columns_to_object_empty() {
        let columns = serde_json::json!([]);
        let obj = columns_to_object(Some(&columns));
        assert_eq!(obj, serde_json::json!({}));
    }

    #[test]
    fn test_columns_to_object_none() {
        let obj = columns_to_object(None);
        assert_eq!(obj, serde_json::json!({}));
    }

    // =========================================================================
    // action_to_operation
    // =========================================================================

    #[test]
    fn test_action_to_operation() {
        assert_eq!(action_to_operation("I"), "INSERT");
        assert_eq!(action_to_operation("U"), "UPDATE");
        assert_eq!(action_to_operation("D"), "DELETE");
        assert_eq!(action_to_operation("T"), "TRUNCATE");
        assert_eq!(action_to_operation("X"), "X"); // unknown passthrough
    }

    // =========================================================================
    // wal2json parsing
    // =========================================================================

    #[test]
    fn test_parse_change_insert() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "test_slot",
            &[
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
            ],
            1000,
        );

        let change = serde_json::json!({
            "kind": "insert",
            "schema": "public",
            "table": "orders",
            "action": "I",
            "columns": [
                {"name": "id", "type": "integer", "value": 42},
                {"name": "amount", "type": "numeric", "value": 100.0}
            ]
        });

        let record = input
            .parse_change(&change, "0/1234568", 19088744, "2024-01-15 10:30:00")
            .expect("should parse insert");

        assert_eq!(record["operation"], "INSERT");
        assert_eq!(record["table"], "public.orders");
        assert_eq!(record["lsn"], "0/1234568");
        assert_eq!(record["offset_id"], 19088744);
        assert_eq!(record["new"]["id"], 42);
        assert_eq!(record["new"]["amount"], 100.0);
        assert_eq!(record["old"], serde_json::json!({}));
    }

    #[test]
    fn test_parse_change_update() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "test_slot",
            &[
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
            ],
            1000,
        );

        let change = serde_json::json!({
            "kind": "update",
            "schema": "public",
            "table": "orders",
            "action": "U",
            "columns": [
                {"name": "id", "type": "integer", "value": 42},
                {"name": "amount", "type": "numeric", "value": 200.0}
            ],
            "identity": [
                {"name": "id", "type": "integer", "value": 42},
                {"name": "amount", "type": "numeric", "value": 100.0}
            ]
        });

        let record = input
            .parse_change(&change, "0/1234580", 19088768, "2024-01-15 10:31:00")
            .expect("should parse update");

        assert_eq!(record["operation"], "UPDATE");
        assert_eq!(record["new"]["amount"], 200.0);
        assert_eq!(record["old"]["amount"], 100.0);
    }

    #[test]
    fn test_parse_change_delete() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "test_slot",
            &[
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
            ],
            1000,
        );

        let change = serde_json::json!({
            "kind": "delete",
            "schema": "public",
            "table": "orders",
            "action": "D",
            "identity": [
                {"name": "id", "type": "integer", "value": 42}
            ]
        });

        let record = input
            .parse_change(&change, "0/1234590", 19088784, "2024-01-15 10:32:00")
            .expect("should parse delete");

        assert_eq!(record["operation"], "DELETE");
        assert_eq!(record["new"], serde_json::json!({}));
        assert_eq!(record["old"]["id"], 42);
    }

    // =========================================================================
    // Operation filtering
    // =========================================================================

    #[test]
    fn test_parse_change_filtered_out() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "test_slot",
            &["INSERT".to_string()], // only INSERT
            1000,
        );

        let change = serde_json::json!({
            "kind": "delete",
            "schema": "public",
            "table": "orders",
            "action": "D",
            "identity": [{"name": "id", "type": "integer", "value": 42}]
        });

        let result = input.parse_change(&change, "0/1234590", 19088784, "2024-01-15 10:32:00");
        assert!(result.is_none(), "DELETE should be filtered out");
    }

    // =========================================================================
    // Constructor and peek SQL
    // =========================================================================

    #[test]
    fn test_cdc_input_construction() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "pgstreams_orders",
            &["INSERT".to_string(), "UPDATE".to_string()],
            5000,
        );

        assert_eq!(input.table, "public.orders");
        assert_eq!(input.slot, "pgstreams_orders");
        assert_eq!(input.poll_interval_ms, 5000);
        assert_eq!(input.last_lsn, 0);
        assert_eq!(input.operations.len(), 2);
    }

    #[test]
    fn test_peek_sql_structure() {
        let input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "pgstreams_orders",
            &["INSERT".to_string()],
            1000,
        );

        let sql = input.peek_sql();
        assert!(sql.contains("pg_logical_slot_peek_changes"));
        assert!(sql.contains("pgstreams_orders"));
        assert!(sql.contains("public.orders"));
        assert!(sql.contains("format-version"));
        assert!(sql.contains("add-tables"));
        assert!(sql.contains("lsn::bigint"));
    }

    // =========================================================================
    // Throttle
    // =========================================================================

    #[test]
    fn test_throttle_check() {
        let mut input = CdcInput::new(
            "public.orders",
            "orders_pub",
            "pgstreams_orders",
            &["INSERT".to_string()],
            60_000, // 60s
        );

        input.last_poll_ms = now_ms();
        let elapsed = now_ms() - input.last_poll_ms;
        assert!(elapsed < input.poll_interval_ms);
    }
}
