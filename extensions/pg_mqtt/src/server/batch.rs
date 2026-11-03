//! Batched storage of PUBLISHes, on the worker's main thread (see ingest.rs)
//!
//! One transaction per batch: the topics' schema bindings in one query, then
//! the messages (multi-row INSERTs), the typed-topic rows, the retained
//! messages, the outbox rows for the other workers, and the subscriptions to
//! deliver to. Every item is answered after the commit, so a subscriber never
//! gets a message that is not stored, and a QoS 1/2 publisher is acknowledged
//! only for what is.
//!
//! If the batch fails (one typed row that does not fit its table aborts the
//! transaction), it is written again item by item, so only that item fails.
//!
//! **IMPORTANT**: runs on the background worker's main thread (SPI).

use pg_spi::{execute_query_in_transaction, ColumnType, SpiError, SpiParam, SpiResult};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use super::ingest::{IngestItem, IngestOutcome};
use crate::protocol::codec::encode_publish_properties;
use crate::protocol::handlers::forwarded_properties;
use crate::protocol::{PublishPacket, ReasonCode};
use crate::storage::source_insert::{
    build_insert, columns_from_rows, table_columns_params, ColumnInfo, TABLE_COLUMNS_SQL,
    TABLE_COLUMNS_TYPES,
};
use crate::storage::spi_client::{subscription_from_row, SUBSCRIPTIONS_SQL, SUBSCRIPTION_COLUMNS};
use crate::validation::{
    check_payload, compile_schema, TopicBinding, ValidationMode, ValidationResult,
};

/// Most items written in one transaction
pub const MAX_BATCH: usize = 500;

/// Rows per multi-row INSERT into pgmqtt.messages (4 parameters each)
const MESSAGE_ROWS_PER_INSERT: usize = 250;

/// Rows per multi-row INSERT into pgmqtt.outbox (7 parameters each)
const OUTBOX_ROWS_PER_INSERT: usize = 100;

/// Where an item's message is stored
enum Target {
    /// pgmqtt.messages (no typed table bound)
    Messages,
    /// A typed topic's backing table
    Typed(String, serde_json::Value),
    /// Bound to a typed table but not JSON: accepted (LOG mode) and not stored
    Nowhere,
    /// Not accepted
    Rejected(ReasonCode),
}

/// Writes batches for one worker
pub struct BatchWriter {
    worker_id: i32,
    has_peers: bool,
    /// Compiled schemas by their definition text
    schemas: HashMap<String, Arc<jsonschema::Validator>>,
}

impl BatchWriter {
    pub fn new(worker_id: i32, worker_count: i32) -> Self {
        Self {
            worker_id,
            has_peers: worker_count > 1,
            schemas: HashMap::new(),
        }
    }

    /// Store a batch and answer every item
    pub fn write(&mut self, items: Vec<IngestItem>) {
        match self.in_transaction(&items) {
            Ok(outcomes) => {
                for (item, outcome) in items.into_iter().zip(outcomes) {
                    let _ = item.done.send(outcome);
                }
            }
            Err(e) if items.len() > 1 => {
                eprintln!(
                    "pg_mqtt worker {}: a batch of {} failed ({}); writing it item by item",
                    self.worker_id,
                    items.len(),
                    e
                );
                for item in items {
                    self.write(vec![item]);
                }
            }
            Err(e) => {
                let item = items.into_iter().next().expect("one item");
                eprintln!(
                    "pg_mqtt worker {}: failed to store a message on '{}': {}",
                    self.worker_id, item.packet.topic, e
                );
                let _ = item
                    .done
                    .send(IngestOutcome::rejected(ReasonCode::UnspecifiedError));
            }
        }
    }

    /// Run write_items in a transaction of its own, turning a PostgreSQL ERROR
    /// into an Err (as pg_spi does for a single request)
    fn in_transaction(&mut self, items: &[IngestItem]) -> Result<Vec<IngestOutcome>, String> {
        use pgrx::pg_sys::panic::CaughtError;
        use pgrx::pg_sys::PgTryBuilder;

        unsafe {
            pg_sys::SetCurrentStatementStartTimestamp();
            pg_sys::StartTransactionCommand();
            pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
        }

        let outcome = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| self.write_items(items)))
            .catch_others(|caught| {
                Err(match caught {
                    CaughtError::PostgresError(report)
                    | CaughtError::ErrorReport(report)
                    | CaughtError::RustPanic {
                        ereport: report, ..
                    } => report.message().to_string(),
                })
            })
            .execute();

        unsafe {
            if outcome.is_ok() {
                pg_sys::PopActiveSnapshot();
                pg_sys::CommitTransactionCommand();
            } else {
                pg_sys::AbortCurrentTransaction();
            }
        }
        outcome
    }

    /// Everything a batch writes, in the current transaction
    fn write_items(&mut self, items: &[IngestItem]) -> Result<Vec<IngestOutcome>, String> {
        let bindings = self.bindings(items)?;

        // Decide where each message goes
        let targets: Vec<Target> = items
            .iter()
            .map(|item| target_of(&item.packet, bindings.get(&item.packet.topic)))
            .collect();

        // Untyped messages: multi-row INSERTs
        let untyped: Vec<&PublishPacket> = items
            .iter()
            .zip(&targets)
            .filter(|(_, t)| matches!(t, Target::Messages))
            .map(|(item, _)| &item.packet)
            .collect();
        for chunk in untyped.chunks(MESSAGE_ROWS_PER_INSERT) {
            insert_messages(chunk)?;
        }

        // Typed messages: one INSERT each, columns read once per table
        let mut columns: HashMap<&str, Vec<ColumnInfo>> = HashMap::new();
        for target in &targets {
            if let Target::Typed(table, json) = target {
                if !columns.contains_key(table.as_str()) {
                    let rows = query(
                        TABLE_COLUMNS_SQL,
                        &table_columns_params(table),
                        &TABLE_COLUMNS_TYPES,
                    )?;
                    columns.insert(table, columns_from_rows(&rows.rows));
                }
                let (sql, params) = build_insert(table, &columns[table.as_str()], json)
                    .map_err(|e| e.to_string())?;
                query(&sql, &params, &[])?;
            }
        }

        // Accepted messages: retained and handed to the other workers
        let accepted: Vec<&IngestItem> = items
            .iter()
            .zip(&targets)
            .filter(|(_, t)| !matches!(t, Target::Rejected(_)))
            .map(|(item, _)| item)
            .collect();
        for item in accepted.iter().filter(|i| i.packet.retain) {
            retain(&item.packet)?;
        }
        if self.has_peers {
            for chunk in accepted.chunks(OUTBOX_ROWS_PER_INSERT) {
                self.enqueue_outbox(chunk)?;
            }
        }

        // Who to deliver to, as of this transaction
        let subscriptions = Arc::new(
            query(SUBSCRIPTIONS_SQL, &[], &SUBSCRIPTION_COLUMNS)?
                .rows
                .iter()
                .map(subscription_from_row)
                .collect::<Vec<_>>(),
        );

        Ok(targets
            .iter()
            .map(|target| match target {
                Target::Rejected(reason_code) => IngestOutcome::rejected(*reason_code),
                _ => IngestOutcome {
                    reason_code: ReasonCode::Success,
                    subscriptions: subscriptions.clone(),
                },
            })
            .collect())
    }

    /// The schema bindings of the batch's topics, in one query
    fn bindings(&mut self, items: &[IngestItem]) -> Result<HashMap<String, TopicBinding>, String> {
        let mut topics: Vec<&str> = items.iter().map(|i| i.packet.topic.as_str()).collect();
        topics.sort_unstable();
        topics.dedup();
        let rows = query(
            "SELECT ts.topic, s.schema_def::text, ts.source_table, ts.validation_mode
             FROM pgmqtt.topic_schemas ts
             JOIN pgmqtt.schemas s ON ts.schema_id = s.id
             WHERE ts.topic IN (SELECT jsonb_array_elements_text($1::jsonb))",
            &[SpiParam::Json(Some(serde_json::json!(topics)))],
            &[
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
                ColumnType::Text,
            ],
        )?;

        let mut bindings = HashMap::new();
        for row in &rows.rows {
            let (Some(topic), Some(schema_text)) = (row.get_string(0), row.get_string(1)) else {
                continue;
            };
            let validator = match self.schemas.get(&schema_text) {
                Some(v) => v.clone(),
                None => {
                    let v = Arc::new(compile_schema(&schema_text)?);
                    self.schemas.insert(schema_text, v.clone());
                    v
                }
            };
            bindings.insert(
                topic,
                TopicBinding {
                    validator,
                    source_table: row.get_string(2),
                    mode: ValidationMode::from_column(row.get_string(3).as_deref()),
                },
            );
        }
        Ok(bindings)
    }

    /// Outbox rows for subscribers on the other workers (design/pg_mqtt/fanout.md)
    fn enqueue_outbox(&self, items: &[&IngestItem]) -> Result<(), String> {
        let mut values = Vec::with_capacity(items.len());
        let mut params = Vec::with_capacity(items.len() * 7);
        for (i, item) in items.iter().enumerate() {
            let n = i * 7;
            values.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                n + 1,
                n + 2,
                n + 3,
                n + 4,
                n + 5,
                n + 6,
                n + 7
            ));
            let packet = &item.packet;
            params.extend([
                SpiParam::Text(Some(packet.topic.clone())),
                SpiParam::Bytea(Some(packet.payload.clone())),
                SpiParam::Int4(Some(self.worker_id)),
                SpiParam::Int4(Some(packet.qos as i32)),
                SpiParam::Bool(Some(packet.retain)),
                SpiParam::Bytea(Some(encode_publish_properties(&forwarded_properties(
                    &packet.properties,
                )))),
                SpiParam::Text(item.publisher.clone()),
            ]);
        }
        let sql = format!(
            "INSERT INTO pgmqtt.outbox
                 (topic, payload, origin_worker, qos, retain, properties, origin_client)
             VALUES {}",
            values.join(", ")
        );
        query(&sql, &params, &[]).map(|_| ())
    }
}

/// Where a message goes, given its topic's binding
fn target_of(packet: &PublishPacket, binding: Option<&TopicBinding>) -> Target {
    let Some(binding) = binding else {
        return Target::Messages;
    };
    let (result, json) = check_payload(&binding.validator, &packet.payload);
    if let ValidationResult::Invalid(err) = &result {
        if binding.mode == ValidationMode::Strict {
            // STRICT mode: drop the message; a QoS 1/2 publisher learns why
            eprintln!("pg_mqtt: rejecting publish to '{}': {}", packet.topic, err);
            return Target::Rejected(ReasonCode::PayloadFormatInvalid);
        }
        eprintln!(
            "pg_mqtt: schema violation on '{}' (LOG mode): {}",
            packet.topic, err
        );
    }
    match (&binding.source_table, json) {
        (Some(table), Some(json)) => Target::Typed(table.clone(), json),
        (Some(_), None) => Target::Nowhere,
        (None, _) => Target::Messages,
    }
}

/// Insert untyped messages into pgmqtt.messages with one statement
fn insert_messages(packets: &[&PublishPacket]) -> Result<(), String> {
    let mut values = Vec::with_capacity(packets.len());
    let mut params = Vec::with_capacity(packets.len() * 4);
    for (i, packet) in packets.iter().enumerate() {
        let n = i * 4;
        values.push(format!("(${}, ${}, ${}, ${})", n + 1, n + 2, n + 3, n + 4));
        params.extend([
            SpiParam::Text(Some(packet.topic.clone())),
            SpiParam::Bytea(Some(packet.payload.clone())),
            SpiParam::Int4(Some(packet.qos as i32)),
            SpiParam::Bool(Some(packet.retain)),
        ]);
    }
    let sql = format!(
        "INSERT INTO pgmqtt.messages (topic, payload, qos, retain) VALUES {}",
        values.join(", ")
    );
    query(&sql, &params, &[]).map(|_| ())
}

/// RETAIN: keep the message for future subscribers; an empty payload removes
/// the topic's retained message (MQTT 5 3.3.1.3)
fn retain(packet: &PublishPacket) -> Result<(), String> {
    if packet.payload.is_empty() {
        query(
            "DELETE FROM pgmqtt.retained WHERE topic = $1",
            &[SpiParam::Text(Some(packet.topic.clone()))],
            &[],
        )?;
        return Ok(());
    }
    query(
        "INSERT INTO pgmqtt.retained (topic, payload, qos, properties, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (topic)
         DO UPDATE SET payload = EXCLUDED.payload, qos = EXCLUDED.qos,
                       properties = EXCLUDED.properties, updated_at = EXCLUDED.updated_at",
        &[
            SpiParam::Text(Some(packet.topic.clone())),
            SpiParam::Bytea(Some(packet.payload.clone())),
            SpiParam::Int4(Some(packet.qos as i32)),
            SpiParam::Bytea(Some(encode_publish_properties(&forwarded_properties(
                &packet.properties,
            )))),
        ],
        &[],
    )
    .map(|_| ())
}

fn query(sql: &str, params: &[SpiParam], columns: &[ColumnType]) -> Result<SpiResult, String> {
    execute_query_in_transaction(sql, params, columns).map_err(|e: SpiError| e.to_string())
}
