//! MQTT-specific storage queries
//!
//! This module provides the high-level storage operations for MQTT:
//! - Managing subscriptions
//! - Reading retained messages and the outbox
//! - Session management
//! - Topic wildcard matching

use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};
use crate::server::fanout;

/// MQTT storage client for database operations
#[derive(Clone)]
pub struct MqttStorageClient {
    bridge: SpiBridge,
}

/// A subscription record from the database
#[derive(Debug, Clone)]
pub struct Subscription {
    pub client_id: String,
    pub topic_filter: String,
    pub qos: i16,
    /// Forward the RETAIN flag as published instead of clearing it
    pub retain_as_published: bool,
    /// Do not deliver the client's own publishes (No Local)
    pub no_local: bool,
    /// When it was made, in microseconds since the epoch: a message reaching a
    /// worker through the outbox goes only to subscriptions as old as itself
    pub created_micros: i64,
}

/// A retained message from the database
#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: i16,
    /// PUBLISH property block (codec::encode_publish_properties)
    pub properties: Vec<u8>,
}

/// Every subscription of every client, in subscription_from_row's columns
pub const SUBSCRIPTIONS_SQL: &str =
    "SELECT client_id, topic_filter, qos, retain_as_published, no_local,
            (extract(epoch FROM created_at) * 1000000)::bigint
     FROM pgmqtt.subscriptions";

/// Columns of the subscription queries, in subscription_from_row's order
pub const SUBSCRIPTION_COLUMNS: [ColumnType; 6] = [
    ColumnType::Text,
    ColumnType::Text,
    ColumnType::Int32,
    ColumnType::Bool,
    ColumnType::Bool,
    ColumnType::Int64,
];

impl MqttStorageClient {
    /// Create a new MQTT storage client
    pub fn new(bridge: SpiBridge) -> Self {
        Self { bridge }
    }

    /// Add or update a subscription for a client. Returns true when the
    /// subscription is new, false when it replaced an existing one (MQTT 5
    /// retain handling 1 sends retained messages only for new ones).
    pub async fn add_subscription(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: i16,
        retain_as_published: bool,
        no_local: bool,
    ) -> Result<bool, SpiError> {
        let sql = "INSERT INTO pgmqtt.subscriptions
                       (client_id, topic_filter, qos, retain_as_published, no_local)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (client_id, topic_filter)
                   DO UPDATE SET qos = EXCLUDED.qos,
                                 retain_as_published = EXCLUDED.retain_as_published,
                                 no_local = EXCLUDED.no_local
                   RETURNING (xmax = 0)";

        let result = self
            .bridge
            .query(
                sql,
                vec![
                    SpiParam::Text(Some(client_id.to_string())),
                    SpiParam::Text(Some(topic_filter.to_string())),
                    SpiParam::Int4(Some(qos as i32)),
                    SpiParam::Bool(Some(retain_as_published)),
                    SpiParam::Bool(Some(no_local)),
                ],
                vec![ColumnType::Bool],
            )
            .await?;
        Ok(result
            .first()
            .and_then(|row| row.get_bool(0))
            .unwrap_or(true))
    }

    /// Retained messages whose topic matches a filter
    ///
    /// Filtered in Rust (`topic_matches`), like subscriptions.
    pub async fn get_matching_retained(
        &self,
        topic_filter: &str,
    ) -> Result<Vec<RetainedMessage>, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT topic, payload, qos, properties FROM pgmqtt.retained ORDER BY topic",
                vec![],
                vec![
                    ColumnType::Text,
                    ColumnType::Bytea,
                    ColumnType::Int32,
                    ColumnType::Bytea,
                ],
            )
            .await?;

        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                let topic = row.get_string(0)?;
                topic_matches(topic_filter, &topic).then(|| RetainedMessage {
                    topic,
                    payload: row.get_bytes(1).unwrap_or_default(),
                    qos: row.get_i32(2).unwrap_or(0) as i16,
                    properties: row.get_bytes(3).unwrap_or_default(),
                })
            })
            .collect())
    }

    /// Remove a subscription for a client
    pub async fn remove_subscription(
        &self,
        client_id: &str,
        topic_filter: &str,
    ) -> Result<(), SpiError> {
        let sql = "DELETE FROM pgmqtt.subscriptions
                   WHERE client_id = $1 AND topic_filter = $2";

        self.bridge
            .execute(
                sql,
                vec![
                    SpiParam::Text(Some(client_id.to_string())),
                    SpiParam::Text(Some(topic_filter.to_string())),
                ],
            )
            .await
    }

    /// Remove all subscriptions for a client
    pub async fn remove_all_subscriptions(&self, client_id: &str) -> Result<(), SpiError> {
        let sql = "DELETE FROM pgmqtt.subscriptions WHERE client_id = $1";

        self.bridge
            .execute(sql, vec![SpiParam::Text(Some(client_id.to_string()))])
            .await
    }

    /// Get every subscription of every client
    pub async fn get_all_subscriptions(&self) -> Result<Vec<Subscription>, SpiError> {
        let result = self
            .bridge
            .query(SUBSCRIPTIONS_SQL, vec![], SUBSCRIPTION_COLUMNS.to_vec())
            .await?;

        Ok(result.rows.iter().map(subscription_from_row).collect())
    }

    /// Read outbox rows of transactions that finished since `cursor`, skipping
    /// this worker's own. Returns the new horizon and the rows.
    pub async fn poll_outbox(
        &self,
        cursor: Option<u64>,
        worker_id: i32,
    ) -> Result<(u64, Vec<fanout::OutboxRow>), SpiError> {
        let result = self
            .bridge
            .query(
                fanout::POLL_SQL,
                vec![
                    SpiParam::Text(cursor.map(|c| c.to_string())),
                    SpiParam::Int4(Some(worker_id)),
                ],
                vec![
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Bytea,
                    ColumnType::Int32,
                    ColumnType::Bool,
                    ColumnType::Bytea,
                    ColumnType::Text,
                    ColumnType::Int64,
                ],
            )
            .await?;
        let rows = result
            .rows
            .iter()
            .map(|r| fanout::PollRow {
                horizon: r.get_string(0),
                topic: r.get_string(1),
                payload: r.get_bytes(2),
                qos: r.get_i32(3).ok(),
                retain: r.get_bool(4),
                properties: r.get_bytes(5),
                origin_client: r.get_string(6),
                created_micros: r.get_i64(7).ok(),
            })
            .collect();
        fanout::parse_poll(rows).map_err(SpiError::TypeConversion)
    }

    /// Delete outbox rows below a horizon every worker has passed
    pub async fn cleanup_outbox(&self, bound: u64) -> Result<(), SpiError> {
        self.bridge
            .execute(
                fanout::CLEANUP_SQL,
                vec![SpiParam::Text(Some(bound.to_string()))],
            )
            .await
    }

    /// Get all subscriptions for a specific client
    pub async fn get_client_subscriptions(
        &self,
        client_id: &str,
    ) -> Result<Vec<Subscription>, SpiError> {
        let sql = "SELECT client_id, topic_filter, qos, retain_as_published, no_local,
                          (extract(epoch FROM created_at) * 1000000)::bigint
                   FROM pgmqtt.subscriptions WHERE client_id = $1";

        let result = self
            .bridge
            .query(
                sql,
                vec![SpiParam::Text(Some(client_id.to_string()))],
                SUBSCRIPTION_COLUMNS.to_vec(),
            )
            .await?;

        Ok(result.rows.iter().map(subscription_from_row).collect())
    }

    /// Create or update a client session
    pub async fn upsert_session(&self, client_id: &str) -> Result<(), SpiError> {
        let sql = "INSERT INTO pgmqtt.sessions (client_id, connected_at, last_seen)
                   VALUES ($1, NOW(), NOW())
                   ON CONFLICT (client_id)
                   DO UPDATE SET connected_at = NOW(), last_seen = NOW()";

        self.bridge
            .execute(sql, vec![SpiParam::Text(Some(client_id.to_string()))])
            .await
    }

    /// Update last seen timestamp for a session
    pub async fn update_session_heartbeat(&self, client_id: &str) -> Result<(), SpiError> {
        let sql = "UPDATE pgmqtt.sessions SET last_seen = NOW() WHERE client_id = $1";

        self.bridge
            .execute(sql, vec![SpiParam::Text(Some(client_id.to_string()))])
            .await
    }

    /// Remove a client session
    pub async fn remove_session(&self, client_id: &str) -> Result<(), SpiError> {
        let sql = "DELETE FROM pgmqtt.sessions WHERE client_id = $1";

        self.bridge
            .execute(sql, vec![SpiParam::Text(Some(client_id.to_string()))])
            .await
    }
}

/// A subscription from a SUBSCRIPTIONS_SQL row
pub fn subscription_from_row(row: &super::SpiRow) -> Subscription {
    Subscription {
        client_id: row.get_string(0).unwrap_or_default(),
        topic_filter: row.get_string(1).unwrap_or_default(),
        qos: row.get_i32(2).unwrap_or(0) as i16,
        retain_as_published: row.get_bool(3).unwrap_or(false),
        no_local: row.get_bool(4).unwrap_or(false),
        created_micros: row.get_i64(5).unwrap_or(0),
    }
}

/// Check if a topic matches an MQTT topic filter
///
/// MQTT topic filter wildcards:
/// - `+` matches exactly one topic level
/// - `#` matches zero or more levels (must be the last character)
///
/// Examples:
/// - `sensors/+/temperature` matches `sensors/room1/temperature`
/// - `sensors/#` matches `sensors`, `sensors/room1`, `sensors/room1/temp`
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    // A filter starting with a wildcard does not match a topic starting
    // with $ (MQTT 5 4.7.2)
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }

    let filter_parts: Vec<&str> = filter.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();

    let mut fi = 0;
    let mut ti = 0;

    while fi < filter_parts.len() {
        let fp = filter_parts[fi];

        if fp == "#" {
            // # matches everything remaining (including nothing)
            return true;
        }

        if ti >= topic_parts.len() {
            // Topic is shorter than filter (and no # to match rest)
            return false;
        }

        if fp == "+" {
            // + matches exactly one level
            fi += 1;
            ti += 1;
        } else if fp == topic_parts[ti] {
            // Exact match
            fi += 1;
            ti += 1;
        } else {
            // No match
            return false;
        }
    }

    // Must have consumed entire topic
    ti == topic_parts.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_matches_exact() {
        assert!(topic_matches(
            "sensors/room1/temperature",
            "sensors/room1/temperature"
        ));
        assert!(!topic_matches(
            "sensors/room1/temperature",
            "sensors/room2/temperature"
        ));
    }

    #[test]
    fn test_topic_matches_single_level_wildcard() {
        assert!(topic_matches(
            "sensors/+/temperature",
            "sensors/room1/temperature"
        ));
        assert!(topic_matches(
            "sensors/+/temperature",
            "sensors/room2/temperature"
        ));
        assert!(!topic_matches(
            "sensors/+/temperature",
            "sensors/room1/humidity"
        ));
        assert!(!topic_matches(
            "sensors/+/temperature",
            "sensors/temperature"
        ));
        assert!(!topic_matches(
            "sensors/+/temperature",
            "sensors/room1/sub/temperature"
        ));
    }

    #[test]
    fn test_topic_matches_multi_level_wildcard() {
        assert!(topic_matches("sensors/#", "sensors"));
        assert!(topic_matches("sensors/#", "sensors/room1"));
        assert!(topic_matches("sensors/#", "sensors/room1/temperature"));
        assert!(topic_matches("sensors/#", "sensors/room1/sub/deep/value"));
        assert!(!topic_matches("sensors/#", "other/room1"));
    }

    #[test]
    fn test_topic_matches_combined_wildcards() {
        assert!(topic_matches("sensors/+/#", "sensors/room1"));
        assert!(topic_matches("sensors/+/#", "sensors/room1/temperature"));
        assert!(topic_matches("sensors/+/#", "sensors/room1/sub/deep"));
        assert!(!topic_matches("sensors/+/#", "sensors"));
    }

    #[test]
    fn test_topic_matches_root_wildcard() {
        assert!(topic_matches("#", "anything"));
        assert!(topic_matches("#", "sensors/room1/temperature"));
        assert!(topic_matches("#", ""));
    }

    #[test]
    fn test_wildcards_do_not_match_dollar_topics() {
        assert!(!topic_matches("#", "$SYS/broker"));
        assert!(!topic_matches("+/broker", "$SYS/broker"));
        assert!(topic_matches("$SYS/#", "$SYS/broker"));
    }

    #[test]
    fn test_topic_matches_edge_cases() {
        assert!(topic_matches("+", "single"));
        assert!(!topic_matches("+", "multi/level"));
        assert!(topic_matches("+/+", "a/b"));
        assert!(!topic_matches("+/+", "a/b/c"));
    }
}
