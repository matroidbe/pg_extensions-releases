//! MQTT-specific storage queries
//!
//! This module provides the high-level storage operations for MQTT:
//! - Storing published messages
//! - Managing subscriptions
//! - Session management
//! - Topic wildcard matching

use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};

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
}

impl MqttStorageClient {
    /// Create a new MQTT storage client
    pub fn new(bridge: SpiBridge) -> Self {
        Self { bridge }
    }

    /// Store a published message
    pub async fn store_message(
        &self,
        topic: &str,
        payload: Option<&[u8]>,
        qos: i16,
        retain: bool,
    ) -> Result<i64, SpiError> {
        let sql = "INSERT INTO pgmqtt.messages (topic, payload, qos, retain)
                   VALUES ($1, $2, $3, $4)
                   RETURNING message_id";

        self.bridge
            .query_one_i64(
                sql,
                vec![
                    SpiParam::Text(Some(topic.to_string())),
                    SpiParam::Bytea(payload.map(|p| p.to_vec())),
                    SpiParam::Int4(Some(qos as i32)),
                    SpiParam::Text(Some(retain.to_string())),
                ],
            )
            .await
    }

    /// Add or update a subscription for a client
    pub async fn add_subscription(
        &self,
        client_id: &str,
        topic_filter: &str,
        qos: i16,
    ) -> Result<(), SpiError> {
        let sql = "INSERT INTO pgmqtt.subscriptions (client_id, topic_filter, qos)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (client_id, topic_filter)
                   DO UPDATE SET qos = EXCLUDED.qos";

        self.bridge
            .execute(
                sql,
                vec![
                    SpiParam::Text(Some(client_id.to_string())),
                    SpiParam::Text(Some(topic_filter.to_string())),
                    SpiParam::Int4(Some(qos as i32)),
                ],
            )
            .await
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

    /// Get all subscriptions that match a topic
    ///
    /// This uses PostgreSQL's pattern matching to handle MQTT wildcards:
    /// - `+` matches exactly one topic level
    /// - `#` matches zero or more topic levels (must be last)
    pub async fn get_matching_subscriptions(
        &self,
        topic: &str,
    ) -> Result<Vec<Subscription>, SpiError> {
        // For simplicity in MVP, we fetch all subscriptions and filter in Rust
        // This is not optimal for large subscription counts, but works for MVP
        let sql = "SELECT client_id, topic_filter, qos FROM pgmqtt.subscriptions";

        let result = self
            .bridge
            .query(
                sql,
                vec![],
                vec![ColumnType::Text, ColumnType::Text, ColumnType::Int32],
            )
            .await?;

        let mut matches = Vec::new();
        for row in &result.rows {
            let client_id = row.get_string(0).unwrap_or_default();
            let topic_filter = row.get_string(1).unwrap_or_default();
            let qos = row.get_i32(2).unwrap_or(0) as i16;

            if topic_matches(&topic_filter, topic) {
                matches.push(Subscription {
                    client_id,
                    topic_filter,
                    qos,
                });
            }
        }

        Ok(matches)
    }

    /// Get all subscriptions for a specific client
    pub async fn get_client_subscriptions(
        &self,
        client_id: &str,
    ) -> Result<Vec<Subscription>, SpiError> {
        let sql =
            "SELECT client_id, topic_filter, qos FROM pgmqtt.subscriptions WHERE client_id = $1";

        let result = self
            .bridge
            .query(
                sql,
                vec![SpiParam::Text(Some(client_id.to_string()))],
                vec![ColumnType::Text, ColumnType::Text, ColumnType::Int32],
            )
            .await?;

        let mut subs = Vec::new();
        for row in &result.rows {
            let client_id = row.get_string(0).unwrap_or_default();
            let topic_filter = row.get_string(1).unwrap_or_default();
            let qos = row.get_i32(2).unwrap_or(0) as i16;

            subs.push(Subscription {
                client_id,
                topic_filter,
                qos,
            });
        }

        Ok(subs)
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
    fn test_topic_matches_edge_cases() {
        assert!(topic_matches("+", "single"));
        assert!(!topic_matches("+", "multi/level"));
        assert!(topic_matches("+/+", "a/b"));
        assert!(!topic_matches("+/+", "a/b/c"));
    }
}
