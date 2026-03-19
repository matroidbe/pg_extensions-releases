//! MQTT packet handlers
//!
//! This module contains the business logic for handling each MQTT packet type.

use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use super::codec::*;
use super::types::*;
use crate::storage::source_insert::MqttSourceInserter;
use crate::storage::MqttStorageClient;
use crate::validation::{MqttSchemaValidator, ValidationMode, ValidationResult};

/// Client session state
#[derive(Debug)]
pub struct ClientSession {
    pub client_id: String,
    pub connected: bool,
    pub keep_alive: u16,
}

/// Shared state for message routing
pub struct MqttBrokerState {
    /// Connected clients: client_id -> sender for outgoing messages
    pub clients: RwLock<HashMap<String, broadcast::Sender<BytesMut>>>,
}

impl MqttBrokerState {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Register a client connection
    pub async fn register_client(&self, client_id: &str) -> broadcast::Receiver<BytesMut> {
        let (tx, rx) = broadcast::channel(100);
        let mut clients = self.clients.write().await;
        clients.insert(client_id.to_string(), tx);
        rx
    }

    /// Unregister a client connection
    pub async fn unregister_client(&self, client_id: &str) {
        let mut clients = self.clients.write().await;
        clients.remove(client_id);
    }

    /// Send a message to a specific client
    pub async fn send_to_client(&self, client_id: &str, data: BytesMut) -> bool {
        let clients = self.clients.read().await;
        if let Some(tx) = clients.get(client_id) {
            tx.send(data).is_ok()
        } else {
            false
        }
    }
}

impl Default for MqttBrokerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle a CONNECT packet
pub async fn handle_connect(
    packet: &ConnectPacket,
    storage: &MqttStorageClient,
    _state: &Arc<MqttBrokerState>,
) -> Result<(BytesMut, ClientSession), BytesMut> {
    // Validate the connection
    if packet.protocol_version != 5 {
        let connack = ConnackPacket {
            session_present: false,
            reason_code: ReasonCode::UnsupportedProtocolVersion,
            properties: ConnackProperties::default(),
        };
        return Err(encode_connack(&connack));
    }

    // Generate client ID if empty
    let client_id = if packet.client_id.is_empty() {
        format!(
            "pg_mqtt_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
    } else {
        packet.client_id.clone()
    };

    // Create session
    if let Err(e) = storage.upsert_session(&client_id).await {
        pgrx::warning!("Failed to create session: {}", e);
    }

    // Clean start: remove existing subscriptions if requested
    if packet.flags.clean_start {
        if let Err(e) = storage.remove_all_subscriptions(&client_id).await {
            pgrx::warning!("Failed to clean subscriptions: {}", e);
        }
    }

    let session = ClientSession {
        client_id: client_id.clone(),
        connected: true,
        keep_alive: packet.keep_alive,
    };

    // Build CONNACK response with our capabilities
    let properties = ConnackProperties {
        maximum_qos: Some(0),          // Only QoS 0 supported
        retain_available: Some(false), // Retained messages not supported yet
        wildcard_subscription_available: Some(true),
        subscription_identifier_available: Some(false),
        shared_subscription_available: Some(false),
        // Assign client identifier if we generated one
        assigned_client_identifier: if packet.client_id.is_empty() {
            Some(client_id)
        } else {
            None
        },
        ..Default::default()
    };

    let connack = ConnackPacket {
        session_present: !packet.flags.clean_start,
        reason_code: ReasonCode::Success,
        properties,
    };

    Ok((encode_connack(&connack), session))
}

/// Handle a PUBLISH packet
///
/// Validates payloads against bound schemas and routes to backing tables for typed topics.
pub async fn handle_publish(
    packet: &PublishPacket,
    storage: &MqttStorageClient,
    state: &Arc<MqttBrokerState>,
    validator: &MqttSchemaValidator,
    inserter: &MqttSourceInserter,
) -> Result<Option<BytesMut>, BytesMut> {
    // Validate payload against schema if one is bound to this topic
    let mut source_table: Option<String> = None;
    match validator
        .validate_payload(&packet.topic, &packet.payload)
        .await
    {
        Ok((ValidationResult::Invalid(err), ValidationMode::Strict, _)) => {
            // STRICT mode: drop the message (QoS 0 has no error ack)
            eprintln!("pg_mqtt: rejecting publish to '{}': {}", packet.topic, err);
            return Ok(None);
        }
        Ok((ValidationResult::Invalid(err), ValidationMode::Log, info)) => {
            eprintln!(
                "pg_mqtt: schema violation on '{}' (LOG mode): {}",
                packet.topic, err
            );
            source_table = info.and_then(|i| i.source_table);
        }
        Ok((_, _, info)) => {
            // Valid or NoSchema — proceed
            source_table = info.and_then(|i| i.source_table);
        }
        Err(e) => {
            // Validation infrastructure error — log and proceed
            eprintln!(
                "pg_mqtt: schema validation error for '{}': {}",
                packet.topic, e
            );
        }
    }

    // Route to backing table if typed topic, otherwise to pgmqtt.messages
    if let Some(ref table) = source_table {
        // Parse payload as JSON and insert into backing table
        if let Ok(payload_str) = std::str::from_utf8(&packet.payload) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(payload_str) {
                if let Err(e) = inserter.insert_message(table, &json_value).await {
                    eprintln!(
                        "pg_mqtt: failed to insert into typed table '{}': {}",
                        table, e
                    );
                }
            }
        }
    } else {
        // Default: store in pgmqtt.messages
        if let Err(e) = storage
            .store_message(
                &packet.topic,
                Some(&packet.payload),
                packet.qos as i16,
                packet.retain,
            )
            .await
        {
            pgrx::warning!("Failed to store message: {}", e);
        }
    }

    // Find matching subscriptions and forward the message (regardless of storage path)
    match storage.get_matching_subscriptions(&packet.topic).await {
        Ok(subscriptions) => {
            let publish_packet = encode_publish(packet);
            for sub in subscriptions {
                state
                    .send_to_client(&sub.client_id, publish_packet.clone())
                    .await;
            }
        }
        Err(e) => {
            pgrx::warning!("Failed to get subscriptions: {}", e);
        }
    }

    // QoS 0 doesn't require acknowledgment
    Ok(None)
}

/// Handle a SUBSCRIBE packet
pub async fn handle_subscribe(
    packet: &SubscribePacket,
    client_id: &str,
    storage: &MqttStorageClient,
) -> BytesMut {
    let mut reason_codes = Vec::new();

    for sub in &packet.subscriptions {
        // Only support QoS 0 for now - Success (0x00) is same as GrantedQoS0
        let granted_qos = if sub.options.qos == QoS::AtMostOnce {
            ReasonCode::Success // GrantedQoS0
        } else {
            // Downgrade to QoS 0
            ReasonCode::Success // GrantedQoS0
        };

        // Store the subscription
        match storage
            .add_subscription(client_id, &sub.topic_filter, 0)
            .await
        {
            Ok(_) => reason_codes.push(granted_qos),
            Err(e) => {
                pgrx::warning!("Failed to add subscription: {}", e);
                reason_codes.push(ReasonCode::UnspecifiedError);
            }
        }
    }

    let suback = SubackPacket {
        packet_id: packet.packet_id,
        properties: SubackProperties::default(),
        reason_codes,
    };

    encode_suback(&suback)
}

/// Handle an UNSUBSCRIBE packet
pub async fn handle_unsubscribe(
    packet: &UnsubscribePacket,
    client_id: &str,
    storage: &MqttStorageClient,
) -> BytesMut {
    let mut reason_codes = Vec::new();

    for topic_filter in &packet.topic_filters {
        match storage.remove_subscription(client_id, topic_filter).await {
            Ok(_) => reason_codes.push(ReasonCode::Success),
            Err(e) => {
                pgrx::warning!("Failed to remove subscription: {}", e);
                reason_codes.push(ReasonCode::NoSubscriptionExisted);
            }
        }
    }

    let unsuback = UnsubackPacket {
        packet_id: packet.packet_id,
        properties: UnsubackProperties::default(),
        reason_codes,
    };

    encode_unsuback(&unsuback)
}

/// Handle a PINGREQ packet
pub fn handle_pingreq() -> BytesMut {
    encode_pingresp()
}

/// Handle a DISCONNECT packet
pub async fn handle_disconnect(
    _packet: &DisconnectPacket,
    client_id: &str,
    storage: &MqttStorageClient,
    state: &Arc<MqttBrokerState>,
) {
    // Remove session
    if let Err(e) = storage.remove_session(client_id).await {
        pgrx::warning!("Failed to remove session: {}", e);
    }

    // Unregister from broker state
    state.unregister_client(client_id).await;
}
