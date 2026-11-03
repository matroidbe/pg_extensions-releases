//! MQTT packet handlers
//!
//! This module contains the business logic for handling each MQTT packet type.

use bytes::BytesMut;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::client_queue::{ClientQueue, Pushed, DEFAULT_MAX_QUEUED};
use super::codec::*;
use super::types::*;
use crate::server::ingest::{IngestOutcome, IngestQueue};
use crate::storage::spi_client::{topic_matches, Subscription};
use crate::storage::MqttStorageClient;

/// QoS 2 messages a client may have sent us and not yet released (PUBREL):
/// the Receive Maximum this broker announces in CONNACK
pub const SERVER_RECEIVE_MAXIMUM: u16 = 1024;

/// A Last Will: published by the broker when the connection ends without a
/// DISCONNECT (or with DISCONNECT reason 0x04).
///
/// The Will Delay Interval is not kept: a Will goes out at the end of the delay
/// or of the session, whichever comes first (MQTT 5 3.1.3.2.2), and sessions
/// here end with the connection.
#[derive(Debug, Clone)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    /// Properties the Will is published with (user properties, content type …)
    pub properties: PublishProperties,
}

/// Client session state
#[derive(Debug)]
pub struct ClientSession {
    pub client_id: String,
    /// MQTT 3.1.1 or 5, from CONNECT
    pub version: ProtocolVersion,
    pub connected: bool,
    pub keep_alive: u16,
    /// Most unacknowledged QoS 1 and 2 messages the client accepts (Receive
    /// Maximum)
    pub receive_maximum: u16,
    /// Largest packet the client accepts (Maximum Packet Size)
    pub maximum_packet_size: Option<u32>,
    /// Which connection of this client id holds the session
    /// (MqttBrokerState::register_client)
    pub generation: u64,
    pub will: Option<Will>,
}

/// A message on its way to one connected client. The connection task assigns
/// the packet id and encodes it.
#[derive(Debug, Clone)]
pub struct Delivery {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub properties: PublishProperties,
}

/// The connection that currently holds a client id
struct ClientHandle {
    generation: u64,
    queue: Arc<ClientQueue>,
}

/// Shared state for message routing
pub struct MqttBrokerState {
    /// Connected clients: client_id -> the connection holding it
    clients: RwLock<HashMap<String, ClientHandle>>,
    next_generation: AtomicU64,
    /// This worker's id, recorded as the origin of the messages it enqueues
    pub worker_id: i32,
    /// Whether other workers exist that need this worker's publishes
    pub has_peers: bool,
    /// Messages one client may have waiting (pg_mqtt.max_queued_messages)
    max_queued: usize,
}

impl MqttBrokerState {
    /// State of a single worker with no peers
    pub fn new() -> Self {
        Self::for_worker(0, 1, DEFAULT_MAX_QUEUED)
    }

    /// State of worker `worker_id` out of `worker_count`, queueing at most
    /// `max_queued` messages per client
    pub fn for_worker(worker_id: i32, worker_count: i32, max_queued: usize) -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
            worker_id,
            has_peers: worker_count > 1,
            max_queued,
        }
    }

    /// Register a client connection. Returns the queue of messages to it and
    /// the generation that identifies this connection.
    ///
    /// A connection already holding the client id on this worker is taken
    /// over: it is told to disconnect with reason 0x8E (MQTT 5 3.1.4).
    pub async fn register_client(&self, client_id: &str) -> (Arc<ClientQueue>, u64) {
        let queue = Arc::new(ClientQueue::new(self.max_queued));
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let previous = self.clients.write().await.insert(
            client_id.to_string(),
            ClientHandle {
                generation,
                queue: queue.clone(),
            },
        );
        if let Some(previous) = previous {
            previous.queue.close(ReasonCode::SessionTakenOver);
        }
        (queue, generation)
    }

    /// Unregister a client connection, unless a newer connection took the
    /// client id over
    pub async fn unregister_client(&self, client_id: &str, generation: u64) {
        let mut clients = self.clients.write().await;
        if clients.get(client_id).map(|c| c.generation) == Some(generation) {
            clients.remove(client_id);
        }
    }

    /// Whether a connection holds this client id on this worker
    pub async fn is_connected(&self, client_id: &str) -> bool {
        self.clients.read().await.contains_key(client_id)
    }

    /// Whether any client is connected to this worker
    pub async fn has_clients(&self) -> bool {
        !self.clients.read().await.is_empty()
    }

    /// Queue a message for a connected client. A client that does not keep
    /// up loses QoS 0 messages, or is disconnected rather than lose QoS 1 or 2
    /// ones (protocol/client_queue.rs).
    pub async fn send_to_client(&self, client_id: &str, delivery: Delivery) -> bool {
        let queue = match self.clients.read().await.get(client_id) {
            Some(client) => client.queue.clone(),
            None => return false,
        };
        match queue.push(delivery) {
            Pushed::Queued => true,
            Pushed::DroppedQos0(dropped) => {
                // Once, then a total when the connection ends (tcp.rs)
                if dropped == 1 {
                    eprintln!(
                        "pg_mqtt: client '{}' is not keeping up, dropping QoS 0 messages",
                        client_id
                    );
                }
                true
            }
            Pushed::Overflow => {
                eprintln!(
                    "pg_mqtt: client '{}' is not keeping up: {} messages waiting, disconnecting it (pg_mqtt.max_queued_messages)",
                    client_id, self.max_queued
                );
                false
            }
            Pushed::Closing => false,
        }
    }
}

impl Default for MqttBrokerState {
    fn default() -> Self {
        Self::new()
    }
}

fn qos_from_i16(qos: i16) -> QoS {
    match qos {
        i16::MIN..=0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

/// Who gets a message published to `topic` by `publisher`, at which QoS and
/// with which RETAIN flag.
///
/// - A client with several matching subscriptions gets it once, at the highest
///   QoS they grant, never above the published QoS (MQTT 5 4.8.1 allows this).
/// - RETAIN is cleared unless a matching subscription asked for Retain As
///   Published.
/// - A No Local subscription does not match its own client's publishes.
pub fn plan_deliveries<'a>(
    subscriptions: impl IntoIterator<Item = &'a Subscription>,
    topic: &str,
    qos: QoS,
    retain: bool,
    publisher: Option<&str>,
) -> Vec<(&'a str, QoS, bool)> {
    let mut per_client: BTreeMap<&str, (i16, bool)> = BTreeMap::new();
    for sub in subscriptions {
        if !topic_matches(&sub.topic_filter, topic) {
            continue;
        }
        if sub.no_local && publisher == Some(sub.client_id.as_str()) {
            continue;
        }
        let entry = per_client
            .entry(sub.client_id.as_str())
            .or_insert((sub.qos, sub.retain_as_published));
        entry.0 = entry.0.max(sub.qos);
        entry.1 |= sub.retain_as_published;
    }
    per_client
        .into_iter()
        .map(|(client, (sub_qos, rap))| {
            let delivered = (qos as i16).min(sub_qos);
            (client, qos_from_i16(delivered), retain && rap)
        })
        .collect()
}

/// Properties a message is forwarded and stored with: what the publisher sent,
/// without the Topic Alias, which belongs to the publisher's connection
pub fn forwarded_properties(properties: &PublishProperties) -> PublishProperties {
    PublishProperties {
        topic_alias: None,
        ..properties.clone()
    }
}

/// Queue a message for this worker's matching subscribers
#[allow(clippy::too_many_arguments)]
async fn deliver_locally<'a>(
    subscriptions: impl IntoIterator<Item = &'a Subscription>,
    topic: &str,
    payload: &[u8],
    qos: QoS,
    retain: bool,
    properties: &PublishProperties,
    publisher: Option<&str>,
    state: &MqttBrokerState,
) {
    for (client, qos, retain) in plan_deliveries(subscriptions, topic, qos, retain, publisher) {
        let delivery = Delivery {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos,
            retain,
            properties: properties.clone(),
        };
        state.send_to_client(client, delivery).await;
    }
}

/// Handle a CONNECT packet
///
/// Sessions end with their connection: this broker keeps no session state for
/// a disconnected client. A client asking for a Session Expiry Interval is
/// told 0 in the CONNACK (MQTT 5 3.2.2.3.2), and Session Present is always 0.
pub async fn handle_connect(
    packet: &ConnectPacket,
    storage: &MqttStorageClient,
    _state: &Arc<MqttBrokerState>,
) -> Result<(BytesMut, ClientSession), BytesMut> {
    // The codec only parses CONNECTs of versions it speaks
    let version =
        ProtocolVersion::from_level(packet.protocol_version).unwrap_or(ProtocolVersion::V5);
    let refuse = |reason_code| {
        encode_connack(
            &ConnackPacket {
                session_present: false,
                reason_code,
                properties: ConnackProperties::default(),
            },
            version,
        )
    };

    // MQTT 3.1.1 assigns no client id to a client that wants its session
    // kept (MQTT 3.1.1 3.1.3-8)
    if !version.is_v5() && packet.client_id.is_empty() && !packet.flags.clean_start {
        return Err(refuse(ReasonCode::ClientIdentifierNotValid));
    }

    let will = match (&packet.will_topic, packet.flags.will_flag) {
        (Some(topic), true) => {
            let will_props = packet.will_properties.clone().unwrap_or_default();
            Some(Will {
                topic: topic.clone(),
                payload: packet.will_payload.clone().unwrap_or_default(),
                qos: packet.flags.will_qos,
                retain: packet.flags.will_retain,
                properties: PublishProperties {
                    payload_format_indicator: will_props.payload_format_indicator,
                    message_expiry_interval: will_props.message_expiry_interval,
                    topic_alias: None,
                    response_topic: will_props.response_topic,
                    correlation_data: will_props.correlation_data,
                    content_type: will_props.content_type,
                    user_properties: will_props.user_properties,
                },
            })
        }
        _ => None,
    };

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
        eprintln!("pg_mqtt: failed to create session: {}", e);
    }

    // No session survives its connection, so a new connection starts clean
    // whatever Clean Start says
    if let Err(e) = storage.remove_all_subscriptions(&client_id).await {
        eprintln!("pg_mqtt: failed to clean subscriptions: {}", e);
    }

    let session = ClientSession {
        client_id: client_id.clone(),
        version,
        connected: true,
        keep_alive: packet.keep_alive,
        receive_maximum: packet.properties.receive_maximum.unwrap_or(u16::MAX),
        maximum_packet_size: packet.properties.maximum_packet_size,
        generation: 0,
        will,
    };

    // Build CONNACK response with our capabilities. Maximum QoS is absent:
    // QoS 2 is supported.
    let asked_for_expiry = packet
        .properties
        .session_expiry_interval
        .is_some_and(|v| v > 0);
    let properties = ConnackProperties {
        session_expiry_interval: asked_for_expiry.then_some(0),
        receive_maximum: Some(SERVER_RECEIVE_MAXIMUM),
        retain_available: Some(true),
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
        session_present: false,
        reason_code: ReasonCode::Success,
        properties,
    };

    Ok((encode_connack(&connack, version), session))
}

/// Store a published message (batched, see server/ingest.rs) and forward it:
/// what a Last Will goes through. Returns the reason code a PUBACK or PUBREC
/// would carry.
pub async fn route_publish(
    packet: &PublishPacket,
    publisher: Option<&str>,
    ingest: &IngestQueue,
    state: &MqttBrokerState,
) -> ReasonCode {
    let outcome = ingest
        .submit(packet.clone(), publisher.map(str::to_string))
        .await
        .await
        .unwrap_or_else(|_| IngestOutcome::rejected(ReasonCode::ServerUnavailable));
    if outcome.accepted() {
        deliver_published(packet, publisher, &outcome, state).await;
    }
    outcome.reason_code
}

/// Forward a stored message to this worker's matching subscribers, using the
/// subscriptions of the transaction that stored it. Subscribers on other
/// workers get it through the outbox the same transaction wrote.
pub async fn deliver_published(
    packet: &PublishPacket,
    publisher: Option<&str>,
    outcome: &IngestOutcome,
    state: &MqttBrokerState,
) {
    deliver_locally(
        outcome.subscriptions.iter(),
        &packet.topic,
        &packet.payload,
        packet.qos,
        packet.retain,
        &forwarded_properties(&packet.properties),
        publisher,
        state,
    )
    .await;
}

/// What to send before closing a connection for `reason_code`: DISCONNECT in
/// MQTT 5, nothing in MQTT 3.1.1, where the server just closes the connection
pub fn disconnect_for(version: ProtocolVersion, reason_code: ReasonCode) -> BytesMut {
    if !version.is_v5() {
        return BytesMut::new();
    }
    encode_disconnect(&DisconnectPacket {
        reason_code,
        properties: DisconnectProperties::default(),
    })
}

/// Check a PUBLISH before it is queued for storage. Err carries what to send
/// before closing the connection for a protocol error.
pub fn check_publish(packet: &PublishPacket, session: &ClientSession) -> Result<(), BytesMut> {
    let disconnect = |reason_code| disconnect_for(session.version, reason_code);
    // CONNACK announced no Topic Alias Maximum, so the client may send no
    // Topic Alias (MQTT 5 3.3.2.3.4)
    if packet.properties.topic_alias.is_some() {
        return Err(disconnect(ReasonCode::TopicAliasInvalid));
    }
    if packet.topic.is_empty() || packet.topic.contains(['+', '#']) {
        return Err(disconnect(ReasonCode::TopicNameInvalid));
    }
    Ok(())
}

/// Publish a connection's Last Will
pub async fn publish_will(
    will: &Will,
    client_id: &str,
    ingest: &IngestQueue,
    state: &MqttBrokerState,
) {
    let packet = PublishPacket {
        dup: false,
        qos: will.qos,
        retain: will.retain,
        topic: will.topic.clone(),
        packet_id: None,
        properties: will.properties.clone(),
        payload: will.payload.clone(),
    };
    route_publish(&packet, Some(client_id), ingest, state).await;
}

/// Why a topic filter cannot be subscribed to, if it cannot
fn filter_problem(filter: &str) -> Option<ReasonCode> {
    if filter.starts_with("$share/") {
        return Some(ReasonCode::SharedSubscriptionsNotSupported);
    }
    let levels: Vec<&str> = filter.split('/').collect();
    let invalid = filter.is_empty()
        || levels.iter().enumerate().any(|(i, level)| {
            (level.contains('#') && (*level != "#" || i != levels.len() - 1))
                || (level.contains('+') && *level != "+")
        });
    invalid.then_some(ReasonCode::TopicFilterInvalid)
}

/// Handle a SUBSCRIBE packet
///
/// Returns the SUBACK and the retained messages to send after it.
pub async fn handle_subscribe(
    packet: &SubscribePacket,
    client_id: &str,
    version: ProtocolVersion,
    storage: &MqttStorageClient,
) -> (BytesMut, Vec<Delivery>) {
    let mut reason_codes = Vec::new();
    let mut retained = Vec::new();
    let mut retained_topics = HashSet::new();

    for sub in &packet.subscriptions {
        if let Some(reason_code) = filter_problem(&sub.topic_filter) {
            reason_codes.push(reason_code);
            continue;
        }
        let granted = sub.options.qos;

        // Store the subscription
        let is_new = match storage
            .add_subscription(
                client_id,
                &sub.topic_filter,
                granted as i16,
                sub.options.retain_as_published,
                sub.options.no_local,
            )
            .await
        {
            Ok(is_new) => {
                reason_codes.push(match granted {
                    QoS::AtMostOnce => ReasonCode::Success, // GrantedQoS0
                    QoS::AtLeastOnce => ReasonCode::GrantedQoS1,
                    QoS::ExactlyOnce => ReasonCode::GrantedQoS2,
                });
                is_new
            }
            Err(e) => {
                eprintln!("pg_mqtt: failed to add subscription: {}", e);
                reason_codes.push(ReasonCode::UnspecifiedError);
                continue;
            }
        };

        // Retain Handling: 0 send retained messages, 1 only for a new
        // subscription, 2 never (MQTT 5 3.8.3.1)
        let send_retained = match sub.options.retain_handling {
            0 => true,
            1 => is_new,
            _ => false,
        };
        if !send_retained {
            continue;
        }
        match storage.get_matching_retained(&sub.topic_filter).await {
            Ok(messages) => {
                for message in messages {
                    if !retained_topics.insert(message.topic.clone()) {
                        continue;
                    }
                    retained.push(Delivery {
                        topic: message.topic,
                        payload: message.payload,
                        qos: qos_from_i16(message.qos.min(granted as i16)),
                        retain: true,
                        properties: decode_publish_properties(&message.properties),
                    });
                }
            }
            Err(e) => {
                eprintln!(
                    "pg_mqtt: failed to read retained messages for '{}': {}",
                    sub.topic_filter, e
                );
            }
        }
    }

    let suback = SubackPacket {
        packet_id: packet.packet_id,
        properties: SubackProperties::default(),
        reason_codes,
    };

    (encode_suback(&suback, version), retained)
}

/// Handle an UNSUBSCRIBE packet
pub async fn handle_unsubscribe(
    packet: &UnsubscribePacket,
    client_id: &str,
    version: ProtocolVersion,
    storage: &MqttStorageClient,
) -> BytesMut {
    let mut reason_codes = Vec::new();

    for topic_filter in &packet.topic_filters {
        match storage.remove_subscription(client_id, topic_filter).await {
            Ok(_) => reason_codes.push(ReasonCode::Success),
            Err(e) => {
                eprintln!("pg_mqtt: failed to remove subscription: {}", e);
                reason_codes.push(ReasonCode::NoSubscriptionExisted);
            }
        }
    }

    let unsuback = UnsubackPacket {
        packet_id: packet.packet_id,
        properties: UnsubackProperties::default(),
        reason_codes,
    };

    encode_unsuback(&unsuback, version)
}

/// Handle a PINGREQ packet
pub fn handle_pingreq() -> BytesMut {
    encode_pingresp()
}

/// Handle a DISCONNECT packet. Returns whether the client asked for its Will
/// to be published anyway (reason 0x04); any other DISCONNECT discards it.
pub fn handle_disconnect(packet: &DisconnectPacket) -> bool {
    packet.reason_code == ReasonCode::DisconnectWithWillMessage
}

/// End a client's session when its connection ends: drop its subscriptions
/// and session row, unless a newer connection took the client id over
pub async fn end_session(client_id: &str, storage: &MqttStorageClient, state: &MqttBrokerState) {
    if state.is_connected(client_id).await {
        return;
    }
    if let Err(e) = storage.remove_all_subscriptions(client_id).await {
        eprintln!(
            "pg_mqtt: failed to drop subscriptions of '{}': {}",
            client_id, e
        );
    }
    if let Err(e) = storage.remove_session(client_id).await {
        eprintln!("pg_mqtt: failed to remove session '{}': {}", client_id, e);
    }
}

/// Deliver outbox rows to the subscribers connected to this worker
/// (design/pg_mqtt/fanout.md). Each client gets a message once, however many
/// of its filters match.
/// The subscriptions made no later than `micros` (since the epoch)
pub fn subscriptions_as_of(
    subscriptions: &[Subscription],
    micros: i64,
) -> impl Iterator<Item = &Subscription> {
    subscriptions
        .iter()
        .filter(move |s| s.created_micros <= micros)
}

pub async fn deliver_outbox_rows(
    rows: &[crate::server::fanout::OutboxRow],
    storage: &MqttStorageClient,
    state: &MqttBrokerState,
) -> Result<(), crate::storage::SpiError> {
    if rows.is_empty() || !state.has_clients().await {
        return Ok(());
    }
    let subscriptions = storage.get_all_subscriptions().await?;
    for row in rows {
        // Only subscriptions that existed when the message was published: one
        // made after it, before this worker polled, must not get it
        deliver_locally(
            subscriptions_as_of(&subscriptions, row.created_micros),
            &row.topic,
            &row.payload,
            qos_from_i16(row.qos as i16),
            row.retain,
            &decode_publish_properties(&row.properties),
            row.origin_client.as_deref(),
            state,
        )
        .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::client_queue::Next;

    fn sub(client: &str, filter: &str, qos: i16, rap: bool) -> Subscription {
        Subscription {
            client_id: client.to_string(),
            topic_filter: filter.to_string(),
            qos,
            retain_as_published: rap,
            no_local: false,
            created_micros: 0,
        }
    }

    #[test]
    fn delivery_qos_is_the_lower_of_publish_and_subscription() {
        let subs = [
            sub("a", "t/#", 0, false),
            sub("b", "t/#", 1, false),
            sub("c", "t/#", 2, false),
        ];
        let plan = plan_deliveries(&subs, "t/x", QoS::AtLeastOnce, false, None);
        assert_eq!(
            plan,
            vec![
                ("a", QoS::AtMostOnce, false),
                ("b", QoS::AtLeastOnce, false),
                ("c", QoS::AtLeastOnce, false)
            ]
        );
        let plan = plan_deliveries(&subs, "t/x", QoS::ExactlyOnce, false, None);
        assert_eq!(plan[2], ("c", QoS::ExactlyOnce, false));
        let plan = plan_deliveries(&subs, "t/x", QoS::AtMostOnce, false, None);
        assert!(plan.iter().all(|(_, qos, _)| *qos == QoS::AtMostOnce));
    }

    #[test]
    fn overlapping_subscriptions_deliver_once_at_the_highest_qos() {
        let subs = [
            sub("a", "t/#", 0, false),
            sub("a", "t/+", 2, false),
            sub("a", "other", 1, false),
        ];
        let plan = plan_deliveries(&subs, "t/x", QoS::ExactlyOnce, false, None);
        assert_eq!(plan, vec![("a", QoS::ExactlyOnce, false)]);
    }

    #[test]
    fn retain_is_cleared_unless_retain_as_published() {
        let subs = [sub("a", "t", 0, false), sub("b", "t", 0, true)];
        let plan = plan_deliveries(&subs, "t", QoS::AtMostOnce, true, None);
        assert_eq!(
            plan,
            vec![("a", QoS::AtMostOnce, false), ("b", QoS::AtMostOnce, true)]
        );
    }

    #[test]
    fn no_local_skips_only_the_publishers_own_subscription() {
        let mut own = sub("a", "t", 1, false);
        own.no_local = true;
        let subs = [own, sub("b", "t", 1, false)];
        let plan = plan_deliveries(&subs, "t", QoS::AtLeastOnce, false, Some("a"));
        assert_eq!(plan, vec![("b", QoS::AtLeastOnce, false)]);
        let plan = plan_deliveries(&subs, "t", QoS::AtLeastOnce, false, Some("b"));
        assert_eq!(plan.len(), 2);
    }

    #[test]
    fn outbox_messages_skip_later_subscriptions() {
        let mut old = sub("old", "t", 1, false);
        old.created_micros = 100;
        let mut new = sub("new", "t", 1, false);
        new.created_micros = 300;
        let subs = [old, new];
        let as_of: Vec<&str> = subscriptions_as_of(&subs, 200)
            .map(|s| s.client_id.as_str())
            .collect();
        assert_eq!(as_of, vec!["old"]);
        assert_eq!(subscriptions_as_of(&subs, 300).count(), 2);
    }

    #[test]
    fn topic_alias_is_not_forwarded() {
        let props = PublishProperties {
            topic_alias: Some(3),
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        let forwarded = forwarded_properties(&props);
        assert_eq!(forwarded.topic_alias, None);
        assert_eq!(forwarded.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn filter_validation() {
        assert_eq!(filter_problem("a/b"), None);
        assert_eq!(filter_problem("a/+/b"), None);
        assert_eq!(filter_problem("a/#"), None);
        assert_eq!(filter_problem("#"), None);
        assert_eq!(
            filter_problem("a/#/b"),
            Some(ReasonCode::TopicFilterInvalid)
        );
        assert_eq!(filter_problem("a/b#"), Some(ReasonCode::TopicFilterInvalid));
        assert_eq!(filter_problem("a+/b"), Some(ReasonCode::TopicFilterInvalid));
        assert_eq!(filter_problem(""), Some(ReasonCode::TopicFilterInvalid));
        assert_eq!(
            filter_problem("$share/g/a"),
            Some(ReasonCode::SharedSubscriptionsNotSupported)
        );
    }

    #[tokio::test]
    async fn takeover_tells_the_old_connection_and_keeps_the_new_one() {
        let state = MqttBrokerState::new();
        let (old_queue, old_gen) = state.register_client("c").await;
        let (_new_queue, new_gen) = state.register_client("c").await;
        assert_ne!(old_gen, new_gen);
        assert!(matches!(
            old_queue.next(true).await,
            Next::Close(ReasonCode::SessionTakenOver)
        ));

        // The old connection closing does not unregister the new one
        state.unregister_client("c", old_gen).await;
        assert!(state.is_connected("c").await);
        state.unregister_client("c", new_gen).await;
        assert!(!state.is_connected("c").await);
    }
}
