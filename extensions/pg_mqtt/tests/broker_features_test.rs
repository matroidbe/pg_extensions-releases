//! Integration tests for Last Will, retained messages, QoS 1 and 2 and sessions
//!
//! These tests require a running PostgreSQL instance with pg_mqtt extension loaded.
//! Run with: cargo test --test broker_features_test -- --nocapture --test-threads=1
//!
//! Clients that must misbehave (vanish, stop sending) speak raw MQTT over a
//! TcpStream; well-behaved ones use rumqttc.

mod common;

use rumqttc::v5::mqttbytes::v5::{Filter, PubAckReason, RetainForwardRule};
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{Client, Event, Incoming, MqttOptions};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A unique suffix so reruns against the same database do not see each
/// other's retained messages
fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A message as a subscriber received it
#[derive(Debug)]
struct Received {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
}

/// A connected subscriber whose received messages arrive on `rx`
struct Subscriber {
    _client: Client,
    rx: mpsc::Receiver<Received>,
}

impl Subscriber {
    fn next(&self, within: Duration) -> Option<Received> {
        self.rx.recv_timeout(within).ok()
    }
}

/// Connect, subscribe and return once the SUBACK has arrived
fn subscribe_with(client_id: &str, filter: Filter) -> Subscriber {
    let mut opts = MqttOptions::new(client_id, common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_clean_start(true);
    let (client, mut connection) = Client::new(opts, 10);
    client
        .subscribe_many(vec![filter])
        .expect("Failed to subscribe");

    let (suback_tx, suback_rx) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Incoming::SubAck(_))) => {
                    let _ = suback_tx.send(());
                }
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    let received = Received {
                        topic: String::from_utf8_lossy(&p.topic).into_owned(),
                        payload: p.payload.to_vec(),
                        qos: p.qos,
                        retain: p.retain,
                    };
                    if tx.send(received).is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    suback_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("no SUBACK within 5s");
    Subscriber {
        _client: client,
        rx,
    }
}

fn subscribe(client_id: &str, filter: &str, qos: QoS) -> Subscriber {
    subscribe_with(client_id, Filter::new(filter, qos))
}

/// Publish one message and return the PUBACK reason (QoS 1) or None (QoS 0,
/// once the packet is out)
fn publish(
    client_id: &str,
    topic: &str,
    payload: &[u8],
    qos: QoS,
    retain: bool,
) -> Option<PubAckReason> {
    let mut opts = MqttOptions::new(client_id, common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut connection) = Client::new(opts, 10);
    client
        .publish(topic, qos, retain, payload.to_vec())
        .expect("Failed to publish");

    let mut result = None;
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                result = Some(ack.reason);
                break;
            }
            Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                // Give the broker a moment to route it before we hang up
                thread::sleep(Duration::from_millis(200));
                break;
            }
            Ok(_) => {}
            Err(e) => panic!("publisher connection failed: {e:?}"),
        }
    }
    let _ = client.disconnect();
    result
}

// ----------------------------------------------------------------------------
// Raw MQTT 5 for clients that misbehave
// ----------------------------------------------------------------------------

fn put_string(buf: &mut Vec<u8>, s: &[u8]) {
    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buf.extend_from_slice(s);
}

fn put_varint(buf: &mut Vec<u8>, mut n: usize) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if n == 0 {
            break;
        }
    }
}

/// An MQTT 5 CONNECT with a Will (QoS `will_qos`, RETAIN `will_retain`)
fn connect_with_will(
    client_id: &str,
    keep_alive: u16,
    will_topic: &str,
    will_payload: &[u8],
    will_qos: u8,
    will_retain: bool,
) -> Vec<u8> {
    let mut body = Vec::new();
    put_string(&mut body, b"MQTT");
    body.push(5);
    let mut flags = 0x02 | 0x04 | (will_qos << 3);
    if will_retain {
        flags |= 0x20;
    }
    body.push(flags);
    body.extend_from_slice(&keep_alive.to_be_bytes());
    body.push(0); // no CONNECT properties
    put_string(&mut body, client_id.as_bytes());
    body.push(0); // no Will properties
    put_string(&mut body, will_topic.as_bytes());
    put_string(&mut body, will_payload);

    let mut packet = vec![0x10];
    put_varint(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

/// Open a raw connection, send CONNECT and read the CONNACK. Returns the
/// stream and the CONNACK reason code.
fn raw_connect(connect: &[u8]) -> (TcpStream, u8) {
    let mut stream = TcpStream::connect(common::mqtt_addr()).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(connect).unwrap();
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("no CONNACK");
    assert_eq!(header[0], 0x20, "expected CONNACK");
    // Drain the rest of the CONNACK (properties)
    let remaining = header[1] as usize;
    let mut rest = vec![0u8; remaining.saturating_sub(2)];
    stream.read_exact(&mut rest).unwrap();
    (stream, header[3])
}

// ============================================================================
// Last Will
// ============================================================================

#[test]
fn test_will_published_when_connection_drops() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("will/drop/{}", unique());
    let sub = subscribe("will-drop-sub", &topic, QoS::AtLeastOnce);

    let (stream, reason) = raw_connect(&connect_with_will(
        "will-drop-client",
        30,
        &topic,
        b"gone",
        1,
        false,
    ));
    assert_eq!(reason, 0x00);
    drop(stream); // vanish without DISCONNECT

    let got = sub
        .next(Duration::from_secs(5))
        .expect("Will not published after the connection dropped");
    assert_eq!(got.topic, topic);
    assert_eq!(got.payload, b"gone");
    assert_eq!(got.qos, QoS::AtLeastOnce);
}

#[test]
fn test_will_discarded_on_normal_disconnect() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("will/clean/{}", unique());
    let sub = subscribe("will-clean-sub", &topic, QoS::AtMostOnce);

    let (mut stream, _) = raw_connect(&connect_with_will(
        "will-clean-client",
        30,
        &topic,
        b"should not arrive",
        0,
        false,
    ));
    stream.write_all(&[0xE0, 0x00]).unwrap(); // DISCONNECT, normal
    drop(stream);

    assert!(
        sub.next(Duration::from_secs(2)).is_none(),
        "Will published after a normal DISCONNECT"
    );
}

#[test]
fn test_will_published_on_disconnect_with_will_message() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("will/requested/{}", unique());
    let sub = subscribe("will-requested-sub", &topic, QoS::AtMostOnce);

    let (mut stream, _) = raw_connect(&connect_with_will(
        "will-requested-client",
        30,
        &topic,
        b"asked for",
        0,
        false,
    ));
    stream.write_all(&[0xE0, 0x01, 0x04]).unwrap(); // DISCONNECT 0x04
    drop(stream);

    let got = sub
        .next(Duration::from_secs(5))
        .expect("Will not published after DISCONNECT 0x04");
    assert_eq!(got.payload, b"asked for");
}

#[test]
fn test_keep_alive_timeout_publishes_will() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("will/keepalive/{}", unique());
    let sub = subscribe("will-keepalive-sub", &topic, QoS::AtMostOnce);

    // Keep-alive 1 s, then silence: the broker gives up after 1.5 s
    let (mut stream, _) = raw_connect(&connect_with_will(
        "will-keepalive-client",
        1,
        &topic,
        b"timed out",
        0,
        false,
    ));

    let got = sub
        .next(Duration::from_secs(5))
        .expect("Will not published after the keep-alive expired");
    assert_eq!(got.payload, b"timed out");

    // The broker said why: DISCONNECT with reason 0x8D (and no properties), then closed
    let mut disconnect = [0u8; 4];
    stream.read_exact(&mut disconnect).expect("no DISCONNECT");
    assert_eq!(disconnect, [0xE0, 0x02, 0x8D, 0x00]);
}

#[test]
fn test_session_takeover_publishes_the_old_connections_will() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("will/takeover/{}", unique());
    let sub = subscribe("will-takeover-sub", &topic, QoS::AtMostOnce);

    // Same client id twice. Connections spread over workers (SO_REUSEPORT)
    // and takeover is per worker, so retry until both land on one worker.
    for attempt in 0..20 {
        let client_id = format!("will-takeover-{attempt}");
        let (mut first, _) =
            raw_connect(&connect_with_will(&client_id, 30, &topic, b"old", 0, false));
        let (mut second, _) =
            raw_connect(&connect_with_will(&client_id, 30, &topic, b"new", 0, false));

        first
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut disconnect = [0u8; 4];
        let taken_over = first.read_exact(&mut disconnect).is_ok();
        // Clean up the second connection without its Will
        second.write_all(&[0xE0, 0x00]).unwrap();
        if !taken_over {
            first.write_all(&[0xE0, 0x00]).unwrap();
            continue;
        }

        assert_eq!(
            disconnect,
            [0xE0, 0x02, 0x8E, 0x00],
            "expected Session taken over"
        );
        let got = sub
            .next(Duration::from_secs(5))
            .expect("the taken-over connection's Will was not published");
        assert_eq!(got.payload, b"old");
        assert!(
            sub.next(Duration::from_secs(1)).is_none(),
            "the new connection's Will was published too"
        );
        return;
    }
    panic!("never got two connections on the same worker");
}

// ============================================================================
// Retained messages
// ============================================================================

#[test]
fn test_retained_message_reaches_later_subscriber() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("retain/late/{}", unique());

    // QoS 1 so each is stored before the next step: a QoS 0 publish is
    // stored asynchronously and could land after the subscription
    publish("retain-late-pub", &topic, b"first", QoS::AtLeastOnce, true);
    publish("retain-late-pub", &topic, b"latest", QoS::AtLeastOnce, true);

    let sub = subscribe("retain-late-sub", &topic, QoS::AtLeastOnce);
    let got = sub
        .next(Duration::from_secs(5))
        .expect("retained message not sent on subscribe");
    assert_eq!(
        got.payload, b"latest",
        "only the last retained message is kept"
    );
    assert!(got.retain, "a retained message on subscribe has RETAIN set");
    assert_eq!(got.qos, QoS::AtLeastOnce);
    assert!(sub.next(Duration::from_millis(500)).is_none());

    let stored = common::run_sql(&format!(
        "SELECT encode(payload, 'escape') || '/' || qos FROM pgmqtt.retained WHERE topic = '{topic}'"
    ))
    .unwrap();
    assert_eq!(stored.trim(), "latest/1");
}

#[test]
fn test_retained_message_cleared_by_empty_payload() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("retain/clear/{}", unique());

    // QoS 1: stored before the next step (see above)
    publish("retain-clear-pub", &topic, b"x", QoS::AtLeastOnce, true);
    publish("retain-clear-pub", &topic, b"", QoS::AtLeastOnce, true);

    let sub = subscribe("retain-clear-sub", &topic, QoS::AtMostOnce);
    assert!(
        sub.next(Duration::from_secs(1)).is_none(),
        "a cleared retained message was still sent"
    );
}

#[test]
fn test_retained_wildcard_and_retain_handling() {
    skip_if_no_server!(common::mqtt_addr());
    let base = format!("retain/wild/{}", unique());
    publish(
        "retain-wild-pub",
        &format!("{base}/a"),
        b"a",
        QoS::AtLeastOnce,
        true,
    );
    publish(
        "retain-wild-pub",
        &format!("{base}/b"),
        b"b",
        QoS::AtLeastOnce,
        true,
    );

    let sub = subscribe("retain-wild-sub", &format!("{base}/+"), QoS::AtMostOnce);
    let mut got: Vec<Vec<u8>> = (0..2)
        .map(|_| sub.next(Duration::from_secs(5)).expect("retained").payload)
        .collect();
    got.sort();
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec()]);

    // Retain Handling 2: no retained messages
    let mut filter = Filter::new(format!("{base}/+"), QoS::AtMostOnce);
    filter.retain_forward_rule = RetainForwardRule::Never;
    let never = subscribe_with("retain-wild-never", filter);
    assert!(never.next(Duration::from_secs(1)).is_none());
}

#[test]
fn test_live_message_clears_retain_unless_retain_as_published() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("retain/live/{}", unique());
    let plain = subscribe("retain-live-plain", &topic, QoS::AtMostOnce);
    let mut filter = Filter::new(topic.clone(), QoS::AtMostOnce);
    filter.preserve_retain = true;
    let rap = subscribe_with("retain-live-rap", filter);

    publish("retain-live-pub", &topic, b"now", QoS::AtMostOnce, true);

    let got = plain
        .next(Duration::from_secs(5))
        .expect("plain subscriber");
    assert!(!got.retain, "RETAIN must be cleared for a live message");
    let got = rap.next(Duration::from_secs(5)).expect("rap subscriber");
    assert!(got.retain, "Retain As Published keeps RETAIN");
}

#[test]
fn test_retained_will_for_a_status_topic() {
    skip_if_no_server!(common::mqtt_addr());
    // The Sparkplug STATE pattern: a retained "online" while connected, a
    // retained "offline" Will when the connection dies
    let topic = format!("status/{}", unique());
    let (stream, _) = raw_connect(&connect_with_will(
        "status-client",
        30,
        &topic,
        b"offline",
        1,
        true,
    ));
    publish("status-online", &topic, b"online", QoS::AtLeastOnce, true);
    drop(stream);
    thread::sleep(Duration::from_millis(500));

    let sub = subscribe("status-sub", &topic, QoS::AtLeastOnce);
    let got = sub.next(Duration::from_secs(5)).expect("retained status");
    assert_eq!(got.payload, b"offline");
    assert!(got.retain);
}

// ============================================================================
// QoS 1
// ============================================================================

#[test]
fn test_qos1_publish_is_acknowledged() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("qos1/ack/{}", unique());
    let reason = publish("qos1-ack-pub", &topic, b"hello", QoS::AtLeastOnce, false);
    assert!(
        matches!(reason, Some(PubAckReason::Success)),
        "expected PUBACK Success, got {reason:?}"
    );
}

#[test]
fn test_qos1_rejected_payload_is_nacked() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("qos1/strict/{}", unique());
    common::execute_sql(&format!(
        "SELECT pgmqtt.mqtt_create_typed_topic('{topic}', \
         '{{\"type\":\"object\",\"properties\":{{\"v\":{{\"type\":\"number\"}}}},\"required\":[\"v\"]}}'::jsonb, \
         NULL, 'STRICT')"
    ))
    .expect("create typed topic");

    let reason = publish(
        "qos1-strict-pub",
        &topic,
        b"not json",
        QoS::AtLeastOnce,
        false,
    );
    assert!(
        matches!(reason, Some(PubAckReason::PayloadFormatInvalid)),
        "expected PUBACK Payload format invalid, got {reason:?}"
    );
}

/// PUBLISHes are stored in batches, one transaction each. A row that does not
/// fit its table aborts that transaction; the batch is then written item by
/// item, so only that message fails and the others in its batch are stored.
#[test]
fn test_one_bad_row_does_not_fail_its_batch() {
    skip_if_no_server!(common::mqtt_addr());
    let id = unique();
    let topic = format!("batch/fallback/{id}");
    let table = format!("public.batch_fallback_{id}");
    // LOG mode lets a schema violation through to the insert, which fails
    common::execute_sql(&format!("CREATE TABLE {table} (n integer NOT NULL)"))
        .expect("create table");
    common::execute_sql(&format!(
        "SELECT pgmqtt.mqtt_create_typed_topic_from_table('{topic}', '{table}', 'LOG')"
    ))
    .expect("create typed topic");

    // Pipelined: the client sends all ten before any PUBACK comes back
    let mut opts = MqttOptions::new("batch-fallback-pub", common::MQTT_HOST, common::mqtt_port());
    opts.set_outgoing_inflight_upper_limit(100);
    let (client, mut connection) = Client::new(opts, 100);
    for i in 0..10 {
        let payload = if i == 4 {
            r#"{"n": "not a number"}"#.to_string()
        } else {
            format!(r#"{{"n": {i}}}"#)
        };
        client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .unwrap();
    }
    let mut reasons = Vec::new();
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                reasons.push(ack.reason);
                if reasons.len() == 10 {
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => panic!("{e:?}"),
        }
    }
    let _ = client.disconnect();

    // PUBACKs come in publish order
    for (i, reason) in reasons.iter().enumerate() {
        if i == 4 {
            assert!(
                matches!(reason, PubAckReason::UnspecifiedError),
                "the bad row: {reason:?}"
            );
        } else {
            assert!(
                matches!(reason, PubAckReason::Success),
                "row {i}: {reason:?}"
            );
        }
    }
    let stored = common::run_sql(&format!("SELECT count(*) FROM {table}")).unwrap();
    assert_eq!(stored.trim(), "9");
}

#[test]
fn test_qos1_delivery_at_the_lower_qos() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("qos1/deliver/{}", unique());
    let at1 = subscribe("qos1-deliver-1", &topic, QoS::AtLeastOnce);
    let at0 = subscribe("qos1-deliver-0", &topic, QoS::AtMostOnce);

    publish("qos1-deliver-pub", &topic, b"m", QoS::AtLeastOnce, false);

    // rumqttc acknowledges the QoS 1 delivery itself
    assert_eq!(
        at1.next(Duration::from_secs(5)).unwrap().qos,
        QoS::AtLeastOnce
    );
    assert_eq!(
        at0.next(Duration::from_secs(5)).unwrap().qos,
        QoS::AtMostOnce
    );

    // A QoS 0 publish never goes out at QoS 1
    publish("qos1-deliver-pub", &topic, b"m0", QoS::AtMostOnce, false);
    assert_eq!(
        at1.next(Duration::from_secs(5)).unwrap().qos,
        QoS::AtMostOnce
    );
}

/// A QoS 2 PUBLISH from a raw client: topic, packet id, no properties, payload
fn qos2_publish(topic: &str, packet_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    put_string(&mut body, topic.as_bytes());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    let mut packet = vec![0x34];
    put_varint(&mut packet, body.len());
    packet.extend_from_slice(&body);
    packet
}

#[test]
fn test_qos2_inbound_flow_delivers_exactly_once() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("qos2/in/{}", unique());
    let sub = subscribe("qos2-in-sub", &topic, QoS::ExactlyOnce);
    let (mut stream, _) = raw_connect(&connect_with_will(
        "qos2-in-client",
        30,
        "qos2/will",
        b"x",
        0,
        false,
    ));

    // PUBLISH, then the same PUBLISH again as if our PUBREC was lost
    stream.write_all(&qos2_publish(&topic, 7, b"once")).unwrap();
    let mut pubrec = [0u8; 4];
    stream.read_exact(&mut pubrec).expect("no PUBREC");
    assert_eq!(pubrec, [0x50, 0x02, 0x00, 0x07]);
    stream.write_all(&qos2_publish(&topic, 7, b"once")).unwrap();
    stream.read_exact(&mut pubrec).expect("no second PUBREC");
    assert_eq!(pubrec, [0x50, 0x02, 0x00, 0x07]);

    // PUBREL -> PUBCOMP
    stream.write_all(&[0x62, 0x02, 0x00, 0x07]).unwrap();
    let mut pubcomp = [0u8; 4];
    stream.read_exact(&mut pubcomp).expect("no PUBCOMP");
    assert_eq!(pubcomp, [0x70, 0x02, 0x00, 0x07]);

    let got = sub.next(Duration::from_secs(5)).expect("not delivered");
    assert_eq!(got.payload, b"once");
    assert_eq!(got.qos, QoS::ExactlyOnce);
    assert!(
        sub.next(Duration::from_secs(1)).is_none(),
        "a resent QoS 2 PUBLISH was delivered twice"
    );
    stream.write_all(&[0xE0, 0x00]).unwrap();
}

#[test]
fn test_wildcards_skip_dollar_topics() {
    skip_if_no_server!(common::mqtt_addr());
    let sub = subscribe("dollar-sub", "#", QoS::AtMostOnce);
    // Drain retained messages other tests left behind
    while sub.next(Duration::from_millis(500)).is_some() {}
    publish(
        "dollar-pub",
        &format!("$dollar/{}", unique()),
        b"x",
        QoS::AtMostOnce,
        false,
    );
    assert!(
        sub.next(Duration::from_secs(1)).is_none(),
        "'#' matched a topic starting with $"
    );
}

#[test]
fn test_session_ends_with_the_connection() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("session/{}", unique());
    // Subscribe, ask for a session that outlives the connection, leave
    let mut opts = MqttOptions::new("session-client", common::MQTT_HOST, common::mqtt_port());
    opts.set_clean_start(false);
    let (client, mut connection) = Client::new(opts, 10);
    client.subscribe(topic.clone(), QoS::AtLeastOnce).unwrap();
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                assert!(!ack.session_present);
            }
            Ok(Event::Incoming(Incoming::SubAck(_))) => break,
            Ok(_) => {}
            Err(e) => panic!("{e:?}"),
        }
    }
    let _ = client.disconnect();
    for event in connection.iter() {
        if event.is_err() {
            break;
        }
    }
    thread::sleep(Duration::from_millis(500));

    let rows = common::run_sql(
        "SELECT count(*) FROM pgmqtt.subscriptions WHERE client_id = 'session-client'",
    )
    .unwrap();
    assert_eq!(rows.trim(), "0", "the subscription outlived its connection");
}

#[test]
fn test_connack_advertises_capabilities() {
    skip_if_no_server!(common::mqtt_addr());
    let mut opts = MqttOptions::new("connack-caps", common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    let (_client, mut connection) = Client::new(opts, 10);
    for event in connection.iter() {
        if let Ok(Event::Incoming(Incoming::ConnAck(ack))) = event {
            let props = ack.properties.expect("CONNACK properties");
            // Maximum QoS absent: QoS 2 is supported
            assert_eq!(props.max_qos, None);
            assert_eq!(props.retain_available, Some(1));
            assert_eq!(props.receive_max, Some(1024));
            assert_eq!(props.shared_subscription_available, Some(0));
            return;
        }
    }
    panic!("no CONNACK");
}

// ============================================================================
// Subscribers that do not keep up
// ============================================================================

/// pg_mqtt.max_queued_messages of the server under test
fn max_queued() -> usize {
    common::run_sql("SELECT current_setting('pg_mqtt.max_queued_messages')")
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

/// A raw subscriber with Receive Maximum 1: it gets one QoS 1 message at a
/// time, until it acknowledges it
fn slow_subscriber(client_id: &str, filter: &str) -> TcpStream {
    let mut body = Vec::new();
    put_string(&mut body, b"MQTT");
    body.extend_from_slice(&[5, 0x02, 0, 30]);
    body.extend_from_slice(&[3, 0x21, 0, 1]); // Receive Maximum 1
    put_string(&mut body, client_id.as_bytes());
    let mut connect = vec![0x10];
    put_varint(&mut connect, body.len());
    connect.extend_from_slice(&body);
    let (mut stream, rc) = raw_connect(&connect);
    assert_eq!(rc, 0);

    let mut body = vec![0, 1, 0]; // packet id 1, no properties
    put_string(&mut body, filter.as_bytes());
    body.push(1); // QoS 1
    let mut subscribe = vec![0x82];
    put_varint(&mut subscribe, body.len());
    subscribe.extend_from_slice(&body);
    stream.write_all(&subscribe).unwrap();
    let (header, _) = read_packet(&mut stream).expect("no SUBACK");
    assert_eq!(header, 0x90);
    stream
}

/// Read one packet: its first byte and its body; None on timeout or close
fn read_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).ok()?;
    let (mut len, mut shift) = (0usize, 0);
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).ok()?;
        len |= ((byte[0] & 0x7f) as usize) << shift;
        shift += 7;
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).ok()?;
    Some((first[0], body))
}

/// The packet id (QoS 1 and 2) and payload of an MQTT 5 PUBLISH
fn publish_parts(header: u8, body: &[u8]) -> (Option<u16>, Vec<u8>) {
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2 + topic_len;
    let packet_id = (header & 0x06 != 0).then(|| {
        at += 2;
        u16::from_be_bytes([body[at - 2], body[at - 1]])
    });
    let (mut props, mut shift) = (0usize, 0);
    loop {
        let byte = body[at];
        at += 1;
        props |= ((byte & 0x7f) as usize) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    (packet_id, body[at + props..].to_vec())
}

/// Publish messages in order on one connection; the last must be QoS 1, and
/// this returns once it is acknowledged (so all were routed)
fn publish_all(client_id: &str, messages: Vec<(String, QoS, Vec<u8>)>) {
    assert_eq!(messages.last().unwrap().1, QoS::AtLeastOnce);
    let acks = messages.iter().filter(|m| m.1 == QoS::AtLeastOnce).count();
    let mut opts = MqttOptions::new(client_id, common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(30));
    let (client, mut connection) = Client::new(opts, 100);
    let sender = thread::spawn(move || {
        for (topic, qos, payload) in messages {
            client.publish(topic, qos, false, payload).unwrap();
        }
        client
    });
    let mut acked = 0;
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Incoming::PubAck(_))) => {
                acked += 1;
                if acked == acks {
                    break;
                }
            }
            Ok(_) => {}
            Err(e) => panic!("publisher connection failed: {e:?}"),
        }
    }
    let _ = sender.join().unwrap().disconnect();
}

#[test]
fn test_slow_qos1_subscriber_is_disconnected_rather_than_losing_messages() {
    skip_if_no_server!(common::mqtt_addr());
    let cap = max_queued();
    let topic = format!("slow/q1/{}", unique());
    let mut sub = slow_subscriber("slow-q1-sub", &topic);
    sub.set_read_timeout(Some(Duration::from_secs(60))).unwrap();

    // One in flight, `cap` waiting, and one more than fits
    let messages = (0..cap + 2)
        .map(|n| (topic.clone(), QoS::AtLeastOnce, n.to_string().into_bytes()))
        .collect();
    publish_all("slow-q1-pub", messages);

    let mut publishes = 0;
    loop {
        let (header, body) = read_packet(&mut sub).expect("no DISCONNECT");
        match header >> 4 {
            3 => publishes += 1,
            14 => {
                assert_eq!(body.first(), Some(&0x97), "expected Quota exceeded");
                break;
            }
            other => panic!("unexpected packet type {other}"),
        }
    }
    // It never acknowledged: only the first message was sent
    assert_eq!(publishes, 1);
}

#[test]
fn test_slow_subscriber_loses_its_oldest_qos0_messages() {
    skip_if_no_server!(common::mqtt_addr());
    let cap = max_queued();
    let id = unique();
    let topic = format!("slow/q0/{id}");
    let mut sub = slow_subscriber("slow-q0-sub", &topic);

    // "a" takes the only in-flight slot
    publish_all(
        "slow-q0-pub-a",
        vec![(topic.clone(), QoS::AtLeastOnce, b"a".to_vec())],
    );
    let (header, body) = read_packet(&mut sub).expect("no first PUBLISH");
    let (a_id, payload) = publish_parts(header, &body);
    assert_eq!(payload, b"a");

    // "b" waits for the slot at the front of the queue, and `cap` QoS 0
    // messages behind it are one more than fits: the oldest is dropped
    let mut messages = vec![(topic.clone(), QoS::AtLeastOnce, b"b".to_vec())];
    messages.extend((0..cap).map(|n| (topic.clone(), QoS::AtMostOnce, n.to_string().into_bytes())));
    messages.push((format!("slow/end/{id}"), QoS::AtLeastOnce, vec![]));
    publish_all("slow-q0-pub-b", messages);
    // Time for another worker's fan-out
    thread::sleep(Duration::from_millis(500));

    let a_id = a_id.unwrap().to_be_bytes();
    sub.write_all(&[0x40, 0x02, a_id[0], a_id[1]]).unwrap();
    let (header, body) = read_packet(&mut sub).expect("no PUBLISH b");
    let (b_id, payload) = publish_parts(header, &body);
    assert_eq!(payload, b"b", "a QoS 1 message was dropped");
    let b_id = b_id.unwrap().to_be_bytes();
    sub.write_all(&[0x40, 0x02, b_id[0], b_id[1]]).unwrap();

    sub.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut qos0 = Vec::new();
    while let Some((header, body)) = read_packet(&mut sub) {
        assert_eq!(header >> 4, 3, "expected only PUBLISHes, got {header:#x}");
        let (_, payload) = publish_parts(header, &body);
        qos0.push(String::from_utf8(payload).unwrap());
    }
    assert_eq!(qos0.len(), cap - 1);
    assert_eq!(qos0.first().map(String::as_str), Some("1"));
    assert_eq!(qos0.last(), Some(&(cap - 1).to_string()));
    sub.write_all(&[0xE0, 0x00]).unwrap();
}
