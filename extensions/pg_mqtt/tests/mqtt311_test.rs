//! Integration tests for MQTT 3.1.1 clients, alone and next to MQTT 5 ones
//!
//! Gateways and Sparkplug edge nodes often speak MQTT 3.1.1, so a 3.1.1 client
//! must get the same broker: QoS 1/2, retained messages, the Will, and messages
//! from and to MQTT 5 clients.
//!
//! These tests require a running PostgreSQL instance with pg_mqtt extension loaded.
//! Run with: cargo test --test mqtt311_test -- --nocapture --test-threads=1

// The SQL helpers in common are not needed here
#[allow(dead_code)]
mod common;

use rumqttc::{Client, Event, LastWill, MqttOptions, Packet, QoS};
use std::io::Read;
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn unique() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A message as a subscriber received it: (topic, payload, qos, retain)
type Received = (String, Vec<u8>, u8, bool);

/// An MQTT 3.1.1 client subscribed to `filter`; returns once SUBACK arrived
fn subscribe_v311(client_id: &str, filter: &str, qos: QoS) -> (Client, mpsc::Receiver<Received>) {
    let mut opts = MqttOptions::new(client_id, common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut connection) = Client::new(opts, 10);
    client.subscribe(filter, qos).expect("subscribe");
    let (suback_tx, suback_rx) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    let _ = suback_tx.send(());
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let msg = (p.topic.clone(), p.payload.to_vec(), p.qos as u8, p.retain);
                    if tx.send(msg).is_err() {
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
    (client, rx)
}

/// Publish from an MQTT 3.1.1 client and wait for its PUBACK/PUBCOMP (QoS 1/2)
/// or for the packet to go out (QoS 0)
fn publish_v311(client_id: &str, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
    let mut opts = MqttOptions::new(client_id, common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut connection) = Client::new(opts, 10);
    client
        .publish(topic, qos, retain, payload.to_vec())
        .expect("publish");
    for event in connection.iter() {
        match event {
            Ok(Event::Incoming(Packet::PubAck(_))) if qos == QoS::AtLeastOnce => break,
            Ok(Event::Incoming(Packet::PubComp(_))) if qos == QoS::ExactlyOnce => break,
            Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                thread::sleep(Duration::from_millis(200));
                break;
            }
            Ok(_) => {}
            Err(e) => panic!("publisher connection failed: {e:?}"),
        }
    }
    let _ = client.disconnect();
}

#[test]
fn test_v311_publish_subscribe_at_every_qos() {
    skip_if_no_server!(common::mqtt_addr());
    let topic = format!("v311/qos/{}", unique());
    let (_sub, rx) = subscribe_v311("v311-qos-sub", &topic, QoS::ExactlyOnce);

    for (qos, n) in [
        (QoS::AtMostOnce, 0u8),
        (QoS::AtLeastOnce, 1),
        (QoS::ExactlyOnce, 2),
    ] {
        publish_v311("v311-qos-pub", &topic, &[b'0' + n], qos, false);
        let (_, payload, got_qos, retain) = rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("QoS {n} message not delivered"));
        assert_eq!(payload, [b'0' + n]);
        assert_eq!(got_qos, n);
        assert!(!retain);
    }
}

#[test]
fn test_v311_retained_and_will_reach_v5_subscribers() {
    skip_if_no_server!(common::mqtt_addr());
    // A 3.1.1 edge node: retained "online", and a retained "offline" Will
    let topic = format!("v311/state/{}", unique());
    let mut opts = MqttOptions::new("v311-edge", common::MQTT_HOST, common::mqtt_port());
    opts.set_keep_alive(Duration::from_secs(5));
    opts.set_last_will(LastWill::new(&topic, "offline", QoS::AtLeastOnce, true));
    let (edge, mut connection) = Client::new(opts, 10);
    edge.publish(&topic, QoS::AtLeastOnce, true, "online")
        .unwrap();
    for event in connection.iter() {
        if let Ok(Event::Incoming(Packet::PubAck(_))) = event {
            break;
        }
    }

    // A late 3.1.1 subscriber gets the retained "online"
    let (_late, rx) = subscribe_v311("v311-state-late", &topic, QoS::AtLeastOnce);
    let (_, payload, _, retain) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(payload, b"online");
    assert!(retain);

    // The edge node vanishes: its Will replaces the retained message
    drop(edge);
    drop(connection);
    let (_, payload, _, _) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Will not delivered");
    assert_eq!(payload, b"offline");
}

#[test]
fn test_v311_and_v5_clients_exchange_messages() {
    skip_if_no_server!(common::mqtt_addr());
    use rumqttc::v5::mqttbytes::v5::PublishProperties;
    use rumqttc::v5::{Client as Client5, Event as Event5, Incoming, MqttOptions as Options5};

    let topic = format!("v311/mixed/{}", unique());
    let (_sub, rx) = subscribe_v311("v311-mixed-sub", &topic, QoS::AtLeastOnce);

    // An MQTT 5 publish with properties reaches the 3.1.1 subscriber, which
    // has no way to receive them
    let opts = Options5::new("v5-mixed-pub", common::MQTT_HOST, common::mqtt_port());
    let (client, mut connection) = Client5::new(opts, 10);
    let props = PublishProperties {
        content_type: Some("text/plain".into()),
        user_properties: vec![("k".into(), "v".into())],
        ..Default::default()
    };
    client
        .publish_with_properties(
            &topic,
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            false,
            "from v5",
            props,
        )
        .unwrap();
    for event in connection.iter() {
        if let Ok(Event5::Incoming(Incoming::PubAck(_))) = event {
            break;
        }
    }

    let (_, payload, qos, _) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(payload, b"from v5");
    assert_eq!(qos, 1);
}

#[test]
fn test_v311_keep_alive_timeout_closes_without_disconnect() {
    skip_if_no_server!(common::mqtt_addr());
    // CONNECT, level 4, Clean Session, keep-alive 1 s, client id "v311-ka"
    let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x01];
    body.extend_from_slice(&[0x00, 0x07]);
    body.extend_from_slice(b"v311-ka");
    let mut packet = vec![0x10, body.len() as u8];
    packet.extend_from_slice(&body);

    let mut stream = TcpStream::connect(common::mqtt_addr()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::io::Write::write_all(&mut stream, &packet).unwrap();
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).unwrap();
    assert_eq!(connack, [0x20, 0x02, 0x00, 0x00], "3.1.1 CONNACK, accepted");

    // Silence: after 1.5 s the broker closes the socket. MQTT 3.1.1 has no
    // server DISCONNECT, so nothing arrives before the close.
    let start = Instant::now();
    let mut rest = Vec::new();
    let n = stream.read_to_end(&mut rest).unwrap_or(0);
    assert_eq!(n, 0, "the broker sent {rest:?} before closing");
    assert!(start.elapsed() >= Duration::from_millis(1200));
}

#[test]
fn test_unsupported_protocol_level_is_refused() {
    skip_if_no_server!(common::mqtt_addr());
    // CONNECT with protocol level 6, which no MQTT version uses
    let body = [
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x06, 0x02, 0x00, 0x0A, 0x00, 0x01, b'x',
    ];
    let mut packet = vec![0x10, body.len() as u8];
    packet.extend_from_slice(&body);
    let mut stream = TcpStream::connect(common::mqtt_addr()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::io::Write::write_all(&mut stream, &packet).unwrap();
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).unwrap();
    assert_eq!(
        connack,
        [0x20, 0x02, 0x00, 0x01],
        "unacceptable protocol version"
    );
}
