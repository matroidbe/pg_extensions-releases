//! Load generator for pg_mqtt: N MQTT 5 clients publish M messages each and
//! the run reports how fast the broker accepted them (QoS 1/2: until every
//! PUBACK/PUBCOMP arrived; QoS 0: until a subscriber received them all).
//!
//!     cargo run --release --example mqtt_load -- --port 1883 --clients 4 \
//!         --messages 20000 --qos 1 --inflight 100
//!
//! Options: --host, --port, --clients, --messages (per client), --qos,
//! --inflight (each client's in-flight window), --payload (bytes, at least 8),
//! --topic (prefix; client c publishes to <prefix>/<c>), --subscriber (also
//! count deliveries to one subscriber and report end-to-end latency),
//! --json (payload {"n": i, "t": send time}, for typed topics), --rate (each
//! client's messages per second; default as fast as the broker takes them),
//! --timeline (with --subscriber: the worst latency of each second of the run).
//!
//! Every message carries its send time (first 8 bytes, or "t" with --json), so
//! the subscriber reports the latency from publish to delivery.

use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use rumqttc::Outgoing;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Args {
    host: String,
    port: u16,
    clients: usize,
    messages: u64,
    qos: QoS,
    inflight: u16,
    payload: usize,
    topic: String,
    subscriber: bool,
    json: bool,
    rate: Option<f64>,
    timeline: bool,
}

fn parse_args() -> Args {
    let mut args = Args {
        host: "127.0.0.1".into(),
        port: 1883,
        clients: 4,
        messages: 10_000,
        qos: QoS::AtLeastOnce,
        inflight: 100,
        payload: 64,
        topic: "bench".into(),
        subscriber: false,
        json: false,
        rate: None,
        timeline: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--host" => args.host = value(),
            "--port" => args.port = value().parse().expect("--port"),
            "--clients" => args.clients = value().parse().expect("--clients"),
            "--messages" => args.messages = value().parse().expect("--messages"),
            "--qos" => {
                args.qos = match value().as_str() {
                    "0" => QoS::AtMostOnce,
                    "1" => QoS::AtLeastOnce,
                    "2" => QoS::ExactlyOnce,
                    q => panic!("--qos {q}"),
                }
            }
            "--inflight" => args.inflight = value().parse().expect("--inflight"),
            "--payload" => args.payload = value().parse().expect("--payload"),
            "--topic" => args.topic = value(),
            "--subscriber" => args.subscriber = true,
            "--json" => args.json = true,
            "--rate" => args.rate = Some(value().parse().expect("--rate")),
            "--timeline" => args.timeline = true,
            other => panic!("unknown option {other}"),
        }
    }
    args
}

fn options(args: &Args, id: String) -> MqttOptions {
    let mut opts = MqttOptions::new(id, &args.host, args.port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_outgoing_inflight_upper_limit(args.inflight);
    opts.set_max_packet_size(Some(1 << 20));
    opts
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// A payload carrying its send time
fn payload(args: &Args, n: u64) -> Vec<u8> {
    let sent = now_micros();
    if args.json {
        return format!(r#"{{"n": {n}, "t": {sent}}}"#).into_bytes();
    }
    let mut p = vec![b'x'; args.payload.max(8)];
    p[..8].copy_from_slice(&sent.to_be_bytes());
    p
}

/// The send time a payload carries
fn sent_at(json: bool, payload: &[u8]) -> Option<u64> {
    if json {
        let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
        return v["t"].as_u64();
    }
    Some(u64::from_be_bytes(payload.get(..8)?.try_into().ok()?))
}

fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1000.0
}

#[tokio::main]
async fn main() {
    let args = Arc::new(parse_args());
    let total = args.messages * args.clients as u64;
    let run = std::process::id();

    // Optional subscriber, counting what reaches it
    let received = Arc::new(AtomicU64::new(0));
    // (received at, latency), both in microseconds
    let latencies = Arc::new(Mutex::new(Vec::<(u64, u64)>::new()));
    // Why the subscriber's connection ended early, if it did
    let sub_ended = Arc::new(Mutex::new(None::<String>));
    if args.subscriber {
        let (client, mut eventloop) =
            AsyncClient::new(options(&args, format!("load-sub-{run}")), 10_000);
        client
            .subscribe(format!("{}/#", args.topic), QoS::AtLeastOnce)
            .await
            .unwrap();
        let received = received.clone();
        let latencies = latencies.clone();
        let sub_ended = sub_ended.clone();
        let json = args.json;
        tokio::spawn(async move {
            let _client = client;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        received.fetch_add(1, Ordering::Relaxed);
                        if let Some(sent) = sent_at(json, &p.payload) {
                            let now = now_micros();
                            latencies
                                .lock()
                                .unwrap()
                                .push((now, now.saturating_sub(sent)));
                        }
                    }
                    Ok(Event::Incoming(Packet::Disconnect(d))) => {
                        *sub_ended.lock().unwrap() =
                            Some(format!("broker disconnected it: {:?}", d.reason_code));
                        break;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        sub_ended
                            .lock()
                            .unwrap()
                            .get_or_insert_with(|| format!("{e:?}"));
                        break;
                    }
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let acked = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut tasks = Vec::new();
    for c in 0..args.clients {
        let args = args.clone();
        let acked = acked.clone();
        let (client, mut eventloop) =
            AsyncClient::new(options(&args, format!("load-pub-{run}-{c}")), 1_000);
        let done = args.messages;
        let qos = args.qos;
        let topic = format!("{}/{c}", args.topic);
        let publisher_args = args.clone();
        // Event loop: count acknowledgements (or sent QoS 0 publishes)
        tasks.push(tokio::spawn(async move {
            let mut count = 0;
            while count < done {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::PubAck(_))) if qos == QoS::AtLeastOnce => count += 1,
                    Ok(Event::Incoming(Packet::PubComp(_))) if qos == QoS::ExactlyOnce => {
                        count += 1
                    }
                    Ok(Event::Outgoing(Outgoing::Publish(_))) if qos == QoS::AtMostOnce => {
                        count += 1
                    }
                    Ok(_) => {}
                    Err(e) => panic!("client {c}: {e:?}"),
                }
            }
            acked.fetch_add(count, Ordering::Relaxed);
            // Keep the connection until the publisher hung up
            let _ = tokio::time::timeout(Duration::from_millis(200), eventloop.poll()).await;
        }));
        // Publisher: the request queue blocks when the in-flight window is full
        tokio::spawn(async move {
            let args = publisher_args;
            let start = Instant::now();
            for n in 0..done {
                if let Some(rate) = args.rate {
                    let due = start + Duration::from_secs_f64(n as f64 / rate);
                    tokio::time::sleep_until(due.into()).await;
                }
                client
                    .publish(topic.clone(), qos, false, payload(&args, n))
                    .await
                    .unwrap();
            }
        });
    }
    for t in tasks {
        t.await.unwrap();
    }
    let elapsed = start.elapsed();

    let mut delivered_note = String::new();
    if args.subscriber {
        let deadline = Instant::now() + Duration::from_secs(30);
        while received.load(Ordering::Relaxed) < total
            && Instant::now() < deadline
            && sub_ended.lock().unwrap().is_none()
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let all = start.elapsed();
        let samples = latencies.lock().unwrap().clone();
        if args.timeline {
            let first = samples.iter().map(|s| s.0).min().unwrap_or(0);
            let mut worst = std::collections::BTreeMap::<u64, u64>::new();
            for (at, l) in &samples {
                let w = worst.entry((at - first) / 1_000_000).or_default();
                *w = (*w).max(*l);
            }
            for (second, l) in worst {
                println!("t={second:>4}s worst {:.1} ms", l as f64 / 1000.0);
            }
        }
        let mut lat: Vec<u64> = samples.iter().map(|s| s.1).collect();
        lat.sort_unstable();
        delivered_note = format!(
            ", subscriber got {} in {:.2}s ({:.0} msg/s), latency p50 {:.1} ms p99 {:.1} ms max {:.1} ms",
            received.load(Ordering::Relaxed),
            all.as_secs_f64(),
            received.load(Ordering::Relaxed) as f64 / all.as_secs_f64(),
            percentile(&lat, 0.5),
            percentile(&lat, 0.99),
            percentile(&lat, 1.0),
        );
        if let Some(why) = sub_ended.lock().unwrap().as_ref() {
            delivered_note.push_str(&format!(", subscriber ended: {why}"));
        }
    }
    println!(
        "{} clients x {} msgs, QoS {}, inflight {}, {}{}: {} acked in {:.2}s = {:.0} msg/s{}",
        args.clients,
        args.messages,
        args.qos as u8,
        args.inflight,
        if args.json {
            "JSON".to_string()
        } else {
            format!("{} B", args.payload)
        },
        args.rate
            .map(|r| format!(", {r} msg/s per client"))
            .unwrap_or_default(),
        acked.load(Ordering::Relaxed),
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64(),
        delivered_note
    );
}
