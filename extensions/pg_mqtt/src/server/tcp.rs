//! MQTT TCP server implementation using tokio with SPI bridge
//!
//! This module runs as part of the PostgreSQL background worker.
//! It uses a polling architecture where:
//! - Tokio tasks handle network I/O and send SPI requests via channels
//! - The main background worker thread polls for SPI requests and executes them
//!
//! Multiple workers can listen on the same port using SO_REUSEPORT, with the
//! kernel load-balancing connections across workers.
//!
//! **IMPORTANT**: pgrx FFI can ONLY be called from the main background worker thread.
//! All tokio worker threads must avoid any pgrx imports or function calls.

use bytes::BytesMut;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::Instant;

use crate::protocol::client_queue::{ClientQueue, Next};
use crate::protocol::codec::*;
use crate::protocol::handlers::*;
use crate::protocol::types::*;
use crate::server::batch::{BatchWriter, MAX_BATCH};
use crate::server::fanout::{FanoutCursor, POLL_INTERVAL};
use crate::server::ingest::{IngestOutcome, IngestQueue, INGEST_QUEUE};
use crate::storage::{execute_spi_request, MqttStorageClient, SpiBridge};

/// Longest idle sleep of the main loop; requests wake it at once
const IDLE_WAIT: Duration = Duration::from_millis(100);

/// Most QoS 1 and 2 messages in flight to one client
const MAX_INFLIGHT: usize = 1024;

/// Most PUBLISHes of one client waiting for storage before the broker stops
/// reading from it
const MAX_INBOUND: usize = SERVER_RECEIVE_MAXIMUM as usize;

/// Connection counter for monitoring
static CONNECTION_COUNT: AtomicU32 = AtomicU32::new(0);

/// Global shutdown flag
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested
pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Request shutdown
pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Create a TCP listener with SO_REUSEPORT enabled
///
/// SO_REUSEPORT allows multiple processes to bind to the same port,
/// with the kernel load-balancing connections across them.
fn create_reuseport_listener(addr: &str, port: u16) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

    // Enable SO_REUSEPORT - allows multiple listeners on same port
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;

    // Bind and listen
    let addr: SocketAddr = format!("{}:{}", addr, port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    // Convert to non-blocking tokio listener
    socket.set_nonblocking(true)?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Start the MQTT TCP server with SPI bridge
///
/// This function sets up the tokio runtime and runs it in a way that allows
/// polling for SPI requests on the main thread.
///
/// Each worker binds to the same port using SO_REUSEPORT, allowing the kernel
/// to load-balance connections across workers.
pub fn run_server(
    host: &str,
    port: u16,
    worker_id: i32,
    worker_count: i32,
    max_queued: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Reset shutdown flag at start
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

    // Create SPI bridge with capacity for pending requests
    let (bridge, mut receiver) = SpiBridge::waking_worker(1024);
    let bridge = Arc::new(bridge);

    // PUBLISHes are stored in batches, by this thread (server/ingest.rs)
    let (ingest, mut ingest_rx) = IngestQueue::waking_worker(INGEST_QUEUE);
    let mut writer = BatchWriter::new(worker_id, worker_count);

    // Create the tokio runtime
    // Its threads must never receive a signal: pgrx's handler panics off the
    // main thread (pg_bgworker::with_signals_blocked).
    let runtime = pg_bgworker::with_signals_blocked(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
    })?;

    // Spawn the server task
    let host_owned = host.to_string();
    let bridge_clone = bridge.clone();

    let server_handle = runtime.spawn(async move {
        run_server_async(
            &host_owned,
            port,
            bridge_clone,
            ingest,
            worker_id,
            worker_count,
            max_queued,
        )
        .await
        .map_err(|e| e.to_string())
    });

    // Main polling loop - runs on bgworker thread where SPI is valid
    // Import pgrx here since this runs on main thread where FFI is valid
    use pgrx::bgworkers::BackgroundWorker;

    pgrx::log!("pg_mqtt worker {}: starting SPI polling loop", worker_id);

    loop {
        // Service pending interrupts — in particular ProcSignalBarrier, which
        // is how DROP DATABASE (FORCE) asks every backend to release the
        // database. wait_latch alone is NOT enough: pgrx's wait_latch only
        // calls WaitLatch and checks the SIGTERM/postmaster-death flags, it
        // never runs CHECK_FOR_INTERRUPTS, so ProcSignalBarrierPending stays
        // set and DROP DATABASE hangs forever on WaitForProcSignalBarrier.
        pgrx::check_for_interrupts!();

        // Check for shutdown signal from PostgreSQL
        if BackgroundWorker::sigterm_received() {
            pgrx::log!(
                "pg_mqtt worker {}: received SIGTERM, initiating shutdown",
                worker_id
            );
            request_shutdown();
            break;
        }

        // The listener task ends only on shutdown. If it ended anyway (e.g. the
        // port could not be bound), fail so the supervisor restarts the worker.
        if server_handle.is_finished() {
            let reason = match runtime.block_on(server_handle) {
                Ok(Err(e)) => e,
                Ok(Ok(())) => "the listener stopped".to_string(),
                Err(e) => format!("the listener task failed: {e}"),
            };
            request_shutdown();
            runtime.shutdown_timeout(Duration::from_secs(5));
            return Err(format!("MQTT listener on {host}:{port}: {reason}").into());
        }

        // Apply pg_reload_conf(); stop serving when disabled
        if pg_bgworker::reload_config_if_signalled() && !crate::PG_MQTT_ENABLED.get() {
            pgrx::log!(
                "pg_mqtt worker {}: disabled via pg_mqtt.enabled=false, stopping",
                worker_id
            );
            request_shutdown();
            break;
        }
        crate::SUPERVISOR.tick(worker_id as usize);

        // Poll for SPI requests and execute them
        let mut processed = 0;
        while let Some(request) = receiver.try_recv() {
            execute_spi_request(request);
            processed += 1;
            // Process up to 100 requests per iteration to avoid blocking
            if processed >= 100 {
                break;
            }
        }

        // Store what connections queued since the last batch, in one
        // transaction: while it commits, the next batch fills
        let mut batch = Vec::new();
        while batch.len() < MAX_BATCH {
            match ingest_rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        let wrote = !batch.is_empty();
        if wrote {
            writer.write(batch);
        }

        // Each bridge request sets our latch (SpiBridge::waking_worker), so
        // this sleeps until there is work; IDLE_WAIT only paces the loop's
        // housekeeping. It was a 1 ms poll: ~1,000 wakeups a second, idle.
        // Idle wait. wait_latch (not thread::sleep) so the latch wakes us
        // promptly when a procsignal (e.g. a barrier) arrives — the actual
        // interrupt processing happens via check_for_interrupts!() at the top
        // of the loop. wait_latch also gives this backend a real wait_event
        // in pg_stat_activity (PG_WAIT_EXTENSION). Returns false on SIGTERM
        // or postmaster death — break in either case.
        if processed == 0 && !wrote && !BackgroundWorker::wait_latch(Some(IDLE_WAIT)) {
            break;
        }
    }

    // Shutdown the runtime
    pgrx::log!("pg_mqtt worker {}: shutting down tokio runtime", worker_id);
    runtime.shutdown_timeout(Duration::from_secs(5));

    // Log active connections at shutdown
    let active = CONNECTION_COUNT.load(Ordering::SeqCst);
    if active > 0 {
        pgrx::log!(
            "pg_mqtt worker {}: shut down with {} active connection(s)",
            worker_id,
            active
        );
    }

    pgrx::log!("pg_mqtt worker {}: server stopped", worker_id);
    Ok(())
}

/// Async server implementation
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions
/// (like pgrx::log!) from here. Use eprintln! for logging instead.
async fn run_server_async(
    host: &str,
    port: u16,
    bridge: Arc<SpiBridge>,
    ingest: IngestQueue,
    worker_id: i32,
    worker_count: i32,
    max_queued: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Use SO_REUSEPORT to allow multiple workers to bind to the same port
    let listener = create_reuseport_listener(host, port)?;

    eprintln!(
        "pg_mqtt worker {}: listening on {}:{}",
        worker_id, host, port
    );

    // Create storage client using SPI bridge
    let storage = Arc::new(MqttStorageClient::new((*bridge).clone()));

    // Create broker state for message routing
    let broker_state = Arc::new(MqttBrokerState::for_worker(
        worker_id,
        worker_count,
        max_queued,
    ));

    // Create a notify for shutdown signaling to spawned tasks
    let shutdown_notify = Arc::new(Notify::new());

    // Deliver messages published from SQL or on other workers
    tokio::spawn(run_fanout(storage.clone(), broker_state.clone(), worker_id));

    // Accept loop with shutdown check
    loop {
        // Check for shutdown
        if is_shutdown_requested() {
            eprintln!(
                "pg_mqtt worker {}: shutdown requested, stopping accept loop",
                worker_id
            );
            shutdown_notify.notify_waiters();
            break;
        }

        // Use tokio::select! to handle both accept and a timeout for signal checking
        tokio::select! {
            // Accept new connections
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        eprintln!("pg_mqtt worker {}: new connection from {}", worker_id, addr);
                        let storage_clone = storage.clone();
                        let broker_state_clone = broker_state.clone();
                        let shutdown_notify_clone = shutdown_notify.clone();
                        let ingest_clone = ingest.clone();
                        let wid = worker_id;

                        tokio::spawn(async move {
                            if let Err(e) =
                                handle_connection(socket, storage_clone, broker_state_clone, ingest_clone, shutdown_notify_clone, wid)
                                    .await
                            {
                                // Don't log errors during shutdown
                                if !is_shutdown_requested() {
                                    eprintln!("pg_mqtt worker {}: connection error: {}", wid, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        // Don't log errors during shutdown
                        if !is_shutdown_requested() {
                            eprintln!("pg_mqtt worker {}: accept error: {}", worker_id, e);
                        }
                    }
                }
            }
            // Periodic timeout to check for shutdown signals
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Just loop back to check shutdown flag
            }
        }
    }

    // Give connections a moment to close gracefully
    tokio::time::sleep(Duration::from_millis(100)).await;

    eprintln!("pg_mqtt worker {}: async server stopped", worker_id);
    Ok(())
}

/// Poll `pgmqtt.outbox` and deliver new rows to this worker's subscribers
/// (design/pg_mqtt/fanout.md). Runs until shutdown.
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions.
async fn run_fanout(storage: Arc<MqttStorageClient>, state: Arc<MqttBrokerState>, worker_id: i32) {
    let mut cursor = FanoutCursor::new();
    let mut last_error: Option<String> = None;

    while !is_shutdown_requested() {
        tokio::time::sleep(POLL_INTERVAL).await;

        let result = match storage.poll_outbox(cursor.cursor(), worker_id).await {
            Ok((horizon, rows)) => {
                let rows = cursor.advance(horizon, rows);
                deliver_outbox_rows(&rows, &storage, &state).await
            }
            Err(e) => Err(e),
        };
        let result = match (result, cursor.cleanup_bound(std::time::Instant::now())) {
            (Ok(()), Some(bound)) => storage.cleanup_outbox(bound).await,
            (result, _) => result,
        };

        // Log each distinct failure once, not every 50 ms
        match result {
            Ok(()) => last_error = None,
            Err(e) => {
                let msg = e.to_string();
                if last_error.as_deref() != Some(msg.as_str()) && !is_shutdown_requested() {
                    eprintln!(
                        "pg_mqtt worker {}: outbox fan-out failed: {}",
                        worker_id, msg
                    );
                }
                last_error = Some(msg);
            }
        }
    }
}

/// How a connection ended, which decides whether its Will is published
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Close {
    /// DISCONNECT with a normal reason: the Will is discarded
    Graceful,
    /// DISCONNECT with reason 0x04: publish the Will anyway
    WillRequested,
    /// Socket closed, protocol error, keep-alive timeout or takeover
    Abnormal,
    /// The broker is stopping
    Shutdown,
}

/// Where an outgoing QoS 1 or 2 message is in its acknowledgement flow
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Awaiting {
    /// QoS 1, sent: waiting for PUBACK
    Puback,
    /// QoS 2, sent: waiting for PUBREC
    Pubrec,
    /// QoS 2, PUBREL sent: waiting for PUBCOMP
    Pubcomp,
}

/// Per-connection state
#[derive(Default)]
struct Connection {
    session: Option<ClientSession>,
    /// Messages the broker routes to this connection
    queue: Option<Arc<ClientQueue>>,
    /// Retained messages to send after a SUBACK, before what the queue holds.
    /// Not part of the queue's bound: a wildcard subscription may match more
    /// retained topics than a slow client is allowed to fall behind.
    retained: VecDeque<Delivery>,
    last_packet_id: u16,
    /// QoS 1 and 2 messages sent and not yet acknowledged
    inflight: HashMap<u16, Awaiting>,
    /// Packet ids of QoS 2 messages received and not yet released (PUBREL)
    received_qos2: HashSet<u16>,
    /// PUBLISHes queued for storage, oldest first: each is acknowledged and
    /// forwarded when its batch commits, in the order they arrived
    inbound: VecDeque<Inbound>,
}

/// What a connection answers a PUBLISH with once it is stored
enum Ack {
    /// QoS 0: nothing
    None,
    Puback(u16),
    Pubrec(u16),
}

/// A PUBLISH from this connection on its way to storage
struct Inbound {
    outcome: oneshot::Receiver<IngestOutcome>,
    ack: Ack,
    /// The message to forward once stored; None for a resent QoS 2 PUBLISH,
    /// which was stored (or is being) the first time
    packet: Option<PublishPacket>,
}

impl Inbound {
    /// An entry that is already answered: a resent QoS 2 PUBLISH gets its
    /// PUBREC again, in order, without being stored twice
    fn answered(ack: Ack) -> Self {
        let (tx, outcome) = oneshot::channel();
        let _ = tx.send(IngestOutcome::rejected(ReasonCode::Success));
        Self {
            outcome,
            ack,
            packet: None,
        }
    }
}

impl Connection {
    /// The MQTT version the client connected with; MQTT 5 until CONNECT
    fn version(&self) -> ProtocolVersion {
        self.session.as_ref().map(|s| s.version).unwrap_or_default()
    }

    /// What to send before closing the connection for `reason_code`
    fn disconnect(&self, reason_code: ReasonCode) -> BytesMut {
        disconnect_for(self.version(), reason_code)
    }

    /// Most QoS 1 and 2 messages in flight at once: the client's Receive
    /// Maximum, capped so a client that never acknowledges cannot grow it
    /// unbounded
    fn inflight_limit(&self) -> usize {
        let receive_maximum = self
            .session
            .as_ref()
            .map(|s| s.receive_maximum)
            .unwrap_or(u16::MAX);
        (receive_maximum as usize).min(MAX_INFLIGHT)
    }

    fn next_packet_id(&mut self) -> u16 {
        loop {
            self.last_packet_id = self.last_packet_id.wrapping_add(1);
            if self.last_packet_id != 0 && !self.inflight.contains_key(&self.last_packet_id) {
                return self.last_packet_id;
            }
        }
    }

    /// Whether another QoS 1 or 2 message may be sent: beyond the client's
    /// Receive Maximum they wait in the queue until an acknowledgement frees a
    /// slot (MQTT 5 4.9)
    fn acked_slot_free(&self) -> bool {
        self.inflight.len() < self.inflight_limit()
    }

    /// Encode and write a message, assigning a packet id for QoS 1 and 2
    async fn write(&mut self, socket: &mut TcpStream, delivery: Delivery) -> std::io::Result<()> {
        let packet_id = match delivery.qos {
            QoS::AtMostOnce => None,
            qos => {
                let id = self.next_packet_id();
                let awaiting = if qos == QoS::AtLeastOnce {
                    Awaiting::Puback
                } else {
                    Awaiting::Pubrec
                };
                self.inflight.insert(id, awaiting);
                Some(id)
            }
        };
        let packet = encode_publish(
            &PublishPacket {
                dup: false,
                qos: delivery.qos,
                retain: delivery.retain,
                topic: delivery.topic,
                packet_id,
                properties: delivery.properties,
                payload: delivery.payload,
            },
            self.version(),
        );
        // Never send a packet larger than the client accepts; such a message
        // is discarded as if delivered (MQTT 5 3.1.2.11.4)
        let too_large = self
            .session
            .as_ref()
            .and_then(|s| s.maximum_packet_size)
            .is_some_and(|max| packet.len() > max as usize);
        if too_large {
            if let Some(id) = packet_id {
                self.inflight.remove(&id);
            }
            return Ok(());
        }
        socket.write_all(&packet).await
    }
}

/// The server closes a connection that sent nothing for one and a half times
/// its keep-alive (MQTT 5 3.1.2.10); 0 turns the check off
fn keep_alive_limit(keep_alive: u16) -> Option<Duration> {
    (keep_alive > 0).then(|| Duration::from_millis(keep_alive as u64 * 1500))
}

/// Handle a single MQTT client connection
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions.
async fn handle_connection(
    mut socket: TcpStream,
    storage: Arc<MqttStorageClient>,
    broker_state: Arc<MqttBrokerState>,
    ingest: IngestQueue,
    shutdown_notify: Arc<Notify>,
    worker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    CONNECTION_COUNT.fetch_add(1, Ordering::SeqCst);

    let mut conn = Connection::default();
    let result = serve_connection(
        &mut socket,
        &mut conn,
        &storage,
        &broker_state,
        &ingest,
        &shutdown_notify,
        worker_id,
    )
    .await;

    // Messages this client published are stored whether or not it is still
    // here: forward them before anything else (its Will comes after them)
    if !is_shutdown_requested() {
        let publisher = conn.session.as_ref().map(|s| s.client_id.clone());
        for inbound in conn.inbound.drain(..) {
            if let (Some(packet), Ok(outcome)) = (inbound.packet, inbound.outcome.await) {
                if outcome.accepted() {
                    deliver_published(&packet, publisher.as_deref(), &outcome, &broker_state).await;
                }
            }
        }
    }

    // However the connection ended, let go of the client id and end its
    // session, then publish the Will unless the client disconnected normally
    // (MQTT 5 3.1.2.5)
    if let Some(session) = conn.session.take() {
        broker_state
            .unregister_client(&session.client_id, session.generation)
            .await;
        let dropped = conn.queue.as_ref().map_or(0, |q| q.dropped_qos0());
        if dropped > 0 {
            eprintln!(
                "pg_mqtt worker {}: client '{}' did not keep up, {} QoS 0 message(s) dropped",
                worker_id, session.client_id, dropped
            );
        }
        let close = result.as_ref().copied().unwrap_or(Close::Abnormal);
        if close != Close::Shutdown {
            end_session(&session.client_id, &storage, &broker_state).await;
        }
        if let (Some(will), Close::Abnormal | Close::WillRequested) = (session.will, close) {
            publish_will(&will, &session.client_id, &ingest, &broker_state).await;
        }
    }

    CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
    result.map(|_| ())
}

/// Read packets and forward routed messages until the connection ends
#[allow(clippy::too_many_arguments)]
async fn serve_connection(
    socket: &mut TcpStream,
    conn: &mut Connection,
    storage: &Arc<MqttStorageClient>,
    broker_state: &Arc<MqttBrokerState>,
    ingest: &IngestQueue,
    shutdown_notify: &Arc<Notify>,
    worker_id: i32,
) -> Result<Close, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = BytesMut::with_capacity(4096);
    let mut last_received = Instant::now();

    loop {
        // Check for shutdown
        if is_shutdown_requested() {
            eprintln!(
                "pg_mqtt worker {}: connection closing due to shutdown",
                worker_id
            );
            return Ok(Close::Shutdown);
        }

        let keep_alive_deadline = conn
            .session
            .as_ref()
            .and_then(|s| keep_alive_limit(s.keep_alive))
            .map(|limit| last_received + limit);

        // Retained messages first, as the client's Receive Maximum allows
        let acked_slot_free = conn.acked_slot_free();
        let next_retained = conn
            .retained
            .front()
            .is_some_and(|d| d.qos == QoS::AtMostOnce || acked_slot_free);
        if let Some(delivery) = next_retained.then(|| conn.retained.pop_front()).flatten() {
            if let Err(e) = conn.write(socket, delivery).await {
                if !is_shutdown_requested() {
                    eprintln!(
                        "pg_mqtt worker {}: failed to send a retained message: {}",
                        worker_id, e
                    );
                }
                return Ok(Close::Abnormal);
            }
            continue;
        }

        let queue = conn.queue.clone();

        // Use select to handle reading, forwarded messages, keep-alive and shutdown
        tokio::select! {
            // Wait for shutdown notification
            _ = shutdown_notify.notified() => {
                eprintln!("pg_mqtt worker {}: connection closing due to shutdown notification", worker_id);
                return Ok(Close::Shutdown);
            }
            // Nothing received within the keep-alive limit
            _ = async {
                match keep_alive_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                let _ = socket.write_all(&conn.disconnect(ReasonCode::KeepAliveTimeout)).await;
                return Ok(Close::Abnormal);
            }
            // The oldest queued PUBLISH was stored (or refused): answer it
            outcome = async {
                match conn.inbound.front_mut() {
                    Some(inbound) => (&mut inbound.outcome).await,
                    None => std::future::pending().await,
                }
            } => {
                let inbound = conn.inbound.pop_front().expect("the front was awaited");
                let outcome = outcome
                    .unwrap_or_else(|_| IngestOutcome::rejected(ReasonCode::ServerUnavailable));
                // A client that waits for this answer is not idle
                last_received = Instant::now();
                if let Err(e) = answer_publish(socket, conn, broker_state, inbound, outcome).await {
                    if !is_shutdown_requested() {
                        eprintln!("pg_mqtt worker {}: failed to acknowledge: {}", worker_id, e);
                    }
                    return Ok(Close::Abnormal);
                }
            }
            // Messages routed to this client
            next = async {
                match &queue {
                    Some(queue) => queue.next(acked_slot_free).await,
                    None => std::future::pending().await,
                }
            } => {
                match next {
                    Next::Deliver(delivery) => {
                        if let Err(e) = conn.write(socket, delivery).await {
                            if !is_shutdown_requested() {
                                eprintln!("pg_mqtt worker {}: failed to forward message: {}", worker_id, e);
                            }
                            return Ok(Close::Abnormal);
                        }
                    }
                    // Taken over, or too far behind
                    Next::Close(reason_code) => {
                        let _ = socket.write_all(&conn.disconnect(reason_code)).await;
                        linger_close(socket).await;
                        return Ok(Close::Abnormal);
                    }
                }
            }
            // Read data into buffer, unless the client has as many PUBLISHes
            // waiting for storage as it may: then it waits (backpressure)
            result = socket.read_buf(&mut buf), if conn.inbound.len() < MAX_INBOUND => {
                match result {
                    Ok(0) => {
                        // Connection closed by client
                        if !is_shutdown_requested() {
                            eprintln!("pg_mqtt worker {}: connection closed by client", worker_id);
                        }
                        return Ok(Close::Abnormal);
                    }
                    Ok(_n) => {
                        last_received = Instant::now();
                        // Process complete MQTT packets
                        loop {
                            match parse_packet(&buf, conn.version()) {
                                Ok(Some((packet, consumed))) => {
                                    // Remove consumed bytes from buffer
                                    let _ = buf.split_to(consumed);

                                    match handle_mqtt_packet(
                                        packet,
                                        conn,
                                        storage,
                                        broker_state,
                                        ingest,
                                        socket,
                                    ).await {
                                        Ok(None) => {}
                                        Ok(Some(close)) => return Ok(close),
                                        Err(response) => {
                                            // Answer what was accepted before the
                                            // offending packet, then the error, and close
                                            flush_inbound(socket, conn, broker_state).await;
                                            let _ = socket.write_all(&response).await;
                                            linger_close(socket).await;
                                            return Ok(Close::Abnormal);
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // Need more data
                                    break;
                                }
                                Err(e) => {
                                    if !is_shutdown_requested() {
                                        eprintln!("pg_mqtt worker {}: parse error: {}", worker_id, e);
                                    }
                                    // A CONNECT of a version we do not speak gets the
                                    // MQTT 3.1.1 refusal every version understands
                                    // (MQTT 3.1.1 3.1.2-2); anything else unparsable
                                    // is a malformed packet
                                    let response = match e {
                                        ProtocolError::UnsupportedVersion(_) => {
                                            BytesMut::from(&[0x20, 0x02, 0x00, 0x01][..])
                                        }
                                        _ => conn.disconnect(ReasonCode::MalformedPacket),
                                    };
                                    let _ = socket.write_all(&response).await;
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}

/// How long a closing connection is read from after the broker's FIN
const LINGER: Duration = Duration::from_secs(1);

/// Close after an error without resetting the connection: send FIN, then read
/// and discard until the client closes too (or LINGER passes). A client still
/// answering what it received (PUBRELs for PUBRECs) would otherwise get a TCP
/// reset and fail on its next write, before it reads the DISCONNECT.
async fn linger_close(socket: &mut TcpStream) {
    let _ = socket.shutdown().await;
    let mut discard = [0u8; 4096];
    let _ = tokio::time::timeout(LINGER, async {
        while matches!(socket.read(&mut discard).await, Ok(n) if n > 0) {}
    })
    .await;
}

/// Answer every queued PUBLISH, in order, once its batch committed
async fn flush_inbound(
    socket: &mut TcpStream,
    conn: &mut Connection,
    broker_state: &MqttBrokerState,
) {
    while let Some(mut inbound) = conn.inbound.pop_front() {
        let outcome = (&mut inbound.outcome)
            .await
            .unwrap_or_else(|_| IngestOutcome::rejected(ReasonCode::ServerUnavailable));
        if answer_publish(socket, conn, broker_state, inbound, outcome)
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Answer a PUBLISH whose batch committed: forward it, then acknowledge it
async fn answer_publish(
    socket: &mut TcpStream,
    conn: &mut Connection,
    broker_state: &MqttBrokerState,
    inbound: Inbound,
    outcome: IngestOutcome,
) -> std::io::Result<()> {
    let version = conn.version();
    if let Some(packet) = &inbound.packet {
        if outcome.accepted() {
            let publisher = conn.session.as_ref().map(|s| s.client_id.as_str());
            deliver_published(packet, publisher, &outcome, broker_state).await;
        }
    }
    match inbound.ack {
        Ack::None => Ok(()),
        Ack::Puback(packet_id) => {
            socket
                .write_all(&encode_puback(packet_id, outcome.reason_code, version))
                .await
        }
        Ack::Pubrec(packet_id) => {
            // A refused QoS 2 message ends here: no PUBREL follows
            if !outcome.accepted() {
                conn.received_qos2.remove(&packet_id);
            }
            socket
                .write_all(&encode_pubrec(packet_id, outcome.reason_code, version))
                .await
        }
    }
}

/// The session of a connection past CONNECT
fn session_of(conn: &Connection) -> &ClientSession {
    conn.session
        .as_ref()
        .expect("only CONNECT is handled before a session exists")
}

/// Handle a single MQTT packet
///
/// Returns Ok(Some(close)) when the client disconnected, Ok(None) to
/// continue, or Err(response) to send an error and close.
#[allow(clippy::too_many_arguments)]
async fn handle_mqtt_packet(
    packet: MqttPacket,
    conn: &mut Connection,
    storage: &Arc<MqttStorageClient>,
    broker_state: &Arc<MqttBrokerState>,
    ingest: &IngestQueue,
    socket: &mut TcpStream,
) -> Result<Option<Close>, BytesMut> {
    let write_failed = |_| BytesMut::new();

    // Before CONNECT only CONNECT is allowed (MQTT 5 3.1.0-1)
    if conn.session.is_none() && !matches!(packet, MqttPacket::Connect(_)) {
        return Err(BytesMut::new());
    }
    let client_id = conn
        .session
        .as_ref()
        .map(|s| s.client_id.clone())
        .unwrap_or_default();
    let version = conn.version();

    match packet {
        MqttPacket::Connect(connect) => {
            // A second CONNECT is a protocol error (MQTT 5 3.1.0-2)
            if conn.session.is_some() {
                return Err(conn.disconnect(ReasonCode::ProtocolError));
            }
            let (response, mut new_session) =
                handle_connect(&connect, storage, broker_state).await?;
            // Register client for message routing and get receiver for forwarded messages
            let (queue, generation) = broker_state.register_client(&new_session.client_id).await;
            new_session.generation = generation;
            conn.queue = Some(queue);
            conn.session = Some(new_session);
            socket.write_all(&response).await.map_err(write_failed)?;
            Ok(None)
        }

        MqttPacket::Publish(publish) => {
            check_publish(&publish, session_of(conn))?;
            let ack = match (publish.qos, publish.packet_id) {
                (QoS::AtMostOnce, _) => Ack::None,
                (QoS::AtLeastOnce, Some(packet_id)) => Ack::Puback(packet_id),
                (QoS::ExactlyOnce, Some(packet_id)) => {
                    // A resent PUBLISH whose PUBREC was lost is not stored
                    // again: exactly once (MQTT 5 4.3.3)
                    if conn.received_qos2.contains(&packet_id) {
                        conn.inbound
                            .push_back(Inbound::answered(Ack::Pubrec(packet_id)));
                        return Ok(None);
                    }
                    if conn.received_qos2.len() >= SERVER_RECEIVE_MAXIMUM as usize {
                        return Err(conn.disconnect(ReasonCode::ReceiveMaximumExceeded));
                    }
                    conn.received_qos2.insert(packet_id);
                    Ack::Pubrec(packet_id)
                }
                // QoS 1 or 2 without a packet id cannot be parsed
                (_, None) => return Err(conn.disconnect(ReasonCode::MalformedPacket)),
            };
            // Waits while the ingest queue is full: backpressure
            let outcome = ingest.submit(publish.clone(), Some(client_id)).await;
            conn.inbound.push_back(Inbound {
                outcome,
                ack,
                packet: Some(publish),
            });
            Ok(None)
        }

        // The client released a QoS 2 message it sent
        MqttPacket::Pubrel { packet_id } => {
            conn.received_qos2.remove(&packet_id);
            socket
                .write_all(&encode_pubcomp(packet_id))
                .await
                .map_err(write_failed)?;
            Ok(None)
        }

        MqttPacket::Subscribe(subscribe) => {
            let (response, retained) =
                handle_subscribe(&subscribe, &client_id, version, storage).await;
            socket.write_all(&response).await.map_err(write_failed)?;
            // Retained messages follow the SUBACK
            conn.retained.extend(retained);
            Ok(None)
        }

        MqttPacket::Unsubscribe(unsubscribe) => {
            let response = handle_unsubscribe(&unsubscribe, &client_id, version, storage).await;
            socket.write_all(&response).await.map_err(write_failed)?;
            Ok(None)
        }

        MqttPacket::Pingreq => {
            let response = handle_pingreq();
            socket.write_all(&response).await.map_err(write_failed)?;
            Ok(None)
        }

        MqttPacket::Disconnect(disconnect) => Ok(Some(if handle_disconnect(&disconnect) {
            Close::WillRequested
        } else {
            Close::Graceful
        })),

        // The client acknowledged a QoS 1 message we sent
        MqttPacket::Puback { packet_id } => {
            if conn.inflight.get(&packet_id) == Some(&Awaiting::Puback) {
                conn.inflight.remove(&packet_id);
            }
            Ok(None)
        }

        // The client received a QoS 2 message we sent: release it, unless it
        // refused it, which ends the flow (MQTT 5 4.3.3)
        MqttPacket::Pubrec {
            packet_id,
            reason_code,
        } => {
            if conn.inflight.get(&packet_id) == Some(&Awaiting::Pubrec) {
                if reason_code >= 0x80 {
                    conn.inflight.remove(&packet_id);
                } else {
                    conn.inflight.insert(packet_id, Awaiting::Pubcomp);
                    socket
                        .write_all(&encode_pubrel(packet_id))
                        .await
                        .map_err(write_failed)?;
                }
            }
            Ok(None)
        }

        // The client completed a QoS 2 message we sent
        MqttPacket::Pubcomp { packet_id } => {
            if conn.inflight.get(&packet_id) == Some(&Awaiting::Pubcomp) {
                conn.inflight.remove(&packet_id);
            }
            Ok(None)
        }

        MqttPacket::Auth => {
            // Enhanced auth not supported
            Ok(None)
        }

        // These are server-to-client packets, shouldn't receive them
        MqttPacket::Connack(_)
        | MqttPacket::Suback(_)
        | MqttPacket::Unsuback(_)
        | MqttPacket::Pingresp => {
            // Protocol error - received server packet from client
            Err(conn.disconnect(ReasonCode::ProtocolError))
        }
    }
}

/// Get the current connection count
#[allow(dead_code)]
pub fn get_connection_count() -> u32 {
    CONNECTION_COUNT.load(Ordering::SeqCst)
}
