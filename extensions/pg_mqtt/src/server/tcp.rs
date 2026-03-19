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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Notify};

use crate::protocol::codec::*;
use crate::protocol::handlers::*;
use crate::protocol::types::*;
use crate::storage::source_insert::MqttSourceInserter;
use crate::storage::{execute_spi_request, MqttStorageClient, SpiBridge};
use crate::validation::MqttSchemaValidator;

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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Reset shutdown flag at start
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

    // Create SPI bridge with capacity for pending requests
    let (bridge, mut receiver) = SpiBridge::new(1024);
    let bridge = Arc::new(bridge);

    // Create the tokio runtime
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    // Spawn the server task
    let host_owned = host.to_string();
    let bridge_clone = bridge.clone();

    let _server_handle = runtime.spawn(async move {
        if let Err(e) = run_server_async(&host_owned, port, bridge_clone, worker_id).await {
            // NOTE: Cannot use pgrx::warning! here as this runs on tokio threads
            eprintln!("pg_mqtt worker {}: server error: {}", worker_id, e);
        }
    });

    // Main polling loop - runs on bgworker thread where SPI is valid
    // Import pgrx here since this runs on main thread where FFI is valid
    use pgrx::bgworkers::BackgroundWorker;

    pgrx::log!("pg_mqtt worker {}: starting SPI polling loop", worker_id);

    loop {
        // Check for shutdown signal from PostgreSQL
        if BackgroundWorker::sigterm_received() {
            pgrx::log!(
                "pg_mqtt worker {}: received SIGTERM, initiating shutdown",
                worker_id
            );
            request_shutdown();
            break;
        }

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

        // Small sleep to avoid busy-waiting when there are no requests
        // This also allows checking for shutdown signals
        if processed == 0 {
            std::thread::sleep(Duration::from_micros(100));
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
    worker_id: i32,
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
    let broker_state = Arc::new(MqttBrokerState::new());

    // Create schema validator and source inserter for typed topics
    let validator = Arc::new(MqttSchemaValidator::new(bridge.clone()));
    let inserter = Arc::new(MqttSourceInserter::new(bridge.clone()));

    // Create a notify for shutdown signaling to spawned tasks
    let shutdown_notify = Arc::new(Notify::new());

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
                        let validator_clone = validator.clone();
                        let inserter_clone = inserter.clone();
                        let wid = worker_id;

                        tokio::spawn(async move {
                            if let Err(e) =
                                handle_connection(socket, storage_clone, broker_state_clone, validator_clone, inserter_clone, shutdown_notify_clone, wid)
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

/// Handle a single MQTT client connection
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions.
async fn handle_connection(
    mut socket: TcpStream,
    storage: Arc<MqttStorageClient>,
    broker_state: Arc<MqttBrokerState>,
    validator: Arc<MqttSchemaValidator>,
    inserter: Arc<MqttSourceInserter>,
    shutdown_notify: Arc<Notify>,
    worker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    CONNECTION_COUNT.fetch_add(1, Ordering::SeqCst);

    let mut buf = BytesMut::with_capacity(4096);
    let mut session: Option<ClientSession> = None;
    // Receiver for messages forwarded from other clients (via broker)
    let mut message_rx: Option<broadcast::Receiver<BytesMut>> = None;

    loop {
        // Check for shutdown
        if is_shutdown_requested() {
            eprintln!(
                "pg_mqtt worker {}: connection closing due to shutdown",
                worker_id
            );
            break;
        }

        // Use select to handle reading, forwarded messages, and shutdown notification
        tokio::select! {
            // Wait for shutdown notification
            _ = shutdown_notify.notified() => {
                eprintln!("pg_mqtt worker {}: connection closing due to shutdown notification", worker_id);
                break;
            }
            // Receive forwarded messages from other clients
            msg = async {
                match &mut message_rx {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match msg {
                    Ok(data) => {
                        // Forward the publish message to this client
                        if let Err(e) = socket.write_all(&data).await {
                            if !is_shutdown_requested() {
                                eprintln!("pg_mqtt worker {}: failed to forward message: {}", worker_id, e);
                            }
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // Client is slow, some messages were dropped
                        eprintln!("pg_mqtt worker {}: client lagged, dropped {} messages", worker_id, n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // Channel closed, likely during shutdown
                        break;
                    }
                }
            }
            // Read data into buffer with timeout
            result = socket.read_buf(&mut buf) => {
                match result {
                    Ok(0) => {
                        // Connection closed by client
                        if !is_shutdown_requested() {
                            eprintln!("pg_mqtt worker {}: connection closed by client", worker_id);
                        }
                        break;
                    }
                    Ok(_n) => {
                        // Process complete MQTT packets
                        loop {
                            match parse_packet(&buf) {
                                Ok(Some((packet, consumed))) => {
                                    // Remove consumed bytes from buffer
                                    let _ = buf.split_to(consumed);

                                    // Handle the packet
                                    match handle_mqtt_packet(
                                        packet,
                                        &mut session,
                                        &mut message_rx,
                                        &storage,
                                        &broker_state,
                                        &validator,
                                        &inserter,
                                        &mut socket,
                                    ).await {
                                        Ok(should_disconnect) => {
                                            if should_disconnect {
                                                // Clean disconnect
                                                if let Some(ref sess) = session {
                                                    broker_state.unregister_client(&sess.client_id).await;
                                                }
                                                CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
                                                return Ok(());
                                            }
                                        }
                                        Err(response) => {
                                            // Send error response and close
                                            let _ = socket.write_all(&response).await;
                                            CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
                                            return Ok(());
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
                                    CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if !is_shutdown_requested() {
                            CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
                            return Err(e.into());
                        }
                        break;
                    }
                }
            }
        }
    }

    // Cleanup on disconnect
    if let Some(ref sess) = session {
        broker_state.unregister_client(&sess.client_id).await;
    }

    CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
    Ok(())
}

/// Handle a single MQTT packet
///
/// Returns Ok(true) if the connection should be closed (normal disconnect),
/// Ok(false) to continue, or Err(response) to send an error and close.
#[allow(clippy::too_many_arguments)]
async fn handle_mqtt_packet(
    packet: MqttPacket,
    session: &mut Option<ClientSession>,
    message_rx: &mut Option<broadcast::Receiver<BytesMut>>,
    storage: &Arc<MqttStorageClient>,
    broker_state: &Arc<MqttBrokerState>,
    validator: &Arc<MqttSchemaValidator>,
    inserter: &Arc<MqttSourceInserter>,
    socket: &mut TcpStream,
) -> Result<bool, BytesMut> {
    match packet {
        MqttPacket::Connect(connect) => {
            match handle_connect(&connect, storage, broker_state).await {
                Ok((response, new_session)) => {
                    // Register client for message routing and get receiver for forwarded messages
                    let rx = broker_state.register_client(&new_session.client_id).await;
                    *message_rx = Some(rx);
                    *session = Some(new_session);
                    socket
                        .write_all(&response)
                        .await
                        .map_err(|_| response.clone())?;
                    Ok(false)
                }
                Err(response) => Err(response),
            }
        }

        MqttPacket::Publish(publish) => {
            if session.is_none() {
                // Must be connected first
                return Err(BytesMut::new());
            }

            if let Ok(Some(resp)) =
                handle_publish(&publish, storage, broker_state, validator, inserter).await
            {
                socket.write_all(&resp).await.map_err(|_| resp.clone())?;
            }
            Ok(false)
        }

        MqttPacket::Subscribe(subscribe) => {
            let client_id = session.as_ref().map(|s| s.client_id.as_str()).unwrap_or("");
            if client_id.is_empty() {
                return Err(BytesMut::new());
            }

            let response = handle_subscribe(&subscribe, client_id, storage).await;
            socket
                .write_all(&response)
                .await
                .map_err(|_| response.clone())?;
            Ok(false)
        }

        MqttPacket::Unsubscribe(unsubscribe) => {
            let client_id = session.as_ref().map(|s| s.client_id.as_str()).unwrap_or("");
            if client_id.is_empty() {
                return Err(BytesMut::new());
            }

            let response = handle_unsubscribe(&unsubscribe, client_id, storage).await;
            socket
                .write_all(&response)
                .await
                .map_err(|_| response.clone())?;
            Ok(false)
        }

        MqttPacket::Pingreq => {
            let response = handle_pingreq();
            socket
                .write_all(&response)
                .await
                .map_err(|_| response.clone())?;
            Ok(false)
        }

        MqttPacket::Disconnect(disconnect) => {
            let client_id = session.as_ref().map(|s| s.client_id.as_str()).unwrap_or("");
            if !client_id.is_empty() {
                handle_disconnect(&disconnect, client_id, storage, broker_state).await;
            }
            Ok(true) // Signal to close connection
        }

        // QoS acknowledgment packets - not supported in QoS 0 only mode
        MqttPacket::Puback { .. }
        | MqttPacket::Pubrec { .. }
        | MqttPacket::Pubrel { .. }
        | MqttPacket::Pubcomp { .. } => {
            // Ignore - we only support QoS 0
            Ok(false)
        }

        MqttPacket::Auth => {
            // Enhanced auth not supported
            Ok(false)
        }

        // These are server-to-client packets, shouldn't receive them
        MqttPacket::Connack(_)
        | MqttPacket::Suback(_)
        | MqttPacket::Unsuback(_)
        | MqttPacket::Pingresp => {
            // Protocol error - received server packet from client
            Err(BytesMut::new())
        }
    }
}

/// Get the current connection count
#[allow(dead_code)]
pub fn get_connection_count() -> u32 {
    CONNECTION_COUNT.load(Ordering::SeqCst)
}
