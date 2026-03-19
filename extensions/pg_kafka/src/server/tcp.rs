//! TCP server implementation using tokio with SPI bridge
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

use bytes::{Buf, BytesMut};
use socket2::{Domain, Protocol, Socket, Type};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::protocol::{frame_message, handle_request, parse_request_header, write_response_header};
use crate::storage::{execute_spi_request, SpiBridge, SpiStorageClient};
use pg_observability::{
    decrement_connections, describe_kafka_metrics, error, increment_connections, info,
    init_prometheus_on_port, init_tracing, warn,
};

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

/// Metrics configuration for the server
pub struct MetricsConfig {
    /// Whether metrics endpoint is enabled
    pub enabled: bool,
    /// Port for the Prometheus metrics endpoint
    pub port: u16,
}

/// Start the TCP server with SPI bridge
///
/// This function sets up the tokio runtime and runs it in a way that allows
/// polling for SPI requests on the main thread.
///
/// Each worker binds to the same port using SO_REUSEPORT, allowing the kernel
/// to load-balance connections across workers.
///
/// If `metrics_config` is provided and enabled, worker 0 will start a Prometheus
/// metrics endpoint on the specified port.
pub fn run_server(
    host: &str,
    port: u16,
    advertised_host: &str,
    worker_id: i32,
    metrics_config: Option<MetricsConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing before anything else
    init_tracing();

    // Register Kafka metric descriptions (once at startup)
    describe_kafka_metrics();

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
    let advertised_host_owned = advertised_host.to_string();
    let bridge_clone = bridge.clone();

    let _server_handle = runtime.spawn(async move {
        if let Err(e) = run_server_async(
            &host_owned,
            port,
            &advertised_host_owned,
            bridge_clone,
            worker_id,
            metrics_config,
        )
        .await
        {
            // NOTE: Cannot use pgrx::warning! here as this runs on tokio threads
            error!(worker_id, error = %e, "Server error");
        }
    });

    // Main polling loop - runs on bgworker thread where SPI is valid
    // Import pgrx here since this runs on main thread where FFI is valid
    use pgrx::bgworkers::BackgroundWorker;

    pgrx::log!("pg_kafka worker {}: starting SPI polling loop", worker_id);

    loop {
        // Check for shutdown signal from PostgreSQL
        if BackgroundWorker::sigterm_received() {
            pgrx::log!(
                "pg_kafka worker {}: received SIGTERM, initiating shutdown",
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
    pgrx::log!("pg_kafka worker {}: shutting down tokio runtime", worker_id);
    runtime.shutdown_timeout(Duration::from_secs(5));

    // Log active connections at shutdown
    let active = CONNECTION_COUNT.load(Ordering::SeqCst);
    if active > 0 {
        pgrx::log!(
            "pg_kafka worker {}: shut down with {} active connection(s)",
            worker_id,
            active
        );
    }

    pgrx::log!("pg_kafka worker {}: server stopped", worker_id);
    Ok(())
}

/// Async server implementation
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions
/// (like pgrx::log!) from here. Use tracing macros (info!, warn!, error!) instead.
async fn run_server_async(
    host: &str,
    port: u16,
    advertised_host: &str,
    bridge: Arc<SpiBridge>,
    worker_id: i32,
    metrics_config: Option<MetricsConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize Prometheus metrics endpoint (worker 0 only)
    // This MUST be called from within the tokio runtime
    if worker_id == 0 {
        if let Some(ref config) = metrics_config {
            if config.enabled {
                match init_prometheus_on_port(config.port) {
                    Ok(()) => {
                        info!(
                            worker_id,
                            metrics_port = config.port,
                            "Prometheus metrics endpoint started"
                        );
                    }
                    Err(e) => {
                        warn!(worker_id, error = %e, "Failed to start Prometheus metrics endpoint");
                    }
                }
            }
        }
    }

    // Use SO_REUSEPORT to allow multiple workers to bind to the same port
    let listener = create_reuseport_listener(host, port)?;

    info!(worker_id, host, port, "Kafka server listening");

    // Create storage client using SPI bridge
    let storage = Arc::new(SpiStorageClient::new(bridge));

    // Create a notify for shutdown signaling to spawned tasks
    let shutdown_notify = Arc::new(Notify::new());

    // Accept loop with shutdown check
    loop {
        // Check for shutdown
        if is_shutdown_requested() {
            info!(worker_id, "Shutdown requested, stopping accept loop");
            shutdown_notify.notify_waiters();
            break;
        }

        // Use tokio::select! to handle both accept and a timeout for signal checking
        tokio::select! {
            // Accept new connections
            result = listener.accept() => {
                match result {
                    Ok((socket, addr)) => {
                        info!(worker_id, %addr, "New connection accepted");
                        let storage_clone = storage.clone();
                        let advertised_host_clone = advertised_host.to_string();
                        let port_i32 = port as i32;
                        let shutdown_notify_clone = shutdown_notify.clone();
                        let wid = worker_id;

                        tokio::spawn(async move {
                            if let Err(e) =
                                handle_connection(socket, storage_clone, &advertised_host_clone, port_i32, shutdown_notify_clone, wid)
                                    .await
                            {
                                // Don't log errors during shutdown
                                if !is_shutdown_requested() {
                                    error!(worker_id = wid, error = %e, "Connection error");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        // Don't log errors during shutdown
                        if !is_shutdown_requested() {
                            error!(worker_id, error = %e, "Accept error");
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

    info!(worker_id, "Async server stopped");
    Ok(())
}

/// Handle a single client connection
///
/// NOTE: This runs on tokio worker threads, so we cannot call pgrx FFI functions.
/// Use tracing macros (info!, warn!, error!) for logging.
async fn handle_connection(
    mut socket: TcpStream,
    storage: Arc<SpiStorageClient>,
    broker_host: &str,
    broker_port: i32,
    shutdown_notify: Arc<Notify>,
    worker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    CONNECTION_COUNT.fetch_add(1, Ordering::SeqCst);
    increment_connections(); // Prometheus gauge

    let mut buf = BytesMut::with_capacity(4096);

    loop {
        // Check for shutdown
        if is_shutdown_requested() {
            info!(worker_id, "Connection closing due to shutdown");
            break;
        }

        // Use select to handle both reading and shutdown notification
        tokio::select! {
            // Wait for shutdown notification
            _ = shutdown_notify.notified() => {
                info!(worker_id, "Connection closing due to shutdown notification");
                break;
            }
            // Read data into buffer with timeout
            result = socket.read_buf(&mut buf) => {
                match result {
                    Ok(0) => {
                        // Connection closed by client
                        if !is_shutdown_requested() {
                            info!(worker_id, "Connection closed by client");
                        }
                        break;
                    }
                    Ok(_n) => {
                        // Process complete messages
                        while buf.len() >= 4 {
                            // Read message size (first 4 bytes)
                            let size = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

                            // Check if we have the complete message
                            if buf.len() < 4 + size {
                                break; // Need more data
                            }

                            // Extract the message
                            buf.advance(4); // Skip size prefix
                            let message_bytes = buf.split_to(size);

                            // Parse and handle the request
                            let mut cursor = Cursor::new(message_bytes.as_ref());

                            match parse_request_header(&mut cursor) {
                                Ok(header) => {
                                    // Handle the request
                                    match handle_request(&header, &mut cursor, broker_host, broker_port, &storage)
                                        .await
                                    {
                                        Ok(response_body) => {
                                            // Build response with header
                                            let mut response = BytesMut::with_capacity(4 + response_body.len());
                                            write_response_header(&mut response, header.correlation_id);
                                            response.extend_from_slice(&response_body);

                                            // Frame and send
                                            let framed = frame_message(response);
                                            socket.write_all(&framed).await?;
                                        }
                                        Err(e) => {
                                            if !is_shutdown_requested() {
                                                warn!(worker_id, error = %e, "Request handling error");
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    if !is_shutdown_requested() {
                                        warn!(worker_id, error = %e, "Header parse error");
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if !is_shutdown_requested() {
                            return Err(e.into());
                        }
                        break;
                    }
                }
            }
        }
    }

    CONNECTION_COUNT.fetch_sub(1, Ordering::SeqCst);
    decrement_connections(); // Prometheus gauge
    Ok(())
}

/// Get the current connection count
#[allow(dead_code)]
pub fn get_connection_count() -> u32 {
    CONNECTION_COUNT.load(Ordering::SeqCst)
}
