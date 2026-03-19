//! S3 HTTP server with SPI bridge
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

use pg_spi::{execute_spi_request, SpiBridge};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

use crate::s3;
use crate::storage::spi_client::S3StorageClient;

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

    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;

    let addr: SocketAddr = format!("{}:{}", addr, port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    socket.set_nonblocking(true)?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Start the S3 HTTP server with SPI bridge
///
/// This function sets up the tokio runtime and runs it in a way that allows
/// polling for SPI requests on the main thread.
pub fn run_server(
    host: &str,
    port: u16,
    data_dir: &str,
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
    let data_dir_owned = data_dir.to_string();
    let bridge_clone = bridge.clone();

    let _server_handle = runtime.spawn(async move {
        if let Err(e) =
            run_server_async(&host_owned, port, &data_dir_owned, bridge_clone, worker_id).await
        {
            eprintln!("pg_s3 worker {}: server error: {}", worker_id, e);
        }
    });

    // Main polling loop - runs on bgworker thread where SPI is valid
    use pgrx::bgworkers::BackgroundWorker;

    pgrx::log!("pg_s3 worker {}: starting SPI polling loop", worker_id);

    loop {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!(
                "pg_s3 worker {}: received SIGTERM, initiating shutdown",
                worker_id
            );
            request_shutdown();
            break;
        }

        let mut processed = 0;
        while let Some(request) = receiver.try_recv() {
            execute_spi_request(request);
            processed += 1;
            if processed >= 100 {
                break;
            }
        }

        if processed == 0 {
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    pgrx::log!("pg_s3 worker {}: shutting down tokio runtime", worker_id);
    runtime.shutdown_timeout(Duration::from_secs(5));

    pgrx::log!("pg_s3 worker {}: server stopped", worker_id);
    Ok(())
}

/// Async server implementation running on tokio threads
async fn run_server_async(
    host: &str,
    port: u16,
    data_dir: &str,
    bridge: Arc<SpiBridge>,
    worker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = create_reuseport_listener(host, port)?;

    // Initialize content store directory
    crate::storage::ContentStore::ensure_directories(data_dir).await?;

    let storage = Arc::new(S3StorageClient::new(bridge));

    eprintln!(
        "pg_s3 worker {}: HTTP server listening on {}:{}",
        worker_id, host, port
    );

    loop {
        if is_shutdown_requested() {
            eprintln!(
                "pg_s3 worker {}: shutdown requested, stopping accept loop",
                worker_id
            );
            break;
        }

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let storage_clone = storage.clone();
                        let data_dir = data_dir.to_string();
                        let wid = worker_id;

                        tokio::spawn(async move {
                            if let Err(e) = s3::handle_connection(stream, storage_clone, data_dir, wid).await {
                                if !is_shutdown_requested() {
                                    eprintln!("pg_s3 worker {}: connection error: {}", wid, e);
                                }
                            }
                        });
                    }
                    Err(e) => {
                        if !is_shutdown_requested() {
                            eprintln!("pg_s3 worker {}: accept error: {}", worker_id, e);
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // Periodic check for shutdown
            }
        }
    }

    Ok(())
}
