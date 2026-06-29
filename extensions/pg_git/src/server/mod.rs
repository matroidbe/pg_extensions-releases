//! HTTP git server module.
//!
//! Runs a hyper HTTP server inside a PostgreSQL background worker,
//! following the pg_kafka/pg_mqtt pattern.

pub mod git_http;
pub mod http;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use pg_spi::{execute_spi_request, SpiBridge, SpiReceiver};
use pgrx::bgworkers::BackgroundWorker;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn request_shutdown() {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Run the HTTP git server with integrated SPI polling.
pub fn run_server(host: &str, port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);

    let (bridge, mut receiver): (SpiBridge, SpiReceiver) = SpiBridge::new(256);
    let bridge: Arc<SpiBridge> = Arc::new(bridge);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    let host_owned = host.to_string();
    let bridge_clone = bridge.clone();
    let _server_handle = runtime.spawn(async move {
        if let Err(e) = run_http_server(&host_owned, port, bridge_clone).await {
            pgrx::log!("pg_git: HTTP server error: {}", e);
        }
    });

    pgrx::log!("pg_git: HTTP server listening on {}:{}", host, port);

    // Main SPI polling loop
    loop {
        if BackgroundWorker::sigterm_received() {
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

        // Idle wait. Must use wait_latch (not thread::sleep) so the bgworker
        // processes Postgres interrupts — in particular ProcSignalBarrier
        // (SIGUSR1), which is how DROP DATABASE asks every backend to release
        // its connection. thread::sleep stays inside libc and never runs the
        // CFI that handles the barrier, so DROP DATABASE hangs indefinitely.
        // wait_latch also gives this backend a real wait_event in
        // pg_stat_activity (PG_WAIT_EXTENSION). Returns false on SIGTERM or
        // postmaster death — break in either case.
        if processed == 0 && !BackgroundWorker::wait_latch(Some(Duration::from_millis(1))) {
            break;
        }
    }

    runtime.shutdown_timeout(Duration::from_secs(5));
    pgrx::log!("pg_git: HTTP server stopped");
    Ok(())
}

async fn run_http_server(
    host: &str,
    port: u16,
    bridge: Arc<SpiBridge>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = create_listener(host, port)?;

    loop {
        if is_shutdown_requested() {
            break;
        }

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => {
                        let bridge_clone = bridge.clone();
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let service = service_fn(move |req| {
                                let bridge = bridge_clone.clone();
                                async move {
                                    http::handle_request(req, bridge).await
                                }
                            });

                            if let Err(e) = http1::Builder::new()
                                .serve_connection(io, service)
                                .await
                            {
                                pgrx::log!("pg_git: connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        pgrx::log!("pg_git: accept error: {}", e);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    Ok(())
}

fn create_listener(host: &str, port: u16) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;

    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{}", e)))?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    socket.set_nonblocking(true)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}
