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
    // No pgrx calls on tokio threads: the error is returned to this thread
    let server_handle = runtime.spawn(async move {
        run_http_server(&host_owned, port, bridge_clone)
            .await
            .map_err(|e| e.to_string())
    });

    pgrx::log!("pg_git: HTTP server listening on {}:{}", host, port);

    // Main SPI polling loop
    loop {
        // Service pending interrupts, in particular the ProcSignalBarrier of
        // DROP DATABASE: pgrx's wait_latch never runs CHECK_FOR_INTERRUPTS, so
        // without this the drop hangs.
        pgrx::check_for_interrupts!();

        if BackgroundWorker::sigterm_received() {
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
            return Err(format!("HTTP listener on {host}:{port}: {reason}").into());
        }

        // Apply pg_reload_conf(); stop serving when disabled
        if pg_bgworker::reload_config_if_signalled() && !crate::config::PG_GIT_ENABLED.get() {
            pgrx::log!("pg_git HTTP worker: disabled via pg_git.enabled=false, stopping");
            request_shutdown();
            break;
        }
        crate::worker::SUPERVISOR.tick(crate::worker::HTTP_SLOT);

        let mut processed = 0;
        while let Some(request) = receiver.try_recv() {
            execute_spi_request(request);
            processed += 1;
            if processed >= 100 {
                break;
            }
        }

        // Idle wait. wait_latch (not thread::sleep) so the latch wakes us
        // promptly when a procsignal (e.g. a barrier) arrives; the interrupt
        // itself is processed by check_for_interrupts!() at the top of the
        // loop. wait_latch also gives this backend a real wait_event in
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
