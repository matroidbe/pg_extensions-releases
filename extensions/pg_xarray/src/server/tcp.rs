//! Synchronous (std::net) TCP accept loop for the WMS endpoint.
//!
//! Why std and not tokio: the SPI / raster path internally uses
//! `tokio::runtime::Runtime::block_on(...)` (for OpenDAL async reads
//! in `srf::fetch_mesh`). Running that from inside a tokio runtime
//! triggers tokio's nested-runtime panic. Using `std::net` here keeps
//! the WMS server free of any tokio context, so the existing reader
//! path Just Works.
//!
//! Single-threaded by design — each connection is handled inline on
//! the bgworker's main thread. v1 trades concurrency for simplicity
//! and a small code surface. For 10–100 concurrent users with
//! per-tile compute of ~50 ms this is enough; for higher throughput
//! v2 can introduce a worker-thread pool via the SPI-bridge pattern.
//!
//! Shutdown: the listener is set to non-blocking, so
//! `WouldBlock` returns from `accept()` are the cue to call
//! `BackgroundWorker::wait_latch` (paired with check_for_interrupts!(),
//! which actually services ProcSignalBarrier —
//! see memory note `feedback-bgworker-wait-latch`) and check
//! `sigterm_received`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use super::http::{parse_request, ParseError};
use super::wms;

const READ_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const IDLE_LATCH: Duration = Duration::from_millis(500);

/// Bind the listener and run the accept loop. Returns when SIGTERM
/// arrives. Called on the bgworker's main thread.
pub fn run(host: &str, port: u16, cache_seconds: u32) {
    let listener = match TcpListener::bind((host, port)) {
        Ok(l) => l,
        Err(e) => {
            pgrx::warning!("pg_xarray WMS: failed to bind {}:{} — {}", host, port, e);
            return;
        }
    };
    if let Err(e) = listener.set_nonblocking(true) {
        pgrx::warning!("pg_xarray WMS: set_nonblocking: {}", e);
        return;
    }
    pgrx::log!("pg_xarray WMS: listening on {}:{}", host, port);

    loop {
        // Service ProcSignalBarrier etc. — wait_latch never runs
        // CHECK_FOR_INTERRUPTS, so without this DROP DATABASE hangs
        // waiting for this worker to absorb the barrier.
        pgrx::check_for_interrupts!();

        if pgrx::bgworkers::BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_xarray WMS: SIGTERM received, shutting down");
            return;
        }
        match listener.accept() {
            Ok((stream, _addr)) => {
                serve_connection(stream, cache_seconds);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection — sleep on the bgworker latch.
                // wait_latch returns false on SIGTERM or postmaster death;
                // exiting on false is what keeps a dead postmaster's
                // worker from spinning on instantly-returning WaitLatch.
                if !pgrx::bgworkers::BackgroundWorker::wait_latch(Some(IDLE_LATCH)) {
                    return;
                }
            }
            Err(e) => {
                pgrx::warning!("pg_xarray WMS: accept error: {}", e);
                if !pgrx::bgworkers::BackgroundWorker::wait_latch(Some(IDLE_LATCH)) {
                    return;
                }
            }
        }
    }
}

/// Read one HTTP request, dispatch to the WMS handler, write the
/// response, close. Sync I/O — runs inline on the bgworker's main
/// thread so SPI calls inside the handler are valid.
fn serve_connection(mut stream: TcpStream, cache_seconds: u32) {
    if let Err(e) = stream.set_read_timeout(Some(READ_TIMEOUT)) {
        pgrx::warning!("pg_xarray WMS: set_read_timeout: {}", e);
        return;
    }
    if let Err(e) = stream.set_write_timeout(Some(READ_TIMEOUT)) {
        pgrx::warning!("pg_xarray WMS: set_write_timeout: {}", e);
        return;
    }
    // The listener is non-blocking, but the accepted stream inherits
    // that on some platforms — make sure reads block per the timeout.
    let _ = stream.set_nonblocking(false);

    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 2048];

    let read_result: Result<(), &'static str> = loop {
        match stream.read(&mut chunk) {
            Ok(0) => break Err("client closed before request was complete"),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > MAX_REQUEST_BYTES {
                    break Err("request exceeded MAX_REQUEST_BYTES");
                }
                match parse_request(&buf) {
                    Ok(_) => break Ok(()),
                    Err(ParseError::Incomplete) => continue,
                    Err(ParseError::InvalidRequest) => break Err("malformed HTTP request"),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                break Err("request read timed out");
            }
            Err(_) => break Err("request read failed"),
        }
    };

    let response_bytes = match read_result {
        Ok(()) => match parse_request(&buf) {
            Ok((req, _)) => {
                // SPI inside the WMS handler needs an active
                // transaction. `BackgroundWorker::transaction` opens
                // one + commits when the closure returns. We're on
                // the bgworker's main stack (no tokio context) so
                // this is the spec-correct call site.
                pgrx::bgworkers::BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
                    wms::handle_request(&req, cache_seconds).to_bytes()
                }))
            }
            Err(_) => {
                wms::ogc_exception(400, "InvalidRequest", "malformed HTTP request").to_bytes()
            }
        },
        Err(msg) => wms::ogc_exception(400, "InvalidRequest", msg).to_bytes(),
    };

    let _ = stream.write_all(&response_bytes);
    let _ = stream.shutdown(std::net::Shutdown::Both);
}
