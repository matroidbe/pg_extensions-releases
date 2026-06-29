//! Background worker entry point.
//!
//! Idle-loop pattern:
//!   * When `pg_xarray.wms_enabled = false` (default), sit in
//!     `BackgroundWorker::wait_latch` for short intervals — this is
//!     the [feedback-bgworker-wait-latch] memory's load-bearing
//!     constraint: tokio-driven workers that use `std::thread::sleep`
//!     hang DROP DATABASE for the full sleep interval.
//!   * When enabled, hand control over to `tcp::run` which builds its
//!     own tokio runtime; `tcp::run` returns when SIGTERM arrives.
//!
//! The worker entry symbol is re-exported via `pub use` from `lib.rs`
//! so it ends up in the .so's dynamic symbol table — without that
//! `BackgroundWorkerBuilder::set_function(...)` can't find it.

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

use super::{
    bind_host, database, tcp, wait, DISABLED_POLL_INTERVAL, WMS_CACHE_SECONDS, WMS_ENABLED,
    WMS_PORT,
};

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_xarray_wms_worker_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let db = database();
    BackgroundWorker::connect_worker_to_spi(Some(db.as_str()), None);

    pgrx::log!("pg_xarray WMS bgworker started (db='{}')", db);

    loop {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_xarray WMS bgworker: SIGTERM, exiting");
            return;
        }

        if !WMS_ENABLED.get() {
            // Disabled — sleep on the latch for a short interval so a
            // SIGHUP that flips the GUC on can wake us promptly. Using
            // wait_latch (not thread::sleep) keeps the worker
            // responsive to ProcSignalBarrier — DROP DATABASE won't hang.
            if wait(DISABLED_POLL_INTERVAL) {
                return;
            }
            continue;
        }

        // Enabled — run the TCP accept loop. This blocks until SIGTERM
        // breaks the loop inside tcp::run, then returns here.
        let host = bind_host();
        let port = WMS_PORT.get() as u16;
        let cache = WMS_CACHE_SECONDS.get().max(0) as u32;
        pgrx::log!(
            "pg_xarray WMS: starting listener on {}:{} (cache_seconds={})",
            host,
            port,
            cache
        );
        tcp::run(&host, port, cache);

        // tcp::run returned — most likely SIGTERM. Loop top will exit.
        if BackgroundWorker::sigterm_received() {
            return;
        }
        // If we got here without SIGTERM, the listener failed to bind
        // or the runtime crashed; back off briefly before retrying so
        // we don't busy-loop on bind errors.
        if wait(DISABLED_POLL_INTERVAL) {
            return;
        }
    }
}
