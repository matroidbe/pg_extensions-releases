//! Background worker entry point.
//!
//! Idle-loop pattern:
//!   * When `pg_xarray.wms_enabled = false` (default), wait on the latch
//!     (`pg_bgworker::wait_until`, which also services interrupts so
//!     DROP DATABASE does not hang, and applies `pg_reload_conf()`).
//!   * When enabled, hand control to `tcp::run` (std::net accept loop on
//!     this thread). It returns on SIGTERM (exit), on a reload that
//!     disables the server or changes its settings (loop again), or with
//!     an error such as a bind failure (a supervised failure).
//!
//! The worker entry symbol is re-exported via `pub use` from `lib.rs`
//! so it ends up in the .so's dynamic symbol table — without that
//! `BackgroundWorkerBuilder::set_function(...)` can't find it.

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};

use super::{bind_host, database, tcp, WMS_CACHE_SECONDS, WMS_ENABLED, WMS_PORT};

/// Restarts, backoff and the failed state of the WMS worker
/// (design/bgworker-supervision)
pub static SUPERVISOR: pg_bgworker::supervision::Supervisor = unsafe {
    pg_bgworker::supervision::Supervisor::new(
        c"pg_xarray_supervision",
        c"pg_xarray.max_worker_failures",
    )
};

pg_bgworker::supervision_sql!(crate::server::worker::SUPERVISOR);

pub const WMS_SLOT: usize = 0;

#[pgrx::pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_xarray_wms_worker_main(_arg: pgrx::pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Back off after failures, or wait for reset_workers() once failed too often
    if !SUPERVISOR.start(WMS_SLOT, "pg_xarray WMS") {
        SUPERVISOR.exit_clean(WMS_SLOT);
    }

    let db = database();
    BackgroundWorker::connect_worker_to_spi(Some(db.as_str()), None);

    pgrx::log!("pg_xarray WMS bgworker started (db='{}')", db);

    loop {
        // Disabled: wait until a reload turns pg_xarray.wms_enabled on.
        // Then serve nothing until the extension's catalog exists in this
        // database.
        if !pg_bgworker::wait_until(|| WMS_ENABLED.get())
            || !pg_bgworker::wait_for_extension("pg_xarray", &db)
        {
            SUPERVISOR.exit_clean(WMS_SLOT);
        }

        // Run the accept loop until SIGTERM, a settings change, or failure
        let host = bind_host();
        let port = WMS_PORT.get() as u16;
        let cache = WMS_CACHE_SECONDS.get().max(0) as u32;
        pgrx::log!(
            "pg_xarray WMS: starting listener on {}:{} (cache_seconds={})",
            host,
            port,
            cache
        );
        match tcp::run(&host, port, cache) {
            Ok(tcp::RunEnd::Stopped) => SUPERVISOR.exit_clean(WMS_SLOT),
            Ok(tcp::RunEnd::Reconfigure) => continue,
            Err(e) => SUPERVISOR.exit_failed(WMS_SLOT, &format!("pg_xarray WMS: {e}")),
        }
    }
}
