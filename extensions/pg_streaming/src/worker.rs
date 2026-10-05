//! Background worker entry points for pg_streaming
//!
//! Three worker types:
//! - Coordinator (1): manages pipeline lifecycle and executor assignments
//! - Executor (N): processes assigned pipelines in a poll-process-commit loop
//! - Timer (1): fires window close events, TTL cleanup, state expiry
//!
//! IMPORTANT: All SPI calls in background workers MUST be wrapped in
//! `BackgroundWorker::transaction()` to properly set up the transaction context
//! (StartTransactionCommand, PushActiveSnapshot, CommitTransactionCommand).
//! Calling Spi::get_one() etc. without this wrapper causes a segfault.

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

use crate::config::{PG_STREAMING_DATABASE, PG_STREAMING_ENABLED, PG_STREAMING_POLL_INTERVAL_MS};
use crate::engine::coordinator::run_coordinator_tick;
use crate::engine::executor::run_executor_tick;

/// Restarts, backoff and the failed state of the workers
/// (design/bgworker-supervision)
pub static SUPERVISOR: pg_bgworker::supervision::Supervisor = unsafe {
    pg_bgworker::supervision::Supervisor::new(
        c"pg_streaming_supervision",
        c"pg_streaming.max_worker_failures",
    )
};

pg_bgworker::supervision_sql!(crate::worker::SUPERVISOR);

/// Supervision slots: coordinator, timer, then one per executor
const COORDINATOR_SLOT: usize = 0;
const TIMER_SLOT: usize = 1;
fn executor_slot(worker_id: i32) -> usize {
    2 + worker_id as usize
}

/// Shared startup: supervision, database, then wait while disabled and until
/// the extension exists. Exits (for restart) on SIGTERM.
fn start_worker(slot: usize, name: &str) {
    if !SUPERVISOR.start(slot, name) {
        SUPERVISOR.exit_clean(slot);
    }
    let database = pg_bgworker::resolve_database(
        pg_bgworker::guc_str(&PG_STREAMING_DATABASE).as_deref(),
        None,
    );
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);
    log!("{}: started, pid={}", name, std::process::id());

    // While disabled, wait (pg_streaming.enabled follows reloads); then do
    // nothing until the extension's schema exists in this database.
    if !PG_STREAMING_ENABLED.get() {
        log!("{}: disabled via pg_streaming.enabled=false; waiting", name);
    }
    if !pg_bgworker::wait_until(|| PG_STREAMING_ENABLED.get())
        || !pg_bgworker::wait_for_extension("pg_streaming", &database)
    {
        SUPERVISOR.exit_clean(slot);
    }
}

/// One round of a worker loop: process interrupts (pgrx's wait_latch never
/// runs CHECK_FOR_INTERRUPTS, so DROP DATABASE would hang), apply reloads,
/// and report health. Returns whether to do work this round (enabled).
fn loop_round(slot: usize) -> bool {
    pgrx::check_for_interrupts!();
    pg_bgworker::reload_config_if_signalled();
    SUPERVISOR.tick(slot);
    PG_STREAMING_ENABLED.get()
}

/// Coordinator background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_coordinator_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    start_worker(COORDINATOR_SLOT, "pg_streaming coordinator");

    // Coordinator loop: assign pipelines to executors, monitor health
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        if !loop_round(COORDINATOR_SLOT) {
            continue;
        }

        BackgroundWorker::transaction(|| {
            run_coordinator_tick();
        });
    }

    log!("pg_streaming coordinator: shutting down");
    SUPERVISOR.exit_clean(COORDINATOR_SLOT);
}

/// Executor background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_executor_main(arg: pg_sys::Datum) {
    let worker_id = arg.value() as i32;

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let slot = executor_slot(worker_id);
    start_worker(slot, &format!("pg_streaming executor {}", worker_id));

    // Cache of compiled pipelines — survives across ticks
    let mut compiled = HashMap::new();

    // Executor loop: discover pipelines, process batches, commit offsets.
    // The poll interval is read every round, so a reload applies.
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(
        PG_STREAMING_POLL_INTERVAL_MS.get() as u64,
    ))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        if !loop_round(slot) {
            continue;
        }

        BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
            run_executor_tick(worker_id, &mut compiled);
        }));
    }

    log!("pg_streaming executor {}: shutting down", worker_id);
    SUPERVISOR.exit_clean(slot);
}

/// Timer background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_timer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    start_worker(TIMER_SLOT, "pg_streaming timer");

    // Timer loop: fire window close events, TTL cleanup, state expiry
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
        loop_round(TIMER_SLOT);
    }

    log!("pg_streaming timer: shutting down");
    SUPERVISOR.exit_clean(TIMER_SLOT);
}
