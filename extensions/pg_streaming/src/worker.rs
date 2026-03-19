//! Background worker entry points for pg_streaming
//!
//! Three worker types:
//! - Coordinator (1): manages pipeline lifecycle and executor assignments
//! - Executor (N): processes assigned pipelines in a poll-process-commit loop
//! - Timer (1): fires window close events, TTL cleanup, state expiry

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

use crate::config::{
    DEFAULT_DATABASE, PG_STREAMING_DATABASE, PG_STREAMING_ENABLED, PG_STREAMING_POLL_INTERVAL_MS,
};
use crate::engine::coordinator::run_coordinator_tick;
use crate::engine::executor::run_executor_tick;

/// Coordinator background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_coordinator_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_setting = PG_STREAMING_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    log!(
        "pg_streaming coordinator: started, pid={}",
        std::process::id()
    );

    if !PG_STREAMING_ENABLED.get() {
        log!("pg_streaming coordinator: disabled via pg_streaming.enabled=false");
        return;
    }

    // Coordinator loop: assign pipelines to executors, monitor health
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }

        run_coordinator_tick();
    }

    log!("pg_streaming coordinator: shutting down");
}

/// Executor background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_executor_main(arg: pg_sys::Datum) {
    let worker_id = arg.value() as i32;

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_setting = PG_STREAMING_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    log!(
        "pg_streaming executor {}: started, pid={}",
        worker_id,
        std::process::id()
    );

    if !PG_STREAMING_ENABLED.get() {
        log!(
            "pg_streaming executor {}: disabled via pg_streaming.enabled=false",
            worker_id
        );
        return;
    }

    let poll_interval_ms = PG_STREAMING_POLL_INTERVAL_MS.get() as u64;

    // Cache of compiled pipelines — survives across ticks
    let mut compiled = HashMap::new();

    // Executor loop: discover pipelines, process batches, commit offsets
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(poll_interval_ms))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }

        run_executor_tick(worker_id, &mut compiled);
    }

    log!("pg_streaming executor {}: shutting down", worker_id);
}

/// Timer background worker main function
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_streaming_timer_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_setting = PG_STREAMING_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    log!("pg_streaming timer: started, pid={}", std::process::id());

    if !PG_STREAMING_ENABLED.get() {
        log!("pg_streaming timer: disabled via pg_streaming.enabled=false");
        return;
    }

    // Timer loop: fire window close events, TTL cleanup, state expiry
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(60))) {
        if BackgroundWorker::sigterm_received() {
            break;
        }
    }

    log!("pg_streaming timer: shutting down");
}
