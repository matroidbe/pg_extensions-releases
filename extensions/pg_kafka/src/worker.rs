//! Background worker initialization and main loop for pg_kafka

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::config::{
    DEFAULT_DATABASE, DEFAULT_HOST, PG_KAFKA_ADVERTISED_HOST, PG_KAFKA_DATABASE, PG_KAFKA_ENABLED,
    PG_KAFKA_HOST, PG_KAFKA_METRICS_ENABLED, PG_KAFKA_METRICS_PORT, PG_KAFKA_PORT,
    PG_KAFKA_WORKER_COUNT,
};
use crate::server::{run_server, MetricsConfig};

/// Extension initialization - register background worker and GUCs
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_int_guc(
        c"pg_kafka.port",
        c"Port for the Kafka protocol server",
        c"TCP port that pg_kafka listens on for Kafka protocol connections",
        &PG_KAFKA_PORT,
        1,
        65535,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_kafka.host",
        c"Host address for the Kafka protocol server",
        c"IP address or hostname that pg_kafka binds to",
        &PG_KAFKA_HOST,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_kafka.advertised_host",
        c"Advertised host address for Kafka clients",
        c"The hostname/IP that clients will use to connect. Defaults to pg_kafka.host if not set.",
        &PG_KAFKA_ADVERTISED_HOST,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_kafka.enabled",
        c"Enable the Kafka protocol server",
        c"When true, pg_kafka starts automatically with PostgreSQL",
        &PG_KAFKA_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_kafka.worker_count",
        c"Number of parallel Kafka protocol handlers",
        c"Each handler binds to the same port with SO_REUSEPORT. Requires restart to take effect.",
        &PG_KAFKA_WORKER_COUNT,
        1,
        32,
        pgrx::GucContext::Sighup, // Use Sighup so extension can be loaded dynamically for tests
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_kafka.database",
        c"Database for pg_kafka to connect to",
        c"The database where pg_kafka extension is installed and topics are stored. Defaults to 'postgres'.",
        &PG_KAFKA_DATABASE,
        pgrx::GucContext::Sighup, // Use Sighup to allow dynamic extension loading for tests
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_kafka.metrics_port",
        c"Port for Prometheus metrics endpoint",
        c"Set to 0 to disable. Only worker 0 starts the endpoint.",
        &PG_KAFKA_METRICS_PORT,
        1024,
        65535,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_kafka.metrics_enabled",
        c"Enable Prometheus metrics endpoint",
        c"When true, exposes metrics at pg_kafka.metrics_port",
        &PG_KAFKA_METRICS_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Register N background workers
    let worker_count = PG_KAFKA_WORKER_COUNT.get();
    for i in 0..worker_count {
        BackgroundWorkerBuilder::new(&format!("pg_kafka worker {}", i))
            .set_function("pg_kafka_worker_main")
            .set_library("pg_kafka")
            .set_argument(Some(pg_sys::Datum::from(i as i64)))
            .enable_shmem_access(None)
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }
}

/// Background worker main function - runs the TCP server
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_kafka_worker_main(arg: pg_sys::Datum) {
    // Extract worker ID from argument
    let worker_id = arg.value() as i32;

    // Set up signal handlers
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Connect to the database for SPI access
    let db_setting = PG_KAFKA_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    log!(
        "pg_kafka worker {}: started, pid={}",
        worker_id,
        std::process::id()
    );

    // Check if enabled
    if !PG_KAFKA_ENABLED.get() {
        log!(
            "pg_kafka worker {}: disabled via pg_kafka.enabled=false",
            worker_id
        );
        return;
    }

    let port = PG_KAFKA_PORT.get() as u16;
    let host_setting = PG_KAFKA_HOST.get();
    let host = host_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_HOST);

    // Get advertised host - defaults to bind host if not set
    let advertised_host_setting = PG_KAFKA_ADVERTISED_HOST.get();
    let advertised_host = advertised_host_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(host);

    log!(
        "pg_kafka worker {}: starting TCP server on {}:{}",
        worker_id,
        host,
        port
    );
    if advertised_host != host {
        log!(
            "pg_kafka worker {}: advertising as {}:{} to clients",
            worker_id,
            advertised_host,
            port
        );
    }

    // Build metrics configuration
    let metrics_config = Some(MetricsConfig {
        enabled: PG_KAFKA_METRICS_ENABLED.get(),
        port: PG_KAFKA_METRICS_PORT.get() as u16,
    });

    // Run server with SPI bridge polling loop
    // The server creates its own tokio runtime and polls for SPI requests on this thread
    if let Err(e) = run_server(host, port, advertised_host, worker_id, metrics_config) {
        log!("pg_kafka worker {}: server error: {}", worker_id, e);
    }

    log!("pg_kafka worker {}: shutting down", worker_id);
}
