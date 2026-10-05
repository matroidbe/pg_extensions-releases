//! Background worker initialization and main loop for pg_kafka

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

use crate::config::{
    DEFAULT_HOST, PG_KAFKA_ADVERTISED_HOST, PG_KAFKA_ADVERTISED_PORT, PG_KAFKA_DATABASE,
    PG_KAFKA_ENABLED, PG_KAFKA_HOST, PG_KAFKA_METRICS_ENABLED, PG_KAFKA_METRICS_PORT,
    PG_KAFKA_PORT, PG_KAFKA_WORKER_COUNT,
};
use crate::server::{run_server, shared_advertised, AdvertisedConfig, MetricsConfig};
use pg_bgworker::supervision::Supervisor;

/// Restarts, backoff and the failed state of the workers
/// (design/bgworker-supervision)
pub static SUPERVISOR: Supervisor =
    unsafe { Supervisor::new(c"pg_kafka_supervision", c"pg_kafka.max_worker_failures") };

pg_bgworker::supervision_sql!(crate::worker::SUPERVISOR);

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
        c"The hostname/IP clients are told to connect to. If not set, the address the client reached.",
        &PG_KAFKA_ADVERTISED_HOST,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_kafka.advertised_port",
        c"Advertised port for Kafka clients",
        c"The port clients are told to connect to, when it differs from pg_kafka.port (NAT, proxy, container port mapping). 0 means pg_kafka.port.",
        &PG_KAFKA_ADVERTISED_PORT,
        0,
        65535,
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

    SUPERVISOR.init();

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

/// The advertised listener settings (main thread only: reads GUCs)
pub fn advertised_config() -> AdvertisedConfig {
    AdvertisedConfig {
        host: pg_bgworker::guc_str(&PG_KAFKA_ADVERTISED_HOST),
        port: PG_KAFKA_ADVERTISED_PORT.get(),
    }
}

/// Wait until pg_kafka.enabled is on (it follows reloads). Returns `false`
/// when the worker should exit instead (SIGTERM or postmaster death).
fn wait_until_enabled(worker_id: i32) -> bool {
    if !PG_KAFKA_ENABLED.get() {
        log!(
            "pg_kafka worker {}: disabled via pg_kafka.enabled=false; waiting",
            worker_id
        );
    }
    pg_bgworker::wait_until(|| PG_KAFKA_ENABLED.get())
}

/// Background worker main function - runs the TCP server
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_kafka_worker_main(arg: pg_sys::Datum) {
    // Extract worker ID from argument
    let worker_id = arg.value() as i32;

    // Set up signal handlers
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Back off after failures, or wait for reset_workers() once failed too often
    let slot = worker_id as usize;
    if !SUPERVISOR.start(slot, &format!("pg_kafka worker {}", worker_id)) {
        SUPERVISOR.exit_clean(slot);
    }

    // Connect to the database for SPI access
    let database =
        pg_bgworker::resolve_database(pg_bgworker::guc_str(&PG_KAFKA_DATABASE).as_deref(), None);
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);

    log!(
        "pg_kafka worker {}: started, pid={}",
        worker_id,
        std::process::id()
    );

    // Serve nothing while disabled, or until the extension's schema exists in
    // this database
    if !wait_until_enabled(worker_id) || !pg_bgworker::wait_for_extension("pg_kafka", &database) {
        SUPERVISOR.exit_clean(slot);
    }

    let port = PG_KAFKA_PORT.get() as u16;
    let host_setting = PG_KAFKA_HOST.get();
    let host = host_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_HOST);

    // Advertised address, refreshed on every configuration reload
    let advertised = shared_advertised(advertised_config());

    log!(
        "pg_kafka worker {}: starting TCP server on {}:{}",
        worker_id,
        host,
        port
    );

    // Build metrics configuration
    let metrics_config = Some(MetricsConfig {
        enabled: PG_KAFKA_METRICS_ENABLED.get(),
        port: PG_KAFKA_METRICS_PORT.get() as u16,
    });

    // After a reload: publish the new advertised address, and keep serving
    // only while still enabled
    let reload_advertised = advertised.clone();
    let on_reload = move || {
        let config = advertised_config();
        log!(
            "pg_kafka worker {}: configuration reloaded; advertised host {}, port {}",
            worker_id,
            config
                .host
                .as_deref()
                .unwrap_or("<address the client reached>"),
            if config.port > 0 {
                config.port
            } else {
                i32::from(port)
            }
        );
        *reload_advertised.write().unwrap_or_else(|e| e.into_inner()) = config;
        PG_KAFKA_ENABLED.get()
    };

    // Run server with SPI bridge polling loop
    // The server creates its own tokio runtime and polls for SPI requests on this thread
    match run_server(
        host,
        port,
        advertised,
        &on_reload,
        worker_id,
        metrics_config,
    ) {
        // SIGTERM, or disabled by a reload
        Ok(()) => {
            log!("pg_kafka worker {}: shutting down", worker_id);
            SUPERVISOR.exit_clean(slot)
        }
        Err(e) => SUPERVISOR.exit_failed(slot, &format!("pg_kafka worker {worker_id}: {e}")),
    }
}
