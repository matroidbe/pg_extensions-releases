//! WMS HTTP server inside a Postgres background worker.
//!
//! Exposes a read-only OGC WMS 1.3.0 endpoint (GetCapabilities + GetMap)
//! against the catalog. Off by default — opt in via the
//! `pg_xarray.wms_enabled` GUC.
//!
//! Layout follows the in-tree convention from `pg_kafka` / `pg_mqtt` /
//! `pg_s3` / `pg_git`:
//!   * GUCs declared here, registered from `_PG_init`.
//!   * `BackgroundWorkerBuilder::new("pg_xarray_wms")` registered
//!     unconditionally — the `wms_enabled` flag is checked inside the
//!     worker loop so users can toggle it without restarting Postgres.
//!   * Worker entry function `pg_xarray_wms_worker_main` is `pub use`-d
//!     from `lib.rs` so the symbol shows up in the .so's dynamic table.

pub mod http;
pub mod tcp;
pub mod wms;
pub mod worker;

use std::ffi::CString;
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

pub use worker::pg_xarray_wms_worker_main;

/// Off by default — explicit opt-in keeps an unconfigured cluster from
/// suddenly listening on the network.
pub static WMS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Default port chosen outside the test.sh 299xx range used by sister
/// extensions so the pgrx-dev cluster can run pg_kafka + pg_mqtt + pg_xarray
/// side-by-side without collisions.
/// Default WMS listener port and bind host
pub const DEFAULT_WMS_PORT: i32 = 7800;
pub const DEFAULT_WMS_BIND_HOST: &str = "127.0.0.1";

pub static WMS_PORT: GucSetting<i32> = GucSetting::<i32>::new(DEFAULT_WMS_PORT);

/// Bind host — falls back to `'127.0.0.1'` when unset (see `bind_host()`).
/// Users opt into a public bind by setting this to `'0.0.0.0'`.
pub static WMS_BIND_HOST: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// `Cache-Control: max-age=N` value on every GetMap response. Default
/// 60 s — a reverse proxy / browser cache then absorbs steady-state
/// read traffic. Set to 0 to disable caching (e.g., while iterating on
/// data in dev).
pub static WMS_CACHE_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

/// Database the bgworker connects to for catalog lookups. A bgworker
/// registered via `shared_preload_libraries` lives at the cluster
/// level, but SPI access needs a specific database. Set this to
/// whichever database has `CREATE EXTENSION pg_xarray` installed —
/// catalogs in other databases are not visible to this worker.
pub static WMS_DATABASE: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// Register GUCs + the background worker. Called from `_PG_init`.
pub fn init() {
    worker::SUPERVISOR.init();
    GucRegistry::define_bool_guc(
        c"pg_xarray.wms_enabled",
        c"Enable the pg_xarray WMS HTTP server bgworker.",
        c"When false (default) the bgworker sits idle. Toggle via \
          ALTER SYSTEM SET pg_xarray.wms_enabled = on; SELECT pg_reload_conf();",
        &WMS_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_xarray.wms_port",
        c"TCP port the pg_xarray WMS server listens on.",
        c"Default 7800. Pick something outside 299xx (test.sh) and the \
          sister-extension defaults (kafka 19092, mqtt 11883).",
        &WMS_PORT,
        1,
        65535,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_xarray.wms_bind_host",
        c"TCP bind host for the pg_xarray WMS server.",
        c"Default '127.0.0.1' (loopback only). Use '0.0.0.0' to expose \
          on all interfaces; put a reverse proxy with auth in front.",
        &WMS_BIND_HOST,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_xarray.wms_cache_seconds",
        c"Cache-Control max-age (seconds) on GetMap responses.",
        c"Default 60. Set to 0 to disable browser/proxy caching.",
        &WMS_CACHE_SECONDS,
        0,
        86400,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_xarray.database",
        c"Database the WMS bgworker reads the catalog from.",
        c"Defaults to 'postgres'. Set to whichever database has \
          CREATE EXTENSION pg_xarray installed. Catalogs in other \
          databases are invisible to this worker.",
        &WMS_DATABASE,
        GucContext::Sighup,
        GucFlags::default(),
    );

    // Worker is registered unconditionally; the `enabled` check happens
    // inside the worker loop so users can toggle without a restart.
    BackgroundWorkerBuilder::new("pg_xarray_wms")
        .set_function("pg_xarray_wms_worker_main")
        .set_library("pg_xarray")
        .set_argument(None)
        .enable_spi_access()
        // Without a restart time (pgrx's default is BGW_NEVER_RESTART) any
        // exit, even a terminate, would stop the WMS until a server restart
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Read the current host from the GUC. Returns owned `String` so the
/// worker doesn't hold the GUC's borrow across awaits.
pub fn bind_host() -> String {
    WMS_BIND_HOST
        .get()
        .and_then(|cs: CString| cs.to_str().ok().map(|s| s.to_string()))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_WMS_BIND_HOST.to_string())
}

/// Database to connect to for catalog lookups. Owned `String`.
pub fn database() -> String {
    pg_bgworker::resolve_database(pg_bgworker::guc_str(&WMS_DATABASE).as_deref(), None)
}
