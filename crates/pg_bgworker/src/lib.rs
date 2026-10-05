//! Shared background-worker startup (design/bgworker-config).
//!
//! Every background-worker extension reads `<ext>.database` to choose the
//! database its workers connect to, and must not crash-loop while that
//! database does not have the extension yet.

pub mod supervision;

use std::ffi::CString;
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{GucSetting, Spi};

/// Database a worker connects to when nothing is configured.
pub const DEFAULT_DATABASE: &str = "postgres";

/// The database a worker connects to: `<ext>.database` if set, else a
/// deprecated alias setting if set, else [`DEFAULT_DATABASE`]. Empty values
/// count as unset.
pub fn resolve_database(primary: Option<&str>, alias: Option<&str>) -> String {
    [primary, alias]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .unwrap_or(DEFAULT_DATABASE)
        .to_string()
}

/// Logged once while a worker waits for `CREATE EXTENSION`.
pub fn waiting_message(extension: &str, database: &str) -> String {
    format!(
        "{extension}: extension not installed in database \"{database}\"; waiting — \
         run CREATE EXTENSION {extension} there, or set {extension}.database"
    )
}

/// The value of a string GUC, if set.
pub fn guc_str(setting: &GucSetting<Option<CString>>) -> Option<String> {
    setting
        .get()
        .and_then(|v| v.to_str().ok().map(str::to_string))
}

/// How often a waiting worker re-checks for its extension.
pub const RECHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Block until `extension` exists in the database this worker is connected
/// to (call after `connect_worker_to_spi`). Logs once while waiting.
///
/// Returns `true` once the extension exists, `false` when the worker should
/// exit instead (SIGTERM, or the postmaster died).
pub fn wait_for_extension(extension: &str, database: &str) -> bool {
    let mut announced = false;
    loop {
        let installed = BackgroundWorker::transaction(|| {
            Spi::get_one_with_args::<bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = $1)",
                &[extension.into()],
            )
        });
        if let Ok(Some(true)) = installed {
            if announced {
                pgrx::log!("{extension}: extension found in database \"{database}\"; starting");
            }
            return true;
        }
        if !announced {
            pgrx::log!("{}", waiting_message(extension, database));
            announced = true;
        }
        // pgrx's wait_latch never runs CHECK_FOR_INTERRUPTS: without this the
        // ProcSignalBarrier of DROP DATABASE is never acknowledged
        pgrx::check_for_interrupts!();
        reload_config_if_signalled();
        if !BackgroundWorker::wait_latch(Some(RECHECK_INTERVAL))
            || BackgroundWorker::sigterm_received()
        {
            return false;
        }
    }
}

/// Apply a pending configuration reload. Call from the worker's main loop
/// (outside any transaction); returns `true` when settings were reloaded.
///
/// pgrx's SIGHUP handler only records the signal: without this, a worker
/// keeps the settings it started with and ignores `pg_reload_conf()`.
/// Workers must attach `SignalWakeFlags::SIGHUP`.
pub fn reload_config_if_signalled() -> bool {
    if !BackgroundWorker::sighup_received() {
        return false;
    }
    unsafe {
        (&raw mut pgrx::pg_sys::ConfigReloadPending).write_volatile(0);
        pgrx::pg_sys::ProcessConfigFile(pgrx::pg_sys::GucContext::PGC_SIGHUP);
    }
    true
}

/// Block until `ready()` holds (e.g. `<ext>.enabled` turned on), checking
/// once a second and applying configuration reloads. Returns `false` when the
/// worker should exit instead (SIGTERM or postmaster death).
///
/// A disabled worker waits rather than exits: an exit restarts it every
/// `bgw_restart_time`, and a worker that exits 0 is never started again.
pub fn wait_until(mut ready: impl FnMut() -> bool) -> bool {
    while !ready() {
        // See wait_for_extension: DROP DATABASE must not hang on us
        pgrx::check_for_interrupts!();
        if !BackgroundWorker::wait_latch(Some(Duration::from_secs(1)))
            || BackgroundWorker::sigterm_received()
        {
            return false;
        }
        reload_config_if_signalled();
    }
    true
}

/// End the worker so the postmaster starts it again after its restart
/// interval.
///
/// A worker that returns normally exits with code 0, and the postmaster then
/// unregisters it for good. A SIGTERM from `pg_terminate_backend()` or
/// `DROP DATABASE … WITH (FORCE)` would stop the service until the server
/// restarts. During server shutdown nothing is restarted, so this is safe
/// there too.
pub fn exit_for_restart() -> ! {
    unsafe { pgrx::pg_sys::proc_exit(1) };
    unreachable!("proc_exit returned")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_setting_wins() {
        assert_eq!(resolve_database(Some("app"), Some("old")), "app");
    }

    #[test]
    fn alias_is_used_when_primary_is_unset_or_empty() {
        assert_eq!(resolve_database(None, Some("old")), "old");
        assert_eq!(resolve_database(Some("  "), Some("old")), "old");
    }

    #[test]
    fn defaults_to_postgres() {
        assert_eq!(resolve_database(None, None), "postgres");
        assert_eq!(resolve_database(Some(""), Some("")), "postgres");
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        assert_eq!(resolve_database(Some(" app "), None), "app");
    }

    #[test]
    fn waiting_message_names_the_fix() {
        let m = waiting_message("pg_kafka", "postgres");
        assert!(m.contains("\"postgres\""), "{m}");
        assert!(m.contains("CREATE EXTENSION pg_kafka"), "{m}");
        assert!(m.contains("pg_kafka.database"), "{m}");
    }
}
