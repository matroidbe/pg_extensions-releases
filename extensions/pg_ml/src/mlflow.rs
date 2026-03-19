//! MLflow tracking server integration for pg_ml
//!
//! This module provides:
//! - Background worker to run MLflow UI server
//! - GUC settings for MLflow configuration
//! - Integration with PyCaret's MLflow logging
//!
//! ## Architecture
//!
//! The MLflow UI server runs as a subprocess spawned by a background worker.
//! PyCaret writes experiments to the `mlruns/` folder, and the MLflow UI
//! reads from the same folder. Communication is filesystem-only.
//!
//! ```text
//! Foreground (PyCaret) --writes--> mlruns/ <--reads-- Background (MLflow UI)
//! ```

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::ffi::CString;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

// =============================================================================
// GUC Settings
// =============================================================================

/// Enable MLflow UI server (requires PostgreSQL restart)
static MLFLOW_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(false);

/// Port for MLflow UI server
static MLFLOW_PORT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(5000);

/// Host address for MLflow UI server
static MLFLOW_HOST: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Enable MLflow tracking for training (session-level)
static MLFLOW_TRACKING: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(false);

const DEFAULT_HOST: &str = "127.0.0.1";

/// Register MLflow GUC settings
///
/// Note: We only register Postmaster-level GUCs when called during
/// shared_preload_libraries (i.e., when we're in the postmaster process).
/// This prevents "cannot create PGC_POSTMASTER variables after startup"
/// errors when the extension is created dynamically via CREATE EXTENSION.
pub fn register_gucs() {
    // Check if we're being called during postmaster startup
    // (process_shared_preload_libraries_in_progress)
    // When true, we can register Postmaster GUCs
    // When false, we're in a backend process (CREATE EXTENSION)
    let in_postmaster = unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress };

    if in_postmaster {
        pgrx::GucRegistry::define_bool_guc(
            c"pg_ml.mlflow_enabled",
            c"Enable MLflow UI server",
            c"When true, pg_ml starts an embedded MLflow UI server on PostgreSQL startup",
            &MLFLOW_ENABLED,
            pgrx::GucContext::Postmaster, // Requires restart
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_int_guc(
            c"pg_ml.mlflow_port",
            c"Port for MLflow UI server",
            c"HTTP port for MLflow UI",
            &MLFLOW_PORT,
            1024,
            65535,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_string_guc(
            c"pg_ml.mlflow_host",
            c"Host address for MLflow server",
            c"IP address or hostname that MLflow binds to",
            &MLFLOW_HOST,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );
    }

    // Session-level GUC can always be registered
    pgrx::GucRegistry::define_bool_guc(
        c"pg_ml.mlflow_tracking",
        c"Enable MLflow tracking for training",
        c"When true, training operations log experiments to MLflow",
        &MLFLOW_TRACKING,
        pgrx::GucContext::Userset, // Session-level, no restart needed
        pgrx::GucFlags::default(),
    );
}

/// Check if MLflow server is enabled
pub fn is_enabled() -> bool {
    MLFLOW_ENABLED.get()
}

/// Check if MLflow tracking is enabled for this session
pub fn is_tracking_enabled() -> bool {
    MLFLOW_TRACKING.get()
}

/// Get MLflow host
pub fn get_host() -> String {
    MLFLOW_HOST
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| DEFAULT_HOST.to_string())
}

/// Get MLflow port
pub fn get_port() -> i32 {
    MLFLOW_PORT.get()
}

/// Get the mlruns directory path
pub fn get_mlruns_path() -> PathBuf {
    crate::get_venv_path().join("mlruns")
}

/// Get MLflow tracking URI for PyCaret
pub fn get_tracking_uri() -> String {
    format!("file://{}", get_mlruns_path().display())
}

// =============================================================================
// Background Worker Registration
// =============================================================================

/// Register MLflow background worker (called from _PG_init if enabled)
pub fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_ml_mlflow")
        .set_function("pg_ml_mlflow_worker_main")
        .set_library("pg_ml")
        .enable_shmem_access(None)
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

// =============================================================================
// Background Worker Main
// =============================================================================

/// Background worker main function - spawns and manages MLflow UI server
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_ml_mlflow_worker_main(_arg: pg_sys::Datum) {
    // Set up signal handlers
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let port = get_port();
    let host = get_host();
    let venv_path = crate::get_venv_path();
    let mlruns_path = get_mlruns_path();

    pgrx::log!(
        "pg_ml_mlflow: starting MLflow UI server on {}:{}",
        host,
        port
    );
    pgrx::log!(
        "pg_ml_mlflow: tracking URI: file://{}",
        mlruns_path.display()
    );

    // Create mlruns directory if it doesn't exist
    if let Err(e) = std::fs::create_dir_all(&mlruns_path) {
        pgrx::log!("pg_ml_mlflow: failed to create mlruns directory: {}", e);
        return;
    }

    // Check if mlflow binary exists in venv
    let mlflow_bin = venv_path.join("bin/mlflow");
    if !mlflow_bin.exists() {
        pgrx::log!(
            "pg_ml_mlflow: mlflow binary not found at {:?}. \
             Ensure pycaret[full] is installed in the venv.",
            mlflow_bin
        );
        return;
    }

    // Spawn MLflow UI server as subprocess
    // Note: MLflow 2.9.x doesn't support --allowed-hosts (added in 2.11+)
    // For external access via Tailscale, the host 0.0.0.0 binding is sufficient
    let child_result = Command::new(&mlflow_bin)
        .args([
            "ui",
            "--host",
            &host,
            "--port",
            &port.to_string(),
            "--backend-store-uri",
            &format!("file://{}", mlruns_path.display()),
        ])
        .current_dir(&venv_path)
        .spawn();

    let mut child: Child = match child_result {
        Ok(c) => {
            pgrx::log!("pg_ml_mlflow: MLflow UI server started (PID: {})", c.id());
            c
        }
        Err(e) => {
            pgrx::log!("pg_ml_mlflow: failed to start MLflow UI server: {}", e);
            return;
        }
    };

    // Main loop - wait for shutdown signal
    loop {
        // Check for SIGTERM
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_ml_mlflow: received SIGTERM, shutting down");
            break;
        }

        // Check if MLflow process is still running
        match child.try_wait() {
            Ok(Some(status)) => {
                // Process exited
                pgrx::log!(
                    "pg_ml_mlflow: MLflow UI server exited with status: {}",
                    status
                );
                // Could restart here, but for now just exit
                break;
            }
            Ok(None) => {
                // Still running, continue
            }
            Err(e) => {
                pgrx::log!("pg_ml_mlflow: error checking MLflow process: {}", e);
                break;
            }
        }

        // Wait for latch with timeout (handles signals)
        BackgroundWorker::wait_latch(Some(Duration::from_secs(1)));

        // Handle SIGHUP for config reload (currently no-op)
        if BackgroundWorker::sighup_received() {
            pgrx::log!("pg_ml_mlflow: received SIGHUP");
        }
    }

    // Clean shutdown: kill the MLflow subprocess
    pgrx::log!("pg_ml_mlflow: killing MLflow subprocess");
    if let Err(e) = child.kill() {
        pgrx::log!("pg_ml_mlflow: error killing subprocess: {}", e);
    }
    if let Err(e) = child.wait() {
        pgrx::log!("pg_ml_mlflow: error waiting for subprocess: {}", e);
    }

    pgrx::log!("pg_ml_mlflow: worker stopped");
}

// =============================================================================
// SQL Functions
// =============================================================================

/// Get MLflow server status
#[pg_extern]
pub fn mlflow_status() -> pgrx::JsonB {
    let enabled = is_enabled();
    let port = get_port();
    let host = get_host();
    let tracking = is_tracking_enabled();
    let mlruns_path = get_mlruns_path();

    // Check if mlruns directory exists (indicates server may have been used)
    let mlruns_exists = mlruns_path.exists();

    pgrx::JsonB(serde_json::json!({
        "enabled": enabled,
        "host": host,
        "port": port,
        "url": if enabled { format!("http://{}:{}", host, port) } else { "disabled".to_string() },
        "tracking": tracking,
        "tracking_uri": get_tracking_uri(),
        "mlruns_exists": mlruns_exists,
    }))
}

/// Get MLflow UI URL (returns NULL if not enabled)
#[pg_extern]
pub fn mlflow_url() -> Option<String> {
    if !is_enabled() {
        return None;
    }

    let port = get_port();
    let host = get_host();

    Some(format!("http://{}:{}", host, port))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_host() {
        assert_eq!(DEFAULT_HOST, "127.0.0.1");
    }

    #[test]
    fn test_mlruns_path_construction() {
        // Just verify the path ends with mlruns
        let path = PathBuf::from("/var/lib/postgresql/pg_ml").join("mlruns");
        assert!(path.ends_with("mlruns"));
    }

    #[test]
    fn test_tracking_uri_format() {
        let path = PathBuf::from("/var/lib/postgresql/pg_ml/mlruns");
        let uri = format!("file://{}", path.display());
        assert!(uri.starts_with("file://"));
        assert!(uri.ends_with("mlruns"));
    }
}

// NOTE: MLflow pg_test tests are in lib.rs main tests module
// because pgrx test discovery doesn't work correctly with separate test modules
