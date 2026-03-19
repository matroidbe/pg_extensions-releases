//! Configuration and GUC settings for pg_ml

use std::ffi::CString;
use std::path::PathBuf;

/// GUC setting for the virtual environment path
pub static VENV_PATH: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// GUC setting for the uv binary path override
pub static UV_PATH: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// GUC setting for auto-setup on first use
pub static AUTO_SETUP: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

/// Default venv path - set at compile time from PG_ML_VENV_PATH env var
/// Falls back to /var/lib/postgresql/pg_ml if not set
pub const DEFAULT_VENV_PATH: &str = match option_env!("PG_ML_VENV_PATH") {
    Some(path) => path,
    None => "/var/lib/postgresql/pg_ml",
};

/// Get the configured venv path
pub fn get_venv_path() -> PathBuf {
    VENV_PATH
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_VENV_PATH))
}

/// Get the Python binary path within the venv
pub fn get_python_path() -> PathBuf {
    get_venv_path().join("bin/python")
}

/// Register GUC settings
pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.venv_path",
        c"Path to store Python venv",
        c"Directory where pg_ml stores Python virtual environment",
        &VENV_PATH,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.uv_path",
        c"Path to uv binary",
        c"Override the bundled uv binary location",
        &UV_PATH,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_ml.auto_setup",
        c"Auto-setup venv on first use",
        c"If true, automatically create venv when first ML function is called",
        &AUTO_SETUP,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );
}
