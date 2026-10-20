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

/// Venv path compiled in from PG_ML_VENV_PATH (`make install` sets it to the
/// venv it creates). An empty value counts as unset: a prebuilt bottle is built
/// with PG_ML_VENV_PATH="" so neither the build machine's path nor the dev
/// `.venv` from .cargo/config.toml ends up in it.
pub const COMPILED_VENV_PATH: Option<&str> = match option_env!("PG_ML_VENV_PATH") {
    Some(path) if !path.is_empty() => Some(path),
    _ => None,
};

/// The Python a venv created at runtime is built on. It has to be the
/// interpreter whose libpython pg_ml linked (PYO3_PYTHON at build time): a
/// bottle linked against Homebrew's python@3.11 is built with
/// PG_ML_BASE_PYTHON=/opt/homebrew/opt/python@3.11/bin/python3.11, so the
/// venv's packages and the embedded interpreter agree. Unset, uv picks any 3.11.
pub const BASE_PYTHON: &str = match option_env!("PG_ML_BASE_PYTHON") {
    Some(python) if !python.is_empty() => python,
    _ => "3.11",
};

/// Where the venv lives: the pg_ml.venv_path GUC, else the compiled-in path,
/// else `<data directory>/pg_ml/venv` (always writable by the server, on any
/// machine a bottle is installed on), else /var/lib/postgresql/pg_ml.
pub fn get_venv_path() -> PathBuf {
    VENV_PATH
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .map(PathBuf::from)
        .or_else(|| COMPILED_VENV_PATH.map(PathBuf::from))
        .or_else(|| data_directory().map(|d| d.join("pg_ml").join("venv")))
        .unwrap_or_else(|| PathBuf::from("/var/lib/postgresql/pg_ml"))
}

/// The server's data directory, when running inside one.
fn data_directory() -> Option<PathBuf> {
    // SAFETY: DataDir is set once at postmaster start and never freed; a null
    // pointer (outside a server) is checked before it is read.
    unsafe {
        let dir = pgrx::pg_sys::DataDir;
        if dir.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(dir)
            .to_str()
            .ok()
            .map(PathBuf::from)
    }
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
