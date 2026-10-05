//! Python virtual environment management for pg_ml

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use crate::config::{get_python_path, get_venv_path, AUTO_SETUP, UV_PATH};
use crate::error::PgMlError;

/// Hashed export of uv.lock, embedded at compile time so a venv created at
/// runtime gets exactly the packages this build was tested with
/// (regenerate with ./check-python-lock.sh --fix)
const LOCKED_REQUIREMENTS: &str = include_str!("../requirements.lock.txt");

/// uv arguments that make the venv match the lockfile exactly, refusing any
/// package whose hash differs from the one recorded at lock time
fn install_requirements_args(
    python: &std::path::Path,
    requirements: &std::path::Path,
) -> Vec<std::ffi::OsString> {
    let mut args: Vec<std::ffi::OsString> = ["pip", "sync", "--require-hashes", "--python"]
        .map(Into::into)
        .into();
    args.push(python.into());
    args.push(requirements.into());
    args
}

/// Search for uv binary in bundled locations and system PATH
/// This is the pure path search, separate from GUC access
fn find_uv_in_paths() -> Result<PathBuf, PgMlError> {
    // 1. Check bundled with extension (next to .so file)
    // pgrx doesn't expose $libdir directly, so we check common locations
    let bundled_paths = [
        "/usr/share/postgresql/16/extension/uv",
        "/usr/share/postgresql/15/extension/uv",
        "/usr/share/postgresql/14/extension/uv",
        "/usr/lib/postgresql/16/lib/uv",
        "/usr/lib/postgresql/15/lib/uv",
        "/usr/lib/postgresql/14/lib/uv",
    ];
    for path in bundled_paths {
        let p = PathBuf::from(path);
        if p.exists() {
            return Ok(p);
        }
    }

    // 2. Check system PATH
    if let Ok(path) = which::which("uv") {
        return Ok(path);
    }

    Err(PgMlError::UvNotFound)
}

/// Find the uv binary in order of preference:
/// 1. GUC pg_ml.uv_path (if set)
/// 2. Bundled with extension ($libdir/uv)
/// 3. System PATH
pub fn find_uv() -> Result<PathBuf, PgMlError> {
    // 1. Check GUC override (only accessible within PostgreSQL context)
    if let Some(path) = UV_PATH
        .get()
        .and_then(|s: std::ffi::CString| s.into_string().ok())
    {
        let p = PathBuf::from(&path);
        if p.exists() {
            return Ok(p);
        }
    }

    // 2. Search in bundled locations and system PATH
    find_uv_in_paths()
}

/// Ensure the Python virtual environment exists and has required packages
pub fn ensure_venv() -> Result<(), PgMlError> {
    let venv_path = get_venv_path();
    let python_path = get_python_path();

    // If venv already exists with Python, we're done
    if python_path.exists() {
        return Ok(());
    }

    let uv = find_uv()?;

    // Create parent directory if needed
    if let Some(parent) = venv_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create venv with Python 3.11 (must match the version pg_ml was compiled against)
    // The Makefile ensures PYO3_PYTHON points to this venv before building
    pgrx::info!(
        "pg_ml: Creating Python 3.11 virtual environment at {:?}...",
        venv_path
    );
    let output = Command::new(&uv)
        .args(["venv", "--python", "3.11"])
        .arg(&venv_path)
        .output()?;

    if !output.status.success() {
        return Err(PgMlError::VenvCreationFailed(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    // Write the locked requirements to a temp file for uv pip sync
    let requirements_path = venv_path.join("requirements.lock.txt");
    {
        let mut file = std::fs::File::create(&requirements_path)?;
        file.write_all(LOCKED_REQUIREMENTS.as_bytes())?;
    }

    // Install the locked dependencies (the Makefile installs the same file)
    pgrx::info!("pg_ml: Installing PyCaret and dependencies (this may take a few minutes)...");
    let output = Command::new(&uv)
        .args(install_requirements_args(&python_path, &requirements_path))
        .output()?;

    if !output.status.success() {
        // Clean up the requirements file on failure
        std::fs::remove_file(&requirements_path).ok();
        return Err(PgMlError::PackageInstallFailed(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    // Also install uv into the venv (for runtime package installation if needed)
    let output = Command::new(&uv)
        .args([
            "pip",
            "install",
            "--python",
            python_path.to_str().unwrap_or("python"),
            "uv",
        ])
        .output()?;

    if !output.status.success() {
        pgrx::warning!(
            "pg_ml: Failed to install uv into venv: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        // Not fatal, continue
    }

    // Clean up the requirements file
    std::fs::remove_file(&requirements_path).ok();

    pgrx::info!("pg_ml: Virtual environment setup complete!");
    Ok(())
}

/// Find the pythonX.Y directory inside a prefix/lib/ directory.
fn find_python_version_dir(base: &std::path::Path) -> Option<String> {
    let lib_dir = base.join("lib");
    if let Ok(entries) = std::fs::read_dir(&lib_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("python3.") && entry.path().is_dir() {
                return Some(name_str.into_owned());
            }
        }
    }
    None
}

/// Find the base Python prefix from the venv's pyvenv.cfg.
/// The `home` key points to `<base>/bin`, so the parent is the prefix.
fn find_base_python_prefix(venv: &std::path::Path) -> Option<PathBuf> {
    let pyvenv_cfg = venv.join("pyvenv.cfg");
    let cfg_content = std::fs::read_to_string(pyvenv_cfg).ok()?;
    for line in cfg_content.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("home") {
            if let Some(val) = rest.trim().strip_prefix('=') {
                let base_bin = val.trim();
                return std::path::Path::new(base_bin).parent().map(PathBuf::from);
            }
        }
    }
    None
}

static PYTHON_INITIALIZED: OnceLock<Result<(), String>> = OnceLock::new();

/// Initialize Python with the virtual environment activated
pub fn ensure_python() -> Result<(), PgMlError> {
    let result = PYTHON_INITIALIZED.get_or_init(|| {
        // Check if auto_setup is enabled and venv doesn't exist
        let python_path = get_python_path();
        if !python_path.exists() {
            if AUTO_SETUP.get() {
                if let Err(e) = ensure_venv() {
                    return Err(format!("Failed to setup venv: {}", e));
                }
            } else {
                return Err(
                    "Virtual environment not found. Run SELECT pgml.setup_venv() first.".into(),
                );
            }
        }

        // Set environment variables for the venv.
        //
        // The extension must be built with PYO3_PYTHON pointing to the venv's
        // Python binary (via test.sh or Makefile). This ensures PyO3 links against
        // the correct libpython. The rpath embedded in the .so ensures the right
        // libpython is found at runtime.
        //
        // PYTHONHOME is set to the base Python prefix (from pyvenv.cfg) so the
        // embedded interpreter can find its stdlib (encodings, math, _struct, etc).
        // PYTHONPATH includes the venv's site-packages for installed packages.
        let venv = get_venv_path();
        std::env::set_var("VIRTUAL_ENV", &venv);

        // Set PYTHONHOME to the base Python prefix so stdlib is found
        if let Some(base_prefix) = find_base_python_prefix(&venv) {
            std::env::set_var("PYTHONHOME", &base_prefix);
        }

        // Add venv site-packages to PYTHONPATH so installed packages are importable
        if let Some(ver) = find_python_version_dir(&venv) {
            let site_packages = venv.join("lib").join(&ver).join("site-packages");
            let existing = std::env::var("PYTHONPATH").unwrap_or_default();
            if existing.is_empty() {
                std::env::set_var("PYTHONPATH", &site_packages);
            } else {
                std::env::set_var(
                    "PYTHONPATH",
                    format!("{}:{}", site_packages.display(), existing),
                );
            }
        }

        let bin_path = venv.join("bin");
        let current_path = std::env::var("PATH").unwrap_or_default();
        std::env::set_var("PATH", format!("{}:{}", bin_path.display(), current_path));

        // PyO3 with auto-initialize will use these env vars
        // The Python interpreter is initialized lazily on first Python::with_gil call
        Ok(())
    });

    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(PgMlError::PythonError(msg.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_venv_path() {
        use crate::config::DEFAULT_VENV_PATH;
        // Default path constant - either from PG_ML_VENV_PATH env var at compile time
        // or the hardcoded default
        assert!(
            DEFAULT_VENV_PATH == "/var/lib/postgresql/pg_ml" || DEFAULT_VENV_PATH.contains("pg_ml"),
            "DEFAULT_VENV_PATH should contain 'pg_ml', got: {}",
            DEFAULT_VENV_PATH
        );
    }

    #[test]
    fn test_python_path_construction() {
        let venv = PathBuf::from("/var/lib/postgresql/pg_ml");
        let python = venv.join("bin/python");
        assert_eq!(
            python,
            PathBuf::from("/var/lib/postgresql/pg_ml/bin/python")
        );
    }

    /// Requirement lines (one per package) of the embedded lockfile export
    fn locked_requirements() -> Vec<&'static str> {
        LOCKED_REQUIREMENTS
            .lines()
            .filter(|l| l.starts_with(|c: char| c.is_ascii_alphanumeric()))
            .collect()
    }

    #[test]
    fn test_locked_requirements_pin_every_package_with_hashes() {
        let reqs = locked_requirements();
        assert!(reqs.len() > 100, "expected the full PyCaret tree");
        for req in &reqs {
            assert!(req.contains("=="), "not pinned: {req}");
            assert!(req.ends_with('\\'), "no hashes follow: {req}");
        }
        assert!(!LOCKED_REQUIREMENTS.contains("-e "), "editable entry");
        assert!(!LOCKED_REQUIREMENTS.contains("git+"), "VCS entry");
    }

    #[test]
    fn test_locked_requirements_keep_compatibility_pins() {
        let version = |name: &str| {
            locked_requirements()
                .into_iter()
                .find_map(|r| r.strip_prefix(&format!("{name}==")))
                .and_then(|r| r.split([' ', ';']).next())
                .unwrap_or_else(|| panic!("{name} missing from lockfile export"))
        };
        assert!(version("pycaret").starts_with("3.3."));
        assert!(version("mlflow").starts_with("2.9."));
        assert!(version("dask").starts_with("2024."));
        let setuptools_major: u32 = version("setuptools")
            .split('.')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        assert!(setuptools_major < 81);
    }

    #[test]
    fn test_install_args_sync_lockfile_with_hashes() {
        let args = install_requirements_args(
            std::path::Path::new("/venv/bin/python"),
            std::path::Path::new("/venv/requirements.lock.txt"),
        );
        assert_eq!(
            args,
            [
                "pip",
                "sync",
                "--require-hashes",
                "--python",
                "/venv/bin/python",
                "/venv/requirements.lock.txt",
            ]
            .map(std::ffi::OsString::from)
        );
    }

    #[test]
    fn test_find_uv_in_paths() {
        // This test verifies find_uv_in_paths doesn't panic
        // It may or may not find uv depending on system
        let result = find_uv_in_paths();
        // We just check it returns a result (Ok or Err)
        assert!(result.is_ok() || result.is_err());
    }
}
