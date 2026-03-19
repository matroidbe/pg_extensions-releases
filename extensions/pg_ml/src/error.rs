//! Error types for pg_ml

use pyo3::prelude::*;
use thiserror::Error;

/// Error types for pg_ml operations
#[derive(Error, Debug)]
pub enum PgMlError {
    #[error("uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh")]
    UvNotFound,

    #[error("Failed to create venv: {0}")]
    VenvCreationFailed(String),

    #[error("Failed to install packages: {0}")]
    PackageInstallFailed(String),

    #[error("Python error: {0}")]
    PythonError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    #[error("Table already exists: {0}.{1}")]
    TableExists(String, String),

    #[error("JSON error: {0}")]
    JsonError(String),

    #[error("SPI error: {0}")]
    SpiError(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Model not found: {0}")]
    ModelNotFound(String),

    #[error("Setup required: must call pgml.setup() before this function")]
    SetupRequired,

    #[error("Embedding API error: {0}")]
    EmbeddingApiError(String),

    #[error("Embedding provider not configured: {0}")]
    EmbeddingNotConfigured(String),

    #[error("HTTP request failed: {0}")]
    HttpError(String),

    #[error("Unsupported embedding provider: {0}")]
    UnsupportedProvider(String),
}

impl From<reqwest::Error> for PgMlError {
    fn from(err: reqwest::Error) -> Self {
        PgMlError::HttpError(err.to_string())
    }
}

impl From<PyErr> for PgMlError {
    fn from(err: PyErr) -> Self {
        Python::with_gil(|py| {
            let traceback = err
                .traceback(py)
                .and_then(|tb| tb.format().ok())
                .unwrap_or_default();
            PgMlError::PythonError(format!("{}\n{}", err, traceback))
        })
    }
}
