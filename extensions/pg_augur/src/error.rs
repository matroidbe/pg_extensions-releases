use thiserror::Error;

#[derive(Debug, Error)]
pub enum AugurPgError {
    #[error("unsupported algorithm '{0}' for the given task")]
    UnsupportedAlgorithm(String),

    #[error("unsupported task '{0}' (expected classification|regression|time_series)")]
    UnsupportedTask(String),

    #[error("project '{0}' not found")]
    ProjectNotFound(String),

    #[error("no deployed model for project '{0}'")]
    NoDeployedModel(String),

    #[error("invalid relation name '{0}'")]
    InvalidRelation(String),

    #[error("target column '{0}' not found in source table")]
    TargetNotFound(String),

    #[error("source table has no rows")]
    EmptyTable,

    #[error("feature mismatch: model expects {expected} features, got {actual}")]
    FeatureMismatch { expected: usize, actual: usize },

    #[error("augur error: {0}")]
    Augur(String),

    #[error("polars error: {0}")]
    Polars(String),

    #[error("SPI error: {0}")]
    Spi(String),

    #[error("JSON error: {0}")]
    Json(String),

    #[error("DSL parse error: {0}")]
    DslParse(String),

    #[error("feature not yet supported in pg_augur: {0}")]
    NotSupported(String),

    #[error("{0}")]
    Other(String),
}

impl From<augur_core::error::AugurError> for AugurPgError {
    fn from(e: augur_core::error::AugurError) -> Self {
        Self::Augur(e.to_string())
    }
}

impl From<polars::error::PolarsError> for AugurPgError {
    fn from(e: polars::error::PolarsError) -> Self {
        Self::Polars(e.to_string())
    }
}

impl From<serde_json::Error> for AugurPgError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e.to_string())
    }
}
