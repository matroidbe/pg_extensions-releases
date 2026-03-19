//! Error types for pg_ortools

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgOrtoolsError {
    #[error("Problem not found: {0}")]
    ProblemNotFound(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Invalid constraint: {0}")]
    InvalidConstraint(String),

    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Solver error: {0}")]
    SolverError(String),

    #[error("SPI error: {0}")]
    SpiError(String),

    #[error("JSON error: {0}")]
    JsonError(String),

    #[error("Job not found: {0}")]
    JobNotFound(i64),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
}

impl From<pgrx::spi::Error> for PgOrtoolsError {
    fn from(err: pgrx::spi::Error) -> Self {
        PgOrtoolsError::SpiError(err.to_string())
    }
}
