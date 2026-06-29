//! Error types for the ortools_core optimization engine.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrtoolsCoreError {
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

    #[error("JSON error: {0}")]
    JsonError(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
}
