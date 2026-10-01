use thiserror::Error;

/// Error type for invalid distribution parameters or operations.
///
/// The messages match the ones pg_prob raised via `pgrx::error!` so the SQL
/// surface stays identical when the pgrx wrappers delegate here.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ProbError(pub String);

impl ProbError {
    pub fn new(msg: impl Into<String>) -> Self {
        ProbError(msg.into())
    }
}
