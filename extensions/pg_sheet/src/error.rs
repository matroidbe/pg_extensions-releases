//! Error types for pg_sheet.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgSheetError {
    #[error("Sheet '{0}' not found")]
    SheetNotFound(String),

    #[error("Sheet '{0}' already exists")]
    SheetAlreadyExists(String),

    #[error("Column '{0}' not found in sheet '{1}'")]
    ColumnNotFound(String, String),

    #[error("Column '{0}' already exists in sheet '{1}'")]
    ColumnAlreadyExists(String, String),

    #[error("Column '{0}' is a source column and cannot be modified")]
    SourceColumnReadOnly(String),

    #[error("Invalid column type: {0}")]
    InvalidColumnType(String),

    #[error("Formula parse error: {0}")]
    FormulaParse(String),

    #[error("Formula evaluation error: {0}")]
    FormulaEval(String),

    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Cell is locked by another user")]
    CellLocked,

    #[error("Snapshot '{0}' not found")]
    SnapshotNotFound(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Invalid source table: {0}")]
    InvalidSource(String),
}

impl From<pgrx::spi::Error> for PgSheetError {
    fn from(e: pgrx::spi::Error) -> Self {
        PgSheetError::Database(e.to_string())
    }
}
