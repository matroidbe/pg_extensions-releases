//! Core types for pg_sheet.
//!
//! These types represent the internal data model. They are NOT PostgresType —
//! they're Rust structs used for internal logic. All SQL I/O uses JSONB or text.

use serde::{Deserialize, Serialize};

/// Metadata for a sheet, stored in pgsheet._sheets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SheetMeta {
    pub name: String,
    pub source_schema: String,
    pub source_table: String,
    pub source_filter: Option<String>,
}

/// Column metadata, stored in pgsheet._columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub sheet_name: String,
    pub col_name: String,
    pub col_type: String,
    pub is_source: bool,
    pub is_formula: bool,
    pub formula_expr: Option<String>,
    pub formula_sql: Option<String>,
}

/// A single cell value as stored in the overlay table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellValue {
    pub entity_id: String,
    pub col_name: String,
    pub value: Option<String>,
    pub formula: Option<String>,
    pub evaluated: Option<String>,
}

/// Parsed formula representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedFormula {
    /// Original formula text (e.g., "=SUM(B2:B10)")
    pub text: String,
    /// Cell references found (e.g., ["B2", "B10", "C3"])
    pub cell_refs: Vec<CellRef>,
    /// Column references found (e.g., ["revenue", "confidence"])
    pub col_refs: Vec<String>,
    /// Functions used (e.g., ["SUM", "IF"])
    pub functions: Vec<String>,
    /// Translated SQL expression (if translatable)
    pub sql: Option<String>,
    /// Whether this formula can be fully evaluated in SQL
    pub sql_translatable: bool,
}

/// A reference to a cell in the spreadsheet grid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CellRef {
    /// Column letter(s) (e.g., "A", "B", "AA")
    pub col: String,
    /// Row number (1-based, like Excel)
    pub row: u32,
}

impl CellRef {
    pub fn new(col: &str, row: u32) -> Self {
        Self {
            col: col.to_uppercase(),
            row,
        }
    }

    /// Convert column letter to 0-based index (A=0, B=1, ..., Z=25, AA=26)
    pub fn col_index(&self) -> u32 {
        let mut idx = 0u32;
        for c in self.col.chars() {
            idx = idx * 26 + (c as u32 - 'A' as u32 + 1);
        }
        idx.saturating_sub(1)
    }
}

/// A range of cells (e.g., B2:B10).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellRange {
    pub start: CellRef,
    pub end: CellRef,
}

/// Snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: String,
    pub sheet_name: String,
    pub label: Option<String>,
    pub cell_count: i64,
    pub created_at: String,
}

/// Supported SQL column types for overlay columns.
pub const VALID_COLUMN_TYPES: &[&str] = &[
    "text",
    "integer",
    "bigint",
    "numeric",
    "boolean",
    "money",
    "date",
    "timestamp",
    "timestamptz",
    "uuid",
    "jsonb",
    "double precision",
    "real",
];

/// Validate a column type string.
pub fn validate_column_type(ty: &str) -> bool {
    let lower = ty.to_lowercase();
    VALID_COLUMN_TYPES.contains(&lower.as_str())
        || lower.starts_with("numeric(")
        || lower.starts_with("varchar(")
        || lower.starts_with("char(")
}
