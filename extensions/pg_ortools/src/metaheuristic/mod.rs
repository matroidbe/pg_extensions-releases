//! Metaheuristic solver adapter — thin SPI wrapper around ortools_core.
//!
//! Re-exports core types and provides database-loading functions that
//! feed assignment problems to `ortools_core::metaheuristic::solve_local()`.

pub mod db_loader;

pub use db_loader::{is_valid_constraint_type, parse_algorithm, solve_from_db};
