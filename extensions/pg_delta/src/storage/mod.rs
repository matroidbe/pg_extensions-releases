//! Storage operations for Delta Lake integration.
//!
//! This module handles:
//! - Reading Delta tables into Postgres
//! - Writing Postgres data to Delta tables
//! - Delta table metadata queries
//! - Storage configuration and connectivity testing

mod config;
mod convert;
mod delta_ops;

pub use config::*;
#[allow(unused_imports)]
pub use convert::*;
pub use delta_ops::*;
