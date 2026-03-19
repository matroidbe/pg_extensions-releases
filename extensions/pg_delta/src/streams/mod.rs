//! Stream management for pg_delta.
//!
//! This module handles:
//! - Background worker main loop
//! - Stream lifecycle management
//! - Ingest streams (Delta → Postgres)
//! - Export streams (Postgres → Delta)

mod export;
mod ingest;
mod manager;

pub use manager::run_stream_manager;
