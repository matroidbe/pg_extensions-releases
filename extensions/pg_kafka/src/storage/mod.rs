//! Storage layer for pg_kafka
//!
//! Provides async database access for the TCP server.
//!
//! This module uses SPI (Server Programming Interface) for in-process database access.
//! The SpiBridge provides a channel-based interface for tokio tasks to execute
//! queries on the background worker's main thread where SPI is valid.

pub mod parse;
pub mod source_insert;
pub mod spi_bridge;
pub mod spi_client;
pub mod validation;

pub use spi_bridge::{execute_spi_request, SpiBridge};
pub use spi_client::{MessageToStore, SpiStorageClient, StorageError};
