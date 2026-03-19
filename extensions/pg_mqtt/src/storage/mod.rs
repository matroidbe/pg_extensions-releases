//! Storage layer for pg_mqtt
//!
//! Provides async database access via the SPI bridge pattern.

pub mod source_insert;
pub mod spi_bridge;
pub mod spi_client;

pub use spi_bridge::{
    execute_spi_request, ColumnType, SpiBridge, SpiError, SpiParam, SpiReceiver, SpiResult, SpiRow,
    SpiValue,
};
pub use spi_client::MqttStorageClient;
