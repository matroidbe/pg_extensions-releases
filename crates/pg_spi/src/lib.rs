//! Async-safe SPI bridge for PostgreSQL extensions
//!
//! Provides channel-based communication between async Tokio tasks and
//! PostgreSQL's synchronous SPI (Server Programming Interface).
//!
//! # Architecture
//!
//! ```text
//! Tokio Task --> SpiBridge::query() --> Channel --> SpiExecutor --> SPI
//!                      |                                |
//!                      +---- oneshot::Receiver <--------+
//! ```
//!
//! The SpiExecutor runs on the background worker's main thread where SPI
//! access is valid. Tokio tasks send query requests through a channel and
//! await responses via oneshot channels.
//!
//! # Usage
//!
//! ```rust,ignore
//! use pg_spi::{SpiBridge, SpiReceiver, execute_spi_request, SpiParam, ColumnType};
//!
//! // Create bridge and receiver
//! let (bridge, mut receiver) = SpiBridge::new(100);
//!
//! // In async context (tokio task)
//! let result = bridge.query(
//!     "SELECT id, name FROM users WHERE active = $1",
//!     vec![SpiParam::Bool(Some(true))],
//!     vec![ColumnType::Int64, ColumnType::Text],
//! ).await?;
//!
//! // In PostgreSQL context (background worker main thread)
//! while let Some(request) = receiver.try_recv() {
//!     execute_spi_request(request);
//! }
//! ```

mod bridge;
mod execute;
mod types;

pub use bridge::{create_bridge, SpiBridge, SpiReceiver, SpiRequest};
pub use execute::execute_spi_request;
pub use types::{ColumnType, SpiError, SpiParam, SpiResult, SpiRow, SpiValue};
