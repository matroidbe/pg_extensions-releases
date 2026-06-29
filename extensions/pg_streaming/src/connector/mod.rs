//! Input and output connector traits and implementations

pub mod bridge;
pub mod input;
pub mod output;
pub mod parser;
pub mod registry;
pub mod sdk;
pub mod secrets;

#[allow(unused_imports)]
pub use sdk::{AsyncSink, AsyncSource, Codec, Cursor, ParseContext, Parser, SourceItem};

use crate::record::RecordBatch;

/// Input connector trait — reads batches of records from a source.
///
/// Owned by one engine worker at a time and only `&mut`-accessed from
/// there, so `Send` is enough; `Sync` would be over-tight because some
/// bridge connectors hold `!Sync` internals (e.g., `Box<dyn FnOnce>`
/// factories, `mpsc::Receiver`s).
pub trait InputConnector: Send {
    /// Initialize the connector (resolve offsets, cache IDs, etc.).
    /// Called once before the first poll.
    fn initialize(&mut self, pipeline_name: &str) -> Result<(), String> {
        let _ = pipeline_name;
        Ok(())
    }

    /// Poll for up to `batch_size` records. Returns the batch and the
    /// max offset seen (for committing after successful processing).
    fn poll(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String>;

    /// Commit the given offset (mark records up to this offset as processed)
    fn commit(&mut self, pipeline_name: &str, offset: i64) -> Result<(), String>;
}

/// Output connector trait — writes processed records to a destination.
///
/// Same Send-only rationale as `InputConnector`: bridges hold !Sync
/// internals (e.g., `std::sync::mpsc::Receiver`).
pub trait OutputConnector: Send {
    /// Write a batch of processed records
    fn write(&self, records: &RecordBatch) -> Result<(), String>;
}

/// Drop output connector — discards all records (useful for testing/debugging)
pub struct DropOutput;

impl OutputConnector for DropOutput {
    fn write(&self, _records: &RecordBatch) -> Result<(), String> {
        Ok(())
    }
}
