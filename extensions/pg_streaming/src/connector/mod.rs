//! Input and output connector traits and implementations

pub mod input;
pub mod output;

use crate::record::RecordBatch;

/// Input connector trait — reads batches of records from a source
pub trait InputConnector: Send + Sync {
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

/// Output connector trait — writes processed records to a destination
pub trait OutputConnector: Send + Sync {
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
