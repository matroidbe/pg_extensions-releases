//! Processor trait and implementations

pub mod aggregate;
pub mod cep;
pub mod chain;
pub mod dedupe;
pub mod filter;
pub mod join;
pub mod log;
pub mod mapping;
pub mod sql_enrichment;
pub mod unnest;
pub mod window;

use crate::record::RecordBatch;

/// Processor trait — transforms a batch of records
pub trait Processor: Send + Sync {
    /// Process a batch and return the (possibly filtered/transformed) result
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String>;

    /// Human-readable name for this processor
    fn name(&self) -> &str;
}
