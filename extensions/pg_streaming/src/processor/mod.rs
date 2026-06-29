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

#[cfg(feature = "xarray")]
pub mod xarray_header;

use crate::record::RecordBatch;

/// Processor trait — transforms a batch of records.
///
/// Owned by one engine worker at a time. `Send` is enough; some custom
/// processors hold `!Sync` runtime state internally.
pub trait Processor: Send {
    /// Process a batch and return the (possibly filtered/transformed) result
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String>;

    /// Human-readable name for this processor
    fn name(&self) -> &str;

    /// Whether this processor maintains state and needs to be called even on
    /// empty batches (e.g., window processors need ticks to emit closed windows).
    fn is_stateful(&self) -> bool {
        false
    }
}
