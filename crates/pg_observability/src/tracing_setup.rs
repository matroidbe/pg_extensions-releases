//! Tracing initialization for pgrx background workers
//!
//! PostgreSQL background workers have a dual-context architecture:
//! - Main bgworker thread: can call pgrx FFI (pgrx::log!)
//! - Tokio worker threads: MUST NOT call pgrx FFI
//!
//! This module provides a tracing subscriber that routes all output to stderr,
//! which PostgreSQL captures to its logs. This works safely from any thread.

use std::io;
use std::sync::Once;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static INIT: Once = Once::new();

/// Initialize tracing for pgrx background worker
///
/// Safe to call multiple times - only initializes once.
/// Uses stderr which PostgreSQL captures to its logs.
///
/// # Environment Variables
///
/// - `RUST_LOG`: Controls log filtering (e.g., `RUST_LOG=pg_kafka=debug,info`)
///
/// # Example
///
/// ```rust
/// use pg_observability::init_tracing;
///
/// // In worker startup
/// init_tracing();
/// ```
pub fn init_tracing() {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        let fmt_layer = fmt::layer()
            .with_target(true)
            .with_thread_ids(true)
            .with_level(true)
            .with_writer(io::stderr)
            .with_ansi(false); // PostgreSQL logs don't support ANSI colors

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_tracing_can_be_called_multiple_times() {
        // Should not panic when called multiple times
        init_tracing();
        init_tracing();
        init_tracing();
    }
}
