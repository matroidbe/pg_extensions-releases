//! Shared observability infrastructure for pg_extensions
//!
//! This crate provides tracing and metrics setup that works correctly within pgrx
//! background workers. Tracing routes all output to stderr, which PostgreSQL
//! captures to its logs. Metrics are exposed via a Prometheus HTTP endpoint.
//!
//! # Usage
//!
//! ```rust,ignore
//! use pg_observability::{init_tracing, init_prometheus_on_port, info, warn, error};
//! use pg_observability::kafka_metrics::{describe_kafka_metrics, increment_produced};
//!
//! // Call once at worker startup (safe to call multiple times)
//! init_tracing();
//!
//! // Register Kafka metric descriptions
//! describe_kafka_metrics();
//!
//! // Start Prometheus endpoint (only from worker 0, inside tokio runtime)
//! if worker_id == 0 && metrics_enabled {
//!     init_prometheus_on_port(9187)?;
//! }
//!
//! // Use structured logging
//! info!(worker_id = 0, port = 9092, "Server started");
//!
//! // Use metrics
//! counter!("requests_total").increment(1);
//! increment_produced("my-topic", 1);
//! ```

pub mod kafka_metrics;
mod metrics_setup;
mod tracing_setup;

pub use kafka_metrics::{
    decrement_connections, describe_kafka_metrics, increment_connections, increment_consumed,
    increment_produced, record_produce_latency, set_active_connections,
};
pub use metrics_setup::{init_prometheus, init_prometheus_on_port};
pub use tracing_setup::init_tracing;

// Re-export tracing macros for convenience
pub use tracing::{debug, error, info, instrument, span, trace, warn, Level};

// Re-export metrics macros for convenience
pub use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};
