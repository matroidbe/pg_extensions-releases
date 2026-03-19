//! Kafka-specific metrics for pg_kafka extension
//!
//! This module provides helper functions for recording pg_kafka-specific
//! Prometheus metrics. All functions are thread-safe and designed to be
//! called from tokio worker threads.
//!
//! # Usage
//!
//! ```rust,ignore
//! use pg_observability::kafka_metrics::{describe_kafka_metrics, increment_produced, record_produce_latency};
//!
//! // Call once at worker startup to register metric descriptions
//! describe_kafka_metrics();
//!
//! // Record metrics during operations
//! let start = std::time::Instant::now();
//! // ... produce operation ...
//! record_produce_latency(start.elapsed().as_secs_f64());
//! increment_produced("my-topic", 1);
//! ```

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// Register all pg_kafka metric descriptions
///
/// Call once at worker startup to ensure Prometheus output includes
/// HELP and TYPE annotations for all metrics.
pub fn describe_kafka_metrics() {
    describe_counter!(
        "pgkafka_messages_produced_total",
        "Total number of messages produced to pg_kafka topics"
    );
    describe_counter!(
        "pgkafka_messages_consumed_total",
        "Total number of messages consumed from pg_kafka topics"
    );
    describe_gauge!(
        "pgkafka_active_connections",
        "Number of active Kafka protocol connections"
    );
    describe_histogram!(
        "pgkafka_produce_latency_seconds",
        "Latency of produce operations in seconds"
    );
}

/// Increment produced message counter
///
/// # Arguments
///
/// * `topic` - The topic name (used as a label)
/// * `count` - Number of messages produced
pub fn increment_produced(topic: &str, count: u64) {
    counter!("pgkafka_messages_produced_total", "topic" => topic.to_string()).increment(count);
}

/// Increment consumed message counter
///
/// # Arguments
///
/// * `topic` - The topic name (used as a label)
/// * `consumer_group` - The consumer group ID (used as a label)
/// * `count` - Number of messages consumed
pub fn increment_consumed(topic: &str, consumer_group: &str, count: u64) {
    counter!(
        "pgkafka_messages_consumed_total",
        "topic" => topic.to_string(),
        "group" => consumer_group.to_string()
    )
    .increment(count);
}

/// Set active connection count
///
/// # Arguments
///
/// * `count` - Current number of active connections
#[allow(dead_code)]
pub fn set_active_connections(count: u64) {
    gauge!("pgkafka_active_connections").set(count as f64);
}

/// Increment active connection count
pub fn increment_connections() {
    gauge!("pgkafka_active_connections").increment(1.0);
}

/// Decrement active connection count
pub fn decrement_connections() {
    gauge!("pgkafka_active_connections").decrement(1.0);
}

/// Record produce operation latency
///
/// # Arguments
///
/// * `seconds` - Latency in seconds (use `Instant::elapsed().as_secs_f64()`)
pub fn record_produce_latency(seconds: f64) {
    histogram!("pgkafka_produce_latency_seconds").record(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_kafka_metrics_is_callable() {
        // Just verify the function can be called without panicking
        // Actual metrics registration requires a global recorder (Prometheus)
        // which we don't have in unit tests
        describe_kafka_metrics();
    }

    #[test]
    fn test_metric_functions_are_callable() {
        // These won't actually record without a global recorder,
        // but they should not panic
        increment_produced("test-topic", 1);
        increment_consumed("test-topic", "test-group", 5);
        set_active_connections(10);
        increment_connections();
        decrement_connections();
        record_produce_latency(0.05);
    }
}
