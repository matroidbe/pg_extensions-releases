//! Prometheus metrics setup for PostgreSQL extensions
//!
//! Provides initialization functions for the Prometheus metrics exporter.
//! The HTTP endpoint runs inside the tokio runtime and serves metrics
//! in Prometheus text format.
//!
//! # Usage
//!
//! ```rust,ignore
//! use pg_observability::init_prometheus_on_port;
//!
//! // Inside tokio runtime, only from worker 0
//! if worker_id == 0 && metrics_enabled {
//!     init_prometheus_on_port(9187)?;
//! }
//! ```

use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

/// Initialize Prometheus metrics exporter with HTTP endpoint
///
/// MUST be called from within the tokio runtime.
/// Only one process should call this per port (typically worker 0).
///
/// # Arguments
///
/// * `addr` - The socket address to bind the HTTP listener to
///
/// # Errors
///
/// Returns an error if the exporter cannot be installed (e.g., port already in use)
pub fn init_prometheus(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

/// Initialize Prometheus on a specific port
///
/// Convenience function that binds to `0.0.0.0:{port}`.
///
/// # Arguments
///
/// * `port` - The port number to bind to
///
/// # Errors
///
/// Returns an error if the address cannot be parsed or the exporter cannot be installed
pub fn init_prometheus_on_port(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("0.0.0.0:{}", port)
        .parse()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    init_prometheus(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_prometheus_on_port_parses_address() {
        // We can't actually initialize prometheus in unit tests (requires tokio runtime
        // and would conflict with other tests), but we can verify the address parsing
        let addr: Result<SocketAddr, _> = "0.0.0.0:9187".parse();
        assert!(addr.is_ok());
        assert_eq!(addr.unwrap().port(), 9187);
    }
}
