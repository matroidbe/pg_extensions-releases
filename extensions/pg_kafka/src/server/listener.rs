//! The broker address advertised to clients (design/pg_kafka/advertised-listener.md)
//!
//! Pure Rust, no pgrx: connection tasks on tokio threads read this.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

/// What the operator configured: `pg_kafka.advertised_host` and
/// `pg_kafka.advertised_port` (0 = unset)
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AdvertisedConfig {
    pub host: Option<String>,
    pub port: i32,
}

/// The configuration shared with connection tasks. The worker's main thread
/// replaces it after a configuration reload; tasks read it per request.
pub type SharedAdvertised = Arc<RwLock<AdvertisedConfig>>;

pub fn shared_advertised(config: AdvertisedConfig) -> SharedAdvertised {
    Arc::new(RwLock::new(config))
}

/// A snapshot of the shared configuration
pub fn current(shared: &SharedAdvertised) -> AdvertisedConfig {
    // A writer cannot panic mid-assignment, so a poisoned lock still holds a
    // whole value
    shared.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// The `(host, port)` to put in Metadata and FindCoordinator responses for a
/// connection whose local (server-side) address is `local`.
///
/// Host: the configured host, else the address the client reached, never
/// `0.0.0.0`. Port: the configured port, else the port we bind.
pub fn advertised_address(
    config: &AdvertisedConfig,
    bind_port: u16,
    local: Option<SocketAddr>,
) -> (String, i32) {
    let host = match config.host.as_deref().map(str::trim) {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => match local {
            Some(addr) if !addr.ip().is_unspecified() => addr.ip().to_string(),
            _ => "localhost".to_string(),
        },
    };
    let port = if config.port > 0 {
        config.port
    } else {
        i32::from(bind_port)
    };
    (host, port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn local() -> Option<SocketAddr> {
        Some("10.0.0.5:9092".parse().unwrap())
    }

    fn config(host: Option<&str>, port: i32) -> AdvertisedConfig {
        AdvertisedConfig {
            host: host.map(str::to_string),
            port,
        }
    }

    #[test]
    fn test_configured_host_and_port_win() {
        let cfg = config(Some("kafka.example.com"), 5591);
        assert_eq!(
            advertised_address(&cfg, 9092, local()),
            ("kafka.example.com".to_string(), 5591)
        );
    }

    #[test]
    fn test_unset_port_falls_back_to_bind_port() {
        let cfg = config(Some("kafka.example.com"), 0);
        assert_eq!(advertised_address(&cfg, 9092, local()).1, 9092);
    }

    #[test]
    fn test_unset_host_uses_the_address_the_client_reached() {
        let cfg = config(None, 0);
        assert_eq!(
            advertised_address(&cfg, 9092, local()),
            ("10.0.0.5".to_string(), 9092)
        );
    }

    #[test]
    fn test_blank_host_counts_as_unset() {
        let cfg = config(Some("  "), 0);
        assert_eq!(advertised_address(&cfg, 9092, local()).0, "10.0.0.5");
    }

    #[test]
    fn test_configured_host_is_trimmed() {
        let cfg = config(Some(" kafka.example.com "), 0);
        assert_eq!(
            advertised_address(&cfg, 9092, local()).0,
            "kafka.example.com"
        );
    }

    #[test]
    fn test_never_advertises_the_unspecified_address() {
        // Unknown local address (should not happen for an accepted socket)
        let cfg = config(None, 0);
        assert_eq!(advertised_address(&cfg, 9092, None).0, "localhost");
        let unspecified: SocketAddr = "0.0.0.0:9092".parse().unwrap();
        assert_eq!(
            advertised_address(&cfg, 9092, Some(unspecified)).0,
            "localhost"
        );
    }

    #[test]
    fn test_shared_config_updates_are_seen() {
        let shared = shared_advertised(config(None, 0));
        assert_eq!(current(&shared), config(None, 0));
        *shared.write().unwrap() = config(Some("new.example.com"), 5591);
        assert_eq!(current(&shared), config(Some("new.example.com"), 5591));
    }
}
