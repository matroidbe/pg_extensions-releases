//! Administrative functions for pg_kafka

use pgrx::prelude::*;

use crate::config::{
    DEFAULT_HOST, PG_KAFKA_ADVERTISED_PORT, PG_KAFKA_ENABLED, PG_KAFKA_HOST, PG_KAFKA_PORT,
};
use crate::worker::advertised_config;

/// Get server status as JSON
#[pg_extern]
pub fn status() -> pgrx::JsonB {
    let enabled = PG_KAFKA_ENABLED.get();
    let port = PG_KAFKA_PORT.get();
    let host = PG_KAFKA_HOST
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_HOST)
        .to_string();
    // null: each client is told the address it reached
    let advertised_host = advertised_config().host.filter(|h| !h.trim().is_empty());
    let advertised_port = match PG_KAFKA_ADVERTISED_PORT.get() {
        0 => port,
        p => p,
    };

    pgrx::JsonB(serde_json::json!({
        "enabled": enabled,
        "port": port,
        "host": host,
        "advertised_host": advertised_host,
        "advertised_port": advertised_port,
        "info": "Server runs as background worker. Check PostgreSQL logs for status."
    }))
}
