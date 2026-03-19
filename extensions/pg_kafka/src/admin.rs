//! Administrative functions for pg_kafka

use pgrx::prelude::*;

use crate::config::{
    DEFAULT_HOST, PG_KAFKA_ADVERTISED_HOST, PG_KAFKA_ENABLED, PG_KAFKA_HOST, PG_KAFKA_PORT,
};

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
    let advertised_host = PG_KAFKA_ADVERTISED_HOST
        .get()
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(&host)
        .to_string();

    pgrx::JsonB(serde_json::json!({
        "enabled": enabled,
        "port": port,
        "host": host,
        "advertised_host": advertised_host,
        "info": "Server runs as background worker. Check PostgreSQL logs for status."
    }))
}
