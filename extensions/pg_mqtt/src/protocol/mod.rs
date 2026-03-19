//! MQTT 5.0 protocol implementation
//!
//! This module implements the MQTT 5.0 binary protocol:
//! - Packet encoding/decoding
//! - Packet type definitions
//! - Protocol handlers

pub mod codec;
pub mod handlers;
pub mod types;

pub use codec::{parse_packet, ProtocolError};
pub use handlers::{
    handle_connect, handle_disconnect, handle_pingreq, handle_publish, handle_subscribe,
    handle_unsubscribe, ClientSession, MqttBrokerState,
};
pub use types::*;
