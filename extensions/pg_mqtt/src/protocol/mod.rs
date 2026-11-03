//! MQTT 5.0 protocol implementation
//!
//! This module implements the MQTT 5.0 binary protocol:
//! - Packet encoding/decoding
//! - Packet type definitions
//! - Protocol handlers

pub mod client_queue;
pub mod codec;
pub mod handlers;
pub mod types;

pub use codec::{parse_packet, ProtocolError};
pub use handlers::{
    check_publish, deliver_published, disconnect_for, end_session, handle_connect,
    handle_disconnect, handle_pingreq, handle_subscribe, handle_unsubscribe, publish_will,
    route_publish, ClientSession, Delivery, MqttBrokerState, Will, SERVER_RECEIVE_MAXIMUM,
};
pub use types::*;
