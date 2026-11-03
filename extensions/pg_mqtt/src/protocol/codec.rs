//! MQTT 5.0 wire protocol encoder/decoder
//!
//! MQTT uses a binary protocol with variable-length encoding for the remaining length field.

use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

use super::types::*;

/// Protocol error type
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("incomplete packet, need more data")]
    Incomplete,
    #[error("invalid packet: {0}")]
    Invalid(String),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("malformed remaining length")]
    MalformedRemainingLength,
    #[error("packet too large")]
    PacketTooLarge,
}

/// Maximum packet size (256 MB per MQTT spec)
pub const MAX_PACKET_SIZE: usize = 268_435_455;

/// Read variable byte integer (MQTT remaining length encoding)
///
/// Uses continuation bit encoding: if the high bit is set, more bytes follow.
/// Maximum 4 bytes, representing values up to 268,435,455.
pub fn read_variable_int(buf: &mut Cursor<&[u8]>) -> Result<usize, ProtocolError> {
    let mut value: usize = 0;
    let mut multiplier: usize = 1;

    for _ in 0..4 {
        if !buf.has_remaining() {
            return Err(ProtocolError::Incomplete);
        }
        let byte = buf.get_u8();
        value += ((byte & 0x7F) as usize) * multiplier;

        if (byte & 0x80) == 0 {
            return Ok(value);
        }

        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(ProtocolError::MalformedRemainingLength);
        }
    }

    Err(ProtocolError::MalformedRemainingLength)
}

/// Write variable byte integer
pub fn write_variable_int(buf: &mut BytesMut, mut value: usize) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Read a UTF-8 string (2-byte length prefix)
pub fn read_string(buf: &mut Cursor<&[u8]>) -> Result<String, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Incomplete);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|e| ProtocolError::Invalid(format!("invalid UTF-8: {}", e)))
}

/// Write a UTF-8 string
pub fn write_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

/// Read binary data (2-byte length prefix)
pub fn read_binary(buf: &mut Cursor<&[u8]>) -> Result<Vec<u8>, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Incomplete);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

/// Write binary data
pub fn write_binary(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u16(data.len() as u16);
    buf.put_slice(data);
}

/// Read u16
pub fn read_u16(buf: &mut Cursor<&[u8]>) -> Result<u16, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_u16())
}

/// Read u32
pub fn read_u32(buf: &mut Cursor<&[u8]>) -> Result<u32, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_u32())
}

/// Read a single byte
pub fn read_u8(buf: &mut Cursor<&[u8]>) -> Result<u8, ProtocolError> {
    if !buf.has_remaining() {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_u8())
}

/// Parse MQTT 5.0 properties
pub fn read_properties(buf: &mut Cursor<&[u8]>) -> Result<Vec<(u8, PropertyValue)>, ProtocolError> {
    let props_len = read_variable_int(buf)?;
    if buf.remaining() < props_len {
        return Err(ProtocolError::Incomplete);
    }

    let end_pos = buf.position() as usize + props_len;
    let mut properties = Vec::new();

    while (buf.position() as usize) < end_pos {
        let prop_id = read_u8(buf)?;
        let value = match prop_id {
            // Byte properties
            0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => {
                PropertyValue::Byte(read_u8(buf)?)
            }
            // Two-byte integer properties
            0x13 | 0x21 | 0x22 | 0x23 => PropertyValue::TwoByteInt(read_u16(buf)?),
            // Four-byte integer properties
            0x02 | 0x11 | 0x18 | 0x27 => PropertyValue::FourByteInt(read_u32(buf)?),
            // Variable byte integer properties
            0x0B => PropertyValue::VarInt(read_variable_int(buf)? as u32),
            // UTF-8 string properties
            0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F => {
                PropertyValue::String(read_string(buf)?)
            }
            // Binary data properties
            0x09 | 0x16 => PropertyValue::Binary(read_binary(buf)?),
            // String pair (user property)
            0x26 => {
                let key = read_string(buf)?;
                let value = read_string(buf)?;
                PropertyValue::StringPair(key, value)
            }
            _ => {
                return Err(ProtocolError::Invalid(format!(
                    "unknown property id: {}",
                    prop_id
                )))
            }
        };
        properties.push((prop_id, value));
    }

    Ok(properties)
}

/// Property value types
#[derive(Debug, Clone)]
pub enum PropertyValue {
    Byte(u8),
    TwoByteInt(u16),
    FourByteInt(u32),
    VarInt(u32),
    String(String),
    Binary(Vec<u8>),
    StringPair(String, String),
}

/// Write properties
pub fn write_properties(buf: &mut BytesMut, properties: &[(u8, PropertyValue)]) {
    if properties.is_empty() {
        buf.put_u8(0); // Zero-length properties
        return;
    }

    // Calculate properties length
    let mut props_buf = BytesMut::new();
    for (id, value) in properties {
        props_buf.put_u8(*id);
        match value {
            PropertyValue::Byte(v) => props_buf.put_u8(*v),
            PropertyValue::TwoByteInt(v) => props_buf.put_u16(*v),
            PropertyValue::FourByteInt(v) => props_buf.put_u32(*v),
            PropertyValue::VarInt(v) => write_variable_int(&mut props_buf, *v as usize),
            PropertyValue::String(s) => write_string(&mut props_buf, s),
            PropertyValue::Binary(b) => write_binary(&mut props_buf, b),
            PropertyValue::StringPair(k, v) => {
                write_string(&mut props_buf, k);
                write_string(&mut props_buf, v);
            }
        }
    }

    write_variable_int(buf, props_buf.len());
    buf.put(props_buf);
}

/// Parse a CONNECT packet
pub fn parse_connect(buf: &mut Cursor<&[u8]>) -> Result<ConnectPacket, ProtocolError> {
    // Protocol name
    let protocol_name = read_string(buf)?;
    if protocol_name != "MQTT" {
        return Err(ProtocolError::Invalid(format!(
            "invalid protocol name: {}",
            protocol_name
        )));
    }

    // Protocol version: 4 (MQTT 3.1.1) or 5 (MQTT 5.0)
    let protocol_version = read_u8(buf)?;
    let version = ProtocolVersion::from_level(protocol_version)
        .ok_or(ProtocolError::UnsupportedVersion(protocol_version))?;

    // Connect flags
    let flags_byte = read_u8(buf)?;
    let flags = ConnectFlags::from_byte(flags_byte);

    // Keep alive
    let keep_alive = read_u16(buf)?;

    // Properties (MQTT 5 only)
    let properties = if version.is_v5() {
        parse_connect_properties(&read_properties(buf)?)
    } else {
        ConnectProperties::default()
    };

    // Payload
    let client_id = read_string(buf)?;

    // Will properties and payload
    let (will_properties, will_topic, will_payload) = if flags.will_flag {
        let will_props = if version.is_v5() {
            parse_will_properties(&read_properties(buf)?)
        } else {
            WillProperties::default()
        };
        let topic = read_string(buf)?;
        let payload = read_binary(buf)?;
        (Some(will_props), Some(topic), Some(payload))
    } else {
        (None, None, None)
    };

    // Username
    let username = if flags.username_flag {
        Some(read_string(buf)?)
    } else {
        None
    };

    // Password
    let password = if flags.password_flag {
        Some(read_binary(buf)?)
    } else {
        None
    };

    Ok(ConnectPacket {
        protocol_name,
        protocol_version,
        flags,
        keep_alive,
        properties,
        client_id,
        will_properties,
        will_topic,
        will_payload,
        username,
        password,
    })
}

fn parse_connect_properties(props: &[(u8, PropertyValue)]) -> ConnectProperties {
    let mut result = ConnectProperties::default();
    for (id, value) in props {
        match (id, value) {
            (0x11, PropertyValue::FourByteInt(v)) => result.session_expiry_interval = Some(*v),
            (0x21, PropertyValue::TwoByteInt(v)) => result.receive_maximum = Some(*v),
            (0x27, PropertyValue::FourByteInt(v)) => result.maximum_packet_size = Some(*v),
            (0x22, PropertyValue::TwoByteInt(v)) => result.topic_alias_maximum = Some(*v),
            (0x19, PropertyValue::Byte(v)) => result.request_response_information = Some(*v != 0),
            (0x17, PropertyValue::Byte(v)) => result.request_problem_information = Some(*v != 0),
            (0x15, PropertyValue::String(s)) => result.authentication_method = Some(s.clone()),
            (0x16, PropertyValue::Binary(b)) => result.authentication_data = Some(b.clone()),
            _ => {}
        }
    }
    result
}

fn parse_will_properties(props: &[(u8, PropertyValue)]) -> WillProperties {
    let mut result = WillProperties::default();
    for (id, value) in props {
        match (id, value) {
            (0x18, PropertyValue::FourByteInt(v)) => result.will_delay_interval = Some(*v),
            (0x01, PropertyValue::Byte(v)) => result.payload_format_indicator = Some(*v),
            (0x02, PropertyValue::FourByteInt(v)) => result.message_expiry_interval = Some(*v),
            (0x03, PropertyValue::String(s)) => result.content_type = Some(s.clone()),
            (0x08, PropertyValue::String(s)) => result.response_topic = Some(s.clone()),
            (0x09, PropertyValue::Binary(b)) => result.correlation_data = Some(b.clone()),
            (0x26, PropertyValue::StringPair(k, v)) => {
                result.user_properties.push((k.clone(), v.clone()))
            }
            _ => {}
        }
    }
    result
}

/// The MQTT 3.1.1 CONNACK return code for a reason (MQTT 3.1.1 3.2.2.3)
fn connack_return_code_v311(reason_code: ReasonCode) -> u8 {
    match reason_code {
        ReasonCode::Success => 0x00,
        ReasonCode::UnsupportedProtocolVersion => 0x01,
        ReasonCode::ClientIdentifierNotValid => 0x02,
        ReasonCode::BadUserNameOrPassword => 0x04,
        ReasonCode::NotAuthorized => 0x05,
        _ => 0x03, // Server unavailable
    }
}

/// Encode a CONNACK packet
pub fn encode_connack(packet: &ConnackPacket, version: ProtocolVersion) -> BytesMut {
    if !version.is_v5() {
        let mut buf = BytesMut::with_capacity(4);
        buf.put_u8((PacketType::Connack as u8) << 4);
        buf.put_u8(2);
        buf.put_u8(u8::from(packet.session_present));
        buf.put_u8(connack_return_code_v311(packet.reason_code));
        return buf;
    }

    let mut body = BytesMut::new();

    // Acknowledge flags
    let ack_flags = if packet.session_present { 0x01 } else { 0x00 };
    body.put_u8(ack_flags);

    // Reason code
    body.put_u8(packet.reason_code.into());

    // Properties
    let mut props = Vec::new();

    if let Some(v) = packet.properties.session_expiry_interval {
        props.push((0x11, PropertyValue::FourByteInt(v)));
    }
    if let Some(v) = packet.properties.receive_maximum {
        props.push((0x21, PropertyValue::TwoByteInt(v)));
    }
    if let Some(v) = packet.properties.maximum_qos {
        props.push((0x24, PropertyValue::Byte(v)));
    }
    if let Some(v) = packet.properties.retain_available {
        props.push((0x25, PropertyValue::Byte(if v { 1 } else { 0 })));
    }
    if let Some(v) = packet.properties.maximum_packet_size {
        props.push((0x27, PropertyValue::FourByteInt(v)));
    }
    if let Some(ref s) = packet.properties.assigned_client_identifier {
        props.push((0x12, PropertyValue::String(s.clone())));
    }
    if let Some(v) = packet.properties.topic_alias_maximum {
        props.push((0x22, PropertyValue::TwoByteInt(v)));
    }
    if let Some(v) = packet.properties.wildcard_subscription_available {
        props.push((0x28, PropertyValue::Byte(if v { 1 } else { 0 })));
    }
    if let Some(v) = packet.properties.subscription_identifier_available {
        props.push((0x29, PropertyValue::Byte(if v { 1 } else { 0 })));
    }
    if let Some(v) = packet.properties.shared_subscription_available {
        props.push((0x2A, PropertyValue::Byte(if v { 1 } else { 0 })));
    }
    if let Some(v) = packet.properties.server_keep_alive {
        props.push((0x13, PropertyValue::TwoByteInt(v)));
    }

    write_properties(&mut body, &props);

    // Build final packet with fixed header
    let mut packet_buf = BytesMut::new();
    packet_buf.put_u8((PacketType::Connack as u8) << 4);
    write_variable_int(&mut packet_buf, body.len());
    packet_buf.put(body);

    packet_buf
}

/// Parse a PUBLISH packet
pub fn parse_publish(
    buf: &mut Cursor<&[u8]>,
    flags: u8,
    version: ProtocolVersion,
) -> Result<PublishPacket, ProtocolError> {
    let dup = (flags & 0x08) != 0;
    let qos = QoS::try_from((flags >> 1) & 0x03)
        .map_err(|_| ProtocolError::Invalid("invalid QoS".to_string()))?;
    let retain = (flags & 0x01) != 0;

    let topic = read_string(buf)?;

    let packet_id = if qos != QoS::AtMostOnce {
        Some(read_u16(buf)?)
    } else {
        None
    };

    let properties = if version.is_v5() {
        parse_publish_properties(&read_properties(buf)?)
    } else {
        PublishProperties::default()
    };

    // Remaining bytes are the payload
    let payload = buf.chunk().to_vec();

    Ok(PublishPacket {
        dup,
        qos,
        retain,
        topic,
        packet_id,
        properties,
        payload,
    })
}

fn parse_publish_properties(props: &[(u8, PropertyValue)]) -> PublishProperties {
    let mut result = PublishProperties::default();
    for (id, value) in props {
        match (id, value) {
            (0x01, PropertyValue::Byte(v)) => result.payload_format_indicator = Some(*v),
            (0x02, PropertyValue::FourByteInt(v)) => result.message_expiry_interval = Some(*v),
            (0x23, PropertyValue::TwoByteInt(v)) => result.topic_alias = Some(*v),
            (0x08, PropertyValue::String(s)) => result.response_topic = Some(s.clone()),
            (0x09, PropertyValue::Binary(b)) => result.correlation_data = Some(b.clone()),
            (0x03, PropertyValue::String(s)) => result.content_type = Some(s.clone()),
            (0x26, PropertyValue::StringPair(k, v)) => {
                result.user_properties.push((k.clone(), v.clone()))
            }
            _ => {}
        }
    }
    result
}

/// Encode the properties a PUBLISH carries (MQTT 5 3.3.2.3)
pub fn publish_properties_list(properties: &PublishProperties) -> Vec<(u8, PropertyValue)> {
    let mut props = Vec::new();
    if let Some(v) = properties.payload_format_indicator {
        props.push((0x01, PropertyValue::Byte(v)));
    }
    if let Some(v) = properties.message_expiry_interval {
        props.push((0x02, PropertyValue::FourByteInt(v)));
    }
    if let Some(v) = properties.topic_alias {
        props.push((0x23, PropertyValue::TwoByteInt(v)));
    }
    if let Some(ref s) = properties.response_topic {
        props.push((0x08, PropertyValue::String(s.clone())));
    }
    if let Some(ref b) = properties.correlation_data {
        props.push((0x09, PropertyValue::Binary(b.clone())));
    }
    if let Some(ref s) = properties.content_type {
        props.push((0x03, PropertyValue::String(s.clone())));
    }
    for (k, v) in &properties.user_properties {
        props.push((0x26, PropertyValue::StringPair(k.clone(), v.clone())));
    }
    props
}

/// PUBLISH properties as stored next to a message (retained, outbox):
/// the property block of the wire format
pub fn encode_publish_properties(properties: &PublishProperties) -> Vec<u8> {
    let mut buf = BytesMut::new();
    write_properties(&mut buf, &publish_properties_list(properties));
    buf.to_vec()
}

/// Inverse of encode_publish_properties; empty or unreadable bytes give no
/// properties
pub fn decode_publish_properties(bytes: &[u8]) -> PublishProperties {
    if bytes.is_empty() {
        return PublishProperties::default();
    }
    read_properties(&mut Cursor::new(bytes))
        .map(|props| parse_publish_properties(&props))
        .unwrap_or_default()
}

/// Encode a PUBLISH packet; MQTT 3.1.1 has no properties to carry
pub fn encode_publish(packet: &PublishPacket, version: ProtocolVersion) -> BytesMut {
    let mut body = BytesMut::new();

    // Topic name
    write_string(&mut body, &packet.topic);

    // Packet identifier (only for QoS > 0)
    if packet.qos != QoS::AtMostOnce {
        if let Some(id) = packet.packet_id {
            body.put_u16(id);
        }
    }

    // Properties
    if version.is_v5() {
        write_properties(&mut body, &publish_properties_list(&packet.properties));
    }

    // Payload
    body.put_slice(&packet.payload);

    // Build fixed header
    let mut flags = (PacketType::Publish as u8) << 4;
    if packet.dup {
        flags |= 0x08;
    }
    flags |= (packet.qos as u8) << 1;
    if packet.retain {
        flags |= 0x01;
    }

    let mut packet_buf = BytesMut::new();
    packet_buf.put_u8(flags);
    write_variable_int(&mut packet_buf, body.len());
    packet_buf.put(body);

    packet_buf
}

/// Parse a SUBSCRIBE packet
pub fn parse_subscribe(
    buf: &mut Cursor<&[u8]>,
    version: ProtocolVersion,
) -> Result<SubscribePacket, ProtocolError> {
    let packet_id = read_u16(buf)?;

    let raw_props = if version.is_v5() {
        read_properties(buf)?
    } else {
        Vec::new()
    };
    let mut properties = SubscribeProperties::default();
    for (id, value) in &raw_props {
        if let (0x0B, PropertyValue::VarInt(v)) = (id, value) {
            properties.subscription_identifier = Some(*v);
        }
    }

    let mut subscriptions = Vec::new();
    while buf.has_remaining() {
        let topic_filter = read_string(buf)?;
        let options_byte = read_u8(buf)?;
        let options = SubscriptionOptions::from_byte(options_byte);
        subscriptions.push(SubscriptionRequest {
            topic_filter,
            options,
        });
    }

    Ok(SubscribePacket {
        packet_id,
        properties,
        subscriptions,
    })
}

/// Encode a SUBACK packet. MQTT 3.1.1 has no properties and only the return
/// codes 0x00-0x02 (granted QoS) and 0x80 (failure).
pub fn encode_suback(packet: &SubackPacket, version: ProtocolVersion) -> BytesMut {
    let mut body = BytesMut::new();

    // Packet identifier
    body.put_u16(packet.packet_id);

    // Properties
    if version.is_v5() {
        let mut props = Vec::new();
        if let Some(ref s) = packet.properties.reason_string {
            props.push((0x1F, PropertyValue::String(s.clone())));
        }
        write_properties(&mut body, &props);
    }

    // Reason codes
    for code in &packet.reason_codes {
        let code = u8::from(*code);
        body.put_u8(if version.is_v5() || code < 0x80 {
            code
        } else {
            0x80
        });
    }

    // Build fixed header
    let mut packet_buf = BytesMut::new();
    packet_buf.put_u8((PacketType::Suback as u8) << 4);
    write_variable_int(&mut packet_buf, body.len());
    packet_buf.put(body);

    packet_buf
}

/// Parse an UNSUBSCRIBE packet
pub fn parse_unsubscribe(
    buf: &mut Cursor<&[u8]>,
    version: ProtocolVersion,
) -> Result<UnsubscribePacket, ProtocolError> {
    let packet_id = read_u16(buf)?;

    if version.is_v5() {
        read_properties(buf)?;
    }
    let properties = UnsubscribeProperties::default();

    let mut topic_filters = Vec::new();
    while buf.has_remaining() {
        topic_filters.push(read_string(buf)?);
    }

    Ok(UnsubscribePacket {
        packet_id,
        properties,
        topic_filters,
    })
}

/// Encode an UNSUBACK packet; in MQTT 3.1.1 it is just the packet id
pub fn encode_unsuback(packet: &UnsubackPacket, version: ProtocolVersion) -> BytesMut {
    let mut body = BytesMut::new();

    // Packet identifier
    body.put_u16(packet.packet_id);

    if !version.is_v5() {
        let mut packet_buf = BytesMut::new();
        packet_buf.put_u8((PacketType::Unsuback as u8) << 4);
        write_variable_int(&mut packet_buf, body.len());
        packet_buf.put(body);
        return packet_buf;
    }

    // Properties
    let mut props = Vec::new();
    if let Some(ref s) = packet.properties.reason_string {
        props.push((0x1F, PropertyValue::String(s.clone())));
    }
    write_properties(&mut body, &props);

    // Reason codes
    for code in &packet.reason_codes {
        body.put_u8((*code).into());
    }

    // Build fixed header
    let mut packet_buf = BytesMut::new();
    packet_buf.put_u8((PacketType::Unsuback as u8) << 4);
    write_variable_int(&mut packet_buf, body.len());
    packet_buf.put(body);

    packet_buf
}

/// Parse a DISCONNECT packet
pub fn parse_disconnect(buf: &mut Cursor<&[u8]>) -> Result<DisconnectPacket, ProtocolError> {
    // Reason code (default to normal if not present)
    let reason_code = if buf.has_remaining() {
        match read_u8(buf)? {
            0x00 => ReasonCode::Success, // Normal disconnection
            0x04 => ReasonCode::DisconnectWithWillMessage,
            0x80 => ReasonCode::UnspecifiedError,
            0x81 => ReasonCode::MalformedPacket,
            0x82 => ReasonCode::ProtocolError,
            0x89 => ReasonCode::ServerBusy,
            0x8D => ReasonCode::KeepAliveTimeout,
            0x8E => ReasonCode::SessionTakenOver,
            _ => ReasonCode::UnspecifiedError,
        }
    } else {
        ReasonCode::Success // Normal disconnection
    };

    let properties = if buf.has_remaining() {
        let raw_props = read_properties(buf)?;
        parse_disconnect_properties(&raw_props)
    } else {
        DisconnectProperties::default()
    };

    Ok(DisconnectPacket {
        reason_code,
        properties,
    })
}

fn parse_disconnect_properties(props: &[(u8, PropertyValue)]) -> DisconnectProperties {
    let mut result = DisconnectProperties::default();
    for (id, value) in props {
        match (id, value) {
            (0x11, PropertyValue::FourByteInt(v)) => result.session_expiry_interval = Some(*v),
            (0x1F, PropertyValue::String(s)) => result.reason_string = Some(s.clone()),
            (0x1C, PropertyValue::String(s)) => result.server_reference = Some(s.clone()),
            _ => {}
        }
    }
    result
}

/// Encode a DISCONNECT packet
pub fn encode_disconnect(packet: &DisconnectPacket) -> BytesMut {
    let mut body = BytesMut::new();

    // Reason code
    body.put_u8(packet.reason_code.into());

    // Properties
    let mut props = Vec::new();
    if let Some(v) = packet.properties.session_expiry_interval {
        props.push((0x11, PropertyValue::FourByteInt(v)));
    }
    if let Some(ref s) = packet.properties.reason_string {
        props.push((0x1F, PropertyValue::String(s.clone())));
    }
    if let Some(ref s) = packet.properties.server_reference {
        props.push((0x1C, PropertyValue::String(s.clone())));
    }
    write_properties(&mut body, &props);

    // Build fixed header
    let mut packet_buf = BytesMut::new();
    packet_buf.put_u8((PacketType::Disconnect as u8) << 4);
    write_variable_int(&mut packet_buf, body.len());
    packet_buf.put(body);

    packet_buf
}

/// Encode PINGRESP packet
pub fn encode_pingresp() -> BytesMut {
    let mut buf = BytesMut::with_capacity(2);
    buf.put_u8((PacketType::Pingresp as u8) << 4);
    buf.put_u8(0); // Zero remaining length
    buf
}

/// Encode a PUBACK, PUBREC, PUBREL or PUBCOMP packet. On success the reason
/// code and properties are omitted (remaining length 2); otherwise the reason
/// code follows the packet id and the property length is omitted (remaining
/// length 3). MQTT 3.1.1 has no reason code: always remaining length 2.
fn encode_publish_ack(
    packet_type: PacketType,
    packet_id: u16,
    reason_code: ReasonCode,
    version: ProtocolVersion,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    // PUBREL's fixed header flags are 0010 (MQTT 5 3.6.1)
    let flags = if packet_type == PacketType::Pubrel {
        0x02
    } else {
        0x00
    };
    buf.put_u8(((packet_type as u8) << 4) | flags);
    if reason_code == ReasonCode::Success || !version.is_v5() {
        buf.put_u8(2);
        buf.put_u16(packet_id);
    } else {
        buf.put_u8(3);
        buf.put_u16(packet_id);
        buf.put_u8(reason_code.into());
    }
    buf
}

/// Encode a PUBACK packet (QoS 1)
pub fn encode_puback(
    packet_id: u16,
    reason_code: ReasonCode,
    version: ProtocolVersion,
) -> BytesMut {
    encode_publish_ack(PacketType::Puback, packet_id, reason_code, version)
}

/// Encode a PUBREC packet (QoS 2, step 1)
pub fn encode_pubrec(
    packet_id: u16,
    reason_code: ReasonCode,
    version: ProtocolVersion,
) -> BytesMut {
    encode_publish_ack(PacketType::Pubrec, packet_id, reason_code, version)
}

/// Encode a PUBREL packet (QoS 2, step 2)
pub fn encode_pubrel(packet_id: u16) -> BytesMut {
    encode_publish_ack(
        PacketType::Pubrel,
        packet_id,
        ReasonCode::Success,
        ProtocolVersion::V5,
    )
}

/// Encode a PUBCOMP packet (QoS 2, step 3)
pub fn encode_pubcomp(packet_id: u16) -> BytesMut {
    encode_publish_ack(
        PacketType::Pubcomp,
        packet_id,
        ReasonCode::Success,
        ProtocolVersion::V5,
    )
}

/// Try to parse a complete MQTT packet from a buffer
///
/// Returns Ok(Some((packet, bytes_consumed))) if a complete packet was parsed,
/// Ok(None) if more data is needed, or Err on protocol error. `version` is
/// the connection's (from its CONNECT); a CONNECT carries its own.
pub fn parse_packet(
    data: &[u8],
    version: ProtocolVersion,
) -> Result<Option<(MqttPacket, usize)>, ProtocolError> {
    if data.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(data);

    // Fixed header
    let first_byte = read_u8(&mut cursor)?;
    let packet_type = (first_byte >> 4) & 0x0F;
    let flags = first_byte & 0x0F;

    // Remaining length
    let remaining_length = match read_variable_int(&mut cursor) {
        Ok(len) => len,
        Err(ProtocolError::Incomplete) => return Ok(None),
        Err(e) => return Err(e),
    };

    let header_len = cursor.position() as usize;
    let total_len = header_len + remaining_length;

    if data.len() < total_len {
        return Ok(None);
    }

    // Parse the packet body
    let body = &data[header_len..total_len];
    let mut body_cursor = Cursor::new(body);

    let packet = match PacketType::try_from(packet_type) {
        Ok(PacketType::Connect) => MqttPacket::Connect(parse_connect(&mut body_cursor)?),
        Ok(PacketType::Publish) => {
            MqttPacket::Publish(parse_publish(&mut body_cursor, flags, version)?)
        }
        Ok(PacketType::Subscribe) => {
            MqttPacket::Subscribe(parse_subscribe(&mut body_cursor, version)?)
        }
        Ok(PacketType::Unsubscribe) => {
            MqttPacket::Unsubscribe(parse_unsubscribe(&mut body_cursor, version)?)
        }
        Ok(PacketType::Pingreq) => MqttPacket::Pingreq,
        Ok(PacketType::Disconnect) => MqttPacket::Disconnect(parse_disconnect(&mut body_cursor)?),
        Ok(PacketType::Puback) => {
            let packet_id = read_u16(&mut body_cursor)?;
            MqttPacket::Puback { packet_id }
        }
        Ok(PacketType::Pubrec) => {
            let packet_id = read_u16(&mut body_cursor)?;
            // Absent reason code means Success (0x00)
            let reason_code = if body_cursor.has_remaining() {
                read_u8(&mut body_cursor)?
            } else {
                0
            };
            MqttPacket::Pubrec {
                packet_id,
                reason_code,
            }
        }
        Ok(PacketType::Pubrel) => {
            let packet_id = read_u16(&mut body_cursor)?;
            MqttPacket::Pubrel { packet_id }
        }
        Ok(PacketType::Pubcomp) => {
            let packet_id = read_u16(&mut body_cursor)?;
            MqttPacket::Pubcomp { packet_id }
        }
        Ok(PacketType::Auth) => MqttPacket::Auth,
        _ => {
            return Err(ProtocolError::Invalid(format!(
                "unsupported packet type: {}",
                packet_type
            )))
        }
    };

    Ok(Some((packet, total_len)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_variable_int_encoding() {
        let mut buf = BytesMut::new();

        // Test single byte
        write_variable_int(&mut buf, 0);
        assert_eq!(&buf[..], &[0]);
        buf.clear();

        write_variable_int(&mut buf, 127);
        assert_eq!(&buf[..], &[127]);
        buf.clear();

        // Test two bytes
        write_variable_int(&mut buf, 128);
        assert_eq!(&buf[..], &[0x80, 0x01]);
        buf.clear();

        write_variable_int(&mut buf, 16383);
        assert_eq!(&buf[..], &[0xFF, 0x7F]);
        buf.clear();

        // Test three bytes
        write_variable_int(&mut buf, 16384);
        assert_eq!(&buf[..], &[0x80, 0x80, 0x01]);
    }

    #[test]
    fn test_variable_int_decoding() {
        let data = [0x00];
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(read_variable_int(&mut cursor).unwrap(), 0);

        let data = [127];
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(read_variable_int(&mut cursor).unwrap(), 127);

        let data = [0x80, 0x01];
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(read_variable_int(&mut cursor).unwrap(), 128);

        let data = [0xFF, 0x7F];
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(read_variable_int(&mut cursor).unwrap(), 16383);
    }

    #[test]
    fn test_string_encoding() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, "hello");
        assert_eq!(&buf[..], &[0, 5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_string_decoding() {
        let data = [0, 5, b'h', b'e', b'l', b'l', b'o'];
        let mut cursor = Cursor::new(&data[..]);
        assert_eq!(read_string(&mut cursor).unwrap(), "hello");
    }

    #[test]
    fn test_pingresp_encoding() {
        let buf = encode_pingresp();
        assert_eq!(&buf[..], &[0xD0, 0x00]); // PINGRESP with zero remaining length
    }

    #[test]
    fn test_puback_encoding() {
        let buf = encode_puback(0x1234, ReasonCode::Success, ProtocolVersion::V5);
        assert_eq!(&buf[..], &[0x40, 0x02, 0x12, 0x34]);

        let buf = encode_puback(7, ReasonCode::PayloadFormatInvalid, ProtocolVersion::V5);
        assert_eq!(&buf[..], &[0x40, 0x03, 0x00, 0x07, 0x99]);

        // What we send parses back as a PUBACK for the same packet id
        let (packet, consumed) = parse_packet(&buf, ProtocolVersion::V5).unwrap().unwrap();
        assert_eq!(consumed, 5);
        assert!(matches!(packet, MqttPacket::Puback { packet_id: 7 }));
    }

    #[test]
    fn test_qos2_ack_encoding() {
        assert_eq!(
            &encode_pubrec(9, ReasonCode::Success, ProtocolVersion::V5)[..],
            &[0x50, 0x02, 0x00, 0x09]
        );
        assert_eq!(&encode_pubrel(9)[..], &[0x62, 0x02, 0x00, 0x09]);
        assert_eq!(&encode_pubcomp(9)[..], &[0x70, 0x02, 0x00, 0x09]);

        // A PUBREC with an error reason parses with it
        let buf = encode_pubrec(9, ReasonCode::PayloadFormatInvalid, ProtocolVersion::V5);
        let (packet, _) = parse_packet(&buf, ProtocolVersion::V5).unwrap().unwrap();
        assert!(matches!(
            packet,
            MqttPacket::Pubrec {
                packet_id: 9,
                reason_code: 0x99
            }
        ));
    }

    #[test]
    fn test_v311_connect_parses_without_properties() {
        // CONNECT, level 4, clean session + Will QoS 1, keep-alive 30,
        // client id "c", Will topic "w", Will payload "x"
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x0E, 0x00, 0x1E];
        body.extend_from_slice(&[0x00, 0x01, b'c', 0x00, 0x01, b'w', 0x00, 0x01, b'x']);
        let mut packet = vec![0x10, body.len() as u8];
        packet.extend_from_slice(&body);
        let (parsed, _) = parse_packet(&packet, ProtocolVersion::V5).unwrap().unwrap();
        let MqttPacket::Connect(connect) = parsed else {
            panic!("not a CONNECT");
        };
        assert_eq!(connect.protocol_version, 4);
        assert_eq!(connect.client_id, "c");
        assert_eq!(connect.will_topic.as_deref(), Some("w"));
        assert_eq!(connect.will_payload.as_deref(), Some(&b"x"[..]));
        assert_eq!(connect.flags.will_qos, QoS::AtLeastOnce);
    }

    #[test]
    fn test_v311_encodings_have_no_properties_or_reasons() {
        let v4 = ProtocolVersion::V311;
        let connack = ConnackPacket {
            session_present: false,
            reason_code: ReasonCode::ClientIdentifierNotValid,
            properties: ConnackProperties {
                receive_maximum: Some(10),
                ..Default::default()
            },
        };
        assert_eq!(&encode_connack(&connack, v4)[..], &[0x20, 0x02, 0x00, 0x02]);

        let publish = PublishPacket {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "t".into(),
            packet_id: Some(5),
            properties: PublishProperties {
                content_type: Some("x".into()),
                ..Default::default()
            },
            payload: b"p".to_vec(),
        };
        assert_eq!(
            &encode_publish(&publish, v4)[..],
            &[0x32, 0x06, 0x00, 0x01, b't', 0x00, 0x05, b'p']
        );
        let (parsed, _) = parse_packet(&encode_publish(&publish, v4), v4)
            .unwrap()
            .unwrap();
        assert!(matches!(parsed, MqttPacket::Publish(p) if p.payload == b"p"));

        assert_eq!(
            &encode_puback(5, ReasonCode::PayloadFormatInvalid, v4)[..],
            &[0x40, 0x02, 0x00, 0x05]
        );
        let suback = SubackPacket {
            packet_id: 1,
            properties: SubackProperties::default(),
            reason_codes: vec![ReasonCode::GrantedQoS2, ReasonCode::TopicFilterInvalid],
        };
        assert_eq!(
            &encode_suback(&suback, v4)[..],
            &[0x90, 0x04, 0x00, 0x01, 0x02, 0x80]
        );
        let unsuback = UnsubackPacket {
            packet_id: 1,
            properties: UnsubackProperties::default(),
            reason_codes: vec![ReasonCode::Success],
        };
        assert_eq!(
            &encode_unsuback(&unsuback, v4)[..],
            &[0xB0, 0x02, 0x00, 0x01]
        );
    }

    #[test]
    fn test_publish_properties_roundtrip() {
        let props = PublishProperties {
            payload_format_indicator: Some(1),
            content_type: Some("application/json".into()),
            correlation_data: Some(vec![0xff, 0x00, 0x33]),
            response_topic: Some("reply/here".into()),
            user_properties: vec![("a".into(), "2".into()), ("c".into(), "3".into())],
            ..Default::default()
        };
        let decoded = decode_publish_properties(&encode_publish_properties(&props));
        assert_eq!(decoded.payload_format_indicator, Some(1));
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.correlation_data, Some(vec![0xff, 0x00, 0x33]));
        assert_eq!(decoded.response_topic.as_deref(), Some("reply/here"));
        assert_eq!(decoded.user_properties, props.user_properties);
        assert_eq!(decode_publish_properties(&[]).user_properties, vec![]);
    }
}
