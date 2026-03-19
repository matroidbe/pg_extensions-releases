//! Kafka wire protocol encoder/decoder
//!
//! Kafka uses big-endian binary encoding with length-prefixed messages.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self, Cursor};

use super::types::RequestHeader;

/// Error type for protocol encoding/decoding
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("incomplete message, need more data")]
    Incomplete,
    #[error("invalid message: {0}")]
    Invalid(String),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Read a big-endian i16
pub fn read_i16(buf: &mut Cursor<&[u8]>) -> Result<i16, ProtocolError> {
    if buf.remaining() < 2 {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_i16())
}

/// Read a big-endian i32
pub fn read_i32(buf: &mut Cursor<&[u8]>) -> Result<i32, ProtocolError> {
    if buf.remaining() < 4 {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_i32())
}

/// Read a big-endian i64
#[allow(dead_code)]
pub fn read_i64(buf: &mut Cursor<&[u8]>) -> Result<i64, ProtocolError> {
    if buf.remaining() < 8 {
        return Err(ProtocolError::Incomplete);
    }
    Ok(buf.get_i64())
}

/// Read a nullable string (i16 length prefix, -1 = null)
pub fn read_nullable_string(buf: &mut Cursor<&[u8]>) -> Result<Option<String>, ProtocolError> {
    let len = read_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|e| ProtocolError::Invalid(format!("invalid UTF-8 string: {}", e)))
}

/// Read bytes (i32 length prefix)
#[allow(dead_code)]
pub fn read_bytes(buf: &mut Cursor<&[u8]>) -> Result<Bytes, ProtocolError> {
    let len = read_i32(buf)?;
    if len < 0 {
        return Ok(Bytes::new());
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Bytes::from(bytes))
}

/// Write a big-endian i16
pub fn write_i16(buf: &mut BytesMut, value: i16) {
    buf.put_i16(value);
}

/// Write a big-endian i32
pub fn write_i32(buf: &mut BytesMut, value: i32) {
    buf.put_i32(value);
}

/// Write a big-endian i64
#[allow(dead_code)]
pub fn write_i64(buf: &mut BytesMut, value: i64) {
    buf.put_i64(value);
}

/// Write a nullable string
pub fn write_nullable_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        None => write_i16(buf, -1),
        Some(s) => {
            write_i16(buf, s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
    }
}

/// Write a non-null string
pub fn write_string(buf: &mut BytesMut, value: &str) {
    write_i16(buf, value.len() as i16);
    buf.put_slice(value.as_bytes());
}

/// Parse request header from buffer
pub fn parse_request_header(buf: &mut Cursor<&[u8]>) -> Result<RequestHeader, ProtocolError> {
    let api_key = read_i16(buf)?;
    let api_version = read_i16(buf)?;
    let correlation_id = read_i32(buf)?;
    let client_id = read_nullable_string(buf)?;

    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

/// Write response header
pub fn write_response_header(buf: &mut BytesMut, correlation_id: i32) {
    write_i32(buf, correlation_id);
}

/// Frame a complete message with length prefix
pub fn frame_message(body: BytesMut) -> BytesMut {
    let mut framed = BytesMut::with_capacity(4 + body.len());
    framed.put_i32(body.len() as i32);
    framed.put(body);
    framed
}

/// Write an unsigned varint (for flexible/compact encoding)
/// Used for COMPACT_ARRAY lengths (actual length + 1) and COMPACT_STRING lengths
pub fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80; // More bytes follow
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Write empty tagged fields section (just 0x00)
pub fn write_empty_tagged_fields(buf: &mut BytesMut) {
    buf.put_u8(0);
}
