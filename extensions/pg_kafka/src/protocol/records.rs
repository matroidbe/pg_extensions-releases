//! Kafka RecordBatch encoding/decoding
//!
//! Implements the Kafka RecordBatch format (magic=2) used in Produce and Fetch APIs.
//! See: https://kafka.apache.org/documentation/#recordbatch

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;

use super::codec::ProtocolError;

/// A single record within a RecordBatch
#[derive(Debug, Clone)]
pub struct Record {
    /// Record attributes (currently unused, always 0)
    pub attributes: i8,
    /// Timestamp delta from batch first_timestamp
    pub timestamp_delta: i64,
    /// Offset delta from batch base_offset
    pub offset_delta: i32,
    /// Record key (optional)
    pub key: Option<Bytes>,
    /// Record value
    pub value: Bytes,
    /// Record headers
    pub headers: Vec<RecordHeader>,
}

/// A record header (key-value pair)
#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

/// A batch of records (Kafka's RecordBatch format, magic=2)
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// First offset in the batch
    pub base_offset: i64,
    /// Partition leader epoch (we use 0)
    pub partition_leader_epoch: i32,
    /// Batch attributes (compression, timestamp type, etc.)
    pub attributes: i16,
    /// Offset of last record relative to base_offset
    pub last_offset_delta: i32,
    /// Timestamp of first record
    pub first_timestamp: i64,
    /// Max timestamp in batch
    pub max_timestamp: i64,
    /// Producer ID for idempotent producer (-1 if not used)
    pub producer_id: i64,
    /// Producer epoch (-1 if not used)
    pub producer_epoch: i16,
    /// Base sequence number (-1 if not used)
    pub base_sequence: i32,
    /// The records
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 0,
            first_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}

// ============================================================================
// Varint/Varlong encoding (Kafka uses protobuf-style zigzag encoding)
// ============================================================================

/// Read a variable-length signed integer (zigzag decoded)
pub fn read_varint(buf: &mut Cursor<&[u8]>) -> Result<i32, ProtocolError> {
    let unsigned = read_unsigned_varint(buf)?;
    // Zigzag decode: (unsigned >> 1) ^ -(unsigned & 1)
    Ok(((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32)))
}

/// Read a variable-length signed long (zigzag decoded)
pub fn read_varlong(buf: &mut Cursor<&[u8]>) -> Result<i64, ProtocolError> {
    let unsigned = read_unsigned_varlong(buf)?;
    // Zigzag decode
    Ok(((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64)))
}

/// Read an unsigned varint
fn read_unsigned_varint(buf: &mut Cursor<&[u8]>) -> Result<u32, ProtocolError> {
    let mut result: u32 = 0;
    let mut shift = 0;

    loop {
        if buf.remaining() < 1 {
            return Err(ProtocolError::Incomplete);
        }
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 32 {
            return Err(ProtocolError::Invalid("varint too long".to_string()));
        }
    }

    Ok(result)
}

/// Read an unsigned varlong
fn read_unsigned_varlong(buf: &mut Cursor<&[u8]>) -> Result<u64, ProtocolError> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buf.remaining() < 1 {
            return Err(ProtocolError::Incomplete);
        }
        let byte = buf.get_u8();
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(ProtocolError::Invalid("varlong too long".to_string()));
        }
    }

    Ok(result)
}

/// Write a variable-length signed integer (zigzag encoded)
pub fn write_varint(buf: &mut BytesMut, value: i32) {
    // Zigzag encode: (value << 1) ^ (value >> 31)
    let unsigned = ((value << 1) ^ (value >> 31)) as u32;
    write_unsigned_varint(buf, unsigned);
}

/// Write a variable-length signed long (zigzag encoded)
pub fn write_varlong(buf: &mut BytesMut, value: i64) {
    // Zigzag encode
    let unsigned = ((value << 1) ^ (value >> 63)) as u64;
    write_unsigned_varlong(buf, unsigned);
}

/// Write an unsigned varint
fn write_unsigned_varint(buf: &mut BytesMut, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Write an unsigned varlong
fn write_unsigned_varlong(buf: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

// ============================================================================
// Variable-length bytes and strings (for records)
// ============================================================================

/// Read variable-length bytes (varint length prefix, -1 = null)
fn read_varlen_bytes(buf: &mut Cursor<&[u8]>) -> Result<Option<Bytes>, ProtocolError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(Some(Bytes::from(bytes)))
}

/// Read variable-length string (varint length prefix)
fn read_varlen_string(buf: &mut Cursor<&[u8]>) -> Result<String, ProtocolError> {
    let len = read_varint(buf)?;
    if len < 0 {
        return Ok(String::new());
    }
    let len = len as usize;
    if buf.remaining() < len {
        return Err(ProtocolError::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|e| ProtocolError::Invalid(format!("invalid UTF-8: {}", e)))
}

/// Write variable-length bytes (varint length prefix, -1 for null)
fn write_varlen_bytes(buf: &mut BytesMut, value: Option<&[u8]>) {
    match value {
        None => write_varint(buf, -1),
        Some(bytes) => {
            write_varint(buf, bytes.len() as i32);
            buf.put_slice(bytes);
        }
    }
}

/// Write variable-length string
fn write_varlen_string(buf: &mut BytesMut, value: &str) {
    write_varint(buf, value.len() as i32);
    buf.put_slice(value.as_bytes());
}

// ============================================================================
// RecordBatch decoding
// ============================================================================

/// Decode a RecordBatch from the wire format
///
/// Format:
/// - base_offset: i64
/// - batch_length: i32 (length of everything after this field)
/// - partition_leader_epoch: i32
/// - magic: i8 (must be 2)
/// - crc: u32 (CRC of everything after crc field)
/// - attributes: i16
/// - last_offset_delta: i32
/// - first_timestamp: i64
/// - max_timestamp: i64
/// - producer_id: i64
/// - producer_epoch: i16
/// - base_sequence: i32
/// - records_count: i32
/// - records: [Record]
pub fn decode_record_batch(buf: &mut Cursor<&[u8]>) -> Result<RecordBatch, ProtocolError> {
    // Check minimum size for header
    if buf.remaining() < 12 {
        return Err(ProtocolError::Incomplete);
    }

    let base_offset = buf.get_i64();
    let batch_length = buf.get_i32();

    if batch_length < 0 {
        return Err(ProtocolError::Invalid("negative batch length".to_string()));
    }

    let batch_length = batch_length as usize;
    if buf.remaining() < batch_length {
        return Err(ProtocolError::Incomplete);
    }

    let partition_leader_epoch = buf.get_i32();
    let magic = buf.get_i8();

    if magic != 2 {
        return Err(ProtocolError::Invalid(format!(
            "unsupported magic byte: {}, expected 2",
            magic
        )));
    }

    let _crc = buf.get_u32(); // We skip CRC validation for now
    let attributes = buf.get_i16();
    let last_offset_delta = buf.get_i32();
    let first_timestamp = buf.get_i64();
    let max_timestamp = buf.get_i64();
    let producer_id = buf.get_i64();
    let producer_epoch = buf.get_i16();
    let base_sequence = buf.get_i32();
    let records_count = buf.get_i32();

    if records_count < 0 {
        return Err(ProtocolError::Invalid("negative records count".to_string()));
    }

    let mut records = Vec::with_capacity(records_count as usize);
    for _ in 0..records_count {
        records.push(decode_record(buf)?);
    }

    Ok(RecordBatch {
        base_offset,
        partition_leader_epoch,
        attributes,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
    })
}

/// Decode a single Record
fn decode_record(buf: &mut Cursor<&[u8]>) -> Result<Record, ProtocolError> {
    let _length = read_varint(buf)?; // Record length (we ignore, read until done)
    let attributes = buf.get_i8();
    let timestamp_delta = read_varlong(buf)?;
    let offset_delta = read_varint(buf)?;
    let key = read_varlen_bytes(buf)?;
    let value = read_varlen_bytes(buf)?.unwrap_or_default();

    // Headers
    let headers_count = read_varint(buf)?;
    let mut headers = Vec::new();
    if headers_count > 0 {
        for _ in 0..headers_count {
            let key = read_varlen_string(buf)?;
            let value = read_varlen_bytes(buf)?;
            headers.push(RecordHeader { key, value });
        }
    }

    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}

// ============================================================================
// RecordBatch encoding
// ============================================================================

/// Encode a RecordBatch to wire format
pub fn encode_record_batch(batch: &RecordBatch) -> BytesMut {
    let mut buf = BytesMut::with_capacity(256);

    // We'll write the batch body first, then prepend base_offset and batch_length
    let mut body = BytesMut::with_capacity(256);

    body.put_i32(batch.partition_leader_epoch);
    body.put_i8(2); // magic = 2
    body.put_u32(0); // CRC placeholder (we don't compute it, clients don't require it for responses)
    body.put_i16(batch.attributes);
    body.put_i32(batch.last_offset_delta);
    body.put_i64(batch.first_timestamp);
    body.put_i64(batch.max_timestamp);
    body.put_i64(batch.producer_id);
    body.put_i16(batch.producer_epoch);
    body.put_i32(batch.base_sequence);
    body.put_i32(batch.records.len() as i32);

    for record in &batch.records {
        encode_record(&mut body, record);
    }

    // Now write the full batch
    buf.put_i64(batch.base_offset);
    buf.put_i32(body.len() as i32);
    buf.put(body);

    buf
}

/// Encode a single Record
fn encode_record(buf: &mut BytesMut, record: &Record) {
    // Build record body first to get length
    let mut record_body = BytesMut::with_capacity(64);

    record_body.put_i8(record.attributes);
    write_varlong(&mut record_body, record.timestamp_delta);
    write_varint(&mut record_body, record.offset_delta);
    write_varlen_bytes(&mut record_body, record.key.as_deref());
    write_varlen_bytes(&mut record_body, Some(&record.value));

    // Headers
    write_varint(&mut record_body, record.headers.len() as i32);
    for header in &record.headers {
        write_varlen_string(&mut record_body, &header.key);
        write_varlen_bytes(&mut record_body, header.value.as_deref());
    }

    // Write length + body
    write_varint(buf, record_body.len() as i32);
    buf.put(record_body);
}

/// Create a RecordBatch from a list of messages (for Fetch responses)
pub fn create_record_batch(
    base_offset: i64,
    first_timestamp: i64,
    messages: Vec<(Option<Bytes>, Bytes)>,
) -> RecordBatch {
    let mut records = Vec::with_capacity(messages.len());
    let max_timestamp = first_timestamp;

    for (i, (key, value)) in messages.into_iter().enumerate() {
        records.push(Record {
            attributes: 0,
            timestamp_delta: 0, // All same timestamp for simplicity
            offset_delta: i as i32,
            key,
            value,
            headers: Vec::new(),
        });
    }

    let last_offset_delta = if records.is_empty() {
        0
    } else {
        (records.len() - 1) as i32
    };

    RecordBatch {
        base_offset,
        partition_leader_epoch: 0,
        attributes: 0,
        last_offset_delta,
        first_timestamp,
        max_timestamp,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encode_decode_positive() {
        let values = [0, 1, 127, 128, 255, 16383, 16384, i32::MAX];
        for &val in &values {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, val);

            let mut cursor = Cursor::new(buf.as_ref());
            let decoded = read_varint(&mut cursor).unwrap();
            assert_eq!(decoded, val, "failed for value {}", val);
        }
    }

    #[test]
    fn test_varint_encode_decode_negative() {
        let values = [-1, -127, -128, -255, -16383, -16384, i32::MIN];
        for &val in &values {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, val);

            let mut cursor = Cursor::new(buf.as_ref());
            let decoded = read_varint(&mut cursor).unwrap();
            assert_eq!(decoded, val, "failed for value {}", val);
        }
    }

    #[test]
    fn test_varlong_encode_decode() {
        let values: [i64; 8] = [0, 1, -1, 127, -128, i64::MAX, i64::MIN, 1234567890123];
        for &val in &values {
            let mut buf = BytesMut::new();
            write_varlong(&mut buf, val);

            let mut cursor = Cursor::new(buf.as_ref());
            let decoded = read_varlong(&mut cursor).unwrap();
            assert_eq!(decoded, val, "failed for value {}", val);
        }
    }

    #[test]
    fn test_record_batch_roundtrip() {
        let batch = RecordBatch {
            base_offset: 100,
            partition_leader_epoch: 0,
            attributes: 0,
            last_offset_delta: 2,
            first_timestamp: 1234567890000,
            max_timestamp: 1234567890000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![
                Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 0,
                    key: Some(Bytes::from("key1")),
                    value: Bytes::from("value1"),
                    headers: vec![],
                },
                Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 1,
                    key: None,
                    value: Bytes::from("value2"),
                    headers: vec![RecordHeader {
                        key: "header1".to_string(),
                        value: Some(Bytes::from("hval1")),
                    }],
                },
                Record {
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 2,
                    key: Some(Bytes::from("key3")),
                    value: Bytes::from("value3"),
                    headers: vec![],
                },
            ],
        };

        // Encode
        let encoded = encode_record_batch(&batch);

        // Decode
        let mut cursor = Cursor::new(encoded.as_ref());
        let decoded = decode_record_batch(&mut cursor).unwrap();

        // Verify
        assert_eq!(decoded.base_offset, batch.base_offset);
        assert_eq!(decoded.last_offset_delta, batch.last_offset_delta);
        assert_eq!(decoded.first_timestamp, batch.first_timestamp);
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(
            decoded.records[0].key.as_ref().map(|b| b.as_ref()),
            Some(b"key1".as_slice())
        );
        assert_eq!(decoded.records[0].value.as_ref(), b"value1");
        assert_eq!(decoded.records[1].key, None);
        assert_eq!(decoded.records[1].value.as_ref(), b"value2");
        assert_eq!(decoded.records[1].headers.len(), 1);
        assert_eq!(decoded.records[1].headers[0].key, "header1");
    }

    #[test]
    fn test_empty_record_batch() {
        let batch = RecordBatch::default();
        let encoded = encode_record_batch(&batch);

        let mut cursor = Cursor::new(encoded.as_ref());
        let decoded = decode_record_batch(&mut cursor).unwrap();

        assert_eq!(decoded.base_offset, 0);
        assert_eq!(decoded.records.len(), 0);
    }

    #[test]
    fn test_create_record_batch() {
        let messages = vec![
            (Some(Bytes::from("k1")), Bytes::from("v1")),
            (None, Bytes::from("v2")),
        ];

        let batch = create_record_batch(50, 1000000, messages);

        assert_eq!(batch.base_offset, 50);
        assert_eq!(batch.first_timestamp, 1000000);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.last_offset_delta, 1);
        assert_eq!(batch.records[0].offset_delta, 0);
        assert_eq!(batch.records[1].offset_delta, 1);
    }
}
