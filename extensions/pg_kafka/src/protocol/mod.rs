//! Kafka protocol implementation
//!
//! This module handles encoding/decoding of Kafka wire protocol messages.

mod api;
mod codec;
mod records;
mod types;

pub use api::*;
pub use codec::*;
#[allow(unused_imports)]
pub use records::{
    create_record_batch, decode_record_batch, encode_record_batch, Record, RecordBatch,
    RecordHeader,
};
#[allow(unused_imports)]
pub use types::*;
