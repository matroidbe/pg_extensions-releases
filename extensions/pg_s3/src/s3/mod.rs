//! S3 REST API request handling
//!
//! Parses incoming HTTP requests and routes them to the appropriate
//! S3 operation (GetObject, PutObject, ListBuckets, etc.)

pub mod error;
pub mod handlers;
pub mod http;
mod router;
pub mod xml;

pub use router::handle_connection;
