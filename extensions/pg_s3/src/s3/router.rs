//! S3 request router
//!
//! Routes HTTP requests to the appropriate S3 operation handler
//! based on method and path.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::s3::error::S3Error;
use crate::s3::handlers;
use crate::s3::http::{parse_request, parse_s3_path, HttpRequest, HttpResponse, Method};
use crate::storage::spi_client::S3StorageClient;

/// Maximum request size (headers + body): 1 GB
const MAX_REQUEST_SIZE: usize = 1024 * 1024 * 1024;
/// Read buffer size: 64 KB
const READ_BUF_SIZE: usize = 64 * 1024;

/// Handle a single HTTP connection (may serve multiple requests via keep-alive)
pub async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<S3StorageClient>,
    data_dir: String,
    _worker_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = Vec::with_capacity(READ_BUF_SIZE);

    loop {
        // Read data from socket
        let mut tmp = vec![0u8; READ_BUF_SIZE];
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break; // Connection closed
        }
        buf.extend_from_slice(&tmp[..n]);

        // Try to parse a complete request
        loop {
            if buf.len() > MAX_REQUEST_SIZE {
                let resp = S3Error::EntityTooLarge.to_response();
                stream.write_all(&resp.to_bytes()).await?;
                return Ok(());
            }

            match parse_request(&buf, MAX_REQUEST_SIZE) {
                Ok((request, consumed)) => {
                    let response = route_request(&request, &storage, &data_dir).await;
                    stream.write_all(&response.to_bytes()).await?;

                    // Remove consumed bytes
                    buf.drain(..consumed);

                    // Check for connection close
                    let close = request
                        .headers
                        .get("connection")
                        .map(|v| v.eq_ignore_ascii_case("close"))
                        .unwrap_or(false);

                    if close {
                        return Ok(());
                    }

                    // If buffer is empty, break inner loop to read more
                    if buf.is_empty() {
                        break;
                    }
                    // Otherwise try to parse another pipelined request
                }
                Err(crate::s3::http::ParseError::Incomplete) => {
                    break; // Need more data
                }
                Err(_) => {
                    let resp = HttpResponse::xml(
                        400,
                        &crate::s3::xml::error_xml(
                            "InvalidRequest",
                            "Could not parse HTTP request",
                            "/",
                        ),
                    );
                    stream.write_all(&resp.to_bytes()).await?;
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

/// Route a parsed request to the appropriate handler
async fn route_request(
    req: &HttpRequest,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    let (bucket, key) = parse_s3_path(&req.path);

    match (&req.method, bucket, key) {
        // Root operations
        (Method::Get, None, None) => handlers::handle_list_buckets(storage).await,

        // Bucket operations
        (Method::Put, Some(b), None) => handlers::handle_create_bucket(b, storage, data_dir).await,
        (Method::Delete, Some(b), None) => {
            handlers::handle_delete_bucket(b, storage, data_dir).await
        }
        (Method::Head, Some(b), None) => handlers::handle_head_bucket(b, storage).await,
        (Method::Get, Some(b), None) => {
            handlers::handle_list_objects(b, &req.query_params, storage).await
        }

        // Object operations
        (Method::Put, Some(b), Some(k)) => {
            let content_type = req
                .headers
                .get("content-type")
                .map(|s| s.as_str())
                .unwrap_or("application/octet-stream");

            // Extract x-amz-meta-* headers
            let metadata_headers: Vec<(String, String)> = req
                .headers
                .iter()
                .filter(|(k, _)| k.starts_with("x-amz-meta-"))
                .map(|(k, v)| {
                    let meta_key = k.strip_prefix("x-amz-meta-").unwrap_or(k);
                    (meta_key.to_string(), v.clone())
                })
                .collect();

            handlers::handle_put_object(
                b,
                k,
                &req.body,
                content_type,
                metadata_headers,
                storage,
                data_dir,
            )
            .await
        }
        (Method::Get, Some(b), Some(k)) => {
            handlers::handle_get_object(b, k, storage, data_dir).await
        }
        (Method::Delete, Some(b), Some(k)) => {
            handlers::handle_delete_object(b, k, storage, data_dir).await
        }
        (Method::Head, Some(b), Some(k)) => handlers::handle_head_object(b, k, storage).await,

        // Method not allowed
        _ => S3Error::MethodNotAllowed.to_response(),
    }
}
