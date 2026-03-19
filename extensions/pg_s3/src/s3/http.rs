//! HTTP/1.1 request parser and response builder
//!
//! Manual HTTP parsing over raw TCP, matching the pg_kafka/pg_mqtt approach.

use std::collections::HashMap;
use std::fmt;

/// HTTP method
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Method {
    Get,
    Put,
    Delete,
    Head,
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Method::Get => write!(f, "GET"),
            Method::Put => write!(f, "PUT"),
            Method::Delete => write!(f, "DELETE"),
            Method::Head => write!(f, "HEAD"),
        }
    }
}

/// Parsed HTTP request
pub struct HttpRequest {
    pub method: Method,
    pub path: String,
    pub query_params: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

/// HTTP response
pub struct HttpResponse {
    pub status: u16,
    pub status_text: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn xml(status: u16, body: &str) -> Self {
        let body_bytes = body.as_bytes().to_vec();
        Self {
            status,
            status_text: status_text(status),
            headers: vec![
                ("Content-Type".into(), "application/xml".into()),
                ("Content-Length".into(), body_bytes.len().to_string()),
            ],
            body: body_bytes,
        }
    }

    pub fn binary(
        status: u16,
        content_type: &str,
        body: Vec<u8>,
        extra_headers: Vec<(String, String)>,
    ) -> Self {
        let mut headers = vec![
            ("Content-Type".into(), content_type.to_string()),
            ("Content-Length".into(), body.len().to_string()),
        ];
        headers.extend(extra_headers);
        Self {
            status,
            status_text: status_text(status),
            headers,
            body,
        }
    }

    pub fn no_content() -> Self {
        Self {
            status: 204,
            status_text: "No Content",
            headers: vec![("Content-Length".into(), "0".into())],
            body: Vec::new(),
        }
    }

    pub fn ok_empty() -> Self {
        Self {
            status: 200,
            status_text: "OK",
            headers: vec![("Content-Length".into(), "0".into())],
            body: Vec::new(),
        }
    }

    pub fn ok_with_headers(headers: Vec<(String, String)>) -> Self {
        let has_content_length = headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("Content-Length"));
        let mut all_headers = Vec::with_capacity(headers.len() + 1);
        if !has_content_length {
            all_headers.push(("Content-Length".into(), "0".into()));
        }
        all_headers.extend(headers);
        Self {
            status: 200,
            status_text: "OK",
            headers: all_headers,
            body: Vec::new(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.body.len());
        out.extend_from_slice(
            format!("HTTP/1.1 {} {}\r\n", self.status, self.status_text).as_bytes(),
        );
        out.extend_from_slice(b"Server: pg_s3\r\n");
        for (k, v) in &self.headers {
            out.extend_from_slice(format!("{}: {}\r\n", k, v).as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        out
    }
}

/// Parse error
#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    InvalidMethod(String),
    InvalidRequest,
    BodyTooLarge,
}

/// Parse an HTTP/1.1 request from a buffer.
/// Returns the parsed request and the number of bytes consumed.
pub fn parse_request(buf: &[u8], max_body_size: usize) -> Result<(HttpRequest, usize), ParseError> {
    // Find end of headers
    let header_end = find_header_end(buf).ok_or(ParseError::Incomplete)?;
    let header_bytes = &buf[..header_end];
    let header_str = std::str::from_utf8(header_bytes).map_err(|_| ParseError::InvalidRequest)?;

    let mut lines = header_str.lines();

    // Parse request line
    let request_line = lines.next().ok_or(ParseError::InvalidRequest)?;
    let mut parts = request_line.split_whitespace();
    let method_str = parts.next().ok_or(ParseError::InvalidRequest)?;
    let uri = parts.next().ok_or(ParseError::InvalidRequest)?;
    // HTTP version is optional to parse

    let method = match method_str {
        "GET" => Method::Get,
        "PUT" => Method::Put,
        "DELETE" => Method::Delete,
        "HEAD" => Method::Head,
        other => return Err(ParseError::InvalidMethod(other.to_string())),
    };

    // Split path and query string
    let (path, query_params) = parse_uri(uri);

    // Parse headers
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }

    // Read body based on Content-Length
    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    if content_length > max_body_size {
        return Err(ParseError::BodyTooLarge);
    }

    let total_size = header_end + 4 + content_length; // +4 for \r\n\r\n
    if buf.len() < total_size {
        return Err(ParseError::Incomplete);
    }

    let body = buf[header_end + 4..total_size].to_vec();

    Ok((
        HttpRequest {
            method,
            path,
            query_params,
            headers,
            body,
        },
        total_size,
    ))
}

/// Find the position of \r\n\r\n (returns position of the first \r)
fn find_header_end(buf: &[u8]) -> Option<usize> {
    (0..buf.len().saturating_sub(3)).find(|&i| &buf[i..i + 4] == b"\r\n\r\n")
}

/// Parse URI into path and query parameters
fn parse_uri(uri: &str) -> (String, HashMap<String, String>) {
    let mut params = HashMap::new();
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));

    // URL-decode the path
    let decoded_path = url_decode(path);

    if !query.is_empty() {
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                params.insert(url_decode(k), url_decode(v));
            } else {
                params.insert(url_decode(pair), String::new());
            }
        }
    }

    (decoded_path, params)
}

/// Simple URL decoding (percent-decoding)
fn url_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            result.push(b' ');
            i += 1;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).into_owned()
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

/// Parse the S3 path into (bucket, key) components.
/// "/" → (None, None)
/// "/bucket" → (Some("bucket"), None)
/// "/bucket/key/path" → (Some("bucket"), Some("key/path"))
pub fn parse_s3_path(path: &str) -> (Option<&str>, Option<&str>) {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return (None, None);
    }

    match trimmed.split_once('/') {
        Some((bucket, key)) if !key.is_empty() => (Some(bucket), Some(key)),
        _ => (Some(trimmed), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_path_root() {
        assert_eq!(parse_s3_path("/"), (None, None));
    }

    #[test]
    fn test_parse_s3_path_bucket() {
        assert_eq!(parse_s3_path("/my-bucket"), (Some("my-bucket"), None));
    }

    #[test]
    fn test_parse_s3_path_bucket_trailing_slash() {
        assert_eq!(parse_s3_path("/my-bucket/"), (Some("my-bucket"), None));
    }

    #[test]
    fn test_parse_s3_path_key() {
        assert_eq!(
            parse_s3_path("/my-bucket/path/to/file.txt"),
            (Some("my-bucket"), Some("path/to/file.txt"))
        );
    }

    #[test]
    fn test_parse_uri_simple() {
        let (path, params) = parse_uri("/my-bucket");
        assert_eq!(path, "/my-bucket");
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_uri_with_query() {
        let (path, params) = parse_uri("/my-bucket?list-type=2&prefix=docs/");
        assert_eq!(path, "/my-bucket");
        assert_eq!(params.get("list-type"), Some(&"2".to_string()));
        assert_eq!(params.get("prefix"), Some(&"docs/".to_string()));
    }

    #[test]
    fn test_url_decode() {
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("path/to/file"), "path/to/file");
        assert_eq!(url_decode("a%2Fb"), "a/b");
    }

    #[test]
    fn test_parse_request_get() {
        let raw = b"GET /my-bucket HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let (req, consumed) = parse_request(raw, 1024).unwrap();
        assert_eq!(req.method, Method::Get);
        assert_eq!(req.path, "/my-bucket");
        assert!(req.body.is_empty());
        assert_eq!(consumed, raw.len());
    }

    #[test]
    fn test_parse_request_put_with_body() {
        let raw = b"PUT /bucket/key HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello";
        let (req, consumed) = parse_request(raw, 1024).unwrap();
        assert_eq!(req.method, Method::Put);
        assert_eq!(req.path, "/bucket/key");
        assert_eq!(req.body, b"hello");
        assert_eq!(consumed, raw.len());
    }

    #[test]
    fn test_parse_request_incomplete() {
        let raw = b"GET /bucket HTTP/1.1\r\nHost: loc";
        assert!(matches!(
            parse_request(raw, 1024),
            Err(ParseError::Incomplete)
        ));
    }

    #[test]
    fn test_response_to_bytes() {
        let resp = HttpResponse::xml(200, "<Ok/>");
        let bytes = resp.to_bytes();
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Content-Type: application/xml\r\n"));
        assert!(s.ends_with("<Ok/>"));
    }
}
