//! HTTP/1.1 request parser and response builder.
//!
//! Hand-rolled over raw TCP, lifted from
//! [`extensions/pg_s3/src/s3/http.rs`](../../pg_s3/src/s3/http.rs) with the
//! S3 path-parsing helpers stripped. Same shape so future maintenance
//! crossover is cheap.

use std::collections::HashMap;
use std::fmt;

/// HTTP method — WMS only uses GET / HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Other(String),
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Method::Get => write!(f, "GET"),
            Method::Head => write!(f, "HEAD"),
            Method::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Parsed HTTP request.
#[allow(dead_code)] // `path` + `headers` are forward-compat surface.
pub struct HttpRequest {
    pub method: Method,
    pub path: String,
    /// Query params are normalised to lowercase keys so WMS arguments
    /// (`SERVICE`, `service`, `Service`) all hash to the same slot —
    /// per the OGC spec, parameter names are case-insensitive.
    pub query_params: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}

/// HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub status_text: &'static str,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn xml(status: u16, body: String) -> Self {
        let body_bytes = body.into_bytes();
        Self {
            status,
            status_text: status_text(status),
            headers: vec![
                (
                    "Content-Type".into(),
                    "application/xml; charset=utf-8".into(),
                ),
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

    #[allow(dead_code)] // forward-compat: GetFeatureInfo + WMS-T descriptors will use text/plain.
    pub fn text(status: u16, body: &str) -> Self {
        let body_bytes = body.as_bytes().to_vec();
        Self {
            status,
            status_text: status_text(status),
            headers: vec![
                ("Content-Type".into(), "text/plain; charset=utf-8".into()),
                ("Content-Length".into(), body_bytes.len().to_string()),
            ],
            body: body_bytes,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.body.len());
        out.extend_from_slice(
            format!("HTTP/1.1 {} {}\r\n", self.status, self.status_text).as_bytes(),
        );
        out.extend_from_slice(b"Server: pg_xarray\r\n");
        for (k, v) in &self.headers {
            out.extend_from_slice(format!("{}: {}\r\n", k, v).as_bytes());
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&self.body);
        out
    }
}

/// Parse error.
#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    InvalidRequest,
}

/// Parse an HTTP/1.1 request. Returns the parsed request and the
/// number of bytes consumed (including any body — WMS requests are
/// header-only GETs so body is always empty).
pub fn parse_request(buf: &[u8]) -> Result<(HttpRequest, usize), ParseError> {
    let header_end = find_header_end(buf).ok_or(ParseError::Incomplete)?;
    let header_bytes = &buf[..header_end];
    let header_str = std::str::from_utf8(header_bytes).map_err(|_| ParseError::InvalidRequest)?;

    let mut lines = header_str.lines();
    let request_line = lines.next().ok_or(ParseError::InvalidRequest)?;
    let mut parts = request_line.split_whitespace();
    let method_str = parts.next().ok_or(ParseError::InvalidRequest)?;
    let uri = parts.next().ok_or(ParseError::InvalidRequest)?;

    let method = match method_str {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        other => Method::Other(other.to_string()),
    };

    let (path, query_params) = parse_uri(uri);

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_lowercase(), value.trim().to_string());
        }
    }

    Ok((
        HttpRequest {
            method,
            path,
            query_params,
            headers,
        },
        header_end + 4, // +4 for \r\n\r\n
    ))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    (0..buf.len().saturating_sub(3)).find(|&i| &buf[i..i + 4] == b"\r\n\r\n")
}

fn parse_uri(uri: &str) -> (String, HashMap<String, String>) {
    let mut params = HashMap::new();
    let (path, query) = uri.split_once('?').unwrap_or((uri, ""));
    let decoded_path = url_decode(path);
    if !query.is_empty() {
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                // Lowercase the key so WMS param matching is case-insensitive.
                params.insert(url_decode(k).to_lowercase(), url_decode(v));
            } else {
                params.insert(url_decode(pair).to_lowercase(), String::new());
            }
        }
    }
    (decoded_path, params)
}

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
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_get() {
        let req =
            b"GET /wms?service=WMS&request=GetCapabilities HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let (parsed, n) = parse_request(req).unwrap();
        assert_eq!(parsed.method, Method::Get);
        assert_eq!(parsed.path, "/wms");
        assert_eq!(parsed.query_params.get("service").unwrap(), "WMS");
        assert_eq!(
            parsed.query_params.get("request").unwrap(),
            "GetCapabilities"
        );
        assert_eq!(parsed.headers.get("host").unwrap(), "localhost");
        assert_eq!(n, req.len());
    }

    #[test]
    fn parse_request_query_keys_lowercased() {
        // OGC: parameter names are case-insensitive.
        let req = b"GET /wms?SERVICE=WMS&Request=GetMap HTTP/1.1\r\nHost: x\r\n\r\n";
        let (parsed, _) = parse_request(req).unwrap();
        assert!(parsed.query_params.contains_key("service"));
        assert!(parsed.query_params.contains_key("request"));
        assert_eq!(parsed.query_params.get("service").unwrap(), "WMS");
    }

    #[test]
    fn parse_incomplete_request() {
        // No \r\n\r\n yet.
        let req = b"GET /wms HTTP/1.1\r\nHost: localhost\r\n";
        assert!(matches!(parse_request(req), Err(ParseError::Incomplete)));
    }

    #[test]
    fn url_decode_percent_and_plus() {
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("a+b"), "a b");
        assert_eq!(url_decode("WATER%20DEPTH"), "WATER DEPTH");
    }

    #[test]
    fn response_to_bytes_includes_server_header() {
        let r = HttpResponse::text(200, "hi");
        let bytes = r.to_bytes();
        let s = std::str::from_utf8(&bytes).unwrap();
        assert!(s.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(s.contains("Server: pg_xarray\r\n"));
        assert!(s.contains("Content-Type: text/plain"));
        assert!(s.ends_with("hi"));
    }
}
