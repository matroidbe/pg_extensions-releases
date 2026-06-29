//! WMS 1.3.0 request router.
//!
//! Dispatches `?SERVICE=WMS&REQUEST=<op>&...` requests to the right
//! handler. v1 supports `GetCapabilities` (XML manifest of layers)
//! and `GetMap` (PNG tile of a layer at a time/bbox/viewport).
//!
//! OGC exceptions are returned as `ServiceExceptionReport` XML per
//! the spec — clients (QGIS, ArcGIS, OpenLayers) parse the
//! `<ServiceException code="…">` element and surface it as a user-
//! facing message.

pub mod capabilities;
pub mod get_map;

use crate::server::http::{HttpRequest, HttpResponse, Method};

/// Default `Cache-Control` header for GetMap responses — 60 s lets a
/// reverse proxy / browser absorb steady-state read traffic without
/// hitting Postgres. Configurable via the `pg_xarray.wms_cache_seconds`
/// GUC (see `server::mod`).
#[allow(dead_code)] // referenced from external docs / test fixtures.
pub const DEFAULT_CACHE_SECONDS: u32 = 60;

/// Dispatch a parsed HTTP request to the WMS handler. Returns the
/// HTTP response — including OGC error XML for malformed requests.
///
/// **Called from the bgworker's main thread** — SPI is valid here.
pub fn handle_request(req: &HttpRequest, cache_seconds: u32) -> HttpResponse {
    if !matches!(req.method, Method::Get | Method::Head) {
        return ogc_exception(
            405,
            "InvalidRequest",
            &format!("WMS only supports GET/HEAD, got {}", req.method),
        );
    }

    let service = req
        .query_params
        .get("service")
        .map(|s| s.as_str())
        .unwrap_or("");
    if !service.eq_ignore_ascii_case("WMS") {
        return ogc_exception(
            400,
            "InvalidRequest",
            "missing or unsupported SERVICE parameter (must be WMS)",
        );
    }

    let request = req
        .query_params
        .get("request")
        .map(|s| s.as_str())
        .unwrap_or("");
    match request.to_ascii_lowercase().as_str() {
        "getcapabilities" => match capabilities::build(&req.query_params) {
            Ok(xml) => HttpResponse::xml(200, xml),
            Err(msg) => ogc_exception(500, "InternalError", &msg),
        },
        "getmap" => get_map::handle(&req.query_params, cache_seconds),
        "" => ogc_exception(
            400,
            "InvalidRequest",
            "missing REQUEST parameter (expected GetCapabilities or GetMap)",
        ),
        other => ogc_exception(
            400,
            "OperationNotSupported",
            &format!("REQUEST={other} not supported (v1 covers GetCapabilities and GetMap)"),
        ),
    }
}

/// Build an OGC `ServiceExceptionReport` XML body and wrap it in an
/// HTTP response. WMS clients parse the inner `<ServiceException>`
/// element to surface user-facing errors.
pub fn ogc_exception(status: u16, code: &str, message: &str) -> HttpResponse {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ServiceExceptionReport version=\"1.3.0\" \
           xmlns=\"http://www.opengis.net/ogc\">\n  \
           <ServiceException code=\"{}\">{}</ServiceException>\n\
         </ServiceExceptionReport>\n",
        xml_escape(code),
        xml_escape(message),
    );
    HttpResponse::xml(status, body)
}

/// Minimal XML attribute/text escape — enough for the OGC error path.
pub fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_request_returns_ogc_exception() {
        let mut params = std::collections::HashMap::new();
        params.insert("service".into(), "WMS".into());
        params.insert("request".into(), "GetFoo".into());
        let req = HttpRequest {
            method: Method::Get,
            path: "/wms".into(),
            query_params: params,
            headers: Default::default(),
        };
        let resp = handle_request(&req, 60);
        assert_eq!(resp.status, 400);
        let body = std::str::from_utf8(&resp.body).unwrap();
        assert!(body.contains("<ServiceException"));
        assert!(body.contains("OperationNotSupported"));
    }

    #[test]
    fn missing_service_param_is_400() {
        let req = HttpRequest {
            method: Method::Get,
            path: "/wms".into(),
            query_params: Default::default(),
            headers: Default::default(),
        };
        let resp = handle_request(&req, 60);
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn xml_escape_handles_punctuation() {
        assert_eq!(xml_escape("a < b & c"), "a &lt; b &amp; c");
    }
}
