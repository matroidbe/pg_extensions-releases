//! WMS 1.3.0 `GetMap` request handler.
//!
//! Parses the standard parameters (`LAYERS`, `BBOX`, `WIDTH`, `HEIGHT`,
//! `CRS`, `TIME`, `STYLES`, `FORMAT`) and dispatches to the raster
//! pipeline. Three vendor parameters, named as in ncWMS / THREDDS, set
//! the colour scale:
//!
//! * `COLORSCALERANGE=min,max`: the range the colormap spans (default:
//!   the variable's catalog `valid_min`/`valid_max`, else the data's);
//! * `BELOWMINCOLOR`, `ABOVEMAXCOLOR`: `extend` (default, the end colour)
//!   or `transparent`. `COLORSCALERANGE=0.01,25&BELOWMINCOLOR=transparent`
//!   on a water depth draws only the wet cells, so a base map shows
//!   through the dry ground.
//!
//! Returns the PNG bytes as the response body, with
//! `Cache-Control: max-age=N` so a reverse proxy (nginx, Varnish,
//! CDN) can absorb steady-state read traffic.

use std::collections::HashMap;

use pgrx::prelude::*;

use crate::raster::{self, OutOfRange};
use crate::server::http::HttpResponse;

use super::ogc_exception;

/// Handle a GetMap request. Called from the bgworker's main thread —
/// SPI is valid.
pub fn handle(params: &HashMap<String, String>, cache_seconds: u32) -> HttpResponse {
    let parsed = match parse(params) {
        Ok(p) => p,
        Err((code, msg)) => return ogc_exception(400, code, &msg),
    };

    // CRS check: v1 serves only the layer's native CRS. If the client
    // asks for a different EPSG, fail clearly instead of silently
    // rendering wrong coordinates.
    let layer_srid = match lookup_layer_srid(&parsed.dataset, &parsed.variable) {
        Some(s) => s,
        None => {
            return ogc_exception(
                404,
                "LayerNotDefined",
                &format!(
                    "layer '{}:{}' is not in the catalog",
                    parsed.dataset, parsed.variable
                ),
            );
        }
    };
    if let Some(requested_srid) = parsed.crs_srid {
        if requested_srid != layer_srid {
            return ogc_exception(
                400,
                "InvalidCRS",
                &format!(
                    "layer '{}:{}' is in EPSG:{}; CRS reprojection is out of scope for v1 \
                     (requested EPSG:{})",
                    parsed.dataset, parsed.variable, layer_srid, requested_srid
                ),
            );
        }
    }

    // Build the bbox WKT in the layer's native CRS.
    let bbox_wkt = format!(
        "POLYGON(({mnx} {mny}, {mxx} {mny}, {mxx} {mxy}, {mnx} {mxy}, {mnx} {mny}))",
        mnx = parsed.min_x,
        mny = parsed.min_y,
        mxx = parsed.max_x,
        mxy = parsed.max_y,
    );

    // Dispatch to the existing raster impl. pgrx::error! inside this
    // call site would panic the worker; instead we PgTry-style catch
    // by pre-validating above and letting the impl produce empty if
    // there's nothing to render.
    let bytes = match catch_render(
        &parsed.dataset,
        &parsed.variable,
        parsed.time,
        &bbox_wkt,
        parsed.width,
        parsed.height,
        &parsed.style,
        parsed.render_options(),
    ) {
        Ok(b) => b,
        Err(msg) => return ogc_exception(500, "InternalError", &msg),
    };

    HttpResponse::binary(
        200,
        "image/png",
        bytes,
        vec![("Cache-Control".into(), format!("max-age={cache_seconds}"))],
    )
}

/// Call the raster impl. v1 accepts that a `pgrx::error!` inside the
/// impl will longjmp out of this stack — the bgworker's tokio runtime
/// survives because each connection runs in its own `spawn`ed task
/// and PG's error recovery resets the SPI state on every catch_unwind
/// boundary. The pre-validation in `parse` + `lookup_layer_srid` above
/// catches the common case (missing layer, bad bbox) before reaching
/// this site.
fn catch_render(
    dataset: &str,
    variable: &str,
    at_time: Option<TimestampWithTimeZone>,
    bbox_wkt: &str,
    width: i32,
    height: i32,
    style: &str,
    options: pgrx::JsonB,
) -> Result<Vec<u8>, String> {
    Ok(raster::xarray_to_png_impl(
        dataset,
        variable,
        at_time,
        Some(bbox_wkt),
        width,
        height,
        style,
        Some(options),
    ))
}

/// Parsed + validated WMS GetMap params.
struct GetMapParams {
    dataset: String,
    variable: String,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    width: i32,
    height: i32,
    crs_srid: Option<i32>,
    time: Option<TimestampWithTimeZone>,
    style: String,
    color_range: Option<(f64, f64)>,
    below: OutOfRange,
    above: OutOfRange,
}

impl GetMapParams {
    /// The `pgx.xarray_to_png` options these parameters stand for.
    fn render_options(&self) -> pgrx::JsonB {
        let mode = |m: OutOfRange| match m {
            OutOfRange::Extend => "extend",
            OutOfRange::Transparent => "transparent",
        };
        let mut o = serde_json::json!({
            "below_min": mode(self.below),
            "above_max": mode(self.above),
        });
        if let Some((lo, hi)) = self.color_range {
            o["vmin"] = lo.into();
            o["vmax"] = hi.into();
        }
        pgrx::JsonB(o)
    }
}

fn parse(params: &HashMap<String, String>) -> Result<GetMapParams, (&'static str, String)> {
    let layers = params
        .get("layers")
        .ok_or(("MissingParameterValue", "LAYERS is required".to_string()))?;
    if layers.is_empty() {
        return Err(("MissingParameterValue", "LAYERS is empty".into()));
    }
    // v1: single layer per GetMap call (no multi-layer composition).
    let (dataset, variable) = layers.split_once(':').ok_or((
        "InvalidParameterValue",
        format!("LAYERS='{layers}' must be '<dataset>:<variable>' (GeoServer convention)"),
    ))?;

    let bbox = params
        .get("bbox")
        .ok_or(("MissingParameterValue", "BBOX is required".to_string()))?;
    let coords: Vec<f64> = bbox
        .split(',')
        .map(|s| s.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ("InvalidParameterValue", format!("BBOX parse: {e}")))?;
    if coords.len() != 4 {
        return Err((
            "InvalidParameterValue",
            format!(
                "BBOX must have 4 comma-separated values, got {}",
                coords.len()
            ),
        ));
    }
    let (min_x, min_y, max_x, max_y) = (coords[0], coords[1], coords[2], coords[3]);
    if !(max_x > min_x && max_y > min_y) {
        return Err((
            "InvalidParameterValue",
            format!("BBOX is degenerate ({min_x},{min_y} → {max_x},{max_y})"),
        ));
    }

    let width = params
        .get("width")
        .ok_or(("MissingParameterValue", "WIDTH is required".to_string()))?
        .parse::<i32>()
        .map_err(|_| ("InvalidParameterValue", "WIDTH not an integer".to_string()))?;
    let height = params
        .get("height")
        .ok_or(("MissingParameterValue", "HEIGHT is required".to_string()))?
        .parse::<i32>()
        .map_err(|_| ("InvalidParameterValue", "HEIGHT not an integer".to_string()))?;
    if width <= 0 || height <= 0 || width > 8192 || height > 8192 {
        return Err((
            "InvalidParameterValue",
            format!("WIDTH/HEIGHT must be in (0, 8192], got {width}x{height}"),
        ));
    }

    let crs_srid = params
        .get("crs")
        .or_else(|| params.get("srs")) // WMS 1.1.1 alias
        .and_then(|s| parse_epsg(s));

    let time = params
        .get("time")
        .filter(|s| !s.is_empty())
        .map(|s| iso_to_pg_timestamp(s))
        .transpose()
        .map_err(|e| ("InvalidParameterValue", format!("TIME parse: {e}")))?;

    let style = params
        .get("styles")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "viridis".to_string());

    let color_range = params
        .get("colorscalerange")
        .filter(|s| !s.is_empty())
        .map(|s| {
            let bad = || {
                (
                    "InvalidParameterValue",
                    format!("COLORSCALERANGE='{s}' must be 'min,max' with min < max"),
                )
            };
            let (lo, hi) = s.split_once(',').ok_or_else(bad)?;
            let lo: f64 = lo.trim().parse().map_err(|_| bad())?;
            let hi: f64 = hi.trim().parse().map_err(|_| bad())?;
            if lo.is_finite() && hi.is_finite() && lo < hi {
                Ok((lo, hi))
            } else {
                Err(bad())
            }
        })
        .transpose()?;
    let out_of_range = |key: &str, name: &str| {
        params
            .get(key)
            .filter(|s| !s.is_empty())
            .map(|s| {
                OutOfRange::parse(s).ok_or((
                    "InvalidParameterValue",
                    format!("{name}='{s}' must be 'extend' or 'transparent'"),
                ))
            })
            .transpose()
            .map(|m| m.unwrap_or(OutOfRange::Extend))
    };
    let below = out_of_range("belowmincolor", "BELOWMINCOLOR")?;
    let above = out_of_range("abovemaxcolor", "ABOVEMAXCOLOR")?;

    Ok(GetMapParams {
        dataset: dataset.to_string(),
        variable: variable.to_string(),
        min_x,
        min_y,
        max_x,
        max_y,
        width,
        height,
        crs_srid,
        time,
        style,
        color_range,
        below,
        above,
    })
}

fn parse_epsg(s: &str) -> Option<i32> {
    let upper = s.trim().to_ascii_uppercase();
    let stripped = upper.strip_prefix("EPSG:")?;
    stripped.parse::<i32>().ok()
}

/// Parse an ISO 8601 timestamp into a Postgres `timestamptz` via SPI.
/// Using PG's own parser avoids reimplementing the spec (handles
/// `2024-06-01T00:00:00Z`, `2024-06-01 00:00:00+00`, etc.).
fn iso_to_pg_timestamp(s: &str) -> Result<TimestampWithTimeZone, String> {
    let parsed: Result<Option<TimestampWithTimeZone>, spi::Error> = Spi::connect(|client| {
        let mut t = client.select("SELECT $1::timestamptz", Some(1), &[s.into()])?;
        match t.next() {
            Some(row) => Ok(row.get::<TimestampWithTimeZone>(1)?),
            None => Ok(None),
        }
    });
    match parsed {
        Ok(Some(t)) => Ok(t),
        Ok(None) => Err(format!("'{s}' parsed to NULL")),
        Err(e) => Err(format!("{e}")),
    }
}

/// Look up the SRID a layer is registered against — matches the
/// `enumerate_layers` query in `capabilities.rs`.
fn lookup_layer_srid(dataset: &str, variable: &str) -> Option<i32> {
    let sql = r#"
        SELECT COALESCE(v.srid, d.default_srid, 4326)
        FROM   pgx.chunks       c
        JOIN   pgx.variables    v  ON v.id = c.variable_id
        JOIN   pgx.datasets     d  ON d.id = v.dataset_id
        WHERE  d.name = $1
          AND  v.name = $2
          AND  c.mesh_version_id IS NOT NULL
        ORDER  BY c.id
        LIMIT  1
    "#;
    let res: Result<Option<i32>, spi::Error> = Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match t.next() {
            Some(row) => Ok(row.get::<i32>(1)?),
            None => Ok(None),
        }
    });
    res.ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raster::OutOfRange;

    fn params(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut p: HashMap<String, String> = [
            ("layers", "flood:WATER DEPTH"),
            ("bbox", "0,0,10,10"),
            ("width", "4"),
            ("height", "4"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        for (k, v) in extra {
            p.insert(k.to_string(), v.to_string());
        }
        p
    }

    #[test]
    fn colour_scale_defaults_to_the_catalog_and_extends() {
        let p = parse(&params(&[])).unwrap();
        assert_eq!(p.color_range, None);
        assert_eq!(p.below, OutOfRange::Extend);
        assert_eq!(p.above, OutOfRange::Extend);
    }

    #[test]
    fn ncwms_colour_scale_parameters() {
        let p = parse(&params(&[
            ("colorscalerange", "0.01,25"),
            ("belowmincolor", "transparent"),
            ("abovemaxcolor", "extend"),
        ]))
        .unwrap();
        assert_eq!(p.color_range, Some((0.01, 25.0)));
        assert_eq!(p.below, OutOfRange::Transparent);
        assert_eq!(p.above, OutOfRange::Extend);
        // case-insensitive values, as clients send them
        let p = parse(&params(&[("abovemaxcolor", "TRANSPARENT")])).unwrap();
        assert_eq!(p.above, OutOfRange::Transparent);
    }

    #[test]
    fn bad_colour_scale_parameters_are_rejected() {
        for (k, v) in [
            ("colorscalerange", "25"),
            ("colorscalerange", "a,b"),
            ("colorscalerange", "25,0"),
            ("colorscalerange", "1,1"),
            ("belowmincolor", "0xFF0000"),
        ] {
            let err = parse(&params(&[(k, v)])).err();
            assert_eq!(
                err.map(|e| e.0),
                Some("InvalidParameterValue"),
                "{k}={v} should be rejected"
            );
        }
    }
}
