//! Memory reader — synthetic deterministic cells for testing the full
//! catalog → reader → SRF flow without external file formats.
//!
//! Two URI shapes are supported:
//!   * `memory://grid?nx=N&ny=M&value=V` — `nx × ny` regular grid
//!     centered at (0, 0), all cells carrying `value`. Used by the
//!     structured `pgx.fetch` path.
//!   * `memory://nodes?ids=1,2,3&values=10.0,20.0,30.0` — one
//!     cell per (node_id, value) pair, with `lat`/`lon` left None.
//!     Used by the unstructured `pgx.fetch_mesh` path so the SRF can
//!     test the catalog-join-to-mesh path without a real format.

use super::{Cell, ChunkLocator, ChunkReader, CoordFilter};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct MemoryReader;

impl MemoryReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ChunkReader for MemoryReader {
    fn format_name(&self) -> &'static str {
        "memory"
    }

    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String> {
        // memory://nodes?ids=...&values=... — unstructured: one cell per
        // (node_id, value) pair, no lat/lon. Spatial pruning happens in
        // the SRF (via the JOIN to mesh_nodes/mesh_cells), so we don't
        // apply bbox here.
        if locator.uri.starts_with("memory://nodes") {
            return read_nodes_chunk(&locator.uri, filter);
        }
        let params = parse_memory_uri(&locator.uri)?;
        let nx = params
            .get("nx")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(3);
        let ny = params
            .get("ny")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(3);
        let value = params
            .get("value")
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(1.0);
        if nx <= 0 || ny <= 0 {
            return Err(format!(
                "memory: nx and ny must be positive (got {nx}, {ny})"
            ));
        }
        if nx > 10_000 || ny > 10_000 {
            return Err(format!("memory: grid too large ({nx} x {ny}); refusing"));
        }

        let mut cells = Vec::with_capacity((nx * ny) as usize);
        let half_x = (nx - 1) as f64 / 2.0;
        let half_y = (ny - 1) as f64 / 2.0;

        'outer: for j in 0..ny {
            for i in 0..nx {
                let lon = (i as f64) - half_x;
                let lat = (j as f64) - half_y;
                if let Some(b) = &filter.bbox_2d {
                    if !b.contains(lat, lon) {
                        continue;
                    }
                }
                cells.push(Cell {
                    lat: Some(lat),
                    lon: Some(lon),
                    level: None,
                    time: None,
                    node_id: None,
                    value,
                });
                if let Some(max) = filter.max_cells {
                    if cells.len() >= max {
                        break 'outer;
                    }
                }
            }
        }
        Ok(cells)
    }
}

/// Decode a `memory://nodes?ids=1,2,3&values=10.0,20.0,30.0` URI into
/// per-node `Cell`s. `ids.len()` must equal `values.len()`; mismatches
/// raise. Used only by the `pgx.fetch_mesh` test path — real
/// unstructured readers will emit the same `Cell { node_id, value, .. }`
/// shape from their own file format.
fn read_nodes_chunk(uri: &str, filter: &CoordFilter) -> Result<Vec<Cell>, String> {
    let params = parse_memory_uri(uri)?;
    let ids = params
        .get("ids")
        .ok_or_else(|| "memory://nodes: missing 'ids' parameter".to_string())?;
    let values = params
        .get("values")
        .ok_or_else(|| "memory://nodes: missing 'values' parameter".to_string())?;
    let ids: Vec<i64> = ids
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<i64>())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("memory://nodes: bad ids list: {e}"))?;
    let values: Vec<f64> = values
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<f64>())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("memory://nodes: bad values list: {e}"))?;
    if ids.len() != values.len() {
        return Err(format!(
            "memory://nodes: ids ({}) and values ({}) length mismatch",
            ids.len(),
            values.len()
        ));
    }
    let mut cells = Vec::with_capacity(ids.len());
    for (node_id, value) in ids.into_iter().zip(values.into_iter()) {
        cells.push(Cell {
            lat: None,
            lon: None,
            level: None,
            time: None,
            node_id: Some(node_id),
            value,
        });
        if let Some(max) = filter.max_cells {
            if cells.len() >= max {
                break;
            }
        }
    }
    Ok(cells)
}

fn parse_memory_uri(uri: &str) -> Result<HashMap<String, String>, String> {
    if !uri.starts_with("memory://") {
        return Err(format!("memory reader expects memory:// URIs, got '{uri}'"));
    }
    let after_scheme = &uri["memory://".len()..];
    let query = after_scheme.split_once('?').map(|(_, q)| q).unwrap_or("");
    let mut out = HashMap::new();
    for kv in query.split('&').filter(|s| !s.is_empty()) {
        match kv.split_once('=') {
            Some((k, v)) => {
                out.insert(k.to_string(), v.to_string());
            }
            None => {
                return Err(format!("memory uri: malformed parameter '{kv}'"));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Bbox2D;

    fn locator(uri: &str) -> ChunkLocator {
        ChunkLocator {
            uri: uri.to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn nodes_uri_emits_node_indexed_cells() {
        let r = MemoryReader::new();
        let cells = r
            .read_chunk(
                &locator("memory://nodes?ids=1,2,3&values=10.5,20.5,30.5"),
                &CoordFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(cells.len(), 3);
        assert_eq!(cells[0].node_id, Some(1));
        assert_eq!(cells[0].value, 10.5);
        assert_eq!(cells[2].node_id, Some(3));
        // No spatial coords on unstructured cells.
        assert!(cells.iter().all(|c| c.lat.is_none() && c.lon.is_none()));
    }

    #[tokio::test]
    async fn nodes_uri_rejects_length_mismatch() {
        let r = MemoryReader::new();
        let err = r
            .read_chunk(
                &locator("memory://nodes?ids=1,2,3&values=10.0"),
                &CoordFilter::default(),
            )
            .await
            .unwrap_err();
        assert!(err.contains("length mismatch"), "got: {err}");
    }

    #[tokio::test]
    async fn reads_3x3_default_grid() {
        let r = MemoryReader::new();
        let cells = r
            .read_chunk(&locator("memory://grid?value=42"), &CoordFilter::default())
            .await
            .unwrap();
        assert_eq!(cells.len(), 9);
        // Center cell is at (0, 0) with the configured value.
        let center = cells
            .iter()
            .find(|c| c.lat == Some(0.0) && c.lon == Some(0.0))
            .unwrap();
        assert_eq!(center.value, 42.0);
    }

    #[tokio::test]
    async fn reads_5x5_with_nx_ny() {
        let r = MemoryReader::new();
        let cells = r
            .read_chunk(
                &locator("memory://grid?nx=5&ny=5&value=1"),
                &CoordFilter::default(),
            )
            .await
            .unwrap();
        assert_eq!(cells.len(), 25);
        // Corners should be at +/-2 in both directions.
        assert!(cells
            .iter()
            .any(|c| c.lat == Some(-2.0) && c.lon == Some(-2.0)));
        assert!(cells
            .iter()
            .any(|c| c.lat == Some(2.0) && c.lon == Some(2.0)));
    }

    #[tokio::test]
    async fn bbox_filter_drops_cells_outside_box() {
        let r = MemoryReader::new();
        let filter = CoordFilter {
            bbox_2d: Some(Bbox2D {
                min_lat: 0.0,
                min_lon: 0.0,
                max_lat: 2.0,
                max_lon: 2.0,
            }),
            ..Default::default()
        };
        let cells = r
            .read_chunk(&locator("memory://grid?nx=5&ny=5&value=1"), &filter)
            .await
            .unwrap();
        // 5x5 grid centered at 0,0 → lat,lon ∈ {-2,-1,0,1,2}. Bbox keeps
        // 3 lat values (0,1,2) × 3 lon values (0,1,2) = 9.
        assert_eq!(cells.len(), 9);
        for c in &cells {
            assert!(c.lat.unwrap() >= 0.0 && c.lat.unwrap() <= 2.0);
            assert!(c.lon.unwrap() >= 0.0 && c.lon.unwrap() <= 2.0);
        }
    }

    #[tokio::test]
    async fn max_cells_caps_output() {
        let r = MemoryReader::new();
        let filter = CoordFilter {
            max_cells: Some(4),
            ..Default::default()
        };
        let cells = r
            .read_chunk(&locator("memory://grid?nx=10&ny=10"), &filter)
            .await
            .unwrap();
        assert_eq!(cells.len(), 4);
    }

    #[tokio::test]
    async fn rejects_oversized_grid() {
        let r = MemoryReader::new();
        let err = r
            .read_chunk(
                &locator("memory://grid?nx=20000&ny=20000"),
                &CoordFilter::default(),
            )
            .await
            .unwrap_err();
        assert!(err.contains("too large"));
    }

    #[tokio::test]
    async fn rejects_non_memory_uri() {
        let r = MemoryReader::new();
        let err = r
            .read_chunk(&locator("s3://bucket/key"), &CoordFilter::default())
            .await
            .unwrap_err();
        assert!(err.contains("memory://"));
    }

    #[test]
    fn parse_memory_uri_extracts_kv() {
        let p = parse_memory_uri("memory://grid?nx=4&ny=2&value=9.5").unwrap();
        assert_eq!(p.get("nx"), Some(&"4".to_string()));
        assert_eq!(p.get("ny"), Some(&"2".to_string()));
        assert_eq!(p.get("value"), Some(&"9.5".to_string()));
    }

    #[test]
    fn parse_memory_uri_no_query_is_empty_map() {
        let p = parse_memory_uri("memory://grid").unwrap();
        assert!(p.is_empty());
    }

    #[test]
    fn parse_memory_uri_rejects_other_scheme() {
        assert!(parse_memory_uri("file:///tmp/x").is_err());
    }
}
