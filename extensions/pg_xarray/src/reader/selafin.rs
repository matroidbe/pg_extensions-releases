//! SELAFIN / SERAFIN reader — the binary format TELEMAC and TELEMAC-
//! MASCARET write for unstructured-mesh hydraulic simulations
//! (water depth, free-surface elevation, flow velocity, sediment
//! transport, etc).
//!
//! Format reference: TELEMAC user manual "Serafin format" appendix.
//! Pure-Rust parser — there is no SELAFIN crate, but the file structure
//! is small and well-specified: a sequence of Fortran sequential
//! unformatted records (big-endian by convention). Every record is
//! `[u32_len_be][data of len bytes][u32_len_be]` — the suffix length
//! repeats the prefix so callers can step backwards too.
//!
//! File layout:
//!
//! ```text
//!   1) Title              80 bytes
//!   2) NBV1, NBV2         8 bytes (2 i32) — number of variables / quadratic vars
//!   3) Variable names     32 * NBV1 bytes (ASCII + units, space-padded)
//!   4) IPARAM             40 bytes (10 i32 — flags / coord system / etc)
//!   4b) DATE              24 bytes (6 i32 y/m/d/h/m/s) — only if IPARAM[9] == 1
//!   5) Mesh sizes         16 bytes (NELEM, NPOIN, NDP, IKLE_extra)
//!   6) IKLE               4 * NELEM * NDP bytes — connectivity (1-based)
//!   7) IPOBO              4 * NPOIN bytes — boundary point flags
//!   8) X                  4 * NPOIN f32 — node X coords
//!   9) Y                  4 * NPOIN f32 — node Y coords
//!  Then per-timestep:
//!   T) time               4 bytes (1 f32 — seconds since DATE)
//!   V1..VN) values        4 * NPOIN f32 — one record per variable
//! ```
//!
//! V1 scope: 2D triangular meshes (NDP=3) only. 3D prism meshes and
//! NDP=4 quads work for the catalog but the reader returns per-node
//! values exactly as in 2D — the SRF joins to `pgx.mesh_nodes` for
//! geometry regardless.

use super::{Cell, ChunkLocator, ChunkReader, CoordFilter};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use std::io::Read;

#[derive(Debug, Default)]
pub struct SelafinReader;

impl SelafinReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ChunkReader for SelafinReader {
    fn format_name(&self) -> &'static str {
        "selafin"
    }

    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String> {
        let path = local_path(&locator.uri)?;
        let chunk_key = locator
            .chunk_key
            .as_deref()
            .ok_or_else(|| "selafin: chunk_key is required".to_string())?
            .to_string();
        let parsed = parse_chunk_key(&chunk_key)?;
        let byte_offset = locator
            .byte_offset
            .ok_or_else(|| "selafin: byte_offset is required for chunk reads".to_string())?;
        let byte_length = locator
            .byte_length
            .ok_or_else(|| "selafin: byte_length is required for chunk reads".to_string())?;
        let filter = filter.clone();
        tokio::task::spawn_blocking(move || {
            decode_chunk(&path, byte_offset, byte_length, &parsed, &filter)
        })
        .await
        .map_err(|e| format!("selafin: blocking task join: {e}"))?
    }
}

/// Walker output — everything `register_file_impl` needs to populate
/// the catalog when it sees a SELAFIN file. The mesh side
/// (`mesh_nodes` + `mesh_cells`) is auto-created on first call; the
/// per-variable [`pgx_zarr_walker::VariableWalk`] carries one chunk
/// row per timestep.
pub struct SelafinWalk {
    /// Node geometries (1-based file-native IDs) — populates
    /// `pgx.mesh_nodes` once per mesh_version.
    pub nodes: Vec<SelafinNode>,
    /// Cells with node connectivity + centroid — populates
    /// `pgx.mesh_cells` once per mesh_version.
    pub cells: Vec<SelafinCell>,
    /// Per-(variable, timestep) chunk records pointing at the data
    /// slab in the source file.
    pub variable_walk: pgx_zarr_walker::VariableWalk,
    /// Bbox of the mesh's nodes — used to set `pgx.mesh_versions.extent`.
    pub mesh_extent_wkt: String,
    /// Earliest + latest timestamp the variable carries. The mesh's
    /// validity covers this range.
    pub time_from: Option<DateTime<Utc>>,
    pub time_to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SelafinNode {
    pub node_id: i64,
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone)]
pub struct SelafinCell {
    pub cell_id: i64,
    pub node_ids: Vec<i64>,
    pub centroid_x: f64,
    pub centroid_y: f64,
}

/// Top-level walk: parse the header, populate nodes + cells, and emit
/// one `ChunkRecord` per `(variable, timestep)` pair. Variables are
/// matched against `variable` case-insensitively after trimming.
pub fn walk_selafin(uri: &str, variable: &str) -> Result<SelafinWalk, String> {
    let path = local_path(uri)?;
    let bytes = std::fs::read(&path).map_err(|e| format!("selafin: read '{path}': {e}"))?;
    walk_bytes(&bytes, uri, variable)
}

// ----- core parser ------------------------------------------------------------

fn walk_bytes(bytes: &[u8], uri: &str, variable: &str) -> Result<SelafinWalk, String> {
    let mut p = Parser::new(bytes);
    // 1) title
    let _title = p.read_record_str(80)?;
    // 2) NBV1, NBV2
    let header_ints = p.read_record_i32(2)?;
    let nbv1 = header_ints[0] as usize;
    let _nbv2 = header_ints[1] as usize;
    if nbv1 == 0 {
        return Err("selafin: NBV1 == 0 (no variables in file)".into());
    }
    // 3) variable names — NBV1 * 32 chars
    let var_names_raw = p.read_record_bytes(32 * nbv1)?;
    let var_names: Vec<String> = (0..nbv1)
        .map(|i| {
            let s = &var_names_raw[i * 32..(i + 1) * 32];
            String::from_utf8_lossy(s).trim().to_string()
        })
        .collect();
    // 4) IPARAM (10 i32s)
    let iparam = p.read_record_i32(10)?;
    let has_date = iparam[9] == 1;
    let date = if has_date {
        let d = p.read_record_i32(6)?;
        Some(d)
    } else {
        None
    };
    let epoch = match date {
        Some(d) => Utc
            .with_ymd_and_hms(
                d[0],
                d[1] as u32,
                d[2] as u32,
                d[3] as u32,
                d[4] as u32,
                d[5] as u32,
            )
            .single()
            .unwrap_or_else(|| Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()),
        None => Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
    };
    // 5) NELEM, NPOIN, NDP, _ikle_extra
    let sizes = p.read_record_i32(4)?;
    let nelem = sizes[0] as usize;
    let npoin = sizes[1] as usize;
    let ndp = sizes[2] as usize;
    if ndp < 3 {
        return Err(format!("selafin: NDP={ndp} < 3 — not a triangle mesh"));
    }
    if npoin == 0 || nelem == 0 {
        return Err("selafin: NPOIN or NELEM == 0".into());
    }
    // 6) IKLE
    let ikle_flat = p.read_record_i32(nelem * ndp)?;
    // 7) IPOBO (we don't use it but must consume)
    let _ipobo = p.read_record_i32(npoin)?;
    // 8) X
    let xs = p.read_record_f32(npoin)?;
    // 9) Y
    let ys = p.read_record_f32(npoin)?;

    // Build nodes
    let nodes: Vec<SelafinNode> = (0..npoin)
        .map(|i| SelafinNode {
            node_id: (i + 1) as i64,
            x: xs[i] as f64,
            y: ys[i] as f64,
        })
        .collect();
    // Build cells — IKLE is row-major with `ndp` columns per cell.
    let cells: Vec<SelafinCell> = (0..nelem)
        .map(|ce| {
            let ids: Vec<i64> = (0..ndp).map(|k| ikle_flat[ce * ndp + k] as i64).collect();
            let (mut cx, mut cy) = (0.0, 0.0);
            for &nid in &ids {
                // SELAFIN IKLE is 1-based.
                if nid >= 1 && (nid as usize) <= npoin {
                    cx += nodes[(nid - 1) as usize].x;
                    cy += nodes[(nid - 1) as usize].y;
                }
            }
            let n = ids.len() as f64;
            SelafinCell {
                cell_id: (ce + 1) as i64,
                node_ids: ids,
                centroid_x: cx / n,
                centroid_y: cy / n,
            }
        })
        .collect();

    // Match variable index
    let needle = variable.trim().to_ascii_lowercase();
    let var_idx = var_names
        .iter()
        .position(|v| v.to_ascii_lowercase() == needle)
        .or_else(|| {
            var_names
                .iter()
                .position(|v| v.to_ascii_lowercase().contains(&needle))
        })
        .ok_or_else(|| {
            format!("selafin: variable '{variable}' not found in file. Available: {var_names:?}")
        })?;

    // Walk timesteps. Each step: [time record (4-byte f32)] + nbv1 *
    // [NPOIN f32 record]. For each step we record:
    //   * byte_offset = absolute file position of the requested
    //     variable's record (including the 4-byte length prefix).
    //   * byte_length = full Fortran record length = 4 + 4*NPOIN + 4.
    //
    // Each value record has a fixed size — we don't need to fully
    // decode the unneeded variables, just skip past them.
    let mut chunks: Vec<pgx_zarr_walker::ChunkRecord> = Vec::new();
    let mut time_lo: Option<DateTime<Utc>> = None;
    let mut time_hi: Option<DateTime<Utc>> = None;
    let value_record_size = 4 + 4 * npoin + 4;
    let mut step_idx: usize = 0;
    while !p.at_end() {
        let t_secs = match p.read_record_f32(1) {
            Ok(v) => v[0] as f64,
            Err(_) => break,
        };
        let t = epoch + chrono::Duration::milliseconds((t_secs * 1000.0) as i64);
        time_lo = Some(time_lo.map(|lo| lo.min(t)).unwrap_or(t));
        time_hi = Some(time_hi.map(|hi| hi.max(t)).unwrap_or(t));

        // For each variable: if it's our target, capture byte offset.
        for v in 0..nbv1 {
            let rec_start = p.position();
            if v == var_idx {
                let chunk_key = format!("{}@{}", variable, step_idx);
                chunks.push(pgx_zarr_walker::ChunkRecord {
                    variable: variable.to_string(),
                    uri: uri.to_string(),
                    chunk_key,
                    bbox_wkt: None, // mesh-extent bbox is on the
                    // mesh_version; per-chunk bbox of an
                    // unstructured variable is the same as
                    // the whole mesh, so leave NULL.
                    time_from: Some(t),
                    time_to: Some(t),
                    z_range: None,
                    byte_offset: Some(rec_start as i64),
                    byte_length: Some(value_record_size as i64),
                });
            }
            // Skip the whole record regardless.
            p.skip(value_record_size)?;
        }
        step_idx += 1;
    }

    if chunks.is_empty() {
        return Err(format!(
            "selafin: variable '{variable}' has no timesteps in file"
        ));
    }

    // Bbox of all nodes — used to set mesh_versions.extent.
    let (x_min, x_max) = xs.iter().fold((f32::INFINITY, f32::NEG_INFINITY), |a, &x| {
        (a.0.min(x), a.1.max(x))
    });
    let (y_min, y_max) = ys.iter().fold((f32::INFINITY, f32::NEG_INFINITY), |a, &y| {
        (a.0.min(y), a.1.max(y))
    });
    let mesh_extent_wkt = format!(
        "POLYGON(({x_min} {y_min}, {x_max} {y_min}, {x_max} {y_max}, {x_min} {y_max}, {x_min} {y_min}))"
    );

    let meta = pgx_zarr_walker::VariableMeta {
        dtype: Some("float32".to_string()),
        long_name: Some(var_names[var_idx].clone()),
        dim_order: vec![Some("time".into()), Some("node".into())],
        ..Default::default()
    };

    let variable_walk = pgx_zarr_walker::VariableWalk {
        name: variable.to_string(),
        meta,
        chunks,
    };

    Ok(SelafinWalk {
        nodes,
        cells,
        variable_walk,
        mesh_extent_wkt,
        time_from: time_lo,
        time_to: time_hi,
    })
}

// ----- chunk decode -----------------------------------------------------------

#[derive(Debug)]
struct ChunkKeyParsed {
    #[allow(dead_code)]
    variable: String,
    #[allow(dead_code)]
    step: usize,
}

fn parse_chunk_key(key: &str) -> Result<ChunkKeyParsed, String> {
    match key.rsplit_once('@') {
        Some((var, step)) => {
            let step = step
                .parse::<usize>()
                .map_err(|e| format!("selafin: bad chunk_key '{key}': {e}"))?;
            Ok(ChunkKeyParsed {
                variable: var.to_string(),
                step,
            })
        }
        None => Err(format!("selafin: chunk_key '{key}' missing @<step> suffix")),
    }
}

fn decode_chunk(
    path: &str,
    byte_offset: i64,
    byte_length: i64,
    _key: &ChunkKeyParsed,
    filter: &CoordFilter,
) -> Result<Vec<Cell>, String> {
    let mut f = std::fs::File::open(path).map_err(|e| format!("selafin: open '{path}': {e}"))?;
    use std::io::{Seek, SeekFrom};
    f.seek(SeekFrom::Start(byte_offset as u64))
        .map_err(|e| format!("selafin: seek {byte_offset} in '{path}': {e}"))?;
    let mut buf = vec![0u8; byte_length as usize];
    f.read_exact(&mut buf)
        .map_err(|e| format!("selafin: read {byte_length} at {byte_offset}: {e}"))?;
    if buf.len() < 8 {
        return Err(format!(
            "selafin: chunk too short ({} bytes < 8)",
            buf.len()
        ));
    }
    let prefix = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let data_end = 4 + prefix;
    if data_end + 4 > buf.len() {
        return Err(format!(
            "selafin: Fortran record overflow (prefix={prefix}, slab={} bytes)",
            buf.len()
        ));
    }
    let suffix = u32::from_be_bytes(buf[data_end..data_end + 4].try_into().unwrap()) as usize;
    if prefix != suffix {
        return Err(format!(
            "selafin: Fortran record mismatch (prefix {prefix} vs suffix {suffix})"
        ));
    }
    let data = &buf[4..data_end];
    if !data.len().is_multiple_of(4) {
        return Err(format!(
            "selafin: record data {} not a multiple of 4",
            data.len()
        ));
    }
    let npoin = data.len() / 4;
    let max = filter.max_cells.unwrap_or(usize::MAX);
    let mut cells = Vec::with_capacity(npoin.min(max));
    for i in 0..npoin {
        let v = f32::from_be_bytes(data[i * 4..(i + 1) * 4].try_into().unwrap()) as f64;
        cells.push(Cell {
            lat: None,
            lon: None,
            level: None,
            time: None, // populated by the SRF from chunk's time_range
            node_id: Some((i + 1) as i64),
            value: v,
        });
        if cells.len() >= max {
            break;
        }
    }
    Ok(cells)
}

// ----- Fortran-record helpers -------------------------------------------------

struct Parser<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Parser { buf, pos: 0 }
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn at_end(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn skip(&mut self, n: usize) -> Result<(), String> {
        if self.pos + n > self.buf.len() {
            return Err(format!(
                "selafin: skip {n} from {} overflows {} byte buffer",
                self.pos,
                self.buf.len()
            ));
        }
        self.pos += n;
        Ok(())
    }

    /// Read a Fortran sequential unformatted record. Returns the
    /// inner data slice (without the length prefix/suffix). Validates
    /// `prefix == suffix == expected_len`.
    fn read_record_bytes(&mut self, expected_len: usize) -> Result<&'a [u8], String> {
        if self.pos + 4 > self.buf.len() {
            return Err("selafin: EOF reading record prefix".into());
        }
        let prefix =
            u32::from_be_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        if prefix != expected_len {
            return Err(format!(
                "selafin: record prefix {prefix} != expected {expected_len} at offset {}",
                self.pos
            ));
        }
        let data_start = self.pos + 4;
        let data_end = data_start + prefix;
        let suffix_end = data_end + 4;
        if suffix_end > self.buf.len() {
            return Err(format!(
                "selafin: record at offset {} runs past EOF (need {} bytes, have {})",
                self.pos,
                suffix_end - self.pos,
                self.buf.len() - self.pos
            ));
        }
        let suffix =
            u32::from_be_bytes(self.buf[data_end..data_end + 4].try_into().unwrap()) as usize;
        if suffix != prefix {
            return Err(format!(
                "selafin: record suffix {suffix} != prefix {prefix} at offset {}",
                self.pos
            ));
        }
        let out = &self.buf[data_start..data_end];
        self.pos = suffix_end;
        Ok(out)
    }

    fn read_record_str(&mut self, expected_len: usize) -> Result<String, String> {
        let bytes = self.read_record_bytes(expected_len)?;
        Ok(String::from_utf8_lossy(bytes).trim().to_string())
    }

    fn read_record_i32(&mut self, count: usize) -> Result<Vec<i32>, String> {
        let bytes = self.read_record_bytes(count * 4)?;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            out.push(i32::from_be_bytes(
                bytes[i * 4..(i + 1) * 4].try_into().unwrap(),
            ));
        }
        Ok(out)
    }

    fn read_record_f32(&mut self, count: usize) -> Result<Vec<f32>, String> {
        let bytes = self.read_record_bytes(count * 4)?;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            out.push(f32::from_be_bytes(
                bytes[i * 4..(i + 1) * 4].try_into().unwrap(),
            ));
        }
        Ok(out)
    }
}

// ----- URI handling -----------------------------------------------------------

fn local_path(uri: &str) -> Result<String, String> {
    if let Some(rest) = uri.strip_prefix("fs://") {
        Ok(rest.to_string())
    } else if let Some(rest) = uri.strip_prefix("file://") {
        Ok(rest.to_string())
    } else if uri.starts_with('/') {
        Ok(uri.to_string())
    } else {
        Err(format!(
            "selafin: URI '{uri}' not supported — V1 reads local files only \
             (fs:// or absolute path). Download via pg_streaming opendal_sink first."
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_name_is_selafin() {
        assert_eq!(SelafinReader::new().format_name(), "selafin");
    }

    #[test]
    fn parse_chunk_key_round_trip() {
        let parsed = parse_chunk_key("WATER DEPTH@5").unwrap();
        assert_eq!(parsed.variable, "WATER DEPTH");
        assert_eq!(parsed.step, 5);
    }

    #[test]
    fn parse_chunk_key_rejects_missing_at() {
        assert!(parse_chunk_key("WATER DEPTH").is_err());
    }
}
