//! Self-describing metadata pulled off a Zarr v3 variable's `zarr.json`.
//!
//! `VariableMeta` is what the catalog needs to populate `pgx.variables`
//! with the file's own description — `units`, `standard_name`, CF
//! packing — instead of leaving those columns NULL. `CfPacking` is the
//! triplet `decode_chunk_values` consumes to turn stored bytes into
//! physical f64 values.
//!
//! Both types are plain Rust — no pgrx — so `pg_xarray` AND
//! `pg_streaming` (which can't depend on each other) can share them
//! through this rlib.

use crate::ChunkRecord;
use serde_json::Value;

/// What `enumerate_zarr_chunks` returns for each requested variable —
/// the parsed metadata + every chunk record for the variable's chunk
/// grid. Callers iterate the outer Vec, populate `pgx.variables` from
/// `meta`, then insert one `pgx.chunks` row per `chunks` entry.
#[derive(Debug, Clone, Default)]
pub struct VariableWalk {
    /// The variable name, exactly as passed in to the walker.
    pub name: String,
    pub meta: VariableMeta,
    pub chunks: Vec<ChunkRecord>,
}

/// CF "data packing": physical = stored * scale + offset, with
/// `fill_value` recognising a sentinel byte pattern as "missing".
///
/// Default constructor `identity()` is the no-op (scale=1, offset=0,
/// no fill); existing f32/f64 stores keep working byte-identically.
#[derive(Debug, Clone, Copy)]
pub struct CfPacking {
    pub scale: f64,
    pub offset: f64,
    /// When set, any stored cell whose decoded value equals this
    /// number (compared as f64) becomes `f64::NAN` after packing is
    /// applied. Matches xarray/numpy convention.
    pub fill_value: Option<f64>,
}

impl CfPacking {
    pub const fn identity() -> Self {
        Self {
            scale: 1.0,
            offset: 0.0,
            fill_value: None,
        }
    }
}

impl Default for CfPacking {
    fn default() -> Self {
        Self::identity()
    }
}

/// Everything the catalog wants to know about a Zarr variable at
/// register time. Read once per variable in `enumerate_zarr_chunks`;
/// surfaced alongside the per-chunk `ChunkRecord`s so the SQL
/// `register_file` call can populate `pgx.variables` and the per-chunk
/// SPI inserts without re-reading `zarr.json`.
#[derive(Debug, Clone, Default)]
pub struct VariableMeta {
    /// Raw Zarr dtype string, e.g. `"float32"`, `"int16"`, `"<u2"`.
    pub dtype: Option<String>,
    /// `dimension_names` from `zarr.json`, with `None` for unnamed dims.
    pub dim_order: Vec<Option<String>>,
    /// CF `units` attribute, e.g. `"K"`, `"m s-1"`, `"hours since 1970-01-01"`.
    pub units: Option<String>,
    /// CF `standard_name` attribute, e.g. `"air_temperature"`.
    pub standard_name: Option<String>,
    /// CF `long_name` attribute — the human-readable description.
    pub long_name: Option<String>,
    /// CF data-packing triple. `None` means no scaling — common for
    /// already-float arrays; populated when the file declares any of
    /// `scale_factor` / `add_offset` / `_FillValue` / `missing_value`.
    pub packing: Option<CfPacking>,
    /// CF `valid_range` / `valid_min` + `valid_max` — the inclusive
    /// physical-value envelope outside which the producer considers
    /// the data invalid. We store but don't enforce; queries can
    /// filter on `value BETWEEN valid_min AND valid_max` themselves.
    pub valid_min: Option<f64>,
    pub valid_max: Option<f64>,
    /// The full attribute bag minus the typed keys above — stuffed
    /// into `pgx.variables.metadata` JSONB so nothing is lost.
    pub raw_attrs: Value,
}

impl VariableMeta {
    /// Pull CF-convention attributes out of a Zarr v3 `attributes`
    /// object. Keys we recognise (and strip from `raw_attrs`):
    /// `units`, `standard_name`, `long_name`, `scale_factor`,
    /// `add_offset`, `_FillValue`, `missing_value`, `valid_range`,
    /// `valid_min`, `valid_max`. Everything else stays in `raw_attrs`.
    pub fn from_attributes(attrs: &Value) -> Self {
        let mut out = VariableMeta::default();
        let obj = match attrs.as_object() {
            Some(o) => o,
            None => return out,
        };

        let mut residue = serde_json::Map::new();
        let mut scale_factor: Option<f64> = None;
        let mut add_offset: Option<f64> = None;
        let mut fill_value: Option<f64> = None;
        let mut missing_value: Option<f64> = None;
        let mut valid_min: Option<f64> = None;
        let mut valid_max: Option<f64> = None;

        for (key, val) in obj.iter() {
            match key.as_str() {
                "units" => out.units = val.as_str().map(String::from),
                "standard_name" => out.standard_name = val.as_str().map(String::from),
                "long_name" => out.long_name = val.as_str().map(String::from),
                "scale_factor" => scale_factor = val.as_f64(),
                "add_offset" => add_offset = val.as_f64(),
                "_FillValue" => fill_value = val.as_f64(),
                "missing_value" => missing_value = val.as_f64(),
                "valid_min" => valid_min = val.as_f64(),
                "valid_max" => valid_max = val.as_f64(),
                "valid_range" => {
                    // CF: a 2-element array [min, max].
                    if let Some(arr) = val.as_array() {
                        if arr.len() == 2 {
                            valid_min = arr[0].as_f64();
                            valid_max = arr[1].as_f64();
                        }
                    }
                }
                _ => {
                    residue.insert(key.clone(), val.clone());
                }
            }
        }

        // Promote any of {scale, offset, fill} → CfPacking, with sane
        // defaults for the unset components. `missing_value` falls back
        // to `_FillValue` only if the latter wasn't set (real-world
        // files use one or the other, not both, but we tolerate both).
        if scale_factor.is_some()
            || add_offset.is_some()
            || fill_value.is_some()
            || missing_value.is_some()
        {
            out.packing = Some(CfPacking {
                scale: scale_factor.unwrap_or(1.0),
                offset: add_offset.unwrap_or(0.0),
                fill_value: fill_value.or(missing_value),
            });
        }
        out.valid_min = valid_min;
        out.valid_max = valid_max;
        out.raw_attrs = Value::Object(residue);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn identity_packing_is_noop() {
        let p = CfPacking::identity();
        assert_eq!(p.scale, 1.0);
        assert_eq!(p.offset, 0.0);
        assert!(p.fill_value.is_none());
    }

    #[test]
    fn parses_typed_keys() {
        let attrs = json!({
            "units": "K",
            "standard_name": "air_temperature",
            "long_name": "2-metre temperature",
            "scale_factor": 0.01,
            "add_offset": 273.15,
            "_FillValue": -9999.0,
            "_ARRAY_DIMENSIONS": ["time", "lat", "lon"]
        });
        let m = VariableMeta::from_attributes(&attrs);
        assert_eq!(m.units.as_deref(), Some("K"));
        assert_eq!(m.standard_name.as_deref(), Some("air_temperature"));
        assert_eq!(m.long_name.as_deref(), Some("2-metre temperature"));
        let p = m.packing.expect("CF packing");
        assert_eq!(p.scale, 0.01);
        assert_eq!(p.offset, 273.15);
        assert_eq!(p.fill_value, Some(-9999.0));
        // Unknown keys land in raw_attrs.
        assert!(m.raw_attrs.get("_ARRAY_DIMENSIONS").is_some());
        // Typed keys are stripped.
        assert!(m.raw_attrs.get("units").is_none());
    }

    #[test]
    fn valid_range_two_element_array() {
        let attrs = json!({ "valid_range": [0.0, 100.0] });
        let m = VariableMeta::from_attributes(&attrs);
        assert_eq!(m.valid_min, Some(0.0));
        assert_eq!(m.valid_max, Some(100.0));
    }

    #[test]
    fn no_packing_when_no_cf_keys() {
        let attrs = json!({ "units": "K" });
        let m = VariableMeta::from_attributes(&attrs);
        assert!(m.packing.is_none());
    }

    #[test]
    fn missing_value_falls_back_to_fill_value_when_fill_absent() {
        let attrs = json!({ "missing_value": 9.999e20 });
        let m = VariableMeta::from_attributes(&attrs);
        let p = m
            .packing
            .expect("missing_value alone still produces packing");
        assert_eq!(p.fill_value, Some(9.999e20));
        assert_eq!(p.scale, 1.0);
        assert_eq!(p.offset, 0.0);
    }
}
