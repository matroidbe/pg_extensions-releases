//! IFC georeferencing — extract IfcSite lat/lon and IfcMapConversion / IfcProjectedCRS
//! so BIM solids can be placed in a real-world projected CRS.

use super::entity::{EntityStore, IfcEntity, IfcValue};

/// Site location parsed from `IfcSite`. Latitude / longitude are decimal degrees
/// in WGS84 (north / east positive). Elevation is metres (model length unit).
#[derive(Debug, Clone, PartialEq)]
pub struct IfcSiteLocation {
    pub global_id: String,
    pub name: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub elevation: Option<f64>,
}

/// IFC4 `IfcMapConversion` joined with the referenced `IfcProjectedCRS`.
#[derive(Debug, Clone, PartialEq)]
pub struct IfcMapConversionData {
    pub eastings: f64,
    pub northings: f64,
    pub orthogonal_height: f64,
    pub x_axis_abscissa: f64,
    pub x_axis_ordinate: f64,
    pub scale: f64,
    pub source_crs_name: Option<String>,
    pub target_crs_name: Option<String>,
}

impl IfcMapConversionData {
    /// Planar rotation in radians, derived from `XAxisAbscissa` / `XAxisOrdinate`.
    pub fn rotation_rad(&self) -> f64 {
        self.x_axis_ordinate.atan2(self.x_axis_abscissa)
    }

    /// Planar rotation in degrees.
    pub fn rotation_deg(&self) -> f64 {
        self.rotation_rad().to_degrees()
    }

    /// 4x3 row-major affine that maps IFC local coords -> target CRS units.
    /// Format matches `solid_transform`: 12 floats laid out as
    /// `[r00 r01 r02 tx, r10 r11 r12 ty, r20 r21 r22 tz]`.
    pub fn to_affine(&self) -> [f64; 12] {
        let c = self.x_axis_abscissa * self.scale;
        let s = self.x_axis_ordinate * self.scale;
        [
            c,
            -s,
            0.0,
            self.eastings,
            s,
            c,
            0.0,
            self.northings,
            0.0,
            0.0,
            self.scale,
            self.orthogonal_height,
        ]
    }
}

/// Convert an `IfcCompoundPlaneAngleMeasure` (list of 3 or 4 integers
/// `[deg, min, sec, millionths_of_arc_second]`) to decimal degrees.
///
/// Per the IFC spec all components share the sign of the first non-zero
/// component, so we sign the magnitude once at the end.
pub fn compound_angle_to_decimal(parts: &[IfcValue]) -> Option<f64> {
    if parts.is_empty() {
        return None;
    }
    let mut nums: Vec<f64> = Vec::with_capacity(4);
    for p in parts.iter().take(4) {
        nums.push(p.as_f64()?);
    }
    let sign = nums
        .iter()
        .find(|n| **n != 0.0)
        .map(|n| n.signum())
        .unwrap_or(1.0);
    let mag = nums.iter().map(|n| n.abs()).collect::<Vec<_>>();
    let deg = mag.first().copied().unwrap_or(0.0);
    let min = mag.get(1).copied().unwrap_or(0.0);
    let sec = mag.get(2).copied().unwrap_or(0.0);
    let micros = mag.get(3).copied().unwrap_or(0.0);
    Some(sign * (deg + min / 60.0 + sec / 3600.0 + micros / 3_600_000_000.0))
}

fn list_of(value: &IfcValue) -> Option<&[IfcValue]> {
    match value {
        IfcValue::List(v) => Some(v),
        IfcValue::Typed(_, inner) => list_of(inner),
        _ => None,
    }
}

/// Extract all `IfcSite` location records from the store.
pub fn extract_site_locations(store: &EntityStore) -> Vec<IfcSiteLocation> {
    let mut out = Vec::new();
    for ent in store.by_type("IfcSite") {
        let global_id = store.global_id(ent).unwrap_or_default();
        let name = store.name(ent);

        // IfcSite: attr 9 = RefLatitude, attr 10 = RefLongitude, attr 11 = RefElevation
        let lat = ent
            .attributes
            .get(9)
            .filter(|v| !v.is_null())
            .and_then(list_of)
            .and_then(compound_angle_to_decimal);
        let lon = ent
            .attributes
            .get(10)
            .filter(|v| !v.is_null())
            .and_then(list_of)
            .and_then(compound_angle_to_decimal);
        let elevation = ent
            .attributes
            .get(11)
            .filter(|v| !v.is_null())
            .and_then(|v| v.as_f64());

        out.push(IfcSiteLocation {
            global_id,
            name,
            lat,
            lon,
            elevation,
        });
    }
    out
}

fn crs_name(store: &EntityStore, attr: Option<&IfcValue>) -> Option<String> {
    let id = attr?.as_ref()?;
    let ent = store.get(id)?;
    // IfcProjectedCRS / IfcCoordinateReferenceSystem attr 0 = Name
    ent.attributes
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Resolve a `SourceCRS` reference (which can be either an
/// `IfcCoordinateReferenceSystem` carrying a Name, or an
/// `IfcGeometricRepresentationContext` which has no human CRS name).
fn source_crs_name(store: &EntityStore, attr: Option<&IfcValue>) -> Option<String> {
    let id = attr?.as_ref()?;
    let ent = store.get(id)?;
    if ent
        .type_name
        .eq_ignore_ascii_case("IfcGeometricRepresentationContext")
    {
        // Source is the local model context; no projected name to report.
        return None;
    }
    ent.attributes
        .first()
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Extract the first `IfcMapConversion` (if any). IFC permits multiple in
/// principle but the de-facto convention is one per file.
pub fn extract_map_conversion(store: &EntityStore) -> Option<IfcMapConversionData> {
    let ent: &IfcEntity = store.by_type("IfcMapConversion").into_iter().next()?;
    // attrs: 0 SourceCRS, 1 TargetCRS, 2 Eastings, 3 Northings, 4 OrthogonalHeight,
    //        5 XAxisAbscissa, 6 XAxisOrdinate, 7 Scale
    let eastings = ent.attributes.get(2)?.as_f64()?;
    let northings = ent.attributes.get(3)?.as_f64()?;
    let orthogonal_height = ent.attributes.get(4)?.as_f64()?;
    let x_axis_abscissa = ent
        .attributes
        .get(5)
        .filter(|v| !v.is_null())
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);
    let x_axis_ordinate = ent
        .attributes
        .get(6)
        .filter(|v| !v.is_null())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let scale = ent
        .attributes
        .get(7)
        .filter(|v| !v.is_null())
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let source_crs_name = source_crs_name(store, ent.attributes.first());
    let target_crs_name = crs_name(store, ent.attributes.get(1));

    Some(IfcMapConversionData {
        eastings,
        northings,
        orthogonal_height,
        x_axis_abscissa,
        x_axis_ordinate,
        scale,
        source_crs_name,
        target_crs_name,
    })
}

/// Parse `filepath`, route on `target_srid`, and return the 4×3 row-major
/// affine that maps IFC local coordinates to `target_srid`.
///
/// Same routing as `solid_georeference`:
///   * `4326` → tangent-plane (lon°, lat°, elev_m), anchored at IfcSite lat/lon
///   * `4978` → ECEF metres, anchored at IfcSite lat/lon
///   * other → must match the file's `IfcProjectedCRS` (EPSG:NNNN), uses the
///     file's `IfcMapConversion` verbatim
///
/// The closures let callers reuse this code from places that don't depend
/// on `crate::functions::transforms` (e.g. the mesh-emission path) — pass
/// `transforms::tangent_plane_affine` and `transforms::ecef_affine`.
#[allow(clippy::type_complexity)]
pub fn build_affine_for_file<TP, EC>(
    filepath: &str,
    target_srid: i32,
    error_prefix: &str,
    tangent_plane_affine: TP,
    ecef_affine: EC,
) -> [f64; 12]
where
    TP: Fn(f64, f64, f64, f64, f64) -> [f64; 12],
    EC: Fn(f64, f64, f64, f64, f64) -> [f64; 12],
{
    let data =
        std::fs::read(filepath).unwrap_or_else(|e| pgrx::error!("{error_prefix}: read file: {e}"));
    let parse_result = crate::ifc::parser::parse(&data)
        .unwrap_or_else(|e| pgrx::error!("{error_prefix}: parse: {e}"));
    let store = crate::ifc::entity::EntityStore::new(parse_result.entities);
    let mc = extract_map_conversion(&store);

    match target_srid {
        4326 | 4978 => {
            let site = extract_site_locations(&store)
                .into_iter()
                .find(|x| x.lat.is_some() && x.lon.is_some())
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "{error_prefix}: target_srid={target_srid} requires \
                         IfcSite.RefLatitude / RefLongitude (not present in {filepath})"
                    )
                });
            let lat = site.lat.unwrap();
            let lon = site.lon.unwrap();
            let elev = site.elevation.unwrap_or(0.0);
            let (rotation_deg, scale) = match mc.as_ref() {
                Some(m) => (m.rotation_deg(), m.scale),
                None => (0.0, 1.0),
            };
            if target_srid == 4326 {
                tangent_plane_affine(lat, lon, elev, rotation_deg, scale)
            } else {
                ecef_affine(lat, lon, elev, rotation_deg, scale)
            }
        }
        projected => {
            let mc = mc.unwrap_or_else(|| {
                pgrx::error!(
                    "{error_prefix}: {filepath} has no IfcMapConversion, \
                     cannot honour target_srid={projected}"
                )
            });
            if let Some(file_srid) = mc
                .target_crs_name
                .as_deref()
                .and_then(|n| n.strip_prefix("EPSG:"))
                .and_then(|n| n.parse::<i32>().ok())
            {
                if file_srid != projected {
                    pgrx::error!(
                        "{error_prefix}: target_srid={projected} doesn't match \
                         file's IfcProjectedCRS '{}' (SRID {file_srid})",
                        mc.target_crs_name.as_deref().unwrap_or("?"),
                    );
                }
            }
            mc.to_affine()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ifc::parser;
    use std::collections::HashMap;

    fn parse_fixture(path: &str) -> EntityStore {
        let data = std::fs::read(path).expect("read fixture");
        let pr = parser::parse(&data).expect("parse fixture");
        EntityStore::new(pr.entities)
    }

    #[test]
    fn test_compound_angle_to_decimal_three_parts() {
        // 50 deg 50 min 0 sec = 50 + 50/60 = 50.8333...
        let parts = vec![
            IfcValue::Integer(50),
            IfcValue::Integer(50),
            IfcValue::Integer(0),
        ];
        let d = compound_angle_to_decimal(&parts).unwrap();
        assert!((d - 50.833_333_333_333_33).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn test_compound_angle_to_decimal_four_parts() {
        // 4 deg 21 min 0 sec 0 micros = 4.35
        let parts = vec![
            IfcValue::Integer(4),
            IfcValue::Integer(21),
            IfcValue::Integer(0),
            IfcValue::Integer(0),
        ];
        let d = compound_angle_to_decimal(&parts).unwrap();
        assert!((d - 4.35).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn test_compound_angle_negative() {
        // -33 deg 51 min 30 sec should produce a negative decimal degree.
        let parts = vec![
            IfcValue::Integer(-33),
            IfcValue::Integer(51),
            IfcValue::Integer(30),
        ];
        let d = compound_angle_to_decimal(&parts).unwrap();
        assert!(d < 0.0);
        assert!((d.abs() - 33.858_333_333_333_33).abs() < 1e-9, "got {d}");
    }

    #[test]
    fn test_compound_angle_micros_precision() {
        // 0 deg 0 min 0 sec 3_600_000_000 micros = 1 deg exactly.
        let parts = vec![
            IfcValue::Integer(0),
            IfcValue::Integer(0),
            IfcValue::Integer(0),
            IfcValue::Integer(3_600_000_000),
        ];
        let d = compound_angle_to_decimal(&parts).unwrap();
        assert!((d - 1.0).abs() < 1e-12, "got {d}");
    }

    #[test]
    fn test_extract_site_locations_fixture() {
        let store = parse_fixture("test_data/georef_building.ifc");
        let sites = extract_site_locations(&store);
        assert_eq!(sites.len(), 1);
        let s = &sites[0];
        assert_eq!(s.global_id, "0002_SITE_____GUID");
        assert_eq!(s.name.as_deref(), Some("TestSite"));
        let lat = s.lat.expect("lat");
        let lon = s.lon.expect("lon");
        assert!((lat - 50.833_333_333_333_33).abs() < 1e-9);
        assert!((lon - 4.35).abs() < 1e-9);
        assert_eq!(s.elevation, Some(15.0));
    }

    #[test]
    fn test_extract_site_locations_no_geo() {
        // minimal_building.ifc has IfcSite with all geo fields = $.
        let store = parse_fixture("test_data/minimal_building.ifc");
        let sites = extract_site_locations(&store);
        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].lat, None);
        assert_eq!(sites[0].lon, None);
        assert_eq!(sites[0].elevation, None);
    }

    #[test]
    fn test_extract_map_conversion_fixture() {
        let store = parse_fixture("test_data/georef_building.ifc");
        let mc = extract_map_conversion(&store).expect("map conversion present");
        assert_eq!(mc.eastings, 597_500.0);
        assert_eq!(mc.northings, 5_630_500.0);
        assert_eq!(mc.orthogonal_height, 15.0);
        assert_eq!(mc.x_axis_abscissa, 1.0);
        assert_eq!(mc.x_axis_ordinate, 0.0);
        assert_eq!(mc.scale, 1.0);
        assert_eq!(mc.target_crs_name.as_deref(), Some("EPSG:32631"));
        assert!(mc.source_crs_name.is_none());
        assert!((mc.rotation_deg()).abs() < 1e-12);
    }

    #[test]
    fn test_extract_map_conversion_absent() {
        let store = parse_fixture("test_data/minimal_building.ifc");
        assert!(extract_map_conversion(&store).is_none());
    }

    #[test]
    fn test_to_affine_translation_only() {
        let mc = IfcMapConversionData {
            eastings: 100.0,
            northings: 200.0,
            orthogonal_height: 50.0,
            x_axis_abscissa: 1.0,
            x_axis_ordinate: 0.0,
            scale: 1.0,
            source_crs_name: None,
            target_crs_name: None,
        };
        let m = mc.to_affine();
        // Row-major [R|t]: rotation = identity, translation = (100, 200, 50)
        assert_eq!(
            m,
            [1.0, 0.0, 0.0, 100.0, 0.0, 1.0, 0.0, 200.0, 0.0, 0.0, 1.0, 50.0]
        );
    }

    #[test]
    fn test_to_affine_rotation_90_deg() {
        // 90 deg about Z: abscissa = cos(90) = 0, ordinate = sin(90) = 1
        let mc = IfcMapConversionData {
            eastings: 0.0,
            northings: 0.0,
            orthogonal_height: 0.0,
            x_axis_abscissa: 0.0,
            x_axis_ordinate: 1.0,
            scale: 1.0,
            source_crs_name: None,
            target_crs_name: None,
        };
        let m = mc.to_affine();
        // R = [[0,-1,0],[1,0,0],[0,0,1]] (rotation by +90deg about Z)
        let eps = 1e-12;
        assert!((m[0] - 0.0).abs() < eps);
        assert!((m[1] - -1.0).abs() < eps);
        assert!((m[4] - 1.0).abs() < eps);
        assert!((m[5] - 0.0).abs() < eps);
        assert!((m[10] - 1.0).abs() < eps);
        assert!((mc.rotation_deg() - 90.0).abs() < 1e-9);
    }

    #[test]
    fn test_no_panic_on_missing_attrs() {
        // Ensure extraction doesn't panic on entities with too-few attributes.
        let mut entities = HashMap::new();
        entities.insert(
            1,
            IfcEntity {
                id: 1,
                type_name: "IfcSite".to_string(),
                attributes: vec![IfcValue::String("only_guid".to_string())],
            },
        );
        let store = EntityStore::new(entities);
        let sites = extract_site_locations(&store);
        assert_eq!(sites.len(), 1);
        assert!(sites[0].lat.is_none());
    }
}
