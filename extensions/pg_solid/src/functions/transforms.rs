use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

/// Move solid by offset. Returns new solid with recomputed header.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_translate(s: Solid, dx: f64, dy: f64, dz: f64) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"));
    let moved = ffi::transform::translate(&shape, dx, dy, dz)
        .unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"));
    Solid::from_occt_shape(&moved).unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"))
}

/// Rotate solid around axis through origin. Angle in degrees.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_rotate(s: Solid, axis_x: f64, axis_y: f64, axis_z: f64, angle_deg: f64) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"));
    let angle_rad = angle_deg.to_radians();
    let rotated = ffi::transform::rotate(&shape, axis_x, axis_y, axis_z, angle_rad)
        .unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"));
    Solid::from_occt_shape(&rotated).unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"))
}

/// Apply 4x3 affine transformation matrix (12 floats, row-major: [R|t]).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_transform(s: Solid, matrix: Vec<f64>) -> Solid {
    if matrix.len() != 12 {
        pgrx::error!(
            "solid_transform: matrix must have exactly 12 elements (3x4 row-major), got {}",
            matrix.len()
        );
    }
    let mat: [f64; 12] = matrix.try_into().unwrap();
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"));
    let transformed = ffi::transform::transform(&shape, &mat)
        .unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"));
    Solid::from_occt_shape(&transformed).unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"))
}

/// Place a solid currently in IFC local metres at WGS84 `(lat, lon, elevation)`.
/// `target_srid` selects the output coordinate system:
///
///   - **4326** (default): geographic. Output is `(lon°, lat°, elevation_m)`.
///     Local metres are converted to small angular offsets via the tangent-plane
///     approximation at the anchor, keeping vertex magnitudes web-viewer-friendly
///     (single-precision safe). Suitable for direct glTF / Cesium / Mapbox use.
///   - **4978**: geocentric (ECEF). Output is metres in EPSG:4978. Use this only
///     when you have a precision-preserving renderer — single-precision floats
///     lose ~6 cm at 6.4 Mm from origin.
///
/// `rotation_deg` is the in-plane yaw around the local up axis (CCW seen from above).
/// Use this for IFC2x3 files which carry `IfcSite.RefLatitude` / `RefLongitude` but
/// no `IfcMapConversion`. For IFC4+ files prefer `solid_georeference`.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_georeference_lonlat(
    s: Solid,
    lat: f64,
    lon: f64,
    elevation: f64,
    rotation_deg: default!(f64, 0.0),
    scale: default!(f64, 1.0),
    target_srid: default!(i32, 4326),
) -> Solid {
    let mat = match target_srid {
        4326 => tangent_plane_affine(lat, lon, elevation, rotation_deg, scale),
        4978 => ecef_affine(lat, lon, elevation, rotation_deg, scale),
        other => pgrx::error!(
            "solid_georeference_lonlat: target_srid must be 4326 (geographic) or 4978 (ECEF), got {other}"
        ),
    };

    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_georeference_lonlat: {e}"));
    let transformed = ffi::transform::transform(&shape, &mat)
        .unwrap_or_else(|e| pgrx::error!("solid_georeference_lonlat: {e}"));
    Solid::from_occt_shape(&transformed)
        .unwrap_or_else(|e| pgrx::error!("solid_georeference_lonlat: {e}"))
}

/// Tangent-plane (small-angle) affine mapping local ENU metres at the anchor
/// `(lat, lon, elevation)` to geographic `(lon°, lat°, elevation_m)`.
/// 4x3 row-major `[R|t]` in the same format `solid_transform` expects.
///
/// Local x = "east of anchor in metres", y = "north", z = "up".
pub(crate) fn tangent_plane_affine(
    lat: f64,
    lon: f64,
    elevation: f64,
    rotation_deg: f64,
    scale: f64,
) -> [f64; 12] {
    // Metres per degree at the anchor's geodetic latitude (WGS84). The longitude
    // factor depends on latitude; the latitude factor is essentially constant for
    // a building-sized footprint, so we use the standard 111319.488 m/deg.
    const METRES_PER_DEG: f64 = 111_319.488;
    let lat_r = lat.to_radians();
    let cos_lat = lat_r.cos().max(1e-9);
    let lon_per_m = 1.0 / (METRES_PER_DEG * cos_lat);
    let lat_per_m = 1.0 / METRES_PER_DEG;

    // In-plane rotation about up (CCW). Rotates local (east, north).
    let theta = rotation_deg.to_radians();
    let c = theta.cos();
    let sn = theta.sin();
    let s = scale;

    // Row 0 (lon°) picks up east contribution / KE and north contribution / KE.
    // Row 1 (lat°) picks up east / KN and north / KN.
    // Row 2 (elev m) is straight scale * z + elevation.
    [
        s * c * lon_per_m,
        -s * sn * lon_per_m,
        0.0,
        lon,
        s * sn * lat_per_m,
        s * c * lat_per_m,
        0.0,
        lat,
        0.0,
        0.0,
        s,
        elevation,
    ]
}

/// ENU-to-ECEF affine: maps local ENU metres at the anchor to EPSG:4978
/// geocentric metres. 4x3 row-major `[R|t]`.
pub(crate) fn ecef_affine(
    lat: f64,
    lon: f64,
    elevation: f64,
    rotation_deg: f64,
    scale: f64,
) -> [f64; 12] {
    const A: f64 = 6_378_137.0;
    const F: f64 = 1.0 / 298.257_223_563;
    let e2 = F * (2.0 - F);

    let lat_r = lat.to_radians();
    let lon_r = lon.to_radians();
    let sin_lat = lat_r.sin();
    let cos_lat = lat_r.cos();
    let sin_lon = lon_r.sin();
    let cos_lon = lon_r.cos();

    let n = A / (1.0 - e2 * sin_lat * sin_lat).sqrt();
    let origin_x = (n + elevation) * cos_lat * cos_lon;
    let origin_y = (n + elevation) * cos_lat * sin_lon;
    let origin_z = (n * (1.0 - e2) + elevation) * sin_lat;

    let east = [-sin_lon, cos_lon, 0.0];
    let north = [-sin_lat * cos_lon, -sin_lat * sin_lon, cos_lat];
    let up = [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat];

    let theta = rotation_deg.to_radians();
    let c = theta.cos();
    let sn = theta.sin();
    let re = [
        c * east[0] + sn * north[0],
        c * east[1] + sn * north[1],
        c * east[2] + sn * north[2],
    ];
    let rn = [
        -sn * east[0] + c * north[0],
        -sn * east[1] + c * north[1],
        -sn * east[2] + c * north[2],
    ];

    [
        scale * re[0],
        scale * rn[0],
        scale * up[0],
        origin_x,
        scale * re[1],
        scale * rn[1],
        scale * up[1],
        origin_y,
        scale * re[2],
        scale * rn[2],
        scale * up[2],
        origin_z,
    ]
}
