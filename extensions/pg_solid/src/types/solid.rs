use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::ffi::CStr;

use super::bbox3d::BBox3D;
use crate::ffi;

/// Pre-computed header fields stored alongside B-Rep bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolidHeader {
    pub version: u8,
    pub flags: u8,
    pub brep_length: u32,
    pub bbox: [f32; 6], // xmin, ymin, zmin, xmax, ymax, zmax
    pub volume: f64,
    pub surface_area: f64,
    // OBB data (12 doubles = 96 bytes)
    pub obb_center: [f64; 3],
    pub obb_x_axis: [f64; 3],
    pub obb_y_axis: [f64; 3],
    pub obb_half_size: [f64; 3],
}

impl SolidHeader {
    pub fn bbox3d(&self) -> BBox3D {
        BBox3D::from_array(self.bbox)
    }

    /// OBB volume = 8 * hx * hy * hz.
    pub fn obb_volume(&self) -> f64 {
        8.0 * self.obb_half_size[0] * self.obb_half_size[1] * self.obb_half_size[2]
    }
}

/// 3D solid geometry backed by OCCT B-Rep.
///
/// Stored as: SolidHeader + raw OCCT .brep bytes.
/// Measurement functions read from the pre-computed header (zero OCCT cost).
#[derive(Debug, Clone, PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub struct Solid {
    pub header: SolidHeader,
    pub brep_bytes: Vec<u8>,
}

impl Solid {
    /// Build a Solid from an OCCT shape, computing all header fields.
    pub fn from_occt_shape(shape: &ffi::OcctShape) -> Result<Self, String> {
        let brep_bytes = ffi::brep::write(shape)?;
        let bbox = ffi::props::bbox(shape)?;
        let volume = ffi::props::volume(shape)?;
        let surface_area = ffi::props::surface_area(shape)?;
        let obb = ffi::props::obb(shape)?;

        Ok(Self {
            header: SolidHeader {
                version: 1,
                flags: 0,
                brep_length: brep_bytes.len() as u32,
                bbox,
                volume,
                surface_area,
                obb_center: obb.center,
                obb_x_axis: obb.x_axis,
                obb_y_axis: obb.y_axis,
                obb_half_size: obb.half_size,
            },
            brep_bytes,
        })
    }

    /// Deserialize the stored BRep back to an OCCT shape for computation.
    pub fn to_occt_shape(&self) -> Result<ffi::OcctShape, String> {
        ffi::brep::read(&self.brep_bytes)
    }
}

impl InOutFuncs for Solid {
    // Text input: hex-encoded brep bytes
    fn input(input: &CStr) -> Self {
        let s = input.to_str().unwrap_or("");
        let s = s.trim();

        let brep_bytes = hex::decode(s).unwrap_or_else(|e| {
            pgrx::error!("invalid solid hex input: {e}");
        });

        let shape = ffi::brep::read(&brep_bytes).unwrap_or_else(|e| {
            pgrx::error!("invalid BRep data: {e}");
        });

        Self::from_occt_shape(&shape).unwrap_or_else(|e| {
            pgrx::error!("failed to compute solid properties: {e}");
        })
    }

    // Text output: hex-encoded brep bytes
    fn output(&self, buffer: &mut pgrx::StringInfo) {
        buffer.push_str(&hex::encode(&self.brep_bytes));
    }
}
