use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::ffi::CStr;

#[derive(Debug, Clone, PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub struct Vec3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Vec3D {
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self { x, y, z }
    }
}

impl InOutFuncs for Vec3D {
    // Format: VEC3D(x y z)
    fn input(input: &CStr) -> Self {
        let s = input.to_str().unwrap_or("");
        let s = s.trim();
        let inner = s
            .strip_prefix("VEC3D(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or_else(|| pgrx::error!("invalid VEC3D format, expected: VEC3D(x y z)"));

        let parts: Vec<f64> = inner
            .split_whitespace()
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| pgrx::error!("invalid number in VEC3D: {s}"))
            })
            .collect();

        if parts.len() != 3 {
            pgrx::error!("VEC3D requires exactly 3 coordinates");
        }

        Self::new(parts[0], parts[1], parts[2])
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        buffer.push_str(&format!(
            "VEC3D({} {} {})",
            format_f64(self.x),
            format_f64(self.y),
            format_f64(self.z),
        ));
    }
}

fn format_f64(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < i64::MAX as f64 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}
