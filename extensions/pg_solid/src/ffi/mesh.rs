use std::fmt::Write;

use super::{last_error, OcctShape};

/// Raw mesh data extracted from OCCT tessellation.
pub struct MeshData {
    pub positions: Vec<f32>, // flat [x,y,z, x,y,z, ...]
    pub normals: Vec<f32>,   // flat [nx,ny,nz, ...]
    pub indices: Vec<u32>,   // flat [i0,i1,i2, ...]
    pub vertex_count: usize,
    pub triangle_count: usize,
}

impl MeshData {
    /// Extract tessellated mesh from an OCCT shape.
    pub fn extract(
        shape: &OcctShape,
        linear_deflection: f64,
        angular_deflection: f64,
    ) -> Result<Self, String> {
        let mut pos_ptr: *mut f32 = std::ptr::null_mut();
        let mut pos_count: usize = 0;
        let mut norm_ptr: *mut f32 = std::ptr::null_mut();
        let mut norm_count: usize = 0;
        let mut idx_ptr: *mut u32 = std::ptr::null_mut();
        let mut idx_count: usize = 0;

        let rc = unsafe {
            super::occt_mesh_extract(
                shape.ptr(),
                linear_deflection,
                angular_deflection,
                &mut pos_ptr,
                &mut pos_count,
                &mut norm_ptr,
                &mut norm_count,
                &mut idx_ptr,
                &mut idx_count,
            )
        };
        if rc != 0 {
            return Err(last_error());
        }

        // Copy to owned Vecs
        let positions = unsafe { std::slice::from_raw_parts(pos_ptr, pos_count) }.to_vec();
        let normals = unsafe { std::slice::from_raw_parts(norm_ptr, norm_count) }.to_vec();
        let indices = unsafe { std::slice::from_raw_parts(idx_ptr, idx_count) }.to_vec();

        // Free C++ allocations
        unsafe { super::occt_mesh_free(pos_ptr, norm_ptr, idx_ptr) };

        Ok(Self {
            vertex_count: pos_count / 3,
            triangle_count: idx_count / 3,
            positions,
            normals,
            indices,
        })
    }

    /// Serialize as Wavefront OBJ (text).
    pub fn to_obj(&self) -> Vec<u8> {
        let mut out = String::new();
        let _ = writeln!(out, "# pg_solid OBJ export");
        let _ = writeln!(
            out,
            "# vertices: {} triangles: {}",
            self.vertex_count, self.triangle_count
        );

        // Vertex positions
        for i in 0..self.vertex_count {
            let _ = writeln!(
                out,
                "v {:.8} {:.8} {:.8}",
                self.positions[i * 3],
                self.positions[i * 3 + 1],
                self.positions[i * 3 + 2]
            );
        }

        // Vertex normals
        for i in 0..self.vertex_count {
            let _ = writeln!(
                out,
                "vn {:.8} {:.8} {:.8}",
                self.normals[i * 3],
                self.normals[i * 3 + 1],
                self.normals[i * 3 + 2]
            );
        }

        // Faces (OBJ uses 1-based indices)
        for i in 0..self.triangle_count {
            let i0 = self.indices[i * 3] + 1;
            let i1 = self.indices[i * 3 + 1] + 1;
            let i2 = self.indices[i * 3 + 2] + 1;
            let _ = writeln!(out, "f {i0}//{i0} {i1}//{i1} {i2}//{i2}");
        }

        out.into_bytes()
    }

    /// Serialize as glTF Binary v2 (GLB).
    pub fn to_glb(&self) -> Vec<u8> {
        // Build binary buffer: positions + normals + indices
        let pos_bytes = self.vertex_count * 3 * 4; // f32
        let norm_bytes = self.vertex_count * 3 * 4; // f32
        let idx_bytes = self.triangle_count * 3 * 4; // u32

        let norm_offset = pos_bytes;
        let idx_offset = pos_bytes + norm_bytes;
        let total_bin = pos_bytes + norm_bytes + idx_bytes;

        // Pad BIN to 4-byte alignment
        let bin_padding = (4 - (total_bin % 4)) % 4;
        let padded_bin = total_bin + bin_padding;

        // Compute POSITION min/max bounds
        let (mut min_x, mut min_y, mut min_z) = (f32::MAX, f32::MAX, f32::MAX);
        let (mut max_x, mut max_y, mut max_z) = (f32::MIN, f32::MIN, f32::MIN);
        for i in 0..self.vertex_count {
            let x = self.positions[i * 3];
            let y = self.positions[i * 3 + 1];
            let z = self.positions[i * 3 + 2];
            min_x = min_x.min(x);
            min_y = min_y.min(y);
            min_z = min_z.min(z);
            max_x = max_x.max(x);
            max_y = max_y.max(y);
            max_z = max_z.max(z);
        }

        let idx_count = self.triangle_count * 3;

        // Build JSON (compact, no pretty-printing)
        let json = format!(
            r#"{{"asset":{{"version":"2.0","generator":"pg_solid"}},"scene":0,"scenes":[{{"nodes":[0]}}],"nodes":[{{"mesh":0}}],"meshes":[{{"primitives":[{{"attributes":{{"POSITION":0,"NORMAL":1}},"indices":2}}]}}],"accessors":[{{"bufferView":0,"componentType":5126,"type":"VEC3","count":{},"min":[{:.8},{:.8},{:.8}],"max":[{:.8},{:.8},{:.8}]}},{{"bufferView":1,"componentType":5126,"type":"VEC3","count":{}}},{{"bufferView":2,"componentType":5125,"type":"SCALAR","count":{}}}],"bufferViews":[{{"buffer":0,"byteOffset":0,"byteLength":{},"target":34962}},{{"buffer":0,"byteOffset":{},"byteLength":{},"target":34962}},{{"buffer":0,"byteOffset":{},"byteLength":{},"target":34963}}],"buffers":[{{"byteLength":{}}}]}}"#,
            self.vertex_count,
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
            self.vertex_count,
            idx_count,
            pos_bytes, // bufferView 0 byteLength
            norm_offset,
            norm_bytes, // bufferView 1
            idx_offset,
            idx_bytes, // bufferView 2
            total_bin, // buffer byteLength (unpadded, per spec)
        );

        let json_bytes = json.as_bytes();
        let json_padding = (4 - (json_bytes.len() % 4)) % 4;
        let padded_json = json_bytes.len() + json_padding;

        // Total GLB size: header(12) + JSON chunk header(8) + padded JSON + BIN chunk header(8) + padded BIN
        let total_size = 12 + 8 + padded_json + 8 + padded_bin;

        let mut glb = Vec::with_capacity(total_size);

        // GLB header
        glb.extend_from_slice(b"glTF"); // magic
        glb.extend_from_slice(&2u32.to_le_bytes()); // version
        glb.extend_from_slice(&(total_size as u32).to_le_bytes()); // total length

        // JSON chunk
        glb.extend_from_slice(&(padded_json as u32).to_le_bytes()); // chunk length
        glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes()); // "JSON"
        glb.extend_from_slice(json_bytes);
        glb.extend(std::iter::repeat_n(0x20u8, json_padding)); // pad with spaces

        // BIN chunk
        glb.extend_from_slice(&(padded_bin as u32).to_le_bytes()); // chunk length
        glb.extend_from_slice(&0x004E4942u32.to_le_bytes()); // "BIN\0"

        // Write positions as f32 LE
        for &val in &self.positions {
            glb.extend_from_slice(&val.to_le_bytes());
        }
        // Write normals as f32 LE
        for &val in &self.normals {
            glb.extend_from_slice(&val.to_le_bytes());
        }
        // Write indices as u32 LE
        for &val in &self.indices {
            glb.extend_from_slice(&val.to_le_bytes());
        }
        // Pad BIN with zeros
        glb.extend(std::iter::repeat_n(0u8, bin_padding));

        glb
    }

    /// Serialize as USD ASCII (USDA).
    pub fn to_usda(&self) -> Vec<u8> {
        let mut out = String::new();
        let _ = writeln!(out, "#usda 1.0");
        let _ = writeln!(out, "(");
        let _ = writeln!(out, "    defaultPrim = \"Root\"");
        let _ = writeln!(out, ")");
        let _ = writeln!(out);
        let _ = writeln!(out, "def Xform \"Root\" {{");
        let _ = writeln!(out, "    def Mesh \"geometry\" {{");

        // faceVertexCounts (all triangles = all 3s)
        let _ = write!(out, "        int[] faceVertexCounts = [");
        for i in 0..self.triangle_count {
            if i > 0 {
                let _ = write!(out, ", ");
            }
            let _ = write!(out, "3");
        }
        let _ = writeln!(out, "]");

        // faceVertexIndices
        let _ = write!(out, "        int[] faceVertexIndices = [");
        for (i, idx) in self.indices.iter().enumerate() {
            if i > 0 {
                let _ = write!(out, ", ");
            }
            let _ = write!(out, "{idx}");
        }
        let _ = writeln!(out, "]");

        // points
        let _ = writeln!(out, "        point3f[] points = [");
        for i in 0..self.vertex_count {
            let comma = if i + 1 < self.vertex_count { "," } else { "" };
            let _ = writeln!(
                out,
                "            ({:.8}, {:.8}, {:.8}){comma}",
                self.positions[i * 3],
                self.positions[i * 3 + 1],
                self.positions[i * 3 + 2],
            );
        }
        let _ = writeln!(out, "        ]");

        // normals
        let _ = writeln!(out, "        normal3f[] normals = [");
        for i in 0..self.vertex_count {
            let comma = if i + 1 < self.vertex_count { "," } else { "" };
            let _ = writeln!(
                out,
                "            ({:.8}, {:.8}, {:.8}){comma}",
                self.normals[i * 3],
                self.normals[i * 3 + 1],
                self.normals[i * 3 + 2],
            );
        }
        let _ = writeln!(out, "        ]");

        let _ = writeln!(out, "        uniform token subdivisionScheme = \"none\"");
        let _ = writeln!(out, "    }}");
        let _ = writeln!(out, "}}");

        out.into_bytes()
    }
}
