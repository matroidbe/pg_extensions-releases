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

    /// Apply a 4×3 row-major affine `[R|t]` to every vertex in place,
    /// and the linear part `R` to every normal (re-normalised). The
    /// matrix layout matches `solid_transform` / `ffi::transform::transform`.
    ///
    /// Use this to push georeferencing into the *mesh* path after solids
    /// have already been processed through OCCT in their native (metre-
    /// scale) frame. OCCT booleans are numerically miserable on the
    /// degree-scale output of the tangent-plane affine — fuse first,
    /// transform vertices last.
    pub fn apply_affine(&mut self, mat: &[f64; 12]) {
        // Positions: full 4×3 affine.
        for i in 0..self.vertex_count {
            let x = self.positions[i * 3] as f64;
            let y = self.positions[i * 3 + 1] as f64;
            let z = self.positions[i * 3 + 2] as f64;
            let nx = mat[0] * x + mat[1] * y + mat[2] * z + mat[3];
            let ny = mat[4] * x + mat[5] * y + mat[6] * z + mat[7];
            let nz = mat[8] * x + mat[9] * y + mat[10] * z + mat[11];
            self.positions[i * 3] = nx as f32;
            self.positions[i * 3 + 1] = ny as f32;
            self.positions[i * 3 + 2] = nz as f32;
        }
        // Normals: linear part `R` only, then renormalise per-vertex.
        // (For correctness under non-uniform scale the inverse-transpose
        // is required; for the tangent-plane affine the anisotropy is
        // cos(lat) along longitude — the shader's per-fragment normalize
        // covers the residual error.)
        for i in 0..self.vertex_count {
            let x = self.normals[i * 3] as f64;
            let y = self.normals[i * 3 + 1] as f64;
            let z = self.normals[i * 3 + 2] as f64;
            let nx = mat[0] * x + mat[1] * y + mat[2] * z;
            let ny = mat[4] * x + mat[5] * y + mat[6] * z;
            let nz = mat[8] * x + mat[9] * y + mat[10] * z;
            let len = (nx * nx + ny * ny + nz * nz).sqrt().max(1e-30);
            self.normals[i * 3] = (nx / len) as f32;
            self.normals[i * 3 + 1] = (ny / len) as f32;
            self.normals[i * 3 + 2] = (nz / len) as f32;
        }
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

    /// Human-readable axis-order hint to embed in `asset.extras` so viewers
    /// can interpret the vertex coordinates without guessing. Picked to
    /// match common SRIDs pg_solid emits today; falls back to "x,y,z metres"
    /// for unknown / projected SRIDs.
    fn axis_order_for_srid(srid: i32) -> &'static str {
        match srid {
            4326 => "longitude_deg,latitude_deg,ellipsoidal_height_m",
            4978 => "ecef_x_m,ecef_y_m,ecef_z_m",
            // Any projected CRS (e.g. EPSG:32631 UTM) lives in metres on a
            // plane; the viewer treats it as a local Cartesian frame.
            _ => "easting_m,northing_m,height_m",
        }
    }

    /// Serialize as glTF Binary v2 (GLB), tagging the asset with `target_srid`
    /// so a downstream viewer knows what CRS the vertex coords live in. Writes
    /// `asset.extras = {"srid": <i32>, "axis_order": <string>}`.
    pub fn to_glb(&self, target_srid: i32) -> Vec<u8> {
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

        let axis_order = Self::axis_order_for_srid(target_srid);

        // Build JSON (compact, no pretty-printing). `asset.extras` carries
        // the CRS + axis-order hint for downstream viewers.
        let json = format!(
            r#"{{"asset":{{"version":"2.0","generator":"pg_solid","extras":{{"srid":{srid},"axis_order":"{axis_order}"}}}},"scene":0,"scenes":[{{"nodes":[0]}}],"nodes":[{{"mesh":0}}],"meshes":[{{"primitives":[{{"attributes":{{"POSITION":0,"NORMAL":1}},"indices":2}}]}}],"accessors":[{{"bufferView":0,"componentType":5126,"type":"VEC3","count":{vc},"min":[{minx:.8},{miny:.8},{minz:.8}],"max":[{maxx:.8},{maxy:.8},{maxz:.8}]}},{{"bufferView":1,"componentType":5126,"type":"VEC3","count":{vc}}},{{"bufferView":2,"componentType":5125,"type":"SCALAR","count":{ic}}}],"bufferViews":[{{"buffer":0,"byteOffset":0,"byteLength":{posb},"target":34962}},{{"buffer":0,"byteOffset":{noff},"byteLength":{normb},"target":34962}},{{"buffer":0,"byteOffset":{ioff},"byteLength":{idxb},"target":34963}}],"buffers":[{{"byteLength":{tot}}}]}}"#,
            srid = target_srid,
            axis_order = axis_order,
            vc = self.vertex_count,
            minx = min_x,
            miny = min_y,
            minz = min_z,
            maxx = max_x,
            maxy = max_y,
            maxz = max_z,
            ic = idx_count,
            posb = pos_bytes,
            noff = norm_offset,
            normb = norm_bytes,
            ioff = idx_offset,
            idxb = idx_bytes,
            tot = total_bin,
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
}

/// Serialize multiple meshes into a single GLB. Emits one glTF mesh +
/// node per input; all nodes parented to scene 0. `names[i]` (if `Some`)
/// becomes the `nodes[i].name` so downstream pickers can map a hit back
/// to e.g. an IfcGlobalId.
///
/// This is the "no-fuse multi-element building" path: each `MeshData`
/// stays separate, no `BRepAlgoAPI_Fuse` runs, and the viewer shows
/// every input row.
pub fn write_multi_glb(meshes: &[MeshData], names: &[Option<String>], target_srid: i32) -> Vec<u8> {
    use std::fmt::Write as _;

    let axis_order = MeshData::axis_order_for_srid(target_srid);

    // Per-mesh sizing.
    struct Sized {
        pos_offset: usize,
        norm_offset: usize,
        idx_offset: usize,
        pos_bytes: usize,
        norm_bytes: usize,
        idx_bytes: usize,
        idx_count: usize,
        vertex_count: usize,
        min: [f32; 3],
        max: [f32; 3],
    }

    let mut sized: Vec<Sized> = Vec::with_capacity(meshes.len());
    let mut total_bin: usize = 0;
    for m in meshes {
        let pos_bytes = m.vertex_count * 3 * 4;
        let norm_bytes = m.vertex_count * 3 * 4;
        let idx_bytes = m.triangle_count * 3 * 4;
        let (mut min, mut max) = ([f32::MAX; 3], [f32::MIN; 3]);
        for i in 0..m.vertex_count {
            for k in 0..3 {
                let v = m.positions[i * 3 + k];
                if v < min[k] {
                    min[k] = v;
                }
                if v > max[k] {
                    max[k] = v;
                }
            }
        }
        sized.push(Sized {
            pos_offset: total_bin,
            norm_offset: total_bin + pos_bytes,
            idx_offset: total_bin + pos_bytes + norm_bytes,
            pos_bytes,
            norm_bytes,
            idx_bytes,
            idx_count: m.triangle_count * 3,
            vertex_count: m.vertex_count,
            min,
            max,
        });
        total_bin += pos_bytes + norm_bytes + idx_bytes;
    }

    // Build JSON.
    let mut accessors = String::new();
    let mut buffer_views = String::new();
    let mut mesh_blocks = String::new();
    let mut node_blocks = String::new();
    let mut scene_nodes = String::new();

    for (i, s) in sized.iter().enumerate() {
        if i > 0 {
            accessors.push(',');
            buffer_views.push(',');
            mesh_blocks.push(',');
            node_blocks.push(',');
            scene_nodes.push(',');
        }
        let acc_pos = i * 3;
        let acc_norm = i * 3 + 1;
        let acc_idx = i * 3 + 2;
        let bv_pos = i * 3;
        let bv_norm = i * 3 + 1;
        let bv_idx = i * 3 + 2;

        let _ = write!(
            accessors,
            r#"{{"bufferView":{bv_pos},"componentType":5126,"type":"VEC3","count":{vc},"min":[{minx:.8},{miny:.8},{minz:.8}],"max":[{maxx:.8},{maxy:.8},{maxz:.8}]}},{{"bufferView":{bv_norm},"componentType":5126,"type":"VEC3","count":{vc}}},{{"bufferView":{bv_idx},"componentType":5125,"type":"SCALAR","count":{ic}}}"#,
            bv_pos = bv_pos,
            bv_norm = bv_norm,
            bv_idx = bv_idx,
            vc = s.vertex_count,
            minx = s.min[0],
            miny = s.min[1],
            minz = s.min[2],
            maxx = s.max[0],
            maxy = s.max[1],
            maxz = s.max[2],
            ic = s.idx_count,
        );
        let _ = write!(
            buffer_views,
            r#"{{"buffer":0,"byteOffset":{po},"byteLength":{pb},"target":34962}},{{"buffer":0,"byteOffset":{no},"byteLength":{nb},"target":34962}},{{"buffer":0,"byteOffset":{io},"byteLength":{ib},"target":34963}}"#,
            po = s.pos_offset,
            pb = s.pos_bytes,
            no = s.norm_offset,
            nb = s.norm_bytes,
            io = s.idx_offset,
            ib = s.idx_bytes,
        );
        let _ = write!(
            mesh_blocks,
            r#"{{"primitives":[{{"attributes":{{"POSITION":{acc_pos},"NORMAL":{acc_norm}}},"indices":{acc_idx}}}]}}"#,
            acc_pos = acc_pos,
            acc_norm = acc_norm,
            acc_idx = acc_idx,
        );
        match names.get(i).and_then(|n| n.as_ref()) {
            Some(name) => {
                let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
                let _ = write!(node_blocks, r#"{{"mesh":{i},"name":"{escaped}"}}"#);
            }
            None => {
                let _ = write!(node_blocks, r#"{{"mesh":{i}}}"#);
            }
        }
        let _ = write!(scene_nodes, "{i}");
    }

    let json = format!(
        r#"{{"asset":{{"version":"2.0","generator":"pg_solid","extras":{{"srid":{srid},"axis_order":"{axis_order}","mesh_count":{n}}}}},"scene":0,"scenes":[{{"nodes":[{scene_nodes}]}}],"nodes":[{node_blocks}],"meshes":[{mesh_blocks}],"accessors":[{accessors}],"bufferViews":[{buffer_views}],"buffers":[{{"byteLength":{tot}}}]}}"#,
        srid = target_srid,
        axis_order = axis_order,
        n = meshes.len(),
        scene_nodes = scene_nodes,
        node_blocks = node_blocks,
        mesh_blocks = mesh_blocks,
        accessors = accessors,
        buffer_views = buffer_views,
        tot = total_bin,
    );

    let json_bytes = json.as_bytes();
    let json_padding = (4 - (json_bytes.len() % 4)) % 4;
    let padded_json = json_bytes.len() + json_padding;
    let bin_padding = (4 - (total_bin % 4)) % 4;
    let padded_bin = total_bin + bin_padding;
    let total_size = 12 + 8 + padded_json + 8 + padded_bin;

    let mut glb = Vec::with_capacity(total_size);
    glb.extend_from_slice(b"glTF");
    glb.extend_from_slice(&2u32.to_le_bytes());
    glb.extend_from_slice(&(total_size as u32).to_le_bytes());
    glb.extend_from_slice(&(padded_json as u32).to_le_bytes());
    glb.extend_from_slice(&0x4E4F534Au32.to_le_bytes()); // "JSON"
    glb.extend_from_slice(json_bytes);
    glb.extend(std::iter::repeat_n(0x20u8, json_padding));
    glb.extend_from_slice(&(padded_bin as u32).to_le_bytes());
    glb.extend_from_slice(&0x004E4942u32.to_le_bytes()); // "BIN\0"
    for m in meshes {
        for &v in &m.positions {
            glb.extend_from_slice(&v.to_le_bytes());
        }
        for &v in &m.normals {
            glb.extend_from_slice(&v.to_le_bytes());
        }
        for &v in &m.indices {
            glb.extend_from_slice(&v.to_le_bytes());
        }
    }
    glb.extend(std::iter::repeat_n(0u8, bin_padding));
    glb
}

impl MeshData {
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
