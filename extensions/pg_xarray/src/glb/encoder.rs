//! GLB byte-stream serialiser. Takes a finalised [`super::builder::GlbSceneBuilder`]
//! and produces the 12-byte header + JSON chunk + BIN chunk byte
//! sequence per the glTF 2.0 binary spec.

use super::builder::{json_for, GlbSceneBuilder};

const MAGIC_GLTF: &[u8; 4] = b"glTF";
const CHUNK_JSON: u32 = 0x4E4F534A; // "JSON"
const CHUNK_BIN: u32 = 0x004E4942; // "BIN\0"

pub(super) fn encode(b: GlbSceneBuilder) -> Vec<u8> {
    let json_value = json_for(&b);
    let json_bytes = serde_json::to_vec(&json_value).expect("glb json serialisation");

    // Pad JSON with ASCII space to 4-byte alignment per spec.
    let json_pad = (4 - (json_bytes.len() % 4)) % 4;
    let padded_json_len = json_bytes.len() + json_pad;

    // Pad BIN with zeros to 4-byte alignment per spec.
    let bin_pad = (4 - (b.bin.len() % 4)) % 4;
    let padded_bin_len = b.bin.len() + bin_pad;

    let total = 12 + 8 + padded_json_len + 8 + padded_bin_len;

    let mut glb = Vec::with_capacity(total);

    // 12-byte header
    glb.extend_from_slice(MAGIC_GLTF);
    glb.extend_from_slice(&2u32.to_le_bytes());
    glb.extend_from_slice(&(total as u32).to_le_bytes());

    // JSON chunk
    glb.extend_from_slice(&(padded_json_len as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_JSON.to_le_bytes());
    glb.extend_from_slice(&json_bytes);
    glb.extend(std::iter::repeat_n(0x20u8, json_pad));

    // BIN chunk
    glb.extend_from_slice(&(padded_bin_len as u32).to_le_bytes());
    glb.extend_from_slice(&CHUNK_BIN.to_le_bytes());
    glb.extend_from_slice(&b.bin);
    glb.extend(std::iter::repeat_n(0u8, bin_pad));

    debug_assert_eq!(glb.len(), total, "encoded GLB size mismatch");
    glb
}

/// Parse a GLB into its JSON chunk (as `serde_json::Value`) and BIN
/// bytes. Used by tests to round-trip through the encoder.
#[cfg(any(test, feature = "pg_test"))]
pub fn parse_glb(glb: &[u8]) -> Result<(serde_json::Value, Vec<u8>), String> {
    if glb.len() < 12 || &glb[0..4] != MAGIC_GLTF {
        return Err("not a GLB (magic mismatch)".into());
    }
    let total = u32::from_le_bytes(glb[8..12].try_into().unwrap()) as usize;
    if total != glb.len() {
        return Err(format!(
            "GLB length mismatch: header={total} actual={}",
            glb.len()
        ));
    }
    if glb.len() < 12 + 8 {
        return Err("GLB truncated (no JSON chunk)".into());
    }
    let json_len = u32::from_le_bytes(glb[12..16].try_into().unwrap()) as usize;
    let json_type = u32::from_le_bytes(glb[16..20].try_into().unwrap());
    if json_type != CHUNK_JSON {
        return Err(format!("first chunk is not JSON (0x{json_type:08x})"));
    }
    let json_end = 20 + json_len;
    if glb.len() < json_end {
        return Err("GLB truncated (JSON chunk overruns)".into());
    }
    let json_value: serde_json::Value =
        serde_json::from_slice(&glb[20..json_end]).map_err(|e| format!("JSON parse: {e}"))?;
    if glb.len() < json_end + 8 {
        return Ok((json_value, Vec::new()));
    }
    let bin_len = u32::from_le_bytes(glb[json_end..json_end + 4].try_into().unwrap()) as usize;
    let bin_type = u32::from_le_bytes(glb[json_end + 4..json_end + 8].try_into().unwrap());
    if bin_type != CHUNK_BIN {
        return Err(format!("second chunk is not BIN (0x{bin_type:08x})"));
    }
    let bin_start = json_end + 8;
    let bin_end = bin_start + bin_len;
    if glb.len() < bin_end {
        return Err("GLB truncated (BIN chunk overruns)".into());
    }
    Ok((json_value, glb[bin_start..bin_end].to_vec()))
}

// ---------------------------------------------------------------------------
// Unit tests — pure Rust, no Postgres dependency.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glb::builder::{
        AnimChannel, AnimPath, AnimSampler, GlbSceneBuilder, MorphAttrs, PrimitiveAttrs,
        PrimitiveMode, SamplerInterp,
    };

    // Build a small (4 vertices, 2 triangle) animated scene with 3
    // keyframes — 2 morph targets — and verify the GLB byte stream
    // round-trips through the parser and carries the expected shape.
    #[test]
    fn round_trip_animated_quad() {
        let mut b = GlbSceneBuilder::new("pg_xarray-test");

        // 4 vertices in a unit quad on the XY plane.
        let positions: [f32; 12] = [
            0.0, 0.0, 0.0, //
            1.0, 0.0, 0.0, //
            0.0, 1.0, 0.0, //
            1.0, 1.0, 0.0,
        ];
        // 2 triangles: 0-1-2 and 1-3-2.
        let indices: [u32; 6] = [0, 1, 2, 1, 3, 2];
        // RGBA u8 colors at base keyframe.
        let colors: [u8; 16] = [
            255, 0, 0, 255, //
            0, 255, 0, 255, //
            0, 0, 255, 255, //
            255, 255, 0, 255,
        ];

        let pos_bytes: Vec<u8> = positions.iter().flat_map(|f| f.to_le_bytes()).collect();
        let idx_bytes: Vec<u8> = indices.iter().flat_map(|u| u.to_le_bytes()).collect();
        let col_bytes: Vec<u8> = colors.to_vec();

        let bv_pos = b.add_buffer_view(&pos_bytes, Some(34962));
        let bv_idx = b.add_buffer_view(&idx_bytes, Some(34963));
        let bv_col = b.add_buffer_view(&col_bytes, Some(34962));

        let acc_pos = b.add_accessor_vec3_f32(bv_pos, 4, Some(([0.0, 0.0, 0.0], [1.0, 1.0, 0.0])));
        let acc_idx = b.add_accessor_scalar_u32(bv_idx, 6);
        let acc_col = b.add_accessor_vec4_u8_norm(bv_col, 4);

        // 2 morph targets: each carries delta-Z to displace upward.
        // Morph target k carries Z deltas where node[i] rises by 0.5*(k+1).
        let mut morphs = Vec::new();
        for k in 0..2 {
            let dz = 0.5 * (k as f32 + 1.0);
            let delta_pos: Vec<u8> = (0..4)
                .flat_map(|_| [0f32, 0f32, dz].into_iter().flat_map(|f| f.to_le_bytes()))
                .collect();
            // Morph COLOR_0 deltas — must be VEC4 f32 per spec.
            let delta_col: Vec<u8> = (0..4)
                .flat_map(|_| {
                    [0f32, 0f32, 0f32, 0f32]
                        .into_iter()
                        .flat_map(|f| f.to_le_bytes())
                })
                .collect();
            let bvp = b.add_buffer_view(&delta_pos, Some(34962));
            let bvc = b.add_buffer_view(&delta_col, Some(34962));
            let ap = b.add_accessor_vec3_f32(bvp, 4, None);
            let ac = b.add_accessor_vec4_f32(bvc, 4);
            morphs.push(MorphAttrs {
                position: Some(ap),
                color0: Some(ac),
            });
        }

        let prim = b.add_primitive(
            PrimitiveAttrs {
                position: Some(acc_pos),
                normal: None,
                color0: Some(acc_col),
            },
            Some(acc_idx),
            PrimitiveMode::Triangles,
            morphs,
        );

        let mesh = b.add_mesh(vec![prim], Some(vec![0.0, 0.0]));
        let node = b.add_node(Some(mesh), None, None);
        b.add_scene(vec![node]);

        // Animation: 3 keyframes (0s, 1s, 2s). At k=0 both weights are 0,
        // at k=1 weight[0]=1, at k=2 weight[1]=1. STEP interp.
        let times: [f32; 3] = [0.0, 1.0, 2.0];
        let weights: [f32; 6] = [0.0, 0.0, 1.0, 0.0, 0.0, 1.0];
        let t_bytes: Vec<u8> = times.iter().flat_map(|f| f.to_le_bytes()).collect();
        let w_bytes: Vec<u8> = weights.iter().flat_map(|f| f.to_le_bytes()).collect();
        let bv_t = b.add_buffer_view(&t_bytes, None);
        let bv_w = b.add_buffer_view(&w_bytes, None);
        let acc_t = b.add_accessor_scalar_f32(bv_t, 3, Some((0.0, 2.0)));
        let acc_w = b.add_accessor_scalar_f32(bv_w, 6, None);

        b.add_animation(
            vec![AnimSampler {
                input: acc_t,
                output: acc_w,
                interpolation: SamplerInterp::Step,
            }],
            vec![AnimChannel {
                sampler: 0,
                target_node: node,
                target_path: AnimPath::Weights,
            }],
        );

        let glb = b.build();

        assert_eq!(&glb[0..4], b"glTF");
        assert_eq!(u32::from_le_bytes(glb[4..8].try_into().unwrap()), 2);
        assert_eq!(
            u32::from_le_bytes(glb[8..12].try_into().unwrap()) as usize,
            glb.len()
        );
        // Whole length must be 4-byte aligned.
        assert_eq!(glb.len() % 4, 0);

        let (json, _bin) = parse_glb(&glb).expect("parse round-trip");
        assert_eq!(json["asset"]["version"], "2.0");
        assert_eq!(json["meshes"][0]["primitives"][0]["mode"], 4);
        assert_eq!(
            json["meshes"][0]["primitives"][0]["targets"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(json["animations"].as_array().unwrap().len(), 1);
        assert_eq!(
            json["animations"][0]["samplers"][0]["interpolation"],
            "STEP"
        );
    }

    // A static (no-animation) GLB should not emit an "animations" key.
    #[test]
    fn omits_animations_when_empty() {
        let mut b = GlbSceneBuilder::new("test");
        let pos: Vec<u8> = [0f32, 0f32, 0f32, 1f32, 0f32, 0f32, 0f32, 1f32, 0f32]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let idx: Vec<u8> = [0u32, 1, 2].iter().flat_map(|u| u.to_le_bytes()).collect();
        let bv_pos = b.add_buffer_view(&pos, Some(34962));
        let bv_idx = b.add_buffer_view(&idx, Some(34963));
        let acc_pos = b.add_accessor_vec3_f32(bv_pos, 3, None);
        let acc_idx = b.add_accessor_scalar_u32(bv_idx, 3);
        let prim = b.add_primitive(
            PrimitiveAttrs {
                position: Some(acc_pos),
                ..Default::default()
            },
            Some(acc_idx),
            PrimitiveMode::Triangles,
            vec![],
        );
        let mesh = b.add_mesh(vec![prim], None);
        let node = b.add_node(Some(mesh), None, None);
        b.add_scene(vec![node]);
        let glb = b.build();
        let (json, _) = parse_glb(&glb).unwrap();
        assert!(json.get("animations").is_none());
    }

    // LINES primitive should set mode = 1.
    #[test]
    fn lines_primitive_mode_is_1() {
        let mut b = GlbSceneBuilder::new("test");
        let pos: Vec<u8> = [0f32, 0f32, 0f32, 1f32, 0f32, 0f32]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let bv = b.add_buffer_view(&pos, Some(34962));
        let acc = b.add_accessor_vec3_f32(bv, 2, None);
        let prim = b.add_primitive(
            PrimitiveAttrs {
                position: Some(acc),
                ..Default::default()
            },
            None,
            PrimitiveMode::Lines,
            vec![],
        );
        let mesh = b.add_mesh(vec![prim], None);
        let node = b.add_node(Some(mesh), None, None);
        b.add_scene(vec![node]);
        let glb = b.build();
        let (json, _) = parse_glb(&glb).unwrap();
        assert_eq!(json["meshes"][0]["primitives"][0]["mode"], 1);
    }
}
