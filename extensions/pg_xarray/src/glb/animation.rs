//! Turn an [`AssembledMesh`] into a populated [`GlbSceneBuilder`] —
//! base attributes, morph-target deltas, optional flow-arrow LINES
//! primitive, and a single STEP-weighted animation sampler that drives
//! all per-keyframe morphs.
//!
//! All buffers are stored as little-endian f32 / u8 / u32. The base
//! POSITION carries (x, y, z) at keyframe 0; subsequent keyframes
//! contribute morph deltas. Per-vertex COLOR_0 is VEC4 u8 normalized
//! at the base, VEC4 f32 on morph deltas (the glTF spec forbids
//! normalized integer types on morph accessors).

use serde_json::json;

use super::builder::{
    AnimChannel, AnimPath, AnimSampler, GlbSceneBuilder, MorphAttrs, PrimitiveAttrs, PrimitiveMode,
    SamplerInterp,
};
use super::colormap;
use super::mesh_assembly::AssembledMesh;

const BV_TARGET_ARRAY: u32 = 34962; // ARRAY_BUFFER
const BV_TARGET_ELEMENT_ARRAY: u32 = 34963; // ELEMENT_ARRAY_BUFFER

#[derive(Debug, Clone)]
pub struct SceneOptions<'a> {
    pub colormap_name: &'a str,
    pub vmin: f64,
    pub vmax: f64,
    pub z_scale: f64,
    pub arrow_scale: f64,
    /// Multiplier applied to keyframe times — values > 1 compress
    /// playback. Sim seconds are divided by this before being written
    /// into the GLB sampler input.
    pub time_scale: f64,
    /// Optional metadata to inline into `asset.extras` for round-tripping.
    pub extras: Option<serde_json::Value>,
}

/// Build a complete scene from a populated mesh + value series.
///
/// Emits one TRIANGLES primitive for the water surface. Adds a LINES
/// primitive in a sibling node when any keyframe carries flow data.
/// Both primitives share the same animation sampler.
pub fn build_scene(mesh: &AssembledMesh, opts: &SceneOptions<'_>) -> GlbSceneBuilder {
    let mut b = GlbSceneBuilder::new("pg_xarray");
    if let Some(extras) = &opts.extras {
        b.set_asset_extras(extras.clone());
    }

    let n_vertices = mesh.vertex_count as usize;
    let n_keyframes = mesh.keyframes.len();
    assert!(
        n_keyframes >= 1,
        "build_scene requires at least one keyframe"
    );
    let lut = colormap::lookup(opts.colormap_name);

    // --------- base POSITION (x, y, z * z_scale) ---------
    let mut base_pos = Vec::with_capacity(n_vertices * 3 * 4);
    let (mut min_x, mut min_y, mut min_z) = (f32::INFINITY, f32::INFINITY, f32::INFINITY);
    let (mut max_x, mut max_y, mut max_z) =
        (f32::NEG_INFINITY, f32::NEG_INFINITY, f32::NEG_INFINITY);
    for i in 0..n_vertices {
        let x = mesh.base_xy[i * 2];
        let y = mesh.base_xy[i * 2 + 1];
        let z = (sanitize(mesh.keyframes[0].z[i]) * opts.z_scale) as f32;
        base_pos.extend_from_slice(&x.to_le_bytes());
        base_pos.extend_from_slice(&y.to_le_bytes());
        base_pos.extend_from_slice(&z.to_le_bytes());
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        min_z = min_z.min(z);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
        max_z = max_z.max(z);
    }
    let bv_pos = b.add_buffer_view(&base_pos, Some(BV_TARGET_ARRAY));
    let acc_pos = b.add_accessor_vec3_f32(
        bv_pos,
        n_vertices,
        Some(([min_x, min_y, min_z], [max_x, max_y, max_z])),
    );

    // --------- base NORMAL (0, 0, 1) per vertex ---------
    let mut base_nrm = Vec::with_capacity(n_vertices * 3 * 4);
    for _ in 0..n_vertices {
        base_nrm.extend_from_slice(&0f32.to_le_bytes());
        base_nrm.extend_from_slice(&0f32.to_le_bytes());
        base_nrm.extend_from_slice(&1f32.to_le_bytes());
    }
    let bv_nrm = b.add_buffer_view(&base_nrm, Some(BV_TARGET_ARRAY));
    let acc_nrm = b.add_accessor_vec3_f32(bv_nrm, n_vertices, None);

    // --------- base COLOR_0 (RGBA u8 normalized) ---------
    let base_colors_rgb: Vec<[u8; 3]> = (0..n_vertices)
        .map(|i| {
            let v = mesh.keyframes[0].color[i];
            let t = colormap::normalize(v, opts.vmin, opts.vmax);
            colormap::sample(lut, t)
        })
        .collect();
    let mut base_col = Vec::with_capacity(n_vertices * 4);
    for c in &base_colors_rgb {
        base_col.extend_from_slice(&[c[0], c[1], c[2], 255]);
    }
    let bv_col = b.add_buffer_view(&base_col, Some(BV_TARGET_ARRAY));
    let acc_col = b.add_accessor_vec4_u8_norm(bv_col, n_vertices);

    // --------- indices ---------
    let mut idx_bytes = Vec::with_capacity(mesh.triangles.len() * 4);
    for i in &mesh.triangles {
        idx_bytes.extend_from_slice(&i.to_le_bytes());
    }
    let bv_idx = b.add_buffer_view(&idx_bytes, Some(BV_TARGET_ELEMENT_ARRAY));
    let acc_idx = b.add_accessor_scalar_u32(bv_idx, mesh.triangles.len());

    // --------- per-keyframe morph targets (k >= 1) ---------
    let mut surface_morphs: Vec<MorphAttrs> = Vec::with_capacity(n_keyframes.saturating_sub(1));
    for k in 1..n_keyframes {
        // POSITION delta — only Z changes. Track min/max so the morph
        // accessor satisfies the glTF spec's bounds requirement for
        // every POSITION accessor (base + morph deltas).
        let mut dpos = Vec::with_capacity(n_vertices * 3 * 4);
        let (mut min_dz, mut max_dz) = (f32::INFINITY, f32::NEG_INFINITY);
        for i in 0..n_vertices {
            let z0 = sanitize(mesh.keyframes[0].z[i]) * opts.z_scale;
            let zk = sanitize(mesh.keyframes[k].z[i]) * opts.z_scale;
            let dz = (zk - z0) as f32;
            dpos.extend_from_slice(&0f32.to_le_bytes());
            dpos.extend_from_slice(&0f32.to_le_bytes());
            dpos.extend_from_slice(&dz.to_le_bytes());
            min_dz = min_dz.min(dz);
            max_dz = max_dz.max(dz);
        }
        if !min_dz.is_finite() {
            min_dz = 0.0;
            max_dz = 0.0;
        }
        let bv = b.add_buffer_view(&dpos, Some(BV_TARGET_ARRAY));
        let acc_dpos = b.add_accessor_vec3_f32(
            bv,
            n_vertices,
            Some(([0.0, 0.0, min_dz], [0.0, 0.0, max_dz])),
        );

        // COLOR_0 delta — VEC4 f32.
        let mut dcol = Vec::with_capacity(n_vertices * 4 * 4);
        #[allow(clippy::needless_range_loop)]
        for i in 0..n_vertices {
            let v = mesh.keyframes[k].color[i];
            let t = colormap::normalize(v, opts.vmin, opts.vmax);
            let rgb = colormap::sample(lut, t);
            let base = base_colors_rgb[i];
            // Encode as deltas of normalized [0, 1] floats so the renderer
            // produces final_color = base_norm + sum(weight_k * delta_k).
            let dr = (rgb[0] as f32 - base[0] as f32) / 255.0;
            let dg = (rgb[1] as f32 - base[1] as f32) / 255.0;
            let db = (rgb[2] as f32 - base[2] as f32) / 255.0;
            dcol.extend_from_slice(&dr.to_le_bytes());
            dcol.extend_from_slice(&dg.to_le_bytes());
            dcol.extend_from_slice(&db.to_le_bytes());
            dcol.extend_from_slice(&0f32.to_le_bytes()); // alpha delta
        }
        let bv = b.add_buffer_view(&dcol, Some(BV_TARGET_ARRAY));
        let acc_dcol = b.add_accessor_vec4_f32(bv, n_vertices);

        surface_morphs.push(MorphAttrs {
            position: Some(acc_dpos),
            color0: Some(acc_dcol),
        });
    }

    // --------- surface primitive + mesh + node ---------
    let surface_weights = if n_keyframes > 1 {
        Some(vec![0.0f32; n_keyframes - 1])
    } else {
        None
    };
    let surface_prim = b.add_primitive(
        PrimitiveAttrs {
            position: Some(acc_pos),
            normal: Some(acc_nrm),
            color0: Some(acc_col),
        },
        Some(acc_idx),
        PrimitiveMode::Triangles,
        surface_morphs,
    );
    let surface_mesh = b.add_mesh(vec![surface_prim], surface_weights);
    let surface_node = b.add_node(
        Some(surface_mesh),
        None,
        Some(json!({
            "kind": "water-surface",
            "colormap": opts.colormap_name,
            "vmin": opts.vmin,
            "vmax": opts.vmax,
            "z_scale": opts.z_scale,
            "srid": mesh.srid,
        })),
    );

    // --------- optional flow-arrows (LINES) ---------
    let arrows_node: Option<u32> = if mesh.keyframes[0].flow_uv.is_some() {
        Some(build_arrows_node(&mut b, mesh, opts, n_keyframes))
    } else {
        None
    };
    let mut scene_root_nodes = vec![surface_node];
    if let Some(n) = arrows_node {
        scene_root_nodes.push(n);
    }
    b.add_scene(scene_root_nodes);

    // --------- animation (one sampler, channels per animated node) ---------
    if n_keyframes > 1 {
        // input: keyframe times, optionally compressed via time_scale so
        // long simulations don't play out at real-time. glTF spec
        // requires min/max on every animation sampler input accessor.
        let time_div = if opts.time_scale > 0.0 {
            opts.time_scale as f32
        } else {
            1.0
        };
        let mut t_bytes = Vec::with_capacity(n_keyframes * 4);
        let (mut t_min, mut t_max) = (f32::INFINITY, f32::NEG_INFINITY);
        for k in &mesh.keyframes {
            let t = k.time_seconds / time_div;
            t_bytes.extend_from_slice(&t.to_le_bytes());
            t_min = t_min.min(t);
            t_max = t_max.max(t);
        }
        let bv_t = b.add_buffer_view(&t_bytes, None);
        let acc_t = b.add_accessor_scalar_f32(bv_t, n_keyframes, Some((t_min, t_max)));

        // output: STEP weights — N_keyframes × (N_morphs) identity-row matrix.
        let n_morphs = n_keyframes - 1;
        let mut w_bytes = Vec::with_capacity(n_keyframes * n_morphs * 4);
        for k in 0..n_keyframes {
            for m in 0..n_morphs {
                let w: f32 = if k == m + 1 { 1.0 } else { 0.0 };
                w_bytes.extend_from_slice(&w.to_le_bytes());
            }
        }
        let bv_w = b.add_buffer_view(&w_bytes, None);
        let acc_w = b.add_accessor_scalar_f32(bv_w, n_keyframes * n_morphs, None);

        let sampler_idx = 0u32;
        let mut channels = vec![AnimChannel {
            sampler: sampler_idx,
            target_node: surface_node,
            target_path: AnimPath::Weights,
        }];
        if let Some(an) = arrows_node {
            channels.push(AnimChannel {
                sampler: sampler_idx,
                target_node: an,
                target_path: AnimPath::Weights,
            });
        }
        b.add_animation(
            vec![AnimSampler {
                input: acc_t,
                output: acc_w,
                interpolation: SamplerInterp::Step,
            }],
            channels,
        );
    }

    b
}

/// Build the arrows LINES node — two vertices per surface node (tail +
/// tip), one segment per node, animated via tip-displacement morphs.
fn build_arrows_node(
    b: &mut GlbSceneBuilder,
    mesh: &AssembledMesh,
    opts: &SceneOptions<'_>,
    n_keyframes: usize,
) -> u32 {
    let n_vertices = mesh.vertex_count as usize;
    let arrow_scale = opts.arrow_scale as f32;

    // Base positions: 2 vertices per node — tail, tip.
    let mut base_pos = Vec::with_capacity(n_vertices * 2 * 3 * 4);
    let mut indices: Vec<u32> = Vec::with_capacity(n_vertices * 2);
    let kf0 = &mesh.keyframes[0];
    let flow0 = kf0
        .flow_uv
        .as_ref()
        .expect("arrows requires flow_uv on keyframe[0]");
    let (mut min_x, mut min_y, mut min_z) = (f32::INFINITY, f32::INFINITY, f32::INFINITY);
    let (mut max_x, mut max_y, mut max_z) =
        (f32::NEG_INFINITY, f32::NEG_INFINITY, f32::NEG_INFINITY);
    let mut update_bounds = |x: f32, y: f32, z: f32| {
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        min_z = min_z.min(z);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
        max_z = max_z.max(z);
    };
    #[allow(clippy::needless_range_loop)]
    for i in 0..n_vertices {
        let x = mesh.base_xy[i * 2];
        let y = mesh.base_xy[i * 2 + 1];
        let z = (sanitize(kf0.z[i]) * opts.z_scale) as f32;
        let (u0, v0) = flow0[i];
        let tip_x = x + arrow_scale * u0 as f32;
        let tip_y = y + arrow_scale * v0 as f32;
        let tip_z = z;
        // tail
        base_pos.extend_from_slice(&x.to_le_bytes());
        base_pos.extend_from_slice(&y.to_le_bytes());
        base_pos.extend_from_slice(&z.to_le_bytes());
        update_bounds(x, y, z);
        // tip
        base_pos.extend_from_slice(&tip_x.to_le_bytes());
        base_pos.extend_from_slice(&tip_y.to_le_bytes());
        base_pos.extend_from_slice(&tip_z.to_le_bytes());
        update_bounds(tip_x, tip_y, tip_z);
        indices.push((i * 2) as u32);
        indices.push((i * 2 + 1) as u32);
    }
    let bv_pos = b.add_buffer_view(&base_pos, Some(BV_TARGET_ARRAY));
    let acc_pos = b.add_accessor_vec3_f32(
        bv_pos,
        n_vertices * 2,
        Some(([min_x, min_y, min_z], [max_x, max_y, max_z])),
    );

    let mut idx_bytes = Vec::with_capacity(indices.len() * 4);
    for i in &indices {
        idx_bytes.extend_from_slice(&i.to_le_bytes());
    }
    let bv_idx = b.add_buffer_view(&idx_bytes, Some(BV_TARGET_ELEMENT_ARRAY));
    let acc_idx = b.add_accessor_scalar_u32(bv_idx, indices.len());

    // Per-keyframe morphs: tail delta = 0, tip delta = arrow_scale*(u_k - u_0, v_k - v_0, 0).
    let mut morphs: Vec<MorphAttrs> = Vec::with_capacity(n_keyframes.saturating_sub(1));
    for k in 1..n_keyframes {
        let kf = &mesh.keyframes[k];
        let flow_k = kf
            .flow_uv
            .as_ref()
            .expect("arrows requires flow_uv on every keyframe");
        let mut dpos = Vec::with_capacity(n_vertices * 2 * 3 * 4);
        let (mut min_du, mut max_du) = (0.0f32, 0.0f32);
        let (mut min_dv, mut max_dv) = (0.0f32, 0.0f32);
        for i in 0..n_vertices {
            // tail delta — all zero.
            dpos.extend_from_slice(&0f32.to_le_bytes());
            dpos.extend_from_slice(&0f32.to_le_bytes());
            dpos.extend_from_slice(&0f32.to_le_bytes());
            // tip delta
            let (u0, v0) = flow0[i];
            let (uk, vk) = flow_k[i];
            let du = arrow_scale * (uk - u0) as f32;
            let dv = arrow_scale * (vk - v0) as f32;
            dpos.extend_from_slice(&du.to_le_bytes());
            dpos.extend_from_slice(&dv.to_le_bytes());
            dpos.extend_from_slice(&0f32.to_le_bytes());
            min_du = min_du.min(du);
            max_du = max_du.max(du);
            min_dv = min_dv.min(dv);
            max_dv = max_dv.max(dv);
        }
        let bv = b.add_buffer_view(&dpos, Some(BV_TARGET_ARRAY));
        let acc_dpos = b.add_accessor_vec3_f32(
            bv,
            n_vertices * 2,
            Some(([min_du, min_dv, 0.0], [max_du, max_dv, 0.0])),
        );
        morphs.push(MorphAttrs {
            position: Some(acc_dpos),
            color0: None,
        });
    }

    let weights = if n_keyframes > 1 {
        Some(vec![0.0f32; n_keyframes - 1])
    } else {
        None
    };
    let prim = b.add_primitive(
        PrimitiveAttrs {
            position: Some(acc_pos),
            normal: None,
            color0: None,
        },
        Some(acc_idx),
        PrimitiveMode::Lines,
        morphs,
    );
    let mesh_idx = b.add_mesh(vec![prim], weights);
    b.add_node(
        Some(mesh_idx),
        None,
        Some(json!({
            "kind": "flow-arrows",
            "arrow_scale": opts.arrow_scale,
        })),
    )
}

#[inline]
fn sanitize(v: f64) -> f64 {
    if v.is_finite() {
        v
    } else {
        0.0
    }
}
