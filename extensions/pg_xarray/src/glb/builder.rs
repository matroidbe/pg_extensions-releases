//! In-memory glTF 2.0 scene graph.
//!
//! `GlbSceneBuilder` accumulates buffer bytes, accessors, bufferViews,
//! primitives, meshes, nodes, scenes, and animations. The public API
//! is intentionally small — pg_xarray's `glb/builder.rs` callers add
//! one accessor at a time and the builder threads the indices.
//!
//! `build()` lives in [`super::encoder`] and turns the accumulated
//! state into a GLB byte sequence (12-byte header + JSON chunk + BIN
//! chunk, 4-byte aligned per the spec).

use serde_json::{json, Value};

// glTF componentType constants
const COMP_U8: u32 = 5121;
const COMP_U32: u32 = 5125;
const COMP_F32: u32 = 5126;

// glTF primitive mode constants (only the two we use)
#[derive(Clone, Copy, Debug)]
pub enum PrimitiveMode {
    Triangles = 4,
    Lines = 1,
}

/// Attributes referenced by accessor index.
#[derive(Clone, Debug, Default)]
pub struct PrimitiveAttrs {
    pub position: Option<u32>,
    pub normal: Option<u32>,
    pub color0: Option<u32>,
}

/// Morph-target deltas referenced by accessor index. Per the glTF 2.0
/// spec, morph-delta accessors for `POSITION` must be VEC3 f32 and
/// morph-delta accessors for `COLOR_0` must be VEC4 f32 (normalized
/// integer types are forbidden on morph deltas).
#[derive(Clone, Debug, Default)]
pub struct MorphAttrs {
    pub position: Option<u32>,
    pub color0: Option<u32>,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // `Linear` is forward-compat builder API.
pub enum SamplerInterp {
    Step,
    Linear,
}

impl SamplerInterp {
    fn as_str(self) -> &'static str {
        match self {
            SamplerInterp::Step => "STEP",
            SamplerInterp::Linear => "LINEAR",
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Translation/Rotation/Scale are forward-compat builder API.
pub enum AnimPath {
    Weights,
    Translation,
    Rotation,
    Scale,
}

impl AnimPath {
    fn as_str(self) -> &'static str {
        match self {
            AnimPath::Weights => "weights",
            AnimPath::Translation => "translation",
            AnimPath::Rotation => "rotation",
            AnimPath::Scale => "scale",
        }
    }
}

#[derive(Clone, Debug)]
pub struct AnimSampler {
    pub input: u32,  // accessor: keyframe times (SCALAR f32)
    pub output: u32, // accessor: values at each keyframe
    pub interpolation: SamplerInterp,
}

#[derive(Clone, Debug)]
pub struct AnimChannel {
    pub sampler: u32,
    pub target_node: u32,
    pub target_path: AnimPath,
}

// ---------------------------------------------------------------------------
// Internal records
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(super) struct BufferView {
    pub byte_offset: usize,
    pub byte_length: usize,
    pub target: Option<u32>,
}

#[derive(Clone, Debug)]
pub(super) struct Accessor {
    pub buffer_view: u32,
    pub component_type: u32,
    pub kind: &'static str, // "SCALAR" | "VEC2" | "VEC3" | "VEC4"
    pub count: usize,
    pub normalized: bool,
    pub min_max: Option<(Vec<f32>, Vec<f32>)>,
}

#[derive(Clone, Debug)]
pub(super) struct Primitive {
    pub attrs: PrimitiveAttrs,
    pub indices: Option<u32>,
    pub mode: PrimitiveMode,
    pub targets: Vec<MorphAttrs>,
}

#[derive(Clone, Debug)]
pub(super) struct Mesh {
    pub primitives: Vec<u32>,
    pub weights: Option<Vec<f32>>,
}

#[derive(Clone, Debug)]
pub(super) struct Node {
    pub mesh: Option<u32>,
    pub matrix: Option<[f32; 16]>,
    pub extras: Option<Value>,
}

#[derive(Clone, Debug)]
pub(super) struct Scene {
    pub nodes: Vec<u32>,
}

#[derive(Clone, Debug)]
pub(super) struct Animation {
    pub samplers: Vec<AnimSampler>,
    pub channels: Vec<AnimChannel>,
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

pub struct GlbSceneBuilder {
    pub(super) generator: String,
    pub(super) bin: Vec<u8>,
    pub(super) buffer_views: Vec<BufferView>,
    pub(super) accessors: Vec<Accessor>,
    pub(super) primitives: Vec<Primitive>,
    pub(super) meshes: Vec<Mesh>,
    pub(super) nodes: Vec<Node>,
    pub(super) scenes: Vec<Scene>,
    pub(super) animations: Vec<Animation>,
    pub(super) asset_extras: Option<Value>,
}

impl GlbSceneBuilder {
    pub fn new(generator: &str) -> Self {
        Self {
            generator: generator.to_string(),
            bin: Vec::new(),
            buffer_views: Vec::new(),
            accessors: Vec::new(),
            primitives: Vec::new(),
            meshes: Vec::new(),
            nodes: Vec::new(),
            scenes: Vec::new(),
            animations: Vec::new(),
            asset_extras: None,
        }
    }

    pub fn set_asset_extras(&mut self, extras: Value) {
        self.asset_extras = Some(extras);
    }

    /// Append `data` to the BIN buffer, padding so the next view also
    /// starts 4-byte aligned. Returns the new bufferView index.
    pub fn add_buffer_view(&mut self, data: &[u8], target: Option<u32>) -> u32 {
        let byte_offset = self.bin.len();
        self.bin.extend_from_slice(data);
        // Pad to 4-byte alignment for the NEXT view (per glTF spec).
        while !self.bin.len().is_multiple_of(4) {
            self.bin.push(0);
        }
        let bv = BufferView {
            byte_offset,
            byte_length: data.len(),
            target,
        };
        self.buffer_views.push(bv);
        (self.buffer_views.len() - 1) as u32
    }

    fn add_accessor(&mut self, a: Accessor) -> u32 {
        self.accessors.push(a);
        (self.accessors.len() - 1) as u32
    }

    pub fn add_accessor_vec3_f32(
        &mut self,
        bv: u32,
        count: usize,
        minmax: Option<([f32; 3], [f32; 3])>,
    ) -> u32 {
        let mm = minmax.map(|(mn, mx)| (mn.to_vec(), mx.to_vec()));
        self.add_accessor(Accessor {
            buffer_view: bv,
            component_type: COMP_F32,
            kind: "VEC3",
            count,
            normalized: false,
            min_max: mm,
        })
    }

    pub fn add_accessor_vec4_u8_norm(&mut self, bv: u32, count: usize) -> u32 {
        self.add_accessor(Accessor {
            buffer_view: bv,
            component_type: COMP_U8,
            kind: "VEC4",
            count,
            normalized: true,
            min_max: None,
        })
    }

    pub fn add_accessor_vec4_f32(&mut self, bv: u32, count: usize) -> u32 {
        self.add_accessor(Accessor {
            buffer_view: bv,
            component_type: COMP_F32,
            kind: "VEC4",
            count,
            normalized: false,
            min_max: None,
        })
    }

    pub fn add_accessor_scalar_u32(&mut self, bv: u32, count: usize) -> u32 {
        self.add_accessor(Accessor {
            buffer_view: bv,
            component_type: COMP_U32,
            kind: "SCALAR",
            count,
            normalized: false,
            min_max: None,
        })
    }

    pub fn add_accessor_scalar_f32(
        &mut self,
        bv: u32,
        count: usize,
        minmax: Option<(f32, f32)>,
    ) -> u32 {
        let mm = minmax.map(|(mn, mx)| (vec![mn], vec![mx]));
        self.add_accessor(Accessor {
            buffer_view: bv,
            component_type: COMP_F32,
            kind: "SCALAR",
            count,
            normalized: false,
            min_max: mm,
        })
    }

    pub fn add_primitive(
        &mut self,
        attrs: PrimitiveAttrs,
        indices: Option<u32>,
        mode: PrimitiveMode,
        targets: Vec<MorphAttrs>,
    ) -> u32 {
        self.primitives.push(Primitive {
            attrs,
            indices,
            mode,
            targets,
        });
        (self.primitives.len() - 1) as u32
    }

    pub fn add_mesh(&mut self, primitives: Vec<u32>, weights: Option<Vec<f32>>) -> u32 {
        self.meshes.push(Mesh {
            primitives,
            weights,
        });
        (self.meshes.len() - 1) as u32
    }

    pub fn add_node(
        &mut self,
        mesh: Option<u32>,
        matrix: Option<[f32; 16]>,
        extras: Option<Value>,
    ) -> u32 {
        self.nodes.push(Node {
            mesh,
            matrix,
            extras,
        });
        (self.nodes.len() - 1) as u32
    }

    pub fn add_scene(&mut self, root_nodes: Vec<u32>) -> u32 {
        self.scenes.push(Scene { nodes: root_nodes });
        (self.scenes.len() - 1) as u32
    }

    pub fn add_animation(&mut self, samplers: Vec<AnimSampler>, channels: Vec<AnimChannel>) {
        self.animations.push(Animation { samplers, channels });
    }

    pub fn build(self) -> Vec<u8> {
        super::encoder::encode(self)
    }
}

// ---------------------------------------------------------------------------
// JSON assembly — called by encoder.rs after the BIN buffer is final.
// ---------------------------------------------------------------------------

pub(super) fn json_for(b: &GlbSceneBuilder) -> Value {
    let asset = if let Some(extras) = &b.asset_extras {
        json!({
            "version": "2.0",
            "generator": b.generator,
            "extras": extras,
        })
    } else {
        json!({
            "version": "2.0",
            "generator": b.generator,
        })
    };

    let buffer_views: Vec<Value> = b
        .buffer_views
        .iter()
        .map(|bv| {
            let mut o = json!({
                "buffer": 0,
                "byteOffset": bv.byte_offset,
                "byteLength": bv.byte_length,
            });
            if let Some(t) = bv.target {
                o["target"] = json!(t);
            }
            o
        })
        .collect();

    let accessors: Vec<Value> = b
        .accessors
        .iter()
        .map(|a| {
            let mut o = json!({
                "bufferView": a.buffer_view,
                "componentType": a.component_type,
                "type": a.kind,
                "count": a.count,
            });
            if a.normalized {
                o["normalized"] = json!(true);
            }
            if let Some((mn, mx)) = &a.min_max {
                o["min"] = json!(mn);
                o["max"] = json!(mx);
            }
            o
        })
        .collect();

    let primitives: Vec<Value> = b
        .primitives
        .iter()
        .map(|p| {
            let mut attrs = serde_json::Map::new();
            if let Some(pos) = p.attrs.position {
                attrs.insert("POSITION".into(), json!(pos));
            }
            if let Some(nrm) = p.attrs.normal {
                attrs.insert("NORMAL".into(), json!(nrm));
            }
            if let Some(c0) = p.attrs.color0 {
                attrs.insert("COLOR_0".into(), json!(c0));
            }
            let mut o = json!({
                "attributes": Value::Object(attrs),
                "mode": p.mode as u32,
            });
            if let Some(idx) = p.indices {
                o["indices"] = json!(idx);
            }
            if !p.targets.is_empty() {
                let targets: Vec<Value> = p
                    .targets
                    .iter()
                    .map(|t| {
                        let mut m = serde_json::Map::new();
                        if let Some(pos) = t.position {
                            m.insert("POSITION".into(), json!(pos));
                        }
                        if let Some(c0) = t.color0 {
                            m.insert("COLOR_0".into(), json!(c0));
                        }
                        Value::Object(m)
                    })
                    .collect();
                o["targets"] = json!(targets);
            }
            o
        })
        .collect();

    // glTF wants meshes that reference primitive indices, but the
    // primitives in our flat list need to be inlined inside their
    // owning mesh. We materialise that here.
    let meshes: Vec<Value> = b
        .meshes
        .iter()
        .map(|m| {
            let prims: Vec<Value> = m
                .primitives
                .iter()
                .map(|i| primitives[*i as usize].clone())
                .collect();
            let mut o = json!({ "primitives": prims });
            if let Some(w) = &m.weights {
                o["weights"] = json!(w);
            }
            o
        })
        .collect();

    let nodes: Vec<Value> = b
        .nodes
        .iter()
        .map(|n| {
            let mut o = serde_json::Map::new();
            if let Some(m) = n.mesh {
                o.insert("mesh".into(), json!(m));
            }
            if let Some(mx) = n.matrix {
                o.insert("matrix".into(), json!(mx.to_vec()));
            }
            if let Some(e) = &n.extras {
                o.insert("extras".into(), e.clone());
            }
            Value::Object(o)
        })
        .collect();

    let scenes: Vec<Value> = b
        .scenes
        .iter()
        .map(|s| json!({ "nodes": s.nodes }))
        .collect();

    let animations: Vec<Value> = b
        .animations
        .iter()
        .map(|a| {
            let samplers: Vec<Value> = a
                .samplers
                .iter()
                .map(|s| {
                    json!({
                        "input": s.input,
                        "output": s.output,
                        "interpolation": s.interpolation.as_str(),
                    })
                })
                .collect();
            let channels: Vec<Value> = a
                .channels
                .iter()
                .map(|c| {
                    json!({
                        "sampler": c.sampler,
                        "target": {
                            "node": c.target_node,
                            "path": c.target_path.as_str(),
                        }
                    })
                })
                .collect();
            json!({ "samplers": samplers, "channels": channels })
        })
        .collect();

    let mut root = json!({
        "asset": asset,
        "scene": 0,
        "scenes": scenes,
        "nodes": nodes,
        "meshes": meshes,
        "accessors": accessors,
        "bufferViews": buffer_views,
        "buffers": [{ "byteLength": b.bin.len() }],
    });
    if !animations.is_empty() {
        root["animations"] = json!(animations);
    }
    root
}
