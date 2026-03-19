use super::entity::{EntityStore, IfcEntity, IfcValue};
use crate::ffi;
use crate::ffi::OcctShape;

/// Convert an IFC element's geometry to an OCCT shape.
/// Follows: IfcProduct -> IfcProductDefinitionShape -> IfcShapeRepresentation -> geometry items.
/// Then applies IfcLocalPlacement transforms.
pub fn element_to_shape(
    store: &EntityStore,
    element: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    // IfcProduct: attr 5 = ObjectPlacement, attr 6 = Representation
    let rep_id = match element.attributes.get(6).and_then(|v| v.as_ref()) {
        Some(id) => id,
        None => return Ok(None),
    };

    let shape = resolve_product_def_shape(store, rep_id)?;
    let mut shape = match shape {
        Some(s) => s,
        None => return Ok(None),
    };

    // Apply placement chain
    if let Some(placement_id) = element.attributes.get(5).and_then(|v| v.as_ref()) {
        let matrix = resolve_placement_chain(store, placement_id)?;
        if let Some(mat) = matrix {
            shape = ffi::transform::transform(&shape, &mat)?;
        }
    }

    Ok(Some(shape))
}

/// Resolve IfcProductDefinitionShape -> IfcShapeRepresentation(s) -> geometry.
fn resolve_product_def_shape(
    store: &EntityStore,
    pds_id: u64,
) -> Result<Option<OcctShape>, String> {
    let pds = match store.get(pds_id) {
        Some(e) => e,
        None => return Ok(None),
    };

    // IfcProductDefinitionShape: attr 2 = Representations (list of IfcShapeRepresentation refs)
    let reps = match pds.attributes.get(2).and_then(|v| v.as_list()) {
        Some(list) => list,
        None => return Ok(None),
    };

    // Prefer "Body" representation
    let mut body_rep = None;
    let mut fallback_rep = None;

    for rep_val in reps {
        let rep_id = match rep_val.as_ref() {
            Some(id) => id,
            None => continue,
        };
        let rep = match store.get(rep_id) {
            Some(e) => e,
            None => continue,
        };

        // IfcShapeRepresentation: attr 1 = RepresentationIdentifier (e.g., "Body")
        let identifier = rep.attributes.get(1).and_then(|v| v.as_str()).unwrap_or("");

        if identifier.eq_ignore_ascii_case("Body") {
            body_rep = Some(rep);
            break;
        }
        if fallback_rep.is_none() {
            fallback_rep = Some(rep);
        }
    }

    let rep = body_rep.or(fallback_rep);
    let rep = match rep {
        Some(r) => r,
        None => return Ok(None),
    };

    resolve_shape_representation(store, rep)
}

/// Resolve IfcShapeRepresentation -> geometry items.
fn resolve_shape_representation(
    store: &EntityStore,
    rep: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    // IfcShapeRepresentation: attr 3 = Items (list of geometry item refs)
    let items = match rep.attributes.get(3).and_then(|v| v.as_list()) {
        Some(list) => list,
        None => return Ok(None),
    };

    let mut shapes: Vec<OcctShape> = Vec::new();

    for item_val in items {
        let item_id = match item_val.as_ref() {
            Some(id) => id,
            None => continue,
        };
        let item = match store.get(item_id) {
            Some(e) => e,
            None => continue,
        };

        match geometry_item_to_shape(store, item) {
            Ok(Some(shape)) => shapes.push(shape),
            Ok(None) => {}
            Err(_) => {} // Skip failed geometry items silently
        }
    }

    match shapes.len() {
        0 => Ok(None),
        1 => Ok(Some(shapes.into_iter().next().unwrap())),
        _ => {
            // Multiple items — compound them
            let refs: Vec<&OcctShape> = shapes.iter().collect();
            let compound = ffi::ifc_geom::make_compound(&refs)?;
            Ok(Some(compound))
        }
    }
}

/// Convert a single IFC geometry item to an OCCT shape.
fn geometry_item_to_shape(
    store: &EntityStore,
    item: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    match item.type_name.as_str() {
        "IFCEXTRUDEDAREASOLID" => convert_extruded_area_solid(store, item),
        "IFCFACETEDBREP" => convert_faceted_brep(store, item),
        "IFCBOOLEANCLIPPINGRESULT" | "IFCBOOLEANRESULT" => convert_boolean_result(store, item, 0),
        "IFCMAPPEDITEM" => convert_mapped_item(store, item),
        _ => Ok(None), // Unsupported geometry type
    }
}

// ── IfcExtrudedAreaSolid ────────────────────────────────────────────

/// IfcExtrudedAreaSolid: attr 0=SweptArea, attr 1=Position, attr 2=ExtrudedDirection, attr 3=Depth
fn convert_extruded_area_solid(
    store: &EntityStore,
    entity: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    let profile_id = entity
        .attributes
        .first()
        .and_then(|v| v.as_ref())
        .ok_or("IfcExtrudedAreaSolid: missing SweptArea")?;
    let profile = store
        .get(profile_id)
        .ok_or("IfcExtrudedAreaSolid: SweptArea not found")?;

    let direction_id = entity
        .attributes
        .get(2)
        .and_then(|v| v.as_ref())
        .ok_or("IfcExtrudedAreaSolid: missing ExtrudedDirection")?;
    let dir = resolve_direction(store, direction_id)?;

    let depth = entity
        .attributes
        .get(3)
        .and_then(|v| v.as_f64())
        .ok_or("IfcExtrudedAreaSolid: missing Depth")?;

    let face = match profile_to_face(store, profile)? {
        Some(f) => f,
        None => return Ok(None),
    };

    // Extrude: direction * depth
    let extrusion = [dir[0] * depth, dir[1] * depth, dir[2] * depth];
    let mut solid = ffi::ifc_geom::extrude_face(&face, extrusion[0], extrusion[1], extrusion[2])?;

    // Apply position transform (attr 1)
    if let Some(pos_id) = entity.attributes.get(1).and_then(|v| v.as_ref()) {
        if let Some(mat) = resolve_axis2_placement_3d(store, pos_id)? {
            solid = ffi::transform::transform(&solid, &mat)?;
        }
    }

    Ok(Some(solid))
}

/// Convert an IfcProfileDef to an OCCT face.
fn profile_to_face(store: &EntityStore, profile: &IfcEntity) -> Result<Option<OcctShape>, String> {
    match profile.type_name.as_str() {
        "IFCRECTANGLEPROFILEDEF" => {
            // attr 0=ProfileType, attr 1=ProfileName, attr 2=Position, attr 3=XDim, attr 4=YDim
            let x_dim = profile
                .attributes
                .get(3)
                .and_then(|v| v.as_f64())
                .ok_or("IfcRectangleProfileDef: missing XDim")?;
            let y_dim = profile
                .attributes
                .get(4)
                .and_then(|v| v.as_f64())
                .ok_or("IfcRectangleProfileDef: missing YDim")?;
            let mut face = ffi::ifc_geom::make_rectangle_face(x_dim, y_dim)?;

            // Apply profile position (IfcAxis2Placement2D) if present
            if let Some(pos_id) = profile.attributes.get(2).and_then(|v| v.as_ref()) {
                if let Some(mat) = resolve_axis2_placement_2d_as_3d(store, pos_id)? {
                    face = ffi::transform::transform(&face, &mat)?;
                }
            }
            Ok(Some(face))
        }
        "IFCCIRCLEPROFILEDEF" => {
            // attr 0=ProfileType, attr 1=ProfileName, attr 2=Position, attr 3=Radius
            let radius = profile
                .attributes
                .get(3)
                .and_then(|v| v.as_f64())
                .ok_or("IfcCircleProfileDef: missing Radius")?;
            let mut face = ffi::ifc_geom::make_circle_face(radius)?;

            if let Some(pos_id) = profile.attributes.get(2).and_then(|v| v.as_ref()) {
                if let Some(mat) = resolve_axis2_placement_2d_as_3d(store, pos_id)? {
                    face = ffi::transform::transform(&face, &mat)?;
                }
            }
            Ok(Some(face))
        }
        "IFCARBITRARYCLOSEDPROFILEDEF" => {
            // attr 0=ProfileType, attr 1=ProfileName, attr 2=OuterCurve
            let curve_id = profile
                .attributes
                .get(2)
                .and_then(|v| v.as_ref())
                .ok_or("IfcArbitraryClosedProfileDef: missing OuterCurve")?;
            let points = resolve_polyline_points(store, curve_id)?;
            if points.len() < 3 {
                return Ok(None);
            }
            let face = ffi::ifc_geom::make_polygon_face(&points)?;
            Ok(Some(face))
        }
        _ => Ok(None), // Unsupported profile type
    }
}

// ── IfcFacetedBrep ──────────────────────────────────────────────────

fn convert_faceted_brep(
    store: &EntityStore,
    entity: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    // attr 0 = Outer (IfcClosedShell)
    let shell_id = entity
        .attributes
        .first()
        .and_then(|v| v.as_ref())
        .ok_or("IfcFacetedBrep: missing Outer")?;
    let shell = store
        .get(shell_id)
        .ok_or("IfcFacetedBrep: outer shell not found")?;

    // IfcClosedShell: attr 0 = CfsFaces (list of IfcFace refs)
    let faces_list = shell
        .attributes
        .first()
        .and_then(|v| v.as_list())
        .ok_or("IfcClosedShell: missing faces")?;

    let mut all_faces: Vec<Vec<[f64; 3]>> = Vec::new();

    for face_val in faces_list {
        let face_id = match face_val.as_ref() {
            Some(id) => id,
            None => continue,
        };
        let face = match store.get(face_id) {
            Some(e) => e,
            None => continue,
        };

        // IfcFace: attr 0 = Bounds (list of IfcFaceBound refs)
        let bounds = match face.attributes.first().and_then(|v| v.as_list()) {
            Some(list) => list,
            None => continue,
        };

        // Take the first (outer) bound
        if let Some(bound_val) = bounds.first() {
            if let Some(bound_id) = bound_val.as_ref() {
                if let Some(bound) = store.get(bound_id) {
                    // IfcFaceBound: attr 0 = Bound (IfcPolyLoop), attr 1 = Orientation
                    if let Some(loop_id) = bound.attributes.first().and_then(|v| v.as_ref()) {
                        if let Some(poly_loop) = store.get(loop_id) {
                            // IfcPolyLoop: attr 0 = Polygon (list of IfcCartesianPoint refs)
                            if let Some(point_refs) =
                                poly_loop.attributes.first().and_then(|v| v.as_list())
                            {
                                let mut points = Vec::new();
                                for pt_val in point_refs {
                                    if let Some(pt_id) = pt_val.as_ref() {
                                        if let Some(pt) = store.get(pt_id) {
                                            if let Ok(coords) = resolve_cartesian_point(pt) {
                                                points.push(coords);
                                            }
                                        }
                                    }
                                }
                                if points.len() >= 3 {
                                    all_faces.push(points);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if all_faces.is_empty() {
        return Ok(None);
    }

    ffi::ifc_geom::make_faceted_solid(&all_faces).map(Some)
}

// ── IfcBooleanResult ────────────────────────────────────────────────

fn convert_boolean_result(
    store: &EntityStore,
    entity: &IfcEntity,
    depth: u32,
) -> Result<Option<OcctShape>, String> {
    if depth > 20 {
        return Err("IfcBooleanResult: recursion depth exceeded".into());
    }

    // attr 0 = Operator (.UNION. / .INTERSECTION. / .DIFFERENCE.)
    // attr 1 = FirstOperand, attr 2 = SecondOperand
    let operator = entity
        .attributes
        .first()
        .and_then(|v| v.as_enum())
        .ok_or("IfcBooleanResult: missing Operator")?;

    let first_id = entity
        .attributes
        .get(1)
        .and_then(|v| v.as_ref())
        .ok_or("IfcBooleanResult: missing FirstOperand")?;
    let second_id = entity
        .attributes
        .get(2)
        .and_then(|v| v.as_ref())
        .ok_or("IfcBooleanResult: missing SecondOperand")?;

    let first_entity = store
        .get(first_id)
        .ok_or("IfcBooleanResult: first operand not found")?;
    let second_entity = store
        .get(second_id)
        .ok_or("IfcBooleanResult: second operand not found")?;

    let first_shape = resolve_boolean_operand(store, first_entity, depth + 1)?
        .ok_or("IfcBooleanResult: first operand has no geometry")?;
    let second_shape = resolve_boolean_operand(store, second_entity, depth + 1)?
        .ok_or("IfcBooleanResult: second operand has no geometry")?;

    let result = match operator {
        "UNION" => ffi::boolean::fuse(&first_shape, &second_shape)?,
        "INTERSECTION" => ffi::boolean::common(&first_shape, &second_shape)?,
        "DIFFERENCE" => ffi::boolean::cut(&first_shape, &second_shape)?,
        _ => return Err(format!("IfcBooleanResult: unknown operator {operator}")),
    };

    Ok(Some(result))
}

fn resolve_boolean_operand(
    store: &EntityStore,
    entity: &IfcEntity,
    depth: u32,
) -> Result<Option<OcctShape>, String> {
    match entity.type_name.as_str() {
        "IFCEXTRUDEDAREASOLID" => convert_extruded_area_solid(store, entity),
        "IFCBOOLEANCLIPPINGRESULT" | "IFCBOOLEANRESULT" => {
            convert_boolean_result(store, entity, depth)
        }
        "IFCFACETEDBREP" => convert_faceted_brep(store, entity),
        "IFCHALFSPACESOLID" => convert_half_space(store, entity),
        "IFCPOLYGONALBOUNDEDHALFSPACE" => convert_half_space(store, entity),
        _ => Ok(None),
    }
}

fn convert_half_space(
    _store: &EntityStore,
    entity: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    // IfcHalfSpaceSolid: attr 0 = BaseSurface (IfcPlane), attr 1 = AgreementFlag
    // For now, approximate half-space as a very large box
    // This is a simplification — proper half-space clipping would need OCCT half-space
    let _surface_id = entity.attributes.first().and_then(|v| v.as_ref());
    let _agreement = entity
        .attributes
        .get(1)
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    // Create a large box as approximation (100m cube)
    let face = ffi::ifc_geom::make_rectangle_face(100000.0, 100000.0)?;
    let solid = ffi::ifc_geom::extrude_face(&face, 0.0, 0.0, 100000.0)?;
    Ok(Some(solid))
}

// ── IfcMappedItem ───────────────────────────────────────────────────

fn convert_mapped_item(
    store: &EntityStore,
    entity: &IfcEntity,
) -> Result<Option<OcctShape>, String> {
    // attr 0 = MappingSource (IfcRepresentationMap), attr 1 = MappingTarget (transform)
    let source_id = entity
        .attributes
        .first()
        .and_then(|v| v.as_ref())
        .ok_or("IfcMappedItem: missing MappingSource")?;
    let source = store
        .get(source_id)
        .ok_or("IfcMappedItem: MappingSource not found")?;

    // IfcRepresentationMap: attr 0 = MappingOrigin, attr 1 = MappedRepresentation
    let rep_id = source
        .attributes
        .get(1)
        .and_then(|v| v.as_ref())
        .ok_or("IfcRepresentationMap: missing MappedRepresentation")?;
    let rep = store
        .get(rep_id)
        .ok_or("IfcRepresentationMap: MappedRepresentation not found")?;

    let shape = match resolve_shape_representation(store, rep)? {
        Some(s) => s,
        None => return Ok(None),
    };

    // Apply MappingTarget transform (IfcCartesianTransformationOperator3D)
    if let Some(target_id) = entity.attributes.get(1).and_then(|v| v.as_ref()) {
        if let Some(target) = store.get(target_id) {
            if let Some(mat) = resolve_transform_operator(store, target)? {
                return Ok(Some(ffi::transform::transform(&shape, &mat)?));
            }
        }
    }

    // Apply MappingOrigin transform
    if let Some(origin_id) = source.attributes.first().and_then(|v| v.as_ref()) {
        if let Some(mat) = resolve_axis2_placement_3d(store, origin_id)? {
            return Ok(Some(ffi::transform::transform(&shape, &mat)?));
        }
    }

    Ok(Some(shape))
}

// ── Coordinate/Placement Resolution ─────────────────────────────────

/// Extract [x, y, z] from IfcCartesianPoint.
fn resolve_cartesian_point(entity: &IfcEntity) -> Result<[f64; 3], String> {
    let coords = entity
        .attributes
        .first()
        .and_then(|v| v.as_list())
        .ok_or("IfcCartesianPoint: missing Coordinates")?;
    let x = coords.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
    let y = coords.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let z = coords.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
    Ok([x, y, z])
}

/// Extract [dx, dy, dz] from IfcDirection.
fn resolve_direction(store: &EntityStore, dir_id: u64) -> Result<[f64; 3], String> {
    let entity = store.get(dir_id).ok_or("direction entity not found")?;
    let ratios = entity
        .attributes
        .first()
        .and_then(|v| v.as_list())
        .ok_or("IfcDirection: missing DirectionRatios")?;
    let dx = ratios.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dy = ratios.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dz = ratios.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
    Ok([dx, dy, dz])
}

/// Resolve IfcAxis2Placement3D to a 4x3 affine matrix (stored as [f64; 12]).
fn resolve_axis2_placement_3d(
    store: &EntityStore,
    placement_id: u64,
) -> Result<Option<[f64; 12]>, String> {
    let entity = match store.get(placement_id) {
        Some(e) => e,
        None => return Ok(None),
    };

    // Not an axis placement? Skip
    if entity.type_name != "IFCAXIS2PLACEMENT3D" {
        return Ok(None);
    }

    // attr 0 = Location (IfcCartesianPoint)
    let location = match entity.attributes.first().and_then(|v| v.as_ref()) {
        Some(id) => {
            let pt = store
                .get(id)
                .ok_or("IfcAxis2Placement3D: Location not found")?;
            resolve_cartesian_point(pt)?
        }
        None => [0.0, 0.0, 0.0],
    };

    // attr 1 = Axis (Z direction, optional)
    let z_dir = match entity.attributes.get(1) {
        Some(IfcValue::Ref(id)) => resolve_direction(store, *id)?,
        _ => [0.0, 0.0, 1.0],
    };

    // attr 2 = RefDirection (X direction, optional)
    let x_dir = match entity.attributes.get(2) {
        Some(IfcValue::Ref(id)) => resolve_direction(store, *id)?,
        _ => [1.0, 0.0, 0.0],
    };

    Ok(Some(build_placement_matrix(location, z_dir, x_dir)))
}

/// Resolve IfcAxis2Placement2D as a 3D transform (Z=0).
fn resolve_axis2_placement_2d_as_3d(
    store: &EntityStore,
    placement_id: u64,
) -> Result<Option<[f64; 12]>, String> {
    let entity = match store.get(placement_id) {
        Some(e) => e,
        None => return Ok(None),
    };

    // IfcAxis2Placement2D: attr 0 = Location (2D point), attr 1 = RefDirection (2D direction)
    let location = match entity.attributes.first().and_then(|v| v.as_ref()) {
        Some(id) => {
            let pt = store
                .get(id)
                .ok_or("IfcAxis2Placement2D: Location not found")?;
            resolve_cartesian_point(pt)? // z=0 for 2D points
        }
        None => [0.0, 0.0, 0.0],
    };

    // If location is at origin and no ref direction, skip transform
    if location[0].abs() < 1e-10
        && location[1].abs() < 1e-10
        && location[2].abs() < 1e-10
        && entity
            .attributes
            .get(1)
            .map(|v| v.is_null())
            .unwrap_or(true)
    {
        return Ok(None);
    }

    let x_dir = match entity.attributes.get(1) {
        Some(IfcValue::Ref(id)) => resolve_direction(store, *id)?,
        _ => [1.0, 0.0, 0.0],
    };

    Ok(Some(build_placement_matrix(
        location,
        [0.0, 0.0, 1.0],
        x_dir,
    )))
}

/// Resolve IfcLocalPlacement chain to a 4x3 matrix.
fn resolve_placement_chain(
    store: &EntityStore,
    placement_id: u64,
) -> Result<Option<[f64; 12]>, String> {
    let entity = match store.get(placement_id) {
        Some(e) => e,
        None => return Ok(None),
    };

    if entity.type_name != "IFCLOCALPLACEMENT" {
        return Ok(None);
    }

    // IfcLocalPlacement: attr 0 = PlacementRelTo (optional), attr 1 = RelativePlacement
    let relative_matrix = match entity.attributes.get(1).and_then(|v| v.as_ref()) {
        Some(id) => resolve_axis2_placement_3d(store, id)?,
        None => None,
    };

    // Chain with parent placement
    let parent_matrix = match entity.attributes.first() {
        Some(IfcValue::Ref(parent_id)) => resolve_placement_chain(store, *parent_id)?,
        _ => None,
    };

    match (parent_matrix, relative_matrix) {
        (Some(parent), Some(relative)) => Ok(Some(compose_matrices(&parent, &relative))),
        (Some(m), None) | (None, Some(m)) => Ok(Some(m)),
        (None, None) => Ok(None),
    }
}

/// Resolve IfcCartesianTransformationOperator3D to a 4x3 matrix.
fn resolve_transform_operator(
    store: &EntityStore,
    entity: &IfcEntity,
) -> Result<Option<[f64; 12]>, String> {
    // IfcCartesianTransformationOperator3D:
    // attr 0 = Axis1 (X), attr 1 = Axis2 (Y), attr 2 = LocalOrigin, attr 3 = Scale, attr 4 = Axis3 (Z)
    let origin = match entity.attributes.get(2).and_then(|v| v.as_ref()) {
        Some(id) => {
            let pt = store.get(id).ok_or("transform: origin not found")?;
            resolve_cartesian_point(pt)?
        }
        None => [0.0, 0.0, 0.0],
    };

    let x_dir = match entity.attributes.first() {
        Some(IfcValue::Ref(id)) => resolve_direction(store, *id)?,
        _ => [1.0, 0.0, 0.0],
    };

    let z_dir = match entity.attributes.get(4) {
        Some(IfcValue::Ref(id)) => resolve_direction(store, *id)?,
        _ => [0.0, 0.0, 1.0],
    };

    let scale = entity
        .attributes
        .get(3)
        .and_then(|v| v.as_f64())
        .unwrap_or(1.0);

    let mut mat = build_placement_matrix(origin, z_dir, x_dir);
    // Apply scale to rotation part
    for i in 0..3 {
        for j in 0..3 {
            mat[i * 4 + j] *= scale;
        }
    }

    Ok(Some(mat))
}

/// Resolve IfcPolyline -> list of 3D points.
fn resolve_polyline_points(store: &EntityStore, curve_id: u64) -> Result<Vec<[f64; 3]>, String> {
    let curve = store.get(curve_id).ok_or("polyline not found")?;

    // IfcPolyline: attr 0 = Points (list of IfcCartesianPoint refs)
    let point_refs = curve
        .attributes
        .first()
        .and_then(|v| v.as_list())
        .ok_or("IfcPolyline: missing Points")?;

    let mut points = Vec::new();
    for pt_val in point_refs {
        if let Some(pt_id) = pt_val.as_ref() {
            if let Some(pt) = store.get(pt_id) {
                points.push(resolve_cartesian_point(pt)?);
            }
        }
    }

    Ok(points)
}

// ── Linear Algebra Helpers ──────────────────────────────────────────

fn cross(a: [f64; 3], b: [f64; 3]) -> [f64; 3] {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn normalize(v: [f64; 3]) -> [f64; 3] {
    let len = (v[0] * v[0] + v[1] * v[1] + v[2] * v[2]).sqrt();
    if len < 1e-15 {
        return v;
    }
    [v[0] / len, v[1] / len, v[2] / len]
}

/// Build a 4x3 row-major affine matrix from origin, Z-axis, and X-axis direction.
/// Layout: [Xx Xy Xz Tx | Yx Yy Yz Ty | Zx Zy Zz Tz] stored as [f64; 12].
/// OCCT's gp_Trsf expects column-major, but our ffi::transform uses row-major [4x3].
fn build_placement_matrix(origin: [f64; 3], z_dir: [f64; 3], x_dir: [f64; 3]) -> [f64; 12] {
    let z = normalize(z_dir);
    let y = normalize(cross(z, x_dir));
    let x = normalize(cross(y, z));

    [
        x[0], y[0], z[0], origin[0], x[1], y[1], z[1], origin[1], x[2], y[2], z[2], origin[2],
    ]
}

/// Compose two 4x3 affine matrices: result = parent * child.
fn compose_matrices(parent: &[f64; 12], child: &[f64; 12]) -> [f64; 12] {
    let mut result = [0.0f64; 12];

    // parent and child are 3x4 row-major: [R00 R01 R02 T0, R10 R11 R12 T1, R20 R21 R22 T2]
    // We treat them as 4x4 with implicit last row [0 0 0 1].

    for row in 0..3 {
        for col in 0..3 {
            result[row * 4 + col] = parent[row * 4] * child[col]
                + parent[row * 4 + 1] * child[4 + col]
                + parent[row * 4 + 2] * child[8 + col];
        }
        // Translation column
        result[row * 4 + 3] = parent[row * 4] * child[3]
            + parent[row * 4 + 1] * child[7]
            + parent[row * 4 + 2] * child[11]
            + parent[row * 4 + 3];
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cross_product() {
        let result = cross([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        assert!((result[2] - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_normalize() {
        let n = normalize([3.0, 4.0, 0.0]);
        assert!((n[0] - 0.6).abs() < 1e-10);
        assert!((n[1] - 0.8).abs() < 1e-10);
    }

    #[test]
    fn test_identity_placement_matrix() {
        let mat = build_placement_matrix([0.0, 0.0, 0.0], [0.0, 0.0, 1.0], [1.0, 0.0, 0.0]);
        assert!((mat[0] - 1.0).abs() < 1e-10); // Xx
        assert!((mat[5] - 1.0).abs() < 1e-10); // Yy
        assert!((mat[10] - 1.0).abs() < 1e-10); // Zz
        assert!(mat[3].abs() < 1e-10); // Tx
        assert!(mat[7].abs() < 1e-10); // Ty
        assert!(mat[11].abs() < 1e-10); // Tz
    }

    #[test]
    fn test_compose_translations() {
        let m1 = [1.0, 0.0, 0.0, 10.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0];
        let m2 = [1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 5.0, 0.0, 0.0, 1.0, 0.0];
        let result = compose_matrices(&m1, &m2);
        assert!((result[3] - 10.0).abs() < 1e-10); // Tx
        assert!((result[7] - 5.0).abs() < 1e-10); // Ty
    }
}
