pgrx::pg_module_magic!();

mod ffi;
mod functions;
mod gist;
mod ifc;
mod types;

pub use functions::aggregates::*;
pub use functions::boolean::*;
pub use functions::constructors::*;
pub use functions::explode::*;
pub use functions::export::*;
pub use functions::ifc::*;
pub use functions::measurements::*;
pub use functions::operators::*;
pub use functions::predicates::*;
pub use functions::transforms::*;
pub use gist::*;
pub use types::bbox3d::BBox3D;
pub use types::point3d::Point3D;
pub use types::solid::Solid;
pub use types::vec3d::Vec3D;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pgsolid, public").expect("SET search_path failed");
    }

    #[pg_test]
    fn test_solid_box_volume() {
        set_search_path();
        let result = Spi::get_one::<f64>("SELECT solid_volume(solid_box(5000, 200, 3000))")
            .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = 5000.0 * 200.0 * 3000.0;
        assert!(
            (volume - expected).abs() < 1.0,
            "box volume: expected {expected}, got {volume}"
        );
    }

    #[pg_test]
    fn test_solid_cylinder_volume() {
        set_search_path();
        let result = Spi::get_one::<f64>("SELECT solid_volume(solid_cylinder(150, 3000))")
            .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = std::f64::consts::PI * 150.0 * 150.0 * 3000.0;
        assert!(
            (volume - expected).abs() / expected < 1e-6,
            "cylinder volume: expected {expected}, got {volume}"
        );
    }

    #[pg_test]
    fn test_solid_sphere_volume() {
        set_search_path();
        let result =
            Spi::get_one::<f64>("SELECT solid_volume(solid_sphere(500))").expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = (4.0 / 3.0) * std::f64::consts::PI * 500.0_f64.powi(3);
        assert!(
            (volume - expected).abs() / expected < 1e-6,
            "sphere volume: expected {expected}, got {volume}"
        );
    }

    #[pg_test]
    fn test_solid_box_surface_area() {
        set_search_path();
        let result = Spi::get_one::<f64>("SELECT solid_surface_area(solid_box(100, 200, 300))")
            .expect("SPI failed");
        let area = result.expect("NULL result");
        let expected = 2.0 * (100.0 * 200.0 + 100.0 * 300.0 + 200.0 * 300.0);
        assert!(
            (area - expected).abs() < 1.0,
            "box surface area: expected {expected}, got {area}"
        );
    }

    #[pg_test]
    fn test_solid_box_bbox() {
        set_search_path();
        // Use the BBox3D type directly to check approximate values
        // (OCCT adds tiny tolerance to bounding boxes)
        let result = Spi::get_one::<crate::BBox3D>("SELECT solid_bbox(solid_box(100, 200, 300))")
            .expect("SPI failed");
        let bbox = result.expect("NULL result");
        assert!(bbox.xmin.abs() < 0.001, "xmin: {}", bbox.xmin);
        assert!(bbox.ymin.abs() < 0.001, "ymin: {}", bbox.ymin);
        assert!(bbox.zmin.abs() < 0.001, "zmin: {}", bbox.zmin);
        assert!((bbox.xmax - 100.0).abs() < 0.001, "xmax: {}", bbox.xmax);
        assert!((bbox.ymax - 200.0).abs() < 0.001, "ymax: {}", bbox.ymax);
        assert!((bbox.zmax - 300.0).abs() < 0.001, "zmax: {}", bbox.zmax);
    }

    #[pg_test]
    fn test_solid_brep_roundtrip() {
        set_search_path();
        let result = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_brep(solid_to_brep(solid_box(100, 200, 300))))",
        )
        .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (volume - expected).abs() < 1.0,
            "roundtrip volume: expected {expected}, got {volume}"
        );
    }

    #[pg_test]
    fn test_solid_is_valid() {
        set_search_path();
        let result = Spi::get_one::<bool>("SELECT solid_is_valid(solid_box(100, 200, 300))")
            .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_face_count_box() {
        set_search_path();
        let result = Spi::get_one::<i32>("SELECT solid_face_count(solid_box(100, 200, 300))")
            .expect("SPI failed");
        assert_eq!(result, Some(6), "a box has 6 faces");
    }

    #[pg_test]
    fn test_solid_centroid_box() {
        set_search_path();
        let result =
            Spi::get_one::<String>("SELECT solid_centroid(solid_box(100, 200, 300))::text")
                .expect("SPI failed");
        let centroid = result.expect("NULL result");
        assert_eq!(centroid, "POINT3D(50 100 150)");
    }

    #[pg_test]
    fn test_solid_cone_volume() {
        set_search_path();
        let result = Spi::get_one::<f64>("SELECT solid_volume(solid_cone(300, 0, 1000))")
            .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = (1.0 / 3.0) * std::f64::consts::PI * 300.0_f64.powi(2) * 1000.0;
        assert!(
            (volume - expected).abs() / expected < 1e-6,
            "cone volume: expected {expected}, got {volume}"
        );
    }

    // ===== Phase 2: Transforms =====

    #[pg_test]
    fn test_solid_translate_centroid() {
        set_search_path();
        let result = Spi::get_one::<crate::Point3D>(
            "SELECT solid_centroid(solid_translate(solid_box(100, 100, 100), 500, 300, 0))",
        )
        .expect("SPI failed");
        let pt = result.expect("NULL result");
        assert!(
            (pt.x - 550.0).abs() < 0.01
                && (pt.y - 350.0).abs() < 0.01
                && (pt.z - 50.0).abs() < 0.01,
            "centroid: expected ~(550,350,50), got ({},{},{})",
            pt.x,
            pt.y,
            pt.z
        );
    }

    #[pg_test]
    fn test_solid_translate_volume_unchanged() {
        set_search_path();
        let result = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_translate(solid_box(100, 200, 300), 1000, 2000, 3000))",
        )
        .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (volume - expected).abs() < 1.0,
            "translated volume: expected {expected}, got {volume}"
        );
    }

    #[pg_test]
    fn test_solid_rotate_volume_unchanged() {
        set_search_path();
        let result = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_rotate(solid_box(100, 200, 300), 0, 0, 1, 90))",
        )
        .expect("SPI failed");
        let volume = result.expect("NULL result");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (volume - expected).abs() < 1.0,
            "rotated volume: expected {expected}, got {volume}"
        );
    }

    // ===== Phase 2: Spatial Predicates =====

    #[pg_test]
    fn test_solid_distance_separated() {
        set_search_path();
        // Two boxes: [0,10] and [20,30] along X — gap of 10
        let result = Spi::get_one::<f64>(
            "SELECT solid_distance(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 20, 0, 0))",
        )
        .expect("SPI failed");
        let dist = result.expect("NULL result");
        assert!(
            (dist - 10.0).abs() < 0.1,
            "distance: expected ~10, got {dist}"
        );
    }

    #[pg_test]
    fn test_solid_distance_overlapping() {
        set_search_path();
        // Two boxes at the same position — distance 0
        let result =
            Spi::get_one::<f64>("SELECT solid_distance(solid_box(10,10,10), solid_box(10,10,10))")
                .expect("SPI failed");
        let dist = result.expect("NULL result");
        assert!(
            dist < 0.001,
            "overlapping distance: expected ~0, got {dist}"
        );
    }

    #[pg_test]
    fn test_solid_intersects_true() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_intersects(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true), "overlapping boxes should intersect");
    }

    #[pg_test]
    fn test_solid_intersects_false() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_intersects(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false), "separated boxes should not intersect");
    }

    #[pg_test]
    fn test_solid_contains_point_inside() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_contains_point(solid_box(100,100,100), 'POINT3D(50 50 50)'::point3d)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_contains_point_outside() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_contains_point(solid_box(100,100,100), 'POINT3D(200 50 50)'::point3d)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    // ===== Phase 2: Overlap Operator =====

    #[pg_test]
    fn test_overlap_operator_true() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_box(10,10,10) && solid_translate(solid_box(10,10,10), 5, 5, 5)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_overlap_operator_false() {
        set_search_path();
        let result = Spi::get_one::<bool>(
            "SELECT solid_box(10,10,10) && solid_translate(solid_box(10,10,10), 100, 100, 100)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    // ===== Phase 2: GiST Index =====

    #[pg_test]
    fn test_gist_index_creation() {
        set_search_path();
        Spi::run("CREATE TABLE test_gist(id serial, geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run(
            "INSERT INTO test_gist(geom) \
             SELECT pgsolid.solid_translate(pgsolid.solid_box(10,10,10), i*20.0, 0, 0) \
             FROM generate_series(1,50) AS s(i)",
        )
        .expect("INSERT");
        Spi::run("CREATE INDEX test_gist_idx ON test_gist USING gist(geom)")
            .expect("CREATE INDEX should succeed");
    }

    #[pg_test]
    fn test_gist_index_query() {
        set_search_path();
        Spi::run("CREATE TABLE test_gist2(id serial, geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run(
            "INSERT INTO test_gist2(geom) \
             SELECT pgsolid.solid_translate(pgsolid.solid_box(10,10,10), i*100.0, 0, 0) \
             FROM generate_series(1,100) AS s(i)",
        )
        .expect("INSERT");
        Spi::run("CREATE INDEX ON test_gist2 USING gist(geom)").expect("CREATE INDEX");

        // Query: find solids overlapping bbox [0,0,0 to 500,10,10]
        // Boxes at i*100 with width 10: boxes 1-5 (at 100..500) overlap
        let seq_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_gist2 WHERE geom && pgsolid.solid_box(500, 10, 10)",
        )
        .expect("SPI failed")
        .expect("NULL");

        // The query box [0,500]x[0,10]x[0,10] should overlap with
        // boxes at positions 100,200,300,400 (i=1..4, since box at 500 starts at 500+10=510 bbox)
        assert!(
            seq_count >= 4,
            "expected at least 4 matches, got {seq_count}"
        );
    }

    // ===== Phase 2 deferred: <-> Distance Operator =====

    #[pg_test]
    fn test_distance_operator_value() {
        set_search_path();
        // Two boxes: [0,10]^3 and [20,30]^3 — AABB gap = 10 along X
        let result = Spi::get_one::<f64>(
            "SELECT solid_box(10,10,10) <-> solid_translate(solid_box(10,10,10), 20, 0, 0)",
        )
        .expect("SPI failed");
        let dist = result.expect("NULL result");
        assert!(
            (dist - 10.0).abs() < 0.1,
            "expected distance ~10, got {dist}"
        );
    }

    #[pg_test]
    fn test_distance_operator_overlapping() {
        set_search_path();
        // Same box — distance 0
        let result = Spi::get_one::<f64>("SELECT solid_box(10,10,10) <-> solid_box(10,10,10)")
            .expect("SPI failed");
        let dist = result.expect("NULL result");
        assert!(
            dist < 0.001,
            "overlapping distance should be ~0, got {dist}"
        );
    }

    #[pg_test]
    fn test_knn_distance_operator() {
        set_search_path();
        Spi::run("CREATE TABLE test_knn(id serial, geom pgsolid.solid)").expect("CREATE TABLE");
        // Place 10 boxes at x=100,200,...,1000
        Spi::run(
            "INSERT INTO test_knn(geom) \
             SELECT pgsolid.solid_translate(pgsolid.solid_box(10,10,10), i*100.0, 0, 0) \
             FROM generate_series(1,10) AS s(i)",
        )
        .expect("INSERT");
        Spi::run("CREATE INDEX ON test_knn USING gist(geom)").expect("CREATE INDEX");

        // KNN: find 3 nearest to origin box [0,10]^3
        // Nearest should be at x=100 (dist=90), then x=200 (dist=190), then x=300 (dist=290)
        let nearest_id = Spi::get_one::<i32>(
            "SELECT id FROM test_knn ORDER BY geom <-> pgsolid.solid_box(10,10,10) LIMIT 1",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(nearest_id, 1, "nearest should be id=1 (at x=100)");
    }

    // ===== Phase 2 deferred: OBB =====

    #[pg_test]
    fn test_solid_obb_box_aligned() {
        set_search_path();
        // Axis-aligned box 100x200x300: OBB half-sizes should be ~50, ~100, ~150
        let result = Spi::get_one::<f64>("SELECT solid_obb_volume(solid_box(100, 200, 300))")
            .expect("SPI failed");
        let obb_vol = result.expect("NULL result");
        let expected = 100.0 * 200.0 * 300.0; // 8 * 50 * 100 * 150
        assert!(
            (obb_vol - expected).abs() / expected < 0.01,
            "aligned box OBB volume: expected ~{expected}, got {obb_vol}"
        );
    }

    #[pg_test]
    fn test_solid_obb_rotated_tighter() {
        set_search_path();
        // A long thin beam (100x10x10) rotated 45° around Z:
        // AABB grows (becomes ~78x78x10), but OBB volume stays ~10000
        let rotated = "solid_rotate(solid_box(100, 10, 10), 0, 0, 1, 45)";
        let obb_vol = Spi::get_one::<f64>(&format!("SELECT solid_obb_volume({rotated})"))
            .expect("SPI failed")
            .expect("NULL");
        // Compute AABB volume via solid_volume of a box reconstructed from the bbox
        let bbox = Spi::get_one::<crate::BBox3D>(&format!("SELECT solid_bbox({rotated})"))
            .expect("SPI failed")
            .expect("NULL");
        let aabb_vol = (bbox.xmax - bbox.xmin) as f64
            * (bbox.ymax - bbox.ymin) as f64
            * (bbox.zmax - bbox.zmin) as f64;
        assert!(
            obb_vol < aabb_vol,
            "OBB volume ({obb_vol}) should be < AABB volume ({aabb_vol}) for rotated beam"
        );
    }

    #[pg_test]
    fn test_solid_obb_volume_rotation_invariant() {
        set_search_path();
        // OBB volume should be unchanged by rotation
        let vol_orig = Spi::get_one::<f64>("SELECT solid_obb_volume(solid_box(100, 200, 300))")
            .expect("SPI failed")
            .expect("NULL");
        let vol_rotated = Spi::get_one::<f64>(
            "SELECT solid_obb_volume(solid_rotate(solid_box(100, 200, 300), 1, 1, 1, 37))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (vol_orig - vol_rotated).abs() / vol_orig < 0.01,
            "OBB volume should be rotation-invariant: {vol_orig} vs {vol_rotated}"
        );
    }

    #[pg_test]
    fn test_solid_obb_text_output() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT solid_obb(solid_box(100, 200, 300))")
            .expect("SPI failed");
        let obb = result.expect("NULL result");
        assert!(
            obb.starts_with("OBB("),
            "OBB text should start with 'OBB(', got: {obb}"
        );
    }

    // ===== Phase 3: Boolean Operations =====

    #[pg_test]
    fn test_solid_union_overlapping_boxes() {
        set_search_path();
        // Two boxes: [0,10]^3 and [5,15]x[0,10]x[0,10] overlap in a 5x10x10 region
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        // Union volume = 2*1000 - 500 overlap = 1500
        assert!(
            (vol - 1500.0).abs() < 10.0,
            "union volume: expected ~1500, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_union_disjoint_boxes() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (vol - 2000.0).abs() < 10.0,
            "disjoint union: expected ~2000, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_difference() {
        set_search_path();
        // Cut a smaller box from a larger one
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_difference(solid_box(10,10,10), solid_translate(solid_box(5,5,5), 2.5, 2.5, 2.5)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        // 1000 - 125 = 875
        assert!(
            (vol - 875.0).abs() < 10.0,
            "difference volume: expected ~875, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_difference_no_overlap() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_difference(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (vol - 1000.0).abs() < 10.0,
            "no-overlap difference: expected ~1000, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_intersection() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_intersection(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        // Overlap region: 5x10x10 = 500
        assert!(
            (vol - 500.0).abs() < 10.0,
            "intersection volume: expected ~500, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_intersection_valid() {
        set_search_path();
        let valid = Spi::get_one::<bool>(
            "SELECT solid_is_valid(solid_intersection(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0)))",
        )
        .expect("SPI failed");
        assert_eq!(valid, Some(true), "intersection result should be valid");
    }

    #[pg_test]
    fn test_solid_union_cylinder_box() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_union(solid_box(10,10,10), solid_cylinder(5, 10)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let box_vol = 1000.0;
        let cyl_vol = std::f64::consts::PI * 25.0 * 10.0;
        assert!(
            vol > box_vol && vol < box_vol + cyl_vol,
            "mixed union volume: {vol}, should be between {box_vol} and {}",
            box_vol + cyl_vol
        );
    }

    // ===== Phase 3: Shape Healing =====

    #[pg_test]
    fn test_solid_heal_preserves_valid() {
        set_search_path();
        let vol = Spi::get_one::<f64>("SELECT solid_volume(solid_heal(solid_box(100, 200, 300)))")
            .expect("SPI failed")
            .expect("NULL");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (vol - expected).abs() < 1.0,
            "healed volume: expected {expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_heal_result_valid() {
        set_search_path();
        let valid =
            Spi::get_one::<bool>("SELECT solid_is_valid(solid_heal(solid_box(100, 200, 300)))")
                .expect("SPI failed");
        assert_eq!(valid, Some(true));
    }

    // ===== Phase 3: STL Export =====

    #[pg_test]
    fn test_solid_to_stl_returns_binary() {
        set_search_path();
        let stl = Spi::get_one::<Vec<u8>>("SELECT solid_to_stl(solid_box(10, 10, 10))")
            .expect("SPI failed")
            .expect("NULL");
        // Binary STL: 80-byte header + 4-byte count + N*50 bytes
        assert!(stl.len() >= 84, "STL too short: {} bytes", stl.len());
        let tri_count = u32::from_le_bytes([stl[80], stl[81], stl[82], stl[83]]);
        // A box has 12 triangles (2 per face * 6 faces)
        assert_eq!(
            tri_count, 12,
            "box STL triangle count: expected 12, got {tri_count}"
        );
        assert_eq!(
            stl.len(),
            684,
            "box STL size: expected 684, got {}",
            stl.len()
        );
    }

    #[pg_test]
    fn test_solid_to_stl_sphere() {
        set_search_path();
        let stl = Spi::get_one::<Vec<u8>>("SELECT solid_to_stl(solid_sphere(100))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(stl.len() >= 84, "STL too short");
        let tri_count = u32::from_le_bytes([stl[80], stl[81], stl[82], stl[83]]);
        assert!(
            tri_count > 50,
            "sphere should have >50 triangles, got {tri_count}"
        );
    }

    #[pg_test]
    fn test_solid_to_stl_custom_deflection() {
        set_search_path();
        let stl_coarse =
            Spi::get_one::<Vec<u8>>("SELECT solid_to_stl(solid_sphere(100), 10.0, 1.0)")
                .expect("SPI failed")
                .expect("NULL");
        let stl_fine = Spi::get_one::<Vec<u8>>("SELECT solid_to_stl(solid_sphere(100), 0.1, 0.1)")
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            stl_fine.len() > stl_coarse.len(),
            "finer deflection should produce more triangles: coarse={}, fine={}",
            stl_coarse.len(),
            stl_fine.len()
        );
    }

    // ===== Phase 3: IFC Extrusion =====

    #[pg_test]
    fn test_solid_from_ifc_extrusion() {
        set_search_path();
        // Extrude a 100x200 rectangle along Z by 500 → volume = 100*200*500 = 10,000,000
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_ifc_extrusion(100, 200, 0, 0, 500))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = 100.0 * 200.0 * 500.0;
        assert!(
            (vol - expected).abs() / expected < 0.01,
            "extrusion volume: expected ~{expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_from_ifc_extrusion_valid() {
        set_search_path();
        let valid = Spi::get_one::<bool>(
            "SELECT solid_is_valid(solid_from_ifc_extrusion(100, 200, 0, 0, 500))",
        )
        .expect("SPI failed");
        assert_eq!(valid, Some(true));
    }

    // ===== STEP Import/Export =====

    #[pg_test]
    fn test_solid_step_roundtrip() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_step(solid_to_step(solid_box(100, 200, 300))))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (vol - expected).abs() / expected < 0.01,
            "STEP roundtrip volume: expected ~{expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_from_step_valid() {
        set_search_path();
        let valid = Spi::get_one::<bool>(
            "SELECT solid_is_valid(solid_from_step(solid_to_step(solid_cylinder(50, 100))))",
        )
        .expect("SPI failed");
        assert_eq!(valid, Some(true), "STEP-imported shape should be valid");
    }

    #[pg_test]
    fn test_solid_to_step_nonempty() {
        set_search_path();
        let len = Spi::get_one::<i32>("SELECT length(solid_to_step(solid_box(10, 10, 10)))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            len > 100,
            "STEP output should be non-trivial, got {len} bytes"
        );
    }

    // ===== IGES Import/Export =====

    #[pg_test]
    fn test_solid_iges_roundtrip() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_iges(solid_to_iges(solid_box(100, 200, 300))))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (vol - expected).abs() / expected < 0.01,
            "IGES roundtrip volume: expected ~{expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_from_iges_valid() {
        set_search_path();
        let valid = Spi::get_one::<bool>(
            "SELECT solid_is_valid(solid_from_iges(solid_to_iges(solid_sphere(100))))",
        )
        .expect("SPI failed");
        assert_eq!(valid, Some(true), "IGES-imported shape should be valid");
    }

    #[pg_test]
    fn test_solid_to_iges_nonempty() {
        set_search_path();
        let len = Spi::get_one::<i32>("SELECT length(solid_to_iges(solid_box(10, 10, 10)))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            len > 100,
            "IGES output should be non-trivial, got {len} bytes"
        );
    }

    // ===== File-based Import =====

    #[pg_test]
    fn test_solid_from_step_file() {
        set_search_path();
        // Generate STEP bytes via SQL, write to /tmp from Rust, then read back via SQL
        let step_bytes = Spi::get_one::<Vec<u8>>("SELECT solid_to_step(solid_box(100, 200, 300))")
            .expect("SPI failed")
            .expect("NULL");
        std::fs::write("/tmp/pg_solid_test.step", &step_bytes).expect("write STEP file");
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_step_file('/tmp/pg_solid_test.step'))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (vol - expected).abs() / expected < 0.01,
            "STEP file roundtrip volume: expected ~{expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_from_iges_file() {
        set_search_path();
        // Generate IGES bytes via SQL, write to /tmp from Rust, then read back via SQL
        let iges_bytes = Spi::get_one::<Vec<u8>>("SELECT solid_to_iges(solid_box(50, 50, 50))")
            .expect("SPI failed")
            .expect("NULL");
        std::fs::write("/tmp/pg_solid_test.iges", &iges_bytes).expect("write IGES file");
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(solid_from_iges_file('/tmp/pg_solid_test.iges'))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = 50.0 * 50.0 * 50.0;
        assert!(
            (vol - expected).abs() / expected < 0.01,
            "IGES file roundtrip volume: expected ~{expected}, got {vol}"
        );
    }

    // ===== Phase 5: OBJ/GLB/USDA Export =====

    #[pg_test]
    fn test_solid_to_obj_box() {
        set_search_path();
        let obj = Spi::get_one::<Vec<u8>>("SELECT solid_to_obj(solid_box(10, 10, 10))")
            .expect("SPI failed")
            .expect("NULL");
        let text = String::from_utf8(obj).expect("invalid UTF-8");
        let v_count = text.lines().filter(|l| l.starts_with("v ")).count();
        let vn_count = text.lines().filter(|l| l.starts_with("vn ")).count();
        let f_count = text.lines().filter(|l| l.starts_with("f ")).count();
        assert!(v_count >= 8, "box should have >= 8 vertices, got {v_count}");
        assert_eq!(v_count, vn_count, "vertex count should equal normal count");
        assert_eq!(
            f_count, 12,
            "box should have 12 triangle faces, got {f_count}"
        );
    }

    #[pg_test]
    fn test_solid_to_obj_sphere() {
        set_search_path();
        let obj = Spi::get_one::<Vec<u8>>("SELECT solid_to_obj(solid_sphere(100))")
            .expect("SPI failed")
            .expect("NULL");
        let text = String::from_utf8(obj).expect("invalid UTF-8");
        let f_count = text.lines().filter(|l| l.starts_with("f ")).count();
        assert!(
            f_count > 50,
            "sphere should have >50 triangles, got {f_count}"
        );
    }

    #[pg_test]
    fn test_solid_to_glb_valid_header() {
        set_search_path();
        let glb = Spi::get_one::<Vec<u8>>("SELECT solid_to_glb(solid_box(10, 10, 10))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(glb.len() >= 12, "GLB too short: {} bytes", glb.len());
        // GLB magic: "glTF"
        assert_eq!(&glb[0..4], b"glTF", "GLB magic bytes mismatch");
        // Version 2
        let version = u32::from_le_bytes([glb[4], glb[5], glb[6], glb[7]]);
        assert_eq!(version, 2, "GLB version should be 2, got {version}");
        // Total length should match actual size
        let total_len = u32::from_le_bytes([glb[8], glb[9], glb[10], glb[11]]);
        assert_eq!(total_len as usize, glb.len(), "GLB total length mismatch");
    }

    #[pg_test]
    fn test_solid_to_glb_box() {
        set_search_path();
        let glb = Spi::get_one::<Vec<u8>>("SELECT solid_to_glb(solid_box(100, 200, 300))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            glb.len() > 200,
            "GLB should be substantial, got {} bytes",
            glb.len()
        );
        // JSON chunk type at offset 16 should be 0x4E4F534A ("JSON")
        let chunk_type = u32::from_le_bytes([glb[16], glb[17], glb[18], glb[19]]);
        assert_eq!(chunk_type, 0x4E4F534A, "first chunk should be JSON");
    }

    #[pg_test]
    fn test_solid_to_usda_box() {
        set_search_path();
        let usda = Spi::get_one::<Vec<u8>>("SELECT solid_to_usda(solid_box(10, 10, 10))")
            .expect("SPI failed")
            .expect("NULL");
        let text = String::from_utf8(usda).expect("invalid UTF-8");
        assert!(
            text.starts_with("#usda 1.0"),
            "USDA should start with '#usda 1.0'"
        );
        assert!(text.contains("def Mesh"), "USDA should contain 'def Mesh'");
        assert!(
            text.contains("point3f[]"),
            "USDA should contain 'point3f[]'"
        );
        assert!(
            text.contains("normal3f[]"),
            "USDA should contain 'normal3f[]'"
        );
    }

    #[pg_test]
    fn test_solid_to_usda_sphere() {
        set_search_path();
        let usda = Spi::get_one::<Vec<u8>>("SELECT solid_to_usda(solid_sphere(100))")
            .expect("SPI failed")
            .expect("NULL");
        let text = String::from_utf8(usda).expect("invalid UTF-8");
        assert!(
            text.starts_with("#usda 1.0"),
            "USDA should start with '#usda 1.0'"
        );
        assert!(
            text.contains("faceVertexCounts"),
            "USDA should contain 'faceVertexCounts'"
        );
    }

    #[pg_test]
    fn test_mesh_deflection_affects_count() {
        set_search_path();
        let obj_coarse =
            Spi::get_one::<Vec<u8>>("SELECT solid_to_obj(solid_sphere(100), 10.0, 1.0)")
                .expect("SPI failed")
                .expect("NULL");
        let obj_fine = Spi::get_one::<Vec<u8>>("SELECT solid_to_obj(solid_sphere(100), 0.1, 0.1)")
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            obj_fine.len() > obj_coarse.len(),
            "finer deflection should produce larger OBJ: coarse={}, fine={}",
            obj_coarse.len(),
            obj_fine.len()
        );
    }

    #[pg_test]
    fn test_solid_to_obj_after_boolean() {
        set_search_path();
        let obj = Spi::get_one::<Vec<u8>>(
            "SELECT solid_to_obj(solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0)))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let text = String::from_utf8(obj).expect("invalid UTF-8");
        let f_count = text.lines().filter(|l| l.starts_with("f ")).count();
        assert!(f_count > 0, "boolean union export should have triangles");
    }

    #[pg_test]
    fn test_solid_export_obj_file() {
        set_search_path();
        let path = Spi::get_one::<String>(
            "SELECT solid_export_obj(solid_box(10, 10, 10), '/tmp/pg_solid_test.obj')",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(path, "/tmp/pg_solid_test.obj");
        let content = std::fs::read_to_string(&path).expect("read OBJ file");
        assert!(content.contains("v "), "OBJ file should contain vertices");
        assert!(content.contains("f "), "OBJ file should contain faces");
    }

    #[pg_test]
    fn test_solid_export_glb_file() {
        set_search_path();
        let path = Spi::get_one::<String>(
            "SELECT solid_export_glb(solid_box(10, 10, 10), '/tmp/pg_solid_test.glb')",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(path, "/tmp/pg_solid_test.glb");
        let data = std::fs::read(&path).expect("read GLB file");
        assert_eq!(&data[0..4], b"glTF", "GLB file should have glTF magic");
    }

    #[pg_test]
    fn test_solid_export_usda_file() {
        set_search_path();
        let path = Spi::get_one::<String>(
            "SELECT solid_export_usda(solid_box(10, 10, 10), '/tmp/pg_solid_test.usda')",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(path, "/tmp/pg_solid_test.usda");
        let content = std::fs::read_to_string(&path).expect("read USDA file");
        assert!(
            content.starts_with("#usda 1.0"),
            "USDA file should start with header"
        );
    }

    // ===== Phase 6: Predicates, Measurements, Operations =====

    #[pg_test]
    fn test_solid_within_clearance_true() {
        set_search_path();
        // Two boxes: [0,10] and [20,30] along X — gap of 10, clearance 15 → true
        let result = Spi::get_one::<bool>(
            "SELECT solid_within_clearance(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 20, 0, 0), 15)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_within_clearance_false() {
        set_search_path();
        // gap of 10, clearance 5 → false
        let result = Spi::get_one::<bool>(
            "SELECT solid_within_clearance(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 20, 0, 0), 5)",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    #[pg_test]
    fn test_solid_contains_true() {
        set_search_path();
        // Large box [0,100]^3 contains small box [25,75]^3
        let result = Spi::get_one::<bool>(
            "SELECT solid_contains(solid_box(100,100,100), solid_translate(solid_box(50,50,50), 25, 25, 25))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_contains_false() {
        set_search_path();
        // Box [0,100]^3 does NOT contain box [50,150] (extends beyond)
        let result = Spi::get_one::<bool>(
            "SELECT solid_contains(solid_box(100,100,100), solid_translate(solid_box(100,100,100), 50, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    #[pg_test]
    fn test_solid_touches_true() {
        set_search_path();
        // Two boxes: [0,10]^3 and [10,20]x[0,10]x[0,10] — touching at x=10
        let result = Spi::get_one::<bool>(
            "SELECT solid_touches(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 10, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_touches_false_overlapping() {
        set_search_path();
        // Two overlapping boxes — not touching, overlapping
        let result = Spi::get_one::<bool>(
            "SELECT solid_touches(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    #[pg_test]
    fn test_solid_hosts_true() {
        set_search_path();
        // Room [0,100]^3 hosts furniture at [25,75]^3
        let result = Spi::get_one::<bool>(
            "SELECT solid_hosts(solid_box(100,100,100), solid_translate(solid_box(50,50,50), 25, 25, 25))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_solid_hosts_false() {
        set_search_path();
        // Guest centroid is outside host
        let result = Spi::get_one::<bool>(
            "SELECT solid_hosts(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))",
        )
        .expect("SPI failed");
        assert_eq!(result, Some(false));
    }

    #[pg_test]
    fn test_solid_dimensions_box() {
        set_search_path();
        // Box 300 x 200 x 100 → dimensions sorted descending: 300, 200, 100
        let result =
            Spi::get_one::<String>("SELECT solid_dimensions(solid_box(300, 200, 100))::text")
                .expect("SPI failed");
        let dims = result.expect("NULL result");
        // OBB half-sizes for axis-aligned box: 150, 100, 50 → dims: 300, 200, 100
        assert!(dims.contains("300"), "should contain 300: {dims}");
        assert!(dims.contains("200"), "should contain 200: {dims}");
        assert!(dims.contains("100"), "should contain 100: {dims}");
    }

    #[pg_test]
    fn test_solid_check_valid() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT solid_check(solid_box(100, 200, 300))")
            .expect("SPI failed");
        assert_eq!(result, Some("OK".to_string()));
    }

    #[pg_test]
    fn test_solid_offset_expands() {
        set_search_path();
        // Offset a box outward by 10 — volume should increase
        let vol_orig = Spi::get_one::<f64>("SELECT solid_volume(solid_box(100, 100, 100))")
            .expect("SPI failed")
            .expect("NULL");
        let vol_offset =
            Spi::get_one::<f64>("SELECT solid_volume(solid_offset(solid_box(100, 100, 100), 10))")
                .expect("SPI failed")
                .expect("NULL");
        assert!(
            vol_offset > vol_orig,
            "offset(+10) should increase volume: {vol_orig} vs {vol_offset}"
        );
    }

    #[pg_test]
    fn test_solid_offset_shrinks() {
        set_search_path();
        // Offset a box inward by -10 — volume should decrease
        let vol_orig = Spi::get_one::<f64>("SELECT solid_volume(solid_box(100, 100, 100))")
            .expect("SPI failed")
            .expect("NULL");
        let vol_offset =
            Spi::get_one::<f64>("SELECT solid_volume(solid_offset(solid_box(100, 100, 100), -10))")
                .expect("SPI failed")
                .expect("NULL");
        assert!(
            vol_offset < vol_orig,
            "offset(-10) should decrease volume: {vol_orig} vs {vol_offset}"
        );
    }

    #[pg_test]
    fn test_solid_shared_face_area_touching() {
        set_search_path();
        // Two boxes: [0,10]^3 and [10,20]x[0,10]x[0,10] — shared face = 10x10 = 100
        let area = Spi::get_one::<f64>(
            "SELECT solid_shared_face_area(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 10, 0, 0))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (area - 100.0).abs() < 1.0,
            "shared face area: expected ~100, got {area}"
        );
    }

    #[pg_test]
    fn test_solid_shared_face_area_separated() {
        set_search_path();
        // Two separated boxes — no shared face
        let area = Spi::get_one::<f64>(
            "SELECT solid_shared_face_area(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            area < 1.0,
            "separated boxes should have ~0 shared face area, got {area}"
        );
    }

    // ===== Phase 7: Aggregates =====

    #[pg_test]
    fn test_solid_agg_union_three_boxes() {
        set_search_path();
        // Union of three non-overlapping boxes, each 10x10x10 = 1000
        Spi::run("CREATE TABLE test_agg_union(geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run(
            "INSERT INTO test_agg_union VALUES \
             (pgsolid.solid_box(10,10,10)), \
             (pgsolid.solid_translate(pgsolid.solid_box(10,10,10), 20, 0, 0)), \
             (pgsolid.solid_translate(pgsolid.solid_box(10,10,10), 40, 0, 0))",
        )
        .expect("INSERT");
        let vol =
            Spi::get_one::<f64>("SELECT solid_volume(solid_agg_union(geom)) FROM test_agg_union")
                .expect("SPI failed")
                .expect("NULL");
        assert!(
            (vol - 3000.0).abs() < 10.0,
            "union of 3 disjoint boxes: expected ~3000, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_agg_union_single() {
        set_search_path();
        Spi::run("CREATE TABLE test_agg_union2(geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run("INSERT INTO test_agg_union2 VALUES (pgsolid.solid_box(100, 200, 300))")
            .expect("INSERT");
        let vol =
            Spi::get_one::<f64>("SELECT solid_volume(solid_agg_union(geom)) FROM test_agg_union2")
                .expect("SPI failed")
                .expect("NULL");
        let expected = 100.0 * 200.0 * 300.0;
        assert!(
            (vol - expected).abs() < 1.0,
            "single solid union: expected {expected}, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_agg_bbox_two_separated() {
        set_search_path();
        // Two boxes: [0,10]^3 and [90,100]^3 → combined bbox [0,100]x[0,10]x[0,10]
        Spi::run("CREATE TABLE test_agg_bbox(geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run(
            "INSERT INTO test_agg_bbox VALUES \
             (pgsolid.solid_box(10,10,10)), \
             (pgsolid.solid_translate(pgsolid.solid_box(10,10,10), 90, 0, 0))",
        )
        .expect("INSERT");
        let bbox = Spi::get_one::<crate::BBox3D>("SELECT solid_agg_bbox(geom) FROM test_agg_bbox")
            .expect("SPI failed")
            .expect("NULL");
        assert!(bbox.xmin.abs() < 0.01, "xmin: {}", bbox.xmin);
        assert!(bbox.ymin.abs() < 0.01, "ymin: {}", bbox.ymin);
        assert!(bbox.zmin.abs() < 0.01, "zmin: {}", bbox.zmin);
        assert!((bbox.xmax - 100.0).abs() < 0.01, "xmax: {}", bbox.xmax);
        assert!((bbox.ymax - 10.0).abs() < 0.01, "ymax: {}", bbox.ymax);
        assert!((bbox.zmax - 10.0).abs() < 0.01, "zmax: {}", bbox.zmax);
    }

    #[pg_test]
    fn test_solid_agg_bbox_single() {
        set_search_path();
        Spi::run("CREATE TABLE test_agg_bbox2(geom pgsolid.solid)").expect("CREATE TABLE");
        Spi::run("INSERT INTO test_agg_bbox2 VALUES (pgsolid.solid_box(100, 200, 300))")
            .expect("INSERT");
        let bbox = Spi::get_one::<crate::BBox3D>("SELECT solid_agg_bbox(geom) FROM test_agg_bbox2")
            .expect("SPI failed")
            .expect("NULL");
        assert!(bbox.xmin.abs() < 0.01, "xmin: {}", bbox.xmin);
        assert!((bbox.xmax - 100.0).abs() < 0.01, "xmax: {}", bbox.xmax);
        assert!((bbox.ymax - 200.0).abs() < 0.01, "ymax: {}", bbox.ymax);
        assert!((bbox.zmax - 300.0).abs() < 0.01, "zmax: {}", bbox.zmax);
    }

    // ===== Phase 7: Slice =====

    #[pg_test]
    fn test_solid_slice_box_z_plane() {
        set_search_path();
        // Slice a 10x10x10 box at z=5 (horizontal plane, normal Z)
        // Should produce a 10x10 = 100 area cross-section
        let area = Spi::get_one::<f64>(
            "SELECT solid_surface_area(solid_slice(solid_box(10,10,10), 0, 0, 5, 0, 0, 1))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (area - 100.0).abs() < 1.0,
            "slice of 10x10x10 box at z=5 should be 100, got {area}"
        );
    }

    #[pg_test]
    fn test_solid_slice_cylinder_mid() {
        set_search_path();
        // Slice a cylinder(r=5, h=20) at z=10
        // Cross-section should be a circle with area = pi*r^2 ≈ 78.54
        let area = Spi::get_one::<f64>(
            "SELECT solid_surface_area(solid_slice(solid_cylinder(5, 20), 0, 0, 10, 0, 0, 1))",
        )
        .expect("SPI failed")
        .expect("NULL");
        let expected = std::f64::consts::PI * 25.0; // pi*r^2
        assert!(
            (area - expected).abs() < 1.0,
            "cylinder slice should be ~{expected}, got {area}"
        );
    }

    #[pg_test]
    fn test_solid_slice_box_y_plane() {
        set_search_path();
        // Slice a 20x10x30 box at y=5 with normal (0,1,0)
        // Cross-section: 20x30 = 600
        let area = Spi::get_one::<f64>(
            "SELECT solid_surface_area(solid_slice(solid_box(20,10,30), 0, 5, 0, 0, 1, 0))",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (area - 600.0).abs() < 5.0,
            "slice of 20x10x30 box at y=5 should be 600, got {area}"
        );
    }

    // ===== Phase 7: 2D Projection =====

    #[pg_test]
    fn test_solid_to_2d_box_top_down() {
        set_search_path();
        // Project a box looking down (0,0,1) — should produce edges
        let face_count = Spi::get_one::<i32>(
            "SELECT solid_face_count(solid_to_2d(solid_box(10,10,10), 0, 0, 1))",
        )
        .expect("SPI failed")
        .expect("NULL");
        // Projection produces edges (not faces), but the BRep compound is stored as a Solid
        // The face_count should be 0 (edges only), but we mainly test it doesn't error
        assert!(
            face_count >= 0,
            "solid_to_2d should return valid result, got face_count={face_count}"
        );
    }

    #[pg_test]
    fn test_solid_to_2d_box_front_view() {
        set_search_path();
        // Project a box looking from the front (1,0,0)
        // Should produce visible outline edges
        let result =
            Spi::get_one::<bool>("SELECT solid_to_2d(solid_box(10, 20, 30), 1, 0, 0) IS NOT NULL")
                .expect("SPI failed")
                .expect("NULL");
        assert!(
            result,
            "solid_to_2d should return non-null for box front view"
        );
    }

    // ---- Phase 8: Compound explode ----

    #[pg_test]
    fn test_solid_explode_single_box() {
        set_search_path();
        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM solid_explode(solid_box(10, 10, 10))")
                .expect("SPI failed")
                .expect("NULL");
        assert_eq!(count, 1, "single box should explode to 1 solid");
    }

    #[pg_test]
    fn test_solid_explode_preserves_volume() {
        set_search_path();
        let vol = Spi::get_one::<f64>(
            "SELECT solid_volume(e) FROM solid_explode(solid_box(10, 20, 30)) e",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (vol - 6000.0).abs() < 1.0,
            "exploded single box volume should be 6000, got {vol}"
        );
    }

    #[pg_test]
    fn test_solid_explode_disjoint_union() {
        set_search_path();
        // Two disjoint boxes: union creates a compound of 2 solids
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM solid_explode(
                solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))
            )",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(
            count, 2,
            "union of 2 disjoint boxes should explode to 2 solids"
        );
    }

    #[pg_test]
    fn test_solid_explode_total_volume() {
        set_search_path();
        let total_vol = Spi::get_one::<f64>(
            "SELECT sum(solid_volume(e)) FROM solid_explode(
                solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))
            ) e",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            (total_vol - 2000.0).abs() < 10.0,
            "total volume after explode should be ~2000, got {total_vol}"
        );
    }

    #[pg_test]
    fn test_solid_num_solids_single() {
        set_search_path();
        let count = Spi::get_one::<i32>("SELECT solid_num_solids(solid_sphere(5))")
            .expect("SPI failed")
            .expect("NULL");
        assert_eq!(count, 1, "single sphere should have 1 solid");
    }

    #[pg_test]
    fn test_solid_num_solids_compound() {
        set_search_path();
        let count = Spi::get_one::<i32>(
            "SELECT solid_num_solids(
                solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 100, 0, 0))
            )",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(count, 2, "union of 2 disjoint boxes should have 2 solids");
    }

    #[pg_test]
    fn test_solid_explode_overlapping_union() {
        set_search_path();
        // Overlapping boxes fuse into 1 body
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM solid_explode(
                solid_union(solid_box(10,10,10), solid_translate(solid_box(10,10,10), 5, 0, 0))
            )",
        )
        .expect("SPI failed")
        .expect("NULL");
        assert_eq!(count, 1, "overlapping union should fuse to 1 solid");
    }

    #[pg_test]
    fn test_solid_explode_cylinder() {
        set_search_path();
        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM solid_explode(solid_cylinder(5, 20))")
                .expect("SPI failed")
                .expect("NULL");
        assert_eq!(count, 1, "single cylinder should explode to 1 solid");
    }

    // ===== Phase 9: IFC Import =====

    fn ifc_test_file() -> String {
        format!(
            "{}/test_data/minimal_building.ifc",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[pg_test]
    fn test_ifc_elements_returns_rows() {
        set_search_path();
        let path = ifc_test_file();
        let count = Spi::get_one::<i64>(&format!("SELECT count(*) FROM ifc_elements('{path}')"))
            .expect("SPI failed")
            .expect("NULL");
        assert!(
            count > 0,
            "ifc_elements should return at least 1 row, got {count}"
        );
    }

    #[pg_test]
    fn test_ifc_elements_wall_type() {
        set_search_path();
        let path = ifc_test_file();
        let ifc_type = Spi::get_one::<String>(&format!(
            "SELECT ifc_type FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall'"
        ))
        .expect("SPI failed");
        assert_eq!(
            ifc_type,
            Some("IfcWall".to_string()),
            "should find an IfcWall element"
        );
    }

    #[pg_test]
    fn test_ifc_elements_wall_has_solid() {
        set_search_path();
        let path = ifc_test_file();
        let vol = Spi::get_one::<f64>(&format!(
            "SELECT solid_volume(solid) FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' AND solid IS NOT NULL LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL volume — wall geometry not converted");
        assert!(
            vol > 0.0,
            "wall solid should have positive volume, got {vol}"
        );
    }

    #[pg_test]
    fn test_ifc_spatial_structure_hierarchy() {
        set_search_path();
        let path = ifc_test_file();
        let count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM ifc_spatial_structure('{path}')"
        ))
        .expect("SPI failed")
        .expect("NULL");
        assert!(
            count >= 4,
            "should have project, site, building, storey — got {count}"
        );
    }

    #[pg_test]
    fn test_ifc_relationships_has_edges() {
        set_search_path();
        let path = ifc_test_file();
        let count =
            Spi::get_one::<i64>(&format!("SELECT count(*) FROM ifc_relationships('{path}')"))
                .expect("SPI failed")
                .expect("NULL");
        assert!(
            count > 0,
            "ifc_relationships should return edges, got {count}"
        );
    }

    #[pg_test]
    fn test_ifc_elements_wall_has_properties() {
        set_search_path();
        let path = ifc_test_file();
        let has_props = Spi::get_one::<bool>(&format!(
            "SELECT properties IS NOT NULL FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL");
        assert!(has_props, "wall should have properties");
    }

    // ===== Phase 8: Georeferencing =====

    fn ifc_georef_file() -> String {
        format!(
            "{}/test_data/georef_building.ifc",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[pg_test]
    fn test_ifc_site_location_returns_lat_lon() {
        set_search_path();
        let path = ifc_georef_file();
        let row = Spi::get_three::<f64, f64, f64>(&format!(
            "SELECT lat, lon, elevation FROM ifc_site_location('{path}')"
        ))
        .expect("SPI failed");
        let lat = row.0.expect("NULL lat");
        let lon = row.1.expect("NULL lon");
        let elev = row.2.expect("NULL elev");
        assert!((lat - 50.833_333_333).abs() < 1e-6, "lat = {lat}");
        assert!((lon - 4.35).abs() < 1e-6, "lon = {lon}");
        assert!((elev - 15.0).abs() < 1e-9, "elev = {elev}");
    }

    #[pg_test]
    fn test_ifc_site_location_no_geo_returns_nulls() {
        set_search_path();
        let path = ifc_test_file(); // minimal_building has no geo fields
        let lat = Spi::get_one::<f64>(&format!("SELECT lat FROM ifc_site_location('{path}')"))
            .expect("SPI failed");
        assert!(lat.is_none(), "expected NULL lat for non-georef fixture");
    }

    #[pg_test]
    fn test_ifc_map_conversion_returns_crs() {
        set_search_path();
        let path = ifc_georef_file();
        let target = Spi::get_one::<String>(&format!(
            "SELECT target_crs FROM ifc_map_conversion('{path}')"
        ))
        .expect("SPI failed");
        assert_eq!(target.as_deref(), Some("EPSG:32631"));
    }

    #[pg_test]
    fn test_ifc_map_conversion_eastings_northings() {
        set_search_path();
        let path = ifc_georef_file();
        let row = Spi::get_three::<f64, f64, f64>(&format!(
            "SELECT eastings, northings, orthogonal_height FROM ifc_map_conversion('{path}')"
        ))
        .expect("SPI failed");
        assert_eq!(row.0, Some(597_500.0));
        assert_eq!(row.1, Some(5_630_500.0));
        assert_eq!(row.2, Some(15.0));
    }

    #[pg_test]
    fn test_solid_georeference_default_4326_lands_near_site() {
        set_search_path();
        let path = ifc_georef_file();
        // Default target_srid = 4326 (geographic). Wall sits at IFC local
        // x = 10; tangent-plane around (50.8333°, 4.35°) maps that to
        // ~10/(111319 * cos(50.83°)) ≈ 1.42e-4° east of the site longitude.
        let pt = Spi::get_one::<crate::Point3D>(&format!(
            "SELECT solid_centroid(solid_georeference(solid, '{path}')) \
             FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL centroid");
        // lon ≈ 4.35 + (10 / (111319 * cos(50.8333°))) ≈ 4.3501424
        assert!(
            (pt.x - 4.350_142).abs() < 1e-5,
            "expected lon near 4.350142, got {}",
            pt.x
        );
        // lat ≈ 50.8333 (wall has y ≈ 0 in IFC local)
        assert!(
            (pt.y - 50.833_333).abs() < 1e-5,
            "expected lat near 50.8333, got {}",
            pt.y
        );
    }

    #[pg_test]
    fn test_solid_georeference_projected_srid_keeps_utm_metres() {
        set_search_path();
        let path = ifc_georef_file();
        // Asking for target_srid = 32631 honours the file's IfcMapConversion
        // verbatim — output is UTM 31N metres. Wall centroid lands near
        // (eastings + 10, northings, +height/2).
        let pt = Spi::get_one::<crate::Point3D>(&format!(
            "SELECT solid_centroid(solid_georeference(solid, '{path}', 32631)) \
             FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL centroid");
        assert!(
            (pt.x - 597_510.0).abs() < 1.0,
            "expected UTM x near 597510, got {}",
            pt.x
        );
        assert!(
            (pt.y - 5_630_500.0).abs() < 1.0,
            "expected UTM y near 5_630_500, got {}",
            pt.y
        );
    }

    #[pg_test]
    fn test_solid_georeference_srid_mismatch_errors() {
        set_search_path();
        let path = ifc_georef_file();
        // File is EPSG:32631 — asking for 32632 must error.
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<f64>(&format!(
                "SELECT solid_volume(solid_georeference(solid, '{path}', 32632)) \
                 FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
            ))
        });
        assert!(
            result.is_err(),
            "solid_georeference must error when target_srid disagrees with IfcProjectedCRS"
        );
    }

    #[pg_test]
    fn test_solid_georeference_no_site_lat_lon_errors_at_4326() {
        set_search_path();
        let path = ifc_test_file(); // no IfcSite lat/lon
                                    // Default target_srid = 4326 now requires IfcSite lat/lon.
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<f64>(&format!(
                "SELECT solid_volume(solid_georeference(solid, '{path}')) \
                 FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
            ))
        });
        assert!(
            result.is_err(),
            "solid_georeference at target_srid=4326 must error without IfcSite lat/lon"
        );
    }

    #[pg_test]
    fn test_solid_georeference_lonlat_default_4326_is_tangent_plane() {
        set_search_path();
        // Default target_srid = 4326. Box at (lat=50.8333°, lon=4.35°)
        // with local centre at (50, 50, 50):
        //   lon ≈ 4.35 + 50/(111319 * cos(50.83°)) ≈ 4.350710
        //   lat ≈ 50.8333 + 50/111319                ≈ 50.833783
        //   elev = 0 + 50 = 50
        let pt = Spi::get_one::<crate::Point3D>(
            "SELECT solid_centroid(solid_georeference_lonlat(\
                solid_box(100, 100, 100), 50.833333, 4.35, 0.0))",
        )
        .expect("SPI failed")
        .expect("NULL centroid");
        assert!(
            (pt.x - 4.350_710).abs() < 1e-5,
            "lon expected ~4.350710, got {}",
            pt.x
        );
        assert!(
            (pt.y - 50.833_783).abs() < 1e-5,
            "lat expected ~50.833783, got {}",
            pt.y
        );
        assert!((pt.z - 50.0).abs() < 1.0, "elev expected ~50, got {}", pt.z);
    }

    #[pg_test]
    fn test_solid_georeference_lonlat_4978_is_ecef() {
        set_search_path();
        // target_srid = 4978: same anchor (lat=0, lon=0, elev=0) lands at
        // ECEF (6378137, 0, 0); box centre maps to (origin_x + 50, 50, 50).
        let pt = Spi::get_one::<crate::Point3D>(
            "SELECT solid_centroid(solid_georeference_lonlat(\
                solid_box(100, 100, 100), 0.0, 0.0, 0.0, 0.0, 1.0, 4978))",
        )
        .expect("SPI failed")
        .expect("NULL centroid");
        assert!(
            (pt.x - (6_378_137.0 + 50.0)).abs() < 1.0,
            "ECEF x expected ~6378187, got {}",
            pt.x
        );
        assert!(
            (pt.y - 50.0).abs() < 1.0,
            "ECEF y expected ~50, got {}",
            pt.y
        );
    }

    #[pg_test]
    fn test_solid_georeference_lonlat_invalid_srid_errors() {
        set_search_path();
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<f64>(
                "SELECT solid_volume(solid_georeference_lonlat(\
                  solid_box(100, 100, 100), 0.0, 0.0, 0.0, 0.0, 1.0, 12345))",
            )
        });
        assert!(
            result.is_err(),
            "solid_georeference_lonlat must error on unsupported target_srid"
        );
    }

    #[pg_test]
    fn test_solid_to_glb_embeds_srid_metadata() {
        set_search_path();
        // Default target_srid = 4326. JSON chunk of the GLB should embed
        // "srid":4326 and "axis_order":"longitude_deg...".
        let glb = Spi::get_one::<Vec<u8>>("SELECT solid_to_glb(solid_box(10, 10, 10))")
            .expect("SPI failed")
            .expect("NULL glb");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        assert!(
            json.contains("\"srid\":4326"),
            "GLB asset.extras missing srid=4326: {json}"
        );
        assert!(
            json.contains("longitude_deg"),
            "GLB asset.extras missing axis_order hint: {json}"
        );
    }

    #[pg_test]
    fn test_solid_to_glb_explicit_srid_passes_through() {
        set_search_path();
        let glb =
            Spi::get_one::<Vec<u8>>("SELECT solid_to_glb(solid_box(10, 10, 10), 0.1, 0.5, 32631)")
                .expect("SPI failed")
                .expect("NULL glb");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        assert!(
            json.contains("\"srid\":32631"),
            "GLB asset.extras missing srid=32631: {json}"
        );
    }

    #[pg_test]
    fn test_solid_to_glb_georef_path_transforms_vertices() {
        set_search_path();
        let path = ifc_georef_file();
        // Default target_srid=4326 + georef_path → tessellate the
        // FIRST IFC wall in local metres, then transform the vertex
        // buffer to (lon°, lat°, m). Parse the GLB and assert the
        // POSITION bbox lies near (4.35°, 50.83°).
        let glb = Spi::get_one::<Vec<u8>>(&format!(
            "SELECT solid_to_glb(solid, 0.1, 0.5, 4326, '{path}') \
             FROM ifc_elements('{path}') WHERE ifc_type = 'IfcWall' LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL glb");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        assert!(json.contains("\"srid\":4326"), "expected srid 4326: {json}");
        // glTF POSITION accessor includes a "min" array; pull the first
        // number out and verify it sits near 4.35° (lon) instead of
        // near 0 (the IFC-local x).
        let needle = "\"min\":[";
        let i = json
            .find(needle)
            .expect("no POSITION min array in glTF JSON");
        let after = &json[i + needle.len()..];
        let comma = after.find(',').unwrap();
        let lon_min: f64 = after[..comma].parse().expect("lon_min not f64");
        assert!(
            (4.30..4.40).contains(&lon_min),
            "expected POSITION min[0] near 4.35° (lon), got {lon_min} — \
             render-time georef did not run"
        );
    }

    #[pg_test]
    fn test_solid_to_glb_no_georef_path_stays_local() {
        set_search_path();
        let path = ifc_georef_file();
        // No georef_path → vertices stay in IFC local metres (~x=10).
        let glb = Spi::get_one::<Vec<u8>>(&format!(
            "SELECT solid_to_glb(solid) \
             FROM ifc_elements('{path}') \
             WHERE ifc_type = 'IfcWall' AND solid IS NOT NULL LIMIT 1"
        ))
        .expect("SPI failed")
        .expect("NULL glb");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        let i = json.find("\"min\":[").unwrap();
        let after = &json[i + "\"min\":[".len()..];
        let comma = after.find(',').unwrap();
        let lon_min: f64 = after[..comma].parse().unwrap();
        assert!(
            lon_min < 50.0,
            "expected local-metre coords (~7m), got {lon_min}"
        );
    }

    #[pg_test]
    fn test_solids_to_multi_glb_one_mesh_per_input() {
        set_search_path();
        let path = ifc_georef_file();
        let glb = Spi::get_one::<Vec<u8>>(&format!(
            "SELECT solids_to_multi_glb(\
                 array_agg(solid), \
                 array_agg(global_id), \
                 0.1, 0.5, 4326, '{path}'\
             ) FROM ifc_elements('{path}') WHERE solid IS NOT NULL"
        ))
        .expect("SPI failed")
        .expect("NULL glb");
        assert_eq!(&glb[0..4], b"glTF", "missing GLB magic");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        // The fixture has 1 wall — multi_glb should still emit a valid
        // GLB with mesh_count = 1 and one named node.
        assert!(
            json.contains("\"mesh_count\":1"),
            "mesh_count missing or != 1: {json}"
        );
        assert!(
            json.contains("\"name\":\"0005_WALL_____GUID\""),
            "expected wall name in nodes[0].name: {json}"
        );
        assert!(json.contains("\"srid\":4326"), "expected srid 4326: {json}");
    }

    #[pg_test]
    fn test_solids_to_multi_glb_local_when_no_georef() {
        set_search_path();
        let path = ifc_georef_file();
        // Without georef_path, vertices stay in local metres.
        let glb = Spi::get_one::<Vec<u8>>(&format!(
            "SELECT solids_to_multi_glb(array_agg(solid), array_agg(global_id)) \
             FROM ifc_elements('{path}') WHERE solid IS NOT NULL"
        ))
        .expect("SPI failed")
        .expect("NULL glb");
        let json_start = 20;
        let json_len = u32::from_le_bytes([glb[12], glb[13], glb[14], glb[15]]) as usize;
        let json = std::str::from_utf8(&glb[json_start..json_start + json_len])
            .expect("non-utf8 JSON chunk");
        let i = json.find("\"min\":[").unwrap();
        let after = &json[i + "\"min\":[".len()..];
        let comma = after.find(',').unwrap();
        let lon_min: f64 = after[..comma].parse().unwrap();
        assert!(
            lon_min < 50.0,
            "expected local-metre POSITION min, got {lon_min}"
        );
    }

    #[pg_test]
    fn test_solid_footprint_wkt_parses() {
        set_search_path();
        let wkt = Spi::get_one::<String>("SELECT solid_footprint_wkt(solid_box(100, 200, 300))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(wkt.starts_with("POLYGON Z (("), "wkt = {wkt}");
        assert!(wkt.matches(',').count() == 4, "5-vertex closed ring");
    }

    #[pg_test]
    fn test_solid_bbox_wkt_parses() {
        set_search_path();
        let wkt = Spi::get_one::<String>("SELECT solid_bbox_wkt(solid_box(100, 200, 300))")
            .expect("SPI failed")
            .expect("NULL");
        assert!(wkt.starts_with("POLYGON Z (("), "wkt = {wkt}");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
