use pgrx::prelude::*;
use pgrx::Internal;

use crate::types::bbox3d::BBox3D;
use crate::types::solid::Solid;

/// Extract BBox3D from a GISTENTRY key datum.
unsafe fn bbox_from_datum(datum: pg_sys::Datum) -> BBox3D {
    pgrx::FromDatum::from_datum(datum, false).expect("GiST entry key should be a valid bbox3d")
}

// ===== 1. consistent =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_consistent(
    entry: Internal,
    query: Solid,
    strategy: i16,
    _subtype: pg_sys::Oid,
    recheck: Internal,
) -> bool {
    let entry_ptr = entry.unwrap().unwrap().cast_mut_ptr::<pg_sys::GISTENTRY>();
    let key = unsafe { bbox_from_datum((*entry_ptr).key) };
    let query_bbox = query.header.bbox3d();

    // Set recheck to false — bbox overlap is the exact semantics of &&
    let recheck_ptr = recheck.unwrap().unwrap().cast_mut_ptr::<bool>();
    unsafe {
        *recheck_ptr = false;
    }

    match strategy as u32 {
        pg_sys::RTOverlapStrategyNumber => key.overlaps(&query_bbox),
        _ => {
            pgrx::error!("solid_gist_consistent: unsupported strategy {strategy}");
        }
    }
}

// ===== 2. union =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_union(entry_vec: Internal, size: Internal) -> BBox3D {
    let vec_ptr = entry_vec
        .unwrap()
        .unwrap()
        .cast_mut_ptr::<pg_sys::GistEntryVector>();
    let n = unsafe { (*vec_ptr).n as usize };
    let entries = unsafe { std::slice::from_raw_parts((*vec_ptr).vector.as_ptr(), n) };

    let mut result = unsafe { bbox_from_datum(entries[0].key) };
    for entry in entries.iter().skip(1) {
        let bbox = unsafe { bbox_from_datum(entry.key) };
        result = result.union(&bbox);
    }

    // Write size output
    let size_ptr = size.unwrap().unwrap().cast_mut_ptr::<i32>();
    unsafe {
        *size_ptr = std::mem::size_of::<BBox3D>() as i32;
    }

    result
}

// ===== 3. compress =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_compress(entry: Internal) -> Internal {
    let entry_ptr = entry.unwrap().unwrap().cast_mut_ptr::<pg_sys::GISTENTRY>();

    let is_leaf = unsafe { (*entry_ptr).leafkey };

    if is_leaf {
        // Leaf node: extract bbox from solid header
        let solid: Solid = unsafe {
            pgrx::FromDatum::from_datum((*entry_ptr).key, false)
                .expect("leaf entry should be a valid solid")
        };
        let bbox = solid.header.bbox3d();
        let bbox_datum = pgrx::IntoDatum::into_datum(bbox).expect("bbox3d should serialize");

        // Allocate a new GISTENTRY with the bbox as key
        unsafe {
            let ret =
                pg_sys::palloc(std::mem::size_of::<pg_sys::GISTENTRY>()) as *mut pg_sys::GISTENTRY;
            (*ret).key = bbox_datum;
            (*ret).rel = (*entry_ptr).rel;
            (*ret).page = (*entry_ptr).page;
            (*ret).offset = (*entry_ptr).offset;
            (*ret).leafkey = false; // now a bbox, not the original solid
            Internal::new(*ret)
        }
    } else {
        // Internal node: already a bbox3d, pass through
        // Need to reconstruct Internal from the raw pointer since we consumed it
        Internal::new(unsafe { *entry_ptr })
    }
}

// ===== 4. decompress =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_decompress(entry: Internal) -> Internal {
    entry
}

// ===== 5. penalty =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_penalty(orig: Internal, new_entry: Internal, penalty: Internal) -> Internal {
    let orig_ptr = orig.unwrap().unwrap().cast_mut_ptr::<pg_sys::GISTENTRY>();
    let new_ptr = new_entry
        .unwrap()
        .unwrap()
        .cast_mut_ptr::<pg_sys::GISTENTRY>();
    let penalty_ptr = penalty.unwrap().unwrap().cast_mut_ptr::<f32>();

    let orig_bbox = unsafe { bbox_from_datum((*orig_ptr).key) };
    let new_bbox = unsafe { bbox_from_datum((*new_ptr).key) };

    unsafe {
        *penalty_ptr = orig_bbox.enlargement(&new_bbox) as f32;
    }

    // Return the penalty pointer as Internal
    Internal::new(penalty_ptr)
}

// ===== 6. picksplit =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_picksplit(entry_vec: Internal, split_vec: Internal) -> Internal {
    let vec_ptr = entry_vec
        .unwrap()
        .unwrap()
        .cast_mut_ptr::<pg_sys::GistEntryVector>();
    let split_ptr = split_vec
        .unwrap()
        .unwrap()
        .cast_mut_ptr::<pg_sys::GIST_SPLITVEC>();

    let n = unsafe { (*vec_ptr).n as usize };
    let entries = unsafe { std::slice::from_raw_parts((*vec_ptr).vector.as_ptr(), n) };

    // Compute bboxes and centers
    let bboxes: Vec<BBox3D> = (0..n)
        .map(|i| unsafe { bbox_from_datum(entries[i].key) })
        .collect();

    // Find axis with largest spread of centers
    let (mut min_x, mut max_x) = (f32::MAX, f32::MIN);
    let (mut min_y, mut max_y) = (f32::MAX, f32::MIN);
    let (mut min_z, mut max_z) = (f32::MAX, f32::MIN);

    for bbox in &bboxes {
        let cx = (bbox.xmin + bbox.xmax) / 2.0;
        let cy = (bbox.ymin + bbox.ymax) / 2.0;
        let cz = (bbox.zmin + bbox.zmax) / 2.0;
        min_x = min_x.min(cx);
        max_x = max_x.max(cx);
        min_y = min_y.min(cy);
        max_y = max_y.max(cy);
        min_z = min_z.min(cz);
        max_z = max_z.max(cz);
    }

    let spread_x = max_x - min_x;
    let spread_y = max_y - min_y;
    let spread_z = max_z - min_z;

    // Sort indices by center along longest axis
    let mut indices: Vec<usize> = (0..n).collect();
    if spread_x >= spread_y && spread_x >= spread_z {
        indices.sort_by(|&a, &b| {
            let ca = (bboxes[a].xmin + bboxes[a].xmax) / 2.0;
            let cb = (bboxes[b].xmin + bboxes[b].xmax) / 2.0;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });
    } else if spread_y >= spread_z {
        indices.sort_by(|&a, &b| {
            let ca = (bboxes[a].ymin + bboxes[a].ymax) / 2.0;
            let cb = (bboxes[b].ymin + bboxes[b].ymax) / 2.0;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        indices.sort_by(|&a, &b| {
            let ca = (bboxes[a].zmin + bboxes[a].zmax) / 2.0;
            let cb = (bboxes[b].zmin + bboxes[b].zmax) / 2.0;
            ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    // Split at midpoint
    let mid = n / 2;

    // Allocate offset arrays via palloc
    let left = unsafe {
        pg_sys::palloc(mid * std::mem::size_of::<pg_sys::OffsetNumber>())
            as *mut pg_sys::OffsetNumber
    };
    let right = unsafe {
        pg_sys::palloc((n - mid) * std::mem::size_of::<pg_sys::OffsetNumber>())
            as *mut pg_sys::OffsetNumber
    };

    // Build left side
    let mut left_bbox = bboxes[indices[0]].clone();
    for i in 0..mid {
        unsafe {
            *left.add(i) = (indices[i] + 1) as pg_sys::OffsetNumber;
        }
        if i > 0 {
            left_bbox = left_bbox.union(&bboxes[indices[i]]);
        }
    }

    // Build right side
    let mut right_bbox = bboxes[indices[mid]].clone();
    for i in mid..n {
        unsafe {
            *right.add(i - mid) = (indices[i] + 1) as pg_sys::OffsetNumber;
        }
        if i > mid {
            right_bbox = right_bbox.union(&bboxes[indices[i]]);
        }
    }

    unsafe {
        (*split_ptr).spl_left = left;
        (*split_ptr).spl_nleft = mid as i32;
        (*split_ptr).spl_ldatum = pgrx::IntoDatum::into_datum(left_bbox).unwrap();
        (*split_ptr).spl_right = right;
        (*split_ptr).spl_nright = (n - mid) as i32;
        (*split_ptr).spl_rdatum = pgrx::IntoDatum::into_datum(right_bbox).unwrap();
    }

    Internal::new(split_ptr)
}

// ===== 7. same =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_same(a: BBox3D, b: BBox3D, result: Internal) -> Internal {
    let result_ptr = result.unwrap().unwrap().cast_mut_ptr::<bool>();
    unsafe {
        *result_ptr = a.same(&b);
    }
    Internal::new(result_ptr)
}

// ===== 8. distance (for KNN) =====
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_gist_distance(
    entry: Internal,
    query: Solid,
    strategy: i16,
    _subtype: pg_sys::Oid,
    recheck: Internal,
) -> f64 {
    let entry_ptr = entry.unwrap().unwrap().cast_mut_ptr::<pg_sys::GISTENTRY>();
    let key = unsafe { bbox_from_datum((*entry_ptr).key) };
    let query_bbox = query.header.bbox3d();

    let recheck_ptr = recheck.unwrap().unwrap().cast_mut_ptr::<bool>();

    match strategy as u32 {
        pg_sys::RTKNNSearchStrategyNumber => {
            // <-> operator: AABB distance is exact (matches operator function)
            unsafe {
                *recheck_ptr = false;
            }
        }
        _ => {
            // Other distance strategies: AABB is a lower bound, need recheck
            unsafe {
                *recheck_ptr = true;
            }
        }
    }

    key.min_distance(&query_bbox)
}

// ===== Operator class registration =====

pgrx::extension_sql!(
    r#"
CREATE OPERATOR CLASS @extschema@.solid_gist_ops
    DEFAULT FOR TYPE @extschema@.solid USING gist AS
    OPERATOR  3  @extschema@.&& (@extschema@.solid, @extschema@.solid),
    OPERATOR  15 @extschema@.<-> (@extschema@.solid, @extschema@.solid) FOR ORDER BY pg_catalog.float_ops,
    FUNCTION  1  @extschema@.solid_gist_consistent(internal, @extschema@.solid, int2, oid, internal),
    FUNCTION  2  @extschema@.solid_gist_union(internal, internal),
    FUNCTION  3  @extschema@.solid_gist_compress(internal),
    FUNCTION  4  @extschema@.solid_gist_decompress(internal),
    FUNCTION  5  @extschema@.solid_gist_penalty(internal, internal, internal),
    FUNCTION  6  @extschema@.solid_gist_picksplit(internal, internal),
    FUNCTION  7  @extschema@.solid_gist_same(@extschema@.bbox3d, @extschema@.bbox3d, internal),
    FUNCTION  8  @extschema@.solid_gist_distance(internal, @extschema@.solid, int2, oid, internal),
    STORAGE      @extschema@.bbox3d;
"#,
    name = "solid_gist_ops",
    requires = [
        "overlap_operators",
        solid_gist_consistent,
        solid_gist_union,
        solid_gist_compress,
        solid_gist_decompress,
        solid_gist_penalty,
        solid_gist_picksplit,
        solid_gist_same,
        solid_gist_distance,
    ]
);
