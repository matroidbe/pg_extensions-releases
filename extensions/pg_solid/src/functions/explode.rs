use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

/// Decompose a compound solid into its individual solid children.
/// If the input contains a single solid body, returns it as-is (1 row).
/// If the input is a compound (e.g., from STEP import or boolean ops),
/// returns each solid body as a separate row.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_explode(s: Solid) -> SetOfIterator<'static, Solid> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_explode: {e}"));

    let count =
        ffi::explode::solid_count(&shape).unwrap_or_else(|e| pgrx::error!("solid_explode: {e}"));

    if count == 0 {
        // Shape has no solid children (e.g., wire/edge compound from slice)
        return SetOfIterator::new(vec![s]);
    }

    let mut solids = Vec::with_capacity(count as usize);
    for i in 0..count {
        let child = ffi::explode::solid_at(&shape, i)
            .unwrap_or_else(|e| pgrx::error!("solid_explode: child {i}: {e}"));
        let solid = Solid::from_occt_shape(&child)
            .unwrap_or_else(|e| pgrx::error!("solid_explode: child {i}: {e}"));
        solids.push(solid);
    }

    SetOfIterator::new(solids)
}

/// Count the number of solid bodies in a shape.
/// A simple solid returns 1. A compound from STEP import may return N.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_num_solids(s: Solid) -> i32 {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_num_solids: {e}"));
    ffi::explode::solid_count(&shape).unwrap_or_else(|e| pgrx::error!("solid_num_solids: {e}"))
}
