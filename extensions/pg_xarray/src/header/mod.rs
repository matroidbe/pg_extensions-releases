//! File-header walkers — open a file URI, read metadata, return one
//! catalog-ready record per chunk in the file.
//!
//! Used by `pgx.register_file` to populate the catalog WITHOUT going
//! through a `pg_streaming` pipeline. The Zarr walker lives in the
//! shared `pgx_zarr_walker` rlib so that `pg_streaming`'s
//! `xarray_header` processor uses the EXACT SAME logic (and produces
//! the same per-chunk bbox).

// Pub re-exports — Step 5 of the dtype+metadata plan will consume
// `VariableMeta` / `VariableWalk` inside this crate; until then they're
// public surface for downstream-only use.
#[allow(unused_imports)]
pub use pgx_zarr_walker::{
    enumerate_zarr_chunks, list_store_variables, ChunkRecord, StoreVariable, VariableMeta,
    VariableWalk,
};
