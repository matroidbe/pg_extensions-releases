//! Output connector implementations

pub mod branch;
pub mod kafka;
pub mod opendal_sink;
pub mod table;

#[cfg(feature = "xarray")]
pub mod xarray_index;
