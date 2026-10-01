//! Output connector implementations

pub mod branch;
pub mod call;
pub mod kafka;
pub mod modbus;
pub mod opendal_sink;
pub mod s7;
pub mod table;

#[cfg(feature = "xarray")]
pub mod xarray_index;
