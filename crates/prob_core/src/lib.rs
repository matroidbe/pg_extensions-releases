//! prob_core: pure Rust probabilistic distribution engine
//!
//! The distribution model behind pg_prob, extracted so it can run anywhere —
//! inside PostgreSQL (via the pg_prob pgrx wrappers) or embedded in another
//! process (e.g. Eidos' SQLite mode registering these as SQL functions).
//!
//! A `Dist` represents either:
//! - A literal (certain) value
//! - A parametric distribution (normal, uniform, etc.)
//! - A computed distribution (lazy expression tree)
//!
//! Arithmetic builds lazy expression trees; Monte Carlo sampling propagates
//! uncertainty through them.
//!
//! The serde JSON shape (`{"t": ..., "p": ...}`) is the on-disk format used by
//! pg_prob's `dist` type — it MUST stay byte-identical so values written by
//! PostgreSQL and by embedded consumers are interchangeable.

pub mod constructors;
pub mod dist;
mod error;
pub mod fit;
pub mod ops;
pub mod sample;

pub use dist::{Dist, DistParams, DistType};
pub use error::ProbError;
