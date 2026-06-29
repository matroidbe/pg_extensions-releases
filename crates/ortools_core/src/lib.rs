//! ortools_core: Pure Rust optimization engine
//!
//! Provides two solving approaches:
//! - MIP (Mixed Integer Programming) via HiGHS solver
//! - Metaheuristic local search (Hill Climbing, Tabu Search, SA, Late Acceptance)
//!
//! This crate has zero database dependencies. Consumers load data into the
//! provided data structures and call solver functions directly.

pub mod error;
pub mod metaheuristic;
pub mod mip;

pub use error::OrtoolsCoreError;
pub use metaheuristic::{
    Algorithm, Assignment, AssignmentProblem, HardConstraint, HardSoftScore, LocalSearchResult,
    SoftConstraint, TypedConstraint,
};
pub use mip::{ConstraintData, ProblemData, VariableData};
