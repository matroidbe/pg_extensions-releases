//! Metaheuristic solver engine for constraint optimization.
//!
//! Provides move-based local search algorithms (Tabu Search, Simulated Annealing,
//! Late Acceptance, Hill Climbing) alongside construction heuristics (First Fit,
//! First Fit Decreasing) for assignment problems.

pub mod algorithms;
pub mod constraint;
pub mod construction;
pub mod move_types;
pub mod orientation;
pub mod parsing;
pub mod problem;
pub mod score;

pub use algorithms::{solve_local, Algorithm, LocalSearchResult};
pub use orientation::{
    build_assignment_problem, format_oriented_result, orient_constraints, parse_orientation,
    Orientation,
};
pub use parsing::{
    format_result, is_valid_constraint_type, parse_algorithm, parse_typed_constraint,
    parse_var_indices,
};
pub use problem::{
    Assignment, AssignmentProblem, HardConstraint, ItemData, SlotData, SoftConstraint,
    TypedConstraint,
};
pub use score::HardSoftScore;
