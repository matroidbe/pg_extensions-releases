//! CP-SAT engine: lazy-clause-generation constraint programming for scheduling.
//!
//! A third solver engine beside [`crate::mip`] (HiGHS) and
//! [`crate::metaheuristic`] (local search), backed by the pure-Rust
//! [Pumpkin](https://github.com/ConSol-Lab/Pumpkin) solver. Unlike a
//! time-indexed MIP re-encoding, this uses real propagation (LCG + global
//! constraints), so it scales on scheduling problems the way CP-SAT does.
//!
//! See `design/pg_ortools/cpsat-engine.md` for the full design.

pub mod model;
mod pumpkin;

pub use model::{Interval, Model, Precedence, Solution, SolveLimits, SolveOutcome};
