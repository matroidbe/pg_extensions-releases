//! Assignment problem representation for metaheuristic solving.
//!
//! An assignment problem maps items to slots (e.g., employees to shifts).
//! Each item is assigned exactly one slot. The solver optimizes which
//! item goes to which slot, subject to hard and soft constraints.

use std::collections::HashMap;

/// Assignment problem: assign `item_count` items to `slot_count` slots.
#[derive(Debug, Clone)]
pub struct AssignmentProblem {
    pub item_count: usize,
    pub slot_count: usize,
    pub constraints: Vec<TypedConstraint>,
    /// pinned[i] = true means item i cannot be moved by the solver.
    pub pinned: Vec<bool>,
    pub item_data: Vec<ItemData>,
    pub slot_data: Vec<SlotData>,
}

/// Metadata for one item (e.g., an employee).
#[derive(Debug, Clone)]
pub struct ItemData {
    pub group: Option<String>,
    pub fields: HashMap<String, f64>,
}

/// Metadata for one slot (e.g., a shift).
#[derive(Debug, Clone)]
pub struct SlotData {
    pub fields: HashMap<String, f64>,
}

/// An assignment: `assignment[item_index]` = slot_index.
pub type Assignment = Vec<usize>;

/// A constraint with its kind (hard or soft).
#[derive(Debug, Clone)]
pub enum TypedConstraint {
    Hard(HardConstraint),
    Soft(SoftConstraint),
}

/// Hard constraints must be satisfied for a feasible solution.
#[derive(Debug, Clone)]
pub enum HardConstraint {
    /// Each slot can have at most `limit` items assigned to it.
    Capacity { limit: usize },
    /// For each slot, each group value must have exactly `count_per_target` items.
    GroupBalance {
        group_field: String,
        count_per_target: usize,
    },
    /// Pre-computed overlap pairs: if items assigned to both slots in a pair
    /// by the same item, it violates the constraint.
    /// `overlap_pairs[item]` = list of (slot_a, slot_b) that overlap.
    NoOverlap {
        overlap_pairs: Vec<Vec<(usize, usize)>>,
    },
    /// Pre-computed feasibility matrix: `feasible[item][slot]` = true if allowed.
    SkillMatch { feasible: Vec<Vec<bool>> },
}

/// Soft constraints are penalized in the objective (lower penalty = better).
#[derive(Debug, Clone)]
pub enum SoftConstraint {
    /// Minimize weighted sum of item costs: weight * Σ costs[item] (for assigned items).
    MinimizeField { costs: Vec<f64>, weight: f64 },
    /// Minimize weighted cross-product: weight * Σ item_costs[i] * slot_costs[j] * x[i][j].
    MinimizeCost {
        item_costs: Vec<f64>,
        slot_costs: Vec<f64>,
        weight: f64,
    },
    /// Reward keeping current assignments. Penalty for each changed assignment.
    PinCurrent {
        current: Vec<Option<usize>>,
        weight: f64,
    },
}
