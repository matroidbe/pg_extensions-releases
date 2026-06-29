//! Move types for local search algorithms.
//!
//! Moves are the atomic operations that modify an assignment during search.
//! Each move has an O(1) apply and undo operation.

use super::problem::{Assignment, AssignmentProblem};
use rand::Rng;

/// A move that modifies an assignment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Move {
    /// Reassign one item to a different slot.
    Change {
        item: usize,
        old_slot: usize,
        new_slot: usize,
    },
    /// Swap the slot assignments of two items.
    Swap { item_a: usize, item_b: usize },
}

impl Move {
    /// Apply this move to an assignment. O(1).
    pub fn apply(&self, assignment: &mut Assignment) {
        match self {
            Move::Change { item, new_slot, .. } => {
                assignment[*item] = *new_slot;
            }
            Move::Swap { item_a, item_b } => {
                assignment.swap(*item_a, *item_b);
            }
        }
    }

    /// Undo this move (restore previous state). O(1).
    pub fn undo(&self, assignment: &mut Assignment) {
        match self {
            Move::Change { item, old_slot, .. } => {
                assignment[*item] = *old_slot;
            }
            Move::Swap { item_a, item_b } => {
                // Swap is self-inverse
                assignment.swap(*item_a, *item_b);
            }
        }
    }
}

/// Generate a random move for the given assignment, skipping pinned items.
/// Returns None if no moves are possible (all items pinned or trivial problem).
pub fn generate_random_move(
    problem: &AssignmentProblem,
    assignment: &Assignment,
    rng: &mut impl Rng,
) -> Option<Move> {
    let unpinned: Vec<usize> = (0..problem.item_count)
        .filter(|&i| !problem.pinned[i])
        .collect();

    if unpinned.is_empty() {
        return None;
    }

    // 70% chance of ChangeMove, 30% chance of SwapMove (if possible)
    let do_swap = unpinned.len() >= 2 && rng.gen_ratio(3, 10);

    if do_swap {
        let idx_a = rng.gen_range(0..unpinned.len());
        let mut idx_b = rng.gen_range(0..unpinned.len() - 1);
        if idx_b >= idx_a {
            idx_b += 1;
        }
        Some(Move::Swap {
            item_a: unpinned[idx_a],
            item_b: unpinned[idx_b],
        })
    } else {
        let item = unpinned[rng.gen_range(0..unpinned.len())];
        let old_slot = assignment[item];
        if problem.slot_count <= 1 {
            return None;
        }
        let mut new_slot = rng.gen_range(0..problem.slot_count - 1);
        if new_slot >= old_slot {
            new_slot += 1;
        }
        Some(Move::Change {
            item,
            old_slot,
            new_slot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metaheuristic::problem::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::collections::HashMap;

    fn make_test_problem(pinned: Vec<bool>) -> AssignmentProblem {
        let n = pinned.len();
        AssignmentProblem {
            item_count: n,
            slot_count: 3,
            constraints: vec![],
            pinned,
            item_data: (0..n)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..3)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn test_change_move_apply() {
        let mut assignment = vec![0, 1, 2];
        let mv = Move::Change {
            item: 0,
            old_slot: 0,
            new_slot: 2,
        };
        mv.apply(&mut assignment);
        assert_eq!(assignment, vec![2, 1, 2]);
    }

    #[test]
    fn test_change_move_undo() {
        let mut assignment = vec![0, 1, 2];
        let mv = Move::Change {
            item: 0,
            old_slot: 0,
            new_slot: 2,
        };
        mv.apply(&mut assignment);
        assert_eq!(assignment[0], 2);
        mv.undo(&mut assignment);
        assert_eq!(assignment, vec![0, 1, 2]);
    }

    #[test]
    fn test_swap_move_apply() {
        let mut assignment = vec![0, 1, 2];
        let mv = Move::Swap {
            item_a: 0,
            item_b: 2,
        };
        mv.apply(&mut assignment);
        assert_eq!(assignment, vec![2, 1, 0]);
    }

    #[test]
    fn test_swap_move_undo() {
        let mut assignment = vec![0, 1, 2];
        let mv = Move::Swap {
            item_a: 0,
            item_b: 2,
        };
        mv.apply(&mut assignment);
        mv.undo(&mut assignment);
        assert_eq!(assignment, vec![0, 1, 2]);
    }

    #[test]
    fn test_random_move_skips_pinned() {
        let problem = make_test_problem(vec![true, false, false]);
        let assignment = vec![0, 1, 2];
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            if let Some(mv) = generate_random_move(&problem, &assignment, &mut rng) {
                match &mv {
                    Move::Change { item, .. } => assert_ne!(*item, 0, "pinned item 0 was moved"),
                    Move::Swap { item_a, item_b } => {
                        assert_ne!(*item_a, 0, "pinned item 0 in swap");
                        assert_ne!(*item_b, 0, "pinned item 0 in swap");
                    }
                }
            }
        }
    }

    #[test]
    fn test_random_move_coverage() {
        let problem = make_test_problem(vec![false, false, false]);
        let assignment = vec![0, 1, 2];
        let mut rng = StdRng::seed_from_u64(123);

        let mut has_change = false;
        let mut has_swap = false;
        for _ in 0..200 {
            if let Some(mv) = generate_random_move(&problem, &assignment, &mut rng) {
                match mv {
                    Move::Change { .. } => has_change = true,
                    Move::Swap { .. } => has_swap = true,
                }
            }
        }
        assert!(has_change, "should generate Change moves");
        assert!(has_swap, "should generate Swap moves");
    }

    #[test]
    fn test_random_move_all_pinned_returns_none() {
        let problem = make_test_problem(vec![true, true, true]);
        let assignment = vec![0, 1, 2];
        let mut rng = StdRng::seed_from_u64(42);
        assert!(generate_random_move(&problem, &assignment, &mut rng).is_none());
    }
}
