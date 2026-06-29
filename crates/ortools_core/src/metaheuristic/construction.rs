//! Construction heuristics for building initial feasible solutions.
//!
//! These algorithms assign items to slots one by one, building a complete
//! assignment from scratch. They run once at the start before improvement
//! algorithms take over.

use super::constraint::evaluate;
use super::problem::*;
use super::score::HardSoftScore;

/// First Fit: assign each item to the first slot that doesn't worsen hard score.
/// Falls back to slot 0 if no feasible slot is found.
pub fn first_fit(problem: &AssignmentProblem) -> Assignment {
    let mut assignment = vec![0usize; problem.item_count];

    for item in 0..problem.item_count {
        let mut best_slot = 0;
        let mut best_score = None;

        for slot in 0..problem.slot_count {
            assignment[item] = slot;
            let score = evaluate(problem, &assignment);
            if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                best_score = Some(score);
                best_slot = slot;
                // If feasible, take it (first fit)
                if score.is_feasible() {
                    break;
                }
            }
        }
        assignment[item] = best_slot;
    }

    assignment
}

/// First Fit Decreasing: sort items by difficulty (fewest feasible slots first),
/// then apply first fit. Harder items get first pick.
pub fn first_fit_decreasing(problem: &AssignmentProblem) -> Assignment {
    // Count feasible slots per item
    let mut item_difficulty: Vec<(usize, usize)> = (0..problem.item_count)
        .map(|item| {
            let feasible_count = count_feasible_slots(problem, item);
            (item, feasible_count)
        })
        .collect();

    // Sort by difficulty: fewest feasible slots first (hardest first)
    item_difficulty.sort_by_key(|&(_, count)| count);

    let mut assignment = vec![0usize; problem.item_count];
    let order: Vec<usize> = item_difficulty.iter().map(|&(item, _)| item).collect();

    for &item in &order {
        let mut best_slot = 0;
        let mut best_score: Option<HardSoftScore> = None;

        for slot in 0..problem.slot_count {
            assignment[item] = slot;
            let score = evaluate(problem, &assignment);
            if best_score.is_none() || score > *best_score.as_ref().unwrap() {
                best_score = Some(score);
                best_slot = slot;
                if score.is_feasible() {
                    break;
                }
            }
        }
        assignment[item] = best_slot;
    }

    assignment
}

/// Count how many slots are individually feasible for an item (ignoring other items).
fn count_feasible_slots(problem: &AssignmentProblem, item: usize) -> usize {
    let mut count = problem.slot_count;
    for constraint in &problem.constraints {
        if let TypedConstraint::Hard(HardConstraint::SkillMatch { feasible }) = constraint {
            if item < feasible.len() {
                count = count.min(feasible[item].iter().filter(|&&f| f).count());
            }
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_problem_with_skill_match() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 3,
            slot_count: 3,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::SkillMatch {
                    feasible: vec![
                        vec![true, false, false],
                        vec![false, true, true],
                        vec![true, true, true],
                    ],
                }),
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 }),
            ],
            pinned: vec![false; 3],
            item_data: (0..3)
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
    fn test_first_fit_feasible() {
        let problem = AssignmentProblem {
            item_count: 3,
            slot_count: 3,
            constraints: vec![TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 })],
            pinned: vec![false; 3],
            item_data: (0..3)
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
        };
        let assignment = first_fit(&problem);
        assert_eq!(assignment.len(), 3);
        let score = evaluate(&problem, &assignment);
        assert!(
            score.is_feasible(),
            "first fit should produce feasible solution"
        );
    }

    #[test]
    fn test_first_fit_respects_skill_match() {
        let problem = make_problem_with_skill_match();
        let assignment = first_fit(&problem);
        let score = evaluate(&problem, &assignment);
        assert_eq!(
            assignment[0], 0,
            "item 0 should be in its only feasible slot"
        );
        assert!(score.is_feasible(), "should be feasible");
    }

    #[test]
    fn test_first_fit_decreasing_order() {
        let problem = make_problem_with_skill_match();
        let assignment = first_fit_decreasing(&problem);
        let score = evaluate(&problem, &assignment);
        assert_eq!(assignment[0], 0, "hardest item (item 0) should get slot 0");
        assert!(score.is_feasible(), "should be feasible");
    }

    #[test]
    fn test_first_fit_decreasing_feasible() {
        let problem = make_problem_with_skill_match();
        let assignment = first_fit_decreasing(&problem);
        let score = evaluate(&problem, &assignment);
        assert!(score.is_feasible());
        let mut slots_used: Vec<usize> = assignment.clone();
        slots_used.sort();
        slots_used.dedup();
        assert_eq!(
            slots_used.len(),
            3,
            "each item should be in a different slot"
        );
    }

    #[test]
    fn test_construction_all_items_assigned() {
        let problem = make_problem_with_skill_match();
        let assignment = first_fit(&problem);
        assert_eq!(assignment.len(), problem.item_count);
        for &slot in &assignment {
            assert!(slot < problem.slot_count);
        }
    }
}
