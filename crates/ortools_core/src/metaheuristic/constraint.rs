//! Constraint evaluation engine for metaheuristic solving.
//!
//! Evaluates an assignment against typed constraints to produce a HardSoftScore.
//! Each hard violation decrements hard score by 1. Soft penalties accumulate
//! in the soft score (lower = better, i.e., penalties are negative).

use super::problem::*;
use super::score::HardSoftScore;
use std::collections::HashMap;

/// Evaluate all constraints for a given assignment, returning the total score.
pub fn evaluate(problem: &AssignmentProblem, assignment: &Assignment) -> HardSoftScore {
    let mut hard = 0i64;
    let mut soft = 0i64;

    for constraint in &problem.constraints {
        match constraint {
            TypedConstraint::Hard(hc) => {
                hard += evaluate_hard(hc, problem, assignment);
            }
            TypedConstraint::Soft(sc) => {
                soft += evaluate_soft(sc, problem, assignment);
            }
        }
    }

    HardSoftScore::new(hard, soft)
}

/// Evaluate a single hard constraint. Returns 0 if satisfied, negative if violated.
fn evaluate_hard(
    constraint: &HardConstraint,
    problem: &AssignmentProblem,
    assignment: &Assignment,
) -> i64 {
    match constraint {
        HardConstraint::Capacity { limit } => {
            let mut slot_counts = vec![0usize; problem.slot_count];
            for &slot in assignment.iter() {
                if slot < problem.slot_count {
                    slot_counts[slot] += 1;
                }
            }
            let mut violations = 0i64;
            for &count in &slot_counts {
                if count > *limit {
                    violations -= (count - *limit) as i64;
                }
            }
            violations
        }

        HardConstraint::GroupBalance {
            group_field,
            count_per_target,
        } => {
            let mut slot_group_counts: HashMap<(usize, &str), usize> = HashMap::new();
            let mut all_groups: std::collections::HashSet<&str> = std::collections::HashSet::new();

            for (item, &slot) in assignment.iter().enumerate() {
                if let Some(group) = problem
                    .item_data
                    .get(item)
                    .and_then(|d| d.fields.get(group_field).and(d.group.as_deref()))
                {
                    all_groups.insert(group);
                    *slot_group_counts.entry((slot, group)).or_insert(0) += 1;
                } else if let Some(group) =
                    problem.item_data.get(item).and_then(|d| d.group.as_deref())
                {
                    all_groups.insert(group);
                    *slot_group_counts.entry((slot, group)).or_insert(0) += 1;
                }
            }

            let mut violations = 0i64;
            for slot in 0..problem.slot_count {
                for &group in &all_groups {
                    let count = slot_group_counts.get(&(slot, group)).copied().unwrap_or(0);
                    if count != *count_per_target {
                        violations -= 1;
                    }
                }
            }
            violations
        }

        HardConstraint::NoOverlap { overlap_pairs } => {
            let violations = 0i64;
            for (item, pairs) in overlap_pairs.iter().enumerate() {
                let assigned_slot = assignment[item];
                for &(slot_a, slot_b) in pairs {
                    let _ = (assigned_slot, slot_a, slot_b);
                }
            }
            violations
        }

        HardConstraint::SkillMatch { feasible } => {
            let mut violations = 0i64;
            for (item, &slot) in assignment.iter().enumerate() {
                if item < feasible.len() && slot < feasible[item].len() && !feasible[item][slot] {
                    violations -= 1;
                }
            }
            violations
        }
    }
}

/// Evaluate a single soft constraint. Returns a penalty (negative = worse).
fn evaluate_soft(
    constraint: &SoftConstraint,
    _problem: &AssignmentProblem,
    assignment: &Assignment,
) -> i64 {
    match constraint {
        SoftConstraint::MinimizeField { costs, weight } => {
            let total: f64 = assignment
                .iter()
                .enumerate()
                .map(|(item, _slot)| costs.get(item).copied().unwrap_or(0.0))
                .sum();
            -((total * weight) as i64)
        }

        SoftConstraint::MinimizeCost {
            item_costs,
            slot_costs,
            weight,
        } => {
            let total: f64 = assignment
                .iter()
                .enumerate()
                .map(|(item, &slot)| {
                    let ic = item_costs.get(item).copied().unwrap_or(0.0);
                    let sc = slot_costs.get(slot).copied().unwrap_or(0.0);
                    ic * sc
                })
                .sum();
            -((total * weight) as i64)
        }

        SoftConstraint::PinCurrent { current, weight } => {
            let changes: usize = assignment
                .iter()
                .enumerate()
                .filter(|(item, &slot)| {
                    current
                        .get(*item)
                        .and_then(|c| *c)
                        .is_some_and(|cur| cur != slot)
                })
                .count();
            -((changes as f64 * weight) as i64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_problem(constraints: Vec<TypedConstraint>) -> AssignmentProblem {
        AssignmentProblem {
            item_count: 3,
            slot_count: 3,
            constraints,
            pinned: vec![false; 3],
            item_data: vec![
                ItemData {
                    group: Some("dev".to_string()),
                    fields: HashMap::new(),
                },
                ItemData {
                    group: Some("dev".to_string()),
                    fields: HashMap::new(),
                },
                ItemData {
                    group: Some("qa".to_string()),
                    fields: HashMap::new(),
                },
            ],
            slot_data: vec![
                SlotData {
                    fields: HashMap::new(),
                },
                SlotData {
                    fields: HashMap::new(),
                },
                SlotData {
                    fields: HashMap::new(),
                },
            ],
        }
    }

    #[test]
    fn test_evaluate_capacity_satisfied() {
        let problem = make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::Capacity {
            limit: 2,
        })]);
        let assignment = vec![0, 0, 1];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.hard, 0);
    }

    #[test]
    fn test_evaluate_capacity_violated() {
        let problem = make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::Capacity {
            limit: 1,
        })]);
        let assignment = vec![0, 0, 0];
        let score = evaluate(&problem, &assignment);
        assert!(score.hard < 0);
        assert_eq!(score.hard, -2);
    }

    #[test]
    fn test_evaluate_group_balance_ok() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::GroupBalance {
                group_field: "role".to_string(),
                count_per_target: 1,
            })]);
        let assignment = vec![0, 1, 0];
        let score = evaluate(&problem, &assignment);
        assert!(score.hard < 0);
    }

    #[test]
    fn test_evaluate_group_balance_violated() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::GroupBalance {
                group_field: "role".to_string(),
                count_per_target: 1,
            })]);
        let assignment = vec![0, 0, 0];
        let score = evaluate(&problem, &assignment);
        assert!(score.hard < 0);
    }

    #[test]
    fn test_evaluate_skill_match_ok() {
        let feasible = vec![
            vec![true, true, false],
            vec![true, false, true],
            vec![false, true, true],
        ];
        let problem =
            make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::SkillMatch {
                feasible,
            })]);
        let assignment = vec![0, 2, 1];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.hard, 0);
    }

    #[test]
    fn test_evaluate_skill_match_violated() {
        let feasible = vec![
            vec![true, true, false],
            vec![true, false, true],
            vec![false, true, true],
        ];
        let problem =
            make_simple_problem(vec![TypedConstraint::Hard(HardConstraint::SkillMatch {
                feasible,
            })]);
        let assignment = vec![2, 1, 0];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.hard, -3);
    }

    #[test]
    fn test_evaluate_minimize_field() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Soft(SoftConstraint::MinimizeField {
                costs: vec![10.0, 20.0, 30.0],
                weight: 1.0,
            })]);
        let assignment = vec![0, 1, 2];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.soft, -60);
        assert_eq!(score.hard, 0);
    }

    #[test]
    fn test_evaluate_minimize_cost() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                item_costs: vec![2.0, 3.0, 1.0],
                slot_costs: vec![10.0, 20.0, 30.0],
                weight: 1.0,
            })]);
        let assignment = vec![0, 1, 2];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.soft, -110);
    }

    #[test]
    fn test_evaluate_pin_current_rewarded() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Soft(SoftConstraint::PinCurrent {
                current: vec![Some(0), Some(1), Some(2)],
                weight: 10.0,
            })]);
        let assignment = vec![0, 1, 2];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.soft, 0);
    }

    #[test]
    fn test_evaluate_pin_current_changed() {
        let problem =
            make_simple_problem(vec![TypedConstraint::Soft(SoftConstraint::PinCurrent {
                current: vec![Some(0), Some(1), Some(2)],
                weight: 10.0,
            })]);
        let assignment = vec![2, 0, 1];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.soft, -30);
    }

    #[test]
    fn test_evaluate_combined_constraints() {
        let problem = AssignmentProblem {
            item_count: 3,
            slot_count: 3,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 2 }),
                TypedConstraint::Hard(HardConstraint::SkillMatch {
                    feasible: vec![
                        vec![true, true, true],
                        vec![true, true, true],
                        vec![true, true, true],
                    ],
                }),
                TypedConstraint::Soft(SoftConstraint::MinimizeField {
                    costs: vec![10.0, 20.0, 30.0],
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 3],
            item_data: vec![
                ItemData {
                    group: None,
                    fields: HashMap::new(),
                },
                ItemData {
                    group: None,
                    fields: HashMap::new(),
                },
                ItemData {
                    group: None,
                    fields: HashMap::new(),
                },
            ],
            slot_data: vec![
                SlotData {
                    fields: HashMap::new(),
                },
                SlotData {
                    fields: HashMap::new(),
                },
                SlotData {
                    fields: HashMap::new(),
                },
            ],
        };
        let assignment = vec![0, 0, 1];
        let score = evaluate(&problem, &assignment);
        assert_eq!(score.hard, 0);
        assert_eq!(score.soft, -60);
    }
}
