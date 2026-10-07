//! Assignment orientation: which side of `x_i_j` gets exactly one partner.
//!
//! The engine assigns each item exactly one slot (`assignment[item] = slot`).
//! A rostering caller means the opposite — every *slot* (shift) gets exactly one
//! *item* (employee), and an item takes many slots. Under `{"each": "slot"}`
//! (typed constraint `assignment`) the caller's problem is handed to the engine
//! transposed: the caller's slots become the engine's items. Constraint configs
//! stay in the caller's terms and are converted here; the solution is mapped
//! back with [`caller_value`]. See `design/pg_ortools/metaheuristic.md`.

use super::problem::*;
use crate::error::OrtoolsCoreError;

/// Which side gets exactly one partner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Orientation {
    /// Each item gets exactly one slot — the engine's own shape.
    #[default]
    EachItem,
    /// Each slot gets exactly one item; an item may take many slots.
    EachSlot,
}

/// Parse an `assignment` typed constraint's config: `{"each": "item" | "slot"}`.
pub fn parse_orientation(config: &serde_json::Value) -> Result<Orientation, OrtoolsCoreError> {
    match config.get("each").and_then(|v| v.as_str()) {
        Some("item") => Ok(Orientation::EachItem),
        Some("slot") => Ok(Orientation::EachSlot),
        other => Err(OrtoolsCoreError::InvalidConstraint(format!(
            "assignment: \"each\" must be \"item\" or \"slot\", got {other:?}"
        ))),
    }
}

/// The engine's (item, slot) for the caller's variable `x_i_j`.
pub fn engine_indices(orientation: Orientation, i: usize, j: usize) -> (usize, usize) {
    match orientation {
        Orientation::EachItem => (i, j),
        Orientation::EachSlot => (j, i),
    }
}

/// Convert caller-term constraints into the engine's terms. `items` / `slots`
/// are the caller's counts. A config whose shape does not match them is an
/// error: a constraint applied to the wrong indices is a wrong plan.
pub fn orient_constraints(
    constraints: Vec<TypedConstraint>,
    orientation: Orientation,
    items: usize,
    slots: usize,
) -> Result<Vec<TypedConstraint>, OrtoolsCoreError> {
    if orientation == Orientation::EachItem {
        return Ok(constraints);
    }
    let invalid = |msg: String| OrtoolsCoreError::InvalidConstraint(msg);
    constraints
        .into_iter()
        .map(|c| {
            Ok(match c {
                // At most N slots per caller item = at most N engine items per
                // engine slot: the same rule, read from the other side.
                TypedConstraint::Hard(HardConstraint::Capacity { limit }) => {
                    TypedConstraint::Hard(HardConstraint::Capacity { limit })
                }
                TypedConstraint::Hard(HardConstraint::GroupBalance { .. }) => {
                    return Err(invalid(
                        "group_balance is not supported with assignment each: slot \
                         (every slot holds exactly one item)"
                            .into(),
                    ))
                }
                TypedConstraint::Hard(HardConstraint::NoOverlap { overlap_pairs }) => {
                    if overlap_pairs.len() != items {
                        return Err(invalid(format!(
                            "no_overlap: overlap_pairs has {} rows, expected one per item ({items})",
                            overlap_pairs.len()
                        )));
                    }
                    if overlap_pairs.iter().flatten().any(|&(a, b)| a >= slots || b >= slots) {
                        return Err(invalid("no_overlap: a slot index is out of range".into()));
                    }
                    TypedConstraint::Hard(HardConstraint::SlotConflicts {
                        conflicts: overlap_pairs,
                    })
                }
                TypedConstraint::Hard(HardConstraint::SlotConflicts { .. }) => {
                    return Err(invalid("slot_conflicts is engine-internal".into()))
                }
                TypedConstraint::Hard(HardConstraint::SkillMatch { feasible }) => {
                    if feasible.len() != items || feasible.iter().any(|r| r.len() != slots) {
                        return Err(invalid(format!(
                            "skill_match: feasible must be {items} rows of {slots}"
                        )));
                    }
                    TypedConstraint::Hard(HardConstraint::SkillMatch {
                        feasible: (0..slots)
                            .map(|j| (0..items).map(|i| feasible[i][j]).collect())
                            .collect(),
                    })
                }
                TypedConstraint::Soft(SoftConstraint::MinimizeField { costs, weight }) => {
                    if costs.len() != items {
                        return Err(invalid(format!(
                            "minimize_field: costs has {} entries, expected {items}",
                            costs.len()
                        )));
                    }
                    // Σ costs[item] over assignments = Σ over engine items (caller
                    // slots) of the cost of the engine slot (caller item) taken.
                    TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                        item_costs: vec![1.0; slots],
                        slot_costs: costs,
                        weight,
                    })
                }
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs,
                    slot_costs,
                    weight,
                }) => {
                    if item_costs.len() != items || slot_costs.len() != slots {
                        return Err(invalid(format!(
                            "minimize_cost: expected {items} item_costs and {slots} slot_costs"
                        )));
                    }
                    TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                        item_costs: slot_costs,
                        slot_costs: item_costs,
                        weight,
                    })
                }
                // Under each: slot, `current` is already per slot (engine item),
                // holding the caller item (engine slot).
                TypedConstraint::Soft(SoftConstraint::PinCurrent { current, weight }) => {
                    if current.len() != slots || current.iter().flatten().any(|&i| i >= items) {
                        return Err(invalid(format!(
                            "pin_current: with each: slot, current must have one entry per slot ({slots})"
                        )));
                    }
                    TypedConstraint::Soft(SoftConstraint::PinCurrent { current, weight })
                }
            })
        })
        .collect()
}

/// The caller's `x_i_j` value (0 / 1) under an engine assignment.
pub fn caller_value(orientation: Orientation, assignment: &Assignment, i: usize, j: usize) -> u8 {
    let (item, slot) = engine_indices(orientation, i, j);
    u8::from(assignment.get(item) == Some(&slot))
}

/// Build the engine's problem from a problem's rows: its variables
/// (`name`, `pinned`) and typed constraints (`type`, `config`). An `assignment`
/// row sets the orientation; any other row whose config does not parse is an
/// error — local search reads only these, so a skipped one is a rule the plan
/// silently ignores. Returns the problem and its orientation.
pub fn build_assignment_problem(
    variables: &[(String, bool)],
    typed: &[(String, serde_json::Value)],
) -> Result<(AssignmentProblem, Orientation), OrtoolsCoreError> {
    let mut orientation = Orientation::EachItem;
    let mut caller_constraints = Vec::new();
    for (ctype, config) in typed {
        if ctype == "assignment" {
            orientation = parse_orientation(config)?;
            continue;
        }
        let parsed = super::parse_typed_constraint(ctype, config).ok_or_else(|| {
            OrtoolsCoreError::InvalidConstraint(format!(
                "typed constraint {ctype}: config {config} does not parse"
            ))
        })?;
        caller_constraints.push(parsed);
    }

    // The caller's grid shape, from its x_i_j variables.
    let indices: Vec<(usize, usize, bool)> = variables
        .iter()
        .filter_map(|(name, pinned)| super::parse_var_indices(name).map(|(i, j)| (i, j, *pinned)))
        .collect();
    let items = indices.iter().map(|&(i, _, _)| i + 1).max().unwrap_or(0);
    let slots = indices.iter().map(|&(_, j, _)| j + 1).max().unwrap_or(0);
    if items == 0 || slots == 0 {
        return Err(OrtoolsCoreError::InvalidParameter(
            "No assignment variables (x_i_j pattern) found".to_string(),
        ));
    }

    let constraints = orient_constraints(caller_constraints, orientation, items, slots)?;
    let (item_count, slot_count) = match orientation {
        Orientation::EachItem => (items, slots),
        Orientation::EachSlot => (slots, items),
    };
    // A pinned variable pins the engine item it belongs to.
    let mut pinned = vec![false; item_count];
    for &(i, j, is_pinned) in &indices {
        if is_pinned {
            pinned[engine_indices(orientation, i, j).0] = true;
        }
    }
    let problem = AssignmentProblem {
        item_count,
        slot_count,
        constraints,
        pinned,
        item_data: (0..item_count)
            .map(|_| ItemData {
                group: None,
                fields: Default::default(),
            })
            .collect(),
        slot_data: (0..slot_count)
            .map(|_| SlotData {
                fields: Default::default(),
            })
            .collect(),
    };
    Ok((problem, orientation))
}

/// The solution JSON, matching `solve_sync`, with every caller variable's value.
pub fn format_oriented_result(
    result: &super::LocalSearchResult,
    var_names: &[String],
    orientation: Orientation,
) -> serde_json::Value {
    let mut values = serde_json::Map::new();
    for name in var_names {
        if let Some((i, j)) = super::parse_var_indices(name) {
            let v = caller_value(orientation, &result.assignment, i, j);
            values.insert(name.clone(), serde_json::Value::from(v));
        }
    }
    serde_json::json!({
        "status": if result.score.is_feasible() { "FEASIBLE" } else { "INFEASIBLE" },
        "method": result.algorithm,
        "objective": result.score.soft.abs(),
        "hard_score": result.score.hard,
        "soft_score": result.score.soft,
        "values": values,
        "iterations": result.iterations,
        "time_ms": result.time_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metaheuristic::{solve_local, Algorithm};
    use std::collections::HashMap;
    use std::time::Duration;

    fn vars(items: usize, slots: usize, pinned: &[(usize, usize)]) -> Vec<(String, bool)> {
        let mut v = Vec::new();
        for i in 0..items {
            for j in 0..slots {
                v.push((format!("x_{i}_{j}"), pinned.contains(&(i, j))));
            }
        }
        v
    }

    #[test]
    fn an_unparseable_config_is_an_error() {
        // What Eidos sent before it sent matrices: a field name.
        let typed = vec![(
            "no_overlap".to_string(),
            serde_json::json!({"time_field": "starts_at"}),
        )];
        let err = build_assignment_problem(&vars(2, 3, &[]), &typed).unwrap_err();
        assert!(err.to_string().contains("no_overlap"), "{err}");
    }

    #[test]
    fn each_slot_builds_the_transposed_problem() {
        let typed = vec![
            (
                "assignment".to_string(),
                serde_json::json!({"each": "slot"}),
            ),
            ("capacity".to_string(), serde_json::json!({"limit": 2})),
        ];
        let (p, o) = build_assignment_problem(&vars(2, 3, &[(1, 2)]), &typed).unwrap();
        assert_eq!(o, Orientation::EachSlot);
        assert_eq!((p.item_count, p.slot_count), (3, 2));
        // x_1_2 pinned → engine item 2 (caller slot 2) pinned.
        assert_eq!(p.pinned, vec![false, false, true]);
        assert_eq!(p.constraints.len(), 1);
    }

    #[test]
    fn without_an_assignment_row_the_problem_is_as_before() {
        let typed = vec![("capacity".to_string(), serde_json::json!({"limit": 1}))];
        let (p, o) = build_assignment_problem(&vars(2, 3, &[(1, 0)]), &typed).unwrap();
        assert_eq!(o, Orientation::EachItem);
        assert_eq!((p.item_count, p.slot_count), (2, 3));
        assert_eq!(p.pinned, vec![false, true]);
    }

    #[test]
    fn the_result_reports_every_caller_variable() {
        let typed = vec![(
            "assignment".to_string(),
            serde_json::json!({"each": "slot"}),
        )];
        let v = vars(2, 3, &[]);
        let (p, o) = build_assignment_problem(&v, &typed).unwrap();
        let r = solve_local(&p, &Algorithm::HillClimbing, Duration::from_millis(50), 1);
        let names: Vec<String> = v.iter().map(|(n, _)| n.clone()).collect();
        let out = format_oriented_result(&r, &names, o);
        let values = out["values"].as_object().unwrap();
        assert_eq!(values.len(), 6);
        // every caller slot has exactly one item
        for j in 0..3 {
            let held: i64 = (0..2)
                .map(|i| values[&format!("x_{i}_{j}")].as_i64().unwrap())
                .sum();
            assert_eq!(held, 1, "slot {j}: {out}");
        }
        assert_eq!(out["status"], "FEASIBLE");
    }

    #[test]
    fn parses_each_item_and_each_slot_and_refuses_anything_else() {
        let p = |s: &str| parse_orientation(&serde_json::from_str(s).unwrap());
        assert_eq!(p(r#"{"each": "item"}"#).unwrap(), Orientation::EachItem);
        assert_eq!(p(r#"{"each": "slot"}"#).unwrap(), Orientation::EachSlot);
        assert!(p(r#"{"each": "shift"}"#).is_err());
        assert!(p(r#"{}"#).is_err());
    }

    #[test]
    fn each_slot_swaps_the_indices() {
        assert_eq!(engine_indices(Orientation::EachItem, 1, 2), (1, 2));
        assert_eq!(engine_indices(Orientation::EachSlot, 1, 2), (2, 1));
    }

    #[test]
    fn each_slot_transposes_feasibility_and_swaps_costs() {
        let out = orient_constraints(
            vec![
                TypedConstraint::Hard(HardConstraint::SkillMatch {
                    feasible: vec![vec![true, true, false], vec![false, true, true]],
                }),
                TypedConstraint::Soft(SoftConstraint::MinimizeField {
                    costs: vec![20.0, 30.0],
                    weight: 1.0,
                }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: vec![20.0, 30.0],
                    slot_costs: vec![8.0, 8.0, 1.0],
                    weight: 2.0,
                }),
            ],
            Orientation::EachSlot,
            2,
            3,
        )
        .unwrap();
        match &out[0] {
            TypedConstraint::Hard(HardConstraint::SkillMatch { feasible }) => assert_eq!(
                feasible,
                &vec![vec![true, false], vec![true, true], vec![false, true]]
            ),
            other => panic!("{other:?}"),
        }
        match &out[1] {
            TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                item_costs,
                slot_costs,
                weight,
            }) => {
                assert_eq!(item_costs, &vec![1.0, 1.0, 1.0]);
                assert_eq!(slot_costs, &vec![20.0, 30.0]);
                assert_eq!(*weight, 1.0);
            }
            other => panic!("{other:?}"),
        }
        match &out[2] {
            TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                item_costs,
                slot_costs,
                ..
            }) => {
                assert_eq!(item_costs, &vec![8.0, 8.0, 1.0]);
                assert_eq!(slot_costs, &vec![20.0, 30.0]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn each_slot_turns_no_overlap_into_slot_conflicts_and_refuses_group_balance() {
        let out = orient_constraints(
            vec![TypedConstraint::Hard(HardConstraint::NoOverlap {
                overlap_pairs: vec![vec![(0, 1)], vec![(0, 1)]],
            })],
            Orientation::EachSlot,
            2,
            3,
        )
        .unwrap();
        assert!(matches!(
            &out[0],
            TypedConstraint::Hard(HardConstraint::SlotConflicts { conflicts })
                if conflicts == &vec![vec![(0, 1)], vec![(0, 1)]]
        ));
        assert!(orient_constraints(
            vec![TypedConstraint::Hard(HardConstraint::GroupBalance {
                group_field: "team".into(),
                count_per_target: 1,
            })],
            Orientation::EachSlot,
            2,
            3,
        )
        .is_err());
    }

    #[test]
    fn a_malformed_matrix_is_an_error_not_a_silent_skip() {
        let short = orient_constraints(
            vec![TypedConstraint::Hard(HardConstraint::SkillMatch {
                feasible: vec![vec![true, true, true]],
            })],
            Orientation::EachSlot,
            2,
            3,
        );
        assert!(short.is_err());
    }

    #[test]
    fn each_item_leaves_constraints_alone() {
        let c = vec![TypedConstraint::Hard(HardConstraint::Capacity { limit: 2 })];
        let out = orient_constraints(c, Orientation::EachItem, 2, 3).unwrap();
        assert!(matches!(
            &out[0],
            TypedConstraint::Hard(HardConstraint::Capacity { limit: 2 })
        ));
    }

    #[test]
    fn caller_values_map_back_through_the_transpose() {
        // engine items = caller slots: slot 0 → item 0, slot 1 → item 1, slot 2 → item 0
        let a: Assignment = vec![0, 1, 0];
        assert_eq!(caller_value(Orientation::EachSlot, &a, 0, 0), 1);
        assert_eq!(caller_value(Orientation::EachSlot, &a, 1, 0), 0);
        assert_eq!(caller_value(Orientation::EachSlot, &a, 1, 1), 1);
        assert_eq!(caller_value(Orientation::EachSlot, &a, 0, 2), 1);
    }

    /// The rostering problem Eidos generates: employees e0 {cook,bar} at 20/h and
    /// e1 {bar} at 30/h; shifts s0 09-17 cook, s1 13-21 bar, s2 22-23 bar. Only
    /// e0 can cook, s0 and s1 overlap, so s0 → e0 and s1 → e1. Solved as stated
    /// (each item one slot) this gave both employees one shift and left the rest
    /// empty.
    #[test]
    fn each_slot_solves_the_rostering_problem() {
        let caller = vec![
            TypedConstraint::Hard(HardConstraint::Capacity { limit: 2 }),
            TypedConstraint::Hard(HardConstraint::SkillMatch {
                feasible: vec![vec![true, true, true], vec![false, true, true]],
            }),
            TypedConstraint::Hard(HardConstraint::NoOverlap {
                overlap_pairs: vec![vec![(0, 1)], vec![(0, 1)]],
            }),
            TypedConstraint::Soft(SoftConstraint::MinimizeField {
                costs: vec![20.0, 30.0],
                weight: 1.0,
            }),
        ];
        let (items, slots) = (2, 3);
        let constraints = orient_constraints(caller, Orientation::EachSlot, items, slots).unwrap();
        let problem = AssignmentProblem {
            item_count: slots,
            slot_count: items,
            constraints,
            pinned: vec![false; slots],
            item_data: (0..slots)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..items)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        };
        for algorithm in [
            Algorithm::HillClimbing,
            Algorithm::TabuSearch { tabu_tenure: 7 },
            Algorithm::LateAcceptance { late_size: 100 },
        ] {
            let r = solve_local(&problem, &algorithm, Duration::from_millis(200), 42);
            assert!(r.score.is_feasible(), "{algorithm:?}: {:?}", r.score);
            let held = |shift: usize| {
                (0..items)
                    .find(|&e| caller_value(Orientation::EachSlot, &r.assignment, e, shift) == 1)
            };
            assert_eq!(held(0), Some(0), "{algorithm:?}: only e0 can cook");
            assert_eq!(held(1), Some(1), "{algorithm:?}: s1 overlaps e0's s0");
            assert!(held(2).is_some(), "{algorithm:?}: every shift is staffed");
        }
    }
}
