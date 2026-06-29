//! Pure parsing and formatting functions for metaheuristic problems.
//!
//! Extracted from the database loader to enable reuse without PostgreSQL dependency.

use super::algorithms::{Algorithm, LocalSearchResult};
use super::problem::*;
use crate::error::OrtoolsCoreError;
use serde_json::Value as JsonValue;

/// Parse variable name `x_i_j` into (item_index, slot_index).
pub fn parse_var_indices(name: &str) -> Option<(usize, usize)> {
    let parts: Vec<&str> = name.split('_').collect();
    if parts.len() == 3 && parts[0] == "x" {
        let i = parts[1].parse::<usize>().ok()?;
        let j = parts[2].parse::<usize>().ok()?;
        Some((i, j))
    } else {
        None
    }
}

/// Parse a typed constraint from its type name and JSON config.
pub fn parse_typed_constraint(
    constraint_type: &str,
    config: &JsonValue,
) -> Option<TypedConstraint> {
    match constraint_type {
        "capacity" => {
            let limit = config.get("limit")?.as_u64()? as usize;
            Some(TypedConstraint::Hard(HardConstraint::Capacity { limit }))
        }
        "group_balance" => {
            let group_field = config.get("group_field")?.as_str()?.to_string();
            let count_per_target = config.get("count_per_target")?.as_u64()? as usize;
            Some(TypedConstraint::Hard(HardConstraint::GroupBalance {
                group_field,
                count_per_target,
            }))
        }
        "no_overlap" => {
            let pairs_json = config.get("overlap_pairs")?;
            let mut overlap_pairs = Vec::new();
            for item_pairs in pairs_json.as_array()? {
                let mut pairs = Vec::new();
                for pair in item_pairs.as_array()? {
                    let arr = pair.as_array()?;
                    let a = arr.first()?.as_u64()? as usize;
                    let b = arr.get(1)?.as_u64()? as usize;
                    pairs.push((a, b));
                }
                overlap_pairs.push(pairs);
            }
            Some(TypedConstraint::Hard(HardConstraint::NoOverlap {
                overlap_pairs,
            }))
        }
        "skill_match" => {
            let feasible_json = config.get("feasible")?;
            let mut feasible = Vec::new();
            for item_slots in feasible_json.as_array()? {
                let row: Vec<bool> = item_slots
                    .as_array()?
                    .iter()
                    .filter_map(|v| v.as_bool())
                    .collect();
                feasible.push(row);
            }
            Some(TypedConstraint::Hard(HardConstraint::SkillMatch {
                feasible,
            }))
        }
        "minimize_field" => {
            let costs: Vec<f64> = config
                .get("costs")?
                .as_array()?
                .iter()
                .filter_map(|v| v.as_f64())
                .collect();
            let weight = config.get("weight")?.as_f64()?;
            Some(TypedConstraint::Soft(SoftConstraint::MinimizeField {
                costs,
                weight,
            }))
        }
        "minimize_cost" => {
            let item_costs: Vec<f64> = config
                .get("item_costs")?
                .as_array()?
                .iter()
                .filter_map(|v| v.as_f64())
                .collect();
            let slot_costs: Vec<f64> = config
                .get("slot_costs")?
                .as_array()?
                .iter()
                .filter_map(|v| v.as_f64())
                .collect();
            let weight = config.get("weight")?.as_f64()?;
            Some(TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                item_costs,
                slot_costs,
                weight,
            }))
        }
        "pin_current" => {
            let weight = config.get("weight")?.as_f64()?;
            let current: Vec<Option<usize>> = config
                .get("current")?
                .as_array()?
                .iter()
                .map(|v| v.as_u64().map(|n| n as usize))
                .collect();
            Some(TypedConstraint::Soft(SoftConstraint::PinCurrent {
                current,
                weight,
            }))
        }
        _ => None,
    }
}

/// Format a LocalSearchResult as JSON matching the solve_sync format.
pub fn format_result(
    result: &LocalSearchResult,
    var_names: &[String],
    item_count: usize,
    slot_count: usize,
) -> Result<serde_json::Value, OrtoolsCoreError> {
    let status = if result.score.is_feasible() {
        "FEASIBLE"
    } else {
        "INFEASIBLE"
    };

    let mut values = serde_json::Map::new();
    for i in 0..item_count {
        for j in 0..slot_count {
            let var_name = format!("x_{}_{}", i, j);
            if var_names.contains(&var_name) {
                let assigned = if result.assignment[i] == j { 1 } else { 0 };
                values.insert(var_name, serde_json::Value::Number(assigned.into()));
            }
        }
    }

    Ok(serde_json::json!({
        "status": status,
        "method": result.algorithm,
        "objective": result.score.soft.abs(),
        "hard_score": result.score.hard,
        "soft_score": result.score.soft,
        "values": values,
        "iterations": result.iterations,
        "time_ms": result.time_ms,
    }))
}

/// Validate constraint_type string.
pub fn is_valid_constraint_type(ctype: &str) -> bool {
    matches!(
        ctype,
        "capacity"
            | "group_balance"
            | "no_overlap"
            | "skill_match"
            | "minimize_field"
            | "minimize_cost"
            | "pin_current"
    )
}

/// Parse algorithm name string into Algorithm enum.
pub fn parse_algorithm(name: &str) -> Result<Algorithm, OrtoolsCoreError> {
    match name {
        "hill_climbing" => Ok(Algorithm::HillClimbing),
        "tabu_search" => Ok(Algorithm::TabuSearch { tabu_tenure: 7 }),
        "simulated_annealing" => Ok(Algorithm::SimulatedAnnealing {
            initial_temp: 1000.0,
            cooling_rate: 0.9999,
        }),
        "late_acceptance" => Ok(Algorithm::LateAcceptance { late_size: 400 }),
        other => Err(OrtoolsCoreError::InvalidParameter(format!(
            "Unknown algorithm: '{}'. Valid: hill_climbing, tabu_search, simulated_annealing, late_acceptance",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metaheuristic::score::HardSoftScore;

    #[test]
    fn test_parse_var_indices() {
        assert_eq!(parse_var_indices("x_0_0"), Some((0, 0)));
        assert_eq!(parse_var_indices("x_3_7"), Some((3, 7)));
        assert_eq!(parse_var_indices("x_12_0"), Some((12, 0)));
        assert_eq!(parse_var_indices("y_0_0"), None);
        assert_eq!(parse_var_indices("x_0"), None);
        assert_eq!(parse_var_indices("x_a_b"), None);
    }

    #[test]
    fn test_parse_typed_constraint_capacity() {
        let config = serde_json::json!({"limit": 2});
        let tc = parse_typed_constraint("capacity", &config);
        assert!(tc.is_some());
        match tc.unwrap() {
            TypedConstraint::Hard(HardConstraint::Capacity { limit }) => {
                assert_eq!(limit, 2);
            }
            _ => panic!("expected Capacity"),
        }
    }

    #[test]
    fn test_parse_typed_constraint_skill_match() {
        let config = serde_json::json!({
            "feasible": [[true, false], [false, true]]
        });
        let tc = parse_typed_constraint("skill_match", &config);
        assert!(tc.is_some());
        match tc.unwrap() {
            TypedConstraint::Hard(HardConstraint::SkillMatch { feasible }) => {
                assert_eq!(feasible.len(), 2);
                assert_eq!(feasible[0], vec![true, false]);
                assert_eq!(feasible[1], vec![false, true]);
            }
            _ => panic!("expected SkillMatch"),
        }
    }

    #[test]
    fn test_parse_typed_constraint_minimize_cost() {
        let config = serde_json::json!({
            "item_costs": [1.0, 2.0],
            "slot_costs": [10.0, 20.0],
            "weight": 1.5
        });
        let tc = parse_typed_constraint("minimize_cost", &config);
        assert!(tc.is_some());
        match tc.unwrap() {
            TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                item_costs,
                slot_costs,
                weight,
            }) => {
                assert_eq!(item_costs, vec![1.0, 2.0]);
                assert_eq!(slot_costs, vec![10.0, 20.0]);
                assert!((weight - 1.5).abs() < f64::EPSILON);
            }
            _ => panic!("expected MinimizeCost"),
        }
    }

    #[test]
    fn test_parse_typed_constraint_unknown() {
        let config = serde_json::json!({"foo": "bar"});
        assert!(parse_typed_constraint("unknown_type", &config).is_none());
    }

    #[test]
    fn test_is_valid_constraint_type() {
        assert!(is_valid_constraint_type("capacity"));
        assert!(is_valid_constraint_type("group_balance"));
        assert!(is_valid_constraint_type("no_overlap"));
        assert!(is_valid_constraint_type("skill_match"));
        assert!(is_valid_constraint_type("minimize_field"));
        assert!(is_valid_constraint_type("minimize_cost"));
        assert!(is_valid_constraint_type("pin_current"));
        assert!(!is_valid_constraint_type("unknown"));
        assert!(!is_valid_constraint_type(""));
    }

    #[test]
    fn test_parse_algorithm() {
        assert!(matches!(
            parse_algorithm("hill_climbing").unwrap(),
            Algorithm::HillClimbing
        ));
        assert!(matches!(
            parse_algorithm("tabu_search").unwrap(),
            Algorithm::TabuSearch { .. }
        ));
        assert!(matches!(
            parse_algorithm("simulated_annealing").unwrap(),
            Algorithm::SimulatedAnnealing { .. }
        ));
        assert!(matches!(
            parse_algorithm("late_acceptance").unwrap(),
            Algorithm::LateAcceptance { .. }
        ));
        assert!(parse_algorithm("bogus").is_err());
    }

    #[test]
    fn test_format_result() {
        let result = LocalSearchResult {
            assignment: vec![1, 0],
            score: HardSoftScore::new(0, -15),
            iterations: 100,
            time_ms: 50,
            algorithm: "late_acceptance".to_string(),
        };
        let var_names = vec![
            "x_0_0".to_string(),
            "x_0_1".to_string(),
            "x_1_0".to_string(),
            "x_1_1".to_string(),
        ];
        let json = format_result(&result, &var_names, 2, 2).unwrap();
        assert_eq!(json["status"], "FEASIBLE");
        assert_eq!(json["method"], "late_acceptance");
        assert_eq!(json["hard_score"], 0);
        assert_eq!(json["soft_score"], -15);
        assert_eq!(json["values"]["x_0_0"], 0);
        assert_eq!(json["values"]["x_0_1"], 1);
        assert_eq!(json["values"]["x_1_0"], 1);
        assert_eq!(json["values"]["x_1_1"], 0);
    }
}
