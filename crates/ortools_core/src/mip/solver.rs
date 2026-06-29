//! MIP solver using good_lp + HiGHS.

use crate::error::OrtoolsCoreError;
use crate::mip::data::ProblemData;
use crate::mip::parser::{parse_constraint, parse_expression};
use good_lp::solvers::highs::highs;
use good_lp::{variable, Expression, ProblemVariables, Solution, SolverModel};
use std::collections::HashMap;

/// Solve a MIP problem from its data structures.
///
/// When `greedy` is true, sets mip_rel_gap=1.0 so HiGHS returns the first feasible solution.
/// Returns the solution as JSON. The caller is responsible for storing the result.
pub fn solve_mip(
    problem_data: &ProblemData,
    _problem_name: &str,
    greedy: bool,
) -> Result<serde_json::Value, OrtoolsCoreError> {
    let mut prob_vars = ProblemVariables::new();
    let mut var_map: HashMap<String, good_lp::Variable> = HashMap::new();

    // Create variables
    for vd in &problem_data.variables {
        let var = match vd.var_type.as_str() {
            "bool" => prob_vars.add(variable().binary()),
            "int" => prob_vars.add(
                variable()
                    .integer()
                    .min(vd.domain_min as f64)
                    .max(vd.domain_max as f64),
            ),
            _ => prob_vars.add(
                variable()
                    .min(vd.domain_min as f64)
                    .max(vd.domain_max as f64),
            ),
        };
        var_map.insert(vd.name.clone(), var);
    }

    // Build objective
    let objective_expr = match &problem_data.objective_expr {
        Some(expr_str) => parse_expression(expr_str, &var_map)?,
        None => Expression::from(0.0),
    };

    // Create solver model with direction
    let mut model = match problem_data.objective_type.as_deref() {
        Some("maximize") => prob_vars.maximise(&objective_expr).using(highs),
        _ => prob_vars.minimise(&objective_expr).using(highs),
    };

    // Configure HiGHS
    model.set_verbose(false);
    if greedy {
        // Accept first feasible solution without optimizing
        model = model.set_option("mip_rel_gap", 1.0);
    }

    // Add constraints
    for c in &problem_data.constraints {
        let constraint = parse_constraint(&c.expression, &var_map)?;
        model = model.with(constraint);
    }

    // Solve
    let start = std::time::Instant::now();
    let solution = model.solve();
    let elapsed_ms = start.elapsed().as_millis() as i64;

    match solution {
        Ok(sol) => {
            // Extract variable values
            let mut values = serde_json::Map::new();
            for (name, var) in &var_map {
                let val = sol.value(*var);
                values.insert(name.clone(), serde_json::json!(val.round() as i64));
            }

            // Evaluate objective
            let obj_value = sol.eval(&objective_expr);

            let method = if greedy { "greedy" } else { "optimal" };
            let result = serde_json::json!({
                "status": "OPTIMAL",
                "method": method,
                "objective": obj_value,
                "values": values,
                "time_ms": elapsed_ms,
            });

            Ok(result)
        }
        Err(e) => {
            let status = match e {
                good_lp::ResolutionError::Infeasible => "INFEASIBLE",
                good_lp::ResolutionError::Unbounded => "UNBOUNDED",
                _ => "ERROR",
            };

            let _result = serde_json::json!({
                "status": status,
                "objective": null,
                "values": {},
                "time_ms": elapsed_ms,
                "error": e.to_string(),
            });

            Err(OrtoolsCoreError::SolverError(format!("{}: {}", status, e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mip::parser::{parse_constraint, parse_expression, parse_term};
    use good_lp::{variable, Expression, ProblemVariables, Solution, SolverModel};

    fn make_vars() -> (ProblemVariables, HashMap<String, good_lp::Variable>) {
        let mut pv = ProblemVariables::new();
        let mut map = HashMap::new();
        map.insert(
            "x".to_string(),
            pv.add(variable().integer().min(0).max(100)),
        );
        map.insert(
            "y".to_string(),
            pv.add(variable().integer().min(0).max(100)),
        );
        map.insert("z".to_string(), pv.add(variable().binary()));
        (pv, map)
    }

    #[test]
    fn test_parse_single_variable() {
        let (_pv, vars) = make_vars();
        let result = parse_expression("x", &vars);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_single_number() {
        let (_pv, vars) = make_vars();
        let result = parse_expression("42", &vars);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiplication() {
        let (_pv, vars) = make_vars();
        assert!(parse_expression("2*x", &vars).is_ok());
        assert!(parse_expression("x*3", &vars).is_ok());
    }

    #[test]
    fn test_parse_addition_chain() {
        let (_pv, vars) = make_vars();
        assert!(parse_expression("2*x + 3*y", &vars).is_ok());
        assert!(parse_expression("x + y + z", &vars).is_ok());
    }

    #[test]
    fn test_parse_subtraction() {
        let (_pv, vars) = make_vars();
        assert!(parse_expression("x - y", &vars).is_ok());
        assert!(parse_expression("2*x - 3*y + z", &vars).is_ok());
    }

    #[test]
    fn test_parse_negative_leading() {
        let (_pv, vars) = make_vars();
        assert!(parse_expression("-x + y", &vars).is_ok());
    }

    #[test]
    fn test_parse_constraint_leq() {
        let (_pv, vars) = make_vars();
        assert!(parse_constraint("x + y <= 100", &vars).is_ok());
    }

    #[test]
    fn test_parse_constraint_geq() {
        let (_pv, vars) = make_vars();
        assert!(parse_constraint("x >= 10", &vars).is_ok());
    }

    #[test]
    fn test_parse_constraint_eq() {
        let (_pv, vars) = make_vars();
        assert!(parse_constraint("x + y == 50", &vars).is_ok());
        assert!(parse_constraint("x = 10", &vars).is_ok());
    }

    #[test]
    fn test_parse_constraint_neq_errors() {
        let (_pv, vars) = make_vars();
        let result = parse_constraint("x != y", &vars);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not supported by MIP"));
    }

    #[test]
    fn test_parse_unknown_variable() {
        let (_pv, vars) = make_vars();
        let result = parse_expression("unknown_var", &vars);
        assert!(result.is_err());
    }

    #[test]
    fn test_solve_simple_problem() {
        // Maximize 2*x + 3*y subject to x + y <= 100, x >= 0, y >= 0
        let mut pv = ProblemVariables::new();
        let x = pv.add(variable().integer().min(0).max(100));
        let y = pv.add(variable().integer().min(0).max(100));

        let objective = 2.0 * x + 3.0 * y;
        let mut model = pv.maximise(&objective).using(highs);
        model.set_verbose(false);
        model = model.with((Expression::from(x) + Expression::from(y)).leq(100.0));

        let solution = model.solve().unwrap();
        // Optimal: x=0, y=100, objective=300
        assert!((solution.value(y) - 100.0).abs() < 0.5);
        assert!((solution.eval(&objective) - 300.0).abs() < 0.5);
    }

    #[test]
    fn test_solve_assignment() {
        // Simple 2x2 assignment: minimize cost
        let mut pv = ProblemVariables::new();
        let a1 = pv.add(variable().binary());
        let a2 = pv.add(variable().binary());
        let b1 = pv.add(variable().binary());
        let b2 = pv.add(variable().binary());

        let cost = 10.0 * a1 + 15.0 * a2 + 12.0 * b1 + 8.0 * b2;

        let mut model = pv.minimise(&cost).using(highs);
        model.set_verbose(false);

        model = model.with((Expression::from(a1) + Expression::from(a2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(b1) + Expression::from(b2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a1) + Expression::from(b1)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a2) + Expression::from(b2)).eq(Expression::from(1.0)));

        let solution = model.solve().unwrap();
        assert!((solution.eval(&cost) - 18.0).abs() < 0.5);
    }

    #[test]
    fn test_solve_greedy_finds_feasible() {
        let mut pv = ProblemVariables::new();
        let a1 = pv.add(variable().binary());
        let a2 = pv.add(variable().binary());
        let b1 = pv.add(variable().binary());
        let b2 = pv.add(variable().binary());

        let cost = 10.0 * a1 + 15.0 * a2 + 12.0 * b1 + 8.0 * b2;

        let mut model = pv.minimise(&cost).using(highs);
        model.set_verbose(false);
        model = model.set_option("mip_rel_gap", 1.0);

        model = model.with((Expression::from(a1) + Expression::from(a2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(b1) + Expression::from(b2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a1) + Expression::from(b1)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a2) + Expression::from(b2)).eq(Expression::from(1.0)));

        let solution = model.solve().unwrap();
        let total = solution.eval(&cost);
        assert!(
            total == 18.0 || total == 27.0,
            "cost should be 18 or 27, got {}",
            total
        );
    }

    // =========================================================================
    // Benchmark tests — standard OR problems with known optimal solutions
    // =========================================================================

    #[rustfmt::skip]
    const BENCH_ASSIGN_5X4: [[f64; 4]; 5] = [
        [90.0, 76.0, 75.0, 70.0],
        [35.0, 85.0, 55.0, 65.0],
        [125.0, 95.0, 90.0, 105.0],
        [45.0, 110.0, 95.0, 115.0],
        [60.0, 105.0, 80.0, 75.0],
    ];

    #[test]
    fn test_benchmark_assignment_5x4() {
        let n_workers = 5;
        let n_tasks = 4;
        let mut pv = ProblemVariables::new();

        let mut x = [[None::<good_lp::Variable>; 4]; 5];
        for i in 0..n_workers {
            for j in 0..n_tasks {
                x[i][j] = Some(pv.add(variable().binary()));
            }
        }

        let mut cost = Expression::from(0.0);
        for i in 0..n_workers {
            for j in 0..n_tasks {
                cost += BENCH_ASSIGN_5X4[i][j] * x[i][j].unwrap();
            }
        }

        let mut model = pv.minimise(&cost).using(highs);
        model.set_verbose(false);

        for i in 0..n_workers {
            let row_sum = (0..n_tasks).fold(Expression::from(0.0), |acc, j| {
                acc + Expression::from(x[i][j].unwrap())
            });
            model = model.with(row_sum.leq(1.0));
        }

        for j in 0..n_tasks {
            let col_sum = (0..n_workers).fold(Expression::from(0.0), |acc, i| {
                acc + Expression::from(x[i][j].unwrap())
            });
            model = model.with(col_sum.eq(Expression::from(1.0)));
        }

        let solution = model.solve().unwrap();
        let objective = solution.eval(&cost);
        assert!(
            (objective - 251.0).abs() < 0.5,
            "5x4 assignment optimal should be 251, got {}",
            objective
        );
    }

    #[test]
    fn test_benchmark_nqueens_4() {
        let n = 4usize;
        let mut pv = ProblemVariables::new();

        let mut q = [[None::<good_lp::Variable>; 4]; 4];
        for r in 0..n {
            for c in 0..n {
                q[r][c] = Some(pv.add(variable().binary()));
            }
        }

        let mut total = Expression::from(0.0);
        for r in 0..n {
            for c in 0..n {
                total += Expression::from(q[r][c].unwrap());
            }
        }

        let mut model = pv.maximise(&total).using(highs);
        model.set_verbose(false);

        for r in 0..n {
            let row_sum = (0..n).fold(Expression::from(0.0), |acc, c| {
                acc + Expression::from(q[r][c].unwrap())
            });
            model = model.with(row_sum.eq(Expression::from(1.0)));
        }

        for c in 0..n {
            let col_sum = (0..n).fold(Expression::from(0.0), |acc, r| {
                acc + Expression::from(q[r][c].unwrap())
            });
            model = model.with(col_sum.eq(Expression::from(1.0)));
        }

        for d in -(n as i32 - 1)..=(n as i32 - 1) {
            let cells: Vec<(usize, usize)> = (0..n)
                .filter_map(|r| {
                    let c = r as i32 - d;
                    if c >= 0 && c < n as i32 {
                        Some((r, c as usize))
                    } else {
                        None
                    }
                })
                .collect();
            if cells.len() >= 2 {
                let diag_sum = cells.iter().fold(Expression::from(0.0), |acc, &(r, c)| {
                    acc + Expression::from(q[r][c].unwrap())
                });
                model = model.with(diag_sum.leq(1.0));
            }
        }

        for s in 0..=(2 * (n - 1)) {
            let cells: Vec<(usize, usize)> = (0..n)
                .filter_map(|r| {
                    if s >= r && s - r < n {
                        Some((r, s - r))
                    } else {
                        None
                    }
                })
                .collect();
            if cells.len() >= 2 {
                let diag_sum = cells.iter().fold(Expression::from(0.0), |acc, &(r, c)| {
                    acc + Expression::from(q[r][c].unwrap())
                });
                model = model.with(diag_sum.leq(1.0));
            }
        }

        let solution = model.solve().unwrap();
        let objective = solution.eval(&total);
        assert!(
            (objective - 4.0).abs() < 0.5,
            "N-Queens 4 should place 4 queens, got {}",
            objective
        );

        let mut placement = Vec::new();
        for r in 0..n {
            for c in 0..n {
                if solution.value(q[r][c].unwrap()) > 0.5 {
                    placement.push((r, c));
                }
            }
        }
        let valid_1 = vec![(0, 1), (1, 3), (2, 0), (3, 2)];
        let valid_2 = vec![(0, 2), (1, 0), (2, 3), (3, 1)];
        assert!(
            placement == valid_1 || placement == valid_2,
            "N-Queens 4 placement {:?} is not a known valid solution",
            placement
        );
    }

    // =========================================================================
    // GAP benchmarks
    // =========================================================================

    fn solve_gap(
        n_agents: usize,
        n_jobs: usize,
        profit: &[&[f64]],
        resource: &[&[f64]],
        capacity: &[f64],
    ) -> f64 {
        let mut pv = ProblemVariables::new();
        let mut x = vec![vec![None::<good_lp::Variable>; n_jobs]; n_agents];
        for i in 0..n_agents {
            for j in 0..n_jobs {
                x[i][j] = Some(pv.add(variable().binary()));
            }
        }

        let mut obj = Expression::from(0.0);
        for i in 0..n_agents {
            for j in 0..n_jobs {
                obj += profit[i][j] * x[i][j].unwrap();
            }
        }

        let mut model = pv.maximise(&obj).using(highs);
        model.set_verbose(false);

        for j in 0..n_jobs {
            let col_sum = (0..n_agents).fold(Expression::from(0.0), |acc, i| {
                acc + Expression::from(x[i][j].unwrap())
            });
            model = model.with(col_sum.eq(Expression::from(1.0)));
        }

        for i in 0..n_agents {
            let resource_sum = (0..n_jobs).fold(Expression::from(0.0), |acc, j| {
                acc + resource[i][j] * x[i][j].unwrap()
            });
            model = model.with(resource_sum.leq(capacity[i]));
        }

        let solution = model.solve().unwrap();
        solution.eval(&obj)
    }

    #[rustfmt::skip]
    const GAP824_PROFIT: [[f64; 24]; 8] = [
        [25.0,23.0,20.0,16.0,19.0,22.0,20.0,16.0,15.0,22.0,15.0,21.0,20.0,23.0,20.0,22.0,19.0,25.0,25.0,24.0,21.0,17.0,23.0,17.0],
        [16.0,19.0,22.0,22.0,19.0,23.0,17.0,24.0,15.0,24.0,18.0,19.0,20.0,24.0,25.0,25.0,19.0,24.0,18.0,21.0,16.0,25.0,15.0,20.0],
        [20.0,18.0,23.0,23.0,23.0,17.0,19.0,16.0,24.0,24.0,17.0,23.0,19.0,22.0,23.0,25.0,23.0,18.0,19.0,24.0,20.0,17.0,23.0,23.0],
        [16.0,16.0,15.0,23.0,15.0,15.0,25.0,22.0,17.0,20.0,19.0,16.0,17.0,17.0,20.0,17.0,17.0,18.0,16.0,18.0,15.0,25.0,22.0,17.0],
        [17.0,23.0,21.0,20.0,24.0,22.0,25.0,17.0,22.0,20.0,16.0,22.0,21.0,23.0,24.0,15.0,22.0,25.0,18.0,19.0,19.0,17.0,22.0,23.0],
        [24.0,21.0,23.0,17.0,21.0,19.0,19.0,17.0,18.0,24.0,15.0,15.0,17.0,18.0,15.0,24.0,19.0,21.0,23.0,24.0,17.0,20.0,16.0,21.0],
        [18.0,21.0,22.0,23.0,22.0,15.0,18.0,15.0,21.0,22.0,15.0,23.0,21.0,25.0,25.0,23.0,20.0,16.0,25.0,17.0,15.0,15.0,18.0,16.0],
        [19.0,24.0,18.0,17.0,21.0,18.0,24.0,25.0,18.0,23.0,21.0,15.0,24.0,23.0,18.0,18.0,23.0,23.0,16.0,20.0,20.0,19.0,25.0,21.0],
    ];

    #[rustfmt::skip]
    const GAP824_RESOURCE: [[f64; 24]; 8] = [
        [ 8.0,18.0,22.0, 5.0,11.0,11.0,22.0,11.0,17.0,22.0,11.0,20.0,13.0,13.0, 7.0,22.0,15.0,22.0,24.0, 8.0, 8.0,24.0,18.0, 8.0],
        [24.0,14.0,11.0,15.0,24.0, 8.0,10.0,15.0,19.0,25.0, 6.0,13.0,10.0,25.0,19.0,24.0,13.0,12.0, 5.0,18.0,10.0,24.0, 8.0, 5.0],
        [22.0,22.0,21.0,22.0,13.0,16.0,21.0, 5.0,25.0,13.0,12.0, 9.0,24.0, 6.0,22.0,24.0,11.0,21.0,11.0,14.0,12.0,10.0,20.0, 6.0],
        [13.0, 8.0,19.0,12.0,19.0,18.0,10.0,21.0, 5.0, 9.0,11.0, 9.0,22.0, 8.0,12.0,13.0, 9.0,25.0,19.0,24.0,22.0, 6.0,19.0,14.0],
        [25.0,16.0,13.0, 5.0,11.0, 8.0, 7.0, 8.0,25.0,20.0,24.0,20.0,11.0, 6.0,10.0,10.0, 6.0,22.0,10.0,10.0,13.0,21.0, 5.0,19.0],
        [19.0,19.0, 5.0,11.0,22.0,24.0,18.0,11.0, 6.0,13.0,24.0,24.0,22.0, 6.0,22.0, 5.0,14.0, 6.0,16.0,11.0, 6.0, 8.0,18.0,10.0],
        [24.0,10.0, 9.0,10.0, 6.0,15.0, 7.0,13.0,20.0, 8.0, 7.0, 9.0,24.0, 9.0,21.0, 9.0,11.0,19.0,10.0, 5.0,23.0,20.0, 5.0,21.0],
        [ 6.0, 9.0, 9.0, 5.0,12.0,10.0,16.0,15.0,19.0,18.0,20.0,18.0,16.0,21.0,11.0,12.0,22.0,16.0,21.0,25.0, 7.0,14.0,16.0,10.0],
    ];

    const GAP824_CAPACITY: [f64; 8] = [36.0, 35.0, 38.0, 34.0, 32.0, 34.0, 31.0, 34.0];

    #[test]
    fn test_benchmark_gap_c824() {
        let profit: Vec<&[f64]> = GAP824_PROFIT.iter().map(|r| r.as_slice()).collect();
        let resource: Vec<&[f64]> = GAP824_RESOURCE.iter().map(|r| r.as_slice()).collect();
        let objective = solve_gap(8, 24, &profit, &resource, &GAP824_CAPACITY);
        assert!(
            (objective - 563.0).abs() < 0.5,
            "GAP c824-1 optimal should be 563, got {}",
            objective
        );
    }

    #[rustfmt::skip]
    const GAP1030_PROFIT: [[f64; 30]; 10] = [
        [25.0,16.0,17.0,24.0,15.0,24.0,24.0,17.0,23.0,16.0,21.0,18.0,18.0,18.0,18.0,20.0,16.0,15.0,25.0,15.0,16.0,23.0,22.0,18.0,23.0,18.0,19.0,20.0,21.0,19.0],
        [16.0,17.0,19.0,18.0,17.0,23.0,15.0,25.0,25.0,16.0,16.0,23.0,21.0,23.0,25.0,19.0,16.0,18.0,16.0,21.0,24.0,15.0,21.0,19.0,20.0,16.0,18.0,25.0,20.0,22.0],
        [19.0,19.0,15.0,25.0,24.0,25.0,16.0,23.0,15.0,20.0,16.0,19.0,18.0,20.0,15.0,17.0,16.0,17.0,21.0,20.0,18.0,19.0,22.0,20.0,19.0,21.0,18.0,20.0,25.0,17.0],
        [17.0,21.0,21.0,21.0,19.0,20.0,24.0,15.0,21.0,15.0,18.0,23.0,18.0,16.0,16.0,18.0,24.0,16.0,22.0,16.0,22.0,19.0,17.0,17.0,22.0,19.0,21.0,16.0,24.0,17.0],
        [18.0,23.0,23.0,17.0,25.0,21.0,22.0,17.0,20.0,24.0,18.0,25.0,16.0,15.0,16.0,18.0,23.0,17.0,25.0,17.0,17.0,23.0,24.0,23.0,19.0,16.0,16.0,22.0,22.0,19.0],
        [24.0,18.0,20.0,18.0,19.0,16.0,22.0,18.0,19.0,23.0,18.0,23.0,21.0,24.0,22.0,25.0,21.0,18.0,21.0,16.0,21.0,22.0,18.0,22.0,15.0,19.0,16.0,22.0,16.0,17.0],
        [20.0,21.0,15.0,24.0,20.0,19.0,16.0,24.0,23.0,24.0,22.0,20.0,24.0,18.0,24.0,20.0,17.0,23.0,17.0,22.0,18.0,21.0,25.0,17.0,21.0,18.0,24.0,21.0,15.0,25.0],
        [15.0,23.0,19.0,21.0,17.0,16.0,22.0,23.0,16.0,15.0,19.0,21.0,15.0,18.0,18.0,21.0,25.0,15.0,15.0,15.0,20.0,21.0,18.0,19.0,18.0,21.0,25.0,23.0,25.0,21.0],
        [17.0,25.0,16.0,24.0,16.0,22.0,16.0,15.0,15.0,17.0,25.0,17.0,20.0,21.0,16.0,16.0,17.0,17.0,25.0,21.0,18.0,17.0,20.0,19.0,23.0,18.0,17.0,20.0,17.0,17.0],
        [17.0,21.0,23.0,22.0,23.0,25.0,25.0,22.0,24.0,18.0,24.0,22.0,15.0,24.0,15.0,16.0,19.0,22.0,25.0,19.0,24.0,17.0,16.0,17.0,17.0,25.0,19.0,16.0,23.0,22.0],
    ];

    #[rustfmt::skip]
    const GAP1030_RESOURCE: [[f64; 30]; 10] = [
        [ 5.0,25.0,21.0,11.0,25.0,10.0,15.0,17.0,10.0, 9.0,10.0,23.0,13.0,13.0,23.0,14.0,24.0,17.0,16.0,21.0,22.0,23.0, 7.0,25.0,17.0,17.0,24.0,24.0, 7.0, 8.0],
        [25.0,14.0,19.0,21.0,22.0,11.0,19.0,14.0,12.0,22.0, 9.0,10.0, 7.0,12.0, 6.0, 9.0, 5.0,23.0,24.0,16.0, 8.0,15.0,17.0,22.0,20.0, 6.0,18.0,15.0, 7.0, 7.0],
        [12.0,14.0,15.0,25.0,18.0,25.0, 6.0, 7.0,10.0,17.0,12.0,10.0,12.0,19.0,10.0,12.0,12.0,10.0, 7.0,21.0, 5.0, 5.0,19.0,21.0,12.0, 6.0,14.0, 6.0,19.0,24.0],
        [24.0,11.0,18.0,14.0, 9.0,13.0,18.0,14.0,11.0,12.0, 9.0,14.0,12.0,18.0, 8.0,19.0,24.0,13.0, 7.0,17.0,22.0,20.0,24.0,15.0,12.0,12.0,20.0,21.0, 6.0,22.0],
        [15.0, 9.0,19.0, 9.0,13.0,19.0,25.0,16.0,14.0,17.0, 6.0, 9.0,15.0,12.0,20.0,21.0, 9.0,17.0,19.0,14.0,23.0,19.0,25.0,22.0,13.0,14.0,14.0,23.0,15.0,23.0],
        [25.0,22.0,12.0, 6.0,11.0,25.0,15.0,13.0,13.0, 9.0, 9.0,13.0,19.0,10.0,18.0,23.0,14.0, 8.0, 9.0,19.0,15.0,21.0,10.0, 5.0,23.0,17.0,22.0,11.0,17.0, 5.0],
        [20.0,10.0,18.0, 7.0,12.0,14.0,12.0,18.0, 8.0,25.0,25.0,14.0,16.0,25.0,23.0, 5.0,22.0,22.0, 7.0,24.0,12.0,13.0, 5.0, 8.0,12.0,25.0,24.0,19.0,15.0,14.0],
        [15.0,18.0,21.0,17.0, 7.0, 5.0,10.0,10.0,25.0,13.0,23.0,10.0, 7.0, 7.0,20.0,18.0,21.0,12.0,24.0,10.0, 7.0,25.0,20.0,13.0,19.0,18.0,16.0, 7.0, 9.0, 6.0],
        [20.0,21.0, 9.0,22.0,18.0, 7.0, 5.0,21.0,15.0,13.0, 8.0,23.0,21.0,21.0,17.0,11.0,24.0,11.0,12.0,15.0,19.0, 8.0, 7.0, 8.0,18.0,22.0,18.0,12.0,18.0,17.0],
        [14.0, 6.0,10.0,23.0,12.0,23.0,12.0, 9.0,20.0,25.0,24.0,23.0,20.0,12.0,25.0,17.0,18.0,20.0,18.0,25.0,17.0,20.0,19.0,12.0, 5.0, 6.0,10.0, 5.0, 7.0,15.0],
    ];

    const GAP1030_CAPACITY: [f64; 10] =
        [40.0, 30.0, 32.0, 37.0, 39.0, 35.0, 38.0, 34.0, 36.0, 37.0];

    #[test]
    fn test_benchmark_gap_c1030() {
        let profit: Vec<&[f64]> = GAP1030_PROFIT.iter().map(|r| r.as_slice()).collect();
        let resource: Vec<&[f64]> = GAP1030_RESOURCE.iter().map(|r| r.as_slice()).collect();
        let objective = solve_gap(10, 30, &profit, &resource, &GAP1030_CAPACITY);
        assert!(
            (objective - 709.0).abs() < 0.5,
            "GAP c1030-1 optimal should be 709, got {}",
            objective
        );
    }
}
