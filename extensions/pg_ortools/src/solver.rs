//! Constraint optimization solver using good_lp + HiGHS
//!
//! Replaces the previous PyO3/Python OR-Tools integration with a pure Rust
//! solver backed by HiGHS (compiled C++). See design/pg_ortools/python-solver.md
//! for the previous implementation.

use crate::error::PgOrtoolsError;
use good_lp::solvers::highs::highs;
use good_lp::{variable, Expression, ProblemVariables, Solution, SolverModel};
use pgrx::prelude::*;
use std::collections::HashMap;

// =============================================================================
// Data Structures
// =============================================================================

#[derive(Debug)]
pub struct VariableData {
    pub name: String,
    pub var_type: String,
    pub domain_min: i64,
    pub domain_max: i64,
}

#[derive(Debug)]
pub struct ConstraintData {
    pub expression: String,
}

#[derive(Debug)]
pub struct ProblemData {
    #[allow(dead_code)]
    pub id: i64,
    pub variables: Vec<VariableData>,
    pub constraints: Vec<ConstraintData>,
    pub objective_type: Option<String>,
    pub objective_expr: Option<String>,
}

// =============================================================================
// Expression Parser
// =============================================================================

/// Parse a term like "42", "x", "2*x", or "x*2" into a good_lp Expression.
fn parse_term(
    term: &str,
    vars: &HashMap<String, good_lp::Variable>,
) -> Result<Expression, PgOrtoolsError> {
    let term = term.trim();

    if term.is_empty() {
        return Err(PgOrtoolsError::InvalidExpression("empty term".to_string()));
    }

    // Single variable
    if let Some(&var) = vars.get(term) {
        return Ok(Expression::from(var));
    }

    // Single number
    if let Ok(n) = term.parse::<f64>() {
        return Ok(Expression::from(n));
    }

    // Multiplication: coeff*var or var*coeff
    if let Some(pos) = term.find('*') {
        let left = term[..pos].trim();
        let right = term[pos + 1..].trim();

        if let (Ok(coeff), Some(&var)) = (left.parse::<f64>(), vars.get(right)) {
            return Ok(coeff * var);
        }
        if let (Some(&var), Ok(coeff)) = (vars.get(left), right.parse::<f64>()) {
            return Ok(coeff * var);
        }

        return Err(PgOrtoolsError::InvalidExpression(format!(
            "cannot parse multiplication: {}",
            term
        )));
    }

    Err(PgOrtoolsError::InvalidExpression(format!(
        "cannot parse term: {}",
        term
    )))
}

/// Parse an arithmetic expression like "2*x + 3*y - z + 5" into a good_lp Expression.
pub fn parse_expression(
    expr: &str,
    vars: &HashMap<String, good_lp::Variable>,
) -> Result<Expression, PgOrtoolsError> {
    let expr = expr.trim();

    if expr.is_empty() {
        return Ok(Expression::from(0.0));
    }

    let mut result = Expression::from(0.0);
    let mut current = String::new();
    let mut sign: f64 = 1.0;

    for (i, ch) in expr.chars().enumerate() {
        match ch {
            '+' => {
                let trimmed = current.trim().to_string();
                if !trimmed.is_empty() {
                    let term = parse_term(&trimmed, vars)?;
                    if sign < 0.0 {
                        result -= term;
                    } else {
                        result += term;
                    }
                }
                current.clear();
                sign = 1.0;
            }
            '-' if i > 0 && !current.trim().is_empty() => {
                let trimmed = current.trim().to_string();
                let term = parse_term(&trimmed, vars)?;
                if sign < 0.0 {
                    result -= term;
                } else {
                    result += term;
                }
                current.clear();
                sign = -1.0;
            }
            '-' if i > 0 && current.trim().is_empty() => {
                // Double sign or sign after operator
                sign = -sign;
            }
            '-' if i == 0 => {
                sign = -1.0;
            }
            _ => {
                current.push(ch);
            }
        }
    }

    // Handle last term
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        let term = parse_term(&trimmed, vars)?;
        if sign < 0.0 {
            result -= term;
        } else {
            result += term;
        }
    }

    Ok(result)
}

/// Parse a constraint string like "x + y <= 10" into a good_lp Constraint.
pub fn parse_constraint(
    expr: &str,
    vars: &HashMap<String, good_lp::Variable>,
) -> Result<good_lp::Constraint, PgOrtoolsError> {
    let expr = expr.trim();

    // Try operators in order (longest first to avoid ambiguity)
    for op in &["<=", ">=", "!=", "==", "<", ">", "="] {
        if let Some(pos) = expr.find(op) {
            let left_str = &expr[..pos];
            let right_str = &expr[pos + op.len()..];

            let left = parse_expression(left_str, vars)?;
            let right = parse_expression(right_str, vars)?;

            return match *op {
                "<=" => Ok((left - right).leq(0.0)),
                ">=" => Ok((left - right).geq(0.0)),
                "==" | "=" => Ok((left - right).eq(Expression::from(0.0))),
                "!=" => Err(PgOrtoolsError::InvalidConstraint(
                    "!= (not-equal) constraints are not supported by MIP solvers. \
                     Use two separate < and > constraints instead."
                        .to_string(),
                )),
                "<" => {
                    // For integer variables: x < y is equivalent to x <= y - 1
                    Ok((left - right).leq(-1.0))
                }
                ">" => {
                    // For integer variables: x > y is equivalent to x >= y + 1
                    Ok((left - right).geq(1.0))
                }
                _ => Err(PgOrtoolsError::InvalidConstraint(format!(
                    "unknown operator: {}",
                    op
                ))),
            };
        }
    }

    Err(PgOrtoolsError::InvalidConstraint(format!(
        "no comparison operator found in: {}",
        expr
    )))
}

// =============================================================================
// Solver
// =============================================================================

/// Solve a problem by loading it from the database and running HiGHS.
/// When `greedy` is true, accepts the first feasible solution without optimizing.
pub fn solve_problem(
    problem_name: &str,
    greedy: bool,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let problem_data = load_problem_from_db(problem_name)?;
    solve_problem_data(&problem_data, problem_name, greedy)
}

/// Solve a problem from its data (can be called without DB access for testing).
/// When `greedy` is true, sets mip_rel_gap=1.0 so HiGHS returns the first feasible solution.
pub fn solve_problem_data(
    problem_data: &ProblemData,
    problem_name: &str,
    greedy: bool,
) -> Result<serde_json::Value, PgOrtoolsError> {
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

            // Store solution in database
            store_solution(problem_name, &result)?;

            Ok(result)
        }
        Err(e) => {
            let status = match e {
                good_lp::ResolutionError::Infeasible => "INFEASIBLE",
                good_lp::ResolutionError::Unbounded => "UNBOUNDED",
                _ => "ERROR",
            };

            let result = serde_json::json!({
                "status": status,
                "objective": null,
                "values": {},
                "time_ms": elapsed_ms,
                "error": e.to_string(),
            });

            // Store failed result too
            store_solution(problem_name, &result)?;

            Err(PgOrtoolsError::SolverError(format!("{}: {}", status, e)))
        }
    }
}

// =============================================================================
// Database Helpers
// =============================================================================

pub fn load_problem_from_db(problem_name: &str) -> Result<ProblemData, PgOrtoolsError> {
    let problem_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?
    .ok_or_else(|| PgOrtoolsError::ProblemNotFound(problem_name.to_string()))?;

    let objective_type = Spi::get_one_with_args::<String>(
        "SELECT objective_type FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?;

    let objective_expr = Spi::get_one_with_args::<String>(
        "SELECT objective_expr FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?;

    let variables = Spi::connect(|client| {
        let mut vars = Vec::new();
        let query = format!(
            "SELECT name::text, var_type::text, domain_min, domain_max \
             FROM pgortools.variables WHERE problem_id = {}",
            problem_id
        );
        let table = client.select(&query, None, &[])?;

        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let var_type: String = row.get(2)?.unwrap_or_else(|| "int".to_string());
            let domain_min: i64 = row.get(3)?.unwrap_or(0);
            let domain_max: i64 = row.get(4)?.unwrap_or(i64::MAX);

            vars.push(VariableData {
                name,
                var_type,
                domain_min,
                domain_max,
            });
        }
        Ok::<_, pgrx::spi::Error>(vars)
    })?;

    let constraints = Spi::connect(|client| {
        let mut cons = Vec::new();
        let query = format!(
            "SELECT expression::text FROM pgortools.constraints WHERE problem_id = {}",
            problem_id
        );
        let table = client.select(&query, None, &[])?;

        for row in table {
            let expression: String = row.get(1)?.unwrap_or_default();
            cons.push(ConstraintData { expression });
        }
        Ok::<_, pgrx::spi::Error>(cons)
    })?;

    Ok(ProblemData {
        id: problem_id,
        variables,
        constraints,
        objective_type,
        objective_expr,
    })
}

pub fn store_solution(
    problem_name: &str,
    solution: &serde_json::Value,
) -> Result<(), PgOrtoolsError> {
    let status = solution["status"].as_str().unwrap_or("UNKNOWN");
    let objective = solution["objective"].as_f64();
    let values = pgrx::JsonB(solution["values"].clone());
    let time_ms = solution["time_ms"].as_i64().unwrap_or(0) as i32;

    Spi::run_with_args(
        r#"
        INSERT INTO pgortools.solutions (problem_id, status, objective_value, variable_values, solve_time_ms)
        SELECT p.id, $2, $3, $4, $5
        FROM pgortools.problems p
        WHERE p.name = $1
        "#,
        &[
            problem_name.into(),
            status.into(),
            objective.into(),
            values.into(),
            time_ms.into(),
        ],
    )?;

    Ok(())
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
        // Workers: A, B. Tasks: 1, 2.
        // Cost: A1=10, A2=15, B1=12, B2=8
        // Each worker does exactly 1 task, each task needs exactly 1 worker
        let mut pv = ProblemVariables::new();
        let a1 = pv.add(variable().binary());
        let a2 = pv.add(variable().binary());
        let b1 = pv.add(variable().binary());
        let b2 = pv.add(variable().binary());

        let cost = 10.0 * a1 + 15.0 * a2 + 12.0 * b1 + 8.0 * b2;

        let mut model = pv.minimise(&cost).using(highs);
        model.set_verbose(false);

        // Each worker assigned to exactly 1 task
        model = model.with((Expression::from(a1) + Expression::from(a2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(b1) + Expression::from(b2)).eq(Expression::from(1.0)));

        // Each task needs exactly 1 worker
        model = model.with((Expression::from(a1) + Expression::from(b1)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a2) + Expression::from(b2)).eq(Expression::from(1.0)));

        let solution = model.solve().unwrap();
        // Optimal: A1=1, B2=1, cost=18
        assert!((solution.eval(&cost) - 18.0).abs() < 0.5);
    }

    #[test]
    fn test_solve_greedy_finds_feasible() {
        // Same 2x2 assignment but with greedy mode (mip_rel_gap=1.0)
        // Should find *a* feasible solution (not necessarily optimal)
        let mut pv = ProblemVariables::new();
        let a1 = pv.add(variable().binary());
        let a2 = pv.add(variable().binary());
        let b1 = pv.add(variable().binary());
        let b2 = pv.add(variable().binary());

        let cost = 10.0 * a1 + 15.0 * a2 + 12.0 * b1 + 8.0 * b2;

        let mut model = pv.minimise(&cost).using(highs);
        model.set_verbose(false);
        // Greedy: accept first feasible solution
        model = model.set_option("mip_rel_gap", 1.0);

        model = model.with((Expression::from(a1) + Expression::from(a2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(b1) + Expression::from(b2)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a1) + Expression::from(b1)).eq(Expression::from(1.0)));
        model = model.with((Expression::from(a2) + Expression::from(b2)).eq(Expression::from(1.0)));

        let solution = model.solve().unwrap();
        // Must be feasible: exactly one of {A1+B2} or {A2+B1} (cost 18 or 27)
        let total = solution.eval(&cost);
        assert!(
            total == 18.0 || total == 27.0,
            "cost should be 18 or 27, got {}",
            total
        );
    }
}
