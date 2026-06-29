//! Expression and constraint parser for MIP problems.

use crate::error::OrtoolsCoreError;
use good_lp::Expression;
use std::collections::HashMap;

/// Parse a term like "42", "x", "2*x", or "x*2" into a good_lp Expression.
pub fn parse_term(
    term: &str,
    vars: &HashMap<String, good_lp::Variable>,
) -> Result<Expression, OrtoolsCoreError> {
    let term = term.trim();

    if term.is_empty() {
        return Err(OrtoolsCoreError::InvalidExpression(
            "empty term".to_string(),
        ));
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

        return Err(OrtoolsCoreError::InvalidExpression(format!(
            "cannot parse multiplication: {}",
            term
        )));
    }

    Err(OrtoolsCoreError::InvalidExpression(format!(
        "cannot parse term: {}",
        term
    )))
}

/// Parse an arithmetic expression like "2*x + 3*y - z + 5" into a good_lp Expression.
pub fn parse_expression(
    expr: &str,
    vars: &HashMap<String, good_lp::Variable>,
) -> Result<Expression, OrtoolsCoreError> {
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
) -> Result<good_lp::Constraint, OrtoolsCoreError> {
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
                "!=" => Err(OrtoolsCoreError::InvalidConstraint(
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
                _ => Err(OrtoolsCoreError::InvalidConstraint(format!(
                    "unknown operator: {}",
                    op
                ))),
            };
        }
    }

    Err(OrtoolsCoreError::InvalidConstraint(format!(
        "no comparison operator found in: {}",
        expr
    )))
}
