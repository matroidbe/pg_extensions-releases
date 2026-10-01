//! Lazy arithmetic operations and aggregate state transitions.
//!
//! Operations build expression trees evaluated during sampling. Literal
//! operands are folded eagerly (matching pg_prob's operator functions).

use crate::dist::{Dist, DistParams, DistType};
use crate::error::ProbError;
use serde::{Deserialize, Serialize};

fn lit(value: f64) -> Dist {
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal { value },
    }
}

// =============================================================================
// Distribution OP Distribution
// =============================================================================

/// Add two distributions (lazy; literals fold)
pub fn dist_add(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return lit(l + r);
    }

    Dist {
        dist_type: DistType::Add,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Subtract two distributions (lazy; literals fold)
pub fn dist_sub(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return lit(l - r);
    }

    Dist {
        dist_type: DistType::Sub,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Multiply two distributions (lazy; literals fold)
pub fn dist_mul(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return lit(l * r);
    }

    Dist {
        dist_type: DistType::Mul,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Divide two distributions (lazy; literals fold, literal 0/0 errors)
pub fn dist_div(left: Dist, right: Dist) -> Result<Dist, ProbError> {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        if r == 0.0 {
            return Err(ProbError::new("division by zero"));
        }
        return Ok(lit(l / r));
    }

    Ok(Dist {
        dist_type: DistType::Div,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    })
}

// =============================================================================
// Distribution OP Scalar (and vice versa)
// =============================================================================

/// Multiply distribution by scalar
pub fn dist_mul_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(v * scalar);
    }

    Dist {
        dist_type: DistType::Mul,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Divide distribution by scalar
pub fn dist_div_scalar(dist: Dist, scalar: f64) -> Result<Dist, ProbError> {
    if scalar == 0.0 {
        return Err(ProbError::new("division by zero"));
    }

    if let Some(v) = dist.as_literal() {
        return Ok(lit(v / scalar));
    }

    Ok(Dist {
        dist_type: DistType::Div,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    })
}

/// Divide scalar by distribution
pub fn scalar_div_dist(scalar: f64, dist: Dist) -> Result<Dist, ProbError> {
    if let Some(v) = dist.as_literal() {
        if v == 0.0 {
            return Err(ProbError::new("division by zero"));
        }
        return Ok(lit(scalar / v));
    }

    // Represented as a binary op with a literal on the left
    Ok(Dist {
        dist_type: DistType::Div,
        params: DistParams::BinaryOp {
            left: Box::new(lit(scalar)),
            right: Box::new(dist),
        },
    })
}

/// Add scalar to distribution
pub fn dist_add_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(v + scalar);
    }

    Dist {
        dist_type: DistType::Add,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Subtract scalar from distribution
pub fn dist_sub_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(v - scalar);
    }

    Dist {
        dist_type: DistType::Sub,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Subtract distribution from scalar
pub fn scalar_sub_dist(scalar: f64, dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(scalar - v);
    }

    Dist {
        dist_type: DistType::Sub,
        params: DistParams::BinaryOp {
            left: Box::new(lit(scalar)),
            right: Box::new(dist),
        },
    }
}

// =============================================================================
// Unary operations
// =============================================================================

/// Negate a distribution
pub fn dist_neg(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(-v);
    }

    Dist {
        dist_type: DistType::Neg,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Absolute value of a distribution
pub fn dist_abs(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(v.abs());
    }

    Dist {
        dist_type: DistType::Abs,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Square root of a distribution
pub fn dist_sqrt(dist: Dist) -> Result<Dist, ProbError> {
    if let Some(v) = dist.as_literal() {
        if v < 0.0 {
            return Err(ProbError::new("cannot take square root of negative number"));
        }
        return Ok(lit(v.sqrt()));
    }

    Ok(Dist {
        dist_type: DistType::Sqrt,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    })
}

/// Exponential of a distribution
pub fn dist_exp(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return lit(v.exp());
    }

    Dist {
        dist_type: DistType::Exp,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Natural log of a distribution
pub fn dist_ln(dist: Dist) -> Result<Dist, ProbError> {
    if let Some(v) = dist.as_literal() {
        if v <= 0.0 {
            return Err(ProbError::new("cannot take log of non-positive number"));
        }
        return Ok(lit(v.ln()));
    }

    Ok(Dist {
        dist_type: DistType::Ln,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    })
}

// =============================================================================
// Min / Max binary operations
// =============================================================================

/// Binary min operation on two distributions (lazy; literals fold)
pub fn dist_min_op(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return lit(l.min(r));
    }
    Dist {
        dist_type: DistType::Min,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Binary max operation on two distributions (lazy; literals fold)
pub fn dist_max_op(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return lit(l.max(r));
    }
    Dist {
        dist_type: DistType::Max,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

// =============================================================================
// Aggregate state transitions (SUM / MIN / MAX / AVG over dist)
// =============================================================================

/// State transition for SUM(dist)
pub fn sum_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_add(s, v)),
    }
}

/// State transition for MIN(dist)
pub fn min_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_min_op(s, v)),
    }
}

/// State transition for MAX(dist)
pub fn max_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_max_op(s, v)),
    }
}

/// State for the AVG(dist) aggregate — tracks sum and count.
/// Serde shape matches pg_prob's `DistAvgState` on-disk format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvgState {
    pub sum: Dist,
    pub count: i64,
}

/// State transition for AVG(dist)
pub fn avg_state(state: Option<AvgState>, value: Option<Dist>) -> Option<AvgState> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(AvgState { sum: v, count: 1 }),
        (Some(s), Some(v)) => Some(AvgState {
            sum: dist_add(s.sum, v),
            count: s.count + 1,
        }),
    }
}

/// Final function for AVG(dist) — divides sum by count
pub fn avg_final(state: Option<AvgState>) -> Option<Dist> {
    state.map(|s| {
        if s.count <= 1 {
            s.sum
        } else {
            // count > 1 is never zero, so the division cannot fail
            dist_div_scalar(s.sum, s.count as f64).expect("count > 1 cannot divide by zero")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constructors::{literal, normal};
    use crate::sample;

    #[test]
    fn test_add_literals_fold() {
        let result = dist_add(literal(10.0), literal(5.0));
        assert!(result.is_literal());
        assert_eq!(result.as_literal(), Some(15.0));
    }

    #[test]
    fn test_add_distributions_creates_add_type() {
        let result = dist_add(normal(100.0, 10.0).unwrap(), normal(50.0, 5.0).unwrap());
        assert!(!result.is_literal());
        assert_eq!(result.dist_type, DistType::Add);
    }

    #[test]
    fn test_sub_mul_literals_fold() {
        assert_eq!(
            dist_sub(literal(10.0), literal(4.0)).as_literal(),
            Some(6.0)
        );
        assert_eq!(
            dist_mul(literal(10.0), literal(4.0)).as_literal(),
            Some(40.0)
        );
    }

    #[test]
    fn test_div_by_zero_literal_errors() {
        assert!(dist_div(literal(10.0), literal(0.0)).is_err());
        assert!(dist_div_scalar(literal(10.0), 0.0).is_err());
        assert!(scalar_div_dist(1.0, literal(0.0)).is_err());
    }

    #[test]
    fn test_scalar_ops() {
        assert_eq!(dist_mul_scalar(literal(10.0), 3.0).as_literal(), Some(30.0));
        assert_eq!(dist_add_scalar(literal(10.0), 3.0).as_literal(), Some(13.0));
        assert_eq!(dist_sub_scalar(literal(10.0), 3.0).as_literal(), Some(7.0));
        assert_eq!(scalar_sub_dist(3.0, literal(10.0)).as_literal(), Some(-7.0));
        assert_eq!(
            scalar_div_dist(10.0, literal(4.0)).unwrap().as_literal(),
            Some(2.5)
        );
    }

    #[test]
    fn test_unary_ops() {
        assert_eq!(dist_neg(literal(42.0)).as_literal(), Some(-42.0));
        assert_eq!(dist_abs(literal(-42.0)).as_literal(), Some(42.0));
        assert_eq!(dist_sqrt(literal(9.0)).unwrap().as_literal(), Some(3.0));
        assert!(dist_sqrt(literal(-1.0)).is_err());
        assert!(dist_ln(literal(0.0)).is_err());
        assert_eq!(dist_exp(literal(0.0)).as_literal(), Some(1.0));
    }

    #[test]
    fn test_min_max_ops() {
        assert_eq!(
            dist_min_op(literal(10.0), literal(5.0)).as_literal(),
            Some(5.0)
        );
        assert_eq!(
            dist_max_op(literal(10.0), literal(5.0)).as_literal(),
            Some(10.0)
        );
    }

    #[test]
    fn test_sum_state() {
        let s = sum_state(None, Some(literal(10.0)));
        let s = sum_state(s, Some(literal(20.0)));
        let s = sum_state(s, None);
        assert_eq!(s.unwrap().as_literal(), Some(30.0));
    }

    #[test]
    fn test_min_max_state() {
        let s = min_state(None, Some(literal(10.0)));
        let s = min_state(s, Some(literal(5.0)));
        let s = min_state(s, Some(literal(8.0)));
        assert_eq!(s.unwrap().as_literal(), Some(5.0));

        let s = max_state(None, Some(literal(10.0)));
        let s = max_state(s, Some(literal(5.0)));
        let s = max_state(s, Some(literal(8.0)));
        assert_eq!(s.unwrap().as_literal(), Some(10.0));
    }

    #[test]
    fn test_avg_state_and_final() {
        let s = avg_state(None, Some(literal(10.0)));
        let s = avg_state(s, Some(literal(20.0)));
        let s = avg_state(s, Some(literal(30.0)));
        let avg = avg_final(s).unwrap();
        assert_eq!(avg.as_literal(), Some(20.0));
    }

    #[test]
    fn test_avg_state_skips_nulls() {
        let s = avg_state(None, Some(literal(10.0)));
        let s = avg_state(s, None);
        let s = avg_state(s, Some(literal(30.0)));
        let avg = avg_final(s).unwrap();
        assert_eq!(avg.as_literal(), Some(20.0));
    }

    #[test]
    fn test_lazy_tree_samples_correctly() {
        // (normal(100, 0) + 5) * 2 = 210 (sigma 0 → deterministic)
        let tree = dist_mul_scalar(dist_add_scalar(normal(100.0, 0.0).unwrap(), 5.0), 2.0);
        assert_eq!(sample::sample(&tree, None), 210.0);
    }
}
