//! Arithmetic operators for distributions
//!
//! These operators build lazy expression trees that are evaluated
//! during sampling. This allows efficient Monte Carlo simulation
//! by sampling once and propagating through the expression tree.

use crate::distribution::{Dist, DistAvgState, DistParams, DistType};
use pgrx::prelude::*;

// =============================================================================
// Distribution + Distribution
// =============================================================================

/// Add two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
pub fn dist_add(left: Dist, right: Dist) -> Dist {
    // Optimization: if both are literals, compute directly
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l + r },
        };
    }

    Dist {
        dist_type: DistType::Add,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Subtract two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
pub fn dist_sub(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l - r },
        };
    }

    Dist {
        dist_type: DistType::Sub,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Multiply two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
pub fn dist_mul(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l * r },
        };
    }

    Dist {
        dist_type: DistType::Mul,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// Divide two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
pub fn dist_div(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        if r == 0.0 {
            pgrx::error!("division by zero");
        }
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l / r },
        };
    }

    Dist {
        dist_type: DistType::Div,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

// =============================================================================
// Distribution OP Scalar (and vice versa)
// =============================================================================

/// Multiply distribution by scalar
#[pg_extern(immutable, parallel_safe)]
pub fn dist_mul_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v * scalar },
        };
    }

    Dist {
        dist_type: DistType::Mul,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Multiply scalar by distribution
#[pg_extern(immutable, parallel_safe)]
pub fn scalar_mul_dist(scalar: f64, dist: Dist) -> Dist {
    dist_mul_scalar(dist, scalar)
}

/// Divide distribution by scalar
#[pg_extern(immutable, parallel_safe)]
pub fn dist_div_scalar(dist: Dist, scalar: f64) -> Dist {
    if scalar == 0.0 {
        pgrx::error!("division by zero");
    }

    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v / scalar },
        };
    }

    Dist {
        dist_type: DistType::Div,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Divide scalar by distribution
#[pg_extern(immutable, parallel_safe)]
pub fn scalar_div_dist(scalar: f64, dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        if v == 0.0 {
            pgrx::error!("division by zero");
        }
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: scalar / v },
        };
    }

    // scalar / dist = scalar * (1/dist) - but we need to represent this
    // We'll use a binary op with literal on the left
    Dist {
        dist_type: DistType::Div,
        params: DistParams::BinaryOp {
            left: Box::new(Dist {
                dist_type: DistType::Literal,
                params: DistParams::Literal { value: scalar },
            }),
            right: Box::new(dist),
        },
    }
}

/// Add scalar to distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_add_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v + scalar },
        };
    }

    Dist {
        dist_type: DistType::Add,
        params: DistParams::ScalarOp {
            dist: Box::new(dist),
            scalar,
        },
    }
}

/// Add distribution to scalar
#[pg_extern(immutable, parallel_safe)]
pub fn scalar_add_dist(scalar: f64, dist: Dist) -> Dist {
    dist_add_scalar(dist, scalar)
}

/// Subtract scalar from distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_sub_scalar(dist: Dist, scalar: f64) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v - scalar },
        };
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
#[pg_extern(immutable, parallel_safe)]
pub fn scalar_sub_dist(scalar: f64, dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: scalar - v },
        };
    }

    Dist {
        dist_type: DistType::Sub,
        params: DistParams::BinaryOp {
            left: Box::new(Dist {
                dist_type: DistType::Literal,
                params: DistParams::Literal { value: scalar },
            }),
            right: Box::new(dist),
        },
    }
}

// =============================================================================
// Unary operations
// =============================================================================

/// Negate a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_neg(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: -v },
        };
    }

    Dist {
        dist_type: DistType::Neg,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Absolute value of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_abs(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v.abs() },
        };
    }

    Dist {
        dist_type: DistType::Abs,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Square root of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_sqrt(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        if v < 0.0 {
            pgrx::error!("cannot take square root of negative number");
        }
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v.sqrt() },
        };
    }

    Dist {
        dist_type: DistType::Sqrt,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Exponential of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_exp(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v.exp() },
        };
    }

    Dist {
        dist_type: DistType::Exp,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

/// Natural log of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn dist_ln(dist: Dist) -> Dist {
    if let Some(v) = dist.as_literal() {
        if v <= 0.0 {
            pgrx::error!("cannot take log of non-positive number");
        }
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: v.ln() },
        };
    }

    Dist {
        dist_type: DistType::Ln,
        params: DistParams::UnaryOp {
            operand: Box::new(dist),
        },
    }
}

// =============================================================================
// SQL Operator Definitions
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Distribution + Distribution
CREATE OPERATOR @extschema@.+ (
    LEFTARG = @extschema@.dist,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.dist_add,
    COMMUTATOR = +
);

-- Distribution - Distribution
CREATE OPERATOR @extschema@.- (
    LEFTARG = @extschema@.dist,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.dist_sub
);

-- Distribution * Distribution
CREATE OPERATOR @extschema@.* (
    LEFTARG = @extschema@.dist,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.dist_mul,
    COMMUTATOR = *
);

-- Distribution / Distribution
CREATE OPERATOR @extschema@./ (
    LEFTARG = @extschema@.dist,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.dist_div
);

-- Distribution * float8
CREATE OPERATOR @extschema@.* (
    LEFTARG = @extschema@.dist,
    RIGHTARG = float8,
    FUNCTION = @extschema@.dist_mul_scalar,
    COMMUTATOR = *
);

-- float8 * Distribution
CREATE OPERATOR @extschema@.* (
    LEFTARG = float8,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.scalar_mul_dist,
    COMMUTATOR = *
);

-- Distribution / float8
CREATE OPERATOR @extschema@./ (
    LEFTARG = @extschema@.dist,
    RIGHTARG = float8,
    FUNCTION = @extschema@.dist_div_scalar
);

-- float8 / Distribution
CREATE OPERATOR @extschema@./ (
    LEFTARG = float8,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.scalar_div_dist
);

-- Distribution + float8
CREATE OPERATOR @extschema@.+ (
    LEFTARG = @extschema@.dist,
    RIGHTARG = float8,
    FUNCTION = @extschema@.dist_add_scalar,
    COMMUTATOR = +
);

-- float8 + Distribution
CREATE OPERATOR @extschema@.+ (
    LEFTARG = float8,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.scalar_add_dist,
    COMMUTATOR = +
);

-- Distribution - float8
CREATE OPERATOR @extschema@.- (
    LEFTARG = @extschema@.dist,
    RIGHTARG = float8,
    FUNCTION = @extschema@.dist_sub_scalar
);

-- float8 - Distribution
CREATE OPERATOR @extschema@.- (
    LEFTARG = float8,
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.scalar_sub_dist
);

-- Unary minus
CREATE OPERATOR @extschema@.- (
    RIGHTARG = @extschema@.dist,
    FUNCTION = @extschema@.dist_neg
);
"#,
    name = "dist_operators",
    requires = [
        dist_add,
        dist_sub,
        dist_mul,
        dist_div,
        dist_mul_scalar,
        scalar_mul_dist,
        dist_div_scalar,
        scalar_div_dist,
        dist_add_scalar,
        scalar_add_dist,
        dist_sub_scalar,
        scalar_sub_dist,
        dist_neg
    ]
);

// =============================================================================
// Coalesce / NULL handling
// =============================================================================

/// Return the first non-NULL distribution (2 args)
#[pg_extern(immutable, parallel_safe, name = "coalesce")]
fn coalesce2(a: Option<Dist>, b: Option<Dist>) -> Option<Dist> {
    a.or(b)
}

/// Return the first non-NULL distribution (3 args)
#[pg_extern(immutable, parallel_safe, name = "coalesce")]
fn coalesce3(a: Option<Dist>, b: Option<Dist>, c: Option<Dist>) -> Option<Dist> {
    a.or(b).or(c)
}

/// Return the first non-NULL distribution (4 args)
#[pg_extern(immutable, parallel_safe, name = "coalesce")]
fn coalesce4(a: Option<Dist>, b: Option<Dist>, c: Option<Dist>, d: Option<Dist>) -> Option<Dist> {
    a.or(b).or(c).or(d)
}

/// Return the distribution if non-NULL, else the default
#[pg_extern(immutable, parallel_safe)]
fn nvl(dist: Option<Dist>, default: Dist) -> Dist {
    dist.unwrap_or(default)
}

// =============================================================================
// Aggregate Functions
// =============================================================================

/// State function for SUM(dist) - uses dist as the state type
#[pg_extern(immutable, parallel_safe)]
fn dist_sum_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_add(s, v)),
    }
}

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.sum(@extschema@.dist) (
    SFUNC = @extschema@.dist_sum_state,
    STYPE = @extschema@.dist
);
"#,
    name = "dist_sum_aggregate",
    requires = [dist_sum_state]
);

// =============================================================================
// MIN aggregate
// =============================================================================

/// Binary min operation on two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
fn dist_min_op(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l.min(r) },
        };
    }
    Dist {
        dist_type: DistType::Min,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// State function for MIN(dist)
#[pg_extern(immutable, parallel_safe)]
fn dist_min_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_min_op(s, v)),
    }
}

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.min(@extschema@.dist) (
    SFUNC = @extschema@.dist_min_state,
    STYPE = @extschema@.dist
);
"#,
    name = "dist_min_aggregate",
    requires = [dist_min_state]
);

// =============================================================================
// MAX aggregate
// =============================================================================

/// Binary max operation on two distributions (lazy)
#[pg_extern(immutable, parallel_safe)]
fn dist_max_op(left: Dist, right: Dist) -> Dist {
    if let (Some(l), Some(r)) = (left.as_literal(), right.as_literal()) {
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: l.max(r) },
        };
    }
    Dist {
        dist_type: DistType::Max,
        params: DistParams::BinaryOp {
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// State function for MAX(dist)
#[pg_extern(immutable, parallel_safe)]
fn dist_max_state(state: Option<Dist>, value: Option<Dist>) -> Option<Dist> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(dist_max_op(s, v)),
    }
}

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.max(@extschema@.dist) (
    SFUNC = @extschema@.dist_max_state,
    STYPE = @extschema@.dist
);
"#,
    name = "dist_max_aggregate",
    requires = [dist_max_state]
);

// =============================================================================
// AVG aggregate
// =============================================================================

/// State function for AVG(dist) - tracks sum and count
#[pg_extern(immutable, parallel_safe)]
fn dist_avg_state(state: Option<DistAvgState>, value: Option<Dist>) -> Option<DistAvgState> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(DistAvgState { sum: v, count: 1 }),
        (Some(s), Some(v)) => Some(DistAvgState {
            sum: dist_add(s.sum, v),
            count: s.count + 1,
        }),
    }
}

/// Final function for AVG(dist) - divides sum by count
#[pg_extern(immutable, parallel_safe)]
fn dist_avg_final(state: Option<DistAvgState>) -> Option<Dist> {
    state.map(|s| {
        if s.count <= 1 {
            s.sum
        } else {
            dist_div_scalar(s.sum, s.count as f64)
        }
    })
}

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.avg(@extschema@.dist) (
    SFUNC = @extschema@.dist_avg_state,
    STYPE = @extschema@.DistAvgState,
    FINALFUNC = @extschema@.dist_avg_final
);
"#,
    name = "dist_avg_aggregate",
    requires = [dist_avg_state, dist_avg_final]
);

// =============================================================================
// Unit Tests (run inside PostgreSQL via pgrx)
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::distribution::{literal_f64 as literal, normal};

    #[pg_test]
    fn test_add_literals() {
        let result = dist_add(literal(10.0), literal(5.0));
        assert!(result.is_literal());
        assert_eq!(result.as_literal(), Some(15.0));
    }

    #[pg_test]
    fn test_add_distributions_creates_add_type() {
        let result = dist_add(normal(100.0, 10.0), normal(50.0, 5.0));
        assert!(!result.is_literal());
        assert_eq!(result.dist_type, DistType::Add);
    }

    #[pg_test]
    fn test_mul_scalar() {
        let result = dist_mul_scalar(literal(10.0), 3.0);
        assert!(result.is_literal());
        assert_eq!(result.as_literal(), Some(30.0));
    }

    #[pg_test]
    fn test_neg() {
        let result = dist_neg(literal(42.0));
        assert_eq!(result.as_literal(), Some(-42.0));
    }

    #[pg_test]
    fn test_coalesce_first_non_null() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.coalesce(NULL::pgprob.dist, pgprob.literal(42.0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 42.0);
    }

    #[pg_test]
    fn test_coalesce_returns_first() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.coalesce(pgprob.literal(10.0), pgprob.literal(20.0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 10.0);
    }

    #[pg_test]
    fn test_coalesce_all_null() {
        let result = Spi::get_one::<bool>(
            "SELECT pgprob.coalesce(NULL::pgprob.dist, NULL::pgprob.dist) IS NULL",
        );
        assert_eq!(result.unwrap().unwrap(), true);
    }

    #[pg_test]
    fn test_coalesce3_cascading() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.coalesce(NULL::pgprob.dist, NULL::pgprob.dist, pgprob.literal(99.0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 99.0);
    }

    #[pg_test]
    fn test_nvl_with_null() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.nvl(NULL::pgprob.dist, pgprob.literal(7.0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 7.0);
    }

    #[pg_test]
    fn test_nvl_with_value() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.nvl(pgprob.literal(5.0), pgprob.literal(7.0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 5.0);
    }

    #[pg_test]
    fn test_min_aggregate_literals() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE test_min (d pgprob.dist)")?;
        Spi::run("INSERT INTO test_min VALUES (pgprob.literal(10)), (pgprob.literal(5)), (pgprob.literal(8))")?;
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.min(d)) FROM test_min");
        assert_eq!(result.unwrap().unwrap(), 5.0);
        Ok(())
    }

    #[pg_test]
    fn test_max_aggregate_literals() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE test_max (d pgprob.dist)")?;
        Spi::run("INSERT INTO test_max VALUES (pgprob.literal(10)), (pgprob.literal(5)), (pgprob.literal(8))")?;
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.max(d)) FROM test_max");
        assert_eq!(result.unwrap().unwrap(), 10.0);
        Ok(())
    }

    #[pg_test]
    fn test_avg_aggregate_literals() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE test_avg (d pgprob.dist)")?;
        Spi::run("INSERT INTO test_avg VALUES (pgprob.literal(10)), (pgprob.literal(20)), (pgprob.literal(30))")?;
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.avg(d)) FROM test_avg");
        assert_eq!(result.unwrap().unwrap(), 20.0);
        Ok(())
    }

    #[pg_test]
    fn test_avg_aggregate_with_null() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE test_avg_null (d pgprob.dist)")?;
        Spi::run(
            "INSERT INTO test_avg_null VALUES (pgprob.literal(10)), (NULL), (pgprob.literal(30))",
        )?;
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.avg(d)) FROM test_avg_null");
        assert_eq!(result.unwrap().unwrap(), 20.0);
        Ok(())
    }

    #[pg_test]
    fn test_if_above_true() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.if_above(pgprob.literal(100), 50.0, pgprob.literal(1), pgprob.literal(0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 1.0);
    }

    #[pg_test]
    fn test_if_above_false() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.if_above(pgprob.literal(10), 50.0, pgprob.literal(1), pgprob.literal(0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 0.0);
    }

    #[pg_test]
    fn test_if_below_true() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.if_below(pgprob.literal(10), 50.0, pgprob.literal(1), pgprob.literal(0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 1.0);
    }

    #[pg_test]
    fn test_if_then_always_true() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.if_then(1.0, pgprob.literal(42), pgprob.literal(0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 42.0);
    }

    #[pg_test]
    fn test_if_then_always_false() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.sample(pgprob.if_then(0.0, pgprob.literal(42), pgprob.literal(0)))",
        );
        assert_eq!(result.unwrap().unwrap(), 0.0);
    }

    #[pg_test]
    fn test_if_then_probabilistic() {
        // 50% chance → mean should be ~0.5
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.mean(pgprob.if_then(0.5, pgprob.literal(1), pgprob.literal(0)), 10000, 42)",
        );
        let m = result.unwrap().unwrap();
        assert!((m - 0.5).abs() < 0.05, "mean was {}", m);
    }
}
