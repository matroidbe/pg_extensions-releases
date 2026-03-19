//! Distribution type definition and constructors
//!
//! The `Dist` type is the core of pgprob. It represents either:
//! - A literal (certain) value
//! - A parametric distribution (normal, uniform, etc.)
//! - A computed distribution (lazy expression tree)

use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

/// The core distribution type stored as JSONB internally.
///
/// Using JSONB storage allows flexible representation of different
/// distribution types and lazy expression trees without requiring
/// a complex custom binary format.
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct Dist {
    /// The type of distribution or operation
    #[serde(rename = "t")]
    pub dist_type: DistType,

    /// Parameters specific to each distribution type
    #[serde(rename = "p")]
    pub params: DistParams,
}

/// Types of distributions and operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DistType {
    // Certain value
    Literal,

    // Parametric distributions
    Normal,
    Uniform,
    Triangular,
    Beta,
    LogNormal,
    Pert,
    Poisson,
    Exponential,

    // Binary operations (lazy evaluation)
    Add,
    Sub,
    Mul,
    Div,

    // Unary operations
    Neg,
    Abs,
    Sqrt,
    Exp,
    Ln,

    // Aggregate operations
    Min,
    Max,

    // Conditional operations
    IfAbove,
    IfBelow,
    IfThen,
}

/// Parameters for distributions and operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistParams {
    /// Literal value
    Literal { value: f64 },

    /// Normal distribution: N(mu, sigma)
    Normal { mu: f64, sigma: f64 },

    /// Uniform distribution: U(min, max)
    Uniform { min: f64, max: f64 },

    /// Triangular distribution: Tri(min, mode, max)
    Triangular { min: f64, mode: f64, max: f64 },

    /// Beta distribution: Beta(alpha, beta) scaled to [min, max]
    Beta {
        alpha: f64,
        beta: f64,
        min: f64,
        max: f64,
    },

    /// Log-normal distribution: LogN(mu, sigma)
    LogNormal { mu: f64, sigma: f64 },

    /// PERT distribution: modified beta with min, mode, max
    Pert {
        min: f64,
        mode: f64,
        max: f64,
        lambda: f64,
    },

    /// Poisson distribution: Pois(lambda)
    Poisson { lambda: f64 },

    /// Exponential distribution: Exp(lambda)
    Exponential { lambda: f64 },

    /// Binary operation with two distribution operands
    BinaryOp { left: Box<Dist>, right: Box<Dist> },

    /// Binary operation with distribution and scalar
    ScalarOp { dist: Box<Dist>, scalar: f64 },

    /// Unary operation
    UnaryOp { operand: Box<Dist> },

    /// Conditional: if test > threshold → then_dist, else → else_dist
    Conditional {
        test: Box<Dist>,
        threshold: f64,
        then_dist: Box<Dist>,
        else_dist: Box<Dist>,
    },

    /// Probability branch: with probability p → then_dist, else → else_dist
    ProbBranch {
        probability: f64,
        then_dist: Box<Dist>,
        else_dist: Box<Dist>,
    },
}

// =============================================================================
// PostgreSQL I/O Functions
// =============================================================================

impl InOutFuncs for Dist {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input.to_str().expect("invalid UTF-8 in dist input");
        serde_json::from_str(s).expect("invalid dist JSON format")
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        let json = serde_json::to_string(self).expect("failed to serialize dist");
        buffer.push_str(&json);
    }
}

// =============================================================================
// Constructor Functions
// =============================================================================

/// Create a literal (certain) distribution from float8
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_f64(value: f64) -> Dist {
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal { value },
    }
}

/// Alias for literal_f64 (used in tests and internal code)
pub fn literal(value: f64) -> Dist {
    literal_f64(value)
}

/// Create a literal (certain) distribution from float4
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_f32(value: f32) -> Dist {
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal {
            value: value as f64,
        },
    }
}

/// Create a literal (certain) distribution from integer
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_i32(value: i32) -> Dist {
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal {
            value: value as f64,
        },
    }
}

/// Create a literal (certain) distribution from numeric
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_numeric(value: pgrx::AnyNumeric) -> Dist {
    let f: f64 = value.try_into().unwrap_or_else(|_| {
        pgrx::error!("Failed to convert numeric to float8");
    });
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal { value: f },
    }
}

/// Create a normal distribution N(mu, sigma)
#[pg_extern(immutable, parallel_safe)]
pub fn normal(mu: f64, sigma: f64) -> Dist {
    if sigma < 0.0 {
        pgrx::error!("normal distribution sigma must be non-negative");
    }
    Dist {
        dist_type: DistType::Normal,
        params: DistParams::Normal { mu, sigma },
    }
}

/// Create a uniform distribution U(min, max)
#[pg_extern(immutable, parallel_safe)]
pub fn uniform(min: f64, max: f64) -> Dist {
    if min > max {
        pgrx::error!("uniform distribution min must be <= max");
    }
    Dist {
        dist_type: DistType::Uniform,
        params: DistParams::Uniform { min, max },
    }
}

/// Create a triangular distribution Tri(min, mode, max)
#[pg_extern(immutable, parallel_safe)]
pub fn triangular(min: f64, mode: f64, max: f64) -> Dist {
    if !(min <= mode && mode <= max) {
        pgrx::error!("triangular distribution requires min <= mode <= max");
    }
    Dist {
        dist_type: DistType::Triangular,
        params: DistParams::Triangular { min, mode, max },
    }
}

/// Create a beta distribution scaled to [min, max]
#[pg_extern(immutable, parallel_safe)]
pub fn beta(alpha: f64, beta_param: f64, min: default!(f64, 0.0), max: default!(f64, 1.0)) -> Dist {
    if alpha <= 0.0 || beta_param <= 0.0 {
        pgrx::error!("beta distribution alpha and beta must be positive");
    }
    if min >= max {
        pgrx::error!("beta distribution min must be < max");
    }
    Dist {
        dist_type: DistType::Beta,
        params: DistParams::Beta {
            alpha,
            beta: beta_param,
            min,
            max,
        },
    }
}

/// Create a log-normal distribution LogN(mu, sigma)
/// mu and sigma are the mean and std of the underlying normal distribution
#[pg_extern(immutable, parallel_safe)]
pub fn lognormal(mu: f64, sigma: f64) -> Dist {
    if sigma < 0.0 {
        pgrx::error!("lognormal distribution sigma must be non-negative");
    }
    Dist {
        dist_type: DistType::LogNormal,
        params: DistParams::LogNormal { mu, sigma },
    }
}

/// Create a PERT distribution (modified beta) with min, mode, max
/// lambda controls the weight of the mode (default 4.0)
#[pg_extern(immutable, parallel_safe)]
pub fn pert(min: f64, mode: f64, max: f64, lambda: default!(f64, 4.0)) -> Dist {
    if !(min <= mode && mode <= max) {
        pgrx::error!("PERT distribution requires min <= mode <= max");
    }
    if min == max {
        // Degenerate case: return literal
        return Dist {
            dist_type: DistType::Literal,
            params: DistParams::Literal { value: min },
        };
    }
    Dist {
        dist_type: DistType::Pert,
        params: DistParams::Pert {
            min,
            mode,
            max,
            lambda,
        },
    }
}

/// Create a Poisson distribution Pois(lambda)
#[pg_extern(immutable, parallel_safe)]
pub fn poisson(lambda: f64) -> Dist {
    if lambda <= 0.0 {
        pgrx::error!("poisson distribution lambda must be positive");
    }
    Dist {
        dist_type: DistType::Poisson,
        params: DistParams::Poisson { lambda },
    }
}

/// Create an exponential distribution Exp(lambda)
#[pg_extern(immutable, parallel_safe)]
pub fn exponential(lambda: f64) -> Dist {
    if lambda <= 0.0 {
        pgrx::error!("exponential distribution lambda must be positive");
    }
    Dist {
        dist_type: DistType::Exponential,
        params: DistParams::Exponential { lambda },
    }
}

// =============================================================================
// Conditional Constructors
// =============================================================================

/// Create a conditional distribution: if test_dist > threshold → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_above(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist {
        dist_type: DistType::IfAbove,
        params: DistParams::Conditional {
            test: Box::new(test_dist),
            threshold,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    }
}

/// Create a conditional distribution: if test_dist < threshold → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_below(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist {
        dist_type: DistType::IfBelow,
        params: DistParams::Conditional {
            test: Box::new(test_dist),
            threshold,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    }
}

/// Create a probability-based conditional: with probability p → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_then(probability: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    if !(0.0..=1.0).contains(&probability) {
        pgrx::error!("probability must be between 0 and 1");
    }
    Dist {
        dist_type: DistType::IfThen,
        params: DistParams::ProbBranch {
            probability,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    }
}

// =============================================================================
// DistAvgState (aggregate state for AVG)
// =============================================================================

/// State type for the AVG(dist) aggregate — tracks sum and count
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct DistAvgState {
    pub sum: Dist,
    pub count: i64,
}

impl InOutFuncs for DistAvgState {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input.to_str().expect("invalid UTF-8 in DistAvgState input");
        serde_json::from_str(s).expect("invalid DistAvgState JSON format")
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        let json = serde_json::to_string(self).expect("failed to serialize DistAvgState");
        buffer.push_str(&json);
    }
}

// =============================================================================
// Cast from float8 to dist
// =============================================================================

pgrx::extension_sql!(
    r#"
CREATE CAST (float8 AS @extschema@.dist)
    WITH FUNCTION @extschema@.literal(float8)
    AS IMPLICIT;

CREATE CAST (float4 AS @extschema@.dist)
    WITH FUNCTION @extschema@.literal(float4)
    AS IMPLICIT;

CREATE CAST (integer AS @extschema@.dist)
    WITH FUNCTION @extschema@.literal(integer)
    AS IMPLICIT;

CREATE CAST (numeric AS @extschema@.dist)
    WITH FUNCTION @extschema@.literal(numeric)
    AS IMPLICIT;
"#,
    name = "dist_casts",
    requires = [literal_f64, literal_f32, literal_i32, literal_numeric]
);

// =============================================================================
// Cast from text to dist (for pg_ml integration)
// =============================================================================

/// Parse a distribution from JSON text.
/// This enables implicit casting from text to dist, which is useful
/// when other extensions (like pg_ml) return distribution JSON as text.
#[pg_extern(immutable, parallel_safe, name = "dist_from_text")]
pub fn dist_from_text(input: &str) -> Dist {
    serde_json::from_str(input).unwrap_or_else(|e| pgrx::error!("Invalid distribution JSON: {}", e))
}

pgrx::extension_sql!(
    r#"
CREATE CAST (text AS @extschema@.dist)
    WITH FUNCTION @extschema@.dist_from_text(text)
    AS IMPLICIT;
"#,
    name = "dist_text_cast",
    requires = [dist_from_text]
);

// =============================================================================
// Helper methods
// =============================================================================

impl Dist {
    /// Check if this distribution is a literal (certain value)
    pub fn is_literal(&self) -> bool {
        self.dist_type == DistType::Literal
    }

    /// Get literal value if this is a literal distribution
    pub fn as_literal(&self) -> Option<f64> {
        if let DistParams::Literal { value } = &self.params {
            Some(*value)
        } else {
            None
        }
    }
}

// =============================================================================
// Unit Tests (run inside PostgreSQL via pgrx)
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_literal_serialization() {
        let d = literal(42.0);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("literal"));
        assert!(json.contains("42"));

        let parsed: Dist = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.as_literal(), Some(42.0));
    }

    #[pg_test]
    fn test_normal_serialization() {
        let d = normal(100.0, 15.0);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("normal"));

        let parsed: Dist = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.dist_type, DistType::Normal);
    }

    #[pg_test]
    fn test_is_literal() {
        assert!(literal(42.0).is_literal());
        assert!(!normal(0.0, 1.0).is_literal());
    }
}
