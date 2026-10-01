//! Distribution type definition
//!
//! The `Dist` type is the core of the probabilistic engine. It represents
//! either a literal (certain) value, a parametric distribution, or a lazy
//! expression tree built from operations on other distributions.
//!
//! The serde attributes here define the canonical JSON wire/storage format
//! (`{"t": "...", "p": {...}}`). pg_prob stores this shape; do not change it.

use serde::{Deserialize, Serialize};

/// The core distribution type, serialized as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constructors;

    /// Lock the exact JSON shape — this IS pg_prob's on-disk format.
    /// If this test breaks, PG-written and embedded-written dist values
    /// are no longer interchangeable.
    #[test]
    fn test_serde_shape_lock_literal() {
        let d = constructors::literal(42.0);
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, r#"{"t":"literal","p":{"Literal":{"value":42.0}}}"#);
    }

    #[test]
    fn test_serde_shape_lock_normal() {
        let d = constructors::normal(100.0, 15.0).unwrap();
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(
            json,
            r#"{"t":"normal","p":{"Normal":{"mu":100.0,"sigma":15.0}}}"#
        );
    }

    #[test]
    fn test_serde_shape_lock_lognormal_snake_case() {
        let d = constructors::lognormal(0.0, 1.0).unwrap();
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(
            json,
            r#"{"t":"log_normal","p":{"LogNormal":{"mu":0.0,"sigma":1.0}}}"#
        );
    }

    #[test]
    fn test_serde_shape_lock_expression_tree() {
        let d = crate::ops::dist_add(
            constructors::normal(100.0, 10.0).unwrap(),
            constructors::literal(5.0),
        );
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(
            json,
            r#"{"t":"add","p":{"BinaryOp":{"left":{"t":"normal","p":{"Normal":{"mu":100.0,"sigma":10.0}}},"right":{"t":"literal","p":{"Literal":{"value":5.0}}}}}}"#
        );
    }

    #[test]
    fn test_serde_round_trip() {
        let d = constructors::pert(10.0, 20.0, 40.0, 4.0).unwrap();
        let json = serde_json::to_string(&d).unwrap();
        let parsed: Dist = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.dist_type, DistType::Pert);
        assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
    }

    #[test]
    fn test_is_literal() {
        assert!(constructors::literal(42.0).is_literal());
        assert!(!constructors::normal(0.0, 1.0).unwrap().is_literal());
    }

    #[test]
    fn test_as_literal() {
        assert_eq!(constructors::literal(42.0).as_literal(), Some(42.0));
        assert_eq!(constructors::normal(0.0, 1.0).unwrap().as_literal(), None);
    }
}
