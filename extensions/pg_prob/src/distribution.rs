//! Distribution type definition and constructors
//!
//! The `Dist` type is the core of pgprob. It represents either:
//! - A literal (certain) value
//! - A parametric distribution (normal, uniform, etc.)
//! - A computed distribution (lazy expression tree)
//!
//! The math and the data model live in the pure `prob_core` crate — this
//! module is the pgrx surface: the PostgresType wrapper, SQL constructors,
//! and casts. `Dist` is a `#[serde(transparent)]` newtype over
//! `prob_core::Dist`, so the JSON text I/O and the varlena binary format are
//! byte-identical to what previous pg_prob versions wrote.

use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

pub use prob_core::{DistParams, DistType};

/// The core distribution type stored as JSONB internally.
///
/// Using JSONB storage allows flexible representation of different
/// distribution types and lazy expression trees without requiring
/// a complex custom binary format.
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
#[serde(transparent)]
pub struct Dist(pub prob_core::Dist);

impl Dist {
    /// Check if this distribution is a literal (certain value)
    pub fn is_literal(&self) -> bool {
        self.0.is_literal()
    }

    /// Get literal value if this is a literal distribution
    pub fn as_literal(&self) -> Option<f64> {
        self.0.as_literal()
    }
}

impl From<prob_core::Dist> for Dist {
    fn from(d: prob_core::Dist) -> Self {
        Dist(d)
    }
}

/// Unwrap a `Result` from prob_core into the SQL error surface
pub(crate) fn ok_or_pg<T>(result: Result<T, prob_core::ProbError>) -> T {
    match result {
        Ok(v) => v,
        Err(e) => pgrx::error!("{}", e),
    }
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
    Dist(prob_core::constructors::literal(value))
}

/// Alias for literal_f64 (used in tests and internal code)
pub fn literal(value: f64) -> Dist {
    literal_f64(value)
}

/// Create a literal (certain) distribution from float4
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_f32(value: f32) -> Dist {
    Dist(prob_core::constructors::literal(value as f64))
}

/// Create a literal (certain) distribution from integer
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_i32(value: i32) -> Dist {
    Dist(prob_core::constructors::literal(value as f64))
}

/// Create a literal (certain) distribution from numeric
#[pg_extern(immutable, parallel_safe, name = "literal")]
pub fn literal_numeric(value: pgrx::AnyNumeric) -> Dist {
    let f: f64 = value.try_into().unwrap_or_else(|_| {
        pgrx::error!("Failed to convert numeric to float8");
    });
    Dist(prob_core::constructors::literal(f))
}

/// Create a normal distribution N(mu, sigma)
#[pg_extern(immutable, parallel_safe)]
pub fn normal(mu: f64, sigma: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::normal(mu, sigma)))
}

/// Create a uniform distribution U(min, max)
#[pg_extern(immutable, parallel_safe)]
pub fn uniform(min: f64, max: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::uniform(min, max)))
}

/// Create a triangular distribution Tri(min, mode, max)
#[pg_extern(immutable, parallel_safe)]
pub fn triangular(min: f64, mode: f64, max: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::triangular(
        min, mode, max,
    )))
}

/// Create a beta distribution scaled to [min, max]
#[pg_extern(immutable, parallel_safe)]
pub fn beta(alpha: f64, beta_param: f64, min: default!(f64, 0.0), max: default!(f64, 1.0)) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::beta(
        alpha, beta_param, min, max,
    )))
}

/// Create a log-normal distribution LogN(mu, sigma)
/// mu and sigma are the mean and std of the underlying normal distribution
#[pg_extern(immutable, parallel_safe)]
pub fn lognormal(mu: f64, sigma: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::lognormal(mu, sigma)))
}

/// Create a PERT distribution (modified beta) with min, mode, max
/// lambda controls the weight of the mode (default 4.0)
#[pg_extern(immutable, parallel_safe)]
pub fn pert(min: f64, mode: f64, max: f64, lambda: default!(f64, 4.0)) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::pert(
        min, mode, max, lambda,
    )))
}

/// Create a Poisson distribution Pois(lambda)
#[pg_extern(immutable, parallel_safe)]
pub fn poisson(lambda: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::poisson(lambda)))
}

/// Create an exponential distribution Exp(lambda)
#[pg_extern(immutable, parallel_safe)]
pub fn exponential(lambda: f64) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::exponential(lambda)))
}

// =============================================================================
// Conditional Constructors
// =============================================================================

/// Create a conditional distribution: if test_dist > threshold → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_above(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist(prob_core::constructors::if_above(
        test_dist.0,
        threshold,
        then_dist.0,
        else_dist.0,
    ))
}

/// Create a conditional distribution: if test_dist < threshold → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_below(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist(prob_core::constructors::if_below(
        test_dist.0,
        threshold,
        then_dist.0,
        else_dist.0,
    ))
}

/// Create a probability-based conditional: with probability p → then_dist, else → else_dist
#[pg_extern(immutable, parallel_safe)]
pub fn if_then(probability: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist(ok_or_pg(prob_core::constructors::if_then(
        probability,
        then_dist.0,
        else_dist.0,
    )))
}

// =============================================================================
// DistAvgState (aggregate state for AVG)
// =============================================================================

/// State type for the AVG(dist) aggregate — tracks sum and count.
/// Transparent wrapper over `prob_core::ops::AvgState` (same JSON shape).
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
#[serde(transparent)]
pub struct DistAvgState(pub prob_core::ops::AvgState);

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
        assert_eq!(parsed.0.dist_type, DistType::Normal);
    }

    #[pg_test]
    fn test_wrapper_json_matches_core() {
        // The transparent wrapper must serialize byte-identically to prob_core
        let wrapped = normal(100.0, 15.0);
        let core = prob_core::constructors::normal(100.0, 15.0).unwrap();
        assert_eq!(
            serde_json::to_string(&wrapped).unwrap(),
            serde_json::to_string(&core).unwrap()
        );
    }

    #[pg_test]
    fn test_is_literal() {
        assert!(literal(42.0).is_literal());
        assert!(!normal(0.0, 1.0).is_literal());
    }
}
