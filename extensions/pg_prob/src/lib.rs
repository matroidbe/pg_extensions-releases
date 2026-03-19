//! pg_prob: Probabilistic data types for PostgreSQL
//!
//! This extension provides a `dist` (distribution) type that represents
//! uncertain values. A distribution can be:
//! - A literal (certain value with zero variance)
//! - A parametric distribution (normal, uniform, triangular, beta, etc.)
//! - A computed distribution (result of operations on other distributions)
//!
//! Distributions propagate uncertainty through arithmetic operations,
//! enabling Monte Carlo simulation directly in SQL.

mod correlation;
mod distribution;
mod fitting;
mod operators;
mod sampling;

pub use correlation::*;
pub use distribution::*;
pub use fitting::*;
pub use operators::*;
pub use sampling::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation (README.md) as a string.
/// This enables runtime discovery of extension capabilities via pg_mcp.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_literal_creation() {
        let result = Spi::get_one::<String>("SELECT pgprob.literal(42.0)::text");
        assert!(result.is_ok());
        let text = result.unwrap().unwrap();
        assert!(text.contains("literal"));
    }

    #[pg_test]
    fn test_normal_creation() {
        let result = Spi::get_one::<String>("SELECT pgprob.normal(100.0, 15.0)::text");
        assert!(result.is_ok());
        let text = result.unwrap().unwrap();
        assert!(text.contains("normal"));
    }

    #[pg_test]
    fn test_sample_literal() {
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.literal(42.0))");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), 42.0);
    }

    #[pg_test]
    fn test_sample_normal() {
        let result = Spi::get_one::<f64>("SELECT pgprob.sample(pgprob.normal(100.0, 15.0), 42)");
        assert!(result.is_ok());
        let value = result.unwrap().unwrap();
        assert!(value > 40.0 && value < 160.0);
    }

    #[pg_test]
    fn test_summarize_literal() {
        let result =
            Spi::get_one::<pgrx::JsonB>("SELECT pgprob.summarize(pgprob.literal(42.0), 1000)");
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        let obj = json.0.as_object().unwrap();
        assert_eq!(obj.get("mean").unwrap().as_f64().unwrap(), 42.0);
        assert_eq!(obj.get("p50").unwrap().as_f64().unwrap(), 42.0);
    }

    #[pg_test]
    fn test_summarize_normal() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgprob.summarize(pgprob.normal(100.0, 10.0), 10000, 42)",
        );
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        let obj = json.0.as_object().unwrap();
        let mean = obj.get("mean").unwrap().as_f64().unwrap();
        let std = obj.get("std").unwrap().as_f64().unwrap();
        assert!((mean - 100.0).abs() < 1.0, "mean was {}", mean);
        assert!((std - 10.0).abs() < 1.0, "std was {}", std);
    }

    // Note: SQL operators (+, *, etc.) for dist type are not yet implemented
    // Tests for operator functionality would go here once operators are added
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
