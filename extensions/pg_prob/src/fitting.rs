//! Distribution fitting from data
//!
//! Aggregate functions that fit distributions to column data:
//! - fit_normal(float8) → dist
//! - fit_uniform(float8) → dist
//! - fit_lognormal(float8) → dist

use crate::distribution::{lognormal, normal, uniform, Dist};
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

// =============================================================================
// FitState — shared aggregate state for all fitters
// =============================================================================

/// Aggregate state tracking running statistics for distribution fitting
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct FitState {
    pub count: i64,
    pub sum: f64,
    pub sum_sq: f64,
    pub sum_ln: f64,
    pub sum_ln_sq: f64,
    pub min: f64,
    pub max: f64,
}

impl InOutFuncs for FitState {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input.to_str().expect("invalid UTF-8 in FitState input");
        serde_json::from_str(s).expect("invalid FitState JSON format")
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        let json = serde_json::to_string(self).expect("failed to serialize FitState");
        buffer.push_str(&json);
    }
}

// =============================================================================
// Shared state transition function
// =============================================================================

/// State function shared by all fitting aggregates
#[pg_extern(immutable, parallel_safe)]
fn fit_state(state: Option<FitState>, value: Option<f64>) -> Option<FitState> {
    let value = match value {
        Some(v) if v.is_finite() => v,
        _ => return state, // skip NULL and non-finite
    };

    match state {
        None => {
            let ln_v = if value > 0.0 { value.ln() } else { 0.0 };
            Some(FitState {
                count: 1,
                sum: value,
                sum_sq: value * value,
                sum_ln: ln_v,
                sum_ln_sq: ln_v * ln_v,
                min: value,
                max: value,
            })
        }
        Some(mut s) => {
            s.count += 1;
            s.sum += value;
            s.sum_sq += value * value;
            if value > 0.0 {
                let ln_v = value.ln();
                s.sum_ln += ln_v;
                s.sum_ln_sq += ln_v * ln_v;
            }
            s.min = s.min.min(value);
            s.max = s.max.max(value);
            Some(s)
        }
    }
}

// =============================================================================
// Final functions
// =============================================================================

/// Final function for fit_normal: compute normal(mean, std) from running stats
#[pg_extern(immutable, parallel_safe)]
fn fit_normal_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.count == 0 {
            return normal(0.0, 1.0);
        }
        let n = s.count as f64;
        let mu = s.sum / n;
        let variance = (s.sum_sq / n - mu * mu).max(0.0);
        let sigma = variance.sqrt().max(0.0001); // guard against zero
        normal(mu, sigma)
    })
}

/// Final function for fit_uniform: compute uniform(min, max) from running stats
#[pg_extern(immutable, parallel_safe)]
fn fit_uniform_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.min == s.max {
            // Degenerate: add small epsilon to avoid min==max error
            uniform(s.min, s.max + 0.0001)
        } else {
            uniform(s.min, s.max)
        }
    })
}

/// Final function for fit_lognormal: compute lognormal(mu, sigma) from log-space running stats
#[pg_extern(immutable, parallel_safe)]
fn fit_lognormal_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.count == 0 {
            return lognormal(0.0, 1.0);
        }
        let n = s.count as f64;
        let mu_ln = s.sum_ln / n;
        let variance_ln = (s.sum_ln_sq / n - mu_ln * mu_ln).max(0.0);
        let sigma_ln = variance_ln.sqrt().max(0.0001); // guard against zero
        lognormal(mu_ln, sigma_ln)
    })
}

// =============================================================================
// SQL Aggregate Definitions
// =============================================================================

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.fit_normal(float8) (
    SFUNC = @extschema@.fit_state,
    STYPE = @extschema@.FitState,
    FINALFUNC = @extschema@.fit_normal_final
);

CREATE AGGREGATE @extschema@.fit_uniform(float8) (
    SFUNC = @extschema@.fit_state,
    STYPE = @extschema@.FitState,
    FINALFUNC = @extschema@.fit_uniform_final
);

CREATE AGGREGATE @extschema@.fit_lognormal(float8) (
    SFUNC = @extschema@.fit_state,
    STYPE = @extschema@.FitState,
    FINALFUNC = @extschema@.fit_lognormal_final
);
"#,
    name = "fit_aggregates",
    requires = [
        fit_state,
        fit_normal_final,
        fit_uniform_final,
        fit_lognormal_final
    ]
);

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_fit_normal_known_data() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE fit_test (v float8)")?;
        Spi::run("INSERT INTO fit_test VALUES (10), (20), (30), (40), (50)")?;
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.mean(pgprob.fit_normal(v), 10000, 42) FROM fit_test",
        );
        let m = result.unwrap().unwrap();
        assert!((m - 30.0).abs() < 2.0, "fitted mean was {}", m);
        Ok(())
    }

    #[pg_test]
    fn test_fit_uniform_extremes() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE fit_uni (v float8)")?;
        Spi::run("INSERT INTO fit_uni VALUES (5), (10), (15), (20), (25)")?;
        // Mean of uniform(5,25) = 15
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.mean(pgprob.fit_uniform(v), 10000, 42) FROM fit_uni",
        );
        let m = result.unwrap().unwrap();
        assert!((m - 15.0).abs() < 1.0, "fitted uniform mean was {}", m);
        Ok(())
    }

    #[pg_test]
    fn test_fit_normal_single_value() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE fit_single (v float8)")?;
        Spi::run("INSERT INTO fit_single VALUES (42)")?;
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.mean(pgprob.fit_normal(v), 1000, 42) FROM fit_single",
        );
        let m = result.unwrap().unwrap();
        assert!((m - 42.0).abs() < 1.0, "fitted mean was {}", m);
        Ok(())
    }

    #[pg_test]
    fn test_fit_lognormal() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE fit_ln (v float8)")?;
        // e^1 ≈ 2.718, e^2 ≈ 7.389, e^3 ≈ 20.086
        Spi::run("INSERT INTO fit_ln SELECT exp(v) FROM generate_series(0.5, 3.0, 0.5) AS v")?;
        let result = Spi::get_one::<String>("SELECT pgprob.fit_lognormal(v)::text FROM fit_ln");
        let text = result.unwrap().unwrap();
        assert!(text.contains("log_normal"), "got: {}", text);
        Ok(())
    }
}
