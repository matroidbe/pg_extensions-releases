//! Distribution fitting from data
//!
//! Aggregate functions that fit distributions to column data:
//! - fit_normal(float8) → dist
//! - fit_uniform(float8) → dist
//! - fit_lognormal(float8) → dist
//! - fit_correlation(float8, float8) → float8

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

/// Final function for fit_uniform: compute uniform(min, max) from running stats.
/// Uses order statistics correction: expands range by range/(n+1) on each side
/// to better estimate the true uniform bounds from a finite sample.
#[pg_extern(immutable, parallel_safe)]
fn fit_uniform_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.min == s.max {
            uniform(s.min, s.max + 0.0001)
        } else if s.count <= 2 {
            uniform(s.min, s.max)
        } else {
            let range = s.max - s.min;
            let padding = range / (s.count as f64 + 1.0);
            uniform(s.min - padding, s.max + padding)
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
// FitCorrState — aggregate state for pairwise correlation
// =============================================================================

/// Aggregate state for computing Pearson correlation between two columns.
/// Uses the formula: r = (n*sum_xy - sum_x*sum_y) /
///   sqrt((n*sum_x2 - sum_x^2) * (n*sum_y2 - sum_y^2))
#[derive(Debug, Clone, Serialize, Deserialize, PostgresType)]
#[inoutfuncs]
pub struct FitCorrState {
    pub count: i64,
    pub sum_x: f64,
    pub sum_y: f64,
    pub sum_x2: f64,
    pub sum_y2: f64,
    pub sum_xy: f64,
}

impl InOutFuncs for FitCorrState {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input.to_str().expect("invalid UTF-8 in FitCorrState input");
        serde_json::from_str(s).expect("invalid FitCorrState JSON format")
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        let json = serde_json::to_string(self).expect("failed to serialize FitCorrState");
        buffer.push_str(&json);
    }
}

/// State function for fit_correlation: accumulates paired (x, y) values
#[pg_extern(immutable, parallel_safe)]
fn fit_corr_state(
    state: Option<FitCorrState>,
    x: Option<f64>,
    y: Option<f64>,
) -> Option<FitCorrState> {
    let (x, y) = match (x, y) {
        (Some(xv), Some(yv)) if xv.is_finite() && yv.is_finite() => (xv, yv),
        _ => return state,
    };

    match state {
        None => Some(FitCorrState {
            count: 1,
            sum_x: x,
            sum_y: y,
            sum_x2: x * x,
            sum_y2: y * y,
            sum_xy: x * y,
        }),
        Some(mut s) => {
            s.count += 1;
            s.sum_x += x;
            s.sum_y += y;
            s.sum_x2 += x * x;
            s.sum_y2 += y * y;
            s.sum_xy += x * y;
            Some(s)
        }
    }
}

/// Final function for fit_correlation: compute Pearson r from running stats
#[pg_extern(immutable, parallel_safe)]
fn fit_corr_final(state: Option<FitCorrState>) -> Option<f64> {
    state.map(|s| {
        if s.count < 2 {
            return 0.0;
        }
        let n = s.count as f64;
        let numerator = n * s.sum_xy - s.sum_x * s.sum_y;
        let denom_x = n * s.sum_x2 - s.sum_x * s.sum_x;
        let denom_y = n * s.sum_y2 - s.sum_y * s.sum_y;

        if denom_x <= 0.0 || denom_y <= 0.0 {
            return 0.0;
        }

        (numerator / (denom_x * denom_y).sqrt()).clamp(-1.0, 1.0)
    })
}

pgrx::extension_sql!(
    r#"
CREATE AGGREGATE @extschema@.fit_correlation(float8, float8) (
    SFUNC = @extschema@.fit_corr_state,
    STYPE = @extschema@.FitCorrState,
    FINALFUNC = @extschema@.fit_corr_final
);
"#,
    name = "fit_corr_aggregate",
    requires = [fit_corr_state, fit_corr_final]
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

    // ----- fit_correlation tests -----

    #[pg_test]
    fn test_fit_correlation_perfect_positive() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_pos (x float8, y float8)")?;
        Spi::run("INSERT INTO corr_pos SELECT v, v * 2 + 1 FROM generate_series(1, 100) AS v")?;
        let r = Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_pos")?.unwrap();
        assert!((r - 1.0).abs() < 0.001, "expected ~1.0, got {}", r);
        Ok(())
    }

    #[pg_test]
    fn test_fit_correlation_perfect_negative() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_neg (x float8, y float8)")?;
        Spi::run("INSERT INTO corr_neg SELECT v, -v FROM generate_series(1, 100) AS v")?;
        let r = Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_neg")?.unwrap();
        assert!((r - (-1.0)).abs() < 0.001, "expected ~-1.0, got {}", r);
        Ok(())
    }

    #[pg_test]
    fn test_fit_correlation_uncorrelated() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_zero (x float8, y float8)")?;
        Spi::run(
            "INSERT INTO corr_zero SELECT sin(v * 0.1), cos(v * 0.1) \
             FROM generate_series(1, 10000) AS v",
        )?;
        let r = Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_zero")?.unwrap();
        assert!(r.abs() < 0.1, "expected near-zero, got {}", r);
        Ok(())
    }

    #[pg_test]
    fn test_fit_correlation_constant_column() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_const (x float8, y float8)")?;
        Spi::run("INSERT INTO corr_const SELECT v, 5.0 FROM generate_series(1, 50) AS v")?;
        let r =
            Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_const")?.unwrap();
        assert_eq!(r, 0.0, "constant column should give 0");
        Ok(())
    }

    #[pg_test]
    fn test_fit_correlation_single_row() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_one (x float8, y float8)")?;
        Spi::run("INSERT INTO corr_one VALUES (1, 2)")?;
        let r = Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_one")?.unwrap();
        assert_eq!(r, 0.0, "single row should give 0");
        Ok(())
    }

    #[pg_test]
    fn test_fit_correlation_with_nulls() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE corr_null (x float8, y float8)")?;
        Spi::run("INSERT INTO corr_null VALUES (1, 2), (NULL, 4), (3, NULL), (4, 8), (5, 10)")?;
        // Should skip rows with NULLs, compute from valid pairs
        let r = Spi::get_one::<f64>("SELECT pgprob.fit_correlation(x, y) FROM corr_null")?;
        assert!(r.is_some(), "should return a value");
        Ok(())
    }

    // ----- improved fit_uniform test -----

    #[pg_test]
    fn test_fit_uniform_padding() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE fit_uni_pad (v float8)")?;
        // 100 values from 10 to 90 — true uniform should extend slightly beyond
        Spi::run("INSERT INTO fit_uni_pad SELECT generate_series(10, 90)")?;
        let json =
            Spi::get_one::<String>("SELECT pgprob.fit_uniform(v)::text FROM fit_uni_pad")?.unwrap();
        // The fitted uniform should have min < 10 and max > 90 due to padding
        let dist: serde_json::Value = serde_json::from_str(&json).unwrap();
        let params = &dist["p"]["Uniform"];
        let min = params["min"].as_f64().unwrap();
        let max = params["max"].as_f64().unwrap();
        assert!(min < 10.0, "expected min < 10, got {}", min);
        assert!(max > 90.0, "expected max > 90, got {}", max);
        Ok(())
    }
}
