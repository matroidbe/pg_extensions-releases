//! Distribution fitting from data.
//!
//! Aggregate-style state machines that fit distributions to observed values:
//! - `FitState` + `fit_normal_final` / `fit_uniform_final` / `fit_lognormal_final`
//! - `FitCorrState` + `fit_corr_final` (pairwise Pearson correlation)
//!
//! The serde shapes match pg_prob's `FitState` / `FitCorrState` on-disk formats.

use crate::dist::{Dist, DistParams, DistType};
use serde::{Deserialize, Serialize};

/// Aggregate state tracking running statistics for distribution fitting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FitState {
    pub count: i64,
    pub sum: f64,
    pub sum_sq: f64,
    pub sum_ln: f64,
    pub sum_ln_sq: f64,
    pub min: f64,
    pub max: f64,
}

/// State transition shared by all fitting aggregates.
/// NULL and non-finite values are skipped.
pub fn fit_state(state: Option<FitState>, value: Option<f64>) -> Option<FitState> {
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

/// Fit a normal(mean, std) from running stats
pub fn fit_normal_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.count == 0 {
            return Dist {
                dist_type: DistType::Normal,
                params: DistParams::Normal {
                    mu: 0.0,
                    sigma: 1.0,
                },
            };
        }
        let n = s.count as f64;
        let mu = s.sum / n;
        let variance = (s.sum_sq / n - mu * mu).max(0.0);
        let sigma = variance.sqrt().max(0.0001); // guard against zero
        Dist {
            dist_type: DistType::Normal,
            params: DistParams::Normal { mu, sigma },
        }
    })
}

/// Fit a uniform(min, max) from running stats.
/// Uses order statistics correction: expands range by range/(n+1) on each side
/// to better estimate the true uniform bounds from a finite sample.
pub fn fit_uniform_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        let (min, max) = if s.min == s.max {
            (s.min, s.max + 0.0001)
        } else if s.count <= 2 {
            (s.min, s.max)
        } else {
            let range = s.max - s.min;
            let padding = range / (s.count as f64 + 1.0);
            (s.min - padding, s.max + padding)
        };
        Dist {
            dist_type: DistType::Uniform,
            params: DistParams::Uniform { min, max },
        }
    })
}

/// Fit a lognormal(mu, sigma) from log-space running stats
pub fn fit_lognormal_final(state: Option<FitState>) -> Option<Dist> {
    state.map(|s| {
        if s.count == 0 {
            return Dist {
                dist_type: DistType::LogNormal,
                params: DistParams::LogNormal {
                    mu: 0.0,
                    sigma: 1.0,
                },
            };
        }
        let n = s.count as f64;
        let mu_ln = s.sum_ln / n;
        let variance_ln = (s.sum_ln_sq / n - mu_ln * mu_ln).max(0.0);
        let sigma_ln = variance_ln.sqrt().max(0.0001); // guard against zero
        Dist {
            dist_type: DistType::LogNormal,
            params: DistParams::LogNormal {
                mu: mu_ln,
                sigma: sigma_ln,
            },
        }
    })
}

/// Aggregate state for computing Pearson correlation between two columns.
/// Uses the formula: r = (n*sum_xy - sum_x*sum_y) /
///   sqrt((n*sum_x2 - sum_x^2) * (n*sum_y2 - sum_y^2))
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FitCorrState {
    pub count: i64,
    pub sum_x: f64,
    pub sum_y: f64,
    pub sum_x2: f64,
    pub sum_y2: f64,
    pub sum_xy: f64,
}

/// State transition for fit_correlation: accumulates paired (x, y) values.
/// Pairs with a NULL or non-finite member are skipped.
pub fn fit_corr_state(
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
pub fn fit_corr_final(state: Option<FitCorrState>) -> Option<f64> {
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

/// Convenience: fit a normal distribution to a slice of values
pub fn fit_normal(values: &[f64]) -> Option<Dist> {
    let state = values.iter().fold(None, |s, v| fit_state(s, Some(*v)));
    fit_normal_final(state)
}

/// Convenience: fit a uniform distribution to a slice of values
pub fn fit_uniform(values: &[f64]) -> Option<Dist> {
    let state = values.iter().fold(None, |s, v| fit_state(s, Some(*v)));
    fit_uniform_final(state)
}

/// Convenience: fit a lognormal distribution to a slice of values
pub fn fit_lognormal(values: &[f64]) -> Option<Dist> {
    let state = values.iter().fold(None, |s, v| fit_state(s, Some(*v)));
    fit_lognormal_final(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sample;

    #[test]
    fn test_fit_normal_known_data() {
        let d = fit_normal(&[10.0, 20.0, 30.0, 40.0, 50.0]).unwrap();
        let m = sample::mean(&d, 10000, Some(42));
        assert!((m - 30.0).abs() < 2.0, "fitted mean was {}", m);
    }

    #[test]
    fn test_fit_normal_single_value() {
        let d = fit_normal(&[42.0]).unwrap();
        let m = sample::mean(&d, 1000, Some(42));
        assert!((m - 42.0).abs() < 1.0, "fitted mean was {}", m);
    }

    #[test]
    fn test_fit_uniform_padding() {
        // 81 values from 10 to 90 — fitted bounds should extend slightly beyond
        let values: Vec<f64> = (10..=90).map(|v| v as f64).collect();
        let d = fit_uniform(&values).unwrap();
        match &d.params {
            DistParams::Uniform { min, max } => {
                assert!(*min < 10.0, "expected min < 10, got {}", min);
                assert!(*max > 90.0, "expected max > 90, got {}", max);
            }
            other => panic!("expected uniform params, got {:?}", other),
        }
    }

    #[test]
    fn test_fit_lognormal_type() {
        let values: Vec<f64> = (1..=6).map(|v| (v as f64 * 0.5).exp()).collect();
        let d = fit_lognormal(&values).unwrap();
        assert_eq!(d.dist_type, DistType::LogNormal);
        let json = serde_json::to_string(&d).unwrap();
        assert!(json.contains("log_normal"), "got: {}", json);
    }

    #[test]
    fn test_fit_state_skips_nulls_and_nonfinite() {
        let s = fit_state(None, Some(10.0));
        let s = fit_state(s, None);
        let s = fit_state(s, Some(f64::NAN));
        let s = fit_state(s, Some(30.0));
        assert_eq!(s.unwrap().count, 2);
    }

    #[test]
    fn test_fit_correlation_perfect_positive() {
        let state = (1..=100).fold(None, |s, v| {
            fit_corr_state(s, Some(v as f64), Some(v as f64 * 2.0 + 1.0))
        });
        let r = fit_corr_final(state).unwrap();
        assert!((r - 1.0).abs() < 0.001, "expected ~1.0, got {}", r);
    }

    #[test]
    fn test_fit_correlation_perfect_negative() {
        let state = (1..=100).fold(None, |s, v| {
            fit_corr_state(s, Some(v as f64), Some(-(v as f64)))
        });
        let r = fit_corr_final(state).unwrap();
        assert!((r + 1.0).abs() < 0.001, "expected ~-1.0, got {}", r);
    }

    #[test]
    fn test_fit_correlation_constant_column() {
        let state = (1..=50).fold(None, |s, v| fit_corr_state(s, Some(v as f64), Some(5.0)));
        assert_eq!(fit_corr_final(state).unwrap(), 0.0);
    }

    #[test]
    fn test_fit_correlation_single_row() {
        let state = fit_corr_state(None, Some(1.0), Some(2.0));
        assert_eq!(fit_corr_final(state).unwrap(), 0.0);
    }

    #[test]
    fn test_fit_correlation_skips_null_pairs() {
        let mut state = None;
        for (x, y) in [
            (Some(1.0), Some(2.0)),
            (None, Some(4.0)),
            (Some(3.0), None),
            (Some(4.0), Some(8.0)),
            (Some(5.0), Some(10.0)),
        ] {
            state = fit_corr_state(state, x, y);
        }
        assert_eq!(state.as_ref().unwrap().count, 3);
        let r = fit_corr_final(state).unwrap();
        assert!((r - 1.0).abs() < 0.001, "expected ~1.0, got {}", r);
    }

    /// Lock the FitState / FitCorrState serde shapes (pg_prob on-disk formats)
    #[test]
    fn test_fit_state_serde_shape_lock() {
        let s = fit_state(None, Some(2.0)).unwrap();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{"count":1,"sum":2.0,"sum_sq":4.0,"sum_ln":{ln},"sum_ln_sq":{ln_sq},"min":2.0,"max":2.0}}"#,
                ln = serde_json::to_string(&2.0_f64.ln()).unwrap(),
                ln_sq = serde_json::to_string(&(2.0_f64.ln() * 2.0_f64.ln())).unwrap()
            )
        );

        let c = fit_corr_state(None, Some(1.0), Some(2.0)).unwrap();
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(
            json,
            r#"{"count":1,"sum_x":1.0,"sum_y":2.0,"sum_x2":1.0,"sum_y2":4.0,"sum_xy":2.0}"#
        );
    }
}
