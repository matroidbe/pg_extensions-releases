//! Monte Carlo sampling and summarization.

use crate::dist::{Dist, DistParams, DistType};
use crate::error::ProbError;
use rand::prelude::*;
use rand::Rng;
use rand::SeedableRng;
use rand_distr::{Beta, Exp, LogNormal, Normal, Poisson, Triangular, Uniform};
use serde_json::json;

/// Create an RNG from an optional seed
pub fn make_rng(seed: Option<i64>) -> Box<dyn RngCore> {
    match seed {
        Some(s) => Box::new(rand::rngs::StdRng::seed_from_u64(s as u64)),
        None => Box::new(rand::thread_rng()),
    }
}

/// Sample a single value from a distribution
pub fn sample(dist: &Dist, seed: Option<i64>) -> f64 {
    let mut rng = make_rng(seed);
    sample_dist(dist, &mut *rng)
}

/// Recursive sampling over the distribution / expression tree.
///
/// Unsupported (type, params) combinations return NaN — the pgrx wrapper adds
/// a `pgrx::warning!` on top of this for the SQL surface.
pub fn sample_dist(dist: &Dist, rng: &mut dyn RngCore) -> f64 {
    match (&dist.dist_type, &dist.params) {
        // Literal: return the value
        (DistType::Literal, DistParams::Literal { value }) => *value,

        // Normal distribution
        (DistType::Normal, DistParams::Normal { mu, sigma }) => {
            if *sigma == 0.0 {
                *mu
            } else {
                let normal = Normal::new(*mu, *sigma).expect("invalid normal params");
                normal.sample(rng)
            }
        }

        // Uniform distribution
        (DistType::Uniform, DistParams::Uniform { min, max }) => {
            if min == max {
                *min
            } else {
                let uniform = Uniform::new(*min, *max);
                uniform.sample(rng)
            }
        }

        // Triangular distribution
        (DistType::Triangular, DistParams::Triangular { min, mode, max }) => {
            if min == max {
                *min
            } else {
                let tri = Triangular::new(*min, *max, *mode).expect("invalid triangular params");
                tri.sample(rng)
            }
        }

        // Beta distribution scaled to [min, max]
        (
            DistType::Beta,
            DistParams::Beta {
                alpha,
                beta,
                min,
                max,
            },
        ) => {
            let b = Beta::new(*alpha, *beta).expect("invalid beta params");
            let sample = b.sample(rng);
            min + sample * (max - min)
        }

        // Log-normal distribution
        (DistType::LogNormal, DistParams::LogNormal { mu, sigma }) => {
            if *sigma == 0.0 {
                mu.exp()
            } else {
                let ln = LogNormal::new(*mu, *sigma).expect("invalid lognormal params");
                ln.sample(rng)
            }
        }

        // PERT distribution (modified beta)
        (
            DistType::Pert,
            DistParams::Pert {
                min,
                mode,
                max,
                lambda,
            },
        ) => {
            if min == max {
                *min
            } else {
                // PERT uses beta distribution with calculated alpha/beta
                let range = max - min;
                let mu = (min + max + lambda * mode) / (lambda + 2.0);
                let alpha = if range > 0.0 {
                    ((mu - min) * (2.0 * mode - min - max)) / ((mode - mu) * (max - min))
                } else {
                    1.0
                };
                let beta_param = alpha * (max - mu) / (mu - min);

                // Handle edge cases
                let alpha = alpha.max(0.001);
                let beta_param = beta_param.max(0.001);

                let b =
                    Beta::new(alpha, beta_param).unwrap_or_else(|_| Beta::new(1.0, 1.0).unwrap());
                let sample = b.sample(rng);
                min + sample * range
            }
        }

        // Poisson distribution
        (DistType::Poisson, DistParams::Poisson { lambda }) => {
            let pois = Poisson::new(*lambda).expect("invalid poisson params");
            pois.sample(rng)
        }

        // Exponential distribution
        (DistType::Exponential, DistParams::Exponential { lambda }) => {
            let exp = Exp::new(*lambda).expect("invalid exponential params");
            exp.sample(rng)
        }

        // Binary operations
        (DistType::Add, DistParams::BinaryOp { left, right }) => {
            sample_dist(left, rng) + sample_dist(right, rng)
        }
        (DistType::Add, DistParams::ScalarOp { dist, scalar }) => sample_dist(dist, rng) + scalar,

        (DistType::Sub, DistParams::BinaryOp { left, right }) => {
            sample_dist(left, rng) - sample_dist(right, rng)
        }
        (DistType::Sub, DistParams::ScalarOp { dist, scalar }) => sample_dist(dist, rng) - scalar,

        (DistType::Mul, DistParams::BinaryOp { left, right }) => {
            sample_dist(left, rng) * sample_dist(right, rng)
        }
        (DistType::Mul, DistParams::ScalarOp { dist, scalar }) => sample_dist(dist, rng) * scalar,

        (DistType::Div, DistParams::BinaryOp { left, right }) => {
            let r = sample_dist(right, rng);
            if r == 0.0 {
                f64::NAN
            } else {
                sample_dist(left, rng) / r
            }
        }
        (DistType::Div, DistParams::ScalarOp { dist, scalar }) => {
            if *scalar == 0.0 {
                f64::NAN
            } else {
                sample_dist(dist, rng) / scalar
            }
        }

        // Unary operations
        (DistType::Neg, DistParams::UnaryOp { operand }) => -sample_dist(operand, rng),
        (DistType::Abs, DistParams::UnaryOp { operand }) => sample_dist(operand, rng).abs(),
        (DistType::Sqrt, DistParams::UnaryOp { operand }) => {
            let v = sample_dist(operand, rng);
            if v < 0.0 {
                f64::NAN
            } else {
                v.sqrt()
            }
        }
        (DistType::Exp, DistParams::UnaryOp { operand }) => sample_dist(operand, rng).exp(),
        (DistType::Ln, DistParams::UnaryOp { operand }) => {
            let v = sample_dist(operand, rng);
            if v <= 0.0 {
                f64::NAN
            } else {
                v.ln()
            }
        }

        // Min/Max operations
        (DistType::Min, DistParams::BinaryOp { left, right }) => {
            let l = sample_dist(left, rng);
            let r = sample_dist(right, rng);
            l.min(r)
        }
        (DistType::Max, DistParams::BinaryOp { left, right }) => {
            let l = sample_dist(left, rng);
            let r = sample_dist(right, rng);
            l.max(r)
        }

        // Conditional operations
        (
            DistType::IfAbove,
            DistParams::Conditional {
                test,
                threshold,
                then_dist,
                else_dist,
            },
        ) => {
            let test_val = sample_dist(test, rng);
            if test_val > *threshold {
                sample_dist(then_dist, rng)
            } else {
                sample_dist(else_dist, rng)
            }
        }
        (
            DistType::IfBelow,
            DistParams::Conditional {
                test,
                threshold,
                then_dist,
                else_dist,
            },
        ) => {
            let test_val = sample_dist(test, rng);
            if test_val < *threshold {
                sample_dist(then_dist, rng)
            } else {
                sample_dist(else_dist, rng)
            }
        }
        (
            DistType::IfThen,
            DistParams::ProbBranch {
                probability,
                then_dist,
                else_dist,
            },
        ) => {
            let u: f64 = rng.gen();
            if u < *probability {
                sample_dist(then_dist, rng)
            } else {
                sample_dist(else_dist, rng)
            }
        }

        // Fallback: unsupported combination
        _ => f64::NAN,
    }
}

/// Sample multiple values from a distribution
pub fn samples(dist: &Dist, n: i32, seed: Option<i64>) -> Vec<f64> {
    if n <= 0 {
        return vec![];
    }

    let mut rng = make_rng(seed);
    (0..n).map(|_| sample_dist(dist, &mut *rng)).collect()
}

/// Compute summary statistics for a distribution via Monte Carlo sampling.
/// Returns `{mean, std, min, max, p5, p10, p25, p50, p75, p90, p95, n}`.
pub fn summarize(dist: &Dist, n: i32, seed: Option<i64>) -> Result<serde_json::Value, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }

    let mut rng = make_rng(seed);

    // Generate samples
    let mut samples: Vec<f64> = (0..n).map(|_| sample_dist(dist, &mut *rng)).collect();

    // Sort for percentile computation
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Compute statistics
    let n_f = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n_f;

    let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f;
    let std = variance.sqrt();

    let min = samples.first().copied().unwrap_or(f64::NAN);
    let max = samples.last().copied().unwrap_or(f64::NAN);

    let percentile = |p: f64| -> f64 {
        let idx = (p * (samples.len() - 1) as f64).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };

    Ok(json!({
        "mean": mean,
        "std": std,
        "min": min,
        "max": max,
        "p5": percentile(0.05),
        "p10": percentile(0.10),
        "p25": percentile(0.25),
        "p50": percentile(0.50),
        "p75": percentile(0.75),
        "p90": percentile(0.90),
        "p95": percentile(0.95),
        "n": n
    }))
}

/// Probability that the distribution is below a threshold
pub fn prob_below(
    dist: &Dist,
    threshold: f64,
    n: i32,
    seed: Option<i64>,
) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| sample_dist(dist, &mut *rng) < threshold)
        .count();

    Ok(count as f64 / n as f64)
}

/// Probability that the distribution is above a threshold
pub fn prob_above(
    dist: &Dist,
    threshold: f64,
    n: i32,
    seed: Option<i64>,
) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| sample_dist(dist, &mut *rng) > threshold)
        .count();

    Ok(count as f64 / n as f64)
}

/// Probability that the distribution is between two thresholds (inclusive)
pub fn prob_between(
    dist: &Dist,
    lower: f64,
    upper: f64,
    n: i32,
    seed: Option<i64>,
) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }
    if lower > upper {
        return Err(ProbError::new("lower must be <= upper"));
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| {
            let v = sample_dist(dist, &mut *rng);
            v >= lower && v <= upper
        })
        .count();

    Ok(count as f64 / n as f64)
}

/// Mean of a distribution via sampling (literals short-circuit)
pub fn mean(dist: &Dist, n: i32, seed: Option<i64>) -> f64 {
    // Optimization: if literal, return directly
    if let Some(v) = dist.as_literal() {
        return v;
    }

    let mut rng = make_rng(seed);

    let sum: f64 = (0..n).map(|_| sample_dist(dist, &mut *rng)).sum();
    sum / n as f64
}

/// A specific percentile of a distribution (p in [0, 1])
pub fn percentile(dist: &Dist, p: f64, n: i32, seed: Option<i64>) -> Result<f64, ProbError> {
    if !(0.0..=1.0).contains(&p) {
        return Err(ProbError::new("percentile must be between 0 and 1"));
    }

    // Optimization: if literal, return directly
    if let Some(v) = dist.as_literal() {
        return Ok(v);
    }

    let mut rng = make_rng(seed);

    let mut samples: Vec<f64> = (0..n).map(|_| sample_dist(dist, &mut *rng)).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let idx = (p * (samples.len() - 1) as f64).round() as usize;
    Ok(samples[idx.min(samples.len() - 1)])
}

/// Variance of a distribution via Monte Carlo sampling
pub fn variance(dist: &Dist, n: i32, seed: Option<i64>) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }
    if dist.as_literal().is_some() {
        return Ok(0.0);
    }

    let mut rng = make_rng(seed);
    let samples: Vec<f64> = (0..n).map(|_| sample_dist(dist, &mut *rng)).collect();
    let n_f = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n_f;
    Ok(samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f)
}

/// Standard deviation of a distribution via Monte Carlo sampling
pub fn stddev(dist: &Dist, n: i32, seed: Option<i64>) -> Result<f64, ProbError> {
    variance(dist, n, seed).map(|v| v.sqrt())
}

/// Covariance between two INDEPENDENTLY sampled distributions.
///
/// Always near-zero by construction — kept only for pg_prob's deprecated
/// SQL surface. Prefer `fit_correlation` on table data.
pub fn covariance(dist1: &Dist, dist2: &Dist, n: i32, seed: Option<i64>) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }

    let mut rng = make_rng(seed);
    let mut sum1 = 0.0_f64;
    let mut sum2 = 0.0_f64;
    let mut sum12 = 0.0_f64;
    let n_f = n as f64;

    for _ in 0..n {
        let v1 = sample_dist(dist1, &mut *rng);
        let v2 = sample_dist(dist2, &mut *rng);
        sum1 += v1;
        sum2 += v2;
        sum12 += v1 * v2;
    }

    Ok(sum12 / n_f - (sum1 / n_f) * (sum2 / n_f))
}

/// Pearson correlation between two INDEPENDENTLY sampled distributions.
///
/// Always near-zero by construction — kept only for pg_prob's deprecated
/// SQL surface. Prefer `fit_correlation` on table data.
pub fn correlation(
    dist1: &Dist,
    dist2: &Dist,
    n: i32,
    seed: Option<i64>,
) -> Result<f64, ProbError> {
    if n <= 0 {
        return Err(ProbError::new("n must be positive"));
    }

    let mut rng = make_rng(seed);
    let mut sum1 = 0.0_f64;
    let mut sum2 = 0.0_f64;
    let mut sum1_sq = 0.0_f64;
    let mut sum2_sq = 0.0_f64;
    let mut sum12 = 0.0_f64;
    let n_f = n as f64;

    for _ in 0..n {
        let v1 = sample_dist(dist1, &mut *rng);
        let v2 = sample_dist(dist2, &mut *rng);
        sum1 += v1;
        sum2 += v2;
        sum1_sq += v1 * v1;
        sum2_sq += v2 * v2;
        sum12 += v1 * v2;
    }

    let mean1 = sum1 / n_f;
    let mean2 = sum2 / n_f;
    let var1 = sum1_sq / n_f - mean1 * mean1;
    let var2 = sum2_sq / n_f - mean2 * mean2;
    let cov = sum12 / n_f - mean1 * mean2;

    let denom = (var1 * var2).sqrt();
    if denom == 0.0 {
        Ok(0.0)
    } else {
        Ok(cov / denom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constructors::{literal, normal, uniform};

    #[test]
    fn test_sample_dist_literal() {
        let d = literal(42.0);
        let mut rng = rand::thread_rng();
        assert_eq!(sample_dist(&d, &mut rng), 42.0);
    }

    #[test]
    fn test_sample_dist_normal_reproducible() {
        let d = normal(100.0, 10.0).unwrap();
        let mut rng1 = rand::rngs::StdRng::seed_from_u64(42);
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(42);

        let s1 = sample_dist(&d, &mut rng1);
        let s2 = sample_dist(&d, &mut rng2);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_seeded_sample_matches_seeded_rng() {
        let d = normal(100.0, 10.0).unwrap();
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        assert_eq!(sample(&d, Some(7)), sample_dist(&d, &mut rng));
    }

    #[test]
    fn test_sample_dist_uniform_range() {
        let d = uniform(10.0, 20.0).unwrap();
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let s = sample_dist(&d, &mut rng);
            assert!((10.0..20.0).contains(&s));
        }
    }

    #[test]
    fn test_samples_len_and_empty() {
        let d = normal(0.0, 1.0).unwrap();
        assert_eq!(samples(&d, 5, Some(1)).len(), 5);
        assert!(samples(&d, 0, Some(1)).is_empty());
        assert!(samples(&d, -3, Some(1)).is_empty());
    }

    #[test]
    fn test_summarize_normal() {
        let d = normal(100.0, 10.0).unwrap();
        let s = summarize(&d, 10000, Some(42)).unwrap();
        let mean = s["mean"].as_f64().unwrap();
        let std = s["std"].as_f64().unwrap();
        assert!((mean - 100.0).abs() < 1.0, "mean was {}", mean);
        assert!((std - 10.0).abs() < 1.0, "std was {}", std);
    }

    #[test]
    fn test_summarize_literal() {
        let s = summarize(&literal(42.0), 1000, None).unwrap();
        assert_eq!(s["mean"].as_f64().unwrap(), 42.0);
        assert_eq!(s["p50"].as_f64().unwrap(), 42.0);
    }

    #[test]
    fn test_summarize_rejects_nonpositive_n() {
        assert!(summarize(&literal(1.0), 0, None).is_err());
    }

    #[test]
    fn test_prob_below_above() {
        let d = normal(0.0, 1.0).unwrap();
        let below = prob_below(&d, 0.0, 20000, Some(42)).unwrap();
        assert!((below - 0.5).abs() < 0.02, "prob_below was {}", below);
        let above = prob_above(&d, 0.0, 20000, Some(42)).unwrap();
        assert!((above - 0.5).abs() < 0.02, "prob_above was {}", above);
    }

    #[test]
    fn test_prob_between_validation() {
        let d = literal(5.0);
        assert!(prob_between(&d, 10.0, 0.0, 100, None).is_err());
        assert_eq!(prob_between(&d, 0.0, 10.0, 100, None).unwrap(), 1.0);
    }

    #[test]
    fn test_mean_literal_short_circuit() {
        assert_eq!(mean(&literal(42.0), 10, None), 42.0);
    }

    #[test]
    fn test_percentile_validation_and_literal() {
        let d = literal(7.0);
        assert!(percentile(&d, 1.5, 100, None).is_err());
        assert_eq!(percentile(&d, 0.9, 100, None).unwrap(), 7.0);
    }

    #[test]
    fn test_variance_and_stddev_normal() {
        let d = normal(0.0, 10.0).unwrap();
        let var = variance(&d, 100000, Some(42)).unwrap();
        assert!((var - 100.0).abs() < 5.0, "variance was {}", var);
        let std = stddev(&d, 100000, Some(42)).unwrap();
        assert!((std - 10.0).abs() < 1.0, "stddev was {}", std);
    }

    #[test]
    fn test_variance_literal_is_zero() {
        assert_eq!(variance(&literal(42.0), 1000, None).unwrap(), 0.0);
    }

    #[test]
    fn test_correlation_independent_near_zero() {
        let d1 = normal(100.0, 10.0).unwrap();
        let d2 = normal(100.0, 10.0).unwrap();
        let cor = correlation(&d1, &d2, 50000, Some(42)).unwrap();
        assert!(cor.abs() < 0.05, "correlation was {}", cor);
    }

    #[test]
    fn test_covariance_literals_zero() {
        let c = covariance(&literal(5.0), &literal(10.0), 10000, None).unwrap();
        assert_eq!(c, 0.0);
    }

    #[test]
    fn test_expression_tree_sampling() {
        // normal(100, 10) + literal(50) → mean ~150
        let tree = crate::ops::dist_add(normal(100.0, 10.0).unwrap(), literal(50.0));
        let m = mean(&tree, 20000, Some(42));
        assert!((m - 150.0).abs() < 1.0, "mean was {}", m);
    }

    #[test]
    fn test_conditional_sampling() {
        let d = crate::constructors::if_above(literal(100.0), 50.0, literal(1.0), literal(0.0));
        assert_eq!(sample(&d, Some(1)), 1.0);
        let d = crate::constructors::if_below(literal(10.0), 50.0, literal(1.0), literal(0.0));
        assert_eq!(sample(&d, Some(1)), 1.0);
    }

    #[test]
    fn test_if_then_probabilistic() {
        let d = crate::constructors::if_then(0.5, literal(1.0), literal(0.0)).unwrap();
        let m = mean(&d, 10000, Some(42));
        assert!((m - 0.5).abs() < 0.05, "mean was {}", m);
    }
}
