//! Sampling and summarization functions
//!
//! This module provides functions to:
//! - Sample from distributions (single value or batch)
//! - Compute summary statistics (mean, std, percentiles)
//! - Probability queries (prob_above, prob_below)

use crate::distribution::{Dist, DistParams, DistType};
use pgrx::prelude::*;
use rand::prelude::*;
use rand::Rng;
use rand::SeedableRng;
use rand_distr::{Beta, Exp, LogNormal, Normal, Poisson, Triangular, Uniform};
use serde_json::json;

// =============================================================================
// RNG Helper
// =============================================================================

/// Create an RNG from an optional seed (DRY helper)
pub(crate) fn make_rng(seed: Option<i64>) -> Box<dyn RngCore> {
    match seed {
        Some(s) => Box::new(rand::rngs::StdRng::seed_from_u64(s as u64)),
        None => Box::new(rand::thread_rng()),
    }
}

// =============================================================================
// Single Sample
// =============================================================================

/// Sample a single value from a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn sample(dist: Dist, seed: default!(Option<i64>, "NULL")) -> f64 {
    let mut rng = make_rng(seed);
    sample_dist(&dist, &mut *rng)
}

/// Internal recursive sampling function
pub(crate) fn sample_dist(dist: &Dist, rng: &mut dyn RngCore) -> f64 {
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

        // Fallback
        _ => {
            pgrx::warning!("unsupported distribution type for sampling");
            f64::NAN
        }
    }
}

// =============================================================================
// Batch Sampling
// =============================================================================

/// Sample multiple values from a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn samples(dist: Dist, n: i32, seed: default!(Option<i64>, "NULL")) -> Vec<f64> {
    if n <= 0 {
        return vec![];
    }

    let mut rng = make_rng(seed);
    (0..n).map(|_| sample_dist(&dist, &mut *rng)).collect()
}

// =============================================================================
// Summary Statistics
// =============================================================================

/// Compute summary statistics for a distribution via Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn summarize(
    dist: Dist,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> pgrx::JsonB {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }

    let mut rng = make_rng(seed);

    // Generate samples
    let mut samples: Vec<f64> = (0..n).map(|_| sample_dist(&dist, &mut *rng)).collect();

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

    pgrx::JsonB(json!({
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

// =============================================================================
// Probability Queries
// =============================================================================

/// Probability that the distribution is below a threshold
#[pg_extern(immutable, parallel_safe)]
pub fn prob_below(
    dist: Dist,
    threshold: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| sample_dist(&dist, &mut *rng) < threshold)
        .count();

    count as f64 / n as f64
}

/// Probability that the distribution is above a threshold
#[pg_extern(immutable, parallel_safe)]
pub fn prob_above(
    dist: Dist,
    threshold: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| sample_dist(&dist, &mut *rng) > threshold)
        .count();

    count as f64 / n as f64
}

/// Probability that the distribution is between two thresholds
#[pg_extern(immutable, parallel_safe)]
pub fn prob_between(
    dist: Dist,
    lower: f64,
    upper: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    if lower > upper {
        pgrx::error!("lower must be <= upper");
    }

    let mut rng = make_rng(seed);

    let count = (0..n)
        .filter(|_| {
            let v = sample_dist(&dist, &mut *rng);
            v >= lower && v <= upper
        })
        .count();

    count as f64 / n as f64
}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Get the mean of a distribution via sampling
#[pg_extern(immutable, parallel_safe)]
pub fn mean(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    // Optimization: if literal, return directly
    if let Some(v) = dist.as_literal() {
        return v;
    }

    let mut rng = make_rng(seed);

    let sum: f64 = (0..n).map(|_| sample_dist(&dist, &mut *rng)).sum();
    sum / n as f64
}

/// Get a specific percentile of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn percentile(
    dist: Dist,
    p: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if !(0.0..=1.0).contains(&p) {
        pgrx::error!("percentile must be between 0 and 1");
    }

    // Optimization: if literal, return directly
    if let Some(v) = dist.as_literal() {
        return v;
    }

    let mut rng = make_rng(seed);

    let mut samples: Vec<f64> = (0..n).map(|_| sample_dist(&dist, &mut *rng)).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let idx = (p * (samples.len() - 1) as f64).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

// =============================================================================
// Variance / StdDev / Covariance / Correlation
// =============================================================================

/// Compute the variance of a distribution via Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn variance(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    if let Some(_v) = dist.as_literal() {
        return 0.0;
    }

    let mut rng = make_rng(seed);
    let samples: Vec<f64> = (0..n).map(|_| sample_dist(&dist, &mut *rng)).collect();
    let n_f = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n_f;
    samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f
}

/// Compute the standard deviation of a distribution via Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn stddev(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    if dist.as_literal().is_some() {
        return 0.0;
    }

    let mut rng = make_rng(seed);
    let samples: Vec<f64> = (0..n).map(|_| sample_dist(&dist, &mut *rng)).collect();
    let n_f = samples.len() as f64;
    let mean = samples.iter().sum::<f64>() / n_f;
    let var = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n_f;
    var.sqrt()
}

/// Compute the covariance between two distributions via paired Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn covariance(
    dist1: Dist,
    dist2: Dist,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }

    let mut rng = make_rng(seed);
    let mut sum1 = 0.0_f64;
    let mut sum2 = 0.0_f64;
    let mut sum12 = 0.0_f64;
    let n_f = n as f64;

    for _ in 0..n {
        let v1 = sample_dist(&dist1, &mut *rng);
        let v2 = sample_dist(&dist2, &mut *rng);
        sum1 += v1;
        sum2 += v2;
        sum12 += v1 * v2;
    }

    sum12 / n_f - (sum1 / n_f) * (sum2 / n_f)
}

/// Compute the Pearson correlation between two distributions via paired Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn correlation(
    dist1: Dist,
    dist2: Dist,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }

    let mut rng = make_rng(seed);
    let mut sum1 = 0.0_f64;
    let mut sum2 = 0.0_f64;
    let mut sum1_sq = 0.0_f64;
    let mut sum2_sq = 0.0_f64;
    let mut sum12 = 0.0_f64;
    let n_f = n as f64;

    for _ in 0..n {
        let v1 = sample_dist(&dist1, &mut *rng);
        let v2 = sample_dist(&dist2, &mut *rng);
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
        0.0
    } else {
        cov / denom
    }
}

// =============================================================================
// Unit Tests (run inside PostgreSQL via pgrx)
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::distribution::{literal, normal, uniform};

    #[pg_test]
    fn test_sample_dist_literal() {
        let d = literal(42.0);
        let mut rng = rand::thread_rng();
        assert_eq!(sample_dist(&d, &mut rng), 42.0);
    }

    #[pg_test]
    fn test_sample_dist_normal_reproducible() {
        let d = normal(100.0, 10.0);
        let mut rng1 = rand::rngs::StdRng::seed_from_u64(42);
        let mut rng2 = rand::rngs::StdRng::seed_from_u64(42);

        let s1 = sample_dist(&d, &mut rng1);
        let s2 = sample_dist(&d, &mut rng2);
        assert_eq!(s1, s2);
    }

    #[pg_test]
    fn test_sample_dist_uniform_range() {
        let d = uniform(10.0, 20.0);
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let s = sample_dist(&d, &mut rng);
            assert!(s >= 10.0 && s < 20.0);
        }
    }

    #[pg_test]
    fn test_variance_literal_is_zero() {
        let result = Spi::get_one::<f64>("SELECT pgprob.variance(pgprob.literal(42.0))");
        assert_eq!(result.unwrap().unwrap(), 0.0);
    }

    #[pg_test]
    fn test_variance_normal() {
        let result =
            Spi::get_one::<f64>("SELECT pgprob.variance(pgprob.normal(0, 10.0), 100000, 42)");
        let var = result.unwrap().unwrap();
        assert!((var - 100.0).abs() < 5.0, "variance was {}", var);
    }

    #[pg_test]
    fn test_stddev_normal() {
        let result =
            Spi::get_one::<f64>("SELECT pgprob.stddev(pgprob.normal(0, 10.0), 100000, 42)");
        let std = result.unwrap().unwrap();
        assert!((std - 10.0).abs() < 1.0, "stddev was {}", std);
    }

    #[pg_test]
    fn test_correlation_independent() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.correlation(pgprob.normal(100, 10), pgprob.normal(100, 10), 50000, 42)",
        );
        let cor = result.unwrap().unwrap();
        // Independent distributions → near-zero correlation
        assert!(cor.abs() < 0.05, "correlation was {}", cor);
    }

    #[pg_test]
    fn test_covariance_literal_is_zero() {
        let result = Spi::get_one::<f64>(
            "SELECT pgprob.covariance(pgprob.literal(5.0), pgprob.literal(10.0))",
        );
        assert_eq!(result.unwrap().unwrap(), 0.0);
    }
}
