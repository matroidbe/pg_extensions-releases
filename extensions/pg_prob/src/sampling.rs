//! Sampling and summarization functions
//!
//! This module provides the SQL surface for:
//! - Sampling from distributions (single value or batch)
//! - Summary statistics (mean, std, percentiles)
//! - Probability queries (prob_above, prob_below)
//!
//! All math delegates to the pure `prob_core` crate.

use crate::distribution::{ok_or_pg, Dist};
use pgrx::prelude::*;
use rand::RngCore;

// =============================================================================
// RNG / sampling shims (used by correlation.rs and simulate.rs)
// =============================================================================

pub(crate) use prob_core::sample::make_rng;

/// Sample one value from a wrapped distribution (crate-internal shim)
pub(crate) fn sample_dist(dist: &Dist, rng: &mut dyn RngCore) -> f64 {
    prob_core::sample::sample_dist(&dist.0, rng)
}

// =============================================================================
// Single Sample
// =============================================================================

/// Sample a single value from a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn sample(dist: Dist, seed: default!(Option<i64>, "NULL")) -> f64 {
    prob_core::sample::sample(&dist.0, seed)
}

// =============================================================================
// Batch Sampling
// =============================================================================

/// Sample multiple values from a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn samples(dist: Dist, n: i32, seed: default!(Option<i64>, "NULL")) -> Vec<f64> {
    prob_core::sample::samples(&dist.0, n, seed)
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
    pgrx::JsonB(ok_or_pg(prob_core::sample::summarize(&dist.0, n, seed)))
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
    ok_or_pg(prob_core::sample::prob_below(&dist.0, threshold, n, seed))
}

/// Probability that the distribution is above a threshold
#[pg_extern(immutable, parallel_safe)]
pub fn prob_above(
    dist: Dist,
    threshold: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    ok_or_pg(prob_core::sample::prob_above(&dist.0, threshold, n, seed))
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
    ok_or_pg(prob_core::sample::prob_between(
        &dist.0, lower, upper, n, seed,
    ))
}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Get the mean of a distribution via sampling
#[pg_extern(immutable, parallel_safe)]
pub fn mean(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    prob_core::sample::mean(&dist.0, n, seed)
}

/// Get a specific percentile of a distribution
#[pg_extern(immutable, parallel_safe)]
pub fn percentile(
    dist: Dist,
    p: f64,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    ok_or_pg(prob_core::sample::percentile(&dist.0, p, n, seed))
}

// =============================================================================
// Variance / StdDev / Covariance / Correlation
// =============================================================================

/// Compute the variance of a distribution via Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn variance(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    ok_or_pg(prob_core::sample::variance(&dist.0, n, seed))
}

/// Compute the standard deviation of a distribution via Monte Carlo sampling
#[pg_extern(immutable, parallel_safe)]
pub fn stddev(dist: Dist, n: default!(i32, 10000), seed: default!(Option<i64>, "NULL")) -> f64 {
    ok_or_pg(prob_core::sample::stddev(&dist.0, n, seed))
}

/// Compute the covariance between two distributions via paired Monte Carlo sampling.
///
/// DEPRECATED: This function samples each distribution independently, so it always
/// returns near-zero for any pair of distributions. Use `fit_correlation(x, y)` on
/// table data to discover real correlations, or use `correlated_pair()` /
/// `correlated_sample()` for correlated sampling.
#[pg_extern(immutable, parallel_safe)]
pub fn covariance(
    dist1: Dist,
    dist2: Dist,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    pgrx::warning!(
        "covariance(dist, dist) samples independently and always returns near-zero. \
         Use fit_correlation(x, y) on table data instead."
    );

    ok_or_pg(prob_core::sample::covariance(&dist1.0, &dist2.0, n, seed))
}

/// Compute the Pearson correlation between two distributions via paired Monte Carlo sampling.
///
/// DEPRECATED: This function samples each distribution independently, so it always
/// returns near-zero for any pair of distributions. Use `fit_correlation(x, y)` on
/// table data to discover real correlations, or use `correlated_pair()` /
/// `correlated_sample()` for correlated sampling.
#[pg_extern(immutable, parallel_safe)]
pub fn correlation(
    dist1: Dist,
    dist2: Dist,
    n: default!(i32, 10000),
    seed: default!(Option<i64>, "NULL"),
) -> f64 {
    pgrx::warning!(
        "correlation(dist, dist) samples independently and always returns near-zero. \
         Use fit_correlation(x, y) on table data instead."
    );

    ok_or_pg(prob_core::sample::correlation(&dist1.0, &dist2.0, n, seed))
}

// =============================================================================
// Unit Tests (run inside PostgreSQL via pgrx)
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use crate::distribution::{literal, normal, uniform};
    use rand::SeedableRng;

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
            assert!((10.0..20.0).contains(&s));
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
