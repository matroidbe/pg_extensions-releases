//! Correlated distribution sampling
//!
//! Uses Gaussian copula to generate correlated samples from arbitrary distributions:
//! 1. Generate correlated standard normals (Cholesky decomposition)
//! 2. Convert to uniform via Phi (normal CDF)
//! 3. Map to target distributions via quantile matching

use crate::distribution::Dist;
use crate::sampling::{make_rng, sample_dist};
use pgrx::prelude::*;
use rand::prelude::*;
use rand_distr::StandardNormal;

// =============================================================================
// Math helpers (no external dependencies)
// =============================================================================

/// Error function approximation (Abramowitz & Stegun 7.1.26, max error 1.5e-7)
fn erf_approx(x: f64) -> f64 {
    let a = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = t
        * (0.254829592
            + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
    let result = 1.0 - poly * (-a * a).exp();
    if x >= 0.0 {
        result
    } else {
        -result
    }
}

/// Standard normal CDF: Phi(x) = P(Z <= x)
fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf_approx(x / std::f64::consts::SQRT_2))
}

/// Cholesky decomposition of a symmetric positive-definite matrix.
/// Input: flat row-major NxN matrix. Output: flat row-major lower-triangular L.
fn cholesky(matrix: &[f64], n: usize) -> Result<Vec<f64>, &'static str> {
    let mut l = vec![0.0; n * n];
    for i in 0..n {
        for j in 0..=i {
            let mut sum = 0.0;
            for k in 0..j {
                sum += l[i * n + k] * l[j * n + k];
            }
            if i == j {
                let diag = matrix[i * n + j] - sum;
                if diag <= 0.0 {
                    return Err("matrix is not positive definite");
                }
                l[i * n + j] = diag.sqrt();
            } else {
                l[i * n + j] = (matrix[i * n + j] - sum) / l[j * n + j];
            }
        }
    }
    Ok(l)
}

// =============================================================================
// correlated_pair — simplified 2-distribution version
// =============================================================================

/// Generate n correlated sample pairs from two distributions using a Gaussian copula.
///
/// Returns a table of (val1, val2) rows where the rank correlation approximates rho.
#[pg_extern(immutable, parallel_safe)]
pub fn correlated_pair(
    dist1: Dist,
    dist2: Dist,
    rho: f64,
    n: default!(i32, 1000),
    seed: default!(Option<i64>, "NULL"),
) -> TableIterator<'static, (name!(val1, f64), name!(val2, f64))> {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    if !(-1.0..=1.0).contains(&rho) {
        pgrx::error!("correlation coefficient rho must be between -1 and 1");
    }

    let n = n as usize;
    let mut rng = make_rng(seed);

    // Step 1: Generate n independent samples from each distribution and sort them
    let mut samples1: Vec<f64> = (0..n).map(|_| sample_dist(&dist1, &mut *rng)).collect();
    let mut samples2: Vec<f64> = (0..n).map(|_| sample_dist(&dist2, &mut *rng)).collect();
    samples1.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    samples2.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Step 2: Generate correlated standard normals using 2x2 Cholesky
    // L = [[1, 0], [rho, sqrt(1-rho^2)]]
    let rho_comp = (1.0 - rho * rho).max(0.0).sqrt();
    let mut corr_uniforms: Vec<(f64, f64)> = Vec::with_capacity(n);
    for _ in 0..n {
        let z1: f64 = rng.sample(StandardNormal);
        let z2: f64 = rng.sample(StandardNormal);
        let w1 = z1;
        let w2 = rho * z1 + rho_comp * z2;
        // Convert to uniform via Phi
        let u1 = norm_cdf(w1);
        let u2 = norm_cdf(w2);
        corr_uniforms.push((u1, u2));
    }

    // Step 3: Map uniforms to sorted samples via quantile matching
    let results: Vec<(f64, f64)> = corr_uniforms
        .iter()
        .map(|(u1, u2)| {
            let idx1 = ((u1 * n as f64) as usize).min(n - 1);
            let idx2 = ((u2 * n as f64) as usize).min(n - 1);
            (samples1[idx1], samples2[idx2])
        })
        .collect();

    TableIterator::new(results)
}

// =============================================================================
// correlated_sample — general N-distribution version
// =============================================================================

/// Generate n correlated sample vectors from multiple distributions using a Gaussian copula.
///
/// - dists: array of distributions
/// - correlations: flat NxN correlation matrix (row-major)
/// - Returns table of (sample_idx, values[]) rows
#[pg_extern(immutable, parallel_safe)]
pub fn correlated_sample(
    dists: Vec<Dist>,
    correlations: Vec<f64>,
    n: default!(i32, 1000),
    seed: default!(Option<i64>, "NULL"),
) -> TableIterator<'static, (name!(sample_idx, i32), name!(values, Vec<f64>))> {
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    let k = dists.len(); // number of distributions
    if k == 0 {
        pgrx::error!("dists array must not be empty");
    }
    if correlations.len() != k * k {
        pgrx::error!(
            "correlations must be a flat {}x{} matrix ({} elements), got {}",
            k,
            k,
            k * k,
            correlations.len()
        );
    }

    let n = n as usize;
    let mut rng = make_rng(seed);

    // Step 1: Cholesky decomposition of correlation matrix
    let chol = cholesky(&correlations, k)
        .unwrap_or_else(|e| pgrx::error!("Cholesky decomposition failed: {}", e));

    // Step 2: Generate independent samples from each distribution, sort them
    let mut sorted_samples: Vec<Vec<f64>> = Vec::with_capacity(k);
    for dist in &dists {
        let mut s: Vec<f64> = (0..n).map(|_| sample_dist(dist, &mut *rng)).collect();
        s.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted_samples.push(s);
    }

    // Step 3: Generate correlated standard normals, convert to uniforms
    let mut results: Vec<(i32, Vec<f64>)> = Vec::with_capacity(n);
    for i in 0..n {
        // Generate k independent standard normals
        let z: Vec<f64> = (0..k).map(|_| rng.sample(StandardNormal)).collect();

        // Multiply by Cholesky factor: w = L * z
        let mut w = vec![0.0; k];
        for row in 0..k {
            let mut sum = 0.0;
            for col in 0..=row {
                sum += chol[row * k + col] * z[col];
            }
            w[row] = sum;
        }

        // Convert to uniforms and map to sorted samples
        let values: Vec<f64> = (0..k)
            .map(|j| {
                let u = norm_cdf(w[j]);
                let idx = ((u * n as f64) as usize).min(n - 1);
                sorted_samples[j][idx]
            })
            .collect();

        results.push((i as i32, values));
    }

    TableIterator::new(results)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_correlated_pair_returns_rows() -> Result<(), spi::Error> {
        let result = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.correlated_pair(
                pgprob.normal(0, 1), pgprob.normal(0, 1), 0.8, 100, 42
            )",
        );
        assert_eq!(result.unwrap().unwrap(), 100);
        Ok(())
    }

    #[pg_test]
    fn test_correlated_pair_zero_correlation() -> Result<(), spi::Error> {
        // With rho=0, correlation should be low
        let result = Spi::get_one::<f64>(
            "WITH samples AS (
                SELECT val1, val2 FROM pgprob.correlated_pair(
                    pgprob.normal(0, 1), pgprob.normal(0, 1), 0.0, 5000, 42
                )
            )
            SELECT corr(val1, val2) FROM samples",
        );
        let cor = result.unwrap().unwrap();
        assert!(
            cor.abs() < 0.1,
            "expected near-zero correlation, got {}",
            cor
        );
        Ok(())
    }

    #[pg_test]
    fn test_correlated_pair_high_correlation() -> Result<(), spi::Error> {
        let result = Spi::get_one::<f64>(
            "WITH samples AS (
                SELECT val1, val2 FROM pgprob.correlated_pair(
                    pgprob.normal(100, 10), pgprob.normal(100, 10), 0.9, 5000, 42
                )
            )
            SELECT corr(val1, val2) FROM samples",
        );
        let cor = result.unwrap().unwrap();
        assert!(cor > 0.8, "expected high correlation, got {}", cor);
        Ok(())
    }

    #[pg_test]
    fn test_correlated_sample_returns_rows() -> Result<(), spi::Error> {
        let result = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.correlated_sample(
                ARRAY[pgprob.normal(0,1), pgprob.normal(0,1)],
                ARRAY[1.0, 0.5, 0.5, 1.0],
                100, 42
            )",
        );
        assert_eq!(result.unwrap().unwrap(), 100);
        Ok(())
    }

    #[pg_test]
    fn test_erf_approx_accuracy() {
        // erf(0) = 0, erf(inf) → 1
        let e0 = super::erf_approx(0.0);
        assert!(e0.abs() < 1e-6, "erf(0) = {}", e0);

        let e_large = super::erf_approx(5.0);
        assert!((e_large - 1.0).abs() < 1e-6, "erf(5) = {}", e_large);

        // erf(1) ≈ 0.8427
        let e1 = super::erf_approx(1.0);
        assert!((e1 - 0.8427).abs() < 0.001, "erf(1) = {}", e1);
    }

    #[pg_test]
    fn test_norm_cdf() {
        // Phi(0) = 0.5
        let p0 = super::norm_cdf(0.0);
        assert!((p0 - 0.5).abs() < 1e-6, "Phi(0) = {}", p0);

        // Phi(-inf) → 0, Phi(inf) → 1
        let p_neg = super::norm_cdf(-10.0);
        assert!(p_neg < 1e-6, "Phi(-10) = {}", p_neg);
        let p_pos = super::norm_cdf(10.0);
        assert!((p_pos - 1.0).abs() < 1e-6, "Phi(10) = {}", p_pos);
    }

    #[pg_test]
    fn test_cholesky_2x2() {
        // [[1, 0.5], [0.5, 1]]
        let l = super::cholesky(&[1.0, 0.5, 0.5, 1.0], 2).unwrap();
        assert!((l[0] - 1.0).abs() < 1e-10); // L[0,0] = 1
        assert!((l[2] - 0.5).abs() < 1e-10); // L[1,0] = 0.5
        assert!((l[3] - (0.75_f64).sqrt()).abs() < 1e-10); // L[1,1] = sqrt(0.75)
    }
}
