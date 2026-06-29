//! Correlated simulation from table data
//!
//! The `simulate` function reads a source table, fits per-column distributions,
//! discovers pairwise correlations, and generates correlated samples using
//! the Gaussian copula.

use crate::correlation::{cholesky, norm_cdf};
use crate::distribution::{lognormal, normal, uniform, Dist};
use crate::sampling::{make_rng, sample_dist};
use pgrx::prelude::*;
use rand::prelude::*;
use rand_distr::StandardNormal;

// =============================================================================
// Internal statistics types
// =============================================================================

/// Running statistics for a single column
struct ColumnStats {
    count: i64,
    sum: f64,
    sum_sq: f64,
    sum_ln: f64,
    sum_ln_sq: f64,
    min: f64,
    max: f64,
}

impl ColumnStats {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            sum_ln: 0.0,
            sum_ln_sq: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn accumulate(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        self.sum_sq += v * v;
        if v > 0.0 {
            let ln_v = v.ln();
            self.sum_ln += ln_v;
            self.sum_ln_sq += ln_v * ln_v;
        }
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn stddev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let mu = self.sum / n;
        let var = (self.sum_sq / n - mu * mu).max(0.0);
        var.sqrt()
    }
}

/// Running statistics for a pair of columns (for Pearson r)
struct PairStats {
    count: i64,
    sum_x: f64,
    sum_y: f64,
    sum_x2: f64,
    sum_y2: f64,
    sum_xy: f64,
}

impl PairStats {
    fn new() -> Self {
        Self {
            count: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_x2: 0.0,
            sum_y2: 0.0,
            sum_xy: 0.0,
        }
    }

    fn accumulate(&mut self, x: f64, y: f64) {
        self.count += 1;
        self.sum_x += x;
        self.sum_y += y;
        self.sum_x2 += x * x;
        self.sum_y2 += y * y;
        self.sum_xy += x * y;
    }

    fn pearson_r(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let num = n * self.sum_xy - self.sum_x * self.sum_y;
        let dx = n * self.sum_x2 - self.sum_x * self.sum_x;
        let dy = n * self.sum_y2 - self.sum_y * self.sum_y;
        if dx <= 0.0 || dy <= 0.0 {
            return 0.0;
        }
        (num / (dx * dy).sqrt()).clamp(-1.0, 1.0)
    }
}

// =============================================================================
// Helper functions
// =============================================================================

/// Parse "schema.table" or "table" into (schema, table)
fn parse_table_name(name: &str) -> (String, String) {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => ("public".to_string(), parts[0].to_string()),
        2 => (parts[0].to_string(), parts[1].to_string()),
        _ => pgrx::error!(
            "invalid table name '{}': expected 'schema.table' or 'table'",
            name
        ),
    }
}

/// Escape single quotes for SQL string literals
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Validate that the table exists and all columns are numeric
fn validate_table_and_columns(schema: &str, table: &str, columns: &[String]) {
    let esc_schema = escape_literal(schema);
    let esc_table = escape_literal(table);

    // Check table exists
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_schema::text = '{}' AND table_name::text = '{}')",
        esc_schema, esc_table
    );
    let exists = Spi::get_one::<bool>(&sql)
        .unwrap_or_else(|e| pgrx::error!("failed to check table existence: {}", e))
        .unwrap_or(false);
    if !exists {
        pgrx::error!("table '{}.{}' does not exist", schema, table);
    }

    // Check each column exists and is numeric
    for col in columns {
        let sql = format!(
            "SELECT data_type::text FROM information_schema.columns \
             WHERE table_schema::text = '{}' AND table_name::text = '{}' \
             AND column_name::text = '{}'",
            esc_schema,
            esc_table,
            escape_literal(col)
        );
        let dt = Spi::get_one::<String>(&sql)
            .unwrap_or_else(|e| pgrx::error!("failed to check column '{}': {}", col, e));
        match dt {
            None => pgrx::error!("column '{}' not found in '{}.{}'", col, schema, table),
            Some(dt) => {
                let dt_lower = dt.to_lowercase();
                let is_numeric = dt_lower.contains("int")
                    || dt_lower.contains("float")
                    || dt_lower.contains("double")
                    || dt_lower.contains("numeric")
                    || dt_lower.contains("decimal")
                    || dt_lower.contains("real");
                if !is_numeric {
                    pgrx::error!("column '{}' has type '{}' which is not numeric", col, dt);
                }
            }
        }
    }
}

/// Read all data from the table and compute column stats + pairwise stats in one pass
fn read_data_and_compute_stats(
    schema: &str,
    table: &str,
    columns: &[String],
) -> (Vec<ColumnStats>, Vec<Vec<PairStats>>) {
    let k = columns.len();

    // Build SELECT: cast all columns to float8
    let col_exprs: Vec<String> = columns
        .iter()
        .map(|c| format!("\"{}\"::float8", c))
        .collect();
    let sql = format!(
        "SELECT {} FROM \"{}\".\"{}\"",
        col_exprs.join(", "),
        schema,
        table
    );

    let mut col_stats: Vec<ColumnStats> = (0..k).map(|_| ColumnStats::new()).collect();
    let mut pair_stats: Vec<Vec<PairStats>> = (0..k)
        .map(|_| (0..k).map(|_| PairStats::new()).collect())
        .collect();

    Spi::connect(|client| {
        let result = client
            .select(&sql, None, &[])
            .unwrap_or_else(|e| pgrx::error!("failed to read source table: {}", e));

        for row in result {
            // Read all column values
            let vals: Vec<Option<f64>> = (0..k)
                .map(|j| row.get::<f64>(j + 1).ok().flatten())
                .collect();

            // Listwise deletion: skip entire row if any column is NULL or non-finite
            let vs: Vec<f64> = match vals
                .iter()
                .map(|v| match v {
                    Some(x) if x.is_finite() => Some(*x),
                    _ => None,
                })
                .collect::<Option<Vec<f64>>>()
            {
                Some(vs) => vs,
                None => continue,
            };

            // Accumulate column stats
            for (j, &v) in vs.iter().enumerate() {
                col_stats[j].accumulate(v);
            }

            // Accumulate pairwise stats (upper triangle)
            for i in 0..k {
                for j in (i + 1)..k {
                    pair_stats[i][j].accumulate(vs[i], vs[j]);
                }
            }
        }
    });

    (col_stats, pair_stats)
}

/// Fit a distribution to column statistics based on requested type
fn fit_column(cs: &ColumnStats, dist_type: &str) -> Dist {
    match dist_type {
        "normal" => {
            let mu = cs.mean();
            let sigma = cs.stddev().max(0.0001);
            normal(mu, sigma)
        }
        "uniform" => {
            if cs.min == cs.max {
                uniform(cs.min, cs.max + 0.0001)
            } else if cs.count <= 2 {
                uniform(cs.min, cs.max)
            } else {
                let range = cs.max - cs.min;
                let padding = range / (cs.count as f64 + 1.0);
                uniform(cs.min - padding, cs.max + padding)
            }
        }
        "lognormal" => {
            if cs.count == 0 {
                return lognormal(0.0, 1.0);
            }
            let n = cs.count as f64;
            let mu_ln = cs.sum_ln / n;
            let var_ln = (cs.sum_ln_sq / n - mu_ln * mu_ln).max(0.0);
            let sigma_ln = var_ln.sqrt().max(0.0001);
            lognormal(mu_ln, sigma_ln)
        }
        _ => pgrx::error!("unsupported dist_type: {}", dist_type),
    }
}

/// Build the full symmetric k x k correlation matrix from pairwise stats
fn build_correlation_matrix(pair_stats: &[Vec<PairStats>], k: usize) -> Vec<f64> {
    let mut matrix = vec![0.0; k * k];
    for i in 0..k {
        matrix[i * k + i] = 1.0; // diagonal
        for j in (i + 1)..k {
            let r = pair_stats[i][j].pearson_r();
            matrix[i * k + j] = r;
            matrix[j * k + i] = r; // symmetric
        }
    }
    matrix
}

/// Ensure the correlation matrix is positive definite by adding a small
/// ridge to the diagonal if Cholesky fails
fn ensure_positive_definite(mut matrix: Vec<f64>, k: usize) -> Vec<f64> {
    if cholesky(&matrix, k).is_ok() {
        return matrix;
    }

    // Save off-diagonal values
    let original = matrix.clone();

    for attempt in 1..=10 {
        let ridge = 0.001 * (attempt as f64);
        for i in 0..k {
            matrix[i * k + i] = 1.0 + ridge;
            for j in 0..k {
                if i != j {
                    matrix[i * k + j] = original[i * k + j] * (1.0 - ridge);
                }
            }
        }
        if cholesky(&matrix, k).is_ok() {
            return matrix;
        }
    }

    // Fallback: identity (no correlations)
    pgrx::warning!(
        "correlation matrix not positive definite after regularization; \
         falling back to identity (no correlations)"
    );
    let mut identity = vec![0.0; k * k];
    for i in 0..k {
        identity[i * k + i] = 1.0;
    }
    identity
}

// =============================================================================
// Main simulate function
// =============================================================================

/// Read a source table, fit distributions per column, discover pairwise
/// correlations, and generate correlated samples.
///
/// - source_table: name of the table to read (schema.table or just table)
/// - columns: column names to include (must be numeric)
/// - n: number of samples to generate (default 1000)
/// - seed: optional RNG seed for reproducibility
/// - dist_type: distribution type to fit: 'normal' (default), 'uniform', 'lognormal'
///
/// Returns table of (sample_idx, values jsonb) where values contains column names as keys.
#[pg_extern]
pub fn simulate(
    source_table: &str,
    columns: Vec<String>,
    n: default!(i32, 1000),
    seed: default!(Option<i64>, "NULL"),
    dist_type: default!(&str, "'normal'"),
) -> TableIterator<'static, (name!(sample_idx, i32), name!(values, pgrx::JsonB))> {
    if columns.is_empty() {
        pgrx::error!("columns array must not be empty");
    }
    if n <= 0 {
        pgrx::error!("n must be positive");
    }
    let valid_types = ["normal", "uniform", "lognormal"];
    if !valid_types.contains(&dist_type) {
        pgrx::error!("dist_type must be one of: {}", valid_types.join(", "));
    }

    let k = columns.len();
    let (schema, table_name) = parse_table_name(source_table);

    // Step 1: Validate table and columns
    validate_table_and_columns(&schema, &table_name, &columns);

    // Step 2: Read data and compute statistics in a single pass
    let (col_stats, pair_stats) = read_data_and_compute_stats(&schema, &table_name, &columns);

    // Step 3: Check we have enough data
    for (i, cs) in col_stats.iter().enumerate() {
        if cs.count < 2 {
            pgrx::error!("column '{}' has fewer than 2 finite values", columns[i]);
        }
    }

    // Step 4: Fit distributions per column
    let dists: Vec<Dist> = col_stats
        .iter()
        .map(|cs| fit_column(cs, dist_type))
        .collect();

    // Step 5: Build and regularize correlation matrix
    let corr_matrix = build_correlation_matrix(&pair_stats, k);
    let corr_matrix = ensure_positive_definite(corr_matrix, k);

    // Step 6: Cholesky decomposition
    let chol = cholesky(&corr_matrix, k)
        .unwrap_or_else(|e| pgrx::error!("Cholesky decomposition failed: {}", e));

    // Step 7: Generate correlated samples
    let n = n as usize;
    let mut rng = make_rng(seed);

    // Pre-generate sorted samples from each distribution
    let mut sorted_samples: Vec<Vec<f64>> = Vec::with_capacity(k);
    for dist in &dists {
        let mut s: Vec<f64> = (0..n).map(|_| sample_dist(dist, &mut *rng)).collect();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted_samples.push(s);
    }

    // Generate correlated normals, map to samples via Gaussian copula
    let mut results: Vec<(i32, pgrx::JsonB)> = Vec::with_capacity(n);
    for i in 0..n {
        let z: Vec<f64> = (0..k).map(|_| rng.sample(StandardNormal)).collect();

        // w = L * z (Cholesky factor)
        let mut w = vec![0.0; k];
        for row in 0..k {
            let mut sum = 0.0;
            for col in 0..=row {
                sum += chol[row * k + col] * z[col];
            }
            w[row] = sum;
        }

        // Map to sorted samples via quantile matching
        let mut map = serde_json::Map::new();
        for j in 0..k {
            let u = norm_cdf(w[j]);
            let idx = ((u * n as f64) as usize).min(n - 1);
            let val = sorted_samples[j][idx];
            map.insert(columns[j].clone(), serde_json::Value::from(val));
        }

        results.push((i as i32, pgrx::JsonB(serde_json::Value::Object(map))));
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
    fn test_simulate_basic() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_src (a float8, b float8)")?;
        Spi::run(
            "INSERT INTO sim_src SELECT v::float8, (v * 2 + 5)::float8 \
             FROM generate_series(1, 200) AS v",
        )?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_src', ARRAY['a', 'b'], 100, 42)",
        )?
        .unwrap();
        assert_eq!(count, 100);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_returns_json_with_columns() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_json (x float8, y float8)")?;
        Spi::run("INSERT INTO sim_json SELECT v, v * 2 FROM generate_series(1, 100) AS v")?;
        let val = Spi::get_one::<pgrx::JsonB>(
            "SELECT values FROM pgprob.simulate('sim_json', ARRAY['x', 'y'], 1, 42) LIMIT 1",
        )?
        .unwrap();
        let obj = val.0.as_object().unwrap();
        assert!(obj.contains_key("x"), "missing 'x' key");
        assert!(obj.contains_key("y"), "missing 'y' key");
        Ok(())
    }

    #[pg_test]
    fn test_simulate_preserves_correlation() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_corr (a float8, b float8)")?;
        Spi::run(
            "INSERT INTO sim_corr SELECT v::float8, (v * 2 + 10)::float8 \
             FROM generate_series(1, 500) AS v",
        )?;
        let r = Spi::get_one::<f64>(
            "WITH sim AS (
                SELECT (values->>'a')::float8 AS a, (values->>'b')::float8 AS b
                FROM pgprob.simulate('sim_corr', ARRAY['a', 'b'], 5000, 42)
            )
            SELECT corr(a, b) FROM sim",
        )?
        .unwrap();
        assert!(r > 0.7, "expected strong positive correlation, got {}", r);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_uniform_dist_type() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_uni (x float8)")?;
        Spi::run("INSERT INTO sim_uni SELECT v::float8 FROM generate_series(1, 200) AS v")?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_uni', ARRAY['x'], 50, 42, 'uniform')",
        )?
        .unwrap();
        assert_eq!(count, 50);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_lognormal_dist_type() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_ln (x float8)")?;
        Spi::run(
            "INSERT INTO sim_ln SELECT exp(v * 0.1)::float8 FROM generate_series(1, 200) AS v",
        )?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_ln', ARRAY['x'], 50, 42, 'lognormal')",
        )?
        .unwrap();
        assert_eq!(count, 50);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_single_column() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_one (v float8)")?;
        Spi::run("INSERT INTO sim_one SELECT v::float8 FROM generate_series(1, 200) AS v")?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_one', ARRAY['v'], 100, 42)",
        )?
        .unwrap();
        assert_eq!(count, 100);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_schema_qualified() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE public.sim_qual (x float8)")?;
        Spi::run("INSERT INTO public.sim_qual SELECT v FROM generate_series(1, 100) AS v")?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('public.sim_qual', ARRAY['x'], 10, 42)",
        )?
        .unwrap();
        assert_eq!(count, 10);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_three_columns() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_three (a float8, b float8, c float8)")?;
        Spi::run(
            "INSERT INTO sim_three SELECT v, v * 2, v * 0.5 FROM generate_series(1, 200) AS v",
        )?;
        let val = Spi::get_one::<pgrx::JsonB>(
            "SELECT values FROM pgprob.simulate('sim_three', ARRAY['a', 'b', 'c'], 1, 42) LIMIT 1",
        )?
        .unwrap();
        let obj = val.0.as_object().unwrap();
        assert_eq!(obj.len(), 3, "expected 3 columns in JSON");
        Ok(())
    }

    #[pg_test]
    fn test_simulate_negative_correlation() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_neg (a float8, b float8)")?;
        Spi::run(
            "INSERT INTO sim_neg SELECT v::float8, (500 - v)::float8 \
             FROM generate_series(1, 500) AS v",
        )?;
        let r = Spi::get_one::<f64>(
            "WITH sim AS (
                SELECT (values->>'a')::float8 AS a, (values->>'b')::float8 AS b
                FROM pgprob.simulate('sim_neg', ARRAY['a', 'b'], 5000, 42)
            )
            SELECT corr(a, b) FROM sim",
        )?
        .unwrap();
        assert!(r < -0.7, "expected strong negative correlation, got {}", r);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_skips_null_rows() -> Result<(), spi::Error> {
        Spi::run("CREATE TABLE sim_nulls (a float8, b float8)")?;
        Spi::run(
            "INSERT INTO sim_nulls SELECT v::float8, (v * 2)::float8 \
             FROM generate_series(1, 100) AS v",
        )?;
        // Add some NULLs — these should be skipped, not cause errors
        Spi::run("INSERT INTO sim_nulls VALUES (NULL, 5), (10, NULL), (NULL, NULL)")?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_nulls', ARRAY['a', 'b'], 50, 42)",
        )?
        .unwrap();
        assert_eq!(count, 50);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_integer_columns() -> Result<(), spi::Error> {
        // Integer columns should work via ::float8 cast
        Spi::run("CREATE TABLE sim_int (a int4, b int4)")?;
        Spi::run("INSERT INTO sim_int SELECT v, v * 3 FROM generate_series(1, 200) AS v")?;
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgprob.simulate('sim_int', ARRAY['a', 'b'], 50, 42)",
        )?
        .unwrap();
        assert_eq!(count, 50);
        Ok(())
    }

    #[pg_test]
    fn test_simulate_values_in_reasonable_range() -> Result<(), spi::Error> {
        // Simulated values should be in the general range of the source data
        Spi::run("CREATE TABLE sim_range (x float8)")?;
        Spi::run("INSERT INTO sim_range SELECT v FROM generate_series(100, 200) AS v")?;
        let avg = Spi::get_one::<f64>(
            "WITH sim AS (
                SELECT (values->>'x')::float8 AS x
                FROM pgprob.simulate('sim_range', ARRAY['x'], 1000, 42)
            )
            SELECT avg(x) FROM sim",
        )?
        .unwrap();
        // Average should be near 150 (center of 100-200)
        assert!(
            (avg - 150.0).abs() < 20.0,
            "expected avg near 150, got {}",
            avg
        );
        Ok(())
    }
}
