# pg_prob - Probabilistic Data Types

Probabilistic data types for Monte Carlo simulation in PostgreSQL. The `dist` type represents uncertain values that propagate uncertainty through arithmetic operations.

## Schema

`pgprob`

## Custom Types

- `dist` - Distribution type supporting literals, parametric distributions, and computed distributions.
- `DistAvgState` - Internal aggregate state for AVG(dist).
- `FitState` - Internal aggregate state for distribution fitting.

## SQL API

### Distribution Constructors

#### literal

Create a literal (certain) distribution from a numeric value.

```sql
SELECT pgprob.literal(42.0);
SELECT 42.0::dist;  -- Implicit cast
```

**Parameters:**
- `value FLOAT8|FLOAT4|INTEGER|NUMERIC` - The certain value

**Returns:** `dist`

#### normal

Create a normal (Gaussian) distribution N(mu, sigma).

```sql
SELECT pgprob.normal(100, 15);  -- IQ distribution
```

**Parameters:**
- `mu FLOAT8` - Mean
- `sigma FLOAT8` - Standard deviation (must be >= 0)

**Returns:** `dist`

#### uniform

Create a uniform distribution U(min, max).

```sql
SELECT pgprob.uniform(0, 100);  -- Random 0-100
```

**Parameters:**
- `min FLOAT8` - Minimum value
- `max FLOAT8` - Maximum value (must be >= min)

**Returns:** `dist`

#### triangular

Create a triangular distribution Tri(min, mode, max).

```sql
SELECT pgprob.triangular(10, 20, 40);  -- Most likely 20
```

**Parameters:**
- `min FLOAT8` - Minimum value
- `mode FLOAT8` - Most likely value
- `max FLOAT8` - Maximum value

**Returns:** `dist`

#### beta

Create a beta distribution scaled to [min, max].

```sql
SELECT pgprob.beta(2, 5, 0, 100);  -- Skewed right
```

**Parameters:**
- `alpha FLOAT8` - Alpha parameter (must be > 0)
- `beta_param FLOAT8` - Beta parameter (must be > 0)
- `min FLOAT8 DEFAULT 0.0` - Scale minimum
- `max FLOAT8 DEFAULT 1.0` - Scale maximum

**Returns:** `dist`

#### lognormal

Create a log-normal distribution LogN(mu, sigma).

```sql
SELECT pgprob.lognormal(0, 0.5);
```

**Parameters:**
- `mu FLOAT8` - Mean of underlying normal
- `sigma FLOAT8` - Std dev of underlying normal (must be >= 0)

**Returns:** `dist`

#### pert

Create a PERT distribution (modified beta) for project estimation.

```sql
SELECT pgprob.pert(5, 10, 20);  -- Task duration estimate
```

**Parameters:**
- `min FLOAT8` - Optimistic estimate
- `mode FLOAT8` - Most likely estimate
- `max FLOAT8` - Pessimistic estimate
- `lambda FLOAT8 DEFAULT 4.0` - Mode weight

**Returns:** `dist`

#### poisson

Create a Poisson distribution Pois(lambda) for count data.

```sql
SELECT pgprob.poisson(5);  -- Events per hour
```

**Parameters:**
- `lambda FLOAT8` - Rate parameter (must be > 0)

**Returns:** `dist`

#### exponential

Create an exponential distribution Exp(lambda) for waiting times.

```sql
SELECT pgprob.exponential(0.1);  -- Time between events
```

**Parameters:**
- `lambda FLOAT8` - Rate parameter (must be > 0)

**Returns:** `dist`

### Conditional Constructors

#### if_above

Create a conditional distribution: if test > threshold, use then_dist, else use else_dist.

```sql
SELECT pgprob.if_above(
    pgprob.normal(100, 20), 90,          -- test: revenue > 90?
    pgprob.normal(100, 20) * 0.05,       -- then: 5% bonus
    pgprob.literal(0)                    -- else: no bonus
);
```

**Parameters:**
- `test_dist dist` - Distribution to test
- `threshold FLOAT8` - Threshold value
- `then_dist dist` - Result when test > threshold
- `else_dist dist` - Result when test <= threshold

**Returns:** `dist`

#### if_below

Create a conditional distribution: if test < threshold, use then_dist, else use else_dist.

```sql
SELECT pgprob.if_below(
    pgprob.normal(100, 20), 50,          -- test: revenue < 50?
    pgprob.literal(0),                   -- then: churned
    pgprob.normal(100, 20)               -- else: retained
);
```

**Parameters:**
- `test_dist dist` - Distribution to test
- `threshold FLOAT8` - Threshold value
- `then_dist dist` - Result when test < threshold
- `else_dist dist` - Result when test >= threshold

**Returns:** `dist`

#### if_then

Create a probability-based conditional: with probability p, use then_dist, else use else_dist.

```sql
SELECT pgprob.if_then(
    0.3,                                 -- 30% chance
    pgprob.literal(0),                   -- of churning
    pgprob.normal(100, 20)               -- else: retained
);
```

**Parameters:**
- `probability FLOAT8` - Probability of then branch (0-1)
- `then_dist dist` - Result with given probability
- `else_dist dist` - Result with complementary probability

**Returns:** `dist`

### Sampling

#### sample

Sample a single value from a distribution.

```sql
SELECT pgprob.sample(pgprob.normal(100, 15));
-- Returns: 97.32 (random)
```

**Parameters:**
- `dist dist` - Distribution to sample
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### samples

Sample multiple values from a distribution.

```sql
SELECT pgprob.samples(pgprob.normal(0, 1), 1000);
-- Returns: ARRAY of 1000 samples
```

**Parameters:**
- `dist dist` - Distribution to sample
- `n INTEGER` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8[]`

### Summary Statistics

#### summarize

Compute summary statistics via Monte Carlo simulation.

```sql
SELECT pgprob.summarize(price * quantity, 10000);
-- {"mean": 1234, "std": 56, "p5": 1100, "p50": 1230, "p95": 1340, ...}
```

**Parameters:**
- `dist dist` - Distribution to summarize
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `JSONB` - Statistics including mean, std, min, max, p5, p10, p25, p50, p75, p90, p95

#### mean

Get the mean of a distribution via sampling.

```sql
SELECT pgprob.mean(pgprob.normal(100, 15));
-- Returns: ~100
```

**Parameters:**
- `dist dist` - Distribution
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### variance

Get the variance of a distribution via sampling.

```sql
SELECT pgprob.variance(pgprob.normal(0, 10), 50000);
-- Returns: ~100
```

**Parameters:**
- `dist dist` - Distribution
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### stddev

Get the standard deviation of a distribution via sampling.

```sql
SELECT pgprob.stddev(pgprob.normal(0, 10), 50000);
-- Returns: ~10
```

**Parameters:**
- `dist dist` - Distribution
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### covariance

Get the covariance between two distributions via paired sampling.

```sql
SELECT pgprob.covariance(
    pgprob.normal(100, 10),
    pgprob.normal(50, 5),
    50000
);
```

**Parameters:**
- `dist1 dist` - First distribution
- `dist2 dist` - Second distribution
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### correlation

Get the Pearson correlation between two distributions via paired sampling.

```sql
SELECT pgprob.correlation(
    pgprob.normal(100, 10),
    pgprob.normal(50, 5),
    50000
);
```

**Parameters:**
- `dist1 dist` - First distribution
- `dist2 dist` - Second distribution
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### percentile

Get a specific percentile of a distribution.

```sql
SELECT pgprob.percentile(pgprob.normal(100, 15), 0.95);
-- Returns: ~124.7 (95th percentile)
```

**Parameters:**
- `dist dist` - Distribution
- `p FLOAT8` - Percentile (0-1)
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

### Probability Queries

#### prob_below

Probability that distribution is below a threshold.

```sql
SELECT pgprob.prob_below(pgprob.normal(100, 15), 85);
-- Returns: ~0.159 (15.9% chance below 85)
```

**Parameters:**
- `dist dist` - Distribution
- `threshold FLOAT8` - Threshold value
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### prob_above

Probability that distribution is above a threshold.

```sql
SELECT pgprob.prob_above(pgprob.normal(100, 15), 115);
-- Returns: ~0.159
```

**Parameters:**
- `dist dist` - Distribution
- `threshold FLOAT8` - Threshold value
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

#### prob_between

Probability that distribution is between two thresholds.

```sql
SELECT pgprob.prob_between(pgprob.normal(100, 15), 85, 115);
-- Returns: ~0.683 (68.3% within 1 std dev)
```

**Parameters:**
- `dist dist` - Distribution
- `lower FLOAT8` - Lower bound
- `upper FLOAT8` - Upper bound
- `n INTEGER DEFAULT 10000` - Number of samples
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `FLOAT8`

### NULL Handling

#### coalesce

Return the first non-NULL distribution. Overloaded for 2, 3, or 4 arguments.

```sql
SELECT pgprob.coalesce(NULL::dist, pgprob.normal(100, 10));
SELECT pgprob.coalesce(NULL::dist, NULL::dist, pgprob.literal(42));
SELECT pgprob.coalesce(a, b, c, pgprob.literal(0));  -- 4-arg form
```

**Parameters:**
- `a, b, [c, [d]] dist` - Distributions to check (2-4 arguments)

**Returns:** `dist` (nullable)

#### nvl

Return the distribution if non-NULL, otherwise the default.

```sql
SELECT pgprob.nvl(nullable_forecast, pgprob.normal(20000, 10000));
```

**Parameters:**
- `dist dist` - Distribution to check (nullable)
- `default dist` - Default distribution if first is NULL

**Returns:** `dist`

### Correlated Sampling

#### correlated_pair

Generate correlated sample pairs from two distributions using a Gaussian copula.

```sql
SELECT val1, val2
FROM pgprob.correlated_pair(
    pgprob.normal(100, 10),   -- distribution 1
    pgprob.normal(50, 5),     -- distribution 2
    0.8,                      -- correlation coefficient (-1 to 1)
    5000,                     -- number of samples
    42                        -- seed
);
```

**Parameters:**
- `dist1 dist` - First distribution
- `dist2 dist` - Second distribution
- `rho FLOAT8` - Correlation coefficient (-1 to 1)
- `n INTEGER DEFAULT 1000` - Number of sample pairs
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `TABLE (val1 FLOAT8, val2 FLOAT8)`

#### correlated_sample

Generate correlated sample vectors from multiple distributions using a Gaussian copula.

```sql
SELECT sample_idx, values
FROM pgprob.correlated_sample(
    ARRAY[pgprob.normal(100, 10), pgprob.normal(50, 5)],
    ARRAY[1.0, 0.8, 0.8, 1.0],   -- flat 2x2 correlation matrix (row-major)
    5000, 42
);
```

**Parameters:**
- `dists dist[]` - Array of distributions
- `correlations FLOAT8[]` - Flat NxN correlation matrix (row-major)
- `n INTEGER DEFAULT 1000` - Number of sample vectors
- `seed BIGINT DEFAULT NULL` - Optional random seed

**Returns:** `TABLE (sample_idx INTEGER, values FLOAT8[])`

## Operators

Distributions support arithmetic operations that propagate uncertainty:

| Operator | Syntax | Description |
|----------|--------|-------------|
| `+` | `dist + dist` or `dist + numeric` | Addition |
| `-` | `dist - dist` or `dist - numeric` | Subtraction |
| `*` | `dist * dist` or `dist * numeric` | Multiplication |
| `/` | `dist / dist` or `dist / numeric` | Division |
| `-` (unary) | `-dist` | Negation |

### Mathematical Functions

| Function | Description |
|----------|-------------|
| `pgprob.dist_abs(dist)` | Absolute value |
| `pgprob.dist_sqrt(dist)` | Square root |
| `pgprob.dist_exp(dist)` | Exponential (e^x) |
| `pgprob.dist_ln(dist)` | Natural logarithm |

### Implicit Casts

Numeric types cast automatically to `dist`:

| From | Example |
|------|---------|
| `float8` | `pgprob.normal(0,1) + 42.0` |
| `float4` | `pgprob.normal(0,1) + 42.0::float4` |
| `integer` | `pgprob.normal(0,1) + 42` |
| `numeric` | `pgprob.normal(0,1) + 42.00::numeric` |
| `text` | `'{"t":"normal","p":{"Normal":{"mu":0,"sigma":1}}}'::pgprob.dist` |

## Aggregates

| Aggregate | Description |
|-----------|-------------|
| `pgprob.sum(dist)` | Sum distributions across rows |
| `pgprob.avg(dist)` | Average distribution across rows (sum / count) |
| `pgprob.min(dist)` | Element-wise minimum across rows |
| `pgprob.max(dist)` | Element-wise maximum across rows |

All aggregates skip NULL values and return a `dist` that can be sampled or summarized.

```sql
SELECT pgprob.mean(pgprob.sum(cost_estimate), 10000) FROM project_tasks;
SELECT pgprob.summarize(pgprob.avg(revenue_forecast)) FROM clients;
```

## Distribution Fitting Aggregates

Fit parametric distributions from column data.

| Aggregate | Description |
|-----------|-------------|
| `pgprob.fit_normal(float8)` | Fit normal(mean, std) from data |
| `pgprob.fit_uniform(float8)` | Fit uniform(min, max) from data |
| `pgprob.fit_lognormal(float8)` | Fit lognormal(mu, sigma) from log-space stats |

```sql
-- Fit a normal distribution from historical revenue
SELECT pgprob.fit_normal(revenue) FROM client_revenue WHERE client_id = 'c01';

-- Fit per client
SELECT client_id, pgprob.fit_normal(revenue) AS rev_dist
FROM client_revenue
GROUP BY client_id;
```

## Example

```sql
-- Monte Carlo project estimation
CREATE TABLE tasks (
    name TEXT,
    duration_estimate dist  -- PERT distribution
);

INSERT INTO tasks VALUES
    ('Design', pgprob.pert(5, 10, 20)),
    ('Build', pgprob.pert(10, 15, 30)),
    ('Test', pgprob.pert(3, 5, 10));

-- Total project duration with uncertainty
SELECT pgprob.summarize(pgprob.sum(duration_estimate)) FROM tasks;
-- {"mean": 30, "p50": 29, "p95": 45, ...}

-- Probability of finishing in 40 days
SELECT pgprob.prob_below(pgprob.sum(duration_estimate), 40) FROM tasks;
-- 0.82 (82% chance)

-- Financial model with conditionals
SELECT pgprob.mean(
    pgprob.if_above(
        pgprob.normal(100000, 20000), 80000,
        pgprob.normal(100000, 20000) * 0.05,  -- bonus if above target
        pgprob.literal(0)
    ),
    10000
) AS expected_bonus;

-- Correlated risk analysis
SELECT corr(val1, val2), avg(val1), avg(val2)
FROM pgprob.correlated_pair(
    pgprob.normal(100, 10),
    pgprob.normal(50, 5),
    0.8, 10000, 42
);
```
