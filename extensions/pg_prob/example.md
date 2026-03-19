# pg_prob Example: SaaS Quarterly Financial Planning

End-to-end example using every pg_prob feature to build a probabilistic financial model for a SaaS company's Q3 forecast.

## Setup

```sql
CREATE EXTENSION pg_prob;

-- Historical data: past quarterly revenue per client
CREATE TABLE client_revenue (
    client_id   text,
    segment     text,       -- 'enterprise', 'mid-market', 'startup'
    quarter     text,
    revenue     float8
);

-- Current client roster
CREATE TABLE clients (
    client_id       text PRIMARY KEY,
    name            text,
    segment         text,
    industry        text,
    active          boolean DEFAULT true
);

-- Analyst overrides (some clients have manual forecasts)
CREATE TABLE analyst_overrides (
    client_id   text PRIMARY KEY,
    forecast    pgprob.dist     -- analyst's subjective distribution
);

-- Segment-level benchmarks
CREATE TABLE segment_benchmarks (
    segment     text PRIMARY KEY,
    avg_dist    pgprob.dist     -- average revenue distribution for this segment
);
```

### Load sample data

```sql
-- 50 clients across 3 segments
INSERT INTO clients (client_id, name, segment, industry) VALUES
    ('c01', 'Acme Corp',      'enterprise',  'automotive'),
    ('c02', 'Globex Inc',     'enterprise',  'automotive'),
    ('c03', 'Initech',        'enterprise',  'finance'),
    ('c04', 'Umbrella Co',    'mid-market',  'healthcare'),
    ('c05', 'Stark Industries','mid-market',  'tech'),
    -- ... (50 clients total)
    ('c50', 'Pied Piper',     'startup',     'tech');

-- 4 quarters of historical revenue per client
INSERT INTO client_revenue (client_id, segment, quarter, revenue) VALUES
    ('c01', 'enterprise', '2025-Q3', 48000), ('c01', 'enterprise', '2025-Q4', 52000),
    ('c01', 'enterprise', '2026-Q1', 47000), ('c01', 'enterprise', '2026-Q2', 51000),
    ('c02', 'enterprise', '2025-Q3', 45000), ('c02', 'enterprise', '2025-Q4', 43000),
    ('c02', 'enterprise', '2026-Q1', 46000), ('c02', 'enterprise', '2026-Q2', 44000),
    -- ... (4 quarters x 50 clients)
    ('c50', 'startup',    '2026-Q2', 3200);

-- Analyst overrides for key accounts
INSERT INTO analyst_overrides VALUES
    ('c01', pgprob.normal(50000, 4000)),      -- analyst is confident about Acme
    ('c03', pgprob.triangular(30000, 45000, 55000));  -- Initech has a wide range

-- Segment benchmarks
INSERT INTO segment_benchmarks VALUES
    ('enterprise',  pgprob.normal(45000, 8000)),
    ('mid-market',  pgprob.normal(18000, 5000)),
    ('startup',     pgprob.lognormal(8.5, 0.6));  -- right-skewed: mostly small, few big
```

---

## Part 1: Distribution Constructors

Every distribution type in action — modeling different kinds of uncertainty.

```sql
-- Revenue follows a normal distribution (symmetric, well-understood)
SELECT pgprob.normal(45000, 8000) AS enterprise_revenue;

-- Infrastructure cost is uniform (we know the range, not the shape)
SELECT pgprob.uniform(80000, 120000) AS infra_cost;

-- Project timeline is triangular (best/most-likely/worst case)
SELECT pgprob.triangular(30, 45, 90) AS project_days;

-- Startup revenue is lognormal (right-skewed: mostly small, few big wins)
SELECT pgprob.lognormal(8.5, 0.6) AS startup_revenue;

-- Support ticket count is Poisson (discrete events per period)
SELECT pgprob.poisson(120.0) AS monthly_tickets;

-- Time between outages is exponential (memoryless)
SELECT pgprob.exponential(0.1) AS days_between_outages;

-- Estimated deal size uses PERT (weighted toward the mode)
SELECT pgprob.pert(10000, 25000, 80000) AS deal_size;

-- Conversion rate is beta (bounded between 0 and 1)
SELECT pgprob.beta(8.0, 2.0, 0.0, 1.0) AS conversion_rate;

-- Known fixed costs are literals (zero uncertainty)
SELECT pgprob.literal(15000) AS office_rent;
```

---

## Part 2: Arithmetic Operators

Build composite distributions with `+`, `-`, `*`, `/` operators.

```sql
-- Total cost = infra + salaries + office (distributions combine automatically)
SELECT
    pgprob.uniform(80000, 120000)
    + pgprob.normal(200000, 15000)
    + pgprob.literal(15000)
    AS total_cost;

-- Profit margin = (revenue - cost) / revenue
SELECT
    (pgprob.normal(400000, 50000) - pgprob.normal(300000, 30000))
    / pgprob.normal(400000, 50000)
    AS profit_margin;

-- Scale a distribution: 10% raise across the board
SELECT pgprob.normal(45000, 8000) * 1.10 AS post_raise_revenue;

-- Discount: subtract a fixed amount
SELECT pgprob.normal(45000, 8000) - 5000.0 AS discounted_revenue;

-- Revenue per employee (scalar division)
SELECT pgprob.normal(400000, 50000) / 25.0 AS revenue_per_head;
```

---

## Part 3: Unary Math Functions

Transform distributions with math operations.

```sql
-- Absolute value: distance from target (could be above or below)
SELECT pgprob.dist_abs(
    pgprob.normal(100000, 20000) - pgprob.literal(95000)
) AS deviation_from_target;

-- Log-transform revenue for normality (common in financial modeling)
SELECT pgprob.dist_ln(pgprob.lognormal(8.5, 0.6)) AS log_revenue;

-- Exponential growth: compound monthly rate
SELECT pgprob.dist_exp(pgprob.normal(0.02, 0.005) * 12.0) AS annual_growth_factor;

-- Square root: volatility scaling (sqrt of time rule)
SELECT pgprob.dist_sqrt(pgprob.literal(4.0)) * pgprob.normal(0, 1000)
    AS two_period_volatility;

-- Negate: model a loss as negative profit
SELECT -pgprob.normal(5000, 2000) AS expected_loss;
```

---

## Part 4: Implicit Casts

Numeric types cast automatically to `dist`, so you can mix scalars and distributions.

```sql
-- Integer cast: 100 becomes literal(100)
SELECT pgprob.normal(50000, 5000) + 100 AS with_bonus;

-- Float cast: works naturally in expressions
SELECT pgprob.normal(50000, 5000) * 1.05 AS five_pct_growth;

-- Numeric cast: precision preserved
SELECT pgprob.normal(50000, 5000) + 99.99 AS adjusted;

-- Text cast: parse distribution from JSON string
SELECT '{"t":"normal","p":{"Normal":{"mu":100,"sigma":10}}}'::pgprob.dist AS from_text;
```

---

## Part 5: Sampling

Draw values from distributions for simulation and inspection.

```sql
-- Single sample (random)
SELECT pgprob.sample(pgprob.normal(100000, 15000));

-- Single sample (reproducible with seed)
SELECT pgprob.sample(pgprob.normal(100000, 15000), 42);

-- Batch sample: 10,000 draws for histogram
SELECT pgprob.samples(pgprob.normal(100000, 15000), 10000, 42);

-- Sample a complex expression
SELECT pgprob.sample(
    pgprob.normal(45000, 8000) * pgprob.beta(8.0, 2.0, 0.0, 1.0)
);

-- Use samples for external analysis
SELECT unnest(pgprob.samples(
    pgprob.normal(100000, 15000) - pgprob.uniform(30000, 50000),
    1000
)) AS profit_sample;
```

---

## Part 6: Summary Statistics

Get a full statistical profile of any distribution or expression.

```sql
-- Full summary: mean, std, min, max, percentiles
SELECT pgprob.summarize(
    pgprob.normal(100000, 15000) - pgprob.uniform(30000, 50000),
    50000,  -- 50k samples for precision
    42      -- seed for reproducibility
);
-- Returns JSON:
-- {
--   "mean": 60000, "std": 18000,
--   "min": ..., "max": ...,
--   "p5": 30000, "p10": 35000, "p25": 47000,
--   "p50": 60000, "p75": 73000, "p90": 83000, "p95": 90000,
--   "n": 50000
-- }

-- Just the mean
SELECT pgprob.mean(
    pgprob.normal(45000, 8000) + pgprob.normal(18000, 5000),
    10000
) AS expected_combined;

-- Specific percentile: P10 (downside scenario)
SELECT pgprob.percentile(
    pgprob.normal(400000, 50000) - pgprob.normal(300000, 30000),
    0.10,
    10000
) AS profit_p10;
```

---

## Part 7: Probability Queries

Ask direct probability questions about your model.

```sql
-- What's the probability profit exceeds 100k?
SELECT pgprob.prob_above(
    pgprob.normal(400000, 50000) - pgprob.normal(300000, 30000),
    100000
) AS prob_profit_above_100k;

-- What's the probability of a loss?
SELECT pgprob.prob_below(
    pgprob.normal(400000, 50000) - pgprob.normal(300000, 30000),
    0
) AS prob_of_loss;

-- What's the probability revenue lands in our forecast range?
SELECT pgprob.prob_between(
    pgprob.normal(400000, 50000),
    350000, 450000
) AS prob_in_range;
```

---

## Part 8: SUM Aggregate

Roll up distributions across rows — the result is still a distribution.

```sql
-- Total revenue across all clients (each has a different distribution)
WITH client_dists AS (
    SELECT
        client_id,
        pgprob.normal(avg(revenue), stddev(revenue)) AS rev_dist
    FROM client_revenue
    GROUP BY client_id
)
SELECT
    pgprob.sum(rev_dist) AS total_revenue,
    pgprob.mean(pgprob.sum(rev_dist), 10000) AS expected_total
FROM client_dists;
```

---

## Part 9: Distribution Fitting

Fit distributions automatically from column data, instead of manually computing parameters.

```sql
-- Fit a normal distribution from historical revenue
WITH fitted AS (
    SELECT
        client_id,
        pgprob.fit_normal(revenue) AS rev_dist
    FROM client_revenue
    GROUP BY client_id
)
SELECT client_id, rev_dist,
       pgprob.mean(rev_dist, 10000) AS expected
FROM fitted
ORDER BY expected DESC
LIMIT 10;

-- Startup revenue is right-skewed — fit lognormal instead
SELECT
    pgprob.fit_lognormal(revenue) AS startup_rev_dist
FROM client_revenue cr
JOIN clients c ON c.client_id = cr.client_id
WHERE c.segment = 'startup';

-- Infrastructure cost looks uniform — fit that
SELECT pgprob.fit_uniform(monthly_cost) AS infra_dist
FROM infra_invoices
WHERE invoice_date >= '2025-07-01';
```

---

## Part 10: Coalesce and NULL Handling

Handle missing distributions with fallback chains.

```sql
-- Chain of fallbacks: analyst override -> fitted history -> segment benchmark -> global default
WITH client_dists AS (
    SELECT
        c.client_id,
        c.segment,
        ao.forecast AS override_dist,
        pgprob.fit_normal(cr.revenue) AS hist_dist,
        sb.avg_dist AS segment_dist
    FROM clients c
    LEFT JOIN analyst_overrides ao ON ao.client_id = c.client_id
    LEFT JOIN client_revenue cr ON cr.client_id = c.client_id
    LEFT JOIN segment_benchmarks sb ON sb.segment = c.segment
    GROUP BY c.client_id, c.segment, ao.forecast, sb.avg_dist
)
SELECT
    client_id,
    -- First non-NULL distribution wins
    pgprob.coalesce(
        override_dist,
        hist_dist,
        segment_dist,
        pgprob.normal(20000, 10000)   -- global fallback
    ) AS revenue_forecast
FROM client_dists;

-- Simpler two-argument form
SELECT
    client_id,
    pgprob.nvl(ao.forecast, pgprob.normal(20000, 10000)) AS forecast
FROM clients c
LEFT JOIN analyst_overrides ao ON ao.client_id = c.client_id;
```

---

## Part 11: Conditional Distributions

Model business rules that change behavior based on uncertain outcomes.

```sql
WITH forecasts AS (
    SELECT
        client_id,
        pgprob.coalesce(ao.forecast, pgprob.fit_normal(cr.revenue))
            AS rev_dist
    FROM clients c
    LEFT JOIN analyst_overrides ao ON ao.client_id = c.client_id
    LEFT JOIN client_revenue cr ON cr.client_id = c.client_id
    GROUP BY c.client_id, ao.forecast
)
SELECT
    client_id,
    rev_dist,

    -- Sales bonus: 5% of revenue IF client revenue exceeds 30k, else nothing
    pgprob.if_above(
        rev_dist, 30000,
        rev_dist * 0.05,             -- then: 5% bonus
        pgprob.literal(0)            -- else: no bonus
    ) AS sales_bonus,

    -- Churn risk: if revenue drops below 10k, 60% chance of churn (lose all revenue)
    pgprob.if_below(
        rev_dist, 10000,
        pgprob.if_then(              -- nested conditional
            0.6,                     -- 60% probability of churn
            pgprob.literal(0),       -- churned: zero revenue
            rev_dist                 -- stayed: keep revenue
        ),
        rev_dist                     -- healthy: keep revenue
    ) AS adjusted_revenue,

    -- Tier pricing: enterprise discount kicks in above 50k
    pgprob.if_above(
        rev_dist, 50000,
        rev_dist * 0.95,             -- 5% volume discount
        rev_dist                     -- standard pricing
    ) AS billed_revenue

FROM forecasts;
```

---

## Part 12: AVG Aggregate

Compute the average distribution across a group — not the mean of means, but the actual average distribution.

```sql
-- Average revenue per client by segment (as a distribution, not scalar)
SELECT
    c.segment,
    pgprob.avg(
        pgprob.coalesce(ao.forecast, pgprob.fit_normal(cr.revenue))
    ) AS avg_client_revenue,

    -- Compare: what does the average client look like per segment?
    pgprob.mean(
        pgprob.avg(pgprob.coalesce(ao.forecast, pgprob.fit_normal(cr.revenue))),
        10000
    ) AS expected_avg,

    pgprob.percentile(
        pgprob.avg(pgprob.coalesce(ao.forecast, pgprob.fit_normal(cr.revenue))),
        0.05, 10000
    ) AS avg_p5

FROM clients c
LEFT JOIN analyst_overrides ao ON ao.client_id = c.client_id
LEFT JOIN client_revenue cr ON cr.client_id = c.client_id
GROUP BY c.segment;

-- Result:
-- segment     | expected_avg | avg_p5
-- enterprise  | 45200        | 38100
-- mid-market  | 18300        | 12400
-- startup     | 6800         | 2100
```

---

## Part 13: MIN/MAX Aggregates

Find best-case and worst-case clients — as distributions, not point estimates.

```sql
-- Worst-case and best-case client revenue (as distributions)
SELECT
    c.segment,

    -- The client with the lowest revenue (distribution of the minimum)
    pgprob.min(pgprob.fit_normal(cr.revenue)) AS worst_client_rev,

    -- The client with the highest revenue (distribution of the maximum)
    pgprob.max(pgprob.fit_normal(cr.revenue)) AS best_client_rev,

    -- Range: difference between best and worst
    pgprob.mean(
        pgprob.max(pgprob.fit_normal(cr.revenue))
        - pgprob.min(pgprob.fit_normal(cr.revenue)),
        10000
    ) AS expected_range

FROM clients c
JOIN client_revenue cr ON cr.client_id = c.client_id
GROUP BY c.segment;

-- Portfolio risk: what's the maximum single-client deviation?
SELECT
    pgprob.summarize(
        pgprob.max(
            pgprob.dist_abs(
                pgprob.fit_normal(cr.revenue) - pgprob.literal(avg(cr.revenue))
            )
        ),
        10000
    ) AS max_deviation_summary
FROM client_revenue cr;
```

---

## Part 14: Variance and Covariance

Quantify risk and measure dependencies between uncertain quantities.

```sql
-- Risk metrics for the full portfolio
WITH portfolio AS (
    SELECT
        pgprob.sum(pgprob.fit_normal(cr.revenue)) AS total_revenue,
        pgprob.uniform(80000, 120000)
        + pgprob.normal(250000, 10000)
        + pgprob.literal(15000) AS total_cost
    FROM client_revenue cr
)
SELECT
    -- How volatile is our revenue forecast?
    pgprob.variance(total_revenue, 50000) AS revenue_variance,
    pgprob.stddev(total_revenue, 50000) AS revenue_risk,

    -- How volatile is our cost forecast?
    pgprob.stddev(total_cost, 50000) AS cost_risk,

    -- Are revenue and cost correlated?
    -- (positive = costs rise with revenue, negative = costs offset revenue)
    pgprob.covariance(total_revenue, total_cost, 50000) AS rev_cost_covariance,
    pgprob.correlation(total_revenue, total_cost, 50000) AS rev_cost_correlation

FROM portfolio;

-- Per-segment risk comparison
SELECT
    c.segment,
    pgprob.stddev(pgprob.sum(pgprob.fit_normal(cr.revenue)), 10000) AS segment_risk,
    pgprob.mean(pgprob.sum(pgprob.fit_normal(cr.revenue)), 10000) AS segment_expected,
    -- Coefficient of variation: risk relative to size
    pgprob.stddev(pgprob.sum(pgprob.fit_normal(cr.revenue)), 10000)
    / pgprob.mean(pgprob.sum(pgprob.fit_normal(cr.revenue)), 10000)
        AS coefficient_of_variation
FROM clients c
JOIN client_revenue cr ON cr.client_id = c.client_id
GROUP BY c.segment;
```

---

## Part 15: Correlated Distributions

Model dependencies between clients in the same industry.

```sql
-- Problem: Acme (c01) and Globex (c02) are both automotive enterprise clients.
-- If one loses revenue (industry downturn), the other likely does too.
-- Independent modeling underestimates this concentration risk.

-- Simple 2-distribution version: correlated_pair returns (val1, val2) rows
SELECT val1 AS acme_revenue, val2 AS globex_revenue
FROM pgprob.correlated_pair(
    pgprob.normal(49500, 2200),    -- Acme's revenue distribution
    pgprob.normal(44500, 1300),    -- Globex's revenue distribution
    0.8,                           -- 80% correlation (same industry)
    5000,                          -- 5000 sample pairs
    42                             -- seed for reproducibility
);

-- Analyze the correlated samples
WITH samples AS (
    SELECT val1, val2
    FROM pgprob.correlated_pair(
        pgprob.normal(49500, 2200),
        pgprob.normal(44500, 1300),
        0.8, 10000, 42
    )
)
SELECT
    corr(val1, val2) AS measured_correlation,  -- should be ~0.8
    avg(val1) AS acme_mean,
    avg(val2) AS globex_mean,
    -- Probability both drop below 40k simultaneously
    count(*) FILTER (WHERE val1 < 40000 AND val2 < 40000)::float / count(*) AS prob_both_drop
FROM samples;

-- General N-distribution version: correlated_sample
-- Correlations passed as a flat row-major array
SELECT sample_idx, values
FROM pgprob.correlated_sample(
    ARRAY[
        pgprob.normal(49500, 2200),     -- Acme
        pgprob.normal(44500, 1300),     -- Globex
        pgprob.normal(18000, 3000)      -- mid-market client in same sector
    ],
    ARRAY[1.0, 0.8, 0.3,              -- row 1: Acme correlations
          0.8, 1.0, 0.3,              -- row 2: Globex correlations
          0.3, 0.3, 1.0],             -- row 3: mid-market correlations
    5000,                              -- 5000 samples
    42                                 -- seed
);
```

---

## Part 16: The Complete Model

The full Q3 forecast using every feature together.

```sql
-- ============================================================
-- STEP 1: Fit distributions from historical data
-- ============================================================
WITH fitted_clients AS (
    SELECT
        c.client_id,
        c.name,
        c.segment,
        c.industry,
        ao.forecast AS override_dist,
        pgprob.fit_normal(cr.revenue) AS hist_dist,
        sb.avg_dist AS segment_dist
    FROM clients c
    LEFT JOIN client_revenue cr ON cr.client_id = c.client_id
    LEFT JOIN analyst_overrides ao ON ao.client_id = c.client_id
    LEFT JOIN segment_benchmarks sb ON sb.segment = c.segment
    WHERE c.active = true
    GROUP BY c.client_id, c.name, c.segment, c.industry,
             ao.forecast, sb.avg_dist
),

-- ============================================================
-- STEP 2: Handle missing data with coalesce
-- ============================================================
base_forecasts AS (
    SELECT
        client_id, name, segment, industry,
        pgprob.coalesce(
            override_dist,              -- analyst says...
            hist_dist,                  -- history says...
            segment_dist,               -- segment average says...
            pgprob.normal(20000, 10000) -- last resort
        ) AS rev_dist
    FROM fitted_clients
),

-- ============================================================
-- STEP 3: Apply business rules with conditionals
-- ============================================================
adjusted_forecasts AS (
    SELECT
        client_id, name, segment, industry,
        rev_dist,

        -- Churn risk: if revenue < 10k, 40% chance client leaves
        pgprob.if_below(
            rev_dist, 10000,
            pgprob.if_then(0.4, pgprob.literal(0), rev_dist),
            rev_dist
        ) AS adjusted_rev,

        -- Sales commission: 5% if revenue > 30k
        pgprob.if_above(rev_dist, 30000,
            rev_dist * 0.05,
            pgprob.literal(0)
        ) AS commission,

        -- Volume discount: 5% off if > 50k
        pgprob.if_above(rev_dist, 50000,
            rev_dist * 0.95,
            rev_dist
        ) AS net_revenue
    FROM base_forecasts
),

-- ============================================================
-- STEP 4: Aggregate with SUM, AVG, MIN, MAX
-- ============================================================
portfolio AS (
    SELECT
        -- Total revenue (SUM)
        pgprob.sum(net_revenue) AS total_revenue,

        -- Total commission cost
        pgprob.sum(commission) AS total_commission,

        -- Average revenue per client (AVG)
        pgprob.avg(net_revenue) AS avg_client_revenue,

        -- Best and worst client (MIN/MAX)
        pgprob.max(net_revenue) AS best_client,
        pgprob.min(net_revenue) AS worst_client
    FROM adjusted_forecasts
),

-- Fixed costs (using different distribution types)
costs AS (
    SELECT
        pgprob.uniform(80000, 120000)           -- infra (contract range)
        + pgprob.normal(250000, 10000)           -- salaries (predictable)
        + pgprob.literal(15000)                  -- office (fixed)
        + pgprob.poisson(120.0) * 45.0           -- support tickets x cost
        AS operating_cost
),

-- ============================================================
-- STEP 5: Compute profit
-- ============================================================
profit AS (
    SELECT
        p.total_revenue,
        p.total_commission,
        p.avg_client_revenue,
        p.best_client,
        p.worst_client,
        c.operating_cost,
        p.total_revenue - c.operating_cost - p.total_commission AS profit
    FROM portfolio p, costs c
)

-- ============================================================
-- STEP 6: Full analysis with variance and probability queries
-- ============================================================
SELECT
    -- Point estimates
    pgprob.mean(profit, 50000) AS expected_profit,
    pgprob.mean(total_revenue, 50000) AS expected_revenue,
    pgprob.mean(operating_cost, 50000) AS expected_cost,

    -- Risk metrics (variance/stddev)
    pgprob.stddev(profit, 50000) AS profit_risk,
    pgprob.stddev(total_revenue, 50000) AS revenue_risk,
    pgprob.correlation(total_revenue, operating_cost, 50000)
        AS revenue_cost_correlation,

    -- Confidence interval
    pgprob.percentile(profit, 0.05, 50000) AS profit_p5,
    pgprob.percentile(profit, 0.50, 50000) AS profit_p50,
    pgprob.percentile(profit, 0.95, 50000) AS profit_p95,

    -- Probability queries
    pgprob.prob_above(profit, 500000, 50000) AS prob_hit_500k,
    pgprob.prob_below(profit, 0, 50000) AS prob_of_loss,
    pgprob.prob_between(profit, 300000, 600000, 50000) AS prob_in_range,

    -- Per-client metrics (AVG, MIN, MAX)
    pgprob.mean(avg_client_revenue, 10000) AS avg_revenue_per_client,
    pgprob.mean(best_client, 10000) AS best_client_expected,
    pgprob.mean(worst_client, 10000) AS worst_client_expected,

    -- Full profit distribution
    pgprob.summarize(profit, 50000, 42) AS profit_distribution

FROM profit;

-- ============================================================
-- BONUS: Concentration risk with correlated sampling
-- ============================================================
-- The two automotive clients (c01, c02) are highly correlated.
-- Model that dependency to reveal concentration risk.

WITH corr_samples AS (
    SELECT val1 AS acme_rev, val2 AS globex_rev
    FROM pgprob.correlated_pair(
        pgprob.normal(49500, 2200),
        pgprob.normal(44500, 1300),
        0.8,        -- 80% correlation: same industry
        50000, 42
    )
)
SELECT
    corr(acme_rev, globex_rev) AS measured_correlation,
    avg(acme_rev) AS acme_expected,
    avg(globex_rev) AS globex_expected,
    -- Joint downside: how often do BOTH drop below 40k?
    count(*) FILTER (WHERE acme_rev < 40000 AND globex_rev < 40000)::float
        / count(*) AS prob_both_below_40k
FROM corr_samples;
```

---

## Feature Coverage

| Feature | Used In |
|---------|---------|
| `literal()` | Parts 1, 3, 11, 16 |
| `normal()` | Parts 1, 2, 15, 16 |
| `uniform()` | Parts 1, 2, 14, 16 |
| `triangular()` | Part 1, setup |
| `beta()` | Parts 1, 5 |
| `lognormal()` | Parts 1, 3, setup |
| `pert()` | Part 1 |
| `poisson()` | Parts 1, 16 |
| `exponential()` | Part 1 |
| `+`, `-`, `*`, `/` operators | Parts 2, 16 |
| Unary `-` | Part 3 |
| `dist_abs()` | Parts 3, 13 |
| `dist_sqrt()` | Part 3 |
| `dist_exp()` | Part 3 |
| `dist_ln()` | Part 3 |
| Implicit casts (int, float, numeric, text) | Part 4 |
| `sample()` | Part 5 |
| `samples()` | Part 5 |
| `summarize()` | Parts 6, 16 |
| `mean()` | Parts 6, 12, 13, 16 |
| `percentile()` | Parts 6, 12, 16 |
| `prob_above()` | Parts 7, 16 |
| `prob_below()` | Parts 7, 16 |
| `prob_between()` | Parts 7, 16 |
| `sum()` aggregate | Parts 8, 14, 16 |
| `fit_normal()` | Parts 9, 10, 13, 14, 16 |
| `fit_lognormal()` | Part 9 |
| `fit_uniform()` | Part 9 |
| `coalesce()` | Parts 10, 11, 12, 16 |
| `nvl()` | Part 10 |
| `if_above()` | Parts 11, 16 |
| `if_below()` | Parts 11, 16 |
| `if_then()` | Parts 11, 16 |
| `avg()` aggregate | Parts 12, 16 |
| `min()` aggregate | Parts 13, 16 |
| `max()` aggregate | Parts 13, 16 |
| `variance()` | Part 14 |
| `stddev()` | Parts 14, 16 |
| `covariance()` | Part 14 |
| `correlation()` | Parts 14, 16 |
| `correlated_pair()` | Part 15, 16 |
| `correlated_sample()` | Part 15 |
