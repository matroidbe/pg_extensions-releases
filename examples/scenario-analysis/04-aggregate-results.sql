-- Aggregate Results: P&L Distribution Analysis
-- Computes probabilistic outcomes from scenario results

--------------------------------------------------------------------------------
-- Get current run
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_run_id UUID;
BEGIN
    -- Get from temp table or find most recent completed run
    BEGIN
        SELECT run_id INTO v_run_id FROM current_run LIMIT 1;
    EXCEPTION WHEN undefined_table THEN
        SELECT run_id INTO v_run_id
        FROM scenario_runs
        WHERE status = 'completed'
        ORDER BY updated_at DESC
        LIMIT 1;

        DROP TABLE IF EXISTS current_run;
        CREATE TEMP TABLE current_run AS SELECT v_run_id AS run_id;
    END;

    IF v_run_id IS NULL THEN
        RAISE EXCEPTION 'No completed run found. Run optimization first.';
    END IF;

    RAISE NOTICE 'Analyzing run: %', v_run_id;
END $$;

--------------------------------------------------------------------------------
-- P&L Distribution Summary
--------------------------------------------------------------------------------

SELECT '═══════════════════════════════════════════════════════════════' AS "";
SELECT '                    P&L DISTRIBUTION ANALYSIS                   ' AS "";
SELECT '═══════════════════════════════════════════════════════════════' AS "";

-- Key percentiles
SELECT 'Key Percentiles' AS metric_group;

WITH run_results AS (
    SELECT pnl
    FROM scenario_results sr
    JOIN current_run cr ON cr.run_id = sr.run_id
)
SELECT
    'P5 (Worst 5%)'::text AS metric,
    percentile_cont(0.05) WITHIN GROUP (ORDER BY pnl)::numeric(12,2) AS value
FROM run_results
UNION ALL
SELECT
    'P10',
    percentile_cont(0.10) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results
UNION ALL
SELECT
    'P25 (Lower Quartile)',
    percentile_cont(0.25) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results
UNION ALL
SELECT
    'P50 (Median)',
    percentile_cont(0.50) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results
UNION ALL
SELECT
    'P75 (Upper Quartile)',
    percentile_cont(0.75) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results
UNION ALL
SELECT
    'P90',
    percentile_cont(0.90) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results
UNION ALL
SELECT
    'P95 (Best 5%)',
    percentile_cont(0.95) WITHIN GROUP (ORDER BY pnl)::numeric(12,2)
FROM run_results;

--------------------------------------------------------------------------------
-- Summary Statistics
--------------------------------------------------------------------------------

SELECT 'Summary Statistics' AS metric_group;

SELECT
    COUNT(*) AS scenarios_analyzed,
    AVG(pnl)::numeric(12,2) AS expected_pnl,
    STDDEV(pnl)::numeric(12,2) AS pnl_volatility,
    MIN(pnl)::numeric(12,2) AS worst_case,
    MAX(pnl)::numeric(12,2) AS best_case,
    (MAX(pnl) - MIN(pnl))::numeric(12,2) AS pnl_range,
    -- Coefficient of variation (relative volatility)
    (STDDEV(pnl) / NULLIF(AVG(pnl), 0) * 100)::numeric(5,1) AS cv_percent
FROM scenario_results sr
JOIN current_run cr ON cr.run_id = sr.run_id;

--------------------------------------------------------------------------------
-- Revenue vs Cost Breakdown
--------------------------------------------------------------------------------

SELECT 'Revenue & Cost Breakdown' AS metric_group;

SELECT
    AVG(total_revenue)::numeric(12,2) AS avg_revenue,
    AVG(total_cost)::numeric(12,2) AS avg_cost,
    AVG(total_revenue - total_cost)::numeric(12,2) AS avg_margin,
    (AVG(total_cost) / NULLIF(AVG(total_revenue), 0) * 100)::numeric(5,1) AS cost_ratio_percent,
    AVG(projects_completed)::numeric(5,1) AS avg_projects_completed,
    AVG(projects_unassigned)::numeric(5,1) AS avg_projects_unassigned
FROM scenario_results sr
JOIN current_run cr ON cr.run_id = sr.run_id;

--------------------------------------------------------------------------------
-- P&L Histogram (Text-based)
--------------------------------------------------------------------------------

SELECT 'P&L Distribution Histogram' AS metric_group;

WITH run_results AS (
    SELECT pnl
    FROM scenario_results sr
    JOIN current_run cr ON cr.run_id = sr.run_id
),
bounds AS (
    SELECT
        MIN(pnl) AS min_pnl,
        MAX(pnl) AS max_pnl,
        (MAX(pnl) - MIN(pnl)) / 20 AS bucket_size
    FROM run_results
),
buckets AS (
    SELECT
        width_bucket(pnl, min_pnl, max_pnl + 0.01, 20) AS bucket,
        COUNT(*) AS cnt,
        min_pnl,
        bucket_size
    FROM run_results, bounds
    GROUP BY bucket, min_pnl, bucket_size
)
SELECT
    (min_pnl + (bucket - 1) * bucket_size)::numeric(10,0) AS range_start,
    (min_pnl + bucket * bucket_size)::numeric(10,0) AS range_end,
    cnt AS scenarios,
    REPEAT('█', (cnt * 40 / (SELECT MAX(cnt) FROM buckets))::int) AS histogram
FROM buckets
ORDER BY bucket;

--------------------------------------------------------------------------------
-- Risk Analysis
--------------------------------------------------------------------------------

SELECT 'Risk Analysis' AS metric_group;

WITH run_results AS (
    SELECT pnl
    FROM scenario_results sr
    JOIN current_run cr ON cr.run_id = sr.run_id
),
stats AS (
    SELECT
        AVG(pnl) AS mean_pnl,
        percentile_cont(0.05) WITHIN GROUP (ORDER BY pnl) AS var_95
    FROM run_results
)
SELECT
    -- Value at Risk (VaR): potential loss at 95% confidence
    (stats.mean_pnl - stats.var_95)::numeric(12,2) AS "VaR_95 (potential loss)",
    -- Probability of negative P&L
    (COUNT(*) FILTER (WHERE pnl < 0) * 100.0 / COUNT(*))::numeric(5,1) AS "prob_negative_pnl_%",
    -- Probability of exceeding expected value
    (COUNT(*) FILTER (WHERE pnl > stats.mean_pnl) * 100.0 / COUNT(*))::numeric(5,1) AS "prob_above_expected_%",
    -- Expected shortfall (average loss in worst 5%)
    (SELECT AVG(pnl) FROM run_results WHERE pnl <= stats.var_95)::numeric(12,2) AS "expected_shortfall_5%"
FROM run_results, stats
GROUP BY stats.mean_pnl, stats.var_95;

--------------------------------------------------------------------------------
-- Scenario Details: Best and Worst Cases
--------------------------------------------------------------------------------

SELECT 'Top 5 Best Scenarios' AS metric_group;

SELECT
    sr.sample_id,
    sr.pnl::numeric(12,2),
    sr.total_revenue::numeric(12,2),
    sr.total_cost::numeric(12,2),
    sr.projects_completed,
    sr.projects_unassigned
FROM scenario_results sr
JOIN current_run cr ON cr.run_id = sr.run_id
ORDER BY sr.pnl DESC
LIMIT 5;

SELECT 'Top 5 Worst Scenarios' AS metric_group;

SELECT
    sr.sample_id,
    sr.pnl::numeric(12,2),
    sr.total_revenue::numeric(12,2),
    sr.total_cost::numeric(12,2),
    sr.projects_completed,
    sr.projects_unassigned
FROM scenario_results sr
JOIN current_run cr ON cr.run_id = sr.run_id
ORDER BY sr.pnl ASC
LIMIT 5;

--------------------------------------------------------------------------------
-- Capacity Utilization Analysis
--------------------------------------------------------------------------------

SELECT 'Capacity Analysis' AS metric_group;

-- Which projects are most often unassigned?
SELECT
    p.name AS project,
    p.required_skill,
    p.revenue,
    p.order_probability,
    COUNT(*) AS times_ordered,
    SUM(CASE WHEN sr.solution->'unassigned' @> jsonb_build_array(
        jsonb_build_object('project_id', p.project_id)
    ) THEN 1 ELSE 0 END) AS times_unassigned,
    (SUM(CASE WHEN sr.solution->'unassigned' @> jsonb_build_array(
        jsonb_build_object('project_id', p.project_id)
    ) THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0))::numeric(5,1) AS unassigned_rate
FROM potential_projects p
JOIN scenario_samples ss ON ss.project_id = p.project_id AND ss.is_ordered = true
JOIN current_run cr ON cr.run_id = ss.run_id
JOIN scenario_results sr ON sr.run_id = ss.run_id AND sr.sample_id = ss.sample_id
GROUP BY p.project_id, p.name, p.required_skill, p.revenue, p.order_probability
HAVING COUNT(*) > 10
ORDER BY times_unassigned DESC
LIMIT 10;

--------------------------------------------------------------------------------
-- Executive Summary
--------------------------------------------------------------------------------

SELECT '═══════════════════════════════════════════════════════════════' AS "";
SELECT '                      EXECUTIVE SUMMARY                         ' AS "";
SELECT '═══════════════════════════════════════════════════════════════' AS "";

WITH run_results AS (
    SELECT pnl, total_revenue, total_cost, projects_completed
    FROM scenario_results sr
    JOIN current_run cr ON cr.run_id = sr.run_id
)
SELECT format(
    E'Based on %s Monte Carlo scenarios:\n\n' ||
    E'  Expected P&L:        %s\n' ||
    E'  P&L Range:           %s to %s\n' ||
    E'  95%% Confidence:      %s to %s\n\n' ||
    E'  Risk of Loss:        %s%% of scenarios have negative P&L\n' ||
    E'  Upside Potential:    %s%% chance of exceeding %s\n\n' ||
    E'  Avg Projects/Period: %s completed\n' ||
    E'  Avg Margin:          %s%%',
    COUNT(*),
    AVG(pnl)::numeric(12,0),
    MIN(pnl)::numeric(12,0),
    MAX(pnl)::numeric(12,0),
    percentile_cont(0.05) WITHIN GROUP (ORDER BY pnl)::numeric(12,0),
    percentile_cont(0.95) WITHIN GROUP (ORDER BY pnl)::numeric(12,0),
    (COUNT(*) FILTER (WHERE pnl < 0) * 100.0 / COUNT(*))::numeric(4,1),
    (COUNT(*) FILTER (WHERE pnl > AVG(pnl) OVER()) * 100.0 / COUNT(*))::numeric(4,1),
    AVG(pnl)::numeric(12,0),
    AVG(projects_completed)::numeric(4,1),
    (AVG(pnl) / NULLIF(AVG(total_revenue), 0) * 100)::numeric(4,1)
) AS summary
FROM run_results;
