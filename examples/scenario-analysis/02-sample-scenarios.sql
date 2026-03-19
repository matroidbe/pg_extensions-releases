-- Sample Scenarios using pg_prob
-- Generates Monte Carlo samples of which projects will be ordered

-- Configuration
\set num_samples 1000

--------------------------------------------------------------------------------
-- Create a new run
--------------------------------------------------------------------------------

-- Start a new analysis run
INSERT INTO scenario_runs (total_samples)
VALUES (:num_samples)
RETURNING run_id AS current_run_id \gset

SELECT 'Started run: ' || :'current_run_id' AS status;

--------------------------------------------------------------------------------
-- Generate scenario samples
--------------------------------------------------------------------------------

-- For each sample, flip a coin for each project based on order_probability
-- Using pg_prob.bernoulli_sample() for probabilistic sampling

INSERT INTO scenario_samples (run_id, sample_id, project_id, is_ordered)
SELECT
    :'current_run_id'::uuid AS run_id,
    s.sample_id,
    p.project_id,
    -- Bernoulli trial: random() < probability means "ordered"
    -- Replace with pg_prob.bernoulli_sample(p.order_probability) when available
    random() < p.order_probability AS is_ordered
FROM
    generate_series(1, :num_samples) AS s(sample_id)
CROSS JOIN
    potential_projects p;

--------------------------------------------------------------------------------
-- Analyze sample distribution
--------------------------------------------------------------------------------

-- How many projects are ordered per scenario?
SELECT 'Projects ordered per scenario' AS info;
SELECT
    MIN(ordered_count) AS min_projects,
    AVG(ordered_count)::numeric(10,1) AS avg_projects,
    MAX(ordered_count) AS max_projects,
    STDDEV(ordered_count)::numeric(10,2) AS stddev_projects
FROM (
    SELECT sample_id, COUNT(*) AS ordered_count
    FROM scenario_samples
    WHERE run_id = :'current_run_id'::uuid AND is_ordered = true
    GROUP BY sample_id
) sub;

-- Distribution of scenario sizes (histogram)
SELECT 'Scenario size distribution' AS info;
SELECT
    ordered_count AS projects_ordered,
    COUNT(*) AS num_scenarios,
    REPEAT('█', (COUNT(*) * 50 / :num_samples)::int) AS histogram
FROM (
    SELECT sample_id, COUNT(*) AS ordered_count
    FROM scenario_samples
    WHERE run_id = :'current_run_id'::uuid AND is_ordered = true
    GROUP BY sample_id
) sub
GROUP BY ordered_count
ORDER BY ordered_count;

-- Revenue distribution across scenarios
SELECT 'Revenue per scenario' AS info;
SELECT
    MIN(scenario_revenue) AS min_revenue,
    AVG(scenario_revenue)::numeric(12,0) AS avg_revenue,
    MAX(scenario_revenue) AS max_revenue,
    STDDEV(scenario_revenue)::numeric(12,0) AS stddev_revenue
FROM (
    SELECT
        ss.sample_id,
        SUM(p.revenue) AS scenario_revenue
    FROM scenario_samples ss
    JOIN potential_projects p ON p.project_id = ss.project_id
    WHERE ss.run_id = :'current_run_id'::uuid AND ss.is_ordered = true
    GROUP BY ss.sample_id
) sub;

--------------------------------------------------------------------------------
-- Output run ID for next step
--------------------------------------------------------------------------------

SELECT
    'Sampling complete. Run ID: ' || :'current_run_id' AS status,
    :num_samples AS samples_generated;

-- Save run_id to a temp table for easy access in next scripts
DROP TABLE IF EXISTS current_run;
CREATE TEMP TABLE current_run AS
SELECT :'current_run_id'::uuid AS run_id;
