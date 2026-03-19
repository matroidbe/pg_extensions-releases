-- Scenario Analysis Schema
-- Tables for projects, consultants, and checkpoint tracking

-- Clean up if re-running
DROP TABLE IF EXISTS scenario_results CASCADE;
DROP TABLE IF EXISTS scenario_samples CASCADE;
DROP TABLE IF EXISTS scenario_runs CASCADE;
DROP TABLE IF EXISTS potential_projects CASCADE;
DROP TABLE IF EXISTS consultants CASCADE;

--------------------------------------------------------------------------------
-- Domain Tables
--------------------------------------------------------------------------------

-- Projects that may or may not be ordered
CREATE TABLE potential_projects (
    project_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    revenue NUMERIC NOT NULL,           -- Revenue if project is completed
    order_probability NUMERIC NOT NULL  -- Probability project will be ordered (0.0-1.0)
        CHECK (order_probability >= 0 AND order_probability <= 1),
    required_skill TEXT NOT NULL,       -- Skill needed to complete
    effort_days INT NOT NULL DEFAULT 5  -- Days of work required
);

-- Available consultants
CREATE TABLE consultants (
    consultant_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    skill TEXT NOT NULL,                -- Consultant's skill
    daily_cost NUMERIC NOT NULL,        -- Cost per day
    available_days INT NOT NULL         -- Days available in planning period
);

--------------------------------------------------------------------------------
-- Checkpoint Tables
--------------------------------------------------------------------------------

-- Track each analysis run
CREATE TABLE scenario_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ DEFAULT now(),
    total_samples INT NOT NULL,
    completed_samples INT DEFAULT 0,
    status TEXT DEFAULT 'running'       -- running, completed, failed
        CHECK (status IN ('running', 'completed', 'failed')),
    last_error TEXT,
    last_sample_id INT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Sampled scenarios (which projects are ordered in each sample)
CREATE TABLE scenario_samples (
    run_id UUID REFERENCES scenario_runs ON DELETE CASCADE,
    sample_id INT NOT NULL,
    project_id INT REFERENCES potential_projects,
    is_ordered BOOLEAN NOT NULL,
    PRIMARY KEY (run_id, sample_id, project_id)
);

-- Index for efficient filtering of ordered projects
CREATE INDEX idx_scenario_samples_ordered
ON scenario_samples (run_id, sample_id)
WHERE is_ordered = true;

-- Optimization results for each scenario
CREATE TABLE scenario_results (
    run_id UUID REFERENCES scenario_runs ON DELETE CASCADE,
    sample_id INT NOT NULL,
    solution JSONB,                     -- Full solution from pg_ortools
    total_revenue NUMERIC,              -- Sum of completed project revenues
    total_cost NUMERIC,                 -- Sum of consultant costs
    pnl NUMERIC,                        -- Revenue - Cost
    projects_completed INT,             -- Count of projects assigned
    projects_unassigned INT,            -- Count of projects that couldn't be assigned
    computed_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (run_id, sample_id)
);

--------------------------------------------------------------------------------
-- Sample Data
--------------------------------------------------------------------------------

-- Insert potential projects with varying probabilities
INSERT INTO potential_projects (name, revenue, order_probability, required_skill, effort_days) VALUES
    ('Cloud Migration Alpha',    50000,  0.80, 'Senior',  10),
    ('Cloud Migration Beta',     45000,  0.60, 'Senior',   8),
    ('API Integration',          30000,  0.90, 'Medior',   5),
    ('Data Pipeline',            35000,  0.70, 'Senior',   7),
    ('Frontend Redesign',        25000,  0.85, 'Medior',   6),
    ('Mobile App MVP',           40000,  0.50, 'Senior',  12),
    ('Security Audit',           20000,  0.95, 'Senior',   3),
    ('Performance Optimization', 15000,  0.75, 'Medior',   4),
    ('Documentation Sprint',      8000,  0.90, 'Junior',   5),
    ('Training Workshop',        12000,  0.65, 'Medior',   2),
    ('Legacy Modernization',     60000,  0.40, 'Senior',  15),
    ('DevOps Setup',             25000,  0.80, 'Medior',   5),
    ('Analytics Dashboard',      22000,  0.70, 'Medior',   6),
    ('Chatbot Integration',      18000,  0.55, 'Junior',   8),
    ('Compliance Review',        16000,  0.85, 'Senior',   4);

-- Insert consultants with different skill levels and costs
INSERT INTO consultants (name, skill, daily_cost, available_days) VALUES
    ('Alice',   'Senior', 1200, 20),
    ('Bob',     'Senior', 1100, 18),
    ('Carol',   'Medior',  800, 22),
    ('David',   'Medior',  750, 20),
    ('Eve',     'Junior',  500, 22),
    ('Frank',   'Senior', 1300, 15),
    ('Grace',   'Medior',  850, 20),
    ('Henry',   'Junior',  450, 22);

--------------------------------------------------------------------------------
-- Helper Views
--------------------------------------------------------------------------------

-- Summary of potential pipeline
CREATE VIEW pipeline_summary AS
SELECT
    COUNT(*) AS total_projects,
    SUM(revenue) AS total_potential_revenue,
    SUM(revenue * order_probability) AS expected_revenue,
    AVG(order_probability) AS avg_probability
FROM potential_projects;

-- Capacity by skill
CREATE VIEW capacity_by_skill AS
SELECT
    skill,
    COUNT(*) AS consultant_count,
    SUM(available_days) AS total_days,
    AVG(daily_cost) AS avg_daily_cost
FROM consultants
GROUP BY skill;

-- Demand by skill (expected)
CREATE VIEW demand_by_skill AS
SELECT
    required_skill AS skill,
    COUNT(*) AS project_count,
    SUM(effort_days) AS total_effort,
    SUM(effort_days * order_probability) AS expected_effort
FROM potential_projects
GROUP BY required_skill;

--------------------------------------------------------------------------------
-- Verify setup
--------------------------------------------------------------------------------

SELECT 'Pipeline Summary' AS info;
SELECT * FROM pipeline_summary;

SELECT 'Capacity by Skill' AS info;
SELECT * FROM capacity_by_skill;

SELECT 'Demand by Skill' AS info;
SELECT * FROM demand_by_skill;
