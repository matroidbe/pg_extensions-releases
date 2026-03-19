-- =============================================================================
-- Consultant Assignment Problem (Declarative API)
-- =============================================================================
--
-- Same problem as consultant-assignment.sql but using the declarative API.
-- This approach reads directly from database tables - much less boilerplate!
--
-- Business Scenario:
-- A consulting firm needs to staff 3 projects (Alpha, Beta, Gamma).
-- Each project team must have exactly:
--   - 1 Junior consultant
--   - 1 Medior consultant
--   - 1 Senior consultant
--
-- Each consultant can only work on 1 project.
-- Goal: Minimize total hourly cost while satisfying all constraints.
--
-- =============================================================================

-- =============================================================================
-- Step 1: Create the data tables
-- =============================================================================

DROP TABLE IF EXISTS consultants CASCADE;
DROP TABLE IF EXISTS client_projects CASCADE;

CREATE TABLE consultants (
    name TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    hourly_rate INT NOT NULL
);

CREATE TABLE client_projects (
    name TEXT PRIMARY KEY
);

-- Insert consultant data
INSERT INTO consultants (name, role, hourly_rate) VALUES
    ('Alice', 'Junior', 50),
    ('Bob', 'Junior', 55),
    ('Carol', 'Junior', 48),
    ('Dave', 'Medior', 80),
    ('Eve', 'Medior', 85),
    ('Frank', 'Medior', 78),
    ('Grace', 'Senior', 120),
    ('Henry', 'Senior', 125),
    ('Ivan', 'Senior', 115);

-- Insert project data
INSERT INTO client_projects (name) VALUES
    ('Alpha'),
    ('Beta'),
    ('Gamma');

-- =============================================================================
-- Step 2: Solve using declarative API (single function call!)
-- =============================================================================
--
-- solve_assignment parameters:
--   problem_name      - Name for this optimization problem
--   resources_table   - Table containing the resources (consultants)
--   resource_name_col - Column with resource names
--   resource_group_col- Column defining groups (roles)
--   resource_cost_col - Column with cost values
--   targets_table     - Table containing targets (projects)
--   target_name_col   - Column with target names
--   group_per_target  - How many of each group per target
--   objective         - 'minimize' or 'maximize'
--
-- This automatically:
--   1. Creates boolean variables for each resource-target pair
--   2. Adds constraint: each resource assigned to exactly 1 target
--   3. Adds constraint: each target gets exactly N of each group
--   4. Sets objective function based on costs
--   5. Solves and returns results

SELECT pgortools.solve_assignment(
    'team_staffing',                     -- problem name
    'consultants', 'name', 'role', 'hourly_rate',  -- resources table
    'client_projects', 'name',           -- targets table
    1,                                   -- 1 of each role per project
    'minimize'                           -- minimize total cost
);

-- =============================================================================
-- Step 3: Parse the solution into a readable format
-- =============================================================================

WITH solution AS (
    SELECT pgortools.solve_assignment(
        'display_staffing',
        'consultants', 'name', 'role', 'hourly_rate',
        'client_projects', 'name',
        1,
        'minimize'
    ) as sol
)
SELECT
    p.target AS project,
    p.resource AS consultant,
    c.role,
    c.hourly_rate
FROM solution s,
     pgortools.parse_assignment(s.sol) p
JOIN consultants c ON c.name = p.resource
WHERE p.assigned = true
ORDER BY p.target,
    CASE c.role
        WHEN 'Junior' THEN 1
        WHEN 'Medior' THEN 2
        ELSE 3
    END;

-- =============================================================================
-- Compare: Procedural vs Declarative
-- =============================================================================
--
-- PROCEDURAL (consultant-assignment.sql):
--   - 27 add_bool_var() calls
--   - 18 add_constraint() calls
--   - Manual objective expression building
--   - ~100 lines of SQL
--
-- DECLARATIVE (this file):
--   - 1 solve_assignment() call
--   - Data lives in regular tables
--   - ~30 lines of SQL
--
-- The declarative API is ideal when:
--   - Your data already lives in tables
--   - You have a standard assignment/allocation problem
--   - You want to avoid repetitive constraint setup
--
-- Use the procedural API when:
--   - You need custom constraint types
--   - You have non-standard problem structures
--   - You want fine-grained control over the model

-- =============================================================================
-- Cleanup (optional)
-- =============================================================================
-- DROP TABLE IF EXISTS consultants CASCADE;
-- DROP TABLE IF EXISTS client_projects CASCADE;
-- SELECT pgortools.drop_problem('team_staffing');
-- SELECT pgortools.drop_problem('display_staffing');
