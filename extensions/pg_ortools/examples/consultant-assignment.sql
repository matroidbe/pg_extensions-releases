-- =============================================================================
-- Consultant Assignment Problem
-- =============================================================================
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
-- Consultants Available:
--   Juniors:  Alice ($50/hr), Bob ($55/hr), Carol ($48/hr)
--   Mediors:  Dave ($80/hr), Eve ($85/hr), Frank ($78/hr)
--   Seniors:  Grace ($120/hr), Henry ($125/hr), Ivan ($115/hr)
--
-- =============================================================================

-- Create the optimization problem
SELECT pgortools.create_problem('consultant_assignment');

-- =============================================================================
-- Decision Variables
-- =============================================================================
-- Boolean variable: consultant_project = 1 if consultant assigned to project

-- Junior assignments
SELECT pgortools.add_bool_var('consultant_assignment', 'alice_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'alice_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'alice_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'bob_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'bob_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'bob_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'carol_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'carol_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'carol_gamma');

-- Medior assignments
SELECT pgortools.add_bool_var('consultant_assignment', 'dave_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'dave_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'dave_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'eve_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'eve_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'eve_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'frank_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'frank_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'frank_gamma');

-- Senior assignments
SELECT pgortools.add_bool_var('consultant_assignment', 'grace_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'grace_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'grace_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'henry_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'henry_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'henry_gamma');
SELECT pgortools.add_bool_var('consultant_assignment', 'ivan_alpha');
SELECT pgortools.add_bool_var('consultant_assignment', 'ivan_beta');
SELECT pgortools.add_bool_var('consultant_assignment', 'ivan_gamma');

-- =============================================================================
-- Constraints: Each consultant works on exactly 1 project
-- =============================================================================

SELECT pgortools.add_constraint('consultant_assignment', 'alice_alpha + alice_beta + alice_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'bob_alpha + bob_beta + bob_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'carol_alpha + carol_beta + carol_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'dave_alpha + dave_beta + dave_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'eve_alpha + eve_beta + eve_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'frank_alpha + frank_beta + frank_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'grace_alpha + grace_beta + grace_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'henry_alpha + henry_beta + henry_gamma == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'ivan_alpha + ivan_beta + ivan_gamma == 1');

-- =============================================================================
-- Constraints: Each project has exactly 1 of each role
-- =============================================================================

-- Each project has exactly 1 Junior
SELECT pgortools.add_constraint('consultant_assignment', 'alice_alpha + bob_alpha + carol_alpha == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'alice_beta + bob_beta + carol_beta == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'alice_gamma + bob_gamma + carol_gamma == 1');

-- Each project has exactly 1 Medior
SELECT pgortools.add_constraint('consultant_assignment', 'dave_alpha + eve_alpha + frank_alpha == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'dave_beta + eve_beta + frank_beta == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'dave_gamma + eve_gamma + frank_gamma == 1');

-- Each project has exactly 1 Senior
SELECT pgortools.add_constraint('consultant_assignment', 'grace_alpha + henry_alpha + ivan_alpha == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'grace_beta + henry_beta + ivan_beta == 1');
SELECT pgortools.add_constraint('consultant_assignment', 'grace_gamma + henry_gamma + ivan_gamma == 1');

-- =============================================================================
-- Objective: Minimize total hourly cost
-- =============================================================================

SELECT pgortools.minimize('consultant_assignment',
    '50*alice_alpha + 50*alice_beta + 50*alice_gamma + ' ||
    '55*bob_alpha + 55*bob_beta + 55*bob_gamma + ' ||
    '48*carol_alpha + 48*carol_beta + 48*carol_gamma + ' ||
    '80*dave_alpha + 80*dave_beta + 80*dave_gamma + ' ||
    '85*eve_alpha + 85*eve_beta + 85*eve_gamma + ' ||
    '78*frank_alpha + 78*frank_beta + 78*frank_gamma + ' ||
    '120*grace_alpha + 120*grace_beta + 120*grace_gamma + ' ||
    '125*henry_alpha + 125*henry_beta + 125*henry_gamma + ' ||
    '115*ivan_alpha + 115*ivan_beta + 115*ivan_gamma'
);

-- =============================================================================
-- Solve the problem
-- =============================================================================

SELECT pgortools.solve('consultant_assignment');

-- =============================================================================
-- Display results nicely
-- =============================================================================

WITH solution AS (
    SELECT pgortools.get_solution('consultant_assignment') as sol
),
assignments AS (
    SELECT
        key as assignment,
        value::int as assigned
    FROM solution, jsonb_each_text(sol)
    WHERE value::int = 1
)
SELECT
    CASE
        WHEN assignment LIKE '%_alpha' THEN 'Alpha'
        WHEN assignment LIKE '%_beta' THEN 'Beta'
        WHEN assignment LIKE '%_gamma' THEN 'Gamma'
    END as project,
    CASE
        WHEN assignment LIKE 'alice_%' THEN 'Alice (Junior, $50/hr)'
        WHEN assignment LIKE 'bob_%' THEN 'Bob (Junior, $55/hr)'
        WHEN assignment LIKE 'carol_%' THEN 'Carol (Junior, $48/hr)'
        WHEN assignment LIKE 'dave_%' THEN 'Dave (Medior, $80/hr)'
        WHEN assignment LIKE 'eve_%' THEN 'Eve (Medior, $85/hr)'
        WHEN assignment LIKE 'frank_%' THEN 'Frank (Medior, $78/hr)'
        WHEN assignment LIKE 'grace_%' THEN 'Grace (Senior, $120/hr)'
        WHEN assignment LIKE 'henry_%' THEN 'Henry (Senior, $125/hr)'
        WHEN assignment LIKE 'ivan_%' THEN 'Ivan (Senior, $115/hr)'
    END as consultant,
    CASE
        WHEN assignment LIKE 'alice_%' OR assignment LIKE 'bob_%' OR assignment LIKE 'carol_%' THEN 'Junior'
        WHEN assignment LIKE 'dave_%' OR assignment LIKE 'eve_%' OR assignment LIKE 'frank_%' THEN 'Medior'
        ELSE 'Senior'
    END as role
FROM assignments
ORDER BY project,
    CASE
        WHEN assignment LIKE 'alice_%' OR assignment LIKE 'bob_%' OR assignment LIKE 'carol_%' THEN 1
        WHEN assignment LIKE 'dave_%' OR assignment LIKE 'eve_%' OR assignment LIKE 'frank_%' THEN 2
        ELSE 3
    END;

-- Show summary
SELECT
    'Total hourly cost: $' || objective_value::int || ' (solved in ' || solve_time_ms || ' ms)' as summary
FROM pgortools.solutions s
JOIN pgortools.problems p ON s.problem_id = p.id
WHERE p.name = 'consultant_assignment'
ORDER BY s.solved_at DESC LIMIT 1;

-- =============================================================================
-- Cleanup (optional)
-- =============================================================================
-- SELECT pgortools.drop_problem('consultant_assignment');
