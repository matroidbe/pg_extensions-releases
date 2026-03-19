-- Optimize Each Scenario with Checkpointing
-- Solves assignment problem for each sampled scenario with resume capability

--------------------------------------------------------------------------------
-- Get current run (or resume a failed run)
--------------------------------------------------------------------------------

-- Try to get run_id from temp table (set by 02-sample-scenarios.sql)
-- Or find the most recent incomplete run to resume
DO $$
DECLARE
    v_run_id UUID;
BEGIN
    -- First try temp table from previous step
    BEGIN
        SELECT run_id INTO v_run_id FROM current_run LIMIT 1;
    EXCEPTION WHEN undefined_table THEN
        v_run_id := NULL;
    END;

    -- If no temp table, find most recent incomplete run
    IF v_run_id IS NULL THEN
        SELECT run_id INTO v_run_id
        FROM scenario_runs
        WHERE status IN ('running', 'failed')
        ORDER BY started_at DESC
        LIMIT 1;
    END IF;

    IF v_run_id IS NULL THEN
        RAISE EXCEPTION 'No active run found. Run 02-sample-scenarios.sql first.';
    END IF;

    -- Store for use in this session
    DROP TABLE IF EXISTS current_run;
    CREATE TEMP TABLE current_run AS SELECT v_run_id AS run_id;

    RAISE NOTICE 'Using run_id: %', v_run_id;
END $$;

--------------------------------------------------------------------------------
-- Optimization function (processes one scenario)
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION optimize_scenario(
    p_run_id UUID,
    p_sample_id INT
) RETURNS VOID AS $$
DECLARE
    v_solution JSONB;
    v_total_revenue NUMERIC := 0;
    v_total_cost NUMERIC := 0;
    v_projects_completed INT := 0;
    v_projects_unassigned INT := 0;
    v_assignment RECORD;
BEGIN
    -- Get projects ordered in this scenario
    -- Solve assignment: match consultants to projects by skill

    -- For this example, we use a simplified greedy assignment
    -- In production, replace with pg_ortools.solve_assignment()

    -- Create temp table for this scenario's projects
    CREATE TEMP TABLE IF NOT EXISTS tmp_scenario_projects (
        project_id INT,
        name TEXT,
        revenue NUMERIC,
        required_skill TEXT,
        effort_days INT,
        assigned_to INT,
        assigned BOOLEAN DEFAULT FALSE
    ) ON COMMIT DROP;

    DELETE FROM tmp_scenario_projects;

    INSERT INTO tmp_scenario_projects (project_id, name, revenue, required_skill, effort_days)
    SELECT p.project_id, p.name, p.revenue, p.required_skill, p.effort_days
    FROM scenario_samples ss
    JOIN potential_projects p ON p.project_id = ss.project_id
    WHERE ss.run_id = p_run_id
      AND ss.sample_id = p_sample_id
      AND ss.is_ordered = true;

    -- Create temp table for consultant availability
    CREATE TEMP TABLE IF NOT EXISTS tmp_consultant_avail (
        consultant_id INT,
        name TEXT,
        skill TEXT,
        daily_cost NUMERIC,
        remaining_days INT
    ) ON COMMIT DROP;

    DELETE FROM tmp_consultant_avail;

    INSERT INTO tmp_consultant_avail
    SELECT consultant_id, name, skill, daily_cost, available_days
    FROM consultants;

    -- Greedy assignment: assign highest-revenue projects first to available consultants
    -- (This simulates what pg_ortools.solve_assignment would do optimally)
    FOR v_assignment IN
        SELECT
            sp.project_id,
            sp.revenue,
            sp.effort_days,
            ca.consultant_id,
            ca.daily_cost
        FROM tmp_scenario_projects sp
        JOIN tmp_consultant_avail ca ON ca.skill = sp.required_skill
            AND ca.remaining_days >= sp.effort_days
        WHERE NOT sp.assigned
        ORDER BY sp.revenue DESC, ca.daily_cost ASC
    LOOP
        -- Check if project still unassigned and consultant still has capacity
        IF NOT EXISTS (
            SELECT 1 FROM tmp_scenario_projects
            WHERE project_id = v_assignment.project_id AND assigned
        ) AND EXISTS (
            SELECT 1 FROM tmp_consultant_avail
            WHERE consultant_id = v_assignment.consultant_id
              AND remaining_days >= v_assignment.effort_days
        ) THEN
            -- Make assignment
            UPDATE tmp_scenario_projects
            SET assigned = TRUE, assigned_to = v_assignment.consultant_id
            WHERE project_id = v_assignment.project_id;

            UPDATE tmp_consultant_avail
            SET remaining_days = remaining_days - v_assignment.effort_days
            WHERE consultant_id = v_assignment.consultant_id;

            v_total_revenue := v_total_revenue + v_assignment.revenue;
            v_total_cost := v_total_cost + (v_assignment.effort_days * v_assignment.daily_cost);
            v_projects_completed := v_projects_completed + 1;
        END IF;
    END LOOP;

    -- Count unassigned projects
    SELECT COUNT(*) INTO v_projects_unassigned
    FROM tmp_scenario_projects
    WHERE NOT assigned;

    -- Build solution JSON
    SELECT jsonb_build_object(
        'assignments', COALESCE(jsonb_agg(jsonb_build_object(
            'project_id', sp.project_id,
            'project_name', sp.name,
            'consultant_id', sp.assigned_to,
            'consultant_name', c.name,
            'revenue', sp.revenue,
            'cost', sp.effort_days * c.daily_cost
        )), '[]'::jsonb),
        'unassigned', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'project_id', project_id,
                'project_name', name,
                'required_skill', required_skill
            )), '[]'::jsonb)
            FROM tmp_scenario_projects WHERE NOT assigned
        )
    ) INTO v_solution
    FROM tmp_scenario_projects sp
    JOIN consultants c ON c.consultant_id = sp.assigned_to
    WHERE sp.assigned;

    -- Store result
    INSERT INTO scenario_results (
        run_id, sample_id, solution,
        total_revenue, total_cost, pnl,
        projects_completed, projects_unassigned
    ) VALUES (
        p_run_id, p_sample_id, v_solution,
        v_total_revenue, v_total_cost, v_total_revenue - v_total_cost,
        v_projects_completed, v_projects_unassigned
    )
    ON CONFLICT (run_id, sample_id) DO UPDATE SET
        solution = EXCLUDED.solution,
        total_revenue = EXCLUDED.total_revenue,
        total_cost = EXCLUDED.total_cost,
        pnl = EXCLUDED.pnl,
        projects_completed = EXCLUDED.projects_completed,
        projects_unassigned = EXCLUDED.projects_unassigned,
        computed_at = now();

END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- Main optimization loop with checkpointing
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_run_id UUID;
    v_total_samples INT;
    v_start_sample INT;
    v_sample_id INT;
    v_batch_size INT := 100;  -- Update checkpoint every N samples
    v_processed INT := 0;
    v_start_time TIMESTAMPTZ;
BEGIN
    -- Get run info
    SELECT run_id INTO v_run_id FROM current_run;

    SELECT total_samples, COALESCE(last_sample_id, 0)
    INTO v_total_samples, v_start_sample
    FROM scenario_runs
    WHERE run_id = v_run_id;

    v_start_time := clock_timestamp();

    RAISE NOTICE 'Starting optimization from sample % to %', v_start_sample + 1, v_total_samples;

    -- Update run status
    UPDATE scenario_runs
    SET status = 'running', updated_at = now()
    WHERE run_id = v_run_id;

    -- Process each sample
    FOR v_sample_id IN v_start_sample + 1 .. v_total_samples LOOP
        BEGIN
            -- Optimize this scenario
            PERFORM optimize_scenario(v_run_id, v_sample_id);

            v_processed := v_processed + 1;

            -- Checkpoint every batch_size samples
            IF v_processed % v_batch_size = 0 THEN
                UPDATE scenario_runs
                SET
                    completed_samples = v_sample_id,
                    last_sample_id = v_sample_id,
                    updated_at = now()
                WHERE run_id = v_run_id;

                RAISE NOTICE 'Checkpoint: % / % samples (% elapsed)',
                    v_sample_id, v_total_samples,
                    clock_timestamp() - v_start_time;

                -- Commit checkpoint (in a real scenario, this would be separate transactions)
                -- For this example, we're in a single transaction
            END IF;

        EXCEPTION WHEN OTHERS THEN
            -- Record failure and re-raise
            UPDATE scenario_runs
            SET
                status = 'failed',
                last_error = SQLERRM,
                last_sample_id = v_sample_id - 1,
                completed_samples = v_sample_id - 1,
                updated_at = now()
            WHERE run_id = v_run_id;

            RAISE EXCEPTION 'Failed at sample %: %', v_sample_id, SQLERRM;
        END;
    END LOOP;

    -- Mark complete
    UPDATE scenario_runs
    SET
        status = 'completed',
        completed_samples = v_total_samples,
        last_sample_id = v_total_samples,
        updated_at = now()
    WHERE run_id = v_run_id;

    RAISE NOTICE 'Optimization complete: % samples in %',
        v_total_samples, clock_timestamp() - v_start_time;

END $$;

--------------------------------------------------------------------------------
-- Summary of optimization run
--------------------------------------------------------------------------------

SELECT 'Optimization Summary' AS info;

SELECT
    sr.run_id,
    sr.status,
    sr.completed_samples,
    sr.total_samples,
    sr.updated_at - sr.started_at AS duration,
    AVG(res.pnl)::numeric(12,2) AS avg_pnl,
    MIN(res.pnl)::numeric(12,2) AS min_pnl,
    MAX(res.pnl)::numeric(12,2) AS max_pnl
FROM scenario_runs sr
JOIN current_run cr ON cr.run_id = sr.run_id
LEFT JOIN scenario_results res ON res.run_id = sr.run_id
GROUP BY sr.run_id, sr.status, sr.completed_samples, sr.total_samples,
         sr.updated_at, sr.started_at;
