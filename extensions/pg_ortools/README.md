# pg_ortools

Constraint optimization using the HiGHS MIP solver directly in PostgreSQL, with async solving via background worker.

## Overview

pg_ortools brings optimization capabilities to PostgreSQL, allowing you to define and solve optimization problems using SQL. It uses the [HiGHS](https://highs.dev/) solver via the [good_lp](https://crates.io/crates/good_lp) Rust abstraction — no Python, no external dependencies.

Solving runs asynchronously in a background worker so it never blocks database connections. For quick problems or testing, a synchronous mode is also available.

## Installation

```bash
cargo pgrx install -p pg_ortools --pg-config /usr/lib/postgresql/16/bin/pg_config
```

Then in PostgreSQL:

```sql
CREATE EXTENSION pg_ortools;
```

No Python environment, no venv, no pip packages needed.

## Quick Start

```sql
-- Create a problem
SELECT pgortools.create_problem('example');

-- Add variables
SELECT pgortools.add_int_var('example', 'x', 0, 100);
SELECT pgortools.add_int_var('example', 'y', 0, 100);

-- Add constraints
SELECT pgortools.add_constraint('example', 'x + y <= 150');

-- Set objective
SELECT pgortools.maximize('example', '2*x + 3*y');

-- Solve asynchronously (returns job_id)
SELECT pgortools.solve('example');
-- Returns: 1

-- Check status
SELECT * FROM pgortools.solve_status(1);

-- Or listen for notifications
LISTEN pgortools_solve;

-- Get result after completion
SELECT pgortools.get_solution('example');
```

### Synchronous Mode

For quick problems or testing:

```sql
SELECT pgortools.solve_sync('example');
-- Returns: {"status": "OPTIMAL", "method": "optimal", "objective": 450.0, "values": {"x": 0, "y": 100}}
```

### Greedy Mode

Returns the first feasible solution without optimizing. Much faster — useful for simulation and feasibility checks:

```sql
SELECT pgortools.solve_greedy('example');
-- Returns: {"status": "OPTIMAL", "method": "greedy", "objective": ..., "values": {...}}
```

The greedy solver uses the same problem definition and constraints. The difference is it accepts any feasible solution instead of searching for the optimal one. Use `solve_sync` when you need the best answer, `solve_greedy` when you need a quick answer.

## Async Workflow

The recommended workflow uses the background worker:

```sql
-- 1. Submit a solve job
SELECT pgortools.solve('my_problem');  -- returns job_id (e.g., 42)

-- 2. Poll for status
SELECT * FROM pgortools.solve_status(42);
-- job_id | problem_name | state    | progress | current_step    | ...
-- 42     | my_problem   | solving  | 0.5      | Building model  | ...

-- 3. Or listen for completion notifications
LISTEN pgortools_solve;
-- Notification payload: {"job_id": 42, "state": "completed", "problem": "my_problem"}

-- 4. Cancel if needed
SELECT pgortools.cancel_solve(42);

-- 5. Get result
SELECT pgortools.get_solution('my_problem');
```

## Functions

### Problem Management

- `pgortools.create_problem(name)` - Create a new optimization problem
- `pgortools.drop_problem(name)` - Delete a problem and all its data

### Variables

- `pgortools.add_int_var(problem, name, min, max)` - Add an integer variable
- `pgortools.add_bool_var(problem, name)` - Add a boolean variable

### Constraints

- `pgortools.add_constraint(problem, expression)` - Add a constraint

Supported constraint syntax:
```sql
-- Arithmetic comparisons
'x + y <= 100'
'2*x - y >= 10'
'x == y'
'x = y'         -- same as ==

-- Variable bounds
'x >= 0'
'y <= 50'
```

**Note:** `!=` is not supported (MIP solver limitation). Use alternative formulations instead.

### Objectives

- `pgortools.maximize(problem, expression)` - Maximize an expression
- `pgortools.minimize(problem, expression)` - Minimize an expression

### Solving

- `pgortools.solve(problem)` - Submit async solve job, returns job_id (bigint)
- `pgortools.solve_sync(problem)` - Solve synchronously, returns optimal JSONB result
- `pgortools.solve_greedy(problem)` - Solve synchronously, returns first feasible solution (fast)
- `pgortools.solve_status(job_id)` - Get job status as a table row
- `pgortools.cancel_solve(job_id)` - Cancel a queued or running job
- `pgortools.get_solution(problem)` - Get the most recent solution

### Declarative API

- `pgortools.solve_assignment(problem, resources_table, name_col, group_col, cost_col, targets_table, target_col, group_per_target, objective)` - Solve assignment problems from tables (async, returns job_id)
- `pgortools.parse_assignment(solution)` - Parse solution JSONB into rows

## Declarative API Example

For common assignment problems:

```sql
-- Create data tables
CREATE TABLE consultants (name TEXT, role TEXT, hourly_rate INT);
CREATE TABLE projects (name TEXT);

INSERT INTO consultants VALUES
    ('Alice', 'Junior', 50), ('Bob', 'Junior', 55),
    ('Dave', 'Medior', 80), ('Eve', 'Medior', 85),
    ('Grace', 'Senior', 120), ('Henry', 'Senior', 125);

INSERT INTO projects VALUES ('Alpha'), ('Beta');

-- Solve: assign consultants to projects, 1 of each role per project
SELECT pgortools.solve_assignment(
    'team_staffing',
    'consultants', 'name', 'role', 'hourly_rate',
    'projects', 'name',
    1,
    'minimize'
);

-- After job completes, get and parse the solution
SELECT * FROM pgortools.parse_assignment(
    pgortools.get_solution('team_staffing')
) WHERE assigned = true;
```

## Configuration

```sql
-- Solver time limit (seconds, default: 300)
SET pg_ortools.solver_time_limit = 600;
```

The following settings require a PostgreSQL restart:

```sql
-- Enable/disable background worker (default: true)
-- Set in postgresql.conf:
-- pg_ortools.solver_worker_enabled = true

-- Job polling interval in milliseconds (default: 1000)
-- pg_ortools.solver_poll_interval = 1000

-- Database for worker connection (default: 'postgres')
-- pg_ortools.solver_database = 'mydb'
```

## Solution Format

The `solve_sync()` and `get_solution()` functions return JSONB:

```json
{
  "status": "Optimal",
  "objective": 450.0,
  "values": {
    "x": 0,
    "y": 100
  }
}
```

Status values:
- `Optimal` - Found the optimal solution
- `SubOptimal` - Found a feasible solution (may not be optimal)
- `Infeasible` - No solution exists
- `Unbounded` - The problem is unbounded

## Scheduling (CP-SAT)

Beyond linear MIP, pg_ortools has a constraint-programming engine for **scheduling**:
interval variables on shared resources with no-overlap / capacity, precedence, and a
wait-minimisation objective. It uses lazy clause generation (the pure-Rust
[Pumpkin](https://github.com/ConSol-Lab/Pumpkin) solver), so it scales on scheduling where a
time-indexed MIP would not. It is part of the default build (the `cpsat` cargo feature, on by
default); a build with `--no-default-features` must add it back, e.g.
`--no-default-features --features pg17,cpsat`.

| Function | Description |
|----------|-------------|
| `add_interval_var(problem, name, duration, earliest_start, latest_end)` | An operation occupying `[start, start+duration)`. |
| `add_resource(problem, name, capacity)` | A station; `capacity = 1` ⇒ no overlap. |
| `add_demand(problem, resource, interval, demand=1)` | Put an interval on a resource. |
| `add_precedence(problem, before, after, gap=0)` | `end[before] + gap ≤ start[after]` (FIFO, grill→plate). |
| `minimize_wait(problem, after, before, weight=1)` | Minimise `weight · (start[after] − end[before])`. |
| `solve_cp_sync(problem)` | Solve inline; returns `{status, objective, intervals:{name:{start,end}}}`. Bounded by `solver_time_limit` and responds to `statement_timeout` / cancel. |
| `solve_cp(problem, time_limit_seconds=NULL)` | Solve asynchronously via the background worker; returns a job_id (poll `solve_status`, read `get_solution`). `NULL` uses `solver_time_limit`. `cancel_solve(job_id)` stops it mid-search. |

CP scheduling proves optimality by search, which on a hard instance can run
effectively forever. Both entry points are therefore **time-bounded** — they
return the best schedule found so far (`status = FEASIBLE`) when the budget is
hit — and **cancellable**: `statement_timeout` / query-cancel stop a
`solve_cp_sync`, and `cancel_solve(job_id)` stops a running `solve_cp` within a
fraction of a second.

```sql
-- Kitchen: two tickets, one grill + one pass, grill→plate, don't let the plate sit
SELECT pgortools.create_problem('dinner_rush');
SELECT pgortools.add_interval_var('dinner_rush', 't1_grill', 8, 0, 60);
SELECT pgortools.add_interval_var('dinner_rush', 't1_plate', 2, 0, 60);
SELECT pgortools.add_resource('dinner_rush', 'grill', 1);
SELECT pgortools.add_demand('dinner_rush', 'grill', 't1_grill', 1);
SELECT pgortools.add_precedence('dinner_rush', 't1_grill', 't1_plate', 0);
SELECT pgortools.minimize_wait('dinner_rush', 't1_plate', 't1_grill', 1);
SELECT pgortools.solve_cp_sync('dinner_rush');
```

See [design/pg_ortools/cpsat-engine.md](../../design/pg_ortools/cpsat-engine.md) for the full design.

## Dependencies

- PostgreSQL 14, 15, 16, or 17
- Rust (build time only)
- No Python, no external runtime dependencies
- CP-SAT scheduling (`cpsat` feature, on by default): pure-Rust Pumpkin solver — still no external runtime

## License

Matroid Source Available License
