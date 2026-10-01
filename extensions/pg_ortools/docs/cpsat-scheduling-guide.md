# CP-SAT Scheduling in pg_ortools — User Guide

Schedule operations onto shared resources, entirely in SQL, solved inside PostgreSQL.
This guide is written around the **restaurant kitchen** case — sequencing tickets across
stations so nothing clashes, orders go out in turn, and food doesn't sit — but the surface
is general (any "assign start times to jobs on shared machines" problem).

> Requires the extension built with the `cpsat` feature. Verify with:
> ```sql
> SELECT proname FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
> WHERE n.nspname = 'pgortools' AND proname IN ('solve_cp','solve_cp_sync');
> ```
> Both rows present ⇒ you're good.

---

## 1. The mental model

You describe a problem as **operations** placed on a timeline, plus rules:

| Concept | In the kitchen | Function |
|---|---|---|
| **Interval** | an operation of fixed length (grill a steak: 6 min) whose *start time* the solver chooses | `add_interval_var` |
| **Resource** | a station with a **capacity** (one grill fits 2 pans → capacity 2; one plating pass → capacity 1) | `add_resource` |
| **Demand** | how much of a station an operation uses while it runs (usually 1) | `add_demand` |
| **Precedence** | "A must finish before B starts" (grill before plate; ticket-1 before ticket-2) | `add_precedence` |
| **Objective** | what "best" means — here, minimise how long a plated dish waits after grilling | `minimize_wait` |

The solver finds start times that satisfy **every** rule and minimise the objective. A
capacity-1 resource is automatically "one-at-a-time" (no overlap); capacity ≥ 2 lets that
many operations run at once.

**Everything is integers.** Pick a time unit (minutes here). Durations, start windows, gaps,
capacities, demands and weights are all whole numbers.

---

## 2. Function reference

All functions live in the `pgortools` schema.

| Function | Purpose |
|---|---|
| `create_problem(name)` | Start a new problem. |
| `add_interval_var(problem, name, duration, earliest_start, latest_end)` | An operation of length `duration`; its start is chosen in `[earliest_start, latest_end − duration]`. `earliest_start` = "not before" (ingredients ready / order fired); `latest_end` = "done by" (service deadline). |
| `add_resource(problem, name, capacity)` | A station. `capacity = 1` ⇒ one-at-a-time. |
| `add_demand(problem, resource, interval, demand)` | Put `interval` on `resource`, using `demand` units (default 1). |
| `add_precedence(problem, before, after, gap)` | `end[before] + gap ≤ start[after]` (default `gap = 0`). |
| `minimize_wait(problem, after, before, weight)` | Minimise `weight × (start[after] − end[before])` — how long `after` waits after `before` finishes (default `weight = 1`). Add several; they sum. |
| `solve_cp_sync(problem)` | Solve **now**, inline. Returns JSONB. |
| `solve_cp(problem)` | Solve **async** via the background worker. Returns a `job_id`. |
| `solve_status(job_id)` | Poll an async job's state. |
| `get_solution(problem)` | Fetch the most recent stored solution as JSONB. |
| `drop_problem(name)` | Delete the problem and everything attached to it. |

---

## 3. The restaurant case, end to end

**Scenario.** Three tickets during a rush. Each ticket is grill → plate. Two stations: a
**grill** that can cook two pans at once (capacity 2) and a single **pass** for plating
(capacity 1). Tickets are fired a couple of minutes apart, everything must be out within a
25-minute window, orders leave the pass in FIFO order, and we don't want a grilled dish
sitting before it's plated.

| Ticket | grill (min) | plate (min) | fired at |
|---|---|---|---|
| 1 | 6 | 2 | 0 |
| 2 | 4 | 2 | 2 |
| 3 | 8 | 3 | 4 |

```sql
SELECT pgortools.create_problem('dinner_rush');

-- Operations: add_interval_var(problem, name, duration, earliest_start, latest_end)
SELECT pgortools.add_interval_var('dinner_rush','t1_grill', 6, 0, 25);
SELECT pgortools.add_interval_var('dinner_rush','t1_plate', 2, 0, 25);
SELECT pgortools.add_interval_var('dinner_rush','t2_grill', 4, 2, 25);  -- fired at 2
SELECT pgortools.add_interval_var('dinner_rush','t2_plate', 2, 0, 25);
SELECT pgortools.add_interval_var('dinner_rush','t3_grill', 8, 4, 25);  -- fired at 4
SELECT pgortools.add_interval_var('dinner_rush','t3_plate', 3, 0, 25);

-- Stations: the grill fits 2 pans; the pass does one plate at a time
SELECT pgortools.add_resource('dinner_rush','grill', 2);
SELECT pgortools.add_resource('dinner_rush','pass',  1);
SELECT pgortools.add_demand('dinner_rush','grill','t1_grill', 1);
SELECT pgortools.add_demand('dinner_rush','grill','t2_grill', 1);
SELECT pgortools.add_demand('dinner_rush','grill','t3_grill', 1);
SELECT pgortools.add_demand('dinner_rush','pass', 't1_plate', 1);
SELECT pgortools.add_demand('dinner_rush','pass', 't2_plate', 1);
SELECT pgortools.add_demand('dinner_rush','pass', 't3_plate', 1);

-- Grill before plate within each ticket
SELECT pgortools.add_precedence('dinner_rush','t1_grill','t1_plate', 0);
SELECT pgortools.add_precedence('dinner_rush','t2_grill','t2_plate', 0);
SELECT pgortools.add_precedence('dinner_rush','t3_grill','t3_plate', 0);

-- FIFO: plate the tickets in order on the pass
SELECT pgortools.add_precedence('dinner_rush','t1_plate','t2_plate', 0);
SELECT pgortools.add_precedence('dinner_rush','t2_plate','t3_plate', 0);

-- Don't let a plated dish sit: minimise the grill→plate gap for each ticket
SELECT pgortools.minimize_wait('dinner_rush','t1_plate','t1_grill', 1);
SELECT pgortools.minimize_wait('dinner_rush','t2_plate','t2_grill', 1);
SELECT pgortools.minimize_wait('dinner_rush','t3_plate','t3_grill', 1);

SELECT pgortools.solve_cp_sync('dinner_rush');
```

### The result

```json
{
  "status": "OPTIMAL",
  "objective": 0,
  "intervals": {
    "t1_grill": {"start": 6,  "end": 12},
    "t1_plate": {"start": 12, "end": 14},
    "t2_grill": {"start": 14, "end": 18},
    "t2_plate": {"start": 18, "end": 20},
    "t3_grill": {"start": 12, "end": 20},
    "t3_plate": {"start": 20, "end": 23}
  }
}
```

**How to read it** — check the invariants, not the exact minutes:

- **`objective: 0`** — every plate starts the instant its grill finishes. No dish waits. (This is the *proven minimum*; `status: OPTIMAL` means the solver proved nothing better exists.)
- **Grill (capacity 2) never overloaded** — at minute 14–18, `t2_grill` and `t3_grill` run together = 2 pans, exactly the limit; `t1_grill` finished at 12.
- **Pass (capacity 1) is strictly serial and FIFO** — plates go `[12,14] → [18,20] → [20,23]`, ticket 1 then 2 then 3.
- **Fire times respected** — `t2_grill` starts ≥ 2, `t3_grill` starts ≥ 4.
- **Everything out by 23 ≤ 25** — inside the service window.

> The exact clock can differ if several schedules are equally optimal — what's guaranteed is
> the objective value and that every rule holds. Turn it into a plan with a plain query:
> ```sql
> SELECT key AS operation,
>        (value->>'start')::int AS start_min,
>        (value->>'end')::int   AS end_min
> FROM jsonb_each(pgortools.solve_cp_sync('dinner_rush')->'intervals')
> ORDER BY start_min;
> ```

Clean up when done: `SELECT pgortools.drop_problem('dinner_rush');`

---

## 4. Recipes

**One machine, no overlap** — a resource with capacity 1; give each operation demand 1.
```sql
SELECT pgortools.add_resource('p','oven', 1);
SELECT pgortools.add_demand('p','oven','bake_a', 1);
SELECT pgortools.add_demand('p','oven','bake_b', 1);   -- a and b can't overlap
```

**A machine that handles N at once** — capacity N. Bigger jobs can weigh more than 1:
```sql
SELECT pgortools.add_resource('p','fryer', 3);          -- 3 baskets
SELECT pgortools.add_demand('p','fryer','big_batch', 2);-- uses 2 of the 3
SELECT pgortools.add_demand('p','fryer','small', 1);
```

**Ordering / FIFO** — chain precedences: `a` before `b` before `c`.
```sql
SELECT pgortools.add_precedence('p','a','b', 0);
SELECT pgortools.add_precedence('p','b','c', 0);
```

**A minimum gap** — cool-down / hand-off time between two steps:
```sql
SELECT pgortools.add_precedence('p','fry','plate', 3);  -- plate ≥ 3 min after fry ends
```

**"Ready at" and "due by"** — use the interval's window: `earliest_start` = ready time,
`latest_end` = deadline. If a deadline can't be met the solve returns `INFEASIBLE` (see below).

**Don't let it sit / keep it hot** — `minimize_wait(after, before, weight)`. Use a bigger
`weight` for dishes that suffer most from waiting, so the solver prioritises them.

---

## 5. Sync vs. async

- **`solve_cp_sync(problem)`** runs inline and returns the JSON immediately. Best for
  interactive planning and small–medium problems. Nothing extra to configure.
- **`solve_cp(problem)`** hands the job to a background worker and returns a `job_id` right
  away — use it for large solves you don't want to block on:
  ```sql
  SELECT pgortools.solve_cp('dinner_rush');            -- returns e.g. 42
  SELECT state FROM pgortools.solve_status(42);        -- 'queued' → 'solving' → 'completed'
  SELECT pgortools.get_solution('dinner_rush');        -- the result, once completed
  ```
  The async worker only runs if the server was started with
  `shared_preload_libraries = 'pg_ortools'` and `pg_ortools.solver_database` pointing at your
  database (a restart is required). Without that, `solve_cp` queues a job that never runs —
  use `solve_cp_sync` instead.

---

## 6. Gotchas & current limits

- **Integers only.** Round durations/times to a unit (minutes, or 30-second ticks if you need
  finer granularity — just scale everything up).
- **Absolute times "float" under a pure wait objective.** `minimize_wait` only shrinks the
  *gaps* between steps, not the overall finish time. If you want the plan pulled as early as
  possible, anchor it: set realistic `earliest_start` (ready) and `latest_end` (deadline), as
  in the example. There is no makespan/tardiness objective yet.
- **`INFEASIBLE` means the rules can't all be met.** Common causes: a `latest_end` deadline
  too tight for the work + ordering, or a capacity-1 resource with more sequential work than
  the horizon allows. Loosen a deadline, add capacity, or relax a precedence.
- **Not yet available:** pinning an operation to a fixed start (frozen tasks), tardiness-vs-due
  penalties, and FIFO expressed as a soft penalty (today FIFO is a hard precedence). These are
  planned; ask if you need them.
- **Re-solving replaces the model.** The `add_*` calls accumulate rows; to re-plan a changed
  service, `drop_problem` and rebuild, or use a fresh problem name per run.

---

## 7. Copy-paste smoke test

Proves the engine is live (deterministic answer — objective 5):
```sql
SELECT pgortools.create_problem('smoke');
SELECT pgortools.add_interval_var('smoke','g', 2, 0, 30);
SELECT pgortools.add_interval_var('smoke','p', 2, 0, 30);
SELECT pgortools.add_precedence('smoke','g','p', 5);      -- p starts ≥ 5 after g ends
SELECT pgortools.minimize_wait('smoke','p','g', 1);
SELECT pgortools.solve_cp_sync('smoke');                  -- {"status":"OPTIMAL","objective":5,...}
SELECT pgortools.drop_problem('smoke');
```
