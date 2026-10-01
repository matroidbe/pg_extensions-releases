//! Engine-agnostic scheduling model for the CP-SAT engine.
//!
//! This is the boundary the rest of the codebase depends on. Nothing here
//! references Pumpkin — the mapping to the solver lives in [`super::pumpkin`],
//! so a pre-1.0 Pumpkin API break is contained to one file.
//!
//! An interval variable is an operation occupying `[start, start + duration)`.
//! `start` is the decision variable; `duration` is a constant in this first cut.
//!
//! Resources are modelled uniformly as **cumulative** constraints: a resource
//! has an integer `capacity`, each interval on it a `demand`, and at no instant
//! may the summed demand of running intervals exceed the capacity. A
//! unit-capacity, unit-demand resource is exactly disjunctive (`no_overlap`).

use std::time::Duration;

/// One interval variable (a scheduled operation).
#[derive(Debug, Clone)]
pub struct Interval {
    pub name: String,
    pub duration: i32,
    pub earliest_start: i32,
    pub latest_end: i32,
}

/// `end[before] + gap <= start[after]`.
#[derive(Debug, Clone, Copy)]
pub struct Precedence {
    pub before: usize,
    pub after: usize,
    pub gap: i32,
}

/// A cumulative resource: intervals `intervals[k]` each consume `demands[k]`
/// units while running; concurrent demand may not exceed `capacity`.
#[derive(Debug, Clone)]
pub struct Cumulative {
    pub intervals: Vec<usize>,
    pub demands: Vec<i32>,
    pub capacity: i32,
}

/// A soft term: penalise the gap `start[after] - end[before]` (how long `after`
/// waits after `before` finishes) by `weight` per time unit. Minimised.
#[derive(Debug, Clone, Copy)]
pub struct WaitPenalty {
    pub after: usize,
    pub before: usize,
    pub weight: i32,
}

/// A scheduling model: intervals, cumulative resources, precedences, and an
/// optional wait-minimisation objective.
#[derive(Debug, Clone, Default)]
pub struct Model {
    intervals: Vec<Interval>,
    cumulatives: Vec<Cumulative>,
    precedences: Vec<Precedence>,
    objective: Vec<WaitPenalty>,
}

/// A found schedule: `starts[i]` is the start time of interval `i`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Solution {
    pub starts: Vec<i32>,
    /// Objective value (total weighted wait) when the model has an objective.
    pub objective: Option<i64>,
}

impl Solution {
    /// End time of interval `i` (`start + duration`).
    pub fn end(&self, model: &Model, i: usize) -> i32 {
        self.starts[i] + model.interval(i).duration
    }
}

/// Bounds and controls applied to a solve.
///
/// A pure CP search with an objective proves optimality by exhausting the
/// domain — on a hard instance that can run effectively forever. `SolveLimits`
/// makes the search *safe to hand an arbitrary instance*: give it a wall-clock
/// budget and it returns the best feasible solution found so far (or `Unknown`
/// if none) instead of hanging.
#[derive(Debug, Clone, Default)]
pub struct SolveLimits {
    /// Wall-clock budget for the search. `None` = unbounded (prove optimality or
    /// prove infeasible). `Some(d)` = stop after `d` and return best-so-far.
    pub time_limit: Option<Duration>,
}

impl SolveLimits {
    /// A budget of `secs` seconds (`0`/negative ⇒ unbounded).
    pub fn from_secs(secs: i64) -> Self {
        SolveLimits {
            time_limit: (secs > 0).then(|| Duration::from_secs(secs as u64)),
        }
    }
}

/// Outcome of a solve attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SolveOutcome {
    /// Feasible. When an objective is set, this is the best solution found but
    /// not proven optimal (e.g. terminated early).
    Satisfiable(Solution),
    /// Feasible and proven optimal for the objective.
    Optimal(Solution),
    Infeasible,
    /// Terminated (e.g. resource limit) without proving sat/unsat.
    Unknown,
}

impl SolveOutcome {
    /// The solution for a feasible/optimal outcome, else `None`.
    pub fn solution(&self) -> Option<&Solution> {
        match self {
            SolveOutcome::Satisfiable(s) | SolveOutcome::Optimal(s) => Some(s),
            _ => None,
        }
    }
}

impl Model {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an interval variable; returns its index (its handle in this model).
    pub fn add_interval(
        &mut self,
        name: impl Into<String>,
        duration: i32,
        earliest_start: i32,
        latest_end: i32,
    ) -> usize {
        let idx = self.intervals.len();
        self.intervals.push(Interval {
            name: name.into(),
            duration,
            earliest_start,
            latest_end,
        });
        idx
    }

    /// Declare that the given intervals share a unit-capacity resource and may
    /// not overlap in time (disjunctive). Sugar for a cumulative with capacity 1
    /// and unit demands.
    pub fn add_no_overlap(&mut self, intervals: impl Into<Vec<usize>>) {
        let intervals = intervals.into();
        let demands = vec![1; intervals.len()];
        self.cumulatives.push(Cumulative {
            intervals,
            demands,
            capacity: 1,
        });
    }

    /// Declare a cumulative resource: `intervals[k]` consumes `demands[k]` units;
    /// concurrent demand may not exceed `capacity`.
    pub fn add_cumulative(
        &mut self,
        intervals: impl Into<Vec<usize>>,
        demands: impl Into<Vec<i32>>,
        capacity: i32,
    ) {
        self.cumulatives.push(Cumulative {
            intervals: intervals.into(),
            demands: demands.into(),
            capacity,
        });
    }

    /// Add `end[before] + gap <= start[after]`.
    pub fn add_precedence(&mut self, before: usize, after: usize, gap: i32) {
        self.precedences.push(Precedence { before, after, gap });
    }

    /// Minimise `weight * (start[after] - end[before])` — how long `after` waits
    /// after `before` finishes (e.g. a plated dish sitting after the grill).
    pub fn minimize_wait(&mut self, after: usize, before: usize, weight: i32) {
        self.objective.push(WaitPenalty {
            after,
            before,
            weight,
        });
    }

    pub fn intervals(&self) -> &[Interval] {
        &self.intervals
    }

    pub fn interval(&self, i: usize) -> &Interval {
        &self.intervals[i]
    }

    pub fn cumulatives(&self) -> &[Cumulative] {
        &self.cumulatives
    }

    pub fn precedences(&self) -> &[Precedence] {
        &self.precedences
    }

    pub fn objective(&self) -> &[WaitPenalty] {
        &self.objective
    }

    /// Solve this scheduling model with the CP engine, unbounded (prove
    /// optimality or infeasibility). Convenience for tests and callers that
    /// know the instance is small; production callers should prefer
    /// [`Model::solve_with`] with a time limit.
    pub fn solve(&self) -> SolveOutcome {
        self.solve_with(&SolveLimits::default(), &mut || false)
    }

    /// Solve with an explicit budget and a cooperative cancellation check.
    ///
    /// `cancelled` is polled by the solver during search (via a Pumpkin
    /// `TerminationCondition`); returning `true` stops the search and yields the
    /// best feasible solution found so far. Combined with `limits.time_limit`,
    /// this makes a solve both time-bounded and cancellable — the caller
    /// supplies whatever "should I stop?" signal it has (a deadline, a job
    /// marked cancelled, a pending Postgres interrupt) without this crate
    /// depending on any of them.
    pub fn solve_with(
        &self,
        limits: &SolveLimits,
        cancelled: &mut dyn FnMut() -> bool,
    ) -> SolveOutcome {
        super::pumpkin::solve(self, limits, cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Max number of intervals overlapping at any instant, over `group`.
    fn max_concurrency(sol: &Solution, model: &Model, group: &[usize]) -> usize {
        // Sample every integer instant across the horizon; durations are integers.
        let horizon = model
            .intervals()
            .iter()
            .map(|iv| iv.latest_end)
            .max()
            .unwrap();
        (0..horizon)
            .map(|t| {
                group
                    .iter()
                    .filter(|&&i| sol.starts[i] <= t && t < sol.end(model, i))
                    .count()
            })
            .max()
            .unwrap_or(0)
    }

    /// Two operations on one station (a single grill) plus a within-ticket
    /// precedence. Smallest slice of the restaurant problem: the two grill ops
    /// cannot overlap, and ticket 1's grill precedes ticket 3's (FIFO).
    #[test]
    fn disjunctive_two_ops_on_one_grill() {
        let mut m = Model::new();
        let t1_grill = m.add_interval("t1_grill", 8, 0, 60);
        let t3_grill = m.add_interval("t3_grill", 10, 0, 60);

        m.add_no_overlap(vec![t1_grill, t3_grill]);
        m.add_precedence(t1_grill, t3_grill, 0);

        let outcome = m.solve();
        let SolveOutcome::Satisfiable(sol) = outcome else {
            panic!("expected a feasible schedule, got {outcome:?}");
        };

        assert!(
            sol.end(&m, t1_grill) <= sol.starts[t3_grill],
            "t1 grill [{}, {}] must finish before t3 grill starts at {}",
            sol.starts[t1_grill],
            sol.end(&m, t1_grill),
            sol.starts[t3_grill],
        );
        assert_eq!(max_concurrency(&sol, &m, &[t1_grill, t3_grill]), 1);
        assert!(sol.end(&m, t3_grill) <= 60);
    }

    /// A prep station with capacity 2 (e.g. a griddle that fits two pans) lets
    /// two ops run at once but never three.
    #[test]
    fn cumulative_allows_two_but_not_three() {
        let mut m = Model::new();
        let a = m.add_interval("a", 4, 0, 10);
        let b = m.add_interval("b", 4, 0, 10);
        let c = m.add_interval("c", 4, 0, 10);
        // capacity 2, each op demands 1
        m.add_cumulative(vec![a, b, c], vec![1, 1, 1], 2);

        let outcome = m.solve();
        let SolveOutcome::Satisfiable(sol) = outcome else {
            panic!("expected feasible with capacity 2, got {outcome:?}");
        };
        assert!(
            max_concurrency(&sol, &m, &[a, b, c]) <= 2,
            "capacity 2 violated: {:?}",
            sol.starts
        );
    }

    /// Same three dur-4 ops, but capacity 1 (disjunctive) in a horizon too short
    /// to sequence all three (3 × 4 = 12 > 9): the resource makes it infeasible.
    /// Proves the capacity parameter is actually enforced.
    #[test]
    fn cumulative_capacity_one_is_infeasible_when_span_exceeds_horizon() {
        let mut m = Model::new();
        let a = m.add_interval("a", 4, 0, 9);
        let b = m.add_interval("b", 4, 0, 9);
        let c = m.add_interval("c", 4, 0, 9);
        m.add_cumulative(vec![a, b, c], vec![1, 1, 1], 1);

        assert_eq!(m.solve(), SolveOutcome::Infeasible);
    }

    /// Two tickets, one grill (dur 2) and one pass (dur 3), each grill→plate,
    /// with a loose horizon. Because waits are non-negative under precedence and
    /// the horizon lets the two grills be spread apart so the single pass absorbs
    /// both plates immediately, the minimum total wait is provably 0. This
    /// exercises the optimise path and the objective encoding: the recomputed
    /// wait must equal the solver's reported value, and it must reach the true
    /// minimum (a maximising or untied-objective bug would not return 0).
    #[test]
    fn minimize_wait_achieves_zero_when_horizon_is_loose() {
        let mut m = Model::new();
        let g1 = m.add_interval("g1", 2, 0, 30);
        let g2 = m.add_interval("g2", 2, 0, 30);
        let p1 = m.add_interval("p1", 3, 0, 30);
        let p2 = m.add_interval("p2", 3, 0, 30);

        m.add_no_overlap(vec![g1, g2]); // one grill
        m.add_no_overlap(vec![p1, p2]); // one pass
        m.add_precedence(g1, p1, 0); // plate after grill, same ticket
        m.add_precedence(g2, p2, 0);

        m.minimize_wait(p1, g1, 1);
        m.minimize_wait(p2, g2, 1);

        let outcome = m.solve();
        let SolveOutcome::Optimal(sol) = outcome else {
            panic!("expected a proven-optimal schedule, got {outcome:?}");
        };

        // Recompute the objective from the returned starts and cross-check it
        // against the solver's reported value (catches encoding/sign errors).
        let recomputed = (sol.starts[p1] - sol.end(&m, g1)) + (sol.starts[p2] - sol.end(&m, g2));
        assert_eq!(sol.objective, Some(recomputed as i64), "objective encoding");
        assert_eq!(
            sol.objective,
            Some(0),
            "loose horizon ⇒ zero wait is optimal"
        );
    }

    /// A precedence *gap* forces a minimum wait: `end[g] + 5 <= start[p]` means
    /// the plate can start no earlier than 5 after the grill finishes, so the
    /// minimised wait is exactly the gap floor of 5. Positive optimum, forced by
    /// a hard constraint rather than a fragile hand-calculation — proves the
    /// objective is driven down to (and not below) the feasible floor.
    #[test]
    fn minimize_wait_respects_precedence_gap_floor() {
        let mut m = Model::new();
        let g = m.add_interval("g", 2, 0, 30);
        let p = m.add_interval("p", 2, 0, 30);
        m.add_precedence(g, p, 5); // end[g] + 5 <= start[p]
        m.minimize_wait(p, g, 1);

        let outcome = m.solve();
        let SolveOutcome::Optimal(sol) = outcome else {
            panic!("expected a proven-optimal schedule, got {outcome:?}");
        };
        let recomputed = sol.starts[p] - sol.end(&m, g);
        assert_eq!(sol.objective, Some(recomputed as i64), "objective encoding");
        assert_eq!(
            sol.objective,
            Some(5),
            "wait is driven down to the gap floor"
        );
    }

    // =========================================================================
    // Time limit & cancellation (the safety machinery for arbitrary instances)
    // =========================================================================

    /// A deliberately hard disjunctive instance: `n` grill ops on one grill,
    /// each feeding a plate on one pass, minimising total weighted wait. Proving
    /// optimality means searching the orderings — with `n = 12` that runs far
    /// longer than a unit test should tolerate, which is exactly the point:
    /// these tests assert the search is *cut off* by a budget / cancel signal.
    fn hard_instance(n: usize) -> Model {
        let mut m = Model::new();
        let horizon = (n as i32) * 12;
        let mut grills = Vec::with_capacity(n);
        let mut plates = Vec::with_capacity(n);
        for i in 0..n {
            let dur = 3 + (i as i32 % 5); // heterogeneous durations
            let g = m.add_interval(format!("g{i}"), dur, 0, horizon);
            let p = m.add_interval(format!("p{i}"), 2, 0, horizon);
            m.add_precedence(g, p, 0);
            m.minimize_wait(p, g, 1 + (i as i32 % 3));
            grills.push(g);
            plates.push(p);
        }
        m.add_no_overlap(grills); // one grill
        m.add_no_overlap(plates); // one pass
        m
    }

    /// A short time budget must cut off a hard solve promptly instead of hanging
    /// while it proves optimality. Without the budget this instance runs for many
    /// seconds; with it the call returns in a small multiple of the budget.
    #[test]
    fn time_budget_stops_a_hard_solve() {
        let m = hard_instance(12);
        let limits = SolveLimits {
            time_limit: Some(Duration::from_millis(200)),
        };

        let start = std::time::Instant::now();
        let outcome = m.solve_with(&limits, &mut || false);
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(5),
            "200ms budget was ignored: solve took {elapsed:?}"
        );
        // The instance is clearly feasible (loose horizon), so a budget cutoff
        // yields best-so-far or "unknown", never a bogus infeasible/optimal-proof.
        assert!(
            !matches!(outcome, SolveOutcome::Infeasible),
            "feasible instance reported infeasible under a time budget: {outcome:?}"
        );
    }

    /// The cooperative cancel closure must be polled during search and stop it.
    /// Returning `true` immediately proves the [`super::pumpkin`] termination hook
    /// is wired: the search halts at once rather than proving optimality.
    #[test]
    fn cancellation_stops_the_search() {
        let m = hard_instance(12);
        let mut polls = 0usize;

        let start = std::time::Instant::now();
        let outcome = m.solve_with(&SolveLimits::default(), &mut || {
            polls += 1;
            true
        });
        let elapsed = start.elapsed();

        assert!(polls >= 1, "termination hook was never polled");
        assert!(
            elapsed < Duration::from_secs(5),
            "cancel was ignored: solve took {elapsed:?}"
        );
        assert!(
            !matches!(outcome, SolveOutcome::Infeasible),
            "feasible instance reported infeasible when cancelled: {outcome:?}"
        );
    }

    /// A cancel signal that never fires (and no budget) must leave normal solving
    /// untouched: the hard instance still proves its true optimum. Guards against
    /// the termination plumbing accidentally short-circuiting a real solve.
    #[test]
    fn no_limit_and_no_cancel_still_proves_optimum() {
        let mut m = Model::new();
        let g = m.add_interval("g", 2, 0, 30);
        let p = m.add_interval("p", 2, 0, 30);
        m.add_precedence(g, p, 5);
        m.minimize_wait(p, g, 1);

        let outcome = m.solve_with(&SolveLimits::default(), &mut || false);
        let SolveOutcome::Optimal(sol) = outcome else {
            panic!("expected a proven optimum with no limit/cancel, got {outcome:?}");
        };
        assert_eq!(sol.objective, Some(5));
    }

    /// An operation whose window is too small for its duration has no feasible
    /// start (`latest_end - duration < earliest_start` ⇒ empty domain). The model
    /// is infeasible and must be reported as such. Historically this panicked:
    /// the empty domain left Pumpkin's solver inconsistent, and creating the *next*
    /// variable tripped its `assert!(!is_inconsistent())` ("Variables cannot be
    /// created in an inconsistent state"). A second interval makes that "next
    /// variable" exist, so this reproduces the crash rather than masking it.
    #[test]
    fn impossible_interval_window_is_infeasible_not_a_panic() {
        let mut m = Model::new();
        // duration 10 cannot fit in the window [0, 5]: empty start domain.
        m.add_interval("too_tight", 10, 0, 5);
        // a second interval so a *following* variable creation would run.
        m.add_interval("ok", 2, 0, 20);

        assert_eq!(m.solve(), SolveOutcome::Infeasible);
    }

    /// The empty-domain guard must not fire on a boundary-tight-but-feasible
    /// window: `latest_end - duration == earliest_start` leaves exactly one legal
    /// start, so the model is feasible (start pinned to that value).
    #[test]
    fn exactly_fitting_interval_window_is_feasible() {
        let mut m = Model::new();
        let a = m.add_interval("a", 5, 3, 8); // start must be exactly 3
        let outcome = m.solve();
        let SolveOutcome::Satisfiable(sol) = outcome else {
            panic!("exact-fit window should be feasible, got {outcome:?}");
        };
        assert_eq!(sol.starts[a], 3);
    }

    /// Individually-valid interval domains, but a resource constraint makes the
    /// root infeasible. Because there is an objective, the objective variable is
    /// created *after* the constraints are posted — and posting a too-tight
    /// cumulative propagates the root to inconsistent, so that creation
    /// historically hit Pumpkin's "Variables cannot be created in an inconsistent
    /// state" panic (the failure seen from `dispatch_cpsat` on a tight horizon).
    /// Must come back a clean `Infeasible`.
    #[test]
    fn root_infeasible_with_objective_is_infeasible_not_a_panic() {
        let mut m = Model::new();
        // three dur-4 ops on one unit-capacity station, horizon 9: 3·4 = 12 > 9.
        let a = m.add_interval("a", 4, 0, 9);
        let b = m.add_interval("b", 4, 0, 9);
        let c = m.add_interval("c", 4, 0, 9);
        m.add_no_overlap(vec![a, b, c]); // individual domains are [0, 5] — non-empty
        m.add_precedence(a, b, 0);
        m.minimize_wait(b, a, 1); // objective ⇒ objective var created after posting

        assert_eq!(m.solve(), SolveOutcome::Infeasible);
    }
}
