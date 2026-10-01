//! The ONLY file that imports Pumpkin. Maps [`Model`] → Pumpkin → [`SolveOutcome`].
//!
//! Isolating the solver here keeps Pumpkin's pre-1.0 API churn contained to one
//! place; the rest of the codebase depends only on `cpsat::Model`.
//!
//! Encoding (verified against Pumpkin 0.4.0):
//! - each interval → one integer `start` var over `[earliest_start, latest_end - duration]`;
//! - precedence `end[b] + gap <= start[a]` → a posted linear inequality;
//! - each cumulative resource → the native `cumulative` global constraint
//!   (no_overlap is just capacity 1 with unit demands);
//! - the wait objective → an integer objective var bound by `equals` to the
//!   weighted sum of `start[after] - end[before]` terms, minimised via LSU.

use std::ops::ControlFlow;

use super::model::{Model, Solution, SolveLimits, SolveOutcome};

use pumpkin_conflict_resolvers::resolvers::ResolutionResolver;
use pumpkin_constraints::{cumulative, equals, less_than_or_equals};
use pumpkin_core::optimisation::linear_sat_unsat::LinearSatUnsat;
use pumpkin_core::optimisation::OptimisationDirection;
use pumpkin_core::results::{
    OptimisationResult, ProblemSolution, SatisfactionResult, SolutionReference,
};
use pumpkin_core::termination::{TerminationCondition, TimeBudget};
use pumpkin_core::variables::TransformableVariable;
use pumpkin_core::{DefaultBrancher, Solver};

/// Adapts our engine-agnostic limits into a Pumpkin [`TerminationCondition`].
///
/// Pumpkin polls `should_stop` throughout the search, so this is the single hook
/// that makes a solve both **time-bounded** (via [`TimeBudget`]) and
/// **cancellable** (via the caller's `cancelled` closure). Without it the search
/// runs with `Indefinite` termination — a pure-CPU loop that never observes a
/// deadline, a cancelled job, or a Postgres interrupt.
struct Termination<'a> {
    budget: Option<TimeBudget>,
    cancelled: &'a mut dyn FnMut() -> bool,
}

impl TerminationCondition for Termination<'_> {
    fn should_stop(&mut self) -> bool {
        self.budget.as_mut().is_some_and(|b| b.should_stop()) || (self.cancelled)()
    }
}

pub fn solve(
    model: &Model,
    limits: &SolveLimits,
    cancelled: &mut dyn FnMut() -> bool,
) -> SolveOutcome {
    // Robustness guard #1: an interval whose window is smaller than its duration
    // (`latest_end - duration < earliest_start`) has an empty start domain.
    // Handing that to `new_bounded_integer` makes Pumpkin panic ("Cannot create
    // an empty domain"). It just means the operation cannot be placed → the model
    // is infeasible. Report that instead of crashing.
    if model
        .intervals()
        .iter()
        .any(|iv| iv.latest_end - iv.duration < iv.earliest_start)
    {
        return SolveOutcome::Infeasible;
    }

    let mut termination = Termination {
        budget: limits.time_limit.map(TimeBudget::starting_now),
        cancelled,
    };
    let mut solver = Solver::default();
    // Proof logging is off, so the constraint tag is a dummy shared by all constraints.
    let tag = solver.new_constraint_tag();

    // One integer start variable per interval.
    let starts: Vec<_> = model
        .intervals()
        .iter()
        .map(|iv| solver.new_bounded_integer(iv.earliest_start, iv.latest_end - iv.duration))
        .collect();

    // Precedence: end[before] + gap <= start[after]
    //   ⇔ start[before] - start[after] <= -(duration[before] + gap)
    for p in model.precedences() {
        let rhs = -(model.interval(p.before).duration + p.gap);
        let _ = solver
            .add_constraint(less_than_or_equals(
                vec![starts[p.before].scaled(1), starts[p.after].scaled(-1)],
                rhs,
                tag,
            ))
            .post();
    }

    // Cumulative resources (no_overlap is capacity 1 with unit demands).
    for res in model.cumulatives() {
        let group_starts: Vec<_> = res.intervals.iter().map(|&i| starts[i]).collect();
        let durations: Vec<i32> = res
            .intervals
            .iter()
            .map(|&i| model.interval(i).duration)
            .collect();
        let _ = solver
            .add_constraint(cumulative(
                group_starts,
                durations,
                res.demands.clone(),
                res.capacity,
                tag,
            ))
            .post();
    }

    // Robustness guard #2: posting the precedence/cumulative constraints can
    // propagate the root to an inconsistent state (e.g. a horizon too tight for
    // the cumulative demand). Any variable created afterwards — notably the
    // objective var below — would then trip Pumpkin's
    // `assert!(!is_inconsistent())` ("Variables cannot be created in an
    // inconsistent state"). A root-inconsistent model is simply infeasible, so
    // return that now rather than panic. (This is the failure `dispatch_cpsat`
    // hit when it built a tight-horizon model inside a transaction.)
    if solver.is_inconsistent() {
        return SolveOutcome::Infeasible;
    }

    let mut brancher = solver.default_brancher();
    let mut resolver = ResolutionResolver::default();

    if model.objective().is_empty() {
        return satisfy(
            &mut solver,
            &mut brancher,
            &mut resolver,
            &mut termination,
            &starts,
        );
    }

    // Objective var: obj == Σ_k weight_k * (start[after_k] - end[before_k]).
    // Rearranged as a linear equality `Σ terms = rhs`:
    //   obj - Σ w·start[after] + Σ w·start[before] = -Σ w·duration[before]
    // Element type (AffineView) is inferred from the pushes below.
    let mut terms = Vec::new();
    let mut rhs = 0i32;
    for w in model.objective() {
        terms.push(starts[w.after].scaled(-w.weight));
        terms.push(starts[w.before].scaled(w.weight));
        rhs -= w.weight * model.interval(w.before).duration;
    }

    // Bound the objective var. Waits are >= 0 under precedence; cap the upper
    // bound at Σ|weight| · max_horizon and allow a symmetric lower bound so the
    // domain is always valid even without precedence.
    let max_horizon = model
        .intervals()
        .iter()
        .map(|iv| iv.latest_end)
        .max()
        .unwrap_or(0);
    let weight_sum: i32 = model.objective().iter().map(|w| w.weight.abs()).sum();
    let bound = weight_sum.saturating_mul(max_horizon).max(1);
    let objective = solver.new_bounded_integer(-bound, bound);

    terms.push(objective.scaled(1));
    let _ = solver.add_constraint(equals(terms, rhs, tag)).post();

    // No-op callback: the `termination` below is what stops the search early
    // (deadline or cancel), surfacing as `OptimisationResult::Stopped` with the
    // best solution found so far.
    let callback = |_: &Solver,
                    _: SolutionReference,
                    _: &DefaultBrancher,
                    _: &ResolutionResolver|
     -> ControlFlow<()> { ControlFlow::Continue(()) };

    let result = solver.optimise(
        &mut brancher,
        &mut termination,
        &mut resolver,
        LinearSatUnsat::new(OptimisationDirection::Minimise, objective, callback),
    );

    match result {
        OptimisationResult::Optimal(solution) => {
            let obj = Some(solution.get_integer_value(objective) as i64);
            SolveOutcome::Optimal(read_solution(&solution, &starts, obj))
        }
        OptimisationResult::Satisfiable(solution) | OptimisationResult::Stopped(solution, _) => {
            let obj = Some(solution.get_integer_value(objective) as i64);
            SolveOutcome::Satisfiable(read_solution(&solution, &starts, obj))
        }
        OptimisationResult::Unsatisfiable => SolveOutcome::Infeasible,
        OptimisationResult::Unknown => SolveOutcome::Unknown,
    }
}

/// Satisfaction (no objective) path.
fn satisfy(
    solver: &mut Solver,
    brancher: &mut DefaultBrancher,
    resolver: &mut ResolutionResolver,
    termination: &mut Termination,
    starts: &[impl pumpkin_core::variables::IntegerVariable],
) -> SolveOutcome {
    // Bind the borrowing `SatisfactionResult` to a named local declared after
    // the borrowed args, so it drops before them at end of scope. On a deadline
    // or cancel this returns `Unknown` (no solution proven).
    let result = solver.satisfy(brancher, termination, resolver);
    match result {
        SatisfactionResult::Satisfiable(satisfiable) => {
            SolveOutcome::Satisfiable(read_solution(&satisfiable.solution(), starts, None))
        }
        SatisfactionResult::Unsatisfiable(..) => SolveOutcome::Infeasible,
        SatisfactionResult::Unknown(..) => SolveOutcome::Unknown,
    }
}

/// Read start times out of a Pumpkin solution, attaching a pre-read objective value.
fn read_solution(
    solution: &impl ProblemSolution,
    starts: &[impl pumpkin_core::variables::IntegerVariable],
    objective: Option<i64>,
) -> Solution {
    Solution {
        starts: starts
            .iter()
            .map(|v| solution.get_integer_value(v.clone()))
            .collect(),
        objective,
    }
}
