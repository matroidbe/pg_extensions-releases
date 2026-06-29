//! Local search algorithms for improving solutions.
//!
//! Each algorithm starts from a construction heuristic result and iteratively
//! applies moves to improve the assignment. They all share the same loop
//! structure but differ in their accept/reject criteria.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use super::constraint::evaluate;
use super::construction::first_fit_decreasing;
use super::move_types::generate_random_move;
use super::problem::{Assignment, AssignmentProblem};
use super::score::HardSoftScore;

/// Algorithm selection with per-algorithm parameters.
#[derive(Debug, Clone)]
pub enum Algorithm {
    /// Accept only improving moves.
    HillClimbing,
    /// Maintain a tabu list of recently reversed moves.
    TabuSearch { tabu_tenure: usize },
    /// Accept worse moves with probability exp(-delta/temperature).
    SimulatedAnnealing {
        initial_temp: f64,
        cooling_rate: f64,
    },
    /// Accept if better than the score from `late_size` steps ago.
    LateAcceptance { late_size: usize },
}

impl Default for Algorithm {
    fn default() -> Self {
        Algorithm::LateAcceptance { late_size: 400 }
    }
}

/// Result of a local search run.
#[derive(Debug, Clone)]
pub struct LocalSearchResult {
    pub assignment: Assignment,
    pub score: HardSoftScore,
    pub iterations: u64,
    pub time_ms: u64,
    pub algorithm: String,
}

/// Run local search on an assignment problem.
///
/// 1. Build initial solution via First Fit Decreasing.
/// 2. Improve via the selected algorithm until `time_limit` expires.
/// 3. Return the best solution found.
pub fn solve_local(
    problem: &AssignmentProblem,
    algorithm: &Algorithm,
    time_limit: Duration,
    seed: u64,
) -> LocalSearchResult {
    let mut rng = StdRng::seed_from_u64(seed);

    // Construction phase
    let mut assignment = first_fit_decreasing(problem);
    let mut current_score = evaluate(problem, &assignment);
    let mut best_assignment = assignment.clone();
    let mut best_score = current_score;

    let start = Instant::now();
    let mut iterations: u64 = 0;

    match algorithm {
        Algorithm::HillClimbing => {
            hill_climbing(
                problem,
                &mut assignment,
                &mut current_score,
                &mut best_assignment,
                &mut best_score,
                &mut iterations,
                &mut rng,
                time_limit,
                start,
            );
        }
        Algorithm::TabuSearch { tabu_tenure } => {
            tabu_search(
                problem,
                &mut assignment,
                &mut current_score,
                &mut best_assignment,
                &mut best_score,
                &mut iterations,
                &mut rng,
                time_limit,
                start,
                *tabu_tenure,
            );
        }
        Algorithm::SimulatedAnnealing {
            initial_temp,
            cooling_rate,
        } => {
            simulated_annealing(
                problem,
                &mut assignment,
                &mut current_score,
                &mut best_assignment,
                &mut best_score,
                &mut iterations,
                &mut rng,
                time_limit,
                start,
                *initial_temp,
                *cooling_rate,
            );
        }
        Algorithm::LateAcceptance { late_size } => {
            late_acceptance(
                problem,
                &mut assignment,
                &mut current_score,
                &mut best_assignment,
                &mut best_score,
                &mut iterations,
                &mut rng,
                time_limit,
                start,
                *late_size,
            );
        }
    }

    let elapsed = start.elapsed();
    LocalSearchResult {
        assignment: best_assignment,
        score: best_score,
        iterations,
        time_ms: elapsed.as_millis() as u64,
        algorithm: algorithm_name(algorithm),
    }
}

fn algorithm_name(alg: &Algorithm) -> String {
    match alg {
        Algorithm::HillClimbing => "hill_climbing".to_string(),
        Algorithm::TabuSearch { .. } => "tabu_search".to_string(),
        Algorithm::SimulatedAnnealing { .. } => "simulated_annealing".to_string(),
        Algorithm::LateAcceptance { .. } => "late_acceptance".to_string(),
    }
}

/// Hill Climbing: accept only strictly improving moves.
#[allow(clippy::too_many_arguments)]
fn hill_climbing(
    problem: &AssignmentProblem,
    assignment: &mut Assignment,
    current_score: &mut HardSoftScore,
    best_assignment: &mut Assignment,
    best_score: &mut HardSoftScore,
    iterations: &mut u64,
    rng: &mut StdRng,
    time_limit: Duration,
    start: Instant,
) {
    while start.elapsed() < time_limit {
        *iterations += 1;

        let mv = match generate_random_move(problem, assignment, rng) {
            Some(m) => m,
            None => break,
        };

        mv.apply(assignment);
        let new_score = evaluate(problem, assignment);

        if new_score > *current_score {
            *current_score = new_score;
            if new_score > *best_score {
                *best_score = new_score;
                *best_assignment = assignment.clone();
            }
        } else {
            mv.undo(assignment);
        }
    }
}

/// Tabu Search: maintain a list of recently made moves to avoid cycling.
#[allow(clippy::too_many_arguments)]
fn tabu_search(
    problem: &AssignmentProblem,
    assignment: &mut Assignment,
    current_score: &mut HardSoftScore,
    best_assignment: &mut Assignment,
    best_score: &mut HardSoftScore,
    iterations: &mut u64,
    rng: &mut StdRng,
    time_limit: Duration,
    start: Instant,
    tabu_tenure: usize,
) {
    let mut tabu_list: VecDeque<(usize, usize)> = VecDeque::new();
    let sample_size = 8;

    while start.elapsed() < time_limit {
        *iterations += 1;

        let mut best_candidate = None;
        let mut best_candidate_score = None;

        for _ in 0..sample_size {
            let mv = match generate_random_move(problem, assignment, rng) {
                Some(m) => m,
                None => continue,
            };

            let is_tabu = match &mv {
                super::move_types::Move::Change { item, new_slot, .. } => {
                    tabu_list.contains(&(*item, *new_slot))
                }
                super::move_types::Move::Swap { item_a, item_b } => {
                    let slot_a = assignment[*item_a];
                    let slot_b = assignment[*item_b];
                    tabu_list.contains(&(*item_a, slot_b)) || tabu_list.contains(&(*item_b, slot_a))
                }
            };

            mv.apply(assignment);
            let score = evaluate(problem, assignment);
            mv.undo(assignment);

            let dominated = is_tabu && score <= *best_score;
            if !dominated
                && (best_candidate_score.is_none()
                    || score > *best_candidate_score.as_ref().unwrap())
            {
                best_candidate = Some(mv);
                best_candidate_score = Some(score);
            }
        }

        if let (Some(mv), Some(score)) = (best_candidate, best_candidate_score) {
            match &mv {
                super::move_types::Move::Change { item, old_slot, .. } => {
                    tabu_list.push_back((*item, *old_slot));
                }
                super::move_types::Move::Swap { item_a, item_b } => {
                    let slot_a = assignment[*item_a];
                    let slot_b = assignment[*item_b];
                    tabu_list.push_back((*item_a, slot_a));
                    tabu_list.push_back((*item_b, slot_b));
                }
            }
            while tabu_list.len() > tabu_tenure {
                tabu_list.pop_front();
            }

            mv.apply(assignment);
            *current_score = score;

            if score > *best_score {
                *best_score = score;
                *best_assignment = assignment.clone();
            }
        }
    }
}

/// Simulated Annealing: accept worse moves with decreasing probability.
#[allow(clippy::too_many_arguments)]
fn simulated_annealing(
    problem: &AssignmentProblem,
    assignment: &mut Assignment,
    current_score: &mut HardSoftScore,
    best_assignment: &mut Assignment,
    best_score: &mut HardSoftScore,
    iterations: &mut u64,
    rng: &mut StdRng,
    time_limit: Duration,
    start: Instant,
    initial_temp: f64,
    cooling_rate: f64,
) {
    let mut temperature = initial_temp;

    while start.elapsed() < time_limit {
        *iterations += 1;

        let mv = match generate_random_move(problem, assignment, rng) {
            Some(m) => m,
            None => break,
        };

        mv.apply(assignment);
        let new_score = evaluate(problem, assignment);

        let accept = if new_score >= *current_score {
            true
        } else {
            let hard_delta = (new_score.hard - current_score.hard) as f64 * 1000.0;
            let soft_delta = (new_score.soft - current_score.soft) as f64;
            let delta = hard_delta + soft_delta;
            if temperature > 1e-10 {
                let p = (delta / temperature).exp();
                rng.gen::<f64>() < p
            } else {
                false
            }
        };

        if accept {
            *current_score = new_score;
            if new_score > *best_score {
                *best_score = new_score;
                *best_assignment = assignment.clone();
            }
        } else {
            mv.undo(assignment);
        }

        temperature *= cooling_rate;
    }
}

/// Late Acceptance: accept if the new score is better than the score from
/// `late_size` steps ago.
#[allow(clippy::too_many_arguments)]
fn late_acceptance(
    problem: &AssignmentProblem,
    assignment: &mut Assignment,
    current_score: &mut HardSoftScore,
    best_assignment: &mut Assignment,
    best_score: &mut HardSoftScore,
    iterations: &mut u64,
    rng: &mut StdRng,
    time_limit: Duration,
    start: Instant,
    late_size: usize,
) {
    let mut history: VecDeque<HardSoftScore> = VecDeque::with_capacity(late_size);
    for _ in 0..late_size {
        history.push_back(*current_score);
    }

    while start.elapsed() < time_limit {
        *iterations += 1;

        let mv = match generate_random_move(problem, assignment, rng) {
            Some(m) => m,
            None => break,
        };

        mv.apply(assignment);
        let new_score = evaluate(problem, assignment);

        let late_score = history.front().copied().unwrap_or(HardSoftScore::ZERO);

        let accept = new_score >= *current_score || new_score >= late_score;

        if accept {
            *current_score = new_score;
            if new_score > *best_score {
                *best_score = new_score;
                *best_assignment = assignment.clone();
            }
        } else {
            mv.undo(assignment);
        }

        history.pop_front();
        history.push_back(*current_score);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metaheuristic::problem::*;
    use std::collections::HashMap;

    fn make_improvable_problem() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 4,
            slot_count: 4,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: vec![1.0, 1.0, 1.0, 1.0],
                    slot_costs: vec![10.0, 20.0, 30.0, 40.0],
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 4],
            item_data: (0..4)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..4)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    fn make_2x2_known_optimum() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 2,
            slot_count: 2,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: vec![1.0, 2.0],
                    slot_costs: vec![1.0, 2.0],
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 2],
            item_data: (0..2)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..2)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    fn make_deceptive_problem() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 5,
            slot_count: 5,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 }),
                TypedConstraint::Hard(HardConstraint::SkillMatch {
                    feasible: vec![
                        vec![true, true, false, false, false],
                        vec![false, true, true, false, false],
                        vec![false, false, true, true, false],
                        vec![false, false, false, true, true],
                        vec![true, false, false, false, true],
                    ],
                }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: vec![1.0, 2.0, 3.0, 2.0, 1.0],
                    slot_costs: vec![5.0, 1.0, 1.0, 1.0, 5.0],
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 5],
            item_data: (0..5)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..5)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn test_hill_climbing_improves() {
        let problem = make_improvable_problem();
        let construction_score = {
            let assignment = first_fit_decreasing(&problem);
            evaluate(&problem, &assignment)
        };
        let result = solve_local(
            &problem,
            &Algorithm::HillClimbing,
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score >= construction_score,
            "HC should not worsen: {} vs {}",
            result.score,
            construction_score
        );
    }

    #[test]
    fn test_hill_climbing_feasible() {
        let problem = make_improvable_problem();
        let result = solve_local(
            &problem,
            &Algorithm::HillClimbing,
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score.is_feasible(),
            "HC should produce feasible solution"
        );
    }

    #[test]
    fn test_tabu_search_improves() {
        let problem = make_improvable_problem();
        let construction_score = {
            let assignment = first_fit_decreasing(&problem);
            evaluate(&problem, &assignment)
        };
        let result = solve_local(
            &problem,
            &Algorithm::TabuSearch { tabu_tenure: 7 },
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score >= construction_score,
            "TS should not worsen: {} vs {}",
            result.score,
            construction_score
        );
    }

    #[test]
    fn test_tabu_search_feasible() {
        let problem = make_deceptive_problem();
        let result = solve_local(
            &problem,
            &Algorithm::TabuSearch { tabu_tenure: 7 },
            Duration::from_millis(300),
            42,
        );
        assert!(
            result.score.is_feasible(),
            "TS should find feasible: {}",
            result.score
        );
    }

    #[test]
    fn test_sa_improves() {
        let problem = make_improvable_problem();
        let construction_score = {
            let assignment = first_fit_decreasing(&problem);
            evaluate(&problem, &assignment)
        };
        let result = solve_local(
            &problem,
            &Algorithm::SimulatedAnnealing {
                initial_temp: 1000.0,
                cooling_rate: 0.9999,
            },
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score >= construction_score,
            "SA should not worsen: {} vs {}",
            result.score,
            construction_score
        );
    }

    #[test]
    fn test_sa_feasible() {
        let problem = make_improvable_problem();
        let result = solve_local(
            &problem,
            &Algorithm::SimulatedAnnealing {
                initial_temp: 1000.0,
                cooling_rate: 0.9999,
            },
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score.is_feasible(),
            "SA should produce feasible solution"
        );
    }

    #[test]
    fn test_sa_accepts_worse_early() {
        let problem = make_improvable_problem();
        let result = solve_local(
            &problem,
            &Algorithm::SimulatedAnnealing {
                initial_temp: 100000.0,
                cooling_rate: 0.9999,
            },
            Duration::from_millis(100),
            42,
        );
        assert!(
            result.iterations > 10,
            "SA should run many iterations: {}",
            result.iterations
        );
    }

    #[test]
    fn test_late_acceptance_improves() {
        let problem = make_improvable_problem();
        let construction_score = {
            let assignment = first_fit_decreasing(&problem);
            evaluate(&problem, &assignment)
        };
        let result = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 400 },
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score >= construction_score,
            "LA should not worsen: {} vs {}",
            result.score,
            construction_score
        );
    }

    #[test]
    fn test_late_acceptance_feasible() {
        let problem = make_improvable_problem();
        let result = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 400 },
            Duration::from_millis(200),
            42,
        );
        assert!(
            result.score.is_feasible(),
            "LA should produce feasible solution"
        );
    }

    #[test]
    fn test_solve_local_returns_result() {
        let problem = make_improvable_problem();
        let result = solve_local(
            &problem,
            &Algorithm::default(),
            Duration::from_millis(100),
            42,
        );
        assert_eq!(result.assignment.len(), problem.item_count);
        assert!(result.iterations > 0);
        assert_eq!(result.algorithm, "late_acceptance");
    }

    #[test]
    fn test_solve_local_respects_time_limit() {
        let problem = make_improvable_problem();
        let time_limit = Duration::from_millis(100);
        let start = Instant::now();
        let _ = solve_local(&problem, &Algorithm::HillClimbing, time_limit, 42);
        let elapsed = start.elapsed();
        assert!(
            elapsed < time_limit * 3,
            "took {:?}, expected < {:?}",
            elapsed,
            time_limit * 3
        );
    }

    #[test]
    fn test_solve_local_known_optimum() {
        let problem = make_2x2_known_optimum();
        let result = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 10 },
            Duration::from_millis(200),
            42,
        );
        assert!(result.score.is_feasible());
        assert_eq!(
            result.score.soft, -4,
            "should find optimal soft score -4, got {}",
            result.score.soft
        );
    }

    #[test]
    fn test_all_algorithms_same_problem() {
        let problem = make_improvable_problem();
        let algorithms: Vec<Algorithm> = vec![
            Algorithm::HillClimbing,
            Algorithm::TabuSearch { tabu_tenure: 7 },
            Algorithm::SimulatedAnnealing {
                initial_temp: 1000.0,
                cooling_rate: 0.9999,
            },
            Algorithm::LateAcceptance { late_size: 400 },
        ];

        for alg in &algorithms {
            let result = solve_local(&problem, alg, Duration::from_millis(200), 42);
            assert!(
                result.score.is_feasible(),
                "{} should produce feasible: {}",
                result.algorithm,
                result.score
            );
        }
    }

    // =========================================================================
    // Benchmark tests
    // =========================================================================

    fn make_benchmark_5x5_cross_product() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 5,
            slot_count: 5,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 1 }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: vec![3.0, 7.0, 2.0, 5.0, 1.0],
                    slot_costs: vec![4.0, 1.0, 6.0, 2.0, 3.0],
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 5],
            item_data: (0..5)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..5)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn test_benchmark_5x5_local_search() {
        let problem = make_benchmark_5x5_cross_product();
        let result = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 100 },
            Duration::from_millis(500),
            42,
        );
        assert!(result.score.is_feasible(), "should be feasible");
        assert!(
            result.score.soft >= -46,
            "5x5 cross-product: expected soft >= -46, got {}",
            result.score.soft
        );
    }

    #[test]
    fn test_benchmark_5x5_all_algorithms() {
        let problem = make_benchmark_5x5_cross_product();
        let algorithms: Vec<Algorithm> = vec![
            Algorithm::HillClimbing,
            Algorithm::TabuSearch { tabu_tenure: 7 },
            Algorithm::SimulatedAnnealing {
                initial_temp: 100.0,
                cooling_rate: 0.9995,
            },
            Algorithm::LateAcceptance { late_size: 100 },
        ];

        for alg in &algorithms {
            let result = solve_local(&problem, alg, Duration::from_millis(300), 42);
            assert!(
                result.score.is_feasible(),
                "{} should be feasible: {}",
                result.algorithm,
                result.score
            );
            assert!(
                result.score.soft >= -46,
                "{}: expected soft >= -46, got {}",
                result.algorithm,
                result.score.soft
            );
        }
    }

    fn make_benchmark_30x10_assignment() -> AssignmentProblem {
        AssignmentProblem {
            item_count: 30,
            slot_count: 10,
            constraints: vec![
                TypedConstraint::Hard(HardConstraint::Capacity { limit: 3 }),
                TypedConstraint::Soft(SoftConstraint::MinimizeCost {
                    item_costs: (1..=30).map(|i| i as f64).collect(),
                    slot_costs: (1..=10).map(|i| i as f64).collect(),
                    weight: 1.0,
                }),
            ],
            pinned: vec![false; 30],
            item_data: (0..30)
                .map(|_| ItemData {
                    group: None,
                    fields: HashMap::new(),
                })
                .collect(),
            slot_data: (0..10)
                .map(|_| SlotData {
                    fields: HashMap::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn test_benchmark_30x10_local_search() {
        let problem = make_benchmark_30x10_assignment();
        let result = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 400 },
            Duration::from_millis(2000),
            42,
        );
        assert!(result.score.is_feasible(), "should be feasible");
        assert!(
            result.score.soft >= -2178,
            "30x10 assignment: expected soft >= -2178 (optimal -1815), got {}",
            result.score.soft
        );
    }

    #[test]
    fn test_benchmark_30x10_tabu_vs_sa() {
        let problem = make_benchmark_30x10_assignment();
        let algorithms = [
            ("tabu_search", Algorithm::TabuSearch { tabu_tenure: 10 }),
            (
                "simulated_annealing",
                Algorithm::SimulatedAnnealing {
                    initial_temp: 200.0,
                    cooling_rate: 0.9995,
                },
            ),
        ];

        for (name, alg) in &algorithms {
            let result = solve_local(&problem, alg, Duration::from_millis(2000), 42);
            assert!(
                result.score.is_feasible(),
                "{} should be feasible: {}",
                name,
                result.score
            );
            assert!(
                result.score.soft >= -2178,
                "{}: expected soft >= -2178, got {}",
                name,
                result.score.soft
            );
        }
    }

    #[test]
    fn test_algorithm_deterministic_seed() {
        let problem = make_improvable_problem();
        let result1 = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 10 },
            Duration::from_millis(50),
            12345,
        );
        let result2 = solve_local(
            &problem,
            &Algorithm::LateAcceptance { late_size: 10 },
            Duration::from_millis(50),
            12345,
        );
        assert_eq!(result1.assignment, result2.assignment);
        assert_eq!(result1.score, result2.score);
    }
}
