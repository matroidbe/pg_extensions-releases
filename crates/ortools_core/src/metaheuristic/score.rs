//! Multi-level scoring for constraint optimization.
//!
//! HardSoftScore provides lexicographic ordering: hard constraints always
//! take priority over soft constraints. A feasible solution (hard == 0) with
//! any soft score is always better than an infeasible solution (hard < 0).

use std::cmp::Ordering;
use std::fmt;

/// Lexicographic score with hard and soft levels.
///
/// - `hard`: 0 means all hard constraints satisfied (feasible).
///   Each violation decrements by 1.
/// - `soft`: weighted penalty sum. Lower is better (for minimization).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct HardSoftScore {
    pub hard: i64,
    pub soft: i64,
}

impl HardSoftScore {
    pub const ZERO: HardSoftScore = HardSoftScore { hard: 0, soft: 0 };

    pub fn new(hard: i64, soft: i64) -> Self {
        Self { hard, soft }
    }

    /// A feasible solution has hard == 0 (no hard constraint violations).
    pub fn is_feasible(&self) -> bool {
        self.hard >= 0
    }
}

impl Ord for HardSoftScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher is better for both levels (hard: 0 > -1, soft: 0 > -100)
        self.hard.cmp(&other.hard).then(self.soft.cmp(&other.soft))
    }
}

impl PartialOrd for HardSoftScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::ops::Add for HardSoftScore {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self {
            hard: self.hard + rhs.hard,
            soft: self.soft + rhs.soft,
        }
    }
}

impl std::ops::Sub for HardSoftScore {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self {
            hard: self.hard - rhs.hard,
            soft: self.soft - rhs.soft,
        }
    }
}

impl fmt::Display for HardSoftScore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}hard/{}soft", self.hard, self.soft)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hard_soft_score_ordering() {
        assert!(HardSoftScore::new(0, -100) > HardSoftScore::new(-1, 0));
        assert!(HardSoftScore::new(0, -9999) > HardSoftScore::new(-1, 9999));
        assert!(HardSoftScore::new(0, 0) > HardSoftScore::new(0, -50));
        assert!(HardSoftScore::new(-1, 0) > HardSoftScore::new(-2, 0));
    }

    #[test]
    fn test_hard_soft_score_equality() {
        assert_eq!(HardSoftScore::new(0, 50), HardSoftScore::new(0, 50));
        assert_ne!(HardSoftScore::new(0, 50), HardSoftScore::new(0, 51));
    }

    #[test]
    fn test_hard_soft_score_add() {
        let a = HardSoftScore::new(-1, -50);
        let b = HardSoftScore::new(-2, -30);
        let sum = a + b;
        assert_eq!(sum, HardSoftScore::new(-3, -80));
    }

    #[test]
    fn test_hard_soft_score_sub() {
        let a = HardSoftScore::new(0, -50);
        let b = HardSoftScore::new(-1, -30);
        let diff = a - b;
        assert_eq!(diff, HardSoftScore::new(1, -20));
    }

    #[test]
    fn test_hard_soft_score_feasible() {
        assert!(HardSoftScore::new(0, -100).is_feasible());
        assert!(HardSoftScore::new(0, 0).is_feasible());
        assert!(!HardSoftScore::new(-1, 0).is_feasible());
        assert!(!HardSoftScore::new(-5, 100).is_feasible());
    }

    #[test]
    fn test_hard_soft_score_display() {
        assert_eq!(
            format!("{}", HardSoftScore::new(-2, -150)),
            "-2hard/-150soft"
        );
    }

    #[test]
    fn test_hard_soft_score_zero() {
        assert_eq!(HardSoftScore::ZERO, HardSoftScore::new(0, 0));
        assert!(HardSoftScore::ZERO.is_feasible());
    }
}
