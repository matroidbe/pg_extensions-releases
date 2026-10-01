//! Distribution constructors with parameter validation.
//!
//! Validation failures return `ProbError` with the same messages pg_prob
//! raised via `pgrx::error!`, so the SQL error surface is unchanged when the
//! pgrx wrappers delegate here.

use crate::dist::{Dist, DistParams, DistType};
use crate::error::ProbError;

/// Create a literal (certain) distribution
pub fn literal(value: f64) -> Dist {
    Dist {
        dist_type: DistType::Literal,
        params: DistParams::Literal { value },
    }
}

/// Create a normal distribution N(mu, sigma)
pub fn normal(mu: f64, sigma: f64) -> Result<Dist, ProbError> {
    if sigma < 0.0 {
        return Err(ProbError::new(
            "normal distribution sigma must be non-negative",
        ));
    }
    Ok(Dist {
        dist_type: DistType::Normal,
        params: DistParams::Normal { mu, sigma },
    })
}

/// Create a uniform distribution U(min, max)
pub fn uniform(min: f64, max: f64) -> Result<Dist, ProbError> {
    if min > max {
        return Err(ProbError::new("uniform distribution min must be <= max"));
    }
    Ok(Dist {
        dist_type: DistType::Uniform,
        params: DistParams::Uniform { min, max },
    })
}

/// Create a triangular distribution Tri(min, mode, max)
pub fn triangular(min: f64, mode: f64, max: f64) -> Result<Dist, ProbError> {
    if !(min <= mode && mode <= max) {
        return Err(ProbError::new(
            "triangular distribution requires min <= mode <= max",
        ));
    }
    Ok(Dist {
        dist_type: DistType::Triangular,
        params: DistParams::Triangular { min, mode, max },
    })
}

/// Create a beta distribution scaled to [min, max]
pub fn beta(alpha: f64, beta_param: f64, min: f64, max: f64) -> Result<Dist, ProbError> {
    if alpha <= 0.0 || beta_param <= 0.0 {
        return Err(ProbError::new(
            "beta distribution alpha and beta must be positive",
        ));
    }
    if min >= max {
        return Err(ProbError::new("beta distribution min must be < max"));
    }
    Ok(Dist {
        dist_type: DistType::Beta,
        params: DistParams::Beta {
            alpha,
            beta: beta_param,
            min,
            max,
        },
    })
}

/// Create a log-normal distribution LogN(mu, sigma).
/// mu and sigma are the mean and std of the underlying normal distribution.
pub fn lognormal(mu: f64, sigma: f64) -> Result<Dist, ProbError> {
    if sigma < 0.0 {
        return Err(ProbError::new(
            "lognormal distribution sigma must be non-negative",
        ));
    }
    Ok(Dist {
        dist_type: DistType::LogNormal,
        params: DistParams::LogNormal { mu, sigma },
    })
}

/// Create a PERT distribution (modified beta) with min, mode, max.
/// lambda controls the weight of the mode (conventionally 4.0).
pub fn pert(min: f64, mode: f64, max: f64, lambda: f64) -> Result<Dist, ProbError> {
    if !(min <= mode && mode <= max) {
        return Err(ProbError::new(
            "PERT distribution requires min <= mode <= max",
        ));
    }
    if min == max {
        // Degenerate case: return literal
        return Ok(literal(min));
    }
    Ok(Dist {
        dist_type: DistType::Pert,
        params: DistParams::Pert {
            min,
            mode,
            max,
            lambda,
        },
    })
}

/// Create a Poisson distribution Pois(lambda)
pub fn poisson(lambda: f64) -> Result<Dist, ProbError> {
    if lambda <= 0.0 {
        return Err(ProbError::new(
            "poisson distribution lambda must be positive",
        ));
    }
    Ok(Dist {
        dist_type: DistType::Poisson,
        params: DistParams::Poisson { lambda },
    })
}

/// Create an exponential distribution Exp(lambda)
pub fn exponential(lambda: f64) -> Result<Dist, ProbError> {
    if lambda <= 0.0 {
        return Err(ProbError::new(
            "exponential distribution lambda must be positive",
        ));
    }
    Ok(Dist {
        dist_type: DistType::Exponential,
        params: DistParams::Exponential { lambda },
    })
}

/// Conditional: if test_dist > threshold → then_dist, else → else_dist
pub fn if_above(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist {
        dist_type: DistType::IfAbove,
        params: DistParams::Conditional {
            test: Box::new(test_dist),
            threshold,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    }
}

/// Conditional: if test_dist < threshold → then_dist, else → else_dist
pub fn if_below(test_dist: Dist, threshold: f64, then_dist: Dist, else_dist: Dist) -> Dist {
    Dist {
        dist_type: DistType::IfBelow,
        params: DistParams::Conditional {
            test: Box::new(test_dist),
            threshold,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    }
}

/// Probability-based conditional: with probability p → then_dist, else → else_dist
pub fn if_then(probability: f64, then_dist: Dist, else_dist: Dist) -> Result<Dist, ProbError> {
    if !(0.0..=1.0).contains(&probability) {
        return Err(ProbError::new("probability must be between 0 and 1"));
    }
    Ok(Dist {
        dist_type: DistType::IfThen,
        params: DistParams::ProbBranch {
            probability,
            then_dist: Box::new(then_dist),
            else_dist: Box::new(else_dist),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_rejects_negative_sigma() {
        assert!(normal(0.0, -1.0).is_err());
        assert!(normal(0.0, 0.0).is_ok());
    }

    #[test]
    fn test_uniform_rejects_inverted_range() {
        assert!(uniform(10.0, 5.0).is_err());
        assert!(uniform(5.0, 5.0).is_ok());
    }

    #[test]
    fn test_triangular_ordering() {
        assert!(triangular(0.0, 5.0, 10.0).is_ok());
        assert!(triangular(5.0, 0.0, 10.0).is_err());
    }

    #[test]
    fn test_beta_validation() {
        assert!(beta(2.0, 2.0, 0.0, 1.0).is_ok());
        assert!(beta(0.0, 2.0, 0.0, 1.0).is_err());
        assert!(beta(2.0, 2.0, 1.0, 1.0).is_err());
    }

    #[test]
    fn test_pert_degenerate_becomes_literal() {
        let d = pert(5.0, 5.0, 5.0, 4.0).unwrap();
        assert_eq!(d.as_literal(), Some(5.0));
    }

    #[test]
    fn test_poisson_exponential_positive_lambda() {
        assert!(poisson(1.5).is_ok());
        assert!(poisson(0.0).is_err());
        assert!(exponential(1.5).is_ok());
        assert!(exponential(-1.0).is_err());
    }

    #[test]
    fn test_if_then_probability_range() {
        let t = literal(1.0);
        let e = literal(0.0);
        assert!(if_then(0.5, t.clone(), e.clone()).is_ok());
        assert!(if_then(1.5, t, e).is_err());
    }
}
