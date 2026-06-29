use crate::error::AugurPgError;
use augur_core::types::TaskType;
use polars::prelude::*;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTask {
    Classification,
    Regression,
    TimeSeries,
}

impl PgTask {
    pub fn as_str(&self) -> &'static str {
        match self {
            PgTask::Classification => "classification",
            PgTask::Regression => "regression",
            PgTask::TimeSeries => "time_series",
        }
    }

    pub fn parse(s: &str) -> Result<PgTask, AugurPgError> {
        match s.to_ascii_lowercase().as_str() {
            "classification" => Ok(PgTask::Classification),
            "regression" => Ok(PgTask::Regression),
            "time_series" | "timeseries" | "forecasting" => Ok(PgTask::TimeSeries),
            other => Err(AugurPgError::UnsupportedTask(other.into())),
        }
    }
}

/// Count distinct non-null values in a Series (up to `cap` before giving up).
fn distinct_count(s: &Series, cap: usize) -> usize {
    if let Ok(ca) = s.i64() {
        let set: HashSet<i64> = ca.into_iter().flatten().take(cap * 4).collect();
        return set.len().min(cap + 1);
    }
    if let Ok(ca) = s.i32() {
        let set: HashSet<i32> = ca.into_iter().flatten().take(cap * 4).collect();
        return set.len().min(cap + 1);
    }
    if let Ok(ca) = s.bool() {
        let set: HashSet<bool> = ca.into_iter().flatten().take(cap * 4).collect();
        return set.len().min(cap + 1);
    }
    if let Ok(ca) = s.str() {
        let set: HashSet<&str> = ca.into_iter().flatten().take(cap * 4).collect();
        return set.len().min(cap + 1);
    }
    cap + 1
}

/// Infer task type from target column dtype + cardinality.
pub fn infer_task(df: &DataFrame, target: &str) -> Result<PgTask, AugurPgError> {
    let s = df
        .column(target)
        .map_err(|e| AugurPgError::Polars(e.to_string()))?;
    let series = s.as_materialized_series().clone();
    let dtype = series.dtype();

    if dtype.is_string() || matches!(dtype, DataType::Boolean) || dtype.is_categorical() {
        return Ok(PgTask::Classification);
    }

    if dtype.is_integer() {
        if distinct_count(&series, 10) <= 10 {
            return Ok(PgTask::Classification);
        }
        return Ok(PgTask::Regression);
    }

    if dtype.is_float() {
        return Ok(PgTask::Regression);
    }

    Err(AugurPgError::UnsupportedTask(format!(
        "cannot infer task from dtype {:?}",
        dtype
    )))
}

/// Map PgTask + (optional) class count to augur TaskType.
pub fn to_augur_task(pg: PgTask, n_classes: Option<usize>) -> TaskType {
    match pg {
        PgTask::Classification => match n_classes {
            Some(2) => TaskType::BinaryClassification,
            _ => TaskType::MulticlassClassification,
        },
        PgTask::Regression => TaskType::Regression,
        PgTask::TimeSeries => TaskType::Forecasting,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_task_names() {
        assert_eq!(
            PgTask::parse("classification").unwrap(),
            PgTask::Classification
        );
        assert_eq!(PgTask::parse("Regression").unwrap(), PgTask::Regression);
        assert_eq!(PgTask::parse("time_series").unwrap(), PgTask::TimeSeries);
        assert_eq!(PgTask::parse("timeseries").unwrap(), PgTask::TimeSeries);
        assert!(PgTask::parse("quantum").is_err());
    }

    #[test]
    fn text_target_is_classification() {
        let df = df! {
            "label" => &["a", "b", "a", "c"],
            "x" => &[1.0, 2.0, 3.0, 4.0],
        }
        .unwrap();
        assert_eq!(infer_task(&df, "label").unwrap(), PgTask::Classification);
    }

    #[test]
    fn float_target_is_regression() {
        let df = df! {
            "y" => &[1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        }
        .unwrap();
        assert_eq!(infer_task(&df, "y").unwrap(), PgTask::Regression);
    }

    #[test]
    fn low_cardinality_int_is_classification() {
        let df = df! {
            "y" => &[0_i64, 1, 0, 1, 0, 1, 0, 1],
        }
        .unwrap();
        assert_eq!(infer_task(&df, "y").unwrap(), PgTask::Classification);
    }

    #[test]
    fn high_cardinality_int_is_regression() {
        let vals: Vec<i64> = (0..100).collect();
        let df = df! { "y" => &vals }.unwrap();
        assert_eq!(infer_task(&df, "y").unwrap(), PgTask::Regression);
    }

    #[test]
    fn to_augur_task_mapping() {
        assert_eq!(
            to_augur_task(PgTask::Classification, Some(2)),
            TaskType::BinaryClassification
        );
        assert_eq!(
            to_augur_task(PgTask::Classification, Some(5)),
            TaskType::MulticlassClassification
        );
        assert_eq!(
            to_augur_task(PgTask::Regression, None),
            TaskType::Regression
        );
        assert_eq!(
            to_augur_task(PgTask::TimeSeries, None),
            TaskType::Forecasting
        );
    }
}
