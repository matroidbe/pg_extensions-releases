use crate::error::AugurPgError;
use crate::task::PgTask;

/// Default AutoML algorithm set per task (used when caller passes `automl` or no algorithm).
#[allow(dead_code)]
pub fn default_algorithms(task: PgTask) -> &'static [&'static str] {
    match task {
        PgTask::Classification => &["lr", "dt", "rf", "nb", "svm", "xgboost", "lightgbm"],
        PgTask::Regression => &[
            "linear",
            "ridge",
            "lasso",
            "enet",
            "dt_reg",
            "rf_reg",
            "xgboost_reg",
            "lightgbm_reg",
        ],
        PgTask::TimeSeries => &["ets", "mstl", "xgboost_fc", "ridge_fc", "lightgbm_fc"],
    }
}

/// Translate a pg_ml/PyCaret-style algorithm ID into Augur's registry ID.
pub fn to_augur_id(pycaret_id: &str, task: PgTask) -> Result<&'static str, AugurPgError> {
    let id = pycaret_id.trim().to_ascii_lowercase();
    let mapped: Option<&'static str> = match (task, id.as_str()) {
        // ── classification ─────────────────────────────────────────────
        (PgTask::Classification, "lr") => Some("lr"),
        (PgTask::Classification, "logistic") => Some("lr"),
        (PgTask::Classification, "dt") => Some("dt"),
        (PgTask::Classification, "rf") => Some("rf"),
        (PgTask::Classification, "et") => Some("rf"),
        (PgTask::Classification, "nb") => Some("nb"),
        (PgTask::Classification, "svm") => Some("svm"),
        (PgTask::Classification, "xgboost") => Some("xgboost"),
        (PgTask::Classification, "lightgbm") => Some("lightgbm"),
        // ── regression ─────────────────────────────────────────────────
        (PgTask::Regression, "lr") => Some("linear"),
        (PgTask::Regression, "linear") => Some("linear"),
        (PgTask::Regression, "ridge") => Some("ridge"),
        (PgTask::Regression, "lasso") => Some("lasso"),
        (PgTask::Regression, "en") => Some("enet"),
        (PgTask::Regression, "elasticnet") => Some("enet"),
        (PgTask::Regression, "enet") => Some("enet"),
        (PgTask::Regression, "dt") => Some("dt_reg"),
        (PgTask::Regression, "dt_reg") => Some("dt_reg"),
        (PgTask::Regression, "rf") => Some("rf_reg"),
        (PgTask::Regression, "rf_reg") => Some("rf_reg"),
        (PgTask::Regression, "et") => Some("rf_reg"),
        (PgTask::Regression, "svm") => Some("svm_reg"),
        (PgTask::Regression, "svm_reg") => Some("svm_reg"),
        (PgTask::Regression, "xgboost") => Some("xgboost_reg"),
        (PgTask::Regression, "xgboost_reg") => Some("xgboost_reg"),
        (PgTask::Regression, "lightgbm") => Some("lightgbm_reg"),
        (PgTask::Regression, "lightgbm_reg") => Some("lightgbm_reg"),
        // ── forecasting ────────────────────────────────────────────────
        (PgTask::TimeSeries, "ets") => Some("ets"),
        (PgTask::TimeSeries, "mstl") => Some("mstl"),
        (PgTask::TimeSeries, "xgboost") => Some("xgboost_fc"),
        (PgTask::TimeSeries, "xgboost_fc") => Some("xgboost_fc"),
        (PgTask::TimeSeries, "lightgbm") => Some("lightgbm_fc"),
        (PgTask::TimeSeries, "lightgbm_fc") => Some("lightgbm_fc"),
        (PgTask::TimeSeries, "ridge") => Some("ridge_fc"),
        (PgTask::TimeSeries, "ridge_fc") => Some("ridge_fc"),
        (PgTask::TimeSeries, "lasso") => Some("lasso_fc"),
        (PgTask::TimeSeries, "lasso_fc") => Some("lasso_fc"),
        (PgTask::TimeSeries, "dt") => Some("dt_fc"),
        (PgTask::TimeSeries, "dt_fc") => Some("dt_fc"),
        _ => None,
    };
    mapped.ok_or_else(|| AugurPgError::UnsupportedAlgorithm(pycaret_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_mapping() {
        assert_eq!(
            to_augur_id("xgboost", PgTask::Classification).unwrap(),
            "xgboost"
        );
        assert_eq!(to_augur_id("RF", PgTask::Classification).unwrap(), "rf");
        assert_eq!(to_augur_id("lr", PgTask::Classification).unwrap(), "lr");
        assert_eq!(to_augur_id("et", PgTask::Classification).unwrap(), "rf");
    }

    #[test]
    fn regression_mapping() {
        assert_eq!(to_augur_id("lr", PgTask::Regression).unwrap(), "linear");
        assert_eq!(to_augur_id("en", PgTask::Regression).unwrap(), "enet");
        assert_eq!(to_augur_id("rf", PgTask::Regression).unwrap(), "rf_reg");
        assert_eq!(
            to_augur_id("xgboost", PgTask::Regression).unwrap(),
            "xgboost_reg"
        );
    }

    #[test]
    fn forecasting_mapping() {
        assert_eq!(to_augur_id("ets", PgTask::TimeSeries).unwrap(), "ets");
        assert_eq!(to_augur_id("mstl", PgTask::TimeSeries).unwrap(), "mstl");
        assert_eq!(
            to_augur_id("xgboost", PgTask::TimeSeries).unwrap(),
            "xgboost_fc"
        );
    }

    #[test]
    fn unsupported_returns_error() {
        assert!(to_augur_id("random_garbage", PgTask::Classification).is_err());
        // kmeans is an augur algo but not a classification/regression model
        assert!(to_augur_id("kmeans", PgTask::Classification).is_err());
    }

    #[test]
    fn default_algorithms_present() {
        assert!(!default_algorithms(PgTask::Classification).is_empty());
        assert!(!default_algorithms(PgTask::Regression).is_empty());
        assert!(!default_algorithms(PgTask::TimeSeries).is_empty());
    }
}
