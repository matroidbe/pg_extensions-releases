//! Training orchestration shared by sync and async paths.
//!
//! Takes parsed inputs, pulls data from Postgres, runs augur setup+fit,
//! serializes the result, and inserts into pgaugur.models.

use crate::algorithms::to_augur_id;
use crate::data;
use crate::error::AugurPgError;
use crate::models;
use crate::task::{infer_task, to_augur_task, PgTask};
use augur::prelude::{
    compare_models, create_model, predict_model, save_model_to_string, setup, SetupConfig,
    TrainedModel,
};
use augur_core::traits::Estimator;
use augur_core::types::TaskType;
use polars::prelude::*;
use std::collections::HashSet;
use std::time::Instant;

pub struct TrainOutcome {
    pub model_id: i64,
    pub algorithm: String,
    pub metrics: serde_json::Value,
    pub deployed: bool,
    #[allow(dead_code)]
    pub training_time_seconds: f64,
}

fn feature_columns(df: &DataFrame, target: &str, exclude: &[String]) -> Vec<String> {
    let excl: HashSet<&str> = exclude.iter().map(|s| s.as_str()).collect();
    df.get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .filter(|c| c != target && !excl.contains(c.as_str()))
        .collect()
}

fn compute_metrics(
    model: &TrainedModel,
    experiment: &augur::prelude::Experiment,
) -> serde_json::Value {
    // Apply model to the held-out test set and compute a small set of metrics.
    let test_df = experiment
        .test_features
        .hstack(&[experiment.test_target.clone().into_column()])
        .ok();

    let preds_result = test_df
        .as_ref()
        .and_then(|df| predict_model(model, experiment, df.clone()).ok());

    let Some(preds_df) = preds_result else {
        return serde_json::json!({});
    };

    let pred_series = match preds_df.column("prediction") {
        Ok(c) => c.as_materialized_series().clone(),
        Err(_) => return serde_json::json!({}),
    };
    let target_series = experiment.test_target.clone();

    let mut metrics = serde_json::Map::new();
    match experiment.task_type {
        TaskType::Regression | TaskType::Forecasting => {
            if let (Ok(preds), Ok(targets)) = (pred_series.f64(), target_series.f64()) {
                let mut sum_abs = 0.0;
                let mut sum_sq = 0.0;
                let mut n = 0usize;
                let mut sum_y = 0.0;
                for (p, t) in preds.into_iter().zip(targets) {
                    if let (Some(p), Some(t)) = (p, t) {
                        let e = p - t;
                        sum_abs += e.abs();
                        sum_sq += e * e;
                        sum_y += t;
                        n += 1;
                    }
                }
                if n > 0 {
                    let mae = sum_abs / n as f64;
                    let mse = sum_sq / n as f64;
                    let rmse = mse.sqrt();
                    metrics.insert("MAE".into(), serde_json::json!(mae));
                    metrics.insert("MSE".into(), serde_json::json!(mse));
                    metrics.insert("RMSE".into(), serde_json::json!(rmse));

                    // R² = 1 - SS_res / SS_tot
                    let mean_y = sum_y / n as f64;
                    let mut ss_tot = 0.0;
                    for t in targets.into_iter().flatten() {
                        ss_tot += (t - mean_y).powi(2);
                    }
                    if ss_tot > 0.0 {
                        let r2 = 1.0 - sum_sq / ss_tot;
                        metrics.insert("R2".into(), serde_json::json!(r2));
                    }
                }
            }
        }
        TaskType::BinaryClassification | TaskType::MulticlassClassification => {
            // Compare predictions vs targets for accuracy.
            // Predictions may be Float64 (e.g., 0.0, 1.0) while targets
            // may be Int64 (0, 1) or String ("setosa"). Cast both to f64
            // first for numeric comparison; fall back to string comparison.
            let n = pred_series.len().min(target_series.len());
            let mut correct = 0usize;

            // Try numeric comparison first (handles int target vs float prediction)
            let pred_f64 = pred_series.cast(&polars::prelude::DataType::Float64).ok();
            let tgt_f64 = target_series.cast(&polars::prelude::DataType::Float64).ok();

            if let (Some(ref pf), Some(ref tf)) = (pred_f64, tgt_f64) {
                if let (Ok(p_ca), Ok(t_ca)) = (pf.f64(), tf.f64()) {
                    for (p, t) in p_ca.into_iter().zip(t_ca) {
                        if let (Some(pv), Some(tv)) = (p, t) {
                            // For classification, round predictions to nearest int
                            if pv.round() == tv.round() {
                                correct += 1;
                            }
                        }
                    }
                }
            } else {
                // Fall back to string comparison (for text labels like "setosa")
                let pred_str = pred_series
                    .cast(&polars::prelude::DataType::String)
                    .unwrap_or(pred_series.clone());
                let tgt_str = target_series
                    .cast(&polars::prelude::DataType::String)
                    .unwrap_or(target_series.clone());
                if let (Ok(p_ca), Ok(t_ca)) = (pred_str.str(), tgt_str.str()) {
                    for (p, t) in p_ca.into_iter().zip(t_ca) {
                        if let (Some(pv), Some(tv)) = (p, t) {
                            if pv == tv {
                                correct += 1;
                            }
                        }
                    }
                }
            }

            if n > 0 {
                let accuracy = correct as f64 / n as f64;
                metrics.insert("Accuracy".into(), serde_json::json!(accuracy));

                // Compute Precision, Recall, F1 for binary classification
                // For multiclass, compute macro-averaged metrics
                let pred_f64_for_pr = pred_series.cast(&polars::prelude::DataType::Float64).ok();
                let tgt_f64_for_pr = target_series.cast(&polars::prelude::DataType::Float64).ok();
                if let (Some(ref pf), Some(ref tf)) = (pred_f64_for_pr, tgt_f64_for_pr) {
                    if let (Ok(p_ca), Ok(t_ca)) = (pf.f64(), tf.f64()) {
                        // Collect unique classes
                        let mut classes = std::collections::BTreeSet::new();
                        for t in t_ca.into_iter().flatten() {
                            classes.insert(t.round() as i64);
                        }

                        let mut total_precision = 0.0;
                        let mut total_recall = 0.0;
                        let mut n_classes = 0usize;

                        for cls in &classes {
                            let mut tp = 0usize;
                            let mut fp = 0usize;
                            let mut fn_ = 0usize;

                            for (p, t) in p_ca.into_iter().zip(t_ca) {
                                if let (Some(pv), Some(tv)) = (p, t) {
                                    let pred_cls = pv.round() as i64;
                                    let true_cls = tv.round() as i64;
                                    if pred_cls == *cls && true_cls == *cls {
                                        tp += 1;
                                    } else if pred_cls == *cls && true_cls != *cls {
                                        fp += 1;
                                    } else if pred_cls != *cls && true_cls == *cls {
                                        fn_ += 1;
                                    }
                                }
                            }

                            let precision = if tp + fp > 0 {
                                tp as f64 / (tp + fp) as f64
                            } else {
                                0.0
                            };
                            let recall = if tp + fn_ > 0 {
                                tp as f64 / (tp + fn_) as f64
                            } else {
                                0.0
                            };
                            total_precision += precision;
                            total_recall += recall;
                            n_classes += 1;
                        }

                        if n_classes > 0 {
                            let macro_precision = total_precision / n_classes as f64;
                            let macro_recall = total_recall / n_classes as f64;
                            let macro_f1 = if macro_precision + macro_recall > 0.0 {
                                2.0 * macro_precision * macro_recall
                                    / (macro_precision + macro_recall)
                            } else {
                                0.0
                            };
                            metrics.insert("Precision".into(), serde_json::json!(macro_precision));
                            metrics.insert("Recall".into(), serde_json::json!(macro_recall));
                            metrics.insert("F1".into(), serde_json::json!(macro_f1));
                        }
                    }
                }
            }
        }
        _ => {}
    }

    serde_json::Value::Object(metrics)
}

#[allow(clippy::too_many_arguments)]
pub fn setup_experiment(
    source_schema: Option<&str>,
    source_table: &str,
    target_column: &str,
    task_override: Option<PgTask>,
    exclude_columns: &[String],
    test_fraction: f64,
    seed: u64,
) -> Result<(augur::prelude::Experiment, PgTask, Vec<String>), AugurPgError> {
    let exclude_refs: Vec<&str> = exclude_columns.iter().map(|s| s.as_str()).collect();
    let df = data::load_table(source_schema, source_table, &exclude_refs)?;
    if !df
        .get_column_names()
        .iter()
        .any(|c| c.as_str() == target_column)
    {
        return Err(AugurPgError::TargetNotFound(target_column.to_string()));
    }

    let pg_task = match task_override {
        Some(t) => t,
        None => infer_task(&df, target_column)?,
    };

    let feats = feature_columns(&df, target_column, exclude_columns);

    let n_classes = match pg_task {
        PgTask::Classification => {
            let col = df
                .column(target_column)
                .map_err(|e| AugurPgError::Polars(e.to_string()))?;
            Some(col.as_materialized_series().n_unique().unwrap_or(2))
        }
        _ => None,
    };
    let task_type = to_augur_task(pg_task, n_classes);

    let config = SetupConfig::new(target_column)
        .task_type(task_type)
        .ignore(exclude_columns.to_vec())
        .test_fraction(test_fraction)
        .seed(seed);

    let experiment = setup(df, config)?;
    Ok((experiment, pg_task, feats))
}

#[allow(clippy::too_many_arguments)]
pub fn train_single(
    project_name: &str,
    source_schema: Option<&str>,
    source_table: &str,
    target_column: &str,
    algorithm: &str,
    task_override: Option<PgTask>,
    exclude_columns: &[String],
    hyperparams: Option<&serde_json::Value>,
    test_fraction: f64,
    seed: u64,
    deploy: bool,
    conformal: bool,
    conformal_method: Option<&str>,
) -> Result<TrainOutcome, AugurPgError> {
    if conformal {
        return Err(AugurPgError::NotSupported(
            "conformal prediction".to_string(),
        ));
    }

    let (experiment, pg_task, feats) = setup_experiment(
        source_schema,
        source_table,
        target_column,
        task_override,
        exclude_columns,
        test_fraction,
        seed,
    )?;

    let augur_id = to_augur_id(algorithm, pg_task)?;

    let t0 = Instant::now();
    let model = create_model(&experiment, augur_id)?;
    let elapsed = t0.elapsed().as_secs_f64();

    let metrics = compute_metrics(&model, &experiment);
    let artifact = save_model_to_string(&model, &experiment)?.into_bytes();
    let label_classes = label_classes_for(&experiment);

    let project_id = models::upsert_project(
        project_name,
        pg_task.as_str(),
        target_column,
        &feats,
        source_schema,
        Some(source_table),
        Some(exclude_columns),
        None,
        None,
    )?;

    let model_id = models::insert_model(
        project_id,
        model.id.as_str(),
        hyperparams,
        &metrics,
        &artifact,
        label_classes.as_deref(),
        deploy,
        conformal,
        conformal_method,
        elapsed,
    )?;

    if deploy {
        crate::predict::invalidate(project_name);
    }

    Ok(TrainOutcome {
        model_id,
        algorithm: model.id.clone(),
        metrics,
        deployed: deploy,
        training_time_seconds: elapsed,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn train_automl(
    project_name: &str,
    source_schema: Option<&str>,
    source_table: &str,
    target_column: &str,
    task_override: Option<PgTask>,
    exclude_columns: &[String],
    include: Option<&[String]>,
    exclude_algs: Option<&[String]>,
    test_fraction: f64,
    seed: u64,
    deploy: bool,
) -> Result<TrainOutcome, AugurPgError> {
    let (experiment, pg_task, feats) = setup_experiment(
        source_schema,
        source_table,
        target_column,
        task_override,
        exclude_columns,
        test_fraction,
        seed,
    )?;

    // Augur's compare_models() uses the registry's full task set. include/exclude
    // filters are applied as a post-pass: we'd ideally customize the registry,
    // but for MVP we pass through and then pick the best model that passes the
    // include/exclude filter. If no models match, we fall back to the leaderboard winner.
    let result = compare_models(&experiment, None)?;

    let chosen = filter_pick(&result.models, include, exclude_algs)
        .or_else(|| result.models.first())
        .ok_or_else(|| AugurPgError::Other("compare_models produced no models".into()))?;

    let metrics = compute_metrics(chosen, &experiment);
    let artifact = save_model_to_string(chosen, &experiment)?.into_bytes();
    let label_classes = label_classes_for(&experiment);

    let project_id = models::upsert_project(
        project_name,
        pg_task.as_str(),
        target_column,
        &feats,
        source_schema,
        Some(source_table),
        Some(exclude_columns),
        None,
        None,
    )?;

    let model_id = models::insert_model(
        project_id,
        chosen.id.as_str(),
        None,
        &metrics,
        &artifact,
        label_classes.as_deref(),
        deploy,
        false,
        None,
        0.0,
    )?;

    if deploy {
        crate::predict::invalidate(project_name);
    }

    Ok(TrainOutcome {
        model_id,
        algorithm: chosen.id.clone(),
        metrics,
        deployed: deploy,
        training_time_seconds: 0.0,
    })
}

fn filter_pick<'a>(
    models: &'a [TrainedModel],
    include: Option<&[String]>,
    exclude: Option<&[String]>,
) -> Option<&'a TrainedModel> {
    let inc: Option<HashSet<&str>> = include.map(|v| v.iter().map(|s| s.as_str()).collect());
    let exc: HashSet<&str> = exclude
        .map(|v| v.iter().map(|s| s.as_str()).collect())
        .unwrap_or_default();
    models.iter().find(|m| {
        let id = m.id.as_str();
        (inc.as_ref().map(|s| s.contains(id)).unwrap_or(true)) && !exc.contains(id)
    })
}

fn label_classes_for(experiment: &augur::prelude::Experiment) -> Option<Vec<String>> {
    if !matches!(
        experiment.task_type,
        TaskType::BinaryClassification | TaskType::MulticlassClassification
    ) {
        return None;
    }
    // Extract unique target labels as strings.
    let target = &experiment.train_target;
    if let Ok(uniq) = target.unique() {
        if let Ok(ca) = uniq.str() {
            return Some(ca.into_iter().flatten().map(|s| s.to_string()).collect());
        }
        if let Ok(ca) = uniq.i64() {
            return Some(ca.into_iter().flatten().map(|i| i.to_string()).collect());
        }
        if let Ok(ca) = uniq.bool() {
            return Some(ca.into_iter().flatten().map(|b| b.to_string()).collect());
        }
    }
    None
}

/// Public wrapper for `compute_metrics` used by the experiment module.
pub fn compute_metrics_from_experiment(
    model: &TrainedModel,
    experiment: &augur::prelude::Experiment,
) -> serde_json::Value {
    compute_metrics(model, experiment)
}

/// Public wrapper for `label_classes_for` used by the experiment module.
pub fn label_classes_for_experiment(
    experiment: &augur::prelude::Experiment,
) -> Option<Vec<String>> {
    label_classes_for(experiment)
}

// Silences unused import warning in non-test builds when Estimator
// methods are only used via augur's own internals.
#[allow(dead_code)]
fn _force_use(_e: &dyn Estimator) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_iris_df() -> DataFrame {
        df! {
            "sl" => &[5.1, 4.9, 7.0, 6.4, 6.3, 5.8],
            "sw" => &[3.5, 3.0, 3.2, 3.2, 3.3, 2.7],
            "pl" => &[1.4, 1.4, 4.7, 4.5, 6.0, 5.1],
            "pw" => &[0.2, 0.2, 1.4, 1.5, 2.5, 1.9],
            "species" => &["setosa", "setosa", "versicolor", "versicolor", "virginica", "virginica"],
        }
        .unwrap()
    }

    fn tiny_regression_df() -> DataFrame {
        df! {
            "x1" => &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
            "x2" => &[5.0, 3.0, 7.0, 2.0, 9.0, 1.0, 8.0, 4.0, 6.0, 10.0, 3.0, 7.0],
            "y" => &[8.1, 7.0, 13.1, 8.0, 19.1, 9.0, 20.1, 14.0, 18.1, 26.0, 16.1, 22.0],
        }
        .unwrap()
    }

    #[test]
    fn feature_columns_excludes_target_and_excluded() {
        let df = tiny_iris_df();
        let exclude = vec!["sw".to_string()];
        let feats = feature_columns(&df, "species", &exclude);
        assert!(feats.contains(&"sl".to_string()));
        assert!(feats.contains(&"pl".to_string()));
        assert!(feats.contains(&"pw".to_string()));
        assert!(!feats.contains(&"sw".to_string()));
        assert!(!feats.contains(&"species".to_string()));
    }

    #[test]
    fn feature_columns_empty_exclude() {
        let df = tiny_iris_df();
        let feats = feature_columns(&df, "species", &[]);
        assert_eq!(feats.len(), 4); // sl, sw, pl, pw
    }

    #[test]
    fn compute_metrics_classification_safe_with_string_targets() {
        // Simulate what happens: target is String, predictions are Float64
        let config = SetupConfig::new("species")
            .task_type(TaskType::MulticlassClassification)
            .test_fraction(0.5);
        let df = tiny_iris_df();
        let experiment = setup(df, config).unwrap();

        let model = create_model(&experiment, "rf").unwrap();
        let metrics = compute_metrics(&model, &experiment);

        // Should not panic (the eq_missing bug we fixed).
        // Metrics should be a JSON object, possibly with Accuracy key.
        assert!(metrics.is_object());
    }

    #[test]
    fn compute_metrics_regression() {
        let config = SetupConfig::new("y")
            .task_type(TaskType::Regression)
            .test_fraction(0.25);
        let df = tiny_regression_df();
        let experiment = setup(df, config).unwrap();

        let model = create_model(&experiment, "linear").unwrap();
        let metrics = compute_metrics(&model, &experiment);

        assert!(metrics.is_object());
        let obj = metrics.as_object().unwrap();
        // Regression metrics should exist and be finite positive numbers.
        assert!(obj.contains_key("MAE"), "should contain MAE: {:?}", obj);
        assert!(obj.contains_key("RMSE"), "should contain RMSE: {:?}", obj);
        let mae = obj["MAE"].as_f64().unwrap();
        let rmse = obj["RMSE"].as_f64().unwrap();
        assert!(
            mae.is_finite() && mae >= 0.0,
            "MAE should be finite non-negative: {mae}"
        );
        assert!(
            rmse.is_finite() && rmse >= 0.0,
            "RMSE should be finite non-negative: {rmse}"
        );
    }

    #[test]
    fn filter_pick_include() {
        let m1 = TrainedModel {
            id: "lr".into(),
            display_name: "LR".into(),
            estimator: Box::new(augur::prelude::XGBoostClassifier::new()),
        };
        let m2 = TrainedModel {
            id: "rf".into(),
            display_name: "RF".into(),
            estimator: Box::new(augur::prelude::XGBoostClassifier::new()),
        };
        let models = vec![m1, m2];
        let inc = vec!["rf".to_string()];
        let picked = filter_pick(&models, Some(&inc), None);
        assert_eq!(picked.unwrap().id, "rf");
    }

    #[test]
    fn filter_pick_exclude() {
        let m1 = TrainedModel {
            id: "lr".into(),
            display_name: "LR".into(),
            estimator: Box::new(augur::prelude::XGBoostClassifier::new()),
        };
        let m2 = TrainedModel {
            id: "rf".into(),
            display_name: "RF".into(),
            estimator: Box::new(augur::prelude::XGBoostClassifier::new()),
        };
        let models = vec![m1, m2];
        let exc = vec!["lr".to_string()];
        let picked = filter_pick(&models, None, Some(&exc));
        assert_eq!(picked.unwrap().id, "rf");
    }

    #[test]
    fn filter_pick_none_matches_returns_none() {
        let m1 = TrainedModel {
            id: "lr".into(),
            display_name: "LR".into(),
            estimator: Box::new(augur::prelude::XGBoostClassifier::new()),
        };
        let models = vec![m1];
        let inc = vec!["xgboost".to_string()];
        assert!(filter_pick(&models, Some(&inc), None).is_none());
    }
}
