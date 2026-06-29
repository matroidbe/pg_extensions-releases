//! Core DSL integration: parse Augur DSL, load data from Postgres, run
//! the experiment via `augur::runner::run()`, and store results.

use crate::data;
use crate::error::AugurPgError;
use crate::models;
use crate::task::{infer_task, to_augur_task, PgTask};
use crate::train;

use augur::dsl::emit_fitted_pipeline;
use augur::prelude::{parse_experiment, run, save_model_to_string, DataSource, RunResult};
/// Summary of a single action that was executed.
#[derive(Debug, Clone)]
pub struct ActionResult {
    pub action: String,
    pub algorithm: Option<String>,
    pub metrics: Option<serde_json::Value>,
    pub model_id: Option<i64>,
    pub fitted_pipeline: Option<String>,
}

/// Full result of running an experiment via DSL.
pub struct ExperimentResult {
    pub experiment_name: String,
    pub actions: Vec<ActionResult>,
}

/// Extract the experiment name from DSL source text.
///
/// Looks for `experiment <name> {` and returns `<name>`.
/// Falls back to a generated name if not found.
fn extract_experiment_name(dsl: &str) -> String {
    for line in dsl.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("experiment") {
            let rest = rest.trim();
            // Take the identifier (word chars) before `{`
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                return name;
            }
        }
    }
    format!(
        "experiment_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    )
}

/// Extract the table name from an augur DataSource, interpreting it as
/// a Postgres table reference.
fn table_from_data_source(ds: &DataSource) -> String {
    match ds {
        DataSource::BuiltIn(name) => name.clone(),
        DataSource::CsvFile(path) => path.clone(),
    }
}

/// Run a full Augur experiment from DSL text.
///
/// Parses the DSL, loads data from the referenced Postgres table,
/// runs all actions via `augur::runner::run()`, and stores resulting
/// models and experiment metadata in pgaugur tables.
pub fn run_experiment(dsl: &str) -> Result<ExperimentResult, AugurPgError> {
    // 1. Parse DSL
    let spec = parse_experiment(dsl).map_err(|e| AugurPgError::DslParse(e.to_string()))?;

    // 2. Extract experiment name
    let experiment_name = extract_experiment_name(dsl);

    // 3. Extract data source and load from Postgres
    let ds = spec
        .data_source
        .as_ref()
        .ok_or_else(|| AugurPgError::DslParse("no 'data:' statement in DSL".into()))?;
    let table_ref = table_from_data_source(ds);
    let (schema, table) = data::parse_relation(&table_ref)?;

    // Collect ignore columns from the config so they're excluded from the load
    let ignore_refs: Vec<&str> = spec
        .config
        .ignore_columns
        .iter()
        .map(|s| s.as_str())
        .collect();
    let df = data::load_table(schema.as_deref(), &table, &ignore_refs)?;

    // 4. Determine task and target from the spec config
    let target_column = spec.config.target.clone();

    // Infer or use the task from DSL
    let pg_task = if let Some(task_type) = spec.config.task_type {
        match task_type {
            augur_core::types::TaskType::BinaryClassification
            | augur_core::types::TaskType::MulticlassClassification => PgTask::Classification,
            augur_core::types::TaskType::Regression => PgTask::Regression,
            augur_core::types::TaskType::Forecasting => PgTask::TimeSeries,
            _ => infer_task(&df, &target_column)?,
        }
    } else {
        infer_task(&df, &target_column)?
    };

    // 5. Run the experiment
    let (results, _rewrites) = run(df.clone(), spec).map_err(AugurPgError::from)?;

    // 6. Process results
    let mut action_results = Vec::new();
    let mut best_algorithm: Option<String> = None;
    let mut best_metrics: Option<serde_json::Value> = None;
    let mut stored_model_id: Option<i64> = None;
    let mut fitted_pipeline_dsl: Option<String> = None;

    // Compute feature columns for project storage
    let feature_columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .filter(|c| c != &target_column)
        .collect();

    for result in results {
        match result {
            RunResult::Setup(ref experiment) => {
                // Extract fitted pipeline DSL
                let pipeline_text = emit_fitted_pipeline(&experiment.preprocessing).ok();
                if pipeline_text.is_some() {
                    fitted_pipeline_dsl = pipeline_text.clone();
                }
                action_results.push(ActionResult {
                    action: "setup".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: fitted_pipeline_dsl.clone(),
                });
            }
            RunResult::Compare(ref compare_result) => {
                // Store the best model
                if let Some(best) = compare_result.best() {
                    // We need an experiment to serialize — re-run setup for serialization
                    let n_classes = if pg_task == PgTask::Classification {
                        let col = df.column(&target_column).ok();
                        col.map(|c| c.as_materialized_series().n_unique().unwrap_or(2))
                    } else {
                        None
                    };
                    let task_type = to_augur_task(pg_task, n_classes);

                    let setup_config = augur::prelude::SetupConfig::new(&target_column)
                        .task_type(task_type)
                        .test_fraction(0.3)
                        .seed(42);
                    let setup_experiment = augur::prelude::setup(df.clone(), setup_config)
                        .map_err(AugurPgError::from)?;

                    let metrics = train::compute_metrics_from_experiment(best, &setup_experiment);
                    let artifact = save_model_to_string(best, &setup_experiment)
                        .map_err(AugurPgError::from)?
                        .into_bytes();
                    let label_classes = train::label_classes_for_experiment(&setup_experiment);

                    let project_id = models::upsert_project(
                        &experiment_name,
                        pg_task.as_str(),
                        &target_column,
                        &feature_columns,
                        schema.as_deref(),
                        Some(&table),
                        None,
                        None,
                        None,
                    )?;

                    let mid = models::insert_model(
                        project_id,
                        &best.id,
                        None,
                        &metrics,
                        &artifact,
                        label_classes.as_deref(),
                        true,  // deploy
                        false, // conformal
                        None,
                        0.0,
                    )?;

                    crate::predict::invalidate(&experiment_name);

                    best_algorithm = Some(best.id.clone());
                    best_metrics = Some(metrics.clone());
                    stored_model_id = Some(mid);

                    action_results.push(ActionResult {
                        action: "compare".to_string(),
                        algorithm: Some(best.id.clone()),
                        metrics: Some(metrics),
                        model_id: Some(mid),
                        fitted_pipeline: None,
                    });
                }
            }
            RunResult::Create(ref model, ref experiment) => {
                let metrics = train::compute_metrics_from_experiment(model, experiment);
                let artifact = save_model_to_string(model, experiment)
                    .map_err(AugurPgError::from)?
                    .into_bytes();
                let label_classes = train::label_classes_for_experiment(experiment);

                // Extract fitted pipeline
                let pipeline_text = emit_fitted_pipeline(&experiment.preprocessing).ok();
                if pipeline_text.is_some() {
                    fitted_pipeline_dsl = pipeline_text.clone();
                }

                let project_id = models::upsert_project(
                    &experiment_name,
                    pg_task.as_str(),
                    &target_column,
                    &feature_columns,
                    schema.as_deref(),
                    Some(&table),
                    None,
                    None,
                    None,
                )?;

                let mid = models::insert_model(
                    project_id,
                    &model.id,
                    None,
                    &metrics,
                    &artifact,
                    label_classes.as_deref(),
                    true,  // deploy
                    false, // conformal
                    None,
                    0.0,
                )?;

                crate::predict::invalidate(&experiment_name);

                best_algorithm = Some(model.id.clone());
                best_metrics = Some(metrics.clone());
                stored_model_id = Some(mid);

                action_results.push(ActionResult {
                    action: "create".to_string(),
                    algorithm: Some(model.id.clone()),
                    metrics: Some(metrics),
                    model_id: Some(mid),
                    fitted_pipeline: pipeline_text,
                });
            }
            RunResult::Tune(ref _tune_result) => {
                action_results.push(ActionResult {
                    action: "tune".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Eda(ref _eda_report) => {
                action_results.push(ActionResult {
                    action: "eda".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::SearchPipeline(ref _search_result) => {
                action_results.push(ActionResult {
                    action: "search_pipeline".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Export(ref _path) => {
                action_results.push(ActionResult {
                    action: "export".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Semantic(ref _report) => {
                action_results.push(ActionResult {
                    action: "semantic".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            // Newer augur actions (ensemble/calibrate/threshold/batch-predict/
            // evaluate/finalize). Recorded as informational stages for now;
            // deploying these as predictable models is future work.
            RunResult::Ensemble(ref _ensemble, ref _experiment) => {
                action_results.push(ActionResult {
                    action: "ensemble".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Threshold(ref _threshold_result) => {
                action_results.push(ActionResult {
                    action: "threshold".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Predict(ref _path, ref _n_rows) => {
                action_results.push(ActionResult {
                    action: "predict".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Calibrated(ref _calibration_result) => {
                action_results.push(ActionResult {
                    action: "calibrate".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Evaluate(ref _evaluate_result) => {
                action_results.push(ActionResult {
                    action: "evaluate".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
            RunResult::Finalized(ref _model) => {
                action_results.push(ActionResult {
                    action: "finalize".to_string(),
                    algorithm: None,
                    metrics: None,
                    model_id: None,
                    fitted_pipeline: None,
                });
            }
        }
    }

    // 7. Store experiment metadata
    let source_table_str = match schema.as_deref() {
        Some(s) => format!("{}.{}", s, table),
        None => table.clone(),
    };

    models::upsert_experiment(
        &experiment_name,
        dsl,
        fitted_pipeline_dsl.as_deref(),
        Some(&source_table_str),
        Some(&target_column),
        Some(pg_task.as_str()),
        best_algorithm.as_deref(),
        best_metrics.as_ref(),
        stored_model_id,
    )?;

    Ok(ExperimentResult {
        experiment_name,
        actions: action_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_name_from_dsl() {
        let dsl = r#"
experiment my_model {
    data: iris
    target: species
    compare
}
"#;
        assert_eq!(extract_experiment_name(dsl), "my_model");
    }

    #[test]
    fn extract_name_with_underscores() {
        let dsl = "experiment customer_churn_v2 {\n    data: customers\n}";
        assert_eq!(extract_experiment_name(dsl), "customer_churn_v2");
    }

    #[test]
    fn extract_name_fallback() {
        let dsl = "data: iris\ntarget: species\ncompare";
        let name = extract_experiment_name(dsl);
        assert!(name.starts_with("experiment_"));
    }

    #[test]
    fn table_from_builtin() {
        let ds = DataSource::BuiltIn("iris".to_string());
        assert_eq!(table_from_data_source(&ds), "iris");
    }

    #[test]
    fn table_from_csv_path() {
        let ds = DataSource::CsvFile("public.customers".to_string());
        assert_eq!(table_from_data_source(&ds), "public.customers");
    }
}
