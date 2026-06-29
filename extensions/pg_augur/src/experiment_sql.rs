//! SQL-exposed functions for the Augur DSL experiment API.

use crate::data;
use crate::error::AugurPgError;
use crate::experiment;
use crate::fdw;
use crate::models;
use crate::predict;
use pgrx::prelude::*;

fn err<T>(res: Result<T, AugurPgError>) -> T {
    res.unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"))
}

// ─────────────────────────────────────────────
// experiment() — run a full DSL experiment
// ─────────────────────────────────────────────

#[allow(clippy::type_complexity)]
#[pg_extern(name = "experiment")]
fn experiment_dsl(
    dsl: &str,
) -> TableIterator<
    'static,
    (
        name!(experiment_name, String),
        name!(action, String),
        name!(algorithm, Option<String>),
        name!(metrics, Option<pgrx::JsonB>),
        name!(model_id, Option<i64>),
        name!(fitted_pipeline, Option<String>),
    ),
> {
    let result = err(experiment::run_experiment(dsl));

    let rows: Vec<_> = result
        .actions
        .into_iter()
        .map(|a| {
            (
                result.experiment_name.clone(),
                a.action,
                a.algorithm,
                a.metrics.map(pgrx::JsonB),
                a.model_id,
                a.fitted_pipeline,
            )
        })
        .collect();

    TableIterator::new(rows)
}

// ─────────────────────────────────────────────
// show_experiment() — display stored experiment
// ─────────────────────────────────────────────

#[allow(clippy::type_complexity)]
#[pg_extern]
fn show_experiment(
    experiment_name: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(dsl_source, String),
        name!(fitted_pipeline, Option<String>),
        name!(task, Option<String>),
        name!(best_algorithm, Option<String>),
        name!(best_metrics, Option<pgrx::JsonB>),
        name!(model_id, Option<i64>),
        name!(run_count, i32),
    ),
> {
    let exp = err(models::get_experiment(experiment_name));
    let exp = exp
        .ok_or_else(|| AugurPgError::Other(format!("experiment '{}' not found", experiment_name)))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let row = (
        exp.name,
        exp.dsl_source,
        exp.fitted_pipeline,
        exp.task,
        exp.best_algorithm,
        exp.best_metrics.map(pgrx::JsonB),
        exp.model_id,
        exp.run_count,
    );
    TableIterator::new(std::iter::once(row))
}

// ─────────────────────────────────────────────
// show_pipeline() — display fitted pipeline DSL
// ─────────────────────────────────────────────

#[pg_extern]
fn show_pipeline(experiment_name: &str) -> Option<String> {
    let exp = err(models::get_experiment(experiment_name));
    let exp = exp
        .ok_or_else(|| AugurPgError::Other(format!("experiment '{}' not found", experiment_name)))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));
    exp.fitted_pipeline
}

// ─────────────────────────────────────────────
// export_model() — export the deployed model as a
// self-contained JSON inference artifact
// ─────────────────────────────────────────────

/// Returns the deployed model's full JSON spec (preprocessing pipeline +
/// model weights + feature names + target). This is a portable inference
/// artifact that can be loaded by `augur::Predictor::from_spec()` in any
/// Rust/Python process, or stored externally for deployment.
#[pg_extern]
fn export_model(project_name: &str) -> String {
    let stored = err(models::get_deployed_model(project_name));
    let json = std::str::from_utf8(&stored.artifact)
        .map_err(|e| AugurPgError::Other(format!("artifact not valid utf-8: {e}")))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));
    json.to_string()
}

// ─────────────────────────────────────────────
// inference_schema() — return the inference schema
// for a deployed model (column types, categories,
// numeric ranges, etc.)
// ─────────────────────────────────────────────

/// Builds an `InferenceSchema` from the deployed model's fitted pipeline
/// state. Returns a JSON object describing expected column types, known
/// categories, numeric ranges, and imputer coverage — usable for input
/// validation before prediction.
#[pg_extern]
fn inference_schema(project_name: &str) -> pgrx::JsonB {
    let stored = err(models::get_deployed_model(project_name));
    let json_str = std::str::from_utf8(&stored.artifact)
        .map_err(|e| AugurPgError::Other(format!("artifact not valid utf-8: {e}")))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));
    let spec = err(augur::prelude::load_model_from_string(json_str).map_err(AugurPgError::from));

    // Build schema from the model spec's pipeline state
    let schema = augur_preprocessing::validate::InferenceSchema::from_pipeline_state(
        &spec.preprocessing,
        std::collections::HashMap::new(), // column_info populated from pipeline state
        spec.target_name.clone(),
        spec.ignore_columns.clone(),
    );

    let schema_json = serde_json::to_value(&schema)
        .unwrap_or_else(|e| pgrx::error!("pg_augur: failed to serialize schema: {e}"));
    pgrx::JsonB(schema_json)
}

// ─────────────────────────────────────────────
// predict_validated() — predict with input validation
// ─────────────────────────────────────────────

/// Runs prediction on a single row (JSONB) with input validation. Returns
/// a JSON object with `prediction`, `project`, `algorithm`, and `warnings`
/// (a list of validation issues found in the input).
#[pg_extern]
fn predict_validated(project_name: &str, row: pgrx::JsonB) -> pgrx::JsonB {
    let project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let df = err(data::row_from_jsonb(&row.0, &project.feature_columns));

    let result = err(predict::with_predictor(project_name, |p| {
        let (out_df, report) = p.predict(df)?;
        let value = predict::single_prediction_value(&out_df)?;

        // Collect validation warnings from the report
        let warnings: Vec<String> = report
            .as_ref()
            .map(|r| {
                r.issues
                    .iter()
                    .map(|issue| {
                        let col = issue.column.as_deref().unwrap_or("(global)");
                        format!("{}: {}", col, issue.message)
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(serde_json::json!({
            "prediction": value,
            "project": project_name,
            "algorithm": p.model_id(),
            "warnings": warnings,
        }))
    }));

    pgrx::JsonB(result)
}

// ─────────────────────────────────────────────
// stage_results() — query stored stage results
// ─────────────────────────────────────────────

/// Returns stored results from each training stage (eda, search_pipeline,
/// compare, create) for a project. Each row is one stage execution.
#[allow(clippy::type_complexity)]
#[pg_extern]
fn stage_results(
    project_name: &str,
    stage: default!(Option<String>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(project, String),
        name!(stage, String),
        name!(result, pgrx::JsonB),
    ),
> {
    let stage_filter = match stage {
        Some(ref s) => format!("AND stage = {}", crate::models::quote_literal(s)),
        None => String::new(),
    };
    let sql = format!(
        "SELECT project_name, stage, result
         FROM pgaugur.stage_results
         WHERE project_name = {} {}
         ORDER BY id",
        crate::models::quote_literal(project_name),
        stage_filter,
    );

    let mut rows = Vec::new();
    pgrx::Spi::connect(|client| -> Result<(), pgrx::spi::SpiError> {
        let result = client.select(&sql, None, &[])?;
        for row in result {
            let project: String = row.get(1)?.unwrap_or_default();
            let stage: String = row.get(2)?.unwrap_or_default();
            let result_json: pgrx::JsonB =
                row.get(3)?.unwrap_or(pgrx::JsonB(serde_json::Value::Null));
            rows.push((project, stage, result_json));
        }
        Ok(())
    })
    .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    TableIterator::new(rows)
}

// ─────────────────────────────────────────────
// train() — trigger training on an FDW feature view
// ─────────────────────────────────────────────

/// Triggers training on an FDW feature view. Resolves the chain of
/// foreign table definitions, builds the pipeline, and executes training.
///
/// Returns the project name of the trained model.
///
/// Supports three patterns:
/// - `train('churn_automl')` — pipeline defined in table OPTIONS
/// - `train('churn_features', action => 'compare')` — override action
/// - `train('churn_tune')` — resolves chained foreign tables
#[pg_extern]
fn train(
    feature_view: &str,
    action: default!(Option<String>, "NULL"),
    algorithm: default!(Option<String>, "NULL"),
) -> String {
    // Look up the foreign table OID in pgaugur schema
    let qualified = if feature_view.contains('.') {
        feature_view.to_string()
    } else {
        format!("pgaugur.{}", feature_view)
    };

    let relid = err(fdw::lookup_foreign_table_oid(&qualified));

    // Resolve the chain
    let mut chain = err(fdw::resolve_chain(relid));

    // Project name = the endpoint table name (the one train() was called on)
    let endpoint_name = err(fdw::get_foreign_table_name_pub(relid));
    chain.project_name = endpoint_name;

    // If action override is provided, append/replace
    if let Some(ref act) = action {
        match act.as_str() {
            "compare" => chain.actions.push(fdw::ChainAction::Compare),
            "create" => chain.actions.push(fdw::ChainAction::Create {
                algorithm: algorithm.clone(),
            }),
            other => pgrx::error!("pgaugur: unknown action '{}'", other),
        }
    } else if let Some(ref algo) = algorithm {
        // Shorthand: algorithm implies create
        chain.actions.push(fdw::ChainAction::Create {
            algorithm: Some(algo.clone()),
        });
    }

    // Must have at least one action
    if chain.actions.is_empty() {
        pgrx::error!(
            "pgaugur: no actions defined for '{}'. Add action/tune/deploy OPTIONS or pass action parameter",
            feature_view
        );
    }

    // Execute the chain synchronously
    err(fdw::execute_train_chain(&chain));

    // Return the project name (= foreign table name)
    fdw::get_foreign_table_name_pub(relid).unwrap_or_else(|_| feature_view.to_string())
}

// ─────────────────────────────────────────────
// start_train() — async training via background worker
// ─────────────────────────────────────────────

/// Queues async training on an FDW feature view via the background worker.
/// Returns the job_id immediately.
///
/// Same chain resolution as `train()`, but serializes the FDW column OPTIONS
/// and chain actions into the job config so the background worker can execute
/// without accessing the foreign table catalog.
#[pg_extern]
fn start_train(
    feature_view: &str,
    action: default!(Option<String>, "NULL"),
    algorithm: default!(Option<String>, "NULL"),
) -> i64 {
    let qualified = if feature_view.contains('.') {
        feature_view.to_string()
    } else {
        format!("pgaugur.{}", feature_view)
    };

    let relid = err(fdw::lookup_foreign_table_oid(&qualified));
    let mut chain = err(fdw::resolve_chain(relid));

    // Project name = endpoint table
    let endpoint_name = err(fdw::get_foreign_table_name_pub(relid));
    chain.project_name = endpoint_name.clone();

    // Action overrides
    if let Some(ref act) = action {
        match act.as_str() {
            "compare" => chain.actions.push(fdw::ChainAction::Compare),
            "create" => chain.actions.push(fdw::ChainAction::Create {
                algorithm: algorithm.clone(),
            }),
            other => pgrx::error!("pgaugur: unknown action '{}'", other),
        }
    } else if let Some(ref algo) = algorithm {
        chain.actions.push(fdw::ChainAction::Create {
            algorithm: Some(algo.clone()),
        });
    }

    if chain.actions.is_empty() {
        pgrx::error!(
            "pgaugur: no actions defined for '{}'. Add action/tune/deploy OPTIONS or pass action parameter",
            feature_view
        );
    }

    // Serialize column OPTIONS and chain actions for the background worker
    let col_opts_json = err(serde_json::to_string(&chain.column_options)
        .map_err(|e| crate::error::AugurPgError::Json(e.to_string())));
    let chain_actions_json = err(serde_json::to_string(&chain.actions)
        .map_err(|e| crate::error::AugurPgError::Json(e.to_string())));

    let source_table_qualified = match &chain.source_schema {
        Some(s) => format!("{}.{}", s, chain.source_table),
        None => chain.source_table.clone(),
    };

    let config = crate::async_training::TrainingJobConfig {
        train_size: 1.0 - chain.test_fraction,
        column_options_json: Some(col_opts_json),
        chain_actions_json: Some(chain_actions_json),
        feature_columns: Some(chain.feature_columns.clone()),
        ..Default::default()
    };

    err(crate::async_training::queue_training_job(
        &endpoint_name,
        &source_table_qualified,
        &chain.target_column,
        &chain.task,
        "fdw",
        None,
        &config,
    ))
}
