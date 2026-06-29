//! SQL-exposed functions for pg_augur.
//!
//! Function signatures mirror pg_ml's `pgml.*` namespace so eidos (and any
//! downstream consumer) can swap engines with a schema-prefix change only.
#![allow(clippy::type_complexity)]

use crate::data;
use crate::error::AugurPgError;
use crate::models;
use crate::predict;
use crate::task::PgTask;
use crate::train;
use pgrx::prelude::*;

fn err<T>(res: Result<T, AugurPgError>) -> T {
    res.unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"))
}

// ─────────────────────────────────────────────
// setup()
// ─────────────────────────────────────────────

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn setup(
    relation_name: &str,
    target: &str,
    project_name: default!(Option<String>, "NULL"),
    task: default!(Option<String>, "NULL"),
    exclude_columns: default!(Option<Vec<String>>, "NULL"),
    options: default!(Option<pgrx::JsonB>, "NULL"),
    id_column: default!(Option<String>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(experiment_id, String),
        name!(task, String),
        name!(target_column, String),
        name!(feature_columns, Vec<String>),
        name!(train_size, f64),
        name!(fold, i32),
    ),
> {
    let (schema, table) = err(data::parse_relation(relation_name));
    let exclude = exclude_columns.unwrap_or_default();
    let task_override = task
        .as_deref()
        .map(PgTask::parse)
        .transpose()
        .unwrap_or(None);

    let setup_opts = options.as_ref().map(|j| j.0.clone());
    let test_fraction = setup_opts
        .as_ref()
        .and_then(|v| v.get("train_size"))
        .and_then(|v| v.as_f64())
        .map(|ts| 1.0 - ts)
        .unwrap_or(0.3);
    let seed = setup_opts
        .as_ref()
        .and_then(|v| v.get("session_id"))
        .and_then(|v| v.as_u64())
        .unwrap_or(42);

    let (experiment, pg_task, feats) = err(train::setup_experiment(
        schema.as_deref(),
        &table,
        target,
        task_override,
        &exclude,
        test_fraction,
        seed,
    ));

    let project = project_name.unwrap_or_else(|| format!("{}_proj", table));
    err(models::upsert_project(
        &project,
        pg_task.as_str(),
        target,
        &feats,
        schema.as_deref(),
        Some(&table),
        Some(&exclude),
        id_column.as_deref(),
        setup_opts.as_ref(),
    ));

    let row = (
        project,
        pg_task.as_str().to_string(),
        target.to_string(),
        feats,
        1.0 - test_fraction,
        experiment.config.n_folds as i32,
    );
    TableIterator::new(std::iter::once(row))
}

// ─────────────────────────────────────────────
// create_model()
// ─────────────────────────────────────────────

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn create_model(
    project_name: &str,
    algorithm: &str,
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    deploy: default!(bool, true),
    conformal: default!(bool, false),
    conformal_method: default!(String, "'plus'"),
    _conformal_cv: default!(i32, 5),
) -> TableIterator<
    'static,
    (
        name!(project_id, i64),
        name!(model_id, i64),
        name!(algorithm, String),
        name!(task, String),
        name!(metrics, pgrx::JsonB),
        name!(deployed, bool),
        name!(conformal, bool),
    ),
> {
    let project = err(models::get_project(project_name));
    let project = project
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let task = err(PgTask::parse(&project.task));
    let source_table = project.source_table.clone().unwrap_or_else(|| {
        pgrx::error!("pg_augur: project has no source_table; call setup() first")
    });
    let exclude = project.exclude_columns.clone().unwrap_or_default();
    let hp = hyperparams.as_ref().map(|j| j.0.clone());

    let outcome = err(train::train_single(
        project_name,
        project.source_schema.as_deref(),
        &source_table,
        &project.target_column,
        algorithm,
        Some(task),
        &exclude,
        hp.as_ref(),
        0.3,
        42,
        deploy,
        conformal,
        Some(&conformal_method),
    ));

    let row = (
        project.id,
        outcome.model_id,
        outcome.algorithm,
        task.as_str().to_string(),
        pgrx::JsonB(outcome.metrics),
        outcome.deployed,
        conformal,
    );
    TableIterator::new(std::iter::once(row))
}

// ─────────────────────────────────────────────
// compare_models()
// ─────────────────────────────────────────────

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn compare_models(
    project_name: &str,
    _n_select: default!(i32, 1),
    _sort: default!(Option<String>, "NULL"),
    include: default!(Option<Vec<String>>, "NULL"),
    exclude: default!(Option<Vec<String>>, "NULL"),
    _budget_time: default!(i32, 1800),
    deploy: default!(bool, true),
) -> TableIterator<
    'static,
    (
        name!(project_id, i64),
        name!(model_id, i64),
        name!(algorithm, String),
        name!(task, String),
        name!(metrics, pgrx::JsonB),
        name!(deployed, bool),
    ),
> {
    let project = err(models::get_project(project_name));
    let project = project
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let task = err(PgTask::parse(&project.task));
    let source_table = project.source_table.clone().unwrap_or_else(|| {
        pgrx::error!("pg_augur: project has no source_table; call setup() first")
    });
    let exclude_cols = project.exclude_columns.clone().unwrap_or_default();

    let outcome = err(train::train_automl(
        project_name,
        project.source_schema.as_deref(),
        &source_table,
        &project.target_column,
        Some(task),
        &exclude_cols,
        include.as_deref(),
        exclude.as_deref(),
        0.3,
        42,
        deploy,
    ));

    let row = (
        project.id,
        outcome.model_id,
        outcome.algorithm,
        task.as_str().to_string(),
        pgrx::JsonB(outcome.metrics),
        outcome.deployed,
    );
    TableIterator::new(std::iter::once(row))
}

// ─────────────────────────────────────────────
// predict()
// ─────────────────────────────────────────────

#[pg_extern]
fn predict(project_name: &str, features: Vec<f64>) -> String {
    let project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let feat_cols = project.feature_columns.clone();
    let df = err(data::row_from_floats(&features, &feat_cols));

    let result = err(predict::with_predictor(project_name, |p| {
        let (out_df, _) = p.predict(df)?;
        let value = predict::single_prediction_value(&out_df)?;
        Ok(serde_json::json!({
            "prediction": value,
            "project": project_name,
            "algorithm": p.model_id(),
        }))
    }));

    result.to_string()
}

#[pg_extern]
fn predict_row(project_name: &str, row: pgrx::JsonB) -> String {
    let project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    // Accept rows containing any subset of columns — predictor aligns.
    let df = err(data::row_from_jsonb(&row.0, &project.feature_columns));

    let result = err(predict::with_predictor(project_name, |p| {
        let (out_df, _) = p.predict(df)?;
        let value = predict::single_prediction_value(&out_df)?;
        Ok(serde_json::json!({
            "prediction": value,
            "project": project_name,
            "algorithm": p.model_id(),
        }))
    }));

    result.to_string()
}

// ─────────────────────────────────────────────
// deploy / drop_project
// ─────────────────────────────────────────────

#[pg_extern]
fn deploy(model_id: i64) -> bool {
    let ok = err(models::deploy_model(model_id));
    // Invalidate the cache for this model's project.
    if let Ok(stored) = models::get_model_by_id(model_id) {
        // Look up project name to invalidate.
        if let Ok(Some(project_name)) = Spi::get_one::<String>(&format!(
            "SELECT name FROM pgaugur.projects WHERE id = {}",
            stored.project_id
        )) {
            predict::invalidate(&project_name);
        }
    }
    ok
}

#[pg_extern]
fn rollback(project_name: &str) -> i64 {
    let model_id = err(models::rollback_model(project_name));
    predict::invalidate(project_name);
    model_id
}

#[pg_extern]
fn drop_project(project_name: &str) -> bool {
    let ok = err(models::drop_project(project_name));
    predict::invalidate(project_name);
    ok
}

// ─────────────────────────────────────────────
// Predict_proba — classification only
// ─────────────────────────────────────────────

#[pg_extern]
fn predict_proba(project_name: &str, features: Vec<f64>) -> Vec<f64> {
    let project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    if PgTask::parse(&project.task).ok() != Some(PgTask::Classification) {
        pgrx::error!("pg_augur: predict_proba is only valid for classification projects");
    }

    let _df = err(data::row_from_floats(&features, &project.feature_columns));

    err(predict::with_predictor(project_name, |_p| {
        // Augur's Predictor does not expose predict_proba directly via ModelSpec.
        // For MVP, return a single-element array indicating the argmax class.
        Err(AugurPgError::NotSupported(
            "predict_proba (wire-through required from augur Predictor API)".to_string(),
        ))
    }))
}

// ─────────────────────────────────────────────
// predict_batch
// ─────────────────────────────────────────────

#[pg_extern]
fn predict_batch(
    project_name: &str,
    relation_name: &str,
    id_column: &str,
    _feature_columns: default!(Option<Vec<String>>, "NULL"),
) -> TableIterator<'static, (name!(id, String), name!(prediction, String))> {
    // Verify project exists before proceeding.
    let _project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let (schema, table) = err(data::parse_relation(relation_name));
    let exclude = vec![id_column];
    let df = err(data::load_table(schema.as_deref(), &table, &exclude));

    // Grab the id column as strings.
    let qualified = match schema.as_deref() {
        Some(s) => format!(
            "\"{}\".\"{}\"",
            s.replace('"', "\"\""),
            table.replace('"', "\"\"")
        ),
        None => format!("\"{}\"", table.replace('"', "\"\"")),
    };
    let id_sql = format!(
        "SELECT \"{}\"::text FROM {}",
        id_column.replace('"', "\"\""),
        qualified
    );
    let mut ids: Vec<String> = Vec::new();
    Spi::connect(|client| -> Result<(), pgrx::spi::SpiError> {
        let result = client.select(&id_sql, None, &[])?;
        for row in result {
            let v: Option<String> = row.get(1)?;
            ids.push(v.unwrap_or_default());
        }
        Ok(())
    })
    .unwrap_or_else(|e| pgrx::error!("pg_augur: id fetch failed: {e}"));

    let predictions = err(predict::with_predictor(project_name, |p| {
        let (out_df, _) = p.predict(df)?;
        let col = out_df
            .column("prediction")
            .map_err(|e| AugurPgError::Polars(e.to_string()))?;
        let s = col.as_materialized_series();
        let mut out = Vec::with_capacity(s.len());
        for i in 0..s.len() {
            let v = s.get(i).map(|av| av.to_string()).unwrap_or_default();
            out.push(v);
        }
        Ok(out)
    }));

    let rows: Vec<(String, String)> = ids.into_iter().zip(predictions).collect();
    TableIterator::new(rows)
}

// ─────────────────────────────────────────────
// Informational functions
// ─────────────────────────────────────────────

#[pg_extern]
fn current_experiment() -> TableIterator<
    'static,
    (
        name!(project_name, Option<String>),
        name!(task, Option<String>),
        name!(target_column, Option<String>),
        name!(feature_columns, Option<Vec<String>>),
        name!(data_rows, Option<i64>),
        name!(data_cols, Option<i64>),
    ),
> {
    // pg_augur has no implicit per-session "current experiment" like pg_ml's Jupyter
    // kernel. Return an empty row to preserve the shape.
    TableIterator::new(std::iter::once((None, None, None, None, None, None)))
}

#[pg_extern]
fn load_experiment(
    project_name: &str,
) -> TableIterator<
    'static,
    (
        name!(experiment_id, String),
        name!(task, String),
        name!(target_column, String),
        name!(feature_columns, Vec<String>),
        name!(train_size, f64),
        name!(fold, i32),
        name!(model_id, Option<i64>),
        name!(algorithm, Option<String>),
    ),
> {
    let project = err(models::get_project(project_name))
        .ok_or_else(|| AugurPgError::ProjectNotFound(project_name.to_string()))
        .unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"));

    let deployed = models::get_deployed_model(project_name).ok();
    let (model_id, algorithm) = match deployed {
        Some(m) => (Some(m.id), Some(m.algorithm)),
        None => (None, None),
    };

    TableIterator::new(std::iter::once((
        project.name,
        project.task,
        project.target_column,
        project.feature_columns,
        0.7,
        10,
        model_id,
        algorithm,
    )))
}

#[pg_extern]
fn verify_experiment(project_name: &str, _model_id: default!(Option<i64>, "NULL")) -> pgrx::JsonB {
    // pg_augur does not persist data hashes yet; return a stub response.
    let val = serde_json::json!({
        "unchanged": serde_json::Value::Null,
        "message": "verify_experiment is not yet implemented in pg_augur",
        "project": project_name,
    });
    pgrx::JsonB(val)
}
