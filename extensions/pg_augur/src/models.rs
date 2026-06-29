use crate::error::AugurPgError;
use pgrx::prelude::*;

pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn text_array_literal(values: &[String]) -> String {
    if values.is_empty() {
        return "ARRAY[]::TEXT[]".to_string();
    }
    let parts: Vec<String> = values.iter().map(|v| quote_literal(v)).collect();
    format!("ARRAY[{}]::TEXT[]", parts.join(","))
}

fn optional_text(value: Option<&str>) -> String {
    match value {
        Some(v) => quote_literal(v),
        None => "NULL".to_string(),
    }
}

fn optional_text_array(values: Option<&[String]>) -> String {
    match values {
        Some(v) => text_array_literal(v),
        None => "NULL::TEXT[]".to_string(),
    }
}

fn optional_jsonb(value: Option<&serde_json::Value>) -> Result<String, AugurPgError> {
    match value {
        Some(v) => {
            let s = serde_json::to_string(v)?;
            Ok(format!("{}::jsonb", quote_literal(&s)))
        }
        None => Ok("NULL::jsonb".to_string()),
    }
}

pub fn ensure_schema() -> Result<(), AugurPgError> {
    Spi::run("CREATE SCHEMA IF NOT EXISTS pgaugur")
        .map_err(|e| AugurPgError::Spi(format!("create schema: {e}")))?;

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgaugur.projects (
            id BIGSERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            task TEXT NOT NULL CHECK (task IN ('classification', 'regression', 'time_series')),
            target_column TEXT NOT NULL,
            feature_columns TEXT[],
            source_schema TEXT,
            source_table TEXT,
            exclude_columns TEXT[],
            setup_options JSONB,
            id_column TEXT,
            index_column TEXT,
            forecast_horizon INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| AugurPgError::Spi(format!("create pgaugur.projects: {e}")))?;

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgaugur.models (
            id BIGSERIAL PRIMARY KEY,
            project_id BIGINT REFERENCES pgaugur.projects(id) ON DELETE CASCADE,
            algorithm TEXT NOT NULL,
            hyperparams JSONB,
            metrics JSONB NOT NULL,
            artifact BYTEA NOT NULL,
            label_classes TEXT[],
            deployed BOOLEAN DEFAULT false,
            conformal BOOLEAN DEFAULT false,
            conformal_method TEXT,
            training_time_seconds FLOAT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| AugurPgError::Spi(format!("create pgaugur.models: {e}")))?;

    Spi::run(
        "DO $$ BEGIN
            CREATE UNIQUE INDEX idx_pgaugur_one_deployed
                ON pgaugur.models (project_id) WHERE deployed = true;
         EXCEPTION WHEN duplicate_table THEN NULL;
         END $$",
    )
    .map_err(|e| AugurPgError::Spi(format!("create deployed index: {e}")))?;

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgaugur.experiment_splits (
            id BIGSERIAL PRIMARY KEY,
            model_id BIGINT REFERENCES pgaugur.models(id) ON DELETE CASCADE,
            id_column TEXT,
            train_ids TEXT[],
            test_ids TEXT[],
            data_hash TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            session_id INTEGER,
            train_size FLOAT,
            snapshot_table TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| AugurPgError::Spi(format!("create experiment_splits: {e}")))?;

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgaugur.training_jobs (
            id BIGSERIAL PRIMARY KEY,
            project_name TEXT NOT NULL,
            source_table TEXT NOT NULL,
            target_column TEXT NOT NULL,
            task TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (mode IN ('single', 'automl', 'fdw')),
            algorithm TEXT,
            config JSONB NOT NULL DEFAULT '{}',
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'setup', 'training', 'completed', 'failed', 'cancelled')),
            progress FLOAT DEFAULT 0.0,
            current_step TEXT,
            algorithms_tested INTEGER DEFAULT 0,
            algorithms_total INTEGER,
            current_algorithm TEXT,
            best_result JSONB,
            model_id BIGINT REFERENCES pgaugur.models(id),
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            worker_pid INTEGER
        )",
    )
    .map_err(|e| AugurPgError::Spi(format!("create training_jobs: {e}")))?;

    Spi::run(
        "DO $$ BEGIN
            CREATE INDEX idx_pgaugur_jobs_state
                ON pgaugur.training_jobs(state)
                WHERE state IN ('queued', 'setup', 'training');
         EXCEPTION WHEN duplicate_table THEN NULL;
         END $$",
    )
    .map_err(|e| AugurPgError::Spi(format!("create jobs state index: {e}")))?;

    Spi::run(
        "DO $$ BEGIN
            CREATE INDEX idx_pgaugur_jobs_project
                ON pgaugur.training_jobs(project_name);
         EXCEPTION WHEN duplicate_table THEN NULL;
         END $$",
    )
    .map_err(|e| AugurPgError::Spi(format!("create jobs project index: {e}")))?;

    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgaugur.experiments (
            id BIGSERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            dsl_source TEXT NOT NULL,
            fitted_pipeline TEXT,
            source_table TEXT,
            target_column TEXT,
            task TEXT,
            best_algorithm TEXT,
            best_metrics JSONB,
            model_id BIGINT REFERENCES pgaugur.models(id) ON DELETE SET NULL,
            run_count INT DEFAULT 0,
            last_run_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| AugurPgError::Spi(format!("create pgaugur.experiments: {e}")))?;

    Ok(())
}

// ─────────────────────────────────────────────
// Project CRUD
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Project {
    pub id: i64,
    pub name: String,
    pub task: String,
    pub target_column: String,
    pub feature_columns: Vec<String>,
    pub source_schema: Option<String>,
    pub source_table: Option<String>,
    pub exclude_columns: Option<Vec<String>>,
    pub id_column: Option<String>,
    pub index_column: Option<String>,
    pub forecast_horizon: Option<i32>,
}

pub fn get_project(name: &str) -> Result<Option<Project>, AugurPgError> {
    let sql = format!(
        "SELECT id, name, task, target_column, feature_columns,
                source_schema, source_table, exclude_columns,
                id_column, index_column, forecast_horizon
         FROM pgaugur.projects WHERE name = {}",
        quote_literal(name)
    );
    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let name: String = row.get(2)?.unwrap_or_default();
            let task: String = row.get(3)?.unwrap_or_default();
            let target: String = row.get(4)?.unwrap_or_default();
            let features: Vec<String> = row.get(5)?.unwrap_or_default();
            let source_schema: Option<String> = row.get(6)?;
            let source_table: Option<String> = row.get(7)?;
            let exclude_columns: Option<Vec<String>> = row.get(8)?;
            let id_column: Option<String> = row.get(9)?;
            let index_column: Option<String> = row.get(10)?;
            let forecast_horizon: Option<i32> = row.get(11)?;
            return Ok(Some(Project {
                id,
                name,
                task,
                target_column: target,
                feature_columns: features,
                source_schema,
                source_table,
                exclude_columns,
                id_column,
                index_column,
                forecast_horizon,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| AugurPgError::Spi(format!("get_project: {e}")))
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_project(
    name: &str,
    task: &str,
    target_column: &str,
    feature_columns: &[String],
    source_schema: Option<&str>,
    source_table: Option<&str>,
    exclude_columns: Option<&[String]>,
    id_column: Option<&str>,
    setup_options: Option<&serde_json::Value>,
) -> Result<i64, AugurPgError> {
    let sql = format!(
        "INSERT INTO pgaugur.projects
           (name, task, target_column, feature_columns,
            source_schema, source_table, exclude_columns, id_column, setup_options)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})
         ON CONFLICT (name) DO UPDATE SET
            task = EXCLUDED.task,
            target_column = EXCLUDED.target_column,
            feature_columns = EXCLUDED.feature_columns,
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            exclude_columns = EXCLUDED.exclude_columns,
            id_column = EXCLUDED.id_column,
            setup_options = EXCLUDED.setup_options
         RETURNING id",
        quote_literal(name),
        quote_literal(task),
        quote_literal(target_column),
        text_array_literal(feature_columns),
        optional_text(source_schema),
        optional_text(source_table),
        optional_text_array(exclude_columns),
        optional_text(id_column),
        optional_jsonb(setup_options)?,
    );

    Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("upsert_project: {e}")))?
        .ok_or_else(|| AugurPgError::Spi("upsert_project returned no row".into()))
}

pub fn drop_project(name: &str) -> Result<bool, AugurPgError> {
    let sql = format!(
        "DELETE FROM pgaugur.projects WHERE name = {} RETURNING id",
        quote_literal(name)
    );
    match Spi::get_one::<i64>(&sql) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(pgrx::spi::SpiError::InvalidPosition) => Ok(false),
        Err(e) => Err(AugurPgError::Spi(format!("drop_project: {e}"))),
    }
}

// ─────────────────────────────────────────────
// Model CRUD
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoredModel {
    pub id: i64,
    pub project_id: i64,
    pub algorithm: String,
    pub artifact: Vec<u8>,
    pub metrics: serde_json::Value,
    pub label_classes: Option<Vec<String>>,
    pub deployed: bool,
}

fn bytea_literal(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(2 + bytes.len() * 2);
    hex.push_str("\\x");
    for b in bytes {
        hex.push_str(&format!("{:02x}", b));
    }
    format!("'{}'::bytea", hex)
}

#[allow(clippy::too_many_arguments)]
pub fn insert_model(
    project_id: i64,
    algorithm: &str,
    hyperparams: Option<&serde_json::Value>,
    metrics: &serde_json::Value,
    artifact: &[u8],
    label_classes: Option<&[String]>,
    deploy: bool,
    conformal: bool,
    conformal_method: Option<&str>,
    training_time_seconds: f64,
) -> Result<i64, AugurPgError> {
    if deploy {
        let undeploy = format!(
            "UPDATE pgaugur.models SET deployed = false
             WHERE project_id = {} AND deployed = true",
            project_id
        );
        Spi::run(&undeploy).map_err(|e| AugurPgError::Spi(format!("undeploy previous: {e}")))?;
    }

    let sql = format!(
        "INSERT INTO pgaugur.models
           (project_id, algorithm, hyperparams, metrics, artifact,
            label_classes, deployed, conformal, conformal_method, training_time_seconds)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})
         RETURNING id",
        project_id,
        quote_literal(algorithm),
        optional_jsonb(hyperparams)?,
        optional_jsonb(Some(metrics))?,
        bytea_literal(artifact),
        optional_text_array(label_classes),
        deploy,
        conformal,
        optional_text(conformal_method),
        training_time_seconds,
    );

    Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("insert_model: {e}")))?
        .ok_or_else(|| AugurPgError::Spi("insert_model returned no row".into()))
}

pub fn get_deployed_model(project_name: &str) -> Result<StoredModel, AugurPgError> {
    let sql = format!(
        "SELECT m.id, m.project_id, m.algorithm, m.artifact, m.metrics,
                m.label_classes, m.deployed
         FROM pgaugur.models m
         JOIN pgaugur.projects p ON p.id = m.project_id
         WHERE p.name = {} AND m.deployed = true",
        quote_literal(project_name)
    );
    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        let row = result.next().ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let project_id: i64 = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let algorithm: String = row.get(3)?.unwrap_or_default();
        let artifact: Vec<u8> = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let metrics: Option<pgrx::JsonB> = row.get(5)?;
        let label_classes: Option<Vec<String>> = row.get(6)?;
        let deployed: bool = row.get(7)?.unwrap_or(false);
        Ok(StoredModel {
            id,
            project_id,
            algorithm,
            artifact,
            metrics: metrics.map(|j| j.0).unwrap_or(serde_json::Value::Null),
            label_classes,
            deployed,
        })
    })
    .map_err(|_: pgrx::spi::SpiError| AugurPgError::NoDeployedModel(project_name.to_string()))
}

pub fn get_model_by_id(model_id: i64) -> Result<StoredModel, AugurPgError> {
    let sql = format!(
        "SELECT id, project_id, algorithm, artifact, metrics,
                label_classes, deployed
         FROM pgaugur.models WHERE id = {}",
        model_id
    );
    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        let row = result.next().ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let project_id: i64 = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let algorithm: String = row.get(3)?.unwrap_or_default();
        let artifact: Vec<u8> = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
        let metrics: Option<pgrx::JsonB> = row.get(5)?;
        let label_classes: Option<Vec<String>> = row.get(6)?;
        let deployed: bool = row.get(7)?.unwrap_or(false);
        Ok(StoredModel {
            id,
            project_id,
            algorithm,
            artifact,
            metrics: metrics.map(|j| j.0).unwrap_or(serde_json::Value::Null),
            label_classes,
            deployed,
        })
    })
    .map_err(|_: pgrx::spi::SpiError| AugurPgError::Other(format!("model id {model_id} not found")))
}

// ─────────────────────────────────────────────
// Experiment CRUD
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoredExperiment {
    pub id: i64,
    pub name: String,
    pub dsl_source: String,
    pub fitted_pipeline: Option<String>,
    pub source_table: Option<String>,
    pub target_column: Option<String>,
    pub task: Option<String>,
    pub best_algorithm: Option<String>,
    pub best_metrics: Option<serde_json::Value>,
    pub model_id: Option<i64>,
    pub run_count: i32,
}

#[allow(clippy::too_many_arguments)]
pub fn upsert_experiment(
    name: &str,
    dsl_source: &str,
    fitted_pipeline: Option<&str>,
    source_table: Option<&str>,
    target_column: Option<&str>,
    task: Option<&str>,
    best_algorithm: Option<&str>,
    best_metrics: Option<&serde_json::Value>,
    model_id: Option<i64>,
) -> Result<i64, AugurPgError> {
    let model_id_sql = match model_id {
        Some(id) => id.to_string(),
        None => "NULL".to_string(),
    };
    let sql = format!(
        "INSERT INTO pgaugur.experiments
           (name, dsl_source, fitted_pipeline, source_table, target_column,
            task, best_algorithm, best_metrics, model_id, run_count, last_run_at)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, 1, NOW())
         ON CONFLICT (name) DO UPDATE SET
            dsl_source = EXCLUDED.dsl_source,
            fitted_pipeline = EXCLUDED.fitted_pipeline,
            source_table = EXCLUDED.source_table,
            target_column = EXCLUDED.target_column,
            task = EXCLUDED.task,
            best_algorithm = EXCLUDED.best_algorithm,
            best_metrics = EXCLUDED.best_metrics,
            model_id = EXCLUDED.model_id,
            run_count = pgaugur.experiments.run_count + 1,
            last_run_at = NOW()
         RETURNING id",
        quote_literal(name),
        quote_literal(dsl_source),
        optional_text(fitted_pipeline),
        optional_text(source_table),
        optional_text(target_column),
        optional_text(task),
        optional_text(best_algorithm),
        optional_jsonb(best_metrics)?,
        model_id_sql,
    );

    Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("upsert_experiment: {e}")))?
        .ok_or_else(|| AugurPgError::Spi("upsert_experiment returned no row".into()))
}

pub fn get_experiment(name: &str) -> Result<Option<StoredExperiment>, AugurPgError> {
    let sql = format!(
        "SELECT id, name, dsl_source, fitted_pipeline, source_table,
                target_column, task, best_algorithm, best_metrics,
                model_id, run_count
         FROM pgaugur.experiments WHERE name = {}",
        quote_literal(name)
    );
    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let name: String = row.get(2)?.unwrap_or_default();
            let dsl_source: String = row.get(3)?.unwrap_or_default();
            let fitted_pipeline: Option<String> = row.get(4)?;
            let source_table: Option<String> = row.get(5)?;
            let target_column: Option<String> = row.get(6)?;
            let task: Option<String> = row.get(7)?;
            let best_algorithm: Option<String> = row.get(8)?;
            let best_metrics: Option<pgrx::JsonB> = row.get(9)?;
            let model_id: Option<i64> = row.get(10)?;
            let run_count: i32 = row.get(11)?.unwrap_or(0);
            return Ok(Some(StoredExperiment {
                id,
                name,
                dsl_source,
                fitted_pipeline,
                source_table,
                target_column,
                task,
                best_algorithm,
                best_metrics: best_metrics.map(|j| j.0),
                model_id,
                run_count,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| AugurPgError::Spi(format!("get_experiment: {e}")))
}

// ─────────────────────────────────────────────
// Stage Results
// ─────────────────────────────────────────────

/// Insert a stage result for a project.
pub fn insert_stage_result(
    project_name: &str,
    stage: &str,
    result: &serde_json::Value,
) -> Result<i64, AugurPgError> {
    // Delete previous result for same project+stage (keep only latest)
    let del_sql = format!(
        "DELETE FROM pgaugur.stage_results WHERE project_name = {} AND stage = {}",
        quote_literal(project_name),
        quote_literal(stage),
    );
    Spi::run(&del_sql).ok();

    let result_json = serde_json::to_string(result)?;
    let sql = format!(
        "INSERT INTO pgaugur.stage_results (project_name, stage, result)
         VALUES ({}, {}, {}::jsonb) RETURNING id",
        quote_literal(project_name),
        quote_literal(stage),
        quote_literal(&result_json),
    );
    Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("insert_stage_result: {e}")))?
        .ok_or_else(|| AugurPgError::Spi("insert_stage_result returned no row".into()))
}

// ─────────────────────────────────────────────
// Typed Stage Tables
// ─────────────────────────────────────────────

/// Insert a compare result row (one per model in the leaderboard).
#[allow(clippy::too_many_arguments)]
pub fn insert_compare_result(
    project_name: &str,
    algorithm: &str,
    display_name: &str,
    metrics: &serde_json::Value,
    rank: i32,
    is_best: bool,
) -> Result<(), AugurPgError> {
    let accuracy = metrics.get("Accuracy").and_then(|v| v.as_f64());
    let precision = metrics.get("Precision").and_then(|v| v.as_f64());
    let recall = metrics.get("Recall").and_then(|v| v.as_f64());
    let f1 = metrics.get("F1").and_then(|v| v.as_f64());
    let mae = metrics.get("MAE").and_then(|v| v.as_f64());
    let mse = metrics.get("MSE").and_then(|v| v.as_f64());
    let rmse = metrics.get("RMSE").and_then(|v| v.as_f64());
    let r2 = metrics.get("R2").and_then(|v| v.as_f64());

    let sql = format!(
        "INSERT INTO pgaugur.compare_results
           (project_name, algorithm, display_name, accuracy, precision_score,
            recall, f1, mae, mse, rmse, r2, rank, is_best)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        quote_literal(project_name),
        quote_literal(algorithm),
        quote_literal(display_name),
        accuracy.map(|v| v.to_string()).unwrap_or("NULL".into()),
        precision.map(|v| v.to_string()).unwrap_or("NULL".into()),
        recall.map(|v| v.to_string()).unwrap_or("NULL".into()),
        f1.map(|v| v.to_string()).unwrap_or("NULL".into()),
        mae.map(|v| v.to_string()).unwrap_or("NULL".into()),
        mse.map(|v| v.to_string()).unwrap_or("NULL".into()),
        rmse.map(|v| v.to_string()).unwrap_or("NULL".into()),
        r2.map(|v| v.to_string()).unwrap_or("NULL".into()),
        rank,
        is_best,
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("insert_compare_result: {e}")))?;
    Ok(())
}

/// Clear previous compare results for a project.
pub fn clear_compare_results(project_name: &str) -> Result<(), AugurPgError> {
    let sql = format!(
        "DELETE FROM pgaugur.compare_results WHERE project_name = {}",
        quote_literal(project_name),
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("clear_compare_results: {e}")))?;
    Ok(())
}

/// Insert an EDA profile row (one per column).
#[allow(clippy::too_many_arguments)]
pub fn insert_eda_profile(
    project_name: &str,
    column_name: &str,
    dtype: &str,
    null_count: i32,
    null_fraction: f64,
    n_unique: i32,
    recommended_impute: Option<&str>,
    recommended_encode: Option<&str>,
    recommended_scale: Option<&str>,
    reasons: &[String],
) -> Result<(), AugurPgError> {
    let reasons_arr = if reasons.is_empty() {
        "ARRAY[]::TEXT[]".to_string()
    } else {
        let parts: Vec<String> = reasons.iter().map(|r| quote_literal(r)).collect();
        format!("ARRAY[{}]::TEXT[]", parts.join(","))
    };
    let sql = format!(
        "INSERT INTO pgaugur.eda_profiles
           (project_name, column_name, dtype, null_count, null_fraction,
            n_unique, recommended_impute, recommended_encode, recommended_scale, reasons)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        quote_literal(project_name),
        quote_literal(column_name),
        quote_literal(dtype),
        null_count,
        null_fraction,
        n_unique,
        optional_text(recommended_impute),
        optional_text(recommended_encode),
        optional_text(recommended_scale),
        reasons_arr,
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("insert_eda_profile: {e}")))?;
    Ok(())
}

/// Clear previous EDA profiles for a project.
pub fn clear_eda_profiles(project_name: &str) -> Result<(), AugurPgError> {
    let sql = format!(
        "DELETE FROM pgaugur.eda_profiles WHERE project_name = {}",
        quote_literal(project_name),
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("clear_eda_profiles: {e}")))?;
    Ok(())
}

/// Insert a lineage record for a training run.
#[allow(clippy::too_many_arguments)]
pub fn insert_lineage(
    project_name: &str,
    train_rows: i32,
    test_rows: i32,
    total_rows: i32,
    test_fraction: f64,
    seed: u64,
    task: &str,
    target_column: &str,
    feature_names: &[String],
    n_preprocessing_steps: i32,
) -> Result<(), AugurPgError> {
    // Clear previous lineage for this project
    let del_sql = format!(
        "DELETE FROM pgaugur.lineage WHERE project_name = {}",
        quote_literal(project_name),
    );
    Spi::run(&del_sql).ok();

    let features_arr = text_array_literal(feature_names);
    let sql = format!(
        "INSERT INTO pgaugur.lineage
           (project_name, train_rows, test_rows, total_rows, test_fraction,
            seed, task, target_column, feature_names, n_preprocessing_steps)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        quote_literal(project_name),
        train_rows,
        test_rows,
        total_rows,
        test_fraction,
        seed,
        quote_literal(task),
        quote_literal(target_column),
        features_arr,
        n_preprocessing_steps,
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("insert_lineage: {e}")))?;
    Ok(())
}

// ─────────────────────────────────────────────
// Plots
// ─────────────────────────────────────────────

/// Insert a plot image for a project.
pub fn insert_plot(
    project_name: &str,
    plot_type: &str,
    format: &str,
    data: &[u8],
) -> Result<(), AugurPgError> {
    let hex = bytea_literal(data);
    let sql = format!(
        "INSERT INTO pgaugur.plots (project_name, plot_type, format, data)
         VALUES ({}, {}, {}, {})",
        quote_literal(project_name),
        quote_literal(plot_type),
        quote_literal(format),
        hex,
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("insert_plot: {e}")))?;
    Ok(())
}

/// Clear previous plots for a project.
pub fn clear_plots(project_name: &str) -> Result<(), AugurPgError> {
    let sql = format!(
        "DELETE FROM pgaugur.plots WHERE project_name = {}",
        quote_literal(project_name),
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("clear_plots: {e}")))?;
    Ok(())
}

pub fn rollback_model(project_name: &str) -> Result<i64, AugurPgError> {
    let quoted = quote_literal(project_name);

    // Find the currently deployed model for the project.
    let sql = format!(
        "SELECT m.id FROM pgaugur.models m
         JOIN pgaugur.projects p ON p.id = m.project_id
         WHERE p.name = {quoted} AND m.deployed = true"
    );
    let current_id: Option<i64> = Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("rollback lookup deployed: {e}")))?;

    let current_id = current_id.ok_or_else(|| {
        AugurPgError::Other(format!("no deployed model for project '{project_name}'"))
    })?;

    // Find the most recent model before the current one.
    let sql = format!(
        "SELECT m.id FROM pgaugur.models m
         JOIN pgaugur.projects p ON p.id = m.project_id
         WHERE p.name = {quoted} AND m.id <> {current_id}
         ORDER BY m.created_at DESC
         LIMIT 1"
    );
    let previous_id: Option<i64> = Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("rollback lookup previous: {e}")))?;

    let previous_id = previous_id.ok_or_else(|| {
        AugurPgError::Other(format!(
            "no previous model to rollback to for project '{project_name}'"
        ))
    })?;

    deploy_model(previous_id)?;
    Ok(previous_id)
}

pub fn deploy_model(model_id: i64) -> Result<bool, AugurPgError> {
    let project_id = {
        let sql = format!(
            "SELECT project_id FROM pgaugur.models WHERE id = {}",
            model_id
        );
        Spi::get_one::<i64>(&sql)
            .map_err(|e| AugurPgError::Spi(format!("lookup project: {e}")))?
            .ok_or_else(|| AugurPgError::Other(format!("model id {model_id} not found")))?
    };

    let undeploy = format!(
        "UPDATE pgaugur.models SET deployed = false
         WHERE project_id = {} AND deployed = true",
        project_id
    );
    Spi::run(&undeploy).map_err(|e| AugurPgError::Spi(format!("undeploy: {e}")))?;

    let deploy = format!(
        "UPDATE pgaugur.models SET deployed = true WHERE id = {}",
        model_id
    );
    Spi::run(&deploy).map_err(|e| AugurPgError::Spi(format!("deploy: {e}")))?;

    Ok(true)
}
