//! Model storage and retrieval for pg_ml
//!
//! Manages the pgml.projects and pgml.models tables for storing
//! trained ML models and their metadata.

use crate::datasets::quote_literal;
use crate::PgMlError;
use pgrx::prelude::*;

// =============================================================================
// Schema Management
// =============================================================================

/// Ensure the pgml schema and tables exist
pub fn ensure_schema() -> Result<(), PgMlError> {
    // Create schema
    Spi::run("CREATE SCHEMA IF NOT EXISTS pgml")
        .map_err(|e| PgMlError::SpiError(format!("Failed to create schema: {}", e)))?;

    // Create projects table
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgml.projects (
            id BIGSERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            task TEXT NOT NULL CHECK (task IN ('classification', 'regression')),
            target_column TEXT NOT NULL,
            feature_columns TEXT[],
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create projects table: {}", e)))?;

    // Create models table
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgml.models (
            id BIGSERIAL PRIMARY KEY,
            project_id BIGINT REFERENCES pgml.projects(id) ON DELETE CASCADE,
            algorithm TEXT NOT NULL,
            hyperparams JSONB,
            metrics JSONB NOT NULL,
            artifact BYTEA NOT NULL,
            label_classes TEXT[],
            deployed BOOLEAN DEFAULT false,
            training_time_seconds FLOAT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create models table: {}", e)))?;

    // Add label_classes column if it doesn't exist (for upgrades)
    Spi::run(
        "DO $$
        BEGIN
            ALTER TABLE pgml.models ADD COLUMN IF NOT EXISTS label_classes TEXT[];
        EXCEPTION
            WHEN duplicate_column THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to add label_classes column: {}", e)))?;

    // Add conformal prediction columns if they don't exist (for upgrades)
    Spi::run(
        "DO $$
        BEGIN
            ALTER TABLE pgml.models ADD COLUMN IF NOT EXISTS conformal BOOLEAN DEFAULT false;
            ALTER TABLE pgml.models ADD COLUMN IF NOT EXISTS conformal_method TEXT;
        EXCEPTION
            WHEN duplicate_column THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to add conformal columns: {}", e)))?;

    // Create unique partial index for deployed models (only one deployed per project)
    // Use DO block to handle "already exists" gracefully
    Spi::run(
        "DO $$
        BEGIN
            CREATE UNIQUE INDEX idx_one_deployed_per_project
            ON pgml.models (project_id) WHERE deployed = true;
        EXCEPTION
            WHEN duplicate_table THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create index: {}", e)))?;

    // Add experiment config columns to projects table (for load_experiment feature)
    Spi::run(
        "DO $$
        BEGIN
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS source_schema TEXT;
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS source_table TEXT;
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS exclude_columns TEXT[];
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS setup_options JSONB;
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS id_column TEXT;
        EXCEPTION
            WHEN duplicate_column THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to add experiment config columns: {}", e)))?;

    // Add time series columns to projects table
    Spi::run(
        "DO $$
        BEGIN
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS index_column TEXT;
            ALTER TABLE pgml.projects ADD COLUMN IF NOT EXISTS forecast_horizon INTEGER;
        EXCEPTION
            WHEN duplicate_column THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to add time series columns: {}", e)))?;

    // Widen task CHECK constraint to include 'time_series' (for upgrades)
    Spi::run(
        "DO $$
        BEGIN
            ALTER TABLE pgml.projects DROP CONSTRAINT IF EXISTS projects_task_check;
            ALTER TABLE pgml.projects ADD CONSTRAINT projects_task_check
                CHECK (task IN ('classification', 'regression', 'time_series'));
        EXCEPTION
            WHEN undefined_object THEN
                BEGIN
                    ALTER TABLE pgml.projects ADD CONSTRAINT projects_task_check
                        CHECK (task IN ('classification', 'regression', 'time_series'));
                EXCEPTION
                    WHEN duplicate_object THEN NULL;
                END;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to update task constraint: {}", e)))?;

    // Create experiment_splits table for reproducibility tracking
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgml.experiment_splits (
            id BIGSERIAL PRIMARY KEY,
            model_id BIGINT REFERENCES pgml.models(id) ON DELETE CASCADE,

            -- Row tracking
            id_column TEXT,
            train_ids TEXT[],
            test_ids TEXT[],

            -- Change detection
            data_hash TEXT NOT NULL,
            row_count INTEGER NOT NULL,

            -- Split parameters
            session_id INTEGER,
            train_size FLOAT,

            -- Optional snapshot
            snapshot_table TEXT,

            created_at TIMESTAMPTZ DEFAULT NOW()
        )",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create experiment_splits table: {}", e)))?;

    // Create index on model_id for fast lookups
    Spi::run(
        "DO $$
        BEGIN
            CREATE INDEX IF NOT EXISTS idx_experiment_splits_model_id
            ON pgml.experiment_splits (model_id);
        EXCEPTION
            WHEN duplicate_table THEN NULL;
        END $$",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create experiment_splits index: {}", e)))?;

    // Create training_jobs table for async training
    Spi::run(
        "CREATE TABLE IF NOT EXISTS pgml.training_jobs (
            id BIGSERIAL PRIMARY KEY,
            project_name TEXT NOT NULL,

            -- Configuration (immutable after creation)
            source_table TEXT NOT NULL,
            target_column TEXT NOT NULL,
            task TEXT NOT NULL,
            mode TEXT NOT NULL CHECK (mode IN ('single', 'automl')),
            algorithm TEXT,
            config JSONB NOT NULL DEFAULT '{}',

            -- State (updated by worker)
            state TEXT NOT NULL DEFAULT 'queued'
                CHECK (state IN ('queued', 'setup', 'training', 'completed', 'failed', 'cancelled')),
            progress FLOAT DEFAULT 0.0,
            current_step TEXT,

            -- AutoML progress
            algorithms_tested INTEGER DEFAULT 0,
            algorithms_total INTEGER,
            current_algorithm TEXT,
            best_result JSONB,

            -- Results
            model_id BIGINT REFERENCES pgml.models(id),
            error_message TEXT,

            -- Timestamps
            created_at TIMESTAMPTZ DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,

            -- Worker tracking
            worker_pid INTEGER
        )",
    )
    .map_err(|e| PgMlError::SpiError(format!("Failed to create training_jobs table: {}", e)))?;

    // Create index for worker polling (find queued jobs quickly)
    Spi::run(
        "DO $$
        BEGIN
            CREATE INDEX IF NOT EXISTS idx_training_jobs_state
            ON pgml.training_jobs(state) WHERE state IN ('queued', 'setup', 'training');
        EXCEPTION
            WHEN duplicate_table THEN NULL;
        END $$",
    )
    .map_err(|e| {
        PgMlError::SpiError(format!("Failed to create training_jobs_state index: {}", e))
    })?;

    // Create index for project queries
    Spi::run(
        "DO $$
        BEGIN
            CREATE INDEX IF NOT EXISTS idx_training_jobs_project
            ON pgml.training_jobs(project_name);
        EXCEPTION
            WHEN duplicate_table THEN NULL;
        END $$",
    )
    .map_err(|e| {
        PgMlError::SpiError(format!(
            "Failed to create training_jobs_project index: {}",
            e
        ))
    })?;

    Ok(())
}

// =============================================================================
// Project Management
// =============================================================================

/// Create or get a project by name
///
/// If a project with the given name exists, returns its ID.
/// Otherwise, creates a new project and returns the new ID.
pub fn ensure_project(
    name: &str,
    task: &str,
    target_column: &str,
    feature_columns: &[String],
) -> Result<i64, PgMlError> {
    // Build feature columns array literal
    let features_array = if feature_columns.is_empty() {
        "NULL".to_string()
    } else {
        let escaped: Vec<String> = feature_columns
            .iter()
            .map(|c| format!("'{}'", c.replace('\'', "''")))
            .collect();
        format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
    };

    // Use INSERT ... ON CONFLICT to handle both cases atomically
    let upsert_sql = format!(
        "INSERT INTO pgml.projects (name, task, target_column, feature_columns)
         VALUES ({}, {}, {}, {})
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
         RETURNING id",
        quote_literal(name),
        quote_literal(task),
        quote_literal(target_column),
        features_array
    );

    Spi::get_one::<i64>(&upsert_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to create project: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No project ID returned".to_string()))
}

/// Get project by name
#[allow(dead_code)]
pub fn get_project(name: &str) -> Result<Option<ProjectInfo>, PgMlError> {
    let sql = format!(
        "SELECT id, name, task, target_column, feature_columns
         FROM pgml.projects WHERE name = {}",
        quote_literal(name)
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let name: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let task: String = row.get(3)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let target: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let features: Vec<String> = row.get(5)?.unwrap_or_default();

            return Ok(Some(ProjectInfo {
                id,
                name,
                task,
                target_column: target,
                feature_columns: features,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| PgMlError::SpiError(format!("Failed to get project: {}", e)))
}

/// Project information
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProjectInfo {
    pub id: i64,
    pub name: String,
    pub task: String,
    pub target_column: String,
    pub feature_columns: Vec<String>,
}

/// Full project configuration including experiment setup options
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProjectConfig {
    pub id: i64,
    pub name: String,
    pub task: String,
    pub target_column: String,
    pub feature_columns: Vec<String>,
    pub source_schema: Option<String>,
    pub source_table: Option<String>,
    pub exclude_columns: Option<Vec<String>>,
    pub setup_options: Option<serde_json::Value>,
    pub id_column: Option<String>,
    pub index_column: Option<String>,
    pub forecast_horizon: Option<i32>,
}

/// Store experiment setup configuration for a project
#[allow(clippy::too_many_arguments)]
pub fn store_setup_config(
    project_name: &str,
    task: &str,
    target_column: &str,
    feature_columns: &[String],
    source_schema: &str,
    source_table: &str,
    exclude_columns: Option<&[String]>,
    setup_options: Option<&serde_json::Value>,
    id_column: Option<&str>,
) -> Result<i64, PgMlError> {
    // Build feature columns array literal
    let features_array = if feature_columns.is_empty() {
        "NULL".to_string()
    } else {
        let escaped: Vec<String> = feature_columns
            .iter()
            .map(|c| format!("'{}'", c.replace('\'', "''")))
            .collect();
        format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
    };

    // Build exclude columns array literal
    let exclude_array = match exclude_columns {
        Some(cols) if !cols.is_empty() => {
            let escaped: Vec<String> = cols
                .iter()
                .map(|c| format!("'{}'", c.replace('\'', "''")))
                .collect();
            format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
        }
        _ => "NULL".to_string(),
    };

    // Build setup options JSONB
    let options_sql = match setup_options {
        Some(opts) => {
            let json_str = serde_json::to_string(opts)
                .map_err(|e| PgMlError::JsonError(format!("Failed to serialize options: {}", e)))?;
            format!("'{}'::jsonb", json_str.replace('\'', "''"))
        }
        None => "NULL".to_string(),
    };

    // Build id_column SQL
    let id_column_sql = match id_column {
        Some(col) => quote_literal(col),
        None => "NULL".to_string(),
    };

    // Use INSERT ... ON CONFLICT to upsert project with full config
    let upsert_sql = format!(
        "INSERT INTO pgml.projects (name, task, target_column, feature_columns, source_schema, source_table, exclude_columns, setup_options, id_column)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})
         ON CONFLICT (name) DO UPDATE SET
            task = EXCLUDED.task,
            target_column = EXCLUDED.target_column,
            feature_columns = EXCLUDED.feature_columns,
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            exclude_columns = EXCLUDED.exclude_columns,
            setup_options = EXCLUDED.setup_options,
            id_column = EXCLUDED.id_column
         RETURNING id",
        quote_literal(project_name),
        quote_literal(task),
        quote_literal(target_column),
        features_array,
        quote_literal(source_schema),
        quote_literal(source_table),
        exclude_array,
        options_sql,
        id_column_sql
    );

    Spi::get_one::<i64>(&upsert_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to store setup config: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No project ID returned".to_string()))
}

/// Store time series experiment configuration for a project
pub fn store_ts_config(
    project_name: &str,
    target_column: &str,
    index_column: &str,
    forecast_horizon: i32,
    source_schema: &str,
    source_table: &str,
    setup_options: Option<&serde_json::Value>,
) -> Result<i64, PgMlError> {
    // Build setup options JSONB
    let options_sql = match setup_options {
        Some(opts) => {
            let json_str = serde_json::to_string(opts)
                .map_err(|e| PgMlError::JsonError(format!("Failed to serialize options: {}", e)))?;
            format!("'{}'::jsonb", json_str.replace('\'', "''"))
        }
        None => "NULL".to_string(),
    };

    let upsert_sql = format!(
        "INSERT INTO pgml.projects (name, task, target_column, feature_columns, source_schema, source_table, setup_options, index_column, forecast_horizon)
         VALUES ({}, 'time_series', {}, ARRAY[]::TEXT[], {}, {}, {}, {}, {})
         ON CONFLICT (name) DO UPDATE SET
            task = EXCLUDED.task,
            target_column = EXCLUDED.target_column,
            feature_columns = EXCLUDED.feature_columns,
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            setup_options = EXCLUDED.setup_options,
            index_column = EXCLUDED.index_column,
            forecast_horizon = EXCLUDED.forecast_horizon
         RETURNING id",
        quote_literal(project_name),
        quote_literal(target_column),
        quote_literal(source_schema),
        quote_literal(source_table),
        options_sql,
        quote_literal(index_column),
        forecast_horizon
    );

    Spi::get_one::<i64>(&upsert_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to store TS config: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No project ID returned".to_string()))
}

/// Get full project configuration including setup options
pub fn get_project_config(project_name: &str) -> Result<Option<ProjectConfig>, PgMlError> {
    let sql = format!(
        "SELECT id, name, task, target_column, feature_columns, source_schema, source_table, exclude_columns, setup_options, id_column, index_column, forecast_horizon
         FROM pgml.projects WHERE name = {}",
        quote_literal(project_name)
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let name: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let task: String = row.get(3)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let target_column: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let feature_columns: Vec<String> = row.get(5)?.unwrap_or_default();
            let source_schema: Option<String> = row.get(6)?;
            let source_table: Option<String> = row.get(7)?;
            let exclude_columns: Option<Vec<String>> = row.get(8)?;
            let setup_options_json: Option<pgrx::JsonB> = row.get(9)?;
            let id_column: Option<String> = row.get(10)?;
            let index_column: Option<String> = row.get(11)?;
            let forecast_horizon: Option<i32> = row.get(12)?;

            let setup_options = setup_options_json.map(|j| j.0);

            return Ok(Some(ProjectConfig {
                id,
                name,
                task,
                target_column,
                feature_columns,
                source_schema,
                source_table,
                exclude_columns,
                setup_options,
                id_column,
                index_column,
                forecast_horizon,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgMlError::SpiError(format!("Failed to get project config: {}", e))
    })
}

// =============================================================================
// Model Management
// =============================================================================

/// Store a trained model
#[allow(clippy::too_many_arguments)]
pub fn store_model(
    project_id: i64,
    algorithm: &str,
    hyperparams: Option<&serde_json::Value>,
    metrics: serde_json::Value,
    model_bytes: &[u8],
    label_classes: Option<&[String]>,
    training_time_seconds: f64,
    deploy: bool,
    conformal: bool,
    conformal_method: Option<&str>,
) -> Result<i64, PgMlError> {
    // If deploying, undeploy any existing deployed model for this project
    if deploy {
        let undeploy_sql = format!(
            "UPDATE pgml.models SET deployed = false WHERE project_id = {} AND deployed = true",
            project_id
        );
        Spi::run(&undeploy_sql)
            .map_err(|e| PgMlError::SpiError(format!("Failed to undeploy old model: {}", e)))?;
    }

    // Insert the new model using SPI with proper escaping
    // For BYTEA, we use the escape format with E'\\x...'
    let hex_bytes = hex::encode(model_bytes);
    let metrics_json = serde_json::to_string(&metrics)
        .map_err(|e| PgMlError::JsonError(format!("Failed to serialize metrics: {}", e)))?;
    let hyperparams_sql = match hyperparams {
        Some(hp) => {
            let hp_json = serde_json::to_string(hp).map_err(|e| {
                PgMlError::JsonError(format!("Failed to serialize hyperparams: {}", e))
            })?;
            format!("'{}'::jsonb", hp_json.replace('\'', "''"))
        }
        None => "NULL".to_string(),
    };

    // Build label_classes array literal
    let label_classes_sql = match label_classes {
        Some(classes) if !classes.is_empty() => {
            let escaped: Vec<String> = classes
                .iter()
                .map(|c| format!("'{}'", c.replace('\'', "''")))
                .collect();
            format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
        }
        _ => "NULL".to_string(),
    };

    // Build conformal_method SQL
    let conformal_method_sql = match conformal_method {
        Some(method) => quote_literal(method),
        None => "NULL".to_string(),
    };

    let insert_sql = format!(
        "INSERT INTO pgml.models (project_id, algorithm, hyperparams, metrics, artifact, label_classes, training_time_seconds, deployed, conformal, conformal_method)
         VALUES ({}, {}, {}, '{}'::jsonb, E'\\\\x{}'::bytea, {}, {}, {}, {}, {})
         RETURNING id",
        project_id,
        quote_literal(algorithm),
        hyperparams_sql,
        metrics_json.replace('\'', "''"),
        hex_bytes,
        label_classes_sql,
        training_time_seconds,
        deploy,
        conformal,
        conformal_method_sql
    );

    Spi::get_one::<i64>(&insert_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to store model: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No model ID returned".to_string()))
}

/// Get deployed model for a project
pub fn get_deployed_model(project_name: &str) -> Result<Option<DeployedModel>, PgMlError> {
    let sql = format!(
        "SELECT m.id, m.artifact, p.feature_columns, p.task, m.label_classes, m.algorithm
         FROM pgml.models m
         JOIN pgml.projects p ON m.project_id = p.id
         WHERE p.name = {} AND m.deployed = true",
        quote_literal(project_name)
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let model_id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let model_bytes: Vec<u8> = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let features: Vec<String> = row.get(3)?.unwrap_or_default();
            let task: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let label_classes: Option<Vec<String>> = row.get(5)?;
            let algorithm: String = row.get(6)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;

            return Ok(Some(DeployedModel {
                model_id,
                model_bytes,
                feature_columns: features,
                task,
                algorithm,
                label_classes,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgMlError::SpiError(format!("Failed to get deployed model: {}", e))
    })
}

/// Get model by ID (for verification purposes)
pub fn get_model_by_id(model_id: i64) -> Result<Option<DeployedModel>, PgMlError> {
    let sql = format!(
        "SELECT m.id, m.artifact, p.feature_columns, p.task, m.label_classes, m.algorithm
         FROM pgml.models m
         JOIN pgml.projects p ON m.project_id = p.id
         WHERE m.id = {}",
        model_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let model_bytes: Vec<u8> = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let features: Vec<String> = row.get(3)?.unwrap_or_default();
            let task: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let label_classes: Option<Vec<String>> = row.get(5)?;
            let algorithm: String = row.get(6)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;

            return Ok(Some(DeployedModel {
                model_id: id,
                model_bytes,
                feature_columns: features,
                task,
                algorithm,
                label_classes,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| PgMlError::SpiError(format!("Failed to get model: {}", e)))
}

/// Deployed model information
#[derive(Debug)]
#[allow(dead_code)]
pub struct DeployedModel {
    pub model_id: i64,
    pub model_bytes: Vec<u8>,
    pub feature_columns: Vec<String>,
    pub task: String,
    pub algorithm: String,
    /// For classification: original class labels for decoding numeric predictions
    pub label_classes: Option<Vec<String>>,
}

/// Deploy a specific model by ID
pub fn deploy_model(model_id: i64) -> Result<bool, PgMlError> {
    // Get the project_id for this model
    let sql = format!("SELECT project_id FROM pgml.models WHERE id = {}", model_id);

    let project_id: i64 = Spi::get_one(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to get project_id: {}", e)))?
        .ok_or_else(|| PgMlError::ModelNotFound(format!("model_id={}", model_id)))?;

    // Undeploy any currently deployed model for this project
    let undeploy_sql = format!(
        "UPDATE pgml.models SET deployed = false WHERE project_id = {} AND deployed = true",
        project_id
    );
    Spi::run(&undeploy_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to undeploy old model: {}", e)))?;

    // Deploy the specified model
    let deploy_sql = format!(
        "UPDATE pgml.models SET deployed = true WHERE id = {}",
        model_id
    );
    Spi::run(&deploy_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to deploy model: {}", e)))?;

    Ok(true)
}

/// Drop a project and all its models
pub fn drop_project(project_name: &str) -> Result<bool, PgMlError> {
    let sql = format!(
        "DELETE FROM pgml.projects WHERE name = {} RETURNING id",
        quote_literal(project_name)
    );

    let deleted: Option<i64> = Spi::get_one(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to drop project: {}", e)))?;

    Ok(deleted.is_some())
}

// =============================================================================
// Experiment Split Management (Reproducibility)
// =============================================================================

/// Information about an experiment's train/test split
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ExperimentSplit {
    pub id: i64,
    pub model_id: i64,
    pub id_column: Option<String>,
    pub train_ids: Option<Vec<String>>,
    pub test_ids: Option<Vec<String>>,
    pub data_hash: String,
    pub row_count: i32,
    pub session_id: Option<i32>,
    pub train_size: Option<f64>,
}

/// Store experiment split information for reproducibility
#[allow(clippy::too_many_arguments)]
pub fn store_experiment_split(
    model_id: i64,
    id_column: Option<&str>,
    train_ids: Option<&[String]>,
    test_ids: Option<&[String]>,
    data_hash: &str,
    row_count: i32,
    session_id: Option<i32>,
    train_size: Option<f64>,
) -> Result<i64, PgMlError> {
    // Build id_column SQL
    let id_column_sql = match id_column {
        Some(col) => quote_literal(col),
        None => "NULL".to_string(),
    };

    // Build train_ids array
    let train_ids_sql = match train_ids {
        Some(ids) if !ids.is_empty() => {
            let escaped: Vec<String> = ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "''")))
                .collect();
            format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
        }
        _ => "NULL".to_string(),
    };

    // Build test_ids array
    let test_ids_sql = match test_ids {
        Some(ids) if !ids.is_empty() => {
            let escaped: Vec<String> = ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "''")))
                .collect();
            format!("ARRAY[{}]::TEXT[]", escaped.join(", "))
        }
        _ => "NULL".to_string(),
    };

    // Build session_id SQL
    let session_id_sql = match session_id {
        Some(sid) => sid.to_string(),
        None => "NULL".to_string(),
    };

    // Build train_size SQL
    let train_size_sql = match train_size {
        Some(ts) => ts.to_string(),
        None => "NULL".to_string(),
    };

    let insert_sql = format!(
        "INSERT INTO pgml.experiment_splits
         (model_id, id_column, train_ids, test_ids, data_hash, row_count, session_id, train_size)
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})
         RETURNING id",
        model_id,
        id_column_sql,
        train_ids_sql,
        test_ids_sql,
        quote_literal(data_hash),
        row_count,
        session_id_sql,
        train_size_sql
    );

    Spi::get_one::<i64>(&insert_sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to store experiment split: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No experiment split ID returned".to_string()))
}

/// Get experiment split for a model
pub fn get_experiment_split(model_id: i64) -> Result<Option<ExperimentSplit>, PgMlError> {
    let sql = format!(
        "SELECT id, model_id, id_column, train_ids, test_ids, data_hash, row_count, session_id, train_size
         FROM pgml.experiment_splits WHERE model_id = {}",
        model_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let model_id: i64 = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let id_column: Option<String> = row.get(3)?;
            let train_ids: Option<Vec<String>> = row.get(4)?;
            let test_ids: Option<Vec<String>> = row.get(5)?;
            let data_hash: String = row.get(6)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let row_count: i32 = row.get(7)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let session_id: Option<i32> = row.get(8)?;
            let train_size: Option<f64> = row.get(9)?;

            return Ok(Some(ExperimentSplit {
                id,
                model_id,
                id_column,
                train_ids,
                test_ids,
                data_hash,
                row_count,
                session_id,
                train_size,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgMlError::SpiError(format!("Failed to get experiment split: {}", e))
    })
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_project_info_construction() {
        let info = ProjectInfo {
            id: 1,
            name: "test_project".to_string(),
            task: "classification".to_string(),
            target_column: "target".to_string(),
            feature_columns: vec!["a".to_string(), "b".to_string()],
        };
        assert_eq!(info.id, 1);
        assert_eq!(info.task, "classification");
    }

    #[test]
    fn test_deployed_model_construction() {
        let model = DeployedModel {
            model_id: 1,
            model_bytes: vec![1, 2, 3],
            feature_columns: vec!["x".to_string(), "y".to_string()],
            task: "regression".to_string(),
            algorithm: "RandomForestRegressor".to_string(),
            label_classes: None,
        };
        assert_eq!(model.model_id, 1);
        assert_eq!(model.model_bytes.len(), 3);
        assert_eq!(model.algorithm, "RandomForestRegressor");
        assert!(model.label_classes.is_none());
    }

    #[test]
    fn test_deployed_model_with_label_classes() {
        let model = DeployedModel {
            model_id: 2,
            model_bytes: vec![4, 5, 6],
            feature_columns: vec!["a".to_string(), "b".to_string()],
            task: "classification".to_string(),
            algorithm: "RandomForestClassifier".to_string(),
            label_classes: Some(vec![
                "setosa".to_string(),
                "versicolor".to_string(),
                "virginica".to_string(),
            ]),
        };
        assert_eq!(model.task, "classification");
        assert_eq!(model.algorithm, "RandomForestClassifier");
        assert_eq!(model.label_classes.as_ref().unwrap().len(), 3);
        assert_eq!(model.label_classes.as_ref().unwrap()[0], "setosa");
    }

    #[test]
    fn test_project_config_construction() {
        let config = ProjectConfig {
            id: 1,
            name: "test_project".to_string(),
            task: "classification".to_string(),
            target_column: "species".to_string(),
            feature_columns: vec!["sepal_length".to_string(), "sepal_width".to_string()],
            source_schema: Some("pgml_samples".to_string()),
            source_table: Some("iris".to_string()),
            exclude_columns: Some(vec!["id".to_string()]),
            setup_options: Some(serde_json::json!({"train_size": 0.8})),
            id_column: Some("id".to_string()),
            index_column: None,
            forecast_horizon: None,
        };
        assert_eq!(config.id, 1);
        assert_eq!(config.source_schema, Some("pgml_samples".to_string()));
        assert_eq!(config.source_table, Some("iris".to_string()));
        assert!(config.setup_options.is_some());
        assert_eq!(config.id_column, Some("id".to_string()));
    }

    #[test]
    fn test_project_config_minimal() {
        let config = ProjectConfig {
            id: 2,
            name: "minimal_project".to_string(),
            task: "regression".to_string(),
            target_column: "target".to_string(),
            feature_columns: vec![],
            source_schema: None,
            source_table: None,
            exclude_columns: None,
            setup_options: None,
            id_column: None,
            index_column: None,
            forecast_horizon: None,
        };
        assert_eq!(config.id, 2);
        assert!(config.source_schema.is_none());
        assert!(config.setup_options.is_none());
        assert!(config.id_column.is_none());
    }

    #[test]
    fn test_experiment_split_construction() {
        let split = ExperimentSplit {
            id: 1,
            model_id: 42,
            id_column: Some("id".to_string()),
            train_ids: Some(vec!["1".to_string(), "3".to_string(), "5".to_string()]),
            test_ids: Some(vec!["2".to_string(), "4".to_string()]),
            data_hash: "abc123".to_string(),
            row_count: 5,
            session_id: Some(42),
            train_size: Some(0.7),
        };
        assert_eq!(split.model_id, 42);
        assert_eq!(split.train_ids.as_ref().unwrap().len(), 3);
        assert_eq!(split.test_ids.as_ref().unwrap().len(), 2);
        assert_eq!(split.data_hash, "abc123");
    }
}
