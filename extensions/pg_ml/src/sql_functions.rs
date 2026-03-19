//! SQL-exposed functions for pg_ml
//!
//! This module contains all the #[pg_extern] functions that are exposed to SQL.
//! These are thin wrappers around internal implementation modules.

use pgrx::prelude::*;
use pyo3::types::PyAnyMethods;

use crate::config::{get_python_path, get_venv_path};
use crate::datasets;
use crate::error::PgMlError;
use crate::models;
use crate::pycaret;
use crate::venv::{ensure_python, ensure_venv, find_uv};

// =============================================================================
// Setup and Status Functions
// =============================================================================

/// Setup the Python virtual environment with PyCaret
#[pg_extern]
pub fn setup_venv() -> String {
    match ensure_venv() {
        Ok(()) => {
            let venv_path = get_venv_path();
            format!(
                "pg_ml: Virtual environment ready at {}",
                venv_path.display()
            )
        }
        Err(e) => {
            pgrx::error!("pg_ml setup_venv failed: {}", e);
        }
    }
}

/// Get information about the Python environment
#[pg_extern]
pub fn python_info() -> String {
    if let Err(e) = ensure_python() {
        pgrx::error!("pg_ml python_info failed: {}", e);
    }

    pyo3::Python::with_gil(|py| {
        let result = py.run(
            c"
import sys
import json

info = {
    'python_version': sys.version,
    'prefix': sys.prefix,
    'executable': sys.executable,
    'packages': {}
}

# Check for key packages
packages = ['pycaret', 'pandas', 'sklearn', 'numpy', 'xgboost', 'lightgbm']
for pkg in packages:
    try:
        mod = __import__(pkg)
        info['packages'][pkg] = getattr(mod, '__version__', 'installed')
    except ImportError:
        info['packages'][pkg] = 'not installed'

result = json.dumps(info, indent=2)
",
            None,
            None,
        );

        match result {
            Ok(_) => {
                // Get the result from Python globals
                match py.import("__main__") {
                    Ok(main) => match main.getattr("result") {
                        Ok(res) => res.extract::<String>().unwrap_or_else(|_| "{}".into()),
                        Err(e) => format!("{{\"error\": \"Failed to get result: {}\"}}", e),
                    },
                    Err(e) => format!("{{\"error\": \"Failed to import __main__: {}\"}}", e),
                }
            }
            Err(e) => {
                format!("{{\"error\": \"{}\"}}", e)
            }
        }
    })
}

/// Returns extension status including venv and Python state
#[pg_extern]
pub fn status() -> String {
    let venv_path = get_venv_path();
    let python_path = get_python_path();
    let venv_exists = python_path.exists();

    let uv_status = match find_uv() {
        Ok(path) => format!("found at {}", path.display()),
        Err(_) => "not found".into(),
    };

    let python_status = if venv_exists {
        match ensure_python() {
            Ok(()) => pyo3::Python::with_gil(|py| py.version().to_string()),
            Err(e) => format!("error: {}", e),
        }
    } else {
        "venv not created".into()
    };

    format!(
        "pg_ml status:\n  venv_path: {}\n  venv_exists: {}\n  uv: {}\n  python: {}",
        venv_path.display(),
        venv_exists,
        uv_status,
        python_status
    )
}

// =============================================================================
// Dataset Functions
// =============================================================================

/// Load a sample dataset from PyCaret into a persistent table
#[pg_extern]
pub fn load_dataset(
    name: &str,
    schema_name: default!(Option<&str>, "NULL"),
    table_name: default!(Option<&str>, "NULL"),
    if_exists: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(schema_name, String),
        name!(table_name, String),
        name!(row_count, i64),
        name!(columns, Vec<String>),
    ),
> {
    let schema = schema_name.unwrap_or("pgml_samples");
    let table = table_name.unwrap_or(name);
    let exists_behavior = if_exists.unwrap_or("error");

    // Validate if_exists parameter
    if !["error", "replace", "skip"].contains(&exists_behavior) {
        pgrx::error!(
            "if_exists must be 'error', 'replace', or 'skip', got '{}'",
            exists_behavior
        );
    }

    // Sanitize identifiers
    let safe_schema = datasets::sanitize_identifier(schema);
    let safe_table = datasets::sanitize_identifier(table);

    match datasets::load_dataset_impl(name, &safe_schema, &safe_table, exists_behavior) {
        Ok(metadata) => TableIterator::once((
            metadata.schema_name,
            metadata.table_name,
            metadata.row_count,
            metadata.columns,
        )),
        Err(e) => {
            pgrx::error!("Failed to load dataset '{}': {}", name, e);
        }
    }
}

/// List available sample datasets from PyCaret
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn list_datasets() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(task, Option<String>),
        name!(rows, Option<i64>),
        name!(features, Option<i64>),
    ),
> {
    match datasets::fetch_dataset_index() {
        Ok(index) => TableIterator::new(
            index
                .into_iter()
                .map(|entry| (entry.name, entry.task, entry.rows, entry.features)),
        ),
        Err(e) => {
            pgrx::error!("Failed to list datasets: {}", e);
        }
    }
}

// =============================================================================
// PyCaret ML Functions
// =============================================================================

/// Initialize a PyCaret experiment on a table
#[pg_extern]
pub fn setup(
    relation_name: &str,
    target: &str,
    project_name: default!(Option<&str>, "NULL"),
    task: default!(Option<&str>, "NULL"),
    exclude_columns: default!(Option<Vec<String>>, "NULL"),
    options: default!(Option<pgrx::JsonB>, "NULL"),
    id_column: default!(Option<&str>, "NULL"),
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
    let (schema_name, table_name) = pycaret::parse_relation_name(relation_name);

    let opts = options.as_ref().map(|j| j.0.clone());

    match pycaret::run_setup(
        &schema_name,
        &table_name,
        target,
        task,
        exclude_columns.as_deref(),
        opts.as_ref(),
        id_column,
    ) {
        Ok(result) => {
            // If project_name is provided, persist the experiment configuration
            if let Some(proj_name) = project_name {
                // Ensure schema exists for storing config
                models::ensure_schema()
                    .unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

                models::store_setup_config(
                    proj_name,
                    &result.task,
                    &result.target_column,
                    &result.feature_columns,
                    &schema_name,
                    &table_name,
                    exclude_columns.as_deref(),
                    opts.as_ref(),
                    id_column,
                )
                .unwrap_or_else(|e| pgrx::error!("Failed to store setup config: {}", e));

                // Store project name in Python state for later reference
                pycaret::set_experiment_project_name(proj_name).unwrap_or_else(|e| {
                    pgrx::warning!("Failed to set project name in state: {}", e)
                });
            }

            TableIterator::once((
                result.experiment_id,
                result.task,
                result.target_column,
                result.feature_columns,
                result.train_size,
                result.fold,
            ))
        }
        Err(e) => {
            pgrx::error!("pgml.setup() failed: {}", e);
        }
    }
}

/// Compare models and return the best one(s)
#[pg_extern]
pub fn compare_models(
    project_name: &str,
    n_select: default!(Option<i32>, "1"),
    sort: default!(Option<&str>, "NULL"),
    include: default!(Option<Vec<String>>, "NULL"),
    exclude: default!(Option<Vec<String>>, "NULL"),
    budget_time: default!(Option<i32>, "1800"),
    deploy: default!(bool, "true"),
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
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Set statement_timeout to allow long-running training
    // Add 5 minute buffer to budget_time for overhead
    let effective_budget = budget_time.unwrap_or(1800); // 30 min default
    let timeout_ms = (effective_budget + 300) * 1000; // budget + 5 min buffer
    pgrx::Spi::run(&format!("SET LOCAL statement_timeout = '{}'", timeout_ms))
        .unwrap_or_else(|e| pgrx::warning!("Could not set statement_timeout: {}", e));

    // Run compare_models
    let start = std::time::Instant::now();
    let result = match pycaret::run_compare_models(
        n_select,
        sort,
        include.as_deref(),
        exclude.as_deref(),
        budget_time,
    ) {
        Ok(r) => r,
        Err(e) => pgrx::error!("pgml.compare_models() failed: {}", e),
    };
    let training_time = start.elapsed().as_secs_f64();

    // Store project and model
    let project_id = models::ensure_project(
        project_name,
        &result.task,
        &result.target_column,
        &result.feature_columns,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to create project: {}", e));

    let model_id = models::store_model(
        project_id,
        &result.algorithm,
        None, // hyperparams
        result.metrics.clone(),
        &result.model_bytes,
        result.label_classes.as_deref(),
        training_time,
        deploy,
        false, // conformal - compare_models doesn't support conformal yet
        None,  // conformal_method
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to store model: {}", e));

    // Store experiment split data for reproducibility (if id_column was provided during setup)
    if let Ok(Some(split_data)) = pycaret::get_experiment_split_data() {
        if let Err(e) = models::store_experiment_split(
            model_id,
            split_data.id_column.as_deref(),
            split_data.train_ids.as_deref(),
            split_data.test_ids.as_deref(),
            split_data.data_hash.as_deref().unwrap_or(""),
            split_data.row_count,
            split_data.session_id,
            split_data.train_size,
        ) {
            pgrx::warning!("Failed to store experiment split data: {}", e);
        }
    }

    TableIterator::once((
        project_id,
        model_id,
        result.algorithm,
        result.task,
        pgrx::JsonB(result.metrics),
        deploy,
    ))
}

/// Train a specific algorithm
#[pg_extern]
pub fn create_model(
    project_name: &str,
    algorithm: &str,
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    deploy: default!(bool, "true"),
    conformal: default!(bool, "false"),
    conformal_method: default!(&str, "'plus'"),
    conformal_cv: default!(i32, "5"),
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
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Set statement_timeout to allow training (10 min default for single model)
    pgrx::Spi::run("SET LOCAL statement_timeout = '600000'") // 10 minutes
        .unwrap_or_else(|e| pgrx::warning!("Could not set statement_timeout: {}", e));

    let hp = hyperparams.as_ref().map(|j| &j.0);

    // Run create_model (with or without conformal prediction)
    let start = std::time::Instant::now();
    let result = if conformal {
        match pycaret::run_create_model_conformal(algorithm, hp, conformal_method, conformal_cv) {
            Ok(r) => r,
            Err(e) => pgrx::error!("pgml.create_model() with conformal failed: {}", e),
        }
    } else {
        match pycaret::run_create_model(algorithm, hp) {
            Ok(r) => r,
            Err(e) => pgrx::error!("pgml.create_model() failed: {}", e),
        }
    };
    let training_time = start.elapsed().as_secs_f64();

    // Store project and model
    let project_id = models::ensure_project(
        project_name,
        &result.task,
        &result.target_column,
        &result.feature_columns,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to create project: {}", e));

    let conformal_method_opt = if conformal {
        Some(conformal_method)
    } else {
        None
    };

    let model_id = models::store_model(
        project_id,
        &result.algorithm,
        hp,
        result.metrics.clone(),
        &result.model_bytes,
        result.label_classes.as_deref(),
        training_time,
        deploy,
        conformal,
        conformal_method_opt,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to store model: {}", e));

    // Store experiment split data for reproducibility (if id_column was provided during setup)
    if let Ok(Some(split_data)) = pycaret::get_experiment_split_data() {
        if let Err(e) = models::store_experiment_split(
            model_id,
            split_data.id_column.as_deref(),
            split_data.train_ids.as_deref(),
            split_data.test_ids.as_deref(),
            split_data.data_hash.as_deref().unwrap_or(""),
            split_data.row_count,
            split_data.session_id,
            split_data.train_size,
        ) {
            pgrx::warning!("Failed to store experiment split data: {}", e);
        }
    }

    TableIterator::once((
        project_id,
        model_id,
        result.algorithm,
        result.task,
        pgrx::JsonB(result.metrics),
        deploy,
        conformal,
    ))
}

// =============================================================================
// Time Series Functions
// =============================================================================

/// Initialize a PyCaret time series experiment on a table
#[allow(clippy::too_many_arguments)]
#[pg_extern]
pub fn setup_timeseries(
    relation_name: &str,
    target: &str,
    index: &str,
    project_name: default!(Option<&str>, "NULL"),
    fh: default!(i32, "12"),
    fold: default!(i32, "3"),
    fold_strategy: default!(&str, "'expanding'"),
    session_id: default!(Option<i32>, "NULL"),
) -> pgrx::JsonB {
    let (schema_name, table_name) = pycaret::parse_relation_name(relation_name);

    let mut opts = serde_json::Map::new();
    if let Some(sid) = session_id {
        opts.insert(
            "session_id".to_string(),
            serde_json::Value::Number(serde_json::Number::from(sid)),
        );
    }
    let options = if opts.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(opts))
    };

    match pycaret::run_setup_timeseries(
        &schema_name,
        &table_name,
        target,
        index,
        fh,
        Some(fold),
        Some(fold_strategy),
        options.as_ref(),
    ) {
        Ok(result) => {
            if let Some(proj_name) = project_name {
                models::ensure_schema()
                    .unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));
                models::store_ts_config(
                    proj_name,
                    target,
                    index,
                    fh,
                    &schema_name,
                    &table_name,
                    options.as_ref(),
                )
                .unwrap_or_else(|e| pgrx::error!("Failed to store TS config: {}", e));
                pycaret::set_experiment_project_name(proj_name)
                    .unwrap_or_else(|e| pgrx::warning!("Failed to set project name: {}", e));
            }
            pgrx::JsonB(serde_json::json!({
                "experiment_id": result.experiment_id,
                "task": "time_series",
                "target_column": result.target_column,
                "index_column": result.index_column,
                "fh": result.fh,
                "fold": result.fold,
                "fold_strategy": result.fold_strategy,
            }))
        }
        Err(e) => {
            pgrx::error!("pgml.setup_timeseries() failed: {}", e);
        }
    }
}

/// Train a specific time series algorithm
#[pg_extern]
pub fn create_ts_model(
    project_name: &str,
    algorithm: &str,
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    deploy: default!(bool, "true"),
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
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Set statement_timeout to allow training (10 min default for single model)
    pgrx::Spi::run("SET LOCAL statement_timeout = '600000'") // 10 minutes
        .unwrap_or_else(|e| pgrx::warning!("Could not set statement_timeout: {}", e));

    let hp = hyperparams.as_ref().map(|j| &j.0);

    let start = std::time::Instant::now();
    let result = match pycaret::run_create_ts_model(algorithm, hp) {
        Ok(r) => r,
        Err(e) => pgrx::error!("pgml.create_ts_model() failed: {}", e),
    };
    let training_time = start.elapsed().as_secs_f64();

    let project_id = models::ensure_project(
        project_name,
        &result.task,
        &result.target_column,
        &result.feature_columns,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to create project: {}", e));

    let model_id = models::store_model(
        project_id,
        &result.algorithm,
        hp,
        result.metrics.clone(),
        &result.model_bytes,
        result.label_classes.as_deref(),
        training_time,
        deploy,
        false,
        None,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to store model: {}", e));

    TableIterator::once((
        project_id,
        model_id,
        result.algorithm,
        result.task,
        pgrx::JsonB(result.metrics),
        deploy,
    ))
}

/// Compare time series models and return the best one(s)
#[pg_extern]
pub fn compare_ts_models(
    project_name: &str,
    n_select: default!(Option<i32>, "1"),
    sort: default!(Option<&str>, "NULL"),
    include: default!(Option<Vec<String>>, "NULL"),
    exclude: default!(Option<Vec<String>>, "NULL"),
    budget_time: default!(Option<i32>, "1800"),
    deploy: default!(bool, "true"),
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
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Set statement_timeout
    let effective_budget = budget_time.unwrap_or(1800);
    let timeout_ms = (effective_budget + 300) * 1000;
    pgrx::Spi::run(&format!("SET LOCAL statement_timeout = '{}'", timeout_ms))
        .unwrap_or_else(|e| pgrx::warning!("Could not set statement_timeout: {}", e));

    let start = std::time::Instant::now();
    let result = match pycaret::run_compare_ts_models(
        n_select,
        sort,
        include.as_deref(),
        exclude.as_deref(),
        budget_time,
    ) {
        Ok(r) => r,
        Err(e) => pgrx::error!("pgml.compare_ts_models() failed: {}", e),
    };
    let training_time = start.elapsed().as_secs_f64();

    let project_id = models::ensure_project(
        project_name,
        &result.task,
        &result.target_column,
        &result.feature_columns,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to create project: {}", e));

    let model_id = models::store_model(
        project_id,
        &result.algorithm,
        None,
        result.metrics.clone(),
        &result.model_bytes,
        result.label_classes.as_deref(),
        training_time,
        deploy,
        false,
        None,
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to store model: {}", e));

    TableIterator::once((
        project_id,
        model_id,
        result.algorithm,
        result.task,
        pgrx::JsonB(result.metrics),
        deploy,
    ))
}

/// Generate forecasts using a deployed time series model
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn forecast(
    project_name: &str,
    fh: default!(Option<i32>, "NULL"),
    return_pred_int: default!(bool, "false"),
    alpha: default!(f64, "0.05"),
) -> TableIterator<
    'static,
    (
        name!(step, i32),
        name!(timestamp, String),
        name!(prediction, f64),
        name!(lower, Option<f64>),
        name!(upper, Option<f64>),
    ),
> {
    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    if deployed.task != "time_series" {
        pgrx::error!(
            "forecast() requires a time_series project, got '{}'",
            deployed.task
        );
    }

    // Get project config to determine forecast horizon and re-setup if needed
    let config = match models::get_project_config(project_name) {
        Ok(Some(c)) => c,
        Ok(None) => pgrx::error!("Project '{}' not found", project_name),
        Err(e) => pgrx::error!("Failed to get project config: {}", e),
    };

    let (source_schema, source_table) = match (&config.source_schema, &config.source_table) {
        (Some(s), Some(t)) => (s.clone(), t.clone()),
        _ => pgrx::error!(
            "Project '{}' has no source table info. Was it created with project_name?",
            project_name
        ),
    };

    let index_col = config.index_column.as_deref().unwrap_or_else(|| {
        pgrx::error!("Project '{}' has no index_column configured", project_name)
    });

    let default_fh = config.forecast_horizon.unwrap_or(12);

    // Re-setup the time series experiment (needed for forecasting context)
    let setup_opts = config.setup_options.clone();
    if let Err(e) = pycaret::run_setup_timeseries(
        &source_schema,
        &source_table,
        &config.target_column,
        index_col,
        default_fh,
        None,
        None,
        setup_opts.as_ref(),
    ) {
        pgrx::error!(
            "Failed to re-initialize TS experiment for forecasting: {}",
            e
        );
    }

    let effective_fh = fh.unwrap_or(default_fh);

    match pycaret::run_forecast(
        &deployed.model_bytes,
        Some(effective_fh),
        return_pred_int,
        alpha,
    ) {
        Ok(rows) => TableIterator::new(
            rows.into_iter()
                .map(|r| (r.step, r.timestamp, r.prediction, r.lower, r.upper)),
        ),
        Err(e) => pgrx::error!("Forecast failed: {}", e),
    }
}

// =============================================================================
// Prediction Functions
// =============================================================================

/// Predict using deployed model with feature array
#[pg_extern]
pub fn predict(project_name: &str, features: Vec<f64>) -> String {
    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    match pycaret::run_predict_single(
        &deployed.model_bytes,
        &features,
        &deployed.feature_columns,
        deployed.label_classes.as_deref(),
    ) {
        Ok(prediction) => prediction,
        Err(e) => pgrx::error!("Prediction failed: {}", e),
    }
}

/// Get class probabilities using deployed model
#[pg_extern]
pub fn predict_proba(project_name: &str, features: Vec<f64>) -> Vec<f64> {
    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    match pycaret::run_predict_proba_single(
        &deployed.model_bytes,
        &features,
        &deployed.feature_columns,
    ) {
        Ok(probas) => probas,
        Err(e) => pgrx::error!("Prediction failed: {}", e),
    }
}

/// Predict with confidence interval using MAPIE conformal prediction
#[pg_extern]
pub fn predict_interval(
    project_name: &str,
    features: Vec<f64>,
    alpha: default!(f64, "0.1"),
) -> pgrx::JsonB {
    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    match pycaret::run_predict_interval_single(
        &deployed.model_bytes,
        &features,
        &deployed.feature_columns,
        alpha,
    ) {
        Ok(result) => pgrx::JsonB(serde_json::json!({
            "prediction": result.prediction,
            "lower": result.lower,
            "upper": result.upper,
            "alpha": result.alpha,
            "has_intervals": result.has_intervals,
        })),
        Err(e) => pgrx::error!("Prediction interval failed: {}", e),
    }
}

/// Predict and return as pg_prob distribution
#[pg_extern]
pub fn predict_dist(
    project_name: &str,
    features: Vec<f64>,
    alpha: default!(f64, "0.1"),
    dist_type: default!(&str, "'triangular'"),
) -> String {
    // Check if pg_prob extension is installed
    let has_pgprob: Option<bool> =
        Spi::get_one("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_prob')")
            .unwrap_or(Some(false));

    if has_pgprob != Some(true) {
        pgrx::error!(
            "pg_prob extension required for predict_dist(). Install with: CREATE EXTENSION pg_prob;"
        );
    }

    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    match pycaret::run_predict_dist_single(
        &deployed.model_bytes,
        &features,
        &deployed.feature_columns,
        alpha,
        dist_type,
    ) {
        Ok(dist_json) => serde_json::to_string(&dist_json)
            .unwrap_or_else(|e| pgrx::error!("Failed to serialize distribution: {}", e)),
        Err(e) => pgrx::error!("Prediction distribution failed: {}", e),
    }
}

/// Batch predict on a table
#[pg_extern]
pub fn predict_batch(
    project_name: &str,
    relation_name: &str,
    id_column: &str,
    feature_columns: default!(Option<Vec<String>>, "NULL"),
) -> TableIterator<'static, (name!(id, String), name!(prediction, String))> {
    let deployed = match models::get_deployed_model(project_name) {
        Ok(Some(m)) => m,
        Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
        Err(e) => pgrx::error!("Model not found: {}", e),
    };

    // Use provided feature_columns or fall back to project's feature_columns
    let mut features = feature_columns.unwrap_or_else(|| deployed.feature_columns.clone());

    // Safety: filter out id_column from features (in case it was incorrectly included during training)
    features.retain(|col| col != id_column);

    if features.is_empty() {
        pgrx::error!("No feature columns specified and project has no stored feature columns");
    }

    let (schema_name, table_name) = pycaret::parse_relation_name(relation_name);

    let results = pycaret::run_predict_batch(
        &deployed.model_bytes,
        &schema_name,
        &table_name,
        id_column,
        &features,
        deployed.label_classes.as_deref(),
    )
    .unwrap_or_else(|e| pgrx::error!("Batch prediction failed: {}", e));

    TableIterator::new(results)
}

// =============================================================================
// Model Management Functions
// =============================================================================

/// Deploy a specific model by ID
#[pg_extern]
pub fn deploy(model_id: i64) -> bool {
    match models::deploy_model(model_id) {
        Ok(result) => result,
        Err(e) => pgrx::error!("Failed to deploy model: {}", e),
    }
}

/// Drop a project and all its models
#[pg_extern]
pub fn drop_project(project_name: &str) -> bool {
    match models::drop_project(project_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("Failed to drop project: {}", e),
    }
}

// =============================================================================
// Experiment Management Functions
// =============================================================================

/// Verify experiment reproducibility
#[pg_extern]
pub fn verify_experiment(
    project_name: &str,
    model_id: default!(Option<i64>, "NULL"),
) -> pgrx::JsonB {
    // Get the model to verify
    let model = if let Some(id) = model_id {
        match models::get_model_by_id(id) {
            Ok(Some(m)) => m,
            Ok(None) => pgrx::error!("Model with ID {} not found", id),
            Err(e) => pgrx::error!("Failed to get model: {}", e),
        }
    } else {
        // Get the deployed model for this project
        match models::get_deployed_model(project_name) {
            Ok(Some(m)) => m,
            Ok(None) => pgrx::error!("No deployed model found for project '{}'", project_name),
            Err(e) => pgrx::error!("Failed to get deployed model: {}", e),
        }
    };

    // Get experiment split data for this model
    let split = match models::get_experiment_split(model.model_id) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return pgrx::JsonB(serde_json::json!({
                "unchanged": null,
                "message": "No experiment tracking data found for this model. Was id_column provided during setup()?",
                "model_id": model.model_id,
            }));
        }
        Err(e) => pgrx::error!("Failed to get experiment split data: {}", e),
    };

    // Get project config to find source table
    let config = match models::get_project_config(project_name) {
        Ok(Some(c)) => c,
        Ok(None) => pgrx::error!("Project configuration not found for '{}'", project_name),
        Err(e) => pgrx::error!("Failed to get project config: {}", e),
    };

    let (source_schema, source_table) = match (config.source_schema, config.source_table) {
        (Some(schema), Some(table)) => (schema, table),
        _ => pgrx::error!(
            "Project '{}' has no source table info. Was it created with project_name parameter?",
            project_name
        ),
    };

    // Compute current data hash
    let current_hash =
        match compute_table_hash(&source_schema, &source_table, config.id_column.as_deref()) {
            Ok(hash) => hash,
            Err(e) => pgrx::error!("Failed to compute current data hash: {}", e),
        };

    let unchanged = current_hash == split.data_hash;
    let message = if unchanged {
        "Data unchanged since training. Experiment is reproducible."
    } else {
        "WARNING: Data has changed since training. Experiment may not be reproducible."
    };

    pgrx::JsonB(serde_json::json!({
        "unchanged": unchanged,
        "current_hash": current_hash,
        "training_hash": split.data_hash,
        "message": message,
        "model_id": model.model_id,
        "row_count": split.row_count,
        "train_rows": split.train_ids.as_ref().map(|ids| ids.len()),
        "test_rows": split.test_ids.as_ref().map(|ids| ids.len()),
        "session_id": split.session_id,
        "train_size": split.train_size,
    }))
}

/// Compute hash for a table's data
fn compute_table_hash(
    schema_name: &str,
    table_name: &str,
    _id_column: Option<&str>,
) -> Result<String, PgMlError> {
    ensure_python()?;

    pyo3::Python::with_gil(|py| {
        // Convert table to DataFrame
        let df = crate::arrow_convert::table_to_dataframe_arrow(
            py,
            schema_name,
            table_name,
            None,
            None,
        )?;

        // Get the PyCaret module to use compute_data_hash
        let module = pycaret::get_pycaret_module_public(py)?;
        let hash_fn = module
            .getattr("compute_data_hash")
            .map_err(PgMlError::from)?;

        let result = hash_fn.call1((df,)).map_err(PgMlError::from)?;
        let hash: String = result.extract().map_err(PgMlError::from)?;

        Ok(hash)
    })
}

/// Load a saved experiment configuration and restore the PyCaret session.
///
/// Also loads the deployed model (if any) into the session, so plot functions
/// like `show_plot()` and `show_interpretation()` work immediately without
/// needing to retrain.
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn load_experiment(
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
    // Get project configuration from database
    let config = match models::get_project_config(project_name) {
        Ok(Some(c)) => c,
        Ok(None) => pgrx::error!("Project '{}' not found", project_name),
        Err(e) => pgrx::error!("Failed to load project config: {}", e),
    };

    // Validate source table info exists
    let (source_schema, source_table) = match (&config.source_schema, &config.source_table) {
        (Some(s), Some(t)) => (s.clone(), t.clone()),
        _ => pgrx::error!(
            "Project '{}' has no source table info. Was it created with project_name parameter?",
            project_name
        ),
    };

    // Re-run setup with the saved configuration (fetches fresh data)
    match pycaret::run_setup(
        &source_schema,
        &source_table,
        &config.target_column,
        Some(&config.task),
        config.exclude_columns.as_deref(),
        config.setup_options.as_ref(),
        config.id_column.as_deref(),
    ) {
        Ok(result) => {
            // Store project name in Python state
            pycaret::set_experiment_project_name(project_name)
                .unwrap_or_else(|e| pgrx::warning!("Failed to set project name in state: {}", e));

            // Load deployed model into session (if one exists)
            let (loaded_model_id, loaded_algorithm) = match models::get_deployed_model(project_name)
            {
                Ok(Some(deployed)) => {
                    let algo = deployed.algorithm.clone();
                    let mid = deployed.model_id;
                    match pycaret::load_model_into_session(
                        &deployed.model_bytes,
                        deployed.model_id,
                        &deployed.algorithm,
                    ) {
                        Ok(()) => (Some(mid), Some(algo)),
                        Err(e) => {
                            pgrx::warning!("Could not load deployed model into session: {}", e);
                            (None, None)
                        }
                    }
                }
                Ok(None) => {
                    pgrx::notice!(
                        "No deployed model found for '{}'. Use create_model() to train one.",
                        project_name
                    );
                    (None, None)
                }
                Err(e) => {
                    pgrx::warning!("Could not fetch deployed model: {}", e);
                    (None, None)
                }
            };

            TableIterator::once((
                result.experiment_id,
                result.task,
                result.target_column,
                result.feature_columns,
                result.train_size,
                result.fold,
                loaded_model_id,
                loaded_algorithm,
            ))
        }
        Err(e) => {
            pgrx::error!("pgml.load_experiment() failed: {}", e);
        }
    }
}

// =============================================================================
// Kernel Functions (ZMQ Jupyter Protocol)
// =============================================================================

/// Initialize the Jupyter kernel and return connection info
#[pg_extern]
pub fn init() -> pgrx::JsonB {
    match pycaret::start_kernel() {
        Ok(connection_info) => pgrx::JsonB(connection_info),
        Err(e) => {
            pgrx::error!("pgml.init() failed: {}", e);
        }
    }
}

// =============================================================================
// Embedding Functions
// =============================================================================

/// Generate an embedding vector for a text input using the configured provider.
///
/// Returns real[] (float4 array). Cast to `::vector` if pgvector is installed.
///
/// Configuration via GUCs:
///   SET pg_ml.embedding_provider = 'openai';
///   SET pg_ml.embedding_api_key = 'sk-...';
///   SET pg_ml.embedding_model = 'text-embedding-3-small';  -- optional
///
/// Examples:
///   SELECT pgml.embed('Hello, world!');
///   SELECT pgml.embed('Hello, world!')::vector;
///   SELECT pgml.embed('Hello', provider => 'voyage', model => 'voyage-code-3');
#[pg_extern]
pub fn embed(
    input: &str,
    provider: default!(Option<&str>, "NULL"),
    model: default!(Option<&str>, "NULL"),
) -> Vec<f32> {
    let result = if provider.is_some() || model.is_some() {
        // Per-call override: build provider with explicit params
        let provider_name = provider.map(|s| s.to_string()).unwrap_or_else(|| {
            crate::embeddings::config::get_provider().unwrap_or_else(|e| pgrx::error!("{}", e))
        });
        let api_key =
            crate::embeddings::config::get_api_key().unwrap_or_else(|e| pgrx::error!("{}", e));
        let model_name = model
            .map(|s| s.to_string())
            .unwrap_or_else(|| crate::embeddings::config::get_model(&provider_name));
        let base_url = crate::embeddings::config::get_api_url(&provider_name);
        let timeout = crate::embeddings::config::get_timeout();

        let p = crate::embeddings::create_provider_with_params(
            &provider_name,
            api_key,
            model_name,
            base_url,
            timeout,
        )
        .unwrap_or_else(|e| pgrx::error!("{}", e));
        p.embed(input)
    } else {
        crate::embeddings::embed(input)
    };

    match result {
        Ok(response) => response.embedding,
        Err(e) => pgrx::error!("pgml.embed() failed: {}", e),
    }
}

/// Generate embeddings for multiple text inputs in a single API call.
///
/// Returns a set of (index, embedding) pairs.
///
/// Example:
///   SELECT * FROM pgml.embed_batch(ARRAY['Hello', 'World', 'Test']);
#[pg_extern]
pub fn embed_batch(
    inputs: Vec<String>,
) -> TableIterator<'static, (name!(index, i32), name!(embedding, Vec<f32>))> {
    let result = match crate::embeddings::embed_batch(&inputs) {
        Ok(response) => response,
        Err(e) => pgrx::error!("pgml.embed_batch() failed: {}", e),
    };

    TableIterator::new(
        result
            .embeddings
            .into_iter()
            .enumerate()
            .map(|(i, emb)| (i as i32, emb)),
    )
}

/// Get information about the configured embedding provider.
///
/// Example:
///   SELECT pgml.embedding_info();
#[pg_extern]
pub fn embedding_info() -> pgrx::JsonB {
    let provider = crate::embeddings::config::EMBEDDING_PROVIDER
        .get()
        .and_then(|s| s.into_string().ok());
    let model = crate::embeddings::config::EMBEDDING_MODEL
        .get()
        .and_then(|s| s.into_string().ok());
    let api_url = crate::embeddings::config::EMBEDDING_API_URL
        .get()
        .and_then(|s| s.into_string().ok());
    let has_key = crate::embeddings::config::EMBEDDING_API_KEY.get().is_some();
    let timeout = crate::embeddings::config::EMBEDDING_TIMEOUT.get();

    pgrx::JsonB(serde_json::json!({
        "provider": provider,
        "model": model,
        "api_url": api_url,
        "api_key_configured": has_key,
        "timeout_seconds": timeout,
    }))
}

// =============================================================================
// Kernel Functions (ZMQ Jupyter Protocol)
// =============================================================================

/// Shutdown the Jupyter kernel
#[pg_extern]
pub fn shutdown() -> bool {
    match pycaret::stop_kernel() {
        Ok(result) => result,
        Err(e) => {
            pgrx::warning!("pgml.shutdown() warning: {}", e);
            false
        }
    }
}

/// Check if the Jupyter kernel is running
#[pg_extern]
pub fn kernel_status() -> bool {
    pycaret::is_kernel_running().unwrap_or_default()
}

/// Get information about the currently active experiment in this session
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn current_experiment() -> TableIterator<
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
    match pycaret::run_get_current_experiment() {
        Ok(Some(exp)) => TableIterator::once((
            exp.project_name,
            exp.task,
            exp.target_column,
            exp.feature_columns,
            exp.data_rows,
            exp.data_cols,
        )),
        Ok(None) => {
            // No experiment active - return row with all NULLs
            TableIterator::once((None, None, None, None, None, None))
        }
        Err(e) => pgrx::error!("Failed to get current experiment: {}", e),
    }
}
