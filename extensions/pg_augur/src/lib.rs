//! pg_augur: Pure-Rust ML for PostgreSQL (Augur-backed).
//!
//! Mirrors pg_ml's SQL API under the `pgaugur` schema so downstream code
//! (e.g. eidos) can swap engines with only a schema-prefix change.

use pgrx::prelude::*;

mod algorithms;
pub mod async_training;
mod async_training_sql;
mod config;
mod data;
pub mod error;
mod experiment;
mod experiment_sql;
mod fdw;
mod fdw_options;
mod fdw_state;
pub mod models;
mod predict;
mod sql_functions;
mod task;
mod train;

pub use async_training::pg_augur_training_worker_main;
pub use async_training_sql::*;
pub use error::AugurPgError;
pub use experiment_sql::*;
pub use fdw_state::*;
pub use sql_functions::*;

pgrx::pg_module_magic!();

// Create the pgaugur tables at CREATE EXTENSION time so they exist before
// any function is called. The `bootstrap` attribute ensures this SQL runs
// first in the generated extension SQL script. The schema itself is created
// by the control file (schema = 'pgaugur').
pgrx::extension_sql!(
    r#"
CREATE TABLE IF NOT EXISTS pgaugur.projects (
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
);

CREATE TABLE IF NOT EXISTS pgaugur.models (
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
);

DO $$ BEGIN
    CREATE UNIQUE INDEX idx_pgaugur_one_deployed
        ON pgaugur.models (project_id) WHERE deployed = true;
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS pgaugur.experiment_splits (
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
);

CREATE TABLE IF NOT EXISTS pgaugur.training_jobs (
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
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_jobs_state
        ON pgaugur.training_jobs(state)
        WHERE state IN ('queued', 'setup', 'training');
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_jobs_project
        ON pgaugur.training_jobs(project_name);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS pgaugur.experiments (
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
);

-- Keep stage_results for backward compat + misc stages (search_pipeline, create)
CREATE TABLE IF NOT EXISTS pgaugur.stage_results (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    stage TEXT NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_stage_results_project
        ON pgaugur.stage_results(project_name, stage);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Typed table: one row per model per compare run
CREATE TABLE IF NOT EXISTS pgaugur.compare_results (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    display_name TEXT NOT NULL,
    accuracy FLOAT8,
    precision_score FLOAT8,
    recall FLOAT8,
    f1 FLOAT8,
    mae FLOAT8,
    mse FLOAT8,
    rmse FLOAT8,
    r2 FLOAT8,
    rank INT,
    is_best BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_compare_project
        ON pgaugur.compare_results(project_name);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Typed table: one row per column per EDA run
CREATE TABLE IF NOT EXISTS pgaugur.eda_profiles (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    dtype TEXT,
    null_count INT,
    null_fraction FLOAT8,
    n_unique INT,
    recommended_impute TEXT,
    recommended_encode TEXT,
    recommended_scale TEXT,
    reasons TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_eda_project
        ON pgaugur.eda_profiles(project_name);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Typed table: one row per training run
CREATE TABLE IF NOT EXISTS pgaugur.lineage (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    train_rows INT NOT NULL,
    test_rows INT NOT NULL,
    total_rows INT NOT NULL,
    test_fraction FLOAT8,
    seed BIGINT,
    task TEXT NOT NULL,
    target_column TEXT NOT NULL,
    feature_names TEXT[],
    n_preprocessing_steps INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_lineage_project
        ON pgaugur.lineage(project_name);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

-- Typed table: plots generated during training
CREATE TABLE IF NOT EXISTS pgaugur.plots (
    id BIGSERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    plot_type TEXT NOT NULL,
    format TEXT DEFAULT 'png',
    data BYTEA NOT NULL,
    width INT,
    height INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

DO $$ BEGIN
    CREATE INDEX idx_pgaugur_plots_project
        ON pgaugur.plots(project_name);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;
"#,
    name = "pgaugur_bootstrap",
    bootstrap
);

// Register the FDW handler and server.
// We use sql=false on the #[pg_extern] functions so pgrx doesn't generate
// SQL with `internal` return type. Instead we manually create them with
// the correct `fdw_handler` / `void` return types.
// `finalize` ensures this runs after all other SQL entities.
pgrx::extension_sql!(
    r#"
CREATE FUNCTION pgaugur.fdw_handler()
    RETURNS fdw_handler
    LANGUAGE c STRICT
    AS 'MODULE_PATHNAME', 'fdw_handler_wrapper';

CREATE FUNCTION pgaugur.fdw_validator(text[], oid)
    RETURNS void
    LANGUAGE c STRICT
    AS 'MODULE_PATHNAME', 'fdw_validator_wrapper';

DO $$ BEGIN
    CREATE FOREIGN DATA WRAPPER pgaugur_fdw
        HANDLER pgaugur.fdw_handler
        VALIDATOR pgaugur.fdw_validator;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE SERVER pgaugur FOREIGN DATA WRAPPER pgaugur_fdw;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
"#,
    name = "pgaugur_fdw_setup",
    finalize
);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    config::register_gucs();
    // Always register the worker; its main loop checks the enabled GUC.
    async_training::register_background_worker();
}

#[pg_extern]
fn extension_docs() -> &'static str {
    "pg_augur: Pure-Rust ML for PostgreSQL. See pgaugur.* functions."
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    // ─── Helpers ───────────────────────────────────────────────────────

    /// Create the pgaugur schema + tables.
    fn ensure() {
        crate::models::ensure_schema().expect("ensure_schema");
    }

    /// Seed a tiny iris-like table for classification (string labels).
    fn seed_iris() {
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgaugur_test_iris (
                sl float8, sw float8, pl float8, pw float8, species text
             )",
        )
        .expect("create iris table");
        Spi::run("DELETE FROM pgaugur_test_iris").ok();
        Spi::run(
            "INSERT INTO pgaugur_test_iris VALUES
                (5.1,3.5,1.4,0.2,'setosa'),(4.9,3.0,1.4,0.2,'setosa'),
                (4.7,3.2,1.3,0.2,'setosa'),(4.6,3.1,1.5,0.2,'setosa'),
                (5.0,3.6,1.4,0.2,'setosa'),(5.4,3.9,1.7,0.4,'setosa'),
                (7.0,3.2,4.7,1.4,'versicolor'),(6.4,3.2,4.5,1.5,'versicolor'),
                (6.9,3.1,4.9,1.5,'versicolor'),(5.5,2.3,4.0,1.3,'versicolor'),
                (6.5,2.8,4.6,1.5,'versicolor'),(5.7,2.8,4.5,1.3,'versicolor'),
                (6.3,3.3,6.0,2.5,'virginica'),(5.8,2.7,5.1,1.9,'virginica'),
                (7.1,3.0,5.9,2.1,'virginica'),(6.3,2.9,5.6,1.8,'virginica'),
                (6.5,3.0,5.8,2.2,'virginica'),(7.6,3.0,6.6,2.1,'virginica')",
        )
        .expect("insert iris data");
    }

    /// Seed a tiny regression table with independent features.
    fn seed_regression() {
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgaugur_test_reg (
                x1 float8, x2 float8, y float8
             )",
        )
        .expect("create reg table");
        Spi::run("DELETE FROM pgaugur_test_reg").ok();
        // x2 is NOT a linear transform of x1 (avoids collinear matrix for OLS).
        // 20 rows so 70/30 split gives ≥14 train rows (enough for 10-fold CV).
        Spi::run(
            "INSERT INTO pgaugur_test_reg VALUES
                (1,5,8.1),(2,3,7.0),(3,7,13.1),(4,2,8.0),
                (5,9,19.1),(6,1,9.0),(7,8,20.1),(8,4,14.0),
                (9,6,18.1),(10,10,26.0),(11,3,16.1),(12,7,22.0),
                (13,5,21.1),(14,9,28.0),(15,2,19.1),(16,8,27.0),
                (17,4,23.1),(18,6,26.0),(19,10,33.1),(20,1,22.0)",
        )
        .expect("insert reg data");
    }

    // ─── Schema ────────────────────────────────────────────────────────

    #[pg_test]
    fn schema_is_created_on_first_setup() {
        ensure();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name='pgaugur')",
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        assert!(exists);

        for tbl in &["projects", "models", "experiment_splits", "training_jobs"] {
            let q = format!(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables
                 WHERE table_schema='pgaugur' AND table_name='{tbl}')"
            );
            let ok = Spi::get_one::<bool>(&q).ok().flatten().unwrap_or(false);
            assert!(ok, "pgaugur.{tbl} should exist");
        }
    }

    #[pg_test]
    fn schema_idempotent() {
        ensure();
        ensure(); // second call should not error
    }

    // ─── Project CRUD ──────────────────────────────────────────────────

    #[pg_test]
    fn upsert_and_drop_project_round_trip() {
        ensure();
        let feats = vec!["a".to_string(), "b".to_string()];
        let id = crate::models::upsert_project(
            "crud_proj",
            "classification",
            "label",
            &feats,
            None,
            Some("t"),
            None,
            None,
            None,
        )
        .expect("upsert");
        assert!(id > 0);

        let fetched = crate::models::get_project("crud_proj")
            .expect("get")
            .expect("row");
        assert_eq!(fetched.name, "crud_proj");
        assert_eq!(fetched.task, "classification");
        assert_eq!(fetched.target_column, "label");
        assert_eq!(fetched.feature_columns, feats);

        let dropped = crate::models::drop_project("crud_proj").expect("drop");
        assert!(dropped);
        assert!(crate::models::get_project("crud_proj").unwrap().is_none());
    }

    #[pg_test]
    fn upsert_project_updates_on_conflict() {
        ensure();
        let feats1 = vec!["a".to_string()];
        let id1 = crate::models::upsert_project(
            "upd_proj",
            "classification",
            "t1",
            &feats1,
            None,
            Some("src1"),
            None,
            None,
            None,
        )
        .unwrap();

        let feats2 = vec!["a".to_string(), "b".to_string()];
        let id2 = crate::models::upsert_project(
            "upd_proj",
            "regression",
            "t2",
            &feats2,
            None,
            Some("src2"),
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(id1, id2, "upsert should return same id");

        let p = crate::models::get_project("upd_proj").unwrap().unwrap();
        assert_eq!(p.task, "regression");
        assert_eq!(p.target_column, "t2");
        assert_eq!(p.feature_columns, feats2);
        assert_eq!(p.source_table.as_deref(), Some("src2"));

        crate::models::drop_project("upd_proj").ok();
    }

    #[pg_test]
    fn drop_nonexistent_project_returns_false() {
        ensure();
        let dropped = crate::models::drop_project("no_such_proj_xyz").unwrap();
        assert!(!dropped);
    }

    // ─── Model insert / deploy ─────────────────────────────────────────

    #[pg_test]
    fn insert_model_and_deploy() {
        ensure();
        let feats = vec!["x".to_string()];
        let proj_id = crate::models::upsert_project(
            "model_proj",
            "regression",
            "y",
            &feats,
            None,
            Some("t"),
            None,
            None,
            None,
        )
        .unwrap();

        let metrics = serde_json::json!({"MAE": 1.5, "R2": 0.95});
        let artifact = b"fake-model-artifact";

        let m1 = crate::models::insert_model(
            proj_id, "linear", None, &metrics, artifact, None, true, false, None, 0.5,
        )
        .unwrap();
        assert!(m1 > 0);

        let deployed = crate::models::get_deployed_model("model_proj").unwrap();
        assert_eq!(deployed.id, m1);
        assert_eq!(deployed.algorithm, "linear");
        assert!(deployed.deployed);

        // Insert second model and deploy — previous should be undeployed.
        let m2 = crate::models::insert_model(
            proj_id, "rf", None, &metrics, artifact, None, true, false, None, 1.0,
        )
        .unwrap();
        let deployed2 = crate::models::get_deployed_model("model_proj").unwrap();
        assert_eq!(deployed2.id, m2);
        assert_eq!(deployed2.algorithm, "rf");

        crate::models::drop_project("model_proj").ok();
    }

    #[pg_test]
    fn deploy_model_switches_active() {
        ensure();
        let feats = vec!["x".to_string()];
        let proj_id = crate::models::upsert_project(
            "deploy_proj",
            "regression",
            "y",
            &feats,
            None,
            Some("t"),
            None,
            None,
            None,
        )
        .unwrap();
        let metrics = serde_json::json!({"R2": 0.9});
        let art = b"art";

        let m1 = crate::models::insert_model(
            proj_id, "a1", None, &metrics, art, None, true, false, None, 0.0,
        )
        .unwrap();
        let m2 = crate::models::insert_model(
            proj_id, "a2", None, &metrics, art, None, false, false, None, 0.0,
        )
        .unwrap();

        assert_eq!(
            crate::models::get_deployed_model("deploy_proj").unwrap().id,
            m1
        );

        crate::models::deploy_model(m2).unwrap();
        assert_eq!(
            crate::models::get_deployed_model("deploy_proj").unwrap().id,
            m2
        );

        crate::models::drop_project("deploy_proj").ok();
    }

    #[pg_test]
    fn get_deployed_model_none_returns_error() {
        ensure();
        let feats = vec!["x".to_string()];
        crate::models::upsert_project(
            "no_model_proj",
            "regression",
            "y",
            &feats,
            None,
            Some("t"),
            None,
            None,
            None,
        )
        .unwrap();
        let result = crate::models::get_deployed_model("no_model_proj");
        assert!(result.is_err());
        crate::models::drop_project("no_model_proj").ok();
    }

    // ─── Data ingestion ────────────────────────────────────────────────

    #[pg_test]
    fn load_table_iris() {
        seed_iris();
        let df = crate::data::load_table(None, "pgaugur_test_iris", &[]).unwrap();
        assert_eq!(df.height(), 18);
        assert_eq!(df.width(), 5); // sl, sw, pl, pw, species
                                   // Check column types
        assert!(df
            .column("sl")
            .unwrap()
            .as_materialized_series()
            .dtype()
            .is_float());
        assert!(df
            .column("species")
            .unwrap()
            .as_materialized_series()
            .dtype()
            .is_string());
    }

    #[pg_test]
    fn load_table_with_exclude() {
        seed_iris();
        let df = crate::data::load_table(None, "pgaugur_test_iris", &["sw", "pw"]).unwrap();
        assert_eq!(df.width(), 3); // sl, pl, species
        let names: Vec<String> = df
            .get_column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        assert!(!names.contains(&"sw".to_string()));
        assert!(!names.contains(&"pw".to_string()));
    }

    #[pg_test]
    fn load_table_regression() {
        seed_regression();
        let df = crate::data::load_table(None, "pgaugur_test_reg", &[]).unwrap();
        assert_eq!(df.height(), 20);
        assert_eq!(df.width(), 3);
    }

    #[pg_test]
    fn load_table_empty_returns_error() {
        Spi::run("CREATE TABLE IF NOT EXISTS pgaugur_test_empty (x float8)").unwrap();
        Spi::run("DELETE FROM pgaugur_test_empty").ok();
        let result = crate::data::load_table(None, "pgaugur_test_empty", &[]);
        assert!(result.is_err());
    }

    #[pg_test]
    fn load_table_with_nulls() {
        Spi::run("CREATE TABLE IF NOT EXISTS pgaugur_test_nulls (a float8, b text)").unwrap();
        Spi::run("DELETE FROM pgaugur_test_nulls").ok();
        Spi::run("INSERT INTO pgaugur_test_nulls VALUES (1.0, 'x'), (NULL, NULL), (3.0, 'z')")
            .unwrap();
        let df = crate::data::load_table(None, "pgaugur_test_nulls", &[]).unwrap();
        assert_eq!(df.height(), 3);
        // NULL should be represented as polars null, not a crash.
        let a = df.column("a").unwrap().as_materialized_series();
        assert_eq!(a.null_count(), 1);
    }

    #[pg_test]
    fn load_table_mixed_types() {
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgaugur_test_types (
                i int, b bigint, f float4, d float8,
                t text, bl boolean, ts timestamptz DEFAULT now()
             )",
        )
        .unwrap();
        Spi::run("DELETE FROM pgaugur_test_types").ok();
        Spi::run(
            "INSERT INTO pgaugur_test_types (i, b, f, d, t, bl)
             VALUES (1, 100, 1.5, 2.5, 'hello', true),
                    (2, 200, 3.5, 4.5, 'world', false)",
        )
        .unwrap();
        let df = crate::data::load_table(None, "pgaugur_test_types", &[]).unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 7);
    }

    // ─── Setup (sync) ──────────────────────────────────────────────────

    #[pg_test]
    fn setup_classification_iris() {
        ensure();
        seed_iris();
        let result: Result<_, pgrx::spi::SpiError> = Spi::connect(|client| {
            let mut rows = client.select(
                "SELECT experiment_id, task, target_column, train_size, fold
                 FROM pgaugur.setup('pgaugur_test_iris', 'species', 'iris_setup_proj')",
                None,
                &[],
            )?;
            let row = rows.next().expect("setup should return a row");
            let task: String = row.get(2)?.unwrap_or_default();
            let target: String = row.get(3)?.unwrap_or_default();
            let train_size: f64 = row.get(4)?.unwrap_or(0.0);
            Ok(Some((task, target, train_size)))
        });
        let (task, target, train_size) = result.unwrap().unwrap();
        assert_eq!(task, "classification");
        assert_eq!(target, "species");
        assert!(train_size > 0.0 && train_size < 1.0);

        // Project should exist after setup.
        let proj = crate::models::get_project("iris_setup_proj").unwrap();
        assert!(proj.is_some());
        let proj = proj.unwrap();
        assert_eq!(proj.task, "classification");
        assert_eq!(proj.feature_columns.len(), 4); // sl, sw, pl, pw
    }

    #[pg_test]
    fn setup_regression() {
        ensure();
        seed_regression();
        let task = Spi::get_one::<String>(
            "SELECT task FROM pgaugur.setup('pgaugur_test_reg', 'y', 'reg_setup_proj')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(task, "regression");
    }

    #[pg_test]
    fn setup_with_explicit_task() {
        ensure();
        seed_iris();
        let task = Spi::get_one::<String>(
            "SELECT task FROM pgaugur.setup(
                'pgaugur_test_iris', 'species', 'explicit_task_proj',
                task => 'classification'
            )",
        )
        .unwrap()
        .unwrap();
        assert_eq!(task, "classification");
    }

    #[pg_test]
    fn setup_with_exclude_columns() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.setup(
                'pgaugur_test_iris', 'species', 'excl_proj',
                exclude_columns => ARRAY['sw', 'pw']
            )",
        )
        .unwrap();
        let proj = crate::models::get_project("excl_proj").unwrap().unwrap();
        assert_eq!(proj.feature_columns.len(), 2); // sl, pl only
        assert!(proj.feature_columns.contains(&"sl".to_string()));
        assert!(proj.feature_columns.contains(&"pl".to_string()));
    }

    // ─── Create model (sync) ──────────────────────────────────────────

    #[pg_test]
    fn create_model_classification_rf() {
        ensure();
        seed_iris();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_iris', 'species', 'cm_clf_proj')")
            .unwrap();

        let result: Result<_, pgrx::spi::SpiError> = Spi::connect(|client| {
            let mut rows = client.select(
                "SELECT model_id, algorithm, task, deployed, conformal
                 FROM pgaugur.create_model('cm_clf_proj', 'rf')",
                None,
                &[],
            )?;
            let row = rows.next().expect("create_model should return a row");
            let model_id: i64 = row.get(1)?.unwrap_or(0);
            let algorithm: String = row.get(2)?.unwrap_or_default();
            let task: String = row.get(3)?.unwrap_or_default();
            let deployed: bool = row.get(4)?.unwrap_or(false);
            let conformal: bool = row.get(5)?.unwrap_or(false);
            Ok(Some((model_id, algorithm, task, deployed, conformal)))
        });
        let (model_id, algorithm, task, deployed, conformal) = result.unwrap().unwrap();
        assert!(model_id > 0, "model_id should be positive");
        assert_eq!(algorithm, "rf");
        assert_eq!(task, "classification");
        assert!(deployed);
        assert!(!conformal);
    }

    #[pg_test]
    fn create_model_regression_linear() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'cm_reg_proj')").unwrap();

        let result: Result<_, pgrx::spi::SpiError> = Spi::connect(|client| {
            let mut rows = client.select(
                "SELECT model_id, algorithm, task, deployed
                 FROM pgaugur.create_model('cm_reg_proj', 'lr')",
                None,
                &[],
            )?;
            let row = rows.next().expect("create_model should return a row");
            let model_id: i64 = row.get(1)?.unwrap_or(0);
            let algorithm: String = row.get(2)?.unwrap_or_default();
            let task: String = row.get(3)?.unwrap_or_default();
            let deployed: bool = row.get(4)?.unwrap_or(false);
            Ok(Some((model_id, algorithm, task, deployed)))
        });
        let (model_id, algorithm, task, deployed) = result.unwrap().unwrap();
        assert!(model_id > 0);
        assert_eq!(algorithm, "linear");
        assert_eq!(task, "regression");
        assert!(deployed);
    }

    #[pg_test]
    fn create_model_no_deploy() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'nodep_proj')").unwrap();

        let deployed = Spi::get_one::<bool>(
            "SELECT deployed FROM pgaugur.create_model('nodep_proj', 'lr', deploy => false)",
        )
        .unwrap()
        .unwrap();
        assert!(!deployed);

        let result = crate::models::get_deployed_model("nodep_proj");
        assert!(result.is_err(), "no model should be deployed");
    }

    #[pg_test(error = "pg_augur: feature not yet supported in pg_augur: conformal prediction")]
    fn create_model_conformal_returns_error() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'conf_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('conf_proj', 'lr', conformal => true)")
            .unwrap();
    }

    // ─── Predict (sync) ───────────────────────────────────────────────

    #[pg_test]
    fn predict_after_create_model() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'pred_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('pred_proj', 'lr')").unwrap();

        let pred_json =
            Spi::get_one::<String>("SELECT pgaugur.predict('pred_proj', ARRAY[5.0, 10.0])")
                .unwrap()
                .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&pred_json).unwrap();
        assert!(
            parsed.get("prediction").is_some(),
            "should have prediction key: {pred_json}"
        );
        assert_eq!(parsed["project"].as_str().unwrap(), "pred_proj");
        // For near-linear data (y≈3*x1), prediction for x1=5,x2=10 should be ~15.
        let pred = parsed["prediction"].as_f64().unwrap();
        assert!(pred > 5.0 && pred < 50.0, "prediction out of range: {pred}");
    }

    #[pg_test]
    fn predict_row_jsonb() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'predrow_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('predrow_proj', 'lr')").unwrap();

        let pred_json = Spi::get_one::<String>(
            "SELECT pgaugur.predict_row('predrow_proj', '{\"x1\": 5.0, \"x2\": 10.0}'::jsonb)",
        )
        .unwrap()
        .unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&pred_json).unwrap();
        assert!(parsed.get("prediction").is_some());
    }

    #[pg_test(error = "pg_augur: project 'nonexistent_proj_xyz' not found")]
    fn predict_nonexistent_project_returns_error() {
        ensure();
        Spi::run("SELECT pgaugur.predict('nonexistent_proj_xyz', ARRAY[1.0, 2.0])").unwrap();
    }

    // ─── Compare models ───────────────────────────────────────────────

    #[pg_test]
    fn compare_models_returns_best() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'cmp_proj')").unwrap();

        let result: Result<_, pgrx::spi::SpiError> = Spi::connect(|client| {
            let mut rows = client.select(
                "SELECT model_id, algorithm, deployed
                 FROM pgaugur.compare_models('cmp_proj')",
                None,
                &[],
            )?;
            let row = rows.next().expect("compare_models should return a row");
            let model_id: i64 = row.get(1)?.unwrap_or(0);
            let algorithm: String = row.get(2)?.unwrap_or_default();
            let deployed: bool = row.get(3)?.unwrap_or(false);
            Ok(Some((model_id, algorithm, deployed)))
        });
        let (model_id, algorithm, deployed) = result.unwrap().unwrap();
        assert!(model_id > 0);
        assert!(!algorithm.is_empty(), "algorithm should not be empty");
        assert!(deployed, "best model should be auto-deployed");
    }

    // ─── Deploy / Drop via SQL ────────────────────────────────────────

    #[pg_test]
    fn deploy_via_sql() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'deploy_sql_proj')")
            .unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('deploy_sql_proj', 'lr', deploy => false)")
            .unwrap();

        // Get the model id.
        let model_id = Spi::get_one::<i64>(
            "SELECT m.id FROM pgaugur.models m
             JOIN pgaugur.projects p ON p.id = m.project_id
             WHERE p.name = 'deploy_sql_proj'",
        )
        .unwrap()
        .unwrap();

        let ok = Spi::get_one::<bool>(&format!("SELECT pgaugur.deploy({})", model_id))
            .unwrap()
            .unwrap();
        assert!(ok);

        let deployed = crate::models::get_deployed_model("deploy_sql_proj").unwrap();
        assert_eq!(deployed.id, model_id);
    }

    #[pg_test]
    fn drop_project_via_sql() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'dropme_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('dropme_proj', 'lr')").unwrap();

        let ok = Spi::get_one::<bool>("SELECT pgaugur.drop_project('dropme_proj')")
            .unwrap()
            .unwrap();
        assert!(ok);

        assert!(crate::models::get_project("dropme_proj").unwrap().is_none());
    }

    // ─── Rollback ─────────────────────────────────────────────────────

    #[pg_test]
    fn rollback_deploys_previous_model() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'rollback_proj')").unwrap();

        // Create two models — both deployed in sequence.
        Spi::run("SELECT * FROM pgaugur.create_model('rollback_proj', 'lr')").unwrap();
        let first_id = Spi::get_one::<i64>(
            "SELECT m.id FROM pgaugur.models m
             JOIN pgaugur.projects p ON p.id = m.project_id
             WHERE p.name = 'rollback_proj' AND m.deployed = true",
        )
        .unwrap()
        .unwrap();

        Spi::run("SELECT * FROM pgaugur.create_model('rollback_proj', 'ridge')").unwrap();
        let second_id = Spi::get_one::<i64>(
            "SELECT m.id FROM pgaugur.models m
             JOIN pgaugur.projects p ON p.id = m.project_id
             WHERE p.name = 'rollback_proj' AND m.deployed = true",
        )
        .unwrap()
        .unwrap();

        assert_ne!(first_id, second_id);

        // Rollback should re-deploy the first model.
        let rolled_back_id = Spi::get_one::<i64>("SELECT pgaugur.rollback('rollback_proj')")
            .unwrap()
            .unwrap();
        assert_eq!(rolled_back_id, first_id);

        let deployed = crate::models::get_deployed_model("rollback_proj").unwrap();
        assert_eq!(deployed.id, first_id);
    }

    #[pg_test]
    fn rollback_no_previous_model_errors() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'rollback_one_proj')")
            .unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('rollback_one_proj', 'lr')").unwrap();

        // Only one model — rollback should fail.
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<i64>("SELECT pgaugur.rollback('rollback_one_proj')").unwrap();
        });
        assert!(result.is_err());
    }

    #[pg_test]
    fn rollback_no_deployed_model_errors() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'rollback_none_proj')")
            .unwrap();

        // No models at all — rollback should fail.
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<i64>("SELECT pgaugur.rollback('rollback_none_proj')").unwrap();
        });
        assert!(result.is_err());
    }

    // ─── Predict batch ────────────────────────────────────────────────

    #[pg_test]
    fn predict_batch_returns_rows() {
        ensure();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgaugur_test_batch (
                id serial, x1 float8, x2 float8, y float8
             )",
        )
        .unwrap();
        Spi::run("DELETE FROM pgaugur_test_batch").ok();
        // x2 is NOT a linear transform of x1 (avoids collinear matrix for OLS).
        Spi::run(
            "INSERT INTO pgaugur_test_batch (x1, x2, y) VALUES
                (1,5,9.5),(2,3,8.5),(3,7,16.5),(4,2,11.0),(5,9,23.5),
                (6,1,13.5),(7,8,26.0),(8,4,22.0),(9,6,27.0),(10,10,35.0)",
        )
        .unwrap();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_batch', 'y', 'batch_proj', exclude_columns => ARRAY['id'])").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('batch_proj', 'lr')").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgaugur.predict_batch('batch_proj', 'pgaugur_test_batch', 'id')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 10, "should return one prediction per input row");
    }

    // ─── Load / verify experiment ─────────────────────────────────────

    #[pg_test]
    fn load_experiment_returns_project_data() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'loadexp_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('loadexp_proj', 'lr')").unwrap();

        let result: Result<_, pgrx::spi::SpiError> = Spi::connect(|client| {
            let mut rows = client.select(
                "SELECT experiment_id, task, target_column, model_id, algorithm
                 FROM pgaugur.load_experiment('loadexp_proj')",
                None,
                &[],
            )?;
            let row = rows.next().expect("should return a row");
            let task: String = row.get(2)?.unwrap_or_default();
            let target: String = row.get(3)?.unwrap_or_default();
            let model_id: Option<i64> = row.get(4)?;
            let algorithm: Option<String> = row.get(5)?;
            Ok(Some((task, target, model_id, algorithm)))
        });
        let (task, target, model_id, algorithm) = result.unwrap().unwrap();
        assert_eq!(task, "regression");
        assert_eq!(target, "y");
        assert!(model_id.is_some());
        assert_eq!(algorithm.unwrap(), "linear");
    }

    #[pg_test]
    fn verify_experiment_returns_jsonb() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'verify_proj')").unwrap();

        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgaugur.verify_experiment('verify_proj')");
        assert!(result.is_ok());
        let jsonb = result.unwrap().unwrap();
        assert!(jsonb.0.get("project").is_some());
    }

    // ─── Async training (in-tx limited) ───────────────────────────────

    #[pg_test]
    fn start_training_returns_job_id() {
        ensure();
        seed_iris();
        let job_id = Spi::get_one::<i64>(
            "SELECT pgaugur.start_training(
                project_name => 'async_proj',
                source_table => 'pgaugur_test_iris',
                target_column => 'species',
                algorithm => 'rf',
                task => 'classification'
            )",
        )
        .unwrap()
        .unwrap();
        assert!(job_id > 0);

        // Job should be in queued state within this transaction.
        let state = Spi::get_one::<String>(&format!(
            "SELECT state FROM pgaugur.training_status({})",
            job_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(state, "queued");
    }

    #[pg_test]
    fn cancel_training_queued_job() {
        ensure();
        seed_iris();
        let job_id = Spi::get_one::<i64>(
            "SELECT pgaugur.start_training(
                project_name => 'cancel_proj',
                source_table => 'pgaugur_test_iris',
                target_column => 'species',
                algorithm => 'rf',
                task => 'classification'
            )",
        )
        .unwrap()
        .unwrap();

        let cancelled =
            Spi::get_one::<bool>(&format!("SELECT pgaugur.cancel_training({})", job_id))
                .unwrap()
                .unwrap();
        assert!(cancelled);

        let state = Spi::get_one::<String>(&format!(
            "SELECT state FROM pgaugur.training_status({})",
            job_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(state, "cancelled");
    }

    #[pg_test]
    fn training_status_full_columns() {
        ensure();
        seed_iris();
        let job_id = Spi::get_one::<i64>(
            "SELECT pgaugur.start_training(
                project_name => 'status_proj',
                source_table => 'pgaugur_test_iris',
                target_column => 'species',
                algorithm => 'lr',
                task => 'classification'
            )",
        )
        .unwrap()
        .unwrap();

        // Verify all 15 columns come back without error.
        let col_count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM (
                SELECT job_id, project_name, state, mode, progress,
                       current_step, algorithms_tested, algorithms_total,
                       current_algorithm, best_so_far, model_id,
                       error_message, started_at, completed_at, elapsed_seconds
                FROM pgaugur.training_status({})
            ) sub",
            job_id
        ))
        .unwrap()
        .unwrap();
        assert_eq!(col_count, 1);
    }

    // ─── Extension docs ───────────────────────────────────────────────

    #[pg_test]
    fn extension_docs_returns_text() {
        let docs = Spi::get_one::<String>("SELECT pgaugur.extension_docs()")
            .unwrap()
            .unwrap();
        assert!(docs.contains("pg_augur"));
    }

    // ─── DSL experiment() ─────────────────────────────────────────────

    #[pg_test]
    fn experiment_dsl_classification() {
        ensure();
        seed_iris();
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgaugur.experiment($augur$
experiment iris_dsl {
    data: pgaugur_test_iris
    target: species
    task: classification
    pipeline {
        impute mean
        scale standard
    }
    compare
}
$augur$)",
        )
        .unwrap()
        .unwrap();
        // compare produces Setup + Compare results → at least 2 rows
        assert!(count >= 2, "expected at least 2 action rows, got {count}");

        // Verify project was created and has a deployed model
        let proj = crate::models::get_project("iris_dsl").unwrap();
        assert!(proj.is_some(), "project iris_dsl should exist");

        // Verify prediction works through the deployed model
        let pred =
            Spi::get_one::<String>("SELECT pgaugur.predict('iris_dsl', ARRAY[5.1,3.5,1.4,0.2])")
                .unwrap()
                .unwrap();
        assert!(pred.contains("prediction"), "prediction response: {pred}");
    }

    #[pg_test]
    fn experiment_dsl_regression_create() {
        ensure();
        seed_regression();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment reg_dsl {
    data: pgaugur_test_reg
    target: y
    task: regression
    pipeline {
        impute mean
        scale standard
    }
    create linear
}
$augur$)",
        )
        .unwrap();

        let pred = Spi::get_one::<String>("SELECT pgaugur.predict('reg_dsl', ARRAY[5.0, 10.0])")
            .unwrap()
            .unwrap();
        assert!(pred.contains("prediction"), "prediction response: {pred}");
    }

    #[pg_test]
    fn experiment_dsl_with_schema_table() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment schema_dsl {
    data: \"public.pgaugur_test_iris\"
    target: species
    task: classification
    pipeline {
        impute mean
    }
    create rf
}
$augur$)",
        )
        .unwrap();

        let proj = crate::models::get_project("schema_dsl").unwrap();
        assert!(proj.is_some(), "project schema_dsl should exist");
    }

    #[pg_test]
    fn show_experiment_returns_stored_data() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment show_dsl {
    data: pgaugur_test_iris
    target: species
    task: classification
    pipeline { impute mean }
    create rf
}
$augur$)",
        )
        .unwrap();

        let name = Spi::get_one::<String>("SELECT name FROM pgaugur.show_experiment('show_dsl')")
            .unwrap()
            .unwrap();
        assert_eq!(name, "show_dsl");
    }

    #[pg_test]
    fn show_pipeline_returns_fitted_dsl() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment pipe_dsl {
    data: pgaugur_test_iris
    target: species
    task: classification
    pipeline {
        impute mean
        scale standard
    }
    create rf
}
$augur$)",
        )
        .unwrap();

        let pipeline = Spi::get_one::<String>("SELECT pgaugur.show_pipeline('pipe_dsl')").unwrap();
        assert!(pipeline.is_some(), "should have a fitted pipeline");
        let p = pipeline.unwrap();
        assert!(
            p.contains("pipeline fitted"),
            "pipeline should contain 'pipeline fitted': {p}"
        );
    }

    #[pg_test]
    fn experiment_stores_metadata() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment meta_dsl {
    data: pgaugur_test_iris
    target: species
    task: classification
    pipeline { impute mean }
    create rf
}
$augur$)",
        )
        .unwrap();

        let exp = crate::models::get_experiment("meta_dsl").unwrap();
        assert!(exp.is_some(), "experiment should be stored");
        let exp = exp.unwrap();
        assert_eq!(exp.name, "meta_dsl");
        assert_eq!(exp.task.as_deref(), Some("classification"));
        assert_eq!(exp.target_column.as_deref(), Some("species"));
        assert!(exp.best_algorithm.is_some());
        assert!(exp.model_id.is_some());
        assert_eq!(exp.run_count, 1);
    }

    // ─── Inference: export, schema, predict_validated ────────────────

    #[pg_test]
    fn export_model_returns_json() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'export_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('export_proj', 'lr')").unwrap();

        let json = Spi::get_one::<String>("SELECT pgaugur.export_model('export_proj')")
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed.get("model_id").is_some(),
            "should contain model_id: {json}"
        );
        assert!(
            parsed.get("preprocessing").is_some(),
            "should contain preprocessing"
        );
        assert!(
            parsed.get("feature_names").is_some(),
            "should contain feature_names"
        );
        assert!(
            parsed.get("target_name").is_some(),
            "should contain target_name"
        );
    }

    #[pg_test]
    fn inference_schema_returns_jsonb() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'schema_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('schema_proj', 'lr')").unwrap();

        let schema = Spi::get_one::<pgrx::JsonB>("SELECT pgaugur.inference_schema('schema_proj')")
            .unwrap()
            .unwrap();
        assert!(
            schema.0.get("target").is_some(),
            "schema should have target field: {:?}",
            schema.0
        );
    }

    #[pg_test]
    fn predict_validated_returns_prediction() {
        ensure();
        seed_regression();
        Spi::run("SELECT * FROM pgaugur.setup('pgaugur_test_reg', 'y', 'pv_proj')").unwrap();
        Spi::run("SELECT * FROM pgaugur.create_model('pv_proj', 'lr')").unwrap();

        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgaugur.predict_validated('pv_proj', '{\"x1\": 5.0, \"x2\": 10.0}'::jsonb)",
        )
        .unwrap()
        .unwrap();
        assert!(
            result.0.get("prediction").is_some(),
            "should contain prediction: {:?}",
            result.0
        );
        assert!(
            result.0.get("warnings").is_some(),
            "should contain warnings array"
        );
    }

    #[pg_test]
    fn export_model_from_dsl_experiment() {
        ensure();
        seed_iris();
        Spi::run(
            "SELECT * FROM pgaugur.experiment($augur$
experiment export_dsl {
    data: pgaugur_test_iris
    target: species
    task: classification
    pipeline { impute mean }
    create rf
}
$augur$)",
        )
        .unwrap();

        // export_model should work on DSL-created projects
        let json = Spi::get_one::<String>("SELECT pgaugur.export_model('export_dsl')")
            .unwrap()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["model_id"].as_str().unwrap(), "rf");

        // predict_validated should also work
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgaugur.predict_validated('export_dsl', '{\"sl\": 5.1, \"sw\": 3.5, \"pl\": 1.4, \"pw\": 0.2}'::jsonb)",
        )
        .unwrap()
        .unwrap();
        assert!(result.0.get("prediction").is_some());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
