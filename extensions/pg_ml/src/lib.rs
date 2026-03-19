//! pg_ml: Machine learning in PostgreSQL using PyCaret
//!
//! This extension provides AutoML capabilities directly in SQL,
//! with zero system dependencies through bundled uv for Python management.

use pgrx::prelude::*;

// Module declarations
mod arrow_convert;
mod async_training;
mod async_training_sql;
mod config;
pub mod datasets;
pub mod embeddings;
pub mod error;
mod mlflow;
pub mod models;
pub mod pycaret;
mod sql_functions;
mod venv;

// Re-export error type
pub use error::PgMlError;

// Re-export commonly needed functions
pub use config::{get_python_path, get_venv_path};
pub use venv::{ensure_python, ensure_venv, find_uv};

// Re-export SQL functions so they are visible to pgrx
pub use async_training_sql::*;
pub use sql_functions::*;

// Re-export background worker entry points so they appear as dynamic symbols
pub use async_training::pg_ml_training_worker_main;
pub use mlflow::pg_ml_mlflow_worker_main;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation (README.md) as a string.
/// This enables runtime discovery of extension capabilities via pg_mcp.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    config::register_gucs();

    // Register embedding GUC settings
    embeddings::config::register_gucs();

    // Register MLflow GUC settings
    mlflow::register_gucs();

    // Register MLflow background worker if enabled
    // Note: GUC value is read at postmaster startup, so mlflow_enabled
    // must be set in postgresql.conf before PostgreSQL starts
    if mlflow::is_enabled() {
        mlflow::register_background_worker();
    }

    // Register async training GUC settings
    async_training::register_gucs();

    // Always register the training background worker.
    // The enabled flag is checked inside worker_main, matching pg_mqtt's pattern.
    // This avoids issues with shared memory cleanup when workers are conditionally registered.
    async_training::register_background_worker();
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PgMlError::UvNotFound;
        assert!(err.to_string().contains("uv not found"));

        let err = PgMlError::VenvCreationFailed("test error".into());
        assert!(err.to_string().contains("test error"));
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_status_returns_info() {
        let result = crate::status();
        assert!(result.contains("pg_ml status"));
        assert!(result.contains("venv_path"));
    }

    #[pg_test]
    fn test_find_uv_function() {
        // Test that find_uv doesn't panic in PostgreSQL context
        let result = crate::find_uv();
        // Either finds uv or returns appropriate error
        match result {
            Ok(path) => assert!(path.exists()),
            Err(e) => assert!(e.to_string().contains("uv not found")),
        }
    }

    // -------------------------------------------------------------------------
    // Dataset Loading Tests
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_load_dataset_creates_schema() {
        // Clean up first
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();

        // Load a small dataset (iris has 150 rows)
        let result = Spi::get_one::<i64>("SELECT row_count FROM pgml.load_dataset('iris')");

        // Should have loaded 150 rows
        assert!(result.is_ok());
        let row_count = result.unwrap().unwrap_or(0);
        assert_eq!(row_count, 150, "Iris dataset should have 150 rows");

        // Verify schema was created
        let schema_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgml_samples')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(schema_exists, "pgml_samples schema should exist");

        // Verify table was created
        let table_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgml_samples' AND table_name = 'iris')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(table_exists, "iris table should exist");
    }

    #[pg_test]
    fn test_load_dataset_if_exists_skip() {
        // Clean up and load iris
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Add a marker column to verify table wasn't replaced
        Spi::run("ALTER TABLE pgml_samples.iris ADD COLUMN test_marker BOOLEAN DEFAULT TRUE")
            .unwrap();

        // Load again with 'skip' - should not replace
        Spi::run("SELECT * FROM pgml.load_dataset('iris', if_exists => 'skip')").unwrap();

        // Verify marker column still exists
        let has_marker = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'pgml_samples'
             AND table_name = 'iris'
             AND column_name = 'test_marker')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(has_marker, "Marker column should still exist after skip");
    }

    #[pg_test]
    fn test_load_dataset_if_exists_replace() {
        // Clean up and load iris
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Add a marker column
        Spi::run("ALTER TABLE pgml_samples.iris ADD COLUMN test_marker BOOLEAN DEFAULT TRUE")
            .unwrap();

        // Load again with 'replace' - should recreate table
        Spi::run("SELECT * FROM pgml.load_dataset('iris', if_exists => 'replace')").unwrap();

        // Verify marker column no longer exists
        let has_marker = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'pgml_samples'
             AND table_name = 'iris'
             AND column_name = 'test_marker')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(!has_marker, "Marker column should NOT exist after replace");
    }

    #[pg_test]
    fn test_load_dataset_custom_table_name() {
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();

        // Load with custom table name
        Spi::run("SELECT * FROM pgml.load_dataset('iris', table_name => 'my_custom_iris')")
            .unwrap();

        // Verify custom table name was used
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgml_samples' AND table_name = 'my_custom_iris')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "Custom table name should be used");
    }

    #[pg_test]
    fn test_load_dataset_custom_schema() {
        Spi::run("DROP SCHEMA IF EXISTS custom_ml_data CASCADE").ok();

        // Load with custom schema
        Spi::run("SELECT * FROM pgml.load_dataset('iris', schema_name => 'custom_ml_data')")
            .unwrap();

        // Verify custom schema was created and used
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'custom_ml_data' AND table_name = 'iris')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(exists, "Custom schema should be used");

        // Cleanup
        Spi::run("DROP SCHEMA custom_ml_data CASCADE").ok();
    }

    #[pg_test]
    fn test_list_datasets_returns_data() {
        // List datasets should return entries
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgml.list_datasets()")
            .unwrap()
            .unwrap_or(0);

        // PyCaret has many datasets, should be at least 10
        assert!(
            count >= 10,
            "Should list at least 10 datasets, got {}",
            count
        );
    }

    #[pg_test]
    fn test_load_dataset_columns_returned() {
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();

        // Load and get column list
        let columns = Spi::get_one::<Vec<String>>("SELECT columns FROM pgml.load_dataset('iris')")
            .unwrap()
            .unwrap_or_default();

        // Iris should have species column
        assert!(
            columns.iter().any(
                |c| c.to_lowercase().contains("species") || c.to_lowercase().contains("target")
            ),
            "Iris should have species/target column, got: {:?}",
            columns
        );
    }

    #[pg_test]
    fn test_load_dataset_data_queryable() {
        // Clean up and load iris
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Verify we can query the actual data
        let actual_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgml_samples.iris")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(actual_count, 150, "Should have 150 rows in the table");

        // Verify we can select specific columns and they have expected values
        // Iris sepal_length values should be between 4.0 and 8.0
        let min_sepal = Spi::get_one::<f64>("SELECT MIN(sepal_length) FROM pgml_samples.iris")
            .unwrap()
            .unwrap_or(0.0);

        let max_sepal = Spi::get_one::<f64>("SELECT MAX(sepal_length) FROM pgml_samples.iris")
            .unwrap()
            .unwrap_or(0.0);

        assert!(
            min_sepal >= 4.0 && min_sepal <= 5.0,
            "Min sepal_length should be around 4.3, got {}",
            min_sepal
        );
        assert!(
            max_sepal >= 7.0 && max_sepal <= 8.0,
            "Max sepal_length should be around 7.9, got {}",
            max_sepal
        );

        // Verify species column has the expected distinct values (3 species)
        let species_count =
            Spi::get_one::<i64>("SELECT COUNT(DISTINCT species) FROM pgml_samples.iris")
                .unwrap()
                .unwrap_or(0);
        assert_eq!(species_count, 3, "Iris should have 3 distinct species");
    }

    #[pg_test]
    fn test_table_to_dataframe_roundtrip() {
        use pyo3::prelude::*;

        // Clean up and load iris
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Ensure Python is initialized with our venv
        crate::ensure_python().expect("Failed to initialize Python");

        // Convert the table back to a DataFrame using JSON path
        Python::with_gil(|py| {
            let df = crate::datasets::table_to_dataframe_impl(
                py,
                "pgml_samples",
                "iris",
                None, // all columns
                None, // no limit
            )
            .expect("Failed to convert table to DataFrame");

            // Verify DataFrame shape: should have 150 rows and 6 columns (5 iris + id)
            let shape = df.getattr("shape").unwrap();
            let shape_tuple: (i64, i64) = shape.extract().unwrap();
            assert_eq!(shape_tuple.0, 150, "DataFrame should have 150 rows");
            assert_eq!(
                shape_tuple.1, 6,
                "DataFrame should have 6 columns (5 iris + id)"
            );

            // Verify column names
            let columns = df.getattr("columns").unwrap();
            let col_list: Vec<String> = columns
                .getattr("tolist")
                .unwrap()
                .call0()
                .unwrap()
                .extract()
                .unwrap();
            assert!(
                col_list.iter().any(|c| c == "sepal_length"),
                "DataFrame should have sepal_length column, got: {:?}",
                col_list
            );
            assert!(
                col_list.iter().any(|c| c == "species"),
                "DataFrame should have species column, got: {:?}",
                col_list
            );

            // Verify we can compute statistics on the DataFrame
            let sepal_col = df.get_item("sepal_length").unwrap();
            let mean_val: f64 = sepal_col.call_method0("mean").unwrap().extract().unwrap();
            // Iris sepal_length mean is approximately 5.84
            assert!(
                mean_val >= 5.5 && mean_val <= 6.2,
                "Mean sepal_length should be around 5.84, got {}",
                mean_val
            );

            // Verify the DataFrame can be used with PyCaret-style operations
            // (e.g., value_counts on species)
            let species_col = df.get_item("species").unwrap();
            let value_counts = species_col.call_method0("value_counts").unwrap();
            let count_len: i64 = value_counts.len().unwrap() as i64;
            assert_eq!(count_len, 3, "Should have 3 unique species");
        });
    }

    #[pg_test]
    fn test_table_to_dataframe_arrow() {
        use pyo3::prelude::*;

        // Clean up and load iris
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Ensure Python is initialized with our venv
        crate::ensure_python().expect("Failed to initialize Python");

        // Convert the table back to a DataFrame using Arrow zero-copy path
        Python::with_gil(|py| {
            let df = crate::arrow_convert::table_to_dataframe_arrow(
                py,
                "pgml_samples",
                "iris",
                None, // all columns
                None, // no limit
            )
            .expect("Failed to convert table to DataFrame via Arrow");

            // Verify DataFrame shape: should have 150 rows and 6 columns (5 iris + id)
            let shape = df.getattr("shape").unwrap();
            let shape_tuple: (i64, i64) = shape.extract().unwrap();
            assert_eq!(shape_tuple.0, 150, "DataFrame should have 150 rows");
            assert_eq!(
                shape_tuple.1, 6,
                "DataFrame should have 6 columns (5 iris + id)"
            );

            // Verify column names
            let columns = df.getattr("columns").unwrap();
            let col_list: Vec<String> = columns
                .getattr("tolist")
                .unwrap()
                .call0()
                .unwrap()
                .extract()
                .unwrap();
            assert!(
                col_list.iter().any(|c| c == "sepal_length"),
                "DataFrame should have sepal_length column, got: {:?}",
                col_list
            );
            assert!(
                col_list.iter().any(|c| c == "species"),
                "DataFrame should have species column, got: {:?}",
                col_list
            );

            // Verify we can compute statistics on the DataFrame
            let sepal_col = df.get_item("sepal_length").unwrap();
            let mean_val: f64 = sepal_col.call_method0("mean").unwrap().extract().unwrap();
            // Iris sepal_length mean is approximately 5.84
            assert!(
                mean_val >= 5.5 && mean_val <= 6.2,
                "Mean sepal_length should be around 5.84, got {}",
                mean_val
            );

            // Verify the DataFrame uses Arrow-backed dtypes
            let dtypes = df.getattr("dtypes").unwrap();
            let dtype_str = dtypes.str().unwrap().to_string();
            assert!(
                dtype_str.contains("pyarrow") || dtype_str.contains("ArrowDtype"),
                "DataFrame should use Arrow-backed dtypes, got: {}",
                dtype_str
            );

            // Verify the DataFrame can be used with PyCaret-style operations
            let species_col = df.get_item("species").unwrap();
            let value_counts = species_col.call_method0("value_counts").unwrap();
            let count_len: i64 = value_counts.len().unwrap() as i64;
            assert_eq!(count_len, 3, "Should have 3 unique species");
        });
    }

    #[pg_test]
    fn test_table_to_arrow_with_numeric_type() {
        use arrow::array::{Array, Float64Array};

        // Create a test table with NUMERIC columns (which require casting to DOUBLE PRECISION)
        Spi::run("DROP TABLE IF EXISTS test_numeric_table CASCADE").ok();
        Spi::run(
            "CREATE TABLE test_numeric_table (
                id SERIAL PRIMARY KEY,
                amount NUMERIC(10,2),
                price DECIMAL(12,4),
                quantity INTEGER,
                name TEXT
            )",
        )
        .unwrap();

        // Insert test data
        Spi::run(
            "INSERT INTO test_numeric_table (amount, price, quantity, name) VALUES
                (123.45, 999.9999, 10, 'item1'),
                (678.90, 0.0001, 20, 'item2'),
                (NULL, 555.5555, NULL, 'item3')",
        )
        .unwrap();

        // Convert the table to Arrow RecordBatch (no Python needed)
        // This tests that NUMERIC/DECIMAL columns are properly cast to DOUBLE PRECISION
        let batch =
            crate::arrow_convert::table_to_arrow("public", "test_numeric_table", None, None)
                .expect("Failed to convert table with NUMERIC columns to Arrow");

        // Verify RecordBatch shape: should have 3 rows and 5 columns
        assert_eq!(batch.num_rows(), 3, "RecordBatch should have 3 rows");
        assert_eq!(batch.num_columns(), 5, "RecordBatch should have 5 columns");

        // Get the schema and verify column types
        let schema = batch.schema();

        // 'amount' column (NUMERIC) should be mapped to Float64
        let amount_field = schema.field_with_name("amount").unwrap();
        assert_eq!(
            amount_field.data_type(),
            &arrow::datatypes::DataType::Float64,
            "NUMERIC column should be mapped to Float64"
        );

        // 'price' column (DECIMAL) should be mapped to Float64
        let price_field = schema.field_with_name("price").unwrap();
        assert_eq!(
            price_field.data_type(),
            &arrow::datatypes::DataType::Float64,
            "DECIMAL column should be mapped to Float64"
        );

        // Verify the actual data values in the 'amount' column
        let amount_col = batch
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("amount column should be Float64Array");

        // Check values: 123.45, 678.90, NULL
        assert!(
            (amount_col.value(0) - 123.45).abs() < 0.001,
            "First amount should be 123.45, got {}",
            amount_col.value(0)
        );
        assert!(
            (amount_col.value(1) - 678.90).abs() < 0.001,
            "Second amount should be 678.90, got {}",
            amount_col.value(1)
        );
        assert!(amount_col.is_null(2), "Third amount should be NULL");

        // Verify the 'price' column data
        let price_col = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("price column should be Float64Array");

        // Check values: 999.9999, 0.0001, 555.5555
        assert!(
            (price_col.value(0) - 999.9999).abs() < 0.00001,
            "First price should be 999.9999, got {}",
            price_col.value(0)
        );
        assert!(
            (price_col.value(1) - 0.0001).abs() < 0.00001,
            "Second price should be 0.0001, got {}",
            price_col.value(1)
        );
        assert!(
            (price_col.value(2) - 555.5555).abs() < 0.00001,
            "Third price should be 555.5555, got {}",
            price_col.value(2)
        );

        // Clean up
        Spi::run("DROP TABLE IF EXISTS test_numeric_table CASCADE").ok();
    }

    // -------------------------------------------------------------------------
    // PyCaret ML Tests
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_setup_returns_experiment_info() {
        // Clean up data (but not pgml schema which contains extension functions)
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup on the iris dataset
        let result =
            Spi::get_one::<String>("SELECT task FROM pgml.setup('pgml_samples.iris', 'species')");

        // Should detect classification task for iris (species column is categorical)
        assert!(result.is_ok());
        let task = result.unwrap().unwrap_or_default();
        assert_eq!(
            task, "classification",
            "Iris should be a classification task"
        );
    }

    #[pg_test]
    fn test_setup_with_explicit_task() {
        // Clean up data (but not pgml schema which contains extension functions)
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup with explicit task
        let result = Spi::get_one::<String>(
            "SELECT task FROM pgml.setup('pgml_samples.iris', 'species', task => 'classification')",
        );

        assert!(result.is_ok());
        let task = result.unwrap().unwrap_or_default();
        assert_eq!(task, "classification");
    }

    #[pg_test]
    fn test_setup_returns_feature_columns() {
        // Clean up data (but not pgml schema which contains extension functions)
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup
        let feature_columns = Spi::get_one::<Vec<String>>(
            "SELECT feature_columns FROM pgml.setup('pgml_samples.iris', 'species')",
        );

        assert!(feature_columns.is_ok());
        let cols = feature_columns.unwrap().unwrap_or_default();

        // Should have 4 feature columns (all except species)
        assert_eq!(cols.len(), 4, "Should have 4 feature columns");
        assert!(
            !cols.contains(&"species".to_string()),
            "species should not be a feature"
        );
        assert!(
            cols.contains(&"sepal_length".to_string()),
            "sepal_length should be a feature"
        );
    }

    #[pg_test]
    fn test_setup_with_exclude_columns() {
        // Clean up data (but not pgml schema which contains extension functions)
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup excluding some columns
        let feature_columns = Spi::get_one::<Vec<String>>(
            "SELECT feature_columns FROM pgml.setup('pgml_samples.iris', 'species',
             exclude_columns => ARRAY['sepal_length', 'sepal_width'])",
        );

        assert!(feature_columns.is_ok());
        let cols = feature_columns.unwrap().unwrap_or_default();

        // Should have 2 feature columns (petal_length, petal_width)
        assert_eq!(
            cols.len(),
            2,
            "Should have 2 feature columns after exclusion"
        );
        assert!(
            !cols.contains(&"sepal_length".to_string()),
            "sepal_length should be excluded"
        );
        assert!(
            !cols.contains(&"sepal_width".to_string()),
            "sepal_width should be excluded"
        );
    }

    #[pg_test]
    fn test_setup_with_options() {
        // Clean up data (but not pgml schema which contains extension functions)
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup with custom options
        let result = Spi::get_one::<f64>(
            "SELECT train_size FROM pgml.setup('pgml_samples.iris', 'species',
             options => '{\"train_size\": 0.9}'::jsonb)",
        );

        assert!(result.is_ok());
        let train_size = result.unwrap().unwrap_or(0.0);
        assert!(
            (train_size - 0.9).abs() < 0.01,
            "Train size should be 0.9, got {}",
            train_size
        );
    }

    #[pg_test]
    fn test_setup_with_project_name() {
        // Clean up
        Spi::run("DROP SCHEMA IF EXISTS pgml_samples CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        Spi::run("SELECT * FROM pgml.load_dataset('iris')").unwrap();

        // Run setup with project_name to save config
        Spi::run(
            "SELECT * FROM pgml.setup('pgml_samples.iris', 'species', project_name => 'iris_test')",
        )
        .unwrap();

        // Verify config was stored in pgml.projects table
        let project_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgml.projects WHERE name = 'iris_test')",
        )
        .unwrap()
        .unwrap_or(false);

        assert!(project_exists, "Project config should be stored");
    }

    // -------------------------------------------------------------------------
    // Time Series Tests
    // -------------------------------------------------------------------------

    /// Helper to create test time series data
    fn create_test_timeseries_table() {
        Spi::run("DROP TABLE IF EXISTS test_ts_data CASCADE").ok();
        Spi::run(
            "CREATE TABLE test_ts_data (
                ds TIMESTAMP NOT NULL,
                y DOUBLE PRECISION NOT NULL
            )",
        )
        .unwrap();

        // Generate 48 monthly data points with a seasonal pattern
        Spi::run(
            "INSERT INTO test_ts_data (ds, y)
             SELECT '2020-01-01'::timestamp + (n * interval '1 month'),
                    100 + 10 * sin(n * 3.14159 / 6) + n * 0.5
             FROM generate_series(0, 47) AS n",
        )
        .unwrap();
    }

    #[pg_test]
    fn test_setup_timeseries_basic() {
        // Clean up
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        create_test_timeseries_table();

        // Run time series setup
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgml.setup_timeseries(
                'public.test_ts_data', 'y', 'ds',
                project_name => 'ts_test',
                fh => 6
            )",
        );

        assert!(result.is_ok(), "setup_timeseries should succeed");
        let jsonb = result.unwrap().unwrap();
        let val = jsonb.0;

        // Verify returned metadata
        assert_eq!(
            val.get("target_column").and_then(|v| v.as_str()),
            Some("y"),
            "Should return target_column"
        );
        assert_eq!(
            val.get("index_column").and_then(|v| v.as_str()),
            Some("ds"),
            "Should return index_column"
        );
        assert_eq!(
            val.get("fh").and_then(|v| v.as_i64()),
            Some(6),
            "Should return forecast horizon"
        );

        // Verify project was stored
        let project_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgml.projects WHERE name = 'ts_test')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(project_exists, "Project should be stored");

        // Clean up
        Spi::run("DROP TABLE IF EXISTS test_ts_data CASCADE").ok();
    }

    #[pg_test]
    fn test_create_ts_model_naive() {
        // Clean up
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        create_test_timeseries_table();

        // Setup TS experiment
        Spi::run(
            "SELECT pgml.setup_timeseries(
                'public.test_ts_data', 'y', 'ds',
                project_name => 'ts_naive_test',
                fh => 6
            )",
        )
        .unwrap();

        // Train a naive model (fastest TS algorithm)
        let result = Spi::get_one::<String>(
            "SELECT algorithm FROM pgml.create_ts_model('ts_naive_test', 'naive')",
        );

        assert!(result.is_ok(), "create_ts_model should succeed");
        let algorithm = result.unwrap().unwrap_or_default();
        assert_eq!(algorithm, "naive", "Algorithm should be 'naive'");

        // Verify model was stored
        let model_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgml.models m
             JOIN pgml.projects p ON m.project_id = p.id
             WHERE p.name = 'ts_naive_test')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(model_exists, "Model should be stored in pgml.models");

        // Clean up
        Spi::run("DROP TABLE IF EXISTS test_ts_data CASCADE").ok();
    }

    #[pg_test]
    fn test_forecast_basic() {
        // Clean up
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        create_test_timeseries_table();

        // Setup and train
        Spi::run(
            "SELECT pgml.setup_timeseries(
                'public.test_ts_data', 'y', 'ds',
                project_name => 'ts_forecast_test',
                fh => 6
            )",
        )
        .unwrap();
        Spi::run("SELECT * FROM pgml.create_ts_model('ts_forecast_test', 'naive')").unwrap();

        // Generate forecast
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgml.forecast('ts_forecast_test')")
            .unwrap()
            .unwrap_or(0);

        // Should return fh=6 forecast rows
        assert_eq!(count, 6, "Should return 6 forecast rows (fh=6)");

        // Verify step numbers are sequential
        let first_step = Spi::get_one::<i32>(
            "SELECT step FROM pgml.forecast('ts_forecast_test') ORDER BY step LIMIT 1",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(first_step, 1, "First step should be 1");

        // Clean up
        Spi::run("DROP TABLE IF EXISTS test_ts_data CASCADE").ok();
    }

    #[pg_test]
    fn test_forecast_with_prediction_intervals() {
        // Clean up
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();
        create_test_timeseries_table();

        // Setup and train
        Spi::run(
            "SELECT pgml.setup_timeseries(
                'public.test_ts_data', 'y', 'ds',
                project_name => 'ts_interval_test',
                fh => 3
            )",
        )
        .unwrap();
        Spi::run("SELECT * FROM pgml.create_ts_model('ts_interval_test', 'naive')").unwrap();

        // Forecast with prediction intervals
        let has_bounds = Spi::get_one::<bool>(
            "SELECT lower IS NOT NULL AND upper IS NOT NULL
             FROM pgml.forecast('ts_interval_test', return_pred_int => true)
             LIMIT 1",
        )
        .unwrap()
        .unwrap_or(false);

        assert!(
            has_bounds,
            "Forecast with return_pred_int should have lower/upper bounds"
        );

        // Clean up
        Spi::run("DROP TABLE IF EXISTS test_ts_data CASCADE").ok();
    }

    #[pg_test]
    fn test_ts_schema_migration() {
        // Verify the schema supports time_series task
        Spi::run("DROP TABLE IF EXISTS pgml.models CASCADE").ok();
        Spi::run("DROP TABLE IF EXISTS pgml.projects CASCADE").ok();

        // ensure_schema should create tables with updated CHECK constraint
        crate::models::ensure_schema().expect("Schema setup should succeed");

        // Should be able to insert a time_series project
        let result = Spi::run(
            "INSERT INTO pgml.projects (name, task, target_column, feature_columns)
             VALUES ('ts_schema_test', 'time_series', 'y', ARRAY['ds'])",
        );
        assert!(
            result.is_ok(),
            "Should be able to insert time_series task: {:?}",
            result.err()
        );

        // Verify index_column and forecast_horizon columns exist
        let has_ts_cols = Spi::get_one::<bool>(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'pgml' AND table_name = 'projects'
                AND column_name = 'index_column'
            )",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(has_ts_cols, "projects table should have index_column");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Setup code for tests
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // Use local .venv folder in pg_ml extension directory
        // This venv should be created during development setup with:
        //   uv venv --python 3.11 .venv && uv pip install --python .venv/bin/python .
        vec![
            "pg_ml.venv_path = '/home/ubuntu/dev/pg_extensions/extensions/pg_ml/.venv'",
            "shared_preload_libraries = 'pg_ml'",
        ]
    }
}
