# pg_ml

Machine learning in PostgreSQL using PyCaret, with zero system dependencies.

## Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Rust toolchain
- cargo-pgrx 0.12+
- Python 3.10+ (auto-managed via uv)

## Installation

```bash
# Install pgrx if not already installed
cargo install cargo-pgrx
cargo pgrx init --pg16 $(which pg_config)

# Build and install
cd extensions/pg_ml
cargo pgrx install --release

# Add to shared_preload_libraries (postgresql.conf)
# shared_preload_libraries = 'pg_ml'
# Restart PostgreSQL
```

## Quickstart

```sql
-- Create extension
CREATE EXTENSION pg_ml;

-- Setup Python environment (one-time, ~2 min)
SELECT pgml.setup_venv();

-- Load sample dataset
SELECT * FROM pgml.load_dataset('iris');

-- Train a model
SELECT * FROM pgml.setup('iris', 'species', project_name => 'iris_classifier');
SELECT * FROM pgml.create_model('iris_classifier', 'rf');  -- random forest

-- Make predictions
SELECT pgml.predict('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
```

See [pg_ml Manual](../../docs/pg_ml.md) for training workflow details.

## Schema

`pgml`

## SQL API

### setup_venv

Create Python virtual environment with PyCaret (one-time setup).

```sql
SELECT pgml.setup_venv();
```

**Returns:** `TEXT` - Setup result message

### status

Get extension status including venv and Python state.

```sql
SELECT pgml.status();
```

**Returns:** `TEXT` - Status message

### python_info

Get Python environment information as JSON.

```sql
SELECT pgml.python_info();
```

**Returns:** `TEXT` - JSON with Python version, packages, etc.

### load_dataset

Load a sample dataset into a PostgreSQL table.

```sql
SELECT * FROM pgml.load_dataset('iris');
```

**Parameters:**
- `name TEXT` - Dataset name (iris, wine, breast_cancer, diabetes, etc.)

**Returns:** `TABLE (schema_name TEXT, table_name TEXT, row_count BIGINT, columns TEXT[])`

### list_datasets

List available PyCaret sample datasets.

```sql
SELECT * FROM pgml.list_datasets();
```

**Returns:** `TABLE (name TEXT, task TEXT, rows BIGINT, features BIGINT)`

### setup

Initialize a PyCaret experiment on a table. Must be called before training.

```sql
SELECT * FROM pgml.setup(
    'customers',
    'churned',
    project_name => 'churn_predictor',
    task => 'classification'
);
```

**Parameters:**
- `relation_name TEXT` - Table or view name
- `target TEXT` - Target column name
- `project_name TEXT DEFAULT NULL` - Project identifier (auto-generated if not provided)
- `task TEXT DEFAULT NULL` - 'classification' or 'regression' (auto-detected if not provided)
- `exclude_columns TEXT[] DEFAULT NULL` - Columns to exclude from features
- `options JSONB DEFAULT NULL` - Additional PyCaret options
- `id_column TEXT DEFAULT NULL` - ID column for reproducibility tracking

**Returns:** `TABLE (experiment_id, task, target_column, feature_columns, train_size, fold)`

### compare_models

AutoML: compare algorithms and deploy the best one(s).

```sql
SELECT * FROM pgml.compare_models(
    'churn_predictor',
    n_select => 3,
    budget_time => 1800
);
```

**Parameters:**
- `project_name TEXT` - Project identifier from setup()
- `n_select INTEGER DEFAULT 1` - Number of best models to return
- `sort TEXT DEFAULT NULL` - Metric to sort by (Accuracy, AUC, F1, etc.)
- `include TEXT[] DEFAULT NULL` - Algorithms to include
- `exclude TEXT[] DEFAULT NULL` - Algorithms to exclude
- `budget_time INTEGER DEFAULT 1800` - Time budget in seconds
- `deploy BOOLEAN DEFAULT true` - Auto-deploy best model

**Returns:** `TABLE (project_id, model_id, algorithm, task, metrics, deployed)`

### create_model

Train a specific algorithm (synchronous - blocks until complete).

```sql
SELECT * FROM pgml.create_model(
    'churn_predictor',
    'xgboost',
    deploy => true
);
```

**Parameters:**
- `project_name TEXT` - Project identifier from setup()
- `algorithm TEXT` - Algorithm name (lr, knn, nb, dt, rf, xgboost, lightgbm, etc.)
- `hyperparams JSONB DEFAULT NULL` - Custom hyperparameters
- `deploy BOOLEAN DEFAULT true` - Auto-deploy the model
- `conformal BOOLEAN DEFAULT false` - Enable conformal prediction
- `conformal_method TEXT DEFAULT 'plus'` - Conformal method
- `conformal_cv INTEGER DEFAULT 5` - Conformal CV folds

**Returns:** `TABLE (project_id, model_id, algorithm, task, metrics, deployed, conformal)`

## Async Training

For long-running training jobs, use async training to avoid blocking connections.

### start_training

Start an asynchronous training job. Returns immediately with a job ID while training runs in a background worker.

```sql
-- Single algorithm
SELECT pgml.start_training(
    'churn_model',
    'analytics.customers',
    'churned',
    algorithm => 'xgboost'
);

-- AutoML with time budget
SELECT pgml.start_training(
    'churn_model',
    'analytics.customers',
    'churned',
    automl => true,
    budget_time => 30,
    conformal => true
);

-- With setup options
SELECT pgml.start_training(
    'price_predictor',
    'sales.products',
    'price',
    task => 'regression',
    automl => true,
    exclude_columns => ARRAY['id', 'created_at'],
    metric => 'rmse',
    setup_options => '{
        "normalize": true,
        "transformation": true,
        "fold": 5,
        "n_jobs": -1
    }'::jsonb
);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `source_table TEXT` - Table or view name (can be schema-qualified)
- `target_column TEXT` - Target column name
- `algorithm TEXT DEFAULT NULL` - Algorithm for single-model training
- `automl BOOLEAN DEFAULT false` - Enable AutoML to compare all algorithms
- `task TEXT DEFAULT NULL` - 'classification' or 'regression' (auto-detected)
- `exclude_columns TEXT[] DEFAULT NULL` - Columns to exclude from features
- `train_size FLOAT DEFAULT 0.8` - Train/test split ratio
- `budget_time INTEGER DEFAULT NULL` - Max training time in minutes (AutoML)
- `conformal BOOLEAN DEFAULT false` - Enable conformal prediction
- `conformal_method TEXT DEFAULT 'plus'` - Conformal method
- `metric TEXT DEFAULT NULL` - Optimization metric (accuracy, f1, rmse, etc.)
- `hyperparams JSONB DEFAULT NULL` - Custom hyperparameters (single algorithm)
- `setup_options JSONB DEFAULT NULL` - PyCaret setup options (see below)

**setup_options JSONB:**
- `train_size` (float) - Train/test split ratio (default 0.8)
- `fold` (int) - Number of CV folds (default 10)
- `normalize` (bool) - Normalize numeric features
- `transformation` (bool) - Apply power transformation
- `n_jobs` (int) - Parallel jobs (-1 for all cores)
- `use_gpu` (bool) - Use GPU acceleration
- `session_id` (int) - Random seed for reproducibility

**Returns:** `BIGINT` - Job ID for status tracking

### training_status

Check the status of an async training job.

```sql
SELECT * FROM pgml.training_status(42);
```

**Parameters:**
- `job_id BIGINT` - Job ID from start_training()

**Returns:** `TABLE` with columns:
| Column | Type | Description |
|--------|------|-------------|
| job_id | BIGINT | Job identifier |
| project_name | TEXT | Project name |
| state | TEXT | 'queued', 'setup', 'training', 'completed', 'failed', 'cancelled' |
| mode | TEXT | 'single' or 'automl' |
| progress | FLOAT | 0.0 to 1.0 |
| current_step | TEXT | Human-readable current step |
| algorithms_tested | INTEGER | (AutoML) Number tested so far |
| algorithms_total | INTEGER | (AutoML) Total to test |
| current_algorithm | TEXT | Algorithm currently training |
| best_so_far | JSONB | Best model found so far |
| model_id | BIGINT | Final model ID (when completed) |
| error_message | TEXT | Error details (when failed) |
| started_at | TIMESTAMPTZ | When training started |
| completed_at | TIMESTAMPTZ | When training finished |
| elapsed_seconds | FLOAT | Training duration |

### cancel_training

Cancel a running or queued training job.

```sql
SELECT pgml.cancel_training(42);
```

**Parameters:**
- `job_id BIGINT` - Job ID to cancel

**Returns:** `BOOLEAN` - True if cancelled, false if already completed/failed

### Notifications

Listen for training completion events:

```sql
LISTEN pgml_training;

-- Notification payload (JSON):
-- {"job_id": 42, "state": "completed", "model_id": 17, "project": "churn_model"}
-- {"job_id": 43, "state": "failed", "error": "Out of memory", "project": "big_model"}
```

### Async Training Configuration

```sql
-- Enable/disable background worker (requires restart)
SET pg_ml.training_worker_enabled = true;  -- default: true

-- Job polling interval in milliseconds
SET pg_ml.training_poll_interval = 1000;   -- default: 1000

-- Database for worker to connect to
SET pg_ml.training_database = 'mydb';      -- default: 'postgres'
```

### predict

Make a single prediction with feature array.

```sql
SELECT pgml.predict('churn_predictor', ARRAY[5.1, 3.5, 1.4, 0.2]);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `features REAL[]` - Feature values in order

**Returns:** `TEXT` - Prediction (class label or numeric value)

### predict_proba

Get class probabilities for classification.

```sql
SELECT pgml.predict_proba('churn_predictor', ARRAY[5.1, 3.5, 1.4, 0.2]);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `features REAL[]` - Feature values

**Returns:** `REAL[]` - Probability for each class

### predict_interval

Predict with confidence interval using conformal prediction.

```sql
SELECT pgml.predict_interval('churn_predictor', ARRAY[5.1, 3.5], 0.1);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `features REAL[]` - Feature values
- `alpha REAL DEFAULT 0.1` - Significance level (1-alpha = confidence)

**Returns:** `JSONB` - Prediction with lower/upper bounds

### predict_dist

Predict and return as pg_prob distribution. Requires pg_prob extension.

```sql
SELECT pgml.predict_dist('sales_predictor', ARRAY[1.0, 2.0]);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `features REAL[]` - Feature values
- `alpha REAL DEFAULT 0.1` - Significance level
- `dist_type TEXT DEFAULT 'triangular'` - Distribution type

**Returns:** `TEXT` - Distribution as pg_prob literal

### predict_batch

Batch prediction on a table.

```sql
SELECT * FROM pgml.predict_batch(
    'churn_predictor',
    'new_customers',
    'customer_id'
);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `relation_name TEXT` - Table to predict on
- `id_column TEXT` - ID column for output
- `feature_columns TEXT[] DEFAULT NULL` - Feature columns (uses project config if NULL)

**Returns:** `TABLE (id TEXT, prediction TEXT)`

### deploy

Deploy a specific model by ID.

```sql
SELECT pgml.deploy('churn_predictor', 5);
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `model_id BIGINT` - Model ID to deploy

**Returns:** `BOOLEAN` - True if deployed

### drop_project

Delete a project and all its models.

```sql
SELECT pgml.drop_project('churn_predictor');
```

**Parameters:**
- `project_name TEXT` - Project identifier

**Returns:** `BOOLEAN` - True if deleted

### verify_experiment

Check if source data has changed since training.

```sql
SELECT pgml.verify_experiment('churn_predictor');
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `model_id BIGINT DEFAULT NULL` - Specific model to verify

**Returns:** `JSONB` - Verification result with data hash comparison

### load_experiment

Reload a saved experiment configuration.

```sql
SELECT * FROM pgml.load_experiment('churn_predictor');
```

**Parameters:**
- `project_name TEXT` - Project identifier

**Returns:** `TABLE (experiment_id, task, target_column, feature_columns, train_size, fold)`

### current_experiment

Get information about the active experiment in this session.

```sql
SELECT * FROM pgml.current_experiment();
```

**Returns:** `TABLE (project_name, task, target_column, feature_columns, data_rows, data_cols)`

### init

Initialize the Jupyter kernel for interactive use.

```sql
SELECT pgml.init();
```

**Returns:** `JSONB` - Kernel connection info (ZMQ ports)

### shutdown

Shutdown the Jupyter kernel.

```sql
SELECT pgml.shutdown();
```

**Returns:** `BOOLEAN` - True if shutdown

### kernel_status

Check if the Jupyter kernel is running.

```sql
SELECT pgml.kernel_status();
```

**Returns:** `BOOLEAN` - True if running

## Supported Algorithms

**Classification:** lr, knn, nb, dt, rf, et, gbc, xgboost, lightgbm, catboost, ada, svm, ridge, lda, qda

**Regression:** lr, lasso, ridge, en, lar, llar, omp, br, ard, par, ransac, tr, huber, kr, svm, knn, dt, rf, et, ada, gbr, xgboost, lightgbm, catboost

## MLflow Integration

pg_ml includes an embedded MLflow tracking server for experiment visualization.

### mlflow_status

Get MLflow server configuration and status.

```sql
SELECT pgml.mlflow_status();
```

**Returns:** `JSONB` with fields:
- `enabled` - Whether MLflow server is running
- `host` - Server bind address
- `port` - Server port
- `url` - Full server URL
- `tracking` - Whether tracking is enabled for this session
- `tracking_uri` - URI for experiment storage
- `mlruns_exists` - Whether experiment data exists

### mlflow_url

Get the MLflow UI URL (NULL if disabled).

```sql
SELECT pgml.mlflow_url();
```

**Returns:** `TEXT` - URL like `http://127.0.0.1:5000` or NULL

### MLflow Configuration

```sql
-- Enable MLflow UI server (requires restart)
SET pg_ml.mlflow_enabled = true;     -- default: false

-- Server host/port (require restart)
SET pg_ml.mlflow_host = '0.0.0.0';   -- default: 127.0.0.1
SET pg_ml.mlflow_port = 5000;        -- default: 5000

-- Enable tracking for this session (no restart)
SET pg_ml.mlflow_tracking = true;    -- default: false
```

When `mlflow_tracking` is enabled, training operations log experiments to MLflow automatically.

## Configuration

```sql
SET pg_ml.venv_path = '/var/lib/postgresql/pg_ml';
SET pg_ml.auto_setup = true;
```
