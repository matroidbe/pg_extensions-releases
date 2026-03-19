# pg_ml Technical Documentation

**AutoML in PostgreSQL using PyCaret**

pg_ml brings machine learning capabilities directly into PostgreSQL with zero system dependencies. Train models, make predictions, and manage ML workflows entirely from SQL.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation & Setup](#2-installation--setup)
3. [Core Concepts](#3-core-concepts)
4. [Quick Start Tutorial](#4-quick-start-tutorial)
5. [SQL API Reference](#5-sql-api-reference)
6. [Supported Algorithms](#6-supported-algorithms)
7. [Configuration Reference](#7-configuration-reference)
8. [Integration Patterns](#8-integration-patterns)
9. [Metrics Reference](#9-metrics-reference)
10. [Reproducibility & Experiment Tracking](#10-reproducibility--experiment-tracking)
11. [Troubleshooting](#11-troubleshooting)
12. [Best Practices](#12-best-practices)
13. [Database Schema Reference](#13-database-schema-reference)

---

## 1. Introduction

### What is pg_ml?

pg_ml is a PostgreSQL extension that enables AutoML (Automated Machine Learning) directly within your database using PyCaret. It allows you to:

- **Train ML models** on table data using 15+ algorithms
- **Make predictions** using deployed models
- **Manage model versions** with deployment switching
- **Track experiments** for reproducibility

### Zero-Dependency Philosophy

pg_ml bundles [uv](https://github.com/astral-sh/uv) - Astral's Rust-based Python package manager. On first use, uv automatically:

1. Downloads Python 3.11 (if needed)
2. Creates a virtual environment
3. Installs PyCaret and all dependencies

No manual Python setup, pip install, or virtual environment management required.

### Key Features

| Feature | Description |
|---------|-------------|
| **AutoML** | Compare 15+ algorithms automatically with `compare_models()` |
| **Simple SQL API** | All operations via SQL functions |
| **Postgres-native storage** | Models stored as BYTEA, use `pg_dump` for backup |
| **Session-based training** | Familiar Jupyter-like workflow |
| **Reproducibility tracking** | Track train/test splits and data hashes |
| **Batch predictions** | Efficient Arrow-based data transfer |

### Comparison with PostgresML

| Feature | pg_ml | PostgresML |
|---------|-------|------------|
| Setup | Zero deps (bundled uv) | Requires Python venv |
| ML Library | PyCaret (AutoML) | Raw sklearn/XGBoost |
| AutoML | Built-in `compare_models()` | Manual algorithm selection |
| Algorithms | 15+ via PyCaret | 40+ (more granular control) |
| LLM/Embeddings | No | Yes |
| GPU Support | No | Yes |
| Complexity | Simple | Full-featured |

**Choose pg_ml** when you want AutoML simplicity. **Choose PostgresML** when you need fine-grained control or GPU support.

---

## 2. Installation & Setup

### Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Internet connection (first use only - downloads ~500MB of packages)
- ~1GB disk space for Python environment

### Installation

```bash
# Build and install (requires Rust and cargo-pgrx)
cd extensions/pg_ml
PG_CONFIG=/path/to/pg_config make install
```

### First-Time Setup

```sql
-- Create the extension
CREATE EXTENSION pg_ml;

-- Trigger venv setup (automatic on first ML operation if pg_ml.auto_setup = true)
SELECT pgml.setup_venv();
```

The first ML operation (or explicit `setup_venv()`) downloads Python and packages. This takes 2-3 minutes on a fast connection.

### Verify Installation

```sql
-- Check extension status
SELECT pgml.status();

-- Output:
-- pg_ml status:
--   venv_path: /var/lib/postgresql/pg_ml
--   venv_exists: true
--   uv: found at /usr/bin/uv
--   python: 3.11.9

-- Get detailed Python environment info
SELECT pgml.python_info();
```

---

## 3. Core Concepts

### Session-Scoped Experiment State

pg_ml uses a **session-based model** similar to a Jupyter notebook:

| Jupyter Notebook | pg_ml |
|------------------|-------|
| Kernel session | PostgreSQL connection |
| Cell execution | SQL query |
| Kernel restart | Reconnect to database |
| Variables persist between cells | Experiment state persists between queries |

**Critical Rules:**

1. **`setup()` + `create_model()`/`compare_models()`** must run in the **same connection**
2. **`predict()`** works from **any connection** (reads model from database)
3. **Reconnecting** clears experiment state (like restarting a Jupyter kernel)

### Projects and Models

- **Project**: A named ML experiment (e.g., "customer_churn_predictor")
- **Model**: A trained algorithm with metrics, stored as a serialized pickle in `pgml.models`
- **Deployment**: Only one model per project can be "deployed" (active for predictions)

```
Project: "iris_classifier"
├── Model 1: Random Forest (accuracy: 0.95) [DEPLOYED] ← predict() uses this one
├── Model 2: XGBoost (accuracy: 0.93)
└── Model 3: Logistic Regression (accuracy: 0.91)
```

### How Prediction Works

Models are **loaded from the database at prediction time** - there is no in-memory caching:

1. `predict('project_name', features)` is called
2. pg_ml queries `pgml.models` for the row where `deployed = true`
3. The `artifact` column (BYTEA) containing the pickled model is fetched
4. The model is unpickled in Python
5. Prediction is made and result returned

This means:
- **No warm-up needed** - predictions work immediately after training
- **Switching models is instant** - just call `pgml.deploy(model_id)`
- **Each prediction has database overhead** - for high-throughput, use `predict_batch()`

### Task Types

pg_ml supports two ML task types:

| Task | Target Column | Example |
|------|---------------|---------|
| **Classification** | Categorical (string or <10 unique values) | Predict species: setosa, versicolor, virginica |
| **Regression** | Numeric (continuous) | Predict house price: 150000, 275000, ... |

**Auto-detection**: If you don't specify `task`, pg_ml auto-detects based on the target column:
- String columns → Classification
- Numeric with <10 unique values → Classification
- Numeric with ≥10 unique values → Regression

---

## 4. Quick Start Tutorial

### Step 1: Load Sample Data

```sql
-- Load the iris dataset into pgml_samples.iris
SELECT * FROM pgml.load_dataset('iris');
```

**Output:**
| schema_name | table_name | row_count | columns |
|-------------|------------|-----------|---------|
| pgml_samples | iris | 150 | {sepal_length,sepal_width,petal_length,petal_width,species} |

### Step 2: Setup Experiment

```sql
-- Initialize the experiment (REQUIRED before training)
SELECT * FROM pgml.setup('pgml_samples.iris', 'species');
```

**Output:**
| experiment_id | task | target_column | feature_columns | train_size | fold |
|---------------|------|---------------|-----------------|------------|------|
| abc123... | classification | species | {sepal_length,sepal_width,petal_length,petal_width} | 0.7 | 10 |

### Step 3: Train a Model

```sql
-- Train a Random Forest model
SELECT * FROM pgml.create_model('iris_classifier', 'rf');
```

**Output:**
| project_id | model_id | algorithm | task | metrics | deployed |
|------------|----------|-----------|------|---------|----------|
| 1 | 1 | rf | classification | {"Accuracy": 0.96, "F1": 0.95, ...} | true |

### Step 4: Make Predictions

```sql
-- Single prediction (works from ANY connection)
SELECT pgml.predict('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
-- Returns: 'Iris-setosa'

-- Get class probabilities
SELECT pgml.predict_proba('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
-- Returns: {0.98, 0.01, 0.01}
```

### Step 5: Check Results

```sql
-- View all projects
SELECT * FROM pgml.projects;

-- View all models for a project
SELECT id, algorithm, metrics->>'Accuracy' as accuracy, deployed
FROM pgml.models
WHERE project_id = (SELECT id FROM pgml.projects WHERE name = 'iris_classifier');
```

---

## 5. SQL API Reference

All functions are in the `pgml` schema.

### 5.1 Environment Functions

#### `pgml.status()`

Returns extension status including venv path, Python version, and uv location.

```sql
pgml.status() RETURNS TEXT
```

**Example:**
```sql
SELECT pgml.status();
-- pg_ml status:
--   venv_path: /var/lib/postgresql/pg_ml
--   venv_exists: true
--   uv: found at /usr/bin/uv
--   python: 3.11.9
```

#### `pgml.setup_venv()`

Explicitly create or update the Python virtual environment.

```sql
pgml.setup_venv() RETURNS TEXT
```

**Example:**
```sql
SELECT pgml.setup_venv();
-- "pg_ml: Virtual environment ready at /var/lib/postgresql/pg_ml"
```

#### `pgml.python_info()`

Get detailed Python environment information as JSON.

```sql
pgml.python_info() RETURNS TEXT
```

**Returns JSON with:**
- `python_version`: Full Python version string
- `prefix`: Python installation prefix
- `executable`: Path to Python executable
- `packages`: Version info for key packages (pycaret, pandas, sklearn, numpy, xgboost, lightgbm)

---

### 5.2 Dataset Functions

#### `pgml.load_dataset()`

Load a sample dataset from PyCaret into a PostgreSQL table.

```sql
pgml.load_dataset(
    name TEXT,                              -- Dataset name (e.g., 'iris', 'boston', 'titanic')
    schema_name TEXT DEFAULT 'pgml_samples', -- Target schema
    table_name TEXT DEFAULT NULL,           -- Table name (defaults to dataset name)
    if_exists TEXT DEFAULT 'error'          -- 'error', 'replace', or 'skip'
) RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    row_count BIGINT,
    columns TEXT[]
)
```

**Automatic ID Column:**

`load_dataset()` automatically adds an `id SERIAL PRIMARY KEY` column to every loaded table. This enables:
- Reproducibility tracking with `id_column` parameter in `setup()`
- Batch predictions with `predict_batch()`
- Easy row identification for auditing

The `id` column is automatically excluded from ML features since it's a primary key.

**Parameters:**
| Parameter | Description |
|-----------|-------------|
| `name` | Dataset name from PyCaret's collection |
| `schema_name` | Schema to create table in (default: `pgml_samples`) |
| `table_name` | Table name (default: same as dataset name) |
| `if_exists` | Behavior when table exists: `'error'` (raise error), `'replace'` (drop and recreate), `'skip'` (return existing metadata) |

**Examples:**
```sql
-- Load iris with defaults (includes auto-generated 'id' column)
SELECT * FROM pgml.load_dataset('iris');

-- Load into custom schema and table
SELECT * FROM pgml.load_dataset('boston',
    schema_name => 'ml_data',
    table_name => 'housing_prices'
);

-- Replace existing table
SELECT * FROM pgml.load_dataset('iris', if_exists => 'replace');

-- Skip if exists (idempotent)
SELECT * FROM pgml.load_dataset('iris', if_exists => 'skip');
```

#### `pgml.list_datasets()`

List all available sample datasets.

```sql
pgml.list_datasets() RETURNS TABLE (
    name TEXT,
    task TEXT,           -- 'Classification', 'Regression', 'Clustering', etc.
    rows BIGINT,
    features BIGINT
)
```

**Available Datasets:**

| Task | Datasets |
|------|----------|
| Classification | iris, diabetes, cancer, blood, titanic, juice, pokemon |
| Regression | boston, gold, insurance |
| Clustering | jewellery, diamond, mice, migration, perfume, population |
| Anomaly Detection | anomaly |

**Example:**
```sql
-- List all datasets
SELECT * FROM pgml.list_datasets();

-- Filter by task type
SELECT * FROM pgml.list_datasets() WHERE task = 'Classification';
```

---

### 5.3 Training Functions

#### `pgml.setup()`

Initialize a PyCaret experiment on a table. **MUST be called before `create_model()` or `compare_models()`**.

```sql
pgml.setup(
    relation_name TEXT,                     -- Table name ('schema.table' or 'table')
    target TEXT,                            -- Target column to predict
    project_name TEXT DEFAULT NULL,         -- Optional: save config for load_experiment()
    task TEXT DEFAULT NULL,                 -- 'classification' or 'regression' (auto-detected)
    exclude_columns TEXT[] DEFAULT NULL,    -- Columns to exclude from features
    options JSONB DEFAULT NULL,             -- PyCaret setup options
    id_column TEXT DEFAULT NULL             -- Primary key for reproducibility tracking
) RETURNS TABLE (
    experiment_id TEXT,
    task TEXT,
    target_column TEXT,
    feature_columns TEXT[],
    train_size FLOAT,
    fold INTEGER
)
```

**Parameters:**
| Parameter | Description |
|-----------|-------------|
| `relation_name` | Table with training data (supports `schema.table` format) |
| `target` | Column name to predict |
| `project_name` | If provided, saves experiment config for later reload via `load_experiment()` |
| `task` | `'classification'` or `'regression'` (auto-detected if NULL) |
| `exclude_columns` | Columns to exclude from features (e.g., IDs, timestamps) |
| `options` | JSONB with PyCaret options (see table below) |
| `id_column` | Primary key column for tracking train/test splits (enables reproducibility verification) |

**Options JSONB:**
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `train_size` | float | 0.7 | Proportion of data for training (0.0-1.0) |
| `fold` | int | 10 | Number of cross-validation folds |
| `session_id` | int | 42 | Random seed for reproducibility |
| `normalize` | bool | false | Normalize numeric features |
| `transformation` | bool | false | Apply power transformation |

**Examples:**
```sql
-- Basic setup
SELECT * FROM pgml.setup('pgml_samples.iris', 'species');

-- Setup with project name for later reload
SELECT * FROM pgml.setup(
    'pgml_samples.iris',
    'species',
    project_name => 'iris_exp'
);

-- Setup with custom options
SELECT * FROM pgml.setup(
    'public.customers',
    'churned',
    exclude_columns => ARRAY['customer_id', 'created_at'],
    options => '{"train_size": 0.8, "fold": 5}'::jsonb
);

-- Setup with reproducibility tracking
SELECT * FROM pgml.setup(
    'pgml_samples.iris',
    'species',
    project_name => 'iris_tracked',
    id_column => 'id'  -- Must be a unique column
);
```

**Feature Column Detection:**
- Target column is automatically excluded
- Primary key columns are automatically excluded
- Columns in `exclude_columns` are excluded
- Remaining columns become features

#### `pgml.create_model()`

Train a specific algorithm. Requires `setup()` to be called first in the same connection.

```sql
pgml.create_model(
    project_name TEXT,                      -- Project name for storing the model
    algorithm TEXT,                         -- Algorithm ID (e.g., 'rf', 'xgboost')
    hyperparams JSONB DEFAULT NULL,         -- Optional hyperparameters
    deploy BOOLEAN DEFAULT true             -- Auto-deploy this model
) RETURNS TABLE (
    project_id BIGINT,
    model_id BIGINT,
    algorithm TEXT,
    task TEXT,
    metrics JSONB,
    deployed BOOLEAN
)
```

**Examples:**
```sql
-- Train a Random Forest
SELECT * FROM pgml.create_model('my_project', 'rf');

-- Train XGBoost with custom hyperparameters
SELECT * FROM pgml.create_model(
    'my_project',
    'xgboost',
    hyperparams => '{"n_estimators": 200, "max_depth": 5, "learning_rate": 0.05}'::jsonb
);

-- Train without auto-deploying
SELECT * FROM pgml.create_model('my_project', 'lr', deploy => false);
```

#### `pgml.compare_models()`

AutoML: compare multiple algorithms and return the best one(s). Requires `setup()` first.

```sql
pgml.compare_models(
    project_name TEXT,                      -- Project name
    n_select INTEGER DEFAULT 1,             -- Number of top models to select (see note below)
    sort TEXT DEFAULT NULL,                 -- Metric to sort by (e.g., 'Accuracy', 'F1')
    include TEXT[] DEFAULT NULL,            -- Only try these algorithms
    exclude TEXT[] DEFAULT NULL,            -- Skip these algorithms
    budget_time INTEGER DEFAULT 1800,       -- Max training time in seconds (30 min)
    deploy BOOLEAN DEFAULT true             -- Auto-deploy best model
) RETURNS TABLE (
    project_id BIGINT,
    model_id BIGINT,
    algorithm TEXT,
    task TEXT,
    metrics JSONB,
    deployed BOOLEAN
)
```

**Important Note on `n_select`:**

Currently, `compare_models()` stores only **one model** in `pgml.models` regardless of the `n_select` value. When `n_select > 1`:
- PyCaret internally compares and selects the top N models
- But only the **best model** (first in the list) is stored and deployed
- The `algorithm` column will contain the algorithm name of the best model

If you need multiple models stored, call `create_model()` separately for each algorithm:
```sql
-- Train multiple algorithms and store each
SELECT * FROM pgml.create_model('my_project', 'rf');
SELECT * FROM pgml.create_model('my_project', 'xgboost', deploy => false);
SELECT * FROM pgml.create_model('my_project', 'lightgbm', deploy => false);
```

**Examples:**
```sql
-- AutoML with defaults (compare all, store best)
SELECT * FROM pgml.compare_models('my_project');

-- Compare only tree-based algorithms
SELECT * FROM pgml.compare_models(
    'my_project',
    include => ARRAY['rf', 'xgboost', 'lightgbm', 'dt', 'et']
);

-- Exclude slow algorithms with 10-minute budget
SELECT * FROM pgml.compare_models(
    'my_project',
    exclude => ARRAY['svm', 'knn'],
    budget_time => 600
);

-- Sort by F1 score instead of Accuracy
SELECT * FROM pgml.compare_models(
    'my_project',
    sort => 'F1'
);
```

#### `pgml.load_experiment()`

Reload a saved experiment configuration. Re-fetches fresh data from the source table.

```sql
pgml.load_experiment(
    project_name TEXT                       -- Previously saved project name
) RETURNS TABLE (
    experiment_id TEXT,
    task TEXT,
    target_column TEXT,
    feature_columns TEXT[],
    train_size FLOAT,
    fold INTEGER
)
```

**Use Case:** MLOps workflows where you retrain on updated data.

```sql
-- Session 1: Initial training with project_name
SELECT * FROM pgml.setup(
    'pgml_samples.iris', 'species',
    project_name => 'iris_exp'
);
SELECT * FROM pgml.create_model('iris_exp', 'rf');

-- Session 2 (later): Reload and train new model
SELECT * FROM pgml.load_experiment('iris_exp');  -- Loads config, fetches fresh data
SELECT * FROM pgml.create_model('iris_exp', 'xgboost');  -- Train with new algorithm
```

#### `pgml.current_experiment()`

Check what experiment is currently loaded in your session.

```sql
pgml.current_experiment() RETURNS TABLE (
    project_name TEXT,
    task TEXT,
    target_column TEXT,
    feature_columns TEXT[],
    data_rows BIGINT,
    data_cols BIGINT
)
```

**Example:**
```sql
-- Check if experiment is loaded
SELECT * FROM pgml.current_experiment();
-- Returns NULL values if no experiment is active
```

---

### 5.4 Prediction Functions

#### `pgml.predict()`

Make a single prediction using the deployed model.

```sql
pgml.predict(
    project_name TEXT,                      -- Project with deployed model
    features FLOAT[]                        -- Feature values (must match training order)
) RETURNS TEXT
```

**Returns:**
- **Classification**: Original class label (e.g., `'Iris-setosa'`)
- **Regression**: Numeric value as string (e.g., `'150000.5'`)

**Examples:**
```sql
-- Classification prediction
SELECT pgml.predict('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
-- Returns: 'Iris-setosa'

-- Regression prediction
SELECT pgml.predict('house_prices', ARRAY[3, 2, 1500, 0.25]);
-- Returns: '275000.0'

-- Predict on existing data
SELECT
    customer_id,
    pgml.predict('churn_predictor', ARRAY[age, income, tenure, usage]) as will_churn
FROM customers
WHERE status = 'active';
```

#### `pgml.predict_proba()`

Get class probabilities (classification only).

```sql
pgml.predict_proba(
    project_name TEXT,
    features FLOAT[]
) RETURNS FLOAT[]
```

**Returns:** Array of probabilities, one per class, summing to ~1.0.

**Example:**
```sql
SELECT pgml.predict_proba('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
-- Returns: {0.98, 0.01, 0.01}
-- (98% setosa, 1% versicolor, 1% virginica)
```

#### `pgml.predict_batch()`

Efficiently predict on all rows of a table. Much faster than calling `predict()` per row.

```sql
pgml.predict_batch(
    project_name TEXT,                      -- Project with deployed model
    relation_name TEXT,                     -- Table to predict on
    id_column TEXT,                         -- Column for row identification
    feature_columns TEXT[] DEFAULT NULL     -- Feature columns (uses project config if NULL)
) RETURNS TABLE (
    id TEXT,
    prediction TEXT
)
```

**Benefits:**
- Model deserialized only once (vs. per-row)
- Vectorized batch prediction in Python
- Zero-copy Arrow data transfer

**Examples:**
```sql
-- Using project's stored feature columns
SELECT * FROM pgml.predict_batch(
    'iris_classifier',
    'pgml_samples.iris',
    'id'
);

-- Explicit feature columns
SELECT * FROM pgml.predict_batch(
    'iris_classifier',
    'pgml_samples.iris',
    'id',
    ARRAY['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
);

-- Join predictions back to original data
SELECT
    i.*,
    p.prediction
FROM pgml_samples.iris i
JOIN pgml.predict_batch('iris_classifier', 'pgml_samples.iris', 'id') p
    ON i.id::text = p.id;
```

---

### 5.5 Model Management

#### `pgml.deploy()`

Deploy a specific model by ID (switches the active model for predictions).

```sql
pgml.deploy(
    model_id BIGINT                         -- ID of the model to deploy
) RETURNS BOOLEAN
```

**Example:**
```sql
-- List models with their IDs
SELECT id, algorithm, metrics->>'Accuracy' as accuracy, deployed
FROM pgml.models
WHERE project_id = (SELECT id FROM pgml.projects WHERE name = 'my_project');

-- Deploy a different model
SELECT pgml.deploy(5);  -- Deploy model with id=5
```

#### `pgml.drop_project()`

Delete a project and all its models.

```sql
pgml.drop_project(
    project_name TEXT
) RETURNS BOOLEAN
```

**Example:**
```sql
SELECT pgml.drop_project('old_experiment');
-- Returns: true if project was found and deleted
```

#### `pgml.verify_experiment()`

Check if source data has changed since model training.

```sql
pgml.verify_experiment(
    project_name TEXT,
    model_id BIGINT DEFAULT NULL            -- Defaults to deployed model
) RETURNS JSONB
```

**Returns JSONB with:**
| Field | Description |
|-------|-------------|
| `unchanged` | `true` if data matches training data |
| `current_hash` | Current data hash |
| `training_hash` | Hash from training time |
| `message` | Human-readable status |
| `row_count` | Number of training rows |
| `train_rows` | Number of rows used in training |
| `test_rows` | Number of rows used in testing |

**Example:**
```sql
SELECT pgml.verify_experiment('iris_classifier');
-- Returns:
-- {
--   "unchanged": true,
--   "message": "Data unchanged since training. Experiment is reproducible.",
--   "row_count": 150,
--   "train_rows": 105,
--   "test_rows": 45
-- }
```

**Note:** Requires `id_column` to be provided during `setup()`.

---

### 5.6 Jupyter Kernel Functions

pg_ml includes an embedded Jupyter-compatible kernel for interactive development.

#### `pgml.init()`

Start the Jupyter kernel.

```sql
pgml.init() RETURNS JSONB
```

**Returns:** Connection info with ZMQ ports (shell, iopub, stdin, control, hb).

#### `pgml.shutdown()`

Stop the Jupyter kernel.

```sql
pgml.shutdown() RETURNS BOOLEAN
```

#### `pgml.kernel_status()`

Check if the kernel is running.

```sql
pgml.kernel_status() RETURNS BOOLEAN
```

---

## 6. Supported Algorithms

### Classification Algorithms

| Algorithm | Code | Description |
|-----------|------|-------------|
| Logistic Regression | `lr` | Simple, fast, interpretable |
| K Nearest Neighbors | `knn` | Instance-based learning |
| Naive Bayes | `nb` | Probabilistic classifier |
| Decision Tree | `dt` | Simple tree-based |
| **Random Forest** | `rf` | Ensemble of trees (recommended) |
| Extra Trees | `et` | Extremely randomized trees |
| **XGBoost** | `xgboost` | Gradient boosting (recommended) |
| **LightGBM** | `lightgbm` | Fast gradient boosting |
| CatBoost | `catboost` | Handles categorical features |
| AdaBoost | `ada` | Boosting algorithm |
| Gradient Boosting | `gbc` | Sklearn gradient boosting |
| SVM | `svm` | Support Vector Machine |
| RBF SVM | `rbfsvm` | SVM with RBF kernel |
| Gaussian Process | `gpc` | Probabilistic classifier |
| MLP | `mlp` | Neural network |
| Ridge | `ridge` | Ridge classifier |
| LDA | `lda` | Linear Discriminant Analysis |
| QDA | `qda` | Quadratic Discriminant Analysis |

### Regression Algorithms

| Algorithm | Code | Description |
|-----------|------|-------------|
| Linear Regression | `lr` | Simple linear model |
| Lasso | `lasso` | L1 regularization |
| Ridge | `ridge` | L2 regularization |
| Elastic Net | `en` | L1+L2 regularization |
| Least Angle Regression | `lar` | LAR algorithm |
| Lasso Least Angle | `llar` | Lasso LAR |
| OMP | `omp` | Orthogonal Matching Pursuit |
| Bayesian Ridge | `br` | Bayesian approach |
| ARD | `ard` | Automatic Relevance Determination |
| Passive Aggressive | `par` | Online learning |
| RANSAC | `ransac` | Robust linear model |
| Theil-Sen | `tr` | Robust regression |
| Huber | `huber` | Robust to outliers |
| Kernel Ridge | `kr` | Kernelized ridge |
| K Nearest Neighbors | `knn` | Instance-based |
| Decision Tree | `dt` | Tree-based |
| **Random Forest** | `rf` | Ensemble (recommended) |
| Extra Trees | `et` | Extremely randomized |
| AdaBoost | `ada` | Boosting |
| Gradient Boosting | `gbr` | Gradient boosting |
| **XGBoost** | `xgboost` | XGBoost (recommended) |
| **LightGBM** | `lightgbm` | Fast gradient boosting |
| CatBoost | `catboost` | Handles categorical |
| MLP | `mlp` | Neural network |
| SVM | `svm` | Support Vector Regression |

### Algorithm Selection Guide

| Scenario | Recommended Algorithm |
|----------|----------------------|
| **Quick baseline** | `rf` (Random Forest) |
| **Best performance** | `xgboost` or `lightgbm` |
| **Interpretability** | `dt` (Decision Tree) or `lr` (Logistic/Linear) |
| **Large datasets** | `lightgbm` (fastest) |
| **Categorical features** | `catboost` |
| **Don't know** | `compare_models()` |

---

## 7. Configuration Reference

### GUC Settings

Configure via `postgresql.conf` or `SET` commands:

| Setting | Type | Default | Context | Description |
|---------|------|---------|---------|-------------|
| `pg_ml.venv_path` | STRING | `/var/lib/postgresql/pg_ml` | Suset | Python venv location |
| `pg_ml.uv_path` | STRING | (auto-detect) | Suset | Override uv binary path |
| `pg_ml.auto_setup` | BOOL | `true` | Suset | Auto-create venv on first use |
| `pg_ml.mlflow_enabled` | BOOL | `false` | Postmaster | Enable MLflow UI server |
| `pg_ml.mlflow_port` | INT | `5000` | Postmaster | MLflow UI HTTP port |
| `pg_ml.mlflow_host` | STRING | `127.0.0.1` | Postmaster | MLflow server bind address |
| `pg_ml.mlflow_tracking` | BOOL | `false` | Userset | Enable MLflow tracking per session |

**Context meanings:**
- `Suset`: Superuser can change, per-session
- `Userset`: Any user can change, per-session
- `Postmaster`: Requires PostgreSQL restart

### Configuration Examples

**postgresql.conf:**
```ini
# Custom venv location
pg_ml.venv_path = '/data/ml/pg_ml'

# Disable auto-setup (require explicit setup_venv())
pg_ml.auto_setup = false

# Enable MLflow UI (requires restart)
pg_ml.mlflow_enabled = true
pg_ml.mlflow_port = 5000
```

**Runtime configuration:**
```sql
-- Change venv path for this session (superuser only)
SET pg_ml.venv_path = '/custom/path';

-- Enable MLflow tracking for this session
SET pg_ml.mlflow_tracking = true;
```

---

## 8. Integration Patterns

### SQL Clients (DBeaver, pgAdmin, psql)

Each SQL editor tab/window maintains its own connection. Run `setup()` and training in the **same tab**.

```sql
-- Run these together in one editor tab
SELECT * FROM pgml.setup('public.iris', 'species');
SELECT * FROM pgml.create_model('iris_classifier', 'rf');

-- Predictions can run in any tab
SELECT pgml.predict('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
```

### DBT Integration

#### Option 1: Using pre_hook

```sql
-- models/ml/train_classifier.sql
{{ config(
    materialized='table',
    pre_hook="SELECT * FROM pgml.setup('{{ ref(\"feature_table\") }}', 'target_column')"
) }}

SELECT * FROM pgml.create_model(
    'classifier_{{ run_started_at.strftime("%Y%m%d") }}',
    'rf'
)
```

#### Option 2: Using run_query

```sql
-- models/ml/train_classifier.sql
{{ config(materialized='table') }}

{% set setup_query %}
SELECT * FROM pgml.setup(
    '{{ ref("feature_table") }}',
    'target_column',
    options => '{"train_size": 0.8}'::jsonb
)
{% endset %}

{% do run_query(setup_query) %}

SELECT * FROM pgml.create_model('my_classifier', 'rf')
```

#### Prediction models (no session needed)

```sql
-- models/predictions.sql
{{ config(materialized='table') }}

SELECT
    id,
    pgml.predict('my_classifier', ARRAY[feat1, feat2, feat3]) as prediction
FROM {{ ref('new_data') }}
```

### Connection Pooling (PgBouncer)

| Pool Mode | Support | Notes |
|-----------|---------|-------|
| **Session mode** | Full | Connection stays with client |
| **Transaction mode** | Partial | Wrap setup + training in single transaction |
| **Statement mode** | Not supported | Cannot maintain session state |

**Transaction mode workaround:**
```sql
BEGIN;
SELECT * FROM pgml.setup('public.features', 'target');
SELECT * FROM pgml.create_model('my_model', 'rf');
COMMIT;
```

### Application Integration

#### Python (psycopg2)

```python
import psycopg2

# Training (must use same connection)
conn = psycopg2.connect("dbname=mydb")
cur = conn.cursor()
cur.execute("SELECT * FROM pgml.setup('public.data', 'target')")
cur.execute("SELECT * FROM pgml.create_model('my_model', 'rf')")
conn.commit()
cur.close()
conn.close()

# Prediction (any connection)
conn = psycopg2.connect("dbname=mydb")
cur = conn.cursor()
cur.execute("SELECT pgml.predict('my_model', ARRAY[1.0, 2.0, 3.0])")
result = cur.fetchone()[0]
```

#### Node.js (pg)

```javascript
const { Client } = require('pg');

// Training
const client = new Client();
await client.connect();
await client.query("SELECT * FROM pgml.setup('public.data', 'target')");
await client.query("SELECT * FROM pgml.create_model('my_model', 'rf')");
await client.end();

// Prediction
const client2 = new Client();
await client2.connect();
const res = await client2.query(
    "SELECT pgml.predict('my_model', $1)",
    [[1.0, 2.0, 3.0]]
);
console.log(res.rows[0].predict);
```

---

## 9. Metrics Reference

### Classification Metrics

| Metric | Key | Range | Description |
|--------|-----|-------|-------------|
| Accuracy | `Accuracy` | 0-1 | Proportion of correct predictions |
| F1 Score | `F1` | 0-1 | Harmonic mean of precision and recall |
| Precision | `Precision` | 0-1 | True positives / (True positives + False positives) |
| Recall | `Recall` | 0-1 | True positives / (True positives + False negatives) |
| AUC | `AUC` | 0-1 | Area under ROC curve |
| MCC | `MCC` | -1 to 1 | Matthews Correlation Coefficient |
| Kappa | `Kappa` | -1 to 1 | Cohen's Kappa |

### Regression Metrics

| Metric | Key | Range | Description |
|--------|-----|-------|-------------|
| MAE | `MAE` | 0-∞ | Mean Absolute Error |
| MSE | `MSE` | 0-∞ | Mean Squared Error |
| RMSE | `RMSE` | 0-∞ | Root Mean Squared Error |
| R² | `R2` | -∞ to 1 | Coefficient of determination |
| RMSLE | `RMSLE` | 0-∞ | Root Mean Squared Log Error |
| MAPE | `MAPE` | 0-∞ | Mean Absolute Percentage Error |

### Interpreting Metrics

```sql
-- Get metrics from a model
SELECT
    algorithm,
    metrics->>'Accuracy' as accuracy,
    metrics->>'F1' as f1_score,
    metrics->>'AUC' as auc
FROM pgml.models
WHERE project_id = (SELECT id FROM pgml.projects WHERE name = 'my_project')
ORDER BY (metrics->>'Accuracy')::float DESC;
```

---

## 10. Reproducibility & Experiment Tracking

pg_ml provides comprehensive reproducibility tracking to answer critical ML governance questions:
- **Which rows were used for training vs testing?**
- **Has the source data changed since training?**
- **Can I reproduce this experiment?**

### Enabling Reproducibility Tracking

To enable full reproducibility tracking, provide an `id_column` during `setup()`:

```sql
-- Enable tracking by specifying the primary key column
SELECT * FROM pgml.setup(
    'pgml_samples.iris',
    'species',
    project_name => 'iris_tracked',
    id_column => 'id'  -- This column identifies each row uniquely
);

-- Train a model (tracking data is automatically captured)
SELECT * FROM pgml.create_model('iris_tracked', 'rf');
```

**Requirements:**
- The `id_column` must uniquely identify each row
- The `project_name` must be provided to persist the configuration
- Both are needed for full reproducibility tracking

### What Gets Tracked

When `id_column` is provided, pg_ml stores the following in `pgml.experiment_splits`:

| Data | Description |
|------|-------------|
| `train_ids` | Array of row IDs used in the training set |
| `test_ids` | Array of row IDs used in the test/holdout set |
| `data_hash` | SHA-256 hash of the entire dataset at training time |
| `row_count` | Total number of rows in the dataset |
| `session_id` | PyCaret's random seed (for reproducible splits) |
| `train_size` | Train/test split ratio (e.g., 0.7) |

### Querying Train/Test Splits

You can directly query which rows were used for training vs testing:

```sql
-- Get the experiment split data for a model
SELECT
    e.id_column,
    e.data_hash,
    e.row_count,
    array_length(e.train_ids, 1) as train_count,
    array_length(e.test_ids, 1) as test_count,
    e.train_size,
    e.session_id
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'iris_tracked' AND m.deployed = true;
```

**Output:**
| id_column | data_hash | row_count | train_count | test_count | train_size | session_id |
|-----------|-----------|-----------|-------------|------------|------------|------------|
| id | abc123... | 150 | 105 | 45 | 0.7 | 42 |

### Retrieving Actual Train/Test Row IDs

```sql
-- Get the actual row IDs used in training
SELECT unnest(e.train_ids) as train_row_id
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'iris_tracked' AND m.deployed = true;

-- Get the actual row IDs used in testing
SELECT unnest(e.test_ids) as test_row_id
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'iris_tracked' AND m.deployed = true;
```

### Joining Back to Original Data

Retrieve the actual training or test data rows:

```sql
-- Get all training rows for a model
WITH split AS (
    SELECT e.train_ids
    FROM pgml.experiment_splits e
    JOIN pgml.models m ON e.model_id = m.id
    JOIN pgml.projects p ON m.project_id = p.id
    WHERE p.name = 'iris_tracked' AND m.deployed = true
)
SELECT i.*
FROM pgml_samples.iris i
WHERE i.id::text = ANY(SELECT unnest(train_ids) FROM split);

-- Get all test rows for a model
WITH split AS (
    SELECT e.test_ids
    FROM pgml.experiment_splits e
    JOIN pgml.models m ON e.model_id = m.id
    JOIN pgml.projects p ON m.project_id = p.id
    WHERE p.name = 'iris_tracked' AND m.deployed = true
)
SELECT i.*
FROM pgml_samples.iris i
WHERE i.id::text = ANY(SELECT unnest(test_ids) FROM split);
```

### Verifying Data Hasn't Changed

Use `verify_experiment()` to check if source data has changed since training:

```sql
SELECT pgml.verify_experiment('iris_tracked');
```

**Returns JSONB:**
```json
{
  "unchanged": true,
  "current_hash": "abc123def456...",
  "training_hash": "abc123def456...",
  "message": "Data unchanged since training. Experiment is reproducible.",
  "model_id": 1,
  "row_count": 150,
  "train_rows": 105,
  "test_rows": 45,
  "session_id": 42,
  "train_size": 0.7
}
```

**When data has changed:**
```json
{
  "unchanged": false,
  "current_hash": "xyz789...",
  "training_hash": "abc123...",
  "message": "WARNING: Data has changed since training. Experiment may not be reproducible.",
  ...
}
```

### Verifying a Specific Model

You can verify any model, not just the deployed one:

```sql
-- List all models with their IDs
SELECT id, algorithm, deployed, created_at
FROM pgml.models
WHERE project_id = (SELECT id FROM pgml.projects WHERE name = 'iris_tracked');

-- Verify a specific model by ID
SELECT pgml.verify_experiment('iris_tracked', model_id => 5);
```

### Complete Reproducibility Workflow

```sql
-- 1. Load data (id column is automatically created)
SELECT * FROM pgml.load_dataset('iris');

-- 2. Setup with tracking enabled (use the auto-created id column)
SELECT * FROM pgml.setup(
    'pgml_samples.iris',
    'species',
    project_name => 'iris_reproducible',
    id_column => 'id',
    options => '{"session_id": 42, "train_size": 0.7}'::jsonb
);

-- 3. Train model
SELECT * FROM pgml.create_model('iris_reproducible', 'rf');

-- 4. Later: Check which rows were used
SELECT
    'Training' as split,
    unnest(e.train_ids) as row_id
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'iris_reproducible' AND m.deployed = true
UNION ALL
SELECT
    'Testing' as split,
    unnest(e.test_ids) as row_id
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'iris_reproducible' AND m.deployed = true;

-- 5. Verify data integrity before making predictions
SELECT pgml.verify_experiment('iris_reproducible');

-- 6. If data changed, you can retrain with same config
SELECT * FROM pgml.load_experiment('iris_reproducible');
SELECT * FROM pgml.create_model('iris_reproducible', 'rf');
```

### Audit Trail Example

Create an audit view for ML governance:

```sql
CREATE VIEW ml_audit AS
SELECT
    p.name as project_name,
    m.id as model_id,
    m.algorithm,
    m.deployed,
    m.created_at as trained_at,
    m.metrics->>'Accuracy' as accuracy,
    e.row_count as total_rows,
    array_length(e.train_ids, 1) as train_rows,
    array_length(e.test_ids, 1) as test_rows,
    e.train_size,
    e.session_id,
    e.data_hash,
    p.source_schema || '.' || p.source_table as source_table
FROM pgml.projects p
JOIN pgml.models m ON p.id = m.project_id
LEFT JOIN pgml.experiment_splits e ON m.id = e.model_id
ORDER BY m.created_at DESC;

-- Query the audit trail
SELECT * FROM ml_audit WHERE project_name = 'iris_reproducible';
```

### Limitations

- **id_column required**: Tracking only works when `id_column` is provided during `setup()`
- **Row IDs stored as TEXT**: IDs are converted to strings for storage
- **Hash is content-based**: The data hash changes if ANY data in the table changes, not just the training columns
- **No automatic snapshots**: The actual data isn't copied, only row IDs are stored

---

## 11. Troubleshooting

### Common Errors

#### "Setup required: must call pgml.setup() before this function"

**Cause:** Called `create_model()` or `compare_models()` without `setup()` in the same connection.

**Solution:** Run `setup()` first, then train:
```sql
SELECT * FROM pgml.setup('my_table', 'target');
SELECT * FROM pgml.create_model('my_project', 'rf');
```

#### "No deployed model found for project 'X'"

**Cause:** No model has been trained and deployed for this project.

**Solutions:**
1. Train a model: `SELECT * FROM pgml.create_model('X', 'rf');`
2. Deploy an existing model: `SELECT pgml.deploy(model_id);`

#### "uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh"

**Cause:** The `uv` binary is not installed or not in PATH.

**Solutions:**
1. Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`
2. Or set `pg_ml.uv_path` to the uv location

#### "Virtual environment not found"

**Cause:** The Python venv hasn't been created yet.

**Solution:**
```sql
SELECT pgml.setup_venv();
```

#### Session state lost between queries

**Cause:** You're using a connection pooler in transaction/statement mode, or reconnecting between queries.

**Solutions:**
1. Use session-mode pooling
2. Run setup + training in a single transaction
3. Keep the same connection open

### Permission Issues

```sql
-- Check venv path
SHOW pg_ml.venv_path;

-- Ensure postgres user can write to venv_path
-- (run as shell on the server)
sudo chown -R postgres:postgres /var/lib/postgresql/pg_ml
```

### Debugging

```sql
-- Check extension status
SELECT pgml.status();

-- Check Python environment
SELECT pgml.python_info();

-- Check current experiment state
SELECT * FROM pgml.current_experiment();

-- List all projects and models
SELECT p.name, m.algorithm, m.deployed, m.created_at
FROM pgml.projects p
JOIN pgml.models m ON p.id = m.project_id
ORDER BY m.created_at DESC;
```

---

## 12. Best Practices

### Feature Engineering

1. **Exclude ID columns** - They don't carry predictive information
   ```sql
   SELECT * FROM pgml.setup('data', 'target', exclude_columns => ARRAY['id', 'created_at']);
   ```

2. **Use `id_column` for reproducibility** - Enables verification
   ```sql
   SELECT * FROM pgml.setup('data', 'target', id_column => 'id', project_name => 'my_exp');
   ```

3. **Preprocess in SQL** - Create a view or table with clean features
   ```sql
   CREATE VIEW ml_features AS
   SELECT
       id,
       age,
       COALESCE(income, 0) as income,  -- Handle NULLs
       CASE WHEN tenure > 12 THEN 'long' ELSE 'short' END as tenure_bucket,
       churned
   FROM customers;
   ```

### Model Selection

1. **Start with `compare_models()`** - Let AutoML find the best algorithm
   ```sql
   SELECT * FROM pgml.compare_models('my_project');
   ```

2. **Use `budget_time` for large datasets** - Prevent runaway training
   ```sql
   SELECT * FROM pgml.compare_models('my_project', budget_time => 300);  -- 5 minutes
   ```

3. **Exclude slow algorithms** if needed
   ```sql
   SELECT * FROM pgml.compare_models('my_project', exclude => ARRAY['svm', 'knn']);
   ```

### Production Deployment

1. **Use `project_name` during setup** - Enables experiment reload
   ```sql
   SELECT * FROM pgml.setup('data', 'target', project_name => 'prod_model');
   ```

2. **Version your models** - Create new models without replacing
   ```sql
   -- Train new model (auto-deploys by default)
   SELECT * FROM pgml.create_model('prod_model', 'xgboost');

   -- If new model is bad, redeploy old one
   SELECT pgml.deploy(old_model_id);
   ```

3. **Use batch predictions** - Much faster than per-row
   ```sql
   SELECT * FROM pgml.predict_batch('prod_model', 'new_data', 'id');
   ```

4. **Monitor with `verify_experiment()`** - Detect data drift
   ```sql
   SELECT pgml.verify_experiment('prod_model');
   ```

### Reproducibility

1. **Always use `id_column`** for critical experiments
2. **Set `session_id` in options** for deterministic splits
   ```sql
   SELECT * FROM pgml.setup('data', 'target', options => '{"session_id": 123}'::jsonb);
   ```
3. **Use `load_experiment()` for retraining** workflows

---

## 13. Database Schema Reference

All pg_ml tables are in the `pgml` schema.

### pgml.projects

Stores project metadata and experiment configuration.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `name` | TEXT | Unique project name |
| `task` | TEXT | 'classification' or 'regression' |
| `target_column` | TEXT | Column being predicted |
| `feature_columns` | TEXT[] | Array of feature column names |
| `source_schema` | TEXT | Original data schema (for reload) |
| `source_table` | TEXT | Original data table (for reload) |
| `exclude_columns` | TEXT[] | Columns excluded from features |
| `setup_options` | JSONB | PyCaret setup options |
| `id_column` | TEXT | Primary key column for tracking |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

### pgml.models

Stores trained models and their metadata.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `project_id` | BIGINT | Foreign key to projects |
| `algorithm` | TEXT | Algorithm code (e.g., 'rf') |
| `hyperparams` | JSONB | Training hyperparameters |
| `metrics` | JSONB | Performance metrics |
| `artifact` | BYTEA | Serialized model (pickle) |
| `label_classes` | TEXT[] | Class labels (classification) |
| `deployed` | BOOLEAN | Is this the active model? |
| `training_time_seconds` | FLOAT | Training duration |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

**Note:** Only one model per project can have `deployed = true` (enforced by partial unique index).

### pgml.experiment_splits

Stores train/test split information for reproducibility.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `model_id` | BIGINT | Foreign key to models |
| `id_column` | TEXT | Column used for row tracking |
| `train_ids` | TEXT[] | Row IDs used in training |
| `test_ids` | TEXT[] | Row IDs used in testing |
| `data_hash` | TEXT | SHA-256 hash of training data |
| `row_count` | INTEGER | Total rows in dataset |
| `session_id` | INTEGER | PyCaret session ID |
| `train_size` | FLOAT | Train/test split ratio |
| `created_at` | TIMESTAMPTZ | Creation timestamp |

### Querying the Schema

```sql
-- List all projects
SELECT name, task, target_column, array_length(feature_columns, 1) as num_features
FROM pgml.projects;

-- List models for a project with metrics
SELECT
    m.id,
    m.algorithm,
    m.metrics->>'Accuracy' as accuracy,
    m.deployed,
    m.training_time_seconds,
    m.created_at
FROM pgml.models m
JOIN pgml.projects p ON m.project_id = p.id
WHERE p.name = 'my_project'
ORDER BY (m.metrics->>'Accuracy')::float DESC;

-- Check experiment reproducibility
SELECT
    e.model_id,
    e.data_hash,
    e.row_count,
    array_length(e.train_ids, 1) as train_rows,
    array_length(e.test_ids, 1) as test_rows
FROM pgml.experiment_splits e
JOIN pgml.models m ON e.model_id = m.id
WHERE m.id = 1;
```

---

## Appendix: Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL Backend                        │
├─────────────────────────────────────────────────────────────┤
│  pg_ml Extension (Rust/pgrx)                                │
│  ├── SQL Functions (setup, create_model, predict, ...)     │
│  ├── Arrow Zero-Copy Transfer (table → DataFrame)          │
│  └── PyO3 Python Bridge                                     │
├─────────────────────────────────────────────────────────────┤
│  Python Virtual Environment                                  │
│  ├── PyCaret (AutoML)                                       │
│  ├── scikit-learn, XGBoost, LightGBM, ...                  │
│  └── PyArrow (zero-copy data transfer)                     │
├─────────────────────────────────────────────────────────────┤
│  pgml Schema                                                 │
│  ├── pgml.projects (project metadata)                       │
│  ├── pgml.models (serialized models as BYTEA)              │
│  └── pgml.experiment_splits (reproducibility tracking)      │
└─────────────────────────────────────────────────────────────┘
```

**Data Flow:**
1. `setup()`: PostgreSQL table → Arrow RecordBatch → pandas DataFrame → PyCaret
2. `create_model()`: PyCaret trains model → pickle serialization → BYTEA in pgml.models
3. `predict()`: Load BYTEA → unpickle → model.predict() → return result

---

*Generated for pg_ml extension. For updates, see the source repository.*
