# pg_ml User Manual

Machine learning in PostgreSQL using PyCaret.

## Overview

pg_ml brings AutoML capabilities to PostgreSQL, allowing you to train, deploy, and serve machine learning models directly from SQL. No external services required - everything runs inside PostgreSQL.

### Key Features

- **Zero Dependencies**: Automatic Python environment setup
- **AutoML**: Compare algorithms and select the best
- **Async Training**: Long-running jobs via background worker
- **Conformal Prediction**: Prediction intervals with coverage guarantees
- **MLflow Integration**: Experiment tracking and visualization

### How It Works

```
+---------------------------------------------------------------+
|                      PostgreSQL                                 |
|                                                                 |
|  +-------------+    +------------------+    +--------------+   |
|  | Training    |--->|     pg_ml        |--->| Predictions  |   |
|  | Data        |    |                  |    | (real-time)  |   |
|  +-------------+    | - Python/PyCaret |    +--------------+   |
|                     | - Model storage  |                        |
|                     | - Serving layer  |                        |
|                     +------------------+                        |
+---------------------------------------------------------------+
```

pg_ml embeds a Python runtime with PyCaret inside PostgreSQL. Models are trained on table data and stored in PostgreSQL, then served via SQL functions.

### When to Use pg_ml

**Good fit:**
- Training on data already in PostgreSQL
- Real-time predictions from SQL
- Rapid prototyping and experimentation
- Edge deployments without ML infrastructure

**Consider external ML platforms when:**
- Need distributed training (very large datasets)
- Require GPU training at scale
- Have existing MLOps infrastructure

## Concepts

### Projects

A project is a named container for experiments and models targeting the same prediction problem:

```sql
SELECT * FROM pgml.setup(
    'customers',
    'churned',
    project_name => 'churn_predictor'
);
```

Each project has:
- One active experiment (setup configuration)
- Multiple models (trained algorithms)
- One deployed model (used for predictions)

### Experiments

An experiment captures the data configuration for training:

```sql
-- View current experiment
SELECT * FROM pgml.current_experiment();
```

Experiment configuration includes:
- Source table/view
- Target column
- Feature columns
- Train/test split
- Cross-validation folds

### Models

A model is a trained algorithm within a project:

```sql
-- Train a specific algorithm
SELECT * FROM pgml.create_model('churn_predictor', 'xgboost');

-- Or compare all algorithms
SELECT * FROM pgml.compare_models('churn_predictor');
```

Model metadata stored:
- Algorithm name
- Hyperparameters
- Training metrics (accuracy, F1, RMSE, etc.)
- Serialized model artifact
- Deployment status

### Conformal Prediction

Conformal prediction provides calibrated prediction intervals:

```sql
-- Train with conformal prediction enabled
SELECT * FROM pgml.create_model(
    'sales_predictor',
    'rf',
    conformal => true
);

-- Get prediction with interval
SELECT pgml.predict_interval('sales_predictor', ARRAY[1.0, 2.0], 0.1);
-- Returns: {"prediction": 42.5, "lower": 38.2, "upper": 46.8}
```

The interval guarantees coverage at the specified confidence level (90% for alpha=0.1).

## Getting Started

### 1. Install the Extension

```sql
CREATE EXTENSION pg_ml CASCADE;
```

### 2. Set Up Python Environment

This is a one-time setup that creates a virtual environment with PyCaret:

```sql
SELECT pgml.setup_venv();
```

This takes 5-10 minutes and installs:
- Python 3.9+
- PyCaret (classification + regression)
- scikit-learn, XGBoost, LightGBM
- All dependencies

Check status:

```sql
SELECT pgml.status();
-- Returns installation status and Python version
```

### 3. Load Sample Data (Optional)

```sql
SELECT * FROM pgml.load_dataset('iris');
-- Creates pgml.iris table with flower classification data
```

Available datasets:
- `iris` - Flower classification (150 rows)
- `wine` - Wine quality regression
- `breast_cancer` - Cancer classification
- `diabetes` - Diabetes regression
- `boston` - Housing price regression

### 4. Configure Experiment

```sql
SELECT * FROM pgml.setup(
    'pgml.iris',           -- table name
    'species',             -- target column
    project_name => 'iris_classifier'
);
```

### 5. Train a Model

```sql
-- Quick: train single algorithm
SELECT * FROM pgml.create_model('iris_classifier', 'rf');

-- Thorough: compare all algorithms
SELECT * FROM pgml.compare_models('iris_classifier', budget_time => 300);
```

### 6. Make Predictions

```sql
SELECT pgml.predict('iris_classifier', ARRAY[5.1, 3.5, 1.4, 0.2]);
-- Returns: 'setosa'
```

## Training Workflow

### Step 1: Prepare Data

Ensure your table has:
- A target column (what you're predicting)
- Feature columns (numeric or categorical inputs)
- No missing values in target (NULLs filtered automatically)

```sql
-- Example: customer churn prediction
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    age INTEGER,
    tenure_months INTEGER,
    monthly_charges NUMERIC,
    total_charges NUMERIC,
    churned BOOLEAN  -- target
);
```

### Step 2: Initialize Experiment

```sql
SELECT * FROM pgml.setup(
    'customers',           -- table name
    'churned',             -- target column
    project_name => 'churn_model',
    task => 'classification'  -- or 'regression'
);
```

**setup() options:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `relation_name` | TEXT | Table or view name |
| `target` | TEXT | Target column |
| `project_name` | TEXT | Project identifier |
| `task` | TEXT | 'classification' or 'regression' (auto-detected if omitted) |
| `exclude_columns` | TEXT[] | Columns to exclude from features |
| `id_column` | TEXT | ID column for reproducibility tracking |
| `options` | JSONB | Additional PyCaret options |

**PyCaret options (via options JSONB):**

```sql
SELECT * FROM pgml.setup(
    'customers',
    'churned',
    project_name => 'churn_model',
    options => '{
        "normalize": true,
        "transformation": true,
        "fold": 5,
        "session_id": 42
    }'::jsonb
);
```

### Step 3: Train Model

**Option A: Single Algorithm**

```sql
SELECT * FROM pgml.create_model('churn_model', 'xgboost');
```

Returns:
| project_id | model_id | algorithm | task | metrics | deployed |
|------------|----------|-----------|------|---------|----------|
| 1 | 3 | xgboost | classification | {"Accuracy": 0.92, ...} | true |

**Option B: AutoML (compare all)**

```sql
SELECT * FROM pgml.compare_models(
    'churn_model',
    budget_time => 300,  -- 5 minutes
    n_select => 3        -- return top 3 models
);
```

PyCaret trains and cross-validates all compatible algorithms, returning the best by default metric.

**Option C: Async Training (non-blocking)**

For long-running training jobs:

```sql
SELECT pgml.start_training(
    'churn_model',
    'public.customers',
    'churned',
    automl => true,
    budget_time => 30  -- minutes
);
-- Returns: 42 (job_id)
```

See [Async Training](#async-training) for details.

### Step 4: Evaluate Results

```sql
-- View all models for a project
SELECT model_id, algorithm, metrics, deployed
FROM pgml.models
WHERE project_name = 'churn_model'
ORDER BY (metrics->>'Accuracy')::float DESC;
```

### Step 5: Deploy Model

The best model is deployed automatically. To switch:

```sql
SELECT pgml.deploy('churn_model', 5);  -- deploy model_id 5
```

### Step 6: Make Predictions

```sql
-- Single prediction
SELECT pgml.predict('churn_model', ARRAY[35, 24, 65.5, 1200.0]);

-- With probabilities (classification)
SELECT pgml.predict_proba('churn_model', ARRAY[35, 24, 65.5, 1200.0]);
-- Returns: {0.15, 0.85}

-- Batch prediction
SELECT * FROM pgml.predict_batch(
    'churn_model',
    'new_customers',  -- table to predict on
    'id'              -- ID column for output
);
```

## Async Training

For training jobs that take minutes or hours, use async training to avoid blocking database connections.

### Starting Async Training

```sql
SELECT pgml.start_training(
    'price_predictor',        -- project name
    'sales.products',         -- source table
    'price',                  -- target column
    task => 'regression',
    automl => true,
    budget_time => 30,        -- max 30 minutes
    exclude_columns => ARRAY['id', 'created_at'],
    conformal => true
);
-- Returns: job_id (e.g., 42)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project_name` | TEXT | required | Project identifier |
| `source_table` | TEXT | required | Table or view name |
| `target_column` | TEXT | required | Target column |
| `algorithm` | TEXT | NULL | Single algorithm name |
| `automl` | BOOLEAN | false | Compare all algorithms |
| `task` | TEXT | NULL | classification/regression |
| `exclude_columns` | TEXT[] | NULL | Columns to exclude |
| `train_size` | FLOAT | 0.8 | Train/test split |
| `budget_time` | INTEGER | NULL | Max time in minutes |
| `conformal` | BOOLEAN | false | Enable conformal prediction |
| `metric` | TEXT | NULL | Optimization metric |
| `setup_options` | JSONB | NULL | PyCaret setup options |

### Monitoring Progress

```sql
SELECT * FROM pgml.training_status(42);
```

Returns:

| Column | Description |
|--------|-------------|
| `job_id` | Job identifier |
| `project_name` | Project name |
| `state` | queued, setup, training, completed, failed, cancelled |
| `progress` | 0.0 to 1.0 |
| `current_step` | Human-readable step description |
| `algorithms_tested` | (AutoML) Count tested |
| `algorithms_total` | (AutoML) Total to test |
| `best_so_far` | Best model found so far (JSONB) |
| `model_id` | Final model ID (when completed) |
| `elapsed_seconds` | Training duration |

### LISTEN/NOTIFY

Get notified when training completes:

```sql
LISTEN pgml_training;

-- In another session, start training
SELECT pgml.start_training(...);

-- When complete, you receive:
-- {"job_id": 42, "state": "completed", "model_id": 17, "project": "my_model"}
```

### Cancellation

```sql
SELECT pgml.cancel_training(42);
```

Returns `true` if cancelled, `false` if already completed/failed.

### Background Worker Configuration

```sql
-- Enable/disable worker (requires restart)
ALTER SYSTEM SET pg_ml.training_worker_enabled = true;

-- Job polling interval
SET pg_ml.training_poll_interval = 1000;  -- ms

-- Target database
ALTER SYSTEM SET pg_ml.training_database = 'mydb';
```

## Model Deployment and Serving

### Deploying Models

Deploy a specific model by ID:

```sql
-- List available models
SELECT model_id, algorithm, metrics, deployed
FROM pgml.models
WHERE project_name = 'churn_model';

-- Deploy model 5
SELECT pgml.deploy('churn_model', 5);
```

Only one model can be deployed per project at a time.

### Prediction Functions

**predict()** - Point prediction

```sql
SELECT pgml.predict('churn_model', ARRAY[35, 24, 65.5, 1200.0]);
-- Returns: 'true' (classification) or '42.5' (regression)
```

**predict_proba()** - Class probabilities (classification only)

```sql
SELECT pgml.predict_proba('churn_model', ARRAY[35, 24, 65.5, 1200.0]);
-- Returns: {0.15, 0.85} (probability for each class)
```

**predict_interval()** - Prediction with confidence interval

Requires model trained with `conformal => true`:

```sql
SELECT pgml.predict_interval('sales_model', ARRAY[1.0, 2.0], 0.1);
-- Returns: {"prediction": 42.5, "lower": 38.2, "upper": 46.8}
```

The interval provides 90% coverage (1 - alpha = 1 - 0.1).

**predict_batch()** - Bulk predictions on a table

```sql
SELECT * FROM pgml.predict_batch(
    'churn_model',
    'new_customers',  -- source table
    'customer_id'     -- ID column
);

-- Returns:
-- | id    | prediction |
-- |-------|------------|
-- | C001  | false      |
-- | C002  | true       |
```

### Feature Order

Features must be provided in the same order as training. Check experiment configuration:

```sql
SELECT feature_columns FROM pgml.current_experiment()
WHERE project_name = 'churn_model';
-- Returns: {age, tenure_months, monthly_charges, total_charges}
```

## Supported Algorithms

### Classification

| Code | Algorithm |
|------|-----------|
| `lr` | Logistic Regression |
| `knn` | K-Nearest Neighbors |
| `nb` | Naive Bayes |
| `dt` | Decision Tree |
| `rf` | Random Forest |
| `et` | Extra Trees |
| `gbc` | Gradient Boosting |
| `xgboost` | XGBoost |
| `lightgbm` | LightGBM |
| `catboost` | CatBoost |
| `ada` | AdaBoost |
| `svm` | Support Vector Machine |
| `ridge` | Ridge Classifier |
| `lda` | Linear Discriminant Analysis |
| `qda` | Quadratic Discriminant Analysis |

### Regression

| Code | Algorithm |
|------|-----------|
| `lr` | Linear Regression |
| `lasso` | Lasso Regression |
| `ridge` | Ridge Regression |
| `en` | Elastic Net |
| `lar` | Least Angle Regression |
| `llar` | Lasso Least Angle |
| `omp` | Orthogonal Matching Pursuit |
| `br` | Bayesian Ridge |
| `ard` | Automatic Relevance Determination |
| `par` | Passive Aggressive |
| `ransac` | RANSAC |
| `tr` | TheilSen |
| `huber` | Huber |
| `kr` | Kernel Ridge |
| `svm` | Support Vector Machine |
| `knn` | K-Nearest Neighbors |
| `dt` | Decision Tree |
| `rf` | Random Forest |
| `et` | Extra Trees |
| `ada` | AdaBoost |
| `gbr` | Gradient Boosting |
| `xgboost` | XGBoost |
| `lightgbm` | LightGBM |
| `catboost` | CatBoost |

## MLflow Integration

pg_ml includes an embedded MLflow server for experiment tracking and visualization.

### Enabling MLflow

```sql
-- Enable MLflow server (requires restart)
ALTER SYSTEM SET pg_ml.mlflow_enabled = true;
ALTER SYSTEM SET pg_ml.mlflow_host = '0.0.0.0';  -- Bind address
ALTER SYSTEM SET pg_ml.mlflow_port = 5000;

-- Enable tracking for current session
SET pg_ml.mlflow_tracking = true;
```

### Using MLflow UI

```sql
SELECT pgml.mlflow_url();
-- Returns: 'http://127.0.0.1:5000'
```

Open this URL in a browser to:
- View experiment runs
- Compare model metrics
- Visualize training progress
- Download model artifacts

### Experiment Tracking

When `mlflow_tracking` is enabled, training operations automatically log:
- Parameters (algorithm, hyperparameters)
- Metrics (accuracy, F1, RMSE, etc.)
- Model artifacts
- Training duration

## Best Practices

### Data Preparation

1. **Handle missing values** before setup:

   ```sql
   -- Option 1: Filter out NULLs
   CREATE VIEW customers_clean AS
   SELECT * FROM customers WHERE age IS NOT NULL;

   -- Option 2: Impute values
   UPDATE customers SET age = 30 WHERE age IS NULL;
   ```

2. **Balance classes** for classification:

   ```sql
   -- Check class balance
   SELECT churned, COUNT(*) FROM customers GROUP BY 1;
   ```

3. **Exclude ID columns**:

   ```sql
   SELECT * FROM pgml.setup(
       'customers',
       'churned',
       exclude_columns => ARRAY['id', 'created_at', 'updated_at']
   );
   ```

### Model Selection

1. **Start with fast algorithms** for exploration:

   ```sql
   -- Quick baseline
   SELECT * FROM pgml.create_model('my_model', 'lr');
   ```

2. **Use AutoML for production**:

   ```sql
   SELECT * FROM pgml.compare_models(
       'my_model',
       budget_time => 600,  -- 10 minutes
       exclude => ARRAY['catboost']  -- slow to train
   );
   ```

3. **Enable conformal prediction** for uncertainty quantification:

   ```sql
   SELECT * FROM pgml.create_model(
       'my_model',
       'rf',
       conformal => true
   );
   ```

### Production Deployment

1. **Monitor model drift**:

   ```sql
   -- Compare current data hash to training data
   SELECT pgml.verify_experiment('my_model');
   ```

2. **Version your models**: Track which model_id is deployed.

3. **Use batch prediction** for throughput:

   ```sql
   -- Faster than individual predictions
   SELECT * FROM pgml.predict_batch('my_model', 'customers', 'id');
   ```

4. **Set resource limits** for training:

   ```sql
   SET statement_timeout = '30min';  -- Limit training time
   ```

### MLflow Best Practices

1. Enable tracking for experiment comparison:

   ```sql
   SET pg_ml.mlflow_tracking = true;
   ```

2. Use consistent project names for related experiments.

3. Review MLflow UI before deploying to production.

## Configuration Reference

See [configuration.md](configuration.md#pg_ml) for the complete list of GUC parameters.

Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pg_ml.venv_path` | (auto) | Python virtual environment path |
| `pg_ml.auto_setup` | `true` | Auto-create venv on extension load |
| `pg_ml.training_worker_enabled` | `true` | Enable async training worker |
| `pg_ml.training_database` | `postgres` | Database for worker connection |
| `pg_ml.mlflow_enabled` | `false` | Enable MLflow UI server |
| `pg_ml.mlflow_port` | `5000` | MLflow server port |

## Troubleshooting

See [troubleshooting.md](troubleshooting.md#pg_ml) for common issues and solutions.

Quick diagnostics:

```sql
-- Check extension status
SELECT pgml.status();

-- Check Python environment
SELECT pgml.python_info();

-- List projects
SELECT DISTINCT project_name FROM pgml.models;

-- Check deployed model
SELECT * FROM pgml.models WHERE project_name = 'my_model' AND deployed;

-- Training job status
SELECT * FROM pgml.training_status(42);
```
