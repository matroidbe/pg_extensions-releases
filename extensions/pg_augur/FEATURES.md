# pg_augur: Declarative ML Inside PostgreSQL

pg_augur brings machine learning directly into PostgreSQL as a native extension. Instead of moving data out to external ML platforms, you define experiments, train models, and run predictions — all in SQL. Built on [Augur](https://github.com/matroidbe/augur), a pure-Rust ML library, with zero Python dependencies.

## What This Unlocks

### 1. ML as SQL — No External Infrastructure

Train, predict, and manage models without leaving your database:

```sql
-- Define features, train, predict — three SQL statements
SELECT * FROM pgaugur.setup('public.customers', 'churned');
SELECT * FROM pgaugur.create_model('customers', 'xgboost');
SELECT pgaugur.predict('customers', ARRAY[45, 55000, 3]);
```

No Jupyter notebooks, no Python environments, no model serving infrastructure, no data movement. The model lives next to the data.

### 2. Augur DSL Experiments

Submit full ML experiments using Augur's declarative DSL directly from SQL. One statement defines the entire pipeline — data source, preprocessing, and training action:

```sql
SELECT * FROM pgaugur.experiment($augur$
experiment customer_churn {
    data: public.customers
    target: churned
    task: classification

    pipeline {
        impute mean
        encode onehot [region, plan_type]
        scale standard
    }

    compare
}
$augur$::text);
```

**What happens:** Augur parses the DSL, loads the Postgres table, applies the preprocessing pipeline, trains all registered algorithms (logistic regression, random forest, XGBoost, LightGBM, SVM, etc.), picks the best via cross-validation, stores the model, and deploys it — all in one call.

The DSL supports the full Augur feature set:
- **Preprocessing:** impute (mean/median/mode), encode (onehot/label/ordinal/target), scale (standard/minmax/robust), PCA, feature selection, outlier removal, derived columns
- **Actions:** `compare` (AutoML), `create <model>` (specific algorithm), `tune` (hyperparameter search)
- **Time series:** lag features, rolling windows, differencing, seasonal periods
- **Model-family routing:** different pipelines for linear vs tree models

### 3. Feature Views via Foreign Data Wrapper

The most powerful capability: define ML feature engineering as **DDL** using PostgreSQL's Foreign Data Wrapper system. Each column's preprocessing is declared via SQL OPTIONS — validated at `CREATE` time, stored in catalogs, visible via `\d`, and portable via `pg_dump`.

```sql
CREATE FOREIGN TABLE pgaugur.churn_features (
    age      FLOAT8   OPTIONS (impute 'mean', scale 'standard'),
    region   TEXT     OPTIONS (encode 'onehot'),
    income   FLOAT8   OPTIONS (impute 'median', scale 'robust'),
    smoker   TEXT     OPTIONS (encode 'onehot'),
    churned  BOOLEAN  OPTIONS (role 'target')
) SERVER pgaugur OPTIONS (
    source_table 'public.customers',
    task         'classification',
    action       'compare'
);
```

**`SELECT` triggers training:**

```sql
-- This one query: loads data, builds pipeline from column OPTIONS,
-- trains all models, deploys the best one
SELECT * FROM pgaugur.churn_features;
```

**Immediately usable for prediction:**

```sql
SELECT pgaugur.predict_row('churn_features',
    '{"age": 35, "region": "west", "income": 72000, "smoker": "no"}'::jsonb);
-- → {"prediction": 0, "algorithm": "xgboost", "project": "churn_features"}
```

#### Why Feature Views Matter

| What you get | How it works |
|---|---|
| **DDL-level validation** | Invalid OPTIONS (e.g., `scale 'bogus'`) rejected at `CREATE` time |
| **Catalog storage** | Feature definitions stored in `pg_catalog`, visible via `\d`, `\det` |
| **pg_dump/restore** | Feature views survive backup and restore — no external config files |
| **ALTER support** | `ALTER FOREIGN TABLE ... ALTER COLUMN ... OPTIONS (SET scale 'robust')` |
| **Standard SQL** | Works with any PostgreSQL client — psql, DBeaver, application code |
| **Composable** | Chain feature views via `source_view` for multi-stage pipelines |

#### Column OPTIONS Reference

| Option | Values | What it does |
|--------|--------|-------------|
| `impute` | `mean`, `median`, `mode`, `drop` | Handle missing values |
| `encode` | `onehot`, `label`, `ordinal`, `target` | Convert categoricals to numeric |
| `scale` | `standard`, `minmax`, `robust`, `maxabs` | Normalize numeric ranges |
| `transform` | `yeo_johnson`, `quantile` | Distribution normalization |
| `role` | `target`, `ignore`, `id` | Mark column purpose |
| `outlier` | `iqr:1.5`, `zscore:3.0` | Remove outliers |

#### Table OPTIONS Reference

| Option | Description |
|--------|-------------|
| `source_table` | Source Postgres table (e.g., `'public.customers'`) |
| `task` | `classification`, `regression`, `forecasting` (auto-detected if omitted) |
| `action` | `compare` (AutoML), `create` (train specific model) |
| `source_view` | Chain to a previous feature view (for multi-stage pipelines) |

### 4. Queryable Fitted State

After training, inspect exactly what the model learned — scaler parameters, encoder categories, imputer fill values:

```sql
SELECT * FROM pgaugur.fitted_params('churn_features');
```

```
 column_name | transformer     | param_name | param_value
-------------|-----------------|------------|--------------------
 age         | MeanImputer     | fill_value | 34.8
 age         | StandardScaler  | mean       | 34.8
 age         | StandardScaler  | std        | 12.6
 income      | MedianImputer   | fill_value | 52000.0
 income      | RobustScaler    | median     | 52000.0
 income      | RobustScaler    | iqr        | 28000.0
 region      | OneHotEncoder   | categories | ["east","north","south","west"]
 smoker      | OneHotEncoder   | categories | ["no","yes"]
```

This enables:
- **Monitoring:** detect data drift by comparing new data stats against fitted scaler params
- **Debugging:** understand exactly how features are transformed before model input
- **Documentation:** auto-generated feature documentation from the fitted pipeline

### 5. Portable Inference Artifacts

Export trained models as self-contained JSON bundles for deployment anywhere:

```sql
-- Full model spec: preprocessing pipeline + model weights + feature names
SELECT pgaugur.export_model('churn_features');
```

The exported JSON contains everything needed to reproduce predictions:
- Fitted preprocessing pipeline (imputer fill values, scaler means/stds, encoder category lists)
- Model weights (base64-encoded)
- Feature names and target column
- Task type and model algorithm

Load it in any Rust process via `augur::Predictor::from_spec()` — no Postgres needed.

### 6. Inference Schema & Validated Prediction

Get the expected input schema for a trained model:

```sql
SELECT pgaugur.inference_schema('churn_features');
```

Returns column specs, expected types, known categories, numeric ranges — usable for input validation in application code.

Predict with automatic validation:

```sql
SELECT pgaugur.predict_validated('churn_features',
    '{"age": 35, "region": "west", "income": 72000, "smoker": "no"}'::jsonb);
-- → {"prediction": 0, "algorithm": "xgboost", "warnings": [], "project": "churn_features"}
```

### 7. Async Training via Background Worker

For long-running training jobs, use the async API — training runs in a PostgreSQL background worker without blocking your session:

```sql
SELECT pgaugur.start_training(
    project_name => 'churn_model',
    source_table => 'public.customers',
    target_column => 'churned',
    automl => true
);
-- Returns job_id immediately

SELECT * FROM pgaugur.training_status(42);
-- Poll: state, progress, current_algorithm, best_so_far
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL                             │
│                                                          │
│  ┌──────────────────────┐   ┌─────────────────────────┐ │
│  │   SQL Interface       │   │   Foreign Data Wrapper   │ │
│  │                       │   │                          │ │
│  │  setup()              │   │  CREATE FOREIGN TABLE    │ │
│  │  create_model()       │   │    column OPTIONS →      │ │
│  │  compare_models()     │   │    feature engineering   │ │
│  │  predict()            │   │                          │ │
│  │  experiment(dsl)      │   │  SELECT * FROM →         │ │
│  │  fitted_params()      │   │    train + return data   │ │
│  │  export_model()       │   │                          │ │
│  └──────────┬───────────┘   └──────────┬──────────────┘ │
│             │                           │                │
│  ┌──────────▼───────────────────────────▼──────────────┐ │
│  │              Augur ML Engine (Pure Rust)              │ │
│  │                                                      │ │
│  │  DSL Parser → SetupConfig → Pipeline → Train → Model │ │
│  │                                                      │ │
│  │  Algorithms: LR, DT, RF, NB, SVM, XGBoost, LightGBM │ │
│  │  Preprocessing: Impute, Encode, Scale, PCA, Select   │ │
│  │  Tasks: Classification, Regression, Forecasting      │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                          │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              Storage (pgaugur schema)                 │ │
│  │  projects │ models │ experiments │ training_jobs      │ │
│  └──────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Three Ways to Use pg_augur

| Approach | Best for | Example |
|----------|----------|---------|
| **Imperative SQL** | Quick exploration, scripting | `SELECT pgaugur.create_model(...)` |
| **Augur DSL** | Full pipeline in one statement | `SELECT pgaugur.experiment($augur$...$augur$)` |
| **Feature Views (FDW)** | Persistent definitions, DDL-managed ML | `CREATE FOREIGN TABLE pgaugur.my_model (...)` |

All three approaches produce compatible models — `predict()`, `export_model()`, `fitted_params()`, and `inference_schema()` work identically regardless of how the model was created.

## Supported Algorithms

| Task | Algorithms |
|------|-----------|
| Classification | Logistic Regression, Decision Tree, Random Forest, Naive Bayes, SVM, XGBoost, LightGBM |
| Regression | Linear, Ridge, Lasso, Elastic Net, Decision Tree, Random Forest, SVM, XGBoost, LightGBM |
| Forecasting | ETS, MSTL, XGBoost, Ridge, LightGBM |

## Requirements

- PostgreSQL 14, 15, 16, 17, or 18
- Rust extension built with [pgrx](https://github.com/pgcentralfoundation/pgrx)
- No Python runtime, no external services, no GPUs required
- Standard `CREATE EXTENSION pg_augur` installation
