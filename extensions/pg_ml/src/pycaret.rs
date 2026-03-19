//! PyCaret Python wrapper integration
//!
//! Provides Rust functions that call the embedded Python PyCaret wrapper,
//! using Arrow zero-copy transfer for efficient data handling.
//!
//! ## Architecture
//!
//! This module mirrors PyCaret's session-based API:
//! 1. `run_setup()` - Initialize experiment (REQUIRED FIRST)
//! 2. `run_compare_models()` / `run_create_model()` - Train models (after setup)
//! 3. `run_predict()` / `run_predict_proba()` - Make predictions
//!
//! The Python wrapper maintains experiment state in module globals, allowing
//! `compare_models()` and `create_model()` to access the setup configuration.

use crate::arrow_convert::table_to_dataframe_arrow;
use crate::PgMlError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

// =============================================================================
// Embedded Python Code
// =============================================================================

/// The embedded Python PyCaret wrapper module
const PYCARET_WRAPPER_CODE: &str = r#"
import os
import sys
import pickle
import uuid
from typing import Optional, Dict, Any, List

# Configure matplotlib to use non-interactive Agg backend BEFORE any imports
# This prevents plt.show() from blocking and allows server-side rendering
import matplotlib
matplotlib.use('Agg')

# Suppress matplotlib font warnings
import warnings
warnings.filterwarnings('ignore', message='.*findfont.*')

# Disable tqdm progress bars globally - prevents BrokenPipeError in Postgres
# where there's no terminal attached to stderr
os.environ['TQDM_DISABLE'] = '1'

# Redirect stderr to devnull to suppress any remaining output attempts
# This is necessary because PyCaret's CommonDisplay still tries to write
# even with verbose=False
class _DevNull:
    def write(self, *args, **kwargs): pass
    def flush(self, *args, **kwargs): pass

_original_stderr = sys.stderr
sys.stderr = _DevNull()

# Module-level state for current experiment
_experiment_state = {
    'experiment_id': None,
    'task': None,
    'target': None,
    'feature_columns': None,
    'setup_complete': False,
    'label_classes': None,  # For classification: maps numeric labels to original text labels
    'project_name': None,   # Project name for persistent storage
    'data_shape': None,     # (rows, columns) of training data
    'id_column': None,      # Primary key column for reproducibility tracking
    'df_with_ids': None,    # DataFrame with ID column preserved for split tracking
    'session_id': None,     # PyCaret session_id used for reproducibility
    'train_size': None,     # Train split ratio used
    'loaded_model': None,   # Deserialized model object loaded from DB (via load_experiment)
    'loaded_model_id': None,      # model ID from pgml.models table
    'loaded_model_algorithm': None,  # algorithm name of loaded model
    'setup_df': None,       # Training DataFrame (for re-initializing PyCaret if needed)
    'setup_kwargs': None,   # kwargs passed to PyCaret's setup() (for re-initialization)
}

# MLflow tracking configuration
_mlflow_config = {
    'enabled': False,
    'tracking_uri': None,
    'experiment_name': None,
}


def configure_mlflow(enabled: bool, tracking_uri: str = None, experiment_name: str = None):
    """Configure MLflow tracking for PyCaret experiments.

    Args:
        enabled: Whether to enable MLflow logging
        tracking_uri: MLflow tracking URI (e.g., 'file:///path/to/mlruns')
        experiment_name: Name for the MLflow experiment
    """
    global _mlflow_config

    _mlflow_config['enabled'] = enabled
    _mlflow_config['tracking_uri'] = tracking_uri
    _mlflow_config['experiment_name'] = experiment_name

    if enabled and tracking_uri:
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        if experiment_name:
            mlflow.set_experiment(experiment_name)


def is_mlflow_enabled() -> bool:
    """Check if MLflow tracking is enabled."""
    return _mlflow_config.get('enabled', False)


def detect_task(df, target: str) -> str:
    """
    Auto-detect task type from target column.

    Classification if:
    - Target is object/string dtype
    - Target has fewer than 10 unique values (categorical)

    Regression otherwise.
    """
    target_col = df[target]

    # String/object types are always classification
    if target_col.dtype == 'object' or str(target_col.dtype).startswith('string'):
        return 'classification'

    # Low cardinality numeric is classification
    n_unique = target_col.nunique()
    if n_unique <= 10:
        return 'classification'

    return 'regression'


def pycaret_setup(df, target, task=None, exclude_columns=None, options=None, id_column=None):
    """Initialize PyCaret experiment. MUST be called before compare_models/create_model.

    Args:
        df: Arrow-backed pandas DataFrame from table_to_dataframe_arrow()
        target: Target column name
        task: 'classification' or 'regression' (auto-detect if None)
        exclude_columns: Columns to drop before training
        options: Dict with setup options (train_size, fold, normalize, etc.)
        id_column: Primary key column for reproducibility tracking (optional)

    Returns:
        dict with experiment metadata
    """
    global _experiment_state

    if options is None:
        options = {}

    if task is None:
        task = detect_task(df, target)

    # Preserve DataFrame with ID column before any modifications
    df_with_ids = df.copy() if id_column and id_column in df.columns else None

    # Remove excluded columns (but not the id_column - we handle it separately)
    if exclude_columns:
        df = df.drop(columns=[c for c in exclude_columns if c in df.columns])

    # Feature columns exclude target and id_column
    feature_columns = [c for c in df.columns if c != target and c != id_column]

    # Remove id_column from training data if present
    if id_column and id_column in df.columns:
        df = df.drop(columns=[id_column])

    # Import appropriate PyCaret module
    if task == 'classification':
        from pycaret.classification import setup
    else:
        from pycaret.regression import setup

    # Build setup() kwargs
    # Enable log_experiment if MLflow tracking is enabled
    mlflow_enabled = is_mlflow_enabled()
    setup_kwargs = {
        'data': df,
        'target': target,
        'session_id': options.get('session_id', 42),
        'verbose': False,
        'html': False,
        'log_experiment': mlflow_enabled,
    }

    # Set experiment name if MLflow is enabled
    if mlflow_enabled and _mlflow_config.get('experiment_name'):
        setup_kwargs['experiment_name'] = _mlflow_config['experiment_name']

    # Map options to setup params
    if 'train_size' in options:
        setup_kwargs['train_size'] = options['train_size']
    if 'fold' in options:
        setup_kwargs['fold'] = options['fold']
    if 'normalize' in options and options['normalize']:
        setup_kwargs['normalize'] = True
    if 'transformation' in options and options['transformation']:
        setup_kwargs['transformation'] = True
    if 'n_jobs' in options:
        setup_kwargs['n_jobs'] = options['n_jobs']
    if 'use_gpu' in options:
        setup_kwargs['use_gpu'] = options['use_gpu']

    # Call PyCaret setup - this initializes the global experiment
    setup(**setup_kwargs)

    # Extract label classes for classification tasks
    # PyCaret uses LabelEncoder to encode string labels to integers
    # We need to get the classes in the exact order the encoder uses (alphabetical)
    label_classes = None
    if task == 'classification':
        try:
            from pycaret.classification import get_config
            # Try to get the pipeline which contains the LabelEncoder
            pipeline = get_config('pipeline')
            if pipeline is not None:
                # Look for LabelEncoder in the pipeline steps
                for name, step in pipeline.steps:
                    if hasattr(step, 'classes_'):
                        label_classes = [str(c) for c in step.classes_.tolist()]
                        break

            # If not found in pipeline, try to get from the data transformer
            if label_classes is None:
                # Fall back to sorted unique values (which is what LabelEncoder uses)
                y = get_config('y')
                if y is not None:
                    # Sort unique values to match LabelEncoder's alphabetical ordering
                    label_classes = sorted([str(v) for v in y.unique().tolist()])
        except Exception:
            pass

    # Get session_id and train_size from options
    session_id = options.get('session_id', 42)
    train_size = options.get('train_size', 0.7)

    # Store state for subsequent calls
    experiment_id = str(uuid.uuid4())
    _experiment_state = {
        'experiment_id': experiment_id,
        'task': task,
        'target': target,
        'feature_columns': feature_columns,
        'setup_complete': True,
        'label_classes': label_classes,
        'project_name': None,  # Will be set separately if project is persisted
        'data_shape': (len(df), len(df.columns)),
        'id_column': id_column,
        'df_with_ids': df_with_ids,
        'session_id': session_id,
        'train_size': train_size,
        'loaded_model': None,  # Will be set by load_model_from_bytes if called
        'loaded_model_id': None,
        'loaded_model_algorithm': None,
        'setup_df': df,             # Keep DataFrame for PyCaret re-initialization
        'setup_kwargs': setup_kwargs,  # Keep kwargs for PyCaret re-initialization
    }

    return {
        'experiment_id': experiment_id,
        'task': task,
        'target_column': target,
        'feature_columns': feature_columns,
        'train_size': train_size,
        'fold': options.get('fold', 10),
    }


def pycaret_compare_models(n_select=1, sort=None, include=None, exclude=None, budget_time=None):
    """Compare models and return the best one(s). Requires setup() first.

    Args:
        n_select: Number of top models to return
        sort: Metric to sort by (Accuracy, F1, R2, etc.)
        include: List of algorithm IDs to include
        exclude: List of algorithm IDs to exclude
        budget_time: Max training time in seconds

    Returns:
        dict with model bytes, algorithm, metrics, task, feature_columns
    """
    global _experiment_state

    if not _experiment_state['setup_complete']:
        raise RuntimeError("Must call setup() before compare_models()")

    task = _experiment_state['task']

    if task == 'classification':
        from pycaret.classification import compare_models, pull
    else:
        from pycaret.regression import compare_models, pull

    # Build compare_models kwargs
    # verbose=False prevents tqdm progress bar which causes BrokenPipeError in Postgres
    kwargs = {'n_select': n_select, 'verbose': False}
    if sort:
        kwargs['sort'] = sort
    if include:
        kwargs['include'] = include
    if exclude:
        kwargs['exclude'] = exclude
    if budget_time:
        kwargs['budget_time'] = budget_time / 60.0  # seconds to minutes

    best = compare_models(**kwargs)
    metrics = pull().iloc[0].to_dict()

    return {
        'model': pickle.dumps(best),
        'algorithm': type(best).__name__,
        'metrics': metrics,
        'task': task,
        'target': _experiment_state['target'],
        'feature_columns': _experiment_state['feature_columns'],
        'label_classes': _experiment_state['label_classes'],
    }


def pycaret_create_model(algorithm, hyperparams=None):
    """Train a specific algorithm. Requires setup() first.

    Args:
        algorithm: Algorithm ID (rf, xgboost, lightgbm, lr, etc.)
        hyperparams: Optional dict of hyperparameters

    Returns:
        dict with model bytes, algorithm, metrics, task, feature_columns
    """
    global _experiment_state

    if not _experiment_state['setup_complete']:
        raise RuntimeError("Must call setup() before create_model()")

    task = _experiment_state['task']

    if task == 'classification':
        from pycaret.classification import create_model, pull
    else:
        from pycaret.regression import create_model, pull

    # verbose=False prevents tqdm progress bar which causes BrokenPipeError in Postgres
    if hyperparams:
        model = create_model(algorithm, verbose=False, **hyperparams)
    else:
        model = create_model(algorithm, verbose=False)

    metrics = pull().iloc[0].to_dict()

    return {
        'model': pickle.dumps(model),
        'algorithm': algorithm,
        'metrics': metrics,
        'task': task,
        'target': _experiment_state['target'],
        'feature_columns': _experiment_state['feature_columns'],
        'label_classes': _experiment_state['label_classes'],
    }


# =============================================================================
# Time Series Forecasting Functions
# =============================================================================

def pycaret_setup_timeseries(df, target, index, fh=12, fold=3, fold_strategy='expanding', options=None):
    """Initialize PyCaret time series experiment.

    Args:
        df: pandas DataFrame with time series data
        target: Target column name (the value to forecast)
        index: Time/date index column name
        fh: Forecast horizon (number of steps ahead)
        fold: Number of cross-validation folds
        fold_strategy: 'expanding' or 'sliding'
        options: Dict with additional setup options (session_id, etc.)

    Returns:
        dict with experiment metadata
    """
    global _experiment_state
    import pandas as pd

    if options is None:
        options = {}

    from pycaret.time_series import TSForecastingExperiment

    # Convert Arrow-backed dtypes to standard numpy dtypes
    # PyCaret TS uses np.issubdtype() which doesn't understand ArrowDtype
    import numpy as np
    import pyarrow as pa
    for col in df.columns:
        if hasattr(df[col].dtype, 'pyarrow_dtype'):
            arrow_type = df[col].dtype.pyarrow_dtype
            if pa.types.is_floating(arrow_type):
                df[col] = df[col].to_numpy(dtype=np.float64, na_value=np.nan)
            elif pa.types.is_integer(arrow_type):
                df[col] = df[col].to_numpy(dtype=np.float64, na_value=np.nan)
            else:
                df[col] = df[col].to_numpy(dtype=object, na_value=None).astype(str)

    # Ensure index column is datetime type
    df[index] = pd.to_datetime(df[index])

    # Set index and sort by time
    df = df.set_index(index)
    df = df.sort_index()

    # Create experiment instance
    exp = TSForecastingExperiment()

    setup_kwargs = {
        'data': df,
        'target': target,
        'fh': fh,
        'fold': fold,
        'fold_strategy': fold_strategy,
        'session_id': options.get('session_id', 42),
        'verbose': False,
        'html': False,
    }

    if 'n_jobs' in options:
        setup_kwargs['n_jobs'] = options['n_jobs']

    exp.setup(**setup_kwargs)

    experiment_id = str(uuid.uuid4())

    _experiment_state = {
        'experiment_id': experiment_id,
        'task': 'time_series',
        'target': target,
        'feature_columns': [],
        'setup_complete': True,
        'label_classes': None,
        'project_name': None,
        'data_shape': (len(df), len(df.columns)),
        'id_column': None,
        'df_with_ids': None,
        'session_id': options.get('session_id', 42),
        'train_size': None,
        'loaded_model': None,
        'loaded_model_id': None,
        'loaded_model_algorithm': None,
        'setup_df': df,
        'setup_kwargs': setup_kwargs,
        'ts_experiment': exp,
        'ts_index_column': index,
        'ts_fh': fh,
    }

    return {
        'experiment_id': experiment_id,
        'task': 'time_series',
        'target_column': target,
        'index_column': index,
        'fh': fh,
        'fold': fold,
        'fold_strategy': fold_strategy,
        'feature_columns': [],
    }


def pycaret_create_ts_model(algorithm, hyperparams=None):
    """Train a specific time series algorithm. Requires setup_timeseries() first.

    Args:
        algorithm: Algorithm ID (e.g., 'naive', 'arima', 'auto_arima', 'ets',
                   'exp_smooth', 'theta', 'prophet', 'lightgbm_cds_dt', etc.)
        hyperparams: Optional dict of hyperparameters

    Returns:
        dict with model bytes, algorithm, metrics, task
    """
    global _experiment_state

    if not _experiment_state.get('setup_complete') or _experiment_state.get('task') != 'time_series':
        raise RuntimeError("Must call pycaret_setup_timeseries() first")

    exp = _experiment_state.get('ts_experiment')
    if exp is None:
        raise RuntimeError("No TSForecastingExperiment available")

    if hyperparams:
        model = exp.create_model(algorithm, verbose=False, **hyperparams)
    else:
        model = exp.create_model(algorithm, verbose=False)

    metrics = exp.pull().iloc[0].to_dict()

    return {
        'model': pickle.dumps(model),
        'algorithm': algorithm,
        'metrics': metrics,
        'task': 'time_series',
        'target': _experiment_state['target'],
        'feature_columns': [],
        'label_classes': None,
    }


def pycaret_compare_ts_models(n_select=1, sort=None, include=None, exclude=None, budget_time=None):
    """Compare time series models and return the best. Requires setup_timeseries() first.

    Args:
        n_select: Number of top models to return
        sort: Metric to sort by (MASE, RMSSE, MAE, RMSE, MAPE, SMAPE)
        include: List of algorithm IDs to include
        exclude: List of algorithm IDs to exclude
        budget_time: Max training time in seconds

    Returns:
        dict with model bytes, algorithm, metrics, task
    """
    global _experiment_state

    if not _experiment_state.get('setup_complete') or _experiment_state.get('task') != 'time_series':
        raise RuntimeError("Must call pycaret_setup_timeseries() first")

    exp = _experiment_state.get('ts_experiment')
    if exp is None:
        raise RuntimeError("No TSForecastingExperiment available")

    kwargs = {'n_select': n_select, 'verbose': False}
    if sort:
        kwargs['sort'] = sort
    if include:
        kwargs['include'] = include
    if exclude:
        kwargs['exclude'] = exclude
    if budget_time:
        kwargs['budget_time'] = budget_time / 60.0

    best = exp.compare_models(**kwargs)
    metrics = exp.pull().iloc[0].to_dict()

    return {
        'model': pickle.dumps(best),
        'algorithm': type(best).__name__,
        'metrics': metrics,
        'task': 'time_series',
        'target': _experiment_state['target'],
        'feature_columns': [],
        'label_classes': None,
    }


def pycaret_forecast(model_bytes, fh=None, return_pred_int=False, alpha=0.05):
    """Generate forecasts from a trained time series model.

    Unlike classification/regression predict, this forecasts future values
    based on the time series the model was trained on.

    Args:
        model_bytes: Pickled model bytes
        fh: Forecast horizon (defaults to experiment's fh)
        return_pred_int: If True, include prediction intervals
        alpha: Significance level for prediction intervals

    Returns:
        list of dicts with {step, timestamp, prediction, lower, upper}
    """
    global _experiment_state

    exp = _experiment_state.get('ts_experiment')
    if exp is None:
        raise RuntimeError("No TSForecastingExperiment available. Call setup_timeseries() first.")

    model = pickle.loads(model_bytes)

    if fh is None:
        fh = _experiment_state.get('ts_fh', 12)

    # PyCaret TS predict_model returns a DataFrame indexed by time
    pred_kwargs = {'estimator': model, 'fh': fh}
    if return_pred_int:
        pred_kwargs['return_pred_int'] = True
        pred_kwargs['alpha'] = alpha

    predictions = exp.predict_model(**pred_kwargs)

    results = []
    for step, (idx, row) in enumerate(predictions.iterrows(), 1):
        entry = {
            'step': step,
            'timestamp': str(idx),
            'prediction': float(row.iloc[0]) if len(row) > 0 else 0.0,
            'lower': None,
            'upper': None,
        }
        if return_pred_int and len(row) >= 3:
            entry['lower'] = float(row.iloc[1])
            entry['upper'] = float(row.iloc[2])
        results.append(entry)

    return results


def predict(model_bytes, df, label_classes=None):
    """Predict using pickled model.

    Args:
        model_bytes: Pickled model bytes
        df: Input DataFrame with features
        label_classes: Optional list of class labels for decoding (classification only).
                      If provided, numeric predictions are mapped back to original labels.

    Returns:
        List of predictions. For classification with label_classes, returns original
        class names (e.g., ['setosa', 'versicolor']). Otherwise returns numeric values.
    """
    model = pickle.loads(model_bytes)
    predictions = model.predict(df).tolist()

    # Decode numeric class labels to original text labels if mapping provided
    if label_classes is not None and len(label_classes) > 0:
        decoded = []
        for pred in predictions:
            try:
                idx = int(pred)
                if 0 <= idx < len(label_classes):
                    decoded.append(str(label_classes[idx]))
                else:
                    decoded.append(str(pred))
            except (ValueError, TypeError):
                # Already a string or non-numeric, keep as-is
                decoded.append(str(pred))
        return decoded

    return predictions


def predict_proba(model_bytes, df):
    """Get class probabilities (classification only)."""
    model = pickle.loads(model_bytes)
    if not hasattr(model, 'predict_proba'):
        raise ValueError("Model does not support probability predictions")
    return model.predict_proba(df).tolist()


# =============================================================================
# Conformal Prediction with MAPIE
# =============================================================================

def pycaret_create_model_conformal(algorithm, hyperparams=None, conformal_method='plus', conformal_cv=5):
    """Train a model with MAPIE conformal prediction wrapper.

    This trains the base model via PyCaret, then wraps it with MAPIE for
    calibrated prediction intervals.

    Args:
        algorithm: Algorithm ID (rf, xgboost, lightgbm, lr, etc.)
        hyperparams: Optional dict of hyperparameters
        conformal_method: MAPIE method ('naive', 'base', 'plus', 'minmax')
        conformal_cv: Cross-validation folds for MAPIE calibration

    Returns:
        dict with model bytes (MAPIE wrapped), algorithm, metrics, task, feature_columns
    """
    global _experiment_state

    if not _experiment_state['setup_complete']:
        raise RuntimeError("Must call setup() before create_model()")

    task = _experiment_state['task']

    # First train the base model with PyCaret
    if task == 'classification':
        from pycaret.classification import create_model, pull, get_config
    else:
        from pycaret.regression import create_model, pull, get_config

    # verbose=False prevents tqdm progress bar
    if hyperparams:
        base_model = create_model(algorithm, verbose=False, **hyperparams)
    else:
        base_model = create_model(algorithm, verbose=False)

    metrics = pull().iloc[0].to_dict()

    # Now wrap with MAPIE for conformal prediction
    # Get the training data from PyCaret's experiment state
    X_train = get_config('X_train')
    y_train = get_config('y_train')

    if task == 'regression':
        # MAPIE 1.2.0+ uses SplitConformalRegressor (MapieRegressor was deprecated)
        try:
            from mapie.regression import SplitConformalRegressor
            # SplitConformalRegressor with prefit=True needs conformalize() instead of fit()
            mapie_model = SplitConformalRegressor(base_model, prefit=True)
            # conformalize uses calibration data to compute conformity scores
            mapie_model.conformalize(X_train, y_train)
        except ImportError:
            # Fallback for older MAPIE versions
            from mapie.regression import MapieRegressor
            mapie_model = MapieRegressor(base_model, method=conformal_method, cv=conformal_cv)
            mapie_model.fit(X_train, y_train)
    else:
        # MAPIE 1.2.0+ uses SplitConformalClassifier
        try:
            from mapie.classification import SplitConformalClassifier
            mapie_model = SplitConformalClassifier(base_model, prefit=True)
            mapie_model.conformalize(X_train, y_train)
        except ImportError:
            from mapie.classification import MapieClassifier
            # For classification, 'score' method works with any classifier
            mapie_model = MapieClassifier(base_model, method='score', cv=conformal_cv)
            mapie_model.fit(X_train, y_train)

    return {
        'model': pickle.dumps(mapie_model),
        'algorithm': algorithm,
        'metrics': metrics,
        'task': task,
        'target': _experiment_state['target'],
        'feature_columns': _experiment_state['feature_columns'],
        'label_classes': _experiment_state['label_classes'],
        'conformal': True,
        'conformal_method': conformal_method,
    }


def predict_interval(model_bytes, df, alpha=0.1):
    """Predict with confidence intervals using MAPIE.

    Args:
        model_bytes: Pickled MAPIE model bytes
        df: Input DataFrame with features
        alpha: Significance level (0.1 = 90% confidence interval)

    Returns:
        dict with:
        - prediction: list of point predictions
        - lower: list of lower bounds (if MAPIE model)
        - upper: list of upper bounds (if MAPIE model)
        - alpha: the alpha value used
        - has_intervals: bool indicating if intervals are available
    """
    model = pickle.loads(model_bytes)

    # Check if it's a MAPIE model
    # MAPIE 1.2.0+ uses SplitConformalRegressor with _mapie_regressor attribute
    # Also check for predict_interval method
    is_mapie = hasattr(model, '_mapie_regressor') or hasattr(model, 'predict_interval')

    if is_mapie:
        # MAPIE model - get prediction intervals
        try:
            # MAPIE 1.2.0 API: predict_interval returns (predictions, intervals)
            # where intervals has shape (n_samples, 2, 1) with [lower, upper]
            if hasattr(model, 'predict_interval'):
                y_pred, y_intervals = model.predict_interval(df)

                # y_intervals shape: (n_samples, 2, 1)
                # y_intervals[:, 0, 0] = lower bounds
                # y_intervals[:, 1, 0] = upper bounds
                if len(y_intervals.shape) == 3:
                    lower = y_intervals[:, 0, 0].tolist()
                    upper = y_intervals[:, 1, 0].tolist()
                elif len(y_intervals.shape) == 2:
                    lower = y_intervals[:, 0].tolist()
                    upper = y_intervals[:, 1].tolist()
                else:
                    lower = y_pred.tolist()
                    upper = y_pred.tolist()

                return {
                    'prediction': y_pred.tolist() if hasattr(y_pred, 'tolist') else list(y_pred),
                    'lower': lower,
                    'upper': upper,
                    'alpha': alpha,
                    'has_intervals': True,
                }
            else:
                # Fallback: try old API with alpha parameter
                y_pred, y_intervals = model.predict(df, alpha=alpha)
                if len(y_intervals.shape) == 3:
                    lower = y_intervals[:, 0, 0].tolist()
                    upper = y_intervals[:, 1, 0].tolist()
                else:
                    lower = y_pred.tolist()
                    upper = y_pred.tolist()

                return {
                    'prediction': y_pred.tolist() if hasattr(y_pred, 'tolist') else list(y_pred),
                    'lower': lower,
                    'upper': upper,
                    'alpha': alpha,
                    'has_intervals': True,
                }

        except Exception as e:
            # If intervals fail, return point predictions
            predictions = model.predict(df)
            if isinstance(predictions, tuple):
                predictions = predictions[0]
            return {
                'prediction': predictions.tolist() if hasattr(predictions, 'tolist') else list(predictions),
                'lower': predictions.tolist() if hasattr(predictions, 'tolist') else list(predictions),
                'upper': predictions.tolist() if hasattr(predictions, 'tolist') else list(predictions),
                'alpha': alpha,
                'has_intervals': False,
            }
    else:
        # Regular model - no intervals available
        predictions = model.predict(df)
        if hasattr(predictions, 'tolist'):
            predictions = predictions.tolist()
        return {
            'prediction': predictions,
            'lower': predictions,
            'upper': predictions,
            'alpha': alpha,
            'has_intervals': False,
        }


def predict_dist(model_bytes, df, alpha=0.1, dist_type='triangular'):
    """Predict and return result as pg_prob distribution JSON.

    Converts MAPIE prediction intervals to pg_prob distribution format.

    Args:
        model_bytes: Pickled MAPIE model bytes
        df: Input DataFrame with features
        alpha: Significance level (0.1 = 90% confidence interval)
        dist_type: Distribution type to use ('normal', 'uniform', 'triangular')

    Returns:
        list of distribution JSON objects compatible with pg_prob.dist
    """
    import math

    intervals = predict_interval(model_bytes, df, alpha)
    results = []

    for i, pred in enumerate(intervals['prediction']):
        lower = intervals['lower'][i]
        upper = intervals['upper'][i]

        # Handle edge case where interval is a single point
        if abs(upper - lower) < 1e-10:
            # Return literal distribution
            # pg_prob format: {"t":"literal", "p":{"Literal":{"value":...}}}
            results.append({'t': 'literal', 'p': {'Literal': {'value': float(pred)}}})
            continue

        if dist_type == 'normal':
            # Derive sigma from interval
            # For 90% CI (alpha=0.1), interval is mean +/- 1.645*sigma
            # For 95% CI (alpha=0.05), interval is mean +/- 1.96*sigma
            # General: z = norm.ppf(1 - alpha/2)
            # We approximate: sigma = (upper - lower) / (2 * z)
            z_score = 1.645 if abs(alpha - 0.1) < 0.01 else (1.96 if abs(alpha - 0.05) < 0.01 else 1.645)
            sigma = (upper - lower) / (2 * z_score)
            # pg_prob format: {"t":"normal", "p":{"Normal":{"mu":..., "sigma":...}}}
            results.append({'t': 'normal', 'p': {'Normal': {'mu': float(pred), 'sigma': float(sigma)}}})

        elif dist_type == 'uniform':
            # All values in interval equally likely
            # pg_prob format: {"t":"uniform", "p":{"Uniform":{"min":..., "max":...}}}
            results.append({'t': 'uniform', 'p': {'Uniform': {'min': float(lower), 'max': float(upper)}}})

        elif dist_type == 'triangular':
            # Peak at prediction, taper to bounds
            # pg_prob format: {"t":"triangular", "p":{"Triangular":{"min":..., "mode":..., "max":...}}}
            results.append({'t': 'triangular', 'p': {'Triangular': {'min': float(lower), 'mode': float(pred), 'max': float(upper)}}})

        else:
            raise ValueError(f"Unknown dist_type: {dist_type}. Use 'normal', 'uniform', or 'triangular'")

    return results


def predict_batch(model_bytes, df, id_column, feature_columns, label_classes=None):
    """Predict on entire DataFrame efficiently.

    Args:
        model_bytes: Pickled model bytes
        df: pandas DataFrame with id column and features
        id_column: Name of the ID column to include in results
        feature_columns: List of column names to use as features
        label_classes: Optional list for decoding (classification)

    Returns:
        List of (id, prediction) tuples
    """
    model = pickle.loads(model_bytes)

    # Get IDs as strings (to support any ID type)
    ids = df[id_column].astype(str).tolist()

    # Select only feature columns in correct order
    X = df[feature_columns]
    predictions = model.predict(X).tolist()

    # Decode labels if classification
    if label_classes is not None and len(label_classes) > 0:
        decoded = []
        for pred in predictions:
            try:
                idx = int(pred)
                if 0 <= idx < len(label_classes):
                    decoded.append(str(label_classes[idx]))
                else:
                    decoded.append(str(pred))
            except (ValueError, TypeError):
                decoded.append(str(pred))
        predictions = decoded
    else:
        # Convert all predictions to strings for consistent return type
        predictions = [str(p) for p in predictions]

    # Return list of (id, prediction) tuples
    return list(zip(ids, predictions))


def compute_data_hash(df):
    """Compute a hash of DataFrame content for change detection.

    Uses pandas to_csv with a fixed format for deterministic hashing.

    Args:
        df: pandas DataFrame

    Returns:
        str: SHA-256 hash of DataFrame content
    """
    import hashlib
    # Sort by all columns for deterministic ordering
    # Convert to CSV string for hashing (this is deterministic)
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    return hashlib.sha256(csv_bytes).hexdigest()


def get_experiment_split_data():
    """Get train/test split information after setup() has been called.

    Returns:
        dict with:
        - train_ids: List of row IDs in training set (if id_column was provided)
        - test_ids: List of row IDs in test set (if id_column was provided)
        - data_hash: SHA-256 hash of source data for change detection
        - row_count: Total number of rows in dataset
        - session_id: PyCaret session_id used (for reproducibility)
        - train_size: Train split ratio used

    Returns None if no experiment is active or id_column was not provided.
    """
    global _experiment_state

    if not _experiment_state['setup_complete']:
        return None

    task = _experiment_state.get('task')
    id_column = _experiment_state.get('id_column')
    df_with_ids = _experiment_state.get('df_with_ids')

    # Get train/test indices from PyCaret
    # PyCaret stores the indices in X_train.index and X_test.index (DataFrame indices)
    if task == 'classification':
        from pycaret.classification import get_config
    else:
        from pycaret.regression import get_config

    try:
        X_train = get_config('X_train')
        X_test = get_config('X_test')
        train_idx = X_train.index.tolist() if X_train is not None else None
        test_idx = X_test.index.tolist() if X_test is not None else None
    except Exception:
        train_idx = None
        test_idx = None

    # Map indices to row IDs if we have the ID column
    train_ids = None
    test_ids = None

    if id_column and df_with_ids is not None and train_idx is not None and test_idx is not None:
        # Convert numpy arrays to Python lists for serialization
        train_idx_list = list(train_idx) if train_idx is not None else []
        test_idx_list = list(test_idx) if test_idx is not None else []

        # Get IDs from the preserved DataFrame using the indices
        try:
            train_ids = df_with_ids.iloc[train_idx_list][id_column].astype(str).tolist()
            test_ids = df_with_ids.iloc[test_idx_list][id_column].astype(str).tolist()
        except Exception:
            train_ids = None
            test_ids = None

    # Compute data hash from original data (with IDs)
    data_hash = None
    row_count = 0
    if df_with_ids is not None:
        data_hash = compute_data_hash(df_with_ids)
        row_count = len(df_with_ids)
    elif _experiment_state.get('data_shape'):
        row_count = _experiment_state['data_shape'][0]

    return {
        'id_column': id_column,
        'train_ids': train_ids,
        'test_ids': test_ids,
        'data_hash': data_hash,
        'row_count': row_count,
        'session_id': _experiment_state.get('session_id'),
        'train_size': _experiment_state.get('train_size'),
    }


def verify_data_unchanged(df, expected_hash, id_column=None):
    """Verify that data has not changed since training.

    Args:
        df: Current DataFrame
        expected_hash: Hash from training time
        id_column: Optional ID column to exclude from hash (for consistent comparison)

    Returns:
        dict with:
        - unchanged: bool indicating if data matches
        - current_hash: current data hash
        - expected_hash: expected hash from training
        - message: human-readable status message
    """
    # If id_column provided, include it in the hash (should match training)
    current_hash = compute_data_hash(df)

    unchanged = current_hash == expected_hash
    if unchanged:
        message = "Data unchanged since training"
    else:
        message = "WARNING: Data has changed since training. Predictions may not be reproducible."

    return {
        'unchanged': unchanged,
        'current_hash': current_hash,
        'expected_hash': expected_hash,
        'message': message,
    }


def get_experiment_state():
    """Return current experiment state for debugging."""
    return _experiment_state.copy()


def set_project_name(project_name):
    """Set the project name in experiment state (called after setup with project_name)."""
    global _experiment_state
    if _experiment_state['setup_complete']:
        _experiment_state['project_name'] = project_name


def load_model_from_bytes(model_bytes, model_id, algorithm):
    """Deserialize a pickled model and store in experiment state.

    Called by load_experiment after setup to restore the deployed model.
    The model becomes available as _experiment_state['loaded_model'] and
    is used as default by show_plot/show_interpretation/show_model_plots.

    Args:
        model_bytes: Pickled model bytes from pgml.models.artifact
        model_id: The model ID from pgml.models table
        algorithm: The algorithm name (e.g. 'RandomForestClassifier')
    """
    global _experiment_state
    model = pickle.loads(model_bytes)
    _experiment_state['loaded_model'] = model
    _experiment_state['loaded_model_id'] = model_id
    _experiment_state['loaded_model_algorithm'] = algorithm


def get_loaded_model():
    """Return the currently loaded model, or None."""
    return _experiment_state.get('loaded_model')


def get_current_experiment():
    """Return current experiment metadata or None if no experiment is active.

    Returns:
        dict with project info if experiment is active, None otherwise
    """
    global _experiment_state
    if not _experiment_state['setup_complete']:
        return None

    return {
        'project_name': _experiment_state.get('project_name'),
        'task': _experiment_state.get('task'),
        'target_column': _experiment_state.get('target'),
        'feature_columns': _experiment_state.get('feature_columns'),
        'data_rows': _experiment_state.get('data_shape', (None, None))[0],
        'data_cols': _experiment_state.get('data_shape', (None, None))[1],
    }


# =============================================================================
# Plot Helper Functions
# =============================================================================

def _display_figure(fig=None):
    """Display a matplotlib figure via the kernel without saving to disk.

    Args:
        fig: matplotlib figure object. If None, uses current figure.
    """
    import base64
    import io as io_module
    import matplotlib.pyplot as plt

    if fig is None:
        fig = plt.gcf()

    if not fig.get_axes():
        return  # Empty figure

    # Save to memory buffer
    buf = io_module.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    img_data = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()

    # Send via kernel if available
    global _kernel_instance
    if _kernel_instance and _kernel_instance._current_header:
        content = {
            'data': {'image/png': img_data, 'text/plain': '<Plot>'},
            'metadata': {},
        }
        _kernel_instance._send_message(
            _kernel_instance.iopub,
            'display_data',
            content,
            _kernel_instance._current_header
        )
    else:
        # Fallback: print marker for client-side handling
        print(f"IMAGE_PNG_BASE64:{img_data}")

    plt.close(fig)


def _ensure_pycaret_experiment():
    """Ensure PyCaret's _CURRENT_EXPERIMENT is set.

    PyCaret's internal experiment state can be lost even within the same Python
    interpreter. This function detects that and re-runs PyCaret's setup() using
    the saved DataFrame and kwargs from _experiment_state.
    """
    task = _experiment_state.get('task', 'classification')

    # Time series uses a stored experiment object
    if task == 'time_series':
        exp = _experiment_state.get('ts_experiment')
        if exp is not None:
            return  # Experiment is active
        # Re-create from saved state
        setup_df = _experiment_state.get('setup_df')
        setup_kwargs = _experiment_state.get('setup_kwargs')
        if setup_df is None or setup_kwargs is None:
            raise RuntimeError("Time series experiment lost. Call setup_timeseries() again.")
        from pycaret.time_series import TSForecastingExperiment
        exp = TSForecastingExperiment()
        exp.setup(**setup_kwargs)
        _experiment_state['ts_experiment'] = exp
        return

    # Check if PyCaret's experiment is active
    try:
        if task == 'classification':
            from pycaret.classification import get_config
        else:
            from pycaret.regression import get_config
        get_config('X_train')
        return  # Experiment is active, nothing to do
    except Exception:
        pass

    # Experiment not active - try to re-create it from saved state
    if not _experiment_state.get('setup_complete'):
        raise RuntimeError(
            "No PyCaret experiment active. Call setup() or load_experiment() first."
        )

    setup_df = _experiment_state.get('setup_df')
    setup_kwargs = _experiment_state.get('setup_kwargs')

    if setup_df is None or setup_kwargs is None:
        raise RuntimeError(
            "PyCaret experiment lost and no saved DataFrame to re-initialize. "
            "Call load_experiment() or setup() again."
        )

    # Re-run PyCaret's setup() with the saved data
    print("Re-initializing PyCaret experiment...")

    if task == 'classification':
        from pycaret.classification import setup
    else:
        from pycaret.regression import setup

    setup(**setup_kwargs)
    print("PyCaret experiment restored.")


def show_plot(estimator=None, plot='auc', **kwargs):
    """Display a PyCaret plot_model visualization.

    Wrapper around pycaret's plot_model that handles display automatically.
    No files are written to disk.

    Args:
        estimator: Trained model object. If None, uses the model loaded by load_experiment.
            If a string is passed as first arg, it is treated as the plot type instead.
        plot: Plot type - 'auc', 'pr', 'confusion_matrix', 'error',
              'class_report', 'boundary', 'rfe', 'learning', 'manifold',
              'calibration', 'vc', 'dimension', 'feature', 'parameter', etc.
        **kwargs: Additional arguments passed to plot_model

    Example:
        rf = create_model('rf')
        show_plot(rf, 'confusion_matrix')
        show_plot(rf, 'feature')
        # After load_experiment:
        show_plot('confusion_matrix')  # uses loaded model
        show_plot(plot='auc')          # also works with keyword
    """
    # If first arg is a string, treat it as plot type (not estimator)
    if isinstance(estimator, str):
        plot = estimator
        estimator = None

    if estimator is None:
        estimator = _experiment_state.get('loaded_model')
        if estimator is None:
            raise RuntimeError("No model available. Train one with create_model() or load one with load_experiment().")

    # Ensure PyCaret's internal experiment state is active
    _ensure_pycaret_experiment()

    import matplotlib.pyplot as plt

    task = _experiment_state.get('task', 'classification')

    if task == 'classification':
        from pycaret.classification import plot_model
    else:
        from pycaret.regression import plot_model

    # Call plot_model without save - it creates a figure
    # Set display=False if supported to prevent IPython display attempts
    plot_model(estimator, plot=plot, save=False, **kwargs)

    # Capture any figures that were created
    for fig_num in plt.get_fignums():
        fig = plt.figure(fig_num)
        _display_figure(fig)


def show_interpretation(estimator=None, plot='summary', use_javascript=True, **kwargs):
    """Display a SHAP interpretation plot.

    Wrapper around pycaret's interpret_model that handles display automatically.
    Requires tree-based models (rf, xgboost, lightgbm, catboost, etc.)
    No files are written to disk.

    Args:
        estimator: Trained tree-based model object. If None, uses the model loaded by load_experiment.
            If a string is passed as first arg, it is treated as the plot type instead.
        plot: Plot type - 'summary', 'correlation', 'reason', 'pdp', 'msa', 'pfi'
        use_javascript: If True, use interactive JavaScript plots for supported types
                       (force plots, waterfall). Falls back to matplotlib for others.
        **kwargs: Additional arguments passed to interpret_model

    Example:
        rf = create_model('rf')
        show_interpretation(rf, 'summary')
        show_interpretation(rf, 'reason', observation=0)
        # After load_experiment:
        show_interpretation('summary')     # uses loaded model
        show_interpretation(plot='summary') # also works with keyword
    """
    # If first arg is a string, treat it as plot type (not estimator)
    if isinstance(estimator, str):
        plot = estimator
        estimator = None

    if estimator is None:
        estimator = _experiment_state.get('loaded_model')
        if estimator is None:
            raise RuntimeError("No model available. Train one with create_model() or load one with load_experiment().")

    # Ensure PyCaret's internal experiment state is active
    _ensure_pycaret_experiment()

    import matplotlib.pyplot as plt

    task = _experiment_state.get('task', 'classification')

    # For 'reason' (force) plots, try to use SHAP's native JavaScript output
    if use_javascript and plot == 'reason':
        try:
            import shap
            if task == 'classification':
                from pycaret.classification import get_config
            else:
                from pycaret.regression import get_config

            X_test = get_config('X_test')
            observation = kwargs.get('observation', 0)

            # Create SHAP explainer and get explanation object (new API in v0.20+)
            explainer = shap.TreeExplainer(estimator)
            explanation = explainer(X_test)

            # For multi-class, select the class
            if len(explanation.shape) == 3:  # Multi-class: (samples, features, classes)
                class_idx = kwargs.get('class_idx', 1)  # Default to positive class
                explanation = explanation[:, :, class_idx]

            # Get single observation explanation
            obs_explanation = explanation[observation]

            # Generate interactive JavaScript force plot using new API
            force_plot = shap.plots.force(obs_explanation, show=False, matplotlib=False)

            # Get HTML representation using save_html to string
            import io as io_module
            html_buffer = io_module.StringIO()
            shap.save_html(html_buffer, force_plot)
            html_content = html_buffer.getvalue()

            # Send via kernel
            global _kernel_instance
            if _kernel_instance and _kernel_instance._current_header:
                content = {
                    'data': {'text/html': html_content, 'text/plain': '<SHAP Force Plot>'},
                    'metadata': {},
                }
                _kernel_instance._send_message(
                    _kernel_instance.iopub,
                    'display_data',
                    content,
                    _kernel_instance._current_header
                )
            else:
                # Fallback: print HTML marker for client-side handling
                print(f"SHAP_HTML:{html_content}")
            return
        except Exception as e:
            # Print error for debugging, then fall back to matplotlib
            print(f"SHAP JS error: {e}, falling back to matplotlib")

    # For 'summary' plots, use SHAP's beeswarm plot (matplotlib only - no JS version exists)
    if plot == 'summary':
        try:
            import shap
            if task == 'classification':
                from pycaret.classification import get_config
            else:
                from pycaret.regression import get_config

            X_test = get_config('X_test')

            # Create SHAP explainer and values
            explainer = shap.TreeExplainer(estimator)
            shap_values = explainer(X_test)

            # For multi-class, select class
            if len(shap_values.shape) == 3:
                class_idx = kwargs.get('class_idx', 1)
                shap_values = shap_values[:, :, class_idx]

            # Create summary plot (beeswarm) - this is matplotlib only
            # Don't create figure beforehand - summary_plot creates its own
            shap.summary_plot(shap_values, X_test, show=False)
            # Get the figure that summary_plot created
            fig = plt.gcf()
            fig.set_size_inches(10, 8)
            _display_figure(fig)
            return
        except Exception as e:
            # Print error for debugging, then fall back to pycaret's interpret_model
            print(f"SHAP summary error: {e}, falling back to PyCaret")

    # For 'bar' plots, use SHAP's bar plot (also matplotlib)
    if plot == 'bar':
        try:
            import shap
            if task == 'classification':
                from pycaret.classification import get_config
            else:
                from pycaret.regression import get_config

            X_test = get_config('X_test')

            # Create SHAP explainer and values
            explainer = shap.TreeExplainer(estimator)
            shap_values = explainer(X_test)

            # For multi-class, select class
            if len(shap_values.shape) == 3:
                class_idx = kwargs.get('class_idx', 1)
                shap_values = shap_values[:, :, class_idx]

            # Create bar plot
            # Don't create figure beforehand - bar plot creates its own
            shap.plots.bar(shap_values, show=False)
            # Get the figure that bar plot created
            fig = plt.gcf()
            fig.set_size_inches(10, 8)
            _display_figure(fig)
            return
        except Exception as e:
            print(f"SHAP bar error: {e}, falling back to PyCaret")

    # Default: use PyCaret's interpret_model with matplotlib capture
    if task == 'classification':
        from pycaret.classification import interpret_model
    else:
        from pycaret.regression import interpret_model

    # Call interpret_model without save
    interpret_model(estimator, plot=plot, save=False, **kwargs)

    # Capture any figures that were created
    for fig_num in plt.get_fignums():
        fig = plt.figure(fig_num)
        _display_figure(fig)


def show_model_plots(estimator=None, plots=None):
    """Display multiple plots for a model at once.

    Args:
        estimator: Trained model object. If None, uses the model loaded by load_experiment.
        plots: List of plot types, or None for defaults

    Example:
        rf = create_model('rf')
        show_model_plots(rf)  # Shows default plots
        show_model_plots(rf, ['confusion_matrix', 'auc', 'feature'])
        # After load_experiment:
        show_model_plots()  # uses loaded model with default plots
    """
    if estimator is None:
        estimator = _experiment_state.get('loaded_model')
        if estimator is None:
            raise RuntimeError("No model available. Train one with create_model() or load one with load_experiment().")
    if plots is None:
        task = _experiment_state.get('task', 'classification')
        if task == 'classification':
            plots = ['confusion_matrix', 'auc', 'feature']
        else:
            plots = ['residuals', 'error', 'feature']

    for plot_type in plots:
        try:
            print(f"=== {plot_type.upper()} ===")
            show_plot(estimator, plot=plot_type)
        except Exception as e:
            print(f"Could not generate {plot_type}: {e}")


# =============================================================================
# Jupyter Kernel Implementation (ZMQ-based)
# =============================================================================

import threading
import io
import traceback as tb_module
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timezone

_kernel_instance = None
_kernel_lock = threading.Lock()


class PgMlKernel:
    """Jupyter-compatible kernel running in a background thread.

    Shares the same Python namespace as pg_ml SQL functions, so variables
    created via Jupyter are accessible from SQL and vice versa.
    """

    def __init__(self):
        import zmq

        self.ctx = zmq.Context()
        self.execution_count = 0
        self.running = False
        self.thread = None

        # Create sockets with OS-assigned ports
        self.shell = self.ctx.socket(zmq.ROUTER)
        self.shell_port = self.shell.bind_to_random_port('tcp://127.0.0.1')

        self.iopub = self.ctx.socket(zmq.PUB)
        self.iopub_port = self.iopub.bind_to_random_port('tcp://127.0.0.1')

        self.stdin = self.ctx.socket(zmq.ROUTER)
        self.stdin_port = self.stdin.bind_to_random_port('tcp://127.0.0.1')

        self.control = self.ctx.socket(zmq.ROUTER)
        self.control_port = self.control.bind_to_random_port('tcp://127.0.0.1')

        self.hb = self.ctx.socket(zmq.REP)
        self.hb_port = self.hb.bind_to_random_port('tcp://127.0.0.1')

        # Shared namespace with pg_ml - use the module's globals
        self.namespace = globals()

        # Also make common imports available
        self.namespace['pd'] = __import__('pandas')
        self.namespace['np'] = __import__('numpy')

        # Pending figures queue for capturing plots
        self._pending_figures = []
        self._current_header = None

        # Patch plt.show() and IPython.display to capture figures
        self._patch_plt_show()
        self._patch_ipython_display()

    def start(self):
        """Start the kernel background thread."""
        if self.running:
            return self.get_connection_info()

        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        return self.get_connection_info()

    def stop(self):
        """Stop the kernel and cleanup."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)

        # Close sockets
        for sock in [self.shell, self.iopub, self.stdin, self.control, self.hb]:
            try:
                sock.close(linger=0)
            except:
                pass

        try:
            self.ctx.term()
        except:
            pass

    def get_connection_info(self):
        """Return connection info for clients."""
        return {
            'shell': self.shell_port,
            'iopub': self.iopub_port,
            'stdin': self.stdin_port,
            'control': self.control_port,
            'hb': self.hb_port,
        }

    def _run_loop(self):
        """Main event loop running in background thread."""
        import zmq

        poller = zmq.Poller()
        poller.register(self.shell, zmq.POLLIN)
        poller.register(self.control, zmq.POLLIN)
        poller.register(self.hb, zmq.POLLIN)

        while self.running:
            try:
                events = dict(poller.poll(100))  # 100ms timeout
            except zmq.ZMQError:
                break

            # Handle heartbeat
            if self.hb in events:
                try:
                    msg = self.hb.recv()
                    self.hb.send(msg)
                except zmq.ZMQError:
                    pass

            # Handle shell messages
            if self.shell in events:
                self._handle_shell_message()

            # Handle control messages
            if self.control in events:
                self._handle_control_message()

    def _handle_shell_message(self):
        """Handle incoming shell channel message."""
        import json

        try:
            frames = self.shell.recv_multipart()
        except:
            return

        # Parse message - ROUTER socket prepends routing identity
        # Format from client (DEALER): [b'', header, parent, metadata, content]
        # Format after ROUTER: [routing_id, b'', header, parent, metadata, content]
        #
        # Standard Jupyter format with delimiter:
        # [routing_id, ..., '<IDS|MSG>', hmac, header, parent, metadata, content]
        try:
            # Try standard format with <IDS|MSG> delimiter
            delim_idx = frames.index(b'<IDS|MSG>')
            identities = frames[:delim_idx]
            # Skip signature at delim_idx + 1
            header = json.loads(frames[delim_idx + 2])
            parent = json.loads(frames[delim_idx + 3])
            metadata = json.loads(frames[delim_idx + 4])
            content = json.loads(frames[delim_idx + 5])
        except ValueError:
            # No <IDS|MSG> delimiter - try simplified format
            # ROUTER prepends routing identity, then client's message follows
            # Client sends: [b'', header, parent, metadata, content]
            # We receive: [routing_id, b'', header, parent, metadata, content]
            try:
                if len(frames) >= 6 and frames[1] == b'':
                    # Simplified format with empty delimiter
                    identities = [frames[0]]
                    header = json.loads(frames[2])
                    parent = json.loads(frames[3])
                    metadata = json.loads(frames[4])
                    content = json.loads(frames[5])
                elif len(frames) >= 5:
                    # No empty delimiter, message starts at index 1
                    identities = [frames[0]]
                    header = json.loads(frames[1])
                    parent = json.loads(frames[2])
                    metadata = json.loads(frames[3])
                    content = json.loads(frames[4])
                else:
                    return
            except (IndexError, json.JSONDecodeError):
                return

        msg_type = header.get('msg_type', '')

        if msg_type == 'execute_request':
            self._handle_execute_request(identities, header, content)
        elif msg_type == 'kernel_info_request':
            self._handle_kernel_info_request(identities, header)

    def _handle_execute_request(self, identities, header, content):
        """Execute Python code and send results."""
        import json
        import sys

        code = content.get('code', '')
        self.execution_count += 1

        # Store current header for helper functions to use
        self._current_header = header

        # Send busy status on iopub
        self._send_status('busy', header)

        # Capture stdout/stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        error_info = None
        result_value = None

        try:
            # Compile and execute
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                # Try eval first for expressions
                try:
                    compiled = compile(code, '<input>', 'eval')
                    result_value = eval(compiled, self.namespace)
                except SyntaxError:
                    # Fall back to exec for statements
                    compiled = compile(code, '<input>', 'exec')
                    exec(compiled, self.namespace)
        except Exception as e:
            error_info = {
                'ename': type(e).__name__,
                'evalue': str(e),
                'traceback': tb_module.format_exc().split('\n'),
            }

        # Send stdout if any
        stdout_text = stdout_capture.getvalue()
        if stdout_text:
            self._send_stream('stdout', stdout_text, header)

        # Send stderr if any
        stderr_text = stderr_capture.getvalue()
        if stderr_text:
            self._send_stream('stderr', stderr_text, header)

        # Send result or error
        if error_info:
            self._send_error(error_info, header)
            reply_content = {
                'status': 'error',
                'execution_count': self.execution_count,
                **error_info,
            }
        else:
            # Send result or check for figures
            if result_value is not None:
                self._send_execute_result(result_value, header)
            # Send any figures captured by plt.show() or IPython.display()
            self._send_pending_figures(header)
            # Also check for any remaining matplotlib figures
            self._check_and_send_figures(header)
            reply_content = {
                'status': 'ok',
                'execution_count': self.execution_count,
                'user_expressions': {},
                'payload': [],
            }

        # Send idle status
        self._send_status('idle', header)

        # Clear current header
        self._current_header = None

        # Send execute_reply
        self._send_message(self.shell, 'execute_reply', reply_content, header, identities)

    def _handle_kernel_info_request(self, identities, header):
        """Handle kernel_info_request."""
        import sys

        content = {
            'protocol_version': '5.3',
            'implementation': 'pg_ml',
            'implementation_version': '0.1.0',
            'language_info': {
                'name': 'python',
                'version': f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}',
                'mimetype': 'text/x-python',
                'file_extension': '.py',
            },
            'banner': 'pg_ml Jupyter Kernel - PostgreSQL ML Extension',
            'status': 'ok',
        }
        self._send_message(self.shell, 'kernel_info_reply', content, header, identities)

    def _handle_control_message(self):
        """Handle control channel messages."""
        import json

        try:
            frames = self.control.recv_multipart()
            delim_idx = frames.index(b'<IDS|MSG>')
            identities = frames[:delim_idx]
            header = json.loads(frames[delim_idx + 2])
            content = json.loads(frames[delim_idx + 5])
        except:
            return

        msg_type = header.get('msg_type', '')

        if msg_type == 'shutdown_request':
            self.running = False
            reply = {'status': 'ok', 'restart': content.get('restart', False)}
            self._send_message(self.control, 'shutdown_reply', reply, header, identities)

    def _send_message(self, socket, msg_type, content, parent_header, identities=None):
        """Send a Jupyter protocol message."""
        import json

        header = {
            'msg_id': str(uuid.uuid4()),
            'session': 'pg_ml_kernel',
            'username': 'pg_ml',
            'date': datetime.now(timezone.utc).isoformat(),
            'msg_type': msg_type,
            'version': '5.3',
        }

        parent = parent_header if parent_header else {}
        metadata = {}

        frames = []
        if identities:
            frames.extend(identities)
        frames.append(b'<IDS|MSG>')
        frames.append(b'')  # Empty signature (no HMAC for now)
        frames.append(json.dumps(header).encode())
        frames.append(json.dumps(parent).encode())
        frames.append(json.dumps(metadata).encode())
        frames.append(json.dumps(content).encode())

        try:
            socket.send_multipart(frames)
        except:
            pass

    def _send_status(self, status, parent_header):
        """Send status message on iopub."""
        self._send_message(self.iopub, 'status', {'execution_state': status}, parent_header)

    def _send_stream(self, name, text, parent_header):
        """Send stream output on iopub."""
        self._send_message(self.iopub, 'stream', {'name': name, 'text': text}, parent_header)

    def _send_execute_result(self, value, parent_header):
        """Send execute_result on iopub with rich MIME types."""
        import base64
        import io

        data = {}

        # Try to get text representation
        try:
            text_repr = repr(value)
        except:
            text_repr = str(value)
        data['text/plain'] = text_repr

        # Check for matplotlib figure
        try:
            import matplotlib.pyplot as plt
            if hasattr(value, 'savefig') or (hasattr(value, '__class__') and 'Figure' in str(type(value))):
                buf = io.BytesIO()
                value.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                buf.seek(0)
                data['image/png'] = base64.b64encode(buf.read()).decode('utf-8')
                plt.close(value)
        except:
            pass

        # Check for current matplotlib figure (for plot_model which shows via plt)
        try:
            import matplotlib.pyplot as plt
            fig = plt.gcf()
            if fig.get_axes():  # Has content
                buf = io.BytesIO()
                fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                buf.seek(0)
                data['image/png'] = base64.b64encode(buf.read()).decode('utf-8')
                plt.close(fig)
        except:
            pass

        # Check for pandas DataFrame/Series - add HTML representation
        try:
            import pandas as pd
            if isinstance(value, (pd.DataFrame, pd.Series)):
                data['text/html'] = value.to_html()
        except:
            pass

        # Check for plotly figure
        try:
            if hasattr(value, 'to_html') and 'plotly' in str(type(value)).lower():
                data['text/html'] = value.to_html(include_plotlyjs='cdn')
        except:
            pass

        content = {
            'execution_count': self.execution_count,
            'data': data,
            'metadata': {},
        }
        self._send_message(self.iopub, 'execute_result', content, parent_header)

    def _send_error(self, error_info, parent_header):
        """Send error on iopub."""
        self._send_message(self.iopub, 'error', error_info, parent_header)

    def _patch_plt_show(self):
        """Patch matplotlib's plt.show() to capture figures before display."""
        try:
            import matplotlib.pyplot as plt
            import base64
            import io as io_module

            kernel = self
            _original_show = plt.show

            def _patched_show(*args, **kwargs):
                """Capture all figures when show() is called."""
                for fig_num in plt.get_fignums():
                    fig = plt.figure(fig_num)
                    if fig.get_axes():  # Has content
                        try:
                            buf = io_module.BytesIO()
                            fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                            buf.seek(0)
                            img_data = base64.b64encode(buf.read()).decode('utf-8')
                            kernel._pending_figures.append(img_data)
                        except:
                            pass
                # With Agg backend, show() is a no-op, but we call it anyway
                # Don't call original show as it might block or cause issues
                plt.close('all')

            plt.show = _patched_show
        except:
            pass

    def _patch_ipython_display(self):
        """Patch IPython display to capture figures sent via display()."""
        try:
            import IPython.display as ipd
            import base64
            import io as io_module

            kernel = self
            _original_display = ipd.display

            def _patched_display(*objs, **kwargs):
                """Capture figures displayed via IPython.display()."""
                import matplotlib.pyplot as plt

                for obj in objs:
                    # Check if it's a matplotlib figure
                    if hasattr(obj, 'savefig'):
                        try:
                            buf = io_module.BytesIO()
                            obj.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                            buf.seek(0)
                            img_data = base64.b64encode(buf.read()).decode('utf-8')
                            kernel._pending_figures.append(img_data)
                        except:
                            pass
                    # Check for PIL/Pillow images
                    elif hasattr(obj, 'save') and hasattr(obj, 'mode'):
                        try:
                            buf = io_module.BytesIO()
                            obj.save(buf, format='PNG')
                            buf.seek(0)
                            img_data = base64.b64encode(buf.read()).decode('utf-8')
                            kernel._pending_figures.append(img_data)
                        except:
                            pass
                    # Check for IPython Image objects
                    elif hasattr(obj, '_repr_png_'):
                        try:
                            png_data = obj._repr_png_()
                            if png_data:
                                if isinstance(png_data, bytes):
                                    img_data = base64.b64encode(png_data).decode('utf-8')
                                else:
                                    img_data = png_data
                                kernel._pending_figures.append(img_data)
                        except:
                            pass
                    elif hasattr(obj, 'data') and hasattr(obj, 'format'):
                        # IPython.display.Image with data attribute
                        try:
                            if obj.format == 'png' and obj.data:
                                if isinstance(obj.data, bytes):
                                    img_data = base64.b64encode(obj.data).decode('utf-8')
                                else:
                                    img_data = obj.data
                                kernel._pending_figures.append(img_data)
                        except:
                            pass

                # Also check if there are any open matplotlib figures
                try:
                    for fig_num in plt.get_fignums():
                        fig = plt.figure(fig_num)
                        if fig.get_axes():
                            buf = io_module.BytesIO()
                            fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                            buf.seek(0)
                            img_data = base64.b64encode(buf.read()).decode('utf-8')
                            if img_data not in kernel._pending_figures:
                                kernel._pending_figures.append(img_data)
                    plt.close('all')
                except:
                    pass

            ipd.display = _patched_display
        except ImportError:
            # IPython not available
            pass
        except:
            pass

    def _send_pending_figures(self, parent_header):
        """Send any figures captured by plt.show()."""
        for i, img_data in enumerate(self._pending_figures):
            content = {
                'data': {'image/png': img_data, 'text/plain': f'<Figure>'},
                'metadata': {},
            }
            self._send_message(self.iopub, 'display_data', content, parent_header)
        self._pending_figures.clear()

    def _check_and_send_figures(self, parent_header):
        """Check for and display any open matplotlib figures."""
        try:
            import matplotlib.pyplot as plt
            import base64
            import io

            # Get all open figures
            for fig_num in plt.get_fignums():
                fig = plt.figure(fig_num)
                if fig.get_axes():  # Has content
                    buf = io.BytesIO()
                    fig.savefig(buf, format='png', bbox_inches='tight', dpi=100)
                    buf.seek(0)
                    img_data = base64.b64encode(buf.read()).decode('utf-8')

                    content = {
                        'data': {'image/png': img_data, 'text/plain': f'<Figure {fig_num}>'},
                        'metadata': {},
                    }
                    self._send_message(self.iopub, 'display_data', content, parent_header)

            # Close all figures after sending
            plt.close('all')
        except:
            pass


def kernel_start():
    """Start the kernel and return connection info.

    Called from pgml.init() SQL function.
    """
    global _kernel_instance, _kernel_lock

    with _kernel_lock:
        if _kernel_instance is not None:
            # Already running, return existing connection info
            return _kernel_instance.get_connection_info()

        _kernel_instance = PgMlKernel()
        return _kernel_instance.start()


def kernel_stop():
    """Stop the kernel.

    Called from pgml.shutdown() SQL function.
    """
    global _kernel_instance, _kernel_lock

    with _kernel_lock:
        if _kernel_instance is None:
            return False

        _kernel_instance.stop()
        _kernel_instance = None
        return True


def kernel_is_running():
    """Check if kernel is running."""
    global _kernel_instance
    return _kernel_instance is not None and _kernel_instance.running
"#;

// =============================================================================
// Result Types
// =============================================================================

/// Result from setup() operation
#[derive(Debug, Clone)]
pub struct SetupResult {
    pub experiment_id: String,
    pub task: String,
    pub target_column: String,
    pub feature_columns: Vec<String>,
    pub train_size: f64,
    pub fold: i32,
}

/// Result from compare_models() or create_model() operations
#[derive(Debug)]
pub struct TrainResult {
    pub model_bytes: Vec<u8>,
    pub algorithm: String,
    pub metrics: serde_json::Value,
    pub task: String,
    pub target_column: String,
    pub feature_columns: Vec<String>,
    /// For classification: original class labels in the order they were encoded (0, 1, 2, ...)
    pub label_classes: Option<Vec<String>>,
}

/// Result from get_experiment_split_data() for reproducibility tracking
#[derive(Debug, Clone)]
pub struct ExperimentSplitData {
    pub id_column: Option<String>,
    pub train_ids: Option<Vec<String>>,
    pub test_ids: Option<Vec<String>>,
    pub data_hash: Option<String>,
    pub row_count: i32,
    pub session_id: Option<i32>,
    pub train_size: Option<f64>,
}

/// Result from time series setup_timeseries() operation
#[derive(Debug, Clone)]
pub struct TsSetupResult {
    pub experiment_id: String,
    pub target_column: String,
    pub index_column: String,
    pub fh: i32,
    pub fold: i32,
    pub fold_strategy: String,
}

/// A single forecast step from time series prediction
#[derive(Debug, Clone)]
pub struct ForecastRow {
    pub step: i32,
    pub timestamp: String,
    pub prediction: f64,
    pub lower: Option<f64>,
    pub upper: Option<f64>,
}

// =============================================================================
// Python Module Management
// =============================================================================

const MODULE_NAME: &str = "pgml_pycaret";

/// Get or create the embedded PyCaret wrapper Python module.
/// The module is cached in sys.modules to preserve state across calls.
fn get_pycaret_module(py: Python<'_>) -> Result<Bound<'_, pyo3::types::PyModule>, PgMlError> {
    let sys = py.import("sys").map_err(PgMlError::from)?;
    let modules = sys.getattr("modules").map_err(PgMlError::from)?;

    // Check if module already exists in sys.modules
    if let Ok(existing) = modules.get_item(MODULE_NAME) {
        if !existing.is_none() {
            return existing
                .downcast_into::<pyo3::types::PyModule>()
                .map_err(|e| PgMlError::PythonError(format!("Module type error: {}", e)));
        }
    }

    // Create new module and cache it
    let module = pyo3::types::PyModule::from_code(
        py,
        std::ffi::CString::new(PYCARET_WRAPPER_CODE)
            .map_err(|e| PgMlError::PythonError(format!("Invalid Python code: {}", e)))?
            .as_c_str(),
        c"pgml_pycaret",
        c"pgml_pycaret",
    )
    .map_err(PgMlError::from)?;

    // Store in sys.modules so state is preserved
    modules
        .set_item(MODULE_NAME, &module)
        .map_err(PgMlError::from)?;

    Ok(module)
}

/// Public wrapper for get_pycaret_module for use from lib.rs
pub fn get_pycaret_module_public(
    py: Python<'_>,
) -> Result<Bound<'_, pyo3::types::PyModule>, PgMlError> {
    get_pycaret_module(py)
}

// =============================================================================
// Rust Bridge Functions
// =============================================================================

/// Initialize a PyCaret experiment on a PostgreSQL table
///
/// This function:
/// 1. Converts the table to an Arrow-backed pandas DataFrame (zero-copy)
/// 2. Calls PyCaret's setup() with the provided options
/// 3. Stores experiment state in Python module globals
///
/// Must be called before `run_compare_models()` or `run_create_model()`.
///
/// # Arguments
/// * `id_column` - Optional primary key column for reproducibility tracking.
///   When provided, train/test split row IDs are captured.
pub fn run_setup(
    schema_name: &str,
    table_name: &str,
    target_column: &str,
    task: Option<&str>,
    exclude_columns: Option<&[String]>,
    options: Option<&serde_json::Value>,
    id_column: Option<&str>,
) -> Result<SetupResult, PgMlError> {
    crate::ensure_python()?;

    // Get primary key columns to auto-exclude from features
    // Primary keys should never be used as ML features
    let pk_columns = crate::arrow_convert::get_primary_key_columns(schema_name, table_name)?;

    // Merge user-provided exclude_columns with auto-detected primary keys
    let all_exclude: Vec<String> = match exclude_columns {
        Some(cols) => {
            let mut combined: Vec<String> = cols.to_vec();
            for pk in pk_columns {
                if !combined.contains(&pk) {
                    combined.push(pk);
                }
            }
            combined
        }
        None => pk_columns,
    };

    // Get all columns and filter out excluded ones BEFORE Arrow conversion
    // This prevents errors from types like UUID that pgrx can't extract
    let all_columns = crate::arrow_convert::get_column_names(schema_name, table_name)?;
    let columns_to_load: Vec<String> = all_columns
        .into_iter()
        .filter(|col| !all_exclude.contains(col))
        .collect();
    let col_refs: Vec<&str> = columns_to_load.iter().map(|s| s.as_str()).collect();

    Python::with_gil(|py| {
        // Configure MLflow tracking before setup
        configure_mlflow_tracking(py)?;

        // Convert table to DataFrame via Arrow zero-copy (excluding problematic columns)
        let df = table_to_dataframe_arrow(py, schema_name, table_name, Some(&col_refs), None)?;

        // Get the PyCaret module
        let module = get_pycaret_module(py)?;
        let setup_fn = module.getattr("pycaret_setup").map_err(PgMlError::from)?;

        // Prepare kwargs
        let kwargs = PyDict::new(py);
        kwargs.set_item("df", &df).map_err(PgMlError::from)?;
        kwargs
            .set_item("target", target_column)
            .map_err(PgMlError::from)?;

        if let Some(t) = task {
            kwargs.set_item("task", t).map_err(PgMlError::from)?;
        }

        // Pass combined exclude list (user-provided + auto-detected primary keys)
        if !all_exclude.is_empty() {
            let py_list =
                PyList::new(py, all_exclude.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;
            kwargs
                .set_item("exclude_columns", py_list)
                .map_err(PgMlError::from)?;
        }

        if let Some(opts) = options {
            let opts_dict = json_to_python_dict(py, opts)?;
            kwargs
                .set_item("options", opts_dict)
                .map_err(PgMlError::from)?;
        }

        if let Some(id_col) = id_column {
            kwargs
                .set_item("id_column", id_col)
                .map_err(PgMlError::from)?;
        }

        // Call setup
        let result = setup_fn.call((), Some(&kwargs)).map_err(PgMlError::from)?;

        // Extract results
        let experiment_id: String = result
            .get_item("experiment_id")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let task_str: String = result
            .get_item("task")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let target_col: String = result
            .get_item("target_column")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let feature_columns: Vec<String> = result
            .get_item("feature_columns")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let train_size: f64 = result
            .get_item("train_size")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let fold: i32 = result
            .get_item("fold")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        Ok(SetupResult {
            experiment_id,
            task: task_str,
            target_column: target_col,
            feature_columns,
            train_size,
            fold,
        })
    })
}

/// Compare models and return the best one(s)
///
/// Requires `run_setup()` to be called first.
pub fn run_compare_models(
    n_select: Option<i32>,
    sort: Option<&str>,
    include: Option<&[String]>,
    exclude: Option<&[String]>,
    budget_time: Option<i32>,
) -> Result<TrainResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let compare_fn = module
            .getattr("pycaret_compare_models")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);

        if let Some(n) = n_select {
            kwargs.set_item("n_select", n).map_err(PgMlError::from)?;
        }

        if let Some(s) = sort {
            kwargs.set_item("sort", s).map_err(PgMlError::from)?;
        }

        if let Some(inc) = include {
            let py_list =
                PyList::new(py, inc.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;
            kwargs
                .set_item("include", py_list)
                .map_err(PgMlError::from)?;
        }

        if let Some(exc) = exclude {
            let py_list =
                PyList::new(py, exc.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;
            kwargs
                .set_item("exclude", py_list)
                .map_err(PgMlError::from)?;
        }

        if let Some(time) = budget_time {
            kwargs
                .set_item("budget_time", time)
                .map_err(PgMlError::from)?;
        }

        let result = compare_fn
            .call((), Some(&kwargs))
            .map_err(PgMlError::from)?;

        extract_train_result(py, &result)
    })
}

/// Train a specific algorithm
///
/// Requires `run_setup()` to be called first.
pub fn run_create_model(
    algorithm: &str,
    hyperparams: Option<&serde_json::Value>,
) -> Result<TrainResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let create_fn = module
            .getattr("pycaret_create_model")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);
        kwargs
            .set_item("algorithm", algorithm)
            .map_err(PgMlError::from)?;

        if let Some(hp) = hyperparams {
            let hp_dict = json_to_python_dict(py, hp)?;
            kwargs
                .set_item("hyperparams", hp_dict)
                .map_err(PgMlError::from)?;
        }

        let result = create_fn.call((), Some(&kwargs)).map_err(PgMlError::from)?;

        extract_train_result(py, &result)
    })
}

/// Make predictions using a saved model
#[allow(dead_code)]
pub fn run_predict(
    model_bytes: &[u8],
    schema_name: &str,
    table_name: &str,
    feature_columns: &[String],
) -> Result<Vec<f64>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        // Convert table to DataFrame, selecting only feature columns
        let col_refs: Vec<&str> = feature_columns.iter().map(|s| s.as_str()).collect();
        let df = table_to_dataframe_arrow(py, schema_name, table_name, Some(&col_refs), None)?;

        let module = get_pycaret_module(py)?;
        let predict_fn = module.getattr("predict").map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);
        let result = predict_fn.call1((py_bytes, &df)).map_err(PgMlError::from)?;

        let predictions: Vec<f64> = result.extract().map_err(PgMlError::from)?;
        Ok(predictions)
    })
}

/// Get class probabilities (classification only)
#[allow(dead_code)]
pub fn run_predict_proba(
    model_bytes: &[u8],
    schema_name: &str,
    table_name: &str,
    feature_columns: &[String],
) -> Result<Vec<Vec<f64>>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let col_refs: Vec<&str> = feature_columns.iter().map(|s| s.as_str()).collect();
        let df = table_to_dataframe_arrow(py, schema_name, table_name, Some(&col_refs), None)?;

        let module = get_pycaret_module(py)?;
        let predict_proba_fn = module.getattr("predict_proba").map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);
        let result = predict_proba_fn
            .call1((py_bytes, &df))
            .map_err(PgMlError::from)?;

        let probas: Vec<Vec<f64>> = result.extract().map_err(PgMlError::from)?;
        Ok(probas)
    })
}

/// Make a single prediction from a feature array
///
/// Returns the prediction as a String. For classification with label_classes,
/// this will be the original class name (e.g., "setosa"). For regression or
/// classification without label_classes, this will be the numeric prediction.
pub fn run_predict_single(
    model_bytes: &[u8],
    features: &[f64],
    feature_columns: &[String],
    label_classes: Option<&[String]>,
) -> Result<String, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let predict_fn = module.getattr("predict").map_err(PgMlError::from)?;

        // Create a single-row DataFrame from the features array
        let pd = py.import("pandas").map_err(PgMlError::from)?;
        let data = PyDict::new(py);
        for (i, col) in feature_columns.iter().enumerate() {
            if i < features.len() {
                let py_list = PyList::new(py, [features[i]]).map_err(PgMlError::from)?;
                data.set_item(col, py_list).map_err(PgMlError::from)?;
            }
        }
        let df = pd
            .call_method1("DataFrame", (data,))
            .map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);

        // Pass label_classes to Python for decoding
        let py_label_classes = match label_classes {
            Some(classes) if !classes.is_empty() => {
                PyList::new(py, classes.iter().map(|s| s.as_str()))
                    .map_err(PgMlError::from)?
                    .into_any()
            }
            _ => py.None().into_bound(py),
        };

        let result = predict_fn
            .call1((py_bytes, df, py_label_classes))
            .map_err(PgMlError::from)?;

        // Result is now a list of strings (decoded labels) or numeric values as strings
        // Try to extract as strings first, fall back to floats if needed
        let predictions: Vec<String> = match result.extract::<Vec<String>>() {
            Ok(strs) => strs,
            Err(_) => {
                // Try extracting as floats and convert to strings
                let float_preds: Vec<f64> = result.extract().map_err(|e| {
                    PgMlError::PythonError(format!("Failed to extract predictions: {}", e))
                })?;
                float_preds.iter().map(|f| f.to_string()).collect()
            }
        };

        predictions
            .first()
            .cloned()
            .ok_or_else(|| PgMlError::PythonError("No predictions returned".to_string()))
    })
}

/// Get class probabilities for a single sample
pub fn run_predict_proba_single(
    model_bytes: &[u8],
    features: &[f64],
    feature_columns: &[String],
) -> Result<Vec<f64>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let predict_proba_fn = module.getattr("predict_proba").map_err(PgMlError::from)?;

        let pd = py.import("pandas").map_err(PgMlError::from)?;
        let data = PyDict::new(py);
        for (i, col) in feature_columns.iter().enumerate() {
            if i < features.len() {
                let py_list = PyList::new(py, [features[i]]).map_err(PgMlError::from)?;
                data.set_item(col, py_list).map_err(PgMlError::from)?;
            }
        }
        let df = pd
            .call_method1("DataFrame", (data,))
            .map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);
        let result = predict_proba_fn
            .call1((py_bytes, df))
            .map_err(PgMlError::from)?;

        let probas: Vec<Vec<f64>> = result.extract().map_err(PgMlError::from)?;
        probas
            .first()
            .cloned()
            .ok_or_else(|| PgMlError::PythonError("No probabilities returned".to_string()))
    })
}

/// Make batch predictions on a table
///
/// Returns a vector of (id, prediction) tuples where id is converted to String
/// to support any ID column type.
pub fn run_predict_batch(
    model_bytes: &[u8],
    schema_name: &str,
    table_name: &str,
    id_column: &str,
    feature_columns: &[String],
    label_classes: Option<&[String]>,
) -> Result<Vec<(String, String)>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let predict_batch_fn = module.getattr("predict_batch").map_err(PgMlError::from)?;

        // Convert table to DataFrame via Arrow zero-copy
        // Include id_column plus all feature columns
        let mut all_columns: Vec<&str> = vec![id_column];
        all_columns.extend(feature_columns.iter().map(|s| s.as_str()));

        let df = crate::arrow_convert::table_to_dataframe_arrow(
            py,
            schema_name,
            table_name,
            Some(&all_columns),
            None,
        )?;

        let py_bytes = PyBytes::new(py, model_bytes);

        // Prepare feature columns as Python list
        let py_feature_cols =
            PyList::new(py, feature_columns.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;

        // Prepare label_classes
        let py_label_classes = match label_classes {
            Some(classes) if !classes.is_empty() => {
                PyList::new(py, classes.iter().map(|s| s.as_str()))
                    .map_err(PgMlError::from)?
                    .into_any()
            }
            _ => py.None().into_bound(py),
        };

        // Call Python predict_batch function
        let result = predict_batch_fn
            .call1((py_bytes, df, id_column, py_feature_cols, py_label_classes))
            .map_err(PgMlError::from)?;

        // Extract list of (id, prediction) tuples
        let predictions: Vec<(String, String)> = result.extract().map_err(PgMlError::from)?;
        Ok(predictions)
    })
}

// =============================================================================
// Conformal Prediction Rust Wrappers
// =============================================================================

/// Create a model with MAPIE conformal prediction wrapper
pub fn run_create_model_conformal(
    algorithm: &str,
    hyperparams: Option<&serde_json::Value>,
    conformal_method: &str,
    conformal_cv: i32,
) -> Result<TrainResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let create_fn = module
            .getattr("pycaret_create_model_conformal")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);
        kwargs
            .set_item("algorithm", algorithm)
            .map_err(PgMlError::from)?;
        kwargs
            .set_item("conformal_method", conformal_method)
            .map_err(PgMlError::from)?;
        kwargs
            .set_item("conformal_cv", conformal_cv)
            .map_err(PgMlError::from)?;

        if let Some(hp) = hyperparams {
            let hp_dict = json_to_python_dict(py, hp)?;
            kwargs
                .set_item("hyperparams", hp_dict)
                .map_err(PgMlError::from)?;
        }

        let result = create_fn.call((), Some(&kwargs)).map_err(PgMlError::from)?;

        extract_train_result(py, &result)
    })
}

/// Result from predict_interval() - includes bounds
#[derive(Debug, Clone)]
pub struct PredictIntervalResult {
    pub prediction: f64,
    pub lower: f64,
    pub upper: f64,
    pub alpha: f64,
    pub has_intervals: bool,
}

/// Predict with confidence interval for a single sample
pub fn run_predict_interval_single(
    model_bytes: &[u8],
    features: &[f64],
    feature_columns: &[String],
    alpha: f64,
) -> Result<PredictIntervalResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let predict_fn = module
            .getattr("predict_interval")
            .map_err(PgMlError::from)?;

        // Create a single-row DataFrame from the features array
        let pd = py.import("pandas").map_err(PgMlError::from)?;
        let data = PyDict::new(py);
        for (i, col) in feature_columns.iter().enumerate() {
            if i < features.len() {
                let py_list = PyList::new(py, [features[i]]).map_err(PgMlError::from)?;
                data.set_item(col, py_list).map_err(PgMlError::from)?;
            }
        }
        let df = pd
            .call_method1("DataFrame", (data,))
            .map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);

        let result = predict_fn
            .call1((py_bytes, df, alpha))
            .map_err(PgMlError::from)?;

        // Extract results from dict
        let predictions: Vec<f64> = result
            .get_item("prediction")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;
        let lowers: Vec<f64> = result
            .get_item("lower")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;
        let uppers: Vec<f64> = result
            .get_item("upper")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;
        let has_intervals: bool = result
            .get_item("has_intervals")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        Ok(PredictIntervalResult {
            prediction: predictions.first().copied().unwrap_or(0.0),
            lower: lowers.first().copied().unwrap_or(0.0),
            upper: uppers.first().copied().unwrap_or(0.0),
            alpha,
            has_intervals,
        })
    })
}

/// Predict and return result as pg_prob distribution JSON for a single sample
pub fn run_predict_dist_single(
    model_bytes: &[u8],
    features: &[f64],
    feature_columns: &[String],
    alpha: f64,
    dist_type: &str,
) -> Result<serde_json::Value, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let predict_fn = module.getattr("predict_dist").map_err(PgMlError::from)?;

        // Create a single-row DataFrame from the features array
        let pd = py.import("pandas").map_err(PgMlError::from)?;
        let data = PyDict::new(py);
        for (i, col) in feature_columns.iter().enumerate() {
            if i < features.len() {
                let py_list = PyList::new(py, [features[i]]).map_err(PgMlError::from)?;
                data.set_item(col, py_list).map_err(PgMlError::from)?;
            }
        }
        let df = pd
            .call_method1("DataFrame", (data,))
            .map_err(PgMlError::from)?;

        let py_bytes = PyBytes::new(py, model_bytes);

        let result = predict_fn
            .call1((py_bytes, df, alpha, dist_type))
            .map_err(PgMlError::from)?;

        // Result is a list of distribution dicts, we want the first one
        let json_module = py.import("json").map_err(PgMlError::from)?;
        let json_str: String = json_module
            .call_method1("dumps", (result.get_item(0).map_err(PgMlError::from)?,))
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        serde_json::from_str(&json_str).map_err(|e| {
            PgMlError::PythonError(format!("Failed to parse distribution JSON: {}", e))
        })
    })
}

// =============================================================================
// Time Series Bridge Functions
// =============================================================================

/// Initialize a PyCaret time series experiment on a PostgreSQL table
#[allow(clippy::too_many_arguments)]
pub fn run_setup_timeseries(
    schema_name: &str,
    table_name: &str,
    target_column: &str,
    index_column: &str,
    fh: i32,
    fold: Option<i32>,
    fold_strategy: Option<&str>,
    options: Option<&serde_json::Value>,
) -> Result<TsSetupResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        // Load all columns (time series needs index + target at minimum)
        let df = table_to_dataframe_arrow(py, schema_name, table_name, None, None)?;

        let module = get_pycaret_module(py)?;
        let setup_fn = module
            .getattr("pycaret_setup_timeseries")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);
        kwargs.set_item("df", &df).map_err(PgMlError::from)?;
        kwargs
            .set_item("target", target_column)
            .map_err(PgMlError::from)?;
        kwargs
            .set_item("index", index_column)
            .map_err(PgMlError::from)?;
        kwargs.set_item("fh", fh).map_err(PgMlError::from)?;

        if let Some(f) = fold {
            kwargs.set_item("fold", f).map_err(PgMlError::from)?;
        }
        if let Some(fs) = fold_strategy {
            kwargs
                .set_item("fold_strategy", fs)
                .map_err(PgMlError::from)?;
        }
        if let Some(opts) = options {
            let opts_dict = json_to_python_dict(py, opts)?;
            kwargs
                .set_item("options", opts_dict)
                .map_err(PgMlError::from)?;
        }

        let result = setup_fn.call((), Some(&kwargs)).map_err(PgMlError::from)?;

        let target_col: String = result
            .get_item("target_column")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let index_col: String = result
            .get_item("index_column")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let result_fh: i32 = result
            .get_item("fh")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let result_fold: i32 = result
            .get_item("fold")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let result_fold_strategy: String = result
            .get_item("fold_strategy")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        let experiment_id: String = result
            .get_item("experiment_id")
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        Ok(TsSetupResult {
            experiment_id,
            target_column: target_col,
            index_column: index_col,
            fh: result_fh,
            fold: result_fold,
            fold_strategy: result_fold_strategy,
        })
    })
}

/// Train a specific time series algorithm
///
/// Requires `run_setup_timeseries()` to be called first.
pub fn run_create_ts_model(
    algorithm: &str,
    hyperparams: Option<&serde_json::Value>,
) -> Result<TrainResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let create_fn = module
            .getattr("pycaret_create_ts_model")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);
        kwargs
            .set_item("algorithm", algorithm)
            .map_err(PgMlError::from)?;

        if let Some(hp) = hyperparams {
            let hp_dict = json_to_python_dict(py, hp)?;
            kwargs
                .set_item("hyperparams", hp_dict)
                .map_err(PgMlError::from)?;
        }

        let result = create_fn.call((), Some(&kwargs)).map_err(PgMlError::from)?;

        extract_train_result(py, &result)
    })
}

/// Compare time series models and return the best
///
/// Requires `run_setup_timeseries()` to be called first.
pub fn run_compare_ts_models(
    n_select: Option<i32>,
    sort: Option<&str>,
    include: Option<&[String]>,
    exclude: Option<&[String]>,
    budget_time: Option<i32>,
) -> Result<TrainResult, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let compare_fn = module
            .getattr("pycaret_compare_ts_models")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);

        if let Some(n) = n_select {
            kwargs.set_item("n_select", n).map_err(PgMlError::from)?;
        }
        if let Some(s) = sort {
            kwargs.set_item("sort", s).map_err(PgMlError::from)?;
        }
        if let Some(inc) = include {
            let py_list =
                PyList::new(py, inc.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;
            kwargs
                .set_item("include", py_list)
                .map_err(PgMlError::from)?;
        }
        if let Some(exc) = exclude {
            let py_list =
                PyList::new(py, exc.iter().map(|s| s.as_str())).map_err(PgMlError::from)?;
            kwargs
                .set_item("exclude", py_list)
                .map_err(PgMlError::from)?;
        }
        if let Some(time) = budget_time {
            kwargs
                .set_item("budget_time", time)
                .map_err(PgMlError::from)?;
        }

        let result = compare_fn
            .call((), Some(&kwargs))
            .map_err(PgMlError::from)?;

        extract_train_result(py, &result)
    })
}

/// Generate forecasts from a trained time series model
///
/// Requires `run_setup_timeseries()` to have been called (the experiment context
/// must be active since forecasting relies on the training data).
pub fn run_forecast(
    model_bytes: &[u8],
    fh: Option<i32>,
    return_pred_int: bool,
    alpha: f64,
) -> Result<Vec<ForecastRow>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let forecast_fn = module
            .getattr("pycaret_forecast")
            .map_err(PgMlError::from)?;

        let kwargs = PyDict::new(py);
        let py_bytes = PyBytes::new(py, model_bytes);
        kwargs
            .set_item("model_bytes", py_bytes)
            .map_err(PgMlError::from)?;

        if let Some(h) = fh {
            kwargs.set_item("fh", h).map_err(PgMlError::from)?;
        }
        kwargs
            .set_item("return_pred_int", return_pred_int)
            .map_err(PgMlError::from)?;
        kwargs.set_item("alpha", alpha).map_err(PgMlError::from)?;

        let result = forecast_fn
            .call((), Some(&kwargs))
            .map_err(PgMlError::from)?;

        // Result is a list of dicts
        let py_list: &Bound<'_, PyList> = result.downcast().map_err(|e| {
            PgMlError::PythonError(format!("Expected list from pycaret_forecast: {}", e))
        })?;

        let mut rows = Vec::with_capacity(py_list.len());
        for item in py_list.iter() {
            let step: i32 = item
                .get_item("step")
                .map_err(PgMlError::from)?
                .extract()
                .map_err(PgMlError::from)?;

            let timestamp: String = item
                .get_item("timestamp")
                .map_err(PgMlError::from)?
                .extract()
                .map_err(PgMlError::from)?;

            let prediction: f64 = item
                .get_item("prediction")
                .map_err(PgMlError::from)?
                .extract()
                .map_err(PgMlError::from)?;

            let lower_val = item.get_item("lower").map_err(PgMlError::from)?;
            let lower: Option<f64> = if lower_val.is_none() {
                None
            } else {
                lower_val.extract().ok()
            };

            let upper_val = item.get_item("upper").map_err(PgMlError::from)?;
            let upper: Option<f64> = if upper_val.is_none() {
                None
            } else {
                upper_val.extract().ok()
            };

            rows.push(ForecastRow {
                step,
                timestamp,
                prediction,
                lower,
                upper,
            });
        }

        Ok(rows)
    })
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Extract TrainResult from Python dict
fn extract_train_result(
    py: Python<'_>,
    result: &Bound<'_, pyo3::PyAny>,
) -> Result<TrainResult, PgMlError> {
    let model_bytes: Vec<u8> = result
        .get_item("model")
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    let algorithm: String = result
        .get_item("algorithm")
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    let metrics_dict = result.get_item("metrics").map_err(PgMlError::from)?;
    let metrics_json = python_dict_to_json(py, &metrics_dict)?;

    let task: String = result
        .get_item("task")
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    let target_column: String = result
        .get_item("target")
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    let feature_columns: Vec<String> = result
        .get_item("feature_columns")
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    // Extract label_classes (may be None for regression)
    let label_classes: Option<Vec<String>> =
        result.get_item("label_classes").ok().and_then(|item| {
            if item.is_none() {
                None
            } else {
                // Convert Python list to Vec<String>, handling various types
                item.extract::<Vec<pyo3::PyObject>>().ok().map(|objs| {
                    objs.iter()
                        .map(|obj| {
                            obj.bind(py)
                                .str()
                                .map(|s| s.to_string())
                                .unwrap_or_default()
                        })
                        .collect()
                })
            }
        });

    Ok(TrainResult {
        model_bytes,
        algorithm,
        metrics: metrics_json,
        task,
        target_column,
        feature_columns,
        label_classes,
    })
}

/// Convert Python dict to serde_json::Value
fn python_dict_to_json(
    py: Python<'_>,
    dict: &Bound<'_, pyo3::PyAny>,
) -> Result<serde_json::Value, PgMlError> {
    let json_module = py.import("json").map_err(PgMlError::from)?;

    // Use default=str to handle non-JSON-serializable types like numpy.float64
    let builtins = py.import("builtins").map_err(PgMlError::from)?;
    let str_fn = builtins.getattr("str").map_err(PgMlError::from)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("default", str_fn).ok();

    let json_str: String = json_module
        .call_method("dumps", (dict,), Some(&kwargs))
        .map_err(PgMlError::from)?
        .extract()
        .map_err(PgMlError::from)?;

    serde_json::from_str(&json_str)
        .map_err(|e| PgMlError::JsonError(format!("Failed to parse metrics JSON: {}", e)))
}

/// Convert serde_json::Value to Python dict
fn json_to_python_dict<'py>(
    py: Python<'py>,
    value: &serde_json::Value,
) -> Result<Bound<'py, PyDict>, PgMlError> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| PgMlError::JsonError(format!("Failed to serialize options: {}", e)))?;

    let json_module = py.import("json").map_err(PgMlError::from)?;
    let dict = json_module
        .call_method1("loads", (json_str,))
        .map_err(PgMlError::from)?
        .downcast_into::<PyDict>()
        .map_err(|e| PgMlError::PythonError(format!("Expected dict: {}", e)))?;

    Ok(dict)
}

/// Parse "schema.table" or "table" into (schema, table)
pub fn parse_relation_name(relation_name: &str) -> (String, String) {
    if let Some((schema, table)) = relation_name.split_once('.') {
        (schema.to_string(), table.to_string())
    } else {
        ("public".to_string(), relation_name.to_string())
    }
}

/// Set the project name in Python experiment state
pub fn set_experiment_project_name(project_name: &str) -> Result<(), PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let set_fn = module
            .getattr("set_project_name")
            .map_err(PgMlError::from)?;

        set_fn.call1((project_name,)).map_err(PgMlError::from)?;

        Ok(())
    })
}

/// Load a model from pickle bytes into the Python experiment state.
///
/// After this call, `show_plot()`, `show_interpretation()`, and `show_model_plots()`
/// can be called without an explicit estimator argument.
pub fn load_model_into_session(
    model_bytes: &[u8],
    model_id: i64,
    algorithm: &str,
) -> Result<(), PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let load_fn = module
            .getattr("load_model_from_bytes")
            .map_err(PgMlError::from)?;

        let py_bytes = pyo3::types::PyBytes::new(py, model_bytes);

        load_fn
            .call1((py_bytes, model_id, algorithm))
            .map_err(PgMlError::from)?;

        Ok(())
    })
}

/// Get experiment split data for reproducibility tracking
///
/// Returns train/test row IDs, data hash, and split parameters.
/// Must be called after `run_setup()`.
pub fn get_experiment_split_data() -> Result<Option<ExperimentSplitData>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let get_fn = module
            .getattr("get_experiment_split_data")
            .map_err(PgMlError::from)?;

        let result = get_fn.call0().map_err(PgMlError::from)?;

        // If None, no experiment is active or id_column was not provided
        if result.is_none() {
            return Ok(None);
        }

        // Extract fields from dict
        let id_column: Option<String> = result.get_item("id_column").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let train_ids: Option<Vec<String>> = result.get_item("train_ids").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let test_ids: Option<Vec<String>> = result.get_item("test_ids").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let data_hash: Option<String> = result.get_item("data_hash").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let row_count: i32 = result
            .get_item("row_count")
            .ok()
            .and_then(|v| v.extract().ok())
            .unwrap_or(0);

        let session_id: Option<i32> = result.get_item("session_id").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let train_size: Option<f64> = result.get_item("train_size").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        Ok(Some(ExperimentSplitData {
            id_column,
            train_ids,
            test_ids,
            data_hash,
            row_count,
            session_id,
            train_size,
        }))
    })
}

/// Result from get_current_experiment()
#[derive(Debug, Clone)]
pub struct CurrentExperimentResult {
    pub project_name: Option<String>,
    pub task: Option<String>,
    pub target_column: Option<String>,
    pub feature_columns: Option<Vec<String>>,
    pub data_rows: Option<i64>,
    pub data_cols: Option<i64>,
}

/// Get the current experiment state from Python
pub fn run_get_current_experiment() -> Result<Option<CurrentExperimentResult>, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let get_fn = module
            .getattr("get_current_experiment")
            .map_err(PgMlError::from)?;

        let result = get_fn.call0().map_err(PgMlError::from)?;

        // If None, no experiment is active
        if result.is_none() {
            return Ok(None);
        }

        // Extract fields from dict
        let project_name: Option<String> = result.get_item("project_name").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let task: Option<String> = result.get_item("task").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let target_column: Option<String> = result.get_item("target_column").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let feature_columns: Option<Vec<String>> = result
            .get_item("feature_columns")
            .ok()
            .and_then(|v| if v.is_none() { None } else { v.extract().ok() });

        let data_rows: Option<i64> = result.get_item("data_rows").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        let data_cols: Option<i64> = result.get_item("data_cols").ok().and_then(|v| {
            if v.is_none() {
                None
            } else {
                v.extract().ok()
            }
        });

        Ok(Some(CurrentExperimentResult {
            project_name,
            task,
            target_column,
            feature_columns,
            data_rows,
            data_cols,
        }))
    })
}

// =============================================================================
// Kernel Bridge Functions
// =============================================================================

/// Start the Jupyter kernel and return connection info
pub fn start_kernel() -> Result<serde_json::Value, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let start_fn = module.getattr("kernel_start").map_err(PgMlError::from)?;

        let result = start_fn.call0().map_err(PgMlError::from)?;

        // Convert Python dict to JSON
        let json_module = py.import("json").map_err(PgMlError::from)?;
        let json_str: String = json_module
            .call_method1("dumps", (&result,))
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;

        serde_json::from_str(&json_str).map_err(|e| {
            PgMlError::JsonError(format!("Failed to parse kernel connection info: {}", e))
        })
    })
}

/// Stop the Jupyter kernel
pub fn stop_kernel() -> Result<bool, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let stop_fn = module.getattr("kernel_stop").map_err(PgMlError::from)?;

        let result: bool = stop_fn
            .call0()
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;
        Ok(result)
    })
}

/// Check if the kernel is running
pub fn is_kernel_running() -> Result<bool, PgMlError> {
    crate::ensure_python()?;

    Python::with_gil(|py| {
        let module = get_pycaret_module(py)?;
        let check_fn = module
            .getattr("kernel_is_running")
            .map_err(PgMlError::from)?;

        let result: bool = check_fn
            .call0()
            .map_err(PgMlError::from)?
            .extract()
            .map_err(PgMlError::from)?;
        Ok(result)
    })
}

// =============================================================================
// MLflow Integration
// =============================================================================

/// Configure MLflow tracking in Python based on GUC settings
fn configure_mlflow_tracking(py: Python<'_>) -> Result<(), PgMlError> {
    let module = get_pycaret_module(py)?;
    let configure_fn = module
        .getattr("configure_mlflow")
        .map_err(PgMlError::from)?;

    // Check if MLflow tracking is enabled via GUC
    let enabled = crate::mlflow::is_tracking_enabled();
    let tracking_uri = if enabled {
        Some(crate::mlflow::get_tracking_uri())
    } else {
        None
    };

    // Get experiment name from options or use a default
    let experiment_name = if enabled {
        Some("pg_ml".to_string())
    } else {
        None
    };

    // Call Python configure_mlflow function
    let kwargs = PyDict::new(py);
    kwargs
        .set_item("enabled", enabled)
        .map_err(PgMlError::from)?;
    if let Some(uri) = tracking_uri {
        kwargs
            .set_item("tracking_uri", uri)
            .map_err(PgMlError::from)?;
    }
    if let Some(name) = experiment_name {
        kwargs
            .set_item("experiment_name", name)
            .map_err(PgMlError::from)?;
    }

    configure_fn
        .call((), Some(&kwargs))
        .map_err(PgMlError::from)?;
    Ok(())
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_relation_name_with_schema() {
        let (schema, table) = parse_relation_name("myschema.mytable");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_parse_relation_name_without_schema() {
        let (schema, table) = parse_relation_name("mytable");
        assert_eq!(schema, "public");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_parse_relation_name_empty() {
        let (schema, table) = parse_relation_name("");
        assert_eq!(schema, "public");
        assert_eq!(table, "");
    }

    #[test]
    fn test_parse_relation_name_multiple_dots() {
        // Only splits on first dot
        let (schema, table) = parse_relation_name("catalog.schema.table");
        assert_eq!(schema, "catalog");
        assert_eq!(table, "schema.table");
    }

    #[test]
    fn test_setup_result_construction() {
        let result = SetupResult {
            experiment_id: "test-uuid".to_string(),
            task: "classification".to_string(),
            target_column: "species".to_string(),
            feature_columns: vec!["sepal_length".to_string(), "sepal_width".to_string()],
            train_size: 0.7,
            fold: 10,
        };
        assert_eq!(result.task, "classification");
        assert_eq!(result.feature_columns.len(), 2);
        assert_eq!(result.train_size, 0.7);
        assert_eq!(result.fold, 10);
    }

    #[test]
    fn test_setup_result_clone() {
        let result = SetupResult {
            experiment_id: "uuid-123".to_string(),
            task: "regression".to_string(),
            target_column: "price".to_string(),
            feature_columns: vec!["size".to_string()],
            train_size: 0.8,
            fold: 5,
        };
        let cloned = result.clone();
        assert_eq!(cloned.experiment_id, result.experiment_id);
        assert_eq!(cloned.task, result.task);
    }

    #[test]
    fn test_train_result_construction() {
        let result = TrainResult {
            model_bytes: vec![1, 2, 3],
            algorithm: "RandomForestClassifier".to_string(),
            metrics: serde_json::json!({"accuracy": 0.95, "f1": 0.92}),
            task: "classification".to_string(),
            target_column: "species".to_string(),
            feature_columns: vec!["a".to_string(), "b".to_string()],
            label_classes: Some(vec![
                "setosa".to_string(),
                "versicolor".to_string(),
                "virginica".to_string(),
            ]),
        };
        assert_eq!(result.algorithm, "RandomForestClassifier");
        assert_eq!(result.model_bytes.len(), 3);
        assert_eq!(result.task, "classification");
        assert_eq!(result.target_column, "species");
        assert_eq!(result.label_classes.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_train_result_with_empty_metrics() {
        let result = TrainResult {
            model_bytes: vec![],
            algorithm: "lr".to_string(),
            metrics: serde_json::json!({}),
            task: "regression".to_string(),
            target_column: "price".to_string(),
            feature_columns: vec![],
            label_classes: None, // Regression has no label classes
        };
        assert!(result.model_bytes.is_empty());
        assert_eq!(result.metrics, serde_json::json!({}));
        assert!(result.label_classes.is_none());
    }

    #[test]
    fn test_pycaret_wrapper_code_is_valid() {
        // Ensure the Python code can be parsed as a C string (no null bytes)
        let c_string = std::ffi::CString::new(PYCARET_WRAPPER_CODE);
        assert!(c_string.is_ok(), "Python code should be valid C string");
    }

    #[test]
    fn test_pycaret_wrapper_code_contains_functions() {
        // Verify the embedded Python code contains expected functions
        assert!(
            PYCARET_WRAPPER_CODE.contains("def detect_task("),
            "Should contain detect_task function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_setup("),
            "Should contain pycaret_setup function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_compare_models("),
            "Should contain pycaret_compare_models function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_create_model("),
            "Should contain pycaret_create_model function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def predict("),
            "Should contain predict function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def predict_proba("),
            "Should contain predict_proba function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def predict_batch("),
            "Should contain predict_batch function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("_experiment_state"),
            "Should contain experiment state dict"
        );
    }

    #[test]
    fn test_pycaret_wrapper_code_has_setup_check() {
        // Verify that compare_models and create_model check for setup completion
        assert!(
            PYCARET_WRAPPER_CODE.contains("if not _experiment_state['setup_complete']"),
            "Should check setup_complete before model training"
        );
        assert!(
            PYCARET_WRAPPER_CODE
                .contains("raise RuntimeError(\"Must call setup() before compare_models()\")"),
            "Should raise error if setup not called before compare_models"
        );
        assert!(
            PYCARET_WRAPPER_CODE
                .contains("raise RuntimeError(\"Must call setup() before create_model()\")"),
            "Should raise error if setup not called before create_model"
        );
    }

    #[test]
    fn test_pycaret_wrapper_uses_pickle() {
        // Verify pickle is used for model serialization
        assert!(
            PYCARET_WRAPPER_CODE.contains("import pickle"),
            "Should import pickle"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("pickle.dumps("),
            "Should use pickle.dumps for serialization"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("pickle.loads("),
            "Should use pickle.loads for deserialization"
        );
    }

    // =========================================================================
    // Time Series struct tests
    // =========================================================================

    #[test]
    fn test_ts_setup_result_construction() {
        let result = TsSetupResult {
            experiment_id: "ts-uuid-123".to_string(),
            target_column: "y".to_string(),
            index_column: "ds".to_string(),
            fh: 12,
            fold: 3,
            fold_strategy: "expanding".to_string(),
        };
        assert_eq!(result.experiment_id, "ts-uuid-123");
        assert_eq!(result.target_column, "y");
        assert_eq!(result.index_column, "ds");
        assert_eq!(result.fh, 12);
        assert_eq!(result.fold, 3);
        assert_eq!(result.fold_strategy, "expanding");
    }

    #[test]
    fn test_ts_setup_result_clone() {
        let result = TsSetupResult {
            experiment_id: "uuid-abc".to_string(),
            target_column: "sales".to_string(),
            index_column: "date".to_string(),
            fh: 6,
            fold: 5,
            fold_strategy: "sliding".to_string(),
        };
        let cloned = result.clone();
        assert_eq!(cloned.experiment_id, result.experiment_id);
        assert_eq!(cloned.fh, result.fh);
        assert_eq!(cloned.fold_strategy, result.fold_strategy);
    }

    #[test]
    fn test_forecast_row_construction() {
        let row = ForecastRow {
            step: 1,
            timestamp: "2024-01-01T00:00:00".to_string(),
            prediction: 42.5,
            lower: Some(38.0),
            upper: Some(47.0),
        };
        assert_eq!(row.step, 1);
        assert_eq!(row.timestamp, "2024-01-01T00:00:00");
        assert_eq!(row.prediction, 42.5);
        assert_eq!(row.lower, Some(38.0));
        assert_eq!(row.upper, Some(47.0));
    }

    #[test]
    fn test_forecast_row_without_intervals() {
        let row = ForecastRow {
            step: 3,
            timestamp: "2024-03-01T00:00:00".to_string(),
            prediction: 100.0,
            lower: None,
            upper: None,
        };
        assert_eq!(row.step, 3);
        assert_eq!(row.prediction, 100.0);
        assert!(row.lower.is_none());
        assert!(row.upper.is_none());
    }

    #[test]
    fn test_forecast_row_debug() {
        let row = ForecastRow {
            step: 1,
            timestamp: "2024-01-01".to_string(),
            prediction: 10.0,
            lower: None,
            upper: None,
        };
        let debug = format!("{:?}", row);
        assert!(debug.contains("ForecastRow"));
        assert!(debug.contains("prediction: 10.0"));
    }

    // =========================================================================
    // Time Series Python function presence tests
    // =========================================================================

    #[test]
    fn test_pycaret_wrapper_contains_ts_functions() {
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_setup_timeseries("),
            "Should contain pycaret_setup_timeseries function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_create_ts_model("),
            "Should contain pycaret_create_ts_model function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_compare_ts_models("),
            "Should contain pycaret_compare_ts_models function"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("def pycaret_forecast("),
            "Should contain pycaret_forecast function"
        );
    }

    #[test]
    fn test_pycaret_wrapper_ts_uses_experiment_state() {
        assert!(
            PYCARET_WRAPPER_CODE.contains("ts_experiment"),
            "Should store TS experiment in state"
        );
        assert!(
            PYCARET_WRAPPER_CODE.contains("TSForecastingExperiment"),
            "Should use PyCaret TSForecastingExperiment"
        );
    }
}
