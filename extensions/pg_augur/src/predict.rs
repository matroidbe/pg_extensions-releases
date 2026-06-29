//! Prediction helpers backed by augur's `Predictor`.
//!
//! Holds an optional LRU-ish cache keyed by project name so repeated
//! predictions on the same deployed model skip re-parsing the JSON spec.
//! The cache is invalidated on deploy/drop.

use crate::error::AugurPgError;
use crate::models;
use augur::prelude::Predictor;
use once_cell::sync::Lazy;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::RwLock;

struct CacheEntry {
    model_id: i64,
    predictor: Predictor,
}

static CACHE: Lazy<RwLock<HashMap<String, CacheEntry>>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Drop cached predictor for a project (on deploy, drop, or external invalidation).
pub fn invalidate(project: &str) {
    if let Ok(mut map) = CACHE.write() {
        map.remove(project);
    }
}

/// Drop all cached predictors.
#[allow(dead_code)]
pub fn invalidate_all() {
    if let Ok(mut map) = CACHE.write() {
        map.clear();
    }
}

fn build_predictor(stored: &models::StoredModel) -> Result<Predictor, AugurPgError> {
    let json = std::str::from_utf8(&stored.artifact)
        .map_err(|e| AugurPgError::Other(format!("artifact not valid utf-8: {e}")))?;
    let spec = augur::prelude::load_model_from_string(json)?;
    let predictor = Predictor::from_spec(spec)?;
    Ok(predictor)
}

/// Run a closure with the cached predictor for `project`, loading it if needed.
pub fn with_predictor<F, R>(project: &str, f: F) -> Result<R, AugurPgError>
where
    F: FnOnce(&Predictor) -> Result<R, AugurPgError>,
{
    // Fast path: read lock.
    {
        let map = CACHE
            .read()
            .map_err(|_| AugurPgError::Other("predictor cache poisoned".into()))?;
        if let Some(entry) = map.get(project) {
            return f(&entry.predictor);
        }
    }

    // Slow path: load the deployed model and populate cache.
    let stored = models::get_deployed_model(project)?;
    let predictor = build_predictor(&stored)?;

    {
        let mut map = CACHE
            .write()
            .map_err(|_| AugurPgError::Other("predictor cache poisoned".into()))?;
        map.insert(
            project.to_string(),
            CacheEntry {
                model_id: stored.id,
                predictor,
            },
        );
    }

    let map = CACHE
        .read()
        .map_err(|_| AugurPgError::Other("predictor cache poisoned".into()))?;
    let entry = map.get(project).expect("just inserted");
    f(&entry.predictor)
}

/// Returns the first value of the "prediction" column as a serde_json::Value.
pub fn single_prediction_value(df: &DataFrame) -> Result<serde_json::Value, AugurPgError> {
    let col = df
        .column("prediction")
        .map_err(|e| AugurPgError::Polars(e.to_string()))?;
    let s = col.as_materialized_series();
    let dtype = s.dtype().clone();
    if dtype.is_float() {
        if let Ok(ca) = s.f64() {
            if let Some(v) = ca.get(0) {
                return Ok(serde_json::json!(v));
            }
        }
        if let Ok(ca) = s.f32() {
            if let Some(v) = ca.get(0) {
                return Ok(serde_json::json!(v as f64));
            }
        }
    }
    if dtype.is_integer() {
        if let Ok(ca) = s.i64() {
            if let Some(v) = ca.get(0) {
                return Ok(serde_json::json!(v));
            }
        }
    }
    if dtype.is_string() {
        if let Ok(ca) = s.str() {
            if let Some(v) = ca.get(0) {
                return Ok(serde_json::json!(v));
            }
        }
    }
    Ok(serde_json::Value::Null)
}

#[allow(dead_code)]
pub fn cached_model_id(project: &str) -> Option<i64> {
    CACHE
        .read()
        .ok()
        .and_then(|m| m.get(project).map(|e| e.model_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_prediction_value_float64() {
        let s = Series::new("prediction".into(), &[3.14_f64]);
        let df = DataFrame::new(vec![s.into_column()]).unwrap();
        let val = single_prediction_value(&df).unwrap();
        assert_eq!(val.as_f64().unwrap(), 3.14);
    }

    #[test]
    fn single_prediction_value_i64() {
        let s = Series::new("prediction".into(), &[42_i64]);
        let df = DataFrame::new(vec![s.into_column()]).unwrap();
        let val = single_prediction_value(&df).unwrap();
        assert_eq!(val.as_i64().unwrap(), 42);
    }

    #[test]
    fn single_prediction_value_string() {
        let s = Series::new("prediction".into(), &["setosa"]);
        let df = DataFrame::new(vec![s.into_column()]).unwrap();
        let val = single_prediction_value(&df).unwrap();
        assert_eq!(val.as_str().unwrap(), "setosa");
    }

    #[test]
    fn single_prediction_value_missing_column() {
        let s = Series::new("other".into(), &[1.0_f64]);
        let df = DataFrame::new(vec![s.into_column()]).unwrap();
        assert!(single_prediction_value(&df).is_err());
    }

    #[test]
    fn invalidate_nonexistent_no_panic() {
        invalidate("does_not_exist");
    }

    #[test]
    fn invalidate_all_no_panic() {
        invalidate_all();
    }
}
