use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::error::PgImageError;

// =============================================================================
// Shared Model Cache
// =============================================================================

static MODEL_CACHE: OnceLock<std::sync::Mutex<HashMap<String, ort::session::Session>>> =
    OnceLock::new();

pub fn get_cache() -> &'static std::sync::Mutex<HashMap<String, ort::session::Session>> {
    MODEL_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

// =============================================================================
// Model Path Resolution
// =============================================================================

/// Resolve a model filename to a full path using the pg_image.model_dir GUC.
pub fn model_path(model_name: &str) -> Result<PathBuf, PgImageError> {
    let dir_str = crate::detect::MODEL_DIR
        .get()
        .map(|cs| cs.to_string_lossy().into_owned())
        .ok_or_else(|| {
            PgImageError::OnnxError(
                "pg_image.model_dir is not set. \
                 Run: PG_CONFIG=/path/to/pg_config make setup-onnx \
                 (or ALTER SYSTEM SET pg_image.model_dir = '/path/to/models')"
                    .into(),
            )
        })?;

    let path = PathBuf::from(dir_str).join(model_name);
    if !path.exists() {
        return Err(PgImageError::OnnxError(format!(
            "Model file not found: {}. \
             Run: PG_CONFIG=/path/to/pg_config make setup-model",
            path.display()
        )));
    }
    Ok(path)
}

// =============================================================================
// Session Loading
// =============================================================================

/// Load an ONNX model or retrieve from cache. Returns a mutable reference
/// to the session via the cache lock guard.
///
/// The caller must hold the returned MutexGuard for the duration of inference.
pub fn run_inference<F, T>(model_name: &str, f: F) -> Result<T, PgImageError>
where
    F: FnOnce(&mut ort::session::Session) -> Result<T, PgImageError>,
{
    let path = model_path(model_name)?;
    let cache = get_cache();
    let mut cache_lock = cache
        .lock()
        .map_err(|e| PgImageError::OnnxError(format!("cache lock error: {}", e)))?;

    let model_key = path.to_string_lossy().to_string();
    if !cache_lock.contains_key(&model_key) {
        let session = ort::session::Session::builder()
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?
            .with_intra_threads(1)
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?
            .commit_from_file(&path)
            .map_err(|e| PgImageError::OnnxError(e.to_string()))?;
        cache_lock.insert(model_key.clone(), session);
    }

    let session = cache_lock
        .get_mut(&model_key)
        .ok_or_else(|| PgImageError::OnnxError("model not in cache after insert".into()))?;

    f(session)
}
