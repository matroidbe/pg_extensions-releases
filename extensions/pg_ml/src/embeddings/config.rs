//! GUC settings for embedding configuration

use std::ffi::CString;

use crate::PgMlError;

/// Embedding provider: 'openai' or 'voyage'
pub static EMBEDDING_PROVIDER: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// API key for the embedding provider
pub static EMBEDDING_API_KEY: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Model name (e.g., 'text-embedding-3-small', 'voyage-3')
pub static EMBEDDING_MODEL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Base URL override (for proxies, Azure OpenAI, etc.)
pub static EMBEDDING_API_URL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// HTTP timeout in seconds
pub static EMBEDDING_TIMEOUT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(30);

/// Register all embedding-related GUC settings
pub fn register_gucs() {
    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.embedding_provider",
        c"Embedding API provider (openai, voyage)",
        c"Which external API to use for generating embeddings",
        &EMBEDDING_PROVIDER,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.embedding_api_key",
        c"API key for the embedding provider",
        c"Authentication key for OpenAI or Voyage AI API",
        &EMBEDDING_API_KEY,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::NO_SHOW_ALL | pgrx::GucFlags::SUPERUSER_ONLY,
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.embedding_model",
        c"Embedding model name",
        c"Model identifier (e.g., text-embedding-3-small, voyage-3)",
        &EMBEDDING_MODEL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.embedding_api_url",
        c"Base URL for embedding API",
        c"Override the default API endpoint (for proxies or Azure OpenAI)",
        &EMBEDDING_API_URL,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_ml.embedding_timeout",
        c"HTTP timeout in seconds for embedding API calls",
        c"Maximum time to wait for an embedding API response",
        &EMBEDDING_TIMEOUT,
        1,
        300,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

/// Get the configured provider name
pub fn get_provider() -> Result<String, PgMlError> {
    EMBEDDING_PROVIDER
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .ok_or_else(|| {
            PgMlError::EmbeddingNotConfigured(
                "pg_ml.embedding_provider not set. Use: SET pg_ml.embedding_provider = 'openai'"
                    .into(),
            )
        })
}

/// Get the configured API key
pub fn get_api_key() -> Result<String, PgMlError> {
    EMBEDDING_API_KEY
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .ok_or_else(|| PgMlError::EmbeddingNotConfigured("pg_ml.embedding_api_key not set".into()))
}

/// Get the model name with provider-specific defaults
pub fn get_model(provider: &str) -> String {
    EMBEDDING_MODEL
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .unwrap_or_else(|| default_model(provider).to_string())
}

/// Get the base URL with provider-specific defaults
pub fn get_api_url(provider: &str) -> String {
    EMBEDDING_API_URL
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .unwrap_or_else(|| default_api_url(provider).to_string())
}

/// Get timeout as Duration
pub fn get_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(EMBEDDING_TIMEOUT.get() as u64)
}

/// Default model per provider
pub fn default_model(provider: &str) -> &'static str {
    match provider {
        "openai" => "text-embedding-3-small",
        "voyage" => "voyage-3",
        _ => "text-embedding-3-small",
    }
}

/// Default API URL per provider
pub fn default_api_url(provider: &str) -> &'static str {
    match provider {
        "openai" => "https://api.openai.com/v1",
        "voyage" => "https://api.voyageai.com/v1",
        _ => "https://api.openai.com/v1",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_model_openai() {
        assert_eq!(default_model("openai"), "text-embedding-3-small");
    }

    #[test]
    fn test_default_model_voyage() {
        assert_eq!(default_model("voyage"), "voyage-3");
    }

    #[test]
    fn test_default_model_unknown_falls_back() {
        assert_eq!(default_model("unknown"), "text-embedding-3-small");
    }

    #[test]
    fn test_default_api_url_openai() {
        assert_eq!(default_api_url("openai"), "https://api.openai.com/v1");
    }

    #[test]
    fn test_default_api_url_voyage() {
        assert_eq!(default_api_url("voyage"), "https://api.voyageai.com/v1");
    }
}
