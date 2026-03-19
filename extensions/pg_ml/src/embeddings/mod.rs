//! External embedding API integration
//!
//! Generates text embeddings via OpenAI or Voyage AI APIs, configured through GUC settings.
//! Returns `Vec<f32>` (maps to `real[]` in SQL), castable to pgvector's `vector` type.

pub mod config;
pub mod openai;
pub mod types;
pub mod voyage;

use crate::PgMlError;
use types::{BatchEmbeddingResponse, EmbeddingResponse};

/// Trait for embedding providers
pub trait EmbeddingProvider {
    /// Generate embedding for a single text input
    fn embed(&self, input: &str) -> Result<EmbeddingResponse, PgMlError>;

    /// Generate embeddings for multiple text inputs in a single API call
    fn embed_batch(&self, inputs: &[String]) -> Result<BatchEmbeddingResponse, PgMlError>;
}

/// Create an embedding provider based on current GUC configuration
pub fn create_provider() -> Result<Box<dyn EmbeddingProvider>, PgMlError> {
    let provider_name = config::get_provider()?;
    let api_key = config::get_api_key()?;
    let model = config::get_model(&provider_name);
    let base_url = config::get_api_url(&provider_name);
    let timeout = config::get_timeout();

    create_provider_with_params(&provider_name, api_key, model, base_url, timeout)
}

/// Create a provider with explicit parameters (for per-call overrides)
pub fn create_provider_with_params(
    provider_name: &str,
    api_key: String,
    model: String,
    base_url: String,
    timeout: std::time::Duration,
) -> Result<Box<dyn EmbeddingProvider>, PgMlError> {
    match provider_name {
        "openai" => Ok(Box::new(openai::OpenAiProvider::new(
            api_key, model, base_url, timeout,
        ))),
        "voyage" => Ok(Box::new(voyage::VoyageProvider::new(
            api_key, model, base_url, timeout,
        ))),
        other => Err(PgMlError::UnsupportedProvider(other.to_string())),
    }
}

/// Generate embedding for a single text input (convenience function)
pub fn embed(input: &str) -> Result<EmbeddingResponse, PgMlError> {
    let provider = create_provider()?;
    provider.embed(input)
}

/// Generate embeddings for multiple inputs (convenience function)
pub fn embed_batch(inputs: &[String]) -> Result<BatchEmbeddingResponse, PgMlError> {
    let provider = create_provider()?;
    provider.embed_batch(inputs)
}
