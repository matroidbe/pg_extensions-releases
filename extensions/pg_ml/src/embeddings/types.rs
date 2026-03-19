//! Shared types for embedding providers

use serde::{Deserialize, Serialize};

/// Result from an embedding provider
#[derive(Debug, Clone)]
pub struct EmbeddingResponse {
    pub embedding: Vec<f32>,
    pub model: String,
    pub usage: Option<UsageInfo>,
}

/// Batch result from an embedding provider
#[derive(Debug, Clone)]
pub struct BatchEmbeddingResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub model: String,
    pub usage: Option<UsageInfo>,
}

/// Token usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageInfo {
    pub prompt_tokens: i64,
    pub total_tokens: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedding_response_construction() {
        let resp = EmbeddingResponse {
            embedding: vec![0.1, 0.2, 0.3],
            model: "test-model".to_string(),
            usage: Some(UsageInfo {
                prompt_tokens: 5,
                total_tokens: 5,
            }),
        };
        assert_eq!(resp.embedding.len(), 3);
        assert_eq!(resp.model, "test-model");
    }

    #[test]
    fn test_batch_embedding_response_empty() {
        let resp = BatchEmbeddingResponse {
            embeddings: vec![],
            model: "test".to_string(),
            usage: None,
        };
        assert!(resp.embeddings.is_empty());
        assert!(resp.usage.is_none());
    }

    #[test]
    fn test_usage_info_serde_roundtrip() {
        let usage = UsageInfo {
            prompt_tokens: 42,
            total_tokens: 42,
        };
        let json = serde_json::to_string(&usage).unwrap();
        let parsed: UsageInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.prompt_tokens, 42);
        assert_eq!(parsed.total_tokens, 42);
    }
}
