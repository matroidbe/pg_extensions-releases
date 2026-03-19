//! Voyage AI embedding provider
//!
//! Supports voyage-3, voyage-3-lite, voyage-code-3, voyage-finance-2.
//! Voyage AI is Anthropic's recommended embedding partner.

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::types::{BatchEmbeddingResponse, EmbeddingResponse, UsageInfo};
use super::EmbeddingProvider;
use crate::PgMlError;

pub struct VoyageProvider {
    client: Client,
    api_key: String,
    model: String,
    base_url: String,
}

#[derive(Serialize)]
struct VoyageRequest<'a> {
    input: Vec<&'a str>,
    model: &'a str,
}

#[derive(Deserialize)]
struct VoyageResponse {
    data: Vec<VoyageEmbedding>,
    model: String,
    usage: VoyageUsage,
}

#[derive(Deserialize)]
struct VoyageEmbedding {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize)]
struct VoyageUsage {
    total_tokens: i64,
}

#[derive(Deserialize)]
struct VoyageErrorResponse {
    detail: String,
}

impl VoyageProvider {
    pub fn new(api_key: String, model: String, base_url: String, timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            api_key,
            model,
            base_url,
        }
    }

    fn call_api(&self, inputs: Vec<&str>) -> Result<VoyageResponse, PgMlError> {
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));

        let request = VoyageRequest {
            input: inputs,
            model: &self.model,
        };

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&request)
            .send()?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            if let Ok(err) = serde_json::from_str::<VoyageErrorResponse>(&body) {
                return Err(PgMlError::EmbeddingApiError(format!(
                    "Voyage AI API error ({}): {}",
                    status, err.detail
                )));
            }
            return Err(PgMlError::EmbeddingApiError(format!(
                "Voyage AI API error ({}): {}",
                status, body
            )));
        }

        response.json::<VoyageResponse>().map_err(|e| {
            PgMlError::EmbeddingApiError(format!("Failed to parse Voyage AI response: {}", e))
        })
    }
}

impl EmbeddingProvider for VoyageProvider {
    fn embed(&self, input: &str) -> Result<EmbeddingResponse, PgMlError> {
        let result = self.call_api(vec![input])?;

        let embedding =
            result.data.into_iter().next().ok_or_else(|| {
                PgMlError::EmbeddingApiError("Empty response from Voyage AI".into())
            })?;

        Ok(EmbeddingResponse {
            embedding: embedding.embedding,
            model: result.model,
            usage: Some(UsageInfo {
                prompt_tokens: result.usage.total_tokens,
                total_tokens: result.usage.total_tokens,
            }),
        })
    }

    fn embed_batch(&self, inputs: &[String]) -> Result<BatchEmbeddingResponse, PgMlError> {
        if inputs.is_empty() {
            return Ok(BatchEmbeddingResponse {
                embeddings: vec![],
                model: self.model.clone(),
                usage: None,
            });
        }

        let input_refs: Vec<&str> = inputs.iter().map(|s| s.as_str()).collect();
        let result = self.call_api(input_refs)?;

        // Sort by index to ensure order matches input
        let mut data = result.data;
        data.sort_by_key(|e| e.index);

        Ok(BatchEmbeddingResponse {
            embeddings: data.into_iter().map(|e| e.embedding).collect(),
            model: result.model,
            usage: Some(UsageInfo {
                prompt_tokens: result.usage.total_tokens,
                total_tokens: result.usage.total_tokens,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = VoyageRequest {
            input: vec!["hello", "world"],
            model: "voyage-3",
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("voyage-3"));
        assert!(json.contains("[\"hello\",\"world\"]"));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "object": "list",
            "data": [{"object": "embedding", "embedding": [0.1, 0.2, 0.3], "index": 0}],
            "model": "voyage-3",
            "usage": {"total_tokens": 5}
        }"#;
        let resp: VoyageResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].embedding.len(), 3);
        assert_eq!(resp.data[0].index, 0);
        assert_eq!(resp.model, "voyage-3");
        assert_eq!(resp.usage.total_tokens, 5);
    }

    #[test]
    fn test_batch_response_ordering() {
        let json = r#"{
            "object": "list",
            "data": [
                {"object": "embedding", "embedding": [0.3], "index": 1},
                {"object": "embedding", "embedding": [0.1], "index": 0}
            ],
            "model": "voyage-3",
            "usage": {"total_tokens": 10}
        }"#;
        let resp: VoyageResponse = serde_json::from_str(json).unwrap();
        let mut data = resp.data;
        data.sort_by_key(|e| e.index);
        assert_eq!(data[0].embedding[0], 0.1);
        assert_eq!(data[1].embedding[0], 0.3);
    }

    #[test]
    fn test_error_response_deserialization() {
        let json = r#"{"detail": "Invalid API key"}"#;
        let err: VoyageErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.detail, "Invalid API key");
    }
}
