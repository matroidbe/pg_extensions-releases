//! OpenAI embedding provider
//!
//! Supports text-embedding-3-small, text-embedding-3-large, and text-embedding-ada-002.

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::types::{BatchEmbeddingResponse, EmbeddingResponse, UsageInfo};
use super::EmbeddingProvider;
use crate::PgMlError;

pub struct OpenAiProvider {
    client: Client,
    api_key: String,
    model: String,
    base_url: String,
}

#[derive(Serialize)]
struct OpenAiRequest<'a> {
    input: serde_json::Value,
    model: &'a str,
    encoding_format: &'a str,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    data: Vec<OpenAiEmbedding>,
    model: String,
    usage: OpenAiUsage,
}

#[derive(Deserialize)]
struct OpenAiEmbedding {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    prompt_tokens: i64,
    total_tokens: i64,
}

#[derive(Deserialize)]
struct OpenAiErrorResponse {
    error: OpenAiErrorDetail,
}

#[derive(Deserialize)]
struct OpenAiErrorDetail {
    message: String,
}

impl OpenAiProvider {
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

    fn call_api(&self, input: serde_json::Value) -> Result<OpenAiResponse, PgMlError> {
        let url = format!("{}/embeddings", self.base_url.trim_end_matches('/'));

        let request = OpenAiRequest {
            input,
            model: &self.model,
            encoding_format: "float",
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
            if let Ok(err) = serde_json::from_str::<OpenAiErrorResponse>(&body) {
                return Err(PgMlError::EmbeddingApiError(format!(
                    "OpenAI API error ({}): {}",
                    status, err.error.message
                )));
            }
            return Err(PgMlError::EmbeddingApiError(format!(
                "OpenAI API error ({}): {}",
                status, body
            )));
        }

        response.json::<OpenAiResponse>().map_err(|e| {
            PgMlError::EmbeddingApiError(format!("Failed to parse OpenAI response: {}", e))
        })
    }
}

impl EmbeddingProvider for OpenAiProvider {
    fn embed(&self, input: &str) -> Result<EmbeddingResponse, PgMlError> {
        let result = self.call_api(serde_json::Value::String(input.to_string()))?;

        let embedding = result
            .data
            .into_iter()
            .next()
            .ok_or_else(|| PgMlError::EmbeddingApiError("Empty response from OpenAI".into()))?;

        Ok(EmbeddingResponse {
            embedding: embedding.embedding,
            model: result.model,
            usage: Some(UsageInfo {
                prompt_tokens: result.usage.prompt_tokens,
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

        let input_array: Vec<serde_json::Value> = inputs
            .iter()
            .map(|s| serde_json::Value::String(s.clone()))
            .collect();

        let result = self.call_api(serde_json::Value::Array(input_array))?;

        // Sort by index to ensure order matches input
        let mut data = result.data;
        data.sort_by_key(|e| e.index);

        Ok(BatchEmbeddingResponse {
            embeddings: data.into_iter().map(|e| e.embedding).collect(),
            model: result.model,
            usage: Some(UsageInfo {
                prompt_tokens: result.usage.prompt_tokens,
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
        let req = OpenAiRequest {
            input: serde_json::Value::String("test".into()),
            model: "text-embedding-3-small",
            encoding_format: "float",
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("text-embedding-3-small"));
        assert!(json.contains("\"input\":\"test\""));
        assert!(json.contains("\"encoding_format\":\"float\""));
    }

    #[test]
    fn test_request_serialization_batch() {
        let inputs = vec![
            serde_json::Value::String("hello".into()),
            serde_json::Value::String("world".into()),
        ];
        let req = OpenAiRequest {
            input: serde_json::Value::Array(inputs),
            model: "text-embedding-3-small",
            encoding_format: "float",
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("[\"hello\",\"world\"]"));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "object": "list",
            "data": [{"object": "embedding", "embedding": [0.1, 0.2, 0.3], "index": 0}],
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 5, "total_tokens": 5}
        }"#;
        let resp: OpenAiResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].embedding.len(), 3);
        assert_eq!(resp.data[0].index, 0);
        assert_eq!(resp.model, "text-embedding-3-small");
        assert_eq!(resp.usage.prompt_tokens, 5);
    }

    #[test]
    fn test_batch_response_ordering() {
        let json = r#"{
            "object": "list",
            "data": [
                {"object": "embedding", "embedding": [0.3], "index": 1},
                {"object": "embedding", "embedding": [0.1], "index": 0}
            ],
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 10, "total_tokens": 10}
        }"#;
        let resp: OpenAiResponse = serde_json::from_str(json).unwrap();
        let mut data = resp.data;
        data.sort_by_key(|e| e.index);
        assert_eq!(data[0].embedding[0], 0.1);
        assert_eq!(data[1].embedding[0], 0.3);
    }

    #[test]
    fn test_error_response_deserialization() {
        let json = r#"{
            "error": {
                "message": "Invalid API key provided",
                "type": "invalid_request_error",
                "param": null,
                "code": "invalid_api_key"
            }
        }"#;
        let err: OpenAiErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(err.error.message, "Invalid API key provided");
    }
}
