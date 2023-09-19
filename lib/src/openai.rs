//! Deals with the OpenAI API.

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Endpoint for embedding.
pub const EMBEDDING_ENDPOINT: &str = "https://api.openai.com/v1/embeddings";

/// Request body for embedding.
#[derive(Clone, Debug, Serialize)]
pub struct EmbeddingRequestBody {
    /// Model.
    pub model: String,
    /// Input text.
    pub input: Vec<String>,
    /// Optional user.
    ///
    /// See [OpenAI docs](https://platform.openai.com/docs/guides/safety-best-practices/end-user-ids).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Response body for embedding.
#[derive(Clone, Debug, Deserialize)]
pub struct EmbeddingResponseBody {
    /// Object type.
    pub object: String,
    /// Embedding output data.
    pub data: Vec<EmbeddingData>,
    /// Model.
    pub model: String,
    /// API usage.
    pub usage: Usage,
}

/// Embedding output data.
#[derive(Clone, Debug, Deserialize)]
pub struct EmbeddingData {
    /// Object type.
    pub object: String,
    /// Embedding vector.
    pub embedding: Vec<f64>,
    /// Index of the input text.
    pub index: usize,
}

/// API usage.
#[derive(Clone, Debug, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub total_tokens: u64,
}

/// Creates an embedding vector of given texts.
///
/// Uses `reqwest` to send a POST request to the OpenAI API.
pub async fn create_embeddings(
    request: &EmbeddingRequestBody,
    api_key: String,
) -> Result<EmbeddingResponseBody, Error> {
    let res = reqwest::Client::new()
        .post(EMBEDDING_ENDPOINT)
        .header("Authorization", format!("Bearer {}", api_key))
        .json(request)
        .send().await?;
    if !res.status().is_success() {
        return Err(Error::HttpError(res.status()));
    }
    let res = res.json::<EmbeddingResponseBody>().await?;
    Ok(res)
}
