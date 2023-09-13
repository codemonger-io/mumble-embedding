//! Deals with the OpenAI API.

use serde::{Deserialize, Serialize};

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
    prompt_tokens: u64,
    total_tokens: u64,
}
