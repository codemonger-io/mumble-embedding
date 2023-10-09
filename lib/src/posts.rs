//! Dealing with posts (mumblings).

use core::ops::Range;
use futures::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::markdown::extract_text_blocks;
use crate::error::Error;
use crate::openai::{EmbeddingRequestBody, create_embeddings};
use crate::s3::ObjectList;
use crate::text::extract_sentences;

/// Post.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Post {
    /// ID.
    pub id: String,
    /// Type.
    #[serde(rename = "type")]
    pub type_: String,
    /// Contents.
    pub content: String,
    /// Published.
    pub published: String,
    /// Source.
    pub source: Option<PostSource>,
}

/// Post source.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostSource {
    /// Content.
    pub content: String,
    /// MIME type
    pub media_type: String,
}

/// Lists all posts of a specified user.
pub async fn list_posts(
    bucket_name: &str,
    username: &str,
) -> impl Stream<Item = Result<Post, Error>> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    let bucket_name = bucket_name.to_string();
    ObjectList::new(
        &bucket_name,
        format!("objects/users/{}/posts/", username),
        client,
    )
        .into_stream()
        .then(move |o| load_post(
            bucket_name.clone(),
            o,
            config.clone(),
        ))
}

async fn load_post(
    bucket_name: String,
    object: aws_sdk_s3::types::Object,
    config: aws_config::SdkConfig,
) -> Result<Post, Error> {
    let client = aws_sdk_s3::Client::new(&config);
    let key = object.key.ok_or(Error::InvalidData(format!("missing key")))?;
    println!("retrieving object: {}", key);
    let result = client.get_object()
        .bucket(bucket_name)
        .key(key)
        .send().await?;
    let body = result.body.collect().await?;
    let post = serde_json::from_slice::<Post>(&body.into_bytes())?;
    Ok(post)
}

/// Sentence in a post.
#[derive(Clone, Debug)]
pub struct PostSentence {
    /// ID of the source post.
    pub post_id: String,
    /// Content.
    pub content: String,
    /// Range in the post.
    pub range: Range<usize>,
}

impl PostSentence {
    /// Returns the ID of the sentence.
    pub fn id(&self) -> String {
        format!("{}#{}-{}", self.post_id, self.range.start, self.range.end)
    }
}

/// Splits a post into sentences.
pub fn split_post_into_sentences(post: Post) -> Vec<PostSentence> {
    let content = if let Some(source) = post.source {
        source.content
    } else {
        post.content
    };
    extract_text_blocks(&content)
        .unwrap()
        .into_iter()
        .flat_map(|block| extract_sentences(&block))
        .map(|(sentence, range)| PostSentence {
            post_id: post.id.clone(),
            content: sentence,
            range,
        })
        .collect()
}

/// Embedding of a content.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Embedding {
    /// ID of the source object.
    pub id: String,
    /// Content that produced the embedding.
    pub content: String,
    /// Embedding vector.
    pub embedding: Vec<f64>,
}

/// Creates embeddings for given sentences.
pub async fn create_embeddings_for_sentences(
    sentences: Vec<PostSentence>,
    api_key: String,
) -> Result<Vec<Embedding>, Error> {
    let request = EmbeddingRequestBody {
        model: format!("text-embedding-ada-002"),
        input: sentences.iter().map(|s| s.content.clone()).collect(),
        user: Some(format!("mumble_embedding")),
    };
    let res = create_embeddings(&request, api_key).await?;
    println!("usage: {:?}", res.usage);
    let mut data = res.data;
    if sentences.len() != data.len() {
        return Err(Error::InvalidData(
            format!("failed to create embeddings of one or more posts"),
        ));
    }
    data.sort_by_key(|d| d.index);
    let embeddings = sentences.into_iter()
        .zip(request.input.into_iter())
        .zip(data.into_iter())
        .map(|((s, content), d)| Embedding {
            id: s.id(),
            content,
            embedding: d.embedding,
        })
        .collect();
    Ok(embeddings)
}
