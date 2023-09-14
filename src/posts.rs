//! Dealing with posts (mumblings).

use serde::{Deserialize, Serialize};
use tokio_stream::Stream;

use crate::error::Error;
use crate::openai::{EmbeddingRequestBody, create_embeddings};
use crate::s3::ObjectList;
use crate::streams::StreamAsyncExt;

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
        .map_async(move |o| load_post(
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

/// Embedding of a content.
#[derive(Clone, Debug, Serialize)]
pub struct Embedding {
    /// ID of the source object.
    pub id: String,
    /// Content that produced the embedding.
    pub content: String,
    /// Embedding vector.
    pub embedding: Vec<f64>,
}

/// Creates embeddings for given posts.
pub async fn create_embeddings_for_posts(
    posts: Vec<Post>,
    api_key: String,
) -> Result<Vec<Embedding>, Error> {
    let request = EmbeddingRequestBody {
        model: format!("text-embedding-ada-002"),
        input: posts.iter()
            .map(|p| {
                if let Some(source) = p.source.as_ref() {
                    source.content.clone()
                } else {
                    p.content.clone()
                }
            })
            .collect(),
        user: Some(format!("mumble_embedding")),
    };
    let res = create_embeddings(&request, api_key).await?;
    println!("usage: {:?}", res.usage);
    let mut data = res.data;
    if posts.len() != data.len() {
        return Err(Error::InvalidData(
            format!("failed to create embeddings of one or more posts"),
        ));
    }
    data.sort_by_key(|d| d.index);
    let embeddings = posts.into_iter()
        .zip(request.input.into_iter())
        .zip(data.into_iter())
        .map(|((p, content), d)| Embedding {
            id: p.id,
            content,
            embedding: d.embedding,
        })
        .collect();
    Ok(embeddings)
}
