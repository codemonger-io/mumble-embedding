//! Dealing with posts (mumblings).

use serde::Deserialize;
use tokio_stream::Stream;

use crate::error::Error;
use crate::s3::ObjectList;
use crate::streams::StreamAsyncExt;

/// Post.
#[derive(Deserialize, Debug)]
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
#[derive(Deserialize, Debug)]
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
