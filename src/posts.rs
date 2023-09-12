//! Dealing with posts (mumblings).

use crate::s3::ObjectList;

/// Lists all posts of a specified user.
pub async fn list_posts(bucket_name: &str, username: &str) -> ObjectList {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);
    ObjectList::new(
        bucket_name,
        format!("objects/users/{}/posts/", username),
        client,
    )
}
