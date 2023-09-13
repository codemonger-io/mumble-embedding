use anyhow::{Error, anyhow};
use std::env;
use tokio_stream::StreamExt;

use mumble_embedding::posts::list_posts;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let username = args.get(1).ok_or(anyhow!("username must be specified"))?;
    println!("pull mumbling from {}", username);
    let objects_bucket_name = env::var("OBJECTS_BUCKET_NAME")?;
    println!("objects bucket name: {}", objects_bucket_name);
    let posts = list_posts(&objects_bucket_name, &username).await;
    let mut posts = posts.take(10);
    while let Some(post) = posts.next().await {
        println!("{}", post.unwrap().content);
    }
    Ok(())
}
