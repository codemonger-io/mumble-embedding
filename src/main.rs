use anyhow::{Context, Error, anyhow};
use std::env;
use std::fs::{File, create_dir_all};
use std::path::Path;
use tokio_stream::StreamExt;
use url::Url;

use mumble_embedding::posts::{create_embeddings_for_posts, list_posts};
use mumble_embedding::streams::StreamAsyncExt;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let username = args.get(1).ok_or(anyhow!("no username specified"))?;
    let out_dir = args.get(2).ok_or(anyhow!("no output directory specified"))?;
    let objects_bucket_name = env::var("OBJECTS_BUCKET_NAME")
        .context("no OBJECTS_BUCKET_NAME set")?;
    println!("objects bucket name: {}", objects_bucket_name);
    let openai_api_key = env::var("OPENAI_API_KEY")
        .context("no OPENAI_API_KEY set")?;
    println!("output directory: {}", out_dir);
    if !Path::new(out_dir).exists() {
        create_dir_all(out_dir)?;
    }
    println!("pulling mumblings of {}", username);
    let posts = list_posts(&objects_bucket_name, &username).await;
    let mut embeddings = posts
        .chunks_timeout(10, core::time::Duration::from_secs(10))
        .then(|p| async {
            if let Ok(p) = p.into_iter().collect::<Result<_, _>>() {
                create_embeddings_for_posts(p, openai_api_key.clone()).await
            } else {
                Err(mumble_embedding::error::Error::InvalidData(
                    format!("failed to create embeddings for a batch"),
                ))
            }
        })
        .flatten_results();
    while let Some(embedding) = embeddings.next().await {
        match embedding {
            Ok(embedding) => {
                println!("created embeddings: {:?}", embedding.id);
                let parsed = Url::parse(&embedding.id)?;
                let name = parsed.path_segments()
                    .ok_or(anyhow!("invalid ID: {}", embedding.id))?
                    .last()
                    .ok_or(anyhow!("invalid ID: {}", embedding.id))?;
                let path = Path::new(out_dir).join(name).with_extension("json");
                println!("saving embedding to {:?}", path);
                let out = File::create(path)?;
                serde_json::to_writer(out, &embedding)?;
            },
            err => {
                err?;
            }
        };
    }
    Ok(())
}
