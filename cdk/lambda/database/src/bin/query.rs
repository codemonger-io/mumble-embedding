//! Query database.
//!
//! # Environment variables
//!
//! - `DATABASE_BUCKET_NAME`: name of the S3 bucekt that contains the database.
//! - `DATABASE_KEY`: key of the database file in the bucket.
//! - `OPENAI_API_KEY`: API key for OpenAI.

use anyhow::Context;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde::Deserialize;
use serde_json::{Value, json};
use std::env;
use tracing::{Level, event};

use flechasdb::asyncdb::stored::{Database, LoadDatabase};
use flechasdb::db::AttributeValue;
use flechasdb::slice::AsSlice;
use flechasdb_s3::asyncfs::S3FileSystem;

use mumble_embedding::openai::{EmbeddingRequestBody, create_embeddings};

#[derive(Clone, Debug, Deserialize)]
struct Query {
    text: String,
}

async fn function_handler(event: LambdaEvent<Query>) -> Result<Value, Error> {
    let time = std::time::Instant::now();
    let (query_text, _context) = event.into_parts();
    let bucket_name = env::var("DATABASE_BUCKET_NAME")
        .context("no DATABASE_BUCKET_NAME set")?;
    let db_key = env::var("DATABASE_KEY")
        .context("no DATABASE_KEY set")?;
    let results = query(bucket_name, db_key, query_text.text).await?;
    event!(
        Level::INFO,
        "total elapsed {} μs",
        time.elapsed().as_micros(),
    );
    Ok(json!({ "results": results }))
}

async fn query(
    bucket_name: String,
    db_key: String,
    query_text: String,
) -> Result<Vec<String>, Error> {
    event!(Level::INFO, "creating embedding for the query");
    let time = std::time::Instant::now();
    let openai_api_key = env::var("OPENAI_API_KEY")
        .context("no OPENAI_API_KEY set")?;
    let query_embedding = create_embeddings(
        &EmbeddingRequestBody {
            model: "text-embedding-ada-002".to_string(),
            input: vec![query_text.to_string()],
            user: Some("mumble_embedding".to_string()),
        },
        openai_api_key,
    ).await?;
    let query_vector: Vec<f32> = query_embedding.data[0].embedding
        .iter()
        .map(|x| *x as f32)
        .collect();
    event!(
        Level::INFO,
        "created embedding for the query in {} μs",
        time.elapsed().as_micros(),
    );
    event!(
        Level::INFO,
        "loading database from S3 bucket: {}/{}",
        bucket_name,
        db_key,
    );
    let path_segments: Vec<&str> = db_key.split('/').collect();
    let base_path = path_segments[0..path_segments.len() - 1].join("/");
    let db_name = path_segments[path_segments.len() - 1].to_string();
    let time = std::time::Instant::now();
    let aws_config = aws_config::load_from_env().await;
    let fs = S3FileSystem::new(
        &aws_config,
        bucket_name,
        base_path,
    );
    let db = Database::<f32, _>::load_database(fs, db_name).await?;
    event!(
        Level::INFO,
        "loaded database in {} μs",
        time.elapsed().as_micros(),
    );
    do_query(&db, &query_vector[..]).await
}

async fn do_query<V>(
    db: &Database<f32, S3FileSystem>,
    query_vector: V,
) -> Result<Vec<String>, Error>
where
    V: AsSlice<f32>,
{
    const K: usize = 10; // k-nearest neighbors
    const NPROBE: usize = 1;
    // queries k-NN
    let time = std::time::Instant::now();
    let results = db.query_with_events(
        query_vector.as_slice(),
        K.try_into().unwrap(),
        NPROBE.try_into().unwrap(),
        |event| {
            event!(
                Level::INFO,
                "{:?} at {} s",
                event,
                time.elapsed().as_secs_f64(),
            );
        },
    ).await?;
    event!(Level::INFO, "queried k-NN in {} μs", time.elapsed().as_micros());

    let time = std::time::Instant::now();
    let results: Result<_, Error> = futures::future::try_join_all(
        results.into_iter().map(|result| async move {
            let content_id = result.get_attribute("content_id").await
                .context("failed to get 'content_id'")?;
            Ok((result, content_id))
        }),
    ).await;
    let results = results.map_err(|err| anyhow::anyhow!(
        "failed to get 'content_id': {}",
        err,
    ))?;
    for (i, (result, content_id)) in results.iter().enumerate() {
        event!(
            Level::INFO,
            "result[{}]:\ncontent ID: {:?}\napprox. distance: {}",
            i,
            content_id,
            result.squared_distance,
        );
    }
    event!(Level::INFO, "printed results in {} μs", time.elapsed().as_micros());

    Ok(
        results
            .into_iter()
            .map(|(_, content_id)| {
                content_id
                    .map(|x| match x {
                        AttributeValue::String(s) => Ok(s.clone()),
                        AttributeValue::Uint64(_) => Err(anyhow::anyhow!(
                            "content_id must be a string but got u64",
                        )),
                    })
                    .unwrap()
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    lambda_runtime::run(service_fn(function_handler)).await
}
