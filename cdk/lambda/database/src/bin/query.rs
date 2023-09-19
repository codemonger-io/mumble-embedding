//! Query database.
//!
//! # Environment variables
//!
//! - `DATABASE_BUCKET_NAME`: name of the S3 bucekt that contains the database.
//! - `DATABASE_KEY`: key of the database file in the bucket.
//! - `OPENAI_API_KEY`: API key for OpenAI.

use anyhow::Context;
use core::cell::Ref;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde::Deserialize;
use serde_json::{Value, json};
use std::env;
use tracing::{Level, event};

use flechasdb::db::AttributeValue;
use flechasdb::db::stored;
use flechasdb::db::stored::{Database, DatabaseStore, LoadDatabase};
use flechasdb::io::FileSystem;
use flechasdb::slice::AsSlice;

use mumble_embedding::fs::S3FileSystem;
use mumble_embedding::openai::{EmbeddingRequestBody, create_embeddings};

#[derive(Clone, Debug, Deserialize)]
struct Query {
    text: String,
}

async fn function_handler(event: LambdaEvent<Query>) -> Result<Value, Error> {
    let (query_text, _context) = event.into_parts();
    let bucket_name = env::var("DATABASE_BUCKET_NAME")
        .context("no DATABASE_BUCKET_NAME set")?;
    let db_key = env::var("DATABASE_KEY")
        .context("no DATABASE_KEY set")?;
    let results = query(bucket_name, db_key, query_text.text).await?;
    Ok(json!({ "results": results }))
}

async fn query(
    bucket_name: String,
    db_key: String,
    query_text: String,
) -> Result<Vec<String>, Error> {
    event!(Level::INFO, "creating embedding for the query");
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
        "loading database from S3 bucket: {}/{}",
        bucket_name,
        db_key,
    );
    let path_segments: Vec<&str> = db_key.split('/').collect();
    let base_path = path_segments[0..path_segments.len() - 1].join("/");
    let db_name = path_segments[path_segments.len() - 1].to_string();
    // needs to spawn a new thread to block on S3 operations
    let handle = tokio::runtime::Handle::try_current()
        .expect("must be within Tokio runtime context");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let join_handle = std::thread::spawn(move || {
        let time = std::time::Instant::now();
        let aws_config = handle.block_on(aws_config::load_from_env());
        let fs = S3FileSystem::new(
            handle.clone(),
            aws_config,
            bucket_name,
            base_path,
        );
        let db = DatabaseStore::<f32, _>::load_database(fs, db_name)
            .expect("failed to load database");
        event!(
            Level::INFO,
            "loaded database in {} μs",
            time.elapsed().as_micros(),
        );
        let res = do_query(&db, &query_vector[..]);
        tx.send(res)
            .or(Err(anyhow::anyhow!("failed to return database")))
            .unwrap();
    });
    let result = rx.await?;
    join_handle.join().expect("failed to join serializer thread");
    result
}

fn do_query<FS, V>(
    db: &Database<f32, FS>,
    query_vector: V,
) -> Result<Vec<String>, Error>
where
    FS: FileSystem,
    V: AsSlice<f32>,
{
    const K: usize = 10; // k-nearest neighbors
    const NPROBE: usize = 1;
    // queries k-NN
    let time = std::time::Instant::now();
    let mut event_time = std::time::Instant::now();
    let results = db.query(
        query_vector.as_slice(),
        K.try_into().unwrap(),
        NPROBE.try_into().unwrap(),
        Some(move |event| {
            match event {
                stored::DatabaseQueryEvent::StartingQueryInitialization |
                stored::DatabaseQueryEvent::StartingPartitionSelection |
                stored::DatabaseQueryEvent::StartingPartitionQuery(_) |
                stored::DatabaseQueryEvent::StartingResultSelection => {
                    event_time = std::time::Instant::now();
                },
                stored::DatabaseQueryEvent::FinishedQueryInitialization => {
                    event!(
                        Level::INFO,
                        "- initialized query in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
                stored::DatabaseQueryEvent::FinishedPartitionSelection => {
                    event!(
                        Level::INFO,
                        "- selected partitions in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
                stored::DatabaseQueryEvent::FinishedPartitionQuery(i) => {
                    event!(
                        Level::INFO,
                        "- queried partition {} in {} μs",
                        i,
                        event_time.elapsed().as_micros(),
                    );
                },
                stored::DatabaseQueryEvent::FinishedResultSelection => {
                    event!(
                        Level::INFO,
                        "- selected results in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
            }
        })
    )?;
    event!(Level::INFO, "queried k-NN in {} μs", time.elapsed().as_micros());
    let time = std::time::Instant::now();
    for (i, result) in results.iter().enumerate() {
        let content_id = db.get_attribute_of(result, "content_id");
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
            .iter()
            .map(|r| db
                .get_attribute_of(r, "content_id")
                .unwrap()
                .map(|x| Ref::map(x, |x| match x {
                    AttributeValue::String(s) => s,
                }).clone())
                .unwrap()
            )
            .collect(),
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
