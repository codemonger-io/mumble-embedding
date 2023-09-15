use anyhow::{Context, Error, anyhow, bail};
use clap::{Parser, Subcommand};
use std::env;
use std::fs::{File, create_dir_all, read_dir};
use std::path::Path;
use tokio_stream::StreamExt;
use url::Url;

use flechasdb::db::{DatabaseBuilder, DatabaseBuilderEvent, DatabaseQueryEvent};
use flechasdb::db::proto::serialize_database;
use flechasdb::io::LocalFileSystem;
use flechasdb::vector::BlockVectorSet;

use mumble_embedding::openai::{EmbeddingRequestBody, create_embeddings};
use mumble_embedding::posts::{
    Embedding,
    create_embeddings_for_posts,
    list_posts,
};
use mumble_embedding::streams::StreamAsyncExt;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands
}

#[derive(Subcommand)]
enum Commands {
    /// Creates embeddings for user's posts.
    Create {
        /// Username whose posts are to be processed.
        username: String,
        /// Output directory where embedding results are to be saved.
        out_dir: String,
    },
    /// Builds a vector database from embedding results.
    Build {
        /// Input directory where embedding results are to be loaded from.
        in_dir: String,
        /// Output directory where the vector database are saved.
        out_dir: String,
        /// Test query.
        test_query: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Create { username, out_dir } => {
            create(username, out_dir).await?;
        },
        Commands::Build { in_dir, out_dir, test_query } => {
            build(in_dir, out_dir, test_query).await?;
        },
    }
    Ok(())
}

async fn create(username: String, out_dir: String) -> Result<(), Error> {
    let objects_bucket_name = env::var("OBJECTS_BUCKET_NAME")
        .context("no OBJECTS_BUCKET_NAME set")?;
    println!("objects bucket name: {}", objects_bucket_name);
    let openai_api_key = env::var("OPENAI_API_KEY")
        .context("no OPENAI_API_KEY set")?;
    println!("output directory: {}", out_dir);
    if !Path::new(&out_dir).exists() {
        create_dir_all(&out_dir)?;
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
                let path = Path::new(&out_dir).join(name).with_extension("json");
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

async fn build(
    in_dir: String,
    out_dir: String,
    test_query: Option<String>,
) -> Result<(), Error> {
    const RESERVED_VECTORS: usize = 1000;
    const VECTOR_SIZE: usize = 1536; // OpenAI embedding vector
    const NUM_PARTITIONS: usize = 1;
    const NUM_DIVISIONS: usize = 12;
    const NUM_CODES: usize = 10;
    let mut data: Vec<f32> = Vec::with_capacity(RESERVED_VECTORS * VECTOR_SIZE);
    let mut contents: Vec<String> = Vec::with_capacity(RESERVED_VECTORS);
    for entry in read_dir(in_dir)? {
        let entry = entry?;
        println!("loading: {:?}", entry.file_name());
        let file = File::open(entry.path())?;
        let embedding: Embedding = serde_json::from_reader(file)?;
        if embedding.embedding.len() != VECTOR_SIZE {
            bail!("invalid vector size: {}", embedding.embedding.len());
        }
        data.extend(embedding.embedding.iter().map(|v| *v as f32));
        contents.push(embedding.content);
    }
    let vs = BlockVectorSet::chunk(data, VECTOR_SIZE.try_into()?)?;
    let time = std::time::Instant::now();
    let mut event_time = std::time::Instant::now();
    let db = DatabaseBuilder::new(vs)
        .with_partitions(NUM_PARTITIONS.try_into().unwrap())
        .with_divisions(NUM_DIVISIONS.try_into().unwrap())
        .with_clusters(NUM_CODES.try_into().unwrap())
        .build(Some(move |event| {
            match event {
                DatabaseBuilderEvent::StartingIdAssignment |
                DatabaseBuilderEvent::StartingPartitioning |
                DatabaseBuilderEvent::StartingSubvectorDivision |
                DatabaseBuilderEvent::StartingQuantization(_) => {
                    event_time = std::time::Instant::now();
                },
                DatabaseBuilderEvent::FinishedIdAssignment => {
                    println!(
                        "assigned vector IDs in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
                DatabaseBuilderEvent::FinishedPartitioning => {
                    println!(
                        "partitioned data in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
                DatabaseBuilderEvent::FinishedSubvectorDivision => {
                    println!(
                        "divided data in {} μs",
                        event_time.elapsed().as_micros(),
                    );
                },
                DatabaseBuilderEvent::FinishedQuantization(i) => {
                    println!(
                        "quantized division {} in {} μs",
                        i,
                        event_time.elapsed().as_micros(),
                    );
                },
            };
        }))?;
    println!("built database in {} μs", time.elapsed().as_micros());

    println!("saving database to {}", out_dir);
    let mut fs = LocalFileSystem::new(&out_dir);
    serialize_database(&db, &mut fs)?;

    // makes a test query if one is given
    if let Some(test_query) = test_query {
        const K: usize = 10; // k-nearest neighbors
        const NPROBE: usize = 1;
        let openai_api_key = env::var("OPENAI_API_KEY")
            .context("no OPENAI_API_KEY set")?;
        let query_embedding = create_embeddings(
            &EmbeddingRequestBody {
                model: "text-embedding-ada-002".to_string(),
                input: vec![test_query.to_string()],
                user: Some("mumble_embedding".to_string()),
            },
            openai_api_key,
        ).await?;
        let query_vector: Vec<f32> = query_embedding.data[0].embedding
            .iter()
            .map(|x| *x as f32)
            .collect();
        let mut event_time = std::time::Instant::now();
        let results = db.query(
            &query_vector,
            K.try_into()?,
            NPROBE.try_into()?,
            Some(move |event| {
                match event {
                    DatabaseQueryEvent::StartingPartitionSelection |
                    DatabaseQueryEvent::StartingPartitionQuery(_) |
                    DatabaseQueryEvent::StartingResultSelection => {
                        event_time = std::time::Instant::now();
                    },
                    DatabaseQueryEvent::FinishedPartitionSelection => {
                        println!(
                            "selected partitions in {} μs",
                            event_time.elapsed().as_micros(),
                        );
                    },
                    DatabaseQueryEvent::FinishedPartitionQuery(i) => {
                        println!(
                            "queried partition {} in {} μs",
                            i,
                            event_time.elapsed().as_micros(),
                        );
                    },
                    DatabaseQueryEvent::FinishedResultSelection => {
                        println!(
                            "selected results in {} μs",
                            event_time.elapsed().as_micros(),
                        );
                    },
                }
            }),
        )?;
        println!("testing query: {}", test_query);
        for (i, result) in results.iter().enumerate() {
            println!(
                "result[{}]:\ncontent: {}\napprox. distance: {}",
                i,
                contents[result.vector_index],
                result.squared_distance,
            );
        }
    }

    Ok(())
}
