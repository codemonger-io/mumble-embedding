use anyhow::{Context, Error, anyhow, bail};
use clap::{Parser, Subcommand};
use futures::stream::StreamExt;
use std::env;
use std::fs::{File, create_dir_all, read_dir};
use std::path::Path;
use url::Url;

use flechasdb::db::AttributeValue;
use flechasdb::db::build::DatabaseBuilder;
use flechasdb::db::build::proto::serialize_database;
use flechasdb::db::stored::{Database, LoadDatabase};
use flechasdb::io::{FileSystem, LocalFileSystem};
use flechasdb::slice::AsSlice;
use flechasdb::vector::BlockVectorSet;
use flechasdb_s3::syncfs::S3FileSystem;

use mumble_embedding::openai::{EmbeddingRequestBody, create_embeddings};
use mumble_embedding::posts::{
    Embedding,
    create_embeddings_for_sentences,
    list_posts,
    split_post_into_sentences,
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
        ///
        /// It is treated as a key in the S3 bucket if `--s3` optiion is given.
        out_dir: String,
        /// Test query.
        test_query: Option<String>,
        /// Whether to save the database in the S3 bucket.
        #[arg(long)]
        s3: bool,
    },
    /// Queries a vector database.
    Query {
        /// Path to the database to be loaded.
        db_path: String,
        /// Query.
        query_text: String,
        /// Whether to load the database from the S3 bucket.
        #[arg(long)]
        s3: bool,
        /// Directory where embedding results are stored.
        /// Resolves the ID to the contents if this is given.
        #[arg(long)]
        embedding_dir: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Create { username, out_dir } => {
            create(username, out_dir).await?;
        },
        Commands::Build { in_dir, out_dir, test_query, s3 } => {
            build(in_dir, out_dir, test_query, s3).await?;
        },
        Commands::Query { db_path, query_text, s3, embedding_dir } => {
            query(db_path, query_text, s3, embedding_dir).await?;
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
        .map(|post| {
            if let Ok(post) = post {
                Ok(split_post_into_sentences(post))
            } else {
                Err(mumble_embedding::error::Error::InvalidData(
                    format!("failed to list posts"),
                ))
            }
        })
        .flatten_results()
        .chunks(10)
        .then(|s| async {
            if let Ok(s) = s.into_iter().collect::<Result<_, _>>() {
                create_embeddings_for_sentences(s, openai_api_key.clone()).await
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
                let fragment = parsed.fragment()
                    .map_or("".to_string(), |f| format!("#{}", f));
                let name = format!("{}{}", name, fragment);
                let path = Path::new(&out_dir)
                    .join(name)
                    .with_extension("json");
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
    s3: bool,
) -> Result<(), Error> {
    const RESERVED_VECTORS: usize = 1000;
    const VECTOR_SIZE: usize = 1536; // OpenAI embedding vector
    const NUM_PARTITIONS: usize = 1;
    const NUM_DIVISIONS: usize = 12;
    const NUM_CODES: usize = 10;
    let mut embeddings: Vec<Embedding> = Vec::with_capacity(RESERVED_VECTORS);
    let mut data: Vec<f32> = Vec::with_capacity(RESERVED_VECTORS * VECTOR_SIZE);
    for entry in read_dir(in_dir)? {
        let entry = entry?;
        println!("loading: {:?}", entry.file_name());
        let file = File::open(entry.path())?;
        let embedding: Embedding = serde_json::from_reader(file)?;
        if embedding.embedding.len() != VECTOR_SIZE {
            bail!("invalid vector size: {}", embedding.embedding.len());
        }
        data.extend(embedding.embedding.iter().map(|v| *v as f32));
        embeddings.push(embedding);
    }
    let vs = BlockVectorSet::chunk(data, VECTOR_SIZE.try_into()?)?;
    let time = std::time::Instant::now();
    let mut db = DatabaseBuilder::new(vs)
        .with_partitions(NUM_PARTITIONS.try_into().unwrap())
        .with_divisions(NUM_DIVISIONS.try_into().unwrap())
        .with_clusters(NUM_CODES.try_into().unwrap())
        .build_with_events(|event| {
            println!("{:?} at {} s", event, time.elapsed().as_secs_f64());
        })?;
    println!("built database in {} μs", time.elapsed().as_micros());
    // assigns content IDs to vectors
    for (i, embedding) in embeddings.iter().enumerate() {
        db.set_attribute_at(i, ("content_id", embedding.id.clone()))?;
    }

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
        let results = db.query_with_events(
            &query_vector,
            K.try_into()?,
            NPROBE.try_into()?,
            |event| {
                println!("{:?} at {} s", event, time.elapsed().as_secs_f64());
            },
        )?;
        println!("testing query: {}", test_query);
        for (i, result) in results.iter().enumerate() {
            println!(
                "result[{}]:\ncontent: {}\napprox. distance: {}",
                i,
                embeddings[result.vector_index].content,
                result.squared_distance,
            );
        }
    }

    let time = std::time::Instant::now();
    if s3 {
        let bucket_name = env::var("DATABASE_BUCKET_NAME")
            .expect("no DATABASE_BUCKET_NAME set");
        println!("saving database to S3: {}/{}", bucket_name, out_dir);
        // needs to spawn a new thread to block on S3 operations
        let handle = tokio::runtime::Handle::try_current()
            .expect("must be within Tokio runtime context");
        let join_handle = std::thread::spawn(move || {
            let aws_config = handle.block_on(aws_config::load_from_env());
            let mut fs = S3FileSystem::new(
                handle,
                &aws_config,
                bucket_name,
                &out_dir,
            );
            serialize_database(&db, &mut fs)
                .expect("failed to serialize database");
        });
        join_handle.join().expect("failed to join serializer thread");
    } else {
        println!("saving database to {}", out_dir);
        let mut fs = LocalFileSystem::new(&out_dir);
        serialize_database(&db, &mut fs)?;
    }
    println!("saved database in {} μs", time.elapsed().as_micros());

    Ok(())
}

async fn query(
    db_path: String,
    query_text: String,
    s3: bool,
    embedding_dir: Option<String>,
) -> Result<(), Error> {
    println!("creating embedding for the query");
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
    let content_ids = if s3 {
        let bucket_name = env::var("DATABASE_BUCKET_NAME")
            .expect("no DATABASE_BUCKET_NAME set");
        println!(
            "loading database from S3 bucket: {}/{}",
            bucket_name,
            db_path,
        );
        let path_segments: Vec<&str> = db_path.split('/').collect();
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
                &aws_config,
                bucket_name,
                base_path,
            );
            let db = Database::<f32, _>::load_database(fs, db_name)
                .expect("failed to load database");
            println!("loaded database in {} μs", time.elapsed().as_micros());
            let res = do_query(&db, &query_vector[..]);
            tx.send(res)
                .or(Err(anyhow::anyhow!("failed to return database")))
                .unwrap();
        });
        let result = rx.await?;
        join_handle.join().expect("failed to join serializer thread");
        result
    } else {
        println!("loading database from {}", db_path);
        let time = std::time::Instant::now();
        let db_path = Path::new(&db_path);
        let db = Database::<f32, _>::load_database(
            LocalFileSystem::new(db_path.parent().unwrap()),
            db_path.file_name().unwrap().to_str().unwrap(),
        )?;
        println!("loaded database in {} μs", time.elapsed().as_micros());
        do_query(&db, &query_vector[..])
    }?;
    if let Some(embedding_dir) = embedding_dir {
        for (i, id) in content_ids.iter().enumerate() {
            let unique_part = get_unique_part(id)?;
            let embedding_path = Path::new(&embedding_dir)
                .join(format!("{}.json", &unique_part));
            let file = File::open(embedding_path)?;
            let embedding: Embedding = serde_json::from_reader(file)?;
            println!("[{}]: {}", i, embedding.content);
        }
    }
    Ok(())
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
    let results = db.query_with_events(
        query_vector.as_slice(),
        K.try_into().unwrap(),
        NPROBE.try_into().unwrap(),
        |event| {
            println!("{:?} at {} s", event, time.elapsed().as_secs_f64());
        },
    )?;
    println!("queried k-NN in {} μs", time.elapsed().as_micros());
    let time = std::time::Instant::now();
    let content_ids = results.into_iter()
        .map(|result| {
            result
                .get_attribute("content_id")
                .map_err(|e| anyhow!("failed to get attribute: {}", e))
                .and_then(|value| value
                    .map(|value| match &*value {
                        AttributeValue::String(s) => Ok(s.clone()),
                        _ => Err(anyhow!("content_id must be a string")),
                    })
                    .unwrap_or(Err(anyhow!("no content_id"))),
                )
        })
        .collect::<Result<Vec<_>, _>>()?;
    println!("obtained attributes in {} μs", time.elapsed().as_micros());
    for (i, id) in content_ids.iter().enumerate() {
        println!("result[{}]:\ncontent ID: {}", i, id);
    }
    Ok(content_ids)
}

// Returns the unique part of a given ID.
fn get_unique_part<'a>(id: &str) -> Result<String, Error> {
    let url = Url::parse(id)?;
    let part = url.path_segments()
        .and_then(|segments| segments.last())
        .map(|part| part.to_string())
        .ok_or(anyhow!("no unique part in ID"))?;
    let fragment = url.fragment()
        .map(|fragment| format!("#{}", fragment))
        .unwrap_or("".to_string());
    Ok(format!("{}{}", part, fragment))
}
