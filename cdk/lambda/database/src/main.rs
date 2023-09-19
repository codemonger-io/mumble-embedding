use lambda_runtime::{LambdaEvent, Error, service_fn};
use serde_json::{Value, json};

async fn function_handler(_event: LambdaEvent<Value>) -> Result<Value, Error> {
    Ok(json!({ "message": "Hello, world!".to_string() }))
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
