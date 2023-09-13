//! Common error.

/// Common error.
#[derive(Debug)]
pub enum Error {
    InvalidData(String),
    SerdeJsonError(serde_json::Error),
    AwsSdkError(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidData(s) => write!(f, "Invalid data: {}", s),
            Error::SerdeJsonError(e) => write!(f, "serde_json::Error: {}", e),
            Error::AwsSdkError(s) => write!(f, "AWS SDK error: {}", s),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerdeJsonError(e)
    }
}

impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for Error {
    fn from(e: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        Error::AwsSdkError(format!("{}", e))
    }
}

impl From<aws_sdk_s3::primitives::ByteStreamError> for Error {
    fn from(e: aws_sdk_s3::primitives::ByteStreamError) -> Self {
        Error::AwsSdkError(format!("{}", e))
    }
}
