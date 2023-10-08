//! Common error.

/// Common error.
#[derive(Debug)]
pub enum Error {
    InvalidData(String),
    InvalidContext(String),
    HttpError(reqwest::StatusCode),
    SerdeJsonError(serde_json::Error),
    ReqwestError(reqwest::Error),
    AwsSdkError(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidData(s) => write!(f, "Invalid data: {}", s),
            Error::InvalidContext(s) => write!(f, "Invalid context: {}", s),
            Error::HttpError(s) => write!(f, "HTTP error: {}", s),
            Error::SerdeJsonError(e) => write!(f, "serde_json::Error: {}", e),
            Error::ReqwestError(e) => write!(f, "reqwest::Error: {}", e),
            Error::AwsSdkError(s) => write!(f, "AWS SDK error: {}", s),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerdeJsonError(e)
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::ReqwestError(e)
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
