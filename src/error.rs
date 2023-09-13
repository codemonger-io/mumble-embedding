//! Common error.

/// Common error.
#[derive(Debug)]
pub enum Error {
    InvalidData(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidData(s) => write!(f, "Invalid data: {}", s),
        }
    }
}
