//! Defines the file system for S3.

use aws_config::SdkConfig;
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use base64::engine::general_purpose::{
    STANDARD as base64_engine,
    URL_SAFE_NO_PAD as url_safe_base64_engine,
};
use std::io::{Read, Write};
use tempfile::NamedTempFile;

use flechasdb::error::Error;
use flechasdb::io::{FileSystem, HashedFileIn, HashedFileOut};

/// `FileSystem` on S3.
pub struct S3FileSystem {
    runtime_handle: tokio::runtime::Handle,
    aws_config: SdkConfig,
    bucket_name: String,
    base_path: String,
}

impl<'a> S3FileSystem {
    /// Creates a new `FileSystem` on S3.
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        aws_config: SdkConfig,
        bucket_name: impl Into<String>,
        base_path: impl Into<String>,
    ) -> S3FileSystem {
        S3FileSystem {
            runtime_handle,
            aws_config,
            bucket_name: bucket_name.into(),
            base_path: base_path.into(),
        }
    }
}

impl FileSystem for S3FileSystem {
    type HashedFileOut = S3HashedFileOut;
    type HashedFileIn = S3HashedFileIn;

    fn create_hashed_file<'a>(&self) -> Result<Self::HashedFileOut, Error> {
        S3HashedFileOut::create(
            self.runtime_handle.clone(),
            self.aws_config.clone(),
            self.bucket_name.clone(),
            self.base_path.clone(),
        )
    }

    fn create_hashed_file_in<P>(
        &self,
        path: P,
    ) -> Result<Self::HashedFileOut, Error>
    where
        P: AsRef<str>,
    {
        S3HashedFileOut::create(
            self.runtime_handle.clone(),
            self.aws_config.clone(),
            self.bucket_name.clone(),
            format!("{}/{}", self.base_path, path.as_ref()),
        )
    }

    fn open_hashed_file<P>(&self, path: P) -> Result<Self::HashedFileIn, Error>
    where
        P: AsRef<str>,
    {
        S3HashedFileIn::open(
            self.runtime_handle.clone(),
            &self.aws_config,
            self.bucket_name.clone(),
            format!("{}/{}", self.base_path, path.as_ref()),
        )
    }
}

/// Writable file in an S3 bucket.
pub struct S3HashedFileOut {
    runtime_handle: tokio::runtime::Handle,
    aws_config: SdkConfig,
    tempfile: NamedTempFile,
    bucket_name: String,
    base_path: String,
    context: ring::digest::Context,
}

impl S3HashedFileOut {
    fn create(
        runtime_handle: tokio::runtime::Handle,
        aws_config: SdkConfig,
        bucket_name: String,
        base_path: String,
    ) -> Result<Self, Error> {
        let tempfile = NamedTempFile::new()?;
        Ok(S3HashedFileOut {
            runtime_handle,
            aws_config,
            tempfile,
            bucket_name,
            base_path,
            context: ring::digest::Context::new(&ring::digest::SHA256),
        })
    }
}

impl Write for S3HashedFileOut {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.context.update(buf);
        self.tempfile.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.tempfile.flush()
    }
}

impl HashedFileOut for S3HashedFileOut {
    /// Uploads the contents to the S3 bucket.
    ///
    /// Blocks until the upload completes.
    /// This function must be called within the context of a Tokio runtime,
    /// otherwise fails with `Error::InvalidContext`.
    fn persist<S>(mut self, extension: S) -> Result<String, Error>
    where
        S: AsRef<str>,
    {
        self.flush()?;
        let digest = self.context.finish();
        let id = url_safe_base64_engine.encode(digest.as_ref());
        let checksum = base64_engine.encode(digest.as_ref());
        let key = format!("{}/{}.{}", self.base_path, id, extension.as_ref());
        let s3 = aws_sdk_s3::Client::new(&self.aws_config);
        let body = self.runtime_handle
            .block_on(ByteStream::from_path(self.tempfile.path()))
            .map_err(|e| Error::InvalidContext(format!(
                "failed to read the temporary file: {}",
                e,
            )))?;
        let res = s3.put_object()
            .bucket(self.bucket_name)
            .key(key)
            .checksum_sha256(checksum)
            .body(body)
            .send();
        self.runtime_handle
            .block_on(res)
            .map_err(|e| Error::InvalidContext(format!(
                "failed to upload the content to S3: {}",
                e,
            )))?;
        Ok(id)
    }
}

/// Readable file in an S3 bucket.
pub struct S3HashedFileIn {
    body: bytes::Bytes,
    read_pos: usize,
    checksum: String,
    context: ring::digest::Context,
}

impl S3HashedFileIn {
    /// Blocks until the download completes.
    /// This function must be called within the context of a Tokio runtime,
    /// otherwise fails with `Error::InvalidContext`.
    fn open(
        runtime_handle: tokio::runtime::Handle,
        aws_config: &SdkConfig,
        bucket_name: String,
        key: String,
    ) -> Result<Self, Error> {
        let s3 = aws_sdk_s3::Client::new(aws_config);
        let res = s3.get_object()
            .bucket(bucket_name)
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send();
        let res = runtime_handle.block_on(res)
            .map_err(|e| Error::InvalidContext(format!(
                "failed to download the content from S3: {}",
                e,
            )))?;
        let checksum = res.checksum_sha256
            .ok_or(Error::InvalidContext(format!(
                "no checksum for the content from S3",
            )))?;
        let body = runtime_handle.block_on(res.body.collect())
            .map_err(|e| Error::InvalidContext(format!(
                "failed to read the content from S3: {}",
                e,
            )))?
            .into_bytes();
        Ok(S3HashedFileIn {
            body,
            read_pos: 0,
            checksum,
            context: ring::digest::Context::new(&ring::digest::SHA256),
        })
    }
}

impl Read for S3HashedFileIn {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut stream = &self.body[self.read_pos..];
        let n = stream.read(buf)?;
        self.read_pos += n;
        self.context.update(&buf[..n]);
        Ok(n)
    }
}

impl HashedFileIn for S3HashedFileIn {
    fn verify(self) -> Result<(), Error> {
        let digest = self.context.finish();
        let checksum = base64_engine.encode(digest.as_ref());
        if checksum == self.checksum {
            Ok(())
        } else {
            Err(Error::VerificationFailure(format!(
                "checksum discrepancy: expected {} but got {}",
                self.checksum,
                checksum,
            )))
        }
    }
}
