//! Deals with Amazon S3.

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::{
    ListObjectsV2Error,
    ListObjectsV2Output,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use tokio_stream::Stream;

type ListObjectsV2FutureOutput = Result<
    ListObjectsV2Output,
    SdkError<ListObjectsV2Error, HttpResponse>,
>;

/// Operation to list objects.
pub struct ObjectList {
    bucket_name: String,
    prefix: String,
    s3: aws_sdk_s3::Client,
}

impl ObjectList {
    /// Creates a new operation to list objects.
    pub fn new(
        bucket_name: impl Into<String>,
        prefix: impl Into<String>,
        s3: aws_sdk_s3::Client,
    ) -> Self {
        Self {
            bucket_name: bucket_name.into(),
            prefix: prefix.into(),
            s3,
        }
    }

    /// Start streaming the objects.
    pub fn into_stream<'a>(self) -> ObjectListStream {
        ObjectListStream::new(self)
    }
}

/// Stream of listed objects.
pub struct ObjectListStream {
    config: ObjectList,
    objects: Vec<aws_sdk_s3::types::Object>,
    next_index: usize,
    pending_request: Option<Pin<Box<dyn Future<Output = ListObjectsV2FutureOutput>>>>,
}

impl ObjectListStream {
    /// Starts streaming the objects.
    pub fn new(config: ObjectList) -> Self {
        let pending_request = config.s3.list_objects_v2()
            .bucket(config.bucket_name.clone())
            .prefix(config.prefix.clone())
            .max_keys(10)
            .send();
        Self {
            config,
            objects: Vec::new(),
            next_index: 0,
            pending_request: Some(Box::pin(pending_request)),
        }
    }
}

impl Stream for ObjectListStream {
    type Item = aws_sdk_s3::types::Object;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        println!("polling");
        while self.next_index < self.objects.len() {
            println!("next object");
            let next_index = self.next_index;
            self.next_index += 1;
            let object = &self.objects[next_index];
            if object.key.is_some() {
                return Poll::Ready(Some(object.clone()));
            } // btw, when does object become None?
        }
        // polls the pending request
        if let Some(pending_request) = self.pending_request.as_mut() {
            match Pin::new(pending_request).poll(cx) {
                Poll::Ready(Ok(results)) => {
                    println!("ready");
                    self.objects = results.contents.unwrap_or_default();
                    self.next_index = 0;
                    let last_key = self.objects.last()
                        .and_then(|o| o.key.clone());
                    if last_key.is_some() {
                        let pending_request = self.config.s3.list_objects_v2()
                            .bucket(self.config.bucket_name.clone())
                            .prefix(self.config.prefix.clone())
                            .max_keys(10)
                            .set_start_after(last_key)
                            .send();
                        self.pending_request = Some(Box::pin(pending_request));
                    } else {
                        self.pending_request = None;
                    }
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
                Poll::Ready(_) => {
                    println!("error");
                    return Poll::Ready(None)
                }
                Poll::Pending => {
                    println!("pending");
                    return Poll::Pending
                },
            }
        } else {
            Poll::Ready(None)
        }
    }
}
