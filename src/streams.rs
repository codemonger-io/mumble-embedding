//! Asynchronous extensions for `Stream`.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use tokio_stream::Stream;

/// Asynchronous extensions for `Stream`.
pub trait StreamAsyncExt: Stream {
    /// Maps items with a given async function.
    fn map_async<F, FT, T>(self, f: F) -> Map<Self, F, FT>
    where
        F: FnMut(Self::Item) -> FT,
        FT: Future<Output = T>,
        Self: Sized,
    {
        Map::new(self, f)
    }
}

impl<ST: Stream> StreamAsyncExt for ST {}

/// Mapping stream.
pub struct Map<ST, F, FT>
where
    ST: Stream + ?Sized,
    F: FnMut(ST::Item) -> FT,
{
    stream: Pin::<Box<ST>>,
    pending_map: Option<Pin<Box<FT>>>,
    f: F,
}

impl<ST, F, FT> Map<ST, F, FT>
where
    ST: Stream,
    F: FnMut(ST::Item) -> FT,
{
    fn new(stream: ST, f: F) -> Self {
        Self {
            stream: Box::pin(stream),
            pending_map: None,
            f,
        }
    }
}

impl<ST, F, FT, T> Stream for Map<ST, F, FT>
where
    ST: Stream,
    F: FnMut(ST::Item) -> FT,
    FT: Future<Output = T>,
    Self: Unpin, // necessary for <DerefMut as Pin>
{
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(pending_map) = self.pending_map.as_mut() {
            println!("waiting for pending map");
            match Pin::new(pending_map).poll(cx) {
                Poll::Ready(t) => {
                    self.pending_map = None;
                    Poll::Ready(Some(t))
                },
                Poll::Pending => Poll::Pending,
            }
        } else {
            println!("waiting for next item");
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    self.pending_map = Some(Box::pin((self.f)(item)));
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
