//! Asynchronous extensions for `Stream`.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;

/// Asynchronous extensions for `Stream`.
pub trait StreamAsyncExt: Stream {
    /// Maps items with a given async function.
    fn map_async<F, FUT, T>(self, f: F) -> Map<Self, F, FUT>
    where
        F: FnMut(Self::Item) -> FUT,
        FUT: Future<Output = T>,
        Self: Sized,
    {
        Map::new(self, f)
    }

    /// Flattens `Result`s whose successful value is an iterable.
    ///
    /// Retains an error as a single item.
    fn flatten_results<T, E>(self) -> FlattenResults<Self, T, E>
    where
        Self: Stream<Item = Result<T, E>> + Sized,
        T: IntoIterator,
    {
        FlattenResults::new(self)
    }
}

impl<ST: Stream> StreamAsyncExt for ST {}

/// Mapping stream.
pub struct Map<ST, F, FUT>
where
    ST: Stream + ?Sized,
    F: FnMut(ST::Item) -> FUT,
{
    stream: Pin::<Box<ST>>,
    pending_map: Option<Pin<Box<FUT>>>,
    f: F,
}

impl<ST, F, FUT> Map<ST, F, FUT>
where
    ST: Stream,
    F: FnMut(ST::Item) -> FUT,
{
    fn new(stream: ST, f: F) -> Self {
        Self {
            stream: Box::pin(stream),
            pending_map: None,
            f,
        }
    }
}

impl<ST, F, FUT, T> Stream for Map<ST, F, FUT>
where
    ST: Stream,
    F: FnMut(ST::Item) -> FUT,
    FUT: Future<Output = T>,
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

/// Flattening stream.
pub struct FlattenResults<ST, T, E>
where
    ST: Stream<Item = Result<T, E>> + ?Sized,
    T: IntoIterator,
{
    stream: Pin::<Box<ST>>,
    iterator: Option<T::IntoIter>,
}

impl<ST, T, E> FlattenResults<ST, T, E>
where
    ST: Stream<Item = Result<T, E>>,
    T: IntoIterator,
{
    fn new(stream: ST) -> Self {
        Self {
            stream: Box::pin(stream),
            iterator: None,
        }
    }
}

impl<ST, T, E> Stream for FlattenResults<ST, T, E>
where
    ST: Stream<Item = Result<T, E>>,
    T: IntoIterator,
    Self: Unpin, // necessary for <DerefMut as Pin>
{
    type Item = Result<T::Item, E>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(iterator) = self.iterator.as_mut() {
            if let Some(item) = iterator.next() {
                Poll::Ready(Some(Ok(item)))
            } else {
                self.iterator = None;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        } else {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(result)) => {
                    match result {
                        Ok(iterable) => {
                            self.iterator = Some(iterable.into_iter());
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        },
                        Err(err) => Poll::Ready(Some(Err(err))),
                    }
                },
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }
    }
}
