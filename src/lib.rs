mod delay;
mod futures_map;
mod futures_set;
mod futures_tuple_set;
mod stream_map;
mod stream_set;

pub use delay::Delay;
pub use futures_map::FuturesMap;
pub use futures_set::FuturesSet;
pub use futures_tuple_set::FuturesTupleSet;
pub use stream_map::StreamMap;
pub use stream_set::StreamSet;

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::time::Duration;

/// A future failed to complete within the given timeout.
#[derive(Debug)]
pub struct Timeout {
    limit: Duration,
}

impl Timeout {
    fn new(duration: Duration) -> Self {
        Self { limit: duration }
    }
}

impl fmt::Display for Timeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "future failed to complete within {:?}", self.limit)
    }
}

/// Error of a future pushing
#[derive(PartialEq)]
pub enum PushError<T> {
    /// The length of the set is equal to the capacity
    BeyondCapacity(T),
    /// The map already contained an item with this key.
    ///
    /// The old item is returned.
    Replaced(T),
}

impl<T> fmt::Debug for PushError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeyondCapacity(_) => f.debug_tuple("BeyondCapacity").finish(),
            Self::Replaced(_) => f.debug_tuple("Replaced").finish(),
        }
    }
}

impl std::error::Error for Timeout {}

#[doc(hidden)]
pub trait AnyStream: futures_util::Stream + Any + Unpin + Send {}

impl<T> AnyStream for T where T: futures_util::Stream + Any + Unpin + Send {}

type BoxStream<T> = Pin<Box<dyn AnyStream<Item = T> + Send>>;

#[doc(hidden)]
pub trait AnyFuture: std::future::Future + Any + Unpin + Send {}

impl<T> AnyFuture for T where T: std::future::Future + Any + Unpin + Send {}

type BoxFuture<T> = Pin<Box<dyn AnyFuture<Output = T> + Send>>;
