use std::task::{ready, Context, Poll};

use crate::{AnyStream, BoxStream, Delay, PushError, StreamMap, Timeout};

/// Represents a set of [Stream]s.
///
/// Each stream must finish within the specified time and the list never outgrows its capacity.
pub struct StreamSet<O> {
    id: u32,
    inner: StreamMap<u32, O>,
}

impl<O> StreamSet<O> {
    pub fn new(make_delay: impl Fn() -> Delay + Send + Sync + 'static, capacity: usize) -> Self {
        Self {
            id: 0,
            inner: StreamMap::new(make_delay, capacity),
        }
    }
}

impl<O> StreamSet<O>
where
    O: Send + 'static,
{
    /// Push a stream into the list.
    ///
    /// This method adds the given stream to the list.
    /// If the length of the list is equal to the capacity, this method returns a error that contains the passed stream.
    /// In that case, the stream is not added to the set.
    pub fn try_push<F>(&mut self, stream: F) -> Result<(), BoxStream<O>>
    where
        F: AnyStream<Item = O>,
    {
        self.id = self.id.wrapping_add(1);

        match self.inner.try_push(self.id, stream) {
            Ok(()) => Ok(()),
            Err(PushError::BeyondCapacity(w)) => Err(w),
            Err(PushError::Replaced(_)) => unreachable!("we never reuse IDs"),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn poll_ready_unpin(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.inner.poll_ready_unpin(cx)
    }

    pub fn poll_next_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<O, Timeout>>> {
        let (_, res) = ready!(self.inner.poll_next_unpin(cx));

        Poll::Ready(res)
    }

    /// Returns an iterator over all streams of type `T` pushed via [`StreamSet::try_push`].
    /// This iterator returns streams in an arbitrary order, which may change.
    ///
    /// If downcasting a stream to `T` fails it will be skipped in the iterator.
    pub fn iter_of_type<T>(&self) -> impl Iterator<Item = &T>
    where
        T: 'static,
    {
        self.inner.iter_of_type().map(|(_, item)| item)
    }

    /// Returns an iterator with mutable access over all streams of type `T`
    /// pushed via [`StreamSet::try_push`].
    /// This iterator returns streams in an arbitrary order, which may change.
    ///
    /// If downcasting a stream to `T` fails it will be skipped in the iterator.
    pub fn iter_mut_of_type<T>(&mut self) -> impl Iterator<Item = &mut T>
    where
        T: 'static,
    {
        self.inner.iter_mut_of_type().map(|(_, item)| item)
    }
}
