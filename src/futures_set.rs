use std::task::{ready, Context, Poll};

use crate::{AnyFuture, BoxFuture, Delay, FuturesMap, PushError, Timeout};

/// Represents a list of [Future]s.
///
/// Each future must finish within the specified time and the list never outgrows its capacity.
pub struct FuturesSet<O> {
    id: u32,
    inner: FuturesMap<u32, O>,
}

impl<O> FuturesSet<O> {
    pub fn new(make_delay: impl Fn() -> Delay + Send + Sync + 'static, capacity: usize) -> Self {
        Self {
            id: 0,
            inner: FuturesMap::new(make_delay, capacity),
        }
    }
}

impl<O> FuturesSet<O>
where
    O: 'static,
{
    /// Push a future into the list.
    ///
    /// This method adds the given future to the list.
    /// If the length of the list is equal to the capacity, this method returns a error that contains the passed future.
    /// In that case, the future is not added to the set.
    pub fn try_push<F>(&mut self, future: F) -> Result<(), BoxFuture<O>>
    where
        F: AnyFuture<Output = O>,
    {
        self.id = self.id.wrapping_add(1);

        match self.inner.try_push(self.id, future) {
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

    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<Result<O, Timeout>> {
        let (_, res) = ready!(self.inner.poll_unpin(cx));

        Poll::Ready(res)
    }

    /// Returns an iterator over all futures of type `T` pushed via [`FuturesSet::try_push`].
    /// The order that futures are returned is not guaranteed.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_of_type<T>(&self) -> impl Iterator<Item = &T>
    where
        T: 'static,
    {
        self.inner.iter_of_type().map(|(_, item)| item)
    }

    /// Returns an iterator with mutable access over all futures of type `T`
    /// pushed via [`FuturesSet::try_push`].
    /// The order that futures are returned is not guaranteed.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_mut_of_type<T>(&mut self) -> impl Iterator<Item = &mut T>
    where
        T: 'static,
    {
        self.inner.iter_mut_of_type().map(|(_, item)| item)
    }
}
