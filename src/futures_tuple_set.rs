use std::collections::HashMap;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use crate::{AnyFuture, BoxFuture, Delay, FuturesMap, PushError, Timeout};

/// Represents a list of tuples of a [Future] and an associated piece of data.
///
/// Each future must finish within the specified time and the list never outgrows its capacity.
pub struct FuturesTupleSet<O, D> {
    id: u32,
    inner: FuturesMap<u32, O>,
    data: HashMap<u32, D>,
}

impl<O, D> FuturesTupleSet<O, D> {
    pub fn new(make_delay: impl Fn() -> Delay + Send + Sync + 'static, capacity: usize) -> Self {
        Self {
            id: 0,
            inner: FuturesMap::new(make_delay, capacity),
            data: HashMap::new(),
        }
    }
}

impl<O, D> FuturesTupleSet<O, D>
where
    O: 'static,
{
    /// Push a future into the list.
    ///
    /// This method adds the given future to the list.
    /// If the length of the list is equal to the capacity, this method returns a error that contains the passed future.
    /// In that case, the future is not added to the set.
    pub fn try_push<F>(&mut self, future: F, data: D) -> Result<(), (BoxFuture<O>, D)>
    where
        F: AnyFuture<Output = O>,
    {
        self.id = self.id.wrapping_add(1);

        match self.inner.try_push(self.id, future) {
            Ok(()) => {}
            Err(PushError::BeyondCapacity(w)) => return Err((w, data)),
            Err(PushError::Replaced(_)) => unreachable!("we never reuse IDs"),
        }
        self.data.insert(self.id, data);

        Ok(())
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

    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<(Result<O, Timeout>, D)> {
        let (id, res) = ready!(self.inner.poll_unpin(cx));
        let data = self.data.remove(&id).expect("must have data for future");

        Poll::Ready((res, data))
    }

    /// Returns an iterator over all futures of type `T` pushed via [`FuturesTupleSet::try_push`].
    /// This iterator returns futures in an arbitrary order, which may change.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_of_type<T>(&self) -> impl Iterator<Item = (Pin<&T>, &D)>
    where
        T: 'static,
    {
        self.inner
            .iter_of_type()
            .map(|(id, item)| (item, self.data.get(id).expect("must have data for future")))
    }

    /// Returns an iterator with mutable access over all futures of type `T`
    /// pushed via [`FuturesTupleSet::try_push`].
    /// This iterator returns futures in an arbitrary order, which may change.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_mut_of_type<T>(&mut self) -> impl Iterator<Item = (Pin<&mut T>, &mut D)>
    where
        T: 'static,
    {
        // We need to convince the borrow checker that we are only returning one mutable reference
        // to each data item. We can do this efficiently by creating a hashmap of mutable
        // references, then removing each item from the hashmap as we iterate over the futures.
        let mut mut_refs_data = self
            .data
            .iter_mut()
            .map(|(id, data)| (*id, data))
            .collect::<HashMap<u32, &mut D>>();

        self.inner.iter_mut_of_type().map(move |(id, item)| {
            let data = mut_refs_data
                .remove(id)
                .expect("each future has a unique id, must have data for future");
            (item, data)
        })
    }
}

#[cfg(all(test, feature = "futures-timer"))]
mod tests {
    use super::*;
    use futures::channel::oneshot;
    use futures_util::future::poll_fn;
    use futures_util::FutureExt;
    use std::future::ready;
    use std::time::Duration;

    #[test]
    fn tracks_associated_data_of_future() {
        let mut set = FuturesTupleSet::new(|| Delay::futures_timer(Duration::from_secs(10)), 10);

        let _ = set.try_push(ready(1), 1);
        let _ = set.try_push(ready(2), 2);

        let (res1, data1) = poll_fn(|cx| set.poll_unpin(cx)).now_or_never().unwrap();
        let (res2, data2) = poll_fn(|cx| set.poll_unpin(cx)).now_or_never().unwrap();

        assert_eq!(res1.unwrap(), data1);
        assert_eq!(res2.unwrap(), data2);
    }

    #[test]
    fn can_iter_typed_futures() {
        const N: usize = 10;
        let mut futures =
            FuturesTupleSet::new(|| Delay::futures_timer(Duration::from_millis(100)), N);
        let mut sender = Vec::with_capacity(N);
        for i in 0..N {
            let (tx, rx) = oneshot::channel::<()>();
            futures
                .try_push(rx, format!("Data{i}"))
                .map_err(|_| PushError::BeyondCapacity(()))
                .unwrap();
            sender.push(tx);
        }
        assert_eq!(futures.iter_of_type::<oneshot::Receiver<()>>().count(), N);
        for (i, (_, data)) in futures.iter_of_type::<oneshot::Receiver<()>>().enumerate() {
            let expect_data = format!("Data{}", N - i - 1); // Reverse order.
            assert_eq!(data, &expect_data);
        }
        assert!(!sender.iter().any(|tx| tx.is_canceled()));

        for (mut rx, _) in futures.iter_mut_of_type::<oneshot::Receiver<()>>() {
            rx.close();
        }
        assert!(sender.iter().all(|tx| tx.is_canceled()));

        // Deliberately try a non-matching type
        assert_eq!(futures.iter_mut_of_type::<()>().count(), 0);
    }
}
