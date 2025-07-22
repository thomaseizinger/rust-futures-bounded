use std::any::Any;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use std::{future, mem};

use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};

use crate::{AnyFuture, BoxFuture, Delay, PushError, Timeout};

/// Represents a map of [`Future`]s.
///
/// Each future must finish within the specified time and the map never outgrows its capacity.
pub struct FuturesMap<ID, O> {
    make_delay: Box<dyn Fn() -> Delay + Send + Sync>,
    capacity: usize,
    inner: FuturesUnordered<TaggedFuture<ID, TimeoutFuture<BoxFuture<O>>>>,
    empty_waker: Option<Waker>,
    full_waker: Option<Waker>,
}

impl<ID, O> FuturesMap<ID, O> {
    pub fn new(make_delay: impl Fn() -> Delay + Send + Sync + 'static, capacity: usize) -> Self {
        Self {
            make_delay: Box::new(make_delay),
            capacity,
            inner: Default::default(),
            empty_waker: None,
            full_waker: None,
        }
    }
}

impl<ID, O> FuturesMap<ID, O>
where
    ID: Clone + Hash + Eq + Send + Unpin + 'static,
    O: 'static,
{
    /// Push a future into the map.
    ///
    /// This method inserts the given future with defined `future_id` to the set.
    /// If the length of the map is equal to the capacity, this method returns [PushError::BeyondCapacity],
    /// that contains the passed future. In that case, the future is not inserted to the map.
    /// If a future with the given `future_id` already exists, then the old future will be replaced by a new one.
    /// In that case, the returned error [PushError::Replaced] contains the old future.
    pub fn try_push<F>(&mut self, future_id: ID, future: F) -> Result<(), PushError<BoxFuture<O>>>
    where
        F: AnyFuture<Output = O>,
    {
        if self.inner.len() >= self.capacity {
            return Err(PushError::BeyondCapacity(Box::pin(future)));
        }

        if let Some(waker) = self.empty_waker.take() {
            waker.wake();
        }

        let old = self.remove(future_id.clone());
        self.inner.push(TaggedFuture {
            tag: future_id,
            inner: TimeoutFuture {
                inner: Box::pin(future),
                timeout: (self.make_delay)(),
                cancelled: false,
            },
        });
        match old {
            None => Ok(()),
            Some(old) => Err(PushError::Replaced(old)),
        }
    }

    pub fn remove(&mut self, id: ID) -> Option<BoxFuture<O>> {
        let tagged = self.inner.iter_mut().find(|s| s.tag == id)?;

        let inner = mem::replace(&mut tagged.inner.inner, Box::pin(future::pending()));
        tagged.inner.cancelled = true;

        Some(inner)
    }

    pub fn contains(&self, id: ID) -> bool {
        self.inner.iter().any(|f| f.tag == id && !f.inner.cancelled)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[allow(unknown_lints, clippy::needless_pass_by_ref_mut)] // &mut Context is idiomatic.
    pub fn poll_ready_unpin(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.inner.len() < self.capacity {
            return Poll::Ready(());
        }

        self.full_waker = Some(cx.waker().clone());

        Poll::Pending
    }

    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<(ID, Result<O, Timeout>)> {
        loop {
            let maybe_result = futures_util::ready!(self.inner.poll_next_unpin(cx));

            match maybe_result {
                None => {
                    self.empty_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Some((id, Ok(output))) => return Poll::Ready((id, Ok(output))),
                Some((id, Err(TimeoutError::Timeout(dur)))) => {
                    return Poll::Ready((id, Err(Timeout::new(dur))))
                }
                Some((_, Err(TimeoutError::Cancelled))) => continue,
            }
        }
    }

    /// Returns an iterator over all futures of type `T` pushed via [`FuturesMap::try_push`].
    /// This iterator returns futures in an arbitrary order, which may change.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_of_type<T>(&self) -> impl Iterator<Item = (&ID, Pin<&T>)>
    where
        T: 'static,
    {
        self.inner.iter().filter_map(|a| {
            let pin = a.inner.inner.as_ref();

            // Safety: We are only changing the type of the pointer, and pinning it again before returning it.
            // None of the fields are accessed.
            let pointer = unsafe { Pin::into_inner_unchecked(pin) };
            let any = pointer as &(dyn Any + Send);
            // SAFETY: this returns `None` and drops a `&T`, which is safe because dropping a reference is trivial.
            let inner = any.downcast_ref::<T>()?;

            // Safety: The pointer is already pinned.
            let pinned = unsafe { Pin::new_unchecked(inner) };

            Some((&a.tag, pinned))
        })
    }

    /// Returns an iterator with mutable access over all futures of type `T` pushed via
    /// [`FuturesMap::try_push`].
    /// This iterator returns futures in an arbitrary order, which may change.
    ///
    /// If downcasting a future to `T` fails it will be skipped in the iterator.
    pub fn iter_mut_of_type<T>(&mut self) -> impl Iterator<Item = (&ID, Pin<&mut T>)>
    where
        T: 'static,
    {
        self.inner.iter_mut().filter_map(|a| {
            let pin = a.inner.inner.as_mut();

            // Safety: We are only temporarily manipulating the pointer and pinning it again further down.
            let pointer = unsafe { Pin::into_inner_unchecked(pin) };
            let any = pointer as &mut (dyn Any + Send);
            let inner = any.downcast_mut::<T>()?;

            // Safety: The pointer is already pinned.
            let pinned = unsafe { Pin::new_unchecked(inner) };

            Some((&a.tag, pinned))
        })
    }
}

struct TimeoutFuture<F> {
    inner: F,
    timeout: Delay,

    cancelled: bool,
}

impl<F> Future for TimeoutFuture<F>
where
    F: Future + Unpin,
{
    type Output = Result<F::Output, TimeoutError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancelled {
            return Poll::Ready(Err(TimeoutError::Cancelled));
        }

        if let Poll::Ready(duration) = self.timeout.poll_unpin(cx) {
            return Poll::Ready(Err(TimeoutError::Timeout(duration)));
        }

        self.inner.poll_unpin(cx).map(Ok)
    }
}

enum TimeoutError {
    Timeout(Duration),
    Cancelled,
}

struct TaggedFuture<T, F> {
    tag: T,
    inner: F,
}

impl<T, F> Future for TaggedFuture<T, F>
where
    T: Clone + Unpin,
    F: Future + Unpin,
{
    type Output = (T, F::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let output = futures_util::ready!(self.inner.poll_unpin(cx));

        Poll::Ready((self.tag.clone(), output))
    }
}

#[cfg(all(test, feature = "futures-timer"))]
mod tests {
    use futures::channel::oneshot;
    use futures_util::task::noop_waker_ref;
    use std::future::{pending, poll_fn, ready};
    use std::pin::Pin;
    use std::time::Instant;

    use super::*;

    #[test]
    fn cannot_push_more_than_capacity_tasks() {
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_secs(10)), 1);

        assert!(futures.try_push("ID_1", ready(())).is_ok());
        matches!(
            futures.try_push("ID_2", ready(())),
            Err(PushError::BeyondCapacity(_))
        );
    }

    #[test]
    fn cannot_push_the_same_id_few_times() {
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_secs(10)), 5);

        assert!(futures.try_push("ID", ready(())).is_ok());
        matches!(
            futures.try_push("ID", ready(())),
            Err(PushError::Replaced(_))
        );
    }

    #[tokio::test]
    async fn futures_timeout() {
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 1);

        let _ = futures.try_push("ID", pending::<()>());
        futures_timer::Delay::new(Duration::from_millis(150)).await;
        let (_, result) = poll_fn(|cx| futures.poll_unpin(cx)).await;

        assert!(result.is_err())
    }

    #[test]
    fn resources_of_removed_future_are_cleaned_up() {
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 1);

        let _ = futures.try_push("ID", pending::<()>());
        futures.remove("ID");

        let poll = futures.poll_unpin(&mut Context::from_waker(noop_waker_ref()));
        assert!(poll.is_pending());

        assert_eq!(futures.len(), 0);
    }

    #[tokio::test]
    async fn replaced_pending_future_is_polled() {
        let mut streams = FuturesMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 3);

        let (_tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let _ = streams.try_push("ID1", rx1);
        let _ = streams.try_push("ID2", rx2);

        let _ = tx2.send(2);
        let (id, res) = poll_fn(|cx| streams.poll_unpin(cx)).await;
        assert_eq!(id, "ID2");
        assert_eq!(res.unwrap().unwrap(), 2);

        let (new_tx1, new_rx1) = oneshot::channel();
        let replaced = streams.try_push("ID1", new_rx1);
        assert!(matches!(replaced.unwrap_err(), PushError::Replaced(_)));

        let _ = new_tx1.send(4);
        let (id, res) = poll_fn(|cx| streams.poll_unpin(cx)).await;

        assert_eq!(id, "ID1");
        assert_eq!(res.unwrap().unwrap(), 4);
    }

    // Each future causes a delay, `Task` only has a capacity of 1, meaning they must be processed in sequence.
    // We stop after NUM_FUTURES tasks, meaning the overall execution must at least take DELAY * NUM_FUTURES.
    #[tokio::test]
    async fn backpressure() {
        const DELAY: Duration = Duration::from_millis(100);
        const NUM_FUTURES: u32 = 10;

        let start = Instant::now();
        Task::new(DELAY, NUM_FUTURES, 1).await;
        let duration = start.elapsed();

        assert!(duration >= DELAY * NUM_FUTURES);
    }

    #[test]
    fn contains() {
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_secs(10)), 1);
        _ = futures.try_push("ID", pending::<()>());
        assert!(futures.contains("ID"));
        _ = futures.remove("ID");
        assert!(!futures.contains("ID"));
    }

    #[test]
    fn can_iter_typed_futures() {
        const N: usize = 10;
        let mut futures = FuturesMap::new(|| Delay::futures_timer(Duration::from_millis(100)), N);
        let mut sender = Vec::with_capacity(N);
        for i in 0..N {
            let (tx, rx) = oneshot::channel::<()>();
            futures.try_push(format!("ID{i}"), rx).unwrap();
            sender.push(tx);
        }
        assert_eq!(futures.iter_of_type::<oneshot::Receiver<()>>().count(), N);
        for (i, (id, _)) in futures.iter_of_type::<oneshot::Receiver<()>>().enumerate() {
            let expect_id = format!("ID{}", N - i - 1); // Reverse order.
            assert_eq!(id, &expect_id);
        }
        assert!(!sender.iter().any(|tx| tx.is_canceled()));

        for (_, mut rx) in futures.iter_mut_of_type::<oneshot::Receiver<()>>() {
            rx.close();
        }
        assert!(sender.iter().all(|tx| tx.is_canceled()));

        // Deliberately try a non-matching type
        assert_eq!(futures.iter_mut_of_type::<()>().count(), 0);
    }

    struct Task {
        future: Duration,
        num_futures: usize,
        num_processed: usize,
        inner: FuturesMap<u8, ()>,
    }

    impl Task {
        fn new(future: Duration, num_futures: u32, capacity: usize) -> Self {
            Self {
                future,
                num_futures: num_futures as usize,
                num_processed: 0,
                inner: FuturesMap::new(|| Delay::futures_timer(Duration::from_secs(60)), capacity),
            }
        }
    }

    impl Future for Task {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();

            while this.num_processed < this.num_futures {
                if let Poll::Ready((_, result)) = this.inner.poll_unpin(cx) {
                    if result.is_err() {
                        panic!("Timeout is great than future delay")
                    }

                    this.num_processed += 1;
                    continue;
                }

                if let Poll::Ready(()) = this.inner.poll_ready_unpin(cx) {
                    // We push the constant future's ID to prove that user can use the same ID
                    // if the future was finished
                    let maybe_future = this
                        .inner
                        .try_push(1u8, futures_timer::Delay::new(this.future));
                    assert!(maybe_future.is_ok(), "we polled for readiness");

                    continue;
                }

                return Poll::Pending;
            }

            Poll::Ready(())
        }
    }
}
