use std::any::Any;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::stream::SelectAll;
use futures_util::{stream, FutureExt, Stream, StreamExt};

use crate::{AnyStream, BoxStream, Delay, PushError, Timeout};

/// Represents a map of [`Stream`]s.
///
/// Each stream must finish within the specified time and the map never outgrows its capacity.
pub struct StreamMap<ID, O> {
    make_delay: Box<dyn Fn() -> Delay + Send + Sync>,
    capacity: usize,
    inner: SelectAll<TaggedStream<ID, TimeoutStream<BoxStream<O>>>>,
    empty_waker: Option<Waker>,
    full_waker: Option<Waker>,
}

impl<ID, O> StreamMap<ID, O>
where
    ID: Clone + Unpin,
{
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

impl<ID, O> StreamMap<ID, O>
where
    ID: Clone + PartialEq + Send + Unpin + 'static,
    O: Send + 'static,
{
    /// Push a stream into the map.
    pub fn try_push<F>(&mut self, id: ID, stream: F) -> Result<(), PushError<BoxStream<O>>>
    where
        F: AnyStream<Item = O>,
    {
        if self.inner.len() >= self.capacity {
            return Err(PushError::BeyondCapacity(Box::pin(stream)));
        }

        if let Some(waker) = self.empty_waker.take() {
            waker.wake();
        }

        let old = self.remove(id.clone());
        self.inner.push(TaggedStream::new(
            id,
            TimeoutStream {
                inner: Box::pin(stream),
                timeout: (self.make_delay)(),
            },
        ));

        match old {
            None => Ok(()),
            Some(old) => Err(PushError::Replaced(old)),
        }
    }

    pub fn remove(&mut self, id: ID) -> Option<BoxStream<O>> {
        let tagged = self.inner.iter_mut().find(|s| s.key == id)?;

        let inner = mem::replace(&mut tagged.inner.inner, Box::pin(stream::pending()));
        tagged.exhausted = true; // Setting this will emit `None` on the next poll and ensure `SelectAll` cleans up the resources.

        Some(inner)
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

    pub fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<(ID, Option<Result<O, Timeout>>)> {
        match futures_util::ready!(self.inner.poll_next_unpin(cx)) {
            None => {
                self.empty_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some((id, Some(Ok(output)))) => Poll::Ready((id, Some(Ok(output)))),
            Some((id, Some(Err(dur)))) => {
                self.remove(id.clone()); // Remove stream, otherwise we keep reporting the timeout.

                Poll::Ready((id, Some(Err(Timeout::new(dur)))))
            }
            Some((id, None)) => Poll::Ready((id, None)),
        }
    }

    /// Returns an iterator over all streams whose inner type is `T`.
    ///
    /// If downcasting a stream to `T` fails it will be skipped in the iterator.
    pub fn iter_typed<T>(&self) -> impl Iterator<Item = (&ID, &T)>
    where
        T: 'static,
    {
        self.inner.iter().filter_map(|a| {
            let pin = a.inner.inner.as_ref();
            let any = Pin::into_inner(pin) as &(dyn Any + Send);
            let inner = any.downcast_ref::<T>()?;
            Some((&a.key, inner))
        })
    }

    /// Returns an iterator with mutable access over all streams whose inner type is `T`.
    ///
    /// If downcasting a stream to `T` fails it will be skipped in the iterator.
    pub fn iter_mut_typed<T>(&mut self) -> impl Iterator<Item = (&mut ID, &mut T)>
    where
        T: 'static,
    {
        self.inner.iter_mut().filter_map(|a| {
            let pin = a.inner.inner.as_mut();
            let any = Pin::into_inner(pin) as &mut (dyn Any + Send);
            let inner = any.downcast_mut::<T>()?;
            Some((&mut a.key, inner))
        })
    }
}

struct TimeoutStream<S> {
    inner: S,
    timeout: Delay,
}

impl<F> Stream for TimeoutStream<F>
where
    F: Stream + Unpin,
{
    type Item = Result<F::Item, Duration>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(dur) = self.timeout.poll_unpin(cx) {
            return Poll::Ready(Some(Err(dur)));
        }

        self.inner.poll_next_unpin(cx).map(|a| a.map(Ok))
    }
}

struct TaggedStream<K, S> {
    key: K,
    inner: S,

    exhausted: bool,
}

impl<K, S> TaggedStream<K, S> {
    fn new(key: K, inner: S) -> Self {
        Self {
            key,
            inner,
            exhausted: false,
        }
    }
}

impl<K, S> Stream for TaggedStream<K, S>
where
    K: Clone + Unpin,
    S: Stream + Unpin,
{
    type Item = (K, Option<S::Item>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.exhausted {
            return Poll::Ready(None);
        }

        match futures_util::ready!(self.inner.poll_next_unpin(cx)) {
            Some(item) => Poll::Ready(Some((self.key.clone(), Some(item)))),
            None => {
                self.exhausted = true;

                Poll::Ready(Some((self.key.clone(), None)))
            }
        }
    }
}

#[cfg(all(test, feature = "futures-timer"))]
mod tests {
    use futures::channel::mpsc;
    use futures_util::stream::{once, pending};
    use futures_util::SinkExt;
    use std::future::{poll_fn, ready, Future};
    use std::pin::Pin;
    use std::time::Instant;

    use super::*;

    #[test]
    fn cannot_push_more_than_capacity_tasks() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_secs(10)), 1);

        assert!(streams.try_push("ID_1", once(ready(()))).is_ok());
        matches!(
            streams.try_push("ID_2", once(ready(()))),
            Err(PushError::BeyondCapacity(_))
        );
    }

    #[test]
    fn cannot_push_the_same_id_few_times() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_secs(10)), 5);

        assert!(streams.try_push("ID", once(ready(()))).is_ok());
        matches!(
            streams.try_push("ID", once(ready(()))),
            Err(PushError::Replaced(_))
        );
    }

    #[tokio::test]
    async fn streams_timeout() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 1);

        let _ = streams.try_push("ID", pending::<()>());
        futures_timer::Delay::new(Duration::from_millis(150)).await;
        let (_, result) = poll_fn(|cx| streams.poll_next_unpin(cx)).await;

        assert!(result.unwrap().is_err())
    }

    #[tokio::test]
    async fn timed_out_stream_gets_removed() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 1);

        let _ = streams.try_push("ID", pending::<()>());
        futures_timer::Delay::new(Duration::from_millis(150)).await;
        poll_fn(|cx| streams.poll_next_unpin(cx)).await;

        let poll = streams.poll_next_unpin(&mut Context::from_waker(
            futures_util::task::noop_waker_ref(),
        ));
        assert!(poll.is_pending())
    }

    #[test]
    fn removing_stream() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 1);

        let _ = streams.try_push("ID", stream::once(ready(())));

        {
            let cancelled_stream = streams.remove("ID");
            assert!(cancelled_stream.is_some());
        }

        let poll = streams.poll_next_unpin(&mut Context::from_waker(
            futures_util::task::noop_waker_ref(),
        ));

        assert!(poll.is_pending());
        assert_eq!(
            streams.len(),
            0,
            "resources of cancelled streams are cleaned up properly"
        );
    }

    #[tokio::test]
    async fn replaced_stream_is_still_registered() {
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_millis(100)), 3);

        let (mut tx1, rx1) = mpsc::channel(5);
        let (mut tx2, rx2) = mpsc::channel(5);

        let _ = streams.try_push("ID1", rx1);
        let _ = streams.try_push("ID2", rx2);

        let _ = tx2.send(2).await;
        let _ = tx1.send(1).await;
        let _ = tx2.send(3).await;
        let (id, res) = poll_fn(|cx| streams.poll_next_unpin(cx)).await;
        assert_eq!(id, "ID1");
        assert_eq!(res.unwrap().unwrap(), 1);
        let (id, res) = poll_fn(|cx| streams.poll_next_unpin(cx)).await;
        assert_eq!(id, "ID2");
        assert_eq!(res.unwrap().unwrap(), 2);
        let (id, res) = poll_fn(|cx| streams.poll_next_unpin(cx)).await;
        assert_eq!(id, "ID2");
        assert_eq!(res.unwrap().unwrap(), 3);

        let (mut new_tx1, new_rx1) = mpsc::channel(5);
        let replaced = streams.try_push("ID1", new_rx1);
        assert!(matches!(replaced.unwrap_err(), PushError::Replaced(_)));

        let _ = new_tx1.send(4).await;
        let (id, res) = poll_fn(|cx| streams.poll_next_unpin(cx)).await;

        assert_eq!(id, "ID1");
        assert_eq!(res.unwrap().unwrap(), 4);
    }

    // Each stream emits 1 item with delay, `Task` only has a capacity of 1, meaning they must be processed in sequence.
    // We stop after NUM_STREAMS tasks, meaning the overall execution must at least take DELAY * NUM_STREAMS.
    #[tokio::test]
    async fn backpressure() {
        const DELAY: Duration = Duration::from_millis(100);
        const NUM_STREAMS: u32 = 10;

        let start = Instant::now();
        Task::new(DELAY, NUM_STREAMS, 1).await;
        let duration = start.elapsed();

        assert!(duration >= DELAY * NUM_STREAMS);
    }

    #[test]
    fn can_iter_named_streams() {
        const N: usize = 10;
        let mut streams = StreamMap::new(|| Delay::futures_timer(Duration::from_millis(100)), N);
        let mut sender = Vec::with_capacity(N);
        for i in 0..N {
            let (tx, rx) = mpsc::channel::<()>(1);
            streams.try_push(format!("ID{i}"), rx).unwrap();
            sender.push(tx);
        }
        assert_eq!(streams.iter_typed::<mpsc::Receiver<()>>().count(), N);
        for (i, (id, _)) in streams.iter_typed::<mpsc::Receiver<()>>().enumerate() {
            let expect_id = format!("ID{}", N - i - 1); // Reverse order.
            assert_eq!(id, &expect_id);
        }
        assert!(!sender.iter().any(|tx| tx.is_closed()));

        for (_, rx) in streams.iter_mut_typed::<mpsc::Receiver<()>>() {
            rx.close();
        }
        assert!(sender.iter().all(|tx| tx.is_closed()));
    }

    struct Task {
        item_delay: Duration,
        num_streams: usize,
        num_processed: usize,
        inner: StreamMap<u8, ()>,
    }

    impl Task {
        fn new(item_delay: Duration, num_streams: u32, capacity: usize) -> Self {
            Self {
                item_delay,
                num_streams: num_streams as usize,
                num_processed: 0,
                inner: StreamMap::new(|| Delay::futures_timer(Duration::from_secs(60)), capacity),
            }
        }
    }

    impl Future for Task {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();

            while this.num_processed < this.num_streams {
                match this.inner.poll_next_unpin(cx) {
                    Poll::Ready((_, Some(result))) => {
                        if result.is_err() {
                            panic!("Timeout is great than item delay")
                        }

                        this.num_processed += 1;
                        continue;
                    }
                    Poll::Ready((_, None)) => {
                        continue;
                    }
                    _ => {}
                }

                if let Poll::Ready(()) = this.inner.poll_ready_unpin(cx) {
                    // We push the constant ID to prove that user can use the same ID if the stream was finished
                    let maybe_future = this
                        .inner
                        .try_push(1u8, once(futures_timer::Delay::new(this.item_delay)));
                    assert!(maybe_future.is_ok(), "we polled for readiness");

                    continue;
                }

                return Poll::Pending;
            }

            Poll::Ready(())
        }
    }
}
