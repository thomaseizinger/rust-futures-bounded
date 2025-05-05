use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::stream::{select_all, BoxStream, SelectAll};
use futures_util::{FutureExt, Stream, StreamExt};

use crate::{Delay, PushError, Timeout};

/// Represents a map of [`Stream`]s.
///
/// Each stream must finish within the specified time and the map never outgrows its capacity.
pub struct StreamMap<ID, O>(StreamMapIterable<ID, BoxStream<'static, O>>);

impl<ID, O> StreamMap<ID, O>
where
    ID: Clone + Unpin,
{
    pub fn new(make_delay: impl Fn() -> Delay + Send + Sync + 'static, capacity: usize) -> Self {
        Self(StreamMapIterable::new(make_delay, capacity))
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
        F: Stream<Item = O> + Send + 'static,
    {
        self.0.try_push(id, stream.boxed())
    }

    pub fn remove(&mut self, id: ID) -> Option<BoxStream<O>> {
        self.0.remove(id)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[allow(unknown_lints, clippy::needless_pass_by_ref_mut)] // &mut Context is idiomatic.
    pub fn poll_ready_unpin(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.0.poll_ready_unpin(cx)
    }

    pub fn poll_next_unpin(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<(ID, Option<Result<O, Timeout>>)> {
        self.0.poll_next_unpin(cx)
    }
}

/// Iterable variant of [`StreamMap`] without boxed streams.
pub struct StreamMapIterable<ID, F> {
    make_delay: Box<dyn Fn() -> Delay + Send + Sync>,
    capacity: usize,
    inner: SelectAll<TaggedStream<ID, TimeoutStream<F>>>,
    empty_waker: Option<Waker>,
    full_waker: Option<Waker>,
}

impl<ID, F> StreamMapIterable<ID, F>
where
    ID: Clone + Unpin,
    F: Stream + Unpin,
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

impl<ID, F> StreamMapIterable<ID, F>
where
    ID: Clone + PartialEq + Send + Unpin + 'static,
    F: Stream + Unpin,
{
    /// Push a stream into the map.
    pub fn try_push(&mut self, id: ID, stream: F) -> Result<(), PushError<F>>
    where
        F: Stream + Send + 'static,
    {
        if self.inner.len() >= self.capacity {
            return Err(PushError::BeyondCapacity(stream));
        }

        if let Some(waker) = self.empty_waker.take() {
            waker.wake();
        }

        let old = self.remove(id.clone());
        self.inner.push(TaggedStream::new(
            id,
            TimeoutStream {
                inner: stream,
                timeout: (self.make_delay)(),
            },
        ));

        match old {
            None => Ok(()),
            Some(old) => Err(PushError::Replaced(old)),
        }
    }

    pub fn remove(&mut self, id: ID) -> Option<F> {
        let tagged = self.inner.iter_mut().find(|s| s.key == id)?;
        let inner = tagged.inner.take()?; // `TaggedStream` will emit `None` on the next poll and ensure `SelectAll` cleans up the resources.
        Some(inner.inner)
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
    ) -> Poll<(ID, Option<Result<F::Item, Timeout>>)> {
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

    pub fn iter(&self) -> Iter<ID, F> {
        Iter(self.inner.iter())
    }

    pub fn iter_mut(&mut self) -> IterMut<ID, F> {
        IterMut(self.inner.iter_mut())
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
    inner: Option<S>,
}

impl<K, S> TaggedStream<K, S> {
    fn new(key: K, inner: S) -> Self {
        Self {
            key,
            inner: Some(inner),
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
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };

        match futures_util::ready!(inner.poll_next_unpin(cx)) {
            Some(item) => Poll::Ready(Some((self.key.clone(), Some(item)))),
            None => {
                self.inner.take();
                Poll::Ready(Some((self.key.clone(), None)))
            }
        }
    }
}

pub struct Iter<'a, ID: Unpin, F: Unpin>(select_all::Iter<'a, TaggedStream<ID, TimeoutStream<F>>>);

impl<'a, ID, F> Iterator for Iter<'a, ID, F>
where
    ID: Clone + Unpin,
    F: Unpin + Stream,
{
    type Item = (ID, &'a F);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;
        Some((next.key.clone(), &next.inner.as_ref()?.inner))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

pub struct IterMut<'a, ID: Unpin, F: Unpin>(
    select_all::IterMut<'a, TaggedStream<ID, TimeoutStream<F>>>,
);

impl<'a, ID, F> Iterator for IterMut<'a, ID, F>
where
    ID: Clone + Unpin,
    F: Unpin + Stream,
{
    type Item = (ID, &'a mut F);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;
        Some((next.key.clone(), &mut next.inner.as_mut()?.inner))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
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

        let _ = streams.try_push("ID", once(ready(())));

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

    #[test]
    fn iterating_streams() {
        const N: usize = 10;
        let mut streams =
            StreamMapIterable::new(|| Delay::futures_timer(Duration::from_millis(100)), N);
        let mut sender = Vec::with_capacity(N);
        for i in 0..N {
            let (tx, rx) = mpsc::channel::<()>(1);
            let _ = streams.try_push(i, rx);
            sender.push(tx);
        }
        assert_eq!(streams.iter().count(), N);
        for (i, (id, _)) in streams.iter().enumerate() {
            let expect_id = N - i - 1; // Reverse order.
            assert_eq!(id, expect_id);
        }
        assert!(!sender.iter().any(|tx| tx.is_closed()));

        for (_, rx) in streams.iter_mut() {
            rx.close();
        }
        assert!(sender.iter().all(|tx| tx.is_closed()));
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
