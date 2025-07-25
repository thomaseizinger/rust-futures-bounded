## Next release

- Allow iterating through future and stream collections.
  See [PR 10](https://github.com/thomaseizinger/rust-futures-bounded/pull/10),
  [PR 11](https://github.com/thomaseizinger/rust-futures-bounded/pull/11)
  and [PR 12](https://github.com/thomaseizinger/rust-futures-bounded/pull/12).

## 0.3.0

- Allow for multiple timer implementations.
  See [PR 5](https://github.com/thomaseizinger/rust-futures-bounded/pull/5).

## 0.2.4

- Add `FuturesMap.contains()`.
  See [PR 3](https://github.com/thomaseizinger/rust-futures-bounded/pull/3).

## 0.2.3

- Introduce `FuturesTupleSet`, holding tuples of a `Future` together with an arbitrary piece of data.
  See [PR 4841](https://github.com/libp2p/rust-libp2p/pull/4841).

## 0.2.2

- Fix an issue where `{Futures,Stream}Map` returns `Poll::Pending` despite being ready after an item has been replaced as part of `try_push`.
  See [PR 4865](https://github.com/libp2p/rust-libp2p/pull/4865).

## 0.2.1

- Add `.len()` getter to `FuturesMap`, `FuturesSet`, `StreamMap` and `StreamSet`.
  See [PR 4745](https://github.com/libp2p/rust-libp2p/pull/4745).

## 0.2.0

- Add `StreamMap` type and remove `Future`-suffix from `PushError::ReplacedFuture` to reuse it for `StreamMap`.
  See [PR 4616](https://github.com/libp2p/rust-libp2p/pull/4616).

## 0.1.0

Initial release.
