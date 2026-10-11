//! Waits around a future that hold it once. Each is a named future over the future it is given,
//! never an `async` layer that awaits it: an `async fn` or block awaiting a future it was handed
//! holds it twice, the argument beside the local its `.await` moves it into (rust-lang/rust#62958;
//! hyper-raft's docs/runtime.md §3.8). In the `focal` binary six such layers held what they wrapped
//! twice, three of them nested around each branch of an MCP call (32,608 bytes, eight times the
//! work).
//!
//! Each gives its output once: polled again after it, it waits rather than poll a finished
//! future, which an `async` block refuses by unwinding.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// `future`'s output, as `map` makes it.
pub fn map<F, M, T>(future: F, map: M) -> Map<F, M>
where
    F: Future,
    M: FnOnce(F::Output) -> T,
{
    Map {
        future,
        map: Some(map),
    }
}

/// `future`'s output beside `key`: how a set of futures answering together names each answer.
pub fn keyed<K, F: Future>(key: K, future: F) -> Keyed<K, F> {
    Keyed {
        future,
        key: Some(key),
    }
}

/// `future`, an unwind out of what it polls contained: `unwound`'s output in place of its own, the
/// future never polled again (CLAUDE.md §1: the boundary is the last fence, never the fix).
pub fn contained<F, U>(future: F, unwound: U) -> Contained<F, U>
where
    F: Future,
    U: FnOnce() -> F::Output,
{
    Contained {
        future,
        unwound: Some(unwound),
    }
}

/// `future`, polled only on a shard's thread, whose timers and sockets it needs: off one, `off`'s
/// output, before the future is polled at all.
pub fn on_shard<F, O>(future: F, off: O) -> OnShard<F, O>
where
    F: Future,
    O: FnOnce() -> F::Output,
{
    OnShard {
        future,
        off: Some(off),
    }
}

pin_project_lite::pin_project! {
    /// The wait of [`map`].
    #[must_use = "a wait does nothing unless awaited"]
    pub struct Map<F, M> {
        #[pin]
        future: F,
        // Taken with the output: `None` once it is given.
        map: Option<M>,
    }
}

impl<F, M, T> Future for Map<F, M>
where
    F: Future,
    M: FnOnce(F::Output) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.project();
        if this.map.is_none() {
            return Poll::Pending;
        }
        let Poll::Ready(output) = this.future.poll(cx) else {
            return Poll::Pending;
        };
        match this.map.take() {
            Some(map) => Poll::Ready(map(output)),
            None => Poll::Pending,
        }
    }
}

impl<F, M> std::fmt::Debug for Map<F, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map")
            .field("given", &self.map.is_none())
            .finish_non_exhaustive()
    }
}

pin_project_lite::pin_project! {
    /// The wait of [`keyed`].
    #[must_use = "a wait does nothing unless awaited"]
    pub struct Keyed<K, F> {
        #[pin]
        future: F,
        // Handed on with the output: `None` once it is given.
        key: Option<K>,
    }
}

impl<K, F: Future> Future for Keyed<K, F> {
    type Output = (K, F::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.key.is_none() {
            return Poll::Pending;
        }
        let Poll::Ready(output) = this.future.poll(cx) else {
            return Poll::Pending;
        };
        match this.key.take() {
            Some(key) => Poll::Ready((key, output)),
            None => Poll::Pending,
        }
    }
}

impl<K, F> std::fmt::Debug for Keyed<K, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyed")
            .field("given", &self.key.is_none())
            .finish_non_exhaustive()
    }
}

pin_project_lite::pin_project! {
    /// The wait of [`contained`].
    #[must_use = "a wait does nothing unless awaited"]
    pub struct Contained<F, U> {
        #[pin]
        future: F,
        // `None` once the output is given, the future's or the unwind's.
        unwound: Option<U>,
    }
}

impl<F, U> Future for Contained<F, U>
where
    F: Future,
    U: FnOnce() -> F::Output,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let mut this = self.project();
        if this.unwound.is_none() {
            return Poll::Pending;
        }
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            this.future.as_mut().poll(cx)
        }));
        match polled {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => {
                *this.unwound = None;
                Poll::Ready(output)
            }
            Err(_) => match this.unwound.take() {
                Some(unwound) => Poll::Ready(unwound()),
                None => Poll::Pending,
            },
        }
    }
}

impl<F, U> std::fmt::Debug for Contained<F, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Contained")
            .field("given", &self.unwound.is_none())
            .finish_non_exhaustive()
    }
}

pin_project_lite::pin_project! {
    /// The wait of [`on_shard`].
    #[must_use = "a wait does nothing unless awaited"]
    pub struct OnShard<F, O> {
        #[pin]
        future: F,
        // `None` once the output is given, the future's or the refusal's.
        off: Option<O>,
    }
}

impl<F, O> Future for OnShard<F, O>
where
    F: Future,
    O: FnOnce() -> F::Output,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        if this.off.is_none() {
            return Poll::Pending;
        }
        if hyper_rt::futures::shard_id().is_none() {
            return match this.off.take() {
                Some(off) => Poll::Ready(off()),
                None => Poll::Pending,
            };
        }
        let Poll::Ready(output) = this.future.poll(cx) else {
            return Poll::Pending;
        };
        *this.off = None;
        Poll::Ready(output)
    }
}

impl<F, O> std::fmt::Debug for OnShard<F, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnShard")
            .field("given", &self.off.is_none())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;

    /// A future holding 16 KiB across its wait, as a request's state does.
    fn heavy() -> impl Future<Output = u8> {
        let state = [7u8; 16 * 1024];
        async move {
            hyper_rt::futures::yield_now().await;
            state.iter().fold(0u8, |sum, byte| sum.wrapping_add(*byte))
        }
    }

    #[test]
    fn each_wait_holds_its_future_once() {
        let one = size_of_val(&heavy());
        let own = 64;
        let off = || 0u8;
        for (at, size) in [
            size_of_val(&map(heavy(), u32::from)),
            size_of_val(&keyed(7u64, heavy())),
            size_of_val(&contained(heavy(), off)),
            size_of_val(&on_shard(heavy(), off)),
            size_of_val(&on_shard(contained(map(heavy(), |out| out), off), off)),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(size <= one + own, "wait {at}: {size} bytes around {one}");
        }
    }

    #[test]
    fn each_wait_gives_its_output_once_and_then_waits() {
        crate::block_on(async {
            let mut mapped = pin!(map(async { 2u8 }, |two| u32::from(two) * 10));
            let mut keyed = pin!(keyed("seven", async { 7u8 }));
            let mut kept = pin!(contained(async { 3u8 }, || 0));
            let mut shard = pin!(on_shard(async { 4u8 }, || 0));
            std::future::poll_fn(|cx| {
                assert_eq!(mapped.as_mut().poll(cx), Poll::Ready(20));
                assert_eq!(keyed.as_mut().poll(cx), Poll::Ready(("seven", 7)));
                assert_eq!(kept.as_mut().poll(cx), Poll::Ready(3));
                assert_eq!(shard.as_mut().poll(cx), Poll::Ready(4));
                assert!(mapped.as_mut().poll(cx).is_pending());
                assert!(keyed.as_mut().poll(cx).is_pending());
                assert!(kept.as_mut().poll(cx).is_pending());
                assert!(shard.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        })
        .unwrap();
    }

    #[test]
    fn an_unwind_is_contained_and_the_future_never_polled_again() {
        let polls = std::cell::Cell::new(0u32);
        let unwound = crate::block_on(async move {
            let polls = &polls;
            let mut kept = pin!(contained(
                std::future::poll_fn(|_| -> Poll<u8> {
                    polls.set(polls.get() + 1);
                    panic!("a dependency unwinds")
                }),
                || 9
            ));
            let first = std::future::poll_fn(|cx| kept.as_mut().poll(cx)).await;
            std::future::poll_fn(|cx| {
                assert!(kept.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            (first, polls.get())
        })
        .unwrap();
        assert_eq!(unwound, (9, 1));
    }

    #[test]
    fn off_a_shard_the_refusal_comes_before_the_future_is_polled() {
        let polled = std::cell::Cell::new(false);
        let mut refused = pin!(on_shard(
            std::future::poll_fn(|_| {
                polled.set(true);
                Poll::Ready(1u8)
            }),
            || 0
        ));
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert_eq!(refused.as_mut().poll(&mut context), Poll::Ready(0));
        assert!(!polled.get());
        assert!(refused.as_mut().poll(&mut context).is_pending());
    }
}
