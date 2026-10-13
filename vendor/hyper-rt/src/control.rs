//! Control messages to a shard: spawn, cancel, shutdown, and the queue they travel in. A message never
//! travels over the wake bitmap, because a spawn carries a boxed future and a wake is a word (§4.3). The
//! queue is control, not a data path: a spawn is admission, and a push refuses instead of blocking when
//! the bound is reached (the bound is the shard's admission limit, since nothing beyond it could be
//! admitted anyway).
//!
//! **The queue** is a bounded ring (`crate::sync::ring`) held in the shard's registry entry, any thread
//! pushing and the shard alone taking. The shard learns of a message from its entry's control flag and
//! kick, never from a waiter, so the queue has no cell and wakes no one: a channel's sender held in the
//! entry would wake through the registry as it dropped, inside the free of the entry by its last reader
//! (`crate::retire`), a call cycle `scripts/check-recursion.py` refuses. It was std's `sync_channel`, whose
//! lock on macOS is a pthread mutex boxed at its first lock: a shard losing its driver allocated as it
//! closed it (`tests/service_resources.rs`).
//!
//! **Closing** drops what is queued and refuses what follows. The shard closes the ring (its mark in the
//! tail, so a push that has not claimed its place is refused with its value) and drains it; a sender whose
//! push landed fences and looks at the mark. Both fences are sequentially consistent, so of the two threads
//! at least one sees the other's write (the store-buffering outcome both fences forbid): the shard's drain
//! finds the message, or the sender finds the mark and drains for itself. No message is left in a closed
//! queue, so none waits for the entry's free to answer its receipt.

#[cfg(loom)]
use loom::sync::atomic::{Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{Ordering, fence};

use crate::error::RtError;
use crate::mem::Encoded;
use crate::sync::ring::{Refused, Ring};
use crate::task::SpawnRequest;

/// A control message.
#[derive(Debug)]
pub enum Control {
    /// Take ownership of a spawn request (a boxed future and its placement).
    Spawn(Box<SpawnRequest>),
    /// Admit a service whose cancellation is completed by its own future. The request must
    /// carry only cold bootstrap ownership until its admission receipt is answered.
    SpawnService(Box<SpawnRequest>),
    /// Cancel the task named by the packed word.
    Cancel(Encoded),
    /// Finish every task and exit the loop.
    Shutdown,
}

/// A shard's control queue (the module doc).
#[derive(Debug)]
pub(crate) struct Queue<T> {
    ring: Ring<T>,
}

/// What a push did.
#[derive(Debug)]
pub(crate) enum Pushed<T> {
    /// Queued for the shard.
    Queued,
    /// The queue holds its bound: the value comes back.
    Full(T),
    /// The queue was closed: the value comes back.
    Closed(T),
    /// The queue closed as the value went in, and what was left in it is dropped, as a close drops what
    /// is queued: the shard took the value first, or its drop answered its receipt.
    Dropped,
}

impl<T> Queue<T> {
    /// An open queue holding at most `bound` values; refused `BadConfig` as the ring refuses it.
    pub(crate) fn new(bound: usize) -> Result<Self, RtError> {
        Ok(Self {
            ring: Ring::new(bound)?,
        })
    }

    /// Adds `value` for the shard, from any thread.
    pub(crate) fn push(&self, value: T) -> Pushed<T> {
        match self.ring.push(value) {
            Ok(()) => {
                fence(Ordering::SeqCst);
                if self.ring.is_closed() {
                    self.ring.drain();
                    return Pushed::Dropped;
                }
                Pushed::Queued
            }
            Err(Refused::Full(value)) => Pushed::Full(value),
            Err(Refused::Closed(value)) => Pushed::Closed(value),
        }
    }

    /// The next value while the queue is open: the shard's drain.
    pub(crate) fn take(&self) -> Option<T> {
        if self.ring.is_closed() {
            return None;
        }
        self.ring.pop()
    }

    /// Closes the queue and drops what it holds: the shard's, at its exit, its driver's loss and its drop
    /// (again, it drops what a late sender left).
    pub(crate) fn close(&self) {
        self.ring.close();
        self.ring.drain();
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::mem::loom_bounds;

    /// A sender pushes once while the shard takes once and then closes, each a thread of its own, on a queue
    /// of one place; once both have joined, the queue holds nothing. The value was taken by the shard, or
    /// dropped by the close or by its sender, and a push the close refused never went in. Without the
    /// sender's look at the mark after its push, or without either fence, an interleaving leaves the value
    /// in the closed queue (the mutations recorded with this model's run).
    #[test]
    fn a_value_pushed_as_the_shard_closes_is_taken_or_dropped_never_left() {
        loom_bounds::explore("control queue: a push against a take and a close", || {
            let queue: &'static Queue<u32> = Box::leak(Box::new(Queue::new(1).unwrap()));
            let sender = loom::thread::spawn(move || queue.push(7));
            let shard = loom::thread::spawn(move || {
                let taken = queue.take();
                queue.close();
                taken
            });
            let pushed = sender.join().unwrap();
            let taken = shard.join().unwrap();
            assert!(
                queue.ring.pop().is_none(),
                "a value was left in the closed queue"
            );
            assert!(
                !matches!(pushed, Pushed::Full(_)),
                "a queue of one place the shard takes from refused the push as full"
            );
            if let Some(value) = taken {
                assert_eq!(value, 7);
                assert!(matches!(pushed, Pushed::Queued | Pushed::Dropped));
            }
        });
    }
}
