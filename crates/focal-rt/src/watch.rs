//! A value watched across threads, as tokio's `sync::watch` was: a sender publishes it from any
//! thread (an owner on its own thread, a task), any number of receivers read the latest from any
//! thread, and a receiver waits for the next change as a hyper-rt task. A sender is cloned for
//! another publisher; the value's word closes as the last of them goes.
//!
//! The value is behind a lock; its version is a hyper-rt watched word, bumped by each change and
//! closed as the sender goes. A receiver is a handle, cloned freely with whatever holds it; it takes
//! one of the word's waiting places only while it waits, so at most `waiters` waits are under way at
//! once, and a wait past them is refused `Capacity`, typed. The value is shared by the sender and
//! its receivers across threads, which is what the reference count is for.
//!
//! A lock is held only to read or to change the value, never across a wait, and no closure given it
//! may panic (focal's code does not); a lock found poisoned is taken as it stands, since a change is
//! a whole replacement or a closure's whole edit.
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use hyper_rt::sync::{SyncError, WatchReceiver, WatchSender};

use crate::RtError;

/// The waits a value made by [`channel`] keeps at once.
pub const WAITERS: usize = 64;

struct Shared<T> {
    value: RwLock<T>,
    /// The version word; `None` once the sender has gone, its word closed.
    word: Mutex<Option<WatchSender>>,
    /// A receiver of the word that never waits: each wait's place is cloned from it.
    template: Mutex<WatchReceiver>,
    /// The senders still live; the last to go closes the word.
    senders: AtomicUsize,
}

/// A publishing half: clone it for another publisher.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// A reading half: clone it for another reader; each tracks the version it last saw.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    seen: u64,
}

/// Why a wait for a change ended without one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecvError {
    /// The sender has gone and nothing is new.
    #[error("the watched value's sender has gone")]
    Closed,
    /// Every waiting place is taken, or the wait is off a shard: no wake could be kept.
    #[error("no wait for the watched value could be kept")]
    Refused,
}

/// The latest value, read under the lock while the guard lives.
pub struct Ref<'a, T>(RwLockReadGuard<'a, T>);

impl<T> Deref for Ref<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// A value starting at `initial`, at most [`WAITERS`] waits at once. Refused `Capacity` at the
/// runtime's cell bound.
pub fn channel<T>(initial: T) -> Result<(Sender<T>, Receiver<T>), RtError> {
    channel_with(initial, WAITERS)
}

/// A value starting at `initial`, at most `waiters` waits at once (one at least).
pub fn channel_with<T>(initial: T, waiters: usize) -> Result<(Sender<T>, Receiver<T>), RtError> {
    // The template takes one of the word's places for good; the waits share the rest.
    let places = waiters.max(1).saturating_add(1);
    let (word, template) = hyper_rt::sync::watch(0, places)?;
    let shared = Arc::new(Shared {
        value: RwLock::new(initial),
        word: Mutex::new(Some(word)),
        template: Mutex::new(template),
        senders: AtomicUsize::new(1),
    });
    Ok((
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared, seen: 0 },
    ))
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}
fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
fn hold<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl<T> Sender<T> {
    /// Publishes `value`, waking every wait.
    pub fn send(&self, value: T) {
        *write(&self.shared.value) = value;
        self.bump();
    }
    /// Publishes `value` and gives back the one it replaces.
    pub fn send_replace(&self, value: T) -> T {
        let previous = std::mem::replace(&mut *write(&self.shared.value), value);
        self.bump();
        previous
    }
    /// Changes the value in place, waking every wait.
    pub fn send_modify(&self, modify: impl FnOnce(&mut T)) {
        modify(&mut write(&self.shared.value));
        self.bump();
    }
    /// Changes the value in place; waits are woken only when `modify` says it changed.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        let modified = modify(&mut write(&self.shared.value));
        if modified {
            self.bump();
        }
        modified
    }
    /// The latest value.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(read(&self.shared.value))
    }
    /// The receivers alive now: a snapshot, as another thread may clone or drop one at once.
    pub fn receiver_count(&self) -> usize {
        Arc::strong_count(&self.shared)
            .saturating_sub(self.shared.senders.load(Ordering::Acquire))
    }
    /// A receiver that has seen the latest version.
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver {
            shared: self.shared.clone(),
            seen: self.version(),
        }
    }
    fn version(&self) -> u64 {
        hold(&self.shared.word)
            .as_ref()
            .map_or(0, WatchSender::borrow)
    }
    fn bump(&self) {
        if let Some(word) = hold(&self.shared.word).as_mut() {
            let next = word.borrow().wrapping_add(1);
            word.send(next);
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        // Each live sender is a value in memory, so the count cannot reach its end.
        let _ = self
            .shared
            .senders
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                Some(live.saturating_add(1))
            });
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let before = self
            .shared
            .senders
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                Some(live.saturating_sub(1))
            });
        // The word closes as its last sender drops: every wait sees the end.
        if matches!(before, Ok(1) | Ok(0)) {
            drop(hold(&self.shared.word).take());
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            seen: self.seen,
        }
    }
}

impl<T> Receiver<T> {
    /// The latest value; what this receiver has seen is unchanged.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref(read(&self.shared.value))
    }
    /// The latest value, marked seen.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.seen = hold(&self.shared.template).borrow();
        Ref(read(&self.shared.value))
    }
    /// Waits for a version this receiver has not seen, and marks it seen: at once for one already
    /// published. `Closed` once the sender has gone with nothing new; `Refused` when no waiting place
    /// is free or the wait is off a shard.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        let mut waiting = hold(&self.shared.template)
            .try_clone()
            .map_err(|_| RecvError::Refused)?;
        loop {
            let latest = waiting.borrow_and_update();
            if latest != self.seen {
                self.seen = latest;
                return Ok(());
            }
            match waiting.changed().await {
                Ok(()) => {}
                Err(SyncError::NotOnShardThread(())) => return Err(RecvError::Refused),
                Err(_) => return Err(RecvError::Closed),
            }
        }
    }
    /// Whether the sender has gone.
    pub fn is_closed(&self) -> bool {
        hold(&self.shared.word).is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A change published from a plain thread wakes a task waiting on a shard, each version is seen
    /// once, and the sender's end ends a wait with nothing new.
    #[test]
    fn a_change_from_a_plain_thread_wakes_a_shard_task_once_and_the_end_closes_its_wait() {
        let (sender, mut receiver) = channel(1u32).unwrap();
        let publisher = std::thread::spawn(move || {
            sender.send(2);
            sender.send_modify(|value| *value += 1);
            assert!(!sender.send_if_modified(|_| false));
            sender
        });
        let sender = publisher.join().unwrap();
        let seen = crate::block_on(async move {
            receiver.changed().await.unwrap();
            let seen = *receiver.borrow();
            drop(sender);
            let ended = receiver.changed().await;
            (seen, ended)
        })
        .unwrap();
        assert_eq!(seen, (3, Err(RecvError::Closed)));
    }

    /// A cloned sender publishes as the first does, and the word closes only as the last sender
    /// goes.
    #[test]
    fn the_word_closes_as_the_last_sender_goes() {
        let (first, mut receiver) = channel(0u32).unwrap();
        let second = first.clone();
        let outcome = crate::block_on(async move {
            drop(first);
            second.send(7);
            receiver.changed().await.unwrap();
            let seen = *receiver.borrow();
            let open = !receiver.is_closed();
            drop(second);
            (seen, open, receiver.changed().await, receiver.is_closed())
        })
        .unwrap();
        assert_eq!(outcome, (7, true, Err(RecvError::Closed), true));
    }

    /// A wait takes a place only while it waits: clones are free, and a wait past the places is
    /// refused typed.
    #[test]
    fn clones_are_free_and_waits_past_the_places_are_refused() {
        let (sender, receiver) = channel_with(0u8, 1).unwrap();
        let readers: Vec<_> = (0..256).map(|_| receiver.clone()).collect();
        assert_eq!(readers.len(), 256);
        let outcome = crate::block_on(async move {
            let mut first = receiver.clone();
            let mut second = receiver.clone();
            let waiting = hyper_rt::combine::race2(first.changed(), async {
                // While the first waits, its place is taken: the second is refused at once.
                second.changed().await
            })
            .await;
            drop(sender);
            match waiting {
                hyper_rt::combine::Either::Second(refused) => refused,
                hyper_rt::combine::Either::First(_) => Ok(()),
            }
        })
        .unwrap();
        assert_eq!(outcome, Err(RecvError::Refused));
    }
}
