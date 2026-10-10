//! The waiter handoff of a synchronization cell (docs/runtime.md §8; mantle's final review of hyper-rt,
//! second pass, finding 2), apart and generic over the atomic so loom drives this exact code.
//!
//! A waiter registers its task word, then checks the state; a publisher changes the state, then takes the
//! word and wakes it. Registration was a plain `store(Release)` and the check an `Acquire` load. That is
//! the store-buffer pattern: each side's load may read the value from before the other's write (x86 does
//! this with store-to-load reordering), so the waiter reads the old state and the publisher reads no
//! waiter, and the waiter sleeps through the change.
//!
//! Registration is now a `swap(AcqRel)` on the same word the publisher swaps, so the two read-modify-writes
//! are ordered. Either the publisher's swap reads the waiter's word and wakes it, or the waiter's swap
//! reads the publisher's (which released the state change before it), and the waiter's check then sees
//! the change.

use std::sync::atomic::Ordering;

/// Format: "no task waits" (no task word is all ones; see `sync::cell`).
pub(crate) const NO_WAITER: u64 = u64::MAX;

/// The atomic word a waiter is handed over in: std's in the runtime, loom's in its model.
pub(crate) trait WaiterWord {
    /// An atomic swap.
    fn swap_word(&self, value: u64, order: Ordering) -> u64;
    /// An atomic store.
    fn store_word(&self, value: u64, order: Ordering);
}

impl WaiterWord for std::sync::atomic::AtomicU64 {
    fn swap_word(&self, value: u64, order: Ordering) -> u64 {
        self.swap(value, order)
    }
    fn store_word(&self, value: u64, order: Ordering) {
        self.store(value, order);
    }
}

#[cfg(loom)]
impl WaiterWord for loom::sync::atomic::AtomicU64 {
    fn swap_word(&self, value: u64, order: Ordering) -> u64 {
        self.swap(value, order)
    }
    fn store_word(&self, value: u64, order: Ordering) {
        self.store(value, order);
    }
}

/// Records `word` as the task to wake: a read-modify-write, ordered against the publisher's
/// [`take_waiter`] (module doc).
pub(crate) fn register_waiter(waiter: &impl WaiterWord, word: u64) {
    waiter.swap_word(word, Ordering::AcqRel);
}

/// Takes the waiting task's word, if one waits: the publisher's half, after its state change.
pub(crate) fn take_waiter(waiter: &impl WaiterWord) -> Option<u64> {
    let word = waiter.swap_word(NO_WAITER, Ordering::AcqRel);
    (word != NO_WAITER).then_some(word)
}

/// Forgets the waiting task, if any.
pub(crate) fn clear_waiter(waiter: &impl WaiterWord) {
    waiter.store_word(NO_WAITER, Ordering::Release);
}

#[cfg(loom)]
#[cfg_attr(
    loom,
    allow(clippy::unwrap_used, clippy::disallowed_types, clippy::panic)
)]
mod loom_tests {
    // `cfg(loom)` only (D-8 exception 3, a test harness): loom's `Arc` shares the model's cells.
    use super::*;
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicU64;

    /// Format: the waiter's task word in the model.
    const WORD: u64 = 7;

    /// The second pass's finding 2, exactly as a watched word's receiver and sender do it. Do: a receiver
    /// registers its word and then checks the version (`aux`, Acquire), against a sender that bumps the
    /// version (`fetch_add`, AcqRel) and then takes the waiter, every interleaving. Expect: the receiver
    /// either sees the new version or is woken — never neither.
    #[test]
    fn a_waiter_either_sees_the_change_or_is_woken() {
        loom::model(|| {
            let waiter = Arc::new(AtomicU64::new(NO_WAITER));
            let version = Arc::new(AtomicU64::new(0));
            let sender = {
                let (waiter, version) = (Arc::clone(&waiter), Arc::clone(&version));
                loom::thread::spawn(move || {
                    version.fetch_add(1, Ordering::AcqRel);
                    take_waiter(&*waiter)
                })
            };
            register_waiter(&*waiter, WORD);
            let saw = version.load(Ordering::Acquire) != 0;
            let woke = sender.join().unwrap() == Some(WORD);
            assert!(saw || woke, "the receiver slept through the change");
        });
    }
}
