//! An entry shared with counted readers and retired by the last of them (docs/runtime.md §4.3; mantle's
//! review of hyper-rt, finding 10b): the registry slot's protocol, apart so loom can drive it.
//!
//! A reader pins the entry for the span of one call: it counts itself in `pins`, then loads the pointer.
//! Retirement swaps the pointer to null, parks it in `retired`, and marks `pins` [`RETIRED`]; whoever
//! then finds the count at zero with the mark set — the retirement itself, or the reader whose unpin
//! leaves exactly the mark — claims the free with one `compare_exchange` from the mark to zero and runs
//! the caller's finish on the entry. Nothing waits: retirement used to spin with `yield_now` until the
//! count reached zero, which a descheduled reader stretched without bound and a retirement from inside a
//! reader's own call never ended.
//!
//! **Why the free is safe**: every operation here is `SeqCst`, so the pin's increment, the reader's load,
//! the swap and the mark sit in one total order. A reader counted before the mark keeps the count above
//! zero until its unpin, so no claim succeeds while it holds the entry; a reader counted after the mark
//! loads after the swap, so it sees null and holds nothing. One claim succeeds per retirement: it clears
//! the mark, and a later one finds no mark to clear.
//!
//! **Bounds**: the count is the readers inside a call at once, each a thread's stack frame (a nested
//! read pins twice), far below the mark's bit; a retirement takes its pointer once and frees it once.

// The pin protocol's raw pointer: the one `unsafe` this module needs (scripts/check-contracts.py).
#![allow(unsafe_code)]

#[cfg(loom)]
use loom::sync::atomic::{AtomicPtr, AtomicU32, Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering, fence};

/// Format: the bit of `pins` that marks a retirement waiting for its last reader. The count below it is
/// readers inside a call at once (threads' frames), which never reach 2³¹.
const RETIRED: u32 = 1 << 31;

/// An entry published once, read under counted pins, and freed by the last reader of its retirement.
#[derive(Debug)]
pub(crate) struct Pinned<T> {
    entry: AtomicPtr<T>,
    pins: AtomicU32,
    retired: AtomicPtr<T>,
}

impl<T> Pinned<T> {
    /// Nothing published, no reader, no retirement.
    #[cfg(not(loom))]
    pub(crate) const fn new() -> Pinned<T> {
        Pinned {
            entry: AtomicPtr::new(std::ptr::null_mut()),
            pins: AtomicU32::new(0),
            retired: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Nothing published, no reader, no retirement (loom's atomics are not `const`).
    #[cfg(loom)]
    pub(crate) fn new() -> Pinned<T> {
        Pinned {
            entry: AtomicPtr::new(std::ptr::null_mut()),
            pins: AtomicU32::new(0),
            retired: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Publishes `entry`. The caller holds the right to publish: nothing is published and no retirement
    /// is pending (the registry's claimed generation, which a retirement frees only after its finish).
    pub(crate) fn publish(&self, entry: Box<T>) {
        self.entry.store(Box::into_raw(entry), Ordering::SeqCst);
    }

    /// The published pointer, unpinned: for the entry's owner, whose own reference keeps it from
    /// retirement (the registry's shard thread). Null when nothing is published.
    pub(crate) fn unguarded(&self) -> *mut T {
        self.entry.load(Ordering::SeqCst)
    }

    /// Runs `f` on the published entry, pinned for the call; `None` when nothing is published. When this
    /// reader is the last of a retirement, `finish` receives the entry after the call (on an unwind too).
    pub(crate) fn read<R>(
        &self,
        f: impl FnOnce(&T) -> R,
        finish: impl FnOnce(Box<T>),
    ) -> Option<R> {
        self.pins.fetch_add(1, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let _pin = Pin {
            pinned: self,
            finish: Some(finish),
        };
        let entry = self.entry.load(Ordering::SeqCst);
        if entry.is_null() {
            return None;
        }
        // SAFETY: a non-null pointer here came from `Box::into_raw` in `publish`. This reader was counted
        // before the load, in the total order of the module's `SeqCst` operations: if a retirement's swap
        // came first the load saw null, and otherwise its mark found this pin counted, so no claim frees the
        // entry before `_pin` unpins after `f` returns (the module's argument).
        Some(f(unsafe { &*entry }))
    }

    /// Retires the published entry: `finish` receives it now when no reader holds it, or from the last
    /// reader's unpin. With nothing published — a retirement of this publication already under way or done —
    /// it does nothing: the swap that takes the pointer is the one claim on the retirement, so a second
    /// `retire` never finishes a second time (mantle's final review, finding 5: it used to finish with
    /// nothing, and the registry published the slot free while a reader still held the entry).
    pub(crate) fn retire(&self, finish: impl FnOnce(Box<T>)) {
        let entry = self.entry.swap(std::ptr::null_mut(), Ordering::SeqCst);
        if entry.is_null() {
            return;
        }
        self.retired.store(entry, Ordering::SeqCst);
        self.pins.fetch_or(RETIRED, Ordering::SeqCst);
        if let Some(entry) = self.claim() {
            finish(entry);
        }
    }

    /// The parked entry, for the one caller whose `compare_exchange` takes the mark away at a zero count.
    fn claim(&self) -> Option<Box<T>> {
        self.pins
            .compare_exchange(RETIRED, 0, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        let entry = self.retired.swap(std::ptr::null_mut(), Ordering::SeqCst);
        if entry.is_null() {
            return None;
        }
        // SAFETY: `entry` came from `Box::into_raw` in `publish`, was swapped out of `entry` by `retire`,
        // and is taken here once: the claim cleared the mark at a zero count, so no reader holds it (the
        // module's argument), and the swap to null hands it to this caller alone.
        Some(unsafe { Box::from_raw(entry) })
    }
}

/// One reader's pin, given back on the call's end or its unwind.
struct Pin<'a, T, F: FnOnce(Box<T>)> {
    pinned: &'a Pinned<T>,
    finish: Option<F>,
}

impl<T, F: FnOnce(Box<T>)> Drop for Pin<'_, T, F> {
    fn drop(&mut self) {
        let before = self.pinned.pins.fetch_sub(1, Ordering::SeqCst);
        if before == RETIRED | 1
            && let Some(entry) = self.pinned.claim()
            && let Some(finish) = self.finish.take()
        {
            finish(entry);
        }
    }
}

#[cfg(loom)]
#[cfg_attr(
    loom,
    allow(clippy::unwrap_used, clippy::disallowed_types, clippy::panic)
)]
mod loom_tests {
    // `cfg(loom)` only (D-8 exception 3, a test harness): loom's `Arc` shares the model's cell between its
    // threads.
    use super::*;
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicUsize;

    /// An entry that counts its free in a cell outside it, so a reader checks for the free without
    /// touching the entry.
    struct Probe {
        frees: Arc<AtomicUsize>,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            self.frees.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn published(frees: &Arc<AtomicUsize>) -> Arc<Pinned<Probe>> {
        let pinned = Arc::new(Pinned::new());
        pinned.publish(Box::new(Probe {
            frees: Arc::clone(frees),
        }));
        pinned
    }

    /// A reader's call: the entry is not freed while it runs.
    fn read(pinned: &Pinned<Probe>, frees: &AtomicUsize) {
        pinned.read(
            |_| assert_eq!(frees.load(Ordering::SeqCst), 0, "freed under a pin"),
            drop,
        );
    }

    /// Do: one reader against one retirement, every interleaving. Expect: the reader never finds its entry
    /// freed inside its call, and the entry is freed exactly once, by whichever finished last.
    #[test]
    fn a_reader_never_sees_its_entry_freed_and_one_frees_it() {
        loom::model(|| {
            let frees = Arc::new(AtomicUsize::new(0));
            let pinned = published(&frees);
            let reader = {
                let pinned = Arc::clone(&pinned);
                let frees = Arc::clone(&frees);
                loom::thread::spawn(move || read(&pinned, &frees))
            };
            pinned.retire(drop);
            reader.join().unwrap();
            assert_eq!(frees.load(Ordering::SeqCst), 1, "freed once");
        });
    }

    /// Do: two readers against one retirement. Expect: as above, with the last of three finishing.
    #[test]
    fn two_readers_and_a_retirement_free_once() {
        loom::model(|| {
            let frees = Arc::new(AtomicUsize::new(0));
            let pinned = published(&frees);
            let readers: Vec<_> = (0..2)
                .map(|_| {
                    let pinned = Arc::clone(&pinned);
                    let frees = Arc::clone(&frees);
                    loom::thread::spawn(move || read(&pinned, &frees))
                })
                .collect();
            pinned.retire(drop);
            for reader in readers {
                reader.join().unwrap();
            }
            assert_eq!(frees.load(Ordering::SeqCst), 1, "freed once");
        });
    }

    /// Do: retire from inside a reader's own call. Expect: the retirement returns, and the reader's unpin
    /// frees the entry once.
    #[test]
    fn a_retirement_inside_a_read_ends_and_the_reader_frees() {
        loom::model(|| {
            let frees = Arc::new(AtomicUsize::new(0));
            let pinned = published(&frees);
            pinned.read(
                |_| {
                    pinned.retire(drop);
                    pinned.retire(|_| panic!("a second retirement finished"));
                },
                drop,
            );
            assert_eq!(frees.load(Ordering::SeqCst), 1, "freed once, by the reader");
        });
    }
}
