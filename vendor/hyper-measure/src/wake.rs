//! Wakers that count: what a test or a benchmark hands a call that takes a
//! [`Waker`], so that it can say how many times each was woken and hear which
//! one was.
//!
//! A waker's data is a [`Slot`] that lives for the rest of the process: it is
//! leaked when the waker is made, so every clone of the waker is the same
//! pointer, no clone or wake allocates, and none can outlive its slot. The
//! leak is one slot a waker made, which a measurement makes a bounded number of
//! (one for each submitter it drives). No reference count is kept, so nothing
//! is shared but the slot's atomic count and the channel it tells.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::task::{RawWaker, RawWakerVTable, Waker};

/// What one waker counts, and whom it tells when woken.
#[derive(Debug)]
pub struct Slot {
    wakes: AtomicU64,
    tag: usize,
    tell: SyncSender<usize>,
}

impl Slot {
    /// How many times the waker was woken.
    pub fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::SeqCst)
    }

    fn wake(&self) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        // A listener that has gone, or is full, misses the tag; the count holds it.
        let _ = self.tell.try_send(self.tag);
    }
}

/// A waker that counts its wakes and sends `tag` to `tell` on each, and its
/// slot, from which the count is read.
pub fn waker(tag: usize, tell: SyncSender<usize>) -> (Waker, &'static Slot) {
    let slot: &'static Slot = Box::leak(Box::new(Slot {
        wakes: AtomicU64::new(0),
        tag,
        tell,
    }));
    let raw = RawWaker::new(std::ptr::from_ref(slot).cast(), &VTABLE);
    // SAFETY: `raw`'s data points to a leaked `Slot`, which lives as long as
    // the process and is `Sync` (an atomic count, a `Sync` sender and a
    // `usize`), so it may be used from any thread at any time; `VTABLE`'s
    // functions only read through that pointer, and clone returns the same
    // pointer with the same table, so the `RawWakerVTable` contract holds.
    let waker = unsafe { Waker::from_raw(raw) };
    (waker, slot)
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);

/// The slot a waker's data points to.
fn slot(data: *const ()) -> &'static Slot {
    // SAFETY: every `RawWaker` with `VTABLE` is made by `waker` (or cloned
    // from one by `clone`), whose data points to a leaked, never freed
    // `Slot`.
    unsafe { &*data.cast::<Slot>() }
}

fn clone(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

fn wake(data: *const ()) {
    slot(data).wake();
}

fn wake_by_ref(data: *const ()) {
    slot(data).wake();
}

/// The slot is leaked, never freed: dropping a waker frees nothing.
fn drop_waker(_data: *const ()) {}
