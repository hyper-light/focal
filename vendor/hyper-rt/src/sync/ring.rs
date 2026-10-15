//! A bounded ring of values with no lock on any path, and the block a primitive's ends share it through
//! (docs/runtime.md §8; mantle `docs/design/event-loop.md` D4). The synchronization primitives carried their
//! values through `std::sync::mpsc::sync_channel`, whose blocking receive and send register in a waiter
//! list behind a pthread mutex (std `sync/mpmc/waker.rs`, `SyncWaker`): mantle's fill profile caught threads
//! in `__psynch_mutexwait` under `SyncWaker::register` while senders held it to notify
//! (`benchmark-results/mantle-boxed-request-ab-20261010/fill5m-sample.txt`). Here the ring is only atomics
//! and the slots' values, and waiting is the cell's waiter word (`crate::handoff`), so neither end ever
//! waits for the other to leave a critical section.
//!
//! **The ring** is Vyukov's bounded queue [C: Dmitry Vyukov, "Bounded MPMC queue", 1024cores.net]: each
//! slot carries a stamp, the position it is free for (`position`) or full at (`position + 1`); a producer
//! claims a position by compare-and-swap on the tail and publishes by storing the stamp, a consumer claims by
//! compare-and-swap on the head and releases the slot for the next lap by storing `position + lap`. A
//! position is a lap above a mark above an index (`lap | index`), the mark the capacity rounded up to a power
//! of two and one lap twice it, as Rust's std encodes its array channel (`sync/mpmc/array.rs`, `mark_bit`), so
//! the capacity is exact and "full at this lap" never reads as "free for the next" (slates' word ring refused
//! a capacity of one for exactly that collision, AUD-29-33).
//!
//! **Closing** sets the mark in the tail (std's disconnect, the same bit): a producer claims a position only
//! by a compare-and-swap from a tail without it, so a push either claimed its place before the close, in the
//! tail's one order of changes, or is refused `Closed` with its value. A check of a primitive's state before
//! the push could not order the two: a receiver's drop that freed a full ring's room between a sender's check
//! and its push let the push in after the close (`tests/sync.rs`, a task waiting for room; 2 of 3 runs with
//! 200 µs between the check and the push). A push that claimed its place before the close and published
//! after the closer drained finds the mark after a fence and drops what is left ([`Ring::drain`]).
//!
//! **No spin.** A slot the consumer has claimed but not yet released reads as full, and a position a
//! producer has claimed but not yet published reads as empty: the call returns at once and the waiter
//! protocol carries progress (the consumer grants room after it releases; a producer wakes the consumer
//! after it publishes). Neither end waits on a peer that the scheduler has preempted mid-operation, which a
//! spin on those two states would do. A compare-and-swap retries only after another end's claim of the same
//! position succeeded, and a view of the tail or the head left behind advances past positions others
//! completed or reads the latest value by a read-modify-write once, so every retry follows another end's
//! progress (lock-free). A view that still makes no sense after that read, a slot's stamp laps older than the
//! latest tail or head, is reported full or empty rather than read again until it changes: nothing bounds how
//! long a plain load may return an old value (loom explores exactly that, and a loop on it never ended), while
//! the waiting protocols fence before their second look, which rules such a stamp out.
//!
//! **The shared block** ([`Shared`]) is the state a primitive's ends share, freed by the last end: its count
//! is the primitive's cell's handle count (`crate::sync::cell`), so the block lives exactly as long as the
//! cell's handles, the existing ownership of every primitive here, and nothing is reference counted twice.
#![allow(unsafe_code)]

use std::mem::MaybeUninit;
use std::ptr::NonNull;

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering, fence};

use super::cell::CellRef;
use crate::error::RtError;

#[cfg(loom)]
use loom::cell::UnsafeCell;

/// std's cell behind loom's interface, so the ring's code is one text under both.
#[cfg(not(loom))]
#[derive(Debug)]
struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    fn new(value: T) -> Self {
        Self(std::cell::UnsafeCell::new(value))
    }

    fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

/// Shape: keeps the head and the tail on separate cache lines (the largest line we target, Apple
/// silicon's 128 bytes): producers write the tail, the consumer the head.
#[repr(align(128))]
#[derive(Debug)]
pub(super) struct Padded<T>(pub(super) T);

/// One slot: the position it is free for or full at, and its value while full.
#[derive(Debug)]
struct Slot<T> {
    stamp: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded ring of `T` for any number of producers and consumers (the primitives here have one consumer).
#[derive(Debug)]
pub(crate) struct Ring<T> {
    head: Padded<AtomicUsize>,
    tail: Padded<AtomicUsize>,
    /// The bit of the tail that marks the ring closed: the capacity rounded up to a power of two, above
    /// every index.
    mark: usize,
    /// One lap of positions: twice the mark, so no position carries it.
    one_lap: usize,
    slots: Box<[Slot<T>]>,
}

/// A push refused, its value handed back.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Refused<T> {
    /// Every place holds a value, or one is still being taken from a lap ago.
    Full(T),
    /// The ring was closed before this push claimed a place.
    Closed(T),
}

// SAFETY: a value moves into a slot on one thread and out of it on another, exactly once each, ordered by the
// slot's stamp (Release by the writer, Acquire by the reader); a `T` that may move between threads makes the
// ring safe to move and to share. No `&T` is ever handed out, so `T: Sync` is not required.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: as for `Send`: every access to a value is the one claim of its position, so shared references to
// the ring never alias a value.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Ring<T> {
    /// A ring holding at most `capacity` values. Refused `BadConfig` for no capacity or one whose positions
    /// do not fit a word beside a lap.
    pub(crate) fn new(capacity: usize) -> Result<Self, RtError> {
        let refused = RtError::BadConfig {
            what: "a ring of no capacity, or one too large for its positions",
        };
        if capacity == 0 {
            return Err(refused);
        }
        let mark = capacity
            .checked_add(1)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(refused.clone())?;
        let one_lap = mark
            .checked_mul(2)
            .filter(|lap| lap.checked_mul(2).is_some())
            .ok_or(refused.clone())?;
        // The slots' allocation must be a valid layout, or collecting them would abort on the overflow.
        std::alloc::Layout::array::<Slot<T>>(capacity).map_err(|_| refused)?;
        let slots = (0..capacity)
            .map(|position| Slot {
                stamp: AtomicUsize::new(position),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();
        Ok(Self {
            head: Padded(AtomicUsize::new(0)),
            tail: Padded(AtomicUsize::new(0)),
            mark,
            one_lap,
            slots,
        })
    }

    /// The most values the ring holds at once.
    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// The position after `position`: the next index of its lap, or the first of the next lap.
    fn next(&self, position: usize) -> usize {
        let index = position & self.mark.wrapping_sub(1);
        if index.saturating_add(1) < self.slots.len() {
            position.wrapping_add(1)
        } else {
            (position & !self.one_lap.wrapping_sub(1)).wrapping_add(self.one_lap)
        }
    }

    /// The slot of `position`.
    fn slot(&self, position: usize) -> Option<&Slot<T>> {
        self.slots.get(position & self.mark.wrapping_sub(1))
    }

    /// Adds `value` at the tail; hands it back when the ring is full, including while the slot the tail
    /// reaches is still being taken from a lap ago, and when the ring is closed.
    pub(crate) fn push(&self, value: T) -> Result<(), Refused<T>> {
        let mut tail = self.tail.0.load(Ordering::Relaxed);
        let mut reread = false;
        loop {
            // A closed tail carries the mark, so no claim below can succeed past a close.
            if tail & self.mark != 0 {
                return Err(Refused::Closed(value));
            }
            let Some(slot) = self.slot(tail) else {
                return Err(Refused::Full(value));
            };
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == tail {
                match self.tail.0.compare_exchange(
                    tail,
                    self.next(tail),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.value.with_mut(|cell| {
                            // SAFETY: the claim of `tail` is this call's alone (the compare-and-swap), and the
                            // stamp it read (Acquire) says the last lap's value was taken and released, so no one
                            // reads or writes this slot until the stamp below publishes it.
                            unsafe { cell.write(MaybeUninit::new(value)) }
                        });
                        slot.stamp.store(tail.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(seen) => tail = seen,
                }
            } else if stamp.wrapping_add(self.one_lap) == tail
                || stamp.wrapping_add(self.one_lap) == tail.wrapping_add(1)
            {
                // The slot still holds the last lap's position, claimed and not yet published or published and
                // not yet taken: the ring is full.
                return Err(Refused::Full(value));
            } else if stamp == tail.wrapping_add(1) {
                // Another producer claimed and published `tail`: the tail is past it.
                tail = self.next(tail);
            } else if !reread {
                // This view of the tail, or of the slot, is a lap or more behind: read the latest tail once (a
                // read-modify-write reads the last value in the tail's order, where a plain load may keep
                // returning an old one).
                reread = true;
                tail = self.tail.0.fetch_add(0, Ordering::Relaxed);
            } else {
                // Still a lap apart with the latest tail: the slot's stamp read is the old one. Reported full
                // rather than read again until it changes, which nothing bounds; a sender that must wait fences
                // before it looks again (`sync::room`), and no stamp that old survives the fence.
                return Err(Refused::Full(value));
            }
        }
    }

    /// Takes the value at the head, if one is published there.
    pub(crate) fn pop(&self) -> Option<T> {
        let mut head = self.head.0.load(Ordering::Relaxed);
        let mut reread = false;
        loop {
            let slot = self.slot(head)?;
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == head.wrapping_add(1) {
                match self.head.0.compare_exchange(
                    head,
                    self.next(head),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: the stamp read (Acquire) says a producer wrote this slot and published it at
                        // `head`, and the claim of `head` is this call's alone, so the value is initialized and
                        // read once; the store below releases the slot for the next lap only after the read.
                        let value = slot.value.with(|cell| unsafe { cell.read().assume_init() });
                        slot.stamp
                            .store(head.wrapping_add(self.one_lap), Ordering::Release);
                        return Some(value);
                    }
                    Err(seen) => head = seen,
                }
            } else if stamp == head {
                return None;
            } else if stamp == head.wrapping_add(self.one_lap) {
                // Another consumer took and released `head`: the head is past it.
                head = self.next(head);
            } else if !reread {
                // This view of the head, or of the slot, is a lap or more behind: read the latest head once
                // (as for the tail).
                reread = true;
                head = self.head.0.fetch_add(0, Ordering::Relaxed);
            } else {
                // The slot's stamp read is the old one: reported empty (as for a push); a receiver that must
                // wait fences before it looks again (`crate::handoff`).
                return None;
            }
        }
    }
}

impl<T> Ring<T> {
    /// Takes the value at the head as the ring's one taker: the primitives here take only through their one
    /// receiver until it closes the ring (`close`, in the receiver's drop), and a drain, the only other taker,
    /// begins only after that close, so while this end may take, the head is its alone and moves by a plain
    /// store where [`Ring::pop`] claims it by a compare-and-swap. A drain that follows reads the head this end
    /// left, or an older one it walks past as it walks past another taker's ([`Ring::pop`]).
    pub(crate) fn pop_owned(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        let slot = self.slot(head)?;
        // Anything but the publication of `head` reads as empty: a position claimed and not yet published, or a
        // stamp this thread reads old, which a waiting receiver's fence before its second look rules out
        // (`crate::handoff`).
        if slot.stamp.load(Ordering::Acquire) != head.wrapping_add(1) {
            return None;
        }
        // SAFETY: the stamp read (Acquire) says a producer wrote this slot and published it at `head`, and the
        // head is this taker's alone (the function's contract: no other end takes while it may), so the value is
        // initialized and read once; the store below releases the slot for the next lap only after the read.
        let value = slot.value.with(|cell| unsafe { cell.read().assume_init() });
        slot.stamp
            .store(head.wrapping_add(self.one_lap), Ordering::Release);
        self.head.0.store(self.next(head), Ordering::Relaxed);
        Some(value)
    }
}

impl<T> Ring<T> {
    /// Refuses every push that has not claimed its place yet; one that has still lands. The mark, then a
    /// fence, so this thread's next look at the slots and a producer's look at the mark after its own fence
    /// cannot both miss the other's write (the store-buffering outcome both fences forbid).
    pub(crate) fn close(&self) {
        self.tail.0.fetch_or(self.mark, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }

    /// Whether the ring is closed, as this thread sees the tail: a producer asks after a fence that
    /// follows its push.
    pub(crate) fn is_closed(&self) -> bool {
        self.tail.0.load(Ordering::Relaxed) & self.mark != 0
    }

    /// Drops the values in the ring, at most its capacity of them: what a closed ring holds that no one
    /// will take. Every value published before this thread's last fence is among the first that many; one
    /// published after it was published by a producer that then finds the mark and drains for itself.
    pub(crate) fn drain(&self) {
        for _ in 0..self.capacity() {
            if self.pop().is_none() {
                break;
            }
        }
    }
}

impl<T> Drop for Ring<T> {
    /// Drops the values still in the ring: with `&mut self` no end can reach it.
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

/// A block a primitive's ends share, freed by the last of them: the count is the primitive's cell's handle
/// count, so an end holds the cell and the block together and gives both back at once, when it drops (after
/// the end's own `Drop`, which may still read the block and wake through the cell).
#[derive(Debug)]
pub(crate) struct Shared<S> {
    block: NonNull<S>,
    cell: CellRef,
    /// The cell's words, looked up once: this end holds one of the cell's handles, so the cell keeps its
    /// generation while the end lives, and the table lookup [`CellRef::cell`] makes at each use would find
    /// these same words.
    words: Option<&'static super::cell::Cell>,
}

// SAFETY: an end reaches the block only through `&S`, and the block is freed only by the end whose release
// was the cell's last handle, after every other end has given its handle back; so a `Shared` may move to and
// be used from any thread when `S` may be shared between threads.
unsafe impl<S: Send + Sync> Send for Shared<S> {}
// SAFETY: as for `Send`.
unsafe impl<S: Send + Sync> Sync for Shared<S> {}

impl<S> Shared<S> {
    /// The first end of `state`, holding the one handle `cell` was claimed with.
    pub(crate) fn new(state: S, cell: CellRef) -> Self {
        Self {
            block: NonNull::from(Box::leak(Box::new(state))),
            cell,
            words: cell.cell(),
        }
    }

    /// Another end of the same block, holding a handle of the cell it takes now.
    pub(crate) fn share(&self) -> Self {
        self.cell.retain();
        Self {
            block: self.block,
            cell: self.cell,
            words: self.words,
        }
    }

    /// The shared state.
    pub(crate) fn get(&self) -> &S {
        // SAFETY: this end holds one of the cell's handles, so the block is not freed (only the release of the
        // last handle frees it, below) for as long as `self` lives.
        unsafe { self.block.as_ref() }
    }

    /// The primitive's cell.
    pub(crate) fn cell(&self) -> CellRef {
        self.cell
    }

    /// The primitive's cell's words, as [`CellRef::cell`] finds them for a live end.
    pub(crate) fn words(&self) -> Option<&'static super::cell::Cell> {
        self.words
    }
}

impl<S> Drop for Shared<S> {
    /// Gives this end's handle back; the last frees the block.
    fn drop(&mut self) {
        if self.cell.release_last() {
            // SAFETY: the block came from `Box::leak` in `new`, and this was the cell's last handle: every
            // other end has dropped, none can reach the block again, and this is its one free.
            drop(unsafe { Box::from_raw(self.block.as_ptr()) });
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn a_ring_is_fifo_bounded_exactly_and_reuses_its_slots_across_laps() {
        for capacity in [1usize, 2, 3, 4, 7] {
            let ring = Ring::new(capacity).unwrap();
            assert_eq!(ring.pop(), None);
            for lap in 0..5u64 {
                for value in 0..capacity {
                    ring.push((lap, value)).unwrap();
                }
                assert_eq!(
                    ring.push((lap, capacity)),
                    Err(Refused::Full((lap, capacity))),
                    "full at {capacity}"
                );
                for value in 0..capacity {
                    assert_eq!(ring.pop(), Some((lap, value)));
                }
                assert_eq!(ring.pop(), None);
            }
        }
        assert!(Ring::<u8>::new(0).is_err());
        assert!(Ring::<u8>::new(usize::MAX).is_err());
    }

    /// A closed ring refuses every push with its value, the room a take frees after the close as well, and
    /// still gives what it held.
    #[test]
    fn a_closed_ring_refuses_pushes_and_still_gives_what_it_held() {
        for capacity in [1usize, 2, 3] {
            let ring = Ring::new(capacity).unwrap();
            ring.push(1u32).unwrap();
            assert!(!ring.is_closed());
            ring.close();
            assert!(ring.is_closed());
            assert_eq!(ring.push(2), Err(Refused::Closed(2)));
            assert_eq!(ring.pop(), Some(1));
            assert_eq!(
                ring.push(3),
                Err(Refused::Closed(3)),
                "room freed after the close"
            );
            assert_eq!(ring.pop(), None);
            ring.close();
            assert!(ring.is_closed(), "a second close changes nothing");
        }
    }

    /// Drops counted in a test's own static, so a dropped value's count is exact.
    static DROPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// A value that counts its drop.
    struct Counted;

    impl Drop for Counted {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Values left in a ring are dropped with it, each once.
    #[test]
    fn a_dropped_ring_drops_its_values() {
        let before = DROPPED.load(std::sync::atomic::Ordering::Relaxed);
        let ring = Ring::new(3).unwrap();
        ring.push(Counted).ok().unwrap();
        ring.push(Counted).ok().unwrap();
        assert_eq!(DROPPED.load(std::sync::atomic::Ordering::Relaxed), before);
        drop(ring);
        assert_eq!(
            DROPPED.load(std::sync::atomic::Ordering::Relaxed),
            before + 2
        );
    }

    /// Many producers and one consumer: every value arrives once, in each producer's own order.
    #[test]
    fn concurrent_producers_deliver_every_value_once_in_their_order() {
        const PRODUCERS: u64 = 4;
        const EACH: u64 = 20_000;
        let ring = Ring::new(8).unwrap();
        let mut last = [None::<u64>; PRODUCERS as usize];
        let mut received = 0u64;
        std::thread::scope(|scope| {
            for producer in 0..PRODUCERS {
                let ring = &ring;
                scope.spawn(move || {
                    for sequence in 0..EACH {
                        let mut value = (producer, sequence);
                        while let Err(Refused::Full(back)) = ring.push(value) {
                            value = back;
                            std::hint::spin_loop();
                        }
                    }
                });
            }
            while received < PRODUCERS * EACH {
                if let Some((producer, sequence)) = ring.pop() {
                    let seen = &mut last[producer as usize];
                    assert!(seen.is_none_or(|previous| previous + 1 == sequence));
                    *seen = Some(sequence);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });
        assert_eq!(ring.pop(), None);
    }
}

#[cfg(test)]
#[cfg(loom)]
mod loom_tests {
    use super::*;
    use crate::mem::loom_bounds;

    /// Two producers each push once and a consumer pops once, all three spawned, on a ring of one slot; then the
    /// main thread, having joined them, drains what is left: every accepted value comes out once and only
    /// accepted values come out. The pushes race for one position, a push meets the slot published or mid-take,
    /// and a take lets the next lap's push in. The consumer is a thread of its own, not the main thread before
    /// its joins: loom's reduction remembers only an object's last access, so a pop the main thread made before
    /// the producers ever ran would be overwritten by a producer's own load and never reordered after a push
    /// (the first draft of this model never popped a published value in any interleaving, which the mutations
    /// recorded in `benchmark-results/hyper-rt-vs-tokio-20261010/tests/loom-channel.txt` showed). Loom's cell
    /// reports a value read that the stamp did not order after its write, and a write over a value still being
    /// read. No step retries, so the model has no spin of its own.
    #[test]
    fn two_producers_and_a_consumer_move_each_value_once_through_one_slot() {
        loom_bounds::explore("ring: two producers, one consumer, one slot", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            let producers: Vec<_> = (0..2u32)
                .map(|value| loom::thread::spawn(move || ring.push(value).is_ok()))
                .collect();
            let consumer = loom::thread::spawn(move || ring.pop());
            let accepted: Vec<u32> = producers
                .into_iter()
                .zip(0..2u32)
                .filter_map(|(producer, value)| producer.join().unwrap().then_some(value))
                .collect();
            let mut out: Vec<u32> = consumer.join().unwrap().into_iter().collect();
            while let Some(value) = ring.pop() {
                out.push(value);
            }
            out.sort_unstable();
            assert_eq!(out, accepted, "out {out:?}, accepted {accepted:?}");
            assert!(!accepted.is_empty(), "an empty slot refused both pushes");
        });
    }

    /// Two producers each push once while the ring's one taker takes once with [`Ring::pop_owned`], each a
    /// thread of its own, on a ring of one slot; then the taker, joined, takes the rest the same way: every
    /// accepted value comes out once and only accepted values come out, the head moved by the taker's stores
    /// alone.
    #[test]
    fn two_producers_and_the_owned_taker_move_each_value_once_through_one_slot() {
        loom_bounds::explore("ring: two producers, the owned taker, one slot", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            let producers: Vec<_> = (0..2u32)
                .map(|value| loom::thread::spawn(move || ring.push(value).is_ok()))
                .collect();
            let taker = loom::thread::spawn(move || ring.pop_owned());
            let accepted: Vec<u32> = producers
                .into_iter()
                .zip(0..2u32)
                .filter_map(|(producer, value)| producer.join().unwrap().then_some(value))
                .collect();
            let mut out: Vec<u32> = taker.join().unwrap().into_iter().collect();
            while let Some(value) = ring.pop_owned() {
                out.push(value);
            }
            out.sort_unstable();
            assert_eq!(out, accepted, "out {out:?}, accepted {accepted:?}");
        });
    }

    /// The receiver's life on one thread: an owned take, then its drop's close and drain, while a producer
    /// pushes and, finding the close after a fence, drains too, as the primitives' senders do. The drains are
    /// two takers after the owned one ended, each claiming by compare-and-swap from the head the owned take
    /// left. Once both have joined the ring holds nothing, each accepted value came out once, and a push refused
    /// kept its value.
    #[test]
    fn an_owned_take_then_a_close_racing_a_push_leaves_nothing() {
        loom_bounds::explore("ring: the owned taker's close against a push", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            ring.push(1).unwrap();
            let producer = loom::thread::spawn(move || {
                let pushed = ring.push(2);
                if pushed.is_ok() {
                    fence(Ordering::SeqCst);
                    if ring.is_closed() {
                        ring.drain();
                    }
                }
                pushed
            });
            let receiver = loom::thread::spawn(move || {
                let taken = ring.pop_owned();
                ring.close();
                ring.drain();
                taken
            });
            let pushed = producer.join().unwrap();
            let taken = receiver.join().unwrap();
            assert_eq!(taken, Some(1), "the owned take found the first value");
            assert!(ring.pop().is_none(), "a value was left in the closed ring");
            assert!(
                matches!(pushed, Ok(()) | Err(Refused::Closed(2) | Refused::Full(2))),
                "{pushed:?}"
            );
        });
    }

    /// A producer pushes once into a full ring of one place while a closer closes it and drains it, each a
    /// thread of its own: the push is refused, full or closed, never let into the room the drain frees after
    /// the close. A check of a closed flag before the push let it in (`tests/sync.rs`).
    #[test]
    fn a_full_ring_closed_and_drained_takes_no_push() {
        loom_bounds::explore("ring: a push against the close of a full ring", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            ring.push(0).unwrap();
            let producer = loom::thread::spawn(move || ring.push(8));
            let closer = loom::thread::spawn(move || {
                ring.close();
                ring.drain();
            });
            let pushed = producer.join().unwrap();
            closer.join().unwrap();
            assert!(
                matches!(pushed, Err(Refused::Full(8) | Refused::Closed(8))),
                "{pushed:?}"
            );
            assert!(ring.pop().is_none(), "a value was left in the closed ring");
        });
    }

    /// A producer pushes once into an empty ring while a closer closes it and drains it; a producer whose
    /// push lands looks at the mark after a fence and drains if it is set, as the primitives' senders do.
    /// Once both have joined the ring holds nothing, and a push refused kept its value.
    #[test]
    fn a_push_racing_a_close_is_refused_taken_or_dropped_never_left() {
        loom_bounds::explore("ring: a push against a close", || {
            let ring: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
            let producer = loom::thread::spawn(move || {
                let pushed = ring.push(7);
                if pushed.is_ok() {
                    fence(Ordering::SeqCst);
                    if ring.is_closed() {
                        ring.drain();
                    }
                }
                pushed
            });
            let closer = loom::thread::spawn(move || {
                ring.close();
                ring.drain();
            });
            let pushed = producer.join().unwrap();
            closer.join().unwrap();
            assert!(ring.pop().is_none(), "a value was left in the closed ring");
            assert!(
                matches!(pushed, Ok(()) | Err(Refused::Closed(7))),
                "{pushed:?}"
            );
        });
    }
}
