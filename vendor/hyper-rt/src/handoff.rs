//! The waiter handoff of a synchronization cell (docs/runtime.md §8; mantle's final review of hyper-rt,
//! second pass, finding 2; mantle `docs/design/event-loop.md` D4), apart and generic over the atomic and the
//! thread handle so loom drives this exact code.
//!
//! A waiter registers in its cell's waiter word, then checks the state; a publisher changes the state, then
//! takes the word and wakes what it names. Each side writes and then reads, the store-buffering shape, so
//! each puts a `SeqCst` fence between its write and its read (the parking protocol's argument,
//! [`crate::parking`]): whichever fence comes first in their total order, the later side's read sees the
//! earlier side's write, so either the waiter's check sees the change or the publisher's read sees the
//! waiter. Registration had been a read-modify-write that the publisher answered with another (a swap of
//! the word to empty on every publication), which needed no fence but wrote the word's cache line on every
//! publication, waiter or not; a publisher now only reads it unless someone waits.
//!
//! **Who waits.** The word names a task (its packed word, woken through the registry) or a plain thread
//! blocked in a call (the address of its `Thread` handle on its own stack, the top bit set: no task word has
//! it, a task word's top bits being a shard id below 2¹⁰). A thread used to wait inside the value's
//! `std::sync::mpsc` channel, whose waiter list is a pthread mutex that blocked receivers and notifying
//! senders contend for (std `sync/mpmc/waker.rs`, `SyncWaker`; mantle's fill profile caught both sides in
//! `__psynch_mutexwait`).
//!
//! **One wake a registration.** A publisher takes a waiter with a compare-and-swap, so of several
//! publishers exactly one wakes each registration and the rest find the word empty.
//!
//! **A thread's handle outlives every read of it.** A publisher that takes a thread marks the word
//! [`CLAIMED`], clones the handle through the address, then empties the word and unparks the clone. The
//! thread leaves its wait only once the word is not [`CLAIMED`]: a thread that withdraws a registration a
//! publisher has claimed parks until that publisher's unpark, which follows its last read of the handle.
#![allow(unsafe_code)]

#[cfg(loom)]
use loom::sync::atomic::fence;
use std::sync::atomic::Ordering;
#[cfg(not(loom))]
use std::sync::atomic::fence;

/// Format: "no one waits" (no task word is all ones: its generation field would be
/// `Encoded::ANY_GENERATION` and its shard `u16::MAX`, which the registry never issues).
pub(crate) const NO_WAITER: u64 = u64::MAX;
/// Format: a publisher has claimed the thread that waited and is reading its handle (an odd value, so
/// never an aligned address).
pub(crate) const CLAIMED: u64 = u64::MAX - 1;
/// Format: the bit that marks a thread's handle address (no task word sets it: see the module doc).
pub(crate) const THREAD: u64 = 1 << 63;

/// The atomic word a waiter is handed over in: std's in the runtime, loom's in its model.
pub(crate) trait WaiterWord {
    /// An atomic load.
    fn load_word(&self, order: Ordering) -> u64;
    /// An atomic swap.
    fn swap_word(&self, value: u64, order: Ordering) -> u64;
    /// An atomic store.
    fn store_word(&self, value: u64, order: Ordering);
    /// A compare-and-swap from `current` to `new`, `AcqRel` on success and `Acquire` on failure.
    fn cas_word(&self, current: u64, new: u64) -> Result<u64, u64>;
}

impl WaiterWord for std::sync::atomic::AtomicU64 {
    fn load_word(&self, order: Ordering) -> u64 {
        self.load(order)
    }
    fn swap_word(&self, value: u64, order: Ordering) -> u64 {
        self.swap(value, order)
    }
    fn store_word(&self, value: u64, order: Ordering) {
        self.store(value, order);
    }
    fn cas_word(&self, current: u64, new: u64) -> Result<u64, u64> {
        self.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

#[cfg(loom)]
impl WaiterWord for loom::sync::atomic::AtomicU64 {
    fn load_word(&self, order: Ordering) -> u64 {
        self.load(order)
    }
    fn swap_word(&self, value: u64, order: Ordering) -> u64 {
        self.swap(value, order)
    }
    fn store_word(&self, value: u64, order: Ordering) {
        self.store(value, order);
    }
    fn cas_word(&self, current: u64, new: u64) -> Result<u64, u64> {
        self.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

/// What a publisher took from the word.
#[derive(Debug)]
pub(crate) enum Taken<H> {
    /// No one waited, or another publisher is already waking the thread that did.
    Nobody,
    /// A task's word, to wake through the registry.
    Task(u64),
    /// A clone of a waiting thread's handle, to unpark.
    Thread(H),
}

/// Records the task `word` as the waiter, then fences, so the caller's check of the state that follows is
/// ordered after the registration (module doc).
pub(crate) fn register_task(waiter: &impl WaiterWord, word: u64) {
    waiter.swap_word(word, Ordering::AcqRel);
    fence(Ordering::SeqCst);
}

/// The word that names `handle`, a thread's handle on its own stack: its address divided by its alignment,
/// which no address leaves at or above the top bit, under [`THREAD`].
pub(crate) fn thread_word<H>(handle: &H) -> u64 {
    let address = u64::try_from(std::ptr::from_ref(handle).expose_provenance()).unwrap_or(0);
    address
        .checked_shr(std::mem::align_of::<H>().trailing_zeros())
        .unwrap_or(0)
        | THREAD
}

/// The address of the handle a thread word names (the inverse of [`thread_word`]).
fn thread_address<H>(word: u64) -> u64 {
    (word & !THREAD)
        .checked_shl(std::mem::align_of::<H>().trailing_zeros())
        .unwrap_or(0)
}

/// Records the thread `word` names as the waiter, then fences (as [`register_task`]).
fn register_thread(waiter: &impl WaiterWord, word: u64) {
    waiter.swap_word(word, Ordering::AcqRel);
    fence(Ordering::SeqCst);
}

/// The publisher's half, after its state change: takes the waiter, if one waits ([`take`] with the runtime's
/// reader of a thread's handle).
pub(crate) fn take_waiter(waiter: &impl WaiterWord) -> Taken<std::thread::Thread> {
    take(waiter, |word| {
        // SAFETY: `take` calls this only with a registered thread word while it holds the word's claim;
        // thread words are registered only by `wait_as_thread`, whose handle lives on its stack and which does
        // not return while the word reads `CLAIMED` (`clone_thread`'s contract).
        unsafe { clone_thread(word) }
    })
}

/// Takes the waiter, if one waits, with `read` cloning a thread's handle from its word while the word reads
/// [`CLAIMED`]. Each compare-and-swap that fails saw the word change since this call read it (the
/// waiter withdrew or registered again, or another publisher took it), and the call ends at the first one
/// that succeeds or the first read of an empty word.
pub(crate) fn take<H>(waiter: &impl WaiterWord, read: impl FnOnce(u64) -> H) -> Taken<H> {
    fence(Ordering::SeqCst);
    let mut word = waiter.load_word(Ordering::Acquire);
    loop {
        if word == NO_WAITER || word == CLAIMED {
            return Taken::Nobody;
        }
        if word & THREAD == 0 {
            match waiter.cas_word(word, NO_WAITER) {
                Ok(_) => return Taken::Task(word),
                Err(seen) => word = seen,
            }
            continue;
        }
        match waiter.cas_word(word, CLAIMED) {
            Ok(_) => {
                let handle = read(word);
                waiter.store_word(NO_WAITER, Ordering::Release);
                return Taken::Thread(handle);
            }
            Err(seen) => word = seen,
        }
    }
}

/// Wakes what the word names, if anything waits there: a task through the registry, a parked thread by its
/// handle ([`take_waiter`]). Of several publishers, one wakes each registration.
pub(crate) fn wake_waiter(waiter: &impl WaiterWord) {
    match take_waiter(waiter) {
        Taken::Task(word) => crate::registry::wake(crate::mem::Encoded::from_word(word)),
        Taken::Thread(thread) => thread.unpark(),
        Taken::Nobody => {}
    }
}

/// A clone of the thread handle `word` names: the reader [`take_waiter`] gives [`take`].
///
/// # Safety
///
/// `word` came from [`thread_word`] of a handle whose thread has not returned from its wait, and the
/// caller holds the word's claim ([`CLAIMED`]): the thread does not leave [`wait_as_thread`] while the word
/// reads [`CLAIMED`], so the handle is alive for the read.
unsafe fn clone_thread(word: u64) -> std::thread::Thread {
    let handle = std::ptr::with_exposed_provenance::<std::thread::Thread>(
        usize::try_from(thread_address::<std::thread::Thread>(word)).unwrap_or(usize::MAX),
    );
    // SAFETY: the function's contract: the handle at `address` is alive while the caller's claim stands.
    unsafe { (*handle).clone() }
}

/// Waits on the calling thread, a plain thread, until `ready` answers: registers the thread's handle in
/// the word between two checks of `ready`, parks until a publisher has taken the registration and finished
/// reading the handle, and checks again. Returns only when no publisher can still read the handle: a
/// registration withdrawn after a publisher claimed it waits out the claim ([`withdraw`]).
pub(crate) fn wait_as_thread<R>(
    waiter: &impl WaiterWord,
    mut ready: impl FnMut() -> Option<R>,
) -> R {
    if let Some(answer) = ready() {
        return answer;
    }
    let me = std::thread::current();
    // `me` lives to the end of this frame, and the wait returns only once no publisher can still read it.
    wait_registered(waiter, thread_word(&me), ready, std::thread::park)
}

/// The wait itself, generic over the park so loom drives this exact code: registers `word` between two checks
/// of `ready`, parks until a publisher has taken the registration and emptied the word (while the word is still
/// this thread's, or claimed, that publisher's unpark is still to come, or the park returned spuriously), and
/// checks again.
pub(crate) fn wait_registered<R>(
    waiter: &impl WaiterWord,
    word: u64,
    mut ready: impl FnMut() -> Option<R>,
    park: impl Fn(),
) -> R {
    loop {
        register_thread(waiter, word);
        if let Some(answer) = ready() {
            withdraw(waiter, word, &park);
            return answer;
        }
        loop {
            park();
            let now = waiter.load_word(Ordering::Acquire);
            if now != word && now != CLAIMED {
                break;
            }
        }
        if let Some(answer) = ready() {
            return answer;
        }
    }
}

/// The thread's withdrawal of its registration `word` once it no longer needs a wake: returns at once when
/// no publisher had claimed it; otherwise parks (`park`) until the claiming publisher has finished reading the
/// handle and emptied the word, whose unpark follows. Every return finds the word not [`CLAIMED`].
fn withdraw(waiter: &impl WaiterWord, word: u64, park: &impl Fn()) {
    if waiter.cas_word(word, NO_WAITER).is_ok() {
        return;
    }
    while waiter.load_word(Ordering::Acquire) == CLAIMED {
        park();
    }
}

/// Forgets the waiting task, if any (a slot's next holder starts with no waiter). Only for a word no thread
/// can be registered in.
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
    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicU64;

    /// Format: the waiter's task word in the model.
    const WORD: u64 = 7;

    /// The second pass's finding 2, with the fences: a receiver registers its word and then checks the
    /// version, against a sender that bumps the version and then takes the waiter, every interleaving: the
    /// receiver either sees the new version or is woken — never neither.
    #[test]
    fn a_waiter_either_sees_the_change_or_is_woken() {
        loom::model(|| {
            let waiter = Arc::new(AtomicU64::new(NO_WAITER));
            let version = Arc::new(AtomicU64::new(0));
            let sender = {
                let (waiter, version) = (Arc::clone(&waiter), Arc::clone(&version));
                loom::thread::spawn(move || {
                    version.fetch_add(1, Ordering::Relaxed);
                    matches!(take(&*waiter, |_| ()), Taken::Task(WORD))
                })
            };
            register_task(&*waiter, WORD);
            let saw = version.load(Ordering::Relaxed) != 0;
            let woke = sender.join().unwrap();
            assert!(saw || woke, "the receiver slept through the change");
        });
    }

    /// Wakes counted across every explored interleaving of the thread model.
    static WOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// A thread waits for a value two publishers race to give it, its handle a cell on its "stack" that it
    /// empties when it leaves (loom reports an overlapping read of it). std's `park` may return spuriously, and
    /// a token an earlier wait left behind ends the next park at once, so the wait must hold up when no
    /// publisher woke it; loom's `park` returns only on a token, so a third thread raises a flag at any moment
    /// and a park that finds it raised returns at once. (The third thread does not unpark the waiter itself:
    /// loom's `unpark` also releases a thread blocked in a join, which loom's join then rejects.) In every
    /// interleaving the thread gets its value, no publisher reads the handle after the thread has left, and no
    /// registration is taken twice. Some interleaving woke the thread through the word, so the claim's path is
    /// not vacuous.
    #[test]
    fn a_thread_waiter_is_woken_once_and_its_handle_is_never_read_after_it_leaves() {
        crate::mem::loom_bounds::explore("handoff: a thread waiter, two publishers", || {
            let waiter = Arc::new(AtomicU64::new(NO_WAITER));
            let value = Arc::new(AtomicU64::new(0));
            let stack = Arc::new(UnsafeCell::new(Some(loom::thread::current())));
            // 1 while the waiting thread's frame lives. The read and the leaving both touch it, so loom orders them
            // both ways (its reduction reorders only operations on a common atomic, and a cell is none).
            let alive = Arc::new(AtomicU64::new(1));
            let word = thread_word(&*stack);
            let publishers: Vec<_> = (0..2)
                .map(|_| {
                    let (waiter, value, stack, alive) = (
                        Arc::clone(&waiter),
                        Arc::clone(&value),
                        Arc::clone(&stack),
                        Arc::clone(&alive),
                    );
                    loom::thread::spawn(move || {
                        value.fetch_add(1, Ordering::Relaxed);
                        let taken = take(&*waiter, |_| {
                            assert_eq!(
                                alive.load(Ordering::Relaxed),
                                1,
                                "a publisher read the handle after its thread left"
                            );
                            // SAFETY: the model's stand-in for the handle on the waiting thread's stack; loom
                            // reports a read that overlaps the thread's removal of it.
                            stack.with(|handle| unsafe { (*handle).clone() }.unwrap())
                        });
                        if let Taken::Thread(thread) = taken {
                            WOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            thread.unpark();
                        }
                    })
                })
                .collect();
            let spurious = Arc::new(loom::sync::atomic::AtomicBool::new(false));
            let stray = {
                let spurious = Arc::clone(&spurious);
                loom::thread::spawn(move || spurious.store(true, Ordering::Relaxed))
            };
            wait_registered(
                &*waiter,
                word,
                || (value.load(Ordering::Relaxed) > 0).then_some(()),
                || {
                    if !spurious.swap(false, Ordering::Relaxed) {
                        loom::thread::park();
                    }
                },
            );
            assert!(value.load(Ordering::Relaxed) > 0, "left without a value");
            // Leaving: the handle on the stack goes; a publisher's read now would overlap this write.
            alive.store(0, Ordering::Relaxed);
            // SAFETY: the model's stand-in for the thread's stack frame ending.
            stack.with_mut(|handle| unsafe { *handle = None });
            for publisher in publishers {
                publisher.join().unwrap();
            }
            stray.join().unwrap();
        });
        assert!(
            WOKEN.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "some interleaving woke the thread through its word"
        );
    }
}
