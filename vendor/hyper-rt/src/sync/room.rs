//! The senders a bounded channel holds waiting for room (docs/runtime.md §8; mantle
//! `docs/design/event-loop.md` D4): one slot each, at most the channel's `waiters`, with no lock on any path.
//!
//! **A slot per waiting sender, freed in place.** A sender that finds the channel full takes a free slot
//! (compare-and-swap, free → waiting) and waits in it: a task registers its word in the slot's waiter word,
//! a plain thread parks with its handle registered there (`crate::handoff`). A sender that stops waiting
//! without a grant (its own retry found room, or its future was dropped) frees its slot on the spot, so the
//! bound is exactly the senders waiting now. The queue this replaces kept a sender's entry until the
//! receiver's grants walked past it, so entries no one waited on held places, and a full queue refused live
//! senders.
//!
//! **Grants go round the slots.** Whoever makes room (the receiver after each take, a sender handing on
//! room it was granted and did not use) grants one waiting slot (waiting → granted) and wakes its waiter.
//! The search starts after the last slot granted, so a waiting sender is passed over at most once a round:
//! every waiter is granted within `waiters` grants of its slot being seen.
//!
//! **No lost room.** The waiter takes its slot, then checks the channel again; the receiver frees a value's
//! slot, then looks at the waiters. Each side writes, then reads what the other wrote, the store-buffering
//! shape, so each puts a `SeqCst` fence between its write and its read (as [`crate::handoff`] and
//! [`crate::parking`] do): whichever fence comes first, the side after it sees the other's write, so either
//! the sender's check finds the room or the grant finds the sender. A waiter counts itself in `waiting`
//! before its fence, so the receiver reads one word after its fence while no one waits.
//!
//! **A grant not seen is handed on.** A grant can reach a place whose sender has just sent on room made
//! earlier; a sender letting its place go hands on any grant it did not see before it sent ([`Room::leave`]).

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

use crate::error::RtError;
use crate::handoff;

/// Format: a slot no sender holds.
const FREE: u64 = 0;
/// Format: a slot whose sender waits for room.
const WAITING: u64 = 1;
/// Format: a slot whose sender was granted room and has not yet let the slot go.
const GRANTED: u64 = 2;

/// One waiting sender's place.
#[derive(Debug)]
struct Slot {
    /// [`FREE`], [`WAITING`] or [`GRANTED`].
    state: AtomicU64,
    /// The waiting sender's waiter word (`crate::handoff`).
    waiter: AtomicU64,
}

/// The senders waiting for room.
#[derive(Debug)]
pub(crate) struct Room {
    slots: Box<[Slot]>,
    /// Slots in [`WAITING`]: a grant reads it after its fence and searches only when some sender waits.
    waiting: AtomicUsize,
    /// Where the next grant's search starts: one past the last slot granted.
    cursor: AtomicUsize,
}

impl Room {
    /// Room for `waiters` senders waiting at once. Refused `BadConfig` for none, or for more than one
    /// allocation can lay out.
    pub(crate) fn new(waiters: usize) -> Result<Self, RtError> {
        let refused = RtError::BadConfig {
            what: "a channel with no room for a waiting sender, or more than an allocation holds",
        };
        if waiters == 0 {
            return Err(refused);
        }
        // The slots' allocation must be a valid layout, or collecting them would abort on the overflow.
        std::alloc::Layout::array::<Slot>(waiters).map_err(|_| refused)?;
        Ok(Self {
            slots: (0..waiters)
                .map(|_| Slot {
                    state: AtomicU64::new(FREE),
                    waiter: AtomicU64::new(handoff::NO_WAITER),
                })
                .collect(),
            waiting: AtomicUsize::new(0),
            cursor: AtomicUsize::new(0),
        })
    }

    /// Takes a free slot for a sender about to wait, counted and fenced: the caller's next look at the
    /// channel is ordered after its slot shows waiting to every grant. `None` when every slot is held.
    pub(crate) fn hold(&self) -> Option<usize> {
        let index = self.slots.iter().position(|slot| {
            slot.state
                .compare_exchange(FREE, WAITING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })?;
        self.counted();
        Some(index)
    }

    /// The held slot `index`, granted, waits again: its room went to another sender. Counted and fenced as
    /// [`Room::hold`].
    pub(crate) fn wait_again(&self, index: usize) {
        if let Some(slot) = self.slots.get(index) {
            // A granted slot is its holder's alone: no grant or holder writes it meanwhile.
            slot.state.store(WAITING, Ordering::Release);
            self.counted();
        }
    }

    /// Counts a slot that went to waiting, then fences (module doc).
    fn counted(&self) {
        self.waiting.fetch_add(1, Ordering::AcqRel);
        fence(Ordering::SeqCst);
    }

    /// Whether the held slot `index` was granted room.
    pub(crate) fn granted(&self, index: usize) -> bool {
        self.slots
            .get(index)
            .is_some_and(|slot| slot.state.load(Ordering::Acquire) == GRANTED)
    }

    /// Registers the task `word` as the held slot's waiter (fenced, `crate::handoff`).
    pub(crate) fn register_task(&self, index: usize, word: u64) {
        if let Some(slot) = self.slots.get(index) {
            handoff::register_task(&slot.waiter, word);
        }
    }

    /// A plain thread's send that found the channel full and holds `place` ([`Room::hold`]): tries `put` (the
    /// send's outcome, or the value back while the channel is full), parks through `park` in the place until
    /// it is granted (`park` adds its own reasons to stop, such as the receiver's end), tries again, and waits
    /// again in its place when the granted room went to another sender. When `put` ends the send the place is
    /// let go, a grant it did not use handed on through `wake` ([`Room::leave`]). Each wait after the first
    /// follows a grant, so the loop advances only as the channel's taker does. The value back for a place the
    /// room does not have (one `hold` never returns).
    pub(crate) fn send_waiting<V, R>(
        &self,
        place: usize,
        value: V,
        mut put: impl FnMut(V) -> Result<R, V>,
        mut park: impl FnMut(&AtomicU64, &dyn Fn() -> bool),
        wake: impl Fn(&AtomicU64),
    ) -> Result<R, V> {
        let Some(slot) = self.slots.get(place) else {
            return Err(value);
        };
        let mut value = value;
        loop {
            // The place shows waiting to every grant before this look (`Room::hold`, `Room::wait_again`).
            value = match put(value) {
                Ok(done) => {
                    self.leave_with(place, false, &wake);
                    return Ok(done);
                }
                Err(value) => value,
            };
            park(&slot.waiter, &|| self.granted(place));
            let granted = self.granted(place);
            value = match put(value) {
                Ok(done) => {
                    self.leave_with(place, granted, &wake);
                    return Ok(done);
                }
                Err(value) => value,
            };
            // Granted, and another sender took the room first: wait again in the same place.
            self.wait_again(place);
        }
    }

    /// A sender lets its place go. Room granted to the place goes on to another waiting sender unless the
    /// sender `used` it: saw the grant, then sent. A grant the sender did not see may be for room it did not
    /// take: a grant can reach a place whose sender has just sent on room made earlier and has not yet let the
    /// place go, and keeping it would leave another sender waiting with room free and no grant coming (loom's
    /// two-sender model below found it). Handing on a grant that was the sender's own costs the next waiter
    /// one look at a full channel.
    pub(crate) fn leave(&self, place: usize, used: bool) {
        self.leave_with(place, used, &|waiter: &AtomicU64| {
            handoff::wake_waiter(waiter)
        });
    }

    /// [`Room::leave`], waking through `wake`.
    fn leave_with(&self, place: usize, used: bool, wake: &impl Fn(&AtomicU64)) {
        if self.release(place) && !used {
            self.grant_one_with(wake);
        }
    }

    /// Lets the held slot `index` go, and says whether it had been granted room. A task's registration is
    /// cleared first (a thread's wait has already withdrawn its own), so the slot's next holder is not woken
    /// for this one.
    fn release(&self, index: usize) -> bool {
        let Some(slot) = self.slots.get(index) else {
            return false;
        };
        handoff::clear_waiter(&slot.waiter);
        if slot
            .state
            .compare_exchange(WAITING, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        // Granted: only a grant leaves waiting, and the holder alone leaves granted.
        slot.state.store(FREE, Ordering::Release);
        true
    }

    /// Grants room to one waiting sender and wakes it, if one waits; for whoever just made room, after the
    /// room is published. Whether one was granted.
    pub(crate) fn grant_one(&self) -> bool {
        self.grant_one_with(&|waiter: &AtomicU64| handoff::wake_waiter(waiter))
    }

    /// [`Room::grant_one`], waking through `wake` (loom drives this exact code with its own threads).
    fn grant_one_with(&self, wake: &impl Fn(&AtomicU64)) -> bool {
        fence(Ordering::SeqCst);
        if self.waiting.load(Ordering::Acquire) == 0 {
            return false;
        }
        let count = self.slots.len();
        let start = self.cursor.load(Ordering::Relaxed);
        for step in 0..count {
            let index = start.wrapping_add(step).checked_rem(count).unwrap_or(0);
            let Some(slot) = self.slots.get(index) else {
                continue;
            };
            if slot.state.load(Ordering::Acquire) == WAITING
                && slot
                    .state
                    .compare_exchange(WAITING, GRANTED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.waiting.fetch_sub(1, Ordering::AcqRel);
                self.cursor.store(index.wrapping_add(1), Ordering::Relaxed);
                wake(&slot.waiter);
                return true;
            }
        }
        false
    }

    /// Wakes every waiting sender to see the channel closed (the receiver went, after saying so).
    pub(crate) fn wake_all(&self) {
        self.wake_all_with(handoff::wake_waiter);
    }

    /// [`Room::wake_all`], waking through `wake`.
    fn wake_all_with(&self, wake: impl Fn(&AtomicU64)) {
        fence(Ordering::SeqCst);
        for slot in &*self.slots {
            if slot.state.load(Ordering::Acquire) == WAITING {
                wake(&slot.waiter);
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    /// A slot is held, granted once, waits again, and is let go; a full room refuses; a grant goes round.
    #[test]
    fn slots_are_held_exactly_and_granted_round_the_room() {
        let room = Room::new(2).unwrap();
        assert!(!room.grant_one(), "no one waits");
        let first = room.hold().unwrap();
        let second = room.hold().unwrap();
        assert_eq!(room.hold(), None, "two slots, two holders");
        assert!(room.grant_one());
        assert!(room.granted(first) && !room.granted(second));
        assert!(room.grant_one(), "the next grant goes to the other holder");
        assert!(room.granted(second));
        assert!(!room.grant_one(), "no one waits now");
        room.wait_again(first);
        assert!(!room.granted(first));
        assert!(!room.release(first), "let go without a grant");
        assert!(room.release(second), "let go after its grant");
        assert_eq!(room.waiting.load(Ordering::Relaxed), 0);
        assert!(
            room.hold().is_some() && room.hold().is_some(),
            "both slots free again"
        );
        assert!(Room::new(0).is_err());
    }
}

#[cfg(test)]
#[cfg(loom)]
// The model keeps each sender's thread handle in loom's cell, whose accesses are `unsafe` by signature; the
// module's own code has none.
#[allow(unsafe_code)]
mod loom_tests {
    use super::*;
    use crate::mem::loom_bounds;
    use crate::sync::ring::{Refused, Ring};
    use loom::cell::UnsafeCell;

    /// A sender waits for room on a full channel of one value while the receiver takes the value and grants,
    /// each on a thread of its own (the receiver spawned first, so loom's first execution has it take before
    /// the sender looks: loom's reduction reorders a later store before an earlier load only when it saw the
    /// store first, as the ring's model notes). In every interleaving the sender's value goes in and the sender
    /// returns: its look after taking a place finds the room, or the grant finds it waiting and wakes it. With
    /// either fence removed loom finds the interleaving where both miss and the sender sleeps for good
    /// (`benchmark-results/hyper-rt-vs-tokio-20261010/tests/loom-channel.txt`).
    #[test]
    fn a_sender_waiting_for_room_is_granted_or_finds_it() {
        loom_bounds::explore(
            "room: a sender waiting on a full channel, a receiver taking",
            || {
                let values: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
                let room: &'static Room = Box::leak(Box::new(Room::new(1).unwrap()));
                values.push(1).unwrap();
                let stacks: &'static [Stack; 1] = Box::leak(Box::new([Stack::new()]));
                let receiver = loom::thread::spawn(move || {
                    assert_eq!(values.pop(), Some(1));
                    room.grant_one_with(&|waiter: &AtomicU64| wake_in(stacks, waiter));
                });
                let sender =
                    loom::thread::spawn(move || send_blocking(values, room, stacks, &stacks[0], 2));
                receiver.join().unwrap();
                sender.join().unwrap();
                assert_eq!(values.pop(), Some(2));
            },
        );
    }

    /// A sender's handle on its "stack" for the model: the cell its waiter word names, and whether its frame
    /// still lives (the reader checks it, so loom orders the read against the leaving).
    struct Stack {
        handle: UnsafeCell<Option<loom::thread::Thread>>,
        alive: loom::sync::atomic::AtomicU64,
    }

    impl Stack {
        fn new() -> Self {
            Self {
                handle: UnsafeCell::new(None),
                alive: loom::sync::atomic::AtomicU64::new(1),
            }
        }
    }

    /// A model sender: sends `value` as `Sender::blocking_send` does, through the same `Room::send_waiting`,
    /// parking as a loom thread whose handle is the model's.
    fn send_blocking(values: &Ring<u32>, room: &Room, stacks: &[Stack], stack: &Stack, value: u32) {
        stack.handle.with_mut(|handle| {
            // SAFETY: the model's stand-in for the sender's handle on its own stack, written before the
            // sender registers it (the registration orders it for a reader).
            unsafe { *handle = Some(loom::thread::current()) }
        });
        // The model's ring is never closed: a refusal is a full ring.
        if let Err(Refused::Full(value) | Refused::Closed(value)) = values.push(value) {
            let place = room.hold().unwrap();
            let word = handoff::thread_word(stack);
            room.send_waiting(
                place,
                value,
                |value| {
                    values
                        .push(value)
                        .map_err(|(Refused::Full(value) | Refused::Closed(value))| value)
                },
                |waiter, granted| {
                    handoff::wait_registered(
                        waiter,
                        word,
                        || granted().then_some(()),
                        loom::thread::park,
                    );
                },
                |waiter| wake_in(stacks, waiter),
            )
            .unwrap();
        }
        leave_stack(stack);
    }

    /// The model sender's frame ends: its handle goes, after which no reader may touch it.
    fn leave_stack(stack: &Stack) {
        stack.alive.store(0, Ordering::Relaxed);
        // SAFETY: the model's stand-in for the sender's frame ending.
        stack.handle.with_mut(|handle| unsafe { *handle = None });
    }

    /// Wakes the model sender whose handle `waiter` names, as `handoff::wake_waiter` wakes a parked thread.
    fn wake_in(stacks: &[Stack], waiter: &AtomicU64) {
        let taken = handoff::take(waiter, |word| {
            let stack = stacks
                .iter()
                .find(|stack| handoff::thread_word(*stack) == word)
                .unwrap();
            assert_eq!(stack.alive.load(Ordering::Relaxed), 1, "read after leaving");
            stack.handle.with(|handle| {
                // SAFETY: the model's stand-in for the handle on the waiting sender's stack; loom reports a
                // read that overlaps the sender's writes of it.
                unsafe { (*handle).clone() }.unwrap()
            })
        });
        if let handoff::Taken::Thread(thread) = taken {
            thread.unpark();
        }
    }

    /// Two senders wait for room on a full channel of one value while the receiver takes twice, granting after
    /// each take: a grant can go to a sender whose room the other then takes, which must wait again in its
    /// place. In every interleaving both values go in, both senders return, and no handle is read after its
    /// sender left.
    #[test]
    fn two_senders_waiting_for_room_both_get_in() {
        loom_bounds::explore(
            "room: two senders waiting on a full channel, a receiver taking twice",
            || {
                let values: &'static Ring<u32> = Box::leak(Box::new(Ring::new(1).unwrap()));
                let room: &'static Room = Box::leak(Box::new(Room::new(2).unwrap()));
                values.push(0).unwrap();
                let stacks: &'static [Stack; 2] = Box::leak(Box::new([Stack::new(), Stack::new()]));
                let receiver = loom::thread::spawn(move || {
                    let mut taken = Vec::new();
                    while taken.len() < 2 {
                        match values.pop() {
                            Some(value) => {
                                taken.push(value);
                                room.grant_one_with(&|waiter: &AtomicU64| wake_in(stacks, waiter));
                            }
                            None => loom::thread::yield_now(),
                        }
                    }
                    taken
                });
                let senders: Vec<_> = [1u32, 2]
                    .into_iter()
                    .zip(stacks.iter())
                    .map(|(value, stack)| {
                        loom::thread::spawn(move || {
                            send_blocking(values, room, stacks, stack, value)
                        })
                    })
                    .collect();
                for sender in senders {
                    sender.join().unwrap();
                }
                let mut taken = receiver.join().unwrap();
                taken.extend(values.pop());
                taken.sort_unstable();
                assert_eq!(taken, vec![0, 1, 2]);
            },
        );
    }
}
