//! The local run queue: a ring of ready slot indices with one pending flag per slot, so a task woken
//! many times between polls appears once and the ring never holds more than the arena's slots (slates'
//! `queue.rs`; §4.3 "intrusive run queue"; no allocation on the wake path).
//!
//! Single-threaded by construction: only the owning shard's thread pushes and pops. The rings are of
//! `Cell`s (docs/runtime.md §3.4), so nothing is borrowed and nothing is refused: slates' version kept its
//! lists in `RefCell`s and counted the re-entrant accesses it refused.
//!
//! A step takes at most its configured poll budget, oldest first. Wakes that arrive while the phase
//! runs join the same FIFO behind tasks already ready and may use its remaining budget. A task waking
//! itself cannot extend that budget, so the phase never drains an unbounded ready set. `batch` can
//! still capture the number ready now for callers that need a fixed snapshot.
//!
//! A task that wakes itself in its own poll (a yield, [`crate::futures::yield_now`], or any future with
//! more to do) is listed apart, and joins the FIFO only when the loop has next read its inputs: its
//! control, wakes from other threads, timers, pollers and the driver's completions go ahead of it.
//! Re-listed in the FIFO at once, such a task took the rest of its step's budget, poll after poll, while
//! those inputs waited for the step to end (`tests/yield_defers.rs`; tokio defers its `yield_now` past
//! its driver's poll the same way, tokio 1.23 #5223). A slot is in one list at most (its flag), and each
//! list holds one entry per slot.

use std::cell::Cell;

/// A first-in-first-out ring of slot numbers, one entry per slot at most: [`crate::cells::CellRing`]'s
/// shape with the numbers held bare, since a slot is read only between the head and the length, so no entry
/// needs a mark of its own.
struct SlotRing {
    slots: Box<[Cell<u32>]>,
    /// The index of the oldest number.
    head: Cell<usize>,
    /// Numbers held.
    len: Cell<usize>,
}

impl SlotRing {
    /// A ring that holds at most `capacity` numbers.
    fn new(capacity: usize) -> Self {
        Self {
            slots: (0..capacity).map(|_| Cell::new(0)).collect(),
            head: Cell::new(0),
            len: Cell::new(0),
        }
    }

    fn len(&self) -> usize {
        self.len.get()
    }

    fn is_empty(&self) -> bool {
        self.len.get() == 0
    }

    /// Appends `slot`, or hands it back when the ring is full.
    #[inline]
    fn push(&self, slot: u32) -> Result<(), u32> {
        let len = self.len.get();
        let capacity = self.slots.len();
        if len >= capacity {
            return Err(slot);
        }
        // `head` and `len` are each below the capacity, so their sum is below twice it: one subtraction wraps it.
        let end = self.head.get().wrapping_add(len);
        let at = if end >= capacity {
            end.wrapping_sub(capacity)
        } else {
            end
        };
        let Some(cell) = self.slots.get(at) else {
            return Err(slot);
        };
        cell.set(slot);
        self.len.set(len.wrapping_add(1));
        Ok(())
    }

    /// Takes the oldest number.
    #[inline]
    fn pop(&self) -> Option<u32> {
        let len = self.len.get();
        if len == 0 {
            return None;
        }
        let head = self.head.get();
        let slot = self.slots.get(head)?.get();
        // `head` is below the capacity, so the next index is at most the capacity: it wraps by a comparison.
        let next = head.wrapping_add(1);
        self.head
            .set(if next >= self.slots.len() { 0 } else { next });
        self.len.set(len.wrapping_sub(1));
        Some(slot)
    }
}

/// The queue.
pub struct LocalQueue {
    pending: Box<[Cell<bool>]>,
    /// The two lists, each holding one entry per slot: `lists[ready]` the ready FIFO, the other the slots
    /// that woke themselves since the loop last released them. A release onto an empty ready FIFO swaps the
    /// lists' roles rather than moving each entry, the case of a round of yields whose batch drained the FIFO
    /// before reading its inputs again.
    lists: [SlotRing; 2],
    /// Which of `lists` is the ready FIFO: 0 or 1.
    ready: Cell<usize>,
    /// Slots `push` has listed, a count the loop reads twice to tell whether polls between listed any.
    listed: Cell<u64>,
    overflow: Cell<u64>,
}

impl std::fmt::Debug for LocalQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalQueue")
            .field("capacity", &self.pending.len())
            .field("ready", &self.ready_list().len())
            .field("yielded", &self.yielded_list().len())
            .finish()
    }
}

impl LocalQueue {
    /// A queue for an arena of `capacity` slots.
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: (0..capacity).map(|_| Cell::new(false)).collect(),
            lists: [SlotRing::new(capacity), SlotRing::new(capacity)],
            ready: Cell::new(0),
            listed: Cell::new(0),
            overflow: Cell::new(0),
        }
    }

    /// The ready FIFO.
    #[inline]
    fn ready_list(&self) -> &SlotRing {
        let [first, second] = &self.lists;
        if self.ready.get() == 0 { first } else { second }
    }

    /// The slots that woke themselves, waiting for their release.
    #[inline]
    fn yielded_list(&self) -> &SlotRing {
        let [first, second] = &self.lists;
        if self.ready.get() == 0 { second } else { first }
    }

    /// Marks a slot ready; a slot already pending is not listed twice. A slot beyond the arena is counted as
    /// overflow and ignored (a stale or foreign word).
    #[inline]
    pub fn push(&self, slot: u32) {
        let Some(flag) = self
            .pending
            .get(usize::try_from(slot).unwrap_or(usize::MAX))
        else {
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        };
        if flag.replace(true) {
            return;
        }
        // A slot is in the ring at most once (its flag), and the ring holds one entry per slot, so it never
        // refuses; were it to, the flag is cleared so the next wake lists the slot again.
        if self.ready_list().push(slot).is_err() {
            flag.set(false);
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        }
        // Compared only with an earlier reading: 2^64 listings between two readings cannot happen.
        self.listed.set(self.listed.get().wrapping_add(1));
    }

    /// The slots `push` has listed so far: two readings differ when it listed any between them.
    pub fn listed(&self) -> u64 {
        self.listed.get()
    }

    /// Marks a slot ready behind the loop's next reading of its inputs: the task woke itself in its own
    /// poll. A slot already pending keeps its place; one beyond the arena is counted as overflow and
    /// ignored.
    #[inline]
    pub fn defer(&self, slot: u32) {
        let Some(flag) = self
            .pending
            .get(usize::try_from(slot).unwrap_or(usize::MAX))
        else {
            self.overflow.set(self.overflow.get().saturating_add(1));
            return;
        };
        if flag.replace(true) {
            return;
        }
        // Never refused, as for `push`: one entry per slot, and the list holds one for every slot.
        if self.yielded_list().push(slot).is_err() {
            flag.set(false);
            self.overflow.set(self.overflow.get().saturating_add(1));
        }
    }

    /// Lists every slot that woke itself behind the slots ready now, oldest first: the loop calls it once its
    /// inputs are in the queue.
    pub fn release_yielded(&self) {
        // Behind nothing: the lists swap roles, the released slots in the order they yielded.
        if self.ready_list().is_empty() {
            self.ready.set(if self.ready.get() == 0 { 1 } else { 0 });
            return;
        }
        let (ready, yielded) = (self.ready_list(), self.yielded_list());
        while let Some(slot) = yielded.pop() {
            // Never refused: a slot is in one list at most, so both hold no more than the arena's slots.
            if ready.push(slot).is_err() {
                if let Some(flag) = self
                    .pending
                    .get(usize::try_from(slot).unwrap_or(usize::MAX))
                {
                    flag.set(false);
                }
                self.overflow.set(self.overflow.get().saturating_add(1));
            }
        }
    }

    /// Whether a slot that woke itself waits for its release.
    pub fn has_yielded(&self) -> bool {
        !self.yielded_list().is_empty()
    }

    /// How many slots the step about to run may take: at most `limit`, and only those ready now, so a wake
    /// during the batch waits for the next step.
    pub fn batch(&self, limit: usize) -> usize {
        self.ready_list().len().min(limit)
    }

    /// Takes the oldest ready slot, clearing its pending flag so a wake during its poll lists it again.
    #[inline]
    pub fn pop(&self) -> Option<u32> {
        let slot = self.ready_list().pop()?;
        if let Some(flag) = self
            .pending
            .get(usize::try_from(slot).unwrap_or(usize::MAX))
        {
            flag.set(false);
        }
        Some(slot)
    }

    /// Whether nothing is ready, yielded tasks included.
    pub fn is_empty(&self) -> bool {
        self.ready_list().is_empty() && self.yielded_list().is_empty()
    }

    /// Ready entries, yielded tasks included.
    pub fn len(&self) -> usize {
        self.ready_list()
            .len()
            .saturating_add(self.yielded_list().len())
    }

    /// Wakes for slots beyond the arena, ignored.
    pub fn overflow(&self) -> u64 {
        self.overflow.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take(q: &LocalQueue, limit: usize) -> Vec<u32> {
        let n = q.batch(limit);
        (0..n).filter_map(|_| q.pop()).collect()
    }

    #[test]
    fn duplicates_collapse_and_drain_returns_in_order() {
        let q = LocalQueue::new(4);
        q.push(2);
        q.push(0);
        q.push(2);
        q.push(3);
        assert_eq!(q.len(), 3);
        assert_eq!(take(&q, usize::MAX), vec![2, 0, 3]);
        assert!(q.is_empty());
        q.push(2);
        assert_eq!(take(&q, usize::MAX), vec![2]);
    }

    #[test]
    fn a_slot_beyond_the_arena_is_counted_not_listed() {
        let q = LocalQueue::new(2);
        q.push(9);
        assert_eq!(q.overflow(), 1);
        assert!(q.is_empty());
    }

    #[test]
    fn a_wake_during_a_drain_lands_in_the_next_batch() {
        let q = LocalQueue::new(3);
        q.push(1);
        let n = q.batch(usize::MAX);
        assert_eq!(n, 1);
        assert_eq!(q.pop(), Some(1));
        q.push(1);
        assert_eq!(take(&q, usize::MAX), vec![1]);
    }

    /// A yielded slot waits for its release, then goes behind every slot listed before it, and a wake while it
    /// waits does not list it twice. Do: yield 1, wake 2 and 1, release, wake 3. Expect: 2, 1, 3.
    #[test]
    fn a_yielded_slot_goes_behind_what_was_ready_at_its_release() {
        let q = LocalQueue::new(4);
        q.defer(1);
        assert!(!q.is_empty(), "a yielded slot is work");
        assert_eq!(q.pop(), None, "but not before its release");
        q.push(2);
        q.push(1);
        assert_eq!(q.len(), 2, "the wake of a yielded slot collapsed");
        q.release_yielded();
        q.push(3);
        assert_eq!(take(&q, usize::MAX), vec![2, 1, 3]);
        assert!(q.is_empty());
    }

    /// §4.3 bounded work: a drain hands out at most its batch, oldest first; the slots left behind keep
    /// their places ahead of any wake that arrives while the batch runs, and a wake for one of them is
    /// still collapsed. Do: queue five, take two, wake one of the left behind and a new one. Expect:
    /// the first two, then the other three in order, then the new one.
    #[test]
    fn a_drain_takes_at_most_its_batch_and_the_rest_keep_their_places() {
        let q = LocalQueue::new(8);
        for slot in 0..5 {
            q.push(slot);
        }
        assert_eq!(take(&q, 2), vec![0, 1]);
        assert_eq!(q.len(), 3, "the other three wait, not re-queued");
        q.push(3);
        q.push(7);
        assert_eq!(
            take(&q, 8),
            vec![2, 3, 4, 7],
            "FIFO; the repeated wake collapsed"
        );
    }
}
