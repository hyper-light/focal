//! Who waits on which handle, in which direction (docs/runtime.md §3.7): the loop's table between the
//! tasks' readiness waits and the driver's one registration per handle.
//!
//! A driver keeps one registration per handle: epoll's `EPOLL_CTL_MOD` replaces the mask and the user word
//! together, kqueue's `EV_ADD` replaces a filter's `udata`, and an AFD poll is one request per socket. So
//! the loop keeps the waiters itself and arms the driver once per handle with the union of the directions
//! waited for; a completion says which directions fired, and each waiter of those is woken. A reader and a
//! writer of one full-duplex stream, or two readers of one socket, each get their wake (mantle's review of
//! hyper-rt, finding 2: a writable registration used to erase the readable one, and the reader slept for
//! good).
//!
//! **One node per wait** (mantle's final review, finding 1): two waits of one task on one handle and
//! direction — a `join` of a read with a `race` of another read and a timer — are two nodes, each named by
//! its wait's ticket, so the race's loser withdraws its own node and the join's still fires. A task's word
//! used to be the node's identity, and the loser's withdrawal stranded the survivor.
//!
//! **Bounded, and allocation-free after the build**: at most `bound` waiters (the shard's
//! `interests_per_shard`, which also bounds the desk's wait slots, so the table is never the first to
//! refuse), in an arena of that many nodes with a free list, indexed by an open-addressed table of
//! handles this module owns: a power of two at least twice `bound`, linear probing, and deletion by
//! backward shift, so no tombstone accumulates and nothing is reallocated after the build (a std map may
//! resize when its tombstones use up its growth room, which the earlier claim missed).

use crate::error::RtError;
use crate::shard::Ticket;

/// Format: the bit that marks a completion's `user_data` as a handle's tag, not a task's word. A task word
/// is `shard:16 | slot:24 | generation:24` with the shard below `registry::MAX_SHARDS` (1,024), so its top
/// bit is never set.
pub const HANDLE_TAG: u64 = 1 << 63;

/// The tag a handle's readiness completions carry.
pub fn tag_of(raw: i32) -> u64 {
    HANDLE_TAG | u64::from(u32::from_ne_bytes(raw.to_ne_bytes()))
}

/// The handle a completion's tag names, if it is one.
pub fn handle_of(user_data: u64) -> Option<i32> {
    if user_data & HANDLE_TAG == 0 {
        return None;
    }
    let low = u32::try_from(user_data & u64::from(u32::MAX)).ok()?;
    Some(i32::from_ne_bytes(low.to_ne_bytes()))
}

/// Directions of readiness, as a set.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Readiness(u8);

impl Readiness {
    /// Nothing.
    pub const NONE: Readiness = Readiness(0);
    /// Readable: data, a connection to accept, end of stream, an error.
    pub const READ: Readiness = Readiness(1);
    /// Writable: send-buffer space, a connect's result, an error.
    pub const WRITE: Readiness = Readiness(2);

    /// One direction.
    pub const fn of(writable: bool) -> Readiness {
        if writable { Self::WRITE } else { Self::READ }
    }

    /// Whether `other`'s directions are all in this set.
    pub const fn contains(self, other: Readiness) -> bool {
        self.0 & other.0 == other.0 && other.0 != 0
    }

    /// Whether the set is empty.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Both sets' directions.
    #[must_use]
    pub const fn union(self, other: Readiness) -> Readiness {
        Readiness(self.0 | other.0)
    }

    /// The set as bits, for a completion's `result`.
    pub fn bits(self) -> i32 {
        i32::from(self.0)
    }

    /// The set a completion's `result` names.
    pub fn from_bits(bits: i32) -> Readiness {
        Readiness(u8::try_from(bits & 0b11).unwrap_or(0))
    }
}

/// Format: the end of a list, and an empty table slot.
const END: u32 = u32::MAX;

/// One waiter: its task word, its wait's ticket, and the next waiter of the same handle and direction.
#[derive(Clone, Copy, Debug)]
struct Node {
    word: u64,
    ticket: Ticket,
    next: u32,
}

/// One handle's waiters: a list per direction.
#[derive(Clone, Copy, Debug)]
struct Lists {
    read: u32,
    write: u32,
}

impl Lists {
    /// Format: no waiter in either direction.
    const EMPTY: Lists = Lists {
        read: END,
        write: END,
    };

    fn head(&mut self, writable: bool) -> &mut u32 {
        if writable {
            &mut self.write
        } else {
            &mut self.read
        }
    }

    /// The directions with waiters.
    fn wanted(self) -> Readiness {
        let read = if self.read == END {
            Readiness::NONE
        } else {
            Readiness::READ
        };
        let write = if self.write == END {
            Readiness::NONE
        } else {
            Readiness::WRITE
        };
        read.union(write)
    }
}

/// One slot of the handle table: a handle and its lists, or empty.
#[derive(Clone, Copy, Debug)]
struct Entry {
    raw: i32,
    lists: Lists,
    used: bool,
}

/// Format: an empty slot of the handle table.
const VACANT: Entry = Entry {
    raw: 0,
    lists: Lists::EMPTY,
    used: false,
};

/// The table.
#[derive(Debug)]
pub(crate) struct Interests {
    nodes: Vec<Node>,
    free: Vec<u32>,
    handles: Box<[Entry]>,
    /// Handles in the table now.
    used: usize,
    bound: usize,
}

/// The table's home slot for `raw` among `slots` (a power of two): Fibonacci hashing of the handle's
/// bits (Knuth, TAOCP vol. 3 §6.4), so consecutive descriptors spread instead of clustering.
fn home(raw: i32, slots: usize) -> usize {
    let bits = u64::from(u32::from_ne_bytes(raw.to_ne_bytes()));
    let mixed = bits.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let shift = 64u32.saturating_sub(slots.trailing_zeros());
    usize::try_from(mixed.checked_shr(shift).unwrap_or(0)).unwrap_or(0)
}

impl Interests {
    /// A table for at most `bound` waiters, reserved now. `Capacity` when the reservation fails.
    pub(crate) fn new(bound: usize) -> Result<Interests, RtError> {
        let refused = || RtError::Capacity {
            what: "readiness waiters",
            bound,
        };
        let mut nodes = Vec::new();
        nodes.try_reserve_exact(bound).map_err(|_| refused())?;
        let mut free = Vec::new();
        free.try_reserve_exact(bound).map_err(|_| refused())?;
        // At most `bound` handles at a load of a half or less: probes stay short and one is always vacant.
        let slots = bound
            .max(1)
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(refused)?;
        let mut handles = Vec::new();
        handles.try_reserve_exact(slots).map_err(|_| refused())?;
        handles.resize(slots, VACANT);
        Ok(Interests {
            nodes,
            free,
            handles: handles.into_boxed_slice(),
            used: 0,
            bound,
        })
    }

    /// Whether no task waits on any handle: then nothing is armed in the driver for this table, and the
    /// driver has no readiness to report but a fire to no one (`remove`'s note).
    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.len() == self.free.len()
    }

    /// Waiters now.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.len().saturating_sub(self.free.len())
    }

    /// The table slot holding `raw`, if it is there.
    fn find(&self, raw: i32) -> Option<usize> {
        let slots = self.handles.len();
        let mask = slots.checked_sub(1)?;
        let mut at = home(raw, slots);
        for _ in 0..slots {
            let entry = self.handles.get(at)?;
            if !entry.used {
                return None;
            }
            if entry.raw == raw {
                return Some(at);
            }
            at = at.wrapping_add(1) & mask;
        }
        None
    }

    fn lists(&self, raw: i32) -> Lists {
        self.find(raw)
            .and_then(|at| self.handles.get(at))
            .map_or(Lists::EMPTY, |entry| entry.lists)
    }

    /// Stores `raw`'s lists, or forgets the handle when none waits; the directions wanted. `Capacity` when
    /// a new handle would pass the bound.
    fn store(&mut self, raw: i32, lists: Lists) -> Result<Readiness, RtError> {
        let wanted = lists.wanted();
        match (self.find(raw), wanted.is_empty()) {
            (Some(at), true) => self.vacate(at),
            (Some(at), false) => {
                if let Some(entry) = self.handles.get_mut(at) {
                    entry.lists = lists;
                }
            }
            (None, true) => {}
            (None, false) => self.insert(raw, lists)?,
        }
        Ok(wanted)
    }

    fn insert(&mut self, raw: i32, lists: Lists) -> Result<(), RtError> {
        let refused = RtError::Capacity {
            what: "readiness handles",
            bound: self.bound,
        };
        if self.used >= self.bound {
            return Err(refused);
        }
        let slots = self.handles.len();
        let mask = slots.checked_sub(1).ok_or(refused.clone())?;
        let mut at = home(raw, slots);
        for _ in 0..slots {
            let Some(entry) = self.handles.get_mut(at) else {
                break;
            };
            if !entry.used {
                *entry = Entry {
                    raw,
                    lists,
                    used: true,
                };
                self.used = self.used.saturating_add(1);
                return Ok(());
            }
            at = at.wrapping_add(1) & mask;
        }
        Err(refused)
    }

    /// Empties slot `at` and shifts back the entries after it whose probe passed through it (Knuth's
    /// Algorithm R, TAOCP vol. 3 §6.4): no tombstone is left.
    fn vacate(&mut self, at: usize) {
        let slots = self.handles.len();
        let Some(mask) = slots.checked_sub(1) else {
            return;
        };
        let mut hole = at;
        let mut next = at.wrapping_add(1) & mask;
        for _ in 0..slots {
            let Some(entry) = self.handles.get(next).copied() else {
                break;
            };
            if !entry.used {
                break;
            }
            let want = home(entry.raw, slots);
            // `entry` may fill the hole when its home is not cyclically within (hole, next].
            let distance_home = next.wrapping_sub(want) & mask;
            let distance_hole = next.wrapping_sub(hole) & mask;
            if distance_home >= distance_hole {
                if let Some(slot) = self.handles.get_mut(hole) {
                    *slot = entry;
                }
                hole = next;
            }
            next = next.wrapping_add(1) & mask;
        }
        if let Some(slot) = self.handles.get_mut(hole) {
            *slot = VACANT;
        }
        self.used = self.used.saturating_sub(1);
    }

    /// A free node holding `word` and `ticket`, or `Capacity`.
    fn node(&mut self, word: u64, ticket: Ticket, next: u32) -> Result<u32, RtError> {
        let fresh = Node { word, ticket, next };
        if let Some(index) = self.free.pop()
            && let Some(node) = self
                .nodes
                .get_mut(usize::try_from(index).unwrap_or(usize::MAX))
        {
            *node = fresh;
            return Ok(index);
        }
        let refused = RtError::Capacity {
            what: "readiness waiters",
            bound: self.bound,
        };
        if self.nodes.len() >= self.bound {
            return Err(refused);
        }
        let index = u32::try_from(self.nodes.len()).map_err(|_| refused)?;
        // Within the reservation: no allocation.
        self.nodes.push(fresh);
        Ok(index)
    }

    fn get(&self, index: u32) -> Option<Node> {
        self.nodes.get(usize::try_from(index).ok()?).copied()
    }

    /// Adds the wait `ticket` names, for task `word`, as a waiter of `raw` in one direction; the directions
    /// now wanted of `raw`, which the loop arms. Every wait is its own node.
    pub(crate) fn add(
        &mut self,
        raw: i32,
        writable: bool,
        word: u64,
        ticket: Ticket,
    ) -> Result<Readiness, RtError> {
        let mut lists = self.lists(raw);
        let head = *lists.head(writable);
        let index = self.node(word, ticket, head)?;
        *lists.head(writable) = index;
        self.store(raw, lists).inspect_err(|_| {
            // Room for it: it came off the free list or the reserved arena.
            self.free.push(index);
        })
    }

    /// Removes the wait `ticket` names from `raw`'s waiters in one direction (a wait dropped before it
    /// fired); the directions still wanted.
    pub(crate) fn remove(&mut self, raw: i32, writable: bool, ticket: Ticket) -> Readiness {
        let Some(at) = self.find(raw) else {
            return Readiness::NONE;
        };
        let mut lists = self
            .handles
            .get(at)
            .map_or(Lists::EMPTY, |entry| entry.lists);
        let mut previous = END;
        let mut at_node = *lists.head(writable);
        // A list is as long as the waiters of one handle and direction, bounded by `bound`.
        for _ in 0..self.bound {
            let Some(node) = self.get(at_node) else {
                break;
            };
            if node.ticket == ticket {
                if previous == END {
                    *lists.head(writable) = node.next;
                } else if let Some(before) = self
                    .nodes
                    .get_mut(usize::try_from(previous).unwrap_or(usize::MAX))
                {
                    before.next = node.next;
                }
                self.free.push(at_node);
                break;
            }
            previous = at_node;
            at_node = node.next;
        }
        // An existing handle: storing its lists never inserts.
        self.store(raw, lists).unwrap_or(Readiness::NONE)
    }

    /// Hands every waiter of `raw` in the directions `fired` to `wake` (its word and ticket); the directions
    /// still wanted, which the loop arms again.
    pub(crate) fn fire(
        &mut self,
        raw: i32,
        fired: Readiness,
        mut wake: impl FnMut(u64, Ticket),
    ) -> Readiness {
        let Some(at) = self.find(raw) else {
            return Readiness::NONE;
        };
        let mut lists = self
            .handles
            .get(at)
            .map_or(Lists::EMPTY, |entry| entry.lists);
        for writable in [false, true] {
            if !fired.contains(Readiness::of(writable)) {
                continue;
            }
            let mut at_node = std::mem::replace(lists.head(writable), END);
            for _ in 0..self.bound {
                let Some(node) = self.get(at_node) else {
                    break;
                };
                wake(node.word, node.ticket);
                self.free.push(at_node);
                at_node = node.next;
            }
        }
        self.store(raw, lists).unwrap_or(Readiness::NONE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(slot: u32) -> Ticket {
        Ticket::for_test(slot)
    }

    #[test]
    fn a_reader_and_a_writer_of_one_handle_each_get_their_wake() {
        let mut table = Interests::new(8).unwrap();
        assert_eq!(table.add(5, false, 100, t(0)).unwrap(), Readiness::READ);
        assert_eq!(
            table.add(5, true, 200, t(1)).unwrap(),
            Readiness::READ.union(Readiness::WRITE)
        );
        let mut woken = Vec::new();
        let left = table.fire(5, Readiness::WRITE, |word, _| woken.push(word));
        assert_eq!(woken, vec![200]);
        assert_eq!(left, Readiness::READ, "the reader still waits, armed again");
        let left = table.fire(5, Readiness::READ, |word, _| woken.push(word));
        assert_eq!(woken, vec![200, 100]);
        assert!(left.is_empty());
        assert_eq!(table.len(), 0);
    }

    /// Finding 1 (mantle's final review): one task's two waits on one handle and direction are two
    /// nodes; withdrawing one leaves the other to fire.
    #[test]
    fn one_tasks_two_waits_on_a_handle_are_two_nodes() {
        let mut table = Interests::new(8).unwrap();
        table.add(3, false, 1, t(0)).unwrap();
        table.add(3, false, 1, t(1)).unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(
            table.remove(3, false, t(1)),
            Readiness::READ,
            "the other wait still wants the handle"
        );
        let mut woken = Vec::new();
        table.fire(3, Readiness::READ, |word, ticket| {
            woken.push((word, ticket))
        });
        assert_eq!(woken, vec![(1, t(0))], "the surviving wait fires");
    }

    #[test]
    fn a_dropped_wait_leaves_and_the_bound_holds() {
        let mut table = Interests::new(2).unwrap();
        table.add(1, false, 10, t(0)).unwrap();
        table.add(2, true, 20, t(1)).unwrap();
        assert!(matches!(
            table.add(3, false, 30, t(2)),
            Err(RtError::Capacity { .. })
        ));
        assert_eq!(table.remove(1, false, t(0)), Readiness::NONE);
        assert_eq!(
            table.add(3, false, 30, t(2)).unwrap(),
            Readiness::READ,
            "the freed node is reused"
        );
        let mut woken = Vec::new();
        table.fire(1, Readiness::READ, |word, _| woken.push(word));
        assert!(woken.is_empty(), "the dropped wait is not woken");
    }

    /// The handle table against a model: handles added and removed in a churning order (deletion by
    /// backward shift must keep every remaining handle findable), with no growth past the bound.
    #[test]
    fn the_handle_table_agrees_with_a_model_under_churn() {
        const BOUND: usize = 64;
        let mut table = Interests::new(BOUND).unwrap();
        let mut model = std::collections::BTreeMap::new();
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        for round in 0..20_000u32 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Descriptors from a small range collide in the table and recur, as reused fds do.
            let raw = i32::try_from(state % 97).unwrap();
            let ticket = t(round);
            if model.len() < BOUND && !model.contains_key(&raw) && state & 1 == 0 {
                table.add(raw, false, u64::from(round), ticket).unwrap();
                model.insert(raw, ticket);
            } else if let Some(held) = model.remove(&raw) {
                assert_eq!(table.remove(raw, false, held), Readiness::NONE);
            }
            for &raw in model.keys() {
                assert!(
                    table.find(raw).is_some(),
                    "handle {raw} lost at round {round}"
                );
            }
            assert_eq!(table.used, model.len());
        }
    }
}
