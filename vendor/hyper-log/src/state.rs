//! What the log holds in memory (mantle docs/design/raft-log.md §4): each group's retained
//! entries, hard state, start and proposals, where each was written, and how many live pieces
//! each segment still holds. The log's owner holds all of it; nothing else reads it.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::format::{HardState, Start};

/// Where a piece of the log was written: its segment's slot in the file and the file offset
/// of its encoded start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Place {
    pub(crate) slot: u32,
    pub(crate) offset: u64,
}

/// A retained entry: its term, where it is, its payload's length and, while it is recent,
/// its bytes, which the update that wrote it handed over.
#[derive(Debug, Clone)]
pub(crate) struct Slot {
    pub(crate) term: u64,
    pub(crate) place: Place,
    pub(crate) len: u32,
    pub(crate) cached: Option<Vec<u8>>,
}

/// An entry the group approved by itself on the fast track.
#[derive(Debug, Clone)]
pub(crate) struct Proposal {
    pub(crate) term: u64,
    pub(crate) place: Place,
    pub(crate) bytes: Vec<u8>,
}

/// One group's durable state.
#[derive(Debug, Clone, Default)]
pub(crate) struct Group {
    pub(crate) start: Start,
    /// Where the start was written; `None` for a group that never recorded one.
    pub(crate) start_at: Option<Place>,
    /// Entries from `start.index + 1` on.
    pub(crate) entries: VecDeque<Slot>,
    pub(crate) hard: Option<(HardState, Place)>,
    pub(crate) proposals: BTreeMap<u64, Proposal>,
    /// Payload bytes of the retained entries.
    pub(crate) bytes: u64,
    /// Payload bytes of the cached entries, which are the retained entries from
    /// `cache_from` on.
    pub(crate) cached: u64,
    pub(crate) cache_from: u64,
    /// Entries through this mark's index, of terms up to its term, that the group's log may
    /// lack, and where the mark was written (docs/design/raft-log.md §6).
    pub(crate) uncertain: Option<(Start, Place)>,
    /// The greatest index an update released the group's proposals through, and where its
    /// record was written.
    pub(crate) released: Option<(u64, Place)>,
}

/// Whether a log whose last entry is `last`, of term `term`, again holds what an uncertainty
/// mark says it may lack. Past the mark's index it holds the entries there, received again.
/// With an entry of a later term, from a leader, it holds every committed entry the mark
/// could cover: terms never fall along a log, so a leader whose log has an entry of a later
/// term at some index has none of the marked terms after it, and the log matches that
/// leader's up to that index (docs/design/raft-log.md §6).
pub(crate) fn resolves(mark: Start, last: u64, term: u64) -> bool {
    last >= mark.index || term > mark.term
}

impl Group {
    /// The index of the last entry held, or the start's when none is.
    pub(crate) fn last(&self) -> Option<u64> {
        let held = u64::try_from(self.entries.len()).ok()?;
        self.start.index.checked_add(held)
    }

    /// The first entry's index.
    pub(crate) fn first(&self) -> Option<u64> {
        self.start.index.checked_add(1)
    }

    pub(crate) fn slot(&self, index: u64) -> Option<&Slot> {
        let i = index.checked_sub(self.first()?)?;
        self.entries.get(usize::try_from(i).ok()?)
    }

    /// The term of `index`, answered for the start's index too.
    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        if index == self.start.index {
            return Some(self.start.term);
        }
        self.slot(index).map(|s| s.term)
    }

    /// The term of the last entry held, or the start's.
    pub(crate) fn last_term(&self) -> u64 {
        self.entries.back().map_or(self.start.term, |s| s.term)
    }

    /// Every live piece of the group: where it is and the bytes it takes there.
    pub(crate) fn pieces(&self) -> impl Iterator<Item = (Place, u64)> + '_ {
        self.start_at
            .map(|p| (p, START_BYTES))
            .into_iter()
            .chain(self.hard.iter().map(|(_, p)| (*p, HARD_STATE_BYTES)))
            .chain(self.uncertain.iter().map(|(_, p)| (*p, UNCERTAIN_BYTES)))
            .chain(self.released.iter().map(|(_, p)| (*p, RELEASED_BYTES)))
            .chain(self.entries.iter().map(|s| (s.place, entry_bytes(s.len))))
            .chain(self.proposals.values().map(|p| {
                let len = u32::try_from(p.bytes.len()).unwrap_or(u32::MAX);
                (p.place, entry_bytes(len).saturating_add(PROPOSAL_EXTRA))
            }))
    }
}

/// Bytes a `Start` record takes in a payload: its kind and group (17), its index and term
/// (format.rs).
pub(crate) const START_BYTES: u64 = 33;
/// Bytes an `Uncertain` record takes: its kind and group (17), the mark's index and term.
pub(crate) const UNCERTAIN_BYTES: u64 = 33;
/// Bytes a `Damaged` record takes: its kind and group.
pub(crate) const DAMAGED_BYTES: u64 = 17;
/// Bytes a `Released` record takes: its kind and group (17), its index.
pub(crate) const RELEASED_BYTES: u64 = 25;
/// Bytes a `HardState` record takes: its kind and group (17), its term, vote and commit.
pub(crate) const HARD_STATE_BYTES: u64 = 41;
/// A proposal's bytes beyond an entry's: its record's kind and group, and its index.
pub(crate) const PROPOSAL_EXTRA: u64 = 25;

/// Bytes an entry of `len` payload bytes takes in a payload.
pub(crate) fn entry_bytes(len: u32) -> u64 {
    u64::from(len).saturating_add(crate::format::ENTRY_HEADER_BYTES)
}

/// The live pieces each segment slot holds and their bytes, and the bytes its frames take,
/// padded, since it opened: what freeing the slot gives back (docs/design/raft-log.md §5).
#[derive(Debug, Clone, Default)]
pub(crate) struct Live {
    counts: Vec<(u64, u64)>,
    used: Vec<u64>,
}

impl Live {
    pub(crate) fn with_slots(slots: usize) -> Self {
        Self {
            counts: vec![(0, 0); slots],
            used: vec![0; slots],
        }
    }

    pub(crate) fn grow(&mut self, slots: usize) {
        if slots > self.counts.len() {
            self.counts.resize(slots, (0, 0));
        }
        if slots > self.used.len() {
            self.used.resize(slots, 0);
        }
    }

    pub(crate) fn add(&mut self, place: Place, bytes: u64) {
        if let Some((count, total)) = self.counts.get_mut(slot_index(place)) {
            *count = count.saturating_add(1);
            *total = total.saturating_add(bytes);
        }
    }

    pub(crate) fn kill(&mut self, place: Place, bytes: u64) {
        if let Some((count, total)) = self.counts.get_mut(slot_index(place)) {
            *count = count.saturating_sub(1);
            *total = total.saturating_sub(bytes);
        }
    }

    /// The live pieces in `slot` and their bytes.
    pub(crate) fn of(&self, slot: u32) -> (u64, u64) {
        usize::try_from(slot)
            .ok()
            .and_then(|s| self.counts.get(s))
            .copied()
            .unwrap_or((0, 0))
    }

    /// The bytes `slot`'s frames take, padded, since it opened.
    pub(crate) fn used(&self, slot: u32) -> u64 {
        usize::try_from(slot)
            .ok()
            .and_then(|s| self.used.get(s))
            .copied()
            .unwrap_or(0)
    }

    /// A frame of `bytes`, padded, written in `slot`.
    pub(crate) fn wrote(&mut self, slot: u32, bytes: u64) {
        if let Some(used) = usize::try_from(slot)
            .ok()
            .and_then(|s| self.used.get_mut(s))
        {
            *used = used.saturating_add(bytes);
        }
    }

    /// A slot opened afresh: no frame is in it yet.
    pub(crate) fn open(&mut self, slot: u32) {
        if let Some(used) = usize::try_from(slot)
            .ok()
            .and_then(|s| self.used.get_mut(s))
        {
            *used = 0;
        }
    }
}

fn slot_index(place: Place) -> usize {
    usize::try_from(place.slot).unwrap_or(usize::MAX)
}

/// Where the next frame goes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Head {
    pub(crate) slot: u32,
    pub(crate) incarnation: u64,
    pub(crate) nonce: u64,
    /// File offset of the next frame.
    pub(crate) offset: u64,
}

/// Which segment slots are live, oldest first, and which are free.
#[derive(Debug, Default)]
pub(crate) struct Segments {
    /// Each slot's incarnation, 0 for one never used, and its nonce.
    pub(crate) incarnation: Vec<u64>,
    pub(crate) nonce: Vec<u64>,
    /// Live segments' slots, the tail first and the head last.
    pub(crate) live: VecDeque<u32>,
    /// Free slots, each with the sequence of the first frame that recorded a tail past it:
    /// reused only once that frame is durable (mantle docs/design/raft-log.md §5).
    pub(crate) free: VecDeque<(u32, u64)>,
}

/// Everything the log knows of its file and its groups.
pub(crate) struct State {
    pub(crate) groups: HashMap<u128, Group>,
    /// Groups found damaged, served to no one until removed (`Recovery::damaged`), each with
    /// where its `Damaged` record is: `None` only while open writes it.
    pub(crate) damaged: HashMap<u128, Option<Place>>,
    pub(crate) live: Live,
    pub(crate) segments: Segments,
    pub(crate) head: Head,
    pub(crate) next_sequence: u64,
    pub(crate) next_incarnation: u64,
    /// The sequence of the last frame flushed.
    pub(crate) durable: u64,
    /// The tail the last frame flushed names.
    pub(crate) durable_tail: u64,
}

impl State {
    /// Whether the segment of `incarnation` is live: in a slot the log still reads.
    pub(crate) fn is_live(&self, incarnation: u64) -> bool {
        self.segments.live.iter().any(|&slot| {
            usize::try_from(slot)
                .ok()
                .and_then(|i| self.segments.incarnation.get(i))
                .is_some_and(|&inc| inc == incarnation)
        })
    }

    pub(crate) fn tail_incarnation(&self) -> u64 {
        self.segments
            .live
            .front()
            .and_then(|&slot| self.segments.incarnation.get(usize::try_from(slot).ok()?))
            .copied()
            .unwrap_or(self.head.incarnation)
    }
}

/// A group as replay rebuilds it: entries may arrive out of order, since relocated copies
/// of old entries follow the newer ones they sit beside (docs/design/raft-log.md §5).
#[derive(Debug, Default)]
pub(crate) struct Replayed {
    pub(crate) start: Start,
    pub(crate) start_at: Option<Place>,
    pub(crate) entries: BTreeMap<u64, Slot>,
    pub(crate) hard: Option<(HardState, Place)>,
    pub(crate) proposals: BTreeMap<u64, Proposal>,
    /// The group's last index as the records replayed so far set it. Relocated copies do
    /// not set it, so it can lag the truth before the first `Entries` replayed, but never
    /// past a proposal written after it.
    pub(crate) last: u64,
    pub(crate) uncertain: Option<(Start, Place)>,
    /// Where a `Damaged` record fenced the group, which then holds nothing else.
    pub(crate) damaged: Option<Place>,
    /// The greatest index a `Released` record released the group's proposals through, and
    /// where it is.
    pub(crate) released: Option<(u64, Place)>,
}

impl Replayed {
    /// Entries at `first` and after are replaced by what follows.
    pub(crate) fn truncate(&mut self, first: u64) {
        self.entries.split_off(&first);
    }

    /// The group's log reached `last`: an uncertainty mark ends when the log holds what it
    /// covers (`resolves`). Proposals outlive it, as they do when the update that reached it
    /// was applied: only a release ends them (`release`).
    pub(crate) fn reach(&mut self, last: u64) {
        self.last = last;
        let term = if last == self.start.index {
            Some(self.start.term)
        } else {
            self.entries.get(&last).map(|s| s.term)
        };
        if let (Some((mark, _)), Some(term)) = (self.uncertain, term)
            && resolves(mark, last, term)
        {
            self.uncertain = None;
        }
    }

    /// A `Released` record at `place`: the proposals through `through` end, as they did when
    /// its update was applied, and it is the group's release unless one before released more.
    pub(crate) fn release(&mut self, through: u64, place: Place) {
        self.proposals = match through.checked_add(1) {
            Some(after) => self.proposals.split_off(&after),
            None => BTreeMap::new(),
        };
        if self.released.is_none_or(|(held, _)| held < through) {
            self.released = Some((through, place));
        }
    }

    /// The group as it runs, or `None` if its entries do not run unbroken from its start to
    /// its last: a record the log acknowledged is missing.
    pub(crate) fn finish(mut self) -> Option<Group> {
        let first = self.start.index.checked_add(1)?;
        let held = self.entries.split_off(&first);
        let mut entries = VecDeque::with_capacity(held.len());
        let mut bytes = 0u64;
        for (expected, (index, slot)) in (first..).zip(held) {
            if index != expected {
                return None;
            }
            bytes = bytes.checked_add(u64::from(slot.len))?;
            entries.push_back(slot);
        }
        let last = self
            .start
            .index
            .checked_add(u64::try_from(entries.len()).ok()?)?;
        Some(Group {
            start: self.start,
            start_at: self.start_at,
            entries,
            hard: self.hard,
            proposals: self.proposals,
            released: self.released,
            bytes,
            cached: 0,
            cache_from: last.checked_add(1)?,
            uncertain: self.uncertain,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(term: u64, slot: u32) -> Slot {
        Slot {
            term,
            place: Place {
                slot,
                offset: u64::from(slot) * 100,
            },
            len: 1,
            cached: None,
        }
    }

    #[test]
    fn replay_accepts_relocated_entries_behind_newer_ones_and_refuses_gaps() {
        let mut r = Replayed::default();
        r.entries.insert(6, slot(2, 3));
        r.entries.insert(7, slot(2, 3));
        // Relocated copies of 1..=5 arrive after 6 and 7.
        for i in 1..=5 {
            r.entries.insert(i, slot(1, 4));
        }
        r.start = Start { index: 2, term: 1 };
        let g = r.finish().unwrap();
        assert_eq!((g.first(), g.last()), (Some(3), Some(7)));
        assert_eq!(g.term(2), Some(1));
        assert_eq!(g.term(6), Some(2));
        assert_eq!(g.term(8), None);
        let mut gap = Replayed::default();
        gap.entries.insert(1, slot(1, 0));
        gap.entries.insert(3, slot(1, 0));
        assert!(gap.finish().is_none());
    }

    #[test]
    fn live_counts_follow_places() {
        let mut live = Live::with_slots(2);
        let p = Place { slot: 1, offset: 0 };
        live.add(p, 10);
        live.add(p, 5);
        live.kill(p, 10);
        assert_eq!(live.of(1), (1, 5));
        live.grow(4);
        live.add(Place { slot: 3, offset: 0 }, 7);
        assert_eq!(live.of(3), (1, 7));
        assert_eq!(live.of(9), (0, 0));
        live.wrote(1, 4096);
        live.wrote(1, 8192);
        assert_eq!(live.used(1), 12288);
        live.open(1);
        assert_eq!(live.used(1), 0);
        assert_eq!(live.used(9), 0);
    }
}
