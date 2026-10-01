//! The fast track (Fast Raft: Castiglia, Goldberg and Patterson, ICDCS
//! 2020; 27 §4).
//!
//! A proposer sends its entry for an index to every voter and not to the
//! leader. A voter that holds nothing at the index holds the entry there,
//! **approved by itself**, and once that is durable tells the leader what it
//! holds: its vote. The leader decides each index in order. With the votes
//! of a classic quorum in, it takes the entry most voted for into its log;
//! with a fast quorum holding that entry, the index is committed without
//! another round.
//!
//! What a member approved by itself is kept here, beside the log and never
//! in it: the log holds what a leader approved and nothing else, so the
//! log is the classic one, elections compare it as they always did, and a
//! leader's entry takes the place of what a member held by taking its
//! index. Because elections see only the log, what a member holds beside
//! it counts for a fast commit only once its log holds an entry of the
//! committing leader's term ([`crate::track`]): an election weighs the log,
//! and the log must then say the member took that leader's word.
//!
//! A leader stamps what it takes with its own term. The term a proposer
//! gave says nothing of the entry, for two proposers of one term propose
//! different entries for one index, and an index and a term name one entry
//! of the log only while one member alone writes each term.
use crate::{
    NodeId,
    error::{Error, Result},
    log::copy_entry,
    proto::Entry,
};

/// A proposal, from a proposer to every voter. It bears no term: what is
/// held may come from anyone.
pub const FAST_PROPOSE: i32 = 100;
/// What a voter holds at an index, to the leader.
pub const FAST_VOTE: i32 = 101;

/// Whether two entries state the same. Their index is where they are held,
/// and their term who stamped them; neither is what they state.
pub fn same(left: &Entry, right: &Entry) -> bool {
    left.entry_type == right.entry_type && left.data == right.data && left.context == right.context
}
fn bytes(entry: &Entry) -> usize {
    entry
        .data
        .capacity()
        .saturating_add(entry.context.capacity())
        .saturating_add(std::mem::size_of::<Entry>())
}

#[derive(Clone, Debug)]
struct Held {
    entry: Entry,
    /// Storage holds it: it may be voted with.
    durable: bool,
    /// Proposed here: its proposer is told what became of it.
    own: bool,
}

/// What this member approved by itself: at most one entry an index, above
/// its log, and no more than the bounds admit. What is refused for room is
/// not held and not voted for, and its proposer proposes it again.
#[derive(Clone, Debug)]
pub struct Proposals {
    /// In order of index.
    held: Vec<Held>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}
impl Proposals {
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            held: Vec::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }
    fn find(&self, index: u64) -> std::result::Result<usize, usize> {
        self.held
            .binary_search_by_key(&index, |held| held.entry.index)
    }
    pub fn len(&self) -> usize {
        self.held.len()
    }
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
    pub fn get(&self, index: u64) -> Option<&Entry> {
        let position = self.find(index).ok()?;
        self.held.get(position).map(|held| &held.entry)
    }
    /// The highest index held.
    pub fn last_index(&self) -> u64 {
        self.held.last().map_or(0, |held| held.entry.index)
    }
    /// Holds `entry` at its index if nothing is held there. False when
    /// something is.
    pub fn hold(&mut self, entry: Entry, durable: bool, own: bool) -> Result<bool> {
        let position = match self.find(entry.index) {
            Ok(_) => return Ok(false),
            Err(position) => position,
        };
        let size = bytes(&entry);
        if self.held.len() >= self.max_entries || self.bytes.saturating_add(size) > self.max_bytes {
            return Err(Error::Capacity("entries approved by this member"));
        }
        self.held
            .try_reserve(1)
            .map_err(|_| Error::Capacity("entries approved by this member"))?;
        self.bytes = self.bytes.saturating_add(size);
        self.held.insert(
            position,
            Held {
                entry,
                durable,
                own,
            },
        );
        Ok(true)
    }
    /// What storage does not hold yet.
    pub fn unstable(&self) -> impl Iterator<Item = &Entry> + Clone {
        self.held
            .iter()
            .filter(|held| !held.durable)
            .map(|held| &held.entry)
    }
    pub fn has_unstable(&self) -> bool {
        self.held.iter().any(|held| !held.durable)
    }
    /// Storage holds what is held at `index`, if it is `entry`. True when
    /// it became durable by this.
    pub fn persisted(&mut self, entry: &Entry) -> bool {
        let Ok(position) = self.find(entry.index) else {
            return false;
        };
        match self.held.get_mut(position) {
            Some(held) if !held.durable && same(&held.entry, entry) => {
                held.durable = true;
                true
            }
            _ => false,
        }
    }
    /// What is durable, in order of index.
    pub fn durable(&self) -> impl Iterator<Item = &Entry> {
        self.held
            .iter()
            .filter(|held| held.durable)
            .map(|held| &held.entry)
    }
    pub fn iter(&self) -> impl Iterator<Item = &Entry> + Clone {
        self.held.iter().map(|held| &held.entry)
    }
    /// The log reaches `index`: what was held at or below it is held no
    /// more. `taken` says whether the log holds the entry that was held;
    /// what was proposed here and not taken is given to `displaced`.
    pub fn release(
        &mut self,
        index: u64,
        mut taken: impl FnMut(&Entry) -> bool,
        displaced: &mut Vec<Entry>,
    ) -> Result<()> {
        let count = self
            .held
            .iter()
            .take_while(|held| held.entry.index <= index)
            .count();
        let own = self.held.iter().take(count).filter(|held| held.own).count();
        displaced.try_reserve(own).map_err(|_| Error::Memory)?;
        for held in self.held.drain(..count) {
            self.bytes = self.bytes.saturating_sub(bytes(&held.entry));
            if held.own && !taken(&held.entry) {
                displaced.push(held.entry);
            }
        }
        if self.held.is_empty() {
            self.held = Vec::new();
        }
        Ok(())
    }
    pub fn resident_bytes(&self) -> usize {
        self.bytes.saturating_add(
            self.held
                .capacity()
                .saturating_mul(std::mem::size_of::<Held>()),
        )
    }
    /// The bytes of what is held, as counted.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    /// Whether the counter says what a walk of what is held says.
    pub(crate) fn check(&self) -> Result<()> {
        let held = self.held.iter().fold(0usize, |total, held| {
            total.saturating_add(bytes(&held.entry))
        });
        if held != self.bytes {
            return Err(Error::Invariant(
                "what is approved by this member is not what its counter says",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Choice {
    entry: Entry,
    /// Who holds it, in order.
    voters: Vec<NodeId>,
}
#[derive(Clone, Debug)]
struct Slot {
    index: u64,
    /// In the order first voted for.
    choices: Vec<Choice>,
}
impl Slot {
    fn voted(&self, member: NodeId) -> bool {
        self.choices
            .iter()
            .any(|choice| choice.voters.binary_search(&member).is_ok())
    }
}

/// What a leader, or one that asks to lead, was told the voters hold at the
/// indexes above its log. A voter holds one entry an index, so it has one
/// vote an index.
#[derive(Clone, Debug)]
pub struct Votes {
    /// In order of index.
    slots: Vec<Slot>,
    bytes: usize,
    max_slots: usize,
    max_bytes: usize,
}
impl Votes {
    pub fn new(max_slots: usize, max_bytes: usize) -> Self {
        Self {
            slots: Vec::new(),
            bytes: 0,
            max_slots,
            max_bytes,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
    pub fn clear(&mut self) {
        self.slots = Vec::new();
        self.bytes = 0;
    }
    fn find(&self, index: u64) -> std::result::Result<usize, usize> {
        self.slots.binary_search_by_key(&index, |slot| slot.index)
    }
    /// The highest index voted at.
    pub fn last_index(&self) -> u64 {
        self.slots.last().map_or(0, |slot| slot.index)
    }
    /// `member` holds `entry` at its index. A vote there is no room for is
    /// an error, and whether it may be dropped is the caller's to say: a
    /// leader may, for it then decides by fewer votes and commits by the
    /// classic track; one that asks to lead may not.
    pub fn vote(&mut self, member: NodeId, entry: &Entry) -> Result<()> {
        let position = match self.find(entry.index) {
            Ok(position) => position,
            Err(position) => {
                if self.slots.len() >= self.max_slots {
                    return Err(Error::Capacity("indexes voted at"));
                }
                self.slots
                    .try_reserve(1)
                    .map_err(|_| Error::Capacity("indexes voted at"))?;
                self.slots.insert(
                    position,
                    Slot {
                        index: entry.index,
                        choices: Vec::new(),
                    },
                );
                position
            }
        };
        let (max_bytes, held) = (self.max_bytes, self.bytes);
        let Some(slot) = self.slots.get_mut(position) else {
            return Err(Error::Invariant("a vote lost its index"));
        };
        if slot.voted(member) {
            return Ok(());
        }
        let known = slot
            .choices
            .iter()
            .position(|choice| same(&choice.entry, entry));
        let choice = match known {
            Some(known) => slot.choices.get_mut(known),
            None => {
                let size = bytes(entry);
                if held.saturating_add(size) > max_bytes || slot.choices.len() >= crate::MAX_MEMBERS
                {
                    return Err(Error::Capacity("entries voted for"));
                }
                slot.choices
                    .try_reserve(1)
                    .map_err(|_| Error::Capacity("entries voted for"))?;
                let entry = copy_entry(entry).map_err(|_| Error::Capacity("entries voted for"))?;
                self.bytes = held.saturating_add(size);
                slot.choices.push(Choice {
                    entry,
                    voters: Vec::new(),
                });
                slot.choices.last_mut()
            }
        };
        let Some(choice) = choice else {
            return Err(Error::Invariant("a vote lost its entry"));
        };
        if let Err(at) = choice.voters.binary_search(&member) {
            choice
                .voters
                .try_reserve(1)
                .map_err(|_| Error::Capacity("voters of an entry"))?;
            choice.voters.insert(at, member);
        }
        Ok(())
    }
    /// Who voted at `index`, for whatever entry, in order.
    pub fn voters(&self, index: u64, into: &mut Vec<NodeId>) -> Result<()> {
        into.clear();
        let Ok(position) = self.find(index) else {
            return Ok(());
        };
        let Some(slot) = self.slots.get(position) else {
            return Ok(());
        };
        for choice in &slot.choices {
            into.try_reserve(choice.voters.len())
                .map_err(|_| Error::Memory)?;
            into.extend_from_slice(&choice.voters);
        }
        into.sort_unstable();
        Ok(())
    }
    /// The entry most voted for at `index` and who holds it. Of two voted
    /// for alike, the one first voted for.
    pub fn most(&self, index: u64) -> Option<(&Entry, &[NodeId])> {
        let position = self.find(index).ok()?;
        let mut most: Option<&Choice> = None;
        for choice in &self.slots.get(position)?.choices {
            if most.is_none_or(|most| choice.voters.len() > most.voters.len()) {
                most = Some(choice);
            }
        }
        most.map(|choice| (&choice.entry, choice.voters.as_slice()))
    }
    /// The log reaches `index`: what was voted at or below it is decided.
    pub fn release(&mut self, index: u64) {
        let count = self
            .slots
            .iter()
            .take_while(|slot| slot.index <= index)
            .count();
        for slot in self.slots.drain(..count) {
            for choice in slot.choices {
                self.bytes = self.bytes.saturating_sub(bytes(&choice.entry));
            }
        }
        if self.slots.is_empty() {
            self.slots = Vec::new();
        }
    }
    pub fn resident_bytes(&self) -> usize {
        let slots = self
            .slots
            .capacity()
            .saturating_mul(std::mem::size_of::<Slot>());
        self.slots
            .iter()
            .fold(self.bytes.saturating_add(slots), |bytes, slot| {
                slot.choices.iter().fold(bytes, |bytes, choice| {
                    bytes.saturating_add(
                        choice
                            .voters
                            .capacity()
                            .saturating_mul(std::mem::size_of::<NodeId>()),
                    )
                })
            })
    }
    /// Whether the counter says what a walk of the votes says.
    pub(crate) fn check(&self) -> Result<()> {
        let voted = self.slots.iter().fold(0usize, |total, slot| {
            slot.choices.iter().fold(total, |total, choice| {
                total.saturating_add(bytes(&choice.entry))
            })
        });
        if voted != self.bytes {
            return Err(Error::Invariant(
                "what the voters hold is not what its counter says",
            ));
        }
        Ok(())
    }
}

/// For each index the leader decided and has not committed, who holds the
/// entry it took, in order of index.
#[derive(Clone, Debug, Default)]
pub struct Decided {
    indexes: Vec<(u64, Vec<NodeId>)>,
}
impl Decided {
    pub fn clear(&mut self) {
        self.indexes = Vec::new();
    }
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
    fn find(&self, index: u64) -> std::result::Result<usize, usize> {
        self.indexes
            .binary_search_by_key(&index, |(index, _)| *index)
    }
    pub fn decide(&mut self, index: u64, holders: &[NodeId], limit: usize) -> Result<()> {
        let Err(position) = self.find(index) else {
            return Ok(());
        };
        if self.indexes.len() >= limit {
            return Err(Error::Capacity("indexes decided and not committed"));
        }
        let mut held = Vec::new();
        held.try_reserve_exact(holders.len())
            .map_err(|_| Error::Memory)?;
        held.extend_from_slice(holders);
        self.indexes.try_reserve(1).map_err(|_| Error::Memory)?;
        self.indexes.insert(position, (index, held));
        Ok(())
    }
    pub fn knows(&self, index: u64) -> bool {
        self.find(index).is_ok()
    }
    /// `member` holds what was taken at `index`.
    pub fn holds(&mut self, index: u64, member: NodeId) -> Result<()> {
        let Ok(position) = self.find(index) else {
            return Ok(());
        };
        let Some((_, holders)) = self.indexes.get_mut(position) else {
            return Ok(());
        };
        if let Err(at) = holders.binary_search(&member) {
            if holders.len() >= crate::MAX_MEMBERS {
                return Err(Error::Capacity("holders of an entry"));
            }
            holders.try_reserve(1).map_err(|_| Error::Memory)?;
            holders.insert(at, member);
        }
        Ok(())
    }
    pub fn holders(&self, index: u64) -> &[NodeId] {
        self.find(index)
            .ok()
            .and_then(|position| self.indexes.get(position))
            .map_or(&[], |(_, holders)| holders.as_slice())
    }
    /// What is committed through `index` is decided no more.
    pub fn release(&mut self, index: u64) {
        let count = self
            .indexes
            .iter()
            .take_while(|(decided, _)| *decided <= index)
            .count();
        self.indexes.drain(..count);
        if self.indexes.is_empty() {
            self.indexes = Vec::new();
        }
    }
    /// What the log no longer holds from `index` on was never decided.
    pub fn truncate(&mut self, index: u64) {
        self.indexes.retain(|(decided, _)| *decided < index);
        if self.indexes.is_empty() {
            self.indexes = Vec::new();
        }
    }
    pub fn resident_bytes(&self) -> usize {
        self.indexes.iter().fold(
            self.indexes
                .capacity()
                .saturating_mul(std::mem::size_of::<(u64, Vec<NodeId>)>()),
            |bytes, (_, holders)| {
                bytes.saturating_add(
                    holders
                        .capacity()
                        .saturating_mul(std::mem::size_of::<NodeId>()),
                )
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, data: &[u8]) -> Entry {
        Entry {
            index,
            term: 1,
            data: data.to_vec(),
            ..Entry::default()
        }
    }
    #[test]
    fn a_member_holds_one_entry_an_index_and_no_more_than_it_may() {
        let mut held = Proposals::new(3, 4096);
        assert!(held.hold(entry(7, b"a"), false, true).unwrap());
        assert!(!held.hold(entry(7, b"b"), false, false).unwrap());
        assert_eq!(held.get(7).unwrap().data, b"a");
        assert!(held.hold(entry(5, b"c"), true, false).unwrap());
        assert!(held.hold(entry(9, b"d"), false, true).unwrap());
        assert_eq!(
            held.hold(entry(8, b"e"), false, false),
            Err(Error::Capacity("entries approved by this member"))
        );
        assert_eq!(held.last_index(), 9);
        assert_eq!(
            held.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![5, 7, 9]
        );
        assert_eq!(
            held.unstable().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![7, 9]
        );
        // Storage holds what was given, and only that.
        assert!(!held.persisted(&entry(7, b"b")));
        assert!(held.persisted(&entry(7, b"a")));
        assert!(!held.persisted(&entry(7, b"a")));
        assert!(!held.persisted(&entry(6, b"a")));
        assert_eq!(
            held.durable().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![5, 7]
        );
        let bytes = held.resident_bytes();
        // The log took "a" at 7 and another entry at 5.
        let mut displaced = Vec::new();
        held.release(7, |entry| entry.data == b"a", &mut displaced)
            .unwrap();
        assert!(displaced.is_empty());
        assert_eq!(held.len(), 1);
        assert!(held.resident_bytes() < bytes);
        held.release(9, |_| false, &mut displaced).unwrap();
        assert_eq!(displaced.len(), 1);
        assert_eq!(displaced[0].data, b"d");
        assert!(held.is_empty() && !held.has_unstable());
        // By its bytes as well.
        let mut held = Proposals::new(8, 2 * bytes_of(64));
        assert!(held.hold(entry(1, &[0; 64]), false, false).unwrap());
        assert!(held.hold(entry(2, &[0; 64]), false, false).unwrap());
        assert!(held.hold(entry(3, &[0; 64]), false, false).is_err());
    }
    fn bytes_of(data: usize) -> usize {
        bytes(&entry(1, &vec![0; data]))
    }
    #[test]
    fn a_voter_has_one_vote_an_index_and_the_most_voted_is_taken() {
        let mut votes = Votes::new(4, 1 << 20);
        votes.vote(1, &entry(5, b"e")).unwrap();
        votes.vote(2, &entry(5, b"f")).unwrap();
        votes.vote(3, &entry(5, b"f")).unwrap();
        // What it holds it holds: a second vote says nothing.
        votes.vote(3, &entry(5, b"e")).unwrap();
        votes.vote(2, &entry(6, b"g")).unwrap();
        let mut voters = Vec::new();
        votes.voters(5, &mut voters).unwrap();
        assert_eq!(voters, vec![1, 2, 3]);
        let (most, holders) = votes.most(5).unwrap();
        assert_eq!(
            (most.data.as_slice(), holders),
            (b"f".as_slice(), [2, 3].as_slice())
        );
        // Voted for alike, the first.
        votes.vote(4, &entry(5, b"e")).unwrap();
        let (most, holders) = votes.most(5).unwrap();
        assert_eq!(
            (most.data.as_slice(), holders),
            (b"e".as_slice(), [1, 4].as_slice())
        );
        votes.voters(9, &mut voters).unwrap();
        assert!(voters.is_empty() && votes.most(9).is_none());
        assert_eq!(votes.last_index(), 6);
        // The term a proposer gave is no part of what an entry states.
        let mut later = entry(6, b"g");
        later.term = 9;
        votes.vote(5, &later).unwrap();
        assert_eq!(votes.most(6).unwrap().1, [2, 5]);
        let bytes = votes.resident_bytes();
        votes.release(5);
        assert!(votes.most(5).is_none() && votes.resident_bytes() < bytes);
        for index in 7..=9 {
            votes.vote(1, &entry(index, b"h")).unwrap();
        }
        assert_eq!(
            votes.vote(1, &entry(10, b"h")),
            Err(Error::Capacity("indexes voted at"))
        );
        let mut votes = Votes::new(4, bytes_of(8));
        votes.vote(1, &entry(1, &[1; 8])).unwrap();
        votes.vote(2, &entry(1, &[1; 8])).unwrap();
        assert_eq!(
            votes.vote(3, &entry(1, &[2; 8])),
            Err(Error::Capacity("entries voted for"))
        );
        votes.clear();
        assert!(votes.is_empty());
    }
    #[test]
    fn who_holds_what_was_taken_is_known_until_it_is_committed() {
        let mut decided = Decided::default();
        decided.decide(5, &[1, 3], 2).unwrap();
        decided.decide(6, &[2], 2).unwrap();
        assert!(decided.decide(7, &[2], 2).is_err());
        decided.decide(5, &[9], 2).unwrap();
        assert_eq!(decided.holders(5), [1, 3]);
        decided.holds(5, 2).unwrap();
        decided.holds(5, 2).unwrap();
        decided.holds(8, 2).unwrap();
        assert_eq!(decided.holders(5), [1, 2, 3]);
        assert!(decided.holders(8).is_empty() && !decided.knows(8));
        assert!(decided.resident_bytes() > 0);
        decided.release(5);
        assert!(!decided.knows(5) && decided.knows(6));
        decided.truncate(6);
        assert!(decided.is_empty());
    }
}
