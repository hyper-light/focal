//! Where a group's writes become durable (`docs/durable.md` §9): the [`LogStore`] a replica
//! writes its group through and reads its group's durable log from. hyper-log's group handle is
//! the disk implementation ([`crate::GroupStore`]); a store that publishes each write before it
//! returns is slates' case, of depth one ([`crate::RamStore`]).
//!
//! A store answers its writes in the order they were submitted, and a write's answer comes only
//! once everything the write holds is durable (I7). A refused write changed nothing, and every
//! write submitted after it, before the replica took the refusal, is refused too
//! ([`Fault::Behind`]): a group's writes never apply out of order.
use std::task::Waker;

use hyper_raft::StorageError;
use hyper_raft::proto::{Entry, EntryType, HardState};

/// A place in the log: an index and the term of the entry there.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Point {
    /// The index.
    pub index: u64,
    /// The term of the entry at the index.
    pub term: u64,
}

/// An entry as the store holds it, read where it lies: nothing is copied to look at it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryRef<'a> {
    /// Its place in the log.
    pub index: u64,
    /// The term of the leader that proposed it.
    pub term: u64,
    /// Its kind.
    pub kind: EntryType,
    /// What the proposer attached.
    pub context: &'a [u8],
    /// The payload.
    pub data: &'a [u8],
}

impl<'a> EntryRef<'a> {
    /// The entry `entry`, read where it is.
    pub fn of(entry: &'a Entry) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
            kind: entry.entry_type,
            context: &entry.context,
            data: &entry.data,
        }
    }

    /// Writes this entry into `into`, reusing the room its buffers already have.
    pub fn copy_into(&self, into: &mut Entry) {
        into.entry_type = self.kind;
        into.term = self.term;
        into.index = self.index;
        into.data.clear();
        into.data.extend_from_slice(self.data);
        into.context.clear();
        into.context.extend_from_slice(self.context);
    }

    /// The bytes the entry takes in a message, as the core's byte bounds count them
    /// (`hyper_raft::proto::encoded_bytes`).
    pub fn encoded_bytes(&self) -> u64 {
        let len = hyper_raft::wire::ENTRY_FIXED_BYTES
            .saturating_add(self.data.len())
            .saturating_add(self.context.len());
        u64::try_from(len).unwrap_or(u64::MAX)
    }

    /// Whether the entry changes the configuration, by either encoding.
    pub fn changes_configuration(&self) -> bool {
        self.kind != EntryType::EntryNormal
    }
}

/// Entries to write from `first` on, replacing whatever the group holds at or after `first`.
/// None of them still says where the group's log ends.
#[derive(Clone, Copy, Debug)]
pub struct Entries<'a> {
    /// The index of the first.
    pub first: u64,
    /// The entries, from `first` on, read where the core holds them.
    pub entries: &'a [Entry],
}

/// One write of a group: everything one `Ready` asks to make durable, or a write of the shell's
/// own (the commit alone, a compaction's start), applied in this order (`docs/durable.md` §2.3):
/// the start, the entries, then the hard state and the fast track's proposals. A store copies it
/// once, into whatever it writes; it borrows from the core.
#[derive(Clone, Copy, Debug, Default)]
pub struct Write<'a> {
    /// The log starts after this point from now on: a snapshot installed there, or a compaction.
    pub start: Option<Point>,
    /// The entries.
    pub entries: Option<Entries<'a>>,
    /// The hard state; the latest written is the group's.
    pub hard_state: Option<HardState>,
    /// What the member approved by itself on the fast track, held beside the log until it
    /// reaches their indexes.
    pub proposals: &'a [Entry],
}

impl Write<'_> {
    /// Whether the write holds nothing to make durable.
    pub fn is_empty(&self) -> bool {
        self.start.is_none()
            && self.entries.is_none()
            && self.hard_state.is_none()
            && self.proposals.is_empty()
    }
}

/// What the store found of the group when it opened, and keeps until the group's log reaches
/// past it (`docs/durable.md` §5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Health {
    /// Everything the member made durable reads back.
    #[default]
    Whole,
    /// A last frame was lost whose persist record survived: the term and vote are restored, and
    /// the log may lack entries the member acknowledged, through this point. Until the log holds
    /// them again, or an entry of a later term, the member judges votes against the mark and does
    /// not campaign.
    Marked(Point),
}

/// The group's durable state as the store holds it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreView {
    /// Where the group's log starts: the entry before its first.
    pub start: Point,
    /// The last entry's index; the start's when it holds none.
    pub last: u64,
    /// The hard state last made durable; all zero for none.
    pub hard_state: HardState,
    /// Whether the log may lack what the member acknowledged.
    pub health: Health,
}

/// Why a write was not made durable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Fault {
    /// Refused for room another write frees (the group's retained bound, the log's segments, its
    /// groups or its queue); it changed nothing, and the replica waits, whole, until the owner
    /// frees room (`docs/durable.md` §2.4).
    #[error("refused for room: {0}")]
    Room(&'static str),
    /// Submitted behind a write that was refused: it changed nothing.
    #[error("a write submitted behind a refused one")]
    Behind,
    /// Held by the store for its owner: a precondition of the store's own, which it names in its
    /// own type ([`LogStore::held`]), is not yet met. It changed nothing, and the replica waits,
    /// whole, until the owner meets it ([`LogStore::release`], `docs/durable.md` §2.4).
    #[error("held by the store for its owner")]
    Held,
    /// The write failed: the device's contents are unknown, and the replica is fenced until it
    /// is reopened from what is durable (Rebello et al., ATC 2020; `docs/research/durable.md`
    /// §5).
    #[error("the write failed: {0}")]
    Failed(&'static str),
}

impl Fault {
    /// Whether the write changed nothing and may be made again once there is room, or once the
    /// owner met what the store held it for.
    pub fn changed_nothing(&self) -> bool {
        matches!(self, Self::Room(_) | Self::Behind | Self::Held)
    }
}

/// Where a group's writes become durable, and what the core reads of its durable log.
///
/// Reads answer from what the answered writes left: a write is in the store's state once its
/// answer has been taken with [`LogStore::poll`].
pub trait LogStore {
    /// What the store may hold a write for until its owner meets it, outside the log: focal's
    /// store holds the first entry that needs a successor decoder until the group's record of
    /// that floor is durable (focal 27 §15.5, O2). [`std::convert::Infallible`] for a store that
    /// holds nothing.
    type Hold;

    /// What the store holds its refused write for, while it holds one ([`Fault::Held`]).
    fn held(&self) -> Option<&Self::Hold>;

    /// The owner met `met`: the store takes the write it held when it is made again.
    fn release(&mut self, met: &Self::Hold);

    /// The writes this store keeps out for the group at once (`docs/durable.md` §6): the core
    /// takes `Ready`s ahead of their persistence up to this many. hyper-log's handle gives one
    /// for each of the log's pipeline frames; a store that completes each write before `submit`
    /// returns gives one.
    fn depth(&self) -> usize;

    /// The group's durable state.
    fn view(&self) -> Result<StoreView, Fault>;

    /// Where the log starts and its last index.
    fn bounds(&self) -> Result<(Point, u64), StorageError>;

    /// The term of `index`, which may be the start's.
    fn term(&self, index: u64) -> Result<u64, StorageError>;

    /// The entries of `[low, high)` in order, appended to `into`: as many as `max_bytes` of
    /// their encoding admit, and one at least.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError>;

    /// Walks the entries of `[low, high)` in order where the store holds them, without copying
    /// any, until `visit` says to stop by returning true. The walk reads a page of at most
    /// `page` bytes at a time, and one entry at least.
    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError>;

    /// The fast track's proposals the store holds beside the log, appended to `into`.
    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError>;

    /// Whether the store takes another write now.
    fn room(&self) -> bool;

    /// Copies `write` into what the store writes and returns once it is on its way; `waker` is
    /// woken once its answer has come. A write refused here changed nothing.
    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault>;

    /// The oldest write's answer, if it has come.
    fn poll(&mut self) -> Option<Result<(), Fault>>;

    /// Makes `write` durable before returning: what a replica writes as it opens, before anything
    /// else (`docs/durable.md` §4.3).
    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault>;
}
