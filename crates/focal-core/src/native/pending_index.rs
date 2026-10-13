//! The pending chain's changes by key (doc 30 §9). A leader's candidate holds
//! its changes alone, not a built root, so a read during preparation finds a
//! key's pending change here before it reads the committed rows: one probe of
//! an ordered map for the newest change of every key, and the candidate that
//! holds it found by its serial. A view as of an older candidate, which a
//! caller asks for by its ticket, probes the candidates before it instead.
//!
//! The index holds one version a changed key, the newest. It is charged per
//! change of every candidate it holds ([`entry_charge`]), a charge each
//! candidate keeps until it is published or discarded, so its entries are
//! bounded by what the pending chain's memory funds.

use super::owner::Pending;
use super::ranges::Fragments;
use super::{Key, NativeError, NativePrepared, Row};
use focal_memory::{Change, Entry};
use std::collections::{BTreeMap, VecDeque};

/// A key's newest pending change: its candidate's serial, and the member and
/// the index of the change there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Version {
    pub(super) serial: u64,
    pub(super) member: usize,
    pub(super) change: usize,
}

/// What one change may take in the index: a B-tree node's slots for its key
/// and version with the node at its least occupancy (half of its eleven,
/// so twice the slot), its share of a node's header and children, and an
/// allocation's overhead.
pub(super) fn entry_charge() -> usize {
    size_of::<(Key, Version)>()
        .saturating_mul(2)
        .saturating_add(size_of::<usize>().saturating_mul(4))
}

/// The index's root node, which may hold fewer keys than [`entry_charge`]
/// assumes of a node: eleven slots, a header and twelve children.
pub(super) fn root_charge() -> usize {
    size_of::<(Key, Version)>()
        .saturating_mul(11)
        .saturating_add(size_of::<usize>().saturating_mul(16))
}

#[derive(Default)]
pub(super) struct PendingIndex {
    newest: BTreeMap<Key, Version>,
}

impl PendingIndex {
    pub(super) fn get(&self, key: &Key) -> Option<Version> {
        self.newest.get(key).copied()
    }

    /// Keys at or after `key` (`exclusive`: after it), with their newest
    /// versions, in key order.
    pub(super) fn range_from(
        &self,
        key: &Key,
        exclusive: bool,
    ) -> std::collections::btree_map::Range<'_, Key, Version> {
        use std::ops::Bound;
        let start = if exclusive {
            Bound::Excluded(*key)
        } else {
            Bound::Included(*key)
        };
        self.newest.range((start, Bound::Unbounded))
    }

    /// `fragments`, the candidate of `serial` newest in the chain, become the
    /// newest pending change of every key they change.
    pub(super) fn add(&mut self, serial: u64, fragments: &Fragments) {
        for (member, change, entry) in fragments.changes() {
            self.newest.insert(
                *entry.key(),
                Version {
                    serial,
                    member,
                    change,
                },
            );
        }
    }

    /// `fragments`, the chain's oldest candidate, are published: every key
    /// whose newest pending change was theirs has none left, its committed
    /// row now that change.
    pub(super) fn remove_published(&mut self, serial: u64, fragments: &Fragments) {
        for (_, _, change) in fragments.changes() {
            if self
                .newest
                .get(change.key())
                .is_some_and(|version| version.serial == serial)
            {
                self.newest.remove(change.key());
            }
        }
    }

    /// The inverse of [`Self::remove_published`] for a publication refused
    /// before anything changed: the oldest candidate's changes are again the
    /// newest of every key no later candidate changes.
    pub(super) fn restore_oldest(&mut self, serial: u64, fragments: &Fragments) {
        for (member, change, entry) in fragments.changes() {
            self.newest.entry(*entry.key()).or_insert(Version {
                serial,
                member,
                change,
            });
        }
    }

    /// `fragments`, the chain's newest candidate, are discarded: every key
    /// whose newest change was theirs takes its newest change among the
    /// candidates before it (`older`), or has none.
    pub(super) fn remove_discarded(
        &mut self,
        serial: u64,
        fragments: &Fragments,
        mut older: impl FnMut(&Key) -> Option<Version>,
    ) {
        for (_, _, change) in fragments.changes() {
            let key = change.key();
            if self
                .newest
                .get(key)
                .is_some_and(|version| version.serial == serial)
            {
                match older(key) {
                    Some(version) => {
                        self.newest.insert(*key, version);
                    }
                    None => {
                        self.newest.remove(key);
                    }
                }
            }
        }
    }
}

/// The most candidates a caller's chain holds (`Core`'s chained preparation
/// and its views); an owner's chain is bounded by its memory instead.
pub(super) const LISTED: usize = 16;

/// The pending chain a view reads through: an owner's, indexed, as of the
/// candidate of `upto`; or a caller's list of candidates (`Core`'s chained
/// preparation), held here and read by probing each, newest first.
#[derive(Clone, Copy)]
pub(super) enum Chain<'a> {
    Indexed {
        index: &'a PendingIndex,
        candidates: &'a VecDeque<Pending>,
        upto: u64,
    },
    Listed(Listed<'a>),
}

/// A caller's chain, oldest first.
#[derive(Clone, Copy)]
pub(super) struct Listed<'a> {
    candidates: [Option<&'a NativePrepared>; LISTED],
    len: usize,
}

impl<'a> Listed<'a> {
    fn iter(self) -> impl DoubleEndedIterator<Item = &'a NativePrepared> + 'a {
        self.candidates.into_iter().take(self.len).flatten()
    }
}

impl<'a> Chain<'a> {
    /// A caller's chain of `pending`, oldest first; none for an empty one.
    pub(super) fn listed(pending: &[&'a NativePrepared]) -> Result<Option<Self>, NativeError> {
        if pending.len() > LISTED {
            return Err(NativeError::Capacity("a caller's chain of candidates"));
        }
        let mut candidates = [None; LISTED];
        for (slot, candidate) in candidates.iter_mut().zip(pending) {
            *slot = Some(*candidate);
        }
        Ok((!pending.is_empty()).then_some(Self::Listed(Listed {
            candidates,
            len: pending.len(),
        })))
    }

    /// This caller's chain with `newest` after its candidates. An owner's
    /// chain takes candidates only as its owner prepares them.
    pub(super) fn then(self, newest: &'a NativePrepared) -> Result<Self, NativeError> {
        match self {
            Self::Indexed { .. } => Err(NativeError::Capacity(
                "an owner's chain takes only the candidates its owner holds",
            )),
            Self::Listed(mut listed) => {
                let slot = listed
                    .candidates
                    .get_mut(listed.len)
                    .ok_or(NativeError::Capacity("a caller's chain of candidates"))?;
                *slot = Some(newest);
                listed.len = listed
                    .len
                    .checked_add(1)
                    .ok_or(NativeError::Capacity("a caller's chain of candidates"))?;
                Ok(Self::Listed(listed))
            }
        }
    }

    pub(super) fn indexed(
        index: &'a PendingIndex,
        candidates: &'a VecDeque<Pending>,
        upto: u64,
    ) -> Self {
        Self::Indexed {
            index,
            candidates,
            upto,
        }
    }

    /// An indexed chain's candidates this view sees: those at or before
    /// `upto`.
    fn seen(candidates: &VecDeque<Pending>, upto: u64) -> usize {
        candidates.partition_point(|row| row.serial() <= upto)
    }

    /// The oldest candidate, the one whose base the published prefix is.
    pub(super) fn head(&self) -> Option<&'a NativePrepared> {
        match *self {
            Self::Indexed {
                candidates, upto, ..
            } => candidates
                .front()
                .filter(|row| row.serial() <= upto)
                .map(Pending::prepared),
            Self::Listed(listed) => listed.iter().next(),
        }
    }

    /// The newest candidate this view sees.
    pub(super) fn tail(&self) -> Option<&'a NativePrepared> {
        match *self {
            Self::Indexed {
                candidates, upto, ..
            } => Self::seen(candidates, upto)
                .checked_sub(1)
                .and_then(|at| candidates.get(at))
                .map(Pending::prepared),
            Self::Listed(listed) => listed.iter().next_back(),
        }
    }

    /// The candidates this view sees, newest first.
    fn newest_first(&self) -> impl Iterator<Item = &'a NativePrepared> + 'a {
        let (indexed, listed) = match *self {
            Self::Indexed {
                candidates, upto, ..
            } => (
                Some(
                    candidates
                        .range(..Self::seen(candidates, upto))
                        .rev()
                        .map(Pending::prepared),
                ),
                None,
            ),
            Self::Listed(listed) => (None, Some(listed.iter().rev())),
        };
        indexed
            .into_iter()
            .flatten()
            .chain(listed.into_iter().flatten())
    }

    /// The pending change of `key` this view sees: `Some(Some(entry))` a put,
    /// `Some(None)` a deletion, `None` none (the committed row stands).
    pub(super) fn get(&self, key: &Key) -> Option<Option<&'a Entry<Key, Row>>> {
        match *self {
            Self::Indexed { index, .. } => self.at(key, index.get(key)?),
            Self::Listed(_) => self.probe(key),
        }
    }

    fn probe(&self, key: &Key) -> Option<Option<&'a Entry<Key, Row>>> {
        self.newest_first()
            .find_map(|candidate| candidate.fragments.change(key))
    }

    fn at(&self, key: &Key, version: Version) -> Option<Option<&'a Entry<Key, Row>>> {
        let Self::Indexed {
            candidates, upto, ..
        } = *self
        else {
            return self.probe(key);
        };
        if version.serial > upto {
            // Its newest change is past this view: the newest at or before it.
            return self.probe(key);
        }
        let at = candidates
            .binary_search_by_key(&version.serial, Pending::serial)
            .ok()?;
        let change = candidates
            .get(at)?
            .prepared()
            .fragments
            .change_at(version.member, version.change)?;
        Some(match change {
            Change::Put(entry) => Some(entry),
            Change::Delete(_) => None,
        })
    }

    /// The pending changes this view sees at or after `key` (`exclusive`:
    /// after it), in key order.
    pub(super) fn changes_from(self, key: &Key, exclusive: bool) -> ChainScan<'a> {
        match self {
            Self::Indexed { index, .. } => ChainScan::Indexed {
                chain: self,
                keys: index.range_from(key, exclusive),
            },
            Self::Listed(candidates) => ChainScan::Listed {
                candidates,
                bound: Some((*key, exclusive)),
            },
        }
    }
}

/// A chain's pending changes in key order: an indexed chain's by its keys; a
/// listed chain's by the least next key among its candidates, found again
/// each step without a buffer (a caller's chain is short).
pub(super) enum ChainScan<'a> {
    Indexed {
        chain: Chain<'a>,
        keys: std::collections::btree_map::Range<'a, Key, Version>,
    },
    Listed {
        candidates: Listed<'a>,
        /// Where the next key lies: at or after a key, or after it.
        bound: Option<(Key, bool)>,
    },
}

impl<'a> Iterator for ChainScan<'a> {
    type Item = Step<'a>;
    fn next(&mut self) -> Option<Step<'a>> {
        match self {
            Self::Indexed { chain, keys } => {
                for (key, version) in keys.by_ref() {
                    if let Some(entry) = chain.at(key, *version) {
                        return Some((key, entry));
                    }
                }
                None
            }
            Self::Listed { candidates, bound } => {
                let (from, exclusive) = (*bound)?;
                let next = candidates
                    .iter()
                    .filter_map(|candidate| {
                        candidate.fragments.changes_from(&from, exclusive).next()
                    })
                    .map(Change::key)
                    .min()?;
                *bound = Some((*next, true));
                let entry = candidates
                    .iter()
                    .rev()
                    .find_map(|candidate| candidate.fragments.change(next))?;
                Some((next, entry))
            }
        }
    }
}

/// The newest change of `key` among `candidates`, oldest first, as a version.
pub(super) fn newest_among(candidates: &VecDeque<Pending>, key: &Key) -> Option<Version> {
    candidates.iter().rev().find_map(|row| {
        row.prepared()
            .fragments
            .position(key)
            .map(|(member, change)| Version {
                serial: row.serial(),
                member,
                change,
            })
    })
}

/// One step of a merged scan: a key and its row, or `None` where a pending
/// change deletes it.
pub(super) type Step<'a> = (&'a Key, Option<&'a Entry<Key, Row>>);

/// `upper`'s steps over `lower`'s, in key order: where both hold a key,
/// `upper`'s wins; a deletion passes through, so a scan drops it at the end.
pub(super) struct Overlay<U: Iterator, L: Iterator> {
    upper: std::iter::Peekable<U>,
    lower: std::iter::Peekable<L>,
}

impl<U: Iterator, L: Iterator> Overlay<U, L> {
    pub(super) fn new(upper: U, lower: L) -> Self {
        Self {
            upper: upper.peekable(),
            lower: lower.peekable(),
        }
    }
}

impl<'a, U, L> Iterator for Overlay<U, L>
where
    U: Iterator<Item = Step<'a>>,
    L: Iterator<Item = Step<'a>>,
{
    type Item = Step<'a>;
    fn next(&mut self) -> Option<Step<'a>> {
        let order = match (self.upper.peek(), self.lower.peek()) {
            (Some(upper), Some(lower)) => upper.0.cmp(lower.0),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => return None,
        };
        match order {
            std::cmp::Ordering::Less => self.upper.next(),
            std::cmp::Ordering::Greater => self.lower.next(),
            std::cmp::Ordering::Equal => {
                self.lower.next();
                self.upper.next()
            }
        }
    }
}
