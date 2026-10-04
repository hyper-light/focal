//! The log as the core sees it: what storage holds, and after it what is
//! not yet durable.
//!
//! `applied <= committed`, and nothing is given to apply that is not both
//! committed and durable here. `persisted` is the highest index known
//! durable; an entry that replaces a durable one lowers it.
use crate::{
    error::{Error, Result, StorageError},
    proto::{self, Entry, Snapshot},
    storage::Storage,
};

/// Entries and a snapshot that storage does not hold yet. The entry at
/// position `i` has the index `offset + i`. `offset` may be at or below
/// what storage holds: the next write truncates storage there.
#[derive(Debug, Default)]
pub struct Unstable {
    pub(crate) snapshot: Option<Snapshot>,
    pub(crate) entries: Vec<Entry>,
    /// About the bytes the entries state (`proto::approximate_bytes`).
    pub(crate) bytes: usize,
    /// The bytes the entries' buffers hold, by capacity: what they cost
    /// resident, kept as they come and go so that asking costs nothing.
    pub(crate) payload: usize,
    pub(crate) offset: u64,
}

fn position(index: u64, offset: u64) -> Option<usize> {
    usize::try_from(index.checked_sub(offset)?).ok()
}
/// A copy that refuses where a clone would abort.
pub(crate) fn copy_entry(entry: &Entry) -> Result<Entry> {
    let mut data = Vec::new();
    data.try_reserve_exact(entry.data.len())
        .map_err(|_| Error::Memory)?;
    data.extend_from_slice(&entry.data);
    let mut context = Vec::new();
    context
        .try_reserve_exact(entry.context.len())
        .map_err(|_| Error::Memory)?;
    context.extend_from_slice(&entry.context);
    Ok(Entry {
        entry_type: entry.entry_type,
        term: entry.term,
        index: entry.index,
        data,
        context,
        sync_log: entry.sync_log,
    })
}
/// Copies the entries after `into`'s, reserving for exactly that many: a
/// page holds no spare room, whichever path builds it.
pub(crate) fn copy_entries_of<'a>(
    entries: impl Iterator<Item = &'a Entry> + Clone,
    into: &mut Vec<Entry>,
) -> Result<()> {
    into.try_reserve_exact(entries.clone().count())
        .map_err(|_| Error::Memory)?;
    for entry in entries {
        into.push(copy_entry(entry)?);
    }
    Ok(())
}
pub(crate) fn copy_entries(entries: &[Entry], into: &mut Vec<Entry>) -> Result<()> {
    into.try_reserve_exact(entries.len())
        .map_err(|_| Error::Memory)?;
    for entry in entries {
        into.push(copy_entry(entry)?);
    }
    Ok(())
}
/// Keeps as many entries as `max_bytes` of their encoding admit, and one at
/// least: the longest prefix whose running total fits.
pub(crate) fn limit_bytes(entries: &mut Vec<Entry>, max_bytes: u64) {
    if entries.len() <= 1 || max_bytes == u64::MAX {
        return;
    }
    let kept = page_of(entries, 0, 0, max_bytes);
    entries.truncate(kept);
}
/// How many of `entries`, in order, a page admits that already holds
/// `held` entries of `used` bytes: the rule of [`limit_bytes`] carried on
/// across a boundary, so that a page is chosen before any of it is copied.
/// Every entry is taken while the running total fits, and the first is
/// taken whatever its bytes when the page holds nothing yet.
pub(crate) fn page_of(entries: &[Entry], held: usize, used: u64, max_bytes: u64) -> usize {
    if max_bytes == u64::MAX {
        return entries.len();
    }
    let mut bytes = used;
    let mut taken = 0usize;
    for entry in entries {
        bytes = bytes.saturating_add(proto::encoded_bytes(entry));
        if held.saturating_add(taken) > 0 && bytes > max_bytes {
            break;
        }
        taken = taken.saturating_add(1);
    }
    taken
}
/// The bytes an entry's buffers hold, by capacity.
pub(crate) fn payload_of(entry: &Entry) -> usize {
    entry
        .data
        .capacity()
        .saturating_add(entry.context.capacity())
}

impl Unstable {
    fn first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snapshot| proto::snapshot_index(snapshot).saturating_add(1))
    }
    fn last_index(&self) -> Option<u64> {
        match u64::try_from(self.entries.len()) {
            Ok(0) | Err(_) => self.snapshot.as_ref().map(proto::snapshot_index),
            Ok(length) => Some(self.offset.saturating_add(length).saturating_sub(1)),
        }
    }
    fn term(&self, index: u64) -> Option<u64> {
        if index < self.offset {
            let snapshot = self.snapshot.as_ref()?;
            return (index == proto::snapshot_index(snapshot))
                .then(|| proto::snapshot_term(snapshot));
        }
        self.entries
            .get(position(index, self.offset)?)
            .map(|entry| entry.term)
    }
    fn end(&self) -> u64 {
        self.offset
            .saturating_add(u64::try_from(self.entries.len()).unwrap_or(u64::MAX))
    }
    fn truncate_and_append(&mut self, entries: &[Entry], limit: usize) -> Result<()> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let after = first.index;
        let kept = if after == self.end() {
            self.entries.len()
        } else if after <= self.offset {
            0
        } else {
            position(after, self.offset).ok_or(Error::Invariant("an index before the offset"))?
        };
        if kept.saturating_add(entries.len()) > limit {
            return Err(Error::Capacity("entries not yet durable"));
        }
        // Everything that can refuse has, before anything is replaced.
        let mut copies = Vec::new();
        copy_entries(entries, &mut copies)?;
        self.entries
            .try_reserve(
                copies
                    .len()
                    .saturating_sub(self.entries.len().saturating_sub(kept)),
            )
            .map_err(|_| Error::Memory)?;
        if after <= self.offset && after != self.end() {
            self.offset = after;
        }
        for entry in self.entries.drain(kept..) {
            self.bytes = self.bytes.saturating_sub(proto::approximate_bytes(&entry));
            self.payload = self.payload.saturating_sub(payload_of(&entry));
        }
        for entry in copies {
            self.bytes = self.bytes.saturating_add(proto::approximate_bytes(&entry));
            self.payload = self.payload.saturating_add(payload_of(&entry));
            self.entries.push(entry);
        }
        Ok(())
    }
    fn slice(&self, low: u64, high: u64) -> Result<&[Entry]> {
        let range = position(low, self.offset)
            .zip(position(high, self.offset))
            .filter(|(low, high)| low <= high)
            .ok_or(Error::Invariant("a range outside what is not yet durable"))?;
        self.entries
            .get(range.0..range.1)
            .ok_or(Error::Invariant("a range outside what is not yet durable"))
    }
    fn restore(&mut self, snapshot: Snapshot) {
        self.entries = Vec::new();
        self.bytes = 0;
        self.payload = 0;
        self.offset = proto::snapshot_index(&snapshot).saturating_add(1);
        self.snapshot = Some(snapshot);
    }
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }
    /// The bytes the entries' buffers hold, by capacity.
    pub fn payload(&self) -> usize {
        self.payload
    }
    /// About the bytes the entries encode to (`proto::approximate_bytes`).
    pub fn encoded_bytes(&self) -> usize {
        self.bytes
    }
    /// The bytes the snapshot holds, by capacity; zero for none.
    pub fn snapshot_bytes(&self) -> usize {
        self.snapshot.as_ref().map_or(0, |snapshot| {
            snapshot
                .data
                .capacity()
                .saturating_add(std::mem::size_of::<Snapshot>())
        })
    }
    /// The bytes held, by capacity: the entries' slots and buffers, and
    /// the snapshot. Nothing is walked: the counters say it.
    pub fn resident_bytes(&self) -> usize {
        self.entries
            .capacity()
            .saturating_mul(std::mem::size_of::<Entry>())
            .saturating_add(self.payload)
            .saturating_add(self.snapshot_bytes())
    }
    /// Whether the counters say what a walk of the entries says.
    pub(crate) fn check(&self) -> Result<()> {
        let (bytes, payload) =
            self.entries
                .iter()
                .fold((0usize, 0usize), |(bytes, payload), entry| {
                    (
                        bytes.saturating_add(proto::approximate_bytes(entry)),
                        payload.saturating_add(payload_of(entry)),
                    )
                });
        if bytes != self.bytes || payload != self.payload {
            return Err(Error::Invariant(
                "what is not yet durable is not what its counters say",
            ));
        }
        Ok(())
    }
}

pub struct Log<S> {
    pub(crate) store: S,
    pub(crate) unstable: Unstable,
    pub(crate) committed: u64,
    pub(crate) persisted: u64,
    pub(crate) applied: u64,
    /// The most entries held that are not yet durable.
    max_unstable: usize,
}

impl<S: Storage> Log<S> {
    pub fn new(store: S, max_unstable: usize) -> Result<Self> {
        let first = store.first_index()?;
        let last = store.last_index()?;
        if first == 0 || last == u64::MAX || last.saturating_add(1) < first {
            return Err(Error::Invariant("the stored log's bounds"));
        }
        Ok(Self {
            store,
            committed: first.saturating_sub(1),
            persisted: last,
            applied: first.saturating_sub(1),
            unstable: Unstable {
                offset: last.saturating_add(1),
                ..Unstable::default()
            },
            max_unstable,
        })
    }
    pub fn store(&self) -> &S {
        &self.store
    }
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
    pub fn committed(&self) -> u64 {
        self.committed
    }
    pub fn persisted(&self) -> u64 {
        self.persisted
    }
    pub fn applied(&self) -> u64 {
        self.applied
    }
    pub fn unstable(&self) -> &Unstable {
        &self.unstable
    }
    pub fn first_index(&self) -> Result<u64> {
        match self.unstable.first_index() {
            Some(index) => Ok(index),
            None => Ok(self.store.first_index()?),
        }
    }
    pub fn last_index(&self) -> Result<u64> {
        match self.unstable.last_index() {
            Some(index) => Ok(index),
            None => Ok(self.store.last_index()?),
        }
    }
    /// The term of the entry at `index`; zero for an index the log does not
    /// reach. An index compacted away is an error.
    pub fn term(&self, index: u64) -> Result<u64> {
        let before = self.first_index()?.saturating_sub(1);
        if index < before || index > self.last_index()? {
            return Ok(0);
        }
        match self.unstable.term(index) {
            Some(term) => Ok(term),
            None => Ok(self.store.term(index)?),
        }
    }
    pub fn last_term(&self) -> Result<u64> {
        self.term(self.last_index()?)
            .map_err(|_| Error::Invariant("the last entry's term is not held"))
    }
    pub fn match_term(&self, index: u64, term: u64) -> bool {
        self.term(index).is_ok_and(|held| held == term)
    }
    /// The index of the first of `entries` that the log does not hold with
    /// the same term; zero when it holds them all.
    pub fn find_conflict(&self, entries: &[Entry]) -> u64 {
        entries
            .iter()
            .find(|entry| !self.match_term(entry.index, entry.term))
            .map_or(0, |entry| entry.index)
    }
    /// The highest index at or below `index` whose term is at most `term`,
    /// and that term; no term when the log cannot say.
    pub fn find_conflict_by_term(&self, index: u64, term: u64) -> Result<(u64, Option<u64>)> {
        if index > self.last_index()? {
            return Ok((index, None));
        }
        let mut conflict = index;
        loop {
            match self.term(conflict) {
                Ok(held) if held > term && conflict > 0 => {
                    conflict = conflict.saturating_sub(1);
                }
                Ok(held) => return Ok((conflict, Some(held))),
                Err(_) => return Ok((conflict, None)),
            }
        }
    }
    /// Appends what a leader sent after `(index, term)`, which the log must
    /// hold. None when it does not; otherwise the first index replaced
    /// (zero for none) and the last index sent.
    pub fn maybe_append(
        &mut self,
        index: u64,
        term: u64,
        committed: u64,
        entries: &[Entry],
    ) -> Result<Option<(u64, u64)>> {
        self.append_after(index, term, committed, entries, false)
    }
    /// As [`Log::maybe_append`]. With `committed_agrees`, what is committed
    /// here is taken to be what the leader holds there, whatever term it
    /// bears: the point the entries follow matches when it is committed,
    /// and an entry at a committed index is set aside.
    ///
    /// A leader holds every committed entry, so this takes nothing on
    /// trust that the terms would not show. It is for the fast track
    /// ([`crate::fast`]), where an entry committed by the fast quorum bears
    /// the term of the leader that took it, and the leader after it, which
    /// took it again at its election, gave it its own.
    pub fn append_after(
        &mut self,
        index: u64,
        term: u64,
        committed: u64,
        entries: &[Entry],
        committed_agrees: bool,
    ) -> Result<Option<(u64, u64)>> {
        let agreed = if committed_agrees { self.committed } else { 0 };
        if (index > agreed || !committed_agrees) && !self.match_term(index, term) {
            return Ok(None);
        }
        let skipped = entries
            .iter()
            .take_while(|entry| entry.index <= agreed)
            .count();
        let entries = entries.get(skipped..).unwrap_or(&[]);
        let index = index.saturating_add(u64::try_from(skipped).unwrap_or(u64::MAX));
        let last_new = index
            .checked_add(u64::try_from(entries.len()).unwrap_or(u64::MAX))
            .ok_or(Error::Violation("an index beyond what can be counted"))?;
        let conflict = self.find_conflict(entries);
        if conflict != 0 {
            if conflict <= self.committed {
                return Err(Error::Violation("an entry replaces a committed one"));
            }
            let start = position(conflict, index.saturating_add(1))
                .ok_or(Error::Violation("entries out of order"))?;
            let suffix = entries
                .get(start..)
                .ok_or(Error::Violation("entries out of order"))?;
            self.append(suffix)?;
            // What replaced a durable entry is not durable.
            self.persisted = self.persisted.min(conflict.saturating_sub(1));
        }
        self.commit_to(committed.min(last_new))?;
        Ok(Some((conflict, last_new)))
    }
    pub fn commit_to(&mut self, to: u64) -> Result<()> {
        if self.committed >= to {
            return Ok(());
        }
        if self.last_index()? < to {
            return Err(Error::Violation("a commit beyond the log"));
        }
        self.committed = to;
        Ok(())
    }
    pub fn applied_to(&mut self, index: u64) -> Result<()> {
        if index == 0 {
            return Ok(());
        }
        if index > self.committed || index < self.applied {
            return Err(Error::Invariant("applied outside what is committed"));
        }
        self.applied = index;
        Ok(())
    }
    /// At opening, what was applied may be ahead of what is known committed.
    pub(crate) fn applied_to_unchecked(&mut self, index: u64) {
        self.applied = index;
    }
    /// The entries through `(index, term)` are handed to storage.
    pub fn stable_entries(&mut self, index: u64, term: u64) -> Result<()> {
        if self.unstable.snapshot.is_some() {
            return Err(Error::Invariant(
                "entries made durable before their snapshot",
            ));
        }
        let last = self
            .unstable
            .entries
            .last()
            .ok_or(Error::Invariant("nothing to make durable"))?;
        if last.index != index || last.term != term {
            return Err(Error::Invariant(
                "what was made durable is not what was given",
            ));
        }
        self.unstable.offset = index.saturating_add(1);
        // Given up, and not emptied: a member that rests holds what it
        // held before it was written to.
        self.unstable.entries = Vec::new();
        self.unstable.bytes = 0;
        self.unstable.payload = 0;
        Ok(())
    }
    pub fn stable_snapshot(&mut self, index: u64) -> Result<()> {
        match &self.unstable.snapshot {
            Some(snapshot) if proto::snapshot_index(snapshot) == index => {
                self.unstable.snapshot = None;
                Ok(())
            }
            _ => Err(Error::Invariant(
                "the snapshot made durable is not the one given",
            )),
        }
    }
    /// Appends after what is committed, replacing what follows.
    pub fn append(&mut self, entries: &[Entry]) -> Result<u64> {
        let Some(first) = entries.first() else {
            return self.last_index();
        };
        if first.index == 0 || first.index.saturating_sub(1) < self.committed {
            return Err(Error::Invariant("an append into what is committed"));
        }
        self.unstable
            .truncate_and_append(entries, self.max_unstable)?;
        self.last_index()
    }
    /// The entries from `index` on: as many as `max_bytes` admit, and at
    /// most `max_entries` of them. Both bound a prefix, so the page is the
    /// same whichever is applied first: cutting the range to `max_entries`
    /// before the bytes are counted takes exactly what cutting the
    /// byte-limited page to `max_entries` would, and copies no more than
    /// the page.
    pub fn entries(&self, index: u64, max_bytes: u64, max_entries: usize) -> Result<Vec<Entry>> {
        let last = self.last_index()?;
        if index > last {
            return Ok(Vec::new());
        }
        let high = index
            .saturating_add(u64::try_from(max_entries).unwrap_or(u64::MAX))
            .min(last.saturating_add(1));
        self.slice(index, high, max_bytes)
    }
    pub fn is_up_to_date(&self, last_index: u64, term: u64) -> Result<bool> {
        let held = self.last_term()?;
        Ok(term > held || (term == held && last_index >= self.last_index()?))
    }
    fn apply_bound(&self) -> u64 {
        self.committed.min(self.persisted)
    }
    /// Whether entries after `since` are committed and durable.
    pub fn has_next_entries_since(&self, since: u64) -> Result<bool> {
        let offset = since.saturating_add(1).max(self.first_index()?);
        Ok(self.apply_bound().saturating_add(1) > offset)
    }
    /// The entries after `since` that are committed and durable.
    pub fn next_entries_since(&self, since: u64, max_bytes: u64) -> Result<Vec<Entry>> {
        let offset = since.saturating_add(1).max(self.first_index()?);
        let high = self.apply_bound().saturating_add(1);
        if high > offset {
            self.slice(offset, high, max_bytes)
        } else {
            Ok(Vec::new())
        }
    }
    pub fn snapshot(&self, request_index: u64, to: u64) -> std::result::Result<Snapshot, Error> {
        if let Some(snapshot) = &self.unstable.snapshot
            && proto::snapshot_index(snapshot) >= request_index
        {
            return copy_snapshot(snapshot);
        }
        Ok(self.store.snapshot(request_index, to)?)
    }
    /// Commits `index` if its entry is of `term`.
    pub fn maybe_commit(&mut self, index: u64, term: u64) -> Result<bool> {
        if index > self.committed && self.term(index).is_ok_and(|held| held == term) {
            self.commit_to(index)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    /// Storage holds the entries through `(index, term)`. An index at or
    /// above what is not yet durable is one a later append replaced while
    /// the write was under way: it is not counted.
    pub fn maybe_persist(&mut self, index: u64, term: u64) -> bool {
        let first_update = match &self.unstable.snapshot {
            Some(snapshot) => proto::snapshot_index(snapshot),
            None => self.unstable.offset,
        };
        if index > self.persisted
            && index < first_update
            && self.store.term(index).is_ok_and(|held| held == term)
        {
            self.persisted = index;
            true
        } else {
            false
        }
    }
    pub fn maybe_persist_snapshot(&mut self, index: u64) -> Result<bool> {
        if index <= self.persisted {
            return Ok(false);
        }
        if index > self.committed {
            return Err(Error::Invariant("a snapshot beyond what is committed"));
        }
        if index >= self.unstable.offset {
            return Err(Error::Invariant(
                "a snapshot at or after entries that follow it",
            ));
        }
        self.persisted = index;
        Ok(true)
    }
    /// The bounds of a range the log is asked about.
    fn bounded(&self, low: u64, high: u64) -> Result<()> {
        if low > high {
            return Err(Error::Invariant("a range that ends before it begins"));
        }
        if low < self.first_index()? {
            return Err(Error::Storage(StorageError::Compacted));
        }
        if high > self.last_index()?.saturating_add(1) {
            return Err(Error::Invariant("a range beyond the log"));
        }
        Ok(())
    }
    /// Whether an entry of `[low, high)` satisfies `predicate`. Nothing is
    /// copied: what storage holds is asked of storage, and what is not yet
    /// durable is read where it is; the first entry that satisfies it ends
    /// the walk.
    pub fn any_entry(
        &self,
        low: u64,
        high: u64,
        mut predicate: impl FnMut(&Entry) -> bool,
    ) -> Result<bool> {
        self.bounded(low, high)?;
        if low < self.unstable.offset
            && self
                .store
                .any_entry(low, high.min(self.unstable.offset), &mut predicate)?
        {
            return Ok(true);
        }
        if high > self.unstable.offset {
            let from = low.max(self.unstable.offset);
            return Ok(self.unstable.slice(from, high)?.iter().any(predicate));
        }
        Ok(false)
    }
    /// The entries of `[low, high)`, as many as `max_bytes` admit: the
    /// longest prefix whose running total fits, and one at least.
    ///
    /// The page is chosen before it is copied, and the copy is reserved
    /// for exactly. Storage chooses its own part by the same rule; when it
    /// gives the whole of what was asked of it, the running total carries
    /// on into what is not yet durable, which is read where it is and
    /// copied only as far as the page reaches. The result is what copying
    /// everything and then cutting it ([`limit_bytes`]) would give, for
    /// both keep the longest prefix that fits and the first entry whatever
    /// its bytes; the cut is kept after all the same, so that a storage
    /// that gives more than its part admits is cut here.
    pub fn slice(&self, low: u64, high: u64, max_bytes: u64) -> Result<Vec<Entry>> {
        self.bounded(low, high)?;
        let mut entries = Vec::new();
        if low == high {
            return Ok(entries);
        }
        // The bytes of the page so far, by the rule the cut counts.
        let mut used = 0u64;
        if low < self.unstable.offset {
            let stored_high = high.min(self.unstable.offset);
            self.store
                .entries(low, stored_high, max_bytes, &mut entries)?;
            let wanted = stored_high.saturating_sub(low);
            if u64::try_from(entries.len()).unwrap_or(u64::MAX) < wanted {
                // Storage cut the page: it is complete.
                return Ok(entries);
            }
            if max_bytes != u64::MAX {
                used = entries.iter().fold(0u64, |bytes, entry| {
                    bytes.saturating_add(proto::encoded_bytes(entry))
                });
            }
        }
        if high > self.unstable.offset {
            let from = low.max(self.unstable.offset);
            let tail = self.unstable.slice(from, high)?;
            let taken = page_of(tail, entries.len(), used, max_bytes);
            entries
                .try_reserve_exact(taken)
                .map_err(|_| Error::Memory)?;
            for entry in tail.get(..taken).unwrap_or(&[]) {
                entries.push(copy_entry(entry)?);
            }
        }
        limit_bytes(&mut entries, max_bytes);
        Ok(entries)
    }
    /// The log begins again after `snapshot`.
    pub fn restore(&mut self, snapshot: Snapshot) -> Result<()> {
        let index = proto::snapshot_index(&snapshot);
        if index < self.committed {
            return Err(Error::Invariant("a snapshot behind what is committed"));
        }
        // Only durable entries at or below the commit are known to be what
        // the snapshot holds.
        self.persisted = self.persisted.min(self.committed);
        self.committed = index;
        self.unstable.restore(snapshot);
        Ok(())
    }
    /// The committed index and its term.
    pub fn commit_info(&self) -> Result<(u64, u64)> {
        let term = self
            .term(self.committed)
            .map_err(|_| Error::Invariant("the committed entry's term is not held"))?;
        Ok((self.committed, term))
    }
}

pub(crate) fn copy_snapshot(snapshot: &Snapshot) -> Result<Snapshot> {
    let mut data = Vec::new();
    data.try_reserve_exact(snapshot.data.len())
        .map_err(|_| Error::Memory)?;
    data.extend_from_slice(&snapshot.data);
    Ok(Snapshot {
        data,
        metadata: snapshot.metadata.clone(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        proto::{ConfState, HardState, SnapshotMetadata},
        storage::InitialState,
    };

    /// Storage in memory for tests: a snapshot and the entries after it.
    #[derive(Clone, Debug, Default)]
    pub(crate) struct Memory {
        pub hard_state: HardState,
        pub configuration: ConfState,
        pub snapshot: Snapshot,
        pub entries: Vec<Entry>,
        /// What the member approved by itself.
        pub proposals: Vec<Entry>,
    }
    impl Memory {
        pub fn with_voters(voters: &[u64]) -> Self {
            Self {
                configuration: ConfState {
                    voters: voters.to_vec(),
                    ..ConfState::default()
                },
                ..Self::default()
            }
        }
        pub fn append(&mut self, entries: &[Entry]) {
            for entry in entries {
                let first = proto::snapshot_index(&self.snapshot) + 1;
                if entry.index < first {
                    continue;
                }
                self.entries.truncate((entry.index - first) as usize);
                self.entries.push(entry.clone());
            }
        }
        pub fn install(&mut self, snapshot: Snapshot) {
            self.configuration = snapshot
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.conf_state.clone())
                .unwrap_or_default();
            self.hard_state.commit = self.hard_state.commit.max(proto::snapshot_index(&snapshot));
            self.entries.clear();
            self.snapshot = snapshot;
        }
        /// Everything through `index` becomes the snapshot.
        pub fn compact(&mut self, index: u64, data: Vec<u8>) {
            let term = Storage::term(self, index).unwrap();
            let first = proto::snapshot_index(&self.snapshot) + 1;
            self.entries.drain(..(index + 1 - first) as usize);
            self.snapshot = Snapshot {
                data,
                metadata: Some(SnapshotMetadata {
                    conf_state: Some(self.configuration.clone()),
                    index,
                    term,
                }),
            };
        }
    }
    impl Storage for Memory {
        fn initial_state(&self) -> std::result::Result<InitialState, StorageError> {
            Ok(InitialState {
                hard_state: self.hard_state.clone(),
                configuration: self.configuration.clone(),
                proposals: self.proposals.clone(),
            })
        }
        fn entries(
            &self,
            low: u64,
            high: u64,
            max_bytes: u64,
            into: &mut Vec<Entry>,
        ) -> std::result::Result<(), StorageError> {
            let first = self.first_index()?;
            if low < first {
                return Err(StorageError::Compacted);
            }
            if low > high || high > self.last_index()? + 1 {
                return Err(StorageError::Unavailable);
            }
            let range = &self.entries[(low - first) as usize..(high - first) as usize];
            let taken = page_of(range, 0, 0, max_bytes);
            into.try_reserve_exact(taken)
                .map_err(|_| StorageError::Unavailable)?;
            into.extend(range[..taken].iter().cloned());
            Ok(())
        }
        fn any_entry(
            &self,
            low: u64,
            high: u64,
            predicate: &mut dyn FnMut(&Entry) -> bool,
        ) -> std::result::Result<bool, StorageError> {
            let first = self.first_index()?;
            if low < first {
                return Err(StorageError::Compacted);
            }
            if low > high || high > self.last_index()? + 1 {
                return Err(StorageError::Unavailable);
            }
            Ok(
                self.entries[(low - first) as usize..(high - first) as usize]
                    .iter()
                    .any(predicate),
            )
        }
        fn term(&self, index: u64) -> std::result::Result<u64, StorageError> {
            let snapshot = proto::snapshot_index(&self.snapshot);
            if index == snapshot {
                return Ok(proto::snapshot_term(&self.snapshot));
            }
            if index < snapshot {
                return Err(StorageError::Compacted);
            }
            self.entries
                .get((index - snapshot - 1) as usize)
                .map(|entry| entry.term)
                .ok_or(StorageError::Unavailable)
        }
        fn first_index(&self) -> std::result::Result<u64, StorageError> {
            Ok(proto::snapshot_index(&self.snapshot) + 1)
        }
        fn last_index(&self) -> std::result::Result<u64, StorageError> {
            Ok(proto::snapshot_index(&self.snapshot) + self.entries.len() as u64)
        }
        fn snapshot(
            &self,
            request_index: u64,
            _to: u64,
        ) -> std::result::Result<Snapshot, StorageError> {
            if proto::snapshot_is_empty(&self.snapshot)
                || proto::snapshot_index(&self.snapshot) < request_index
            {
                return Err(StorageError::SnapshotTemporarilyUnavailable);
            }
            Ok(self.snapshot.clone())
        }
    }

    pub(crate) fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            ..Entry::default()
        }
    }
    pub(crate) fn snapshot(index: u64, term: u64, voters: &[u64]) -> Snapshot {
        Snapshot {
            data: vec![],
            metadata: Some(SnapshotMetadata {
                conf_state: Some(ConfState {
                    voters: voters.to_vec(),
                    ..ConfState::default()
                }),
                index,
                term,
            }),
        }
    }
    fn log_of(entries: &[Entry]) -> Log<Memory> {
        let mut log = Log::new(Memory::default(), 1024).unwrap();
        log.append(entries).unwrap();
        log
    }
    fn indexes(entries: &[Entry]) -> Vec<(u64, u64)> {
        entries
            .iter()
            .map(|entry| (entry.index, entry.term))
            .collect()
    }

    #[test]
    fn a_conflict_is_the_first_entry_the_log_does_not_hold() {
        let held = [entry(1, 1), entry(2, 2), entry(3, 3)];
        for (given, conflict) in [
            (vec![], 0),
            (vec![entry(1, 1), entry(2, 2), entry(3, 3)], 0),
            (vec![entry(2, 2), entry(3, 3)], 0),
            (vec![entry(3, 3)], 0),
            (
                vec![
                    entry(1, 1),
                    entry(2, 2),
                    entry(3, 3),
                    entry(4, 4),
                    entry(5, 4),
                ],
                4,
            ),
            (vec![entry(3, 3), entry(4, 4), entry(5, 4)], 4),
            (vec![entry(4, 4), entry(5, 4)], 4),
            (vec![entry(1, 4), entry(2, 4)], 1),
            (vec![entry(2, 1), entry(3, 4), entry(4, 4)], 2),
            (vec![entry(3, 1), entry(4, 2), entry(5, 4), entry(6, 4)], 3),
        ] {
            assert_eq!(log_of(&held).find_conflict(&given), conflict, "{given:?}");
        }
    }
    #[test]
    fn a_log_is_current_by_its_last_term_and_then_its_length() {
        let log = log_of(&[entry(1, 1), entry(2, 2), entry(3, 3)]);
        for (index, term, current) in [
            (2, 4, true),
            (3, 4, true),
            (4, 4, true),
            (2, 2, false),
            (3, 2, false),
            (4, 2, false),
            (2, 3, false),
            (3, 3, true),
            (4, 3, true),
        ] {
            assert_eq!(log.is_up_to_date(index, term).unwrap(), current);
        }
    }
    #[test]
    fn an_append_replaces_what_follows_it() {
        for (given, last, held, offset) in [
            (vec![], 2, vec![(1, 1), (2, 2)], 3),
            (vec![entry(3, 2)], 3, vec![(1, 1), (2, 2), (3, 2)], 3),
            (vec![entry(1, 2)], 1, vec![(1, 2)], 1),
            (
                vec![entry(2, 3), entry(3, 3)],
                3,
                vec![(1, 1), (2, 3), (3, 3)],
                2,
            ),
        ] {
            let mut store = Memory::default();
            store.append(&[entry(1, 1), entry(2, 2)]);
            let mut log = Log::new(store, 1024).unwrap();
            assert_eq!(log.append(&given).unwrap(), last);
            assert_eq!(
                indexes(&log.entries(1, u64::MAX, usize::MAX).unwrap()),
                held
            );
            assert_eq!(log.unstable.offset, offset);
        }
    }
    #[test]
    fn a_leaders_entries_are_taken_after_a_point_both_hold() {
        let held = [entry(1, 1), entry(2, 2), entry(3, 3)];
        let (last, last_term, commit) = (3u64, 3u64, 1u64);
        // (term and index the entries follow, the leader's commit, entries,
        //  the last index after, the commit after; none when refused)
        type Case = (u64, u64, u64, Vec<Entry>, Option<(u64, u64)>);
        let cases: Vec<Case> = vec![
            (last_term - 1, last, last, vec![entry(last + 1, 4)], None),
            (last_term, last + 1, last, vec![entry(last + 2, 4)], None),
            (last_term, last, last, vec![], Some((last, last))),
            (last_term, last, last + 1, vec![], Some((last, last))),
            (last_term, last, last - 1, vec![], Some((last, last - 1))),
            (last_term, last, 0, vec![], Some((last, commit))),
            (0, 0, last, vec![], Some((0, commit))),
            (
                last_term,
                last,
                last,
                vec![entry(last + 1, 4)],
                Some((last + 1, last)),
            ),
            (
                last_term,
                last,
                last + 1,
                vec![entry(last + 1, 4)],
                Some((last + 1, last + 1)),
            ),
            (
                last_term,
                last,
                last + 2,
                vec![entry(last + 1, 4)],
                Some((last + 1, last + 1)),
            ),
            (
                last_term,
                last,
                last + 2,
                vec![entry(last + 1, 4), entry(last + 2, 4)],
                Some((last + 2, last + 2)),
            ),
            (
                last_term - 1,
                last - 1,
                last,
                vec![entry(last, 4)],
                Some((last, last)),
            ),
            (
                last_term - 2,
                last - 2,
                last,
                vec![entry(last - 1, 4)],
                Some((last - 1, last - 1)),
            ),
            (
                last_term - 2,
                last - 2,
                last,
                vec![entry(last - 1, 4), entry(last, 4)],
                Some((last, last)),
            ),
        ];
        for (term, index, committed, given, expected) in cases {
            let mut log = log_of(&held);
            log.committed = commit;
            let outcome = log.maybe_append(index, term, committed, &given).unwrap();
            assert_eq!(
                outcome.map(|(_, last)| last),
                expected.map(|(last, _)| last)
            );
            if let Some((_, committed)) = expected {
                assert_eq!(log.committed, committed);
                if let Some(first) = given.first() {
                    let taken = log
                        .slice(first.index, first.index + given.len() as u64, u64::MAX)
                        .unwrap();
                    assert_eq!(indexes(&taken), indexes(&given));
                }
            }
        }
        // What another core asserts is refused here, and changes nothing.
        let mut log = log_of(&held);
        log.committed = 3;
        assert_eq!(
            log.maybe_append(0, 0, 3, &[entry(1, 4)]),
            Err(Error::Violation("an entry replaces a committed one"))
        );
        assert_eq!(
            indexes(&log.entries(1, u64::MAX, usize::MAX).unwrap()),
            vec![(1, 1), (2, 2), (3, 3)]
        );
    }
    #[test]
    fn a_commit_never_passes_the_log_or_goes_back() {
        let mut log = log_of(&[entry(1, 1), entry(2, 2), entry(3, 3)]);
        log.committed = 2;
        log.commit_to(3).unwrap();
        assert_eq!(log.committed, 3);
        log.commit_to(1).unwrap();
        assert_eq!(log.committed, 3);
        assert_eq!(
            log.commit_to(4),
            Err(Error::Violation("a commit beyond the log"))
        );
        assert_eq!(log.committed, 3);
        assert!(log.maybe_commit(3, 3).is_ok_and(|committed| !committed));
        let mut log = log_of(&[entry(1, 1), entry(2, 2), entry(3, 3)]);
        assert!(!log.maybe_commit(3, 2).unwrap());
        assert!(log.maybe_commit(2, 2).unwrap());
        assert_eq!(log.commit_info().unwrap(), (2, 2));
    }
    #[test]
    fn what_is_given_to_apply_is_committed_and_durable() {
        let mut store = Memory::default();
        store.install(snapshot(3, 1, &[1]));
        let mut log = Log::new(store, 1024).unwrap();
        assert_eq!((log.committed, log.applied, log.persisted), (3, 3, 3));
        log.append(&[entry(4, 1), entry(5, 1), entry(6, 1)])
            .unwrap();
        log.maybe_commit(5, 1).unwrap();
        // Committed and not durable: nothing to apply.
        assert!(!log.has_next_entries_since(3).unwrap());
        assert!(log.next_entries_since(3, u64::MAX).unwrap().is_empty());
        log.store.append(&[entry(4, 1), entry(5, 1), entry(6, 1)]);
        log.stable_entries(6, 1).unwrap();
        assert!(log.maybe_persist(6, 1));
        assert!(log.has_next_entries_since(3).unwrap());
        assert_eq!(
            indexes(&log.next_entries_since(3, u64::MAX).unwrap()),
            vec![(4, 1), (5, 1)]
        );
        assert_eq!(
            indexes(&log.next_entries_since(4, u64::MAX).unwrap()),
            vec![(5, 1)]
        );
        assert!(!log.has_next_entries_since(5).unwrap());
        log.applied_to(5).unwrap();
        assert!(log.applied_to(6).is_err() && log.applied_to(4).is_err());
    }
    #[test]
    fn what_is_durable_is_counted_only_while_it_is_what_the_log_holds() {
        let mut store = Memory::default();
        store.append(&[entry(1, 1), entry(2, 1)]);
        let mut log = Log::new(store, 1024).unwrap();
        assert_eq!(log.persisted, 2);
        log.append(&[entry(3, 1), entry(4, 1)]).unwrap();
        // Storage wrote them; the core is not told yet.
        log.store.append(&[entry(3, 1), entry(4, 1)]);
        // Another leader's entries replace them meanwhile.
        assert_eq!(
            log.maybe_append(2, 1, 2, &[entry(3, 2), entry(4, 2)])
                .unwrap(),
            Some((3, 4))
        );
        assert_eq!(log.unstable.offset, 3);
        // The write that finished was of entries the log no longer holds.
        assert!(!log.maybe_persist(4, 1));
        assert_eq!(log.persisted, 2);
        log.store.append(&[entry(3, 2), entry(4, 2)]);
        log.stable_entries(4, 2).unwrap();
        assert!(log.maybe_persist(4, 2));
        assert!(!log.maybe_persist(4, 2));
        // Replacing a durable entry lowers what is durable.
        log.maybe_append(3, 2, 2, &[entry(4, 3)]).unwrap();
        assert_eq!(log.persisted, 3);
        assert!(log.stable_entries(4, 2).is_err());
    }
    #[test]
    fn terms_are_read_across_the_snapshot_storage_and_what_is_not_durable() {
        let mut store = Memory::default();
        store.install(snapshot(5, 2, &[1]));
        store.append(&[entry(6, 3)]);
        let mut log = Log::new(store, 1024).unwrap();
        log.append(&[entry(7, 4)]).unwrap();
        for (index, term) in [(3, 0), (4, 0), (5, 2), (6, 3), (7, 4), (8, 0)] {
            assert_eq!(log.term(index).unwrap(), term, "{index}");
        }
        assert_eq!(log.first_index().unwrap(), 6);
        assert_eq!(log.last_term().unwrap(), 4);
        assert_eq!(
            log.slice(5, 6, u64::MAX),
            Err(Error::Storage(StorageError::Compacted))
        );
        assert!(matches!(
            log.slice(6, 9, u64::MAX),
            Err(Error::Invariant(_))
        ));
        assert!(matches!(
            log.slice(7, 6, u64::MAX),
            Err(Error::Invariant(_))
        ));
        assert_eq!(
            indexes(&log.slice(6, 8, u64::MAX).unwrap()),
            vec![(6, 3), (7, 4)]
        );
        assert!(log.slice(7, 7, u64::MAX).unwrap().is_empty());
        // A snapshot that is not durable yet answers for its index.
        log.restore(snapshot(9, 5, &[1])).unwrap();
        assert_eq!(
            (
                log.committed,
                log.first_index().unwrap(),
                log.last_index().unwrap()
            ),
            (9, 10, 9)
        );
        assert_eq!(log.term(9).unwrap(), 5);
        assert_eq!(log.term(8).unwrap(), 0);
        assert!(log.restore(snapshot(8, 5, &[1])).is_err());
        assert!(log.stable_entries(9, 5).is_err());
        assert!(log.stable_snapshot(8).is_err());
        log.stable_snapshot(9).unwrap();
        assert!(log.maybe_persist_snapshot(9).is_ok_and(|_| true));
    }
    #[test]
    fn a_slice_is_cut_at_the_bytes_asked_for_and_never_to_nothing() {
        let payload = |index| Entry {
            index,
            term: 1,
            data: vec![7; 100],
            ..Entry::default()
        };
        let mut store = Memory::default();
        store.append(&[payload(1), payload(2)]);
        let mut log = Log::new(store, 1024).unwrap();
        log.append(&[payload(3), payload(4)]).unwrap();
        let one = proto::encoded_bytes(&payload(1));
        for (max, count) in [
            (0, 1),
            (one, 1),
            (2 * one - 1, 1),
            (2 * one, 2),
            (3 * one, 3),
            (u64::MAX, 4),
        ] {
            assert_eq!(log.slice(1, 5, max).unwrap().len(), count, "{max}");
        }
        assert_eq!(log.entries(3, one, usize::MAX).unwrap().len(), 1);
        assert!(log.entries(5, one, usize::MAX).unwrap().is_empty());
        // An entry bound cuts the same prefix as the bytes would.
        assert_eq!(log.entries(1, u64::MAX, 3).unwrap().len(), 3);
        assert_eq!(log.entries(1, 2 * one, 1).unwrap().len(), 1);
        assert_eq!(log.entries(1, 2 * one, 3).unwrap().len(), 2);
        // A question about the entries copies none of them.
        let mut visited = Vec::new();
        assert!(
            !log.any_entry(1, 5, |entry| {
                visited.push(entry.index);
                false
            })
            .unwrap()
        );
        assert_eq!(visited, vec![1, 2, 3, 4]);
        visited.clear();
        assert!(
            log.any_entry(1, 5, |entry| {
                visited.push(entry.index);
                entry.index == 3
            })
            .unwrap()
        );
        assert_eq!(visited, vec![1, 2, 3]);
        assert!(!log.any_entry(3, 3, |_| true).unwrap());
        assert!(matches!(
            log.any_entry(1, 6, |_| true),
            Err(Error::Invariant(_))
        ));
    }
    /// A page is sized before it is copied: across the stable/unstable
    /// boundary, at every budget, the copy's capacity is its length and
    /// its entries are what copying everything and then cutting gives.
    #[test]
    fn a_page_is_chosen_before_it_is_copied_and_holds_no_spare_room() {
        let payload = |index| Entry {
            index,
            term: 1,
            data: vec![7; 4096],
            ..Entry::default()
        };
        let one = proto::encoded_bytes(&payload(1));
        let (stable, unstable) = (6u64, 6u64);
        let boundary = stable + 1;
        let last = stable + unstable;
        let mut store = Memory::default();
        store.append(&(1..=stable).map(payload).collect::<Vec<_>>());
        let mut log = Log::new(store, 1024).unwrap();
        log.append(&(boundary..=last).map(payload).collect::<Vec<_>>())
            .unwrap();
        // The reference: everything, then the cut.
        let reference = |low: u64, high: u64, max: u64| {
            let mut all: Vec<Entry> = (low..high).map(payload).collect();
            limit_bytes(&mut all, max);
            indexes(&all)
        };
        for low in [1, boundary - 1, boundary, boundary + 1, last] {
            for max in [
                0,
                1,
                one - 1,
                one,
                2 * one - 1,
                2 * one,
                4 * one + 7,
                u64::MAX,
            ] {
                let page = log.slice(low, last + 1, max).unwrap();
                assert_eq!(indexes(&page), reference(low, last + 1, max), "{low} {max}");
                assert_eq!(page.capacity(), page.len(), "{low} {max}");
                for entry in &page {
                    assert_eq!(entry.data.capacity(), entry.data.len());
                }
            }
        }
        // An oversized first entry is taken alone, stable or not.
        let huge = |index| Entry {
            index,
            term: 1,
            data: vec![9; 64 * 1024],
            ..Entry::default()
        };
        let mut store = Memory::default();
        store.append(&[huge(1), payload(2)]);
        let mut log = Log::new(store, 1024).unwrap();
        log.append(&[huge(3), payload(4)]).unwrap();
        for (low, expected) in [(1, vec![(1, 1)]), (3, vec![(3, 1)])] {
            let page = log.slice(low, 5, one).unwrap();
            assert_eq!(indexes(&page), expected);
            assert_eq!(page.capacity(), 1);
        }
        // What follows an oversized stable entry is left for the next page.
        assert_eq!(indexes(&log.slice(2, 5, one).unwrap()), vec![(2, 1)]);
        assert_eq!(indexes(&log.slice(2, 5, 2 * one).unwrap()), vec![(2, 1)]);
    }
    #[test]
    fn a_rejection_names_where_the_logs_may_still_agree() {
        let log = log_of(&[
            entry(1, 1),
            entry(2, 3),
            entry(3, 3),
            entry(4, 3),
            entry(5, 5),
            entry(6, 5),
        ]);
        for (index, term, expected) in [
            (6, 5, (6, Some(5))),
            (6, 4, (4, Some(3))),
            (6, 2, (1, Some(1))),
            (6, 0, (0, Some(0))),
            (3, 3, (3, Some(3))),
            (7, 5, (7, None)),
        ] {
            assert_eq!(log.find_conflict_by_term(index, term).unwrap(), expected);
        }
    }
    #[test]
    fn what_is_not_durable_has_a_bound() {
        let mut log = Log::new(Memory::default(), 3).unwrap();
        log.append(&[entry(1, 1), entry(2, 1)]).unwrap();
        assert_eq!(
            log.append(&[entry(3, 1), entry(4, 1)]),
            Err(Error::Capacity("entries not yet durable"))
        );
        assert_eq!(log.last_index().unwrap(), 2);
        // Replacing does not count what it replaces.
        log.append(&[entry(2, 2), entry(3, 2)]).unwrap();
        assert_eq!(
            indexes(log.unstable.entries()),
            vec![(1, 1), (2, 2), (3, 2)]
        );
        assert_eq!(log.unstable.bytes, 36);
        assert!(log.unstable.resident_bytes() > 0);
        assert_eq!(log.unstable.payload, 0);
        log.unstable.check().unwrap();
        // The bound is full: what is not yet durable admits nothing more
        // until it is written, and then the counters start from nothing.
        let fourth = Entry {
            index: 4,
            term: 2,
            data: vec![1; 100],
            ..Entry::default()
        };
        assert_eq!(
            log.append(std::slice::from_ref(&fourth)),
            Err(Error::Capacity("entries not yet durable"))
        );
        log.store.append(log.unstable.entries());
        log.stable_entries(3, 2).unwrap();
        assert_eq!(log.unstable.bytes, 0);
        assert_eq!(log.unstable.payload, 0);
        log.append(std::slice::from_ref(&fourth)).unwrap();
        assert_eq!(log.unstable.payload, 100);
        assert_eq!(log.unstable.bytes, proto::approximate_bytes(&fourth));
        log.unstable.check().unwrap();
        log.unstable.payload = 1;
        assert!(log.unstable.check().is_err());
    }
}
