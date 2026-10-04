//! What a member keeps of a leader's appends that arrive ahead of a hole in its log (mantle note
//! 32 R17; slates `docs/wip/research/consensus-enhancements.md` §3.5).
//!
//! A leader that sends ahead of its answers (Ongaro's thesis §10.2.2) loses one append, or a path
//! delivers it late, and the ones after it arrive past the end of the member's log. Raft's
//! consistency check refuses each (§5.3), and a member that forgets them makes its leader send them
//! all again once the lost one is sent again: one loss costs a window. ParallelRaft lets a follower
//! acknowledge entries out of order (Cao et al., *PolarFS*, VLDB 2018 §5); Gu et al. show that
//! executing them out of order breaks consistency and correct it by keeping order where it matters
//! (*Raft with out-of-order executions*, IJSI 2021, ParallelRaft-CE). slates' prefix model took the
//! part that is safe and measured: a member keeps what arrived ahead of a hole within one leader's
//! term, acknowledges it in order once the hole is filled, and commits and applies in order;
//! commitment out of order lost a committed entry in twelve steps and was rejected (note 32 R18).
//!
//! What is kept is the leader of this term's own log: a leader never rewrites its log within its
//! term, so the entry it sent for an index is its entry there whenever it is taken. Once an append
//! of the same term places the leader's entries through `last` (the consistency check holds there),
//! the kept entries from `last + 1` continue them exactly as the leader's next append would, and are
//! taken into the log as one (`Raft::take_ahead`). A refusal acknowledges nothing, so nothing kept
//! is ever acknowledged before it is in the log; what acknowledges it is the answer to the append
//! that filled the hole, which leaves only once the write that holds it is durable (`docs/durable.md`
//! I2). So what is kept is never written on its own, and a member that restarts has lost only
//! resends.
//!
//! A change of term or role, or a snapshot, forgets everything kept: it was one leader's, in one
//! term.
use crate::{
    error::{Error, Result},
    proto::Entry,
};

/// Entries kept ahead of a hole, in order of index, each at most once, of one term.
#[derive(Debug, Default)]
pub(crate) struct Early {
    /// The term they were sent in.
    term: u64,
    entries: Vec<Entry>,
}

impl Early {
    /// Whether nothing is kept.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// The kept entries' indexes, in order.
    pub(crate) fn indexes(&self) -> impl Iterator<Item = u64> + '_ {
        self.entries.iter().map(|entry| entry.index)
    }
    /// Forgets everything kept.
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
    /// Keeps `entries`, sent in `term` after `index`, where the log ends at `last` before `index`:
    /// they continue `index` one by one, or the message is refused as a violation and nothing is
    /// kept. What the log already holds is not kept; an entry kept already is kept once. Of what is
    /// kept, the `bound` entries nearest the hole stay: those are what the hole's filling takes
    /// first.
    pub(crate) fn keep(
        &mut self,
        term: u64,
        index: u64,
        last: u64,
        mut entries: Vec<Entry>,
        bound: usize,
    ) -> Result<()> {
        let mut expected = index;
        for entry in &entries {
            expected = expected
                .checked_add(1)
                .ok_or(Error::Violation("entries out of order"))?;
            if entry.index != expected || entry.term > term {
                return Err(Error::Violation("entries out of order"));
            }
        }
        if term != self.term {
            self.clear();
            self.term = term;
        }
        entries.retain(|entry| entry.index > last);
        if entries.is_empty() {
            return Ok(());
        }
        let first = entries.first().map_or(0, |entry| entry.index);
        let after = self.entries.last().map_or(0, |entry| entry.index);
        if first > after {
            // In order, as a window's appends arrive: they go after what is kept.
            let room = bound.saturating_sub(self.entries.len());
            entries.truncate(room);
            self.entries
                .try_reserve(entries.len())
                .map_err(|_| Error::Memory)?;
            self.entries.extend(entries);
        } else {
            self.merge(entries, bound)?;
        }
        Ok(())
    }
    /// Merges a run that arrived out of order with what is kept: one entry an index, the `bound`
    /// nearest the hole.
    fn merge(&mut self, entries: Vec<Entry>, bound: usize) -> Result<()> {
        let mut merged = Vec::new();
        merged
            .try_reserve_exact(self.entries.len().saturating_add(entries.len()).min(bound))
            .map_err(|_| Error::Memory)?;
        let mut kept = std::mem::take(&mut self.entries).into_iter().peekable();
        let mut arrived = entries.into_iter().peekable();
        while merged.len() < bound {
            let take_kept = match (kept.peek(), arrived.peek()) {
                (Some(old), Some(new)) => old.index <= new.index,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let entry = if take_kept {
                kept.next()
            } else {
                arrived.next()
            };
            let Some(entry) = entry else {
                break;
            };
            if merged
                .last()
                .is_some_and(|last: &Entry| last.index == entry.index)
            {
                continue;
            }
            merged.push(entry);
        }
        self.entries = merged;
        Ok(())
    }
    /// The kept entries that continue a log ending at `last`, taken out: the run from `last + 1`
    /// while their indexes follow one another, `room` at most. What lies at or below `last` is
    /// forgotten, for the log holds the leader's entries there now.
    pub(crate) fn take_after(&mut self, last: u64, room: usize) -> Vec<Entry> {
        let below = self.entries.partition_point(|entry| entry.index <= last);
        self.entries.drain(..below);
        let mut next = last;
        let run = self
            .entries
            .iter()
            .take(room)
            .take_while(|entry| {
                let follows = Some(entry.index) == next.checked_add(1);
                next = entry.index;
                follows
            })
            .count();
        if run == 0 {
            return Vec::new();
        }
        if run == self.entries.len() {
            std::mem::take(&mut self.entries)
        } else {
            self.entries.drain(..run).collect()
        }
    }
    /// The bytes held, by capacity.
    pub(crate) fn resident_bytes(&self) -> usize {
        self.entries.iter().fold(
            self.entries
                .capacity()
                .saturating_mul(std::mem::size_of::<Entry>()),
            |bytes, entry| {
                bytes
                    .saturating_add(entry.data.capacity())
                    .saturating_add(entry.context.capacity())
            },
        )
    }
    /// Whether the entries are in order, each once.
    pub(crate) fn check(&self) -> Result<()> {
        if self
            .entries
            .windows(2)
            .any(|pair| matches!(pair, [low, high] if low.index >= high.index))
        {
            return Err(Error::Invariant(
                "what is kept ahead of a hole is out of order",
            ));
        }
        Ok(())
    }
}
