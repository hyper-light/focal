//! A store that makes each write durable before `submit` returns: slates' case, whose
//! publication to its anchor's memory is synchronous (`docs/durable.md` §11, X-1). Its depth is
//! one. Its answer waits for the replica's next drive, which never looks at a write in the drive
//! that submitted it; the waker is woken at once so the owner drives again.
//!
//! What it holds is the group's log in memory: a process that ends loses it, as slates' anchor
//! does on a power loss and not on a daemon's crash. It is also the store tests hold a group's
//! durable state in.
use std::collections::VecDeque;
use std::task::Waker;

use hyper_raft::StorageError;
use hyper_raft::proto::{Entry, HardState};

use crate::store::{EntryRef, Fault, Health, LogStore, Point, StoreView, Write};

/// A group's log held in memory, each write durable as it is submitted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RamStore {
    start: Point,
    /// The entries after the start, in order.
    entries: VecDeque<Entry>,
    hard_state: HardState,
    proposals: Vec<Entry>,
    /// Answers not yet taken: one at most, the depth.
    answers: VecDeque<Result<(), Fault>>,
}

/// The one write a store of depth one holds out.
const DEPTH: usize = 1;

impl RamStore {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// The last index held, the start's when none is.
    fn last(&self) -> u64 {
        self.entries.back().map_or(self.start.index, |e| e.index)
    }

    /// Where `index` lies in `entries`.
    fn at(&self, index: u64) -> Option<usize> {
        let offset = index.checked_sub(self.start.index.checked_add(1)?)?;
        usize::try_from(offset)
            .ok()
            .filter(|&at| at < self.entries.len())
    }

    /// Applies `write` as a log applies an update: start, entries, hard state, proposals.
    fn apply(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        if let Some(start) = write.start {
            if start.index < self.start.index {
                return Err(Fault::Failed("the start moves back"));
            }
            let drop = start
                .index
                .saturating_sub(self.start.index)
                .min(u64::try_from(self.entries.len()).unwrap_or(u64::MAX));
            self.entries
                .drain(..usize::try_from(drop).unwrap_or(usize::MAX));
            self.start = start;
            if self.last() < start.index {
                self.entries.clear();
            }
        }
        if let Some(entries) = write.entries {
            if entries.first <= self.start.index || entries.first > self.last().saturating_add(1) {
                return Err(Fault::Failed(
                    "entries that leave a gap or precede the start",
                ));
            }
            let keep = entries
                .first
                .saturating_sub(self.start.index.saturating_add(1));
            self.entries
                .truncate(usize::try_from(keep).unwrap_or(usize::MAX));
            self.entries.extend(entries.entries.iter().cloned());
        }
        if let Some(hard) = write.hard_state {
            self.hard_state = hard;
        }
        let last = self.last();
        self.proposals.retain(|p| p.index > last);
        self.proposals
            .extend(write.proposals.iter().filter(|p| p.index > last).cloned());
        Ok(())
    }
}

impl LogStore for RamStore {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        DEPTH
    }

    fn view(&self) -> Result<StoreView, Fault> {
        Ok(StoreView {
            start: self.start,
            last: self.last(),
            hard_state: self.hard_state,
            health: Health::Whole,
        })
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        Ok((self.start, self.last()))
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        if index == self.start.index {
            return Ok(self.start.term);
        }
        if index < self.start.index {
            return Err(StorageError::Compacted);
        }
        self.at(index)
            .and_then(|at| self.entries.get(at))
            .map(|e| e.term)
            .ok_or(StorageError::Unavailable)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        let mut total = 0u64;
        self.visit(low, high, u64::MAX, &mut |entry| {
            total = total.saturating_add(entry.encoded_bytes());
            if total > max_bytes && entry.index > low {
                return true;
            }
            let mut copy = Entry::default();
            entry.copy_into(&mut copy);
            into.push(copy);
            false
        })
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        _page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        if low <= self.start.index {
            return Err(StorageError::Compacted);
        }
        if high > self.last().saturating_add(1) {
            return Err(StorageError::Unavailable);
        }
        let Some(from) = self.at(low) else {
            return Ok(());
        };
        let count = usize::try_from(high.saturating_sub(low)).unwrap_or(usize::MAX);
        for entry in self.entries.iter().skip(from).take(count) {
            if visit(EntryRef::of(entry)) {
                break;
            }
        }
        Ok(())
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        into.extend(self.proposals.iter().cloned());
        Ok(())
    }

    fn room(&self) -> bool {
        self.answers.len() < DEPTH
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        if !self.room() {
            return Err(Fault::Room("a write is out"));
        }
        let answer = self.apply(write);
        self.answers.push_back(answer);
        waker.wake_by_ref();
        Ok(())
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        self.answers.pop_front()
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        self.apply(write)
    }
}
