//! hyper-log's group handle as a [`LogStore`]: the first store (`docs/durable.md` §8). One log
//! per device serves every group on it, so one flush commits every group's writes; the handle
//! reads the group's state where the replica is, as its answered writes left it.
//!
//! A write is laid out as one update of the log, entries encoded as mantle's store encodes them
//! (the entry's kind, its context's length, its context and its data), and cut into parts that
//! each fit a frame (`GroupLog::parts`): entries first and the hard state last, so a crash
//! between parts leaves only entries no one acknowledged (`docs/durable.md` §2.3). The write is
//! answered once every part is. Parts beyond the handle's bound wait here and go out as answers
//! free room. A part refused, and every part sent behind it, changes nothing; parts not yet sent
//! then are never sent.
use std::cell::Cell;
use std::collections::VecDeque;
use std::task::Waker;

use hyper_block::block::BlockFile;
use hyper_log::{Fetched, GroupLog, Log, LogError, Proposal, Start, Update};
use hyper_raft::StorageError;
use hyper_raft::proto::{Entry, EntryType, HardState};

use crate::store::{Entries, EntryRef, Fault, Health, LogStore, Point, StoreView, Write};

/// Bytes an entry's encoding adds to its data and context: its kind and its context's length,
/// as mantle's store writes them (mantle `crates/range/src/store.rs`, `ENTRY_OVERHEAD`).
pub const ENTRY_OVERHEAD: usize = 1 + 4;

/// What a write's parts came to.
#[derive(Debug)]
struct Parts {
    total: usize,
    sent: usize,
    answered: usize,
    outcome: Result<(), Fault>,
}

impl Parts {
    /// A part's answer: the first refusal or failure is the write's.
    fn answer(&mut self, answer: Result<(), Fault>) {
        self.answered = self.answered.saturating_add(1);
        if self.outcome.is_ok() {
            self.outcome = answer;
        }
    }
}

/// A group of a hyper-log log, written and read through its handle.
pub struct GroupStore<F: BlockFile + 'static> {
    group: GroupLog<F>,
    /// The most bytes one read of the log gives: a segment's.
    page: u64,
    /// The reservation entries are fetched into, kept between reads while it holds no more
    /// than a page, as hyper-log keeps its own (`Log::entries`).
    fetched: Cell<Option<Fetched>>,
    /// Writes not yet answered, oldest first.
    writes: VecDeque<Parts>,
    /// Parts not yet sent, in order.
    queued: VecDeque<Update>,
    waker: Option<Waker>,
}

impl<F: BlockFile + 'static> std::fmt::Debug for GroupStore<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupStore")
            .field("group", &self.group)
            .field("writes", &self.writes.len())
            .field("queued", &self.queued.len())
            .finish_non_exhaustive()
    }
}

/// Why a group was not claimed.
#[derive(Debug, thiserror::Error)]
pub enum ClaimError {
    /// The log found the group's acknowledged records damaged: the member is rebuilt under a new
    /// identity (`docs/durable.md` §5), and the group's records removed ([`GroupStore::remove`]).
    #[error("the group's acknowledged records are damaged")]
    Damaged,
    /// The log refused.
    #[error("the log: {0}")]
    Log(LogError),
}

impl<F: BlockFile + 'static> GroupStore<F> {
    /// Claims `group` of `log`: from here the group is written through this store alone.
    pub fn claim(log: &Log<F>, group: u128) -> Result<Self, ClaimError> {
        match log.group(group) {
            Ok(handle) => Ok(Self::new(handle, log.config().segment_bytes)),
            Err(LogError::Damaged(_)) => Err(ClaimError::Damaged),
            Err(error) => Err(ClaimError::Log(error)),
        }
    }

    /// Removes every record of `group` from `log`: a damaged group's, before a member under a new
    /// identity opens in its place.
    pub fn remove(log: &Log<F>, group: u128) -> Result<(), LogError> {
        log.write_waiting(
            group,
            Update {
                remove: true,
                ..Update::default()
            },
        )
    }

    /// A store over `group`'s handle, reading at most `page` bytes at once.
    pub fn new(group: GroupLog<F>, page: u64) -> Self {
        Self {
            group,
            page,
            fetched: Cell::new(None),
            writes: VecDeque::new(),
            queued: VecDeque::new(),
            waker: None,
        }
    }

    /// The handle.
    pub fn handle(&self) -> &GroupLog<F> {
        &self.group
    }

    fn fetch(&self, low: u64, high: u64, max_bytes: u64) -> Result<Fetched, StorageError> {
        let into = self.fetched.take().unwrap_or_default();
        self.group
            .fetch(low, high, max_bytes, into)
            .map_err(|e| storage(&e))
    }

    /// Keeps `fetched` for the next read when it held no more than a page.
    fn keep(&self, fetched: Fetched) {
        let held = fetched.iter().fold(0u64, |sum, (_, bytes)| {
            sum.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        });
        if held <= self.page {
            self.fetched.set(Some(fetched));
        }
    }

    /// Sends the parts waiting while the handle has room.
    fn send_queued(&mut self) {
        while self.group.has_room() {
            let Some(part) = self.queued.pop_front() else {
                return;
            };
            let Some(owner) = self.writes.iter_mut().find(|w| w.sent < w.total) else {
                return;
            };
            owner.sent = owner.sent.saturating_add(1);
            let sent = match &self.waker {
                Some(waker) => self.group.submit_waking(part, waker.clone()),
                None => self.group.submit(part),
            };
            if let Err(error) = sent {
                // Never sent: answered here, and nothing behind it goes.
                owner.answer(Err(fault(&error)));
                self.drop_queued();
                return;
            }
        }
    }

    /// Parts not yet sent behind a refused one are never sent: each is answered `Behind`.
    fn drop_queued(&mut self) {
        while self.queued.pop_front().is_some() {
            if let Some(owner) = self.writes.iter_mut().find(|w| w.sent < w.total) {
                owner.sent = owner.sent.saturating_add(1);
                owner.answer(Err(Fault::Behind));
            }
        }
    }
}

/// The log's error as the core reads it.
fn storage(e: &LogError) -> StorageError {
    match e {
        LogError::Compacted { .. } => StorageError::Compacted,
        LogError::Unavailable { .. } => StorageError::Unavailable,
        _ => StorageError::Other("the log failed"),
    }
}

/// The log's refusal or failure as a write's fault.
fn fault(e: &LogError) -> Fault {
    match e {
        LogError::Backlog(_) => Fault::Room("the group's retained bound"),
        LogError::Full => Fault::Room("the log's segments"),
        LogError::TooManyGroups(_) => Fault::Room("the log's groups"),
        LogError::Busy => Fault::Room("the log's queue"),
        LogError::Behind(_) => Fault::Behind,
        LogError::Fenced => Fault::Failed("the log is fenced"),
        LogError::Closed => Fault::Failed("the log is closed"),
        LogError::Disk(_) => Fault::Failed("the device failed"),
        LogError::TooLarge(_) => Fault::Failed("a record larger than a frame"),
        LogError::Damaged(_) => Fault::Failed("the group is damaged"),
        _ => Fault::Failed("the log refused a write the group cannot take"),
    }
}

/// An entry's bytes in the log: its kind, its context's length, its context and its data.
pub fn encode_entry(entry: &Entry) -> Option<Vec<u8>> {
    let context_len = u32::try_from(entry.context.len()).ok()?;
    let len = ENTRY_OVERHEAD
        .checked_add(entry.context.len())?
        .checked_add(entry.data.len())?;
    let mut out = Vec::with_capacity(len);
    out.push(entry.entry_type.byte());
    out.extend_from_slice(&context_len.to_le_bytes());
    out.extend_from_slice(&entry.context);
    out.extend_from_slice(&entry.data);
    Some(out)
}

/// The entry at `index` of `term` whose bytes are `bytes`, read where they lie.
pub fn decode_entry(index: u64, term: u64, bytes: &[u8]) -> Option<EntryRef<'_>> {
    let (&kind, rest) = bytes.split_first()?;
    let kind = EntryType::from_byte(kind)?;
    let (len, rest) = rest.split_at_checked(4)?;
    let len = usize::try_from(u32::from_le_bytes(len.try_into().ok()?)).ok()?;
    let (context, data) = rest.split_at_checked(len)?;
    Some(EntryRef {
        index,
        term,
        kind,
        context,
        data,
    })
}

fn hard_to_log(h: HardState) -> hyper_log::HardState {
    hyper_log::HardState {
        term: h.term,
        vote: h.vote,
        commit: h.commit,
    }
}

/// `write` as one update of the log: the one copy of what the core lent.
fn update_of(write: &Write<'_>) -> Result<Update, Fault> {
    let encoded = |e: &Entry| encode_entry(e).ok_or(Fault::Failed("an entry past what u32 counts"));
    let no_memory = |_| Fault::Failed("no memory for a write");
    // Each list is reserved at its exact length before it is filled: collected through a
    // `Result`, an iterator gives no lower bound, and the list grew by reallocation (traced at
    // 0.005 an entry on mantle's replica workload).
    let entries = match write.entries {
        Some(Entries { first, entries }) => {
            let mut encoded_entries = Vec::new();
            encoded_entries
                .try_reserve_exact(entries.len())
                .map_err(no_memory)?;
            for e in entries {
                encoded_entries.push(hyper_log::Entry {
                    term: e.term,
                    bytes: encoded(e)?,
                });
            }
            Some(hyper_log::Entries {
                first,
                entries: encoded_entries,
            })
        }
        None => None,
    };
    let mut proposals = Vec::new();
    proposals
        .try_reserve_exact(write.proposals.len())
        .map_err(no_memory)?;
    for p in write.proposals {
        proposals.push(Proposal {
            index: p.index,
            term: p.term,
            bytes: encoded(p)?,
        });
    }
    Ok(Update {
        start: write.start.map(|p| Start {
            index: p.index,
            term: p.term,
        }),
        entries,
        hard_state: write.hard_state.map(hard_to_log),
        proposals,
        remove: false,
    })
}

impl<F: BlockFile + 'static> LogStore for GroupStore<F> {
    type Hold = std::convert::Infallible;

    fn held(&self) -> Option<&Self::Hold> {
        None
    }

    fn release(&mut self, met: &Self::Hold) {
        match *met {}
    }

    fn depth(&self) -> usize {
        self.group.depth()
    }

    fn view(&self) -> Result<StoreView, Fault> {
        let Some(view) = self.group.view().map_err(|e| fault(&e))? else {
            return Ok(StoreView::default());
        };
        Ok(StoreView {
            start: Point {
                index: view.start.index,
                term: view.start.term,
            },
            last: view.last,
            hard_state: view
                .hard_state
                .map_or_else(HardState::default, |h| HardState {
                    term: h.term,
                    vote: h.vote,
                    commit: h.commit,
                }),
            health: view.uncertain.map_or(Health::Whole, |mark| {
                Health::Marked(Point {
                    index: mark.index,
                    term: mark.term,
                })
            }),
        })
    }

    fn bounds(&self) -> Result<(Point, u64), StorageError> {
        let (start, last) = self.group.bounds().map_err(|e| storage(&e))?;
        Ok((
            Point {
                index: start.index,
                term: start.term,
            },
            last,
        ))
    }

    fn term(&self, index: u64) -> Result<u64, StorageError> {
        match self.group.term(index) {
            Ok(term) => Ok(term),
            // A group the log holds nothing of starts after index 0, of term 0.
            Err(LogError::Unavailable { .. }) if index == 0 => Ok(0),
            Err(e) => Err(storage(&e)),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: &mut Vec<Entry>,
    ) -> Result<(), StorageError> {
        if low >= high {
            return Ok(());
        }
        let fetched = self.fetch(low, high, max_bytes)?;
        into.try_reserve_exact(fetched.len())
            .map_err(|_| StorageError::Other("no memory for the entries asked"))?;
        let decoded = (low..)
            .zip(fetched.iter())
            .try_for_each(|(index, (term, bytes))| {
                let entry = decode_entry(index, term, bytes)
                    .ok_or(StorageError::Other("an entry does not decode"))?;
                let mut copy = Entry::default();
                entry.copy_into(&mut copy);
                into.push(copy);
                Ok(())
            });
        self.keep(fetched);
        decoded
    }

    fn visit(
        &self,
        low: u64,
        high: u64,
        page: u64,
        visit: &mut dyn FnMut(EntryRef<'_>) -> bool,
    ) -> Result<(), StorageError> {
        let page = page.min(self.page);
        let mut next = low;
        while next < high {
            let fetched = self.fetch(next, high, page)?;
            if fetched.is_empty() {
                self.keep(fetched);
                return Err(StorageError::Unavailable);
            }
            let mut stopped = false;
            let mut failed = false;
            for (term, bytes) in fetched.iter() {
                let Some(entry) = decode_entry(next, term, bytes) else {
                    failed = true;
                    break;
                };
                if visit(entry) {
                    stopped = true;
                    break;
                }
                next = next.saturating_add(1);
            }
            self.keep(fetched);
            if failed {
                return Err(StorageError::Other("an entry does not decode"));
            }
            if stopped {
                return Ok(());
            }
        }
        Ok(())
    }

    fn proposals(&self, into: &mut Vec<Entry>) -> Result<(), StorageError> {
        let Some(view) = self.group.view().map_err(|e| storage(&e))? else {
            return Ok(());
        };
        for proposal in &view.proposals {
            let entry = decode_entry(proposal.index, proposal.term, &proposal.bytes)
                .ok_or(StorageError::Other("a proposal does not decode"))?;
            let mut copy = Entry::default();
            entry.copy_into(&mut copy);
            into.push(copy);
        }
        Ok(())
    }

    fn room(&self) -> bool {
        self.queued.is_empty() && self.group.has_room()
    }

    fn submit(&mut self, write: &Write<'_>, waker: &Waker) -> Result<(), Fault> {
        let update = update_of(write)?;
        let parts = self.group.parts(update).map_err(|e| fault(&e))?;
        self.writes.push_back(Parts {
            total: parts.len(),
            sent: 0,
            answered: 0,
            outcome: Ok(()),
        });
        self.queued.extend(parts);
        match &self.waker {
            Some(held) if held.will_wake(waker) => {}
            _ => self.waker = Some(waker.clone()),
        }
        self.send_queued();
        Ok(())
    }

    fn poll(&mut self) -> Option<Result<(), Fault>> {
        loop {
            let front = self.writes.front_mut()?;
            if front.answered >= front.total {
                return self.writes.pop_front().map(|parts| parts.outcome);
            }
            if front.answered >= front.sent {
                self.send_queued();
                return None;
            }
            let Some(answer) = self.group.poll() else {
                self.send_queued();
                return None;
            };
            let answer = answer.map_err(|e| fault(&e));
            let refused = answer.is_err();
            front.answer(answer);
            if refused {
                self.drop_queued();
            }
        }
    }

    fn write_now(&mut self, write: &Write<'_>) -> Result<(), Fault> {
        let update = update_of(write)?;
        let parts = self.group.parts(update).map_err(|e| fault(&e))?;
        for part in parts {
            self.group.write(part).map_err(|e| fault(&e))?;
        }
        Ok(())
    }
}
