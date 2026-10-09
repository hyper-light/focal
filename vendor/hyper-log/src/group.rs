//! One group's handle on the log: its writer, and the reader of its own state.
//!
//! A Raft group's replica is the only writer of its group's records (mantle
//! docs/design/raft-log.md §3), and the core reads its storage only between its own writes:
//! hyper-raft's `RawNode` takes no call while a `Ready` is out, and what a `Ready` gives to
//! persist the core reads back only once it has advanced past it (`node.rs`, `operate`). So every
//! read the core makes answers from the group's state as its own answered writes left it, and
//! nothing else moves that state: a sweep moves where records lie, not what they say.
//!
//! [`GroupLog`] holds that state with the replica, on the replica's thread: the group's start,
//! its last entry, the terms of every entry it retains as runs of one term (terms never fall
//! along a log, so a log of many entries has few runs), its hard state and marks, and the bytes
//! of its recent entries within the log's `group_cache`, by the rule the log keeps its own cache
//! by. Each write's answer brings the entries' bytes back, the log having written them, and the
//! handle applies the write as the log did; a refusal changes nothing, as the log refuses an
//! update whole. A read of anything the handle holds is answered where the replica is, with no
//! message to the log's owner; only entries older than the cache, a view with proposals, and every
//! read after a failure the handle cannot account for go to the log. mantle's store kept the
//! bounds this way between its writes (mantle docs/measurements/2026-10-01-shared-log.md); the
//! handle keeps all of what the core reads.
//!
//! The log holds the group for its handle: a submission for it from anyone else is refused
//! [`LogError::Claimed`], since the handle could not see it.

use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::SyncSender;
use std::task::Waker;

use hyper_block::block::BlockFile;

use crate::format::{HardState, Start};
use crate::owner::{Message, Query};
use crate::room::GROUP_SUBMISSIONS;
use crate::state::resolves;
use crate::ticket::{self, Answer, Ticket, Waiting};
use crate::writer::{self, Submission};
use crate::{Class, Entries, Entry, Fetched, LogError, Marks, Params, Proposal, Update, View};

/// Writes a handle keeps out at most: the group's own room in the log's queue and the waiters
/// it may have beyond it (`room.rs`), past which the log would refuse it `Busy` too.
const OUT: usize = GROUP_SUBMISSIONS.saturating_mul(2);

/// A group's state as its answered writes left it.
#[derive(Debug, Clone)]
pub(crate) struct Mirror {
    /// Whether the log holds the group.
    pub(crate) exists: bool,
    pub(crate) start: Start,
    pub(crate) last: u64,
    /// The terms of the entries after the start through the last: each run's first index and
    /// term, oldest first.
    pub(crate) runs: VecDeque<(u64, u64)>,
    /// The bytes of the entries from `cache_from` through the last, oldest first.
    pub(crate) cache: VecDeque<Vec<u8>>,
    pub(crate) cache_from: u64,
    /// Bytes in `cache`.
    pub(crate) cached: u64,
    pub(crate) hard: Option<HardState>,
    /// The proposals no update released: index and term.
    pub(crate) proposals: BTreeMap<u64, u64>,
    pub(crate) uncertain: Option<Start>,
    /// The greatest index an update released proposals through.
    pub(crate) released: u64,
}

impl Mirror {
    /// A group the log does not hold, as a new group starts.
    pub(crate) fn none() -> Self {
        Self {
            exists: false,
            start: Start::default(),
            last: 0,
            runs: VecDeque::new(),
            cache: VecDeque::new(),
            cache_from: 1,
            cached: 0,
            hard: None,
            proposals: BTreeMap::new(),
            uncertain: None,
            released: 0,
        }
    }

    /// A group as the log holds it, taking the bytes of the entries the log keeps in memory.
    pub(crate) fn of(g: &mut crate::state::Group) -> Self {
        let last = g.last().unwrap_or(g.start.index);
        let mut runs = VecDeque::new();
        let mut cache = VecDeque::new();
        let mut cached = 0u64;
        let mut cache_from = last.saturating_add(1);
        for (index, slot) in (g.start.index.saturating_add(1)..).zip(g.entries.iter_mut()) {
            if runs.back().is_none_or(|&(_, t)| t != slot.term) {
                runs.push_back((index, slot.term));
            }
            match slot.cached.take() {
                Some(bytes) => {
                    if cache.is_empty() {
                        cache_from = index;
                    }
                    cached = cached.saturating_add(len(&bytes));
                    cache.push_back(bytes);
                }
                // The log's cache is the suffix from its `cache_from`: an entry without bytes
                // after one with them is never kept.
                None => {
                    cache.clear();
                    cached = 0;
                    cache_from = index.saturating_add(1);
                }
            }
        }
        g.cached = 0;
        g.cache_from = last.saturating_add(1);
        Self {
            exists: true,
            start: g.start,
            last,
            runs,
            cache,
            cache_from,
            cached,
            hard: g.hard.map(|(h, _)| h),
            proposals: g.proposals.iter().map(|(&i, p)| (i, p.term)).collect(),
            uncertain: g.uncertain.map(|(mark, _)| mark),
            released: g.released.map_or(0, |(through, _)| through),
        }
    }

    /// The term of the last entry, or the start's.
    fn last_term(&self) -> u64 {
        self.runs.back().map_or(self.start.term, |&(_, t)| t)
    }

    /// The term of `index`, which the group holds, or of its start.
    fn term_of(&self, index: u64) -> Option<u64> {
        if index == self.start.index {
            return Some(self.start.term);
        }
        if index <= self.start.index || index > self.last {
            return None;
        }
        let at = self.runs.partition_point(|&(first, _)| first <= index);
        self.runs.get(at.checked_sub(1)?).map(|&(_, t)| t)
    }

    /// A new start drops the entries through it.
    fn apply_start(&mut self, start: Start) {
        let first = start.index.saturating_add(1);
        while self.cache_from < first && !self.cache.is_empty() {
            if let Some(bytes) = self.cache.pop_front() {
                self.cached = self.cached.saturating_sub(len(&bytes));
            }
            self.cache_from = self.cache_from.saturating_add(1);
        }
        self.last = self.last.max(start.index);
        self.cache_from = self.cache_from.max(first);
        while self.runs.get(1).is_some_and(|&(next, _)| next <= first) {
            self.runs.pop_front();
        }
        if let Some(run) = self.runs.front_mut() {
            run.0 = run.0.max(first);
        }
        if first > self.last {
            self.runs.clear();
        }
        self.start = start;
    }

    /// Entries replace the suffix from `first`; their bytes are kept while recent.
    fn apply_entries(&mut self, first: u64, entries: Vec<Entry>, budget: u64) {
        let end = first.saturating_sub(1);
        while self.last > end && self.last >= self.cache_from && !self.cache.is_empty() {
            if let Some(bytes) = self.cache.pop_back() {
                self.cached = self.cached.saturating_sub(len(&bytes));
            }
            self.last = self.last.saturating_sub(1);
        }
        self.last = self.last.min(end);
        while self.runs.back().is_some_and(|&(run, _)| run > end) {
            self.runs.pop_back();
        }
        if self.cache.is_empty() {
            self.cache_from = first;
        }
        for entry in entries {
            self.last = self.last.saturating_add(1);
            if self.runs.back().is_none_or(|&(_, t)| t != entry.term) {
                self.runs.push_back((self.last, entry.term));
            }
            self.cached = self.cached.saturating_add(len(&entry.bytes));
            self.cache.push_back(entry.bytes);
        }
        self.cache_from = self.cache_from.max(self.start.index.saturating_add(1));
        while self.cached > budget {
            let Some(bytes) = self.cache.pop_front() else {
                break;
            };
            self.cached = self.cached.saturating_sub(len(&bytes));
            self.cache_from = self.cache_from.saturating_add(1);
        }
    }

    /// What the log has reached is no longer uncertain (`writer::reach`); proposals outlive
    /// it until a release.
    fn reach(&mut self) {
        let last_term = self.last_term();
        if self
            .uncertain
            .is_some_and(|mark| resolves(mark, self.last, last_term))
        {
            self.uncertain = None;
        }
    }

    /// An answered write, applied as the log applied it (`writer::apply`).
    fn apply(&mut self, write: Written, entries: Vec<Entry>, budget: u64) {
        if write.remove {
            *self = Self::none();
            return;
        }
        self.exists = true;
        if let Some(start) = write.start {
            self.apply_start(start);
        }
        if let Some(first) = write.first {
            self.apply_entries(first, entries, budget);
        }
        self.reach();
        if let Some(hard) = write.hard {
            self.hard = Some(hard);
        }
        if let Some(through) = write.released {
            self.proposals = match through.checked_add(1) {
                Some(after) => self.proposals.split_off(&after),
                None => BTreeMap::new(),
            };
            self.released = self.released.max(through);
        }
        for (index, term) in write.proposals {
            self.proposals.insert(index, term);
        }
    }
}

fn len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

/// What a write changes, kept until its answer: everything but its entries, which the answer
/// brings back.
struct Written {
    start: Option<Start>,
    first: Option<u64>,
    hard: Option<HardState>,
    proposals: Vec<(u64, u64)>,
    released: Option<u64>,
    remove: bool,
}

/// A write out: its answer to come, and what it changes.
struct Out {
    waiting: Waiting,
    write: Written,
}

/// One group's writer and reader (module docs). It answers reads from the group's state as
/// its answered writes left it: a read while a write is out answers as of the writes answered
/// before it.
pub struct GroupLog<F: BlockFile + 'static> {
    inbox: SyncSender<Message<F>>,
    p: Params,
    group: u128,
    mirror: Mirror,
    /// Writes out, oldest first: at most [`OUT`].
    out: VecDeque<Out>,
    /// A write was answered with a failure the handle cannot account for: reads go to the log.
    stale: bool,
    /// Reads the handle could not answer and asked the log's owner for.
    asked: std::cell::Cell<u64>,
    /// Refusals the handle has taken: each write carries it, and the log refuses a write sent
    /// before the handle took the refusal of an earlier one (`LogError::Behind`).
    epoch: u64,
}

impl<F: BlockFile + 'static> std::fmt::Debug for GroupLog<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupLog")
            .field("group", &self.group)
            .field("out", &self.out.len())
            .field("stale", &self.stale)
            .finish_non_exhaustive()
    }
}

impl<F: BlockFile + 'static> GroupLog<F> {
    pub(crate) fn new(
        inbox: SyncSender<Message<F>>,
        p: Params,
        group: u128,
        mirror: Mirror,
    ) -> Self {
        Self {
            inbox,
            p,
            group,
            mirror,
            out: VecDeque::new(),
            stale: false,
            asked: std::cell::Cell::new(0),
            epoch: 0,
        }
    }

    /// Reads this handle has asked the log's owner for, those it could not answer itself:
    /// entries older than its cache, views with proposals, and every read after a failure.
    pub fn asked(&self) -> u64 {
        self.asked.get()
    }

    /// The group.
    pub fn group(&self) -> u128 {
        self.group
    }

    /// The writes the group usefully keeps out at once, one in each of the log's pipeline frames
    /// (`PIPELINE_FRAMES`): a fourth could not be written before the third flush from now, and
    /// would add only waiting (hyper-raft docs/durable.md §6). A replica takes readies ahead of
    /// their persistence up to this many. The log admits a group's writes
    /// `GROUP_SUBMISSIONS` at a time; one past them waits in the log for its group's room, never
    /// refused, within the handle's own bound (`OUT`), which holds this many and a compaction's.
    pub fn depth(&self) -> usize {
        crate::PIPELINE_FRAMES
    }

    /// Whether the handle sends another write now: fewer than its bound are out (`OUT`), past
    /// which a submission is refused `Busy`.
    pub fn has_room(&self) -> bool {
        self.out.len() < OUT
    }

    /// Writes out and not yet taken back by [`GroupLog::wait`] or [`GroupLog::poll`].
    pub fn outstanding(&self) -> usize {
        self.out.len()
    }

    /// `update` in parts that each fit one frame, in the order they apply, exactly as
    /// `Log::parts` gives them for the group: a pure function of the update and the frame's
    /// room, which the handle holds, so it is answered here with no message to the log's owner.
    pub fn parts(&self, update: Update) -> Result<Vec<Update>, LogError> {
        crate::parts(self.p.frame_room, self.group, update, self.p.tag)
    }

    /// Submits `update`, which waits in the log for room rather than being refused, as
    /// `Log::submit_waiting` does; it returns once the update is on its way, and its answer is
    /// taken with [`GroupLog::wait`] or [`GroupLog::poll`], in the order submitted. `Busy` when
    /// the handle already has as many writes out as the log would take for one group.
    pub fn submit(&mut self, update: Update) -> Result<(), LogError> {
        self.send(update, None, false)
    }

    /// Submits `update` as [`GroupLog::submit`] does, and wakes `waker` once its answer has
    /// come.
    pub fn submit_waking(&mut self, update: Update, waker: Waker) -> Result<(), LogError> {
        self.send(update, Some(waker), false)
    }

    /// Submits `update` and waits until it is durable, or refused.
    pub fn write(&mut self, update: Update) -> Result<(), LogError> {
        // With nothing else out, the wait below is on this write's own ticket from the moment
        // it is sent, so its frame's I/O may be given to this thread to do.
        let waits = self.out.is_empty();
        self.send(update, None, waits)?;
        self.wait().unwrap_or(Err(LogError::Closed))
    }

    /// Waits for the oldest write out to be answered and takes its answer: `None` with none out.
    pub fn wait(&mut self) -> Option<Result<(), LogError>> {
        let out = self.out.pop_front()?;
        let answer = out.waiting.wait();
        Some(self.took(out.write, answer))
    }

    /// The oldest write's answer if it has come: `None` with none out or none come yet.
    pub fn poll(&mut self) -> Option<Result<(), LogError>> {
        let answer = self.out.front()?.waiting.poll()?;
        let out = self.out.pop_front()?;
        Some(self.took(out.write, answer))
    }

    fn send(&mut self, update: Update, waker: Option<Waker>, waits: bool) -> Result<(), LogError> {
        if self.out.len() >= OUT {
            return Err(LogError::Busy);
        }
        let len = writer::submission_len(self.group, &update, Marks::default(), self.p.tag)
            .ok_or(LogError::TooLarge(usize::MAX))?;
        if len > self.p.frame_room {
            return Err(LogError::TooLarge(len));
        }
        let bytes = writer::charge(len).ok_or(LogError::TooLarge(len))?;
        let write = Written {
            start: update.start,
            first: update.entries.as_ref().map(|e| e.first),
            hard: update.hard_state,
            proposals: update.proposals.iter().map(|p| (p.index, p.term)).collect(),
            released: update.released,
            remove: update.remove,
        };
        let (reply, answer) = ticket::port();
        let submission = Submission {
            group: self.group,
            update,
            marks: Marks::default(),
            bytes,
            class: Class::Normal,
            tags: writer::Tags::default(),
            ticket: Ticket::new(reply, waker),
            admit: false,
            handle: true,
            lens: (0, 0),
            waits,
            epoch: self.epoch,
            submitted: crate::stats::now(),
        };
        self.inbox
            .send(Message::Submit {
                submission,
                wait: true,
            })
            .map_err(|_| LogError::Closed)?;
        self.out.push_back(Out {
            waiting: Waiting::new(answer),
            write,
        });
        Ok(())
    }

    /// Applies an answered write: its entries came back with it. A refusal changes nothing; a
    /// failure leaves the handle reading from the log.
    fn took(&mut self, write: Written, answer: Result<Answer, LogError>) -> Result<(), LogError> {
        match answer {
            Ok(Answer::Durable(entries)) => {
                let budget = self.p.config.group_cache;
                self.mirror.apply(write, entries, budget);
                Ok(())
            }
            Ok(_) => {
                self.stale = true;
                Err(LogError::Closed)
            }
            Err(e) => {
                if refuses_whole(&e) {
                    // Writes sent from here on are of a new epoch: the log takes them, while it
                    // refuses those sent before this answer was taken.
                    self.epoch = self.epoch.saturating_add(1);
                } else {
                    self.stale = true;
                }
                Err(e)
            }
        }
    }

    /// Asks the log's owner, for what the handle does not hold.
    fn ask(&self, query: Query) -> Result<Answer, LogError> {
        self.asked.set(self.asked.get().saturating_add(1));
        let (reply, answer) = ticket::port();
        let waiting = Waiting::new(answer);
        self.inbox
            .send(Message::Query(query, Ticket::new(reply, None)))
            .map_err(|_| LogError::Closed)?;
        waiting.wait()
    }

    /// The group's state, as `Log::view` gives it; `None` for a group the log holds nothing of.
    pub fn view(&self) -> Result<Option<View>, LogError> {
        let m = &self.mirror;
        if self.stale || !m.proposals.is_empty() {
            return match self.ask(Query::View(self.group))? {
                Answer::View(view) => Ok(view),
                _ => Err(LogError::Closed),
            };
        }
        if !m.exists {
            return Ok(None);
        }
        Ok(Some(View {
            start: m.start,
            last: m.last,
            hard_state: m.hard,
            proposals: Vec::<Proposal>::new(),
            released: m.released,
            uncertain: m.uncertain,
        }))
    }

    /// Where the group's log starts, and its last entry: the start's index for both when it
    /// holds no entry, and zero for a group the log holds nothing of.
    pub fn bounds(&self) -> Result<(Start, u64), LogError> {
        if self.stale {
            return Ok(self
                .view()?
                .map_or((Start::default(), 0), |v| (v.start, v.last)));
        }
        Ok((self.mirror.start, self.mirror.last))
    }

    /// The term of `index`, which may be the start's, as `Log::term` gives it.
    pub fn term(&self, index: u64) -> Result<u64, LogError> {
        let group = self.group;
        if self.stale {
            return match self.ask(Query::Term(group, index))? {
                Answer::Term(term) => Ok(term),
                _ => Err(LogError::Closed),
            };
        }
        let m = &self.mirror;
        if !m.exists {
            return Err(LogError::Unavailable { group, index });
        }
        if index < m.start.index {
            return Err(LogError::Compacted {
                group,
                first: m.start.index,
            });
        }
        m.term_of(index)
            .ok_or(LogError::Unavailable { group, index })
    }

    /// The entries of `[low, high)` as `Log::fetch` gives them, copied into `into`: from the
    /// handle when it holds them, from the log otherwise.
    pub fn fetch(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        mut into: Fetched,
    ) -> Result<Fetched, LogError> {
        let group = self.group;
        let m = &self.mirror;
        if self.stale {
            return self.fetch_from_log(low, high, max_bytes, into);
        }
        if !m.exists {
            return Err(LogError::Unavailable { group, index: low });
        }
        let first = m.start.index.saturating_add(1);
        if low < first {
            return Err(LogError::Compacted { group, first });
        }
        // Entries older than the cache are read from the file, through the log.
        if low < m.cache_from && low <= m.last && low < high {
            return self.fetch_from_log(low, high, max_bytes, into);
        }
        into.clear();
        let mut total = 0u64;
        for index in low..high {
            let bytes = index
                .checked_sub(m.cache_from)
                .and_then(|at| usize::try_from(at).ok())
                .and_then(|at| m.cache.get(at))
                .filter(|_| index <= m.last)
                .ok_or(LogError::Unavailable { group, index })?;
            total = total.saturating_add(len(bytes));
            if !into.is_empty() && total > max_bytes {
                break;
            }
            let term = m.term_of(index).ok_or(LogError::Corrupt { group, index })?;
            into.push(term, bytes);
        }
        Ok(into)
    }

    fn fetch_from_log(
        &self,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: Fetched,
    ) -> Result<Fetched, LogError> {
        let query = Query::Entries {
            group: self.group,
            low,
            high,
            max_bytes,
            into,
        };
        match self.ask(query)? {
            Answer::Entries(f) => Ok(f),
            _ => Err(LogError::Closed),
        }
    }

    /// The entries of `[low, high)` as [`GroupLog::fetch`] gives them, copied out.
    pub fn entries(&self, low: u64, high: u64, max_bytes: u64) -> Result<Vec<Entry>, LogError> {
        let fetched = self.fetch(low, high, max_bytes, Fetched::new())?;
        Ok(fetched
            .iter()
            .map(|(term, bytes)| Entry {
                term,
                bytes: bytes.to_vec(),
            })
            .collect())
    }
}

impl<F: BlockFile + 'static> Drop for GroupLog<F> {
    fn drop(&mut self) {
        // The log keeps room in its inbox for each group's release.
        let _ = self.inbox.try_send(Message::Release(self.group));
    }
}

/// Whether the log refused the update whole, changing nothing.
fn refuses_whole(e: &LogError) -> bool {
    matches!(
        e,
        LogError::Busy
            | LogError::Full
            | LogError::TooLarge(_)
            | LogError::TooManyGroups(_)
            | LogError::Backlog(_)
            | LogError::Invalid { .. }
            | LogError::Damaged(_)
            | LogError::Claimed(_)
            | LogError::Behind(_)
    )
}

/// The update `Entries` of a write's answer: what the log gives back.
pub(crate) fn given_back(update: &mut Update) -> Vec<Entry> {
    update
        .entries
        .as_mut()
        .map(|e: &mut Entries| std::mem::take(&mut e.entries))
        .unwrap_or_default()
}
