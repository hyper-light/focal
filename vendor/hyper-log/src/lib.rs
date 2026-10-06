//! The shared Raft log (mantle docs/design/raft-log.md): one log per device, shared by every
//! group on it, so that one flush commits every group's writes.
//!
//! A replica submits its `Ready`'s entries, hard state, start and proposals as one [`Update`];
//! the writer gathers every update that arrived while the last flush ran into one frame, writes
//! and flushes it, publishes it to readers, and answers once a later durable record confirms the
//! flush. Reads serve the view a Raft core's storage needs: bounds, terms and entries.
//!
//! From mantle-log at mantle `147f035` (`ORIGIN.md`), with its state held by one owner instead of
//! a lock (mantle note 32 §3.9, L-2): callers reach the owner by message and hear back through
//! tickets, one to one (`ticket.rs`). The owner runs the writer as steps between messages on a
//! thread of its own and hands each I/O, with the device, to a caller of the frame that waits on
//! its answer, or else to the log's I/O thread, so the owner answers callers while a frame is
//! flushed, and a blocking write crosses from its caller to the owner and back, the caller doing
//! its frame's flush as mantle's writer did (`owner.rs`, `device.rs`). A log runs those two
//! threads, whatever its number of groups or callers. A group's replica reads its group through the group's handle ([`GroupLog`]), which
//! answers from the group's state as its writes left it, with no message to the owner
//! (`group.rs`).
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros,
        clippy::cognitive_complexity
    )
)]
pub mod codec;
mod device;
mod error;
pub mod format;
mod group;
mod owner;
mod recover;
mod room;
mod state;
mod stats;
mod ticket;
mod writer;

use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::task::Waker;
use std::thread::JoinHandle;

use hyper_block::block::BlockFile;
use hyper_block::buf::{Alignment, Pool};

pub use error::LogError;
pub use format::{HardState, Start};
pub use group::GroupLog;
pub use stats::LogStats;
pub use ticket::{Fetching, Pending};

use owner::{Message, Owner, Query};
use ticket::{Answer, Ticket, Waiting};
use writer::Submission;

/// The log's parameters. Each comes from the node: the segment size from the device's
/// geometry, the rest from the measured append rate and the replicas placed on the device
/// (mantle docs/design/raft-log.md §4–§5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Bytes of each segment: a multiple of the file's alignment, at least four blocks.
    pub segment_bytes: u64,
    /// Segments the file may grow to.
    pub max_segments: u32,
    /// Groups the log holds at most.
    pub max_groups: usize,
    /// Entries one group may retain.
    pub group_entries: u64,
    /// Payload bytes of entries one group may retain.
    pub group_bytes: u64,
    /// Payload bytes of recent entries one group keeps in memory.
    pub group_cache: u64,
    /// Submissions the queue admits [research/11 §4]. Their bytes are bounded by
    /// [`Log::queue_bytes`], which the frame's size gives.
    pub queue_submissions: usize,
    /// What the writer waits for between frames.
    pub waits: Waits,
}

/// What the writer waits for between frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Waits {
    /// Waits for the submitters the last batch answered while waiting is expected to lower
    /// total latency (mantle docs/design/raft-log.md §3). What a node runs.
    Measured,
    /// Never waits: a batch is what is queued when the writer looks. For a caller whose
    /// submitters never return within a wait, as the replica simulation's, which drives each
    /// member's submissions one at a time.
    Never,
}

/// Entries to write from `first` on, replacing any the group holds at or after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entries {
    /// The first entry's index.
    pub first: u64,
    /// The entries, from `first` on; none says only where the group's entries end.
    pub entries: Vec<Entry>,
}

/// One entry: its term and its bytes, which the log takes and keeps while the entry is recent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The entry's term.
    pub term: u64,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// An entry the replica approved by itself on the fast track (07 §1.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// Its index.
    pub index: u64,
    /// Its term.
    pub term: u64,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// What one replica makes durable at once: one `Ready`'s worth, applied in this order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Update {
    /// The log now starts after this: the engine made the entries before it durable, or a
    /// snapshot was installed there.
    pub start: Option<Start>,
    /// Entries from an index on, replacing any the group holds at or after it.
    pub entries: Option<Entries>,
    /// The hard state; the latest written wins.
    pub hard_state: Option<HardState>,
    /// Entries approved on the fast track, held until the log reaches them.
    pub proposals: Vec<Proposal>,
    /// The replica left this device: every record of the group is dead. Nothing else may
    /// come with it.
    pub remove: bool,
}

/// How urgently a submission is wanted durable, as Tectonic's TrafficClasses order a node's
/// work: latency-sensitive, normal and background [research/01 §1.11]. A frame with room for
/// only some of the waiting updates takes them by class, most urgent first, after those it
/// passed over before (mantle docs/design/raft-log.md §3).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Class {
    /// A replica whose callers wait on the write: a metadata range's ready.
    Latency,
    /// What a submission is unless it says otherwise.
    #[default]
    Normal,
    /// Work no caller waits on: a compaction's start, a rebuild's catch-up.
    Background,
}

/// A group's durable state, as a Raft core's storage reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    /// Where the group's log starts.
    pub start: Start,
    /// The last entry's index, the start's when none is held.
    pub last: u64,
    /// The group's hard state, if it wrote one.
    pub hard_state: Option<HardState>,
    /// Its proposals the log has not reached.
    pub proposals: Vec<Proposal>,
    /// Entries the log may lack, through this mark's index and of terms up to its term,
    /// that a frame no longer readable held (mantle docs/design/raft-log.md §6). Until the log
    /// again reaches the index, or holds an entry of a later term, the replica takes no part
    /// in elections: it may have acknowledged what it no longer holds.
    pub uncertain: Option<Start>,
}

/// What opening found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Frames replayed.
    pub frames: u64,
    /// Groups whose records do not run unbroken, or whose lost last frame held what its
    /// persist record cannot restore: the log serves none of them, and each recovers from
    /// its peers.
    pub damaged: Vec<u128>,
    /// Groups restored from the persist record of a last frame that no longer reads.
    pub restored: Vec<u128>,
}

/// Entries fetched: the caller's reservation, which the log fills with each entry's term and
/// bytes. Handed back with its answer, it keeps its capacity for the next fetch, so a caller that
/// fetches again with it allocates nothing once it has held the most it fetches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fetched {
    bytes: Vec<u8>,
    /// Each entry's term and where its bytes lie in `bytes`.
    entries: Vec<(u64, usize, usize)>,
}

impl Fetched {
    /// An empty reservation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Entries fetched.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entry was fetched.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The `i`th entry fetched: its term and its bytes.
    pub fn get(&self, i: usize) -> Option<(u64, &[u8])> {
        let &(term, start, len) = self.entries.get(i)?;
        let end = start.checked_add(len)?;
        Some((term, self.bytes.get(start..end)?))
    }

    /// Every entry fetched, in index order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &[u8])> + '_ {
        (0..self.entries.len()).filter_map(|i| self.get(i))
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.entries.clear();
    }

    /// Adds an entry held in memory.
    fn push(&mut self, term: u64, bytes: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        self.entries.push((term, start, bytes.len()));
    }

    /// Keeps a place for an entry to be read from the file: its position.
    fn reserve(&mut self, term: u64) -> usize {
        self.entries.push((term, 0, 0));
        self.entries.len().saturating_sub(1)
    }

    /// Fills the place `at` with an entry read from the file.
    fn fill(&mut self, at: usize, payload: &[u8]) {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(payload);
        if let Some(entry) = self.entries.get_mut(at) {
            entry.1 = start;
            entry.2 = payload.len();
        }
    }
}

thread_local! {
    /// The reservation [`Log::entries`] fetches into, kept for this thread's next call: one
    /// segment's bytes at most.
    static KEPT: std::cell::Cell<Option<Fetched>> = const { std::cell::Cell::new(None) };
}

/// What only the restore at open writes with an update (mantle docs/design/raft-log.md §6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Marks {
    /// The group's log may lack entries through this mark.
    pub(crate) uncertain: Option<Start>,
    /// The group's acknowledged records are damaged: the update is otherwise empty.
    pub(crate) damaged: bool,
}

/// What every part of the log knows once it opens, and never changes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Params {
    pub(crate) id: u128,
    pub(crate) config: Config,
    pub(crate) align: Alignment,
    /// Payload bytes one frame holds: no submission whose records take more is admitted.
    pub(crate) frame_room: usize,
    /// Charged bytes the queue holds at most (`PIPELINE_FRAMES`).
    pub(crate) queue_bytes: u64,
}

/// `update` for `group` in parts that each fit a frame of `room` payload bytes
/// (`Log::parts`, `GroupLog::parts`).
pub(crate) fn parts(room: usize, group: u128, update: Update) -> Result<Vec<Update>, LogError> {
    let len_of = |u: &Update| writer::update_len(group, u).ok_or(LogError::TooLarge(usize::MAX));
    if update.remove || len_of(&update)? <= room {
        return Ok(vec![update]);
    }
    let record =
        |r: &format::Record<'_>| format::encoded_len(r).ok_or(LogError::TooLarge(usize::MAX));
    let Update {
        start,
        entries,
        hard_state,
        proposals,
        ..
    } = update;
    let mut parts = Vec::new();
    let mut part = Update {
        start,
        ..Update::default()
    };
    let mut used = len_of(&part)?;
    if let Some(e) = entries {
        let none = record(&format::Record::Entries {
            group,
            first: e.first,
            entries: &[],
        })?;
        let mut first = e.first;
        let mut held: Vec<Entry> = Vec::new();
        let mut held_len = none;
        for (index, entry) in (e.first..).zip(e.entries) {
            let cost = record(&format::Record::Entries {
                group,
                first: index,
                entries: &[(entry.term, &entry.bytes)],
            })?
            .saturating_sub(none);
            if used.saturating_add(held_len).saturating_add(cost) > room {
                if held.is_empty() && used == 0 {
                    return Err(LogError::TooLarge(none.saturating_add(cost)));
                }
                if !held.is_empty() {
                    part.entries = Some(Entries {
                        first,
                        entries: std::mem::take(&mut held),
                    });
                }
                parts.push(std::mem::take(&mut part));
                used = 0;
                held_len = none;
                first = index;
                if none.saturating_add(cost) > room {
                    return Err(LogError::TooLarge(none.saturating_add(cost)));
                }
            }
            held.push(entry);
            held_len = held_len.saturating_add(cost);
        }
        // Entries of none still say where the group's entries end.
        part.entries = Some(Entries {
            first,
            entries: held,
        });
        used = used.saturating_add(held_len);
    }
    if let Some(state) = hard_state {
        let cost = record(&format::Record::HardState { group, state })?;
        if used.saturating_add(cost) > room {
            parts.push(std::mem::take(&mut part));
            used = 0;
        }
        part.hard_state = Some(state);
        used = used.saturating_add(cost);
    }
    for p in proposals {
        let cost = record(&format::Record::Proposal {
            group,
            index: p.index,
            term: p.term,
            bytes: &p.bytes,
        })?;
        if cost > room {
            return Err(LogError::TooLarge(cost));
        }
        if used.saturating_add(cost) > room {
            parts.push(std::mem::take(&mut part));
            used = 0;
        }
        part.proposals.push(p);
        used = used.saturating_add(cost);
    }
    parts.push(part);
    Ok(parts)
}

/// Payload bytes one frame holds: a segment less its header block and the frame's header.
pub(crate) fn frame_room(config: &Config, align: Alignment) -> Result<usize, LogError> {
    let block = u64::try_from(align.get()).map_err(|_| LogError::Config("block"))?;
    config
        .segment_bytes
        .checked_sub(block)
        .and_then(|room| usize::try_from(room).ok())
        .and_then(|room| room.checked_sub(format::FRAME_HEADER_LEN))
        .ok_or(LogError::Config("a segment holds no frame"))
}

/// Frames' worth of charged bytes the log holds for submissions not yet answered: the frame
/// flushed and awaiting the persist record that confirms it, the frame being written, whose
/// record does, and the frame gathering while that flush runs (mantle docs/design/raft-log.md
/// §3). With less, the next frame could gather only once a flush ended and would go out short;
/// bytes past a third frame cannot be written before the third flush from now, so by Little's
/// law they add only waiting [research/11 §4, §5.2]. A group's writes are useful in each of the
/// three frames alike, so it is also the most writes a group's handle keeps out at once
/// ([`GroupLog::depth`]; hyper-raft docs/durable.md §6).
pub(crate) const PIPELINE_FRAMES: usize = 3;

/// What starting a log needs besides its file and its state.
struct Prepared {
    p: Params,
    room: room::Room,
    /// The owner's first batch: the restore of a lost frame, if any.
    first: Vec<Submission>,
    /// The handles that answer the first batch.
    pending: Vec<Pending>,
}

/// A log the file could not be made into, and the file, given back where the log has it.
#[derive(Debug)]
pub struct Refused<F> {
    /// Why.
    pub error: LogError,
    /// The file, unless a thread of the log that held it ended without giving it back.
    pub file: Option<F>,
}

impl<F> Refused<F> {
    fn with(error: LogError, file: F) -> Self {
        Self {
            error,
            file: Some(file),
        }
    }
}

/// What a submitter hears, and how it waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Hears {
    /// Its admission, waited for as it submits, then its answer.
    Admission,
    /// Only its answer, which it waits for from the moment it submits: its frame's I/O may be
    /// given to it to do (`owner::Owner::start`).
    Waits,
    /// Only its answer, through its waker, the caller going on meanwhile.
    Answer,
}

/// The log: a handle to its owner, which any number of threads may share by reference.
pub struct Log<F: BlockFile + 'static> {
    inbox: SyncSender<Message<F>>,
    /// The log's two threads: the owner's, which gives back the file when the log closes, and
    /// the I/O thread.
    threads: [Option<JoinHandle<Option<F>>>; 2],
    p: Params,
}

impl<F: BlockFile + 'static> Log<F> {
    /// Formats a new log `id` in `file`, which must be empty.
    pub fn create(file: F, config: Config, id: u128) -> Result<Self, LogError> {
        Self::try_create(file, config, id).map_err(|r| r.error)
    }

    /// Formats a new log as [`Log::create`] does, giving the file back with a refusal.
    pub fn try_create(file: F, config: Config, id: u128) -> Result<Self, Refused<F>> {
        let state = match recover::create(&file, &config, id) {
            Ok(state) => state,
            Err(error) => return Err(Refused::with(error, file)),
        };
        let (log, _) = Self::start(file, config, id, state, Vec::new())?;
        Ok(log)
    }

    /// Opens log `id` in `file` and recovers it (mantle docs/design/raft-log.md §6). What a
    /// last frame that no longer reads held, as its persist record says, is written back before
    /// the log serves anyone, in one frame.
    pub fn open(file: F, config: Config, id: u128) -> Result<(Self, Recovery), LogError> {
        Self::try_open(file, config, id).map_err(|r| r.error)
    }

    /// Opens a log as [`Log::open`] does, giving the file back with a refusal: what recovery
    /// found damaged stays for whoever repairs or replaces it.
    pub fn try_open(file: F, config: Config, id: u128) -> Result<(Self, Recovery), Refused<F>> {
        let (state, recovery, restores) = match recover::open(&file, &config, id) {
            Ok(opened) => opened,
            Err(error) => return Err(Refused::with(error, file)),
        };
        let (mut log, pending) = Self::start(file, config, id, state, restores)?;
        for p in pending {
            if let Err(error) = p.wait() {
                return Err(Refused {
                    error,
                    file: log.stop(),
                });
            }
        }
        Ok((log, recovery))
    }

    /// Starts the owner and the log's threads, the owner's first batch being `restores`: the
    /// handles that answer them.
    fn start(
        file: F,
        config: Config,
        id: u128,
        state: state::State,
        restores: Vec<recover::Restore>,
    ) -> Result<(Self, Vec<Pending>), Refused<F>> {
        match Self::prepare(file.alignment(), config, id, restores) {
            Ok(prepared) => {
                let log = Self::spawn(file, prepared.p, state, prepared.room, prepared.first)?;
                Ok((log, prepared.pending))
            }
            Err(error) => Err(Refused::with(error, file)),
        }
    }

    /// The log's parameters, its queue's room, and its first batch with the handles that
    /// answer it.
    fn prepare(
        align: Alignment,
        config: Config,
        id: u128,
        restores: Vec<recover::Restore>,
    ) -> Result<Prepared, LogError> {
        let room_bytes = frame_room(&config, align)?;
        let queue_bytes = writer::charge(room_bytes)
            .and_then(|largest| largest.checked_mul(u64::try_from(PIPELINE_FRAMES).ok()?))
            .ok_or(LogError::Config("a queue of three frames past u64"))?;
        let p = Params {
            id,
            config,
            align,
            frame_room: room_bytes,
            queue_bytes,
        };
        let waiters = config
            .max_groups
            .checked_mul(room::GROUP_SUBMISSIONS)
            .ok_or(LogError::Config("waiters past usize"))?;
        let mut room = room::Room::new(room::Limits {
            submissions: config.queue_submissions,
            bytes: queue_bytes,
            waiters,
        });
        // The restore is the owner's first batch, not sent: sent one by one, it could be taken
        // in several batches, and a restore in parts is not atomic. It holds its room in the
        // queue as a sent one does, past the queue's bound if it must, since one frame bounds
        // it.
        let mut pending = Vec::with_capacity(restores.len());
        let mut first = Vec::with_capacity(restores.len());
        for r in restores {
            let marks = Marks {
                uncertain: r.uncertain,
                damaged: r.damaged,
            };
            let bytes = writer::submission_len(r.group, &r.update, marks)
                .and_then(writer::charge)
                .ok_or(LogError::TooLarge(usize::MAX))?;
            room.hold(r.group, bytes)?;
            let (reply, answer) = ticket::port();
            pending.push(Pending(Waiting::new(answer)));
            first.push(Submission {
                group: r.group,
                update: r.update,
                marks,
                bytes,
                class: Class::Normal,
                tags: writer::Tags::default(),
                ticket: Ticket::new(reply, None),
                admit: false,
                handle: false,
                lens: (0, 0),
                waits: false,
                epoch: 0,
                submitted: stats::now(),
            });
        }
        Ok(Prepared {
            p,
            room,
            first,
            pending,
        })
    }

    /// Starts the log's two threads, then hands the owner, with the device and its file, to the
    /// owner's thread: a thread the OS refuses leaves the file with the caller.
    #[allow(
        clippy::disallowed_methods,
        reason = "hyper-log owns its owner's and its device's threads (CLAUDE.md §1, sans-io's one exception)"
    )]
    fn spawn(
        file: F,
        p: Params,
        state: state::State,
        room: room::Room,
        first: Vec<Submission>,
    ) -> Result<Self, Refused<F>> {
        let config = p.config;
        // Everything that may wait in the inbox at once: every submission the queue admits,
        // every waiter for room, the device's words (its word that a frame is settled, its call
        // for a completion to be answered, the I/O thread's word that a job is back), and a
        // release from each group's handle.
        let capacity = config
            .queue_submissions
            .max(1)
            .saturating_add(config.max_groups.saturating_mul(room::GROUP_SUBMISSIONS))
            .saturating_add(device::JOBS)
            .saturating_add(1)
            .saturating_add(config.max_groups);
        let (inbox, messages) = sync_channel(capacity);
        // The owner's thread waits for the owner, which holds the file, until both threads run.
        let (to_owner, owned) = sync_channel::<Box<Owner<F>>>(1);
        let (requests, asked) = sync_channel(device::REQUESTS);
        let Ok(owner_thread) = std::thread::Builder::new()
            .name("hyper-log".into())
            .spawn(move || owned.recv().ok().and_then(|owner| Owner::lead(*owner)))
        else {
            return Err(Refused::with(LogError::Closed, file));
        };
        let done = inbox.clone();
        let Ok(io) = std::thread::Builder::new()
            .name("hyper-log-io".into())
            .spawn(move || {
                device::serve(&asked, &done);
                None
            })
        else {
            // The owner's thread ends once nothing can hand it the owner.
            drop(to_owner);
            let _ = owner_thread.join();
            return Err(Refused::with(LogError::Closed, file));
        };
        let largest = usize::try_from(config.segment_bytes).unwrap_or(usize::MAX);
        let pool = Pool::new(p.align, largest.saturating_mul(2), largest);
        let (more, told) = sync_channel(device::MORE);
        // One frame's flush is told at a time: the owner reads it before the next frame goes out.
        let (flushed, flushes) = sync_channel(1);
        // One job is out at a time, and comes back before the next goes out.
        let (back, returns) = sync_channel(1);
        let (token, tokens) = sync_channel(device::TOKENS);
        let device = device::Device::new(
            file,
            pool,
            config.segment_bytes,
            told,
            flushed,
            inbox.clone(),
            device::Returns {
                returns: back,
                tokens: token,
            },
        );
        let wiring = owner::Wiring {
            device,
            more,
            inbox: messages,
            flushes,
            returns,
            tokens,
            requests,
        };
        let owner = Box::new(Owner::new(p, state, room, first, wiring));
        // The owner's thread waits with room for the owner. Should the send fail, the owner, and
        // the file in it, ended with the thread.
        if to_owner.send(owner).is_err() {
            let _ = (owner_thread.join(), io.join());
            return Err(Refused {
                error: LogError::Closed,
                file: None,
            });
        }
        Ok(Self {
            inbox,
            threads: [Some(owner_thread), Some(io)],
            p,
        })
    }

    /// Submits `update` for `group`: refused at once with `Busy` when the queue is full, and
    /// with `TooLarge` when its records take more than a frame holds, otherwise
    /// answered through the returned handle once it is durable and a later record confirms
    /// so.
    pub fn submit(&self, group: u128, update: Update) -> Result<Pending, LogError> {
        self.send(group, Class::Normal, update, false, None, Hears::Admission)
    }

    /// Submits `update` for `group` as `submit` does, in `class`.
    pub fn submit_in(
        &self,
        group: u128,
        class: Class,
        update: Update,
    ) -> Result<Pending, LogError> {
        self.send(group, class, update, false, None, Hears::Admission)
    }

    /// Submits `update` for `group`, waiting for room in the queue rather than refusing. A
    /// replica cannot have a `Ready` refused, so the log holds it back instead
    /// (mantle docs/design/replica.md §3). Room the writer frees goes to waiters in arrival
    /// order, each told alone, and a fence tells each once, so the wait lasts no longer than the
    /// writer's progress (mantle docs/design/raft-log.md §3).
    pub fn submit_waiting(&self, group: u128, update: Update) -> Result<Pending, LogError> {
        self.send(group, Class::Normal, update, true, None, Hears::Admission)
    }

    /// Submits `update` for `group` as `submit_waiting` does, in `class`.
    pub fn submit_waiting_in(
        &self,
        group: u128,
        class: Class,
        update: Update,
    ) -> Result<Pending, LogError> {
        self.send(group, class, update, true, None, Hears::Admission)
    }

    /// Submits `update` for `group` in `class`, which waits in the log for room rather than
    /// being refused, as `submit_waiting`'s does, and wakes `waker` once its answer has come, so
    /// one thread can keep many submissions out and learn of each answer as it comes (mantle
    /// docs/design/node.md §1.3, measurement.md §10). The call returns once the submission is on
    /// its way, without waiting to hear it admitted: whatever the log says of it, a refusal for
    /// room (`Busy` past the waiters the log holds), a fence or a claimed group included, is its
    /// answer, as with a group's handle (`GroupLog::submit_waking`). The waker is woken exactly
    /// once, also when the log closes before it answers.
    ///
    /// Waiting for the admission was a round trip to the log's owner for every submission, and
    /// the owner woke each caller as it admitted it: a thread keeping many submissions out could
    /// send its next only once the owner had admitted the last, so the submitters a frame
    /// answered came back one owner wake at a time while the device sat idle
    /// (`docs/benchmarks.md`, "Many small appends").
    pub fn submit_waking(
        &self,
        group: u128,
        class: Class,
        update: Update,
        waker: Waker,
    ) -> Result<Pending, LogError> {
        self.send(group, class, update, true, Some(waker), Hears::Answer)
    }

    /// Sends `update` to the owner; the caller hears as `hears` says, and waits here for its
    /// admission when it hears of that.
    fn send(
        &self,
        group: u128,
        class: Class,
        update: Update,
        wait: bool,
        waker: Option<Waker>,
        hears: Hears,
    ) -> Result<Pending, LogError> {
        // Refused before it holds any room: no frame could take it, and every admitted
        // submission fits the byte bound alone, so none waits for a queue that cannot hold it.
        let len = writer::submission_len(group, &update, Marks::default())
            .ok_or(LogError::TooLarge(usize::MAX))?;
        if len > self.p.frame_room {
            return Err(LogError::TooLarge(len));
        }
        let bytes = writer::charge(len).ok_or(LogError::TooLarge(len))?;
        let (reply, answer) = ticket::port();
        let waiting = Waiting::new(answer);
        let submission = Submission {
            group,
            update,
            marks: Marks::default(),
            bytes,
            class,
            tags: writer::Tags::default(),
            ticket: Ticket::new(reply, waker),
            admit: hears == Hears::Admission,
            handle: false,
            lens: (0, 0),
            waits: hears == Hears::Waits,
            epoch: 0,
            submitted: stats::now(),
        };
        let message = Message::Submit { submission, wait };
        let sent = if wait {
            self.inbox.send(message).map_err(|_| LogError::Closed)
        } else {
            self.inbox.try_send(message).map_err(|e| match e {
                TrySendError::Full(_) => LogError::Busy,
                TrySendError::Disconnected(_) => LogError::Closed,
            })
        };
        sent?;
        if hears == Hears::Admission {
            waiting.admitted()?;
        }
        Ok(Pending(waiting))
    }

    /// Asks the owner and waits for its answer.
    fn ask(&self, query: Query) -> Result<Answer, LogError> {
        let (reply, answer) = ticket::port();
        let waiting = Waiting::new(answer);
        self.inbox
            .send(Message::Query(query, Ticket::new(reply, None)))
            .map_err(|_| LogError::Closed)?;
        waiting.wait()
    }

    /// The parameters the log runs with.
    pub fn config(&self) -> Config {
        self.p.config
    }

    /// Payload bytes one frame holds: an update no longer than this fits a frame of its own.
    pub fn frame_room(&self) -> Result<usize, LogError> {
        Ok(self.p.frame_room)
    }

    /// Charged bytes the queue holds at most for submissions not yet answered: three frames
    /// of the largest charge (mantle docs/design/raft-log.md §3). A submission is charged the
    /// bytes its records take in a frame, headers included, and its row in the frame's persist
    /// record, so an empty entry or an update of no records still costs what it holds.
    pub fn queue_bytes(&self) -> u64 {
        self.p.queue_bytes
    }

    /// The most bytes one entry may hold and still fit a frame alone.
    pub fn entry_room(&self) -> Result<usize, LogError> {
        let one = format::encoded_len(&format::Record::Entries {
            group: 0,
            first: 0,
            entries: &[(0, &[])],
        })
        .ok_or(LogError::Config("an entry's record"))?;
        self.frame_room()?
            .checked_sub(one)
            .ok_or(LogError::Config("a frame holds no entry"))
    }

    /// `update` for `group` in parts that each fit one frame, in the order they apply: its
    /// start and entries first, its hard state and proposals last, as etcd's raft persists a
    /// ready's entries before its hard state, together only when the store writes them
    /// atomically. An update that fits is one part. Written in order, the parts leave the
    /// group as the update would, and a crash between them leaves entries whose ready was
    /// never done, so never acknowledged. `TooLarge` when one entry or proposal alone is
    /// more than a frame holds.
    pub fn parts(&self, group: u128, update: Update) -> Result<Vec<Update>, LogError> {
        parts(self.p.frame_room, group, update)
    }

    /// Submits `update` and waits until it is durable; refused at once when the queue is
    /// full. The caller hears only the answer, a refusal included, and is not woken for the
    /// admission between.
    pub fn write(&self, group: u128, update: Update) -> Result<(), LogError> {
        self.send(group, Class::Normal, update, false, None, Hears::Waits)?
            .wait()
    }

    /// Submits `update`, waiting for room, and waits until it is durable, hearing only the
    /// answer.
    pub fn write_waiting(&self, group: u128, update: Update) -> Result<(), LogError> {
        self.send(group, Class::Normal, update, true, None, Hears::Waits)?
            .wait()
    }

    /// The handle through which `group` is written and read from here on (`group.rs`): the log
    /// refuses the group's submissions from anyone else, [`LogError::Claimed`], until the handle
    /// is dropped. Refused `Claimed` while another handle holds it, `Damaged` for a group whose
    /// records are damaged, and `TooManyGroups` past the log's bound on groups.
    pub fn group(&self, group: u128) -> Result<GroupLog<F>, LogError> {
        let (reply, answer) = ticket::port();
        let waiting = Waiting::new(answer);
        self.inbox
            .send(Message::Claim(group, Ticket::new(reply, None)))
            .map_err(|_| LogError::Closed)?;
        match waiting.wait()? {
            Answer::Claimed(mirror) => {
                Ok(GroupLog::new(self.inbox.clone(), self.p, group, *mirror))
            }
            _ => Err(LogError::Closed),
        }
    }

    /// The groups the log holds.
    pub fn groups(&self) -> Result<Vec<u128>, LogError> {
        match self.ask(Query::Groups)? {
            Answer::Groups(groups) => Ok(groups),
            _ => Err(LogError::Closed),
        }
    }

    /// A group's durable state; `None` for a group the log holds nothing of.
    pub fn view(&self, group: u128) -> Result<Option<View>, LogError> {
        match self.ask(Query::View(group))? {
            Answer::View(view) => Ok(view),
            _ => Err(LogError::Closed),
        }
    }

    /// The term of `index`, which may be the start's.
    pub fn term(&self, group: u128, index: u64) -> Result<u64, LogError> {
        match self.ask(Query::Term(group, index))? {
            Answer::Term(term) => Ok(term),
            _ => Err(LogError::Closed),
        }
    }

    /// The entries of `[low, high)`, as many as `max_bytes` of payload admit and one at
    /// least, reading from the file those no longer in memory. Each is copied out of a
    /// reservation the calling thread keeps for this; [`Log::fetch`] fills the caller's own.
    pub fn entries(
        &self,
        group: u128,
        low: u64,
        high: u64,
        max_bytes: u64,
    ) -> Result<Vec<Entry>, LogError> {
        let into = KEPT
            .try_with(|kept| kept.take())
            .ok()
            .flatten()
            .unwrap_or_default();
        let fetched = self.fetch(group, low, high, max_bytes, into)?;
        let entries = fetched
            .iter()
            .map(|(term, bytes)| Entry {
                term,
                bytes: bytes.to_vec(),
            })
            .collect();
        // Kept while it holds no more than a segment, the most one frame's entries take: a larger
        // fetch is a catch-up read whose reservation goes with it.
        let segment = usize::try_from(self.p.config.segment_bytes).unwrap_or(usize::MAX);
        if fetched.bytes.capacity() <= segment {
            let _ = KEPT.try_with(|kept| kept.set(Some(fetched)));
        }
        Ok(entries)
    }

    /// The entries of `[low, high)` as [`Log::entries`] gives them, copied into the caller's
    /// reservation `into`, which comes back filled.
    pub fn fetch(
        &self,
        group: u128,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: Fetched,
    ) -> Result<Fetched, LogError> {
        self.fetch_waking(group, low, high, max_bytes, into, None)?
            .wait()
    }

    /// Fetches as [`Log::fetch`] does, returning at once with a ticket, and wakes `waker`, if
    /// given, once the entries have come.
    pub fn fetch_waking(
        &self,
        group: u128,
        low: u64,
        high: u64,
        max_bytes: u64,
        into: Fetched,
        waker: Option<Waker>,
    ) -> Result<Fetching, LogError> {
        let (reply, answer) = ticket::port();
        let waiting = Waiting::new(answer);
        let query = Query::Entries {
            group,
            low,
            high,
            max_bytes,
            into,
        };
        self.inbox
            .send(Message::Query(query, Ticket::new(reply, waker)))
            .map_err(|_| LogError::Closed)?;
        Ok(Fetching(waiting))
    }

    /// Frames written and flushed since the log opened, and the updates they carried: how
    /// many updates one flush commits.
    pub fn flushed(&self) -> (u64, u64) {
        match self.ask(Query::Flushed) {
            Ok(Answer::Flushed(frames, updates)) => (frames, updates),
            _ => (0, 0),
        }
    }

    /// What the log measured from its opening to now (`docs/durable.md` §13.1), asked of its
    /// owner, which answers it as it comes: the owner never waits on I/O, so a flush in progress
    /// delays no answer and shows as [`LogStats::flushing_since`]. `into`, a box an earlier call
    /// gave back, is filled again, so a reader that keeps it allocates once.
    pub fn stats(&self, into: Option<Box<LogStats>>) -> Result<Box<LogStats>, LogError> {
        match self.ask(Query::Stats(into))? {
            Answer::Stats(stats) => Ok(stats),
            _ => Err(LogError::Closed),
        }
    }

    /// Whether a failed write or flush has fenced the log.
    pub fn is_fenced(&self) -> bool {
        matches!(self.ask(Query::Fenced), Ok(Answer::Fenced(true)))
    }

    /// Runs `look` on the log's file, on the thread that holds the device, between the log's own
    /// I/O, and returns what it returns: how a simulation reaches the device the log owns, to arm
    /// a fault or read its counts.
    pub fn with_file<R: Send + 'static>(
        &self,
        look: impl FnOnce(&F) -> R + Send + 'static,
    ) -> Result<R, LogError> {
        let (result, back) = sync_channel(1);
        let boxed: device::Look<F> = Box::new(move |file: &F| {
            let _ = result.try_send(look(file));
        });
        self.inbox
            .send(Message::Look(boxed))
            .map_err(|_| LogError::Closed)?;
        back.recv().map_err(|_| LogError::Closed)
    }

    /// Closes the log once it has answered every submission it took, and gives back its file.
    pub fn close(mut self) -> Result<F, LogError> {
        self.stop().ok_or(LogError::Closed)
    }

    fn stop(&mut self) -> Option<F> {
        if self.threads.iter().all(Option::is_none) {
            return None;
        }
        let _ = self.inbox.send(Message::Close);
        let mut file = None;
        for thread in &mut self.threads {
            if let Some(Ok(Some(f))) = thread.take().map(JoinHandle::join) {
                file = Some(f);
            }
        }
        file
    }
}

impl<F: BlockFile + 'static> Drop for Log<F> {
    fn drop(&mut self) {
        // The owner answers what it took, then the log's threads end.
        let _ = self.stop();
    }
}
