//! A node of a part's group (doc 34): its replica over the node's log, its liveness stream, and the
//! part's publishes, acknowledgements, redirects and deliveries, on the node's endpoint.
//!
//! **One shard.** The node's endpoint, its replica and its liveness stream run on the shard that
//! serves the node, run to completion (doc 32 D8): a frame read is stepped into the replica, the
//! replica is driven, and what the drive gives out leaves in the same turn. The log runs its own
//! threads and wakes the shard's task when a write it was given is durable; any wake drives the
//! replica, whose drive takes the log's answers (`Replica::drive`).
//!
//! **Publishes go to the leader** (P2). A node that leads the part proposes a publish's batch and
//! owes its publisher an acknowledgement, which it sends when it applies the batch (P5); a node that
//! does not lead redirects the publisher to the leader it knows, by node and term. A node that
//! stops leading redirects every publisher it owed an acknowledgement, whose batches the group may
//! or may not have committed: the publisher sends them again, and the part recognises those it took
//! (P6). A publish the replica cannot take now (the leader's uncommitted bytes at their bound) is
//! parked with its publisher's lane held, so the publisher waits, as a full lane holds it, until the
//! group commits enough to take it. Each entry states the identity that published it (its context),
//! whose sessions every replica judges the batch by.
//!
//! **Every publish is answered.** An acknowledgement or a redirect that finds its publisher's lane
//! full waits, in order, for the lane to make room, and the publisher's publishes are held until it
//! goes (Kafka's network layer mutes a connection while its response waits): a node that stopped
//! leading under load redirected some 2,300 publishes at once and dropped 1,657 of them on the full
//! lane, so their publisher never heard of them (the comparison's 10,000 a second, traced
//! 2026-10-18). Only a publisher whose connection is gone takes nothing: it sends again, on its next
//! connection, what it did not hear. A replica that fences ends the node, which holds nothing it
//! cannot answer while its owner reopens it from what is durable.
//!
//! **Raft frames** (P8) carry hyper-raft's own records, each from the node the transport
//! authenticated: a record whose sender is not the frame's peer is dropped. A frame that finds its
//! lane full waits in its peer's outbox, sent as the lane makes room; the outbox is bounded by the
//! core's own bound on the messages it holds for one member, and a message past it is dropped, as
//! a network may drop it: Raft sends it again.
//!
//! **Elections by suspicion** (doc 33 §4.1): the node-pair liveness stream judges each other node
//! for every group the pair shares, its heartbeats on a lane of their own (`wire::LIVENESS_LANE`),
//! and the replica takes its word (`suspect`, `trust`, `restarted`), as hyper-durable-e2e's members
//! do. A heartbeat leaves only once the node's log made a write durable since the one before was
//! due: where the group wrote none, the node writes an empty update of the stream's own group, so a
//! disk that stops stops the heartbeats with it.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::task::Waker;
use std::time::{Duration, Instant};

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_block::buf::Alignment;
use hyper_block::file::{CachingRequest, DeviceFile};
use hyper_durable::{
    Budget as ReplicaBudget, GroupStore, Output, Replica, ReplicaError, Settings as Shell,
};
use hyper_liveness::{Change, Liveness, Settings as LiveSettings, Suspicion, Write as LiveWrite};
use hyper_log::{Class as LogClass, Facts, Log, Pending, Update};
use hyper_raft::proto::{ConfState, Message};
use hyper_raft::wire::Record;
use hyper_raft::{Config, Limits, Stated, Storage};
use hyper_timing::{Exposure, Trust};
use hyper_transport::{Budget, Classes, Directory, Endpoint, Event, LaneId, PeerId, Refusal};

use crate::BusError;
use crate::batch::{Batch, MIN_BATCH};
use crate::group::{Applied, Outcome, PartMachine};
use crate::node::Node;
use crate::producer::Verdict;
use crate::wire::{self, Ack, Bus, Durability, Kind, LANE, LIVENESS_LANE, Leader, Redirect, Reply};

/// A duration's nanoseconds, a duration past what a u64 holds counted at the most.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Format: the part's group in the node's log.
const PART_GROUP: u128 = 1;
/// Format: the liveness stream's own group in the node's log, whose empty updates prove a flush
/// where the part's group wrote none.
const LIVENESS_GROUP: u128 = 2;
/// Format: the groups a node's log holds: the part's and the liveness stream's.
const LOG_GROUPS: usize = 2;
/// Format: the node log's file, in the node's data directory.
const LOG_FILE: &str = "bus.log";
/// Format: the part's image, in the node's data directory.
const IMAGE_FILE: &str = "part.image";
/// Format: the node's run, in the node's data directory: a count it raises at every start, before
/// the stream's first heartbeat (`hyper_liveness::Settings::run`).
const RUN_FILE: &str = "run";
/// Format: the log's identity: the bus's part log.
const LOG_ID: u128 = u128::from_le_bytes(*b"focal-bus-part-1");

/// What a node of a group is opened with.
#[derive(Clone, Debug)]
pub struct MemberSettings {
    /// This node, as the group and the directory name it: never zero.
    pub id: u64,
    /// Every other node of the group, as the directory names it, and where it listens.
    pub peers: Vec<(u64, SocketAddr)>,
    /// The directory the node keeps its log, its part's image and its run in.
    pub data: PathBuf,
    /// The root key file the node's log's keys are kept under, outside `data`
    /// (`focal_seal::open_or_create`).
    pub key_file: PathBuf,
    /// The bytes of the node's memory budget: what the window and the log's retention follow.
    pub memory_bytes: u64,
    /// Each node of the group's fate, in order of node, one each, the same at every node: what a
    /// failure takes together (doc 33 §4.1). None states no fate, and the group commits on a
    /// majority's disks alone.
    pub fates: Vec<(u64, u64)>,
    /// The distinct fates a commit on entries held in memory must span, with its leader's (core
    /// step R-8, doc 33 §4.4a).
    pub spanning: usize,
}

/// What a group of these fates acknowledges: replicated, held by a majority across `spanning`
/// fates or on a disk among them, where its members' distinct fates reach the span; otherwise
/// persisted, a majority's disks, for a group whose replicas share their fates never claims a
/// memory quorum one failure would erase (doc 33 §4.2).
fn class_of(fates: &[(u64, u64)], spanning: usize) -> Durability {
    let mut distinct: Vec<u64> = fates.iter().map(|(_, fate)| *fate).collect();
    distinct.sort_unstable();
    distinct.dedup();
    if !fates.is_empty() && spanning > 0 && distinct.len() >= spanning {
        Durability::Replicated
    } else {
        Durability::Persisted
    }
}

/// What a histogram of times counted: how many, and its p50, p99 and most, nanoseconds, each at
/// most an eighth past its quantile (`hyper_timing::Histogram`); zeros where it counted none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Times {
    /// How many times it counted.
    pub count: u64,
    /// The median.
    pub p50_ns: u64,
    /// The 99th percentile.
    pub p99_ns: u64,
    /// The most.
    pub max_ns: u64,
}

impl Times {
    fn of(histogram: &hyper_timing::Histogram) -> Self {
        let at = |ppm| histogram.quantile(ppm).unwrap_or(0);
        Self {
            count: histogram.count(),
            p50_ns: at(500_000),
            p99_ns: at(990_000),
            max_ns: at(1_000_000),
        }
    }
}

/// How long a leader's publishes took, by stage ([`Stages`]); a publisher's time beyond the
/// leader's acknowledgement is its connection's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PublishTimes {
    /// From each publish's proposal to its acknowledgement.
    pub acked: Times,
    /// A parked publish's wait before its proposal.
    pub parked: Times,
    /// From a run of entries' append to the write issued for it: its wait for room in the
    /// replica's pipeline of writes out.
    pub issued: Times,
    /// From a run of entries' append to its durability on the leader's log.
    pub persisted: Times,
    /// From a run's append to its commit.
    pub committed: Times,
}

/// What a node of a group did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemberStats {
    /// How long its publishes took while it led, by stage.
    pub publish: PublishTimes,
    /// Publishes proposed while the node led.
    pub proposed: u64,
    /// Publishes parked, their publisher held, while the replica could take no more.
    pub parked: u64,
    /// Publishes redirected to the leader.
    pub redirected: u64,
    /// Publishes sent again while their first was in flight: answered when it commits, never
    /// proposed twice.
    pub duplicates: u64,
    /// Inputs the node's memory could not hold until the window gave memory back for them.
    pub relieved: u64,
    /// Acknowledgements made, sent or waiting for their publisher's lane.
    pub acked: u64,
    /// Answers that waited for their publisher's lane to make room.
    pub answers_waited: u64,
    /// Answers a publisher's connection went before: it sends again what it did not hear.
    pub unanswered: u64,
    /// Raft records dropped: one that did not decode, or whose sender was not the frame's peer.
    pub dropped_records: u64,
    /// Raft messages dropped for a full outbox: Raft sends them again.
    pub dropped_messages: u64,
    /// Heartbeats the stream refused (stale, unknown peer).
    pub refused_heartbeats: u64,
    /// Drives of the replica.
    pub drives: u64,
    /// Drives the replica refused, changing nothing (stalled for room, refused by the core).
    pub refused_drives: u64,
    /// Suspicions the liveness stream told the replica: each a node it judged late.
    pub suspected: u64,
    /// Terms this node saw begin after its first: an election each, its own or another's.
    pub terms: u64,
    /// Compactions started: the log letting go of what the window let go.
    pub compactions: u64,
    /// Compactions the replica refused, changing nothing; asked again at the next turn.
    pub refused_compactions: u64,
    /// Nanoseconds the node spent compacting: making the part's image durable and the log's
    /// start's write submitted.
    pub compaction_ns: u64,
    /// The longest the node went without waiting, nanoseconds: from the end of a wait through
    /// the events it brought and the turn after them, what its timers and its heartbeats waited
    /// behind.
    pub busy_most_ns: u64,
    /// What the node's log wrote to its device, as the log counted it.
    pub log: LogWrites,
    /// What the node's liveness stream did, over the peers it watches.
    pub liveness: LivenessFigures,
    /// The replica's writes by what each was for: a `Ready`'s, a commit alone the fence or a
    /// quiet period asked for, a compaction's start (`hyper_durable::Writes`), each a log update.
    pub writes: hyper_durable::Writes,
}

/// What a node's liveness stream did, summed over the peers it watches
/// (`hyper_liveness::PairReport`): its heartbeats, and the empty writes it asked of the log to
/// prove them, each a frame of the log's where no other durable write came in time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LivenessFigures {
    /// Empty writes the stream asked the log for: heartbeats no other durable write proved.
    pub writes: u64,
    /// Heartbeats sent to the peers.
    pub sent: u64,
    /// Heartbeats taken from the peers.
    pub taken: u64,
    /// Slots that came due while the node was behind and were never sent.
    pub skipped: u64,
    /// Heartbeats refused for their flush proof.
    pub unproven: u64,
    /// The least and the most interval the node's heartbeats to a peer are due at, nanoseconds;
    /// zero before one is scheduled.
    pub sending_least_ns: u64,
    pub sending_most_ns: u64,
    /// The floor under every interval the node sends at (`E[flush] + G`), nanoseconds.
    pub floor_ns: u64,
}

/// What a node's log wrote from its opening, as hyper-log counted it (`hyper_log::LogStats`): what
/// its durable writes cost the device, as Kafka's broker reports its log flushes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogWrites {
    /// Frames written and flushed: each a group of updates made durable together.
    pub frames: u64,
    /// Updates those frames carried: the replica's writes and the liveness stream's.
    pub updates: u64,
    /// Bytes written, each frame, persist record and confirmation padded to the file's alignment.
    pub bytes: u64,
    /// Flushes of the device's cache.
    pub flushes: u64,
    /// Writes made durable on their own (a FUA write), no flush with them.
    pub durable_writes: u64,
    /// Durable writes made with a write and a flush instead.
    pub durable_fallbacks: u64,
    /// Confirmations written and flushed where a durable write was possible, the log's device
    /// having measured the write and flush cheaper on this drive.
    pub flushed_confirmations: u64,
    /// The flushes' p50 and p99, nanoseconds (each at most an eighth more than the quantile).
    pub flush_p50_ns: u64,
    pub flush_p99_ns: u64,
    /// The updates' waits from submission to the flush that let them be answered, p50, p99 and
    /// the most, nanoseconds.
    pub commit_wait_p50_ns: u64,
    pub commit_wait_p99_ns: u64,
    pub commit_wait_max_ns: u64,
    /// Asks for a slot past the file's bound the log refused, each answered `Full`.
    pub growth_refused: u64,
    /// Sweeps the log laid, its tail's live entries copied ahead of a frame's updates when it had
    /// fewer than two segments free, and the bytes they copied: what it wrote a second time.
    pub sweeps: u64,
    pub swept_bytes: u64,
    /// Bytes of zeros written over the slots the log's file grew by, ahead of their first frames
    /// while its device had nothing else to do: what growing the log wrote that no record holds
    /// (counted in `bytes` too).
    pub zeroed_bytes: u64,
    /// Slots filled with zeros whole ahead of their frames, the pieces they were written in, and
    /// fills whose write or flush failed (hyper-log's `LogStats::fills`, durable.md §6.3).
    pub fills: u64,
    pub fill_pieces: u64,
    pub fill_failures: u64,
    /// Whether the log's file system writes its file in place where it would copy it on write
    /// (`DeviceFile::written_in_place`): btrfs's mark, asked as an empty file is opened. `None`
    /// on a file system that writes every file in place.
    pub written_in_place: Option<bool>,
}

impl LogWrites {
    fn of(stats: &hyper_log::LogStats, written_in_place: Option<bool>) -> Self {
        Self {
            written_in_place,
            frames: stats.frames,
            updates: stats.updates,
            bytes: stats.bytes,
            flushes: stats.flushes,
            durable_writes: stats.durable_writes,
            durable_fallbacks: stats.durable_fallbacks,
            flushed_confirmations: stats.flushed_confirmations,
            flush_p50_ns: stats.flush.quantile(500_000).unwrap_or(0),
            flush_p99_ns: stats.flush.quantile(990_000).unwrap_or(0),
            commit_wait_p50_ns: stats.commit_wait.quantile(500_000).unwrap_or(0),
            commit_wait_p99_ns: stats.commit_wait.quantile(990_000).unwrap_or(0),
            commit_wait_max_ns: stats.commit_wait.quantile(1_000_000).unwrap_or(0),
            growth_refused: stats.growth_refused,
            sweeps: stats.sweeps,
            swept_bytes: stats.swept_bytes,
            zeroed_bytes: stats.zeroed_bytes,
            fills: stats.fills,
            fill_pieces: stats.fill_pieces,
            fill_failures: stats.fill_failures,
        }
    }
}

/// What a node of a group tells its operator as it happens, each on the turn it happened: what its
/// liveness stream came to believe of a peer, and each term its replica entered and the member it
/// came to believe led it. Kafka's brokers log an ISR shrink and a controller's election alike.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GroupEvent {
    /// The stream judged a peer late and told the replica: when its freshness point passed and when
    /// the node noticed, on the node's clock, and the heartbeat before.
    Suspected(Suspicion),
    /// A fresh heartbeat left a suspected peer trusted.
    Trusted {
        /// The peer.
        peer: u64,
    },
    /// A heartbeat came from a later run of a peer: it restarted.
    Restarted {
        /// The peer.
        peer: u64,
    },
    /// The replica stood in `term`, believing `leader` led it: zero while an election is under
    /// way, then once more as the leader is known.
    Term {
        /// The term.
        term: u64,
        /// The member it believed led, zero for none.
        leader: u64,
    },
}

/// hyper-durable's budget over the node's memory: what the replica reserves before an input, from
/// the ordinary lane, and what a transition came to hold past it, which cannot be refused, from the
/// completion lane; what that lane cannot hold is owed, counted, and every reservation refused
/// until it is given back (focal-consensus's `FocalBudget`, the same rule).
#[derive(Debug)]
pub struct MemberBudget {
    budget: MemoryBudget,
    ordinary: Option<Allocation>,
    completion: Option<Allocation>,
    owed: u64,
}

impl MemberBudget {
    fn new(budget: MemoryBudget) -> Self {
        Self {
            budget,
            ordinary: None,
            completion: None,
            owed: 0,
        }
    }

    fn hold(&mut self, lane: BudgetLane, bytes: u64) -> bool {
        let Ok(bytes) = usize::try_from(bytes) else {
            return false;
        };
        let Ok(reserved) = self.budget.reserve(BudgetKind::Pending, lane, bytes) else {
            return false;
        };
        let mut taken = reserved.commit();
        let slot = match lane {
            BudgetLane::Ordinary => &mut self.ordinary,
            BudgetLane::Completion | BudgetLane::Committed => &mut self.completion,
        };
        match slot {
            Some(held) => held.absorb(&mut taken).is_ok(),
            None => {
                *slot = Some(taken);
                true
            }
        }
    }
}

/// Gives back up to `bytes` of `held`; what it did not hold, still to give back.
fn give_back(held: &mut Option<Allocation>, bytes: usize) -> usize {
    let Some(allocation) = held.as_mut() else {
        return bytes;
    };
    let given = bytes.min(allocation.bytes());
    match allocation.shrink_to(allocation.bytes().saturating_sub(given)) {
        Ok(()) => bytes.saturating_sub(given),
        Err(_) => bytes,
    }
}

impl MemberBudget {
    /// Holds what is owed on the completion lane once the node's memory has room for it. What a
    /// transition came to hold past that lane is the replica's either way; refused until a release
    /// of the replica's own repaid it, a leader that went into debt while its window filled the
    /// node's memory refused every input after, its followers' acknowledgements among them, whose
    /// commit was the release that would have repaid it, though the window had since given its
    /// memory back (2026-10-19: a group of 24 MiB nodes stopped for good, its leader refusing
    /// 1,752 heartbeat answers and 12 acknowledgements of 272 bytes with 3 MiB of 24 in use).
    fn settle_owed(&mut self) {
        if self.owed > 0 && self.hold(BudgetLane::Completion, self.owed) {
            self.owed = 0;
        }
    }
}

impl ReplicaBudget for MemberBudget {
    const BOUNDED: bool = true;

    fn reserve(&mut self, bytes: u64) -> bool {
        self.settle_owed();
        self.owed == 0 && self.hold(BudgetLane::Ordinary, bytes)
    }

    fn charge(&mut self, bytes: u64) {
        if !self.hold(BudgetLane::Completion, bytes) {
            self.owed = self.owed.saturating_add(bytes);
        }
    }

    fn release(&mut self, bytes: u64) {
        let repaid = bytes.min(self.owed);
        self.owed = self.owed.saturating_sub(repaid);
        let rest = usize::try_from(bytes.saturating_sub(repaid)).unwrap_or(usize::MAX);
        let rest = give_back(&mut self.completion, rest);
        give_back(&mut self.ordinary, rest);
    }
}

type PartReplica = Replica<GroupStore<DeviceFile>, PartMachine, MemberBudget>;

/// What the liveness stream asked during a call into it.
#[derive(Debug, Default)]
struct Asked {
    heartbeats: Vec<(u64, Vec<u8>)>,
    flush: bool,
    changes: Vec<Change>,
}

impl hyper_liveness::Output for Asked {
    fn heartbeat(&mut self, peer: u64, message: &[u8]) {
        self.heartbeats.push((peer, message.to_vec()));
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        self.changes.push(change);
    }
}

/// A publish the replica could not take yet, its publisher's lane held until it does.
#[derive(Debug)]
struct Parked {
    peer: PeerId,
    lane: LaneId,
    batch: Vec<u8>,
    /// When it parked: what it waited before it was proposed is `MemberStats::publish`'s.
    parked_at: Instant,
}

/// How long a leader's publishes took, split where the group's work splits: each publish from
/// its proposal to its acknowledgement and a parked one's wait before its proposal, and each run of
/// entries the leader appended until a write was issued for it (the replica's pipeline of writes
/// out, `hyper_log::GroupLog::depth`, had room), until it was durable on the leader's own log and
/// until a quorum committed it (the leader's write; its followers' writes and their answers). A run
/// is the entries one turn appended, timed from the end of that turn to the end of the turn that saw
/// them issued, durable or committed. Each run queue holds at most `bound` runs, the publishes the node may owe at once
/// (every run a leader's proposals made holds one it owes until its commit); a run past it joins
/// the newest, timed from the newest's start, so a sample overstates a run's time rather than lose
/// it.
struct Stages {
    /// Runs no write was issued for yet: each run's last index, when it was seen appended.
    unissued: VecDeque<(u64, Instant)>,
    /// Runs not yet durable on the leader's log.
    unpersisted: VecDeque<(u64, Instant)>,
    /// Runs not yet committed.
    uncommitted: VecDeque<(u64, Instant)>,
    /// The last index seen appended: the next run starts past it.
    seen: u64,
    bound: usize,
    acked: hyper_timing::Histogram,
    parked: hyper_timing::Histogram,
    issued: hyper_timing::Histogram,
    persisted: hyper_timing::Histogram,
    committed: hyper_timing::Histogram,
}

impl Stages {
    fn new(bound: usize) -> Self {
        Self {
            unissued: VecDeque::new(),
            unpersisted: VecDeque::new(),
            uncommitted: VecDeque::new(),
            seen: 0,
            bound: bound.max(1),
            acked: hyper_timing::Histogram::new(),
            parked: hyper_timing::Histogram::new(),
            issued: hyper_timing::Histogram::new(),
            persisted: hyper_timing::Histogram::new(),
            committed: hyper_timing::Histogram::new(),
        }
    }

    /// What the leader's log says now: its last index, what a write was issued for, what is
    /// durable on it and what is committed. A node that does not lead times nothing: what it
    /// appended as a follower is its leader's.
    fn observe(&mut self, leads: bool, last: u64, reached: Reached, now: Instant) {
        if !leads {
            self.unissued.clear();
            self.unpersisted.clear();
            self.uncommitted.clear();
            self.seen = last;
            return;
        }
        let appended = last > self.seen;
        self.seen = self.seen.max(last);
        for (queue, times, reached) in [
            (&mut self.unissued, &mut self.issued, reached.issued),
            (
                &mut self.unpersisted,
                &mut self.persisted,
                reached.persisted,
            ),
            (
                &mut self.uncommitted,
                &mut self.committed,
                reached.committed,
            ),
        ] {
            while let Some(&(index, at)) = queue.front() {
                if index > reached {
                    break;
                }
                queue.pop_front();
                times.record(nanos(now.saturating_duration_since(at)));
            }
            if !appended {
                continue;
            }
            let full = queue.len() >= self.bound;
            match queue.back_mut() {
                // Durable, or committed, by the end of the turn that appended it.
                _ if last <= reached => times.record(0),
                Some(newest) if full => newest.0 = last,
                _ => queue.push_back((last, now)),
            }
        }
    }
}

/// How far the leader's log has come, by index: what a write was issued for, what is durable and
/// what is committed.
#[derive(Clone, Copy, Debug)]
struct Reached {
    issued: u64,
    persisted: u64,
    committed: u64,
}

/// An answer to a publish: its acknowledgement or its redirect, as sent.
#[derive(Clone, Copy, Debug)]
enum Answer {
    Ack([u8; Ack::BYTES]),
    Redirect([u8; Redirect::BYTES]),
}

impl Answer {
    const fn kind(&self) -> Kind {
        match self {
            Self::Ack(_) => Kind::Ack,
            Self::Redirect(_) => Kind::Redirect,
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::Ack(bytes) => bytes,
            Self::Redirect(bytes) => bytes,
        }
    }
}

/// The key an acknowledgement is owed under: a batch's stream, producer and sequence.
type Owed = (u64, u64, u32);

/// A node of a part's group (module docs).
pub struct Member {
    id: u64,
    peers: Vec<(u64, SocketAddr)>,
    replica: PartReplica,
    log: Log<DeviceFile>,
    /// Whether the log's file is written in place, as its file system said when it was opened.
    written_in_place: Option<bool>,
    liveness: Liveness,
    asked: Asked,
    attached: Vec<u64>,
    liveness_write: Option<(Pending, u64)>,
    liveness_failed: bool,
    out: Output<Applied>,
    /// Raft frames waiting for their peer's lane, oldest first, each peer's at most `outbox_bound`.
    outbox: HashMap<u64, VecDeque<Vec<u8>>>,
    outbox_bound: usize,
    /// Acknowledgements owed to publishers, while the node leads: each batch with the identity
    /// that published it, whose sessions it is judged by (a session is its identity's own, so two
    /// identities naming one session owe apart).
    owed: HashMap<(PeerId, Owed), Instant>,
    /// How long its publishes took while the node led, by stage (boxed: its histograms are 16 KB).
    stages: Box<Stages>,
    /// The most acknowledgements owed at once: a publisher's lane window for each client the node
    /// admits, what the publishers can have unacknowledged.
    owed_bound: usize,
    parked: VecDeque<Parked>,
    /// Answers to publishes whose publisher's lane had no room, each publisher's in the order they
    /// were made: sent as its lane makes room (`Event::LaneRoom`), never dropped, the publisher's
    /// publishes held meanwhile so it sends nothing more to be answered (Kafka's network layer
    /// mutes a connection while its response waits). What waits is what the publisher had in
    /// flight when its lane filled: its acknowledgements owed and the publishes read in the turn
    /// its lane was held, no more.
    answers: HashMap<PeerId, VecDeque<Answer>>,
    /// Whether the replica fenced: the node ends, to be reopened from what is durable.
    fenced: bool,
    /// The latest term this node's replica stood in: the terms it saw begin are counted from it.
    seen_term: u64,
    /// The member the replica believed led at its last turn, zero for none.
    seen_leader: u64,
    /// The window's segments emptied as of the last turn ([`Member::compact_behind_window`]).
    emptied_seen: u64,
    /// A segment was emptied since the log was last compacted.
    compact_wanted: bool,
    /// When the node's last wait ended: its busy period runs from here to its turn's end.
    woke: Option<Instant>,
    /// Whether the node led at its last turn: a node that stops leading redirects what it owed.
    leading: bool,
    /// The drives one turn makes at most: the store's depth and one.
    drives: usize,
    /// A turn left the replica more to do: the next is due at once.
    due: bool,
    origin: Instant,
    encoded: Vec<u8>,
    stats: MemberStats,
    /// What the group's commit achieves, its acknowledgements' class ([`class_of`]).
    class: Durability,
}

impl std::fmt::Debug for Member {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Member")
            .field("id", &self.id)
            .field("leader", &self.replica.leader())
            .field("term", &self.replica.term())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

/// The node's run: the count kept at `path`, raised and made durable before it is used, so each
/// start's is greater than every earlier start's.
fn raise_run(path: &Path) -> Result<u64, BusError> {
    use focal_platform::fs::{FileMedium, Medium};
    let medium = FileMedium;
    let previous = if medium
        .exists(path)
        .map_err(|_| BusError::Configuration("the node's run could not be looked for"))?
    {
        let bytes = medium
            .read(path, 8)
            .map_err(|_| BusError::Configuration("the node's run could not be read"))?;
        let bytes: [u8; 8] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| BusError::Configuration("the node's run is damaged"))?;
        u64::from_le_bytes(bytes)
    } else {
        0
    };
    let run = previous
        .checked_add(1)
        .ok_or(BusError::Capacity("the node's runs"))?;
    let mut medium = FileMedium;
    focal_platform::fs::install(&mut medium, path, &run.to_le_bytes())
        .map_err(|_| BusError::Configuration("the node's run could not be made durable"))?;
    Ok(run)
}

/// The node log's configuration, from what the bus states (hyper-log's `Config::derive`; no field
/// chosen here): a frame holds the largest batch with the shell's record around it; the part's
/// group retains its window, which is at most the node's memory, and what may be uncommitted past
/// it; the file is what both hold.
fn log_config(align: Alignment, memory_bytes: u64) -> Result<hyper_log::Config, BusError> {
    let unfit = || BusError::Configuration("the node's log facts past what this machine addresses");
    let largest = usize::try_from(wire::BATCH_BOUND)
        .map_err(|_| unfit())?
        .checked_add(hyper_durable::ENTRY_OVERHEAD)
        .ok_or_else(unfit)?;
    // The least bytes a group's entry takes as the core counts it: a batch of one empty record.
    let least_entry = u64::try_from(hyper_raft::wire::ENTRY_FIXED_BYTES.saturating_add(MIN_BATCH))
        .map_err(|_| unfit())?;
    let uncommitted_bytes = memory_bytes
        .checked_add(wire::ENTRY_BOUND)
        .ok_or_else(unfit)?;
    hyper_log::Config::derive(&Facts {
        align,
        // focal's node log is always sealed (doc 29 §5).
        sealed: true,
        largest_entry: largest,
        disk_bytes: memory_bytes
            .checked_add(uncommitted_bytes)
            .ok_or_else(unfit)?,
        max_groups: LOG_GROUPS,
        cadence_entries: memory_bytes.checked_div(least_entry).ok_or_else(unfit)?,
        cadence_bytes: memory_bytes,
        uncommitted_entries: uncommitted_bytes
            .checked_div(least_entry)
            .ok_or_else(unfit)?,
        uncommitted_bytes,
        // An append's entries are read from the log for a member behind: one message's at most.
        cache_bytes: wire::RAFT_BOUND,
    })
    .map_err(|_| BusError::Configuration("the node's log facts give no configuration"))
}

/// Opens the node's log in `data`, sealed under the keys `key_file` keeps, or makes it; with
/// whether its file is written in place (`DeviceFile::written_in_place`).
fn open_log(
    data: &Path,
    key_file: &Path,
    memory_bytes: u64,
) -> Result<(Log<DeviceFile>, Option<bool>), BusError> {
    let path = data.join(LOG_FILE);
    let exists = path
        .try_exists()
        .map_err(|_| BusError::Configuration("the node's log could not be looked for"))?;
    if !exists {
        std::fs::File::create(&path)
            .map_err(|_| BusError::Configuration("the node's log could not be made"))?;
        focal_platform::sync_dir(data).map_err(|_| {
            BusError::Configuration("the node's data directory could not be synced")
        })?;
    }
    let block = {
        let file = std::fs::File::open(&path)
            .map_err(|_| BusError::Configuration("the node's log could not be opened"))?;
        hyper_block::file::preferred_block(&file, &path)
            .map_err(|_| BusError::Configuration("the device reports no block size"))?
    };
    let align = Alignment::new(block)
        .map_err(|_| BusError::Configuration("the device's block is not a power of two"))?;
    let config = log_config(align, memory_bytes)?;
    let file = DeviceFile::open(&path, false, CachingRequest::PreferDirect, align)
        .map_err(|_| BusError::Configuration("the node's log could not be opened"))?;
    let written_in_place = file.written_in_place();
    let ((parent, auth), _keys) = focal_seal::open_or_create(data, key_file)
        .map_err(|_| BusError::Configuration("the node's keys could not be opened"))?
        .split();
    let with = hyper_log::With {
        sealing: Some(hyper_log::Sealing { parent, auth }),
        growth: None,
    };
    let log = if exists {
        hyper_log::Log::open_with(file, config, LOG_ID, with)
            .map(|(log, _)| log)
            .map_err(|_| BusError::Configuration("the node's log could not be opened"))?
    } else {
        hyper_log::Log::create_with(file, config, LOG_ID, with)
            .map_err(|_| BusError::Configuration("the node's log could not be made"))?
    };
    Ok((log, written_in_place))
}

impl Member {
    /// The node `settings` names, opened on its log and its part's image, serving the part through
    /// `node`; `members` are the group's voters, this node among them, its founding configuration.
    /// `clients` and `lane_window` bound the acknowledgements it may owe.
    pub fn open(
        settings: MemberSettings,
        node: Node,
        members: Vec<u64>,
        clients: usize,
        lane_window: usize,
        budget: MemoryBudget,
        now: Instant,
    ) -> Result<Self, BusError> {
        // The data directory, owner-only, each directory made durable in its parent.
        focal_platform::fs::create_dir_all_durable(
            &settings.data,
            focal_platform::fs::create_dir_private,
        )
        .map_err(|_| BusError::Configuration("the node's data directory could not be made"))?;
        let run = raise_run(&settings.data.join(RUN_FILE))?;
        let (log, written_in_place) =
            open_log(&settings.data, &settings.key_file, settings.memory_bytes)?;
        let store = GroupStore::claim(&log, PART_GROUP)
            .map_err(|_| BusError::Configuration("the part's group could not be claimed"))?;
        let drives = hyper_durable::LogStore::depth(&store).saturating_add(1);
        let founding = ConfState {
            voters: members.clone(),
            ..ConfState::default()
        };
        let machine = PartMachine::open(
            node,
            settings.data.join(IMAGE_FILE),
            founding,
            members.len(),
        )?;
        let memory = usize::try_from(settings.memory_bytes)
            .map_err(|_| BusError::Configuration("the node's memory"))?;
        let message = usize::try_from(wire::RAFT_BOUND)
            .map_err(|_| BusError::Configuration("the group's largest message"))?;
        let limits = Limits::derive(Stated {
            message,
            members: members.len(),
            memory,
            // Replaced by the store's depth as the replica opens (`Settings::core`).
            depth: 1,
        })
        .map_err(|_| BusError::Configuration("the group's limits"))?;
        let outbox_bound = limits.pending_messages;
        let mut fates = settings.fates.clone();
        fates.sort_unstable();
        let class = class_of(&fates, settings.spanning);
        let shell = Shell {
            core: Config {
                max_size_per_msg: wire::ENTRY_BOUND,
                // What one drive applies: a segment's batches, what one allocation of the part
                // holds, the page an owner reserves before a drive (hyper-durable §2.2).
                max_committed_size_per_ready: u64::try_from(crate::node::NodeConfig::SEGMENT_BYTES)
                    .unwrap_or(u64::MAX),
                max_uncommitted_size: settings.memory_bytes,
                check_quorum: true,
                pre_vote: true,
                seed: settings.id,
                // Held commits where the group states its fates (core step R-8): a follower
                // answers for what it holds in memory under its durable promise, and the leader,
                // counting its own disk, commits on a majority whose fates with its own span
                // `spanning`, or on a majority's disks, whichever comes first.
                fates: (class == Durability::Replicated).then(|| hyper_raft::Fates {
                    of: fates.clone(),
                    spanning: settings.spanning,
                }),
                ..Config::new(settings.id, limits)
            },
            elections: hyper_raft::Elections::Suspicion,
            // The owner's period is the interval its liveness stream sends at, restated each turn
            // as it moves (`Member::restate_period`); before the stream schedules a heartbeat there
            // is none, and a commit that moved alone is written the moment no write is out.
            quiet: Duration::ZERO,
        };
        let replica = Replica::open(&shell, store, machine, MemberBudget::new(budget))
            .map_err(|_| BusError::Configuration("the part's replica could not be opened"))?;
        let liveness = Liveness::new(LiveSettings {
            local: settings.id,
            run,
            max_peers: settings.peers.len(),
            history: Exposure::new(),
            // The monotonic clock's resolution as the system reports it (`clock_getres`).
            resolution: Duration::from_nanos(hyper_rt::machine::clock::resolution_ns()),
        })
        .map_err(|_| BusError::Configuration("the liveness stream"))?;
        Ok(Self {
            id: settings.id,
            peers: settings.peers,
            replica,
            log,
            written_in_place,
            liveness,
            asked: Asked::default(),
            attached: Vec::new(),
            liveness_write: None,
            liveness_failed: false,
            out: Output::default(),
            outbox: HashMap::new(),
            outbox_bound,
            owed: HashMap::new(),
            stages: Box::new(Stages::new(clients.saturating_mul(lane_window))),
            owed_bound: clients.saturating_mul(lane_window),
            parked: VecDeque::new(),
            answers: HashMap::new(),
            fenced: false,
            seen_term: 0,
            seen_leader: 0,
            emptied_seen: 0,
            compact_wanted: false,
            woke: None,
            leading: false,
            drives,
            due: false,
            origin: now,
            encoded: Vec::new(),
            stats: MemberStats::default(),
            class,
        })
    }

    /// Whether the replica fenced: the node takes nothing more until it is reopened from what is
    /// durable, and its owner ends it.
    pub const fn fenced(&self) -> bool {
        self.fenced
    }

    /// The node's id.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// What the node did, its log's writes as the log counts them now.
    pub fn stats(&self) -> MemberStats {
        let mut stats = self.stats;
        if let Ok(log) = self.log.stats(None) {
            stats.log = LogWrites::of(&log, self.written_in_place);
        }
        stats.publish = PublishTimes {
            acked: Times::of(&self.stages.acked),
            parked: Times::of(&self.stages.parked),
            issued: Times::of(&self.stages.issued),
            persisted: Times::of(&self.stages.persisted),
            committed: Times::of(&self.stages.committed),
        };
        stats.writes = self.replica.writes();
        let figures = &mut stats.liveness;
        figures.floor_ns = self.liveness.floor().map_or(0, nanos);
        for report in self
            .attached
            .iter()
            .filter_map(|peer| self.liveness.report(*peer))
        {
            figures.sent = figures.sent.saturating_add(report.sent);
            figures.taken = figures.taken.saturating_add(report.taken);
            figures.skipped = figures.skipped.saturating_add(report.skipped);
            figures.unproven = figures.unproven.saturating_add(report.unproven);
            if let Some(sending) = report.sending.map(nanos) {
                figures.sending_most_ns = figures.sending_most_ns.max(sending);
                figures.sending_least_ns = match figures.sending_least_ns {
                    0 => sending,
                    least => least.min(sending),
                };
            }
        }
        stats
    }

    /// Whether the node leads its part.
    pub fn is_leader(&self) -> bool {
        self.replica.is_leader()
    }

    /// The term the node knows the part in.
    pub fn term(&self) -> u64 {
        self.replica.term()
    }

    /// What the group's commit achieves: the class its acknowledgements state.
    pub const fn class(&self) -> Durability {
        self.class
    }

    /// The leader the node knows, and the term it knows the part in.
    pub fn leader(&self) -> Leader {
        let leader = self.replica.leader();
        Leader {
            node: (leader != 0).then_some(leader),
            term: self.replica.term(),
        }
    }

    /// The part and its subscribers.
    pub fn node(&self) -> &Node {
        self.replica.machine().node()
    }

    /// The part and its subscribers, to change.
    pub fn node_mut(&mut self) -> &mut Node {
        self.replica.machine_mut().node_mut()
    }

    /// The group's commit the replica's log made durable.
    pub fn durable_commit(&self) -> u64 {
        self.replica.durable_commit()
    }

    fn now_ns(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.origin).as_nanos()).unwrap_or(u64::MAX)
    }

    /// Dials every other node of a greater id: one connection a pair, made by the lesser.
    pub fn connect<B, D>(&self, endpoint: &mut Endpoint<Bus, B, D>, now: Instant)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        for (peer, address) in &self.peers {
            if *peer > self.id {
                let _ = endpoint.connect(now, *peer, *address);
            }
        }
    }

    /// Whether `peer` is another node of the group.
    fn is_node(&self, peer: PeerId) -> bool {
        self.peers.iter().any(|(id, _)| *id == peer)
    }

    /// Acts on one of the endpoint's events.
    pub fn on_event<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        event: Event<Bus>,
        now: Instant,
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        match event {
            Event::Frame {
                kind: Kind::Raft,
                peer,
                frame,
                ..
            } => {
                self.hear_record(peer, frame.bytes());
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Heartbeat,
                peer,
                frame,
                ..
            } => {
                let arrival = self.now_ns(now);
                if self
                    .liveness
                    .on_heartbeat(peer, frame.bytes(), arrival, &mut self.asked)
                    .is_err()
                {
                    self.stats.refused_heartbeats = self.stats.refused_heartbeats.saturating_add(1);
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Publish,
                peer,
                lane,
                frame,
            } => {
                self.publish(endpoint, peer, lane, frame.bytes());
                endpoint.release(frame);
            }
            Event::Request {
                exchange,
                kind: Kind::Subscribe,
                ..
            } if !self.replica.is_leader() => {
                let reply = Reply::Redirected(self.leader());
                if endpoint.reply(exchange, &reply.encode(), None).is_ok() {
                    endpoint.end(exchange);
                }
            }
            Event::LaneRoom { peer, .. } if self.is_node(peer) => self.flush_outbox(endpoint, peer),
            // A client's lane with room takes its waiting answers, then the node's deliveries to it.
            room @ Event::LaneRoom { peer, .. } => {
                self.flush_answers(endpoint, peer);
                self.node_mut().on_event(endpoint, room);
            }
            Event::Closed { peer, .. } if self.is_node(peer) => {
                // What waited for the lane goes with the connection; Raft sends it again.
                if let Some(waiting) = self.outbox.get_mut(&peer) {
                    waiting.clear();
                }
            }
            closed @ Event::Closed { peer, .. } => {
                // A publisher whose connection went sends again what it did not hear, on the next.
                if let Some(waiting) = self.answers.remove(&peer) {
                    let gone = u64::try_from(waiting.len()).unwrap_or(u64::MAX);
                    self.stats.unanswered = self.stats.unanswered.saturating_add(gone);
                }
                self.node_mut().on_event(endpoint, closed);
            }
            other => self.node_mut().on_event(endpoint, other),
        }
    }

    /// Steps a Raft record from `peer` into the replica, if it decodes and `peer` sent it.
    fn hear_record(&mut self, peer: PeerId, bytes: &[u8]) {
        match Message::decode(bytes) {
            Ok(message) if message.from == peer && self.is_node(peer) => {
                // A refusal is the core's word on a message it will not take (an older term, a
                // member it does not know): the message is spent either way. One the node's memory
                // could not hold is stepped again once the window gave memory back
                // ([`Member::relieve_window`]): refused for good, a follower whose window had
                // filled its memory took no append again, and fell behind for good.
                if let Err(ReplicaError::Budget(_)) = self.replica.step(message)
                    && self.relieve_window()
                    && let Ok(again) = Message::decode(bytes)
                {
                    let _ = self.replica.step(again);
                }
            }
            _ => self.stats.dropped_records = self.stats.dropped_records.saturating_add(1),
        }
    }

    /// Proposes a publish's batch if the node leads, parks it if the replica can take no more now,
    /// and otherwise redirects its publisher.
    fn publish<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        peer: PeerId,
        lane: LaneId,
        bytes: &[u8],
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let limit = usize::try_from(wire::BATCH_BOUND).unwrap_or(usize::MAX);
        // The transport checked the batch's CRC where the wire says it carries one.
        let parsed = if Bus::embedded_checksum(Kind::Publish).is_some() {
            Batch::parse_verified(bytes, limit)
        } else {
            Batch::parse(bytes, limit)
        };
        let Ok(batch) = parsed else {
            return;
        };
        let key = (batch.stream(), batch.producer(), batch.sequence());
        if !self.replica.is_leader() {
            self.redirect(endpoint, peer, key);
            return;
        }
        if self.owed.contains_key(&(peer, key)) {
            // Proposed already, its acknowledgement owed: the publisher sent it again (a resend
            // while it waited, or a lane's retry), and the acknowledgement the first earns answers
            // both, as Kafka's leader answers a duplicate of a batch in flight from its producer's
            // state before it appends (KIP-98). Proposed again, each copy was replicated and
            // written by every node before the part recognised it.
            self.stats.duplicates = self.stats.duplicates.saturating_add(1);
            return;
        }
        // One the part has taken already (applied, so committed: the publisher did not hear its
        // acknowledgement before it sent it again) is answered from the part's sessions as its
        // apply answered it, never proposed again, as Kafka's leader answers a duplicate from its
        // producer's state before it appends (KIP-98). Only that verdict is taken here: a gap the
        // part sees now may be a proposal still in flight.
        let (first, count) = (u64::from(batch.sequence()), u64::from(batch.count()));
        if let Verdict::Again { base } = self
            .replica
            .machine()
            .node()
            .part()
            .judge(key.0, peer, key.1, first, count)
        {
            let (base, count) = base.map_or((0, 0), |base| (base, batch.count()));
            let ack = Ack {
                stream: key.0,
                producer: key.1,
                sequence: key.2,
                base,
                count,
                class: self.class.code(),
            };
            self.answer(endpoint, peer, Answer::Ack(ack.encode()));
            self.stats.duplicates = self.stats.duplicates.saturating_add(1);
            return;
        }
        if !self.parked.is_empty()
            || self.owed.len() >= self.owed_bound
            || !self.propose(peer, bytes, key)
        {
            // The publisher waits, its lane held, behind what the group has yet to commit.
            self.park(endpoint, peer, lane, bytes);
        }
    }

    /// Proposes `batch`, owing `peer` its acknowledgement; whether the core took it. A core that
    /// holds as many uncommitted bytes as it may refuses it, and its publisher waits.
    fn propose(&mut self, peer: PeerId, batch: &[u8], key: Owed) -> bool {
        // The entry states the identity that published it: every replica judges the batch by that
        // identity's sessions (`group`, doc 34 P6). One the node's memory cannot hold is proposed
        // again once the window gave memory back ([`Member::relieve_window`]); parked without it,
        // it waited for memory the window, holding all it may, never gave.
        let mut proposed = self
            .replica
            .propose(peer.to_le_bytes().to_vec(), batch.to_vec());
        if matches!(proposed, Err(ReplicaError::Budget(_))) && self.relieve_window() {
            proposed = self
                .replica
                .propose(peer.to_le_bytes().to_vec(), batch.to_vec());
        }
        if proposed.is_err() {
            return false;
        }
        self.owed.insert((peer, key), Instant::now());
        self.stats.proposed = self.stats.proposed.saturating_add(1);
        true
    }

    /// Gives the replica memory at the window's expense: the window holds what the node's memory
    /// holds, and gives it back to what is live (`Node::relieve`: its warm segments, then its
    /// oldest batches until a segment goes back), as it does for the transport's frames. Whether
    /// it did.
    fn relieve_window(&mut self) -> bool {
        let relieved = self.replica.machine_mut().node_mut().relieve();
        if relieved {
            self.stats.relieved = self.stats.relieved.saturating_add(1);
        }
        relieved
    }

    /// Parks a publish the replica cannot take yet, holding its publisher's lane.
    fn park<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        peer: PeerId,
        lane: LaneId,
        bytes: &[u8],
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        // The transport reads nothing more from a held lane, but what it read before the hold
        // still comes: it parks behind, in order, never dropped. A publish the node cannot keep
        // (no memory, or a lane it cannot hold) is answered with the leader, sent again by its
        // publisher.
        let mut batch = Vec::new();
        if batch.try_reserve_exact(bytes.len()).is_err()
            || self.parked.try_reserve(1).is_err()
            || endpoint.hold_lane(peer, lane).is_err()
        {
            if let Ok(parsed) = Batch::parse_verified(bytes, bytes.len()) {
                let key = (parsed.stream(), parsed.producer(), parsed.sequence());
                self.redirect(endpoint, peer, key);
            }
            return;
        }
        batch.extend_from_slice(bytes);
        self.parked.push_back(Parked {
            peer,
            lane,
            batch,
            parked_at: Instant::now(),
        });
        self.stats.parked = self.stats.parked.saturating_add(1);
    }

    /// Proposes the parked publishes the replica can take now, letting their lanes go.
    fn unpark<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        // Bounded: each turn takes one parked publish out, or stops.
        while self.owed.len() < self.owed_bound {
            let Some(parked) = self.parked.front() else {
                break;
            };
            let limit = usize::try_from(wire::BATCH_BOUND).unwrap_or(usize::MAX);
            let key = Batch::parse_verified(&parked.batch, limit)
                .map(|batch| (batch.stream(), batch.producer(), batch.sequence()))
                .ok();
            let (peer, lane) = (parked.peer, parked.lane);
            let taken = match key {
                // A copy of one proposed since it parked is answered with the first.
                Some(key) if self.replica.is_leader() && self.owed.contains_key(&(peer, key)) => {
                    self.stats.duplicates = self.stats.duplicates.saturating_add(1);
                    true
                }
                Some(key) if self.replica.is_leader() => {
                    let batch = parked.batch.clone();
                    let parked_at = parked.parked_at;
                    if !self.propose(peer, &batch, key) {
                        // The group has yet to commit enough: the publisher waits on.
                        break;
                    }
                    self.stages.parked.record(nanos(parked_at.elapsed()));
                    true
                }
                Some(key) => {
                    self.redirect(endpoint, peer, key);
                    true
                }
                None => true,
            };
            if taken {
                self.parked.pop_front();
                let answered = self.answers.get(&peer).is_none_or(VecDeque::is_empty);
                let parked = self.parked.iter().any(|p| p.peer == peer && p.lane == lane);
                if answered && !parked {
                    let _ = endpoint.release_lane(peer, lane);
                }
            }
        }
    }

    /// Tells `peer` its publish `key` was not taken here, naming the leader the node knows.
    fn redirect<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId, key: Owed)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let (stream, producer, sequence) = key;
        let redirect = Redirect {
            stream,
            producer,
            sequence,
            leader: self.leader(),
        };
        self.answer(endpoint, peer, Answer::Redirect(redirect.encode()));
        self.stats.redirected = self.stats.redirected.saturating_add(1);
    }

    /// Sends `answer` to the publisher `peer`, or keeps it, in order, while the publisher's lane has
    /// no room or answers wait before it, holding the publisher's publishes meanwhile. A publisher
    /// whose connection is gone takes nothing: it sends again what it did not hear, on the next
    /// connection, and the part knows each batch it took (doc 34 P6).
    fn answer<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId, answer: Answer)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let waiting = self
            .answers
            .get(&peer)
            .is_some_and(|queue| !queue.is_empty());
        if !waiting {
            match endpoint.send_frame(peer, LANE, answer.kind(), answer.bytes()) {
                Ok(()) => return,
                Err(Refusal::LaneFull) => {}
                Err(_) => {
                    self.stats.unanswered = self.stats.unanswered.saturating_add(1);
                    return;
                }
            }
        }
        let queue = self.answers.entry(peer).or_default();
        if queue.try_reserve(1).is_err() {
            // No memory for it: the publisher sends again what it did not hear within its period.
            self.stats.unanswered = self.stats.unanswered.saturating_add(1);
            return;
        }
        queue.push_back(answer);
        self.stats.answers_waited = self.stats.answers_waited.saturating_add(1);
        // Nothing more of the publisher's is read until its answers have gone.
        let _ = endpoint.hold_lane(peer, LANE);
    }

    /// Sends the answers waiting for `peer`'s lane while it has room; once none waits, and no
    /// publish of its is parked, its publishes are read again.
    fn flush_answers<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let Some(queue) = self.answers.get_mut(&peer) else {
            return;
        };
        // Bounded: each turn sends one waiting answer or stops.
        while let Some(front) = queue.front() {
            match endpoint.send_frame(peer, LANE, front.kind(), front.bytes()) {
                Ok(()) => {
                    queue.pop_front();
                }
                Err(Refusal::LaneFull) => return,
                Err(_) => {
                    let gone = u64::try_from(queue.len()).unwrap_or(u64::MAX);
                    self.stats.unanswered = self.stats.unanswered.saturating_add(gone);
                    queue.clear();
                }
            }
        }
        self.answers.remove(&peer);
        if !self.parked.iter().any(|parked| parked.peer == peer) {
            let _ = endpoint.release_lane(peer, LANE);
        }
    }

    /// Sends one Raft message to its member, or keeps it in the member's outbox while the lane is
    /// full. One the node drops (no connection, or the outbox at its bound) is reported unreachable
    /// to the replica, as raft's owner reports a message its transport did not deliver
    /// (`MsgUnreachable`): a leader takes what it sent the member ahead of its answers for lost and
    /// probes it again from what it holds. Untold, a leader whose last appends were dropped waited
    /// for their answers, the member answering its heartbeats and nothing more being sent.
    fn send_message<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, message: &Message)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let to = message.to;
        self.encoded.clear();
        message.encode(&mut self.encoded);
        let waiting = self.outbox.get(&to).is_some_and(|w| !w.is_empty());
        if !waiting {
            match endpoint.send_frame(to, LANE, Kind::Raft, &self.encoded) {
                Ok(()) => return,
                Err(Refusal::LaneFull) => {}
                // No connection yet, or the member is gone.
                Err(_) => {
                    self.stats.dropped_messages = self.stats.dropped_messages.saturating_add(1);
                    let _ = self.replica.report_unreachable(to);
                    return;
                }
            }
        }
        let bound = self.outbox_bound;
        let queue = self.outbox.entry(to).or_default();
        if queue.len() >= bound {
            self.stats.dropped_messages = self.stats.dropped_messages.saturating_add(1);
            let _ = self.replica.report_unreachable(to);
            return;
        }
        queue.push_back(self.encoded.clone());
    }

    /// Sends what waited in `peer`'s outbox while its lane has room.
    fn flush_outbox<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let Some(queue) = self.outbox.get_mut(&peer) else {
            return;
        };
        // Bounded: each turn sends one waiting frame or stops.
        while let Some(front) = queue.front() {
            match endpoint.send_frame(peer, LANE, Kind::Raft, front) {
                Ok(()) => {
                    queue.pop_front();
                }
                Err(Refusal::LaneFull) => return,
                Err(_) => {
                    self.stats.dropped_messages = self
                        .stats
                        .dropped_messages
                        .saturating_add(u64::try_from(queue.len()).unwrap_or(u64::MAX));
                    queue.clear();
                    let _ = self.replica.report_unreachable(peer);
                    return;
                }
            }
        }
    }

    /// Keeps the stream told which nodes the group has: its configuration's other members.
    fn pairs(&mut self) {
        let mut now: Vec<u64> = self.replica.peers().collect();
        now.sort_unstable();
        now.dedup();
        for peer in &now {
            if self.attached.binary_search(peer).is_err() && self.liveness.attach(*peer).is_ok() {
                let suspected = self.liveness.trust(*peer) == Some(Trust::Suspected);
                let _ = if suspected {
                    self.replica.suspect(*peer)
                } else {
                    self.replica.trust(*peer)
                };
            }
        }
        for peer in &self.attached {
            if now.binary_search(peer).is_err() {
                let _ = self.liveness.detach(*peer);
            }
        }
        self.attached = now;
    }

    /// The owner's period, restated as the stream's moves (`Replica::set_quiet`): the least interval
    /// this node's heartbeats go at, the cadence its own stream wakes it at. A commit that moved
    /// alone rides any write that comes within it, as a ledger owner's rides the writes within its
    /// period (focal F17), and is written alone only once a period has passed with no write; at
    /// zero it was written alone the moment no write was out, a second log frame for each entry at
    /// a steady rate. A deferred commit holds no answer back (`RawNode::defer_commit` states the
    /// durable commit in them), so the period costs the leader's activity while its followers state
    /// the durable commit, not an acknowledgement's latency. Until the stream schedules a heartbeat,
    /// and for a member with no peer (it decides alone and writes its commit with its entries), the
    /// period stays what it was.
    fn restate_period(&mut self) {
        let least = self
            .attached
            .iter()
            .filter_map(|peer| {
                self.liveness
                    .report(*peer)
                    .and_then(|report| report.sending)
            })
            .min();
        if let Some(period) = least {
            self.replica.set_quiet(period);
        }
    }

    /// The group's timing from what the stream measured, each pair charged the group's election.
    fn measure(&mut self) {
        if let Ok(Some(span)) = self.replica.measure(&self.liveness) {
            for peer in &self.attached {
                let _ = self.liveness.set_election(*peer, span.election);
            }
        }
    }

    /// Gives the stream what the liveness write made durable, if the log has answered it.
    fn written(&mut self, now: Instant) {
        if let Some((pending, started)) = &self.liveness_write
            && let Some(answer) = pending.poll()
        {
            let started = *started;
            self.liveness_write = None;
            match answer {
                Ok(()) => {
                    let done = self.now_ns(now);
                    self.liveness.on_durable(LiveWrite::Liveness, started, done);
                }
                // The device failed: no heartbeat is proved again, so the peers suspect the node.
                Err(_) => self.liveness_failed = true,
            }
        }
    }

    /// Carries out what the stream asked: its write, its heartbeats, its changes, each change told.
    fn act_on_liveness<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        waker: &Waker,
        now: Instant,
        tell: &mut impl FnMut(&GroupEvent),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if std::mem::take(&mut self.asked.flush)
            && self.liveness_write.is_none()
            && !self.liveness_failed
        {
            let started = self.now_ns(now);
            match self.log.submit_waking(
                LIVENESS_GROUP,
                LogClass::Latency,
                Update::default(),
                waker.clone(),
            ) {
                Ok(pending) => {
                    self.liveness_write = Some((pending, started));
                    self.stats.liveness.writes = self.stats.liveness.writes.saturating_add(1);
                }
                Err(_) => self.liveness_failed = true,
            }
        }
        let heartbeats = std::mem::take(&mut self.asked.heartbeats);
        for (peer, message) in &heartbeats {
            // A heartbeat that finds no connection or a full lane is a late one: the stream
            // judges lateness.
            let _ = endpoint.send_frame(*peer, LIVENESS_LANE, Kind::Heartbeat, message);
        }
        self.asked.heartbeats = heartbeats;
        self.asked.heartbeats.clear();
        let changes = std::mem::take(&mut self.asked.changes);
        for change in &changes {
            let peer = change.peer();
            let _ = match change {
                Change::Suspected(suspicion) => {
                    self.stats.suspected = self.stats.suspected.saturating_add(1);
                    tell(&GroupEvent::Suspected(*suspicion));
                    self.replica.suspect(peer)
                }
                Change::Trusted { .. } => {
                    tell(&GroupEvent::Trusted { peer });
                    self.replica.trust(peer)
                }
                Change::Restarted { .. } => {
                    tell(&GroupEvent::Restarted { peer });
                    self.replica.restarted(peer)
                }
            };
        }
        self.asked.changes = changes;
        self.asked.changes.clear();
    }

    /// One turn of the node's work after the events it took: the liveness stream's, a drive of
    /// the replica, what it gave out sent and acknowledged. `waker` is the shard task's, which the
    /// log wakes when a write it was given is durable; `tell` is told each [`GroupEvent`] the turn
    /// came to.
    pub fn turn<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        waker: &Waker,
        now: Instant,
        tell: &mut impl FnMut(&GroupEvent),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        self.written(now);
        self.pairs();
        self.measure();
        let clock = self.now_ns(now);
        self.liveness.poll(clock, &mut self.asked);
        self.restate_period();
        self.act_on_liveness(endpoint, waker, now, tell);
        // Bounded: each drive takes at most one `Ready`, and no more than the store's depth are out
        // until the log answers, so a turn drives at most the depth and once more; `more` is the
        // replica's word that driving again now does more without waiting for the log, and what is
        // left is the next turn's (`Member::due`).
        self.due = false;
        for _ in 0..self.drives {
            self.out.clear();
            let driven = match self.replica.drive(clock, waker, &mut self.out) {
                Ok(driven) => driven,
                // A fenced replica takes no call until it is reopened from what is durable: the
                // node ends (`Member::fenced`) rather than hold, unanswered, what it owes.
                Err(ReplicaError::Fenced(_)) => {
                    self.fenced = true;
                    break;
                }
                // Refused or stalled, nothing changed: the next turn drives again.
                Err(_) => {
                    self.stats.refused_drives = self.stats.refused_drives.saturating_add(1);
                    break;
                }
            };
            self.stats.drives = self.stats.drives.saturating_add(1);
            if let Some((submitted, answered)) = driven.flushed {
                self.liveness
                    .on_durable(LiveWrite::Log, submitted, answered);
            }
            self.give_out(endpoint);
            self.due = driven.more;
            if !driven.more {
                break;
            }
        }
        let log = self.replica.core().raft.log();
        if let Ok(last) = log.last_index() {
            // The first index no write was issued for: every one before it was given a write.
            let issued = log.unstable().issued().saturating_sub(1).min(last);
            let reached = Reached {
                issued,
                persisted: log.persisted(),
                committed: log.committed(),
            };
            let leads = self.replica.is_leader();
            self.stages.observe(leads, last, reached, Instant::now());
        }
        self.compact_behind_window(waker, clock);
        self.settle_leadership(endpoint);
        self.unpark(endpoint);
        let term = self.replica.term();
        let leader = self.replica.leader();
        if term != self.seen_term || leader != self.seen_leader {
            tell(&GroupEvent::Term { term, leader });
            self.seen_leader = leader;
        }
        if term > self.seen_term {
            if self.seen_term != 0 {
                self.stats.terms = self
                    .stats
                    .terms
                    .saturating_add(term.saturating_sub(self.seen_term));
            }
            self.seen_term = term;
        }
        if let Some(woke) = self.woke.take() {
            let busy = u64::try_from(woke.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.stats.busy_most_ns = self.stats.busy_most_ns.max(busy);
        }
    }

    /// Lets the log go of what the part's window let go (doc 34 P10: the group's log holds what the
    /// window holds), once the window has emptied one of its segments: it gives memory back a
    /// segment at a time (`NodeConfig::SEGMENT_BYTES`), and a compaction makes the part's image
    /// durable and moves the log's start to its point, freeing what the log held for the batches
    /// that segment held. hyper-log retains a group's entries only until its owner compacts (two
    /// of its cadences and what may be uncommitted, `hyper_log::Config::group_bytes`; its file's
    /// bound comes first) and refuses the group's writes past that, so a part whose log was never
    /// compacted took nothing more once it filled: on fedora (2026-10-19) every closed-loop round
    /// stopped for good 17 to 28 seconds in, its leader's publishes parked and its last window
    /// unanswered, as CI's sweep had. A compaction that cannot start now (a start already out, or
    /// no slot for its write) is asked again at the next turn, until it starts or the log holds
    /// nothing the window let go.
    fn compact_behind_window(&mut self, waker: &Waker, clock: u64) {
        let emptied = self.replica.machine().node().segments_emptied();
        if emptied != self.emptied_seen {
            self.emptied_seen = emptied;
            self.compact_wanted = true;
        }
        if !self.compact_wanted {
            return;
        }
        let started = Instant::now();
        let compacted = self.replica.compact(0, clock, waker);
        let took = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.stats.compaction_ns = self.stats.compaction_ns.saturating_add(took);
        match compacted {
            Ok(true) => {
                self.compact_wanted = false;
                self.stats.compactions = self.stats.compactions.saturating_add(1);
            }
            Ok(false) => {
                // Nothing the window let go lies past the log's start: nothing to do until the
                // window empties another segment.
                let released = self.replica.machine().node().released().0;
                let start = self
                    .replica
                    .core()
                    .store()
                    .first_index()
                    .map_or(u64::MAX, |first| first.saturating_sub(1));
                if released <= start {
                    self.compact_wanted = false;
                }
            }
            Err(ReplicaError::Fenced(_)) => self.fenced = true,
            Err(_) => {
                self.stats.refused_compactions = self.stats.refused_compactions.saturating_add(1);
            }
        }
    }

    /// Sends what a drive gave out, and acts on what its state machine answered.
    fn give_out<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let messages = std::mem::take(&mut self.out.messages);
        for message in &messages {
            self.send_message(endpoint, message);
        }
        self.out.messages = messages;
        if self.replica.machine_mut().take_relieved() {
            endpoint.retry_reserving();
        }
        let answers = std::mem::take(&mut self.out.answers);
        for applied in &answers {
            self.node_mut().deliver(endpoint, applied.stream);
            self.acknowledge(endpoint, applied);
        }
        self.out.answers = answers;
    }

    /// Sends the acknowledgement owed for `applied`, if the node owed one.
    fn acknowledge<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, applied: &Applied)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let key = (applied.stream, applied.producer, applied.sequence);
        let peer = applied.owner;
        let Some(proposed_at) = self.owed.remove(&(peer, key)) else {
            return;
        };
        let (base, count, class) = match applied.outcome {
            Outcome::Appended { base } => (base, applied.count, self.class.code()),
            Outcome::Again { base: Some(base) } => (base, applied.count, self.class.code()),
            // Taken before, its offsets no longer said: an acknowledgement of no records.
            Outcome::Again { base: None } => (0, 0, self.class.code()),
            Outcome::OutOfOrder { expected } => (expected, 0, Ack::OUT_OF_ORDER),
            Outcome::Refused(_) => (0, 0, Ack::REFUSED),
        };
        let ack = Ack {
            stream: applied.stream,
            producer: applied.producer,
            sequence: applied.sequence,
            base,
            count,
            class,
        };
        self.answer(endpoint, peer, Answer::Ack(ack.encode()));
        self.stats.acked = self.stats.acked.saturating_add(1);
        self.stages.acked.record(nanos(proposed_at.elapsed()));
    }

    /// A node that stopped leading redirects every publisher it owed: the group may or may not have
    /// committed their batches, and the part recognises those it took when they come again.
    fn settle_leadership<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let leading = self.replica.is_leader();
        if self.leading && !leading {
            let owed: Vec<(PeerId, Owed)> = self.owed.drain().map(|(owed, _)| owed).collect();
            for (peer, key) in owed {
                self.redirect(endpoint, peer, key);
            }
        }
        self.leading = leading;
    }

    /// Tells the stream how late the owner came to its wake: a wait begun before the stream's wake
    /// and ended at or past it, whatever ended it (a deadline, or anything that arrived after the
    /// wake), told before anything the wait brought is fed. The stream's timer granularity `G` is
    /// made of these, and until it is measured it builds no estimator and takes no heartbeat
    /// (`hyper_liveness::Liveness::on_wait`, `Refusal::Unmeasured`): an owner that never tells
    /// its waits elects no leader.
    pub fn waited(&mut self, began: Instant, woke: Instant) {
        self.woke = Some(woke);
        let Some(at) = self.liveness.wake() else {
            return;
        };
        let (began, woke) = (self.now_ns(began), self.now_ns(woke));
        if began < at && woke >= at {
            self.liveness.on_wait(at, woke);
        }
    }

    /// When the node next has work though nothing arrives: at once where a turn left its replica
    /// more to do, and otherwise the replica's deadline or the stream's wake, the sooner.
    pub fn deadline(&self) -> Option<Instant> {
        if self.due {
            return Some(self.origin);
        }
        let at = |ns: u64| self.origin.checked_add(Duration::from_nanos(ns));
        let replica = self.replica.deadline().and_then(at);
        let stream = self.liveness.wake().and_then(at);
        match (replica, stream) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marks(issued: u64, persisted: u64, committed: u64) -> Reached {
        Reached {
            issued,
            persisted,
            committed,
        }
    }

    /// A run waits for room in the pipeline apart from its write: issued when a write takes it,
    /// durable when that write is, so the two stages split a run's time to its disk.
    #[test]
    fn a_leader_times_a_runs_wait_for_its_write_apart_from_the_write() {
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let ms = |ns: u64| ns / 1_000_000;
        let mut stages = Stages::new(4);
        // Two runs: the first issued at once, the second waiting for room.
        stages.observe(true, 10, marks(10, 0, 0), at(0));
        stages.observe(true, 20, marks(10, 0, 0), at(1));
        assert_eq!(stages.issued.count(), 1);
        assert_eq!(stages.issued.quantile(0), Some(0));
        // The first durable at 3 ms; the second issued then, after waiting 2 ms.
        stages.observe(true, 20, marks(20, 10, 10), at(3));
        assert_eq!(stages.issued.count(), 2);
        assert_eq!(ms(stages.issued.quantile(1_000_000).unwrap()), 2);
        assert_eq!(ms(stages.persisted.quantile(1_000_000).unwrap()), 3);
        // Durable 4 ms after its write was issued, 6 after its append.
        stages.observe(true, 20, marks(20, 20, 20), at(7));
        assert_eq!(stages.persisted.count(), 2);
        assert_eq!(ms(stages.persisted.quantile(1_000_000).unwrap()), 6);
        assert!(stages.unissued.is_empty() && stages.unpersisted.is_empty());
    }

    /// Each run of appended entries is timed once at each stage, at the end of the turn that saw it
    /// reach the stage; a run past the bound joins the newest and is timed from its start; a node
    /// that stops leading times nothing it appended, and leading again starts from its last index.
    #[test]
    fn a_leader_times_each_run_of_its_entries_to_durable_and_to_committed() {
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let ms = |ns: u64| ns / 1_000_000;
        let mut stages = Stages::new(2);
        stages.observe(true, 10, marks(0, 0, 0), at(0));
        // Two runs, 1..=10 and 11..=12; the first durable 3 ms after it was appended.
        stages.observe(true, 12, marks(10, 10, 0), at(3));
        assert_eq!(stages.persisted.count(), 1);
        assert_eq!(ms(stages.persisted.quantile(1_000_000).unwrap()), 3);
        // Two runs wait for their commit, so a third joins 11..=12 and is timed from its start;
        // the first is committed only after the third is appended, so it took the bound's room.
        stages.observe(true, 14, marks(10, 10, 0), at(5));
        assert_eq!(stages.uncommitted.len(), 2);
        assert_eq!(stages.uncommitted.back(), Some(&(14, at(3))));
        // The leader's log had room for it: two runs wait there.
        assert_eq!(stages.unpersisted.back(), Some(&(14, at(5))));
        // A run is timed only once its last index has reached the stage.
        stages.observe(true, 14, marks(13, 13, 13), at(8));
        assert_eq!(stages.persisted.count(), 2);
        assert_eq!(stages.committed.count(), 1);
        assert_eq!(ms(stages.committed.quantile(1_000_000).unwrap()), 8);
        stages.observe(true, 14, marks(14, 14, 14), at(9));
        assert_eq!(stages.persisted.count(), 3);
        assert_eq!(stages.committed.count(), 2);
        assert_eq!(ms(stages.committed.quantile(0).unwrap()), 6);
        assert!(stages.unpersisted.is_empty() && stages.uncommitted.is_empty());
        // A run durable and committed by the end of the turn that appended it took no time.
        stages.observe(true, 15, marks(15, 15, 15), at(9));
        assert_eq!(stages.committed.count(), 3);
        assert_eq!(stages.committed.quantile(0), Some(0));
        // A follower's appends are its leader's to time.
        stages.observe(true, 16, marks(15, 15, 15), at(10));
        stages.observe(false, 30, marks(30, 30, 30), at(20));
        assert!(stages.unpersisted.is_empty() && stages.uncommitted.is_empty());
        assert_eq!(stages.committed.count(), 3);
        // Leading again, only what it appends as leader is timed.
        stages.observe(true, 31, marks(30, 30, 30), at(21));
        stages.observe(true, 31, marks(31, 31, 31), at(22));
        assert_eq!(stages.committed.count(), 4);
        assert_eq!(stages.uncommitted.len(), 0);
        assert_eq!(ms(stages.committed.quantile(500_000).unwrap()), 1);
    }
}
