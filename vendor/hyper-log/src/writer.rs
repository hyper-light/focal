//! The writer's rules (mantle docs/design/raft-log.md §3, §5): how a batch is ordered and laid
//! into a frame, where the frame goes, what a sweep of the tail copies, and what a durable frame
//! changes. The owner (`owner.rs`) runs them; they read and change the state it holds.
//!
//! It runs the chunk store's group-commit loop (chunk-store.md §4): it takes every update that
//! arrived while the last flush ran, lays them into one frame, writes the frame, flushes the
//! file once, and publishes the updates to readers. It answers them once a later durable record
//! confirms that flush: the next frame's persist record, or a confirmation written on its own when
//! no frame follows at once (raft-log.md §6). One frame is one batch, so a valid frame with a
//! later sequence proves an earlier one was flushed, which is what lets recovery tell a torn tail
//! from damage (§6).
//!
//! Segments are freed oldest first, so the live ones are always a run of incarnations that each
//! frame's tail names. A tail with no live piece left is freed at once. When updates are waiting
//! and free segments are short, the writer sweeps the tail: the frame first carries copies of
//! every live piece of the tail, and names the next segment as the tail, the cleaning of a
//! log-structured file system applied to the end of a log [RO92]. A group's live entries run
//! unbroken from its start to its last, and the pieces of a record die from its front by a start
//! and from its back by a replacement, so the live part of any record is one run and its copy is
//! no larger than the record. The copies of a whole tail therefore fit in one frame, and a sweep
//! always completes in the frame that begins it. A freed segment is reused only once a durable
//! frame names a tail past it; the last free segment is kept for a frame that does so, which is
//! how the log never runs out of room to free room.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use crate::codec::Writer as Payload;
use crate::format::{self, Owned, Placed, Record};
use crate::state::{
    self, DAMAGED_BYTES, Group, HARD_STATE_BYTES, PROPOSAL_EXTRA, Place, RELEASED_BYTES,
    START_BYTES, Slot, State, UNCERTAIN_BYTES, entry_bytes, resolves,
};
use crate::ticket::Ticket;
use crate::{Class, Config, LogError, Marks, Params, Update};

/// An update on its way to a frame.
pub(crate) struct Submission {
    pub(crate) group: u128,
    pub(crate) update: Update,
    pub(crate) marks: Marks,
    /// What it holds of the queue's byte bound, and what the writer's fair queue charges it.
    pub(crate) bytes: u64,
    pub(crate) class: Class,
    /// Where the writer's fair queue placed it when taken (mantle docs/design/raft-log.md §3).
    pub(crate) tags: Tags,
    /// Where its admission and its answer go.
    pub(crate) ticket: Ticket,
    /// Whether its caller waits to hear it admitted: a caller that waits only for its answer is
    /// not told.
    pub(crate) admit: bool,
    /// Whether it comes from its group's handle (`group.rs`), which keeps the entries' bytes:
    /// they go back with its answer rather than into the log's cache.
    pub(crate) handle: bool,
    /// For a handle's write laid into a frame, where its entries' terms and lengths are in the
    /// frame's list (`owner::Buffers::lens`), the entries having gone back with its answer.
    pub(crate) lens: (usize, usize),
    /// Whether its caller waits on its ticket from the moment it sends it until its answer, so
    /// that its frame's I/O may be given to it to do (`owner::Owner::start`).
    pub(crate) waits: bool,
    /// For a handle's write, how many of the handle's writes had been refused when it was sent
    /// (`GroupLog`): a write sent before its handle heard of a refusal is refused
    /// [`LogError::Behind`], so the group's writes stay in the order submitted. Zero otherwise.
    pub(crate) epoch: u64,
    /// When it was submitted, which its commit wait is timed from (`stats::Tally::waited`); a
    /// restore's, when the log opened.
    pub(crate) submitted: Instant,
}

/// Where the writer's fair queue placed a submission (mantle docs/design/raft-log.md §3).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Tags {
    /// Its place in the order the writer took submissions.
    pub(crate) seq: u64,
    /// Its start tag in start-time fair queueing: the larger of the virtual time when it was
    /// taken and its group's last finish tag, in charged bytes [SFQ96 §2 eq. 4].
    pub(crate) start: u128,
    /// The frame that first passed it over for room, if one has.
    pub(crate) passed: Option<u64>,
}

/// The order a frame considers a submission in: its tier (passed over, then each class), then
/// when it was passed over and its class, or its start tag, then when it was taken.
pub(crate) type Key = (u8, u128, u64);

fn key(s: &Submission) -> Key {
    let class = match s.class {
        Class::Latency => 1u8,
        Class::Normal => 2,
        Class::Background => 3,
    };
    match s.tags.passed {
        // Those one frame passed over go by class among themselves, all before any a later
        // frame passes over.
        Some(frame) => (0, u128::from(frame) << 2 | u128::from(class), s.tags.seq),
        None => (class, s.tags.start, s.tags.seq),
    }
}

/// Puts the backlog in the order a frame considers it (mantle docs/design/raft-log.md §3). A
/// group's update never sorts before one of its own taken earlier: it takes the later key of the
/// two, as Tectonic gives traffic that borrows another TrafficGroup's resources the lower of their
/// classes [research/01 §1.11], so a group's updates stay in the order submitted.
///
/// mantle sorts twice with a stable sort, which allocates; here each sort is unstable over keys
/// made unique by position, which orders exactly as the stable sort does, in `keyed` and `last`,
/// which the owner keeps between frames.
pub(crate) fn order(
    batch: &mut VecDeque<Submission>,
    keyed: &mut Vec<(Key, u64, Submission)>,
    last: &mut HashMap<u128, Key>,
) {
    keyed.clear();
    last.clear();
    // By when each was taken, ties (the restores at open, all taken at once) in batch order.
    keyed.extend(
        (0u64..)
            .zip(batch.drain(..))
            .map(|(at, s)| ((0, u128::from(s.tags.seq), 0), at, s)),
    );
    keyed.sort_unstable_by_key(|(k, at, _)| (*k, *at));
    for (at, (k, place, s)) in (0u64..).zip(keyed.iter_mut()) {
        let own = key(s);
        *k = match last.get(&s.group) {
            Some(&before) if before > own => before,
            _ => own,
        };
        last.insert(s.group, *k);
        *place = at;
    }
    // A group's updates of one key keep the order they were taken in.
    keyed.sort_unstable_by_key(|(k, at, _)| (*k, *at));
    batch.extend(keyed.drain(..).map(|(_, _, s)| s));
}

/// Where an update's records went in the payload. An entry's place follows from its record's:
/// the record's header, then each entry before it (`format::put_entries`); a proposal's from the
/// first proposal's record, each proposal's record following the one before.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Placement {
    start: Option<usize>,
    entries: Option<usize>,
    hard: Option<usize>,
    proposals: Option<usize>,
    uncertain: Option<usize>,
    damaged: Option<usize>,
    /// The tag after each entry's and proposal's bytes in a sealed log: what an entry's or
    /// proposal's stride counts beside its header and bytes.
    tag: usize,
    released: Option<usize>,
}

/// A live piece of the tail copied into the payload.
#[derive(Debug)]
pub(crate) enum Moved {
    Entry { group: u128, index: u64, at: usize },
    Hard { group: u128, at: usize },
    Start { group: u128, at: usize },
    Proposal { group: u128, index: u64, at: usize },
    Uncertain { group: u128, at: usize },
    Damaged { group: u128, at: usize },
    Released { group: u128, at: usize },
}

/// A sweep of the tail laid into the payload.
#[derive(Debug)]
pub(crate) struct Sweep {
    pub(crate) slot: u32,
    pub(crate) moved: Vec<Moved>,
    /// The incarnation of the segment after the tail, which the frame names as the tail.
    pub(crate) next_tail: u64,
}

/// Where a frame goes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Target {
    pub(crate) slot: u32,
    pub(crate) incarnation: u64,
    pub(crate) nonce: u64,
    /// File offset of the frame.
    pub(crate) offset: u64,
    /// Bytes of the frame, padded.
    pub(crate) frame_len: u64,
    /// Whether the frame opens its segment, whose header it writes first.
    pub(crate) opens: bool,
    /// Bytes laid before the payload: a sealed log's key record, when the frame begins a writer
    /// session in a segment it continues ([`format::KEY_RECORD_LEN`]), or none.
    pub(crate) prefix: u64,
}

pub(crate) fn block(p: &Params) -> Result<u64, LogError> {
    u64::try_from(p.align.get()).map_err(|_| LogError::Config("block"))
}

pub(crate) fn slot_start(config: &Config, slot: u32) -> Result<u64, LogError> {
    u64::from(slot)
        .checked_mul(config.segment_bytes)
        .and_then(|s| s.checked_add(crate::recover::persist_area(config)))
        .ok_or(LogError::Damaged("an offset past u64"))
}

/// What a submission's frame makes durable for its group, as its persist record says it.
pub(crate) fn persisted(s: &Submission) -> format::Persisted {
    let u = &s.update;
    format::Persisted {
        group: s.group,
        hard_state: u.hard_state,
        start: u.start,
        entries: u.entries.as_ref().map(|e| format::Written {
            first: e.first,
            count: u64::try_from(e.entries.len()).unwrap_or(u64::MAX),
            term: e.entries.last().map_or(0, |x| x.term),
        }),
        uncertain: s.marks.uncertain,
        removed: u.remove,
        proposals: !u.proposals.is_empty(),
        damaged: s.marks.damaged,
    }
}

/// Bytes a submission's records take in a payload: its update's and its marks'.
pub(crate) fn submission_len(
    group: u128,
    update: &Update,
    marks: Marks,
    tag: usize,
) -> Option<usize> {
    if marks.damaged {
        return format::encoded_len(&Record::Damaged { group }, tag);
    }
    let len = update_len(group, update, tag)?;
    match marks.uncertain {
        Some(mark) => len.checked_add(format::encoded_len(
            &Record::Uncertain { group, mark },
            tag,
        )?),
        None => Some(len),
    }
}

/// What a submission whose records take `len` payload bytes holds of the queue's byte bound
/// until it is answered: those bytes, record and entry headers included, and its group's row in
/// the frame's persist record. An empty entry costs its header and an update of no records its
/// row, so a flood of them fills the bound as surely as large ones do (mantle audit S03).
pub(crate) fn charge(len: usize) -> Option<u64> {
    u64::try_from(len.checked_add(format::PERSIST_GROUP_LEN)?).ok()
}

pub(crate) fn incarnation_of(state: &State, slot: u32) -> u64 {
    usize::try_from(slot)
        .ok()
        .and_then(|s| state.segments.incarnation.get(s))
        .copied()
        .unwrap_or(0)
}

pub(crate) fn nonce_of(state: &State, slot: u32) -> u64 {
    usize::try_from(slot)
        .ok()
        .and_then(|s| state.segments.nonce.get(s))
        .copied()
        .unwrap_or(0)
}

/// Segments a frame could open now: free ones a durable frame has released, and those the
/// file may still grow by: up to `max_segments`, and no further than its owner admitted
/// (`State::ceiling`, `crate::growth`).
pub(crate) fn usable_segments(state: &State, max_segments: u32) -> u64 {
    let max_segments = max_segments.min(state.ceiling);
    let free = state
        .segments
        .free
        .iter()
        .filter(|(_, after)| *after <= state.durable)
        .count();
    let slots = state.segments.incarnation.len();
    let growable = usize::try_from(max_segments)
        .unwrap_or(usize::MAX)
        .saturating_sub(slots);
    u64::try_from(free.saturating_add(growable)).unwrap_or(u64::MAX)
}

/// Frees tail segments that hold no live piece. The next frame names the new tail, and a
/// freed segment is reused only once that frame is durable.
pub(crate) fn drop_dead_tails(state: &mut State) {
    while state.segments.live.len() > 1 {
        let Some(&tail) = state.segments.live.front() else {
            return;
        };
        if state.live.of(tail).0 != 0 {
            return;
        }
        state.segments.live.pop_front();
        state.segments.free.push_back((tail, state.next_sequence));
    }
}

/// Whether the tail should be swept now: free segments are short, a segment follows the
/// tail, and sweeping it makes room: its copies fit one frame, and that frame takes less
/// than the tail's frames do.
pub(crate) fn sweepable(state: &State, p: &Params) -> Result<bool, LogError> {
    let usable = usable_segments(state, p.config.max_segments);
    let Some(&tail) = state.segments.live.front() else {
        return Ok(false);
    };
    if usable >= 2 || state.segments.live.len() < 2 {
        return Ok(false);
    }
    // The copies' bytes at most: every live piece, and a relocated record's header for
    // each, as if no two entries ran together.
    let (count, live_bytes) = state.live.of(tail);
    let run = format::encoded_len(
        &Record::Relocated {
            group: 0,
            first: 0,
            entries: &[],
        },
        p.tag,
    )
    .and_then(|len| u64::try_from(len).ok())
    .ok_or(LogError::Config("a relocated record's header"))?;
    let copies = count
        .checked_mul(run)
        .and_then(|h| h.checked_add(live_bytes));
    let room = u64::try_from(p.frame_room).unwrap_or(u64::MAX);
    let Some(copies) = copies.filter(|&c| c <= room) else {
        return Ok(false);
    };
    let frame = u64::try_from(format::FRAME_HEADER_LEN)
        .ok()
        .and_then(|h| h.checked_add(copies))
        .and_then(|len| p.align.up_u64(len))
        .ok_or(LogError::Damaged("a frame past u64"))?;
    // The sweep makes room if the segment after the tail holds nothing live, so that the
    // frame naming a later tail frees it too, or if its frame takes less than the
    // tail's frames. A tail copied into a segment of its own, as large as its own frames,
    // frees one segment and fills another: the writer once swept every segment in turn so,
    // while the updates waiting never fitted beside the copies. And a small live record
    // left in the oldest segment once held back every dead segment behind it: its copy was
    // as large as its frame, so it was never swept, and no frame larger than the head's
    // room went in again.
    // The segment after the tail, unless it is the head, which takes the next frame.
    let dead_behind = state.segments.live.len() > 2
        && state
            .segments
            .live
            .get(1)
            .is_some_and(|&slot| state.live.of(slot).0 == 0);
    Ok(dead_behind || frame < state.live.used(tail))
}

/// What the device reads for a sweep: the tail's segment, from its first frame to its end, and
/// the incarnation of the segment after it.
pub(crate) struct SweepRead {
    pub(crate) slot: u32,
    pub(crate) segment: crate::recover::Segment,
    pub(crate) offset: u64,
    pub(crate) end: u64,
    pub(crate) next_tail: u64,
}

/// Where the sweep of the tail reads.
pub(crate) fn sweep_read(state: &State, p: &Params) -> Result<SweepRead, LogError> {
    let slot = *state
        .segments
        .live
        .front()
        .ok_or(LogError::Damaged("no tail"))?;
    let next_tail = state
        .segments
        .live
        .get(1)
        .map(|&s| incarnation_of(state, s))
        .ok_or(LogError::Damaged("no segment after the tail"))?;
    let begin = slot_start(&p.config, slot)?;
    let end = begin
        .checked_add(p.config.segment_bytes)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let offset = begin
        .checked_add(block(p)?)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    Ok(SweepRead {
        slot,
        segment: crate::recover::Segment {
            log: p.id,
            incarnation: incarnation_of(state, slot),
            nonce: nonce_of(state, slot),
        },
        offset,
        end,
        next_tail,
    })
}

/// Lays copies of every live piece of the swept frames into the payload.
pub(crate) fn sweep(
    state: &State,
    p: &Params,
    read: &SweepRead,
    frames: &[crate::device::Swept],
    payload: &mut Payload,
    records: &mut u32,
) -> Result<Sweep, LogError> {
    let mut moved = Vec::new();
    for frame in frames {
        let copies = live_copies(state, read.slot, frame.base, &frame.records);
        for copy in &copies {
            let placed = format::put(payload, &copy.record(), p.tag)
                .ok_or(LogError::Damaged("a copy does not encode"))?;
            *records = records.saturating_add(1);
            copy.moved(placed, &mut moved);
        }
        if payload.len() > p.frame_room {
            return Err(LogError::Damaged("a tail's live pieces outgrow a frame"));
        }
    }
    Ok(Sweep {
        slot: read.slot,
        moved,
        next_tail: read.next_tail,
    })
}

/// Where the next frame of `payload_len` bytes goes: the head, or a segment opened for
/// it. The last free segment is kept for a frame that makes room, and while no segment is
/// free the head's room is too, so that the compaction or the tail's advance a full log
/// waits for always has somewhere to go (mantle docs/design/raft-log.md §5). `None` when no
/// segment can take it.
pub(crate) fn target(
    state: &State,
    p: &Params,
    payload_len: usize,
    makes_room: bool,
) -> Result<Option<Target>, LogError> {
    let block = block(p)?;
    let frame_len = frame_len(p, payload_len)?;
    let head = state.head;
    let head_end = slot_start(&p.config, head.slot)?
        .checked_add(p.config.segment_bytes)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let usable = usable_segments(state, p.config.max_segments);
    if head
        .offset
        .checked_add(frame_len)
        .is_some_and(|end| end <= head_end)
        && (usable > 0 || makes_room)
    {
        return Ok(Some(Target {
            slot: head.slot,
            incarnation: head.incarnation,
            nonce: head.nonce,
            offset: head.offset,
            frame_len,
            opens: false,
            prefix: 0,
        }));
    }
    if usable == 0 || (usable == 1 && !makes_room) {
        return Ok(None);
    }
    let reusable = state
        .segments
        .free
        .iter()
        .find(|(_, after)| *after <= state.durable)
        .map(|(slot, _)| *slot);
    let slot = match reusable {
        Some(slot) => slot,
        None => u32::try_from(state.segments.incarnation.len())
            .map_err(|_| LogError::Damaged("more slots than u32"))?,
    };
    let offset = slot_start(&p.config, slot)?
        .checked_add(block)
        .ok_or(LogError::Damaged("an offset past u64"))?;
    Ok(Some(Target {
        slot,
        incarnation: state.next_incarnation,
        nonce: crate::recover::random_nonce()?,
        offset,
        frame_len,
        opens: true,
        prefix: 0,
    }))
}

/// Bytes of a frame whose header is followed by `payload_len` bytes (a sealed log's MAC
/// counted among them), padded to the block.
pub(crate) fn frame_len(p: &Params, payload_len: usize) -> Result<u64, LogError> {
    format::FRAME_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| u64::try_from(len).ok())
        .and_then(|len| p.align.up_u64(len))
        .ok_or(LogError::TooLarge(payload_len))
}

/// A run of live entries being gathered: its first index and its entries' terms and bytes.
type Run<'a> = Option<(u64, Vec<(u64, &'a [u8])>)>;

/// A live piece of a tail frame, to be copied.
enum Copy<'a> {
    Entries {
        group: u128,
        first: u64,
        entries: Vec<(u64, &'a [u8])>,
    },
    Hard {
        group: u128,
        state: format::HardState,
    },
    Start {
        group: u128,
        start: format::Start,
    },
    Proposal {
        group: u128,
        index: u64,
        term: u64,
        bytes: &'a [u8],
    },
    Uncertain {
        group: u128,
        mark: format::Start,
    },
    Damaged {
        group: u128,
    },
    Released {
        group: u128,
        through: u64,
    },
}

impl Copy<'_> {
    fn record(&self) -> Record<'_> {
        match self {
            Copy::Entries {
                group,
                first,
                entries,
            } => Record::Relocated {
                group: *group,
                first: *first,
                entries,
            },
            Copy::Hard { group, state } => Record::HardState {
                group: *group,
                state: *state,
            },
            Copy::Start { group, start } => Record::Start {
                group: *group,
                start: *start,
            },
            Copy::Proposal {
                group,
                index,
                term,
                bytes,
            } => Record::Proposal {
                group: *group,
                index: *index,
                term: *term,
                bytes,
            },
            Copy::Uncertain { group, mark } => Record::Uncertain {
                group: *group,
                mark: *mark,
            },
            Copy::Damaged { group } => Record::Damaged { group: *group },
            Copy::Released { group, through } => Record::Released {
                group: *group,
                through: *through,
            },
        }
    }

    fn moved(&self, placed: Placed, out: &mut Vec<Moved>) {
        match (self, placed) {
            (Copy::Entries { group, .. }, Placed::Entries(at)) => {
                out.extend(at.into_iter().map(|(index, at)| Moved::Entry {
                    group: *group,
                    index,
                    at,
                }));
            }
            (Copy::Hard { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Hard { group: *group, at });
            }
            (Copy::Start { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Start { group: *group, at });
            }
            (Copy::Proposal { group, index, .. }, Placed::Record(at)) => {
                out.push(Moved::Proposal {
                    group: *group,
                    index: *index,
                    at,
                });
            }
            (Copy::Uncertain { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Uncertain { group: *group, at });
            }
            (Copy::Damaged { group }, Placed::Record(at)) => {
                out.push(Moved::Damaged { group: *group, at });
            }
            (Copy::Released { group, .. }, Placed::Record(at)) => {
                out.push(Moved::Released { group: *group, at });
            }
            _ => {}
        }
    }
}

/// The pieces of a tail frame the state still points at, as copies: runs of live entries,
/// and the hard states, starts and proposals still current. `base` is the file offset of the
/// frame's payload.
fn live_copies<'a>(state: &State, slot: u32, base: u64, records: &'a [Owned]) -> Vec<Copy<'a>> {
    let at = |offset: usize| Place {
        slot,
        offset: base.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX)),
    };
    let mut out = Vec::new();
    for record in records {
        match record {
            Owned::Entries { group, entries, .. } | Owned::Relocated { group, entries, .. } => {
                if let Some(g) = state.groups.get(group) {
                    entry_copies(g, *group, entries, &at, &mut out);
                }
            }
            other => out.extend(piece_copy(state, other, &at)),
        }
    }
    out
}

/// The runs of a record's entries that the group still points at, each a copy.
fn entry_copies<'a>(
    g: &Group,
    group: u128,
    entries: &'a [format::Decoded],
    at: &impl Fn(usize) -> Place,
    out: &mut Vec<Copy<'a>>,
) {
    let mut run: Run<'_> = None;
    let flush = |run: &mut Run<'a>, out: &mut Vec<Copy<'a>>| {
        if let Some((first, entries)) = run.take() {
            out.push(Copy::Entries {
                group,
                first,
                entries,
            });
        }
    };
    for e in entries {
        let live = g.slot(e.index).is_some_and(|s| s.place == at(e.at));
        if !live {
            flush(&mut run, out);
            continue;
        }
        let follows = run.as_ref().is_some_and(|(first, held)| {
            first.saturating_add(u64::try_from(held.len()).unwrap_or(u64::MAX)) == e.index
        });
        match (&mut run, follows) {
            (Some((_, held)), true) => held.push((e.term, &e.bytes)),
            _ => {
                flush(&mut run, out);
                run = Some((e.index, vec![(e.term, &e.bytes)]));
            }
        }
    }
    flush(&mut run, out);
}

/// A record other than entries that the state still points at, as a copy.
fn piece_copy<'a>(
    state: &State,
    record: &'a Owned,
    at: &impl Fn(usize) -> Place,
) -> Option<Copy<'a>> {
    let group_of = |group: &u128| state.groups.get(group);
    match record {
        Owned::HardState {
            at: offset,
            group,
            state: hard,
        } => group_of(group)
            .and_then(|g| g.hard)
            .is_some_and(|(_, p)| p == at(*offset))
            .then_some(Copy::Hard {
                group: *group,
                state: *hard,
            }),
        Owned::Start {
            at: offset,
            group,
            start,
        } => group_of(group)
            .is_some_and(|g| g.start_at == Some(at(*offset)))
            .then_some(Copy::Start {
                group: *group,
                start: *start,
            }),
        Owned::Proposal { group, proposal } => group_of(group)
            .and_then(|g| g.proposals.get(&proposal.index))
            .is_some_and(|p| p.place == at(proposal.at))
            .then_some(Copy::Proposal {
                group: *group,
                index: proposal.index,
                term: proposal.term,
                bytes: &proposal.bytes,
            }),
        Owned::Uncertain {
            at: offset,
            group,
            mark,
        } => group_of(group)
            .and_then(|g| g.uncertain)
            .is_some_and(|(_, p)| p == at(*offset))
            .then_some(Copy::Uncertain {
                group: *group,
                mark: *mark,
            }),
        Owned::Damaged { at: offset, group } => (state.damaged.get(group)
            == Some(&Some(at(*offset))))
        .then_some(Copy::Damaged { group: *group }),
        Owned::Released {
            at: offset,
            group,
            through,
        } => group_of(group)
            .and_then(|g| g.released)
            .is_some_and(|(_, p)| p == at(*offset))
            .then_some(Copy::Released {
                group: *group,
                through: *through,
            }),
        // A key record keys its own session's records and is not a piece of any group: a sweep
        // reseals what it copies under the session that writes the copy.
        Owned::Entries { .. }
        | Owned::Relocated { .. }
        | Owned::Removed { .. }
        | Owned::Key { .. } => None,
    }
}

/// Points the state at a relocated piece's new place.
fn move_piece(
    state: &mut State,
    moved: &Moved,
    place: &impl Fn(usize) -> Result<Place, LogError>,
) -> Result<(), LogError> {
    let (live, groups, damaged) = (&mut state.live, &mut state.groups, &mut state.damaged);
    match *moved {
        Moved::Entry { group, index, at } => {
            if let Some(slot) = groups.get_mut(&group).and_then(|g| slot_mut(g, index)) {
                let bytes = entry_bytes(slot.len);
                live.kill(slot.place, bytes);
                slot.place = place(at)?;
                live.add(slot.place, bytes);
            }
        }
        Moved::Hard { group, at } => {
            if let Some((_, p)) = groups.get_mut(&group).and_then(|g| g.hard.as_mut()) {
                live.kill(*p, HARD_STATE_BYTES);
                *p = place(at)?;
                live.add(*p, HARD_STATE_BYTES);
            }
        }
        Moved::Start { group, at } => {
            if let Some(p) = groups.get_mut(&group).and_then(|g| g.start_at.as_mut()) {
                live.kill(*p, START_BYTES);
                *p = place(at)?;
                live.add(*p, START_BYTES);
            }
        }
        Moved::Proposal { group, index, at } => {
            if let Some(p) = groups
                .get_mut(&group)
                .and_then(|g| g.proposals.get_mut(&index))
            {
                let bytes = proposal_bytes(&p.bytes);
                live.kill(p.place, bytes);
                p.place = place(at)?;
                live.add(p.place, bytes);
            }
        }
        Moved::Uncertain { group, at } => {
            if let Some((_, p)) = groups.get_mut(&group).and_then(|g| g.uncertain.as_mut()) {
                live.kill(*p, UNCERTAIN_BYTES);
                *p = place(at)?;
                live.add(*p, UNCERTAIN_BYTES);
            }
        }
        Moved::Damaged { group, at } => {
            if let Some(Some(p)) = damaged.get_mut(&group) {
                live.kill(*p, DAMAGED_BYTES);
                *p = place(at)?;
                live.add(*p, DAMAGED_BYTES);
            }
        }
        Moved::Released { group, at } => {
            if let Some((_, p)) = groups.get_mut(&group).and_then(|g| g.released.as_mut()) {
                live.kill(*p, RELEASED_BYTES);
                *p = place(at)?;
                live.add(*p, RELEASED_BYTES);
            }
        }
    }
    Ok(())
}

fn slot_mut(g: &mut Group, index: u64) -> Option<&mut Slot> {
    let i = index.checked_sub(g.first()?)?;
    g.entries.get_mut(usize::try_from(i).ok()?)
}

fn proposal_bytes(bytes: &[u8]) -> u64 {
    entry_bytes(u32::try_from(bytes.len()).unwrap_or(u32::MAX)).saturating_add(PROPOSAL_EXTRA)
}

/// Whether an update only frees what its group holds: a removal, a damage fence, or a new start
/// that writes no entry or proposal, as a compaction or a snapshot's install does. Such a frame
/// may take the room the log keeps for making room (mantle docs/design/raft-log.md §5).
pub(crate) fn frees(update: &Update, marks: Marks) -> bool {
    if update.remove || marks.damaged {
        return true;
    }
    update.start.is_some()
        && update.proposals.is_empty()
        && marks.uncertain.is_none()
        && update.entries.as_ref().is_none_or(|e| e.entries.is_empty())
}

/// Whether `s` may be written as the group stands, and whether it makes a new group.
pub(crate) fn validate(
    state: &State,
    config: &Config,
    s: &Submission,
    new_groups: usize,
) -> Result<bool, LogError> {
    let (group, update) = (s.group, &s.update);
    let invalid = |reason| LogError::Invalid { group, reason };
    // The fence recovery writes on a damaged group carries nothing else.
    if s.marks.damaged {
        return if *update == Update::default() {
            Ok(false)
        } else {
            Err(invalid("a damage mark carries nothing else"))
        };
    }
    // A damaged group takes nothing but its removal, which is how its replica leaves the
    // device to be rebuilt from its peers.
    if state.damaged.contains_key(&group) && !update.remove {
        return Err(LogError::Damaged(
            "the group's acknowledged records are damaged; it recovers from its peers",
        ));
    }
    let current = state.groups.get(&group);
    if update.remove {
        let alone = update.start.is_none()
            && update.entries.is_none()
            && update.hard_state.is_none()
            && update.proposals.is_empty()
            && update.released.is_none();
        return if alone {
            Ok(false)
        } else {
            Err(invalid("a removal carries nothing else"))
        };
    }
    let new = current.is_none();
    // A fenced group holds its place among the groups until it is removed.
    let held = state.groups.len().saturating_add(state.damaged.len());
    if new && held.saturating_add(new_groups) >= config.max_groups {
        return Err(LogError::TooManyGroups(config.max_groups));
    }
    let empty = Group::default();
    let g = current.unwrap_or(&empty);
    let mut start = g.start;
    let mut last = g.last().ok_or(invalid("an index past u64"))?;
    let mut count = u64::try_from(g.entries.len()).unwrap_or(u64::MAX);
    let mut bytes = g.bytes;
    if let Some(s) = update.start {
        if s.index < start.index {
            return Err(invalid("the start moves back"));
        }
        for index in start.index.saturating_add(1)..=s.index.min(last) {
            if let Some(slot) = g.slot(index) {
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(u64::from(slot.len));
            }
        }
        if s.index > last {
            last = s.index;
        }
        start = s;
    }
    if let Some(e) = &update.entries {
        if e.first <= start.index {
            return Err(invalid("entries at or before the start"));
        }
        if e.first > last.saturating_add(1) {
            return Err(invalid("entries past the last leave a gap"));
        }
        for index in e.first..=last {
            if let Some(slot) = g.slot(index) {
                count = count.saturating_sub(1);
                bytes = bytes.saturating_sub(u64::from(slot.len));
            }
        }
        let added = u64::try_from(e.entries.len()).unwrap_or(u64::MAX);
        count = count.saturating_add(added);
        bytes = e.entries.iter().fold(bytes, |sum, entry| {
            sum.saturating_add(u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX))
        });
        // The entries end at an index; a proposal may sit at or below it, for the log reaching
        // a proposal ends nothing (`state::Replayed::reach`).
        e.first
            .checked_add(added)
            .and_then(|end| end.checked_sub(1))
            .ok_or(invalid("an index past u64"))?;
    }
    if count > config.group_entries || bytes > config.group_bytes {
        return Err(LogError::Backlog(group));
    }
    Ok(new)
}

/// Bytes an update's records take in a payload.
pub(crate) fn update_len(group: u128, update: &Update, tag: usize) -> Option<usize> {
    let mut len = 0usize;
    if update.remove {
        return format::encoded_len(&Record::Removed { group }, tag);
    }
    if let Some(e) = &update.entries {
        let lens = e.entries.iter().map(|x| x.bytes.len());
        len = len.checked_add(format::entries_len(lens, tag)?)?;
    }
    let singles = [
        update.start.map(|start| Record::Start { group, start }),
        update
            .hard_state
            .map(|state| Record::HardState { group, state }),
        update
            .released
            .map(|through| Record::Released { group, through }),
    ];
    for record in singles.iter().flatten() {
        len = len.checked_add(format::encoded_len(record, tag)?)?;
    }
    for p in &update.proposals {
        let proposal = Record::Proposal {
            group,
            index: p.index,
            term: p.term,
            bytes: &p.bytes,
        };
        len = len.checked_add(format::encoded_len(&proposal, tag)?)?;
    }
    Some(len)
}

/// Lays an update's records into the payload: removal alone, or start, entries, hard state
/// and proposals, in the order they apply.
pub(crate) fn encode(
    payload: &mut Payload,
    records: &mut u32,
    group: u128,
    update: &Update,
    marks: Marks,
    tag: usize,
) -> Option<Placement> {
    let mut lay = Lay {
        payload,
        records,
        tag,
    };
    if marks.damaged {
        let damaged = lay.put(&Record::Damaged { group })?;
        return Some(Placement {
            damaged: Some(damaged),
            tag,
            ..Placement::default()
        });
    }
    if update.remove {
        lay.put(&Record::Removed { group })?;
        return Some(Placement::default());
    }
    let start = lay.maybe(update.start.map(|start| Record::Start { group, start }))?;
    let entries = match &update.entries {
        Some(e) => Some(lay.entries(group, e)?),
        None => None,
    };
    let hard = lay.maybe(
        update
            .hard_state
            .map(|state| Record::HardState { group, state }),
    )?;
    // The release before the proposals it is written with, as they apply.
    let released = lay.maybe(
        update
            .released
            .map(|through| Record::Released { group, through }),
    )?;
    let proposals = lay.proposals(group, &update.proposals)?;
    let uncertain = lay.maybe(
        marks
            .uncertain
            .map(|mark| Record::Uncertain { group, mark }),
    )?;
    Some(Placement {
        start,
        entries,
        hard,
        proposals,
        uncertain,
        damaged: None,
        tag,
        released,
    })
}

/// A payload being laid out, and its count of records.
struct Lay<'a> {
    payload: &'a mut Payload,
    records: &'a mut u32,
    tag: usize,
}

impl Lay<'_> {
    /// Appends a record placed whole, and says where `put` placed it.
    fn put(&mut self, record: &Record<'_>) -> Option<usize> {
        *self.records = self.records.checked_add(1)?;
        match format::put(self.payload, record, self.tag)? {
            Placed::Record(at) => Some(at),
            Placed::Entries(_) => None,
        }
    }

    /// Appends `record` if there is one, and says where `put` placed it.
    fn maybe(&mut self, record: Option<Record<'_>>) -> Option<Option<usize>> {
        match record {
            Some(record) => self.put(&record).map(Some),
            None => Some(None),
        }
    }

    /// Appends an update's entries record, and says where it starts.
    fn entries(&mut self, group: u128, e: &crate::Entries) -> Option<usize> {
        *self.records = self.records.checked_add(1)?;
        let entries = e.entries.iter().map(|x| (x.term, x.bytes.as_slice()));
        format::put_entries(self.payload, false, group, e.first, entries, self.tag)
    }

    /// Appends a record for each proposal, and says where the first record starts: `put`
    /// places a proposal at its fields, past the record's kind and group.
    fn proposals(&mut self, group: u128, proposals: &[crate::Proposal]) -> Option<Option<usize>> {
        let mut first = None;
        for p in proposals {
            let at = self.put(&Record::Proposal {
                group,
                index: p.index,
                term: p.term,
                bytes: &p.bytes,
            })?;
            first = first.or(at.checked_sub(format::RECORD_HEADER_LEN));
        }
        Some(first)
    }
}

/// Makes the durable frame visible: the segment it opened, where swept pieces now are,
/// every update's pieces, and the tails that died. The updates' bytes move into the state, which
/// keeps them while they are recent.
pub(crate) fn publish(
    state: &mut State,
    p: &Params,
    target: &Target,
    sweep: Option<Sweep>,
    taken: &mut [(Submission, Placement)],
    tail: u64,
    lens: &[(u64, u32)],
) -> Result<(), LogError> {
    let base = target
        .offset
        .checked_add(format::FRAME_HEADER_BYTES)
        .and_then(|base| base.checked_add(target.prefix))
        .ok_or(LogError::Damaged("an offset past u64"))?;
    let place = |at: usize| -> Result<Place, LogError> {
        Ok(Place {
            slot: target.slot,
            offset: u64::try_from(at)
                .ok()
                .and_then(|at| base.checked_add(at))
                .ok_or(LogError::Damaged("an offset past u64"))?,
        })
    };
    if target.opens {
        open_segment(state, target)?;
    }
    state.live.wrote(target.slot, target.frame_len);
    let sequence = state.next_sequence;
    state.durable = sequence;
    state.durable_tail = tail;
    state.next_sequence = sequence
        .checked_add(1)
        .ok_or(LogError::Damaged("sequences past u64"))?;
    state.head = state::Head {
        slot: target.slot,
        incarnation: target.incarnation,
        nonce: target.nonce,
        offset: target
            .offset
            .checked_add(target.frame_len)
            .ok_or(LogError::Damaged("an offset past u64"))?,
    };
    if let Some(sweep) = sweep {
        for m in &sweep.moved {
            move_piece(state, m, &place)?;
        }
        if state.segments.live.front() != Some(&sweep.slot) || state.live.of(sweep.slot).0 != 0 {
            return Err(LogError::Damaged("a swept tail still holds a live piece"));
        }
        state.segments.live.pop_front();
        // This frame names the tail past it and is durable: the segment is free now.
        state.segments.free.push_back((sweep.slot, sequence));
    }
    for (s, placement) in taken.iter_mut() {
        let handed = lens.get(s.lens.0..s.lens.1).unwrap_or_default();
        apply(state, &p.config, s, placement, &place, handed)?;
    }
    drop_dead_tails(state);
    Ok(())
}

/// Records the segment a frame opened.
fn open_segment(state: &mut State, target: &Target) -> Result<(), LogError> {
    let slot = usize::try_from(target.slot).map_err(|_| LogError::Damaged("slot"))?;
    if state.segments.incarnation.len() <= slot {
        state.segments.incarnation.resize(slot.saturating_add(1), 0);
        state.segments.nonce.resize(slot.saturating_add(1), 0);
        state.live.grow(slot.saturating_add(1));
    }
    if let Some(entry) = state.segments.incarnation.get_mut(slot) {
        *entry = target.incarnation;
    }
    if let Some(entry) = state.segments.nonce.get_mut(slot) {
        *entry = target.nonce;
    }
    state.live.open(target.slot);
    state.segments.free.retain(|(s, _)| *s != target.slot);
    state.segments.live.push_back(target.slot);
    state.next_incarnation = state
        .next_incarnation
        .checked_add(1)
        .ok_or(LogError::Damaged("incarnations past u64"))?;
    Ok(())
}

/// Where a piece laid at a payload offset is in the file.
type Places<'a> = dyn Fn(usize) -> Result<Place, LogError> + 'a;

/// Applies a durable update to the group's state, taking its entries' and proposals' bytes.
fn apply(
    state: &mut State,
    config: &Config,
    s: &mut Submission,
    placement: &Placement,
    place: &Places<'_>,
    handed: &[(u64, u32)],
) -> Result<(), LogError> {
    let (group, marks) = (s.group, s.marks);
    if marks.damaged {
        return apply_damage(state, group, placement, place);
    }
    if s.update.remove {
        remove(state, group);
        return Ok(());
    }
    let live = &mut state.live;
    let g = state.groups.entry(group).or_insert_with(|| Group {
        cache_from: 1,
        ..Group::default()
    });
    let update = &mut s.update;
    apply_start(g, live, update, placement, place)?;
    let handed = s.handle.then_some(handed);
    apply_entries(g, live, config, update, handed, placement, place)?;
    reach(g, live)?;
    apply_hard(g, live, update, placement, place)?;
    apply_released(g, live, update, placement, place)?;
    apply_proposals(g, live, update, placement, place)?;
    apply_mark(g, live, marks, placement, place)
}

/// Whatever the group held is gone; only the fence is live.
fn apply_damage(
    state: &mut State,
    group: u128,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let live = &mut state.live;
    if let Some(g) = state.groups.remove(&group) {
        for (p, bytes) in g.pieces() {
            live.kill(p, bytes);
        }
    }
    if let Some(at) = placement.damaged {
        let new_at = place(at)?;
        if let Some(Some(old)) = state.damaged.insert(group, Some(new_at)) {
            live.kill(old, DAMAGED_BYTES);
        }
        live.add(new_at, DAMAGED_BYTES);
    }
    Ok(())
}

/// Every record of the group dies, its fence too.
fn remove(state: &mut State, group: u128) {
    let live = &mut state.live;
    if let Some(Some(at)) = state.damaged.remove(&group) {
        live.kill(at, DAMAGED_BYTES);
    }
    if let Some(g) = state.groups.remove(&group) {
        for (p, bytes) in g.pieces() {
            live.kill(p, bytes);
        }
    }
}

/// A new start drops the entries before it.
fn apply_start(
    g: &mut Group,
    live: &mut state::Live,
    update: &Update,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let (Some(start), Some(at)) = (update.start, placement.start) else {
        return Ok(());
    };
    if let Some(old) = g.start_at {
        live.kill(old, START_BYTES);
    }
    while g.start.index < start.index {
        let Some(slot) = g.entries.pop_front() else {
            break;
        };
        kill_slot(g, live, &slot);
        g.start.index = g.start.index.saturating_add(1);
    }
    g.start = start;
    let new_at = place(at)?;
    g.start_at = Some(new_at);
    live.add(new_at, START_BYTES);
    g.cache_from = g.cache_from.max(start.index.saturating_add(1));
    Ok(())
}

/// Entries replace the group's suffix from their first; their bytes are kept while recent, by
/// the log or, for a group written through its handle, by the handle, which has them back: what
/// the log keeps of those is their terms and lengths, `handed`.
fn apply_entries(
    g: &mut Group,
    live: &mut state::Live,
    config: &Config,
    update: &mut Update,
    handed: Option<&[(u64, u32)]>,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let Some(e) = &mut update.entries else {
        return Ok(());
    };
    while g.last().is_some_and(|last| last >= e.first) {
        let Some(slot) = g.entries.pop_back() else {
            break;
        };
        kill_slot(g, live, &slot);
    }
    let mut at = placement
        .entries
        .and_then(|record| record.checked_add(format::ENTRIES_HEADER_LEN));
    let mut add = |g: &mut Group, term: u64, len: u32, cached: Option<Vec<u8>>| {
        let here = at.ok_or(LogError::Damaged("an offset past usize"))?;
        at = here
            .checked_add(format::ENTRY_HEADER_LEN)
            .and_then(|a| a.checked_add(usize::try_from(len).ok()?))
            .and_then(|a| a.checked_add(placement.tag));
        let slot = Slot {
            term,
            place: place(here)?,
            len,
            cached,
        };
        live.add(slot.place, entry_bytes(len));
        g.bytes = g.bytes.saturating_add(u64::from(len));
        if slot.cached.is_some() {
            g.cached = g.cached.saturating_add(u64::from(len));
        }
        g.entries.push_back(slot);
        Ok::<(), LogError>(())
    };
    if let Some(handed) = handed {
        for &(term, len) in handed {
            add(g, term, len, None)?;
        }
        // The handle holds the bytes: the log's cache starts past the last entry.
        g.cache_from = g.last().map_or(g.cache_from, |last| last.saturating_add(1));
        return Ok(());
    }
    for entry in &mut e.entries {
        let len = u32::try_from(entry.bytes.len()).map_err(|_| LogError::TooLarge(usize::MAX))?;
        add(g, entry.term, len, Some(std::mem::take(&mut entry.bytes)))?;
    }
    g.cache_from = g
        .cache_from
        .min(e.first)
        .max(g.start.index.saturating_add(1));
    evict(g, config.group_cache);
    Ok(())
}

/// What the log has reached, by entries or by its start, is no longer uncertain once it holds
/// what the mark covers. A proposal outlives the log reaching its index: only a release ends
/// it (`apply_released`).
fn reach(g: &mut Group, live: &mut state::Live) -> Result<(), LogError> {
    let last = g.last().ok_or(LogError::Damaged("an index past u64"))?;
    if let Some((mark, at)) = g.uncertain
        && resolves(mark, last, g.last_term())
    {
        live.kill(at, UNCERTAIN_BYTES);
        g.uncertain = None;
    }
    Ok(())
}

/// The proposals at or below what the update releases end, and its record is the group's
/// release unless an earlier one released more.
fn apply_released(
    g: &mut Group,
    live: &mut state::Live,
    update: &Update,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let (Some(through), Some(at)) = (update.released, placement.released) else {
        return Ok(());
    };
    while let Some(entry) = g.proposals.first_entry() {
        if *entry.key() > through {
            break;
        }
        let p = entry.remove();
        live.kill(p.place, proposal_bytes(&p.bytes));
    }
    if g.released.is_some_and(|(held, _)| held >= through) {
        return Ok(());
    }
    if let Some((_, old)) = g.released {
        live.kill(old, RELEASED_BYTES);
    }
    let new_at = place(at)?;
    g.released = Some((through, new_at));
    live.add(new_at, RELEASED_BYTES);
    Ok(())
}

fn apply_hard(
    g: &mut Group,
    live: &mut state::Live,
    update: &Update,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let (Some(hard), Some(at)) = (update.hard_state, placement.hard) else {
        return Ok(());
    };
    if let Some((_, old)) = g.hard {
        live.kill(old, HARD_STATE_BYTES);
    }
    let new_at = place(at)?;
    g.hard = Some((hard, new_at));
    live.add(new_at, HARD_STATE_BYTES);
    Ok(())
}

/// Each proposal's record follows the one before; its bytes move into the group.
fn apply_proposals(
    g: &mut Group,
    live: &mut state::Live,
    update: &mut Update,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let mut record = placement.proposals;
    for p in &mut update.proposals {
        let start = record.ok_or(LogError::Damaged("an offset past usize"))?;
        record = start
            .checked_add(format::RECORD_HEADER_LEN)
            .and_then(|a| a.checked_add(format::PROPOSAL_FIELDS_LEN))
            .and_then(|a| a.checked_add(p.bytes.len()))
            .and_then(|a| a.checked_add(placement.tag));
        let at = start
            .checked_add(format::RECORD_HEADER_LEN)
            .ok_or(LogError::Damaged("an offset past usize"))?;
        let new_at = place(at)?;
        let bytes = proposal_bytes(&p.bytes);
        let proposal = state::Proposal {
            term: p.term,
            place: new_at,
            bytes: std::mem::take(&mut p.bytes),
        };
        if let Some(old) = g.proposals.insert(p.index, proposal) {
            live.kill(old.place, proposal_bytes(&old.bytes));
        }
        live.add(new_at, bytes);
    }
    Ok(())
}

fn apply_mark(
    g: &mut Group,
    live: &mut state::Live,
    marks: Marks,
    placement: &Placement,
    place: &Places<'_>,
) -> Result<(), LogError> {
    let (Some(mark), Some(at)) = (marks.uncertain, placement.uncertain) else {
        return Ok(());
    };
    if let Some((_, old)) = g.uncertain {
        live.kill(old, UNCERTAIN_BYTES);
    }
    let new_at = place(at)?;
    g.uncertain = Some((mark, new_at));
    live.add(new_at, UNCERTAIN_BYTES);
    Ok(())
}

fn kill_slot(g: &mut Group, live: &mut state::Live, slot: &Slot) {
    live.kill(slot.place, entry_bytes(slot.len));
    g.bytes = g.bytes.saturating_sub(u64::from(slot.len));
    if slot.cached.is_some() {
        g.cached = g.cached.saturating_sub(u64::from(slot.len));
    }
}

/// Drops cached bytes from the oldest cached entry on until the group is within its budget.
fn evict(g: &mut Group, budget: u64) {
    while g.cached > budget {
        let index = g.cache_from;
        let Some(slot) = slot_mut(g, index) else {
            return;
        };
        if slot.cached.take().is_some() {
            let len = u64::from(slot.len);
            g.cached = g.cached.saturating_sub(len);
        }
        g.cache_from = index.saturating_add(1);
    }
}
