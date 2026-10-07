//! A log's [`Config`] derived from what its node and its device state ([`Facts`]), so that no
//! caller copies the log's own constants (the per-group submissions a handle keeps out, a frame's
//! headers) into a number of its own, which would go stale silently when they change.
//!
//! Each field follows from the facts by mantle docs/design/raft-log.md: the segment from the
//! largest entry and the device's block (§2, §3), the file's segments from the disk budget (§2,
//! §5), the groups from the node's admission bound, a group's retention and recent bytes (§4), and
//! the queue from the groups (§3). Facts that cannot hold one frame, or a budget under the log's
//! least file, are refused ([`Unfit`]), never clamped: a clamped configuration would run, and fail
//! later at a write the facts promised.
use hyper_block::buf::{Alignment, MAX_BUFFER};

use crate::{Config, LogError, Waits, format, frame_room, recover, room};

/// What a node and its device state, from which [`Config::derive`] derives a log's configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    /// The device's block, its write unit (raft-log.md §2's `B`): every segment is a multiple of it.
    pub align: Alignment,
    /// Whether the log is sealed (`Log::create_sealed`): a sealed frame also holds its MAC and a
    /// key record, and each record its tag.
    pub sealed: bool,
    /// Bytes of the largest entry an owner submits: the log holds it in one frame of its own.
    pub largest_entry: usize,
    /// Bytes the node's disk budget gives the log's file, its persist area included.
    pub disk_bytes: u64,
    /// The node's admission bound on groups: the most it may be placed, never its current
    /// placement, since groups arrive while the log runs.
    pub max_groups: usize,
    /// Entries between an owner's checkpoints: after one, the group's log starts past it.
    pub cadence_entries: u64,
    /// Payload bytes between an owner's checkpoints, at most.
    pub cadence_bytes: u64,
    /// Entries a group may hold uncommitted past its checkpoint cadence.
    pub uncommitted_entries: u64,
    /// Payload bytes a group may hold uncommitted past its checkpoint cadence.
    pub uncommitted_bytes: u64,
    /// Bytes of recent entries the node gives the log's cache across all its groups.
    pub cache_bytes: u64,
}

/// Why facts give no configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Unfit {
    /// A fact that must be positive is zero.
    #[error("{0} is zero")]
    Zero(&'static str),
    /// No segment the log can read in one buffer holds a frame of the largest entry.
    #[error("an entry of {largest} bytes is past the {most} bytes a segment's frame can hold")]
    Entry {
        /// The stated largest entry.
        largest: usize,
        /// The largest entry a segment of one buffer holds alone.
        most: usize,
    },
    /// The disk budget is under the log's least file: its persist area and three segments.
    #[error("the log needs {needed} bytes of disk at least, and the budget is {budget}")]
    Disk {
        /// The least file, in bytes.
        needed: u64,
        /// The stated budget.
        budget: u64,
    },
    /// A group's retained bytes cannot hold the largest entry.
    #[error("a group retains {retained} bytes, under the largest entry's {largest}")]
    Retention {
        /// The derived retention, in bytes.
        retained: u64,
        /// The stated largest entry.
        largest: usize,
    },
    /// A derived quantity past its type.
    #[error("{0} past its bound")]
    Overflow(&'static str),
}

impl From<Unfit> for LogError {
    fn from(unfit: Unfit) -> Self {
        LogError::Unfit(unfit)
    }
}

/// Bytes an entry's record takes in a frame besides its payload.
fn entry_overhead(sealed: bool) -> Result<usize, Unfit> {
    format::encoded_len(
        &format::Record::Entries {
            group: 0,
            first: 0,
            entries: &[(0, &[])],
        },
        if sealed { format::TAG_LEN } else { 0 },
    )
    .ok_or(Unfit::Overflow("an entry's record"))
}

/// Bytes of a segment past its frame's payload: the segment's header block, the frame's header,
/// and in a sealed log the frame's MAC and a key record (`frame_room`).
fn frame_overhead(block: usize, sealed: bool) -> Result<usize, Unfit> {
    let sealing = if sealed {
        format::MAC_LEN
            .checked_add(format::KEY_RECORD_LEN)
            .ok_or(Unfit::Overflow("a key record"))?
    } else {
        0
    };
    block
        .checked_add(format::FRAME_HEADER_LEN)
        .and_then(|n| n.checked_add(sealing))
        .ok_or(Unfit::Overflow("a frame's headers"))
}

fn up(align: Alignment, n: u64) -> Result<u64, Unfit> {
    align.up_u64(n).ok_or(Unfit::Overflow("a segment"))
}

fn positive(value: u64, name: &'static str) -> Result<u64, Unfit> {
    if value == 0 {
        Err(Unfit::Zero(name))
    } else {
        Ok(value)
    }
}

impl Config {
    /// The configuration `facts` give (mantle docs/design/raft-log.md §2–§5):
    ///
    /// - `segment_bytes`: the least multiple of the block whose frame holds the largest entry in
    ///   a record of its own (§2, §3: an update past one frame is split, and one entry is never
    ///   split), four blocks at least and two persist slots of `max_groups` groups at least
    ///   (§2), and no more than one I/O buffer, through which recovery reads a segment.
    /// - `max_segments`: the segments the disk budget holds after the persist area, one
    ///   segment's length (§2); three at least, the head, its successor and one to reclaim into
    ///   (§5).
    /// - `max_groups`: the node's admission bound; reaching it is `TooManyGroups`.
    /// - `group_entries`, `group_bytes`: a group retains its entries until its engine makes them
    ///   durable, plus a window for lagging followers (§4). The engine's durable point is its
    ///   checkpoint, so a group retains two cadences, the one being checkpointed and the one
    ///   after it, and what may be uncommitted past them; past that a follower is sent the
    ///   checkpoint, and a submission that would pass it is refused `Backlog` until the owner
    ///   compacts.
    /// - `group_cache`: the node's cache shared evenly by its groups (§4).
    /// - `queue_submissions`: the most writes every group's handle keeps in the log at once, so
    ///   the queue never refuses a handle's write for count (§3; hyper-raft docs/durable.md §6).
    /// - `waits`: `Measured`, what a node runs (§3).
    ///
    /// The result passes the check `Log::create` and `Log::open` make.
    pub fn derive(facts: &Facts) -> Result<Config, LogError> {
        let largest = facts.largest_entry;
        positive(
            u64::try_from(largest).map_err(|_| Unfit::Overflow("the largest entry"))?,
            "the largest entry",
        )?;
        let max_groups = facts.max_groups;
        if max_groups == 0 {
            return Err(Unfit::Zero("the admission bound on groups").into());
        }
        let cadence_entries = positive(facts.cadence_entries, "the checkpoint cadence's entries")?;
        let cadence_bytes = positive(facts.cadence_bytes, "the checkpoint cadence's bytes")?;
        let cache = positive(facts.cache_bytes, "the cache")?;

        let block = facts.align.get();
        let block_bytes = u64::try_from(block).map_err(|_| Unfit::Overflow("the block"))?;
        let around = frame_overhead(block, facts.sealed)?
            .checked_add(entry_overhead(facts.sealed)?)
            .ok_or(Unfit::Overflow("a frame's headers"))?;
        let buffer = u64::try_from(MAX_BUFFER).map_err(|_| Unfit::Overflow("a buffer"))?;
        // The largest segment recovery reads in one buffer, at the block: what it holds alone.
        let most_segment = buffer
            .checked_div(block_bytes)
            .and_then(|blocks| blocks.checked_mul(block_bytes))
            .ok_or(Unfit::Overflow("the block"))?;
        let most = usize::try_from(most_segment)
            .ok()
            .and_then(|segment| segment.checked_sub(around))
            .unwrap_or(0);
        let holding = largest
            .checked_add(around)
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(Unfit::Entry { largest, most })?;
        let four = block_bytes
            .checked_mul(4)
            .ok_or(Unfit::Overflow("four blocks"))?;
        let slots = format::persist_len(max_groups)
            .and_then(|len| len.checked_add(if facts.sealed { format::MAC_LEN } else { 0 }))
            .and_then(|len| u64::try_from(len).ok())
            .and_then(|len| facts.align.up_u64(len))
            .and_then(|slot| slot.checked_mul(2))
            .ok_or(Unfit::Overflow("the persist slots"))?;
        let segment_bytes = up(facts.align, holding)?
            .max(four)
            .max(up(facts.align, slots)?);
        if segment_bytes > most_segment {
            return Err(Unfit::Entry { largest, most }.into());
        }

        // The persist area, one segment's length, then the segments.
        let needed = segment_bytes
            .checked_mul(4)
            .ok_or(Unfit::Overflow("the least file"))?;
        let segments = facts
            .disk_bytes
            .checked_sub(segment_bytes)
            .and_then(|room| room.checked_div(segment_bytes))
            .unwrap_or(0);
        if segments < 3 {
            return Err(Unfit::Disk {
                needed,
                budget: facts.disk_bytes,
            }
            .into());
        }
        let max_segments =
            u32::try_from(segments).map_err(|_| Unfit::Overflow("the file's segments"))?;

        let group_entries = cadence_entries
            .checked_mul(2)
            .and_then(|n| n.checked_add(facts.uncommitted_entries))
            .ok_or(Unfit::Overflow("a group's retained entries"))?;
        let group_bytes = cadence_bytes
            .checked_mul(2)
            .and_then(|n| n.checked_add(facts.uncommitted_bytes))
            .ok_or(Unfit::Overflow("a group's retained bytes"))?;
        if u64::try_from(largest).map_or(true, |largest| group_bytes < largest) {
            return Err(Unfit::Retention {
                retained: group_bytes,
                largest,
            }
            .into());
        }
        let groups = u64::try_from(max_groups).map_err(|_| Unfit::Overflow("the groups"))?;
        let group_cache = positive(
            cache.checked_div(groups).unwrap_or(0),
            "the cache shared by every group",
        )?;
        let queue_submissions = max_groups
            .checked_mul(room::GROUP_SUBMISSIONS)
            .ok_or(Unfit::Overflow("the queue's submissions"))?;

        let config = Config {
            segment_bytes,
            max_segments,
            max_groups,
            group_entries,
            group_bytes,
            group_cache,
            queue_submissions,
            waits: Waits::Measured,
        };
        recover::check(&config, facts.align, facts.sealed)?;
        // By construction; checked so a change to a frame's layout fails here, not at a write.
        let one = entry_overhead(facts.sealed)?;
        let room = frame_room(&config, facts.align, facts.sealed)?;
        if room.checked_sub(one).is_none_or(|room| room < largest) {
            return Err(LogError::Config(
                "a derived frame does not hold the largest entry",
            ));
        }
        Ok(config)
    }
}
