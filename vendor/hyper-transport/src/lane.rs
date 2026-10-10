//! Replication lanes (node.md §3.2; mantle note 32 T37, T38).
//!
//! Replication messages from one shard to one peer travel on one long-lived stream, so their order
//! is kept and the stream count does not grow with ranges. A lane is a unidirectional stream that
//! opens with its identifier and then carries frames, each a prefix and its bytes ([`crate::frame`];
//! a frame is a message with no body). A lane's queue is as wide as the core's window (T37): a core
//! that keeps its own window never finds its lane full, and one that does not is refused, typed.
//! A lane is always read: a frame whose class bound or budget cannot take it is skipped and
//! counted, as the replica already refuses what it cannot hold, so one full range never stalls the
//! stream for its shard.

use std::collections::VecDeque;

use hyper_quic::StreamId;

use crate::frame::{PREFIX_BYTES, Prefix};
use crate::{LaneId, Refusal, Reservation};

/// A lane's opening: its identifier and that identifier's CRC-32C, big-endian.
pub(crate) const OPENER_BYTES: usize = 8;

/// The opening of lane `lane`.
pub(crate) fn opener(lane: LaneId) -> [u8; OPENER_BYTES] {
    let id = lane.to_be_bytes();
    let sum = crc32c::crc32c(&id).to_be_bytes();
    let [a, b, c, d] = id;
    let [e, f, g, h] = sum;
    [a, b, c, d, e, f, g, h]
}

/// The lane an opening names, if it verifies.
pub(crate) fn opened(bytes: [u8; OPENER_BYTES]) -> Result<LaneId, Refusal> {
    let [a, b, c, d, e, f, g, h] = bytes;
    let id = [a, b, c, d];
    if crc32c::crc32c(&id).to_be_bytes() != [e, f, g, h] {
        return Err(Refusal::Corrupt);
    }
    Ok(LaneId::from_be_bytes(id))
}

/// A lane to a peer.
#[derive(Debug)]
pub(crate) struct LaneOut {
    pub(crate) lane: LaneId,
    pub(crate) stream: Option<StreamId>,
    pub(crate) opener_written: usize,
    /// Frames waiting, each its prefix and bytes, with the rank of its class.
    pub(crate) queue: VecDeque<(u8, Reservation)>,
    /// What of the front frame was written.
    pub(crate) written: usize,
}

impl LaneOut {
    pub(crate) fn new(lane: LaneId, window: usize) -> Self {
        Self {
            lane,
            stream: None,
            opener_written: 0,
            queue: VecDeque::with_capacity(window),
            written: 0,
        }
    }
}

/// Where a lane from a peer stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reading {
    /// The opening is arriving.
    Opener,
    /// A frame's prefix is arriving.
    Prefix,
    /// A frame's bytes are arriving into a reservation.
    Frame,
    /// A frame's bytes are being skipped.
    Skipping,
}

/// A lane from a peer.
#[derive(Debug)]
pub(crate) struct LaneIn {
    pub(crate) stream: StreamId,
    pub(crate) lane: LaneId,
    pub(crate) state: Reading,
    pub(crate) opener: [u8; OPENER_BYTES],
    pub(crate) prefix: [u8; PREFIX_BYTES],
    pub(crate) filled: usize,
    pub(crate) decoded: Option<Prefix>,
    /// The frame's kind code.
    pub(crate) kind: u16,
    pub(crate) frame: Option<Reservation>,
    /// The frame's bytes still to arrive.
    pub(crate) left: u64,
    /// The rank of the frame's class; a skipped frame's counts as the most urgent's.
    pub(crate) rank: u8,
    /// Its frames waiting for the owner: at most the core's window.
    pub(crate) queued: usize,
    /// It is not read until a seat for its next frame is free.
    pub(crate) paused: bool,
}

impl LaneIn {
    pub(crate) fn new(stream: StreamId) -> Self {
        Self {
            stream,
            lane: 0,
            state: Reading::Opener,
            opener: [0; OPENER_BYTES],
            prefix: [0; PREFIX_BYTES],
            filled: 0,
            decoded: None,
            kind: 0,
            frame: None,
            left: 0,
            rank: 0,
            queued: 0,
            paused: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_opening_names_its_lane_and_a_damaged_one_none() {
        let bytes = opener(0xDEAD_BEEF);
        assert_eq!(opened(bytes), Ok(0xDEAD_BEEF));
        let mut damaged = bytes;
        damaged[2] ^= 4;
        assert_eq!(opened(damaged), Err(Refusal::Corrupt));
    }
}
