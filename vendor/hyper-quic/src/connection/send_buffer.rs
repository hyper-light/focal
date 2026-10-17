use std::{collections::VecDeque, ops::Range};

use bytes::{Buf, Bytes};

use crate::{VarInt, range_set::RangeSet};

/// Buffer of outgoing retransmittable stream data
#[derive(Default, Debug)]
pub(super) struct SendBuffer {
    /// Data queued by the application but not yet acknowledged. May or may not have been sent.
    /// Each piece carries the stream offset of its first byte, so a read finds the piece holding
    /// its offset by binary search: walked from the front, every read of new data passed every
    /// piece in flight before it, about two a frame (its prefix and its body), and decimated
    /// acknowledgements keep a millisecond of them in flight (at 100,000 frames a second on a busy
    /// core the walk was 5.5% of the node's time, `populate_packet`'s most of it).
    unacked_segments: VecDeque<Piece>,
    /// Total size of `unacked_segments`
    unacked_len: usize,
    /// Acknowledged bytes removed from the first segment's view but still held by its allocation
    front_trimmed: usize,
    /// The first offset that hasn't been written by the application, i.e. the offset past the end of `unacked`
    offset: u64,
    /// The first offset that hasn't been sent
    ///
    /// Always lies in (offset - unacked.len())..offset
    unsent: u64,
    /// Acknowledged ranges which couldn't be discarded yet as they don't include the earliest
    /// offset in `unacked`
    // TODO: Recover storage from these by compacting (#700)
    acks: RangeSet,
    /// Previously transmitted ranges deemed lost
    retransmits: RangeSet,
}

impl SendBuffer {
    /// Construct an empty buffer at the initial offset
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Append application data to the end of the stream
    pub(super) fn write(&mut self, data: Bytes) {
        // Bytes held in memory, and a stream offset the send window bounds below 2^62: neither
        // sum saturates.
        if data.is_empty() {
            return;
        }
        let start = self.offset;
        self.unacked_len = self.unacked_len.saturating_add(data.len());
        self.offset = self.offset.saturating_add(len_u64(data.len()));
        self.unacked_segments
            .push_back(Piece { start, bytes: data });
    }

    /// The offset of the first byte still held: `unacked_len` is at most `offset`.
    fn base_offset(&self) -> u64 {
        self.offset.saturating_sub(len_u64(self.unacked_len))
    }

    /// Discard a range of acknowledged stream data
    pub(super) fn ack(&mut self, mut range: Range<u64>) {
        // Clamp the range to data which is still tracked
        let base_offset = self.base_offset();
        range.start = base_offset.max(range.start);
        range.end = base_offset.max(range.end);

        // A range acknowledged through one copy of a packet is not sent again for another's loss
        if !self.retransmits.is_empty() {
            self.retransmits.remove(range.clone());
        }
        // Acknowledged in order with nothing acknowledged past it, as a stream's data almost always
        // is: the front moves on without the set, where each acknowledged frame cost an insertion
        // into it and its removal again (two searches of a B-tree a frame).
        if range.start == base_offset && self.acks.is_empty() {
            self.advance(range.end.saturating_sub(range.start));
            return;
        }
        self.acks.insert(range);

        while self.acks.min() == Some(self.base_offset()) {
            let Some(prefix) = self.acks.pop_min() else {
                break;
            };
            self.advance(prefix.end.saturating_sub(prefix.start));
        }
    }

    /// Lets go of the first `acked` bytes held, acknowledged: at most what is held.
    fn advance(&mut self, acked: u64) {
        let mut to_advance =
            usize::try_from(acked).map_or(self.unacked_len, |n| n.min(self.unacked_len));
        self.unacked_len = self.unacked_len.saturating_sub(to_advance);
        while to_advance > 0 {
            // `unacked_len` counts the bytes of these segments.
            let Some(front) = self.unacked_segments.front_mut() else {
                break;
            };

            if let Some(rest) = to_advance.checked_sub(front.bytes.len()) {
                to_advance = rest;
                self.unacked_segments.pop_front();
                self.front_trimmed = 0;

                if self.unacked_segments.len().saturating_mul(4) < self.unacked_segments.capacity()
                {
                    self.unacked_segments.shrink_to_fit();
                }
            } else {
                front.bytes.advance(to_advance);
                front.start = front.start.saturating_add(len_u64(to_advance));
                // Within the front segment's length.
                self.front_trimmed = self.front_trimmed.saturating_add(to_advance);
                to_advance = 0;
            }
        }
    }

    /// Compute the next range to transmit on this stream and update state to account for that
    /// transmission.
    ///
    /// `max_len` here includes the space which is available to transmit the
    /// offset and length of the data to send. The caller has to guarantee that
    /// there is at least enough space available to write maximum-sized metadata
    /// (8 byte offset + 8 byte length).
    ///
    /// The method returns a tuple:
    /// - The first return value indicates the range of data to send
    /// - The second return value indicates whether the length needs to be encoded
    ///   in the STREAM frames metadata (`true`), or whether it can be omitted
    ///   since the selected range will fill the whole packet.
    ///
    /// The room left saturates at zero: metadata that does not fit leaves no room for data, where
    /// upstream's subtraction underflowed (it asserted the guarantee in debug builds only).
    pub(super) fn poll_transmit(&mut self, mut max_len: usize) -> (Range<u64>, bool) {
        let mut encode_length = false;

        if let Some(range) = self.retransmits.pop_min() {
            // Retransmit sent data

            // When the offset is known, we know how many bytes are required to encode it.
            // Offset 0 requires no space
            if range.start != 0 {
                max_len = max_len.saturating_sub(VarInt::size_of(range.start));
            }
            if range.end.saturating_sub(range.start) < len_u64(max_len) {
                encode_length = true;
                max_len = max_len.saturating_sub(8);
            }

            let end = range.end.min(len_u64(max_len).saturating_add(range.start));
            if end != range.end {
                self.retransmits.insert(end..range.end);
            }
            return (range.start..end, encode_length);
        }

        // Transmit new data

        // When the offset is known, we know how many bytes are required to encode it.
        // Offset 0 requires no space
        if self.unsent != 0 {
            max_len = max_len.saturating_sub(VarInt::size_of(self.unsent));
        }
        if self.offset.saturating_sub(self.unsent) < len_u64(max_len) {
            encode_length = true;
            max_len = max_len.saturating_sub(8);
        }

        let end = self
            .offset
            .min(len_u64(max_len).saturating_add(self.unsent));
        let result = self.unsent..end;
        self.unsent = end;
        (result, encode_length)
    }

    /// Returns data which is associated with a range
    ///
    /// This function can return a subset of the range, if the data is stored
    /// in noncontiguous fashion in the send buffer. In this case callers
    /// should call the function again with an incremented start offset to
    /// retrieve more data.
    pub(super) fn get(&self, offsets: Range<u64>) -> &[u8] {
        // The pieces are in offset order and none is empty: the one holding `offsets.start` is
        // the last that starts at or before it.
        let after = self
            .unacked_segments
            .partition_point(|piece| piece.start <= offsets.start);
        let Some(piece) = after
            .checked_sub(1)
            .and_then(|at| self.unacked_segments.get(at))
        else {
            return &[];
        };
        // Both are offsets into the piece, a usize long.
        let start =
            usize::try_from(offsets.start.saturating_sub(piece.start)).unwrap_or(usize::MAX);
        let end = usize::try_from(offsets.end.saturating_sub(piece.start))
            .unwrap_or(usize::MAX)
            .min(piece.bytes.len());
        piece.bytes.get(start..end.max(start)).unwrap_or(&[])
    }

    /// Release abandoned data while preserving the final offset for RESET_STREAM
    pub(super) fn discard(&mut self) {
        *self = Self {
            offset: self.offset,
            unsent: self.offset,
            ..Self::default()
        };
    }

    /// Queue a range of sent but unacknowledged data to be retransmitted
    /// Only sent data can be lost (upstream asserted that in debug builds).
    ///
    /// Only what is still unacknowledged goes again: a packet's frames may travel in two packets
    /// (a probe's or a copy's), and the loss of one after the other's acknowledgement leaves
    /// nothing to send.
    pub(super) fn retransmit(&mut self, mut range: Range<u64>) {
        let base_offset = self.base_offset();
        range.start = base_offset.max(range.start);
        range.end = base_offset.max(range.end);
        if range.is_empty() {
            return;
        }
        self.retransmits.insert(range);
        self.retransmits.subtract(&self.acks);
    }

    /// Called before any data is acknowledged, while everything written is held (upstream
    /// asserted that in debug builds).
    pub(super) fn retransmit_all_for_0rtt(&mut self) {
        self.unsent = 0;
    }

    /// First stream offset unwritten by the application, i.e. the offset that the next write will
    /// begin at
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// Whether every byte sent so far has been acknowledged and none waits to be sent again: what
    /// the buffer still holds is unsent (patch Q8)
    pub(super) fn sent_acked(&self) -> bool {
        self.retransmits.is_empty()
            && u64::try_from(self.unacked_len)
                .is_ok_and(|unacked| unacked == self.offset.saturating_sub(self.unsent))
    }

    /// Whether all sent data has been acknowledged
    pub(super) fn is_fully_acked(&self) -> bool {
        self.unacked_len == 0
    }

    /// Whether there's data to send
    ///
    /// There may be sent unacknowledged data even when this is false.
    pub(super) fn has_unsent_data(&self) -> bool {
        self.unsent != self.offset || !self.retransmits.is_empty()
    }

    /// Bytes written by the application and never sent
    pub(super) fn unsent_bytes(&self) -> u64 {
        self.offset.saturating_sub(self.unsent)
    }

    /// Bytes still retained from application writes, including acknowledged data
    pub(super) fn buffered(&self) -> u64 {
        len_u64(self.unacked_len.saturating_add(self.front_trimmed))
    }
}

/// A write still held: its bytes not yet acknowledged, and the stream offset of the first.
#[derive(Debug)]
struct Piece {
    start: u64,
    bytes: Bytes,
}

/// A length in memory as a stream offset: a usize fits a u64 on every supported target.
fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_with_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => 13 byte data isn't enough
        // with 8 bytes reserved for length 11 payload bytes will fit
        assert_eq!(buf.poll_transmit(19), (0..11, true));
        assert_eq!(
            buf.poll_transmit(MSG.len() + 16 - 11),
            (11..MSG.len() as u64, true)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn fragment_without_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with some extra data!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => can be filled by 34 bytes payload
        assert_eq!(buf.poll_transmit(19), (0..19, false));
        assert_eq!(
            buf.poll_transmit(MSG.len() - 19 + 1),
            (19..MSG.len() as u64, false)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn reserves_encoded_offset() {
        let mut buf = SendBuffer::new();

        // Pretend we have more than 1 GB of data in the buffer
        let chunk: Bytes = Bytes::from_static(&[0; 1024 * 1024]);
        for _ in 0..1025 {
            buf.write(chunk.clone());
        }

        const SIZE1: u64 = 64;
        const SIZE2: u64 = 16 * 1024;
        const SIZE3: u64 = 1024 * 1024 * 1024;

        // Offset 0 requires no space
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        buf.retransmit(0..16);
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        let mut transmitted = 16u64;

        // Offset 16 requires 1 byte
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        buf.retransmit(transmitted..SIZE1);
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        transmitted = SIZE1;

        // Offset 64 requires 2 bytes
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        buf.retransmit(transmitted..SIZE2);
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        transmitted = SIZE2;

        // Offset 16384 requires requires 4 bytes
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        buf.retransmit(transmitted..SIZE3);
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        transmitted = SIZE3;

        // Offset 1GB requires 8 bytes
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
        buf.retransmit(transmitted..transmitted + chunk.len() as u64);
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
    }

    #[test]
    fn multiple_segments() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        const MSG_LEN: u64 = MSG.len() as u64;

        const SEG1: &[u8] = b"He";
        buf.write(SEG1.into());
        const SEG2: &[u8] = b"llo,";
        buf.write(SEG2.into());
        const SEG3: &[u8] = b" w";
        buf.write(SEG3.into());
        const SEG4: &[u8] = b"o";
        buf.write(SEG4.into());
        const SEG5: &[u8] = b"rld!";
        buf.write(SEG5.into());

        assert_eq!(aggregate_unacked(&buf), MSG);

        assert_eq!(buf.poll_transmit(16), (0..8, true));
        assert_eq!(buf.get(0..5), SEG1);
        assert_eq!(buf.get(2..8), SEG2);
        assert_eq!(buf.get(6..8), SEG3);

        assert_eq!(buf.poll_transmit(16), (8..MSG_LEN, true));
        assert_eq!(buf.get(8..MSG_LEN), SEG4);
        assert_eq!(buf.get(9..MSG_LEN), SEG5);

        assert_eq!(buf.poll_transmit(42), (MSG_LEN..MSG_LEN, true));

        // Now drain the segments
        buf.ack(0..1);
        assert_eq!(aggregate_unacked(&buf), &MSG[1..]);
        buf.ack(0..3);
        assert_eq!(aggregate_unacked(&buf), &MSG[3..]);
        buf.ack(3..5);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(7..9);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(4..7);
        assert_eq!(aggregate_unacked(&buf), &MSG[9..]);
        buf.ack(0..MSG_LEN);
        assert_eq!(aggregate_unacked(&buf), &[] as &[u8]);
    }

    #[test]
    fn retransmit() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        // Transmit two frames
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        // Lose the first, but not the second
        buf.retransmit(0..16);
        // Ensure we only retransmit the lost frame, then continue sending fresh data
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (23..MSG.len() as u64, true));
        // Lose the second frame
        buf.retransmit(16..23);
        assert_eq!(buf.poll_transmit(16), (16..23, true));
    }

    #[test]
    fn acknowledged_data_is_never_sent_again() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        // A copy of the first frame's packet is acknowledged, then the original is lost: nothing
        // of it goes again.
        buf.ack(0..16);
        buf.retransmit(0..16);
        assert!(buf.retransmits.is_empty());
        // Part of the second acknowledged out of order, then the whole declared lost: only the
        // rest goes again.
        buf.ack(16..20);
        buf.retransmit(16..23);
        assert_eq!(buf.poll_transmit(16), (20..23, true));
        // A retransmission queued and then acknowledged through the other copy is not sent.
        buf.retransmit(20..23);
        buf.ack(20..23);
        assert!(buf.retransmits.is_empty());
        assert_eq!(buf.poll_transmit(16), (23..MSG.len() as u64, true));
    }

    #[test]
    fn ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..8, true));
        buf.ack(0..8);
        assert_eq!(aggregate_unacked(&buf), &MSG[8..]);
    }

    #[test]
    fn reordered_ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        buf.ack(16..23);
        assert_eq!(aggregate_unacked(&buf), MSG);
        buf.ack(0..16);
        assert_eq!(aggregate_unacked(&buf), &MSG[23..]);
        assert!(buf.acks.is_empty());
    }

    /// A read finds its bytes wherever they are held: across hundreds of uneven writes, with
    /// partial and whole acknowledgements trimming the front, every read returns the bytes written
    /// at its offsets, from the piece holding its start (a read crossing a piece's end returns that
    /// piece's rest, and its caller reads on), and nothing before the acknowledged front.
    #[test]
    fn reads_find_their_piece_among_many() {
        let mut buf = SendBuffer::new();
        let mut written = Vec::new();
        let mut state = 0x9e37_79b9_u32;
        let mut next = move || {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            state >> 16
        };
        for i in 0..400usize {
            let len = (next() % 97 + 1) as usize;
            let piece: Vec<u8> = (0..len).map(|j| (i + j) as u8).collect();
            written.extend_from_slice(&piece);
            buf.write(piece.into());
        }
        let total = written.len() as u64;
        while buf.poll_transmit(1200).0.end < total {}
        let mut acked = 0u64;
        while acked < total {
            for _ in 0..8 {
                let start = acked + u64::from(next()) % (total - acked);
                let end = (start + u64::from(next() % 300)).min(total);
                let got = buf.get(start..end);
                assert!(!got.is_empty() || start == end);
                assert!(start + got.len() as u64 <= end);
                let from = start as usize;
                assert_eq!(got, &written[from..from + got.len()]);
            }
            let step = u64::from(next() % 150 + 1).min(total - acked);
            buf.ack(acked..acked + step);
            acked += step;
            assert!(buf.get(0..acked).is_empty());
            assert_eq!(buf.base_offset(), acked);
        }
        assert!(buf.is_fully_acked());
    }

    /// Acknowledgements in order move the front without the set of ranges acknowledged ahead,
    /// and one ahead of the front waits in it until the gap before it is acknowledged, after which
    /// the front has moved past both and the set is empty again.
    #[test]
    fn in_order_acknowledgements_leave_the_set_alone() {
        let mut buf = SendBuffer::new();
        for _ in 0..8 {
            buf.write(Bytes::from_static(&[1; 100]));
        }
        while buf.poll_transmit(64).0.end < 800 {}
        buf.ack(0..150);
        assert!(buf.acks.is_empty());
        assert_eq!(buf.base_offset(), 150);
        buf.ack(300..400);
        assert!(!buf.acks.is_empty());
        assert_eq!(buf.base_offset(), 150);
        buf.ack(150..300);
        assert!(buf.acks.is_empty());
        assert_eq!(buf.base_offset(), 400);
        buf.ack(350..450);
        assert!(buf.acks.is_empty());
        assert_eq!(buf.base_offset(), 450);
        assert_eq!(buf.get(450..460), &[1; 10]);
    }

    fn aggregate_unacked(buf: &SendBuffer) -> Vec<u8> {
        let mut result = Vec::new();
        for segment in buf.unacked_segments.iter().map(|piece| &piece.bytes) {
            result.extend_from_slice(&segment[..]);
        }
        result
    }
}
