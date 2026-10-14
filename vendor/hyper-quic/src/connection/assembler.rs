use std::{
    cmp::{Ordering, max},
    collections::{BinaryHeap, binary_heap::PeekMut},
    mem,
};

use bytes::{Buf, Bytes, BytesMut};

use crate::range_set::RangeSet;

/// Helper to assemble unordered stream frames into an ordered stream
#[derive(Debug, Default)]
pub(super) struct Assembler {
    state: State,
    data: BinaryHeap<Buffer>,
    /// Total number of buffered bytes, including duplicates in ordered mode.
    buffered: usize,
    /// Estimated number of allocated bytes, will never be less than `buffered`.
    allocated: usize,
    /// Number of bytes read by the application. When only ordered reads have been used, this is the
    /// length of the contiguous prefix of the stream which has been consumed by the application,
    /// aka the stream offset.
    bytes_read: u64,
    end: u64,
    /// The most spans the stream may hold, for the window its peer had at the last insertion
    /// ([`max_spans`]).
    spans: usize,
}

impl Assembler {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Reset to the initial state
    pub(super) fn reinit(&mut self) {
        let old_data = mem::take(&mut self.data);
        *self = Self::default();
        self.data = old_data;
        self.data.clear();
    }

    pub(super) fn ensure_ordering(&mut self, ordered: bool) -> Result<(), IllegalOrderedRead> {
        if ordered && !self.state.is_ordered() {
            return Err(IllegalOrderedRead);
        } else if !ordered && self.state.is_ordered() {
            // Enter unordered mode
            if !self.data.is_empty() {
                // Get rid of possible duplicates
                self.defragment();
            }
            let mut recvd = RangeSet::new();
            recvd.insert(0..self.bytes_read);
            for chunk in &self.data {
                recvd.insert(chunk.offset..chunk.end());
            }
            self.state = State::Unordered { recvd };
        }
        Ok(())
    }

    /// Get the the next chunk
    pub(super) fn read(&mut self, max_length: usize, ordered: bool) -> Option<Chunk> {
        loop {
            let mut chunk = self.data.peek_mut()?;

            if ordered {
                if chunk.offset > self.bytes_read {
                    // Next chunk is after current read index
                    return None;
                } else if chunk.end() <= self.bytes_read {
                    // Next chunk is useless as the read index is beyond its end
                    self.buffered = self.buffered.saturating_sub(chunk.bytes.len());
                    self.allocated = self.allocated.saturating_sub(chunk.allocation_size);
                    PeekMut::pop(chunk);
                    continue;
                }

                // Determine `start` and `len` of the slice of useful data in chunk: the read
                // index lies within it.
                let start =
                    to_usize(self.bytes_read.saturating_sub(chunk.offset)).min(chunk.bytes.len());
                if start > 0 {
                    chunk.bytes.advance(start);
                    chunk.offset = chunk.offset.saturating_add(len_u64(start));
                    self.buffered = self.buffered.saturating_sub(start);
                }
            }

            // Stream offsets are below 2^62 and lengths are of data in memory: the sums do not
            // saturate.
            return Some(if max_length < chunk.bytes.len() {
                self.bytes_read = self.bytes_read.saturating_add(len_u64(max_length));
                let offset = chunk.offset;
                chunk.offset = chunk.offset.saturating_add(len_u64(max_length));
                self.buffered = self.buffered.saturating_sub(max_length);
                Chunk::new(offset, chunk.bytes.split_to(max_length))
            } else {
                self.bytes_read = self.bytes_read.saturating_add(len_u64(chunk.bytes.len()));
                self.buffered = self.buffered.saturating_sub(chunk.bytes.len());
                self.allocated = self.allocated.saturating_sub(chunk.allocation_size);
                let chunk = PeekMut::pop(chunk);
                Chunk::new(chunk.offset, chunk.bytes)
            });
        }
    }

    /// Copy fragmented chunk data to new chunks backed by a single buffer
    ///
    /// This makes sure we're not unnecessarily holding on to many larger allocations.
    /// We merge contiguous chunks in the process of doing so.
    fn defragment(&mut self) {
        // Chunks smaller than min_chunk_size are merged regardless of fragmentation
        let min_chunk_size = max(
            self.buffered.div_ceil(self.spans.max(MAX_CHUNKS)),
            MIN_RETAINED_CHUNK_SIZE,
        );
        let new = BinaryHeap::with_capacity(self.data.len());
        let old = mem::replace(&mut self.data, new);
        let mut buffers = old.into_sorted_vec();
        self.buffered = 0;
        let mut fragmented_buffered: usize = 0;
        let mut offset = 0;
        for chunk in buffers.iter_mut().rev() {
            chunk.try_mark_defragment(offset);
            let size = chunk.bytes.len();
            offset = chunk.end();
            // Bytes in memory: the sums do not saturate.
            self.buffered = self.buffered.saturating_add(size);
            if !chunk.defragmented || size < min_chunk_size {
                fragmented_buffered = fragmented_buffered.saturating_add(size);
            }
        }
        self.allocated = self.buffered;
        let mut buffer = BytesMut::with_capacity(fragmented_buffered);
        let mut offset: u64 = 0;
        for chunk in buffers.into_iter().rev() {
            // bytes might be empty after try_mark_defragment
            if chunk.bytes.is_empty() {
                continue;
            }
            if chunk.defragmented && chunk.bytes.len() >= min_chunk_size {
                self.data.push(chunk);
                continue;
            }
            // Overlap is resolved by try_mark_defragment
            if chunk.offset != offset.saturating_add(len_u64(buffer.len())) {
                if !buffer.is_empty() {
                    self.data
                        .push(Buffer::new_defragmented(offset, buffer.split().freeze()));
                }
                offset = chunk.offset;
            }
            buffer.extend_from_slice(&chunk.bytes);
        }
        if !buffer.is_empty() {
            self.data
                .push(Buffer::new_defragmented(offset, buffer.split().freeze()));
        }
    }

    // Note: If a packet contains many frames from the same stream, the estimated over-allocation
    // will be much higher because we are counting the same allocation multiple times.
    /// Takes `bytes` at stream offset `offset`, received in a packet whose payload is
    /// `allocation_size` bytes. `limit` is the offset the peer may send up to, its flow control
    /// limit: the spans the stream may hold are bounded by the window between it and what the
    /// application has read ([`max_spans`]), and past that bound the frame is refused.
    pub(super) fn insert(
        &mut self,
        mut offset: u64,
        mut bytes: Bytes,
        allocation_size: usize,
        limit: u64,
    ) -> Result<(), TooManyChunks> {
        self.spans = max_spans(limit.saturating_sub(self.bytes_read));
        // `allocation_size` is at least `bytes.len()` (upstream asserted that in debug builds).
        // Stream offsets are below 2^62, checked when the frame was decoded, so the sums of an
        // offset and a length do not saturate.
        let data_end = offset.saturating_add(len_u64(bytes.len()));
        self.end = self.end.max(data_end);
        if let State::Unordered { ref mut recvd } = self.state {
            // Discard duplicate data
            for duplicate in recvd.replace(offset..data_end) {
                // Each duplicate lies within the data from `offset` on.
                if duplicate.start > offset {
                    let before = to_usize(duplicate.start.saturating_sub(offset)).min(bytes.len());
                    let buffer = Buffer::new(offset, bytes.split_to(before), allocation_size);
                    // Bytes in memory: the sums do not saturate.
                    self.buffered = self.buffered.saturating_add(buffer.bytes.len());
                    self.allocated = self.allocated.saturating_add(buffer.allocation_size);
                    self.data.push(buffer);
                    offset = duplicate.start;
                }
                bytes.advance(to_usize(duplicate.end.saturating_sub(offset)).min(bytes.len()));
                offset = duplicate.end;
            }
        } else if offset < self.bytes_read {
            if data_end <= self.bytes_read {
                return Ok(());
            } else {
                let diff = self.bytes_read.saturating_sub(offset);
                offset = self.bytes_read;
                bytes.advance(to_usize(diff).min(bytes.len()));
            }
        }

        // No early return when empty: the dedup loop above may already have pushed chunks.
        if !bytes.is_empty() {
            let buffer = Buffer::new(offset, bytes, allocation_size);
            // Bytes in memory: the sums do not saturate.
            self.buffered = self.buffered.saturating_add(buffer.bytes.len());
            self.allocated = self.allocated.saturating_add(buffer.allocation_size);
            self.data.push(buffer);
        }
        // `self.buffered` also counts duplicate bytes, therefore we use
        // `self.end - self.bytes_read` as an upper bound of buffered unique
        // bytes. This will cause a defragmentation if the amount of duplicate
        // bytes exceedes a proportion of the receive window size.
        let buffered = self
            .buffered
            .min(to_usize(self.end.saturating_sub(self.bytes_read)));
        // `allocated` is never less than `buffered`.
        let over_allocation = self.allocated.saturating_sub(buffered);
        // Rationale: on the one hand, we want to defragment rarely, ideally never
        // in non-pathological scenarios. However, a pathological or malicious
        // peer could send us one-byte frames, and since we use reference-counted
        // buffers in order to prevent copying, this could result in keeping a lot
        // of memory allocated. This limits over-allocation in proportion to the
        // buffered data. The constants are chosen somewhat arbitrarily and try to
        // balance between defragmentation overhead and over-allocation.
        let threshold = 32768.max(buffered.saturating_mul(3) / 2);
        // Small gapped frames hold over-allocation below the threshold, so bound the count too:
        // compacted past twice the bound, so a flood of mergeable frames cannot force a
        // defragmentation a frame, and refused past the bound itself.
        if over_allocation > threshold || self.data.len() > self.spans.saturating_mul(2) {
            self.defragment();
            if self.data.len() > self.spans {
                return Err(TooManyChunks);
            }
        }

        Ok(())
    }

    /// Number of bytes consumed by the application
    pub(super) fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Discard all buffered data
    pub(super) fn clear(&mut self) {
        self.data.clear();
        self.buffered = 0;
        self.allocated = 0;
    }
}

/// A chunk of data from the receive stream
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    /// The offset in the stream
    pub offset: u64,
    /// The contents of the chunk
    pub bytes: Bytes,
}

impl Chunk {
    fn new(offset: u64, bytes: Bytes) -> Self {
        Self { offset, bytes }
    }
}

#[derive(Debug, Eq)]
struct Buffer {
    offset: u64,
    bytes: Bytes,
    /// Size of the allocation behind `bytes`, if `defragmented == false`.
    /// Otherwise this will be set to `bytes.len()` by `try_mark_defragment`.
    /// Will never be less than `bytes.len()`.
    allocation_size: usize,
    defragmented: bool,
}

impl Buffer {
    /// Constructs a new fragmented Buffer
    fn new(offset: u64, bytes: Bytes, allocation_size: usize) -> Self {
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: false,
        }
    }

    /// The stream offset just past this buffer's data: below 2^62, so the sum does not
    /// saturate.
    fn end(&self) -> u64 {
        self.offset.saturating_add(len_u64(self.bytes.len()))
    }

    /// Constructs a new defragmented Buffer
    fn new_defragmented(offset: u64, bytes: Bytes) -> Self {
        let allocation_size = bytes.len();
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: true,
        }
    }

    /// Discards data before `offset` and flags `self` as defragmented if it has good utilization
    fn try_mark_defragment(&mut self, offset: u64) {
        let duplicate = to_usize(offset.saturating_sub(self.offset));
        self.offset = self.offset.max(offset);
        if duplicate >= self.bytes.len() {
            // All bytes are duplicate
            self.bytes = Bytes::new();
            self.defragmented = true;
            self.allocation_size = 0;
            return;
        }
        self.bytes.advance(duplicate);
        // Make sure that fragmented buffers with high utilization become defragmented and
        // defragmented buffers remain defragmented
        self.defragmented =
            self.defragmented || self.bytes.len().saturating_mul(6) / 5 >= self.allocation_size;
        if self.defragmented {
            // Make sure that defragmented buffers do not contribute to over-allocation
            self.allocation_size = self.bytes.len();
        }
    }
}

impl Ord for Buffer {
    // Invert ordering based on offset (max-heap, min offset first),
    // prioritize longer chunks at the same offset.
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset
            .cmp(&other.offset)
            .reverse()
            .then(self.bytes.len().cmp(&other.bytes.len()))
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        (self.offset, self.bytes.len()) == (other.offset, other.bytes.len())
    }
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Ordered,
    Unordered {
        /// The set of offsets that have been received from the peer, including portions not yet
        /// read by the application.
        recvd: RangeSet,
    },
}

impl State {
    fn is_ordered(&self) -> bool {
        matches!(self, Self::Ordered)
    }
}

/// Error indicating that an ordered read was performed on a stream after an unordered read
#[derive(Debug)]
pub struct IllegalOrderedRead;

/// Error indicating that too many chunks are buffered due to maliciously small/gapped frames
#[derive(Debug)]
pub(crate) struct TooManyChunks;

/// The fewest spans any stream may hold: upstream's fixed bound on the distinct spans kept for a
/// stream ("ngtcp2 uses a threshold of 4000 -- try to be a little more conservative?"), which
/// hyper-quic keeps as the floor of [`max_spans`]. A window of frames of at least `d` bytes, every
/// other one lost, holds at most `window / (2d)` spans, so a window of at most `2 · 1,024 · d`
/// never reaches it whatever its spans cost (patch Q5).
pub const MAX_CHUNKS: usize = 1024;

/// The most spans a stream may hold out of order when its peer may send `window` bytes past what
/// the application has read: as many as keep the spans' own memory (a [`Buffer`] each) within the
/// window at their most, twice the bound before a compaction, and never fewer than
/// [`MAX_CHUNKS`].
///
/// A peer opens a span with each frame it sends past a gap, so the spans' memory is what it can
/// make the receiver hold beyond the window's data. Bounded by the window, it stays within what
/// the window already grants, as Linux budgets a receive buffer's overhead at the advertised
/// window (`tcp_adv_win_scale` = 1: "Count buffering overhead as bytes/2^tcp_adv_win_scale",
/// Documentation/networking/ip-sysctl.rst). A legitimate sender's frames carry a packet's stream
/// bytes, `d`, so a window with every other frame lost holds `window / (2d)` spans, inside the
/// bound for frames of at least a [`Buffer`]'s size. Upstream's fixed 1,024 closed the connection
/// with "too many gaps in stream buffer" once such a window passed `2 · 1,024 · d`, 2.3 MB at the
/// smallest datagram: a slow start's overshoot at 100 Mbit/s and 100 ms left 1,025 spans in a
/// 4.8 MB reach (`docs/benchmarks.md`, "slates' congestion bake-off on hyper-quic").
fn max_spans(window: u64) -> usize {
    let per_span = u64::try_from(mem::size_of::<Buffer>().saturating_mul(2)).unwrap_or(u64::MAX);
    usize::try_from(window.checked_div(per_span).unwrap_or(0))
        .unwrap_or(usize::MAX)
        .max(MAX_CHUNKS)
}

/// Minimum size of a defragmented chunk that is retained without coalescing.
///
/// Chunks below this size will be copied on every compaction. 128 bytes balances
/// being cheap to copy while reducing the overhead of many small buffer nodes.
const MIN_RETAINED_CHUNK_SIZE: usize = 128;

/// A length in memory as a stream offset: a usize fits a u64 on every supported target.
fn len_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// A stream distance as a length in memory: one larger than a usize is larger than any data
/// held, which every use clamps to.
fn to_usize(distance: u64) -> usize {
    usize::try_from(distance).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    /// A peer's limit at or below what was read: the stream holds the floor's spans,
    /// [`MAX_CHUNKS`], upstream's bound.
    const AT_FLOOR: u64 = 0;

    #[test]
    fn assemble_ordered() {
        let mut x = Assembler::new();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 1), Some(ref y) if &y[..] == b"1");
        assert_matches!(next(&mut x, 3), Some(ref y) if &y[..] == b"23");
        x.insert(3, Bytes::from_static(b"456"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        x.insert(6, Bytes::from_static(b"789"), 3, AT_FLOOR)
            .unwrap();
        x.insert(9, Bytes::from_static(b"10"), 2, AT_FLOOR).unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"789");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"10");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_unordered() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"456"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), None);
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_duplicate_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5, AT_FLOOR)
            .unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contained_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"12345"), 5, AT_FLOOR)
            .unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_contains_compact() {
        let mut x = Assembler::new();
        x.insert(1, Bytes::from_static(b"234"), 3, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"12345"), 5, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 3, AT_FLOOR)
            .unwrap();
        x.insert(1, Bytes::from_static(b"234"), 3, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"4");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_overlapping_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"123"), 4, AT_FLOOR)
            .unwrap();
        x.insert(1, Bytes::from_static(b"234"), 4, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1, AT_FLOOR).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1, AT_FLOOR).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1, AT_FLOOR).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_complex_compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1"), 1, AT_FLOOR).unwrap();
        x.insert(2, Bytes::from_static(b"3"), 1, AT_FLOOR).unwrap();
        x.insert(4, Bytes::from_static(b"5"), 1, AT_FLOOR).unwrap();
        x.insert(0, Bytes::from_static(b"123456"), 6, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn assemble_old() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"1234"), 4, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
        x.insert(0, Bytes::from_static(b"1234"), 4, AT_FLOOR)
            .unwrap();
        assert_matches!(next(&mut x, 32), None);
    }

    #[test]
    fn compact() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 4, AT_FLOOR)
            .unwrap();
        x.insert(3, Bytes::from_static(b"def"), 4, AT_FLOOR)
            .unwrap();
        x.insert(9, Bytes::from_static(b"jkl"), 4, AT_FLOOR)
            .unwrap();
        x.insert(12, Bytes::from_static(b"mno"), 4, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abcdef"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jklmno"))
        );
    }

    #[test]
    fn defrag_with_missing_prefix() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
    }

    #[test]
    fn defrag_read_chunk() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 4, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 4, AT_FLOOR)
            .unwrap();
        x.insert(7, Bytes::from_static(b"hij"), 4, AT_FLOOR)
            .unwrap();
        x.insert(11, Bytes::from_static(b"lmn"), 4, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"abcdef");
        x.insert(5, Bytes::from_static(b"fghijklmn"), 9, AT_FLOOR)
            .unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"ghijklmn");
        x.insert(13, Bytes::from_static(b"nopq"), 4, AT_FLOOR)
            .unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"opq");
        x.insert(15, Bytes::from_static(b"pqrs"), 4, AT_FLOOR)
            .unwrap();
        assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"rs");
        assert_matches!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn unordered_happy_path() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"abc"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(3, Bytes::from_static(b"def"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn unordered_dedup() {
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        x.insert(3, Bytes::from_static(b"def"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(3, Bytes::from_static(b"def"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(0, Bytes::from_static(b"a"), 1, AT_FLOOR).unwrap();
        x.insert(0, Bytes::from_static(b"abcdefghi"), 9, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(0, Bytes::from_static(b"a"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(1, Bytes::from_static(b"bc"))
        );
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(6, Bytes::from_static(b"ghi"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(9, Bytes::from_static(b"jkl"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(12, Bytes::from_static(b"mno"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(
            next_unordered(&mut x),
            Chunk::new(12, Bytes::from_static(b"mno"))
        );
        assert_eq!(x.read(usize::MAX, false), None);
        x.insert(2, Bytes::from_static(b"cde"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(x.read(usize::MAX, false), None);
    }

    #[test]
    fn chunks_dedup() {
        let mut x = Assembler::new();
        x.insert(3, Bytes::from_static(b"def"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(0, Bytes::from_static(b"a"), 1, AT_FLOOR).unwrap();
        x.insert(1, Bytes::from_static(b"bcdefghi"), 9, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"abcd"), 4, AT_FLOOR)
            .unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abcd")))
        );
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(4, Bytes::from_static(b"efghi")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(8, Bytes::from_static(b"ijkl"), 4, AT_FLOOR)
            .unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(9, Bytes::from_static(b"jkl")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(12, Bytes::from_static(b"mno"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(12, Bytes::from_static(b"mno")))
        );
        assert_eq!(x.read(usize::MAX, true), None);
        x.insert(2, Bytes::from_static(b"cde"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(x.read(usize::MAX, true), None);
    }

    #[test]
    fn ordered_eager_discard() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3, AT_FLOOR)
            .unwrap();
        assert_eq!(x.data.len(), 1);
        assert_eq!(
            x.read(usize::MAX, true),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        x.insert(0, Bytes::from_static(b"ab"), 2, AT_FLOOR).unwrap();
        assert_eq!(x.data.len(), 0);
        x.insert(2, Bytes::from_static(b"cd"), 2, AT_FLOOR).unwrap();
        assert_eq!(
            x.data.peek(),
            Some(&Buffer::new(3, Bytes::from_static(b"d"), 2))
        );
    }

    #[test]
    fn ordered_insert_unordered_read() {
        let mut x = Assembler::new();
        x.insert(0, Bytes::from_static(b"abc"), 3, AT_FLOOR)
            .unwrap();
        x.insert(0, Bytes::from_static(b"abc"), 3, AT_FLOOR)
            .unwrap();
        x.ensure_ordering(false).unwrap();
        assert_eq!(
            x.read(3, false),
            Some(Chunk::new(0, Bytes::from_static(b"abc")))
        );
        assert_eq!(x.read(3, false), None);
    }

    #[test]
    fn ordered_lossless_stream_is_not_rejected() {
        const FRAME_LEN: usize = 32;

        let mut x = Assembler::new();
        let data = Bytes::from(vec![0; FRAME_LEN]);
        let mut offset = 0u64;
        while offset < (2 * (2 * MAX_CHUNKS) * FRAME_LEN) as u64 {
            x.insert(offset, data.clone(), FRAME_LEN, AT_FLOOR)
                .expect("contiguous lossless data must not be rejected");
            offset += FRAME_LEN as u64;
        }
    }

    #[test]
    fn defrag_respects_max_chunks_at_rounding_boundary() {
        let mut x = Assembler::new();
        let data = Bytes::from(vec![0; MIN_RETAINED_CHUNK_SIZE]);
        let mut offset = 0;
        // Fill with minimium sized chunks.
        for _ in 0..MAX_CHUNKS {
            x.insert(offset, data.clone(), data.len(), AT_FLOOR)
                .unwrap();
            offset += data.len() as u64;
        }
        // Add 1 extra chunk, defragment must coalesce the previous
        // chunks to stay within MAX_CHUNKS
        x.insert(offset, Bytes::from_static(b"x"), 1, AT_FLOOR)
            .unwrap();
        x.defragment();
        assert!(x.data.len() <= MAX_CHUNKS);
    }

    #[test]
    fn bounded_chunks_under_low_over_allocation() {
        // Gapped frames whose `allocation_size` equals their length hold
        // `over_allocation` at zero, so that trigger never fires.
        let mut x = Assembler::new();
        // Withhold offset 0 so an ordered reader can never drain anything.
        let mut offset = 1u64;
        let mut result = Ok(());
        for _ in 0..(MAX_CHUNKS * 8) {
            result = x.insert(offset, Bytes::from_static(b"gap"), 3, AT_FLOOR);
            if result.is_err() {
                break;
            }
            offset += 3 + 1; // 3 data bytes, 1 byte gap
        }
        assert_matches!(result, Err(TooManyChunks));
        assert!(
            x.data.len() <= (2 * MAX_CHUNKS) + 1,
            "chunk count {} exceeded the bound",
            x.data.len()
        );
    }

    #[test]
    fn bounded_chunks_unordered_overlap_flood() {
        // Overlapping frames whose tail is already received: the dedup loop pushes
        // the fresh head byte and leaves `bytes` empty, and `end` never rises, so
        // the flood costs the peer no flow control.
        let mut x = Assembler::new();
        x.ensure_ordering(false).unwrap();
        let top = 1_000_000u64;
        x.insert(top, Bytes::from_static(b"ab"), 2, AT_FLOOR)
            .unwrap();
        for k in 0..(4 * MAX_CHUNKS as u64) {
            x.insert(top - k - 1, Bytes::from_static(b"ab"), 2, AT_FLOOR)
                .unwrap();
            assert!(
                x.data.len() <= (2 * MAX_CHUNKS) + 1,
                "chunk count {} exceeded the bound at k={k}",
                x.data.len()
            );
        }
    }

    #[test]
    fn bounded_chunks_duplicate_flood() {
        // Duplicates against a stream already at the cap. Ordered mode does not dedup,
        // so each one pushes a chunk; they must not be rejected, or compact every frame.
        let mut x = Assembler::new();
        // Withhold offset 0 so nothing can be drained.
        for i in 0..MAX_CHUNKS as u64 {
            x.insert(1 + i * 4, Bytes::from_static(b"abc"), 3, AT_FLOOR)
                .unwrap();
        }
        let mut max_len = x.data.len();
        for _ in 0..(3 * MAX_CHUNKS) {
            x.insert(1, Bytes::from_static(b"abc"), 3, AT_FLOOR)
                .expect("duplicate flood must not be rejected");
            max_len = max_len.max(x.data.len());
            assert!(
                x.data.len() <= (2 * MAX_CHUNKS) + 1,
                "chunk count {} exceeded the bound",
                x.data.len()
            );
        }
        // Compacting on every frame would pin the count at `MAX_CHUNKS`.
        assert!(
            max_len > MAX_CHUNKS,
            "buffer compacted on every frame (max observed len {max_len})"
        );
    }

    /// A window of frames of a packet's stream bytes with every other frame lost is held whole,
    /// and read back byte for byte once the lost frames come: upstream's fixed bound refused its
    /// 1,025th span, a little past 2.3 MB.
    #[test]
    fn a_window_with_every_other_frame_lost_is_held() {
        // A packet's stream bytes at the smallest datagram (hyper-transport's `d`)
        const FRAME: u64 = 1_134;
        const WINDOW: u64 = 10 << 20;
        let frame = |k: u64| Bytes::from(vec![(k % 251) as u8; FRAME as usize]);
        let mut x = Assembler::new();
        let frames = WINDOW / FRAME;
        for k in (1..frames).step_by(2) {
            x.insert(k * FRAME, frame(k), FRAME as usize, WINDOW)
                .expect("a legitimate sender's window is held");
        }
        assert!(x.data.len() > MAX_CHUNKS, "{} spans", x.data.len());
        for k in (0..frames).step_by(2) {
            x.insert(k * FRAME, frame(k), FRAME as usize, WINDOW)
                .unwrap();
        }
        let mut read = 0;
        while let Some(chunk) = x.read(usize::MAX, true) {
            assert_eq!(chunk.offset, read);
            for (at, byte) in chunk.bytes.iter().enumerate() {
                assert_eq!(u64::from(*byte), ((read + at as u64) / FRAME) % 251);
            }
            read += chunk.bytes.len() as u64;
        }
        assert_eq!(read, frames * FRAME);
    }

    /// A peer of one-byte frames, each past a gap, is refused once its spans pass what the window
    /// pays for, and the spans held never cost more than the window.
    #[test]
    fn spans_past_what_the_window_pays_for_are_refused() {
        let bound = 4 * MAX_CHUNKS;
        let window = (bound * 2 * mem::size_of::<Buffer>()) as u64;
        assert_eq!(max_spans(window), bound);
        let mut x = Assembler::new();
        // Withhold offset 0 so nothing can be read
        let mut offset = 1;
        let mut refused = false;
        for _ in 0..(4 * bound) {
            if x.insert(offset, Bytes::from_static(b"a"), 1, window)
                .is_err()
            {
                refused = true;
                break;
            }
            assert!(
                (x.data.len() * mem::size_of::<Buffer>()) as u64 <= window,
                "{} spans",
                x.data.len()
            );
            offset += 2;
        }
        assert!(refused);
        assert_eq!(x.data.len(), 2 * bound + 1);
    }

    fn next_unordered(x: &mut Assembler) -> Chunk {
        x.read(usize::MAX, false).unwrap()
    }

    fn next(x: &mut Assembler, size: usize) -> Option<Bytes> {
        x.read(size, true).map(|chunk| chunk.bytes)
    }
}
