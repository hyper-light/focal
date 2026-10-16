//! A part: the unit of the bus a shard owns, replicates and moves. It holds many streams, their
//! batches interleaved in one log, and is written by its owner alone (the single-writer principle,
//! Thompson et al.'s Disruptor; Redpanda's shard-per-core partitions).
//!
//! Appending a batch checks it whole (its CRC and records), gives its records the stream's next
//! offsets, copies it once into the log, stamps its base offset there and chains it after the
//! stream's last batch. A delivery reads the stored bytes as they are, following the chain from
//! the batch it delivered last. A stream's floor moving up lets its batches below go, walking the
//! chain from its first, and a segment none of whose batches is held goes back to the pool.

use crate::BusError;
use crate::batch::{self, Batch};
use crate::log::{PartLog, Position, SegmentPool};
use crate::stream::StreamTable;
use focal_memory::MemoryBudget;

/// What a part holds at most, and the batches it takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartLimits {
    /// Streams the part holds at once.
    pub streams: usize,
    /// A batch's bytes at most.
    pub batch_bytes: usize,
}

/// A batch appended: where it went and the offsets it took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    /// The stream it was appended to.
    pub stream: u64,
    /// The offset of its first record.
    pub base: u64,
    /// Its records.
    pub count: u16,
    /// Its place in the part's log.
    pub position: Position,
}

/// A part's streams and log.
#[derive(Debug)]
pub struct Part {
    log: PartLog,
    streams: StreamTable,
    limits: PartLimits,
}

impl Part {
    /// An empty part: no segment and no stream held; its stream table charged to `budget`.
    pub fn new(limits: PartLimits, budget: MemoryBudget) -> Self {
        Self {
            log: PartLog::new(),
            streams: StreamTable::new(limits.streams, budget),
            limits,
        }
    }

    /// Streams held.
    pub fn streams(&self) -> usize {
        self.streams.len()
    }

    /// The part's stream table.
    pub const fn stream_table(&self) -> &StreamTable {
        &self.streams
    }

    /// The part's log.
    pub const fn log(&self) -> &PartLog {
        &self.log
    }

    /// Checks `bytes` as a batch and appends it to its stream.
    pub fn append(&mut self, pool: &mut SegmentPool, bytes: &[u8]) -> Result<Appended, BusError> {
        let batch = Batch::parse(bytes, self.limits.batch_bytes).map_err(BusError::Batch)?;
        self.append_batch(pool, batch)
    }

    /// [`Self::append`] for a batch whose CRC the transport that carried it verified as it arrived
    /// ([`crate::batch::EMBEDDED`]): every other check is made.
    pub fn append_verified(
        &mut self,
        pool: &mut SegmentPool,
        bytes: &[u8],
    ) -> Result<Appended, BusError> {
        let batch =
            Batch::parse_verified(bytes, self.limits.batch_bytes).map_err(BusError::Batch)?;
        self.append_batch(pool, batch)
    }

    fn append_batch(
        &mut self,
        pool: &mut SegmentPool,
        batch: Batch<'_>,
    ) -> Result<Appended, BusError> {
        let bytes = batch.bytes();
        let stream_id = batch.stream();
        let count = batch.count();
        let stream = self.streams.get_or_insert(stream_id)?;
        let base = stream.next;
        let next = base
            .checked_add(u64::from(count))
            .ok_or(BusError::Capacity("stream offsets"))?;
        let position = self.log.append(pool, bytes)?;
        let word = position
            .to_word()
            .ok_or(BusError::Capacity("log positions"))?;
        if let Some(stored) = self.log.batch_mut(position) {
            batch::stamp(stored, base)?;
        }
        match stream.last {
            Some(last) => {
                if let Some(previous) = self.log.batch_mut(Position::from_word(last)) {
                    batch::stamp_next(previous, word)?;
                }
            }
            None => stream.first = Some(word),
        }
        stream.last = Some(word);
        stream.next = next;
        Ok(Appended {
            stream: stream_id,
            base,
            count,
            position,
        })
    }

    /// The stored batch at `position`, as it is delivered.
    pub fn batch(&self, position: Position) -> Option<Batch<'_>> {
        Some(Batch::trusted(self.log.batch(position)?))
    }

    /// The batch after `batch` in its stream, once it is appended.
    pub fn after(&self, batch: &Batch<'_>) -> Option<Batch<'_>> {
        self.batch(Position::from_word(batch.next()?))
    }

    /// Where the oldest batch `stream` holds is (a position word).
    pub fn first_word(&self, stream: u64) -> Option<u64> {
        self.streams.get(stream)?.first
    }

    /// The oldest batch `stream` holds.
    pub fn first(&self, stream: u64) -> Option<Batch<'_>> {
        self.batch(Position::from_word(self.streams.get(stream)?.first?))
    }

    /// The batch of `stream` holding `offset`, found by walking the stream's chain from its first
    /// batch: a seek, which a live delivery never needs (it follows the chain from the batch it
    /// delivered last).
    pub fn seek(&self, stream: u64, offset: u64) -> Option<Batch<'_>> {
        let mut at = self.first(stream)?;
        loop {
            if offset < at.base_offset() {
                return None;
            }
            if offset < at.end_offset() {
                return Some(at);
            }
            at = self.after(&at)?;
        }
    }

    /// The first offset `stream` still holds and the offset its next record takes.
    pub fn bounds(&self, stream: u64) -> Option<(u64, u64)> {
        let state = self.streams.get(stream)?;
        Some((state.floor, state.next))
    }

    /// Lets `stream`'s batches below `floor` go: a batch goes once every record in it is below.
    pub fn release_below(
        &mut self,
        pool: &mut SegmentPool,
        stream: u64,
        floor: u64,
    ) -> Result<(), BusError> {
        let Some(state) = self.streams.get_mut(stream) else {
            return Err(BusError::UnknownStream(stream));
        };
        let floor = floor.min(state.next);
        if floor <= state.floor {
            return Ok(());
        }
        state.floor = floor;
        while let Some(word) = state.first {
            let position = Position::from_word(word);
            let Some(stored) = self.log.batch(position) else {
                return Err(BusError::Capacity("a held batch's segment was let go"));
            };
            let first = Batch::trusted(stored);
            if first.end_offset() > floor {
                break;
            }
            state.first = first.next();
            if state.first.is_none() {
                state.last = None;
            }
            self.log.release(pool, position)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BatchBuilder, Producing};

    struct Shard {
        pool: SegmentPool,
        part: Part,
        builder: BatchBuilder,
    }

    fn shard(segment: usize) -> Shard {
        let budget = MemoryBudget::new(1 << 30, 0).unwrap();
        Shard {
            pool: SegmentPool::new(segment, budget.clone()).unwrap(),
            part: Part::new(
                PartLimits {
                    streams: 1_000,
                    batch_bytes: segment / 8,
                },
                budget,
            ),
            builder: BatchBuilder::new(),
        }
    }

    impl Shard {
        fn publish(&mut self, stream: u64, payloads: &[&[u8]]) -> Appended {
            self.builder.begin(&Producing {
                stream,
                producer: 1,
                sequence: 0,
                flags: 0,
                timestamp: 1,
            });
            for payload in payloads {
                self.builder.push(1, payload).unwrap();
            }
            let bytes = self.builder.finish().unwrap().to_vec();
            self.part.append(&mut self.pool, &bytes).unwrap()
        }
    }

    #[test]
    fn streams_interleave_in_the_log_and_each_reads_its_own_offsets() {
        let mut shard = shard(64 * 1024);
        for at in 0..300_u64 {
            let stream = at % 3;
            let payload = at.to_le_bytes();
            let appended = shard.publish(stream, &[&payload, &payload]);
            assert_eq!(appended.base, at / 3 * 2);
        }
        for stream in 0..3_u64 {
            assert_eq!(shard.part.bounds(stream), Some((0, 200)));
            // A live tail: the chain from the first batch, every batch of the stream in order.
            let mut at = shard.part.first(stream);
            let mut base = 0;
            while let Some(batch) = at {
                assert_eq!(batch.stream(), stream);
                assert_eq!(batch.base_offset(), base);
                let record = batch.records().next().unwrap().unwrap();
                assert_eq!(record.payload, (base / 2 * 3 + stream).to_le_bytes());
                // The stored batch still passes its producer's CRC with its fields stamped.
                Batch::parse(batch.bytes(), 1 << 20).unwrap();
                base += 2;
                at = shard.part.after(&batch);
            }
            assert_eq!(base, 200);
            for offset in [0, 1, 99, 199] {
                let batch = shard.part.seek(stream, offset).unwrap();
                assert_eq!(batch.base_offset(), offset / 2 * 2);
            }
            assert!(shard.part.seek(stream, 200).is_none());
        }
    }

    #[test]
    fn releasing_every_stream_gives_every_segment_back_but_the_tail() {
        let mut shard = shard(4 * 1024);
        for at in 0..2_000_u64 {
            shard.publish(at % 50, &[&[7; 120]]);
        }
        assert!(shard.pool.in_use() > 50);
        for stream in 0..50 {
            shard
                .part
                .release_below(&mut shard.pool, stream, 20)
                .unwrap();
        }
        // Half of each stream went: the first offset held is 20 and a seek below it finds nothing.
        assert!(shard.part.seek(3, 19).is_none());
        assert_eq!(shard.part.first(3).unwrap().base_offset(), 20);
        for stream in 0..50 {
            shard
                .part
                .release_below(&mut shard.pool, stream, 40)
                .unwrap();
        }
        // Only the tail segment stays, taking batches.
        assert_eq!(shard.pool.in_use(), 1);
        assert!(shard.part.first(3).is_none());
        // A stream that held nothing chains its next batch as its first.
        let appended = shard.publish(3, &[b"again"]);
        assert_eq!(appended.base, 40);
        assert_eq!(shard.part.first(3).unwrap().base_offset(), 40);
    }

    #[test]
    fn a_damaged_batch_changes_nothing() {
        let mut shard = shard(64 * 1024);
        shard.publish(5, &[b"first"]);
        shard.builder.begin(&Producing {
            stream: 5,
            producer: 1,
            sequence: 1,
            flags: 0,
            timestamp: 1,
        });
        shard.builder.push(1, b"second").unwrap();
        let mut bytes = shard.builder.finish().unwrap().to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        assert!(matches!(
            shard.part.append(&mut shard.pool, &bytes),
            Err(BusError::Batch(batch::BatchError::Crc { .. }))
        ));
        assert_eq!(shard.part.bounds(5), Some((0, 1)));
        assert_eq!(shard.part.first(5).unwrap().next(), None);
    }
}
