//! A batch: one producer's burst of records to one stream. It is the unit the bus appends,
//! replicates, flushes and delivers, the same bytes in each place (Kafka's record batch, KIP-98's
//! format v2, is the precedent: the producer builds it, the broker checks and stores it whole).
//!
//! The layout, every integer little-endian:
//!
//! | bytes | field | written by |
//! |---|---|---|
//! | 0..4 | `length`: the batch's bytes, header included | producer |
//! | 4..8 | `crc`: CRC-32C (Castagnoli) of bytes `24..length` | producer |
//! | 8..16 | `base_offset`: the stream offset of the first record | the part's owner |
//! | 16..24 | `next`: where the stream's next batch is in this part's log | the part's owner |
//! | 24..32 | `stream` | producer |
//! | 32..40 | `producer`: the producing session | producer |
//! | 40..44 | `sequence`: the producer's sequence of the first record | producer |
//! | 44..46 | `count`: records, at least one | producer |
//! | 46..48 | `flags`: the payloads' codec and dictionary, and the class | producer |
//! | 48..56 | `timestamp`: the first record's time, µs since the Unix epoch | producer |
//! | 56.. | records | producer |
//!
//! `next` chains a stream's batches through the part's log, so the log is its own index: a stream
//! costs the places of its first and last batch held, whatever it holds, where a separate index
//! would cost an entry a batch and a block a stream (step 1 measured a 2 KiB block a stream at 1 to
//! 5.3 times an event's payload for sparse streams). It names a place in this node's log alone; a
//! replica or a receiver ignores it, and no CRC covers it.
//!
//! A record is its payload's length and its time after the batch's, each an unsigned LEB128, then
//! the payload: opaque bytes, compressed by the producer or not, which the bus never reads.
//!
//! The CRC covers everything the producer wrote except the length (the frame the transport and
//! the log delimit by) and nothing the owner stamps: the owner writes `base_offset` and `next` in place without
//! touching the CRC, as Kafka leaves `baseOffset` and `partitionLeaderEpoch` outside its batch CRC
//! so a broker assigns them without recomputing it. The receiver of a delivered batch checks the
//! same CRC the producer computed, so a fault anywhere between (a core, a disk, a copy) is caught
//! end to end (Hochschild et al., "Cores that don't count", HotOS 2021; Dixit et al., "Silent Data
//! Corruptions at Scale", 2021).

use crate::BusError;

/// The header's bytes.
pub const HEADER: usize = 56;
/// The least a batch can be: its header and one empty record of two one-byte varints.
pub const MIN_BATCH: usize = HEADER + 2;

const LENGTH: core::ops::Range<usize> = 0..4;
const CRC: core::ops::Range<usize> = 4..8;
const BASE_OFFSET: core::ops::Range<usize> = 8..16;
const NEXT: core::ops::Range<usize> = 16..24;
const STREAM: core::ops::Range<usize> = 24..32;
const PRODUCER: core::ops::Range<usize> = 32..40;
const SEQUENCE: core::ops::Range<usize> = 40..44;
const COUNT: core::ops::Range<usize> = 44..46;
const FLAGS: core::ops::Range<usize> = 46..48;
const TIMESTAMP: core::ops::Range<usize> = 48..56;
/// Where the CRC starts: after the fields the owner stamps.
const COVERED_FROM: usize = 24;
/// Where a batch's CRC is and what it covers, as the transport that carries it checks it
/// (`hyper_transport::Classes::embedded_checksum`, `wire`): the transport's own checksum covers
/// the rest, so each byte of a publish or a delivery is summed once on its way.
pub const EMBEDDED: hyper_transport::Embedded = hyper_transport::Embedded {
    covered_from: COVERED_FROM,
    at: CRC.start,
};
/// `next` before the stream's next batch is appended.
pub const NO_NEXT: u64 = u64::MAX;

/// What is wrong with bytes offered as a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchError {
    /// Fewer bytes than a header and one record.
    Short,
    /// The length field is not the number of bytes offered.
    Length { stated: u32, offered: usize },
    /// The CRC does not match the bytes it covers.
    Crc { stated: u32, computed: u32 },
    /// No record, or more records stated than the bytes hold.
    Count,
    /// A record's varint or payload runs past the batch, or bytes are left after the last record.
    Record,
    /// Larger than the bound the caller allows.
    TooLarge { bytes: usize, limit: usize },
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Short => f.write_str("batch shorter than its header and one record"),
            Self::Length { stated, offered } => {
                write!(f, "batch states {stated} bytes but {offered} were offered")
            }
            Self::Crc { stated, computed } => {
                write!(
                    f,
                    "batch CRC {stated:#010x} but its bytes give {computed:#010x}"
                )
            }
            Self::Count => f.write_str("batch states no record, or more than it holds"),
            Self::Record => f.write_str("batch record runs past its end, or bytes follow the last"),
            Self::TooLarge { bytes, limit } => {
                write!(f, "batch of {bytes} bytes is over the {limit}-byte bound")
            }
        }
    }
}

impl std::error::Error for BatchError {}

fn le_u16(bytes: &[u8], at: core::ops::Range<usize>) -> Option<u16> {
    bytes
        .get(at)
        .and_then(|field| field.try_into().ok())
        .map(u16::from_le_bytes)
}

fn le_u32(bytes: &[u8], at: core::ops::Range<usize>) -> Option<u32> {
    bytes
        .get(at)
        .and_then(|field| field.try_into().ok())
        .map(u32::from_le_bytes)
}

fn le_u64(bytes: &[u8], at: core::ops::Range<usize>) -> Option<u64> {
    bytes
        .get(at)
        .and_then(|field| field.try_into().ok())
        .map(u64::from_le_bytes)
}

/// An unsigned LEB128 at the start of `bytes`: its value and how many bytes it took. At most ten
/// bytes, the last of which may carry only one bit of a 64-bit value.
fn varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0_u64;
    for (at, byte) in bytes.iter().take(10).enumerate() {
        let shift = u32::try_from(at).ok()?.checked_mul(7)?;
        let bits = u64::from(byte & 0x7F);
        if shift == 63 && bits > 1 {
            return None;
        }
        value |= bits.checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some((value, at.checked_add(1)?));
        }
    }
    None
}

/// The most a record's head takes: its length and its delta, each a `u64` in LEB128, ten bytes at
/// most.
const RECORD_HEAD_MAX: usize = 20;

fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let low = u8::try_from(value & 0x7F).unwrap_or(0);
        value = value.wrapping_shr(7);
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

/// A batch's bytes, checked: its length, CRC and records agree.
#[derive(Clone, Copy, Debug)]
pub struct Batch<'a> {
    bytes: &'a [u8],
}

impl<'a> Batch<'a> {
    /// Checks `bytes` as a whole batch of at most `limit` bytes.
    pub fn parse(bytes: &'a [u8], limit: usize) -> Result<Self, BatchError> {
        Self::check(bytes, limit, true)
    }

    /// [`Self::parse`] but for the CRC: for a batch whose CRC the transport that carried it
    /// verified as it arrived ([`EMBEDDED`]). A node summed each publish twice, the transport's
    /// sum and this, before the transport checked the batch's own.
    pub fn parse_verified(bytes: &'a [u8], limit: usize) -> Result<Self, BatchError> {
        Self::check(bytes, limit, false)
    }

    fn check(bytes: &'a [u8], limit: usize, crc: bool) -> Result<Self, BatchError> {
        if bytes.len() > limit {
            return Err(BatchError::TooLarge {
                bytes: bytes.len(),
                limit,
            });
        }
        if bytes.len() < MIN_BATCH {
            return Err(BatchError::Short);
        }
        let stated = le_u32(bytes, LENGTH).ok_or(BatchError::Short)?;
        if usize::try_from(stated).ok() != Some(bytes.len()) {
            return Err(BatchError::Length {
                stated,
                offered: bytes.len(),
            });
        }
        if crc {
            let stated_crc = le_u32(bytes, CRC).ok_or(BatchError::Short)?;
            let covered = bytes.get(COVERED_FROM..).ok_or(BatchError::Short)?;
            let computed = crc32c(covered);
            if computed != stated_crc {
                return Err(BatchError::Crc {
                    stated: stated_crc,
                    computed,
                });
            }
        }
        let count = le_u16(bytes, COUNT).ok_or(BatchError::Short)?;
        if count == 0 {
            return Err(BatchError::Count);
        }
        let batch = Self { bytes };
        let mut seen = 0_u16;
        let mut records = batch.records();
        for record in records.by_ref() {
            record?;
            seen = seen.checked_add(1).ok_or(BatchError::Count)?;
        }
        if seen != count {
            return Err(BatchError::Count);
        }
        if records.rest != 0 {
            return Err(BatchError::Record);
        }
        Ok(batch)
    }

    /// A batch whose bytes were checked when they were stored; their reader has kept them since
    /// (the part's own log), so no check is repeated.
    pub(crate) const fn trusted(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// The batch's bytes, as they are stored and delivered.
    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// The stream offset of the first record; zero until the owner stamps it.
    pub fn base_offset(&self) -> u64 {
        le_u64(self.bytes, BASE_OFFSET).unwrap_or(0)
    }

    /// The offset after the batch's last record.
    pub fn end_offset(&self) -> u64 {
        self.base_offset().saturating_add(u64::from(self.count()))
    }

    /// Where the stream's next batch is in the part's log, once it is appended.
    pub fn next(&self) -> Option<u64> {
        le_u64(self.bytes, NEXT).filter(|next| *next != NO_NEXT)
    }

    /// The stream the batch is for.
    pub fn stream(&self) -> u64 {
        le_u64(self.bytes, STREAM).unwrap_or(0)
    }

    /// The producing session.
    pub fn producer(&self) -> u64 {
        le_u64(self.bytes, PRODUCER).unwrap_or(0)
    }

    /// The producer's sequence of the first record.
    pub fn sequence(&self) -> u32 {
        le_u32(self.bytes, SEQUENCE).unwrap_or(0)
    }

    /// Records in the batch.
    pub fn count(&self) -> u16 {
        le_u16(self.bytes, COUNT).unwrap_or(0)
    }

    /// The producer's flags: codec, dictionary and class.
    pub fn flags(&self) -> u16 {
        le_u16(self.bytes, FLAGS).unwrap_or(0)
    }

    /// The first record's time, µs since the Unix epoch.
    pub fn timestamp(&self) -> u64 {
        le_u64(self.bytes, TIMESTAMP).unwrap_or(0)
    }

    /// The records, in order.
    pub fn records(&self) -> Records<'a> {
        let body = self.bytes.get(HEADER..).unwrap_or(&[]);
        Records {
            body,
            at: 0,
            rest: body.len(),
            time: self.timestamp(),
        }
    }
}

/// One record of a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    /// The record's time, µs since the Unix epoch.
    pub timestamp: u64,
    /// Its payload, as the producer wrote it.
    pub payload: &'a [u8],
}

/// The records of a batch, read in order.
#[derive(Clone, Debug)]
pub struct Records<'a> {
    body: &'a [u8],
    at: usize,
    /// Bytes not yet read.
    rest: usize,
    time: u64,
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<Record<'a>, BatchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest == 0 {
            return None;
        }
        let item = (|| {
            let tail = self.body.get(self.at..).ok_or(BatchError::Record)?;
            let (length, took_length) = varint(tail).ok_or(BatchError::Record)?;
            let tail = tail.get(took_length..).ok_or(BatchError::Record)?;
            let (delta, took_delta) = varint(tail).ok_or(BatchError::Record)?;
            let length = usize::try_from(length).map_err(|_| BatchError::Record)?;
            let payload = tail
                .get(took_delta..)
                .and_then(|tail| tail.get(..length))
                .ok_or(BatchError::Record)?;
            let took = took_length
                .checked_add(took_delta)
                .and_then(|took| took.checked_add(length))
                .ok_or(BatchError::Record)?;
            self.at = self.at.checked_add(took).ok_or(BatchError::Record)?;
            self.rest = self.rest.checked_sub(took).ok_or(BatchError::Record)?;
            Ok(Record {
                timestamp: self.time.saturating_add(delta),
                payload,
            })
        })();
        if item.is_err() {
            // A malformed record ends the reading: nothing after it can be found.
            self.rest = 0;
        }
        Some(item)
    }
}

/// Writes `base_offset` into a stored batch, its `next` none yet.
pub(crate) fn stamp(bytes: &mut [u8], base_offset: u64) -> Result<(), BusError> {
    bytes
        .get_mut(BASE_OFFSET)
        .ok_or(BusError::Batch(BatchError::Short))?
        .copy_from_slice(&base_offset.to_le_bytes());
    stamp_next(bytes, NO_NEXT)
}

/// Writes where the stream's next batch is into a stored batch.
pub(crate) fn stamp_next(bytes: &mut [u8], next: u64) -> Result<(), BusError> {
    bytes
        .get_mut(NEXT)
        .ok_or(BusError::Batch(BatchError::Short))?
        .copy_from_slice(&next.to_le_bytes());
    Ok(())
}

/// The length a stored batch states: what a reader of the log takes after the batch's place.
pub(crate) fn stated_length(bytes: &[u8]) -> Option<usize> {
    usize::try_from(le_u32(bytes, LENGTH)?).ok()
}

/// What a producer states once for a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Producing {
    /// The stream the batch is for.
    pub stream: u64,
    /// The producing session.
    pub producer: u64,
    /// The producer's sequence of the batch's first record.
    pub sequence: u32,
    /// Codec, dictionary and class.
    pub flags: u16,
    /// The first record's time, µs since the Unix epoch.
    pub timestamp: u64,
}

/// Builds a batch into a buffer it keeps: a producer's side of the format.
#[derive(Debug, Default)]
pub struct BatchBuilder {
    bytes: Vec<u8>,
    count: u16,
    timestamp: u64,
}

impl BatchBuilder {
    /// A builder with no batch begun.
    pub fn new() -> Self {
        Self::default()
    }

    /// Begins a batch for `producing`, dropping any unfinished one and keeping the buffer.
    pub fn begin(&mut self, producing: &Producing) {
        self.bytes.clear();
        self.bytes.extend_from_slice(&[0; 16]);
        self.bytes.extend_from_slice(&NO_NEXT.to_le_bytes());
        self.bytes
            .extend_from_slice(&producing.stream.to_le_bytes());
        self.bytes
            .extend_from_slice(&producing.producer.to_le_bytes());
        self.bytes
            .extend_from_slice(&producing.sequence.to_le_bytes());
        self.bytes.extend_from_slice(&0_u16.to_le_bytes());
        self.bytes.extend_from_slice(&producing.flags.to_le_bytes());
        self.bytes
            .extend_from_slice(&producing.timestamp.to_le_bytes());
        self.count = 0;
        self.timestamp = producing.timestamp;
    }

    /// The batch's bytes so far, its header's included.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether no batch is begun.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The records pushed since the batch began.
    pub const fn count(&self) -> u16 {
        self.count
    }

    /// The most a record of `payload` bytes adds to a batch: its length's and its delta's varints,
    /// ten bytes each at most, and its bytes.
    pub const fn record_bytes(payload: usize) -> usize {
        payload.saturating_add(RECORD_HEAD_MAX)
    }

    /// Adds a record of `payload` at `timestamp` (µs; not before the batch's first).
    pub fn push(&mut self, timestamp: u64, payload: &[u8]) -> Result<(), BusError> {
        self.count = self
            .count
            .checked_add(1)
            .ok_or(BusError::Batch(BatchError::Count))?;
        push_varint(
            &mut self.bytes,
            u64::try_from(payload.len()).unwrap_or(u64::MAX),
        );
        push_varint(&mut self.bytes, timestamp.saturating_sub(self.timestamp));
        self.bytes.extend_from_slice(payload);
        Ok(())
    }

    /// Finishes the batch: its count, length and CRC written. The bytes stay the builder's until
    /// the next [`Self::begin`].
    pub fn finish(&mut self) -> Result<&[u8], BusError> {
        let length = u32::try_from(self.bytes.len()).map_err(|_| {
            BusError::Batch(BatchError::TooLarge {
                bytes: self.bytes.len(),
                limit: usize::try_from(u32::MAX).unwrap_or(usize::MAX),
            })
        })?;
        let short = || BusError::Batch(BatchError::Short);
        self.bytes
            .get_mut(COUNT)
            .ok_or_else(short)?
            .copy_from_slice(&self.count.to_le_bytes());
        self.bytes
            .get_mut(LENGTH)
            .ok_or_else(short)?
            .copy_from_slice(&length.to_le_bytes());
        let crc = crc32c(self.bytes.get(COVERED_FROM..).ok_or_else(short)?);
        self.bytes
            .get_mut(CRC)
            .ok_or_else(short)?
            .copy_from_slice(&crc.to_le_bytes());
        Ok(&self.bytes)
    }
}

/// The CRC-32C (`CRC-32/ISCSI`) of `bytes`, crc-fast's, which chooses its widest carry-less multiply
/// when it runs: the crc32c crate took its slow path in every build for the generic target (135.5
/// ns a 1,230-byte batch against 18.1 on the bare-metal bench; hyper-transport's `docs/transport.md`
/// §4n). The same function: the same value for the same bytes.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let sum = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, bytes);
    u32::try_from(sum & u64::from(u32::MAX)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The batch sum is CRC-32C (`CRC-32/ISCSI`): the catalogue's check value for "123456789", and
    /// zero for nothing.
    #[test]
    fn the_batch_sum_is_crc32c() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    fn producing() -> Producing {
        Producing {
            stream: 7,
            producer: 9,
            sequence: 41,
            flags: 3,
            timestamp: 1_792_022_400_000_000,
        }
    }

    #[test]
    fn a_built_batch_parses_to_what_was_put_in_it() {
        let mut builder = BatchBuilder::new();
        builder.begin(&producing());
        builder.push(1_792_022_400_000_000, b"first").unwrap();
        builder.push(1_792_022_400_000_250, b"").unwrap();
        builder.push(1_792_022_400_001_000, &[0xAB; 300]).unwrap();
        let bytes = builder.finish().unwrap().to_vec();
        let batch = Batch::parse(&bytes, 1 << 20).unwrap();
        assert_eq!(
            (batch.stream(), batch.producer(), batch.sequence()),
            (7, 9, 41)
        );
        assert_eq!(
            (batch.count(), batch.flags(), batch.base_offset()),
            (3, 3, 0)
        );
        let records: Vec<_> = batch.records().map(Result::unwrap).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"first");
        assert_eq!(records[1].timestamp, 1_792_022_400_000_250);
        assert_eq!(records[2].payload, &[0xAB; 300][..]);
    }

    #[test]
    fn stamping_the_base_offset_and_next_leaves_the_crc_valid() {
        let mut builder = BatchBuilder::new();
        builder.begin(&producing());
        builder.push(1_792_022_400_000_000, b"claim").unwrap();
        let mut bytes = builder.finish().unwrap().to_vec();
        stamp(&mut bytes, 12_345).unwrap();
        assert_eq!(Batch::parse(&bytes, 1 << 20).unwrap().next(), None);
        stamp_next(&mut bytes, 77).unwrap();
        let batch = Batch::parse(&bytes, 1 << 20).unwrap();
        assert_eq!((batch.base_offset(), batch.next()), (12_345, Some(77)));
        assert_eq!(batch.end_offset(), 12_346);
        assert_eq!(stated_length(&bytes), Some(bytes.len()));
    }

    #[test]
    fn every_flipped_bit_the_crc_covers_is_refused() {
        let mut builder = BatchBuilder::new();
        builder.begin(&producing());
        builder.push(1_792_022_400_000_000, b"testament").unwrap();
        let bytes = builder.finish().unwrap().to_vec();
        for at in COVERED_FROM..bytes.len() {
            for bit in 0..8 {
                let mut damaged = bytes.clone();
                damaged[at] ^= 1 << bit;
                assert!(
                    matches!(Batch::parse(&damaged, 1 << 20), Err(BatchError::Crc { .. })),
                    "byte {at} bit {bit} passed"
                );
            }
        }
    }

    /// What the transport checks of a publish or a delivery ([`EMBEDDED`]) is the batch's own CRC:
    /// a batch as built, and as stamped by its owner, passes it, and every flipped bit the CRC
    /// covers fails it, so the transport's check stands for [`Batch::parse`]'s and
    /// [`Batch::parse_verified`] skips no check that was not made.
    #[test]
    fn the_transport_s_check_is_the_batch_s_own_crc() {
        let mut builder = BatchBuilder::new();
        builder.begin(&producing());
        builder.push(1_792_022_400_000_000, b"claim").unwrap();
        let mut bytes = builder.finish().unwrap().to_vec();
        assert_eq!(EMBEDDED.verify(&bytes), Ok(()));
        stamp(&mut bytes, 12_345).unwrap();
        stamp_next(&mut bytes, 77).unwrap();
        assert_eq!(EMBEDDED.verify(&bytes), Ok(()));
        for at in COVERED_FROM..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[at] ^= 1;
            assert!(EMBEDDED.verify(&damaged).is_err(), "byte {at} passed");
        }
        let verified = Batch::parse_verified(&bytes, 1 << 20).unwrap();
        assert_eq!(
            verified.bytes(),
            Batch::parse(&bytes, 1 << 20).unwrap().bytes()
        );
    }

    #[test]
    fn a_wrong_length_a_false_count_or_trailing_bytes_are_refused() {
        let mut builder = BatchBuilder::new();
        builder.begin(&producing());
        builder.push(1_792_022_400_000_000, b"x").unwrap();
        let bytes = builder.finish().unwrap().to_vec();
        assert!(matches!(
            Batch::parse(&bytes[..bytes.len() - 1], 1 << 20),
            Err(BatchError::Length { .. })
        ));
        assert_eq!(
            Batch::parse(&bytes, MIN_BATCH).err(),
            Some(BatchError::TooLarge {
                bytes: bytes.len(),
                limit: MIN_BATCH
            })
        );
        // A count of two over one record, its CRC made to match: the count is what is refused.
        let mut lying = bytes.clone();
        lying[COUNT].copy_from_slice(&2_u16.to_le_bytes());
        let crc = crc32c(&lying[COVERED_FROM..]);
        lying[CRC].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(Batch::parse(&lying, 1 << 20).err(), Some(BatchError::Count));
        // Bytes after the last record, the length and CRC made to match: a stray byte is no
        // record, and a well-formed record the count does not state is one too many.
        let restamped = |extra: &[u8]| {
            let mut trailing = bytes.clone();
            trailing.extend_from_slice(extra);
            let length = u32::try_from(trailing.len()).unwrap();
            trailing[LENGTH].copy_from_slice(&length.to_le_bytes());
            let crc = crc32c(&trailing[COVERED_FROM..]);
            trailing[CRC].copy_from_slice(&crc.to_le_bytes());
            trailing
        };
        assert_eq!(
            Batch::parse(&restamped(&[0]), 1 << 20).err(),
            Some(BatchError::Record)
        );
        assert_eq!(
            Batch::parse(&restamped(&[0, 0]), 1 << 20).err(),
            Some(BatchError::Count)
        );
    }

    #[test]
    fn varints_round_trip_at_their_edges_and_an_overlong_one_is_refused() {
        for value in [
            0,
            1,
            127,
            128,
            16_383,
            16_384,
            u64::from(u32::MAX),
            u64::MAX,
        ] {
            let mut out = Vec::new();
            push_varint(&mut out, value);
            assert_eq!(varint(&out), Some((value, out.len())));
        }
        assert_eq!(varint(&[0xFF; 11]), None);
        assert_eq!(
            varint(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02]),
            None
        );
    }
}
