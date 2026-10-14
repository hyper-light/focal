//! Recorded native mutations, distinct from requests and frozen V1 formats.
//!
//! Encoding reads only the actual captured write set and its immutable candidate.
//! It never reruns authorization, participant work or lifecycle reduction. These
//! dormant bytes are not yet a registered WAL decoder or an activation promise.
//! Structural inspection does not prove model validity, complete history, local
//! evidence custody or permission to publish a recovered root.
pub mod archive;
mod buffer;
pub mod checkpoint;
pub mod seal;
pub use buffer::FundedRecord;
#[cfg(test)]
pub(in crate::native) use buffer::future_quote as future_record_quote;
#[cfg(test)]
pub(in crate::native) use buffer::{HEAP_EXPANSION, header_fixed_bytes, row_fixed_bytes};
pub(in crate::native) use buffer::{PendingRecord, future as future_record_bytes};
mod events;
mod evidence;
mod fixed;
mod inspect;
mod lifecycle;
mod lifecycle_fields;
pub mod materialize;
mod read_audit;
mod read_claim;
mod read_dispatch;
mod read_evaluation;
mod read_events;
mod read_evidence;
mod retire;
pub use retire::{RETIRE_MAGIC, RetireRecord};
mod read_fields;
mod read_history;
mod read_index;
mod read_rows;
mod read_scopes;
mod read_source;
mod read_validate;
mod read_validate_aggregate;
mod read_validate_attempts;
mod read_validate_audit;
mod read_validate_evidence;
mod read_validate_index;
pub mod recovery;
pub mod replay;
mod replay_index;
mod replay_projection;
mod replay_validate;
mod replay_validate_index;
mod rows;
mod spilled;

#[cfg(test)]
mod bound_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(in crate::native) fn check_recorded_operation(
    operation: NativeOperation,
    fact: NativeFact,
) -> Result<(), NativeError> {
    replay_validate::check_operation_test(operation, fact)
}

use super::input_codec::{bytes, descriptors, types};
use super::*;
pub use bytes::Error as CodecError;
use bytes::{
    CountingSink, Sink, SliceSink, write_count, write_raw, write_u8, write_u16, write_u32,
    write_u64,
};
pub(in crate::native) use checkpoint::{ArchiveFrame, archive_frame, seal_frame};
pub(in crate::native) use events::{Bindings as EventBindings, event_in as encode_event};
pub(in crate::native) use evidence::content_profile;
pub use fixed::RowFamily;
pub(in crate::native) use read_events::event_in as decode_event;
pub(in crate::native) use read_evidence::content_profile as read_content_profile;
pub(in crate::native) use read_evidence::{
    creation_decode_bound, decode_creation, decode_definition,
};
pub(in crate::native) use read_history::event_object;
pub(in crate::native) use spilled::{ClaimRows, decode_claim};
/// Codec work an outcome's record encoding takes at most: a fixed-width row's.
const OUTCOME_VISITS: usize = 4096;
/// `outcome`'s record encoding written into the front of `into`: its length (a spilled outcome's
/// bytes, doc 31 M3).
pub(in crate::native) fn encode_outcome(
    outcome: NativeOutcome,
    into: &mut [u8],
) -> Result<usize, CodecError> {
    let mut count = CountingSink::new(into.len(), OUTCOME_VISITS);
    fixed::outcome(&mut count, outcome)?;
    let len = count.len();
    let bytes = into.get_mut(..len).ok_or(CodecError::Capacity)?;
    let mut sink = SliceSink::new(bytes, OUTCOME_VISITS);
    fixed::outcome(&mut sink, outcome)?;
    sink.finish()?;
    Ok(len)
}
/// The outcome whose record encoding `bytes` are, exactly.
pub(in crate::native) fn decode_outcome(bytes: &[u8]) -> Result<NativeOutcome, CodecError> {
    let mut cursor = bytes::Cursor::new(bytes, bytes.len(), OUTCOME_VISITS)?;
    let outcome = fixed::read_outcome(&mut cursor)?;
    cursor.finish()?;
    Ok(outcome)
}
/// A held creation result's record encoding written into the front of `into`: its length, or none
/// where it does not fit (a spilled row's bytes, doc 31 M3).
pub(in crate::native) fn encode_creation(
    row: &crate::native::creation_result::OwnedCreationResult,
    into: &mut [u8],
) -> Option<usize> {
    let visits = into.len().checked_mul(4)?.checked_add(4096)?;
    let mut count = CountingSink::new(into.len(), visits);
    evidence::creation(&mut count, row, None).ok()?;
    let len = count.len();
    let mut sink = SliceSink::new(into.get_mut(..len)?, visits);
    evidence::creation(&mut sink, row, None).ok()?;
    sink.finish().ok()?;
    Some(len)
}
/// A held claim row's record encoding written into the front of `into`: its length, or none where
/// it does not fit (a spilled row's bytes, doc 31 M3).
pub(in crate::native) fn encode_claim(
    row: &crate::native::OwnedClaim,
    into: &mut [u8],
) -> Option<usize> {
    let visits = into.len().checked_mul(4)?.checked_add(4096)?;
    let mut count = CountingSink::new(into.len(), visits);
    lifecycle::claim(&mut count, row).ok()?;
    let len = count.len();
    let mut sink = SliceSink::new(into.get_mut(..len)?, visits);
    lifecycle::claim(&mut sink, row).ok()?;
    sink.finish().ok()?;
    Some(len)
}
/// A held authored definition's record encoding written into the front of `into`: its length, or
/// none where it does not fit (a spilled row's bytes, doc 31 M3).
pub(in crate::native) fn encode_definition(
    row: &crate::native::owned::OwnedDeclaration,
    into: &mut [u8],
) -> Option<usize> {
    let visits = into.len().checked_mul(4)?.checked_add(4096)?;
    let mut count = CountingSink::new(into.len(), visits);
    evidence::definition(&mut count, row, None).ok()?;
    let len = count.len();
    let mut sink = SliceSink::new(into.get_mut(..len)?, visits);
    evidence::definition(&mut sink, row, None).ok()?;
    sink.finish().ok()?;
    Some(len)
}
/// A measuring sink for a two-pass encoding.
pub(in crate::native) fn counting_sink(max_bytes: usize, max_visits: usize) -> CountingSink {
    CountingSink::new(max_bytes, max_visits)
}
/// A writing sink over exactly the quoted bytes.
pub(in crate::native) fn slice_sink(bytes: &mut [u8], max_visits: usize) -> SliceSink<'_> {
    SliceSink::new(bytes, max_visits)
}
/// The most bytes one key takes as a record writes it, with room to spare: the widest key, a
/// result's, is a tag, three identities, a target and two counters.
const KEY_BYTES: usize = 256;
/// The most visits writing one key takes: a write a field, each its bytes and one more.
const KEY_VISITS: usize = 4 * KEY_BYTES;
/// Feed `key`, as a record writes it, to `hasher`.
pub(in crate::native) fn hash_key(hasher: &mut blake3::Hasher, key: Key) -> Result<(), CodecError> {
    let mut buffer = [0u8; KEY_BYTES];
    let mut sink = slice_sink(&mut buffer, KEY_VISITS);
    fixed::key(&mut sink, key)?;
    hasher.update(sink.written());
    Ok(())
}
pub use archive::{ArchiveHeader, StructuralArchive};
pub use inspect::{
    EncodedRow, InspectionLimits, InspectionQuote, RecordHeader, RecordRows, StructuralRecord,
};
pub use seal::{SealHeader, StructuralSeal};

pub const MAGIC: [u8; 8] = *b"FCMUTATE";
// Version 2 records the actual graph snapshot boundary on consequence events.
// These dormant native bytes are separate from the frozen live V1 formats.
// Version 5 orders rows by the storage layout (25 §3): affinity, family, fields.
pub const VERSION: u16 = 6;
const HASH_DOMAIN: &str = "focal.native.record.v2";

#[derive(Debug, Clone, Copy)]
pub struct EncodingLimits {
    pub bytes: usize,
    pub visits: usize,
    pub rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingQuote {
    pub bytes: usize,
    /// Complete measurement/write traversal allowance, including row sizing,
    /// bounded index lookups and hashing. Each pass consumes this allowance.
    pub visits: usize,
    pub rows: usize,
    pub hash: ContentHash,
}

/// Borrowing prevents a candidate from changing or being consumed while its
/// exact record is measured or written. Output storage remains caller-funded;
/// this plan allocates no buffer and creates no reference-counted owner.
pub struct EncodingPlan<'a> {
    prepared: &'a NativePrepared,
    quote: EncodingQuote,
    /// Where the candidate's spilled rows' bytes are read from (doc 31 M3).
    payloads: Option<&'a dyn crate::native::payloads::PayloadRead>,
}
impl<'a> EncodingPlan<'a> {
    /// Measures the record: its bytes and the visits its write takes. Its
    /// hash is its write's ([`Self::write_into`]); the quote's is zero until
    /// then.
    pub fn prepare(
        prepared: &'a NativePrepared,
        limits: EncodingLimits,
    ) -> Result<Self, CodecError> {
        Self::prepare_with(prepared, limits, None)
    }
    /// As [`Self::prepare`], for a candidate whose rows were spilled to the payload log
    /// `payloads` reads (doc 31 M3).
    pub fn prepare_with(
        prepared: &'a NativePrepared,
        limits: EncodingLimits,
        payloads: Option<&'a dyn crate::native::payloads::PayloadRead>,
    ) -> Result<Self, CodecError> {
        // A retirement's record carries its families, never its rows (doc 31 M2).
        if let Some(body) = prepared.retire.as_ref() {
            let retired = &body.retired;
            if retired.len() > limits.rows {
                return Err(CodecError::Capacity);
            }
            let mut sink = CountingSink::new(limits.bytes, limits.visits);
            retire::frame(&mut sink, prepared, body)?;
            let hashed = sink.len();
            digest_visits(&mut sink, hashed)?;
            sink.write(&[0; 32])?;
            return Ok(Self {
                prepared,
                quote: EncodingQuote {
                    bytes: sink.len(),
                    visits: sink.visits_used(),
                    rows: retired.len(),
                    hash: ContentHash([0; 32]),
                },
                payloads,
            });
        }
        if prepared.mutation_count() == 0 || prepared.mutation_count() > limits.rows {
            return Err(CodecError::Capacity);
        }
        let mut sink = CountingSink::new(limits.bytes, limits.visits);
        frame(&mut sink, prepared, payloads, |sink, key, value, ledger| {
            // Each row's length precedes it: measured once here, and its
            // write's visits charged as it is measured.
            let mut measure = RowSize { sink, bytes: 0 };
            rows::value(&mut measure, key, value, ledger, payloads)?;
            let size = measure.bytes;
            write_count(sink, size)?;
            sink.count(size)
        })?;
        let hashed = sink.len();
        digest_visits(&mut sink, hashed)?;
        sink.write(&[0; 32])?;
        Ok(Self {
            prepared,
            quote: EncodingQuote {
                bytes: sink.len(),
                visits: sink.visits_used(),
                rows: prepared.mutation_count(),
                hash: ContentHash([0; 32]),
            },
            payloads,
        })
    }
    pub fn quote(&self) -> EncodingQuote {
        self.quote
    }
    /// A wrong-sized destination is refused before any byte is changed. The
    /// destination must already be charged through its eventual WAL ownership.
    /// Each row is written once, behind a length patched in once it is known,
    /// and the digest taken of the whole record at once: the stream it had
    /// been, a field at a time, with each row encoded twice (once to measure
    /// its length), cost twice the encoding and a hash of every field.
    pub fn write_into(&self, output: &mut [u8]) -> Result<ContentHash, CodecError> {
        if output.len() != self.quote.bytes {
            return Err(CodecError::Capacity);
        }
        let mut sink = SliceSink::new(output, self.quote.visits);
        if let Some(body) = self.prepared.retire.as_ref() {
            retire::frame(&mut sink, self.prepared, body)?;
            let hashed = sink.len();
            digest_visits(&mut sink, hashed)?;
            let mut hash = retire::hasher();
            hash.update(sink.written());
            let hash = ContentHash(*hash.finalize().as_bytes());
            sink.write(&hash.0)?;
            if sink.len() != self.quote.bytes || sink.visits_used() != self.quote.visits {
                return Err(CodecError::InvalidTag("retirement record source"));
            }
            sink.finish()?;
            return Ok(hash);
        }
        frame(
            &mut sink,
            self.prepared,
            self.payloads,
            |sink, key, value, ledger| {
                let at = sink.len();
                write_count(sink, 0)?;
                rows::value(sink, key, value, ledger, self.payloads)?;
                let size = sink
                    .len()
                    .checked_sub(at)
                    .and_then(|written| written.checked_sub(4))
                    .ok_or(CodecError::Capacity)?;
                sink.patch_u32(at, u32::try_from(size).map_err(|_| CodecError::Capacity)?)
            },
        )?;
        let hashed = sink.len();
        digest_visits(&mut sink, hashed)?;
        let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
        hash.update(sink.written());
        let hash = ContentHash(*hash.finalize().as_bytes());
        sink.write(&hash.0)?;
        if sink.len() != self.quote.bytes || sink.visits_used() != self.quote.visits {
            return Err(CodecError::InvalidTag("record source"));
        }
        sink.finish()?;
        Ok(hash)
    }
}

/// The digest's work: its construction and finalization, and the `hashed`
/// bytes before it.
fn digest_visits(sink: &mut impl Sink, hashed: usize) -> Result<(), CodecError> {
    sink.visit(256)?;
    sink.visit(add(1, hashed)?)
}

fn add(a: usize, b: usize) -> Result<usize, CodecError> {
    a.checked_add(b).ok_or(CodecError::Capacity)
}

/// Bytes gathered before the digest takes them: four BLAKE3 chunks, so it hashes
/// them in parallel (its SIMD paths, NEON or AVX, take whole chunks side by side;
/// a field at a time it compresses one 64-byte block after another, serially).
const HASH_BATCH: usize = 4096;

/// Every byte entering the digest is charged before hashing. The trailer is the
/// hash of all preceding bytes under a separate native-record domain. The bytes
/// reach the hasher in batches: the digest of a stream is the same however it is
/// divided, so batching changes its cost and nothing else.
struct HashSink<'a, S> {
    sink: &'a mut S,
    hash: blake3::Hasher,
    batch: [u8; HASH_BATCH],
    held: usize,
}
impl<'a, S> HashSink<'a, S> {
    fn new(sink: &'a mut S, hash: blake3::Hasher) -> Self {
        Self {
            sink,
            hash,
            batch: [0; HASH_BATCH],
            held: 0,
        }
    }
    fn flush(&mut self) {
        if let Some(held) = self.batch.get(..self.held) {
            self.hash.update(held);
        }
        self.held = 0;
    }
    /// The digest of every byte written.
    fn digest(&mut self) -> ContentHash {
        self.flush();
        ContentHash(*self.hash.finalize().as_bytes())
    }
}
impl<S: Sink> Sink for HashSink<'_, S> {
    fn write(&mut self, mut bytes: &[u8]) -> Result<(), CodecError> {
        self.sink.visit(add(1, bytes.len())?)?;
        self.sink.write(bytes)?;
        // The digest is handed whole batches only until its end, so every
        // update begins on a batch boundary: BLAKE3 hashes an update that
        // begins on a power-of-two boundary as whole subtrees, side by side,
        // while one that begins inside a chunk is compressed a block at a
        // time until the boundary, and a part flushed early left every later
        // batch inside one (blake3 `Hasher::update`; a profile of a root's
        // encoding spent half its time in the portable single-block path).
        while !bytes.is_empty() {
            if self.held == 0 && bytes.len() >= HASH_BATCH {
                let whole = bytes
                    .len()
                    .checked_sub(bytes.len().checked_rem(HASH_BATCH).unwrap_or(0))
                    .ok_or(CodecError::Capacity)?;
                let (batches, rest) = bytes.split_at_checked(whole).ok_or(CodecError::Capacity)?;
                self.hash.update(batches);
                bytes = rest;
                continue;
            }
            let take = HASH_BATCH.saturating_sub(self.held).min(bytes.len());
            let (head, rest) = bytes.split_at_checked(take).ok_or(CodecError::Capacity)?;
            let end = add(self.held, take)?;
            // A field of a few bytes is moved without a call to the C
            // library's memcpy (`focal_memory::short_copy`).
            if !focal_memory::short_copy::copy(
                self.batch
                    .get_mut(self.held..end)
                    .ok_or(CodecError::Capacity)?,
                head,
            ) {
                return Err(CodecError::Capacity);
            }
            self.held = end;
            bytes = rest;
            if self.held == HASH_BATCH {
                self.flush();
            }
        }
        Ok(())
    }
    fn visit(&mut self, amount: usize) -> Result<(), CodecError> {
        self.sink.visit(amount)
    }
}

/// Measure a variable row without staging bytes or per-row offset arrays. Work
/// flows into the enclosing meter before each operation; it cannot reset at a
/// row boundary. The complete row is encoded again after its length prefix.
struct RowSize<'a, S> {
    sink: &'a mut S,
    bytes: usize,
}
impl<S: Sink> Sink for RowSize<'_, S> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.count_unwritten(bytes.len())
    }
    fn visit(&mut self, amount: usize) -> Result<(), CodecError> {
        self.sink.visit(amount)
    }
    fn measures(&self) -> bool {
        true
    }
    fn count_unwritten(&mut self, len: usize) -> Result<(), CodecError> {
        let total = add(self.bytes, len)?;
        u32::try_from(total).map_err(|_| CodecError::Capacity)?;
        self.sink.visit(add(1, len)?)?;
        self.bytes = total;
        Ok(())
    }
}

/// The record's bytes before its digest, each changed row's through `row`
/// (which writes its length and then its body, or counts them).
/// Each row a record carries: its key's family, and the bytes its key and its value take there —
/// what a record's bytes are made of, for a measurement.
#[cfg(test)]
pub(in crate::native) fn row_bytes(
    prepared: &NativePrepared,
) -> Result<Vec<(u16, usize, usize)>, CodecError> {
    let ledger = prepared.outcome.ledger;
    let mut out = Vec::new();
    for (key, deleted) in prepared.writes.entries() {
        let mut keys = CountingSink::new(usize::MAX, usize::MAX);
        fixed::key(&mut keys, key)?;
        let value = match prepared.fragments.get(&key) {
            Some(row) if !deleted => {
                let mut values = CountingSink::new(usize::MAX, usize::MAX);
                rows::value(&mut values, key, row, ledger, None)?;
                values.len()
            }
            _ => 0,
        };
        out.push((super::layout::family(&key), keys.len(), value));
    }
    Ok(out)
}

fn frame<S: Sink>(
    sink: &mut S,
    prepared: &NativePrepared,
    payloads: Option<&dyn crate::native::payloads::PayloadRead>,
    mut row: impl FnMut(&mut S, Key, &Row, LedgerId) -> Result<(), CodecError>,
) -> Result<(), CodecError> {
    let outcome = prepared.outcome;
    if outcome.ledger.tenant.is_zero()
        || outcome.ledger.session.is_zero()
        || outcome.sequence.0 != prepared.fragments.prefix()
        || prepared.fragments.base_prefix().checked_add(1) != Some(outcome.sequence.0)
    {
        return Err(CodecError::InvalidTag("record frame"));
    }
    write_raw(sink, &MAGIC)?;
    write_u16(sink, VERSION)?;
    write_u8(
        sink,
        match prepared.content_profile() {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    types::ledger(sink, outcome.ledger)?;
    write_raw(sink, &prepared.fragments.id().0.to_le_bytes())?;
    write_u64(sink, prepared.fragments.base_prefix())?;
    fixed::outcome(sink, outcome)?;
    write_count(sink, prepared.mutation_count())?;
    let mut previous = None;
    let mut meta = false;
    let mut recorded = false;
    for (key, deleted) in prepared.writes.entries() {
        // The fixed-width key comparison, bounded directory descent (at most
        // usize::BITS levels of <=64 links) and leaf search are all precharged.
        let lookup = usize::try_from(usize::BITS)
            .map_err(|_| CodecError::Capacity)?
            .checked_add(1)
            .and_then(|n| n.checked_mul(64))
            .ok_or(CodecError::Capacity)?;
        sink.visit(lookup)?;
        if previous.is_some_and(|last| last >= key) {
            return Err(CodecError::InvalidTag("record key order"));
        }
        previous = Some(key);
        write_u8(sink, u8::from(!deleted))?;
        fixed::key(sink, key)?;
        let value = prepared.fragments.get(&key);
        match (deleted, value) {
            (true, None) if key != Key::Meta && key != Key::Outcome(outcome.invocation) => {
                write_count(sink, 0)?;
            }
            (false, Some(value)) => {
                rows::family(key, value)?;
                if key == Key::Meta {
                    meta = true;
                }
                if key == Key::Outcome(outcome.invocation) {
                    if !matches!(value, Row::Outcome(row) if row.expand(outcome.ledger, outcome.invocation, payloads).is_ok_and(|recorded| recorded == outcome))
                    {
                        return Err(CodecError::InvalidTag("record outcome"));
                    }
                    recorded = true;
                }
                row(sink, key, value, outcome.ledger)?;
            }
            _ => return Err(CodecError::InvalidTag("record mutation")),
        }
    }
    if !meta || !recorded {
        return Err(CodecError::InvalidTag("record accounting rows"));
    }
    Ok(())
}
