//! Recorded native mutations, distinct from requests and frozen V1 formats.
//!
//! Encoding reads only the actual captured write set and its immutable candidate.
//! It never reruns authorization, participant work or lifecycle reduction. These
//! dormant bytes are not yet a registered WAL decoder or an activation promise.
//! Structural inspection does not prove model validity, complete history, local
//! evidence custody or permission to publish a recovered root.
pub mod checkpoint;
mod buffer;
pub use buffer::FundedRecord;
pub(in crate::native) use buffer::{PendingRecord, future as future_record_bytes};
mod events;
mod evidence;
mod fixed;
mod inspect;
mod lifecycle;
mod lifecycle_fields;
mod read_audit;
mod read_claim;
mod read_dispatch;
mod read_evaluation;
mod read_events;
mod read_evidence;
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
pub mod recovery;
pub mod replay;
mod replay_index;
mod replay_projection;
mod replay_validate;
mod rows;

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
    CountingSink, Sink, SliceSink, write_count, write_raw, write_u8, write_u16, write_u64,
};
pub use fixed::RowFamily;
pub use inspect::{
    EncodedRow, InspectionLimits, InspectionQuote, RecordHeader, RecordRows, StructuralRecord,
};

pub const MAGIC: [u8; 8] = *b"FCMUTATE";
// Version 2 records the actual graph snapshot boundary on consequence events.
// These dormant native bytes are separate from the frozen live V1 formats.
pub const VERSION: u16 = 2;
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
}
impl<'a> EncodingPlan<'a> {
    pub fn prepare(
        prepared: &'a NativePrepared,
        limits: EncodingLimits,
    ) -> Result<Self, CodecError> {
        if prepared.mutation_count() == 0 || prepared.mutation_count() > limits.rows {
            return Err(CodecError::Capacity);
        }
        let mut sink = CountingSink::new(limits.bytes, limits.visits);
        let hash = frame(&mut sink, prepared)?;
        Ok(Self {
            prepared,
            quote: EncodingQuote {
                bytes: sink.len(),
                visits: sink.visits_used(),
                rows: prepared.mutation_count(),
                hash,
            },
        })
    }
    pub fn quote(&self) -> EncodingQuote {
        self.quote
    }
    /// A wrong-sized destination is refused before any byte is changed. The
    /// destination must already be charged through its eventual WAL ownership.
    pub fn write_into(&self, output: &mut [u8]) -> Result<ContentHash, CodecError> {
        if output.len() != self.quote.bytes {
            return Err(CodecError::Capacity);
        }
        let mut sink = SliceSink::new(output, self.quote.visits);
        let hash = frame(&mut sink, self.prepared)?;
        if sink.len() != self.quote.bytes
            || sink.visits_used() != self.quote.visits
            || hash != self.quote.hash
        {
            return Err(CodecError::InvalidTag("record source"));
        }
        sink.finish()?;
        Ok(hash)
    }
}

fn add(a: usize, b: usize) -> Result<usize, CodecError> {
    a.checked_add(b).ok_or(CodecError::Capacity)
}

/// Every byte entering the digest is charged before hashing. The trailer is the
/// hash of all preceding bytes under a separate native-record domain.
struct HashSink<'a, S> {
    sink: &'a mut S,
    hash: blake3::Hasher,
}
impl<S: Sink> Sink for HashSink<'_, S> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        self.sink.visit(add(1, bytes.len())?)?;
        self.sink.write(bytes)?;
        self.hash.update(bytes);
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
        let total = add(self.bytes, bytes.len())?;
        u32::try_from(total).map_err(|_| CodecError::Capacity)?;
        self.sink.visit(add(1, bytes.len())?)?;
        self.bytes = total;
        Ok(())
    }
    fn visit(&mut self, amount: usize) -> Result<(), CodecError> {
        self.sink.visit(amount)
    }
}

fn frame(sink: &mut impl Sink, prepared: &NativePrepared) -> Result<ContentHash, CodecError> {
    let outcome = prepared.outcome;
    if outcome.ledger.tenant.is_zero()
        || outcome.ledger.session.is_zero()
        || outcome.sequence.0 != prepared.range.prefix()
        || prepared.range.base_prefix().checked_add(1) != Some(outcome.sequence.0)
    {
        return Err(CodecError::InvalidTag("record frame"));
    }
    // Hash construction/finalization have fixed bounded work in addition to the
    // separately metered byte stream. No user-controlled derive-key context.
    sink.visit(256)?;
    let mut hashed = HashSink {
        sink,
        hash: blake3::Hasher::new_derive_key(HASH_DOMAIN),
    };
    write_raw(&mut hashed, &MAGIC)?;
    write_u16(&mut hashed, VERSION)?;
    write_u8(
        &mut hashed,
        match prepared.content_profile() {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    types::ledger(&mut hashed, outcome.ledger)?;
    write_raw(&mut hashed, &prepared.range.id().0.to_le_bytes())?;
    write_u64(&mut hashed, prepared.range.base_prefix())?;
    fixed::outcome(&mut hashed, outcome)?;
    write_count(&mut hashed, prepared.mutation_count())?;
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
        hashed.visit(lookup)?;
        if previous.is_some_and(|last| last >= key) {
            return Err(CodecError::InvalidTag("record key order"));
        }
        previous = Some(key);
        write_u8(&mut hashed, u8::from(!deleted))?;
        fixed::key(&mut hashed, key)?;
        let value = prepared.range.get(&key);
        match (deleted, value) {
            (true, None) if key != Key::Meta && key != Key::Outcome(outcome.invocation) => {
                write_count(&mut hashed, 0)?;
            }
            (false, Some(value)) => {
                rows::family(key, value)?;
                if key == Key::Meta {
                    meta = true;
                }
                if key == Key::Outcome(outcome.invocation) {
                    if !matches!(value, Row::Outcome(row) if *row == outcome) {
                        return Err(CodecError::InvalidTag("record outcome"));
                    }
                    recorded = true;
                }
                let mut measure = RowSize {
                    sink: &mut hashed,
                    bytes: 0,
                };
                rows::value(&mut measure, value, outcome.ledger)?;
                let size = measure.bytes;
                write_count(&mut hashed, size)?;
                rows::value(&mut hashed, value, outcome.ledger)?;
            }
            _ => return Err(CodecError::InvalidTag("record mutation")),
        }
    }
    if !meta || !recorded {
        return Err(CodecError::InvalidTag("record accounting rows"));
    }
    let digest = ContentHash(*hashed.hash.finalize().as_bytes());
    write_raw(hashed.sink, &digest.0)?;
    Ok(digest)
}
