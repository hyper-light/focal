//! One immutable candidate's encoded bytes and their original accounting owner.
//! This funds a record buffer, not transport fanout, a WAL append or disk space.
use super::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, RangeWriteEnvelope};

/// A single owned record buffer. NativeOwner retains it until the corresponding
/// candidate is published or discarded; callers borrow bytes through that owner.
#[derive(Debug)]
pub struct FundedRecord {
    bytes: Vec<u8>,
    quote: EncodingQuote,
    // Buffer destruction must precede its accounting credit.
    allocation: Allocation,
}
impl FundedRecord {
    pub fn bytes(&self) -> &[u8] { &self.bytes }
    pub fn hash(&self) -> ContentHash { self.quote.hash }
    pub fn quote(&self) -> EncodingQuote { self.quote }
    pub fn charged_bytes(&self) -> usize { self.allocation.bytes() }
}

#[derive(Debug)]
pub(in crate::native) enum PendingRecord {
    Disabled,
    Reserved { quote: EncodingQuote, allocation: Allocation },
    Encoded(FundedRecord),
}

fn charge(bytes: usize) -> Result<usize, CodecError> {
    add(bytes, focal_memory::ALLOCATOR_OVERHEAD)
}
fn multiply(left: usize, right: usize) -> Result<usize, CodecError> {
    left.checked_mul(right).ok_or(CodecError::Capacity)
}
fn fits(quote: EncodingQuote, limits: EncodingLimits) -> Result<(), CodecError> {
    if quote.bytes > limits.bytes || quote.visits > limits.visits || quote.rows > limits.rows {
        Err(CodecError::Capacity)
    } else { Ok(()) }
}

/// Bound the recorded representation using the future writer's actual aggregate
/// shape. All variable native bodies own their model containers and buffers in
/// incoming_heap. The explicit codec expands host usize to u64, compact event
/// revisions to bindings, enum tags and collection lengths; four encoded bytes
/// per charged native byte cover those expansions on supported 32/64-bit hosts.
/// No body repeats a variable buffer or emits an implicit zero-filled capacity.
/// Fixed inline rows and keys are bounded separately by their native enum sizes.
/// The frame repeats one fixed outcome and its ledger/range/prefix/hash header.
///
/// These are format-version bounds, not input limits multiplied by maximum row
/// size. Changes to stored body expansion must update this bound and its tests.
pub(in crate::native) fn future(range: RangeWriteEnvelope, limits: EncodingLimits)
    -> Result<usize, NativeError>
{
    let shape = range.limits();
    let calculated = (|| {
        let row_fixed = add(multiply(add(size_of::<Key>(), size_of::<Row>())?, 4)?, 8)?;
        let header = add(multiply(size_of::<NativeOutcome>(), 4)?, 128)?;
        let bytes = add(header, add(multiply(shape.changed_keys, row_fixed)?, multiply(shape.incoming_heap, 4)?)?)?;
        // The row sizing pass, writing/hash pass, fixed field visits and scalar
        // graph/index access are all charged by the same encoder. A directory
        // lookup has the codec's explicit machine-width bound for every row.
        let lookup = const { (usize::BITS as usize + 1) * 64 };
        let visits = add(512, add(multiply(bytes, 16)?, multiply(shape.changed_keys, lookup)?)?)?;
        fits(EncodingQuote { bytes, visits, rows: shape.changed_keys, hash: ContentHash([0; 32]) }, limits)?;
        charge(bytes)
    })();
    calculated.map_err(|_| NativeError::Capacity("record buffer envelope"))
}

impl PendingRecord {
    pub(in crate::native) fn reserve(
        prepared: &NativePrepared, source: &MemoryBudget, lane: BudgetLane,
        limits: Option<EncodingLimits>,
    ) -> Result<Self, NativeOwnerError> {
        let Some(limits) = limits else { return Ok(Self::Disabled); };
        let quote = EncodingPlan::prepare(prepared, limits).map_err(NativeOwnerError::Record)?.quote();
        let allocation = source.reserve(BudgetKind::Pending, lane,
            charge(quote.bytes).map_err(NativeOwnerError::Record)?)?.commit();
        Ok(Self::Reserved { quote, allocation })
    }

    pub(in crate::native) fn encode(&mut self, prepared: &NativePrepared, limits: EncodingLimits)
        -> Result<&FundedRecord, NativeOwnerError>
    {
        let quote = match self {
            Self::Disabled => return Err(NativeError::Capacity("owner record buffers are disabled").into()),
            Self::Reserved { quote, .. } => *quote,
            Self::Encoded(record) => record.quote,
        };
        fits(quote, limits).map_err(NativeOwnerError::Record)?;
        if let Self::Reserved { allocation, .. } = self {
            if allocation.bytes() != charge(quote.bytes).map_err(NativeOwnerError::Record)? {
                return Err(NativeError::Capacity("record buffer charge changed").into());
            }
            // The preheld permit remains inside self through every allocation
            // or encoding refusal. A retry can use that identical candidate.
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(quote.bytes).map_err(|_| MemoryError::AllocationFailed)?;
            if bytes.capacity() != quote.bytes { return Err(MemoryError::AllocationFailed.into()); }
            bytes.resize(quote.bytes, 0);
            let plan = EncodingPlan { prepared, quote };
            plan.write_into(&mut bytes).map_err(NativeOwnerError::Record)?;
            let previous = std::mem::replace(self, Self::Disabled);
            let Self::Reserved { allocation, .. } = previous else {
                return Err(NativeError::Capacity("record buffer state changed").into());
            };
            *self = Self::Encoded(FundedRecord { bytes, quote, allocation });
        }
        match self {
            Self::Encoded(record) => Ok(record),
            _ => Err(NativeError::Capacity("record buffer state changed").into()),
        }
    }
}
