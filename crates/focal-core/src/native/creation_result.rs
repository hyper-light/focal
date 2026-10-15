//! Immutable resolution of one authored creation batch. These structural checks
//! do not establish content identity: the owner must bind every entry to the
//! checked input and actual effective source before publishing it with an outcome.
use super::{ContractError, NativeError};
use focal_memory::MemoryError;
use focal_model::{ContentHash, ObjectId};

const ALLOCATION: usize = 4 * size_of::<usize>();

/// Object addresses are family-scoped; unrelated families may reuse ID bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeCreatedFamily {
    Claim,
    Validation,
}

/// One original input position and its permanently resolved opaque address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeCreatedObject {
    pub ordinal: u32,
    pub family: NativeCreatedFamily,
    pub schema: u16,
    pub content: ContentHash,
    pub requested: ObjectId,
    pub resolved: ObjectId,
}

#[derive(Debug)]
pub struct NativeCreationResult {
    entries: Vec<NativeCreatedObject>,
}

fn heap(capacity: usize) -> Result<usize, MemoryError> {
    capacity
        .checked_mul(size_of::<NativeCreatedObject>())
        .and_then(|bytes| bytes.checked_add(if capacity == 0 { 0 } else { ALLOCATION }))
        .ok_or(MemoryError::CounterExhausted("creation result heap"))
}

fn fits(actual: usize, maximum: usize) -> Result<(), MemoryError> {
    if actual > maximum {
        Err(MemoryError::Capacity {
            requested: actual,
            available: maximum,
        })
    } else {
        Ok(())
    }
}

fn visit(remaining: &mut usize) -> Result<(), NativeError> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or(NativeError::Capacity("creation result visits"))?;
    Ok(())
}

impl NativeCreationResult {
    /// Shared scalar restoration/owned-construction checks; no allocation or
    /// authority is conveyed by validating a recorded resolution entry.
    pub(super) fn check_recorded_entry(
        entry: NativeCreatedObject,
        index: usize,
    ) -> Result<(), NativeError> {
        let ordinal =
            u32::try_from(index).map_err(|_| NativeError::Capacity("creation result ordinal"))?;
        if entry.ordinal != ordinal {
            return Err(ContractError::InvalidManifest.into());
        }
        if entry.requested.is_zero() || entry.resolved.is_zero() || entry.content.0 == [0; 32] {
            return Err(ContractError::InvalidTarget.into());
        }
        // Claims exist in descriptor schemas 1 and 2 (doc 21 §5); validation
        // definitions only in schema 1.
        let supported = match entry.family {
            NativeCreatedFamily::Claim => 1..=2,
            NativeCreatedFamily::Validation => 1..=1,
        };
        if !supported.contains(&entry.schema) {
            return Err(ContractError::InvalidPolicy.into());
        }
        Ok(())
    }
    pub(super) fn recorded_entries_conflict(
        previous: NativeCreatedObject,
        entry: NativeCreatedObject,
    ) -> bool {
        previous.family == entry.family
            && (previous.requested == entry.requested || previous.resolved == entry.resolved)
    }
    /// Exact scalar validation allowance for `count` distinct entries. Divide
    /// the even factor before multiplication so an intermediate product cannot
    /// overflow when the resulting triangular count still fits.
    pub fn inspection_visits(count: usize) -> Result<usize, MemoryError> {
        let error = || MemoryError::CounterExhausted("creation result visits");
        let next = count.checked_add(1).ok_or_else(error)?;
        let (left, right) = if count.is_multiple_of(2) {
            (count.checked_div(2).ok_or_else(error)?, next)
        } else {
            (count, next.checked_div(2).ok_or_else(error)?)
        };
        left.checked_mul(right).ok_or_else(error)
    }

    /// Dynamic storage, including allocator bookkeeping, for a buffer of this
    /// capacity. Its inline Vec header is charged by the containing row/input.
    pub fn construction_heap(capacity: usize) -> Result<usize, MemoryError> {
        heap(capacity)
    }

    /// Consume an already funded buffer without copying or allocating. Both its
    /// length and actual capacity must fit the supplied bounds. Inspection uses
    /// one visit per entry and one per earlier-entry comparison, never a hidden
    /// sort or auxiliary index. Refusal drops only the detached candidate.
    pub fn from_owned(
        entries: Vec<NativeCreatedObject>,
        max_objects: usize,
        max_visits: usize,
        max_bytes: usize,
    ) -> Result<Self, NativeError> {
        if entries.is_empty() || entries.len() > max_objects {
            return Err(NativeError::Capacity("creation result objects"));
        }
        fits(heap(entries.capacity())?, max_bytes)?;
        let mut remaining = Self::inspection_visits(entries.len())?;
        if remaining > max_visits {
            return Err(NativeError::Capacity("creation result visits"));
        }
        for (index, entry) in entries.iter().enumerate() {
            visit(&mut remaining)?;
            Self::check_recorded_entry(*entry, index)?;
            for previous in entries.iter().take(index) {
                visit(&mut remaining)?;
                if Self::recorded_entries_conflict(*previous, *entry) {
                    return Err(ContractError::ContentConflict.into());
                }
            }
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[NativeCreatedObject] {
        &self.entries
    }

    /// Actual retained dynamic charge, including unused owned vector capacity.
    pub fn heap_charge(&self) -> Result<usize, MemoryError> {
        heap(self.entries.capacity())
    }

    /// Retained element bytes, excluding allocator bookkeeping and the inline
    /// header. Use heap_charge when reserving the native row's complete heap.
    pub fn retained_heap_bytes(&self) -> Result<usize, MemoryError> {
        self.entries
            .capacity()
            .checked_mul(size_of::<NativeCreatedObject>())
            .ok_or(MemoryError::CounterExhausted("creation result heap"))
    }

    /// Copy an already checked immutable result into exactly quoted storage.
    /// The caller holds this allowance before allocation; the original result
    /// remains unchanged on every refusal. No identity lookup is repeated.
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, MemoryError> {
        let quote = heap(self.entries.len())?;
        fits(quote, max_bytes)?;
        #[cfg(test)]
        let requested = tests::allocation_capacity(self.entries.len())?;
        #[cfg(not(test))]
        let requested = self.entries.len();
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(requested)
            .map_err(|_| MemoryError::AllocationFailed)?;
        fits(heap(entries.capacity())?, quote)?;
        if entries.capacity() < self.entries.len() {
            return Err(MemoryError::AllocationFailed);
        }
        // Capacity was reconciled against the exact positive element quote;
        // no push can grow this buffer.
        entries.extend_from_slice(&self.entries);
        Ok(Self { entries })
    }
}

/// A creation result as its row holds it: its bounded vector inline (no second mapping buffer nor
/// a singleton allocation), or, spilled to the replica's payload log (doc 31 M3), where its record
/// encoding is, nothing on the heap. A spilled one is read on exact retries and by readers alone,
/// decoded and charged while it is held ([`OwnedCreationResult::read`]).
#[derive(Debug)]
pub(super) enum OwnedCreationResult {
    Held(NativeCreationResult),
    Logged(super::payloads::PayloadRef),
}

/// A creation result as a read takes it: a held row's, borrowed, or a spilled row's, decoded and
/// charged while it is held.
#[derive(Debug)]
pub enum NativeCreationRead<'a> {
    Borrowed(&'a NativeCreationResult),
    Decoded {
        result: NativeCreationResult,
        _charge: focal_memory::Allocation,
    },
}
impl std::ops::Deref for NativeCreationRead<'_> {
    type Target = NativeCreationResult;
    fn deref(&self) -> &NativeCreationResult {
        match self {
            Self::Borrowed(result) => result,
            Self::Decoded { result, .. } => result,
        }
    }
}

impl OwnedCreationResult {
    pub(super) const fn container_charge() -> usize {
        0
    }

    pub(super) fn new(result: NativeCreationResult) -> Result<Self, MemoryError> {
        result.heap_charge()?;
        Ok(Self::Held(result))
    }

    /// A held row's result: none for a spilled one. A candidate's rows are held until every
    /// check has read them.
    pub(super) fn held(&self) -> Option<&NativeCreationResult> {
        match self {
            Self::Held(result) => Some(result),
            Self::Logged(_) => None,
        }
    }

    /// The result, a spilled one read back through `payloads`, decoded and charged to `budget` in
    /// `lane` while it is held.
    pub(super) fn read(
        &self,
        payloads: Option<&dyn super::payloads::PayloadRead>,
        budget: &focal_memory::MemoryBudget,
        lane: focal_memory::BudgetLane,
    ) -> Result<NativeCreationRead<'_>, NativeError> {
        match self {
            Self::Held(result) => Ok(NativeCreationRead::Borrowed(result)),
            Self::Logged(at) => {
                let mut buffer = [0u8; super::payloads::SPILLED_BYTES];
                let bytes = buffer
                    .get_mut(..at.len())
                    .ok_or(super::payloads::PayloadError::Corrupt)?;
                payloads
                    .ok_or(super::payloads::PayloadError::Unavailable)?
                    .read(*at, bytes)?;
                let charge = budget
                    .reserve(
                        focal_memory::BudgetKind::Query,
                        lane,
                        super::record_codec::creation_decode_bound(bytes.len())?,
                    )?
                    .commit();
                let result = super::record_codec::decode_creation(bytes)?;
                fits(result.heap_charge()?, charge.bytes())?;
                Ok(NativeCreationRead::Decoded {
                    result,
                    _charge: charge,
                })
            }
        }
    }

    /// This row spilled to `log` as its record encoding, where it is held, the encoding fits a
    /// spilled row and the log takes it: what a candidate's rows become once every check has read
    /// them.
    pub(super) fn spill(
        &self,
        log: &dyn super::payloads::PayloadLog,
    ) -> Result<Option<Self>, NativeError> {
        if !matches!(self, Self::Held(_)) {
            return Ok(None);
        }
        let mut buffer = [0u8; super::payloads::SPILLED_BYTES];
        let Some(len) = super::record_codec::encode_creation(self, &mut buffer) else {
            return Ok(None);
        };
        let bytes = buffer
            .get(..len)
            .ok_or(NativeError::Capacity("creation result row"))?;
        Ok(log.append(bytes).map(Self::Logged))
    }

    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        match self {
            Self::Held(result) => result.heap_charge(),
            Self::Logged(_) => Ok(0),
        }
    }

    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        match self {
            Self::Held(result) => {
                let allowance = self.heap_charge()?;
                let copied = Self::new(result.try_copy(allowance)?)?;
                fits(copied.heap_charge()?, allowance)?;
                Ok(copied)
            }
            Self::Logged(at) => Ok(Self::Logged(*at)),
        }
    }
}

#[cfg(test)]
#[path = "creation_result_tests.rs"]
mod tests;
