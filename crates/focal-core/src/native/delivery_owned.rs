//! Pure Receipt results carry the actual accepted fact and publication position.
//! There is no external attempt, reporter impersonation, or placeholder artifact.
use super::prepare::ALLOCATION;
use super::*;
use focal_model::{ValidationMode, VerdictValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeDeliveryResult {
    result: validation::AcceptedResult,
    sequence: SessionSeq,
    ordinal: u32,
}
const CONTAINER: usize = size_of::<NativeDeliveryResult>() + ALLOCATION;
impl NativeDeliveryResult {
    pub(super) fn new(
        result: validation::AcceptedResult,
        sequence: SessionSeq,
        ordinal: u32,
    ) -> Result<Self, ContractError> {
        if sequence.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        if result.phase() != validation::Phase::Delivery
            || result.mode() != ValidationMode::Required
            || result.verdict() != VerdictValue::Pass
            || result.resulting_state() != validation::State::Validated
            || !matches!(result.target(), validation::Target::Delivery { .. })
            || !matches!(result.receipt(), Some(receipt) if !receipt.receipt.is_zero() && receipt.epoch != 0)
            || result.attempt().is_some()
            || result.evidence().is_some()
            || result.programmatic_evidence().is_some()
            || result.reporter().is_some()
        {
            return Err(ContractError::InvalidTarget);
        }
        Ok(Self {
            result,
            sequence,
            ordinal,
        })
    }
    pub fn result(&self) -> validation::AcceptedResult {
        self.result
    }
    pub(super) fn result_ref(&self) -> &validation::AcceptedResult {
        &self.result
    }
    pub fn sequence(&self) -> SessionSeq {
        self.sequence
    }
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
}

#[derive(Debug)]
pub(super) struct OwnedDeliveryResult(Vec<NativeDeliveryResult>);
impl OwnedDeliveryResult {
    pub(super) const fn container_charge() -> usize {
        CONTAINER
    }
    pub(super) fn new(result: NativeDeliveryResult) -> Result<Self, MemoryError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if rows.capacity() != 1 {
            return Err(MemoryError::AllocationFailed);
        }
        rows.push(result);
        Ok(Self(rows))
    }
    pub(super) fn get(&self) -> Option<&NativeDeliveryResult> {
        match self.0.as_slice() {
            [row] => Some(row),
            _ => None,
        }
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        self.0
            .capacity()
            .checked_mul(size_of::<NativeDeliveryResult>())
            .and_then(|bytes| bytes.checked_add(ALLOCATION))
            .ok_or(MemoryError::CounterExhausted("native delivery heap charge"))
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        if copied.heap_charge()? > self.heap_charge()? {
            return Err(MemoryError::AllocationFailed);
        }
        Ok(copied)
    }
}
