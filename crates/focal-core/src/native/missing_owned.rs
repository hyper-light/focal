//! A missing Required slot is a checked deterministic result, without a worker
//! attempt or an invented evidence artifact. Its original publication cut is
//! retained independently of later response and evaluation revisions.
use super::prepare::ALLOCATION;
use super::*;
use focal_model::{ValidationMode, VerdictValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeMissingResult {
    result: validation::AcceptedResult,
    sequence: SessionSeq,
    ordinal: u32,
}

const CONTAINER: usize = size_of::<NativeMissingResult>() + ALLOCATION;

impl NativeMissingResult {
    /// The owner must also match this capability to its exact registered
    /// evaluation and received response before publishing the enclosing plan.
    pub(super) fn new(
        result: validation::AcceptedResult,
        sequence: SessionSeq,
        ordinal: u32,
    ) -> Result<Self, ContractError> {
        if sequence.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        let validation::Target::MissingSlot { response, .. } = result.target() else {
            return Err(ContractError::InvalidTarget);
        };
        let evaluation = result.binding();
        if result.phase() != validation::Phase::MissingTarget
            || result.mode() != ValidationMode::Required
            || result.verdict() != VerdictValue::Incomplete
            || result.resulting_state() != validation::State::ValidationIncomplete
            || result.ledger().tenant.is_zero()
            || result.ledger().session.is_zero()
            || result.claim().is_zero()
            || result.validation().is_zero()
            || evaluation.ledger != result.ledger()
            || evaluation.object.0 != result.validation().0
            || evaluation.content == ContentHash([0; 32])
            || evaluation.revision.0 == 0
            || response.ledger != result.ledger()
            || response.object.is_zero()
            || response.content == ContentHash([0; 32])
            || response.revision.0 == 0
            || result.generation() == 0
            || !matches!(result.receipt(), Some(receipt) if !receipt.receipt.is_zero() && receipt.epoch != 0)
            || result.attempt().is_some()
            || result.reporter().is_some()
            || result.evidence().is_some()
            || result.programmatic_evidence().is_some()
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
    pub fn result_ref(&self) -> &validation::AcceptedResult {
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
pub(super) struct OwnedMissingResult(Vec<NativeMissingResult>);

impl OwnedMissingResult {
    pub(super) const fn container_charge() -> usize {
        CONTAINER
    }

    pub(super) fn new(result: NativeMissingResult) -> Result<Self, MemoryError> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(1)
            .map_err(|_| MemoryError::AllocationFailed)?;
        if rows.capacity() != 1 {
            return Err(MemoryError::AllocationFailed);
        }
        rows.push(result);
        Ok(Self(rows))
    }

    pub(super) fn get(&self) -> Option<&NativeMissingResult> {
        match self.0.as_slice() {
            [row] => Some(row),
            _ => None,
        }
    }

    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        self.0
            .capacity()
            .checked_mul(size_of::<NativeMissingResult>())
            .and_then(|bytes| bytes.checked_add(ALLOCATION))
            .ok_or(MemoryError::CounterExhausted(
                "native missing result heap charge",
            ))
    }

    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        if copied.heap_charge()? > self.heap_charge()? {
            return Err(MemoryError::AllocationFailed);
        }
        Ok(copied)
    }
}

#[cfg(test)]
#[path = "missing_owned_tests.rs"]
mod tests;
