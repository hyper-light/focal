//! Compact owner-held evaluation membership. Full immutable policies and
//! definitions live in their canonical rows and are never copied into this set.
use super::acceptance::{RegisteredEvaluation, same_target};
use crate::ContentHash;
use crate::lifecycle::claim::ClaimState;
use crate::lifecycle::memory as bytes;
use crate::lifecycle::validation::{Evaluation, Target};
use crate::lifecycle::{Binding, ContractError};

const ALLOCATION: usize = 4 * std::mem::size_of::<usize>();

#[derive(Debug, PartialEq, Eq)]
pub struct RegistrationSet {
    claim: Binding,
    policy: ContentHash,
    rows: Vec<RegisteredEvaluation>,
    max_rows: usize,
    sealed: bool,
    increments_sealed: bool,
}

fn buffer(capacity: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<RegisteredEvaluation>(capacity)?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}

impl RegistrationSet {
    pub fn new(
        claim: &ClaimState,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, ContractError> {
        if max_rows == 0 {
            return Err(ContractError::Capacity);
        }
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        bytes::fits(std::mem::size_of::<Self>(), max_bytes)?;
        Ok(Self {
            claim: claim.binding(),
            policy: claim.acceptance().intent_fingerprint(),
            rows: Vec::new(),
            max_rows,
            sealed: false,
            increments_sealed: false,
        })
    }
    pub fn check(&self, claim: &ClaimState) -> Result<(), ContractError> {
        Binding {
            revision: self.claim.revision,
            ..claim.binding()
        }
        .check(&self.claim)?;
        if claim.binding().revision < self.claim.revision {
            return Err(ContractError::StaleRevision);
        }
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        if self.policy != claim.acceptance().intent_fingerprint() {
            return Err(ContractError::InvalidPolicy);
        }
        Ok(())
    }
    pub fn rows(&self) -> &[RegisteredEvaluation] {
        &self.rows
    }
    pub fn max_rows(&self) -> usize {
        self.max_rows
    }
    /// A compact copy with enough checked spare entries for one atomic target
    /// materialization. Registering that cohort then performs no buffer growth.
    pub fn copy_with_additional_heap_bytes(
        &self,
        additional: usize,
    ) -> Result<usize, ContractError> {
        let count = bytes::add(self.rows.len(), additional)?;
        bytes::fits(count, self.max_rows)?;
        bytes::array::<RegisteredEvaluation>(count)
    }
    pub fn copy_with_additional_charge(&self, additional: usize) -> Result<usize, ContractError> {
        bytes::add(
            std::mem::size_of::<Self>(),
            self.copy_with_additional_heap_bytes(additional)?,
        )
    }
    pub fn try_copy_with_additional(
        &self,
        additional: usize,
        max_bytes: usize,
    ) -> Result<Self, ContractError> {
        bytes::fits(self.copy_with_additional_charge(additional)?, max_bytes)?;
        let count = bytes::add(self.rows.len(), additional)?;
        let mut rows = bytes::reserve(count)?;
        bytes::fits(rows.capacity(), count)?;
        rows.extend_from_slice(&self.rows);
        let copied = Self {
            claim: self.claim,
            policy: self.policy,
            rows,
            max_rows: self.max_rows,
            sealed: self.sealed,
            increments_sealed: self.increments_sealed,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
    pub fn increment_targets_sealed(&self) -> bool {
        self.increments_sealed
    }
    pub fn seal_increment_targets(&mut self, claim: &ClaimState) -> Result<(), ContractError> {
        self.check(claim)?;
        self.increments_sealed = true;
        Ok(())
    }
    pub fn seal_targets(&mut self, claim: &ClaimState) -> Result<(), ContractError> {
        self.check(claim)?;
        self.sealed = true;
        self.increments_sealed = true;
        Ok(())
    }
    /// Registration and its independent EvaluationState are one owner mutation.
    /// A later generation requires a separate checked replacement operation; it
    /// cannot acquire live authority by silently resetting an existing target.
    pub fn register(
        &mut self,
        claim: &ClaimState,
        evaluation: &Evaluation<'_>,
        max_bytes: usize,
    ) -> Result<bool, ContractError> {
        self.check(claim)?;
        let row = claim.acceptance().checked_registration(evaluation)?;
        if let Some(old) = self.rows.iter().find(|old| {
            old.binding().object == row.binding().object && same_target(old.target(), row.target())
        }) {
            return if *old == row {
                Ok(false)
            } else {
                Err(ContractError::StaleEvaluation)
            };
        }
        if claim.status().is_terminal()
            || claim.local_complete()
            || self.sealed
            || (self.increments_sealed && matches!(row.target(), Target::Increment { .. }))
        {
            return Err(ContractError::InvalidTransition);
        }
        let count = self
            .rows
            .len()
            .checked_add(1)
            .ok_or(ContractError::Capacity)?;
        if count > self.max_rows {
            return Err(ContractError::Capacity);
        }
        bytes::fits(self.allocation_charge()?, max_bytes)?;
        if count <= self.rows.capacity() {
            self.rows.push(row);
            return Ok(true);
        }
        let capacity = self
            .rows
            .capacity()
            .checked_mul(2)
            .unwrap_or(self.max_rows)
            .max(count)
            .min(self.max_rows);
        // Precharge old and replacement simultaneously, then reconcile actual
        // capacity before replacing the old complete registration collection.
        bytes::fits(
            bytes::add(self.allocation_charge()?, buffer(capacity)?)?,
            max_bytes,
        )?;
        let mut replacement = bytes::reserve(capacity)?;
        bytes::fits(
            bytes::add(self.allocation_charge()?, buffer(replacement.capacity())?)?,
            max_bytes,
        )?;
        replacement.extend_from_slice(&self.rows);
        replacement.push(row);
        self.rows = replacement;
        Ok(true)
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<RegisteredEvaluation>(self.rows.len())
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<RegisteredEvaluation>(self.rows.capacity())
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(usize::from(!self.rows.is_empty()))
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(usize::from(self.rows.capacity() != 0))
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::add(std::mem::size_of::<Self>(), self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(std::mem::size_of::<Self>(), self.retained_heap_bytes()?)
    }
    fn allocation_charge(&self) -> Result<usize, ContractError> {
        bytes::add(std::mem::size_of::<Self>(), buffer(self.rows.capacity())?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let copied = Self {
            claim: self.claim,
            policy: self.policy,
            rows: bytes::copy(&self.rows)?,
            max_rows: self.max_rows,
            sealed: self.sealed,
            increments_sealed: self.increments_sealed,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}
