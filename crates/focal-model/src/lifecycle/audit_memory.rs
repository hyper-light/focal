//! Fallible owned audit copies retain unused result slots promised at sealing.
//! Byte getters exclude allocator metadata, matching the other native rows;
//! owners reserve heap_allocations/copy_heap_allocations headers separately.
use super::*;
use crate::lifecycle::memory as bytes;

impl AuditCohort {
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::array::<AuditMember>(self.members.len())?,
            bytes::array::<AcceptedResult>(self.result_capacity)?,
        )
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::array::<AuditMember>(self.members.capacity())?,
            bytes::array::<AcceptedResult>(self.results.capacity())?,
        )
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::allocation::<AuditMember>(self.members.len()),
            bytes::allocation::<AcceptedResult>(self.result_capacity),
        )
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::allocation::<AuditMember>(self.members.capacity()),
            bytes::allocation::<AcceptedResult>(self.results.capacity()),
        )
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.results.len(), self.result_capacity)?;
        let charge = self.copy_charge()?;
        bytes::fits(charge, max_bytes)?;
        let mut members = bytes::reserve(self.members.len())?;
        bytes::fits(members.capacity(), self.members.len())?;
        let mut results = bytes::reserve(self.result_capacity)?;
        bytes::fits(results.capacity(), self.result_capacity)?;
        members.extend_from_slice(&self.members);
        results.extend_from_slice(&self.results);
        let copied = Self {
            claim: self.claim,
            issuer: self.issuer,
            sequence: self.sequence,
            members,
            results,
            result_capacity: self.result_capacity,
        };
        bytes::fits(copied.retained_bytes()?, charge)?;
        Ok(copied)
    }
}

impl ResultTestament {
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        self.cohort.copy_heap_bytes()
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        self.cohort.retained_heap_bytes()
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        self.cohort.copy_heap_allocations()
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        self.cohort.heap_allocations()
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        let charge = self.copy_charge()?;
        bytes::fits(charge, max_bytes)?;
        let copied = Self {
            binding: self.binding,
            cohort: self.cohort.try_copy(self.cohort.copy_charge()?)?,
            state: self.state,
        };
        bytes::fits(copied.retained_bytes()?, charge)?;
        Ok(copied)
    }
}
