//! Compact fallible row copying for a native owner. These methods preserve all
//! immutable declarations and effective lifecycle fields without readmission.
use super::*;
use crate::lifecycle::memory as bytes;

impl ClaimState {
    /// Number of buffers requested by a compact copy, for owner allocator overhead.
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::allocation::<ResponseRecord>(self.responses.len());
        for part in [
            self.graph.copy_heap_allocations()?,
            self.lineage.copy_heap_allocations()?,
            self.acceptance.copy_heap_allocations()?,
            self.scopes.copy_heap_allocations()?,
        ] {
            count = bytes::add(count, part)?;
        }
        Ok(count)
    }
    /// Number of actual nonzero-capacity buffers, including reserved empty Vecs.
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        let mut count = bytes::allocation::<ResponseRecord>(self.responses.capacity());
        for part in [
            self.graph.heap_allocations()?,
            self.lineage.heap_allocations()?,
            self.acceptance.heap_allocations()?,
            self.scopes.heap_allocations()?,
        ] {
            count = bytes::add(count, part)?;
        }
        Ok(count)
    }
    /// Dynamic requested capacities for a compact copy. RangeStore separately
    /// charges the inline Entry/row and must not add that storage a second time.
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::array::<ResponseRecord>(self.responses.len())?;
        for part in [
            self.graph.copy_heap_bytes()?,
            self.lineage.copy_heap_bytes()?,
            self.acceptance.copy_heap_bytes()?,
            self.scopes.copy_heap_bytes()?,
        ] {
            size = bytes::add(size, part)?;
        }
        Ok(size)
    }
    /// Actual dynamic Vec capacities, excluding inline row/allocator metadata.
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        let mut size = bytes::array::<ResponseRecord>(self.responses.capacity())?;
        for part in [
            self.graph.retained_heap_bytes()?,
            self.lineage.retained_heap_bytes()?,
            self.acceptance.retained_heap_bytes()?,
            self.scopes.retained_heap_bytes()?,
        ] {
            size = bytes::add(size, part)?;
        }
        Ok(size)
    }
    /// Inline ClaimState plus all requested compact-copy capacities. The entire
    /// charge is computed before the first owned buffer allocation.
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    /// The effective owner reserves `max_bytes` before calling, or uses an
    /// already charged touched-page row. No extra permit is acquired here.
    /// Allocator-reported capacities are checked again before returning; failure
    /// drops all provisional buffers and leaves the original row untouched.
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let copied = Self {
            binding: self.binding,
            issuer: self.issuer,
            subject: self.subject,
            created: self.created,
            graph: self.graph.try_copy(self.graph.copy_charge()?)?,
            lineage: self.lineage.try_copy(self.lineage.copy_charge()?)?,
            acceptance: self.acceptance.try_copy(self.acceptance.copy_charge()?)?,
            scopes: self.scopes.try_copy(self.scopes.copy_charge()?)?,
            status: self.status,
            receipt: self.receipt,
            responses: bytes::copy(&self.responses)?,
            max_responses: self.max_responses,
            deadline: self.deadline,
            local_complete: self.local_complete,
            local_sealed_at: self.local_sealed_at,
            terminal_cut: self.terminal_cut,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

#[cfg(test)]
#[path = "claim_memory_tests.rs"]
mod tests;
