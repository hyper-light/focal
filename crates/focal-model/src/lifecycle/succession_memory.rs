use super::*;
use crate::lifecycle::memory as bytes;

impl Lineage {
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(bytes::allocation::<Correction>(self.corrections.len()))
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(bytes::allocation::<Correction>(self.corrections.capacity()))
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<Correction>(self.corrections.len())
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<Correction>(self.corrections.capacity())
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let copied = Self {
            binding: self.binding,
            cause: match self.cause {
                Cause::Root(id) => Cause::Root(id),
                Cause::Claim(id) => Cause::Claim(id),
            },
            corrections: bytes::copy(&self.corrections)?,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}
