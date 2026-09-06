use super::*;
use crate::lifecycle::memory as bytes;

impl Declaration {
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(bytes::allocation::<Obligation>(self.obligations.len()))
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        Ok(bytes::allocation::<Obligation>(self.obligations.capacity()))
    }
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<Obligation>(self.obligations.len())
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<Obligation>(self.obligations.capacity())
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    /// Copy immutable admitted data without revalidating it or retaining spare capacity.
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let copied = Self {
            obligations: bytes::copy(&self.obligations)?,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_reserved_buffer_is_retained_but_compact_copy_does_not_allocate() {
        let mut original = Declaration::empty();
        original.obligations.reserve_exact(8);
        assert_eq!(original.heap_allocations().unwrap(), 1);
        assert_eq!(original.copy_heap_allocations().unwrap(), 0);
        assert!(original.retained_heap_bytes().unwrap() > 0);
        assert_eq!(original.copy_heap_bytes().unwrap(), 0);
        let charge = original.copy_charge().unwrap();
        assert_eq!(charge, std::mem::size_of::<Declaration>());
        let copied = bytes::fail_after(0, || original.try_copy(charge)).unwrap();
        assert_eq!(copied, original);
        assert_eq!(copied.heap_allocations().unwrap(), 0);
        assert_eq!(copied.retained_bytes().unwrap(), charge);
    }
}
