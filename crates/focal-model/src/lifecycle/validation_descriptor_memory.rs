use super::*;

pub(super) fn authored_heap(
    description: usize,
    quality: usize,
    contributors: usize,
) -> Result<usize, ContractError> {
    bytes::add(
        bytes::add(description, quality)?,
        bytes::array::<ParticipantId>(contributors)?,
    )
}

pub(super) fn authored_allocations(
    description: usize,
    quality: usize,
    contributors: usize,
) -> Result<usize, ContractError> {
    bytes::add(
        bytes::add(
            bytes::allocation::<u8>(description),
            bytes::allocation::<u8>(quality),
        )?,
        bytes::allocation::<ParticipantId>(contributors),
    )
}

pub(super) fn copy<T: Copy>(values: &[T]) -> Result<Box<[T]>, ContractError> {
    let mut owned = bytes::reserve::<T>(values.len())?;
    bytes::fits(owned.capacity(), values.len())?;
    owned.extend_from_slice(values);
    exact(owned)
}

/// `values` as a boxed slice in the buffer they fill (a full buffer converts without moving).
pub(super) fn exact<T>(values: Vec<T>) -> Result<Box<[T]>, ContractError> {
    if values.len() != values.capacity() {
        return Err(ContractError::Capacity);
    }
    Ok(values.into_boxed_slice())
}

pub(super) fn string(value: &str) -> Result<Box<str>, ContractError> {
    let bytes = copy(value.as_bytes())?;
    String::from_utf8(bytes.into_vec())
        .map(String::into_boxed_str)
        .map_err(|_| ContractError::InvalidPolicy)
}

impl ValidationDescriptor {
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(self.declaration.copy_heap_bytes()?, self.authored_heap()?)
    }
    /// Held exactly: what a copy requests is what the descriptor holds.
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.retained_heap_bytes()?,
            self.authored_heap()?,
        )
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.copy_heap_allocations()?,
            self.authored_allocations()?,
        )
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.heap_allocations()?,
            self.authored_allocations()?,
        )
    }
    fn authored_heap(&self) -> Result<usize, ContractError> {
        authored_heap(
            self.description.len(),
            self.quality_bar.as_deref().map_or(0, str::len),
            self.contributed_by.len(),
        )
    }
    fn authored_allocations(&self) -> Result<usize, ContractError> {
        authored_allocations(
            self.description.len(),
            self.quality_bar.as_deref().map_or(0, str::len),
            self.contributed_by.len(),
        )
    }
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.retained_heap_bytes()?)
    }
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        let quote = self.copy_charge()?;
        bytes::fits(quote, max_bytes)?;
        let copied = Self {
            schema: self.schema,
            declaration: self.declaration.try_copy(self.declaration.copy_charge()?)?,
            description: string(&self.description)?,
            quality_bar: self.quality_bar.as_deref().map(string).transpose()?,
            contributed_by: copy(&self.contributed_by)?,
            policy_revision: self.policy_revision,
            specification_hash: self.specification_hash,
        };
        bytes::fits(copied.retained_bytes()?, quote)?;
        Ok(copied)
    }
}
