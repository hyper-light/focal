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

pub(super) fn copy<T: Copy>(values: &[T]) -> Result<Vec<T>, ContractError> {
    let mut owned = bytes::reserve::<T>(values.len())?;
    bytes::fits(owned.capacity(), values.len())?;
    owned.extend_from_slice(values);
    Ok(owned)
}

pub(super) fn string(value: &str) -> Result<String, ContractError> {
    String::from_utf8(copy(value.as_bytes())?).map_err(|_| ContractError::InvalidPolicy)
}

impl ValidationDescriptor {
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.copy_heap_bytes()?,
            authored_heap(
                self.description.len(),
                self.quality_bar.as_ref().map_or(0, String::len),
                self.contributed_by.len(),
            )?,
        )
    }
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.retained_heap_bytes()?,
            authored_heap(
                self.description.capacity(),
                self.quality_bar.as_ref().map_or(0, String::capacity),
                self.contributed_by.capacity(),
            )?,
        )
    }
    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.copy_heap_allocations()?,
            authored_allocations(
                self.description.len(),
                self.quality_bar.as_ref().map_or(0, String::len),
                self.contributed_by.len(),
            )?,
        )
    }
    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.declaration.heap_allocations()?,
            authored_allocations(
                self.description.capacity(),
                self.quality_bar.as_ref().map_or(0, String::capacity),
                self.contributed_by.capacity(),
            )?,
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
