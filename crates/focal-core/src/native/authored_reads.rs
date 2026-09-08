//! Content and lifecycle reads borrow a single effective or retained prefix.
use super::*;
use focal_model::lifecycle::{
    claim_descriptor::ClaimDescriptor, validation_descriptor::ValidationDescriptor,
};

pub(super) fn content(row: Option<&Row>) -> Option<&ClaimDescriptor> {
    match row {
        Some(Row::ClaimContent(row)) => row.descriptor(),
        _ => None,
    }
}
pub(super) fn descriptor(row: Option<&Row>) -> Option<&ValidationDescriptor> {
    match row {
        Some(Row::Definition(row)) => row.descriptor(),
        _ => None,
    }
}
fn result(row: Option<&Row>) -> Option<&NativeCreationResult> {
    match row {
        Some(Row::CreationResult(row)) => Some(row.get()),
        _ => None,
    }
}
impl Core<NativeState> {
    pub fn native_content_profile(&self) -> NativeContentProfile {
        self.state.profile
    }
    pub fn native_claim_content(&self, id: ClaimId) -> Option<&ClaimDescriptor> {
        content(self.state.rows.get(&Key::ClaimContent(id)))
    }
    pub fn native_validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.state.rows.get(&Key::Definition(id)))
    }
    pub fn native_creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Option<&NativeCreationResult> {
        result(self.state.rows.get(&Key::CreationResult(key.into())))
    }
}
impl NativePrepared {
    pub fn claim_content(&self, id: ClaimId) -> Option<&ClaimDescriptor> {
        content(self.range.get(&Key::ClaimContent(id)))
    }
    pub fn validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.range.get(&Key::Definition(id)))
    }
    pub fn creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Option<&NativeCreationResult> {
        result(self.range.get(&Key::CreationResult(key.into())))
    }
}
impl NativeView<'_> {
    pub fn content_profile(&self) -> NativeContentProfile {
        self.0.state.profile
    }
    pub fn claim_content(&self, id: ClaimId) -> Option<&ClaimDescriptor> {
        content(self.0.get(Key::ClaimContent(id)))
    }
    pub fn validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.0.get(Key::Definition(id)))
    }
    pub fn creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Option<&NativeCreationResult> {
        result(self.0.get(Key::CreationResult(key.into())))
    }
}
impl NativeRead {
    pub fn with_authored_claim<T>(
        &self,
        id: ClaimId,
        now: u64,
        project: impl FnOnce(&ClaimDescriptor, &ClaimState) -> T,
    ) -> Result<Option<T>, NativeError> {
        let key = Key::Claim(id);
        let projected = self
            .lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key != key {
                    return Ok(None);
                }
                let state = as_claim(Some(&entry.value)).ok_or(ContractError::InvalidPolicy)?;
                let key = Key::ClaimContent(id);
                let body = self
                    .lease
                    .project_next(&key, false, &Key::End, now, |entry| {
                        let body = if entry.key == key {
                            content(Some(&entry.value))
                        } else {
                            None
                        }
                        .ok_or(ContractError::InvalidPolicy)?;
                        if state.binding().content != body.content_hash()
                            || state.binding().ledger != body.ledger()
                            || state.binding().object.0 != body.id().0
                        {
                            return Err(NativeError::Contract(ContractError::ContentConflict));
                        }
                        Ok(project(body, state))
                    })?
                    .ok_or(ContractError::InvalidPolicy)??;
                Ok(Some(body))
            })?;
        projected.transpose().map(Option::flatten)
    }
    pub fn with_validation_descriptor<T>(
        &self,
        id: ValidationId,
        now: u64,
        project: impl FnOnce(&ValidationDescriptor) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::Definition(id);
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    descriptor(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
    pub fn with_creation_result<T>(
        &self,
        key: impl Into<NativeInvocation>,
        now: u64,
        project: impl FnOnce(&NativeCreationResult) -> T,
    ) -> Result<Option<T>, MemoryError> {
        let key = Key::CreationResult(key.into());
        self.lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    result(Some(&entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}
