//! Content and lifecycle reads borrow a single effective or retained prefix.
use super::owned::HeldClaimBody;
use super::*;
use focal_memory::BudgetLane;
use focal_model::lifecycle::{
    claim_descriptor::ClaimDescriptor, validation_descriptor::ValidationDescriptor,
};

/// The claim content `row` holds, decoded and charged to `budget` in `lane` while it is held
/// (doc 31 M3): none where `row` is not a claim's content.
pub(super) fn content(
    row: Option<&Row>,
    budget: &MemoryBudget,
    lane: BudgetLane,
) -> Result<Option<HeldClaimBody>, NativeError> {
    match row {
        Some(Row::ClaimContent(row)) => row.body(budget, lane).map(Some),
        _ => Ok(None),
    }
}
/// The claim content `row` holds, read in place from its encoding: nothing built, nothing
/// allocated. None where `row` is not a claim's content.
pub(super) fn content_view(
    row: Option<&Row>,
) -> Result<Option<super::input_codec::ClaimBodyInput<'_>>, NativeError> {
    match row {
        Some(Row::ClaimContent(row)) => row.view().map(Some),
        _ => Ok(None),
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
    /// Read claim `id`'s content in place from its encoding, nothing built (doc 31 M3): `read`
    /// sees its action, scopes and relations. None when no content is held for it.
    pub fn with_native_claim_content<T>(
        &self,
        id: ClaimId,
        read: impl FnOnce(&dyn super::NativeClaimContentRead) -> Result<T, NativeError>,
    ) -> Result<Option<T>, NativeError> {
        match content_view(self.state.rows.get(&Key::ClaimContent(id)))? {
            Some(view) => read(&view).map(Some),
            None => Ok(None),
        }
    }
    /// The content of claim `id`, decoded from its encoding and charged to the core's budget
    /// while it is held.
    pub fn native_claim_content(&self, id: ClaimId) -> Result<Option<HeldClaimBody>, NativeError> {
        content(
            self.state.rows.get(&Key::ClaimContent(id)),
            &self.state.budget,
            BudgetLane::Ordinary,
        )
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
    /// The content of claim `id` this candidate writes, decoded and charged to `budget`.
    pub fn written_claim_content(
        &self,
        id: ClaimId,
        budget: &MemoryBudget,
    ) -> Result<Option<HeldClaimBody>, NativeError> {
        content(
            self.fragments.get(&Key::ClaimContent(id)),
            budget,
            BudgetLane::Ordinary,
        )
    }
    pub fn written_validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.fragments.get(&Key::Definition(id)))
    }
    pub fn written_creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Option<&NativeCreationResult> {
        result(self.fragments.get(&Key::CreationResult(key.into())))
    }
}
impl<'a> NativeView<'a> {
    pub fn content_profile(&self) -> NativeContentProfile {
        self.0.state.profile
    }
    /// The content of claim `id` as this view sees it, decoded and charged to the core's
    /// budget while it is held.
    pub fn claim_content(&self, id: ClaimId) -> Result<Option<HeldClaimBody>, NativeError> {
        content(
            self.0.get(Key::ClaimContent(id)),
            &self.0.state.budget,
            BudgetLane::Ordinary,
        )
    }
    pub fn validation_descriptor(&self, id: ValidationId) -> Option<&'a ValidationDescriptor> {
        descriptor(self.0.get(Key::Definition(id)))
    }
    pub fn creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Option<&'a NativeCreationResult> {
        result(self.0.get(Key::CreationResult(key.into())))
    }
}
impl NativeRead {
    /// Project claim `id` and its content, the content decoded from its encoding and charged to
    /// `budget` while it is projected.
    pub fn with_authored_claim<T>(
        &self,
        id: ClaimId,
        now: u64,
        budget: &MemoryBudget,
        project: impl FnOnce(&ClaimDescriptor, &ClaimState) -> T,
    ) -> Result<Option<T>, NativeError> {
        let key = Key::Claim(id);
        let projected = self
            .lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key != key {
                    return Ok(None);
                }
                let state = as_claim(Some(entry.value)).ok_or(ContractError::InvalidPolicy)?;
                let key = Key::ClaimContent(id);
                let body = self
                    .lease
                    .project_next(&key, false, &Key::End, now, |entry| {
                        let body = if entry.key == key {
                            content(Some(entry.value), budget, BudgetLane::Ordinary)?
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
                        Ok(project(&body, state))
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
                    descriptor(Some(entry.value)).map(project)
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
                    result(Some(entry.value)).map(project)
                } else {
                    None
                }
            })
            .map(Option::flatten)
    }
}
