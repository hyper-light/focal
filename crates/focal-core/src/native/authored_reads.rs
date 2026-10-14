//! Content and lifecycle reads borrow a single effective or retained prefix.
use super::owned::{ContentBuffer, HeldClaimBody};
use super::payloads::PayloadRead;
use super::*;
use focal_memory::BudgetLane;
use focal_model::lifecycle::{
    claim_descriptor::ClaimDescriptor, validation_descriptor::ValidationDescriptor,
};

/// The claim content `row` holds, decoded and charged to `budget` in `lane` while it is held
/// (doc 31 M3), read through `payloads` where it is spilled: none where `row` is not a claim's
/// content.
pub(super) fn content(
    row: Option<&Row>,
    payloads: Option<&dyn PayloadRead>,
    budget: &MemoryBudget,
    lane: BudgetLane,
) -> Result<Option<HeldClaimBody>, NativeError> {
    match row {
        Some(Row::ClaimContent(row)) => {
            let mut buffer = ContentBuffer::new();
            row.read(payloads, &mut buffer)?
                .body(budget, lane)
                .map(Some)
        }
        _ => Ok(None),
    }
}
/// The claim content `row` holds, read in place from its encoding (from `payloads` into `buffer`
/// where it is spilled): nothing built, nothing allocated. None where `row` is not a claim's
/// content.
pub(super) fn content_view<'a>(
    row: Option<&'a Row>,
    payloads: Option<&dyn PayloadRead>,
    buffer: &'a mut ContentBuffer,
) -> Result<Option<super::input_codec::ClaimBodyInput<'a>>, NativeError> {
    match row {
        Some(Row::ClaimContent(row)) => row.read(payloads, buffer)?.view().map(Some),
        _ => Ok(None),
    }
}
pub(super) fn descriptor(row: Option<&Row>) -> Option<&ValidationDescriptor> {
    match row {
        Some(Row::Definition(row)) => row.descriptor(),
        _ => None,
    }
}
/// The creation result `row` holds, a spilled one read back through `payloads`, decoded and
/// charged to `budget` while it is held (doc 31 M3).
fn result<'a>(
    row: Option<&'a Row>,
    payloads: Option<&dyn PayloadRead>,
    budget: &MemoryBudget,
) -> Result<Option<NativeCreationRead<'a>>, NativeError> {
    match row {
        Some(Row::CreationResult(row)) => {
            row.read(payloads, budget, BudgetLane::Ordinary).map(Some)
        }
        _ => Ok(None),
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
        let mut buffer = ContentBuffer::new();
        match content_view(
            self.state.rows.get(&Key::ClaimContent(id)),
            self.state.payloads(),
            &mut buffer,
        )? {
            Some(view) => read(&view).map(Some),
            None => Ok(None),
        }
    }
    /// The content of claim `id`, decoded from its encoding and charged to the core's budget
    /// while it is held.
    pub fn native_claim_content(&self, id: ClaimId) -> Result<Option<HeldClaimBody>, NativeError> {
        content(
            self.state.rows.get(&Key::ClaimContent(id)),
            self.state.payloads(),
            &self.state.budget,
            BudgetLane::Ordinary,
        )
    }
    pub fn native_validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.state.rows.get(&Key::Definition(id)))
    }
    /// The result of the creation `key` made, a spilled one read back, decoded and charged to
    /// the core's budget while it is held.
    pub fn native_creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeCreationRead<'_>>, NativeError> {
        result(
            self.state.rows.get(&Key::CreationResult(key.into())),
            self.state.payloads(),
            &self.state.budget,
        )
    }
}
impl NativePrepared {
    /// The content of claim `id` this candidate writes, decoded and charged to `budget`, read
    /// through `payloads` (its core's, [`Core::native_payloads`]) where it is spilled.
    pub fn written_claim_content(
        &self,
        id: ClaimId,
        payloads: Option<&dyn PayloadRead>,
        budget: &MemoryBudget,
    ) -> Result<Option<HeldClaimBody>, NativeError> {
        content(
            self.fragments.get(&Key::ClaimContent(id)),
            payloads,
            budget,
            BudgetLane::Ordinary,
        )
    }
    pub fn written_validation_descriptor(&self, id: ValidationId) -> Option<&ValidationDescriptor> {
        descriptor(self.fragments.get(&Key::Definition(id)))
    }
    /// The creation result this candidate writes for `key`, read through `payloads` (its core's,
    /// [`Core::native_payloads`]) where it is spilled and charged to `budget` while it is held.
    pub fn written_creation_result(
        &self,
        key: impl Into<NativeInvocation>,
        payloads: Option<&dyn PayloadRead>,
        budget: &MemoryBudget,
    ) -> Result<Option<NativeCreationRead<'_>>, NativeError> {
        result(
            self.fragments.get(&Key::CreationResult(key.into())),
            payloads,
            budget,
        )
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
            self.0.payloads(),
            &self.0.state.budget,
            BudgetLane::Ordinary,
        )
    }
    pub fn validation_descriptor(&self, id: ValidationId) -> Option<&'a ValidationDescriptor> {
        descriptor(self.0.get(Key::Definition(id)))
    }
    /// The result of the creation `key` made as this view sees it, a spilled one read back,
    /// decoded and charged to the core's budget while it is held.
    pub fn creation_result(
        &self,
        key: impl Into<NativeInvocation>,
    ) -> Result<Option<NativeCreationRead<'a>>, NativeError> {
        result(
            self.0.get(Key::CreationResult(key.into())),
            self.0.payloads(),
            &self.0.state.budget,
        )
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
                            content(
                                Some(entry.value),
                                self.payloads
                                    .as_deref()
                                    .map(|read| -> &dyn PayloadRead { read }),
                                budget,
                                BudgetLane::Ordinary,
                            )?
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
    /// Project the result of the creation `key` made, a spilled one read back, decoded and
    /// charged to `budget` while it is projected.
    pub fn with_creation_result<T>(
        &self,
        key: impl Into<NativeInvocation>,
        now: u64,
        budget: &MemoryBudget,
        project: impl FnOnce(&NativeCreationResult) -> T,
    ) -> Result<Option<T>, NativeError> {
        let key = Key::CreationResult(key.into());
        let payloads = self
            .payloads
            .as_deref()
            .map(|read| -> &dyn PayloadRead { read });
        let projected = self
            .lease
            .project_next(&key, false, &Key::End, now, |entry| {
                if entry.key == key {
                    result(Some(entry.value), payloads, budget)
                        .map(|read| read.map(|read| project(&read)))
                } else {
                    Ok(None)
                }
            })?;
        projected.transpose().map(Option::flatten)
    }
}
