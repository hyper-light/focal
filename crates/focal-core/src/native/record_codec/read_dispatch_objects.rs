//! Exact restored-row adapters. Narrow model traits retain native lookup errors
//! in one stack-owned cell, then return the original cause at the dispatch edge.
use super::*;
use std::cell::Cell;
use focal_model::lifecycle::{artifact_descriptor::ArtifactDescriptor, claim::ClaimState,
    evidence::{Response, ResponseArtifacts, WorkArtifact}, validation::{AcceptedResult, Declaration, EvaluationState}};
use focal_model::{ArtifactRef, ValidationId};
use focal_model::lifecycle::aggregation::{AcceptancePolicy, PublicationPosition};

pub(super) struct Access<'a, O> { objects: &'a O, lookup: &'a Meter, error: Cell<Option<NativeError>> }
impl<'a, O: Objects> Access<'a, O> {
    pub(super) fn new(objects: &'a O, lookup: &'a Meter) -> Self { Self { objects, lookup, error: Cell::new(None) } }
    pub(super) fn finish<T>(&self, result: Result<T, NativeError>) -> Result<T, NativeError> {
        match self.error.take() { Some(error) => Err(error), None => result }
    }
    fn remember(&self, error: NativeError) -> ContractError {
        let category = match &error {
            NativeError::Capacity(_) | NativeError::Memory(_) => ContractError::Capacity,
            _ => ContractError::InvalidManifest,
        };
        let previous = self.error.take();
        self.error.set(Some(previous.unwrap_or(error)));
        category
    }
    fn contract<T>(&self, value: Result<T, NativeError>) -> Result<T, ContractError> {
        value.map_err(|error| self.remember(error))
    }
    fn audit<T>(&self, value: Result<T, NativeError>) -> Result<T, CodecError> {
        value.map_err(|error| match self.remember(error) {
            ContractError::Capacity => CodecError::Capacity,
            _ => CodecError::InvalidTag("recovery dependency"),
        })
    }
    pub(super) fn ledger(&self) -> LedgerId { self.objects.ledger() }
    fn get(&self, key: Key) -> Result<&Row, NativeError> {
        debit(self.lookup, 256)?;
        self.objects.get(key, self.lookup)?.ok_or(ContractError::MissingEvidence.into())
    }
    pub(super) fn raw_claim(&self, id: ClaimId) -> Result<&[u8], NativeError> {
        self.objects.raw_claim(id, self.lookup)
    }
    pub(super) fn declaration(&self, id: ValidationId) -> Result<&Declaration, NativeError> {
        let Row::Definition(row) = self.get(Key::Definition(id))? else { return Err(invalid()); };
        let value = row.get().ok_or_else(invalid)?;
        if value.binding().ledger != self.ledger() || value.binding().object.0 != id.0 { return Err(invalid()); }
        Ok(value)
    }
    fn claim(&self, id: ClaimId) -> Result<&ClaimState, NativeError> {
        let Row::Claim(row) = self.get(Key::Claim(id))? else { return Err(invalid()); };
        let value = row.claim().ok_or_else(invalid)?;
        if value.binding().ledger != self.ledger() || value.binding().object.0 != id.0 { return Err(invalid()); }
        Ok(value)
    }
    fn response(&self, id: TestamentId) -> Result<&Response, NativeError> {
        let Row::Response(row) = self.get(Key::Response(id))? else { return Err(invalid()); };
        let value = row.get().ok_or_else(invalid)?;
        if value.identity().binding.ledger != self.ledger() || value.identity().binding.object.0 != id.0 { return Err(invalid()); }
        Ok(value)
    }
    fn evaluation(&self, key: EvaluationKey) -> Result<&EvaluationState, NativeError> {
        let Row::Evaluation(row) = self.get(Key::Evaluation(key))? else { return Err(invalid()); };
        let value = row.get().ok_or_else(invalid)?;
        if value.binding().ledger != self.ledger() || EvaluationKey::of(key.claim, value) != key { return Err(invalid()); }
        Ok(value)
    }
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, NativeError> {
        let Row::Artifact(row) = self.get(Key::Artifact(reference.id))? else { return Err(invalid()); };
        let value = row.get().ok_or_else(invalid)?.descriptor();
        if value.ledger() != self.ledger() || value.id() != reference.id || value.content_hash() != reference.hash {
            return Err(ContractError::MissingEvidence.into());
        }
        Ok(value)
    }
    fn work(&self, id: ArtifactId) -> Result<&WorkArtifact, NativeError> {
        let Row::Work(row) = self.get(Key::Work(id))? else { return Err(invalid()); };
        let value = &row.get().ok_or_else(invalid)?.state;
        if value.binding().ledger != self.ledger() || value.binding().object.0 != id.0 { return Err(invalid()); }
        Ok(value)
    }
    /// Borrow the retained result itself, preserving its actual model witness.
    /// At most three fixed-family lookups; no cloned evidence or synthesized
    /// accepted result is used to satisfy another object's hydration contract.
    fn result(&self, key: NativeResultKey) -> Result<(&AcceptedResult, PublicationPosition), NativeError> {
        let mut found = None;
        debit(self.lookup, 1024)?;
        for candidate in [Key::Accepted(key), Key::DeliveryResult(key), Key::MissingResult(key)] {
            let value = match self.objects.get(candidate, self.lookup)? {
                None => continue,
                Some(Row::Accepted(row)) => { let value = row.get().ok_or_else(invalid)?;
                    (value.result_ref(), PublicationPosition { sequence: value.sequence(), ordinal: value.ordinal() }) }
                Some(Row::DeliveryResult(row)) => { let value = row.get().ok_or_else(invalid)?;
                    (value.result_ref(), PublicationPosition { sequence: value.sequence(), ordinal: value.ordinal() }) }
                Some(Row::MissingResult(row)) => { let value = row.get().ok_or_else(invalid)?;
                    (value.result_ref(), PublicationPosition { sequence: value.sequence(), ordinal: value.ordinal() }) }
                Some(_) => return Err(invalid()),
            };
            if found.is_some() || value.0.ledger() != self.ledger() || NativeResultKey::of(*value.0) != key { return Err(invalid()); }
            found = Some(value);
        }
        found.ok_or(ContractError::MissingEvidence.into())
    }
    fn event(&self, at: PublicationPosition) -> Result<NativeEvent, NativeError> {
        let Row::Event(row) = self.get(Key::Event(at.sequence, at.ordinal))? else { return Err(invalid()); };
        debit(self.lookup, 256)?;
        let value = row.get().ok_or_else(invalid)?.expand(self.ledger());
        if value.sequence != at.sequence || value.ordinal != at.ordinal { return Err(invalid()); }
        Ok(value)
    }
}
impl<O: Objects> read_claim::Objects for Access<'_, O> {
    fn declaration(&self, id: ValidationId) -> Result<&Declaration, ContractError> { self.contract(self.declaration(id)) }
    fn response(&self, id: TestamentId) -> Result<&Response, ContractError> { self.contract(self.response(id)) }
    fn evaluation(&self, key: EvaluationKey) -> Result<&EvaluationState, ContractError> { self.contract(self.evaluation(key)) }
}
impl<O: Objects> evidence::Dependencies for Access<'_, O> {
    fn policy(&self, _claim: ClaimId) -> Result<&AcceptancePolicy, ContractError> { Err(ContractError::InvalidPolicy) }
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError> { self.contract(self.artifact(reference)) }
    fn declaration(&self, id: ValidationId) -> Result<&Declaration, ContractError> { self.contract(self.declaration(id)) }
}
impl<O: Objects> ResponseArtifacts for Access<'_, O> {
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError> { self.contract(self.artifact(reference)) }
    fn work(&self, id: ArtifactId) -> Result<&WorkArtifact, ContractError> { self.contract(self.work(id)) }
}
pub(super) struct Policy<'a, 'o, O> { pub(super) access: &'a Access<'o, O>, pub(super) policy: &'a AcceptancePolicy }
impl<O: Objects> evidence::Dependencies for Policy<'_, '_, O> {
    fn policy(&self, claim: ClaimId) -> Result<&AcceptancePolicy, ContractError> {
        if self.policy.claim().object.0 != claim.0 || self.policy.claim().ledger != self.access.ledger() { return Err(ContractError::InvalidPolicy); }
        Ok(self.policy)
    }
    fn artifact(&self, reference: ArtifactRef) -> Result<&ArtifactDescriptor, ContractError> { self.access.contract(self.access.artifact(reference)) }
    fn declaration(&self, id: ValidationId) -> Result<&Declaration, ContractError> { self.access.contract(self.access.declaration(id)) }
}
impl<O: Objects> read_audit::AuditObjects for Access<'_, O> {
    fn prefix(&self) -> SessionSeq { self.objects.prefix() }
    fn claim(&self, id: ClaimId, _meter: &Meter) -> Result<&ClaimState, CodecError> { self.audit(self.claim(id)) }
    fn definition(&self, id: ValidationId, _meter: &Meter) -> Result<&Declaration, CodecError> { self.audit(self.declaration(id)) }
    fn result(&self, key: NativeResultKey, _meter: &Meter) -> Result<(AcceptedResult, PublicationPosition), CodecError> {
        self.audit(self.result(key).map(|(result, at)| (*result, at)))
    }
    fn event(&self, at: PublicationPosition, _meter: &Meter) -> Result<NativeEvent, CodecError> { self.audit(self.event(at)) }
}

pub(super) fn heap(row: &Row) -> Result<usize, NativeError> {
    Ok(match row {
        Row::Claim(value) => value.heap_charge()?, Row::Definition(value) => value.heap_charge()?,
        Row::Evaluation(value) => value.heap_charge()?, Row::Artifact(value) => value.heap_charge()?,
        Row::Accepted(value) => value.heap_charge()?, Row::DeliveryResult(value) => value.heap_charge()?,
        Row::MissingResult(value) => value.heap_charge()?, Row::Work(value) => value.heap_charge()?,
        Row::Diagnostic(value) => value.heap_charge()?, Row::Response(value) => value.heap_charge()?,
        Row::ResultTestament(value) => value.heap_charge()?, Row::Event(value) => value.heap_charge()?,
        Row::ClaimContent(value) => value.heap_charge()?, Row::CreationResult(value) => value.heap_charge()?,
        Row::IncomingHead(_) | Row::IncomingLink(_) | Row::Monitor(_) | Row::MonitorHead(_)
        | Row::MonitorLink(_) | Row::Meta(_) | Row::ArtifactIdentity(_) | Row::Receipt(_)
        | Row::Cycle(_) | Row::RetiredCycleHead(_) | Row::RetiredCycle(_) | Row::WorkSlot(_)
        | Row::ClaimResultTestament(_) | Row::Outcome(_) | Row::ClaimIdentity(_) | Row::DefinitionIdentity(_) => 0,
    })
}
fn binding(value: Binding, ledger: LedgerId, id: [u8; 16]) -> Result<(), NativeError> {
    if value.ledger != ledger || value.object.0 != id { return Err(invalid()); }
    Ok(())
}
pub(super) fn check(key: Key, row: &Row, ledger: LedgerId) -> Result<(), NativeError> {
    mutation::check_family(key, row)?;
    match (key, row) {
        (Key::Claim(id), Row::Claim(value)) => binding(value.claim().ok_or_else(invalid)?.binding(), ledger, id.0)?,
        (Key::Definition(id), Row::Definition(value)) => binding(value.get().ok_or_else(invalid)?.binding(), ledger, id.0)?,
        (Key::Evaluation(key), Row::Evaluation(value)) => {
            let value = value.get().ok_or_else(invalid)?;
            binding(value.binding(), ledger, key.validation.0)?;
            if EvaluationKey::of(key.claim, value) != key { return Err(invalid()); }
        }
        (Key::Artifact(id), Row::Artifact(value)) => binding(value.get().ok_or_else(invalid)?.descriptor().binding(), ledger, id.0)?,
        (Key::Work(id), Row::Work(value)) => binding(value.get().ok_or_else(invalid)?.state.binding(), ledger, id.0)?,
        (Key::Response(id), Row::Response(value)) => binding(value.get().ok_or_else(invalid)?.identity().binding, ledger, id.0)?,
        (Key::ResultTestament(id), Row::ResultTestament(value)) => binding(value.get().ok_or_else(invalid)?.testament().binding(), ledger, id.0)?,
        (Key::ClaimContent(id), Row::ClaimContent(value)) => binding(value.get().ok_or_else(invalid)?.binding(), ledger, id.0)?,
        (Key::Diagnostic(id), Row::Diagnostic(value)) => {
            let snapshot = value.get().ok_or_else(invalid)?.diagnostic.snapshot_v1();
            if snapshot.ledger != ledger || snapshot.diagnostic.artifact.id != id { return Err(invalid()); }
        }
        (Key::Accepted(key), Row::Accepted(value)) => result_key(key, value.get().ok_or_else(invalid)?.result(), ledger)?,
        (Key::DeliveryResult(key), Row::DeliveryResult(value)) => result_key(key, value.get().ok_or_else(invalid)?.result(), ledger)?,
        (Key::MissingResult(key), Row::MissingResult(value)) => result_key(key, value.get().ok_or_else(invalid)?.result(), ledger)?,
        // Fixed-family/EventPlan decoders already verified their key identity;
        // creation invocation correspondence belongs to complete root history.
        _ => {}
    }
    Ok(())
}
fn result_key(key: NativeResultKey, result: AcceptedResult, ledger: LedgerId) -> Result<(), NativeError> {
    if result.ledger() != ledger || NativeResultKey::of(result) != key { return Err(invalid()); }
    Ok(())
}
