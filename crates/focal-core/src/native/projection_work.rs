//! Complete owner work membership without a ledger scan or an allocated cohort.
//! Each cycle's links are first checked for exact finite termination, then read
//! once more to yield its rows. Both walks are linear; no repeated nth lookup or
//! per-object shared ownership is involved.
use super::*;
use focal_model::lifecycle::{
    artifact_descriptor::{WorkProvenance, WorkRole},
    claim::ResponseLink,
    evidence::WorkArtifact,
};

#[derive(Clone, Copy)]
struct Cycle<'b> {
    key: NativeCycleKey,
    holder: ParticipantId,
    response: Option<&'b Response>,
    next: Option<ArtifactId>,
    remaining: usize,
}

struct Works<'a, 'b> {
    view: &'a View<'b>,
    claim: Option<&'b ClaimState>,
    limits: NativeLimits,
    visits: usize,
    open: Option<(NativeCycleKey, ParticipantId)>,
    next_response: Option<TestamentId>,
    remaining_responses: u32,
    latest: Option<ResponseLink>,
    cycle: Option<Cycle<'b>>,
    error: Option<ContractError>,
    done: bool,
}

pub(super) fn works<'a, 'b>(
    view: &'a View<'b>,
    claim: ClaimId,
    limits: NativeLimits,
) -> impl Iterator<Item = Result<&'b WorkArtifact, ContractError>> + 'a {
    let mut cursor = Works {
        view,
        claim: None,
        limits,
        visits: limits.plan_edges,
        open: None,
        next_response: None,
        remaining_responses: 0,
        latest: None,
        cycle: None,
        error: None,
        done: false,
    };
    if let Err(error) = cursor.initialize(claim) {
        cursor.error = Some(error);
    }
    cursor
}

impl<'a, 'b> Works<'a, 'b> {
    fn initialize(&mut self, id: ClaimId) -> Result<(), ContractError> {
        self.charge(1)?;
        let claim = as_claim(self.view.get(Key::Claim(id))).ok_or(ContractError::WrongObject)?;
        if claim.binding().ledger != self.view.ledger() || claim.binding().object.0 != id.0 {
            return Err(ContractError::InvalidTarget);
        }
        let count = u32::try_from(claim.response_count()).map_err(|_| ContractError::Capacity)?;
        if count > claim.max_responses()
            || usize::try_from(count).map_err(|_| ContractError::Capacity)? > self.visits
            || (count != 0) != claim.latest_response().is_some()
        {
            return Err(ContractError::InvalidManifest);
        }
        self.claim = Some(claim);
        self.remaining_responses = count;
        self.latest = claim.latest_response();
        self.next_response = self.latest.map(|row| row.testament);
        match claim.receipt() {
            Some(receipt) => {
                if receipt.fence.receipt.is_zero()
                    || receipt.fence.epoch == 0
                    || receipt.holder.is_zero()
                {
                    return Err(ContractError::StaleReceipt);
                }
                // At the authored maximum there is no open cycle to visit. In
                // particular, a maximal u32 response count must not be incremented.
                if count < claim.max_responses() {
                    self.open = Some((
                        NativeCycleKey {
                            claim: id,
                            receipt: receipt.fence.receipt,
                            epoch: receipt.fence.epoch,
                            cycle: count.checked_add(1).ok_or(ContractError::Capacity)?,
                        },
                        receipt.holder,
                    ));
                }
            }
            None if count == 0 => {}
            None => return Err(ContractError::StaleReceipt),
        }
        Ok(())
    }

    fn charge(&mut self, count: usize) -> Result<(), ContractError> {
        self.visits = self
            .visits
            .checked_sub(count)
            .ok_or(ContractError::Capacity)?;
        Ok(())
    }

    fn load_cycle(
        &mut self,
        key: NativeCycleKey,
        holder: ParticipantId,
        response: Option<&'b Response>,
    ) -> Result<Option<Cycle<'b>>, ContractError> {
        self.charge(1)?;
        let row = match self.view.get(Key::Cycle(key)) {
            None if response.is_none() => return Ok(None),
            Some(Row::Cycle(row)) => row,
            _ => return Err(ContractError::InvalidManifest),
        };
        if row.response != response.map(|row| TestamentId(row.identity().binding.object.0))
            || row.work_head.is_some() != (row.work_count != 0)
            || row.work_count > self.limits.work_artifacts_per_cycle
            || row.diagnostic_head.is_some() != (row.diagnostic_count != 0)
            || row.diagnostic_count > self.limits.diagnostics_per_cycle
        {
            return Err(ContractError::InvalidManifest);
        }
        if let Some(response) = response
            && response
                .manifest()
                .len()
                .checked_add(response.failed_work().len())
                != Some(row.work_count)
        {
            return Err(ContractError::InvalidManifest);
        }
        self.charge(row.work_count)?;
        // Exact termination proves no repeated ID can occur in this immutable
        // singly linked chain. Reject malformed chains before yielding any row.
        let mut next = row.work_head;
        for _ in 0..row.work_count {
            let id = next.ok_or(ContractError::InvalidManifest)?;
            let work =
                as_work(self.view.get(Key::Work(id))).ok_or(ContractError::MissingEvidence)?;
            if work.state.reference().id != id {
                return Err(ContractError::InvalidManifest);
            }
            next = work.next;
        }
        if next.is_some() {
            return Err(ContractError::InvalidManifest);
        }
        Ok(Some(Cycle {
            key,
            holder,
            response,
            next: row.work_head,
            remaining: row.work_count,
        }))
    }

    fn next_cycle(&mut self) -> Result<Option<Cycle<'b>>, ContractError> {
        if let Some((key, holder)) = self.open.take() {
            return self.load_cycle(key, holder, None);
        }
        if self.remaining_responses == 0 {
            if self.next_response.is_some() {
                return Err(ContractError::InvalidManifest);
            }
            self.done = true;
            return Ok(None);
        }
        let id = self.next_response.ok_or(ContractError::InvalidManifest)?;
        self.charge(1)?;
        let response =
            as_response(self.view.get(Key::Response(id))).ok_or(ContractError::MissingEvidence)?;
        let identity = response.identity();
        let claim = self.claim.ok_or(ContractError::WrongObject)?;
        if identity.binding.ledger != claim.binding().ledger
            || identity.binding.object.0 != id.0
            || identity.binding.content == ContentHash([0; 32])
            || identity.binding.revision.0 == 0
            || identity.claim.0 != claim.binding().object.0
            || identity.cycle != self.remaining_responses
            || identity.receipt.receipt.is_zero()
            || identity.receipt.epoch == 0
            || response.respondent().is_zero()
            || (identity.cycle != 1) != identity.prior.is_some()
        {
            return Err(ContractError::InvalidManifest);
        }
        if let Some(latest) = self.latest.take()
            && latest
                != (ResponseLink {
                    testament: id,
                    content: identity.binding.content,
                    receipt: identity.receipt,
                    cycle: identity.cycle,
                    prior: identity.prior,
                })
        {
            return Err(ContractError::InvalidManifest);
        }
        self.remaining_responses = self
            .remaining_responses
            .checked_sub(1)
            .ok_or(ContractError::InvalidManifest)?;
        self.next_response = identity.prior;
        self.load_cycle(
            NativeCycleKey {
                claim: identity.claim,
                receipt: identity.receipt.receipt,
                epoch: identity.receipt.epoch,
                cycle: identity.cycle,
            },
            response.respondent(),
            Some(response),
        )
    }

    fn check_work(
        &mut self,
        cycle: Cycle<'b>,
        id: ArtifactId,
    ) -> Result<&'b NativeWork, ContractError> {
        // Work, slot index, source descriptor/identity, and at most a failed
        // work's diagnostic membership plus diagnostic descriptor/identity.
        // Charge the maximum seven lookups even on the ordinary-output path.
        self.charge(7)?;
        let claim = self.claim.ok_or(ContractError::WrongObject)?;
        let row = as_work(self.view.get(Key::Work(id))).ok_or(ContractError::MissingEvidence)?;
        let work = &row.state;
        let receipt = ReceiptFence {
            receipt: cycle.key.receipt,
            epoch: cycle.key.epoch,
        };
        if work.reference().id != id
            || work.claim() != cycle.key.claim
            || work.binding().ledger != claim.binding().ledger
            || work.binding().revision.0 == 0
            || work.receipt() != receipt
            || work.cycle() != cycle.key.cycle
            || work.producer() != cycle.holder
            || !claim.acceptance().has_slot(work.slot())
            || !matches!(self.view.get(Key::WorkSlot(cycle.key,work.slot())),Some(Row::WorkSlot(found)) if *found == id)
        {
            return Err(ContractError::InvalidManifest);
        }
        let source =
            as_artifact(self.view.get(Key::Artifact(id))).ok_or(ContractError::MissingEvidence)?;
        let descriptor = source.descriptor();
        let generation_failed = work.state() == WorkArtifactState::GenerationFailed;
        let role = if generation_failed {
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Production,
            }
        } else {
            WorkRole::Output { slot: work.slot() }
        };
        if descriptor.id() != id
            || descriptor.content_hash() != work.reference().hash
            || descriptor.ledger() != claim.binding().ledger
            || descriptor.producer() != cycle.holder
            || descriptor.receipt() != Some(receipt)
            || descriptor.result_provenance().is_some()
            || descriptor.work_provenance()
                != Some(WorkProvenance {
                    claim: cycle.key.claim,
                    cycle: cycle.key.cycle,
                    role,
                })
            || !matches!(self.view.get(Key::ArtifactIdentity(descriptor.content_hash())),Some(Row::ArtifactIdentity(found)) if *found == id)
        {
            return Err(ContractError::MissingEvidence);
        }
        self.check_failure(work, claim.issuer())?;
        match cycle.response {
            None => {
                if work.attachment().is_some()
                    || !matches!(
                        work.state(),
                        WorkArtifactState::Generated
                            | WorkArtifactState::Received
                            | WorkArtifactState::ReceiptFailed
                            | WorkArtifactState::GenerationFailed
                    )
                {
                    return Err(ContractError::InvalidManifest);
                }
            }
            Some(response)
                if matches!(
                    work.state(),
                    WorkArtifactState::ReceiptFailed | WorkArtifactState::GenerationFailed
                ) =>
            {
                let failure = response
                    .failed_work()
                    .binary_search_by_key(&work.slot(), |row| row.slot())
                    .ok()
                    .and_then(|index| response.failed_work().get(index))
                    .ok_or(ContractError::InvalidManifest)?;
                if work.attachment().is_some()
                    || failure.binding() != work.binding()
                    || failure.state() != work.state()
                    || Some(failure.diagnostic()) != work.diagnostic()
                {
                    return Err(ContractError::InvalidManifest);
                }
            }
            Some(response) => {
                let entry = response
                    .manifest()
                    .binary_search_by_key(&work.slot(), |row| row.slot)
                    .ok()
                    .and_then(|index| response.manifest().get(index))
                    .ok_or(ContractError::InvalidManifest)?;
                if entry.artifact != work.reference()
                    || work.attachment() != Some(TestamentId(response.identity().binding.object.0))
                    || !matches!(
                        work.state(),
                        WorkArtifactState::Attached
                            | WorkArtifactState::Validating
                            | WorkArtifactState::Validated
                            | WorkArtifactState::ValidationFailed
                    )
                {
                    return Err(ContractError::InvalidManifest);
                }
            }
        }
        Ok(row)
    }

    fn check_failure(
        &self,
        work: &WorkArtifact,
        issuer: ParticipantId,
    ) -> Result<(), ContractError> {
        let failed = matches!(
            work.state(),
            WorkArtifactState::GenerationFailed | WorkArtifactState::ReceiptFailed
        );
        let Some(failure) = work.diagnostic() else {
            return if failed {
                Err(ContractError::MissingEvidence)
            } else {
                Ok(())
            };
        };
        let (producer, role) = match (work.state(), failure.reason) {
            (WorkArtifactState::GenerationFailed, EvidenceFailure::Production) => {
                let diagnostic = as_diagnostic(self.view.get(Key::Diagnostic(failure.artifact.id)))
                    .ok_or(ContractError::MissingEvidence)?;
                if diagnostic.diagnostic.claim() != work.claim()
                    || diagnostic.diagnostic.cycle() != work.cycle()
                    || diagnostic.diagnostic.receipt() != work.receipt()
                    || diagnostic.diagnostic.producer() != work.producer()
                    || diagnostic.diagnostic.artifact() != work.reference()
                    || diagnostic.diagnostic.diagnostic() != failure
                    || failure.artifact != work.reference()
                {
                    return Err(ContractError::MissingEvidence);
                }
                (
                    work.producer(),
                    WorkRole::Diagnostic {
                        reason: EvidenceFailure::Production,
                    },
                )
            }
            (
                WorkArtifactState::ReceiptFailed,
                EvidenceFailure::Structure | EvidenceFailure::Metadata,
            ) => (
                issuer,
                WorkRole::ReceiptRejection {
                    artifact: work.reference(),
                    reason: failure.reason,
                },
            ),
            _ => return Err(ContractError::InvalidManifest),
        };
        let artifact = as_artifact(self.view.get(Key::Artifact(failure.artifact.id)))
            .ok_or(ContractError::MissingEvidence)?;
        let descriptor = artifact.descriptor();
        if descriptor.id() != failure.artifact.id
            || descriptor.content_hash() != failure.artifact.hash
            || descriptor.ledger() != work.binding().ledger
            || descriptor.receipt() != Some(work.receipt())
            || descriptor.producer() != producer
            || descriptor.kind() != "error"
            || descriptor.result_provenance().is_some()
            || descriptor.work_provenance()
                != Some(WorkProvenance {
                    claim: work.claim(),
                    cycle: work.cycle(),
                    role,
                })
            || !matches!(self.view.get(Key::ArtifactIdentity(descriptor.content_hash())),Some(Row::ArtifactIdentity(found)) if *found == failure.artifact.id)
        {
            return Err(ContractError::MissingEvidence);
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<Option<&'b WorkArtifact>, ContractError> {
        loop {
            if let Some(mut cycle) = self.cycle.take()
                && cycle.remaining != 0
            {
                let id = cycle.next.ok_or(ContractError::InvalidManifest)?;
                let row = self.check_work(cycle, id)?;
                cycle.remaining = cycle
                    .remaining
                    .checked_sub(1)
                    .ok_or(ContractError::InvalidManifest)?;
                cycle.next = row.next;
                if cycle.next.is_some() != (cycle.remaining != 0) {
                    return Err(ContractError::InvalidManifest);
                }
                self.cycle = Some(cycle);
                return Ok(Some(&row.state));
            }
            if self.done {
                return Ok(None);
            }
            self.cycle = self.next_cycle()?;
        }
    }
}

impl<'b> Iterator for Works<'_, 'b> {
    type Item = Result<&'b WorkArtifact, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            self.done = true;
            return Some(Err(error));
        }
        if self.done {
            return None;
        }
        match self.advance() {
            Ok(Some(work)) => Some(Ok(work)),
            Ok(None) => None,
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}
