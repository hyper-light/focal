//! Respondent-authored closure and separate response posting/receipt. The owner
//! discovers cycle membership from its own indexes before checking the manifest.
use super::prepare::{ALLOCATION, Extra, Extras, Scratch, add, array, heap, within};
use super::*;
use evidence::{
    CloseReport, Parent, ResponseDiagnostic, ResponseIdentity, ResponseLimits, SlotBinding,
    WorkArtifact,
};
use focal_model::lifecycle::aggregation;
use focal_model::{ArtifactRef, Confidence, ObjectRevision, OutcomeKind};

/// Typed authored report. There is no default outcome or implicit success.
#[derive(Debug)]
pub struct NativeResponseInput {
    pub summary: String,
    pub confidence: Confidence,
    pub outcome: OutcomeKind,
    pub manifest: Vec<SlotBinding>,
    pub diagnostics: Vec<ArtifactRef>,
}
impl NativeResponseInput {
    pub(super) fn heap_charge(&self) -> Result<usize, NativeError> {
        add(
            array::<u8>(self.summary.capacity())?,
            add(
                array::<SlotBinding>(self.manifest.capacity())?,
                array::<ArtifactRef>(self.diagnostics.capacity())?,
            )?,
        )
    }
    pub(super) fn check_limits(&self, limits: NativeLimits) -> Result<(), NativeError> {
        if self.summary.len() > limits.response_summary_bytes
            || self.manifest.len() > super::response_budget::work_limit(limits)?
            || self.diagnostics.len() > limits.diagnostics_per_cycle
        {
            return Err(NativeError::Capacity("authored response"));
        }
        within(self.heap_charge()?, limits.preparation_bytes)
    }
    pub(super) fn hash_into(&self, hash: &mut blake3::Hasher) -> Result<(), NativeError> {
        fn count(hash: &mut blake3::Hasher, n: usize) -> Result<(), NativeError> {
            hash.update(
                &u64::try_from(n)
                    .map_err(|_| NativeError::Capacity("response intent"))?
                    .to_le_bytes(),
            );
            Ok(())
        }
        count(hash, self.summary.len())?;
        hash.update(self.summary.as_bytes());
        hash.update(&[match self.confidence {
            Confidence::Hint => 0,
            Confidence::Tentative => 1,
            Confidence::Committed => 2,
            Confidence::Consensus => 3,
        }]);
        hash.update(&[match self.outcome {
            OutcomeKind::Complete => 0,
            OutcomeKind::Partial => 1,
            OutcomeKind::Refused => 2,
            OutcomeKind::Impossible => 3,
            OutcomeKind::Interrupted => 4,
            OutcomeKind::Failed => 5,
        }]);
        count(hash, self.manifest.len())?;
        for slot in &self.manifest {
            hash.update(&slot.slot.to_le_bytes());
            hash.update(&slot.artifact.id.0);
            hash.update(&slot.artifact.hash.0);
        }
        count(hash, self.diagnostics.len())?;
        for diagnostic in &self.diagnostics {
            hash.update(&diagnostic.id.0);
            hash.update(&diagnostic.hash.0);
        }
        Ok(())
    }
}

fn cycle(view: &View<'_>, key: NativeCycleKey) -> Result<NativeCycle, NativeError> {
    match view.get(Key::Cycle(key)) {
        None => Ok(NativeCycle::default()),
        Some(Row::Cycle(value)) if value.response.is_none() => Ok(*value),
        _ => Err(ContractError::InvalidManifest.into()),
    }
}

fn gather_work(
    view: &View<'_>,
    parent: &Parent,
    cycle: NativeCycle,
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<Vec<WorkArtifact>, NativeError> {
    if cycle.work_count > super::response_budget::work_limit(limits)? {
        return Err(NativeError::Capacity("cycle work membership"));
    }
    let mut rows = scratch.reserve::<WorkArtifact>(cycle.work_count)?;
    let mut next = cycle.work_head;
    for _ in 0..cycle.work_count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let row = as_work(view.get(Key::Work(id))).ok_or(ContractError::MissingEvidence)?;
        let state = row.state;
        if state.reference().id != id
            || state.claim() != parent.claim
            || state.binding().ledger != parent.ledger
            || state.receipt() != parent.receipt
            || state.cycle() != parent.next_cycle
            || state.producer() != parent.holder
            || !matches!(view.get(Key::WorkSlot(NativeCycleKey::of(parent), state.slot())), Some(Row::WorkSlot(found)) if *found == id)
        {
            return Err(ContractError::InvalidManifest.into());
        }
        // The complete set includes failures. The model freezes those separately
        // and attaches only real Generated/Received outputs.
        if !matches!(
            state.state(),
            WorkArtifactState::Generated
                | WorkArtifactState::Received
                | WorkArtifactState::GenerationFailed
                | WorkArtifactState::ReceiptFailed
        ) {
            return Err(ContractError::InvalidTransition.into());
        }
        if let Some(failure) = state.diagnostic() {
            let source = as_artifact(view.get(Key::Artifact(failure.artifact.id)))
                .ok_or(ContractError::MissingEvidence)?;
            let descriptor = source.descriptor();
            if descriptor.binding().ledger != parent.ledger
                || descriptor.content_hash() != failure.artifact.hash
                || descriptor.receipt() != Some(state.receipt())
                || descriptor.kind() != "error"
            {
                return Err(ContractError::MissingEvidence.into());
            }
            use focal_model::lifecycle::artifact_descriptor::{WorkProvenance, WorkRole};
            let (producer, role) = match state.state() {
                WorkArtifactState::GenerationFailed
                    if failure.reason == EvidenceFailure::Production =>
                {
                    (
                        parent.holder,
                        WorkRole::Diagnostic {
                            reason: EvidenceFailure::Production,
                        },
                    )
                }
                WorkArtifactState::ReceiptFailed
                    if matches!(
                        failure.reason,
                        EvidenceFailure::Structure | EvidenceFailure::Metadata
                    ) =>
                {
                    (
                        parent.issuer,
                        WorkRole::ReceiptRejection {
                            artifact: state.reference(),
                            reason: failure.reason,
                        },
                    )
                }
                _ => return Err(ContractError::InvalidManifest.into()),
            };
            if descriptor.producer() != producer
                || descriptor.work_provenance()
                    != Some(WorkProvenance {
                        claim: parent.claim,
                        cycle: parent.next_cycle,
                        role,
                    })
            {
                return Err(ContractError::MissingEvidence.into());
            }
        } else if matches!(
            state.state(),
            WorkArtifactState::GenerationFailed | WorkArtifactState::ReceiptFailed
        ) {
            return Err(ContractError::MissingEvidence.into());
        }
        rows.push(state);
        next = row.next;
    }
    if next.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    rows.sort_unstable_by_key(WorkArtifact::slot);
    if rows.windows(2).any(|pair| match pair {
        [a, b] => a.slot() == b.slot(),
        _ => false,
    }) {
        return Err(ContractError::InvalidManifest.into());
    }
    Ok(rows)
}

fn gather_diagnostics(
    view: &View<'_>,
    parent: &Parent,
    cycle: NativeCycle,
    expected: &[ArtifactRef],
    limits: NativeLimits,
    scratch: &mut Scratch,
) -> Result<Vec<ResponseDiagnostic>, NativeError> {
    if cycle.diagnostic_count > limits.diagnostics_per_cycle
        || cycle.diagnostic_count > limits.plan_edges
        || cycle.diagnostic_count != expected.len()
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut rows = scratch.reserve::<ResponseDiagnostic>(cycle.diagnostic_count)?;
    let mut next = cycle.diagnostic_head;
    for _ in 0..cycle.diagnostic_count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let row =
            as_diagnostic(view.get(Key::Diagnostic(id))).ok_or(ContractError::MissingEvidence)?;
        let diagnostic = row.diagnostic;
        if diagnostic.artifact().id != id
            || diagnostic.claim() != parent.claim
            || diagnostic.cycle() != parent.next_cycle
            || diagnostic.receipt() != parent.receipt
            || diagnostic.producer() != parent.holder
        {
            return Err(ContractError::InvalidManifest.into());
        }
        rows.push(diagnostic);
        next = row.next;
    }
    if next.is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    rows.sort_unstable_by_key(|row| row.artifact().id);
    let mut previous = None;
    for (actual, expected) in rows.iter().zip(expected) {
        if actual.artifact() != *expected || previous.is_some_and(|id| id >= expected.id) {
            return Err(ContractError::InvalidManifest.into());
        }
        previous = Some(expected.id);
    }
    Ok(rows)
}

fn stage_response(
    row: OwnedResponse,
    before: Option<Binding>,
    extras: &mut Extras,
) -> Result<(), NativeError> {
    let response = row.get().ok_or(ContractError::MissingEvidence)?;
    let identity = response.identity();
    let fact = NativeFact::Response {
        claim: identity.claim,
        before,
        after: identity.binding,
        state: response.state(),
    };
    let heap = row.heap_charge()?;
    extras.push(Extra {
        key: Key::Response(TestamentId(identity.binding.object.0)),
        row: Row::Response(row),
        heap,
        fact: Some(fact),
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare(
    command: NativeCommand,
    context: NativeContext,
    view: &View<'_>,
    limits: NativeLimits,
    meta: &mut Meta,
    extras: &mut Extras,
    scratch: &mut Scratch,
) -> Result<transactions::Plan, NativeError> {
    let expected = match &command {
        NativeCommand::CloseResponse { claim, .. }
        | NativeCommand::PostResponse { claim, .. }
        | NativeCommand::ReceiveResponse { claim, .. } => *claim,
        _ => return Err(ContractError::InvalidTransition.into()),
    };
    let old = view
        .claim(ClaimId(expected.object.0))
        .ok_or(ContractError::InvalidTarget)?;
    old.binding().check(&expected)?;
    let parent = Parent::from_claim(old)?;
    let mut rows = scratch.reserve::<ClaimState>(1)?;
    let mut replacement_registry = None;
    match command {
        NativeCommand::CloseResponse {
            response, report, ..
        } => {
            context.principal.require_actor(parent.holder)?;
            parent.require_open_response()?;
            if response.revision != ObjectRevision(1)
                || response.content.0 == [0; 32]
                || view
                    .get(Key::Response(TestamentId(response.object.0)))
                    .is_some()
            {
                return Err(ContractError::InvalidTarget.into());
            }
            report.check_limits(limits)?;
            scratch.charge(report.heap_charge()?)?;
            transactions::increment(&mut meta.responses, 1, limits.responses, "responses")?;
            let key = NativeCycleKey::of(&parent);
            let mut current = cycle(view, key)?;
            let work = gather_work(view, &parent, current, limits, scratch)?;
            let diagnostics =
                gather_diagnostics(view, &parent, current, &report.diagnostics, limits, scratch)?;
            let plan = Response::prepare_close(
                ResponseIdentity {
                    binding: response,
                    claim: parent.claim,
                    receipt: parent.receipt,
                    cycle: parent.next_cycle,
                    prior: parent.latest_response,
                },
                &parent,
                context.principal,
                &work,
                &report.manifest,
                CloseReport {
                    summary: &report.summary,
                    confidence: report.confidence,
                    outcome: report.outcome,
                    diagnostics: &diagnostics,
                    limits: ResponseLimits {
                        artifacts: limits.work_artifacts_per_cycle,
                        diagnostics: limits.diagnostics_per_cycle,
                        summary_bytes: limits.response_summary_bytes,
                        construction_bytes: scratch.remaining()?,
                    },
                },
            )?;
            let allocation_count = plan.construction_heap_allocations()?;
            let charge = add(
                plan.construction_charge(),
                allocation_count
                    .checked_mul(ALLOCATION)
                    .ok_or(NativeError::Capacity("response construction"))?,
            )?;
            scratch.charge(charge)?;
            let closed = plan.build()?;
            let actual = add(
                closed.retained_bytes()?,
                closed
                    .heap_allocations()?
                    .checked_mul(ALLOCATION)
                    .ok_or(NativeError::Capacity("response allocations"))?,
            )?;
            within(actual, charge)?;
            let history_charge = add(
                old.copy_for_response_heap_bytes()?,
                old.copy_for_response_heap_allocations()?
                    .checked_mul(ALLOCATION)
                    .ok_or(NativeError::Capacity("response history"))?,
            )?;
            scratch.charge(history_charge)?;
            let mut changed = old.try_copy_for_response(old.copy_for_response_charge()?)?;
            changed.observe_response(&expected, context.principal, &closed.response)?;
            within(heap(&changed)?, history_charge)?;
            rows.push(changed);
            for state in closed.attachments {
                let id = state.reference().id;
                let previous =
                    as_work(view.get(Key::Work(id))).ok_or(ContractError::MissingEvidence)?;
                let fact = NativeFact::Work {
                    claim: parent.claim,
                    before: Some(previous.state.binding()),
                    after: state.binding(),
                    state: state.state(),
                };
                scratch.charge(OwnedWork::container_charge())?;
                let row = OwnedWork::new(NativeWork {
                    state,
                    next: previous.next,
                })?;
                let heap = row.heap_charge()?;
                extras.push(Extra {
                    key: Key::Work(id),
                    row: Row::Work(row),
                    heap,
                    fact: Some(fact),
                })?;
            }
            current.response = Some(TestamentId(response.object.0));
            extras.push(Extra {
                key: Key::Cycle(key),
                row: Row::Cycle(current),
                heap: 0,
                fact: None,
            })?;
            scratch.charge(OwnedResponse::container_charge())?;
            stage_response(OwnedResponse::new(closed.response)?, None, extras)?;
        }
        NativeCommand::PostResponse {
            expected: response, ..
        }
        | NativeCommand::ReceiveResponse {
            expected: response, ..
        } => {
            let posted = matches!(command, NativeCommand::PostResponse { .. });
            let Some(Row::Response(source_row)) =
                view.get(Key::Response(TestamentId(response.object.0)))
            else {
                return Err(ContractError::InvalidTarget.into());
            };
            let source = source_row.get().ok_or(ContractError::MissingEvidence)?;
            let transition = if posted {
                source.plan_post(&response, &parent, context.principal)?
            } else {
                source.plan_receive(&response, &parent, context.principal)?
            };
            scratch.charge(source_row.heap_charge()?)?;
            let next_row = source_row.transition(
                transition,
                aggregation::PublicationPosition {
                    sequence: SessionSeq(
                        view.prefix()
                            .0
                            .checked_add(1)
                            .ok_or(NativeError::Capacity("response publication sequence"))?,
                    ),
                    // Both post and receipt publish the response observation first.
                    ordinal: 0,
                },
            )?;
            let next = next_row.get().ok_or(ContractError::MissingEvidence)?;
            // A late claimant receipt is an observation of the real posted
            // response. It cannot reopen a sealed/terminal claim or rewrite its
            // acceptance witnesses, but the response keeps its own history.
            if posted || (!old.is_terminal() && !old.local_complete()) {
                scratch.charge(heap(old)?)?;
                let mut changed = old.try_copy(old.retained_bytes()?)?;
                changed.observe_response(&expected, context.principal, next)?;
                within(heap(&changed)?, heap(old)?)?;
                if !posted {
                    let delivery = super::response_budget::delivery_count(&changed, limits)?;
                    let work = super::work_checks::count(&changed, limits)?;
                    super::response_budget::check_receipt_shape(delivery, work, limits)?;
                    let count = add(delivery, work)?;
                    let source = view
                        .owned_claim(parent.claim)?
                        .registrations()
                        .ok_or(ContractError::InvalidPolicy)?;
                    source.check(&changed)?;
                    let heap = source.copy_with_additional_heap_bytes(count)?;
                    let charge = add(heap, if heap == 0 { 0 } else { ALLOCATION })?;
                    scratch.charge(charge)?;
                    let mut registry = source.try_copy_with_additional(
                        count,
                        source.copy_with_additional_charge(count)?,
                    )?;
                    within(transactions::registry_heap(&registry)?, charge)?;
                    super::delivery::prepare(
                        context,
                        view,
                        &changed,
                        next,
                        limits,
                        meta,
                        &mut registry,
                        extras,
                        scratch,
                    )?;
                    super::work_checks::prepare(
                        context,
                        view,
                        &changed,
                        next,
                        limits,
                        meta,
                        &mut registry,
                        extras,
                        scratch,
                    )?;
                    replacement_registry = Some((parent.claim, registry));
                }
                rows.push(changed);
            }
            stage_response(next_row, Some(response), extras)?;
            // Record claimant observation before its derived Receipt facts.
            // The vector is already owned and nonempty; rotating allocates nothing.
            if !posted && !extras.rows.is_empty() {
                extras.rows.rotate_right(1);
            }
        }
        _ => return Err(ContractError::InvalidTransition.into()),
    }
    Ok(transactions::Plan {
        rows,
        registry: replacement_registry,
        created: 0,
    })
}
