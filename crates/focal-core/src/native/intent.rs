use super::prepare::{ALLOCATION, add, array, within};
use super::*;
use focal_model::{ObjectRevision, lifecycle::creation};

pub(super) fn hash_binding(hash: &mut blake3::Hasher, binding: Binding) {
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&binding.revision.0.to_le_bytes());
}

pub(super) fn hash_optional_receipt(hash: &mut blake3::Hasher, receipt: Option<ReceiptFence>) {
    match receipt {
        None => {
            hash.update(&[0]);
        }
        Some(receipt) => {
            hash.update(&[1]);
            hash.update(&receipt.receipt.0);
            hash.update(&receipt.epoch.to_le_bytes());
        }
    }
}

pub(super) fn monitor_deadline_fingerprint(
    ledger: LedgerId,
    input: NativeMonitorDeadlineInput,
) -> Result<ContentHash, NativeError> {
    if input.claim.is_zero()
        || input.monitor.is_zero()
        || input.deadline.timer.is_zero()
        || input.deadline.generation == 0
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/monitor-deadline-intent/1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&input.claim.0);
    hash.update(&input.monitor.0);
    hash.update(&input.deadline.timer.0);
    hash.update(&input.deadline.generation.to_le_bytes());
    hash.update(&input.deadline.at.to_le_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

/// Claim timers have a separate namespace from both actor requests and exact
/// evaluation deadlines. Firing time is deliberately absent from retry identity.
pub(super) fn claim_deadline_fingerprint(
    ledger: LedgerId,
    input: NativeClaimDeadlineInput,
) -> Result<ContentHash, NativeError> {
    if input.claim.is_zero() || input.deadline.timer.is_zero() || input.deadline.generation == 0 {
        return Err(ContractError::InvalidTarget.into());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/claim-deadline-intent/1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&input.claim.0);
    hash.update(&input.deadline.timer.0);
    hash.update(&input.deadline.generation.to_le_bytes());
    hash.update(&input.deadline.at.to_le_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

/// Timer identity is native-only and disjoint from the unchanged actor intent
/// preimage. Actual firing time is an owner observation, never retry identity.
pub(super) fn deadline_fingerprint(
    ledger: LedgerId,
    input: NativeDeadlineInput,
) -> Result<ContentHash, NativeError> {
    let key = input.evaluation;
    if key.claim.is_zero()
        || key.validation.is_zero()
        || key.generation == 0
        || input.deadline.timer.is_zero()
        || input.deadline.generation == 0
    {
        return Err(ContractError::InvalidTarget.into());
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/evaluation-deadline-intent/1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&key.claim.0);
    hash.update(&key.validation.0);
    hash.update(&key.generation.to_le_bytes());
    match key.target {
        EvaluationTarget::Admission => {
            hash.update(&[0]);
        }
        EvaluationTarget::Increment { artifact } => {
            if artifact.is_zero() {
                return Err(ContractError::InvalidTarget.into());
            }
            hash.update(&[1]);
            hash.update(&artifact.0);
        }
        EvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => {
            if response.is_zero() || artifact.is_zero() {
                return Err(ContractError::InvalidTarget.into());
            }
            hash.update(&[2]);
            hash.update(&response.0);
            hash.update(&slot.to_le_bytes());
            hash.update(&artifact.0);
        }
        EvaluationTarget::MissingSlot { response, slot } => {
            if response.is_zero() {
                return Err(ContractError::InvalidTarget.into());
            }
            hash.update(&[3]);
            hash.update(&response.0);
            hash.update(&slot.to_le_bytes());
        }
        EvaluationTarget::Delivery { response } => {
            if response.is_zero() {
                return Err(ContractError::InvalidTarget.into());
            }
            hash.update(&[4]);
            hash.update(&response.0);
        }
    }
    hash.update(&input.deadline.timer.0);
    hash.update(&input.deadline.generation.to_le_bytes());
    hash.update(&input.deadline.at.to_le_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}
pub(super) fn request_hasher(ledger: LedgerId, request: RequestKey) -> blake3::Hasher {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/request-intent/1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&request.principal.0);
    hash.update(&request.epoch.0.to_le_bytes());
    hash.update(&request.id.0);
    hash
}

/// Shared preimage for owned and decoded monitor roots. Cardinality is checked
/// with a bounded number of reads; this is identity, not registration authority.
pub(super) fn hash_monitor_registration(
    hash: &mut blake3::Hasher,
    expected: Binding,
    receipt: Option<ReceiptFence>,
    id: MonitorId,
    mut roots: impl Iterator<Item = Result<WaitPredicate, NativeError>>,
    count: usize,
    deadline: Deadline,
) -> Result<(), NativeError> {
    hash.update(&[24]);
    hash_binding(hash, expected);
    hash_optional_receipt(hash, receipt);
    hash.update(&id.0);
    hash.update(
        &u64::try_from(count)
            .map_err(|_| ContractError::Capacity)?
            .to_le_bytes(),
    );
    for _ in 0..count {
        let root = roots.next().ok_or(ContractError::InvalidManifest)??;
        let (kind, id) = match root {
            WaitPredicate::Satisfied(id) => (0u8, id),
            WaitPredicate::Terminal(id) => (1u8, id),
            WaitPredicate::Released(id) => (2u8, id),
        };
        hash.update(&[kind]);
        hash.update(&id.0);
    }
    if roots.next().is_some() {
        return Err(ContractError::InvalidManifest.into());
    }
    hash.update(&deadline.timer.0);
    hash.update(&deadline.generation.to_le_bytes());
    hash.update(&deadline.at.to_le_bytes());
    Ok(())
}

pub(super) fn fingerprint(
    ledger: LedgerId,
    input: &NativeInput,
) -> Result<ContentHash, NativeError> {
    let mut hash = request_hasher(ledger, input.request);
    match &input.command {
        NativeCommand::CreateAuthored { claims } => {
            hash.update(&[27]);
            hash.update(&super::authored::fingerprint(claims)?.0);
        }
        NativeCommand::RegisterMonitor {
            expected,
            receipt,
            id,
            roots,
            deadline,
        } => {
            hash_monitor_registration(
                &mut hash,
                *expected,
                *receipt,
                *id,
                roots.iter().copied().map(Ok),
                roots.len(),
                *deadline,
            )?;
        }
        NativeCommand::RebindMonitor {
            expected,
            receipt,
            id,
            predecessor,
            successor,
        } => {
            hash.update(&[25]);
            hash_binding(&mut hash, *expected);
            hash_optional_receipt(&mut hash, *receipt);
            hash.update(&id.0);
            hash_binding(&mut hash, *predecessor);
            hash_binding(&mut hash, *successor);
        }
        NativeCommand::CancelMonitor {
            expected,
            receipt,
            id,
        } => {
            hash.update(&[26]);
            hash_binding(&mut hash, *expected);
            hash_optional_receipt(&mut hash, *receipt);
            hash.update(&id.0);
        }
        NativeCommand::ReleaseScope { expected } => {
            hash.update(&[23]);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::GenerateResultTestament { claim, id } => {
            hash.update(&[20]);
            hash_binding(&mut hash, *claim);
            hash.update(&id.0);
        }
        NativeCommand::PostResultTestament { expected } => {
            hash.update(&[21]);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::EnterWholeWork { claim, expected } => {
            hash.update(&[17]);
            hash_binding(&mut hash, *claim);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::SealIncrementTargets { claim } => {
            hash.update(&[16]);
            hash_binding(&mut hash, *claim);
        }
        NativeCommand::FailWorkProduction {
            claim,
            slot,
            diagnostic,
        } => {
            hash.update(&[12]);
            hash_binding(&mut hash, *claim);
            hash.update(&slot.to_le_bytes());
            hash.update(&diagnostic.id.0);
            hash.update(&diagnostic.hash.0);
        }
        NativeCommand::RejectWork {
            claim,
            expected,
            reason,
            artifact,
        } => {
            super::artifact_intent::ArtifactCommand::RejectWork {
                claim: *claim,
                expected: *expected,
                reason: *reason,
            }
            .hash_into(
                &mut hash,
                artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint(),
            )?;
        }
        NativeCommand::SubmitWork {
            claim,
            slot,
            artifact,
        } => {
            super::artifact_intent::ArtifactCommand::SubmitWork {
                claim: *claim,
                slot: *slot,
            }
            .hash_into(
                &mut hash,
                artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint(),
            )?;
        }
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        } => {
            super::artifact_intent::ArtifactCommand::SubmitDiagnostic {
                claim: *claim,
                reason: *reason,
            }
            .hash_into(
                &mut hash,
                artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint(),
            )?;
        }
        NativeCommand::ReceiveWork { claim, expected }
        | NativeCommand::PostResponse { claim, expected }
        | NativeCommand::ReceiveResponse { claim, expected } => {
            hash.update(&[match &input.command {
                NativeCommand::ReceiveWork { .. } => 8,
                NativeCommand::PostResponse { .. } => 10,
                _ => 11,
            }]);
            hash_binding(&mut hash, *claim);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::CloseResponse {
            claim,
            response,
            report,
        } => {
            hash.update(&[9]);
            hash_binding(&mut hash, *claim);
            hash_binding(&mut hash, *response);
            report.hash_into(&mut hash)?;
        }
        NativeCommand::AcquireReceipt { expected, receipt } => {
            hash.update(&[5]);
            hash_binding(&mut hash, *expected);
            hash.update(&receipt.0);
        }
        NativeCommand::AdoptReceipt {
            expected,
            previous,
            receipt,
            holder,
        } => {
            hash.update(&[22]);
            hash_binding(&mut hash, *expected);
            hash.update(&previous.receipt.0);
            hash.update(&previous.epoch.to_le_bytes());
            hash.update(&receipt.0);
            hash.update(&holder.0);
        }
        NativeCommand::Create {
            claims: proposals,
            declarations,
        } => {
            hash.update(&[0]);
            hash.update(&creation::intent_fingerprint(proposals)?.0);
            hash.update(
                &u64::try_from(declarations.len())
                    .map_err(|_| NativeError::Capacity("definitions"))?
                    .to_le_bytes(),
            );
            for declaration in declarations {
                hash.update(&declaration.intent_fingerprint().0);
            }
        }
        NativeCommand::ReportAdmission {
            claim,
            key,
            expected,
            report,
            artifact,
        }
        | NativeCommand::ReportWork {
            claim,
            key,
            expected,
            report,
            artifact,
        }
        | NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact,
        } => {
            let kind = if matches!(&input.command, NativeCommand::ReportWork { .. }) {
                super::artifact_intent::ReportKind::Work
            } else if matches!(&input.command, NativeCommand::ReportIncrement { .. }) {
                super::artifact_intent::ReportKind::Increment
            } else {
                super::artifact_intent::ReportKind::Admission
            };
            super::artifact_intent::ArtifactCommand::Report {
                kind,
                claim: *claim,
                key: *key,
                expected: *expected,
                report: *report,
            }
            .hash_into(
                &mut hash,
                artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint(),
            )?;
        }
        NativeCommand::Cancel { expected } => {
            hash.update(&[1]);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::Post { expected } => {
            hash.update(&[2]);
            hash_binding(&mut hash, *expected);
        }
        NativeCommand::BeginAdmission {
            claim,
            key,
            expected,
        }
        | NativeCommand::BeginWork {
            claim,
            key,
            expected,
        }
        | NativeCommand::BeginIncrement {
            claim,
            key,
            expected,
        } => {
            if matches!(&input.command, NativeCommand::BeginWork { .. }) {
                let EvaluationTarget::Work {
                    response,
                    slot,
                    artifact,
                } = key.target
                else {
                    return Err(ContractError::InvalidTarget.into());
                };
                hash.update(&[18]);
                hash.update(&response.0);
                hash.update(&slot.to_le_bytes());
                hash.update(&artifact.0);
            } else if matches!(&input.command, NativeCommand::BeginIncrement { .. }) {
                let EvaluationTarget::Increment { artifact } = key.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                hash.update(&[14]);
                hash.update(&artifact.0);
            } else {
                if key.target != EvaluationTarget::Admission {
                    return Err(ContractError::InvalidTarget.into());
                }
                hash.update(&[3]);
            }
            hash_binding(&mut hash, *claim);
            hash.update(&key.claim.0);
            hash.update(&key.validation.0);
            hash.update(&key.generation.to_le_bytes());
            hash_binding(&mut hash, *expected);
        }
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

pub(super) fn bound_input(
    command: &NativeCommand,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    if let NativeCommand::CreateAuthored { claims } = command {
        super::authored::bound_input(claims, claims.capacity(), limits)?;
    }
    if let NativeCommand::RegisterMonitor { roots, .. } = command {
        within(roots.len(), limits.plan_edges)?;
        within(
            array::<WaitPredicate>(roots.capacity())?,
            limits.preparation_bytes,
        )?;
    }
    if let NativeCommand::ReportAdmission { artifact, .. }
    | NativeCommand::ReportIncrement { artifact, .. }
    | NativeCommand::ReportWork { artifact, .. }
    | NativeCommand::RejectWork { artifact, .. }
    | NativeCommand::SubmitWork { artifact, .. }
    | NativeCommand::SubmitDiagnostic { artifact, .. } = command
    {
        within(artifact.heap_charge()?, limits.preparation_bytes)?;
    }
    if let NativeCommand::CloseResponse { report, .. } = command {
        report.check_limits(limits)?;
    }
    if let NativeCommand::Create {
        claims: proposals,
        declarations,
    } = command
    {
        if proposals.is_empty() || proposals.len() > limits.plan_nodes {
            return Err(NativeError::Capacity("creation proposals"));
        }
        let mut bytes = array::<Proposal>(proposals.capacity())?;
        if declarations.len() > limits.range.max_batch_entries / 2 {
            return Err(NativeError::Capacity("definition batch"));
        }
        bytes = add(
            bytes,
            array::<validation::Declaration>(declarations.capacity())?,
        )?;
        for declaration in declarations {
            bytes = add(bytes, declaration.retained_heap_bytes()?)?;
            bytes = add(
                bytes,
                declaration
                    .heap_allocations()?
                    .checked_mul(ALLOCATION)
                    .ok_or(NativeError::Capacity("definition input"))?,
            )?;
        }
        for proposal in proposals {
            let definition = &proposal.definition;
            if definition.binding.revision != ObjectRevision(1) {
                return Err(ContractError::StaleRevision.into());
            }
            definition.lineage.check_binding(&definition.binding)?;
            for (heap, allocations) in [
                (
                    definition.graph.retained_heap_bytes()?,
                    definition.graph.heap_allocations()?,
                ),
                (
                    definition.lineage.retained_heap_bytes()?,
                    definition.lineage.heap_allocations()?,
                ),
                (
                    definition.acceptance.retained_heap_bytes()?,
                    definition.acceptance.heap_allocations()?,
                ),
            ] {
                bytes = add(bytes, heap)?;
                bytes = add(
                    bytes,
                    allocations
                        .checked_mul(ALLOCATION)
                        .ok_or(NativeError::Capacity("input charge"))?,
                )?;
            }
            within(bytes, limits.preparation_bytes)?;
        }
    }
    Ok(())
}
