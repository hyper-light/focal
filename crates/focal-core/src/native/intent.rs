use super::prepare::{ALLOCATION, add, array, within};
use super::*;
use focal_model::{ObjectRevision, lifecycle::creation};

fn hash_binding(hash: &mut blake3::Hasher, binding: Binding) {
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    hash.update(&binding.revision.0.to_le_bytes());
}
pub(super) fn fingerprint(
    ledger: LedgerId,
    input: &NativeInput,
) -> Result<ContentHash, NativeError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/request-intent/1");
    hash.update(&ledger.tenant.0);
    hash.update(&ledger.session.0);
    hash.update(&input.request.principal.0);
    hash.update(&input.request.epoch.0.to_le_bytes());
    hash.update(&input.request.id.0);
    match &input.command {
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
            hash.update(&[13]);
            hash_binding(&mut hash, *claim);
            hash_binding(&mut hash, *expected);
            hash.update(&[match reason {
                EvidenceFailure::Work => 0,
                EvidenceFailure::Production => 1,
                EvidenceFailure::Structure => 2,
                EvidenceFailure::Metadata => 3,
            }]);
            hash.update(
                &artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint()
                    .0,
            );
        }
        NativeCommand::SubmitWork {
            claim,
            slot,
            artifact,
        } => {
            hash.update(&[6]);
            hash_binding(&mut hash, *claim);
            hash.update(&slot.to_le_bytes());
            hash.update(
                &artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint()
                    .0,
            );
        }
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        } => {
            hash.update(&[7]);
            hash_binding(&mut hash, *claim);
            hash.update(&[match reason {
                EvidenceFailure::Work => 0,
                EvidenceFailure::Production => 1,
                EvidenceFailure::Structure => 2,
                EvidenceFailure::Metadata => 3,
            }]);
            hash.update(
                &artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint()
                    .0,
            );
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
        | NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact,
        } => {
            if matches!(&input.command, NativeCommand::ReportIncrement { .. }) {
                let EvaluationTarget::Increment { artifact } = key.target else {
                    return Err(ContractError::InvalidTarget.into());
                };
                hash.update(&[15]);
                hash.update(&artifact.0);
            } else {
                if key.target != EvaluationTarget::Admission {
                    return Err(ContractError::InvalidTarget.into());
                }
                hash.update(&[4]);
            }
            hash_binding(&mut hash, *claim);
            hash.update(&key.claim.0);
            hash.update(&key.validation.0);
            hash.update(&key.generation.to_le_bytes());
            hash_binding(&mut hash, *expected);
            hash.update(&report.generation.to_le_bytes());
            hash.update(&[match report.attempt.phase {
                validation::Phase::Programmatic => 0,
                validation::Phase::Quality => 1,
                validation::Phase::Delivery => 2,
                validation::Phase::MissingTarget => 3,
            }]);
            hash.update(&report.attempt.index.to_le_bytes());
            hash.update(&report.attempt.handler.0);
            hash.update(&report.attempt.version.0);
            hash.update(&report.attempt.evaluator.0);
            hash.update(&report.attempt.definition.0);
            hash.update(&[match report.value {
                focal_model::VerdictValue::Pass => 0,
                focal_model::VerdictValue::Fail => 1,
                focal_model::VerdictValue::Incomplete => 2,
                focal_model::VerdictValue::Error => 3,
            }]);
            hash.update(&report.evidence.id.0);
            hash.update(&report.evidence.hash.0);
            hash.update(
                &artifact
                    .get()
                    .ok_or(ContractError::MissingEvidence)?
                    .intent_fingerprint()
                    .0,
            );
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
        | NativeCommand::BeginIncrement {
            claim,
            key,
            expected,
        } => {
            if matches!(&input.command, NativeCommand::BeginIncrement { .. }) {
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
    if let NativeCommand::ReportAdmission { artifact, .. }
    | NativeCommand::ReportIncrement { artifact, .. }
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
