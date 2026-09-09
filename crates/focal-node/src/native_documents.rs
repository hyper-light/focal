//! Projections of committed native rows into the native wire documents. Every
//! document is copied out of a fixed committed prefix; nothing here decodes a
//! frame, admits a mutation or interprets a document as a satisfaction claim.
use focal_core::native::{self as core_native, NativeState};
use focal_ledger::Core;
use focal_model::lifecycle::{
    self, aggregation, claim, claim::ClaimState, evidence, graph, scope, succession, validation,
};
use focal_model::*;
use focal_wire::*;

// The row-to-document projections shared with the delta stream live in the
// core so both surfaces carry one shape; the read-side inverses stay here.
pub(crate) use core_native::event_record::{
    attempt, binding, cut, entitlement, evaluation_key, evaluation_key_of, evaluation_target,
    event_record, failure, fence, invocation, phase, response_state, result_key_of, result_ref,
    testament_state, validation_state, work_state,
};

pub(crate) fn invocation_of(value: NativeInvocationRef) -> core_native::NativeInvocation {
    match value {
        NativeInvocationRef::Request(key) => core_native::NativeInvocation::Request(key),
        NativeInvocationRef::EvaluationDeadline {
            evaluation,
            timer,
            generation,
        } => core_native::NativeInvocation::EvaluationDeadline(core_native::NativeDeadlineKey {
            evaluation: evaluation_key_of(evaluation),
            timer,
            generation,
        }),
        NativeInvocationRef::ClaimDeadline {
            claim,
            timer,
            generation,
        } => core_native::NativeInvocation::ClaimDeadline(core_native::NativeClaimDeadlineKey {
            claim,
            timer,
            generation,
        }),
        NativeInvocationRef::MonitorDeadline {
            claim,
            monitor,
            timer,
            generation,
        } => {
            core_native::NativeInvocation::MonitorDeadline(core_native::NativeMonitorDeadlineKey {
                claim,
                monitor,
                timer,
                generation,
            })
        }
        NativeInvocationRef::Import => core_native::NativeInvocation::Import,
    }
}
pub(crate) fn operation(value: core_native::NativeOperation) -> NativeOperationKind {
    use core_native::NativeOperation as O;
    match value {
        O::RegisterMonitor => NativeOperationKind::RegisterMonitor,
        O::RebindMonitor => NativeOperationKind::RebindMonitor,
        O::CancelMonitor => NativeOperationKind::CancelMonitor,
        O::MonitorDeadline => NativeOperationKind::MonitorDeadline,
        O::ReleaseScope => NativeOperationKind::ReleaseScope,
        O::GenerateResultTestament => NativeOperationKind::GenerateResultTestament,
        O::PostResultTestament => NativeOperationKind::PostResultTestament,
        O::EnterWholeWork => NativeOperationKind::EnterWholeWork,
        O::SealIncrementTargets => NativeOperationKind::SealIncrementTargets,
        O::BeginIncrement => NativeOperationKind::BeginIncrement,
        O::ReportIncrement => NativeOperationKind::ReportIncrement,
        O::FailWorkProduction => NativeOperationKind::FailWorkProduction,
        O::RejectWork => NativeOperationKind::RejectWork,
        O::SubmitWork => NativeOperationKind::SubmitWork,
        O::SubmitDiagnostic => NativeOperationKind::SubmitDiagnostic,
        O::ReceiveWork => NativeOperationKind::ReceiveWork,
        O::CloseResponse => NativeOperationKind::CloseResponse,
        O::PostResponse => NativeOperationKind::PostResponse,
        O::ReceiveResponse => NativeOperationKind::ReceiveResponse,
        O::AcquireReceipt => NativeOperationKind::AcquireReceipt,
        O::AdoptReceipt => NativeOperationKind::AdoptReceipt,
        O::Create => NativeOperationKind::Create,
        O::Cancel => NativeOperationKind::Cancel,
        O::Post => NativeOperationKind::Post,
        O::BeginAdmission => NativeOperationKind::BeginAdmission,
        O::ReportAdmission => NativeOperationKind::ReportAdmission,
        O::BeginWork => NativeOperationKind::BeginWork,
        O::ReportWork => NativeOperationKind::ReportWork,
        O::EvaluationDeadline => NativeOperationKind::EvaluationDeadline,
        O::ClaimDeadline => NativeOperationKind::ClaimDeadline,
        O::Import => NativeOperationKind::Import,
    }
}
pub(crate) fn outcome(value: core_native::NativeOutcome) -> NativeReceipt {
    NativeReceipt {
        invocation: invocation(value.invocation),
        sequence: value.sequence,
        logical_time: value.logical_time,
        operation: operation(value.operation),
        intent: value.intent,
        counts: NativeOutcomeCounts {
            created: value.created,
            changed: value.changed,
            definitions: value.definitions,
            evaluations: value.evaluations,
            artifacts: value.artifacts,
            results: value.results,
            receipts: value.receipts,
            responses: value.responses,
            result_testaments: value.result_testaments,
            events: value.events,
        },
    }
}
pub(crate) fn target(value: validation::Target) -> NativeTarget {
    match value {
        validation::Target::Artifact {
            response,
            slot,
            artifact,
        } => NativeTarget::Artifact {
            response: binding(response),
            slot,
            artifact: binding(artifact),
        },
        validation::Target::MissingSlot { response, slot } => NativeTarget::MissingSlot {
            response: binding(response),
            slot,
        },
        validation::Target::Delivery { response } => NativeTarget::Delivery {
            response: binding(response),
        },
        validation::Target::Admission { claim } => NativeTarget::Admission {
            claim: binding(claim),
        },
        validation::Target::Increment { claim, artifact } => NativeTarget::Increment {
            claim: binding(claim),
            artifact: binding(artifact),
        },
    }
}
fn suppression(value: validation::Suppression) -> NativeSuppression {
    match value {
        validation::Suppression::MissingTarget => NativeSuppression::MissingTarget,
        validation::Suppression::ParentFailure(hash) => NativeSuppression::ParentFailure(hash),
        validation::Suppression::ArtifactFailure(hash) => NativeSuppression::ArtifactFailure(hash),
        validation::Suppression::CohortSealed(hash) => NativeSuppression::CohortSealed(hash),
    }
}
fn blocking(value: aggregation::BlockingCause) -> NativeBlockingCause {
    let key = value.key();
    NativeBlockingCause {
        target: match key.target {
            aggregation::CauseTarget::Admission => NativeCauseTarget::Admission,
            aggregation::CauseTarget::Increment { artifact, content } => {
                NativeCauseTarget::Increment { artifact, content }
            }
            aggregation::CauseTarget::Response(id) => NativeCauseTarget::Response(id),
        },
        declaration_index: key.declaration_index,
        generation: key.generation,
        attempt: key.attempt,
        phase: match key.phase {
            aggregation::CausePhase::Programmatic => NativeCausePhase::Programmatic,
            aggregation::CausePhase::Quality => NativeCausePhase::Quality,
            aggregation::CausePhase::Delivery => NativeCausePhase::Delivery,
            aggregation::CausePhase::MissingTarget => NativeCausePhase::MissingTarget,
        },
        slot: value.slot(),
        artifact: value.artifact(),
        kind: match value.kind() {
            aggregation::BlockingKind::Incomplete => NativeBlockingKind::Incomplete,
            aggregation::BlockingKind::Failed => NativeBlockingKind::Failed,
            aggregation::BlockingKind::Errored => NativeBlockingKind::Errored,
        },
        mode: value.mode(),
        slot_mode: value.slot_mode(),
        evidence: value.evidence(),
    }
}
fn graph_cut(value: graph::TerminalCut) -> NativeGraphCut {
    let origin = value.origin();
    NativeGraphCut {
        sequence: value.sequence(),
        kind: match value.kind() {
            graph::FailureKind::DependencyFailed => NativeFailureKind::DependencyFailed,
            graph::FailureKind::Deadlocked => NativeFailureKind::Deadlocked,
        },
        origin: binding(origin.binding()),
        origin_created: origin.created(),
        origin_terminal: origin.terminal(),
        fingerprint: value.fingerprint(),
        deadline: value.deadline(),
        fired_at: value.fired_at(),
    }
}
fn terminal(value: claim::ClaimTerminalCut) -> NativeTerminalCut {
    match value {
        claim::ClaimTerminalCut::Explicit(value) => NativeTerminalCut::Explicit(cut(value)),
        claim::ClaimTerminalCut::Required(value) => NativeTerminalCut::Required {
            sequence: value.sequence(),
            cause: blocking(value.cause()),
        },
        claim::ClaimTerminalCut::Graph(value) => NativeTerminalCut::Graph(graph_cut(value)),
    }
}
fn response_link(value: claim::ResponseLink) -> NativeResponseLink {
    NativeResponseLink {
        testament: value.testament,
        content: value.content,
        receipt: value.receipt,
        cycle: value.cycle,
        prior: value.prior,
    }
}
fn slot_policies<'a>(
    slots: impl Iterator<Item = aggregation::SlotPolicy<'a>>,
) -> Vec<NativeSlotPolicy> {
    slots
        .map(|slot| NativeSlotPolicy {
            slot: slot.slot,
            missing_declaration_index: slot.missing_declaration_index,
            mode: slot.mode,
            checks: slot
                .checks
                .iter()
                .map(|check| NativeCheckPolicy {
                    declaration_index: check.declaration_index,
                    validation: check.validation,
                    mode: check.mode,
                })
                .collect(),
        })
        .collect()
}
pub(crate) fn monitor(claim: ClaimId, value: &scope::Scope) -> NativeMonitor {
    NativeMonitor {
        claim,
        id: value.id(),
        roots: value.roots().to_vec(),
        deadline: value.deadline(),
        registered: value.registered(),
        disposition: value.disposition().map(|disposition| match disposition {
            scope::MonitorDisposition::Released(value) => {
                NativeMonitorDisposition::Released(cut(value))
            }
            scope::MonitorDisposition::Cancelled(value) => NativeMonitorDisposition::Cancelled {
                terminal: value.terminal,
                cut: cut(value.cut),
            },
        }),
        last_rebinding: value.last_rebinding().map(|value| NativeRebinding {
            predecessor: value.predecessor,
            successor: value.successor,
            cut: cut(value.cut),
        }),
    }
}
fn scopes(claim: ClaimId, registry: &scope::Registry) -> NativeScopes {
    let limits = registry.limits();
    NativeScopes {
        limits: NativeScopeLimits {
            scopes: u32::try_from(limits.scopes).unwrap_or(u32::MAX),
            roots: u32::try_from(limits.roots).unwrap_or(u32::MAX),
            children: u32::try_from(limits.children).unwrap_or(u32::MAX),
        },
        released: registry.released(),
        release_cut: registry.release_cut().map(cut),
        monitors: registry.iter().map(|value| monitor(claim, value)).collect(),
        children: registry
            .children()
            .iter()
            .map(|child| NativeOwnedChild {
                binding: binding(child.binding()),
                registered: child.registered(),
            })
            .collect(),
    }
}
pub(crate) fn claim_content(
    value: &lifecycle::claim_descriptor::ClaimDescriptor,
) -> NativeClaimContent {
    NativeClaimContent {
        binding: binding(value.binding()),
        schema: value.schema(),
        occurrence: value.occurrence(),
        description: value.description().to_owned(),
        action: value.action(),
        cause: value.cause().clone(),
        relations: value.relations().to_vec(),
        scopes: value
            .scopes()
            .map(|scope| Scope {
                kind: scope.kind,
                key: scope.key.to_owned(),
            })
            .collect(),
        requirements: value.requirements().to_vec(),
        slots: slot_policies(value.slots()),
        deadline: value.deadline(),
        policy: value.policy(),
        content_hash: value.content_hash(),
        intent: value.intent_fingerprint(),
    }
}
pub(crate) fn claim(
    core: &Core<NativeState>,
    state: &ClaimState,
    expand: NativeClaimExpand,
) -> NativeClaim {
    let id = ClaimId(state.binding().object.0);
    let lineage: &succession::Lineage = state.lineage();
    NativeClaim {
        binding: binding(state.binding()),
        issuer: state.issuer(),
        subject: state.subject(),
        created: state.created(),
        deadline: state.deadline(),
        status: state.status(),
        origin: match state.origin() {
            claim::ClaimOrigin::Native => NativeClaimOrigin::Native,
            claim::ClaimOrigin::Legacy => NativeClaimOrigin::Legacy,
        },
        released: state.released(),
        receipt: state.receipt().map(entitlement),
        local_complete: state.local_complete(),
        local_sealed_at: state.local_sealed_at(),
        terminal: state.terminal_cut().map(terminal),
        latest_response: state.latest_response().map(response_link),
        response_count: u32::try_from(state.response_count()).unwrap_or(u32::MAX),
        max_responses: state.max_responses(),
        obligations: state
            .graph()
            .obligations()
            .iter()
            .map(|obligation| NativeObligation {
                kind: match obligation.kind {
                    graph::Kind::DependsOn => NativeObligationKind::DependsOn,
                    graph::Kind::Awaits => NativeObligationKind::Awaits,
                },
                target: obligation.target,
            })
            .collect(),
        cause: lineage.cause().clone(),
        corrections: lineage
            .corrections()
            .iter()
            .map(|correction| NativeCorrection {
                kind: match correction.kind {
                    succession::CorrectionKind::Supersedes => NativeCorrectionKind::Supersedes,
                    succession::CorrectionKind::Amends => NativeCorrectionKind::Amends,
                },
                predecessor: correction.predecessor,
            })
            .collect(),
        acceptance: slot_policies(state.acceptance().slots()),
        scopes: expand.scopes.then(|| scopes(id, state.scopes())),
        content: if expand.content {
            core.native_claim_content(id).map(claim_content)
        } else {
            None
        },
    }
}
fn phase_policy(value: validation::PhasePolicyView<'_>) -> NativePhasePolicy {
    NativePhasePolicy {
        evaluator: value.evaluator(),
        definition: value.definition(),
        required_policy: value.required_policy(),
        handlers: value
            .handlers()
            .map(|handler| NativeHandlerPolicy {
                id: handler.handler.id,
                version: handler.handler.version,
                agentic: handler.handler.agentic,
                attempts: handler.attempts,
                proof_schema: handler.proof_schema,
                diagnostic_schema: handler.diagnostic_schema,
            })
            .collect(),
    }
}
pub(crate) fn definition(
    core: &Core<NativeState>,
    value: &validation::Declaration,
) -> NativeDefinition {
    let id = ValidationId(value.binding().object.0);
    NativeDefinition {
        binding: binding(value.binding()),
        claim: value.claim(),
        issuer: value.issuer(),
        declaration_index: value.declaration_index(),
        kind: value.kind(),
        phase: value.declared_phase(),
        mode: value.mode(),
        target: match value.target() {
            validation::TargetDeclaration::WholeWorkSlot { index, name } => {
                NativeTargetDeclaration::WholeWorkSlot {
                    index,
                    name: name.to_owned(),
                }
            }
            validation::TargetDeclaration::Delivery => NativeTargetDeclaration::Delivery,
            validation::TargetDeclaration::Admission => NativeTargetDeclaration::Admission,
            validation::TargetDeclaration::Increment => NativeTargetDeclaration::Increment,
        },
        program: match value.program() {
            validation::ProgramView::Delivery => NativeProgram::Delivery,
            validation::ProgramView::Programmatic { check, quality } => {
                NativeProgram::Programmatic {
                    check: phase_policy(check),
                    quality: quality.map(phase_policy),
                }
            }
            validation::ProgramView::Agentic { check } => NativeProgram::Agentic {
                check: phase_policy(check),
            },
        },
        deadline: value.deadline(),
        attempt_bound: value.attempt_bound(),
        content: core
            .native_validation_descriptor(id)
            .map(|descriptor| NativeDefinitionContent {
                description: descriptor.description().to_owned(),
                quality_bar: descriptor.quality_bar().map(str::to_owned),
                contributed_by: descriptor.contributed_by().to_vec(),
                policy_revision: descriptor.policy_revision(),
                schema: descriptor.schema(),
                content_hash: descriptor.content_hash(),
                specification_hash: descriptor.specification_hash(),
                intent: descriptor.intent_fingerprint(),
            }),
    }
}
pub(crate) fn evaluation(
    declaration: &validation::Declaration,
    key: core_native::EvaluationKey,
    state: &validation::EvaluationState,
) -> Result<NativeEvaluation, AccessError> {
    let bound = (*state)
        .bind(declaration)
        .map_err(|_| AccessError::Unavailable)?;
    Ok(NativeEvaluation {
        binding: binding(state.binding()),
        key: evaluation_key(key),
        target: target(state.target()),
        state: validation_state(state.state()),
        phase: phase(state.phase()),
        declared_phase: bound.declared_phase(),
        declaration_index: bound.declaration_index(),
        issuer: bound.issuer(),
        evaluator: bound.evaluator().ok(),
        mode: bound.mode(),
        receipt: state.receipt(),
        has_begun: state.has_begun(),
        attempt_index: bound.attempt_index(),
        attempt_bound: bound.attempt_bound(),
        fence: state.fence().map(fence),
        suppression: bound.suppression().map(suppression),
        last_result: state
            .last_result()
            .map(|result| result_ref(core_native::NativeResultKey::of(result))),
        sealed: state.sealed(),
        deadline: bound.deadline(),
        current_attempt: bound.current_attempt().ok().map(attempt),
    })
}
fn accepted(
    value: validation::AcceptedResult,
    attempt_value: validation::Attempt,
    sequence: SessionSeq,
    ordinal: u32,
) -> NativeResult {
    NativeResult {
        key: result_ref(core_native::NativeResultKey::of(value)),
        binding: binding(value.binding()),
        declaration_index: value.declaration_index(),
        attempt: attempt(attempt_value),
        verdict: value.verdict(),
        mode: value.mode(),
        receipt: value.receipt(),
        evidence: value.evidence(),
        programmatic_evidence: value.programmatic_evidence(),
        reporter: value.reporter(),
        resulting_state: validation_state(value.resulting_state()),
        sequence,
        ordinal,
    }
}
pub(crate) fn result(value: &core_native::NativeAccepted) -> NativeResult {
    accepted(
        value.result(),
        value.attempt(),
        value.sequence(),
        value.ordinal(),
    )
}
/// The delivery result of a response: the issuer's acknowledgment, which the
/// owner records without any handler attempt.
pub(crate) fn delivery_result(value: &core_native::NativeDeliveryResult) -> NativeDeliveryOutcome {
    let result = value.result();
    NativeDeliveryOutcome {
        key: result_ref(core_native::NativeResultKey::of(result)),
        binding: binding(result.binding()),
        declaration_index: result.declaration_index(),
        verdict: result.verdict(),
        mode: result.mode(),
        receipt: result.receipt(),
        reporter: result.reporter(),
        resulting_state: validation_state(result.resulting_state()),
        sequence: value.sequence(),
        ordinal: value.ordinal(),
    }
}
pub(crate) fn artifact(value: &core_native::NativeArtifact) -> NativeArtifact {
    let descriptor = value.descriptor();
    let custody = value.custody();
    NativeArtifact {
        binding: binding(descriptor.binding()),
        id: descriptor.id(),
        schema: descriptor.schema(),
        kind: descriptor.kind().to_owned(),
        schema_hash: descriptor.schema_hash(),
        metadata: descriptor.metadata().to_vec(),
        payload: match descriptor.payload() {
            lifecycle::artifact_descriptor::PayloadSpec::Inline(bytes) => {
                NativePayload::Inline(bytes.to_vec())
            }
            lifecycle::artifact_descriptor::PayloadSpec::Content(pointer) => {
                NativePayload::Content(content_ref(pointer))
            }
        },
        producer: descriptor.producer(),
        receipt: descriptor.receipt(),
        result_provenance: descriptor.result_provenance().map(|provenance| {
            NativeResultProvenance {
                claim: provenance.claim,
                validation: provenance.validation,
                target: target(provenance.target),
                generation: provenance.generation,
                attempt: attempt(provenance.attempt),
                value: provenance.value,
            }
        }),
        work_provenance: descriptor
            .work_provenance()
            .map(|provenance| NativeWorkProvenance {
                claim: provenance.claim,
                cycle: provenance.cycle,
                role: match provenance.role {
                    lifecycle::artifact_descriptor::WorkRole::Output { slot } => {
                        NativeWorkRole::Output { slot }
                    }
                    lifecycle::artifact_descriptor::WorkRole::Diagnostic { reason } => {
                        NativeWorkRole::Diagnostic {
                            reason: failure(reason),
                        }
                    }
                    lifecycle::artifact_descriptor::WorkRole::ReceiptRejection {
                        artifact,
                        reason,
                    } => NativeWorkRole::ReceiptRejection {
                        artifact,
                        reason: failure(reason),
                    },
                },
            }),
        inputs: descriptor.inputs().to_vec(),
        visibility: descriptor.visibility().map(str::to_owned).collect(),
        content_hash: descriptor.content_hash(),
        custody: NativeCustody {
            payload: content_ref(custody.payload()),
            local_revision: custody.local_revision(),
        },
    }
}
fn content_ref(pointer: lifecycle::artifact_descriptor::ContentPointer) -> ContentRef {
    ContentRef {
        domain: pointer.domain,
        root: pointer.root,
        length: pointer.length,
        class: pointer.class,
    }
}
fn diagnostic(value: evidence::Diagnostic) -> NativeDiagnostic {
    NativeDiagnostic {
        reason: failure(value.reason),
        artifact: value.artifact,
    }
}
pub(crate) fn work(value: &evidence::WorkArtifact) -> NativeWorkArtifact {
    NativeWorkArtifact {
        binding: binding(value.binding()),
        reference: value.reference(),
        claim: value.claim(),
        cycle: value.cycle(),
        slot: value.slot(),
        state: work_state(value.state()),
        producer: value.producer(),
        receipt: value.receipt(),
        attachment: value.attachment(),
        diagnostic: value.diagnostic().map(diagnostic),
        terminal: value.terminal().map(|(sequence, outcome)| {
            (
                sequence,
                match outcome {
                    aggregation::ArtifactOutcome::Pending => NativeArtifactOutcome::Pending,
                    aggregation::ArtifactOutcome::Passed => NativeArtifactOutcome::Passed,
                    aggregation::ArtifactOutcome::Blocked(cause) => {
                        NativeArtifactOutcome::Blocked(blocking(cause))
                    }
                },
            )
        }),
    }
}
pub(crate) fn response(value: &evidence::Response) -> NativeResponse {
    let identity = value.identity();
    NativeResponse {
        binding: binding(identity.binding),
        claim: identity.claim,
        receipt: identity.receipt,
        cycle: identity.cycle,
        prior: identity.prior,
        respondent: value.respondent(),
        state: response_state(value.state()),
        summary: value.summary().to_owned(),
        confidence: value.confidence(),
        outcome: value.reported_outcome(),
        manifest: value
            .manifest()
            .iter()
            .map(|slot| NativeSlotBinding {
                slot: slot.slot,
                artifact: slot.artifact,
            })
            .collect(),
        failed_work: value
            .failed_work()
            .iter()
            .map(|failed| NativeFailedWork {
                binding: binding(failed.binding()),
                slot: failed.slot(),
                state: work_state(failed.state()),
                diagnostic: diagnostic(failed.diagnostic()),
            })
            .collect(),
        diagnostics: value
            .diagnostics()
            .iter()
            .map(|entry| NativeResponseDiagnostic {
                producer: entry.producer(),
                diagnostic: diagnostic(entry.diagnostic()),
            })
            .collect(),
        terminal: value.terminal().map(|outcome| match outcome {
            aggregation::ResponseOutcome::Evaluating => NativeResponseOutcome::Evaluating,
            aggregation::ResponseOutcome::Validated { sequence } => {
                NativeResponseOutcome::Validated { sequence }
            }
            aggregation::ResponseOutcome::Blocked(cut) => NativeResponseOutcome::Blocked {
                sequence: cut.sequence(),
                cause: blocking(cut.cause()),
            },
        }),
    }
}
pub(crate) fn result_testament(
    value: &core_native::NativeResultTestament,
) -> NativeResultTestament {
    let testament = value.testament();
    NativeResultTestament {
        binding: binding(testament.binding()),
        claim: testament.claim(),
        state: testament_state(testament.state()),
    }
}
pub(crate) fn receipt(value: core_native::NativeReceipt) -> NativeReceiptRecord {
    NativeReceiptRecord {
        claim: value.claim,
        fence: value.fence,
        holder: value.holder,
        acquired: value.acquired,
    }
}
pub(crate) fn creation_result(
    invocation_value: core_native::NativeInvocation,
    value: &core_native::NativeCreationResult,
) -> NativeCreationResult {
    NativeCreationResult {
        invocation: invocation(invocation_value),
        created: value
            .entries()
            .iter()
            .map(|entry| NativeCreatedObject {
                ordinal: entry.ordinal,
                family: match entry.family {
                    core_native::NativeCreatedFamily::Claim => NativeCreatedFamily::Claim,
                    core_native::NativeCreatedFamily::Validation => NativeCreatedFamily::Validation,
                },
                schema: entry.schema,
                content: entry.content,
                requested: entry.requested,
                resolved: entry.resolved,
            })
            .collect(),
    }
}
