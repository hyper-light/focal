//! Document to `NativeInput` compilation, one function per authored verb.
//! Every identity the frame carries is either authored, generated here once,
//! or a binding read from the ledger; nothing is derived from wall-clock time.
use crate::{
    CompileError, CompileLimits, Resolved,
    resolve::{EvaluationSelector, ResolvedClaim, ResolvedEvaluation},
};
use focal_client::input::{
    BuildContext, IdGenerator, InputError, parse_action, parse_confidence, parse_evidence_failure,
    parse_hash, parse_id, parse_object_kind, parse_outcome, parse_relation, parse_scope_kind,
    parse_validation_kind, parse_validation_mode, parse_validation_phase, parse_verdict,
    resolve_participant,
};
use focal_client::native_store::{NativeIdentity, NativeIdentityKind};
use focal_client::operations::{
    NativeAdoptReceiptDocument, NativeArtifactTargetDocument, NativeAuditDocument,
    NativeAuthoredOperation, NativeClaimDocument, NativeDeadlineDocument, NativeDiagnosticDocument,
    NativeEvaluationDocument, NativeFailWorkDocument, NativeHandlerDocument, NativeMonitorDocument,
    NativeMonitorRebindDocument, NativeMonitorTargetDocument, NativeObjectReferenceDocument,
    NativePayloadDocument, NativePhaseDocument, NativeReceiptDocument, NativeRejectWorkDocument,
    NativeReportDocument, NativeResponseDocument, NativeResponseTargetDocument,
    NativeTargetDocument, NativeValidationDocument, NativeWorkArtifactDocument,
};
use focal_core::native::{
    EvaluationTarget, NativeArtifactInput, NativeAuthoredProposal, NativeCommand, NativeInput,
    NativeResponseInput, NativeResponseSpec,
};
use focal_evidence::{error_report_schema, test_report_schema};
use focal_model::lifecycle::{
    Binding, Principal,
    aggregation::{CheckPolicy, SlotPolicy},
    artifact_descriptor::{
        ArtifactDescriptor, ArtifactSpec, PayloadSpec, ResultProvenance, WorkProvenance, WorkRole,
    },
    claim_descriptor::{ClaimDescriptor, ClaimSpec, ScopeSpec},
    creation::Owner,
    evidence::{EvidenceFailure, SlotBinding},
    scope::ScopeLimits,
    validation::{self, HandlerPolicy, PhasePolicy, Program, TargetDeclaration},
    validation_descriptor::{ValidationDescriptor, ValidationSpec},
};
use focal_model::*;
use std::collections::BTreeSet;

/// A compiled operation: the owner input and the identities it mints.
#[derive(Debug)]
pub struct Compiled {
    pub input: NativeInput,
    pub created: Vec<NativeIdentity>,
}

/// Compile one authored operation for `request` under the authenticated
/// context. `resolved` must come from the reads named by [`crate::requirements`].
pub fn compile(
    operation: &NativeAuthoredOperation,
    context: &BuildContext,
    request: RequestId,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
) -> Result<Compiled, CompileError> {
    context.validate()?;
    if request.is_zero() {
        return Err(InputError::Invalid("zero request identity").into());
    }
    let key = RequestKey {
        principal: context.actor,
        epoch: RequestEpoch(1),
        id: request,
    };
    let mut created = Vec::new();
    let command = match operation {
        NativeAuthoredOperation::ClaimSubmit(document) => {
            claim(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimChallenge(document) => {
            let document = crate::peer::challenge(document, resolved)?;
            claim(&document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimConsult(document) => {
            let document = crate::peer::consult(document);
            claim(&document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimCorrect(document) => {
            let document = crate::peer::correction(document, context, resolved)?;
            claim(&document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimFollowUp(document) => {
            let document = crate::peer::follow_up(document, context, resolved)?;
            claim(&document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimPost(document) => NativeCommand::Post {
            expected: claim_binding(resolved, &document.claim)?,
        },
        NativeAuthoredOperation::ClaimCancel(document) => NativeCommand::Cancel {
            expected: claim_binding(resolved, &document.claim)?,
        },
        NativeAuthoredOperation::ReceiptAcquire(document) => {
            receipt(document, resolved, ids, &mut created)?
        }
        NativeAuthoredOperation::ArtifactSubmit(document) => {
            work(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ArtifactDiagnostic(document) => {
            diagnostic(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::TestamentSubmit(document) => {
            response(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::TestamentPost(document) => {
            let (claim, expected) = response_target(document, resolved)?;
            NativeCommand::PostResponse { claim, expected }
        }
        NativeAuthoredOperation::TestamentReceive(document) => {
            let (claim, expected) = response_target(document, resolved)?;
            NativeCommand::ReceiveResponse { claim, expected }
        }
        NativeAuthoredOperation::ValidationBegin(document) => begin(document, resolved)?,
        NativeAuthoredOperation::ValidationReport(document) => {
            report(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ClaimReleaseScope(document) => NativeCommand::ReleaseScope {
            expected: claim_binding(resolved, &document.claim)?,
        },
        NativeAuthoredOperation::ReceiptAdopt(document) => {
            adopt(document, context, resolved, ids, &mut created)?
        }
        NativeAuthoredOperation::ArtifactFail(document) => fail_work(document, resolved)?,
        NativeAuthoredOperation::ArtifactReceive(document) => {
            let (claim, expected) = work_target(document, resolved)?;
            NativeCommand::ReceiveWork { claim, expected }
        }
        NativeAuthoredOperation::ArtifactReject(document) => {
            reject_work(document, context, ids, resolved, limits, &mut created)?
        }
        NativeAuthoredOperation::ValidationSealIncrements(document) => {
            NativeCommand::SealIncrementTargets {
                claim: claim_binding(resolved, &document.claim)?,
            }
        }
        NativeAuthoredOperation::ValidationEnterWholeWork(document) => {
            let (claim, expected) = response_target(document, resolved)?;
            NativeCommand::EnterWholeWork { claim, expected }
        }
        NativeAuthoredOperation::AuditGenerate(document) => {
            audit(document, resolved, ids, &mut created)?
        }
        NativeAuthoredOperation::AuditPost(document) => {
            let testament =
                resolved.result_testament(TestamentId(parse_id(&document.testament)?))?;
            if testament.posted {
                return Err(CompileError::Unsupported(
                    "the result testament is already posted",
                ));
            }
            NativeCommand::PostResultTestament {
                expected: testament.binding,
            }
        }
        NativeAuthoredOperation::MonitorRegister(document) => {
            monitor(document, resolved, ids, &mut created)?
        }
        NativeAuthoredOperation::MonitorRebind(document) => rebind(document, resolved)?,
        NativeAuthoredOperation::MonitorCancel(document) => cancel_monitor(document, resolved)?,
    };
    Ok(Compiled {
        input: NativeInput {
            request: key,
            command,
        },
        created,
    })
}

fn text(value: &str, required: bool) -> Result<(), CompileError> {
    if value.len() > 16 * 1024 {
        return Err(InputError::Capacity.into());
    }
    if (required && value.trim().is_empty()) || value.contains('\0') {
        return Err(InputError::Invalid("empty or NUL-containing text").into());
    }
    Ok(())
}
fn allocated(value: Option<&str>, ids: &mut impl IdGenerator) -> Result<[u8; 16], CompileError> {
    match value {
        Some(value) => Ok(parse_id(value)?),
        None => {
            let id = ids.next_id()?;
            if id.iter().all(|byte| *byte == 0) {
                return Err(InputError::Identity.into());
            }
            Ok(id)
        }
    }
}
fn reserve<T>(items: &mut Vec<T>, additional: usize) -> Result<(), CompileError> {
    items
        .try_reserve_exact(additional)
        .map_err(|_| CompileError::Capacity("authored collection"))
}
fn record(
    created: &mut Vec<NativeIdentity>,
    kind: NativeIdentityKind,
    id: [u8; 16],
) -> Result<(), CompileError> {
    reserve(created, 1)?;
    created.push(NativeIdentity { kind, id });
    Ok(())
}
fn deadline(
    document: &NativeDeadlineDocument,
    ids: &mut impl IdGenerator,
) -> Result<Deadline, CompileError> {
    if document.generation == 0 || document.at == 0 {
        return Err(InputError::Invalid("deadline fence").into());
    }
    Ok(Deadline {
        timer: TimerId(allocated(document.timer.as_deref(), ids)?),
        generation: document.generation,
        at: document.at,
    })
}
fn claim_binding(resolved: &Resolved, claim: &str) -> Result<Binding, CompileError> {
    Ok(resolved.claim(ClaimId(parse_id(claim)?))?.binding)
}
/// The claim whose current receipt the authenticated actor holds.
fn held_claim<'a>(
    resolved: &'a Resolved,
    claim: &str,
    actor: ParticipantId,
) -> Result<(&'a ResolvedClaim, ReceiptFence), CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(claim)?))?;
    match claim.receipt {
        Some((holder, fence)) if holder == actor => Ok((claim, fence)),
        Some(_) => Err(CompileError::Unsupported(
            "the claim's current receipt is held by another participant",
        )),
        None => Err(CompileError::Unsupported(
            "the claim has no current receipt; acquire it first",
        )),
    }
}
fn receipt(
    document: &NativeReceiptDocument,
    resolved: &Resolved,
    ids: &mut impl IdGenerator,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let expected = claim_binding(resolved, &document.claim)?;
    let receipt = allocated(document.id.as_deref(), ids)?;
    record(created, NativeIdentityKind::Receipt, receipt)?;
    Ok(NativeCommand::AcquireReceipt {
        expected,
        receipt: ReceiptId(receipt),
    })
}
fn response_target(
    document: &NativeResponseTargetDocument,
    resolved: &Resolved,
) -> Result<(Binding, Binding), CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let response = resolved.response(TestamentId(parse_id(&document.testament)?))?;
    if response.claim.0 != claim.binding.object.0 {
        return Err(CompileError::Unsupported(
            "the testament belongs to a different claim",
        ));
    }
    Ok((claim.binding, response.binding))
}

/// The claim's current entitlement, which fences owner-authored changes.
fn current_fence(claim: &ResolvedClaim) -> Option<ReceiptFence> {
    claim.receipt.map(|(_, fence)| fence)
}
fn adopt(
    document: &NativeAdoptReceiptDocument,
    context: &BuildContext,
    resolved: &Resolved,
    ids: &mut impl IdGenerator,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let previous = current_fence(claim).ok_or(CompileError::Unsupported(
        "the claim has no current receipt to adopt; the subject acquires the first one",
    ))?;
    let holder = resolve_participant(&document.holder, context)?;
    let receipt = allocated(document.id.as_deref(), ids)?;
    record(created, NativeIdentityKind::Receipt, receipt)?;
    Ok(NativeCommand::AdoptReceipt {
        expected: claim.binding,
        previous,
        receipt: ReceiptId(receipt),
        holder,
    })
}
fn fail_work(
    document: &NativeFailWorkDocument,
    resolved: &Resolved,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let diagnostic = resolved.diagnostic(ArtifactId(parse_id(&document.diagnostic)?))?;
    if diagnostic.claim.0 != claim.binding.object.0 {
        return Err(CompileError::Unsupported(
            "the diagnostic belongs to a different claim",
        ));
    }
    if diagnostic.reason != Some(EvidenceFailure::Production) {
        return Err(CompileError::Unsupported(
            "only a production diagnostic records a failed slot",
        ));
    }
    if let Some(hash) = &document.hash
        && parse_hash(hash)? != diagnostic.reference.hash
    {
        return Err(CompileError::Unsupported(
            "the pinned hash differs from the committed diagnostic",
        ));
    }
    Ok(NativeCommand::FailWorkProduction {
        claim: claim.binding,
        slot: document.slot,
        diagnostic: diagnostic.reference,
    })
}
/// The claim and one of its work artifacts, as the issuer observes them.
fn work_target(
    document: &NativeArtifactTargetDocument,
    resolved: &Resolved,
) -> Result<(Binding, Binding), CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let work = resolved.work(ArtifactId(parse_id(&document.artifact)?))?;
    if work.claim.0 != claim.binding.object.0 {
        return Err(CompileError::Unsupported(
            "the work artifact belongs to a different claim",
        ));
    }
    Ok((claim.binding, work.binding))
}
fn reject_work(
    document: &NativeRejectWorkDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let work = resolved.work(ArtifactId(parse_id(&document.artifact)?))?;
    if work.claim.0 != claim.binding.object.0 {
        return Err(CompileError::Unsupported(
            "the work artifact belongs to a different claim",
        ));
    }
    let reason: EvidenceFailure = parse_evidence_failure(&document.reason)?;
    if !matches!(
        reason,
        EvidenceFailure::Structure | EvidenceFailure::Metadata
    ) {
        return Err(InputError::Invalid(
            "a rejection records a structure or metadata failure of the received work",
        )
        .into());
    }
    // The rejection inherits every visibility label of the rejected product.
    let source = resolved.artifact(work.reference.id)?;
    let mut visibility = Vec::new();
    reserve(
        &mut visibility,
        source
            .visibility
            .len()
            .checked_add(document.visibility.len())
            .ok_or(CompileError::Capacity("visibility labels"))?,
    )?;
    for label in source.visibility.iter().chain(&document.visibility) {
        if !visibility.contains(label) {
            visibility.push(label.clone());
        }
    }
    let descriptor = artifact(
        Authoring {
            id: document.id.as_deref(),
            kind: document.kind.as_deref(),
            schema_hash: document.schema_hash.as_deref(),
            metadata: &document.metadata,
            payload: &document.payload,
            inputs: &document.inputs,
            visibility: &visibility,
        },
        context.ledger,
        Provenance {
            producer: context.actor,
            receipt: Some(work.receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: work.claim,
                cycle: work.cycle,
                role: WorkRole::ReceiptRejection {
                    artifact: work.reference,
                    reason,
                },
            }),
            diagnostic: true,
        },
        ids,
        limits,
    )?;
    record(created, NativeIdentityKind::Artifact, descriptor.id().0)?;
    Ok(NativeCommand::RejectWork {
        claim: claim.binding,
        expected: work.binding,
        reason,
        artifact: NativeArtifactInput::new(descriptor)?,
    })
}
fn audit(
    document: &NativeAuditDocument,
    resolved: &Resolved,
    ids: &mut impl IdGenerator,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let claim = claim_binding(resolved, &document.claim)?;
    let id = allocated(document.id.as_deref(), ids)?;
    record(created, NativeIdentityKind::ResultTestament, id)?;
    Ok(NativeCommand::GenerateResultTestament {
        claim,
        id: TestamentId(id),
    })
}
fn monitor(
    document: &NativeMonitorDocument,
    resolved: &Resolved,
    ids: &mut impl IdGenerator,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    if document.roots.is_empty() || document.roots.len() > focal_wire::MAX_MONITOR_ROOTS {
        return Err(InputError::Invalid("a monitor names between one and 64 wait roots").into());
    }
    let mut roots = Vec::new();
    reserve(&mut roots, document.roots.len())?;
    for root in &document.roots {
        let target = ClaimId(parse_id(&root.claim)?);
        let predicate = match root.predicate.as_str() {
            "satisfied" => WaitPredicate::Satisfied(target),
            "terminal" => WaitPredicate::Terminal(target),
            "released" => WaitPredicate::Released(target),
            _ => {
                return Err(InputError::Invalid(
                    "wait predicate must be satisfied, terminal or released",
                )
                .into());
            }
        };
        if roots.contains(&predicate) {
            return Err(InputError::Invalid("duplicate wait root").into());
        }
        roots.push(predicate);
    }
    let id = allocated(document.id.as_deref(), ids)?;
    record(created, NativeIdentityKind::Monitor, id)?;
    Ok(NativeCommand::RegisterMonitor {
        expected: claim.binding,
        receipt: current_fence(claim),
        id: MonitorId(id),
        roots,
        deadline: deadline(&document.deadline, ids)?,
    })
}
fn rebind(
    document: &NativeMonitorRebindDocument,
    resolved: &Resolved,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let predecessor = claim_binding(resolved, &document.predecessor)?;
    let successor = claim_binding(resolved, &document.successor)?;
    if predecessor.object == successor.object {
        return Err(InputError::Invalid("a rebinding names two different claims").into());
    }
    Ok(NativeCommand::RebindMonitor {
        expected: claim.binding,
        receipt: current_fence(claim),
        id: MonitorId(parse_id(&document.monitor)?),
        predecessor,
        successor,
    })
}
fn cancel_monitor(
    document: &NativeMonitorTargetDocument,
    resolved: &Resolved,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    Ok(NativeCommand::CancelMonitor {
        expected: claim.binding,
        receipt: current_fence(claim),
        id: MonitorId(parse_id(&document.monitor)?),
    })
}

// ---- artifacts -------------------------------------------------------------

struct Authoring<'a> {
    id: Option<&'a str>,
    kind: Option<&'a str>,
    schema_hash: Option<&'a str>,
    metadata: &'a [u8],
    payload: &'a NativePayloadDocument,
    inputs: &'a [NativeObjectReferenceDocument],
    visibility: &'a [String],
}
struct Provenance {
    producer: ParticipantId,
    receipt: Option<ReceiptFence>,
    result: Option<ResultProvenance>,
    work: Option<WorkProvenance>,
    diagnostic: bool,
}
fn artifact(
    authoring: Authoring<'_>,
    ledger: LedgerId,
    provenance: Provenance,
    ids: &mut impl IdGenerator,
    limits: &CompileLimits,
) -> Result<ArtifactDescriptor, CompileError> {
    let id = ArtifactId(allocated(authoring.id, ids)?);
    let (kind, schema_hash) = match (authoring.kind, authoring.schema_hash) {
        (Some(kind), Some(hash)) => {
            text(kind, true)?;
            (kind, parse_hash(hash)?)
        }
        (None, None) if provenance.diagnostic => ("error", error_report_schema()),
        (None, None) => ("test-report", test_report_schema()),
        _ => {
            return Err(InputError::Invalid(
                "artifact kind and schema_hash must be given together",
            )
            .into());
        }
    };
    let payload = match authoring.payload {
        NativePayloadDocument::Inline { bytes } => bytes.as_slice(),
        NativePayloadDocument::Text { text } => text.as_bytes(),
    };
    let mut inputs = BTreeSet::new();
    for reference in authoring.inputs {
        let reference = ObjectRef {
            ledger,
            kind: parse_object_kind(&reference.kind)?,
            id: ObjectId(parse_id(&reference.id)?),
        };
        if !inputs.insert(reference) {
            return Err(InputError::Invalid("duplicate artifact input").into());
        }
    }
    let mut visibility = BTreeSet::new();
    for label in authoring.visibility {
        text(label, true)?;
        if !visibility.insert(label.as_str()) {
            return Err(InputError::Invalid("duplicate visibility label").into());
        }
    }
    let mut input_refs = Vec::new();
    reserve(&mut input_refs, inputs.len())?;
    input_refs.extend(inputs);
    let mut labels = Vec::new();
    reserve(&mut labels, visibility.len())?;
    labels.extend(visibility);
    let spec = ArtifactSpec {
        ledger,
        id,
        schema: 1,
        kind,
        schema_hash,
        metadata: authoring.metadata,
        payload: PayloadSpec::Inline(payload),
        producer: provenance.producer,
        receipt: provenance.receipt,
        result: provenance.result,
        work: provenance.work,
        inputs: &input_refs,
        visibility: &labels,
    };
    Ok(ArtifactDescriptor::prepare(spec, limits.artifact)?.build()?)
}
fn work(
    document: &NativeWorkArtifactDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let (claim, fence) = held_claim(resolved, &document.claim, context.actor)?;
    let cycle = claim
        .response_count
        .checked_add(1)
        .ok_or(CompileError::Capacity("response cycles"))?;
    let descriptor = artifact(
        Authoring {
            id: document.id.as_deref(),
            kind: document.kind.as_deref(),
            schema_hash: document.schema_hash.as_deref(),
            metadata: &document.metadata,
            payload: &document.payload,
            inputs: &document.inputs,
            visibility: &document.visibility,
        },
        context.ledger,
        Provenance {
            producer: context.actor,
            receipt: Some(fence),
            result: None,
            work: Some(WorkProvenance {
                claim: ClaimId(claim.binding.object.0),
                cycle,
                role: WorkRole::Output {
                    slot: document.slot,
                },
            }),
            diagnostic: false,
        },
        ids,
        limits,
    )?;
    record(created, NativeIdentityKind::Artifact, descriptor.id().0)?;
    Ok(NativeCommand::SubmitWork {
        claim: claim.binding,
        slot: document.slot,
        artifact: NativeArtifactInput::new(descriptor)?,
    })
}
fn diagnostic(
    document: &NativeDiagnosticDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let (claim, fence) = held_claim(resolved, &document.claim, context.actor)?;
    let reason: EvidenceFailure = parse_evidence_failure(&document.reason)?;
    let cycle = claim
        .response_count
        .checked_add(1)
        .ok_or(CompileError::Capacity("response cycles"))?;
    let descriptor = artifact(
        Authoring {
            id: document.id.as_deref(),
            kind: document.kind.as_deref(),
            schema_hash: document.schema_hash.as_deref(),
            metadata: &document.metadata,
            payload: &document.payload,
            inputs: &document.inputs,
            visibility: &document.visibility,
        },
        context.ledger,
        Provenance {
            producer: context.actor,
            receipt: Some(fence),
            result: None,
            work: Some(WorkProvenance {
                claim: ClaimId(claim.binding.object.0),
                cycle,
                role: WorkRole::Diagnostic { reason },
            }),
            diagnostic: true,
        },
        ids,
        limits,
    )?;
    record(created, NativeIdentityKind::Artifact, descriptor.id().0)?;
    Ok(NativeCommand::SubmitDiagnostic {
        claim: claim.binding,
        reason,
        artifact: NativeArtifactInput::new(descriptor)?,
    })
}

// ---- responses ---------------------------------------------------------------

fn response(
    document: &NativeResponseDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let (claim, _) = held_claim(resolved, &document.claim, context.actor)?;
    text(&document.summary, true)?;
    let confidence = parse_confidence(&document.confidence)?;
    let outcome = parse_outcome(&document.outcome)?;
    let id = allocated(document.id.as_deref(), ids)?;
    let mut manifest = Vec::new();
    reserve(&mut manifest, document.manifest.len())?;
    let mut slots = BTreeSet::new();
    for entry in &document.manifest {
        if !slots.insert(entry.slot) {
            return Err(InputError::Invalid("duplicate manifest slot").into());
        }
        manifest.push(SlotBinding {
            slot: entry.slot,
            artifact: ArtifactRef {
                id: ArtifactId(parse_id(&entry.artifact.id)?),
                hash: parse_hash(&entry.artifact.hash)?,
            },
        });
    }
    // The owner matches citations against the cycle's recorded diagnostics
    // in ascending artifact order; the manifest is ordered by slot.
    manifest.sort_unstable_by_key(|entry| entry.slot);
    let mut diagnostics = Vec::new();
    reserve(&mut diagnostics, document.diagnostics.len())?;
    for reference in &document.diagnostics {
        let reference = ArtifactRef {
            id: ArtifactId(parse_id(&reference.id)?),
            hash: parse_hash(&reference.hash)?,
        };
        if diagnostics
            .iter()
            .any(|cited: &ArtifactRef| cited.id == reference.id)
        {
            return Err(InputError::Invalid("duplicate diagnostic citation").into());
        }
        diagnostics.push(reference);
    }
    diagnostics.sort_unstable_by_key(|reference| reference.id);
    if outcome != OutcomeKind::Complete && diagnostics.is_empty() {
        return Err(InputError::Invalid(
            "a non-complete outcome must cite at least one diagnostic",
        )
        .into());
    }
    let spec = NativeResponseSpec {
        summary: &document.summary,
        confidence,
        outcome,
        manifest: &manifest,
        diagnostics: &diagnostics,
    };
    let plan = NativeResponseInput::prepare(spec, limits.native)?;
    let bytes = plan.construction_bytes();
    let report = plan.build(bytes)?;
    // The response binding names authored content; the owner requires a
    // nonzero hash at revision one and stores it as the testament identity.
    let mut hash = blake3::Hasher::new_derive_key("focal.native-client.response-content.v1");
    hash.update(&context.ledger.tenant.0);
    hash.update(&context.ledger.session.0);
    hash.update(&claim.binding.object.0);
    hash.update(&id);
    hash.update(&(report.summary.len() as u64).to_le_bytes());
    hash.update(report.summary.as_bytes());
    hash.update(&[confidence as u8, outcome as u8]);
    hash.update(&(report.manifest.len() as u64).to_le_bytes());
    for entry in &report.manifest {
        hash.update(&entry.slot.to_le_bytes());
        hash.update(&entry.artifact.id.0);
        hash.update(&entry.artifact.hash.0);
    }
    hash.update(&(report.diagnostics.len() as u64).to_le_bytes());
    for entry in &report.diagnostics {
        hash.update(&entry.id.0);
        hash.update(&entry.hash.0);
    }
    record(created, NativeIdentityKind::Response, id)?;
    Ok(NativeCommand::CloseResponse {
        claim: claim.binding,
        response: Binding {
            ledger: context.ledger,
            object: ObjectId(id),
            content: ContentHash(*hash.finalize().as_bytes()),
            revision: ObjectRevision(1),
        },
        report,
    })
}

// ---- evaluations ---------------------------------------------------------------

/// The current evaluation a begin or report addresses; `phase` and `target`
/// select admission and increment evaluations, `slot` whole-work ones.
fn selected_evaluation<'a>(
    resolved: &'a Resolved,
    claim: &ResolvedClaim,
    validation: &str,
    phase: &str,
    slot: Option<u32>,
    target: Option<&str>,
) -> Result<&'a ResolvedEvaluation, CompileError> {
    let selector = EvaluationSelector::parse(phase, slot, target)?;
    let evaluation = resolved.evaluation(
        ClaimId(claim.binding.object.0),
        ValidationId(parse_id(validation)?),
        selector,
    )?;
    let consistent = matches!(
        (selector, evaluation.key.target),
        (
            EvaluationSelector::WholeWork { .. },
            EvaluationTarget::Work { .. }
        ) | (EvaluationSelector::Admission, EvaluationTarget::Admission)
            | (
                EvaluationSelector::Increment { .. },
                EvaluationTarget::Increment { .. }
            )
    );
    if !consistent {
        return Err(CompileError::Unsupported(
            "the selected evaluation is not of the requested phase",
        ));
    }
    Ok(evaluation)
}
fn begin(
    document: &NativeEvaluationDocument,
    resolved: &Resolved,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let evaluation = selected_evaluation(
        resolved,
        claim,
        &document.validation,
        &document.phase,
        document.slot,
        document.target.as_deref(),
    )?;
    if evaluation.has_begun {
        return Err(CompileError::Unsupported(
            "the evaluation has already begun",
        ));
    }
    let (claim, key, expected) = (claim.binding, evaluation.key, evaluation.binding);
    Ok(match evaluation.key.target {
        EvaluationTarget::Admission => NativeCommand::BeginAdmission {
            claim,
            key,
            expected,
        },
        EvaluationTarget::Increment { .. } => NativeCommand::BeginIncrement {
            claim,
            key,
            expected,
        },
        _ => NativeCommand::BeginWork {
            claim,
            key,
            expected,
        },
    })
}
fn report(
    document: &NativeReportDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let claim = resolved.claim(ClaimId(parse_id(&document.claim)?))?;
    let evaluation = selected_evaluation(
        resolved,
        claim,
        &document.validation,
        &document.phase,
        document.slot,
        document.target.as_deref(),
    )?;
    let attempt = evaluation
        .attempt
        .ok_or(CompileError::Unsupported("the evaluation has not begun"))?;
    if attempt.evaluator != context.actor {
        return Err(CompileError::Unsupported(
            "the current attempt names another evaluator",
        ));
    }
    let value = parse_verdict(&document.verdict)?;
    let diagnostic = matches!(value, VerdictValue::Error | VerdictValue::Incomplete);
    let descriptor = artifact(
        Authoring {
            id: document.id.as_deref(),
            kind: document.kind.as_deref(),
            schema_hash: document.schema_hash.as_deref(),
            metadata: &document.metadata,
            payload: &document.payload,
            inputs: &document.inputs,
            visibility: &document.visibility,
        },
        context.ledger,
        Provenance {
            producer: context.actor,
            receipt: evaluation.receipt,
            result: Some(ResultProvenance {
                claim: evaluation.key.claim,
                validation: evaluation.key.validation,
                target: evaluation.target,
                generation: evaluation.key.generation,
                attempt,
                value,
            }),
            work: None,
            diagnostic,
        },
        ids,
        limits,
    )?;
    let evidence = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    record(created, NativeIdentityKind::Artifact, descriptor.id().0)?;
    let report = validation::Report {
        generation: evaluation.key.generation,
        attempt,
        value,
        evidence,
    };
    let artifact = NativeArtifactInput::new(descriptor)?;
    let (claim, key, expected) = (claim.binding, evaluation.key, evaluation.binding);
    Ok(match evaluation.key.target {
        EvaluationTarget::Admission => NativeCommand::ReportAdmission {
            claim,
            key,
            expected,
            report,
            artifact,
        },
        EvaluationTarget::Increment { .. } => NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact,
        },
        _ => NativeCommand::ReportWork {
            claim,
            key,
            expected,
            report,
            artifact,
        },
    })
}

// ---- creation --------------------------------------------------------------------

/// Authored relations in descriptor schema 1 name committed claims of the
/// same ledger; exact artifact targets arrive with relation target tag 4 (R5).
fn authored_relation(kind: &str, target: &str, ledger: LedgerId) -> Result<Relation, CompileError> {
    let kind = parse_relation(kind)?;
    if !matches!(
        kind,
        RelationKind::DependsOn
            | RelationKind::Awaits
            | RelationKind::Supersedes
            | RelationKind::Amends
            | RelationKind::Refines
            | RelationKind::ConflictsWith
            | RelationKind::DerivedFrom
            | RelationKind::Reviews
            | RelationKind::Invalidates
    ) {
        return Err(InputError::Invalid(
            "authored relations are depends_on, awaits, supersedes, amends, refines, conflicts_with, derived_from, reviews or invalidates; issuer, subject, claim_action and caused_by are derived",
        )
        .into());
    }
    if let Some(evidence) = target.strip_prefix("artifact:") {
        // Exact evidence is the artifact at its committed descriptor hash;
        // only a review or derivation may cite it.
        if !matches!(kind, RelationKind::Reviews | RelationKind::DerivedFrom) {
            return Err(InputError::Invalid(
                "only reviews and derived_from relations may target artifact:ID@HASH",
            )
            .into());
        }
        let (id, hash) = evidence.split_once('@').ok_or(InputError::Invalid(
            "evidence target must be artifact:ID@HASH",
        ))?;
        return Ok(Relation {
            kind,
            target: RelationTarget::Evidence(ArtifactRef {
                id: ArtifactId(parse_id(id)?),
                hash: parse_hash(hash)?,
            }),
        });
    }
    let id = target.strip_prefix("claim:").ok_or(InputError::Invalid(
        "relation target must be claim:ID or artifact:ID@HASH",
    ))?;
    Ok(Relation {
        kind,
        target: RelationTarget::Object(ObjectRef::claim(ledger, ClaimId(parse_id(id)?))),
    })
}
struct Handlers {
    refs: Vec<HandlerRef>,
    attempts: Vec<(u32, ContentHash, ContentHash)>,
}
fn handlers(documents: &[NativeHandlerDocument]) -> Result<Handlers, CompileError> {
    if documents.is_empty() {
        return Err(InputError::Invalid("a check phase needs at least one handler").into());
    }
    let mut refs = Vec::new();
    reserve(&mut refs, documents.len())?;
    let mut attempts = Vec::new();
    reserve(&mut attempts, documents.len())?;
    let mut seen = BTreeSet::new();
    for handler in documents {
        let reference = HandlerRef {
            id: ValidatorId(parse_id(&handler.id)?),
            version: parse_hash(&handler.version)?,
            agentic: handler.agentic,
        };
        if !seen.insert((reference.id, reference.version)) {
            return Err(InputError::Invalid("duplicate pinned handler").into());
        }
        if handler.attempts == 0 {
            return Err(InputError::Invalid("handler attempts must be positive").into());
        }
        let proof = match &handler.proof_schema {
            Some(hash) => parse_hash(hash)?,
            None => test_report_schema(),
        };
        let diagnostic = match &handler.diagnostic_schema {
            Some(hash) => parse_hash(hash)?,
            None => error_report_schema(),
        };
        refs.push(reference);
        attempts.push((handler.attempts, proof, diagnostic));
    }
    Ok(Handlers { refs, attempts })
}
fn policies<'a>(handlers: &'a Handlers) -> Result<Vec<HandlerPolicy<'a>>, CompileError> {
    let mut policies = Vec::new();
    reserve(&mut policies, handlers.refs.len())?;
    for (handler, (attempts, proof_schema, diagnostic_schema)) in
        handlers.refs.iter().zip(&handlers.attempts)
    {
        policies.push(HandlerPolicy {
            handler,
            attempts: *attempts,
            proof_schema: *proof_schema,
            diagnostic_schema: *diagnostic_schema,
        });
    }
    Ok(policies)
}
/// The authored identity of one phase policy: a deterministic digest of its
/// evaluator, ordered handlers and capability requirement.
fn definition(
    label: &[u8],
    evaluator: ParticipantId,
    policies: &[HandlerPolicy<'_>],
    required: Option<ContentHash>,
) -> ContentHash {
    let mut hash = blake3::Hasher::new_derive_key("focal.native-client.phase-definition.v1");
    hash.update(label);
    hash.update(&evaluator.0);
    hash.update(&(policies.len() as u64).to_le_bytes());
    for policy in policies {
        hash.update(&policy.handler.id.0);
        hash.update(&policy.handler.version.0);
        hash.update(&[u8::from(policy.handler.agentic)]);
        hash.update(&policy.attempts.to_le_bytes());
        hash.update(&policy.proof_schema.0);
        hash.update(&policy.diagnostic_schema.0);
    }
    match required {
        Some(required) => {
            hash.update(&[1]);
            hash.update(&required.0);
        }
        None => {
            hash.update(&[0]);
        }
    }
    ContentHash(*hash.finalize().as_bytes())
}
fn optional_hash(value: Option<&String>) -> Result<Option<ContentHash>, CompileError> {
    value
        .map(|hash| parse_hash(hash))
        .transpose()
        .map_err(Into::into)
}
fn declaration(
    index: u32,
    document: &NativeValidationDocument,
    context: &BuildContext,
    claim: ClaimId,
    ids: &mut impl IdGenerator,
    limits: &CompileLimits,
) -> Result<ValidationDescriptor, CompileError> {
    text(&document.description, true)?;
    let kind = parse_validation_kind(&document.kind)?;
    let phase = parse_validation_phase(&document.phase)?;
    let mode = parse_validation_mode(&document.mode)?;
    let id = ValidationId(allocated(document.id.as_deref(), ids)?);
    let deadline = deadline(&document.deadline, ids)?;
    let policy_revision = document.policy_revision.unwrap_or(context.policy_revision);
    if policy_revision != context.policy_revision {
        return Err(InputError::Invalid("policy revision differs from context").into());
    }
    let mut contributors = BTreeSet::new();
    for participant in &document.contributed_by {
        if !contributors.insert(resolve_participant(participant, context)?) {
            return Err(InputError::Invalid("duplicate contributor").into());
        }
    }
    let mut contributed_by = Vec::new();
    reserve(&mut contributed_by, contributors.len())?;
    contributed_by.extend(contributors);
    if let Some(quality_bar) = &document.quality_bar {
        text(quality_bar, true)?;
    }
    let receipt = kind == ValidationKind::Receipt;
    if receipt
        && (!matches!(document.target, None | Some(NativeTargetDocument::Delivery))
            || document.evaluator.is_some()
            || !document.handlers.is_empty()
            || document.quality.is_some()
            || document.quality_bar.is_some()
            || document.required_policy.is_some()
            || phase != ValidationPhase::WholeWork
            || mode != ValidationMode::Required)
    {
        return Err(InputError::Invalid(
            "receipt is the required whole_work delivery check and takes no evaluator, handlers or quality phase",
        )
        .into());
    }
    let target = match (&document.target, receipt) {
        (_, true) => TargetDeclaration::Delivery,
        (Some(NativeTargetDocument::Delivery), false) | (None, false) => {
            return Err(InputError::Invalid(
                "a non-receipt validation names its target: slot, admission or increment",
            )
            .into());
        }
        (Some(NativeTargetDocument::Admission), false) => TargetDeclaration::Admission,
        (Some(NativeTargetDocument::Increment), false) => TargetDeclaration::Increment,
        (Some(NativeTargetDocument::Slot { index, name }), false) => {
            text(name, true)?;
            TargetDeclaration::WholeWorkSlot {
                index: *index,
                name,
            }
        }
    };
    let expected_phase = match target {
        TargetDeclaration::WholeWorkSlot { .. } | TargetDeclaration::Delivery => {
            ValidationPhase::WholeWork
        }
        TargetDeclaration::Admission => ValidationPhase::Admission,
        TargetDeclaration::Increment => ValidationPhase::Increment,
    };
    if phase != expected_phase {
        return Err(InputError::Invalid("validation phase does not match its target").into());
    }
    // Owned handler storage outlives the borrowed policies below.
    let check_handlers = if receipt {
        None
    } else {
        Some(handlers(&document.handlers)?)
    };
    let quality_handlers = match (&document.quality, receipt) {
        (Some(quality), false) => Some(handlers(&quality.handlers)?),
        (Some(_), true) => return Err(InputError::Invalid("receipt takes no quality phase").into()),
        (None, _) => None,
    };
    let check_policies = match &check_handlers {
        Some(handlers) => policies(handlers)?,
        None => Vec::new(),
    };
    let quality_policies = match &quality_handlers {
        Some(handlers) => policies(handlers)?,
        None => Vec::new(),
    };
    let program = if receipt {
        Program::Delivery
    } else {
        let evaluator = document
            .evaluator
            .as_deref()
            .ok_or(InputError::Invalid("a check phase names its evaluator"))?;
        let evaluator = resolve_participant(evaluator, context)?;
        let required_policy = optional_hash(document.required_policy.as_ref())?;
        let check = PhasePolicy {
            evaluator,
            definition: definition(b"check", evaluator, &check_policies, required_policy),
            handlers: &check_policies,
            required_policy,
        };
        let agentic = check_policies
            .iter()
            .filter(|policy| policy.handler.agentic)
            .count();
        let quality = match &document.quality {
            Some(NativePhaseDocument {
                evaluator,
                required_policy,
                ..
            }) => {
                if quality_policies
                    .iter()
                    .any(|policy| !policy.handler.agentic)
                {
                    return Err(
                        InputError::Invalid("quality phase handlers must all be agentic").into(),
                    );
                }
                let evaluator = resolve_participant(evaluator, context)?;
                let required_policy = optional_hash(required_policy.as_ref())?;
                Some(PhasePolicy {
                    evaluator,
                    definition: definition(
                        b"quality",
                        evaluator,
                        &quality_policies,
                        required_policy,
                    ),
                    handlers: &quality_policies,
                    required_policy,
                })
            }
            None => None,
        };
        if document.quality_bar.is_some() != quality.is_some() {
            return Err(InputError::Invalid(
                "quality_bar and the quality phase are declared together",
            )
            .into());
        }
        if agentic == 0 {
            Program::Programmatic { check, quality }
        } else if agentic == check_policies.len() && check_policies.len() == 1 && quality.is_none()
        {
            Program::Agentic { check }
        } else {
            return Err(InputError::Invalid(
                "a check phase is either programmatic handlers (with an optional agentic quality phase) or one agentic handler",
            )
            .into());
        }
    };
    let spec = ValidationSpec {
        ledger: context.ledger,
        id,
        schema: 1,
        claim,
        issuer: context.actor,
        declaration_index: index,
        kind,
        phase,
        mode,
        target,
        program,
        deadline,
        description: &document.description,
        quality_bar: document.quality_bar.as_deref(),
        contributed_by: &contributed_by,
        policy_revision,
    };
    let plan =
        ValidationDescriptor::prepare(Principal::Actor(context.actor), spec, limits.validation)?;
    let charge = plan.construction_charge();
    Ok(plan.build(charge)?)
}
fn claim(
    document: &NativeClaimDocument,
    context: &BuildContext,
    ids: &mut impl IdGenerator,
    resolved: &Resolved,
    limits: &CompileLimits,
    created: &mut Vec<NativeIdentity>,
) -> Result<NativeCommand, CompileError> {
    let ledger = context.ledger;
    text(&document.description, true)?;
    let subject = resolve_participant(&document.target, context)?;
    let action = parse_action(&document.action)?;
    if document.max_responses == 0 {
        return Err(InputError::Invalid("max_responses must be positive").into());
    }
    let claim_id = ClaimId(allocated(document.id.as_deref(), ids)?);
    let occurrence = OccurrenceId(allocated(document.occurrence.as_deref(), ids)?);
    let deadline = document
        .deadline
        .as_ref()
        .map(|value| self::deadline(value, ids))
        .transpose()?;
    let (cause, owner) = match &document.parent {
        Some(parent) => {
            let parent = resolved.claim(ClaimId(parse_id(parent)?))?;
            (
                RelationTarget::Object(ObjectRef::claim(ledger, ClaimId(parent.binding.object.0))),
                Some(Owner {
                    expected: parent.binding,
                    receipt: parent.receipt.map(|(_, fence)| fence),
                }),
            )
        }
        None => (RelationTarget::Root(context.root), None),
    };
    let mut relations = BTreeSet::new();
    for (kind, target) in [
        (
            RelationKind::Issuer,
            RelationTarget::Participant(context.actor),
        ),
        (RelationKind::Subject, RelationTarget::Participant(subject)),
        (RelationKind::ClaimAction, RelationTarget::Action(action)),
        (RelationKind::CausedBy, cause),
    ] {
        relations.insert(Relation { kind, target });
    }
    for relation in &document.relations {
        let relation = authored_relation(&relation.kind, &relation.target, ledger)?;
        if relation.target == RelationTarget::Object(ObjectRef::claim(ledger, claim_id)) {
            return Err(InputError::Invalid("a claim cannot relate to itself").into());
        }
        if !relations.insert(relation) {
            return Err(InputError::Invalid("duplicate relation").into());
        }
    }
    let mut relation_list = Vec::new();
    reserve(&mut relation_list, relations.len())?;
    relation_list.extend(relations);
    // Only a correction invalidates, and it invalidates exactly one challenge
    // while reviewing exactly one verdict artifact (the owner requires the
    // same; refusing here spends no identity).
    let invalidates = relation_list
        .iter()
        .filter(|relation| relation.kind == RelationKind::Invalidates)
        .count();
    let reviewed_evidence = relation_list
        .iter()
        .filter(|relation| {
            relation.kind == RelationKind::Reviews
                && matches!(relation.target, RelationTarget::Evidence(_))
        })
        .count();
    if action == ActionType::Correction {
        if invalidates != 1 || reviewed_evidence != 1 {
            return Err(InputError::Invalid(
                "a correction invalidates exactly one challenge (invalidates: claim:ID) and reviews exactly one verdict artifact (reviews: artifact:ID@HASH)",
            )
            .into());
        }
    } else if invalidates != 0 {
        return Err(InputError::Invalid("only a correction may invalidate a claim").into());
    }
    let mut scopes = BTreeSet::new();
    for scope in &document.scopes {
        if scope.key.is_empty() || scope.key.len() > 1024 || scope.key.contains('\0') {
            return Err(InputError::Invalid("scope key").into());
        }
        if !scopes.insert((parse_scope_kind(&scope.kind)?, scope.key.as_str())) {
            return Err(InputError::Invalid("duplicate scope").into());
        }
    }
    let mut scope_specs = Vec::new();
    reserve(&mut scope_specs, scopes.len())?;
    scope_specs.extend(
        scopes
            .into_iter()
            .map(|(kind, key)| ScopeSpec { kind, key }),
    );
    if document.validations.is_empty() || document.validations.len() > 64 {
        return Err(InputError::Invalid("a claim declares 1..64 validations").into());
    }
    let mut declarations = Vec::new();
    reserve(&mut declarations, document.validations.len())?;
    let mut delivery = None;
    for (index, validation) in document.validations.iter().enumerate() {
        let index = u32::try_from(index).map_err(|_| CompileError::Capacity("declarations"))?;
        if parse_validation_kind(&validation.kind)? == ValidationKind::Receipt {
            if delivery.is_some() {
                return Err(InputError::Invalid("only one receipt (delivery) declaration").into());
            }
            delivery = Some(index);
        }
        declarations.push(declaration(
            index, validation, context, claim_id, ids, limits,
        )?);
    }
    if delivery.is_none() {
        return Err(InputError::Invalid(
            "a claim needs exactly one required receipt (delivery) declaration",
        )
        .into());
    }
    let mut requirements = Vec::new();
    reserve(&mut requirements, declarations.len())?;
    for descriptor in &declarations {
        requirements.push(RequirementRef {
            id: ValidationId(descriptor.binding().object.0),
            specification: descriptor.specification_hash(),
        });
    }
    let validation_of = |index: u32| -> Result<ValidationId, CompileError> {
        declarations
            .get(index as usize)
            .map(|descriptor| ValidationId(descriptor.binding().object.0))
            .ok_or(
                InputError::Invalid("check names a declaration index outside validations").into(),
            )
    };
    let mut slot_checks = Vec::new();
    reserve(&mut slot_checks, document.slots.len())?;
    let mut slot_numbers = BTreeSet::new();
    let mut missing_indices = BTreeSet::new();
    let declared = u32::try_from(document.validations.len())
        .map_err(|_| CompileError::Capacity("declarations"))?;
    for slot in &document.slots {
        if !slot_numbers.insert(slot.slot) {
            return Err(InputError::Invalid("duplicate manifest slot").into());
        }
        let mut checks = Vec::new();
        reserve(&mut checks, slot.checks.len())?;
        for check in &slot.checks {
            checks.push(CheckPolicy {
                declaration_index: check.declaration,
                validation: validation_of(check.declaration)?,
                mode: parse_validation_mode(&check.mode)?,
            });
        }
        // The missing-slot obligation is a virtual declaration index: it must
        // name no authored declaration and be distinct per slot. The default
        // places it after every declaration, offset by the slot number.
        let missing = match slot.missing {
            Some(missing) => missing,
            None => declared
                .checked_add(slot.slot)
                .ok_or(CompileError::Capacity("missing declaration index"))?,
        };
        if missing < declared || !missing_indices.insert(missing) {
            return Err(InputError::Invalid(
                "a slot's missing declaration index must be unused by every declaration and unique per slot",
            )
            .into());
        }
        slot_checks.push((
            slot.slot,
            missing,
            parse_validation_mode(&slot.mode)?,
            checks,
        ));
    }
    let mut slots = Vec::new();
    reserve(&mut slots, slot_checks.len())?;
    for (slot, missing_declaration_index, mode, checks) in &slot_checks {
        slots.push(SlotPolicy {
            slot: *slot,
            missing_declaration_index: *missing_declaration_index,
            mode: *mode,
            checks,
        });
    }
    let policy = document
        .policy
        .as_ref()
        .map(|policy| -> Result<PeerPolicy, CompileError> {
            Ok(PeerPolicy {
                corrective_allowed: policy.corrective_allowed,
                max_follow_ups: policy.max_follow_ups,
                single_issuer: policy.single_issuer,
                escalation: match policy.escalation.as_str() {
                    "none" => Escalation::None,
                    "holder" => Escalation::Holder,
                    "evaluator" => Escalation::Evaluator,
                    _ => {
                        return Err(InputError::Invalid(
                            "policy escalation is none, holder or evaluator",
                        )
                        .into());
                    }
                },
            })
        })
        .transpose()?;
    // A follow-up policy or an exact-evidence relation needs descriptor
    // schema 2; every other claim keeps the frozen schema-1 identity.
    let schema = if policy.is_some()
        || relation_list.iter().any(|relation| {
            matches!(relation.target, RelationTarget::Evidence(_))
                || relation.kind == RelationKind::Invalidates
        }) {
        2
    } else {
        1
    };
    let spec = ClaimSpec {
        ledger,
        id: claim_id,
        schema,
        occurrence,
        description: &document.description,
        relations: &relation_list,
        scopes: &scope_specs,
        requirements: &requirements,
        slots: &slots,
        deadline,
        policy,
    };
    let plan = ClaimDescriptor::prepare(spec, limits.claim)?;
    plan.check_postable()?;
    let charge = plan.construction_charge();
    let content = plan.build(charge)?;
    record(created, NativeIdentityKind::Claim, claim_id.0)?;
    for descriptor in &declarations {
        record(
            created,
            NativeIdentityKind::Validation,
            descriptor.binding().object.0,
        )?;
    }
    let mut claims = Vec::new();
    reserve(&mut claims, 1)?;
    claims.push(NativeAuthoredProposal {
        content,
        declarations,
        max_responses: document.max_responses,
        scope_limits: ScopeLimits {
            scopes: document.scope_limits.scopes as usize,
            roots: document.scope_limits.roots as usize,
            children: document.scope_limits.children as usize,
        },
        owner,
    });
    Ok(NativeCommand::CreateAuthored { claims })
}
