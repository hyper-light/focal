//! Fallible builders for realistic native two-party workflows. They construct
//! typed inputs only; nothing here admits, publishes, or executes anything.
//! Integration tests in other crates (Session, node, CLI) use them so one
//! definition of a claim, its evidence and its evaluation drives every layer.
use super::*;

/// A `Cell` that is `Sync`: test schema verifiers count their calls through
/// it, since a verifier is shared with the materializer's workers.
#[derive(Debug, Default)]
pub struct SyncCell<T>(std::sync::Mutex<T>);
impl<T> SyncCell<T> {
    pub fn new(value: T) -> Self {
        Self(std::sync::Mutex::new(value))
    }
    pub fn get(&self) -> T
    where
        T: Copy,
    {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
    pub fn set(&self, value: T) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
    }
}
use focal_evidence::{error_report_schema, test_report_schema};
use focal_model::lifecycle::creation::Owner;
use focal_model::lifecycle::{
    Principal, aggregation,
    artifact_descriptor::{
        self, ArtifactDescriptor, ArtifactSpec, PayloadSpec, ResultProvenance, WorkProvenance,
        WorkRole,
    },
    claim::ClaimDefinition,
    creation::Proposal,
    evidence::{Parent, SlotBinding},
    graph, scope,
    succession::Lineage,
    validation,
};
use focal_model::{
    ArtifactRef, Cause, Confidence, Deadline, HandlerRef, MonitorId, ObjectId, ObjectRef,
    ObjectRevision, OutcomeKind, ReceiptFence, RequestEpoch, RequestId, RootCommandId, TestamentId,
    TimerId, ValidationKind, ValidationMode, ValidationPhase, ValidatorId, VerdictValue,
    WaitPredicate,
};

/// A well-formed test report accepted by the builtin `test-report` schema.
pub const PROOF: &[u8] = br#"{"passed":3,"failed":0,"skipped":0}"#;
/// A well-formed failing test report.
pub const FAILED_PROOF: &[u8] = br#"{"passed":0,"failed":1,"skipped":0}"#;
/// An evaluator-side diagnostic (tool unavailable).
pub const EVALUATOR_DIAGNOSTIC: &[u8] =
    br#"{"code":"tool_unavailable","message":"The evaluator could not reach its tool."}"#;
/// A respondent-side diagnostic (work failed).
pub const WORK_DIAGNOSTIC: &[u8] =
    br#"{"code":"work_failed","message":"The requested tests could not pass."}"#;

/// The distinct principals of one exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parties {
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub evaluator: ParticipantId,
    pub quality: ParticipantId,
}
impl Parties {
    pub const fn numbered(base: u128) -> Self {
        Self {
            issuer: ParticipantId::from_u128(base),
            subject: ParticipantId::from_u128(base.saturating_add(1)),
            evaluator: ParticipantId::from_u128(base.saturating_add(2)),
            quality: ParticipantId::from_u128(base.saturating_add(3)),
        }
    }
}

pub fn binding(ledger: LedgerId, id: u128) -> Binding {
    Binding {
        ledger,
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
pub fn request(actor: ParticipantId, epoch: u64, id: u128) -> RequestKey {
    RequestKey {
        principal: actor,
        epoch: RequestEpoch(epoch),
        id: RequestId::from_u128(id),
    }
}
pub fn context(actor: ParticipantId, logical_time: u64) -> NativeContext {
    NativeContext {
        principal: Principal::Actor(actor),
        logical_time,
    }
}
pub fn descriptor_limits() -> artifact_descriptor::Limits {
    artifact_descriptor::Limits {
        kind_bytes: 128,
        metadata_bytes: 1024,
        inline_bytes: 65_536,
        inputs: 16,
        visibility_labels: 16,
        visibility_label_bytes: 128,
        construction_bytes: 128 * 1024,
    }
}
fn declaration_limits() -> validation::Limits {
    validation::Limits {
        handlers: 4,
        attempts: 8,
        slot_bytes: 64,
    }
}

/// The mandatory pure Receipt/Delivery requirement at declaration index 0.
pub fn delivery_declaration(
    ledger: LedgerId,
    parties: Parties,
    claim: u128,
    validation: u128,
    at: u64,
) -> Result<validation::Declaration, NativeError> {
    Ok(validation::Declaration::new(
        Principal::Actor(parties.issuer),
        validation::DeclarationSpec {
            binding: binding(ledger, validation),
            claim: ClaimId::from_u128(claim),
            issuer: parties.issuer,
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(validation),
                generation: 1,
                at,
            },
        },
        declaration_limits(),
    )?)
}

/// A whole-work slot check. Programmatic checks name `parties.evaluator`;
/// agentic-only checks name `parties.quality`.
#[allow(clippy::too_many_arguments)] // Every field is an authored fact of the declaration.
pub fn slot_declaration(
    ledger: LedgerId,
    parties: Parties,
    claim: u128,
    validation: u128,
    index: u32,
    slot: u32,
    mode: ValidationMode,
    agentic: bool,
    at: u64,
) -> Result<validation::Declaration, NativeError> {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(77),
        version: ContentHash([77; 32]),
        agentic,
    };
    let handlers = [validation::HandlerPolicy {
        handler: &handler,
        attempts: 2,
        proof_schema: test_report_schema(),
        diagnostic_schema: error_report_schema(),
    }];
    let policy = validation::PhasePolicy {
        evaluator: if agentic {
            parties.quality
        } else {
            parties.evaluator
        },
        definition: ContentHash([78; 32]),
        handlers: &handlers,
        required_policy: None,
    };
    Ok(validation::Declaration::new(
        Principal::Actor(parties.issuer),
        validation::DeclarationSpec {
            binding: binding(ledger, validation),
            claim: ClaimId::from_u128(claim),
            issuer: parties.issuer,
            declaration_index: index,
            kind: ValidationKind::Test,
            phase: ValidationPhase::WholeWork,
            mode,
            target: validation::TargetDeclaration::WholeWorkSlot {
                index: slot,
                name: if slot == 0 { "primary" } else { "secondary" },
            },
            program: if agentic {
                validation::Program::Agentic { check: policy }
            } else {
                validation::Program::Programmatic {
                    check: policy,
                    quality: None,
                }
            },
            deadline: Deadline {
                timer: TimerId::from_u128(validation),
                generation: 1,
                at,
            },
        },
        declaration_limits(),
    )?)
}

/// One work slot of the acceptance manifest and the checks bound to it.
#[derive(Debug, Clone)]
pub struct Slot {
    pub slot: u32,
    pub missing_declaration_index: u32,
    pub mode: ValidationMode,
    pub checks: Vec<aggregation::CheckPolicy>,
}

/// A root claim with its complete acceptance manifest. `declarations` must
/// contain every declaration referenced by `slots` plus the delivery check.
pub fn creation(
    ledger: LedgerId,
    parties: Parties,
    request: RequestKey,
    claim: u128,
    declarations: Vec<validation::Declaration>,
    slots: &[Slot],
) -> Result<NativeInput, NativeError> {
    let binding = binding(ledger, claim);
    let policies: Vec<aggregation::SlotPolicy<'_>> = slots
        .iter()
        .map(|slot| aggregation::SlotPolicy {
            slot: slot.slot,
            missing_declaration_index: slot.missing_declaration_index,
            mode: slot.mode,
            checks: &slot.checks,
        })
        .collect();
    let acceptance = aggregation::AcceptancePolicy::new(
        binding,
        parties.issuer,
        &policies,
        &declarations,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 32,
        },
    )?;
    let proposal = Proposal {
        definition: ClaimDefinition {
            binding,
            issuer: parties.issuer,
            subject: parties.subject,
            deadline: None,
            max_responses: 4,
            // Deliberately untrusted: only the owner assigns a creation position.
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(claim))?,
            acceptance,
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 16,
                children: 8,
            },
        },
        owner: None,
    };
    Ok(NativeInput {
        request,
        command: NativeCommand::Create {
            claims: vec![proposal],
            declarations,
        },
    })
}
pub fn post(request: RequestKey, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::Post { expected },
    }
}
pub fn cancel(request: RequestKey, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::Cancel { expected },
    }
}
pub fn acquire_receipt(request: RequestKey, expected: Binding, receipt: u128) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::AcquireReceipt {
            expected,
            receipt: ReceiptId::from_u128(receipt),
        },
    }
}

/// Authored descriptor content beyond the fixed provenance of a work artifact.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactShape<'a> {
    pub metadata: &'a [u8],
    pub inputs: &'a [ObjectRef],
    pub visibility: &'a [&'a str],
}
impl ArtifactShape<'static> {
    pub const MINIMAL: Self = Self {
        metadata: b"{}",
        inputs: &[],
        visibility: &[],
    };
}
fn work_spec<'a>(
    ledger: LedgerId,
    id: u128,
    producer: ParticipantId,
    parent: &Parent,
    role: WorkRole,
    payload: &'a [u8],
    shape: ArtifactShape<'a>,
) -> ArtifactSpec<'a> {
    let diagnostic = matches!(role, WorkRole::Diagnostic { .. });
    ArtifactSpec {
        ledger,
        id: ArtifactId::from_u128(id),
        schema: 1,
        kind: if diagnostic { "error" } else { "test-report" },
        schema_hash: if diagnostic {
            error_report_schema()
        } else {
            test_report_schema()
        },
        metadata: shape.metadata,
        payload: PayloadSpec::Inline(payload),
        producer,
        receipt: Some(parent.receipt),
        result: None,
        work: Some(WorkProvenance {
            claim: parent.claim,
            cycle: parent.next_cycle,
            role,
        }),
        inputs: shape.inputs,
        visibility: shape.visibility,
    }
}
fn build(spec: ArtifactSpec<'_>) -> Result<ArtifactDescriptor, NativeError> {
    Ok(ArtifactDescriptor::prepare(spec, descriptor_limits())?.build()?)
}
pub fn artifact_ref(descriptor: &ArtifactDescriptor) -> ArtifactRef {
    ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    }
}
/// The respondent's work output for `slot` under its current receipt and cycle.
pub fn work_artifact(
    ledger: LedgerId,
    id: u128,
    parent: &Parent,
    slot: u32,
    payload: &[u8],
) -> Result<(NativeArtifactInput, SlotBinding), NativeError> {
    work_artifact_shaped(ledger, id, parent, slot, payload, ArtifactShape::MINIMAL)
}
/// A work output carrying authored metadata, inputs and visibility labels.
pub fn work_artifact_shaped(
    ledger: LedgerId,
    id: u128,
    parent: &Parent,
    slot: u32,
    payload: &[u8],
    shape: ArtifactShape<'_>,
) -> Result<(NativeArtifactInput, SlotBinding), NativeError> {
    let descriptor = build(work_spec(
        ledger,
        id,
        parent.holder,
        parent,
        WorkRole::Output { slot },
        payload,
        shape,
    ))?;
    let binding = SlotBinding {
        slot,
        artifact: artifact_ref(&descriptor),
    };
    Ok((NativeArtifactInput::new(descriptor)?, binding))
}
/// The respondent's diagnostic for failed or impossible work.
pub fn diagnostic_artifact(
    ledger: LedgerId,
    id: u128,
    parent: &Parent,
    reason: EvidenceFailure,
    payload: &[u8],
) -> Result<(NativeArtifactInput, ArtifactRef), NativeError> {
    diagnostic_artifact_shaped(ledger, id, parent, reason, payload, ArtifactShape::MINIMAL)
}
/// A diagnostic carrying authored metadata, inputs and visibility labels.
pub fn diagnostic_artifact_shaped(
    ledger: LedgerId,
    id: u128,
    parent: &Parent,
    reason: EvidenceFailure,
    payload: &[u8],
    shape: ArtifactShape<'_>,
) -> Result<(NativeArtifactInput, ArtifactRef), NativeError> {
    let descriptor = build(work_spec(
        ledger,
        id,
        parent.holder,
        parent,
        WorkRole::Diagnostic { reason },
        payload,
        shape,
    ))?;
    let reference = artifact_ref(&descriptor);
    Ok((NativeArtifactInput::new(descriptor)?, reference))
}
pub fn submit_work(
    request: RequestKey,
    claim: Binding,
    slot: u32,
    artifact: NativeArtifactInput,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::SubmitWork {
            claim,
            slot,
            artifact,
        },
    }
}
pub fn submit_diagnostic(
    request: RequestKey,
    claim: Binding,
    reason: EvidenceFailure,
    artifact: NativeArtifactInput,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact,
        },
    }
}
/// The respondent's explicit authored testimony for one work cycle.
#[allow(clippy::too_many_arguments)] // Every field is respondent-authored content.
pub fn close_response(
    ledger: LedgerId,
    request: RequestKey,
    claim: Binding,
    response: u128,
    summary: &str,
    confidence: Confidence,
    outcome: OutcomeKind,
    manifest: Vec<SlotBinding>,
    diagnostics: Vec<ArtifactRef>,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::CloseResponse {
            claim,
            response: binding(ledger, response),
            report: NativeResponseInput {
                summary: summary.into(),
                confidence,
                outcome,
                manifest,
                diagnostics,
            },
        },
    }
}
pub fn post_response(request: RequestKey, claim: Binding, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::PostResponse { claim, expected },
    }
}
pub fn receive_response(request: RequestKey, claim: Binding, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::ReceiveResponse { claim, expected },
    }
}
pub fn enter_whole_work(request: RequestKey, claim: Binding, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::EnterWholeWork { claim, expected },
    }
}
pub fn work_key(
    claim: u128,
    validation: u128,
    response: u128,
    slot: u32,
    artifact: ArtifactId,
    generation: u64,
) -> EvaluationKey {
    EvaluationKey {
        claim: ClaimId::from_u128(claim),
        validation: ValidationId::from_u128(validation),
        target: EvaluationTarget::Work {
            response: TestamentId::from_u128(response),
            slot,
            artifact,
        },
        generation,
    }
}
pub fn begin_work(
    request: RequestKey,
    claim: Binding,
    key: EvaluationKey,
    expected: Binding,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::BeginWork {
            claim,
            key,
            expected,
        },
    }
}
/// The evaluator's fenced report for a begun whole-work check, with its typed
/// result artifact bound to the exact target, generation and attempt.
#[allow(clippy::too_many_arguments)] // Every field pins the report to the exact evaluation.
pub fn report_work(
    ledger: LedgerId,
    request: RequestKey,
    artifact_id: u128,
    claim: Binding,
    key: EvaluationKey,
    state: &validation::EvaluationState,
    definition: &validation::Declaration,
    value: VerdictValue,
    payload: &[u8],
) -> Result<NativeInput, NativeError> {
    report_work_shaped(
        ledger,
        request,
        artifact_id,
        claim,
        key,
        state,
        definition,
        value,
        payload,
        ArtifactShape::MINIMAL,
    )
}
/// A whole-work report whose result artifact carries authored metadata,
/// inputs and visibility labels; results must carry every label the work
/// evidence requires.
#[allow(clippy::too_many_arguments)] // Every field pins the report to the exact evaluation.
pub fn report_work_shaped(
    ledger: LedgerId,
    request: RequestKey,
    artifact_id: u128,
    claim: Binding,
    key: EvaluationKey,
    state: &validation::EvaluationState,
    definition: &validation::Declaration,
    value: VerdictValue,
    payload: &[u8],
    shape: ArtifactShape<'_>,
) -> Result<NativeInput, NativeError> {
    let attempt = state.bind(definition)?.current_attempt()?;
    if request.principal != attempt.evaluator {
        return Err(ContractError::WrongActor.into());
    }
    let diagnostic = matches!(value, VerdictValue::Error | VerdictValue::Incomplete);
    let spec = ArtifactSpec {
        ledger,
        id: ArtifactId::from_u128(artifact_id),
        schema: 1,
        kind: if diagnostic { "error" } else { "test-report" },
        schema_hash: if diagnostic {
            error_report_schema()
        } else {
            test_report_schema()
        },
        metadata: shape.metadata,
        payload: PayloadSpec::Inline(payload),
        producer: attempt.evaluator,
        receipt: state.receipt(),
        result: Some(ResultProvenance {
            claim: key.claim,
            validation: key.validation,
            target: state.target(),
            generation: state.generation(),
            attempt,
            value,
        }),
        work: None,
        inputs: shape.inputs,
        visibility: shape.visibility,
    };
    let descriptor = build(spec)?;
    let evidence = artifact_ref(&descriptor);
    Ok(NativeInput {
        request,
        command: NativeCommand::ReportWork {
            claim,
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence,
            },
            artifact: NativeArtifactInput::new(descriptor)?,
        },
    })
}

/// A child claim caused by `parent`, owned under the parent's current binding
/// and, once the parent is received, its current receipt.
#[allow(clippy::too_many_arguments)] // Every field is an authored fact of the child.
pub fn child_creation(
    ledger: LedgerId,
    parties: Parties,
    request: RequestKey,
    claim: u128,
    parent: Binding,
    receipt: Option<ReceiptFence>,
    declarations: Vec<validation::Declaration>,
    slots: &[Slot],
) -> Result<NativeInput, NativeError> {
    let mut input = creation(ledger, parties, request, claim, declarations, slots)?;
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        return Err(ContractError::InvalidTransition.into());
    };
    let proposal = claims.first_mut().ok_or(ContractError::InvalidTransition)?;
    proposal.definition.lineage = Lineage::new(
        proposal.definition.binding,
        Cause::Claim(ClaimId(parent.object.0)),
        &[],
        1,
    )?;
    proposal.owner = Some(Owner {
        expected: parent,
        receipt,
    });
    Ok(input)
}
/// The holder's wait monitor over `roots`, registered under `expected`.
pub fn register_monitor(
    request: RequestKey,
    expected: Binding,
    receipt: Option<ReceiptFence>,
    id: u128,
    roots: Vec<WaitPredicate>,
    deadline: Deadline,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::RegisterMonitor {
            expected,
            receipt,
            id: MonitorId::from_u128(id),
            roots,
            deadline,
        },
    }
}
/// A replacement holder adopting the claim from `previous`.
pub fn adopt_receipt(
    request: RequestKey,
    expected: Binding,
    previous: ReceiptFence,
    receipt: u128,
    holder: ParticipantId,
) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::AdoptReceipt {
            expected,
            previous,
            receipt: ReceiptId::from_u128(receipt),
            holder,
        },
    }
}
pub fn generate_result_testament(request: RequestKey, claim: Binding, id: u128) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::GenerateResultTestament {
            claim,
            id: TestamentId::from_u128(id),
        },
    }
}
pub fn post_result_testament(request: RequestKey, expected: Binding) -> NativeInput {
    NativeInput {
        request,
        command: NativeCommand::PostResultTestament { expected },
    }
}
/// The trusted timer input for the evaluation's currently bound deadline.
pub fn evaluation_deadline(
    view: &NativeView<'_>,
    evaluation: EvaluationKey,
) -> Result<NativeDeadlineInput, NativeError> {
    let state = view
        .evaluation(evaluation)
        .ok_or(ContractError::InvalidTarget)?;
    let definition = view
        .definition(evaluation.validation)
        .ok_or(ContractError::InvalidTarget)?;
    Ok(NativeDeadlineInput {
        evaluation,
        deadline: state.bind(definition)?.deadline(),
    })
}

/// A claim whose graph declares obligations (DependsOn/Awaits) on other claims.
#[allow(clippy::too_many_arguments)] // Every field is an authored fact of the claim.
pub fn creation_with_graph(
    ledger: LedgerId,
    parties: Parties,
    request: RequestKey,
    claim: u128,
    obligations: &[graph::Obligation],
    declarations: Vec<validation::Declaration>,
    slots: &[Slot],
) -> Result<NativeInput, NativeError> {
    let mut input = creation(ledger, parties, request, claim, declarations, slots)?;
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        return Err(ContractError::InvalidTransition.into());
    };
    let proposal = claims.first_mut().ok_or(ContractError::InvalidTransition)?;
    proposal.definition.graph = graph::Declaration::new(obligations, 16)?;
    Ok(input)
}
