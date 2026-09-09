//! Ledger objects an operation must observe before it can be compiled, and
//! the typed bindings extracted from one fixed-prefix read of those objects.
use crate::CompileError;
use focal_client::input::parse_id;
use focal_client::operations::NativeAuthoredOperation;
use focal_core::native::{EvaluationKey, EvaluationTarget};
use focal_model::lifecycle::evidence::EvidenceFailure;
use focal_model::lifecycle::{Binding, validation};
use focal_model::*;
use focal_wire::{
    NativeAttempt, NativeBinding, NativeEvaluation, NativeEvaluationTarget, NativeEvidenceFailure,
    NativeObject, NativeObjectRef, NativePhase, NativeTarget, NativeValidationState,
    NativeWorkArtifactState,
};

/// Which current evaluation of a declaration a verb addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationSelector {
    /// The whole-work evaluation of a manifest slot (any slot when `None`).
    WholeWork {
        slot: Option<u32>,
    },
    Admission,
    /// The increment evaluation of one work artifact (any when `None`).
    Increment {
        artifact: Option<ArtifactId>,
    },
}
impl EvaluationSelector {
    /// `phase` is `whole_work`, `admission` or `increment`; `target` names the
    /// increment's work artifact.
    pub fn parse(
        phase: &str,
        slot: Option<u32>,
        target: Option<&str>,
    ) -> Result<Self, CompileError> {
        use focal_client::input::InputError;
        match (phase, slot, target) {
            ("whole_work", slot, None) => Ok(Self::WholeWork { slot }),
            ("admission", None, None) => Ok(Self::Admission),
            ("increment", None, target) => Ok(Self::Increment {
                artifact: target.map(parse_id).transpose()?.map(ArtifactId),
            }),
            ("whole_work" | "admission", _, Some(_)) => Err(InputError::Invalid(
                "target names an increment's work artifact; it needs phase increment",
            )
            .into()),
            ("admission" | "increment", Some(_), _) => Err(InputError::Invalid(
                "slot selects a whole-work evaluation; it cannot combine with this phase",
            )
            .into()),
            _ => {
                Err(InputError::Invalid("phase must be whole_work, admission or increment").into())
            }
        }
    }
}

/// One read the host performs at a fixed prefix before compiling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requirement {
    Objects(Vec<NativeObjectRef>),
    Evaluations { validation: ValidationId },
}

/// The reads an operation needs, in order. Creation of a root claim needs
/// nothing; every other verb binds to committed objects it names.
pub fn requirements(operation: &NativeAuthoredOperation) -> Result<Vec<Requirement>, CompileError> {
    use NativeAuthoredOperation as Op;
    let claim =
        |value: &str| Ok::<_, CompileError>(NativeObjectRef::Claim(ClaimId(parse_id(value)?)));
    Ok(match operation {
        Op::ClaimSubmit(document) => match &document.parent {
            Some(parent) => vec![Requirement::Objects(vec![claim(parent)?])],
            None => Vec::new(),
        },
        Op::ClaimChallenge(document) => {
            let mut references = Vec::new();
            if let Some(parent) = &document.parent {
                references.push(claim(parent)?);
            }
            if let Some(artifact) = &document.artifact
                && !artifact.contains('@')
            {
                references.push(NativeObjectRef::Artifact(ArtifactId(parse_id(artifact)?)));
            }
            if references.is_empty() {
                Vec::new()
            } else {
                vec![Requirement::Objects(references)]
            }
        }
        Op::ClaimConsult(document) => match &document.parent {
            Some(parent) => vec![Requirement::Objects(vec![claim(parent)?])],
            None => Vec::new(),
        },
        Op::ClaimCorrect(document) => {
            let mut references = vec![claim(&document.challenge)?];
            let verdict = document
                .verdict
                .split_once('@')
                .map_or(document.verdict.as_str(), |(id, _)| id);
            references.push(NativeObjectRef::Artifact(ArtifactId(parse_id(verdict)?)));
            if let Some(parent) = &document.parent {
                references.push(claim(parent)?);
            }
            vec![Requirement::Objects(references)]
        }
        Op::ClaimFollowUp(document) => {
            let mut references = vec![claim(&document.refines)?];
            if let Some(parent) = &document.parent {
                references.push(claim(parent)?);
            }
            vec![Requirement::Objects(references)]
        }
        Op::ClaimPost(document) | Op::ClaimCancel(document) => {
            vec![Requirement::Objects(vec![claim(&document.claim)?])]
        }
        Op::ReceiptAcquire(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
        Op::ArtifactSubmit(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
        Op::ArtifactDiagnostic(document) => {
            vec![Requirement::Objects(vec![claim(&document.claim)?])]
        }
        Op::TestamentSubmit(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
        Op::TestamentPost(document) | Op::TestamentReceive(document) => {
            vec![Requirement::Objects(vec![
                claim(&document.claim)?,
                NativeObjectRef::Response(TestamentId(parse_id(&document.testament)?)),
            ])]
        }
        Op::ValidationBegin(document) => vec![
            Requirement::Objects(vec![claim(&document.claim)?]),
            Requirement::Evaluations {
                validation: ValidationId(parse_id(&document.validation)?),
            },
        ],
        Op::ValidationReport(document) => vec![
            Requirement::Objects(vec![claim(&document.claim)?]),
            Requirement::Evaluations {
                validation: ValidationId(parse_id(&document.validation)?),
            },
        ],
        Op::ClaimReleaseScope(document) | Op::ValidationSealIncrements(document) => {
            vec![Requirement::Objects(vec![claim(&document.claim)?])]
        }
        Op::ReceiptAdopt(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
        Op::ArtifactFail(document) => vec![Requirement::Objects(vec![
            claim(&document.claim)?,
            NativeObjectRef::Diagnostic(ArtifactId(parse_id(&document.diagnostic)?)),
        ])],
        Op::ArtifactReceive(document) => vec![Requirement::Objects(vec![
            claim(&document.claim)?,
            NativeObjectRef::Work(ArtifactId(parse_id(&document.artifact)?)),
        ])],
        Op::ArtifactReject(document) => {
            let artifact = ArtifactId(parse_id(&document.artifact)?);
            vec![Requirement::Objects(vec![
                claim(&document.claim)?,
                NativeObjectRef::Work(artifact),
                NativeObjectRef::Artifact(artifact),
            ])]
        }
        Op::ValidationEnterWholeWork(document) => {
            vec![Requirement::Objects(vec![
                claim(&document.claim)?,
                NativeObjectRef::Response(TestamentId(parse_id(&document.testament)?)),
            ])]
        }
        Op::AuditGenerate(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
        Op::AuditPost(document) => vec![Requirement::Objects(vec![
            NativeObjectRef::ResultTestament(TestamentId(parse_id(&document.testament)?)),
        ])],
        Op::MonitorRegister(document) => {
            vec![Requirement::Objects(vec![claim(&document.claim)?])]
        }
        Op::MonitorRebind(document) => vec![Requirement::Objects(vec![
            claim(&document.claim)?,
            claim(&document.predecessor)?,
            claim(&document.successor)?,
        ])],
        Op::MonitorCancel(document) => vec![Requirement::Objects(vec![claim(&document.claim)?])],
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedClaim {
    pub binding: Binding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub status: ClaimStatus,
    /// The current entitlement: holder and fence.
    pub receipt: Option<(ParticipantId, ReceiptFence)>,
    pub response_count: u32,
    pub latest_response: Option<TestamentId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedResponse {
    pub binding: Binding,
    pub claim: ClaimId,
    pub cycle: u32,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedEvaluation {
    pub binding: Binding,
    pub key: EvaluationKey,
    pub target: validation::Target,
    pub receipt: Option<ReceiptFence>,
    pub evaluator: Option<ParticipantId>,
    pub attempt: Option<validation::Attempt>,
    pub has_begun: bool,
    pub terminal: bool,
}

/// A work artifact of a claim (a generated output or a recorded failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedWork {
    pub binding: Binding,
    pub reference: ArtifactRef,
    pub claim: ClaimId,
    pub cycle: u32,
    pub slot: u32,
    pub state: NativeWorkArtifactState,
    pub producer: ParticipantId,
    pub receipt: ReceiptFence,
    pub attached: bool,
}
/// A committed diagnostic artifact and the failure it records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedDiagnostic {
    pub reference: ArtifactRef,
    pub claim: ClaimId,
    pub cycle: u32,
    pub producer: ParticipantId,
    pub receipt: ReceiptFence,
    pub reason: Option<EvidenceFailure>,
}
/// An artifact descriptor's identity and the visibility a rejection inherits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedArtifact {
    pub id: ArtifactId,
    pub content_hash: ContentHash,
    pub visibility: Vec<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedResultTestament {
    pub binding: Binding,
    pub claim: ClaimId,
    pub posted: bool,
}

/// Typed bindings extracted from the objects the host read. Every binding is
/// exactly what the ledger reported; the owner fences stale values itself.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    pub claims: Vec<ResolvedClaim>,
    pub responses: Vec<ResolvedResponse>,
    pub evaluations: Vec<ResolvedEvaluation>,
    pub works: Vec<ResolvedWork>,
    pub diagnostics: Vec<ResolvedDiagnostic>,
    pub artifacts: Vec<ResolvedArtifact>,
    pub result_testaments: Vec<ResolvedResultTestament>,
}
impl Resolved {
    /// Missing objects are skipped here and reported by the verb that needs
    /// them, so a page containing unrelated objects is still usable.
    pub fn from_objects(ledger: LedgerId, objects: &[NativeObject]) -> Result<Self, CompileError> {
        let mut resolved = Self::default();
        for object in objects {
            match object {
                NativeObject::Claim(claim) => {
                    if claim.binding.object.is_zero() {
                        return Err(CompileError::Unsupported("zero claim binding"));
                    }
                    push(
                        &mut resolved.claims,
                        ResolvedClaim {
                            binding: binding(ledger, claim.binding),
                            issuer: claim.issuer,
                            subject: claim.subject,
                            status: claim.status,
                            receipt: claim.receipt.map(|receipt| (receipt.holder, receipt.fence)),
                            response_count: claim.response_count,
                            latest_response: claim.latest_response.map(|link| link.testament),
                        },
                    )?;
                }
                NativeObject::Response(response) => push(
                    &mut resolved.responses,
                    ResolvedResponse {
                        binding: binding(ledger, response.binding),
                        claim: response.claim,
                        cycle: response.cycle,
                    },
                )?,
                NativeObject::Evaluation(evaluation) => push(
                    &mut resolved.evaluations,
                    resolved_evaluation(ledger, evaluation),
                )?,
                NativeObject::Work(work) => push(
                    &mut resolved.works,
                    ResolvedWork {
                        binding: binding(ledger, work.binding),
                        reference: work.reference,
                        claim: work.claim,
                        cycle: work.cycle,
                        slot: work.slot,
                        state: work.state,
                        producer: work.producer,
                        receipt: work.receipt,
                        attached: work.attachment.is_some(),
                    },
                )?,
                NativeObject::Diagnostic(diagnostic) => push(
                    &mut resolved.diagnostics,
                    ResolvedDiagnostic {
                        reference: diagnostic.reference,
                        claim: diagnostic.claim,
                        cycle: diagnostic.cycle,
                        producer: diagnostic.producer,
                        receipt: diagnostic.receipt,
                        reason: diagnostic.diagnostic.map(|value| failure(value.reason)),
                    },
                )?,
                NativeObject::Artifact(artifact) => {
                    let mut visibility = Vec::new();
                    visibility
                        .try_reserve_exact(artifact.visibility.len())
                        .map_err(|_| CompileError::Capacity("resolved objects"))?;
                    for label in &artifact.visibility {
                        visibility.push(label.clone());
                    }
                    push(
                        &mut resolved.artifacts,
                        ResolvedArtifact {
                            id: artifact.id,
                            content_hash: artifact.content_hash,
                            visibility,
                        },
                    )?;
                }
                NativeObject::ResultTestament(testament) => push(
                    &mut resolved.result_testaments,
                    ResolvedResultTestament {
                        binding: binding(ledger, testament.binding),
                        claim: testament.claim,
                        posted: matches!(
                            testament.state,
                            focal_wire::NativeResultTestamentState::Posted
                        ),
                    },
                )?,
                _ => {}
            }
        }
        Ok(resolved)
    }
    pub fn work(&self, id: ArtifactId) -> Result<&ResolvedWork, CompileError> {
        self.works
            .iter()
            .find(|work| work.reference.id == id)
            .ok_or(CompileError::Missing("work artifact"))
    }
    pub fn diagnostic(&self, id: ArtifactId) -> Result<&ResolvedDiagnostic, CompileError> {
        self.diagnostics
            .iter()
            .find(|diagnostic| diagnostic.reference.id == id)
            .ok_or(CompileError::Missing("diagnostic"))
    }
    pub fn artifact(&self, id: ArtifactId) -> Result<&ResolvedArtifact, CompileError> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.id == id)
            .ok_or(CompileError::Missing("artifact"))
    }
    pub fn result_testament(
        &self,
        id: TestamentId,
    ) -> Result<&ResolvedResultTestament, CompileError> {
        self.result_testaments
            .iter()
            .find(|testament| testament.binding.object.0 == id.0)
            .ok_or(CompileError::Missing("result testament"))
    }
    pub fn claim(&self, id: ClaimId) -> Result<&ResolvedClaim, CompileError> {
        self.claims
            .iter()
            .find(|claim| claim.binding.object.0 == id.0)
            .ok_or(CompileError::Missing("claim"))
    }
    pub fn response(&self, id: TestamentId) -> Result<&ResolvedResponse, CompileError> {
        self.responses
            .iter()
            .find(|response| response.binding.object.0 == id.0)
            .ok_or(CompileError::Missing("response"))
    }
    /// The current evaluation of `validation` under `claim` the selector
    /// names: the highest non-terminal generation of that phase, optionally
    /// restricted to one slot or one increment target.
    pub fn evaluation(
        &self,
        claim: ClaimId,
        validation: ValidationId,
        selector: EvaluationSelector,
    ) -> Result<&ResolvedEvaluation, CompileError> {
        let mut selected: Option<&ResolvedEvaluation> = None;
        for evaluation in &self.evaluations {
            if evaluation.key.claim != claim
                || evaluation.key.validation != validation
                || evaluation.terminal
            {
                continue;
            }
            let matches = match (selector, evaluation.key.target) {
                (EvaluationSelector::WholeWork { slot: None }, EvaluationTarget::Work { .. }) => {
                    true
                }
                (
                    EvaluationSelector::WholeWork { slot: Some(wanted) },
                    EvaluationTarget::Work { slot, .. },
                ) => slot == wanted,
                (EvaluationSelector::Admission, EvaluationTarget::Admission) => true,
                (
                    EvaluationSelector::Increment { artifact: wanted },
                    EvaluationTarget::Increment { artifact },
                ) => wanted.is_none_or(|wanted| wanted == artifact),
                _ => false,
            };
            if !matches {
                continue;
            }
            match selected {
                Some(current) if current.key.generation > evaluation.key.generation => {}
                Some(current) if current.key.generation == evaluation.key.generation => {
                    return Err(CompileError::Unsupported(
                        "several current evaluations match; name the slot or target",
                    ));
                }
                _ => selected = Some(evaluation),
            }
        }
        selected.ok_or(CompileError::Missing("current evaluation"))
    }
}
fn push<T>(into: &mut Vec<T>, value: T) -> Result<(), CompileError> {
    into.try_reserve(1)
        .map_err(|_| CompileError::Capacity("resolved objects"))?;
    into.push(value);
    Ok(())
}
pub(crate) fn binding(ledger: LedgerId, value: NativeBinding) -> Binding {
    Binding {
        ledger,
        object: value.object,
        content: value.content,
        revision: value.revision,
    }
}
fn evaluation_target(value: NativeEvaluationTarget) -> EvaluationTarget {
    match value {
        NativeEvaluationTarget::Admission => EvaluationTarget::Admission,
        NativeEvaluationTarget::Increment { artifact } => EvaluationTarget::Increment { artifact },
        NativeEvaluationTarget::Work {
            response,
            slot,
            artifact,
        } => EvaluationTarget::Work {
            response,
            slot,
            artifact,
        },
        NativeEvaluationTarget::MissingSlot { response, slot } => {
            EvaluationTarget::MissingSlot { response, slot }
        }
        NativeEvaluationTarget::Delivery { response } => EvaluationTarget::Delivery { response },
    }
}
fn target(ledger: LedgerId, value: &NativeTarget) -> validation::Target {
    match *value {
        NativeTarget::Artifact {
            response,
            slot,
            artifact,
        } => validation::Target::Artifact {
            response: binding(ledger, response),
            slot,
            artifact: binding(ledger, artifact),
        },
        NativeTarget::MissingSlot { response, slot } => validation::Target::MissingSlot {
            response: binding(ledger, response),
            slot,
        },
        NativeTarget::Delivery { response } => validation::Target::Delivery {
            response: binding(ledger, response),
        },
        NativeTarget::Admission { claim } => validation::Target::Admission {
            claim: binding(ledger, claim),
        },
        NativeTarget::Increment { claim, artifact } => validation::Target::Increment {
            claim: binding(ledger, claim),
            artifact: binding(ledger, artifact),
        },
    }
}
fn phase(value: NativePhase) -> validation::Phase {
    match value {
        NativePhase::Programmatic => validation::Phase::Programmatic,
        NativePhase::Quality => validation::Phase::Quality,
        NativePhase::Delivery => validation::Phase::Delivery,
        NativePhase::MissingTarget => validation::Phase::MissingTarget,
    }
}
fn attempt(value: NativeAttempt) -> validation::Attempt {
    validation::Attempt {
        phase: phase(value.phase),
        index: value.index,
        handler: value.handler,
        version: value.version,
        evaluator: value.evaluator,
        definition: value.definition,
    }
}
fn failure(value: NativeEvidenceFailure) -> EvidenceFailure {
    match value {
        NativeEvidenceFailure::Work => EvidenceFailure::Work,
        NativeEvidenceFailure::Production => EvidenceFailure::Production,
        NativeEvidenceFailure::Structure => EvidenceFailure::Structure,
        NativeEvidenceFailure::Metadata => EvidenceFailure::Metadata,
    }
}
fn terminal(state: NativeValidationState) -> bool {
    !matches!(
        state,
        NativeValidationState::Ready
            | NativeValidationState::Validating
            | NativeValidationState::ValidatingQualityBar
    )
}
fn resolved_evaluation(ledger: LedgerId, evaluation: &NativeEvaluation) -> ResolvedEvaluation {
    ResolvedEvaluation {
        binding: binding(ledger, evaluation.binding),
        key: EvaluationKey {
            claim: evaluation.key.claim,
            validation: evaluation.key.validation,
            target: evaluation_target(evaluation.key.target),
            generation: evaluation.key.generation,
        },
        target: target(ledger, &evaluation.target),
        receipt: evaluation.receipt,
        evaluator: evaluation.evaluator,
        attempt: evaluation.current_attempt.map(attempt),
        has_begun: evaluation.has_begun,
        terminal: terminal(evaluation.state),
    }
}
