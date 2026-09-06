//! A resumable, deterministic example using actual custody, validators and Raft.
use crate::embedded::{EmbeddedNode, NodeError, NodeIdentity};
use focal_evidence::{Registration, Registry, TestReportValidator, UploadId, test_report_schema};
use focal_ledger::Submission;
use focal_model::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemoReport {
    pub ledger: LedgerId,
    pub claim: ClaimId,
    pub status: ClaimStatus,
    pub sequence: SessionSeq,
    pub artifact: ContentRef,
    pub validation: VerdictValue,
    pub history: Vec<StatusFact>,
}

fn demo_id(identity: &NodeIdentity, name: &str) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"focal.demo.v1\0");
    hasher.update(&identity.ledger.session.0);
    hasher.update(name.as_bytes());
    let mut bytes = [0; 16];
    for (target, source) in bytes.iter_mut().zip(hasher.finalize().as_bytes()) {
        *target = *source;
    }
    bytes
}
fn handler() -> HandlerRef {
    HandlerRef {
        id: ValidatorId::from_u128(2),
        version: ContentHash(*blake3::hash(b"focal.builtin.test-report.v1").as_bytes()),
        agentic: false,
    }
}
pub(crate) fn request(
    identity: &NodeIdentity,
    label: &str,
    actor: ParticipantId,
    command: Command,
    evidence: Vec<EvidenceAttestation>,
) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: identity.ledger,
        principal: actor,
        request_epoch: RequestEpoch(1),
        request_id: RequestId(demo_id(identity, label)),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(identity.root),
            policy_revision: 1,
            logical_time: 0,
            evidence,
        },
        command,
    }
}
fn submit(
    node: &mut EmbeddedNode,
    label: &str,
    actor: ParticipantId,
    command: Command,
    evidence: Vec<EvidenceAttestation>,
) -> Result<MutationReceipt, NodeError> {
    let input = request(&node.identity, label, actor, command, evidence);
    match node.session.submit_local(&input)? {
        Submission::Committed(receipt) => Ok(receipt),
        Submission::Domain(outcome) => Err(NodeError::Domain(outcome.to_string())),
        Submission::Pending(_) => Err(focal_ledger::LedgerError::OutcomeUnknown.into()),
    }
}
pub(crate) fn claim(identity: &NodeIdentity, id: ClaimId) -> Result<NewClaim, NodeError> {
    let mut validations = Vec::new();
    for (label, kind, handlers, schemas) in [
        (
            "receipt-validation",
            ValidationKind::Receipt,
            Vec::new(),
            BTreeSet::new(),
        ),
        (
            "test-validation",
            ValidationKind::Test,
            vec![handler()],
            BTreeSet::from([test_report_schema()]),
        ),
    ] {
        validations.push(NewValidation {
            id: ValidationId(demo_id(identity, label)),
            content: ValidationContent {
                ledger: identity.ledger,
                schema: SCHEMA_MAJOR,
                claim: id,
                kind,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                description: label.into(),
                quality_bar: None,
                evaluator: identity.evaluator,
                handlers,
                evidence_schemas: schemas,
                contributed_by: BTreeSet::from([identity.issuer]),
                policy_revision: 1,
            },
        });
    }
    Ok(NewClaim {
        id,
        content: ClaimContent {
            ledger: identity.ledger,
            schema: SCHEMA_MAJOR,
            occurrence: OccurrenceId(demo_id(identity, "occurrence")),
            description: "Provide a durable passing test report".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(identity.issuer),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(identity.worker),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(identity.root),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: validations
                .iter()
                .map(|v| {
                    Ok(RequirementRef {
                        id: v.id,
                        specification: v.content.specification_hash()?,
                    })
                })
                .collect::<Result<_, CanonicalError>>()?,
            deadline: None,
        },
        validations,
    })
}

pub fn run(node: &mut EmbeddedNode) -> Result<DemoReport, NodeError> {
    let identity = node.identity.clone();
    for (name, actor) in [
        ("issuer-epoch", identity.issuer),
        ("worker-epoch", identity.worker),
        ("evaluator-epoch", identity.evaluator),
    ] {
        submit(
            node,
            name,
            actor,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
            vec![],
        )?;
    }
    let id = ClaimId(demo_id(&identity, "claim"));
    submit(
        node,
        "generate",
        identity.issuer,
        Command::GenerateClaim {
            claim: claim(&identity, id)?,
        },
        vec![],
    )?;
    submit(
        node,
        "post",
        identity.issuer,
        Command::PostClaim { claim: id },
        vec![],
    )?;
    let receipt = ReceiptFence {
        receipt: ReceiptId(demo_id(&identity, "receipt")),
        epoch: 1,
    };
    submit(
        node,
        "receive",
        identity.worker,
        Command::AcquireReceipt {
            claim: id,
            receipt: receipt.receipt,
            epoch: receipt.epoch,
        },
        vec![],
    )?;
    let evidence_set = EvidenceSetId(demo_id(&identity, "evidence-set"));
    submit(
        node,
        "begin-evidence",
        identity.worker,
        Command::BeginEvidenceSet {
            claim: id,
            receipt,
            evidence_set,
        },
        vec![],
    )?;
    let bytes = br#"{"passed":1,"failed":0,"skipped":0}"#;
    let upload = UploadId(demo_id(&identity, "upload"));
    let offset = node.content.begin(
        upload,
        ContentDomainId(identity.ledger.tenant.0),
        ContentClass::Evidence,
        bytes.len() as u64,
        Some(ContentHash(*blake3::hash(bytes).as_bytes())),
    )?;
    if offset < bytes.len() as u64 {
        node.content.append(
            upload,
            offset,
            bytes
                .get(usize::try_from(offset).map_err(|_| NodeError::Identity)?..)
                .ok_or(NodeError::Identity)?,
        )?;
    }
    let content = node.content.seal(upload)?;
    node.content.verify(&content)?;
    let artifact = NewArtifact {
        id: ArtifactId(demo_id(&identity, "artifact")),
        content: ArtifactContent {
            ledger: identity.ledger,
            schema: SCHEMA_MAJOR,
            kind: "test-report".into(),
            schema_hash: test_report_schema(),
            metadata: vec![],
            payload: ArtifactPayload::Content(content.clone()),
            producer: identity.worker,
            receipt: Some(receipt),
            inputs: BTreeSet::new(),
            visibility: BTreeSet::new(),
        },
    };
    let artifact_ref = ArtifactRef {
        id: artifact.id,
        hash: artifact.content.content_hash()?,
    };
    // This attestation is created by trusted ingress after local fsync + schema
    // verification. A replicated host must establish the active custody contract.
    let _: focal_evidence::TestReport =
        serde_json::from_slice(bytes).map_err(|e| NodeError::Domain(e.to_string()))?;
    let attestation = EvidenceAttestation {
        descriptor_hash: artifact_ref.hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    };
    submit(
        node,
        "attach",
        identity.worker,
        Command::AttachArtifact {
            claim: id,
            receipt,
            evidence_set,
            artifact,
        },
        vec![attestation.clone()],
    )?;
    let testament = TestamentId(demo_id(&identity, "testament"));
    submit(
        node,
        "close",
        identity.worker,
        Command::CloseTestament {
            claim: id,
            receipt,
            testament,
            evidence_set,
            manifest: vec![artifact_ref],
            summary: "One executed test passed".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
        },
        vec![attestation],
    )?;
    node.content.finish(upload)?;
    submit(
        node,
        "acknowledge",
        identity.issuer,
        Command::AcknowledgeTestament {
            claim: id,
            testament,
        },
        vec![],
    )?;
    submit(
        node,
        "validate",
        identity.issuer,
        Command::BeginWholeWorkValidation { claim: id },
        vec![],
    )?;
    let validation = ValidationId(demo_id(&identity, "test-validation"));
    let state = node.session.read_at_least(SessionSeq(0))?;
    let run = state
        .runs
        .values()
        .find(|r| r.id.validation == validation)
        .cloned()
        .ok_or_else(|| NodeError::Domain("validation was not scheduled".into()))?;
    if run.final_verdict.is_none() {
        // Only incomplete persisted work runs. Recovery and completed retries do
        // not rerun historical validators or manufacture another lifecycle event.
        let mut registry = Registry::new(1);
        registry
            .register(
                Registration {
                    handler: handler(),
                    evidence_schema: test_report_schema(),
                    max_evidence_bytes: 4096,
                },
                Box::new(TestReportValidator),
            )
            .map_err(|e| NodeError::Domain(e.to_string()))?;
        let evidence_bytes = node.content.read_bytes(&content, 4096)?;
        let evaluation = registry
            .execute(&handler(), test_report_schema(), &evidence_bytes, None)
            .map_err(|e| NodeError::Domain(e.to_string()))?;
        submit(
            node,
            "verdict",
            identity.evaluator,
            Command::RecordValidationVerdict {
                verdict: VerdictRecord {
                    run: run.id,
                    evaluator: identity.evaluator,
                    handler: handler(),
                    attempt: run.handler_index,
                    manifest: run.manifest,
                    value: evaluation.value,
                    evidence: vec![artifact_ref],
                },
            },
            vec![],
        )?;
    }
    submit(
        node,
        "complete",
        identity.issuer,
        Command::CompleteWholeWork { claim: id },
        vec![],
    )?;
    let state = node.session.read_at_least(SessionSeq(0))?;
    let object = state
        .claims
        .get(&id)
        .ok_or_else(|| NodeError::Domain("committed demo claim is missing".into()))?;
    let validation = state
        .runs
        .get(&run.id)
        .ok_or_else(|| NodeError::Domain("committed demo run is missing".into()))?
        .final_verdict
        .ok_or_else(|| NodeError::Domain("validator still pending".into()))?;
    Ok(DemoReport {
        ledger: identity.ledger,
        claim: id,
        status: object.lifecycle().status,
        sequence: state.sequence,
        artifact: content,
        validation,
        history: object.lifecycle().history.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_workflow_recovers_with_identical_proof_and_no_new_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut settings = crate::config::Settings::default();
        settings.node.data_dir = Some(dir.path().to_owned());
        let first;
        {
            let mut node = EmbeddedNode::open(&settings).unwrap();
            first = run(&mut node).unwrap();
            assert_eq!(first.status, ClaimStatus::Satisfied);
            assert_eq!(first.validation, VerdictValue::Pass);
            node.checkpoint().unwrap();
        }
        let mut node = EmbeddedNode::open(&settings).unwrap();
        let second = run(&mut node).unwrap();
        assert_eq!(first.sequence, second.sequence);
        assert_eq!(first.artifact, second.artifact);
        assert_eq!(first.history, second.history);
        node.content.verify(&second.artifact).unwrap();
    }
}
