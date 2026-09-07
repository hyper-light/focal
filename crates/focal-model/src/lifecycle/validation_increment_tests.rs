use super::super::tests as fixtures;
use super::*;
use crate::lifecycle::{aggregation, claim, evidence, graph, scope};
use crate::{
    Confidence, EvidenceAttestation, MonitorId, OutcomeKind, ReceiptId, SessionSeq, WaitPredicate,
};

fn limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 2,
        max_checks: 4,
        max_results: 16,
        max_updates: 8,
    }
}

fn definitions(mode: ValidationMode, program: Program<'_>) -> Vec<Declaration> {
    let definition = claim::tests::definition(4);
    let spec = DeclarationSpec {
        binding: fixtures::binding(202),
        claim: ClaimId(definition.binding.object.0),
        declaration_index: 1,
        target: TargetDeclaration::Increment,
        phase: ValidationPhase::Increment,
        deadline: Deadline {
            at: 1000,
            ..fixtures::specification(mode, program).deadline
        },
        ..fixtures::specification(mode, program)
    };
    vec![
        Declaration::new(
            Principal::Actor(definition.issuer),
            DeclarationSpec {
                binding: fixtures::binding(201),
                declaration_index: 0,
                kind: ValidationKind::Receipt,
                target: TargetDeclaration::Delivery,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                program: Program::Delivery,
                ..spec
            },
            fixtures::limits(),
        )
        .unwrap(),
        Declaration::new(
            Principal::Actor(definition.issuer),
            spec,
            fixtures::limits(),
        )
        .unwrap(),
    ]
}

fn snapshot(claim: &ClaimState) -> graph::Snapshot {
    graph::Snapshot::capture(
        &[claim],
        graph::Limits {
            nodes: 4,
            edges: 8,
            visits: 128,
        },
    )
    .unwrap()
}

fn received(declarations: &[Declaration], max_responses: u32) -> ClaimState {
    let mut definition = claim::tests::definition(max_responses);
    definition.acceptance = aggregation::AcceptancePolicy::new(
        definition.binding,
        definition.issuer,
        &[aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 2,
            mode: ValidationMode::Required,
            checks: &[],
        }],
        declarations,
        limits(),
    )
    .unwrap();
    let mut claim = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    claim
        .post_owned(Principal::Actor(claim.issuer()), claim.binding())
        .unwrap();
    let graph = snapshot(&claim);
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let aggregate = aggregation::ClaimAggregation::new(&claim, limits()).unwrap();
    claim
        .acquire_receipt(
            &claim.binding(),
            Principal::Actor(claim.subject()),
            ReceiptFence {
                receipt: ReceiptId::from_u128(100),
                epoch: 1,
            },
            &aggregate.admission(),
            &start,
            &[],
        )
        .unwrap();
    claim
}

fn attestation(binding: Binding) -> EvidenceAttestation {
    EvidenceAttestation {
        descriptor_hash: binding.content,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    }
}

fn work(claim: &ClaimState, id: u128, slot: u32) -> WorkArtifact {
    let parent = evidence::Parent::from_claim(claim).unwrap();
    let binding = fixtures::binding(id);
    WorkArtifact::generate(
        binding,
        &parent,
        Principal::Actor(parent.holder),
        slot,
        parent.receipt,
        &attestation(binding),
    )
    .unwrap()
}

fn ready<'a>(
    claim: &ClaimState,
    work: &WorkArtifact,
    declaration: &'a Declaration,
) -> Evaluation<'a> {
    Evaluation::materialize_increment(Principal::Actor(claim.issuer()), declaration, claim, work)
        .unwrap()
}

fn begin<'a>(claim: &ClaimState, work: &WorkArtifact, ready: Evaluation<'a>) -> Evaluation<'a> {
    let owner = ready.increment_owner(claim, work, 10).unwrap();
    ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &owner,
        )
        .unwrap()
        .next
}

fn report<'a>(
    claim: &ClaimState,
    work: &WorkArtifact,
    evaluation: Evaluation<'a>,
    value: VerdictValue,
) -> Transition<'a> {
    let owner = evaluation.increment_report_owner(claim, work, 101).unwrap();
    let (report, evidence) = fixtures::report_parts(&evaluation, value);
    evaluation
        .report(
            Principal::Actor(evaluation.evaluator().unwrap()),
            &evaluation.binding(),
            &owner,
            report,
            &evidence,
        )
        .unwrap()
}

fn close(claim: &mut ClaimState, work: WorkArtifact) -> (evidence::Response, WorkArtifact) {
    let parent = evidence::Parent::from_claim(claim).unwrap();
    let plan = evidence::Response::close(
        evidence::ResponseIdentity {
            binding: fixtures::binding(400 + u128::from(parent.next_cycle)),
            claim: parent.claim,
            receipt: parent.receipt,
            cycle: parent.next_cycle,
            prior: parent.latest_response,
        },
        &parent,
        Principal::Actor(parent.holder),
        &[work],
        &[evidence::SlotBinding {
            slot: work.slot(),
            artifact: work.reference(),
        }],
        evidence::CloseReport {
            summary: "The requested work is complete and attached for independent validation.",
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            diagnostics: &[],
            limits: evidence::ResponseLimits {
                artifacts: 1,
                diagnostics: 0,
                summary_bytes: 256,
                construction_bytes: 4096,
            },
        },
    )
    .unwrap();
    claim
        .observe_response(
            &claim.binding(),
            Principal::Actor(parent.holder),
            &plan.response,
        )
        .unwrap();
    (plan.response, plan.attachments[0])
}

fn cut(position: u64) -> claim::ClaimCut {
    claim::ClaimCut {
        position: SessionSeq(position),
        cause: ContentHash([80; 32]),
    }
}

#[test]
fn actual_work_target_survives_receive_and_last_authored_response_close() {
    let declarations = definitions(ValidationMode::Required, fixtures::programmatic(false));
    let mut claim = received(&declarations, 1);
    let generated = work(&claim, 300, 0);
    let ready = ready(&claim, &generated, &declarations[1]);
    assert_eq!(ready.generation(), 1);
    assert_eq!(ready.receipt(), Some(generated.receipt()));
    assert_eq!(
        ready.target(),
        Target::Increment {
            claim: claim.binding(),
            artifact: generated.binding()
        }
    );
    let received = generated
        .receive(
            &generated.binding(),
            &evidence::Parent::from_claim(&claim).unwrap(),
            Principal::Actor(claim.issuer()),
        )
        .unwrap();
    assert!(ready.increment_owner(&claim, &received, 10).is_ok());
    let (mut response, attached) = close(&mut claim, received);
    assert_eq!(
        claim.response_count(),
        usize::try_from(claim.max_responses()).unwrap()
    );
    assert_eq!(attached.state(), WorkArtifactState::Attached);
    let materialized_after_close = Evaluation::materialize_increment(
        Principal::Actor(claim.issuer()),
        &declarations[1],
        &claim,
        &attached,
    )
    .unwrap();
    assert!(
        materialized_after_close
            .increment_owner(&claim, &attached, 10)
            .is_ok()
    );
    let begun = begin(&claim, &attached, ready);
    let parent = evidence::Parent::from_claim(&claim).unwrap();
    response
        .apply(
            response
                .plan_post(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(parent.holder),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), &response)
        .unwrap();
    let claim_before = claim.clone();
    let failure = report(&claim, &attached, begun, VerdictValue::Fail);
    assert_eq!(failure.next.state(), State::ValidationFailed);
    assert_eq!(failure.result.unwrap().target(), ready.target());
    assert_eq!(attached.state(), WorkArtifactState::Attached);
    assert_eq!(claim, claim_before);
    assert_eq!(response.state(), evidence::ResponseState::Posted);
}

#[test]
fn an_earlier_cycle_keeps_its_target_after_a_later_response_is_authored() {
    let declarations = definitions(ValidationMode::Observe, fixtures::programmatic(false));
    let mut claim = received(&declarations, 2);
    let first = work(&claim, 300, 0);
    let begun = begin(&claim, &first, ready(&claim, &first, &declarations[1]));
    let (_, first) = close(&mut claim, first);
    let second = work(&claim, 301, 0);
    assert_eq!(second.cycle(), 2);
    let second_ready = ready(&claim, &second, &declarations[1]);
    assert_eq!(second_ready.generation(), 2);
    let _ = close(&mut claim, second);
    let result = report(&claim, &first, begun, VerdictValue::Pass)
        .result
        .unwrap();
    assert_eq!(result.generation(), 1);
    assert_ne!(result.target(), second_ready.target());
    assert!(begun.increment_report_owner(&claim, &second, 101).is_err());
}

#[test]
fn wrong_actor_definition_slot_content_and_revision_cannot_supply_owner_facts() {
    let declarations = definitions(ValidationMode::Required, fixtures::programmatic(false));
    let claim = received(&declarations, 2);
    let artifact = work(&claim, 300, 0);
    for principal in [
        Principal::Node(claim.issuer()),
        Principal::Actor(claim.subject()),
    ] {
        assert_eq!(
            Evaluation::materialize_increment(principal, &declarations[1], &claim, &artifact)
                .unwrap_err(),
            ContractError::WrongActor
        );
    }
    assert!(
        Evaluation::materialize_increment(
            Principal::Actor(claim.issuer()),
            &declarations[0],
            &claim,
            &artifact
        )
        .is_err()
    );
    let unknown_slot = work(&claim, 301, 99);
    assert!(
        Evaluation::materialize_increment(
            Principal::Actor(claim.issuer()),
            &declarations[1],
            &claim,
            &unknown_slot
        )
        .is_err()
    );
    let received = artifact
        .receive(
            &artifact.binding(),
            &evidence::Parent::from_claim(&claim).unwrap(),
            Principal::Actor(claim.issuer()),
        )
        .unwrap();
    let ready = ready(&claim, &received, &declarations[1]);
    assert_eq!(
        ready.increment_owner(&claim, &artifact, 10).unwrap_err(),
        ContractError::StaleRevision
    );
    let parent = evidence::Parent::from_claim(&claim).unwrap();
    let altered = Binding {
        content: ContentHash([90; 32]),
        ..artifact.binding()
    };
    let altered = WorkArtifact::generate(
        altered,
        &parent,
        Principal::Actor(parent.holder),
        0,
        parent.receipt,
        &attestation(altered),
    )
    .unwrap();
    assert_eq!(
        ready.increment_owner(&claim, &altered, 10).unwrap_err(),
        ContractError::ContentConflict
    );
    let changed = definitions(ValidationMode::Observe, fixtures::programmatic(false));
    assert_eq!(
        Evaluation::materialize_increment(
            Principal::Actor(claim.issuer()),
            &changed[1],
            &claim,
            &artifact
        )
        .unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn receipt_rejection_preserves_existing_increment_begin_and_report_without_rehabilitation() {
    for begun_before_rejection in [false, true] {
        let declarations = definitions(ValidationMode::Required, fixtures::programmatic(false));
        let claim = received(&declarations, 2);
        let artifact = work(&claim, 300, 0);
        let ready = ready(&claim, &artifact, &declarations[1]);
        let earlier = begun_before_rejection.then(|| begin(&claim, &artifact, ready));
        let diagnostic = fixtures::binding(900);
        let failed = artifact
            .reject_receipt(
                &artifact.binding(),
                &evidence::Parent::from_claim(&claim).unwrap(),
                Principal::Actor(claim.issuer()),
                evidence::Diagnostic {
                    reason: evidence::EvidenceFailure::Structure,
                    artifact: ArtifactRef {
                        id: ArtifactId(diagnostic.object.0),
                        hash: diagnostic.content,
                    },
                },
                &attestation(diagnostic),
            )
            .unwrap();
        assert_eq!(ready.state(), State::Ready);
        assert!(ready.last_result().is_none());
        assert!(
            Evaluation::materialize_increment(
                Principal::Actor(claim.issuer()),
                &declarations[1],
                &claim,
                &failed
            )
            .is_err()
        );
        let failed_before = failed;
        let claim_before = claim.clone();
        let begun = earlier.unwrap_or_else(|| begin(&claim, &failed, ready));
        let attempt = begun.current_attempt().unwrap();
        // The designated evaluator reports the actual incomplete input using
        // separate typed diagnostic evidence. Rejection itself minted no verdict.
        let outcome = report(&claim, &failed, begun, VerdictValue::Incomplete);
        let result = outcome.result.unwrap();
        assert_eq!(result.verdict(), VerdictValue::Incomplete);
        assert_eq!(result.attempt(), Some(attempt.index));
        assert_eq!(result.target(), ready.target());
        assert_eq!(result.reporter(), Some(attempt.evaluator));
        assert!(result.evidence().is_some());
        assert_eq!(outcome.next.state(), State::ValidationIncomplete);
        assert_eq!(failed, failed_before);
        assert_eq!(failed.reference(), artifact.reference());
        assert_eq!(failed.state(), WorkArtifactState::ReceiptFailed);
        assert!(failed.attachment().is_none());
        assert_eq!(
            failed.diagnostic().unwrap().artifact.id,
            ArtifactId(diagnostic.object.0)
        );
        assert_eq!(claim, claim_before);
        assert_eq!(claim.status(), ClaimStatus::Received);
        assert!(!claim.local_complete());
    }
}

#[test]
fn ordinary_deadlock_preserves_begun_retry_and_quality_history_and_first_cut() {
    let declarations = definitions(ValidationMode::Observe, fixtures::programmatic(true));
    let mut claim = received(&declarations, 2);
    let artifact = work(&claim, 300, 0);
    let ready = ready(&claim, &artifact, &declarations[1]);
    let begun = begin(&claim, &artifact, ready);
    let monitor = MonitorId::from_u128(50);
    let graph = snapshot(&claim);
    let registration = scope::Registry::prepare_register(
        &claim,
        scope::Authority {
            principal: Principal::Actor(claim.issuer()),
            expected: claim.binding(),
            receipt: claim.receipt().map(|receipt| receipt.fence),
            cut: cut(4),
            now: 10,
        },
        scope::Registration {
            id: monitor,
            roots: &[WaitPredicate::Terminal(ClaimId(claim.binding().object.0))],
            deadline: claim.deadline().unwrap(),
        },
        &graph,
    )
    .unwrap();
    claim
        .apply_scope(&claim.binding(), registration, &[])
        .unwrap();
    let graph = snapshot(&claim);
    let deadlock = graph
        .deadlock(
            ClaimId(claim.binding().object.0),
            claim.deadline().unwrap(),
            100,
        )
        .unwrap();
    claim
        .break_deadlock(&claim.binding(), &deadlock, &[], SessionSeq(5))
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Deadlocked);
    let first_cut = claim.terminal_cut();
    let owner = begun
        .increment_report_owner(&claim, &artifact, 101)
        .unwrap();
    assert!(matches!(owner.parent, ParentState::Failed { .. }));
    assert!(ready.increment_owner(&claim, &artifact, 101).is_err());
    assert!(
        ready
            .increment_report_owner(&claim, &artifact, 101)
            .is_err()
    );
    let graph = snapshot(&claim);
    let release =
        scope::Registry::prepare_release_monitor(&claim, monitor, &graph, cut(6)).unwrap();
    claim.apply_scope(&claim.binding(), release, &[]).unwrap();
    assert_eq!(
        begun
            .increment_report_owner(&claim, &artifact, 101)
            .unwrap()
            .parent,
        owner.parent
    );
    let error_attempt = begun.current_attempt().unwrap();
    let error = report(&claim, &artifact, begun, VerdictValue::Error);
    assert_eq!(error.result.unwrap().attempt(), Some(error_attempt.index));
    let program_attempt = error.next.current_attempt().unwrap();
    let program = report(&claim, &artifact, error.next, VerdictValue::Pass);
    assert_eq!(
        program.result.unwrap().attempt(),
        Some(program_attempt.index)
    );
    assert_eq!(program.next.current_phase(), Phase::Quality);
    let quality_attempt = program.next.current_attempt().unwrap();
    let complete = report(&claim, &artifact, program.next, VerdictValue::Pass);
    let result = complete.result.unwrap();
    assert_eq!(result.attempt(), Some(quality_attempt.index));
    assert_eq!(
        result.programmatic_evidence(),
        program.result.unwrap().evidence()
    );
    assert_eq!(complete.next.state(), State::Validated);
    assert!(complete.next.current_attempt().is_err());
    assert_eq!(claim.terminal_cut(), first_cut);
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(5)));
    assert_eq!(artifact.state(), WorkArtifactState::Generated);
}

#[test]
fn controls_adoption_deadline_and_retained_fences_reject_begun_reports() {
    let declarations = definitions(ValidationMode::Observe, fixtures::programmatic(false));
    let claim = received(&declarations, 2);
    let artifact = work(&claim, 300, 0);
    let begun = begin(
        &claim,
        &artifact,
        ready(&claim, &artifact, &declarations[1]),
    );
    for intent in [
        claim::ClaimIntent::Cancel { cut: cut(5) },
        claim::ClaimIntent::Revoke { cut: cut(5) },
    ] {
        let mut controlled = claim.clone();
        controlled
            .apply(
                &controlled.binding(),
                Principal::Actor(controlled.issuer()),
                intent,
            )
            .unwrap();
        assert_eq!(
            begun
                .increment_report_owner(&controlled, &artifact, 101)
                .unwrap_err(),
            ContractError::StaleEvaluation
        );
    }
    let mut expired = claim.clone();
    expired
        .expire(&expired.binding(), expired.deadline().unwrap(), 100, cut(5))
        .unwrap();
    assert_eq!(
        begun
            .increment_report_owner(&expired, &artifact, 101)
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    let mut superseded = claim.clone();
    superseded
        .supersede_verified(&superseded.binding(), cut(5))
        .unwrap();
    assert_eq!(
        begun
            .increment_report_owner(&superseded, &artifact, 101)
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    let mut adopted = claim.clone();
    adopted
        .apply(
            &adopted.binding(),
            Principal::Actor(adopted.issuer()),
            claim::ClaimIntent::AdoptReceipt {
                previous: artifact.receipt(),
                replacement: claim::ReceiptEntitlement {
                    holder: ParticipantId::from_u128(88),
                    fence: ReceiptFence {
                        receipt: ReceiptId::from_u128(101),
                        epoch: 2,
                    },
                },
            },
        )
        .unwrap();
    assert_eq!(
        begun
            .increment_report_owner(&adopted, &artifact, 101)
            .unwrap_err(),
        ContractError::StaleReceipt
    );
    assert_eq!(
        begun
            .increment_report_owner(&claim, &artifact, begun.deadline().at)
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    let mut owner = begun
        .increment_report_owner(&claim, &artifact, 101)
        .unwrap();
    owner.authority.state = AuthorityState::Fenced(AuthorityFence {
        reason: FenceReason::Evaluation,
        cause: ContentHash([91; 32]),
    });
    let fenced = begun.record_fence(&begun.binding(), &owner).unwrap();
    assert_eq!(
        fenced
            .increment_report_owner(&claim, &artifact, 101)
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert_eq!(fenced.state(), begun.state());
    assert!(fenced.last_result().is_none());
}

#[test]
fn future_quality_policy_requirement_is_refused_before_begin() {
    let Program::Programmatic { check, quality } = fixtures::programmatic(true) else {
        panic!("programmatic fixture")
    };
    let mut quality = quality.unwrap();
    quality.required_policy = Some(ContentHash([92; 32]));
    let declarations = definitions(
        ValidationMode::Required,
        Program::Programmatic {
            check,
            quality: Some(quality),
        },
    );
    let claim = received(&declarations, 2);
    let artifact = work(&claim, 300, 0);
    let ready = ready(&claim, &artifact, &declarations[1]);
    assert_eq!(
        ready.increment_owner(&claim, &artifact, 10).unwrap_err(),
        ContractError::InvalidPolicy
    );
    assert_eq!(ready.state(), State::Ready);
    assert!(!ready.has_begun());
    assert!(ready.last_result().is_none());
}
