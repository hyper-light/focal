//! A real two-Actor contract exchange, with checked capabilities between all four
//! families. Persistence/publication is the successor owner's separate L2–L5 gate.
use super::{aggregation as a, audit, claim as c, evidence as e, validation as v, *};
use crate::*;
use v::tests::{EVALUATOR, ISSUER, binding, declaration, owner_for, programmatic, report_parts};

fn received_owner(
    evaluation: &v::Evaluation<'_>,
    response: &e::Response,
    claim: &c::ClaimState,
    aggregate: &a::ClaimAggregation,
) -> v::OwnerState {
    v::OwnerState {
        readiness: v::Readiness::ResponseReceived(
            v::ResponseReadiness::from_received(
                response,
                evaluation.target(),
                claim,
                &aggregate.decision(),
            )
            .unwrap(),
        ),
        ..owner_for(evaluation)
    }
}

#[test]
fn two_actors_progress_four_families_then_close_audit_without_launching_execution() {
    let work = declaration(ValidationMode::Required, programmatic(false));
    let delivery = v::Declaration::new(
        Principal::Actor(ISSUER),
        v::DeclarationSpec {
            binding: binding(101),
            claim: ClaimId::from_u128(200),
            issuer: ISSUER,
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: v::TargetDeclaration::Delivery,
            program: v::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(20),
                generation: 1,
                at: 100,
            },
        },
        v::Limits {
            handlers: 0,
            attempts: 0,
            slot_bytes: 64,
        },
    )
    .unwrap();
    let checks = [a::CheckPolicy {
        declaration_index: 4,
        validation: ValidationId::from_u128(100),
        mode: ValidationMode::Required,
    }];
    let policies = [a::SlotPolicy {
        slot: 0,
        missing_declaration_index: 1,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let limits = a::Limits {
        max_slots: 1,
        max_checks: 3,
        max_results: 8,
        max_updates: 2,
    };
    let increment = v::Declaration::new(
        Principal::Actor(ISSUER),
        v::DeclarationSpec {
            binding: binding(102),
            claim: ClaimId::from_u128(200),
            issuer: ISSUER,
            declaration_index: 5,
            kind: ValidationKind::Inspection,
            phase: ValidationPhase::Increment,
            mode: ValidationMode::Required,
            target: v::TargetDeclaration::Increment,
            program: programmatic(false),
            deadline: Deadline {
                timer: TimerId::from_u128(20),
                generation: 1,
                at: 100,
            },
        },
        v::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap();
    let declarations = [work, delivery, increment];
    let [work, delivery, increment] = &declarations;
    let mut claim = c::ClaimState::generate(
        Principal::Actor(ISSUER),
        c::ClaimDefinition {
            binding: binding(200),
            issuer: ISSUER,
            subject: EVALUATOR,
            deadline: None,
            max_responses: 2,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(200), RootCommandId::from_u128(1)).unwrap(),
            acceptance: a::AcceptancePolicy::new(
                binding(200),
                ISSUER,
                &policies,
                &declarations,
                limits,
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 8,
                children: 4,
            },
        },
    )
    .unwrap();
    let mut peer = c::ClaimState::generate(
        Principal::Actor(ISSUER),
        c::ClaimDefinition {
            binding: binding(201),
            issuer: ISSUER,
            subject: EVALUATOR,
            deadline: None,
            max_responses: 1,
            created: SessionSeq(1),
            graph: graph::Declaration::empty(),
            lineage: succession::Lineage::root(binding(201), RootCommandId::from_u128(2)).unwrap(),
            acceptance: a::acceptance_for(binding(201), ISSUER),
            scope_limits: scope::ScopeLimits {
                scopes: 4,
                roots: 8,
                children: 4,
            },
        },
    )
    .unwrap();
    let graph_limits = graph::Limits {
        nodes: 2,
        edges: 2,
        visits: 64,
    };
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(ISSUER),
            c::ClaimIntent::Post {
                standing: c::PostingStanding {
                    binding: claim.binding(),
                    standing: c::PredicateState::Passed,
                    target: c::PredicateState::Passed,
                },
            },
        )
        .unwrap();
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(10),
        epoch: 3,
    };
    let mut claim_aggregate = a::ClaimAggregation::new(&claim, limits).unwrap();
    let snapshot = graph::Snapshot::capture(&[&claim, &peer], graph_limits).unwrap();
    let start = snapshot.start(ClaimId::from_u128(200)).unwrap();
    claim
        .acquire_receipt(
            &claim.binding(),
            Principal::Actor(EVALUATOR),
            receipt,
            &claim_aggregate.admission(),
            &start,
            &[&peer],
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &peer], graph_limits).unwrap();
    let monitor = scope::Registry::prepare_register(
        &claim,
        scope::Authority {
            principal: Principal::Actor(ISSUER),
            expected: claim.binding(),
            receipt: Some(receipt),
            cut: c::ClaimCut {
                position: SessionSeq(3),
                cause: ContentHash([60; 32]),
            },
            now: 1,
        },
        scope::Registration {
            id: MonitorId::from_u128(77),
            roots: &[WaitPredicate::Terminal(ClaimId::from_u128(201))],
            deadline: Deadline {
                timer: TimerId::from_u128(77),
                generation: 1,
                at: 100,
            },
        },
        &graph,
    )
    .unwrap();
    claim
        .apply_scope(&claim.binding(), monitor, &[&peer])
        .unwrap();
    claim_aggregate.rebind(&claim).unwrap();
    let parent = e::Parent::from_claim(&claim).unwrap();
    let generated = e::WorkArtifact::generate(
        binding(400),
        &parent,
        Principal::Actor(EVALUATOR),
        0,
        receipt,
        &EvidenceAttestation {
            descriptor_hash: binding(400).content,
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        },
    )
    .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Received);
    let increment = claim_aggregate
        .materialize(
            Principal::Actor(ISSUER),
            increment,
            v::Materialization {
                binding: increment.binding(),
                target: v::Target::Increment {
                    claim: claim.binding(),
                    artifact: generated.binding(),
                },
                slot_name: None,
                generation: 7,
                receipt: Some(receipt),
            },
        )
        .unwrap();
    let increment = increment
        .begin(
            Principal::Actor(EVALUATOR),
            &increment.binding(),
            &owner_for(&increment),
        )
        .unwrap()
        .next;
    assert!(!claim_aggregate.increments_ready());
    // Closing and receiving are independent of a still-running increment check.
    let plan = e::Response::close(
        e::ResponseIdentity {
            binding: binding(300),
            claim: ClaimId::from_u128(200),
            receipt,
            cycle: 1,
            prior: None,
        },
        &parent,
        Principal::Actor(EVALUATOR),
        &[generated],
        &[e::SlotBinding {
            slot: 0,
            artifact: generated.reference(),
        }],
        1,
    )
    .unwrap();
    let mut response = plan.response;
    let artifact = plan.attachments[0];
    claim
        .observe_response(&claim.binding(), Principal::Actor(EVALUATOR), &response)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
    assert_eq!(response.state(), e::ResponseState::Generated);
    assert_eq!(artifact.state(), e::WorkArtifactState::Attached);
    assert!(response.evaluation().is_err());
    response
        .apply(
            response
                .plan_post(
                    &response.identity().binding,
                    &e::Parent::from_claim(&claim).unwrap(),
                    Principal::Actor(EVALUATOR),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(EVALUATOR), &response)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
    response
        .apply(
            response
                .plan_receive(
                    &response.identity().binding,
                    &e::Parent::from_claim(&claim).unwrap(),
                    Principal::Actor(ISSUER),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(ISSUER), &response)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);

    claim_aggregate.rebind(&claim).unwrap();
    let delivery = claim_aggregate
        .materialize(
            Principal::Actor(ISSUER),
            delivery,
            v::Materialization {
                binding: delivery.binding(),
                target: v::Target::Delivery {
                    response: response.identity().binding,
                },
                slot_name: None,
                generation: 7,
                receipt: Some(receipt),
            },
        )
        .unwrap();
    let delivery = delivery
        .receive_delivery(
            Principal::Actor(ISSUER),
            &delivery.binding(),
            &received_owner(&delivery, &response, &claim, &claim_aggregate),
        )
        .unwrap();
    assert_eq!(delivery.next.state(), v::State::Validated);
    assert_eq!(delivery.result.unwrap().reporter(), None);
    assert!(audit::ResultArtifact::from_result(delivery.result.unwrap()).is_err());

    let ready = claim_aggregate
        .materialize(
            Principal::Actor(ISSUER),
            work,
            v::Materialization {
                binding: work.binding(),
                target: v::Target::Artifact {
                    response: response.identity().binding,
                    slot: 0,
                    artifact: artifact.binding(),
                },
                slot_name: Some("output"),
                generation: 7,
                receipt: Some(receipt),
            },
        )
        .unwrap();
    assert!(
        response
            .plan_begin(
                &response.identity().binding,
                &claim,
                Principal::Actor(ISSUER),
                &claim_aggregate.decision()
            )
            .is_err()
    );
    assert!(
        v::ResponseReadiness::from_received(
            &response,
            ready.target(),
            &claim,
            &claim_aggregate.decision()
        )
        .is_err()
    );
    let (mut report, mut facts) = report_parts(&increment, VerdictValue::Pass);
    facts.binding = binding(901);
    report.evidence = ArtifactRef {
        id: ArtifactId::from_u128(901),
        hash: binding(901).content,
    };
    let increment = increment
        .report(
            Principal::Actor(EVALUATOR),
            &increment.binding(),
            &owner_for(&increment),
            report,
            &facts,
        )
        .unwrap();
    claim_aggregate
        .apply_acceptance(SessionSeq(9), &[], &[increment.result.unwrap()])
        .unwrap();
    // Final outcomes alone do not prove that all increment targets were registered.
    assert!(!claim_aggregate.increments_ready());
    claim_aggregate.seal_increment_targets().unwrap();
    assert!(claim_aggregate.increments_ready());
    let owner = received_owner(&ready, &response, &claim, &claim_aggregate);
    assert!(
        ready
            .begin(Principal::Actor(ISSUER), &ready.binding(), &owner)
            .is_err()
    );
    assert!(
        ready
            .begin(Principal::Node(EVALUATOR), &ready.binding(), &owner)
            .is_err()
    );
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    // The evaluator's checked begin advances the independent rows directly; no
    // additional issuer invocation or global Evaluator enrollment is needed.
    let parent = e::Parent::from_claim(&claim).unwrap();
    response
        .apply(
            response
                .plan_evaluation(
                    &response.identity().binding,
                    &claim,
                    &begun,
                    &claim_aggregate.decision(),
                )
                .unwrap(),
        )
        .unwrap();
    let artifact = artifact
        .observe_evaluation(&artifact.binding(), &parent, &response, &begun)
        .unwrap();
    claim
        .observe_evaluation(&claim.binding(), &begun, &claim_aggregate.decision())
        .unwrap();
    claim_aggregate.rebind(&claim).unwrap();
    assert_eq!(response.state(), e::ResponseState::Validating);
    assert_eq!(artifact.state(), e::WorkArtifactState::Validating);
    assert_eq!(claim.status(), ClaimStatus::Validating);

    let mut response_aggregate = a::ResponseAggregation::new(
        &claim,
        response.evaluation().unwrap(),
        &[delivery.result.unwrap()],
        limits,
    )
    .unwrap();
    let (report, facts) = report_parts(&begun, VerdictValue::Pass);
    let evaluated = begun
        .report(
            Principal::Actor(EVALUATOR),
            &begun.binding(),
            &received_owner(&begun, &response, &claim, &claim_aggregate),
            report,
            &facts,
        )
        .unwrap();
    let result = evaluated.result.unwrap();
    let update = response_aggregate.apply(SessionSeq(10), &[result]).unwrap();
    let artifact = artifact
        .apply_aggregate(&artifact.binding(), &update)
        .unwrap();
    response
        .apply(
            response
                .plan_aggregate(&response.identity().binding, &update)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    claim_aggregate.apply(SessionSeq(10), &[update]).unwrap();
    claim
        .apply_aggregate(&claim.binding(), &claim_aggregate.decision())
        .unwrap();
    assert_eq!(evaluated.next.state(), v::State::Validated);
    assert_eq!(artifact.state(), e::WorkArtifactState::Validated);
    assert_eq!(response.state(), e::ResponseState::Validated);
    assert!(claim.local_complete());
    assert_eq!(claim.status(), ClaimStatus::Validating);
    let cut = c::ClaimCut {
        position: SessionSeq(11),
        cause: ContentHash([60; 32]),
    };
    let waiting = graph::Snapshot::capture(&[&claim, &peer], graph_limits).unwrap();
    assert!(waiting.release(ClaimId::from_u128(200)).is_err());
    // Audit seals at local completion before graph satisfaction.
    let targets = claim_aggregate.seal_targets().unwrap();
    let cohort = audit::AuditCohort::seal(
        &claim,
        &targets,
        SessionSeq(10),
        &[delivery.next, evaluated.next, increment.next],
        &[result, delivery.result.unwrap(), increment.result.unwrap()],
        audit::Limits {
            evaluations: 3,
            results: 8,
        },
    )
    .unwrap();
    let mut bundle =
        audit::ResultTestament::generate(binding(500), Principal::Actor(ISSUER), cohort).unwrap();
    bundle
        .post(Principal::Actor(ISSUER), &bundle.binding())
        .unwrap();
    assert_eq!(bundle.state(), audit::ResultTestamentState::Posted);
    assert_eq!(claim.status(), ClaimStatus::Validating);
    peer.apply(
        &peer.binding(),
        Principal::Actor(ISSUER),
        c::ClaimIntent::Cancel { cut },
    )
    .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &peer], graph_limits).unwrap();
    let release = graph.release(ClaimId::from_u128(200)).unwrap();
    claim
        .graph_release(&claim.binding(), &release, &[&peer], cut.position)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Satisfied);
    assert_eq!(artifact.terminal().unwrap().0, SessionSeq(10));
}
