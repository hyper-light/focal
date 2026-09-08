use super::*;
use crate::lifecycle::evidence as e;
use crate::lifecycle::validation as v;
use crate::{
    ClaimId, Confidence, MonitorId, ObjectId, OutcomeKind, ReceiptId, RootCommandId, TimerId,
    ValidationKind, ValidationMode, ValidationPhase, WaitPredicate,
};

fn cut(sequence: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(sequence),
        cause: ContentHash([7; 32]),
    }
}
fn alimits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 8,
        max_checks: 8,
        max_results: 32,
        max_updates: 8,
    }
}
fn glimits() -> graph::Limits {
    graph::Limits {
        nodes: 16,
        edges: 64,
        visits: 8192,
    }
}
fn fixture(
    id: u128,
    slot: Option<ValidationMode>,
    increment: bool,
) -> (ClaimState, Vec<v::Declaration>) {
    let mut definition = crate::lifecycle::claim::tests::definition(4);
    definition.binding.object = ObjectId::from_u128(id);
    definition.lineage =
        succession::Lineage::root(definition.binding, RootCommandId::from_u128(id)).unwrap();
    let binding = definition.binding;
    let issuer = definition.issuer;
    let mut spec = v::tests::specification(ValidationMode::Required, v::Program::Delivery);
    spec.binding = Binding {
        object: ObjectId::from_u128(id + 1000),
        ..binding
    };
    spec.claim = ClaimId::from_u128(id);
    spec.issuer = issuer;
    spec.declaration_index = 0;
    spec.kind = ValidationKind::Receipt;
    spec.target = v::TargetDeclaration::Delivery;
    let mut declarations =
        vec![v::Declaration::new(Principal::Actor(issuer), spec, v::tests::limits()).unwrap()];
    if increment {
        let mut spec =
            v::tests::specification(ValidationMode::Observe, v::tests::programmatic(false));
        spec.binding = Binding {
            object: ObjectId::from_u128(id + 1001),
            ..binding
        };
        spec.claim = ClaimId::from_u128(id);
        spec.issuer = issuer;
        spec.declaration_index = 1;
        spec.phase = ValidationPhase::Increment;
        spec.target = v::TargetDeclaration::Increment;
        declarations
            .push(v::Declaration::new(Principal::Actor(issuer), spec, v::tests::limits()).unwrap());
    }
    let slots: Vec<_> = slot
        .into_iter()
        .map(|mode| aggregation::SlotPolicy {
            slot: 0,
            missing_declaration_index: 99,
            mode,
            checks: &[],
        })
        .collect();
    definition.acceptance =
        aggregation::AcceptancePolicy::new(binding, issuer, &slots, &declarations, alimits())
            .unwrap();
    (
        ClaimState::generate(Principal::Actor(issuer), definition).unwrap(),
        declarations,
    )
}
fn receive(claim: &mut ClaimState) {
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            ClaimIntent::Post {
                standing: PostingStanding {
                    binding: claim.binding(),
                    standing: PredicateState::Passed,
                    target: PredicateState::Passed,
                },
            },
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&*claim], glimits()).unwrap();
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let aggregate = aggregation::ClaimAggregation::new(claim, alimits()).unwrap();
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
}
fn response(claim: &mut ClaimState, id: u128, delivered: bool) -> Response {
    let parent = e::Parent::from_claim(claim).unwrap();
    let mut response = Response::close(
        e::ResponseIdentity {
            binding: Binding {
                object: ObjectId::from_u128(id),
                ..claim.lineage().binding()
            },
            claim: parent.claim,
            receipt: parent.receipt,
            cycle: parent.next_cycle,
            prior: parent.latest_response,
        },
        &parent,
        Principal::Actor(parent.holder),
        &[],
        &[],
        e::CloseReport {
            summary: "The completed cycle has no output attachments.",
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            diagnostics: &[],
            limits: e::ResponseLimits {
                artifacts: 4,
                diagnostics: 4,
                summary_bytes: 256,
                construction_bytes: 65536,
            },
        },
    )
    .unwrap()
    .response;
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), &response)
        .unwrap();
    response
        .apply(
            response
                .plan_post(
                    &response.identity().binding,
                    &e::Parent::from_claim(claim).unwrap(),
                    Principal::Actor(parent.holder),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), &response)
        .unwrap();
    if delivered {
        response
            .apply(
                response
                    .plan_receive(
                        &response.identity().binding,
                        &e::Parent::from_claim(claim).unwrap(),
                        Principal::Actor(claim.issuer()),
                    )
                    .unwrap(),
            )
            .unwrap();
        claim
            .observe_response(
                &claim.binding(),
                Principal::Actor(claim.issuer()),
                &response,
            )
            .unwrap();
    }
    response
}
fn finish(
    claim: &mut ClaimState,
    response: &mut Response,
    declaration: &v::Declaration,
    sequence: u64,
) {
    let mut aggregate = aggregation::ClaimAggregation::new(claim, alimits()).unwrap();
    let delivery = aggregate
        .materialize(
            Principal::Actor(claim.issuer()),
            declaration,
            v::Materialization {
                binding: declaration.binding(),
                target: v::Target::Delivery {
                    response: response.identity().binding,
                },
                slot_name: None,
                generation: u64::from(response.identity().cycle),
                receipt: Some(response.identity().receipt),
            },
        )
        .unwrap();
    let result = delivery
        .receive_delivery(
            Principal::Actor(claim.issuer()),
            &delivery.binding(),
            &delivery.delivery_owner(claim, response, 1).unwrap(),
        )
        .unwrap()
        .result
        .unwrap();
    response
        .apply(
            response
                .plan_begin(
                    &response.identity().binding,
                    claim,
                    Principal::Actor(claim.issuer()),
                    &aggregate.decision(),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .request_evaluation(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            &aggregate.decision(),
        )
        .unwrap();
    aggregate.rebind(claim).unwrap();
    let mut response_aggregate = aggregation::ResponseAggregation::new(
        claim,
        response.evaluation().unwrap(),
        &[result],
        alimits(),
    )
    .unwrap();
    let update = response_aggregate.apply(SessionSeq(sequence), &[]).unwrap();
    response
        .apply(
            response
                .plan_aggregate(&response.identity().binding, &update)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    aggregate.apply(SessionSeq(sequence), &[update]).unwrap();
    claim
        .apply_aggregate(&claim.binding(), &aggregate.decision())
        .unwrap();
}
fn definition(claim: &ClaimState) -> ClaimDefinition {
    ClaimDefinition {
        binding: claim.lineage().binding(),
        issuer: claim.issuer(),
        subject: claim.subject(),
        created: claim.created(),
        deadline: claim.deadline(),
        max_responses: claim.max_responses(),
        graph: claim
            .graph()
            .try_copy(claim.graph().copy_charge().unwrap())
            .unwrap(),
        lineage: claim
            .lineage()
            .try_copy(claim.lineage().copy_charge().unwrap())
            .unwrap(),
        acceptance: claim
            .acceptance()
            .try_copy(claim.acceptance().copy_charge().unwrap())
            .unwrap(),
        scope_limits: claim.scopes().limits(),
    }
}
fn values<'a>(claim: &ClaimState, responses: &[&'a Response]) -> Vec<ClaimResponseValue<'a>> {
    assert_eq!(claim.response_count(), responses.len());
    claim
        .response_snapshots_v1()
        .zip(responses)
        .map(|(history, response)| ClaimResponseValue { history, response })
        .collect()
}
fn restore(claim: &ClaimState, responses: &[&Response]) -> ClaimState {
    let source = claim.scopes().snapshot_v1();
    let scopes = scope::Registry::prepare_hydration_v1(
        claim.binding(),
        claim.created(),
        claim.scopes().limits(),
        &source,
        usize::MAX,
    )
    .unwrap();
    let responses = values(claim, responses);
    let owned = definition(claim);
    let pointer = owned.acceptance.declarations().as_ptr();
    let plan = bytes::fail_after(0, || {
        ClaimState::prepare_hydration_v1(
            owned,
            claim.snapshot_v1(),
            responses.as_slice(),
            scopes,
            usize::MAX,
        )
        .unwrap()
    });
    let bytes = plan.construction_charge().unwrap();
    let visits = plan.build_visits().unwrap();
    let restored = plan.build(bytes, visits).unwrap();
    assert_eq!(&restored, claim);
    assert_eq!(restored.acceptance().declarations().as_ptr(), pointer);
    restored
}
fn monitor(owner: &mut ClaimState, peer: &ClaimState, sequence: u64) {
    let graph = graph::Snapshot::capture(&[&*owner, peer], glimits()).unwrap();
    let transition = scope::Registry::prepare_register(
        owner,
        scope::Authority {
            principal: Principal::Actor(owner.issuer()),
            expected: owner.binding(),
            receipt: owner.receipt().map(|receipt| receipt.fence),
            cut: cut(sequence),
            now: 1,
        },
        scope::Registration {
            id: MonitorId::from_u128(8),
            roots: &[WaitPredicate::Terminal(ClaimId(peer.binding().object.0))],
            deadline: Deadline {
                timer: TimerId::from_u128(8),
                generation: 1,
                at: 100,
            },
        },
        &graph,
    )
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[peer])
        .unwrap();
}

#[test]
fn claim_roundtrip_preserves_adopted_receipts_original_reports_and_late_receipt() {
    let (mut claim, _) = fixture(9, None, false);
    restore(&claim, &[]);
    receive(&mut claim);
    let old = response(&mut claim, 500, true);
    restore(&claim, &[&old]);
    let replacement = ReceiptEntitlement {
        holder: crate::ParticipantId::from_u128(7),
        fence: ReceiptFence {
            receipt: ReceiptId::from_u128(101),
            epoch: 2,
        },
    };
    let adoption = claim
        .prepare_receipt_adoption(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            claim.receipt().unwrap().fence,
            replacement,
            cut(8),
        )
        .unwrap();
    let mut next = claim.try_copy(claim.copy_charge().unwrap()).unwrap();
    next.apply_receipt_adoption(&adoption).unwrap();
    claim = next;
    let adopted = restore(&claim, &[&old]);
    assert_eq!(adopted.recorded_response(&old).unwrap(), (true, true));
    assert_ne!(old.identity().receipt, adopted.receipt().unwrap().fence);
    let mut late = response(&mut claim, 501, false);
    claim
        .apply(
            &claim.binding(),
            Principal::Actor(claim.issuer()),
            ClaimIntent::Cancel { cut: cut(9) },
        )
        .unwrap();
    late.apply(
        late.plan_receive(
            &late.identity().binding,
            &e::Parent::from_claim(&claim).unwrap(),
            Principal::Actor(claim.issuer()),
        )
        .unwrap(),
    )
    .unwrap();
    let restored = restore(&claim, &[&old, &late]);
    assert_eq!(restored.recorded_response(&late).unwrap(), (true, false));
    assert_eq!(restored.status(), ClaimStatus::Cancelled);
}

#[test]
fn local_outcome_and_terminal_scope_release_keep_original_cut_and_owned_buffers() {
    let (mut claim, declarations) = fixture(9, None, false);
    let (mut peer, _) = fixture(10, None, false);
    receive(&mut claim);
    monitor(&mut claim, &peer, 2);
    let mut response = response(&mut claim, 500, true);
    finish(&mut claim, &mut response, &declarations[0], 5);
    assert!(claim.local_complete());
    assert!(!claim.is_terminal());
    restore(&claim, &[&response]);
    peer.apply(
        &peer.binding(),
        Principal::Actor(peer.issuer()),
        ClaimIntent::Cancel { cut: cut(6) },
    )
    .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &peer], glimits()).unwrap();
    let transition =
        scope::Registry::prepare_release_monitor(&claim, MonitorId::from_u128(8), &graph, cut(6))
            .unwrap();
    claim
        .apply_scope(&claim.binding(), transition, &[&peer])
        .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &peer], glimits()).unwrap();
    claim
        .graph_release(
            &claim.binding(),
            &graph.release(ClaimId(claim.binding().object.0)).unwrap(),
            &[&peer],
            SessionSeq(7),
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &peer], glimits()).unwrap();
    let transition = scope::Registry::prepare_release_owner_bounded(
        &claim,
        &graph,
        &[&peer],
        cut(8),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    claim
        .apply_scope(&claim.binding(), transition, &[&peer])
        .unwrap();
    let restored = restore(&claim, &[&response]);
    assert_eq!(restored.local_sealed_at(), Some(SessionSeq(5)));
    assert_eq!(restored.scopes().release_cut(), Some(cut(8)));
    assert_eq!(restored.status(), ClaimStatus::Satisfied);
}

#[test]
fn required_structural_and_graph_failure_provenance_roundtrips_without_reduction() {
    let (mut claim, declarations) = fixture(9, Some(ValidationMode::Required), false);
    receive(&mut claim);
    let mut response = response(&mut claim, 500, true);
    finish(&mut claim, &mut response, &declarations[0], 5);
    assert_eq!(claim.status(), ClaimStatus::ValidationIncomplete);
    restore(&claim, &[&response]);
    let ClaimTerminalCut::Required(cause) = claim.terminal_cut().unwrap() else {
        panic!("required cut")
    };
    let snapshot = cause.snapshot_v1();
    assert_eq!(
        aggregation::TerminalCut::hydrate_v1(snapshot).unwrap(),
        cause
    );
    let mut wrong_target = snapshot;
    wrong_target.cause.key.target = aggregation::CauseTarget::Response(TestamentId::from_u128(999));
    let fields = ClaimSnapshotV1 {
        terminal_cut: Some(ClaimTerminalSnapshotV1::Required(wrong_target)),
        ..claim.snapshot_v1()
    };
    let scopes = claim.scopes().snapshot_v1();
    let scopes = scope::Registry::prepare_hydration_v1(
        claim.binding(),
        claim.created(),
        claim.scopes().limits(),
        &scopes,
        usize::MAX,
    )
    .unwrap();
    let rows = values(&claim, &[&response]);
    assert!(matches!(
        ClaimState::prepare_hydration_v1(
            definition(&claim),
            fields,
            rows.as_slice(),
            scopes,
            usize::MAX
        ),
        Err(ContractError::InvalidTarget)
    ));
    let mut invalid = snapshot;
    invalid.cause.key.attempt = Some(0);
    assert!(aggregation::TerminalCut::hydrate_v1(invalid).is_err());
    let (dependent, _) = fixture(10, None, false);
    let mut dependent_definition = definition(&dependent);
    dependent_definition.graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: graph::Kind::DependsOn,
            target: ClaimId(claim.binding().object.0),
        }],
        1,
    )
    .unwrap();
    let mut dependent = ClaimState::generate(
        Principal::Actor(dependent_definition.issuer),
        dependent_definition,
    )
    .unwrap();
    let graph = graph::Snapshot::capture(&[&claim, &dependent], glimits()).unwrap();
    let failure = graph
        .dependency_failure(ClaimId(dependent.binding().object.0))
        .unwrap();
    dependent
        .dependency_failed(&dependent.binding(), &failure, &[&claim], SessionSeq(6))
        .unwrap();
    restore(&dependent, &[]);
    let ClaimTerminalCut::Graph(cut) = dependent.terminal_cut().unwrap() else {
        panic!("graph cut")
    };
    let original = cut.snapshot_v1();
    assert_eq!(graph::TerminalCut::hydrate_v1(original).unwrap(), cut);
    let mut invalid = original;
    invalid.origin.terminal = SessionSeq(7);
    assert!(graph::TerminalCut::hydrate_v1(invalid).is_err());
}

#[test]
fn owned_children_cancelled_monitors_and_legacy_release_remain_distinct() {
    let (mut owner, _) = fixture(9, None, false);
    let (child, _) = fixture(10, None, false);
    let mut child_definition = definition(&child);
    child_definition.created = SessionSeq(2);
    child_definition.lineage = succession::Lineage::new(
        child_definition.binding,
        crate::Cause::Claim(ClaimId(owner.binding().object.0)),
        &[],
        0,
    )
    .unwrap();
    let mut child = owner
        .generate_child(
            &owner.binding(),
            Principal::Actor(owner.issuer()),
            None,
            child_definition,
            cut(2),
        )
        .unwrap();
    monitor(&mut owner, &child, 3);
    child
        .apply(
            &child.binding(),
            Principal::Actor(child.issuer()),
            ClaimIntent::Cancel { cut: cut(4) },
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&child], glimits()).unwrap();
    let transition = scope::Registry::prepare_release_owner_bounded(
        &child,
        &graph,
        &[],
        cut(5),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    child
        .apply_scope(&child.binding(), transition, &[])
        .unwrap();
    owner
        .apply(
            &owner.binding(),
            Principal::Actor(owner.issuer()),
            ClaimIntent::Revoke { cut: cut(6) },
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&owner, &child], glimits()).unwrap();
    let transition = scope::Registry::prepare_cancel_monitor_bounded(
        &owner,
        scope::Authority {
            principal: Principal::Actor(owner.issuer()),
            expected: owner.binding(),
            receipt: None,
            cut: cut(7),
            now: 1,
        },
        MonitorId::from_u128(8),
        &graph,
        &[&child],
        scope::BuildLimits {
            bytes: usize::MAX,
            visits: usize::MAX,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[&child])
        .unwrap();
    let graph = graph::Snapshot::capture(&[&owner, &child], glimits()).unwrap();
    let transition = scope::Registry::prepare_release_owner_bounded(
        &owner,
        &graph,
        &[&child],
        cut(8),
        usize::MAX,
        usize::MAX,
    )
    .unwrap()
    .build()
    .unwrap();
    owner
        .apply_scope(&owner.binding(), transition, &[&child])
        .unwrap();
    let restored = restore(&owner, &[]);
    assert_eq!(restored.scopes().children()[0].registered(), SessionSeq(2));
    assert_eq!(
        restored
            .scopes()
            .iter()
            .next()
            .unwrap()
            .cancellation()
            .unwrap()
            .terminal,
        SessionSeq(6)
    );
    let (mut legacy, _) = fixture(20, None, false);
    let (peer, _) = fixture(21, None, false);
    monitor(&mut legacy, &peer, 2);
    legacy
        .apply(
            &legacy.binding(),
            Principal::Actor(legacy.issuer()),
            ClaimIntent::Cancel { cut: cut(3) },
        )
        .unwrap();
    let graph = graph::Snapshot::capture(&[&legacy, &peer], glimits()).unwrap();
    let transition =
        scope::Registry::prepare_release_owner(&legacy, &graph, &[&peer], cut(4)).unwrap();
    legacy
        .apply_scope(&legacy.binding(), transition, &[&peer])
        .unwrap();
    let restored = restore(&legacy, &[]);
    assert!(restored.scopes().released());
    assert!(restored.scopes().iter().next().unwrap().active());
}

#[test]
fn claim_and_scope_quotes_refuse_before_build_and_each_allocation_retries_exactly() {
    let (mut claim, _) = fixture(9, None, false);
    let (peer, _) = fixture(10, None, false);
    receive(&mut claim);
    monitor(&mut claim, &peer, 2);
    let response = response(&mut claim, 500, true);
    let source = claim.scopes().snapshot_v1();
    let rows = values(&claim, &[&response]);
    let scope_plan = || {
        scope::Registry::prepare_hydration_v1(
            claim.binding(),
            claim.created(),
            claim.scopes().limits(),
            &source,
            usize::MAX,
        )
        .unwrap()
    };
    let inspection = scope_plan().inspection_visits();
    assert!(matches!(
        scope::Registry::prepare_hydration_v1(
            claim.binding(),
            claim.created(),
            claim.scopes().limits(),
            &source,
            inspection - 1
        ),
        Err(ContractError::Capacity)
    ));
    for after in 0..3 {
        let owned = definition(&claim);
        let plan = bytes::fail_after(0, || {
            ClaimState::prepare_hydration_v1(
                owned,
                claim.snapshot_v1(),
                rows.as_slice(),
                scope_plan(),
                usize::MAX,
            )
            .unwrap()
        });
        let charge = plan.construction_charge().unwrap();
        let visits = plan.build_visits().unwrap();
        assert!(matches!(
            bytes::fail_after(after, || plan.build(charge, visits)),
            Err(ContractError::Capacity)
        ));
        assert_eq!(claim.recorded_response(&response).unwrap(), (true, true));
        restore(&claim, &[&response]);
    }
    for bytes_short in [true, false] {
        let plan = ClaimState::prepare_hydration_v1(
            definition(&claim),
            claim.snapshot_v1(),
            rows.as_slice(),
            scope_plan(),
            usize::MAX,
        )
        .unwrap();
        let charge = plan.construction_charge().unwrap();
        let visits = plan.build_visits().unwrap();
        assert!(matches!(
            plan.build(
                charge - usize::from(bytes_short),
                visits - usize::from(!bytes_short)
            ),
            Err(ContractError::Capacity)
        ));
    }
    let visits = ClaimState::prepare_hydration_v1(
        definition(&claim),
        claim.snapshot_v1(),
        rows.as_slice(),
        scope_plan(),
        usize::MAX,
    )
    .unwrap()
    .inspection_visits();
    assert!(matches!(
        ClaimState::prepare_hydration_v1(
            definition(&claim),
            claim.snapshot_v1(),
            rows.as_slice(),
            scope_plan(),
            visits - 1
        ),
        Err(ContractError::Capacity)
    ));
}

#[test]
fn malformed_claim_fields_and_substituted_response_source_do_not_create_rows() {
    let (mut claim, _) = fixture(9, None, false);
    receive(&mut claim);
    let response = response(&mut claim, 500, true);
    let source = claim.scopes().snapshot_v1();
    let rows = values(&claim, &[&response]);
    let fields = claim.snapshot_v1();
    let scope_plan = || {
        scope::Registry::prepare_hydration_v1(
            claim.binding(),
            claim.created(),
            claim.scopes().limits(),
            &source,
            usize::MAX,
        )
        .unwrap()
    };
    let cases = [
        ClaimSnapshotV1 {
            receipt: None,
            ..fields
        },
        ClaimSnapshotV1 {
            responses: 2,
            ..fields
        },
        ClaimSnapshotV1 {
            created: SessionSeq(0),
            ..fields
        },
        ClaimSnapshotV1 {
            status: ClaimStatus::Satisfied,
            ..fields
        },
        ClaimSnapshotV1 {
            local_complete: true,
            ..fields
        },
        ClaimSnapshotV1 {
            local_sealed_at: Some(SessionSeq(2)),
            ..fields
        },
        ClaimSnapshotV1 {
            subject: crate::ParticipantId::from_u128(77),
            ..fields
        },
    ];
    for invalid in cases {
        assert!(
            ClaimState::prepare_hydration_v1(
                definition(&claim),
                invalid,
                rows.as_slice(),
                scope_plan(),
                usize::MAX
            )
            .is_err()
        );
    }
    let mut bad = rows.clone();
    bad[0].history.link.content = ContentHash([91; 32]);
    assert!(
        ClaimState::prepare_hydration_v1(
            definition(&claim),
            fields,
            bad.as_slice(),
            scope_plan(),
            usize::MAX
        )
        .is_err()
    );
    let mut bad = rows.clone();
    bad[0].history.posted = false;
    assert!(
        ClaimState::prepare_hydration_v1(
            definition(&claim),
            fields,
            bad.as_slice(),
            scope_plan(),
            usize::MAX
        )
        .is_err()
    );
    restore(&claim, &[&response]);
}

#[path = "registration_snapshot_tests.rs"]
mod registration_tests;
