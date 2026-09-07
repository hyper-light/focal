use super::*;
use crate::{
    ArtifactId, ArtifactRef, ObjectId, ObjectRevision, ReceiptId, SessionId, TenantId, TimerId,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const SUBJECT: ParticipantId = ParticipantId::from_u128(2);
const OTHER: ParticipantId = ParticipantId::from_u128(3);
fn cut(n: u64) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(n),
        cause: ContentHash([7; 32]),
    }
}
fn fence(n: u64) -> ReceiptFence {
    ReceiptFence {
        receipt: ReceiptId::from_u128(u128::from(n)),
        epoch: n,
    }
}
fn deadline() -> Deadline {
    Deadline {
        timer: TimerId::from_u128(4),
        generation: 3,
        at: 100,
    }
}
pub(crate) fn definition(max: u32) -> ClaimDefinition {
    let binding = Binding {
        ledger: crate::LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        object: ObjectId::from_u128(9),
        content: ContentHash([5; 32]),
        revision: ObjectRevision(1),
    };
    ClaimDefinition {
        binding,
        issuer: ISSUER,
        subject: SUBJECT,
        deadline: Some(deadline()),
        max_responses: max,
        created: SessionSeq(1),
        graph: graph::Declaration::empty(),
        lineage: succession::Lineage::root(binding, crate::RootCommandId::from_u128(1)).unwrap(),
        acceptance: acceptance(binding, ISSUER),
        scope_limits: scope::ScopeLimits {
            scopes: 8,
            roots: 32,
            children: 8,
        },
    }
}
pub(crate) fn acceptance(binding: Binding, issuer: ParticipantId) -> aggregation::AcceptancePolicy {
    aggregation::acceptance_for(binding, issuer)
}
fn generated(max: u32) -> ClaimState {
    ClaimState::generate(Principal::Actor(ISSUER), definition(max)).unwrap()
}
fn graph_limits() -> graph::Limits {
    graph::Limits {
        nodes: 16,
        edges: 32,
        visits: 1024,
    }
}
fn posting(claim: &ClaimState) -> ClaimIntent {
    ClaimIntent::Post {
        standing: PostingStanding {
            binding: claim.binding(),
            standing: PredicateState::Passed,
            target: PredicateState::Passed,
        },
    }
}
fn acquiring(_claim: &ClaimState) -> ReceiptFence {
    fence(1)
}
enum TestIntent {
    Claim(ClaimIntent),
    Receipt(ReceiptFence),
    RequestEvaluation,
}
impl From<ClaimIntent> for TestIntent {
    fn from(value: ClaimIntent) -> Self {
        Self::Claim(value)
    }
}
impl From<ReceiptFence> for TestIntent {
    fn from(value: ReceiptFence) -> Self {
        Self::Receipt(value)
    }
}
fn apply(
    claim: &mut ClaimState,
    actor: Principal,
    intent: impl Into<TestIntent>,
) -> Result<(), ContractError> {
    match intent.into() {
        TestIntent::RequestEvaluation => {
            let aggregate = aggregation::ClaimAggregation::new(
                claim,
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 8,
                    max_results: 16,
                    max_updates: 16,
                },
            )?;
            claim.request_evaluation(&claim.binding(), actor, &aggregate.decision())
        }
        TestIntent::Claim(intent) => claim.apply(&claim.binding(), actor, intent),
        TestIntent::Receipt(request) => {
            let graph = graph::Snapshot::capture(&[claim], graph_limits())?;
            let start = graph.start(ClaimId(claim.binding().object.0))?;
            let aggregate = aggregation::ClaimAggregation::new(
                claim,
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 8,
                    max_results: 16,
                    max_updates: 16,
                },
            )?;
            claim.acquire_receipt(
                &claim.binding(),
                actor,
                request,
                &aggregate.admission(),
                &start,
                &[],
            )
        }
    }
}
fn posted() -> ClaimState {
    let mut claim = generated(3);
    let event = posting(&claim);
    apply(&mut claim, Principal::Actor(ISSUER), event).unwrap();
    claim
}
fn received() -> ClaimState {
    let mut claim = posted();
    let event = acquiring(&claim);
    apply(&mut claim, Principal::Actor(SUBJECT), event).unwrap();
    claim
}
fn link(claim: &ClaimState) -> ResponseLink {
    ResponseLink {
        testament: TestamentId::from_u128(100 + claim.response_count() as u128),
        content: ContentHash([11; 32]),
        receipt: claim.receipt().unwrap().fence,
        cycle: claim.response_count() as u32 + 1,
        prior: claim.latest_response().map(|row| row.testament),
    }
}
fn response(
    claim: &mut ClaimState,
    actor: Principal,
    link: ResponseLink,
    event: ResponseEvent,
) -> Result<(), ContractError> {
    claim.record_response(&claim.binding(), actor, link, ReportStamp::fixture(), event)
}
fn first_response() -> (ClaimState, ResponseLink) {
    let mut claim = received();
    let first = link(&claim);
    response(
        &mut claim,
        Principal::Actor(SUBJECT),
        first,
        ResponseEvent::Generated,
    )
    .unwrap();
    (claim, first)
}
fn acknowledged() -> ClaimState {
    let (mut claim, first) = first_response();
    response(
        &mut claim,
        Principal::Actor(SUBJECT),
        first,
        ResponseEvent::Posted,
    )
    .unwrap();
    response(
        &mut claim,
        Principal::Actor(ISSUER),
        first,
        ResponseEvent::Received,
    )
    .unwrap();
    claim
}
fn validating() -> ClaimState {
    let mut claim = acknowledged();
    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        TestIntent::RequestEvaluation,
    )
    .unwrap();
    claim
}
#[test]
fn generation_requires_actor_issuer_and_valid_bounded_identity() {
    let valid = generated(3);
    for principal in [
        Principal::Actor(SUBJECT),
        Principal::Actor(OTHER),
        Principal::Node(ISSUER),
        Principal::Node(SUBJECT),
    ] {
        assert_eq!(
            ClaimState::generate(principal, definition(3)),
            Err(ContractError::WrongActor)
        );
    }
    assert_eq!(
        ClaimState::generate(Principal::Actor(ISSUER), definition(0)),
        Err(ContractError::Capacity)
    );
    let zero = Binding {
        object: ObjectId::from_u128(0),
        ..valid.binding()
    };
    assert_eq!(
        ClaimState::generate(
            Principal::Actor(ISSUER),
            ClaimDefinition {
                binding: zero,
                ..definition(3)
            }
        ),
        Err(ContractError::InvalidTarget)
    );
    assert_eq!(valid.status(), ClaimStatus::Generated);
    assert_eq!(valid.receipt(), None);
    assert!(!valid.local_complete());
}

#[test]
fn post_receipt_and_progress_follow_the_exact_actor_role_matrix() {
    for principal in [
        Principal::Actor(ISSUER),
        Principal::Actor(SUBJECT),
        Principal::Actor(OTHER),
        Principal::Node(ISSUER),
        Principal::Node(SUBJECT),
    ] {
        let mut claim = generated(3);
        let before = claim.clone();
        let event = posting(&claim);
        let result = apply(&mut claim, principal, event);
        if principal == Principal::Actor(ISSUER) {
            assert!(result.is_ok());
            assert_eq!(claim.status(), ClaimStatus::Posted);
        } else {
            assert_eq!(result, Err(ContractError::WrongActor));
            assert_eq!(claim, before);
        }
        let mut claim = posted();
        let before = claim.clone();
        let event = acquiring(&claim);
        let result = apply(&mut claim, principal, event);
        if principal == Principal::Actor(SUBJECT) {
            assert!(result.is_ok());
            assert_eq!(claim.status(), ClaimStatus::Received);
        } else {
            assert_eq!(result, Err(ContractError::WrongActor));
            assert_eq!(claim, before);
        }
        let mut claim = received();
        let before = claim.clone();
        let result = apply(
            &mut claim,
            principal,
            ClaimIntent::Progress { receipt: fence(1) },
        );
        if principal == Principal::Actor(SUBJECT) {
            assert!(result.is_ok());
            assert_eq!(claim.status(), ClaimStatus::Progressed);
            assert!(!claim.local_complete());
        } else {
            assert_eq!(result, Err(ContractError::WrongActor));
            assert_eq!(claim, before);
        }
    }
}

#[test]
fn pending_failed_or_stale_standing_never_posts() {
    for state in [PredicateState::Pending, PredicateState::Failed] {
        for field in 0..2 {
            let mut claim = generated(3);
            let before = claim.clone();
            let mut standing = PostingStanding {
                binding: claim.binding(),
                standing: PredicateState::Passed,
                target: PredicateState::Passed,
            };
            if field == 0 {
                standing.standing = state;
            } else {
                standing.target = state;
            }
            assert!(
                apply(
                    &mut claim,
                    Principal::Actor(ISSUER),
                    ClaimIntent::Post { standing }
                )
                .is_err()
            );
            assert_eq!(claim, before);
        }
    }
    let mut claim = generated(3);
    let stale = posting(&claim);
    let event = posting(&claim);
    apply(&mut claim, Principal::Actor(ISSUER), event).unwrap();
    let before = claim.clone();
    assert_eq!(
        apply(&mut claim, Principal::Actor(ISSUER), stale),
        Err(ContractError::StaleRevision)
    );
    assert_eq!(claim, before);
}

#[test]
fn first_and_later_responses_record_independent_facts_without_phase_regression() {
    for attained in [
        ClaimStatus::TestamentGenerated,
        ClaimStatus::TestamentAcknowledged,
        ClaimStatus::Validating,
    ] {
        let (mut claim, first) = first_response();
        if attained != ClaimStatus::TestamentGenerated {
            response(
                &mut claim,
                Principal::Actor(SUBJECT),
                first,
                ResponseEvent::Posted,
            )
            .unwrap();
            response(
                &mut claim,
                Principal::Actor(ISSUER),
                first,
                ResponseEvent::Received,
            )
            .unwrap();
        }
        if attained == ClaimStatus::Validating {
            apply(
                &mut claim,
                Principal::Actor(ISSUER),
                TestIntent::RequestEvaluation,
            )
            .unwrap();
        }
        assert_eq!(claim.status(), attained);
        let later = link(&claim);
        for (principal, event) in [
            (Principal::Actor(SUBJECT), ResponseEvent::Generated),
            (Principal::Actor(SUBJECT), ResponseEvent::Posted),
            (Principal::Actor(ISSUER), ResponseEvent::Received),
        ] {
            response(&mut claim, principal, later, event).unwrap();
            let expected = if attained == ClaimStatus::TestamentGenerated
                && event == ResponseEvent::Received
            {
                ClaimStatus::TestamentAcknowledged
            } else {
                attained
            };
            assert_eq!(claim.status(), expected);
            assert!(!claim.local_complete());
        }
        if attained == ClaimStatus::TestamentGenerated {
            response(
                &mut claim,
                Principal::Actor(SUBJECT),
                first,
                ResponseEvent::Posted,
            )
            .unwrap();
            assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
            response(
                &mut claim,
                Principal::Actor(ISSUER),
                first,
                ResponseEvent::Received,
            )
            .unwrap();
            assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
        }
        assert_eq!(claim.response_count(), 2);
        assert_eq!(claim.latest_response(), Some(later));
    }
}

#[test]
fn receiving_the_second_authored_response_first_permits_evaluation_without_receiving_the_first() {
    use super::super::evidence::{
        CloseReport, Response, ResponseIdentity, ResponseLimits, ResponseState,
    };

    fn close_and_post(claim: &mut ClaimState, id: u128) -> Response {
        let parent = Parent::from_claim(claim).unwrap();
        let mut response = Response::close(
            ResponseIdentity {
                binding: Binding {
                    object: ObjectId::from_u128(id),
                    revision: ObjectRevision(1),
                    ..claim.binding()
                },
                claim: parent.claim,
                receipt: parent.receipt,
                cycle: parent.next_cycle,
                prior: parent.latest_response,
            },
            &parent,
            Principal::Actor(SUBJECT),
            &[],
            &[],
            CloseReport {
                summary: "The requested work is complete.",
                confidence: crate::Confidence::Committed,
                outcome: crate::OutcomeKind::Complete,
                diagnostics: &[],
                limits: ResponseLimits {
                    artifacts: 0,
                    diagnostics: 0,
                    summary_bytes: 128,
                    construction_bytes: 64 * 1024,
                },
            },
        )
        .unwrap()
        .response;
        claim
            .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &response)
            .unwrap();
        let parent = Parent::from_claim(claim).unwrap();
        let posted = response
            .plan_post(
                &response.identity().binding,
                &parent,
                Principal::Actor(SUBJECT),
            )
            .unwrap();
        response.apply(posted).unwrap();
        claim
            .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &response)
            .unwrap();
        response
    }

    let mut claim = received();
    let entitlement = claim.receipt().unwrap();
    let mut first = close_and_post(&mut claim, 101);
    let mut second = close_and_post(&mut claim, 102);
    assert_eq!(first.identity().cycle, 1);
    assert_eq!(second.identity().cycle, 2);
    assert_eq!(second.identity().prior, Some(TestamentId::from_u128(101)));
    assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
    assert_eq!(first.state(), ResponseState::Posted);
    assert_eq!(second.state(), ResponseState::Posted);

    let parent = Parent::from_claim(&claim).unwrap();
    let received = second
        .plan_receive(
            &second.identity().binding,
            &parent,
            Principal::Actor(ISSUER),
        )
        .unwrap();
    second.apply(received).unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(ISSUER), &second)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
    assert_eq!(first.state(), ResponseState::Posted);
    assert_eq!(second.state(), ResponseState::Received);
    assert_eq!(
        claim.received_report(
            first.identity().binding,
            parent.receipt,
            first.report_stamp()
        ),
        Err(ContractError::InvalidTarget)
    );
    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        TestIntent::RequestEvaluation,
    )
    .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Validating);
    assert_eq!(first.state(), ResponseState::Posted);

    let parent = Parent::from_claim(&claim).unwrap();
    let received = first
        .plan_receive(&first.identity().binding, &parent, Principal::Actor(ISSUER))
        .unwrap();
    first.apply(received).unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(ISSUER), &first)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Validating);
    assert_eq!(claim.receipt(), Some(entitlement));
    assert_eq!(claim.response_count(), 2);
    assert_eq!(
        claim.latest_response().unwrap().testament,
        TestamentId::from_u128(102)
    );
    assert_eq!(first.state(), ResponseState::Received);
    assert_eq!(second.state(), ResponseState::Received);
    assert!(!claim.local_complete());
}

#[test]
fn response_roles_identity_lineage_and_posted_receipt_are_checked_without_partial_change() {
    let (original, first) = first_response();
    for principal in [
        Principal::Actor(SUBJECT),
        Principal::Actor(OTHER),
        Principal::Node(ISSUER),
    ] {
        let mut claim = original.clone();
        let before = claim.clone();
        assert_eq!(
            response(&mut claim, principal, first, ResponseEvent::Received),
            Err(ContractError::WrongActor)
        );
        assert_eq!(claim, before);
    }
    let mut claim = original.clone();
    let before = claim.clone();
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(ISSUER),
            first,
            ResponseEvent::Received
        ),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(claim, before);
    for malformed in [
        ResponseLink {
            content: ContentHash([99; 32]),
            ..first
        },
        ResponseLink { cycle: 99, ..first },
        ResponseLink {
            prior: Some(first.testament),
            ..first
        },
    ] {
        let mut claim = original.clone();
        let before = claim.clone();
        assert_eq!(
            response(
                &mut claim,
                Principal::Actor(SUBJECT),
                malformed,
                ResponseEvent::Posted
            ),
            Err(ContractError::InvalidTarget)
        );
        assert_eq!(claim, before);
    }
    let mut claim = original;
    let next = link(&claim);
    let before = claim.clone();
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(SUBJECT),
            ResponseLink {
                prior: None,
                ..next
            },
            ResponseEvent::Generated
        ),
        Err(ContractError::InvalidTarget)
    );
    assert_eq!(claim, before);
}

#[test]
fn adoption_requires_issuer_and_fences_old_holder_response_and_delivery_authority() {
    let original = acknowledged();
    let replacement = ReceiptEntitlement {
        holder: OTHER,
        fence: fence(2),
    };
    for principal in [
        Principal::Actor(SUBJECT),
        Principal::Actor(OTHER),
        Principal::Node(ISSUER),
    ] {
        let mut claim = original.clone();
        let before = claim.clone();
        assert_eq!(
            apply(
                &mut claim,
                principal,
                ClaimIntent::AdoptReceipt {
                    previous: fence(1),
                    replacement
                }
            ),
            Err(ContractError::WrongActor)
        );
        assert_eq!(claim, before);
    }
    let mut claim = original;
    let first = claim.latest_response().unwrap();
    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        ClaimIntent::AdoptReceipt {
            previous: fence(1),
            replacement,
        },
    )
    .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
    assert_eq!(claim.receipt(), Some(replacement));
    let before = claim.clone();
    assert_eq!(
        apply(
            &mut claim,
            Principal::Actor(ISSUER),
            TestIntent::RequestEvaluation
        ),
        Err(ContractError::StaleReceipt)
    );
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(ISSUER),
            first,
            ResponseEvent::Received
        ),
        Err(ContractError::StaleReceipt)
    );
    assert_eq!(claim, before);
    let later = link(&claim);
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(SUBJECT),
            later,
            ResponseEvent::Generated
        ),
        Err(ContractError::WrongActor)
    );
    response(
        &mut claim,
        Principal::Actor(OTHER),
        later,
        ResponseEvent::Generated,
    )
    .unwrap();
    response(
        &mut claim,
        Principal::Actor(OTHER),
        later,
        ResponseEvent::Posted,
    )
    .unwrap();
    response(
        &mut claim,
        Principal::Actor(ISSUER),
        later,
        ResponseEvent::Received,
    )
    .unwrap();
    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        TestIntent::RequestEvaluation,
    )
    .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Validating);
}

#[test]
fn adoption_before_first_delivery_allows_replacement_cycle_to_enter_evaluation() {
    for original_posted in [false, true] {
        let (mut claim, abandoned) = first_response();
        if original_posted {
            response(
                &mut claim,
                Principal::Actor(SUBJECT),
                abandoned,
                ResponseEvent::Posted,
            )
            .unwrap();
        }
        let old_record = claim.responses.first().copied().unwrap();
        apply(
            &mut claim,
            Principal::Actor(ISSUER),
            ClaimIntent::AdoptReceipt {
                previous: fence(1),
                replacement: ReceiptEntitlement {
                    holder: OTHER,
                    fence: fence(2),
                },
            },
        )
        .unwrap();
        assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
        assert_eq!(
            response(
                &mut claim,
                Principal::Actor(ISSUER),
                abandoned,
                ResponseEvent::Received
            ),
            Err(ContractError::StaleReceipt)
        );
        let replacement = link(&claim);
        assert_eq!(replacement.prior, Some(abandoned.testament));
        assert_eq!(replacement.cycle, 2);
        response(
            &mut claim,
            Principal::Actor(OTHER),
            replacement,
            ResponseEvent::Generated,
        )
        .unwrap();
        response(
            &mut claim,
            Principal::Actor(OTHER),
            replacement,
            ResponseEvent::Posted,
        )
        .unwrap();
        assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
        response(
            &mut claim,
            Principal::Actor(ISSUER),
            replacement,
            ResponseEvent::Received,
        )
        .unwrap();
        assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
        assert_eq!(claim.responses.first(), Some(&old_record));
        apply(
            &mut claim,
            Principal::Actor(ISSUER),
            TestIntent::RequestEvaluation,
        )
        .unwrap();
        assert_eq!(claim.status(), ClaimStatus::Validating);
        assert_eq!(claim.responses.first(), Some(&old_record));
    }
}

#[test]
fn local_completion_seals_responses_and_waits_for_graph_before_satisfaction() {
    let mut claim = validating();
    let before = graph::Snapshot::capture(&[&claim], graph_limits()).unwrap();
    assert!(matches!(
        before.release(ClaimId(claim.binding().object.0)),
        Err(ContractError::InvalidTransition)
    ));
    claim
        .apply_derived(
            &claim.binding(),
            DerivedClaimFact::LocalCompletion {
                sequence: SessionSeq(1),
            },
        )
        .unwrap();
    assert!(claim.local_complete());
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(1)));
    assert_eq!(claim.status(), ClaimStatus::Validating);
    let before = claim.clone();
    let later = link(&claim);
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(SUBJECT),
            later,
            ResponseEvent::Generated
        ),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(
        apply(
            &mut claim,
            Principal::Actor(ISSUER),
            ClaimIntent::AdoptReceipt {
                previous: fence(1),
                replacement: ReceiptEntitlement {
                    holder: OTHER,
                    fence: fence(2)
                }
            }
        ),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(claim, before);
    let snapshot = graph::Snapshot::capture(&[&claim], graph_limits()).unwrap();
    let release = snapshot.release(ClaimId(claim.binding().object.0)).unwrap();
    claim
        .graph_release(&claim.binding(), &release, &[], SessionSeq(2))
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Satisfied);
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(1)));
    assert_eq!(
        claim.terminal_cut(),
        Some(ClaimTerminalCut::Explicit(ClaimCut {
            position: SessionSeq(2),
            cause: release.fingerprint().unwrap()
        }))
    );
}

#[test]
fn fenced_or_sealed_evaluator_cannot_enter_claim_validation() {
    use super::validation as v;
    let original = acknowledged();
    let response = original.latest_response().unwrap();
    let declaration = v::Declaration::new(
        Principal::Actor(ISSUER),
        v::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(500),
                ..original.binding()
            },
            claim: ClaimId(original.binding().object.0),
            issuer: ISSUER,
            declaration_index: 0,
            kind: crate::ValidationKind::Inspection,
            phase: crate::ValidationPhase::WholeWork,
            mode: crate::ValidationMode::Required,
            target: v::TargetDeclaration::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            program: v::tests::programmatic(false),
            deadline: deadline(),
        },
        v::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap();
    let target = v::Target::Artifact {
        response: Binding {
            object: ObjectId(response.testament.0),
            content: response.content,
            ..original.binding()
        },
        slot: 0,
        artifact: Binding {
            object: ObjectId::from_u128(900),
            ..original.binding()
        },
    };
    let ready = v::Evaluation::materialize(
        Principal::Actor(ISSUER),
        &declaration,
        v::Materialization {
            binding: declaration.binding(),
            target,
            slot_name: Some("output"),
            generation: 1,
            receipt: Some(fence(1)),
        },
    )
    .unwrap();
    let begun = ready
        .begin(
            Principal::Actor(ready.evaluator().unwrap()),
            &ready.binding(),
            &v::tests::owner_for(&ready),
        )
        .unwrap()
        .next;
    let mut fence_owner = v::tests::owner_for(&begun);
    fence_owner.authority.state = v::AuthorityState::Fenced(v::AuthorityFence {
        reason: v::FenceReason::Evaluation,
        cause: ContentHash([77; 32]),
    });
    let fenced = begun.record_fence(&begun.binding(), &fence_owner).unwrap();
    let mut seal_owner = v::tests::owner_for(&begun);
    seal_owner.cohort = v::Cohort::Sealed {
        cause: ContentHash([78; 32]),
    };
    let sealed = begun.record_seal(&begun.binding(), &seal_owner).unwrap();
    for rejected in [ready, fenced, sealed] {
        let mut claim = original.clone();
        assert_eq!(
            claim.observe_evaluation(
                &claim.binding(),
                &rejected,
                &aggregation::ClaimAggregation::new(
                    &claim,
                    aggregation::Limits {
                        max_slots: 8,
                        max_checks: 8,
                        max_results: 16,
                        max_updates: 16
                    }
                )
                .unwrap()
                .decision()
            ),
            Err(ContractError::InvalidTransition)
        );
        assert_eq!(claim, original);
    }
    let mut claim = original;
    claim
        .observe_evaluation(
            &claim.binding(),
            &begun,
            &aggregation::ClaimAggregation::new(
                &claim,
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 8,
                    max_results: 16,
                    max_updates: 16,
                },
            )
            .unwrap()
            .decision(),
        )
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Validating);
}

#[test]
fn local_sealing_records_the_original_cut_and_rejects_earlier_terminalization() {
    let mut claim = validating();
    assert_eq!(claim.local_sealed_at(), None);
    let before = claim.clone();
    assert_eq!(
        claim.apply_derived(
            &claim.binding(),
            DerivedClaimFact::LocalCompletion {
                sequence: SessionSeq(0)
            }
        ),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(claim, before);
    claim
        .apply_derived(
            &claim.binding(),
            DerivedClaimFact::LocalCompletion {
                sequence: SessionSeq(8),
            },
        )
        .unwrap();
    let before = claim.clone();
    assert_eq!(
        apply(
            &mut claim,
            Principal::Actor(ISSUER),
            ClaimIntent::Cancel { cut: cut(7) }
        ),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(claim, before);
    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        ClaimIntent::Cancel { cut: cut(9) },
    )
    .unwrap();
    assert_eq!(claim.local_sealed_at(), Some(SessionSeq(8)));
    assert_eq!(
        claim.terminal_cut(),
        Some(ClaimTerminalCut::Explicit(cut(9)))
    );
    let mut direct = posted();
    apply(
        &mut direct,
        Principal::Actor(ISSUER),
        ClaimIntent::Cancel { cut: cut(4) },
    )
    .unwrap();
    assert_eq!(direct.local_sealed_at(), Some(SessionSeq(4)));
    assert!(!direct.local_complete());
}

#[test]
fn all_twenty_states_preserve_terminal_status_content_and_original_cut() {
    assert_eq!(ClaimStatus::ALL.len(), 20);
    let mut terminal_count = 0;
    for status in ClaimStatus::ALL {
        let mut claim = validating();
        claim.status = *status;
        claim.terminal_cut = terminal(*status).then_some(ClaimTerminalCut::Explicit(cut(4)));
        claim.local_sealed_at = terminal(*status).then_some(SessionSeq(4));
        assert_eq!(terminal(*status), status.is_terminal());
        if !terminal(*status) {
            continue;
        }
        terminal_count += 1;
        let before = claim.clone();
        let first = claim.latest_response().unwrap();
        let intents = [
            posting(&claim),
            ClaimIntent::Progress { receipt: fence(1) },
            ClaimIntent::AdoptReceipt {
                previous: fence(1),
                replacement: ReceiptEntitlement {
                    holder: OTHER,
                    fence: fence(2),
                },
            },
            ClaimIntent::Cancel { cut: cut(5) },
            ClaimIntent::Revoke { cut: cut(5) },
        ];
        for intent in intents {
            assert_eq!(
                apply(&mut claim, Principal::Actor(ISSUER), intent),
                Err(ContractError::InvalidTransition)
            );
            assert_eq!(claim, before);
        }
        for event in [
            ResponseEvent::Generated,
            ResponseEvent::Posted,
            ResponseEvent::Received,
        ] {
            assert_eq!(
                response(&mut claim, Principal::Actor(ISSUER), first, event),
                Err(ContractError::InvalidTransition)
            );
            assert_eq!(claim, before);
        }
        assert_eq!(
            claim.expire(&claim.binding(), deadline(), 100, cut(5)),
            Err(ContractError::InvalidTransition)
        );
        assert_eq!(
            claim.apply_derived(
                &claim.binding(),
                DerivedClaimFact::LocalCompletion {
                    sequence: SessionSeq(1)
                }
            ),
            Err(ContractError::InvalidTransition)
        );
        assert_eq!(claim, before);
    }
    assert_eq!(terminal_count, 13);
}

#[test]
fn controls_are_issuer_only_and_matching_deadline_is_a_separate_owner_fact() {
    for control in [
        ClaimIntent::Cancel { cut: cut(2) },
        ClaimIntent::Revoke { cut: cut(2) },
    ] {
        for principal in [
            Principal::Actor(SUBJECT),
            Principal::Actor(OTHER),
            Principal::Node(ISSUER),
        ] {
            let mut claim = received();
            let before = claim.clone();
            assert_eq!(
                apply(&mut claim, principal, control),
                Err(ContractError::WrongActor)
            );
            assert_eq!(claim, before);
        }
        let mut claim = received();
        apply(&mut claim, Principal::Actor(ISSUER), control).unwrap();
        assert!(terminal(claim.status()));
    }
    let mut claim = generated(3);
    let before = claim.clone();
    assert_eq!(
        claim.expire(
            &claim.binding(),
            Deadline {
                generation: 99,
                ..deadline()
            },
            100,
            cut(1)
        ),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(
        claim.expire(&claim.binding(), deadline(), 99, cut(1)),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(
        claim.expire(&claim.binding(), deadline(), 100, cut(0)),
        Err(ContractError::InvalidCut)
    );
    assert_eq!(claim, before);
    claim
        .expire(&claim.binding(), deadline(), 100, cut(1))
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::Expired);
}

#[test]
fn boundary_failures_require_real_durable_evidence_and_the_narrow_phase_writer() {
    let artifact = ArtifactRef {
        id: ArtifactId::from_u128(200),
        hash: ContentHash([8; 32]),
    };
    let diagnostic = Diagnostic {
        reason: super::super::evidence::EvidenceFailure::Production,
        artifact,
    };
    let custody = EvidenceAttestation {
        descriptor_hash: artifact.hash,
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    };
    for (original, boundary, writer, status) in [
        (
            generated(3),
            BoundaryFailure::Post,
            ISSUER,
            ClaimStatus::PostFailed,
        ),
        (
            posted(),
            BoundaryFailure::Receipt,
            SUBJECT,
            ClaimStatus::ReceiptFailed,
        ),
    ] {
        for principal in [Principal::Actor(OTHER), Principal::Node(writer)] {
            let mut claim = original.clone();
            let before = claim.clone();
            assert_eq!(
                claim.report_boundary_failure(
                    &claim.binding(),
                    principal,
                    boundary,
                    diagnostic,
                    &custody,
                    cut(1)
                ),
                Err(ContractError::WrongActor)
            );
            assert_eq!(claim, before);
        }
        let mut claim = original;
        let before = claim.clone();
        assert_eq!(
            claim.report_boundary_failure(
                &claim.binding(),
                Principal::Actor(writer),
                boundary,
                diagnostic,
                &EvidenceAttestation {
                    durable: false,
                    ..custody.clone()
                },
                cut(1)
            ),
            Err(ContractError::MissingEvidence)
        );
        assert_eq!(claim, before);
        claim
            .report_boundary_failure(
                &claim.binding(),
                Principal::Actor(writer),
                boundary,
                diagnostic,
                &custody,
                cut(1),
            )
            .unwrap();
        assert_eq!(claim.status(), status);
    }
}

fn response_diagnostic(claim: &ClaimState) -> ResponseDiagnostic {
    use crate::CanonicalContent;
    let parent = Parent::from_claim(claim).unwrap();
    let content = crate::ArtifactContent {
        ledger: parent.ledger,
        schema: 1,
        kind: "error".into(),
        schema_hash: ContentHash([20; 32]),
        metadata: vec![],
        payload: crate::ArtifactPayload::Inline(b"tool failed; no requested work product".to_vec()),
        producer: parent.holder,
        receipt: Some(parent.receipt),
        inputs: std::collections::BTreeSet::new(),
        visibility: std::collections::BTreeSet::new(),
    };
    let hash = content.content_hash().unwrap();
    let artifact = crate::Artifact::new(
        content,
        hash,
        crate::ArtifactLifecycle {
            created: SessionSeq(1),
            custody_revision: 1,
        },
    );
    let diagnostic = Diagnostic {
        reason: super::super::evidence::EvidenceFailure::Work,
        artifact: ArtifactRef {
            id: ArtifactId::from_u128(200),
            hash,
        },
    };
    ResponseDiagnostic::record(
        &parent,
        Principal::Actor(parent.holder),
        parent.receipt,
        diagnostic,
        (diagnostic.artifact.id, &artifact),
        &EvidenceAttestation {
            descriptor_hash: hash,
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        },
    )
    .unwrap()
}

fn authored_response(
    claim: &ClaimState,
    summary: &str,
    outcome: crate::OutcomeKind,
    diagnostics: &[ResponseDiagnostic],
) -> super::super::evidence::Response {
    use super::super::evidence::{CloseReport, Response, ResponseIdentity, ResponseLimits};
    let parent = Parent::from_claim(claim).unwrap();
    Response::close(
        ResponseIdentity {
            binding: Binding {
                object: ObjectId::from_u128(100),
                revision: ObjectRevision(1),
                ..claim.binding()
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
        CloseReport {
            summary,
            confidence: crate::Confidence::Committed,
            outcome,
            diagnostics,
            limits: ResponseLimits {
                artifacts: 0,
                diagnostics: 4,
                summary_bytes: 1024,
                construction_bytes: 64 * 1024,
            },
        },
    )
    .unwrap()
    .response
}

#[test]
fn checked_response_history_keeps_late_receipt_separate_from_claimant_acceptance() {
    use super::super::evidence::ResponseState;

    let mut claim = received();
    let mut testament = authored_response(
        &claim,
        "The requested work is complete.",
        crate::OutcomeKind::Complete,
        &[],
    );
    let substituted = authored_response(
        &claim,
        "A different report with the same externally supplied binding.",
        crate::OutcomeKind::Complete,
        &[],
    );
    assert_eq!(
        claim.response_history(&testament),
        Err(ContractError::InvalidTarget)
    );
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &testament)
        .unwrap();
    let history = claim.response_history(&testament).unwrap();
    assert!(!history.posted());
    assert!(!history.received());
    assert_eq!(
        claim.response_history(&substituted),
        Err(ContractError::ContentConflict)
    );

    let parent = Parent::from_claim(&claim).unwrap();
    let transition = testament
        .plan_post(
            &testament.identity().binding,
            &parent,
            Principal::Actor(SUBJECT),
        )
        .unwrap();
    testament.apply(transition).unwrap();
    assert!(!claim.response_history(&testament).unwrap().posted());
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &testament)
        .unwrap();
    let history = claim.response_history(&testament).unwrap();
    assert!(history.posted());
    assert!(!history.received());

    let mut adopted = claim.clone();
    apply(
        &mut adopted,
        Principal::Actor(ISSUER),
        ClaimIntent::AdoptReceipt {
            previous: parent.receipt,
            replacement: ReceiptEntitlement {
                holder: OTHER,
                fence: fence(2),
            },
        },
    )
    .unwrap();
    assert_eq!(adopted.response_history(&testament).unwrap(), history);
    assert_eq!(
        adopted.received_report(
            testament.identity().binding,
            parent.receipt,
            testament.report_stamp()
        ),
        Err(ContractError::StaleReceipt)
    );

    apply(
        &mut claim,
        Principal::Actor(ISSUER),
        ClaimIntent::Cancel { cut: cut(10) },
    )
    .unwrap();
    let terminal_claim = claim.clone();
    let parent = Parent::from_claim(&claim).unwrap();
    let transition = testament
        .plan_receive(
            &testament.identity().binding,
            &parent,
            Principal::Actor(ISSUER),
        )
        .unwrap();
    testament.apply(transition).unwrap();
    assert_eq!(testament.state(), ResponseState::Received);
    assert_eq!(claim.response_history(&testament).unwrap(), history);
    assert_eq!(claim, terminal_claim);
    assert_eq!(
        claim.received_report(
            testament.identity().binding,
            parent.receipt,
            testament.report_stamp()
        ),
        Err(ContractError::InvalidTarget)
    );
}

#[test]
fn a_receipt_and_a_closing_incident_never_generate_or_terminalize_a_response() {
    use super::super::evidence::ResponseState;
    let mut claim = received();
    assert_eq!(claim.response_count(), 0);
    assert_eq!(claim.latest_response(), None);
    assert_eq!(claim.status(), ClaimStatus::Received);
    let diagnostic = response_diagnostic(&claim);
    let before = claim.clone();
    for principal in [
        Principal::Actor(ISSUER),
        Principal::Actor(OTHER),
        Principal::Node(SUBJECT),
    ] {
        assert_eq!(
            claim.plan_closing_failure(&claim.binding(), principal, diagnostic, cut(1)),
            Err(ContractError::WrongActor)
        );
    }
    assert_eq!(
        claim.plan_closing_failure(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            diagnostic,
            cut(0)
        ),
        Err(ContractError::InvalidCut)
    );
    let incident = claim
        .plan_closing_failure(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            diagnostic,
            cut(1),
        )
        .unwrap();
    assert_eq!(claim, before);
    assert_eq!(incident.claim(), claim.binding());
    assert_eq!(*incident.diagnostic(), diagnostic);
    assert_eq!(incident.cut(), cut(1));
    claim.apply_closing_incident(&incident).unwrap();
    assert_eq!(claim.response_count(), 0);
    assert_eq!(claim.status(), ClaimStatus::Received);
    assert_eq!(claim.terminal_cut(), None);
    assert_eq!(claim.local_sealed_at(), None);
    assert!(!claim.local_complete());
    assert_eq!(
        claim.apply_closing_incident(&incident),
        Err(ContractError::StaleRevision)
    );

    let mut testament = authored_response(
        &claim,
        "The tool failed; the error artifact explains why no binary was produced.",
        crate::OutcomeKind::Failed,
        &[diagnostic],
    );
    assert_eq!(testament.state(), ResponseState::Generated);
    assert!(testament.manifest().is_empty());
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &testament)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
    assert_eq!(claim.terminal_cut(), None);
    let parent = Parent::from_claim(&claim).unwrap();
    testament
        .apply(
            testament
                .plan_post(
                    &testament.identity().binding,
                    &parent,
                    Principal::Actor(SUBJECT),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &testament)
        .unwrap();
    assert_eq!(testament.state(), ResponseState::Posted);
    assert_eq!(claim.status(), ClaimStatus::TestamentGenerated);
    assert_eq!(
        claim.received_report(
            testament.identity().binding,
            parent.receipt,
            testament.report_stamp()
        ),
        Err(ContractError::InvalidTarget)
    );
    testament
        .apply(
            testament
                .plan_receive(
                    &testament.identity().binding,
                    &parent,
                    Principal::Actor(ISSUER),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(ISSUER), &testament)
        .unwrap();
    assert_eq!(claim.status(), ClaimStatus::TestamentAcknowledged);
    assert_eq!(testament.state(), ResponseState::Received);
    assert_eq!(testament.reported_outcome(), crate::OutcomeKind::Failed);
    assert_eq!(testament.diagnostics()[0], diagnostic);
    assert_eq!(testament.terminal(), None);
    assert_eq!(claim.terminal_cut(), None);
    assert!(!claim.local_complete());
    claim
        .received_report(
            testament.identity().binding,
            parent.receipt,
            testament.report_stamp(),
        )
        .unwrap();
}

#[test]
fn claim_observation_rejects_substituted_reports_and_stale_incident_publication() {
    let mut claim = received();
    let diagnostic = response_diagnostic(&claim);
    let incident = claim
        .plan_closing_failure(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            diagnostic,
            cut(1),
        )
        .unwrap();
    let original = authored_response(
        &claim,
        "I completed the work.",
        crate::OutcomeKind::Complete,
        &[],
    );
    let mut alternate = authored_response(
        &claim,
        "I failed to complete the work.",
        crate::OutcomeKind::Failed,
        &[diagnostic],
    );
    assert_eq!(original.identity(), alternate.identity());
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &original)
        .unwrap();
    let parent = Parent::from_claim(&claim).unwrap();
    alternate
        .apply(
            alternate
                .plan_post(
                    &alternate.identity().binding,
                    &parent,
                    Principal::Actor(SUBJECT),
                )
                .unwrap(),
        )
        .unwrap();
    let before = claim.clone();
    assert_eq!(
        claim.observe_response(&claim.binding(), Principal::Actor(SUBJECT), &alternate),
        Err(ContractError::ContentConflict)
    );
    assert_eq!(
        claim.apply_closing_incident(&incident),
        Err(ContractError::StaleRevision)
    );
    assert_eq!(claim, before);
    let mut original = original;
    original
        .apply(
            original
                .plan_post(
                    &original.identity().binding,
                    &parent,
                    Principal::Actor(SUBJECT),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(SUBJECT), &original)
        .unwrap();
    original
        .apply(
            original
                .plan_receive(
                    &original.identity().binding,
                    &parent,
                    Principal::Actor(ISSUER),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(ISSUER), &original)
        .unwrap();
    alternate
        .apply(
            alternate
                .plan_receive(
                    &alternate.identity().binding,
                    &parent,
                    Principal::Actor(ISSUER),
                )
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        claim.received_report(
            alternate.identity().binding,
            parent.receipt,
            alternate.report_stamp()
        ),
        Err(ContractError::ContentConflict)
    );
}

#[test]
fn revision_identity_and_capacity_failures_do_not_publish_partial_response_or_phase() {
    let mut claim = received();
    let before = claim.clone();
    for bad in [
        Binding {
            revision: ObjectRevision(0),
            ..claim.binding()
        },
        Binding {
            content: ContentHash([99; 32]),
            ..claim.binding()
        },
        Binding {
            object: ObjectId::from_u128(99),
            ..claim.binding()
        },
    ] {
        assert!(
            claim
                .apply(
                    &bad,
                    Principal::Actor(SUBJECT),
                    ClaimIntent::Progress { receipt: fence(1) }
                )
                .is_err()
        );
        assert_eq!(claim, before);
    }
    claim.binding.revision = ObjectRevision(u64::MAX);
    let before = claim.clone();
    let first = link(&claim);
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(SUBJECT),
            first,
            ResponseEvent::Generated
        ),
        Err(ContractError::Capacity)
    );
    assert_eq!(claim, before);
    let (mut claim, _) = first_response();
    claim.max_responses = 1;
    let before = claim.clone();
    let later = link(&claim);
    assert_eq!(
        response(
            &mut claim,
            Principal::Actor(SUBJECT),
            later,
            ResponseEvent::Generated
        ),
        Err(ContractError::Capacity)
    );
    assert_eq!(claim, before);
}

// A projection fixture for the graph algebra, including cycles whose historical
// local outcomes predate a later registered wait. It is not an admission history.
pub(crate) fn local_projection_for_graph(claim: &mut ClaimState) {
    claim.status = ClaimStatus::Validating;
    claim.local_complete = true;
    let registered = claim
        .scopes()
        .iter()
        .map(|scope| scope.registered().0)
        .max()
        .unwrap_or(0);
    claim.local_sealed_at = Some(SessionSeq(claim.created().0.max(registered) + 1));
    claim.binding = claim.binding.next().unwrap();
}

#[test]
fn receipt_requires_the_complete_pinned_admission_policy() {
    let mut definition = definition(3);
    let base = validation::DeclarationSpec {
        binding: Binding {
            object: ObjectId::from_u128(900),
            ..definition.binding
        },
        claim: ClaimId(definition.binding.object.0),
        issuer: ISSUER,
        declaration_index: 900,
        kind: crate::ValidationKind::Receipt,
        phase: crate::ValidationPhase::WholeWork,
        mode: crate::ValidationMode::Required,
        target: validation::TargetDeclaration::Delivery,
        program: validation::Program::Delivery,
        deadline: deadline(),
    };
    let limits = validation::Limits {
        handlers: 4,
        attempts: 8,
        slot_bytes: 64,
    };
    let delivery = validation::Declaration::new(Principal::Actor(ISSUER), base, limits).unwrap();
    let admission = validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(901),
                ..definition.binding
            },
            declaration_index: 901,
            kind: crate::ValidationKind::Inspection,
            phase: crate::ValidationPhase::Admission,
            target: validation::TargetDeclaration::Admission,
            program: validation::tests::programmatic(false),
            ..base
        },
        limits,
    )
    .unwrap();
    let bounds = aggregation::Limits {
        max_slots: 8,
        max_checks: 16,
        max_results: 16,
        max_updates: 16,
    };
    definition.acceptance = aggregation::AcceptancePolicy::new(
        definition.binding,
        ISSUER,
        &[],
        &[delivery, admission],
        bounds,
    )
    .unwrap();
    let mut claim = ClaimState::generate(Principal::Actor(ISSUER), definition).unwrap();
    let event = posting(&claim);
    apply(&mut claim, Principal::Actor(ISSUER), event).unwrap();
    let aggregate = aggregation::ClaimAggregation::new(&claim, bounds).unwrap();
    let graph = graph::Snapshot::capture(&[&claim], graph_limits()).unwrap();
    let start = graph.start(ClaimId(claim.binding().object.0)).unwrap();
    let before = claim.clone();
    assert_eq!(
        aggregate.admission().outcome(),
        aggregation::AdmissionOutcome::Pending
    );
    assert_eq!(
        claim.acquire_receipt(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            fence(1),
            &aggregate.admission(),
            &start,
            &[]
        ),
        Err(ContractError::InvalidTransition)
    );
    assert_eq!(claim, before);
    // A real checked policy with the same object identity but a truncated
    // declaration set still cannot supply this claim's admission proof.
    let alternate = posted();
    assert_eq!(alternate.binding(), claim.binding());
    let truncated = aggregation::ClaimAggregation::new(&alternate, bounds).unwrap();
    assert_eq!(
        truncated.admission().outcome(),
        aggregation::AdmissionOutcome::Passed
    );
    assert_eq!(
        claim.acquire_receipt(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            fence(1),
            &truncated.admission(),
            &start,
            &[]
        ),
        Err(ContractError::InvalidPolicy)
    );
    assert_eq!(claim, before);
}
