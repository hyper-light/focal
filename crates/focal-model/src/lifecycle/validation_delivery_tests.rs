use super::*;
use crate::lifecycle::{aggregation, claim, evidence, graph};
use crate::{Confidence, ObjectId, ObjectRevision, OutcomeKind, ReceiptId, SessionSeq, TimerId};

fn limits() -> aggregation::Limits {
    aggregation::Limits {
        max_slots: 4,
        max_checks: 4,
        max_results: 16,
        max_updates: 8,
    }
}

fn declaration(binding: Binding, issuer: ParticipantId, at: u64) -> Declaration {
    Declaration::new(
        Principal::Actor(issuer),
        DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(901),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer,
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: TargetDeclaration::Delivery,
            program: Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(902),
                generation: 1,
                at,
            },
        },
        Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn received_claim() -> (ClaimState, Declaration) {
    let mut definition = claim::tests::definition(4);
    let declaration = declaration(definition.binding, definition.issuer, 1000);
    definition.acceptance = aggregation::AcceptancePolicy::new(
        definition.binding,
        definition.issuer,
        &[],
        std::slice::from_ref(&declaration),
        limits(),
    )
    .unwrap();
    let mut claim = ClaimState::generate(Principal::Actor(definition.issuer), definition).unwrap();
    claim
        .post_owned(Principal::Actor(claim.issuer()), claim.binding())
        .unwrap();
    let snapshot = graph::Snapshot::capture(
        &[&claim],
        graph::Limits {
            nodes: 4,
            edges: 8,
            visits: 64,
        },
    )
    .unwrap();
    let start = snapshot.start(ClaimId(claim.binding().object.0)).unwrap();
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
    (claim, declaration)
}

fn generated_response(claim: &mut ClaimState) -> Response {
    let parent = Parent::from_claim(claim).unwrap();
    let response = Response::close(
        evidence::ResponseIdentity {
            binding: Binding {
                ledger: parent.ledger,
                object: ObjectId::from_u128(1000 + u128::from(parent.next_cycle)),
                content: ContentHash([7; 32]),
                revision: ObjectRevision(1),
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
        evidence::CloseReport {
            summary: "Work completed; no output slots were requested.",
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            diagnostics: &[],
            limits: evidence::ResponseLimits {
                artifacts: 0,
                diagnostics: 0,
                summary_bytes: 256,
                construction_bytes: 4096,
            },
        },
    )
    .unwrap()
    .response;
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), &response)
        .unwrap();
    response
}

fn post(claim: &mut ClaimState, response: &mut Response) {
    let parent = Parent::from_claim(claim).unwrap();
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
        .observe_response(&claim.binding(), Principal::Actor(parent.holder), response)
        .unwrap();
}

fn receive(claim: &mut ClaimState, response: &mut Response) {
    let parent = Parent::from_claim(claim).unwrap();
    response
        .apply(
            response
                .plan_receive(
                    &response.identity().binding,
                    &parent,
                    Principal::Actor(parent.issuer),
                )
                .unwrap(),
        )
        .unwrap();
    claim
        .observe_response(&claim.binding(), Principal::Actor(parent.issuer), response)
        .unwrap();
}

#[test]
fn actual_claimant_receipt_passes_without_an_external_attempt_or_artifact() {
    let (mut claim, declaration) = received_claim();
    let mut response = generated_response(&mut claim);
    post(&mut claim, &mut response);
    receive(&mut claim, &mut response);
    let principal = Principal::Actor(claim.issuer());
    let ready =
        Evaluation::materialize_delivery(principal, &declaration, &claim, &response).unwrap();
    assert_eq!(ready.state(), State::Ready);
    assert_eq!(ready.generation(), 1);
    let owner = ready.delivery_owner(&claim, &response, 30).unwrap();
    let transition = ready
        .receive_delivery(principal, &ready.binding(), &owner)
        .unwrap();
    let result = transition.result.unwrap();
    assert_eq!(result, transition.next.last_result().unwrap());
    assert_eq!(
        result.target(),
        Target::Delivery {
            response: response.identity().binding
        }
    );
    assert_eq!(result.binding(), declaration.binding().next().unwrap());
    assert_eq!(result.verdict(), VerdictValue::Pass);
    assert_eq!(result.resulting_state(), State::Validated);
    assert_eq!(result.phase(), Phase::Delivery);
    assert_eq!(result.receipt(), Some(response.identity().receipt));
    assert_eq!(result.attempt(), None);
    assert_eq!(result.evidence(), None);
    assert_eq!(result.programmatic_evidence(), None);
    assert_eq!(result.reporter(), None);
    assert!(!transition.next.has_begun());
    assert_eq!(claim.status(), crate::ClaimStatus::TestamentAcknowledged);
    assert!(!claim.local_complete());

    let mut later = generated_response(&mut claim);
    post(&mut claim, &mut later);
    receive(&mut claim, &mut later);
    let next = Evaluation::materialize_delivery(principal, &declaration, &claim, &later).unwrap();
    assert_eq!(next.generation(), 2);
    assert_ne!(next.target(), ready.target());
    assert!(ready.delivery_owner(&claim, &later, 30).is_err());
    assert_eq!(result.generation(), 1);
}

#[test]
fn generated_posted_and_unrecorded_receipt_cannot_manufacture_delivery_readiness() {
    let (mut claim, declaration) = received_claim();
    let principal = Principal::Actor(claim.issuer());
    let mut response = generated_response(&mut claim);
    assert!(Evaluation::materialize_delivery(principal, &declaration, &claim, &response).is_err());
    post(&mut claim, &mut response);
    assert!(Evaluation::materialize_delivery(principal, &declaration, &claim, &response).is_err());
    let parent = Parent::from_claim(&claim).unwrap();
    response
        .apply(
            response
                .plan_receive(&response.identity().binding, &parent, principal)
                .unwrap(),
        )
        .unwrap();
    assert!(Evaluation::materialize_delivery(principal, &declaration, &claim, &response).is_err());
    claim
        .observe_response(&claim.binding(), principal, &response)
        .unwrap();
    assert!(Evaluation::materialize_delivery(principal, &declaration, &claim, &response).is_ok());
}

#[test]
fn definition_actor_and_terminal_parent_fences_cannot_be_replaced_by_owner_flags() {
    let (mut claim, declared) = received_claim();
    let mut response = generated_response(&mut claim);
    post(&mut claim, &mut response);
    receive(&mut claim, &mut response);
    let principal = Principal::Actor(claim.issuer());
    assert!(
        Evaluation::materialize_delivery(
            Principal::Node(claim.issuer()),
            &declared,
            &claim,
            &response
        )
        .is_err()
    );
    assert!(
        Evaluation::materialize_delivery(
            Principal::Actor(claim.subject()),
            &declared,
            &claim,
            &response
        )
        .is_err()
    );
    let substituted = declaration(claim.acceptance().claim(), claim.issuer(), 2000);
    assert!(Evaluation::materialize_delivery(principal, &substituted, &claim, &response).is_err());
    let ready = Evaluation::materialize_delivery(principal, &declared, &claim, &response).unwrap();
    claim
        .apply(
            &claim.binding(),
            principal,
            claim::ClaimIntent::Cancel {
                cut: claim::ClaimCut {
                    position: SessionSeq(20),
                    cause: ContentHash([8; 32]),
                },
            },
        )
        .unwrap();
    assert!(ready.delivery_owner(&claim, &response, 30).is_err());
    assert!(Evaluation::materialize_delivery(principal, &declared, &claim, &response).is_err());
}

#[test]
fn declared_deadline_does_not_turn_a_late_observation_into_a_pass() {
    let (mut claim, declaration) = received_claim();
    let mut response = generated_response(&mut claim);
    post(&mut claim, &mut response);
    receive(&mut claim, &mut response);
    let principal = Principal::Actor(claim.issuer());
    let ready =
        Evaluation::materialize_delivery(principal, &declaration, &claim, &response).unwrap();
    let before = ready.into_state();
    for time in [1000, 1001] {
        let owner = ready.delivery_owner(&claim, &response, time).unwrap();
        assert_eq!(
            ready
                .receive_delivery(principal, &ready.binding(), &owner)
                .unwrap_err(),
            ContractError::StaleEvaluation
        );
        assert_eq!(ready.into_state(), before);
        assert_eq!(ready.last_result(), None);
        assert_eq!(response.state(), ResponseState::Received);
    }
    assert!(
        ready
            .receive_delivery(
                principal,
                &ready.binding(),
                &ready.delivery_owner(&claim, &response, 999).unwrap()
            )
            .is_ok()
    );
}
