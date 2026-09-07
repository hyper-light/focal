use super::*;
use focal_model::lifecycle::aggregation::PublicationPosition;

fn position(sequence: SessionSeq, ordinal: u32) -> PublicationPosition {
    PublicationPosition { sequence, ordinal }
}

fn closed(f: &mut Fixture, id: u128) {
    f.commit(SUBJECT, f.close(id, OutcomeKind::Complete, vec![], vec![]));
}

fn posted(f: &mut Fixture, id: u128) {
    closed(f, id);
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(id),
        },
    );
}

#[test]
fn authored_close_and_post_do_not_invent_receipt_or_evaluation_entry() {
    let mut f = Fixture::new();
    closed(&mut f, 900);
    let id = TestamentId::from_u128(900);
    let generated = f.owner.committed().response_record(id).unwrap();
    assert_eq!(generated.response().state(), ResponseState::Generated);
    assert_eq!((generated.received(), generated.entered()), (None, None));
    let authored_summary = generated.response().summary().to_owned();
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let posted = f.owner.committed().response_record(id).unwrap();
    assert_eq!(posted.response().state(), ResponseState::Posted);
    assert_eq!((posted.received(), posted.entered()), (None, None));
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let received = f.owner.committed().response_record(id).unwrap();
    assert_eq!(received.response().state(), ResponseState::Received);
    assert_eq!(received.received(), Some(position(outcome.sequence, 0)));
    assert!(received.entered().is_none());
    assert_eq!(received.response().summary(), authored_summary);
    assert!(matches!(
        f.owner.committed().event(outcome.sequence, 0).unwrap().fact,
        NativeFact::Response {
            state: ResponseState::Received,
            ..
        }
    ));
}

#[test]
fn pending_receipt_retry_discard_and_pinned_reads_preserve_exact_publication() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    let id = TestamentId::from_u128(900);
    let pinned = f.owner.pin(0, 100).unwrap();
    let baseline = f.owner.budget_stats();
    let claim = f.claim();
    let response = f.response(900);
    let request = request(ISSUER, 90);
    let input = || NativeInput {
        request,
        command: NativeCommand::ReceiveResponse {
            claim,
            expected: response,
        },
    };
    let NativeStaging::Prepared { candidate, outcome } =
        f.owner.prepare(context(ISSUER, 90), input(), None).unwrap()
    else {
        panic!("fresh receipt")
    };
    let expected = Some(position(outcome.sequence, 0));
    assert_eq!(
        f.owner.effective().response_record(id).unwrap().received(),
        expected
    );
    assert_eq!(
        f.owner
            .candidate(candidate)
            .unwrap()
            .response_record(id)
            .unwrap()
            .received(),
        expected
    );
    assert!(
        f.owner
            .committed()
            .response_record(id)
            .unwrap()
            .received()
            .is_none()
    );
    assert_eq!(
        pinned
            .with_response_record(id, 0, |row| (row.received(), row.entered()))
            .unwrap(),
        Some((None, None))
    );
    assert_eq!(
        f.owner.prepare(context(ISSUER, 0), input(), None).unwrap(),
        NativeStaging::Existing {
            candidate: Some(candidate),
            outcome
        }
    );
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.owner.budget_stats(), baseline);
    assert!(
        f.owner
            .effective()
            .response_record(id)
            .unwrap()
            .received()
            .is_none()
    );
    let NativeStaging::Prepared {
        candidate: replacement,
        outcome: repeated,
    } = f.owner.prepare(context(ISSUER, 90), input(), None).unwrap()
    else {
        panic!("restaged receipt")
    };
    assert_ne!(replacement, candidate);
    assert_eq!(repeated, outcome);
    f.owner.publish_after_durable(replacement).unwrap();
    assert_eq!(
        f.owner.committed().response_record(id).unwrap().received(),
        expected
    );
    assert_eq!(
        f.owner
            .prepare(context(ISSUER, 5000), input(), None)
            .unwrap(),
        NativeStaging::Existing {
            candidate: None,
            outcome
        }
    );
    assert_eq!(
        f.owner.committed().response_record(id).unwrap().received(),
        expected
    );
    assert_eq!(
        pinned
            .with_response_record(id, 0, |row| row.received())
            .unwrap(),
        Some(None)
    );
    f.owner.release(&pinned).unwrap();
}

#[test]
fn late_terminal_receipt_records_observation_without_rewriting_claim_history() {
    let mut f = Fixture::new();
    posted(&mut f, 900);
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    let view = f.owner.committed();
    let claim = view.claim(ClaimId::from_u128(1)).unwrap();
    let before = claim.try_copy(claim.copy_charge().unwrap()).unwrap();
    let membership = view
        .registrations(ClaimId::from_u128(1))
        .unwrap()
        .rows()
        .len();
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let view = f.owner.committed();
    let record = view.response_record(TestamentId::from_u128(900)).unwrap();
    assert_eq!(record.received(), Some(position(outcome.sequence, 0)));
    assert!(record.entered().is_none());
    assert_eq!(record.response().state(), ResponseState::Received);
    // Equality includes private response-history flags and the terminal cut.
    assert_eq!(view.claim(ClaimId::from_u128(1)).unwrap(), &before);
    assert_eq!(
        view.registrations(ClaimId::from_u128(1))
            .unwrap()
            .rows()
            .len(),
        membership
    );
    assert_eq!(
        (
            outcome.changed,
            outcome.evaluations,
            outcome.results,
            outcome.events
        ),
        (0, 0, 0, 1)
    );
}

#[test]
fn copied_neighbor_response_retains_receipt_position_and_the_pin_keeps_original_row() {
    let mut f = Fixture::with_limits(NativeLimits {
        range: RangeConfig {
            page_entries: 128,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 16,
        plan_edges: 256,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 16,
        ..NativeLimits::default()
    });
    posted(&mut f, 900);
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let id = TestamentId::from_u128(900);
    let pin = f.owner.pin(0, 100).unwrap();
    let before = f.owner.committed().response_record(id).unwrap() as *const _ as usize;
    let expected = Some(position(outcome.sequence, 0));
    // Adjacent response-key insertion rebuilds the shared leaf. The old record
    // remains alive through the pin, making pointer inequality prove a copy.
    closed(&mut f, 901);
    let record = f.owner.committed().response_record(id).unwrap();
    assert_ne!(record as *const _ as usize, before);
    assert_eq!((record.received(), record.entered()), (expected, None));
    assert_eq!(
        pin.with_response_record(id, 0, |row| (
            row as *const _ as usize,
            row.received(),
            row.entered()
        ))
        .unwrap(),
        Some((before, expected, None))
    );
    assert_eq!(
        f.owner
            .committed()
            .response_record(TestamentId::from_u128(901))
            .unwrap()
            .received(),
        None
    );
    f.owner.release(&pin).unwrap();
}

#[test]
fn checked_owned_transitions_require_strict_entry_order_and_copies_keep_both_cuts() {
    let mut f = Fixture::new();
    closed(&mut f, 900);
    let source = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    let generated =
        OwnedResponse::new(source.try_copy(source.retained_bytes().unwrap()).unwrap()).unwrap();
    let source_claim = f.owner.committed().claim(ClaimId::from_u128(1)).unwrap();
    let mut claim = source_claim
        .try_copy(source_claim.copy_charge().unwrap())
        .unwrap();
    let parent = evidence::Parent::from_claim(&claim).unwrap();
    let post = || {
        generated
            .get()
            .unwrap()
            .plan_post(
                &generated.get().unwrap().identity().binding,
                &parent,
                Principal::Actor(SUBJECT),
            )
            .unwrap()
    };
    assert!(
        generated
            .transition(post(), position(SessionSeq(0), 0))
            .is_err()
    );
    let posted = generated
        .transition(post(), position(SessionSeq(40), 0))
        .unwrap();
    claim
        .observe_response(
            &claim.binding(),
            Principal::Actor(SUBJECT),
            posted.get().unwrap(),
        )
        .unwrap();
    let parent = evidence::Parent::from_claim(&claim).unwrap();
    let receive = posted
        .get()
        .unwrap()
        .plan_receive(
            &posted.get().unwrap().identity().binding,
            &parent,
            Principal::Actor(ISSUER),
        )
        .unwrap();
    let received_at = position(SessionSeq(41), 2);
    let received = posted.transition(receive, received_at).unwrap();
    claim
        .observe_response(
            &claim.binding(),
            Principal::Actor(ISSUER),
            received.get().unwrap(),
        )
        .unwrap();
    let aggregate = aggregation::ClaimAggregation::new(
        &claim,
        aggregation::Limits {
            max_slots: 8,
            max_checks: 16,
            max_results: 32,
            max_updates: 32,
        },
    )
    .unwrap();
    let enter = || {
        received
            .get()
            .unwrap()
            .plan_begin(
                &received.get().unwrap().identity().binding,
                &claim,
                Principal::Actor(ISSUER),
                &aggregate.decision(),
            )
            .unwrap()
    };
    for invalid in [
        position(SessionSeq(0), 0),
        position(SessionSeq(40), u32::MAX),
        position(SessionSeq(41), 1),
        received_at,
    ] {
        assert!(matches!(
            received.transition(enter(), invalid),
            Err(NativeError::Contract(ContractError::InvalidCut))
        ));
        assert_eq!(received.get().unwrap().state(), ResponseState::Received);
        assert_eq!(received.record().unwrap().received(), Some(received_at));
        assert!(received.record().unwrap().entered().is_none());
    }
    let entered_at = position(SessionSeq(41), 3);
    let entered = received.transition(enter(), entered_at).unwrap();
    assert_eq!(entered.get().unwrap().state(), ResponseState::Validating);
    assert_eq!(
        (
            entered.record().unwrap().received(),
            entered.record().unwrap().entered()
        ),
        (Some(received_at), Some(entered_at))
    );
    let copied = entered.copy().unwrap();
    assert_ne!(
        copied.record().unwrap() as *const _,
        entered.record().unwrap() as *const _
    );
    assert_eq!(
        copied.heap_charge().unwrap(),
        entered.heap_charge().unwrap()
    );
    assert_eq!(
        copied.record().unwrap().response(),
        entered.record().unwrap().response()
    );
    assert_eq!(
        (
            copied.record().unwrap().received(),
            copied.record().unwrap().entered()
        ),
        (Some(received_at), Some(entered_at))
    );
    assert_eq!(
        (
            generated.record().unwrap().received(),
            generated.record().unwrap().entered()
        ),
        (None, None)
    );
    assert_eq!(
        (
            posted.record().unwrap().received(),
            posted.record().unwrap().entered()
        ),
        (None, None)
    );
    assert!(
        OwnedResponse::new(
            copied
                .get()
                .unwrap()
                .try_copy(copied.get().unwrap().retained_bytes().unwrap())
                .unwrap()
        )
        .is_err()
    );
}
