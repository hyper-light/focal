use super::*;

const CLAIM: ClaimId = ClaimId::from_u128(1);
const RESPONSE: TestamentId = TestamentId::from_u128(900);

fn assert_pending(fixture: &Fixture) {
    let budget = fixture.owner.budget_stats();
    let range = fixture.owner.range_stats();
    let prefix = fixture.owner.effective().sequence();
    let binding = fixture.claim();
    let pending = fixture.owner.pending_len();
    fixture
        .owner
        .with_effective_acceptance(CLAIM, |projection| {
            assert_eq!(
                fixture.owner.budget_stats().used,
                budget.used + projection.construction_charge()
            );
            let decision = projection.claim_decision();
            assert_eq!(decision.binding(), binding);
            assert_eq!(decision.sequence(), prefix);
            assert_eq!(decision.outcome(), aggregation::AggregateOutcome::Pending);
            assert!(projection.response_decision(RESPONSE).is_none());
            assert_eq!(decision.witnesses().count(), 0);
        })
        .unwrap();
    assert_eq!(fixture.owner.budget_stats(), budget);
    assert_eq!(fixture.owner.range_stats(), range);
    assert_eq!(fixture.owner.effective().sequence(), prefix);
    assert_eq!(fixture.claim(), binding);
    assert_eq!(fixture.owner.pending_len(), pending);
    assert!(
        !fixture
            .owner
            .effective()
            .claim(CLAIM)
            .unwrap()
            .local_complete()
    );
}

fn posted(fixture: &mut Fixture) {
    let left = fixture.work(801, 0);
    let right = fixture.work(802, 1);
    fixture.commit(
        SUBJECT,
        fixture.close(900, OutcomeKind::Complete, vec![left, right], vec![]),
    );
    fixture.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
}

#[test]
fn custody_and_receipt_do_not_enter_or_complete_zero_check_response_slots() {
    let mut fixture = Fixture::new();
    assert_pending(&fixture);
    let left = fixture.work(801, 0);
    let right = fixture.work(802, 1);
    assert_pending(&fixture);
    fixture.commit(
        SUBJECT,
        fixture.close(900, OutcomeKind::Complete, vec![left, right], vec![]),
    );
    assert_eq!(
        fixture
            .owner
            .effective()
            .response(RESPONSE)
            .unwrap()
            .state(),
        ResponseState::Generated
    );
    assert_pending(&fixture);
    fixture.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
    assert_pending(&fixture);
    let outcome = fixture.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
    let record = fixture.owner.effective().response_record(RESPONSE).unwrap();
    assert_eq!(record.response().state(), ResponseState::Received);
    assert_eq!(record.received().unwrap().sequence, outcome.sequence);
    assert!(record.entered().is_none());
    assert_eq!(outcome.results, 1);
    assert_pending(&fixture);
    for id in [left.artifact.id, right.artifact.id] {
        assert_eq!(
            fixture.owner.effective().work(id).unwrap().state.state(),
            WorkArtifactState::Attached
        );
    }
}

#[test]
fn pending_receipt_projection_is_discardable_without_state_or_scratch_leak() {
    let mut fixture = Fixture::new();
    posted(&mut fixture);
    let original_claim = fixture.claim();
    let original_response = fixture.response(900);
    let budget = fixture.owner.budget_stats();
    let range = fixture.owner.range_stats();
    let committed_prefix = fixture.owner.committed().sequence();
    let NativeStaging::Prepared { candidate, outcome } = fixture
        .stage(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: original_claim,
                expected: original_response,
            },
        )
        .unwrap()
    else {
        panic!("fresh response receipt");
    };
    let pending_budget = fixture.owner.budget_stats();
    assert!(pending_budget.used > budget.used);
    assert_eq!(fixture.owner.pending_len(), 1);
    assert_eq!(fixture.owner.effective().sequence(), outcome.sequence);
    assert_eq!(
        fixture
            .owner
            .effective()
            .response(RESPONSE)
            .unwrap()
            .state(),
        ResponseState::Received
    );
    assert_eq!(
        fixture
            .owner
            .committed()
            .response(RESPONSE)
            .unwrap()
            .state(),
        ResponseState::Posted
    );
    assert_pending(&fixture);
    assert_eq!(fixture.owner.budget_stats(), pending_budget);
    assert_eq!(fixture.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(fixture.owner.pending_len(), 0);
    assert_eq!(fixture.owner.budget_stats(), budget);
    assert_eq!(fixture.owner.range_stats(), range);
    assert_eq!(fixture.owner.effective().sequence(), committed_prefix);
    assert_eq!(fixture.claim(), original_claim);
    assert_eq!(fixture.response(900), original_response);
    let restored = fixture.owner.effective().response_record(RESPONSE).unwrap();
    assert_eq!(restored.response().state(), ResponseState::Posted);
    assert_eq!((restored.received(), restored.entered()), (None, None));
    assert_pending(&fixture);
}

#[test]
fn unknown_claim_refuses_projection_before_callback_without_any_budget_change() {
    let mut fixture = Fixture::new();
    posted(&mut fixture);
    let budget = fixture.owner.budget_stats();
    let range = fixture.owner.range_stats();
    let mut called = false;
    let result = fixture
        .owner
        .with_effective_acceptance(ClaimId::from_u128(999), |_| {
            called = true;
        });
    assert!(matches!(
        result,
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::InvalidTarget
        )))
    ));
    assert!(!called);
    assert_eq!(fixture.owner.budget_stats(), budget);
    assert_eq!(fixture.owner.range_stats(), range);
    assert_eq!(fixture.owner.pending_len(), 0);
    assert_pending(&fixture);
}

#[test]
fn callback_error_releases_projection_reservation_and_preserves_owner_rows() {
    let mut fixture = Fixture::new();
    posted(&mut fixture);
    fixture.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
    let budget = fixture.owner.budget_stats();
    let range = fixture.owner.range_stats();
    let binding = fixture.claim();
    let response = fixture.response(900);
    let result = fixture
        .owner
        .with_effective_acceptance(CLAIM, |projection| {
            assert_eq!(
                projection.claim_decision().outcome(),
                aggregation::AggregateOutcome::Pending
            );
            assert!(projection.response_decision(RESPONSE).is_none());
            assert_eq!(
                fixture.owner.budget_stats().used,
                budget.used + projection.construction_charge()
            );
            Err::<(), _>("consumer declined the projected view")
        })
        .unwrap();
    assert_eq!(result, Err("consumer declined the projected view"));
    assert_eq!(fixture.owner.budget_stats(), budget);
    assert_eq!(fixture.owner.range_stats(), range);
    assert_eq!(fixture.claim(), binding);
    assert_eq!(fixture.response(900), response);
    assert_eq!(fixture.owner.pending_len(), 0);
    assert_pending(&fixture);
}
