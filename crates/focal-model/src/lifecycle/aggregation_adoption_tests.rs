use super::*;

fn generated(fixture: &Fixture, id: u128) -> WorkArtifact {
    let parent = evidence::Parent::from_claim(&fixture.claim).unwrap();
    WorkArtifact::generate(
        binding(id),
        &parent,
        Principal::Actor(parent.holder),
        0,
        parent.receipt,
        &EvidenceAttestation {
            descriptor_hash: binding(id).content,
            custody_revision: 1,
            durable: true,
            schema_valid: true,
        },
    )
    .unwrap()
}
fn increment(fixture: &Fixture, work: &WorkArtifact) -> v::EvaluationState {
    let definition = fixture
        .definitions
        .iter()
        .find(|row| row.target() == v::TargetDeclaration::Increment)
        .unwrap();
    v::Evaluation::materialize_increment(Principal::Actor(ISSUER), definition, &fixture.claim, work)
        .unwrap()
        .into_state()
}
fn adopt(fixture: &mut Fixture, sequence: u64) {
    let old = fixture.claim.receipt().unwrap();
    let token = fixture
        .claim
        .prepare_receipt_adoption(
            &fixture.claim.binding(),
            Principal::Actor(ISSUER),
            old.fence,
            claim::ReceiptEntitlement {
                holder: EVALUATOR,
                fence: ReceiptFence {
                    receipt: ReceiptId::from_u128(10),
                    epoch: old.fence.epoch + 1,
                },
            },
            claim::ClaimCut {
                position: SessionSeq(sequence),
                cause: ContentHash([91; 32]),
            },
        )
        .unwrap();
    let mut changed = fixture
        .claim
        .try_copy(fixture.claim.copy_charge().unwrap())
        .unwrap();
    changed.apply_receipt_adoption(&token).unwrap();
    fixture.registry.adopt_receipt(&token).unwrap();
    for state in &mut fixture.evaluations {
        let definition = fixture
            .definitions
            .iter()
            .find(|row| row.binding().object == state.binding().object)
            .unwrap();
        *state = state.adopt_receipt(definition, &token).unwrap();
    }
    fixture.claim = changed;
    fixture.prefix = SessionSeq(sequence);
}

#[test]
fn old_entered_response_keeps_its_history_when_replacement_increment_targets_are_open() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], true);
    let work = generated(&fixture, 400);
    let ready = increment(&fixture, &work);
    let definition = fixture
        .definitions
        .iter()
        .find(|row| row.binding().object == ready.binding().object)
        .unwrap();
    let ready = ready.bind(definition).unwrap();
    fixture
        .registry
        .register(&fixture.claim, &ready, 65536)
        .unwrap();
    let owner = ready.increment_owner(&fixture.claim, &work, 0).unwrap();
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    let complete = v::tests::report_value(&begun, VerdictValue::Pass);
    fixture
        .results
        .push((complete.result.unwrap(), position(2, 0)));
    fixture.evaluations.push(complete.next.into_state());
    let response = fixture.receive(300, &[(0, 400)], 3);
    fixture
        .registry
        .seal_increment_targets(&fixture.claim)
        .unwrap();
    fixture.enter(response, 4);
    assert!(fixture.projection().claim_decision().increments_ready());
    let response_before = fixture.responses[response].response.identity();
    let result_before = fixture.results[0].0;
    adopt(&mut fixture, 5);
    assert!(!fixture.registry.increment_targets_sealed());
    let projected = fixture.projection();
    assert!(!projected.claim_decision().increments_ready());
    assert_eq!(
        projected.claim_decision().outcome(),
        AggregateOutcome::Pending
    );
    assert_eq!(
        fixture.responses[response].response.identity(),
        response_before
    );
    assert_eq!(fixture.results[0].0, result_before);
    drop(projected);
    fixture
        .registry
        .seal_increment_targets(&fixture.claim)
        .unwrap();
    assert!(fixture.projection().claim_decision().increments_ready());
}

#[test]
fn retired_open_work_is_audited_while_same_cycle_replacement_work_gates_current_readiness() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], true);
    let old_work = generated(&fixture, 400);
    let old_ready = increment(&fixture, &old_work);
    let definition = fixture
        .definitions
        .iter()
        .find(|row| row.binding().object == old_ready.binding().object)
        .unwrap();
    fixture
        .registry
        .register(&fixture.claim, &old_ready.bind(definition).unwrap(), 65536)
        .unwrap();
    fixture.evaluations.push(old_ready);
    fixture.works.push(old_work);
    fixture
        .registry
        .seal_increment_targets(&fixture.claim)
        .unwrap();
    adopt(&mut fixture, 2);
    assert_eq!(
        fixture.evaluations[0].fence().unwrap().reason,
        v::FenceReason::ReceiptAdoption
    );
    let current_work = generated(&fixture, 401);
    assert_eq!(current_work.cycle(), old_work.cycle());
    assert_ne!(current_work.receipt(), old_work.receipt());
    let current_ready = increment(&fixture, &current_work);
    let definition = fixture
        .definitions
        .iter()
        .find(|row| row.binding().object == current_ready.binding().object)
        .unwrap();
    let ready = current_ready.bind(definition).unwrap();
    fixture
        .registry
        .register(&fixture.claim, &ready, 65536)
        .unwrap();
    let owner = ready
        .increment_owner(&fixture.claim, &current_work, 0)
        .unwrap();
    let begun = ready
        .begin(Principal::Actor(EVALUATOR), &ready.binding(), &owner)
        .unwrap()
        .next;
    let complete = v::tests::report_value(&begun, VerdictValue::Pass);
    let final_state = complete.next.into_state();
    let result = complete.result.unwrap();
    fixture.evaluations.push(begun.into_state());
    fixture.receive(300, &[(0, 401)], 3);
    fixture
        .registry
        .seal_increment_targets(&fixture.claim)
        .unwrap();
    assert!(!fixture.projection().claim_decision().increments_ready());
    fixture.evaluations[1] = final_state;
    fixture.results.push((result, position(4, 0)));
    fixture.prefix = SessionSeq(4);
    assert!(fixture.projection().claim_decision().increments_ready());
    assert_eq!(fixture.evaluations[0].last_result(), None);
    assert_eq!(fixture.works[0], old_work);
    // Omission of a retired row still fails closed, even though it is excluded
    // from the replacement receipt's business readiness.
    fixture.works.remove(0);
    assert!(prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).is_err());
}
