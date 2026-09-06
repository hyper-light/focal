use super::*;
use crate::lifecycle::Principal;
use crate::lifecycle::evidence::{Parent, Response, ResponseIdentity, SlotBinding, WorkArtifact};
use crate::lifecycle::validation::tests::{AcceptedFixture, accepted_fixture};
use crate::{
    ClaimStatus, ContentHash, EvidenceAttestation, LedgerId, ObjectId, ObjectRevision,
    ParticipantId, ReceiptId, SessionId, TenantId,
};

fn binding(id: u128) -> Binding {
    Binding {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn parent() -> Parent {
    Parent {
        ledger: binding(1).ledger,
        claim: ClaimId::from_u128(1),
        issuer: ParticipantId::from_u128(2),
        holder: ParticipantId::from_u128(3),
        receipt: ReceiptFence {
            receipt: ReceiptId::from_u128(4),
            epoch: 1,
        },
        status: ClaimStatus::Received,
        local_complete: false,
        latest_response: None,
        next_cycle: 0,
    }
}
fn limits() -> Limits {
    Limits {
        max_slots: 8,
        max_checks: 16,
        max_results: 32,
        max_updates: 8,
    }
}
fn check(index: u32, mode: ValidationMode) -> CheckPolicy {
    CheckPolicy {
        declaration_index: index,
        validation: ValidationId::from_u128(1000 + u128::from(index)),
        mode,
    }
}
fn generated_response(id: u128, artifacts: &[(u32, u128)]) -> Response {
    let p = parent();
    let artifacts: Vec<_> = artifacts
        .iter()
        .map(|(slot, id)| {
            WorkArtifact::generate(
                binding(*id),
                &p,
                Principal::Actor(p.holder),
                *slot,
                p.receipt,
                &EvidenceAttestation {
                    descriptor_hash: binding(*id).content,
                    custody_revision: 1,
                    durable: true,
                    schema_valid: true,
                },
            )
            .unwrap()
        })
        .collect();
    let manifest: Vec<_> = artifacts
        .iter()
        .map(|a| SlotBinding {
            slot: a.slot(),
            artifact: a.reference(),
        })
        .collect();
    Response::close(
        ResponseIdentity {
            binding: binding(id),
            claim: p.claim,
            receipt: p.receipt,
            cycle: 0,
            prior: None,
        },
        &p,
        Principal::Actor(p.holder),
        &artifacts,
        &manifest,
        8,
    )
    .unwrap()
    .response
}
fn response(id: u128, artifacts: &[(u32, u128)]) -> Response {
    let p = parent();
    let mut response = generated_response(id, artifacts);
    response
        .apply(
            response
                .plan_post(&response.identity().binding, &p, Principal::Actor(p.holder))
                .unwrap(),
        )
        .unwrap();
    response
        .apply(
            response
                .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    response
        .apply(
            response
                .plan_begin_fixture(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    response
}
fn delivery(response: &Response) -> AcceptedResult {
    delivery_for(response, 900)
}
fn delivery_for(response: &Response, index: u32) -> AcceptedResult {
    delivery_for_definition(
        response,
        index,
        super::super::validation::DefinitionStamp::fixture(),
    )
}
fn delivery_for_definition(
    response: &Response,
    index: u32,
    definition: super::super::validation::DefinitionStamp,
) -> AcceptedResult {
    accepted_fixture(AcceptedFixture {
        definition,
        binding: binding(u128::from(index)),
        claim: parent().claim,
        receipt: Some(response.identity().receipt),
        target: Target::Delivery {
            response: response.identity().binding,
        },
        index,
        mode: ValidationMode::Required,
        verdict: VerdictValue::Pass,
        phase: Phase::Delivery,
        generation: 0,
        attempt: None,
        evidence: None,
        programmatic: None,
        terminal: true,
    })
}
fn result_spec(
    response: &Response,
    artifact: u128,
    slot: u32,
    check: CheckPolicy,
    verdict: VerdictValue,
) -> AcceptedFixture {
    let proof = ArtifactRef {
        id: ArtifactId::from_u128(10_000 + u128::from(check.declaration_index)),
        hash: ContentHash([8; 32]),
    };
    AcceptedFixture {
        definition: super::super::validation::DefinitionStamp::fixture(),
        binding: Binding {
            object: ObjectId(check.validation.0),
            ..binding(1)
        },
        claim: parent().claim,
        receipt: Some(response.identity().receipt),
        target: Target::Artifact {
            response: response.identity().binding,
            slot,
            artifact: binding(artifact),
        },
        index: check.declaration_index,
        mode: check.mode,
        verdict,
        phase: Phase::Programmatic,
        generation: 1,
        attempt: Some(0),
        evidence: Some(proof),
        programmatic: None,
        terminal: true,
    }
}
fn result(
    response: &Response,
    artifact: u128,
    slot: u32,
    check: CheckPolicy,
    verdict: VerdictValue,
) -> AcceptedResult {
    accepted_fixture(result_spec(response, artifact, slot, check, verdict))
}
fn fixture_policy(
    slots: &[SlotPolicy<'_>],
    limits: Limits,
) -> Result<AcceptancePolicy, ContractError> {
    AcceptancePolicy::fixture(
        binding(1),
        parent().issuer,
        slots,
        binding(900),
        900,
        limits,
    )
}
fn claim_index(
    slots: &[SlotPolicy<'_>],
    limits: Limits,
) -> Result<ClaimAggregation, ContractError> {
    let policy = fixture_policy(slots, limits)?;
    ClaimAggregation::from_policy(binding(1), ClaimStatus::Validating, &policy, limits)
}
fn index(response: &Response, slots: &[SlotPolicy<'_>]) -> ResponseAggregation {
    ResponseAggregation::from_policy(
        binding(1),
        &fixture_policy(slots, limits()).unwrap(),
        response.evaluation().unwrap(),
        &[delivery(response)],
        limits(),
    )
    .unwrap()
}

#[test]
fn complementary_artifact_checks_cannot_manufacture_a_slot_pass() {
    let checks = [
        check(1, ValidationMode::Required),
        check(2, ValidationMode::Required),
    ];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let a = response(100, &[(0, 10)]);
    let b = response(101, &[(0, 11)]);
    let first = index(&a, &policies)
        .apply(
            SessionSeq(1),
            &[
                result(&a, 10, 0, checks[0], VerdictValue::Pass),
                result(&a, 10, 0, checks[1], VerdictValue::Fail),
            ],
        )
        .unwrap();
    let second = index(&b, &policies)
        .apply(
            SessionSeq(1),
            &[
                result(&b, 11, 0, checks[0], VerdictValue::Fail),
                result(&b, 11, 0, checks[1], VerdictValue::Pass),
            ],
        )
        .unwrap();
    assert!(first.witnesses().is_empty());
    assert!(second.witnesses().is_empty());
    let mut claim = claim_index(&policies, limits()).unwrap();
    let AggregateOutcome::Blocked(cut) = claim.apply(SessionSeq(1), &[second, first]).unwrap()
    else {
        panic!("cross-artifact pass");
    };
    assert_eq!(
        cut.cause().key().target,
        CauseTarget::Response(TestamentId::from_u128(100))
    );
    assert_eq!(
        cut.cause().artifact().unwrap().id,
        ArtifactId::from_u128(10)
    );
}

#[test]
fn checks_accumulate_only_on_the_same_id_and_digest() {
    let checks = [
        check(1, ValidationMode::Required),
        check(2, ValidationMode::Required),
    ];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let response = response(100, &[(0, 10)]);
    let mut state = index(&response, &policies);
    let mut claim = claim_index(&policies, limits()).unwrap();
    let first = state
        .apply(
            SessionSeq(1),
            &[result(&response, 10, 0, checks[0], VerdictValue::Pass)],
        )
        .unwrap();
    assert_eq!(
        claim.apply(SessionSeq(1), &[first]).unwrap(),
        AggregateOutcome::Pending
    );
    let wrong_id = result(&response, 11, 0, checks[1], VerdictValue::Pass);
    assert_eq!(
        state.apply(SessionSeq(2), &[wrong_id]).unwrap_err(),
        ContractError::InvalidTarget
    );
    let mut wrong_hash = result_spec(&response, 10, 0, checks[1], VerdictValue::Pass);
    if let Target::Artifact { artifact, .. } = &mut wrong_hash.target {
        artifact.content = ContentHash([9; 32]);
    }
    assert_eq!(
        state
            .apply(SessionSeq(2), &[accepted_fixture(wrong_hash)])
            .unwrap_err(),
        ContractError::InvalidTarget
    );
    let mut pinned = result_spec(&response, 10, 0, checks[1], VerdictValue::Pass);
    if let Target::Artifact { response, .. } = &mut pinned.target {
        response.revision = ObjectRevision(2);
    }
    let second = state
        .apply(SessionSeq(2), &[accepted_fixture(pinned)])
        .unwrap();
    assert_eq!(second.witnesses()[0].checks().len(), 2);
    assert_eq!(
        claim.apply(SessionSeq(2), &[second]).unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(2)
        }
    );
    assert_eq!(
        claim.witnesses().next().unwrap().artifact().id,
        ArtifactId::from_u128(10)
    );
}

#[test]
fn delivery_requires_real_post_receipt_evaluation_and_pure_receipt_result() {
    let p = parent();
    let mut response = generated_response(100, &[]);
    assert_eq!(
        response.evaluation().unwrap_err(),
        ContractError::InvalidTransition
    );
    response
        .apply(
            response
                .plan_post(&response.identity().binding, &p, Principal::Actor(p.holder))
                .unwrap(),
        )
        .unwrap();
    assert!(response.evaluation().is_err());
    response
        .apply(
            response
                .plan_receive(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    assert!(response.evaluation().is_err());
    response
        .apply(
            response
                .plan_begin_fixture(&response.identity().binding, &p, Principal::Actor(p.issuer))
                .unwrap(),
        )
        .unwrap();
    let bogus = result(
        &response,
        10,
        0,
        check(1, ValidationMode::Required),
        VerdictValue::Pass,
    );
    assert!(matches!(
        ResponseAggregation::from_policy(
            binding(1),
            &fixture_policy(&[], limits()).unwrap(),
            response.evaluation().unwrap(),
            &[bogus],
            limits()
        ),
        Err(ContractError::InvalidPolicy)
    ));
    let update = index(&response, &[]).apply(SessionSeq(2), &[]).unwrap();
    let mut claim = claim_index(&[], limits()).unwrap();
    assert_eq!(
        claim.apply(SessionSeq(1), &[]).unwrap(),
        AggregateOutcome::Pending
    );
    assert_eq!(
        claim.apply(SessionSeq(2), &[update]).unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(2)
        }
    );
    assert_eq!(
        claim.delivery().unwrap().validation(),
        ValidationId::from_u128(900)
    );
}

#[test]
fn covered_failure_preserves_response_cut_and_unrelated_begun_work_can_finish() {
    let a_check = [check(1, ValidationMode::Required)];
    let b_check = [check(2, ValidationMode::Required)];
    let policies = [
        SlotPolicy {
            slot: 0,
            missing_declaration_index: 100,
            mode: ValidationMode::Required,
            checks: &a_check,
        },
        SlotPolicy {
            slot: 1,
            missing_declaration_index: 101,
            mode: ValidationMode::Required,
            checks: &b_check,
        },
    ];
    let alternative = response(100, &[(0, 10), (1, 11)]);
    let failing = response(101, &[(0, 12), (1, 13)]);
    let mut alt = index(&alternative, &policies);
    let mut failed = index(&failing, &policies);
    let mut claim = claim_index(&policies, limits()).unwrap();
    let first = alt
        .apply(
            SessionSeq(1),
            &[result(&alternative, 10, 0, a_check[0], VerdictValue::Pass)],
        )
        .unwrap();
    assert_eq!(
        claim.apply(SessionSeq(1), &[first]).unwrap(),
        AggregateOutcome::Pending
    );
    let second = failed
        .apply(
            SessionSeq(2),
            &[result(&failing, 12, 0, a_check[0], VerdictValue::Fail)],
        )
        .unwrap();
    let original = second.response_outcome();
    assert!(matches!(original, ResponseOutcome::Blocked(_)));
    assert_eq!(
        claim.apply(SessionSeq(2), &[second]).unwrap(),
        AggregateOutcome::Pending
    );
    let last = failed
        .apply(
            SessionSeq(3),
            &[result(&failing, 13, 1, b_check[0], VerdictValue::Pass)],
        )
        .unwrap();
    assert_eq!(last.response_outcome(), original);
    assert_eq!(last.outcome_for_slot(0).unwrap().sequence(), SessionSeq(2));
    assert_eq!(
        last.outcome_for_slot(1).unwrap().outcome(),
        ArtifactOutcome::Passed
    );
    assert_eq!(
        claim.apply(SessionSeq(3), &[last]).unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(3)
        }
    );
    assert_eq!(
        claim
            .witnesses()
            .map(SlotWitness::response)
            .collect::<Vec<_>>(),
        [TestamentId::from_u128(100), TestamentId::from_u128(101)]
    );
}

#[test]
fn later_alternative_never_heals_first_committed_failure() {
    let checks = [check(1, ValidationMode::Required)];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let failing = response(100, &[(0, 10)]);
    let alternative = response(101, &[(0, 11)]);
    let failure = index(&failing, &policies)
        .apply(
            SessionSeq(1),
            &[result(&failing, 10, 0, checks[0], VerdictValue::Error)],
        )
        .unwrap();
    let mut claim = claim_index(&policies, limits()).unwrap();
    let original = claim.apply(SessionSeq(1), &[failure]).unwrap();
    let later = index(&alternative, &policies)
        .apply(
            SessionSeq(2),
            &[result(&alternative, 11, 0, checks[0], VerdictValue::Pass)],
        )
        .unwrap();
    assert_eq!(claim.apply(SessionSeq(2), &[later]).unwrap(), original);
    assert!(claim.witnesses().next().is_none());
    let AggregateOutcome::Blocked(cut) = original else {
        panic!("failure lost");
    };
    assert_eq!(cut.sequence(), SessionSeq(1));
    assert_eq!(cut.cause().kind(), BlockingKind::Errored);
}

#[test]
fn simultaneous_coverage_wins_before_failure_in_either_update_order() {
    let checks = [check(1, ValidationMode::Required)];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    for reverse in [false, true] {
        let failed = response(100, &[(0, 10)]);
        let passed = response(101, &[(0, 11)]);
        let failure = index(&failed, &policies)
            .apply(
                SessionSeq(1),
                &[result(&failed, 10, 0, checks[0], VerdictValue::Fail)],
            )
            .unwrap();
        assert!(matches!(
            failure.response_outcome(),
            ResponseOutcome::Blocked(_)
        ));
        let success = index(&passed, &policies)
            .apply(
                SessionSeq(1),
                &[result(&passed, 11, 0, checks[0], VerdictValue::Pass)],
            )
            .unwrap();
        let mut updates = vec![failure, success];
        if reverse {
            updates.reverse();
        }
        let mut claim = claim_index(&policies, limits()).unwrap();
        assert_eq!(
            claim.apply(SessionSeq(1), &updates).unwrap(),
            AggregateOutcome::LocalComplete {
                sequence: SessionSeq(1)
            }
        );
        assert_eq!(
            updates
                .iter()
                .map(|update| update.causes().len())
                .sum::<usize>(),
            1
        );
    }
}

#[test]
fn optional_slot_failure_and_observe_check_have_distinct_artifact_consequences() {
    for (slot_mode, check_mode, blocked) in [
        (ValidationMode::Observe, ValidationMode::Required, true),
        (ValidationMode::Required, ValidationMode::Observe, false),
    ] {
        let checks = [check(1, check_mode)];
        let policies = [SlotPolicy {
            slot: 0,
            missing_declaration_index: 100,
            mode: slot_mode,
            checks: &checks,
        }];
        let response = response(100, &[(0, 10)]);
        let update = index(&response, &policies)
            .apply(
                SessionSeq(1),
                &[result(&response, 10, 0, checks[0], VerdictValue::Fail)],
            )
            .unwrap();
        assert_eq!(
            matches!(
                update.outcome_for_slot(0).unwrap().outcome(),
                ArtifactOutcome::Blocked(_)
            ),
            blocked
        );
        assert!(matches!(
            update.response_outcome(),
            ResponseOutcome::Validated { .. }
        ));
        let mut claim = claim_index(&policies, limits()).unwrap();
        assert_eq!(
            claim.apply(SessionSeq(1), &[update]).unwrap(),
            AggregateOutcome::LocalComplete {
                sequence: SessionSeq(1)
            }
        );
        assert_eq!(
            claim.witnesses().count(),
            usize::from(slot_mode == ValidationMode::Required)
        );
        assert!(claim.witnesses().all(|witness| witness.checks().is_empty()));
    }
}

#[test]
fn missing_zero_check_slot_is_incomplete_but_present_zero_check_artifact_passes() {
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &[],
    }];
    let absent = response(100, &[]);
    let update = index(&absent, &policies).apply(SessionSeq(1), &[]).unwrap();
    let ResponseOutcome::Blocked(cut) = update.response_outcome() else {
        panic!("missing slot stayed pending");
    };
    assert_eq!(cut.cause().kind(), BlockingKind::Incomplete);
    assert_eq!(cut.cause().artifact(), None);
    assert_eq!(cut.cause().key().declaration_index, 100);
    assert_eq!(cut.cause().key().generation, None);
    assert_eq!(cut.cause().key().attempt, None);
    let present = response(101, &[(0, 10)]);
    let update = index(&present, &policies)
        .apply(SessionSeq(1), &[])
        .unwrap();
    assert_eq!(
        update.witnesses()[0].artifact().id,
        ArtifactId::from_u128(10)
    );
    assert!(update.witnesses()[0].checks().is_empty());
}

#[test]
fn missing_target_order_uses_explicit_declarations_and_collisions_are_rejected() {
    let policies = [
        SlotPolicy {
            slot: 0,
            missing_declaration_index: 77,
            mode: ValidationMode::Required,
            checks: &[],
        },
        SlotPolicy {
            slot: 1,
            missing_declaration_index: 22,
            mode: ValidationMode::Required,
            checks: &[],
        },
    ];
    let response = response(100, &[]);
    let update = index(&response, &policies)
        .apply(SessionSeq(1), &[])
        .unwrap();
    assert_eq!(
        update
            .causes()
            .iter()
            .map(|cause| cause.key().declaration_index)
            .collect::<Vec<_>>(),
        [22, 77]
    );
    let mut claim = claim_index(&policies, limits()).unwrap();
    let AggregateOutcome::Blocked(cut) = claim.apply(SessionSeq(1), &[update]).unwrap() else {
        panic!("missing slot did not block");
    };
    assert_eq!(cut.cause().slot(), Some(1));
    assert_eq!(cut.cause().key().declaration_index, 22);

    let duplicate_missing = [
        policies[0],
        SlotPolicy {
            missing_declaration_index: 77,
            ..policies[1]
        },
    ];
    assert!(matches!(
        claim_index(&duplicate_missing, limits()),
        Err(ContractError::InvalidPolicy)
    ));
    let colliding_check = [check(77, ValidationMode::Required)];
    let check_collision = [
        policies[0],
        SlotPolicy {
            checks: &colliding_check,
            ..policies[1]
        },
    ];
    assert!(matches!(
        claim_index(&check_collision, limits()),
        Err(ContractError::InvalidPolicy)
    ));
}

#[test]
fn admission_and_increment_results_cannot_fill_whole_work_slots() {
    let checks = [check(1, ValidationMode::Required)];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let response = response(100, &[(0, 10)]);
    let mut state = index(&response, &policies);
    for target in [
        Target::Admission { claim: binding(1) },
        Target::Increment {
            claim: binding(1),
            artifact: binding(10),
        },
    ] {
        let mut fact = result_spec(&response, 10, 0, checks[0], VerdictValue::Pass);
        fact.target = target;
        assert_eq!(
            state
                .apply(SessionSeq(1), &[accepted_fixture(fact)])
                .unwrap_err(),
            ContractError::InvalidTarget
        );
        assert_eq!(state.outcome(), ResponseOutcome::Evaluating);
    }
}

#[test]
fn quality_intermediate_pass_is_pending_and_terminal_witness_keeps_both_proofs() {
    let checks = [check(1, ValidationMode::Required)];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let response = response(100, &[(0, 10)]);
    let mut state = index(&response, &policies);
    let mut intermediate = result_spec(&response, 10, 0, checks[0], VerdictValue::Pass);
    intermediate.terminal = false;
    let programmatic = intermediate.evidence;
    let first = state
        .apply(SessionSeq(1), &[accepted_fixture(intermediate)])
        .unwrap();
    assert!(first.witnesses().is_empty());
    assert_eq!(first.response_outcome(), ResponseOutcome::Evaluating);
    let mut final_result = result_spec(&response, 10, 0, checks[0], VerdictValue::Pass);
    final_result.phase = Phase::Quality;
    final_result.attempt = Some(1);
    final_result.programmatic = programmatic;
    final_result.evidence = Some(ArtifactRef {
        id: ArtifactId::from_u128(20_000),
        hash: ContentHash([9; 32]),
    });
    let final_evidence = final_result.evidence;
    let last = state
        .apply(SessionSeq(2), &[accepted_fixture(final_result)])
        .unwrap();
    let proof = last.witnesses()[0].checks()[0];
    assert_eq!(proof.programmatic_evidence, programmatic);
    assert_eq!(proof.evidence, final_evidence);
}

#[test]
fn canonical_causes_use_declaration_order_not_severity_or_result_arrival() {
    let checks = [
        check(1, ValidationMode::Required),
        check(2, ValidationMode::Required),
        check(3, ValidationMode::Required),
    ];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let response = response(100, &[(0, 10)]);
    let facts = [
        result(&response, 10, 0, checks[0], VerdictValue::Error),
        result(&response, 10, 0, checks[1], VerdictValue::Incomplete),
        result(&response, 10, 0, checks[2], VerdictValue::Fail),
    ];
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let update = index(&response, &policies)
            .apply(SessionSeq(1), &order.map(|i| facts[i]))
            .unwrap();
        assert_eq!(update.causes().len(), 3);
        let mut claim = claim_index(&policies, limits()).unwrap();
        let AggregateOutcome::Blocked(cut) = claim.apply(SessionSeq(1), &[update]).unwrap() else {
            panic!("failure lost");
        };
        assert_eq!(cut.cause().kind(), BlockingKind::Errored);
        assert_eq!(cut.cause().key().declaration_index, 1);
    }
    let base = CauseKey {
        target: CauseTarget::Response(TestamentId::from_u128(100)),
        declaration_index: 1,
        generation: Some(1),
        attempt: Some(0),
        phase: CausePhase::Programmatic,
    };
    assert!(
        base < CauseKey {
            phase: CausePhase::Quality,
            ..base
        }
    );
    assert!(
        CauseKey {
            phase: CausePhase::Quality,
            ..base
        } < CauseKey {
            attempt: Some(1),
            ..base
        }
    );
    assert!(
        CauseKey {
            attempt: Some(u32::MAX),
            ..base
        } < CauseKey {
            generation: Some(2),
            ..base
        }
    );
    assert!(
        CauseKey {
            generation: Some(u64::MAX),
            ..base
        } < CauseKey {
            declaration_index: 2,
            ..base
        }
    );
    assert!(
        CauseKey {
            declaration_index: u32::MAX,
            ..base
        } < CauseKey {
            target: CauseTarget::Response(TestamentId::from_u128(101)),
            ..base
        }
    );
}

#[test]
fn conflicting_complete_keys_reject_atomically_and_exact_duplicate_causes_coalesce() {
    let checks = [check(1, ValidationMode::Required)];
    let policies = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let response = response(100, &[(0, 10)]);
    let mut state = index(&response, &policies);
    let first = result(&response, 10, 0, checks[0], VerdictValue::Error);
    let second = result(&response, 10, 0, checks[0], VerdictValue::Fail);
    assert_eq!(
        state.apply(SessionSeq(1), &[first, second]).unwrap_err(),
        ContractError::ConflictingCause
    );
    assert_eq!(state.outcome(), ResponseOutcome::Evaluating);
    let accepted = state.apply(SessionSeq(1), &[first, first]).unwrap();
    assert_eq!(accepted.causes().len(), 1);
    let original = accepted.response_outcome();
    assert_eq!(
        state.apply(SessionSeq(1), &[first]).unwrap_err(),
        ContractError::InvalidCut
    );
    assert_eq!(state.outcome(), original);
    assert_eq!(
        state.apply(SessionSeq(2), &vec![first; 33]).unwrap_err(),
        ContractError::Capacity
    );
    assert_eq!(state.outcome(), original);
}

fn declaration_for(
    index: u32,
    target: super::super::validation::TargetDeclaration<'static>,
    mode: ValidationMode,
) -> super::super::validation::Declaration {
    use super::super::validation::{Declaration, DeclarationSpec, Program, TargetDeclaration};
    let delivery = target == TargetDeclaration::Delivery;
    Declaration::new(
        Principal::Actor(parent().issuer),
        DeclarationSpec {
            binding: binding(if delivery {
                u128::from(index)
            } else {
                1000 + u128::from(index)
            }),
            claim: parent().claim,
            issuer: parent().issuer,
            declaration_index: index,
            kind: if delivery {
                crate::ValidationKind::Receipt
            } else {
                crate::ValidationKind::Inspection
            },
            phase: match target {
                TargetDeclaration::Admission => crate::ValidationPhase::Admission,
                TargetDeclaration::Increment => crate::ValidationPhase::Increment,
                _ => crate::ValidationPhase::WholeWork,
            },
            mode,
            target,
            program: if delivery {
                Program::Delivery
            } else {
                super::super::validation::tests::programmatic(false)
            },
            deadline: crate::Deadline {
                timer: crate::TimerId::from_u128(1),
                generation: 1,
                at: 100,
            },
        },
        super::super::validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}
fn nonartifact_evaluation<'a>(
    declaration: &'a super::super::validation::Declaration,
    artifact: Option<u128>,
    generation: u64,
) -> super::super::validation::Evaluation<'a> {
    super::super::validation::Evaluation::materialize(
        Principal::Actor(parent().issuer),
        declaration,
        super::super::validation::Materialization {
            binding: declaration.binding(),
            target: artifact.map_or(Target::Admission { claim: binding(1) }, |artifact| {
                Target::Increment {
                    claim: binding(1),
                    artifact: binding(artifact),
                }
            }),
            slot_name: None,
            generation,
            receipt: artifact.map(|_| parent().receipt),
        },
    )
    .unwrap()
}
fn nonartifact_result(
    evaluation: &super::super::validation::Evaluation<'_>,
    verdict: VerdictValue,
    terminal: bool,
) -> AcceptedResult {
    accepted_fixture(AcceptedFixture {
        definition: evaluation.definition_stamp(),
        binding: evaluation.binding(),
        claim: evaluation.claim(),
        receipt: evaluation.receipt(),
        target: evaluation.target(),
        index: evaluation.declaration_index(),
        mode: evaluation.mode(),
        verdict,
        phase: Phase::Programmatic,
        generation: evaluation.generation(),
        attempt: Some(0),
        evidence: Some(ArtifactRef {
            id: ArtifactId::from_u128(10000 + u128::from(evaluation.declaration_index())),
            hash: ContentHash([8; 32]),
        }),
        programmatic: None,
        terminal,
    })
}
fn acceptance_index(
    declarations: &[super::super::validation::Declaration],
    status: ClaimStatus,
) -> ClaimAggregation {
    let policy =
        AcceptancePolicy::new(binding(1), parent().issuer, &[], declarations, limits()).unwrap();
    ClaimAggregation::from_policy(binding(1), status, &policy, limits()).unwrap()
}
fn delivered_for(aggregate: &ClaimAggregation, id: u128, sequence: u64) -> ResponseUpdate {
    let response = response(id, &[]);
    let definition = aggregate
        .policy
        .declaration(ValidationId::from_u128(900))
        .unwrap()
        .definition_stamp();
    ResponseAggregation::from_policy(
        binding(1),
        &aggregate.policy,
        response.evaluation().unwrap(),
        &[delivery_for_definition(&response, 900, definition)],
        limits(),
    )
    .unwrap()
    .apply(SessionSeq(sequence), &[])
    .unwrap()
}

#[test]
fn complete_policy_requires_delivery_and_exact_check_catalog() {
    use super::super::validation::TargetDeclaration as D;
    let checks = [check(1, ValidationMode::Required)];
    let slots = [SlotPolicy {
        slot: 0,
        missing_declaration_index: 100,
        mode: ValidationMode::Required,
        checks: &checks,
    }];
    let receipt = declaration_for(900, D::Delivery, ValidationMode::Required);
    assert_eq!(
        AcceptancePolicy::new(binding(1), parent().issuer, &slots, &[receipt], limits())
            .unwrap_err(),
        ContractError::InvalidPolicy
    );
    let check = declaration_for(
        1,
        D::WholeWorkSlot {
            index: 0,
            name: "output",
        },
        ValidationMode::Required,
    );
    assert_eq!(
        AcceptancePolicy::new(binding(1), parent().issuer, &slots, &[check], limits()).unwrap_err(),
        ContractError::InvalidPolicy
    );
    let all = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(
            1,
            D::WholeWorkSlot {
                index: 0,
                name: "output",
            },
            ValidationMode::Required,
        ),
        declaration_for(10, D::Admission, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Observe),
    ];
    let policy =
        AcceptancePolicy::new(binding(1), parent().issuer, &slots, &all, limits()).unwrap();
    assert_eq!(policy.declarations().len(), 4);
    assert_eq!(policy.declarations()[0].index(), 1);
    assert_eq!(
        AcceptancePolicy::new(
            binding(1),
            ParticipantId::from_u128(99),
            &slots,
            &all,
            limits()
        )
        .unwrap_err(),
        ContractError::InvalidPolicy
    );
}

#[test]
fn required_admission_cannot_be_bypassed_by_complete_response() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(10, D::Admission, ValidationMode::Required),
    ];
    let evaluation = nonartifact_evaluation(&declarations[1], None, 1);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Validating);
    aggregate.register(&evaluation).unwrap();
    let delivered = delivered_for(&aggregate, 100, 1);
    assert_eq!(
        aggregate.apply(SessionSeq(1), &[delivered]).unwrap(),
        AggregateOutcome::Pending
    );
    assert_eq!(aggregate.admission().outcome(), AdmissionOutcome::Pending);
    let pending = nonartifact_result(&evaluation, VerdictValue::Error, false);
    assert_eq!(
        aggregate
            .apply_acceptance(SessionSeq(2), &[], &[pending])
            .unwrap(),
        AggregateOutcome::Pending
    );
    let pass = nonartifact_result(&evaluation, VerdictValue::Pass, true);
    assert_eq!(
        aggregate
            .apply_acceptance(SessionSeq(3), &[], &[pass])
            .unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(3)
        }
    );
    assert_eq!(aggregate.admission().outcome(), AdmissionOutcome::Passed);
}

#[test]
fn admission_failure_is_a_typed_pre_receipt_cut_and_retains_first_commit() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(10, D::Admission, ValidationMode::Required),
        declaration_for(11, D::Admission, ValidationMode::Required),
    ];
    let first = nonartifact_evaluation(&declarations[1], None, 1);
    let second = nonartifact_evaluation(&declarations[2], None, 1);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Posted);
    aggregate.register(&first).unwrap();
    aggregate.register(&second).unwrap();
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(1),
                &[],
                &[nonartifact_result(&second, VerdictValue::Error, true)]
            )
            .unwrap(),
        AggregateOutcome::Pending
    );
    let original = aggregate.admission().outcome();
    let AdmissionOutcome::Blocked(cut) = original else {
        panic!("expected admission failure");
    };
    assert_eq!(cut.cause().key().target, CauseTarget::Admission);
    assert_eq!(cut.cause().key().declaration_index, 11);
    aggregate
        .apply_acceptance(
            SessionSeq(2),
            &[],
            &[nonartifact_result(&first, VerdictValue::Fail, true)],
        )
        .unwrap();
    assert_eq!(aggregate.admission().outcome(), original);
}

#[test]
fn required_increment_requires_nonempty_sealed_complete_exact_target_set() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let mut empty = acceptance_index(&declarations, ClaimStatus::Validating);
    empty.seal_targets().unwrap();
    let delivered = delivered_for(&empty, 100, 1);
    assert_eq!(
        empty.apply(SessionSeq(1), &[delivered]).unwrap(),
        AggregateOutcome::Pending
    );
    assert!(!empty.increments_ready());
    let first = nonartifact_evaluation(&declarations[1], Some(10), 1);
    let second = nonartifact_evaluation(&declarations[1], Some(11), 1);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Validating);
    aggregate.register(&first).unwrap();
    aggregate.register(&second).unwrap();
    let delivered = delivered_for(&aggregate, 100, 1);
    aggregate
        .apply_acceptance(
            SessionSeq(1),
            &[delivered],
            &[nonartifact_result(&first, VerdictValue::Pass, true)],
        )
        .unwrap();
    assert!(!aggregate.increments_ready());
    aggregate
        .apply_acceptance(
            SessionSeq(2),
            &[],
            &[nonartifact_result(&second, VerdictValue::Pass, true)],
        )
        .unwrap();
    assert_eq!(aggregate.outcome(), AggregateOutcome::Pending);
    aggregate.seal_targets().unwrap();
    assert!(aggregate.increments_ready());
    assert_eq!(
        aggregate.apply(SessionSeq(3), &[]).unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(3)
        }
    );
}

#[test]
fn increment_failure_rejects_increment_before_closure_then_blocks_required_acceptance() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let evaluation = nonartifact_evaluation(&declarations[1], Some(10), 1);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Received);
    aggregate.register(&evaluation).unwrap();
    aggregate.seal_targets().unwrap();
    let failure = nonartifact_result(&evaluation, VerdictValue::Fail, true);
    assert_eq!(
        aggregate
            .apply_acceptance(SessionSeq(1), &[], &[failure])
            .unwrap(),
        AggregateOutcome::Pending
    );
    assert!(aggregate.increments_ready());
    // The production rebind obtains this phase only from private ClaimState.
    aggregate.status = ClaimStatus::Validating;
    let delivered = delivered_for(&aggregate, 100, 2);
    let AggregateOutcome::Blocked(cut) = aggregate.apply(SessionSeq(2), &[delivered]).unwrap()
    else {
        panic!("required acceptance must fail");
    };
    assert_eq!(
        cut.cause().key().target,
        CauseTarget::Increment {
            artifact: ArtifactId::from_u128(10),
            content: binding(10).content
        }
    );
    assert_eq!(cut.sequence(), SessionSeq(2));
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(3),
                &[],
                &[nonartifact_result(&evaluation, VerdictValue::Pass, true)]
            )
            .unwrap_err(),
        ContractError::ConflictingCause
    );
    assert_eq!(aggregate.outcome(), AggregateOutcome::Blocked(cut));
}

#[test]
fn registry_rejects_undeclared_or_stale_targets_without_partial_results() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let registered = nonartifact_evaluation(&declarations[1], Some(10), 1);
    let unregistered = nonartifact_evaluation(&declarations[1], Some(11), 1);
    let stale = nonartifact_evaluation(&declarations[1], Some(10), 2);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Validating);
    aggregate.register(&registered).unwrap();
    assert_eq!(
        aggregate.register(&stale).unwrap_err(),
        ContractError::StaleEvaluation
    );
    let pass = nonartifact_result(&registered, VerdictValue::Pass, true);
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(1),
                &[],
                &[
                    pass,
                    nonartifact_result(&unregistered, VerdictValue::Pass, true)
                ]
            )
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert!(aggregate.decision().nonartifact_witnesses().is_empty());
    aggregate.seal_targets().unwrap();
    assert_eq!(
        aggregate.register(&unregistered).unwrap_err(),
        ContractError::InvalidTransition
    );
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(1),
                &[],
                &[nonartifact_result(&stale, VerdictValue::Pass, true)]
            )
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
}

#[test]
fn observe_nonartifact_failure_does_not_block_and_cross_scope_cause_order_is_stable() {
    use super::super::validation::TargetDeclaration as D;
    let optional = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Observe),
    ];
    let evaluation = nonartifact_evaluation(&optional[1], Some(10), 1);
    let mut aggregate = acceptance_index(&optional, ClaimStatus::Validating);
    aggregate.register(&evaluation).unwrap();
    let delivered = delivered_for(&aggregate, 100, 1);
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(1),
                &[delivered],
                &[nonartifact_result(&evaluation, VerdictValue::Error, true)]
            )
            .unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(1)
        }
    );
    assert_eq!(aggregate.decision().nonartifact_witnesses().len(), 1);
    let required = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(10, D::Admission, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let admission = nonartifact_evaluation(&required[1], None, 1);
    let increment = nonartifact_evaluation(&required[2], Some(10), 1);
    for reverse in [false, true] {
        let mut aggregate = acceptance_index(&required, ClaimStatus::Validating);
        aggregate.register(&admission).unwrap();
        aggregate.register(&increment).unwrap();
        aggregate.seal_targets().unwrap();
        let mut causes = [
            nonartifact_result(&admission, VerdictValue::Incomplete, true),
            nonartifact_result(&increment, VerdictValue::Error, true),
        ];
        if reverse {
            causes.reverse();
        }
        let AggregateOutcome::Blocked(cut) = aggregate
            .apply_acceptance(SessionSeq(1), &[], &causes)
            .unwrap()
        else {
            panic!("required causes block");
        };
        assert_eq!(cut.cause().key().target, CauseTarget::Admission);
        assert_eq!(cut.cause().kind(), BlockingKind::Incomplete);
    }
}

#[test]
fn sealing_increment_targets_preserves_later_delivery_registration_and_full_audit_seal() {
    use super::super::validation::{Materialization, TargetDeclaration as D};
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Received);
    let materialization = Materialization {
        binding: declarations[1].binding(),
        target: Target::Increment {
            claim: binding(1),
            artifact: binding(10),
        },
        slot_name: None,
        generation: 1,
        receipt: Some(parent().receipt),
    };
    let evaluation = aggregate
        .materialize(
            Principal::Actor(parent().issuer),
            &declarations[1],
            materialization,
        )
        .unwrap();
    aggregate.seal_increment_targets().unwrap();
    aggregate
        .apply_acceptance(
            SessionSeq(1),
            &[],
            &[nonartifact_result(&evaluation, VerdictValue::Pass, true)],
        )
        .unwrap();
    assert!(aggregate.increments_ready());
    assert!(aggregate.registry().sealed_targets().is_none());
    let receipt = aggregate
        .materialize(
            Principal::Actor(parent().issuer),
            &declarations[0],
            Materialization {
                binding: declarations[0].binding(),
                target: Target::Delivery {
                    response: binding(100),
                },
                slot_name: None,
                generation: 1,
                receipt: Some(parent().receipt),
            },
        )
        .unwrap();
    assert_eq!(
        receipt.target(),
        Target::Delivery {
            response: binding(100)
        }
    );
    let sealed = aggregate.seal_targets().unwrap();
    assert_eq!(sealed.rows().len(), 2);
    assert_eq!(sealed.policy().declarations().len(), 2);
    assert!(aggregate.registry().sealed_targets().is_some());
    assert_eq!(
        aggregate
            .materialize(
                Principal::Actor(parent().issuer),
                &declarations[0],
                Materialization {
                    binding: declarations[0].binding(),
                    target: Target::Delivery {
                        response: binding(101)
                    },
                    slot_name: None,
                    generation: 1,
                    receipt: Some(parent().receipt)
                }
            )
            .unwrap_err(),
        ContractError::InvalidTransition
    );
}

#[test]
fn receipt_and_lifecycle_revision_cannot_create_a_second_increment_obligation_or_result() {
    use super::super::validation::{Evaluation, Materialization, TargetDeclaration as D};
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(20, D::Increment, ValidationMode::Required),
    ];
    let first = nonartifact_evaluation(&declarations[1], Some(10), 1);
    let mut aggregate = acceptance_index(&declarations, ClaimStatus::Validating);
    aggregate.register(&first).unwrap();
    let foreign_receipt = ReceiptFence {
        epoch: 2,
        ..parent().receipt
    };
    let foreign = Evaluation::materialize(
        Principal::Actor(parent().issuer),
        &declarations[1],
        Materialization {
            binding: declarations[1].binding(),
            target: first.target(),
            slot_name: None,
            generation: 1,
            receipt: Some(foreign_receipt),
        },
    )
    .unwrap();
    assert_eq!(
        aggregate
            .apply_acceptance(
                SessionSeq(1),
                &[],
                &[nonartifact_result(&foreign, VerdictValue::Pass, true)]
            )
            .unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert_eq!(
        aggregate.register(&foreign).unwrap_err(),
        ContractError::StaleEvaluation
    );
    let revised = Evaluation::materialize(
        Principal::Actor(parent().issuer),
        &declarations[1],
        Materialization {
            binding: declarations[1].binding(),
            target: Target::Increment {
                claim: binding(1).next().unwrap(),
                artifact: binding(10).next().unwrap(),
            },
            slot_name: None,
            generation: 1,
            receipt: Some(parent().receipt),
        },
    )
    .unwrap();
    assert_eq!(
        aggregate.register(&revised).unwrap_err(),
        ContractError::StaleEvaluation
    );
    assert_eq!(aggregate.registry().rows().len(), 1);
    assert!(aggregate.decision().nonartifact_witnesses().is_empty());
}

#[test]
fn multiple_required_receipts_need_complete_unique_exact_response_proofs() {
    use super::super::validation::TargetDeclaration as D;
    let declarations = [
        declaration_for(900, D::Delivery, ValidationMode::Required),
        declaration_for(901, D::Delivery, ValidationMode::Required),
    ];
    let policy =
        AcceptancePolicy::new(binding(1), parent().issuer, &[], &declarations, limits()).unwrap();
    let response = response(100, &[]);
    let first = delivery_for_definition(&response, 900, declarations[0].definition_stamp());
    let second = delivery_for_definition(&response, 901, declarations[1].definition_stamp());
    assert!(matches!(
        ResponseAggregation::from_policy(
            binding(1),
            &policy,
            response.evaluation().unwrap(),
            &[first],
            limits()
        ),
        Err(ContractError::MissingEvidence)
    ));
    assert!(matches!(
        ResponseAggregation::from_policy(
            binding(1),
            &policy,
            response.evaluation().unwrap(),
            &[first, first],
            limits()
        ),
        Err(ContractError::InvalidManifest)
    ));
    let mut response_index = ResponseAggregation::from_policy(
        binding(1),
        &policy,
        response.evaluation().unwrap(),
        &[second, first],
        limits(),
    )
    .unwrap();
    let update = response_index.apply(SessionSeq(1), &[]).unwrap();
    assert_eq!(update.delivery_results(), &[first, second]);
    let mut aggregate =
        ClaimAggregation::from_policy(binding(1), ClaimStatus::Validating, &policy, limits())
            .unwrap();
    assert_eq!(
        aggregate.apply(SessionSeq(1), &[update]).unwrap(),
        AggregateOutcome::LocalComplete {
            sequence: SessionSeq(1)
        }
    );
    assert_eq!(aggregate.decision().delivery_results(), &[first, second]);
}
