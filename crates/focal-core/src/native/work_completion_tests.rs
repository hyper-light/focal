use super::*;
use crate::native::report_tests::{self as reports, EVALUATOR, QUALITY};
use focal_memory::{Allocation, BudgetKind, BudgetLane};
use focal_model::lifecycle::{artifact_descriptor::ResultProvenance, graph};

#[path = "work_monitor_completion_tests.rs"]
mod monitor_tests;

fn setup(mode: ValidationMode) -> (Fixture, EvaluationKey) {
    let mut f = checked_slot_fixture_with_visits(mode, 65_536);
    complete_response(&mut f, 900, 801);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: ArtifactId::from_u128(801),
        },
        generation: 1,
    };
    let actor = if mode == ValidationMode::Observe {
        QUALITY
    } else {
        EVALUATOR
    };
    let expected = f.owner.effective().evaluation(key).unwrap().binding();
    f.commit(
        actor,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected,
        },
    );
    (f, key)
}
fn report(
    f: &Fixture,
    key: EvaluationKey,
    id: u128,
    value: VerdictValue,
) -> (ParticipantId, NativeCommand) {
    let view = f.owner.effective();
    let state = view.evaluation(key).unwrap();
    let attempt = state
        .bind(view.definition(key.validation).unwrap())
        .unwrap()
        .current_attempt()
        .unwrap();
    let mut spec = reports::artifact_spec(id, attempt.evaluator, value);
    spec.receipt = state.receipt();
    spec.visibility = &[];
    spec.result = Some(ResultProvenance {
        claim: key.claim,
        validation: key.validation,
        target: state.target(),
        generation: state.generation(),
        attempt,
        value,
    });
    let artifact = reports::descriptor(spec);
    let evidence = ArtifactRef {
        id: artifact.id(),
        hash: artifact.content_hash(),
    };
    (
        attempt.evaluator,
        NativeCommand::ReportWork {
            claim: f.claim(),
            key,
            expected: state.binding(),
            report: validation::Report {
                generation: state.generation(),
                attempt,
                value,
                evidence,
            },
            artifact: NativeArtifactInput::new(artifact).unwrap(),
        },
    )
}
fn exhaust(f: &Fixture) -> Allocation {
    let source = f.owner.budget_for_test();
    source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit()
}

#[test]
fn observe_begin_seals_its_own_live_attempt_and_rolls_back_or_reports_under_pressure() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Observe, 65_536);
    let slots = complete_response(&mut f, 900, 801);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: slots[0].artifact.id,
        },
        generation: 1,
    };
    let before_budget = f.owner.budget_stats();
    let before_range = f.owner.range_stats();
    let before_sequence = f.owner.effective().sequence();
    let before_claim = f.claim();
    let before_evaluation = *f.owner.effective().evaluation(key).unwrap();
    let before_receipt = f.owner.effective().receipt(ReceiptId::from_u128(701));
    let before_response = f.response(900);
    let received = f
        .owner
        .effective()
        .response_record(RESPONSE)
        .unwrap()
        .received();
    let before_work = slots.map(|slot| {
        let work = f.owner.effective().work(slot.artifact.id).unwrap();
        (work.state.binding(), work.state.state())
    });
    let before_members = f
        .owner
        .effective()
        .registrations(CLAIM)
        .unwrap()
        .rows()
        .to_vec();
    assert!(!before_evaluation.has_begun());
    assert_eq!(before_evaluation.sealed(), None);
    assert!(
        !f.owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .is_sealed()
    );

    let NativeStaging::Prepared { candidate, outcome } = f
        .stage(
            QUALITY,
            NativeCommand::BeginWork {
                claim: before_claim,
                key,
                expected: before_evaluation.binding(),
            },
        )
        .unwrap()
    else {
        panic!("the actual Observe Begin creates one unpublished candidate")
    };
    let events = (0..outcome.events)
        .map(|ordinal| {
            f.owner
                .effective()
                .event(outcome.sequence, ordinal)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let transitions = events
        .iter()
        .filter_map(|event| match event.fact {
            NativeFact::Evaluation {
                kind,
                key: member,
                before,
                after,
                attempt,
                ..
            } if member == key => Some((event.ordinal, kind, before, after, attempt)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [begun, sealed] = transitions.as_slice() else {
        panic!("Begin and cohort recording are distinct events for one final evaluation row")
    };
    assert_eq!(begun.0, 0);
    assert_eq!(begun.1, NativeEvaluationEventKind::Begun);
    assert_eq!(begun.2, Some(before_evaluation.binding()));
    assert_eq!(begun.3, before_evaluation.binding().next().unwrap());
    assert_eq!(sealed.1, NativeEvaluationEventKind::Sealed);
    assert!(sealed.0 > begun.0);
    assert_eq!(sealed.2, Some(begun.3));
    assert_eq!(sealed.3, begun.3.next().unwrap());
    assert_eq!(sealed.4, begun.4);
    let attempt = begun.4.unwrap();
    assert_eq!(attempt.evaluator, QUALITY);
    let final_evaluation = f.owner.effective().evaluation(key).unwrap();
    assert_eq!(final_evaluation.binding(), sealed.3);
    assert!(final_evaluation.has_begun());
    assert!(final_evaluation.sealed().is_some());
    assert!(!final_evaluation.state().is_terminal());
    assert_eq!(final_evaluation.last_result(), None);
    assert_eq!(
        final_evaluation
            .bind(f.owner.effective().definition(key.validation).unwrap())
            .unwrap()
            .current_attempt()
            .unwrap(),
        attempt
    );
    assert_eq!(
        (outcome.evaluations, outcome.artifacts, outcome.results),
        (1, 0, 0)
    );
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
    assert!(
        f.owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .is_sealed()
    );
    assert_eq!(
        f.owner.effective().response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    assert_eq!(
        f.owner
            .effective()
            .response_record(RESPONSE)
            .unwrap()
            .received(),
        received
    );
    assert_eq!(
        f.owner.effective().receipt(ReceiptId::from_u128(701)),
        before_receipt
    );
    for slot in slots {
        assert_eq!(
            f.owner
                .effective()
                .work(slot.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Validated
        );
    }
    assert_eq!(
        *f.owner.committed().evaluation(key).unwrap(),
        before_evaluation
    );
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().binding(),
        before_claim
    );

    // The composed candidate must unwind its seal rebinding before removing
    // the newly installed grant, including every independently staged object.
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(f.owner.budget_stats(), before_budget);
    assert_eq!(f.owner.range_stats(), before_range);
    assert_eq!(f.owner.pending_len(), 0);
    assert_eq!(f.owner.effective().sequence(), before_sequence);
    assert_eq!(f.claim(), before_claim);
    assert_eq!(
        *f.owner.effective().evaluation(key).unwrap(),
        before_evaluation
    );
    assert_eq!(
        f.owner.effective().receipt(ReceiptId::from_u128(701)),
        before_receipt
    );
    assert_eq!(f.response(900), before_response);
    let response = f.owner.effective().response_record(RESPONSE).unwrap();
    assert_eq!(response.response().state(), ResponseState::Received);
    assert_eq!(response.received(), received);
    assert_eq!(response.entered(), None);
    assert_eq!(
        f.owner.effective().registrations(CLAIM).unwrap().rows(),
        before_members
    );
    assert!(
        !f.owner
            .effective()
            .registrations(CLAIM)
            .unwrap()
            .is_sealed()
    );
    for (slot, before) in slots.into_iter().zip(before_work) {
        let work = f.owner.effective().work(slot.artifact.id).unwrap();
        assert_eq!((work.state.binding(), work.state.state()), before);
    }

    let begun_outcome = f.commit(
        QUALITY,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected: before_evaluation.binding(),
        },
    );
    let sealed_state = *f.owner.committed().evaluation(key).unwrap();
    let sealed_claim = f.claim();
    let sealed_cut = f.owner.committed().claim(CLAIM).unwrap().terminal_cut();
    assert!(sealed_state.sealed().is_some());
    let (actor, command) = report(&f, key, 1842, VerdictValue::Pass);
    let NativeCommand::ReportWork {
        expected,
        report: submitted,
        ..
    } = &command
    else {
        panic!("genuine late observer report")
    };
    assert_eq!(*expected, sealed_state.binding());
    assert_eq!(submitted.attempt, attempt);
    let pressure = exhaust(&f);
    // This can succeed with no ancestor capacity only if Begin installed and
    // rebound the held grant to the actual final sealed evaluation binding.
    let reported = f.commit(actor, command);
    assert!(reported.sequence > begun_outcome.sequence);
    assert_eq!(
        (reported.events, reported.changed, reported.responses),
        (3, 0, 0)
    );
    let completed = f.owner.committed().evaluation(key).unwrap();
    assert!(completed.state().is_terminal());
    assert_eq!(completed.sealed(), sealed_state.sealed());
    let accepted = completed.last_result().unwrap();
    assert_eq!(accepted.verdict(), VerdictValue::Pass);
    assert_eq!(accepted.evidence().unwrap().id, ArtifactId::from_u128(1842));
    assert_eq!(f.claim(), sealed_claim);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().terminal_cut(),
        sealed_cut
    );
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    drop(pressure);
}

#[test]
fn held_work_chain_reports_error_then_pass_with_all_ancestor_capacity_exhausted() {
    let (mut f, key) = setup(ValidationMode::Required);
    let pin = f.owner.pin(0, 10_000).unwrap();
    let pressure = exhaust(&f);
    let (actor, command) = report(&f, key, 1801, VerdictValue::Error);
    let retry = f.commit(actor, command);
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().status(),
        ClaimStatus::Validating
    );
    assert!(
        !f.owner
            .effective()
            .evaluation(key)
            .unwrap()
            .state()
            .is_terminal()
    );
    let (actor, command) = report(&f, key, 1802, VerdictValue::Pass);
    let done = f.commit(actor, command);
    assert!(done.sequence > retry.sequence);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        f.owner.committed().response(RESPONSE).unwrap().state(),
        ResponseState::Validated
    );
    assert_eq!(
        f.owner
            .committed()
            .work(ArtifactId::from_u128(801))
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Validated
    );
    assert_eq!(
        pin.with_response_record(RESPONSE, 0, |row| row.response().state())
            .unwrap(),
        Some(ResponseState::Validating)
    );
    drop(pressure);
}

#[test]
fn full_authored_response_and_registry_growth_stays_inside_the_original_work_grant() {
    let (mut f, key) = setup(ValidationMode::Required);
    for cycle in 2..=4 {
        complete_response(&mut f, 899 + cycle, 800 + cycle * 2);
    }
    assert_eq!(
        f.owner.effective().claim(CLAIM).unwrap().response_count(),
        4
    );
    let pressure = exhaust(&f);
    let (actor, command) = report(&f, key, 1810, VerdictValue::Pass);
    f.commit(actor, command);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(903))
            .unwrap()
            .state(),
        ResponseState::Received
    );
    drop(pressure);
}

#[test]
fn new_incoming_dependency_is_refused_without_changing_the_protected_target() {
    let (mut f, key) = setup(ValidationMode::Required);
    let original = f.claim();
    let mut incoming = creation(100, 2, &[], None);
    let NativeCommand::Create { claims, .. } = &mut incoming.command else {
        panic!("create")
    };
    claims[0].definition.graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: graph::Kind::DependsOn,
            target: CLAIM,
        }],
        8,
    )
    .unwrap();
    assert_refused_unchanged(&mut f, ISSUER, incoming.command);
    assert_eq!(f.claim(), original);
    assert!(f.owner.effective().claim(ClaimId::from_u128(2)).is_none());
    f.commit(ISSUER, creation(101, 3, &[], None).command);
    assert_eq!(f.claim(), original);
    assert!(f.owner.committed().claim(ClaimId::from_u128(3)).is_some());
    let (actor, command) = report(&f, key, 1820, VerdictValue::Pass);
    f.commit(actor, command);
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
}

#[test]
fn owned_child_growth_is_refused_only_while_the_original_graph_promise_is_held() {
    fn child(f: &Fixture) -> NativeCommand {
        let mut input = creation(105, 2, &[], None);
        let NativeCommand::Create { claims, .. } = &mut input.command else {
            panic!("create")
        };
        let claim = &mut claims[0];
        claim.definition.lineage = focal_model::lifecycle::succession::Lineage::new(
            claim.definition.binding,
            focal_model::Cause::Claim(CLAIM),
            &[],
            0,
        )
        .unwrap();
        claim.owner = Some(focal_model::lifecycle::creation::Owner {
            expected: f.claim(),
            receipt: f
                .owner
                .effective()
                .claim(CLAIM)
                .unwrap()
                .receipt()
                .map(|r| r.fence),
        });
        input.command
    }

    // The same authored ownership relation is valid without a held Work grant.
    let mut unheld = checked_slot_fixture_with_visits(ValidationMode::Required, 65_536);
    complete_response(&mut unheld, 900, 801);
    unheld.commit(ISSUER, enter(&unheld, 900));
    assert_eq!(
        unheld.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Validating
    );
    unheld.commit(ISSUER, child(&unheld));
    assert!(
        unheld
            .owner
            .committed()
            .claim(ClaimId::from_u128(2))
            .is_some()
    );

    let (mut held, key) = setup(ValidationMode::Required);
    let budget = held.owner.budget_stats();
    let range = held.owner.range_stats();
    let binding = held.claim();
    let sequence = held.owner.effective().sequence();
    assert!(matches!(
        held.stage(ISSUER, child(&held)),
        Err(NativeOwnerError::Native(NativeError::Contract(
            ContractError::InvalidPolicy
        )))
    ));
    assert_eq!(held.owner.budget_stats(), budget);
    assert_eq!(held.owner.range_stats(), range);
    assert_eq!(held.claim(), binding);
    assert_eq!(held.owner.effective().sequence(), sequence);
    assert_eq!(held.owner.pending_len(), 0);
    assert!(
        held.owner
            .effective()
            .claim(ClaimId::from_u128(2))
            .is_none()
    );
    let pressure = exhaust(&held);
    let (actor, command) = report(&held, key, 1821, VerdictValue::Pass);
    held.commit(actor, command);
    assert_eq!(
        held.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    drop(pressure);
}

#[test]
fn pending_work_report_rollback_restores_credit_protections_and_original_evidence() {
    let (mut f, key) = setup(ValidationMode::Required);
    let before = f.owner.budget_stats();
    let binding = f.owner.effective().evaluation(key).unwrap().binding();
    let (actor, command) = report(&f, key, 1830, VerdictValue::Error);
    let NativeStaging::Prepared { candidate, .. } = f.stage(actor, command).unwrap() else {
        panic!("prepared")
    };
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(1830))
            .is_some()
    );
    f.owner.discard_from(candidate).unwrap();
    assert_eq!(f.owner.budget_stats(), before);
    assert_eq!(
        f.owner.effective().evaluation(key).unwrap().binding(),
        binding
    );
    assert!(
        f.owner
            .effective()
            .artifact(ArtifactId::from_u128(1830))
            .is_none()
    );
    let pressure = exhaust(&f);
    let (actor, command) = report(&f, key, 1831, VerdictValue::Error);
    f.commit(actor, command);
    let (actor, command) = report(&f, key, 1832, VerdictValue::Pass);
    f.commit(actor, command);
    drop(pressure);
}

#[test]
fn agentic_observe_retry_remains_funded_after_independent_parent_completion() {
    let (mut f, key) = setup(ValidationMode::Observe);
    let cut = f.owner.committed().claim(CLAIM).unwrap().terminal_cut();
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    let pressure = exhaust(&f);
    for (id, value) in [(1840, VerdictValue::Error), (1841, VerdictValue::Pass)] {
        let (actor, command) = report(&f, key, id, value);
        assert_eq!(actor, QUALITY);
        let outcome = f.commit(actor, command);
        assert_eq!(
            (outcome.events, outcome.changed, outcome.responses),
            (3, 0, 0)
        );
        assert_eq!(
            f.owner.committed().claim(CLAIM).unwrap().terminal_cut(),
            cut
        );
    }
    assert!(
        f.owner
            .committed()
            .evaluation(key)
            .unwrap()
            .state()
            .is_terminal()
    );
    drop(pressure);
}
