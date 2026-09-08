use crate::native::claim_changes::{OriginalPlan, SealedChanges};
use crate::native::prepare::{Extras, Scratch};
use crate::native::report_tests as fixture;
use crate::native::*;
use focal_memory::BudgetLane;
use focal_model::lifecycle::claim::ClaimCut;
use focal_model::{ValidationMode, VerdictValue};

struct Staged {
    plan: transactions::Plan,
    extras: Extras,
    meta: Meta,
    outcome: NativeOutcome,
    scratch: Scratch,
}

fn stage(
    core: &Core<NativeState>,
    input: NativeInput,
    operation: NativeOperation,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
) -> Staged {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let intent = intent::fingerprint(view.ledger(), &input).unwrap();
    let sequence = SessionSeq(view.prefix().0 + 1);
    let mut meta = view.meta();
    meta.logical_time += 1;
    meta.outcomes += 1;
    let mut scratch = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut extras = Extras::new(
        core.limits.range.max_batch_entries,
        core.limits.preparation_bytes,
    )
    .unwrap();
    let mut plan = transactions::prepare(
        input.command,
        input.request,
        evidence,
        fixture::context(input.request.principal, meta.logical_time),
        ClaimCut {
            position: sequence,
            cause: intent,
        },
        &view,
        core.limits,
        &mut meta,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    plan.rows.sort_unstable_by_key(|row| row.binding().object);
    let count = |predicate: fn(&Key) -> bool| {
        u32::try_from(extras.rows.iter().filter(|row| predicate(&row.key)).count()).unwrap()
    };
    let events = extras.events()
        + if extras.journal.is_some() {
            0
        } else {
            claim_changes::event_count(&plan.rows, &view, operation).unwrap()
        };
    meta.events += events;
    let outcome = NativeOutcome {
        ledger: view.ledger(),
        invocation: input.request.into(),
        sequence,
        logical_time: meta.logical_time,
        operation,
        intent,
        created: u32::try_from(plan.created).unwrap(),
        changed: u32::try_from(plan.rows.len()).unwrap(),
        definitions: count(|key| matches!(key, Key::Definition(_))),
        evaluations: count(|key| matches!(key, Key::Evaluation(_))),
        artifacts: count(|key| matches!(key, Key::Artifact(_))),
        results: count(|key| {
            matches!(
                key,
                Key::Accepted(_) | Key::DeliveryResult(_) | Key::MissingResult(_)
            )
        }),
        receipts: count(|key| matches!(key, Key::Receipt(_))),
        responses: count(|key| matches!(key, Key::Response(_))),
        result_testaments: count(|key| matches!(key, Key::ResultTestament(_))),
        events: u32::try_from(events).unwrap(),
    };
    Staged {
        plan,
        extras,
        meta,
        outcome,
        scratch,
    }
}

fn original_history(core: &Core<NativeState>, staged: &Staged) -> Vec<NativeFact> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut workspace = Vec::with_capacity(staged.plan.rows.len());
    let mut facts = Vec::with_capacity(usize::try_from(staged.outcome.events).unwrap());
    let count = claim_changes::visit_history(
        &staged.plan.rows,
        &staged.extras,
        &view,
        staged.outcome.operation,
        &mut workspace,
        |fact| {
            assert!(facts.len() < facts.capacity());
            facts.push(fact);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, facts.len());
    assert_eq!(count, usize::try_from(staged.outcome.events).unwrap());
    facts
}

fn assemble(core: &Core<NativeState>, staged: Staged) -> SealedChanges {
    let Staged {
        plan,
        extras,
        meta,
        outcome,
        mut scratch,
    } = staged;
    let view = View {
        state: &core.state,
        tail: None,
    };
    OriginalPlan::check(
        plan,
        extras,
        meta,
        outcome,
        &view,
        core.limits,
        &mut scratch,
    )
    .unwrap()
    .with_seals(&mut scratch)
    .unwrap()
    .into_changes(core.limits.preparation_bytes, &mut scratch)
    .unwrap()
}

fn publish(core: &mut Core<NativeState>, changes: SealedChanges) -> NativeOutcome {
    let outcome = changes.outcome;
    let range = core
        .state
        .rows
        .prepare_batch_with(
            outcome.sequence.0,
            changes.changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.publish_native(NativePrepared {
        range,
        outcome,
        writes: mutation::WriteSet::unrecorded(),
    })
    .unwrap()
}

fn failure_fixture() -> Core<NativeState> {
    let mut core = fixture::core();
    // The real complete-policy guards for the cohort walk are deliberately
    // larger than this fixture's original small report-only traversal budget.
    core.limits.plan_edges = 65_536;
    fixture::publish(
        &mut core,
        10,
        fixture::creation(
            1,
            1,
            &[
                (ValidationMode::Required, false),
                (ValidationMode::Observe, false),
                (ValidationMode::Observe, false),
            ],
            None,
        ),
    );
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    for index in [1, 2] {
        let claim = core
            .native_claim(fixture::key(index).claim)
            .unwrap()
            .binding();
        let evaluation = core
            .native_evaluation(fixture::key(index))
            .unwrap()
            .binding();
        fixture::publish(
            &mut core,
            30,
            fixture::begin(10 + u128::from(index), claim, index, evaluation),
        );
    }
    core
}

fn failure_input(core: &Core<NativeState>) -> NativeInput {
    fixture::report_for(
        core,
        None,
        1901,
        1,
        VerdictValue::Fail,
        fixture::descriptor(fixture::artifact_spec(
            1901,
            fixture::EVALUATOR,
            VerdictValue::Fail,
        )),
    )
}

fn suppression(
    core: &Core<NativeState>,
    key: EvaluationKey,
    state: validation::EvaluationState,
) -> Option<validation::Suppression> {
    state
        .bind(core.native_definition(key.validation).unwrap())
        .unwrap()
        .suppression()
}

#[test]
fn required_admission_failure_preserves_accepted_result_and_seals_each_actual_sibling() {
    let mut core = failure_fixture();
    let claimant = fixture::key(1).claim;
    let begun = *core.native_evaluation(fixture::key(2)).unwrap();
    let ready = *core.native_evaluation(fixture::key(3)).unwrap();
    assert!(begun.has_begun());
    assert!(!ready.has_begun());
    let input = failure_input(&core);
    let mut custody = fixture::Custody::new();
    let evidence = fixture::verified(&mut custody, &input);
    let staged = stage(
        &core,
        input,
        NativeOperation::ReportAdmission,
        Some(&evidence),
    );
    let original = original_history(&core, &staged);
    assert_eq!(original.len(), 4);
    let NativeFact::Accepted { key: accepted_key } = original[2] else {
        panic!("the actual Accepted result is the third original event")
    };
    let changes = assemble(&core, staged);
    let changed = changes
        .seals
        .iter()
        .filter(|seal| seal.changed())
        .collect::<Vec<_>>();
    assert_eq!(changed.len(), 2);
    assert!(changed.iter().any(|seal| seal.previous() == begun));
    assert!(changed.iter().any(|seal| seal.previous() == ready));
    let outcome = publish(&mut core, changes);
    let facts = fixture::events(&core, outcome);
    assert_eq!(&facts[..original.len()], original);
    assert_eq!(facts.len(), original.len() + 3);
    let accepted = core.native_result(accepted_key).unwrap();
    assert_eq!(
        (accepted.sequence(), accepted.ordinal()),
        (outcome.sequence, 2)
    );
    let reported = core.native_evaluation(fixture::key(1)).unwrap();
    assert_eq!(reported.binding(), accepted.result().binding());
    assert!(reported.state().is_terminal());
    assert_eq!(reported.sealed(), None);
    assert_eq!(reported.last_result(), Some(accepted.result()));
    let begun_final = core.native_evaluation(fixture::key(2)).unwrap();
    assert_eq!(begun_final.binding(), begun.binding().next().unwrap());
    assert_eq!(begun_final.state(), begun.state());
    assert_eq!(
        suppression(&core, fixture::key(2), *begun_final),
        suppression(&core, fixture::key(2), begun)
    );
    assert!(begun_final.has_begun());
    assert!(begun_final.sealed().is_some());
    assert_eq!(begun_final.last_result(), begun.last_result());
    let ready_final = core.native_evaluation(fixture::key(3)).unwrap();
    assert_eq!(ready_final.binding(), ready.binding().next().unwrap());
    assert_eq!(ready_final.state(), ready.state());
    assert!(!ready_final.has_begun());
    assert!(matches!(
        suppression(&core, fixture::key(3), *ready_final),
        Some(validation::Suppression::CohortSealed(cause)) if Some(cause) == ready_final.sealed()
    ));
    assert_eq!(ready_final.sealed(), begun_final.sealed());
    assert_eq!(ready_final.last_result(), None);
    assert!(core.native_registrations(claimant).unwrap().is_sealed());
    assert_eq!(
        core.native_claim(claimant).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
    assert_eq!(outcome.artifacts, 1);
    assert_eq!(outcome.results, 1);

    // Begun observers retain independent report authority after the parent
    // fails; their later result does not create a second cohort or claim cut.
    let sealed_claim = core.native_claim(claimant).unwrap().binding();
    let late = fixture::report_for(
        &core,
        None,
        1904,
        2,
        VerdictValue::Pass,
        fixture::descriptor(fixture::artifact_spec(
            1904,
            fixture::EVALUATOR,
            VerdictValue::Pass,
        )),
    );
    let late_evidence = fixture::verified(&mut custody, &late);
    let staged = stage(
        &core,
        late,
        NativeOperation::ReportAdmission,
        Some(&late_evidence),
    );
    let late_original = original_history(&core, &staged);
    let changes = assemble(&core, staged);
    assert!(changes.seals.is_empty());
    let late_outcome = publish(&mut core, changes);
    assert_eq!(fixture::events(&core, late_outcome), late_original);
    assert_eq!(core.native_claim(claimant).unwrap().binding(), sealed_claim);
    assert_eq!(
        core.native_claim(claimant).unwrap().local_sealed_at(),
        Some(outcome.sequence)
    );
    let late_result = core
        .native_evaluation(fixture::key(2))
        .unwrap()
        .last_result()
        .unwrap();
    assert_eq!(late_result.verdict(), VerdictValue::Pass);
    assert_eq!(
        late_result.evidence().unwrap().id,
        ArtifactId::from_u128(1904)
    );
}

#[test]
fn missing_whole_work_entry_keeps_original_positions_and_seals_staged_suppression() {
    let mut core = work_authority::history_fixture(false, true);
    let claimant = ClaimId::from_u128(1);
    let claim = core.native_claim(claimant).unwrap().binding();
    let expected = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    let received = core
        .native_response_record(TestamentId::from_u128(900))
        .unwrap()
        .received();
    let old_registry_len = core.native_registrations(claimant).unwrap().rows().len();
    let staged = stage(
        &core,
        NativeInput {
            request: fixture::request(fixture::ISSUER, 1902),
            command: NativeCommand::EnterWholeWork { claim, expected },
        },
        NativeOperation::EnterWholeWork,
        None,
    );
    let original = original_history(&core, &staged);
    assert_eq!(staged.extras.journal.as_deref(), Some(original.as_slice()));
    let (ordinal, key) = original
        .iter()
        .enumerate()
        .find_map(|(ordinal, fact)| match fact {
            NativeFact::Missing { key } => Some((ordinal, *key)),
            _ => None,
        })
        .unwrap();
    assert!(
        !core
            .native_evaluation(key.evaluation)
            .unwrap()
            .state()
            .is_terminal()
    );
    let missing_observer = staged
        .extras
        .rows
        .iter()
        .find_map(|extra| match (&extra.key, &extra.row) {
            (Key::Evaluation(key), Row::Evaluation(row)) => {
                let state = row.get().unwrap();
                (suppression(&core, *key, *state) == Some(validation::Suppression::MissingTarget))
                    .then_some((*key, *state))
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        suppression(
            &core,
            missing_observer.0,
            *core.native_evaluation(missing_observer.0).unwrap()
        ),
        None
    );
    let changes = assemble(&core, staged);
    assert!(
        changes
            .seals
            .iter()
            .any(|seal| seal.previous() == missing_observer.1)
    );
    let outcome = publish(&mut core, changes);
    let facts = fixture::events(&core, outcome);
    assert_eq!(&facts[..original.len()], original);
    let result = core.native_missing_result(key).unwrap();
    assert_eq!(
        (result.sequence(), result.ordinal()),
        (outcome.sequence, ordinal as u32)
    );
    assert_eq!(result.result().phase(), validation::Phase::MissingTarget);
    assert_eq!(result.result().attempt(), None);
    assert_eq!(result.result().evidence(), None);
    assert_eq!(
        core.native_evaluation(key.evaluation)
            .unwrap()
            .last_result(),
        Some(result.result())
    );
    let registry = core.native_registrations(claimant).unwrap();
    assert!(registry.is_sealed());
    assert_eq!(registry.rows().len(), old_registry_len);
    let observer = core.native_evaluation(missing_observer.0).unwrap();
    assert_eq!(
        suppression(&core, missing_observer.0, *observer),
        Some(validation::Suppression::MissingTarget)
    );
    assert_eq!(
        observer.binding(),
        missing_observer.1.binding().next().unwrap()
    );
    assert!(observer.sealed().is_some());
    for registered in registry.rows() {
        let key = transactions::key_for_registered(claimant, *registered);
        let state = core.native_evaluation(key).unwrap();
        assert!(state.state().is_terminal() || state.sealed().is_some());
    }
    let response = core
        .native_response_record(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(response.received(), received);
    let entered = response.entered().unwrap();
    assert_eq!(entered.sequence, outcome.sequence);
    assert!(entered.ordinal < result.ordinal());
    assert!(matches!(
        facts[entered.ordinal as usize],
        NativeFact::Response {
            state: ResponseState::Validating,
            ..
        }
    ));
    assert_eq!(outcome.artifacts, 0);
    assert_eq!(outcome.results, 1);
}

#[test]
fn claimant_receipt_without_local_completion_keeps_delivery_and_adds_no_seal() {
    let mut core = work_authority::history_fixture(true, false);
    let claimant = ClaimId::from_u128(1);
    let claim = core.native_claim(claimant).unwrap().binding();
    let expected = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    let staged = stage(
        &core,
        NativeInput {
            request: fixture::request(fixture::ISSUER, 1903),
            command: NativeCommand::ReceiveResponse { claim, expected },
        },
        NativeOperation::ReceiveResponse,
        None,
    );
    let original = original_history(&core, &staged);
    let NativeFact::Delivery { key } = original[2] else {
        panic!("the actual Delivery result remains at ordinal 2")
    };
    let changes = assemble(&core, staged);
    assert!(changes.seals.is_empty());
    let outcome = publish(&mut core, changes);
    assert_eq!(fixture::events(&core, outcome), original);
    let result = core.native_delivery_result(key).unwrap();
    assert_eq!((result.sequence(), result.ordinal()), (outcome.sequence, 2));
    assert_eq!(result.result().attempt(), None);
    assert_eq!(result.result().evidence(), None);
    assert!(!core.native_registrations(claimant).unwrap().is_sealed());
    assert_eq!(core.native_claim(claimant).unwrap().local_sealed_at(), None);
}

#[test]
fn suffix_refusal_for_bytes_or_visits_leaves_the_actual_source_reusable() {
    let core = failure_fixture();
    let before_sequence = core.native_sequence();
    let before_claim = core.native_claim(fixture::key(1).claim).unwrap().binding();
    let before = [1, 2, 3].map(|index| *core.native_evaluation(fixture::key(index)).unwrap());
    let before_budget = core.state.budget.stats();
    let mut custody = fixture::Custody::new();
    for short_bytes in [true, false] {
        let input = failure_input(&core);
        let evidence = fixture::verified(&mut custody, &input);
        let Staged {
            plan,
            extras,
            meta,
            outcome,
            mut scratch,
        } = stage(
            &core,
            input,
            NativeOperation::ReportAdmission,
            Some(&evidence),
        );
        let view = View {
            state: &core.state,
            tail: None,
        };
        let mut limits = core.limits;
        if !short_bytes {
            // The original Admission failure now has a real checked journal.
            // Give that phase its quoted allowance, then prove the larger
            // complete-cohort suffix refuses without publishing any prefix.
            let sources: Vec<_> = plan
                .rows
                .iter()
                .map(|row| view.claim(ClaimId(row.binding().object.0)).unwrap())
                .collect();
            limits.plan_edges =
                admission_graph::checker_visits_bound(&sources, extras.rows.len(), extras.events())
                    .unwrap();
        }
        let original =
            OriginalPlan::check(plan, extras, meta, outcome, &view, limits, &mut scratch).unwrap();
        if short_bytes {
            scratch.max = scratch.used;
        }
        let refused = original.with_seals(&mut scratch);
        if short_bytes {
            assert!(matches!(
                refused,
                Err(NativeError::Capacity("preparation bytes"))
            ));
        } else {
            assert!(matches!(
                refused,
                Err(NativeError::Capacity("cohort seal visits"))
            ));
        }
        assert_eq!(core.native_sequence(), before_sequence);
        assert_eq!(
            core.native_claim(fixture::key(1).claim).unwrap().binding(),
            before_claim
        );
        assert_eq!(
            [1, 2, 3].map(|index| *core.native_evaluation(fixture::key(index)).unwrap()),
            before
        );
        assert!(
            !core
                .native_registrations(fixture::key(1).claim)
                .unwrap()
                .is_sealed()
        );
        assert_eq!(core.state.budget.stats(), before_budget);
    }
    let input = failure_input(&core);
    let evidence = fixture::verified(&mut custody, &input);
    let staged = stage(
        &core,
        input,
        NativeOperation::ReportAdmission,
        Some(&evidence),
    );
    assert!(!assemble(&core, staged).seals.is_empty());
    assert_eq!(core.state.budget.stats(), before_budget);
}
