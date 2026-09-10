use super::*;
use crate::native::prepare::{Extras, Scratch};
use crate::native::report_tests as fixture;
use focal_memory::BudgetLane;
use focal_model::lifecycle::{claim::ClaimCut, creation, succession::Lineage};
use focal_model::{Cause, ObjectRevision, ValidationMode, VerdictValue};

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
    logical_time: u64,
    evidence: Option<&focal_evidence::VerifiedNativeArtifact>,
) -> Staged {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let intent = intent::fingerprint(view.ledger(), &input).unwrap();
    let sequence = SessionSeq(view.prefix().0 + 1);
    let mut meta = view.meta();
    meta.logical_time = logical_time;
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
        fixture::context(input.request.principal, logical_time),
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
        logical_time,
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

fn history(core: &Core<NativeState>, staged: &Staged) -> Vec<NativeFact> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut workspace = Vec::<claim_changes::History>::with_capacity(staged.plan.rows.len());
    let mut facts = Vec::with_capacity(usize::try_from(staged.outcome.events).unwrap());
    let count = claim_changes::visit_history(
        &staged.plan.rows,
        &staged.extras,
        &view,
        staged.outcome.operation,
        &mut workspace,
        |fact| {
            if facts.len() == facts.capacity() {
                return Err(NativeError::Capacity("test history sink"));
            }
            facts.push(fact);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, facts.len());
    assert_eq!(count, usize::try_from(staged.outcome.events).unwrap());
    facts
}

fn publish(core: &mut Core<NativeState>, staged: Staged) -> (NativeOutcome, Vec<NativeFact>) {
    let facts = history(core, &staged);
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
    let changes = claim_changes::changes(
        plan,
        extras,
        meta,
        outcome,
        &view,
        core.limits,
        core.limits.preparation_bytes,
        &mut scratch,
    )
    .unwrap();
    let range = core
        .state
        .rows
        .prepare_batch_with(
            outcome.sequence.0,
            changes,
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.publish_native(NativePrepared {
        fragments: range,
        outcome,
        writes: mutation::WriteSet::unrecorded(),
    })
    .unwrap();
    assert_eq!(fixture::events(core, outcome), facts);
    (outcome, facts)
}

fn creation_input() -> NativeInput {
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for (id, owner) in [(9u128, None), (5, Some(9)), (2, Some(5)), (7, Some(9))] {
        let NativeCommand::Create {
            claims: mut one,
            declarations: mut definitions,
        } = fixture::creation(1, id, &[], None).command
        else {
            panic!("creation fixture")
        };
        if let Some(owner) = owner {
            one[0].definition.lineage = Lineage::new(
                fixture::binding(id),
                Cause::Claim(ClaimId::from_u128(owner)),
                &[],
                8,
            )
            .unwrap();
            one[0].owner = Some(creation::Owner {
                expected: fixture::binding(owner),
                receipt: None,
            });
        }
        claims.append(&mut one);
        declarations.append(&mut definitions);
    }
    NativeInput {
        request: fixture::request(fixture::ISSUER, 1),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}

#[test]
fn creation_visitor_preserves_reverse_id_children_and_each_intermediate_parent_revision() {
    let mut core = fixture::core();
    let staged = stage(&core, creation_input(), NativeOperation::Create, 10, None);
    let (outcome, facts) = publish(&mut core, staged);
    assert_eq!(outcome.events, 11);
    let created = facts[..4]
        .iter()
        .map(|fact| match fact {
            NativeFact::Claim(event) => {
                assert_eq!(event.kind, NativeEventKind::Created);
                assert_eq!(event.before, None);
                assert_eq!(event.after.revision, ObjectRevision(1));
                ClaimId(event.after.object.0)
            }
            _ => panic!("all claims are created before child registration"),
        })
        .collect::<Vec<_>>();
    assert_eq!(created, [2u128, 5, 7, 9].map(ClaimId::from_u128));
    let registered = facts[4..7]
        .iter()
        .map(|fact| match fact {
            NativeFact::Claim(event) => {
                assert_eq!(event.kind, NativeEventKind::ChildRegistered);
                *event
            }
            _ => panic!("child registration history"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        registered
            .iter()
            .map(|event| ClaimId(event.owned_child.unwrap().object.0))
            .collect::<Vec<_>>(),
        [2u128, 5, 7].map(ClaimId::from_u128)
    );
    assert_eq!(
        registered[1].owned_child.unwrap().revision,
        ObjectRevision(2)
    );
    assert_eq!(registered[1].before.unwrap().revision, ObjectRevision(1));
    assert_eq!(registered[2].before.unwrap().revision, ObjectRevision(2));
    assert_eq!(
        registered[2].after,
        core.native_claim(ClaimId::from_u128(9)).unwrap().binding()
    );
    assert!(
        facts[7..]
            .iter()
            .all(|fact| matches!(fact, NativeFact::Definition { .. }))
    );
}

#[test]
fn failing_admission_report_keeps_original_accepted_ordinal_before_claim_failure() {
    let mut core = fixture::running(&[(ValidationMode::Required, false)]);
    let original = core.native_claim(fixture::key(1).claim).unwrap();
    let historical = original
        .try_copy(original.retained_bytes().unwrap())
        .unwrap();
    let input = fixture::report_for(
        &core,
        None,
        901,
        1,
        VerdictValue::Fail,
        fixture::descriptor(fixture::artifact_spec(
            901,
            fixture::EVALUATOR,
            VerdictValue::Fail,
        )),
    );
    let mut custody = fixture::Custody::new();
    let evidence = fixture::verified(&mut custody, &input);
    let staged = stage(
        &core,
        input,
        NativeOperation::ReportAdmission,
        100,
        Some(&evidence),
    );
    let (outcome, facts) = publish(&mut core, staged);
    assert_eq!(outcome.events, 4);
    let result = core
        .native_evaluation(fixture::key(1))
        .unwrap()
        .last_result()
        .unwrap();
    let key = NativeResultKey::of(result);
    let accepted = core.native_result(key).unwrap();
    assert_eq!(
        (accepted.sequence(), accepted.ordinal()),
        (outcome.sequence, 2)
    );
    assert_eq!(accepted.result(), result);
    assert_eq!(accepted.attempt().index, result.attempt().unwrap());
    assert!(matches!(facts[0], NativeFact::Artifact { .. }));
    assert!(
        matches!(facts[1], NativeFact::Evaluation { kind: NativeEvaluationEventKind::Reported, after, attempt: Some(attempt), .. } if after == result.binding() && attempt == accepted.attempt())
    );
    assert_eq!(facts[2], NativeFact::Accepted { key });
    assert!(matches!(
        facts[3],
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::PostFailed,
            ..
        })
    ));
    // A genuine older Posted row cannot be replayed as a new transition from
    // the now-committed failure. The unpublished sink prefix is disposable.
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut workspace = Vec::<claim_changes::History>::with_capacity(1);
    let extras = Extras::new(0, 0).unwrap();
    assert!(
        claim_changes::visit_history(
            &[historical],
            &extras,
            &view,
            NativeOperation::Post,
            &mut workspace,
            |_| Ok(())
        )
        .is_err()
    );
    assert_eq!(
        core.native_evaluation(fixture::key(1))
            .unwrap()
            .last_result(),
        Some(result)
    );
    assert_eq!(fixture::events(&core, outcome), facts);
}

#[test]
fn insufficient_workspace_sink_failure_and_unordered_final_rows_leave_source_reusable() {
    let core = fixture::core();
    let mut staged = stage(&core, creation_input(), NativeOperation::Create, 10, None);
    let expected = history(&core, &staged);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let before_rows = staged
        .plan
        .rows
        .iter()
        .map(ClaimState::binding)
        .collect::<Vec<_>>();
    let before_extras = staged
        .extras
        .rows
        .iter()
        .map(|row| (row.key, row.fact))
        .collect::<Vec<_>>();
    let before_budget = core.state.budget.stats();
    let mut short = Vec::<claim_changes::History>::with_capacity(staged.plan.rows.len() - 1);
    let mut emitted = 0usize;
    assert!(
        claim_changes::visit_history(
            &staged.plan.rows,
            &staged.extras,
            &view,
            staged.outcome.operation,
            &mut short,
            |_| {
                emitted += 1;
                Ok(())
            }
        )
        .is_err()
    );
    assert_eq!(emitted, 0);
    let mut workspace = Vec::<claim_changes::History>::with_capacity(staged.plan.rows.len());
    assert!(matches!(
        claim_changes::visit_history(
            &staged.plan.rows,
            &staged.extras,
            &view,
            staged.outcome.operation,
            &mut workspace,
            |_| {
                emitted += 1;
                if emitted == 2 {
                    Err(NativeError::Capacity("test sink"))
                } else {
                    Ok(())
                }
            }
        ),
        Err(NativeError::Capacity("test sink"))
    ));
    assert_eq!(emitted, 2);
    staged.plan.rows.swap(0, 1);
    emitted = 0;
    assert!(
        claim_changes::visit_history(
            &staged.plan.rows,
            &staged.extras,
            &view,
            staged.outcome.operation,
            &mut workspace,
            |_| {
                emitted += 1;
                Ok(())
            }
        )
        .is_err()
    );
    assert_eq!(emitted, 0);
    staged.plan.rows.swap(0, 1);
    assert_eq!(
        staged
            .plan
            .rows
            .iter()
            .map(ClaimState::binding)
            .collect::<Vec<_>>(),
        before_rows
    );
    assert_eq!(
        staged
            .extras
            .rows
            .iter()
            .map(|row| (row.key, row.fact))
            .collect::<Vec<_>>(),
        before_extras
    );
    assert_eq!(core.state.budget.stats(), before_budget);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    assert_eq!(history(&core, &staged), expected);
}

#[test]
fn claimant_receipt_keeps_the_original_delivery_result_before_derived_claim_history() {
    let mut core = work_authority::history_fixture(true, false);
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let response = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    let input = NativeInput {
        request: fixture::request(fixture::ISSUER, 902),
        command: NativeCommand::ReceiveResponse {
            claim,
            expected: response,
        },
    };
    let logical_time = View {
        state: &core.state,
        tail: None,
    }
    .meta()
    .logical_time
        + 1;
    let staged = stage(
        &core,
        input,
        NativeOperation::ReceiveResponse,
        logical_time,
        None,
    );
    let (outcome, facts) = publish(&mut core, staged);
    assert!(matches!(
        facts[0],
        NativeFact::Response {
            state: ResponseState::Received,
            ..
        }
    ));
    assert!(matches!(
        facts[1],
        NativeFact::Evaluation {
            kind: NativeEvaluationEventKind::Materialized,
            before: None,
            state: validation::State::Ready,
            ..
        }
    ));
    let NativeFact::Delivery { key } = facts[2] else {
        panic!("the original Delivery publication remains ordinal 2")
    };
    let result = core.native_delivery_result(key).unwrap();
    assert_eq!((result.sequence(), result.ordinal()), (outcome.sequence, 2));
    assert_eq!(
        result.result().target(),
        core.native_evaluation(key.evaluation).unwrap().target()
    );
    assert_eq!(result.result().phase(), validation::Phase::Delivery);
    assert_eq!(result.result().attempt(), None);
    assert_eq!(result.result().evidence(), None);
    assert!(matches!(
        facts.last(),
        Some(NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::TestamentAcknowledged,
            ..
        }))
    ));
    assert_eq!(outcome.artifacts, 0);
}

#[test]
fn explicit_whole_work_journal_preserves_internal_missing_and_entry_positions_exactly() {
    let mut core = work_authority::history_fixture(false, true);
    let claim = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let expected = core
        .native_response(TestamentId::from_u128(900))
        .unwrap()
        .identity()
        .binding;
    let received = core
        .native_response_record(TestamentId::from_u128(900))
        .unwrap()
        .received();
    let input = NativeInput {
        request: fixture::request(fixture::ISSUER, 903),
        command: NativeCommand::EnterWholeWork { claim, expected },
    };
    let logical_time = View {
        state: &core.state,
        tail: None,
    }
    .meta()
    .logical_time
        + 1;
    let mut staged = stage(
        &core,
        input,
        NativeOperation::EnterWholeWork,
        logical_time,
        None,
    );
    let original = staged.extras.journal.as_ref().unwrap().clone();
    assert_eq!(history(&core, &staged), original);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut workspace = Vec::<claim_changes::History>::with_capacity(staged.plan.rows.len());
    let mut emitted = 0usize;
    assert!(
        claim_changes::visit_history(
            &staged.plan.rows,
            &staged.extras,
            &view,
            NativeOperation::ReportAdmission,
            &mut workspace,
            |_| {
                emitted += 1;
                Ok(())
            }
        )
        .is_err()
    );
    assert_eq!(emitted, 0);
    assert!(staged.extras.rows[0].fact.is_none());
    staged.extras.rows[0].fact = Some(NativeFact::Registrations { claim });
    assert!(
        claim_changes::visit_history(
            &staged.plan.rows,
            &staged.extras,
            &view,
            NativeOperation::EnterWholeWork,
            &mut workspace,
            |_| {
                emitted += 1;
                Ok(())
            }
        )
        .is_err()
    );
    assert_eq!(emitted, 0);
    staged.extras.rows[0].fact = None;
    assert_eq!(history(&core, &staged), original);
    let (outcome, facts) = publish(&mut core, staged);
    assert_eq!(facts, original);
    let (ordinal, result_key) = facts
        .iter()
        .enumerate()
        .find_map(|(ordinal, fact)| match fact {
            NativeFact::Missing { key } => Some((ordinal, *key)),
            _ => None,
        })
        .unwrap();
    let missing = core.native_missing_result(result_key).unwrap();
    assert_eq!(
        (missing.sequence(), missing.ordinal()),
        (outcome.sequence, u32::try_from(ordinal).unwrap())
    );
    assert_eq!(missing.result().phase(), validation::Phase::MissingTarget);
    assert_eq!(missing.result().evidence(), None);
    assert_eq!(missing.result().attempt(), None);
    let response = core
        .native_response_record(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(response.received(), received);
    let entered = response.entered().unwrap();
    assert_eq!(entered.sequence, outcome.sequence);
    assert!(entered.ordinal < missing.ordinal());
    assert!(matches!(
        facts[usize::try_from(entered.ordinal).unwrap()],
        NativeFact::Response {
            state: ResponseState::Validating,
            ..
        }
    ));
    assert_eq!(outcome.artifacts, 0);
    assert_eq!(outcome.results, 1);
}
