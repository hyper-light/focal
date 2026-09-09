use super::*;
use crate::native::report_tests as f;
use focal_model::lifecycle::graph::{Kind, Obligation};

fn quote(
    core: &Core<NativeState>,
    limits: NativeLimits,
) -> Result<
    (
        CompletionEnvelope,
        Option<crate::native::completion_book::GraphMembers>,
    ),
    NativeError,
> {
    let view = View {
        state: &core.state,
        tail: None,
    };
    let registered = crate::native::admission_authority::registered_any(
        &view,
        core.native_claim(key(1).claim).unwrap().binding(),
        key(1),
    )?;
    CompletionEnvelope::derive_admission(
        &view,
        limits,
        &registered,
        descriptor_limits(limits, registered.parent, registered.registry)?,
        evidence(),
    )
}
fn peer(core: &mut Core<NativeState>, id: u128, target: u128) {
    let mut input = creation(20_000 + id, id, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("create")
    };
    claims[0].definition.graph = focal_model::lifecycle::graph::Declaration::new(
        &[Obligation {
            kind: Kind::DependsOn,
            target: ClaimId::from_u128(target),
        }],
        1,
    )
    .unwrap();
    publish(core, 40, input);
}

#[test]
fn isolated_required_parent_is_protected_and_failure_credit_is_still_one_time() {
    let core = running(&[(ValidationMode::Required, true)]);
    let baseline = core.state.budget.stats();
    let (envelope, members) = quote(&core, core.limits).unwrap();
    let members = members.unwrap();
    assert_eq!(members.ids(), &[key(1).claim]);
    assert!(envelope.has_graph());
    assert!(!envelope.is_work());
    let failure = envelope
        .report_storage(CompletionUse::AdmissionFailure)
        .unwrap();
    // Eleven primary rows, the cohort's rows, the report artifact's index
    // rows and verdict, a status move for the parent and every sealed
    // cohort claim, and the due timers the failure can retire: every
    // deleted key beyond the status moves (doc 22 §7).
    let cohort = envelope.cohort();
    let timers = failure.limits().deleted_keys - (1 + cohort.claims());
    assert!(timers > 1 + cohort.claims() + cohort.evaluations());
    assert_eq!(
        failure.limits().changed_keys,
        11 + cohort.changed_keys()
            + crate::native::index_rows::report_rows(16).unwrap()
            + crate::native::index_rows::STATUS_ROWS * (1 + cohort.claims())
            + timers
    );
    assert_eq!(
        envelope.slots().events,
        3 * envelope.reports() as usize + 1 + envelope.cohort().events()
    );
    assert_eq!(
        envelope.total_retained_bytes(),
        (envelope.reports() as usize - 1)
            * envelope
                .per_report_retained_bytes(CompletionUse::Regular)
                .unwrap()
            + envelope
                .per_report_retained_bytes(CompletionUse::AdmissionFailure)
                .unwrap()
    );
    let view = View {
        state: &core.state,
        tail: None,
    };
    envelope
        .check_graph_with_members(
            &view,
            core.limits,
            members.ids(),
            &mut prepare::Scratch {
                used: 0,
                max: envelope.graph_check_bytes(),
            },
        )
        .unwrap();
    drop(members);
    assert_eq!(core.state.budget.stats(), baseline);
}

#[test]
fn actual_dependency_failure_and_automatic_seals_fit_held_range_and_construction() {
    let mut core = running(&[
        (ValidationMode::Required, false),
        (ValidationMode::Observe, false),
    ]);
    peer(&mut core, 2, 1);
    let (envelope, members) = quote(&core, core.limits).unwrap();
    assert_eq!(
        members.as_ref().unwrap().ids(),
        &[key(1).claim, ClaimId::from_u128(2)]
    );
    let mut custody = f::Custody::new();
    let input = report_for(
        &core,
        None,
        29_001,
        1,
        VerdictValue::Fail,
        descriptor(artifact_spec(29_002, EVALUATOR, VerdictValue::Fail)),
    );
    let verified = f::verified(&mut custody, &input);
    let before = ReportParent::capture(core.native_claim(key(1).claim).unwrap());
    let prepare::Checked::Fresh(fresh) = core
        .check_native_chain(context(EVALUATOR, 100), input, std::iter::empty())
        .unwrap()
    else {
        panic!("fresh")
    };
    let candidate = fresh
        .build_with_completion(&core.state.budget, Some(&verified), Some(&envelope))
        .unwrap();
    assert_eq!(
        candidate.claim(key(1).claim).unwrap().status(),
        ClaimStatus::PostFailed
    );
    assert_eq!(
        candidate.claim(ClaimId::from_u128(2)).unwrap().status(),
        ClaimStatus::DependencyFailed
    );
    let Some(Row::Event(event)) = candidate
        .range
        .get(&Key::Event(candidate.outcome().sequence, 3))
    else {
        panic!("failure fact")
    };
    let NativeFact::Claim(event) = event.get().unwrap().expand(core.state.ledger).fact else {
        panic!("claim fact")
    };
    assert_eq!(
        before
            .completion_use_prepared(
                candidate.outcome().operation,
                candidate.claim(key(1).claim),
                Some(event)
            )
            .unwrap(),
        CompletionUse::AdmissionFailure
    );
    let mut wrong = event;
    wrong.after = wrong.after.next().unwrap();
    assert!(
        before
            .completion_use_prepared(
                candidate.outcome().operation,
                candidate.claim(key(1).claim),
                Some(wrong)
            )
            .is_err()
    );
    assert!(candidate.outcome().events as usize <= envelope.slots().events);
}

#[test]
fn new_incoming_member_and_insufficient_failure_shape_refuse_without_changing_budget() {
    let mut core = running(&[(ValidationMode::Required, false)]);
    let (envelope, members) = quote(&core, core.limits).unwrap();
    peer(&mut core, 2, 1);
    let baseline = core.state.budget.stats();
    let view = View {
        state: &core.state,
        tail: None,
    };
    assert!(
        envelope
            .check_graph_with_members(
                &view,
                core.limits,
                members.as_ref().unwrap().ids(),
                &mut prepare::Scratch {
                    used: 0,
                    max: envelope.graph_check_bytes()
                }
            )
            .is_err()
    );
    assert_eq!(core.state.budget.stats(), baseline);
    let (current, current_members) = quote(&core, core.limits).unwrap();
    let changes = current
        .report_storage(CompletionUse::AdmissionFailure)
        .unwrap()
        .limits()
        .changed_keys;
    drop(current_members);
    let baseline = core.state.budget.stats();
    // One row short of the failed shape narrows the promised result artifact
    // by one input (doc 22 §7) rather than refusing.
    let inputs = current.descriptor_limits().inputs;
    assert!(inputs > 0);
    let mut limited = core.limits;
    limited.range.max_batch_entries = changes - 1;
    let (narrowed, narrowed_members) = quote(&core, limited).unwrap();
    assert_eq!(narrowed.descriptor_limits().inputs, inputs - 1);
    assert_eq!(
        narrowed
            .report_storage(CompletionUse::AdmissionFailure)
            .unwrap()
            .limits()
            .changed_keys,
        changes - 1
    );
    drop(narrowed_members);
    // Below the shape of an input-free result artifact nothing can narrow.
    limited.range.max_batch_entries = changes - inputs - 1;
    assert!(quote(&core, limited).is_err());
    assert_eq!(core.state.budget.stats(), baseline);
}

#[test]
fn observer_has_no_parent_failure_graph_obligation() {
    let core = running(&[(ValidationMode::Observe, false)]);
    let baseline = core.state.budget.stats();
    let (envelope, members) = quote(&core, core.limits).unwrap();
    assert!(members.is_none());
    assert!(!envelope.has_graph());
    assert!(
        envelope
            .report_storage(CompletionUse::AdmissionFailure)
            .is_err()
    );
    assert_eq!(core.state.budget.stats(), baseline);
}
