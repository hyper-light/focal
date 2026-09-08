use super::*;
use crate::native::report_tests::{self as f, ISSUER, binding};

fn terminal_core() -> Core<NativeState> {
    let mut core = f::core();
    f::publish(&mut core, 1, f::creation(1, 1, &[], None));
    f::publish(
        &mut core,
        2,
        NativeInput {
            request: f::request(ISSUER, 2),
            command: NativeCommand::Cancel {
                expected: binding(1),
            },
        },
    );
    core
}

fn context(view: &View<'_>, max: usize) -> (ClaimCut, Scratch) {
    (
        ClaimCut {
            position: SessionSeq(view.prefix().0 + 1),
            cause: ContentHash([37; 32]),
        },
        Scratch { used: 0, max },
    )
}

fn transition(
    plan: &OwnerReleasePlan<'_, '_>,
    cut: ClaimCut,
    scratch: &mut Scratch,
) -> scope::Transition {
    let quote = scope::Registry::prepare_release_owner_bounded(
        plan.source,
        plan.snapshot(),
        plan.peers(),
        cut,
        scratch.remaining().unwrap(),
        plan.limits.plan_edges,
    )
    .unwrap();
    let charge = quote.construction_charge();
    scratch.charge(charge).unwrap();
    let transition = quote.build().unwrap();
    assert!(transition.construction_charge().unwrap() <= charge);
    transition
}

#[test]
fn real_owner_release_capability_preserves_terminal_truth_and_keeps_normal_guard_strict() {
    let core = terminal_core();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let source = view.claim(ClaimId::from_u128(1)).unwrap();
    let (cut, mut scratch) = context(&view, core.limits.preparation_bytes);
    let plan = owner_release(&view, source, cut, core.limits, &mut scratch).unwrap();
    assert!(plan.peers().is_empty());
    let transition = transition(&plan, cut, &mut scratch);
    let root = copy(source, &mut scratch).unwrap();
    let released = plan.apply(root, transition).unwrap();
    assert_eq!(released.claim().terminal_cut(), source.terminal_cut());
    assert_eq!(released.claim().local_sealed_at(), source.local_sealed_at());
    assert_eq!(released.claim().status(), source.status());
    assert_eq!(released.claim().binding(), source.binding().next().unwrap());
    let forged_bypass = released
        .claim()
        .try_copy(released.claim().retained_bytes().unwrap())
        .unwrap();
    let mut extras = Extras::new(0, 0).unwrap();
    extras.begin_journal(8, &mut scratch).unwrap();
    assert!(matches!(
        prepare(
            &view,
            forged_bypass,
            cut,
            core.limits,
            &mut extras,
            &mut scratch
        ),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    journal(
        &mut extras,
        source.binding(),
        released.claim(),
        NativeEventKind::OwnerReleased,
        None,
    )
    .unwrap();
    let rows = prepare_released(released, core.limits, &mut extras, &mut scratch).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].scopes().released());
    assert_eq!(rows[0].scopes().release_cut(), Some(cut));
    assert_eq!(extras.events(), 1);
    assert!(!source.scopes().released());
}

#[test]
fn pending_terminal_source_is_pinned_without_publishing_cancellation_or_release() {
    let mut core = f::core();
    f::publish(&mut core, 1, f::creation(1, 1, &[], None));
    let pending = f::prepared(core.prepare_native(
        f::context(ISSUER, 2),
        NativeInput {
            request: f::request(ISSUER, 2),
            command: NativeCommand::Cancel {
                expected: binding(1),
            },
        },
        &[],
    ));
    let view = View {
        state: &core.state,
        tail: Some(&pending),
    };
    let source = view.claim(ClaimId::from_u128(1)).unwrap();
    let (cut, mut scratch) = context(&view, core.limits.preparation_bytes);
    let plan = owner_release(&view, source, cut, core.limits, &mut scratch).unwrap();
    let token = transition(&plan, cut, &mut scratch);
    let released = plan
        .apply(copy(source, &mut scratch).unwrap(), token)
        .unwrap();
    let mut extras = Extras::new(0, 0).unwrap();
    extras.begin_journal(8, &mut scratch).unwrap();
    journal(
        &mut extras,
        source.binding(),
        released.claim(),
        NativeEventKind::OwnerReleased,
        None,
    )
    .unwrap();
    let rows = prepare_released(released, core.limits, &mut extras, &mut scratch).unwrap();
    assert_eq!(
        rows[0].scopes().release_cut().unwrap().position,
        SessionSeq(pending.outcome.sequence.0 + 1)
    );
    assert!(
        !pending
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .scopes()
            .released()
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(core.native_sequence(), SessionSeq(1));
}

#[test]
fn real_transition_with_different_cut_and_changed_copied_source_are_refused() {
    let core = terminal_core();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let source = view.claim(ClaimId::from_u128(1)).unwrap();
    for changed_source in [false, true] {
        let (cut, mut scratch) = context(&view, core.limits.preparation_bytes);
        let plan = owner_release(&view, source, cut, core.limits, &mut scratch).unwrap();
        let later = ClaimCut {
            position: SessionSeq(cut.position.0 + 1),
            ..cut
        };
        let token = transition(
            &plan,
            if changed_source { cut } else { later },
            &mut scratch,
        );
        let mut root = copy(source, &mut scratch).unwrap();
        if changed_source {
            let first = transition(&plan, cut, &mut scratch);
            root.apply_scope(&root.binding(), first, plan.peers())
                .unwrap();
        }
        assert!(plan.apply(root, token).is_err());
        assert!(!source.scopes().released());
    }
}

#[test]
fn live_owner_and_insufficient_source_workspace_refuse_without_source_changes() {
    let mut core = f::core();
    f::publish(&mut core, 1, f::creation(1, 1, &[], None));
    let view = View {
        state: &core.state,
        tail: None,
    };
    let source = view.claim(ClaimId::from_u128(1)).unwrap();
    let (cut, mut scratch) = context(&view, core.limits.preparation_bytes);
    assert!(owner_release(&view, source, cut, core.limits, &mut scratch).is_err());
    assert_eq!(scratch.used, 0);
    let core = terminal_core();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let source = view.claim(ClaimId::from_u128(1)).unwrap();
    let (cut, mut scratch) = context(&view, 0);
    let before = core.native_budget();
    assert!(owner_release(&view, source, cut, core.limits, &mut scratch).is_err());
    assert_eq!(core.native_budget(), before);
    assert!(!source.scopes().released());
}
