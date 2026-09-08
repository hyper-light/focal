use super::*;
use crate::native::report_tests::{self as f, ISSUER};
use focal_model::{Deadline, MonitorId, TimerId};

fn core() -> Core<NativeState> {
    let mut core = f::core();
    core.limits.plan_nodes = 32;
    core.limits.plan_edges = 2 * 1024 * 1024;
    core.limits.preparation_bytes = 16 * 1024 * 1024;
    core
}
fn create(core: &mut Core<NativeState>, id: u128) {
    f::publish(core, 1, f::creation(id, id, &[], None));
}
fn register(
    core: &mut Core<NativeState>,
    owner: u128,
    monitor: u128,
    roots: Vec<WaitPredicate>,
) -> NativeOutcome {
    let source = core.native_claim(ClaimId::from_u128(owner)).unwrap();
    let input = NativeInput {
        request: f::request(ISSUER, 1000 + monitor),
        command: NativeCommand::RegisterMonitor {
            expected: source.binding(),
            receipt: source.receipt().map(|receipt| receipt.fence),
            id: MonitorId::from_u128(monitor),
            roots,
            deadline: Deadline {
                timer: TimerId::from_u128(2000 + monitor),
                generation: 1,
                at: 1000,
            },
        },
    };
    let time = View {
        state: &core.state,
        tail: None,
    }
    .meta()
    .logical_time
        + 1;
    f::publish(core, time, input)
}
fn cancel(core: &mut Core<NativeState>, id: u128) -> NativeOutcome {
    let expected = core.native_claim(ClaimId::from_u128(id)).unwrap().binding();
    let time = View {
        state: &core.state,
        tail: None,
    }
    .meta()
    .logical_time
        + 1;
    f::publish(
        core,
        time,
        NativeInput {
            request: f::request(ISSUER, 3000 + id),
            command: NativeCommand::Cancel { expected },
        },
    )
}
fn facts(core: &Core<NativeState>, outcome: NativeOutcome) -> Vec<NativeClaimEvent> {
    (0..outcome.events)
        .filter_map(|ordinal| {
            core.native_event(outcome.sequence, ordinal)
                .unwrap()
                .claim_event()
        })
        .collect()
}
fn released(core: &Core<NativeState>, owner: u128, monitor: u128) -> bool {
    core.native_claim(ClaimId::from_u128(owner))
        .unwrap()
        .scopes()
        .monitor(MonitorId::from_u128(monitor))
        .unwrap()
        .release_cut()
        .is_some()
}

#[test]
fn actual_registration_settles_in_same_prefix_after_its_original_registered_event() {
    let mut core = core();
    create(&mut core, 1);
    create(&mut core, 2);
    cancel(&mut core, 2);
    let outcome = register(
        &mut core,
        1,
        10,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let events = facts(&core, outcome);
    assert_eq!(events.len(), 2);
    assert!(
        matches!(events[0].kind, NativeEventKind::Monitor(NativeMonitorEvent::Registered { id, .. }) if id == MonitorId::from_u128(10))
    );
    assert!(
        matches!(events[1].kind, NativeEventKind::Monitor(NativeMonitorEvent::Released { id, .. }) if id == MonitorId::from_u128(10))
    );
    assert_eq!(events[1].before, Some(events[0].after));
    assert_eq!(events[1].after, events[0].after.next().unwrap());
    assert!(released(&core, 1, 10));
    let view = View {
        state: &core.state,
        tail: None,
    };
    assert_eq!(
        crate::native::monitor_index::subscribers(&view, ClaimId::from_u128(2), core.limits)
            .count(),
        0
    );
    assert_eq!(view.meta().monitors, 1);
    assert_eq!(view.meta().monitor_links, 1);
}

#[test]
fn terminal_owner_receives_real_monitor_release_without_repainting_original_terminal_cut() {
    let mut core = core();
    create(&mut core, 1);
    create(&mut core, 2);
    register(
        &mut core,
        1,
        10,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    cancel(&mut core, 1);
    let before = core.native_claim(ClaimId::from_u128(1)).unwrap();
    let binding = before.binding();
    let terminal = before.terminal_cut();
    let local_cut = before.local_sealed_at();
    assert!(!released(&core, 1, 10));
    let outcome = cancel(&mut core, 2);
    let owner = core.native_claim(ClaimId::from_u128(1)).unwrap();
    assert!(released(&core, 1, 10));
    assert_eq!(owner.binding(), binding.next().unwrap());
    assert_eq!(owner.terminal_cut(), terminal);
    assert_eq!(owner.local_sealed_at(), local_cut);
    assert_eq!(owner.status(), ClaimStatus::Cancelled);
    assert!(!owner.scopes().released());
    assert_eq!(
        facts(&core, outcome)
            .iter()
            .filter(|fact| matches!(
                fact.kind,
                NativeEventKind::Monitor(NativeMonitorEvent::Released { .. })
            ))
            .count(),
        1
    );
}

#[test]
fn multiple_same_owner_and_shared_chain_releases_have_exact_intermediate_revisions() {
    let mut core = core();
    for id in 1..=3 {
        create(&mut core, id);
    }
    register(
        &mut core,
        1,
        10,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    register(
        &mut core,
        1,
        11,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    register(
        &mut core,
        3,
        12,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let before = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let outcome = cancel(&mut core, 2);
    let events = facts(&core, outcome);
    let owner: Vec<_> = events
        .iter()
        .filter(|event| event.after.object == before.object)
        .collect();
    assert_eq!(owner.len(), 2);
    assert_eq!(owner[0].before, Some(before));
    assert_eq!(owner[1].before, Some(owner[0].after));
    assert_eq!(owner[1].after, before.next().unwrap().next().unwrap());
    for (id, monitor) in [(1, 10), (1, 11), (3, 12)] {
        assert!(released(&core, id, monitor));
    }
    let view = View {
        state: &core.state,
        tail: None,
    };
    assert_eq!(
        crate::native::monitor_index::subscribers(&view, ClaimId::from_u128(2), core.limits)
            .count(),
        0
    );
    assert_eq!(view.meta().monitors, 3);
    assert_eq!(view.meta().monitor_links, 3);
}

#[test]
fn held_original_union_survives_disposition_shrink_and_quote_refusal_is_read_only() {
    let mut core = core();
    create(&mut core, 1);
    create(&mut core, 2);
    register(
        &mut core,
        1,
        10,
        vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
    );
    let (budget, ids) = {
        let view = View {
            state: &core.state,
            tail: None,
        };
        let root = view.claim(ClaimId::from_u128(2)).unwrap();
        let plan = preflight(
            &view,
            root,
            core.limits,
            None,
            &mut Scratch {
                used: 0,
                max: core.limits.preparation_bytes,
            },
        )
        .unwrap();
        assert_eq!(plan.budget().charges().monitor_events, 1);
        assert_eq!(plan.budget().charges().monitor_index_rows, 4);
        assert!(plan.budget().charges().monitor_visits > 0);
        let before = core.state.budget.stats();
        let limits = NativeLimits {
            plan_edges: plan.budget().monitor.writer_visits - 1,
            ..core.limits
        };
        assert!(
            preflight(
                &view,
                root,
                limits,
                None,
                &mut Scratch {
                    used: 0,
                    max: limits.preparation_bytes
                }
            )
            .is_err()
        );
        assert_eq!(core.state.budget.stats(), before);
        (
            plan.budget(),
            plan.members()
                .iter()
                .map(|claim| ClaimId(claim.binding().object.0))
                .collect::<Vec<_>>(),
        )
    };
    cancel(&mut core, 2);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(2)).unwrap();
    assert!(
        preflight(
            &view,
            root,
            core.limits,
            Some(budget),
            &mut Scratch {
                used: 0,
                max: core.limits.preparation_bytes
            }
        )
        .is_err()
    );
    let plan = preflight_with_members(
        &view,
        root,
        core.limits,
        Some(budget),
        &ids,
        &mut Scratch {
            used: 0,
            max: core.limits.preparation_bytes,
        },
    )
    .unwrap();
    assert_eq!(plan.members().len(), 2);
    assert_eq!(plan.budget().charges().monitor_events, 0);
}
