//! Isolated index tests use real checked model scope transitions and retained
//! native rows. Raw range publication deliberately avoids claiming command or
//! graph-consequence integration coverage (covered by the owner tests).
use super::*;
use crate::native::report_tests::{self as f, ISSUER};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::TimerId;
use focal_model::lifecycle::{
    claim::{ClaimCut, ClaimIntent},
    graph,
};

fn id(n: u128) -> ClaimId {
    ClaimId::from_u128(n)
}
fn mid(n: u128) -> MonitorId {
    MonitorId::from_u128(n)
}
fn view(core: &Core<NativeState>) -> View<'_> {
    View {
        state: &core.state,
        tail: None,
    }
}
fn scratch() -> Scratch {
    Scratch {
        used: 0,
        max: 16 * 1024 * 1024,
    }
}
fn cut(core: &Core<NativeState>) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(core.state.rows.prefix() + 1),
        cause: ContentHash([82; 32]),
    }
}
fn fixture() -> Core<NativeState> {
    let mut core = Core::new_native(
        f::binding(1).ledger,
        RangeId(9931),
        NativeLimits {
            plan_nodes: 16,
            plan_edges: 1_000_000,
            preparation_bytes: 16 * 1024 * 1024,
            range: RangeConfig {
                page_entries: 1,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    for n in 1..=3 {
        f::publish(
            &mut core,
            u64::try_from(n).unwrap(),
            f::creation(n, n, &[], None),
        );
    }
    core
}
fn graph<'a>(
    core: &'a Core<NativeState>,
    owner: &'a ClaimState,
) -> (graph::Snapshot, Vec<&'a ClaimState>) {
    let mut rows: Vec<_> = (1..=3).map(|n| core.native_claim(id(n)).unwrap()).collect();
    for row in &mut rows {
        if row.binding().object == owner.binding().object {
            *row = owner;
        }
    }
    let graph = graph::Snapshot::capture(
        &rows,
        graph::Limits {
            nodes: 16,
            edges: 64,
            visits: 1_000_000,
        },
    )
    .unwrap();
    rows.retain(|row| row.binding().object != owner.binding().object);
    (graph, rows)
}
fn register(
    core: &Core<NativeState>,
    owner_id: u128,
    monitor_id: u128,
    roots: &[WaitPredicate],
) -> (ClaimState, scope::Event) {
    let owner = core.native_claim(id(owner_id)).unwrap();
    let (graph, peers) = graph(core, owner);
    let plan = scope::Registry::prepare_register_bounded(
        owner,
        scope::Authority {
            principal: Principal::Actor(ISSUER),
            expected: owner.binding(),
            receipt: owner.receipt().map(|r| r.fence),
            cut: cut(core),
            now: 10,
        },
        scope::Registration {
            id: mid(monitor_id),
            roots,
            deadline: Deadline {
                timer: TimerId::from_u128(400 + monitor_id),
                generation: 1,
                at: 1000,
            },
        },
        &graph,
        &peers,
        scope::BuildLimits {
            bytes: 16 * 1024 * 1024,
            visits: 1_000_000,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let event = plan.event();
    let mut next = owner.try_copy(usize::MAX).unwrap();
    next.apply_scope(&owner.binding(), plan, &peers).unwrap();
    (next, event)
}
fn record(extras: &mut Extras, before: Binding, next: &ClaimState, event: scope::Event) {
    extras
        .record(NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Monitor(NativeMonitorEvent::from_scope(event).unwrap()),
            owned_child: None,
            before: Some(before),
            after: next.binding(),
            status: next.status(),
        }))
        .unwrap();
}
fn extras(
    core: &Core<NativeState>,
    next: &ClaimState,
    event: scope::Event,
) -> (Extras, MonitorChanges) {
    let mut scratch = scratch();
    let mut extras = Extras::new(128, super::super::prepare::array::<Extra>(256).unwrap()).unwrap();
    extras.begin_journal(16, &mut scratch).unwrap();
    let source = view(core);
    let before = source.claim(ClaimId(next.binding().object.0)).unwrap();
    let changes = stage(
        &source,
        before,
        next,
        event,
        core.limits,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    record(&mut extras, before.binding(), next, event);
    (extras, changes)
}
fn install(core: &mut Core<NativeState>, next: ClaimState, extras: Extras, added: MonitorChanges) {
    let source = view(core);
    let mut owned = source
        .owned_claim(ClaimId(next.binding().object.0))
        .unwrap()
        .copy()
        .unwrap();
    *owned.parts_mut().unwrap().0 = next;
    let key = Key::Claim(ClaimId(owned.claim().unwrap().binding().object.0));
    let mut changes = vec![Change::Put(Entry::new(
        key,
        Row::Claim(owned.copy().unwrap()),
        owned.heap_charge().unwrap(),
    ))];
    for extra in extras.rows {
        changes.push(Change::Put(Entry::new(extra.key, extra.row, extra.heap)));
    }
    let mut meta = source.meta();
    meta.monitors += added.monitors;
    meta.monitor_links += added.links;
    changes.push(Change::Put(Entry::new(Key::Meta, Row::Meta(meta), 0)));
    let candidate = core
        .state
        .rows
        .prepare_batch_with(
            core.state.rows.prefix() + 1,
            changes,
            BudgetLane::Completion,
            super::super::prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(candidate).unwrap();
}
fn members(core: &Core<NativeState>, target: u128) -> Vec<(ClaimId, MonitorId)> {
    subscribers(&view(core), id(target), core.limits)
        .map(|row| {
            let row = row.unwrap();
            (ClaimId(row.owner.binding().object.0), row.monitor.id())
        })
        .collect()
}
fn replace(core: &mut Core<NativeState>, key: Key, row: Row) {
    let candidate = core
        .state
        .rows
        .prepare_batch_with(
            core.state.rows.prefix() + 1,
            vec![Change::Put(Entry::new(key, row, 0))],
            BudgetLane::Completion,
            super::super::prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(candidate).unwrap();
}

#[test]
fn typed_roots_share_one_link_and_two_owners_preserve_original_provenance() {
    let mut core = fixture();
    for (owner, monitor) in [(1, 51), (2, 52)] {
        let (next, event) = register(
            &core,
            owner,
            monitor,
            &[
                WaitPredicate::Terminal(id(3)),
                WaitPredicate::Released(id(3)),
            ],
        );
        let (extras, added) = extras(&core, &next, event);
        assert_eq!(
            added,
            MonitorChanges {
                monitors: 1,
                links: 1
            }
        );
        assert_eq!(
            check_journal(
                &view(&core),
                std::slice::from_ref(&next),
                &extras,
                core.limits,
                &mut scratch()
            )
            .unwrap(),
            extras.rows.len()
        );
        install(&mut core, next, extras, added);
    }
    assert_eq!(members(&core, 3), vec![(id(2), mid(52)), (id(1), mid(51))]);
    let source = view(&core);
    let Some(Row::Monitor(allocation)) = source.get(Key::Monitor(mid(51))) else {
        panic!("allocation")
    };
    assert_eq!(allocation.owner, f::binding(1));
    assert_eq!(allocation.registered, SessionSeq(4));
    assert_eq!(source.meta().monitors, 2);
    assert_eq!(source.meta().monitor_links, 2);
    let Some(Row::MonitorLink(Some(link))) = source.get(Key::MonitorLink(id(3), mid(51))) else {
        panic!("link")
    };
    assert_eq!(link.previous, Some(mid(52)));
}

#[test]
fn global_monitor_identity_reuse_by_another_owner_refuses_without_publication() {
    let mut core = fixture();
    let (next, event) = register(&core, 1, 51, &[WaitPredicate::Terminal(id(3))]);
    let (extras, added) = extras(&core, &next, event);
    install(&mut core, next, extras, added);
    let (next, event) = register(&core, 2, 51, &[WaitPredicate::Terminal(id(3))]);
    let baseline = core.native_budget();
    let mut extras = Extras::new(16, super::super::prepare::array::<Extra>(32).unwrap()).unwrap();
    assert!(
        stage(
            &view(&core),
            core.native_claim(id(2)).unwrap(),
            &next,
            event,
            core.limits,
            &mut extras,
            &mut scratch()
        )
        .is_err()
    );
    assert!(extras.rows.is_empty());
    assert_eq!(core.native_budget(), baseline);
    assert_eq!(members(&core, 3), vec![(id(1), mid(51))]);
}

#[test]
fn full_chain_is_checked_before_first_yield_and_exact_visit_bound_includes_reread() {
    let mut core = fixture();
    let (next, event) = register(&core, 1, 51, &[WaitPredicate::Terminal(id(3))]);
    let (extras, added) = extras(&core, &next, event);
    install(&mut core, next, extras, added);
    let source = view(&core);
    let owner = source.claim(id(1)).unwrap();
    let scope = owner.scopes().monitor(mid(51)).unwrap();
    let exact = 2 + subscriber_visits(owner, scope).unwrap();
    assert_eq!(
        check_subscribers(&source, id(3), core.limits).unwrap(),
        exact
    );
    let baseline = core.native_budget();
    assert_eq!(
        subscribers(
            &source,
            id(3),
            NativeLimits {
                plan_edges: exact,
                ..core.limits
            }
        )
        .count(),
        1
    );
    let mut refused = subscribers(
        &source,
        id(3),
        NativeLimits {
            plan_edges: exact - 1,
            ..core.limits
        },
    );
    assert!(matches!(refused.next(), Some(Err(ContractError::Capacity))));
    assert!(refused.next().is_none());
    drop(refused);
    assert_eq!(core.native_budget(), baseline);
    let Some(Row::MonitorLink(Some(link))) = source.get(Key::MonitorLink(id(3), mid(51))) else {
        panic!("link")
    };
    let mut corrupt = *link;
    corrupt.next = Some(mid(51));
    replace(
        &mut core,
        Key::MonitorLink(id(3), mid(51)),
        Row::MonitorLink(Some(corrupt)),
    );
    let source = view(&core);
    let mut refused = subscribers(&source, id(3), core.limits);
    assert!(matches!(
        refused.next(),
        Some(Err(ContractError::InvalidManifest))
    ));
    assert!(refused.next().is_none());
}

#[test]
fn journal_replay_rejects_omitted_neighbors_unrelated_rows_and_wrong_provenance() {
    let mut core = fixture();
    let (next, event) = register(&core, 1, 51, &[WaitPredicate::Terminal(id(3))]);
    let (extra, added) = extras(&core, &next, event);
    install(&mut core, next, extra, added);
    for corruption in 0..4 {
        let (next, event) = register(&core, 2, 52, &[WaitPredicate::Terminal(id(3))]);
        let (mut extra, _) = extras(&core, &next, event);
        match corruption {
            0 => {
                extra
                    .rows
                    .retain(|row| row.key != Key::MonitorLink(id(3), mid(51)));
            }
            1 => {
                extra
                    .push(Extra {
                        key: Key::MonitorHead(id(1)),
                        row: Row::MonitorHead(MonitorHead::default()),
                        heap: 0,
                        fact: None,
                    })
                    .unwrap();
            }
            2 => {
                for row in &mut extra.rows {
                    if let Row::Monitor(allocation) = &mut row.row {
                        allocation.registered = SessionSeq(1);
                    }
                }
            }
            _ => {
                extra.journal.as_mut().unwrap().clear();
            }
        }
        let baseline = core.native_budget();
        assert!(
            check_journal(
                &view(&core),
                std::slice::from_ref(&next),
                &extra,
                core.limits,
                &mut scratch()
            )
            .is_err()
        );
        assert_eq!(core.native_budget(), baseline);
    }
    assert_eq!(members(&core, 3), vec![(id(1), mid(51))]);
}

#[test]
fn cancelled_monitor_unlinks_neighbors_and_retains_allocation_and_tombstone() {
    let mut core = fixture();
    for (owner, monitor) in [(1, 51), (2, 52)] {
        let (next, event) = register(&core, owner, monitor, &[WaitPredicate::Terminal(id(3))]);
        let (extra, added) = extras(&core, &next, event);
        install(&mut core, next, extra, added);
    }
    // A terminal owner can dispose its own wait without asserting settlement.
    let source = view(&core);
    let original = source.claim(id(2)).unwrap();
    let mut terminal = original.try_copy(usize::MAX).unwrap();
    terminal
        .apply(
            &original.binding(),
            Principal::Actor(ISSUER),
            ClaimIntent::Cancel { cut: cut(&core) },
        )
        .unwrap();
    let (graph, peers) = graph(&core, &terminal);
    let transition = scope::Registry::prepare_cancel_monitor_bounded(
        &terminal,
        scope::Authority {
            principal: Principal::Actor(ISSUER),
            expected: terminal.binding(),
            receipt: None,
            cut: cut(&core),
            now: 20,
        },
        mid(52),
        &graph,
        &peers,
        scope::BuildLimits {
            bytes: 16 * 1024 * 1024,
            visits: 1_000_000,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let event = transition.event();
    let mut next = terminal.try_copy(usize::MAX).unwrap();
    next.apply_scope(&terminal.binding(), transition, &peers)
        .unwrap();
    let quote = release_bound(
        &source,
        &terminal,
        terminal.scopes().monitor(mid(52)).unwrap(),
        core.limits,
    )
    .unwrap();
    assert_eq!(
        (quote.extra_rows, quote.temporary_bytes, quote.incoming_heap),
        (4, 0, 0)
    );
    let mut extra = Extras::new(128, super::super::prepare::array::<Extra>(256).unwrap()).unwrap();
    extra.begin_journal(8, &mut scratch()).unwrap();
    let added = stage(
        &source,
        &terminal,
        &next,
        event,
        NativeLimits {
            plan_edges: quote.visits,
            ..core.limits
        },
        &mut extra,
        &mut scratch(),
    )
    .unwrap();
    record(&mut extra, terminal.binding(), &next, event);
    assert_eq!(added, MonitorChanges::default());
    check_journal(
        &source,
        std::slice::from_ref(&next),
        &extra,
        core.limits,
        &mut scratch(),
    )
    .unwrap();
    install(&mut core, next, extra, added);
    assert_eq!(members(&core, 3), vec![(id(1), mid(51))]);
    assert!(matches!(
        view(&core).get(Key::MonitorLink(id(3), mid(52))),
        Some(Row::MonitorLink(None))
    ));
    assert!(matches!(
        view(&core).get(Key::Monitor(mid(52))),
        Some(Row::Monitor(_))
    ));
    assert_eq!(view(&core).meta().monitors, 2);
    assert_eq!(view(&core).meta().monitor_links, 2);
}
