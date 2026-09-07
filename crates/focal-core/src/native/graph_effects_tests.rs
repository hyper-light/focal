use super::*;
use crate::native::report_tests::{self as f, ISSUER, binding};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::lifecycle::graph::{Kind, Obligation};

fn core() -> Core<NativeState> {
    Core::new_native(
        binding(1).ledger,
        RangeId(6901),
        NativeLimits {
            range: RangeConfig {
                page_entries: 1,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            plan_nodes: 32,
            plan_edges: 4096,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 32,
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}
fn create(core: &mut Core<NativeState>, id: u128, obligations: &[(Kind, u128)]) {
    let mut input = f::creation(id, id, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("create")
    };
    let obligations: Vec<_> = obligations
        .iter()
        .map(|&(kind, target)| Obligation {
            kind,
            target: ClaimId::from_u128(target),
        })
        .collect();
    claims[0].definition.graph = graph::Declaration::new(&obligations, 16).unwrap();
    f::publish(core, id as u64, input);
}
fn cancel(core: &Core<NativeState>, id: u128) -> NativePrepared {
    let claim = core.native_claim(ClaimId::from_u128(id)).unwrap();
    f::prepared(core.prepare_native(
        f::context(ISSUER, 100),
        NativeInput {
            request: f::request(ISSUER, 100 + id),
            command: NativeCommand::Cancel {
                expected: claim.binding(),
            },
        },
        &[],
    ))
}
fn changed_root(prepared: &NativePrepared, id: u128) -> ClaimState {
    let source = prepared.claim(ClaimId::from_u128(id)).unwrap();
    source.try_copy(source.copy_charge().unwrap()).unwrap()
}
fn cut(view: &View<'_>) -> ClaimCut {
    ClaimCut {
        position: SessionSeq(view.prefix().0 + 1),
        cause: ContentHash([98; 32]),
    }
}
fn extras(scratch: &mut Scratch) -> Extras {
    let mut extras = Extras::new(0, 0).unwrap();
    extras.begin_journal(32, scratch).unwrap();
    extras
}

#[test]
fn reverse_transitive_dependencies_fail_from_original_root_without_repainting_terminals_or_awaits()
{
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    create(&mut core, 3, &[(Kind::DependsOn, 2)]);
    create(&mut core, 4, &[(Kind::Awaits, 1)]);
    create(&mut core, 5, &[(Kind::DependsOn, 1)]);
    create(&mut core, 6, &[]);
    core.publish_native(cancel(&core, 5)).unwrap();
    let terminal = core
        .native_claim(ClaimId::from_u128(5))
        .unwrap()
        .terminal_cut();
    let candidate = cancel(&core, 1);
    let root = changed_root(&candidate, 1);
    let root_binding = root.binding();
    let root_created = root.created();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut scratch = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut extras = extras(&mut scratch);
    let before = core.state.budget.stats();
    let rows = prepare(
        &view,
        root,
        cut(&view),
        core.limits,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (ClaimId(row.binding().object.0), row.status()))
            .collect::<Vec<_>>(),
        vec![
            (ClaimId::from_u128(1), ClaimStatus::Cancelled),
            (ClaimId::from_u128(2), ClaimStatus::DependencyFailed),
            (ClaimId::from_u128(3), ClaimStatus::DependencyFailed),
        ]
    );
    assert_eq!(rows[0].binding(), root_binding);
    for row in &rows[1..] {
        let Some(focal_model::lifecycle::claim::ClaimTerminalCut::Graph(terminal)) =
            row.terminal_cut()
        else {
            panic!("graph cut")
        };
        assert_eq!(terminal.origin().binding(), root_binding);
        assert_eq!(terminal.origin().created(), root_created);
        assert_eq!(terminal.sequence(), cut(&view).position);
    }
    let events = extras.journal.as_ref().unwrap();
    assert_eq!(events.len(), 2);
    for (event, changed) in events.iter().zip(&rows[1..]) {
        let NativeFact::Claim(event) = event else {
            panic!("claim history")
        };
        assert_eq!(event.kind, NativeEventKind::DependencyFailed);
        let original = core
            .native_claim(ClaimId(changed.binding().object.0))
            .unwrap();
        assert_eq!(event.before, Some(original.binding()));
        assert_eq!(event.after, original.binding().next().unwrap());
    }
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(4)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(5))
            .unwrap()
            .terminal_cut(),
        terminal
    );
    assert_eq!(
        core.native_claim(ClaimId::from_u128(6)).unwrap().status(),
        ClaimStatus::Generated
    );
    assert_eq!(core.state.budget.stats(), before);
}

#[test]
fn incomplete_reverse_links_and_closure_limits_refuse_without_partial_publication() {
    for malformed in 0..3 {
        let mut core = core();
        create(&mut core, 1, &[]);
        create(&mut core, 2, &[(Kind::DependsOn, 1)]);
        if malformed == 0 {
            let id = ClaimId::from_u128(1);
            let view = View {
                state: &core.state,
                tail: None,
            };
            let Some(Row::IncomingHead(head)) = view.get(Key::IncomingHead(id)) else {
                panic!("head")
            };
            let mut head = *head;
            head.count += 1;
            let bad = core
                .state
                .rows
                .prepare_batch_with(
                    core.state.rows.prefix() + 1,
                    vec![Change::Put(Entry::new(
                        Key::IncomingHead(id),
                        Row::IncomingHead(head),
                        0,
                    ))],
                    BudgetLane::Completion,
                    |_| panic!("single-row replacement"),
                )
                .unwrap();
            core.state.rows.publish(bad).unwrap();
        }
        let candidate = cancel(&core, 1);
        let root = changed_root(&candidate, 1);
        let view = View {
            state: &core.state,
            tail: None,
        };
        let mut limits = core.limits;
        match malformed {
            1 => limits.plan_nodes = 1,
            2 => limits.plan_edges = 1,
            _ => {}
        }
        let mut scratch = Scratch {
            used: 0,
            max: limits.preparation_bytes,
        };
        let mut extras = extras(&mut scratch);
        assert!(prepare(&view, root, cut(&view), limits, &mut extras, &mut scratch).is_err());
        assert!(extras.journal.unwrap().is_empty());
        assert_eq!(
            view.claim(ClaimId::from_u128(1)).unwrap().status(),
            ClaimStatus::Generated
        );
        assert_eq!(
            view.claim(ClaimId::from_u128(2)).unwrap().status(),
            ClaimStatus::Generated
        );
    }
}

#[test]
fn construction_budget_and_publication_cut_are_checked_before_graph_mutation() {
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    let candidate = cancel(&core, 1);
    let view = View {
        state: &core.state,
        tail: None,
    };
    for bad in 0..3 {
        let mut scratch = Scratch {
            used: 0,
            max: core.limits.preparation_bytes,
        };
        let mut extras = extras(&mut scratch);
        let mut at = cut(&view);
        match bad {
            0 => at.position = view.prefix(),
            1 => at.cause = ContentHash([0; 32]),
            _ => scratch.max = scratch.used + 1,
        }
        assert!(
            prepare(
                &view,
                changed_root(&candidate, 1),
                at,
                core.limits,
                &mut extras,
                &mut scratch
            )
            .is_err()
        );
        assert!(extras.journal.unwrap().is_empty());
        assert_eq!(
            view.claim(ClaimId::from_u128(2)).unwrap().status(),
            ClaimStatus::Generated
        );
    }
}

#[test]
fn already_terminal_connected_rows_keep_their_exact_original_cuts() {
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    core.publish_native(cancel(&core, 2)).unwrap();
    let old = core.native_claim(ClaimId::from_u128(2)).unwrap();
    let old = old.try_copy(old.copy_charge().unwrap()).unwrap();
    let candidate = cancel(&core, 1);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let mut scratch = Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    };
    let mut extras = extras(&mut scratch);
    let rows = prepare(
        &view,
        changed_root(&candidate, 1),
        cut(&view),
        core.limits,
        &mut extras,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(extras.journal.unwrap().is_empty());
    assert_eq!(core.native_claim(ClaimId::from_u128(2)).unwrap(), &old);
}
