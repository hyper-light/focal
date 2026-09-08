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
fn future_heap_ceiling_prices_peer_history_and_registry_copies_without_repricing_topology() {
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
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
    let original = plan.budget();
    let expanded = plan
        .with_future_heaps(|claim| {
            Ok((
                heap(claim)? + 100,
                transactions::registry_heap(registry(&view, claim)?)? + 200,
            ))
        })
        .unwrap();
    assert_eq!(expanded.charges().nodes, original.charges().nodes);
    assert_eq!(expanded.charges().changed_rows, 2);
    assert_eq!(
        expanded.charges().preparation_bytes,
        original.charges().preparation_bytes + 600
    );
    assert_eq!(
        expanded.charges().incoming_heap_bytes,
        original.charges().incoming_heap_bytes + 600
    );
    assert_eq!(expanded.graph_bytes, original.graph_bytes + 100);
    assert_eq!(expanded.membership, original.membership);
    expanded.check_candidate(&plan).unwrap();
    assert!(
        plan.with_future_heaps(|claim| Ok((heap(claim)?.saturating_sub(1), 0)))
            .is_err()
    );
}

#[test]
fn terminal_peer_keeps_its_source_identity_without_a_future_copy_obligation() {
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    core.publish_native(cancel(&core, 2)).unwrap();
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
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
    let mut copied = Vec::new();
    let expanded = plan
        .with_future_heaps(|claim| {
            copied.push(ClaimId(claim.binding().object.0));
            Ok((
                heap(claim)?,
                transactions::registry_heap(registry(&view, claim)?)?,
            ))
        })
        .unwrap();
    assert_eq!(copied, vec![ClaimId::from_u128(1)]);
    assert_eq!(plan.members().len(), 2);
    assert_eq!(expanded.charges().changed_rows, 1);
    expanded.check_candidate(&plan).unwrap();
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
        let original = view.claim(ClaimId(row.binding().object.0)).unwrap();
        assert!(heap(row).unwrap() <= heap(original).unwrap());
        assert_eq!(row.response_count(), original.response_count());
        assert_eq!(row.scopes(), original.scopes());
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

fn scratch(core: &Core<NativeState>) -> Scratch {
    Scratch {
        used: 0,
        max: core.limits.preparation_bytes,
    }
}

#[test]
fn preflight_pins_complete_sources_and_bounds_the_actual_consequence_rows() {
    let mut core = core();
    let input = f::creation(1, 1, &[(focal_model::ValidationMode::Observe, false)], None);
    f::publish(&mut core, 1, input);
    f::publish(&mut core, 2, f::post(20, binding(1)));
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    create(&mut core, 3, &[(Kind::Awaits, 1)]);
    create(&mut core, 4, &[]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
    let mut staged = scratch(&core);
    let plan = preflight(&view, root, core.limits, None, &mut staged).unwrap();
    assert_eq!(
        plan.members()
            .iter()
            .map(|row| ClaimId(row.binding().object.0))
            .collect::<Vec<_>>(),
        vec![
            ClaimId::from_u128(1),
            ClaimId::from_u128(2),
            ClaimId::from_u128(3)
        ]
    );
    assert!(std::ptr::eq(plan.members()[0], root));
    assert_eq!(registry(&view, plan.members()[0]).unwrap().rows().len(), 1);
    let budget = plan.budget();
    let quote = budget.charges();
    assert_eq!(
        (quote.nodes, quote.changed_rows, quote.graph_events),
        (3, 3, 3)
    );
    assert!(quote.registry_heap_bytes > 0);
    assert!(
        quote.preparation_bytes >= staged.used + quote.claim_heap_bytes + quote.registry_heap_bytes
    );
    drop(plan);
    let candidate = cancel(&core, 1);
    let root = changed_root(&candidate, 1);
    let mut staged = scratch(&core);
    preflight(&view, &root, core.limits, Some(budget), &mut staged).unwrap();
    let mut staged = scratch(&core);
    let mut events = extras(&mut staged);
    let rows = prepare(
        &view,
        root,
        cut(&view),
        core.limits,
        &mut events,
        &mut staged,
    )
    .unwrap();
    budget.check_output(&view, &rows, events.events()).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(events.events(), 1);
    assert_eq!(
        core.native_claim(ClaimId::from_u128(2)).unwrap().status(),
        ClaimStatus::Generated
    );
}

#[test]
fn proposed_reverse_link_growth_is_detected_even_when_target_row_is_unchanged() {
    let mut core = core();
    create(&mut core, 1, &[]);
    let original = core.native_claim(ClaimId::from_u128(1)).unwrap().binding();
    let budget = {
        let view = View {
            state: &core.state,
            tail: None,
        };
        preflight(
            &view,
            view.claim(ClaimId::from_u128(1)).unwrap(),
            core.limits,
            None,
            &mut scratch(&core),
        )
        .unwrap()
        .budget()
    };
    let mut input = f::creation(2, 2, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("create")
    };
    claims[0].definition.graph = graph::Declaration::new(
        &[Obligation {
            kind: Kind::DependsOn,
            target: ClaimId::from_u128(1),
        }],
        1,
    )
    .unwrap();
    let candidate = f::prepared(core.prepare_native(f::context(ISSUER, 2), input, &[]));
    let view = View {
        state: &core.state,
        tail: Some(&candidate),
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
    assert_eq!(root.binding(), original);
    let before = core.native_budget();
    assert!(matches!(
        preflight(&view, root, core.limits, Some(budget), &mut scratch(&core)),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    let replacement = preflight(&view, root, core.limits, None, &mut scratch(&core)).unwrap();
    assert_eq!(replacement.members().len(), 2);
    assert_eq!(
        replacement.members()[1].binding(),
        candidate.claim(ClaimId::from_u128(2)).unwrap().binding()
    );
    assert_eq!(core.native_budget(), before);
    assert!(core.native_claim(ClaimId::from_u128(2)).is_none());
}

#[test]
fn registry_growth_and_prefix_rewind_cannot_reuse_a_cheaper_graph_quote() {
    let mut core = core();
    f::publish(
        &mut core,
        1,
        f::creation(1, 1, &[(focal_model::ValidationMode::Observe, false)], None),
    );
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
    let original = preflight(&view, root, core.limits, None, &mut scratch(&core))
        .unwrap()
        .budget();
    assert_eq!(original.charges().registry_heap_bytes, 0);
    let candidate =
        f::prepared(core.prepare_native(f::context(ISSUER, 2), f::post(2, root.binding()), &[]));
    let pending = View {
        state: &core.state,
        tail: Some(&candidate),
    };
    let changed = pending.claim(ClaimId::from_u128(1)).unwrap();
    assert!(matches!(
        preflight(
            &pending,
            changed,
            core.limits,
            Some(original),
            &mut scratch(&core)
        ),
        Err(NativeError::Capacity(_))
    ));
    let next = preflight(&pending, changed, core.limits, None, &mut scratch(&core))
        .unwrap()
        .budget();
    assert!(next.charges().registry_heap_bytes > 0);
    assert_eq!(next.charges().nodes, original.charges().nodes);
    assert!(matches!(
        preflight(&view, root, core.limits, Some(next), &mut scratch(&core)),
        Err(NativeError::Contract(ContractError::StaleRevision))
    ));
    assert_eq!(
        core.native_claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Generated
    );
}

#[test]
fn graph_quote_checks_remaining_stage_bytes_before_copying_or_mutating_sources() {
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[(Kind::DependsOn, 1)]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let root = view.claim(ClaimId::from_u128(1)).unwrap();
    let budget = preflight(&view, root, core.limits, None, &mut scratch(&core))
        .unwrap()
        .budget();
    let remaining = budget.charges().preparation_bytes - heap(root).unwrap();
    let before = core.native_budget();
    preflight(
        &view,
        root,
        core.limits,
        None,
        &mut Scratch {
            used: 0,
            max: remaining,
        },
    )
    .unwrap();
    assert!(matches!(
        preflight(
            &view,
            root,
            core.limits,
            None,
            &mut Scratch {
                used: 0,
                max: remaining - 1
            }
        ),
        Err(NativeError::Capacity(_))
    ));
    assert_eq!(core.native_budget(), before);
    assert_eq!(root.status(), ClaimStatus::Generated);
    let mut wrong = core.limits;
    wrong.plan_edges -= 1;
    assert!(matches!(
        preflight(&view, root, wrong, Some(budget), &mut scratch(&core)),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
}

#[test]
fn retained_active_scope_without_its_actual_subscription_refuses_graph_preflight() {
    use focal_model::lifecycle::scope;
    use focal_model::{Deadline, MonitorId, TimerId, WaitPredicate};
    let mut core = core();
    create(&mut core, 1, &[]);
    create(&mut core, 2, &[]);
    let view = View {
        state: &core.state,
        tail: None,
    };
    let original = view.claim(ClaimId::from_u128(1)).unwrap();
    let target = view.claim(ClaimId::from_u128(2)).unwrap();
    let mut changed = original
        .try_copy(original.retained_bytes().unwrap())
        .unwrap();
    let graph = graph::Snapshot::capture(
        &[original, target],
        graph::Limits {
            nodes: 2,
            edges: 2,
            visits: 100,
        },
    )
    .unwrap();
    let transition = scope::Registry::prepare_register(
        &changed,
        scope::Authority {
            principal: Principal::Actor(ISSUER),
            expected: changed.binding(),
            receipt: None,
            cut: cut(&view),
            now: 0,
        },
        scope::Registration {
            id: MonitorId::from_u128(1),
            roots: &[WaitPredicate::Satisfied(ClaimId::from_u128(2))],
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 100,
            },
        },
        &graph,
    )
    .unwrap();
    changed
        .apply_scope(&changed.binding(), transition, &[target])
        .unwrap();
    let registered = view
        .owned_claim(ClaimId::from_u128(1))
        .unwrap()
        .registrations()
        .unwrap();
    let registered = registered
        .try_copy(registered.retained_bytes().unwrap())
        .unwrap();
    let owned = OwnedClaim::new(changed, registered).unwrap();
    let heap = owned.heap_charge().unwrap();
    // Corrupt native storage by importing a real model scope while omitting its
    // required allocation and subscription rows. Discovery must fail closed.
    let replacement = core
        .state
        .rows
        .prepare_batch_with(
            core.state.rows.prefix() + 1,
            vec![Change::Put(Entry::new(
                Key::Claim(ClaimId::from_u128(1)),
                Row::Claim(owned),
                heap,
            ))],
            BudgetLane::Completion,
            |_| panic!("single-row replacement"),
        )
        .unwrap();
    core.state.rows.publish(replacement).unwrap();
    let view = View {
        state: &core.state,
        tail: None,
    };
    assert!(matches!(
        preflight(
            &view,
            view.claim(ClaimId::from_u128(1)).unwrap(),
            core.limits,
            None,
            &mut scratch(&core)
        ),
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
}
