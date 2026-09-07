use super::*;
use crate::native::report_tests::{self as fixtures, ISSUER, binding, context};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::lifecycle::graph;

fn id(value: u128) -> ClaimId {
    ClaimId::from_u128(value)
}

fn core() -> Core<NativeState> {
    Core::new_native(
        binding(1).ledger,
        RangeId(2901),
        NativeLimits {
            plan_nodes: 32,
            plan_edges: 4096,
            preparation_bytes: 1024 * 1024,
            range: RangeConfig {
                page_entries: 1,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn create(request: u128, definitions: &[(u128, &[(Kind, u128)])]) -> NativeInput {
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for (claim, edges) in definitions {
        let NativeCommand::Create {
            claims: mut rows,
            declarations: mut definitions,
        } = fixtures::creation(request, *claim, &[], None).command
        else {
            panic!("creation fixture");
        };
        let mut obligations: Vec<_> = edges
            .iter()
            .map(|(kind, target)| Obligation {
                kind: *kind,
                target: id(*target),
            })
            .collect();
        obligations.sort_unstable();
        rows[0].definition.graph = graph::Declaration::new(&obligations, 32).unwrap();
        claims.append(&mut rows);
        declarations.append(&mut definitions);
    }
    NativeInput {
        request: fixtures::request(ISSUER, request),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}

fn view(core: &Core<NativeState>) -> View<'_> {
    View {
        state: &core.state,
        tail: None,
    }
}
fn members(core: &Core<NativeState>, target: u128) -> Vec<ClaimId> {
    incoming(&view(core), id(target), core.limits)
        .map(|row| ClaimId(row.unwrap().binding().object.0))
        .collect()
}
fn prepare(core: &Core<NativeState>, time: u64, input: NativeInput) -> NativePrepared {
    match core
        .prepare_native(context(ISSUER, time), input, &[])
        .unwrap()
    {
        NativePreparation::Prepared(candidate) => candidate,
        NativePreparation::Existing { .. } => panic!("fresh create"),
    }
}
fn replace(core: &mut Core<NativeState>, key: Key, row: Row) {
    let prepared = core
        .state
        .rows
        .prepare_batch_with(
            core.state.rows.prefix() + 1,
            vec![Change::Put(Entry::new(key, row, 0))],
            BudgetLane::Completion,
            |row| match row {
                // Inserting a forged key can split an occupied one-entry leaf.
                // Preserve the actual inline neighbor while corrupting only
                // the explicitly selected head/link under test.
                Row::IncomingHead(row) => Ok(Row::IncomingHead(*row)),
                Row::IncomingLink(row) => Ok(Row::IncomingLink(*row)),
                _ => panic!("unexpected non-index neighbor"),
            },
        )
        .unwrap();
    core.state.rows.publish(prepared).unwrap();
}
fn seeded() -> Core<NativeState> {
    let mut core = core();
    fixtures::publish(
        &mut core,
        1,
        create(
            1,
            &[
                (1, &[]),
                (2, &[(Kind::DependsOn, 1)]),
                (3, &[(Kind::Awaits, 1)]),
                (4, &[]),
            ],
        ),
    );
    core
}

#[test]
fn same_batch_targets_and_shared_heads_index_each_dependent_once() {
    let mut core = core();
    let outcome = fixtures::publish(
        &mut core,
        1,
        create(
            1,
            &[
                (
                    3,
                    &[(Kind::DependsOn, 1), (Kind::Awaits, 1), (Kind::Awaits, 2)],
                ),
                (1, &[]),
                (4, &[(Kind::DependsOn, 1)]),
                (2, &[]),
            ],
        ),
    );
    assert_eq!(outcome.created, 4);
    assert_eq!(outcome.events, 8); // Claim plus immutable declaration, no fake index events.
    assert_eq!(members(&core, 1), vec![id(4), id(3)]);
    assert_eq!(members(&core, 2), vec![id(3)]);
    assert!(members(&core, 3).is_empty());
    assert!(
        matches!(view(&core).get(Key::IncomingHead(id(1))), Some(Row::IncomingHead(IncomingHead { head: Some(found), count: 2 })) if *found == id(4))
    );
    assert!(matches!(
        view(&core).get(Key::IncomingLink(id(1), id(3))),
        Some(Row::IncomingLink(IncomingLink { next: None }))
    ));
    assert!(
        matches!(view(&core).get(Key::IncomingLink(id(1), id(4))), Some(Row::IncomingLink(IncomingLink { next: Some(found) })) if *found == id(3))
    );
}

#[test]
fn pending_append_and_discard_preserve_committed_membership_and_budget() {
    let mut core = seeded();
    let baseline = core.native_budget();
    let original = members(&core, 1);
    let candidate = prepare(
        &core,
        2,
        create(2, &[(5, &[(Kind::DependsOn, 1), (Kind::Awaits, 1)])]),
    );
    let tail = View {
        state: &core.state,
        tail: Some(&candidate),
    };
    let discovered: Vec<_> = incoming(&tail, id(1), core.limits)
        .map(|row| ClaimId(row.unwrap().binding().object.0))
        .collect();
    assert_eq!(discovered, vec![id(5), id(3), id(2)]);
    assert_eq!(members(&core, 1), original);
    assert!(core.native_claim(id(5)).is_none());
    drop(candidate);
    assert_eq!(core.native_budget(), baseline);
    assert_eq!(members(&core, 1), original);
    let candidate = prepare(
        &core,
        2,
        create(2, &[(5, &[(Kind::DependsOn, 1), (Kind::Awaits, 1)])]),
    );
    core.publish_native(candidate).unwrap();
    assert_eq!(members(&core, 1), vec![id(5), id(3), id(2)]);
}

#[test]
fn iterator_is_bounded_and_rejects_unknown_targets_without_allocation() {
    let core = seeded();
    let budget = core.native_budget();
    let source = view(&core);
    let mut bounded = incoming(
        &source,
        id(1),
        NativeLimits {
            plan_nodes: 1,
            ..core.limits
        },
    );
    assert!(matches!(bounded.next(), Some(Err(ContractError::Capacity))));
    assert!(bounded.next().is_none());
    let mut limited = incoming(
        &source,
        id(1),
        NativeLimits {
            plan_edges: 1,
            ..core.limits
        },
    );
    assert!(matches!(limited.next(), Some(Err(ContractError::Capacity))));
    assert!(limited.next().is_none());
    let mut absent = incoming(&source, id(99), core.limits);
    assert!(matches!(
        absent.next(),
        Some(Err(ContractError::InvalidTarget))
    ));
    assert!(absent.next().is_none());
    assert_eq!(core.native_budget(), budget);
    assert_eq!(members(&core, 1), vec![id(3), id(2)]);
}

#[test]
fn malformed_chain_is_rejected_before_exposing_any_dependent() {
    for (key, row) in [
        (
            Key::IncomingHead(id(1)),
            Row::IncomingHead(IncomingHead {
                head: None,
                count: 2,
            }),
        ),
        (
            Key::IncomingHead(id(1)),
            Row::IncomingHead(IncomingHead {
                head: Some(id(3)),
                count: 1,
            }),
        ),
        (
            Key::IncomingHead(id(1)),
            Row::IncomingHead(IncomingHead {
                head: Some(id(3)),
                count: 3,
            }),
        ),
        (
            Key::IncomingLink(id(1), id(2)),
            Row::IncomingLink(IncomingLink { next: Some(id(3)) }),
        ),
        (
            Key::IncomingLink(id(1), id(3)),
            Row::IncomingLink(IncomingLink { next: Some(id(99)) }),
        ),
        (
            Key::IncomingLink(id(1), id(3)),
            Row::IncomingHead(IncomingHead::default()),
        ),
    ] {
        let mut core = seeded();
        replace(&mut core, key, row);
        let budget = core.native_budget();
        let source = view(&core);
        let mut found = incoming(&source, id(1), core.limits);
        assert!(matches!(found.next(), Some(Err(_))));
        assert!(found.next().is_none());
        assert_eq!(core.native_budget(), budget);
    }
}

#[test]
fn plausible_link_cannot_replace_the_dependents_actual_immutable_declaration() {
    let mut core = seeded();
    replace(
        &mut core,
        Key::IncomingHead(id(4)),
        Row::IncomingHead(IncomingHead {
            head: Some(id(2)),
            count: 1,
        }),
    );
    replace(
        &mut core,
        Key::IncomingLink(id(4), id(2)),
        Row::IncomingLink(IncomingLink { next: None }),
    );
    let source = view(&core);
    let mut found = incoming(&source, id(4), core.limits);
    assert!(matches!(
        found.next(),
        Some(Err(ContractError::InvalidTarget))
    ));
    assert!(found.next().is_none());
    assert_eq!(members(&core, 1), vec![id(3), id(2)]);
}

#[test]
fn create_cannot_append_to_a_corrupt_head_or_publish_partial_index_rows() {
    let mut core = seeded();
    replace(
        &mut core,
        Key::IncomingHead(id(1)),
        Row::IncomingHead(IncomingHead {
            head: Some(id(3)),
            count: 1,
        }),
    );
    let budget = core.native_budget();
    let range = core.native_stats();
    let result = core.prepare_native(
        context(ISSUER, 3),
        create(
            3,
            &[(5, &[(Kind::DependsOn, 4)]), (6, &[(Kind::Awaits, 1)])],
        ),
        &[],
    );
    assert!(matches!(
        result,
        Err(NativeError::Contract(ContractError::InvalidManifest))
    ));
    assert_eq!(core.native_budget(), budget);
    assert_eq!(core.native_stats(), range);
    assert!(core.native_claim(id(5)).is_none());
    assert!(core.native_claim(id(6)).is_none());
    assert!(view(&core).get(Key::IncomingHead(id(4))).is_none());
    assert!(view(&core).get(Key::IncomingLink(id(4), id(5))).is_none());
}
