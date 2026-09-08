//! Check actual control prefixes and real model graph/scope replacements.
use super::*;
use crate::native::{
    control_graph, monitor_index,
    prepare::{Extras, Scratch},
    report_tests as f,
};
use focal_model::lifecycle::{
    claim::ClaimCut,
    creation, graph, scope,
    succession::{CorrectionKind, Lineage},
};
use focal_model::{Cause, Deadline, MonitorId, TimerId, WaitPredicate};

fn id(n: u128) -> ClaimId {
    ClaimId::from_u128(n)
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
        max: 32 * 1024 * 1024,
    }
}
fn fixture() -> Core<NativeState> {
    let mut core = f::core();
    core.limits.plan_edges = 8 * 1024 * 1024;
    core.limits.preparation_bytes = 32 * 1024 * 1024;
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for n in 1..=3 {
        let NativeCommand::Create {
            claims: mut one,
            declarations: mut definitions,
        } = f::creation(1, n, &[], None).command
        else {
            panic!("create")
        };
        if n == 2 {
            one[0].definition.graph = graph::Declaration::new(
                &[graph::Obligation {
                    kind: graph::Kind::DependsOn,
                    target: id(1),
                }],
                8,
            )
            .unwrap();
        }
        claims.append(&mut one);
        declarations.append(&mut definitions);
    }
    f::publish(
        &mut core,
        1,
        NativeInput {
            request: f::request(f::ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    let expected = core.native_claim(id(3)).unwrap().binding();
    f::publish(
        &mut core,
        2,
        NativeInput {
            request: f::request(f::ISSUER, 2),
            command: NativeCommand::RegisterMonitor {
                expected,
                receipt: None,
                id: MonitorId::from_u128(90),
                roots: vec![WaitPredicate::Terminal(id(2))],
                deadline: Deadline {
                    timer: TimerId::from_u128(91),
                    generation: 1,
                    at: 1000,
                },
            },
        },
    );
    core
}
struct Staged {
    plan: transactions::Plan,
    extras: Extras,
    meta: Meta,
    outcome: NativeOutcome,
    scratch: Scratch,
}
fn stage(core: &Core<NativeState>, input: NativeInput, operation: NativeOperation) -> Staged {
    let view = view(core);
    let sequence = SessionSeq(view.prefix().0 + 1);
    let intent = intent::fingerprint(view.ledger(), &input).unwrap();
    let mut meta = view.meta();
    meta.logical_time = 10;
    meta.outcomes += 1;
    let mut extras = Extras::new(
        core.limits.range.max_batch_entries,
        core.limits.preparation_bytes,
    )
    .unwrap();
    let mut scratch = scratch();
    let mut plan = transactions::prepare(
        input.command,
        input.request,
        None,
        f::context(f::ISSUER, 10),
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
    let count = |test: fn(Key) -> bool| {
        u32::try_from(extras.rows.iter().filter(|row| test(row.key)).count()).unwrap()
    };
    let events = extras.events()
        + if extras.journal.is_some() {
            0
        } else {
            event_count(&plan.rows, &view, operation).unwrap()
        };
    meta.events += events;
    let outcome = NativeOutcome {
        ledger: view.ledger(),
        invocation: input.request.into(),
        sequence,
        logical_time: 10,
        operation,
        intent,
        created: u32::try_from(plan.created).unwrap(),
        changed: u32::try_from(plan.rows.len()).unwrap(),
        definitions: count(|key| matches!(key, Key::Definition(_))),
        evaluations: count(|key| matches!(key, Key::Evaluation(_))),
        artifacts: 0,
        results: 0,
        receipts: 0,
        responses: 0,
        result_testaments: 0,
        events: u32::try_from(events).unwrap(),
    };
    assert!(extras.control_graph.is_some());
    control_graph::check(&plan.rows, &extras, &view, outcome, core.limits).unwrap();
    check_journal(&plan.rows, &extras, &view, core.limits).unwrap();
    monitor_index::check_journal(&view, &plan.rows, &extras, core.limits, &mut scratch).unwrap();
    Staged {
        plan,
        extras,
        meta,
        outcome,
        scratch,
    }
}
fn cancellation(core: &Core<NativeState>) -> Staged {
    stage(
        core,
        NativeInput {
            request: f::request(f::ISSUER, 3),
            command: NativeCommand::Cancel {
                expected: core.native_claim(id(1)).unwrap().binding(),
            },
        },
        NativeOperation::Cancel,
    )
}
fn limits() -> graph::Limits {
    graph::Limits {
        nodes: 32,
        edges: 1024,
        visits: 1_000_000,
    }
}

#[test]
fn control_peer_terminal_cut_must_use_actual_candidate_publication() {
    let core = fixture();
    let mut staged = cancellation(&core);
    let forged = {
        let root = staged
            .extras
            .control_graph
            .as_ref()
            .unwrap()
            .originals()
            .iter()
            .find(|row| row.binding().object.0 == id(1).0)
            .unwrap();
        let peer = core.native_claim(id(2)).unwrap();
        let observer = core.native_claim(id(3)).unwrap();
        let graph = graph::Snapshot::capture(&[root, peer, observer], limits()).unwrap();
        let failure = graph
            .dependency_failure_with_budget(id(2), usize::MAX)
            .unwrap();
        let mut replacement = peer.try_copy(usize::MAX).unwrap();
        replacement
            .dependency_failed(
                &peer.binding(),
                &failure,
                &[root, observer],
                SessionSeq(staged.outcome.sequence.0 + 1),
            )
            .unwrap();
        replacement
    };
    let peer = staged
        .plan
        .rows
        .iter_mut()
        .find(|row| row.binding().object.0 == id(2).0)
        .unwrap();
    assert_eq!(forged.binding(), peer.binding());
    assert_eq!(forged.status(), peer.status());
    assert_ne!(forged.terminal_cut(), peer.terminal_cut());
    *peer = forged;
    assert!(matches!(
        control_graph::check(
            &staged.plan.rows,
            &staged.extras,
            &view(&core),
            staged.outcome,
            core.limits
        ),
        Err(NativeError::Contract(ContractError::InvalidCut))
    ));
}

#[test]
fn duplicate_terminal_fact_cannot_hide_an_unjournaled_owner_release() {
    let core = fixture();
    let mut staged = cancellation(&core);
    let (released, before) = {
        let rows: Vec<_> = staged.plan.rows.iter().collect();
        let source = rows
            .iter()
            .copied()
            .find(|row| row.binding().object.0 == id(2).0)
            .unwrap();
        let peers: Vec<_> = rows
            .iter()
            .copied()
            .filter(|row| row.binding().object != source.binding().object)
            .collect();
        let graph = graph::Snapshot::capture(&rows, limits()).unwrap();
        let transition = scope::Registry::prepare_release_owner_bounded(
            source,
            &graph,
            &peers,
            ClaimCut {
                position: staged.outcome.sequence,
                cause: staged.outcome.intent,
            },
            usize::MAX,
            1_000_000,
        )
        .unwrap()
        .build()
        .unwrap();
        let mut released = source.try_copy(usize::MAX).unwrap();
        released
            .apply_scope(&source.binding(), transition, &peers)
            .unwrap();
        (released, source.binding())
    };
    assert!(released.scopes().released());
    let fake = NativeFact::Claim(NativeClaimEvent {
        graph: None,
        kind: NativeEventKind::DependencyFailed,
        owned_child: None,
        before: Some(before),
        after: released.binding(),
        status: released.status(),
    });
    *staged
        .plan
        .rows
        .iter_mut()
        .find(|row| row.binding().object.0 == id(2).0)
        .unwrap() = released;
    staged.extras.record(fake).unwrap();
    assert!(check_journal(&staged.plan.rows, &staged.extras, &view(&core), core.limits).is_err());
    assert!(
        monitor_index::check_journal(
            &view(&core),
            &staged.plan.rows,
            &staged.extras,
            core.limits,
            &mut scratch()
        )
        .is_err()
    );
    assert!(
        control_graph::check(
            &staged.plan.rows,
            &staged.extras,
            &view(&core),
            staged.outcome,
            core.limits
        )
        .is_err()
    );
}

#[test]
fn creation_with_new_parent_child_and_supersession_keeps_true_original_prefix() {
    let core = fixture();
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for n in 4..=6 {
        let NativeCommand::Create {
            claims: mut one,
            declarations: mut definitions,
        } = f::creation(3, n, &[], (n == 6).then_some(CorrectionKind::Supersedes)).command
        else {
            panic!("create")
        };
        if n == 5 {
            one[0].definition.lineage =
                Lineage::new(f::binding(n), Cause::Claim(id(4)), &[], 8).unwrap();
            one[0].owner = Some(creation::Owner {
                expected: f::binding(4),
                receipt: None,
            });
        }
        claims.append(&mut one);
        declarations.append(&mut definitions);
    }
    let staged = stage(
        &core,
        NativeInput {
            request: f::request(f::ISSUER, 3),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
        NativeOperation::Create,
    );
    let proof = staged.extras.control_graph.as_ref().unwrap();
    let parent = proof
        .originals()
        .iter()
        .find(|row| row.binding().object.0 == id(4).0)
        .unwrap();
    assert_eq!(parent.binding().revision.0, 2);
    assert_eq!(parent.scopes().children().len(), 1);
    assert!(proof.prefix().iter().any(|fact|matches!(fact,NativeFact::Claim(event) if event.kind==NativeEventKind::Created && event.after.object.0==id(4).0 && event.after.revision.0==1)));
    assert!(staged.extras.journal.as_ref().unwrap().iter().any(|fact|matches!(fact,NativeFact::Claim(event) if event.kind==NativeEventKind::Monitor(NativeMonitorEvent::Released{id:MonitorId::from_u128(90),cut:ClaimCut{position:staged.outcome.sequence,cause:staged.outcome.intent}}))));
    assert_eq!(
        staged
            .plan
            .rows
            .iter()
            .find(|row| row.binding().object.0 == id(1).0)
            .unwrap()
            .status(),
        ClaimStatus::Superseded
    );
    let Staged {
        plan,
        extras,
        meta,
        outcome,
        mut scratch,
    } = staged;
    OriginalPlan::check(
        plan,
        extras,
        meta,
        outcome,
        &view(&core),
        core.limits,
        &mut scratch,
    )
    .unwrap();
}
