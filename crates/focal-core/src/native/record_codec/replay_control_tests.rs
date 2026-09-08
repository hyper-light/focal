//! Ordinary dependency propagation is independent of runtime monitor presence.
//! The same actual control journal must remain valid after detached replay.
use super::*;
use focal_model::lifecycle::{graph, succession::CorrectionKind};

fn control_core() -> Core<NativeState> {
    Core::new_native(
        f::ledger(),
        RangeId(14_001),
        NativeLimits {
            plan_nodes: 32,
            plan_edges: 65_536,
            preparation_bytes: 2 << 20,
            range: RangeConfig {
                page_entries: 4,
                max_batch_entries: 128,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        MemoryBudget::new(128 << 20, 16 << 20).unwrap(),
    )
    .unwrap()
}
type Spec<'a> = (u128, Option<CorrectionKind>, &'a [(graph::Kind, u128)]);
fn create_controls(request: u128, specs: &[Spec<'_>]) -> NativeInput {
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for &(id, correction, edges) in specs {
        let NativeCommand::Create {
            claims: mut claim,
            declarations: mut definitions,
        } = f::creation(request, id, &[], correction).command
        else {
            panic!("create");
        };
        let obligations = edges
            .iter()
            .map(|&(kind, target)| graph::Obligation {
                kind,
                target: ClaimId::from_u128(target),
            })
            .collect::<Vec<_>>();
        claim[0].definition.graph = graph::Declaration::new(&obligations, 16).unwrap();
        claims.append(&mut claim);
        declarations.append(&mut definitions);
    }
    NativeInput {
        request: f::request(f::ISSUER, request),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}
fn status(core: &Core<NativeState>, id: u128) -> ClaimStatus {
    core.native_claim(ClaimId::from_u128(id)).unwrap().status()
}

#[test]
fn cancellation_propagates_pure_dependencies_without_monitors_and_replays_atomically() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = control_core();
    let mut recovered = restored(&original, 14_002, &store);
    apply(
        &mut original,
        &mut recovered,
        10,
        create_controls(
            1,
            &[
                (1, None, &[]),
                (2, None, &[(graph::Kind::DependsOn, 1)]),
                (3, None, &[(graph::Kind::DependsOn, 2)]),
                (4, None, &[(graph::Kind::Awaits, 1)]),
            ],
        ),
        &store,
    );
    let expected = original
        .native_claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let prepared = prepare_input(
        &original,
        20,
        NativeInput {
            request: f::request(f::ISSUER, 2),
            command: NativeCommand::Cancel { expected },
        },
    );
    let outcome = prepared.outcome();
    assert_eq!(
        prepared.claim(ClaimId::from_u128(1)).unwrap().status(),
        ClaimStatus::Cancelled
    );
    for id in [2, 3] {
        assert_eq!(
            prepared.claim(ClaimId::from_u128(id)).unwrap().status(),
            ClaimStatus::DependencyFailed
        );
    }
    assert_eq!(status(&original, 1), ClaimStatus::Generated);
    let bytes = encode_mutation(&prepared);
    let before = checkpoint::encode(&recovered);
    let budget = recovered.native_budget();
    let candidate = replay(&recovered, &bytes, original.state.rows.id(), &store).unwrap();
    drop(candidate);
    assert_eq!(recovered.native_budget(), budget);
    assert_eq!(checkpoint::encode(&recovered), before);
    let candidate = replay(&recovered, &bytes, original.state.rows.id(), &store).unwrap();
    original.publish_native(prepared).unwrap();
    recovered.publish_native(candidate).unwrap();
    checkpoint::compare(&original, &recovered);
    assert_eq!(status(&recovered, 1), ClaimStatus::Cancelled);
    assert_eq!(status(&recovered, 2), ClaimStatus::DependencyFailed);
    assert_eq!(status(&recovered, 3), ClaimStatus::DependencyFailed);
    assert_eq!(status(&recovered, 4), ClaimStatus::Generated);
    for id in [1, 2, 3] {
        let claim = recovered.native_claim(ClaimId::from_u128(id)).unwrap();
        assert_eq!(claim.local_sealed_at(), Some(outcome.sequence));
        assert!(!claim.scopes().released());
        assert_eq!(claim.response_count(), 0);
    }
    assert_eq!(
        (outcome.artifacts, outcome.responses, outcome.results),
        (0, 0, 0)
    );
    let graph_events = (0..outcome.events)
        .filter_map(|ordinal| {
            recovered
                .native_event(outcome.sequence, ordinal)
                .unwrap()
                .claim_event()
        })
        .filter(|event| event.kind == NativeEventKind::DependencyFailed)
        .collect::<Vec<_>>();
    assert_eq!(graph_events.len(), 2);
    assert!(graph_events.iter().all(|event| event.graph.is_some()));
    match recovered.state.rows.get(&Key::Meta).unwrap() {
        Row::Meta(meta) => assert_eq!(meta.monitors, 0),
        _ => panic!("metadata"),
    }
}

#[test]
fn multi_root_creation_keeps_disconnected_successors_and_settles_new_and_existing_dependents() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = control_core();
    let mut recovered = restored(&original, 14_003, &store);
    apply(
        &mut original,
        &mut recovered,
        10,
        create_controls(
            1,
            &[(1, None, &[]), (2, None, &[(graph::Kind::DependsOn, 1)])],
        ),
        &store,
    );
    let bytes = apply(
        &mut original,
        &mut recovered,
        20,
        create_controls(
            2,
            &[
                (5, Some(CorrectionKind::Supersedes), &[]),
                (6, None, &[]),
                (7, None, &[(graph::Kind::DependsOn, 1)]),
            ],
        ),
        &store,
    );
    let outcome = inspect(&bytes).header().outcome;
    assert_eq!(outcome.created, 3);
    assert_eq!(status(&recovered, 1), ClaimStatus::Superseded);
    assert_eq!(status(&recovered, 2), ClaimStatus::DependencyFailed);
    assert_eq!(status(&recovered, 5), ClaimStatus::Generated);
    assert_eq!(status(&recovered, 6), ClaimStatus::Generated);
    assert_eq!(status(&recovered, 7), ClaimStatus::DependencyFailed);
    let Some(Row::Claim(newly_failed)) =
        recovered.state.rows.get(&Key::Claim(ClaimId::from_u128(7)))
    else {
        panic!("newly created failed claim")
    };
    let registry = newly_failed.registrations().unwrap();
    assert!(registry.rows().is_empty());
    assert!(registry.is_sealed());
    assert_eq!(registry.snapshot_v1().claim, f::binding(7));
    assert_eq!(registry.snapshot_v1().sealed_at, Some(outcome.sequence));
    let events = (0..outcome.events)
        .map(|ordinal| recovered.native_event(outcome.sequence, ordinal).unwrap())
        .collect::<Vec<_>>();
    let original_prefix_end = events
        .iter()
        .filter_map(|event| {
            event
                .claim_event()
                .filter(|claim| {
                    matches!(
                        claim.kind,
                        NativeEventKind::Created | NativeEventKind::Superseded
                    )
                })
                .map(|_| event.ordinal + 1)
        })
        .max()
        .unwrap();
    let dependent = events
        .iter()
        .filter_map(|event| event.claim_event())
        .filter(|event| event.kind == NativeEventKind::DependencyFailed)
        .collect::<Vec<_>>();
    assert_eq!(dependent.len(), 2);
    assert!(
        dependent
            .iter()
            .all(|event| event.graph.unwrap().before_ordinal >= original_prefix_end)
    );
    let reopened = restored(&recovered, 14_004, &store);
    checkpoint::compare(&original, &reopened);
    match recovered.state.rows.get(&Key::Meta).unwrap() {
        Row::Meta(meta) => assert_eq!(meta.monitors, 0),
        _ => panic!("metadata"),
    }
}
