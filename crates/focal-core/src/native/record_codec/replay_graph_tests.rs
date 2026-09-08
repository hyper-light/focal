use super::*;
use focal_model::lifecycle::{claim::ClaimTerminalCut, graph};
use focal_model::{Deadline, TimerId, VerdictValue};

fn dependent(core: &mut Core<NativeState>, id: u128, parent: u128) {
    let mut input = f::creation(10_000 + id, id, &[], None);
    let NativeCommand::Create { claims, .. } = &mut input.command else {
        panic!("create")
    };
    claims[0].definition.graph = graph::Declaration::new(
        &[graph::Obligation {
            kind: graph::Kind::DependsOn,
            target: ClaimId::from_u128(parent),
        }],
        1,
    )
    .unwrap();
    f::publish(core, 40 + id as u64, input);
}
fn graph_events(record: &[u8]) -> Vec<(NativeEvent, usize)> {
    inspect(record)
        .rows(usize::MAX)
        .unwrap()
        .map(Result::unwrap)
        .filter_map(|row| {
            let Key::Event(..) = row.key else {
                return None;
            };
            let mut cursor = bytes::Cursor::new(row.body(), row.body().len(), usize::MAX).unwrap();
            let event = super::super::super::read_events::event(&mut cursor).unwrap();
            let NativeFact::Claim(value) = event.fact else {
                return None;
            };
            value.graph.map(|_| {
                (
                    event,
                    row.body().as_ptr() as usize - record.as_ptr() as usize + row.body().len() - 4,
                )
            })
        })
        .collect()
}
fn refused_graph(core: &Core<NativeState>, bytes: &[u8], source: RangeId, store: &ContentStore) {
    let before = checkpoint::encode(core);
    let budget = core.native_budget();
    assert!(replay(core, bytes, source, store).is_err());
    assert_eq!(core.native_budget(), budget);
    assert_eq!(checkpoint::encode(core), before);
}

fn verify_report(
    store: &mut ContentStore,
    input: &NativeInput,
    budget: &MemoryBudget,
) -> focal_evidence::VerifiedNativeArtifact {
    let NativeCommand::ReportAdmission { artifact, .. } = &input.command else {
        panic!("admission report")
    };
    store
        .verify_native_artifact(
            input.request,
            artifact.get().unwrap(),
            focal_model::ContentDomainId::from_u128(93),
            budget,
            &BuiltinNativeSchemas,
        )
        .unwrap()
}

#[test]
fn original_shared_dependency_snapshot_survives_replay_and_refuses_stale_capture_or_path() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = checkpoint::store(directory.path());
    let mut original = f::running(&[(ValidationMode::Required, false)]);
    dependent(&mut original, 2, 1);
    dependent(&mut original, 3, 2);
    let mut recovered = restored(&original, 11_001, &store);
    let source = original.state.rows.id();
    let custody = checkpoint::budget();
    let input = f::report_for(
        &original,
        None,
        20_001,
        1,
        VerdictValue::Fail,
        f::descriptor(f::artifact_spec(20_001, f::EVALUATOR, VerdictValue::Fail)),
    );
    let proof = verify_report(&mut store, &input, &custody);
    let candidate = f::report(&original, input, &[], &proof);
    let bytes = encode_mutation(&candidate);
    let events = graph_events(&bytes);
    assert_eq!(events.len(), 2);
    let NativeFact::Claim(first) = events[0].0.fact else {
        panic!("claim")
    };
    let NativeFact::Claim(second) = events[1].0.fact else {
        panic!("claim")
    };
    assert_eq!(first.graph, second.graph);
    assert_eq!(first.graph.unwrap().before_ordinal, events[0].0.ordinal);
    assert!(events[1].0.ordinal > second.graph.unwrap().before_ordinal);
    // Both graph facts were computed against the same original batch. Making
    // the second consume the already-failed first dependent changes its path.
    let mut changed = bytes.clone();
    let offset = events[1].1;
    changed[offset..offset + 4].copy_from_slice(&events[1].0.ordinal.to_le_bytes());
    checksum(&mut changed);
    refused_graph(&recovered, &changed, source, &store);
    let mut changed = bytes.clone();
    for (_, offset) in &events {
        changed[*offset..*offset + 4].copy_from_slice(&0u32.to_le_bytes());
    }
    checksum(&mut changed);
    refused_graph(&recovered, &changed, source, &store);
    let Some(ClaimTerminalCut::Graph(cut)) = candidate
        .claim(ClaimId::from_u128(3))
        .unwrap()
        .terminal_cut()
    else {
        panic!("cut")
    };
    let mut changed = bytes.clone();
    let body = inspect(&bytes)
        .rows(usize::MAX)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.key == Key::Claim(ClaimId::from_u128(3)))
        .unwrap();
    let hash = body
        .body()
        .windows(32)
        .position(|value| value == cut.fingerprint().0)
        .unwrap();
    changed[body_offset(&bytes, body.key) + hash] ^= 1;
    checksum(&mut changed);
    refused_graph(&recovered, &changed, source, &store);
    let replayed = replay(&recovered, &bytes, source, &store).unwrap();
    original.publish_native(candidate).unwrap();
    recovered.publish_native(replayed).unwrap();
    checkpoint::compare(&original, &recovered);
    assert_eq!(
        recovered
            .native_claim(ClaimId::from_u128(3))
            .unwrap()
            .status(),
        ClaimStatus::DependencyFailed
    );
}

#[test]
fn actual_timer_scc_victim_and_later_negative_expiry_roundtrip() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let mut original = f::core();
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for (id, peer) in [(1, 2), (2, 1)] {
        let NativeCommand::Create {
            claims: mut rows,
            declarations: definitions,
        } = f::creation(1, id, &[], None).command
        else {
            panic!("create")
        };
        rows[0].definition.deadline = Some(Deadline {
            timer: TimerId::from_u128(30_000 + id),
            generation: 1,
            at: 100,
        });
        rows[0].definition.graph = graph::Declaration::new(
            &[graph::Obligation {
                kind: graph::Kind::Awaits,
                target: ClaimId::from_u128(peer),
            }],
            1,
        )
        .unwrap();
        claims.extend(rows);
        declarations.extend(definitions);
    }
    f::publish(
        &mut original,
        10,
        NativeInput {
            request: f::request(f::ISSUER, 1),
            command: NativeCommand::Create {
                claims,
                declarations,
            },
        },
    );
    let mut recovered = restored(&original, 11_002, &store);
    let source = original.state.rows.id();
    let deadline = original
        .native_claim(ClaimId::from_u128(2))
        .unwrap()
        .deadline()
        .unwrap();
    let mut owner = NativeOwner::new(original).unwrap();
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare_claim_deadline(
            NativeClaimDeadlineInput {
                claim: ClaimId::from_u128(2),
                deadline,
            },
            100,
        )
        .unwrap()
    else {
        panic!("timer")
    };
    let bytes = encode_mutation(owner.prepared_candidate(candidate).unwrap());
    let events = graph_events(&bytes);
    assert!(events.iter().any(|(event, _)| matches!(
        event.fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Deadlocked,
            ..
        })
    )));
    assert!(events.iter().any(|(event, _)| matches!(
        event.fact,
        NativeFact::Claim(NativeClaimEvent {
            kind: NativeEventKind::Expired,
            ..
        })
    )));
    let replayed = replay(&recovered, &bytes, source, &store).unwrap();
    recovered.publish_native(replayed).unwrap();
    owner.publish_after_durable(candidate).unwrap();
    assert_eq!(
        recovered
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status()
    );
    assert_eq!(
        recovered
            .native_claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::Expired
    );
}

#[test]
fn actual_monitor_release_between_graph_cuts_preserves_each_original_revision() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = checkpoint::store(directory.path());
    let mut original = f::running(&[(ValidationMode::Required, false)]);
    original.limits.plan_edges = 2 * 1024 * 1024;
    original.limits.preparation_bytes = 16 * 1024 * 1024;
    dependent(&mut original, 2, 1);
    dependent(&mut original, 3, 2);
    let id = ClaimId::from_u128(2);
    let before = original.native_claim(id).unwrap().binding();
    f::publish(
        &mut original,
        50,
        NativeInput {
            request: f::request(f::ISSUER, 40_001),
            command: NativeCommand::RegisterMonitor {
                expected: before,
                receipt: None,
                id: focal_model::MonitorId::from_u128(40_001),
                roots: vec![focal_model::WaitPredicate::Terminal(ClaimId::from_u128(1))],
                deadline: Deadline {
                    timer: TimerId::from_u128(40_001),
                    generation: 1,
                    at: 1000,
                },
            },
        },
    );
    let mut recovered = restored(&original, 11_003, &store);
    let source = original.state.rows.id();
    let custody = checkpoint::budget();
    let input = f::report_for(
        &original,
        None,
        40_002,
        1,
        VerdictValue::Fail,
        f::descriptor(f::artifact_spec(40_002, f::EVALUATOR, VerdictValue::Fail)),
    );
    let proof = verify_report(&mut store, &input, &custody);
    let candidate = f::report(&original, input, &[], &proof);
    let bytes = encode_mutation(&candidate);
    let graphs = graph_events(&bytes);
    assert_eq!(graphs.len(), 2);
    for (event, _) in &graphs {
        let NativeFact::Claim(value) = event.fact else {
            panic!("claim")
        };
        assert_eq!(value.graph.unwrap().before_ordinal, event.ordinal);
    }
    let replayed = replay(&recovered, &bytes, source, &store).unwrap();
    original.publish_native(candidate).unwrap();
    recovered.publish_native(replayed).unwrap();
    checkpoint::compare(&original, &recovered);
    let claim = recovered.native_claim(id).unwrap();
    assert!(
        claim
            .scopes()
            .monitor(focal_model::MonitorId::from_u128(40_001))
            .unwrap()
            .release_cut()
            .is_some()
    );
    assert_eq!(claim.status(), ClaimStatus::DependencyFailed);
}
