use super::*;
use crate::access::{Recorder, WriteState};

fn footprint(reads: &[AccessKey], writes: &[AccessKey]) -> AccessFootprint {
    AccessFootprint {
        ledger: ledger(),
        base: SessionSeq(0),
        reads: reads.iter().copied().collect(),
        writes: writes.iter().copied().collect(),
        session_exclusive: false,
    }
}

#[test]
fn absent_rows_empty_predicates_and_structural_counts_are_tracked() {
    let mut state = Core::new(ledger(), Limits::default()).state;
    let recorder = Recorder::new(ledger(), SessionSeq(0), 100);
    let missing = ClaimId::from_u128(999);
    {
        let mut tracked = WriteState::new(&mut state, &recorder);
        assert!(tracked.claims.get(&missing).is_none());
        assert!(tracked.claims.get_mut(&missing).is_none());
        assert!(tracked.monitors.values().next().is_none());
        assert_eq!(tracked.epochs.len(), 0);
        tracked.epochs.insert(
            ISSUER,
            EpochWindow {
                minimum: RequestEpoch(1),
                admitted: BTreeSet::from([RequestEpoch(1)]),
            },
        );
        tracked.epochs.get_mut(&ISSUER).unwrap().minimum = RequestEpoch(2);
        tracked.set_sequence(SessionSeq(1));
    }
    let actual = recorder.finish();
    assert!(actual.reads.contains(&AccessKey::Claim(missing)));
    assert!(!actual.writes.contains(&AccessKey::Claim(missing)));
    assert!(
        actual
            .reads
            .contains(&AccessKey::Scan(StateTable::Monitors))
    );
    assert!(actual.reads.contains(&AccessKey::Count(StateTable::Epochs)));
    assert!(
        actual
            .writes
            .contains(&AccessKey::Count(StateTable::Epochs))
    );
    assert!(actual.writes.contains(&AccessKey::Epoch(ISSUER)));
    assert!(actual.writes.contains(&AccessKey::Sequence));
    assert_eq!(state.epochs[&ISSUER].minimum, RequestEpoch(2));
}

#[test]
fn replacement_writes_do_not_claim_a_structural_count_change() {
    let mut state = setup().state;
    let recorder = Recorder::new(ledger(), state.sequence, 100);
    WriteState::new(&mut state, &recorder).epochs.insert(
        ISSUER,
        EpochWindow {
            minimum: RequestEpoch(1),
            admitted: BTreeSet::from([RequestEpoch(1)]),
        },
    );
    let actual = recorder.finish();
    assert!(actual.reads.contains(&AccessKey::Epoch(ISSUER)));
    assert!(actual.writes.contains(&AccessKey::Epoch(ISSUER)));
    assert!(
        !actual
            .writes
            .contains(&AccessKey::Count(StateTable::Epochs))
    );
}

#[test]
fn complete_footprint_audit_catches_missing_reads_writes_and_phantoms() {
    let row = AccessKey::Claim(ClaimId::from_u128(1));
    let scan = AccessKey::Scan(StateTable::Claims);
    let actual = footprint(&[row], &[row]);
    assert!(!footprint(&[], &[row]).covers(&actual));
    assert!(!footprint(&[row], &[]).covers(&actual));
    assert!(footprint(&[scan], &[scan]).covers(&actual));
    assert!(!footprint(&[row], &[]).covers(&footprint(&[scan], &[])));
    assert!(footprint(&[scan], &[]).conflicts(&footprint(&[], &[row])));
    assert!(footprint(&[], &[row]).conflicts(&footprint(&[scan], &[])));
    assert!(!footprint(&[scan], &[]).conflicts(&footprint(&[row], &[])));
    assert!(
        !footprint(&[AccessKey::Count(StateTable::Claims)], &[]).conflicts(&footprint(&[], &[row]))
    );
    assert!(
        footprint(&[AccessKey::Count(StateTable::Claims)], &[]).conflicts(&footprint(
            &[],
            &[row, AccessKey::Count(StateTable::Claims)]
        ))
    );
    let mut other_namespace = actual.clone();
    other_namespace.ledger.session = SessionId::from_u128(123);
    assert!(!actual.conflicts(&other_namespace));
}

#[test]
fn trace_capacity_collapses_safely_without_changing_result_or_persisted_bytes() {
    let core = setup();
    let command = input(
        11,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(1),
        },
    );
    let prepared = core.prepare(&command).unwrap();
    let actual = core.prepare_tracked(&command, 10_000);
    assert!(!actual.accesses.session_exclusive);
    for cap in [0, 1, 2, 4] {
        let collapsed = core.prepare_tracked(&command, cap);
        assert_eq!(collapsed.result.unwrap(), prepared);
        assert!(collapsed.accesses.session_exclusive);
        assert!(collapsed.accesses.reads.is_empty() && collapsed.accesses.writes.is_empty());
        assert!(collapsed.accesses.covers(&actual.accesses));
        let mut expected = core.clone();
        let mut bounded = core.clone();
        let result = expected
            .apply(SessionSeq(core.sequence().0 + 1), prepared.clone())
            .unwrap();
        let tracked =
            bounded.apply_tracked(SessionSeq(core.sequence().0 + 1), prepared.clone(), cap);
        assert_eq!(tracked.result.unwrap(), result);
        assert!(tracked.accesses.session_exclusive);
        assert_eq!(
            expected.encode_checkpoint().unwrap(),
            bounded.encode_checkpoint().unwrap()
        );
    }
}

#[test]
fn rejected_and_duplicate_admission_trace_the_observed_state_without_writes() {
    let mut core = setup();
    let command = input(
        11,
        ISSUER,
        Command::PostClaim {
            claim: ClaimId::from_u128(999),
        },
    );
    let before = core.encode_checkpoint().unwrap();
    let traced = core.prepare_tracked(&command, 100);
    assert_eq!(traced.result, core.prepare(&command));
    assert!(traced.result.is_err());
    assert!(
        traced
            .accesses
            .reads
            .contains(&AccessKey::Claim(ClaimId::from_u128(999)))
    );
    assert!(traced.accesses.writes.is_empty());
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    let command = input(
        12,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(1),
        },
    );
    let accepted = apply(&mut core, command.clone());
    let duplicate = core.prepare_tracked(&command, 100);
    assert_eq!(
        duplicate.result,
        Err(DomainOutcome::Duplicate(Box::new(accepted.receipt.clone())))
    );
    assert!(
        duplicate
            .accesses
            .reads
            .contains(&AccessKey::Receipt(accepted.receipt.key))
    );
    assert!(duplicate.accesses.writes.is_empty());
}

#[test]
fn claim_creation_tracks_id_namespace_identity_adjacency_and_deadline_owner() {
    let core = setup();
    let mut claim = new_claim(1);
    claim.content.deadline = Some(Deadline {
        timer: TimerId::from_u128(500),
        generation: 1,
        at: 10_000,
    });
    let hash = claim.content.content_hash().unwrap();
    let validation = claim.validations[0].clone();
    let trace = core.prepare_tracked(&input(11, ISSUER, Command::GenerateClaim { claim }), 1000);
    trace.result.unwrap();
    for key in [
        AccessKey::Claim(ClaimId::from_u128(1)),
        AccessKey::Validation(ValidationId::from_u128(1)),
        AccessKey::Artifact(ArtifactId::from_u128(1)),
        AccessKey::Testament(TestamentId::from_u128(1)),
        AccessKey::Identity(ObjectKind::Claim, hash),
    ] {
        assert!(
            trace.accesses.reads.contains(&key),
            "missing absence/identity read {key:?}"
        );
    }
    for key in [
        AccessKey::Claim(ClaimId::from_u128(1)),
        AccessKey::Validation(validation.id),
        AccessKey::Identity(ObjectKind::Claim, hash),
        AccessKey::Identity(
            ObjectKind::Validation,
            validation.content.content_hash().unwrap(),
        ),
    ] {
        assert!(
            trace.accesses.writes.contains(&key),
            "missing object/index write {key:?}"
        );
    }
    assert!(
        trace
            .accesses
            .reads
            .contains(&AccessKey::Scan(StateTable::Claims))
    );
    assert!(
        trace
            .accesses
            .reads
            .contains(&AccessKey::Scan(StateTable::Monitors))
    );
}

#[test]
fn mutable_monitor_and_nested_evidence_fields_cannot_escape_write_tracking() {
    let mut state = setup().state;
    let id = EvidenceSetId::from_u128(1);
    // A mutable reference may change any nested fields; the whole row is written
    // before that reference leaves the table, covering all descendants.
    let recorder = Recorder::new(ledger(), state.sequence, 100);
    {
        let mut tracked = WriteState::new(&mut state, &recorder);
        tracked.evidence_sets.insert(
            id,
            EvidenceSet {
                id,
                claim: ClaimId::from_u128(1),
                receipt: ReceiptFence {
                    receipt: ReceiptId::from_u128(1),
                    epoch: 1,
                },
                artifacts: Vec::new(),
                closed: false,
            },
        );
        tracked.evidence_sets.get_mut(&id).unwrap().closed = true;
        let monitor = MonitorId::from_u128(2);
        tracked.monitors.insert(
            monitor,
            Monitor {
                id: monitor,
                owner: ClaimId::from_u128(1),
                roots: BTreeSet::new(),
                deadline: Deadline {
                    timer: TimerId::from_u128(3),
                    generation: 1,
                    at: 10_000,
                },
                registered: SessionSeq(0),
                released: None,
            },
        );
        tracked
            .monitors
            .get_mut(&monitor)
            .unwrap()
            .roots
            .insert(WaitPredicate::Terminal(ClaimId::from_u128(3)));
    }
    let trace = recorder.finish();
    assert!(trace.writes.contains(&AccessKey::EvidenceSet(id)));
    assert!(state.evidence_sets[&id].closed);
    assert!(
        trace
            .writes
            .contains(&AccessKey::Monitor(MonitorId::from_u128(2)))
    );
}

#[test]
fn staged_failure_records_writes_but_never_publishes_the_draft() {
    let mut core = setup();
    let command = input(
        11,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(1),
        },
    );
    let prepared = core.prepare(&command).unwrap();
    core.limits.max_objects = 1; // claim plus its validation exceed this bound
    let before = core.encode_checkpoint().unwrap();
    let traced = core.prepare_tracked(&command, 1000);
    assert_eq!(traced.result, core.prepare(&command));
    assert!(traced.result.is_err());
    assert!(
        traced
            .accesses
            .writes
            .contains(&AccessKey::Claim(ClaimId::from_u128(1)))
    );
    assert_eq!(core.encode_checkpoint().unwrap(), before);
    let sequence = SessionSeq(core.sequence().0 + 1);
    let applied = core.apply_tracked(sequence, prepared, 1000);
    assert!(matches!(applied.result, Err(CoreError::Determinism(_))));
    assert!(
        applied
            .accesses
            .writes
            .contains(&AccessKey::Claim(ClaimId::from_u128(1)))
    );
    assert_eq!(core.encode_checkpoint().unwrap(), before);
}

#[test]
fn optimized_audit_matches_explicit_pairwise_alias_checks() {
    let keys = [
        AccessKey::Ledger,
        AccessKey::Sequence,
        AccessKey::Limits,
        AccessKey::Scan(StateTable::Claims),
        AccessKey::Scan(StateTable::Epochs),
        AccessKey::Count(StateTable::Claims),
        AccessKey::Count(StateTable::Epochs),
        AccessKey::Claim(ClaimId::from_u128(1)),
        AccessKey::Claim(ClaimId::from_u128(2)),
        AccessKey::Epoch(ISSUER),
        AccessKey::Epoch(WORKER),
    ];
    for left in keys {
        for right in keys {
            assert_eq!(
                footprint(&[left], &[]).covers(&footprint(&[right], &[])),
                left.covers(right)
            );
            assert_eq!(
                footprint(&[], &[left]).conflicts(&footprint(&[right], &[])),
                left.overlaps(right)
            );
            assert_eq!(
                footprint(&[left], &[]).conflicts(&footprint(&[], &[right])),
                left.overlaps(right)
            );
            assert_eq!(
                footprint(&[], &[left]).conflicts(&footprint(&[], &[right])),
                left.overlaps(right)
            );
        }
    }
}
