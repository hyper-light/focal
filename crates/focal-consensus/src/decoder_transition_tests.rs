use super::tests::{activated, configured, pause, shared};
use super::*;

const BEFORE: [u8; 32] = [41; 32];
const AFTER: [u8; 32] = [42; 32];
const NEXT: [u8; 32] = [43; 32];

fn pair() -> DecoderPair {
    DecoderPair {
        predecessor: BEFORE,
        successor: AFTER,
    }
}
fn reopen(path: &Path) -> DurableNode {
    DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), path).unwrap()
}

#[test]
fn pair_registration_cannot_bypass_original_floor_or_change_its_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut node = reopen(directory.path());
    node.confirm_decoder(BEFORE).unwrap();
    node.drain().unwrap();
    node.begin_decoder_floor(BEFORE).unwrap();
    assert!(!node.try_finish_decoder_floor().unwrap());
    // Explicit additive confirmation also works while the baseline is retained.
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    assert!(matches!(
        node.begin_decoder_transition(),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(matches!(
        node.begin_decoder_floor(AFTER),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    for (a, b) in [
        (BEFORE, BEFORE),
        (AFTER, BEFORE),
        (BEFORE, NEXT),
        (NEXT, AFTER),
    ] {
        assert!(matches!(
            node.confirm_decoder_pair(a, b),
            Err(ConsensusError::DecoderMismatch)
        ));
    }
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.finish_decoder_floor().unwrap();
    assert_eq!(node.required_decoder(), Some(BEFORE));
    assert!(node.decoder_floor_ready(BEFORE));
    assert!(!node.decoder_floor_ready(AFTER));
    node.begin_decoder_transition().unwrap();
    node.begin_decoder_floor(BEFORE).unwrap();
    assert!(node.persistence_pending());
    assert!(!node.decoder_floor_ready(BEFORE));
    node.confirm_decoder(BEFORE).unwrap();
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.finish_decoder_floor().unwrap();
    assert_eq!(node.required_decoder(), Some(AFTER));
    assert!(node.decoder_floor_ready(BEFORE));
    assert!(node.decoder_floor_ready(AFTER));
    assert!(!node.decoder_floor_ready(NEXT));
    node.begin_decoder_floor(BEFORE).unwrap();
    node.begin_decoder_transition().unwrap();
    assert!(!node.persistence_pending());
    assert!(matches!(
        node.begin_decoder_floor(AFTER),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.confirm_decoder(AFTER),
        Err(ConsensusError::DecoderMismatch)
    ));

    let fresh = tempfile::tempdir().unwrap();
    let mut node = reopen(fresh.path());
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.drain().unwrap();
    assert!(matches!(
        node.begin_decoder_transition(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.begin_decoder_floor(AFTER),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert_eq!(node.required_decoder(), None);
}

#[test]
fn transitioned_recovery_requires_the_complete_pair_before_any_participation() {
    let directory = tempfile::tempdir().unwrap();
    let mut node = activated(directory.path());
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.begin_decoder_transition().unwrap();
    node.finish_decoder_floor().unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"unchanged-application-entry".to_vec())
        .unwrap();
    let committed = node.drain().unwrap().committed;
    drop(node);
    let mut node = reopen(directory.path());
    assert_eq!(node.required_decoder(), Some(AFTER));
    for hash in [BEFORE, AFTER, NEXT] {
        assert!(matches!(
            node.confirm_decoder(hash),
            Err(ConsensusError::DecoderMismatch)
        ));
        assert!(!node.decoder_floor_ready(hash));
    }
    for (a, b) in [(BEFORE, NEXT), (AFTER, BEFORE), (AFTER, NEXT)] {
        assert!(matches!(
            node.confirm_decoder_pair(a, b),
            Err(ConsensusError::DecoderMismatch)
        ));
    }
    assert!(matches!(
        node.campaign(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.tick(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.propose(vec![1]),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.read_index(vec![1]),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.try_drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        node.begin_checkpoint(1, vec![]),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    let term = node.status().term;
    let mut vote = Message {
        from: 2,
        to: 1,
        term: 99,
        ..Default::default()
    };
    vote.set_msg_type(MessageType::MsgRequestVote);
    assert!(matches!(
        node.step(vote),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert_eq!(node.status().term, term);
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    assert!(node.decoder_floor_ready(BEFORE));
    assert!(node.decoder_floor_ready(AFTER));
    let replay = node.drain().unwrap();
    assert_eq!(replay.committed, committed);
    assert!(replay.messages.is_empty());
}

#[test]
fn transition_waits_for_actual_fsync_under_pressure_and_retries_cannot_replace_it() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal = shared(directory.path(), &budget);
    let mut node = configured(&wal, &budget, 2);
    node.begin_decoder_floor(BEFORE).unwrap();
    node.finish_decoder_floor().unwrap();
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.campaign().unwrap();
    let index = node.drain().unwrap().applied_index;
    let paused = pause(&wal);
    let stats = budget.stats();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap();
    node.begin_decoder_transition().unwrap();
    assert!(!node.try_finish_decoder_floor().unwrap());
    let retained = budget.stats().used;
    for _ in 0..8 {
        node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
        node.begin_decoder_transition().unwrap();
        node.begin_decoder_floor(BEFORE).unwrap();
        assert!(matches!(
            node.confirm_decoder_pair(AFTER, BEFORE),
            Err(ConsensusError::DecoderMismatch)
        ));
        assert!(matches!(
            node.confirm_decoder_pair(BEFORE, NEXT),
            Err(ConsensusError::DecoderMismatch)
        ));
        assert!(matches!(
            node.confirm_decoder(AFTER),
            Err(ConsensusError::DecoderMismatch)
        ));
        assert!(!node.try_finish_decoder_floor().unwrap());
        assert!(node.try_drain().unwrap().is_none());
        assert_eq!(node.required_decoder(), Some(BEFORE));
        assert!(!node.decoder_floor_ready(BEFORE));
        assert!(!node.decoder_floor_ready(AFTER));
        assert_eq!(budget.stats().used, retained);
    }
    assert!(matches!(
        node.campaign(),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(matches!(
        node.begin_checkpoint(index, vec![1]),
        Err(ConsensusError::PersistencePending)
    ));
    paused.resume().unwrap();
    node.finish_decoder_floor().unwrap();
    assert!(node.decoder_floor_ready(BEFORE));
    assert!(node.decoder_floor_ready(AFTER));
    let writes = wal.stats().unwrap().appended_records;
    node.begin_decoder_floor(BEFORE).unwrap();
    node.begin_decoder_transition().unwrap();
    node.finish_decoder_floor().unwrap();
    assert_eq!(wal.stats().unwrap().appended_records, writes);
    drop(pressure);
}

#[test]
fn abandoned_transition_and_both_group_checkpoints_preserve_original_order_and_tail() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal = shared(directory.path(), &budget);
    let mut node = configured(&wal, &budget, 2);
    let mut other = configured(&wal, &budget, 3);
    node.begin_decoder_floor(BEFORE).unwrap();
    node.finish_decoder_floor().unwrap();
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    let paused = pause(&wal);
    node.begin_decoder_transition().unwrap();
    assert!(!node.try_finish_decoder_floor().unwrap());
    drop(node);
    paused.resume().unwrap();
    wal.stats().unwrap();
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [2; 16]),
        wal.clone(),
        &budget,
    )
    .unwrap();
    assert_eq!(node.required_decoder(), Some(AFTER));
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.drain().unwrap();
    node.campaign().unwrap();
    let index = node.drain().unwrap().applied_index;
    node.checkpoint(index, b"original-app-snapshot".to_vec())
        .unwrap();
    node.propose(b"unchanged-tail".to_vec()).unwrap();
    let tail = node.drain().unwrap().committed;
    other.campaign().unwrap();
    let index = other.drain().unwrap().applied_index;
    other.checkpoint(index, b"other-snapshot".to_vec()).unwrap();
    drop(node);
    drop(other);
    let lease = wal.lease(LogicalLogId([2; 16])).unwrap();
    let mut records = Vec::new();
    lease
        .replay(|record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
    assert_eq!(records[0].kind, RecordKind::Identity);
    assert_eq!(records[1], floor_record([2; 16], BEFORE).unwrap());
    assert_eq!(records[2], transition_record([2; 16], pair()).unwrap());
    assert_eq!(records[3].kind, RecordKind::Snapshot);
    assert_eq!(
        records
            .iter()
            .filter(|r| r.kind == RecordKind::DecoderFloor)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| r.kind == RecordKind::DecoderTransition)
            .count(),
        1
    );
    drop(lease);
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [2; 16]),
        wal.clone(),
        &budget,
    )
    .unwrap();
    assert!(matches!(
        node.confirm_decoder(AFTER),
        Err(ConsensusError::DecoderMismatch)
    ));
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    let replay = node.drain().unwrap();
    assert_eq!(replay.snapshot.unwrap().data, b"original-app-snapshot");
    assert_eq!(replay.committed, tail);
    let mut other =
        DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [3; 16]), wal, &budget).unwrap();
    assert_eq!(other.required_decoder(), None);
    assert_eq!(
        other.drain().unwrap().snapshot.unwrap().data,
        b"other-snapshot"
    );
}

#[test]
fn ambiguous_transition_failure_never_advertises_and_recovery_obeys_actual_fence() {
    for point in [FaultPoint::AfterDataSync, FaultPoint::AfterFenceInstall] {
        let directory = tempfile::tempdir().unwrap();
        let mut node = activated(directory.path());
        node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
        node.inject_fault_once(point);
        node.begin_decoder_transition().unwrap();
        assert!(node.finish_decoder_floor().is_err());
        assert!(!node.decoder_floor_ready(BEFORE));
        assert!(!node.decoder_floor_ready(AFTER));
        assert!(matches!(node.campaign(), Err(ConsensusError::Failed)));
        drop(node);
        let mut node = reopen(directory.path());
        let transitioned = point == FaultPoint::AfterFenceInstall;
        assert_eq!(
            node.required_decoder(),
            Some(if transitioned { AFTER } else { BEFORE })
        );
        if transitioned {
            assert!(matches!(
                node.confirm_decoder(BEFORE),
                Err(ConsensusError::DecoderMismatch)
            ));
        } else {
            node.confirm_decoder(BEFORE).unwrap();
            assert!(node.decoder_floor_ready(BEFORE));
        }
        node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
        node.drain().unwrap();
        node.begin_decoder_transition().unwrap();
        node.finish_decoder_floor().unwrap();
        assert!(node.decoder_floor_ready(AFTER));
    }
}

#[test]
fn transition_envelope_is_exact_and_rejects_every_malformed_boundary() {
    let record = transition_record([2; 16], pair()).unwrap();
    let mut expected = b"FOCALDT1".to_vec();
    expected.extend_from_slice(&[0, 1]);
    expected.extend_from_slice(&BEFORE);
    expected.extend_from_slice(&AFTER);
    assert_eq!(record.payload, expected);
    assert_eq!(record.payload.len(), 74);
    assert_eq!(decode_transition(&record).unwrap(), pair());
    for length in 0..74 {
        let mut malformed = record.clone();
        malformed.payload.truncate(length);
        assert!(decode_transition(&malformed).is_err());
    }
    for offset in [0, 7, 8, 9] {
        let mut malformed = record.clone();
        malformed.payload[offset] ^= 1;
        assert!(decode_transition(&malformed).is_err());
    }
    let mut malformed = record.clone();
    malformed.payload.push(0);
    assert!(decode_transition(&malformed).is_err());
    malformed = record.clone();
    malformed.index = 1;
    assert!(decode_transition(&malformed).is_err());
    malformed = record.clone();
    malformed.term = 1;
    assert!(decode_transition(&malformed).is_err());
    malformed = record;
    malformed.payload[42..74].copy_from_slice(&BEFORE);
    assert!(decode_transition(&malformed).is_err());
    assert!(
        transition_record(
            [2; 16],
            DecoderPair {
                predecessor: BEFORE,
                successor: BEFORE
            }
        )
        .is_err()
    );
    // Hashes are opaque identities, with no zero prohibition or numeric order.
    let opaque = DecoderPair {
        predecessor: [255; 32],
        successor: [0; 32],
    };
    assert_eq!(
        decode_transition(&transition_record([2; 16], opaque).unwrap()).unwrap(),
        opaque
    );
}

#[test]
fn replay_rejects_unbound_duplicate_reversed_and_second_transitions() {
    for scenario in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let wal = SharedWal::open(
            directory.path(),
            WalOptions::new(WalIdentity {
                node: 1,
                cluster: [1; 16],
                stream: 0,
            }),
        )
        .unwrap();
        let mut records = Vec::new();
        if scenario != 0 {
            records.push(identity_record(&NodeConfig::single(1, [1; 16], [2; 16])).unwrap());
        }
        if scenario >= 2 {
            records.push(floor_record([2; 16], BEFORE).unwrap());
        }
        if scenario == 2 {
            records.push(
                transition_record(
                    [2; 16],
                    DecoderPair {
                        predecessor: AFTER,
                        successor: BEFORE,
                    },
                )
                .unwrap(),
            );
        } else {
            records.push(transition_record([2; 16], pair()).unwrap());
        }
        match scenario {
            3 => records.push(transition_record([2; 16], pair()).unwrap()),
            4 => records.push(
                transition_record(
                    [2; 16],
                    DecoderPair {
                        predecessor: AFTER,
                        successor: NEXT,
                    },
                )
                .unwrap(),
            ),
            5 => records.push(floor_record([2; 16], AFTER).unwrap()),
            _ => {}
        }
        let mut lease = wal.lease(LogicalLogId([2; 16])).unwrap();
        lease.append_in(&records, BudgetLane::Completion).unwrap();
        drop(lease);
        drop(wal);
        assert!(
            matches!(
                DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()),
                Err(ConsensusError::Corruption(_))
            ),
            "scenario {scenario}"
        );
    }
}

#[test]
fn transition_admission_cannot_overlap_ready_or_checkpoint_work() {
    let directory = tempfile::tempdir().unwrap();
    let mut node = activated(directory.path());
    assert!(matches!(
        node.begin_decoder_transition(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    node.confirm_decoder_pair(BEFORE, AFTER).unwrap();
    node.campaign().unwrap();
    assert!(matches!(
        node.begin_decoder_transition(),
        Err(ConsensusError::PersistencePending)
    ));
    let index = node.drain().unwrap().applied_index;
    node.begin_checkpoint(index, b"still-predecessor".to_vec())
        .unwrap();
    assert!(matches!(
        node.begin_decoder_transition(),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(node.cancel_unadmitted_checkpoint());
    node.begin_decoder_transition().unwrap();
    assert!(matches!(
        node.begin_checkpoint(index, vec![]),
        Err(ConsensusError::PersistencePending)
    ));
    node.finish_decoder_floor().unwrap();
    node.checkpoint(index, b"unchanged-after-transition".to_vec())
        .unwrap();
}
