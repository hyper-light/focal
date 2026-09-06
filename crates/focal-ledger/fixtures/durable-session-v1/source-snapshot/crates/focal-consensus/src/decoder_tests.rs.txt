use super::*;
use std::sync::mpsc;

const HASH: [u8; 32] = [41; 32];

fn activated(path: &Path) -> DurableNode {
    let mut node = DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), path).unwrap();
    node.confirm_decoder(HASH).unwrap();
    node.drain().unwrap();
    node.begin_decoder_floor(HASH).unwrap();
    assert!(!node.decoder_floor_ready(HASH));
    node.finish_decoder_floor().unwrap();
    node
}

#[test]
fn decoder_floor_requires_actual_application_confirmation_before_any_raft_participation() {
    let directory = tempfile::tempdir().unwrap();
    let mut node = activated(directory.path());
    assert!(node.decoder_floor_ready(HASH));
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"after-floor".to_vec()).unwrap();
    let original = node.drain().unwrap();
    drop(node);
    let mut reopened =
        DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()).unwrap();
    assert_eq!(reopened.required_decoder(), Some(HASH));
    assert!(!reopened.decoder_floor_ready(HASH));
    assert!(matches!(
        reopened.confirm_decoder([42; 32]),
        Err(ConsensusError::DecoderMismatch)
    ));
    assert!(matches!(
        reopened.campaign(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        reopened.tick(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        reopened.propose(vec![1]),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        reopened.read_index(vec![1]),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        reopened.drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert!(matches!(
        reopened.try_drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    let mut vote = Message {
        from: 2,
        to: 1,
        term: 99,
        ..Default::default()
    };
    vote.set_msg_type(MessageType::MsgRequestVote);
    let term = reopened.status().term;
    assert!(matches!(
        reopened.step(vote),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    assert_eq!(reopened.status().term, term);
    reopened.confirm_decoder(HASH).unwrap();
    assert!(reopened.decoder_floor_ready(HASH));
    let replay = reopened.drain().unwrap();
    assert_eq!(replay.committed, original.committed);
    assert!(replay.messages.is_empty());
    reopened.campaign().unwrap();
    reopened.drain().unwrap();
}

fn shared(path: &Path, budget: &MemoryBudget) -> SharedWal {
    SharedWal::open_with_budget(
        path,
        WalOptions::new(WalIdentity {
            node: 1,
            cluster: [1; 16],
            stream: 0,
        }),
        focal_log::WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap()
}
fn configured(shared: &SharedWal, budget: &MemoryBudget, group: u8) -> DurableNode {
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [group; 16]),
        shared.clone(),
        budget,
    )
    .unwrap();
    node.confirm_decoder(HASH).unwrap();
    node.drain().unwrap();
    node
}
/// A real bounded replay channel holds the existing disk owner. No timer or
/// scheduler race substitutes for proving that the floor has not been fsynced.
fn pause(shared: &SharedWal) -> (mpsc::SyncSender<()>, std::thread::JoinHandle<WalLease>) {
    let mut lease = shared.lease(LogicalLogId([250; 16])).unwrap();
    let records: Vec<_> = (1..=3)
        .map(|index| Record {
            log: LogicalLogId([250; 16]),
            kind: RecordKind::Entry,
            index,
            term: 1,
            payload: vec![1],
        })
        .collect();
    lease.append_in(&records, BudgetLane::Completion).unwrap();
    let (entered, entry) = mpsc::sync_channel(1);
    let (resume, resumed) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let mut first = true;
        lease
            .replay(|_| {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    resumed.recv().unwrap();
                }
                Ok(())
            })
            .unwrap();
        lease
    });
    entry.recv().unwrap();
    (resume, worker)
}

#[test]
fn floor_advertisement_waits_for_real_fsync_and_uses_completion_under_ordinary_pressure() {
    let directory = tempfile::tempdir().unwrap();
    // The default WAL checkpoint reserves three maximum-size (16MiB) record
    // buffers. Leave that explicit completion workspace available while fully
    // exhausting ordinary admission.
    let budget = MemoryBudget::new(128 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
    let wal = shared(directory.path(), &budget);
    let mut node = configured(&wal, &budget, 2);
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"committed-before-floor".to_vec()).unwrap();
    let index = node.drain().unwrap().applied_index;
    let (resume, worker) = pause(&wal);
    let stats = budget.stats();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap();
    assert!(
        budget
            .reserve(BudgetKind::Payload, BudgetLane::Ordinary, 1)
            .is_err()
    );
    node.begin_decoder_floor(HASH).unwrap();
    assert!(node.persistence_pending());
    assert!(!node.try_finish_decoder_floor().unwrap());
    assert_eq!(node.required_decoder(), None);
    let retained = budget.stats().used;
    for _ in 0..8 {
        assert!(!node.try_finish_decoder_floor().unwrap());
        assert!(node.try_drain().unwrap().is_none());
        assert!(!node.decoder_floor_ready(HASH));
        assert_eq!(budget.stats().used, retained);
    }
    assert!(matches!(
        node.campaign(),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(matches!(
        node.begin_checkpoint(index, vec![2]),
        Err(ConsensusError::PersistencePending)
    ));
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    node.finish_decoder_floor().unwrap();
    assert!(node.decoder_floor_ready(HASH));
    node.checkpoint(index, b"checkpoint-retains-floor".to_vec())
        .unwrap();
    drop(pressure);
    drop(node);
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [2; 16]),
        wal.clone(),
        &budget,
    )
    .unwrap();
    assert_eq!(node.required_decoder(), Some(HASH));
    assert!(matches!(
        node.drain(),
        Err(ConsensusError::DecoderUnconfirmed)
    ));
    node.confirm_decoder(HASH).unwrap();
    let events = node.drain().unwrap();
    assert_eq!(events.snapshot.unwrap().data, b"checkpoint-retains-floor");
    assert!(events.committed.is_empty());
}

#[test]
fn lost_floor_waiter_reopens_with_requirement_and_checkpoint_tail_keeps_exact_scope() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let wal = shared(directory.path(), &budget);
    let mut node = configured(&wal, &budget, 2);
    let mut other = configured(&wal, &budget, 3);
    other.campaign().unwrap();
    other.drain().unwrap();
    other.propose(b"other-group".to_vec()).unwrap();
    other.drain().unwrap();
    let (resume, worker) = pause(&wal);
    node.begin_decoder_floor(HASH).unwrap();
    assert!(!node.try_finish_decoder_floor().unwrap());
    drop(node); // An admitted append continues after the application waiter is gone.
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    wal.stats().unwrap(); // FIFO barrier proves the abandoned write has finished.
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [2; 16]),
        wal.clone(),
        &budget,
    )
    .unwrap();
    assert_eq!(node.required_decoder(), Some(HASH));
    node.confirm_decoder(HASH).unwrap();
    node.drain().unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose(b"before-snapshot".to_vec()).unwrap();
    let index = node.drain().unwrap().applied_index;
    node.checkpoint(index, b"snapshot".to_vec()).unwrap();
    node.propose(b"tail".to_vec()).unwrap();
    let tail = node.drain().unwrap().committed;
    let writes = wal.stats().unwrap().appended_records;
    node.begin_decoder_floor(HASH).unwrap();
    node.finish_decoder_floor().unwrap();
    assert_eq!(
        wal.stats().unwrap().appended_records,
        writes,
        "exact floor retry cannot append"
    );
    drop(node);
    drop(other);
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [2; 16]),
        wal.clone(),
        &budget,
    )
    .unwrap();
    node.confirm_decoder(HASH).unwrap();
    let events = node.drain().unwrap();
    assert_eq!(events.snapshot.unwrap().data, b"snapshot");
    assert_eq!(events.committed, tail);
    let mut other =
        DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [3; 16]), wal, &budget).unwrap();
    assert_eq!(
        other.required_decoder(),
        None,
        "floor is scoped to the logical group"
    );
    assert_eq!(other.drain().unwrap().committed[0].data, b"other-group");
}

#[test]
fn ambiguous_floor_fsync_never_advertises_and_recovery_uses_the_actual_fence() {
    for point in [FaultPoint::AfterDataSync, FaultPoint::AfterFenceInstall] {
        let directory = tempfile::tempdir().unwrap();
        let mut node =
            DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()).unwrap();
        node.confirm_decoder(HASH).unwrap();
        node.drain().unwrap();
        node.inject_fault_once(point);
        node.begin_decoder_floor(HASH).unwrap();
        assert!(node.finish_decoder_floor().is_err());
        assert!(!node.decoder_floor_ready(HASH));
        assert!(matches!(node.campaign(), Err(ConsensusError::Failed)));
        drop(node);
        let mut recovered =
            DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()).unwrap();
        assert_eq!(
            recovered.required_decoder(),
            (point == FaultPoint::AfterFenceInstall).then_some(HASH)
        );
        if point == FaultPoint::AfterFenceInstall {
            assert!(matches!(
                recovered.drain(),
                Err(ConsensusError::DecoderUnconfirmed)
            ));
        }
        recovered.confirm_decoder(HASH).unwrap();
        recovered.drain().unwrap();
        recovered.begin_decoder_floor(HASH).unwrap();
        recovered.finish_decoder_floor().unwrap();
        assert!(recovered.decoder_floor_ready(HASH));
    }
}

#[test]
fn floor_and_checkpoint_admission_never_overlap_and_malformed_floor_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let mut node =
        DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()).unwrap();
    node.confirm_decoder(HASH).unwrap();
    node.drain().unwrap();
    node.campaign().unwrap();
    assert!(matches!(
        node.begin_decoder_floor(HASH),
        Err(ConsensusError::PersistencePending)
    ));
    let index = node.drain().unwrap().applied_index;
    node.begin_checkpoint(index, b"no-floor-yet".to_vec())
        .unwrap();
    assert!(matches!(
        node.begin_decoder_floor(HASH),
        Err(ConsensusError::PersistencePending)
    ));
    assert!(node.cancel_unadmitted_checkpoint());
    node.begin_decoder_floor(HASH).unwrap();
    node.finish_decoder_floor().unwrap();
    assert!(matches!(
        node.confirm_decoder([42; 32]),
        Err(ConsensusError::DecoderMismatch)
    ));
    assert!(node.decoder_floor_ready(HASH));
    let mut record = floor_record([2; 16], HASH).unwrap();
    assert_eq!(decode_floor(&record).unwrap(), HASH);
    record.term = 1;
    assert!(decode_floor(&record).is_err());
    record.term = 0;
    record.payload.push(0);
    assert!(decode_floor(&record).is_err());
    drop(node);
    let wal = SharedWal::open(
        directory.path(),
        WalOptions::new(WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        }),
    )
    .unwrap();
    let mut lease = wal.lease(LogicalLogId([2; 16])).unwrap();
    lease
        .append_in(
            &[floor_record([2; 16], HASH).unwrap()],
            BudgetLane::Completion,
        )
        .unwrap();
    drop(lease);
    drop(wal);
    assert!(matches!(
        DurableNode::open(NodeConfig::single(1, [1; 16], [2; 16]), directory.path()),
        Err(ConsensusError::Corruption(
            "duplicate or unbound decoder floor"
        ))
    ));
}
