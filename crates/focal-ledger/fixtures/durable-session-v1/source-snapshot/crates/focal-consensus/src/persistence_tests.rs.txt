use super::*;
use std::sync::mpsc;

fn shared(dir: &std::path::Path) -> SharedWal {
    SharedWal::open(
        dir,
        WalOptions::new(WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        }),
    )
    .unwrap()
}
fn blocker(wal: &SharedWal) -> WalLease {
    let mut lease = wal.lease(LogicalLogId([250; 16])).unwrap();
    let records: Vec<_> = (1..=3)
        .map(|index| Record {
            log: LogicalLogId([250; 16]),
            kind: RecordKind::Entry,
            index,
            term: 1,
            payload: vec![1],
        })
        .collect();
    lease.append(&records).unwrap();
    lease
}
/// The real bounded replay channel stops the disk owner behind this callback;
/// no test-only disk API, sleeps or timing assumption controls the write batch.
fn pause(lease: WalLease) -> (mpsc::SyncSender<()>, std::thread::JoinHandle<WalLease>) {
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
fn node(wal: &SharedWal, group: u8, budget: &MemoryBudget) -> DurableNode {
    let mut node = DurableNode::open_on_wal_in(
        NodeConfig::single(1, [1; 16], [group; 16]),
        wal.clone(),
        budget,
    )
    .unwrap();
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    node
}

#[test]
fn queue_pressure_retains_ready_and_light_admission_without_clearing_authority() {
    for light in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let wal = SharedWal::open_with_budget(
            dir.path(),
            WalOptions::new(WalIdentity {
                cluster: [1; 16],
                node: 1,
                stream: 0,
            }),
            focal_log::WalWriterLimits {
                queue_items: 1,
                ..Default::default()
            },
            MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
        let mut node = node(&wal, 1, &budget);
        let lease = blocker(&wal);
        let mut filler = wal.lease(LogicalLogId([249; 16])).unwrap();
        node.propose(vec![7; 4096]).unwrap();
        if light {
            assert!(node.try_drain().unwrap().is_none());
            wal.stats().unwrap(); // Exact FIFO barrier after the first Ready fence.
        }
        let (resume, worker) = pause(lease);
        let mut tickets = Vec::new();
        for index in 1..=8 {
            let record = Record {
                log: LogicalLogId([249; 16]),
                kind: RecordKind::Entry,
                index,
                term: 1,
                payload: vec![1],
            };
            match filler.append_async_in(&[record], BudgetLane::Completion) {
                Ok(ticket) => tickets.push(ticket),
                Err(focal_log::LogError::Capacity) => break,
                Err(error) => panic!("unexpected filler failure: {error}"),
            }
        }
        assert!(!tickets.is_empty());
        assert!(node.try_drain().unwrap().is_none());
        assert!(node.persistence_pending());
        let used = budget.stats().used;
        for _ in 0..8 {
            assert!(node.try_drain().unwrap().is_none());
            assert_eq!(budget.stats().used, used);
        }
        assert!(matches!(
            node.drain(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(!node.failed);
        assert!(matches!(
            node.tick(),
            Err(ConsensusError::PersistencePending)
        ));
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        for mut ticket in tickets {
            ticket.wait_blocking().unwrap();
        }
        let events = node.drain().unwrap();
        assert_eq!(events.committed.len(), 1);
        assert_eq!(events.committed[0].data, vec![7; 4096]);
        assert!(!node.persistence_pending());
        drop(events);
        drop(node);
        assert_eq!(budget.stats().used, 0);
    }
}
#[test]
fn single_owner_queues_many_groups_into_one_covering_flush() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
    let mut nodes: Vec<_> = (1..=12).map(|group| node(&wal, group, &budget)).collect();
    let lease = blocker(&wal);
    let before = wal.stats().unwrap();
    for (index, node) in nodes.iter_mut().enumerate() {
        node.propose(vec![index as u8; 4096]).unwrap();
    }
    let (resume, worker) = pause(lease);
    for node in &mut nodes {
        assert!(node.try_drain().unwrap().is_none());
    }
    for node in &mut nodes {
        assert!(node.persistence_pending());
        assert!(node.try_drain().unwrap().is_none());
        assert!(!node.has_committed_current_term());
        assert!(matches!(
            node.tick(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.propose(vec![9]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.read_index(vec![1]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.checkpoint(node.status().applied_index, vec![1]),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.campaign(),
            Err(ConsensusError::PersistencePending)
        ));
        assert!(matches!(
            node.step(Message::default()),
            Err(ConsensusError::PersistencePending)
        ));
    }
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    let after_ready = wal.stats().unwrap();
    assert_eq!(after_ready.group_commits - before.group_commits, 1);
    // A Ready receipt alone cannot release the single-voter committed entry:
    // each group must first queue and persist its LightReady hard-state update.
    for node in &mut nodes {
        assert!(node.try_drain().unwrap().is_none());
    }
    for (index, node) in nodes.iter_mut().enumerate() {
        let events = node.drain().unwrap();
        assert_eq!(events.committed.len(), 1);
        assert_eq!(events.committed[0].data, vec![index as u8; 4096]);
        assert!(!node.persistence_pending());
        assert!(node.has_committed_current_term());
        assert!(node.drain().unwrap().committed.is_empty());
    }
    drop(nodes);
    assert_eq!(budget.stats().used, 0);
}
#[test]
fn light_ready_failure_never_releases_success_and_recovery_uses_its_exact_fence() {
    for fault in [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let wal = shared(dir.path());
        let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
        let mut node = node(&wal, 1, &budget);
        let published = node.status().applied_index;
        let lease = blocker(&wal);
        let (resume, worker) = pause(lease);
        node.propose(vec![42; 8192]).unwrap();
        assert!(node.try_drain().unwrap().is_none());
        assert_eq!(node.status().applied_index, published);
        resume.send(()).unwrap();
        drop(worker.join().unwrap());
        // A synchronous actor command is a FIFO barrier behind the initial
        // Ready write and installs the cut specifically for the LightReady.
        node.inject_fault_once(fault);
        assert!(node.try_drain().unwrap().is_none());
        assert_eq!(node.status().applied_index, published);
        assert!(!node.has_committed_current_term());
        assert!(node.drain().is_err());
        assert_eq!(node.status().applied_index, published);
        assert!(matches!(node.tick(), Err(ConsensusError::Failed)));
        drop(node);
        drop(wal);
        assert_eq!(budget.stats().used, 0);
        let wal = shared(dir.path());
        let mut node =
            DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [1; 16]), wal, &budget)
                .unwrap();
        let recovered = node.drain().unwrap();
        if fault == FaultPoint::AfterFenceInstall {
            assert_eq!(recovered.committed[0].data, vec![42; 8192]);
        } else {
            assert!(recovered.committed.is_empty());
        }
    }
}
#[test]
fn dropping_pending_owner_releases_ram_but_cannot_cancel_an_admitted_write() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(64 * 1024 * 1024, 16 * 1024 * 1024).unwrap();
    let mut node = node(&wal, 1, &budget);
    let lease = blocker(&wal);
    let (resume, worker) = pause(lease);
    node.propose(vec![17; 32 * 1024]).unwrap();
    assert!(node.try_drain().unwrap().is_none());
    assert!(budget.stats().used > 32 * 1024);
    drop(node);
    assert_eq!(budget.stats().used, 0);
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    let mut node =
        DurableNode::open_on_wal_in(NodeConfig::single(1, [1; 16], [1; 16]), wal, &budget).unwrap();
    assert!(node.drain().unwrap().committed.is_empty());
    node.campaign().unwrap();
    let recovered = node.drain().unwrap();
    assert_eq!(recovered.committed.len(), 1);
    assert_eq!(recovered.committed[0].data, vec![17; 32 * 1024]);
    assert!(node.drain().unwrap().committed.is_empty());
}

#[test]
fn an_outstanding_ready_keeps_reserved_group_memory_through_publication() {
    let dir = tempfile::tempdir().unwrap();
    let wal = shared(dir.path());
    let budget = MemoryBudget::new(32 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut node = node(&wal, 1, &budget);
    let lease = blocker(&wal);
    let (resume, worker) = pause(lease);
    node.propose(vec![19; 64 * 1024]).unwrap();
    assert!(node.try_drain().unwrap().is_none());
    let pressure = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap()
        .commit();
    assert!(node.try_drain().unwrap().is_none());
    resume.send(()).unwrap();
    drop(worker.join().unwrap());
    // The group reuses its admitted staging permit; its separate physical WAL
    // remains responsible for disk queue admission, including LightReady.
    let mut events = node.drain().unwrap();
    assert_eq!(events.committed[0].data, vec![19; 64 * 1024]);
    let permit = events.take_allocation().unwrap();
    drop(pressure);
    drop(node);
    assert_eq!(budget.stats().used, permit.bytes());
    drop(events);
    drop(permit);
    assert_eq!(budget.stats().used, 0);
}
