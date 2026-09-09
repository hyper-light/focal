use super::*;

#[test]
fn borrowed_record_survives_capacity_refusal_then_commits_exact_original_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 << 20, 32 << 20).unwrap();
    let config = NodeConfig::single(1, [76; 16], [77; 16]);
    let mut node = DurableNode::open_in(config.clone(), directory.path(), &budget).unwrap();
    node.campaign().unwrap();
    drop(node.drain().unwrap());

    // The application retains and funds its record independently of Raft.
    let source_charge = budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 1024)
        .unwrap();
    let mut record = vec![42; 1024];
    let address = record.as_ptr();
    let term = node.status().term;
    let commit = node.status().committed_index;
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let full = budget.stats();
    assert!(matches!(
        node.propose_borrowed_in(&record, BudgetLane::Completion),
        Err(ConsensusError::Capacity)
    ));
    assert_eq!(budget.stats(), full);
    assert_eq!(record.as_ptr(), address);
    assert_eq!(record, vec![42; 1024]);
    assert_eq!(node.status().term, term);
    assert_eq!(node.status().committed_index, commit);
    drop(pressure);

    node.propose_borrowed_in(&record, BudgetLane::Completion)
        .unwrap();
    // Admission copies only after its own charge. Mutating a caller's unrelated
    // scratch after the borrow ends cannot alter the accepted Raft proposal.
    record.fill(99);
    let events = node.drain().unwrap();
    assert_eq!(events.committed.len(), 1);
    assert_eq!(events.committed[0].data, vec![42; 1024]);
    let committed = events.committed[0].clone();
    assert!(committed.index > commit);
    assert_eq!(committed.term, term);
    drop(events);
    drop(record);
    drop(source_charge);
    drop(node);
    assert_eq!(budget.stats().used, 0);

    let mut node = DurableNode::open_in(config, directory.path(), &budget).unwrap();
    let events = node.drain().unwrap();
    assert_eq!(events.committed, vec![committed]);
    drop(events);
    drop(node);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn borrowed_record_refuses_wrong_authority_and_invalid_sizes_without_new_raft_charge() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 << 20, 32 << 20).unwrap();
    let mut config = NodeConfig::single(1, [78; 16], [79; 16]);
    config.max_entry_bytes = 1024;
    let mut node = DurableNode::open_in(config, directory.path(), &budget).unwrap();
    let before = budget.stats();
    assert!(matches!(
        node.propose_borrowed_in(b"retained", BudgetLane::Completion),
        Err(ConsensusError::NotLeader { .. })
    ));
    assert_eq!(budget.stats(), before);
    node.campaign().unwrap();
    drop(node.drain().unwrap());
    let before = budget.stats();
    for value in [&[][..], &[1; 1025][..]] {
        assert!(matches!(
            node.propose_borrowed_in(value, BudgetLane::Completion),
            Err(ConsensusError::Capacity)
        ));
        assert_eq!(budget.stats(), before);
    }
    assert!(node.drain().unwrap().committed.is_empty());
}

#[test]
fn funded_checkpoint_transfers_source_lifetime_without_losing_durable_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let budget = MemoryBudget::new(128 << 20, 32 << 20).unwrap();
    let config = NodeConfig::single(1, [80; 16], [81; 16]);
    let mut node = DurableNode::open_in(config.clone(), directory.path(), &budget).unwrap();
    node.campaign().unwrap();
    node.drain().unwrap();
    node.propose_borrowed_in(b"before", BudgetLane::Completion)
        .unwrap();
    let index = node.drain().unwrap().applied_index;
    let old = budget.stats();
    let insufficient = budget
        .reserve(BudgetKind::Recovery, BudgetLane::Completion, 1)
        .unwrap()
        .commit();
    assert!(matches!(
        node.begin_checkpoint_funded(index, vec![7; 1024], insufficient),
        Err(ConsensusError::Capacity)
    ));
    assert_eq!(budget.stats(), old);
    assert!(!node.checkpoint_pending());

    let allocation = budget
        .reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            1024 + 4 * size_of::<usize>(),
        )
        .unwrap()
        .commit();
    let before = budget.stats().by_kind[BudgetKind::Recovery as usize];
    node.begin_checkpoint_funded(index, vec![8; 1024], allocation)
        .unwrap();
    assert!(node.checkpoint_pending());
    // Staging for the moved snapshot, retained entries and hard state is held
    // under consensus Pending only while the checkpoint is in flight.
    let pending_used = budget.stats().used;
    assert!(pending_used > old.used);
    // The original input was consumed and destroyed during preparation. Its
    // now separately owned snapshot/records are covered by consensus Pending.
    assert_eq!(
        budget.stats().by_kind[BudgetKind::Recovery as usize],
        before - 1024 - 4 * size_of::<usize>()
    );
    node.finish_checkpoint().unwrap();
    let finished_used = budget.stats().used;
    assert!(
        finished_used < pending_used,
        "staging released after finish"
    );
    assert!(!node.checkpoint_pending());
    node.propose_borrowed_in(b"after", BudgetLane::Completion)
        .unwrap();
    node.drain().unwrap();
    drop(node);
    assert_eq!(budget.stats().used, 0);
    let mut restored = DurableNode::open_in(config, directory.path(), &budget).unwrap();
    let events = restored.drain().unwrap();
    assert_eq!(events.snapshot.as_ref().unwrap().index, index);
    assert_eq!(events.snapshot.as_ref().unwrap().data, vec![8; 1024]);
    assert_eq!(events.committed.len(), 1);
    assert_eq!(events.committed[0].data, b"after");
}
