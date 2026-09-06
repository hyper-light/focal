use super::*;
fn options() -> WalOptions {
    WalOptions {
        identity: WalIdentity {
            cluster: [1; 16],
            node: 1,
            stream: 0,
        },
        segment_bytes: 4096,
        max_record_bytes: 1024,
        max_batch_bytes: 16 * 1024,
    }
}
fn record(log: u8, index: u64) -> Record {
    Record {
        log: LogicalLogId([log; 16]),
        kind: RecordKind::Entry,
        index,
        term: 1,
        payload: vec![log; 32],
    }
}
fn memory() -> MemoryBudget {
    MemoryBudget::new(32 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}

#[test]
fn append_preflight_rejects_impossible_ancestor_capacity_without_using_current_free_space() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(4 * 1024 * 1024, 1024 * 1024).unwrap();
    let child = parent.child(32 * 1024 * 1024, 8 * 1024 * 1024).unwrap();
    let mut options = options();
    options.max_batch_bytes = 16 * 1024 * 1024;
    let shared =
        SharedWal::open_with_budget(dir.path(), options, WalWriterLimits::default(), child)
            .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let records: Vec<_> = (1..=3000)
        .map(|index| Record {
            payload: vec![1; 900],
            ..record(1, index)
        })
        .collect();
    assert!(
        shared
            .validate_batch(LogicalLogId([1; 16]), &records)
            .is_ok()
    );
    let before = parent.stats().used;
    assert!(matches!(
        lease.validate_append(&records),
        Err(LogError::Capacity)
    ));
    assert_eq!(parent.stats().used, before);
    let small = [record(1, 1)];
    let pressure = parent
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            parent.stats().limit - parent.stats().used,
        )
        .unwrap();
    lease.validate_append(&small).unwrap();
    assert!(matches!(
        lease.append_async_in(&small, BudgetLane::Completion),
        Err(LogError::Capacity)
    ));
    drop(pressure);
    lease.append_in(&small, BudgetLane::Completion).unwrap();
    assert_eq!(shared.stats().unwrap().appended_records, 1);
}
fn pause(shared: &SharedWal) -> mpsc::SyncSender<()> {
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, paused) = mpsc::sync_channel(1);
    shared.send(Command::Pause(entered, paused)).unwrap();
    waiting.recv().unwrap();
    resume
}
fn records(lease: &WalLease) -> Vec<Record> {
    let mut output = Vec::new();
    lease
        .replay(|record| {
            output.push(record);
            Ok(())
        })
        .unwrap();
    output
}
#[tokio::test]
async fn queued_groups_share_flush_without_acknowledging_before_fence() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
    let resume = pause(&shared);
    let mut first = a.append_async(&[record(1, 1)]).unwrap();
    let mut second = b.append_async(&[record(2, 1)]).unwrap();
    let mut third = a.append_async(&[record(1, 2)]).unwrap();
    assert!(first.try_complete().is_none());
    assert!(second.try_complete().is_none());
    assert!(third.try_complete().is_none());
    resume.send(()).unwrap();
    let first = first.await.unwrap();
    assert_eq!(second.await.unwrap(), first);
    assert_eq!(third.await.unwrap(), first);
    assert_eq!(first.sequence, 3);
    assert_eq!(shared.stats().unwrap().group_commits, 1);
    assert_eq!(records(&a), vec![record(1, 1), record(1, 2)]);
    assert_eq!(records(&b), vec![record(2, 1)]);
}
#[tokio::test]
async fn queue_backpressure_reserves_metadata_slots_and_cancelled_tickets_still_persist() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits {
            queue_items: 2,
            ..Default::default()
        },
        budget.clone(),
    )
    .unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let resume = pause(&shared);
    let first = a.append_async(&[record(1, 1)]).unwrap();
    drop(a.append_async(&[record(1, 2)]).unwrap());
    assert!(matches!(
        a.append_async(&[record(1, 3)]),
        Err(LogError::Capacity)
    ));
    let mut hard = record(1, 3);
    hard.kind = RecordKind::HardState;
    let metadata = a.append_async(&[hard.clone()]).unwrap();
    resume.send(()).unwrap();
    first.await.unwrap();
    metadata.await.unwrap();
    assert_eq!(records(&a), vec![record(1, 1), record(1, 2), hard]);
    drop(a);
    drop(shared);
    assert_eq!(budget.stats().used, 0);
}
#[tokio::test]
async fn every_affected_receipt_fails_on_ambiguous_flush_and_recovery_uses_fence() {
    for point in [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
        let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
        a.append(&[record(1, 1)]).unwrap();
        a.try_inject_fault_once(point).unwrap();
        let resume = pause(&shared);
        let first = a.append_async(&[record(1, 2)]).unwrap();
        let second = b.append_async(&[record(2, 1)]).unwrap();
        resume.send(()).unwrap();
        assert!(first.await.is_err());
        assert!(second.await.is_err());
        assert!(matches!(a.append(&[record(1, 3)]), Err(LogError::Failed)));
        assert!(matches!(shared.identity(), Err(LogError::Failed)));
        drop(a);
        drop(b);
        drop(shared);
        let recovered = SharedWal::open(dir.path(), options()).unwrap();
        let a = recovered.lease(LogicalLogId([1; 16])).unwrap();
        let b = recovered.lease(LogicalLogId([2; 16])).unwrap();
        if point == FaultPoint::AfterFenceInstall {
            assert_eq!(records(&a), vec![record(1, 1), record(1, 2)]);
            assert_eq!(records(&b), vec![record(2, 1)]);
        } else {
            assert_eq!(records(&a), vec![record(1, 1)]);
            assert!(records(&b).is_empty());
        }
    }
}
#[test]
fn startup_scans_once_and_each_log_replays_only_its_indexed_frames_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    {
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        for log in 1..=3 {
            let mut lease = shared.lease(LogicalLogId([log; 16])).unwrap();
            lease.append(&[record(log, 1), record(log, 2)]).unwrap();
        }
    }
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    assert_eq!(shared.stats().unwrap().startup_scan_records, 6);
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&a), vec![record(1, 1), record(1, 2)]);
    assert_eq!(shared.stats().unwrap().replayed_records, 2);
    let mut checkpoint = record(1, 2);
    checkpoint.kind = RecordKind::Snapshot;
    a.rewrite_checkpoint(&[checkpoint.clone()]).unwrap();
    let b = shared.lease(LogicalLogId([2; 16])).unwrap();
    let c = shared.lease(LogicalLogId([3; 16])).unwrap();
    assert_eq!(records(&a), vec![checkpoint]);
    assert_eq!(records(&b), vec![record(2, 1), record(2, 2)]);
    assert_eq!(records(&c), vec![record(3, 1), record(3, 2)]);
    assert_eq!(shared.stats().unwrap().indexed_records, 5);
}
#[tokio::test]
async fn final_handle_joins_with_outstanding_ticket_and_lease_reuse_is_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert!(matches!(
        shared.lease(LogicalLogId([1; 16])),
        Err(LogError::LogicalLocked)
    ));
    let receipt = lease.append_async(&[record(1, 1)]).unwrap();
    drop(lease);
    let lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&lease), vec![record(1, 1)]);
    drop(lease);
    drop(shared);
    assert!(receipt.await.is_ok());
    assert_eq!(budget.stats().used, 0);
    let recovered = SharedWal::open(dir.path(), options()).unwrap();
    assert_eq!(recovered.stats().unwrap().startup_scan_records, 1);
}
#[test]
fn rejected_oversized_wrong_group_and_failed_visitor_leave_writer_usable() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert!(matches!(a.append(&[record(2, 1)]), Err(LogError::Identity)));
    let mut huge = record(1, 1);
    huge.payload = vec![0; 1024];
    assert!(matches!(a.append(&[huge]), Err(LogError::Capacity)));
    a.append(&[record(1, 1), record(1, 2)]).unwrap();
    assert!(matches!(
        a.replay(|_| Err(LogError::Capacity)),
        Err(LogError::Capacity)
    ));
    a.append(&[record(1, 3)]).unwrap();
    assert_eq!(records(&a).len(), 3);
}

#[tokio::test]
async fn batch_request_limit_splits_flushes_without_reordering_a_group() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits {
            max_batch_requests: 2,
            ..Default::default()
        },
        memory(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let resume = pause(&shared);
    let first = lease.append_async(&[record(1, 1)]).unwrap();
    let second = lease.append_async(&[record(1, 2)]).unwrap();
    let third = lease.append_async(&[record(1, 3)]).unwrap();
    resume.send(()).unwrap();
    let first = first.await.unwrap();
    assert_eq!(second.await.unwrap(), first);
    assert!(third.await.unwrap().sequence > first.sequence);
    assert_eq!(shared.stats().unwrap().group_commits, 2);
    assert_eq!(
        records(&lease),
        vec![record(1, 1), record(1, 2), record(1, 3)]
    );
}
#[test]
fn index_admission_failure_rolls_back_before_enqueue_and_empty_batches_do_not_grow_index() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let before = budget.stats().used;
    let hold = reserve(
        &budget,
        BudgetKind::Control,
        BudgetLane::Completion,
        32 * 1024 * 1024 - before - 768,
    )
    .unwrap();
    let held = budget.stats().used;
    assert!(matches!(
        lease.append(&[record(1, 1)]),
        Err(LogError::Capacity)
    ));
    assert_eq!(budget.stats().used, held);
    drop(hold);
    assert_eq!(budget.stats().used, before);
    for _ in 0..100 {
        lease.append(&[]).unwrap();
    }
    assert_eq!(shared.stats().unwrap().indexed_records, 0);
    assert_eq!(budget.stats().used, before);
    lease.append(&[record(1, 1)]).unwrap();
    assert_eq!(records(&lease), vec![record(1, 1)]);
}
#[test]
fn unconsumed_completed_ticket_keeps_its_charge_after_final_writer_handle_drops() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let ticket = lease.append_async(&[record(1, 1)]).unwrap();
    // The synchronous stats command is ordered after the asynchronous append.
    assert_eq!(shared.stats().unwrap().appended_records, 1);
    drop(lease);
    drop(shared);
    assert_eq!(budget.stats().used, 1024);
    drop(ticket);
    assert_eq!(budget.stats().used, 0);
    SharedWal::open(dir.path(), options()).unwrap();
}

#[tokio::test]
async fn checkpoint_is_a_fifo_barrier_between_pending_appends() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
    b.append(&[record(2, 1)]).unwrap();
    let resume = pause(&shared);
    let before = a.append_async(&[record(1, 1)]).unwrap();
    let mut snapshot = record(1, 1);
    snapshot.kind = RecordKind::Snapshot;
    let (sender, receiver) = mpsc::sync_channel(1);
    shared
        .batch(
            a.log,
            a.generation,
            &[snapshot.clone()],
            Reply::Blocking(sender),
            true,
            BudgetLane::Ordinary,
        )
        .unwrap();
    let after = a.append_async(&[record(1, 2)]).unwrap();
    resume.send(()).unwrap();
    let before = before.await.unwrap();
    let checkpoint = receiver.recv().unwrap().unwrap();
    let after = after.await.unwrap();
    assert!(checkpoint.generation > before.generation);
    assert_eq!(after.generation, checkpoint.generation);
    assert!(after.sequence > checkpoint.sequence);
    assert_eq!(records(&a), vec![snapshot, record(1, 2)]);
    assert_eq!(records(&b), vec![record(2, 1)]);
}
#[test]
fn indexed_replay_detects_changed_durable_bytes_and_stops_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let position = a.append(&[record(1, 1)]).unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .open(segment_path(dir.path(), position.generation, 0))
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_LEN + FRAME_HEADER as u64))
        .unwrap();
    file.write_all(&[88]).unwrap();
    file.sync_all().unwrap();
    assert!(matches!(
        a.replay(|_| Ok(())),
        Err(LogError::Corruption { .. })
    ));
    assert!(matches!(shared.identity(), Err(LogError::Failed)));
    assert!(matches!(a.append(&[record(1, 2)]), Err(LogError::Failed)));
}

#[test]
fn checkpoint_preserves_every_active_empty_group_index() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut empty = shared.lease(LogicalLogId([2; 16])).unwrap();
    a.append(&[record(1, 1)]).unwrap();
    a.rewrite_checkpoint(&[]).unwrap();
    // Both the emptied checkpoint target and unrelated empty lease remain usable.
    empty.append(&[record(2, 1)]).unwrap();
    a.append(&[record(1, 2)]).unwrap();
    assert_eq!(records(&empty), vec![record(2, 1)]);
    assert_eq!(records(&a), vec![record(1, 2)]);
    assert_eq!(shared.stats().unwrap().indexed_records, 2);
}

#[tokio::test]
async fn durable_completion_and_metadata_receipts_use_reserved_ram_under_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budget(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        budget.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let ordinary = reserve(
        &budget,
        BudgetKind::Pending,
        BudgetLane::Ordinary,
        24 * 1024 * 1024 - budget.stats().ordinary_used,
    )
    .unwrap();
    assert!(matches!(
        lease.append_async(&[record(1, 1)]),
        Err(LogError::Capacity)
    ));
    let mut hard = record(1, 1);
    hard.kind = RecordKind::HardState;
    lease.append_async(&[hard.clone()]).unwrap().await.unwrap();
    lease
        .append_async_in(&[record(1, 2)], BudgetLane::Completion)
        .unwrap()
        .await
        .unwrap();
    lease
        .append_in(&[record(1, 3)], BudgetLane::Completion)
        .unwrap();
    assert_eq!(records(&lease), vec![hard, record(1, 2), record(1, 3)]);
    let mut snapshot = record(1, 3);
    snapshot.kind = RecordKind::Snapshot;
    assert!(matches!(
        lease.rewrite_checkpoint(&[snapshot.clone()]),
        Err(LogError::Capacity)
    ));
    lease
        .rewrite_checkpoint_in(&[snapshot.clone()], BudgetLane::Completion)
        .unwrap();
    assert_eq!(records(&lease), vec![snapshot]);
    drop(ordinary);
    drop(lease);
    drop(shared);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn replay_callbacks_reject_synchronous_reentry_before_queueing_and_guard_unwinds() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
    a.append(&[record(1, 1), record(1, 2)]).unwrap();
    let mut visited = 0;
    a.replay(|_| {
        visited += 1;
        assert!(matches!(shared.stats(), Err(LogError::ReplayReentry)));
        assert!(matches!(shared.identity(), Err(LogError::ReplayReentry)));
        assert!(matches!(
            shared.lease(LogicalLogId([3; 16])),
            Err(LogError::ReplayReentry)
        ));
        assert!(matches!(
            b.append(&[record(2, 1)]),
            Err(LogError::ReplayReentry)
        ));
        assert!(matches!(
            b.rewrite_checkpoint(&[]),
            Err(LogError::ReplayReentry)
        ));
        assert!(matches!(b.replay(|_| Ok(())), Err(LogError::ReplayReentry)));
        assert!(matches!(
            b.try_inject_fault_once(FaultPoint::AfterAppend),
            Err(LogError::ReplayReentry)
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(visited, 2);
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = a.replay(|_| panic!("visitor failure"));
    }));
    assert!(unwind.is_err());
    b.append(&[record(2, 1)]).unwrap();
    assert_eq!(records(&b), vec![record(2, 1)]);
    assert_eq!(records(&a).len(), 2);
    assert_eq!(shared.stats().unwrap().indexed_records, 3);
}

#[test]
fn replay_can_enqueue_async_work_but_cannot_wait_on_the_disk_owner() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut source = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut target = shared.lease(LogicalLogId([2; 16])).unwrap();
    source
        .append(&[record(1, 1), record(1, 2), record(1, 3)])
        .unwrap();
    let mut requested = false;
    source
        .replay(|_| {
            if !requested {
                let mut ticket = Box::pin(target.append_async(&[record(2, 1)]).unwrap());
                let mut context = Context::from_waker(std::task::Waker::noop());
                assert!(matches!(
                    ticket.as_mut().poll(&mut context),
                    Poll::Ready(Err(LogError::ReplayReentry))
                ));
                requested = true;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        records(&target),
        vec![record(2, 1)],
        "declined wait does not cancel admitted write"
    );
}

#[tokio::test]
async fn consumed_append_receipts_return_typed_errors_for_every_poll_order() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut observed = Box::pin(lease.append_async(&[record(1, 1)]).unwrap());
    shared.stats().unwrap();
    assert!(matches!(observed.try_complete(), Some(Ok(_))));
    assert!(matches!(
        observed.as_mut().await,
        Err(LogError::ReceiptConsumed)
    ));
    assert!(matches!(
        observed.try_complete(),
        Some(Err(LogError::ReceiptConsumed))
    ));
    let mut context = Context::from_waker(std::task::Waker::noop());
    for _ in 0..3 {
        assert!(matches!(
            observed.as_mut().poll(&mut context),
            Poll::Ready(Err(LogError::ReceiptConsumed))
        ));
    }
    let mut awaited = Box::pin(lease.append_async(&[record(1, 2)]).unwrap());
    assert!(awaited.as_mut().await.is_ok());
    assert!(matches!(
        awaited.as_mut().poll(&mut context),
        Poll::Ready(Err(LogError::ReceiptConsumed))
    ));
    assert!(matches!(
        awaited.try_complete(),
        Some(Err(LogError::ReceiptConsumed))
    ));
    assert_eq!(records(&lease), vec![record(1, 1), record(1, 2)]);
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_ticket_wait_is_runtime_independent_and_receipt_is_consumed_once() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut ticket = lease.append_async(&[record(1, 1)]).unwrap();
    assert!(ticket.wait_blocking().is_ok());
    assert!(matches!(
        ticket.wait_blocking(),
        Err(LogError::ReceiptConsumed)
    ));
    assert!(matches!(
        ticket.try_complete(),
        Some(Err(LogError::ReceiptConsumed))
    ));
    assert_eq!(records(&lease), vec![record(1, 1)]);
}
