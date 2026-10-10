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
            .validate_batch(LogicalLogId([1; 16]), &records, None)
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
    // Three records and the commit frame that closed them (doc 28).
    assert_eq!(first.sequence, 4);
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
        // A commit whose frame was flushed is durable without a fence (doc 28); the frame is written only
        // after the data's flush.
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
    // A checkpoint is a commit of the one generation: its frames and its
    // floor follow what was queued before it and precede what came after.
    assert_eq!(checkpoint.generation, before.generation);
    assert!(checkpoint.sequence > before.sequence);
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

#[test]
fn a_batch_is_promised_its_volume_bytes_before_queueing_and_charged_after_its_fence() {
    use focal_memory::{DiskBudget, DiskBudgetConfig};
    let dir = tempfile::tempdir().unwrap();
    // A watermark above any real volume refuses every fresh batch before a
    // byte is written, and keeps nothing promised on refusal.
    let guarded = DiskBudget::new(DiskBudgetConfig {
        headroom: u64::MAX / 2,
        completion_reserve: 0,
        sample_interval: 4,
    })
    .unwrap();
    let shared = SharedWal::open_with_budgets(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        memory(),
        guarded.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert!(matches!(
        lease.append(&[record(1, 1)]),
        Err(LogError::Capacity)
    ));
    assert_eq!(guarded.stats().outstanding, 0);
    // The unpromised free bytes are reported as sampled; the watermark that
    // refused the batch is the envelope's, which admission compares against.
    assert!(shared.available_bytes().unwrap() > 0);
    assert_eq!(guarded.available(BudgetLane::Completion), 0);
    assert_eq!(shared.stats().unwrap().appended_records, 0);
    drop(lease);
    drop(shared);
    // Without a watermark the sample still charges every durable batch, so
    // a run of admissions between samples cannot promise the same bytes twice.
    let open = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    let shared = SharedWal::open_with_budgets(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        memory(),
        open.clone(),
    )
    .unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    let before = shared.available_bytes().unwrap();
    assert!(before > 0);
    lease.append(&[record(1, 1), record(1, 2)]).unwrap();
    let stats = open.stats();
    assert_eq!(stats.outstanding, 0);
    assert!(stats.free.unwrap() < before);
    assert_eq!(shared.available_bytes().unwrap(), stats.free.unwrap());
    // The checkpoint rewrite is promised the same way.
    lease.rewrite_checkpoint(&[record(1, 1)]).unwrap();
    assert_eq!(open.stats().outstanding, 0);
    assert!(open.stats().free.unwrap() < stats.free.unwrap());
}

/// The audit's F46: a history admitted under a budget reopens under it. The
/// index of a reopened history costs what its records cost — one chunk a
/// group — never what the batches that wrote it cost, so the bytes retained
/// after a reopen are at most those retained after the appends; the reopen's
/// own transient is the scan's buffer, three records at most.
#[test]
fn an_admitted_history_reopens_within_the_budget_that_admitted_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options();
    options.max_record_bytes = 4096;
    options.max_batch_bytes = 64 * 1024;
    options.segment_bytes = 1 << 20;
    let ample = MemoryBudget::new(64 << 20, 16 << 20).unwrap();
    let retained = {
        let shared = SharedWal::open_with_budget(
            dir.path(),
            options.clone(),
            WalWriterLimits::default(),
            ample.clone(),
        )
        .unwrap();
        let idle = ample.stats().used;
        let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
        let records: Vec<Record> = (1..=256)
            .map(|index| Record {
                payload: vec![7; 64],
                ..record(1, index)
            })
            .collect();
        lease.append(&records).unwrap();
        drop(lease);
        let after = ample.stats().used;
        assert!(after > idle, "the history is retained: {after} > {idle}");
        after
    };
    assert_eq!(ample.stats().used, 0);
    // The bytes that admitted the history, plus the scan's transient buffer,
    // admit its reopen; what the reopen retains is at most what the appends
    // retained.
    let same = MemoryBudget::new(retained + 3 * options.max_record_bytes, 4096).unwrap();
    let reopened = SharedWal::open_with_budget(
        dir.path(),
        options.clone(),
        WalWriterLimits::default(),
        same.clone(),
    )
    .unwrap_or_else(|error| panic!("{error:?} under {retained} + the scan buffer"));
    assert_eq!(reopened.stats().unwrap().startup_scan_records, 256);
    let after_reopen = same.stats().used;
    assert!(
        after_reopen <= retained,
        "reopen retains {after_reopen} > appends {retained}"
    );
    let lease = reopened.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&lease).len(), 256);
}

/// Across interleaved groups, batches of every size, a checkpoint's rewrite
/// and a reopen, the index never costs more than the appends that wrote it,
/// and every log replays exactly what was written.
#[test]
fn the_packed_index_never_costs_more_than_the_appends_across_groups_batches_and_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options();
    options.max_record_bytes = 4096;
    options.max_batch_bytes = 64 * 1024;
    options.segment_bytes = 1 << 20;
    let budget = MemoryBudget::new(64 << 20, 16 << 20).unwrap();
    let (retained, expected) = {
        let shared = SharedWal::open_with_budget(
            dir.path(),
            options.clone(),
            WalWriterLimits::default(),
            budget.clone(),
        )
        .unwrap();
        let mut leases: Vec<_> = (1..=3u8)
            .map(|log| shared.lease(LogicalLogId([log; 16])).unwrap())
            .collect();
        let batches: [(usize, &[u64]); 3] = [(0, &[1, 7, 64]), (1, &[3, 3, 3, 3]), (2, &[100])];
        let mut written = vec![Vec::new(), Vec::new(), Vec::new()];
        let mut next = [1u64; 3];
        // Interleaved: one batch of each log in turn.
        for round in 0..4 {
            for (slot, sizes) in batches {
                let Some(size) = sizes.get(round) else {
                    continue;
                };
                let log = slot as u8 + 1;
                let records: Vec<Record> = (0..*size)
                    .map(|_| {
                        let index = next[slot];
                        next[slot] += 1;
                        record(log, index)
                    })
                    .collect();
                leases[slot].append(&records).unwrap();
                written[slot].extend(records);
            }
        }
        let after_appends = budget.stats().used;
        // The rewrite of one log packs the replacement index: it costs no
        // more than the appends did.
        let mut checkpoint = record(1, 72);
        checkpoint.kind = RecordKind::Snapshot;
        leases[0].rewrite_checkpoint(&[checkpoint.clone()]).unwrap();
        written[0] = vec![checkpoint];
        let after_checkpoint = budget.stats().used;
        assert!(
            after_checkpoint <= after_appends,
            "the rewrite retains {after_checkpoint} > the appends {after_appends}"
        );
        for (slot, lease) in leases.iter().enumerate() {
            assert_eq!(records(lease), written[slot], "log {}", slot + 1);
        }
        drop(leases);
        (budget.stats().used, written)
    };
    assert_eq!(budget.stats().used, 0);
    let same = MemoryBudget::new(retained + 3 * options.max_record_bytes, 4096).unwrap();
    let reopened = SharedWal::open_with_budget(
        dir.path(),
        options.clone(),
        WalWriterLimits::default(),
        same.clone(),
    )
    .unwrap_or_else(|error| panic!("{error:?} under {retained} + the scan buffer"));
    assert!(
        same.stats().used <= retained,
        "{} > {retained}",
        same.stats().used
    );
    assert_eq!(reopened.stats().unwrap().startup_scan_records, 1 + 12 + 100);
    for (slot, expected) in expected.iter().enumerate() {
        let lease = reopened.lease(LogicalLogId([slot as u8 + 1; 16])).unwrap();
        assert_eq!(&records(&lease), expected, "log {} after reopen", slot + 1);
    }
}

// ---- The audit's F14: a checkpoint is its group's own, and the log is
// ---- cleaned by a base that moves.

fn snapshot(log: u8, index: u64) -> Record {
    Record {
        kind: RecordKind::Snapshot,
        ..record(log, index)
    }
}
/// What a record takes on disk as one frame.
fn frame(record: &Record) -> u64 {
    (FRAME_HEADER + postcard::experimental::serialized_size(record).unwrap()) as u64
}
/// Clean until the base can move no further; the writer's counts then.
fn settle(shared: &SharedWal) -> WalWriterStats {
    let (done, settled) = mpsc::sync_channel(1);
    shared.send(Command::Settle(done)).unwrap();
    settled.recv().unwrap();
    shared.stats().unwrap()
}
/// Hold the base still between commands, or let it move.
fn idle(shared: &SharedWal, idle: bool) {
    let (done, set) = mpsc::sync_channel(1);
    shared.send(Command::Idle(idle, done)).unwrap();
    set.recv().unwrap();
}
fn fence(dir: &Path) -> Fence {
    read_fence(&dir.join("CURRENT")).unwrap()
}
/// The segments on disk, in order.
fn segments(dir: &Path) -> Vec<u64> {
    let mut found: Vec<u64> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let rest = name.strip_prefix("wal-")?.strip_suffix(".seg")?.to_string();
            rest.split_once('-')?.1.parse().ok()
        })
        .collect();
    found.sort_unstable();
    found
}
fn due(stats: &WalWriterStats, options: &WalOptions) -> bool {
    stats.physical_bytes - stats.live_bytes > stats.live_bytes + options.segment_bytes
}

#[test]
fn a_checkpoint_writes_what_its_group_keeps_and_its_floor_and_asks_the_volume_for_those_bytes() {
    use focal_memory::{DiskBudget, DiskBudgetConfig};
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    let shared = SharedWal::open_with_budgets(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        memory(),
        disk.clone(),
    )
    .unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
    let mut kept = Vec::new();
    for index in 1..=60 {
        a.append(&[record(1, index)]).unwrap();
        b.append(&[record(2, index)]).unwrap();
        kept.push(record(2, index));
    }
    let before = shared.stats().unwrap();
    // What is on disk and not live is the commit frames, one a group commit, each its header alone while the
    // base stays (doc 28).
    assert_eq!(
        before.physical_bytes - before.live_bytes,
        before.group_commits * FRAME_HEADER as u64
    );
    assert_eq!(before.checkpoint_bytes, 0);
    // The volume has room for the checkpoint's own frames and no more: a
    // fraction of the log, which holds two groups' histories.
    let checkpoint = snapshot(1, 60);
    let floor = frame(&Record {
        log: LogicalLogId([1; 16]),
        kind: RecordKind::Floor,
        // The floor names the sequence after the tail: every record and every commit frame took one.
        index: before.appended_records + before.group_commits + 1,
        term: 1,
        payload: Vec::new(),
    });
    let own = frame(&checkpoint) + floor;
    // The checkpoint's commit is one commit frame more on the volume (doc 28).
    let commit = COMMIT_FRAME_BYTES as u64;
    assert!(own * 20 < before.physical_bytes);
    disk.observe(own + commit - 1);
    assert!(matches!(
        a.rewrite_checkpoint_in(std::slice::from_ref(&checkpoint), BudgetLane::Completion),
        Err(LogError::Capacity)
    ));
    assert_eq!(disk.stats().outstanding, 0);
    assert_eq!(records(&a).len(), 60);
    disk.observe(own + commit);
    a.rewrite_checkpoint_in(std::slice::from_ref(&checkpoint), BudgetLane::Completion)
        .unwrap();
    let after = shared.stats().unwrap();
    assert_eq!(after.checkpoint_bytes, own);
    assert_eq!(after.relocated_bytes, 0);
    // The other group's frames were not read, moved or written: the log
    // grew by the checkpoint and its commit, less what the base passed of
    // the dead frames at its head (commit frames among them, doc 28), and
    // what the group held before is dead.
    // The checkpoint's commit frame carries the base when its cleaning moved it.
    let grown = after.physical_bytes + (after.reclaimed_bytes - before.reclaimed_bytes)
        - before.physical_bytes
        - own;
    assert!(grown == FRAME_HEADER as u64 || grown == commit, "{grown}");
    assert_eq!(after.live_bytes, before.live_bytes / 2 + frame(&checkpoint));
    assert_eq!(after.indexed_records, 61);
    assert_eq!(records(&a), vec![checkpoint.clone()]);
    assert_eq!(records(&b), kept);
    drop((a, b, shared));
    // A reopen learns the floor from the log.
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    assert_eq!(shared.stats().unwrap().startup_scan_records, 61);
    let a = shared.lease(LogicalLogId([1; 16])).unwrap();
    let b = shared.lease(LogicalLogId([2; 16])).unwrap();
    assert_eq!(records(&a), vec![checkpoint]);
    assert_eq!(records(&b), kept);
}

/// A batch's caller is told once what the batch held is given back, or
/// charged: asked on the writer's own thread at the moment it answers
/// (`Persisted`), the volume has nothing outstanding and the memory that
/// waits holds the caller's receipt and nothing of the batch — for a
/// checkpoint the volume refuses and for an append it takes. Told first, a
/// caller that looked at once found its own refused batch's bytes still
/// outstanding: `a_checkpoint_writes_what_its_group_keeps_...` met that on
/// Linux, where the writer's thread let go later than the caller looked.
#[test]
fn a_batchs_caller_is_told_once_what_the_batch_held_is_given_back() {
    use focal_memory::{DiskBudget, DiskBudgetConfig};
    use std::sync::{Arc, Mutex};
    let dir = tempfile::tempdir().unwrap();
    let disk = DiskBudget::new(DiskBudgetConfig {
        headroom: 0,
        completion_reserve: 0,
        sample_interval: u32::MAX,
    })
    .unwrap();
    let budget = memory();
    let shared = SharedWal::open_with_budgets(
        dir.path(),
        options(),
        WalWriterLimits::default(),
        budget.clone(),
        disk.clone(),
    )
    .unwrap();
    let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
    a.append(&[record(1, 1)]).unwrap();
    let waiting = |budget: &MemoryBudget| budget.stats().by_kind[BudgetKind::Pending as usize];
    let at_rest = waiting(&budget);
    type Seen = Arc<Mutex<Vec<(u64, usize)>>>;
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let look = |seen: &Seen| -> Persisted {
        let (seen, disk, budget) = (seen.clone(), disk.clone(), budget.clone());
        Box::new(move || {
            seen.lock().unwrap().push((
                disk.stats().outstanding,
                budget.stats().by_kind[BudgetKind::Pending as usize],
            ));
        })
    };
    // A checkpoint whose own frame the volume has room for and whose floor
    // it has not: refused by the writer, with the frame's bytes reserved.
    let checkpoint = snapshot(1, 1);
    let floor = frame(&Record {
        log: LogicalLogId([1; 16]),
        kind: RecordKind::Floor,
        index: shared.stats().unwrap().appended_records + 1,
        term: 1,
        payload: Vec::new(),
    });
    disk.observe(frame(&checkpoint) + floor - 1);
    let mut refused = a
        .rewrite_checkpoint_async_notified(
            std::slice::from_ref(&checkpoint),
            BudgetLane::Completion,
            Some(look(&seen)),
        )
        .unwrap();
    assert!(matches!(refused.wait_blocking(), Err(LogError::Capacity)));
    // The look is taken on the writer's thread after the answer is sent
    // (the wake follows the answer, so an owner woken finds it there): the
    // caller waits for it before it asks again, or the look would see what
    // the caller asked next.
    let looked = |seen: &Seen, times: usize| {
        for _ in 0..10_000 {
            if seen.lock().unwrap().len() >= times {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("the writer never looked: {:?}", seen.lock().unwrap());
    };
    looked(&seen, 1);
    // An append the volume takes.
    disk.observe(1 << 20);
    let mut taken = a
        .append_async_notified(&[record(1, 2)], BudgetLane::Ordinary, Some(look(&seen)))
        .unwrap();
    taken.wait_blocking().unwrap();
    looked(&seen, 2);
    // The receipt a caller holds is its own; nothing else waited.
    let receipt = waiting(&budget) - at_rest;
    assert!(receipt > 0);
    drop((refused, taken));
    assert_eq!(waiting(&budget), at_rest);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(0, at_rest + receipt / 2), (0, at_rest + receipt)]
    );
}

/// A cold group whose frames stand at the head of the log, behind a hot
/// one that appends and checkpoints: the base meets the cold frames and
/// writes them again at the tail, lap after lap, in the group's order; the
/// log holds no more dead bytes than live ones and a segment; and a reopen
/// finds every group as it was.
#[test]
fn a_cold_group_the_base_meets_is_written_again_in_its_order_and_the_log_keeps_its_bound() {
    let dir = tempfile::tempdir().unwrap();
    let budget = memory();
    let cold_records: Vec<Record> = (1..=40).map(|index| record(1, index)).collect();
    let kept = {
        let shared = SharedWal::open_with_budget(
            dir.path(),
            options(),
            WalWriterLimits::default(),
            budget.clone(),
        )
        .unwrap();
        let mut cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        let mut hot = shared.lease(LogicalLogId([2; 16])).unwrap();
        for batch in cold_records.chunks(8) {
            cold.append(batch).unwrap();
        }
        let mut kept = Vec::new();
        for round in 1..=120u64 {
            let batch: Vec<Record> = (0..16).map(|at| record(2, round * 100 + at)).collect();
            hot.append(&batch).unwrap();
            kept = vec![snapshot(2, round)];
            hot.rewrite_checkpoint(&kept).unwrap();
            if round % 25 == 0 {
                assert_eq!(records(&cold), cold_records, "round {round}");
                assert_eq!(records(&hot), kept, "round {round}");
            }
        }
        let stats = settle(&shared);
        assert!(!due(&stats, &options()), "{stats:?}");
        assert_eq!(stats.indexed_records, 41);
        // The cold frames were met more than once: a frame written again
        // is written again as it stands, under the origin it carries.
        assert!(stats.relocated_records > 80, "{stats:?}");
        assert!(stats.reclaimed_segments > 0, "{stats:?}");
        // What cleaning wrote is less than what it freed.
        assert!(stats.relocated_bytes < stats.reclaimed_bytes, "{stats:?}");
        // The files on disk are the base's segment to the tail's.
        let base = fence(dir.path()).base;
        let held = segments(dir.path());
        assert_eq!(held.first(), Some(&base.segment));
        assert!(base.segment > 0);
        assert!(
            (held.len() as u64) * options().segment_bytes
                <= stats.physical_bytes + 2 * options().segment_bytes,
            "{held:?} {stats:?}"
        );
        assert_eq!(records(&cold), cold_records);
        assert_eq!(records(&hot), kept);
        kept
    };
    assert_eq!(budget.stats().used, 0);
    {
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        assert_eq!(shared.stats().unwrap().startup_scan_records, 41);
        let mut cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        let hot = shared.lease(LogicalLogId([2; 16])).unwrap();
        assert_eq!(records(&cold), cold_records);
        assert_eq!(records(&hot), kept);
        // What the group writes next follows what it held.
        cold.append(&[record(1, 41)]).unwrap();
        let mut all = cold_records.clone();
        all.push(record(1, 41));
        assert_eq!(records(&cold), all);
    }
    // A reader of the stream in its physical order cannot give a group its
    // order once frames were written again, and says so.
    let wal = Wal::open(dir.path(), options()).unwrap();
    assert!(matches!(wal.replay(|_| Ok(())), Err(LogError::Relocated)));
}

/// A crash at every durability boundary of a commit that cleans, and
/// after the fence that moved the base before the segments behind it are
/// removed: a reopen finds the cold group whole and in order, and the hot
/// one as the fence left it.
#[test]
fn a_crash_at_every_cut_of_a_cleaning_commit_recovers_every_group() {
    for point in [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
        FaultPoint::AfterBaseFence,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cold_records: Vec<Record> = (1..=40).map(|index| record(1, index)).collect();
        // The base moves inside commits alone, so the cut is a commit's.
        let shared = SharedWal::open_still(dir.path(), options()).unwrap();
        let mut cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        let mut hot = shared.lease(LogicalLogId([2; 16])).unwrap();
        cold.append(&cold_records).unwrap();
        let mut kept = Vec::new();
        let mut round = 0u64;
        // Until cleaning has begun.
        while shared.stats().unwrap().relocated_records == 0 {
            round += 1;
            assert!(round < 100);
            let batch: Vec<Record> = (0..16).map(|at| record(2, round * 100 + at)).collect();
            hot.append(&batch).unwrap();
            kept = vec![snapshot(2, round)];
            hot.rewrite_checkpoint(&kept).unwrap();
        }
        hot.try_inject_fault_once(point).unwrap();
        // Commits until the cut is reached: every one acknowledged is kept,
        // and the one that was cut is there or not as its fence is.
        let mut cut_at: Option<Vec<Record>> = None;
        let mut cut = false;
        for _ in 0..100 {
            round += 1;
            let batch: Vec<Record> = (0..16).map(|at| record(2, round * 100 + at)).collect();
            let mut next = kept.clone();
            next.extend(batch.iter().cloned());
            match hot.append(&batch) {
                Ok(_) => kept = next,
                Err(_) => {
                    cut_at = Some(next);
                    cut = true;
                    break;
                }
            }
            if shared.identity().is_err() {
                // The cut fell after the commit's fence and its replies.
                cut = true;
                break;
            }
            let next = vec![snapshot(2, round)];
            match hot.rewrite_checkpoint(&next) {
                Ok(_) => kept = next,
                Err(_) => {
                    cut_at = Some(next);
                    cut = true;
                    break;
                }
            }
            if shared.identity().is_err() {
                cut = true;
                break;
            }
        }
        assert!(cut, "{point:?}");
        assert!(matches!(shared.identity(), Err(LogError::Failed)));
        drop((cold, hot, shared));
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        let cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        let hot = shared.lease(LogicalLogId([2; 16])).unwrap();
        assert_eq!(records(&cold), cold_records, "{point:?}");
        match point {
            // The commit frame was not written: the commit is not there.
            FaultPoint::AfterAppend | FaultPoint::AfterDataSync => {
                assert!(cut_at.is_some());
                assert_eq!(records(&hot), kept, "{point:?}")
            }
            // The commit frame was flushed and the caller was not told: it is there (doc 28).
            FaultPoint::AfterFenceInstall => {
                assert_eq!(Some(records(&hot)), cut_at, "{point:?}")
            }
            // The commit was whole and acknowledged; the segments behind
            // its base were still there, and the reopen removed them.
            FaultPoint::AfterBaseFence => {
                assert!(cut_at.is_none());
                assert_eq!(records(&hot), kept, "{point:?}")
            }
        }
        // Nothing before the base is left on disk.
        assert_eq!(
            segments(dir.path()).first(),
            Some(&fence(dir.path()).base.segment),
            "{point:?}"
        );
    }
}

/// A reopen whose base stands inside the frames a checkpoint kept: the
/// floor's count is of the frames the scan reads, and those the base
/// passed are counted where they were written again.
#[test]
fn a_base_inside_the_frames_a_checkpoint_kept_recovers_the_group_whole() {
    let dir = tempfile::tempdir().unwrap();
    let cold_records: Vec<Record> = (1..=40).map(|index| snapshot(1, index)).collect();
    let mut inside = 0;
    let mut round = 0u64;
    let mut kept = Vec::new();
    {
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        let mut cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        cold.rewrite_checkpoint(&cold_records).unwrap();
    }
    // The group's frames are the forty a checkpoint kept, from sequence one.
    while inside < 3 {
        assert!(round < 400, "the base never stood inside the checkpoint");
        let shared = SharedWal::open_still(dir.path(), options()).unwrap();
        let cold = shared.lease(LogicalLogId([1; 16])).unwrap();
        let mut hot = shared.lease(LogicalLogId([2; 16])).unwrap();
        assert_eq!(records(&cold), cold_records, "round {round}");
        assert_eq!(records(&hot), kept, "round {round}");
        round += 1;
        let batch: Vec<Record> = (0..4).map(|at| record(2, round * 100 + at)).collect();
        hot.append(&batch).unwrap();
        kept = vec![snapshot(2, round)];
        hot.rewrite_checkpoint(&kept).unwrap();
        let base = fence(dir.path()).base;
        if (1..40).contains(&base.sequence) {
            inside += 1;
        }
    }
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let cold = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&cold), cold_records);
}

/// A group that checkpoints to nothing frees the log behind it with no
/// further command: the writer cleans while nothing waits, and the frames
/// of the group that stays are written again once.
#[test]
fn garbage_is_worked_off_while_no_command_waits() {
    let dir = tempfile::tempdir().unwrap();
    let shared = SharedWal::open_still(dir.path(), options()).unwrap();
    let mut stays = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut leaves = shared.lease(LogicalLogId([2; 16])).unwrap();
    let mut held = Vec::new();
    for round in 0..25u64 {
        let batch: Vec<Record> = (1..=4).map(|at| record(1, round * 4 + at)).collect();
        stays.append(&batch).unwrap();
        held.extend(batch);
        let batch: Vec<Record> = (0..40).map(|at| record(2, round * 100 + at)).collect();
        leaves.append(&batch).unwrap();
    }
    leaves.rewrite_checkpoint(&[]).unwrap();
    let before = shared.stats().unwrap();
    // The checkpoint's commit read a batch of the log and no more: most
    // of what it freed is still there.
    assert!(due(&before, &options()), "{before:?}");
    let first = segments(dir.path());
    idle(&shared, true);
    let after = settle(&shared);
    assert!(!due(&after, &options()), "{after:?}");
    assert!(after.reclaimed_segments > before.reclaimed_segments);
    assert!(segments(dir.path()).len() < first.len());
    assert!(after.relocated_records <= 100, "{after:?}");
    assert_eq!(records(&stays), held);
    assert!(records(&leaves).is_empty());
}

#[test]
fn a_fence_of_the_first_version_opens_as_a_prefix_from_the_start_and_is_written_forward() {
    #[derive(Serialize)]
    struct First {
        version: u32,
        identity: WalIdentity,
        position: DurablePosition,
    }
    let dir = tempfile::tempdir().unwrap();
    {
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
        lease.append(&[record(1, 1), record(1, 2)]).unwrap();
    }
    let current = fence(dir.path());
    assert_eq!(current.base, DurableBase::default());
    let data = postcard::to_stdvec(&First {
        version: 1,
        identity: current.identity,
        position: current.position,
    })
    .unwrap();
    let mut bytes = FENCE_MAGIC.to_vec();
    bytes.extend_from_slice(&data);
    bytes.extend_from_slice(&crc32fast::hash(&data).to_le_bytes());
    std::fs::write(dir.path().join("CURRENT"), &bytes).unwrap();
    let shared = SharedWal::open(dir.path(), options()).unwrap();
    let mut lease = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&lease), vec![record(1, 1), record(1, 2)]);
    lease.append(&[record(1, 3)]).unwrap();
    let written = std::fs::read(dir.path().join("CURRENT")).unwrap();
    let (version, _) = postcard::take_from_bytes::<u32>(&written[8..]).unwrap();
    assert_eq!(version, FENCE_VERSION);
    assert_eq!(fence(dir.path()).base, DurableBase::default());
}

/// The stream read in its physical order, by its single owner: a floor's
/// dead frames and the floors themselves are not delivered.
#[test]
fn a_single_owner_replays_the_live_records_of_a_stream_with_floors() {
    let dir = tempfile::tempdir().unwrap();
    let checkpoint = snapshot(1, 3);
    {
        let shared = SharedWal::open(dir.path(), options()).unwrap();
        let mut a = shared.lease(LogicalLogId([1; 16])).unwrap();
        let mut b = shared.lease(LogicalLogId([2; 16])).unwrap();
        a.append(&[record(1, 1), record(1, 2)]).unwrap();
        b.append(&[record(2, 1)]).unwrap();
        a.append(&[record(1, 3)]).unwrap();
        a.rewrite_checkpoint(std::slice::from_ref(&checkpoint))
            .unwrap();
        b.append(&[record(2, 2)]).unwrap();
    }
    let wal = Wal::open(dir.path(), options()).unwrap();
    let mut seen = Vec::new();
    wal.replay(|record| {
        seen.push(record);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![record(2, 1), checkpoint, record(2, 2)]);
}

/// The floor and the moved frame are the physical layer's own records: a
/// caller's batch that holds one is refused before anything is queued, and
/// the largest record a batch admits is moved like any other.
#[test]
fn a_caller_cannot_write_the_physical_layers_records_and_the_largest_record_is_moved() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = options();
    options.max_record_bytes = 1024;
    options.max_batch_bytes = 1024 + FRAME_HEADER;
    let shared = SharedWal::open(dir.path(), options.clone()).unwrap();
    let mut cold = shared.lease(LogicalLogId([1; 16])).unwrap();
    let mut hot = shared.lease(LogicalLogId([2; 16])).unwrap();
    for kind in [RecordKind::Floor, RecordKind::Moved] {
        let forged = Record {
            kind,
            ..record(1, 1)
        };
        assert!(matches!(
            cold.append(std::slice::from_ref(&forged)),
            Err(LogError::Identity)
        ));
        assert!(matches!(
            cold.rewrite_checkpoint(std::slice::from_ref(&forged)),
            Err(LogError::Identity)
        ));
    }
    // A record whose encoding is exactly the bound: its frame alone is a
    // batch, and written again it is a batch and its wrapper.
    let mut largest = record(1, 1);
    largest.payload = vec![7; 1024 - 21];
    assert_eq!(
        postcard::experimental::serialized_size(&largest).unwrap(),
        1024
    );
    cold.append(std::slice::from_ref(&largest)).unwrap();
    let mut round = 0u64;
    while shared.stats().unwrap().relocated_records == 0 {
        round += 1;
        assert!(round < 200, "{:?}", shared.stats().unwrap());
        hot.append(&[record(2, round)]).unwrap();
        hot.rewrite_checkpoint(&[]).unwrap();
        settle(&shared);
    }
    assert_eq!(records(&cold), vec![largest.clone()]);
    drop((cold, hot, shared));
    let shared = SharedWal::open(dir.path(), options).unwrap();
    let cold = shared.lease(LogicalLogId([1; 16])).unwrap();
    assert_eq!(records(&cold), vec![largest]);
}

/// A seeded generator for the histories below: the same seed, the same
/// history.
struct Seeded(u64);
impl Seeded {
    fn next(&mut self) -> u64 {
        // SplitMix64.
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Seeded histories over several groups: appends, checkpoints that keep
/// anything from nothing to a few records, leases given up and taken again,
/// cleaning inside commits and between them, a cut at every durability
/// boundary, and reopens. After every command the group it touched replays
/// exactly what the model holds; after every cut and reopen every group
/// does, the cut command there or not; and a settled log keeps its bound.
#[test]
fn seeded_histories_of_appends_checkpoints_cuts_and_reopens_replay_every_group_as_written() {
    const GROUPS: usize = 5;
    const POINTS: [FaultPoint; 4] = [
        FaultPoint::AfterAppend,
        FaultPoint::AfterDataSync,
        FaultPoint::AfterFenceInstall,
        FaultPoint::AfterBaseFence,
    ];
    fn leases(shared: &SharedWal) -> Vec<Option<WalLease>> {
        (0..GROUPS)
            .map(|at| Some(shared.lease(LogicalLogId([at as u8 + 1; 16])).unwrap()))
            .collect()
    }
    // The seeds of an ordinary run; a campaign names its own.
    let number = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    };
    let first = number("FOCAL_SEED_START", 1);
    // Frames written again, over every writer of every seed: a writer's
    // count starts at its open.
    let mut moved = 0u64;
    for seed in first..first + number("FOCAL_SEED_COUNT", 4) {
        let mut random = Seeded(seed);
        let dir = tempfile::tempdir().unwrap();
        let budget = memory();
        let open = |budget: &MemoryBudget| {
            SharedWal::open_with_budget(
                dir.path(),
                options(),
                WalWriterLimits::default(),
                budget.clone(),
            )
            .unwrap()
        };
        let mut shared = open(&budget);
        let mut held = leases(&shared);
        let mut model: Vec<Vec<Record>> = vec![Vec::new(); GROUPS];
        let mut serial = 0u64;
        let mut writer_moved = 0u64;
        for step in 0..500 {
            let at = random.below(GROUPS as u64) as usize;
            let log = at as u8 + 1;
            // A cut armed earlier and met by the writer's own cleaning
            // stops it between commands: the step reopens, nothing cut.
            let stopped = shared.identity().is_err();
            let choice = if stopped { 16 } else { random.below(100) };
            // A group's lease is given up and taken again now and then.
            if choice < 4 {
                held[at] = None;
                held[at] = Some(shared.lease(LogicalLogId([log; 16])).unwrap());
                assert_eq!(records(held[at].as_ref().unwrap()), model[at]);
                continue;
            }
            if choice < 8 {
                idle(&shared, random.below(2) == 0);
                continue;
            }
            if choice < 12 {
                let stats = settle(&shared);
                assert!(
                    !due(&stats, &options()),
                    "seed {seed} step {step}: {stats:?}"
                );
                assert_eq!(
                    stats.indexed_records,
                    model.iter().map(Vec::len).sum::<usize>(),
                    "seed {seed} step {step}"
                );
                writer_moved = writer_moved.max(stats.relocated_records);
                continue;
            }
            if choice < 16 {
                let point = POINTS[random.below(4) as usize];
                held[at]
                    .as_mut()
                    .unwrap()
                    .try_inject_fault_once(point)
                    .unwrap();
                continue;
            }
            let reopen = choice < 20;
            let mut after = model[at].clone();
            let result = if reopen {
                Ok(())
            } else if choice < 40 {
                // A checkpoint keeps from nothing to three records.
                let keep: Vec<Record> = (0..random.below(4))
                    .map(|_| {
                        serial += 1;
                        Record {
                            payload: vec![log; random.below(200) as usize],
                            ..snapshot(log, serial)
                        }
                    })
                    .collect();
                after = keep.clone();
                held[at]
                    .as_mut()
                    .unwrap()
                    .rewrite_checkpoint(&keep)
                    .map(|_| ())
            } else {
                let batch: Vec<Record> = (0..1 + random.below(6))
                    .map(|_| {
                        serial += 1;
                        Record {
                            payload: vec![log; random.below(200) as usize],
                            ..record(log, serial)
                        }
                    })
                    .collect();
                after.extend(batch.iter().cloned());
                held[at].as_mut().unwrap().append(&batch).map(|_| ())
            };
            // A cut armed earlier may fall after a commit's replies.
            let failed = result.is_err() || shared.identity().is_err();
            if result.is_ok() {
                model[at] = after.clone();
            }
            if let Ok(stats) = shared.stats() {
                writer_moved = writer_moved.max(stats.relocated_records);
            }
            if !failed && !reopen {
                assert_eq!(
                    records(held[at].as_ref().unwrap()),
                    model[at],
                    "seed {seed} step {step} log {log}"
                );
                continue;
            }
            moved += std::mem::take(&mut writer_moved);
            held.clear();
            drop(shared);
            assert_eq!(budget.stats().used, 0, "seed {seed} step {step}");
            shared = open(&budget);
            held = leases(&shared);
            for (other, lease) in held.iter().enumerate() {
                let replayed = records(lease.as_ref().unwrap());
                if other == at && replayed != model[at] {
                    // The cut command is there whole, or not at all.
                    assert_eq!(replayed, after, "seed {seed} step {step} log {log}");
                    model[at] = after.clone();
                } else {
                    assert_eq!(
                        replayed,
                        model[other],
                        "seed {seed} step {step} log {}",
                        other + 1
                    );
                }
            }
            assert_eq!(
                segments(dir.path()).first(),
                Some(&fence(dir.path()).base.segment),
                "seed {seed} step {step}"
            );
        }
        if let Ok(stats) = shared.stats() {
            writer_moved = writer_moved.max(stats.relocated_records);
        }
        moved += writer_moved;
    }
    // The histories are long enough for the base to have met live frames:
    // what the model checked includes groups whose frames were moved.
    assert!(moved > 0);
}
