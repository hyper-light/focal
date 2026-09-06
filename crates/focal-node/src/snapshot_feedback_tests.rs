use super::*;
fn message(index: u64) -> Message {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.to = 2;
    message.term = 3;
    message.mut_snapshot().mut_metadata().index = index;
    message
}
#[test]
fn snapshot_feedback_drop_replacement_and_retry_hold_exact_flight_and_charge() {
    let budget = MemoryBudget::new(32 * 1024, 0).unwrap();
    let mut feedback = SnapshotFeedback::default();
    let old = feedback.begin(&message(4), &budget).unwrap().unwrap();
    let new = feedback.begin(&message(4), &budget).unwrap().unwrap();
    assert!(old.send(SnapshotStatus::Failure).is_err());
    drop(new);
    assert_eq!(budget.stats().used, 4096);
    let mut attempts = Vec::new();
    assert_eq!(
        feedback.poll(3, |peer, term, index, status| {
            attempts.push((peer, term, index, status));
            Err("persistence pending")
        }),
        Err("persistence pending")
    );
    assert_eq!(budget.stats().used, 4096);
    feedback
        .poll(3, |peer, term, index, status| {
            attempts.push((peer, term, index, status));
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(attempts, vec![(2, 3, 4, SnapshotStatus::Failure); 2]);
    assert_eq!(budget.stats().used, 0);
    let sender = feedback.begin(&message(5), &budget).unwrap().unwrap();
    feedback
        .poll(4, |_, _, _, _| -> Result<(), ()> { panic!("old term") })
        .unwrap();
    assert!(sender.send(SnapshotStatus::Finish).is_err());
    assert_eq!(budget.stats().used, 0);
}
#[test]
fn failed_new_snapshot_admission_invalidates_prior_same_index_sender() {
    let budget = MemoryBudget::new(8192, 0).unwrap();
    let mut feedback = SnapshotFeedback::default();
    let old = feedback.begin(&message(4), &budget).unwrap().unwrap();
    // A different node budget models replacement admission after owner pressure.
    // The old receiver still must be invalidated before reporting new failure.
    let empty = MemoryBudget::new(4096, 0).unwrap();
    let pressure = empty
        .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)
        .unwrap()
        .commit();
    assert!(matches!(
        feedback.begin(&message(4), &empty),
        Err(SnapshotFeedbackError::Capacity)
    ));
    assert!(old.send(SnapshotStatus::Finish).is_err());
    assert_eq!(budget.stats().used, 0);
    drop(pressure);
    let current = feedback.begin(&message(4), &empty).unwrap().unwrap();
    current.send(SnapshotStatus::Finish).unwrap();
    feedback
        .poll(3, |peer, term, index, status| {
            assert_eq!(
                (peer, term, index, status),
                (2, 3, 4, SnapshotStatus::Finish)
            );
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(empty.stats().used, 0);
}
