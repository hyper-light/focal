//! Actor paths use actual claim, receipt, supersession and timer histories.
use super::*;
use focal_model::lifecycle::succession::CorrectionKind;

fn command(actor: ParticipantId, id: u128, command: NativeCommand) -> NativeInput {
    NativeInput {
        request: request(actor, id),
        command,
    }
}
fn register(expected: Binding, receipt: Option<ReceiptFence>, id: u128) -> NativeCommand {
    NativeCommand::RegisterMonitor {
        expected,
        receipt,
        id: MonitorId::from_u128(id),
        roots: vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
        deadline: timer(1, id, 100).deadline,
    }
}
fn candidate(
    owner: &mut NativeOwner,
    input: NativeInput,
    time: u64,
) -> (NativeCandidate, NativeOutcome) {
    match owner
        .prepare(context(input.request.principal, time), input, None)
        .unwrap()
    {
        NativeStaging::Prepared { candidate, outcome } => (candidate, outcome),
        other => panic!("fresh monitor command: {other:?}"),
    }
}
fn refused(owner: &mut NativeOwner, input: NativeInput, time: u64) {
    let sequence = owner.effective().sequence();
    let pending = owner.pending_len();
    assert!(
        owner
            .prepare(context(input.request.principal, time), input, None)
            .is_err()
    );
    assert_eq!(owner.effective().sequence(), sequence);
    assert_eq!(owner.pending_len(), pending);
}

#[test]
fn registration_requires_claimant_current_receipt_and_current_binding_but_exact_retries_survive() {
    let core = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    let id = ClaimId::from_u128(1);
    let before_receipt = owner.effective().claim(id).unwrap().binding();
    let (acquisition, _) = candidate(
        &mut owner,
        command(
            SUBJECT,
            80_001,
            NativeCommand::AcquireReceipt {
                expected: before_receipt,
                receipt: ReceiptId::from_u128(80_001),
            },
        ),
        25,
    );
    owner.publish_after_durable(acquisition).unwrap();
    let source = owner.committed().claim(id).unwrap();
    let expected = source.binding();
    let receipt = source.receipt().unwrap().fence;
    let original_status = source.status();
    refused(
        &mut owner,
        command(SUBJECT, 80_002, register(expected, Some(receipt), 1)),
        30,
    );
    refused(
        &mut owner,
        command(ISSUER, 80_003, register(expected, None, 1)),
        30,
    );
    refused(
        &mut owner,
        command(
            ISSUER,
            80_004,
            register(
                expected,
                Some(ReceiptFence {
                    epoch: receipt.epoch + 1,
                    ..receipt
                }),
                1,
            ),
        ),
        30,
    );
    refused(
        &mut owner,
        command(ISSUER, 80_005, register(before_receipt, Some(receipt), 1)),
        30,
    );
    assert!(
        owner
            .prepare(
                NativeContext {
                    principal: Principal::Node(ISSUER),
                    logical_time: 30
                },
                command(ISSUER, 80_006, register(expected, Some(receipt), 1)),
                None,
            )
            .is_err()
    );
    assert!(
        owner
            .committed()
            .claim(id)
            .unwrap()
            .scopes()
            .monitor(MonitorId::from_u128(1))
            .is_none()
    );
    let (registered, outcome) = candidate(
        &mut owner,
        command(ISSUER, 80_007, register(expected, Some(receipt), 1)),
        30,
    );
    assert_eq!(outcome.operation, NativeOperation::RegisterMonitor);
    let after = owner.effective().claim(id).unwrap();
    assert_eq!(after.binding(), expected.next().unwrap());
    assert_eq!(after.status(), original_status);
    assert_eq!(after.receipt().unwrap().fence, receipt);
    assert!(
        after
            .scopes()
            .monitor(MonitorId::from_u128(1))
            .unwrap()
            .active()
    );
    assert_eq!(
        (outcome.responses, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), command(ISSUER, 80_007, register(expected, Some(receipt), 1)), None).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: Some(ticket) } if actual == outcome && ticket == registered)
    );
    refused(
        &mut owner,
        command(ISSUER, 80_008, register(expected, Some(receipt), 2)),
        31,
    );
    owner.publish_after_durable(registered).unwrap();
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), command(ISSUER, 80_007, register(expected, Some(receipt), 1)), None).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
}

#[test]
fn cancelling_a_terminal_owners_monitor_preserves_failure_and_requires_separate_owner_release() {
    let core = authored_posted(&[
        (1, 500, &[(ValidationMode::Observe, false)], &[]),
        (2, 500, &[], &[]),
    ]);
    let mut owner = NativeOwner::new(core).unwrap();
    let input = timer(1, 1, 100);
    register_timer(&mut owner, input, 2, 25);
    let live = owner.effective().claim(input.claim).unwrap().binding();
    refused(
        &mut owner,
        command(
            ISSUER,
            81_001,
            NativeCommand::CancelMonitor {
                expected: live,
                receipt: None,
                id: input.monitor,
            },
        ),
        26,
    );
    begin_claim(&mut owner, 1, 1, 31);
    let (expired, expired_outcome) = monitor_fire(&mut owner, input, 100);
    owner.publish_after_durable(expired).unwrap();
    let before = owner
        .committed()
        .claim(input.claim)
        .unwrap()
        .try_copy(usize::MAX)
        .unwrap();
    let check = *owner.committed().evaluation(claim_key(1, 1)).unwrap();
    assert_eq!(before.status(), ClaimStatus::Expired);
    assert!(before.scopes().monitor(input.monitor).unwrap().active());
    let expected = before.binding();
    refused(
        &mut owner,
        command(
            SUBJECT,
            81_002,
            NativeCommand::CancelMonitor {
                expected,
                receipt: None,
                id: input.monitor,
            },
        ),
        101,
    );
    let (cancelled, outcome) = candidate(
        &mut owner,
        command(
            ISSUER,
            81_003,
            NativeCommand::CancelMonitor {
                expected,
                receipt: None,
                id: input.monitor,
            },
        ),
        101,
    );
    let after = owner.effective().claim(input.claim).unwrap();
    assert_eq!(after.status(), before.status());
    assert_eq!(after.terminal_cut(), before.terminal_cut());
    assert_eq!(after.local_sealed_at(), before.local_sealed_at());
    assert_eq!(after.receipt(), before.receipt());
    assert_eq!(after.response_count(), before.response_count());
    assert!(!after.released());
    let scope = after.scopes().monitor(input.monitor).unwrap();
    let original_scope = before.scopes().monitor(input.monitor).unwrap();
    assert_eq!(scope.roots(), original_scope.roots());
    assert_eq!(scope.deadline(), original_scope.deadline());
    assert_eq!(scope.registered(), original_scope.registered());
    assert!(scope.release_cut().is_none());
    let cancellation = scope.cancellation().unwrap();
    assert_eq!(cancellation.terminal, expired_outcome.sequence);
    assert_eq!(cancellation.cut.position, outcome.sequence);
    assert_eq!(cancellation.cut.cause, outcome.intent);
    assert_eq!(
        owner.effective().evaluation(claim_key(1, 1)).unwrap(),
        &check
    );
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(2))
            .unwrap()
            .status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        (outcome.responses, outcome.artifacts, outcome.results),
        (0, 0, 0)
    );
    assert_eq!(owner.committed().claim(input.claim).unwrap(), &before);
    owner.publish_after_durable(cancelled).unwrap();
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), command(ISSUER, 81_003, NativeCommand::CancelMonitor { expected, receipt: None, id: input.monitor }), None).unwrap(),
        NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
    let expected = owner.committed().claim(input.claim).unwrap().binding();
    let (released, _) = candidate(
        &mut owner,
        command(ISSUER, 81_004, NativeCommand::ReleaseScope { expected }),
        102,
    );
    owner.publish_after_durable(released).unwrap();
    let released = owner.committed().claim(input.claim).unwrap();
    assert!(released.released());
    assert_eq!(released.status(), before.status());
    assert_eq!(released.terminal_cut(), before.terminal_cut());
    assert_eq!(
        released
            .scopes()
            .monitor(input.monitor)
            .unwrap()
            .cancellation(),
        Some(cancellation)
    );
}

#[test]
fn rebinding_requires_real_supersession_and_moves_only_the_named_monitor_roots() {
    let core = authored_posted(&[(1, 500, &[], &[]), (2, 500, &[], &[]), (4, 500, &[], &[])]);
    let mut owner = NativeOwner::new(core).unwrap();
    let owner_id = ClaimId::from_u128(2);
    let monitor = MonitorId::from_u128(3);
    let expected = owner.effective().claim(owner_id).unwrap().binding();
    let (registered, _) = candidate(
        &mut owner,
        command(
            ISSUER,
            82_001,
            NativeCommand::RegisterMonitor {
                expected,
                receipt: None,
                id: monitor,
                roots: vec![WaitPredicate::Satisfied(ClaimId::from_u128(1))],
                deadline: timer(2, 3, 200).deadline,
            },
        ),
        25,
    );
    owner.publish_after_durable(registered).unwrap();
    let before = owner
        .committed()
        .claim(owner_id)
        .unwrap()
        .try_copy(usize::MAX)
        .unwrap();
    let predecessor = owner
        .committed()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let unrelated = owner
        .committed()
        .claim(ClaimId::from_u128(4))
        .unwrap()
        .binding();
    refused(
        &mut owner,
        command(
            ISSUER,
            82_002,
            NativeCommand::RebindMonitor {
                expected: before.binding(),
                receipt: None,
                id: monitor,
                predecessor,
                successor: unrelated,
            },
        ),
        26,
    );
    let (supersession, _) = candidate(
        &mut owner,
        creation(82_003, 3, &[], Some(CorrectionKind::Supersedes)),
        30,
    );
    owner.publish_after_durable(supersession).unwrap();
    let actual_predecessor = owner
        .committed()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .binding();
    let successor = owner
        .committed()
        .claim(ClaimId::from_u128(3))
        .unwrap()
        .binding();
    assert_eq!(
        owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Superseded
    );
    assert_eq!(owner.committed().claim(owner_id).unwrap(), &before);
    refused(
        &mut owner,
        command(
            ISSUER,
            82_004,
            NativeCommand::RebindMonitor {
                expected: before.binding(),
                receipt: None,
                id: monitor,
                predecessor,
                successor,
            },
        ),
        31,
    );
    let (rebound, outcome) = candidate(
        &mut owner,
        command(
            ISSUER,
            82_005,
            NativeCommand::RebindMonitor {
                expected: before.binding(),
                receipt: None,
                id: monitor,
                predecessor: actual_predecessor,
                successor,
            },
        ),
        31,
    );
    let after = owner.effective().claim(owner_id).unwrap();
    assert_eq!(after.status(), before.status());
    assert_eq!(after.graph(), before.graph());
    assert_eq!(after.lineage(), before.lineage());
    assert_eq!(after.receipt(), before.receipt());
    let scope = after.scopes().monitor(monitor).unwrap();
    assert!(scope.active());
    assert_eq!(
        scope.roots(),
        &[WaitPredicate::Satisfied(ClaimId::from_u128(3))]
    );
    assert_eq!(
        scope.deadline(),
        before.scopes().monitor(monitor).unwrap().deadline()
    );
    assert_eq!(
        scope.registered(),
        before.scopes().monitor(monitor).unwrap().registered()
    );
    let change = scope.last_rebinding().unwrap();
    assert_eq!(change.predecessor, ClaimId::from_u128(1));
    assert_eq!(change.successor, ClaimId::from_u128(3));
    assert_eq!(change.cut.position, outcome.sequence);
    assert_eq!(change.cut.cause, outcome.intent);
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .binding(),
        actual_predecessor
    );
    assert_eq!(
        owner
            .effective()
            .claim(ClaimId::from_u128(3))
            .unwrap()
            .binding(),
        successor
    );
    assert_eq!(owner.committed().claim(owner_id).unwrap(), &before);
    owner.publish_after_durable(rebound).unwrap();
    assert!(
        matches!(owner.prepare(context(ISSUER, 0), command(ISSUER, 82_005, NativeCommand::RebindMonitor {
        expected: before.binding(), receipt: None, id: monitor, predecessor: actual_predecessor, successor,
    }), None).unwrap(), NativeStaging::Existing { outcome: actual, candidate: None } if actual == outcome)
    );
}
