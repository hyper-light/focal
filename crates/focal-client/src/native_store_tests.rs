use super::*;
use crate::operation_store::OperationIntent;
use focal_wire::{NativeOperationKind, NativeOutcomeCounts};

fn context() -> OperationContext {
    OperationContext {
        cluster: [7; 16],
        principal: ParticipantId::from_u128(3),
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
    }
}
fn intent(canonical: &'static [u8]) -> OperationIntent<'static> {
    OperationIntent {
        name: "claim.post",
        version: 2,
        canonical,
    }
}
/// A syntactically valid actor frame header carrying `key` for `ledger`.
pub(crate) fn frame(ledger: LedgerId, key: RequestKey, command: u8) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"FCNINPUT");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.push(1); // authored profile
    bytes.push(0); // actor namespace
    bytes.extend_from_slice(&ledger.tenant.0);
    bytes.extend_from_slice(&ledger.session.0);
    assert_eq!(bytes.len(), 44);
    bytes.extend_from_slice(&key.principal.0);
    bytes.extend_from_slice(&key.epoch.0.to_le_bytes());
    bytes.extend_from_slice(&key.id.0);
    bytes.push(command);
    bytes.extend_from_slice(b"body");
    bytes
}
fn prepared(context: &OperationContext, id: RequestId, command: u8) -> PreparedNativeRequest {
    let key = RequestKey {
        principal: context.principal,
        epoch: RequestEpoch(1),
        id,
    };
    PreparedNativeRequest {
        request: RequestEnvelope {
            protocol: NATIVE_PROTOCOL_VERSION,
            ledger: context.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: id,
            operation: Operation::Native {
                frame: frame(context.ledger, key, command),
            },
        },
        fingerprint: ContentHash([command.max(1); 32]),
        created: vec![NativeIdentity {
            kind: NativeIdentityKind::Claim,
            id: [9; 16],
        }],
    }
}
fn receipt(context: &OperationContext, id: RequestId, fingerprint: ContentHash) -> NativeReceipt {
    NativeReceipt {
        invocation: NativeInvocationRef::Request(RequestKey {
            principal: context.principal,
            epoch: RequestEpoch(1),
            id,
        }),
        sequence: SessionSeq(4),
        logical_time: 10,
        operation: NativeOperationKind::Post,
        intent: fingerprint,
        counts: NativeOutcomeCounts {
            created: 0,
            changed: 1,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 1,
        },
    }
}
fn ids(seed: u128) -> impl FnMut() -> Result<[u8; 16], InputError> {
    let mut next = seed;
    move || {
        next += 1;
        Ok(next.to_be_bytes())
    }
}

#[test]
fn references_round_trip_and_reject_foreign_prefixes_and_zero() {
    let id = NativeOperationId(RequestId::from_u128(0xabc));
    let text = id.to_string();
    assert_eq!(text, format!("n1:{:032x}", 0xabc));
    assert_eq!(text.parse::<NativeOperationId>().unwrap(), id);
    for bad in [
        "m1:00000000000000000000000000000abc",
        "n1:0",
        "n1:",
        &format!("n1:{:032x}", 0),
    ] {
        assert!(bad.parse::<NativeOperationId>().is_err(), "{bad}");
    }
}

#[test]
fn prepare_claims_an_identity_once_then_retry_returns_the_exact_frame_and_binds_receipts() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let store = NativeOperationStore::create(&path, NativeStoreLimits::default()).unwrap();
    assert!(NativeOperationStore::create(&path, NativeStoreLimits::default()).is_err());
    let context = context();
    let mut generator = ids(100);
    let mut expansions = 0;
    let operation = store
        .prepare(
            context,
            intent(b"{\"claim\":\"a\"}"),
            None,
            &mut generator,
            |id| {
                expansions += 1;
                Ok(prepared(&context, id, 23))
            },
        )
        .unwrap();
    assert_eq!(expansions, 1);
    assert_eq!(operation.id.request(), RequestId::from_u128(101));
    assert_eq!(operation.stage(), NativeStage::Pending);
    assert_eq!(operation.created.len(), 1);
    assert_eq!(store.outstanding().unwrap(), vec![operation.id]);
    assert_eq!(store.usage().unwrap().operations, 1);

    // Same identity and intent: no second expansion, identical request.
    let again = store
        .prepare(
            context,
            intent(b"{\"claim\":\"a\"}"),
            Some(operation.id),
            &mut generator,
            |_| panic!("must not expand a prepared operation"),
        )
        .unwrap();
    assert_eq!(again, operation);
    let reopened = NativeOperationStore::open(&path, NativeStoreLimits::default()).unwrap();
    assert_eq!(reopened.retry(operation.id, &context).unwrap(), operation);
    // A different intent under the claimed identity is refused, as is a
    // different context.
    assert!(matches!(
        store.prepare(
            context,
            intent(b"{\"claim\":\"b\"}"),
            Some(operation.id),
            &mut generator,
            |_| { panic!("no expansion") }
        ),
        Err(NativeStoreError::IntentConflict)
    ));
    let other = OperationContext {
        principal: ParticipantId::from_u128(4),
        ..context
    };
    assert!(matches!(
        store.retry(operation.id, &other),
        Err(NativeStoreError::ContextMismatch)
    ));
    assert!(matches!(
        store.retry(NativeOperationId(RequestId::from_u128(999)), &context),
        Err(NativeStoreError::MissingOperation)
    ));

    // Only a receipt proving this exact frame is recorded.
    let wrong_intent = receipt(&context, operation.id.request(), ContentHash([5; 32]));
    assert!(matches!(
        store.record_reply(
            operation.id,
            &context,
            &NativeMutationReply::Committed(wrong_intent)
        ),
        Err(NativeStoreError::ReceiptMismatch)
    ));
    let pending = NativeMutationReply::Pending(focal_wire::NativeTicket {
        key: operation.key(),
        intent: operation.fingerprint,
    });
    assert!(matches!(
        store.record_reply(operation.id, &context, &pending),
        Err(NativeStoreError::NotCommitted)
    ));
    assert!(matches!(
        store.record_delivered(operation.id, &context),
        Err(NativeStoreError::NotCommitted)
    ));
    let committed = receipt(&context, operation.id.request(), operation.fingerprint);
    assert_eq!(
        store
            .record_reply(
                operation.id,
                &context,
                &NativeMutationReply::Committed(committed)
            )
            .unwrap(),
        NativeStage::Completed
    );
    let completed = store.retry(operation.id, &context).unwrap();
    assert_eq!(completed.receipt, Some(committed));
    assert_eq!(completed.request, operation.request);
    // Committed but not yet reported: still listed, so a lost reply is found.
    assert!(!completed.delivered);
    assert_eq!(store.outstanding().unwrap(), vec![operation.id]);
    // A committed receipt is never replaced by a refusal, and re-recording
    // the identical receipt is harmless while a different one is not.
    let refusal = focal_wire::NativeRefusal {
        kind: focal_wire::NativeRefusalKind::Conflict,
        detail: "late".into(),
    };
    assert!(matches!(
        store.record_refusal(operation.id, &context, &refusal),
        Err(NativeStoreError::ReceiptMismatch)
    ));
    assert_eq!(
        store
            .record_reply(
                operation.id,
                &context,
                &NativeMutationReply::Committed(committed)
            )
            .unwrap(),
        NativeStage::Completed
    );
    let mut different = committed;
    different.sequence = SessionSeq(5);
    assert!(matches!(
        store.record_reply(
            operation.id,
            &context,
            &NativeMutationReply::Committed(different)
        ),
        Err(NativeStoreError::ReceiptMismatch)
    ));
    // Reported: no longer outstanding, still answered from its journal, and
    // retirable once a claim needs the slot.
    store.record_delivered(operation.id, &context).unwrap();
    let reported = store.retry(operation.id, &context).unwrap();
    assert!(reported.delivered && reported.receipt == Some(committed));
    assert_eq!(store.retired(operation.id).unwrap(), None);
    assert!(store.outstanding().unwrap().is_empty());
    assert_eq!(store.usage().unwrap().operations, 1);
    store.record_delivered(operation.id, &context).unwrap();
    assert!(matches!(
        store.record_refusal(operation.id, &context, &refusal),
        Err(NativeStoreError::ReceiptMismatch)
    ));
    // A capacity refusal admitted nothing: the frame stays journaled for an
    // explicit retry, no longer outstanding once the refusal was reported,
    // and a later commit of the same frame is recorded.
    let capacity = focal_wire::NativeRefusal {
        kind: focal_wire::NativeRefusalKind::Capacity,
        detail: "full".into(),
    };
    let refused = store
        .prepare(
            context,
            intent(b"{\"claim\":\"z\"}"),
            None,
            &mut generator,
            |id| Ok(prepared(&context, id, 22)),
        )
        .unwrap();
    assert_eq!(store.outstanding().unwrap(), vec![refused.id]);
    store
        .record_refusal(refused.id, &context, &capacity)
        .unwrap();
    assert!(store.outstanding().unwrap().is_empty());
    let reopened = store.retry(refused.id, &context).unwrap();
    assert_eq!(reopened.refusal, Some(capacity.clone()));
    assert!(reopened.receipt.is_none() && reopened.delivered);
    assert_eq!(reopened.request, refused.request);
    let committed_late = receipt(&context, refused.id.request(), refused.fingerprint);
    assert_eq!(
        store
            .record_reply(
                refused.id,
                &context,
                &NativeMutationReply::Committed(committed_late)
            )
            .unwrap(),
        NativeStage::Completed
    );
    assert_eq!(store.outstanding().unwrap(), vec![refused.id]);
    // A closed refusal answers the frame for good: reported, it is no
    // longer outstanding, still inspectable, and retirable.
    let closed = store
        .prepare(
            context,
            intent(b"{\"claim\":\"y\"}"),
            None,
            &mut generator,
            |id| Ok(prepared(&context, id, 21)),
        )
        .unwrap();
    store.record_refusal(closed.id, &context, &refusal).unwrap();
    let reported = store.retry(closed.id, &context).unwrap();
    assert!(reported.delivered && reported.refusal == Some(refusal.clone()));
    assert_eq!(store.retired(closed.id).unwrap(), None);
    assert_eq!(store.outstanding().unwrap(), vec![refused.id]);
    // A capacity refusal is no result to acknowledge: the frame waits.
    let waiting = store
        .prepare(
            context,
            intent(b"{\"claim\":\"x\"}"),
            None,
            &mut generator,
            |id| Ok(prepared(&context, id, 20)),
        )
        .unwrap();
    store
        .record_refusal(waiting.id, &context, &capacity)
        .unwrap();
    assert!(matches!(
        store.record_delivered(waiting.id, &context),
        Err(NativeStoreError::NotCommitted)
    ));
    assert_eq!(store.outstanding().unwrap(), vec![refused.id]);
}

/// The audit's F04: a reported operation retires when a claim needs its
/// slot — the one reported longest ago first — so the journal is bounded
/// by its capacity, not by the work ever done; a retired identity stays
/// taken under any intent, and the retired identities are bounded too.
#[test]
fn a_reported_operation_retires_when_a_claim_needs_its_slot_and_its_identity_stays_taken() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let limits = NativeStoreLimits {
        max_operations: 2,
        max_reserved_bytes: 16 * 1024 * 1024,
    };
    let store = NativeOperationStore::create(&path, limits).unwrap();
    let context = context();
    let directory =
        |id: NativeOperationId| path.join(format!("{:032x}", u128::from_be_bytes(id.request().0)));
    let mut generator = ids(500);
    let mut finished = Vec::new();
    // Five operations through a journal of two: each reported, each
    // making room for the next once the journal is full.
    for round in 0..5u8 {
        let operation = store
            .prepare(context, intent(b"{}"), None, &mut generator, |id| {
                Ok(prepared(&context, id, 10 + round))
            })
            .unwrap();
        let committed = receipt(&context, operation.id.request(), operation.fingerprint);
        store
            .record_reply(
                operation.id,
                &context,
                &NativeMutationReply::Committed(committed),
            )
            .unwrap();
        store.record_delivered(operation.id, &context).unwrap();
        assert_eq!(
            store.usage().unwrap().operations,
            u32::from(round).saturating_add(1).min(2)
        );
        assert!(directory(operation.id).exists());
        finished.push(operation.id);
    }
    // The first three retired, in order; two identities stay taken (the
    // journal's capacity), the oldest of the three gone; the last two are
    // reported and still answered.
    for (index, id) in finished.iter().enumerate() {
        assert_eq!(directory(*id).exists(), index >= 3, "{index}");
        assert_eq!(
            store.retired(*id).unwrap().is_some(),
            index == 1 || index == 2,
            "{index}"
        );
    }
    assert_eq!(store.retired_count().unwrap(), 2);
    assert!(matches!(
        store.retry(finished[0], &context),
        Err(NativeStoreError::MissingOperation)
    ));
    assert!(store.retry(finished[4], &context).unwrap().delivered);
    let taken = finished[2];
    assert!(matches!(
        store.retry(taken, &context),
        Err(NativeStoreError::Retired)
    ));
    for canonical in [&b"{}"[..], &b"{\"other\":1}"[..]] {
        assert!(matches!(
            store.prepare(
                context,
                intent(canonical),
                Some(taken),
                &mut generator,
                |_| { panic!("a retired identity is never expanded") }
            ),
            Err(NativeStoreError::Retired)
        ));
    }
    // A generated identity that is retired or live is passed over, and the
    // claim takes the room of the operation reported longest ago.
    let mut colliding = ids(501);
    let fresh = store
        .prepare(context, intent(b"{}"), None, &mut colliding, |id| {
            Ok(prepared(&context, id, 20))
        })
        .unwrap();
    assert_eq!(fresh.id.request(), RequestId::from_u128(506));
    assert!(!directory(finished[3]).exists());
    assert!(store.retired(finished[3]).unwrap().is_some());
    // Reopening keeps it all and sweeps nothing live.
    let reopened = NativeOperationStore::open(&path, limits).unwrap();
    assert_eq!(reopened.retired_count().unwrap(), 2);
    assert_eq!(reopened.outstanding().unwrap(), vec![fresh.id]);
    assert_eq!(reopened.usage().unwrap().operations, 2);
}

/// The audit's F05: a claim whose expansion fails is released with the
/// failure, and a claim that never became ready — no bytes ever left under
/// it — is swept when the store is opened; a retirement interrupted before
/// its directory left is completed the same way.
#[test]
fn a_failed_or_interrupted_claim_holds_no_slot() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let limits = NativeStoreLimits {
        max_operations: 1,
        max_reserved_bytes: 16 * 1024 * 1024,
    };
    let store = NativeOperationStore::create(&path, limits).unwrap();
    let context = context();
    let mut generator = ids(600);
    // Expansion fails: the claim is released, nothing is listed, the next
    // operation has the slot.
    assert!(matches!(
        store.prepare(context, intent(b"{}"), None, &mut generator, |_| {
            Err(NativeStoreError::Expansion(InputError::Capacity))
        }),
        Err(NativeStoreError::Expansion(InputError::Capacity))
    ));
    assert_eq!(store.usage().unwrap().operations, 0);
    assert!(store.outstanding().unwrap().is_empty());
    // A crash after the claim, and one after the frame but before ready:
    // the next open releases the slot, since nothing was sent.
    for fault in [Fault::Claimed, Fault::Prepared] {
        store.inject(fault);
        assert!(
            store
                .prepare(context, intent(b"{}"), None, &mut generator, |id| {
                    Ok(prepared(&context, id, 23))
                })
                .is_err()
        );
        assert_eq!(store.usage().unwrap().operations, 1, "{fault:?}");
        let reopened = NativeOperationStore::open(&path, limits).unwrap();
        assert_eq!(reopened.usage().unwrap().operations, 0, "{fault:?}");
        assert_eq!(reopened.retired_count().unwrap(), 0);
    }
    // A reported operation fills the journal; the next claim retires it,
    // and a crash between the retired identity's durability and its
    // directory's removal is completed at the next open.
    let operation = store
        .prepare(context, intent(b"{}"), None, &mut generator, |id| {
            Ok(prepared(&context, id, 23))
        })
        .unwrap();
    let component = format!("{:032x}", u128::from_be_bytes(operation.id.request().0));
    store
        .record_reply(
            operation.id,
            &context,
            &NativeMutationReply::Committed(receipt(
                &context,
                operation.id.request(),
                operation.fingerprint,
            )),
        )
        .unwrap();
    store.record_delivered(operation.id, &context).unwrap();
    store.inject(Fault::Retired);
    assert!(
        store
            .prepare(context, intent(b"{}"), None, &mut generator, |id| {
                Ok(prepared(&context, id, 22))
            })
            .is_err()
    );
    assert!(path.join(&component).exists());
    assert_eq!(store.usage().unwrap().operations, 0);
    assert!(store.retired(operation.id).unwrap().is_some());
    let reopened = NativeOperationStore::open(&path, limits).unwrap();
    assert!(!path.join(&component).exists());
    assert!(matches!(
        reopened.retry(operation.id, &context),
        Err(NativeStoreError::Retired)
    ));
    let next = reopened
        .prepare(context, intent(b"{}"), None, &mut generator, |id| {
            Ok(prepared(&context, id, 22))
        })
        .unwrap();
    assert_eq!(reopened.outstanding().unwrap(), vec![next.id]);
}

/// The audit's F06: every transition reads, judges and writes under one
/// hold of the lock, so whichever of two processes' words arrives second is
/// judged against the first's — a committed receipt is never overwritten by
/// a stale refusal, and a delivery never regresses.
#[test]
fn concurrent_transitions_never_lose_a_committed_receipt() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let limits = NativeStoreLimits {
        max_operations: 64,
        max_reserved_bytes: 1024 * 1024 * 1024,
    };
    let store = NativeOperationStore::create(&path, limits).unwrap();
    let context = context();
    let mut generator = ids(700);
    let refusal = focal_wire::NativeRefusal {
        kind: focal_wire::NativeRefusalKind::Conflict,
        detail: "stale".into(),
    };
    for round in 0..24u8 {
        let operation = store
            .prepare(context, intent(b"{}"), None, &mut generator, |id| {
                Ok(prepared(&context, id, 1 + round % 20))
            })
            .unwrap();
        let committed = receipt(&context, operation.id.request(), operation.fingerprint);
        let id = operation.id;
        let (a, b) = (
            NativeOperationStore::open(&path, limits).unwrap(),
            NativeOperationStore::open(&path, limits).unwrap(),
        );
        let refusal = refusal.clone();
        std::thread::scope(|scope| {
            let commit = scope.spawn(|| {
                let reply = NativeMutationReply::Committed(committed);
                let recorded = a.record_reply(id, &context, &reply);
                let delivered = a.record_delivered(id, &context);
                (recorded, delivered)
            });
            let refuse = scope.spawn(|| b.record_refusal(id, &context, &refusal));
            let (recorded, delivered) = commit.join().unwrap();
            let refused = refuse.join().unwrap();
            // The receipt is recorded and delivered whichever came first:
            // a refusal before it is replaced by the owner's receipt, one
            // after it is judged against the receipt and refused.
            assert!(
                recorded.is_ok() && delivered.is_ok(),
                "round {round}: {recorded:?} {delivered:?}"
            );
            assert!(
                matches!(refused, Ok(()) | Err(NativeStoreError::ReceiptMismatch)),
                "round {round}: {refused:?}"
            );
            let final_state = store.retry(id, &context).unwrap();
            assert_eq!(final_state.receipt, Some(committed), "round {round}");
            assert!(
                final_state.delivered && final_state.refusal.is_none(),
                "round {round}"
            );
        });
    }
}

#[test]
fn expansion_must_carry_the_claimed_identity_and_the_native_profile() {
    let root = tempfile::tempdir().unwrap();
    let store =
        NativeOperationStore::create(root.path().join("native"), NativeStoreLimits::default())
            .unwrap();
    let context = context();
    let mut generator = ids(200);
    for mutate in [
        (|value: &mut PreparedNativeRequest| value.request.request_id = RequestId::from_u128(77))
            as fn(&mut PreparedNativeRequest),
        |value| value.request.protocol = focal_wire::PROTOCOL_VERSION,
        |value| value.request.request_epoch = RequestEpoch(2),
        |value| value.fingerprint = ContentHash([0; 32]),
        |value| value.request.operation = Operation::Summary,
        |value| {
            let Operation::Native { frame } = &mut value.request.operation else {
                unreachable!()
            };
            frame[44] ^= 1; // another principal in the frame header
        },
    ] {
        let result = store.prepare(context, intent(b"{}"), None, &mut generator, |id| {
            let mut value = prepared(&context, id, 23);
            mutate(&mut value);
            Ok(value)
        });
        assert!(matches!(result, Err(NativeStoreError::InvalidRequest)));
    }
    // Refused expansions leave a claimed identity that can be completed
    // later under the same intent, never silently adopted under another.
    assert!(store.outstanding().unwrap().is_empty());
    let claimed = NativeOperationId(RequestId::from_u128(201));
    assert!(matches!(
        store.retry(claimed, &context),
        Err(NativeStoreError::Incomplete)
    ));
    let completed = store
        .prepare(
            context,
            intent(b"{}"),
            Some(claimed),
            &mut generator,
            |id| Ok(prepared(&context, id, 23)),
        )
        .unwrap();
    assert_eq!(completed.id, claimed);
    assert_eq!(store.usage().unwrap().operations, 6);
}

#[test]
fn interrupted_initialization_resumes_without_minting_a_second_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let context = context();
    for fault in [Fault::Claimed, Fault::Prepared, Fault::Ready] {
        let _ = std::fs::remove_dir_all(&path);
        let store = NativeOperationStore::create(&path, NativeStoreLimits::default()).unwrap();
        store.inject(fault);
        let mut generator = ids(300);
        let failed = store.prepare(context, intent(b"{}"), None, &mut generator, |id| {
            Ok(prepared(&context, id, 23))
        });
        assert!(failed.is_err(), "{fault:?}");
        let claimed = NativeOperationId(RequestId::from_u128(301));
        let recovered = store
            .prepare(
                context,
                intent(b"{}"),
                Some(claimed),
                &mut generator,
                |id| {
                    assert_eq!(fault, Fault::Claimed, "{fault:?} must not expand again");
                    Ok(prepared(&context, id, 23))
                },
            )
            .unwrap();
        assert_eq!(recovered.id, claimed);
        assert_eq!(recovered.stage(), NativeStage::Pending);
        assert_eq!(store.retry(claimed, &context).unwrap(), recovered);
        assert_eq!(store.usage().unwrap().operations, 1);
    }
}

#[test]
fn capacity_and_limits_are_enforced_and_stored_with_the_catalogue() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("native");
    let limits = NativeStoreLimits {
        max_operations: 1,
        max_reserved_bytes: 16 * 1024 * 1024,
    };
    let store = NativeOperationStore::create(&path, limits).unwrap();
    let context = context();
    let mut generator = ids(400);
    store
        .prepare(context, intent(b"{}"), None, &mut generator, |id| {
            Ok(prepared(&context, id, 23))
        })
        .unwrap();
    assert!(matches!(
        store.prepare(context, intent(b"{}"), None, &mut generator, |id| {
            Ok(prepared(&context, id, 23))
        }),
        Err(NativeStoreError::Capacity)
    ));
    assert!(matches!(
        NativeOperationStore::open(&path, NativeStoreLimits::default()),
        Err(NativeStoreError::Store(StoreError::LimitsMismatch))
    ));
    assert!(
        NativeOperationStore::create(
            root.path().join("tiny"),
            NativeStoreLimits {
                max_operations: 0,
                ..limits
            }
        )
        .is_err()
    );
}
