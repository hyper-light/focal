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
    store.record_delivered(operation.id, &context).unwrap();
    assert!(store.retry(operation.id, &context).unwrap().delivered);
    assert!(store.outstanding().unwrap().is_empty());
    store.record_delivered(operation.id, &context).unwrap();
    // A committed receipt is never replaced by a refusal.
    let refusal = focal_wire::NativeRefusal {
        kind: focal_wire::NativeRefusalKind::Conflict,
        detail: "late".into(),
    };
    assert!(matches!(
        store.record_refusal(operation.id, &context, &refusal),
        Err(NativeStoreError::ReceiptMismatch)
    ));
    // A refused frame stays journaled for an explicit retry but is no longer
    // outstanding once the refusal was reported.
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
        .record_refusal(refused.id, &context, &refusal)
        .unwrap();
    assert!(store.outstanding().unwrap().is_empty());
    let reopened = store.retry(refused.id, &context).unwrap();
    assert_eq!(reopened.refusal, Some(refusal));
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
    // Re-recording the identical receipt is harmless; a different one is not.
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
