use super::*;
use crate::Operation;
use focal_wire::ManagedOperation;
use std::cell::Cell;

fn context() -> OperationContext {
    OperationContext {
        cluster: [1; 16],
        principal: ParticipantId::from_u128(2),
        ledger: LedgerId {
            tenant: TenantId::from_u128(3),
            session: SessionId::from_u128(4),
        },
    }
}
fn limits() -> ManagedStoreLimits {
    ManagedStoreLimits {
        window: 2,
        ..ManagedStoreLimits::default()
    }
}
fn registration() -> RequestStreamControlInput {
    let c = context();
    RequestStreamControlInput {
        cluster: c.cluster,
        ledger: c.ledger,
        principal: c.principal,
        id: RequestId::from_u128(5),
        command: RequestStreamCommand::Register {
            slot: 0,
            expected_generation: 0,
            owner: RequestId::from_u128(6),
            window: limits().window,
        },
    }
}
fn receipt_for(
    input: &RequestStreamControlInput,
    index: u64,
    outcome: RequestStreamControlOutcome,
) -> RequestStreamControlReceipt {
    RequestStreamControlReceipt {
        cluster: input.cluster,
        ledger: input.ledger,
        principal: input.principal,
        id: input.id,
        intent_hash: input.intent_hash().unwrap(),
        raft_index: index,
        outcome,
    }
}
fn create(root: &Path) -> ManagedOperationStore {
    let store = ManagedOperationStore::create(root, context(), limits(), registration()).unwrap();
    let stream = store.status().unwrap().stream;
    store
        .record_registration(receipt_for(
            &registration(),
            10,
            RequestStreamControlOutcome::Registered(RequestStreamState::Active {
                stream,
                owner: RequestId::from_u128(6),
                revision: 1,
                window: limits().window,
                acknowledged_through: 0,
            }),
        ))
        .unwrap();
    store
}
fn intent() -> OperationIntent<'static> {
    OperationIntent {
        name: "claim.post",
        version: 1,
        canonical: br#"{"claim":"00000000000000000000000000000001"}"#,
    }
}
fn request(key: ManagedRequestKey) -> RequestEnvelope {
    RequestEnvelope {
        protocol: focal_wire::MANAGED_PROTOCOL_VERSION,
        ledger: key.stream.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: key.id,
        operation: Operation::Managed {
            key,
            operation: ManagedOperation::Submit {
                expected_revision: None,
                command: Command::PostClaim {
                    claim: ClaimId::from_u128(1),
                },
            },
        },
    }
}
fn receipt(request: &RequestEnvelope, index: u64) -> ManagedReceipt {
    let (key, _, intent_hash) = focal_wire::managed_request_identity(request).unwrap();
    ManagedReceipt {
        key,
        sequence: SessionSeq(1),
        raft_index: index,
        intent_hash,
        outcome: ManagedReceiptOutcome::Domain(CommandResult::Noop),
    }
}
fn complete(store: &ManagedOperationStore, id: ManagedOperationId, index: u64) -> ManagedReceipt {
    let prepared = store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    let receipt = receipt(&prepared.request, index);
    store.record_receipt(id, &receipt).unwrap();
    receipt
}
fn ack(store: &ManagedOperationStore, control_id: u128, index: u64) {
    let input = store
        .prepare_acknowledgment(RequestId::from_u128(control_id), limits().window)
        .unwrap();
    let RequestStreamCommand::Acknowledge {
        stream,
        expected_revision,
        through,
        ..
    } = &input.command
    else {
        panic!("ack");
    };
    let receipt = receipt_for(
        &input,
        index,
        RequestStreamControlOutcome::Acknowledged {
            stream: *stream,
            revision: expected_revision + 1,
            through: *through,
        },
    );
    store.record_control(receipt).unwrap();
}

#[test]
fn explicit_namespace_round_trips_and_rejects_aliases() {
    let expected = "m1:00000000:0000000000000001:0000000000000002:00000000000000000000000000000003";
    let id: ManagedOperationId = expected.parse().unwrap();
    assert_eq!(id.to_string(), expected);
    assert_eq!(id.key(context()).ordinal, 2);
    assert_eq!(ManagedOperationId::from_key(id.key(context())).unwrap(), id);
    for text in [
        "00000000000000000000000000000003",
        "m2:0:1:2:3",
        "m1:00000000:0000000000000000:0000000000000002:00000000000000000000000000000003",
        "m1:00000000:0000000000000001:0000000000000000:00000000000000000000000000000003",
        "m1:00000000:0000000000000001:0000000000000002:0000000000000000000000000000000A",
    ] {
        assert!(matches!(
            text.parse::<ManagedOperationId>(),
            Err(ManagedStoreError::InvalidId)
        ));
    }
}

#[test]
fn registration_reservations_and_intent_survive_reopen_without_expanding_again() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let store = ManagedOperationStore::create(&root, context(), limits(), registration()).unwrap();
    assert!(matches!(
        store.reserve(RequestId::from_u128(20)),
        Err(ManagedStoreError::NotRegistered)
    ));
    assert_eq!(store.registration().unwrap(), registration());
    drop(store);
    let store = ManagedOperationStore::open(&root, context(), limits()).unwrap();
    let stream = store.status().unwrap().stream;
    let registered = receipt_for(
        &registration(),
        10,
        RequestStreamControlOutcome::Registered(RequestStreamState::Active {
            stream,
            owner: RequestId::from_u128(6),
            revision: 1,
            window: limits().window,
            acknowledged_through: 0,
        }),
    );
    store.record_registration(registered.clone()).unwrap();
    store.record_registration(registered).unwrap();
    let id = store.reserve(RequestId::from_u128(20)).unwrap();
    assert_eq!(store.outstanding().unwrap(), vec![id]);
    let original = store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    drop(store);
    let store = ManagedOperationStore::open(&root, context(), limits()).unwrap();
    let saved = store
        .prepare(id, intent(), |_| panic!("retry expanded"))
        .unwrap();
    assert_eq!(saved.request, original.request);
    let wrong = OperationIntent {
        canonical: b"different",
        ..intent()
    };
    assert!(matches!(
        store.prepare(id, wrong, |_| panic!("conflict expanded")),
        Err(ManagedStoreError::Conflict)
    ));
}

#[test]
fn ack_retires_only_contiguous_results_and_reclaims_capacity_without_id_reuse() {
    let temp = tempfile::tempdir().unwrap();
    let store = create(&temp.path().join("managed"));
    let first = store.reserve(RequestId::from_u128(20)).unwrap();
    let second = store.reserve(RequestId::from_u128(21)).unwrap();
    complete(&store, second, 12);
    assert!(matches!(
        store.prepare_acknowledgment(RequestId::from_u128(30), 2),
        Err(ManagedStoreError::Unresolved)
    ));
    assert!(matches!(
        store.reserve(RequestId::from_u128(22)),
        Err(ManagedStoreError::Capacity)
    ));
    complete(&store, first, 11);
    ack(&store, 30, 13);
    let third = store.reserve(RequestId::from_u128(22)).unwrap();
    assert_eq!(third.key(context()).ordinal, 3);
    assert!(!store.root().join(prepared_name(1)).exists());
    assert!(!store.root().join(receipt_name(2)).exists());
    let called = Cell::new(false);
    assert!(matches!(
        store.prepare(first, intent(), |_| {
            called.set(true);
            Ok(request(first.key(context())))
        }),
        Err(ManagedStoreError::Retired)
    ));
    assert!(!called.get());
    assert!(matches!(
        store.request(second),
        Err(ManagedStoreError::Retired)
    ));
    drop(store);
    let store =
        ManagedOperationStore::open(temp.path().join("managed"), context(), limits()).unwrap();
    assert!(matches!(
        store.prepare(first, intent(), |_| panic!("retired ID expanded")),
        Err(ManagedStoreError::Retired)
    ));
    assert_eq!(store.outstanding().unwrap(), vec![third]);
}

#[test]
fn unknown_seal_ack_and_close_replies_preserve_exact_controls_and_gap_identity() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let store = create(&root);
    let gap = store.reserve(RequestId::from_u128(20)).unwrap();
    let control = store.prepare_seal(gap, RequestId::from_u128(30)).unwrap();
    assert!(matches!(
        store.prepare(gap, intent(), |_| panic!("sealed gap expanded")),
        Err(ManagedStoreError::Stopped)
    ));
    drop(store);
    let store = ManagedOperationStore::open(&root, context(), limits()).unwrap();
    assert_eq!(store.pending_control().unwrap(), Some(control.clone()));
    assert!(matches!(
        store.prepare_acknowledgment(RequestId::from_u128(31), 2),
        Err(ManagedStoreError::ControlPending)
    ));
    let RequestStreamCommand::Seal {
        key,
        family,
        intent_hash,
        ..
    } = &control.command
    else {
        panic!("seal");
    };
    let outcome = ManagedReceipt {
        key: *key,
        sequence: SessionSeq(0),
        raft_index: 11,
        intent_hash: *intent_hash,
        outcome: ManagedReceiptOutcome::Sealed { family: *family },
    };
    let sealed = receipt_for(
        &control,
        11,
        RequestStreamControlOutcome::Sealed(Box::new(outcome.clone())),
    );
    store.record_control(sealed.clone()).unwrap();
    store.record_control(sealed).unwrap();
    assert_eq!(store.receipt(gap).unwrap(), Some(outcome));
    assert!(matches!(
        store.prepare_close(RequestId::from_u128(32)),
        Err(ManagedStoreError::Unresolved)
    ));
    assert_eq!(store.stop_issuance().unwrap(), 1);
    assert!(matches!(
        store.reserve(RequestId::from_u128(22)),
        Err(ManagedStoreError::Stopped)
    ));
    ack(&store, 31, 12);
    let control = store.prepare_close(RequestId::from_u128(32)).unwrap();
    drop(store);
    let store = ManagedOperationStore::open(&root, context(), limits()).unwrap();
    assert_eq!(store.pending_control().unwrap(), Some(control.clone()));
    let stream = store.status().unwrap().stream;
    let closed = receipt_for(
        &control,
        13,
        RequestStreamControlOutcome::Closed {
            stream,
            vacant_generation: stream.generation,
        },
    );
    store.record_control(closed).unwrap();
    assert!(store.status().unwrap().closed);
    assert!(matches!(
        store.receipt(gap),
        Err(ManagedStoreError::Retired)
    ));
}

#[test]
fn receipt_scope_hash_family_and_repeated_outcome_are_bound() {
    let temp = tempfile::tempdir().unwrap();
    let store = create(&temp.path().join("managed"));
    let id = store.reserve(RequestId::from_u128(20)).unwrap();
    let prepared = store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    let original = receipt(&prepared.request, 11);
    for change in 0..5 {
        let mut wrong = original.clone();
        match change {
            0 => wrong.intent_hash.0[0] ^= 1,
            1 => wrong.key.stream.principal = ParticipantId::from_u128(99),
            2 => wrong.key.ordinal += 1,
            3 => wrong.raft_index = 0,
            _ => {
                wrong.outcome = ManagedReceiptOutcome::Cursor {
                    revision: 1,
                    floor: SessionSeq(0),
                    record: None,
                }
            }
        }
        assert!(matches!(
            store.record_receipt(id, &wrong),
            Err(ManagedStoreError::ReceiptMismatch)
        ));
    }
    store.record_receipt(id, &original).unwrap();
    let mut changed = original.clone();
    changed.sequence = SessionSeq(2);
    assert!(matches!(
        store.record_receipt(id, &changed),
        Err(ManagedStoreError::ReceiptMismatch)
    ));
    assert_eq!(store.receipt(id).unwrap(), Some(original));
}

#[test]
fn missing_initialized_or_prepared_state_fails_closed_and_another_store_cannot_adopt_id() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let store = create(&root);
    let id = store.reserve(RequestId::from_u128(20)).unwrap();
    store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    let other = create(&temp.path().join("other"));
    assert!(matches!(
        other.prepare(id, intent(), |_| panic!("foreign missing ID expanded")),
        Err(ManagedStoreError::Missing)
    ));
    std::fs::remove_file(root.join(prepared_name(1))).unwrap();
    assert!(matches!(
        store.reserve(RequestId::from_u128(21)),
        Err(ManagedStoreError::Corrupt)
    ));
    assert!(ManagedOperationStore::open(&root, context(), limits()).is_err());
    assert!(ManagedOperationStore::create(&root, context(), limits(), registration()).is_err());
    std::fs::remove_file(root.join(STATE)).unwrap();
    assert!(ManagedOperationStore::open(&root, context(), limits()).is_err());
}

#[test]
fn independently_opened_handles_serialize_allocation_and_release_lock_before_network() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let one = create(&root);
    let two = ManagedOperationStore::open(&root, context(), limits()).unwrap();
    let first = one.reserve(RequestId::from_u128(20)).unwrap();
    // The first request may remain unknown while another owner reserves work.
    let _waiting_request = one
        .prepare(first, intent(), |key| Ok(request(key)))
        .unwrap();
    let second = two.reserve(RequestId::from_u128(21)).unwrap();
    assert_ne!(first.key(context()).ordinal, second.key(context()).ordinal);
    let lock = Directory::open_managed(&root).unwrap();
    assert!(matches!(
        one.status(),
        Err(ManagedStoreError::Store(StoreError::Locked))
    ));
    drop(lock);
    assert_eq!(one.outstanding().unwrap(), vec![first, second]);
}

#[test]
fn prepared_and_receipt_publication_crash_cuts_recover_without_expansion() {
    let temp = tempfile::tempdir().unwrap();
    let store = create(&temp.path().join("managed"));
    let id = store.reserve(RequestId::from_u128(20)).unwrap();
    let original = store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    // Cut after immutable request publication but before its catalogue flag.
    let (directory, mut state) = store.load().unwrap();
    state.entries[0].prepared = false;
    store.save(&directory, &state).unwrap();
    drop(directory);
    let adopted = store
        .prepare(id, intent(), |_| panic!("published request re-expanded"))
        .unwrap();
    assert_eq!(adopted.request, original.request);
    let receipt = receipt(&adopted.request, 11);
    store.record_receipt(id, &receipt).unwrap();
    // Cut after receipt fsync but before its catalogue commitment. Reading the
    // complete original frame re-establishes durability before creating an ACK.
    let (directory, mut state) = store.load().unwrap();
    state.entries[0].receipt = None;
    store.save(&directory, &state).unwrap();
    drop(directory);
    assert_eq!(store.receipt(id).unwrap(), Some(receipt));
    ack(&store, 30, 12);
    // ACK publication precedes deletion; reopening resumes the bounded cleanup.
    assert!(store.root().join(prepared_name(1)).exists());
    assert!(store.root().join(receipt_name(1)).exists());
    let reopened = ManagedOperationStore::open(store.root(), context(), limits()).unwrap();
    assert!(matches!(
        reopened.request(id),
        Err(ManagedStoreError::Retired)
    ));
    assert!(!store.root().join(prepared_name(1)).exists());
    assert!(!store.root().join(receipt_name(1)).exists());
}

#[test]
fn cursor_and_domain_share_the_ack_manifest_without_acknowledging_delta_consumption() {
    let temp = tempfile::tempdir().unwrap();
    let store = create(&temp.path().join("managed"));
    let domain = store.reserve(RequestId::from_u128(20)).unwrap();
    let cursor = store.reserve(RequestId::from_u128(21)).unwrap();
    let domain_receipt = complete(&store, domain, 11);
    let prepared = store
        .prepare(
            cursor,
            OperationIntent {
                name: "stream.open",
                canonical: b"open",
                version: 1,
            },
            |key| {
                let mut request = request(key);
                request.operation = Operation::Managed {
                    key,
                    operation: ManagedOperation::Cursor(crate::StreamRequest::Open {
                        consumer: crate::ConsumerId([4; 16]),
                        filter: crate::DeltaFilter::All,
                        start: None,
                        seed: false,
                        credits: crate::Credits {
                            items: 1,
                            bytes: 1024,
                        },
                    }),
                };
                Ok(request)
            },
        )
        .unwrap();
    let (key, _, intent_hash) = focal_wire::managed_request_identity(&prepared.request).unwrap();
    let cursor_receipt = ManagedReceipt {
        key,
        intent_hash,
        sequence: SessionSeq(1),
        raft_index: 12,
        outcome: ManagedReceiptOutcome::Cursor {
            revision: 1,
            floor: SessionSeq(0),
            record: None,
        },
    };
    store.record_receipt(cursor, &cursor_receipt).unwrap();
    let control = store
        .prepare_acknowledgment(RequestId::from_u128(30), 2)
        .unwrap();
    let RequestStreamCommand::Acknowledge {
        through, receipts, ..
    } = control.command
    else {
        panic!("ack");
    };
    assert_eq!(through, 2);
    assert_eq!(
        receipts,
        vec![
            ManagedReceiptAck {
                key: domain_receipt.key,
                receipt_hash: domain_receipt.content_hash().unwrap()
            },
            ManagedReceiptAck {
                key: cursor_receipt.key,
                receipt_hash: cursor_receipt.content_hash().unwrap()
            },
        ]
    );
    // Only request receipts appear; no consumer cursor position is advanced.
    assert_eq!(store.request(cursor).unwrap().request, prepared.request);
}

#[test]
fn private_files_reject_symlinks_and_preserve_legacy_layout_separation() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let store = create(&root);
    let alias = temp.path().join("alias");
    symlink(&root, &alias).unwrap();
    assert!(ManagedOperationStore::open(alias, context(), limits()).is_err());
    assert!(crate::operation_store::OperationStore::open(&root, Default::default()).is_err());
    let id = store.reserve(RequestId::from_u128(20)).unwrap();
    store.prepare(id, intent(), |key| Ok(request(key))).unwrap();
    std::fs::set_permissions(
        root.join(prepared_name(1)),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(matches!(
        store.request(id),
        Err(ManagedStoreError::Store(StoreError::Permissions))
    ));
}

#[test]
fn allocation_process_child() {
    let Some(root) = std::env::var_os("FOCAL_MANAGED_TEST_ROOT") else {
        return;
    };
    let seed: u128 = std::env::var("FOCAL_MANAGED_TEST_SEED")
        .unwrap()
        .parse()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let store = loop {
        match ManagedOperationStore::open(&root, context(), ManagedStoreLimits::default()) {
            Ok(store) => break store,
            Err(ManagedStoreError::Store(StoreError::Locked))
                if std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(std::time::Duration::from_millis(1))
            }
            _ => panic!("open concurrent store"),
        }
    };
    for index in 0..4 {
        loop {
            match store.reserve(RequestId::from_u128(seed + index)) {
                Ok(_) => break,
                Err(ManagedStoreError::Store(StoreError::Locked))
                    if std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
                _ => panic!("reserve concurrent ordinal"),
            }
        }
    }
}

#[test]
fn concurrent_processes_issue_distinct_durable_ordinals() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("managed");
    let limits = ManagedStoreLimits::default();
    let mut registration = registration();
    if let RequestStreamCommand::Register { window, .. } = &mut registration.command {
        *window = limits.window;
    }
    let store =
        ManagedOperationStore::create(&root, context(), limits, registration.clone()).unwrap();
    store
        .record_registration(receipt_for(
            &registration,
            10,
            RequestStreamControlOutcome::Registered(RequestStreamState::Active {
                stream: store.status().unwrap().stream,
                owner: RequestId::from_u128(6),
                revision: 1,
                window: limits.window,
                acknowledged_through: 0,
            }),
        ))
        .unwrap();
    let mut children = Vec::new();
    for index in 1..=4 {
        children.push(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "managed_store::tests::allocation_process_child",
                    "--nocapture",
                ])
                .env("FOCAL_MANAGED_TEST_ROOT", &root)
                .env("FOCAL_MANAGED_TEST_SEED", (index * 100).to_string())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    for child in children {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let saved = store.outstanding().unwrap();
    assert_eq!(saved.len(), 16);
    for (index, id) in saved.iter().enumerate() {
        assert_eq!(id.key(context()).ordinal, index as u64 + 1);
    }
    let distinct: std::collections::BTreeSet<_> =
        saved.iter().map(|id| id.key(context()).id).collect();
    assert_eq!(distinct.len(), 16);
}

#[test]
fn close_at_maximum_control_revision_does_not_require_another_revision() {
    let temp = tempfile::tempdir().unwrap();
    let store = create(&temp.path().join("managed"));
    let (directory, mut state) = store.load().unwrap();
    state.revision = u64::MAX;
    store.save(&directory, &state).unwrap();
    drop(directory);
    assert_eq!(store.stop_issuance().unwrap(), 0);
    let input = store.prepare_close(RequestId::from_u128(30)).unwrap();
    let stream = store.status().unwrap().stream;
    let receipt = receipt_for(
        &input,
        11,
        RequestStreamControlOutcome::Closed {
            stream,
            vacant_generation: stream.generation,
        },
    );
    store.record_control(receipt).unwrap();
    assert!(store.status().unwrap().closed);
}

#[test]
fn only_committed_later_registration_establishes_old_generation_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let original = create(&temp.path().join("original"));
    let old = original.reserve(RequestId::from_u128(20)).unwrap();
    let mut registration = registration();
    if let RequestStreamCommand::Register {
        expected_generation,
        ..
    } = &mut registration.command
    {
        *expected_generation = 1;
    }
    let later = ManagedOperationStore::create(
        temp.path().join("later"),
        context(),
        limits(),
        registration.clone(),
    )
    .unwrap();
    assert!(matches!(
        later.request(old),
        Err(ManagedStoreError::Missing)
    ));
    later
        .record_registration(receipt_for(
            &registration,
            100,
            RequestStreamControlOutcome::Registered(RequestStreamState::Active {
                stream: later.status().unwrap().stream,
                owner: RequestId::from_u128(6),
                revision: 1,
                window: limits().window,
                acknowledged_through: 0,
            }),
        ))
        .unwrap();
    assert!(matches!(
        later.prepare(old, intent(), |_| panic!("old generation expanded")),
        Err(ManagedStoreError::Retired)
    ));
}
