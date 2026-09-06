use super::*;
use crate::{
    MutationReply, Operation,
    input::{BuildContext, ClaimDocument, InputFormat, parse_document},
    operations::AuthoredOperation,
    pending::OperationStage,
};
use focal_core::Core;
use focal_model::*;
use std::{cell::Cell, fs};

const ID: &str = "000000000000000000000000000000ab";
const SECOND: &str = "000000000000000000000000000000cd";
fn context() -> OperationContext {
    OperationContext {
        cluster: [9; 16],
        principal: ParticipantId::from_u128(1),
        ledger: LedgerId {
            tenant: TenantId::from_u128(2),
            session: SessionId::from_u128(3),
        },
    }
}
fn authored() -> AuthoredOperation {
    let document:ClaimDocument=parse_document(br#"{
        "description":"the original authored intent",
        "target":"00000000000000000000000000000002",
        "validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"receipt evidence","evaluator":"00000000000000000000000000000001"}]
    }"#,InputFormat::Json).unwrap();
    AuthoredOperation::ClaimSubmit(document)
}
fn intent(canonical: &[u8]) -> OperationIntent<'_> {
    OperationIntent {
        name: authored().descriptor().name,
        version: 1,
        canonical,
    }
}
fn requests(seed: u128) -> (RequestEnvelope, RequestEnvelope) {
    let context = context();
    let mut next = seed + 100;
    let operation = authored()
        .build(
            &BuildContext {
                ledger: context.ledger,
                actor: context.principal,
                root: RootCommandId::from_u128(30),
                policy_revision: 1,
            },
            &mut || {
                next += 1;
                Ok(next.to_be_bytes())
            },
        )
        .unwrap()
        .into_wire(None)
        .unwrap();
    let open = RequestEnvelope {
        protocol: focal_wire::PROTOCOL_VERSION,
        ledger: context.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(seed),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
    };
    let request = RequestEnvelope {
        request_id: RequestId::from_u128(seed + 1),
        operation,
        ..open.clone()
    };
    (open, request)
}
fn execute(core: &mut Core, request: &RequestEnvelope) -> MutationReply {
    let (runtime, expected_revision, command) = match &request.operation {
        Operation::OpenEpoch { epoch } => (true, None, Command::NegotiateEpoch { epoch: *epoch }),
        Operation::Submit {
            expected_revision,
            command,
        } => (false, *expected_revision, command.clone()),
        _ => panic!("saved mutation"),
    };
    let input = AuthenticatedInput {
        ledger: context().ledger,
        principal: context().principal,
        request_epoch: request.request_epoch,
        request_id: request.request_id,
        expected_revision,
        command,
        authority: AuthorityContext {
            runtime,
            cause: Cause::Root(RootCommandId::from_u128(30)),
            policy_revision: 1,
            logical_time: 0,
            evidence: Vec::new(),
        },
    };
    match core.prepare(&input) {
        Ok(prepared) => MutationReply::Committed(
            core.apply(SessionSeq(core.sequence().0 + 1), prepared)
                .unwrap()
                .receipt,
        ),
        Err(DomainOutcome::Duplicate(receipt)) => {
            MutationReply::Domain(DomainOutcome::Duplicate(receipt))
        }
        other => panic!("real admission failed: {other:?}"),
    }
}
fn never_expand() -> Result<(RequestEnvelope, RequestEnvelope), StoreError> {
    panic!("retry generated new IDs")
}

#[test]
fn caller_known_id_survives_both_lost_remote_replies_with_exact_core_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("operations");
    let limits = StoreLimits::default();
    let store = OperationStore::create(&root, limits).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    let expanded = Cell::new(0);
    let mut journal = store
        .open_or_create(ID, context(), intent(&canonical), || {
            expanded.set(expanded.get() + 1);
            Ok(requests(10))
        })
        .unwrap();
    assert_eq!(journal.path(), store.operation_path(ID).unwrap());
    let mut core = Core::new(context().ledger, Limits::default());
    for stage in [OperationStage::Command, OperationStage::Completed] {
        let bytes = postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap();
        execute(&mut core, journal.next_request().unwrap().unwrap());
        let committed = core.sequence();
        drop(journal);
        let reopened = OperationStore::open(&root, limits).unwrap();
        journal = reopened
            .open_or_create(ID, context(), intent(&canonical), never_expand)
            .unwrap();
        assert_eq!(
            postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap(),
            bytes
        );
        let duplicate = execute(&mut core, journal.next_request().unwrap().unwrap());
        assert!(matches!(
            duplicate,
            MutationReply::Domain(DomainOutcome::Duplicate(_))
        ));
        assert_eq!(core.sequence(), committed);
        assert_eq!(journal.record_reply(&duplicate).unwrap(), stage);
    }
    let receipt = journal.receipt().unwrap().clone();
    drop(journal);
    let journal = store.open_existing(ID, &context()).unwrap();
    assert_eq!(journal.receipt(), Some(&receipt));
    assert_eq!(core.snapshot().claims.len(), 1);
    assert_eq!(expanded.get(), 1);
}

#[test]
fn intent_and_context_are_checked_before_expansion_and_journal_lock_is_per_id() {
    let temp = tempfile::tempdir().unwrap();
    let store = OperationStore::create(temp.path().join("store"), StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    let first = store
        .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
        .unwrap();
    assert!(matches!(
        store.open_or_create(ID, context(), intent(&canonical), never_expand),
        Err(StoreError::Pending(PendingError::Locked))
    ));
    for different in [
        OperationIntent {
            name: "testament.submit",
            ..intent(&canonical)
        },
        OperationIntent {
            version: 2,
            ..intent(&canonical)
        },
        intent(b"changed"),
    ] {
        assert!(matches!(
            store.open_or_create(ID, context(), different, never_expand),
            Err(StoreError::IntentConflict)
        ));
    }
    let mut other = context();
    other.principal = ParticipantId::from_u128(55);
    assert!(matches!(
        store.open_or_create(ID, other, intent(&canonical), never_expand),
        Err(StoreError::ContextMismatch)
    ));
    assert!(matches!(
        store.open_existing(ID, &other),
        Err(StoreError::ContextMismatch)
    ));
    // Keeping the first operation owner alive must not keep catalogue admission
    // locked or serialize unrelated in-flight CLI/MCP network waits.
    let second = store
        .open_or_create(SECOND, context(), intent(&canonical), || Ok(requests(20)))
        .unwrap();
    assert_eq!(store.usage().unwrap().operations, 2);
    assert_ne!(first.path(), second.path());
    drop(first);
    assert_eq!(
        store
            .open_existing(&ID.to_uppercase(), &context())
            .unwrap()
            .path(),
        store.operation_path(ID).unwrap()
    );
}

#[test]
fn claimed_without_complete_envelopes_never_reruns_generator() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    store.fault.set(Some(Fault::Claimed));
    assert!(matches!(
        store.open_or_create(ID, context(), intent(&canonical), never_expand),
        Err(StoreError::Io(_))
    ));
    let reopened = OperationStore::open(&root, StoreLimits::default()).unwrap();
    assert!(matches!(
        reopened.open_or_create(ID, context(), intent(&canonical), never_expand),
        Err(StoreError::Incomplete)
    ));
    assert!(matches!(
        reopened.open_existing(ID, &context()),
        Err(StoreError::Incomplete)
    ));
    assert_eq!(reopened.usage().unwrap().operations, 1);
    assert!(matches!(
        reopened.retry(SECOND, context(), intent(&canonical)),
        Err(StoreError::MissingOperation)
    ));
    assert!(matches!(
        reopened.open_existing(SECOND, &context()),
        Err(StoreError::MissingOperation)
    ));
    assert_eq!(reopened.usage().unwrap().operations, 1);
    assert!(!root.join(component(parse_id(SECOND).unwrap())).exists());
}

#[test]
fn each_complete_prepared_creation_boundary_recovers_without_reexpansion() {
    for fault in [Fault::Prepared, Fault::Journal, Fault::Ready] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        store.fault.set(Some(fault));
        assert!(matches!(
            store.open_or_create(ID, context(), intent(&canonical), || Ok(requests(10))),
            Err(StoreError::Io(_))
        ));
        drop(store);
        let store = OperationStore::open(&root, StoreLimits::default()).unwrap();
        let journal = store.open_existing(ID, &context()).unwrap();
        assert_eq!(journal.next_request().unwrap(), Some(&requests(10).0));
        drop(journal);
        let journal = store
            .open_or_create(ID, context(), intent(&canonical), never_expand)
            .unwrap();
        assert!(journal.matches_requests(&requests(10).0, &requests(10).1));
    }
}

#[test]
fn aggregate_quota_reserves_all_future_journal_generations_including_failed_claims() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let limits = StoreLimits {
        max_operations: 8,
        max_reserved_bytes: ROOT_RESERVATION + 2 * OPERATION_RESERVATION,
    };
    let store = OperationStore::create(&root, limits).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    assert!(matches!(
        store.open_or_create(ID, context(), intent(&canonical), || Err(
            InputError::Capacity.into()
        )),
        Err(StoreError::Expansion(InputError::Capacity))
    ));
    let second = store
        .open_or_create(SECOND, context(), intent(&canonical), || Ok(requests(20)))
        .unwrap();
    assert_eq!(
        store.usage().unwrap(),
        StoreUsage {
            operations: 2,
            reserved_bytes: limits.max_reserved_bytes
        }
    );
    assert!(matches!(
        store.open_or_create(
            "000000000000000000000000000000ef",
            context(),
            intent(&canonical),
            never_expand
        ),
        Err(StoreError::Capacity)
    ));
    assert!(matches!(
        OperationStore::open(&root, StoreLimits::default()),
        Err(StoreError::LimitsMismatch)
    ));
    drop(second);
    drop(store);
    assert_eq!(
        OperationStore::open(&root, limits)
            .unwrap()
            .usage()
            .unwrap()
            .reserved_bytes,
        limits.max_reserved_bytes
    );
}

#[test]
fn oversized_intent_and_invalid_ids_are_rejected_before_catalogue_admission() {
    let temp = tempfile::tempdir().unwrap();
    let store = OperationStore::create(temp.path().join("store"), StoreLimits::default()).unwrap();
    for id in [
        "",
        "../escape",
        "00000000000000000000000000000000",
        "000000000000000000000000000000ax",
    ] {
        assert!(matches!(
            store.open_or_create(id, context(), intent(b"{}"), never_expand),
            Err(StoreError::InvalidId)
        ));
    }
    assert!(matches!(
        store.open_or_create(
            ID,
            context(),
            intent(&vec![0; MAX_INTENT_BYTES + 1]),
            never_expand
        ),
        Err(StoreError::Capacity)
    ));
    assert_eq!(store.usage().unwrap().operations, 0);
}

#[test]
fn loss_of_ready_journal_prepared_or_root_catalogue_fails_closed() {
    for remove in [
        "INITIALIZED",
        "catalogue.bin",
        "prepared.bin",
        "journal",
        "operation",
        "store",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        drop(
            store
                .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
                .unwrap(),
        );
        let operation = root.join(component(parse_id(ID).unwrap()));
        match remove {
            "INITIALIZED" | "catalogue.bin" => fs::remove_file(root.join(remove)).unwrap(),
            "prepared.bin" => fs::remove_file(operation.join(remove)).unwrap(),
            "journal" => fs::remove_dir_all(operation.join(remove)).unwrap(),
            "operation" => fs::remove_dir_all(&operation).unwrap(),
            "store" => fs::remove_dir_all(&root).unwrap(),
            _ => unreachable!(),
        }
        assert!(matches!(
            store.open_or_create(ID, context(), intent(&canonical), never_expand),
            Err(StoreError::Corrupt)
        ));
        if root.exists() {
            assert!(matches!(
                OperationStore::create(&root, StoreLimits::default()),
                Err(StoreError::Exists)
            ));
        } else {
            assert!(matches!(
                OperationStore::open(&root, StoreLimits::default()),
                Err(StoreError::Corrupt)
            ));
        }
    }
}

#[test]
fn private_files_catalogue_lock_checksum_and_exact_prepared_journal_binding_are_enforced() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    let directory = files::Directory::open(&root).unwrap();
    assert!(matches!(store.usage(), Err(StoreError::Locked)));
    drop(directory);
    let journal = store
        .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
        .unwrap();
    let path = journal.path().to_path_buf();
    drop(journal);
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(root.join(CATALOGUE))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    fs::remove_dir_all(&path).unwrap();
    let (open, request) = requests(20);
    drop(OperationJournal::create(&path, context(), open, request).unwrap());
    assert!(matches!(
        store.open_existing(ID, &context()),
        Err(StoreError::Corrupt)
    ));
    let mut bytes = fs::read(root.join(CATALOGUE)).unwrap();
    bytes[15] ^= 1;
    fs::write(root.join(CATALOGUE), bytes).unwrap();
    assert!(matches!(store.usage(), Err(StoreError::Corrupt)));
}

#[test]
fn partial_root_and_partial_journal_are_never_overwritten_as_fresh_operations() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    drop(files::Directory::create(&root).unwrap());
    assert!(matches!(
        OperationStore::open(&root, StoreLimits::default()),
        Err(StoreError::Corrupt)
    ));
    assert!(matches!(
        OperationStore::create(&root, StoreLimits::default()),
        Err(StoreError::Exists)
    ));
    let root = temp.path().join("second");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    store.fault.set(Some(Fault::Prepared));
    assert!(
        store
            .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
            .is_err()
    );
    let journal = store.operation_path(ID).unwrap();
    fs::create_dir(&journal).unwrap();
    assert!(store.open_existing(ID, &context()).is_err());
    assert_eq!(fs::read_dir(&journal).unwrap().count(), 0);
}

#[test]
fn symlinks_hardlinks_and_unindexed_operation_directories_are_rejected() {
    use std::os::unix::fs::{DirBuilderExt, symlink};
    for replacement in ["symlink", "hardlink", "directory"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        drop(
            store
                .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
                .unwrap(),
        );
        let operation = root.join(component(parse_id(ID).unwrap()));
        let prepared = operation.join(PREPARED);
        match replacement {
            "symlink" => {
                let moved = temp.path().join("original");
                fs::rename(&operation, &moved).unwrap();
                symlink(moved, &operation).unwrap();
            }
            "hardlink" => fs::hard_link(&prepared, temp.path().join("second-link")).unwrap(),
            "directory" => {
                fs::remove_file(&prepared).unwrap();
                fs::create_dir(&prepared).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            store.open_existing(ID, &context()),
            Err(StoreError::Permissions)
        ));
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join(component(parse_id(ID).unwrap())))
        .unwrap();
    assert!(matches!(
        store.open_or_create(ID, context(), intent(b"{}"), never_expand),
        Err(StoreError::Corrupt)
    ));
    assert_eq!(store.usage().unwrap().operations, 0);
}

#[test]
fn saved_intent_is_checked_against_catalogue_when_inspecting_without_authored_input() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    drop(
        store
            .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
            .unwrap(),
    );
    let directory = files::Directory::open(&root).unwrap();
    let mut prepared = store
        .prepared(&directory, parse_id(ID).unwrap(), true)
        .unwrap();
    prepared.canonical.push(b' ');
    directory
        .write(
            &format!("{}/{PREPARED}", component(parse_id(ID).unwrap())),
            PREPARED_MAGIC,
            &encode(&prepared, PREPARED_BYTES).unwrap(),
            true,
        )
        .unwrap();
    drop(directory);
    assert!(matches!(
        store.open_existing(ID, &context()),
        Err(StoreError::Corrupt)
    ));
}

#[test]
fn complete_creation_link_window_recovers_catalogue_and_prepared_without_regeneration() {
    use std::os::unix::fs::MetadataExt;
    for catalogue_window in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        let destination = if catalogue_window {
            root.join(CATALOGUE)
        } else {
            store.fault.set(Some(Fault::Prepared));
            assert!(
                store
                    .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
                    .is_err()
            );
            root.join(component(parse_id(ID).unwrap())).join(PREPARED)
        };
        let pending = destination.with_extension("pending");
        let original = fs::read(&destination).unwrap();
        // Reconstruct the exact post-link/pre-unlink state: the complete synced
        // record is accessible through precisely these two same-inode names.
        fs::rename(&destination, &pending).unwrap();
        fs::File::open(&pending).unwrap().sync_all().unwrap();
        fs::hard_link(&pending, &destination).unwrap();
        assert_eq!(
            fs::metadata(&pending).unwrap().ino(),
            fs::metadata(&destination).unwrap().ino()
        );
        assert_eq!(fs::metadata(&destination).unwrap().nlink(), 2);
        drop(store);
        let recovered = OperationStore::open(&root, StoreLimits::default()).unwrap();
        if !catalogue_window {
            let journal = recovered
                .open_or_create(ID, context(), intent(&canonical), never_expand)
                .unwrap();
            assert!(journal.matches_requests(&requests(10).0, &requests(10).1));
        }
        assert!(!pending.exists());
        assert_eq!(fs::metadata(&destination).unwrap().nlink(), 1);
        assert_eq!(fs::read(&destination).unwrap(), original);
    }
}

#[test]
fn prepared_envelopes_resume_each_uninitialized_journal_creation_midpoint() {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    for midpoint in ["directory", "lock", "fragment", "complete_record"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        store.fault.set(Some(Fault::Prepared));
        assert!(
            store
                .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
                .is_err()
        );
        let journal = store.operation_path(ID).unwrap();
        if midpoint == "complete_record" {
            let (epoch, request) = requests(10);
            drop(OperationJournal::create(&journal, context(), epoch, request).unwrap());
            fs::remove_file(journal.join("INITIALIZED")).unwrap();
        } else {
            fs::DirBuilder::new().mode(0o700).create(&journal).unwrap();
            if midpoint != "directory" {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(journal.join("LOCK"))
                    .unwrap()
                    .sync_all()
                    .unwrap();
            }
            if midpoint == "fragment" {
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(journal.join(".pending"))
                    .unwrap();
                file.write_all(b"FCLOP001partial initial frame").unwrap();
                file.sync_all().unwrap();
            }
        }
        drop(store);
        let store = OperationStore::open(&root, StoreLimits::default()).unwrap();
        let recovered = store
            .open_or_create(ID, context(), intent(&canonical), never_expand)
            .unwrap();
        assert!(recovered.matches_requests(&requests(10).0, &requests(10).1));
        assert_eq!(recovered.stage(), OperationStage::OpenEpoch);
        assert!(journal.join("state.bin").is_file());
        assert!(journal.join("INITIALIZED").is_file());
        assert!(!journal.join(".pending").exists());
    }
}

#[test]
fn creation_link_recovery_rejects_wrong_inodes_extra_links_symlinks_modes_and_bad_frames() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    for catalogue_window in [true, false] {
        for corruption in ["arbitrary", "third", "inode", "symlink", "mode", "checksum"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("store");
            let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
            let destination = if catalogue_window {
                root.join(CATALOGUE)
            } else {
                let canonical = authored().canonical_intent().unwrap();
                store.fault.set(Some(Fault::Prepared));
                assert!(
                    store
                        .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
                        .is_err()
                );
                root.join(component(parse_id(ID).unwrap())).join(PREPARED)
            };
            let pending = destination.with_extension("pending");
            let other = destination.with_extension("other");
            match corruption {
                "arbitrary" => fs::hard_link(&destination, &other).unwrap(),
                "inode" => {
                    fs::hard_link(&destination, &other).unwrap();
                    fs::copy(&destination, &pending).unwrap();
                    fs::hard_link(&pending, destination.with_extension("fourth")).unwrap();
                    assert_ne!(
                        fs::metadata(&destination).unwrap().ino(),
                        fs::metadata(&pending).unwrap().ino()
                    );
                }
                "symlink" => {
                    fs::hard_link(&destination, &other).unwrap();
                    symlink(&destination, &pending).unwrap();
                }
                _ => {
                    fs::hard_link(&destination, &pending).unwrap();
                    match corruption {
                        "third" => fs::hard_link(&destination, &other).unwrap(),
                        "mode" => {
                            fs::set_permissions(&destination, fs::Permissions::from_mode(0o644))
                                .unwrap()
                        }
                        "checksum" => {
                            let mut bytes = fs::read(&destination).unwrap();
                            bytes[15] ^= 1;
                            fs::write(&destination, bytes).unwrap();
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let original = fs::read(&destination).unwrap();
            let pending_existed = fs::symlink_metadata(&pending).is_ok();
            if catalogue_window {
                assert!(store.usage().is_err());
            } else {
                assert!(store.open_existing(ID, &context()).is_err());
            }
            assert_eq!(fs::read(&destination).unwrap(), original);
            assert_eq!(fs::symlink_metadata(&pending).is_ok(), pending_existed);
            assert!(fs::metadata(&destination).unwrap().nlink() >= 2);
        }
    }
}

#[test]
fn linked_catalogue_is_recovered_before_replace_temporary_cleanup() {
    use std::os::unix::fs::MetadataExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("store");
    let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
    let destination = root.join(CATALOGUE);
    let pending = destination.with_extension("pending");
    let directory = files::Directory::open(&root).unwrap();
    let payload = directory
        .read(CATALOGUE, CATALOGUE_MAGIC, CATALOGUE_BYTES)
        .unwrap();
    fs::hard_link(&destination, &pending).unwrap();
    directory
        .write(CATALOGUE, CATALOGUE_MAGIC, &payload, true)
        .unwrap();
    assert!(!pending.exists());
    assert_eq!(fs::metadata(&destination).unwrap().nlink(), 1);
    drop(directory);
    assert_eq!(store.usage().unwrap().operations, 0);
}

#[test]
fn journal_resume_preserves_corruption_initialization_and_ready_fences() {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    for corruption in [
        "corrupt_state",
        "initialized_missing_state",
        "ready_missing_both",
        "unexpected",
        "different_request",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = OperationStore::create(&root, StoreLimits::default()).unwrap();
        let canonical = authored().canonical_intent().unwrap();
        let ready = corruption == "ready_missing_both";
        if !ready {
            store.fault.set(Some(Fault::Prepared));
        }
        let initial = store.open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)));
        if ready {
            drop(initial.unwrap());
        } else {
            assert!(initial.is_err());
        }
        let path = store.operation_path(ID).unwrap();
        if corruption == "unexpected" {
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path.join("foreign"))
                .unwrap();
        } else if !ready {
            let (epoch, request) = requests(if corruption == "different_request" {
                20
            } else {
                10
            });
            drop(OperationJournal::create(&path, context(), epoch, request).unwrap());
        }
        match corruption {
            "corrupt_state" => {
                fs::write(path.join("state.bin"), b"broken committed journal frame").unwrap()
            }
            "initialized_missing_state" => fs::remove_file(path.join("state.bin")).unwrap(),
            "ready_missing_both" => {
                fs::remove_file(path.join("state.bin")).unwrap();
                fs::remove_file(path.join("INITIALIZED")).unwrap();
            }
            "different_request" => fs::remove_file(path.join("INITIALIZED")).unwrap(),
            "unexpected" => {}
            _ => unreachable!(),
        }
        let before: std::collections::BTreeMap<_, _> = fs::read_dir(&path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), fs::read(entry.path()).unwrap())
            })
            .collect();
        assert!(store.open_existing(ID, &context()).is_err());
        let after: std::collections::BTreeMap<_, _> = fs::read_dir(&path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), fs::read(entry.path()).unwrap())
            })
            .collect();
        assert_eq!(before, after, "repair mutated rejected {corruption}");
    }
}

#[test]
fn journal_resume_keeps_valid_receipts_even_before_catalogue_readiness() {
    let temp = tempfile::tempdir().unwrap();
    let store = OperationStore::create(temp.path().join("store"), StoreLimits::default()).unwrap();
    let canonical = authored().canonical_intent().unwrap();
    store.fault.set(Some(Fault::Journal));
    assert!(
        store
            .open_or_create(ID, context(), intent(&canonical), || Ok(requests(10)))
            .is_err()
    );
    let path = store.operation_path(ID).unwrap();
    let mut journal = OperationJournal::open(&path, &context()).unwrap();
    let mut core = Core::new(context().ledger, Limits::default());
    while let Some(request) = journal.next_request().unwrap() {
        let reply = execute(&mut core, request);
        journal.record_reply(&reply).unwrap();
    }
    let receipt = journal.receipt().unwrap().clone();
    let original = fs::read(path.join("state.bin")).unwrap();
    drop(journal);
    let recovered = store.open_existing(ID, &context()).unwrap();
    assert_eq!(recovered.receipt(), Some(&receipt));
    assert_eq!(recovered.stage(), OperationStage::Completed);
    assert_eq!(fs::read(path.join("state.bin")).unwrap(), original);
}
