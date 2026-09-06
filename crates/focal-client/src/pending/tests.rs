use super::*;
use crate::input::{BuildContext, ClaimDocument, InputFormat, parse_document};
use focal_core::Core;
use std::{fs, path::Path, time::Duration};

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
fn requests(seed: u128) -> (RequestEnvelope, RequestEnvelope) {
    let context = context();
    let document: ClaimDocument = parse_document(br#"{
      "description":"private authored operation payload",
      "target":"00000000000000000000000000000002",
      "validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"receipt evidence","evaluator":"00000000000000000000000000000001"}]
    }"#, InputFormat::Json).unwrap();
    let mut next = seed + 100;
    let command = document
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
        operation: Operation::Submit {
            expected_revision: None,
            command,
        },
        ..open.clone()
    };
    (open, request)
}
fn create(path: &Path, seed: u128) -> OperationJournal {
    let (open, request) = requests(seed);
    OperationJournal::create(path, context(), open, request).unwrap()
}
fn execute(core: &mut Core, request: &RequestEnvelope) -> MutationReply {
    let (runtime, expected_revision, command) = match &request.operation {
        Operation::OpenEpoch { epoch } => (true, None, Command::NegotiateEpoch { epoch: *epoch }),
        Operation::Submit {
            expected_revision,
            command,
        } => (false, *expected_revision, command.clone()),
        _ => panic!("journal has only epoch and domain mutation"),
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
        Err(other) => panic!("real domain admission: {other:?}"),
    }
}

#[test]
fn expanded_authored_ids_epoch_and_business_receipts_survive_lost_replies() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operation");
    let mut core = Core::new(context().ledger, Limits::default());
    let mut journal = create(&path, 10);
    assert_eq!(journal.path(), path);
    assert_eq!(journal.stage(), OperationStage::OpenEpoch);
    for expected_stage in [OperationStage::Command, OperationStage::Completed] {
        let original = postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap();
        let received = execute(&mut core, journal.next_request().unwrap().unwrap());
        let before_retry = core.sequence();
        // Remote commitment is real; deliberately lose the response before it
        // reaches the private journal, then reconstruct the same client command.
        drop(journal);
        journal = OperationJournal::open(&path, &context()).unwrap();
        assert_eq!(
            postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap(),
            original
        );
        let retried = execute(&mut core, journal.next_request().unwrap().unwrap());
        assert!(matches!(
            retried,
            MutationReply::Domain(DomainOutcome::Duplicate(_))
        ));
        assert_eq!(core.sequence(), before_retry);
        assert_eq!(journal.record_reply(&retried).unwrap(), expected_stage);
        assert_eq!(journal.record_reply(&received).unwrap(), expected_stage);
    }
    let receipt = journal.receipt().unwrap().clone();
    assert_eq!(core.snapshot().claims.len(), 1);
    assert!(matches!(receipt.outcome, CommandResult::Generated(_)));
    drop(journal);
    let journal = OperationJournal::open(&path, &context()).unwrap();
    assert_eq!(journal.receipt(), Some(&receipt));
    assert!(journal.next_request().unwrap().is_none());
}

#[test]
fn remote_inspection_binds_the_business_request_in_every_stage_without_writing() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operation");
    let mut journal = create(&path, 10);
    let business = journal.business_request().unwrap().clone();
    let mut core = Core::new(context().ledger, Limits::default());
    let epoch = execute(&mut core, journal.next_request().unwrap().unwrap());
    let committed = execute(&mut core, &business);
    let MutationReply::Committed(receipt) = &committed else {
        panic!("committed")
    };
    for stage in [
        OperationStage::OpenEpoch,
        OperationStage::Command,
        OperationStage::Completed,
    ] {
        assert_eq!(journal.stage(), stage);
        assert_eq!(journal.business_request().unwrap(), &business);
        let before = fs::read(path.join("state.bin")).unwrap();
        journal.validate_business_receipt(receipt).unwrap();
        for change in 0..5 {
            let mut forged = receipt.clone();
            match change {
                0 => forged.command_hash = ContentHash([42; 32]),
                1 => forged.key.id = RequestId::from_u128(500),
                2 => forged.key.principal = ParticipantId::from_u128(500),
                3 => forged.key.epoch = RequestEpoch(2),
                _ => forged.sequence = SessionSeq(0),
            }
            assert!(matches!(
                journal.validate_business_receipt(&forged),
                Err(PendingError::ReceiptMismatch)
            ));
        }
        if stage == OperationStage::Completed {
            let mut forged = receipt.clone();
            forged.outcome = CommandResult::Generated(Vec::new());
            assert!(matches!(
                journal.validate_business_receipt(&forged),
                Err(PendingError::ReceiptMismatch)
            ));
        }
        assert_eq!(fs::read(path.join("state.bin")).unwrap(), before);
        match stage {
            OperationStage::OpenEpoch => {
                journal.record_reply(&epoch).unwrap();
            }
            OperationStage::Command => {
                journal.record_reply(&committed).unwrap();
            }
            OperationStage::Completed => {}
        }
    }
}

#[test]
fn concurrent_operations_share_fixed_epoch_without_retiring_each_others_identity() {
    let directory = tempfile::tempdir().unwrap();
    let mut first = create(&directory.path().join("first"), 10);
    let mut second = create(&directory.path().join("second"), 20);
    let mut core = Core::new(context().ledger, Limits::default());
    for journal in [&mut first, &mut second] {
        let reply = execute(&mut core, journal.next_request().unwrap().unwrap());
        assert_eq!(
            journal.record_reply(&reply).unwrap(),
            OperationStage::Command
        );
    }
    let unknown = first.next_request().unwrap().unwrap().clone();
    execute(&mut core, &unknown);
    let reply = execute(&mut core, second.next_request().unwrap().unwrap());
    second.record_reply(&reply).unwrap();
    drop(first);
    let mut first = OperationJournal::open(directory.path().join("first"), &context()).unwrap();
    assert_eq!(first.next_request().unwrap(), Some(&unknown));
    first.record_reply(&execute(&mut core, &unknown)).unwrap();
    assert_eq!(core.snapshot().claims.len(), 2);
    assert_eq!(first.receipt().unwrap().key.epoch, RequestEpoch(1));
    assert_eq!(second.receipt().unwrap().key.epoch, RequestEpoch(1));
}

#[test]
fn receipt_context_hash_epoch_and_noncommit_results_cannot_advance_the_journal() {
    let directory = tempfile::tempdir().unwrap();
    let mut journal = create(&directory.path().join("operation"), 10);
    let mut core = Core::new(context().ledger, Limits::default());
    let MutationReply::Committed(receipt) =
        execute(&mut core, journal.next_request().unwrap().unwrap())
    else {
        panic!("fresh receipt")
    };
    for change in 0..6 {
        let mut forged = receipt.clone();
        match change {
            0 => forged.key.principal = ParticipantId::from_u128(44),
            1 => forged.key.id = journal.state.request.request_id,
            2 => forged.ledger.session = SessionId::from_u128(44),
            3 => forged.command_hash = ContentHash([44; 32]),
            4 => forged.outcome = CommandResult::EpochAdmitted(RequestEpoch(2)),
            _ => forged.sequence = SessionSeq(0),
        }
        assert!(matches!(
            journal.record_reply(&MutationReply::Committed(forged)),
            Err(PendingError::ReceiptMismatch)
        ));
        assert_eq!(journal.stage(), OperationStage::OpenEpoch);
    }
    for reply in [
        MutationReply::Domain(DomainOutcome::refuse(
            ErrorCode::WrongActor,
            "private rejected text",
        )),
        MutationReply::Pending(receipt.key),
    ] {
        let error = journal.record_reply(&reply).unwrap_err();
        assert!(matches!(error, PendingError::NotCommitted));
        assert!(!error.to_string().contains("private"));
        assert_eq!(journal.stage(), OperationStage::OpenEpoch);
    }
    journal
        .record_reply(&MutationReply::Committed(receipt))
        .unwrap();
    let bytes = postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap();
    drop(journal);
    let journal = OperationJournal::open(directory.path().join("operation"), &context()).unwrap();
    assert_eq!(
        postcard::to_stdvec(journal.next_request().unwrap().unwrap()).unwrap(),
        bytes
    );
}

#[test]
fn every_ambiguous_write_retains_exact_old_or_new_generation_and_requires_reopen() {
    for fault in [
        files::Fault::FileSynced,
        files::Fault::Renamed,
        files::Fault::DirectorySynced,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operation");
        let mut journal = create(&path, 10);
        let mut core = Core::new(context().ledger, Limits::default());
        let reply = execute(&mut core, journal.next_request().unwrap().unwrap());
        journal.directory.inject(fault);
        assert!(matches!(
            journal.record_reply(&reply),
            Err(PendingError::Io(_))
        ));
        assert!(matches!(journal.next_request(), Err(PendingError::Failed)));
        assert!(matches!(
            journal.record_reply(&reply),
            Err(PendingError::Failed)
        ));
        drop(journal);
        let mut recovered = OperationJournal::open(&path, &context()).unwrap();
        assert_eq!(
            recovered.stage(),
            if fault == files::Fault::FileSynced {
                OperationStage::OpenEpoch
            } else {
                OperationStage::Command
            }
        );
        // The same exact receipt reconciles either generation, without a new
        // epoch identity or a new command, and clears the orphan temp safely.
        recovered.record_reply(&reply).unwrap();
        let command = execute(&mut core, recovered.next_request().unwrap().unwrap());
        recovered.record_reply(&command).unwrap();
        assert_eq!(recovered.stage(), OperationStage::Completed);
        assert_eq!(core.snapshot().claims.len(), 1);
    }
}

#[test]
fn initial_creation_recovers_only_a_complete_renamed_record() {
    let temporary = tempfile::tempdir().unwrap();
    let source = create(&temporary.path().join("source"), 10);
    let bytes = encode(&source.state).unwrap();
    for fault in [
        files::Fault::FileSynced,
        files::Fault::Renamed,
        files::Fault::DirectorySynced,
    ] {
        let path = temporary.path().join(format!("{fault:?}"));
        let directory = files::Directory::create(&path).unwrap();
        directory.inject(fault);
        assert!(matches!(
            directory.install(&bytes, true),
            Err(PendingError::Io(_))
        ));
        drop(directory);
        if fault == files::Fault::FileSynced {
            // An unrenamed temporary was never exposed for transmission and is
            // insufficient to reconstruct an authoritative request identity.
            assert!(matches!(
                OperationJournal::open(&path, &context()),
                Err(PendingError::Corrupt)
            ));
            assert!(!path.join("INITIALIZED").exists());
        } else {
            let recovered = OperationJournal::open(&path, &context()).unwrap();
            assert_eq!(
                recovered.next_request().unwrap(),
                source.next_request().unwrap()
            );
            assert!(path.join("INITIALIZED").exists());
        }
    }
}

#[test]
fn valid_checksum_cannot_hide_invalid_generation_or_trailing_state() {
    let temporary = tempfile::tempdir().unwrap();
    for changed in ["generation", "trailing", "same_request", "protocol"] {
        let path = temporary.path().join(changed);
        let mut journal = create(&path, 10);
        match changed {
            "generation" => journal.state.generation = 3,
            "same_request" => {
                journal.state.request.request_id = journal.state.open_epoch.request_id
            }
            "protocol" => journal.state.request.protocol = 0,
            _ => {}
        }
        let mut bytes = encode(&journal.state).unwrap();
        if changed == "trailing" {
            bytes.push(0);
        }
        // The private framing writer supplies a valid checksum. Recovery must
        // still validate exact format, progression, and envelope identity.
        journal.directory.install(&bytes, false).unwrap();
        drop(journal);
        assert!(OperationJournal::open(&path, &context()).is_err());
    }
}

#[test]
fn missing_corrupt_initialized_or_entire_operation_state_is_never_recreated() {
    let directory = tempfile::tempdir().unwrap();
    for name in ["record", "both", "whole", "checksum", "marker"] {
        let path = directory.path().join(name);
        let journal = create(&path, 10);
        drop(journal);
        match name {
            "record" => fs::remove_file(path.join("state.bin")).unwrap(),
            "both" => {
                fs::remove_file(path.join("state.bin")).unwrap();
                fs::remove_file(path.join("INITIALIZED")).unwrap();
            }
            "whole" => fs::remove_dir_all(&path).unwrap(),
            "checksum" => {
                let mut bytes = fs::read(path.join("state.bin")).unwrap();
                bytes[30] ^= 1;
                fs::write(path.join("state.bin"), bytes).unwrap();
            }
            _ => fs::write(path.join("INITIALIZED"), b"wrong").unwrap(),
        }
        assert!(matches!(
            OperationJournal::open(&path, &context()),
            Err(PendingError::Corrupt)
        ));
        assert!(!path.join("state.bin").exists() || name == "checksum" || name == "marker");
    }
    // Loss while the owner is alive also cannot turn replace into initialize.
    let path = directory.path().join("live");
    let mut journal = create(&path, 10);
    let mut core = Core::new(context().ledger, Limits::default());
    let reply = execute(&mut core, journal.next_request().unwrap().unwrap());
    fs::remove_file(path.join("state.bin")).unwrap();
    fs::remove_file(path.join("INITIALIZED")).unwrap();
    assert!(matches!(
        journal.record_reply(&reply),
        Err(PendingError::Corrupt)
    ));
    assert!(!path.join("state.bin").exists());
}

#[test]
fn locks_private_modes_scope_and_wire_capacity_are_checked_before_transmission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operation");
    let journal = create(&path, 10);
    assert!(matches!(
        OperationJournal::open(&path, &context()),
        Err(PendingError::Locked)
    ));
    let other_path = path.clone();
    assert!(
        std::thread::spawn(move || matches!(
            OperationJournal::open(&other_path, &context()),
            Err(PendingError::Locked)
        ))
        .join()
        .unwrap()
    );
    let (open, command) = requests(10);
    assert!(matches!(
        OperationJournal::create(&path, context(), open.clone(), command.clone()),
        Err(PendingError::Exists)
    ));
    drop(journal);
    for which in 0..3 {
        let mut wrong = context();
        match which {
            0 => wrong.cluster = [4; 16],
            1 => wrong.principal = ParticipantId::from_u128(44),
            _ => wrong.ledger.session = SessionId::from_u128(44),
        }
        assert!(matches!(
            OperationJournal::open(&path, &wrong),
            Err(PendingError::ContextMismatch)
        ));
    }
    let mut epoch_two = open.clone();
    epoch_two.request_epoch = RequestEpoch(2);
    let invalid = directory.path().join("invalid");
    assert!(matches!(
        OperationJournal::create(&invalid, context(), epoch_two, command.clone()),
        Err(PendingError::EpochPolicy)
    ));
    assert!(!invalid.exists());
    let mut floor = command.clone();
    floor.operation = Operation::Submit {
        expected_revision: None,
        command: Command::AdvanceEpochFloor {
            minimum: RequestEpoch(2),
        },
    };
    assert!(matches!(
        OperationJournal::create(&invalid, context(), open.clone(), floor),
        Err(PendingError::EpochPolicy)
    ));
    assert!(!invalid.exists());
    let mut large = command;
    large.operation = Operation::Submit {
        expected_revision: None,
        command: Command::RecordProgress {
            claim: ClaimId::from_u128(1),
            receipt: ReceiptFence {
                receipt: ReceiptId::from_u128(1),
                epoch: 1,
            },
            message: "x".repeat(900 * 1024),
        },
    };
    let large_path = directory.path().join("large");
    let journal =
        OperationJournal::create(&large_path, context(), open.clone(), large.clone()).unwrap();
    drop(journal);
    let journal = OperationJournal::open(&large_path, &context()).unwrap();
    assert_eq!(journal.state.request, large);
    let Operation::Submit {
        command: Command::RecordProgress { message, .. },
        ..
    } = &mut large.operation
    else {
        panic!("shape")
    };
    message.push_str(&"x".repeat(200 * 1024));
    assert!(matches!(
        OperationJournal::create(&invalid, context(), open, large),
        Err(PendingError::Capacity)
    ));
    assert!(!invalid.exists());
}

#[test]
#[cfg(unix)]
fn symlinks_hardlinks_and_nonprivate_files_are_rejected() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operation");
    drop(create(&path, 10));
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o700);
    for file in ["LOCK", "state.bin", "INITIALIZED"] {
        assert_eq!(fs::metadata(path.join(file)).unwrap().mode() & 0o777, 0o600);
    }
    let linked = directory.path().join("linked");
    symlink(&path, &linked).unwrap();
    assert!(matches!(
        OperationJournal::open(&linked, &context()),
        Err(PendingError::Permissions)
    ));
    fs::hard_link(
        path.join("state.bin"),
        directory.path().join("shared-state"),
    )
    .unwrap();
    assert!(matches!(
        OperationJournal::open(&path, &context()),
        Err(PendingError::Permissions)
    ));
    fs::remove_file(directory.path().join("shared-state")).unwrap();
    fs::set_permissions(path.join("state.bin"), fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        OperationJournal::open(&path, &context()),
        Err(PendingError::Permissions)
    ));
}

struct CommitAndLose {
    core: std::sync::Mutex<Core>,
}
impl crate::ClientTransport for CommitAndLose {
    fn request<'a>(
        &'a self,
        _: Option<&'a crate::RouteHint>,
        request: &'a RequestEnvelope,
    ) -> crate::TransportFuture<'a> {
        Box::pin(async move {
            execute(&mut self.core.lock().unwrap(), request);
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn cancelling_network_wait_preserves_the_locked_operation_and_exact_request() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operation");
    // Deliberately keep sync journal ownership outside the canceled network
    // future; production CLI uses this same separation on its OS main thread.
    let journal = create(&path, 10);
    let request = journal.next_request().unwrap().unwrap().clone();
    let client = crate::Client::new(
        CommitAndLose {
            core: std::sync::Mutex::new(Core::new(context().ledger, Limits::default())),
        },
        crate::RetryPolicy::default(),
        crate::WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), client.submit(request.clone()))
            .await
            .is_err()
    );
    assert!(matches!(
        OperationJournal::open(&path, &context()),
        Err(PendingError::Locked)
    ));
    drop(journal);
    let journal = OperationJournal::open(&path, &context()).unwrap();
    assert_eq!(journal.next_request().unwrap(), Some(&request));
    assert_eq!(journal.stage(), OperationStage::OpenEpoch);
}
