use super::*;
use std::{future::Future, task::Poll};

fn queued_host(budget: MemoryBudget) -> (ControlHost, mpsc::Receiver<JournalWork>) {
    let namespace = LedgerId {
        tenant: focal_model::TenantId::from_u128(1),
        session: focal_model::SessionId::from_u128(1),
    };
    let (sender, _owner) = mpsc::sync_channel(1);
    let (journals, receiver) = mpsc::sync_channel(1);
    let (_progress, progress) = watch::channel(ControlProgressState {
        value: ControlProgress {
            identity: ControlIdentity {
                cluster: focal_directory::ClusterId([1; 16]),
                group: [2; 16],
                scope: ControlScope::Root,
                genesis: [3; 32],
            },
            node: 1,
            leader: 1,
            term: 1,
            applied_index: 1,
            revisions: ControlRevisions::default(),
            dropped_replication: 0,
            peers_unreachable: 0,
            appends_rejected: 0,
            frames_held: 0,
            frames_let_go: 0,
            frames_stale: 0,
            peer_reports_coalesced: 0,
            peer_reports_dropped: 0,
            stopped: false,
            snapshot_index: 0,
            peers: Vec::new(),
            failure: None,
        },
        _allocation: None,
    })
    .unwrap();
    (
        ControlHost {
            peers: sender.clone(),
            sender,
            journals,
            progress,
            config: ControlHostConfig::new(namespace),
            limits: ControlHost::wire_limits(),
            budget,
            pace: Default::default(),
        },
        receiver,
    )
}
/// Polls `future` once with this task's own waker (hyper-rt refuses any
/// other): it must still wait.
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(matches!(future.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
}

#[test]
fn rejected_journal_admission_returns_exact_ownership_and_never_changes_disk() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("intent");
        let mut journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        journal.replace(b"old-intent").unwrap();
        let before = std::fs::read(path.join("journal.bin")).unwrap();
        let budget = MemoryBudget::new(128 * 1024, 32 * 1024).unwrap();
        let (host, queue) = queued_host(budget.clone());
        let secret = b"private-exact-controller-intent";

        // A small logical payload can still own a large allocation. Admission must
        // account for that retained capacity before the queue takes ownership.
        let mut bytes = Vec::with_capacity(256 * 1024);
        bytes.extend_from_slice(secret);
        let Err(failure) = host.persist_local_intent(journal, bytes).await else {
            panic!("capacity must reject")
        };
        assert!(matches!(failure.error, LocalIntentError::Capacity));
        let (returned, bytes) = *failure.rejected.unwrap();
        assert_eq!(bytes, secret);
        assert!(bytes.capacity() >= 256 * 1024);
        assert_eq!(returned.read().unwrap().unwrap().as_slice(), b"old-intent");
        assert!(queue.try_recv().is_err());
        assert_eq!(budget.stats().used, 0);

        // The journal writer's queue holds one write: another journal's, admitted first and
        // still waiting, takes it.
        let other = directory.path().join("other");
        let held =
            PrivateJournal::open(&other, &focal_seal::test_keys_beside(&other).unwrap()).unwrap();
        let mut first = Box::pin(host.persist_local_intent(held, b"first".to_vec()));
        pending(first.as_mut()).await;
        let queued = budget.stats().used;
        let Err(failure) = host.persist_local_intent(returned, secret.to_vec()).await else {
            panic!("full queue must reject")
        };
        assert!(matches!(failure.error, LocalIntentError::Capacity));
        let debug = format!("{failure:?}");
        assert!(!debug.contains(std::str::from_utf8(secret).unwrap()));
        let (returned, bytes) = *failure.rejected.unwrap();
        assert_eq!(bytes, secret);
        // Only the write admitted first is charged.
        assert_eq!(budget.stats().used, queued);
        drop(first);
        drop(queue);
        assert_eq!(budget.stats().used, 0);
        let Err(failure) = host.persist_local_intent(returned, bytes).await else {
            panic!("disconnected queue must reject")
        };
        assert!(matches!(failure.error, LocalIntentError::Unavailable));
        let (returned, bytes) = *failure.rejected.unwrap();
        assert_eq!(bytes, secret);
        assert_eq!(std::fs::read(path.join("journal.bin")).unwrap(), before);
        drop(returned);
        assert_eq!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .as_slice(),
            b"old-intent"
        );
        assert_eq!(budget.stats().used, 0);
    })
    .unwrap()
}

#[test]
fn canceled_queued_intent_retains_lock_and_charge_until_persistence_or_unknown_drop() {
    focal_rt::block_on(async move {
        for persist in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("intent");
            let mut journal =
                PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
            journal.replace(b"previous").unwrap();
            let budget = MemoryBudget::new(128 * 1024, 32 * 1024).unwrap();
            let (host, queue) = queued_host(budget.clone());
            let mut save =
                Box::pin(host.persist_local_intent(journal, b"new exact intent".to_vec()));
            pending(save.as_mut()).await;
            let admitted = budget.stats().used;
            assert!(admitted >= 16 * 1024);
            assert!(matches!(
                PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
                Err(EnrollmentError::Locked)
            ));
            if persist {
                drop(save);
                assert_eq!(budget.stats().used, admitted);
                let Ok(JournalWork::Write(write)) = queue.try_recv() else {
                    panic!("queued local intent")
                };
                write.persist();
                assert_eq!(budget.stats().used, 0);
            } else {
                // The owner disappeared after queue admission. The caller cannot
                // assume whether IO began, even though this test drops it first.
                drop(queue);
                let Err(failure) = save.await else {
                    panic!("dropped admitted write")
                };
                assert!(matches!(failure.error, LocalIntentError::Unavailable));
                assert!(failure.rejected.is_none());
                assert_eq!(budget.stats().used, 0);
            }
            let reopened =
                PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
            let expected: &[u8] = if persist {
                b"new exact intent"
            } else {
                b"previous"
            };
            assert_eq!(reopened.read().unwrap().unwrap().as_slice(), expected);
        }
    })
    .unwrap()
}

#[test]
fn delivered_journal_reply_retains_charge_and_lock_until_consumed() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("intent");
        let journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        let budget = MemoryBudget::new(128 * 1024, 32 * 1024).unwrap();
        let (host, queue) = queued_host(budget.clone());
        let mut save = Box::pin(host.persist_local_intent(journal, b"committed".to_vec()));
        pending(save.as_mut()).await;
        let charged = budget.stats().used;
        let Ok(JournalWork::Write(write)) = queue.try_recv() else {
            panic!("queued local intent")
        };
        write.persist();
        assert_eq!(budget.stats().used, charged);
        assert!(matches!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()),
            Err(EnrollmentError::Locked)
        ));
        let journal = save.await.unwrap();
        assert_eq!(budget.stats().used, 0);
        assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"committed");
        drop(journal);
        assert_eq!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .as_slice(),
            b"committed"
        );
    })
    .unwrap()
}

#[test]
fn a_canceled_journal_write_is_made_while_the_owner_waits_on_its_wal_and_reopens() {
    // focal-log's writer, paused: a node below the storage level.
    let _level = crate::upgrade::announce_for_test(crate::upgrade::BELOW_STORAGE_LEVEL);
    focal_rt::block_on(async move {
        use crate::directory_bootstrap::tests::prepared_network;
        use focal_directory::{AuthorityCommand, AuthorityOperation};
        let directory = tempfile::tempdir().unwrap();
        let (network, _) = prepared_network(directory.path()).await;
        let namespace = network.state.genesis.root_namespace;
        let revision = network.control.authority().unwrap().revision();
        let budget = MemoryBudget::new(4 * 1024 * 1024, 2 * 1024 * 1024).unwrap();
        let (host, owner, outgoing) = ControlHost::spawn(
            network.control,
            crate::cluster::NoDirectoryAuthority,
            ControlHostConfig::new(namespace),
            budget.clone(),
        )
        .unwrap();
        let path = directory.path().join("journal");
        let mut journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        journal.replace(b"before").unwrap();
        let pause = crate::storage_start::test_wal(&network.storage.storage)
            .pause_for_test()
            .unwrap();
        let runtime = AuthenticatedPeer::local(PeerGrant {
            principal: focal_model::ParticipantId::from_u128(771),
            tenants: [namespace.tenant].into_iter().collect(),
            role: PeerRole::Runtime,
        })
        .unwrap();
        let mut write = Box::pin(host.submit(
            runtime.clone(),
            ControlRequest {
                id: ControlRequestId {
                    client: runtime.principal().0,
                    sequence: 1,
                },
                acknowledged_through: 0,
                command: ControlCommand::Authority(AuthorityCommand {
                    expected_revision: revision,
                    enrollment_revision: 1,
                    decided_at: crate::network_bootstrap::unix_time().unwrap(),
                    operation: AuthorityOperation::AdvanceClock,
                }),
            },
        ));
        pending(write.as_mut()).await;
        // The owner waits on its WAL, whose durability is blocked. A journal write is not queued
        // behind it: its caller cancels it once admitted, and the host's journal writer makes it
        // all the same while the owner still waits.
        let mut save =
            Box::pin(host.persist_local_intent(journal, b"after canceled caller".to_vec()));
        pending(save.as_mut()).await;
        drop(save);
        // A later request to the journal writer proves the canceled write has been made; the
        // abandoned reply released its journal lock and admission charge, the owner still held.
        host.settle_local_intents().await.unwrap();
        assert!(!host.progress().stopped);
        let mut journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        assert_eq!(
            journal.read().unwrap().unwrap().as_slice(),
            b"after canceled caller"
        );
        pause.resume().unwrap();
        crate::test_waits::charged(
            || vec![host.periods()],
            Duration::from_secs(5),
            crate::test_waits::CONTROL_TICK,
            write,
        )
        .await
        .unwrap()
        .unwrap();
        journal = host
            .persist_local_intent(journal, b"returned ownership".to_vec())
            .await
            .unwrap();
        assert_eq!(
            journal.read().unwrap().unwrap().as_slice(),
            b"returned ownership"
        );
        drop(journal);
        host.stop().await.unwrap();
        owner.join().unwrap();
        drop(host);
        drop(outgoing);
        assert_eq!(budget.stats().used, 0);
        assert_eq!(
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap())
                .unwrap()
                .read()
                .unwrap()
                .unwrap()
                .as_slice(),
            b"returned ownership"
        );
    })
    .unwrap()
}

/// A journal write the owner admitted is waited for to its answer, however long the disk takes,
/// and the journal comes back with it for the next write. Given 5 s of the wall's clock, a write
/// that took longer was let go with the journal it held and every later write found none: the
/// founder's root network owner, and twelve nodes' placement intents, stopped for good under a
/// 33-node run's load (fedora, 2026-10-30). The owner here answers once the test takes the write
/// from its queue, past those 5 s, as a disk under that load did.
#[test]
fn an_admitted_journal_write_is_waited_for_and_the_journal_comes_back() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("intent");
        let journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        let budget = MemoryBudget::new(1024 * 1024, 32 * 1024).unwrap();
        let (host, queue) = queued_host(budget.clone());
        let mut slot = Some(journal);
        for (bytes, slow) in [(b"first".as_slice(), true), (b"second".as_slice(), false)] {
            {
                let save = save_local_intent(&host, &mut slot, bytes.to_vec());
                let mut save = std::pin::pin!(save);
                pending(save.as_mut()).await;
                if slow {
                    focal_rt::time::sleep(Duration::from_secs(6)).await.unwrap();
                    pending(save.as_mut()).await;
                }
                let Ok(JournalWork::Write(write)) = queue.try_recv() else {
                    panic!("the write was admitted to the journal writer's queue")
                };
                write.persist();
                save.await.unwrap();
            }
            assert_eq!(
                slot.as_ref().unwrap().read().unwrap().unwrap().as_slice(),
                bytes
            );
        }
        drop(slot);
        assert_eq!(budget.stats().used, 0);
    })
    .unwrap()
}

/// A local intent's journal is written on its host's journal writer, never by the control owner:
/// on the root's owner, every placement intent's journal write held the owner in `fsync` while a
/// busy volume took its time, and the root replica took no input meanwhile (fleet33 k29,
/// 2026-11-02). Do: found a network, stop its root owner, save an intent through its host.
/// Expect: the journal is written and reads back, the owner taking no part.
#[test]
fn a_local_intent_is_written_without_its_control_owner() {
    focal_rt::block_on(async move {
        let data = tempfile::tempdir().unwrap();
        let mut settings = crate::config::Settings::default();
        settings.node.data_dir = Some(data.path().into());
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = crate::network_bootstrap::FoundingNetwork::open(&settings)
            .await
            .unwrap();
        let namespace = network.state.genesis.root_namespace;
        let (host, owner, _outgoing) = ControlHost::spawn(
            network.control,
            crate::cluster::NoDirectoryAuthority,
            ControlHostConfig::new(namespace),
            network.budget.clone(),
        )
        .unwrap();
        host.stop().await.unwrap();
        owner.0.join().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("intent");
        let journal =
            PrivateJournal::open(&path, &focal_seal::test_keys_beside(&path).unwrap()).unwrap();
        let mut slot = Some(journal);
        save_local_intent(&host, &mut slot, b"written-off-the-owner".to_vec())
            .await
            .unwrap();
        assert_eq!(
            slot.as_ref().unwrap().read().unwrap().unwrap().as_slice(),
            b"written-off-the-owner"
        );
    })
    .unwrap()
}
