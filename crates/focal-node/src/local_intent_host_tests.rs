use super::*;
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

fn queued_host(budget: MemoryBudget) -> (ControlHost, mpsc::Receiver<Work>) {
    let namespace = LedgerId {
        tenant: focal_model::TenantId::from_u128(1),
        session: focal_model::SessionId::from_u128(1),
    };
    let (sender, receiver) = mpsc::sync_channel(1);
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
            stopped: false,
        },
        _allocation: None,
    });
    (
        ControlHost {
            peers: sender.clone(),
            sender,
            progress,
            config: ControlHostConfig::new(namespace),
            limits: ControlHost::wire_limits(),
            budget,
        },
        receiver,
    )
}
fn pending<F: Future>(future: std::pin::Pin<&mut F>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

#[tokio::test]
async fn rejected_journal_admission_returns_exact_ownership_and_never_changes_disk() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intent");
    let mut journal = PrivateJournal::open(&path).unwrap();
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
    let (returned, bytes) = failure.rejected.unwrap();
    assert_eq!(bytes, secret);
    assert!(bytes.capacity() >= 256 * 1024);
    assert_eq!(returned.read().unwrap().unwrap().as_slice(), b"old-intent");
    assert!(queue.try_recv().is_err());
    assert_eq!(budget.stats().used, 0);

    let (reply, _reply) = oneshot::channel();
    host.sender.try_send(Work::Campaign(reply)).ok().unwrap();
    let Err(failure) = host.persist_local_intent(returned, secret.to_vec()).await else {
        panic!("full queue must reject")
    };
    assert!(matches!(failure.error, LocalIntentError::Capacity));
    let debug = format!("{failure:?}");
    assert!(!debug.contains(std::str::from_utf8(secret).unwrap()));
    let (returned, bytes) = failure.rejected.unwrap();
    assert_eq!(bytes, secret);
    assert_eq!(budget.stats().used, 0);
    drop(queue);
    let Err(failure) = host.persist_local_intent(returned, bytes).await else {
        panic!("disconnected queue must reject")
    };
    assert!(matches!(failure.error, LocalIntentError::Unavailable));
    let (returned, bytes) = failure.rejected.unwrap();
    assert_eq!(bytes, secret);
    assert_eq!(std::fs::read(path.join("journal.bin")).unwrap(), before);
    drop(returned);
    assert_eq!(
        PrivateJournal::open(&path)
            .unwrap()
            .read()
            .unwrap()
            .unwrap()
            .as_slice(),
        b"old-intent"
    );
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn canceled_queued_intent_retains_lock_and_charge_until_persistence_or_unknown_drop() {
    for persist in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("intent");
        let mut journal = PrivateJournal::open(&path).unwrap();
        journal.replace(b"previous").unwrap();
        let budget = MemoryBudget::new(128 * 1024, 32 * 1024).unwrap();
        let (host, queue) = queued_host(budget.clone());
        let mut save = Box::pin(host.persist_local_intent(journal, b"new exact intent".to_vec()));
        pending(save.as_mut());
        let admitted = budget.stats().used;
        assert!(admitted >= 16 * 1024);
        assert!(matches!(
            PrivateJournal::open(&path),
            Err(EnrollmentError::Locked)
        ));
        if persist {
            drop(save);
            assert_eq!(budget.stats().used, admitted);
            let Work::PersistLocalIntent(write) = queue.try_recv().unwrap() else {
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
        let reopened = PrivateJournal::open(&path).unwrap();
        let expected: &[u8] = if persist {
            b"new exact intent"
        } else {
            b"previous"
        };
        assert_eq!(reopened.read().unwrap().unwrap().as_slice(), expected);
    }
}

#[tokio::test]
async fn delivered_journal_reply_retains_charge_and_lock_until_consumed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intent");
    let journal = PrivateJournal::open(&path).unwrap();
    let budget = MemoryBudget::new(128 * 1024, 32 * 1024).unwrap();
    let (host, queue) = queued_host(budget.clone());
    let mut save = Box::pin(host.persist_local_intent(journal, b"committed".to_vec()));
    pending(save.as_mut());
    let charged = budget.stats().used;
    let Work::PersistLocalIntent(write) = queue.try_recv().unwrap() else {
        panic!("intent")
    };
    write.persist();
    assert_eq!(budget.stats().used, charged);
    assert!(matches!(
        PrivateJournal::open(&path),
        Err(EnrollmentError::Locked)
    ));
    let journal = save.await.unwrap();
    assert_eq!(budget.stats().used, 0);
    assert_eq!(journal.read().unwrap().unwrap().as_slice(), b"committed");
    drop(journal);
    assert_eq!(
        PrivateJournal::open(&path)
            .unwrap()
            .read()
            .unwrap()
            .unwrap()
            .as_slice(),
        b"committed"
    );
}

#[tokio::test]
async fn physical_control_owner_persists_canceled_journal_behind_blocked_wal_and_reopens() {
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
    let mut journal = PrivateJournal::open(&path).unwrap();
    journal.replace(b"before").unwrap();
    let pause = network.wal.pause_for_test().unwrap();
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
    pending(write.as_mut());
    // FIFO owner work: this intent is queued after the mutation whose WAL
    // durability is blocked, so cancellation necessarily precedes journal IO.
    let mut save = Box::pin(host.persist_local_intent(journal, b"after canceled caller".to_vec()));
    pending(save.as_mut());
    drop(save);
    assert!(matches!(
        PrivateJournal::open(&path),
        Err(EnrollmentError::Locked)
    ));
    assert!(budget.stats().used >= 16 * 1024);
    pause.resume().unwrap();
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .unwrap()
        .unwrap();
    // A later owner request proves the queued write has been processed; the
    // abandoned reply releases its journal lock and admission charge.
    drop(host.observe_root().await.unwrap());
    let mut journal = PrivateJournal::open(&path).unwrap();
    assert_eq!(
        journal.read().unwrap().unwrap().as_slice(),
        b"after canceled caller"
    );
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
        PrivateJournal::open(&path)
            .unwrap()
            .read()
            .unwrap()
            .unwrap()
            .as_slice(),
        b"returned ownership"
    );
}
