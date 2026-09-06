use super::*;
use crate::control_host::ControlHost;
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};
use std::time::Duration;

fn runtime(plan: FirstDirectoryPlan) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: focal_model::ParticipantId::from_u128(777),
        tenants: [plan.namespace().tenant].into_iter().collect(),
        role: PeerRole::Runtime,
    })
    .unwrap()
}

#[tokio::test]
async fn directory_owner_is_registered_before_blocking_recovery_and_retains_escaped_egress_charge()
{
    let directory = tempfile::tempdir().unwrap();
    let (network, plan) = prepared_network(directory.path()).await;
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    let source_index = permit.root_index();
    let paused = network.wal.pause_for_test().unwrap();
    let (returned, observed) = std::sync::mpsc::sync_channel(1);
    // A real blocked WAL catches accidental synchronous open in the constructor.
    // The watchdog always resumes it, even if that regression occurs, so no
    // failed assertion can leave a stranded physical writer behind.
    let watchdog = std::thread::spawn(move || {
        let immediate = observed.recv_timeout(Duration::from_secs(1)).is_ok();
        paused.resume().unwrap();
        immediate
    });
    let (host, owner, outgoing) =
        ControlHost::spawn_directory(permit, network.wal.clone(), budget.clone()).unwrap();
    let before = host.progress();
    returned.send(()).unwrap();
    assert!(
        watchdog.join().unwrap(),
        "constructor waited on disk before exposing physical ownership"
    );
    assert_eq!(before.identity, plan.identity().unwrap());
    assert_eq!(before.node, plan.founder_node());
    assert_eq!(before.leader, 0);
    assert_eq!(before.applied_index, 0);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let progress = host.progress();
            assert!(!progress.stopped, "directory activation failed");
            if progress.applied_index > 0 && progress.leader == progress.node {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let authority = host
        .read(
            runtime(plan),
            focal_model::RequestId::from_u128(1),
            ControlRead::Authority,
        )
        .await
        .unwrap();
    let ControlReadResult::Authority(Some(authority)) = authority else {
        panic!("installed directory authority missing");
    };
    assert_eq!(authority.source_index, source_index);
    assert_eq!(authority.identity, plan.identity().unwrap());
    drop(authority);
    host.stop().await.unwrap();
    owner.join().unwrap();
    let fixed = budget.stats().used;
    assert!(
        fixed >= 2 * 1024 * 1024,
        "fixed stack/queue guard escaped its watch"
    );
    drop(host);
    assert_eq!(
        budget.stats().used,
        fixed,
        "egress must retain the same fixed allowance"
    );
    drop(outgoing);
    assert_eq!(budget.stats().used, 0);
}

#[tokio::test]
async fn directory_startup_failure_closes_ingress_and_preserves_host_charge_after_egress_drop() {
    let directory = tempfile::tempdir().unwrap();
    let (network, plan) = prepared_network(directory.path()).await;
    let budget = MemoryBudget::new(256 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
    let permit =
        authorize_first_directory(&network.control, plan, unix_time().unwrap(), &budget).unwrap();
    let wrong = focal_log::SharedWal::open(
        directory.path().join("wrong-wal"),
        focal_log::WalOptions::new(focal_log::WalIdentity {
            cluster: [211; 16],
            node: plan.founder_node(),
            stream: 0,
        }),
    )
    .unwrap();
    let (host, owner, outgoing) =
        ControlHost::spawn_directory(permit, wrong, budget.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), host.closed())
        .await
        .unwrap();
    owner.join().unwrap();
    assert!(host.progress().stopped);
    assert_eq!(host.progress().applied_index, 0);
    assert!(
        host.read(
            runtime(plan),
            focal_model::RequestId::from_u128(2),
            ControlRead::State
        )
        .await
        .is_err()
    );
    let fixed = budget.stats().used;
    assert!(fixed >= 2 * 1024 * 1024);
    drop(outgoing);
    assert_eq!(
        budget.stats().used,
        fixed,
        "escaped host must retain fixed channel storage"
    );
    drop(host);
    assert_eq!(budget.stats().used, 0);
}
