//! A crowded partition splits on the founder into two root delegations, the
//! founder's own session keeps serving from the upper one, a restart reopens
//! the hosted partition from its record, and the two merge back once they
//! are small; every step is a committed directory fact.
use crate::{
    network_service::tests::{Running, settings},
    placement_agent::tests::{observe, partition_host},
};
use focal_control::ControlBootstrap;
use focal_directory::{NamespaceKey, NamespaceRange, PartitionCheckpoint, PartitionId};
use std::{collections::BTreeMap, time::Duration};

async fn delegations(running: &Running) -> BTreeMap<NamespaceKey, focal_directory::Delegation> {
    let root = running.handles.control.observe_root().await.unwrap();
    let ControlBootstrap::Root { directory, .. } = &root.snapshot().state else {
        panic!("root state");
    };
    directory.delegations.as_ref().clone()
}
async fn wait_delegations(
    running: &Running,
    what: &str,
    condition: impl Fn(&BTreeMap<NamespaceKey, focal_directory::Delegation>) -> bool,
) -> BTreeMap<NamespaceKey, focal_directory::Delegation> {
    let reached = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let current = delegations(running).await;
            if condition(&current) {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    match reached {
        Ok(current) => current,
        Err(_) => {
            let status = running.handles.placement.status().await;
            panic!(
                "root delegations never reached: {what}; agent {:?}; hosted {}",
                status.map(|status| (
                    status.last_error,
                    status.root_intents,
                    status.partition_intents
                )),
                running.handles.directory.hosted().len()
            )
        }
    }
}
async fn partition_state(running: &Running, partition: PartitionId) -> Option<PartitionCheckpoint> {
    let host = running.handles.directory.host_of(partition)?;
    if host.progress().applied_index == 0 {
        return None;
    }
    let (checkpoint, _, _) = observe(running, &host, 77).await.ok()?;
    Some(checkpoint)
}
async fn wait_partition(
    running: &Running,
    partition: PartitionId,
    what: &str,
    condition: impl Fn(&PartitionCheckpoint) -> bool,
) -> PartitionCheckpoint {
    let reached = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if let Some(state) = partition_state(running, partition).await
                && condition(&state)
            {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    match reached {
        Ok(state) => state,
        Err(_) => {
            let status = running.handles.placement.status().await;
            panic!(
                "partition never reached: {what}; agent {:?}; state {:?}",
                status.map(|status| (
                    status.last_error,
                    status.root_intents,
                    status.partition_intents
                )),
                partition_state(running, partition)
                    .await
                    .map(|state| (state.sealed, state.delegation))
            )
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crowded_partition_splits_survives_a_restart_and_merges_back() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let founder = Running::start(&settings).await;
    // Test knobs for this cluster alone, read at every tick: one session
    // already crowds a partition, and nothing merges while a half holds more
    // than nothing.
    let cluster = founder.handles.control.progress().identity.cluster.0;
    super::override_thresholds(cluster, 1, 0);
    let ledger = founder.status.ledger;
    let founder_node = founder.status.node;
    let first = founder.handles.directory.plan().partition();
    let _host = partition_host(&founder).await;

    // The founder's session registers, the partition seals, a destination
    // group is hosted on the image, the root splits, the destination
    // installs and the source releases.
    let split = wait_delegations(&founder, "two delegations", |current| current.len() == 2).await;
    let at = NamespaceKey::of(ledger);
    let lower = split.get(&NamespaceKey::MIN).unwrap();
    let upper = split.get(&at).unwrap();
    assert_eq!(lower.partition, first);
    assert_eq!(lower.epoch, 2);
    assert_eq!(
        lower.namespace,
        NamespaceRange {
            start: NamespaceKey::MIN,
            end: Some(at)
        }
    );
    assert_eq!(upper.epoch, 2);
    assert_eq!(
        upper.namespace,
        NamespaceRange {
            start: at,
            end: None
        }
    );
    assert_ne!(upper.partition, first);
    assert_ne!(upper.log_group, lower.log_group);
    let fence = upper.activation.unwrap();
    assert_eq!(lower.activation, Some(fence));
    assert_eq!(fence.source, first);
    assert_eq!(fence.destination, upper.partition);
    // Both partitions serve at the new epoch: the session lives above.
    let lower_state = wait_partition(&founder, first, "source released", |state| {
        state.sealed.is_none() && state.delegation.epoch == 2
    })
    .await;
    assert!(lower_state.sessions.is_empty());
    assert!(lower_state.nodes.contains_key(&founder_node));
    let upper_state = wait_partition(
        &founder,
        upper.partition,
        "destination installed",
        |state| state.sealed.is_none() && state.delegation.epoch == 2,
    )
    .await;
    assert_eq!(upper_state.delegation, *upper);
    assert!(upper_state.sessions.contains_key(&ledger));
    assert!(upper_state.nodes.contains_key(&founder_node));
    // The agent keeps reporting into both partitions, and the session log
    // keeps serving.
    let reported = wait_partition(&founder, upper.partition, "load reported above", |state| {
        state
            .nodes
            .get(&founder_node)
            .is_some_and(|node| node.load.is_some())
    })
    .await;
    assert!(reported.sessions[&ledger].pending.is_none());
    founder
        .handles
        .ledger
        .as_ref()
        .unwrap()
        .membership()
        .await
        .unwrap();
    assert!(
        directory
            .path()
            .join("cluster/partitions")
            .read_dir()
            .unwrap()
            .count()
            == 1
    );

    // A restart reopens the hosted partition from its record and leaves the
    // delegations untouched.
    founder.stop().await;
    let founder = Running::start(&settings).await;
    let after = wait_delegations(&founder, "two delegations after restart", |current| {
        current.len() == 2
    })
    .await;
    assert_eq!(after, split);
    let reopened = wait_partition(
        &founder,
        upper.partition,
        "hosted partition reopened",
        |state| state.sealed.is_none() && state.sessions.contains_key(&ledger),
    )
    .await;
    assert_eq!(reopened.delegation, *upper);
    assert_eq!(founder.handles.directory.hosted().len(), 2);

    // Both halves are small enough to merge once the knobs allow it: the
    // upper seals for the lower, the root merges, the lower absorbs.
    super::override_thresholds(cluster, 3, 1);
    let merged = wait_delegations(&founder, "one delegation", |current| current.len() == 1).await;
    let only = merged.get(&NamespaceKey::MIN).unwrap();
    assert_eq!(only.partition, first);
    assert_eq!(only.namespace, NamespaceRange::all());
    assert_eq!(only.epoch, 3);
    let merge = only.activation.unwrap();
    assert_eq!(merge.source, upper.partition);
    assert_eq!(merge.destination, first);
    let absorbed = wait_partition(&founder, first, "upper absorbed", |state| {
        state.delegation.epoch == 3 && state.sessions.contains_key(&ledger)
    })
    .await;
    assert_eq!(absorbed.delegation, *only);
    assert!(absorbed.sealed.is_none());
    // The merged-away partition is forgotten for restarts, and the union stays
    // under the split threshold.
    tokio::time::timeout(Duration::from_secs(30), async {
        while directory
            .path()
            .join("cluster/partitions")
            .read_dir()
            .unwrap()
            .count()
            != 0
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(delegations(&founder).await.len(), 1);
    founder.stop().await;
}
