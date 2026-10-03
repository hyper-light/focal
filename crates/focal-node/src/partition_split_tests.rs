//! A crowded partition splits on the founder into two root delegations, the
//! founder's own session keeps serving from the upper one, a restart reopens
//! the hosted partition from its record, and the two merge back once they
//! are small; every step is a committed directory fact.
use crate::{
    network_service::tests::{Running, settings, try_until, until},
    placement_agent::tests::{join_peer, observe, partition_host},
};
use focal_consensus::MembershipChange;
use focal_control::{
    ControlBootstrap, ControlCommand, ControlMembershipCommand, ControlRequest, ControlRequestId,
};
use focal_directory::{NamespaceKey, NamespaceRange, PartitionCheckpoint, PartitionId};
use focal_model::ParticipantId;
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

/// One membership change of a hosted partition group, submitted to the
/// replica `running` hosts as an administrator would (24 §13): the
/// administrator's principal is the request's client, the directory's
/// namespace its tenant. Asked again — bounded — while the group is not
/// ready for it (a learner catching up before its promotion).
async fn change_group(
    running: &Running,
    partition: PartitionId,
    client: [u8; 16],
    sequence: u64,
    change: MembershipChange,
) {
    let host = running.handles.directory.host_of(partition).unwrap();
    let namespace = running.handles.directory.namespace();
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId(client),
        tenants: BTreeSet::from([namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let committed = try_until(&[running], Duration::from_secs(60), async || {
        let witness = host.witness_membership().await.ok()?;
        let request = ControlRequest {
            id: ControlRequestId { client, sequence },
            acknowledged_through: sequence.checked_sub(1)?,
            command: ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: witness.configuration.configuration_index,
                expected: witness.configuration.configuration.clone(),
                change,
            }),
        };
        match host.submit(peer.clone(), request).await {
            Ok(receipt) => Some(receipt),
            Err(
                focal_control::ControlFailure::NotReady
                | focal_control::ControlFailure::CompareFailed
                | focal_control::ControlFailure::Unavailable
                | focal_control::ControlFailure::OutcomeUnknown,
            ) => None,
            Err(error) => panic!("{change:?}: {error:?}"),
        }
    })
    .await;
    assert!(
        committed.is_ok(),
        "{change:?} never committed: {committed:?}"
    );
}

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
    let reached = try_until(&[running], Duration::from_secs(120), async || {
        let current = delegations(running).await;
        condition(&current).then_some(current)
    })
    .await;
    match reached {
        Ok(current) => current,
        Err(spent) => {
            let status = running.handles.placement.status().await;
            // What each hosted partition stood at: sealed or not, its
            // revision and epoch, and the configuration its group applied.
            let mut hosted = Vec::new();
            for partition in running.handles.directory.hosted() {
                let state = partition_state(running, partition.plan.partition()).await;
                let witness = partition.host.witness_membership().await.ok();
                hosted.push((
                    partition.plan.partition(),
                    partition.host.progress().leader,
                    state.as_ref().map(|state| {
                        (
                            state.revision,
                            state.delegation.epoch,
                            state
                                .sealed
                                .as_ref()
                                .map(|seal| (seal.destination, seal.revision)),
                        )
                    }),
                    witness.map(|witness| witness.configuration.configuration.voters.clone()),
                ));
            }
            panic!(
                "root delegations never reached: {what}: {spent}; hosted {hosted:?}; agent {:?}; delegations {:?}",
                status.map(|status| (
                    status.last_error,
                    status.last_refusal,
                    status.retrying,
                    status.root_intents,
                    status.partition_intents
                )),
                delegations(running).await,
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
    let reached = try_until(&[running], Duration::from_secs(120), async || {
        partition_state(running, partition)
            .await
            .filter(|state| condition(state))
    })
    .await;
    match reached {
        Ok(state) => state,
        Err(spent) => {
            let status = running.handles.placement.status().await;
            panic!(
                "partition never reached: {what}: {spent}; service ended {:?}; agent {:?}; state {:?}",
                running.ended(),
                status.map(|status| (
                    status.last_error,
                    status.last_refusal,
                    status.retrying,
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

    // A host joined after the split is seated in the destination's group
    // (24 §13; the audit's F24): admitted as a learner through the founder's
    // replica, the root's grant seats it and its agent hosts a replica from
    // the group's identity the grant carries — no image — which the
    // destination's founder, having compacted at founding, brings up by
    // snapshot: the member's state shows the session it never replayed.
    // Promoted once caught up, it votes through the merge below.
    let peer_dir = tempfile::tempdir().unwrap();
    let peer_settings = crate::network_service::tests::settings(peer_dir.path());
    let (peer, peer_node) = join_peer(&founder, directory.path(), "host", &peer_settings).await;
    let client = [7; 16];
    change_group(
        &founder,
        upper.partition,
        client,
        1,
        MembershipChange::AddLearner { node: peer_node },
    )
    .await;
    let seated = wait_partition(
        &peer,
        upper.partition,
        "member brought up by snapshot",
        |state| state.sessions.contains_key(&ledger) && state.delegation.epoch == 2,
    )
    .await;
    assert_eq!(seated.delegation, *upper);
    assert_eq!(peer.handles.directory.hosted().len(), 1);
    change_group(
        &founder,
        upper.partition,
        client,
        2,
        MembershipChange::Promote { node: peer_node },
    )
    .await;
    let witness = founder
        .handles
        .directory
        .host_of(upper.partition)
        .unwrap()
        .witness_membership()
        .await
        .unwrap();
    let mut voters = witness.configuration.configuration.voters.clone();
    voters.sort_unstable();
    let mut expected = vec![founder_node, peer_node];
    expected.sort_unstable();
    assert_eq!(voters, expected);

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
    until(
        "the merged-away partition is forgotten",
        &[&founder],
        Duration::from_secs(30),
        async || {
            (directory
                .path()
                .join("cluster/partitions")
                .read_dir()
                .unwrap()
                .count()
                == 0)
                .then_some(())
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(delegations(&founder).await.len(), 1);
    // The merged-away group's seat is gone with it: the member hosts
    // nothing of it any more.
    until(
        "the member's seat in the merged-away group is gone",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || {
            peer.handles
                .directory
                .host_of(upper.partition)
                .is_none()
                .then_some(())
        },
    )
    .await;
    peer.stop().await;
    founder.stop().await;
}
