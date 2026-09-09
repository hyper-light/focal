//! Node-level liveness: three hosts confirm each other over real QUIC, a
//! stopped host becomes a committed directory fact and revives with a higher
//! incarnation on restart; probes are bound to their authenticated sender,
//! a gossiped suspicion of oneself is refuted, and a loaded host's
//! extension is granted, rate-limited and refused when overloaded.
use crate::{
    liveness::{
        LivenessConfig, LivenessEvent, LivenessHandle, MemberStatus,
        coordinates::NetworkCoordinate,
        gossip::LivenessUpdate,
        health::LocalHealth,
        wire::{
            ExtensionOutcome, ExtensionRequest, PROBE_SCHEMA, ProbeKind, ProbeOutcome, ProbeReply,
            ProbeRequest,
        },
    },
    network_service::tests::{Running, settings},
    placement_agent::tests::{join_peer, partition_host, wait_for},
};
use focal_memory::MemoryBudget;
use std::time::Duration;

const MIB: usize = 1024 * 1024;

async fn view_when(
    running: &Running,
    what: &str,
    condition: impl Fn(&crate::liveness::LivenessView) -> bool,
) -> crate::liveness::LivenessView {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let view = running.handles.liveness.view();
            if condition(&view) {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("liveness view never reached: {what}"))
}
async fn confirmed(running: &Running, members: &[u64]) -> crate::liveness::LivenessView {
    view_when(running, "members confirmed", |view| {
        view.generation > 0
            && members.iter().all(|node| {
                view.member(*node)
                    .is_some_and(|member| member.confirmed && member.status == MemberStatus::Alive)
            })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_host_is_committed_dead_by_the_partition_leader_and_revived_on_restart() {
    let founder_dir = tempfile::tempdir().unwrap();
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let founder_settings = settings(founder_dir.path());
    let peer_settings = [settings(dirs[0].path()), settings(dirs[1].path())];
    let founder = Running::start(&founder_settings).await;
    let (peer_a, node_a) =
        join_peer(&founder, founder_dir.path(), "host-a", &peer_settings[0]).await;
    let (peer_b, node_b) =
        join_peer(&founder, founder_dir.path(), "host-b", &peer_settings[1]).await;
    let founder_node = founder.status.node;
    let host = partition_host(&founder).await;

    // Every host confirms the others by acknowledgement and learns a
    // coordinate from the round trips.
    let view = confirmed(&founder, &[node_a, node_b]).await;
    assert!(view.incarnation > 0);
    assert!(view.probes_sent > 0);
    assert!(view.events.iter().any(|event| {
        matches!(event, LivenessEvent::Confirmed { node } if *node == node_a || *node == node_b)
    }));
    confirmed(&peer_a, &[founder_node, node_b]).await;
    // Its own acknowledged probes place it in the coordinate space; the
    // founder's probes are answered from the driver's state.
    let peer_view = view_when(&peer_a, "coordinate learned", |view| {
        view.coordinate.samples > 0 && view.probes_answered > 0
    })
    .await;
    assert!(peer_view.probes_sent > 0);
    // Alive is the default: no verdict is committed for a healthy host.
    let (checkpoint, _, _) = wait_for(&founder, &host, "hosts enrolled", |state| {
        state.nodes.contains_key(&node_a) && state.nodes.contains_key(&node_b)
    })
    .await
    .expect("service ended");
    assert!(checkpoint.nodes[&node_b].liveness.is_none());
    assert!(checkpoint.nodes[&node_b].is_alive());

    // Host b leaves without notice.
    peer_b.stop().await;
    let (dead, _, _) = wait_for(&founder, &host, "host-b declared dead", |state| {
        state
            .nodes
            .get(&node_b)
            .and_then(|record| record.liveness)
            .is_some_and(|liveness| !liveness.alive)
    })
    .await
    .expect("service ended");
    let verdict = dead.nodes[&node_b].liveness.unwrap();
    assert!(!dead.nodes[&node_b].is_alive());
    assert!(verdict.witness >= 1);
    assert!(verdict.decided_at > 0);
    assert_eq!(dead.nodes[&node_b].enrollment.generation, 1);
    // Host a is untouched by b's loss.
    assert!(dead.nodes[&node_a].is_alive());
    let view = founder.handles.liveness.view();
    assert_eq!(view.member(node_b).unwrap().status, MemberStatus::Dead);
    assert_eq!(
        view.member(node_b).unwrap().incarnation,
        verdict.incarnation
    );
    assert!(
        view.events.iter().any(|event| {
            matches!(event, LivenessEvent::ProbeTimeout { node } if *node == node_b)
        })
    );
    assert!(view.events.iter().any(|event| {
        matches!(event, LivenessEvent::Suspected { node, .. } if *node == node_b)
    }));
    assert!(
        view.events
            .iter()
            .any(|event| { matches!(event, LivenessEvent::Died { node, .. } if *node == node_b) })
    );
    // A death is never duplicated: the committed fact stays at one verdict.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (still, _, _) = wait_for(&founder, &host, "verdict stable", |_| true)
        .await
        .expect("service ended");
    assert_eq!(still.nodes[&node_b].liveness, Some(verdict));

    // Host b returns with a fresh incarnation and is revived, not re-enrolled.
    let peer_b = Running::start(&peer_settings[1]).await;
    let (revived, _, _) = wait_for(&founder, &host, "host-b revived", |state| {
        state
            .nodes
            .get(&node_b)
            .and_then(|record| record.liveness)
            .is_some_and(|liveness| liveness.alive)
    })
    .await
    .expect("service ended");
    let revival = revived.nodes[&node_b].liveness.unwrap();
    assert!(revival.incarnation > verdict.incarnation);
    assert!(revival.decided_at >= verdict.decided_at);
    assert_eq!(revived.nodes[&node_b].enrollment.generation, 1);
    assert!(revived.nodes[&node_b].is_alive());
    let view = founder.handles.liveness.view();
    assert!(
        view.events.iter().any(|event| {
            matches!(event, LivenessEvent::Revived { node, .. } if *node == node_b)
        })
    );
    // Host a sees the same membership through its own probes and gossip.
    let peer_view = confirmed(&peer_a, &[founder_node, node_b]).await;
    assert!(peer_view.member(node_b).unwrap().incarnation >= revival.incarnation);
    peer_b.stop().await;
    peer_a.stop().await;
    founder.stop().await;
}

fn probe(sender: u64, generation: u64, sequence: u64, config: &LivenessConfig) -> ProbeRequest {
    ProbeRequest {
        schema: PROBE_SCHEMA,
        kind: ProbeKind::Direct,
        sender,
        generation,
        sequence,
        incarnation: 1,
        coordinate: NetworkCoordinate::origin(&config.vivaldi),
        health: LocalHealth::default(),
        extension: None,
        updates: Vec::new(),
    }
}
async fn answer(handle: &LivenessHandle, peer: u64, request: &ProbeRequest) -> ProbeReply {
    let bytes = handle
        .answer(peer, &request.encode().unwrap())
        .await
        .unwrap();
    ProbeReply::decode(&bytes, &handle.config().vivaldi).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probes_bind_their_sender_refute_self_suspicion_and_ration_extensions() {
    let founder_dir = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let peer_settings = settings(dir.path());
    let founder = Running::start(&founder_settings).await;
    let (peer_a, node_a) = join_peer(&founder, founder_dir.path(), "host-a", &peer_settings).await;
    let founder_node = founder.status.node;
    let view = confirmed(&founder, &[node_a]).await;
    let handle = &founder.handles.liveness;
    let config = *handle.config();
    let member = *view.member(node_a).unwrap();

    // A probe speaks only for the node the certificate authorized.
    let request = probe(node_a, member.generation, 7, &config);
    assert_eq!(
        handle
            .answer(founder_node, &request.encode().unwrap())
            .await
            .unwrap_err(),
        crate::liveness::ProbeError::Invalid
    );
    let reply = answer(handle, node_a, &request).await;
    assert_eq!(reply.outcome, ProbeOutcome::Ack);
    assert_eq!(reply.node, founder_node);
    assert_eq!(reply.sequence, 7);
    assert_eq!(reply.generation, view.generation);
    assert_eq!(reply.incarnation, view.incarnation);
    assert!(reply.extension.is_none());

    // A loaded member asks for time: granted once with witnessed progress,
    // rate-limited within a period, refused while it reports overload.
    let mut request = probe(node_a, member.generation, 8, &config);
    request.extension = Some(ExtensionRequest {
        incarnation: member.incarnation,
        witness: 1,
        overloaded: false,
    });
    let reply = answer(handle, node_a, &request).await;
    let Some(ExtensionOutcome::Granted { millis }) = reply.extension else {
        panic!("first extension is granted: {:?}", reply.extension);
    };
    assert!(millis >= config.extension_min_grant_ms);
    let after = handle.view();
    assert_eq!(after.member(node_a).unwrap().extensions, 1);
    assert_eq!(after.member(node_a).unwrap().extended_ms, millis);
    assert!(after.events.iter().any(|event| {
        matches!(event, LivenessEvent::ExtensionGranted { node, .. } if *node == node_a)
    }));
    request.sequence = 9;
    request.extension = Some(ExtensionRequest {
        incarnation: member.incarnation,
        witness: 2,
        overloaded: false,
    });
    let reply = answer(handle, node_a, &request).await;
    assert_eq!(reply.extension, Some(ExtensionOutcome::Denied));
    tokio::time::sleep(config.period + Duration::from_millis(50)).await;
    request.sequence = 10;
    request.extension = Some(ExtensionRequest {
        incarnation: member.incarnation,
        witness: 3,
        overloaded: true,
    });
    let reply = answer(handle, node_a, &request).await;
    assert_eq!(reply.extension, Some(ExtensionOutcome::Denied));
    assert_eq!(handle.view().member(node_a).unwrap().extensions, 1);

    // Gossip that suspects this node at its incarnation is refuted by moving
    // past it; the refutation rides the answer.
    let before = handle.view().incarnation;
    let mut request = probe(node_a, member.generation, 11, &config);
    request.updates = vec![LivenessUpdate {
        node: founder_node,
        generation: view.generation,
        incarnation: before,
        status: MemberStatus::Suspect,
        origin: node_a,
    }];
    let reply = answer(handle, node_a, &request).await;
    assert_eq!(reply.incarnation, before + 1);
    assert!(reply.updates.iter().any(|update| {
        update.node == founder_node
            && update.status == MemberStatus::Alive
            && update.incarnation == before + 1
    }));
    let after = handle.view();
    assert_eq!(after.incarnation, before + 1);
    assert!(after.events.iter().any(|event| {
        matches!(
            event,
            LivenessEvent::SelfRefutation { incarnation, accuser }
                if *incarnation == before + 1 && *accuser == node_a
        )
    }));
    // Stale gossip about an older incarnation changes nothing.
    request.sequence = 12;
    let reply = answer(handle, node_a, &request).await;
    assert_eq!(reply.incarnation, before + 1);
    peer_a.stop().await;
    founder.stop().await;
}

#[test]
fn the_driver_refuses_an_unusable_configuration_and_charges_its_state() {
    let budget = MemoryBudget::new(64 * MIB, 16 * MIB).unwrap();
    let namespace = focal_model::LedgerId {
        tenant: focal_model::TenantId([1; 16]),
        session: focal_model::SessionId([2; 16]),
    };
    let mut bad = LivenessConfig::default();
    bad.timeout_cap_ms = bad.base_timeout_ms - 1;
    assert!(LivenessHandle::channel(&budget, bad, 1, namespace).is_err());
    assert!(LivenessHandle::channel(&budget, LivenessConfig::default(), 0, namespace).is_err());
    let before = budget.stats().used;
    let (handle, driver) =
        LivenessHandle::channel(&budget, LivenessConfig::default(), 1, namespace).unwrap();
    assert!(budget.stats().used > before);
    assert_eq!(handle.view().node, 1);
    assert_eq!(handle.view().generation, 0);
    drop(driver);
    drop(handle);
    assert_eq!(budget.stats().used, before);
}
