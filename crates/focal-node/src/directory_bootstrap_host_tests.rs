use super::*;
use crate::{config::Settings, network_bootstrap::FoundingNetwork};
use focal_consensus::{MessageType, NodeConfig};

/// A root that leads a term it has committed nothing in holds the directory's barrier read, as Raft's
/// read index waits for a leader's first commit in its term (Ongaro's thesis §6.4), where it refused
/// it `NotReady` (a raft-rs leader dropped such a read; focal's core defers it): this leader can
/// commit nothing, so a preparation asked while the barrier waits is refused for capacity, one at
/// a time, and the barrier is answered `Unavailable` at its request's time, which the bootstrap's
/// rounds retry as they retried `NotReady`; the host serves on.
#[test]
fn elected_root_without_current_term_commit_holds_the_directory_barrier_and_answers_retryably() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let mut settings = Settings::default();
        settings.node.data_dir = Some(directory.path().join("founder"));
        settings.node.advertise = Some("127.0.0.1:7443".into());
        let network = FoundingNetwork::open(&settings).await.unwrap();
        let founder = network.directory.identity();
        let plan = FirstDirectoryPlan::derive(founder.cluster, founder.node).unwrap();
        let nodes = vec![founder.node, founder.node + 1, founder.node + 2];
        let mut replicas = Vec::new();
        for node in &nodes {
            let mut replica = ControlReplica::open(
                ControlOptions::new(NodeConfig::joining(
                    *node,
                    founder.cluster,
                    network.state.genesis.root.group,
                    nodes.clone(),
                    vec![],
                )),
                network.state.genesis.bootstrap.clone(),
                MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
                directory.path().join(format!("replica-{node}")),
            )
            .unwrap();
            replica
                .drain(&crate::cluster::NoDirectoryAuthority)
                .unwrap();
            replicas.push(replica);
        }
        replicas[0].campaign().unwrap();
        // Deliver actual durable voting traffic, but hold back append/heartbeat
        // traffic so this elected leader cannot commit its current-term no-op.
        for _ in 0..16 {
            let mut votes = Vec::new();
            for replica in &mut replicas {
                let events = replica
                    .drain(&crate::cluster::NoDirectoryAuthority)
                    .unwrap();
                votes.extend(events.messages.into_iter().filter(|message| {
                    matches!(
                        message.msg_type,
                        MessageType::MsgRequestPreVote
                            | MessageType::MsgRequestPreVoteResponse
                            | MessageType::MsgRequestVote
                            | MessageType::MsgRequestVoteResponse
                    )
                }));
            }
            for message in votes {
                replicas
                    .iter_mut()
                    .find(|replica| replica.status().node_id == message.to)
                    .unwrap()
                    .step(message)
                    .unwrap();
            }
        }
        let replica = replicas.remove(0);
        assert_eq!(replica.status().role, StateRole::Leader);
        assert_eq!(replica.status().committed_index, 0);
        let budget = MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap();
        let mut config = ControlHostConfig::new(network.state.genesis.root_namespace);
        config.tick = Duration::from_secs(1);
        config.request_timeout = Duration::from_secs(2);
        let (host, owner, _outgoing) = ControlHost::spawn(
            replica,
            crate::cluster::NoDirectoryAuthority,
            config,
            budget,
        )
        .unwrap();
        let (held, behind) =
            hyper_rt::combine::join2(host.prepare_directory(plan), host.prepare_directory(plan))
                .await;
        assert!(
            matches!(held, Err(DirectoryBootstrapError::Unavailable)),
            "{:?}",
            held.as_ref().err()
        );
        assert!(
            matches!(behind, Err(DirectoryBootstrapError::Capacity)),
            "{:?}",
            behind.as_ref().err()
        );
        assert!(!host.progress().stopped);
        let again = host.prepare_directory(plan).await;
        assert!(
            matches!(again, Err(DirectoryBootstrapError::Unavailable)),
            "{:?}",
            again.as_ref().err()
        );
        assert!(!host.progress().stopped);
        host.stop().await.unwrap();
        owner.join().unwrap();
    })
    .unwrap()
}

#[test]
fn directory_admission_normalizes_pressure_without_hiding_corruption_or_failed_owners() {
    use focal_consensus::ConsensusError;
    use focal_memory::MemoryError;
    for error in [
        ControlError::Capacity,
        ControlError::Busy,
        ControlError::Memory(MemoryError::Capacity {
            requested: 2,
            available: 1,
        }),
        ControlError::Memory(MemoryError::AllocationFailed),
        ControlError::Consensus(ConsensusError::Capacity),
    ] {
        assert!(matches!(
            admission_error(error),
            DirectoryBootstrapError::Capacity
        ));
    }
    assert!(matches!(
        admission_error(ControlError::Consensus(ConsensusError::PersistencePending)),
        DirectoryBootstrapError::NotReady
    ));
    assert!(matches!(
        admission_error(ControlError::Consensus(ConsensusError::NotLeader {
            leader: 2
        })),
        DirectoryBootstrapError::Unavailable
    ));
    for error in [
        ControlError::Failed,
        ControlError::Corrupt("state"),
        ControlError::Memory(MemoryError::WrongArena),
        ControlError::Consensus(ConsensusError::DependencyFailure),
    ] {
        assert!(matches!(
            admission_error(error),
            DirectoryBootstrapError::Control(_)
        ));
    }
}
