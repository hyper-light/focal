use super::*;
use crate::{config::Settings, network_bootstrap::FoundingNetwork};
use focal_consensus::{MessageType, NodeConfig};

#[tokio::test]
async fn elected_root_without_current_term_commit_returns_retryable_directory_readiness() {
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
                    message.get_msg_type(),
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
    let (host, owner, _outgoing) = ControlHost::spawn(
        replica,
        crate::cluster::NoDirectoryAuthority,
        config,
        budget,
    )
    .unwrap();
    assert!(matches!(
        host.prepare_directory(plan).await,
        Err(DirectoryBootstrapError::NotReady)
    ));
    assert!(!host.progress().stopped);
    assert!(matches!(
        host.prepare_directory(plan).await,
        Err(DirectoryBootstrapError::NotReady)
    ));
    host.stop().await.unwrap();
    owner.join().unwrap();
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
