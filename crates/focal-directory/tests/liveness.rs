#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Committed liveness verdicts: the partition's rules for them, what the
//! planner, the placement check and the guarantee report make of a dead
//! member, and the checkpoint schemas that carry them.
use focal_directory::*;
use focal_memory::MemoryBudget;
use focal_model::{ContentHash, LedgerId, RouteEpoch, SessionId, SessionSeq, TenantId};
use std::collections::{BTreeMap, BTreeSet};

const VERIFIED: ContentHash = ContentHash([9; 32]);
struct Evidence;
impl AuthorityVerifier for Evidence {
    fn verify_enrollment(&self, value: &NodeEnrollment) -> Result<(), DirectoryError> {
        verify(value.attestation)
    }
    fn verify_session_fence(&self, value: &SessionFence) -> Result<(), DirectoryError> {
        verify(value.record_hash)
    }
    fn verify_replica_ready(&self, value: &ReplicaReady) -> Result<(), DirectoryError> {
        verify(value.attestation)
    }
    fn verify_delegation(&self, value: &DelegationFence) -> Result<(), DirectoryError> {
        verify(value.destination_ready)
    }
    fn verify_custody(&self, value: &CustodyProof) -> Result<(), DirectoryError> {
        verify(value.attestation)
    }
}
fn verify(hash: ContentHash) -> Result<(), DirectoryError> {
    if hash == VERIFIED {
        Ok(())
    } else {
        Err(DirectoryError::UnverifiedAuthority)
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(32 * 1024 * 1024, 4 * 1024 * 1024).unwrap()
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(1),
    }
}
fn enrollment(id: u64) -> NodeEnrollment {
    NodeEnrollment {
        node: id,
        generation: 1,
        region: RegionId::from_u128(u128::from(id)),
        zone: ZoneId::from_u128(u128::from(id)),
        endpoint: format!("node-{id}:443"),
        identity: ContentHash([id as u8; 32]),
        authority_epoch: 1,
        attestation: VERIFIED,
        eligible: true,
    }
}
fn load(id: u64) -> NodeLoad {
    NodeLoad {
        node: id,
        generation: 1,
        report: 1,
        available_memory: 1_000_000,
        active_weight: 1,
        disk_available: 1 << 30,
        capability: 0,
    }
}
fn partition() -> DirectoryPartition {
    DirectoryPartition::new(
        ClusterId::from_u128(1),
        Delegation {
            namespace: NamespaceRange::all(),
            partition: PartitionId::from_u128(1),
            region: RegionId::from_u128(1),
            log_group: LogGroupId::from_u128(101),
            epoch: 1,
            activation: None,
        },
        PartitionConfig::default(),
        budget(),
    )
    .unwrap()
}
fn attempt(
    partition: &mut DirectoryPartition,
    operation: PartitionOperation,
) -> Result<(), DirectoryError> {
    let command = PartitionCommand {
        expected_revision: partition.revision(),
        delegation_epoch: partition.checkpoint().delegation.epoch,
        operation,
    };
    let prepared = partition.prepare(&command, &Evidence)?;
    partition.publish(prepared)
}
fn apply(partition: &mut DirectoryPartition, operation: PartitionOperation) {
    attempt(partition, operation).unwrap();
}
fn liveness(node: u64, alive: bool, incarnation: u64, at: i64) -> PartitionOperation {
    PartitionOperation::Liveness {
        node,
        generation: 1,
        alive,
        incarnation,
        witness: 1,
        decided_at: at,
    }
}
fn policy(failures: u16) -> PlacementPolicy {
    PlacementPolicy {
        durability: DurabilityIntent {
            survive: FailureClass::Node,
            max_failures: failures,
        },
        residency: BTreeSet::new(),
        home_regions: BTreeSet::new(),
        required_memory: 10,
    }
}
fn spec(members: &[u64]) -> PlacementSpec {
    let members: BTreeMap<u64, u64> = members.iter().map(|id| (*id, 1)).collect();
    PlacementSpec {
        policy: policy(u16::try_from(members.len() / 2).unwrap()),
        placement: Placement {
            preferred_leader: *members.keys().next().unwrap(),
            voters: members.clone(),
            materializers: members.clone(),
            content_copies: members,
        },
    }
}
fn fence(spec: &PlacementSpec) -> SessionFence {
    SessionFence {
        kind: SessionFenceKind::Created,
        ledger: ledger(),
        log_group: LogGroupId::from_u128(7),
        operation: OperationId::from_u128(1),
        sequence: SessionSeq(0),
        index: focal_model::RaftIndex(1),
        term: focal_model::RaftTerm(1),
        from_route: RouteEpoch(0),
        to_route: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        placement_digest: placement_digest(spec).unwrap(),
        record_hash: VERIFIED,
    }
}
fn fleet(partition: &mut DirectoryPartition) {
    for id in 1..=3 {
        apply(
            partition,
            PartitionOperation::Enroll {
                node: enrollment(id),
                expected_generation: None,
            },
        );
        apply(partition, PartitionOperation::ReportLoad { load: load(id) });
    }
}

#[test]
fn a_verdict_binds_the_generation_never_regresses_and_only_changes_by_incarnation() {
    let mut partition = partition();
    fleet(&mut partition);
    // No verdict means alive; an "alive" verdict with none recorded is noise.
    assert!(partition.checkpoint().nodes[&2].is_alive());
    assert!(matches!(
        attempt(&mut partition, liveness(2, true, 1, 100)),
        Err(DirectoryError::Duplicate)
    ));
    // The wrong generation, an unknown node and malformed input are refused.
    assert!(matches!(
        attempt(
            &mut partition,
            PartitionOperation::Liveness {
                node: 2,
                generation: 2,
                alive: false,
                incarnation: 1,
                witness: 1,
                decided_at: 100,
            }
        ),
        Err(DirectoryError::StaleNode)
    ));
    assert!(matches!(
        attempt(&mut partition, liveness(9, false, 1, 100)),
        Err(DirectoryError::Missing)
    ));
    assert!(matches!(
        attempt(
            &mut partition,
            PartitionOperation::Liveness {
                node: 2,
                generation: 1,
                alive: false,
                incarnation: 1,
                witness: 0,
                decided_at: 100,
            }
        ),
        Err(DirectoryError::Invalid(_))
    ));
    apply(&mut partition, liveness(2, false, 1, 100));
    let record = &partition.checkpoint().nodes[&2];
    assert!(!record.is_alive());
    assert_eq!(
        record.liveness,
        Some(NodeLiveness {
            alive: false,
            incarnation: 1,
            witness: 1,
            decided_at: 100,
        })
    );
    // The same verdict again, an older incarnation and an older decision
    // are stale; a revival at the same incarnation changes the verdict, and
    // a later incarnation always wins.
    assert!(matches!(
        attempt(&mut partition, liveness(2, false, 1, 101)),
        Err(DirectoryError::StaleNode)
    ));
    assert!(matches!(
        attempt(&mut partition, liveness(2, true, 0, 101)),
        Err(DirectoryError::StaleNode)
    ));
    assert!(matches!(
        attempt(&mut partition, liveness(2, true, 1, 99)),
        Err(DirectoryError::StaleNode)
    ));
    apply(&mut partition, liveness(2, true, 1, 101));
    assert!(partition.checkpoint().nodes[&2].is_alive());
    apply(&mut partition, liveness(2, false, 3, 102));
    assert_eq!(
        partition.checkpoint().nodes[&2]
            .liveness
            .unwrap()
            .incarnation,
        3
    );
    // A load report keeps the verdict; re-enrollment at the next generation
    // starts without one.
    apply(
        &mut partition,
        PartitionOperation::ReportLoad {
            load: NodeLoad {
                report: 2,
                ..load(2)
            },
        },
    );
    assert!(!partition.checkpoint().nodes[&2].is_alive());
    apply(
        &mut partition,
        PartitionOperation::Enroll {
            node: NodeEnrollment {
                generation: 2,
                ..enrollment(2)
            },
            expected_generation: Some(1),
        },
    );
    assert!(partition.checkpoint().nodes[&2].is_alive());
    assert_eq!(partition.checkpoint().nodes[&2].liveness, None);
}

#[test]
fn a_dead_member_is_skipped_by_the_planner_fails_the_placement_and_blocks_the_guarantee() {
    let mut partition = partition();
    fleet(&mut partition);
    let three = spec(&[1, 2, 3]);
    apply(
        &mut partition,
        PartitionOperation::CreateSession {
            ledger: ledger(),
            log_group: LogGroupId::from_u128(7),
            placement: three.clone(),
            authority: fence(&three),
        },
    );
    let nodes = &partition.checkpoint().nodes;
    assert!(verify_placement(&three, nodes, 31).is_ok());
    let proposal = propose_placement(nodes, &policy(1), 31, 1).unwrap();
    assert_eq!(proposal.spec.placement.voters.len(), 3);
    let before = effective_guarantee(&partition.checkpoint().sessions[&ledger()], nodes).unwrap();
    assert_eq!(before.achieved, Some(policy(1).durability));
    assert!(before.blocked_by.is_empty());
    apply(&mut partition, liveness(3, false, 1, 100));
    let nodes = &partition.checkpoint().nodes;
    assert!(matches!(
        verify_placement(&three, nodes, 31),
        Err(DirectoryError::DeadNode)
    ));
    assert!(matches!(
        propose_placement(nodes, &policy(1), 31, 1),
        Err(DirectoryError::NoPlacement)
    ));
    let two = propose_placement(nodes, &policy(0), 31, 1).unwrap();
    assert!(!two.spec.placement.voters.contains_key(&3));
    let after = effective_guarantee(&partition.checkpoint().sessions[&ledger()], nodes).unwrap();
    assert_eq!(
        after.achieved,
        Some(DurabilityIntent {
            survive: FailureClass::Node,
            max_failures: 0
        })
    );
    assert!(
        after
            .blocked_by
            .iter()
            .any(|blocker| { blocker.node == Some(3) && blocker.reason == BlockReason::DeadNode })
    );
    // A creation naming a dead voter is refused outright.
    assert!(matches!(
        attempt(
            &mut partition,
            PartitionOperation::CreateSession {
                ledger: LedgerId {
                    tenant: TenantId::from_u128(1),
                    session: SessionId::from_u128(2),
                },
                log_group: LogGroupId::from_u128(8),
                placement: three.clone(),
                authority: fence(&three),
            }
        ),
        Err(DirectoryError::DeadNode)
    ));
    apply(&mut partition, liveness(3, true, 2, 101));
    assert!(verify_placement(&three, &partition.checkpoint().nodes, 31).is_ok());
}

#[test]
fn schema_two_and_three_checkpoints_both_restore_and_verdicts_survive_a_round_trip() {
    let mut partition = partition();
    fleet(&mut partition);
    apply(&mut partition, liveness(1, false, 4, 100));
    let checkpoint = partition.checkpoint().clone();
    assert_eq!(checkpoint.schema, PARTITION_CHECKPOINT_SCHEMA);
    let bytes = postcard::to_allocvec(&checkpoint).unwrap();
    let decoded = PartitionCheckpoint::decode_any(&bytes).unwrap();
    assert_eq!(decoded, checkpoint);
    let restored =
        DirectoryPartition::restore(decoded, PartitionConfig::default(), budget()).unwrap();
    assert!(!restored.checkpoint().nodes[&1].is_alive());
    // A schema 2 checkpoint (no liveness) restores every node as alive.
    let legacy = PartitionCheckpointV2 {
        schema: 2,
        cluster: checkpoint.cluster,
        delegation: checkpoint.delegation,
        revision: checkpoint.revision,
        sealed: None,
        nodes: checkpoint
            .nodes
            .iter()
            .map(|(id, node)| {
                (
                    *id,
                    NodeRecordV2 {
                        enrollment: node.enrollment.clone(),
                        load: node.load,
                    },
                )
            })
            .collect(),
        sessions: BTreeMap::new(),
    };
    let converted =
        PartitionCheckpoint::decode_any(&postcard::to_allocvec(&legacy).unwrap()).unwrap();
    assert_eq!(converted.schema, PARTITION_CHECKPOINT_SCHEMA);
    assert!(converted.nodes.values().all(|node| node.liveness.is_none()));
    DirectoryPartition::restore(converted, PartitionConfig::default(), budget()).unwrap();
    // A malformed verdict does not restore.
    let mut corrupt = checkpoint.clone();
    corrupt.nodes.get_mut(&1).unwrap().liveness = Some(NodeLiveness {
        alive: false,
        incarnation: 4,
        witness: 0,
        decided_at: 100,
    });
    assert!(matches!(
        DirectoryPartition::restore(corrupt, PartitionConfig::default(), budget()),
        Err(DirectoryError::Invalid(_))
    ));
}
