#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Assignment progress, refusals, retiring copies, the measured guarantee and
//! schema 1 restoration of the partition directory.
use focal_directory::*;
use focal_memory::MemoryBudget;
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
use std::collections::{BTreeMap, BTreeSet};

const VERIFIED: ContentHash = ContentHash([9; 32]);
const OP: OperationId = OperationId::from_u128(2);
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
fn load(id: u64, report: u64, disk: u64) -> NodeLoad {
    NodeLoad {
        node: id,
        generation: 1,
        report,
        available_memory: 1_000_000,
        active_weight: 4 - id,
        disk_available: disk,
    }
}
fn partition(config: PartitionConfig) -> DirectoryPartition {
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
        config,
        budget(),
    )
    .unwrap()
}
fn apply(partition: &mut DirectoryPartition, operation: PartitionOperation) {
    attempt_operation(partition, operation).unwrap();
}
fn attempt_operation(
    partition: &mut DirectoryPartition,
    operation: PartitionOperation,
) -> Result<(), DirectoryError> {
    let command = PartitionCommand {
        expected_revision: partition.revision(),
        delegation_epoch: partition.checkpoint().delegation.epoch,
        operation,
    };
    let encoded = postcard::to_allocvec(&command).unwrap();
    let replay: PartitionCommand = postcard::from_bytes(&encoded).unwrap();
    let prepared = partition.prepare(&replay, &Evidence)?;
    partition.publish(prepared)
}
fn nodes(partition: &mut DirectoryPartition) {
    for id in 1..=3 {
        apply(
            partition,
            PartitionOperation::Enroll {
                node: enrollment(id),
                expected_generation: None,
            },
        );
        apply(
            partition,
            PartitionOperation::ReportLoad {
                load: load(id, 1, 1 << 30),
            },
        );
    }
}
fn policy(class: FailureClass, failures: u16) -> PlacementPolicy {
    PlacementPolicy {
        durability: DurabilityIntent {
            survive: class,
            max_failures: failures,
        },
        residency: BTreeSet::new(),
        home_regions: BTreeSet::new(),
        required_memory: 10,
    }
}
fn single(node: u64) -> PlacementSpec {
    let members = BTreeMap::from([(node, 1)]);
    PlacementSpec {
        policy: policy(FailureClass::Node, 0),
        placement: Placement {
            voters: members.clone(),
            materializers: members.clone(),
            content_copies: members,
            preferred_leader: node,
        },
    }
}
fn fence(
    spec: &PlacementSpec,
    kind: SessionFenceKind,
    op: u128,
    sequence: u64,
    route: u64,
) -> SessionFence {
    SessionFence {
        kind,
        ledger: ledger(),
        log_group: LogGroupId::from_u128(500),
        operation: OperationId::from_u128(op),
        sequence: SessionSeq(sequence),
        index: RaftIndex(sequence + 1),
        term: RaftTerm(1),
        from_route: RouteEpoch(route - 1),
        to_route: RouteEpoch(route),
        membership_epoch: route,
        placement_epoch: route,
        placement_digest: placement_digest(spec).unwrap(),
        record_hash: VERIFIED,
    }
}
fn create(partition: &mut DirectoryPartition) {
    let spec = single(1);
    let authority = fence(&spec, SessionFenceKind::Created, 1, 1, 1);
    apply(
        partition,
        PartitionOperation::CreateSession {
            ledger: ledger(),
            log_group: authority.log_group,
            placement: spec,
            authority,
        },
    );
}
fn change(partition: &mut DirectoryPartition, change: SessionChange) {
    attempt(partition, change).unwrap();
}
fn attempt(
    partition: &mut DirectoryPartition,
    change: SessionChange,
) -> Result<(), DirectoryError> {
    let revision = partition.get(ledger()).unwrap().unwrap().revision;
    attempt_operation(
        partition,
        PartitionOperation::Session {
            ledger: ledger(),
            expected_revision: revision,
            change,
        },
    )
}
fn session(partition: &DirectoryPartition) -> SessionDescriptor {
    partition.get(ledger()).unwrap().unwrap().clone()
}
fn plan(partition: &DirectoryPartition) -> PendingPlacement {
    session(partition).pending.unwrap()
}
fn ready(node: u64, through: u64) -> ReplicaReady {
    ReplicaReady {
        ledger: ledger(),
        operation: OP,
        route_epoch: RouteEpoch(2),
        node,
        node_generation: 1,
        through: SessionSeq(through),
        custody: VERIFIED,
        attestation: VERIFIED,
    }
}
fn progress(
    spec: &PlacementSpec,
    node: u64,
    phase: AssignmentPhase,
    through: u64,
    attempt: u32,
) -> AssignmentProgress {
    AssignmentProgress {
        node,
        node_generation: 1,
        roles: roles_of(&spec.placement, node),
        phase,
        attempt,
        through: SessionSeq(through),
        custody_epoch: if phase.at_least(AssignmentPhase::CustodyVerified) {
            2
        } else {
            0
        },
        refusal: None,
    }
}
fn report(node: u64, phase: AssignmentPhase, through: u64) -> SessionChange {
    SessionChange::Progress {
        operation: OP,
        progress: progress(&regional(), node, phase, through, 1),
    }
}
fn regional() -> PlacementSpec {
    let members: BTreeMap<u64, u64> = (1..=3).map(|id| (id, 1)).collect();
    PlacementSpec {
        policy: policy(FailureClass::Region, 1),
        placement: Placement {
            voters: members.clone(),
            materializers: members.clone(),
            content_copies: BTreeMap::from([(2, 1), (3, 1)]),
            preferred_leader: 3,
        },
    }
}
fn planned(partition: &mut DirectoryPartition, desired: &PlacementSpec) {
    nodes(partition);
    create(partition);
    change(
        partition,
        SessionChange::Plan {
            operation: OP,
            desired: desired.clone(),
            observations: desired
                .placement
                .nodes()
                .into_iter()
                .map(|node| (node, 1))
                .collect(),
        },
    );
}
fn phases(partition: &DirectoryPartition) -> BTreeMap<u64, AssignmentPhase> {
    plan(partition)
        .progress
        .iter()
        .map(|(node, progress)| (*node, progress.phase))
        .collect()
}

#[test]
fn assignments_climb_one_ladder_and_activation_retires_the_copies_left_behind() {
    let mut directory = partition(PartitionConfig::default());
    let desired = regional();
    nodes(&mut directory);
    create(&mut directory);
    // A plan cites the load reports it relied on; it cannot cite the future or
    // a node outside the placement.
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Plan {
                operation: OP,
                desired: desired.clone(),
                observations: BTreeMap::from([(1, 2)]),
            }
        ),
        Err(DirectoryError::StaleNode)
    );
    assert!(matches!(
        attempt(
            &mut directory,
            SessionChange::Plan {
                operation: OP,
                desired: single(1),
                observations: BTreeMap::from([(2, 1)]),
            }
        ),
        Err(DirectoryError::Invalid(_))
    ));
    change(
        &mut directory,
        SessionChange::Plan {
            operation: OP,
            desired: desired.clone(),
            observations: BTreeMap::from([(1, 1), (2, 1), (3, 1)]),
        },
    );
    assert_eq!(plan(&directory).phase, PlacementPhase::Planned);
    assert!(plan(&directory).progress.is_empty());
    assert_eq!(
        attempt(&mut directory, report(1, AssignmentPhase::Installed, 0)),
        Err(DirectoryError::Phase)
    );
    let guarantee =
        effective_guarantee(&session(&directory), &directory.checkpoint().nodes).unwrap();
    assert_eq!(guarantee.desired, desired.policy.durability);
    assert_eq!(
        guarantee.achieved,
        Some(DurabilityIntent {
            survive: FailureClass::Node,
            max_failures: 0
        })
    );
    assert_eq!(guarantee.phase, Some(PlacementPhase::Planned));
    change(
        &mut directory,
        SessionChange::BeginPreparation { operation: OP },
    );
    assert_eq!(plan(&directory).phase, PlacementPhase::Preparing);
    assert_eq!(
        phases(&directory),
        (1..=3).map(|id| (id, AssignmentPhase::Assigned)).collect()
    );
    assert_eq!(
        plan(&directory).progress[&1].roles,
        BTreeSet::from([AssignmentRole::Voter, AssignmentRole::Materializer])
    );
    assert_eq!(
        plan(&directory).progress[&2].roles,
        BTreeSet::from([
            AssignmentRole::Voter,
            AssignmentRole::Materializer,
            AssignmentRole::ContentCopy
        ])
    );
    // Beginning twice is the same beginning.
    change(
        &mut directory,
        SessionChange::BeginPreparation { operation: OP },
    );
    assert_eq!(plan(&directory).phase, PlacementPhase::Preparing);
    for node in 1..=3 {
        change(&mut directory, report(node, AssignmentPhase::Installed, 0));
    }
    assert_eq!(plan(&directory).phase, PlacementPhase::Catchup);
    change(&mut directory, report(1, AssignmentPhase::CaughtUp, 3));
    // Progress never moves backwards within an attempt, and an exact repeat is
    // a no-op rather than a conflict.
    assert_eq!(
        attempt(&mut directory, report(1, AssignmentPhase::Installed, 3)),
        Err(DirectoryError::StaleEpoch)
    );
    assert_eq!(
        attempt(&mut directory, report(1, AssignmentPhase::CaughtUp, 2)),
        Err(DirectoryError::StaleEpoch)
    );
    let revision = session(&directory).revision;
    change(&mut directory, report(1, AssignmentPhase::CaughtUp, 3));
    assert_eq!(session(&directory).revision, revision + 1);
    assert_eq!(plan(&directory).progress[&1].through, SessionSeq(3));
    // Custody is proven by the copy's own signed readiness, never asserted by
    // the controller.
    assert_eq!(
        attempt(
            &mut directory,
            report(1, AssignmentPhase::CustodyVerified, 3)
        ),
        Err(DirectoryError::NotReady)
    );
    assert_eq!(
        attempt(&mut directory, report(1, AssignmentPhase::Promoted, 3)),
        Err(DirectoryError::NotReady)
    );
    for node in 2..=3 {
        change(&mut directory, report(node, AssignmentPhase::CaughtUp, 3));
    }
    assert_eq!(plan(&directory).phase, PlacementPhase::Custody);
    for node in 1..=3 {
        change(
            &mut directory,
            SessionChange::Ready {
                ready: ready(node, 4),
            },
        );
        assert_eq!(
            plan(&directory).progress[&node].phase,
            AssignmentPhase::CustodyVerified
        );
        assert_eq!(plan(&directory).progress[&node].through, SessionSeq(4));
        assert_eq!(plan(&directory).progress[&node].custody_epoch, 2);
    }
    assert_eq!(plan(&directory).phase, PlacementPhase::Promoting);
    // Reported rows must describe the assignment they advance.
    let mut wrong_roles = progress(&desired, 1, AssignmentPhase::Promoted, 4, 1);
    wrong_roles.roles.insert(AssignmentRole::ContentCopy);
    assert!(matches!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: wrong_roles
            }
        ),
        Err(DirectoryError::Invalid(_))
    ));
    let mut wrong_epoch = progress(&desired, 1, AssignmentPhase::Promoted, 4, 1);
    wrong_epoch.custody_epoch = 1;
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: wrong_epoch
            }
        ),
        Err(DirectoryError::StaleEpoch)
    );
    let mut stale_generation = progress(&desired, 1, AssignmentPhase::Promoted, 4, 1);
    stale_generation.node_generation = 2;
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: stale_generation
            }
        ),
        Err(DirectoryError::StaleEpoch)
    );
    let mut outsider = progress(&desired, 4, AssignmentPhase::Installed, 0, 1);
    outsider.roles = BTreeSet::from([AssignmentRole::Voter]);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: outsider
            }
        ),
        Err(DirectoryError::StaleEpoch)
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: progress(&desired, 1, AssignmentPhase::Active, 4, 1)
            }
        ),
        Err(DirectoryError::Phase)
    );
    let barrier = fence(&desired, SessionFenceKind::Cutover, 2, 6, 2);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Cutover {
                operation: OP,
                authority: barrier.clone()
            }
        ),
        Err(DirectoryError::NotReady)
    );
    for node in 1..=3 {
        change(&mut directory, report(node, AssignmentPhase::Promoted, 4));
    }
    let guarantee =
        effective_guarantee(&session(&directory), &directory.checkpoint().nodes).unwrap();
    assert_eq!(
        guarantee.blocked_by,
        vec![Blocker {
            node: None,
            reason: BlockReason::AwaitingCutover
        }]
    );
    change(
        &mut directory,
        SessionChange::Cutover {
            operation: OP,
            authority: barrier,
        },
    );
    assert_eq!(plan(&directory).phase, PlacementPhase::Cutover);
    // A refusal cannot fail an assignment once the log has cut over.
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: Refusal {
                    operation: OP,
                    code: RefusalCode::Unreachable,
                    node: Some(2),
                    attempt: 1,
                    at: 7,
                },
            }
        ),
        Err(DirectoryError::Phase)
    );
    let activation = fence(&desired, SessionFenceKind::Activated, 2, 7, 2);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Activate {
                operation: OP,
                authority: activation.clone()
            }
        ),
        Err(DirectoryError::NotReady)
    );
    let guarantee =
        effective_guarantee(&session(&directory), &directory.checkpoint().nodes).unwrap();
    assert_eq!(guarantee.blocked_by.len(), 4);
    assert!(guarantee.blocked_by.iter().all(|blocker| matches!(
        blocker.reason,
        BlockReason::Assignment(AssignmentPhase::Promoted) | BlockReason::AwaitingActivation
    )));
    for node in 1..=3 {
        change(
            &mut directory,
            SessionChange::Ready {
                ready: ready(node, 6),
            },
        );
    }
    change(
        &mut directory,
        SessionChange::Activate {
            operation: OP,
            authority: activation.clone(),
        },
    );
    let active = session(&directory);
    assert_eq!(active.active, desired);
    assert!(active.pending.is_none());
    assert!(active.retiring.is_empty());
    let guarantee = effective_guarantee(&active, &directory.checkpoint().nodes).unwrap();
    assert_eq!(guarantee.achieved, Some(desired.policy.durability));
    assert!(guarantee.blocked_by.is_empty());
    assert_eq!(guarantee.phase, None);

    // Shrinking back to one node leaves two copies to drain and retire.
    let shrunk = single(3);
    change(
        &mut directory,
        SessionChange::Plan {
            operation: OperationId::from_u128(3),
            desired: shrunk.clone(),
            observations: BTreeMap::new(),
        },
    );
    change(
        &mut directory,
        SessionChange::BeginPreparation {
            operation: OperationId::from_u128(3),
        },
    );
    let mut ready_three = ready(3, 8);
    ready_three.operation = OperationId::from_u128(3);
    ready_three.route_epoch = RouteEpoch(3);
    change(
        &mut directory,
        SessionChange::Ready {
            ready: ready_three.clone(),
        },
    );
    let mut promoted = progress(&shrunk, 3, AssignmentPhase::Promoted, 8, 1);
    promoted.custody_epoch = 3;
    change(
        &mut directory,
        SessionChange::Progress {
            operation: OperationId::from_u128(3),
            progress: promoted,
        },
    );
    let barrier = fence(&shrunk, SessionFenceKind::Cutover, 3, 8, 3);
    change(
        &mut directory,
        SessionChange::Cutover {
            operation: OperationId::from_u128(3),
            authority: barrier,
        },
    );
    let activation = fence(&shrunk, SessionFenceKind::Activated, 3, 9, 3);
    change(
        &mut directory,
        SessionChange::Activate {
            operation: OperationId::from_u128(3),
            authority: activation,
        },
    );
    let active = session(&directory);
    assert_eq!(
        active
            .retiring
            .iter()
            .map(|(node, copy)| (*node, copy.phase, copy.custody_epoch, copy.through))
            .collect::<Vec<_>>(),
        vec![
            (1, AssignmentPhase::Active, 2, SessionSeq(8)),
            (2, AssignmentPhase::Active, 2, SessionSeq(8))
        ]
    );
    assert_eq!(
        active.retiring[&2].roles,
        BTreeSet::from([
            AssignmentRole::Voter,
            AssignmentRole::Materializer,
            AssignmentRole::ContentCopy
        ])
    );
    let guarantee = effective_guarantee(&active, &directory.checkpoint().nodes).unwrap();
    assert_eq!(
        guarantee.blocked_by,
        vec![
            Blocker {
                node: Some(1),
                reason: BlockReason::Draining
            },
            Blocker {
                node: Some(2),
                reason: BlockReason::Draining
            }
        ]
    );
    let wrong = OperationId::from_u128(2);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Drain {
                operation: wrong,
                node: 1
            }
        ),
        Err(DirectoryError::WrongOperation)
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Drain {
                operation: OperationId::from_u128(3),
                node: 3
            }
        ),
        Err(DirectoryError::Missing)
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Retire {
                operation: OperationId::from_u128(3),
                node: 1
            }
        ),
        Err(DirectoryError::Phase)
    );
    change(
        &mut directory,
        SessionChange::Drain {
            operation: OperationId::from_u128(3),
            node: 1,
        },
    );
    assert_eq!(
        session(&directory).retiring[&1].phase,
        AssignmentPhase::Draining
    );
    change(
        &mut directory,
        SessionChange::Retire {
            operation: OperationId::from_u128(3),
            node: 1,
        },
    );
    assert!(!session(&directory).retiring.contains_key(&1));
    // Retiring again after a lost reply finds nothing to do.
    change(
        &mut directory,
        SessionChange::Retire {
            operation: OperationId::from_u128(3),
            node: 1,
        },
    );
    assert_eq!(session(&directory).retiring.len(), 1);
    // A restored checkpoint carries every row unchanged.
    let restored = DirectoryPartition::restore(
        directory.checkpoint().clone(),
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    assert_eq!(restored.checkpoint(), directory.checkpoint());
    assert_eq!(restored.checkpoint().schema, PARTITION_CHECKPOINT_SCHEMA);
}

#[test]
fn refusals_fail_one_attempt_and_the_record_stays_bounded() {
    let config = PartitionConfig {
        max_refusals: 2,
        ..PartitionConfig::default()
    };
    let mut directory = partition(config);
    let desired = regional();
    planned(&mut directory, &desired);
    let refusal = |node: Option<u64>, attempt: u32, code: RefusalCode, op: u128| Refusal {
        operation: OperationId::from_u128(op),
        code,
        node,
        attempt,
        at: 100,
    };
    // Nothing is assigned before preparation begins.
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: refusal(Some(2), 1, RefusalCode::DiskCapacity, 2),
            }
        ),
        Err(DirectoryError::Phase)
    );
    change(
        &mut directory,
        SessionChange::BeginPreparation { operation: OP },
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: refusal(Some(2), 2, RefusalCode::DiskCapacity, 2),
            }
        ),
        Err(DirectoryError::StaleEpoch)
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: refusal(Some(2), 1, RefusalCode::DiskCapacity, 3),
            }
        ),
        Err(DirectoryError::WrongOperation)
    );
    change(
        &mut directory,
        SessionChange::Refuse {
            operation: OP,
            refusal: refusal(Some(2), 1, RefusalCode::DiskCapacity, 2),
        },
    );
    let plan_now = plan(&directory);
    assert_eq!(plan_now.phase, PlacementPhase::Failed);
    assert_eq!(plan_now.progress[&2].phase, AssignmentPhase::Failed);
    assert_eq!(
        plan_now.progress[&2].refusal,
        Some(RefusalCode::DiskCapacity)
    );
    assert_eq!(session(&directory).refusals.len(), 1);
    // The same refusal again changes nothing; a second refusal of a failed
    // attempt is a phase error.
    let revision = session(&directory).revision;
    change(
        &mut directory,
        SessionChange::Refuse {
            operation: OP,
            refusal: refusal(Some(2), 1, RefusalCode::DiskCapacity, 2),
        },
    );
    assert_eq!(session(&directory).revision, revision + 1);
    assert_eq!(session(&directory).refusals.len(), 1);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: refusal(Some(2), 1, RefusalCode::Unreachable, 2),
            }
        ),
        Err(DirectoryError::Phase)
    );
    // The failed attempt accepts no more progress; a new attempt restarts.
    assert_eq!(
        attempt(&mut directory, report(2, AssignmentPhase::Installed, 0)),
        Err(DirectoryError::Phase)
    );
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Cutover {
                operation: OP,
                authority: fence(&desired, SessionFenceKind::Cutover, 2, 6, 2),
            }
        ),
        Err(DirectoryError::NotReady)
    );
    let guarantee =
        effective_guarantee(&session(&directory), &directory.checkpoint().nodes).unwrap();
    assert!(guarantee.blocked_by.contains(&Blocker {
        node: Some(2),
        reason: BlockReason::Refused(RefusalCode::DiskCapacity)
    }));
    assert_eq!(guarantee.phase, Some(PlacementPhase::Failed));
    change(
        &mut directory,
        SessionChange::Progress {
            operation: OP,
            progress: progress(&desired, 2, AssignmentPhase::Assigned, 0, 2),
        },
    );
    assert_eq!(plan(&directory).phase, PlacementPhase::Preparing);
    assert_eq!(plan(&directory).progress[&2].attempt, 2);
    assert_eq!(plan(&directory).progress[&2].refusal, None);
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Progress {
                operation: OP,
                progress: progress(&desired, 2, AssignmentPhase::Installed, 0, 1),
            }
        ),
        Err(DirectoryError::StaleEpoch)
    );
    // Plan-level refusals name no node and never the live plan; the ring
    // keeps only the newest entries.
    assert_eq!(
        attempt(
            &mut directory,
            SessionChange::Refuse {
                operation: OP,
                refusal: refusal(None, 1, RefusalCode::NoPlacement, 2),
            }
        ),
        Err(DirectoryError::WrongOperation)
    );
    for op in 10..13 {
        change(
            &mut directory,
            SessionChange::Refuse {
                operation: OperationId::from_u128(op),
                refusal: refusal(None, 1, RefusalCode::NoPlacement, op),
            },
        );
    }
    let recorded: Vec<_> = session(&directory)
        .refusals
        .iter()
        .map(|refusal| refusal.operation)
        .collect();
    assert_eq!(
        recorded,
        vec![OperationId::from_u128(11), OperationId::from_u128(12)]
    );
    change(&mut directory, SessionChange::Abort { operation: OP });
    assert!(session(&directory).pending.is_none());
    let guarantee =
        effective_guarantee(&session(&directory), &directory.checkpoint().nodes).unwrap();
    assert_eq!(guarantee.blocked_by.len(), 2);
    assert!(guarantee.blocked_by.iter().all(|blocker| blocker
        == &Blocker {
            node: None,
            reason: BlockReason::Refused(RefusalCode::NoPlacement)
        }));
    let restored =
        DirectoryPartition::restore(directory.checkpoint().clone(), config, budget()).unwrap();
    assert_eq!(restored.checkpoint(), directory.checkpoint());
    let tight = PartitionConfig {
        max_refusals: 1,
        ..config
    };
    assert_eq!(
        DirectoryPartition::restore(directory.checkpoint().clone(), tight, budget()).err(),
        Some(DirectoryError::Capacity)
    );
}

#[test]
fn the_measured_guarantee_never_exceeds_what_live_nodes_provide() {
    let mut directory = partition(PartitionConfig::default());
    let desired = regional();
    planned(&mut directory, &desired);
    change(
        &mut directory,
        SessionChange::BeginPreparation { operation: OP },
    );
    for node in 1..=3 {
        change(
            &mut directory,
            SessionChange::Ready {
                ready: ready(node, 5),
            },
        );
        change(&mut directory, report(node, AssignmentPhase::Promoted, 5));
    }
    change(
        &mut directory,
        SessionChange::Cutover {
            operation: OP,
            authority: fence(&desired, SessionFenceKind::Cutover, 2, 5, 2),
        },
    );
    change(
        &mut directory,
        SessionChange::Activate {
            operation: OP,
            authority: fence(&desired, SessionFenceKind::Activated, 2, 6, 2),
        },
    );
    let nodes = directory.checkpoint().nodes.clone();
    let active = session(&directory);
    assert_eq!(
        effective_guarantee(&active, &nodes).unwrap().achieved,
        Some(DurabilityIntent {
            survive: FailureClass::Region,
            max_failures: 1
        })
    );
    // A re-enrolled node no longer holds the incarnation the placement names.
    let mut stale = nodes.clone();
    stale.get_mut(&2).unwrap().enrollment.generation = 2;
    let report = effective_guarantee(&active, &stale).unwrap();
    assert_eq!(
        report.achieved,
        Some(DurabilityIntent {
            survive: FailureClass::Region,
            max_failures: 0
        })
    );
    assert_eq!(report.blocked_by.len(), 3);
    assert!(report.blocked_by.iter().all(|blocker| blocker
        == &Blocker {
            node: Some(2),
            reason: BlockReason::StaleNode
        }));
    // Two voters gone leaves no quorum at all.
    let mut lost = stale.clone();
    lost.remove(&3);
    let report = effective_guarantee(&active, &lost).unwrap();
    assert_eq!(report.achieved.unwrap().max_failures, 0);
    assert!(report.blocked_by.contains(&Blocker {
        node: Some(3),
        reason: BlockReason::MissingNode
    }));
    // A member whose promised failure domain is unknown cannot be measured.
    let mut unknown = nodes.clone();
    unknown.get_mut(&1).unwrap().enrollment.region = RegionId::UNKNOWN;
    let report = effective_guarantee(&active, &unknown).unwrap();
    assert_eq!(report.achieved, None);
    assert_eq!(
        report.blocked_by,
        vec![
            Blocker {
                node: Some(1),
                reason: BlockReason::UnknownDomain
            },
            Blocker {
                node: Some(1),
                reason: BlockReason::UnknownDomain
            }
        ]
    );
    // Ineligible nodes count as lost.
    let mut ineligible = nodes;
    ineligible.get_mut(&1).unwrap().enrollment.eligible = false;
    let report = effective_guarantee(&active, &ineligible).unwrap();
    assert_eq!(report.achieved.unwrap().max_failures, 0);
    assert!(report.blocked_by.contains(&Blocker {
        node: Some(1),
        reason: BlockReason::IneligibleNode
    }));
}

#[test]
fn schema_one_checkpoints_restore_with_progress_derived_from_readiness() {
    let mut directory = partition(PartitionConfig::default());
    let desired = regional();
    planned(&mut directory, &desired);
    change(
        &mut directory,
        SessionChange::BeginPreparation { operation: OP },
    );
    for node in 1..=3 {
        change(
            &mut directory,
            SessionChange::Ready {
                ready: ready(node, 5),
            },
        );
        change(&mut directory, report(node, AssignmentPhase::Promoted, 5));
    }
    let barrier = fence(&desired, SessionFenceKind::Cutover, 2, 5, 2);
    change(
        &mut directory,
        SessionChange::Cutover {
            operation: OP,
            authority: barrier.clone(),
        },
    );
    let current = directory.checkpoint().clone();
    let descriptor = &current.sessions[&ledger()];
    let pending = descriptor.pending.as_ref().unwrap();
    let legacy = PartitionCheckpointV1 {
        schema: 1,
        cluster: current.cluster,
        delegation: current.delegation,
        revision: current.revision,
        sealed: None,
        nodes: current
            .nodes
            .iter()
            .map(|(id, node)| {
                (
                    *id,
                    NodeRecordV1 {
                        enrollment: node.enrollment.clone(),
                        load: node.load.map(|load| NodeLoadV1 {
                            node: load.node,
                            generation: load.generation,
                            report: load.report,
                            available_memory: load.available_memory,
                            active_weight: load.active_weight,
                        }),
                    },
                )
            })
            .collect(),
        sessions: BTreeMap::from([(
            ledger(),
            SessionDescriptorV1 {
                ledger: descriptor.ledger,
                log_group: descriptor.log_group,
                revision: descriptor.revision,
                route_epoch: descriptor.route_epoch,
                membership_epoch: descriptor.membership_epoch,
                placement_epoch: descriptor.placement_epoch,
                active: descriptor.active.clone(),
                authority: descriptor.authority.clone(),
                pending: Some(PendingPlacementV1 {
                    operation: pending.operation,
                    next_route: pending.next_route,
                    next_membership: pending.next_membership,
                    next_placement: pending.next_placement,
                    desired: pending.desired.clone(),
                    phase: PlacementPhaseV1::Preparing,
                    ready: pending.ready.clone(),
                    barrier: Some(barrier),
                }),
            },
        )]),
    };
    let bytes = postcard::to_allocvec(&legacy).unwrap();
    let converted = PartitionCheckpoint::decode_any(&bytes).unwrap();
    assert_eq!(converted.schema, PARTITION_CHECKPOINT_SCHEMA);
    let restored =
        DirectoryPartition::restore(converted.clone(), PartitionConfig::default(), budget())
            .unwrap();
    let plan = restored
        .get(ledger())
        .unwrap()
        .unwrap()
        .pending
        .clone()
        .unwrap();
    assert_eq!(plan.phase, PlacementPhase::Cutover);
    assert!(plan.observations.is_empty());
    for node in 1..=3 {
        assert_eq!(plan.progress[&node].phase, AssignmentPhase::Promoted);
        assert_eq!(plan.progress[&node].through, SessionSeq(5));
        assert_eq!(plan.progress[&node].custody_epoch, 2);
    }
    assert_eq!(
        restored.checkpoint().nodes[&1].load.unwrap().disk_available,
        0
    );
    // The converted state continues exactly where schema 1 stopped.
    let mut restored = restored;
    change(
        &mut restored,
        SessionChange::Activate {
            operation: OP,
            authority: fence(&desired, SessionFenceKind::Activated, 2, 6, 2),
        },
    );
    assert_eq!(session(&restored).active, desired);
    // Schema 2 bytes decode directly; unknown schemas and trailing bytes fail.
    let current_bytes = postcard::to_allocvec(&current).unwrap();
    assert_eq!(
        PartitionCheckpoint::decode_any(&current_bytes).unwrap(),
        current
    );
    let mut trailing = current_bytes.clone();
    trailing.push(0);
    assert!(matches!(
        PartitionCheckpoint::decode_any(&trailing),
        Err(DirectoryError::Invalid(_))
    ));
    let mut future = current;
    future.schema = PARTITION_CHECKPOINT_SCHEMA + 1;
    assert!(matches!(
        PartitionCheckpoint::decode_any(&postcard::to_allocvec(&future).unwrap()),
        Err(DirectoryError::Invalid(_))
    ));
    assert!(matches!(
        DirectoryPartition::restore(future, PartitionConfig::default(), budget()),
        Err(DirectoryError::Invalid(_))
    ));
    // A schema 1 fence over a voter that never reported readiness cannot be
    // represented; the restore refuses rather than inventing custody.
    let mut orphaned = legacy;
    orphaned
        .sessions
        .get_mut(&ledger())
        .unwrap()
        .pending
        .as_mut()
        .unwrap()
        .ready
        .remove(&2);
    assert_eq!(
        PartitionCheckpoint::decode_any(&postcard::to_allocvec(&orphaned).unwrap()).err(),
        Some(DirectoryError::Phase)
    );
}

#[test]
fn the_planner_skips_nodes_without_disk_headroom_and_prefers_roomier_ones() {
    let mut directory = partition(PartitionConfig::default());
    for id in 1..=3 {
        apply(
            &mut directory,
            PartitionOperation::Enroll {
                node: enrollment(id),
                expected_generation: None,
            },
        );
    }
    for (id, disk) in [(1, 0), (2, 1 << 20), (3, 1 << 30)] {
        let mut load = load(id, 1, disk);
        load.active_weight = 1;
        apply(&mut directory, PartitionOperation::ReportLoad { load });
    }
    let nodes = &directory.checkpoint().nodes;
    assert_eq!(
        propose_placement(nodes, &policy(FailureClass::Region, 1), 31, 1),
        Err(DirectoryError::NoPlacement)
    );
    let proposal = propose_placement(nodes, &policy(FailureClass::Region, 1), 31, 0).unwrap();
    assert_eq!(proposal.spec.placement.voters.len(), 3);
    let roomy = propose_placement(nodes, &policy(FailureClass::Node, 0), 31, 1).unwrap();
    assert_eq!(roomy.spec.placement.preferred_leader, 3);
    assert_eq!(roomy.observations, BTreeMap::from([(3, 1)]));
    let checkpoint = directory.checkpoint().clone();
    let digest = partition_checkpoint_digest(&checkpoint).unwrap();
    assert_ne!(digest, ContentHash([0; 32]));
    assert_eq!(checkpoint.nodes[&1].load.unwrap().disk_available, 0);
}

/// Published holders (25 §9): monotone per session, idempotent for the same
/// publication, conflicting at the same epoch, in key order with unique
/// identities, and every holding replica a member of the active placement
/// at its enrolled generation.
#[test]
fn holders_publish_in_epoch_order_for_placement_members_only() {
    let mut directory = partition(PartitionConfig::default());
    nodes(&mut directory);
    create(&mut directory);
    let holder =
        |member: u128, start: Option<u8>, node: Option<u64>, generation: Option<u64>| RangeHolder {
            member: focal_memory::RangeId::from_u128(member),
            start: start.map(|byte| [byte; 16]),
            node,
            generation,
        };
    let publish = |epoch: u64, members: Vec<RangeHolder>| SessionChange::Holders {
        holders: RangeHolders { epoch, members },
    };
    assert!(session(&directory).holders.is_none());
    // Epoch zero, no members, a first member with a start, or a later one
    // without: refused as invalid.
    for bad in [
        publish(0, vec![holder(1, None, None, None)]),
        publish(1, Vec::new()),
        publish(1, vec![holder(1, Some(4), None, None)]),
        publish(
            1,
            vec![holder(1, None, None, None), holder(2, None, None, None)],
        ),
        // Descending affinities, a repeated identity, a node without its
        // generation.
        publish(
            1,
            vec![
                holder(1, None, None, None),
                holder(2, Some(9), None, None),
                holder(3, Some(4), None, None),
            ],
        ),
        publish(
            1,
            vec![holder(1, None, None, None), holder(1, Some(4), None, None)],
        ),
        publish(1, vec![holder(1, None, Some(1), None)]),
    ] {
        assert_eq!(
            attempt(&mut directory, bad),
            Err(DirectoryError::Invalid("range holders"))
        );
    }
    // A holder outside the active placement, or at another generation.
    assert_eq!(
        attempt(
            &mut directory,
            publish(1, vec![holder(1, None, Some(2), Some(1))])
        ),
        Err(DirectoryError::Missing)
    );
    assert_eq!(
        attempt(
            &mut directory,
            publish(1, vec![holder(1, None, Some(1), Some(2))])
        ),
        Err(DirectoryError::StaleNode)
    );
    // The voters hold the one member at epoch one.
    change(
        &mut directory,
        publish(1, vec![holder(1, None, None, None)]),
    );
    let published = session(&directory).holders.unwrap();
    assert_eq!(published.epoch, 1);
    assert_eq!(published.members, vec![holder(1, None, None, None)]);
    // The same publication is idempotent; a different one at the same epoch
    // conflicts; an older epoch is stale.
    change(
        &mut directory,
        publish(1, vec![holder(1, None, None, None)]),
    );
    assert_eq!(
        attempt(
            &mut directory,
            publish(1, vec![holder(1, None, Some(1), Some(1))])
        ),
        Err(DirectoryError::CompareFailed)
    );
    // Epoch three: the member moved to node one's replica and split.
    change(
        &mut directory,
        publish(
            3,
            vec![
                holder(1, None, Some(1), Some(1)),
                holder(7, Some(8), Some(1), Some(1)),
            ],
        ),
    );
    assert_eq!(
        attempt(
            &mut directory,
            publish(2, vec![holder(1, None, None, None)])
        ),
        Err(DirectoryError::StaleEpoch)
    );
    let published = session(&directory).holders.unwrap();
    assert_eq!(published.epoch, 3);
    assert_eq!(published.members.len(), 2);
    assert_eq!(
        published.members[1].member,
        focal_memory::RangeId::from_u128(7)
    );
    // A checkpoint carries the publication; a schema 6 checkpoint restores
    // with none.
    let checkpoint = directory.checkpoint().clone();
    assert_eq!(checkpoint.schema, PARTITION_CHECKPOINT_SCHEMA);
    let bytes = postcard::to_stdvec(&checkpoint).unwrap();
    let restored = PartitionCheckpoint::decode_any(&bytes).unwrap();
    assert_eq!(restored.sessions[&ledger()].holders, Some(published));
    let older = PartitionCheckpointV6 {
        schema: 6,
        cluster: checkpoint.cluster,
        delegation: checkpoint.delegation,
        revision: checkpoint.revision,
        sealed: checkpoint.sealed.clone(),
        nodes: checkpoint.nodes.clone(),
        sessions: checkpoint
            .sessions
            .iter()
            .map(|(ledger, session)| (*ledger, SessionDescriptorV6::from(session.clone())))
            .collect(),
        routes: checkpoint.routes.clone(),
        routes_from: checkpoint.routes_from,
    };
    let bytes = postcard::to_stdvec(&older).unwrap();
    let restored = PartitionCheckpoint::decode_any(&bytes).unwrap();
    assert_eq!(restored.schema, PARTITION_CHECKPOINT_SCHEMA);
    assert!(restored.sessions[&ledger()].holders.is_none());
    assert_eq!(restored.sessions[&ledger()].founder, Some(1));
}
