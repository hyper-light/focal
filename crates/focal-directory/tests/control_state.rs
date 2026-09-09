#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_directory::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
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
fn ledger(tenant: u128, session: u128) -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(tenant),
        session: SessionId::from_u128(session),
    }
}
fn delegation(id: u128, namespace: NamespaceRange) -> Delegation {
    Delegation {
        namespace,
        partition: PartitionId::from_u128(id),
        region: RegionId::from_u128(1),
        log_group: LogGroupId::from_u128(id + 100),
        epoch: 1,
        activation: None,
    }
}
fn enrollment(id: u64, region: u128) -> NodeEnrollment {
    NodeEnrollment {
        node: id,
        generation: 1,
        region: RegionId::from_u128(region),
        zone: ZoneId::from_u128(id.into()),
        endpoint: format!("node-{id}:443"),
        identity: ContentHash([id as u8; 32]),
        authority_epoch: 1,
        attestation: VERIFIED,
        eligible: true,
    }
}
fn partition(budget: MemoryBudget, id: u128, namespace: NamespaceRange) -> DirectoryPartition {
    DirectoryPartition::new(
        ClusterId::from_u128(1),
        delegation(id, namespace),
        PartitionConfig::default(),
        budget,
    )
    .unwrap()
}
fn apply(partition: &mut DirectoryPartition, operation: PartitionOperation) {
    let command = PartitionCommand {
        expected_revision: partition.revision(),
        delegation_epoch: partition.checkpoint().delegation.epoch,
        operation,
    };
    let encoded = postcard::to_allocvec(&command).unwrap();
    let replay: PartitionCommand = postcard::from_bytes(&encoded).unwrap();
    let prepared = partition.prepare(&replay, &Evidence).unwrap();
    partition.publish(prepared).unwrap();
}
fn nodes(partition: &mut DirectoryPartition) {
    for id in 1..=3 {
        apply(
            partition,
            PartitionOperation::Enroll {
                node: enrollment(id, id.into()),
                expected_generation: None,
            },
        );
        apply(
            partition,
            PartitionOperation::ReportLoad {
                load: NodeLoad {
                    node: id,
                    generation: 1,
                    report: 1,
                    available_memory: 1_000_000,
                    active_weight: 4 - id,
                    disk_available: 1 << 30,
                },
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
fn initial_spec() -> PlacementSpec {
    let single = BTreeMap::from([(1, 1)]);
    PlacementSpec {
        policy: policy(FailureClass::Node, 0),
        placement: Placement {
            voters: single.clone(),
            materializers: single.clone(),
            content_copies: single,
            preferred_leader: 1,
        },
    }
}
fn fence(
    spec: &PlacementSpec,
    kind: SessionFenceKind,
    session: LedgerId,
    op: u128,
    sequence: u64,
    route: u64,
) -> SessionFence {
    SessionFence {
        kind,
        ledger: session,
        log_group: LogGroupId::from_u128(500),
        operation: OperationId::from_u128(op),
        sequence: SessionSeq(sequence),
        index: RaftIndex(sequence),
        term: RaftTerm(1),
        from_route: RouteEpoch(route - 1),
        to_route: RouteEpoch(route),
        membership_epoch: route,
        placement_epoch: route,
        placement_digest: placement_digest(spec).unwrap(),
        record_hash: VERIFIED,
    }
}
fn create(partition: &mut DirectoryPartition, session: LedgerId) {
    let spec = initial_spec();
    let authority = fence(&spec, SessionFenceKind::Created, session, 1, 1, 1);
    apply(
        partition,
        PartitionOperation::CreateSession {
            ledger: session,
            log_group: authority.log_group,
            placement: spec,
            authority,
        },
    );
}
fn change(partition: &mut DirectoryPartition, session: LedgerId, change: SessionChange) {
    let revision = partition.get(session).unwrap().unwrap().revision;
    apply(
        partition,
        PartitionOperation::Session {
            ledger: session,
            expected_revision: revision,
            change,
        },
    );
}
fn ready(session: LedgerId, op: u128, route: u64, node: u64, through: u64) -> ReplicaReady {
    ReplicaReady {
        ledger: session,
        operation: OperationId::from_u128(op),
        route_epoch: RouteEpoch(route),
        node,
        node_generation: 1,
        through: SessionSeq(through),
        custody: VERIFIED,
        attestation: VERIFIED,
    }
}
fn promoted(
    spec: &PlacementSpec,
    node: u64,
    through: u64,
    custody_epoch: u64,
) -> AssignmentProgress {
    AssignmentProgress {
        node,
        node_generation: 1,
        roles: roles_of(&spec.placement, node),
        phase: AssignmentPhase::Promoted,
        attempt: 1,
        through: SessionSeq(through),
        custody_epoch,
        refusal: None,
    }
}
fn attempt(
    partition: &DirectoryPartition,
    session: LedgerId,
    change: SessionChange,
) -> Result<PreparedPartitionUpdate, DirectoryError> {
    partition.prepare(
        &PartitionCommand {
            expected_revision: partition.revision(),
            delegation_epoch: partition.checkpoint().delegation.epoch,
            operation: PartitionOperation::Session {
                ledger: session,
                expected_revision: partition
                    .get(session)?
                    .ok_or(DirectoryError::Missing)?
                    .revision,
                change,
            },
        },
        &Evidence,
    )
}

#[test]
fn independent_namespace_partitions_never_store_or_mutate_each_others_sessions() {
    let split = NamespaceKey::of(ledger(10, 0));
    let mut west = partition(
        budget(),
        1,
        NamespaceRange {
            start: NamespaceKey::MIN,
            end: Some(split),
        },
    );
    let mut east = partition(
        budget(),
        2,
        NamespaceRange {
            start: split,
            end: None,
        },
    );
    nodes(&mut west);
    nodes(&mut east);
    create(&mut west, ledger(1, 1));
    create(&mut east, ledger(10, 1));
    assert_eq!(west.checkpoint().sessions.len(), 1);
    assert_eq!(east.checkpoint().sessions.len(), 1);
    assert_eq!(
        west.get(ledger(10, 1)),
        Err(DirectoryError::OutsideNamespace)
    );
    let east_revision = east.revision();
    let command = PartitionCommand {
        expected_revision: west.revision(),
        delegation_epoch: 2,
        operation: PartitionOperation::ReportLoad {
            load: NodeLoad {
                node: 1,
                generation: 1,
                report: 2,
                available_memory: 100,
                active_weight: 100,
                disk_available: 1 << 30,
            },
        },
    };
    assert!(matches!(
        west.prepare(&command, &Evidence),
        Err(DirectoryError::StaleEpoch)
    ));
    assert_eq!(east.revision(), east_revision);
    let encoded = postcard::to_allocvec(west.checkpoint()).unwrap();
    let recovered = DirectoryPartition::restore(
        postcard::from_bytes(&encoded).unwrap(),
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    assert_eq!(west.checkpoint(), recovered.checkpoint());
}

#[test]
fn placement_remains_active_until_cutover_and_all_exact_incarnations_are_ready() {
    let allowance = budget();
    let mut directory = partition(allowance, 1, NamespaceRange::all());
    nodes(&mut directory);
    let session = ledger(1, 1);
    create(&mut directory, session);
    let original = directory.lookup(session, 1).unwrap();
    let desired = propose_placement(
        &directory.checkpoint().nodes,
        &policy(FailureClass::Region, 1),
        31,
        1,
    )
    .unwrap()
    .spec;
    change(
        &mut directory,
        session,
        SessionChange::Plan {
            operation: OperationId::from_u128(2),
            desired: desired.clone(),
            observations: BTreeMap::new(),
        },
    );
    assert_eq!(
        directory.get(session).unwrap().unwrap().active,
        initial_spec()
    );
    let activation = fence(&desired, SessionFenceKind::Activated, session, 2, 6, 2);
    assert!(matches!(
        attempt(
            &directory,
            session,
            SessionChange::Activate {
                operation: OperationId::from_u128(2),
                authority: activation.clone()
            }
        ),
        Err(DirectoryError::NotReady)
    ));
    change(
        &mut directory,
        session,
        SessionChange::BeginPreparation {
            operation: OperationId::from_u128(2),
        },
    );
    let barrier = fence(&desired, SessionFenceKind::Cutover, session, 2, 5, 2);
    // The cutover fence commits the next membership epoch, so every desired
    // voter must be promoted before the directory records it.
    assert!(matches!(
        attempt(
            &directory,
            session,
            SessionChange::Cutover {
                operation: OperationId::from_u128(2),
                authority: barrier.clone()
            }
        ),
        Err(DirectoryError::NotReady)
    ));
    for node in 1..=3 {
        let through = if node == 3 { 4 } else { 5 };
        change(
            &mut directory,
            session,
            SessionChange::Ready {
                ready: ready(session, 2, 2, node, through),
            },
        );
        change(
            &mut directory,
            session,
            SessionChange::Progress {
                operation: OperationId::from_u128(2),
                progress: promoted(&desired, node, through, 2),
            },
        );
    }
    assert_eq!(
        directory
            .get(session)
            .unwrap()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .phase,
        PlacementPhase::Promoting
    );
    change(
        &mut directory,
        session,
        SessionChange::Cutover {
            operation: OperationId::from_u128(2),
            authority: barrier,
        },
    );
    assert_eq!(
        directory
            .get(session)
            .unwrap()
            .unwrap()
            .pending
            .as_ref()
            .unwrap()
            .phase,
        PlacementPhase::Cutover
    );
    assert!(matches!(
        attempt(
            &directory,
            session,
            SessionChange::Activate {
                operation: OperationId::from_u128(2),
                authority: activation.clone()
            }
        ),
        Err(DirectoryError::NotReady)
    ));
    assert_eq!(
        directory.lookup(session, 1).unwrap().route_epoch,
        original.route_epoch
    );
    change(
        &mut directory,
        session,
        SessionChange::Ready {
            ready: ready(session, 2, 2, 3, 5),
        },
    );
    let mut forged = activation.clone();
    forged.placement_digest = ContentHash([0; 32]);
    assert!(matches!(
        attempt(
            &directory,
            session,
            SessionChange::Activate {
                operation: OperationId::from_u128(2),
                authority: forged
            }
        ),
        Err(DirectoryError::CompareFailed)
    ));
    change(
        &mut directory,
        session,
        SessionChange::Activate {
            operation: OperationId::from_u128(2),
            authority: activation.clone(),
        },
    );
    let active = directory.get(session).unwrap().unwrap();
    assert_eq!(active.active, desired);
    assert_eq!(
        (
            active.route_epoch,
            active.membership_epoch,
            active.placement_epoch
        ),
        (RouteEpoch(2), 2, 2)
    );
    assert!(active.pending.is_none());
    assert!(active.retiring.is_empty());
    // Exact operation can be reconciled after a lost directory response.
    change(
        &mut directory,
        session,
        SessionChange::Activate {
            operation: OperationId::from_u128(2),
            authority: activation,
        },
    );
    let route = directory.lookup(session, 1).unwrap();
    let local = ServingFence {
        node: route.leader,
        ledger: session,
        route_epoch: route.route_epoch,
        placement_epoch: route.placement_epoch,
        node_generation: route.leader_generation,
        activation: route.activation,
        retired_after: None,
    };
    assert_eq!(
        local.check(&original, SessionSeq(7), true),
        Err(DirectoryError::StaleEpoch)
    );
    local.check(&route, SessionSeq(7), true).unwrap();
}

#[test]
fn retired_owner_never_accepts_new_mutations_or_snapshots_after_the_cut() {
    let mut directory = partition(budget(), 1, NamespaceRange::all());
    nodes(&mut directory);
    create(&mut directory, ledger(1, 1));
    let route = directory.lookup(ledger(1, 1), 1).unwrap();
    let fence = ServingFence {
        node: 1,
        ledger: route.ledger,
        route_epoch: route.route_epoch,
        placement_epoch: 1,
        node_generation: 1,
        activation: route.activation,
        retired_after: Some(SessionSeq(5)),
    };
    fence.check(&route, SessionSeq(5), false).unwrap();
    assert_eq!(
        fence.check(&route, SessionSeq(6), false),
        Err(DirectoryError::StaleEpoch)
    );
    assert_eq!(
        fence.check(&route, SessionSeq(4), true),
        Err(DirectoryError::StaleEpoch)
    );
    let mut restarted = fence;
    restarted.node_generation = 2;
    assert_eq!(
        restarted.check(&route, SessionSeq(4), false),
        Err(DirectoryError::StaleEpoch)
    );
}

#[test]
fn membership_and_load_reports_bind_verified_node_incarnations() {
    let mut directory = partition(budget(), 1, NamespaceRange::all());
    nodes(&mut directory);
    let mut forged = enrollment(4, 1);
    forged.attestation = ContentHash([1; 32]);
    let command = PartitionCommand {
        expected_revision: directory.revision(),
        delegation_epoch: 1,
        operation: PartitionOperation::Enroll {
            node: forged,
            expected_generation: None,
        },
    };
    assert!(matches!(
        directory.prepare(&command, &Evidence),
        Err(DirectoryError::UnverifiedAuthority)
    ));
    let mut replacement = enrollment(1, 1);
    replacement.generation = 2;
    apply(
        &mut directory,
        PartitionOperation::Enroll {
            node: replacement,
            expected_generation: Some(1),
        },
    );
    let stale = PartitionCommand {
        expected_revision: directory.revision(),
        delegation_epoch: 1,
        operation: PartitionOperation::ReportLoad {
            load: NodeLoad {
                node: 1,
                generation: 1,
                report: 99,
                available_memory: u64::MAX,
                active_weight: 0,
                disk_available: 1 << 30,
            },
        },
    };
    assert!(matches!(
        directory.prepare(&stale, &Evidence),
        Err(DirectoryError::StaleNode)
    ));
    assert!(directory.checkpoint().nodes[&1].load.is_none());
    assert_eq!(
        verify_placement(&initial_spec(), &directory.checkpoint().nodes, 31),
        Err(DirectoryError::StaleNode)
    );
}

#[test]
fn measured_placement_and_worst_domain_loss_cover_quorum_and_content_independently() {
    let mut directory = partition(budget(), 1, NamespaceRange::all());
    nodes(&mut directory);
    let regional = policy(FailureClass::Region, 1);
    let proposal = propose_placement(&directory.checkpoint().nodes, &regional, 31, 1).unwrap();
    assert_eq!(proposal.spec.placement.preferred_leader, 3); // lowest measured load
    assert_eq!(proposal.observations.len(), 3);
    let mut bad = proposal.spec.clone();
    bad.placement.content_copies = BTreeMap::from([(1, 1)]);
    assert_eq!(
        verify_placement(&bad, &directory.checkpoint().nodes, 31),
        Err(DirectoryError::Custody)
    );
    let mut residency = regional;
    residency.residency = BTreeSet::from([RegionId::from_u128(1), RegionId::from_u128(2)]);
    assert_eq!(
        propose_placement(&directory.checkpoint().nodes, &residency, 31, 1),
        Err(DirectoryError::NoPlacement)
    );
    let mut missing = directory.checkpoint().nodes.clone();
    missing.get_mut(&1).unwrap().enrollment.region = RegionId::from_u128(0);
    assert!(matches!(
        verify_placement(&proposal.spec, &missing, 31),
        Err(DirectoryError::Invalid(_))
    ));
}

#[test]
fn metadata_partition_transfer_is_sealed_cas_fenced_and_hash_checked() {
    let allowance = budget();
    let mut root = RootDirectory::new(
        ClusterId::from_u128(1),
        RootConfig::default(),
        allowance.clone(),
    )
    .unwrap();
    let root_apply = |root: &mut RootDirectory, operation| {
        let command = RootCommand {
            expected_revision: root.revision(),
            operation,
        };
        let update = root.prepare(&command, &Evidence).unwrap();
        root.publish(update).unwrap();
    };
    root_apply(
        &mut root,
        RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId::from_u128(1),
                label: "west".into(),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    );
    root_apply(
        &mut root,
        RootOperation::Delegate {
            delegation: delegation(1, NamespaceRange::all()),
        },
    );
    let mut source = partition(allowance.clone(), 1, NamespaceRange::all());
    nodes(&mut source);
    create(&mut source, ledger(1, 1));
    apply(
        &mut source,
        PartitionOperation::SealForTransfer {
            operation: OperationId::from_u128(10),
            destination: PartitionId::from_u128(2),
            next_epoch: 2,
        },
    );
    let checkpoint = source.checkpoint().clone();
    let fence = DelegationFence {
        cluster: checkpoint.cluster,
        operation: OperationId::from_u128(10),
        source: PartitionId::from_u128(1),
        destination: PartitionId::from_u128(2),
        namespace: NamespaceRange::all(),
        from_epoch: 1,
        to_epoch: 2,
        sealed_revision: checkpoint.revision,
        checkpoint: partition_checkpoint_digest(&checkpoint).unwrap(),
        destination_ready: VERIFIED,
    };
    let mut destination = delegation(2, NamespaceRange::all());
    destination.epoch = 2;
    let command = RootCommand {
        expected_revision: root.revision(),
        operation: RootOperation::Transfer {
            start: NamespaceKey::MIN,
            expected_epoch: 1,
            destination,
            fence,
        },
    };
    let update = root.prepare(&command, &Evidence).unwrap();
    root.publish(update).unwrap();
    assert!(matches!(
        root.prepare(&command, &Evidence),
        Err(DirectoryError::CompareFailed)
    ));
    let active = root.resolve(ledger(1, 1)).unwrap().clone();
    let installed = DirectoryPartition::install_transferred(
        checkpoint.clone(),
        active.clone(),
        &Evidence,
        PartitionConfig::default(),
        allowance.clone(),
    )
    .unwrap();
    assert_eq!(
        installed.lookup(ledger(1, 1), 2).unwrap().partition,
        PartitionId::from_u128(2)
    );
    assert!(installed.checkpoint().sealed.is_none());
    let blocked = PartitionCommand {
        expected_revision: source.revision(),
        delegation_epoch: 1,
        operation: PartitionOperation::ReportLoad {
            load: NodeLoad {
                node: 1,
                generation: 1,
                report: 2,
                available_memory: 10,
                active_weight: 1,
                disk_available: 1 << 30,
            },
        },
    };
    assert!(matches!(
        source.prepare(&blocked, &Evidence),
        Err(DirectoryError::StaleEpoch)
    ));
    let mut tampered = checkpoint;
    tampered.nodes.get_mut(&1).unwrap().enrollment.endpoint = "wrong:443".into();
    assert!(matches!(
        DirectoryPartition::install_transferred(
            tampered,
            active,
            &Evidence,
            PartitionConfig::default(),
            allowance
        ),
        Err(DirectoryError::CompareFailed)
    ));
}

#[test]
fn root_rejects_overlapping_delegations_without_global_session_rows() {
    let mut root =
        RootDirectory::new(ClusterId::from_u128(1), RootConfig::default(), budget()).unwrap();
    let register = RootCommand {
        expected_revision: 0,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId::from_u128(1),
                label: "local".into(),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    };
    let prepared = root.prepare(&register, &Evidence).unwrap();
    root.publish(prepared).unwrap();
    let split = NamespaceKey::of(ledger(10, 0));
    for entry in [
        delegation(
            1,
            NamespaceRange {
                start: NamespaceKey::MIN,
                end: Some(split),
            },
        ),
        delegation(
            2,
            NamespaceRange {
                start: split,
                end: None,
            },
        ),
    ] {
        let command = RootCommand {
            expected_revision: root.revision(),
            operation: RootOperation::Delegate { delegation: entry },
        };
        let prepared = root.prepare(&command, &Evidence).unwrap();
        root.publish(prepared).unwrap();
    }
    let overlap = RootCommand {
        expected_revision: root.revision(),
        operation: RootOperation::Delegate {
            delegation: delegation(3, NamespaceRange::all()),
        },
    };
    assert!(matches!(
        root.prepare(&overlap, &Evidence),
        Err(DirectoryError::Duplicate)
    ));
    assert_eq!(
        root.resolve(ledger(1, 1)).unwrap().partition,
        PartitionId::from_u128(1)
    );
    assert_eq!(
        root.resolve(ledger(10, 1)).unwrap().partition,
        PartitionId::from_u128(2)
    );
    assert_eq!(root.checkpoint().delegations.len(), 2);
}

#[test]
fn unknown_geography_delegates_without_region_rows_and_preserves_transfer_fences() {
    let cluster = ClusterId::from_u128(1);
    let mut root = RootDirectory::new(cluster, RootConfig::default(), budget()).unwrap();
    let mut first = delegation(1, NamespaceRange::all());
    first.region = RegionId::UNKNOWN;
    let prepared = root
        .prepare(
            &RootCommand {
                expected_revision: 0,
                operation: RootOperation::Delegate {
                    delegation: first.clone(),
                },
            },
            &Evidence,
        )
        .unwrap();
    root.publish(prepared).unwrap();
    assert!(root.checkpoint().regions.is_empty());
    let recovered = RootDirectory::restore(
        postcard::from_bytes(&postcard::to_stdvec(root.checkpoint()).unwrap()).unwrap(),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    assert_eq!(recovered.resolve(ledger(1, 1)), Some(&first));

    let mut source =
        DirectoryPartition::new(cluster, first, PartitionConfig::default(), budget()).unwrap();
    let mut node = enrollment(1, 0);
    node.zone = ZoneId([0; 16]);
    apply(
        &mut source,
        PartitionOperation::Enroll {
            node,
            expected_generation: None,
        },
    );
    create(&mut source, ledger(1, 1));
    for class in [FailureClass::Zone, FailureClass::Region] {
        let mut unsupported = initial_spec();
        unsupported.policy.durability.survive = class;
        assert!(verify_placement(&unsupported, &source.checkpoint().nodes, 31).is_err());
    }
    let operation = OperationId::from_u128(3);
    apply(
        &mut source,
        PartitionOperation::SealForTransfer {
            operation,
            destination: PartitionId::from_u128(2),
            next_epoch: 2,
        },
    );
    let checkpoint = source.checkpoint().clone();
    let mut destination = delegation(2, NamespaceRange::all());
    destination.region = RegionId::UNKNOWN;
    destination.epoch = 2;
    let fence = DelegationFence {
        cluster,
        operation,
        source: PartitionId::from_u128(1),
        destination: destination.partition,
        namespace: NamespaceRange::all(),
        from_epoch: 1,
        to_epoch: 2,
        sealed_revision: source.revision(),
        checkpoint: partition_checkpoint_digest(&checkpoint).unwrap(),
        destination_ready: VERIFIED,
    };
    let transfer = |destination: Delegation, fence: DelegationFence| RootCommand {
        expected_revision: root.revision(),
        operation: RootOperation::Transfer {
            start: NamespaceKey::MIN,
            expected_epoch: 1,
            destination,
            fence,
        },
    };
    let mut forged = fence.clone();
    forged.destination_ready = ContentHash([1; 32]);
    assert!(matches!(
        root.prepare(&transfer(destination.clone(), forged), &Evidence),
        Err(DirectoryError::UnverifiedAuthority)
    ));
    let mut missing_region = destination.clone();
    missing_region.region = RegionId::from_u128(99);
    assert!(matches!(
        root.prepare(&transfer(missing_region, fence.clone()), &Evidence),
        Err(DirectoryError::Invalid("delegation metadata"))
    ));
    let prepared = root
        .prepare(&transfer(destination, fence), &Evidence)
        .unwrap();
    root.publish(prepared).unwrap();
    let installed = DirectoryPartition::install_transferred(
        checkpoint,
        root.resolve(ledger(1, 1)).unwrap().clone(),
        &Evidence,
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    assert_eq!(
        installed.lookup(ledger(1, 1), 2).unwrap().partition,
        PartitionId::from_u128(2)
    );
    assert!(root.checkpoint().regions.is_empty());
}

#[test]
fn known_region_delegation_still_requires_registration_and_cas_epochs() {
    let mut root =
        RootDirectory::new(ClusterId::from_u128(1), RootConfig::default(), budget()).unwrap();
    let delegate = |revision| RootCommand {
        expected_revision: revision,
        operation: RootOperation::Delegate {
            delegation: delegation(1, NamespaceRange::all()),
        },
    };
    assert!(matches!(
        root.prepare(&delegate(0), &Evidence),
        Err(DirectoryError::Invalid("delegation metadata"))
    ));
    let register = |id, expected_epoch, epoch, revision| RootCommand {
        expected_revision: revision,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id,
                label: "region-a".into(),
                authority_epoch: epoch,
            },
            expected_epoch,
        },
    };
    assert!(matches!(
        root.prepare(&register(RegionId::UNKNOWN, None, 1, 0), &Evidence),
        Err(DirectoryError::Invalid("region metadata"))
    ));
    let region = RegionId::from_u128(1);
    let prepared = root
        .prepare(&register(region, None, 1, 0), &Evidence)
        .unwrap();
    root.publish(prepared).unwrap();
    assert!(matches!(
        root.prepare(&register(region, Some(1), 3, 1), &Evidence),
        Err(DirectoryError::StaleEpoch)
    ));
    let prepared = root.prepare(&delegate(1), &Evidence).unwrap();
    root.publish(prepared).unwrap();
    assert_eq!(root.resolve(ledger(1, 1)).unwrap().region, region);
    assert_eq!(root.checkpoint().regions[&region].authority_epoch, 1);
}

#[test]
fn prepared_partition_updates_are_atomic_and_do_not_allocate_at_publication() {
    let allowance = budget();
    let mut directory = partition(allowance.clone(), 1, NamespaceRange::all());
    let command = PartitionCommand {
        expected_revision: 0,
        delegation_epoch: 1,
        operation: PartitionOperation::Enroll {
            node: enrollment(1, 1),
            expected_generation: None,
        },
    };
    let before = allowance.stats();
    let aborted = directory.prepare(&command, &Evidence).unwrap();
    assert!(directory.checkpoint().nodes.is_empty());
    drop(aborted);
    assert_eq!(allowance.stats(), before);
    let ready = directory.prepare(&command, &Evidence).unwrap();
    let stale = directory.prepare(&command, &Evidence).unwrap();
    let occupied = allowance
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            allowance.stats().limit - allowance.stats().used,
        )
        .unwrap();
    directory.publish(ready).unwrap();
    assert_eq!(
        directory.publish(stale),
        Err(DirectoryError::StalePreparation)
    );
    drop(occupied);
    assert_eq!(directory.checkpoint().nodes.len(), 1);
}

#[test]
fn recovery_rejects_malformed_pending_fences_and_checkpoint_epochs() {
    let mut directory = partition(budget(), 1, NamespaceRange::all());
    nodes(&mut directory);
    let session = ledger(1, 1);
    create(&mut directory, session);
    change(
        &mut directory,
        session,
        SessionChange::Plan {
            operation: OperationId::from_u128(2),
            desired: initial_spec(),
            observations: BTreeMap::new(),
        },
    );
    let checkpoint = directory.checkpoint().clone();
    let mut malformed = checkpoint.clone();
    malformed
        .sessions
        .get_mut(&session)
        .unwrap()
        .pending
        .as_mut()
        .unwrap()
        .next_membership += 1;
    assert!(matches!(
        DirectoryPartition::restore(malformed, PartitionConfig::default(), budget()),
        Err(DirectoryError::StaleEpoch)
    ));
    let mut malformed = checkpoint.clone();
    malformed.sessions.get_mut(&session).unwrap().authority.kind = SessionFenceKind::Cutover;
    assert!(matches!(
        DirectoryPartition::restore(malformed, PartitionConfig::default(), budget()),
        Err(DirectoryError::StaleEpoch)
    ));
    let mut malformed = checkpoint.clone();
    malformed.delegation.epoch += 1;
    assert!(matches!(
        DirectoryPartition::restore(malformed, PartitionConfig::default(), budget()),
        Err(DirectoryError::StaleEpoch)
    ));
    let mut malformed = checkpoint;
    malformed
        .sessions
        .get_mut(&session)
        .unwrap()
        .pending
        .as_mut()
        .unwrap()
        .barrier = Some(fence(
        &initial_spec(),
        SessionFenceKind::Cutover,
        session,
        2,
        2,
        2,
    ));
    assert!(matches!(
        DirectoryPartition::restore(malformed, PartitionConfig::default(), budget()),
        Err(DirectoryError::Phase)
    ));
}

#[test]
fn empty_domain_placement_uses_increasing_raft_fences_without_invented_mutations() {
    let mut directory = partition(budget(), 1, NamespaceRange::all());
    nodes(&mut directory);
    let session = ledger(7, 9);
    let spec = initial_spec();
    let mut created = fence(&spec, SessionFenceKind::Created, session, 1, 0, 1);
    created.index = RaftIndex(10);
    apply(
        &mut directory,
        PartitionOperation::CreateSession {
            ledger: session,
            log_group: created.log_group,
            placement: spec.clone(),
            authority: created,
        },
    );
    change(
        &mut directory,
        session,
        SessionChange::Plan {
            operation: OperationId::from_u128(2),
            desired: spec.clone(),
            observations: BTreeMap::new(),
        },
    );
    change(
        &mut directory,
        session,
        SessionChange::BeginPreparation {
            operation: OperationId::from_u128(2),
        },
    );
    let mut cutover = fence(&spec, SessionFenceKind::Cutover, session, 2, 0, 2);
    cutover.membership_epoch = 1;
    cutover.index = RaftIndex(11);
    let mut stale = cutover.clone();
    stale.index = RaftIndex(10);
    assert!(matches!(
        attempt(
            &directory,
            session,
            SessionChange::Cutover {
                operation: OperationId::from_u128(2),
                authority: stale
            }
        ),
        Err(DirectoryError::StaleEpoch)
    ));
    change(
        &mut directory,
        session,
        SessionChange::Ready {
            ready: ready(session, 2, 2, 1, 0),
        },
    );
    change(
        &mut directory,
        session,
        SessionChange::Progress {
            operation: OperationId::from_u128(2),
            progress: promoted(&spec, 1, 0, 2),
        },
    );
    change(
        &mut directory,
        session,
        SessionChange::Cutover {
            operation: OperationId::from_u128(2),
            authority: cutover,
        },
    );
    let mut activated = fence(&spec, SessionFenceKind::Activated, session, 2, 0, 2);
    activated.membership_epoch = 1;
    activated.index = RaftIndex(12);
    change(
        &mut directory,
        session,
        SessionChange::Activate {
            operation: OperationId::from_u128(2),
            authority: activated,
        },
    );
    assert_eq!(
        directory.get(session).unwrap().unwrap().authority.sequence,
        SessionSeq(0)
    );
    let recovered = DirectoryPartition::restore(
        directory.checkpoint().clone(),
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    assert_eq!(
        recovered.get(session).unwrap().unwrap().route_epoch,
        RouteEpoch(2)
    );
}
