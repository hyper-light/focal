//! Namespace split and merge in the directory model: a partition seals the
//! upper part of its keys for a new partition, the root commits the split
//! under one fence, the destination installs the sealed image, the source
//! releases what left; two adjacent partitions merge back under a bounded
//! absorb; every schema-3 checkpoint restores with a whole-namespace seal.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_directory::*;
use focal_memory::MemoryBudget;
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
fn enrollment(id: u64) -> NodeEnrollment {
    NodeEnrollment {
        node: id,
        generation: 1,
        region: RegionId::from_u128(1),
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
fn command(partition: &DirectoryPartition, operation: PartitionOperation) -> PartitionCommand {
    PartitionCommand {
        expected_revision: partition.revision(),
        delegation_epoch: partition.checkpoint().delegation.epoch,
        operation,
    }
}
fn apply(partition: &mut DirectoryPartition, operation: PartitionOperation) {
    let command = command(partition, operation);
    let encoded = postcard::to_allocvec(&command).unwrap();
    let replay: PartitionCommand = postcard::from_bytes(&encoded).unwrap();
    let prepared = partition.prepare(&replay, &Evidence).unwrap();
    partition.publish(prepared).unwrap();
}
fn refused(partition: &DirectoryPartition, operation: PartitionOperation) -> DirectoryError {
    partition
        .prepare(&command(partition, operation), &Evidence)
        .err()
        .expect("refused")
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
    }
}
fn create(partition: &mut DirectoryPartition, ledger: LedgerId, group: u128) {
    let single = BTreeMap::from([(1, 1)]);
    let spec = PlacementSpec {
        policy: PlacementPolicy {
            durability: DurabilityIntent {
                survive: FailureClass::Node,
                max_failures: 0,
            },
            residency: BTreeSet::new(),
            home_regions: BTreeSet::new(),
            required_memory: 10,
        },
        placement: Placement {
            voters: single.clone(),
            materializers: single.clone(),
            content_copies: single,
            preferred_leader: 1,
        },
    };
    let authority = SessionFence {
        kind: SessionFenceKind::Created,
        ledger,
        log_group: LogGroupId::from_u128(group),
        operation: OperationId::from_u128(1),
        sequence: SessionSeq(1),
        index: RaftIndex(1),
        term: RaftTerm(1),
        from_route: RouteEpoch(0),
        to_route: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        placement_digest: placement_digest(&spec).unwrap(),
        record_hash: VERIFIED,
    };
    apply(
        partition,
        PartitionOperation::CreateSession {
            ledger,
            log_group: authority.log_group,
            placement: spec,
            authority,
        },
    );
}
fn root(budget: &MemoryBudget, delegations: Vec<Delegation>) -> RootDirectory {
    let mut root = RootDirectory::new(
        ClusterId::from_u128(1),
        RootConfig::default(),
        budget.clone(),
    )
    .unwrap();
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
    for delegation in delegations {
        root_apply(&mut root, RootOperation::Delegate { delegation });
    }
    root
}
fn root_apply(root: &mut RootDirectory, operation: RootOperation) {
    let command = RootCommand {
        expected_revision: root.revision(),
        operation,
    };
    let update = root.prepare(&command, &Evidence).unwrap();
    root.publish(update).unwrap();
}
fn root_refused(root: &RootDirectory, operation: RootOperation) -> DirectoryError {
    let command = RootCommand {
        expected_revision: root.revision(),
        operation,
    };
    root.prepare(&command, &Evidence).err().expect("refused")
}
fn load(node: u64) -> PartitionOperation {
    PartitionOperation::ReportLoad {
        load: NodeLoad {
            node,
            generation: 1,
            report: 7,
            available_memory: 10,
            active_weight: 1,
            disk_available: 1 << 30,
        },
    }
}

#[test]
fn a_partition_splits_at_a_key_and_both_halves_route_every_session_exactly_once() {
    let allowance = budget();
    let mut root = root(&allowance, vec![delegation(1, NamespaceRange::all())]);
    let mut source = partition(allowance.clone(), 1, NamespaceRange::all());
    nodes(&mut source);
    for (index, ledger) in [ledger(1, 1), ledger(1, 2), ledger(2, 1), ledger(3, 1)]
        .into_iter()
        .enumerate()
    {
        create(&mut source, ledger, 1_000 + index as u128);
    }
    let at = NamespaceKey::of(ledger(2, 1));
    let moved = NamespaceRange {
        start: at,
        end: None,
    };
    // The split key must fall strictly inside the namespace.
    assert!(matches!(
        refused(
            &source,
            PartitionOperation::SealForSplit {
                operation: OperationId::from_u128(10),
                destination: PartitionId::from_u128(2),
                next_epoch: 2,
                at: NamespaceKey::MIN,
            }
        ),
        DirectoryError::Invalid(_)
    ));
    apply(
        &mut source,
        PartitionOperation::SealForSplit {
            operation: OperationId::from_u128(10),
            destination: PartitionId::from_u128(2),
            next_epoch: 2,
            at,
        },
    );
    let seal = source.checkpoint().sealed.clone().unwrap();
    assert_eq!(seal.moved, moved);
    assert_eq!(seal.source, PartitionId::from_u128(1));
    assert_eq!(seal.revision, source.revision());
    // Sealed: nothing but the release goes through.
    assert!(matches!(
        refused(&source, load(1)),
        DirectoryError::StaleEpoch
    ));
    // The image every source voter derives: the sealed state with only the
    // moved sessions.
    let image = source.split_image().unwrap();
    assert_eq!(
        image.sessions.keys().copied().collect::<Vec<_>>(),
        vec![ledger(2, 1), ledger(3, 1)]
    );
    // The image is the destination from the start: derived group, the
    // provisional delegation, the source's seal.
    let group = split_group_id(ClusterId::from_u128(1), OperationId::from_u128(10));
    assert_eq!(image.delegation.partition, PartitionId::from_u128(2));
    assert_eq!(image.delegation.log_group, group);
    assert_eq!(image.delegation.namespace, moved);
    assert_eq!(image.delegation.epoch, 2);
    assert!(image.delegation.activation.is_none());
    assert_eq!(image.sealed, source.checkpoint().sealed);
    assert_eq!(
        image.sealed.as_ref().unwrap().source,
        PartitionId::from_u128(1)
    );
    assert_eq!(image.nodes.len(), 3);
    // An image restores as a valid provisional partition state.
    let encoded = postcard::to_allocvec(&image).unwrap();
    assert_eq!(PartitionCheckpoint::decode_any(&encoded).unwrap(), image);
    DirectoryPartition::restore(image.clone(), PartitionConfig::default(), allowance.clone())
        .unwrap();
    let digest = partition_checkpoint_digest(&image).unwrap();
    let fence = DelegationFence {
        cluster: ClusterId::from_u128(1),
        operation: OperationId::from_u128(10),
        source: PartitionId::from_u128(1),
        destination: PartitionId::from_u128(2),
        namespace: moved,
        from_epoch: 1,
        to_epoch: 2,
        sealed_revision: seal.revision,
        checkpoint: digest,
        destination_ready: VERIFIED,
    };
    let mut destination = delegation(2, moved);
    destination.epoch = 2;
    destination.log_group = group;
    // A wrong key or a wrong destination geometry is refused before any
    // proof is consulted.
    assert!(matches!(
        root_refused(
            &root,
            RootOperation::Split {
                start: NamespaceKey::MIN,
                at: NamespaceKey::MIN,
                expected_epoch: 1,
                destination,
                fence,
            }
        ),
        DirectoryError::Invalid(_)
    ));
    let mut wrong = destination;
    wrong.namespace = NamespaceRange::all();
    assert!(matches!(
        root_refused(
            &root,
            RootOperation::Split {
                start: NamespaceKey::MIN,
                at,
                expected_epoch: 1,
                destination: wrong,
                fence,
            }
        ),
        DirectoryError::StaleEpoch
    ));
    let split = RootOperation::Split {
        start: NamespaceKey::MIN,
        at,
        expected_epoch: 1,
        destination,
        fence,
    };
    root_apply(&mut root, split.clone());
    assert!(matches!(
        root_refused(&root, split),
        DirectoryError::CompareFailed
    ));
    let kept = *root.resolve(ledger(1, 2)).unwrap();
    assert_eq!(kept.partition, PartitionId::from_u128(1));
    assert_eq!(kept.epoch, 2);
    assert_eq!(
        kept.namespace,
        NamespaceRange {
            start: NamespaceKey::MIN,
            end: Some(at)
        }
    );
    assert_eq!(kept.activation.as_ref(), Some(&fence));
    let upper = *root.resolve(ledger(2, 1)).unwrap();
    assert_eq!(upper.partition, PartitionId::from_u128(2));
    assert_eq!(upper.namespace, moved);
    assert_eq!(upper.epoch, 2);
    assert_eq!(
        root.resolve(ledger(3, 1)).unwrap().partition,
        upper.partition
    );
    // The destination installs the hash-verified image under its delegation.
    let installed = DirectoryPartition::install_transferred(
        image.clone(),
        upper,
        &Evidence,
        PartitionConfig::default(),
        allowance.clone(),
    )
    .unwrap();
    assert!(installed.checkpoint().sealed.is_none());
    assert_eq!(installed.checkpoint().delegation, upper);
    assert_eq!(
        installed.lookup(ledger(3, 1), 2).unwrap().partition,
        PartitionId::from_u128(2)
    );
    assert!(matches!(
        installed.get(ledger(1, 1)),
        Err(DirectoryError::OutsideNamespace)
    ));
    let mut tampered = image.clone();
    tampered.sessions.remove(&ledger(3, 1));
    assert!(matches!(
        DirectoryPartition::install_transferred(
            tampered,
            upper,
            &Evidence,
            PartitionConfig::default(),
            allowance.clone()
        ),
        Err(DirectoryError::CompareFailed)
    ));
    // The source releases the moved part under the kept delegation only.
    let mut wrong = kept;
    wrong.epoch = 3;
    assert!(matches!(
        refused(&source, PartitionOperation::Release { delegation: wrong }),
        DirectoryError::StaleEpoch
    ));
    let mut unverified = kept;
    if let Some(fence) = &mut unverified.activation {
        fence.destination_ready = ContentHash([1; 32]);
    }
    assert!(matches!(
        refused(
            &source,
            PartitionOperation::Release {
                delegation: unverified
            }
        ),
        DirectoryError::UnverifiedAuthority
    ));
    apply(
        &mut source,
        PartitionOperation::Release { delegation: kept },
    );
    let state = source.checkpoint();
    assert!(state.sealed.is_none());
    assert_eq!(state.delegation, kept);
    assert_eq!(
        state.sessions.keys().copied().collect::<Vec<_>>(),
        vec![ledger(1, 1), ledger(1, 2)]
    );
    assert!(matches!(
        source.get(ledger(2, 1)),
        Err(DirectoryError::OutsideNamespace)
    ));
    assert_eq!(
        source.lookup(ledger(1, 1), 2).unwrap().partition,
        PartitionId::from_u128(1)
    );
    // Both halves serve again at the new epoch; the old epoch is stale.
    apply(&mut source, load(1));
    assert!(matches!(
        source.lookup(ledger(1, 1), 1),
        Err(DirectoryError::StaleEpoch)
    ));
    // A restored source keeps the same shape.
    let encoded = postcard::to_allocvec(source.checkpoint()).unwrap();
    let restored = PartitionCheckpoint::decode_any(&encoded).unwrap();
    assert_eq!(&restored, source.checkpoint());
}

#[test]
fn two_adjacent_partitions_merge_back_under_a_bounded_absorb() {
    let allowance = budget();
    let at = NamespaceKey::of(ledger(2, 1));
    let lower = NamespaceRange {
        start: NamespaceKey::MIN,
        end: Some(at),
    };
    let upper = NamespaceRange {
        start: at,
        end: None,
    };
    let mut root = root(&allowance, vec![delegation(1, lower), delegation(2, upper)]);
    let mut left = partition(allowance.clone(), 1, lower);
    nodes(&mut left);
    create(&mut left, ledger(1, 1), 1_000);
    create(&mut left, ledger(1, 2), 1_001);
    let mut right = partition(allowance.clone(), 2, upper);
    nodes(&mut right);
    create(&mut right, ledger(2, 1), 1_002);
    create(&mut right, ledger(3, 1), 1_003);
    // The right partition seals everything for the left one.
    apply(
        &mut right,
        PartitionOperation::SealForTransfer {
            operation: OperationId::from_u128(20),
            destination: PartitionId::from_u128(1),
            next_epoch: 2,
        },
    );
    let moved = right.checkpoint().clone();
    assert_eq!(moved.sealed.as_ref().unwrap().moved, upper);
    assert!(matches!(
        refused(&right, load(1)),
        DirectoryError::StaleEpoch
    ));
    let fence = DelegationFence {
        cluster: ClusterId::from_u128(1),
        operation: OperationId::from_u128(20),
        source: PartitionId::from_u128(2),
        destination: PartitionId::from_u128(1),
        namespace: upper,
        from_epoch: 1,
        to_epoch: 2,
        sealed_revision: moved.revision,
        checkpoint: partition_checkpoint_digest(&moved).unwrap(),
        destination_ready: VERIFIED,
    };
    // Only adjacent delegations merge, at their exact epochs.
    assert!(matches!(
        root_refused(
            &root,
            RootOperation::Merge {
                start: NamespaceKey::MIN,
                right: at,
                expected_epoch: 2,
                right_expected_epoch: 1,
                fence,
            }
        ),
        DirectoryError::CompareFailed
    ));
    root_apply(
        &mut root,
        RootOperation::Merge {
            start: NamespaceKey::MIN,
            right: at,
            expected_epoch: 1,
            right_expected_epoch: 1,
            fence,
        },
    );
    assert_eq!(root.checkpoint().delegations.len(), 1);
    let merged = *root.resolve(ledger(3, 1)).unwrap();
    assert_eq!(merged.partition, PartitionId::from_u128(1));
    assert_eq!(merged.namespace, NamespaceRange::all());
    assert_eq!(merged.epoch, 2);
    assert_eq!(merged.activation.as_ref(), Some(&fence));
    // The left partition absorbs the hash-verified sealed checkpoint.
    let mut tampered = moved.clone();
    tampered.sessions.remove(&ledger(3, 1));
    assert!(matches!(
        refused(
            &left,
            PartitionOperation::Absorb {
                delegation: merged,
                moved: Box::new(tampered),
            }
        ),
        DirectoryError::CompareFailed
    ));
    let tight = DirectoryPartition::restore(
        left.checkpoint().clone(),
        PartitionConfig {
            max_absorb_sessions: 1,
            ..PartitionConfig::default()
        },
        allowance.clone(),
    )
    .unwrap();
    assert!(matches!(
        refused(
            &tight,
            PartitionOperation::Absorb {
                delegation: merged,
                moved: Box::new(moved.clone()),
            }
        ),
        DirectoryError::Capacity
    ));
    apply(
        &mut left,
        PartitionOperation::Absorb {
            delegation: merged,
            moved: Box::new(moved.clone()),
        },
    );
    let state = left.checkpoint();
    assert_eq!(state.delegation, merged);
    assert!(state.sealed.is_none());
    assert_eq!(state.sessions.len(), 4);
    assert_eq!(state.nodes.len(), 3);
    assert_eq!(
        left.lookup(ledger(3, 1), 2).unwrap().partition,
        PartitionId::from_u128(1)
    );
    assert_eq!(
        left.lookup(ledger(1, 1), 2).unwrap().partition,
        PartitionId::from_u128(1)
    );
    // Absorbing twice is a duplicate, and the whole-namespace source never
    // releases.
    assert!(matches!(
        refused(
            &left,
            PartitionOperation::Absorb {
                delegation: merged,
                moved: Box::new(moved.clone()),
            }
        ),
        DirectoryError::StaleEpoch
    ));
    assert!(matches!(
        refused(&right, PartitionOperation::Release { delegation: merged }),
        DirectoryError::WrongOperation | DirectoryError::StaleEpoch
    ));
    drop(tight);
    apply(&mut left, load(2));
}

#[test]
fn a_schema_three_checkpoint_restores_with_a_seal_over_its_whole_namespace() {
    let allowance = budget();
    let mut source = partition(allowance, 1, NamespaceRange::all());
    nodes(&mut source);
    apply(
        &mut source,
        PartitionOperation::SealForTransfer {
            operation: OperationId::from_u128(30),
            destination: PartitionId::from_u128(2),
            next_epoch: 2,
        },
    );
    let current = source.checkpoint().clone();
    let legacy = PartitionCheckpointV3 {
        schema: 3,
        cluster: current.cluster,
        delegation: current.delegation,
        revision: current.revision,
        sealed: Some(PartitionSealV3 {
            operation: OperationId::from_u128(30),
            destination: PartitionId::from_u128(2),
            next_epoch: 2,
            revision: current.revision,
        }),
        nodes: current.nodes.clone(),
        sessions: legacy_sessions(&current.sessions),
    };
    let encoded = postcard::to_allocvec(&legacy).unwrap();
    let converted = PartitionCheckpoint::decode_any(&encoded).unwrap();
    // A converted checkpoint knows no route history: its log is complete
    // only from its own revision, and it records no founding node.
    assert!(converted.routes.is_empty());
    assert_eq!(converted.routes_from, current.revision);
    let mut expected = current.clone();
    expected.routes_from = current.revision;
    for session in expected.sessions.values_mut() {
        session.founder = None;
    }
    assert_eq!(converted, expected);
    assert_eq!(converted.sealed.unwrap().moved, NamespaceRange::all());
    let mut future = current.clone();
    future.schema = PARTITION_CHECKPOINT_SCHEMA + 1;
    assert!(PartitionCheckpoint::decode_any(&postcard::to_allocvec(&future).unwrap()).is_err());
}

#[test]
fn the_route_log_reports_exactly_what_moved_and_a_cache_too_far_behind_reads_a_gap() {
    let allowance = budget();
    let tight = PartitionConfig {
        max_route_log: 2,
        ..PartitionConfig::default()
    };
    let mut source = DirectoryPartition::new(
        ClusterId::from_u128(1),
        delegation(1, NamespaceRange::all()),
        tight,
        allowance.clone(),
    )
    .unwrap();
    nodes(&mut source);
    // Enrollments change no route.
    let quiet = source.route_changes(0);
    assert!(quiet.changes.is_empty());
    assert_eq!(quiet.after_revision, 0);
    assert_eq!(quiet.through_revision, source.revision());
    create(&mut source, ledger(1, 1), 1_000);
    let first = source.revision();
    create(&mut source, ledger(1, 2), 1_001);
    let second = source.revision();
    let batch = source.route_changes(0);
    assert_eq!(batch.partition, PartitionId::from_u128(1));
    assert_eq!(batch.delegation_epoch, 1);
    assert_eq!(batch.after_revision, 0);
    assert_eq!(batch.through_revision, second);
    assert_eq!(
        batch.changes,
        vec![
            RouteInvalidation {
                ledger: ledger(1, 1),
                route_epoch: RouteEpoch(1)
            },
            RouteInvalidation {
                ledger: ledger(1, 2),
                route_epoch: RouteEpoch(1)
            }
        ]
    );
    // Only what changed after the watched revision.
    let later = source.route_changes(first);
    assert_eq!(later.after_revision, first);
    assert_eq!(later.changes.len(), 1);
    assert_eq!(later.changes[0].ledger, ledger(1, 2));
    // A third change evicts the oldest: a cache behind the eviction point
    // gets a batch starting later than it asked, which it reads as a gap.
    create(&mut source, ledger(1, 3), 1_002);
    let state = source.checkpoint();
    assert_eq!(state.routes.len(), 2);
    assert_eq!(state.routes_from, first);
    let gap = source.route_changes(0);
    assert_eq!(gap.after_revision, first);
    assert_eq!(gap.changes.len(), 2);
    let exact = source.route_changes(first);
    assert_eq!(exact.after_revision, first);
    assert_eq!(exact.changes.len(), 2);
    let current = source.route_changes(source.revision());
    assert!(current.changes.is_empty());
    // A cache applies the batches as intended: a gap clears the partition.
    let mut cache = RouteCache::new(RouteCacheConfig::default(), allowance.clone()).unwrap();
    cache
        .insert(source.lookup(ledger(1, 1), 1).unwrap(), 1, 10)
        .unwrap();
    let watched = cache.watched_partitions();
    assert_eq!(watched, 1);
    let stale_batch = InvalidationBatch {
        partition: PartitionId::from_u128(1),
        delegation_epoch: 1,
        after_revision: first,
        through_revision: source.revision(),
        changes: gap.changes.clone(),
    };
    // The cache's watch starts at the inserted route's source revision (the
    // current one); an older batch is a no-op, a mismatched start a gap.
    assert_eq!(cache.invalidate(&stale_batch).unwrap(), 0);
    // A schema-4 checkpoint restores with an empty log complete at its
    // revision.
    let legacy = PartitionCheckpointV4 {
        schema: 4,
        cluster: state.cluster,
        delegation: state.delegation,
        revision: state.revision,
        sealed: None,
        nodes: state.nodes.clone(),
        sessions: legacy_sessions(&state.sessions),
    };
    let converted =
        PartitionCheckpoint::decode_any(&postcard::to_allocvec(&legacy).unwrap()).unwrap();
    assert!(converted.routes.is_empty());
    assert_eq!(converted.routes_from, state.revision);
    // The current schema records who founded each session; a converted
    // checkpoint does not, and a host falls back to the cluster founder.
    assert!(state.sessions.values().all(|s| s.founder == Some(1)));
    let mut unfounded = state.sessions.clone();
    for session in unfounded.values_mut() {
        session.founder = None;
    }
    assert_eq!(converted.sessions, unfounded);
    // A schema-5 checkpoint keeps its route log through the conversion.
    let five = PartitionCheckpointV5 {
        schema: 5,
        cluster: state.cluster,
        delegation: state.delegation,
        revision: state.revision,
        sealed: None,
        nodes: state.nodes.clone(),
        sessions: legacy_sessions(&state.sessions),
        routes: state.routes.clone(),
        routes_from: state.routes_from,
    };
    let converted_five =
        PartitionCheckpoint::decode_any(&postcard::to_allocvec(&five).unwrap()).unwrap();
    assert_eq!(converted_five.routes, state.routes);
    assert_eq!(converted_five.routes_from, state.routes_from);
    assert_eq!(converted_five.sessions, unfounded);
    assert_eq!(converted_five.schema, PARTITION_CHECKPOINT_SCHEMA);
    let restored = DirectoryPartition::restore(converted, tight, allowance).unwrap();
    assert_eq!(restored.route_changes(0).after_revision, state.revision);
}
fn legacy_sessions(
    sessions: &BTreeMap<LedgerId, SessionDescriptor>,
) -> BTreeMap<LedgerId, SessionDescriptorV5> {
    sessions
        .iter()
        .map(|(ledger, session)| (*ledger, SessionDescriptorV5::from(session.clone())))
        .collect()
}
