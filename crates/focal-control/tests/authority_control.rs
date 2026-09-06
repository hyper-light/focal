#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_consensus::{NodeConfig, PbMessageExt};
use focal_control::*;
use focal_directory::*;
use focal_enrollment::{BootstrapAuthority, EnrollmentLimits, FoundingEnrollmentDraft, JoinKey};
use focal_memory::MemoryBudget;
use focal_model::ContentHash;
use std::path::Path;

const CLUSTER: [u8; 16] = [81; 16];
const ROOT: [u8; 16] = [82; 16];
const PART: [u8; 16] = [83; 16];
const NOW: i64 = 1_800_000_000;
struct AttemptedBypass;
impl AuthorityVerifier for AttemptedBypass {
    fn verify_enrollment(&self, _: &NodeEnrollment) -> Result<(), DirectoryError> {
        panic!("installed authority must never invoke caller verifier")
    }
    fn verify_session_fence(&self, _: &SessionFence) -> Result<(), DirectoryError> {
        panic!("installed authority must never invoke caller verifier")
    }
    fn verify_replica_ready(&self, _: &ReplicaReady) -> Result<(), DirectoryError> {
        panic!("installed authority must never invoke caller verifier")
    }
    fn verify_delegation(&self, _: &DelegationFence) -> Result<(), DirectoryError> {
        panic!("installed authority must never invoke caller verifier")
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(192 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
struct Group {
    replicas: Vec<ControlReplica>,
    options: Vec<ControlOptions>,
    bootstrap: ControlBootstrap,
    next: u64,
    path: std::path::PathBuf,
    leader: usize,
}
impl Group {
    fn open(path: &Path, bootstrap: ControlBootstrap, group: [u8; 16]) -> Self {
        let options = (1..=3)
            .map(|node| {
                let mut config = NodeConfig::single(node, CLUSTER, group);
                config.voters = vec![1, 2, 3];
                ControlOptions::new(config)
            })
            .collect::<Vec<_>>();
        let replicas = options
            .iter()
            .map(|options| {
                ControlReplica::open(
                    options.clone(),
                    bootstrap.clone(),
                    budget(),
                    path.join(options.consensus.node_id.to_string()),
                )
                .unwrap()
            })
            .collect();
        let mut result = Self {
            replicas,
            options,
            bootstrap,
            next: 1,
            path: path.to_owned(),
            leader: 0,
        };
        result.pump();
        result.replicas[0].campaign().unwrap();
        result.pump();
        result
    }
    fn pump(&mut self) {
        self.pump_excluding(None);
    }
    fn pump_excluding(&mut self, excluded: Option<usize>) {
        for _ in 0..128 {
            let mut messages = Vec::new();
            for (index, replica) in self.replicas.iter_mut().enumerate() {
                if Some(index) != excluded {
                    messages.extend(replica.drain(&AttemptedBypass).unwrap().messages);
                }
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                let from = message.from;
                let to = usize::try_from(message.to - 1).unwrap();
                if Some(to) != excluded {
                    self.replicas[to]
                        .step_authenticated(from, &message.write_to_bytes().unwrap())
                        .unwrap();
                }
            }
        }
        panic!("metadata did not quiesce")
    }
    fn request(&self, command: ControlCommand) -> ControlRequest {
        ControlRequest {
            id: ControlRequestId {
                client: [9; 16],
                sequence: self.next,
            },
            acknowledged_through: self.next - 1,
            command,
        }
    }
    fn commit(&mut self, command: ControlCommand) -> ControlReceipt {
        let request = self.request(command);
        assert_eq!(
            self.replicas[self.leader]
                .submit(request.clone(), &AttemptedBypass)
                .unwrap(),
            ControlSubmission::Pending(request.id)
        );
        self.pump();
        let receipt = self.replicas[self.leader]
            .receipt(request.id)
            .unwrap()
            .unwrap();
        for replica in &self.replicas {
            assert_eq!(replica.receipt(request.id).unwrap(), Some(receipt));
        }
        self.next += 1;
        receipt
    }
    fn snapshot(&mut self) -> ControlAuthoritySnapshot {
        let context = b"installed-authority-test".to_vec();
        self.replicas[self.leader]
            .read_index(context.clone())
            .unwrap();
        let mut barrier = false;
        for _ in 0..128 {
            let mut messages = Vec::new();
            for replica in &mut self.replicas {
                let events = replica.drain(&AttemptedBypass).unwrap();
                barrier |= events
                    .read_states
                    .iter()
                    .any(|read| read.context == context);
                messages.extend(events.messages);
            }
            for message in messages {
                let from = message.from;
                let to = usize::try_from(message.to - 1).unwrap();
                self.replicas[to]
                    .step_authenticated(from, &message.write_to_bytes().unwrap())
                    .unwrap();
            }
            if barrier {
                break;
            }
        }
        assert!(barrier);
        self.pump();
        let ControlReadResult::Authority(Some(snapshot)) = self.replicas[self.leader]
            .read_local(&ControlRead::Authority)
            .unwrap()
        else {
            panic!("activated authority")
        };
        snapshot
    }
    fn restart(&mut self) {
        self.replicas[0].checkpoint().unwrap(); // Other replicas replay their WAL without a checkpoint.
        self.replicas.clear();
        self.replicas = self
            .options
            .iter()
            .map(|options| {
                ControlReplica::open(
                    options.clone(),
                    self.bootstrap.clone(),
                    budget(),
                    self.path.join(options.consensus.node_id.to_string()),
                )
                .unwrap()
            })
            .collect();
        self.pump();
        self.leader = 1;
        self.replicas[1].campaign().unwrap();
        self.pump();
    }
}
fn evidence(snapshot: &ControlAuthoritySnapshot, now: i64) -> ControlEvidence {
    let registry = focal_enrollment::EnrollmentRegistry::restore(
        &snapshot.enrollment,
        CLUSTER,
        EnrollmentLimits::default(),
    )
    .unwrap();
    ControlEvidence {
        authority_revision: snapshot.authority.revision,
        enrollment_revision: registry.revision(),
        decided_at: now,
        proofs: vec![],
    }
}
#[test]
fn quorum_activation_installed_topology_revocation_and_checkpoint_replay_are_authoritative() {
    let dirs = tempfile::tempdir().unwrap();
    let ca = BootstrapAuthority::open_or_create(
        dirs.path().join("ca"),
        CLUSTER,
        vec!["localhost".into()],
        NOW,
    )
    .unwrap();
    let key = JoinKey::open_or_create(dirs.path().join("key"), CLUSTER).unwrap();
    let founder = FoundingEnrollmentDraft::open_or_create(
        dirs.path().join("founder"),
        &ca,
        &key,
        1,
        [5; 16],
        EnrollmentLimits::default(),
        NOW,
    )
    .unwrap();
    let root = RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
    let bootstrap = ControlBootstrap::root(&root, founder.registry()).unwrap();
    let mut roots = Group::open(&dirs.path().join("root"), bootstrap, ROOT);
    let original_identity = roots.replicas[0].identity();
    roots.commit(ControlCommand::Root(RootCommand {
        expected_revision: 0,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId([1; 16]),
                label: "local-region".into(),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    }));
    // Legacy checkpoint before activation remains readable and keeps its genesis.
    roots.restart();
    assert_eq!(roots.replicas[0].identity(), original_identity);
    let activation = roots.request(ControlCommand::ActivateAuthority(
        AuthorityActivation::Root {
            expected_root_revision: 1,
            expected_enrollment_revision: 1,
            decided_at: NOW,
        },
    ));
    let isolated = roots.leader;
    roots.replicas[isolated]
        .submit(activation.clone(), &AttemptedBypass)
        .unwrap();
    // Persist the proposal, then drop its outbound messages. Local admission and
    // disk persistence cannot install an authority without the metadata quorum.
    let isolated_events = roots.replicas[isolated].drain(&AttemptedBypass).unwrap();
    assert!(isolated_events.completed.is_none());
    assert!(
        roots
            .replicas
            .iter()
            .all(|replica| replica.authority().is_none())
    );
    let mut replacement = None;
    for _ in 0..80 {
        for (index, replica) in roots.replicas.iter_mut().enumerate() {
            if index != isolated {
                replica.tick().unwrap();
            }
        }
        roots.pump_excluding(Some(isolated));
        replacement = roots
            .replicas
            .iter()
            .enumerate()
            .find_map(|(index, replica)| {
                (index != isolated && replica.status().role == focal_consensus::StateRole::Leader)
                    .then_some(index)
            });
        if replacement.is_some() {
            break;
        }
    }
    roots.leader = replacement.expect("the surviving quorum elects a leader");
    roots.replicas[roots.leader]
        .submit(activation.clone(), &AttemptedBypass)
        .unwrap();
    roots.pump_excluding(Some(isolated));
    let activated = roots.replicas[roots.leader]
        .receipt(activation.id)
        .unwrap()
        .unwrap();
    assert!(roots.replicas[isolated].authority().is_none());
    for _ in 0..2 {
        roots.replicas[roots.leader].tick().unwrap();
        roots.pump();
    }
    for replica in &mut roots.replicas {
        assert!(replica.authority().is_some());
        assert_eq!(
            replica
                .submit(activation.clone(), &AttemptedBypass)
                .unwrap(),
            ControlSubmission::Existing(activated)
        );
    }
    roots.next += 1;
    let snapshot = roots.snapshot();
    assert_eq!(snapshot.source, original_identity);
    let legacy = roots.request(ControlCommand::Root(RootCommand {
        expected_revision: 1,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId([2; 16]),
                label: "forged".into(),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    }));
    assert!(matches!(
        roots.replicas[roots.leader].submit(legacy, &AttemptedBypass),
        Err(ControlError::Directory(DirectoryError::UnverifiedAuthority))
    ));
    let mut grant = NodeTopologyGrant {
        enrollment: NodeEnrollment {
            node: 1,
            generation: 1,
            region: RegionId([1; 16]),
            zone: ZoneId([2; 16]),
            endpoint: "127.0.0.1:7443".into(),
            identity: ContentHash(focal_enrollment::server_fingerprint(
                &founder.receipt().certificate,
            )),
            authority_epoch: 1,
            attestation: ContentHash([0; 32]),
            eligible: true,
        },
        principal: [5; 16],
        expires_at: NOW + 600,
    };
    let mut bad = grant.clone();
    bad.enrollment.region = RegionId([99; 16]);
    let request = roots.request(ControlCommand::Authority(AuthorityCommand {
        expected_revision: snapshot.authority.revision,
        enrollment_revision: 1,
        decided_at: NOW,
        operation: AuthorityOperation::GrantNode {
            grant: bad,
            expected_generation: None,
        },
    }));
    assert!(
        roots.replicas[roots.leader]
            .submit(request, &AttemptedBypass)
            .is_err()
    );
    roots.commit(ControlCommand::Authority(AuthorityCommand {
        expected_revision: snapshot.authority.revision,
        enrollment_revision: 1,
        decided_at: NOW,
        operation: AuthorityOperation::GrantNode {
            grant: grant.clone(),
            expected_generation: None,
        },
    }));
    let delegation = Delegation {
        namespace: NamespaceRange::all(),
        partition: PartitionId([3; 16]),
        region: RegionId([1; 16]),
        log_group: LogGroupId(PART),
        epoch: 1,
        activation: None,
    };
    let snapshot = roots.snapshot();
    roots.commit(ControlCommand::VerifiedRoot(VerifiedRootCommand {
        command: RootCommand {
            expected_revision: 1,
            operation: RootOperation::Delegate {
                delegation: delegation.clone(),
            },
        },
        evidence: evidence(&snapshot, NOW + 1),
    }));
    let initial = roots.snapshot();
    let directory = DirectoryPartition::new(
        ClusterId(CLUSTER),
        delegation,
        PartitionConfig::default(),
        budget(),
    )
    .unwrap();
    let mut parts = Group::open(
        &dirs.path().join("partition"),
        ControlBootstrap::partition(&directory),
        PART,
    );
    let mut rogue = initial.clone();
    rogue.source.genesis = [9; 32];
    let request = parts.request(ControlCommand::ActivateAuthority(
        AuthorityActivation::Partition {
            expected_partition_revision: 0,
            snapshot: rogue,
        },
    ));
    assert!(parts.replicas[0].submit(request, &AttemptedBypass).is_err());
    parts.commit(ControlCommand::ActivateAuthority(
        AuthorityActivation::Partition {
            expected_partition_revision: 0,
            snapshot: initial.clone(),
        },
    ));
    let node = initial.authority.nodes[&1].enrollment.clone();
    let mut forged = node.clone();
    forged.zone = ZoneId([99; 16]);
    let request = parts.request(ControlCommand::VerifiedPartition(
        VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: 0,
                delegation_epoch: 1,
                operation: PartitionOperation::Enroll {
                    node: forged,
                    expected_generation: None,
                },
            },
            evidence: evidence(&initial, NOW + 2),
        },
    ));
    assert!(matches!(
        parts.replicas[0].submit(request, &AttemptedBypass),
        Err(ControlError::Directory(DirectoryError::UnverifiedAuthority))
    ));
    let command = PartitionCommand {
        expected_revision: 0,
        delegation_epoch: 1,
        operation: PartitionOperation::Enroll {
            node,
            expected_generation: None,
        },
    };
    let legacy = parts.request(ControlCommand::Partition(command.clone()));
    assert!(matches!(
        parts.replicas[0].submit(legacy, &AttemptedBypass),
        Err(ControlError::Directory(DirectoryError::UnverifiedAuthority))
    ));
    parts.commit(ControlCommand::VerifiedPartition(
        VerifiedPartitionCommand {
            command,
            evidence: evidence(&initial, NOW + 2),
        },
    ));
    grant.enrollment.generation = 2;
    roots.commit(ControlCommand::Authority(AuthorityCommand {
        expected_revision: initial.authority.revision,
        enrollment_revision: 1,
        decided_at: NOW + 3,
        operation: AuthorityOperation::GrantNode {
            grant,
            expected_generation: Some(1),
        },
    }));
    let revoke = roots.replicas[roots.leader]
        .enrollment()
        .unwrap()
        .prepare_revoke(founder.receipt().invitation, NOW + 4)
        .unwrap();
    assert_eq!(
        postcard::to_stdvec(&ControlCommand::Enrollment(revoke.clone())).unwrap()[0],
        1
    );
    roots.commit(ControlCommand::Enrollment(revoke));
    let revoked = roots.snapshot();
    // Installation records destination time independently of the root snapshot's
    // last authority mutation, preserving monotonic local verification time.
    parts.commit(ControlCommand::InstallAuthority(AuthorityInstallation {
        expected_source_index: initial.source_index,
        decided_at: NOW + 5,
        snapshot: revoked.clone(),
    }));
    let request = parts.request(ControlCommand::VerifiedPartition(
        VerifiedPartitionCommand {
            command: PartitionCommand {
                expected_revision: 1,
                delegation_epoch: 1,
                operation: PartitionOperation::Enroll {
                    node: revoked.authority.nodes[&1].enrollment.clone(),
                    expected_generation: Some(1),
                },
            },
            evidence: evidence(&revoked, NOW + 5),
        },
    ));
    assert!(matches!(
        parts.replicas[0].submit(request.clone(), &AttemptedBypass),
        Err(ControlError::Directory(DirectoryError::UnverifiedAuthority))
    ));
    let stale = parts.request(ControlCommand::InstallAuthority(AuthorityInstallation {
        expected_source_index: revoked.source_index,
        decided_at: NOW + 6,
        snapshot: initial,
    }));
    assert!(parts.replicas[0].submit(stale, &AttemptedBypass).is_err());
    roots.restart();
    parts.restart();
    assert_eq!(roots.replicas[0].identity(), original_identity);
    assert!(roots.replicas.iter().all(|root| root.authority().is_some()));
    for part in &parts.replicas {
        assert_eq!(
            part.partition().unwrap().checkpoint().nodes[&1]
                .enrollment
                .generation,
            1
        );
    }
    assert!(matches!(
        parts.replicas[parts.leader].submit(request, &AttemptedBypass),
        Err(ControlError::Directory(DirectoryError::UnverifiedAuthority))
    ));
}

#[test]
fn legacy_bootstrap_genesis_and_command_bytes_remain_unchanged() {
    let root = RootDirectory::new(ClusterId(CLUSTER), RootConfig::default(), budget()).unwrap();
    // The enrollment bytes are opaque in this codec fixture; this is never used
    // to open a replica or install certificate authority.
    let bootstrap = ControlBootstrap::Root {
        directory: root.checkpoint().clone(),
        enrollment: vec![1, 2, 3],
    };
    let mut expected = vec![0, 1]; // Root bootstrap, root checkpoint schema 1.
    expected.extend_from_slice(&CLUSTER);
    expected.extend_from_slice(&[0, 0, 0, 3, 1, 2, 3]);
    assert_eq!(postcard::to_stdvec(&bootstrap).unwrap(), expected);
    let options = ControlOptions::new(NodeConfig::single(1, CLUSTER, ROOT));
    assert_eq!(
        bootstrap.identity(&options).unwrap().genesis,
        blake3::derive_key("focal.control.genesis.v1", &expected)
    );
    let command = ControlCommand::Root(RootCommand {
        expected_revision: 0,
        operation: RootOperation::RegisterRegion {
            region: RegionRecord {
                id: RegionId([1; 16]),
                label: "a".into(),
                authority_epoch: 1,
            },
            expected_epoch: None,
        },
    });
    let mut expected = vec![0, 0, 0]; // Root command, revision 0, RegisterRegion.
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[1, b'a', 1, 0]);
    assert_eq!(postcard::to_stdvec(&command).unwrap(), expected);
    let partition = ControlCommand::Partition(PartitionCommand {
        expected_revision: 0,
        delegation_epoch: 1,
        operation: PartitionOperation::SealForTransfer {
            operation: OperationId([1; 16]),
            destination: PartitionId([2; 16]),
            next_epoch: 2,
        },
    });
    let mut expected = vec![2, 0, 1, 0];
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[2; 16]);
    expected.push(2);
    assert_eq!(postcard::to_stdvec(&partition).unwrap(), expected);
}
