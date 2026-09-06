use super::*;
use focal_consensus::{
    ConfChangeSingle, ConfChangeType, ConfChangeV2, DurableNode, NodeConfig, PbMessageExt,
};
use focal_enrollment::{
    BootstrapAuthority, CredentialMaterial, EnrollmentLimits, EnrollmentReceipt,
    EnrollmentRegistry, EnrollmentRole, Invitation, InviteOptions, JoinKey, JoinPreparation,
    server_fingerprint,
};
use focal_memory::MemoryBudget;
use focal_model::{
    ContentHash, LedgerId, RaftIndex, RaftTerm, RouteEpoch, SessionId, SessionSeq, TenantId,
};
use std::{
    collections::BTreeMap,
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 8 * 1024 * 1024).unwrap()
}
fn tls(invitation: &Invitation, authority: &BootstrapAuthority) -> rustls::ClientConnection {
    // Rustls requires shared configuration. No application-owned shared state.
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(Arc::new(
        authority.server_identity().server_config().unwrap(),
    ))
    .unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return client;
        }
    }
    panic!("TLS did not finish")
}
struct Fixture {
    dir: tempfile::TempDir,
    now: i64,
    enrollment: EnrollmentRegistry,
    authority: AuthorityRegistry,
    metadata: DurableNode,
    credentials: BTreeMap<u64, CredentialMaterial>,
    receipts: BTreeMap<u64, EnrollmentReceipt>,
}
impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let issuer = BootstrapAuthority::open_or_create(
            dir.path().join("ca"),
            [61; 16],
            vec!["localhost".into()],
            now,
        )
        .unwrap();
        let mut enrollment = EnrollmentRegistry::new(
            [61; 16],
            issuer.ca_certificate().to_vec(),
            2,
            EnrollmentLimits::default(),
        )
        .unwrap();
        let mut credentials = BTreeMap::new();
        let mut receipts = BTreeMap::new();
        for ordinal in 0..4 {
            let draft = enrollment
                .prepare_invitation(
                    &issuer,
                    InviteOptions {
                        endpoint: "127.0.0.1:8443".into(),
                        server_name: "localhost".into(),
                        role: EnrollmentRole::Node,
                        expires_at: now + 600,
                    },
                    now,
                )
                .unwrap();
            enrollment
                .apply_committed(draft.command(), enrollment.applied_index() + 1)
                .unwrap();
            let invitation = draft.release(&enrollment).unwrap();
            let key = JoinKey::open_or_create(dir.path().join(format!("key{ordinal}")), [61; 16])
                .unwrap();
            let request = invitation
                .request_after_tls(&tls(&invitation, &issuer), &key, now)
                .unwrap();
            let JoinPreparation::Commit(command) =
                enrollment.prepare_join(&issuer, &request, now).unwrap()
            else {
                panic!("new enrollment")
            };
            enrollment
                .apply_committed(&command, enrollment.applied_index() + 1)
                .unwrap();
            let receipt = enrollment.release(&request, now).unwrap();
            let node = receipt.identity.node_id.unwrap();
            credentials.insert(
                node,
                key.complete(&receipt, issuer.ca_certificate(), now)
                    .unwrap(),
            );
            receipts.insert(node, receipt);
        }
        let anchor = AuthorityAnchor {
            cluster: ClusterId([61; 16]),
            metadata_group: LogGroupId([62; 16]),
            genesis: ContentHash(*blake3::hash(b"metadata genesis").as_bytes()),
            namespace: NamespaceRange::all(),
            enrollment_ca: ContentHash(server_fingerprint(issuer.ca_certificate())),
        };
        let authority =
            AuthorityRegistry::new(anchor, AuthorityConfig::default(), budget()).unwrap();
        let mut metadata = DurableNode::open(
            NodeConfig::single(2, [61; 16], [62; 16]),
            dir.path().join("metadata-wal"),
        )
        .unwrap();
        metadata.campaign().unwrap();
        metadata.drain().unwrap();
        Self {
            dir,
            now,
            enrollment,
            authority,
            metadata,
            credentials,
            receipts,
        }
    }
    fn command(&self, operation: AuthorityOperation) -> AuthorityCommand {
        AuthorityCommand {
            expected_revision: self.authority.revision(),
            enrollment_revision: self.enrollment.revision(),
            decided_at: self.now,
            operation,
        }
    }
    fn commit(&mut self, operation: AuthorityOperation) {
        let command = self.command(operation);
        let prepared = self.authority.prepare(&command, &self.enrollment).unwrap();
        self.metadata
            .propose(postcard::to_allocvec(&command).unwrap())
            .unwrap();
        let events = self.metadata.drain().unwrap();
        let entry = events.committed.first().unwrap();
        assert_eq!(
            postcard::from_bytes::<AuthorityCommand>(&entry.data).unwrap(),
            command
        );
        self.authority.publish(prepared, entry.index).unwrap();
    }
    fn grant(&self, node: u64) -> NodeTopologyGrant {
        let receipt = &self.receipts[&node];
        NodeTopologyGrant {
            enrollment: NodeEnrollment {
                node,
                generation: 1,
                region: RegionId([1; 16]),
                zone: ZoneId([2; 16]),
                endpoint: format!("node{node}.localhost:8443"),
                identity: ContentHash(server_fingerprint(&receipt.certificate)),
                authority_epoch: 1,
                attestation: ContentHash([0; 32]),
                eligible: true,
            },
            principal: receipt.identity.principal,
            expires_at: self.now + 600,
        }
    }
    fn install_nodes(&mut self) {
        for node in 2..=5 {
            self.commit(AuthorityOperation::GrantNode {
                grant: self.grant(node),
                expected_generation: None,
            });
        }
    }
    fn session_group(&self) -> GroupAuthorityGrant {
        GroupAuthorityGrant {
            group: LogGroupId([63; 16]),
            genesis: ContentHash(*blake3::hash(b"session genesis").as_bytes()),
            scope: GroupScope::Session(ledger()),
            membership_epoch: 1,
            voters: BTreeMap::from([(2, 1), (3, 1), (4, 1)]),
            outgoing_voters: BTreeMap::new(),
            learners: BTreeMap::new(),
            expires_at: self.now + 500,
        }
    }
}
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId([1; 16]),
        session: SessionId([2; 16]),
    }
}

#[test]
fn topology_requires_enrolled_identity_and_actual_metadata_publication() {
    let mut fixture = Fixture::new();
    let grant = fixture.grant(2);
    let pending = fixture
        .authority
        .prepare(
            &fixture.command(AuthorityOperation::GrantNode {
                grant: grant.clone(),
                expected_generation: None,
            }),
            &fixture.enrollment,
        )
        .unwrap();
    let provisional = pending.checkpoint().nodes[&2].enrollment.clone();
    assert!(
        fixture
            .authority
            .verifier(&fixture.enrollment, &[], fixture.now)
            .unwrap()
            .verify_enrollment(&provisional)
            .is_err()
    );
    assert!(
        AuthorityRegistry::restore(
            pending.checkpoint().clone(),
            &pending.checkpoint().anchor,
            AuthorityConfig::default(),
            budget()
        )
        .is_err()
    );
    drop(pending);
    let mut forgery = grant.clone();
    forgery.principal = [9; 16];
    assert!(
        fixture
            .authority
            .prepare(
                &fixture.command(AuthorityOperation::GrantNode {
                    grant: forgery,
                    expected_generation: None
                }),
                &fixture.enrollment
            )
            .is_err()
    );
    fixture.commit(AuthorityOperation::GrantNode {
        grant,
        expected_generation: None,
    });
    let enrolled = fixture.authority.node(2).unwrap().enrollment.clone();
    let verifier = fixture
        .authority
        .verifier(&fixture.enrollment, &[], fixture.now)
        .unwrap();
    verifier.verify_enrollment(&enrolled).unwrap();
    for tampered in [
        NodeEnrollment {
            zone: ZoneId([9; 16]),
            ..enrolled.clone()
        },
        NodeEnrollment {
            endpoint: "rogue:1".into(),
            ..enrolled.clone()
        },
        NodeEnrollment {
            generation: 2,
            ..enrolled.clone()
        },
    ] {
        assert!(verifier.verify_enrollment(&tampered).is_err());
    }
    assert!(
        fixture
            .authority
            .verifier(&fixture.enrollment, &[], fixture.now + 600)
            .unwrap()
            .verify_enrollment(&enrolled)
            .is_err()
    );
    let checkpoint = fixture.authority.checkpoint().clone();
    let mut bad_anchor = checkpoint.anchor.clone();
    bad_anchor.cluster = ClusterId([99; 16]);
    assert!(
        AuthorityRegistry::restore(
            checkpoint.clone(),
            &bad_anchor,
            AuthorityConfig::default(),
            budget()
        )
        .is_err()
    );
    let restored = AuthorityRegistry::restore(
        checkpoint.clone(),
        &checkpoint.anchor,
        AuthorityConfig::default(),
        budget(),
    )
    .unwrap();
    restored
        .verifier(&fixture.enrollment, &[], fixture.now)
        .unwrap()
        .verify_enrollment(&enrolled)
        .unwrap();
    let revoke = fixture
        .enrollment
        .prepare_revoke(fixture.receipts[&2].invitation, fixture.now)
        .unwrap();
    fixture
        .enrollment
        .apply_committed(&revoke, fixture.enrollment.applied_index() + 1)
        .unwrap();
    assert!(
        restored
            .verifier(&fixture.enrollment, &[], fixture.now)
            .unwrap()
            .verify_enrollment(&enrolled)
            .is_err()
    );
}

fn pump(nodes: &mut BTreeMap<u64, DurableNode>) {
    for _ in 0..32 {
        let mut messages = Vec::new();
        for node in nodes.values_mut() {
            messages.extend(node.drain().unwrap().messages);
        }
        if messages.is_empty() {
            return;
        }
        for message in messages {
            if let Some(node) = nodes.get_mut(&message.to) {
                node.step(message).unwrap();
            }
        }
    }
    panic!("Raft simulation did not drain")
}
#[test]
fn real_committed_configuration_needs_installed_quorum_and_exact_scoped_signatures() {
    let mut fixture = Fixture::new();
    fixture.install_nodes();
    let group = fixture.session_group();
    fixture.commit(AuthorityOperation::BootstrapGroup {
        grant: group.clone(),
    });
    let mut nodes = BTreeMap::new();
    for id in 2..=4 {
        let mut config = NodeConfig::single(id, [61; 16], group.group.0);
        config.voters = vec![2, 3, 4];
        nodes.insert(
            id,
            DurableNode::open(config, fixture.dir.path().join(format!("session{id}"))).unwrap(),
        );
    }
    nodes.get_mut(&2).unwrap().campaign().unwrap();
    pump(&mut nodes);
    let mut change = ConfChangeV2::default();
    let mut add = ConfChangeSingle {
        node_id: 5,
        ..Default::default()
    };
    add.set_change_type(ConfChangeType::AddLearnerNode);
    change.changes.push(add);
    let record_hash = ContentHash(*blake3::hash(&change.write_to_bytes().unwrap()).as_bytes());
    nodes
        .get_mut(&2)
        .unwrap()
        .propose_conf_change(change)
        .unwrap();
    pump(&mut nodes);
    let mut next = group.clone();
    next.membership_epoch = 2;
    next.learners.insert(5, 1);
    let status = nodes[&2].status();
    for node in nodes.values() {
        assert_eq!(node.status().learners, vec![5]);
        assert_eq!(node.status().committed_index, status.committed_index);
    }
    let statement = AuthorityStatement {
        anchor: fixture.authority.checkpoint().anchor.clone(),
        authority_revision: fixture.authority.revision(),
        enrollment_revision: fixture.enrollment.revision(),
        group: group.group,
        group_genesis: group.genesis,
        membership_epoch: 1,
        issued_at: fixture.now,
        expires_at: fixture.now + 100,
        fact: AuthorityFact::Membership {
            next,
            index: RaftIndex(status.committed_index),
            term: RaftTerm(status.term),
            record_hash,
        },
    };
    let mut proof = AuthorityProof {
        statement,
        signatures: Vec::new(),
    };
    proof
        .signatures
        .push(proof.statement.sign(&fixture.credentials[&2]).unwrap());
    let prepare = |f: &Fixture, proof: AuthorityProof| {
        f.authority.prepare(
            &f.command(AuthorityOperation::ChangeGroup { proof }),
            &f.enrollment,
        )
    };
    assert!(matches!(
        prepare(&fixture, proof.clone()),
        Err(DirectoryError::Quorum)
    ));
    let mut duplicate = proof.clone();
    duplicate.signatures.push(duplicate.signatures[0].clone());
    assert!(matches!(
        prepare(&fixture, duplicate),
        Err(DirectoryError::Duplicate)
    ));
    let mut nonmember = proof.clone();
    nonmember
        .signatures
        .push(nonmember.statement.sign(&fixture.credentials[&5]).unwrap());
    assert!(matches!(
        prepare(&fixture, nonmember),
        Err(DirectoryError::StaleNode)
    ));
    proof
        .signatures
        .push(proof.statement.sign(&fixture.credentials[&3]).unwrap());
    prepare(&fixture, proof.clone()).unwrap();
    for bad in [
        AuthorityProof {
            statement: AuthorityStatement {
                group_genesis: ContentHash([7; 32]),
                ..proof.statement.clone()
            },
            ..proof.clone()
        },
        AuthorityProof {
            statement: AuthorityStatement {
                enrollment_revision: 1,
                ..proof.statement.clone()
            },
            ..proof.clone()
        },
        AuthorityProof {
            statement: AuthorityStatement {
                expires_at: fixture.now,
                ..proof.statement.clone()
            },
            ..proof.clone()
        },
    ] {
        assert!(prepare(&fixture, bad).is_err());
    }
    let saved = fixture.authority.checkpoint().clone();
    fixture.authority = AuthorityRegistry::restore(
        saved.clone(),
        &saved.anchor,
        AuthorityConfig::default(),
        budget(),
    )
    .unwrap();
    fixture.enrollment = EnrollmentRegistry::restore(
        &fixture.enrollment.checkpoint().unwrap(),
        [61; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    prepare(&fixture, proof.clone()).unwrap();
    fixture.commit(AuthorityOperation::ChangeGroup {
        proof: proof.clone(),
    });
    assert_eq!(
        fixture.authority.group(group.group).unwrap().learners,
        BTreeMap::from([(5, 1)])
    );
    assert!(prepare(&fixture, proof).is_err()); // Exact old metadata proof cannot reconfigure again.
}

#[test]
fn certificates_and_topology_do_not_invent_session_or_custody_authority() {
    let mut fixture = Fixture::new();
    fixture.install_nodes();
    let group = fixture.session_group();
    fixture.commit(AuthorityOperation::BootstrapGroup {
        grant: group.clone(),
    });
    let verifier = fixture
        .authority
        .verifier(&fixture.enrollment, &[], fixture.now)
        .unwrap();
    let uncommitted = SessionFence {
        kind: SessionFenceKind::Created,
        ledger: ledger(),
        log_group: group.group,
        operation: OperationId([1; 16]),
        sequence: SessionSeq(1),
        index: RaftIndex(1),
        term: RaftTerm(1),
        from_route: RouteEpoch(0),
        to_route: RouteEpoch(1),
        membership_epoch: 1,
        placement_epoch: 1,
        placement_digest: ContentHash([2; 32]),
        record_hash: ContentHash([3; 32]),
    };
    assert!(matches!(
        verifier.verify_session_fence(&uncommitted),
        Err(DirectoryError::UnverifiedAuthority)
    ));
    let ready = ReplicaReady {
        ledger: ledger(),
        operation: OperationId([1; 16]),
        route_epoch: RouteEpoch(2),
        node: 2,
        node_generation: 1,
        through: SessionSeq(1),
        custody: ContentHash([4; 32]),
        attestation: ContentHash([5; 32]),
    };
    assert!(verifier.verify_replica_ready(&ready).is_err());
    let mut restricted = fixture.authority.checkpoint().clone();
    restricted.anchor.namespace = NamespaceRange {
        start: NamespaceKey::of(ledger()),
        end: Some(NamespaceKey([2; 32])),
    };
    // Existing node grant hashes bind the anchor, so rebinding a checkpoint fails.
    assert!(
        AuthorityRegistry::restore(
            restricted.clone(),
            &restricted.anchor,
            AuthorityConfig::default(),
            budget()
        )
        .is_err()
    );
    let config = AuthorityConfig {
        max_members: 1,
        ..AuthorityConfig::default()
    };
    assert!(
        AuthorityRegistry::restore(
            fixture.authority.checkpoint().clone(),
            &fixture.authority.checkpoint().anchor,
            config,
            budget()
        )
        .is_err()
    );
}

#[test]
fn grant_capacity_namespace_and_explicit_generations_fail_without_publication() {
    let mut fixture = Fixture::new();
    let mut anchor = fixture.authority.checkpoint().anchor.clone();
    anchor.namespace = NamespaceRange {
        start: NamespaceKey::of(ledger()),
        end: Some(NamespaceKey([2; 32])),
    };
    let memory = budget();
    fixture.authority = AuthorityRegistry::new(
        anchor,
        AuthorityConfig {
            max_nodes: 1,
            ..AuthorityConfig::default()
        },
        memory.clone(),
    )
    .unwrap();
    fixture.commit(AuthorityOperation::GrantNode {
        grant: fixture.grant(2),
        expected_generation: None,
    });
    let before = fixture.authority.checkpoint().clone();
    let charged = memory.stats().used;
    let second = fixture.command(AuthorityOperation::GrantNode {
        grant: fixture.grant(3),
        expected_generation: None,
    });
    assert!(
        fixture
            .authority
            .prepare(&second, &fixture.enrollment)
            .is_err()
    );
    assert_eq!(memory.stats().used, charged);
    assert_eq!(fixture.authority.checkpoint(), &before);
    let mut group = fixture.session_group();
    group.voters = BTreeMap::from([(2, 1)]);
    group.scope = GroupScope::Session(LedgerId {
        tenant: TenantId([99; 16]),
        ..ledger()
    });
    assert!(matches!(
        fixture.authority.prepare(
            &fixture.command(AuthorityOperation::BootstrapGroup { grant: group }),
            &fixture.enrollment
        ),
        Err(DirectoryError::OutsideNamespace)
    ));
    let mut regrant = fixture.grant(2);
    regrant.enrollment.generation = 3;
    assert!(matches!(
        fixture.authority.prepare(
            &fixture.command(AuthorityOperation::GrantNode {
                grant: regrant,
                expected_generation: Some(1)
            }),
            &fixture.enrollment
        ),
        Err(DirectoryError::StaleNode)
    ));
    assert_eq!(fixture.authority.checkpoint(), &before);
}
