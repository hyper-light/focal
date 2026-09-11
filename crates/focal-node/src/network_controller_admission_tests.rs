use super::*;
use crate::{
    cluster::NoDirectoryAuthority,
    config::Settings,
    control_host::{ControlHostConfig, ControlOwner, ControlReplicationFrame},
    network_bootstrap::FoundingNetwork,
    node_directory::NodeDirectory,
};
use focal_enrollment::{BootstrapAuthority, Invitation, InviteOptions, JoinKey, JoinPreparation};
use focal_model::ContentHash;

// These tests use the founder's actual CA and durable root, including both
// enrollment commands for each joining identity. Only the registry bound changes
// before authority activation, so a full table needs just one real node grant.
struct Fixture {
    host: ControlHost,
    owner: ControlOwner,
    state: NetworkState,
    authority: BootstrapAuthority,
    now: i64,
    sequence: u64,
    budget: MemoryBudget,
    _replication: tokio::sync::mpsc::Receiver<ControlReplicationFrame>,
    _wal: focal_log::SharedWal,
    directory: NodeDirectory,
}
impl Fixture {
    async fn open(path: &std::path::Path, max_nodes: usize) -> Self {
        let mut settings = Settings::default();
        settings.node.data_dir = Some(path.to_owned());
        settings.node.advertise = Some("127.0.0.1:45678".into());
        let FoundingNetwork {
            state,
            control,
            wal,
            directory,
            ..
        } = FoundingNetwork::open(&settings).await.unwrap();
        drop(control);
        let mut options = ControlOptions::new(focal_consensus::NodeConfig::single(
            state.node,
            state.genesis.founder.cluster,
            state.genesis.root.group,
        ));
        options.authority.max_nodes = max_nodes;
        let budget = MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap();
        let mut control = ControlReplica::open_on_wal(
            options,
            state.genesis.bootstrap.clone(),
            budget.clone(),
            wal.clone(),
        )
        .unwrap();
        control.drain(&NoDirectoryAuthority).unwrap();
        control.campaign().unwrap();
        let recovered = control.drain(&NoDirectoryAuthority).unwrap();
        let (host, owner, replication) = ControlHost::spawn_recovered(
            control,
            NoDirectoryAuthority,
            ControlHostConfig::new(state.genesis.root_namespace),
            budget.clone(),
            recovered,
        )
        .unwrap();
        let now = unix_time().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            directory.root().join("cluster/network/authority"),
            state.genesis.founder.cluster,
            vec![state.sponsor.server_name.clone()],
            now,
        )
        .unwrap();
        let mut fixture = Self {
            host,
            owner,
            state,
            authority,
            now,
            sequence: 0,
            budget,
            _replication: replication,
            _wal: wal,
            directory,
        };
        let observation = fixture.observe().await;
        let command = next_root_command(
            &fixture.state,
            &observation,
            &BTreeSet::new(),
            fixture.now,
            &[],
        )
        .unwrap()
        .unwrap();
        assert!(matches!(command, ControlCommand::ActivateAuthority(_)));
        fixture.commit(command).await;
        fixture
    }
    fn peer(&self) -> AuthenticatedPeer {
        AuthenticatedPeer::local(PeerGrant {
            principal: ParticipantId([219; 16]),
            tenants: BTreeSet::from([self.state.genesis.root_namespace.tenant]),
            role: PeerRole::Runtime,
        })
        .unwrap()
    }
    fn request(&self, command: ControlCommand) -> ControlRequest {
        ControlRequest {
            id: ControlRequestId {
                client: [219; 16],
                sequence: self.sequence + 1,
            },
            acknowledged_through: self.sequence,
            command,
        }
    }
    async fn commit(&mut self, command: ControlCommand) -> ControlReceipt {
        let request = self.request(command);
        let result = self.host.submit(self.peer(), request.clone()).await;
        assert!(
            result.is_ok(),
            "command {:?} failed: {result:?}",
            request.command
        );
        let receipt = result.unwrap();
        assert_eq!(receipt.request, request.id);
        assert!(receipt.committed_index > 0);
        self.sequence += 1;
        receipt
    }
    async fn observe(&self) -> RootObservation {
        self.host.observe_root().await.unwrap()
    }
    async fn enrollment(&self) -> EnrollmentRegistry {
        let observation = self.observe().await;
        let ControlBootstrap::Root { enrollment, .. } = &observation.snapshot().state else {
            panic!("founding root");
        };
        EnrollmentRegistry::restore(
            enrollment,
            self.state.genesis.founder.cluster,
            EnrollmentLimits::default(),
        )
        .unwrap()
    }
    async fn enroll(&mut self) -> EnrollmentReceipt {
        let draft = self
            .enrollment()
            .await
            .prepare_invitation(
                &self.authority,
                InviteOptions {
                    endpoint: self.state.advertise.to_string(),
                    server_name: self.state.sponsor.server_name.clone(),
                    role: EnrollmentRole::Node,
                    expires_at: self.now + 600,
                },
                self.now,
            )
            .unwrap();
        self.commit(ControlCommand::Enrollment(draft.command().clone()))
            .await;
        let invitation = draft.release(&self.enrollment().await).unwrap();
        let key = JoinKey::open_or_create(
            self.directory
                .root()
                .join(format!("test-join-{}", self.sequence)),
            self.state.genesis.founder.cluster,
        )
        .unwrap();
        let tls = tls(
            &invitation,
            &self.authority,
            &self.state.sponsor.server_name,
        );
        let request = invitation.request_after_tls(&tls, &key, self.now).unwrap();
        let JoinPreparation::Commit(command) = self
            .enrollment()
            .await
            .prepare_join(&self.authority, &request, self.now)
            .unwrap()
        else {
            panic!("new joining identity");
        };
        self.commit(ControlCommand::Enrollment(command)).await;
        let receipt = self.enrollment().await.release(&request, self.now).unwrap();
        let registry = PeerRegistry::new(1).unwrap();
        let fingerprint = registry
            .register_certificate(
                &receipt.certificate,
                node_grant(
                    receipt.identity.node_id.unwrap(),
                    receipt.identity.principal,
                    &self.state,
                ),
            )
            .unwrap();
        let response = dispatch(
            &self.host,
            registry.authenticate(fingerprint).unwrap(),
            RequestEnvelope {
                protocol: PROTOCOL_VERSION,
                ledger: self.state.genesis.root_namespace,
                route_epoch: RouteEpoch(1),
                request_epoch: RequestEpoch(1),
                request_id: RequestId(receipt.request),
                operation: Operation::NodeContact {
                    group: self.state.genesis.root.group,
                    sequence: 1,
                    acknowledged_through: 0,
                    expected_generation: 0,
                    advertise: self.state.advertise,
                    region: None,
                    zone: None,
                    endpoint: None,
                },
            },
            &ControlHost::wire_limits(),
        )
        .await;
        let Response::Control { response } = response.result else {
            panic!("node contact rejected");
        };
        assert!(matches!(
            ControlReply::decode(&response, 128 * 1024).unwrap(),
            ControlReply::Committed(_)
        ));
        receipt
    }
    async fn authority_command(&self, operation: AuthorityOperation) -> ControlCommand {
        let observation = self.observe().await;
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: observation.authority().unwrap().revision,
            enrollment_revision: observation.snapshot().revisions.enrollment,
            decided_at: self.now,
            operation,
        })
    }
    async fn grant(&self, receipt: &EnrollmentReceipt) -> NodeTopologyGrant {
        let observation = self.observe().await;
        let node = receipt.identity.node_id.unwrap();
        let Some(ControlCommand::Authority(AuthorityCommand {
            operation: AuthorityOperation::GrantNode { grant, .. },
            ..
        })) = next_root_command(
            &self.state,
            &observation,
            &BTreeSet::from([node]),
            self.now,
            &[],
        )
        .unwrap()
        else {
            panic!("first capability grant");
        };
        grant
    }
    async fn install(&mut self, grant: NodeTopologyGrant, expected_generation: Option<u64>) {
        let command = self
            .authority_command(AuthorityOperation::GrantNode {
                grant,
                expected_generation,
            })
            .await;
        self.commit(command).await;
    }
    async fn close(self) {
        self.host.stop().await.unwrap();
        self.owner.join().unwrap();
    }
}

fn tls(
    invitation: &Invitation,
    authority: &BootstrapAuthority,
    server_name: &str,
) -> rustls::ClientConnection {
    // Rustls owns shared immutable connection configuration.
    let mut client = rustls::ClientConnection::new(
        std::sync::Arc::new(invitation.client_config().unwrap()),
        rustls::pki_types::ServerName::try_from(server_name.to_owned()).unwrap(),
    )
    .unwrap();
    let mut server = rustls::ServerConnection::new(std::sync::Arc::new(
        authority.server_identity().server_config().unwrap(),
    ))
    .unwrap();
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            server.process_new_packets().unwrap();
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut std::io::Cursor::new(bytes)).unwrap();
            client.process_new_packets().unwrap();
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return client;
        }
    }
    panic!("TLS handshake did not complete");
}

#[tokio::test]
async fn withdrawn_grant_fences_a_reopened_journal_intent_without_consuming_its_request() {
    let dir = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(dir.path(), 1).await;
    let receipt = fixture.enroll().await;
    let node = receipt.identity.node_id.unwrap();
    let grant = fixture.grant(&receipt).await;
    fixture.install(grant.clone(), None).await;
    let observation = fixture.observe().await;
    let command = next_root_command(
        &fixture.state,
        &observation,
        &BTreeSet::from([node]),
        fixture.now,
        &[],
    )
    .unwrap()
    .unwrap();
    assert!(matches!(command, ControlCommand::Membership(_)));
    let mut admission = RootAdmission::open(&fixture.state, dir.path()).unwrap();
    let intent = ControlRequest {
        id: ControlRequestId {
            client: admission.principal.0,
            sequence: 1,
        },
        acknowledged_through: 0,
        command,
    };
    admission.intent.pending = Some(intent.clone());
    admission.save().unwrap();
    drop(admission);
    let mut admission = RootAdmission::open(&fixture.state, dir.path()).unwrap();
    assert_eq!(admission.intent.pending.as_ref(), Some(&intent));
    let mut withdrawn = grant.clone();
    withdrawn.enrollment.generation = 2;
    withdrawn.enrollment.eligible = false;
    fixture.install(withdrawn, Some(1)).await;
    let after_withdrawal = fixture.observe().await;
    // Submit the old durable intent even with its old observation. The serialized
    // owner must recheck the currently committed capability before proposing.
    admission
        .advance(
            &fixture.state,
            &observation,
            &BTreeSet::from([node]),
            &fixture.host,
            &fixture.budget,
        )
        .await
        .unwrap();
    assert!(admission.intent.pending.is_none());
    assert_eq!(admission.intent.completed, 0);
    let rejected = fixture.observe().await;
    assert_eq!(rejected.configuration(), after_withdrawal.configuration());
    assert!(!rejected.configuration().configuration.contains(node));
    drop(admission);
    let mut admission = RootAdmission::open(&fixture.state, dir.path()).unwrap();
    assert!(admission.intent.pending.is_none());
    assert_eq!(admission.intent.completed, 0);

    let mut restored = grant.clone();
    restored.enrollment.generation = 3;
    fixture.install(restored, Some(2)).await;
    admission
        .advance(
            &fixture.state,
            &fixture.observe().await,
            &BTreeSet::from([node]),
            &fixture.host,
            &fixture.budget,
        )
        .await
        .unwrap();
    assert_eq!(admission.intent.completed, 1);
    assert!(admission.intent.pending.is_none());
    assert!(
        fixture
            .observe()
            .await
            .configuration()
            .configuration
            .learners
            .contains(&node)
    );

    // A later withdrawal cannot erase an already committed exact receipt.
    let mut withdrawn = grant;
    withdrawn.enrollment.generation = 4;
    withdrawn.enrollment.eligible = false;
    fixture.install(withdrawn, Some(3)).await;
    let retry_peer = AuthenticatedPeer::local(PeerGrant {
        principal: admission.principal,
        tenants: BTreeSet::from([fixture.state.genesis.root_namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let retry = fixture.host.submit(retry_peer, intent).await.unwrap();
    assert_eq!(retry.request.sequence, 1);
    fixture.close().await;
}

#[tokio::test]
async fn expired_or_mismatched_capabilities_cannot_select_a_learner() {
    let dir = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(dir.path(), 1).await;
    let receipt = fixture.enroll().await;
    let node = receipt.identity.node_id.unwrap();
    let mut grant = fixture.grant(&receipt).await;
    let initial = fixture.observe().await;
    for wrong_principal in [true, false] {
        let mut malformed = grant.clone();
        if wrong_principal {
            malformed.principal = [123; 16];
        } else {
            malformed.enrollment.identity = ContentHash([124; 32]);
        }
        let command = fixture
            .authority_command(AuthorityOperation::GrantNode {
                grant: malformed,
                expected_generation: None,
            })
            .await;
        assert_eq!(
            fixture
                .host
                .submit(fixture.peer(), fixture.request(command))
                .await,
            Err(ControlFailure::Unauthorized)
        );
        let observation = fixture.observe().await;
        assert_eq!(observation.authority(), initial.authority());
        assert_eq!(observation.configuration(), initial.configuration());
        assert!(matches!(
            next_root_command(
                &fixture.state,
                &observation,
                &BTreeSet::from([node]),
                fixture.now,
                &[]
            )
            .unwrap(),
            Some(ControlCommand::Authority(_))
        ));
    }
    grant.expires_at = fixture.now + 60;
    fixture.install(grant.clone(), None).await;
    let observation = fixture.observe().await;
    assert!(matches!(
        next_root_command(
            &fixture.state,
            &observation,
            &BTreeSet::from([node]),
            grant.expires_at - 1,
            &[]
        )
        .unwrap(),
        Some(ControlCommand::Membership(_))
    ));
    assert_eq!(
        next_root_command(
            &fixture.state,
            &observation,
            &BTreeSet::from([node]),
            grant.expires_at,
            &[]
        )
        .unwrap(),
        None
    );
    // Advancing the committed authority clock also fences an already-built
    // membership command at the owner, even without a controller refresh.
    let command = next_root_command(
        &fixture.state,
        &observation,
        &BTreeSet::from([node]),
        fixture.now,
        &[],
    )
    .unwrap()
    .unwrap();
    fixture.now = grant.expires_at;
    let advance = fixture
        .authority_command(AuthorityOperation::AdvanceClock)
        .await;
    fixture.commit(advance).await;
    assert_eq!(
        fixture
            .host
            .submit(fixture.peer(), fixture.request(command))
            .await,
        Err(ControlFailure::CompareFailed)
    );
    assert!(
        !fixture
            .observe()
            .await
            .configuration()
            .configuration
            .contains(node)
    );
    fixture.close().await;
}

#[tokio::test]
async fn full_capability_table_admits_an_existing_grant_before_an_ungranted_lower_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(dir.path(), 1).await;
    let lower = fixture.enroll().await;
    let higher = fixture.enroll().await;
    let lower_node = lower.identity.node_id.unwrap();
    let higher_node = higher.identity.node_id.unwrap();
    assert!(lower_node < higher_node);
    let higher_grant = fixture.grant(&higher).await;
    fixture.install(higher_grant, None).await;
    let lower_grant = fixture.grant(&lower).await;
    let overflow = fixture
        .authority_command(AuthorityOperation::GrantNode {
            grant: lower_grant,
            expected_generation: None,
        })
        .await;
    assert_eq!(
        fixture
            .host
            .submit(fixture.peer(), fixture.request(overflow))
            .await,
        Err(ControlFailure::Capacity)
    );
    let observation = fixture.observe().await;
    assert_eq!(observation.authority().unwrap().nodes.len(), 1);
    let command = next_root_command(
        &fixture.state,
        &observation,
        &BTreeSet::from([lower_node, higher_node]),
        fixture.now,
        &[],
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        command,
        ControlCommand::Membership(ControlMembershipCommand {
            change: MembershipChange::AddLearner { node },
            ..
        }) if node == higher_node
    ));
    fixture.commit(command).await;
    let admitted = fixture.observe().await;
    assert_eq!(admitted.authority().unwrap().nodes.len(), 1);
    assert!(
        admitted
            .configuration()
            .configuration
            .learners
            .contains(&higher_node)
    );
    assert!(!admitted.configuration().configuration.contains(lower_node));
    fixture.close().await;
}
