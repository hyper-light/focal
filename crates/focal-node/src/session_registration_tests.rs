use super::*;
use crate::{
    cluster::NoDirectoryAuthority,
    control_host::{ControlHost, ControlHostConfig},
    network_bootstrap::{FoundingNetwork, unix_time},
    placement_proof::ProofWindow,
};
use focal_consensus::{DurableNode, NodeConfig};
use focal_control::*;
use focal_directory::*;
use focal_ledger::{SessionLimits, Submission};
use focal_model::*;
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};

fn budget() -> MemoryBudget {
    MemoryBudget::new(256 * 1024 * 1024, 64 * 1024 * 1024).unwrap()
}
fn commit(owner: &mut ControlReplica, sequence: u64, command: ControlCommand) {
    let id = ControlRequestId {
        client: [114; 16],
        sequence,
    };
    owner
        .submit(
            ControlRequest {
                id,
                acknowledged_through: sequence - 1,
                command,
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    for _ in 0..8 {
        owner.drain(&NoDirectoryAuthority).unwrap();
    }
    assert!(owner.receipt(id).unwrap().is_some());
}
fn peer(namespace: LedgerId, principal: ParticipantId) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal,
        tenants: BTreeSet::from([namespace.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
async fn setup(path: &std::path::Path) -> (FoundingNetwork, Settings, Session, FirstDirectoryPlan) {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(path.into());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let id = network.directory.identity().clone();
    let consensus = DurableNode::open_on_wal(
        NodeConfig::single(id.node, id.cluster, id.ledger.session.0),
        network.wal.clone(),
    )
    .unwrap();
    let mut session = Session::from_node(id.ledger, consensus, SessionLimits::default()).unwrap();
    session.campaign().unwrap();
    for _ in 0..8 {
        session.poll().unwrap();
    }
    for (label, command) in [
        (
            "registration-epoch",
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        ),
        (
            "registration-claim",
            Command::GenerateClaim {
                claim: crate::demo::claim(&id, ClaimId::from_u128(901)).unwrap(),
            },
        ),
    ] {
        assert!(matches!(
            session
                .submit_local(&crate::demo::request(
                    &id,
                    label,
                    id.issuer,
                    command,
                    vec![]
                ))
                .unwrap(),
            Submission::Committed(_)
        ));
    }
    let now = unix_time().unwrap();
    let directory = FirstDirectoryPlan::derive(id.cluster, id.node).unwrap();
    commit(
        &mut network.control,
        1,
        ControlCommand::Root(RootCommand {
            expected_revision: 0,
            operation: RootOperation::Delegate {
                delegation: directory.delegation(),
            },
        }),
    );
    commit(
        &mut network.control,
        2,
        ControlCommand::ActivateAuthority(AuthorityActivation::Root {
            expected_root_revision: 1,
            expected_enrollment_revision: 1,
            decided_at: now,
        }),
    );
    commit(
        &mut network.control,
        3,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: 1,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::GrantNode {
                grant: NodeTopologyGrant {
                    enrollment: NodeEnrollment {
                        node: id.node,
                        generation: 1,
                        region: RegionId::UNKNOWN,
                        zone: ZoneId([0; 16]),
                        endpoint: "127.0.0.1:7443".into(),
                        identity: ContentHash(network.receipt.public_key),
                        authority_epoch: 1,
                        attestation: ContentHash::default(),
                        eligible: true,
                    },
                    principal: id.issuer.0,
                    expires_at: now + 300,
                },
                expected_generation: None,
            },
        }),
    );
    commit(
        &mut network.control,
        4,
        ControlCommand::Authority(AuthorityCommand {
            expected_revision: 2,
            enrollment_revision: 1,
            decided_at: now,
            operation: AuthorityOperation::BootstrapGroup {
                grant: GroupAuthorityGrant {
                    group: directory.group(),
                    genesis: ContentHash(directory.identity().unwrap().genesis),
                    scope: GroupScope::Partition {
                        partition: directory.partition(),
                        namespace: NamespaceRange::all(),
                    },
                    membership_epoch: 1,
                    voters: BTreeMap::from([(id.node, 1)]),
                    outgoing_voters: BTreeMap::new(),
                    learners: BTreeMap::new(),
                    expires_at: now + 300,
                },
            },
        }),
    );
    (network, settings, session, directory)
}
fn views(owner: &ControlReplica) -> (ControlSnapshot, ControlAuthoritySnapshot) {
    let ControlReadResult::State(state) = owner.read_local(&ControlRead::State).unwrap() else {
        panic!()
    };
    let ControlReadResult::Authority(Some(authority)) =
        owner.read_local(&ControlRead::Authority).unwrap()
    else {
        panic!()
    };
    (state, authority)
}

#[tokio::test]
async fn founder_registration_preserves_existing_application_and_replays_exact_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let (network, settings, mut session, partition_plan) = setup(directory.path()).await;
    let memory = budget();
    let id = network.directory.identity().clone();
    let application = session.read_at_least(SessionSeq(0)).unwrap().clone();
    assert!(!application.claims.is_empty());
    let plan = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &settings,
        session.memory_stats().limit as u64,
        &memory,
    )
    .unwrap();
    assert_eq!(plan.founder(), &id);
    let operation = plan.operation();
    let client = plan.client();
    let (host, owner, outgoing) = ControlHost::spawn(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        budget(),
    )
    .unwrap();
    let observation = host.observe_root().await.unwrap();
    let now = unix_time().unwrap();
    let initial = plan.prepare(&observation, now, &memory).unwrap();
    assert_eq!(
        initial.placement_request().placement.policy.required_memory,
        session.memory_stats().limit as u64
    );
    let grant = initial.root_command().unwrap().clone();
    let operator = peer(network.state.genesis.root_namespace, ParticipantId(client));
    host.submit(
        operator.clone(),
        ControlRequest {
            id: ControlRequestId {
                client,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: grant,
        },
    )
    .await
    .unwrap();
    session
        .propose_placement(initial.placement_request())
        .unwrap();
    assert!(
        session
            .placement_witness(initial.placement_request())
            .unwrap()
            .is_none()
    );
    for _ in 0..8 {
        session.poll().unwrap();
    }
    assert_eq!(session.read_at_least(SessionSeq(0)).unwrap(), &application);
    let witness = session
        .placement_witness(initial.placement_request())
        .unwrap()
        .unwrap();
    assert_eq!(witness.fence().sequence, session.sequence());
    let refreshed = host.observe_root().await.unwrap();
    assert!(
        plan.prepare(&refreshed, now, &memory)
            .unwrap()
            .root_command()
            .is_none()
    );
    let permit = host.prepare_directory(partition_plan).await.unwrap();
    let mut partition = permit
        .open(network.wal.clone(), &budget(), None)
        .unwrap()
        .into_replica();
    let window = ProofWindow {
        issued_at: now,
        expires_at: now + 60,
    };
    let signed = host
        .prepare_session_proof(
            session
                .placement_witness(initial.placement_request())
                .unwrap()
                .unwrap(),
            window,
        )
        .await
        .unwrap()
        .sign(&network.credentials)
        .unwrap();
    let (state, installed) = views(&partition);
    assert!(matches!(
        initial.create_session(&state, &installed, &witness, signed, now, &memory),
        Err(SessionRegistrationError::NotReady)
    ));
    let enroll = initial
        .partition_enrollment(&state, &installed, now, &memory)
        .unwrap()
        .unwrap();
    commit(&mut partition, 1, enroll.command().clone());
    let (state, installed) = views(&partition);
    assert!(
        initial
            .partition_enrollment(&state, &installed, now, &memory)
            .unwrap()
            .is_none()
    );
    let sign = || {
        host.prepare_session_proof(
            session
                .placement_witness(initial.placement_request())
                .unwrap()
                .unwrap(),
            window,
        )
    };
    let signed = sign().await.unwrap().sign(&network.credentials).unwrap();
    let create = initial
        .create_session(&state, &installed, &witness, signed, now, &memory)
        .unwrap()
        .unwrap();
    let serialized = postcard::to_stdvec(create.command()).unwrap();
    commit(&mut partition, 2, create.command().clone());
    assert_eq!(session.read_at_least(SessionSeq(0)).unwrap(), &application);
    let descriptor = &partition.partition().unwrap().checkpoint().sessions[&id.ledger];
    assert_eq!(descriptor.authority, *witness.fence());
    assert_eq!(descriptor.active, initial.placement_request().placement);
    assert_eq!(descriptor.active.policy.durability.max_failures, 0);
    // A newer authority version requires a refreshed directory installation,
    // not silently accepting a signature with mismatched pinned revisions.
    let mut stale = installed.clone();
    stale.authority.revision += 1;
    let signed = sign().await.unwrap().sign(&network.credentials).unwrap();
    assert!(
        initial
            .create_session(&state, &stale, &witness, signed, now, &memory)
            .is_err()
    );
    drop(partition);
    let permit = host.prepare_directory(partition_plan).await.unwrap();
    let partition = permit
        .open(network.wal.clone(), &budget(), None)
        .unwrap()
        .into_replica();
    let (state, installed) = views(&partition);
    let signed = sign().await.unwrap().sign(&network.credentials).unwrap();
    assert!(
        initial
            .create_session(&state, &installed, &witness, signed, now, &memory)
            .unwrap()
            .is_none()
    );
    assert_eq!(postcard::to_stdvec(create.command()).unwrap(), serialized);
    session.checkpoint().unwrap();
    drop(session);
    let consensus = DurableNode::open_on_wal(
        NodeConfig::single(id.node, id.cluster, id.ledger.session.0),
        network.wal.clone(),
    )
    .unwrap();
    let session = Session::from_node(id.ledger, consensus, SessionLimits::default()).unwrap();
    let recovered = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &settings,
        session.memory_stats().limit as u64,
        &memory,
    )
    .unwrap();
    assert_eq!(recovered.operation(), operation);
    assert_eq!(recovered.client(), client);
    assert_eq!(recovered.founder().root, id.root);
    assert_eq!(recovered.founder().issuer, id.issuer);
    assert_eq!(session.read_at_least(SessionSeq(0)).unwrap(), &application);
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop((
        host,
        outgoing,
        observation,
        refreshed,
        plan,
        recovered,
        initial,
        enroll,
        create,
    ));
    assert_eq!(memory.stats().used, 0);
}

#[tokio::test]
async fn registration_refuses_stronger_policy_foreign_identity_and_expired_node_grants() {
    let directory = tempfile::tempdir().unwrap();
    let (network, settings, session, _) = setup(directory.path()).await;
    let memory = budget();
    let required = session.memory_stats().limit as u64;
    assert!(
        FirstSessionPlan::capture(
            &session,
            &network.state.genesis,
            &settings,
            required - 1,
            &memory
        )
        .is_err()
    );
    let mut foreign = network.state.genesis.clone();
    foreign.founder.issuer = ParticipantId::from_u128(999);
    assert!(FirstSessionPlan::capture(&session, &foreign, &settings, required, &memory).is_err());
    assert_eq!(memory.stats().used, 0);
    // A session the node hosts alone at another ledger registers under the
    // node's identity at that ledger, with its own operation identity; the
    // founder's plan refuses it, as does a host from another cluster, a
    // zero node or a ledger the facts do not describe.
    let created = LedgerId {
        tenant: TenantId::from_u128(9),
        session: SessionId::from_u128(77),
    };
    let mut facts = HostedSessionFacts::from_session(&session).unwrap();
    facts.ledger = created;
    let mut host = network.state.genesis.founder.clone();
    host.ledger = created;
    let hosted = FirstSessionPlan::capture_hosted(
        &facts,
        &network.state.genesis,
        &host,
        &settings,
        required,
        &memory,
    )
    .unwrap();
    assert_eq!(hosted.ledger(), created);
    assert_eq!(hosted.founder(), &host);
    let founder_plan = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &settings,
        required,
        &memory,
    )
    .unwrap();
    assert_ne!(hosted.operation(), founder_plan.operation());
    assert_ne!(hosted.client(), founder_plan.client());
    assert!(
        FirstSessionPlan::capture_facts(
            &facts,
            &network.state.genesis,
            &settings,
            required,
            &memory
        )
        .is_err()
    );
    let mut elsewhere = host.clone();
    elsewhere.cluster = [200; 16];
    assert!(
        FirstSessionPlan::capture_hosted(
            &facts,
            &network.state.genesis,
            &elsewhere,
            &settings,
            required,
            &memory
        )
        .is_err()
    );
    let mut nobody = host.clone();
    nobody.node = 0;
    assert!(
        FirstSessionPlan::capture_hosted(
            &facts,
            &network.state.genesis,
            &nobody,
            &settings,
            required,
            &memory
        )
        .is_err()
    );
    let mut other = host.clone();
    other.ledger.session = SessionId::from_u128(78);
    assert!(
        FirstSessionPlan::capture_hosted(
            &facts,
            &network.state.genesis,
            &other,
            &settings,
            required,
            &memory
        )
        .is_err()
    );
    drop(hosted);
    drop(founder_plan);
    assert_eq!(memory.stats().used, 0);
    let plan = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &settings,
        required,
        &memory,
    )
    .unwrap();
    let mut stronger = settings.clone();
    stronger.durability.max_failures = 1;
    let stronger = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &stronger,
        required,
        &memory,
    )
    .unwrap();
    let mut residency = settings.clone();
    residency
        .placement
        .residency
        .push("not-yet-registered".into());
    let residency = FirstSessionPlan::capture(
        &session,
        &network.state.genesis,
        &residency,
        required,
        &memory,
    )
    .unwrap();
    let (host, owner, outgoing) = ControlHost::spawn(
        network.control,
        NoDirectoryAuthority,
        ControlHostConfig::new(network.state.genesis.root_namespace),
        budget(),
    )
    .unwrap();
    let observation = host.observe_root().await.unwrap();
    let now = unix_time().unwrap();
    assert!(matches!(
        stronger.prepare(&observation, now, &memory),
        Err(SessionRegistrationError::PolicyUnsatisfied)
    ));
    assert!(matches!(
        residency.prepare(&observation, now, &memory),
        Err(SessionRegistrationError::NotReady)
    ));
    let expiry = observation.authority().unwrap().nodes[&plan.founder().node].expires_at;
    assert!(plan.prepare(&observation, expiry, &memory).is_err());
    let pressure = MemoryBudget::new(1024, 512).unwrap();
    assert!(matches!(
        plan.prepare(&observation, now, &pressure),
        Err(SessionRegistrationError::Capacity)
    ));
    assert_eq!(pressure.stats().used, 0);
    let ControlBootstrap::Root { enrollment, .. } = &observation.snapshot().state else {
        panic!("root observation");
    };
    let registry = EnrollmentRegistry::restore(
        enrollment,
        plan.founder().cluster,
        EnrollmentLimits::default(),
    )
    .unwrap();
    let revoke = registry
        .prepare_revoke(network.receipt.invitation, now)
        .unwrap();
    host.submit(
        peer(
            network.state.genesis.root_namespace,
            ParticipantId(plan.client()),
        ),
        ControlRequest {
            id: ControlRequestId {
                client: plan.client(),
                sequence: 1,
            },
            acknowledged_through: 0,
            command: ControlCommand::Enrollment(revoke),
        },
    )
    .await
    .unwrap();
    let revoked = host.observe_root().await.unwrap();
    assert!(plan.prepare(&revoked, now, &memory).is_err());
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop((
        host,
        outgoing,
        observation,
        revoked,
        plan,
        stronger,
        residency,
    ));
    assert_eq!(memory.stats().used, 0);
}

#[test]
fn policy_labels_require_one_actual_registered_nonzero_region() {
    let mut root = RootCheckpoint {
        schema: 1,
        cluster: ClusterId([1; 16]),
        revision: 1,
        regions: BTreeMap::new(),
        delegations: BTreeMap::new(),
    };
    let labels = vec!["west".into()];
    assert!(matches!(
        resolve_regions(&labels, &root),
        Err(SessionRegistrationError::NotReady)
    ));
    root.regions.insert(
        RegionId::from_u128(2),
        RegionRecord {
            id: RegionId::from_u128(2),
            label: "west".into(),
            authority_epoch: 1,
        },
    );
    assert_eq!(
        resolve_regions(&labels, &root).unwrap(),
        BTreeSet::from([RegionId::from_u128(2)])
    );
    root.regions.insert(
        RegionId::from_u128(3),
        RegionRecord {
            id: RegionId::from_u128(3),
            label: "west".into(),
            authority_epoch: 1,
        },
    );
    assert!(matches!(
        resolve_regions(&labels, &root),
        Err(SessionRegistrationError::PolicyUnsatisfied)
    ));
    root.regions.clear();
    root.regions.insert(
        RegionId::UNKNOWN,
        RegionRecord {
            id: RegionId::UNKNOWN,
            label: "west".into(),
            authority_epoch: 1,
        },
    );
    assert!(matches!(
        resolve_regions(&labels, &root),
        Err(SessionRegistrationError::PolicyUnsatisfied)
    ));
}
