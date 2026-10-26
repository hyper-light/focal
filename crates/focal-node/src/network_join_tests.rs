use super::*;
use crate::{
    embedded::new_identity,
    network_state::{root_group, root_namespace},
};
use focal_control::{ControlBootstrap, ControlOptions};
use focal_directory::{RootConfig, RootDirectory};
use focal_enrollment::{
    BootstrapAuthority, EnrollmentServer, FoundingEnrollmentDraft, InviteOptions, JoinFailure,
    JoinPreparation, JoinResponse, TransportLimits,
};
use focal_memory::MemoryBudget;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
struct Fixture {
    _disk: tempfile::TempDir,
    authority: BootstrapAuthority,
    registry: EnrollmentRegistry,
    genesis: NetworkGenesis,
}
impl Fixture {
    fn new() -> Self {
        let disk = tempfile::tempdir().unwrap();
        let identity = new_identity().unwrap();
        let authority = BootstrapAuthority::open_or_create(
            disk.path().join("authority"),
            &focal_seal::test_keys_beside(disk.path().join("authority")).unwrap(),
            identity.cluster,
            vec!["enrollment.focal.test".into()],
            now(),
        )
        .unwrap();
        let key = JoinKey::open_or_create(
            disk.path().join("founder-key"),
            &focal_seal::test_keys_beside(disk.path().join("founder-key")).unwrap(),
            identity.cluster,
        )
        .unwrap();
        let draft = FoundingEnrollmentDraft::open_or_create(
            disk.path().join("founder"),
            &focal_seal::test_keys_beside(disk.path().join("founder")).unwrap(),
            &authority,
            &key,
            identity.node,
            identity.issuer.0,
            EnrollmentLimits::default(),
            0,
            now(),
        )
        .unwrap();
        let registry = EnrollmentRegistry::restore(
            &draft.registry().checkpoint().unwrap(),
            identity.cluster,
            EnrollmentLimits::default(),
        )
        .unwrap();
        let directory = RootDirectory::new(
            focal_directory::ClusterId(identity.cluster),
            RootConfig::default(),
            MemoryBudget::new(8 * 1024 * 1024, 1024 * 1024).unwrap(),
        )
        .unwrap();
        let bootstrap = ControlBootstrap::root(&directory, &registry).unwrap();
        let options = ControlOptions::new(focal_consensus::NodeConfig::single(
            identity.node,
            identity.cluster,
            root_group(identity.cluster),
        ));
        let genesis = NetworkGenesis {
            root: bootstrap.identity(&options).unwrap(),
            root_namespace: root_namespace(&identity),
            founder: identity,
            bootstrap,
        };
        Self {
            _disk: disk,
            authority,
            registry,
            genesis,
        }
    }
    fn invite(&mut self, address: SocketAddr, role: EnrollmentRole) -> Invitation {
        self.invite_until(address, role, now() + 600)
    }
    fn invite_until(
        &mut self,
        address: SocketAddr,
        role: EnrollmentRole,
        expires_at: i64,
    ) -> Invitation {
        let draft = self
            .registry
            .prepare_invitation(
                &self.authority,
                InviteOptions {
                    endpoint: address.to_string(),
                    server_name: "enrollment.focal.test".into(),
                    role,
                    expires_at,
                },
                now(),
            )
            .unwrap();
        self.registry
            .apply_committed(draft.command(), self.registry.applied_index() + 1)
            .unwrap();
        draft.release(&self.registry).unwrap()
    }
    fn bundle(&mut self) -> NodeInvitation {
        let invitation = self.invite("127.0.0.1:7443".parse().unwrap(), EnrollmentRole::Node);
        NodeInvitation::new("worker-2", self.genesis.clone(), invitation).unwrap()
    }
}
fn settings(path: &Path) -> Settings {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(path.to_owned());
    settings
}
fn addresses() -> (SocketAddr, SocketAddr) {
    (
        "0.0.0.0:7444".parse().unwrap(),
        "127.0.0.1:7444".parse().unwrap(),
    )
}

#[test]
fn invitation_pins_genesis_trust_and_redacts_every_debug_surface() {
    let mut fixture = Fixture::new();
    let bundle = fixture.bundle();
    let bytes = bundle.encode().unwrap();
    let decoded = NodeInvitation::decode(&bytes).unwrap();
    assert_eq!(decoded.encode().unwrap().as_slice(), bytes.as_slice());
    let token = bundle.invitation.expose_token().unwrap();
    assert!(!format!("{bundle:?}").contains(token.as_str()));
    let mut bad = bundle.genesis.clone();
    bad.root_namespace.session = focal_model::SessionId::from_u128(999);
    assert!(NodeInvitation::new("worker", bad, bundle.invitation.clone()).is_err());
    let other = Fixture::new();
    assert!(NodeInvitation::new("worker", other.genesis, bundle.invitation.clone()).is_err());
    let client = fixture.invite("127.0.0.1:7443".parse().unwrap(), EnrollmentRole::Client);
    assert!(NodeInvitation::new("client", fixture.genesis.clone(), client).is_err());
    assert!(NodeInvitation::new("bad\nname", fixture.genesis, bundle.invitation.clone()).is_err());
    let mut trailing = bytes.to_vec();
    trailing.push(0);
    assert!(NodeInvitation::decode(&trailing).is_err());
    assert!(NodeInvitation::decode(&vec![0; MAX_BUNDLE + 1]).is_err());
}
#[test]
#[cfg(unix)]
fn private_output_is_noclobber_and_recovers_both_atomic_install_crash_windows() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let mut fixture = Fixture::new();
    let bundle = fixture.bundle();
    let output = fixture._disk.path().join("invitation");
    bundle.write_new(&output).unwrap();
    let bytes = fs::read(&output).unwrap();
    assert_eq!(
        fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    bundle.write_new(&output).unwrap();
    assert_eq!(fs::read(&output).unwrap(), bytes);
    let other = NodeInvitation::new(
        "another-node",
        bundle.genesis.clone(),
        bundle.invitation.clone(),
    )
    .unwrap();
    assert!(matches!(other.write_new(&output), Err(JoinError::Conflict)));
    assert_eq!(fs::read(&output).unwrap(), bytes);
    let digest = blake3::hash(output.file_name().unwrap().as_encoded_bytes()).to_hex();
    let temporary = output
        .parent()
        .unwrap()
        .join(format!(".focal-invitation-{digest}.pending"));
    fs::hard_link(&output, &temporary).unwrap(); // installed target before temporary unlink
    bundle.write_new(&output).unwrap();
    assert!(!temporary.exists());
    fs::remove_file(&output).unwrap();
    fs::write(&temporary, b"interrupted partial file").unwrap();
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
    bundle.write_new(&output).unwrap();
    assert_eq!(fs::read(&output).unwrap(), bytes);
    fs::set_permissions(&output, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(matches!(
        NodeInvitation::load(&output),
        Err(JoinError::Permissions)
    ));
    fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = output.with_extension("symlink");
    symlink(&output, &alias).unwrap();
    assert!(matches!(
        bundle.write_new(&alias),
        Err(JoinError::Permissions)
    ));
    // A delivered invitation is read through a link and may be group-readable
    // (a mounted secret, 24 §24), never group-writable or world-readable.
    assert_eq!(
        NodeInvitation::load(&alias).unwrap().encode().unwrap(),
        bundle.encode().unwrap()
    );
    fs::set_permissions(&output, fs::Permissions::from_mode(0o440)).unwrap();
    assert!(NodeInvitation::load(&output).is_ok());
    for mode in [0o660, 0o604, 0o444, 0o404] {
        fs::set_permissions(&output, fs::Permissions::from_mode(mode)).unwrap();
        assert!(
            matches!(NodeInvitation::load(&output), Err(JoinError::Permissions)),
            "{mode:o}"
        );
    }
    fs::set_permissions(&output, fs::Permissions::from_mode(0o600)).unwrap();
}
#[test]
fn journal_precedes_network_reuses_key_and_rejects_changed_intent_or_lost_key() {
    let mut fixture = Fixture::new();
    let bundle = fixture.bundle();
    let bytes = bundle.encode().unwrap();
    let disk = tempfile::tempdir().unwrap();
    let settings = settings(disk.path());
    let (listen, advertise) = addresses();
    let pending = PendingJoin::open(&settings, bundle, listen, advertise).unwrap();
    assert!(disk.path().join("JOIN/journal.bin").is_file());
    assert!(disk.path().join("JOIN.initialized").is_file());
    assert!(!disk.path().join("IDENTITY").exists());
    assert!(matches!(
        NodeDirectory::open(&settings),
        Err(NodeError::Locked)
    ));
    let request = pending.request_id();
    let csr = pending.csr().to_vec();
    drop(pending);
    let changed = "127.0.0.1:7445".parse().unwrap();
    assert!(matches!(
        PendingJoin::open(
            &settings,
            NodeInvitation::decode(&bytes).unwrap(),
            listen,
            changed
        ),
        Err(JoinError::Conflict)
    ));
    let recovered = PendingJoin::resume(&settings).unwrap();
    assert_eq!(recovered.request_id(), request);
    assert_eq!(recovered.csr(), csr);
    assert!(recovered.enrollment().unwrap().is_none());
    drop(recovered);
    assert!(matches!(
        JoinedNode::open(&settings, now()),
        Err(JoinError::Pending)
    ));
    fs::remove_dir_all(disk.path().join("JOIN/node-key")).unwrap();
    assert!(PendingJoin::resume(&settings).is_err());
    fs::remove_dir_all(disk.path().join("JOIN")).unwrap();
    assert!(NodeDirectory::open(&settings).is_err());
    assert!(
        PendingJoin::open(
            &settings,
            NodeInvitation::decode(&bytes).unwrap(),
            listen,
            advertise
        )
        .is_err()
    );
}
#[test]
fn existing_local_identity_cannot_be_replaced_by_joining() {
    let mut fixture = Fixture::new();
    let disk = tempfile::tempdir().unwrap();
    let settings = settings(disk.path());
    let original = NodeDirectory::open(&settings).unwrap().identity().clone();
    let (listen, advertise) = addresses();
    assert!(PendingJoin::open(&settings, fixture.bundle(), listen, advertise).is_err());
    assert_eq!(
        NodeDirectory::open(&settings).unwrap().identity(),
        &original
    );
    assert!(!disk.path().join("JOIN").exists());
}

#[test]
fn pinned_enrollment_unknown_reply_restart_installs_exact_node_without_membership() {
    focal_rt::block_on(async move {
        let mut fixture = Fixture::new();
        let server = Arc::new(
            EnrollmentServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                &fixture.authority.server_identity(),
                TransportLimits {
                    timeout: Duration::from_secs(3),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let invitation = fixture.invite(server.local_addr().unwrap(), EnrollmentRole::Node);
        let bundle = NodeInvitation::new("worker-2", fixture.genesis.clone(), invitation).unwrap();
        let founder = fixture.genesis.founder.clone();
        let committed = Arc::new(Mutex::new((
            fixture.authority,
            fixture.registry,
            Vec::new(),
            false,
        )));
        let authority = committed.clone();
        let serving = server.clone();
        let task = focal_rt::spawn(async move {
            serving
                .serve(move |request: focal_enrollment::JoinRequest| {
                    let authority = authority.clone();
                    async move {
                        let mut state = authority.lock().unwrap();
                        let preparation = state.1.prepare_join(&state.0, &request, now()).unwrap();
                        let receipt = match preparation {
                            JoinPreparation::Existing(receipt) => receipt,
                            JoinPreparation::Commit(command) => {
                                let index = state.1.applied_index() + 1;
                                state.2.push(command.encode().unwrap()); // authority test fixture retains exact committed command
                                state.1.apply_committed(&command, index).unwrap();
                                state.1.release(&request, now()).unwrap()
                            }
                        };
                        if !state.3 {
                            state.3 = true;
                            JoinResponse::Rejected(JoinFailure::OutcomeUnknown)
                        } else {
                            JoinResponse::Enrolled(receipt)
                        }
                    }
                })
                .await
                .unwrap()
        })
        .unwrap();
        let disk = tempfile::tempdir().unwrap();
        let settings = settings(disk.path());
        let (listen, advertise) = addresses();
        let pending = PendingJoin::open(&settings, bundle, listen, advertise).unwrap();
        let request = pending.request_id();
        let csr = pending.csr().to_vec();
        let client = EnrollmentClient::bind(
            "127.0.0.1:0".parse().unwrap(),
            TransportLimits {
                timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(matches!(
            pending.redeem(&client, now()).await,
            Err(JoinError::Transport(JoinTransportError::OutcomeUnknown))
        ));
        assert!(!disk.path().join("IDENTITY").exists());
        assert!(pending.enrollment().unwrap().is_none());
        drop(pending);
        let pending = PendingJoin::resume(&settings).unwrap();
        assert_eq!(pending.request_id(), request);
        assert_eq!(pending.csr(), csr);
        let receipt = pending.redeem(&client, now()).await.unwrap();
        assert_eq!(receipt.identity.node_id, Some(founder.node + 1));
        assert_eq!(committed.lock().unwrap().2.len(), 1);
        // A forged identity in an otherwise valid receipt cannot replace the saved
        // signed certificate or install its requested physical node.
        let mut forged = receipt.clone();
        forged.identity.node_id = Some(999);
        assert!(pending.install(forged, now()).is_err());
        assert!(!disk.path().join("IDENTITY").exists());
        // Receipt is durable even if the process stops before physical identity install.
        let joined = JoinedNode::open(&settings, now()).unwrap();
        assert_eq!(joined.receipt, receipt);
        let mut assigned = founder.clone();
        assigned.node = receipt.identity.node_id.unwrap();
        assert_eq!(joined.directory.identity(), &assigned);
        assert_eq!(joined.state.genesis.founder, founder);
        assert_eq!(joined.state.advertise, advertise);
        // A live physical owner keeps its locks; Unix context discovery is a
        // read-only verification of its installed key/receipt, not another owner.
        let key_path = disk.path().join("JOIN/node-key/join-key.bin");
        let receipt_path = disk.path().join("JOIN/node-key/enrollment.bin");
        let original_key = fs::read(&key_path).unwrap();
        let original_receipt = fs::read(&receipt_path).unwrap();
        assert_eq!(
            joined_unix_principal(
                disk.path(),
                Some(&focal_seal::test_keys(disk.path()).unwrap()),
                &assigned,
                now()
            )
            .unwrap(),
            focal_model::ParticipantId(receipt.identity.principal)
        );
        assert_eq!(fs::read(&key_path).unwrap(), original_key);
        assert_eq!(fs::read(&receipt_path).unwrap(), original_receipt);
        let mut forged = receipt.clone();
        forged.identity.principal = [99; 16];
        let mut record = b"FCLKEY01".to_vec();
        record.extend_from_slice(&postcard::to_stdvec(&forged).unwrap());
        let checksum = *blake3::hash(&record).as_bytes();
        record.extend_from_slice(&checksum);
        fs::write(&receipt_path, &record).unwrap();
        assert!(
            joined_unix_principal(
                disk.path(),
                Some(&focal_seal::test_keys(disk.path()).unwrap()),
                &assigned,
                now()
            )
            .is_err()
        );
        fs::write(&receipt_path, &original_receipt).unwrap();
        fs::remove_file(&key_path).unwrap();
        assert!(
            joined_unix_principal(
                disk.path(),
                Some(&focal_seal::test_keys(disk.path()).unwrap()),
                &assigned,
                now()
            )
            .is_err()
        );
        assert!(!key_path.exists());
        fs::write(&key_path, &original_key).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&key_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let request = joined.discovery_request().unwrap();
        assert_eq!(request.ledger, joined.state.genesis.root_namespace);
        let Operation::PeerControl { group, request } = request.operation else {
            panic!("Node-only discovery")
        };
        assert_eq!(group, joined.state.genesis.root.group);
        assert_eq!(
            ControlRpc::decode_read_only(&request, 64).unwrap(),
            ControlRead::State
        );
        assert!(!disk.path().join("wal").exists());
        assert!(!disk.path().join("cluster").exists());
        let contact = joined.contact_request().unwrap();
        assert!(matches!(contact.operation, Operation::NodeContact {
            group, sequence:1, acknowledged_through:0, expected_generation:0, advertise:address, ..
        } if group==joined.state.genesis.root.group && address==advertise));
        drop(joined);
        let joined = JoinedNode::open(&settings, now()).unwrap();
        assert_eq!(joined.receipt, receipt);
        assert_eq!(joined.contact_request().unwrap(), contact);
        drop(joined);
        fs::remove_dir_all(disk.path().join("JOIN")).unwrap();
        fs::remove_file(disk.path().join("JOIN.initialized")).unwrap();
        assert!(
            local_unix_principal(
                disk.path(),
                Some(&focal_seal::test_keys(disk.path()).unwrap()),
                &assigned,
                now()
            )
            .is_err()
        );
        client.close();
        server.close().unwrap();
        task.await.unwrap();
    })
    .unwrap()
}

/// What a scripted sponsor holds: its authority and registry, the joiner's asks, and the joins it
/// committed.
struct Scripted {
    authority: BootstrapAuthority,
    registry: EnrollmentRegistry,
    asks: usize,
    commits: usize,
}
/// A sponsor that answers each join with the next of `answers`, then with the enrollment: the
/// joiner's asks counted, and the receipt committed once, at the first ask, however many follow.
fn scripted_sponsor(
    fixture: Fixture,
    server: Arc<EnrollmentServer>,
    answers: Vec<JoinFailure>,
) -> (Arc<Mutex<Scripted>>, focal_rt::JoinHandle<()>) {
    let state = Arc::new(Mutex::new(Scripted {
        authority: fixture.authority,
        registry: fixture.registry,
        asks: 0,
        commits: 0,
    }));
    let serving = state.clone();
    let task = focal_rt::spawn(async move {
        server
            .serve(move |request: focal_enrollment::JoinRequest| {
                let state = serving.clone();
                let answers = answers.clone();
                async move {
                    let mut guard = state.lock().unwrap();
                    let state = &mut *guard;
                    // A join the registry refuses (an ask that arrives after its invitation
                    // expired) is answered with the refusal, as a sponsor answers it.
                    let prepared =
                        match state
                            .registry
                            .prepare_join(&state.authority, &request, now())
                        {
                            Ok(prepared) => prepared,
                            Err(refused) => {
                                state.asks += 1;
                                return JoinResponse::Rejected(JoinFailure::from(&refused));
                            }
                        };
                    let receipt = match prepared {
                        JoinPreparation::Existing(receipt) => receipt,
                        JoinPreparation::Commit(command) => {
                            let index = state.registry.applied_index() + 1;
                            state.registry.apply_committed(&command, index).unwrap();
                            state.commits += 1;
                            state.registry.release(&request, now()).unwrap()
                        }
                    };
                    let ask = state.asks;
                    state.asks += 1;
                    match answers.get(ask) {
                        Some(failure) => JoinResponse::Rejected(*failure),
                        None => JoinResponse::Enrolled(receipt),
                    }
                }
            })
            .await
            .unwrap()
    })
    .unwrap();
    (state, task)
}

/// A join whose sponsor's group cannot decide (between leaders), has no room, or cannot say what
/// came of the ask is asked again, the same join, until the sponsor answers: the joiner is
/// enrolled, committed once at the first ask. Before, the joiner stopped at the first such answer,
/// and a node joining while the root group elected under a loaded disk exited.
#[test]
fn a_join_asks_again_while_its_sponsor_cannot_decide_and_is_enrolled_once() {
    focal_rt::block_on(async move {
        let mut fixture = Fixture::new();
        let server = Arc::new(
            EnrollmentServer::bind(
                "127.0.0.1:0".parse().unwrap(),
                &fixture.authority.server_identity(),
                TransportLimits::default(),
            )
            .unwrap(),
        );
        let invitation = fixture.invite(server.local_addr().unwrap(), EnrollmentRole::Node);
        let bundle = NodeInvitation::new("worker-2", fixture.genesis.clone(), invitation).unwrap();
        let answers = vec![
            JoinFailure::Unavailable,
            JoinFailure::Capacity,
            JoinFailure::OutcomeUnknown,
        ];
        let (state, task) = scripted_sponsor(fixture, server.clone(), answers);
        let disk = tempfile::tempdir().unwrap();
        let settings = settings(disk.path());
        let (listen, advertise) = addresses();
        let pending = PendingJoin::open(&settings, bundle, listen, advertise).unwrap();
        let client =
            EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default())
                .unwrap();
        let receipt = pending.redeem_settled(&client).await.unwrap();
        assert!(receipt.identity.node_id.is_some());
        {
            let state = state.lock().unwrap();
            assert_eq!(
                state.asks, 4,
                "three answers that said to ask again, then the enrollment"
            );
            assert_eq!(state.commits, 1, "the join was committed once");
        }
        client.close();
        server.close().unwrap();
        task.await.unwrap();
    })
    .unwrap()
}

/// A join asked again stops where asking cannot succeed: at once at a terminal refusal, which no ask
/// changes, after any number of asks the sponsor answered unavailable; and at the invitation's
/// expiry, after which the founder admits no new join, with the sponsor's last answer. The asks are
/// the facts the first two cases judge, under an invitation that outlives them; the third judges
/// only that the join ends at the expiry, for how many asks fit before it is the machine's: under
/// load even the first can reach the sponsor after a two-second invitation expired, and is refused
/// so (`JoinFailure::Expired`).
#[test]
fn a_join_stops_asking_at_its_invitations_expiry_and_at_a_terminal_refusal() {
    use JoinFailure::{Expired, Revoked, Unavailable};
    focal_rt::block_on(async move {
        // The sponsor's answers in turn, and the invitation's lifetime in seconds.
        let cases = [
            (vec![Unavailable, Unavailable, Revoked], 600),
            (vec![Revoked], 600),
            (vec![Unavailable; 4096], 2),
        ];
        for (answers, lifetime) in cases {
            let mut fixture = Fixture::new();
            let server = Arc::new(
                EnrollmentServer::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    &fixture.authority.server_identity(),
                    TransportLimits::default(),
                )
                .unwrap(),
            );
            let expires = now() + lifetime;
            let invitation =
                fixture.invite_until(server.local_addr().unwrap(), EnrollmentRole::Node, expires);
            let bundle =
                NodeInvitation::new("worker-2", fixture.genesis.clone(), invitation).unwrap();
            let (state, task) = scripted_sponsor(fixture, server.clone(), answers.clone());
            let disk = tempfile::tempdir().unwrap();
            let settings = settings(disk.path());
            let (listen, advertise) = addresses();
            let pending = PendingJoin::open(&settings, bundle, listen, advertise).unwrap();
            let client =
                EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default())
                    .unwrap();
            let ended = pending.redeem_settled(&client).await;
            let refused = |failure: JoinFailure| {
                matches!(&ended, Err(JoinError::Transport(JoinTransportError::Rejected(answer))) if *answer == failure)
            };
            let asks = state.lock().unwrap().asks;
            if answers.contains(&Revoked) {
                assert!(refused(Revoked), "{ended:?}");
                assert_eq!(
                    asks,
                    answers.len(),
                    "asked again while the sponsor was unavailable, and not after its refusal"
                );
            } else {
                // Its last answer, or the expiry itself where its last ask reached the sponsor
                // after it.
                assert!(refused(Unavailable) || refused(Expired), "{ended:?}");
                assert!(now() >= expires, "the join stopped before its invitation expired");
            }
            client.close();
            server.close().unwrap();
            task.await.unwrap();
        }
    })
    .unwrap()
}

/// An operator who advertises a `host:port` name has that name carried into
/// the join and committed with the first contact (24 §24): peers re-resolve
/// it when the address behind it stops answering, which is how a rescheduled
/// host stays reachable. Before this, the join dropped the name and every
/// host's committed contact was address-only, so a host that moved was
/// unreachable until declared dead. The name comes from the operator's
/// configuration on open and on resume alike; an address literal names
/// nothing to re-resolve and leaves it unset.
#[test]
fn a_join_carries_the_advertised_name_so_peers_can_re_resolve_a_moved_host() {
    let mut fixture = Fixture::new();
    let (listen, advertise) = addresses();
    let disk = tempfile::tempdir().unwrap();
    let mut named = settings(disk.path());
    named.node.listen = Some(listen);
    named.node.advertise = Some("host-b.example:7444".into());
    let pending = PendingJoin::open(&named, fixture.bundle(), listen, advertise).unwrap();
    assert_eq!(pending.endpoint.as_deref(), Some("host-b.example:7444"));
    drop(pending);
    let resumed = PendingJoin::resume(&named).unwrap();
    assert_eq!(
        resumed.endpoint.as_deref(),
        Some("host-b.example:7444"),
        "a resumed join derives the name from the same configuration"
    );
    drop(resumed);
    let disk = tempfile::tempdir().unwrap();
    let mut addressed = settings(disk.path());
    addressed.node.listen = Some(listen);
    addressed.node.advertise = Some(advertise.to_string());
    let pending = PendingJoin::open(&addressed, fixture.bundle(), listen, advertise).unwrap();
    assert!(
        pending.endpoint.is_none(),
        "an address literal names nothing to re-resolve"
    );
}

/// A client adopts an endorsed issuer it did not hold (24 §11): recorded
/// beside its journal, once per issuer, the newest within the bound a
/// verifier holds, and read back only when the file is what it wrote.
#[test]
fn a_client_adopts_the_endorsed_issuers_it_did_not_hold_within_the_bound_and_refuses_a_damaged_record()
 {
    let dir = tempfile::tempdir().unwrap();
    let directory = dir.path().join("enrollment-alice");
    std::fs::create_dir_all(&directory).unwrap();
    // The data directory's keys, which its adoptions are sealed under from its at-rest level; with
    // no fence recorded, written in the clear, as every earlier binary wrote them (29 §12.10).
    let keys = focal_seal::test_keys(dir.path()).unwrap();
    assert!(adopted_issuers(&keys, &directory).unwrap().is_empty());
    let authority_at = |name: &str| {
        focal_enrollment::BootstrapAuthority::open_or_create(
            dir.path().join(name),
            &focal_seal::test_keys_beside(dir.path().join(name)).unwrap(),
            [9; 16],
            vec!["localhost".into()],
            1_783_000_000,
        )
        .unwrap()
    };
    let first = authority_at("first");
    let adopted = focal_wire::Adopted {
        anchor: first.ca_certificate().to_vec(),
        issuer: None,
    };
    assert!(adopt_issuer(&keys, &directory, &adopted).unwrap());
    assert!(!adopt_issuer(&keys, &directory, &adopted).unwrap());
    let recorded = adopted_issuers(&keys, &directory).unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0], first.issuer_record().unwrap());
    // The bound: the oldest adoption leaves for the newest.
    let mut fingerprints = vec![recorded[0].fingerprint];
    for name in ["second", "third", "fourth", "fifth"] {
        let authority = authority_at(name);
        assert!(
            adopt_issuer(
                &keys,
                &directory,
                &focal_wire::Adopted {
                    anchor: authority.ca_certificate().to_vec(),
                    issuer: None,
                },
            )
            .unwrap()
        );
        fingerprints.push(authority.issuer_record().unwrap().fingerprint);
    }
    let recorded = adopted_issuers(&keys, &directory).unwrap();
    assert_eq!(recorded.len(), focal_wire::MAX_TRUST_ROOTS);
    assert_eq!(
        recorded
            .iter()
            .map(|issuer| issuer.fingerprint)
            .collect::<Vec<_>>(),
        fingerprints[1..].to_vec()
    );
    // A damaged record is refused, never trusted.
    let path = directory.join("trust-adopted.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        adopted_issuers(&keys, &directory),
        Err(JoinError::Invalid)
    ));
}
