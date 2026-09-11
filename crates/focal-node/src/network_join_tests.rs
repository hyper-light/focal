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
            identity.cluster,
            vec!["enrollment.focal.test".into()],
            now(),
        )
        .unwrap();
        let key =
            JoinKey::open_or_create(disk.path().join("founder-key"), identity.cluster).unwrap();
        let draft = FoundingEnrollmentDraft::open_or_create(
            disk.path().join("founder"),
            &authority,
            &key,
            identity.node,
            identity.issuer.0,
            EnrollmentLimits::default(),
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
        let draft = self
            .registry
            .prepare_invitation(
                &self.authority,
                InviteOptions {
                    endpoint: address.to_string(),
                    server_name: "enrollment.focal.test".into(),
                    role,
                    expires_at: now() + 600,
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

#[tokio::test]
async fn pinned_enrollment_unknown_reply_restart_installs_exact_node_without_membership() {
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
    let task = tokio::spawn(async move {
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
    });
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
        joined_unix_principal(disk.path(), &assigned, now()).unwrap(),
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
    assert!(joined_unix_principal(disk.path(), &assigned, now()).is_err());
    fs::write(&receipt_path, &original_receipt).unwrap();
    fs::remove_file(&key_path).unwrap();
    assert!(joined_unix_principal(disk.path(), &assigned, now()).is_err());
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
    assert!(local_unix_principal(disk.path(), &assigned, now()).is_err());
    client.close();
    server.close();
    task.await.unwrap();
}
