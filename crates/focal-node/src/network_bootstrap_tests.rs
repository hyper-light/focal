use super::*;
use crate::embedded::EmbeddedNode;
use focal_control::{ControlCommand, ControlRequest, ControlRequestId};
use focal_enrollment::EnrollmentError;
use std::{
    fs,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn settings(root: &std::path::Path) -> Settings {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_owned());
    settings.node.advertise = Some("127.0.0.1:7443".into());
    settings
}

#[tokio::test]
async fn local_expansion_keeps_identity_log_genesis_and_credentials_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(directory.path());
    settings.node.advertise = None;
    let local = EmbeddedNode::open(&settings).unwrap();
    let identity = local.identity.clone();
    let local_term = local.session.status().term;
    drop(local);
    settings.node.advertise = Some("127.0.0.1:7443".into());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    assert_eq!(network.directory.identity(), &identity);
    assert_eq!(network.state.genesis.founder, identity);
    let consensus = focal_consensus::DurableNode::open_on_wal(
        focal_consensus::NodeConfig::single(
            identity.node,
            identity.cluster,
            identity.ledger.session.0,
        ),
        network.wal.clone(),
    )
    .unwrap();
    assert_eq!(consensus.status().term, local_term);
    drop(consensus);
    let state = network.state.clone();
    let certificate = network.credentials.certificate_chain().to_vec();
    let budget = network.budget.clone();
    assert!(budget.stats().used >= 256 * 1024);
    assert!(matches!(
        FoundingNetwork::open(&settings).await,
        Err(NetworkError::Node(NodeError::Locked))
    ));
    drop(network);
    assert_eq!(budget.stats().used, 0);
    settings.node.advertise = None;
    let recovered = FoundingNetwork::open(&settings).await.unwrap();
    assert_eq!(recovered.state, state);
    assert_eq!(recovered.credentials.certificate_chain(), certificate);
    drop(recovered);
    assert!(matches!(
        EmbeddedNode::open(&settings),
        Err(NodeError::NetworkRequired)
    ));
}

#[tokio::test]
async fn committed_revocation_cannot_be_undone_by_reopening_the_genesis_draft() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let mut network = FoundingNetwork::open(&settings).await.unwrap();
    let registry = network.control.enrollment().unwrap();
    let founder = registry.enrollments().next().unwrap();
    let command = registry
        .prepare_revoke(founder.invitation, unix_time().unwrap())
        .unwrap();
    let id = ControlRequestId {
        client: [7; 16],
        sequence: 1,
    };
    network
        .control
        .submit(
            ControlRequest {
                id,
                acknowledged_through: 0,
                command: ControlCommand::Enrollment(command),
            },
            &NoDirectoryAuthority,
        )
        .unwrap();
    network.control.drain(&NoDirectoryAuthority).unwrap();
    assert!(network.control.receipt(id).unwrap().is_some());
    drop(network);
    assert!(matches!(
        FoundingNetwork::open(&settings).await,
        Err(NetworkError::Enrollment(EnrollmentError::Revoked))
    ));
}

#[tokio::test]
async fn expansion_rejects_missing_policy_before_creating_network_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(directory.path());
    settings.node.advertise = None;
    let local = EmbeddedNode::open(&settings).unwrap();
    let identity = local.identity.clone();
    drop(local);
    fs::remove_file(directory.path().join("POLICY")).unwrap();
    settings.node.advertise = Some("127.0.0.1:7443".into());
    assert!(matches!(
        FoundingNetwork::open(&settings).await,
        Err(NetworkError::Node(NodeError::Identity))
    ));
    assert!(!directory.path().join("POLICY").exists());
    assert!(!directory.path().join("cluster/network").exists());
    assert_eq!(
        crate::embedded::decode_identity(&directory.path().join("IDENTITY")).unwrap(),
        identity
    );
}

#[tokio::test]
async fn interrupted_manifest_install_reuses_private_intent_and_refuses_missing_keys() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(directory.path());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let state = network.state.clone();
    drop(network);
    // Crash before the final public installation: the private founding intent
    // and committed root still require exact recovery, even without NETWORK.
    fs::remove_file(directory.path().join("NETWORK.initialized")).unwrap();
    fs::remove_file(directory.path().join("NETWORK")).unwrap();
    settings.node.advertise = None;
    assert!(matches!(
        EmbeddedNode::open(&settings),
        Err(NodeError::NetworkRequired)
    ));
    settings.node.advertise = Some(state.advertise.to_string());
    let recovered = FoundingNetwork::open(&settings).await.unwrap();
    assert_eq!(recovered.state, state);
    drop(recovered);
    // A durable descendant must never cause open_or_create to replace a lost key.
    fs::remove_file(directory.path().join("NETWORK.initialized")).unwrap();
    fs::remove_file(directory.path().join("NETWORK")).unwrap();
    let key = directory
        .path()
        .join("cluster/network/node-key/join-key.bin");
    fs::remove_file(&key).unwrap();
    assert!(matches!(
        FoundingNetwork::open(&settings).await,
        Err(NetworkError::MissingCredentials)
    ));
    assert!(!key.exists());
}

#[tokio::test]
async fn manifest_validates_ca_genesis_and_exact_framing_before_installation() {
    let directory = tempfile::tempdir().unwrap();
    let settings = settings(directory.path());
    let network = FoundingNetwork::open(&settings).await.unwrap();
    let mut bad = network.state.clone();
    let other = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        other.path().join("ca"),
        [9; 16],
        vec!["other.internal".into()],
        unix_time().unwrap(),
    )
    .unwrap();
    bad.sponsor.ca_certificate = authority.ca_certificate().to_vec();
    assert!(bad.validate(network.directory.identity()).is_err());
    let mut bad = network.state.clone();
    if let ControlBootstrap::Root { directory, .. } = &mut bad.genesis.bootstrap {
        directory.revision = 1;
    }
    // Even a recomputed digest cannot turn a live root checkpoint into genesis.
    let identity = network.directory.identity();
    bad.genesis.root = bad
        .genesis
        .bootstrap
        .identity(&ControlOptions::new(focal_consensus::NodeConfig::single(
            identity.node,
            identity.cluster,
            bad.genesis.root.group,
        )))
        .unwrap();
    assert!(bad.validate(identity).is_err());
    assert!(bad.install(&network.directory).is_err());
    assert_eq!(
        NetworkState::load(&network.directory).unwrap(),
        Some(network.state.clone())
    );
    let path = directory.path().join("NETWORK");
    let mut payload = postcard::to_stdvec(&network.state).unwrap();
    payload.push(0);
    let mut bytes = b"FCLNET01".to_vec();
    bytes.extend_from_slice(blake3::hash(&payload).as_bytes());
    bytes.extend_from_slice(&payload);
    fs::write(&path, bytes).unwrap();
    assert!(NetworkState::load(&network.directory).is_err());
    fs::remove_file(path).unwrap();
    assert!(matches!(
        NetworkState::load(&network.directory),
        Err(NodeError::Identity)
    ));
}

#[tokio::test]
async fn invalid_guarantees_and_endpoints_do_not_initialize_private_network_state() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(directory.path());
    settings.durability.max_failures = 1;
    assert!(matches!(
        FoundingNetwork::open(&settings).await,
        Err(NetworkError::Placement(_))
    ));
    assert!(!directory.path().join("cluster/network").exists());
    assert!(!directory.path().join("POLICY").exists());
    settings.durability.max_failures = 0;
    for address in [
        "0.0.0.0:7443",
        "224.0.0.1:7443",
        "255.255.255.255:7443",
        "127.0.0.1:0",
    ] {
        settings.node.advertise = Some(address.into());
        assert!(resolve_addresses(&settings).await.is_err(), "{address}");
    }
    settings.node.advertise = Some("127.0.0.1:7443".into());
    settings.node.listen = Some("0.0.0.0:7443".parse().unwrap());
    assert!(resolve_addresses(&settings).await.is_ok());
    settings.node.listen = Some("224.0.0.1:7443".parse().unwrap());
    assert!(resolve_addresses(&settings).await.is_err());
}

#[test]
fn missing_runtime_is_a_typed_error_before_filesystem_or_dns_work() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(&directory.path().join("unused"));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        pin!(FoundingNetwork::open(&settings))
            .as_mut()
            .poll(&mut context),
        Poll::Ready(Err(NetworkError::RuntimeRequired))
    ));
    assert!(!directory.path().join("unused").exists());
    settings.node.advertise = Some("localhost:7443".into());
    assert!(matches!(
        pin!(resolve_addresses(&settings))
            .as_mut()
            .poll(&mut context),
        Poll::Ready(Err(_))
    ));
}

#[test]
fn missing_timer_driver_is_contained_during_endpoint_resolution() {
    let directory = tempfile::tempdir().unwrap();
    let mut settings = settings(directory.path());
    settings.node.advertise = Some("localhost:7443".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(resolve_addresses(&settings)),
        Err(NodeError::Io(_))
    ));
}
