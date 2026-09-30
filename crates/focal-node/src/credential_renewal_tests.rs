use crate::{
    config::ConfigError,
    embedded::NodeError,
    network_bootstrap::{NetworkError, signer_principal, unix_time},
    network_service::{
        ServiceError,
        tests::{Running, TestSettings, settings, try_until, until},
    },
    placement_agent::tests::join_peer,
};
use focal_control::{ControlRead, ControlReadResult};
use focal_enrollment::{EnrollmentLimits, EnrollmentRegistry};
use focal_model::RequestId;
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole, certificate_fingerprint};
use std::{collections::BTreeSet, time::Duration};

/// The node id of the founder whose data directory is `dir`.
fn founder_node(dir: &std::path::Path) -> u64 {
    crate::embedded::decode_identity(&dir.join("IDENTITY"))
        .unwrap()
        .node
}
/// The founder's receipt file: the credential its key holds.
fn founder_receipt_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("cluster")
        .join("network")
        .join("node-key")
        .join("enrollment.bin")
}
/// The enrollment registry as the root has committed it.
async fn root_registry(founder: &Running, founder_dir: &std::path::Path) -> EnrollmentRegistry {
    let cluster = crate::embedded::decode_identity(&founder_dir.join("IDENTITY"))
        .unwrap()
        .cluster;
    let observation = founder.handles.control.observe_root().await.unwrap();
    let focal_control::ControlBootstrap::Root { enrollment, .. } = &observation.snapshot().state
    else {
        panic!("the root's state is its enrollment registry")
    };
    EnrollmentRegistry::restore(enrollment, cluster, EnrollmentLimits::default()).unwrap()
}

/// The founder's own runtime reads its root state.
fn root_reader(founder: &Running, founder_dir: &std::path::Path) -> AuthenticatedPeer {
    let identity = crate::embedded::decode_identity(&founder_dir.join("IDENTITY")).unwrap();
    AuthenticatedPeer::local(PeerGrant {
        principal: signer_principal(identity.cluster),
        tenants: BTreeSet::from([founder.status.ledger.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}

async fn wait_for_contact(
    founder: &Running,
    founder_dir: &std::path::Path,
    node: u64,
    fingerprint: [u8; 32],
) {
    let mut last = None;
    let mut sequence = 0u128;
    let result = try_until(&[founder], Duration::from_secs(30), async || {
        sequence += 1;
        match founder
            .handles
            .control
            .read(
                root_reader(founder, founder_dir),
                RequestId::from_u128(sequence),
                ControlRead::Contacts,
            )
            .await
        {
            Ok(ControlReadResult::Contacts(snapshot)) => {
                if snapshot.contacts.records.iter().any(|contact| {
                    contact.node == node && contact.certificate_fingerprint == fingerprint
                }) {
                    return Some(());
                }
                last = Some(format!("{:?}", snapshot.contacts.records));
            }
            other => last = Some(format!("{other:?}")),
        }
        None
    })
    .await;
    assert!(
        result.is_ok(),
        "the renewed certificate was never announced to the root: {last:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_host_renews_its_credential_presents_it_everywhere_and_converges_after_a_crash() {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let peer_settings = settings(peer_dir.path());
    let founder = Running::start(&founder_settings).await;
    let (peer, node) = join_peer(&founder, founder_dir.path(), "host", &peer_settings).await;
    let before = peer.handles.credentials.current().await.unwrap();
    assert_eq!(before.node, node);
    assert_eq!(before.renewals, 0);
    let receipt_path = peer_dir
        .path()
        .join("JOIN")
        .join("node-key")
        .join("enrollment.bin");
    let held = std::fs::read(&receipt_path).unwrap();
    // A renewal must extend the credential beyond the second it was issued.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // The controller asks the sponsor under the credential it holds, installs
    // the renewal under the same key and presents it at once.
    let renewed = peer.handles.credentials.renew().await.unwrap();
    assert_eq!(renewed.node, node);
    assert_eq!(renewed.principal, before.principal);
    assert!(renewed.expires_at > before.expires_at);
    assert!(renewed.issued_at >= before.issued_at);
    assert_ne!(
        renewed.certificate_fingerprint,
        before.certificate_fingerprint
    );
    assert_eq!(renewed.renewals, 1);
    assert_ne!(std::fs::read(&receipt_path).unwrap(), held);
    // Give the controller a loop to announce; if it stopped, say why.
    tokio::time::sleep(Duration::from_millis(600)).await;
    match peer.handles.credentials.current().await {
        Ok(current) => assert_eq!(current, renewed),
        Err(error) => panic!(
            "the host's controller stopped after renewing ({error}): {:?}",
            peer.outcome().await
        ),
    }
    // The peer announces its renewed certificate to the root: the founder
    // authorizes the new connection and records the fingerprint it presented.
    wait_for_contact(
        &founder,
        founder_dir.path(),
        node,
        renewed.certificate_fingerprint,
    )
    .await;
    // The placement agent signs with the renewed credential from now on and
    // keeps its state.
    let status = peer.handles.placement.status().await.unwrap();
    assert_eq!(status.node, node);
    // Crash window: the sponsor committed the renewal but the holder still
    // has the previous receipt on disk. A retry is answered with the committed
    // renewal, never a second issuance, and the holder converges on it.
    peer.stop().await;
    std::fs::write(&receipt_path, &held).unwrap();
    let peer = Running::start(&peer_settings).await;
    // The controller sees the registry ahead of the receipt it holds and
    // converges on the committed renewal by itself.
    let converged = until(
        "the restarted host converges on the committed renewal",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || {
            let current = match peer.handles.credentials.current().await {
                Ok(current) => current,
                Err(error) => return Some(Err(error)),
            };
            if current.renewals == 1 {
                return Some(Ok(current));
            }
            assert_eq!(current.expires_at, before.expires_at);
            None
        },
    )
    .await;
    let converged = match converged {
        Ok(converged) => converged,
        Err(error) => panic!(
            "the restarted host's controller stopped ({error}): {:?}",
            peer.outcome().await
        ),
    };
    assert_eq!(converged.expires_at, renewed.expires_at);
    assert_eq!(
        converged.certificate_fingerprint,
        renewed.certificate_fingerprint
    );
    // A later renewal under the renewed credential commits again.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let again = peer.handles.credentials.renew().await.unwrap();
    assert!(again.expires_at > converged.expires_at);
    assert_eq!(again.renewals, 2);
    wait_for_contact(
        &founder,
        founder_dir.path(),
        node,
        again.certificate_fingerprint,
    )
    .await;
    peer.stop().await;
    founder.stop().await;
}

/// The identity the root's grant names for `node`, once it names it.
async fn granted_identity(founder: &Running, node: u64) -> Option<[u8; 32]> {
    let observation = founder.handles.control.observe_root().await.ok()?;
    let authority = observation.authority()?;
    authority
        .nodes
        .get(&node)
        .map(|grant| grant.enrollment.identity.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_joined_host_rotates_its_key_is_regranted_under_it_and_adopts_a_committed_rotation_after_a_crash()
 {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let peer_settings = settings(peer_dir.path());
    let founder = Running::start(&founder_settings).await;
    let (peer, node) = join_peer(&founder, founder_dir.path(), "host", &peer_settings).await;
    let before = peer.handles.credentials.current().await.unwrap();
    assert_eq!(before.rotations, 0);
    // The root granted the node under the key it enrolled with.
    let granted = until(
        "the node is granted",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || granted_identity(&founder, node).await,
    )
    .await;
    assert_eq!(granted, before.key_identity);
    let key_dir = peer_dir.path().join("JOIN").join("node-key");
    let held_key = std::fs::read(key_dir.join("join-key.bin")).unwrap();
    let held_receipt = std::fs::read(key_dir.join("enrollment.bin")).unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // The rotation: a new key under the same identity and principal,
    // presented at once and announced to the root.
    let rotated = peer.handles.credentials.rotate().await.unwrap();
    assert_eq!(rotated.node, node);
    assert_eq!(rotated.principal, before.principal);
    assert_ne!(rotated.key_identity, before.key_identity);
    assert_ne!(
        rotated.certificate_fingerprint,
        before.certificate_fingerprint
    );
    assert_eq!(rotated.rotations, 1);
    assert_eq!(rotated.renewals, 0);
    let adopted_key = std::fs::read(key_dir.join("join-key.bin")).unwrap();
    assert_ne!(adopted_key, held_key);
    assert!(
        !peer_dir
            .path()
            .join("JOIN")
            .join("node-key.next")
            .join("join-key.bin")
            .exists(),
        "the staged key is cleared once adopted"
    );
    wait_for_contact(
        &founder,
        founder_dir.path(),
        node,
        rotated.certificate_fingerprint,
    )
    .await;
    // The root re-grants the node under its new key.
    until(
        "the node is re-granted under its rotated key",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || {
            (granted_identity(&founder, node).await == Some(rotated.key_identity)).then_some(())
        },
    )
    .await;
    // Under the rotated key a renewal is an ordinary renewal.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let renewed = peer.handles.credentials.renew().await.unwrap();
    assert_eq!(renewed.key_identity, rotated.key_identity);
    assert_eq!(renewed.renewals, 1);
    assert_eq!(renewed.rotations, 1);
    // Crash window: the sponsor committed a rotation but the holder still
    // has the previous key and receipt, with the new key staged. The
    // restarted host adopts the committed rotation by itself.
    peer.stop().await;
    let staged = peer_dir.path().join("JOIN").join("node-key.next");
    std::fs::create_dir_all(&staged).unwrap();
    crate::set_test_mode(&staged, 0o700);
    for name in ["join-key.bin", "join-key.bin.initialized"] {
        let source = key_dir.join(name);
        if source.exists() {
            std::fs::copy(&source, staged.join(name)).unwrap();
        }
    }
    std::fs::write(key_dir.join("join-key.bin"), &held_key).unwrap();
    std::fs::write(key_dir.join("enrollment.bin"), &held_receipt).unwrap();
    let peer = Running::start(&peer_settings).await;
    let converged = until(
        "the restarted host adopts the committed rotation",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || match peer.handles.credentials.current().await {
            Ok(current) if current.rotations == 1 => Some(Ok(current)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        },
    )
    .await;
    let converged = match converged {
        Ok(converged) => converged,
        Err(error) => panic!(
            "the restarted host's controller stopped ({error}): {:?}",
            peer.outcome().await
        ),
    };
    assert_eq!(converged.key_identity, rotated.key_identity);
    assert_eq!(
        converged.certificate_fingerprint,
        renewed.certificate_fingerprint
    );
    peer.stop().await;
    founder.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_founder_renews_and_rotates_its_own_credential_and_restarts_on_what_it_holds() {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let founder_settings = settings(founder_dir.path());
    let peer_settings = settings(peer_dir.path());
    let founder = Running::start(&founder_settings).await;
    let founder_id = founder_node(founder_dir.path());
    let (peer, node) = join_peer(&founder, founder_dir.path(), "host", &peer_settings).await;
    let before = founder.handles.credentials.current().await.unwrap();
    assert_eq!(before.node, founder_id);
    assert_eq!(before.renewals, 0);
    assert_eq!(before.rotations, 0);
    let receipt_path = founder_receipt_path(founder_dir.path());
    let held = std::fs::read(&receipt_path).unwrap();
    let genesis = root_registry(&founder, founder_dir.path())
        .await
        .enrollments()
        .find(|listed| listed.identity.node_id == Some(founder_id))
        .unwrap()
        .clone();
    assert_eq!(genesis.revision, 1);
    assert_eq!(
        certificate_fingerprint(&genesis.certificate),
        before.certificate_fingerprint
    );
    // A renewal must extend the credential beyond the second it was issued.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    // The founder asks the enrollment host it runs, installs the renewal
    // under its genesis key and presents it at once.
    let renewed = founder.handles.credentials.renew().await.unwrap();
    assert_eq!(renewed.node, founder_id);
    assert_eq!(renewed.principal, before.principal);
    assert_eq!(renewed.key_identity, before.key_identity);
    assert!(renewed.expires_at > before.expires_at);
    assert_ne!(
        renewed.certificate_fingerprint,
        before.certificate_fingerprint
    );
    assert_eq!(renewed.renewals, 1);
    assert_ne!(std::fs::read(&receipt_path).unwrap(), held);
    // The registry lists the renewal under the founder's identity, issued
    // under the founding subject at its new revision, with the genesis
    // certificate retiring through the grace.
    let registry = root_registry(&founder, founder_dir.path()).await;
    let listed = registry
        .enrollments()
        .find(|listed| listed.identity.node_id == Some(founder_id))
        .unwrap()
        .clone();
    assert_eq!(listed.identity, genesis.identity);
    assert_eq!(listed.public_key, genesis.public_key);
    assert!(listed.revision > genesis.revision);
    assert_eq!(
        certificate_fingerprint(&listed.certificate),
        renewed.certificate_fingerprint
    );
    let now = unix_time().unwrap();
    assert_eq!(
        registry
            .authorize_certificate(&genesis.certificate, now)
            .unwrap(),
        genesis.identity
    );
    assert!(
        registry
            .retired(now)
            .any(|(retired, _)| retired.certificate == genesis.certificate)
    );
    // It announces the renewed certificate to the root and keeps serving:
    // the joined host renews through it, and a new host enrolls after.
    wait_for_contact(
        &founder,
        founder_dir.path(),
        founder_id,
        renewed.certificate_fingerprint,
    )
    .await;
    let peer_renewed = peer.handles.credentials.renew().await.unwrap();
    assert_eq!(peer_renewed.node, node);
    assert_eq!(peer_renewed.renewals, 1);
    let second_dir = tempfile::tempdir().unwrap();
    let second_settings = settings(second_dir.path());
    let (second, second_node) =
        join_peer(&founder, founder_dir.path(), "second", &second_settings).await;
    assert_ne!(second_node, node);
    second.stop().await;
    // Crash window: the registry committed the renewal but the founder
    // still holds the genesis receipt. It starts on the committed renewal
    // at once, without another issuance.
    founder.stop().await;
    std::fs::write(&receipt_path, &held).unwrap();
    let founder = Running::start(&founder_settings).await;
    let restarted = founder.handles.credentials.current().await.unwrap();
    assert_eq!(
        restarted.certificate_fingerprint,
        renewed.certificate_fingerprint
    );
    assert_eq!(restarted.expires_at, renewed.expires_at);
    assert_eq!(restarted.renewals, 0);
    assert_ne!(std::fs::read(&receipt_path).unwrap(), held);
    // A rotation moves the founder to a fresh key under its identity; the
    // root re-grants it under the new key, and a restart finds the rotated
    // key beside the genesis draft.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let rotated = founder.handles.credentials.rotate().await.unwrap();
    assert_eq!(rotated.principal, before.principal);
    assert_ne!(rotated.key_identity, before.key_identity);
    assert_eq!(rotated.rotations, 1);
    assert!(
        !founder_dir
            .path()
            .join("cluster")
            .join("network")
            .join("node-key.next")
            .join("join-key.bin")
            .exists(),
        "the staged key is cleared once adopted"
    );
    until(
        "the founder is re-granted under its rotated key",
        &[&founder, &peer],
        Duration::from_secs(30),
        async || {
            (granted_identity(&founder, founder_id).await == Some(rotated.key_identity))
                .then_some(())
        },
    )
    .await;
    founder.stop().await;
    let founder = Running::start(&founder_settings).await;
    let restarted = founder.handles.credentials.current().await.unwrap();
    assert_eq!(restarted.key_identity, rotated.key_identity);
    assert_eq!(
        restarted.certificate_fingerprint,
        rotated.certificate_fingerprint
    );
    // Under the rotated key a renewal is an ordinary renewal, carrying the
    // founder's principal, and the joined host still renews through it.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let again = founder.handles.credentials.renew().await.unwrap();
    assert_eq!(again.key_identity, rotated.key_identity);
    assert_eq!(again.principal, before.principal);
    assert!(again.expires_at > rotated.expires_at);
    wait_for_contact(
        &founder,
        founder_dir.path(),
        founder_id,
        again.certificate_fingerprint,
    )
    .await;
    let peer_again = peer.handles.credentials.renew().await.unwrap();
    assert_eq!(peer_again.renewals, 2);
    peer.stop().await;
    founder.stop().await;
}

/// A lifetime short enough for the test to watch a credential renew itself
/// twice: its window is four seconds, so a renewal has that long to commit.
const SHORT_LIFETIME: u64 = 12;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_short_lifetime_renews_every_node_ahead_of_expiry_and_admits_a_late_joiner() {
    let founder_dir = tempfile::tempdir().unwrap();
    let peer_dir = tempfile::tempdir().unwrap();
    let mut founder_settings = settings(founder_dir.path());
    founder_settings.value.node.credential_lifetime_seconds = Some(SHORT_LIFETIME);
    let peer_settings = settings(peer_dir.path());
    let founder = Running::start(&founder_settings).await;
    let founder_id = founder_node(founder_dir.path());
    let genesis_summary = founder.handles.credentials.current().await.unwrap();
    assert_eq!(
        genesis_summary.expires_at - genesis_summary.issued_at,
        SHORT_LIFETIME as i64
    );
    let genesis = root_registry(&founder, founder_dir.path())
        .await
        .enrollments()
        .find(|listed| listed.identity.node_id == Some(founder_id))
        .unwrap()
        .clone();
    assert_eq!(
        certificate_fingerprint(&genesis.certificate),
        genesis_summary.certificate_fingerprint
    );
    // The lifetime is the cluster's: a joined host's credential is issued
    // for it too.
    let (peer, node) = join_peer(&founder, founder_dir.path(), "host", &peer_settings).await;
    let joined = peer.handles.credentials.current().await.unwrap();
    assert_eq!(joined.expires_at - joined.issued_at, SHORT_LIFETIME as i64);
    // Each renews itself in the last third of its lifetime, twice over,
    // without being asked.
    let (founder_renewed, peer_renewed) = until(
        "the founder and the host renew themselves twice",
        &[&founder, &peer],
        Duration::from_secs(60),
        async || {
            let founder = founder.handles.credentials.current().await.ok()?;
            let peer = peer.handles.credentials.current().await.ok()?;
            (founder.renewals >= 2 && peer.renewals >= 2).then_some((founder, peer))
        },
    )
    .await;
    assert_eq!(founder_renewed.node, founder_id);
    assert_eq!(founder_renewed.key_identity, genesis_summary.key_identity);
    assert_eq!(peer_renewed.node, node);
    let now = unix_time().unwrap();
    assert!(
        now >= genesis_summary.expires_at,
        "two renewals span a lifetime"
    );
    assert!(founder_renewed.expires_at > now);
    assert!(peer_renewed.expires_at > now);
    // The genesis credentials have expired: the registry authorizes neither
    // any longer, and lists each node under what it renewed to.
    let registry = root_registry(&founder, founder_dir.path()).await;
    assert!(
        matches!(
            registry.authorize_certificate(&genesis.certificate, now),
            Err(focal_enrollment::EnrollmentError::Expired
                | focal_enrollment::EnrollmentError::Unauthorized)
        ),
        "{:?}",
        registry.authorize_certificate(&genesis.certificate, now)
    );
    for (node, summary) in [(founder_id, &founder_renewed), (node, &peer_renewed)] {
        let listed = registry
            .enrollments()
            .find(|listed| listed.identity.node_id == Some(node))
            .unwrap();
        assert!(listed.expires_at >= summary.expires_at);
        assert_eq!(listed.public_key, summary.key_identity);
    }
    // A new host enrolls after the genesis credentials expired: the founder
    // serves on what it renewed to.
    let late_dir = tempfile::tempdir().unwrap();
    let late_settings = settings(late_dir.path());
    let (late, late_node) = join_peer(&founder, founder_dir.path(), "late", &late_settings).await;
    assert_ne!(late_node, node);
    let late_summary = late.handles.credentials.current().await.unwrap();
    assert_eq!(
        late_summary.expires_at - late_summary.issued_at,
        SHORT_LIFETIME as i64
    );
    late.stop().await;
    peer.stop().await;
    founder.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_credential_lifetime_is_committed_at_genesis_and_a_later_change_is_refused() {
    let founder_dir = tempfile::tempdir().unwrap();
    let mut founder_settings = settings(founder_dir.path());
    founder_settings.value.node.credential_lifetime_seconds = Some(3600);
    let founder = Running::start(&founder_settings).await;
    let genesis = founder.handles.credentials.current().await.unwrap();
    assert_eq!(genesis.expires_at - genesis.issued_at, 3600);
    founder.stop().await;
    // A start that asks for another lifetime is refused by the field's
    // name, like any committed policy; the committed one starts.
    let changed = TestSettings {
        value: {
            let mut value = founder_settings.value.clone();
            value.node.credential_lifetime_seconds = Some(7200);
            value
        },
        socket: founder_settings.socket.try_clone().unwrap(),
    };
    let error = changed.open().await.err().unwrap();
    assert!(
        matches!(
            error,
            ServiceError::Bootstrap(NetworkError::Node(NodeError::Config(
                ConfigError::CommittedPolicyChange {
                    field: "node.credential_lifetime_seconds"
                }
            )))
        ),
        "{error}"
    );
    let founder = Running::start(&founder_settings).await;
    let restarted = founder.handles.credentials.current().await.unwrap();
    assert_eq!(restarted.expires_at, genesis.expires_at);
    assert_eq!(restarted.expires_at - restarted.issued_at, 3600);
    founder.stop().await;
}
