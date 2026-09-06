#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Invitation release and QUIC redemption backed by the actual shared node WAL.
use focal_enrollment::*;
use focal_model::RequestId;
use focal_node::{cluster::*, config::Settings, embedded::EmbeddedNode};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn open_node(root: &std::path::Path) -> EmbeddedNode {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.to_owned());
    EmbeddedNode::open(&settings).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_metadata_commits_before_invite_and_join_release_and_recovers_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let node = open_node(directory.path());
    let founder = node.identity.node;
    let cluster = node.identity.cluster;
    let mut root = RootEnrollment::for_node(&node, now()).unwrap();
    let server = Arc::new(
        EnrollmentServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            &root.server_identity(),
            TransportLimits::default(),
        )
        .unwrap(),
    );
    let endpoint = server.local_addr().unwrap();
    let intent = InviteIntent {
        endpoint: endpoint.to_string(),
        role: EnrollmentRole::Node,
        lifetime_seconds: 600,
    };
    let invitation = root
        .invite(RequestId::from_u128(1), &intent, now())
        .unwrap();
    let original = invitation.expose_token().unwrap();
    assert_eq!(node.session.sequence().0, 0); // metadata has independent ordering
    drop(root); // no checkpoint: recover committed root decision from same WAL
    let mut root = RootEnrollment::for_node(&node, now()).unwrap();
    let replayed = root
        .invite(RequestId::from_u128(1), &intent, now() + 1)
        .unwrap();
    assert!(replayed.expose_token().unwrap() == original);
    let mut conflict = intent.clone();
    conflict.role = EnrollmentRole::Client;
    assert!(matches!(
        root.invite(RequestId::from_u128(1), &conflict, now()),
        Err(ClusterError::IntentConflict)
    ));
    let (host, owner) = EnrollmentHost::spawn(root).unwrap();
    let serving = server.clone();
    let handler = host.clone();
    let task = tokio::spawn(async move { serving.serve(handler).await });
    let key_directory = tempfile::tempdir().unwrap();
    let key = JoinKey::open_or_create(key_directory.path().join("credentials"), cluster).unwrap();
    let client =
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()).unwrap();
    let receipt = client
        .redeem(endpoint, &invitation, &key, now())
        .await
        .unwrap();
    assert_eq!(receipt.identity.node_id, Some(founder + 1));
    assert_eq!(receipt.identity.cluster, cluster);
    let material = key
        .complete(&receipt, &invitation.trust().ca_certificate, now())
        .unwrap();
    assert_eq!(material.certificate_chain()[0], receipt.certificate);
    assert_eq!(
        client
            .redeem(endpoint, &invitation, &key, now())
            .await
            .unwrap(),
        receipt
    );
    server.close();
    task.await.unwrap().unwrap();
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop(host);
    drop(node);
    let node = open_node(directory.path());
    let mut root = RootEnrollment::for_node(&node, now()).unwrap();
    assert_eq!(
        root.registry().unwrap().enrollments().next(),
        Some(&receipt)
    );
    assert!(
        root.invite(RequestId::from_u128(1), &intent, now())
            .unwrap()
            .expose_token()
            .unwrap()
            == original
    );
    root.revoke(invitation.id(), now()).unwrap();
    let revision = root.registry().unwrap().revision();
    root.revoke(invitation.id(), now()).unwrap();
    assert_eq!(root.registry().unwrap().revision(), revision);
    assert!(matches!(
        root.invite(RequestId::from_u128(1), &intent, now()),
        Err(ClusterError::Enrollment(EnrollmentError::Revoked))
    ));
    root.checkpoint().unwrap();
}
