use super::*;
use crate::{
    cluster_admin::ClusterAdmin,
    network_join::{ClientInvitation, NodeInvitation, PendingClientJoin},
};
use focal_enrollment::{EnrollmentClient, EnrollmentRole, TransportLimits};

#[tokio::test]
async fn client_invitation_enrolls_only_actor_and_preserves_key_across_restart_and_loss() {
    let founder_disk = tempfile::tempdir().unwrap();
    let client_disk = tempfile::tempdir().unwrap();
    for path in [founder_disk.path(), client_disk.path()] {
        crate::set_test_mode(path, 0o700);
    }
    let settings = settings(founder_disk.path());
    let founder = Running::start(&settings).await;
    let admin = ClusterAdmin::open(&settings).unwrap();
    let file = client_disk.path().join("actor.invite");
    admin.invite_client("reviewer", &file).await.unwrap();
    let original = std::fs::read(&file).unwrap();
    admin.invite_client("reviewer", &file).await.unwrap();
    assert_eq!(std::fs::read(&file).unwrap(), original);
    assert!(NodeInvitation::decode(&original).is_err());
    let bundle = ClientInvitation::load(&file).unwrap();
    let invitation = bundle.invitation().id();
    assert_eq!(bundle.invitation().role(), EnrollmentRole::Client);
    let path = client_disk.path().join("reviewer.join");
    let pending = PendingClientJoin::open(&path, bundle).unwrap();
    let request = pending.request_id();
    let csr = pending.csr().to_vec();
    let client =
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()).unwrap();
    let receipt = pending.redeem(&client, unix_time().unwrap()).await.unwrap();
    assert_eq!(receipt.identity.role, EnrollmentRole::Client);
    assert_eq!(receipt.identity.node_id, None);
    assert_ne!(receipt.identity.principal, [0; 16]);
    let material = pending.credentials(unix_time().unwrap()).unwrap();
    assert_eq!(
        material.certificate_chain().first().unwrap(),
        &receipt.certificate
    );
    drop(pending);
    let resumed = PendingClientJoin::resume(&path).unwrap();
    assert_eq!(resumed.request_id(), request);
    assert_eq!(resumed.csr(), csr);
    assert_eq!(
        resumed.redeem(&client, unix_time().unwrap()).await.unwrap(),
        receipt
    );
    assert!(!client_disk.path().join("IDENTITY").exists());
    let observed = admin
        .read(crate::network_admin::AdminRead::Invitation { id: invitation })
        .await
        .unwrap();
    let focal_client::admin::AdminResult::Invitations { entries, .. } = observed else {
        panic!("invitation projection")
    };
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].role, "client");
    assert_eq!(entries[0].credential.as_ref().unwrap().node, None);
    drop(resumed);
    std::fs::remove_dir_all(&path).unwrap();
    assert!(PendingClientJoin::open(&path, ClientInvitation::load(&file).unwrap()).is_err());
    founder.stop().await;
}
