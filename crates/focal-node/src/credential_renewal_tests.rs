use super::*;
use crate::{
    network_bootstrap::signer_principal,
    network_service::tests::{Running, settings},
    placement_agent::tests::join_peer,
};
use focal_control::{ControlRead, ControlReadResult};
use focal_model::RequestId;
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};
use std::{collections::BTreeSet, time::Duration};

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
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let mut sequence = 1u128;
        loop {
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
                        return;
                    }
                    last = Some(format!("{:?}", snapshot.contacts.records));
                }
                other => last = Some(format!("{other:?}")),
            }
            sequence += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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
    // The founder's identity is the bootstrap authority's own certificate.
    assert_eq!(
        founder.handles.credentials.renew().await,
        Err(RenewalError::Unsupported)
    );
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
    let converged = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let current = match peer.handles.credentials.current().await {
                Ok(current) => current,
                Err(error) => return Err(error),
            };
            if current.renewals == 1 {
                return Ok(current);
            }
            assert_eq!(current.expires_at, before.expires_at);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the restarted host never converged on the committed renewal");
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
