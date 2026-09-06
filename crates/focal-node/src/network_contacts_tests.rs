use super::*;
use crate::cluster::NoDirectoryAuthority;
use focal_consensus::NodeConfig;
use focal_control::*;
use focal_directory::{RootConfig, RootDirectory};
use focal_enrollment::{
    BootstrapAuthority, EnrollmentLimits, EnrollmentRegistry, FoundingEnrollmentDraft, JoinKey,
};
use focal_memory::MemoryBudget;
use focal_model::{
    LedgerId, ParticipantId, RequestEpoch, RequestId, RouteEpoch, SessionId, TenantId,
};
use focal_wire::*;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
const CLUSTER: [u8; 16] = [71; 16];
const GROUP: [u8; 16] = [72; 16];
const PRINCIPAL: [u8; 16] = [73; 16];
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
fn namespace() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(71),
        session: SessionId::from_u128(72),
    }
}
fn budget() -> MemoryBudget {
    MemoryBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap()
}
fn options() -> ControlOptions {
    ControlOptions::new(NodeConfig::single(1, CLUSTER, GROUP))
}
fn packet() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: namespace(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::NodeContact {
            group: GROUP,
            sequence: 1,
            acknowledged_through: 0,
            expected_generation: 0,
            advertise: "127.0.0.1:7444".parse().unwrap(),
        },
    }
}
fn node_grant(node: u64) -> PeerGrant {
    PeerGrant {
        principal: ParticipantId(PRINCIPAL),
        tenants: BTreeSet::from([namespace().tenant]),
        role: PeerRole::Node { node_id: node },
    }
}
fn decode(response: ResponseEnvelope) -> ControlReply {
    let Response::Control { response } = response.result else {
        panic!("expected root reply")
    };
    ControlReply::decode(
        &response,
        ControlHost::wire_limits().max_frame_bytes as usize,
    )
    .unwrap()
}
fn ready(replica: &mut ControlReplica) {
    replica.drain(&NoDirectoryAuthority).unwrap();
    replica.campaign().unwrap();
    replica.drain(&NoDirectoryAuthority).unwrap();
}

#[tokio::test]
async fn contact_uses_active_committed_certificate_and_exact_receipt_survives_lost_reply_restart() {
    let disk = tempfile::tempdir().unwrap();
    let authority = BootstrapAuthority::open_or_create(
        disk.path().join("ca"),
        CLUSTER,
        vec!["root.focal.test".into()],
        now(),
    )
    .unwrap();
    let key = JoinKey::open_or_create(disk.path().join("key"), CLUSTER).unwrap();
    let draft = FoundingEnrollmentDraft::open_or_create(
        disk.path().join("genesis-node"),
        &authority,
        &key,
        2,
        PRINCIPAL,
        EnrollmentLimits::default(),
        now(),
    )
    .unwrap();
    let receipt = draft.receipt().clone();
    let material = key
        .complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    let registry = EnrollmentRegistry::restore(
        &draft.registry().checkpoint().unwrap(),
        CLUSTER,
        EnrollmentLimits::default(),
    )
    .unwrap();
    let root = RootDirectory::new(
        focal_directory::ClusterId(CLUSTER),
        RootConfig::default(),
        budget(),
    )
    .unwrap();
    let bootstrap = ControlBootstrap::root(&root, &registry).unwrap();
    let path = disk.path().join("root");
    let allowance = budget();
    let mut replica =
        ControlReplica::open(options(), bootstrap.clone(), allowance.clone(), &path).unwrap();
    ready(&mut replica);
    let (host, owner, _outgoing) = ControlHost::spawn(
        replica,
        NoDirectoryAuthority,
        crate::control_host::ControlHostConfig::new(namespace()),
        allowance,
    )
    .unwrap();
    let contacts = NodeContactHost::new(host.clone()).unwrap();
    let peers = PeerRegistry::new(4).unwrap();
    let fingerprint = peers
        .register_certificate(&receipt.certificate, node_grant(2))
        .unwrap();
    let peer = peers.authenticate(fingerprint).unwrap();
    let limits = ControlHost::wire_limits();
    let server_material = authority.server_identity();
    let server = Arc::new(
        QuicServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            server_tls(
                TlsIdentity::from_pkcs8(
                    server_material.certificate_chain().to_vec(),
                    server_material.private_key_der().to_vec(),
                ),
                vec![authority.ca_certificate().to_vec()],
                &limits,
            )
            .unwrap(),
            peers.clone(),
            limits.clone(),
        )
        .unwrap(),
    );
    let serving = server.clone();
    let lost = Arc::new(AtomicBool::new(false));
    let handler = contacts.clone();
    let dropped = lost.clone();
    let task = tokio::spawn(async move {
        serving
            .serve(move |request: VerifiedRequest| {
                let handler = handler.clone();
                let dropped = dropped.clone();
                async move {
                    let header = request.request().clone();
                    let response = handler.handle(request).await;
                    if matches!(decode(response.clone()), ControlReply::Committed(_))
                        && !dropped.swap(true, Ordering::SeqCst)
                    {
                        return header.reply(Response::Control {
                            response: ControlReply::Rejected(ControlFailure::OutcomeUnknown)
                                .encode(4096)
                                .unwrap(),
                        });
                    }
                    response
                }
            })
            .await
            .unwrap()
    });
    let connector = QuicConnector::bind(
        "127.0.0.1:0".parse().unwrap(),
        client_tls(
            TlsIdentity::from_pkcs8(
                material.certificate_chain().to_vec(),
                material.private_key_der().to_vec(),
            ),
            vec![authority.ca_certificate().to_vec()],
            &limits,
        )
        .unwrap(),
        limits.clone(),
    )
    .unwrap();
    let remote = connector
        .connect(server.local_addr().unwrap(), "root.focal.test")
        .await
        .unwrap();
    assert_eq!(
        decode(remote.request(&packet()).await.unwrap()),
        ControlReply::Rejected(ControlFailure::OutcomeUnknown)
    );
    let ControlReply::Committed(first) = decode(remote.request(&packet()).await.unwrap()) else {
        panic!("committed retry")
    };
    assert!(lost.load(Ordering::SeqCst));
    assert_eq!(
        first.request,
        ControlRequestId {
            client: PRINCIPAL,
            sequence: 1
        }
    );
    let ControlReadResult::Contacts(snapshot) = host
        .read(peer.clone(), RequestId::from_u128(2), ControlRead::Contacts)
        .await
        .unwrap()
    else {
        panic!("contacts")
    };
    assert_eq!(snapshot.contacts.records.len(), 1);
    let row = &snapshot.contacts.records[0];
    assert_eq!(row.node, 2);
    assert_eq!(row.certificate_fingerprint, fingerprint);
    assert_eq!(row.server_name, receipt.identity.server_name);
    assert_eq!(row.committed_index, first.committed_index);
    let ControlReadResult::Membership(membership) = host
        .read(
            peer.clone(),
            RequestId::from_u128(3),
            ControlRead::Membership,
        )
        .await
        .unwrap()
    else {
        panic!("membership")
    };
    assert_eq!(membership.voters, vec![1]);
    assert!(membership.learners.is_empty());
    let mut conflict = packet();
    if let Operation::NodeContact { advertise, .. } = &mut conflict.operation {
        *advertise = "127.0.0.1:7445".parse().unwrap();
    }
    assert_eq!(
        decode(remote.request(&conflict).await.unwrap()),
        ControlReply::Rejected(ControlFailure::RetryConflict)
    );
    peers
        .register_certificate(&receipt.certificate, node_grant(99))
        .unwrap();
    assert_eq!(
        decode(remote.request(&packet()).await.unwrap()),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    peers
        .register_certificate(&receipt.certificate, node_grant(2))
        .unwrap();
    remote.close();
    server.close();
    task.await.unwrap();
    host.stop().await.unwrap();
    owner.join().unwrap();
    drop((host, contacts));
    let allowance = budget();
    let mut replica = ControlReplica::open(options(), bootstrap, allowance.clone(), &path).unwrap();
    ready(&mut replica);
    assert_eq!(replica.receipt(first.request).unwrap(), Some(first));
    assert_eq!(replica.contacts().unwrap(), &snapshot.contacts);
    let (host, owner, _outgoing) = ControlHost::spawn(
        replica,
        NoDirectoryAuthority,
        crate::control_host::ControlHostConfig::new(namespace()),
        allowance,
    )
    .unwrap();
    let contacts = NodeContactHost::new(host.clone()).unwrap();
    let retry = verify_request(peer.clone(), packet(), &limits).unwrap();
    assert_eq!(
        decode(contacts.handle(retry).await),
        ControlReply::Committed(first)
    );
    let revoke = registry.prepare_revoke(receipt.invitation, now()).unwrap();
    let operator = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(99),
        tenants: BTreeSet::from([namespace().tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    // Generic Runtime control does not assert another Node's network endpoint.
    assert_eq!(
        host.submit(
            operator.clone(),
            ControlRequest {
                id: ControlRequestId {
                    client: operator.principal().0,
                    sequence: 1
                },
                acknowledged_through: 0,
                command: ControlCommand::NodeContact(NodeContactCommand {
                    node: 2,
                    principal: PRINCIPAL,
                    certificate_fingerprint: fingerprint,
                    advertise: "127.0.0.1:7555".parse().unwrap(),
                    expected_generation: 1,
                    decided_at: now(),
                }),
            }
        )
        .await,
        Err(ControlFailure::Unauthorized),
    );
    host.submit(
        operator,
        ControlRequest {
            id: ControlRequestId {
                client: ParticipantId::from_u128(99).0,
                sequence: 1,
            },
            acknowledged_through: 0,
            command: ControlCommand::Enrollment(revoke),
        },
    )
    .await
    .unwrap();
    // The connection grant intentionally remains cached: owner reauthorization
    // rejects even the original receipt after the enrollment revocation commits.
    assert_eq!(
        decode(
            contacts
                .handle(verify_request(peer, packet(), &limits).unwrap())
                .await
        ),
        ControlReply::Rejected(ControlFailure::Unauthorized)
    );
    host.stop().await.unwrap();
    owner.join().unwrap();
}
