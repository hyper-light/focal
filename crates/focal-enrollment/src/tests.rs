use crate::*;
use focal_log::{FaultPoint, LogicalLogId, Record, RecordKind, Wal, WalIdentity, WalOptions};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use std::{
    io::Cursor,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[test]
fn privately_saved_invitation_recovers_without_releasing_uncommitted_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [91; 16]);
    let mut registry = registry(&authority);
    let pending = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap()
        .persist(dir.path().join("pending"), [92; 32])
        .unwrap();
    assert!(matches!(
        pending.release(&registry),
        Err(EnrollmentError::NotCommitted)
    ));
    let command = pending.command().clone();
    let id = pending.id();
    drop(pending); // crash before proposal; private bytes remain under owner-only custody
    let pending = PendingInvitation::open(dir.path().join("pending"), authority.cluster()).unwrap();
    assert_eq!(pending.command(), &command);
    assert_eq!(pending.id(), id);
    assert_eq!(pending.intent_hash(), [92; 32]);
    registry.apply_committed(&command, 1).unwrap();
    let first = pending.release(&registry).unwrap().expose_token().unwrap();
    drop(pending); // crash after commit, before a client could receive the token
    let pending = PendingInvitation::open(dir.path().join("pending"), authority.cluster()).unwrap();
    let second = pending.release(&registry).unwrap().expose_token().unwrap();
    assert!(first == second);
    let revoke = registry.prepare_revoke(id, now()).unwrap();
    registry.apply_committed(&revoke, 2).unwrap();
    assert!(matches!(
        pending.release(&registry),
        Err(EnrollmentError::Revoked)
    ));
}
fn authority(dir: &tempfile::TempDir, cluster: ClusterId) -> BootstrapAuthority {
    BootstrapAuthority::open_or_create(
        dir.path().join("authority"),
        cluster,
        vec!["localhost".into()],
        now(),
    )
    .unwrap()
}
fn registry(authority: &BootstrapAuthority) -> EnrollmentRegistry {
    EnrollmentRegistry::new(
        authority.cluster(),
        authority.ca_certificate().to_vec(),
        2,
        EnrollmentLimits::default(),
    )
    .unwrap()
}
fn invite(
    registry: &mut EnrollmentRegistry,
    authority: &BootstrapAuthority,
    role: EnrollmentRole,
) -> Invitation {
    let draft = registry
        .prepare_invitation(
            authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    registry
        .apply_committed(draft.command(), registry.applied_index() + 1)
        .unwrap();
    draft.release(registry).unwrap()
}
fn commit(registry: &mut EnrollmentRegistry, preparation: JoinPreparation) {
    let JoinPreparation::Commit(command) = preparation else {
        panic!("expected a new enrollment")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
}
fn handshake(
    invitation: &Invitation,
    server: rustls::ServerConfig,
) -> Result<rustls::ClientConnection, rustls::Error> {
    let mut client = rustls::ClientConnection::new(
        Arc::new(invitation.client_config().unwrap()),
        ServerName::try_from(invitation.trust().server_name.clone()).unwrap(),
    )?;
    let mut server = rustls::ServerConnection::new(Arc::new(server))?;
    for _ in 0..32 {
        if client.wants_write() {
            let mut bytes = Vec::new();
            client.write_tls(&mut bytes).unwrap();
            server.read_tls(&mut Cursor::new(bytes)).unwrap();
            server.process_new_packets()?;
        }
        if server.wants_write() {
            let mut bytes = Vec::new();
            server.write_tls(&mut bytes).unwrap();
            client.read_tls(&mut Cursor::new(bytes)).unwrap();
            client.process_new_packets()?;
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(client);
        }
    }
    panic!("TLS handshake did not finish")
}

#[test]
fn committed_metadata_precedes_delivery_and_exact_retry_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let draft = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    // A draft cannot release before its metadata is committed.
    let uncommitted = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    assert!(matches!(
        uncommitted.release(&registry),
        Err(EnrollmentError::NotCommitted)
    ));
    registry.apply_committed(draft.command(), 1).unwrap();
    let invitation = draft.release(&registry).unwrap();
    let token = invitation.expose_token().unwrap();
    assert!(!format!("{invitation:?}").contains(&hex(&invitation.data.secret.0)));
    let invitation = Invitation::parse(&token).unwrap();
    let key_dir = dir.path().join("join");
    let key = JoinKey::open_or_create(&key_dir, [1; 16]).unwrap();
    let connection = handshake(
        &invitation,
        authority.server_identity().server_config().unwrap(),
    )
    .unwrap();
    let request = invitation
        .request_after_tls(&connection, &key, now())
        .unwrap();
    assert!(matches!(
        registry.release(&request, now()),
        Err(EnrollmentError::NotCommitted)
    ));
    let preparation = registry.prepare_join(&authority, &request, now()).unwrap();
    let JoinPreparation::Commit(command) = preparation else {
        panic!("new enrollment")
    };
    let bytes = command.encode().unwrap();
    assert!(
        !bytes
            .windows(32)
            .any(|window| window == invitation.data.secret.0)
    );
    assert!(!format!("{command:?}").contains(&hex(&invitation.data.secret.0)));
    registry
        .apply_committed(&EnrollmentCommand::decode(&bytes).unwrap(), 2)
        .unwrap();
    let receipt = registry.release(&request, now()).unwrap();
    assert_eq!(receipt.identity.node_id, Some(2));
    assert_eq!(
        registry
            .authorize_certificate(&receipt.certificate, now())
            .unwrap(),
        receipt.identity
    );
    key.complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    let original_csr = key.csr().to_vec();
    drop(key);
    let restored_key = JoinKey::open_or_create(&key_dir, [1; 16]).unwrap();
    assert_eq!(restored_key.csr(), original_csr);
    assert_eq!(restored_key.enrollment().unwrap(), Some(receipt.clone()));
    let restored = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    let request_again = invitation
        .request_after_tls(&connection, &restored_key, now())
        .unwrap();
    assert_eq!(*request.encode().unwrap(), *request_again.encode().unwrap());
    let JoinPreparation::Existing(retried) = restored
        .prepare_join(&authority, &request_again, now())
        .unwrap()
    else {
        panic!("must return committed identity")
    };
    assert_eq!(retried, receipt);
    assert_eq!(restored.enrollments().count(), 1);
}

#[test]
fn expired_revoked_wrong_cluster_role_and_identity_reuse_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    let key = JoinKey::open_or_create(dir.path().join("join"), [1; 16]).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    let mut forged = request.clone();
    forged.cluster = [2; 16];
    assert!(matches!(
        registry.prepare_join(&authority, &forged, now()),
        Err(EnrollmentError::WrongCluster)
    ));
    forged = request.clone();
    forged.role = EnrollmentRole::Client;
    assert!(matches!(
        registry.prepare_join(&authority, &forged, now()),
        Err(EnrollmentError::Unauthorized)
    ));
    forged = request.clone();
    forged.secret.0[0] ^= 1;
    assert!(matches!(
        registry.prepare_join(&authority, &forged, now()),
        Err(EnrollmentError::Unauthorized)
    ));
    forged = request.clone();
    forged.csr[50] ^= 1;
    assert!(registry.prepare_join(&authority, &forged, now()).is_err());
    assert!(matches!(
        registry.prepare_join(&authority, &request, invitation.expires_at()),
        Err(EnrollmentError::Expired)
    ));
    let prepared = registry.prepare_join(&authority, &request, now()).unwrap();
    commit(&mut registry, prepared);
    let receipt = registry.release(&request, now()).unwrap();
    let other_key = JoinKey::open_or_create(dir.path().join("other"), [1; 16]).unwrap();
    let after_admission_expiry = invitation.expires_at() + 1;
    let saved_retry = invitation.request(&key, after_admission_expiry).unwrap();
    assert_eq!(
        registry
            .release(&saved_retry, after_admission_expiry)
            .unwrap(),
        receipt
    );
    assert!(matches!(
        registry.release(&saved_retry, receipt.expires_at),
        Err(EnrollmentError::Expired)
    ));
    let other_request = invitation.request(&other_key, now()).unwrap();
    assert!(matches!(
        registry.prepare_join(&authority, &other_request, now()),
        Err(EnrollmentError::Used)
    ));
    let another_invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    let same_key_again = another_invitation.request(&key, now()).unwrap();
    assert!(matches!(
        registry.prepare_join(&authority, &same_key_again, now()),
        Err(EnrollmentError::Used)
    ));
    let revoke = registry.prepare_revoke(invitation.id(), now()).unwrap();
    registry
        .apply_committed(&revoke, registry.applied_index() + 1)
        .unwrap();
    assert!(matches!(
        registry.release(&request, now()),
        Err(EnrollmentError::Revoked)
    ));
    assert!(matches!(
        registry.authorize_certificate(&receipt.certificate, now()),
        Err(EnrollmentError::Revoked)
    ));
    assert_eq!(registry.enrollments().count(), 1);
}

#[test]
fn concurrent_preparations_commit_one_identity_and_the_loser_retries() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    let first_key = JoinKey::open_or_create(dir.path().join("first"), [1; 16]).unwrap();
    let other_key = JoinKey::open_or_create(dir.path().join("other"), [1; 16]).unwrap();
    let first = invitation.request(&first_key, now()).unwrap();
    let other = invitation.request(&other_key, now()).unwrap();
    let JoinPreparation::Commit(first_command) =
        registry.prepare_join(&authority, &first, now()).unwrap()
    else {
        panic!()
    };
    let JoinPreparation::Commit(other_command) =
        registry.prepare_join(&authority, &other, now()).unwrap()
    else {
        panic!()
    };
    registry.apply_committed(&first_command, 2).unwrap();
    assert!(matches!(
        registry.apply_committed(&other_command, 3),
        Err(EnrollmentError::Conflict)
    ));
    assert!(matches!(
        registry.prepare_join(&authority, &other, now()),
        Err(EnrollmentError::Used)
    ));
    assert!(matches!(
        registry.prepare_join(&authority, &first, now()),
        Ok(JoinPreparation::Existing(_))
    ));
    assert_eq!(registry.enrollments().count(), 1);
}

#[test]
fn metadata_wal_failure_never_releases_certificate_and_replay_is_identical() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let draft = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let options = WalOptions::new(WalIdentity {
        cluster: [1; 16],
        node: 1,
        stream: 7,
    });
    let mut wal = Wal::open(dir.path().join("wal"), options.clone()).unwrap();
    let record = |index, command: &EnrollmentCommand| Record {
        log: LogicalLogId([7; 16]),
        kind: RecordKind::Entry,
        index,
        term: 1,
        payload: command.encode().unwrap(),
    };
    wal.append(&[record(1, draft.command())]).unwrap();
    registry.apply_committed(draft.command(), 1).unwrap();
    let invitation = draft.release(&registry).unwrap();
    let key = JoinKey::open_or_create(dir.path().join("join"), [1; 16]).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    let JoinPreparation::Commit(command) =
        registry.prepare_join(&authority, &request, now()).unwrap()
    else {
        panic!()
    };
    wal.inject_fault_once(FaultPoint::AfterDataSync);
    assert!(wal.append(&[record(2, &command)]).is_err());
    assert!(matches!(
        registry.release(&request, now()),
        Err(EnrollmentError::NotCommitted)
    ));
    drop(wal);
    let mut replayed = super::tests::registry(&authority);
    let mut recovered = Wal::open(dir.path().join("wal"), options).unwrap();
    recovered
        .replay(|record| {
            replayed
                .apply_committed(
                    &EnrollmentCommand::decode(&record.payload).unwrap(),
                    record.index,
                )
                .unwrap();
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        replayed.release(&request, now()),
        Err(EnrollmentError::NotCommitted)
    ));
    recovered.append(&[record(2, &command)]).unwrap();
    replayed.apply_committed(&command, 2).unwrap();
    assert_eq!(
        replayed.release(&request, now()).unwrap().identity.node_id,
        Some(2)
    );
}

#[test]
fn caller_csr_names_and_privileges_are_replaced_by_assigned_identity() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Client);
    let key = JoinKey::open_or_create(dir.path().join("join"), [1; 16]).unwrap();
    let mut request = invitation.request(&key, now()).unwrap();
    let attack_key = KeyPair::generate().unwrap();
    let mut parameters =
        CertificateParams::new(vec!["admin.internal".into(), "*.example.com".into()]).unwrap();
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    parameters.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::CodeSigning,
    ];
    request.csr = parameters
        .serialize_request(&attack_key)
        .unwrap()
        .der()
        .to_vec();
    let preparation = registry.prepare_join(&authority, &request, now()).unwrap();
    commit(&mut registry, preparation);
    let receipt = registry.release(&request, now()).unwrap();
    assert_eq!(receipt.identity.role, EnrollmentRole::Client);
    assert_eq!(receipt.identity.node_id, None);
    assert!(!receipt.identity.server_name.contains("admin"));
    crate::pki::verify_issued(&receipt, authority.ca_certificate()).unwrap();
    assert!(
        key.complete(&receipt, authority.ca_certificate(), now())
            .is_err()
    );
}

#[test]
fn tls_ca_and_name_verification_does_not_replace_the_exact_invited_leaf_pin() {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_ca_cert_der(ca.der(), &ca_key).unwrap();
    let leaf = |key: &KeyPair| {
        let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.signed_by(key, &issuer).unwrap()
    };
    let invited_key = KeyPair::generate().unwrap();
    let invited_leaf = leaf(&invited_key);
    let other_key = KeyPair::generate().unwrap();
    let other_leaf = leaf(&other_key);
    let invitation = Invitation {
        data: crate::invitation::InvitationData {
            schema: 1,
            id: [8; 16],
            cluster: [1; 16],
            role: EnrollmentRole::Node,
            expires_at: now() + 600,
            secret: crate::pki::SecretBytes(vec![7; 32]),
            trust: ServerTrust {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                ca_certificate: ca.der().to_vec(),
                server_fingerprint: server_fingerprint(invited_leaf.der()),
            },
        },
    };
    let config = |certificate: Vec<u8>, key: &KeyPair| {
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.into()],
            PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
        config.alpn_protocols = vec![ENROLLMENT_ALPN.to_vec()];
        config
    };
    let dir = tempfile::tempdir().unwrap();
    let key = JoinKey::open_or_create(dir.path().join("join"), [1; 16]).unwrap();
    let wrong = handshake(&invitation, config(other_leaf.der().to_vec(), &other_key)).unwrap();
    assert!(matches!(
        invitation.request_after_tls(&wrong, &key, now()),
        Err(EnrollmentError::Unauthorized)
    ));
    let valid = handshake(
        &invitation,
        config(invited_leaf.der().to_vec(), &invited_key),
    )
    .unwrap();
    invitation.request_after_tls(&valid, &key, now()).unwrap();
    let stranger_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    let stranger = stranger_params.self_signed(&other_key).unwrap();
    assert!(handshake(&invitation, config(stranger.der().to_vec(), &other_key)).is_err());
}

#[cfg(unix)]
#[test]
fn private_material_is_owner_only_cluster_bound_locked_and_missing_state_fails_closed() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority");
    let original = authority(&dir, [1; 16]);
    let ca = original.ca_certificate().to_vec();
    assert_eq!(
        std::fs::metadata(path.join("authority.bin"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(matches!(
        BootstrapAuthority::open_or_create(&path, [1; 16], vec!["localhost".into()], now()),
        Err(EnrollmentError::Locked)
    ));
    drop(original);
    assert!(matches!(
        BootstrapAuthority::open_or_create(&path, [2; 16], vec!["localhost".into()], now()),
        Err(EnrollmentError::WrongCluster)
    ));
    let recovered =
        BootstrapAuthority::open_or_create(&path, [1; 16], vec!["localhost".into()], now())
            .unwrap();
    assert_eq!(recovered.ca_certificate(), ca);
    drop(recovered);
    std::fs::set_permissions(
        path.join("authority.bin"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert!(matches!(
        BootstrapAuthority::open_or_create(&path, [1; 16], vec!["localhost".into()], now()),
        Err(EnrollmentError::Permissions)
    ));
    std::fs::remove_file(path.join("authority.bin")).unwrap();
    assert!(matches!(
        BootstrapAuthority::open_or_create(&path, [1; 16], vec!["localhost".into()], now()),
        Err(EnrollmentError::Corrupt)
    ));
}

#[test]
fn prepared_publication_checks_lineage_revision_and_metadata_budget() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let mut other = super::tests::registry(&authority);
    let draft = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let prepared = registry.prepare_command(draft.command()).unwrap();
    assert!(matches!(
        other.publish(prepared, 1),
        Err(EnrollmentError::Conflict)
    ));
    let prepared = registry.prepare_command(draft.command()).unwrap();
    let stale = registry.prepare_command(draft.command()).unwrap();
    let predicted = prepared.charged_bytes();
    registry.publish(prepared, 12).unwrap();
    assert_eq!(registry.charged_bytes(), predicted);
    assert_eq!(registry.applied_index(), 12);
    assert!(matches!(
        registry.publish(stale, 13),
        Err(EnrollmentError::Conflict)
    ));
    draft.release(&registry).unwrap();

    let limits = EnrollmentLimits {
        max_checkpoint_bytes: 16 * 1024,
        ..EnrollmentLimits::default()
    };
    let mut bounded = EnrollmentRegistry::new(
        [1; 16],
        authority.ca_certificate().to_vec(),
        2,
        limits.clone(),
    )
    .unwrap();
    loop {
        let candidate = bounded.prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Client,
                expires_at: now() + 600,
            },
            now(),
        );
        match candidate {
            Ok(draft) => bounded
                .apply_committed(draft.command(), bounded.applied_index() + 1)
                .unwrap(),
            Err(EnrollmentError::Capacity) => break,
            Err(error) => panic!("unexpected {error}"),
        }
    }
    assert!(bounded.charged_bytes() <= limits.max_checkpoint_bytes);
    let bytes = bounded.checkpoint().unwrap();
    EnrollmentRegistry::restore(&bytes, [1; 16], limits).unwrap();
}

#[tokio::test]
async fn quic_pin_precedes_token_and_unknown_commit_retries_the_same_enrollment() {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    let dir = tempfile::tempdir().unwrap();
    let authority = Arc::new(authority(&dir, [1; 16]));
    let mut registry = registry(&authority);
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    let registry = Arc::new(Mutex::new(registry));
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_registry = registry.clone();
    let signer = authority.clone();
    let counted = calls.clone();
    let handler: Arc<dyn JoinHandler> = Arc::new(move |request: JoinRequest| {
        let mut registry = handler_registry.lock().unwrap();
        let preparation = registry.prepare_join(&signer, &request, now()).unwrap();
        if let JoinPreparation::Commit(command) = preparation {
            let prepared = registry.prepare_command(&command).unwrap();
            let index = registry.applied_index() + 1;
            registry.publish(prepared, index).unwrap();
        }
        let receipt = registry.release(&request, now()).unwrap();
        let first = counted.fetch_add(1, Ordering::SeqCst) == 0;
        async move {
            if first {
                JoinResponse::Rejected(JoinFailure::OutcomeUnknown)
            } else {
                JoinResponse::Enrolled(receipt)
            }
        }
    });
    let server = Arc::new(
        EnrollmentServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            &authority.server_identity(),
            TransportLimits::default(),
        )
        .unwrap(),
    );
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(handler).await });
    let client =
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()).unwrap();
    let key = JoinKey::open_or_create(dir.path().join("join"), [1; 16]).unwrap();
    let mut bad_pin = invitation.clone();
    bad_pin.data.trust.server_fingerprint[0] ^= 1;
    assert!(matches!(
        client
            .redeem(server.local_addr().unwrap(), &bad_pin, &key, now())
            .await,
        Err(JoinTransportError::Enrollment(
            EnrollmentError::Unauthorized
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // Even a correctly authenticated server connection cannot force an oversized
    // body allocation or reach metadata by declaring an unbounded frame length.
    let mut raw_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let tls = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(invitation.client_config().unwrap())
            .unwrap(),
    ));
    raw_endpoint.set_default_client_config(tls);
    let raw = raw_endpoint
        .connect(server.local_addr().unwrap(), "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut receive) = raw.open_bi().await.unwrap();
    let mut header = [0u8; 16];
    header[..8].copy_from_slice(b"FCLENR01");
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&1u16.to_be_bytes());
    header[12..].copy_from_slice(&u32::MAX.to_be_bytes());
    send.write_all(&header).await.unwrap();
    send.finish().unwrap();
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        receive.read_chunk(1, true),
    )
    .await
    .unwrap();
    assert!(rejected.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    raw.close(0u8.into(), b"done");
    assert!(matches!(
        client
            .redeem(server.local_addr().unwrap(), &invitation, &key, now())
            .await,
        Err(JoinTransportError::OutcomeUnknown)
    ));
    let receipt = client
        .redeem(server.local_addr().unwrap(), &invitation, &key, now())
        .await
        .unwrap();
    assert_eq!(receipt.identity.node_id, Some(2));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(registry.lock().unwrap().enrollments().count(), 1);
    key.complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    client.close();
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn restored_registry_owns_fresh_publication_lineage_and_raw_decode_cannot_publish() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = EnrollmentRegistry::new(
        [1; 16],
        authority.ca_certificate().to_vec(),
        2,
        EnrollmentLimits::default(),
    )
    .unwrap();
    let draft = registry
        .prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: now() + 600,
            },
            now(),
        )
        .unwrap();
    let bytes = registry.checkpoint().unwrap();
    let mut restored =
        EnrollmentRegistry::restore(&bytes, [1; 16], EnrollmentLimits::default()).unwrap();
    let raw: EnrollmentRegistry = postcard::from_bytes(&bytes).unwrap();
    assert!(matches!(
        raw.prepare_command(draft.command()),
        Err(EnrollmentError::Conflict)
    ));
    let prepared = registry.prepare_command(draft.command()).unwrap();
    assert!(matches!(
        restored.publish(prepared, 1),
        Err(EnrollmentError::Conflict)
    ));
    let prepared = registry.prepare_command(draft.command()).unwrap();
    registry.publish(prepared, 1).unwrap();
    let prepared = restored.prepare_command(draft.command()).unwrap();
    restored.publish(prepared, 1).unwrap();
    assert_eq!(
        registry.checkpoint().unwrap(),
        restored.checkpoint().unwrap()
    );
}

#[test]
fn enrollment_client_rejects_missing_runtime_before_socket_creation() {
    assert!(matches!(
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()),
        Err(JoinTransportError::Unavailable)
    ));
}

#[test]
fn node_statements_bind_payload_cluster_certificate_role_and_revocation() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [75; 16]);
    let mut registry = registry(&authority);
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    let key = JoinKey::open_or_create(dir.path().join("node-key"), authority.cluster()).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    let prepared = registry.prepare_join(&authority, &request, now()).unwrap();
    commit(&mut registry, prepared);
    let receipt = registry.release(&request, now()).unwrap();
    let credential = key
        .complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    let statement = b"exact scoped public statement";
    let proof = credential
        .sign_node_statement(authority.cluster(), statement)
        .unwrap();
    assert_eq!(
        registry
            .verify_node_statement(&proof, statement, now())
            .unwrap(),
        receipt.identity
    );
    assert!(
        registry
            .verify_node_statement(&proof, b"different statement", now())
            .is_err()
    );
    let mut bad = proof.clone();
    bad.cluster = [76; 16];
    assert!(matches!(
        registry.verify_node_statement(&bad, statement, now()),
        Err(EnrollmentError::WrongCluster)
    ));
    let mut bad = proof.clone();
    bad.signature[0] ^= 1;
    assert!(
        registry
            .verify_node_statement(&bad, statement, now())
            .is_err()
    );
    let mut bad = proof.clone();
    bad.certificate = authority.ca_certificate().to_vec();
    assert!(
        registry
            .verify_node_statement(&bad, statement, now())
            .is_err()
    );
    let checkpoint = registry.checkpoint().unwrap();
    let mut registry = EnrollmentRegistry::restore(
        &checkpoint,
        authority.cluster(),
        EnrollmentLimits::default(),
    )
    .unwrap();
    registry
        .verify_node_statement(&proof, statement, now())
        .unwrap();
    let revoke = registry.prepare_revoke(receipt.invitation, now()).unwrap();
    registry
        .apply_committed(&revoke, registry.applied_index() + 1)
        .unwrap();
    assert!(matches!(
        registry.verify_node_statement(&proof, statement, now()),
        Err(EnrollmentError::Revoked)
    ));

    let invitation = invite(&mut registry, &authority, EnrollmentRole::Client);
    let key = JoinKey::open_or_create(dir.path().join("client-key"), authority.cluster()).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    let prepared = registry.prepare_join(&authority, &request, now()).unwrap();
    commit(&mut registry, prepared);
    let receipt = registry.release(&request, now()).unwrap();
    let credential = key
        .complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    let proof = credential
        .sign_node_statement(authority.cluster(), statement)
        .unwrap();
    assert!(matches!(
        registry.verify_node_statement(&proof, statement, now()),
        Err(EnrollmentError::Unauthorized)
    ));
}
