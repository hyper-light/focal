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

fn enroll_node(
    dir: &tempfile::TempDir,
    registry: &mut EnrollmentRegistry,
    authority: &BootstrapAuthority,
    name: &str,
) -> (JoinKey, EnrollmentReceipt, CredentialMaterial) {
    let invitation = invite(registry, authority, EnrollmentRole::Node);
    let key = JoinKey::open_or_create(dir.path().join(name), [1; 16]).unwrap();
    let request = invitation.request(&key, now()).unwrap();
    commit(
        registry,
        registry.prepare_join(authority, &request, now()).unwrap(),
    );
    let receipt = registry.release(&request, now()).unwrap();
    let material = key
        .complete(&receipt, authority.ca_certificate(), now())
        .unwrap();
    (key, receipt, material)
}

#[test]
fn a_renewal_keeps_the_key_and_identity_retires_the_old_certificate_after_grace_and_is_idempotent()
{
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let (key, first, material) = enroll_node(&dir, &mut registry, &authority, "node");
    assert_eq!(
        certificate_key_hash(&first.certificate).unwrap(),
        first.public_key
    );
    let request = material.renewal_request(&key, &first).unwrap();
    assert_eq!(request.holds_until(), first.expires_at);
    let at = now() + 10;
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, at, 30)
        .unwrap()
    else {
        panic!("a first renewal commits")
    };
    assert_eq!(command.renewed_invitation(), Some(first.invitation));
    assert!(command.revoked_invitation().is_none());
    // Nothing is released before the commit is applied.
    assert!(matches!(
        registry.release_renewal(&request, at),
        Err(EnrollmentError::NotCommitted)
    ));
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&request, at).unwrap();
    assert_eq!(renewed.identity, first.identity);
    assert_eq!(renewed.public_key, first.public_key);
    assert_eq!(renewed.request, first.request);
    assert_eq!(renewed.csr_hash, first.csr_hash);
    assert_eq!(renewed.issued_at, at);
    assert_eq!(
        renewed.expires_at,
        at + EnrollmentLimits::default().credential_lifetime as i64
    );
    assert_ne!(renewed.certificate, first.certificate);
    assert_eq!(
        certificate_key_hash(&renewed.certificate).unwrap(),
        first.public_key
    );
    // Both certificates authorize during the grace; only the new one after.
    assert_eq!(
        registry
            .authorize_certificate(&first.certificate, at + 29)
            .unwrap(),
        first.identity
    );
    assert_eq!(
        registry
            .authorize_certificate(&renewed.certificate, at + 29)
            .unwrap(),
        first.identity
    );
    assert_eq!(registry.retired(at + 29).count(), 1);
    assert_eq!(registry.retired(at + 30).count(), 0);
    assert!(matches!(
        registry.authorize_certificate(&first.certificate, at + 30),
        Err(EnrollmentError::Expired)
    ));
    assert!(
        registry
            .authorize_certificate(&renewed.certificate, at + 30)
            .is_ok()
    );
    assert_eq!(registry.enrollments().count(), 1);
    // The same request again finds the committed renewal; a request signed
    // under the retired certificate still does while it authorizes.
    assert!(matches!(
        registry.prepare_renew(&authority, &request, at + 5, 30).unwrap(),
        RenewPreparation::Existing(receipt) if receipt == renewed
    ));
    assert!(matches!(
        registry.prepare_renew(&authority, &request, at + 31, 30),
        Err(EnrollmentError::Expired)
    ));
    // Under the renewed credential a further renewal commits again.
    let material = key.renew(&renewed, authority.ca_certificate(), at).unwrap();
    assert_eq!(material.certificate_chain()[0], renewed.certificate);
    assert_eq!(key.enrollment().unwrap().unwrap(), renewed);
    // Installing the older receipt again is refused; the same one is a no-op.
    assert!(matches!(
        key.renew(&first, authority.ca_certificate(), at),
        Err(EnrollmentError::Conflict)
    ));
    key.renew(&renewed, authority.ca_certificate(), at).unwrap();
    let again = material.renewal_request(&key, &renewed).unwrap();
    let RenewPreparation::Commit(second) = registry
        .prepare_renew(&authority, &again, at + 40, 30)
        .unwrap()
    else {
        panic!("a renewal under the current certificate commits")
    };
    registry
        .apply_committed(&second, registry.applied_index() + 1)
        .unwrap();
    // The first certificate's grace has passed: it left the retired table.
    assert_eq!(registry.retired(at + 41).count(), 1);
    assert!(matches!(
        registry.authorize_certificate(&first.certificate, at + 41),
        Err(EnrollmentError::Unauthorized)
    ));
    // A restored registry validates the retired table and keeps authorizing.
    let restored = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert_eq!(restored.retired(at + 41).count(), 1);
    assert!(
        restored
            .authorize_certificate(&renewed.certificate, at + 41)
            .is_ok()
    );
    let latest = restored.release_renewal(&again, at + 41).unwrap();
    assert!(
        restored
            .authorize_certificate(&latest.certificate, at + 41)
            .is_ok()
    );
    assert_eq!(restored.charged_bytes(), registry.charged_bytes());
}

#[test]
fn renewals_need_the_holder_s_own_key_and_a_live_unrevoked_enrollment() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let (key, receipt, material) = enroll_node(&dir, &mut registry, &authority, "node");
    // A renewal decided within the second the certificate was issued cannot
    // extend it and is refused rather than committed for nothing.
    let same_second = material.renewal_request(&key, &receipt).unwrap();
    let refused = registry
        .prepare_renew(&authority, &same_second, receipt.issued_at, 30)
        .err();
    assert!(
        matches!(refused, Some(EnrollmentError::Conflict)),
        "{refused:?}"
    );
    let (other_key, other_receipt, other_material) =
        enroll_node(&dir, &mut registry, &authority, "other");
    // A request signed by another enrolled node, or naming another node's
    // receipt, is refused.
    let forged = other_material.renewal_request(&key, &receipt);
    assert!(forged.is_ok());
    assert!(matches!(
        registry.prepare_renew(&authority, &forged.unwrap(), now(), 30),
        Err(EnrollmentError::Unauthorized)
    ));
    let mismatched = material
        .renewal_request(&other_key, &other_receipt)
        .unwrap();
    assert!(matches!(
        registry.prepare_renew(&authority, &mismatched, now(), 30),
        Err(EnrollmentError::Unauthorized)
    ));
    // A client credential cannot renew through the node statement path.
    let client_invitation = invite(&mut registry, &authority, EnrollmentRole::Client);
    let client_key = JoinKey::open_or_create(dir.path().join("client"), [1; 16]).unwrap();
    let request = client_invitation.request(&client_key, now()).unwrap();
    let preparation = registry.prepare_join(&authority, &request, now()).unwrap();
    commit(&mut registry, preparation);
    let client_receipt = registry.release(&request, now()).unwrap();
    let client_material = client_key
        .complete(&client_receipt, authority.ca_certificate(), now())
        .unwrap();
    let client_request = client_material
        .renewal_request(&client_key, &client_receipt)
        .unwrap();
    assert!(matches!(
        registry.prepare_renew(&authority, &client_request, now(), 30),
        Err(EnrollmentError::Unauthorized)
    ));
    // A revoked enrollment cannot renew, and a committed renewal of a
    // revoked enrollment authorizes neither certificate.
    let request = material.renewal_request(&key, &receipt).unwrap();
    // The sponsor decides later than the enrollment it renews.
    let later = now() + 5;
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, later, 30)
        .unwrap()
    else {
        panic!("commit")
    };
    let revoke = registry.prepare_revoke(receipt.invitation, later).unwrap();
    registry
        .apply_committed(&revoke, registry.applied_index() + 1)
        .unwrap();
    assert!(matches!(
        registry.apply_committed(&command, registry.applied_index() + 1),
        Err(EnrollmentError::Conflict)
    ));
    assert!(matches!(
        registry.prepare_renew(&authority, &request, later, 30),
        Err(EnrollmentError::Revoked)
    ));
    // A stale command against a moved registry is a conflict, never applied.
    let stale = registry
        .prepare_revoke(other_receipt.invitation, later)
        .unwrap();
    registry
        .apply_committed(&stale, registry.applied_index() + 1)
        .unwrap();
    assert!(matches!(
        registry.authorize_certificate(&receipt.certificate, later),
        Err(EnrollmentError::Revoked)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_renews_over_the_enrollment_transport_and_a_join_only_handler_refuses() {
    use std::sync::Mutex;
    let dir = tempfile::tempdir().unwrap();
    let authority = Arc::new(authority(&dir, [1; 16]));
    let mut registry = registry(&authority);
    let (key, receipt, material) = enroll_node(&dir, &mut registry, &authority, "node");
    let registry = Arc::new(Mutex::new(registry));
    struct Renewing {
        registry: Arc<Mutex<EnrollmentRegistry>>,
        authority: Arc<BootstrapAuthority>,
    }
    impl JoinHandler for Renewing {
        fn handle(&self, _: JoinRequest) -> JoinFuture<'_> {
            Box::pin(async { JoinResponse::Rejected(JoinFailure::Unauthorized) })
        }
        fn renew(&self, request: RenewRequest) -> JoinFuture<'_> {
            let mut registry = self.registry.lock().unwrap();
            // The sponsor decides later than the enrollment it renews.
            let at = now() + 5;
            let response = match registry.prepare_renew(&self.authority, &request, at, 30) {
                Ok(RenewPreparation::Existing(receipt)) => JoinResponse::Enrolled(receipt),
                Ok(RenewPreparation::Commit(command)) => {
                    let prepared = registry.prepare_command(&command).unwrap();
                    let index = registry.applied_index() + 1;
                    registry.publish(prepared, index).unwrap();
                    JoinResponse::Enrolled(registry.release_renewal(&request, at).unwrap())
                }
                Err(error) => JoinResponse::Rejected(JoinFailure::from(&error)),
            };
            Box::pin(async move { response })
        }
    }
    let server = Arc::new(
        EnrollmentServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            &authority.server_identity(),
            TransportLimits::default(),
        )
        .unwrap(),
    );
    let handler: Arc<dyn JoinHandler> = Arc::new(Renewing {
        registry: registry.clone(),
        authority: authority.clone(),
    });
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(handler).await });
    let client =
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()).unwrap();
    let trust = ServerTrust {
        endpoint: server.local_addr().unwrap().to_string(),
        server_name: "localhost".into(),
        ca_certificate: authority.ca_certificate().to_vec(),
        server_fingerprint: server_fingerprint(authority.server_certificate()),
    };
    let request = material.renewal_request(&key, &receipt).unwrap();
    let renewed = client
        .renew(server.local_addr().unwrap(), &trust, request.clone(), now())
        .await
        .unwrap();
    assert!(renewed.expires_at > receipt.expires_at);
    assert_eq!(renewed.public_key, receipt.public_key);
    // The same request again is answered with the committed renewal.
    let again = client
        .renew(server.local_addr().unwrap(), &trust, request, now())
        .await
        .unwrap();
    assert_eq!(again, renewed);
    // A wrong leaf pin never reaches the handler.
    let mut bad = trust.clone();
    bad.server_fingerprint[0] ^= 1;
    let request = material.renewal_request(&key, &receipt).unwrap();
    assert!(matches!(
        client
            .renew(server.local_addr().unwrap(), &bad, request, now())
            .await,
        Err(JoinTransportError::Enrollment(
            EnrollmentError::Unauthorized
        ))
    ));
    client.close();
    server.close();
    task.await.unwrap().unwrap();
    // A handler that only enrolls refuses renewals by default.
    let server = Arc::new(
        EnrollmentServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            &authority.server_identity(),
            TransportLimits::default(),
        )
        .unwrap(),
    );
    let join_only: Arc<dyn JoinHandler> =
        Arc::new(|_: JoinRequest| async { JoinResponse::Rejected(JoinFailure::Unauthorized) });
    let running = server.clone();
    let task = tokio::spawn(async move { running.serve(join_only).await });
    let client =
        EnrollmentClient::bind("127.0.0.1:0".parse().unwrap(), TransportLimits::default()).unwrap();
    let trust = ServerTrust {
        endpoint: server.local_addr().unwrap().to_string(),
        ..trust
    };
    let request = material.renewal_request(&key, &renewed).unwrap();
    assert!(matches!(
        client
            .renew(server.local_addr().unwrap(), &trust, request, now())
            .await,
        Err(JoinTransportError::Rejected(JoinFailure::Unauthorized))
    ));
    client.close();
    server.close();
    task.await.unwrap().unwrap();
}

#[test]
fn tenants_are_admitted_once_under_the_founder_authority_and_survive_restore_and_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [7; 16]);
    let mut registry = registry(&authority);
    assert_eq!(registry.tenants().count(), 0);
    let tenant = [5; 16];
    assert!(matches!(
        registry.prepare_admit_tenant(&authority, [0; 16], now()),
        Err(EnrollmentError::Invalid)
    ));
    let command = registry
        .prepare_admit_tenant(&authority, tenant, now())
        .unwrap();
    assert_eq!(command.admitted_tenant(), Some(tenant));
    assert!(command.revoked_invitation().is_none());
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert!(registry.admits_tenant(tenant));
    assert_eq!(registry.tenants().collect::<Vec<_>>(), vec![tenant]);
    // Admitting again is a conflict the operator reads as done; the stale
    // command replayed against the advanced revision is refused too.
    assert!(matches!(
        registry.prepare_admit_tenant(&authority, tenant, now()),
        Err(EnrollmentError::Conflict)
    ));
    assert!(matches!(
        registry.apply_committed(&command, registry.applied_index() + 1),
        Err(EnrollmentError::Conflict)
    ));
    // A restore keeps the admission; a schema-2 checkpoint restores with none.
    let bytes = registry.checkpoint().unwrap();
    let restored =
        EnrollmentRegistry::restore(&bytes, authority.cluster(), EnrollmentLimits::default())
            .unwrap();
    assert!(restored.admits_tenant(tenant));
    let legacy = registry.encode_as_schema_two_for_tests().unwrap();
    let upgraded =
        EnrollmentRegistry::restore(&legacy, authority.cluster(), EnrollmentLimits::default())
            .unwrap();
    assert_eq!(upgraded.tenants().count(), 0);
    assert_eq!(upgraded.revision(), registry.revision());
    let tight = EnrollmentLimits {
        max_tenants: 1,
        ..EnrollmentLimits::default()
    };
    let mut small = EnrollmentRegistry::new(
        authority.cluster(),
        authority.ca_certificate().to_vec(),
        2,
        tight,
    )
    .unwrap();
    let first = small
        .prepare_admit_tenant(&authority, [1; 16], now())
        .unwrap();
    small
        .apply_committed(&first, small.applied_index() + 1)
        .unwrap();
    assert!(matches!(
        small.prepare_admit_tenant(&authority, [2; 16], now()),
        Err(EnrollmentError::Capacity)
    ));
}
