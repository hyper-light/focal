use crate::*;
use focal_log::{FaultPoint, LogicalLogId, Record, RecordKind, Wal, WalIdentity, WalOptions};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use std::{
    collections::BTreeSet,
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
    key.complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
    crate::pki::verify_issued(&receipt, std::iter::once(authority.ca_certificate())).unwrap();
    assert!(
        key.complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
                successor_fingerprint: None,
                issuers: vec![IssuerRecord::of(ca.der(), None).unwrap()],
            },
        },
    };
    let config = |certificate: Vec<u8>, key: &KeyPair| {
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
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
    key.complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
        .complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
        .complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
        .complete(&receipt, authority.issuers().unwrap().trusted(), now())
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
    let material = key
        .renew(&renewed, authority.issuers().unwrap().trusted(), at)
        .unwrap();
    assert_eq!(material.certificate_chain()[0], renewed.certificate);
    assert_eq!(key.enrollment().unwrap().unwrap(), renewed);
    // Installing the older receipt again is refused; the same one is a no-op.
    assert!(matches!(
        key.renew(&first, authority.issuers().unwrap().trusted(), at),
        Err(EnrollmentError::Conflict)
    ));
    key.renew(&renewed, authority.issuers().unwrap().trusted(), at)
        .unwrap();
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

/// A member's replica follows the registry its credential was committed
/// in, and after a restart knows only what its log says committed: the
/// member may open holding a renewal the registry it recovered has not
/// reached. It presents it; nothing else it is not listed with passes.
#[test]
fn a_member_opens_on_a_registry_that_has_not_reached_the_renewal_it_holds() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let (key, first, material) = enroll_node(&dir, &mut registry, &authority, "node");
    let (_, other, _) = enroll_node(&dir, &mut registry, &authority, "other");
    // What the member's replica holds when it restarts: the registry before
    // the renewal.
    let behind = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    let request = material.renewal_request(&key, &first).unwrap();
    let at = now() + 10;
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, at, 30)
        .unwrap()
    else {
        panic!("a first renewal commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&request, at).unwrap();
    assert!(renewed.revision > behind.revision());
    // The registry that committed it authorizes it; the one behind does not
    // know the certificate, and stands by it as a later credential of the
    // identity it lists.
    registry.authorize_held(&renewed, at).unwrap();
    assert!(matches!(
        behind.authorize_certificate(&renewed.certificate, at),
        Err(EnrollmentError::Unauthorized)
    ));
    behind.authorize_held(&renewed, at).unwrap();
    behind.authorize_held(&first, at).unwrap();
    // Out of its validity it is refused as any credential is.
    assert!(matches!(
        behind.authorize_held(&renewed, at - 1),
        Err(EnrollmentError::Expired)
    ));
    assert!(matches!(
        behind.authorize_held(&renewed, renewed.expires_at),
        Err(EnrollmentError::Expired)
    ));
    // A receipt that says another identity, or another enrollment, than
    // the one its certificate was issued for is not the CA's.
    let mut forged = renewed.clone();
    forged.identity = other.identity.clone();
    assert!(behind.authorize_held(&forged, at).is_err());
    let mut forged = renewed.clone();
    forged.invitation = other.invitation;
    assert!(behind.authorize_held(&forged, at).is_err());
    // A revision the registry has reached without listing the certificate
    // is no renewal it has yet to apply.
    let mut stale = renewed.clone();
    stale.revision = behind.revision();
    assert!(matches!(
        behind.authorize_held(&stale, at),
        Err(EnrollmentError::Unauthorized)
    ));
    // A certificate another authority issued for the same key and names.
    let elsewhere = tempfile::tempdir().unwrap();
    let foreign = self::authority(&elsewhere, [1; 16]);
    let mut forged = renewed.clone();
    forged.certificate = foreign
        .issue(
            key.csr(),
            &renewed.identity,
            at,
            EnrollmentLimits::default().credential_lifetime,
        )
        .unwrap();
    assert!(matches!(
        behind.authorize_held(&forged, at),
        Err(EnrollmentError::Unauthorized)
    ));
    // A revoked enrollment is refused whatever it holds.
    let mut revoked = EnrollmentRegistry::restore(
        &behind.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    let revoke = revoked.prepare_revoke(first.invitation, now()).unwrap();
    revoked
        .apply_committed(&revoke, revoked.applied_index() + 1)
        .unwrap();
    let mut later = renewed.clone();
    later.revision = revoked.revision() + 1;
    assert!(matches!(
        revoked.authorize_held(&later, at),
        Err(EnrollmentError::Revoked)
    ));
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
        .complete(
            &client_receipt,
            authority.issuers().unwrap().trusted(),
            now(),
        )
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
        successor_fingerprint: None,
        issuers: authority.issuers().unwrap().trusted().cloned().collect(),
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

#[test]
fn a_rotation_changes_the_key_under_the_same_identity_and_the_old_key_signs_only_through_the_grace()
{
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let mut registry = registry(&authority);
    let (key, first, material) = enroll_node(&dir, &mut registry, &authority, "node");
    let next = JoinKey::open_or_create(dir.path().join("node-next"), [1; 16]).unwrap();
    assert_ne!(next.key_identity().unwrap(), key.key_identity().unwrap());
    // A rotation request names the new key under the current credential.
    let request = material.rotation_request(&key, &next, &first).unwrap();
    assert!(request.is_rotation());
    assert_eq!(request.request_id(), next.request_id());
    // Rotating to the key already held is refused up front.
    assert!(matches!(
        material.rotation_request(&key, &key, &first),
        Err(EnrollmentError::Unauthorized)
    ));
    let at = now() + 10;
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, at, 30)
        .unwrap()
    else {
        panic!("a first rotation commits")
    };
    assert_eq!(command.renewed_invitation(), Some(first.invitation));
    assert!(matches!(
        registry.release_renewal(&request, at),
        Err(EnrollmentError::NotCommitted)
    ));
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let rotated = registry.release_renewal(&request, at).unwrap();
    assert_eq!(rotated.identity, first.identity);
    assert_eq!(rotated.request, next.request_id());
    assert_eq!(rotated.public_key, next.key_identity().unwrap());
    assert_ne!(rotated.public_key, first.public_key);
    assert_eq!(
        certificate_key_hash(&rotated.certificate).unwrap(),
        rotated.public_key
    );
    assert_eq!(registry.enrollments().count(), 1);
    assert_eq!(
        registry.enrollments().next().unwrap().public_key,
        rotated.public_key
    );
    // The checkpoint taken after a rotation restores: the retired
    // credential names the previous key and request, the enrolled-key index
    // names only the new key.
    let restored = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert_eq!(
        restored.checkpoint().unwrap(),
        registry.checkpoint().unwrap()
    );
    assert!(matches!(
        restored.prepare_renew(&authority, &request, at + 5, 30).unwrap(),
        RenewPreparation::Existing(receipt) if receipt == rotated
    ));
    // Both certificates authorize during the grace; only the new after.
    assert_eq!(
        registry
            .authorize_certificate(&first.certificate, at + 29)
            .unwrap(),
        first.identity
    );
    assert_eq!(
        registry
            .authorize_certificate(&rotated.certificate, at + 29)
            .unwrap(),
        first.identity
    );
    assert!(matches!(
        registry.authorize_certificate(&first.certificate, at + 30),
        Err(EnrollmentError::Expired)
    ));
    // The same request again finds the committed rotation (the proof still
    // carries the previous key, retired but authorizing); after the grace it
    // is refused.
    assert!(matches!(
        registry.prepare_renew(&authority, &request, at + 5, 30).unwrap(),
        RenewPreparation::Existing(receipt) if receipt == rotated
    ));
    assert!(matches!(
        registry.prepare_renew(&authority, &request, at + 31, 30),
        Err(EnrollmentError::Expired)
    ));
    // A later rotation request under the previous credential, for yet
    // another key, is refused once the grace passed (checked below).
    let other = JoinKey::open_or_create(dir.path().join("node-other"), [1; 16]).unwrap();
    let stale = material.rotation_request(&key, &other, &first).unwrap();
    let next_request = next.request_id();
    // The holder adopts the rotation: the primary directory now holds the
    // new key and receipt, the staged material is cleared.
    let adopted = key
        .rotate_into(&next, &rotated, authority.issuers().unwrap().trusted(), at)
        .unwrap();
    assert_eq!(adopted.certificate_chain()[0], rotated.certificate);
    drop(key);
    drop(next);
    let reopened = JoinKey::open_or_create(dir.path().join("node"), [1; 16]).unwrap();
    assert_eq!(reopened.request_id(), next_request);
    assert_eq!(reopened.enrollment().unwrap().unwrap(), rotated);
    assert_eq!(reopened.key_identity().unwrap(), rotated.public_key);
    let restaged = JoinKey::open_or_create(dir.path().join("node-next"), [1; 16]).unwrap();
    assert_ne!(restaged.request_id(), next_request);
    assert_ne!(restaged.key_identity().unwrap(), rotated.public_key);
    // Under the new key a renewal is an ordinary renewal, and a second
    // rotation under the old (retired) key is refused once the grace passed.
    let renewal = adopted.renewal_request(&reopened, &rotated).unwrap();
    assert!(!renewal.is_rotation());
    assert!(matches!(
        registry.prepare_renew(&authority, &renewal, at + 40, 30),
        Ok(RenewPreparation::Commit(_))
    ));
    assert!(matches!(
        registry.prepare_renew(&authority, &stale, at + 40, 30),
        Err(EnrollmentError::Expired | EnrollmentError::Unauthorized)
    ));
}

#[test]
fn the_upgrade_fence_rises_once_under_the_founder_authority_and_survives_schema_three_checkpoints()
{
    let dir = tempfile::tempdir().unwrap();
    let founder = authority(&dir, [1; 16]);
    let mut registry = registry(&founder);
    assert_eq!(registry.fence(), UpgradeFence::default());
    // A schema-3 checkpoint (no fence) restores with none.
    let legacy = registry.encode_as_schema_three_for_tests().unwrap();
    let upgraded =
        EnrollmentRegistry::restore(&legacy, founder.cluster(), EnrollmentLimits::default())
            .unwrap();
    assert_eq!(upgraded.fence(), UpgradeFence::default());
    assert_eq!(upgraded.revision(), registry.revision());
    // Zero and a lower level are invalid; the fence rises through a
    // committed command and reports its activation.
    assert!(matches!(
        registry.prepare_activate_fence(&founder, 0, now()),
        Err(EnrollmentError::Invalid)
    ));
    let at = now() + 5;
    let command = registry.prepare_activate_fence(&founder, 2, at).unwrap();
    assert_eq!(command.activated_fence(), Some(2));
    assert_eq!(command.admitted_tenant(), None);
    let revision = registry.revision() + 1;
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert_eq!(
        registry.fence(),
        UpgradeFence {
            level: 2,
            activated_at: at,
            revision,
        }
    );
    // The same level again is a conflict (done); a lower one is invalid;
    // the same command cannot apply twice.
    assert!(matches!(
        registry.prepare_activate_fence(&founder, 2, at + 1),
        Err(EnrollmentError::Conflict)
    ));
    assert!(matches!(
        registry.prepare_activate_fence(&founder, 1, at + 1),
        Err(EnrollmentError::Invalid)
    ));
    assert!(matches!(
        registry.apply_committed(&command, registry.applied_index() + 1),
        Err(EnrollmentError::Conflict)
    ));
    // Another authority cannot raise it; a restore keeps it.
    let other_dir = tempfile::tempdir().unwrap();
    let other = authority(&other_dir, [1; 16]);
    assert!(registry.prepare_activate_fence(&other, 3, at + 1).is_err());
    let bytes = registry.checkpoint().unwrap();
    let restored =
        EnrollmentRegistry::restore(&bytes, founder.cluster(), EnrollmentLimits::default())
            .unwrap();
    assert_eq!(restored.fence().level, 2);
    let higher = registry
        .prepare_activate_fence(&founder, 3, at + 2)
        .unwrap();
    registry
        .apply_committed(&higher, registry.applied_index() + 1)
        .unwrap();
    assert_eq!(registry.fence().level, 3);
    assert_eq!(registry.fence().activated_at, at + 2);
}

/// The founder's genesis identity: the founding registry and receipt, the
/// key they were issued for and the material the key completes with.
fn founder(
    dir: &tempfile::TempDir,
    authority: &BootstrapAuthority,
    name: &str,
    limits: EnrollmentLimits,
) -> (
    EnrollmentRegistry,
    JoinKey,
    EnrollmentReceipt,
    CredentialMaterial,
) {
    let key = JoinKey::open_or_create(dir.path().join(name), [1; 16]).unwrap();
    let (registry, receipt) =
        EnrollmentRegistry::founding(authority, &key, 1, [7; 16], limits, 0, now()).unwrap();
    let material = key
        .complete(&receipt, authority.issuers().unwrap().trusted(), now())
        .unwrap();
    (registry, key, receipt, material)
}

#[test]
fn the_founder_renews_under_its_founding_subject_and_rotates_carrying_its_principal() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let (mut registry, key, genesis, material) =
        founder(&dir, &authority, "founder-key", EnrollmentLimits::default());
    assert_eq!(genesis.revision, 1);
    assert!(crate::pki::founding_principal(&genesis).unwrap());
    // The founder's principal is assigned, not derived from its key.
    assert_ne!(
        genesis.identity,
        assigned(
            authority.cluster(),
            EnrollmentRole::Node,
            1,
            genesis.public_key
        )
    );
    let request = material.renewal_request(&key, &genesis).unwrap();
    let at = now() + 10;
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, at, 30)
        .unwrap()
    else {
        panic!("the founder's first renewal commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&request, at).unwrap();
    // The same identity, principal and key under a fresh certificate that
    // carries the founding subject at its new revision.
    assert_eq!(renewed.identity, genesis.identity);
    assert_eq!(renewed.public_key, genesis.public_key);
    assert_eq!(renewed.revision, 2);
    assert_eq!(renewed.issued_at, at);
    assert_eq!(
        renewed.expires_at,
        at + EnrollmentLimits::default().credential_lifetime as i64
    );
    assert!(crate::pki::founding_principal(&renewed).unwrap());
    assert!(crate::pki::identity_bound(&renewed).unwrap());
    assert_eq!(
        registry
            .authorize_certificate(&renewed.certificate, at + 31)
            .unwrap(),
        genesis.identity
    );
    assert!(matches!(
        registry.authorize_certificate(&genesis.certificate, at + 30),
        Err(EnrollmentError::Expired)
    ));
    // A checkpoint of the renewed registry restores and still authorizes it:
    // the founding subject is accepted at every revision.
    let restored = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        authority.cluster(),
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert_eq!(
        restored
            .authorize_certificate(&renewed.certificate, at + 31)
            .unwrap(),
        genesis.identity
    );
    // The holder installs the renewal over the genesis receipt under its key.
    let material = key
        .renew(&renewed, authority.issuers().unwrap().trusted(), at)
        .unwrap();
    assert_eq!(material.certificate_chain()[0], renewed.certificate);
    assert_eq!(key.enrollment().unwrap().unwrap(), renewed);
    // A rotation of the founder's key carries the assigned principal to the
    // new key in a CA-signed subject, as any rotation does.
    let next = JoinKey::open_or_create(dir.path().join("founder-key.next"), [1; 16]).unwrap();
    let rotation = material.rotation_request(&key, &next, &renewed).unwrap();
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &rotation, at + 5, 30)
        .unwrap()
    else {
        panic!("the founder's rotation commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let rotated = registry.release_renewal(&rotation, at + 5).unwrap();
    assert_eq!(rotated.identity, genesis.identity);
    assert_eq!(rotated.public_key, csr_key_hash(next.csr()).unwrap());
    assert!(crate::pki::carried_principal(&rotated).unwrap());
    assert!(!crate::pki::founding_principal(&rotated).unwrap());
    let rotated_material = key
        .rotate_into(
            &next,
            &rotated,
            authority.issuers().unwrap().trusted(),
            at + 5,
        )
        .unwrap();
    assert_eq!(rotated_material.certificate_chain()[0], rotated.certificate);
    drop(key);
    drop(next);
    // The key directory now holds the rotated key and its receipt.
    let key = JoinKey::open_or_create(dir.path().join("founder-key"), [1; 16]).unwrap();
    assert_eq!(key.key_identity().unwrap(), rotated.public_key);
    assert_eq!(key.enrollment().unwrap().unwrap(), rotated);
    // Under the rotated key a renewal carries the principal again.
    let again = rotated_material.renewal_request(&key, &rotated).unwrap();
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &again, at + 10, 30)
        .unwrap()
    else {
        panic!("a renewal after the rotation commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed_again = registry.release_renewal(&again, at + 10).unwrap();
    assert!(crate::pki::carried_principal(&renewed_again).unwrap());
    assert_eq!(renewed_again.public_key, rotated.public_key);
}

#[test]
fn a_restored_registry_keeps_the_lifetimes_it_committed_and_the_capacities_it_is_given() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let committed = EnrollmentLimits {
        credential_lifetime: 3600,
        max_invitation_lifetime: 600,
        ..EnrollmentLimits::default()
    };
    let (registry, _key, genesis, _material) =
        founder(&dir, &authority, "hour-key", committed.clone());
    assert_eq!(genesis.expires_at - genesis.issued_at, 3600);
    let checkpoint = registry.checkpoint().unwrap();
    // A process restoring under the standard limits adopts the committed
    // lifetimes: they are the cluster's policy, not the process's bound.
    let restored =
        EnrollmentRegistry::restore(&checkpoint, [1; 16], EnrollmentLimits::default()).unwrap();
    assert_eq!(restored.limits(), &committed);
    // A capacity that differs from the restoring process's is refused, as before.
    let smaller = EnrollmentLimits {
        max_enrollments: 16,
        ..EnrollmentLimits::default()
    };
    assert!(matches!(
        EnrollmentRegistry::restore(&checkpoint, [1; 16], smaller),
        Err(EnrollmentError::Corrupt)
    ));
    // A lifetime the registry would not admit cannot be founded.
    let key = JoinKey::open_or_create(dir.path().join("short-key"), [1; 16]).unwrap();
    for lifetime in [0, MIN_CREDENTIAL_LIFETIME - 1, MAX_CREDENTIAL_LIFETIME + 1] {
        assert!(matches!(
            EnrollmentRegistry::founding(
                &authority,
                &key,
                1,
                [7; 16],
                EnrollmentLimits {
                    credential_lifetime: lifetime,
                    ..EnrollmentLimits::default()
                },
                0,
                now(),
            ),
            Err(EnrollmentError::Capacity)
        ));
    }
    // The shortest lifetime founds, and a renewal in its second second extends it.
    let shortest = EnrollmentLimits {
        credential_lifetime: MIN_CREDENTIAL_LIFETIME,
        ..EnrollmentLimits::default()
    };
    let (mut registry, key, genesis, material) =
        founder(&dir, &authority, "shortest-key", shortest);
    let request = material.renewal_request(&key, &genesis).unwrap();
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, genesis.issued_at + 1, 30)
        .unwrap()
    else {
        panic!("a renewal a second after the issue commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry
        .release_renewal(&request, genesis.issued_at + 1)
        .unwrap();
    assert_eq!(renewed.expires_at, genesis.expires_at + 1);
}

#[test]
fn the_bootstrap_server_certificate_is_recorded_staged_and_presented_once_older_invitations_close()
{
    let dir = tempfile::tempdir().unwrap();
    let mut authority = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        3600,
        EnrollmentLimits::issuer_lifetime_for(3600),
        now(),
    )
    .unwrap();
    // The certificate lasts the lifetime it was issued for, and a founding
    // registry names it from the start.
    let (issued_at, expires_at) = authority.server_validity().unwrap();
    assert_eq!(expires_at - issued_at, 3600);
    let (mut registry, _key, _genesis, _material) =
        founder(&dir, &authority, "founder-key", EnrollmentLimits::default());
    let genesis_record = ServerRecord::of(authority.server_certificate()).unwrap();
    assert_eq!(registry.bootstrap().current, genesis_record);
    assert!(registry.bootstrap().successor.is_none());
    assert!(
        registry
            .prepare_bootstrap_server(&authority, now())
            .unwrap()
            .is_none()
    );
    // An invitation issued before the staging pins the current certificate
    // alone.
    let older = invite(&mut registry, &authority, EnrollmentRole::Node);
    assert_eq!(older.trust().successor_fingerprint, None);
    // The authority stages a successor (once; asked again it is the same),
    // and the registry commits it with the invitations open at the time.
    let at = now();
    let successor = authority.stage_successor(at, 3600).unwrap();
    assert_eq!(authority.stage_successor(at + 5, 3600).unwrap(), successor);
    assert_ne!(server_fingerprint(&successor), genesis_record.fingerprint);
    let command = registry
        .prepare_bootstrap_server(&authority, at)
        .unwrap()
        .unwrap();
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let staged = registry.bootstrap().successor.clone().unwrap();
    assert_eq!(staged.record.fingerprint, server_fingerprint(&successor));
    assert_eq!(staged.staged_at, at);
    assert_eq!(staged.awaiting, BTreeSet::from([older.id()]));
    assert_eq!(registry.bootstrap().current, genesis_record);
    // Not presented while that invitation is open: nothing to commit.
    assert!(!registry.bootstrap_ready_to_activate(at + 1));
    assert!(
        registry
            .prepare_bootstrap_server(&authority, at + 1)
            .unwrap()
            .is_none()
    );
    // An invitation issued now carries both pins, so it redeems whichever
    // certificate the founder presents; the older one accepts only the
    // current.
    let newer = invite(&mut registry, &authority, EnrollmentRole::Client);
    assert_eq!(
        newer.trust().successor_fingerprint,
        Some(server_fingerprint(&successor))
    );
    newer
        .trust()
        .verify_chain(&[authority.server_certificate().to_vec().into()], now())
        .unwrap();
    newer
        .trust()
        .verify_chain(&[successor.clone().into()], now())
        .unwrap();
    assert!(matches!(
        older
            .trust()
            .verify_chain(&[successor.clone().into()], now()),
        Err(EnrollmentError::Unauthorized)
    ));
    // The newer invitation was issued after the staging: it is not awaited.
    assert_eq!(
        registry.bootstrap().successor.as_ref().unwrap().awaiting,
        BTreeSet::from([older.id()])
    );
    // Once the older invitation has expired the successor is activated:
    // the registry names it current, and the authority presents it.
    let later = older.expires_at() + 1;
    assert!(registry.bootstrap_ready_to_activate(later));
    let command = registry
        .prepare_bootstrap_server(&authority, later)
        .unwrap()
        .unwrap();
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert_eq!(
        registry.bootstrap().current,
        ServerRecord::of(&successor).unwrap()
    );
    assert!(registry.bootstrap().successor.is_none());
    // The activation committed and the authority has yet to present it: the
    // registry has nothing more to say (a crash here is reconciled by
    // presenting).
    assert!(
        registry
            .prepare_bootstrap_server(&authority, later)
            .unwrap()
            .is_none()
    );
    let identity = authority.activate_successor().unwrap();
    assert_eq!(identity.certificate_chain()[0], successor);
    assert_eq!(authority.server_certificate(), successor.as_slice());
    assert!(authority.successor().is_none());
    assert!(matches!(
        authority.activate_successor(),
        Err(EnrollmentError::NotCommitted)
    ));
    assert!(
        registry
            .prepare_bootstrap_server(&authority, later)
            .unwrap()
            .is_none()
    );
    // Reopened, the authority holds the successor as its certificate.
    drop(authority);
    let authority = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        3600,
        EnrollmentLimits::issuer_lifetime_for(3600),
        now(),
    )
    .unwrap();
    assert_eq!(authority.server_certificate(), successor.as_slice());
    // A move the registry does not admit is refused: a stage over a current
    // it does not name, a successor equal to the current, an activation of
    // nothing.
    for change in [
        Change::BootstrapServer {
            current: genesis_record,
            successor: None,
        },
        Change::BootstrapServer {
            current: registry.bootstrap().current,
            successor: Some(StagedRecord {
                record: registry.bootstrap().current,
                staged_at: later,
                awaiting: BTreeSet::new(),
            }),
        },
    ] {
        let mut copy = registry.clone();
        let command = EnrollmentCommand::for_tests(copy.revision(), later, change);
        assert!(matches!(
            copy.apply_committed(&command, copy.applied_index() + 1),
            Err(EnrollmentError::Invalid)
        ));
    }
    // The checkpoint restores the record; a schema-4 checkpoint names no
    // certificate, and the founder records the one it holds.
    let restored = EnrollmentRegistry::restore(
        &registry.checkpoint().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert_eq!(restored.bootstrap(), registry.bootstrap());
    let mut legacy = EnrollmentRegistry::restore(
        &registry.encode_as_schema_four_for_tests().unwrap(),
        [1; 16],
        EnrollmentLimits::default(),
    )
    .unwrap();
    assert!(legacy.bootstrap().current.is_unknown());
    let command = legacy
        .prepare_bootstrap_server(&authority, later)
        .unwrap()
        .unwrap();
    legacy
        .apply_committed(&command, legacy.applied_index() + 1)
        .unwrap();
    assert_eq!(legacy.bootstrap(), registry.bootstrap());
    // A schema-1 authority bundle opens with no successor and is written
    // forward; a schema-1 token decodes to the same invitation with the
    // fingerprint that schema bound.
    authority.save_as_schema_one_for_tests().unwrap();
    drop(authority);
    let authority = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        3600,
        EnrollmentLimits::issuer_lifetime_for(3600),
        now(),
    )
    .unwrap();
    assert_eq!(authority.server_certificate(), successor.as_slice());
    assert!(authority.successor().is_none());
    let mut fresh = self::registry(&authority);
    let plain = invite(&mut fresh, &authority, EnrollmentRole::Node);
    let legacy_token = plain.expose_token_as_schema_one_for_tests().unwrap();
    let decoded = Invitation::parse(&legacy_token).unwrap();
    assert_eq!(decoded.id(), plain.id());
    assert_eq!(decoded.trust(), plain.trust());
    assert_eq!(decoded.data.schema, 1);
    let one_pin = ServerTrustV1 {
        endpoint: plain.trust().endpoint.clone(),
        server_name: plain.trust().server_name.clone(),
        ca_certificate: plain.trust().ca_certificate.clone(),
        server_fingerprint: plain.trust().server_fingerprint,
    };
    assert_eq!(
        decoded.data.trust_fingerprint().unwrap(),
        hash(
            "focal.enrollment.server-trust.v1",
            &encode(&one_pin).unwrap()
        )
    );
    assert_ne!(
        decoded.data.trust_fingerprint().unwrap(),
        plain.data.trust_fingerprint().unwrap()
    );
}

/// The issuer succeeds itself (24 §11, the audit's F13 stage 3): a
/// successor the authority issues and the current issuer endorses is
/// staged — committed, so every verifier trusts it before anything is
/// issued under it — then activated; a credential renewed under it presents
/// the endorsed chain; the predecessor retires once nothing live was issued
/// under it, the bootstrap server certificate included; older checkpoints
/// restore with the genesis issuer alone; the moves refuse what they must.
#[test]
fn the_issuer_succeeds_itself_endorsed_by_its_predecessor_and_retires_once_nothing_live_was_issued_under_it()
 {
    let dir = tempfile::tempdir().unwrap();
    let lifetime = 3600u64;
    let issuer_lifetime = EnrollmentLimits::issuer_lifetime_for(lifetime);
    let limits = EnrollmentLimits {
        credential_lifetime: lifetime,
        issuer_lifetime,
        ..EnrollmentLimits::default()
    };
    let mut authority = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        lifetime,
        issuer_lifetime,
        now(),
    )
    .unwrap();
    let (issued_at, expires_at) = authority.issuer_validity().unwrap();
    assert_eq!(expires_at - issued_at, issuer_lifetime as i64);
    let (mut registry, _founder_key, founder_receipt, _material) =
        founder(&dir, &authority, "founder-key", limits.clone());
    // The genesis issuer alone, named from the start, unendorsed.
    let genesis = registry.issuers().clone();
    assert_eq!(genesis.current.certificate, authority.ca_certificate());
    assert!(genesis.current.endorsement.is_none());
    assert!(genesis.successor.is_none() && genesis.retiring.is_none());
    assert_eq!(genesis.current, authority.issuer_record().unwrap());
    assert!(
        registry
            .prepare_issuer(&authority, now())
            .unwrap()
            .is_none()
    );
    let early = registry.clone();
    // A credential issued under it chains to it alone.
    let (node_key, receipt, material) = enroll_node(&dir, &mut registry, &authority, "node-key");
    assert_eq!(material.certificate_chain().len(), 2);
    // Staged: a fresh issuer, endorsed by the genesis issuer (once; asked
    // again it is the same), committed with when it was staged.
    let at = now();
    let staged = authority.stage_issuer(at, issuer_lifetime).unwrap();
    assert_eq!(
        authority.stage_issuer(at + 5, issuer_lifetime).unwrap(),
        staged
    );
    assert_ne!(staged.fingerprint, genesis.current.fingerprint);
    assert_eq!(staged.expires_at - staged.issued_at, issuer_lifetime as i64);
    let endorsement = staged.endorsement.clone().unwrap();
    assert!(focal_wire::issued_by(&endorsement, authority.ca_certificate()).unwrap());
    assert!(crate::pki::endorses(&endorsement, &staged.certificate).unwrap());
    let command = registry.prepare_issuer(&authority, at).unwrap().unwrap();
    assert!(matches!(
        command.change(),
        Change::Issuer(IssuerChange::Stage(s)) if s.record == staged && s.staged_at == at
    ));
    // A staging whose endorsement is not the current issuer's is refused.
    let stranger = BootstrapAuthority::open_or_create_for(
        dir.path().join("stranger"),
        [1; 16],
        vec!["localhost".into()],
        lifetime,
        issuer_lifetime,
        at,
    )
    .unwrap();
    let forged = command
        .clone()
        .with_change(Change::Issuer(IssuerChange::Stage(StagedIssuer {
            record: IssuerRecord {
                endorsement: Some(stranger.ca_certificate().to_vec()),
                ..staged.clone()
            },
            staged_at: at,
        })));
    assert!(matches!(
        registry.apply_committed(&forged, registry.applied_index() + 1),
        Err(EnrollmentError::Invalid)
    ));
    // Nor an activation or a retirement with nothing staged or retiring.
    for change in [IssuerChange::Activate, IssuerChange::Retire] {
        let premature = command.clone().with_change(Change::Issuer(change));
        assert!(matches!(
            registry.apply_committed(&premature, registry.applied_index() + 1),
            Err(EnrollmentError::Invalid)
        ));
    }
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert_eq!(
        registry.issuers().successor.as_ref().unwrap().record,
        staged
    );
    assert_eq!(registry.issuers().current, genesis.current);
    assert_eq!(registry.trust_roots().count(), 2);
    // An invitation issued now carries both issuers; a credential is still
    // the genesis issuer's until the activation.
    let invitation = invite(&mut registry, &authority, EnrollmentRole::Node);
    assert_eq!(invitation.trust().issuers.len(), 2);
    assert_eq!(
        invitation.trust().ca_certificate,
        genesis.current.certificate
    );
    // Activated at the next step: the successor issues, the genesis issuer
    // retires once nothing live was issued under it.
    let command = registry
        .prepare_issuer(&authority, at + 1)
        .unwrap()
        .unwrap();
    assert!(matches!(
        command.change(),
        Change::Issuer(IssuerChange::Activate)
    ));
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert_eq!(registry.issuers().current, staged);
    assert_eq!(registry.issuers().retiring, Some(genesis.current.clone()));
    assert!(registry.issuers().successor.is_none());
    // The activation committed; the authority has yet to adopt it: nothing
    // more to commit until it does.
    assert!(
        registry
            .prepare_issuer(&authority, at + 2)
            .unwrap()
            .is_none()
    );
    authority.activate_issuer().unwrap();
    assert_eq!(authority.issuer_record().unwrap(), staged);
    assert!(authority.issuer_successor().unwrap().is_none());
    // The bootstrap server certificate is still the genesis issuer's, and
    // its chain says so.
    let server = authority.server_identity();
    assert_eq!(server.certificate_chain().len(), 2);
    assert_eq!(server.certificate_chain()[1], genesis.current.certificate);
    // A renewal is issued under the successor and presents the endorsed
    // chain: the certificate, the successor, its endorsement.
    let at = at + 3;
    let request = material.renewal_request(&node_key, &receipt).unwrap();
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &request, at, 30)
        .unwrap()
    else {
        panic!("a renewal under the successor commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&request, at).unwrap();
    assert!(focal_wire::issued_by(&renewed.certificate, &staged.certificate).unwrap());
    let material = node_key
        .renew(&renewed, registry.issuers().trusted(), at)
        .unwrap();
    assert_eq!(material.certificate_chain().len(), 3);
    assert_eq!(material.certificate_chain()[1], staged.certificate);
    assert_eq!(material.certificate_chain()[2], endorsement);
    // The genesis issuer is in use while a credential issued under it lives
    // — the founder's, the retired previous certificate — and while the
    // bootstrap server certificate is under it: nothing to commit.
    assert!(registry.retiring_issuer_in_use(&authority, at));
    assert!(registry.prepare_issuer(&authority, at).unwrap().is_none());
    // The bootstrap server certificate moves under the successor: staged
    // because it is not under the issuer, activated once the invitation
    // open at the staging has closed.
    authority.stage_successor(at, lifetime).unwrap();
    let command = registry
        .prepare_bootstrap_server(&authority, at)
        .unwrap()
        .unwrap();
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let closed = at + 700;
    assert!(registry.bootstrap_ready_to_activate(closed));
    let command = registry
        .prepare_bootstrap_server(&authority, closed)
        .unwrap()
        .unwrap();
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let presented = authority.activate_successor().unwrap();
    assert_eq!(presented.certificate_chain().len(), 3);
    assert_eq!(presented.certificate_chain()[1], staged.certificate);
    assert!(focal_wire::issued_by(authority.server_certificate(), &staged.certificate).unwrap());
    // Still in use: the founder's genesis-issued credential lives.
    assert!(registry.retiring_issuer_in_use(&authority, closed));
    assert!(closed < founder_receipt.expires_at);
    // Once every credential issued under it has expired, it retires: the
    // successor is the one trusted issuer.
    let later = founder_receipt.expires_at.max(receipt.expires_at) + 1;
    assert!(!registry.retiring_issuer_in_use(&authority, later));
    let command = registry.prepare_issuer(&authority, later).unwrap().unwrap();
    assert!(matches!(
        command.change(),
        Change::Issuer(IssuerChange::Retire)
    ));
    // A retirement decided while one still lived would be refused.
    let too_early = command.clone().with_decided_at(closed);
    assert!(matches!(
        registry.apply_committed(&too_early, registry.applied_index() + 1),
        Err(EnrollmentError::Invalid)
    ));
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    assert!(registry.issuers().retiring.is_none());
    assert_eq!(registry.trust_roots().count(), 1);
    assert_eq!(registry.issuers().current, staged);
    assert!(
        registry
            .prepare_issuer(&authority, later)
            .unwrap()
            .is_none()
    );
    // The authority holds what the registry names.
    let held = authority.issuers().unwrap();
    assert_eq!(held.current, staged);
    assert!(held.successor.is_none() && held.retiring.is_none());
    // The genesis issuer stays the cluster's identity.
    assert_eq!(registry.ca_certificate(), genesis.current.certificate);
    assert!(
        registry
            .prepare_invitation(
                &authority,
                InviteOptions {
                    endpoint: "127.0.0.1:8443".into(),
                    server_name: "localhost".into(),
                    role: EnrollmentRole::Node,
                    expires_at: later + 600,
                },
                later,
            )
            .is_ok()
    );
    // The succession restores; a schema-5 checkpoint restores with the
    // genesis issuer alone.
    let restored =
        EnrollmentRegistry::restore(&registry.checkpoint().unwrap(), [1; 16], limits.clone())
            .unwrap();
    assert_eq!(restored.issuers(), registry.issuers());
    assert_eq!(restored.charged_bytes(), registry.charged_bytes());
    let five = EnrollmentRegistry::restore(
        &early.encode_as_schema_five_for_tests().unwrap(),
        [1; 16],
        limits.clone(),
    )
    .unwrap();
    assert_eq!(five.issuers(), early.issuers());
    assert_eq!(five.charged_bytes(), early.charged_bytes());
    assert_eq!(five.limits().issuer_lifetime, issuer_lifetime);
    // The authority reopens on the successor with the bundle it saved.
    drop(authority);
    let reopened = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        lifetime,
        issuer_lifetime,
        later,
    )
    .unwrap();
    assert_eq!(reopened.issuer_record().unwrap(), staged);
    assert_eq!(reopened.issuer_certificate(), staged.certificate);
    assert_eq!(reopened.ca_certificate(), genesis.current.certificate);
    assert_eq!(reopened.server_identity().certificate_chain().len(), 3);
}

/// A trust holding the genesis issuer alone verifies the bootstrap server's
/// chain under a successor through the endorsement, and refuses a chain
/// without it or endorsed by a stranger (24 §11).
#[test]
fn an_older_trust_verifies_an_endorsed_chain_and_refuses_an_unendorsed_or_forged_one() {
    let dir = tempfile::tempdir().unwrap();
    let lifetime = 3600u64;
    let issuer_lifetime = EnrollmentLimits::issuer_lifetime_for(lifetime);
    let mut authority = BootstrapAuthority::open_or_create_for(
        dir.path().join("authority"),
        [1; 16],
        vec!["localhost".into()],
        lifetime,
        issuer_lifetime,
        now(),
    )
    .unwrap();
    let (registry, _key, _genesis, _material) =
        founder(&dir, &authority, "founder-key", EnrollmentLimits::default());
    let older = invite(&mut registry.clone(), &authority, EnrollmentRole::Node);
    assert_eq!(older.trust().issuers.len(), 1);
    let at = now();
    let staged = authority.stage_issuer(at, issuer_lifetime).unwrap();
    authority.activate_issuer().unwrap();
    authority.stage_successor(at, lifetime).unwrap();
    let presented = authority.activate_successor().unwrap();
    let chain: Vec<rustls::pki_types::CertificateDer<'_>> = presented
        .certificate_chain()
        .iter()
        .map(|certificate| certificate.clone().into())
        .collect();
    assert_eq!(chain.len(), 3);
    // Through the endorsement: accepted. The pin is the staged server
    // certificate's, which the older trust does not carry, so the pin
    // alone refuses; a trust pinning it verifies the chain.
    let pinned = ServerTrust {
        server_fingerprint: server_fingerprint(authority.server_certificate()),
        ..older.trust().clone()
    };
    pinned.verify_chain(&chain, at).unwrap();
    assert!(matches!(
        older.trust().verify_chain(&chain, at),
        Err(EnrollmentError::Unauthorized)
    ));
    // Without the endorsement: refused.
    assert!(matches!(
        pinned.verify_chain(&chain[..2], at),
        Err(EnrollmentError::Unauthorized)
    ));
    // Endorsed by a stranger: refused.
    let stranger = BootstrapAuthority::open_or_create_for(
        dir.path().join("stranger"),
        [1; 16],
        vec!["localhost".into()],
        lifetime,
        issuer_lifetime,
        at,
    )
    .unwrap();
    let forged: Vec<rustls::pki_types::CertificateDer<'_>> = vec![
        chain[0].clone(),
        chain[1].clone(),
        stranger.ca_certificate().to_vec().into(),
    ];
    assert!(matches!(
        pinned.verify_chain(&forged, at),
        Err(EnrollmentError::Unauthorized)
    ));
    // A trust that holds the successor needs no endorsement.
    let newer = ServerTrust {
        issuers: vec![staged.clone()],
        ..pinned.clone()
    };
    newer.verify_chain(&chain[..1], at).unwrap();
    // More certificates than a chain may carry: refused.
    let padded: Vec<rustls::pki_types::CertificateDer<'_>> =
        std::iter::repeat_n(chain[1].clone(), 6).collect();
    assert!(pinned.verify_chain(&padded, at).is_err());
}

/// Closed records leave the registry (the audit's F22): past the later of
/// its invitation's expiry and its credential's, nothing of a record can
/// regain meaning — a token is expired, a certificate expired, a revocation
/// holds by time — so a cluster's onboarding history never exhausts the
/// bound its live population is held to. The bound holds while records are
/// open; a renewal keeps its record; a compacted token or certificate is
/// unknown, which never redeems or authorizes; restores rebuild the index
/// and an older checkpoint restores with nothing compacted.
#[test]
fn closed_records_compact_so_onboarding_outlives_the_active_bound() {
    let dir = tempfile::tempdir().unwrap();
    let authority = authority(&dir, [1; 16]);
    let limits = EnrollmentLimits {
        max_invitations: 8,
        max_enrollments: 8,
        credential_lifetime: 3600,
        issuer_lifetime: EnrollmentLimits::issuer_lifetime_for(3600),
        ..EnrollmentLimits::default()
    };
    let (mut registry, _founder_key, founder_receipt, _material) =
        founder(&dir, &authority, "founder-key", limits.clone());
    let t0 = now();
    let invite_at = |registry: &mut EnrollmentRegistry, at: i64| {
        let draft = registry.prepare_invitation(
            &authority,
            InviteOptions {
                endpoint: "127.0.0.1:8443".into(),
                server_name: "localhost".into(),
                role: EnrollmentRole::Node,
                expires_at: at + 600,
            },
            at,
        )?;
        registry
            .apply_committed(draft.command(), registry.applied_index() + 1)
            .unwrap();
        Ok::<_, EnrollmentError>(draft.release(registry).unwrap())
    };
    // The floor moves with any committed decision.
    let advance = |registry: &mut EnrollmentRegistry, at: i64, tenant: u8| {
        let command = registry
            .prepare_admit_tenant(&authority, [tenant; 16], at)
            .unwrap();
        registry
            .apply_committed(&command, registry.applied_index() + 1)
            .unwrap();
    };
    // The founder's record and seven invitations fill the bound; the
    // eighth invitation is refused for capacity.
    let first: Vec<Invitation> = (0..7)
        .map(|_| invite_at(&mut registry, t0).unwrap())
        .collect();
    assert_eq!(registry.enrollments().count(), 1);
    assert!(matches!(
        invite_at(&mut registry, t0),
        Err(EnrollmentError::Capacity)
    ));
    let full = registry.checkpoint().unwrap().len();
    assert_eq!(registry.compacted(), 0);
    // Once they expired, the unredeemed invitations are closed and leave
    // the table with the next committed decision; the bound is free again.
    let t1 = t0 + 601;
    advance(&mut registry, t1, 9);
    assert_eq!(registry.compacted(), 7);
    assert!(registry.invitation_status(first[0].id()).is_none());
    // An expired token of a compacted invitation is unknown: it never
    // redeems.
    let stale_key = JoinKey::open_or_create(dir.path().join("stale"), [1; 16]).unwrap();
    let stale = first[1].request(&stale_key, t1).unwrap();
    assert!(matches!(
        registry.prepare_join(&authority, &stale, t1),
        Err(EnrollmentError::Unauthorized | EnrollmentError::Expired)
    ));
    // A second generation: one consumed, one revoked, the rest left open.
    let second: Vec<Invitation> = (0..7)
        .map(|_| invite_at(&mut registry, t1).unwrap())
        .collect();
    let key = JoinKey::open_or_create(dir.path().join("node"), [1; 16]).unwrap();
    let request = second[0].request(&key, t1).unwrap();
    let prepared = registry.prepare_join(&authority, &request, t1).unwrap();
    commit(&mut registry, prepared);
    let receipt = registry.release(&request, t1).unwrap();
    let material = key
        .complete(&receipt, registry.issuers().trusted(), t1)
        .unwrap();
    let revoke = registry.prepare_revoke(second[1].id(), t1).unwrap();
    registry
        .apply_committed(&revoke, registry.applied_index() + 1)
        .unwrap();
    assert!(registry.invitation_revoked(second[1].id()).unwrap());
    // Past the second generation's expiry: the open and the revoked
    // invitations are closed and gone; the consumed one lives with its
    // credential; a replay of the revoked token is unknown, which never
    // redeems.
    let t2 = t1 + 601;
    advance(&mut registry, t2, 10);
    assert_eq!(registry.compacted(), 13);
    assert!(registry.invitation_status(second[1].id()).is_none());
    assert!(registry.invitation_status(second[0].id()).is_some());
    let revoked_key = JoinKey::open_or_create(dir.path().join("revoked"), [1; 16]).unwrap();
    let replay = second[1].request(&revoked_key, t2).unwrap();
    assert!(matches!(
        registry.prepare_join(&authority, &replay, t2),
        Err(EnrollmentError::Unauthorized | EnrollmentError::Expired | EnrollmentError::Revoked)
    ));
    assert_eq!(
        registry
            .authorize_certificate(&receipt.certificate, t2)
            .unwrap(),
        receipt.identity
    );
    // A renewal moves the record's closing with the credential: past the
    // first credential's expiry the renewed record stays.
    let renewal = material.renewal_request(&key, &receipt).unwrap();
    let RenewPreparation::Commit(command) = registry
        .prepare_renew(&authority, &renewal, t2, 30)
        .unwrap()
    else {
        panic!("a renewal commits")
    };
    registry
        .apply_committed(&command, registry.applied_index() + 1)
        .unwrap();
    let renewed = registry.release_renewal(&renewal, t2).unwrap();
    let t3 = t1 + 3601;
    assert!(t3 > founder_receipt.expires_at);
    advance(&mut registry, t3, 11);
    // The founder's own unrenewed credential closed too: its record is
    // gone with the first generation's; the renewed node's stays.
    assert_eq!(registry.compacted(), 14);
    assert_eq!(registry.enrollments().count(), 1);
    assert_eq!(
        registry
            .authorize_certificate(&renewed.certificate, t3)
            .unwrap(),
        renewed.identity
    );
    // The index is derived from the records: a restore rebuilds it and
    // carries the count; a schema-6 checkpoint restores with none counted
    // and compacts the same records from there.
    let restored =
        EnrollmentRegistry::restore(&registry.checkpoint().unwrap(), [1; 16], limits.clone())
            .unwrap();
    assert_eq!(restored.compacted(), 14);
    assert_eq!(restored.charged_bytes(), registry.charged_bytes());
    let mut six = EnrollmentRegistry::restore(
        &registry.encode_as_schema_six_for_tests().unwrap(),
        [1; 16],
        limits.clone(),
    )
    .unwrap();
    assert_eq!(six.compacted(), 0);
    assert_eq!(six.enrollments().count(), 1);
    // Past the renewed credential's expiry, that record closes as well:
    // the certificate is unknown, which never authorizes.
    let t4 = t2 + 3601;
    for registry in [&mut registry, &mut six] {
        advance(registry, t4, 12);
        assert_eq!(registry.enrollments().count(), 0);
        assert!(
            registry
                .authorize_certificate(&renewed.certificate, t4)
                .is_err()
        );
        assert!(registry.retired(t4).next().is_none());
    }
    assert_eq!(registry.compacted(), 15);
    assert_eq!(six.compacted(), 1);
    // A lifetime of onboarding past the bound, and the checkpoint never
    // grows past a full table.
    for generation in 0..6_i64 {
        let at = t4 + 1 + generation * 700;
        for _ in 0..7 {
            invite_at(&mut registry, at).unwrap();
        }
        assert!(registry.checkpoint().unwrap().len() <= full);
        advance(&mut registry, at + 601, 20 + generation as u8);
    }
    assert_eq!(registry.compacted(), 15 + 42);
    assert!(registry.checkpoint().unwrap().len() < full);
}
