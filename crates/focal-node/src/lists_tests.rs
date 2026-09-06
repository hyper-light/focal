use super::*;
use crate::{config::Settings, demo, embedded::EmbeddedNode};
use focal_ledger::Submission;

fn fixture(path: &std::path::Path) -> EmbeddedNode {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(path.to_owned());
    let mut node = EmbeddedNode::open(&settings).unwrap();
    demo::run(&mut node).unwrap();
    node
}
fn peer(node: &EmbeddedNode) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: node.identity.issuer,
        tenants: [node.identity.ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn page(
    views: &mut ReadViews,
    node: &mut EmbeddedNode,
    request: &ListRequest,
) -> Result<ListPage, AccessError> {
    let principal = peer(node);
    let scope = list_scope(&principal, node.identity.ledger, &request.filter).unwrap();
    views.list(
        &mut node.session,
        ListReadContext {
            principal: principal.principal(),
            scope,
            request_id: RequestId::from_u128(1),
            barrier: None,
        },
        request,
        &WireLimits::default(),
    )
}
fn query(kind: ObjectKind) -> ListRequest {
    ListRequest {
        filter: ListFilter::new(kind),
        cursor: None,
        max_items: 100,
        max_visits: 100,
    }
}
fn submit(
    node: &mut EmbeddedNode,
    label: &str,
    command: Command,
    evidence: Vec<EvidenceAttestation>,
) {
    let actor = match &command {
        Command::RegisterArtifact { artifact } => artifact.content.producer,
        _ => node.identity.issuer,
    };
    let input = demo::request(&node.identity, label, actor, command, evidence);
    let result = node.session.submit_local(&input).unwrap();
    assert!(matches!(result, Submission::Committed(_)), "{result:?}");
}
fn add_claim(node: &mut EmbeddedNode, id: u128) {
    let mut claim = demo::claim(&node.identity, ClaimId::from_u128(id)).unwrap();
    claim
        .validations
        .retain(|value| value.content.kind == ValidationKind::Receipt);
    for validation in &mut claim.validations {
        validation.id = ValidationId::from_u128(id + 10_000);
    }
    claim.content.requirements = claim
        .validations
        .iter()
        .map(|value| RequirementRef {
            id: value.id,
            specification: value.content.specification_hash().unwrap(),
        })
        .collect();
    claim.content.occurrence = OccurrenceId::from_u128(id);
    submit(
        node,
        &format!("list-claim-{id}"),
        Command::GenerateClaim { claim },
        vec![],
    );
}

#[test]
fn all_families_and_conjunctive_indexes_preserve_manifest_membership() {
    let root = tempfile::tempdir().unwrap();
    let mut node = fixture(root.path());
    let mut views = ReadViews::new();
    for (kind, count) in [
        (ObjectKind::Claim, 1),
        (ObjectKind::Testament, 1),
        (ObjectKind::Validation, 2),
        (ObjectKind::Artifact, 1),
    ] {
        let result = page(&mut views, &mut node, &query(kind)).unwrap();
        assert_eq!(result.objects.len(), count);
        assert!(result.next.is_none());
    }
    let claim = *node
        .session
        .read_at_least(SessionSeq(0))
        .unwrap()
        .claims
        .keys()
        .next()
        .unwrap();
    let testament = *node
        .session
        .read_at_least(SessionSeq(0))
        .unwrap()
        .testaments
        .keys()
        .next()
        .unwrap();
    let mut claims = query(ObjectKind::Claim);
    claims.filter.claim = Some(claim);
    claims.filter.source = Some(node.identity.issuer);
    claims.filter.target = Some(node.identity.worker);
    claims.filter.action = Some(ActionType::Work);
    claims.filter.status = Some(ClaimStatus::Satisfied);
    assert_eq!(
        page(&mut views, &mut node, &claims).unwrap().objects.len(),
        1
    );
    claims.filter.status = Some(ClaimStatus::Posted);
    assert!(
        page(&mut views, &mut node, &claims)
            .unwrap()
            .objects
            .is_empty()
    );
    claims.filter.claim = None;
    claims.filter.status = Some(ClaimStatus::Satisfied);
    assert_eq!(
        page(&mut views, &mut node, &claims).unwrap().objects.len(),
        1
    );
    let mut validations = query(ObjectKind::Validation);
    validations.filter.claim = Some(claim);
    validations.filter.evaluator = Some(node.identity.evaluator);
    validations.filter.validation_kind = Some(ValidationKind::Test);
    validations.filter.phase = Some(ValidationPhase::WholeWork);
    validations.filter.mode = Some(ValidationMode::Required);
    assert_eq!(
        page(&mut views, &mut node, &validations)
            .unwrap()
            .objects
            .len(),
        1
    );
    validations.filter.evaluator = Some(node.identity.worker);
    assert!(
        page(&mut views, &mut node, &validations)
            .unwrap()
            .objects
            .is_empty()
    );
    let mut testaments = query(ObjectKind::Testament);
    testaments.filter.claim = Some(claim);
    assert_eq!(
        page(&mut views, &mut node, &testaments)
            .unwrap()
            .objects
            .len(),
        1
    );

    let original = node
        .session
        .read_at_least(SessionSeq(0))
        .unwrap()
        .artifacts
        .values()
        .next()
        .unwrap()
        .content()
        .clone();
    let mut related = NewArtifact {
        id: ArtifactId::from_u128(777),
        content: original.clone(),
    };
    related.content.metadata = b"related but never included in the testament".to_vec();
    related
        .content
        .inputs
        .insert(ObjectRef::claim(node.identity.ledger, claim));
    let evidence = EvidenceAttestation {
        descriptor_hash: related.content.content_hash().unwrap(),
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    };
    submit(
        &mut node,
        "related-artifact",
        Command::RegisterArtifact { artifact: related },
        vec![evidence],
    );
    assert_eq!(
        page(&mut views, &mut node, &query(ObjectKind::Artifact))
            .unwrap()
            .objects
            .len(),
        2
    );
    let mut artifacts = query(ObjectKind::Artifact);
    artifacts.filter.testament = Some(testament);
    artifacts.filter.producer = Some(original.producer);
    artifacts.filter.artifact_kind = Some(original.kind);
    artifacts.filter.schema = Some(original.schema_hash);
    artifacts.filter.claim = Some(claim);
    let manifest = page(&mut views, &mut node, &artifacts).unwrap();
    assert_eq!(manifest.objects.len(), 1);
    let mut small_manifest = artifacts.clone();
    small_manifest.max_visits = 1;
    let skipped_parent = page(&mut views, &mut node, &small_manifest).unwrap();
    assert!(skipped_parent.objects.is_empty());
    assert_eq!(skipped_parent.visited, 1);
    assert!(skipped_parent.next.is_some());
    small_manifest.cursor = skipped_parent.next;
    assert_eq!(
        page(&mut views, &mut node, &small_manifest)
            .unwrap()
            .objects
            .len(),
        1
    );
    assert!(
        !matches!(manifest.objects[0],ReadObject::Artifact{id,..} if id == ArtifactId::from_u128(777))
    );
    artifacts.filter.testament = None;
    assert_eq!(
        page(&mut views, &mut node, &artifacts)
            .unwrap()
            .objects
            .len(),
        1
    );
    artifacts.filter.testament = Some(testament);
    artifacts.filter.claim = Some(ClaimId::from_u128(999));
    assert!(
        page(&mut views, &mut node, &artifacts)
            .unwrap()
            .objects
            .is_empty()
    );
}

#[test]
fn zero_match_pages_advance_and_cursors_bind_query_principal_role_route_and_prefix() {
    let root = tempfile::tempdir().unwrap();
    let mut node = fixture(root.path());
    for id in 1..=3 {
        add_claim(&mut node, id);
    }
    let mut views = ReadViews::new();
    let mut request = query(ObjectKind::Claim);
    request.filter.source = Some(node.identity.worker); // no matching issuer
    request.max_visits = 1;
    request.max_items = 1;
    let first = page(&mut views, &mut node, &request).unwrap();
    assert!(first.objects.is_empty());
    assert_eq!(first.objects.capacity(), 0);
    assert_eq!(first.visited, 1);
    assert!(first.next.is_some());
    request.cursor = first.next.clone();
    let second = page(&mut views, &mut node, &request).unwrap();
    assert_ne!(first.next, second.next);
    assert_eq!(first.token, second.token);
    assert!(second.objects.is_empty());
    assert_eq!(page(&mut views, &mut node, &request).unwrap(), second); // exact cursor retry
    add_claim(&mut node, 4); // absent from this pinned four-claim snapshot
    let mut count = 2;
    request.cursor = second.next;
    while request.cursor.is_some() {
        let next = page(&mut views, &mut node, &request).unwrap();
        assert!(next.objects.is_empty());
        assert_eq!(next.token, first.token);
        assert_eq!(next.visited, 1);
        request.cursor = next.next;
        count += 1;
        assert!(count <= 4);
    }
    assert_eq!(count, 4);
    request.cursor = first.next;
    let original = request.clone();
    request.cursor.as_mut().unwrap().bytes[0] ^= 1;
    assert_eq!(
        page(&mut views, &mut node, &request),
        Err(AccessError::InvalidRequest)
    );
    request = original.clone();
    request.filter.source = Some(node.identity.issuer);
    assert_eq!(
        page(&mut views, &mut node, &request),
        Err(AccessError::Unauthorized)
    );
    for (principal, role) in [
        (node.identity.worker, PeerRole::Actor),
        (node.identity.issuer, PeerRole::Runtime),
    ] {
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal,
            tenants: [node.identity.ledger.tenant].into_iter().collect(),
            role,
        })
        .unwrap();
        let scope = list_scope(&peer, node.identity.ledger, &original.filter).unwrap();
        assert_eq!(
            views.list(
                &mut node.session,
                ListReadContext {
                    principal,
                    scope,
                    request_id: RequestId::from_u128(3),
                    barrier: None,
                },
                &original,
                &WireLimits::default()
            ),
            Err(AccessError::Unauthorized)
        );
    }
    views.route_epoch = RouteEpoch(2);
    assert_eq!(
        page(&mut views, &mut node, &original),
        Err(AccessError::SnapshotExpired)
    );
    views.route_epoch = RouteEpoch(1);
    let mut fresh = ReadViews::new();
    assert_eq!(
        page(&mut fresh, &mut node, &original),
        Err(AccessError::SnapshotExpired)
    );
    views.started -= std::time::Duration::from_secs(31);
    assert_eq!(
        page(&mut views, &mut node, &original),
        Err(AccessError::SnapshotExpired)
    );
}

#[test]
fn visit_bytes_and_response_bytes_stop_before_materializing_an_oversized_object() {
    let root = tempfile::tempdir().unwrap();
    let mut node = fixture(root.path());
    let mut views = ReadViews::new();
    let request = query(ObjectKind::Claim);
    let peer = peer(&node);
    let scope = list_scope(&peer, node.identity.ledger, &request.filter).unwrap();
    for limits in [
        WireLimits {
            max_cost: 1,
            ..WireLimits::default()
        },
        WireLimits {
            max_frame_bytes: OUTPUT_OVERHEAD as u32 + 1,
            ..WireLimits::default()
        },
    ] {
        assert_eq!(
            views.list(
                &mut node.session,
                ListReadContext {
                    principal: peer.principal(),
                    scope,
                    request_id: RequestId::from_u128(1),
                    barrier: None
                },
                &request,
                &limits
            ),
            Err(AccessError::Capacity)
        );
    }
}

#[tokio::test]
async fn local_owner_lists_under_actor_authority_and_preserves_reply_charge() {
    let root = tempfile::tempdir().unwrap();
    let node = fixture(root.path());
    let actor = peer(&node);
    let ledger = node.identity.ledger;
    let (host, owner) = crate::host::LocalHost::spawn(node, WireLimits::default()).unwrap();
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(1),
        operation: Operation::List(query(ObjectKind::Validation)),
    };
    let reply = dispatch_accounted(&host, actor, request.clone(), &WireLimits::default()).await;
    let Response::Listed(page) = &reply.envelope().result else {
        panic!("{:?}", reply.envelope())
    };
    assert_eq!(page.objects.len(), 2);
    validate_response(&request, reply.envelope(), None, &WireLimits::default()).unwrap();
    host.stop().await.unwrap();
    owner.join().unwrap();
    assert_eq!(page.objects.len(), 2); // delivered snapshot materialization outlives owner
}

#[test]
fn validator_contract_pages_bind_filters_and_preserve_empty_prefix_continuations() {
    let root = tempfile::tempdir().unwrap();
    let mut node = fixture(root.path());
    for id in 100..106 {
        add_claim(&mut node, id);
    }
    let actor = peer(&node);
    let ledger = node.identity.ledger;
    let mut views = ReadViews::new();
    let mut query = ValidatorRequest {
        query: ListRequest {
            filter: ListFilter::new(ObjectKind::Validation),
            cursor: None,
            max_items: 1,
            max_visits: 1,
        },
        handler: None,
        version: None,
        agentic: None,
        evidence_schema: None,
    };
    let read = |views: &mut ReadViews,
                node: &mut EmbeddedNode,
                query: &ValidatorRequest,
                actor: &AuthenticatedPeer| {
        let scope = validator_scope(actor, ledger, query).unwrap();
        views.validators(
            &mut node.session,
            ListReadContext {
                principal: actor.principal(),
                scope,
                request_id: RequestId::from_u128(801),
                barrier: None,
            },
            query,
            &WireLimits::default(),
        )
    };
    let first = read(&mut views, &mut node, &query, &actor).unwrap();
    assert!(first.objects.is_empty());
    assert_eq!(first.visited, 1);
    assert!(first.next.is_some());
    query.query.cursor = first.next.clone();
    let retry = read(&mut views, &mut node, &query, &actor).unwrap();
    assert_eq!(read(&mut views, &mut node, &query, &actor).unwrap(), retry);
    let mut changed = query.clone();
    changed.agentic = Some(false);
    assert_eq!(
        read(&mut views, &mut node, &changed, &actor),
        Err(AccessError::Unauthorized)
    );
    changed = query.clone();
    changed.query.max_visits = 2;
    assert_eq!(
        read(&mut views, &mut node, &changed, &actor),
        Err(AccessError::Unauthorized)
    );
    changed = query.clone();
    changed.query.cursor.as_mut().unwrap().bytes[0] ^= 1;
    assert_eq!(
        read(&mut views, &mut node, &changed, &actor),
        Err(AccessError::InvalidRequest)
    );
    let stranger = AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(999),
        tenants: [ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap();
    assert_eq!(
        read(&mut views, &mut node, &query, &stranger),
        Err(AccessError::Unauthorized)
    );
    // A later mutation cannot extend a previously pinned contract catalogue.
    add_claim(&mut node, 107);
    let mut matches = 0;
    let mut visits = first.visited;
    let mut done = false;
    for _ in 0..12 {
        let page = read(&mut views, &mut node, &query, &actor).unwrap();
        assert_eq!(page.token, first.token);
        visits += page.visited;
        matches += page.objects.len();
        for object in &page.objects {
            assert!(
                matches!(object,ReadObject::Validation{value,..} if !value.content().handlers.is_empty())
            );
        }
        query.query.cursor = page.next;
        if query.query.cursor.is_none() {
            done = true;
            break;
        }
    }
    assert!(done);
    assert_eq!(matches, 1);
    assert_eq!(visits, 8);
    views.expire_test_views();
    query.query.cursor = first.next;
    assert_eq!(
        read(&mut views, &mut node, &query, &actor),
        Err(AccessError::SnapshotExpired)
    );
}

#[path = "selection_read_tests.rs"]
mod selection_tests;
