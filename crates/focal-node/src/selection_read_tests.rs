use super::*;
fn select(
    views: &mut ReadViews,
    node: &mut EmbeddedNode,
    request: &SelectionRequest,
) -> Result<ListPage, AccessError> {
    let principal = peer(node);
    let scope = selection_scope(&principal, node.identity.ledger, request)?;
    views.selection(
        &mut node.session,
        ListReadContext {
            principal: principal.principal(),
            scope,
            request_id: RequestId::from_u128(900),
            barrier: None,
        },
        request,
        &WireLimits::default(),
    )
}
#[test]
fn selection_reads_all_four_canonical_families_and_checks_returned_predicates() {
    let root = tempfile::tempdir().unwrap();
    let mut node = fixture(root.path());
    let mut views = ReadViews::new();
    let mut snapshots = Vec::new();
    for kind in [
        ObjectKind::Claim,
        ObjectKind::Testament,
        ObjectKind::Artifact,
        ObjectKind::Validation,
    ] {
        let values = page(&mut views, &mut node, &query(kind)).unwrap();
        let mut predicates = SelectionPredicates::default();
        let created = match &values.objects[0] {
            ReadObject::Claim { value, .. } => {
                predicates.scopes = value.content().scopes.iter().cloned().collect();
                predicates.relations = value.content().relations.iter().cloned().collect();
                value.lifecycle().created
            }
            ReadObject::Testament { value, .. } => {
                predicates.outcome = Some(value.content().outcome);
                predicates.confidence = Some(value.content().confidence);
                value.lifecycle().created
            }
            ReadObject::Artifact { value, .. } => value.lifecycle().created,
            ReadObject::Validation { value, .. } => value.lifecycle().created,
            _ => panic!("object"),
        };
        predicates.created_after = Some(SessionSeq(created.0 - 1));
        predicates.created_through = Some(created);
        let selected = SelectionRequest {
            query: query(kind),
            predicates,
        };
        let result = select(&mut views, &mut node, &selected).unwrap();
        assert!(!result.objects.is_empty());
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: node.identity.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(900),
            operation: Operation::Select(selected.clone()),
        };
        validate_response(
            &request,
            &request.reply(Response::Listed(result.clone())),
            None,
            &WireLimits::default(),
        )
        .unwrap();
        let mut forged = request.clone();
        let Operation::Select(selection) = &mut forged.operation else {
            unreachable!()
        };
        selection.predicates.created_after = Some(created);
        selection.predicates.created_through = None;
        assert!(
            validate_response(
                &forged,
                &forged.reply(Response::Listed(result)),
                None,
                &WireLimits::default()
            )
            .is_err()
        );
        snapshots.push(selected);
    }
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
    let claim = *node
        .session
        .read_at_least(SessionSeq(0))
        .unwrap()
        .claims
        .keys()
        .next()
        .unwrap();
    let input = ObjectRef::claim(node.identity.ledger, claim);
    let mut related = NewArtifact {
        id: ArtifactId::from_u128(12345),
        content: original,
    };
    related.content.inputs.insert(input);
    let evidence = EvidenceAttestation {
        descriptor_hash: related.content.content_hash().unwrap(),
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    };
    submit(
        &mut node,
        "selection-artifact",
        Command::RegisterArtifact { artifact: related },
        vec![evidence],
    );
    let request = SelectionRequest {
        query: query(ObjectKind::Artifact),
        predicates: SelectionPredicates {
            inputs: vec![input],
            ..Default::default()
        },
    };
    let result = select(&mut views, &mut node, &request).unwrap();
    assert_eq!(result.objects.len(), 1);
    assert!(
        matches!(result.objects[0],ReadObject::Artifact{id,..} if id==ArtifactId::from_u128(12345))
    );
    node.session.checkpoint().unwrap();
    drop(views);
    drop(node);
    let mut node = fixture(root.path());
    let mut views = ReadViews::new();
    for selected in snapshots {
        assert!(
            !select(&mut views, &mut node, &selected)
                .unwrap()
                .objects
                .is_empty()
        );
    }
    assert_eq!(
        select(&mut views, &mut node, &request)
            .unwrap()
            .objects
            .len(),
        1
    );
}
