use super::tests::{context, id, parse};
use super::*;
use focal_wire::*;
use serde_json::json;

#[test]
fn predicates_lower_without_generated_identity_and_legacy_bytes_stay_unchanged() {
    for (name, kind) in [
        ("claim.list", ObjectKind::Claim),
        ("artifact.list", ObjectKind::Artifact),
        ("testament.list", ObjectKind::Testament),
        ("validation.list", ObjectKind::Validation),
    ] {
        let authored = parse(name, &json!({}));
        let legacy = authored
            .build(&context(), &mut || panic!("generated identity"))
            .unwrap()
            .into_wire(None)
            .unwrap();
        assert_eq!(
            legacy,
            Operation::List(ListDocument::default().build(kind, &context()).unwrap())
        );
        assert_eq!(postcard::to_stdvec(&legacy).unwrap()[0], 13);
        let selected = parse(name, &json!({"created_after":0,"created_through":42}))
            .build(&context(), &mut || panic!("generated identity"))
            .unwrap();
        assert!(selected.clone().into_wire(Some(ObjectRevision(1))).is_err());
        let operation = selected.into_wire(None).unwrap();
        assert!(!operation.is_mutation());
        assert_eq!(operation.registered_tag(), 24);
        assert_eq!(postcard::to_stdvec(&operation).unwrap()[0], 23);
        let Operation::Select(query) = operation else {
            panic!("select")
        };
        query
            .validate(context().ledger, &WireLimits::default())
            .unwrap();
    }
    let operation=parse("claim.list",&json!({"scopes":[{"kind":"file","key":"a:b.rs"}],"relations":[{"kind":"issuer","target":"participant:self"},{"kind":"claim_action","target":"action:work"}],"caused_by":format!("root:{}",id(9))})).build(&context(),&mut ||panic!("generated")).unwrap().into_wire(None).unwrap();
    let Operation::Select(query) = operation else {
        panic!("select")
    };
    assert_eq!(
        query.predicates.relations[0].target,
        RelationTarget::Participant(context().actor)
    );
    assert_eq!(query.predicates.scopes[0].key, "a:b.rs");
    assert_eq!(query.predicates.relations.len(), 3);
}
#[test]
fn predicates_reject_wrong_family_limits_unknown_activity_and_untyped_relations() {
    for (name, value) in [
        ("claim.list", json!({"outcome":"complete"})),
        (
            "artifact.list",
            json!({"scopes":[{"kind":"file","key":"a"}]}),
        ),
        (
            "testament.list",
            json!({"inputs":[{"kind":"claim","id":id(1)}]}),
        ),
        ("validation.list", json!({"confidence":"committed"})),
        (
            "claim.list",
            json!({"relations":[{"kind":"depends_on","target":id(1)}]}),
        ),
        ("claim.list", json!({"caused_by":"participant:self"})),
        ("claim.list", json!({"created_after":5,"created_through":5})),
        (
            "claim.list",
            json!({"scopes":vec![json!({"kind":"file","key":"a"});17]}),
        ),
    ] {
        assert!(
            parse(name, &value)
                .build(&context(), &mut || panic!("generated"))
                .is_err(),
            "{name}: {value}"
        );
    }
    for field in ["changed_since", "last_changed", "result"] {
        assert!(
            parse_json(
                "validation.list",
                &serde_json::to_vec(&json!({field:1})).unwrap()
            )
            .is_err()
        );
    }
    let document = ListDocument {
        created_after: Some(1),
        ..ListDocument::default()
    };
    assert!(document.build(ObjectKind::Claim, &context()).is_err());
}
