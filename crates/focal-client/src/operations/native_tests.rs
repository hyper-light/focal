use super::tests::{assert_shape, context, id, id_hash};
use super::*;
use focal_wire::NativeOperationKind;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn fixture(name: &str) -> Value {
    match name {
        "claim.submit" => json!({
            "description": "Inspect the module and report.",
            "target": id(2),
            "scopes": [{"kind": "file", "key": "src/lib.rs"}],
            "relations": [{"kind": "reviews", "target": format!("claim:{}", id(40))}],
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}},
                {"kind": "test", "description": "Run the suite.", "target": {"type": "slot", "index": 0, "name": "primary"},
                 "evaluator": id(3), "handlers": [{"id": id(77), "version": id_hash(77)}], "deadline": {"at": 10_000}}
            ],
            "slots": [{"slot": 0, "checks": [{"declaration": 1}]}]
        }),
        "claim.post" | "claim.cancel" => json!({"claim": id(10)}),
        "receipt.acquire" => json!({"claim": id(10), "id": id(13)}),
        "artifact.submit" => {
            json!({"claim": id(10), "slot": 0, "payload": {"type": "text", "text": "{\"passed\":1,\"failed\":0,\"skipped\":0}"}})
        }
        "artifact.diagnostic" => {
            json!({"claim": id(10), "reason": "work", "payload": {"type": "inline", "bytes": [123, 125]}})
        }
        "testament.submit" => {
            json!({"claim": id(10), "summary": "Done.", "confidence": "committed", "outcome": "complete",
            "manifest": [{"slot": 0, "artifact": {"id": id(15), "hash": id_hash(5)}}]})
        }
        "testament.post" | "testament.receive" => json!({"claim": id(10), "testament": id(16)}),
        "validation.begin" => json!({"claim": id(10), "validation": id(11)}),
        "validation.report" => {
            json!({"claim": id(10), "validation": id(11), "verdict": "pass", "payload": {"type": "text", "text": "{\"passed\":1,\"failed\":0,\"skipped\":0}"}})
        }
        "claim.release_scope" | "validation.seal_increments" => json!({"claim": id(10)}),
        "receipt.adopt" => json!({"claim": id(10), "holder": id(4), "id": id(13)}),
        "artifact.fail" => {
            json!({"claim": id(10), "slot": 0, "diagnostic": id(15)})
        }
        "artifact.receive" => json!({"claim": id(10), "artifact": id(15)}),
        "artifact.reject" => {
            json!({"claim": id(10), "artifact": id(15), "reason": "structure", "payload": {"type": "text", "text": "{\"code\":\"bad\",\"message\":\"Malformed report.\"}"}})
        }
        "validation.enter_whole_work" => json!({"claim": id(10), "testament": id(16)}),
        "audit.generate" => json!({"claim": id(10), "id": id(17)}),
        "audit.post" => json!({"testament": id(17)}),
        "monitor.register" => json!({
            "claim": id(10),
            "roots": [{"predicate": "satisfied", "claim": id(40)}, {"predicate": "released", "claim": id(41)}],
            "deadline": {"at": 10_000}
        }),
        "monitor.rebind" => {
            json!({"claim": id(10), "monitor": id(18), "predecessor": id(40), "successor": id(41)})
        }
        "monitor.cancel" => json!({"claim": id(10), "monitor": id(18)}),
        "claim.challenge" => json!({
            "description": "Prove the report covers the edge cases.",
            "target": id(2),
            "artifact": format!("{}@{}", id(40), id_hash(40)),
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}},
                {"kind": "test", "description": "Run the suite.", "target": {"type": "slot", "index": 0, "name": "primary"},
                 "evaluator": id(3), "handlers": [{"id": id(77), "version": id_hash(77)}], "deadline": {"at": 10_000}}
            ],
            "slots": [{"slot": 0, "checks": [{"declaration": 1}]}],
            "policy": {"corrective_allowed": true, "max_follow_ups": 1, "single_issuer": true, "escalation": "evaluator"}
        }),
        "claim.consult" => json!({
            "description": "Which cases does the parser leave undefined?",
            "target": id(2),
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}
            ],
            "policy": {"max_follow_ups": 2, "escalation": "holder"}
        }),
        "claim.correct" => json!({
            "challenge": id(40),
            "verdict": id(41),
            "description": "Redo the inspection with the missing cases.",
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}
            ]
        }),
        "claim.follow_up" => json!({
            "refines": id(40),
            "description": "And the unicode cases?",
            "validations": [
                {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 10_000}}
            ]
        }),
        _ => panic!("fixture {name}"),
    }
}

/// Every list field, defaults included, so the schema property set is checked
/// exactly against the released document.
fn list_fixture(name: &str) -> Value {
    let page = json!({"cursor": null, "limit": 100, "max_visits": 1024});
    let mut fixture = match name {
        "claim.list" => json!({
            "issuer": "self", "subject": id(2), "status": "posted", "action": "work",
            "scope": {"kind": "file", "key": "src/lib.rs"},
            "relation": {"kind": "reviews", "target": format!("claim:{}", id(40))},
            "created_after": 3
        }),
        "artifact.list" => {
            json!({"producer": id(2), "kind": "test-report", "schema": id_hash(9), "input": id(10)})
        }
        "validation.list" => json!({"claim": id(10), "evaluator": "self"}),
        "evaluation.list" => {
            json!({"claim": id(10), "validation": id(11), "evaluator": id(3), "verdict": "pass"})
        }
        "testament.list" | "monitor.list" => json!({"claim": id(10)}),
        "receipt.list" => json!({"holder": "self", "claim": id(10)}),
        "event.list" => json!({"after": {"sequence": 4, "ordinal": 1}}),
        _ => panic!("list fixture {name}"),
    };
    for (field, value) in page.as_object().unwrap() {
        fixture[field] = value.clone();
    }
    fixture
}

#[test]
fn native_catalog_is_sorted_versioned_and_every_field_has_a_schema_property() {
    assert_eq!(native_descriptors().len(), 43);
    let mut previous = "";
    for descriptor in native_descriptors() {
        assert!(previous < descriptor.name, "{}", descriptor.name);
        previous = descriptor.name;
        assert_eq!(descriptor.version, 2);
        assert_eq!(descriptor.wire, WireProfile::Native);
        assert_eq!(descriptor.retry, RetryIdentity::NativeN1);
        assert_eq!(descriptor.capability, Capability::Actor);
        assert!(std::ptr::eq(
            find_native(descriptor.name).unwrap(),
            descriptor
        ));
        if descriptor.result_kind == ResultKind::List {
            assert!(!descriptor.mutation && !descriptor.destructive);
            let fixture = list_fixture(descriptor.name);
            let list =
                parse_native_list_json(descriptor.name, &serde_json::to_vec(&fixture).unwrap())
                    .unwrap();
            assert_eq!(list.name(), descriptor.name);
            assert_eq!(list.page().limit, 100);
            assert_eq!(list.page().max_visits, 1024);
            assert!(list.page().cursor.is_none());
            let schema = descriptor.input_schema().unwrap();
            let fields: BTreeSet<_> = fixture.as_object().unwrap().keys().collect();
            let schema_fields: BTreeSet<_> =
                schema["properties"].as_object().unwrap().keys().collect();
            assert_eq!(fields, schema_fields, "{}", descriptor.name);
            assert_shape(&schema, &schema, &fixture);
            // Reparsing the expanded document is the identity.
            let expanded = serde_json::to_value(&list).unwrap();
            let reparsed = parse_native_list_json(
                descriptor.name,
                &serde_json::to_vec(&expanded["input"]).unwrap(),
            )
            .unwrap();
            assert_eq!(reparsed, list);
            let mut forged = fixture.clone();
            forged["forged"] = json!(true);
            assert!(
                parse_native_list_json(descriptor.name, &serde_json::to_vec(&forged).unwrap())
                    .is_err()
            );
            assert!(parse_native_read_json(descriptor.name, b"{}").is_err());
            assert!(parse_native_json(descriptor.name, b"{}").is_err());
            continue;
        }
        if !descriptor.mutation {
            assert_eq!(descriptor.result_kind, ResultKind::Read);
            assert!(!descriptor.destructive);
            let fixture = match descriptor.name {
                "ledger.standing" => json!({}),
                "claim.wait" => json!({"claim": id(10), "until": "testament", "timeout_ms": 30000}),
                "validation.context" => json!({
                    "validation": id(11), "phase": "increment", "slot": null, "target": id(15),
                    "generation": 1, "results_after": null, "limit": 16
                }),
                _ => json!({"id": id(10)}),
            };
            let read =
                parse_native_read_json(descriptor.name, &serde_json::to_vec(&fixture).unwrap())
                    .unwrap();
            assert_eq!(read.name(), descriptor.name);
            let schema = descriptor.input_schema().unwrap();
            let fields: BTreeSet<_> = fixture.as_object().unwrap().keys().collect();
            let schema_fields: BTreeSet<_> =
                schema["properties"].as_object().unwrap().keys().collect();
            assert_eq!(fields, schema_fields, "{}", descriptor.name);
            assert_shape(&schema, &schema, &fixture);
            let mut forged = fixture.clone();
            forged["forged"] = json!(true);
            assert!(
                parse_native_read_json(descriptor.name, &serde_json::to_vec(&forged).unwrap())
                    .is_err()
            );
            assert!(
                parse_native_json(descriptor.name, &serde_json::to_vec(&fixture).unwrap()).is_err()
            );
            continue;
        }
        assert_eq!(descriptor.result_kind, ResultKind::Mutation);
        assert!(parse_native_read_json(descriptor.name, b"{}").is_err());
        let fixture = fixture(descriptor.name);
        let authored =
            parse_native_json(descriptor.name, &serde_json::to_vec(&fixture).unwrap()).unwrap();
        assert_eq!(authored.descriptor().name, descriptor.name);
        assert_eq!(authored.name(), descriptor.name);
        let serialized = serde_json::to_value(&authored).unwrap();
        let expanded = &serialized["input"];
        let schema = descriptor.input_schema().unwrap();
        let fields: BTreeSet<_> = expanded.as_object().unwrap().keys().collect();
        let schema_fields: BTreeSet<_> = schema["properties"].as_object().unwrap().keys().collect();
        assert_eq!(fields, schema_fields, "{}", descriptor.name);
        assert_shape(&schema, &schema, &fixture);
        assert_shape(&schema, &schema, expanded);
        let reparsed =
            parse_native_json(descriptor.name, &serde_json::to_vec(expanded).unwrap()).unwrap();
        assert_eq!(
            authored.canonical_intent().unwrap(),
            reparsed.canonical_intent().unwrap()
        );
        assert_eq!(
            schema["$id"],
            Value::String(format!("urn:focal:operation:{}:input:2", descriptor.name))
        );
        // Unknown fields and a foreign operation name are refused.
        let mut forged = fixture.clone();
        forged["forged"] = json!(true);
        assert!(parse_native_json(descriptor.name, &serde_json::to_vec(&forged).unwrap()).is_err());
    }
    assert!(parse_native_json("claim.get", b"{}").is_err());
    assert!(parse_native_json("claim.progress", b"{}").is_err());
    assert!(parse_native_read_json("claim.list", b"{}").is_err());
    assert!(parse_native_read_json("claim.get", br#"{"id":"zz"}"#).is_ok());
    assert_eq!(
        native_descriptors().iter().filter(|d| d.mutation).count(),
        27
    );
    assert_eq!(
        native_descriptors()
            .iter()
            .filter(|d| d.result_kind == ResultKind::List)
            .count(),
        8
    );
    // A page document without bounds takes the released defaults; a
    // testament or monitor list needs its claim.
    assert!(parse_native_list_json("testament.list", b"{}").is_err());
    let list = parse_native_list_json("claim.list", b"{}").unwrap();
    assert_eq!(list.page(), &NativeListPageDocument::default());
    // The V1 catalog keeps its own rows: same names, version one, V1 wire.
    for name in [
        "claim.submit",
        "claim.post",
        "testament.submit",
        "validation.begin",
    ] {
        let legacy = find(name).unwrap();
        assert_eq!(
            (legacy.version, legacy.wire, legacy.retry),
            (1, WireProfile::V1, RetryIdentity::ManagedM1)
        );
    }
    assert!(find("artifact.diagnostic").is_none());
    assert!(find("validation.report").is_none());
}

#[test]
fn native_documents_refuse_derived_relations_and_malformed_targets_at_parse_or_build_time() {
    let context = context();
    let _ = context;
    // Parsing accepts the string; the compiler refuses derived relations. The
    // schema already refuses them, which this test pins.
    let schema = find_native("claim.submit").unwrap().input_schema().unwrap();
    let relation = &schema["$defs"]["relation"]["properties"]["kind"]["enum"];
    for derived in ["issuer", "subject", "claim_action", "caused_by"] {
        assert!(
            !relation
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == derived)
        );
    }
    // Identities are exact 32-digit nonzero hexadecimal in the schema and the DTO decoder.
    let id_schema = &schema["$defs"]["id"];
    assert_eq!(id_schema["pattern"], json!("^[0-9a-fA-F]{32}$"));
    assert!(parse_native_json("testament.post", br#"{"claim":5,"testament":"x"}"#).is_err());
    assert!(
        parse_native_json(
            "claim.submit",
            br#"{"description":"","target":"self","validations":[]}"#
        )
        .is_ok()
    );
    assert!(
        parse_native_json(
            "artifact.submit",
            br#"{"claim":"a","slot":0,"payload":{"type":"file","path":"x"}}"#
        )
        .is_err()
    );
}

#[test]
fn coverage_table_maps_every_frame_tag_once_and_only_exposed_rows_have_descriptors() {
    let table = native_coverage_table();
    assert_eq!(table.len(), NativeOperationKind::ALL.len());
    let mut tags = BTreeSet::new();
    let mut exposed = BTreeSet::new();
    let mut wire_only = BTreeSet::new();
    for row in table {
        assert_eq!(row.operation.participant_authored(), !row.tags.is_empty());
        for tag in row.tags {
            assert!(tags.insert(*tag), "duplicate tag {tag}");
        }
        match row.exposure {
            NativeExposure::AuthoredTool => {
                let descriptor = row
                    .descriptor()
                    .unwrap_or_else(|| panic!("{}", row.operation.name()));
                assert_eq!(Some(descriptor.name), row.name);
                assert!(!row.cli.is_empty());
                assert!(!row.tags.is_empty());
                exposed.insert(row.name.unwrap());
            }
            NativeExposure::InternalTimer
            | NativeExposure::Activation
            | NativeExposure::Retirement => {
                assert!(row.name.is_none() && row.tags.is_empty() && row.cli.is_empty());
                assert_eq!(row.actor, NativeActor::Internal);
                assert!(row.descriptor().is_none());
            }
            NativeExposure::WireOnly => {
                assert!(row.name.is_none() && row.cli.is_empty() && !row.tags.is_empty());
                wire_only.insert(row.operation.name());
            }
        }
    }
    // Every participant frame tag 0..=27 belongs to exactly one operation.
    assert_eq!(tags, (0..focal_wire::NATIVE_COMMAND_TAGS).collect());
    // Every native descriptor is claimed by exactly one exposed row; the
    // peer verbs are authored shapes of claim.submit and are claimed through
    // it.
    let catalog: BTreeSet<_> = native_descriptors()
        .iter()
        .filter(|d| d.mutation && authored_shape(d).is_none())
        .map(|d| d.name)
        .collect();
    assert_eq!(exposed, catalog);
    let shapes: Vec<_> = native_descriptors()
        .iter()
        .filter_map(|d| authored_shape(d).map(|shape| (d.name, shape)))
        .collect();
    assert_eq!(
        shapes,
        [
            ("claim.challenge", "claim.submit"),
            ("claim.consult", "claim.submit"),
            ("claim.correct", "claim.submit"),
            ("claim.follow_up", "claim.submit"),
        ]
    );
    assert!(shapes.iter().all(|(_, shape)| exposed.contains(shape)));
    // Every participant operation has an authored surface; the wire-only
    // exposure stays defined so a future owner operation must choose.
    assert!(wire_only.is_empty(), "{wire_only:?}");
    // The four evaluation verbs share two descriptors selected by phase.
    let shared: Vec<_> = NativeOperationKind::ALL
        .iter()
        .map(|operation| native_coverage(*operation))
        .filter(|row| row.name == Some("validation.begin"))
        .map(|row| row.operation)
        .collect();
    assert_eq!(
        shared,
        [
            NativeOperationKind::BeginIncrement,
            NativeOperationKind::BeginAdmission,
            NativeOperationKind::BeginWork
        ]
    );
    assert!(NATIVE_RETRY.len() > 40);
}
