use super::*;
use focal_core::Core;
use focal_wire::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[test]
fn canonical_writer_grows_geometrically_and_refuses_overflow_without_mutation() {
    use std::io::Write;
    let mut writer = LimitedJson(Vec::new());
    let mut previous = 0;
    let mut growths = 0;
    for _ in 0..MAX_INPUT_BYTES {
        writer.write_all(b"x").unwrap();
        if writer.0.capacity() != previous {
            growths += 1;
            previous = writer.0.capacity();
        }
        assert!(writer.0.capacity() <= MAX_INPUT_BYTES);
    }
    assert!(growths <= 12, "{growths} growths for byte-at-a-time writes");
    assert!(writer.write_all(b"y").is_err());
    assert_eq!(writer.0.len(), MAX_INPUT_BYTES);
    assert!(writer.0.iter().all(|byte| *byte == b'x'));
}

#[test]
fn reconciliation_inputs_are_strict_read_only_and_never_generate_or_replace_identity() {
    for (name, input, expected) in [
        (
            "request.epoch",
            json!({"epoch":2}),
            ReconcileQuery::Epoch {
                epoch: RequestEpoch(2),
            },
        ),
        (
            "request.status",
            json!({"epoch":2,"request_id":id(99)}),
            ReconcileQuery::Receipt {
                epoch: RequestEpoch(2),
                request: RequestId::from_u128(99),
            },
        ),
    ] {
        let document = parse(name, &input);
        let intent = document.canonical_intent().unwrap();
        assert!(document.descriptor().read_only());
        assert!(document.descriptor().idempotent());
        assert!(!document.descriptor().destructive);
        assert_eq!(document.descriptor().result_kind, ResultKind::Reconcile);
        let mut forbidden_generator = || panic!("a reconciliation query generated identity");
        let planned = document
            .clone()
            .build(&context(), &mut forbidden_generator)
            .unwrap();
        assert_eq!(planned, PlannedOperation::Reconcile(expected));
        assert!(planned.clone().into_wire(Some(ObjectRevision(1))).is_err());
        assert_eq!(
            planned.into_wire(None).unwrap(),
            Operation::Reconcile(expected)
        );
        assert_eq!(document.canonical_intent().unwrap(), intent);
        let mut forged = input.clone();
        forged["principal"] = json!(id(700));
        assert!(parse_json(name, &serde_json::to_vec(&forged).unwrap()).is_err());
        forged.as_object_mut().unwrap().remove("principal");
        forged["epoch"] = json!(0);
        assert!(
            parse(name, &forged)
                .build(&context(), &mut forbidden_generator)
                .is_err()
        );
        forged["epoch"] = json!(2);
        forged["expected_revision"] = json!(1);
        assert!(parse_json(name, &serde_json::to_vec(&forged).unwrap()).is_err());
    }
    assert!(parse_json("request.epoch", br#"{"epoch":1,"epoch":2}"#).is_err());
    assert!(parse_json("request.status", br#"{"epoch":1}"#).is_err());
    assert!(
        parse(
            "request.status",
            &json!({"epoch":1,"request_id":"00000000000000000000000000000000"})
        )
        .build(&context(), &mut || panic!("generated"))
        .is_err()
    );
}

#[test]
fn cursor_reconciliation_output_preserves_the_distinct_committed_result_family() {
    let context = context();
    let key = RequestKey {
        principal: context.actor,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(90),
    };
    let result = ApplicationResult {
        schema_version: 1,
        operation_id: None,
        condition: "observed".into(),
        result: OperationOutput::Reconcile {
            reply: ReconcileReply {
                token: ReadToken {
                    ledger: context.ledger,
                    sequence: SessionSeq(4),
                    route_epoch: RouteEpoch(1),
                },
                applied_index: 12,
                page: ReconcilePage {
                    schema: RECONCILE_SCHEMA,
                    ledger: context.ledger,
                    principal: context.actor,
                    sequence: SessionSeq(4),
                    result: ReconcileResult::Receipt {
                        key,
                        epoch: EpochReconciliation {
                            epoch: key.epoch,
                            minimum: Some(key.epoch),
                            latest_admitted: Some(key.epoch),
                            admitted: true,
                        },
                        resolution: ReceiptResolution::CommittedCursor(Box::new(
                            CursorMutationReceipt {
                                ledger: context.ledger,
                                key,
                                intent_hash: ContentHash([7; 32]),
                                revision: 3,
                                domain_sequence: SessionSeq(2),
                                raft_index: 8,
                                floor: SessionSeq(1),
                                record: None,
                            },
                        )),
                    },
                },
            },
        },
    };
    assert!(!result.is_error());
    let value = serde_json::to_value(&result).unwrap();
    assert_eq!(
        value["result"]["reply"]["page"]["result"]["Receipt"]["resolution"]["CommittedCursor"]["raft_index"],
        8
    );
    let schema = find("request.status").unwrap().output_schema().unwrap();
    assert_shape(&schema, &schema, &value);
    assert_eq!(
        serde_json::from_value::<ApplicationResult>(value).unwrap(),
        result
    );
}

pub(super) fn id(value: u128) -> String {
    format!("{value:032x}")
}
fn hash(value: u8) -> String {
    format!("{value:02x}").repeat(32)
}
pub(super) fn context() -> BuildContext {
    BuildContext {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        actor: ParticipantId::from_u128(3),
        root: RootCommandId::from_u128(4),
        policy_revision: 1,
    }
}
fn claim() -> Value {
    json!({"id":id(10),"occurrence":id(11),"description":"Inspect report","target":id(6),"validations":[{"id":id(12),"kind":"receipt","phase":"whole_work","mode":"required","description":"Received proof","evaluator":id(3)}]})
}
fn fixture(name: &str) -> Value {
    match name {
        "validator.get" => json!({"id":id(21),"version":hash(8)}),
        "claim.submit" => claim(),
        "claim.submit_batch" => json!({"claims":[claim()]}),
        "claim.wait" => json!({"claim":id(10),"until":"satisfied","timeout_ms":1000}),
        "claim.post" => json!({"claim":id(10)}),
        "claim.progress" => {
            json!({"claim":id(10),"receipt":{"id":id(13),"epoch":1},"message":"Working"})
        }
        "claim.cancel" => json!({"claim":id(10),"reason":"No longer needed"}),
        "claim.supersede" => json!({"predecessor":id(9),"successor":claim()}),
        "testament.receive" => json!({"claim":id(10),"testament":id(16)}),
        "validation.begin_increment" => {
            json!({"claim":id(10),"validation":id(12),"target_hash":hash(7),"manifest":hash(9)})
        }
        "validation.begin" | "validation.complete" => json!({"claim":id(10)}),
        "validation.submit" => {
            json!({"validation":id(12),"target_hash":hash(7),"phase":"whole_work","epoch":1,"handler":{"id":id(21),"version":hash(8),"agentic":false},"attempt":0,"manifest":hash(9),"value":"pass","evidence":[{"id":id(15),"hash":hash(6)}]})
        }
        "artifact.register" => {
            json!({"id":id(15),"kind":"text","schema_hash":hash(5),"payload":{"type":"text","text":"Independent evidence"}})
        }
        "monitor.register" => {
            json!({"monitor":id(19),"owner":id(10),"roots":[{"predicate":"terminal","claim":id(10)}],"deadline":{"timer":id(20),"generation":1,"at":100}})
        }
        "ledger.summary" => json!({}),
        "ledger.traverse" => json!({"roots":[format!("claim:{}",id(10))]}),
        "request.epoch" => json!({"epoch":3}),
        "request.status" => json!({"epoch":3,"request_id":id(44)}),
        "receipt.acquire" => json!({"claim":id(10),"id":id(13),"epoch":1}),
        "evidence.begin" => json!({"claim":id(10),"receipt":{"id":id(13),"epoch":1},"id":id(14)}),
        "artifact.submit" => {
            json!({"claim":id(10),"receipt":{"id":id(13),"epoch":1},"evidence_set":id(14),"id":id(15),"kind":"text","schema_hash":hash(5),"payload":{"type":"text","text":"Verified report"}})
        }
        "testament.submit" => {
            json!({"id":id(16),"claim":id(10),"receipt":{"id":id(13),"epoch":1},"evidence_set":id(14),"manifest":[],"summary":"Stopped work","confidence":"tentative","outcome":"interrupted"})
        }
        name if name.ends_with(".list") => json!({}),
        name if name.ends_with(".get") => json!({"id":id(10)}),
        "validation.context" => json!({"id":id(10)}),
        _ => panic!("fixture {name}"),
    }
}
pub(super) fn parse(name: &str, value: &Value) -> AuthoredOperation {
    parse_json(name, &serde_json::to_vec(value).unwrap()).unwrap()
}

#[test]
fn catalog_is_complete_sorted_and_every_released_builder_preserves_field_and_wire_parity() {
    assert_eq!(descriptors().len(), 34);
    assert_eq!(COMMAND_INVENTORY.len(), 29);
    let mut previous = "";
    for descriptor in descriptors() {
        assert!(previous < descriptor.name);
        previous = descriptor.name;
        assert_eq!(find(descriptor.name).unwrap().version, 1);
        let fixture = fixture(descriptor.name);
        let authored = parse(descriptor.name, &fixture);
        assert_eq!(authored.descriptor().name, descriptor.name);
        let serialized = serde_json::to_value(&authored).unwrap();
        let expanded = &serialized["input"];
        // Every DTO serialized field has a schema property. Reading it back
        // preserves defaults and canonical intent, independent of input order.
        let schema = descriptor.input_schema().unwrap();
        let fields: BTreeSet<_> = expanded.as_object().unwrap().keys().collect();
        let schema_fields: BTreeSet<_> = schema["properties"].as_object().unwrap().keys().collect();
        assert_eq!(fields, schema_fields, "{}", descriptor.name);
        assert_shape(&schema, &schema, &fixture);
        assert_shape(&schema, &schema, expanded);
        let reparsed = parse(descriptor.name, expanded);
        assert_eq!(
            authored.canonical_intent().unwrap(),
            reparsed.canonical_intent().unwrap()
        );
        let mut no_ids = || Err(InputError::Identity);
        if descriptor.name == "claim.wait" {
            let first = authored.build(&context(), &mut no_ids).unwrap();
            assert_eq!(first, reparsed.build(&context(), &mut no_ids).unwrap());
            assert!(matches!(&first, PlannedOperation::ClaimWait { .. }));
            assert!(first.into_wire(None).is_err());
            assert!(!descriptor.mutation);
            continue;
        }
        if descriptor.name == "validation.context" {
            let first = authored.build(&context(), &mut no_ids).unwrap();
            let second = reparsed.build(&context(), &mut no_ids).unwrap();
            assert_eq!(first, second);
            assert!(matches!(
                &first,
                PlannedOperation::ValidationContext(ReadRequest {
                    query: focal_wire::ReadQuery::ValidationResults { .. },
                    ..
                })
            ));
            assert!(first.into_wire(None).is_err());
            assert!(!descriptor.mutation);
            continue;
        }
        let first = authored
            .build(&context(), &mut no_ids)
            .unwrap()
            .into_wire(None)
            .unwrap();
        let second = reparsed
            .build(&context(), &mut no_ids)
            .unwrap()
            .into_wire(None)
            .unwrap();
        assert_eq!(
            postcard::to_stdvec(&first).unwrap(),
            postcard::to_stdvec(&second).unwrap()
        );
        assert_eq!(wire_coverage(&first).mutation, descriptor.mutation);
        assert_eq!(wire_coverage(&first).exposure, Exposure::AuthoredTool);
        if participant_protocol(&first) == PEER_PROTOCOL_VERSION {
            assert!(
                matches!(first, Operation::Submit { ref command, .. } if peer_command(command))
            );
            // The old profile remains role restricted. The new profile checks
            // command-specific standing in the ledger owner after authentication.
            assert_ne!(
                focal_wire::capability(&first),
                focal_wire::Capability::Actor
            );
        } else {
            assert_eq!(
                focal_wire::capability(&first),
                focal_wire::Capability::Actor
            );
        }
    }
    let names: BTreeSet<_> = COMMAND_INVENTORY.iter().map(|entry| entry.name).collect();
    assert_eq!(names.len(), 29);
    assert!(
        COMMAND_INVENTORY
            .iter()
            .all(|entry| entry.exposure != Exposure::AuthoredTool || find(entry.name).is_some())
    );
}

#[test]
fn strict_inputs_and_semantic_predicates_reject_spoofing_without_allocating_ids() {
    for descriptor in descriptors() {
        for forbidden in [
            "authority",
            "issuer",
            "runtime",
            "operation_id",
            "expected_revision",
            "principal",
            "logical_time",
        ] {
            let mut value = fixture(descriptor.name);
            value
                .as_object_mut()
                .unwrap()
                .insert(forbidden.into(), json!(true));
            assert!(parse_json(descriptor.name, &serde_json::to_vec(&value).unwrap()).is_err());
        }
    }
    for bytes in [
        br#"{"claim":"a","claim":"b"}"#.as_slice(),
        br#"{"claim":"a","cl\u0061im":"b"}"#,
        br#"{} {}"#,
    ] {
        assert!(parse_json("claim.post", bytes).is_err());
    }
    assert!(parse_json("claim.post", &vec![b' '; MAX_INPUT_BYTES + 1]).is_err());
    assert!(parse_json("cluster.control", b"{}").is_err());
    let mut calls = 0;
    let mut ids = || {
        calls += 1;
        Ok([5; 16])
    };
    for value in [
        json!({"claim":id(10),"epoch":0}),
        json!({"claim":id(0),"epoch":1}),
    ] {
        assert!(
            parse("receipt.acquire", &value)
                .build(&context(), &mut ids)
                .is_err()
        );
    }
    assert_eq!(calls, 0);
    for (name, value) in [
        ("claim.list", json!({"testament":id(12)})),
        ("testament.list", json!({"source":"self"})),
        ("artifact.list", json!({"evaluator":"self"})),
        ("validation.list", json!({"producer":"self"})),
        ("claim.list", json!({"limit":0})),
        ("claim.list", json!({"max_visits":1025})),
        ("claim.list", json!({"cursor":"0"})),
        (
            "claim.get",
            json!({"id":id(10),"after":{"target_hash":hash(7),"phase":"whole_work","epoch":1}}),
        ),
        (
            "validation.get",
            json!({"id":id(12),"after":{"target_hash":hash(7),"phase":"whole_work","epoch":1}}),
        ),
    ] {
        assert!(
            parse(name, &value)
                .build(&context(), &mut || Ok([9; 16]))
                .is_err(),
            "{name} {value}"
        );
    }
}

#[test]
fn reads_lists_and_aliases_preserve_selected_ledger_and_exact_cursors() {
    let operation=parse("validation.get",&json!({"id":id(12),"prefix":{"sequence":88,"route_epoch":3},"after":{"target_hash":hash(7),"phase":"whole_work","epoch":2,"attempt":4},"limit":3})).build(&context(),&mut||Err(InputError::Identity)).unwrap();
    let PlannedOperation::Read(request) = operation else {
        panic!("read")
    };
    assert_eq!(
        request.consistency,
        ReadConsistency::Exact(ReadToken {
            ledger: context().ledger,
            sequence: SessionSeq(88),
            route_epoch: RouteEpoch(3)
        })
    );
    assert!(
        matches!(request.query,ReadQuery::ValidationResults{id:validation,after:Some(ValidationResultPosition{attempt:Some(4),..})} if validation==ValidationId::from_u128(12))
    );
    let PlannedOperation::List(list)=parse("claim.list",&json!({"source":"self","target":id(6),"status":"posted","action":"work","cursor":"aB00","limit":7,"max_visits":11})).build(&context(),&mut||Err(InputError::Identity)).unwrap() else {panic!("list")};
    assert_eq!(list.filter.source, Some(context().actor));
    assert_eq!(list.filter.target, Some(ParticipantId::from_u128(6)));
    assert_eq!(list.cursor.unwrap().bytes, vec![171, 0]);
    assert_eq!(list.max_items, 7);
    assert_eq!(list.max_visits, 11);
    assert!(
        PlannedOperation::Read(request)
            .into_wire(Some(ObjectRevision(1)))
            .is_err()
    );
}

#[test]
fn generated_ids_only_expand_during_build_and_lifecycle_commands_are_actually_admissible() {
    let mut value = claim();
    value.as_object_mut().unwrap().remove("id");
    value.as_object_mut().unwrap().remove("occurrence");
    let authored = parse("claim.submit", &value);
    let intent = authored.canonical_intent().unwrap();
    assert_eq!(intent, authored.canonical_intent().unwrap());
    let mut next = 100u128;
    let PlannedOperation::Mutation(command) = authored
        .build(&context(), &mut || {
            next += 1;
            Ok(next.to_be_bytes())
        })
        .unwrap()
    else {
        panic!("mutation")
    };
    assert_eq!(next, 102);
    let Command::GenerateClaim { claim } = command else {
        panic!("claim")
    };
    assert_eq!(claim.id, ClaimId::from_u128(101));
    assert_eq!(claim.content.occurrence, OccurrenceId::from_u128(102));

    let mut core = Core::new(context().ledger, Limits::default());
    let apply = |core: &mut Core, actor: ParticipantId, command: Command| {
        let input = AuthenticatedInput {
            ledger: context().ledger,
            principal: actor,
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(u128::from(core.sequence().0) + 1),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: matches!(command, Command::NegotiateEpoch { .. }),
                cause: Cause::Root(context().root),
                policy_revision: 1,
                logical_time: 0,
                evidence: vec![],
            },
            command,
        };
        let prepared = core.prepare(&input).unwrap();
        core.apply_serial(SessionSeq(core.sequence().0 + 1), prepared)
            .unwrap();
    };
    for actor in [context().actor, ParticipantId::from_u128(6)] {
        apply(
            &mut core,
            actor,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
    }
    for name in [
        "claim.submit",
        "claim.post",
        "receipt.acquire",
        "claim.progress",
        "evidence.begin",
        "testament.submit",
    ] {
        let mut actor = context();
        if matches!(
            name,
            "receipt.acquire" | "claim.progress" | "evidence.begin" | "testament.submit"
        ) {
            actor.actor = ParticipantId::from_u128(6);
        }
        let PlannedOperation::Mutation(command) = parse(name, &fixture(name))
            .build(&actor, &mut || Err(InputError::Identity))
            .unwrap()
        else {
            panic!("mutation")
        };
        assert_eq!(command_coverage(&command).name, name);
        apply(&mut core, actor.actor, command);
    }
    assert_eq!(
        core.snapshot()
            .claims
            .get(&ClaimId::from_u128(10))
            .unwrap()
            .lifecycle()
            .status,
        ClaimStatus::TestamentGenerated
    );
}

#[test]
fn typed_outputs_match_published_envelope_and_preserve_domain_conditions() {
    let page = ReadPage {
        token: ReadToken {
            ledger: context().ledger,
            sequence: SessionSeq(1),
            route_epoch: RouteEpoch(1),
        },
        objects: vec![],
        next: None,
    };
    let outputs = [
        OperationOutput::Read { page: page.clone() },
        OperationOutput::List {
            page: ListPage {
                token: page.token,
                objects: vec![],
                next: None,
                visited: 0,
            },
        },
        OperationOutput::Reconcile {
            reply: ReconcileReply {
                applied_index: 20,
                token: page.token,
                page: ReconcilePage {
                    schema: RECONCILE_SCHEMA,
                    ledger: page.token.ledger,
                    principal: context().actor,
                    sequence: page.token.sequence,
                    result: ReconcileResult::Epoch(EpochReconciliation {
                        epoch: RequestEpoch(1),
                        minimum: None,
                        latest_admitted: None,
                        admitted: false,
                    }),
                },
            },
        },
        OperationOutput::Mutation {
            reply: MutationReply::Pending(RequestKey {
                principal: context().actor,
                epoch: RequestEpoch(1),
                id: RequestId::from_u128(1),
            }),
        },
        OperationOutput::Mutation {
            reply: MutationReply::Domain(DomainOutcome::Refuse {
                code: ErrorCode::WrongActor,
                detail: "wrong actor".into(),
            }),
        },
        OperationOutput::Error {
            code: "unavailable".into(),
            detail: "owner unavailable".into(),
        },
    ];
    let schema = descriptors()[0].output_schema().unwrap();
    for (index, output) in outputs.into_iter().enumerate() {
        let result = ApplicationResult {
            schema_version: 1,
            operation_id: None,
            condition: "observed".into(),
            result: output,
        };
        result.validate_metadata().unwrap();
        assert_eq!(result.is_error(), index >= 3);
        let value = serde_json::to_value(&result).unwrap();
        assert_shape(&schema, &schema, &value);
        assert_eq!(
            serde_json::from_value::<ApplicationResult>(value).unwrap(),
            result
        );
    }
}

// Small test-only checker for the finite structural schema subset we emit.
// Production does not evaluate arbitrary schemas or fetch remote references.
fn assert_shape(root: &Value, schema: &Value, value: &Value) {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        assert_shape(
            root,
            root.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
            value,
        );
        return;
    }
    if let Some(choices) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Value::as_array)
    {
        // Select through discriminants/types without catching test panics.
        let selected = choices
            .iter()
            .find(|candidate| shape_matches(root, candidate, value))
            .unwrap_or_else(|| panic!("no schema branch {value} {schema}"));
        assert_shape(root, selected, value);
        return;
    }
    if let Some(choices) = schema.get("enum").and_then(Value::as_array) {
        assert!(choices.contains(value), "{value} {schema}");
    }
    if let Some(expected) = schema.get("const") {
        assert_eq!(value, expected);
    }
    if let Some(kind) = schema.get("type") {
        assert!(type_matches(kind, value), "{value} {schema}");
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required {
                assert!(object.contains_key(key.as_str().unwrap()));
            }
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, value) in object {
                if let Some(property) = properties.get(name) {
                    assert_shape(root, property, value)
                } else {
                    assert_ne!(
                        schema.get("additionalProperties"),
                        Some(&Value::Bool(false)),
                        "{name}"
                    );
                }
            }
        }
    }
    if let Some(array) = value.as_array() {
        if let Some(max) = schema.get("maxItems").and_then(Value::as_u64) {
            assert!(array.len() as u64 <= max);
        }
        if let Some(item) = schema.get("items") {
            for value in array {
                assert_shape(root, item, value);
            }
        }
    }
    if let Some(number) = value.as_u64() {
        if let Some(min) = schema.get("minimum").and_then(Value::as_u64) {
            assert!(number >= min);
        }
        if let Some(max) = schema.get("maximum").and_then(Value::as_u64) {
            assert!(number <= max);
        }
    }
}
fn type_matches(kind: &Value, value: &Value) -> bool {
    if let Some(kinds) = kind.as_array() {
        return kinds.iter().any(|kind| type_matches(kind, value));
    }
    match kind.as_str() {
        Some("object") => value.is_object(),
        Some("array") => value.is_array(),
        Some("string") => value.is_string(),
        Some("integer") => value.is_u64() || value.is_i64(),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        _ => false,
    }
}
fn shape_matches(root: &Value, schema: &Value, value: &Value) -> bool {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return shape_matches(
            root,
            root.pointer(reference.strip_prefix('#').unwrap()).unwrap(),
            value,
        );
    }
    if schema
        .get("const")
        .is_some_and(|expected| expected != value)
    {
        return false;
    }
    if schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|choices| !choices.contains(value))
    {
        return false;
    }
    if let Some(choices) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Value::as_array)
        && !choices
            .iter()
            .any(|candidate| shape_matches(root, candidate, value))
    {
        return false;
    }
    if schema
        .get("type")
        .is_some_and(|kind| !type_matches(kind, value))
    {
        return false;
    }
    if let Some(object) = value.as_object() {
        if schema
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| {
                required
                    .iter()
                    .any(|key| !object.contains_key(key.as_str().unwrap()))
            })
        {
            return false;
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, value) in object {
                match properties.get(name) {
                    Some(property) if !shape_matches(root, property, value) => return false,
                    None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                        return false;
                    }
                    _ => {}
                }
            }
        }
    }
    true
}

#[test]
fn shared_mutation_intent_matches_frozen_legacy_fence_and_binds_revision() {
    // Frozen legacy MCP IntentFence encoded revision before the tagged DTO.
    #[derive(serde::Serialize)]
    struct LegacyIntentFence<'a> {
        revision: Option<ObjectRevision>,
        authored: &'a AuthoredOperation,
    }
    let post = parse("claim.post", &json!({"claim":id(10)}));
    assert_eq!(post.canonical_mutation_intent(None).unwrap(),
        br#"{"revision":null,"authored":{"operation":"claim.post","input":{"claim":"0000000000000000000000000000000a"}}}"#);
    assert_eq!(post.canonical_mutation_intent(Some(ObjectRevision(7))).unwrap(),
        br#"{"revision":7,"authored":{"operation":"claim.post","input":{"claim":"0000000000000000000000000000000a"}}}"#);
    for descriptor in descriptors()
        .iter()
        .filter(|descriptor| descriptor.mutation)
    {
        let authored = parse(descriptor.name, &fixture(descriptor.name));
        let original = authored.canonical_intent().unwrap();
        let mut encodings = BTreeSet::new();
        for revision in [
            None,
            Some(ObjectRevision(0)),
            Some(ObjectRevision(1)),
            Some(ObjectRevision(u64::MAX)),
        ] {
            let canonical = authored.canonical_mutation_intent(revision).unwrap();
            assert_eq!(
                canonical,
                serde_json::to_vec(&LegacyIntentFence {
                    revision,
                    authored: &authored
                })
                .unwrap(),
                "{}",
                descriptor.name
            );
            assert!(encodings.insert(canonical));
        }
        assert_eq!(authored.canonical_intent().unwrap(), original);
    }
}

#[test]
fn preflight_rejects_semantic_inputs_without_expanding_the_authored_identity_fields() {
    let mut invalid = claim();
    invalid.as_object_mut().unwrap().remove("id");
    invalid.as_object_mut().unwrap().remove("occurrence");
    invalid["target"] = json!("self"); // ordinary work cannot target its issuer
    let invalid = parse("claim.submit", &invalid);
    let before = invalid.canonical_intent().unwrap();
    assert!(matches!(
        invalid.preflight(&context()),
        Err(InputError::Invalid("self targeting requires handoff"))
    ));
    assert_eq!(invalid.canonical_intent().unwrap(), before);
    if let AuthoredOperation::ClaimSubmit(document) = &invalid {
        assert!(document.id.is_none() && document.occurrence.is_none());
    } else {
        panic!("claim")
    }
    for (name, input) in [
        ("receipt.acquire", json!({"claim":id(10),"epoch":0})),
        (
            "claim.cancel",
            json!({"claim":id(10),"reason":"x".repeat(16 * 1024 + 1)}),
        ),
        (
            "artifact.submit",
            json!({"claim":id(10),"receipt":{"id":id(13),"epoch":1},"evidence_set":id(14),"kind":"text","schema_hash":hash(5),"payload":{"type":"content","reference":{"domain":id(20),"root":hash(6),"length":1,"class":"unknown"}}}),
        ),
    ] {
        let input = parse(name, &input);
        let canonical = input.canonical_intent().unwrap();
        assert!(input.preflight(&context()).is_err(), "{name}");
        assert_eq!(input.canonical_intent().unwrap(), canonical);
    }
    let mut bad_context = context();
    bad_context.policy_revision = 0;
    assert!(
        parse("claim.post", &json!({"claim":id(10)}))
            .preflight(&bad_context)
            .is_err()
    );
}

#[test]
fn preflight_synthetic_ids_exclude_explicit_lineage_and_validation_ids() {
    let mut input = claim();
    input.as_object_mut().unwrap().remove("id");
    input.as_object_mut().unwrap().remove("occurrence");
    input["relations"] = Value::Array(
        (1..=252)
            .map(|number| {
                let kind = match number {
                    1 => "supersedes",
                    10 => "amends",
                    _ => "derived_from",
                };
                json!({"kind":kind,"target":id(number).to_uppercase()})
            })
            .collect(),
    );
    input["validations"][0]["id"] = json!(id(3));
    let mut second = input["validations"][0].clone();
    second.as_object_mut().unwrap().remove("id");
    second["description"] = json!("independent receipt check");
    input["validations"].as_array_mut().unwrap().push(second);
    let authored = parse("claim.submit", &input);
    let before = authored
        .canonical_mutation_intent(Some(ObjectRevision(3)))
        .unwrap();
    // A naive synthetic sequence invents self-lineage at id=1, although a real
    // generated claim has no authored identity. The preflight must avoid it.
    let mut naive = 0u128;
    assert!(matches!(
        authored.clone().build(&context(), &mut || {
            naive += 1;
            Ok(naive.to_be_bytes())
        }),
        Err(InputError::Invalid("self lineage"))
    ));
    authored.preflight(&context()).unwrap();
    assert_eq!(
        authored
            .canonical_mutation_intent(Some(ObjectRevision(3)))
            .unwrap(),
        before
    );
    let mut actual = 1_000u128;
    let PlannedOperation::Mutation(Command::GenerateClaim { claim }) = authored
        .build(&context(), &mut || {
            actual += 1;
            Ok(actual.to_be_bytes())
        })
        .unwrap()
    else {
        panic!("generation")
    };
    assert_eq!(actual, 1_003);
    assert_eq!(claim.id, ClaimId::from_u128(1_001));
    assert_eq!(claim.content.occurrence, OccurrenceId::from_u128(1_002));
    assert_eq!(claim.validations[0].id, ValidationId::from_u128(3));
    assert_eq!(claim.validations[1].id, ValidationId::from_u128(1_003));
}

#[test]
fn authored_id_scan_deduplicates_exact_strings_and_ignores_escaped_prose() {
    let mut values: Vec<String> = (1..=4_096u128)
        .rev()
        .map(|number| id(number).to_uppercase())
        .collect();
    values.extend([id(10), id(20), id(0)]);
    values.extend([
        format!("quoted \"{}\" prose", id(5_000)),
        format!("{} suffix", id(5_001)),
        format!("prefix {}", id(5_002)),
        hash(6),
    ]);
    let canonical = serde_json::to_vec(&values).unwrap();
    assert!(canonical.len() < MAX_INPUT_BYTES);
    let ids = authored_ids(&canonical).unwrap();
    assert_eq!(ids.len(), 4_096);
    assert_eq!(ids.first(), Some(&1u128.to_be_bytes()));
    assert_eq!(ids.last(), Some(&4_096u128.to_be_bytes()));
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    for absent in [0u128, 5_000, 5_001, 5_002] {
        assert!(ids.binary_search(&absent.to_be_bytes()).is_err());
    }
}
