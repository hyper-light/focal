use super::*;
use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};
use serde_json::{Value, json};

fn budget() -> MemoryBudget {
    MemoryBudget::new(64 * 1024 * 1024, 32 * 1024 * 1024).unwrap()
}
fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: "Read a value".into(),
        input_schema: json!({"type":"object","properties":{}}),
        output_schema: json!({"type":"object"}),
        read_only: true,
        destructive: false,
        idempotent: true,
    }
}
fn protocol(limits: Limits, names: &[&str]) -> Protocol {
    Protocol::new(
        limits,
        budget(),
        ServerInfo {
            name: "focal".into(),
            version: "0.1.0".into(),
        },
        names.iter().map(|n| tool(n)).collect(),
    )
    .unwrap()
}
fn modern(id: Value, method: &str, mut params: Value) -> Vec<u8> {
    params.as_object_mut().unwrap().insert("_meta".into(),json!({"io.modelcontextprotocol/protocolVersion":MODERN_VERSION,"io.modelcontextprotocol/clientCapabilities":{}}));
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})).unwrap()
}
fn reply(action: Action) -> Value {
    let Action::Reply(frame) = action else {
        panic!("expected reply")
    };
    assert_eq!(frame.as_bytes().last(), Some(&b'\n'));
    serde_json::from_slice(frame.as_bytes()).unwrap()
}
fn call(action: Action) -> ToolCall {
    let Action::Call(call) = action else {
        panic!("expected call")
    };
    call
}
fn legacy_ready(p: &mut Protocol) {
    let result=reply(p.receive(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#).unwrap());
    assert_eq!(result["result"]["protocolVersion"], LEGACY_VERSION);
    assert!(matches!(
        p.receive(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
            .unwrap(),
        Action::NoReply
    ));
}

#[test]
fn modern_discovery_and_direct_call_match_pinned_schema() {
    let mut p = protocol(Limits::default(), &["claim.get"]);
    let result = reply(
        p.receive(&modern(json!(1), "server/discover", json!({})))
            .unwrap(),
    );
    validate_fixture(MODERN_VERSION, "DiscoverResult", &result["result"]);
    assert_eq!(
        result["result"]["supportedVersions"],
        json!([MODERN_VERSION, LEGACY_VERSION])
    );
    assert_eq!(result["result"]["cacheScope"], "private");
    let mut direct = protocol(Limits::default(), &["claim.get"]);
    let work = call(
        direct
            .receive(&modern(
                json!("read"),
                "tools/call",
                json!({"name":"claim.get","arguments":{"id":"abc"}}),
            ))
            .unwrap(),
    );
    assert_eq!(work.arguments["id"], "abc");
    let frame = direct
        .complete(work.token, &json!({"value":7}), false)
        .unwrap()
        .unwrap();
    let result: Value = serde_json::from_slice(frame.as_bytes()).unwrap();
    validate_fixture(MODERN_VERSION, "CallToolResult", &result["result"]);
    assert_eq!(result["result"]["structuredContent"], json!({"value":7}));
    assert_eq!(
        serde_json::from_str::<Value>(result["result"]["content"][0]["text"].as_str().unwrap())
            .unwrap(),
        json!({"value":7})
    );
}

#[test]
fn legacy_negotiation_and_modern_metadata_are_separate() {
    let mut p = protocol(Limits::default(), &["claim.get"]);
    assert_eq!(
        reply(
            p.receive(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
                .unwrap()
        )["error"]["code"],
        -32602
    );
    let init=reply(p.receive(br#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#).unwrap());
    validate_fixture(LEGACY_VERSION, "InitializeResult", &init["result"]);
    assert_eq!(init["result"]["protocolVersion"], LEGACY_VERSION);
    assert_eq!(
        reply(
            p.receive(br#"{"jsonrpc":"2.0","id":3,"method":"tools/list"}"#)
                .unwrap()
        )["error"]["code"],
        -32602
    );
    p.receive(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .unwrap();
    let page = reply(
        p.receive(br#"{"jsonrpc":"2.0","id":4,"method":"tools/list"}"#)
            .unwrap(),
    );
    validate_fixture(LEGACY_VERSION, "ListToolsResult", &page["result"]);
    assert!(page["result"].get("resultType").is_none());
    assert_eq!(
        reply(
            p.receive(br#"{"jsonrpc":"2.0","id":5,"method":"ping"}"#)
                .unwrap()
        )["result"],
        json!({})
    );
    let page = reply(
        p.receive(&modern(json!(6), "tools/list", json!({})))
            .unwrap(),
    );
    assert_eq!(page["result"]["resultType"], "complete");
    let bad=br#"{"jsonrpc":"2.0","id":7,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/clientCapabilities":{}}}}"#;
    assert_eq!(reply(p.receive(bad).unwrap())["error"]["code"], -32602);
}

#[test]
fn version_capabilities_and_protocol_error_codes() {
    let mut p = protocol(Limits::default(), &[]);
    let request = modern(json!(1), "tools/list", json!({}));
    let mut v: Value = serde_json::from_slice(&request).unwrap();
    v["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
    let e = reply(p.receive(&serde_json::to_vec(&v).unwrap()).unwrap());
    assert_eq!(e["error"]["code"], -32022);
    assert_eq!(e["error"]["data"]["requested"], "2099-01-01");
    assert_eq!(reply(p.receive(b"{").unwrap())["error"]["code"], -32700);
    assert_eq!(reply(p.receive(b"[]").unwrap())["error"]["code"], -32600);
    assert_eq!(
        reply(p.receive(&modern(json!(1), "unknown", json!({}))).unwrap())["error"]["code"],
        -32601
    );
    assert_eq!(
        reply(
            p.receive(&modern(json!(1), "tools/call", json!({"name":"absent"})))
                .unwrap()
        )["error"]["code"],
        -32602
    );
    let mut v: Value = serde_json::from_slice(&request).unwrap();
    v["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] =
        json!({"future-capability":true,"extensions":{"org.example/custom":{}}});
    assert!(
        reply(p.receive(&serde_json::to_vec(&v).unwrap()).unwrap())
            .get("result")
            .is_some()
    );
    v["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"] = json!({"roots":false});
    assert_eq!(
        reply(p.receive(&serde_json::to_vec(&v).unwrap()).unwrap())["error"]["code"],
        -32602
    );
}

#[test]
fn cancellation_keeps_slot_then_suppresses_reply_and_fences_reused_id() {
    let mut p = protocol(
        Limits {
            max_active_calls: 1,
            ..Limits::default()
        },
        &["claim.get"],
    );
    let bytes = modern(json!(1), "tools/call", json!({"name":"claim.get"}));
    let work = call(p.receive(&bytes).unwrap());
    assert!(matches!(p.receive(&bytes), Err(ProtocolError::DuplicateId)));
    assert!(
        matches!(p.receive(br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#).unwrap(),Action::Cancel(token) if token==work.token)
    );
    assert_eq!(p.active_calls(), 1);
    let busy = reply(
        p.receive(&modern(json!(2), "tools/call", json!({"name":"claim.get"})))
            .unwrap(),
    );
    assert_eq!(busy["result"]["isError"], true);
    assert!(p.complete(work.token, &json!({}), false).unwrap().is_none());
    let new = call(p.receive(&bytes).unwrap());
    assert_ne!(work.token, new.token);
    assert!(p.complete(work.token, &json!({}), false).unwrap().is_none());
    assert!(p.complete(new.token, &json!({}), false).unwrap().is_some());
    assert!(matches!(
        p.receive(
            br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#
        )
        .unwrap(),
        Action::NoReply
    ));
}

#[test]
fn string_and_numeric_ids_remain_distinct_and_tool_failure_is_not_rpc_error() {
    let mut p = protocol(Limits::default(), &["claim.get"]);
    let a = call(
        p.receive(&modern(json!(1), "tools/call", json!({"name":"claim.get"})))
            .unwrap(),
    );
    let b = call(
        p.receive(&modern(
            json!("1"),
            "tools/call",
            json!({"name":"claim.get"}),
        ))
        .unwrap(),
    );
    let frame = p
        .fail(a.token, "backend unavailable; inspect original operation")
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(frame.as_bytes()).unwrap();
    assert_eq!(v["id"], 1);
    assert!(v.get("error").is_none());
    assert_eq!(v["result"]["isError"], true);
    let frame = p.complete(b.token, &json!({}), false).unwrap().unwrap();
    let v: Value = serde_json::from_slice(frame.as_bytes()).unwrap();
    assert_eq!(v["id"], "1");
}

#[test]
fn paged_catalog_cursor_binds_owner_catalog_position_and_expiry() {
    let limits = Limits {
        tools_per_page: 1,
        cursor_ttl_ms: 10,
        ..Limits::default()
    };
    let mut p = protocol(limits, &["b", "a", "c"]);
    let first = reply(
        p.receive_at(&modern(json!(1), "tools/list", json!({})), 100)
            .unwrap(),
    );
    validate_fixture(MODERN_VERSION, "ListToolsResult", &first["result"]);
    assert_eq!(first["result"]["tools"][0]["name"], "a");
    let cursor = first["result"]["nextCursor"].as_str().unwrap();
    let next = modern(json!(2), "tools/list", json!({"cursor":cursor}));
    assert_eq!(
        reply(p.receive_at(&next, 101).unwrap())["result"]["tools"][0]["name"],
        "b"
    );
    let mut other = protocol(limits, &["a", "b", "c"]);
    assert_eq!(
        reply(other.receive_at(&next, 101).unwrap())["error"]["code"],
        -32602
    );
    let changed = cursor.replacen("1:", "2:", 1);
    assert_eq!(
        reply(
            p.receive_at(
                &modern(json!(3), "tools/list", json!({"cursor":changed})),
                102
            )
            .unwrap()
        )["error"]["code"],
        -32602
    );
    assert_eq!(
        reply(p.receive_at(&next, 110).unwrap())["error"]["code"],
        -32602
    );
    assert_eq!(
        reply(p.receive_at(&next, 99).unwrap())["error"]["code"],
        -32602
    );
}

#[test]
fn framing_fragmentation_bounds_eof_and_detached_ownership() {
    let budget = budget();
    let start = budget.stats().used;
    let mut decoder = FrameDecoder::new(32, budget.clone()).unwrap();
    let input = b"{\"utf8\":\"\xc3\xa9\"}\r\n{}\n";
    for byte in &input[..14] {
        assert!(
            decoder
                .push(std::slice::from_ref(byte))
                .unwrap()
                .1
                .is_none()
        );
    }
    let (used, frame) = decoder.push(&input[14..]).unwrap();
    assert_eq!(used, 1);
    let frame = frame.unwrap();
    assert_eq!(frame.as_bytes(), b"{\"utf8\":\"\xc3\xa9\"}");
    drop(decoder);
    assert!(budget.stats().used > start);
    drop(frame);
    assert_eq!(budget.stats().used, start);
    let mut decoder = FrameDecoder::new(2, budget.clone()).unwrap();
    assert!(matches!(decoder.push(b"123"), Err(ProtocolError::Frame)));
    assert_eq!(budget.stats().used, start);
    assert!(matches!(decoder.push(b"\n"), Err(ProtocolError::Closed)));
    let mut decoder = FrameDecoder::new(4, budget.clone()).unwrap();
    decoder.push(b"{}").unwrap();
    assert!(matches!(decoder.finish(), Err(ProtocolError::Frame)));
    assert_eq!(budget.stats().used, start);
}

#[test]
fn parser_rejects_duplicate_keys_depth_nodes_and_invalid_ids() {
    let mut p = protocol(
        Limits {
            max_depth: 4,
            max_nodes: 32,
            ..Limits::default()
        },
        &[],
    );
    for bytes in [
        b"{\"jsonrpc\":\"2.0\",\"jsonrpc\":\"2.0\"}".as_slice(),
        b"[[[[[[1]]]]]]",
        b"\xff",
    ] {
        assert_eq!(reply(p.receive(bytes).unwrap())["error"]["code"], -32700);
    }
    for id in [
        json!(null),
        json!(1.5),
        json!(true),
        json!(vec![1]),
        json!("x".repeat(129)),
    ] {
        let bytes = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":"x"})).unwrap();
        assert_eq!(reply(p.receive(&bytes).unwrap())["error"]["code"], -32600);
    }
}

#[test]
fn tool_and_response_allocations_survive_protocol_drop_and_control_survives_pressure() {
    let budget = budget();
    let mut p = Protocol::new(
        Limits::default(),
        budget.clone(),
        ServerInfo {
            name: "focal".into(),
            version: "1".into(),
        },
        vec![tool("claim.get")],
    )
    .unwrap();
    let work = call(
        p.receive(&modern(
            json!(1),
            "tools/call",
            json!({"name":"claim.get","arguments":{"data":"value"}}),
        ))
        .unwrap(),
    );
    let frame = p
        .complete(work.token, &json!({"data":"result"}), false)
        .unwrap()
        .unwrap();
    drop(p);
    assert!(budget.stats().used > 0);
    drop(work);
    assert!(budget.stats().used > 0);
    drop(frame);
    assert_eq!(budget.stats().used, 0);
    let mut p = Protocol::new(
        Limits::default(),
        budget.clone(),
        ServerInfo {
            name: "focal".into(),
            version: "1".into(),
        },
        vec![tool("claim.get")],
    )
    .unwrap();
    let stats = budget.stats();
    let fill = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap()
        .commit();
    let result = reply(
        p.receive(&modern(json!(1), "tools/call", json!({"name":"claim.get"})))
            .unwrap(),
    );
    assert_eq!(result["result"]["isError"], true);
    assert!(
        reply(
            p.receive(&modern(json!(2), "server/discover", json!({})))
                .unwrap()
        )
        .get("result")
        .is_some()
    );
    drop(fill);
}

#[test]
fn bounded_encoding_retains_call_on_failure_and_catches_serializer_panic() {
    let mut p = protocol(
        Limits {
            max_response_bytes: 512,
            ..Limits::default()
        },
        &["claim.get"],
    );
    let work = call(
        p.receive(&modern(json!(1), "tools/call", json!({"name":"claim.get"})))
            .unwrap(),
    );
    assert!(matches!(
        p.complete(work.token, &json!({"large":"x".repeat(600)}), false),
        Err(ProtocolError::Encode)
    ));
    assert_eq!(p.active_calls(), 1);
    struct Bad;
    impl serde::Serialize for Bad {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("injected serializer panic")
        }
    }
    assert!(matches!(
        p.complete(work.token, &Bad, false),
        Err(ProtocolError::Dependency)
    ));
    assert_eq!(p.active_calls(), 1);
    assert!(
        p.fail(work.token, "result exceeded capacity")
            .unwrap()
            .is_some()
    );
}

#[test]
fn legacy_call_result_and_unknown_notifications() {
    let mut p = protocol(Limits::default(), &["claim.get"]);
    legacy_ready(&mut p);
    let work = call(
        p.receive(
            br#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"claim.get"}}"#,
        )
        .unwrap(),
    );
    let frame = p
        .complete(work.token, &json!({"ok":true}), false)
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_slice(frame.as_bytes()).unwrap();
    validate_fixture(LEGACY_VERSION, "CallToolResult", &v["result"]);
    assert!(v["result"].get("resultType").is_none());
    assert!(matches!(
        p.receive(
            br#"{"jsonrpc":"2.0","method":"notifications/future","params":{"anything":true}}"#
        )
        .unwrap(),
        Action::NoReply
    ));
}

// Checks the official schema constructs reached by the emitted protocol messages.
// This test helper is deliberately not a production/general JSON Schema engine.
fn validate_fixture(version: &str, name: &str, value: &Value) {
    let schema: Value = serde_json::from_str(if version == MODERN_VERSION {
        include_str!("../fixtures/2026-07-28.schema.json")
    } else {
        include_str!("../fixtures/2025-11-25.schema.json")
    })
    .unwrap();
    assert!(
        matches_schema(&schema["$defs"][name], value, &schema, 0),
        "{name}: {value}"
    );
}
fn matches_schema(schema: &Value, value: &Value, root: &Value, depth: usize) -> bool {
    assert!(depth < 128);
    if let Some(r) = schema.get("$ref").and_then(Value::as_str) {
        return matches_schema(
            root.pointer(r.strip_prefix('#').unwrap()).unwrap(),
            value,
            root,
            depth + 1,
        );
    }
    if schema.get("const").is_some_and(|v| v != value) {
        return false;
    }
    if schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|v| !v.contains(value))
    {
        return false;
    }
    if schema
        .get("anyOf")
        .and_then(Value::as_array)
        .is_some_and(|a| !a.iter().any(|s| matches_schema(s, value, root, depth + 1)))
    {
        return false;
    }
    if schema
        .get("oneOf")
        .and_then(Value::as_array)
        .is_some_and(|a| {
            a.iter()
                .filter(|s| matches_schema(s, value, root, depth + 1))
                .count()
                != 1
        })
    {
        return false;
    }
    if let Some(kind) = schema.get("type").and_then(Value::as_str)
        && !match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => panic!("unknown type {kind}"),
        }
    {
        return false;
    }
    if let Some(min) = schema.get("minimum").and_then(Value::as_f64)
        && value.as_f64().is_some_and(|v| v < min)
    {
        return false;
    }
    if schema
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|a| !a.iter().all(|k| value.get(k.as_str().unwrap()).is_some()))
    {
        return false;
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (key, s) in properties {
            if let Some(v) = value.get(key)
                && !matches_schema(s, v, root, depth + 1)
            {
                return false;
            }
        }
    }
    if let Some(items) = schema.get("items")
        && let Some(a) = value.as_array()
        && !a.iter().all(|v| matches_schema(items, v, root, depth + 1))
    {
        return false;
    }
    if let Some(additional) = schema.get("additionalProperties")
        && let Some(o) = value.as_object()
    {
        for (k, v) in o {
            if schema.get("properties").and_then(|p| p.get(k)).is_none()
                && (additional == &Value::Bool(false)
                    || (!additional.is_boolean()
                        && !matches_schema(additional, v, root, depth + 1)))
            {
                return false;
            }
        }
    }
    true
}

#[test]
fn fixture_digests() {
    let provenance: Value =
        serde_json::from_str(include_str!("../fixtures/provenance.json")).unwrap();
    assert_eq!(
        provenance["commit"],
        "e76e9c572c6f2bfcb730357101acc90f2f802e02"
    );
    for (name, bytes) in [
        (
            "2026-07-28.schema.json",
            include_bytes!("../fixtures/2026-07-28.schema.json").as_slice(),
        ),
        (
            "2025-11-25.schema.json",
            include_bytes!("../fixtures/2025-11-25.schema.json").as_slice(),
        ),
        (
            "2026-07-28.schema.ts",
            include_bytes!("../fixtures/2026-07-28.schema.ts").as_slice(),
        ),
        (
            "2025-11-25.schema.ts",
            include_bytes!("../fixtures/2025-11-25.schema.ts").as_slice(),
        ),
        (
            "UPSTREAM-LICENSE",
            include_bytes!("../fixtures/UPSTREAM-LICENSE").as_slice(),
        ),
    ] {
        let entry = provenance["schemas"]
            .as_array()
            .unwrap()
            .iter()
            .chain(provenance["sources"].as_array().unwrap())
            .find(|e| e["file"] == name)
            .unwrap();
        assert_eq!(entry["blake3"], blake3::hash(bytes).to_hex().as_str());
        assert_eq!(entry["bytes"].as_u64().unwrap(), bytes.len() as u64);
    }
}
