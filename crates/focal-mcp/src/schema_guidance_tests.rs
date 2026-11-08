//! Every advertised tool's input schema as the latest specification asks of `Tool.inputSchema`
//! (2026-07-28, with its erratum of 2026-11-06, modelcontextprotocol 38353251, #3185): an object at
//! its root ("`type: \"object\"` is required at the root"), its arguments in the root's
//! `properties`, and no argument whose type is checked only inside a composition branch that a
//! sibling branch can take instead. The erratum's words: "A subschema that fails on a type error
//! is indistinguishable from one that does not apply, so type constraints placed inside `oneOf`
//! branches are not enforced for whichever branch does not match"; its example accepted
//! `{"id": "r1", "name": 123}` because the branch that typed `name` failed and the other matched.
use serde_json::Value;
use std::collections::BTreeSet;

/// Every tool the adapter can advertise: the V1 catalogue with its transfer, watch and
/// administration pages, and the native catalogue for each role and profile a standing can hold.
fn tools() -> Vec<crate::Tool> {
    let mut tools = crate::catalog::catalog().unwrap();
    crate::catalog_transfer::append(&mut tools).unwrap();
    crate::catalog_watch::append(&mut tools).unwrap();
    crate::catalog_admin::append(&mut tools).unwrap();
    for role in [
        focal_wire::NativePeerRole::Actor,
        focal_wire::NativePeerRole::Evaluator,
        focal_wire::NativePeerRole::Runtime,
    ] {
        for profile in [
            focal_wire::NativeProfile::AuthoredV1,
            focal_wire::NativeProfile::ProjectionOnly,
        ] {
            tools.extend(
                crate::catalog_native::catalog(&focal_wire::NativeStanding {
                    principal: focal_model::ParticipantId::from_u128(1),
                    role,
                    profile,
                    native_sequence: focal_model::SessionSeq(1),
                    logical_time: 1,
                })
                .unwrap(),
            );
        }
    }
    tools
}

fn resolve<'a>(schema: &'a Value, root: &'a Value) -> &'a Value {
    match schema.get("$ref").and_then(Value::as_str) {
        Some(reference) => root
            .pointer(reference.strip_prefix('#').unwrap())
            .unwrap_or_else(|| panic!("unresolved {reference}")),
        None => schema,
    }
}

fn keys(schema: &Value, name: &str) -> BTreeSet<String> {
    schema
        .get(name)
        .and_then(Value::as_object)
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

/// Whether `branch` can take an object while leaving `key` unchecked: it admits objects (no type,
/// or `object`; no `const` or `enum`), it is open to properties it does not name, and it does not
/// type `key` itself.
fn takes_unchecked(branch: &Value, key: &str) -> bool {
    let admits_objects = match branch.get("type") {
        None => true,
        Some(Value::String(kind)) => kind == "object",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == "object"),
        Some(_) => false,
    } && branch.get("const").is_none()
        && branch.get("enum").is_none();
    let open = branch.get("additionalProperties") != Some(&Value::Bool(false));
    admits_objects && open && !keys(branch, "properties").contains(key)
}

/// The places in `schema` where a property is typed only inside a `oneOf`/`anyOf` branch that a
/// sibling branch can take with it unchecked, as `path: keyword[branch].key`. Walked with a stack:
/// `typed` is what the instance at that place has typed already (its own `properties` and those of
/// the compositions it sits inside), and a `$ref` is followed once on each path.
fn bypasses(input: &Value) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack: Vec<(&Value, String, BTreeSet<String>, BTreeSet<String>)> =
        vec![(input, String::new(), BTreeSet::new(), BTreeSet::new())];
    while let Some((node, path, inherited, followed)) = stack.pop() {
        assert!(
            path.len() < 4096,
            "a schema deeper than a tool's could be: {path}"
        );
        let mut followed = followed;
        let node = match node.get("$ref").and_then(Value::as_str) {
            Some(reference) if !followed.insert(reference.to_owned()) => continue,
            _ => resolve(node, input),
        };
        let mut typed = inherited;
        typed.extend(keys(node, "properties"));
        if let Some(properties) = node.get("properties").and_then(Value::as_object) {
            for (name, child) in properties {
                stack.push((
                    child,
                    format!("{path}/{name}"),
                    BTreeSet::new(),
                    followed.clone(),
                ));
            }
        }
        if let Some(items) = node.get("items") {
            stack.push((
                items,
                format!("{path}[]"),
                BTreeSet::new(),
                followed.clone(),
            ));
        }
        for keyword in ["oneOf", "anyOf", "allOf"] {
            let Some(branches) = node.get(keyword).and_then(Value::as_array) else {
                continue;
            };
            let branches: Vec<&Value> = branches.iter().map(|b| resolve(b, input)).collect();
            for (index, branch) in branches.iter().enumerate() {
                if keyword != "allOf" {
                    for key in keys(branch, "properties") {
                        let bypassed = !typed.contains(&key)
                            && branches.iter().enumerate().any(|(other, sibling)| {
                                other != index && takes_unchecked(sibling, &key)
                            });
                        if bypassed {
                            found.push(format!("{path}: {keyword}[{index}].{key}"));
                        }
                    }
                }
                stack.push((
                    branch,
                    format!("{path}:{keyword}[{index}]"),
                    typed.clone(),
                    followed.clone(),
                ));
            }
        }
    }
    found
}

#[test]
fn every_tool_takes_an_object_whose_arguments_its_root_names() {
    let tools = tools();
    assert!(tools.len() > 50, "{} tools", tools.len());
    for tool in &tools {
        let input = &tool.input_schema;
        assert_eq!(input["type"], "object", "{}", tool.name);
        let named = keys(input, "properties");
        assert!(input["properties"].is_object(), "{}", tool.name);
        // Every argument a composition at the root asks for or types is one the root names, so a
        // client that lists `inputSchema.properties` lists every argument.
        for keyword in ["oneOf", "anyOf", "allOf"] {
            for branch in input
                .get(keyword)
                .and_then(Value::as_array)
                .unwrap_or(&Vec::new())
            {
                let branch = resolve(branch, input);
                for key in keys(branch, "properties").into_iter().chain(
                    branch
                        .get("required")
                        .and_then(Value::as_array)
                        .unwrap_or(&Vec::new())
                        .iter()
                        .filter_map(|key| key.as_str().map(str::to_owned)),
                ) {
                    assert!(
                        named.contains(&key),
                        "{}: {keyword} names `{key}`, which the root does not",
                        tool.name
                    );
                }
            }
        }
    }
}

#[test]
fn no_tool_checks_an_argument_only_in_a_branch_another_branch_can_take_instead() {
    let mut found = Vec::new();
    for tool in tools() {
        for place in bypasses(&tool.input_schema) {
            found.push(format!("{} {place}", tool.name));
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}

/// The check finds the erratum's own example as it stood before the fix, and passes it as fixed.
#[test]
fn the_check_finds_the_example_the_erratum_fixed() {
    let before = serde_json::json!({"type":"object","oneOf":[
        {"properties":{"id":{"type":"string"}},"required":["id"]},
        {"properties":{"name":{"type":"string"}},"required":["name"]}
    ]});
    assert_eq!(bypasses(&before), [": oneOf[0].id", ": oneOf[1].name"]);
    let after = serde_json::json!({"type":"object",
        "properties":{"id":{"type":"string"},"name":{"type":"string"}},
        "oneOf":[{"required":["id"]},{"required":["name"]}]});
    assert!(bypasses(&after).is_empty());
    // A tagged union whose branches are closed cannot be bypassed: a branch that fails cannot be
    // stood in for by another, which refuses what it does not name.
    let closed = serde_json::json!({"type":"object","properties":{"payload":{"oneOf":[
        {"type":"object","additionalProperties":false,"required":["type","bytes"],
         "properties":{"type":{"const":"inline"},"bytes":{"type":"array"}}},
        {"type":"object","additionalProperties":false,"required":["type","text"],
         "properties":{"type":{"const":"text"},"text":{"type":"string"}}}
    ]}}});
    assert!(bypasses(&closed).is_empty());
}
