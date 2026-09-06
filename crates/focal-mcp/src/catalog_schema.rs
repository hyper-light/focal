//! Discard only unreachable definitions from a tool's owned schema. The shared
//! registry includes every authored DTO; retaining those for every tool would
//! multiply unrelated schema residency and prevent first-call admission.
use crate::ProtocolError;
use serde_json::{Map, Value};

const MAX_DEFINITIONS: usize = 256;

pub(super) fn specialize_output(schema: &mut Value, kinds: &[&str]) -> Result<(), ProtocolError> {
    let branches = schema
        .get_mut("properties")
        .and_then(|v| v.get_mut("result"))
        .and_then(|v| v.get_mut("oneOf"))
        .and_then(Value::as_array_mut)
        .ok_or(ProtocolError::Limits)?;
    // Fail closed if the released result schema no longer contains a required
    // branch. Never manufacture a permissive fallback for an unknown variant.
    for kind in kinds {
        if branches
            .iter()
            .filter(|branch| branch_kind(branch) == Some(kind))
            .count()
            != 1
        {
            return Err(ProtocolError::Limits);
        }
    }
    branches.retain(|branch| branch_kind(branch).is_some_and(|kind| kinds.contains(&kind)));
    prune_definitions(schema)
}

fn branch_kind(branch: &Value) -> Option<&str> {
    branch
        .get("properties")?
        .get("kind")?
        .get("const")?
        .as_str()
}

pub(super) fn prune_definitions(schema: &mut Value) -> Result<(), ProtocolError> {
    let root = schema.as_object_mut().ok_or(ProtocolError::Limits)?;
    let Some(definitions) = root.remove("$defs") else {
        return Ok(());
    };
    let Value::Object(mut definitions) = definitions else {
        return Err(ProtocolError::Limits);
    };
    if definitions.len() > MAX_DEFINITIONS {
        return Err(ProtocolError::Limits);
    }
    let mut needed = [false; MAX_DEFINITIONS];
    mark_reference(root, &definitions, &mut needed, 0)?;
    for value in root.values() {
        mark(value, &definitions, &mut needed, 0)?;
    }
    let mut index = 0;
    definitions.retain(|_, _| {
        let keep = needed.get(index).copied().unwrap_or(false);
        index = index.saturating_add(1);
        keep
    });
    if !definitions.is_empty() {
        root.insert("$defs".into(), Value::Object(definitions));
    }
    Ok(())
}

fn mark(
    value: &Value,
    definitions: &Map<String, Value>,
    needed: &mut [bool; MAX_DEFINITIONS],
    depth: usize,
) -> Result<(), ProtocolError> {
    if depth >= 64 {
        return Err(ProtocolError::Limits);
    }
    let next = depth.checked_add(1).ok_or(ProtocolError::Limits)?;
    match value {
        Value::Object(object) => {
            mark_reference(object, definitions, needed, next)?;
            for child in object.values() {
                mark(child, definitions, needed, next)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                mark(child, definitions, needed, next)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn mark_reference(
    object: &Map<String, Value>,
    definitions: &Map<String, Value>,
    needed: &mut [bool; MAX_DEFINITIONS],
    depth: usize,
) -> Result<(), ProtocolError> {
    if let Some(reference) = object
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|s| s.strip_prefix("#/$defs/"))
    {
        let index = definitions
            .keys()
            .position(|key| key == reference)
            .ok_or(ProtocolError::Limits)?;
        let slot = needed.get_mut(index).ok_or(ProtocolError::Limits)?;
        if !*slot {
            *slot = true;
            mark(
                definitions.get(reference).ok_or(ProtocolError::Limits)?,
                definitions,
                needed,
                depth,
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Action, Limits, Protocol, ServerInfo};
    use focal_memory::{BudgetKind, BudgetLane, MemoryBudget};

    #[test]
    fn pruning_retains_transitive_cycles_and_rejects_missing_definitions() {
        let mut schema = serde_json::json!({"type":"object","properties":{"a":{"$ref":"#/$defs/a"}},"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"},"unused":{"type":"integer"}}});
        prune_definitions(&mut schema).unwrap();
        assert_eq!(schema["$defs"].as_object().unwrap().len(), 2);
        assert!(schema["$defs"].get("unused").is_none());
        let mut missing = serde_json::json!({"type":"object","$ref":"#/$defs/missing","$defs":{}});
        assert!(prune_definitions(&mut missing).is_err());
    }

    #[test]
    fn specialized_catalogue_preserves_exact_released_branches_and_unique_schema_ids() {
        let full = focal_client::operations::descriptors()[0]
            .output_schema()
            .unwrap();
        let original = full["properties"]["result"]["oneOf"].as_array().unwrap();
        let mut tools = crate::catalog::catalog().unwrap();
        crate::catalog_admin::append(&mut tools).unwrap();
        crate::catalog_transfer::append(&mut tools).unwrap();
        crate::catalog_watch::append(&mut tools).unwrap();
        let mut ids = std::collections::BTreeSet::new();
        for tool in &tools {
            let branches = tool.output_schema["properties"]["result"]["oneOf"]
                .as_array()
                .unwrap();
            assert!(branches.len() < original.len(), "{}", tool.name);
            assert!(
                branches.iter().all(|branch| original.contains(branch)),
                "{}",
                tool.name
            );
            assert!(
                branches
                    .iter()
                    .any(|branch| branch["properties"]["kind"]["const"] == "error")
            );
            // Admin tools intentionally share their identical family schema.
            if !tool.name.starts_with("cluster.") {
                assert!(ids.insert(tool.output_schema["$id"].as_str().unwrap()));
            }
            if let Some(definitions) = tool.output_schema["$defs"].as_object() {
                for (key, definition) in definitions {
                    assert_eq!(Some(definition), full["$defs"].get(key));
                }
            }
        }
        for (name, expected) in [
            ("claim.submit", vec!["mutation", "error", "managed"]),
            ("validation.context", vec!["error", "validation_context"]),
            ("upload.begin", vec!["error", "upload"]),
            ("artifact.download", vec!["error", "artifact_payload"]),
            ("watch.inspect", vec!["watch", "watches", "error"]),
            ("request.reserve", vec!["error", "managed_request"]),
        ] {
            let tool = tools.iter().find(|tool| tool.name == name).unwrap();
            let actual: Vec<_> = tool.output_schema["properties"]["result"]["oneOf"]
                .as_array()
                .unwrap()
                .iter()
                .map(|branch| branch["properties"]["kind"]["const"].as_str().unwrap())
                .collect();
            assert_eq!(actual, expected, "{name}");
        }
        let mut missing = full;
        assert!(specialize_output(&mut missing, &["invented"]).is_err());
    }

    #[test]
    fn full_optional_catalogue_leaves_room_for_a_real_first_job() {
        const MIB: usize = 1024 * 1024;
        let budget = MemoryBudget::new(128 * MIB, 80 * MIB).unwrap();
        let count = focal_client::operations::descriptors().len()
            + 6
            + crate::catalog_admin::TOOL_COUNT
            + crate::catalog_transfer::TOOL_COUNT
            + crate::catalog_watch::TOOL_COUNT;
        let construction = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Ordinary,
                (count + 1) * 512 * 1024,
            )
            .unwrap()
            .commit();
        let mut tools = crate::catalog::catalog().unwrap();
        crate::catalog_admin::append(&mut tools).unwrap();
        crate::catalog_transfer::append(&mut tools).unwrap();
        crate::catalog_watch::append(&mut tools).unwrap();
        let limits = Limits {
            max_frame_bytes: 278_528,
            max_response_bytes: 16 * MIB,
            max_active_calls: 1,
            ..Limits::default()
        };
        let mut protocol = Protocol::new(
            limits,
            budget.clone(),
            ServerInfo {
                name: "focal".into(),
                version: "1".into(),
            },
            tools,
        )
        .unwrap();
        drop(construction);
        let catalog = budget.stats();
        let _queues = budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, 64 * 1024)
            .unwrap()
            .commit();
        let _decoder = crate::FrameDecoder::new(limits.max_frame_bytes, budget.clone()).unwrap();
        let request = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":crate::MODERN_VERSION,"io.modelcontextprotocol/clientCapabilities":{}},"name":"upload.begin","arguments":{"upload_id":"00000000000000000000000000000001","length":0,"digest":"00".repeat(32)}}});
        let Action::Call(_call) = protocol
            .receive(&serde_json::to_vec(&request).unwrap())
            .unwrap()
        else {
            panic!("first tool call rejected");
        };
        let result = budget.reserve(BudgetKind::Control, BudgetLane::Ordinary, 32 * MIB);
        assert!(
            result.is_ok(),
            "catalogue={catalog:?}, before job={:?}, result={result:?}",
            budget.stats()
        );
    }
}
