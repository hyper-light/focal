use crate::catalog;
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct Manifest {
    schema_version: u16,
    adapter: Adapter,
    skills: Vec<Skill>,
}
#[derive(Deserialize)]
struct Adapter {
    name: String,
    version: String,
    recovery_contract_version: u16,
    transfer_contract_version: u16,
    watch_contract_version: u16,
    administration_contract_version: u16,
}
#[derive(Deserialize)]
struct Skill {
    name: String,
    version: u16,
    required_operations: Vec<Required>,
    required_recovery_tools: Vec<Required>,
    required_transfer_tools: Vec<Required>,
    required_watch_tools: Vec<Required>,
    required_admin_tools: Vec<Required>,
}
#[derive(Deserialize)]
struct Required {
    name: String,
    version: u16,
}

#[test]
fn skills_require_only_real_advertised_tools_and_exact_recovery_contract() {
    let manifest: Manifest = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../skills/manifest.json"
    )))
    .unwrap();
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.adapter.name, "focal-mcp");
    assert_eq!(manifest.adapter.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest.adapter.recovery_contract_version, 2);
    assert_eq!(manifest.adapter.transfer_contract_version, 1);
    assert_eq!(manifest.adapter.watch_contract_version, 1);
    assert_eq!(manifest.adapter.administration_contract_version, 1);
    let mut catalog = catalog::catalog().unwrap();
    crate::catalog_transfer::append(&mut catalog).unwrap();
    crate::catalog_watch::append(&mut catalog).unwrap();
    crate::catalog_admin::append(&mut catalog).unwrap();
    let mut required = BTreeSet::new();
    for skill in manifest.skills {
        assert_eq!(
            skill.version,
            match skill.name.as_str() {
                "focal-claims" => 7,
                "focal-evidence" => 6,
                "focal-validation" => 2,
                _ => 1,
            }
        );
        for operation in skill.required_admin_tools {
            assert_eq!(
                operation.version,
                manifest.adapter.administration_contract_version
            );
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            assert!(tool.name.starts_with("cluster."));
            assert_eq!(tool.input_schema["additionalProperties"], false);
            assert!(tool.input_schema["properties"].get("authority").is_none());
            if let Some(id) = tool.input_schema["properties"].get("operation_id") {
                let prefix = if operation.name.starts_with("cluster.replicas.") {
                    "^r1:"
                } else {
                    "^a1:"
                };
                assert!(id["pattern"].as_str().unwrap().starts_with(prefix));
                assert!(
                    operation.name.ends_with(".retry") || operation.name.ends_with(".reconcile")
                );
            }
            required.insert(operation.name);
        }
        for operation in skill.required_operations {
            let descriptor = focal_client::operations::find(&operation.name).unwrap();
            assert_eq!(descriptor.version, operation.version);
            assert_eq!(operation.version, 1);
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            assert!(tool.idempotent);
            if descriptor.mutation {
                assert!(
                    tool.input_schema
                        .get("required")
                        .and_then(serde_json::Value::as_array)
                        .unwrap()
                        .iter()
                        .any(|field| field == "operation_id")
                );
            }
            required.insert(operation.name);
        }
        for operation in skill.required_watch_tools {
            assert_eq!(operation.version, manifest.adapter.watch_contract_version);
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            assert!(tool.idempotent);
            assert!(!tool.destructive);
            assert_eq!(tool.read_only, operation.name == "watch.inspect");
            assert_eq!(tool.input_schema["additionalProperties"], false);
            assert!(
                tool.input_schema["properties"]
                    .get("operation_id")
                    .is_none()
            );
            required.insert(operation.name);
        }
        for operation in skill.required_transfer_tools {
            assert_eq!(
                operation.version,
                manifest.adapter.transfer_contract_version
            );
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            assert!(tool.idempotent);
            assert_eq!(tool.read_only, operation.name == "artifact.download");
            assert_eq!(tool.destructive, operation.name == "upload.cancel");
            assert_eq!(tool.input_schema["additionalProperties"], false);
            assert_eq!(
                tool.input_schema["$id"],
                format!(
                    "urn:focal:transfer:{}:input:{}",
                    operation.name, operation.version
                )
            );
            let fields = tool.input_schema["required"].as_array().unwrap();
            let identity = if tool.read_only { "id" } else { "upload_id" };
            assert!(fields.iter().any(|v| v == identity));
            assert!(
                tool.input_schema["properties"]
                    .get("operation_id")
                    .is_none()
            );
            required.insert(operation.name);
        }
        for operation in skill.required_recovery_tools {
            assert_eq!(
                operation.version,
                manifest.adapter.recovery_contract_version
            );
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            let (read_only, destructive, idempotent, fields) = match operation.name.as_str() {
                "request.inspect" => (true, false, true, vec!["operation_id", "remote"]),
                "request.retry" => (false, true, true, vec!["operation_id"]),
                "request.reserve" => (false, false, false, vec![]),
                "request.pending" => (true, false, true, vec![]),
                "request.acknowledge" | "request.seal" => (false, true, true, vec!["operation_id"]),
                other => panic!("unexpected recovery tool: {other}"),
            };
            assert_eq!(tool.read_only, read_only, "{}", tool.name);
            assert_eq!(tool.destructive, destructive, "{}", tool.name);
            assert_eq!(tool.idempotent, idempotent, "{}", tool.name);
            let required_fields = if fields.is_empty() {
                serde_json::json!([])
            } else {
                serde_json::json!(["operation_id"])
            };
            assert_eq!(tool.input_schema.get("required"), Some(&required_fields));
            assert_eq!(
                tool.input_schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false))
            );
            let properties = tool
                .input_schema
                .get("properties")
                .and_then(serde_json::Value::as_object)
                .unwrap();
            assert_eq!(
                properties
                    .keys()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>(),
                fields.into_iter().collect()
            );
            if matches!(
                operation.name.as_str(),
                "request.acknowledge" | "request.seal"
            ) {
                let id = properties.get("operation_id").unwrap();
                assert_eq!(id.get("minLength"), Some(&serde_json::json!(78)));
                assert_eq!(id.get("maxLength"), Some(&serde_json::json!(78)));
                assert_eq!(
                    id.get("pattern"),
                    Some(&serde_json::json!(
                        "^m1:[0-9a-f]{8}:[0-9a-f]{16}:[0-9a-f]{16}:[0-9a-f]{32}$"
                    ))
                );
            }
            assert_eq!(
                tool.output_schema
                    .get("$id")
                    .and_then(serde_json::Value::as_str),
                Some(format!("urn:focal:mcp:{}:output:1", tool.name).as_str())
            );
            required.insert(operation.name);
        }
    }
    assert_eq!(
        required,
        catalog.into_iter().map(|tool| tool.name).collect()
    );
    assert_eq!(
        required.len(),
        focal_client::operations::descriptors().len()
            + 6
            + crate::catalog_transfer::TOOL_COUNT
            + crate::catalog_watch::TOOL_COUNT
            + crate::catalog_admin::TOOL_COUNT
    );
}
