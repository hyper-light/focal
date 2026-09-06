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
}
#[derive(Deserialize)]
struct Skill {
    required_operations: Vec<Required>,
    required_recovery_tools: Vec<Required>,
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
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.adapter.name, "focal-mcp");
    assert_eq!(manifest.adapter.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest.adapter.recovery_contract_version, 1);
    let catalog = catalog::catalog().unwrap();
    let mut required = BTreeSet::new();
    for skill in manifest.skills {
        for operation in skill.required_operations {
            let descriptor = focal_client::operations::find(&operation.name).unwrap();
            assert_eq!(descriptor.version, operation.version);
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
        for operation in skill.required_recovery_tools {
            assert_eq!(
                operation.version,
                manifest.adapter.recovery_contract_version
            );
            let tool = catalog
                .iter()
                .find(|tool| tool.name == operation.name)
                .unwrap();
            assert!(tool.idempotent);
            assert_eq!(tool.read_only, operation.name == "request.inspect");
            assert_eq!(tool.destructive, operation.name == "request.retry");
            assert_eq!(
                tool.input_schema.get("required"),
                Some(&serde_json::json!(["operation_id"]))
            );
            assert_eq!(
                tool.input_schema.get("additionalProperties"),
                Some(&serde_json::Value::Bool(false))
            );
            assert_eq!(
                tool.input_schema
                    .get("properties")
                    .and_then(serde_json::Value::as_object)
                    .unwrap()
                    .len(),
                if operation.name == "request.inspect" {
                    2
                } else {
                    1
                }
            );
            assert_eq!(
                tool.output_schema
                    .get("$id")
                    .and_then(serde_json::Value::as_str),
                Some("urn:focal:application-result:1")
            );
            required.insert(operation.name);
        }
    }
    assert_eq!(
        required,
        catalog.into_iter().map(|tool| tool.name).collect()
    );
    assert_eq!(required.len(), 20);
}
