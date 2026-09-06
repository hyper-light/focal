#![allow(clippy::unwrap_used, clippy::panic, clippy::disallowed_macros)]
use focal_client::operations;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u16,
    adapter: Adapter,
    resources: Vec<Resource>,
    skills: Vec<Skill>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Adapter {
    name: String,
    version: String,
    recovery_contract_version: u16,
    transfer_contract_version: u16,
    watch_contract_version: u16,
    administration_contract_version: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resource {
    path: String,
    blake3: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Skill {
    name: String,
    version: u16,
    path: String,
    blake3: String,
    references: Vec<String>,
    required_operations: Vec<Required>,
    required_recovery_tools: Vec<Required>,
    required_transfer_tools: Vec<Required>,
    required_watch_tools: Vec<Required>,
    required_admin_tools: Vec<Required>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Required {
    name: String,
    version: u16,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontmatter {
    name: String,
    description: String,
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../skills")
}
fn resource(path: &str, digest: &str) -> String {
    assert!(
        Path::new(path)
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    );
    let bytes = fs::read(root().join(path)).unwrap();
    assert!(bytes.len() < 64 * 1024);
    assert_eq!(
        blake3::hash(&bytes).to_hex().as_str(),
        digest,
        "changed skill/reference: {path}"
    );
    String::from_utf8(bytes).unwrap()
}

#[test]
fn packaged_skills_pin_real_application_versions_and_complete_relative_resources() {
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(root().join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.schema_version, 2);
    assert_eq!(manifest.adapter.name, "focal-mcp");
    assert_eq!(manifest.adapter.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(manifest.adapter.recovery_contract_version, 2);
    assert_eq!(manifest.adapter.transfer_contract_version, 1);
    assert_eq!(manifest.adapter.watch_contract_version, 1);
    assert_eq!(manifest.adapter.administration_contract_version, 1);
    assert_eq!(manifest.skills.len(), 4);
    let mut resources = BTreeSet::new();
    let mut reference_content = String::new();
    for item in &manifest.resources {
        assert!(resources.insert(item.path.clone()), "duplicate resource");
        reference_content.push_str(&resource(&item.path, &item.blake3));
    }
    let mut all_operations = BTreeSet::new();
    let mut names = BTreeSet::new();
    for skill in manifest.skills {
        assert!(names.insert(skill.name.clone()));
        assert_eq!(
            skill.version,
            match skill.name.as_str() {
                "focal-claims" => 8,
                "focal-evidence" => 7,
                "focal-validation" => 3,
                _ => 1,
            }
        );
        assert_eq!(skill.path, format!("{}/SKILL.md", skill.name));
        let content = resource(&skill.path, &skill.blake3);
        assert!(content.starts_with(&format!("---\nname: {}\ndescription: ", skill.name)));
        let (header, _) = content
            .strip_prefix("---\n")
            .unwrap()
            .split_once("\n---\n")
            .unwrap();
        let frontmatter: Frontmatter = serde_saphyr::from_str(header).unwrap();
        assert_eq!(frontmatter.name, skill.name);
        assert!(!frontmatter.description.is_empty());
        assert!(content.contains("../manifest.json"));
        for reference in skill.references {
            assert!(resources.contains(&reference));
            assert!(content.contains(&format!("../{reference}")));
        }
        let mut required = BTreeSet::new();
        for operation in skill.required_operations {
            assert!(required.insert(operation.name.clone()));
            let Some(descriptor) = operations::find(&operation.name) else {
                panic!(
                    "skill requires an unimplemented operation: {}",
                    operation.name
                );
            };
            assert_eq!(descriptor.version, operation.version, "{}", operation.name);
            assert_eq!(operation.version, 1);
            assert!(
                content.contains(&format!("`{}`", operation.name)),
                "unused skill requirement: {}",
                operation.name
            );
            let schema = descriptor.input_schema().unwrap();
            assert_eq!(
                schema.get("$id").and_then(serde_json::Value::as_str),
                Some(
                    format!(
                        "urn:focal:operation:{}:input:{}",
                        descriptor.name, descriptor.version
                    )
                    .as_str()
                )
            );
            all_operations.insert(operation.name);
        }
        for watch in skill.required_watch_tools {
            assert_eq!(watch.version, manifest.adapter.watch_contract_version);
            assert!(content.contains(&format!("`{}`", watch.name)));
            assert!(reference_content.contains(&format!("`{}`", watch.name)));
        }
        let mut administration = BTreeSet::new();
        for tool in skill.required_admin_tools {
            assert_eq!(
                tool.version,
                manifest.adapter.administration_contract_version
            );
            assert!(tool.name.starts_with("cluster."));
            assert!(administration.insert(tool.name.clone()));
            assert!(content.contains(&format!("`{}`", tool.name)));
        }
        assert_eq!(administration.is_empty(), skill.name != "focal-cluster");
        for transfer in skill.required_transfer_tools {
            assert_eq!(transfer.version, manifest.adapter.transfer_contract_version);
            assert!(content.contains(&format!("`{}`", transfer.name)));
            assert!(reference_content.contains(&format!("`{}`", transfer.name)));
        }
        let mut recovery = BTreeSet::new();
        for operation in skill.required_recovery_tools {
            assert!(recovery.insert(operation.name.clone()));
            assert_eq!(
                operation.version,
                manifest.adapter.recovery_contract_version
            );
            assert!(reference_content.contains(&format!("`{}`", operation.name)));
        }
        let expected_recovery = if skill.name == "focal-cluster" {
            BTreeSet::new()
        } else {
            BTreeSet::from([
                "request.inspect".to_owned(),
                "request.retry".to_owned(),
                "request.reserve".to_owned(),
                "request.pending".to_owned(),
                "request.acknowledge".to_owned(),
                "request.seal".to_owned(),
            ])
        };
        assert_eq!(recovery, expected_recovery);
    }
    assert_eq!(
        names,
        BTreeSet::from([
            "focal-claims".to_owned(),
            "focal-evidence".to_owned(),
            "focal-validation".to_owned(),
            "focal-cluster".to_owned()
        ])
    );
    assert_eq!(
        all_operations,
        operations::descriptors()
            .iter()
            .map(|descriptor| descriptor.name.to_owned())
            .collect()
    );
}
