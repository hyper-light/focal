#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Real executable discovery must work without a node, config, socket or journal.
use serde_json::Value;
use std::{
    path::Path,
    process::{Command, Output, Stdio},
};

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--config")
        .arg(root.join("absent-config.yaml"))
        .arg("--data-dir")
        .arg(root.join("absent-node"))
        .args(args)
        .output()
        .unwrap()
}
fn json(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn offline_catalog_schemas_examples_and_legacy_contracts_need_no_state() {
    let root = tempfile::tempdir().unwrap();
    let catalog = json(root.path(), &["list", "schemas", "--format", "json"]);
    let yaml = run(root.path(), &["list", "schemas", "--format", "yaml"]);
    assert!(
        yaml.status.success(),
        "{}",
        String::from_utf8_lossy(&yaml.stderr)
    );
    assert_eq!(
        serde_saphyr::from_slice::<Value>(&yaml.stdout).unwrap(),
        catalog
    );
    let entries = catalog["operations"].as_array().unwrap();
    assert_eq!(entries.len(), focal_client::operations::descriptors().len());
    assert!(
        entries
            .iter()
            .any(|entry| entry["name"] == "validation.context")
    );
    for entry in entries {
        let name = entry["name"].as_str().unwrap();
        let descriptor = focal_client::operations::find(name).unwrap();
        assert_eq!(
            json(root.path(), &["get", "schema", name]),
            descriptor.input_schema().unwrap()
        );
        assert_eq!(
            json(
                root.path(),
                &["get", "schema", name, "--direction", "output"]
            ),
            descriptor.output_schema().unwrap()
        );
        if entry["example_available"] == true {
            let value = json(root.path(), &["get", "example", name]);
            focal_client::operations::parse_json(name, &serde_json::to_vec(&value).unwrap())
                .unwrap();
        }
    }
    let test_report = json(root.path(), &["get", "schema", "test-report"]);
    assert_eq!(test_report["name"], "focal.test_report.v1");
    assert_eq!(
        test_report["hash"],
        focal_evidence::test_report_schema().to_string()
    );
    let registry = json(root.path(), &["get", "schema", "domain-registry"]);
    assert!(registry["vocabularies"].is_object());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn actual_shell_completions_and_unknown_discovery_fail_without_side_effects() {
    let root = tempfile::tempdir().unwrap();
    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        let output = run(root.path(), &["generate", "completion", shell]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        let script = String::from_utf8(output.stdout).unwrap();
        assert!(script.contains("operation-id"));
        if matches!(shell, "bash" | "zsh") {
            assert!(script.contains("validation.context"));
        }
        assert!(script.contains("schema"));
    }
    for args in [
        vec!["get", "schema", "future.lifecycle"],
        vec!["get", "example", "future.lifecycle"],
        vec!["get", "schema", "test-report", "--direction", "output"],
        vec!["generate", "completion", "unknown-shell"],
    ] {
        let output = run(root.path(), &args);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn closed_discovery_output_is_an_io_error_not_a_panic() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["generate", "completion", "bash"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let result = child.wait_with_output().unwrap();
    assert!(!result.status.success());
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(!error.contains("panicked"), "{error}");
    assert!(error.contains("pipe"), "{error}");
}

#[test]
fn actual_help_describes_local_configuration_and_only_supported_family_filters() {
    let root = tempfile::tempdir().unwrap();
    let help = run(root.path(), &["--help"]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("YAML configuration file"));
    assert!(help.contains("durable ledger and local client state"));
    assert!(help.contains("start node"));
    assert!(help.contains("GLOBAL OPTIONS"));
    assert!(!help.contains("running founder"));
    for (family, present, absent) in [
        ("claims", "--source", "--producer"),
        ("testaments", "--confidence", "--source"),
        ("artifacts", "--producer", "--confidence"),
        ("validations", "--evaluator", "--status"),
    ] {
        let output = run(root.path(), &["list", family, "--help"]);
        assert!(output.status.success());
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(help.contains(present), "{family}: {help}");
        assert!(!help.contains(absent), "{family}: {help}");
        assert!(help.contains("--created-through"));
        assert!(help.contains("--claim"));
    }
    let output = run(root.path(), &["get", "claim", "--help"]);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--source"));
    assert!(help.contains("--scope"));
    assert!(!help.contains("--producer"));
    assert!(!help.contains("--confidence"));
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

/// The native engine's discovery offline (the audit's F09): its catalogue
/// lists every native descriptor with an example, every example decodes
/// through the native contract that `submit`/the native tools decode with,
/// shape-only validation accepts each one, a shared name's V1 example is
/// refused by the native engine by name, and nothing touches the disk.
#[test]
fn the_native_catalogue_examples_and_validation_need_no_state_either() {
    use focal_client::operations::{WireProfile, native_descriptors};
    let root = tempfile::tempdir().unwrap();
    let catalog = json(
        root.path(),
        &["list", "schemas", "--native", "--format", "json"],
    );
    assert_eq!(catalog["engine"], "native");
    let entries = catalog["operations"].as_array().unwrap();
    assert_eq!(entries.len(), native_descriptors().len());
    assert_eq!(entries.len(), 44);
    for entry in entries {
        assert_eq!(entry["engine"], "native", "{entry}");
        assert_eq!(entry["version"], 2, "{entry}");
        assert_eq!(entry["example_available"], true, "{entry}");
    }
    for descriptor in native_descriptors() {
        assert_eq!(descriptor.wire, WireProfile::Native);
        let name = descriptor.name;
        let example = json(root.path(), &["get", "example", name, "--native"]);
        assert_eq!(
            example,
            focal_client::operations::example(WireProfile::Native, name).unwrap(),
            "{name}"
        );
        // The example decodes through the engine's own decoder and the
        // command's shape-only validation, which is that decoder.
        focal_client::operations::decode_application(
            WireProfile::Native,
            name,
            example.to_string().as_bytes(),
        )
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        let file = root.path().join("example.json");
        std::fs::write(&file, example.to_string()).unwrap();
        let validated = run(
            root.path(),
            &[
                "validate",
                "document",
                name,
                "--native",
                "--shape-only",
                "--file",
                file.to_str().unwrap(),
            ],
        );
        assert!(
            validated.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&validated.stderr)
        );
        std::fs::remove_file(&file).unwrap();
    }
    // A claim's native example names a subject other than the issuer: the
    // owner never posts a claim on oneself.
    let claim = json(root.path(), &["get", "example", "claim.submit", "--native"]);
    assert_ne!(claim["target"], "self");
    // The V1 example of the shared name is refused by the native engine, by
    // the field the native contract lacks, and a V1-only name under --native
    // is refused by name: exit 2, no redirect, no file.
    let v1 = json(root.path(), &["get", "example", "claim.submit"]);
    assert_eq!(v1["target"], "self");
    let file = root.path().join("v1.json");
    std::fs::write(&file, v1.to_string()).unwrap();
    let refused = run(
        root.path(),
        &[
            "validate",
            "document",
            "claim.submit",
            "--native",
            "--shape-only",
            "--file",
            file.to_str().unwrap(),
        ],
    );
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("evidence_schemas"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    std::fs::remove_file(&file).unwrap();
    let batch = run(
        root.path(),
        &["get", "example", "claim.submit_batch", "--native"],
    );
    assert_eq!(batch.status.code(), Some(2), "{batch:?}");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
