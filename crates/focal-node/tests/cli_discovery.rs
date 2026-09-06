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
    let catalog = json(root.path(), &["schema", "list", "--format", "json"]);
    let yaml = run(root.path(), &["schema", "list", "--format", "yaml"]);
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
            json(root.path(), &["schema", "get", name]),
            descriptor.input_schema().unwrap()
        );
        assert_eq!(
            json(
                root.path(),
                &["schema", "get", name, "--direction", "output"]
            ),
            descriptor.output_schema().unwrap()
        );
        if entry["example_available"] == true {
            let value = json(root.path(), &["schema", "example", name]);
            focal_client::operations::parse_json(name, &serde_json::to_vec(&value).unwrap())
                .unwrap();
        }
    }
    let test_report = json(root.path(), &["schema", "get", "test-report"]);
    assert_eq!(test_report["name"], "focal.test_report.v1");
    assert_eq!(
        test_report["hash"],
        focal_evidence::test_report_schema().to_string()
    );
    let registry = json(root.path(), &["schema", "get", "domain-registry"]);
    assert!(registry["vocabularies"].is_object());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn actual_shell_completions_and_unknown_discovery_fail_without_side_effects() {
    let root = tempfile::tempdir().unwrap();
    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        let output = run(root.path(), &["completion", shell]);
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
        vec!["schema", "get", "future.lifecycle"],
        vec!["schema", "example", "future.lifecycle"],
        vec!["schema", "get", "test-report", "--direction", "output"],
        vec!["completion", "unknown-shell"],
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
        .args(["completion", "bash"])
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
    assert!(help.contains("selected physical node"));
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
