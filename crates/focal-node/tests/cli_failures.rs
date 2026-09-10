#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::{os::fd::OwnedFd, os::unix::net::UnixStream, process::Command};

#[test]
fn closed_stdout_returns_an_io_error_without_panicking() {
    // A disconnected socket makes the first write fail deterministically,
    // independent of whether parent or child is scheduled first after spawn.
    let (output, reader) = UnixStream::pair().unwrap();
    drop(reader);
    let output: OwnedFd = output.into();
    let result = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["deployment", "schema"])
        .stdout(output)
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(1));
    let diagnostic = String::from_utf8_lossy(&result.stderr);
    assert!(diagnostic.contains("focal:"), "{diagnostic}");
    assert!(!diagnostic.contains("panicked"), "{diagnostic}");
}

#[test]
fn authored_errors_follow_selected_machine_format_on_stderr_without_network_writes() {
    use focal_node::{config::Settings, embedded::EmbeddedNode};
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    drop(EmbeddedNode::open(&settings).unwrap());
    for format in ["json", "yaml"] {
        for (arguments, code) in [
            (vec!["get", "artifact", "bad-id"], "invalid_input"),
            (
                vec![
                    "submit",
                    "claim",
                    "--json",
                    r#"{"description":"one","description":"two"}"#,
                ],
                "invalid_input",
            ),
            (
                vec!["list", "artifacts", "--source", "self"],
                "invalid_input",
            ),
        ] {
            let output = Command::new(env!("CARGO_BIN_EXE_focal"))
                .arg("--data-dir")
                .arg(root.path())
                .args(&arguments)
                .args(["--format", format])
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.stdout.is_empty());
            let value: serde_json::Value = if format == "json" {
                serde_json::from_slice(&output.stderr).unwrap_or_else(|error| {
                    panic!(
                        "{arguments:?}: {error}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                })
            } else {
                serde_saphyr::from_str(std::str::from_utf8(&output.stderr).unwrap()).unwrap()
            };
            assert_eq!(value["schema_version"], 1);
            assert_eq!(value["error"]["code"], code);
            assert_eq!(value["error"]["exit_code"], 2);
            assert!(
                !value["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("panicked")
            );
        }
    }
    assert!(!root.path().join("CLI.requests").exists());
}

#[test]
fn deployment_unmet_guarantee_has_distinct_exit_and_no_activation() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let config = root.path().join("regional.yaml");
    std::fs::write(
        &config,
        "version: 1\ndurability:\n  survive: region\n  max_failures: 1\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--config")
        .arg(config)
        .args(["deployment", "explain"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(9));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["condition"], "GuaranteeUnsatisfied");
    assert_eq!(value["activated"], false);
    assert!(String::from_utf8_lossy(&output.stderr).contains("guarantee_unsatisfied"));
}

/// Configuration ownership (doc 08 §2): an unknown key fails by its full
/// path, a committed policy is not changed by a later start and the
/// refusal names the field, and `deployment explain` names every value's
/// source with the committed revision.
#[test]
fn committed_policy_and_unknown_keys_are_refused_by_name() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let data = root.path().join("node");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::set_permissions(&data, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
    let unknown = root.path().join("unknown.yaml");
    std::fs::write(&unknown, "version: 1\nnode:\n  shards: 3\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", unknown.to_str().unwrap(), "--data-dir"])
        .arg(&data)
        .args(["identity"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("node.shards"), "{stderr}");
    // The store pins the default policy on its first open.
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(&data)
        .args(["demo"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let explained = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(&data)
        .args(["deployment", "explain"])
        .output()
        .unwrap();
    assert!(explained.status.success(), "{explained:?}");
    let value: serde_json::Value = serde_json::from_slice(&explained.stdout).unwrap();
    assert_eq!(value["condition"], "PlanValid");
    assert_eq!(value["committed_revision"], 1, "{value}");
    assert_eq!(value["effective"]["durability"]["max_failures"], 0);
    assert_eq!(
        value["sources"]["durability.max_failures"],
        serde_json::json!({"committed": 1}),
        "{value}"
    );
    assert_eq!(value["sources"]["node.data_dir"], "command_line", "{value}");
    // A later start with a different committed field is refused by name and
    // leaves the pinned policy in place.
    let changed = root.path().join("changed.yaml");
    std::fs::write(&changed, "version: 1\ndurability:\n  max_failures: 1\n").unwrap();
    let pinned = std::fs::read(data.join("POLICY")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", changed.to_str().unwrap(), "--data-dir"])
        .arg(&data)
        .args(["demo"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("durability.max_failures"), "{stderr}");
    assert!(stderr.contains("deployment plan"), "{stderr}");
    assert_eq!(std::fs::read(data.join("POLICY")).unwrap(), pinned);
    // Restating the committed value is not a change.
    let same = root.path().join("same.yaml");
    std::fs::write(&same, "version: 1\ndurability:\n  max_failures: 0\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--config", same.to_str().unwrap(), "--data-dir"])
        .arg(&data)
        .args(["identity"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}
