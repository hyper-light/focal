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
