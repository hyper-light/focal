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
//! Real CLI processes preserve private operation identity and exact OS paths.
use focal_client::pending::{OperationContext, OperationJournal, OperationStage};
use focal_node::{config::Settings, embedded::EmbeddedNode};
use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    fs,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::Path,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

fn execute(root: &Path, args: &[&OsStr]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Output is bounded below 8 KiB in this fixture, so neither pipe can fill.
    // A regression must not leave an unbounded child process running in CI.
    let started = Instant::now();
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if started.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!(
                "CLI exceeded bounded retry duration: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn structured(output: &Output, code: i32) -> Value {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    assert!(output.stdout.len() < 8192);
    serde_json::from_slice(&output.stdout).unwrap()
}
fn case(name: OsString) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("node data with spaces");
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.clone());
    let node = EmbeddedNode::open(&settings).unwrap();
    let identity = node.identity.clone();
    drop(node);
    assert!(!root.join("focal.sock").exists());
    let operation = directory.path().join(name);
    let validation = OsStr::new(
        r#"{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receipt of testament","evaluator":"self"}"#,
    );
    let output = execute(
        &root,
        &[
            OsStr::new("submit"),
            OsStr::new("claim"),
            OsStr::new("--target"),
            OsStr::new("self"),
            OsStr::new("--action"),
            OsStr::new("handoff"),
            OsStr::new("--description"),
            OsStr::new("Report whose request identity must survive"),
            OsStr::new("--validation-json"),
            validation,
            OsStr::new("--operation"),
            operation.as_os_str(),
            OsStr::new("--format"),
            OsStr::new("json"),
        ],
    );
    #[cfg(target_os = "macos")]
    if operation.to_str().is_none() {
        // Native macOS rejects this filename with EILSEQ; its sandbox can
        // reject it first with EPERM. No journal can be created on this host.
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("operation journal I/O failed"), "{stderr}");
        assert!(
            stderr.contains("os error 92") || stderr.contains("os error 1"),
            "{stderr}"
        );
        assert!(!stderr.contains("panicked"));
        assert!(output.stdout.is_empty());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        assert!(!root.join("client").exists());
        assert!(!root.join("focal.sock").exists());
        return;
    }
    let pending = structured(&output, 7);
    assert_eq!(pending["schema_version"], 1);
    assert_eq!(pending["condition"], "OutcomeUnknown");
    assert_eq!(
        pending["operation"],
        serde_json::to_value(operation.to_str()).unwrap()
    );
    assert_eq!(
        pending["operation_path_bytes"],
        serde_json::to_value(operation.as_os_str().as_bytes()).unwrap()
    );
    let context = OperationContext {
        cluster: identity.cluster,
        principal: identity.issuer,
        ledger: identity.ledger,
    };
    let journal = OperationJournal::open(&operation, &context).unwrap();
    assert_eq!(journal.stage(), OperationStage::OpenEpoch);
    assert!(journal.receipt().is_none());
    assert!(journal.epoch_receipt().is_none());
    let request = journal.next_request().unwrap().unwrap().clone();
    assert_eq!(pending["request"], request.request_id.to_string());
    let saved = fs::read(operation.join("state.bin")).unwrap();
    drop(journal);

    // Inspect is local and does not need a service. Passing the exact OsString
    // works when the JSON string path is null due to non-UTF-8 bytes.
    let inspected = structured(
        &execute(
            &root,
            &[
                OsStr::new("request"),
                OsStr::new("inspect"),
                operation.as_os_str(),
                OsStr::new("--format"),
                OsStr::new("json"),
            ],
        ),
        0,
    );
    assert_eq!(inspected["stage"], "OpenEpoch");
    assert_eq!(inspected["pending_request"], pending["request"]);
    assert_eq!(inspected["operation"], pending["operation"]);
    assert_eq!(
        inspected["operation_path_bytes"],
        pending["operation_path_bytes"]
    );
    assert_eq!(fs::read(operation.join("state.bin")).unwrap(), saved);

    let retried = structured(
        &execute(
            &root,
            &[
                OsStr::new("request"),
                OsStr::new("retry"),
                operation.as_os_str(),
                OsStr::new("--format"),
                OsStr::new("json"),
            ],
        ),
        7,
    );
    assert_eq!(retried, pending);
    let recovered = OperationJournal::open(&operation, &context).unwrap();
    assert_eq!(recovered.stage(), OperationStage::OpenEpoch);
    assert_eq!(recovered.next_request().unwrap(), Some(&request));
    assert_eq!(fs::read(operation.join("state.bin")).unwrap(), saved);
    assert!(!root.join("focal.sock").exists());
}

#[test]
#[cfg(not(target_os = "macos"))]
fn non_utf8_operation_path_has_lossless_json_and_exact_pending_retry() {
    case(OsString::from_vec(b"operation with \xff byte".to_vec()));
}

#[test]
#[cfg(target_os = "macos")]
fn macos_rejects_non_utf8_operation_path_before_creating_or_sending_a_request() {
    case(OsString::from_vec(b"operation with \xff byte".to_vec()));
}

#[test]
fn data_directory_and_operation_spaces_survive_inspection_and_pending_retry() {
    case(OsString::from("operation with spaces"));
}
