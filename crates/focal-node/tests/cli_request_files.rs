#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
use focal_model::{RequestEpoch, RequestId};
use focal_wire::{Operation, RequestEnvelope};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(root: &Path) -> Server {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = command(root, &["start"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(stdout).lines() {
            text.push_str(&line.unwrap());
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap()["condition"],
        "Ready"
    );
    Server(child)
}
fn command(root: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command.arg("--data-dir").arg(root).args(args);
    command
}
fn run(root: &Path, args: &[&str]) -> Output {
    command(root, args).output().unwrap()
}
fn json(root: &Path, args: &[&str]) -> Value {
    let out = run(root, args);
    assert!(
        out.status.success(),
        "{:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}

fn claim() -> Value {
    json!({"description":"Frozen raw request", "target":"self", "action":"handoff", "scopes":[{"kind":"file","key":"request.txt"}], "validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Delivery", "evaluator":"self"}]})
}
fn success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    output
}
#[test]
fn strict_shape_validation_is_offline_and_selected_preflight_never_silently_falls_back() {
    let root = tempfile::tempdir().unwrap();
    let text = claim().to_string();
    let out = success(run(
        root.path(),
        &[
            "--config",
            "/not/a/config",
            "schema",
            "validate",
            "claim.submit",
            "--shape-only",
            "--json",
            &text,
        ],
    ));
    assert!(String::from_utf8_lossy(&out.stdout).contains("document shape only"));
    assert!(
        !run(
            root.path(),
            &["schema", "validate", "claim.submit", "--json", &text]
        )
        .status
        .success()
    );
    for bad in [
        "{\"description\":\"a\",\"description\":\"b\"}",
        "{\"unknown\":1}",
        "{}",
        "[]",
    ] {
        let out = run(
            root.path(),
            &[
                "schema",
                "validate",
                "claim.submit",
                "--shape-only",
                "--json",
                bad,
            ],
        );
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
    }
    let oversized = "x".repeat(focal_client::input::MAX_INPUT_BYTES + 1);
    let input = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(input.path(), oversized).unwrap();
    assert!(
        !run(
            root.path(),
            &[
                "schema",
                "validate",
                "claim.submit",
                "--shape-only",
                "--file",
                input.path().to_str().unwrap(),
                "--input-format",
                "yaml"
            ]
        )
        .status
        .success()
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
#[test]
fn offline_build_freezes_ids_checks_private_no_clobber_and_rejects_unknown_wire_fields() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let mut settings = focal_node::config::Settings::default();
    settings.node.data_dir = Some(root.path().to_owned());
    drop(focal_node::embedded::EmbeddedNode::open(&settings).unwrap());
    let text = claim().to_string();
    let file = output.path().join("saved.json");
    let path = file.to_str().unwrap();
    assert!(
        !run(
            root.path(),
            &[
                "request",
                "build",
                "claim.submit",
                "--json",
                &text,
                "--output",
                path
            ]
        )
        .status
        .success()
    );
    assert!(!file.exists());
    success(run(
        root.path(),
        &["schema", "validate", "claim.submit", "--json", &text],
    ));
    let yaml = serde_saphyr::to_string(&claim()).unwrap();
    success(run(
        root.path(),
        &["schema", "validate", "claim.submit", "--yaml", &yaml],
    ));
    let authored = output.path().join("claim.yaml");
    std::fs::write(&authored, yaml).unwrap();
    success(run(
        root.path(),
        &[
            "request",
            "build",
            "claim.submit",
            "--file",
            authored.to_str().unwrap(),
            "--request-epoch",
            "7",
            "--output",
            path,
        ],
    ));
    let bytes = std::fs::read(&file).unwrap();
    let request: RequestEnvelope = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(request.request_epoch, RequestEpoch(7));
    assert!(!request.request_id.is_zero());
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert!(
        !run(
            root.path(),
            &[
                "request",
                "build",
                "claim.submit",
                "--json",
                &text,
                "--request-epoch",
                "7",
                "--output",
                path
            ]
        )
        .status
        .success()
    );
    assert_eq!(std::fs::read(&file).unwrap(), bytes);
    let checked = success(run(
        root.path(),
        &["--config", "/missing", "request", "check", path],
    ));
    assert!(String::from_utf8_lossy(&checked.stdout).contains("server acceptance unchecked"));
    assert!(!root.path().join("CLI.requests").exists());
    let mut invalid: Value = serde_json::from_slice(&bytes).unwrap();
    invalid["operation"]["Submit"]["not_a_command_field"] = json!(true);
    let bad = output.path().join("bad.json");
    std::fs::write(&bad, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(
        !run(root.path(), &["request", "check", bad.to_str().unwrap()])
            .status
            .success()
    );
    let mut invalid = request.clone();
    invalid.request_id = RequestId([0; 16]);
    std::fs::write(&bad, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(
        !run(root.path(), &["request", "check", bad.to_str().unwrap()])
            .status
            .success()
    );
    let mut malformed = bytes.clone();
    malformed.extend_from_slice(b"{}");
    std::fs::write(&bad, malformed).unwrap();
    assert!(
        !run(root.path(), &["request", "check", bad.to_str().unwrap()])
            .status
            .success()
    );
    let exact = output.path().join("get.json");
    success(run(
        root.path(),
        &[
            "request",
            "build",
            "claim.get",
            "--json",
            "{\"id\":\"00000000000000000000000000000001\"}",
            "--output",
            exact.to_str().unwrap(),
        ],
    ));
    let read: RequestEnvelope = serde_json::from_slice(&std::fs::read(&exact).unwrap()).unwrap();
    assert!(matches!(read.operation, Operation::Read(_)));
    assert_eq!(read.request_epoch, RequestEpoch(1));
    let composed = output.path().join("composed.json");
    assert!(
        !run(
            root.path(),
            &[
                "request",
                "build",
                "validation.context",
                "--json",
                "{\"id\":\"00000000000000000000000000000001\"}",
                "--output",
                composed.to_str().unwrap()
            ]
        )
        .status
        .success()
    );
    assert!(!composed.exists());
    let identity = std::fs::read(root.path().join("IDENTITY")).unwrap();
    assert!(
        !run(
            root.path(),
            &[
                "--client-context",
                "missing",
                "schema",
                "validate",
                "claim.submit",
                "--json",
                &text
            ]
        )
        .status
        .success()
    );
    assert_eq!(
        std::fs::read(root.path().join("IDENTITY")).unwrap(),
        identity
    );
}
#[test]
fn raw_send_and_legacy_path_retry_reuse_exact_file_across_server_restart() {
    let root = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let server = start(root.path());
    let file = output.path().join("claim.json");
    let text = claim().to_string();
    let path = file.to_str().unwrap();
    success(run(
        root.path(),
        &[
            "request",
            "build",
            "claim.submit",
            "--json",
            &text,
            "--request-epoch",
            "1",
            "--output",
            path,
        ],
    ));
    let bytes = std::fs::read(&file).unwrap();
    let request: RequestEnvelope = serde_json::from_slice(&bytes).unwrap();
    // The offline checker is intentionally narrower than the legacy sender.
    // Protocol-2 read-only managed state must still reach real authenticated ingress.
    let identity = focal_node::embedded::decode_identity(&root.path().join("IDENTITY")).unwrap();
    let managed = RequestEnvelope {
        protocol: focal_wire::MANAGED_PROTOCOL_VERSION,
        request_id: RequestId::from_u128(899),
        operation: Operation::RequestStreamRead {
            cluster: identity.cluster,
            query: focal_model::RequestStreamQuery::Slot { slot: 0 },
        },
        ..request.clone()
    };
    let managed_file = output.path().join("managed.json");
    std::fs::write(&managed_file, serde_json::to_vec(&managed).unwrap()).unwrap();
    assert!(
        !run(
            root.path(),
            &["request", "check", managed_file.to_str().unwrap()]
        )
        .status
        .success()
    );
    let current = json(
        root.path(),
        &["request", "send", managed_file.to_str().unwrap()],
    );
    assert!(
        current["result"]["RequestStreamRead"].is_object(),
        "{current}"
    );
    assert_eq!(
        json(root.path(), &["request", managed_file.to_str().unwrap()]),
        current
    );
    let before = json(root.path(), &["status"]);
    let open = RequestEnvelope {
        request_id: RequestId::from_u128(900),
        operation: Operation::OpenEpoch {
            epoch: RequestEpoch(1),
        },
        ..request.clone()
    };
    let epoch = output.path().join("epoch.json");
    std::fs::write(&epoch, serde_json::to_vec(&open).unwrap()).unwrap();
    let _opened = json(root.path(), &["request", "send", epoch.to_str().unwrap()]);
    let committed = json(root.path(), &["request", "send", path]);
    let state = json(root.path(), &["status"]);
    assert_ne!(state["result"], before["result"]);
    assert!(committed["result"]["Submitted"].is_object(), "{committed}");
    drop(server);
    let _server = start(root.path());
    let retry = json(root.path(), &["request", path]);
    assert_eq!(retry["request_id"], committed["request_id"]);
    assert_eq!(retry["request_epoch"], committed["request_epoch"]);
    assert_eq!(json(root.path(), &["status"])["result"], state["result"]);
    assert_eq!(std::fs::read(&file).unwrap(), bytes);
}
