#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
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

fn cli(root: &Path, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    json(root, &args)
}
fn document(root: &Path, verb: &[&str], input: Value, yaml: bool) -> Value {
    let text = if yaml {
        serde_saphyr::to_string(&input).unwrap()
    } else {
        input.to_string()
    };
    let mut args = verb.to_vec();
    args.extend([if yaml { "--yaml" } else { "--json" }, &text]);
    cli(root, &args)
}
fn claim(root: &Path, id: &str) {
    document(
        root,
        &["submit", "claim"],
        json!({"id":id,"description":"Document mode workflow","target":"self","action":"handoff","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Delivery","evaluator":"self"}]}),
        false,
    );
}
#[test]
fn scalar_document_modes_execute_actual_receipt_lifecycle_without_changing_authority() {
    let root = tempfile::tempdir().unwrap();
    let _server = start(root.path());
    let id = "00000000000000000000000000000001";
    claim(root.path(), id);
    document(root.path(), &["claim", "post"], json!({"claim":id}), false);
    let acquired = document(
        root.path(),
        &["receipt", "acquire"],
        json!({"claim":id,"epoch":1}),
        true,
    );
    let receipt = acquired["result"]["receipt"].as_str().unwrap();
    let fence = json!({"id":receipt,"epoch":1});
    document(
        root.path(),
        &["claim", "progress"],
        json!({"claim":id,"receipt":fence,"message":"Prepared response"}),
        false,
    );
    let evidence = document(
        root.path(),
        &["evidence", "begin"],
        json!({"claim":id,"receipt":fence}),
        true,
    );
    let evidence_id = evidence["result"]["evidence_set"].as_str().unwrap();
    let closed = document(
        root.path(),
        &["submit", "testament"],
        json!({"claim":id,"receipt":fence,"evidence_set":evidence_id,"manifest":[],"summary":"Delivered","confidence":"committed","outcome":"complete"}),
        false,
    );
    let testament = closed["result"]["testament"].as_str().unwrap();
    document(
        root.path(),
        &["testament", "receive"],
        json!({"claim":id,"testament":testament}),
        true,
    );
    document(
        root.path(),
        &["validation", "begin"],
        json!({"claim":id}),
        false,
    );
    document(
        root.path(),
        &["validation", "complete"],
        json!({"claim":id}),
        true,
    );
    let read = cli(root.path(), &["get", "claim", id]);
    let observed: focal_model::Claim =
        serde_json::from_value(read["result"]["object"]["Claim"]["value"].clone()).unwrap();
    assert_eq!(
        observed.lifecycle().status,
        focal_model::ClaimStatus::Satisfied
    );
    assert!(observed.lifecycle().local_complete);
    for until in ["satisfied", "terminal", "released"] {
        assert_eq!(
            cli(
                root.path(),
                &[
                    "claim",
                    "wait",
                    id,
                    "--until",
                    until,
                    "--timeout-ms",
                    "1000"
                ]
            )["condition"],
            "Met"
        );
    }
    let other = "00000000000000000000000000000002";
    claim(root.path(), other);
    let cancel = root.path().join("cancel.yaml");
    std::fs::write(
        &cancel,
        format!("claim: '{other}'\nreason: Finished elsewhere\n"),
    )
    .unwrap();
    cli(
        root.path(),
        &["claim", "cancel", "--file", cancel.to_str().unwrap()],
    );
    let prefix = json(root.path(), &["status"])["result"].clone();
    for args in [
        vec![
            "claim",
            "post",
            id,
            "--json",
            "{\"claim\":\"00000000000000000000000000000001\"}",
        ],
        vec![
            "validation",
            "begin",
            "--json",
            "{\"claim\":\"00000000000000000000000000000001\",\"runtime\":true}",
        ],
        vec![
            "receipt",
            "acquire",
            "--json",
            "{\"claim\":\"a\",\"claim\":\"b\",\"epoch\":1}",
        ],
    ] {
        assert!(!run(root.path(), &args).status.success());
        assert_eq!(json(root.path(), &["status"])["result"], prefix);
    }
}
