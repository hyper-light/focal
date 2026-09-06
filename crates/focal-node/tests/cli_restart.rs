#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Real executable, real Unix credentials, real fsync and process termination.
#![cfg(unix)]
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap(), "start"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            text.push_str(&line);
            text.push('\n');
            if let Ok(value) = serde_json::from_str::<Value>(&text) {
                let _ = send.send(value);
                break;
            }
        }
    });
    let guard = Server(child);
    let ready = receive
        .recv_timeout(Duration::from_secs(15))
        .expect("server did not publish readiness");
    assert_eq!(ready["condition"], "Ready");
    guard
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn request(root: &Path, identity: &Value, operation: Value, id: u8) -> Value {
    let path = root.join(format!("request-{id}.json"));
    let mut key = vec![0; 16];
    key[15] = id;
    let value = json!({"protocol":1,"ledger":identity["ledger"],"route_epoch":1,"request_epoch":1,"request_id":key,"operation":operation});
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    cli(root, &["request", path.to_str().unwrap()])
}
#[test]
fn acknowledged_request_survives_kill_and_same_key_retry() {
    let root = tempfile::Builder::new()
        .prefix("focal-cli-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = start(root.path());
    let identity = cli(root.path(), &["identity"]);
    let request = json!({"protocol":1,"ledger":identity["ledger"],"route_epoch":1,"request_epoch":1,"request_id":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16],"operation":{"Submit":{"expected_revision":null,"command":{"NegotiateEpoch":{"epoch":1}}}}});
    let path = root.path().join("request.json");
    std::fs::write(&path, serde_json::to_vec(&request).unwrap()).unwrap();
    let first = cli(root.path(), &["request", path.to_str().unwrap()]);
    assert_eq!(first["result"]["Submitted"]["Committed"]["sequence"], 1);
    let refused_owner = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.path().to_str().unwrap(), "start"])
        .output()
        .unwrap();
    assert!(!refused_owner.status.success());
    drop(server); // kill, no graceful checkpoint or shutdown callback
    let _server = start(root.path());
    assert_eq!(identity, cli(root.path(), &["identity"]));
    let retried = cli(root.path(), &["request", path.to_str().unwrap()]);
    assert_eq!(first, retried);
    let status = cli(root.path(), &["status"]);
    assert_eq!(status["result"]["Read"]["token"]["sequence"], 1);
}

#[test]
fn upload_offsets_and_sealed_evidence_survive_process_kills() {
    let root = tempfile::Builder::new()
        .prefix("focal-upload-")
        .tempdir_in("/tmp")
        .unwrap();
    let server = start(root.path());
    let identity = cli(root.path(), &["identity"]);
    let upload = vec![7; 16];
    let bytes = b"abcdefghij";
    let begin = json!({"Upload":{"Begin":{"upload":upload,"length":bytes.len(),"digest":blake3::hash(bytes).as_bytes(),"class":2}}});
    assert_eq!(
        request(root.path(), &identity, begin.clone(), 1)["result"]["Upload"]["Offset"],
        0
    );
    assert_eq!(
        request(
            root.path(),
            &identity,
            json!({"Upload":{"Append":{"upload":upload,"offset":0,"bytes":bytes[..4]}}}),
            2
        )["result"]["Upload"]["Offset"],
        4
    );
    drop(server);
    let server = start(root.path());
    assert_eq!(
        request(root.path(), &identity, begin, 1)["result"]["Upload"]["Offset"],
        4
    );
    request(
        root.path(),
        &identity,
        json!({"Upload":{"Append":{"upload":upload,"offset":4,"bytes":bytes[4..]}}}),
        3,
    );
    let sealed = request(
        root.path(),
        &identity,
        json!({"Upload":{"Seal":{"upload":upload}}}),
        4,
    );
    let content = sealed["result"]["Upload"]["Sealed"].clone();
    assert!(content.is_object());
    drop(server);
    let _server = start(root.path());
    assert_eq!(
        request(
            root.path(),
            &identity,
            json!({"Upload":{"Seal":{"upload":upload}}}),
            4
        ),
        sealed
    );
    let range = request(
        root.path(),
        &identity,
        json!({"Download":{"content":content,"offset":3,"max_bytes":4}}),
        5,
    );
    assert_eq!(range["result"]["Content"]["bytes"], json!(b"defg"));
    assert_eq!(range["result"]["Content"]["eof"], false);
}

#[test]
fn stream_ack_and_unacknowledged_delivery_survive_process_kills() {
    let root = tempfile::Builder::new()
        .prefix("focal-stream-")
        .tempdir_in("/tmp")
        .unwrap();
    cli(root.path(), &["demo"]);
    let server = start(root.path());
    let identity = cli(root.path(), &["identity"]);
    let operation = json!({"Stream":{"Open":{"consumer":vec![8;16],"filter":"All","start":null,"seed":false,"credits":{"items":1,"bytes":65536}}}});
    let first = request(root.path(), &identity, operation.clone(), 201);
    let cursor = first["result"]["Stream"]["cursor"].clone();
    assert!(cursor.is_object());
    assert_eq!(
        first["result"]["Stream"]["acknowledged"]["position"]["sequence"],
        0
    );
    drop(server);
    let server = start(root.path());
    assert_eq!(request(root.path(), &identity, operation, 201), first);
    let poll = json!({"Stream":{"Poll":{"cursor":cursor,"filter":"All","acknowledged":cursor,"credits":{"items":1,"bytes":65536}}}});
    let acked = request(root.path(), &identity, poll.clone(), 202);
    assert_eq!(acked["result"]["Stream"]["acknowledged"], cursor);
    drop(server);
    let _server = start(root.path());
    assert_eq!(request(root.path(), &identity, poll, 202), acked);
}
