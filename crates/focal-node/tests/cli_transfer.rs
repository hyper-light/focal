#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
#[path = "support/cli_upload_control.rs"]
mod upload_control;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, ChildStdin, Command, Output, Stdio},
    sync::mpsc::{self, Receiver},
    time::Duration,
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(root: &Path) -> Process {
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root)
            .arg("start")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let mut text = String::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            text.push_str(&line);
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
    process
}
fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let result = run(root, &args);
    assert!(
        result.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    serde_json::from_slice(&result.stdout).unwrap()
}
fn id(value: u128) -> String {
    format!("{value:032x}")
}
fn setup(root: &Path) -> (String, String, String) {
    let claim = id(100);
    let document = json!({"id":claim,"target":"self","action":"handoff","description":"Durable large evidence","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive result","evaluator":"self"}]});
    cli(root, &["submit", "claim", "--json", &document.to_string()]);
    cli(root, &["claim", "post", &claim]);
    let receipt = cli(root, &["receipt", "acquire", &claim])["result"]["receipt"]
        .as_str()
        .unwrap()
        .to_owned();
    let set = cli(
        root,
        &[
            "evidence",
            "begin",
            "--claim",
            &claim,
            "--receipt",
            &receipt,
            "--receipt-epoch",
            "1",
        ],
    )["result"]["evidence_set"]
        .as_str()
        .unwrap()
        .to_owned();
    (claim, receipt, set)
}
fn payload() -> Vec<u8> {
    let mut bytes = vec![b' '; 300_000];
    bytes.extend_from_slice(br#"{"passed":7,"failed":0,"skipped":0}"#);
    bytes
}

#[test]
fn large_cli_file_stays_owned_after_failure_and_retry_uploads_attaches_downloads_exactly() {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    let server = start(root.path());
    let (claim, receipt, set) = setup(root.path());
    let operation = cli(root.path(), &["request", "reserve"])["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let bytes = payload();
    let source = root.path().join("large report.json");
    std::fs::write(&source, &bytes).unwrap();
    let artifact = id(201);
    let schema = focal_evidence::test_report_schema().to_string();
    drop(server);
    let output = run(
        root.path(),
        &[
            "submit",
            "artifact",
            "--claim",
            &claim,
            "--receipt",
            &receipt,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            &set,
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--id",
            &artifact,
            "--payload-file",
            source.to_str().unwrap(),
            "--operation-id",
            &operation,
            "--format",
            "json",
        ],
    );
    assert_eq!(
        output.status.code(),
        Some(7),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let pending: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(pending["operation_id"], operation);
    assert_eq!(pending["condition"], "UploadPending");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    std::fs::remove_file(&source).unwrap();
    let _server = start(root.path());
    let committed = cli(
        root.path(),
        &["request", "retry", "--operation-id", &operation],
    );
    assert_eq!(committed["condition"], "Committed");
    assert_eq!(committed["operation_id"], operation);
    assert_eq!(committed["result"]["artifact"], artifact);
    let output_path = root.path().join("verified result.json");
    let output = run(
        root.path(),
        &[
            "get",
            "artifact",
            &artifact,
            "--output",
            output_path.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), bytes);
    let existing = run(
        root.path(),
        &[
            "get",
            "artifact",
            &artifact,
            "--output",
            output_path.to_str().unwrap(),
        ],
    );
    assert!(!existing.status.success());
    assert_eq!(std::fs::read(&output_path).unwrap(), bytes);
    // Successful ordinary file submission allocates its own managed identity
    // and remains quiet; no transfer ceremony is required from the caller.
    let mut second_bytes = bytes.clone();
    second_bytes.push(b'\n');
    std::fs::write(&source, &second_bytes).unwrap();
    let artifact2 = id(202);
    let output = run(
        root.path(),
        &[
            "submit",
            "artifact",
            "--claim",
            &claim,
            "--receipt",
            &receipt,
            "--receipt-epoch",
            "1",
            "--evidence-set",
            &set,
            "--kind",
            "test-report",
            "--schema-hash",
            &schema,
            "--id",
            &artifact2,
            "--payload-file",
            source.to_str().unwrap(),
            "--format",
            "json",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let committed: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(committed["result"]["artifact"], artifact2);
}

#[test]
fn independent_large_registration_retries_owned_bytes_in_managed_and_legacy_contexts() {
    for legacy in [false, true] {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let server = start(root.path());
        let operation = if legacy {
            root.path()
                .join("proof operation")
                .to_str()
                .unwrap()
                .to_owned()
        } else {
            cli(root.path(), &["request", "reserve"])["operation_id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let bytes = payload();
        let source = root.path().join("independent report.json");
        std::fs::write(&source, &bytes).unwrap();
        let artifact = id(501);
        let schema = focal_evidence::test_report_schema().to_string();
        drop(server);
        let flag = if legacy {
            "--operation"
        } else {
            "--operation-id"
        };
        let result = run(
            root.path(),
            &[
                "artifact",
                "register",
                "--kind",
                "test-report",
                "--schema-hash",
                &schema,
                "--id",
                &artifact,
                "--payload-file",
                source.to_str().unwrap(),
                flag,
                &operation,
                "--format",
                "json",
            ],
        );
        assert_eq!(
            result.status.code(),
            Some(7),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!String::from_utf8_lossy(&result.stderr).contains("panicked"));
        if !legacy {
            let pending: Value = serde_json::from_slice(&result.stdout).unwrap();
            assert_eq!(pending["condition"], "UploadPending");
            assert_eq!(pending["operation_id"], operation);
        }
        std::fs::remove_file(&source).unwrap();
        let _server = start(root.path());
        let committed = if legacy {
            cli(root.path(), &["request", "retry", &operation])
        } else {
            cli(
                root.path(),
                &["request", "retry", "--operation-id", &operation],
            )
        };
        if legacy {
            assert_eq!(committed["stage"], "Completed", "{committed}");
            assert!(!committed["receipt"].is_null());
        } else {
            assert_eq!(committed["condition"], "Committed", "{committed}");
        }
        assert_eq!(committed["result"]["artifact"], artifact);
        let output = root.path().join("registered proof.json");
        let result = run(
            root.path(),
            &[
                "get",
                "artifact",
                &artifact,
                "--output",
                output.to_str().unwrap(),
            ],
        );
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(std::fs::read(output).unwrap(), bytes);
        // No claim, execution receipt or evidence set was invented for proof.
        let listed = cli(root.path(), &["list", "claims"]);
        assert!(listed["results"].as_array().unwrap().is_empty(), "{listed}");
    }
}

struct Mcp {
    _process: Process,
    input: ChildStdin,
    output: Receiver<Value>,
    next: u64,
    modern: bool,
}
impl Mcp {
    fn start(root: &Path, modern: bool) -> Self {
        let mut process = Process(
            Command::new(env!("CARGO_BIN_EXE_focal"))
                .arg("--data-dir")
                .arg(root)
                .args(["mcp", "serve"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let input = process.0.stdin.take().unwrap();
        let stdout = process.0.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if send.send(serde_json::from_str(&line).unwrap()).is_err() {
                    break;
                }
            }
        });
        let mut result = Self {
            _process: process,
            input,
            output,
            next: 0,
            modern,
        };
        if !modern {
            result.request("initialize",json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"transfer-test","version":"1"}}));
            result.send(json!({"jsonrpc":"2.0","method":"notifications/initialized","params":{}}));
        }
        result
    }
    fn send(&mut self, value: Value) {
        serde_json::to_writer(&mut self.input, &value).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
    }
    fn request(&mut self, method: &str, mut params: Value) -> Value {
        self.next += 1;
        if self.modern {
            params["_meta"] = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}});
        }
        self.send(json!({"jsonrpc":"2.0","id":self.next,"method":method,"params":params}));
        let result = self.output.recv_timeout(Duration::from_secs(20)).unwrap();
        assert_eq!(result["id"], self.next);
        assert!(result.get("error").is_none(), "{result}");
        result
    }
    fn call(&mut self, name: &str, args: Value) -> Value {
        self.request("tools/call", json!({"name":name,"arguments":args}))
    }
    fn success(&mut self, name: &str, args: Value) -> Value {
        let result = self.call(name, args);
        assert_eq!(result["result"]["isError"], false, "{result}");
        result["result"]["structuredContent"].clone()
    }
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut text, "{byte:02x}").unwrap();
    }
    text
}
#[test]
fn both_mcp_profiles_upload_restart_attach_and_retrieve_all_payload_pages() {
    for modern in [true, false] {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let _server = start(root.path());
        let (claim, receipt, set) = setup(root.path());
        let bytes = payload();
        let upload = id(301);
        let mut mcp = Mcp::start(root.path(), modern);
        let begin = json!({"upload_id":upload,"length":bytes.len(),"digest":blake3::hash(&bytes).to_hex().to_string(),"class":"evidence"});
        let reply = mcp.success("upload.begin", begin.clone());
        assert_eq!(reply["result"]["progress"]["received"], 0);
        let first = bytes.chunks(65_536).next().unwrap();
        mcp.success(
            "upload.append",
            json!({"upload_id":upload,"offset":0,"bytes_hex":hex(first)}),
        );
        drop(mcp);
        let mut mcp = Mcp::start(root.path(), modern);
        assert_eq!(
            mcp.success("upload.begin", begin.clone())["result"]["progress"]["received"],
            first.len()
        );
        let changed = mcp.call(
            "upload.append",
            json!({"upload_id":upload,"offset":0,"bytes_hex":"00"}),
        );
        assert_eq!(
            changed["result"]["structuredContent"]["result"]["code"],
            "operation_conflict"
        );
        for (offset, chunk) in bytes.chunks(65_536).enumerate().skip(1) {
            let reply = mcp.success(
                "upload.append",
                json!({"upload_id":upload,"offset":offset*65_536,"bytes_hex":hex(chunk)}),
            );
            assert!(reply["result"]["progress"]["reference"].is_null());
        }
        let sealed = mcp.success("upload.seal", json!({"upload_id":upload}));
        assert_eq!(sealed["condition"], "Sealed");
        let reference: focal_model::ContentRef =
            serde_json::from_value(sealed["result"]["progress"]["reference"].clone()).unwrap();
        assert_ne!(
            reference.root,
            focal_model::ContentHash(*blake3::hash(&bytes).as_bytes())
        );
        let artifact = id(302);
        let submission=mcp.success("artifact.submit",json!({"operation_id":id(303),"id":artifact,"claim":claim,"receipt":{"id":receipt,"epoch":1},"evidence_set":set,"kind":"test-report","schema_hash":focal_evidence::test_report_schema().to_string(),"payload":{"type":"content","reference":{"domain":reference.domain.to_string(),"root":reference.root.to_string(),"length":reference.length,"class":"evidence"}}}));
        assert_eq!(submission["condition"], "Committed");
        let mut result = Vec::new();
        let mut token = Value::Null;
        let mut hash = None;
        loop {
            let page = mcp.success(
                "artifact.download",
                json!({"id":artifact,"token":token,"offset":result.len(),"max_bytes":60_000}),
            );
            let output = &page["result"];
            if let Some(expected) = &hash {
                assert_eq!(&output["content_hash"], expected);
            } else {
                hash = Some(output["content_hash"].clone());
            }
            token = output["token"].clone();
            let chunk: focal_wire::ContentChunk =
                serde_json::from_value(output["chunk"].clone()).unwrap();
            assert_eq!(chunk.offset, result.len() as u64);
            result.extend(chunk.bytes);
            if chunk.eof {
                break;
            }
        }
        assert_eq!(result, bytes);
        // Independent evaluator proof uses the same durable content path, with
        // no respondent receipt. CLI retrieval must work for this family too.
        let proof = id(304);
        let registered = mcp.success("artifact.register", json!({
            "operation_id": id(305), "id": proof, "kind":"test-report",
            "schema_hash":focal_evidence::test_report_schema().to_string(),
            "payload":{"type":"content","reference":{"domain":reference.domain.to_string(),"root":reference.root.to_string(),"length":reference.length,"class":"evidence"}}
        }));
        assert_eq!(registered["condition"], "Committed");
        let proof_path = root.path().join("independent-proof.json");
        let downloaded = run(
            root.path(),
            &[
                "get",
                "artifact",
                &proof,
                "--output",
                proof_path.to_str().unwrap(),
            ],
        );
        assert!(
            downloaded.status.success(),
            "{}",
            String::from_utf8_lossy(&downloaded.stderr)
        );
        assert_eq!(std::fs::read(&proof_path).unwrap(), bytes);
        let cancelled = mcp.success("upload.cancel", json!({"upload_id":upload}));
        assert_eq!(
            cancelled["result"]["progress"]["reference"],
            sealed["result"]["progress"]["reference"]
        );
    }
}
