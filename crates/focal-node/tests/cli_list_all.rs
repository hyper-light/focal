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
fn populate(root: &Path) {
    let schema = focal_evidence::test_report_schema().to_string();
    for n in 1..=3 {
        let claim = format!("{:032x}", n * 100);
        let document = json!({"id":claim,"occurrence":format!("{:032x}",n*100+1),"description":"All-page workflow","target":"self","action":"handoff","validations":[{"id":format!("{:032x}",n*100+2),"kind":"receipt","phase":"whole_work","mode":"required","description":"Delivery","evaluator":"self"}]});
        cli(root, &["submit", "claim", "--json", &document.to_string()]);
        cli(root, &["claim", "post", &claim]);
        let received = cli(root, &["receipt", "acquire", &claim]);
        let receipt = received["result"]["receipt"].as_str().unwrap();
        let opened = cli(
            root,
            &[
                "evidence",
                "begin",
                "--claim",
                &claim,
                "--receipt",
                receipt,
                "--receipt-epoch",
                "1",
            ],
        );
        let evidence = opened["result"]["evidence_set"].as_str().unwrap();
        let report = json!({"passed":n,"failed":0,"skipped":0}).to_string();
        let artifact = cli(
            root,
            &[
                "submit",
                "artifact",
                "--claim",
                &claim,
                "--receipt",
                receipt,
                "--receipt-epoch",
                "1",
                "--evidence-set",
                evidence,
                "--kind",
                "test-report",
                "--schema-hash",
                &schema,
                "--text",
                &report,
            ],
        );
        let reference = format!(
            "{}:{}",
            artifact["result"]["artifact"].as_str().unwrap(),
            artifact["result"]["hash"].as_str().unwrap()
        );
        cli(
            root,
            &[
                "submit",
                "testament",
                "--claim",
                &claim,
                "--receipt",
                receipt,
                "--receipt-epoch",
                "1",
                "--evidence-set",
                evidence,
                "--artifact",
                &reference,
                "--summary",
                "Delivered",
                "--confidence",
                "committed",
                "--outcome",
                "complete",
            ],
        );
    }
}
fn pages(output: Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
#[test]
fn all_four_families_stream_fixed_prefix_and_empty_filtered_pages_without_duplicates() {
    let root = tempfile::tempdir().unwrap();
    let _server = start(root.path());
    populate(root.path());
    for family in ["claims", "testaments", "artifacts", "validations"] {
        let all = pages(run(
            root.path(),
            &[
                "list",
                family,
                "--all",
                "--limit",
                "1",
                "--max-visits",
                "1",
                "--format",
                "json",
            ],
        ));
        assert!(all.len() >= 3, "{family}: {all:?}");
        let prefix = &all[0]["token"];
        let mut ids = std::collections::BTreeSet::new();
        for (index, page) in all.iter().enumerate() {
            assert_eq!(&page["token"], prefix);
            assert!(page["visited"].as_u64().unwrap() <= 1);
            for object in page["results"].as_array().unwrap() {
                assert!(ids.insert(object["id"].as_str().unwrap()));
            }
            assert_eq!(page["cursor"].is_null(), index + 1 == all.len());
        }
        assert_eq!(ids.len(), 3, "{family}");
    }
    // The appended Select operation keeps its extra predicates on every cursor hop.
    let selected = pages(run(
        root.path(),
        &[
            "list",
            "claims",
            "--all",
            "--created-after",
            "0",
            "--limit",
            "1",
            "--max-visits",
            "1",
            "--format",
            "json",
        ],
    ));
    assert!(selected.len() >= 3);
    let selected_prefix = &selected[0]["token"];
    let selected_ids: std::collections::BTreeSet<_> = selected
        .iter()
        .flat_map(|page| {
            assert_eq!(&page["token"], selected_prefix);
            page["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|object| object["id"].as_str().unwrap())
        })
        .collect();
    assert_eq!(selected_ids.len(), 3);
    let empty = pages(run(
        root.path(),
        &[
            "list",
            "artifacts",
            "--all",
            "--kind",
            "never-matches",
            "--limit",
            "1",
            "--max-visits",
            "1",
            "--format",
            "json",
        ],
    ));
    assert!(empty.len() >= 3);
    assert!(
        empty
            .iter()
            .all(|page| page["results"].as_array().unwrap().is_empty())
    );
    assert!(empty.first().unwrap()["cursor"].is_string());
    assert!(empty.last().unwrap()["cursor"].is_null());
    let yaml = run(
        root.path(),
        &[
            "list",
            "claims",
            "--all",
            "--limit",
            "1",
            "--max-visits",
            "1",
            "--format",
            "yaml",
        ],
    );
    assert!(
        yaml.status.success(),
        "{}",
        String::from_utf8_lossy(&yaml.stderr)
    );
    let text = std::str::from_utf8(&yaml.stdout).unwrap();
    assert_eq!(text.lines().filter(|line| *line == "---").count(), 3);
    for document in text.split("---\n").filter(|text| !text.trim().is_empty()) {
        let _: Value = serde_saphyr::from_str(document).unwrap();
    }
    let mut child = command(
        root.path(),
        &[
            "list", "claims", "--all", "--limit", "1", "--format", "json",
        ],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    drop(child.stdout.take());
    let failed = child.wait_with_output().unwrap();
    assert!(!failed.status.success());
    assert!(
        String::from_utf8_lossy(&failed.stderr)
            .contains("List incomplete after 0 fully flushed pages")
    );
}
#[test]
fn all_list_can_cancel_blocked_output_and_reports_partial_progress() {
    use std::io::Read;
    let root = tempfile::tempdir().unwrap();
    let _server = start(root.path());
    let claims:Vec<_>=(0..10).map(|n|json!({"id":format!("{:032x}",10000+n*10),"occurrence":format!("{:032x}",10001+n*10),"description":"x".repeat(16*1024),"target":"self","action":"handoff","validations":[{"id":format!("{:032x}",10002+n*10),"kind":"receipt","phase":"whole_work","mode":"required","description":"Delivery","evaluator":"self"}]})).collect();
    // Keep each admitted mutation below the existing command/staging bound;
    // only the read-side output aggregate needs to fill the test pipe.
    for claim in claims {
        cli(
            root.path(),
            &["submit", "claim", "--json", &claim.to_string()],
        );
    }
    let mut child = command(
        root.path(),
        &[
            "list",
            "claims",
            "--all",
            "--limit",
            "1",
            "--max-visits",
            "1",
            "--format",
            "json",
        ],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut first = Vec::new();
    let mut byte = [0u8; 1];
    while stdout.read_exact(&mut byte).is_ok() {
        first.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    let page: Value = serde_json::from_slice(&first).unwrap();
    assert!(page["cursor"].is_string());
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    // Keep stdout open and unread: cancellation must not depend on a sink draining.
    let (send, receive) = mpsc::channel();
    let join = std::thread::spawn(move || {
        send.send(child.wait_with_output().unwrap()).unwrap();
    });
    let output = receive
        .recv_timeout(Duration::from_secs(10))
        .expect("cancelled list exits despite blocked stdout");
    drop(stdout);
    join.join().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("List incomplete after"), "{error}");
    assert!(error.contains("--cursor"), "{error}");
}
