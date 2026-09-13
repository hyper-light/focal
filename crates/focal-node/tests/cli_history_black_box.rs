#![cfg(unix)]
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! R11 §1 black-box linearizability through the real `focal` binary. Native
//! claims are submitted by the CLI with `--trace-file`, capturing each client
//! request/response exchange as JSON lines. The server is then killed and its
//! durable log reopened offline (`focal_node::history`) to recover the
//! authoritative committed prefix, and the client's observed outcomes are
//! checked linearizable against that independent order — the true black box: a
//! separate process, a real Unix transport, and a publication order taken from
//! the durable log, not from any client's belief.

use focal_client::{TraceEntry, TraceOutcome};
use focal_core::native::{NativeInvocation, NativeOutcome};
use focal_model::{CommandResult, ContentHash, LedgerId, MutationReceipt, RequestKey, SessionSeq};
use focal_sim::history::{self, Consistency, Event, Initial, Outcome, Request};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Output, Stdio},
    sync::mpsc,
    time::Duration,
};

#[path = "support/ports.rs"]
mod ports;

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

fn scratch(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in("/tmp")
        .unwrap()
}

fn start(root: &Path, advertise: &str) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args([
            "--data-dir",
            root.to_str().unwrap(),
            "start",
            "--advertise",
            advertise,
        ])
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
    let server = Server(child);
    let status = receive
        .recv_timeout(Duration::from_secs(25))
        .expect("server did not publish readiness");
    assert!(
        matches!(status["condition"].as_str(), Some("Ready" | "CatchingUp")),
        "{status}"
    );
    server
}

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str().unwrap()])
        .args(args)
        .output()
        .unwrap()
}

fn cli(root: &Path, args: &[&str]) -> Value {
    let mut args = args.to_vec();
    args.extend(["--format", "json"]);
    let output = run(root, &args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{args:?}: {error}: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn admin(root: &Path, args: &[&str]) -> Value {
    let output = run(root, args);
    assert!(
        output.status.success(),
        "{args:?}: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn committed(value: &Value) -> String {
    assert_eq!(value["condition"], "Committed", "{value}");
    value["operation_id"].as_str().unwrap().to_string()
}

// ---- checker glue (a black-box client cannot compute full-state digests, so
// state markers are per-prefix-unique values; the workload is mutations only). ----

fn receipt(
    ledger: LedgerId,
    key: RequestKey,
    sequence: SessionSeq,
    intent: ContentHash,
) -> MutationReceipt {
    MutationReceipt {
        ledger,
        key,
        sequence,
        command_hash: intent,
        outcome: CommandResult::Noop,
    }
}

fn state_marker(sequence: SessionSeq) -> ContentHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&sequence.0.to_be_bytes());
    ContentHash(bytes)
}

fn outcome_to_receipt(outcome: &NativeOutcome) -> MutationReceipt {
    let key = match outcome.invocation {
        NativeInvocation::Request(key) => key,
        other => panic!("workload issues only participant requests: {other:?}"),
    };
    receipt(outcome.ledger, key, outcome.sequence, outcome.intent)
}

/// Merge one principal's mutation trace with the independent publications into
/// the checker's event stream (publications emitted just before the completion
/// that observes them, in sequence order).
fn build_history(
    principal: focal_model::ParticipantId,
    entries: &[TraceEntry],
    publications: &[MutationReceipt],
) -> Vec<Event> {
    struct Point {
        nanos: u128,
        invoke: bool,
        call: u64,
        entry: TraceEntry,
        key: RequestKey,
    }
    let mut points = Vec::new();
    for (call, entry) in entries.iter().enumerate() {
        let key = RequestKey {
            principal,
            epoch: entry.request_epoch,
            id: entry.request_id,
        };
        points.push(Point {
            nanos: entry.invoked_nanos,
            invoke: true,
            call: call as u64,
            entry: entry.clone(),
            key,
        });
        points.push(Point {
            nanos: entry.completed_nanos,
            invoke: false,
            call: call as u64,
            entry: entry.clone(),
            key,
        });
    }
    points.sort_by(|a, b| a.nanos.cmp(&b.nanos).then(b.invoke.cmp(&a.invoke)));

    let mut events = Vec::new();
    let mut next_pub = 0usize;
    let emit_through = |events: &mut Vec<Event>, next: &mut usize, upto: SessionSeq| {
        while *next < publications.len() && publications[*next].sequence <= upto {
            let published = publications[*next].clone();
            let hash = state_marker(published.sequence);
            events.push(Event::Publish {
                receipt: published,
                state_hash: hash,
            });
            *next += 1;
        }
    };
    for point in &points {
        if point.invoke {
            let command_hash = match point.entry.outcome {
                TraceOutcome::Committed { command_hash, .. } => command_hash,
                _ => ContentHash([0; 32]),
            };
            events.push(Event::Invoke {
                call: point.call,
                request: Request::Mutation {
                    ledger: point.entry.ledger,
                    key: point.key,
                    command_hash,
                },
            });
        } else {
            let outcome = match point.entry.outcome {
                TraceOutcome::Committed {
                    sequence,
                    command_hash,
                } => {
                    emit_through(&mut events, &mut next_pub, sequence);
                    Outcome::Committed(receipt(
                        point.entry.ledger,
                        point.key,
                        sequence,
                        command_hash,
                    ))
                }
                TraceOutcome::Read { sequence } => Outcome::Read {
                    sequence,
                    state_hash: state_marker(sequence),
                },
                TraceOutcome::Refused => Outcome::Refused,
                TraceOutcome::Unknown => Outcome::Unknown,
            };
            events.push(Event::Complete {
                call: point.call,
                outcome,
            });
        }
    }
    emit_through(&mut events, &mut next_pub, SessionSeq(u64::MAX));
    events
}

fn claim_document(target: &str, n: u128) -> String {
    json!({
        "description": format!("Deliver report {n}."),
        "target": target,
        "validations": [
            {"kind": "receipt", "description": "Record delivery.", "deadline": {"at": 4_102_444_800_000u64}}
        ]
    })
    .to_string()
}

#[test]
fn cli_native_creates_are_linearizable_against_the_offline_log() {
    let founder = scratch("focal-cli-hist-");
    private(founder.path());
    let root = founder.path();

    let activation = admin(root, &["cluster", "replicas", "activate-native"]);
    assert_eq!(activation["activated"], true, "{activation}");
    let advertise = ports::address();
    let server = start(root, &advertise);

    // The founder authenticates as the issuer; the worker is the claim target.
    let identity = focal_node::embedded::decode_identity(&root.join("IDENTITY")).unwrap();
    let issuer = identity.issuer;
    let worker = identity.worker.to_string();

    // Submit three native claims through the binary, each capturing its client
    // exchange to a shared trace file.
    let trace_path = root.join("trace.jsonl");
    let trace_arg = trace_path.to_str().unwrap();
    for n in 0..3u128 {
        let value = cli(
            root,
            &[
                "--trace-file",
                trace_arg,
                "submit",
                "claim",
                "--json",
                &claim_document(&worker, n),
            ],
        );
        let id = committed(&value);
        assert!(id.starts_with("n1:"), "{value}");
    }

    // Stop the server so its durable log can be reopened offline.
    drop(server);

    // Independent publication order from the durable log.
    let outcomes = focal_node::history::offline_native_publications(root, &identity)
        .expect("offline reopen of the killed node");
    let publications: Vec<MutationReceipt> = outcomes.iter().map(outcome_to_receipt).collect();
    assert!(
        publications.len() >= 3,
        "at least the three creates committed"
    );

    // The captured trace (mutations only — a black-box read has no checkable
    // full-state digest).
    let text = std::fs::read_to_string(&trace_path).expect("trace file written");
    let entries: Vec<TraceEntry> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("trace line parses as TraceEntry"))
        .filter(|entry: &TraceEntry| entry.mutation)
        .collect();
    let committed_entries: Vec<_> = entries
        .iter()
        .filter(|entry| matches!(entry.outcome, TraceOutcome::Committed { .. }))
        .cloned()
        .collect();
    assert_eq!(committed_entries.len(), 3, "three committed creates traced");

    // Every traced commit matches an independently recovered publication.
    for entry in &committed_entries {
        let TraceOutcome::Committed {
            sequence,
            command_hash,
        } = entry.outcome
        else {
            panic!("committed_entries is filtered to committed outcomes")
        };
        let key = RequestKey {
            principal: issuer,
            epoch: entry.request_epoch,
            id: entry.request_id,
        };
        let published = publications
            .iter()
            .find(|receipt| receipt.key == key)
            .expect("traced commit is published");
        assert_eq!(published.sequence, sequence);
        assert_eq!(published.command_hash, command_hash);
    }

    let base = publications[0].sequence.0 - 1;
    let initial = [Initial {
        ledger: identity.ledger,
        sequence: SessionSeq(base),
        state_hash: state_marker(SessionSeq(base)),
    }];
    let events = build_history(issuer, &committed_entries, &publications);
    let report = history::check(&initial, &events, 512).unwrap();
    assert!(report.publications >= 3, "{report:?}");
    assert_eq!(report.unknown, 0, "{report:?}");
    let _ = Consistency::Linearizable;
}
