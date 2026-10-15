//! The documented native quickstart, run verbatim against a fresh native
//! ledger through the real binary (the audit's F09): the example discovery
//! prints for the native engine is what `submit` accepts, `post` posts it,
//! `get` reads it back posted, and the context-backed validation names the
//! engine it validated against. Only the ids earlier commands printed are
//! substituted, as a reader would.
#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[path = "support/fleet.rs"]
mod fleet;

use fleet::*;
use serde_json::Value;

fn line_value<'a>(output: &'a str, key: &str) -> Option<&'a str> {
    output
        .lines()
        .find_map(|line| line.strip_prefix(key).map(str::trim))
}

#[test]
fn the_documented_native_quickstart_runs_verbatim() {
    let node = Node::new("quickstart");
    activate_native(&node);
    let _server = start(&node, &[]);
    // `focal get example claim.submit --native > claim.json`
    let example = run(&node, None, &["get", "example", "claim.submit", "--native"]);
    assert!(
        example.status.success(),
        "{}",
        String::from_utf8_lossy(&example.stderr)
    );
    let document: Value = serde_json::from_slice(&example.stdout).unwrap();
    assert_ne!(
        document["target"], "self",
        "a native claim names another participant"
    );
    let claim_file = node.root().join("claim.json");
    std::fs::write(&claim_file, &example.stdout).unwrap();
    // `focal submit claim --file claim.json`
    let submitted = run(
        &node,
        None,
        &["submit", "claim", "--file", claim_file.to_str().unwrap()],
    );
    let text = String::from_utf8_lossy(&submitted.stdout).into_owned();
    assert!(
        submitted.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&submitted.stderr)
    );
    assert_eq!(
        line_value(&text, "CONDITION\t").unwrap(),
        "Committed",
        "{text}"
    );
    assert!(
        line_value(&text, "OPERATION_ID\t")
            .unwrap()
            .starts_with("n1:"),
        "{text}"
    );
    let claim = text
        .lines()
        .find_map(|line| line.strip_prefix("CREATED\tclaim\t"))
        .expect("the created claim's id")
        .trim()
        .to_owned();
    assert_eq!(claim.len(), 32, "{text}");
    // `focal post claim <CLAIM>`
    let posted = run(&node, None, &["post", "claim", &claim]);
    let text = String::from_utf8_lossy(&posted.stdout).into_owned();
    assert!(
        posted.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&posted.stderr)
    );
    assert_eq!(
        line_value(&text, "CONDITION\t").unwrap(),
        "Committed",
        "{text}"
    );
    assert_eq!(line_value(&text, "OPERATION\t").unwrap(), "post", "{text}");
    // `focal get claim <CLAIM>`: the claim, posted.
    let object = read_claim(&node, &claim);
    assert_eq!(claim_id(&object), claim, "{object}");
    assert_eq!(object["Claim"]["status"], 2, "posted: {object}");
    // The context-backed validation names the engine it compiled against.
    let validated = run(
        &node,
        None,
        &[
            "validate",
            "document",
            "claim.submit",
            "--file",
            claim_file.to_str().unwrap(),
        ],
    );
    let text = String::from_utf8_lossy(&validated.stdout).into_owned();
    assert!(
        validated.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&validated.stderr)
    );
    assert!(
        text.starts_with("valid claim.submit (native engine;"),
        "{text}"
    );
    // The V1 example is refused by the native ledger, by the field its
    // contract lacks: no silent fallback to the other engine.
    let v1 = run(&node, None, &["get", "example", "claim.submit"]);
    let v1_file = node.root().join("v1.json");
    std::fs::write(&v1_file, &v1.stdout).unwrap();
    let refused = run(
        &node,
        None,
        &[
            "validate",
            "document",
            "claim.submit",
            "--file",
            v1_file.to_str().unwrap(),
        ],
    );
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("evidence_schemas"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
}
