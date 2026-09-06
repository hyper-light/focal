use super::*;
use focal_model::{CommandResult, ManagedReceipt, ManagedReceiptOutcome};

fn read(mcp: &mut Mcp) -> focal_wire::LedgerSummary {
    let result = mcp.success("ledger.summary", json!({}));
    assert_eq!(result["condition"], "Observed");
    assert!(result["operation_id"].is_null());
    serde_json::from_value(result["result"]["summary"].clone()).unwrap()
}
fn counts(value: &focal_wire::LedgerSummary) -> [u64; 6] {
    [
        value.claims,
        value.testaments,
        value.artifacts,
        value.validations,
        value.evidence_sets,
        value.validation_runs,
    ]
}
#[test]
fn cli_and_both_mcp_profiles_observe_six_committed_counts_without_mutation_or_graph_download() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    let empty = read(&mut mcp);
    assert_eq!(counts(&empty), [0; 6]);
    mcp.consumed_mutation("claim.submit", claim());
    mcp.consumed_mutation("claim.post", json!({"claim":id(100)}));
    mcp.consumed_mutation(
        "receipt.acquire",
        json!({"claim":id(100),"id":id(201),"epoch":1}),
    );
    let receipt = json!({"id":id(201),"epoch":1});
    mcp.consumed_mutation(
        "evidence.begin",
        json!({"claim":id(100),"receipt":receipt,"id":id(202)}),
    );
    let artifact=mcp.consumed_mutation("artifact.submit",json!({"claim":id(100),"receipt":receipt,"evidence_set":id(202),"kind":"test-report","schema_hash":focal_evidence::test_report_schema().to_string(),"payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}));
    let artifact: ManagedReceipt =
        serde_json::from_value(artifact["result"]["receipt"].clone()).unwrap();
    let ManagedReceiptOutcome::Domain(CommandResult::Artifact(reference)) = artifact.outcome else {
        panic!("artifact receipt")
    };
    mcp.consumed_mutation("testament.submit",json!({"id":id(203),"claim":id(100),"receipt":receipt,"evidence_set":id(202),"manifest":[{"id":reference.id.to_string(),"hash":reference.hash.to_string()}],"summary":"One proof","confidence":"committed","outcome":"complete"}));
    mcp.consumed_mutation(
        "testament.receive",
        json!({"claim":id(100),"testament":id(203)}),
    );
    mcp.consumed_mutation("validation.begin", json!({"claim":id(100)}));
    let observed = read(&mut mcp);
    assert_eq!(counts(&observed), [1, 1, 1, 1, 1, 1]);
    assert!(observed.token.sequence > empty.token.sequence);
    assert_eq!(read(&mut mcp), observed);
    for format in ["json", "yaml", "table"] {
        let output = Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root.path())
            .args(["ledger", "summary", "--format", format])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if format == "table" {
            let text = String::from_utf8(output.stdout).unwrap();
            assert!(text.contains("CLAIMS"));
            assert!(text.contains("VALIDATION RUNS"));
        } else {
            let value: Value = if format == "json" {
                serde_json::from_slice(&output.stdout).unwrap()
            } else {
                serde_saphyr::from_str(std::str::from_utf8(&output.stdout).unwrap()).unwrap()
            };
            let actual: focal_wire::LedgerSummary =
                serde_json::from_value(value["summary"].clone()).unwrap();
            assert_eq!(actual, observed);
        }
    }
    let bad = mcp.call("ledger.summary", json!({"prefix":observed.token}));
    assert_eq!(
        bad["result"]["structuredContent"]["result"]["code"],
        "invalid_input"
    );
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    let recovered = read(&mut mcp);
    assert_eq!(counts(&recovered), counts(&observed));
    assert_eq!(recovered.token, observed.token);
    assert!(recovered.applied_index >= observed.applied_index);
    assert_eq!(read(&mut mcp), recovered);
    mcp.finish();
}
