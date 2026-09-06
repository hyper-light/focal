use super::*;
fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .args(["--format", "json"])
        .output()
        .unwrap()
}
#[test]
fn both_mcp_profiles_and_cli_share_unique_ambiguous_and_empty_page_claim_selection() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    mcp.consumed_mutation("claim.submit", claim());
    let unique = mcp.success(
        "claim.get",
        json!({"source":"self","target":"self","max_visits":1}),
    );
    assert_eq!(
        unique["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let first = cli(
        root.path(),
        &["get", "claim", "--source", "self", "--target", "self"],
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let exact = mcp.success("claim.get", json!({"id":id(100)}));
    assert_eq!(
        exact["result"]["page"]["objects"],
        unique["result"]["page"]["objects"]
    );
    let mut second = claim();
    second["id"] = json!(id(200));
    second["occurrence"] = json!(id(201));
    second["validations"][0]["id"] = json!(id(202));
    second["target"] = json!(id(55));
    mcp.consumed_mutation("claim.submit", second);
    // A target residual consumes the first candidate without a match. The
    // singular resolver must still reach and prove the second claim unique.
    let empty = mcp.success(
        "claim.list",
        json!({"source":"self","target":id(55),"max_visits":1,"limit":2}),
    );
    assert!(
        empty["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!empty["result"]["page"]["next"].is_null());
    let selected = mcp.success(
        "claim.get",
        json!({"source":"self","target":id(55),"max_visits":1}),
    );
    assert_eq!(
        selected["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    mcp.consumed_mutation("claim.post", json!({"claim":id(200)}));
    for modern in [true, false] {
        if !modern {
            mcp.finish();
            mcp = Mcp::start(root.path(), false);
        }
        let ambiguous = mcp.call("claim.get", json!({"source":"self","max_visits":1}));
        assert_eq!(
            ambiguous["result"]["structuredContent"]["result"]["code"], "ambiguous",
            "{ambiguous}"
        );
        let absent = mcp.call(
            "claim.get",
            json!({"source":"self","status":"cancelled","max_visits":1}),
        );
        assert_eq!(
            absent["result"]["structuredContent"]["result"]["code"],
            "not_found"
        );
        let posted = mcp.success(
            "claim.get",
            json!({"source":"self","status":"posted","max_visits":1}),
        );
        assert_eq!(
            posted["result"]["page"]["objects"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            posted["result"]["page"]["objects"][0]["Claim"]["id"],
            serde_json::to_value(focal_model::ClaimId::from_u128(200)).unwrap()
        );
        let bad = mcp.call("claim.get", json!({"id":id(100),"source":"self"}));
        assert_eq!(
            bad["result"]["structuredContent"]["result"]["code"],
            "invalid_input"
        );
    }
    let ambiguous = cli(root.path(), &["get", "claim", "--source", "self"]);
    assert_eq!(ambiguous.status.code(), Some(5));
    let absent = cli(
        root.path(),
        &["get", "claim", "--source", "self", "--status", "cancelled"],
    );
    assert_eq!(absent.status.code(), Some(4));
    let posted = cli(
        root.path(),
        &["get", "claim", "--source", "self", "--status", "posted"],
    );
    assert!(
        posted.status.success(),
        "{}",
        String::from_utf8_lossy(&posted.stderr)
    );
    mcp.finish();
}
