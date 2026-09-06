use super::*;
use focal_model::{CommandResult, ManagedReceipt, ManagedReceiptOutcome};
fn list(mcp: &mut Mcp, name: &str, input: Value) -> focal_wire::ListPage {
    let value = mcp.success(name, input);
    assert_eq!(value["condition"], "Listed");
    assert!(value["operation_id"].is_null());
    serde_json::from_value(value["result"]["page"].clone()).unwrap()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn cli(root: &Path, args: &[&str]) -> Vec<Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    if args.contains(&"--all") {
        std::str::from_utf8(&output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    } else {
        vec![serde_json::from_slice(&output.stdout).unwrap()]
    }
}
#[test]
fn extended_list_predicates_cli_mcp_exact_pages_and_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    mcp.consumed_mutation("claim.submit", claim());
    let mut second = claim();
    second["id"] = json!(id(200));
    second["occurrence"] = json!(id(201));
    second["validations"][0]["id"] = json!(id(202));
    second["scopes"][0]["key"] = json!("second.rs");
    mcp.consumed_mutation("claim.submit", second);
    let input = json!({"scopes":[{"kind":"file","key":"second.rs"}],"relations":[{"kind":"issuer","target":"participant:self"}],"created_after":0,"max_visits":1,"limit":1});
    let first = list(&mut mcp, "claim.list", input.clone());
    assert!(first.objects.is_empty());
    assert!(first.next.is_some());
    let mut later = claim();
    later["id"] = json!(id(300));
    later["occurrence"] = json!(id(301));
    later["validations"][0]["id"] = json!(id(302));
    later["scopes"][0]["key"] = json!("second.rs");
    mcp.consumed_mutation("claim.submit", later);
    let mut next = input.clone();
    next["cursor"] = json!(hex(&first.next.as_ref().unwrap().bytes));
    let continuation = list(&mut mcp, "claim.list", next.clone());
    assert_eq!(continuation.token, first.token);
    assert_eq!(continuation.objects.len(), 1);
    assert!(continuation.next.is_none());
    let mut changed = next.clone();
    changed["scopes"][0]["key"] = json!("report.json");
    assert_eq!(mcp.call("claim.list", changed)["result"]["isError"], true);
    let pages = cli(
        root.path(),
        &[
            "list",
            "claims",
            "--scope",
            "file:second.rs",
            "--relation",
            "issuer=participant:self",
            "--created-after",
            "0",
            "--all",
            "--max-visits",
            "1",
            "--limit",
            "1",
        ],
    );
    assert_eq!(
        pages
            .iter()
            .map(|page| page["results"].as_array().unwrap().len())
            .sum::<usize>(),
        2
    );
    assert!(pages.iter().all(|page| page["token"] == pages[0]["token"]));
    let unique=mcp.success("claim.get",json!({"scopes":[{"kind":"file","key":"report.json"}],"relations":[{"kind":"issuer","target":"participant:self"}],"max_visits":1}));
    assert_eq!(
        unique["result"]["page"]["objects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        cli(
            root.path(),
            &[
                "get",
                "claim",
                "--scope",
                "file:report.json",
                "--relation",
                "issuer=participant:self"
            ]
        )[0]["result"]["id"],
        json!(id(100))
    );
    let ambiguous = mcp.call(
        "claim.get",
        json!({"scopes":[{"kind":"file","key":"second.rs"}],"max_visits":1}),
    );
    assert_eq!(
        ambiguous["result"]["structuredContent"]["result"]["code"],
        "ambiguous"
    );
    let bad = mcp.call("claim.get", json!({"id":id(100),"created_after":0}));
    assert_eq!(bad["result"]["isError"], true);
    mcp.consumed_mutation("claim.post", json!({"claim":id(100)}));
    mcp.consumed_mutation(
        "receipt.acquire",
        json!({"claim":id(100),"id":id(401),"epoch":1}),
    );
    let receipt = json!({"id":id(401),"epoch":1});
    mcp.consumed_mutation(
        "evidence.begin",
        json!({"claim":id(100),"receipt":receipt,"id":id(402)}),
    );
    let proof=mcp.consumed_mutation("artifact.submit",json!({"id":id(403),"claim":id(100),"receipt":receipt,"evidence_set":id(402),"kind":"test-report","schema_hash":focal_evidence::test_report_schema().to_string(),"inputs":[{"kind":"claim","id":id(100)}],"payload":{"type":"text","text":"{\"passed\":1,\"failed\":0,\"skipped\":0}"}}));
    let receipt_value: ManagedReceipt =
        serde_json::from_value(proof["result"]["receipt"].clone()).unwrap();
    let ManagedReceiptOutcome::Domain(CommandResult::Artifact(reference)) = receipt_value.outcome
    else {
        panic!("artifact")
    };
    mcp.consumed_mutation("testament.submit",json!({"id":id(404),"claim":id(100),"receipt":receipt,"evidence_set":id(402),"manifest":[{"id":reference.id.to_string(),"hash":reference.hash.to_string()}],"summary":"Proof","confidence":"committed","outcome":"complete"}));
    let cases = [
        (
            "artifact.list",
            json!({"inputs":[{"kind":"claim","id":id(100)}],"created_after":0}),
            "artifacts",
            vec!["--input".to_owned(), format!("claim:{}", id(100))],
        ),
        (
            "testament.list",
            json!({"outcome":"complete","confidence":"committed","created_after":0}),
            "testaments",
            vec![
                "--outcome".into(),
                "complete".into(),
                "--confidence".into(),
                "committed".into(),
            ],
        ),
        (
            "validation.list",
            json!({"claim":id(100),"created_after":0}),
            "validations",
            vec!["--claim".into(), id(100)],
        ),
    ];
    for (tool, input, family, flags) in &cases {
        let expected = list(&mut mcp, tool, input.clone());
        assert_eq!(expected.objects.len(), 1);
        let mut args = vec!["list", *family, "--created-after", "0"];
        args.extend(flags.iter().map(String::as_str));
        assert_eq!(
            cli(root.path(), &args)[0]["results"]
                .as_array()
                .unwrap()
                .len(),
            expected.objects.len()
        );
    }
    let exact_claim = mcp.success("claim.get", json!({"id":id(100)}));
    let claim: focal_wire::ReadPage =
        serde_json::from_value(exact_claim["result"]["page"].clone()).unwrap();
    let focal_wire::ReadObject::Claim { value, .. } = &claim.objects[0] else {
        panic!("claim")
    };
    let cause = value
        .content()
        .relations
        .iter()
        .find_map(|relation| match relation.target {
            focal_model::RelationTarget::Root(root) => Some(format!("root:{root}")),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        list(
            &mut mcp,
            "claim.list",
            json!({"claim":id(100),"caused_by":cause})
        )
        .objects
        .len(),
        1
    );
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(mcp.call("claim.list", next)["result"]["isError"], true);
    for (tool, input, _, _) in &cases {
        assert_eq!(list(&mut mcp, tool, input.clone()).objects.len(), 1);
    }
    for field in ["changed_since", "validation_result"] {
        assert_eq!(
            mcp.call("validation.list", json!({field:1}))["result"]["isError"],
            true
        );
    }
    mcp.finish();
}
