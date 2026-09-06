use super::*;

fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .output()
        .unwrap()
}
fn wait_input(until: &str) -> Value {
    json!({"claim":id(100),"until":until,"timeout_ms":250})
}

#[test]
fn claim_wait_cli_and_both_mcp_profiles_observe_without_mutation_and_survive_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    mcp.consumed_mutation("claim.submit", claim());
    let before = mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"].clone();
    let pending = mcp.success("claim.wait", wait_input("satisfied"));
    assert_eq!(pending["condition"], "Pending");
    assert!(pending["operation_id"].is_null());
    assert_eq!(pending["result"]["result"]["observation"]["token"], before);
    for format in ["json", "yaml", "table"] {
        let out = cli(
            root.path(),
            &[
                "claim",
                "wait",
                &id(100),
                "--until",
                "satisfied",
                "--timeout-ms",
                "250",
                "--format",
                format,
            ],
        );
        assert_eq!(
            out.status.code(),
            Some(6),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        match format {
            "json" => assert_eq!(
                serde_json::from_slice::<Value>(&out.stdout).unwrap()["condition"],
                "Pending"
            ),
            "yaml" => assert_eq!(
                serde_saphyr::from_str::<Value>(std::str::from_utf8(&out.stdout).unwrap()).unwrap()
                    ["condition"],
                "Pending"
            ),
            _ => assert!(
                std::str::from_utf8(&out.stdout)
                    .unwrap()
                    .starts_with("Pending ")
            ),
        }
    }
    // The strict document form uses the same authored read without reserving an operation.
    let out = cli(
        root.path(),
        &[
            "claim",
            "wait",
            "--yaml",
            &format!("claim: '{}'\nuntil: terminal\ntimeout_ms: 250\n", id(100)),
            "--format",
            "json",
        ],
    );
    assert_eq!(out.status.code(), Some(6));
    assert_eq!(
        mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"],
        before
    );

    let mut waiting = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args([
            "claim",
            "wait",
            &id(100),
            "--until",
            "terminal",
            "--timeout-ms",
            "5000",
            "--format",
            "json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // This interval is test coordination only; production pacing is deadline driven.
    std::thread::sleep(Duration::from_millis(100));
    mcp.consumed_mutation(
        "claim.cancel",
        json!({"claim":id(100),"reason":"completed elsewhere"}),
    );
    let began = Instant::now();
    while waiting.try_wait().unwrap().is_none() {
        assert!(began.elapsed() < Duration::from_secs(7));
        std::thread::sleep(Duration::from_millis(10));
    }
    let out = waiting.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&out.stdout).unwrap()["condition"],
        "Met"
    );
    assert_eq!(
        mcp.success("claim.wait", wait_input("satisfied"))["condition"],
        "Unmet"
    );
    let terminal =
        mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"].clone();
    let unknown = mcp.call(
        "claim.wait",
        json!({"claim":id(999),"until":"terminal","timeout_ms":250}),
    );
    assert_eq!(
        unknown["result"]["structuredContent"]["result"]["code"],
        "not_found"
    );
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    for modern in [false, true] {
        let mut mcp = Mcp::start(root.path(), modern);
        let result = mcp.success("claim.wait", wait_input("terminal"));
        assert_eq!(result["condition"], "Met");
        assert_eq!(result["result"]["result"]["observation"]["token"], terminal);
        assert_eq!(
            mcp.success("claim.wait", wait_input("satisfied"))["condition"],
            "Unmet"
        );
        mcp.finish();
    }
}

#[test]
fn claim_wait_cancellation_stops_observation_and_mcp_remains_responsive() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    mcp.consumed_mutation("claim.submit", claim());
    let before = mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"].clone();
    mcp.next += 1;
    let cancelled = mcp.next;
    mcp.send(json!({"jsonrpc":"2.0","id":cancelled,"method":"tools/call","params":{"name":"claim.wait","arguments":{"claim":id(100),"until":"satisfied","timeout_ms":30000}}}));
    std::thread::sleep(Duration::from_millis(80));
    mcp.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":cancelled,"reason":"stop observing"}}));
    assert!(mcp.request("tools/list", json!({}))["result"]["tools"].is_array());
    assert_eq!(
        mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"],
        before
    );
    let mut waiting = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args([
            "claim",
            "wait",
            &id(100),
            "--until",
            "satisfied",
            "--timeout-ms",
            "30000",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(80));
    assert!(
        Command::new("kill")
            .args(["-INT", &waiting.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let began = Instant::now();
    while waiting.try_wait().unwrap().is_none() {
        assert!(began.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!waiting.wait_with_output().unwrap().status.success());
    assert_eq!(
        mcp.success("claim.get", json!({"id":id(100)}))["result"]["page"]["token"],
        before
    );
    mcp.finish();
}
