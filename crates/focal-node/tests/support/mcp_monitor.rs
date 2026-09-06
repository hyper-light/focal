use super::*;
use focal_wire::MonitorPage;

fn read(mcp: &mut Mcp, monitor: u128) -> MonitorPage {
    let value = mcp.success("monitor.get", json!({"id":id(monitor)}));
    assert!(value["operation_id"].is_null());
    serde_json::from_value(value["result"]["page"].clone()).unwrap()
}
fn cli(root: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root)
        .args(args)
        .args(["--format", "json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn actual_monitor_cli_mcp_registration_retry_and_release_survive_restart() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    mcp.consumed_mutation("claim.submit", claim());
    let mut waited = claim();
    waited["id"] = json!(id(200));
    waited["occurrence"] = json!(id(201));
    waited["validations"][0]["id"] = json!(id(202));
    mcp.consumed_mutation("claim.submit", waited);
    let op = mcp.reserve();
    let registered = mcp.managed_mutation("monitor.register", &op, json!({"monitor":id(300),"owner":id(100),"roots":[{"predicate":"terminal","claim":id(200)}],"deadline":{"timer":id(310),"generation":1,"at":4102444800u64}}));
    let pending = read(&mut mcp, 300);
    assert!(pending.monitor.as_ref().unwrap().released.is_none());
    let cli_registered = cli(
        root.path(),
        &[
            "monitor",
            "register",
            "--id",
            &id(301),
            "--owner",
            &id(100),
            "--root",
            &format!("released:{}", id(200)),
            "--timer",
            &id(311),
            "--generation",
            "2",
            "--at",
            "4102444800",
        ],
    );
    assert_eq!(cli_registered["result"]["monitor"], id(301));
    let cli_page: MonitorPage =
        serde_json::from_value(cli(root.path(), &["monitor", "get", &id(300)])).unwrap();
    assert_eq!(cli_page.monitor, pending.monitor);
    mcp.finish();
    drop(server);
    let server = start(root.path());
    let mut mcp = Mcp::start(root.path(), false);
    assert_eq!(read(&mut mcp, 300).monitor, pending.monitor);
    let retried = mcp.success("request.retry", json!({"operation_id":op}));
    assert_eq!(
        retried["result"]["receipt"],
        registered["result"]["receipt"]
    );
    mcp.success("request.acknowledge", json!({"operation_id":op}));
    cli(
        root.path(),
        &[
            "claim",
            "cancel",
            &id(200),
            "--reason",
            "Waiting target is canceled",
        ],
    );
    let released = read(&mut mcp, 300);
    let other = read(&mut mcp, 301);
    assert!(released.monitor.as_ref().unwrap().released.is_some());
    assert_eq!(
        released.monitor.as_ref().unwrap().released,
        other.monitor.as_ref().unwrap().released
    );
    let absent = mcp.call("monitor.get", json!({"id":id(999)}));
    assert_eq!(absent["result"]["isError"], true);
    assert_eq!(
        absent["result"]["structuredContent"]["result"]["code"],
        "not_found"
    );
    let missing = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--data-dir")
        .arg(root.path())
        .args(["monitor", "get", &id(999), "--format", "json"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(4));
    let page: MonitorPage = serde_json::from_slice(&missing.stdout).unwrap();
    assert!(page.monitor.is_none());
    mcp.finish();
    drop(server);
    let _server = start(root.path());
    let mut mcp = Mcp::start(root.path(), true);
    assert_eq!(read(&mut mcp, 300).monitor, released.monitor);
    mcp.finish();
}
