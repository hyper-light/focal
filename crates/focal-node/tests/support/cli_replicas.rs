use super::*;

#[test]
fn actual_cli_and_mcp_administer_installed_data_membership_with_distinct_restart_receipts() {
    let founder = tempfile::tempdir_in("/tmp").unwrap();
    let peer = tempfile::tempdir_in("/tmp").unwrap();
    for path in [founder.path(), peer.path()] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let listen = address();
    let (server, _) = start(founder.path(), Some(&listen));
    let mut mcp = super::client_context::PeerMcp::open(founder.path());
    let identity = success(founder.path(), &["inspect", "identity"]).0;
    let inspected = mcp.call("diagnose.node.identity", json!({}));
    assert_eq!(
        inspected["result"]["result"]["identity"]["node"],
        identity["node"]
    );
    assert_eq!(
        success(founder.path(), &["inspect", "node", "--identity"]).0["result"],
        inspected["result"]["result"]
    );
    let config = mcp.call("diagnose.node.listener", json!({}));
    assert_eq!(
        config["result"]["result"]["configuration"]["advertise"],
        listen
    );
    assert_eq!(
        success(founder.path(), &["inspect", "node", "--listener"]).0["result"],
        config["result"]["result"]
    );
    let health = mcp.call("diagnose.node.health", json!({}));
    assert_eq!(health["result"]["result"]["health"]["root_stopped"], false);
    assert_eq!(health["result"]["result"]["health"]["running"], 1);
    let listed = mcp.call("cluster.replicas.list", json!({}));
    let rows = listed["result"]["result"]["replicas"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["sequence"], 0);
    let session = rows[0]["session"].as_str().unwrap().to_owned();
    let diagnostics = mcp.call("diagnose.cluster.replicas", json!({"session":session}));
    let observed = &diagnostics["result"]["result"]["diagnostics"];
    assert_eq!(observed["session"], session);
    assert_eq!(observed["sequence"], 0);
    assert_eq!(observed["managed_active"], false);
    assert_eq!(observed["required_decoder"], serde_json::Value::Null);
    assert_eq!(
        observed["compiled_managed_decoder"].as_str().unwrap().len(),
        64
    );
    let cli_diagnostics = success(founder.path(), &["inspect", "replicas", "--replicas"]).0;
    assert_eq!(
        cli_diagnostics["result"]["diagnostics"]["required_decoder"],
        serde_json::Value::Null
    );
    let shown = mcp.call("cluster.replicas.show", json!({"session":session}));
    let config = shown["result"]["result"]["membership"].clone();
    let root = success(founder.path(), &["inspect", "membership"]).0;
    assert_ne!(config["group"], root["result"]["configuration"]["group"]);
    let invitation = peer.path().join("peer.invite");
    let invited = mcp.call(
        "cluster.invite",
        json!({"node":"replica-peer","output":invitation.to_str().unwrap()}),
    );
    assert_eq!(invited["result"]["result"]["kind"], "invitation_written");
    let encoded = std::fs::read(&invitation).unwrap();
    mcp.call(
        "cluster.invite",
        json!({"node":"replica-peer","output":invitation.to_str().unwrap()}),
    );
    assert_eq!(std::fs::read(&invitation).unwrap(), encoded);
    success(
        peer.path(),
        &[
            "join",
            "cluster",
            "--invite-file",
            invitation.to_str().unwrap(),
            "--advertise",
            &address(),
        ],
    );
    let identity = success(peer.path(), &["inspect", "identity"]).0;
    let node = identity["node"].as_u64().unwrap();
    let admitted = success(
        founder.path(),
        &[
            "add",
            "replica-learner",
            "--session",
            &session,
            "--node",
            &node.to_string(),
            "--expected-configuration-index",
            &config["configuration_index"].as_u64().unwrap().to_string(),
        ],
    )
    .0;
    assert_eq!(admitted["result"]["kind"], "replica_committed");
    assert_eq!(admitted["result"]["membership"]["learners"], json!([node]));
    let previous = admitted["result"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(previous.starts_with("r1:"));
    let removed = mcp.call(
        "cluster.replicas.remove",
        json!({"session":session,"node":node}),
    );
    let committed = removed["result"]["result"].clone();
    assert_eq!(committed["kind"], "replica_committed");
    assert_eq!(committed["membership"]["learners"], json!([]));
    let reference = committed["operation_id"].as_str().unwrap().to_owned();
    assert_ne!(reference, previous);
    assert_eq!(
        success(founder.path(), &["inspect", "replica-request"]).0["result"],
        committed
    );
    assert!(
        !command(founder.path(), &["retry", "replica-request", &previous])
            .status
            .success()
    );
    let inventory = success(founder.path(), &["list", "replicas"]).0;
    assert_eq!(
        inventory["result"]["replicas"][0]["sequence"], 0,
        "membership metadata never consumes domain sequence"
    );
    drop(mcp);
    drop(server);
    let (_restart, _) = start(founder.path(), None);
    assert_eq!(
        success(founder.path(), &["retry", "replica-request", &reference]).0["result"],
        committed
    );
    assert_eq!(
        success(
            founder.path(),
            &["reconcile", "replica-request", &reference]
        )
        .0["result"],
        committed
    );
}
