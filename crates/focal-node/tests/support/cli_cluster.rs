use super::*;
use std::time::Instant;

fn eventually(root: &Path, args: &[&str], condition: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let output = command(root, args);
        if output.status.success() {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            if condition(&value) {
                return value;
            }
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not converge: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn actual_cli_promotes_caught_up_learner_transfers_and_removes_with_exact_restart_receipt() {
    let founder = tempfile::Builder::new()
        .prefix("focal-cluster-admin-")
        .tempdir_in("/tmp")
        .unwrap();
    let peer = tempfile::Builder::new()
        .prefix("focal-cluster-peer-")
        .tempdir_in("/tmp")
        .unwrap();
    for path in [founder.path(), peer.path()] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let founder_address = address();
    let peer_address = address();
    let (server, _) = start(founder.path(), Some(&founder_address));
    let (identity, _) = success(founder.path(), &["identity"]);
    let founder_node = identity["node"].as_u64().unwrap();
    let invitation = founder.path().join("peer.invite");
    success(
        founder.path(),
        &[
            "cluster",
            "invite",
            "--node",
            "peer",
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    success(
        peer.path(),
        &[
            "join",
            "--invite-file",
            invitation.to_str().unwrap(),
            "--advertise",
            &peer_address,
        ],
    );
    let (peer_identity, _) = success(peer.path(), &["identity"]);
    let peer_node = peer_identity["node"].as_u64().unwrap();
    let (peer_server, _) = start(peer.path(), None);
    let configuration = eventually(
        founder.path(),
        &["cluster", "membership", "show"],
        |value| {
            value["result"]["configuration"]["learners"]
                .as_array()
                .is_some_and(|nodes| nodes.contains(&json!(peer_node)))
        },
    );
    let expected = configuration["result"]["configuration"]["configuration_index"]
        .as_u64()
        .unwrap()
        .to_string();
    let promoted = command(
        founder.path(),
        &[
            "cluster",
            "membership",
            "promote",
            "--node",
            &peer_node.to_string(),
            "--expected-configuration-index",
            &expected,
        ],
    );
    let inspected = success(founder.path(), &["cluster", "request", "inspect"]).0;
    let reference = inspected["result"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let receipt = eventually(
        founder.path(),
        &["cluster", "request", "retry", &reference],
        |value| value["result"]["kind"] == "committed",
    );
    if promoted.status.success() {
        assert_eq!(
            serde_json::from_slice::<Value>(&promoted.stdout).unwrap(),
            receipt
        );
    }
    assert!(receipt["result"]["committed_index"].as_u64().unwrap() > 0);
    let committed = eventually(
        founder.path(),
        &["cluster", "membership", "show"],
        |value| {
            value["result"]["configuration"]["voters"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 2)
        },
    );
    assert!(
        committed["result"]["configuration"]["learners"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let transfer = success(
        founder.path(),
        &[
            "cluster",
            "leader",
            "transfer",
            "--node",
            &peer_node.to_string(),
        ],
    )
    .0;
    assert_eq!(transfer["result"]["kind"], "transfer_initiated");
    let leader = eventually(peer.path(), &["cluster", "status"], |value| {
        value["result"]["leader"] == peer_node
    });
    assert_eq!(leader["result"]["node"], peer_node);
    success(
        peer.path(),
        &[
            "cluster",
            "leader",
            "transfer",
            "--node",
            &founder_node.to_string(),
        ],
    );
    eventually(founder.path(), &["cluster", "status"], |value| {
        value["result"]["leader"] == founder_node
    });
    let removal = success(
        founder.path(),
        &[
            "cluster",
            "membership",
            "remove",
            "--node",
            &peer_node.to_string(),
        ],
    )
    .0;
    assert_eq!(removal["result"]["kind"], "committed");
    assert!(
        !command(founder.path(), &["cluster", "request", "retry", &reference])
            .status
            .success()
    );
    let latest = removal["result"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(peer_server);
    drop(server);
    let (_restart, _) = start(founder.path(), None);
    assert_eq!(
        success(founder.path(), &["cluster", "request", "retry", &latest]).0,
        removal
    );
    assert_eq!(
        success(founder.path(), &["cluster", "request", "inspect"]).0,
        removal
    );
}

#[test]
fn actual_cli_invitation_inspection_revocation_and_retry_never_disclose_token() {
    let founder = tempfile::Builder::new()
        .prefix("focal-revoke-admin-")
        .tempdir_in("/tmp")
        .unwrap();
    let peer = tempfile::Builder::new()
        .prefix("focal-revoke-peer-")
        .tempdir_in("/tmp")
        .unwrap();
    for path in [founder.path(), peer.path()] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let (_server, _) = start(founder.path(), Some(&address()));
    let path = founder.path().join("unused.invite");
    success(
        founder.path(),
        &[
            "cluster",
            "invite",
            "--node",
            "unused",
            "--output",
            path.to_str().unwrap(),
        ],
    );
    let bundle = NodeInvitation::load(&path).unwrap();
    let id = bundle
        .invitation()
        .id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let token = bundle.invitation().expose_token().unwrap();
    let (observed, output) = success(founder.path(), &["cluster", "invitations", "get", &id]);
    assert_redacted(&output, &token);
    assert_eq!(observed["result"]["entries"][0]["id"], id);
    assert_eq!(observed["result"]["entries"][0]["revoked"], false);
    let revision = observed["result"]["revision"].as_u64().unwrap().to_string();
    let (revoked, output) = success(
        founder.path(),
        &[
            "cluster",
            "invitations",
            "revoke",
            &id,
            "--expected-revision",
            &revision,
        ],
    );
    assert_redacted(&output, &token);
    let operation = revoked["result"]["operation_id"].as_str().unwrap();
    assert_eq!(
        success(founder.path(), &["cluster", "request", "retry", operation]).0,
        revoked
    );
    let (observed, output) = success(
        founder.path(),
        &["cluster", "credentials", "get", "--invitation", &id],
    );
    assert_redacted(&output, &token);
    assert_eq!(observed["result"]["entries"][0]["revoked"], true);
    assert!(observed["result"]["entries"][0]["credential"].is_null());
    let stale = command(
        founder.path(),
        &[
            "cluster",
            "invitations",
            "list",
            "--expected-revision",
            &revision,
        ],
    );
    assert!(!stale.status.success());
    let rejected = command(
        peer.path(),
        &[
            "join",
            "--invite-file",
            path.to_str().unwrap(),
            "--advertise",
            &address(),
        ],
    );
    assert!(!rejected.status.success());
    assert_redacted(&rejected, &token);
    assert!(!peer.path().join("IDENTITY").exists());
}
