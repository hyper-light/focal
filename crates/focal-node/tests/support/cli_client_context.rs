use super::*;
use std::io::Write;

pub(super) struct PeerMcp {
    _process: Server,
    input: std::process::ChildStdin,
    output: std::sync::mpsc::Receiver<Value>,
    next: u64,
}
impl PeerMcp {
    pub(super) fn open(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_focal"))
            .arg("--data-dir")
            .arg(root)
            .args(["mcp", "serve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if send.send(serde_json::from_str(&line).unwrap()).is_err() {
                    break;
                }
            }
        });
        Self {
            _process: Server(child),
            input,
            output,
            next: 1,
        }
    }
    fn response(&mut self, name: &str, args: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let input = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args,"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}});
        serde_json::to_writer(&mut self.input, &input).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        let result = self.output.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(result["id"], id, "{result}");
        assert!(result.get("error").is_none(), "{result}");
        result["result"].clone()
    }
    pub(super) fn call(&mut self, name: &str, args: Value) -> Value {
        let result = self.response(name, args);
        assert_eq!(result["isError"], false, "{result}");
        result["structuredContent"].clone()
    }
    /// A call the adapter must refuse outright: the raw JSON-RPC error.
    pub(super) fn refused(&mut self, name: &str, args: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let input = json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args,"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}});
        serde_json::to_writer(&mut self.input, &input).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        let result = self.output.recv_timeout(Duration::from_secs(30)).unwrap();
        assert_eq!(result["id"], id, "{result}");
        assert!(result["error"].is_object(), "not refused: {result}");
        result["error"].clone()
    }
}

#[test]
fn enrolled_named_context_reads_over_quic_and_mcp_survives_server_and_client_restart() {
    let founder = tempfile::Builder::new()
        .prefix("focal-context-root-")
        .tempdir_in("/tmp")
        .unwrap();
    let client = tempfile::Builder::new()
        .prefix("focal-context-actor-")
        .tempdir_in("/tmp")
        .unwrap();
    for path in [founder.path(), client.path()] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let (server, _) = start(founder.path(), Some(&address()));
    let mut admin_mcp = PeerMcp::open(founder.path());
    let status = admin_mcp.call("cluster.status", json!({}));
    assert_eq!(status["result"]["kind"], "administration");
    assert!(
        status["result"]["result"]["applied_index"]
            .as_u64()
            .unwrap()
            > 0
    );
    let invitations = admin_mcp.call("cluster.invitations.list", json!({"limit":2}));
    assert_eq!(invitations["result"]["result"]["kind"], "invitations");
    drop(admin_mcp);
    let invitation = client.path().join("alice.invite");
    success(
        founder.path(),
        &[
            "cluster",
            "client",
            "invite",
            "--name",
            "alice",
            "--output",
            invitation.to_str().unwrap(),
        ],
    );
    success(
        client.path(),
        &[
            "context",
            "enroll",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    success(
        client.path(),
        &[
            "context",
            "enroll",
            "alice",
            "--invite-file",
            invitation.to_str().unwrap(),
        ],
    );
    let (listed, _) = success(
        client.path(),
        &[
            "--client-context",
            "alice",
            "list",
            "claims",
            "--format",
            "json",
        ],
    );
    assert!(listed["results"].is_array(), "{listed}");
    success(client.path(), &["context", "use", "alice"]);
    // The enrolled certificate is an Actor. All peer transitions therefore
    // use the participant protocol and exact revision fencing, not Runtime.
    let cli = |args: &[&str]| {
        let mut args = args.to_vec();
        args.extend(["--format", "json"]);
        success(client.path(), &args).0
    };
    let claim_id = format!("{:032x}", 501);
    let validation_id = format!("{:032x}", 502);
    let claim = json!({"id":claim_id,"target":"self","action":"handoff","description":"Return an explicit participant response","validations":[{"id":validation_id,"kind":"receipt","phase":"whole_work","mode":"required","description":"Receive response","evaluator":"self"}]});
    cli(&["submit", "claim", "--json", &claim.to_string()]);
    cli(&["claim", "post", &claim_id]);
    let acquired = cli(&["receipt", "acquire", &claim_id]);
    let receipt = acquired["result"]["receipt"].as_str().unwrap();
    let begun = cli(&[
        "evidence",
        "begin",
        "--claim",
        &claim_id,
        "--receipt",
        receipt,
        "--receipt-epoch",
        "1",
    ]);
    let evidence_set = begun["result"]["evidence_set"].as_str().unwrap();
    // Pure Receipt validation accepts an empty artifact manifest. It still
    // needs the actual participant-authored Testament and receive transition.
    let closed = cli(&[
        "submit",
        "testament",
        "--claim",
        &claim_id,
        "--receipt",
        receipt,
        "--receipt-epoch",
        "1",
        "--evidence-set",
        evidence_set,
        "--summary",
        "Response delivered",
        "--confidence",
        "committed",
        "--outcome",
        "complete",
    ]);
    let testament = closed["result"]["testament"].as_str().unwrap();
    cli(&["testament", "receive", testament, "--claim", &claim_id]);
    cli(&["validation", "begin", "--claim", &claim_id]);
    cli(&["validation", "complete", "--claim", &claim_id]);
    let context = cli(&["get", "validation", &validation_id, "--context"]);
    let context: focal_client::validation_context::ValidationContext =
        serde_json::from_value(context["context"].clone()).unwrap();
    assert_eq!(
        context.claim.lifecycle().status,
        focal_model::ClaimStatus::Satisfied
    );
    let mut mcp = PeerMcp::open(client.path());
    // An enrolled participant's adapter never lists the operator surface, and
    // a hidden tool invoked directly is refused, not served.
    let refused = mcp.refused("cluster.status", json!({}));
    assert_eq!(refused["message"], "Unknown tool", "{refused}");
    assert_eq!(mcp.call("claim.list", json!({}))["result"]["kind"], "list");
    drop(mcp);
    drop(server);
    let (_restart, _) = start(founder.path(), None);
    let (reopened, _) = success(client.path(), &["list", "claims", "--format", "json"]);
    assert_eq!(reopened["results"].as_array().unwrap().len(), 1);
    let mut mcp = PeerMcp::open(client.path());
    assert_eq!(mcp.call("claim.list", json!({}))["result"]["kind"], "list");
    let bundle = focal_node::network_join::ClientInvitation::load(&invitation).unwrap();
    let invitation_id = bundle
        .invitation()
        .id()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    success(
        founder.path(),
        &[
            "cluster",
            "credentials",
            "revoke",
            "--invitation",
            &invitation_id,
        ],
    );
    // The controller installs committed grants on its bounded refresh loop.
    // Keep using the same MCP connector/QUIC connection until that projection
    // observes the revoke, then require its next request to remain denied.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let result = mcp.response("claim.list", json!({}));
        if result["isError"] == true {
            assert!(
                matches!(
                    result["structuredContent"]["result"]["code"].as_str(),
                    Some("unauthorized" | "transport")
                ),
                "{result}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "revoked grant was retained"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(mcp.response("claim.list", json!({}))["isError"], true);
    let revoked = success(
        founder.path(),
        &[
            "cluster",
            "credentials",
            "get",
            "--invitation",
            &invitation_id,
        ],
    )
    .0;
    assert_eq!(revoked["result"]["entries"][0]["revoked"], true);
    assert!(!client.path().join("IDENTITY").exists());
}
