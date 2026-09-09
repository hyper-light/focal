//! The adapter on a native ledger: the engine probe, the native catalogue,
//! exact journaled frames, explicit acknowledgment and the recovery tools,
//! driven through the real stdio server with a controlled transport.
use super::*;
use focal_client::native_store::{NativeOperationStore, NativeStoreLimits};
use focal_client::operations::native_descriptors;
use focal_native_client::{CompileLimits, fingerprint};
use std::path::PathBuf;

struct Journal {
    path: PathBuf,
}
impl crate::NativeJournal for Journal {
    fn initialized(&self) -> bool {
        self.path.exists()
    }
    fn open(self: Box<Self>) -> Result<NativeOperationStore, crate::JournalError> {
        if self.path.exists() {
            NativeOperationStore::open(&self.path, NativeStoreLimits::default())
        } else {
            NativeOperationStore::create(&self.path, NativeStoreLimits::default())
        }
        .map_err(|error| Box::new(error) as crate::JournalError)
    }
}
fn standing() -> NativeStanding {
    NativeStanding {
        principal: context().principal,
        role: NativePeerRole::Actor,
        profile: NativeProfile::AuthoredV1,
        native_sequence: SessionSeq(2),
        logical_time: 7,
    }
}
fn page(request: &RequestEnvelope, objects: Vec<NativeObject>) -> Response {
    Response::NativeRead(NativeReadPage {
        token: ReadToken {
            ledger: request.ledger,
            sequence: SessionSeq(2),
            route_epoch: request.route_epoch,
        },
        native_sequence: SessionSeq(2),
        logical_time: 7,
        objects,
        next: None,
        visited: 1,
    })
}
enum Probe {
    Native,
    LegacyProtocol,
    LegacyOperation,
    Transport,
}
struct NativeRunning {
    running: Running,
}
impl NativeRunning {
    /// Start the adapter with a native journal at `journal` and answer the
    /// probe as scripted. The probe is the first request of every start.
    fn start(root: &Path, journal: &Path, probe: Probe) -> Self {
        let operations = root.join("operations");
        let running = Running::start_with(&operations, operations.exists(), |backend| {
            backend.with_native_journal(Box::new(Journal {
                path: journal.to_path_buf(),
            }))
        });
        let observed = running.observed();
        let Operation::NativeRead(read) = &observed.request.operation else {
            panic!("the probe must be the standing read");
        };
        assert_eq!(read.query, NativeReadQuery::Standing);
        assert_eq!(observed.request.protocol, NATIVE_PROTOCOL_VERSION);
        let reply = match probe {
            Probe::Native => page(&observed.request, vec![NativeObject::Standing(standing())]),
            Probe::LegacyProtocol => Response::Error(AccessError::UnsupportedProtocol),
            Probe::LegacyOperation => Response::Error(AccessError::UnsupportedOperation),
            Probe::Transport => {
                drop(observed.response);
                return Self { running };
            }
        };
        observed
            .response
            .send(Ok(observed.request.reply(reply)))
            .unwrap();
        Self { running }
    }
    fn names(&mut self, id: u64) -> Vec<String> {
        let mut names = Vec::new();
        let mut cursor = None;
        let mut next = id;
        loop {
            let params = match &cursor {
                Some(cursor) => json!({"cursor": cursor}),
                None => json!({}),
            };
            self.running.rpc(next, "tools/list", params);
            let response = self.running.response(next);
            for tool in response["result"]["tools"].as_array().unwrap() {
                names.push(tool["name"].as_str().unwrap().to_string());
            }
            match response["result"]["nextCursor"].as_str() {
                Some(value) => cursor = Some(value.to_string()),
                None => return names,
            }
            next += 1;
        }
    }
    /// One advertised tool by name, whichever catalogue page carries it.
    fn find_tool(&mut self, id: u64, name: &str) -> Value {
        let mut cursor: Option<String> = None;
        let mut next = id;
        loop {
            let params = match &cursor {
                Some(cursor) => json!({"cursor": cursor}),
                None => json!({}),
            };
            self.running.rpc(next, "tools/list", params);
            let response = self.running.response(next);
            if let Some(tool) = response["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|tool| tool["name"] == name)
            {
                return tool.clone();
            }
            match response["result"]["nextCursor"].as_str() {
                Some(value) => cursor = Some(value.to_string()),
                None => panic!("tool {name} is not advertised"),
            }
            next += 1;
        }
    }
    fn tool(&mut self, id: u64, name: &str, args: Value) -> Value {
        self.running.tool(id, name, args);
        self.running.response(id)
    }
    fn native_frame(&self) -> (Observed, Vec<u8>) {
        let observed = self.running.observed();
        let Operation::Native { frame } = &observed.request.operation else {
            panic!(
                "expected a native frame, got {:?}",
                observed.request.operation
            );
        };
        assert_eq!(observed.request.protocol, NATIVE_PROTOCOL_VERSION);
        let frame = frame.clone();
        (observed, frame)
    }
    fn commit(&self, kind: NativeOperationKind) -> NativeReceipt {
        let (observed, frame) = self.native_frame();
        let receipt = receipt(&observed.request, &frame, kind);
        observed
            .response
            .send(Ok(observed.request.reply(Response::Native(
                NativeMutationReply::Committed(receipt),
            ))))
            .unwrap();
        receipt
    }
}
fn receipt(request: &RequestEnvelope, frame: &[u8], kind: NativeOperationKind) -> NativeReceipt {
    let limits = CompileLimits::default();
    NativeReceipt {
        invocation: NativeInvocationRef::Request(RequestKey {
            principal: context().principal,
            epoch: RequestEpoch(1),
            id: request.request_id,
        }),
        sequence: SessionSeq(9),
        logical_time: 11,
        operation: kind,
        intent: fingerprint(frame, limits.native, limits.frame).unwrap(),
        counts: NativeOutcomeCounts {
            created: 2,
            changed: 0,
            definitions: 1,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 1,
        },
    }
}
fn native_claim() -> Value {
    json!({"description":"persist this exact occurrence","target":"00000000000000000000000000000002",
        "validations":[{"kind":"receipt","description":"receipt evidence","deadline":{"at":10000}}]})
}
fn claim_object(id: ClaimId) -> NativeObject {
    NativeObject::Claim(Box::new(NativeClaim {
        binding: NativeBinding {
            object: ObjectId(id.0),
            content: ContentHash([3; 32]),
            revision: ObjectRevision(1),
        },
        issuer: context().principal,
        subject: ParticipantId::from_u128(2),
        created: SessionSeq(1),
        deadline: None,
        status: ClaimStatus::Generated,
        origin: NativeClaimOrigin::Native,
        released: false,
        receipt: None,
        local_complete: false,
        local_sealed_at: None,
        terminal: None,
        latest_response: None,
        response_count: 0,
        max_responses: 4,
        obligations: Vec::new(),
        cause: Cause::Root(build().root),
        corrections: Vec::new(),
        acceptance: Vec::new(),
        scopes: None,
        content: None,
    }))
}

#[test]
fn the_probe_selects_the_native_catalogue_and_frames_are_journaled_and_acknowledged() {
    let root = tempfile::tempdir().unwrap();
    let journal = root.path().join("native");
    let mut mcp = NativeRunning::start(root.path(), &journal, Probe::Native);

    // The catalogue: every native descriptor plus the four recovery tools,
    // and none of the V1 application, managed, transfer or watch tools.
    let names = mcp.names(1);
    let mut expected: Vec<String> = native_descriptors()
        .iter()
        .map(|descriptor| descriptor.name.to_string())
        .chain(
            [
                "request.inspect",
                "request.retry",
                "request.pending",
                "request.acknowledge",
            ]
            .map(String::from),
        )
        .collect();
    // The protocol serves its catalogue in name order.
    expected.sort();
    assert_eq!(names, expected);
    let submit = mcp.find_tool(10, "claim.submit");
    assert!(
        submit["inputSchema"]["properties"]["operation_id"]["pattern"]
            .as_str()
            .unwrap()
            .starts_with("^n1:")
    );
    assert!(
        !submit["inputSchema"]["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "operation_id")
    );
    assert_eq!(
        submit["outputSchema"]["$id"],
        "urn:focal:mcp:claim.submit:output:2"
    );

    // Creation needs no read: the frame is journaled, sent and committed.
    mcp.running.tool(11, "claim.submit", native_claim());
    let committed = mcp.commit(NativeOperationKind::Create);
    let result = application(&mcp.running.response(11));
    assert_eq!(result.schema_version, 2);
    assert_eq!(result.condition, "Committed");
    let id = result.operation_id.clone().unwrap();
    assert!(id.starts_with("n1:"));
    let OperationOutput::Native {
        receipt: bound,
        created,
    } = &result.result
    else {
        panic!("{result:?}");
    };
    assert_eq!(*bound, committed);
    assert_eq!(created.len(), 2);
    let claim = created[0];
    assert_eq!(
        claim.kind,
        focal_client::native_store::NativeIdentityKind::Claim
    );

    // The result stays listed until it is acknowledged; the receipt is
    // reprinted without a send, and a retry of a committed operation sends
    // nothing either.
    let pending = application(&mcp.tool(12, "request.pending", json!({})));
    assert_eq!(
        pending.result,
        OperationOutput::ManagedRequests {
            operation_ids: vec![id.clone()]
        }
    );
    let inspected = application(&mcp.tool(13, "request.inspect", json!({"operation_id": id})));
    assert_eq!(inspected.condition, "Committed");
    assert_eq!(inspected.result, result.result);
    let retried = application(&mcp.tool(14, "request.retry", json!({"operation_id": id})));
    assert_eq!(retried.result, result.result);
    assert!(mcp.running.requests.try_recv().is_err());
    let consumed = application(&mcp.tool(15, "request.acknowledge", json!({"operation_id": id})));
    assert_eq!(consumed.condition, "Consumed");
    let pending = application(&mcp.tool(16, "request.pending", json!({})));
    assert_eq!(
        pending.result,
        OperationOutput::ManagedRequests {
            operation_ids: Vec::new()
        }
    );

    // A post binds to the claim read once before the send.
    let claim_hex: String = claim.id.iter().map(|b| format!("{b:02x}")).collect();
    mcp.running
        .tool(17, "claim.post", json!({"claim": claim_hex}));
    let observed = mcp.running.observed();
    let Operation::NativeRead(read) = &observed.request.operation else {
        panic!("the post reads its binding first");
    };
    assert_eq!(
        read.query,
        NativeReadQuery::Objects(vec![NativeObjectRef::Claim(ClaimId(claim.id))])
    );
    let reply = page(&observed.request, vec![claim_object(ClaimId(claim.id))]);
    observed
        .response
        .send(Ok(observed.request.reply(reply)))
        .unwrap();
    mcp.commit(NativeOperationKind::Post);
    let posted = application(&mcp.running.response(17));
    assert_eq!(posted.condition, "Committed");
    let post_id = posted.operation_id.clone().unwrap();
    assert_ne!(post_id, id);
    application(&mcp.tool(30, "request.acknowledge", json!({"operation_id": post_id})));

    // A pending ticket is an unknown outcome; the exact frame is retried
    // byte for byte and the committed receipt binds to it.
    let mut second = native_claim();
    second["description"] = json!("a second occurrence");
    mcp.running.tool(18, "claim.submit", second);
    let (observed, frame) = mcp.native_frame();
    let key = RequestKey {
        principal: context().principal,
        epoch: RequestEpoch(1),
        id: observed.request.request_id,
    };
    observed
        .response
        .send(Ok(observed.request.reply(Response::Native(
            NativeMutationReply::Pending(NativeTicket {
                key,
                intent: ContentHash([1; 32]),
            }),
        ))))
        .unwrap();
    let unknown = application(&mcp.running.response(18));
    assert_eq!(unknown.condition, "OutcomeUnknown");
    let second_id = unknown.operation_id.clone().unwrap();
    let pending = application(&mcp.tool(19, "request.pending", json!({})));
    assert_eq!(
        pending.result,
        OperationOutput::ManagedRequests {
            operation_ids: vec![second_id.clone()]
        }
    );
    let inspected =
        application(&mcp.tool(20, "request.inspect", json!({"operation_id": second_id})));
    assert_eq!(inspected.condition, "Pending");
    mcp.running
        .tool(21, "request.retry", json!({"operation_id": second_id}));
    let (observed, retried_frame) = mcp.native_frame();
    assert_eq!(retried_frame, frame);
    assert_eq!(observed.request.request_id, key.id);
    let receipt = receipt(
        &observed.request,
        &retried_frame,
        NativeOperationKind::Create,
    );
    observed
        .response
        .send(Ok(observed.request.reply(Response::Native(
            NativeMutationReply::Committed(receipt),
        ))))
        .unwrap();
    let retried = application(&mcp.running.response(21));
    assert_eq!(retried.condition, "Committed");
    assert_eq!(retried.operation_id.as_deref(), Some(second_id.as_str()));

    // A refusal is a typed result, recorded as reported, and leaves the
    // pending list; the journaled frame stays inspectable.
    let mut third = native_claim();
    third["description"] = json!("a third occurrence");
    third["operation_id"] = json!("n1:000000000000000000000000000000ab");
    mcp.running.tool(22, "claim.submit", third);
    let (observed, _) = mcp.native_frame();
    assert_eq!(observed.request.request_id, RequestId::from_u128(0xab));
    let refusal = NativeRefusal {
        kind: NativeRefusalKind::Refused(NativeErrorCode::StaleRevision),
        detail: "stale".into(),
    };
    observed
        .response
        .send(Ok(observed.request.reply(Response::Native(
            NativeMutationReply::Refused(refusal.clone()),
        ))))
        .unwrap();
    let refused = mcp.running.response(22);
    assert_eq!(refused["result"]["isError"], true);
    let refused = application(&refused);
    assert_eq!(refused.condition, "Error");
    assert_eq!(
        refused.operation_id.as_deref(),
        Some("n1:000000000000000000000000000000ab")
    );
    assert_eq!(
        refused.result,
        OperationOutput::NativeRefused {
            refusal: refusal.clone()
        }
    );
    // Only the unacknowledged committed retry remains listed.
    let pending = application(&mcp.tool(23, "request.pending", json!({})));
    assert_eq!(
        pending.result,
        OperationOutput::ManagedRequests {
            operation_ids: vec![second_id.clone()]
        }
    );
    let inspected = application(&mcp.tool(
        24,
        "request.inspect",
        json!({"operation_id": "n1:000000000000000000000000000000ab"}),
    ));
    assert_eq!(inspected.result, OperationOutput::NativeRefused { refusal });

    // Reads go straight to the owner; V1 and hidden tools are not served.
    mcp.running.tool(25, "ledger.standing", json!({}));
    let observed = mcp.running.observed();
    let reply = page(&observed.request, vec![NativeObject::Standing(standing())]);
    observed
        .response
        .send(Ok(observed.request.reply(reply)))
        .unwrap();
    let read = application(&mcp.running.response(25));
    assert_eq!(read.condition, "Read");
    assert!(matches!(read.result, OperationOutput::NativeRead { .. }));
    for name in [
        "ledger.traverse",
        "request.reserve",
        "upload.begin",
        "watch.open",
    ] {
        let response = mcp.running.tool_response(26, name, json!({}));
        assert!(
            response["error"].is_object() || response["result"]["isError"] == true,
            "{name}: {response}"
        );
    }
    mcp.running.stop();

    // A restart on the same journal probes again and finds the second
    // operation's receipt bound; only acknowledgment retires it.
    let mut mcp = NativeRunning::start(root.path(), &journal, Probe::Native);
    let pending = application(&mcp.tool(1, "request.pending", json!({})));
    assert_eq!(
        pending.result,
        OperationOutput::ManagedRequests {
            operation_ids: vec![second_id.clone()]
        }
    );
    let inspected =
        application(&mcp.tool(2, "request.inspect", json!({"operation_id": second_id})));
    assert_eq!(inspected.condition, "Committed");
    mcp.running.stop();
}

#[test]
fn legacy_answers_and_an_unreachable_owner_keep_the_v1_catalogue_unless_the_journal_exists() {
    let root = tempfile::tempdir().unwrap();
    let journal = root.path().join("native");
    for probe in [
        Probe::LegacyProtocol,
        Probe::LegacyOperation,
        Probe::Transport,
    ] {
        let mut mcp = NativeRunning::start(root.path(), &journal, probe);
        let names = mcp.names(1);
        assert!(names.iter().any(|name| name == "claim.list"));
        assert!(names.iter().any(|name| name == "request.reserve"));
        assert!(!names.iter().any(|name| name == "ledger.standing"));
        assert!(!journal.exists());
        mcp.running.stop();
    }
    // Once a native journal exists, an unreachable owner refuses to start
    // the adapter as V1 rather than minting identities in the wrong namespace.
    NativeOperationStore::create(&journal, NativeStoreLimits::default()).unwrap();
    let operations = root.path().join("operations");
    let running = Running::start_with(&operations, operations.exists(), |backend| {
        backend.with_native_journal(Box::new(Journal {
            path: journal.clone(),
        }))
    });
    let observed = running.observed();
    drop(observed.response);
    let result = running
        .done
        .recv_timeout(Duration::from_secs(5))
        .expect("the adapter must stop");
    assert!(matches!(result, Err(ServeError::Probe(_))), "{result:?}");
}
