use crate::{Backend, MODERN_VERSION, ServeError, serve};
use focal_client::{
    Client, ClientTransport, RetryPolicy, TransportFuture,
    input::BuildContext,
    operation_store::{OperationStore, StoreLimits},
    operations::{ApplicationResult, OperationOutput},
    pending::OperationContext,
};
use focal_core::Core;
use focal_model::Limits as CoreLimits;
use focal_model::*;
use focal_wire::*;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    path::Path,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const OPERATION_ID: &str = "000000000000000000000000000000ab";
/// How long a wait may see no progress before it is over: a wedge, not
/// slowness (27 §3.1 P8).
const FROZEN: Duration = Duration::from_secs(60);
struct Observed {
    request: RequestEnvelope,
    response: hyper_rt::sync::OneshotSender<Result<ResponseEnvelope, WireError>>,
    /// Disconnected once the backend's wait for the response is gone: answered, or dropped as its
    /// call was cancelled.
    waiting: mpsc::Receiver<()>,
}
impl Observed {
    /// Whether the backend no longer waits for this response.
    fn abandoned(&self) -> bool {
        matches!(
            self.waiting.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        )
    }
}
struct Controlled {
    requests: mpsc::SyncSender<Observed>,
}
impl ClientTransport for Controlled {
    fn request<'a>(
        &'a self,
        _: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let (response, receive) =
                hyper_rt::sync::oneshot().map_err(|_| WireError::Connection)?;
            let (alive, waiting) = mpsc::sync_channel::<()>(1);
            self.requests
                .send(Observed {
                    request: request.clone(),
                    response,
                    waiting,
                })
                .map_err(|_| WireError::Connection)?;
            // Held for the wait's life: its drop tells the test the wait is gone.
            let _alive = alive;
            receive.await.map_err(|_| WireError::Connection)?
        })
    }
}
fn context() -> OperationContext {
    OperationContext {
        cluster: [9; 16],
        principal: ParticipantId::from_u128(1),
        ledger: LedgerId {
            tenant: TenantId::from_u128(2),
            session: SessionId::from_u128(3),
        },
    }
}
fn build() -> BuildContext {
    BuildContext {
        ledger: context().ledger,
        actor: context().principal,
        root: RootCommandId::from_u128(30),
        policy_revision: 1,
    }
}
fn claim() -> Value {
    json!({"operation_id":OPERATION_ID,"description":"persist this exact occurrence","target":"00000000000000000000000000000002","validations":[{"kind":"receipt","phase":"whole_work","mode":"required","description":"receipt evidence","evaluator":"00000000000000000000000000000001"}]})
}

struct Running {
    input: UnixStream,
    output: BufReader<UnixStream>,
    requests: mpsc::Receiver<Observed>,
    done: mpsc::Receiver<Result<(), ServeError>>,
    join: Option<thread::JoinHandle<()>>,
}
impl Running {
    fn start(path: &Path, reopen: bool) -> Self {
        Self::start_with(path, reopen, |backend| backend)
    }
    fn start_with(
        path: &Path,
        reopen: bool,
        configure: impl FnOnce(Backend<Controlled>) -> Backend<Controlled>,
    ) -> Self {
        Self::start_writing(path, reopen, configure, |server| server)
    }
    /// A runner whose output is `writer` over the server's end of the socket.
    fn start_writing<W: Write + Send + 'static>(
        path: &Path,
        reopen: bool,
        configure: impl FnOnce(Backend<Controlled>) -> Backend<Controlled>,
        writer: impl FnOnce(UnixStream) -> W,
    ) -> Self {
        let store = if reopen {
            OperationStore::open(
                path,
                StoreLimits::default(),
                focal_client::at_rest::AtRest::clear(),
            )
        } else {
            OperationStore::create(
                path,
                StoreLimits::default(),
                focal_client::at_rest::AtRest::clear(),
            )
        }
        .unwrap();
        let (send, requests) = mpsc::sync_channel(4);
        let client = Client::new(
            Controlled { requests: send },
            RetryPolicy {
                max_attempts: 1,
                max_elapsed: Duration::from_secs(10),
                base_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            },
            WireLimits::default(),
            1,
        )
        .unwrap();
        let backend = configure(Backend::new(client, build(), context(), store).unwrap());
        let (client, server) = UnixStream::pair().unwrap();
        client.set_read_timeout(Some(FROZEN)).unwrap();
        client.set_write_timeout(Some(FROZEN)).unwrap();
        let reader = server.try_clone().unwrap();
        let writer = writer(server);
        let (finish, done) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            let _ = finish.send(serve(backend, reader, writer));
        });
        Self {
            input: client.try_clone().unwrap(),
            output: BufReader::new(client),
            requests,
            done,
            join: Some(join),
        }
    }
    fn rpc(&mut self, id: u64, method: &str, mut params: Value) {
        params.as_object_mut().unwrap().insert("_meta".into(),json!({"io.modelcontextprotocol/protocolVersion":MODERN_VERSION,"io.modelcontextprotocol/clientCapabilities":{}}));
        self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
    }
    fn tool(&mut self, id: u64, name: &str, args: Value) {
        self.rpc(id, "tools/call", json!({"name":name,"arguments":args}));
    }
    fn tool_response(&mut self, id: u64, name: &str, args: Value) -> Value {
        self.tool(id, name, args);
        self.response(id)
    }
    fn send(&mut self, value: Value) {
        serde_json::to_writer(&mut self.input, &value).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
    }
    fn response(&mut self, id: u64) -> Value {
        let mut line = String::new();
        assert_ne!(
            self.output.read_line(&mut line).unwrap(),
            0,
            "runner closed before response {id}"
        );
        let response: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(
            response["id"], id,
            "unexpected/cancelled response: {response}"
        );
        response
    }
    fn observed(&self) -> Observed {
        self.requests.recv_timeout(FROZEN).unwrap()
    }
    fn stop(mut self) {
        self.input.shutdown(Shutdown::Write).unwrap();
        let result = self
            .done
            .recv_timeout(FROZEN)
            .expect("EOF did not finish: nothing for the frozen window");
        assert!(result.is_ok(), "runner shutdown: {result:?}");
        self.join.take().unwrap().join().unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.input.shutdown(Shutdown::Both);
        if let Some(join) = self.join.take()
            && join.is_finished()
        {
            let _ = join.join();
        }
    }
}
fn apply(core: &mut Core, request: &RequestEnvelope) -> MutationReply {
    let (runtime, expected_revision, command) = match &request.operation {
        Operation::OpenEpoch { epoch } => (true, None, Command::NegotiateEpoch { epoch: *epoch }),
        Operation::Submit {
            expected_revision,
            command,
        } => (false, *expected_revision, command.clone()),
        _ => panic!("unexpected nonmutation"),
    };
    let input = AuthenticatedInput {
        ledger: context().ledger,
        principal: context().principal,
        request_epoch: request.request_epoch,
        request_id: request.request_id,
        expected_revision,
        command,
        authority: AuthorityContext {
            runtime,
            cause: Cause::Root(build().root),
            policy_revision: 1,
            logical_time: 0,
            evidence: Vec::new(),
        },
    };
    match core.prepare(&input) {
        Ok(prepared) => MutationReply::Committed(
            core.apply(SessionSeq(core.sequence().0 + 1), prepared)
                .unwrap()
                .receipt,
        ),
        Err(DomainOutcome::Duplicate(receipt)) => {
            MutationReply::Domain(DomainOutcome::Duplicate(receipt))
        }
        outcome => panic!("core admission: {outcome:?}"),
    }
}
fn answer(observed: Observed, result: MutationReply) {
    let response = observed.request.reply(Response::Submitted(result));
    observed.response.send(Ok(response)).unwrap();
}
fn application(response: &Value) -> ApplicationResult {
    serde_json::from_value(response["result"]["structuredContent"].clone()).unwrap()
}
/// An inspection asked right after a cancellation: served at once, behind the
/// cancelled call at the worker, never refused because that call had not yet
/// ended when it was asked (a cancelled call holds no capacity).
fn inspect_after_cancel(runner: &mut Running, id: u64) -> ApplicationResult {
    runner.tool(id, "request.inspect", json!({"operation_id":OPERATION_ID}));
    let response = runner.response(id);
    assert!(
        response["result"].get("structuredContent").is_some(),
        "an inspection after a cancellation was refused: {response}"
    );
    assert_eq!(
        response["result"]["isError"], false,
        "inspection is successful even while its saved mutation is pending"
    );
    application(&response)
}

#[test]
fn pending_mutation_keeps_control_responsive_cancel_suppressed_and_restart_retries_exact() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations");
    let mut core = Core::new(context().ledger, CoreLimits::default());
    let mut runner = Running::start(&path, false);
    runner.tool(1, "claim.submit", claim());
    let epoch = runner.observed();
    assert!(matches!(
        epoch.request.operation,
        Operation::OpenEpoch { .. }
    ));
    let receipt = apply(&mut core, &epoch.request);
    answer(epoch, receipt);
    let pending = runner.observed();
    let saved = pending.request.clone();
    let receipt = apply(&mut core, &pending.request);
    assert_eq!(core.sequence(), SessionSeq(2));
    runner.rpc(2, "server/discover", json!({}));
    assert!(
        runner.response(2)["result"]
            .get("supportedVersions")
            .is_some()
    );
    runner.rpc(3, "tools/list", json!({}));
    assert!(
        !runner.response(3)["result"]["tools"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    runner
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}));
    // Asked before the cancelled call has ended: it waits behind it.
    let inspected = inspect_after_cancel(&mut runner, 10);
    assert!(pending.abandoned(), "backend request did not cancel");
    assert_eq!(inspected.condition, "Pending");
    assert_eq!(inspected.operation_id.as_deref(), Some(OPERATION_ID));
    assert!(
        matches!(inspected.result,OperationOutput::Mutation{reply:MutationReply::Pending(key)} if key.id==saved.request_id)
    );
    runner.stop();
    drop(pending);

    let mut restarted = Running::start(&path, true);
    for (rpc_id, forge_hash) in [(90, true), (91, false)] {
        restarted.tool(
            rpc_id,
            "request.inspect",
            json!({"operation_id":OPERATION_ID,"remote":true}),
        );
        let observed = restarted.observed();
        let Operation::Reconcile(query) = observed.request.operation else {
            panic!("inspection must only query")
        };
        assert_eq!(
            query,
            ReconcileQuery::Receipt {
                epoch: saved.request_epoch,
                request: saved.request_id
            }
        );
        let mut page = core
            .reconcile(context().ledger, context().principal, &query)
            .unwrap()
            .to_owned()
            .unwrap();
        if forge_hash {
            let ReconcileResult::Receipt {
                resolution: ReceiptResolution::Committed(receipt),
                ..
            } = &mut page.result
            else {
                panic!("retained receipt")
            };
            receipt.command_hash = ContentHash([42; 32]);
        }
        let reply = ReconcileReply {
            token: ReadToken {
                ledger: page.ledger,
                route_epoch: observed.request.route_epoch,
                sequence: page.sequence,
            },
            applied_index: page.sequence.0 + 1,
            page,
        };
        let response = observed.request.reply(Response::Reconciled(reply));
        observed.response.send(Ok(response)).unwrap();
        let response = restarted.response(rpc_id);
        let result = application(&response);
        assert_eq!(response["result"]["isError"], forge_hash);
        if forge_hash {
            assert!(matches!(result.result, OperationOutput::Error { .. }));
        } else {
            assert!(matches!(result.result, OperationOutput::Reconcile { .. }));
        }
    }
    restarted.tool(92, "request.inspect", json!({"operation_id":OPERATION_ID}));
    assert_eq!(application(&restarted.response(92)).condition, "Pending");
    assert!(restarted.requests.try_recv().is_err());
    restarted.tool(100, "request.retry", json!({"operation_id":OPERATION_ID}));
    let retried = restarted.observed();
    assert_eq!(retried.request, saved);
    let repeated = apply(&mut core, &retried.request);
    assert!(matches!(
        repeated,
        MutationReply::Domain(DomainOutcome::Duplicate(_))
    ));
    answer(retried, repeated);
    let completed = application(&restarted.response(100));
    assert_eq!(completed.condition, "Committed");
    assert_eq!(
        completed.result,
        OperationOutput::Mutation { reply: receipt }
    );
    assert_eq!(core.sequence(), SessionSeq(2));
    restarted.tool(101, "claim.submit", claim());
    let same = application(&restarted.response(101));
    assert_eq!(same, completed);
    assert!(restarted.requests.try_recv().is_err());
    let mut changed = claim();
    changed["description"] = json!("different intent");
    restarted.tool(102, "claim.submit", changed);
    let conflict = application(&restarted.response(102));
    assert!(
        matches!(conflict.result,OperationOutput::Error{ref code,..} if code=="operation_conflict"),
        "{conflict:?}"
    );
    assert!(restarted.requests.try_recv().is_err());
    restarted.stop();
}

#[test]
fn unknown_reply_and_eof_leave_exact_epoch_request_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations");
    let mut runner = Running::start(&path, false);
    runner.tool(1, "claim.submit", claim());
    let first = runner.observed();
    let saved = first.request.clone();
    first.response.send(Err(WireError::Timeout)).unwrap();
    let result = application(&runner.response(1));
    assert_eq!(result.condition, "OutcomeUnknown");
    assert_eq!(result.operation_id.as_deref(), Some(OPERATION_ID));
    runner.stop();
    let mut reopened = Running::start(&path, true);
    reopened.tool(2, "request.retry", json!({"operation_id":OPERATION_ID}));
    let pending = reopened.observed();
    assert_eq!(pending.request, saved);
    // EOF cancels the pending backend call: the shutdown is clean (`stop`
    // refuses the bounded one, which would have waited for it).
    reopened.stop();
    assert!(pending.abandoned());
    let store = OperationStore::open(
        &path,
        StoreLimits::default(),
        focal_client::at_rest::AtRest::clear(),
    )
    .unwrap();
    let journal = store.open_existing(OPERATION_ID, &context()).unwrap();
    assert_eq!(journal.next_request().unwrap(), Some(&saved));
}

/// Calls a client sends together (Codex and Claude Code send a model's tool calls at once) are each
/// answered, in the order they came, the worker running one at a time: none is refused because the
/// one before it still runs. A call cancelled while it waits never runs and is never answered.
#[test]
fn calls_sent_together_wait_their_turn_and_one_cancelled_while_waiting_never_runs() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations");
    let mut core = Core::new(context().ledger, CoreLimits::default());
    let mut runner = Running::start(&path, false);
    // The first holds the worker: its epoch waits on the ledger until this test answers it.
    runner.tool(1, "claim.submit", claim());
    let epoch = runner.observed();
    for id in [2, 3, 4] {
        runner.tool(id, "request.inspect", json!({"operation_id":OPERATION_ID}));
    }
    runner
        .send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3}}));
    // Answered at once, after every frame before it was taken: the three calls wait (a refusal
    // would have been answered first) and the cancellation is in.
    runner.rpc(5, "tools/list", json!({}));
    assert!(
        runner.response(5)["result"]["tools"].is_array(),
        "a waiting call was answered before its turn"
    );
    let receipt = apply(&mut core, &epoch.request);
    answer(epoch, receipt);
    let pending = runner.observed();
    let receipt = apply(&mut core, &pending.request);
    answer(pending, receipt);
    assert_eq!(application(&runner.response(1)).condition, "Committed");
    // Then the waiting calls in their order; the cancelled one is never answered (`response`
    // refuses any other id).
    for id in [2, 4] {
        let response = runner.response(id);
        assert_eq!(response["result"]["isError"], false, "{response}");
        assert_eq!(application(&response).condition, "Committed", "{response}");
    }
    assert!(
        runner.requests.try_recv().is_err(),
        "an inspection reached the ledger"
    );
    runner.stop();
}

/// Output whose first write waits for `gate`: a writer held up, as a busy machine holds one.
struct Gated {
    gate: Option<mpsc::Receiver<()>>,
    inner: UnixStream,
}
impl Write for Gated {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(gate) = self.gate.take() {
            let _ = gate.recv();
        }
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Answers asked for together wait for a writer that is held up, rather than ending the
/// connection. Five parallel skill reads ended a Codex agent's connection on a busy machine, the
/// queue's two places full while the writer waited.
#[test]
fn answers_asked_for_together_wait_for_a_writer_held_up() {
    let temp = tempfile::tempdir().unwrap();
    let (release, gate) = mpsc::channel::<()>();
    let mut runner = Running::start_writing(
        &temp.path().join("operations"),
        false,
        |backend| backend,
        move |inner| Gated {
            gate: Some(gate),
            inner,
        },
    );
    let uri = "skill://focal-claims/references/workflow-contract.md";
    for id in 1..=16u64 {
        runner.rpc(id, "resources/read", json!({"uri": uri}));
    }
    // One answer is held at the gate and two queue behind it, so the fourth finds the queue full.
    // A server that ended the connection there says so here; this one has nothing to say, and how
    // long this looks decides only how surely the old defect would be seen, never whether a good
    // run passes.
    assert!(
        matches!(
            runner.done.recv_timeout(Duration::from_millis(500)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "the connection ended while the output was held up"
    );
    drop(release);
    for id in 1..=16u64 {
        let read = runner.response(id)["result"].clone();
        assert!(
            read["contents"][0]["text"].as_str().unwrap().len() > 20_000,
            "{id}"
        );
    }
    runner.stop();
}

/// The wait for the output is charged to its progress: a queue that drains takes every item in
/// order; one whose writer reports nothing for the bound is refused, typed; a gone writer is an
/// error at once.
#[test]
fn a_full_output_queue_waits_on_the_writers_progress_and_gives_up_without_any() {
    let (sender, receive) = mpsc::sync_channel(1);
    let (report, written) = mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut taken = Vec::new();
        while let Ok(item) = receive.recv() {
            taken.push(item);
            let _ = report.try_send(());
        }
        taken
    });
    for item in 0..64u32 {
        crate::stdio::send_waiting(&sender, &written, FROZEN, item).unwrap();
    }
    drop(sender);
    assert_eq!(reader.join().unwrap(), (0..64).collect::<Vec<u32>>());

    let (sender, _held) = mpsc::sync_channel(1);
    let (_report, written) = mpsc::sync_channel::<()>(1);
    sender.try_send(0u32).unwrap();
    assert!(matches!(
        crate::stdio::send_waiting(&sender, &written, Duration::from_millis(20), 1u32),
        Err(ServeError::Backpressure)
    ));

    let (sender, _held) = mpsc::sync_channel(1);
    let (report, written) = mpsc::sync_channel::<()>(1);
    sender.try_send(0u32).unwrap();
    drop(report);
    assert!(matches!(
        crate::stdio::send_waiting(&sender, &written, FROZEN, 1u32),
        Err(ServeError::Owner)
    ));
}

/// More calls sent together than the protocol's old fixed bound of eight are each answered, in
/// order: a waiting call is charged what it holds, its decoded frame, so the budget bounds the
/// wait rather than a count (`Limits::calls_within`). Codex sent eleven at once.
#[test]
fn more_calls_sent_together_than_eight_are_each_answered_in_order() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations");
    let mut core = Core::new(context().ledger, CoreLimits::default());
    let mut runner = Running::start(&path, false);
    runner.tool(1, "claim.submit", claim());
    let epoch = runner.observed();
    let waiting: Vec<u64> = (2..=24).collect();
    for id in &waiting {
        runner.tool(*id, "request.inspect", json!({"operation_id":OPERATION_ID}));
    }
    runner.rpc(100, "tools/list", json!({}));
    assert!(
        runner.response(100)["result"]["tools"].is_array(),
        "a waiting call was answered before its turn"
    );
    let receipt = apply(&mut core, &epoch.request);
    answer(epoch, receipt);
    let pending = runner.observed();
    let receipt = apply(&mut core, &pending.request);
    answer(pending, receipt);
    assert_eq!(application(&runner.response(1)).condition, "Committed");
    for id in waiting {
        let response = runner.response(id);
        assert_eq!(response["result"]["isError"], false, "{response}");
    }
    runner.stop();
}

#[path = "native_stdio_tests.rs"]
mod native;
#[path = "traversal_backend_tests.rs"]
mod traversal;

#[test]
fn a_withheld_tool_invoked_directly_is_refused_and_never_dispatched() {
    // The authorization boundary (protocol.rs): a tools/call is accepted only
    // when the tool is in this connection's catalogue, which stdio assembles
    // from the backend's capabilities. The default backend has no admin
    // capability, so admin tools are absent from tools/list; invoking one
    // directly must be refused at the protocol layer and never reach the owner —
    // listing is not the security boundary, dispatch membership is.
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations");
    let mut runner = Running::start(&path, false);
    let response = runner.tool_response(1, "cluster.status", json!({}));
    assert_eq!(response["error"]["code"], -32602, "{response}");
    assert_eq!(response["error"]["message"], "Unknown tool", "{response}");
    assert!(
        runner
            .requests
            .recv_timeout(Duration::from_millis(200))
            .is_err(),
        "a withheld tool must not dispatch a request to the owner"
    );
    runner.stop();
}

#[path = "code_stdio_tests.rs"]
mod code;

#[path = "skills_stdio_tests.rs"]
mod skills;
