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
use tokio::sync::oneshot;

const OPERATION_ID: &str = "000000000000000000000000000000ab";
struct Observed {
    request: RequestEnvelope,
    response: oneshot::Sender<Result<ResponseEnvelope, WireError>>,
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
            let (response, receive) = oneshot::channel();
            self.requests
                .send(Observed {
                    request: request.clone(),
                    response,
                })
                .map_err(|_| WireError::Connection)?;
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
        let store = if reopen {
            OperationStore::open(path, StoreLimits::default())
        } else {
            OperationStore::create(path, StoreLimits::default())
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
        let backend = Backend::new(client, build(), context(), store).unwrap();
        let (client, server) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let reader = server.try_clone().unwrap();
        let (finish, done) = mpsc::sync_channel(1);
        let join = thread::spawn(move || {
            let _ = finish.send(serve(backend, reader, server));
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
        self.requests.recv_timeout(Duration::from_secs(5)).unwrap()
    }
    fn stop(mut self) {
        self.input.shutdown(Shutdown::Write).unwrap();
        let result = self
            .done
            .recv_timeout(Duration::from_secs(5))
            .expect("EOF did not finish within bound");
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
fn inspect_when_retired(runner: &mut Running, start: u64) -> ApplicationResult {
    for offset in 0..40 {
        let id = start + offset;
        runner.tool(id, "request.inspect", json!({"operation_id":OPERATION_ID}));
        let response = runner.response(id);
        if response["result"].get("structuredContent").is_some() {
            assert_eq!(
                response["result"]["isError"], false,
                "inspection is successful even while its saved mutation is pending"
            );
            return application(&response);
        }
        thread::sleep(Duration::from_millis(5));
    }
    panic!("cancelled backend did not release active protocol slot")
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
    let deadline = Instant::now() + Duration::from_secs(2);
    while !pending.response.is_closed() {
        assert!(Instant::now() < deadline, "backend request did not cancel");
        thread::sleep(Duration::from_millis(2));
    }
    let inspected = inspect_when_retired(&mut runner, 10);
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
    let start = Instant::now();
    reopened.stop();
    assert!(start.elapsed() < Duration::from_secs(4));
    assert!(pending.response.is_closed());
    let store = OperationStore::open(&path, StoreLimits::default()).unwrap();
    let journal = store.open_existing(OPERATION_ID, &context()).unwrap();
    assert_eq!(journal.next_request().unwrap(), Some(&saved));
}
