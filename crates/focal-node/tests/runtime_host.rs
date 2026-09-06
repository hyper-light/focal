#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! Real threaded replica owners drive runtime stages while the transport moves
//! authenticated Raft messages; no test calls Runtime::drive on their behalf.
use focal_consensus::NodeConfig;
use focal_evidence::{Registration, TestReportValidator, test_report_schema};
use focal_ledger::{Session, SessionLimits, Submission};
use focal_model::*;
use focal_node::fleet::{ReplicaConfig, ReplicaHost, ReplicaOwner};
use focal_runtime::*;
use focal_wire::*;
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const WORKER: ParticipantId = ParticipantId::from_u128(2);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
const CLAIM: ClaimId = ClaimId::from_u128(10);
const PASS: &[u8] = br#"{"passed":3,"failed":0,"skipped":0}"#;
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn identity() -> RuntimeIdentity {
    RuntimeIdentity {
        ledger: ledger(),
        principal: EVALUATOR,
        epoch: RequestEpoch(1),
        root: RootCommandId::from_u128(1),
        policy_revision: 1,
    }
}
fn handler(id: u128, agentic: bool) -> HandlerRef {
    HandlerRef {
        id: ValidatorId::from_u128(id),
        version: ContentHash([id as u8; 32]),
        agentic,
    }
}

fn input(id: u128, principal: ParticipantId, command: Command) -> AuthenticatedInput {
    let evidence = match &command {
        Command::AttachArtifact { artifact, .. } | Command::RegisterArtifact { artifact } => {
            vec![EvidenceAttestation {
                descriptor_hash: artifact.content.content_hash().unwrap(),
                custody_revision: 1,
                durable: true,
                schema_valid: true,
            }]
        }
        _ => Vec::new(),
    };
    AuthenticatedInput {
        ledger: ledger(),
        principal,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(1)),
            policy_revision: 1,
            logical_time: 0,
            evidence,
        },
        command,
    }
}

fn claim(
    handlers: Vec<HandlerRef>,
    quality: Option<String>,
    deadline: Option<Deadline>,
) -> NewClaim {
    let validations = [ValidationKind::Receipt, ValidationKind::Test]
        .into_iter()
        .enumerate()
        .map(|(index, kind)| NewValidation {
            id: ValidationId::from_u128(20 + index as u128),
            content: ValidationContent {
                ledger: ledger(),
                schema: 1,
                claim: CLAIM,
                kind,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                description: "proof".into(),
                quality_bar: if kind == ValidationKind::Receipt {
                    None
                } else {
                    quality.clone()
                },
                evaluator: EVALUATOR,
                handlers: if kind == ValidationKind::Receipt {
                    Vec::new()
                } else {
                    handlers.clone()
                },
                evidence_schemas: if kind == ValidationKind::Receipt {
                    BTreeSet::new()
                } else {
                    BTreeSet::from([test_report_schema()])
                },
                contributed_by: BTreeSet::from([ISSUER]),
                policy_revision: 1,
            },
        })
        .collect::<Vec<_>>();
    NewClaim {
        id: CLAIM,
        content: ClaimContent {
            ledger: ledger(),
            schema: 1,
            occurrence: OccurrenceId::from_u128(10),
            description: "execute the pinned report validator".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ISSUER),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(WORKER),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(1)),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: validations
                .iter()
                .map(|v| RequirementRef {
                    id: v.id,
                    specification: v.content.specification_hash().unwrap(),
                })
                .collect(),
            deadline,
        },
        validations,
    }
}

fn populate(
    handlers: Vec<HandlerRef>,
    quality: Option<String>,
    evidence: Option<ArtifactPayload>,
    deadline: Option<Deadline>,
    mut commit: impl FnMut(u128, ParticipantId, Command),
) {
    for (index, actor) in [ISSUER, WORKER, EVALUATOR].into_iter().enumerate() {
        commit(
            100 + index as u128,
            actor,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
    }
    commit(
        1,
        ISSUER,
        Command::GenerateClaim {
            claim: claim(handlers, quality, deadline),
        },
    );
    commit(2, ISSUER, Command::PostClaim { claim: CLAIM });
    let fence = ReceiptFence {
        receipt: ReceiptId::from_u128(30),
        epoch: 1,
    };
    commit(
        3,
        WORKER,
        Command::AcquireReceipt {
            claim: CLAIM,
            receipt: fence.receipt,
            epoch: 1,
        },
    );
    let set = EvidenceSetId::from_u128(40);
    commit(
        4,
        WORKER,
        Command::BeginEvidenceSet {
            claim: CLAIM,
            receipt: fence,
            evidence_set: set,
        },
    );
    let mut manifest = Vec::new();
    if let Some(payload) = evidence {
        let artifact = NewArtifact {
            id: ArtifactId::from_u128(50),
            content: ArtifactContent {
                ledger: ledger(),
                schema: 1,
                kind: "test-report".into(),
                schema_hash: test_report_schema(),
                metadata: Vec::new(),
                payload,
                producer: WORKER,
                receipt: Some(fence),
                inputs: BTreeSet::new(),
                visibility: BTreeSet::new(),
            },
        };
        manifest.push(ArtifactRef {
            id: artifact.id,
            hash: artifact.content.content_hash().unwrap(),
        });
        commit(
            5,
            WORKER,
            Command::AttachArtifact {
                claim: CLAIM,
                receipt: fence,
                evidence_set: set,
                artifact,
            },
        );
    }
    commit(
        6,
        WORKER,
        Command::CloseTestament {
            claim: CLAIM,
            receipt: fence,
            testament: TestamentId::from_u128(60),
            evidence_set: set,
            manifest,
            summary: "test output".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
        },
    );
}

fn node_config(index: usize) -> NodeConfig {
    let mut config = NodeConfig::single(index as u64 + 1, [11; 16], ledger().session.0);
    config.voters = vec![1, 2, 3];
    config
}
fn open_sessions(root: &Path) -> Vec<Session> {
    (0..3)
        .map(|index| {
            Session::open(
                root.join(index.to_string()),
                ledger(),
                node_config(index),
                SessionLimits::default(),
            )
            .unwrap()
        })
        .collect()
}
fn pump(nodes: &mut [Session]) {
    let mut messages = Vec::new();
    for node in nodes.iter_mut() {
        messages.extend(node.poll().unwrap().messages);
    }
    for message in messages {
        nodes[message.to as usize - 1].step(message).unwrap();
    }
}
fn prepared_sessions(root: &Path) -> Vec<Session> {
    let mut nodes = open_sessions(root);
    nodes[0].campaign().unwrap();
    for _ in 0..30 {
        pump(&mut nodes);
    }
    assert!(nodes[0].is_authoritative());
    populate(
        vec![handler(1, false)],
        None,
        Some(ArtifactPayload::Inline(PASS.to_vec())),
        None,
        |id, actor, command| {
            let input = input(id, actor, command);
            assert!(matches!(
                nodes[0].propose(&input).unwrap(),
                Submission::Pending(_)
            ));
            for _ in 0..15 {
                pump(&mut nodes);
            }
            assert!(
                nodes[0]
                    .receipt(&RequestKey {
                        principal: input.principal,
                        epoch: input.request_epoch,
                        id: input.request_id
                    })
                    .is_some()
            );
        },
    );
    nodes
}
struct FixedClock;
impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        1
    }
}
fn catalog() -> Arc<Catalog> {
    let mut catalog = Catalog::new(1);
    catalog
        .register(
            Registration {
                handler: handler(1, false),
                evidence_schema: test_report_schema(),
                max_evidence_bytes: 4096,
            },
            ExecutionPolicy {
                revision: 1,
                timeout_ms: 60_000,
                max_concurrency: 1,
                retry: RetryContract::ReadOnly,
            },
            Box::new(TestReportValidator),
        )
        .unwrap();
    Arc::new(catalog)
}
struct Counted {
    calls: Arc<AtomicUsize>,
    inner: RegistryExecutor,
}
impl Executor for Counted {
    fn execute(&self, task: &Task, cancellation: &Cancellation) -> WorkerOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(task, cancellation)
    }
}
struct LatePass {
    calls: AtomicUsize,
    cancelled: AtomicUsize,
}
impl Executor for LatePass {
    fn execute(&self, _: &Task, cancellation: &Cancellation) -> WorkerOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let until = Instant::now() + Duration::from_secs(15);
        while !cancellation.is_cancelled() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        if cancellation.is_cancelled() {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
        }
        WorkerOutcome {
            value: VerdictValue::Pass,
            code: DiagnosticCode::Evaluated,
            reason: "late result after authority loss".into(),
        }
    }
}
fn runtime(catalog: Arc<Catalog>, executor: Arc<dyn Executor>, tiny: bool) -> Runtime {
    Runtime::with_executor(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            max_pending_bytes: if tiny { 1 } else { 128 * 1024 },
            ..RuntimeConfig::default()
        },
        identity(),
        catalog,
        executor,
        Arc::new(FixedClock),
    )
    .unwrap()
}
fn peer(node: u64) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(node as u128),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Node { node_id: node },
    })
    .unwrap()
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(999),
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}
struct Fleet {
    hosts: Vec<ReplicaHost>,
    owners: Vec<ReplicaOwner>,
    pumps: Vec<tokio::task::JoinHandle<()>>,
    isolated: Arc<AtomicU8>,
}
impl Fleet {
    fn start(nodes: Vec<Session>, runtimes: Vec<Runtime>) -> Self {
        let mut hosts = Vec::new();
        let mut owners = Vec::new();
        let mut channels = Vec::new();
        for (session, runtime) in nodes.into_iter().zip(runtimes) {
            let mut config = ReplicaConfig::new(RootCommandId::from_u128(1));
            config.tick = Duration::from_millis(20);
            config.request_timeout = Duration::from_millis(500);
            let (host, owner, channel) = ReplicaHost::spawn_with_runtime(
                session,
                config,
                ReplicaHost::wire_limits(),
                Some(runtime),
            )
            .unwrap();
            hosts.push(host);
            owners.push(owner);
            channels.push(channel);
        }
        let isolated = Arc::new(AtomicU8::new(0));
        let pumps = channels
            .into_iter()
            .enumerate()
            .map(|(index, mut channel)| {
                let hosts = hosts.clone();
                let isolated = isolated.clone();
                tokio::spawn(async move {
                    while let Some(frame) = channel.recv().await {
                        let source = index as u8 + 1;
                        let blocked = isolated.load(Ordering::SeqCst);
                        if blocked == source || blocked == frame.target as u8 {
                            continue;
                        }
                        let response = dispatch(
                            &hosts[frame.target as usize - 1],
                            peer(source as u64),
                            frame.request.clone(),
                            &ReplicaHost::wire_limits(),
                        )
                        .await;
                        assert!(
                            matches!(
                                response.result,
                                Response::PeerAccepted
                                    | Response::Error(
                                        AccessError::Unavailable
                                            | AccessError::OutcomeUnknown
                                            | AccessError::Capacity
                                    )
                            ),
                            "{response:?}"
                        );
                    }
                })
            })
            .collect();
        Self {
            hosts,
            owners,
            pumps,
            isolated,
        }
    }
    async fn claim(&self, index: usize) -> Option<(ClaimStatus, SessionSeq)> {
        let response = dispatch(
            &self.hosts[index],
            actor(),
            request(
                9000,
                Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    query: ReadQuery::Objects(vec![ObjectRef {
                        ledger: ledger(),
                        kind: ObjectKind::Claim,
                        id: ObjectId(CLAIM.0),
                    }]),
                    max_items: 1,
                }),
            ),
            &ReplicaHost::wire_limits(),
        )
        .await;
        if let Response::Read(page) = response.result
            && let Some(ReadObject::Claim { value, .. }) = page.objects.first()
        {
            Some((value.lifecycle().status, page.token.sequence))
        } else {
            None
        }
    }
    async fn terminal(&self, excluding: Option<usize>) -> (usize, SessionSeq) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (index, host) in self.hosts.iter().enumerate() {
                    assert!(!host.progress().stopped, "owner stopped unexpectedly");
                    if Some(index) == excluding || host.progress().node != host.progress().leader {
                        continue;
                    }
                    if let Some((ClaimStatus::Satisfied, sequence)) = self.claim(index).await {
                        return (index, sequence);
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("runtime did not finish through replica owner")
    }
    async fn all_at(&self, sequence: SessionSeq) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .hosts
                .iter()
                .any(|host| host.progress().sequence < sequence)
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn stop(mut self) {
        for host in &self.hosts {
            host.stop().await.unwrap();
        }
        for owner in self.owners.drain(..) {
            owner.join().unwrap();
        }
        for pump in self.pumps.drain(..) {
            pump.abort();
            if let Err(error) = pump.await {
                assert!(error.is_cancelled(), "transport task panicked: {error}");
            }
        }
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn threaded_owners_recover_execution_after_leader_loss_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let nodes = prepared_sessions(root.path());
    let catalog = catalog();
    let late = Arc::new(LatePass {
        calls: AtomicUsize::new(0),
        cancelled: AtomicUsize::new(0),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(Counted {
        calls: calls.clone(),
        inner: RegistryExecutor::new(catalog.clone(), Arc::new(InlineOnly)),
    });
    let fleet = Fleet::start(
        nodes,
        vec![
            runtime(catalog.clone(), late.clone(), false),
            runtime(catalog.clone(), executor.clone(), false),
            runtime(catalog.clone(), executor.clone(), false),
        ],
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while late.calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fleet.isolated.store(1, Ordering::SeqCst);
    let (_, sequence) = fleet.terminal(Some(0)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(late.cancelled.load(Ordering::SeqCst), 1);
    fleet.isolated.store(0, Ordering::SeqCst);
    fleet.all_at(sequence).await;
    fleet.stop().await;

    let nodes = open_sessions(root.path());
    assert!(nodes.iter().all(|session| !session.is_authoritative()));
    let runtimes = (0..3)
        .map(|_| runtime(catalog.clone(), executor.clone(), false))
        .collect();
    let restarted = Fleet::start(nodes, runtimes);
    let (_, recovered) = restarted.terminal(None).await;
    assert_eq!(sequence, recovered);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "recovery reran durable completed evidence"
    );
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_capacity_keeps_the_replicated_owner_serving() {
    let root = tempfile::tempdir().unwrap();
    let nodes = prepared_sessions(root.path());
    let before = nodes[0].sequence();
    let catalog = catalog();
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(Counted {
        calls: calls.clone(),
        inner: RegistryExecutor::new(catalog.clone(), Arc::new(InlineOnly)),
    });
    let runtimes = (0..3)
        .map(|_| runtime(catalog.clone(), executor.clone(), true))
        .collect();
    let fleet = Fleet::start(nodes, runtimes);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(fleet.hosts.iter().all(|host| !host.progress().stopped));
    let response = dispatch(
        &fleet.hosts[0],
        actor(),
        request(
            77,
            Operation::OpenEpoch {
                epoch: RequestEpoch(1),
            },
        ),
        &ReplicaHost::wire_limits(),
    )
    .await;
    let Response::Submitted(MutationReply::Committed(receipt)) = response.result else {
        panic!("{response:?}");
    };
    assert_eq!(receipt.sequence, SessionSeq(before.0 + 1));
    fleet.all_at(receipt.sequence).await;
    fleet.stop().await;
}
