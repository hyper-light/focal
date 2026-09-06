use super::*;
use focal_consensus::NodeConfig;
use focal_evidence::{
    Evaluation, Registration, RegistryError, TestReportValidator, Validator, test_report_schema,
};
use focal_ledger::SessionLimits;
use std::{
    collections::BTreeSet,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const WORKER: ParticipantId = ParticipantId::from_u128(2);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
const CLAIM: ClaimId = ClaimId::from_u128(10);
struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
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
fn policy(retry: RetryContract) -> ExecutionPolicy {
    ExecutionPolicy {
        revision: 1,
        timeout_ms: 100,
        max_concurrency: 1,
        retry,
    }
}
fn catalog(handlers: Vec<(HandlerRef, Box<dyn Validator>)>, retry: RetryContract) -> Arc<Catalog> {
    let mut catalog = Catalog::new(8);
    for (handler, implementation) in handlers {
        catalog
            .register(
                Registration {
                    handler,
                    evidence_schema: test_report_schema(),
                    max_evidence_bytes: 4096,
                },
                policy(retry),
                implementation,
            )
            .unwrap();
    }
    Arc::new(catalog)
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
fn submit(session: &mut Session, id: u128, principal: ParticipantId, command: Command) {
    let outcome = session
        .submit_local(&input(id, principal, command))
        .unwrap();
    assert!(matches!(outcome, Submission::Committed(_)), "{outcome:?}");
}
fn open(path: &std::path::Path) -> Session {
    let mut session = Session::open(
        path,
        ledger(),
        NodeConfig::single(1, [1; 16], ledger().session.0),
        SessionLimits::default(),
    )
    .unwrap();
    session.campaign().unwrap();
    for _ in 0..5 {
        session.poll().unwrap();
    }
    session
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
fn fixture(
    handlers: Vec<HandlerRef>,
    quality: Option<String>,
    evidence: Option<&[u8]>,
    deadline: Option<Deadline>,
) -> (tempfile::TempDir, Session) {
    fixture_payload(
        handlers,
        quality,
        evidence.map(|bytes| ArtifactPayload::Inline(bytes.to_vec())),
        deadline,
    )
}
fn fixture_payload(
    handlers: Vec<HandlerRef>,
    quality: Option<String>,
    evidence: Option<ArtifactPayload>,
    deadline: Option<Deadline>,
) -> (tempfile::TempDir, Session) {
    let directory = tempfile::tempdir().unwrap();
    let mut session = open(directory.path());
    populate(
        handlers,
        quality,
        evidence,
        deadline,
        |id, actor, command| submit(&mut session, id, actor, command),
    );
    (directory, session)
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
fn runtime(catalog: Arc<Catalog>, clock: Arc<TestClock>) -> Runtime {
    Runtime::new(
        RuntimeConfig::default(),
        identity(),
        catalog,
        Arc::new(InlineOnly),
        clock,
    )
    .unwrap()
}
fn status(session: &Session) -> ClaimStatus {
    session.read_at_least(SessionSeq(0)).unwrap().claims[&CLAIM]
        .lifecycle()
        .status
}
fn drive_until(
    runtime: &mut Runtime,
    session: &mut Session,
    predicate: impl Fn(&Runtime, &Session) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        runtime.drive_local(session).unwrap();
        if predicate(runtime, session) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "runtime failed to progress at {:?}",
            status(session)
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
const PASS: &[u8] = br#"{"passed":3,"failed":0,"skipped":0}"#;

#[test]
fn real_report_worker_completes_durable_proof_and_replays_without_execution() {
    let (directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let mut runtime = runtime(
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        clock,
    );
    drive_until(&mut runtime, &mut session, |_, s| {
        status(s) == ClaimStatus::Satisfied
    });
    let before = session.read_at_least(SessionSeq(0)).unwrap().clone();
    assert!(
        before
            .artifacts
            .values()
            .any(|a| a.content().kind == "runtime-dispatch")
    );
    let run = before
        .runs
        .values()
        .find(|r| r.id.validation == ValidationId::from_u128(21))
        .unwrap();
    assert_eq!(run.attempts.len(), 1);
    assert_eq!(run.final_verdict, Some(VerdictValue::Pass));
    assert_eq!(run.attempts[0].evidence.len(), 2);
    session.checkpoint().unwrap();
    drop(runtime);
    drop(session);
    let session = open(directory.path());
    assert_eq!(session.read_at_least(SessionSeq(0)).unwrap(), &before);
}
#[test]
fn missing_evidence_becomes_incomplete_with_durable_diagnostic() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, None, None);
    let mut runtime = runtime(
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(1))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::ValidationIncomplete);
    let run = session
        .read_at_least(SessionSeq(0))
        .unwrap()
        .runs
        .values()
        .find(|r| r.id.validation == ValidationId::from_u128(21))
        .unwrap();
    assert_eq!(run.attempts[0].evidence.len(), 1);
}
struct CountingValidator(Arc<AtomicUsize>, VerdictValue);
impl Validator for CountingValidator {
    fn evaluate(&self, _: &[u8], quality: Option<&str>) -> Result<Evaluation, RegistryError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Evaluation {
            value: self.1,
            reason: quality.unwrap_or("counted").into(),
        })
    }
}
#[test]
fn unavailable_policy_falls_back_but_negative_evidence_does_not() {
    let (_directory, mut session) = fixture(
        vec![handler(1, false), handler(2, false)],
        None,
        Some(PASS),
        None,
    );
    let mut runtime = runtime(
        catalog(
            vec![(handler(2, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(1))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Satisfied);
    let state = session.read_at_least(SessionSeq(0)).unwrap();
    let run = state
        .runs
        .values()
        .find(|r| r.id.validation == ValidationId::from_u128(21))
        .unwrap();
    assert_eq!(
        run.attempts.iter().map(|v| v.value).collect::<Vec<_>>(),
        [VerdictValue::Error, VerdictValue::Pass]
    );
    let (_directory, mut session) = fixture(
        vec![handler(1, false), handler(2, false)],
        None,
        Some(br#"{"passed":3,"failed":1,"skipped":0}"#),
        None,
    );
    let count = Arc::new(AtomicUsize::new(0));
    let mut runtime = super::tests::runtime(
        catalog(
            vec![
                (handler(1, false), Box::new(TestReportValidator)),
                (
                    handler(2, false),
                    Box::new(CountingValidator(Arc::clone(&count), VerdictValue::Pass)),
                ),
            ],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(1))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::ValidationFailed);
    assert_eq!(count.load(Ordering::SeqCst), 0);
}
#[test]
fn quality_evaluator_runs_only_after_programmatic_pass() {
    let (_directory, mut session) = fixture(
        vec![handler(1, false), handler(2, true)],
        Some("looks good".into()),
        Some(PASS),
        None,
    );
    let count = Arc::new(AtomicUsize::new(0));
    let mut runtime = runtime(
        catalog(
            vec![
                (handler(1, false), Box::new(TestReportValidator)),
                (
                    handler(2, true),
                    Box::new(CountingValidator(Arc::clone(&count), VerdictValue::Pass)),
                ),
            ],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(1))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(status(&session), ClaimStatus::Satisfied);
}
struct Gate {
    started: AtomicUsize,
    open: Mutex<bool>,
    wake: Condvar,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: AtomicUsize::new(0),
            open: Mutex::new(false),
            wake: Condvar::new(),
        })
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.wake.notify_all();
    }
    fn wait_started(&self) {
        let until = Instant::now() + Duration::from_secs(2);
        while self.started.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
    }
}
impl Executor for Gate {
    fn reconcile(&self, _task: &Task, _cancellation: &Cancellation) -> Reconciliation {
        if self.started.load(Ordering::SeqCst) == 0 {
            Reconciliation::NotStarted
        } else {
            Reconciliation::Indeterminate
        }
    }
    fn execute(&self, _: &Task, _: &Cancellation) -> WorkerOutcome {
        self.started.fetch_add(1, Ordering::SeqCst);
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.wake.wait(open).unwrap();
        }
        WorkerOutcome {
            value: VerdictValue::Pass,
            code: DiagnosticCode::Evaluated,
            reason: "late pass".into(),
        }
    }
}
#[test]
fn timeout_publishes_error_and_retains_worker_capacity_until_late_exit() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let gate = Gate::new();
    let config = RuntimeConfig {
        workers: 1,
        max_inflight: 1,
        ..Default::default()
    };
    let mut runtime = Runtime::with_executor(
        config,
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    clock.0.store(101, Ordering::SeqCst);
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::ValidationErrored);
    assert_eq!(runtime.active_count(), 1);
    gate.release();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 0);
    assert_eq!(status(&session), ClaimStatus::ValidationErrored);
}
#[test]
fn receipt_adoption_discards_old_result_and_uses_a_new_dispatch_fence() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let config = RuntimeConfig {
        workers: 1,
        max_inflight: 1,
        ..Default::default()
    };
    let mut runtime = Runtime::with_executor(
        config,
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        clock,
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    let old = runtime.active.keys().next().copied().unwrap();
    submit(
        &mut session,
        500,
        ISSUER,
        Command::AdoptReceipt {
            claim: CLAIM,
            previous: ReceiptFence {
                receipt: ReceiptId::from_u128(30),
                epoch: 1,
            },
            receipt: ReceiptId::from_u128(31),
            holder: WORKER,
            epoch: 2,
        },
    );
    runtime.drive_local(&mut session).unwrap();
    assert!(runtime.active[&old].completed);
    gate.release();
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(gate.started.load(Ordering::SeqCst), 2);
    assert_eq!(status(&session), ClaimStatus::Satisfied);
}
#[test]
fn restart_reconciles_indeterminate_external_effect_without_blind_execution() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let catalog = catalog(
        vec![(handler(1, false), Box::new(TestReportValidator))],
        RetryContract::Reconcile,
    );
    let mut first = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog.clone(),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    drive_until(&mut first, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    drop(first);
    gate.release();
    let mut restarted = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog,
        gate.clone(),
        clock,
    )
    .unwrap();
    drive_until(&mut restarted, &mut session, |r, _| {
        r.active.values().any(|a| a.indeterminate && !a.running)
    });
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    assert_eq!(status(&session), ClaimStatus::Validating);
    assert!(
        session
            .read_at_least(SessionSeq(0))
            .unwrap()
            .artifacts
            .values()
            .any(|a| a.content().schema_hash
                == ContentHash(*blake3::hash(DIAGNOSTIC_SCHEMA).as_bytes()))
    );
}
#[test]
fn persisted_diagnostic_finishes_after_restart_without_running_handler_again() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let catalog = catalog(
        vec![(handler(1, false), Box::new(TestReportValidator))],
        RetryContract::ReadOnly,
    );
    let mut first = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog.clone(),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    drive_until(&mut first, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    let assignment = *first.active.keys().next().unwrap();
    first
        .persist_diagnostic(
            &mut session,
            assignment,
            WorkerOutcome {
                value: VerdictValue::Pass,
                code: DiagnosticCode::Evaluated,
                reason: "committed before owner crash".into(),
            },
            false,
            &mut DriveReport::default(),
        )
        .unwrap();
    drop(first);
    gate.release();
    let mut restarted = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog,
        gate.clone(),
        clock,
    )
    .unwrap();
    drive_until(&mut restarted, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    assert_eq!(status(&session), ClaimStatus::Satisfied);
}
#[test]
fn due_claim_timer_reconciles_after_restart_and_late_duplicate_is_inert() {
    let (directory, session) = fixture(
        vec![handler(1, false)],
        None,
        Some(PASS),
        Some(Deadline {
            timer: TimerId::from_u128(99),
            generation: 1,
            at: 10,
        }),
    );
    drop(session);
    let mut session = open(directory.path());
    let mut runtime = runtime(
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(10))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Expired);
    let sequence = session.sequence();
    for _ in 0..3 {
        runtime.drive_local(&mut session).unwrap();
    }
    assert_eq!(session.sequence(), sequence);
}

#[test]
fn late_worker_completion_cannot_beat_timeout_between_owner_ticks() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let mut runtime = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    clock.0.store(101, Ordering::SeqCst);
    gate.release();
    // Deliver a completed worker event before the next owner timer check.
    std::thread::sleep(Duration::from_millis(10));
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::ValidationErrored);
}
struct ResolvedEffect {
    calls: AtomicUsize,
}
impl Executor for ResolvedEffect {
    fn execute(&self, _: &Task, _: &Cancellation) -> WorkerOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("completed effect must not execute again")
    }
    fn reconcile(&self, _: &Task, _: &Cancellation) -> Reconciliation {
        Reconciliation::Completed(WorkerOutcome {
            value: VerdictValue::Pass,
            code: DiagnosticCode::Evaluated,
            reason: "external system proves completed effect".into(),
        })
    }
}
#[test]
fn known_reconciled_effect_can_complete_after_original_deadline_without_reexecution() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let catalog = catalog(
        vec![(handler(1, false), Box::new(TestReportValidator))],
        RetryContract::Reconcile,
    );
    let mut first = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog.clone(),
        gate.clone(),
        clock.clone(),
    )
    .unwrap();
    drive_until(&mut first, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    drop(first);
    gate.release();
    clock.0.store(200, Ordering::SeqCst);
    let executor = Arc::new(ResolvedEffect {
        calls: AtomicUsize::new(0),
    });
    let mut restarted = Runtime::with_executor(
        RuntimeConfig::default(),
        identity(),
        catalog,
        executor.clone(),
        clock,
    )
    .unwrap();
    drive_until(&mut restarted, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Satisfied);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn durable_content_chunks_are_read_and_validated_off_owner() {
    let content_dir = tempfile::tempdir().unwrap();
    let mut store = focal_evidence::ContentStore::open(
        content_dir.path(),
        focal_evidence::StoreLimits {
            max_content_bytes: 4096,
            max_staging_bytes: 8192,
            max_uploads: 2,
            chunk_bytes: 8,
            max_manifest_bytes: 4096,
        },
    )
    .unwrap();
    let upload = focal_evidence::UploadId([1; 16]);
    store
        .begin(
            upload,
            ContentDomainId(ledger().tenant.0),
            ContentClass::Evidence,
            PASS.len() as u64,
            Some(ContentHash(*blake3::hash(PASS).as_bytes())),
        )
        .unwrap();
    let mut offset = 0;
    for chunk in PASS.chunks(8) {
        offset = store.append(upload, offset, chunk).unwrap();
    }
    let content = store.seal(upload).unwrap();
    let (_directory, mut session) = fixture_payload(
        vec![handler(1, false)],
        None,
        Some(ArtifactPayload::Content(content)),
        None,
    );
    let reader = SharedStoreReader::new(Arc::new(std::sync::RwLock::new(store)), 8).unwrap();
    let mut runtime = Runtime::new(
        RuntimeConfig::default(),
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(reader),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Satisfied);
}
#[test]
fn evidence_capacity_refuses_before_dispatch_and_does_not_drop_obligation() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let config = RuntimeConfig {
        max_evidence_bytes: 4,
        ..Default::default()
    };
    let mut runtime = Runtime::new(
        config,
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(InlineOnly),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    for _ in 0..4 {
        if matches!(
            runtime.drive_local(&mut session),
            Err(RuntimeError::Capacity)
        ) {
            break;
        }
    }
    assert_eq!(status(&session), ClaimStatus::Validating);
    assert_eq!(runtime.active_count(), 0);
    assert!(
        !session
            .read_at_least(SessionSeq(0))
            .unwrap()
            .artifacts
            .values()
            .any(|a| a.content().kind == "runtime-dispatch")
    );
    assert!(
        session
            .read_at_least(SessionSeq(0))
            .unwrap()
            .runs
            .values()
            .any(|run| run.final_verdict.is_none())
    );
}
#[test]
fn monitor_deadline_is_reconciled_from_durable_definition() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    submit(
        &mut session,
        500,
        ISSUER,
        Command::RegisterMonitor {
            monitor: MonitorId::from_u128(90),
            owner: CLAIM,
            roots: BTreeSet::from([WaitPredicate::Satisfied(CLAIM)]),
            deadline: Deadline {
                timer: TimerId::from_u128(91),
                generation: 1,
                at: 10,
            },
        },
    );
    let mut runtime = runtime(
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        Arc::new(TestClock(AtomicU64::new(10))),
    );
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Deadlocked);
}

#[test]
fn owner_term_change_discards_old_worker_result_before_a_new_dispatch() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let mut runtime = Runtime::with_executor(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    let term = session.status().term;
    let mut heartbeat = focal_consensus::Message::default();
    heartbeat.set_msg_type(focal_consensus::MessageType::MsgHeartbeat);
    heartbeat.from = 2;
    heartbeat.to = 1;
    heartbeat.term = term + 1;
    session.step(heartbeat).unwrap();
    session.poll().unwrap();
    assert!(!session.is_authoritative());
    assert!(matches!(
        runtime.drive_local(&mut session),
        Err(RuntimeError::Ledger(
            focal_ledger::LedgerError::NotReady { .. }
        ))
    ));
    session.campaign().unwrap();
    for _ in 0..5 {
        session.poll().unwrap();
    }
    assert!(session.is_authoritative());
    runtime.drive_local(&mut session).unwrap();
    assert!(runtime.active.values().all(|active| active.completed));
    gate.release();
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(gate.started.load(Ordering::SeqCst), 2);
}
#[test]
fn catalog_rejects_conflicting_or_unbounded_execution_policy() {
    let mut catalog = Catalog::new(2);
    let registration = Registration {
        handler: handler(1, false),
        evidence_schema: test_report_schema(),
        max_evidence_bytes: 100,
    };
    catalog
        .register(
            registration.clone(),
            policy(RetryContract::ReadOnly),
            Box::new(TestReportValidator),
        )
        .unwrap();
    let mut changed = policy(RetryContract::ReadOnly);
    changed.timeout_ms += 1;
    assert!(matches!(
        catalog.register(registration.clone(), changed, Box::new(TestReportValidator)),
        Err(RuntimeError::Registry(RegistryError::Conflict))
    ));
    let mut zero = policy(RetryContract::ReadOnly);
    zero.max_concurrency = 0;
    assert!(matches!(
        catalog.register(registration, zero, Box::new(TestReportValidator)),
        Err(RuntimeError::Configuration(_))
    ));
}
#[test]
fn per_handler_concurrency_remains_bounded_with_spare_worker_threads() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let second = ClaimId::from_u128(110);
    let mut new = claim(vec![handler(1, false)], None, None);
    new.id = second;
    new.content.occurrence = OccurrenceId::from_u128(110);
    for (index, v) in new.validations.iter_mut().enumerate() {
        v.id = ValidationId::from_u128(120 + index as u128);
        v.content.claim = second;
    }
    new.content.requirements = new
        .validations
        .iter()
        .map(|v| RequirementRef {
            id: v.id,
            specification: v.content.specification_hash().unwrap(),
        })
        .collect();
    submit(
        &mut session,
        201,
        ISSUER,
        Command::GenerateClaim { claim: new },
    );
    submit(
        &mut session,
        202,
        ISSUER,
        Command::PostClaim { claim: second },
    );
    let receipt = ReceiptFence {
        receipt: ReceiptId::from_u128(130),
        epoch: 1,
    };
    let set = EvidenceSetId::from_u128(140);
    submit(
        &mut session,
        203,
        WORKER,
        Command::AcquireReceipt {
            claim: second,
            receipt: receipt.receipt,
            epoch: 1,
        },
    );
    submit(
        &mut session,
        204,
        WORKER,
        Command::BeginEvidenceSet {
            claim: second,
            receipt,
            evidence_set: set,
        },
    );
    let artifact = NewArtifact {
        id: ArtifactId::from_u128(150),
        content: ArtifactContent {
            ledger: ledger(),
            schema: 1,
            kind: "test-report".into(),
            schema_hash: test_report_schema(),
            metadata: Vec::new(),
            payload: ArtifactPayload::Inline(PASS.to_vec()),
            producer: WORKER,
            receipt: Some(receipt),
            inputs: BTreeSet::new(),
            visibility: BTreeSet::new(),
        },
    };
    let reference = ArtifactRef {
        id: artifact.id,
        hash: artifact.content.content_hash().unwrap(),
    };
    submit(
        &mut session,
        205,
        WORKER,
        Command::AttachArtifact {
            claim: second,
            receipt,
            evidence_set: set,
            artifact,
        },
    );
    submit(
        &mut session,
        206,
        WORKER,
        Command::CloseTestament {
            claim: second,
            receipt,
            testament: TestamentId::from_u128(160),
            evidence_set: set,
            manifest: vec![reference],
            summary: "second report".into(),
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
        },
    );
    let gate = Gate::new();
    let mut runtime = Runtime::with_executor(
        RuntimeConfig {
            workers: 2,
            max_inflight: 2,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    for _ in 0..5 {
        runtime.drive_local(&mut session).unwrap();
    }
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.active_count(), 1);
    gate.release();
    drive_until(&mut runtime, &mut session, |_, s| {
        s.read_at_least(SessionSeq(0))
            .unwrap()
            .claims
            .values()
            .all(|claim| claim.lifecycle().status.is_terminal())
    });
    assert_eq!(gate.started.load(Ordering::SeqCst), 2);
}

#[test]
fn dropping_owner_transfers_payload_charge_until_blocked_callback_exits() {
    let (_directory, mut session) = fixture(vec![handler(1, false)], None, Some(PASS), None);
    let gate = Gate::new();
    let mut runtime = Runtime::with_executor(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![(handler(1, false), Box::new(TestReportValidator))],
            RetryContract::ReadOnly,
        ),
        gate.clone(),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |r, _| r.active_count() == 1);
    gate.wait_started();
    let budget = runtime.budget.clone();
    let charged = budget.stats().used;
    let before = Instant::now();
    drop(runtime);
    assert!(before.elapsed() < Duration::from_secs(1));
    assert!(
        budget.stats().used > 0,
        "worker still owns its evidence allocation"
    );
    assert!(
        budget.stats().used < charged,
        "owner metadata was released independently"
    );
    gate.release();
    let until = Instant::now() + Duration::from_secs(2);
    while budget.stats().used != 0 {
        assert!(
            Instant::now() < until,
            "detached callback charge did not release"
        );
        std::thread::yield_now();
    }
}

#[test]
fn unwinding_read_only_handler_returns_error_and_worker_runs_fallback() {
    struct PanickingValidator;
    impl Validator for PanickingValidator {
        fn evaluate(&self, _: &[u8], _: Option<&str>) -> Result<Evaluation, RegistryError> {
            panic!("deliberate test-only callback failure");
        }
    }
    let (_directory, mut session) = fixture(
        vec![handler(1, false), handler(2, false)],
        None,
        Some(PASS),
        None,
    );
    let mut runtime = Runtime::new(
        RuntimeConfig {
            workers: 1,
            max_inflight: 1,
            ..Default::default()
        },
        identity(),
        catalog(
            vec![
                (handler(1, false), Box::new(PanickingValidator)),
                (handler(2, false), Box::new(TestReportValidator)),
            ],
            RetryContract::ReadOnly,
        ),
        Arc::new(InlineOnly),
        Arc::new(TestClock(AtomicU64::new(1))),
    )
    .unwrap();
    drive_until(&mut runtime, &mut session, |_, s| status(s).is_terminal());
    assert_eq!(status(&session), ClaimStatus::Satisfied);
    let state = session.read_at_least(SessionSeq(0)).unwrap();
    let run = state
        .runs
        .values()
        .find(|run| run.id.validation == ValidationId::from_u128(21))
        .unwrap();
    assert_eq!(
        run.attempts
            .iter()
            .map(|verdict| verdict.value)
            .collect::<Vec<_>>(),
        [VerdictValue::Error, VerdictValue::Pass]
    );
    assert!(
        state
            .artifacts
            .values()
            .filter(|artifact| artifact.content().kind == "error")
            .any(|artifact| {
                let ArtifactPayload::Inline(bytes) = &artifact.content().payload else {
                    return false;
                };
                postcard::from_bytes::<Diagnostic>(bytes)
                    .is_ok_and(|diagnostic| diagnostic.outcome.code == DiagnosticCode::Panicked)
            })
    );
}

#[path = "quorum_tests.rs"]
mod quorum;

#[test]
fn host_retry_classification_preserves_resource_pressure_and_integrity_failures() {
    use focal_consensus::ConsensusError;
    use focal_graph::GraphError;
    use focal_ledger::LedgerError;
    use focal_memory::MemoryError;
    for error in [
        RuntimeError::Capacity,
        RuntimeError::Memory(MemoryError::Capacity {
            requested: 2,
            available: 1,
        }),
        RuntimeError::Ledger(LedgerError::Memory(MemoryError::AllocationFailed)),
        RuntimeError::Ledger(LedgerError::Graph(GraphError::Memory(
            MemoryError::Capacity {
                requested: 2,
                available: 1,
            },
        ))),
        RuntimeError::Ledger(LedgerError::Consensus(ConsensusError::Capacity)),
        RuntimeError::Ledger(LedgerError::NotReady { leader: 2 }),
        RuntimeError::Domain(DomainOutcome::refuse(
            ErrorCode::StaleReceipt,
            "pending adoption",
        )),
    ] {
        assert!(error.is_retryable(), "{error}");
    }
    for error in [
        RuntimeError::Corrupt,
        RuntimeError::Memory(MemoryError::WrongRange),
        RuntimeError::Ledger(LedgerError::Failed),
        RuntimeError::Ledger(LedgerError::Consensus(ConsensusError::DependencyFailure)),
        RuntimeError::Ledger(LedgerError::Graph(GraphError::IndexMismatch)),
        RuntimeError::Domain(DomainOutcome::refuse(
            ErrorCode::IdempotencyConflict,
            "different accepted input",
        )),
    ] {
        assert!(!error.is_retryable(), "{error}");
    }
}
