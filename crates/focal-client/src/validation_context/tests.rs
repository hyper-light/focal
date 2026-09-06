use super::*;
use crate::{RetryPolicy, TransportFuture};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Mutex,
};

const VALIDATION: ValidationId = ValidationId::from_u128(20);
const CLAIM: ClaimId = ClaimId::from_u128(10);
const TESTAMENT: TestamentId = TestamentId::from_u128(30);
fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn token() -> ReadToken {
    ReadToken {
        ledger: ledger(),
        sequence: SessionSeq(10),
        route_epoch: RouteEpoch(1),
    }
}
fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(100),
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::ValidationResults {
                id: VALIDATION,
                after: None,
            },
            max_items: 4,
        }),
    }
}
fn stored<C: CanonicalContent, L>(content: C, lifecycle: L) -> StoredObject<C, L> {
    let hash = content.content_hash().unwrap();
    StoredObject::new(content, hash, lifecycle)
}
struct Fixture {
    validation: Validation,
    claim: Claim,
    testament: Testament,
    records: Vec<ValidationResult>,
}
impl Fixture {
    fn new() -> Self {
        let handler = HandlerRef {
            id: ValidatorId::from_u128(50),
            version: ContentHash([3; 32]),
            agentic: false,
        };
        let evaluator = ParticipantId::from_u128(3);
        let validation = stored(
            ValidationContent {
                ledger: ledger(),
                schema: SCHEMA_MAJOR,
                claim: CLAIM,
                kind: ValidationKind::Inspection,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                description: "check exact evidence".into(),
                quality_bar: None,
                evaluator,
                handlers: vec![handler.clone()],
                evidence_schemas: BTreeSet::new(),
                contributed_by: [evaluator].into_iter().collect(),
                policy_revision: 1,
            },
            ValidationLifecycle {
                created: SessionSeq(1),
                latest_epoch: 1,
            },
        );
        let receipt = ReceiptFence {
            receipt: ReceiptId::from_u128(11),
            epoch: 1,
        };
        let evidence_set = EvidenceSetId::from_u128(40);
        let claim = stored(
            ClaimContent {
                ledger: ledger(),
                schema: SCHEMA_MAJOR,
                occurrence: OccurrenceId::from_u128(70),
                description: "work".into(),
                relations: [
                    Relation {
                        kind: RelationKind::Issuer,
                        target: RelationTarget::Participant(evaluator),
                    },
                    Relation {
                        kind: RelationKind::Subject,
                        target: RelationTarget::Participant(ParticipantId::from_u128(4)),
                    },
                ]
                .into_iter()
                .collect(),
                scopes: BTreeSet::new(),
                requirements: vec![RequirementRef {
                    id: VALIDATION,
                    specification: validation.content().specification_hash().unwrap(),
                }],
                deadline: None,
            },
            ClaimLifecycle {
                status: ClaimStatus::Validating,
                revision: ObjectRevision(3),
                created: SessionSeq(1),
                history: vec![
                    StatusFact {
                        status: ClaimStatus::Generated,
                        sequence: SessionSeq(1),
                    },
                    StatusFact {
                        status: ClaimStatus::Validating,
                        sequence: SessionSeq(8),
                    },
                ],
                receipt: Some(Receipt {
                    fence: receipt,
                    holder: ParticipantId::from_u128(4),
                    acquired: SessionSeq(2),
                }),
                evidence_set: Some(evidence_set),
                testament: Some(TESTAMENT),
                local_complete: false,
                released: false,
                terminal_witness: None,
            },
        );
        let artifacts = vec![ArtifactRef {
            id: ArtifactId::from_u128(60),
            hash: ContentHash([7; 32]),
        }];
        let testament = stored(
            TestamentContent {
                ledger: ledger(),
                schema: SCHEMA_MAJOR,
                claim: CLAIM,
                receipt,
                evidence_set,
                artifacts: artifacts.clone(),
                summary: "done".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
            },
            TestamentLifecycle {
                created: SessionSeq(7),
                acknowledged: Some(SessionSeq(8)),
            },
        );
        // Deliberately historical: do not relabel this run as the current
        // closing testament or invent a single artifact target from its schema.
        let run = ValidationRunId {
            validation: VALIDATION,
            target_hash: ContentHash([8; 32]),
            phase: ValidationPhase::WholeWork,
            epoch: 1,
        };
        let records = vec![
            ValidationResult {
                position: ValidationResultPosition { run, attempt: None },
                value: ValidationResultValue::Run(ValidationRunSummary {
                    id: run,
                    claim: CLAIM,
                    evaluator,
                    manifest: ContentHash([9; 32]),
                    handler_index: 0,
                    quality_phase: false,
                    attempt_count: 1,
                    final_verdict: Some(VerdictValue::Pass),
                }),
            },
            ValidationResult {
                position: ValidationResultPosition {
                    run,
                    attempt: Some(0),
                },
                value: ValidationResultValue::Attempt(VerdictRecord {
                    run,
                    evaluator,
                    handler,
                    attempt: 0,
                    manifest: ContentHash([9; 32]),
                    value: VerdictValue::Pass,
                    evidence: artifacts,
                }),
            },
        ];
        Self {
            validation,
            claim,
            testament,
            records,
        }
    }
    fn pages(&self) -> Vec<ReadPage> {
        vec![
            ReadPage {
                token: token(),
                objects: vec![ReadObject::ValidationResults {
                    id: VALIDATION,
                    value: self.validation.clone(),
                    records: self.records.clone(),
                    next: self.records.last().map(|r| r.position),
                }],
                next: None,
            },
            ReadPage {
                token: token(),
                objects: vec![ReadObject::Claim {
                    id: CLAIM,
                    value: self.claim.clone(),
                }],
                next: None,
            },
            ReadPage {
                token: token(),
                objects: vec![ReadObject::Testament {
                    id: TESTAMENT,
                    value: self.testament.clone(),
                }],
                next: None,
            },
        ]
    }
}
struct Mock {
    replies: Mutex<VecDeque<(Response, Duration)>>,
    calls: Mutex<Vec<RequestEnvelope>>,
}
impl Mock {
    fn pages(pages: Vec<ReadPage>) -> Self {
        Self::responses(pages.into_iter().map(Response::Read))
    }
    fn responses(replies: impl IntoIterator<Item = Response>) -> Self {
        Self {
            replies: Mutex::new(replies.into_iter().map(|r| (r, Duration::ZERO)).collect()),
            calls: Mutex::new(vec![]),
        }
    }
    fn calls(&self) -> Vec<RequestEnvelope> {
        self.calls.lock().unwrap().clone()
    }
}
impl ClientTransport for &Mock {
    fn request<'a>(
        &'a self,
        _: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(request.clone());
            let (response, delay) = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("at most three logical read calls");
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(request.reply(response))
        })
    }
}
fn client(mock: &Mock, limits: WireLimits, timeout: Duration) -> Client<&Mock> {
    Client::new(
        mock,
        RetryPolicy {
            max_attempts: 1,
            max_elapsed: timeout,
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
        limits,
        1,
    )
    .unwrap()
}
fn normal(mock: &Mock) -> Client<&Mock> {
    client(mock, WireLimits::default(), Duration::from_secs(1))
}
fn invalid_response(error: ValidationContextError) -> bool {
    matches!(
        error,
        ValidationContextError::Client(ClientError::InvalidResponse)
    )
}

#[tokio::test]
async fn context_pins_all_objects_and_preserves_old_runs_original_manifest_and_cursor() {
    let fixture = Fixture::new();
    let mock = Mock::pages(fixture.pages());
    let context = normal(&mock).validation_context(request()).await.unwrap();
    assert_eq!(context.token, token());
    assert_eq!(context.validation_id, VALIDATION);
    assert_eq!(context.records, fixture.records);
    assert_eq!(context.next, fixture.records.last().map(|r| r.position));
    assert_eq!(context.testament.unwrap().value, fixture.testament);
    let calls = mock.calls();
    assert_eq!(calls.len(), 3);
    let mut identities = BTreeSet::new();
    for (index, request) in calls.iter().enumerate() {
        assert!(!request.operation.is_mutation());
        assert!(identities.insert(request.request_id));
        if index > 0 {
            let Operation::Read(read) = &request.operation else {
                panic!("read")
            };
            assert_eq!(read.consistency, ReadConsistency::Exact(token()));
            assert_eq!(read.max_items, 1);
            assert_eq!(request.route_epoch, token().route_epoch);
        }
    }
    let encoded = serde_json::to_vec(&context.validation).unwrap();
    assert!(!encoded.is_empty());
}

#[tokio::test]
async fn no_closing_testament_uses_only_two_reads() {
    let mut fixture = Fixture::new();
    let mut lifecycle = fixture.claim.lifecycle().clone();
    lifecycle.testament = None;
    fixture.claim = fixture.claim.with_lifecycle(lifecycle);
    let mut pages = fixture.pages();
    pages.pop();
    let mock = Mock::pages(pages);
    let context = normal(&mock).validation_context(request()).await.unwrap();
    assert!(context.testament.is_none());
    assert_eq!(mock.calls().len(), 2);
}

#[tokio::test]
async fn receipt_adoption_keeps_the_original_closing_testament_and_never_rebinds_old_runs() {
    let mut fixture = Fixture::new();
    let mut lifecycle = fixture.claim.lifecycle().clone();
    let receipt = lifecycle.receipt.as_mut().unwrap();
    receipt.fence = ReceiptFence {
        receipt: ReceiptId::from_u128(99),
        epoch: 2,
    };
    receipt.acquired = SessionSeq(9);
    fixture.claim = fixture.claim.with_lifecycle(lifecycle);
    let mock = Mock::pages(fixture.pages());
    let context = normal(&mock).validation_context(request()).await.unwrap();
    assert_ne!(
        context.claim.lifecycle().receipt.as_ref().unwrap().fence,
        context.testament.as_ref().unwrap().value.content().receipt
    );
    assert_eq!(context.records, fixture.records);
}

#[tokio::test]
async fn continuation_preserves_exact_token_and_position_even_when_envelope_route_is_default() {
    let fixture = Fixture::new();
    let mut pages = fixture.pages();
    let after = fixture.records[0].position;
    let ReadObject::ValidationResults { records, .. } = &mut pages[0].objects[0] else {
        panic!("results")
    };
    records.remove(0);
    let exact = ReadToken {
        route_epoch: RouteEpoch(7),
        ..token()
    };
    for page in &mut pages {
        page.token = exact;
    }
    let mut input = request();
    let Operation::Read(read) = &mut input.operation else {
        panic!("read")
    };
    read.consistency = ReadConsistency::Exact(exact);
    read.query = ReadQuery::ValidationResults {
        id: VALIDATION,
        after: Some(after),
    };
    let mock = Mock::pages(pages);
    let context = normal(&mock).validation_context(input).await.unwrap();
    assert_eq!(context.token, exact);
    assert_eq!(context.records, fixture.records[1..]);
    assert!(
        mock.calls()
            .iter()
            .all(|r| r.route_epoch == exact.route_epoch)
    );
    assert!(
        matches!(&mock.calls()[0].operation, Operation::Read(ReadRequest { query: ReadQuery::ValidationResults { after: Some(position), .. }, .. }) if *position == after)
    );
}

#[tokio::test]
async fn nonexistent_requirement_is_distinct_from_a_missing_referenced_parent() {
    let mock = Mock::pages(vec![ReadPage {
        token: token(),
        objects: vec![],
        next: None,
    }]);
    assert!(matches!(
        normal(&mock).validation_context(request()).await,
        Err(ValidationContextError::NotFound)
    ));
    assert_eq!(mock.calls().len(), 1);
    for missing in [1, 2] {
        let mut pages = Fixture::new().pages();
        pages[missing].objects.clear();
        let mock = Mock::pages(pages);
        assert!(invalid_response(
            normal(&mock)
                .validation_context(request())
                .await
                .unwrap_err()
        ));
        assert_eq!(mock.calls().len(), missing + 1);
    }
}

#[tokio::test]
async fn rejects_mismatched_membership_hash_parent_manifest_scope_and_future_lifecycle() {
    for case in 0..10 {
        let mut fixture = Fixture::new();
        match case {
            0 => {
                let mut content = fixture.claim.content().clone();
                content.requirements.clear();
                fixture.claim = stored(content, fixture.claim.lifecycle().clone());
            }
            1 => {
                let mut content = fixture.claim.content().clone();
                content.requirements[0].specification = ContentHash([77; 32]);
                fixture.claim = stored(content, fixture.claim.lifecycle().clone());
            }
            2 => {
                fixture.validation = StoredObject::new(
                    fixture.validation.content().clone(),
                    ContentHash([77; 32]),
                    fixture.validation.lifecycle().clone(),
                );
            }
            3 => {
                fixture.claim = StoredObject::new(
                    fixture.claim.content().clone(),
                    ContentHash([77; 32]),
                    fixture.claim.lifecycle().clone(),
                );
            }
            4 => {
                let mut content = fixture.testament.content().clone();
                content.claim = ClaimId::from_u128(44);
                fixture.testament = stored(content, fixture.testament.lifecycle().clone());
            }
            5 => {
                let mut content = fixture.testament.content().clone();
                content.evidence_set = EvidenceSetId::from_u128(44);
                fixture.testament = stored(content, fixture.testament.lifecycle().clone());
            }
            6 => {
                fixture.testament = StoredObject::new(
                    fixture.testament.content().clone(),
                    ContentHash([77; 32]),
                    fixture.testament.lifecycle().clone(),
                );
            }
            7 => {
                let mut lifecycle = fixture.validation.lifecycle().clone();
                lifecycle.created = SessionSeq(11);
                fixture.validation = fixture.validation.with_lifecycle(lifecycle);
            }
            8 => {
                let mut lifecycle = fixture.testament.lifecycle().clone();
                lifecycle.acknowledged = Some(SessionSeq(11));
                fixture.testament = fixture.testament.with_lifecycle(lifecycle);
            }
            _ => {
                let mut content = fixture.validation.content().clone();
                content.schema = SCHEMA_MAJOR + 1;
                fixture.validation = stored(content, fixture.validation.lifecycle().clone());
            }
        }
        let mock = Mock::pages(fixture.pages());
        assert!(
            invalid_response(
                normal(&mock)
                    .validation_context(request())
                    .await
                    .unwrap_err()
            ),
            "case {case}"
        );
    }
}

#[tokio::test]
async fn transport_route_retry_preserves_the_old_exact_token_and_read_identity() {
    let first = Fixture::new().pages().remove(0);
    let mock = Mock::responses([
        Response::Read(first),
        Response::Error(AccessError::RouteChanged(RouteHint {
            epoch: RouteEpoch(2),
            endpoint: "localhost:9999".into(),
            server_name: "node".into(),
        })),
        Response::Error(AccessError::SnapshotExpired),
    ]);
    let client = Client::new(
        &mock,
        RetryPolicy {
            max_attempts: 2,
            max_elapsed: Duration::from_secs(1),
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.validation_context(request()).await,
        Err(ValidationContextError::Client(ClientError::Access(
            AccessError::SnapshotExpired
        )))
    ));
    let calls = mock.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[1].request_id, calls[2].request_id);
    assert_eq!(calls[2].route_epoch, RouteEpoch(2));
    for call in &calls[1..] {
        assert!(matches!(call.operation, Operation::Read(ReadRequest {
            consistency: ReadConsistency::Exact(saved), ..
        }) if saved == token()));
    }
}

#[tokio::test]
async fn expiry_route_change_and_wrong_prefix_never_restart_at_a_fresh_snapshot() {
    for error in [
        AccessError::SnapshotExpired,
        AccessError::RouteChanged(RouteHint {
            epoch: RouteEpoch(2),
            endpoint: "localhost:9999".into(),
            server_name: "node".into(),
        }),
    ] {
        let first = Fixture::new().pages().remove(0);
        let mock = Mock::responses([Response::Read(first), Response::Error(error)]);
        assert!(normal(&mock).validation_context(request()).await.is_err());
        let calls = mock.calls();
        assert_eq!(calls.len(), 2);
        assert!(matches!(
            calls[1].operation,
            Operation::Read(ReadRequest {
                consistency: ReadConsistency::Exact(_),
                ..
            })
        ));
    }
    let mut pages = Fixture::new().pages();
    pages[1].token.sequence = SessionSeq(11);
    let mock = Mock::pages(pages);
    assert!(invalid_response(
        normal(&mock)
            .validation_context(request())
            .await
            .unwrap_err()
    ));
    assert_eq!(mock.calls().len(), 2);
}

#[tokio::test]
async fn rejects_wrong_query_stale_modes_cursor_without_exact_and_bad_scope_before_io() {
    for case in 0..6 {
        let mut input = request();
        let Operation::Read(read) = &mut input.operation else {
            panic!("read")
        };
        match case {
            0 => read.query = ReadQuery::Objects(vec![]),
            1 => read.consistency = ReadConsistency::StaleProjection,
            2 => read.consistency = ReadConsistency::AtLeast(token()),
            3 => {
                read.query = ReadQuery::ValidationResults {
                    id: VALIDATION,
                    after: Some(Fixture::new().records[0].position),
                }
            }
            4 => read.max_items = 0,
            _ => {
                read.consistency = ReadConsistency::Exact(ReadToken {
                    ledger: LedgerId {
                        tenant: TenantId::from_u128(999),
                        ..ledger()
                    },
                    ..token()
                })
            }
        }
        let mock = Mock::responses([]);
        assert!(
            matches!(
                normal(&mock).validation_context(input).await,
                Err(ValidationContextError::InvalidRequest)
            ),
            "case {case}"
        );
        assert!(mock.calls().is_empty());
    }
}

#[tokio::test]
async fn aggregate_json_bound_rejects_a_combination_even_when_each_page_fits() {
    let fixture = Fixture::new();
    let pages = fixture.pages();
    let largest = pages
        .iter()
        .map(|p| serde_json::to_vec(p).unwrap().len())
        .max()
        .unwrap();
    let total: usize = pages
        .iter()
        .map(|p| serde_json::to_vec(p).unwrap().len())
        .sum();
    assert!(total > largest);
    let limits = WireLimits {
        max_frame_bytes: u32::try_from(largest.max(1024)).unwrap(),
        ..WireLimits::default()
    };
    let mock = Mock::pages(pages);
    assert!(matches!(
        client(&mock, limits, Duration::from_secs(1))
            .validation_context(request())
            .await,
        Err(ValidationContextError::Capacity)
    ));
    assert!(mock.calls().len() <= 3);
}

#[tokio::test]
async fn whole_call_deadline_bounds_three_individually_valid_waits() {
    let mock = Mock::pages(Fixture::new().pages());
    for (_, delay) in mock.replies.lock().unwrap().iter_mut() {
        *delay = Duration::from_millis(25);
    }
    let result = client(&mock, WireLimits::default(), Duration::from_millis(40))
        .validation_context(request())
        .await;
    assert!(matches!(
        result,
        Err(ValidationContextError::Client(ClientError::Transport))
    ));
    assert!(mock.calls().len() <= 2);
}

#[test]
fn missing_time_driver_and_missing_runtime_are_typed_read_failures() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mock = Mock::pages(Fixture::new().pages());
    assert!(matches!(
        runtime.block_on(normal(&mock).validation_context(request())),
        Err(ValidationContextError::Client(ClientError::Transport))
    ));
    assert!(mock.calls().is_empty());
    let client = normal(&mock);
    let mut future = pin!(client.validation_context(request()));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Err(ValidationContextError::Client(ClientError::Transport)))
    ));
}
