use super::*;
use crate::{RetryPolicy, TransportFuture};
use focal_model::*;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Mutex,
};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(5),
        operation: Operation::Read(ReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: ReadQuery::Objects(vec![ObjectRef::claim(ledger(), ClaimId::from_u128(10))]),
            max_items: 1,
        }),
    }
}
fn response(status: ClaimStatus, released: bool, sequence: u64) -> Response {
    let content = ClaimContent {
        ledger: ledger(),
        schema: SCHEMA_MAJOR,
        occurrence: OccurrenceId::from_u128(11),
        description: "observed claim".into(),
        relations: [
            Relation {
                kind: RelationKind::Issuer,
                target: RelationTarget::Participant(ParticipantId::from_u128(3)),
            },
            Relation {
                kind: RelationKind::Subject,
                target: RelationTarget::Participant(ParticipantId::from_u128(4)),
            },
            Relation {
                kind: RelationKind::ClaimAction,
                target: RelationTarget::Action(ActionType::Handoff),
            },
        ]
        .into_iter()
        .collect(),
        scopes: BTreeSet::new(),
        requirements: Vec::new(),
        deadline: None,
    };
    let hash = content.content_hash().unwrap();
    Response::Read(ReadPage {
        token: ReadToken {
            ledger: ledger(),
            sequence: SessionSeq(sequence),
            route_epoch: RouteEpoch(1),
        },
        next: None,
        objects: vec![ReadObject::Claim {
            id: ClaimId::from_u128(10),
            value: StoredObject::new(
                content,
                hash,
                ClaimLifecycle {
                    status,
                    revision: ObjectRevision(sequence),
                    created: SessionSeq(1),
                    history: Vec::new(),
                    receipt: None,
                    evidence_set: None,
                    testament: None,
                    local_complete: status == ClaimStatus::Satisfied,
                    released,
                    terminal_witness: None,
                },
            ),
        }],
    })
}
struct Mock {
    responses: Mutex<VecDeque<(Response, Duration)>>,
    seen: Mutex<Vec<(RequestEnvelope, std::time::Instant)>>,
}
impl Mock {
    fn new(responses: Vec<Response>) -> Self {
        Self {
            responses: Mutex::new(responses.into_iter().map(|r| (r, Duration::ZERO)).collect()),
            seen: Mutex::new(Vec::new()),
        }
    }
}
impl ClientTransport for &Mock {
    fn request<'a>(
        &'a self,
        _: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            self.seen
                .lock()
                .unwrap()
                .push((request.clone(), std::time::Instant::now()));
            let (response, delay) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("bounded probe count");
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(request.reply(response))
        })
    }
}
fn client(mock: &Mock) -> Client<&Mock> {
    Client::new(
        mock,
        RetryPolicy {
            max_attempts: 1,
            max_elapsed: Duration::from_secs(30),
            base_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        },
        WireLimits::default(),
        4,
    )
    .unwrap()
}

#[tokio::test]
async fn predicates_use_observed_status_and_release_without_inventing_work() {
    for (until, status, released, expected) in [
        (
            ClaimWaitUntil::Satisfied,
            ClaimStatus::Satisfied,
            false,
            ClaimWaitCondition::Met,
        ),
        (
            ClaimWaitUntil::Satisfied,
            ClaimStatus::Cancelled,
            false,
            ClaimWaitCondition::Unmet,
        ),
        (
            ClaimWaitUntil::Terminal,
            ClaimStatus::Cancelled,
            false,
            ClaimWaitCondition::Met,
        ),
        (
            ClaimWaitUntil::Released,
            ClaimStatus::Cancelled,
            true,
            ClaimWaitCondition::Met,
        ),
        (
            ClaimWaitUntil::Released,
            ClaimStatus::Cancelled,
            false,
            ClaimWaitCondition::Pending,
        ),
        (
            ClaimWaitUntil::Satisfied,
            ClaimStatus::Posted,
            false,
            ClaimWaitCondition::Pending,
        ),
    ] {
        let mock = Mock::new(vec![response(status, released, 10)]);
        let result = client(&mock)
            .claim_wait(request(), until, Duration::from_millis(20))
            .await
            .unwrap();
        assert_eq!(result.condition, expected);
        assert_eq!(result.observation.status, status);
        assert_eq!(result.observation.token.sequence, SessionSeq(10));
        assert_eq!(result.probes, 1);
        assert_eq!(mock.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn probes_are_fresh_paced_and_monotone_and_deadline_keeps_last_observation() {
    let mock = Mock::new(vec![
        response(ClaimStatus::Posted, false, 10),
        response(ClaimStatus::Satisfied, true, 11),
    ]);
    let result = client(&mock)
        .claim_wait(request(), ClaimWaitUntil::Satisfied, Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(result.condition, ClaimWaitCondition::Met);
    assert_eq!(result.probes, 2);
    {
        let seen = mock.seen.lock().unwrap();
        assert_ne!(seen[0].0.request_id, seen[1].0.request_id);
        assert!(seen[1].1.duration_since(seen[0].1) >= Duration::from_secs(1));
        for (request, _) in seen.iter() {
            assert!(matches!(
                &request.operation,
                Operation::Read(ReadRequest {
                    consistency: ReadConsistency::Linearizable,
                    ..
                })
            ));
        }
    }
    let mock = Mock::new(vec![
        response(ClaimStatus::Posted, false, 10),
        response(ClaimStatus::Satisfied, true, 11),
    ]);
    mock.responses.lock().unwrap()[1].1 = Duration::from_secs(5);
    let result = client(&mock)
        .claim_wait(
            request(),
            ClaimWaitUntil::Satisfied,
            Duration::from_millis(1050),
        )
        .await
        .unwrap();
    assert_eq!(result.condition, ClaimWaitCondition::Pending);
    assert_eq!(result.probes, 1);
    assert_eq!(result.observation.token.sequence, SessionSeq(10));
}

#[tokio::test]
async fn later_transport_errors_and_regressing_prefix_are_not_disguised_as_pending() {
    let mock = Mock::new(vec![
        response(ClaimStatus::Posted, false, 10),
        Response::Error(AccessError::Unavailable),
    ]);
    assert!(matches!(
        client(&mock)
            .claim_wait(request(), ClaimWaitUntil::Satisfied, Duration::from_secs(3))
            .await,
        Err(ClaimWaitError::Client(_))
    ));
    let mock = Mock::new(vec![
        response(ClaimStatus::Posted, false, 10),
        response(ClaimStatus::Posted, false, 9),
    ]);
    assert!(matches!(
        client(&mock)
            .claim_wait(request(), ClaimWaitUntil::Satisfied, Duration::from_secs(3))
            .await,
        Err(ClaimWaitError::Client(ClientError::InvalidResponse))
    ));
}

#[tokio::test]
async fn missing_claim_and_cancellation_leave_no_followup_probe() {
    let mut missing = response(ClaimStatus::Posted, false, 10);
    if let Response::Read(page) = &mut missing {
        page.objects.clear();
    }
    let mock = Mock::new(vec![missing]);
    assert!(matches!(
        client(&mock)
            .claim_wait(request(), ClaimWaitUntil::Satisfied, Duration::from_secs(1))
            .await,
        Err(ClaimWaitError::NotFound)
    ));
    let mock = Mock::new(vec![response(ClaimStatus::Posted, false, 10)]);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            client(&mock).claim_wait(
                request(),
                ClaimWaitUntil::Satisfied,
                Duration::from_secs(30)
            )
        )
        .await
        .is_err()
    );
    assert_eq!(mock.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn same_prefix_projection_changes_and_terminal_rewrites_are_rejected() {
    for (first, second, until) in [
        (
            response(ClaimStatus::Posted, false, 10),
            response(ClaimStatus::Satisfied, true, 10),
            ClaimWaitUntil::Satisfied,
        ),
        (
            response(ClaimStatus::Cancelled, false, 10),
            response(ClaimStatus::Expired, true, 11),
            ClaimWaitUntil::Released,
        ),
    ] {
        let mock = Mock::new(vec![first, second]);
        assert!(matches!(
            client(&mock)
                .claim_wait(request(), until, Duration::from_secs(3))
                .await,
            Err(ClaimWaitError::Client(ClientError::InvalidResponse))
        ));
    }
}

#[test]
fn absent_time_driver_and_invalid_input_fail_without_transmitting() {
    let mock = Mock::new(vec![]);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(client(&mock).claim_wait(
            request(),
            ClaimWaitUntil::Satisfied,
            Duration::from_secs(1)
        )),
        Err(ClaimWaitError::Client(ClientError::Transport))
    ));
    for duration in [Duration::ZERO, Duration::from_secs(31)] {
        assert!(matches!(
            runtime.block_on(client(&mock).claim_wait(
                request(),
                ClaimWaitUntil::Satisfied,
                duration
            )),
            Err(ClaimWaitError::InvalidRequest)
        ));
    }
    assert!(mock.seen.lock().unwrap().is_empty());
}
