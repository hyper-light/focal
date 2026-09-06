use super::*;
use crate::{
    RetryPolicy, TransportFuture,
    input::{BuildContext, InputError},
    operations::{ClaimSelector, PlannedOperation, parse_json},
};
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
fn token() -> ReadToken {
    ReadToken {
        ledger: ledger(),
        sequence: SessionSeq(10),
        route_epoch: RouteEpoch(1),
    }
}
fn object(id: u128) -> ReadObject {
    let content = ClaimContent {
        ledger: ledger(),
        schema: SCHEMA_MAJOR,
        occurrence: OccurrenceId::from_u128(id),
        description: "selection".into(),
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
    ReadObject::Claim {
        id: ClaimId::from_u128(id),
        value: StoredObject::new(
            content,
            hash,
            ClaimLifecycle {
                status: ClaimStatus::Generated,
                revision: ObjectRevision(1),
                created: SessionSeq(1),
                history: vec![],
                receipt: None,
                evidence_set: None,
                testament: None,
                local_complete: false,
                released: false,
                terminal_witness: None,
            },
        ),
    }
}
fn cursor(n: u8) -> ListCursor {
    ListCursor { bytes: vec![n] }
}
fn page(objects: Vec<ReadObject>, next: Option<u8>) -> Response {
    Response::Listed(ListPage {
        token: token(),
        objects,
        next: next.map(cursor),
        visited: 1,
    })
}
fn request() -> RequestEnvelope {
    let mut filter = ListFilter::new(ObjectKind::Claim);
    filter.source = Some(ParticipantId::from_u128(3));
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(70),
        operation: Operation::List(ListRequest {
            filter,
            cursor: None,
            max_items: 2,
            max_visits: 1,
        }),
    }
}
struct Mock {
    replies: Mutex<VecDeque<Response>>,
    seen: Mutex<Vec<RequestEnvelope>>,
    delay: Duration,
}
impl Mock {
    fn new(replies: Vec<Response>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            seen: Mutex::new(Vec::new()),
            delay: Duration::ZERO,
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
            self.seen.lock().unwrap().push(request.clone());
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(request.reply(
                self.replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("bounded exact requests"),
            ))
        })
    }
}
fn client(mock: &Mock) -> Client<&Mock> {
    Client::new(mock, RetryPolicy::default(), WireLimits::default(), 4).unwrap()
}
#[tokio::test]
async fn filtered_selection_preserves_empty_page_progress_and_proves_unique_end() {
    let mock = Mock::new(vec![
        page(vec![], Some(1)),
        page(vec![object(10)], Some(2)),
        page(vec![], None),
    ]);
    let found = client(&mock).claim_get(request()).await.unwrap();
    assert_eq!(found.objects, vec![object(10)]);
    assert_eq!(found.token, token());
    assert!(found.next.is_none());
    let seen = mock.seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    for (index, envelope) in seen.iter().enumerate() {
        let Operation::List(list) = &envelope.operation else {
            panic!("list")
        };
        assert_eq!(
            list.cursor,
            if index == 0 {
                None
            } else {
                Some(cursor(index as u8))
            }
        );
        assert_eq!(list.filter.source, Some(ParticipantId::from_u128(3)));
        assert_eq!(envelope.route_epoch, token().route_epoch);
    }
    assert_ne!(seen[0].request_id, seen[1].request_id);
    assert_ne!(seen[1].request_id, seen[2].request_id);
}
#[tokio::test]
async fn absent_ambiguous_expired_and_exhausted_are_distinct_and_never_restart() {
    let mock = Mock::new(vec![page(vec![], None)]);
    assert!(matches!(
        client(&mock).claim_get(request()).await,
        Err(ClaimGetError::NotFound)
    ));
    let mock = Mock::new(vec![
        page(vec![object(10)], Some(1)),
        page(vec![object(11)], None),
    ]);
    assert!(matches!(
        client(&mock).claim_get(request()).await,
        Err(ClaimGetError::Ambiguous)
    ));
    let mock = Mock::new(vec![
        page(vec![object(10)], Some(1)),
        Response::Error(AccessError::SnapshotExpired),
    ]);
    assert!(matches!(
        client(&mock).claim_get(request()).await,
        Err(ClaimGetError::Client(ClientError::Access(
            AccessError::SnapshotExpired
        )))
    ));
    assert_eq!(mock.seen.lock().unwrap().len(), 2);
    let mock = Mock::new((1..=64).map(|n| page(vec![], Some(n))).collect());
    assert!(matches!(
        client(&mock).claim_get(request()).await,
        Err(ClaimGetError::Incomplete)
    ));
    assert_eq!(mock.seen.lock().unwrap().len(), 64);
}
#[tokio::test]
async fn inconsistent_prefix_cursor_scope_and_filters_are_invalid() {
    for mode in 0..4 {
        let mut second = page(vec![], Some(1));
        if let Response::Listed(page) = &mut second {
            match mode {
                0 => {
                    page.token.sequence = SessionSeq(11);
                    page.next = None;
                }
                1 => {}
                2 => {
                    page.objects.push(object(12));
                    if let ReadObject::Claim { value, .. } = &mut page.objects[0] {
                        let mut content = value.content().clone();
                        content.ledger.session = SessionId::from_u128(9);
                        *value = StoredObject::new(
                            content.clone(),
                            content.content_hash().unwrap(),
                            value.lifecycle().clone(),
                        );
                    }
                }
                _ => {
                    page.objects.push(object(12));
                    if let ReadObject::Claim { value, .. } = &mut page.objects[0] {
                        let mut content = value.content().clone();
                        content.relations.clear();
                        *value = StoredObject::new(
                            content.clone(),
                            content.content_hash().unwrap(),
                            value.lifecycle().clone(),
                        );
                    }
                }
            }
        }
        let mock = Mock::new(vec![page(vec![], Some(1)), second]);
        assert!(
            matches!(
                client(&mock).claim_get(request()).await,
                Err(ClaimGetError::Client(ClientError::InvalidResponse))
            ),
            "{mode}"
        );
    }
}
#[tokio::test]
async fn exact_read_preserves_saved_route_and_whole_call_deadline_is_bounded() {
    let mut saved = token();
    saved.route_epoch = RouteEpoch(3);
    let mock = Mock::new(vec![Response::Read(ReadPage {
        token: saved,
        objects: vec![object(10)],
        next: None,
    })]);
    let mut input = request();
    input.operation = Operation::Read(ReadRequest {
        consistency: ReadConsistency::Exact(saved),
        query: ReadQuery::Objects(vec![ObjectRef::claim(ledger(), ClaimId::from_u128(10))]),
        max_items: 1,
    });
    assert_eq!(client(&mock).claim_get(input).await.unwrap().token, saved);
    assert_eq!(mock.seen.lock().unwrap()[0].route_epoch, RouteEpoch(3));
    let mut mock = Mock::new(vec![page(vec![], Some(1)), page(vec![], Some(2))]);
    mock.delay = Duration::from_millis(25);
    let policy = RetryPolicy {
        max_elapsed: Duration::from_millis(40),
        max_backoff: Duration::from_millis(20),
        ..RetryPolicy::default()
    };
    let client = Client::new(&mock, policy, WireLimits::default(), 4).unwrap();
    assert!(matches!(
        client.claim_get(request()).await,
        Err(ClaimGetError::Incomplete)
    ));
    assert_eq!(mock.seen.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn invalid_selectors_do_not_reach_transport() {
    for mode in 0..4 {
        let mock = Mock::new(vec![]);
        let mut input = request();
        let Operation::List(list) = &mut input.operation else {
            panic!()
        };
        match mode {
            0 => list.filter = ListFilter::new(ObjectKind::Claim),
            1 => list.cursor = Some(cursor(1)),
            2 => list.max_items = 1,
            _ => list.filter.kind = ObjectKind::Artifact,
        };
        assert!(matches!(
            client(&mock).claim_get(input).await,
            Err(ClaimGetError::InvalidRequest)
        ));
        assert!(mock.seen.lock().unwrap().is_empty());
    }
}
#[test]
fn authored_exact_compatibility_and_filter_scope_are_checked_without_id_generation() {
    let context = BuildContext {
        ledger: ledger(),
        actor: ParticipantId::from_u128(3),
        root: RootCommandId::from_u128(5),
        policy_revision: 1,
    };
    let build = |value: serde_json::Value| {
        parse_json("claim.get", &serde_json::to_vec(&value).unwrap())
            .unwrap()
            .build(&context, &mut || Err(InputError::Identity))
    };
    let PlannedOperation::ClaimGet(ClaimSelector::Filter(list)) =
        build(serde_json::json!({"source":"self","target":format!("{:032x}",4),"max_visits":1}))
            .unwrap()
    else {
        panic!()
    };
    assert_eq!(list.max_visits, 1);
    assert_eq!(list.filter.source, Some(context.actor));
    let PlannedOperation::ClaimGet(ClaimSelector::Exact(read))=build(serde_json::json!({"id":format!("{:032x}",10),"after":null,"prefix":{"sequence":9,"route_epoch":3},"limit":1})).unwrap()else{panic!()};
    assert_eq!(read.max_items, 1);
    assert!(matches!(read.consistency, ReadConsistency::Exact(_)));
    for value in [
        serde_json::json!({}),
        serde_json::json!({"id":format!("{:032x}",10),"source":"self"}),
        serde_json::json!({"source":"self","limit":1}),
        serde_json::json!({"source":"self","prefix":{"sequence":9,"route_epoch":1}}),
    ] {
        assert!(build(value).is_err());
    }
    assert!(parse_json("claim.get", br#"{"source":"self","producer":"self"}"#).is_err());
}

#[test]
fn missing_time_driver_is_contained_before_transport() {
    let mock = Mock::new(Vec::new());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(client(&mock).claim_get(request())),
        Err(ClaimGetError::Client(ClientError::Transport))
    ));
    assert!(mock.seen.lock().unwrap().is_empty());
}
