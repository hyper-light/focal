//! Client semantics for the native profile against an embedded handler.
use crate::native_store::tests::frame;
use crate::*;
use focal_model::*;
use focal_wire::*;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(1),
        session: SessionId::from_u128(2),
    }
}
fn key() -> RequestKey {
    RequestKey {
        principal: ParticipantId::from_u128(3),
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(99),
    }
}
fn envelope(operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: key().id,
        operation,
    }
}
fn receipt() -> NativeReceipt {
    NativeReceipt {
        invocation: NativeInvocationRef::Request(key()),
        sequence: SessionSeq(3),
        logical_time: 5,
        operation: NativeOperationKind::Post,
        intent: ContentHash([4; 32]),
        counts: NativeOutcomeCounts {
            created: 0,
            changed: 1,
            definitions: 0,
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
fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        max_elapsed: Duration::from_secs(2),
        base_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    }
}
fn peer() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: key().principal,
        tenants: BTreeSet::from([ledger().tenant]),
        role: PeerRole::Actor,
    })
    .unwrap()
}
type Frames = Arc<Mutex<Vec<Vec<u8>>>>;
/// Replies in order, then repeats the last one; records every frame seen.
struct Scripted {
    native: bool,
    replies: Mutex<Vec<NativeMutationReply>>,
    frames: Frames,
}
impl RequestHandler for Scripted {
    fn supports_managed_requests(&self) -> bool {
        true
    }
    fn supports_participant_requests(&self) -> bool {
        true
    }
    fn supports_native_requests(&self) -> bool {
        self.native
    }
    fn handle(&self, request: VerifiedRequest) -> HandlerFuture<'_> {
        Box::pin(async move {
            let envelope = request.request();
            let result = match &envelope.operation {
                Operation::Native { frame } => {
                    match native_frame_admissible(frame, request.peer(), envelope) {
                        Ok(_) => {
                            self.frames.lock().unwrap().push(frame.clone());
                            let mut replies = self.replies.lock().unwrap();
                            let reply = if replies.len() > 1 {
                                replies.remove(0)
                            } else {
                                replies[0].clone()
                            };
                            Response::Native(reply)
                        }
                        Err(error) => Response::Error(error),
                    }
                }
                Operation::NativeRead(read) => Response::NativeRead(NativeReadPage {
                    token: ReadToken {
                        ledger: envelope.ledger,
                        sequence: SessionSeq(2),
                        route_epoch: envelope.route_epoch,
                    },
                    native_sequence: SessionSeq(2),
                    logical_time: 7,
                    objects: vec![NativeObject::Standing(NativeStanding {
                        principal: request.peer().principal(),
                        role: NativePeerRole::Actor,
                        profile: NativeProfile::AuthoredV1,
                        native_sequence: SessionSeq(2),
                        logical_time: 7,
                    })],
                    next: None,
                    visited: read.max_items,
                }),
                _ => Response::Error(AccessError::UnsupportedOperation),
            };
            envelope.reply(result)
        })
    }
}
fn client(
    native: bool,
    replies: Vec<NativeMutationReply>,
) -> (Client<EmbeddedTransport<Scripted>>, Frames) {
    let frames = Arc::new(Mutex::new(Vec::new()));
    let handler = Scripted {
        native,
        replies: Mutex::new(replies),
        frames: frames.clone(),
    };
    let transport = EmbeddedTransport::new(peer(), handler, WireLimits::default()).unwrap();
    (
        Client::new(transport, policy(), WireLimits::default(), 4).unwrap(),
        frames,
    )
}
fn submit() -> RequestEnvelope {
    envelope(Operation::Native {
        frame: frame(ledger(), key(), 23),
    })
}
fn refusal() -> NativeMutationReply {
    NativeMutationReply::Refused(NativeRefusal {
        kind: NativeRefusalKind::Refused(NativeErrorCode::StaleRevision),
        detail: "stale".into(),
    })
}

#[tokio::test]
async fn a_pending_ticket_is_resent_as_the_identical_frame_until_it_commits() {
    let pending = NativeMutationReply::Pending(NativeTicket {
        key: key(),
        intent: ContentHash([4; 32]),
    });
    let (client, frames) = client(
        true,
        vec![pending, NativeMutationReply::Committed(receipt())],
    );
    let reply = client.submit_native(submit()).await.unwrap();
    assert_eq!(reply, NativeMutationReply::Committed(receipt()));
    let frames = frames.lock().unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0], frames[1]);
    assert_eq!(frames[0], frame(ledger(), key(), 23));
}

#[tokio::test]
async fn a_ticket_that_never_commits_is_reported_unknown_with_the_exact_request_retained() {
    let pending = NativeMutationReply::Pending(NativeTicket {
        key: key(),
        intent: ContentHash([4; 32]),
    });
    let (client, frames) = client(true, vec![pending]);
    let original = submit();
    let error = client.submit_native(original.clone()).await.unwrap_err();
    let ClientError::OutcomeUnknown { request } = error else {
        panic!("pending native outcome must stay unknown: {error}")
    };
    assert_eq!(*request, original);
    assert_eq!(frames.lock().unwrap().len(), 3);
    assert_eq!(
        failure::client(&ClientError::OutcomeUnknown { request }).code,
        "outcome_unknown"
    );
}

#[tokio::test]
async fn refusals_are_final_replies_even_after_an_uncertain_attempt() {
    let pending = NativeMutationReply::Pending(NativeTicket {
        key: key(),
        intent: ContentHash([4; 32]),
    });
    let (client, frames) = client(true, vec![pending, refusal()]);
    let reply = client.submit_native(submit()).await.unwrap();
    assert_eq!(reply, refusal());
    assert_eq!(frames.lock().unwrap().len(), 2);
    let failure = failure::native_reply(&reply).unwrap();
    assert_eq!((failure.code, failure.exit_code), ("stale_revision", 5));
    assert_eq!(
        failure::native_reply(&NativeMutationReply::Committed(receipt())),
        None
    );
}

#[tokio::test]
async fn native_reads_return_pages_and_wrong_operations_are_configuration_errors() {
    let (client, _) = client(true, vec![refusal()]);
    let page = client
        .native_read(envelope(Operation::NativeRead(NativeReadRequest {
            consistency: ReadConsistency::Linearizable,
            query: NativeReadQuery::Standing,
            max_items: 1,
        })))
        .await
        .unwrap();
    assert_eq!(page.native_sequence, SessionSeq(2));
    assert!(matches!(page.objects[..], [NativeObject::Standing(_)]));
    assert!(matches!(
        client.native_read(submit()).await,
        Err(ClientError::Configuration)
    ));
    assert!(matches!(
        client.submit_native(envelope(Operation::Summary)).await,
        Err(ClientError::Configuration)
    ));
    assert!(matches!(
        client.native_list(submit()).await,
        Err(ClientError::Configuration)
    ));
}

#[tokio::test]
async fn a_handler_without_the_native_engine_refuses_the_profile_before_any_frame_is_seen() {
    let (client, frames) = client(false, vec![refusal()]);
    let error = client.submit_native(submit()).await.unwrap_err();
    assert!(matches!(
        error,
        ClientError::Access(AccessError::UnsupportedProtocol)
    ));
    assert!(frames.lock().unwrap().is_empty());
}
