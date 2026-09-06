use super::*;
use focal_model::*;
use focal_wire::*;
use std::{sync::Mutex, time::Duration};

fn request() -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(7),
        request_id: RequestId::from_u128(99),
        operation: Operation::Submit {
            expected_revision: None,
            command: Command::RecordProgress {
                claim: ClaimId::from_u128(5),
                receipt: ReceiptFence {
                    receipt: ReceiptId::from_u128(6),
                    epoch: 1,
                },
                message: "private-progress-payload".into(),
            },
        },
    }
}
fn receipt(request: &RequestEnvelope) -> MutationReceipt {
    MutationReceipt {
        ledger: request.ledger,
        key: RequestKey {
            principal: ParticipantId::from_u128(3),
            epoch: request.request_epoch,
            id: request.request_id,
        },
        sequence: SessionSeq(12),
        command_hash: ContentHash([9; 32]),
        outcome: CommandResult::Noop,
    }
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        max_elapsed: Duration::from_secs(1),
        base_backoff: Duration::ZERO,
        max_backoff: Duration::ZERO,
    }
}

struct LostReply {
    requests: Mutex<Vec<RequestEnvelope>>,
    always_fail: bool,
}
impl ClientTransport for LostReply {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            assert_eq!(request, &super::tests::request());
            if self.always_fail || requests.len() == 1 {
                return Err(WireError::Timeout);
            }
            Ok(
                request.reply(Response::Submitted(MutationReply::Committed(receipt(
                    request,
                )))),
            )
        })
    }
}
#[tokio::test]
async fn lost_reply_retries_the_identical_command_and_request_identity() {
    let client = Client::new(
        LostReply {
            requests: Mutex::new(vec![]),
            always_fail: false,
        },
        policy(),
        WireLimits::default(),
        8,
    )
    .unwrap();
    let expected = request();
    let response = client.submit(expected.clone()).await.unwrap();
    assert_eq!(response, MutationReply::Committed(receipt(&expected)));
}
#[tokio::test]
async fn exhausted_retry_retains_exact_request_and_redacts_error_payload() {
    let client = Client::new(
        LostReply {
            requests: Mutex::new(vec![]),
            always_fail: true,
        },
        policy(),
        WireLimits::default(),
        8,
    )
    .unwrap();
    let expected = request();
    let error = client.submit(expected.clone()).await.unwrap_err();
    assert!(!format!("{error:?}").contains("private-progress-payload"));
    let ClientError::OutcomeUnknown { request } = error else {
        panic!("must preserve unknown outcome")
    };
    assert_eq!(*request, expected);
}
struct Redirect {
    seen: Mutex<Vec<(Option<RouteHint>, RequestEnvelope)>>,
}
impl ClientTransport for Redirect {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut seen = self.seen.lock().unwrap();
            seen.push((route.cloned(), request.clone()));
            if route.is_none() {
                Ok(
                    request.reply(Response::Error(AccessError::RouteChanged(RouteHint {
                        epoch: RouteEpoch(2),
                        endpoint: "127.0.0.1:7777".into(),
                        server_name: "localhost".into(),
                    }))),
                )
            } else {
                assert_eq!(request.request_id, RequestId::from_u128(99));
                assert_eq!(request.request_epoch, RequestEpoch(7));
                assert_eq!(request.route_epoch, RouteEpoch(2));
                assert_eq!(request.operation, super::tests::request().operation);
                Ok(
                    request.reply(Response::Submitted(MutationReply::Committed(receipt(
                        request,
                    )))),
                )
            }
        })
    }
}
#[tokio::test]
async fn route_changes_preserve_retry_identity_and_command() {
    let client = Client::new(
        Redirect {
            seen: Mutex::new(vec![]),
        },
        policy(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.submit(request()).await.unwrap(),
        MutationReply::Committed(_)
    ));
}

struct LostAppendReply {
    first: Mutex<Option<RequestEnvelope>>,
    always_fail: bool,
}
impl ClientTransport for LostAppendReply {
    fn request<'a>(
        &'a self,
        _route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut first = self.first.lock().unwrap();
            if let Some(original) = &*first {
                assert_eq!(
                    original, request,
                    "retry must preserve offset and content bytes"
                );
            } else {
                *first = Some(request.clone());
                return Err(WireError::Timeout);
            }
            if self.always_fail {
                return Err(WireError::Timeout);
            }
            let Operation::Upload(UploadRequest::Append { offset, bytes, .. }) = &request.operation
            else {
                panic!("expected append")
            };
            Ok(request.reply(Response::Upload(UploadReply::Offset(
                offset + bytes.len() as u64,
            ))))
        })
    }
}

#[tokio::test]
async fn upload_retry_preserves_offset_bytes_and_unknown_outcome() {
    for always_fail in [false, true] {
        let client = Client::new(
            LostAppendReply {
                first: Mutex::new(None),
                always_fail,
            },
            policy(),
            WireLimits::default(),
            8,
        )
        .unwrap();
        let mut expected = request();
        expected.operation = Operation::Upload(UploadRequest::Append {
            upload: [9; 16],
            offset: 123,
            bytes: b"immutable evidence chunk".to_vec(),
        });
        let result = client.upload(expected.clone()).await;
        if always_fail {
            let ClientError::OutcomeUnknown { request } = result.unwrap_err() else {
                panic!("upload outcome must remain unknown")
            };
            assert_eq!(*request, expected);
        } else {
            assert_eq!(result.unwrap(), UploadReply::Offset(147));
        }
    }
}

#[test]
fn unrepresentable_retry_deadlines_are_rejected_before_scheduling() {
    let policy = RetryPolicy {
        max_elapsed: std::time::Duration::MAX,
        ..Default::default()
    };
    struct Unused;
    impl ClientTransport for Unused {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            _: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async { Err(focal_wire::WireError::Connection) })
        }
    }
    assert!(matches!(
        Client::new(Unused, policy, WireLimits::default(), 1),
        Err(ClientError::Configuration)
    ));
    use std::future::Future;
    let client = Client::new(Unused, RetryPolicy::default(), WireLimits::default(), 1).unwrap();
    let future = client.request(request());
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(ClientError::Transport))
    ));
}
