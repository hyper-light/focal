use super::*;

#[derive(Clone, Copy)]
enum Step {
    Timeout,
    Allocation,
    Redirect(u64),
    Pending,
    Refuse,
    Inform,
    Committed,
    Duplicate,
    Capacity,
}
struct Script<'a> {
    seen: &'a Mutex<Vec<RequestEnvelope>>,
    steps: Vec<Step>,
}
impl ClientTransport for Script<'_> {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let mut seen = self.seen.lock().unwrap();
            let step = self.steps[seen.len()];
            assert_eq!(
                route.map(|route| route.epoch).unwrap_or(RouteEpoch(1)),
                request.route_epoch
            );
            seen.push(request.clone());
            let result = match step {
                Step::Timeout => return Err(WireError::Timeout),
                Step::Allocation => return Err(WireError::Allocation),
                Step::Redirect(epoch) => Response::Error(AccessError::RouteChanged(RouteHint {
                    epoch: RouteEpoch(epoch),
                    endpoint: "127.0.0.1:7777".into(),
                    server_name: "localhost".into(),
                })),
                Step::Pending => Response::Submitted(MutationReply::Pending(receipt(request).key)),
                Step::Refuse => Response::Submitted(MutationReply::Domain(DomainOutcome::Refuse {
                    code: ErrorCode::WrongActor,
                    detail: "Current admission is refused".into(),
                })),
                Step::Inform => Response::Submitted(MutationReply::Domain(DomainOutcome::Inform {
                    claim: Some(ClaimId::from_u128(5)),
                    reason: InformReason::AlreadyApplied,
                })),
                Step::Committed => Response::Submitted(MutationReply::Committed(receipt(request))),
                Step::Duplicate => Response::Submitted(MutationReply::Domain(
                    DomainOutcome::Duplicate(Box::new(receipt(request))),
                )),
                Step::Capacity => Response::Error(AccessError::Capacity),
            };
            Ok(request.reply(result))
        })
    }
}

#[tokio::test]
async fn later_route_or_domain_rejection_cannot_replace_an_unknown_write_identity() {
    for steps in [
        vec![Step::Timeout, Step::Redirect(2), Step::Redirect(2)],
        vec![Step::Timeout, Step::Redirect(2), Step::Redirect(1)],
        vec![Step::Pending, Step::Refuse],
        vec![Step::Pending, Step::Inform],
        // Decoding can fail after a mutation reached the server. A later
        // refusal cannot prove that the original request was never applied.
        vec![Step::Allocation, Step::Refuse],
    ] {
        let seen = Mutex::new(Vec::new());
        let count = steps.len();
        let client = Client::new(
            Script { seen: &seen, steps },
            policy(),
            WireLimits::default(),
            1,
        )
        .unwrap();
        let original = request();
        let ClientError::OutcomeUnknown { request: retained } =
            client.request(original.clone()).await.unwrap_err()
        else {
            panic!("later rejection erased earlier ambiguity")
        };
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), count);
        assert_eq!(retained.route_epoch, seen.last().unwrap().route_epoch);
        for observed in seen.iter().chain(std::iter::once(retained.as_ref())) {
            let mut normalized = observed.clone();
            normalized.route_epoch = original.route_epoch;
            assert_eq!(
                normalized, original,
                "only routing may change during exact retry"
            );
        }
    }
}

#[tokio::test]
async fn definite_admission_response_and_exact_committed_retry_remain_distinct() {
    for steps in [
        vec![Step::Refuse],
        vec![Step::Inform],
        vec![Step::Timeout, Step::Committed],
        vec![Step::Allocation, Step::Committed],
        vec![Step::Pending, Step::Duplicate],
    ] {
        let seen = Mutex::new(Vec::new());
        let count = steps.len();
        let last = *steps.last().unwrap();
        let client = Client::new(
            Script { seen: &seen, steps },
            policy(),
            WireLimits::default(),
            1,
        )
        .unwrap();
        let original = request();
        let response = client.request(original.clone()).await.unwrap();
        match last {
            Step::Refuse => assert!(matches!(
                response.result,
                Response::Submitted(MutationReply::Domain(DomainOutcome::Refuse { .. }))
            )),
            Step::Inform => assert!(matches!(
                response.result,
                Response::Submitted(MutationReply::Domain(DomainOutcome::Inform { .. }))
            )),
            Step::Committed => assert_eq!(
                response.result,
                Response::Submitted(MutationReply::Committed(receipt(&original)))
            ),
            Step::Duplicate => assert_eq!(
                response.result,
                Response::Submitted(MutationReply::Domain(DomainOutcome::Duplicate(Box::new(
                    receipt(&original)
                ))))
            ),
            _ => panic!("test script"),
        }
        assert_eq!(seen.lock().unwrap().len(), count);
    }
}

#[tokio::test]
async fn capacity_refusals_are_resent_with_backoff_and_reported_as_refusals_not_unknown_outcomes() {
    let quick = RetryPolicy {
        max_attempts: 4,
        max_elapsed: Duration::from_secs(5),
        base_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
    };
    // A full ingress that drains: the same request is sent again unchanged
    // and the committed reply is returned.
    let seen = Mutex::new(Vec::new());
    let client = Client::new(
        Script {
            seen: &seen,
            steps: vec![Step::Capacity, Step::Capacity, Step::Committed],
        },
        quick.clone(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    let original = request();
    let reply = client.request(original.clone()).await.unwrap();
    assert!(matches!(
        reply.result,
        Response::Submitted(MutationReply::Committed(_))
    ));
    {
        let sent = seen.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert!(
            sent.iter()
                .all(|request| request.request_id == original.request_id)
        );
    }
    // A refusal on every attempt is the refusal, never an unknown outcome:
    // nothing was admitted, so nothing is left to reconcile.
    let seen = Mutex::new(Vec::new());
    let client = Client::new(
        Script {
            seen: &seen,
            steps: vec![Step::Capacity; 4],
        },
        quick.clone(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.request(request()).await,
        Err(ClientError::Access(AccessError::Capacity))
    ));
    assert_eq!(seen.lock().unwrap().len(), 4);
    // Once a send was uncertain, later refusals cannot prove non-execution.
    let seen = Mutex::new(Vec::new());
    let client = Client::new(
        Script {
            seen: &seen,
            steps: vec![
                Step::Timeout,
                Step::Capacity,
                Step::Capacity,
                Step::Capacity,
            ],
        },
        quick,
        WireLimits::default(),
        1,
    )
    .unwrap();
    assert!(matches!(
        client.request(request()).await,
        Err(ClientError::OutcomeUnknown { .. })
    ));
}
