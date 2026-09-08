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
