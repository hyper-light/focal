use super::*;
use crate::{demo, embedded::NodeIdentity};
use focal_consensus::NodeConfig;
use focal_ledger::SessionLimits;

fn identity() -> NodeIdentity {
    NodeIdentity {
        schema: 1,
        cluster: [61; 16],
        node: 1,
        ledger: LedgerId {
            tenant: TenantId::from_u128(61),
            session: SessionId::from_u128(62),
        },
        issuer: ParticipantId::from_u128(63),
        worker: ParticipantId::from_u128(64),
        evaluator: ParticipantId::from_u128(65),
        root: RootCommandId::from_u128(66),
    }
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: identity().issuer,
        tenants: [identity().ledger.tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn claim(id: u128) -> Command {
    let mut claim = demo::claim(&identity(), ClaimId::from_u128(id)).unwrap();
    claim.content.occurrence = OccurrenceId::from_u128(id);
    claim
        .validations
        .retain(|value| value.content.kind == ValidationKind::Receipt);
    for validation in &mut claim.validations {
        validation.id = ValidationId::from_u128(id + 10_000);
    }
    claim.content.requirements = claim
        .validations
        .iter()
        .map(|value| RequirementRef {
            id: value.id,
            specification: value.content.specification_hash().unwrap(),
        })
        .collect();
    Command::GenerateClaim { claim }
}
fn pump_sessions(sessions: &mut [Session]) {
    let mut messages = Vec::new();
    for session in sessions.iter_mut() {
        messages.extend(session.poll().unwrap().messages);
    }
    for message in messages {
        sessions[message.to as usize - 1].step(message).unwrap();
    }
}
struct Fixture {
    _hosts: Vec<ReplicaHost>,
    owners: Vec<Owner>,
    outgoing: Vec<async_mpsc::Receiver<ReplicationFrame>>,
}
impl Fixture {
    fn open(path: &std::path::Path) -> Self {
        let mut sessions: Vec<_> = (1..=3)
            .map(|node| {
                let mut config =
                    NodeConfig::single(node, identity().cluster, identity().ledger.session.0);
                config.voters = vec![1, 2, 3];
                Session::open(
                    path.join(node.to_string()),
                    identity().ledger,
                    config,
                    SessionLimits::default(),
                )
                .unwrap()
            })
            .collect();
        sessions[0].campaign().unwrap();
        for _ in 0..30 {
            pump_sessions(&mut sessions);
        }
        assert!(sessions[0].is_authoritative());
        for (label, command) in [(
            "epoch".to_owned(),
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        )]
        .into_iter()
        .chain((1..=3).map(|id| (format!("claim-{id}"), claim(id))))
        {
            let input = demo::request(&identity(), &label, identity().issuer, command, vec![]);
            assert!(matches!(
                sessions[0].propose(&input).unwrap(),
                Submission::Pending(_)
            ));
            for _ in 0..15 {
                pump_sessions(&mut sessions);
            }
            assert!(
                sessions[0]
                    .receipt(&RequestKey {
                        principal: input.principal,
                        epoch: input.request_epoch,
                        id: input.request_id
                    })
                    .is_some()
            );
        }
        let mut fixture = Self {
            _hosts: vec![],
            owners: vec![],
            outgoing: vec![],
        };
        for session in sessions {
            let budget = MemoryBudget::new(64 * 1024 * 1024, 24 * 1024 * 1024).unwrap();
            let (sender, _receiver) = mpsc::sync_channel(4);
            let (outbound, outgoing) = async_mpsc::channel(128);
            let (host, owner) = ReplicaHost::assemble(
                session,
                ReplicaConfig::new(identity().root),
                ReplicaHost::wire_limits(),
                None,
                budget,
                HostSender::Direct(sender),
                outbound,
            )
            .unwrap();
            fixture._hosts.push(host);
            fixture.owners.push(owner);
            fixture.outgoing.push(outgoing);
        }
        fixture
    }
    fn pump(&mut self) {
        for owner in &mut self.owners {
            owner.drain().unwrap();
        }
        let mut messages = Vec::new();
        for (sender, receiver) in self.outgoing.iter_mut().enumerate() {
            while let Ok(frame) = receiver.try_recv() {
                messages.push((sender as u64 + 1, frame));
            }
        }
        for (sender, frame) in messages {
            let Operation::Raft { message, .. } = &frame.request.operation else {
                panic!("Raft expected")
            };
            self.owners[frame.target as usize - 1]
                .session
                .step_authenticated(sender, message)
                .unwrap();
        }
    }
    fn request(&mut self, node: usize, list: ListRequest) -> oneshot::Receiver<OwnedResponse> {
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: identity().ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(700),
            operation: Operation::List(list),
        };
        let owner = &mut self.owners[node];
        let verified = verify_request(actor(), request, &owner.client_limits).unwrap();
        let charge = owner
            .budget
            .reserve(BudgetKind::Query, BudgetLane::Ordinary, 4 * 1024 * 1024)
            .unwrap()
            .commit();
        let (send, receive) = oneshot::channel();
        owner.request(verified, send, charge, None);
        receive
    }
}
fn list() -> ListRequest {
    ListRequest {
        filter: ListFilter::new(ObjectKind::Claim),
        cursor: None,
        max_items: 1,
        max_visits: 1,
    }
}
fn listed(reply: OwnedResponse) -> ListPage {
    match reply.into_envelope().result {
        Response::Listed(page) => page,
        other => panic!("{other:?}"),
    }
}

#[path = "fleet_reconciliation_owner_tests.rs"]
mod reconciliation_tests;
#[test]
fn quorum_list_requires_fresh_barrier_and_continuations_keep_the_committed_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let mut fixture = Fixture::open(directory.path());
    let mut first = fixture.request(0, list());
    for _ in 0..5 {
        fixture.owners[0].drain().unwrap();
    }
    assert!(
        matches!(first.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
        "a previously authoritative leader cannot answer a new list without a current quorum read"
    );
    for _ in 0..10 {
        fixture.pump();
    }
    let first = listed(first.try_recv().unwrap());
    assert_eq!(first.objects.len(), 1);
    assert!(first.next.is_some());
    let prefix = first.token;
    let input = demo::request(&identity(), "claim-4", identity().issuer, claim(4), vec![]);
    assert!(matches!(
        fixture.owners[0].session.propose(&input).unwrap(),
        Submission::Pending(_)
    ));
    for _ in 0..15 {
        fixture.pump();
    }
    assert!(fixture.owners[0].session.sequence() > prefix.sequence);
    let mut continuation = list();
    continuation.cursor = first.next;
    let mut count = 1;
    while continuation.cursor.is_some() {
        let mut response = fixture.request(0, continuation.clone());
        let page = listed(response.try_recv().unwrap()); // exact pages need no new quorum exchange
        assert_eq!(page.token, prefix);
        assert_eq!(page.objects.len(), 1);
        assert!(!matches!(page.objects[0],ReadObject::Claim{id,..} if id==ClaimId::from_u128(4)));
        continuation.cursor = page.next;
        count += 1;
        assert!(count <= 3);
    }
    assert_eq!(count, 3);
    let mut minority = fixture.request(0, list());
    for _ in 0..3 {
        fixture.owners[0].drain().unwrap();
    }
    assert!(matches!(
        minority.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    fixture.owners[0].pending.front_mut().unwrap().deadline = Instant::now();
    fixture.owners[0].expire_pending();
    assert!(matches!(
        minority.try_recv().unwrap().into_envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    assert!(fixture.owners[0].pending.is_empty());
    let mut follower = fixture.request(1, list());
    assert!(matches!(
        follower.try_recv().unwrap().into_envelope().result,
        Response::Error(AccessError::Unavailable)
    ));
    for owner in &mut fixture.owners {
        owner.close();
    }
}
