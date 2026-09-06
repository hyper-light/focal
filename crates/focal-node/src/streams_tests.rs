use crate::{config::Settings, embedded::EmbeddedNode, reads::ReadViews, streams::Streams};
use focal_model::*;
use focal_stream::*;
use focal_wire::*;
use std::collections::BTreeSet;

fn open(root: &std::path::Path) -> EmbeddedNode {
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.into());
    EmbeddedNode::open(&settings).unwrap()
}
fn peer(node: &EmbeddedNode) -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: node.identity.issuer,
        tenants: BTreeSet::from([node.identity.ledger.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap()
}
fn invoke(
    node: &mut EmbeddedNode,
    views: &mut ReadViews,
    streams: &mut Streams,
    id: u128,
    operation: StreamRequest,
) -> Result<StreamReply, AccessError> {
    let peer = peer(node);
    let limits = WireLimits::default();
    let request = RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: node.identity.ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation: Operation::Stream(operation.clone()),
    };
    verify_request(peer.clone(), request.clone(), &limits)?;
    let reply = streams.handle(
        &mut node.session,
        views,
        &peer,
        &request,
        &operation,
        &limits,
    )?;
    validate_response(
        &request,
        &request.reply(Response::Stream(reply.clone())),
        Some(peer.principal()),
        &limits,
    )
    .unwrap();
    assert!(
        encode_payload(
            &request.reply(Response::Stream(reply.clone())),
            limits.max_frame_bytes
        )
        .is_ok()
    );
    Ok(reply)
}
fn register(seed: bool) -> StreamRequest {
    StreamRequest::Open {
        consumer: ConsumerId::from_u128(100),
        filter: DeltaFilter::All,
        start: None,
        seed,
        credits: Credits {
            items: 1,
            bytes: 64 * 1024,
        },
    }
}
#[test]
fn delivery_replays_after_restart_but_only_committed_ack_releases_progress() {
    let root = tempfile::tempdir().unwrap();
    let mut node = open(root.path());
    let report = crate::demo::run(&mut node).unwrap();
    let mut views = ReadViews::new();
    let mut streams = Streams::new().unwrap();
    let first = invoke(&mut node, &mut views, &mut streams, 1, register(false)).unwrap();
    assert_eq!(first.acknowledged.position.sequence, SessionSeq(0));
    assert!(first.cursor.position > first.acknowledged.position);
    assert_eq!(first.events.len(), 1);
    drop(node); // no application checkpoint; reopen via actual WAL recovery
    let mut node = open(root.path());
    let mut views = ReadViews::new();
    let replay = invoke(&mut node, &mut views, &mut streams, 1, register(false)).unwrap();
    assert_eq!(first, replay);
    let poll = StreamRequest::Poll {
        cursor: replay.cursor,
        filter: DeltaFilter::All,
        acknowledged: Some(replay.cursor),
        credits: Credits {
            items: 1,
            bytes: 64 * 1024,
        },
    };
    let acked = invoke(&mut node, &mut views, &mut streams, 2, poll.clone()).unwrap();
    assert_eq!(acked.acknowledged, replay.cursor);
    assert!(acked.cursor.position > acked.acknowledged.position);
    let revision = node.session.cursor_revision();
    node.checkpoint().unwrap();
    drop(node);
    let mut node = open(root.path());
    let mut views = ReadViews::new();
    assert_eq!(
        invoke(&mut node, &mut views, &mut streams, 2, poll).unwrap(),
        acked
    );
    assert_eq!(node.session.cursor_revision(), revision);
    assert_eq!(node.session.sequence(), report.sequence);
}

#[test]
fn seed_pages_keep_the_captured_prefix_while_new_writes_wait_in_the_tail() {
    let root = tempfile::tempdir().unwrap();
    let mut node = open(root.path());
    let report = crate::demo::run(&mut node).unwrap();
    let mut views = ReadViews::new();
    let mut streams = Streams::new().unwrap();
    let first = invoke(&mut node, &mut views, &mut streams, 10, register(true)).unwrap();
    let page = first.seed.unwrap();
    assert!(first.events.is_empty());
    assert_eq!(page.token.sequence, report.sequence);
    assert!(page.next.is_some());
    let mut claim = crate::demo::claim(&node.identity, ClaimId::from_u128(999)).unwrap();
    claim.content.description = "A new claim after the seed prefix".into();
    for (index, validation) in claim.validations.iter_mut().enumerate() {
        validation.id = ValidationId::from_u128(5000 + index as u128);
        claim.content.requirements[index].id = validation.id;
    }
    let command = crate::demo::request(
        &node.identity,
        "seed-concurrent-write",
        node.identity.issuer,
        Command::GenerateClaim { claim },
        vec![],
    );
    let outcome = node.session.submit_local(&command).unwrap();
    assert!(
        matches!(outcome, focal_ledger::Submission::Committed(_)),
        "{outcome:?}"
    );
    let tail_sequence = node.session.sequence();
    let next = views
        .read(
            &mut node.session,
            node.identity.issuer,
            &ReadRequest {
                consistency: ReadConsistency::Exact(page.token),
                query: ReadQuery::Scan { after: page.next },
                max_items: 10,
            },
            RequestId::from_u128(11),
            &WireLimits::default(),
        )
        .unwrap();
    assert_eq!(next.token, page.token);
    assert_eq!(next.objects.len(), 4);
    assert_eq!(next.next, None);
    invoke(
        &mut node,
        &mut views,
        &mut streams,
        12,
        StreamRequest::CompleteSeed {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            snapshot: page.token.sequence,
        },
    )
    .unwrap();
    let tail = invoke(
        &mut node,
        &mut views,
        &mut streams,
        13,
        StreamRequest::Poll {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            acknowledged: None,
            credits: Credits {
                items: 10,
                bytes: 64 * 1024,
            },
        },
    )
    .unwrap();
    assert!(tail.events.iter().any(|event| matches!(event, StreamEvent::Delta { delta, .. } if delta.id.sequence == tail_sequence)));
    assert_eq!(
        tail.cursor.position,
        Position::resolved(node.identity.ledger, tail_sequence)
    );
    assert_eq!(tail.acknowledged.position.sequence, page.token.sequence);
}

#[test]
fn new_seed_generation_fences_old_cursor_and_request_key_cannot_change_intent() {
    let root = tempfile::tempdir().unwrap();
    let mut node = open(root.path());
    crate::demo::run(&mut node).unwrap();
    let mut views = ReadViews::new();
    let mut streams = Streams::new().unwrap();
    let first = invoke(&mut node, &mut views, &mut streams, 20, register(true)).unwrap();
    let newer = invoke(&mut node, &mut views, &mut streams, 21, register(true)).unwrap();
    assert_eq!(newer.cursor.generation, first.cursor.generation + 1);
    assert!(matches!(
        invoke(
            &mut node,
            &mut views,
            &mut streams,
            22,
            StreamRequest::CompleteSeed {
                cursor: first.cursor,
                filter: DeltaFilter::All,
                snapshot: first.cursor.position.sequence,
            }
        ),
        Err(AccessError::ResyncRequired { .. })
    ));
    assert_eq!(
        invoke(&mut node, &mut views, &mut streams, 21, register(false)),
        Err(AccessError::InvalidRequest)
    );
    assert_eq!(node.session.cursor_revision(), 2);
}

#[tokio::test]
async fn disconnected_projection_expires_without_another_client_request() {
    let root = tempfile::tempdir().unwrap();
    let mut node = open(root.path());
    crate::demo::run(&mut node).unwrap();
    let sequence = node.session.sequence();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let expiry = now + 100;
    let input = focal_ledger::CursorInput {
        ledger: node.identity.ledger,
        key: RequestKey {
            principal: node.identity.issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(31),
        },
        intent_hash: ContentHash([31; 32]),
        command: CursorCommand {
            expected_revision: 0,
            now,
            operation: CursorOperation::Register {
                consumer: ConsumerId::from_u128(31),
                scope: ContentHash([32; 32]),
                filter: DeltaFilter::All,
                start: Position::resolved(node.identity.ledger, sequence),
                expires_at: expiry,
            },
        },
    };
    node.session.submit_cursor_local(&input).unwrap();
    let (host, owner) = crate::host::LocalHost::spawn(node, WireLimits::default()).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    host.stop().await.unwrap();
    owner.join().unwrap();
    let node = open(root.path());
    assert!(node.session.cursor_clock() >= expiry);
    assert_eq!(node.session.cursor_revision(), 2);
    assert_eq!(node.session.next_cursor_expiry(), None);
    assert_eq!(node.session.sequence(), sequence);
}

struct StreamCluster {
    root: tempfile::TempDir,
    identity: crate::embedded::NodeIdentity,
    sessions: Vec<focal_ledger::Session>,
}
impl StreamCluster {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let identity = crate::embedded::NodeIdentity {
            schema: 1,
            cluster: [71; 16],
            node: 1,
            ledger: LedgerId {
                tenant: TenantId::from_u128(71),
                session: SessionId::from_u128(72),
            },
            issuer: ParticipantId::from_u128(73),
            worker: ParticipantId::from_u128(74),
            evaluator: ParticipantId::from_u128(75),
            root: RootCommandId::from_u128(76),
        };
        let sessions = (1..=3)
            .map(|node| {
                let mut config = focal_consensus::NodeConfig::single(
                    node,
                    identity.cluster,
                    identity.ledger.session.0,
                );
                config.voters = vec![1, 2, 3];
                focal_ledger::Session::open(
                    root.path().join(node.to_string()),
                    identity.ledger,
                    config,
                    focal_ledger::SessionLimits::default(),
                )
                .unwrap()
            })
            .collect();
        let mut cluster = Self {
            root,
            identity,
            sessions,
        };
        cluster.sessions[0].campaign().unwrap();
        cluster.settle(&[0, 1, 2]);
        assert!(cluster.sessions[0].is_authoritative());
        cluster.submit(
            0,
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
        cluster
    }
    fn peer(&self) -> AuthenticatedPeer {
        AuthenticatedPeer::local(PeerGrant {
            principal: self.identity.issuer,
            tenants: [self.identity.ledger.tenant].into_iter().collect(),
            role: PeerRole::Runtime,
        })
        .unwrap()
    }
    fn request(&self, id: u128, stream: StreamRequest) -> RequestEnvelope {
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.identity.ledger,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(id),
            operation: Operation::Stream(stream),
        };
        verify_request(self.peer(), request.clone(), &WireLimits::default()).unwrap();
        request
    }
    fn settle(&mut self, active: &[usize]) {
        for _ in 0..40 {
            let mut messages = Vec::new();
            for &node in active {
                messages.extend(self.sessions[node].poll().unwrap().messages);
            }
            let quiet = messages.is_empty();
            for message in messages {
                let target = message.to as usize - 1;
                if active.contains(&target) {
                    self.sessions[target].step(message).unwrap();
                }
            }
            if quiet {
                break;
            }
        }
    }
    fn submit(&mut self, node: usize, id: u128, command: Command) {
        let epoch = match command {
            Command::NegotiateEpoch { epoch } => epoch,
            _ => RequestEpoch(1),
        };
        let input = AuthenticatedInput {
            ledger: self.identity.ledger,
            principal: self.identity.issuer,
            request_epoch: epoch,
            request_id: RequestId::from_u128(id),
            expected_revision: None,
            authority: AuthorityContext {
                runtime: true,
                cause: Cause::Root(self.identity.root),
                policy_revision: 1,
                logical_time: 0,
                evidence: vec![],
            },
            command,
        };
        assert!(matches!(
            self.sessions[node].propose(&input).unwrap(),
            focal_ledger::Submission::Pending(_)
        ));
        self.settle(&[0, 1, 2]);
        assert!(
            self.sessions[node]
                .receipt(&RequestKey {
                    principal: input.principal,
                    epoch,
                    id: input.request_id
                })
                .is_some()
        );
    }
    fn begin(
        &mut self,
        node: usize,
        streams: &mut Streams,
        request: &RequestEnvelope,
    ) -> crate::streams::PendingStream {
        let Operation::Stream(stream) = &request.operation else {
            panic!("stream request")
        };
        let peer = self.peer();
        streams
            .begin(
                &mut self.sessions[node],
                &peer,
                request,
                stream,
                &WireLimits::default(),
            )
            .unwrap()
    }
    fn drive(
        &mut self,
        node: usize,
        streams: &mut Streams,
        views: &mut ReadViews,
        pending: &mut crate::streams::PendingStream,
        stop_after_proposal: bool,
    ) -> Option<StreamReply> {
        for _ in 0..40 {
            let mut messages = Vec::new();
            let mut reply = None;
            for index in 0..self.sessions.len() {
                let events = self.sessions[index].poll().unwrap();
                if index == node {
                    reply = streams
                        .advance(
                            &mut self.sessions[index],
                            views,
                            pending,
                            &events,
                            &WireLimits::default(),
                        )
                        .unwrap();
                }
                messages.extend(events.messages);
            }
            for message in messages {
                let target = message.to as usize - 1;
                self.sessions[target].step(message).unwrap();
            }
            if reply.is_some() || (stop_after_proposal && self.sessions[node].pending_count() > 0) {
                return reply;
            }
        }
        None
    }
    fn invoke(
        &mut self,
        node: usize,
        streams: &mut Streams,
        views: &mut ReadViews,
        request: &RequestEnvelope,
    ) -> StreamReply {
        let mut pending = self.begin(node, streams, request);
        let reply = self
            .drive(node, streams, views, &mut pending, false)
            .unwrap();
        validate_response(
            request,
            &request.reply(Response::Stream(reply.clone())),
            Some(self.identity.issuer),
            &WireLimits::default(),
        )
        .unwrap();
        reply
    }
    fn elect_majority(&mut self) -> usize {
        // Expire the former leader's lease by advancing real Raft clocks on
        // the surviving majority. Either survivor may win its randomized term.
        for _ in 0..100 {
            for index in [1, 2] {
                self.sessions[index].tick().unwrap();
            }
            self.settle(&[1, 2]);
            if let Some(leader) = [1, 2]
                .into_iter()
                .find(|index| self.sessions[*index].is_authoritative())
            {
                self.sessions[leader].tick().unwrap();
                self.sessions[leader].tick().unwrap();
                self.settle(&[0, 1, 2]);
                return leader;
            }
        }
        panic!("surviving majority did not elect");
    }
    fn reopen(&mut self, index: usize) {
        let old = self.sessions.remove(index);
        drop(old);
        let node = index as u64 + 1;
        let mut config = focal_consensus::NodeConfig::single(
            node,
            self.identity.cluster,
            self.identity.ledger.session.0,
        );
        config.voters = vec![1, 2, 3];
        let session = focal_ledger::Session::open(
            self.root.path().join(node.to_string()),
            self.identity.ledger,
            config,
            focal_ledger::SessionLimits::default(),
        )
        .unwrap();
        self.sessions.insert(index, session);
    }
}

#[test]
fn multi_voter_stream_waits_for_read_quorum_and_durable_ack_then_retries_after_restart() {
    let mut cluster = StreamCluster::new();
    cluster.submit(
        0,
        2,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(2),
        },
    );
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let request = cluster.request(101, register(false));
    let mut pending = cluster.begin(0, &mut streams, &request);
    let events = cluster.sessions[0].poll().unwrap();
    assert!(!events.messages.is_empty());
    assert!(
        streams
            .advance(
                &mut cluster.sessions[0],
                &mut views,
                &mut pending,
                &events,
                &WireLimits::default()
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(cluster.sessions[0].cursor_revision(), 0);
    assert_eq!(cluster.sessions[0].pending_count(), 0);
    assert_eq!(pending.interrupted(), AccessError::Unavailable);
    // ReadIndex peer messages are forwarded by the host, never swallowed by Streams.
    for message in events.messages {
        let index = message.to as usize - 1;
        cluster.sessions[index].step(message).unwrap();
    }
    let first = cluster
        .drive(0, &mut streams, &mut views, &mut pending, false)
        .unwrap();
    assert_eq!(
        first.acknowledged.position,
        Position::origin(cluster.identity.ledger)
    );
    assert!(first.cursor.position > first.acknowledged.position);
    drop(pending);
    let ack = cluster.request(
        102,
        StreamRequest::Poll {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            acknowledged: Some(first.cursor),
            credits: Credits {
                items: 1,
                bytes: 64 * 1024,
            },
        },
    );
    let mut pending = cluster.begin(0, &mut streams, &ack);
    assert!(
        cluster
            .drive(0, &mut streams, &mut views, &mut pending, true)
            .is_none()
    );
    assert_eq!(pending.interrupted(), AccessError::OutcomeUnknown);
    let events = cluster.sessions[0].poll().unwrap();
    assert!(!events.messages.is_empty());
    assert!(
        streams
            .advance(
                &mut cluster.sessions[0],
                &mut views,
                &mut pending,
                &events,
                &WireLimits::default()
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        cluster.sessions[0]
            .cursor(ConsumerId::from_u128(100))
            .unwrap()
            .token
            .position,
        first.acknowledged.position
    );
    drop(pending); // client disconnected after admission, before a durable ACK
    for message in events.messages {
        let index = message.to as usize - 1;
        cluster.sessions[index].step(message).unwrap();
    }
    cluster.settle(&[0, 1, 2]);
    let key = RequestKey {
        principal: cluster.identity.issuer,
        epoch: RequestEpoch(1),
        id: ack.request_id,
    };
    let durable = cluster.sessions[0].cursor_receipt(&key).unwrap().clone();
    assert_eq!(durable.record.as_ref().unwrap().token, first.cursor);
    let revision = cluster.sessions[0].cursor_revision();
    cluster.reopen(0);
    cluster.sessions[1].campaign().unwrap();
    cluster.settle(&[0, 1, 2]);
    assert!(cluster.sessions[1].is_authoritative());
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let retried = cluster.invoke(1, &mut streams, &mut views, &ack);
    assert_eq!(retried.acknowledged, first.cursor);
    assert_eq!(cluster.sessions[1].cursor_revision(), revision);
    assert_eq!(cluster.sessions[1].cursor_receipt(&key), Some(&durable));
}

#[test]
fn lost_leadership_rejects_pending_response_and_retry_commits_on_majority() {
    let mut cluster = StreamCluster::new();
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let open = cluster.request(201, register(false));
    let first = cluster.invoke(0, &mut streams, &mut views, &open);
    let ack = cluster.request(
        202,
        StreamRequest::Poll {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            acknowledged: Some(first.cursor),
            credits: Credits {
                items: 1,
                bytes: 64 * 1024,
            },
        },
    );
    let mut pending = cluster.begin(0, &mut streams, &ack);
    assert!(
        cluster
            .drive(0, &mut streams, &mut views, &mut pending, true)
            .is_none()
    );
    // Neither follower receives the cursor proposal. The other two elect a
    // current-term leader with the last committed cursor still at its old ACK.
    let outgoing = cluster.sessions[0].poll().unwrap();
    assert!(!outgoing.messages.is_empty());
    drop(outgoing);
    let leader = cluster.elect_majority();
    assert_eq!(
        streams.advance(
            &mut cluster.sessions[0],
            &mut views,
            &mut pending,
            &focal_ledger::SessionEvents::default(),
            &WireLimits::default()
        ),
        Err(AccessError::OutcomeUnknown)
    );
    let key = RequestKey {
        principal: cluster.identity.issuer,
        epoch: RequestEpoch(1),
        id: ack.request_id,
    };
    assert!(cluster.sessions[leader].cursor_receipt(&key).is_none());
    drop(pending);
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let retry = cluster.invoke(leader, &mut streams, &mut views, &ack);
    assert_eq!(retry.acknowledged, first.cursor);
    assert!(cluster.sessions[leader].cursor_receipt(&key).is_some());
}

#[test]
fn multi_voter_seed_pages_pin_old_prefix_and_complete_before_tail_delivery() {
    let mut cluster = StreamCluster::new();
    let claim = crate::demo::claim(&cluster.identity, ClaimId::from_u128(777)).unwrap();
    cluster.submit(0, 2, Command::GenerateClaim { claim });
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let open = cluster.request(301, register(true));
    let first = cluster.invoke(0, &mut streams, &mut views, &open);
    let page = first.seed.as_ref().unwrap();
    assert!(page.next.is_some());
    let prefix = page.token.sequence;
    cluster.submit(
        0,
        3,
        Command::PostClaim {
            claim: ClaimId::from_u128(777),
        },
    );
    let tail = cluster.sessions[0].sequence();
    let rest = views
        .read(
            &mut cluster.sessions[0],
            cluster.identity.issuer,
            &ReadRequest {
                consistency: ReadConsistency::Exact(page.token),
                query: ReadQuery::Scan { after: page.next },
                max_items: 10,
            },
            RequestId::from_u128(302),
            &WireLimits::default(),
        )
        .unwrap();
    assert_eq!(rest.token.sequence, prefix);
    assert!(rest.next.is_none());
    let retried = cluster.invoke(0, &mut streams, &mut views, &open);
    assert_eq!(retried.seed, first.seed);
    assert_eq!(retried.cursor.generation, first.cursor.generation);
    let complete = cluster.request(
        303,
        StreamRequest::CompleteSeed {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            snapshot: prefix,
        },
    );
    cluster.invoke(0, &mut streams, &mut views, &complete);
    let poll = cluster.request(
        304,
        StreamRequest::Poll {
            cursor: first.cursor,
            filter: DeltaFilter::All,
            acknowledged: None,
            credits: Credits {
                items: 10,
                bytes: 64 * 1024,
            },
        },
    );
    let delivered = cluster.invoke(0, &mut streams, &mut views, &poll);
    assert!(
        delivered
            .events
            .iter()
            .any(|event| matches!(event,StreamEvent::Delta {delta,..} if delta.id.sequence==tail))
    );
    assert!(
        delivered.events.iter().all(
            |event| !matches!(event,StreamEvent::Delta {delta,..} if delta.id.sequence<=prefix)
        )
    );
    assert_eq!(
        delivered.cursor.position,
        Position::resolved(cluster.identity.ledger, tail)
    );
    assert_eq!(delivered.acknowledged.position.sequence, prefix);
    let leader = cluster.elect_majority();
    let mut views = ReadViews::new();
    let mut streams = Streams::new().unwrap();
    // The new host cannot reconstruct a lost old in-memory seed lease. It gives
    // a typed expiry rather than silently seeding a newer prefix under old ID.
    let mut pending = cluster.begin(leader, &mut streams, &open);
    let mut outcome = None;
    for _ in 0..40 {
        let mut messages = vec![];
        for index in 0..3 {
            let events = cluster.sessions[index].poll().unwrap();
            if index == leader {
                match streams.advance(
                    &mut cluster.sessions[index],
                    &mut views,
                    &mut pending,
                    &events,
                    &WireLimits::default(),
                ) {
                    Err(error) => outcome = Some(error),
                    Ok(None) => {}
                    Ok(Some(_)) => panic!("old seed unexpectedly recreated"),
                }
            }
            messages.extend(events.messages);
        }
        for message in messages {
            let index = message.to as usize - 1;
            cluster.sessions[index].step(message).unwrap();
        }
        if outcome.is_some() {
            break;
        }
    }
    assert_eq!(outcome, Some(AccessError::SnapshotExpired));
}

#[test]
fn network_stream_cannot_renew_or_reset_a_protected_consumer() {
    let mut cluster = StreamCluster::new();
    let peer = cluster.peer();
    let input = focal_ledger::CursorInput {
        ledger: cluster.identity.ledger,
        key: RequestKey {
            principal: cluster.identity.issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(401),
        },
        intent_hash: ContentHash([40; 32]),
        command: CursorCommand {
            expected_revision: 0,
            now: 0,
            operation: CursorOperation::RegisterProtected {
                consumer: ConsumerId::from_u128(100),
                scope: stream_scope(&peer, cluster.identity.ledger, &DeltaFilter::All).unwrap(),
                filter: DeltaFilter::All,
                start: Position::origin(cluster.identity.ledger),
            },
        },
    };
    cluster.sessions[0].submit_cursor_control(&input).unwrap();
    cluster.settle(&[0, 1, 2]);
    let token = cluster.sessions[0]
        .cursor(ConsumerId::from_u128(100))
        .unwrap()
        .token;
    let request = cluster.request(
        402,
        StreamRequest::Poll {
            cursor: token,
            filter: DeltaFilter::All,
            acknowledged: Some(token),
            credits: Credits {
                items: 1,
                bytes: 65536,
            },
        },
    );
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let mut pending = cluster.begin(0, &mut streams, &request);
    let mut refused = None;
    for _ in 0..40 {
        let mut messages = vec![];
        for index in 0..3 {
            let events = cluster.sessions[index].poll().unwrap();
            if index == 0 {
                match streams.advance(
                    &mut cluster.sessions[index],
                    &mut views,
                    &mut pending,
                    &events,
                    &WireLimits::default(),
                ) {
                    Err(error) => refused = Some(error),
                    Ok(None) => {}
                    Ok(Some(_)) => panic!("protected cursor escaped"),
                }
            }
            messages.extend(events.messages);
        }
        for message in messages {
            let index = message.to as usize - 1;
            cluster.sessions[index].step(message).unwrap();
        }
        if refused.is_some() {
            break;
        }
    }
    assert_eq!(refused, Some(AccessError::Unauthorized));
    assert_eq!(cluster.sessions[0].cursor_revision(), 1);
    assert_eq!(
        cluster.sessions[0]
            .cursor(ConsumerId::from_u128(100))
            .unwrap()
            .mode,
        CursorMode::Protected
    );
}

#[test]
fn pending_stream_requests_are_bounded_and_drop_releases_owned_staging() {
    let mut cluster = StreamCluster::new();
    let peer = cluster.peer();
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let request = cluster.request(501, register(false));
    let Operation::Stream(stream) = &request.operation else {
        panic!("stream request")
    };
    // The convenience wrapper rejects before polling, so it cannot consume
    // peer messages or start a hidden proposal in a replicated group.
    assert_eq!(
        streams.handle(
            &mut cluster.sessions[0],
            &mut views,
            &peer,
            &request,
            stream,
            &WireLimits::default()
        ),
        Err(AccessError::Unavailable)
    );
    assert!(cluster.sessions[0].poll().unwrap().messages.is_empty());
    assert_eq!(streams.charged_bytes(), 0);
    let mut held = vec![];
    let mut exhausted = false;
    for id in 502..702 {
        let request = cluster.request(id, register(false));
        let Operation::Stream(stream) = &request.operation else {
            panic!("stream request")
        };
        match streams.begin(
            &mut cluster.sessions[0],
            &peer,
            &request,
            stream,
            &WireLimits::default(),
        ) {
            Ok(pending) => held.push(pending),
            Err(AccessError::Capacity) => {
                exhausted = true;
                break;
            }
            Err(error) => panic!("unexpected admission failure: {error:?}"),
        }
    }
    assert!(exhausted);
    assert!(!held.is_empty());
    assert!(streams.charged_bytes() > 0);
    assert_eq!(cluster.sessions[0].pending_count(), 0);
    assert_eq!(cluster.sessions[0].cursor_revision(), 0);
    drop(held);
    assert_eq!(streams.charged_bytes(), 0);
}

#[test]
fn changed_intent_during_a_pending_request_cannot_replace_its_durable_outcome() {
    let mut cluster = StreamCluster::new();
    let mut streams = Streams::new().unwrap();
    let mut views = ReadViews::new();
    let original = cluster.request(801, register(false));
    let mut first = cluster.begin(0, &mut streams, &original);
    assert!(
        cluster
            .drive(0, &mut streams, &mut views, &mut first, true)
            .is_none()
    );
    let changed = cluster.request(
        801,
        StreamRequest::Open {
            consumer: ConsumerId::from_u128(999),
            filter: DeltaFilter::All,
            start: None,
            seed: false,
            credits: Credits {
                items: 1,
                bytes: 65536,
            },
        },
    );
    let mut conflict = cluster.begin(0, &mut streams, &changed);
    let mut refused = None;
    let mut reply = None;
    for _ in 0..40 {
        let mut messages = vec![];
        for index in 0..3 {
            let events = cluster.sessions[index].poll().unwrap();
            if index == 0 {
                if reply.is_none() {
                    reply = streams
                        .advance(
                            &mut cluster.sessions[index],
                            &mut views,
                            &mut first,
                            &events,
                            &WireLimits::default(),
                        )
                        .unwrap();
                }
                if refused.is_none() {
                    match streams.advance(
                        &mut cluster.sessions[index],
                        &mut views,
                        &mut conflict,
                        &events,
                        &WireLimits::default(),
                    ) {
                        Err(error) => refused = Some(error),
                        Ok(None) => {}
                        Ok(Some(_)) => panic!("conflicting intent was accepted"),
                    }
                }
            }
            messages.extend(events.messages);
        }
        for message in messages {
            let index = message.to as usize - 1;
            cluster.sessions[index].step(message).unwrap();
        }
        if refused.is_some() && reply.is_some() {
            break;
        }
    }
    assert_eq!(refused, Some(AccessError::InvalidRequest));
    assert_eq!(
        reply.unwrap().cursor.key.consumer,
        ConsumerId::from_u128(100)
    );
    assert_eq!(cluster.sessions[0].cursor_revision(), 1);
    assert!(
        cluster.sessions[0]
            .cursor(ConsumerId::from_u128(999))
            .is_none()
    );
    drop(first);
    drop(conflict);
    assert_eq!(streams.charged_bytes(), 0);
}
