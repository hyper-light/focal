#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! R11 §1 black-box linearizability: real `focal_client::Client` instances
//! (each carrying a `TraceSink`) drive a native workload against an in-process
//! node. Their *client-side* traces are merged with the owner's authoritative
//! committed receipts and handed to the `focal-sim` history checker, which
//! verifies that what each client observed is consistent with a single serial
//! commit order (contiguous prefix, exactly-once commit, no premature success).
//!
//! The publication order comes from the owner's own reply receipts
//! (`NativeReceipt` — the authoritative statement of what durably committed),
//! not from any client's belief about the outcome. A client's `Complete` is
//! then checked against that independent order. (A fully independent offline
//! WAL reader over the node's *unified* consensus log — the `focal ledger
//! publications` path — is a deeper follow-up; the bare-`NativeSession` offline
//! replay is already covered in focal-ledger's
//! `recovered_prefix_is_a_linearizable_publication_history`.)

use focal_client::{
    Client, ClientError, ClientTransport, EmbeddedTransport, RetryPolicy, TraceEntry, TraceOutcome,
    TraceSink, TransportFuture, pending::OperationContext,
};
use focal_core::native::{NativeCommand, NativeInput, input_codec};
use focal_ledger::NativeContentProfile;
use focal_model::lifecycle::{
    Binding, Principal, aggregation, claim::ClaimDefinition, creation::Proposal, graph, scope,
    succession::Lineage, validation,
};
use focal_model::*;
use focal_node::{config::Settings, embedded::EmbeddedNode, host::LocalHost};
use focal_sim::history::{self, Consistency, Event, Initial, Outcome, Request};
use focal_wire::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Collects every traced exchange a client completes.
#[derive(Default)]
struct RecordingSink {
    entries: Mutex<Vec<TraceEntry>>,
}
impl RecordingSink {
    fn drain(&self) -> Vec<TraceEntry> {
        self.entries.lock().unwrap().clone()
    }
}
/// A handle the client owns; delegates to the shared sink the test keeps.
struct SinkHandle(Arc<RecordingSink>);
impl TraceSink for SinkHandle {
    fn record(&self, entry: TraceEntry) {
        if let Ok(mut entries) = self.0.entries.lock() {
            entries.push(entry);
        }
    }
}

// ---- native claim frames (shaped as in the node's own native host tests) ----

fn native_binding(ledger: LedgerId, id: u128) -> Binding {
    Binding {
        ledger,
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}

fn definition(issuer: ParticipantId, binding: Binding) -> validation::Declaration {
    let claim_id = u128::from_be_bytes(binding.object.0);
    validation::Declaration::new(
        Principal::Actor(issuer),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(claim_id.checked_add(10_000).unwrap()),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer,
            declaration_index: 900,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 100,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

fn proposal(ledger: LedgerId, issuer: ParticipantId, subject: ParticipantId, id: u128) -> Proposal {
    let binding = native_binding(ledger, id);
    Proposal {
        definition: ClaimDefinition {
            binding,
            issuer,
            subject,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding,
                issuer,
                &[],
                &[definition(issuer, binding)],
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 32,
                    max_updates: 8,
                },
            )
            .unwrap(),
            scope_limits: scope::ScopeLimits {
                scopes: 8,
                roots: 32,
                children: 16,
            },
        },
        owner: None,
    }
}

fn create(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    request: u128,
    id: u128,
) -> NativeInput {
    let proposals = vec![proposal(ledger, issuer, worker, id)];
    let declarations = proposals
        .iter()
        .map(|p| definition(issuer, p.definition.binding))
        .collect();
    NativeInput {
        request: RequestKey {
            principal: issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        },
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    }
}

fn frame(ledger: LedgerId, input: &NativeInput) -> Vec<u8> {
    let plan = input_codec::EncodingPlan::prepare(
        input_codec::InputFrame::Request {
            ledger,
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        input_codec::EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 28,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}

fn create_envelope(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    request: u128,
    id: u128,
) -> RequestEnvelope {
    let input = create(ledger, issuer, worker, request, id);
    RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::Native {
            frame: frame(ledger, &input),
        },
    }
}

/// The generic receipt the checker compares. A native commit's identity is
/// (ledger, key, sequence, intent); the generic result is not part of it, and
/// every projection here uses the same placeholder, so the field is inert.
fn receipt(
    ledger: LedgerId,
    key: RequestKey,
    sequence: SessionSeq,
    intent: ContentHash,
) -> MutationReceipt {
    MutationReceipt {
        ledger,
        key,
        sequence,
        command_hash: intent,
        outcome: CommandResult::Noop,
    }
}

/// A per-prefix-unique publication marker. A black-box client cannot compute the
/// owner's full-state digest for a read, so this stands in for the state hash
/// (unused without reads, but distinct per sequence).
fn state_marker(sequence: SessionSeq) -> ContentHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&sequence.0.to_be_bytes());
    ContentHash(bytes)
}

/// Merge per-client traces with the owner's authoritative publications into the
/// checker's event stream. Invoke/Complete are ordered by the client's
/// single-host timestamps; each publication is emitted just before the first
/// completion that observes it (so a success never precedes its publication) and
/// in sequence order (so the prefix stays contiguous). Publications no client
/// completed still appear, keeping the prefix whole.
fn build_history(
    traces: &[(ParticipantId, Vec<TraceEntry>)],
    publications: &[MutationReceipt],
) -> Vec<Event> {
    struct Point {
        nanos: u128,
        invoke: bool,
        call: u64,
        entry: TraceEntry,
        key: RequestKey,
    }
    let mut points = Vec::new();
    let mut call = 0u64;
    for (principal, entries) in traces {
        for entry in entries {
            let key = RequestKey {
                principal: *principal,
                epoch: entry.request_epoch,
                id: entry.request_id,
            };
            points.push(Point {
                nanos: entry.invoked_nanos,
                invoke: true,
                call,
                entry: entry.clone(),
                key,
            });
            points.push(Point {
                nanos: entry.completed_nanos,
                invoke: false,
                call,
                entry: entry.clone(),
                key,
            });
            call += 1;
        }
    }
    // Stable by time; at equal timestamps an invoke precedes a completion.
    points.sort_by(|a, b| a.nanos.cmp(&b.nanos).then(b.invoke.cmp(&a.invoke)));

    let mut events = Vec::new();
    let mut next_pub = 0usize;
    let emit_pubs_through = |events: &mut Vec<Event>, next: &mut usize, upto: SessionSeq| {
        while *next < publications.len() && publications[*next].sequence <= upto {
            let published = publications[*next].clone();
            let hash = state_marker(published.sequence);
            events.push(Event::Publish {
                receipt: published,
                state_hash: hash,
            });
            *next += 1;
        }
    };

    for point in &points {
        if point.invoke {
            let request = if point.entry.mutation {
                let command_hash = match point.entry.outcome {
                    TraceOutcome::Committed { command_hash, .. } => command_hash,
                    // Checked only on a committed completion; inert otherwise.
                    _ => ContentHash([0; 32]),
                };
                Request::Mutation {
                    ledger: point.entry.ledger,
                    key: point.key,
                    command_hash,
                }
            } else {
                Request::Read {
                    ledger: point.entry.ledger,
                    consistency: Consistency::Linearizable,
                }
            };
            events.push(Event::Invoke {
                call: point.call,
                request,
            });
        } else {
            let outcome = match point.entry.outcome {
                TraceOutcome::Committed {
                    sequence,
                    command_hash,
                } => {
                    emit_pubs_through(&mut events, &mut next_pub, sequence);
                    Outcome::Committed(receipt(
                        point.entry.ledger,
                        point.key,
                        sequence,
                        command_hash,
                    ))
                }
                TraceOutcome::Read { sequence } => Outcome::Read {
                    sequence,
                    state_hash: state_marker(sequence),
                },
                TraceOutcome::Refused => Outcome::Refused,
                TraceOutcome::Unknown => Outcome::Unknown,
            };
            events.push(Event::Complete {
                call: point.call,
                outcome,
            });
        }
    }
    // Any committed-but-uncompleted publications keep the prefix contiguous.
    emit_pubs_through(&mut events, &mut next_pub, SessionSeq(u64::MAX));
    events
}

#[test]
fn concurrent_traced_clients_produce_a_linearizable_native_history() {
    let root = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    focal_node::native_activation::activate_local(&settings, NativeContentProfile::ProjectionOnly)
        .unwrap();

    let node = EmbeddedNode::open(&settings).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let context = OperationContext {
        cluster: node.identity.cluster,
        ledger,
        principal: issuer,
    };
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();

    let make_client = || {
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: context.principal,
            tenants: BTreeSet::from([ledger.tenant]),
            role: PeerRole::Runtime,
        })
        .unwrap();
        Client::new(
            EmbeddedTransport::new(peer, host.clone(), limits.clone()).unwrap(),
            RetryPolicy::default(),
            limits.clone(),
            1,
        )
        .unwrap()
    };

    let sink_a = Arc::new(RecordingSink::default());
    let sink_b = Arc::new(RecordingSink::default());
    let client_a = make_client().with_trace(Box::new(SinkHandle(Arc::clone(&sink_a))));
    let client_b = make_client().with_trace(Box::new(SinkHandle(Arc::clone(&sink_b))));

    // Two clients (same principal, disjoint request ids and claim ids) submit
    // creates in an interleaved order. Each reply is the owner's authoritative
    // statement of a durable commit.
    let mut publications: Vec<MutationReceipt> = Vec::new();
    let submit = |client: &Client<EmbeddedTransport<LocalHost>>,
                  request: u128,
                  claim: u128|
     -> NativeReceipt {
        let envelope = create_envelope(ledger, issuer, worker, request, claim);
        match runtime.block_on(client.request(envelope)).unwrap().result {
            Response::Native(NativeMutationReply::Committed(reply)) => reply,
            other => panic!("native create ({request}): {other:?}"),
        }
    };
    // Interleave A, B, A, B.
    for (client, request, claim) in [
        (&client_a, 1u128, 100u128),
        (&client_b, 3, 200),
        (&client_a, 2, 101),
        (&client_b, 4, 201),
    ] {
        let reply = submit(client, request, claim);
        let key = RequestKey {
            principal: issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        };
        publications.push(receipt(ledger, key, reply.sequence, reply.intent));
    }
    publications.sort_by_key(|receipt| receipt.sequence.0);
    assert_eq!(
        publications.iter().map(|r| r.sequence).collect::<Vec<_>>(),
        vec![SessionSeq(1), SessionSeq(2), SessionSeq(3), SessionSeq(4)],
        "the owner assigned a contiguous commit order"
    );

    let traces = vec![(issuer, sink_a.drain()), (issuer, sink_b.drain())];
    // Each traced exchange committed and its trace matches the owner's receipt.
    for (_, entries) in &traces {
        assert_eq!(entries.len(), 2, "each client submitted two creates");
        for entry in entries {
            assert!(entry.mutation);
            let TraceOutcome::Committed {
                sequence,
                command_hash,
            } = entry.outcome
            else {
                panic!(
                    "expected a committed native create, got {:?}",
                    entry.outcome
                );
            };
            let published = publications
                .iter()
                .find(|r| r.key.id == entry.request_id)
                .expect("every traced commit is published");
            assert_eq!(published.sequence, sequence);
            assert_eq!(published.command_hash, command_hash);
        }
    }

    let initial = [Initial {
        ledger,
        sequence: SessionSeq(publications[0].sequence.0 - 1),
        state_hash: ContentHash([0; 32]),
    }];
    let events = build_history(&traces, &publications);
    let report = history::check(&initial, &events, 256).unwrap();
    assert_eq!(report.publications, 4);
    assert_eq!(report.reads, 0);
    assert_eq!(report.unknown, 0);
    assert_eq!(report.pending, 0);

    // A checker sanity anchor: dropping a publication breaks the prefix, so a
    // fabricated success (one no owner receipt backs) is caught.
    let mut tampered = events.clone();
    tampered.retain(|event| !matches!(event, Event::Publish { receipt, .. } if receipt.sequence == SessionSeq(4)));
    assert!(
        history::check(&initial, &tampered, 256).is_err(),
        "a completion without its publication must be rejected"
    );

    drop(client_a);
    drop(client_b);
    drop(host);
    owner.join().unwrap();
}

/// A transport that forwards to the owner but drops the reply until `deliver`
/// is set, modelling a lost response. The owner still processes each forwarded
/// request (committing the first time it sees the identity, exact-retrying
/// afterwards), so dropping replies never double-commits.
struct DropReplies<T> {
    inner: T,
    deliver: Arc<AtomicBool>,
}
impl<T: ClientTransport> ClientTransport for DropReplies<T> {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            let reply = self.inner.request(route, request).await;
            if self.deliver.load(Ordering::SeqCst) {
                reply
            } else {
                // The owner processed the request; drop its response.
                let _ = reply;
                Err(WireError::Timeout)
            }
        })
    }
}

#[test]
fn lost_reply_then_exact_retry_commits_exactly_once() {
    let root = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut settings = Settings::default();
    settings.node.data_dir = Some(root.path().into());
    focal_node::native_activation::activate_local(&settings, NativeContentProfile::ProjectionOnly)
        .unwrap();

    let node = EmbeddedNode::open(&settings).unwrap();
    let ledger = node.identity.ledger;
    let issuer = node.identity.issuer;
    let worker = node.identity.worker;
    let cluster = node.identity.cluster;
    let limits = WireLimits::default();
    let (host, owner) = LocalHost::spawn(node, limits.clone()).unwrap();
    let _ = OperationContext {
        cluster,
        ledger,
        principal: issuer,
    };

    let deliver = Arc::new(AtomicBool::new(false));
    let peer = AuthenticatedPeer::local(PeerGrant {
        principal: issuer,
        tenants: BTreeSet::from([ledger.tenant]),
        role: PeerRole::Runtime,
    })
    .unwrap();
    let sink = Arc::new(RecordingSink::default());
    // A short policy so the first request exhausts its window quickly and the
    // application observes an unknown outcome.
    let policy = RetryPolicy {
        max_attempts: 3,
        max_elapsed: std::time::Duration::from_secs(5),
        base_backoff: std::time::Duration::from_millis(1),
        max_backoff: std::time::Duration::from_millis(5),
    };
    let client = Client::new(
        DropReplies {
            inner: EmbeddedTransport::new(peer, host.clone(), limits.clone()).unwrap(),
            deliver: Arc::clone(&deliver),
        },
        policy,
        limits.clone(),
        1,
    )
    .unwrap()
    .with_trace(Box::new(SinkHandle(Arc::clone(&sink))));

    // First attempt: replies are dropped, so the client exhausts its window and
    // reports an unknown outcome — though the owner committed the request.
    let first = runtime.block_on(client.request(create_envelope(ledger, issuer, worker, 1, 100)));
    assert!(
        matches!(first, Err(ClientError::OutcomeUnknown { .. })),
        "a lost reply on a mutation is an unknown outcome, got {first:?}"
    );

    // The application retries the identical request; the reply now arrives and
    // the owner returns the same committed receipt (exact retry, no re-commit).
    deliver.store(true, Ordering::SeqCst);
    let retry_receipt = match runtime
        .block_on(client.request(create_envelope(ledger, issuer, worker, 1, 100)))
        .unwrap()
        .result
    {
        Response::Native(NativeMutationReply::Committed(receipt)) => receipt,
        other => panic!("exact retry: {other:?}"),
    };
    assert_eq!(retry_receipt.sequence, SessionSeq(1), "exactly one commit");

    // A second, cleanly-delivered create.
    let second_receipt = match runtime
        .block_on(client.request(create_envelope(ledger, issuer, worker, 2, 101)))
        .unwrap()
        .result
    {
        Response::Native(NativeMutationReply::Committed(receipt)) => receipt,
        other => panic!("second create: {other:?}"),
    };
    assert_eq!(second_receipt.sequence, SessionSeq(2));

    let key1 = RequestKey {
        principal: issuer,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(1),
    };
    let key2 = RequestKey {
        principal: issuer,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(2),
    };
    let publications = vec![
        receipt(ledger, key1, retry_receipt.sequence, retry_receipt.intent),
        receipt(ledger, key2, second_receipt.sequence, second_receipt.intent),
    ];

    let entries = sink.drain();
    // Three exchanges: unknown, then the committed retry, then the second create.
    assert_eq!(entries.len(), 3);
    assert!(matches!(entries[0].outcome, TraceOutcome::Unknown));
    assert_eq!(entries[0].request_id, RequestId::from_u128(1));
    assert!(matches!(entries[1].outcome, TraceOutcome::Committed { .. }));
    assert_eq!(entries[1].request_id, RequestId::from_u128(1));

    let traces = vec![(issuer, entries)];
    let initial = [Initial {
        ledger,
        sequence: SessionSeq(0),
        state_hash: ContentHash([0; 32]),
    }];
    let events = build_history(&traces, &publications);
    let report = history::check(&initial, &events, 256).unwrap();
    assert_eq!(report.publications, 2, "the retried request committed once");
    assert_eq!(report.unknown, 1, "the lost-reply attempt is unknown");
    assert_eq!(report.pending, 0);

    // If the owner had double-committed the retried key, the checker would catch
    // it: a second publication of key one at the next contiguous sequence keeps
    // the prefix whole, so the key collision is what trips the checker.
    let mut doubled = events.clone();
    doubled.push(Event::Publish {
        receipt: receipt(ledger, key1, SessionSeq(3), retry_receipt.intent),
        state_hash: state_marker(SessionSeq(3)),
    });
    assert_eq!(
        history::check(&initial, &doubled, 256),
        Err(history::HistoryError::DuplicateCommit)
    );

    drop(client);
    drop(host);
    owner.join().unwrap();
}
