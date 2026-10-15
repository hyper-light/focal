use super::*;
use crate::{demo, embedded::NodeIdentity};
use focal_consensus::{DurableNode, NodeConfig};
use focal_ledger::SessionLimits;
use focal_log::{SharedWal, WalIdentity, WalOptions, WalWriterLimits};

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(880),
        session: SessionId::from_u128(881),
    }
}
fn actor() -> AuthenticatedPeer {
    AuthenticatedPeer::local(PeerGrant {
        principal: ParticipantId::from_u128(882),
        tenants: [ledger().tenant].into_iter().collect(),
        role: PeerRole::Actor,
    })
    .unwrap()
}
fn request(id: u128, operation: Operation) -> RequestEnvelope {
    RequestEnvelope {
        protocol: PROTOCOL_VERSION,
        ledger: ledger(),
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(id),
        operation,
    }
}
fn submit(id: u128, command: Command) -> RequestEnvelope {
    request(
        id,
        Operation::Submit {
            expected_revision: None,
            command,
        },
    )
}

#[test]
fn native_disposal_uses_fleet_completion_admission_when_ordinary_memory_is_full() {
    use focal_core::native::{NativeCommand, NativeInput, input_codec};
    let expected = focal_model::lifecycle::Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(900),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    };
    let budget = MemoryBudget::new(16 * 1024, 4 * 1024).unwrap();
    let ordinary = budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 12 * 1024)
        .unwrap()
        .commit();
    assert!(
        budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, 1)
            .is_err()
    );
    for protocol in [NATIVE_PROTOCOL_VERSION, NATIVE_WRITTEN_PROTOCOL_VERSION] {
        for (command, completion) in [
            (NativeCommand::Cancel { expected }, true),
            (NativeCommand::ReleaseScope { expected }, true),
            (
                NativeCommand::CancelMonitor {
                    expected,
                    receipt: None,
                    id: MonitorId::from_u128(901),
                },
                true,
            ),
            (NativeCommand::Post { expected }, false),
        ] {
            let input = NativeInput {
                request: RequestKey {
                    principal: actor().principal(),
                    epoch: RequestEpoch(1),
                    id: RequestId::from_u128(4),
                },
                command,
            };
            let plan = input_codec::EncodingPlan::prepare(
                input_codec::InputFrame::Request {
                    ledger: ledger(),
                    profile: focal_ledger::NativeContentProfile::ProjectionOnly,
                    input: &input,
                },
                input_codec::EncodingLimits {
                    bytes: 1 << 20,
                    visits: 1 << 28,
                },
            )
            .unwrap();
            let mut frame = vec![0; plan.quote().bytes];
            plan.write_into(&mut frame).unwrap();
            let mut envelope = request(4, Operation::Native { frame });
            envelope.protocol = protocol;
            let verified = verify_request(actor(), envelope, &WireLimits::default()).unwrap();
            assert_eq!(completion_request(&verified), completion);
            let lane = if completion_request(&verified) {
                BudgetLane::Completion
            } else {
                BudgetLane::Ordinary
            };
            let admission = budget.reserve(BudgetKind::Pending, lane, 512);
            assert_eq!(admission.is_ok(), completion);
            drop(admission);
        }
    }
    drop(ordinary);
    assert_eq!(budget.stats().used, 0);
}
#[test]
fn completion_and_receipt_probe_use_reserved_bytes_and_queue_slots() {
    focal_rt::block_on(async move {
        let directory = tempfile::tempdir().unwrap();
        let node = MemoryBudget::new(1024 * 1024 * 1024, 256 * 1024 * 1024).unwrap();
        let tenant = node.child(512 * 1024 * 1024, 128 * 1024 * 1024).unwrap();
        let wal = SharedWal::open_with_budget(
            directory.path(),
            WalOptions::new(WalIdentity {
                cluster: [88; 16],
                node: 1,
                stream: 0,
            }),
            WalWriterLimits::default(),
            node.child(128 * 1024 * 1024, 32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let consensus = DurableNode::open_on_wal_in(
            NodeConfig::single(1, [88; 16], ledger().session.0),
            wal.clone(),
            &tenant,
        )
        .unwrap();
        let session =
            Session::from_node_in(ledger(), consensus, SessionLimits::default(), &tenant).unwrap();
        let root = RootCommandId::from_u128(883);
        let mut config = ReplicaConfig::new(root);
        config.tick = Duration::from_millis(20);
        let (hosts, owner, outgoing) = ReplicaFleet::spawn(
            1,
            vec![FleetReplica { session, config }],
            vec![FleetTenant {
                tenant: ledger().tenant,
                weight: 1,
                budget: tenant.clone(),
            }],
            node.clone(),
            ReplicaHost::wire_limits(),
        )
        .unwrap();
        let host = &hosts[&ledger()];
        let limits = ReplicaHost::wire_limits();
        crate::test_waits::within(
            &[host],
            Duration::from_secs(10),
            Duration::from_millis(20),
            async {
                loop {
                    let reply = dispatch(
                        host,
                        actor(),
                        request(
                            99,
                            Operation::Read(ReadRequest {
                                consistency: ReadConsistency::Linearizable,
                                query: ReadQuery::Objects(vec![]),
                                max_items: 1,
                            }),
                        ),
                        &limits,
                    )
                    .await;
                    if matches!(reply.result, Response::Read(_)) {
                        break;
                    }
                    focal_rt::time::sleep(Duration::from_millis(20))
                        .await
                        .unwrap();
                }
            },
        )
        .await
        .unwrap();
        let reply = dispatch(
            host,
            actor(),
            request(
                1,
                Operation::OpenEpoch {
                    epoch: RequestEpoch(1),
                },
            ),
            &limits,
        )
        .await;
        assert!(
            matches!(
                reply.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{reply:?}"
        );
        let identity = NodeIdentity {
            schema: 1,
            cluster: [88; 16],
            node: 1,
            ledger: ledger(),
            issuer: actor().principal(),
            worker: ParticipantId::from_u128(884),
            evaluator: ParticipantId::from_u128(885),
            root,
        };
        let claim = ClaimId::from_u128(886);
        let new_claim = demo::claim(&identity, claim).unwrap();
        let reply = dispatch(
            host,
            actor(),
            submit(
                2,
                Command::GenerateClaim {
                    claim: new_claim.clone(),
                },
            ),
            &limits,
        )
        .await;
        assert!(
            matches!(
                reply.result,
                Response::Submitted(MutationReply::Committed(_))
            ),
            "{reply:?}"
        );
        let stats = tenant.stats();
        let bytes = tenant
            .reserve(
                BudgetKind::Payload,
                BudgetLane::Ordinary,
                stats.limit - stats.completion_reserve - stats.ordinary_used,
            )
            .unwrap();
        let HostSender::Group { slots, .. } = &host.sender else {
            panic!("group sender required");
        };
        let stats = slots.stats();
        let items = slots
            .reserve(
                BudgetKind::Pending,
                BudgetLane::Ordinary,
                stats.limit - stats.completion_reserve - stats.ordinary_used,
            )
            .unwrap();
        let ordinary = dispatch(
            host,
            actor(),
            submit(3, Command::GenerateClaim { claim: new_claim }),
            &limits,
        )
        .await;
        assert!(matches!(
            ordinary.result,
            Response::Error(AccessError::Capacity)
        ));
        // Priority is a scheduling choice, not an authority grant.
        assert!(
            verify_request(
                actor(),
                submit(
                    4,
                    Command::RevokeClaim {
                        claim,
                        reason: "forged runtime".into()
                    }
                ),
                &limits
            )
            .is_err()
        );
        let completion = submit(
            5,
            Command::CancelClaim {
                claim,
                reason: "issuer ended the existing work".into(),
            },
        );
        let verified = verify_request(actor(), completion.clone(), &limits).unwrap();
        assert!(completion_request(&verified));
        let probe = host.probe_receipt(verified).await.unwrap();
        assert!(probe.known.is_none());
        drop(probe);
        let reply = dispatch(host, actor(), completion.clone(), &limits).await;
        let Response::Submitted(MutationReply::Committed(receipt)) = reply.result else {
            panic!("completion did not commit: {reply:?}");
        };
        assert!(matches!(
            receipt.outcome,
            CommandResult::Claim {
                status: ClaimStatus::Cancelled,
                ..
            }
        ));
        let probe = host
            .probe_receipt(verify_request(actor(), completion, &limits).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            probe.known,
            Some(Response::Submitted(MutationReply::Committed(_)))
        ));
        drop(probe);
        assert!(!host.progress().stopped);
        drop(items);
        drop(bytes);
        host.stop().await.unwrap();
        owner.join().unwrap();
        drop(hosts);
        drop(outgoing);
        drop(wal);
        assert_eq!(node.stats().used, 0);
    })
    .unwrap()
}
