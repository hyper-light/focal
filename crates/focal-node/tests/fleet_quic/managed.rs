use super::*;
fn managed_request(id: u128, operation: Operation) -> RequestEnvelope {
    let mut value = request(id, operation);
    value.protocol = MANAGED_PROTOCOL_VERSION;
    value
}
async fn exchange(fleet: &Fleet, node: usize, request: &RequestEnvelope) -> Response {
    let endpoint = &fleet.routes[&(node as u64 + 1)];
    let remote = fleet
        .actor_connector
        .connect(endpoint.address, &endpoint.server_name)
        .await
        .unwrap();
    let reply = remote.request(request).await.unwrap();
    remote.close();
    reply.result
}
async fn current_leader(fleet: &Fleet, excluding: Option<usize>) -> usize {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, replica) in fleet.replicas.iter().enumerate() {
                let progress = replica.host.progress();
                if excluding != Some(index)
                    && !progress.stopped
                    && progress.node == progress.leader
                    && progress.term > 0
                {
                    let request = request(
                        9999,
                        Operation::Read(ReadRequest {
                            consistency: ReadConsistency::Linearizable,
                            query: ReadQuery::Objects(vec![]),
                            max_items: 1,
                        }),
                    );
                    if matches!(exchange(fleet, index, &request).await, Response::Read(_)) {
                        return index;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn install_support(fleet: &Fleet, leader: usize) {
    // Only an actual managed demand begins this upgrade. Its authenticated
    // current-member probes make the other voters durably promise support.
    for target in 1..=3 {
        if target == leader as u64 + 1 {
            continue;
        }
        let request = managed_request(
            450 + u128::from(target),
            Operation::ManagedSupport { group: [8; 16] },
        );
        fleet.replicas[leader]
            .pool
            .send_managed_support(target, &request)
            .await
            .unwrap();
    }

    for (index, replica) in fleet.replicas.iter().enumerate() {
        for target in 1..=3 {
            if target == index as u64 + 1 {
                continue;
            }
            let request = managed_request(
                500 + u128::from(target),
                Operation::ManagedSupport { group: [8; 16] },
            );
            let fact = replica
                .pool
                .send_managed_support(target, &request)
                .await
                .unwrap();
            assert_eq!(fact.node, target);
            replica
                .host
                .record_managed_support(target, fact)
                .await
                .unwrap();
        }
        assert!(
            replica
                .host
                .managed_support()
                .await
                .unwrap()
                .targets()
                .next()
                .is_none()
        );
    }
}
fn new_claim() -> NewClaim {
    let validation = NewValidation {
        id: ValidationId::from_u128(702),
        content: ValidationContent {
            ledger: ledger(),
            schema: 1,
            claim: ClaimId::from_u128(700),
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            description: "receipt proof".into(),
            quality_bar: None,
            evaluator: ParticipantId::from_u128(998),
            handlers: Vec::new(),
            evidence_schemas: BTreeSet::new(),
            contributed_by: BTreeSet::from([ParticipantId::from_u128(999)]),
            policy_revision: 1,
        },
    };
    NewClaim {
        id: ClaimId::from_u128(700),
        content: ClaimContent {
            ledger: ledger(),
            schema: 1,
            occurrence: OccurrenceId::from_u128(703),
            description: "managed namespace survives durable reply loss".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ParticipantId::from_u128(999)),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(ParticipantId::from_u128(997)),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(3)),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: validation.id,
                specification: validation.content.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![validation],
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_mtls_support_domain_cursor_retirement_quorum_and_disk_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let mut fleet = Fleet::open(directory.path(), true).await;
    let leader = current_leader(&fleet, None).await;
    let register = managed_request(
        100,
        Operation::RequestStreamControl {
            cluster: [7; 16],
            command: RequestStreamCommand::Register {
                slot: 0,
                expected_generation: 0,
                owner: RequestId([1; 16]),
                window: 4,
            },
        },
    );
    assert!(matches!(
        exchange(&fleet, leader, &register).await,
        Response::Error(AccessError::OutcomeUnknown)
    ));
    let probe = managed_request(101, Operation::ManagedSupport { group: [8; 16] });
    assert!(matches!(
        exchange(&fleet, leader, &probe).await,
        Response::Error(AccessError::Unauthorized)
    ));
    install_support(&fleet, leader).await;
    let Response::RequestStreamControlled(registered) = exchange(&fleet, leader, &register).await
    else {
        panic!("registration")
    };
    let RequestStreamControlOutcome::Registered(RequestStreamState::Active { stream, .. }) =
        registered.receipt.outcome
    else {
        panic!("active")
    };
    let key = ManagedRequestKey {
        stream,
        ordinal: 1,
        id: RequestId::from_u128(102),
    };
    let mutation = managed_request(
        102,
        Operation::Managed {
            key,
            operation: ManagedOperation::Submit {
                expected_revision: None,
                command: Command::GenerateClaim { claim: new_claim() },
            },
        },
    );
    let response = exchange(&fleet, leader, &mutation).await;
    let Response::Managed(domain) = response else {
        panic!("domain: {response:?}")
    };
    assert_eq!(domain.receipt.sequence, SessionSeq(1));
    assert!(matches!(
        domain.receipt.outcome,
        ManagedReceiptOutcome::Domain(CommandResult::Generated(_))
    ));
    fleet.all_at(SessionSeq(1)).await;
    let cursor_key = ManagedRequestKey {
        stream,
        ordinal: 2,
        id: RequestId::from_u128(103),
    };
    let cursor = managed_request(
        103,
        Operation::Managed {
            key: cursor_key,
            operation: ManagedOperation::Cursor(StreamRequest::Open {
                consumer: ConsumerId([7; 16]),
                filter: DeltaFilter::All,
                start: None,
                seed: false,
                credits: Credits {
                    items: 4,
                    bytes: 4096,
                },
            }),
        },
    );
    let Response::Managed(cursor_receipt) = exchange(&fleet, leader, &cursor).await else {
        panic!("cursor")
    };
    assert!(cursor_receipt.stream.is_some());
    assert_eq!(cursor_receipt.receipt.sequence, domain.receipt.sequence);
    assert!(cursor_receipt.receipt.raft_index > domain.receipt.raft_index);
    let lookup = managed_request(
        104,
        Operation::RequestStreamRead {
            cluster: [7; 16],
            query: RequestStreamQuery::Receipt { key: cursor_key },
        },
    );
    let Response::RequestStreamRead(found) = exchange(&fleet, leader, &lookup).await else {
        panic!("read")
    };
    assert!(
        matches!(found.page.result,RequestStreamReadResult::Receipt{resolution:ManagedReceiptResolution::Retained(ref receipt),..} if **receipt==cursor_receipt.receipt)
    );
    fleet.isolate(leader);
    assert!(matches!(
        exchange(&fleet, leader, &lookup).await,
        Response::Error(AccessError::Unavailable)
    ));
    let next = current_leader(&fleet, Some(leader)).await;
    let Response::Managed(retried) = exchange(&fleet, next, &mutation).await else {
        panic!("retry")
    };
    assert_eq!(retried.receipt, domain.receipt);
    let slot = managed_request(
        105,
        Operation::RequestStreamRead {
            cluster: [7; 16],
            query: RequestStreamQuery::Slot { slot: 0 },
        },
    );
    let Response::RequestStreamRead(found) = exchange(&fleet, next, &slot).await else {
        panic!("slot")
    };
    let RequestStreamReadResult::Slot(RequestStreamState::Active { revision, .. }) =
        found.page.result
    else {
        panic!("revision")
    };
    let ack = managed_request(
        106,
        Operation::RequestStreamControl {
            cluster: [7; 16],
            command: RequestStreamCommand::Acknowledge {
                stream,
                expected_revision: revision,
                through: 2,
                receipts: vec![
                    ManagedReceiptAck {
                        key,
                        receipt_hash: domain.receipt.content_hash().unwrap(),
                    },
                    ManagedReceiptAck {
                        key: cursor_key,
                        receipt_hash: cursor_receipt.receipt.content_hash().unwrap(),
                    },
                ],
            },
        },
    );
    let Response::RequestStreamControlled(acknowledged) = exchange(&fleet, next, &ack).await else {
        panic!("acknowledgment")
    };
    assert!(matches!(
        exchange(&fleet, next, &mutation).await,
        Response::Error(AccessError::ManagedRetired { through: 2 })
    ));
    let Response::RequestStreamRead(retired) = exchange(&fleet, next, &lookup).await else {
        panic!("retiredread")
    };
    assert!(matches!(
        retired.page.result,
        RequestStreamReadResult::Receipt {
            resolution: ManagedReceiptResolution::Retired { through: 2 },
            ..
        }
    ));
    fleet.stop().await;
    let mut reopened = Fleet::open(directory.path(), true).await;
    let owner = current_leader(&reopened, None).await;
    // The durable decoder promises and committed activation survive restart.
    // One voter is unreachable and its process-local support cache is empty;
    // exact retries and NEW writes still need only the normal healthy quorum.
    let offline = (owner + 1) % 3;
    reopened.isolate(offline);
    let owner = current_leader(&reopened, Some(offline)).await;
    assert!(
        reopened.replicas[owner]
            .host
            .managed_support()
            .await
            .unwrap()
            .targets()
            .next()
            .is_none()
    );
    let Response::RequestStreamControlled(retried_ack) = exchange(&reopened, owner, &ack).await
    else {
        panic!("recovered control retry")
    };
    assert_eq!(retried_ack.receipt, acknowledged.receipt);
    let Response::RequestStreamRead(retired) = exchange(&reopened, owner, &lookup).await else {
        panic!("recoveredread")
    };
    assert!(matches!(
        retired.page.result,
        RequestStreamReadResult::Receipt {
            resolution: ManagedReceiptResolution::Retired { through: 2 },
            ..
        }
    ));
    assert!(matches!(
        exchange(&reopened, owner, &mutation).await,
        Response::Error(AccessError::ManagedRetired { through: 2 })
    ));
    let mut second_claim = new_claim();
    second_claim.id = ClaimId::from_u128(710);
    second_claim.content.occurrence = OccurrenceId::from_u128(713);
    for validation in &mut second_claim.validations {
        validation.id = ValidationId::from_u128(712);
        validation.content.claim = second_claim.id;
    }
    second_claim.content.requirements = second_claim
        .validations
        .iter()
        .map(|validation| RequirementRef {
            id: validation.id,
            specification: validation.content.specification_hash().unwrap(),
        })
        .collect();
    let next_mutation = managed_request(
        107,
        Operation::Managed {
            key: ManagedRequestKey {
                stream,
                ordinal: 3,
                id: RequestId::from_u128(107),
            },
            operation: ManagedOperation::Submit {
                expected_revision: None,
                command: Command::GenerateClaim {
                    claim: second_claim,
                },
            },
        },
    );
    let response = exchange(&reopened, owner, &next_mutation).await;
    let Response::Managed(committed) = response else {
        panic!("new request after restart with one voter offline: {response:?}")
    };
    assert_eq!(committed.receipt.sequence, SessionSeq(2));
    assert!(matches!(
        committed.receipt.outcome,
        ManagedReceiptOutcome::Domain(CommandResult::Generated(_))
    ));
    for (ordinal, id, command) in [
        (
            4,
            108,
            Command::PostClaim {
                claim: ClaimId::from_u128(9999),
            },
        ),
        (
            5,
            109,
            Command::AcquireReceipt {
                claim: ClaimId::from_u128(700),
                receipt: ReceiptId::from_u128(1001),
                epoch: 1,
            },
        ),
    ] {
        let key = ManagedRequestKey {
            stream,
            ordinal,
            id: RequestId::from_u128(id),
        };
        let request = managed_request(
            id,
            Operation::Managed {
                key,
                operation: ManagedOperation::Submit {
                    expected_revision: None,
                    command,
                },
            },
        );
        let result = exchange(&reopened, owner, &request).await;
        match result {
            Response::Submitted(MutationReply::Domain(DomainOutcome::Refuse {
                code: ErrorCode::UnknownObject,
                ..
            })) if ordinal == 4 => {}
            Response::Submitted(MutationReply::Domain(DomainOutcome::Inform {
                reason: InformReason::Status(ClaimStatus::Generated),
                ..
            })) if ordinal == 5 => {}
            other => panic!("transient managed domain response: {other:?}"),
        }
        let lookup = managed_request(
            id + 100,
            Operation::RequestStreamRead {
                cluster: [7; 16],
                query: RequestStreamQuery::Receipt { key },
            },
        );
        let Response::RequestStreamRead(unknown) = exchange(&reopened, owner, &lookup).await else {
            panic!("fresh managed lookup")
        };
        assert_eq!(unknown.page.sequence, SessionSeq(2));
        assert!(matches!(
            unknown.page.result,
            RequestStreamReadResult::Receipt {
                resolution: ManagedReceiptResolution::Unknown,
                ..
            }
        ));
    }
    reopened.stop().await;
}
