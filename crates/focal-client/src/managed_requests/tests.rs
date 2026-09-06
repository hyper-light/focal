use super::*;
use crate::operation_store::OperationIntent;
use focal_wire::{ManagedOperation, ReadToken, RequestStreamControlReply, RequestStreamReadReply};
fn context() -> OperationContext {
    OperationContext {
        cluster: [1; 16],
        principal: ParticipantId::from_u128(2),
        ledger: LedgerId {
            tenant: TenantId::from_u128(3),
            session: SessionId::from_u128(4),
        },
    }
}
fn limits() -> ManagedStoreLimits {
    ManagedStoreLimits {
        window: 2,
        ..ManagedStoreLimits::default()
    }
}
fn ids(start: u128) -> impl IdGenerator {
    let mut next = start;
    move || {
        next += 1;
        Ok(next.to_be_bytes())
    }
}
fn open(parent: &Path) -> ManagedRequests {
    ManagedRequests::open(parent, "CLI.requests", context(), limits()).unwrap()
}
fn token(request: &RequestEnvelope) -> ReadToken {
    ReadToken {
        ledger: request.ledger,
        sequence: SessionSeq(1),
        route_epoch: request.route_epoch,
    }
}
fn read_reply(request: &RequestEnvelope, state: RequestStreamState) -> ResponseEnvelope {
    request.reply(Response::RequestStreamRead(RequestStreamReadReply {
        token: token(request),
        page: RequestStreamRead {
            schema: MANAGED_REQUEST_SCHEMA,
            cluster: context().cluster,
            ledger: context().ledger,
            principal: context().principal,
            sequence: SessionSeq(1),
            raft_index: 20,
            result: RequestStreamReadResult::Slot(state),
        },
    }))
}
fn registration_state(request: &RequestEnvelope) -> RequestStreamState {
    let Operation::RequestStreamControl {
        command:
            RequestStreamCommand::Register {
                slot,
                expected_generation,
                owner,
                window,
            },
        ..
    } = request.operation
    else {
        panic!("register")
    };
    RequestStreamState::Active {
        stream: RequestStreamIdentity {
            cluster: context().cluster,
            ledger: context().ledger,
            principal: context().principal,
            slot,
            generation: expected_generation + 1,
        },
        owner,
        revision: 1,
        window,
        acknowledged_through: 0,
    }
}
fn control_reply(request: &RequestEnvelope, index: u64) -> ResponseEnvelope {
    let Operation::RequestStreamControl { cluster, command } = &request.operation else {
        panic!("control")
    };
    let outcome = match command {
        RequestStreamCommand::Register { .. } => {
            RequestStreamControlOutcome::Registered(registration_state(request))
        }
        RequestStreamCommand::Acknowledge {
            stream,
            expected_revision,
            through,
            ..
        } => RequestStreamControlOutcome::Acknowledged {
            stream: *stream,
            revision: expected_revision + 1,
            through: *through,
        },
        _ => panic!("test control"),
    };
    request.reply(Response::RequestStreamControlled(
        RequestStreamControlReply {
            token: token(request),
            receipt: RequestStreamControlReceipt {
                cluster: *cluster,
                ledger: request.ledger,
                principal: context().principal,
                id: request.request_id,
                intent_hash: request_stream_control_hash(
                    *cluster,
                    request.ledger,
                    context().principal,
                    command,
                )
                .unwrap(),
                raft_index: index,
                outcome,
            },
        },
    ))
}
fn register(owner: &ManagedRequests, ids: &mut impl IdGenerator) -> RequestEnvelope {
    let read = owner.maintenance(ids).unwrap().unwrap();
    assert!(matches!(
        read.operation,
        Operation::RequestStreamRead {
            query: RequestStreamQuery::Slot { slot: 0 },
            ..
        }
    ));
    owner
        .accept_maintenance(
            &read,
            read_reply(
                &read,
                RequestStreamState::Vacant {
                    slot: 0,
                    generation: 0,
                },
            ),
        )
        .unwrap();
    let request = owner.maintenance(ids).unwrap().unwrap();
    owner
        .accept_maintenance(&request, control_reply(&request, 10))
        .unwrap();
    assert!(owner.maintenance(ids).unwrap().is_none());
    request
}
fn complete(store: &ManagedOperationStore, request_id: u128) -> ManagedOperationId {
    let id = store.reserve(RequestId::from_u128(request_id)).unwrap();
    let prepared = store
        .prepare(
            id,
            OperationIntent {
                name: "claim.post",
                version: 1,
                canonical: b"{}",
            },
            |key| {
                Ok(RequestEnvelope {
                    protocol: focal_wire::MANAGED_PROTOCOL_VERSION,
                    ledger: key.stream.ledger,
                    route_epoch: RouteEpoch(1),
                    request_epoch: RequestEpoch(1),
                    request_id: key.id,
                    operation: Operation::Managed {
                        key,
                        operation: ManagedOperation::Submit {
                            expected_revision: None,
                            command: Command::PostClaim {
                                claim: ClaimId::from_u128(1),
                            },
                        },
                    },
                })
            },
        )
        .unwrap();
    let (key, _, intent_hash) = focal_wire::managed_request_identity(&prepared.request).unwrap();
    store
        .record_receipt(
            id,
            &ManagedReceipt {
                key,
                sequence: SessionSeq(1),
                raft_index: 11,
                intent_hash,
                outcome: ManagedReceiptOutcome::Domain(CommandResult::Noop),
            },
        )
        .unwrap();
    id
}

#[test]
fn first_use_is_two_roundtrips_and_lost_registration_retries_exactly() {
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    let read = owner.maintenance(&mut entropy).unwrap().unwrap();
    assert_eq!(
        open(dir.path())
            .maintenance(&mut ids(200))
            .unwrap()
            .unwrap(),
        read
    );
    owner
        .accept_maintenance(
            &read,
            read_reply(
                &read,
                RequestStreamState::Vacant {
                    slot: 0,
                    generation: 0,
                },
            ),
        )
        .unwrap();
    let register = owner.maintenance(&mut entropy).unwrap().unwrap();
    assert!(matches!(owner.store(), Err(ManagedRequestsError::NotReady)));
    owner
        .accept_maintenance(
            &register,
            register.reply(Response::Error(AccessError::OutcomeUnknown)),
        )
        .unwrap_err();
    let reopened = open(dir.path());
    assert_eq!(
        reopened.maintenance(&mut ids(300)).unwrap().unwrap(),
        register
    );
    let response = control_reply(&register, 10);
    reopened
        .accept_maintenance(&register, response.clone())
        .unwrap();
    assert!(reopened.maintenance(&mut entropy).unwrap().is_none());
    reopened.accept_maintenance(&register, response).unwrap();
    assert_eq!(reopened.store().unwrap().status().unwrap().stream.slot, 0);
}

#[test]
fn registration_conflict_requires_generation_fence_and_never_adopts_public_owner() {
    let dir = private_dir();
    let a = open(dir.path());
    let b = ManagedRequests::open(dir.path(), "MCP.requests", context(), limits()).unwrap();
    let mut ia = ids(100);
    let mut ib = ids(200);
    let qa = a.maintenance(&mut ia).unwrap().unwrap();
    let qb = b.maintenance(&mut ib).unwrap().unwrap();
    for (owner, q) in [(&a, &qa), (&b, &qb)] {
        owner
            .accept_maintenance(
                q,
                read_reply(
                    q,
                    RequestStreamState::Vacant {
                        slot: 0,
                        generation: 0,
                    },
                ),
            )
            .unwrap();
    }
    let ra = a.maintenance(&mut ia).unwrap().unwrap();
    let rb = b.maintenance(&mut ib).unwrap().unwrap();
    a.accept_maintenance(&ra, control_reply(&ra, 10)).unwrap();
    a.maintenance(&mut ia).unwrap();
    b.accept_maintenance(&rb, rb.reply(Response::Error(AccessError::ManagedConflict)))
        .unwrap();
    let probe = b.maintenance(&mut ib).unwrap().unwrap();
    // Even a linearizable vacant read cannot retract an unknown registration.
    b.accept_maintenance(
        &probe,
        read_reply(
            &probe,
            RequestStreamState::Vacant {
                slot: 0,
                generation: 0,
            },
        ),
    )
    .unwrap();
    assert_eq!(b.maintenance(&mut ib).unwrap().unwrap(), rb);
    b.accept_maintenance(&rb, rb.reply(Response::Error(AccessError::ManagedConflict)))
        .unwrap();
    let probe = b.maintenance(&mut ib).unwrap().unwrap();
    b.accept_maintenance(&probe, read_reply(&probe, registration_state(&ra)))
        .unwrap();
    let scan = b.maintenance(&mut ib).unwrap().unwrap();
    b.accept_maintenance(&scan, read_reply(&scan, registration_state(&ra)))
        .unwrap();
    let next = b.maintenance(&mut ib).unwrap().unwrap();
    assert!(matches!(
        next.operation,
        Operation::RequestStreamRead {
            query: RequestStreamQuery::Slot { slot: 1 },
            ..
        }
    ));
    assert!(matches!(b.store(), Err(ManagedRequestsError::NotReady)));
}

#[test]
fn undelivered_prefix_blocks_ack_and_lost_ack_never_forgets_saved_outcomes() {
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    register(&owner, &mut entropy);
    let store = owner.store().unwrap();
    let first = complete(&store, 1);
    let second = complete(&store, 2);
    owner.mark_delivered(second).unwrap();
    assert!(owner.maintenance(&mut entropy).unwrap().is_none());
    assert_eq!(store.status().unwrap().retired_through, 0);
    open(dir.path()).mark_delivered(first).unwrap();
    let ack = owner.maintenance(&mut entropy).unwrap().unwrap();
    assert!(
        matches!(&ack.operation,Operation::RequestStreamControl{command:RequestStreamCommand::Acknowledge{through:2,receipts,..},..}if receipts.len()==2)
    );
    assert!(store.receipt(first).unwrap().is_some());
    let reopened = open(dir.path());
    assert_eq!(reopened.maintenance(&mut ids(999)).unwrap().unwrap(), ack);
    let response = control_reply(&ack, 20);
    reopened.accept_maintenance(&ack, response.clone()).unwrap();
    assert!(reopened.maintenance(&mut entropy).unwrap().is_none());
    reopened.accept_maintenance(&ack, response).unwrap();
    assert!(matches!(
        store.request(first),
        Err(ManagedStoreError::Retired)
    ));
    assert!(matches!(
        store.request(second),
        Err(ManagedStoreError::Retired)
    ));
    assert_eq!(
        std::fs::read_dir(dir.path().join("CLI.requests"))
            .unwrap()
            .count(),
        3
    );
}

#[test]
fn committed_child_initialization_resumes_but_ready_child_loss_never_reinitializes() {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    let read = owner.maintenance(&mut entropy).unwrap().unwrap();
    owner
        .accept_maintenance(
            &read,
            read_reply(
                &read,
                RequestStreamState::Vacant {
                    slot: 0,
                    generation: 0,
                },
            ),
        )
        .unwrap();
    let request = owner.maintenance(&mut entropy).unwrap().unwrap();
    owner
        .accept_maintenance(&request, control_reply(&request, 10))
        .unwrap();
    let child = dir.path().join("CLI.requests");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&child)
        .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(child.join("LOCK"))
        .unwrap()
        .sync_all()
        .unwrap();
    let reopened = open(dir.path());
    assert!(reopened.maintenance(&mut entropy).unwrap().is_none());
    assert!(reopened.store().unwrap().status().unwrap().registered);
    std::fs::remove_dir_all(&child).unwrap();
    assert!(ManagedRequests::open(dir.path(), "CLI.requests", context(), limits()).is_err());
    assert!(!child.exists());
}

#[test]
fn missing_state_marker_context_and_tampered_receipt_fail_closed() {
    let dir = private_dir();
    assert!(matches!(
        ManagedRequests::open_existing(dir.path(), "CLI.requests", context(), limits()),
        Err(ManagedRequestsError::Missing)
    ));
    let owner = open(dir.path());
    let mut entropy = ids(10);
    let request = owner.maintenance(&mut entropy).unwrap().unwrap();
    let mut reply = read_reply(
        &request,
        RequestStreamState::Vacant {
            slot: 0,
            generation: 0,
        },
    );
    if let Response::RequestStreamRead(r) = &mut reply.result {
        r.page.principal = ParticipantId::from_u128(999);
    }
    assert!(matches!(
        owner.accept_maintenance(&request, reply),
        Err(ManagedRequestsError::InvalidResponse)
    ));
    let wrong = OperationContext {
        cluster: [7; 16],
        ..context()
    };
    assert!(ManagedRequests::open(dir.path(), "CLI.requests", wrong, limits()).is_err());
    std::fs::remove_file(dir.path().join("CLI.requests.managed-owner")).unwrap();
    assert!(ManagedRequests::open(dir.path(), "CLI.requests", context(), limits()).is_err());
}

#[test]
fn generation_exhaustion_skips_without_wrap_and_discovery_is_bounded() {
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    for slot in 0..SCAN {
        let read = owner.maintenance(&mut entropy).unwrap().unwrap();
        owner
            .accept_maintenance(
                &read,
                read_reply(
                    &read,
                    RequestStreamState::Vacant {
                        slot,
                        generation: u64::MAX,
                    },
                ),
            )
            .unwrap();
    }
    assert!(matches!(
        owner.maintenance(&mut entropy),
        Err(ManagedRequestsError::Exhausted)
    ));
    assert!(matches!(owner.store(), Err(ManagedRequestsError::NotReady)));
}

#[test]
fn route_refresh_binds_token_principal_and_original_control_payload() {
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    let request = owner.maintenance(&mut entropy).unwrap().unwrap();
    let mut routed = request.clone();
    routed.route_epoch = RouteEpoch(2);
    owner
        .accept_maintenance(
            &request,
            read_reply(
                &routed,
                RequestStreamState::Vacant {
                    slot: 0,
                    generation: 0,
                },
            ),
        )
        .unwrap();
    let registration = owner.maintenance(&mut entropy).unwrap().unwrap();
    assert!(matches!(
        registration.operation,
        Operation::RequestStreamControl { .. }
    ));
}

fn private_dir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    dir
}

#[test]
fn ack_crash_between_child_publication_and_coordinator_publication_recovers() {
    let dir = private_dir();
    let owner = open(dir.path());
    let mut entropy = ids(100);
    register(&owner, &mut entropy);
    let store = owner.store().unwrap();
    let id = complete(&store, 1);
    owner.mark_delivered(id).unwrap();
    // A child ACK can be durable before the coordinator saves its envelope.
    let child_control = store
        .prepare_acknowledgment(RequestId::from_u128(777), 1)
        .unwrap();
    let request = open(dir.path()).maintenance(&mut entropy).unwrap().unwrap();
    assert_eq!(request.request_id, child_control.id);
    let response = control_reply(&request, 20);
    let Response::RequestStreamControlled(reply) = &response.result else {
        panic!("control")
    };
    store.record_control(reply.receipt.clone()).unwrap(); // crash before outer update
    let reopened = open(dir.path());
    assert_eq!(
        reopened.maintenance(&mut entropy).unwrap().unwrap(),
        request
    );
    reopened.accept_maintenance(&request, response).unwrap();
    assert!(reopened.maintenance(&mut entropy).unwrap().is_none());
    assert!(matches!(store.request(id), Err(ManagedStoreError::Retired)));
}

#[test]
fn only_pre_network_initialization_can_repair_a_partial_external_marker() {
    let dir = private_dir();
    open(dir.path());
    let marker = dir.path().join("CLI.requests.managed-lock");
    std::fs::write(&marker, b"FCLM").unwrap();
    let owner = open(dir.path());
    assert_eq!(std::fs::read(&marker).unwrap(), MAGIC);
    owner.maintenance(&mut ids(100)).unwrap();
    std::fs::write(&marker, b"FCLM").unwrap();
    assert!(ManagedRequests::open(dir.path(), "CLI.requests", context(), limits()).is_err());
    assert_eq!(std::fs::read(&marker).unwrap(), b"FCLM");
}

#[test]
fn concurrent_coordinator_child() {
    let Ok(parent) = std::env::var("FOCAL_COORDINATOR_CHILD") else {
        return;
    };
    let output = std::env::var("FOCAL_COORDINATOR_OUTPUT").unwrap();
    let mut entropy = ids(output.parse::<u128>().unwrap() * 1000);
    let mut result = None;
    for _ in 0..500 {
        let attempt =
            ManagedRequests::open(Path::new(&parent), "CLI.requests", context(), limits())
                .and_then(|owner| owner.maintenance(&mut entropy));
        match attempt {
            Ok(Some(request)) => {
                result = Some(request);
                break;
            }
            Err(ManagedRequestsError::Store(StoreError::Locked)) => {
                std::thread::sleep(std::time::Duration::from_millis(2))
            }
            other => panic!("unexpected concurrent initialization: {other:?}"),
        }
    }
    let request = result.expect("short locked operation completes");
    std::fs::write(
        Path::new(&parent).join(format!("result-{output}")),
        postcard::to_stdvec(&request).unwrap(),
    )
    .unwrap();
}

#[test]
fn concurrent_processes_adopt_one_exact_registration_discovery_intent() {
    let dir = private_dir();
    let exe = std::env::current_exe().unwrap();
    let mut children = Vec::new();
    for child in ["1", "2"] {
        children.push(
            std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "managed_requests::tests::concurrent_coordinator_child",
                    "--nocapture",
                ])
                .env("FOCAL_COORDINATOR_CHILD", dir.path())
                .env("FOCAL_COORDINATOR_OUTPUT", child)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    for mut child in children {
        assert!(child.wait().unwrap().success());
    }
    let a = std::fs::read(dir.path().join("result-1")).unwrap();
    let b = std::fs::read(dir.path().join("result-2")).unwrap();
    assert_eq!(a, b);
    let pending = open(dir.path())
        .maintenance(&mut ids(9000))
        .unwrap()
        .unwrap();
    assert_eq!(postcard::to_stdvec(&pending).unwrap(), a);
}
