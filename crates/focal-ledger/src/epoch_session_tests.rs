fn independent_epoch_requests(count: usize) -> Vec<AuthenticatedInput> {
    (0..count)
        .map(|index| {
            let mut request = input(
                index as u128 + 1,
                Command::NegotiateEpoch {
                    epoch: RequestEpoch(1),
                },
            );
            request.principal = ParticipantId::from_u128(index as u128 + 100);
            request
        })
        .collect()
}

#[test]
fn populated_core_completion_needs_bounded_staging_under_ordinary_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SessionLimits {
        memory_bytes: 128 * 1024 * 1024,
        completion_reserve_bytes: 8 * 1024 * 1024,
        ..SessionLimits::default()
    };
    let mut session = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut session);
    session
        .submit_local(&input(
            1,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        ))
        .unwrap();
    for index in 0..24u128 {
        let validation = NewValidation {
            id: ValidationId::from_u128(index + 1000),
            content: ValidationContent {
                ledger: identity(),
                schema: 1,
                claim: ClaimId::from_u128(index + 100),
                kind: ValidationKind::Receipt,
                phase: ValidationPhase::WholeWork,
                mode: ValidationMode::Required,
                description: "receipt".into(),
                quality_bar: None,
                evaluator: ParticipantId::from_u128(1),
                handlers: Vec::new(),
                evidence_schemas: Default::default(),
                contributed_by: [ParticipantId::from_u128(1)].into_iter().collect(),
                policy_revision: 1,
            },
        };
        let claim = NewClaim {
            id: ClaimId::from_u128(index + 100),
            content: ClaimContent {
                ledger: identity(),
                schema: 1,
                occurrence: OccurrenceId::from_u128(index + 100),
                description: "x".repeat(8192),
                relations: [
                    Relation {
                        kind: RelationKind::Issuer,
                        target: RelationTarget::Participant(ParticipantId::from_u128(1)),
                    },
                    Relation {
                        kind: RelationKind::Subject,
                        target: RelationTarget::Participant(ParticipantId::from_u128(2)),
                    },
                    Relation {
                        kind: RelationKind::ClaimAction,
                        target: RelationTarget::Action(ActionType::Work),
                    },
                    Relation {
                        kind: RelationKind::CausedBy,
                        target: RelationTarget::Root(RootCommandId::from_u128(1)),
                    },
                ]
                .into_iter()
                .collect(),
                scopes: Default::default(),
                requirements: vec![RequirementRef {
                    id: validation.id,
                    specification: validation.content.specification_hash().unwrap(),
                }],
                deadline: None,
            },
            validations: vec![validation],
        };
        let request = input(index + 100, Command::GenerateClaim { claim });
        let result = session.submit_local(&request).unwrap();
        assert!(matches!(result, Submission::Committed(_)), "{result:?}");
    }
    assert!(
        reference_charge(session.core.snapshot()).unwrap()
            > session.limits.completion_reserve_bytes
    );
    assert!(session.staging_bytes() < session.limits.completion_reserve_bytes);
    let stats = session.memory_stats();
    let pressure = session
        .budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Ordinary,
            stats.limit - stats.completion_reserve - stats.ordinary_used,
        )
        .unwrap();
    let mut completion = input(
        500,
        Command::NegotiateEpoch {
            epoch: RequestEpoch(2),
        },
    );
    completion.request_epoch = RequestEpoch(2);
    assert!(matches!(
        session.submit_local(&completion).unwrap(),
        Submission::Committed(_)
    ));
    assert_eq!(session.sequence(), SessionSeq(26));
    assert!(session.pending_rows.is_empty());
    assert!(session.apply_workspace.is_none());
    drop(pressure);
    session.audit_graph().unwrap();
}
#[test]
fn default_session_executes_epoch_workers_and_forced_audit_fallback_matches_receipts() {
    for fallback in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut limits = SessionLimits::default();
        limits.apply.max_workers = 4;
        let mut session = Session::open(dir.path(), identity(), config(), limits.clone()).unwrap();
        elect(&mut session);
        session.omit_epoch_declarations = fallback;
        let mut oracle = Core::new(identity(), limits.core);
        let requests = independent_epoch_requests(8);
        let mut expected = Vec::new();
        for request in &requests {
            let prepared = oracle.prepare(request).unwrap();
            expected.push(
                oracle
                    .apply_serial(SessionSeq(oracle.sequence().0 + 1), prepared)
                    .unwrap(),
            );
            assert!(matches!(
                session.propose(request).unwrap(),
                Submission::Pending(_)
            ));
        }
        assert_eq!(session.pending_rows.len(), 8);
        assert_eq!(session.sequence(), SessionSeq(0));
        let events = session.poll().unwrap();
        assert_eq!(events.committed, expected);
        let report = session.last_epoch_report().unwrap();
        assert_eq!(report.commands, 8);
        assert_eq!(report.serial_fallback, fallback);
        if !fallback {
            assert_eq!(report.max_parallel, 4);
        }
        assert_eq!(
            session.core.normalized_bytes().unwrap(),
            oracle.normalized_bytes().unwrap()
        );
        assert!(session.pending_rows.is_empty());
        session.audit_graph().unwrap();
        assert_eq!(
            session.core_charge.bytes() + session.core_completion_charge.bytes(),
            reference_charge(session.core.snapshot()).unwrap()
        );
    }
}
#[test]
fn corrupt_later_epoch_input_and_graph_provenance_never_publish_an_earlier_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let requests = independent_epoch_requests(2);
    for request in &requests {
        session.propose(request).unwrap();
    }
    let before = session.core.normalized_bytes().unwrap();
    let mut events = session.consensus.drain().unwrap();
    assert_eq!(events.committed.len(), 2);
    let mut bad: PreparedMutation =
        postcard::from_bytes(events.committed[1].data.strip_prefix(ENTRY_MAGIC).unwrap()).unwrap();
    bad.base = SessionSeq(99);
    events.committed[1].data = ENTRY_MAGIC.to_vec();
    events.committed[1]
        .data
        .extend(postcard::to_stdvec(&bad).unwrap());
    assert!(session.apply_events(events).is_err());
    assert_eq!(session.core.normalized_bytes().unwrap(), before);
    assert_eq!(session.graph_sequence(), SessionSeq(0));
    session.clear_pending();
    session.release_empty_slots().unwrap();
    let root = Core::new(identity(), session.limits.core.clone());
    let mut pending = PendingState::new();
    pending.reserve(1).unwrap();
    let stage = root.stage_pending(&pending, &requests[0]).unwrap();
    let foreign = GraphStore::from_state(
        root.snapshot(),
        RangeId(999),
        GraphConfig::default(),
        session.budget.clone(),
    )
    .unwrap();
    let bad_graph = foreign
        .prepare_patch(root.view(), stage.patch(), None, BudgetLane::Completion)
        .unwrap();
    assert!(session.graph.validate_publication([&bad_graph]).is_err());
    assert_eq!(session.core.normalized_bytes().unwrap(), before);
}
#[test]
fn epoch_workspace_is_reserved_before_consensus_and_failed_staging_reclaims_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut limits = SessionLimits::default();
    limits.apply.max_bytes = 1;
    let mut session = Session::open(dir.path(), identity(), config(), limits).unwrap();
    elect(&mut session);
    let baseline = session.memory_stats().used;
    let index = session.status().committed_index;
    assert!(matches!(
        session.propose(&independent_epoch_requests(1)[0]),
        Err(LedgerError::Capacity)
    ));
    assert_eq!(session.pending_count(), 0);
    assert!(session.pending_rows.is_empty());
    assert_eq!(session.status().committed_index, index);
    assert_eq!(session.memory_stats().used, baseline);
}
#[test]
fn quorum_epoch_results_and_restarted_followers_match_the_serial_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let limits = SessionLimits::default();
    let mut sessions = Vec::new();
    let configs = (1..=3)
        .map(|id| {
            let mut config = NodeConfig::single(id, [9; 16], identity().session.0);
            config.voters = vec![1, 2, 3];
            config
        })
        .collect::<Vec<_>>();
    for config in &configs {
        sessions.push(
            Session::open(
                dir.path().join(config.node_id.to_string()),
                identity(),
                config.clone(),
                limits.clone(),
            )
            .unwrap(),
        );
    }
    sessions[0].campaign().unwrap();
    pump_sessions(&mut sessions);
    let requests = independent_epoch_requests(6);
    let mut oracle = Core::new(identity(), limits.core.clone());
    for request in &requests {
        let prepared = oracle.prepare(request).unwrap();
        oracle
            .apply_serial(SessionSeq(oracle.sequence().0 + 1), prepared)
            .unwrap();
        sessions[0].propose(request).unwrap();
    }
    assert_eq!(sessions[0].sequence(), SessionSeq(0));
    pump_sessions(&mut sessions);
    assert!(sessions[0].last_epoch_report().unwrap().max_parallel > 1);
    for session in &sessions {
        assert_eq!(
            session.core.normalized_bytes().unwrap(),
            oracle.normalized_bytes().unwrap()
        );
        session.audit_graph().unwrap();
    }
    drop(sessions);
    for config in configs {
        let session = Session::open(
            dir.path().join(config.node_id.to_string()),
            identity(),
            config,
            limits.clone(),
        )
        .unwrap();
        assert_eq!(
            session.core.normalized_bytes().unwrap(),
            oracle.normalized_bytes().unwrap()
        );
        session.audit_graph().unwrap();
    }
}

#[test]
fn nonblocking_poll_keeps_pending_rows_until_the_same_epoch_is_published() {
    let dir = tempfile::tempdir().unwrap();
    let mut session =
        Session::open(dir.path(), identity(), config(), SessionLimits::default()).unwrap();
    elect(&mut session);
    let requests = independent_epoch_requests(3);
    for request in &requests {
        assert!(matches!(
            session.propose(request).unwrap(),
            Submission::Pending(_)
        ));
    }
    let mut committed = Vec::new();
    for _ in 0..1000 {
        match session.try_poll().unwrap() {
            None => {
                assert_eq!(session.sequence(), SessionSeq(0));
                assert_eq!(session.pending_rows.len(), 3);
                assert!(session.apply_workspace.is_some());
                assert!(session.persistence_pending());
            }
            Some(events) => committed.extend(events.committed),
        }
        if session.sequence() == SessionSeq(3) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(session.sequence(), SessionSeq(3));
    assert_eq!(committed.len(), 3);
    assert!(session.pending_rows.is_empty());
    assert!(session.apply_workspace.is_none());
    assert!(!session.persistence_pending());
    session.audit_graph().unwrap();
    for request in &requests {
        assert!(matches!(
            session.propose(request).unwrap(),
            Submission::Committed(_)
        ));
    }
}
