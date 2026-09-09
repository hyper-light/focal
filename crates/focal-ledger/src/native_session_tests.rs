use super::*;
use crate::native_checkpoint;
use focal_consensus::NodeConfig;
use focal_core::native::{NativeCommand, NativeLimits, input_codec, record_codec::recovery};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{BudgetKind, BudgetLane, RangeConfig};
use focal_model::lifecycle::{
    Binding, Principal, aggregation, artifact_descriptor, claim::ClaimDefinition, claim_descriptor,
    creation::Proposal, evidence::ResponseLimits, graph, scope, succession::Lineage, validation,
    validation_descriptor,
};
use focal_model::{
    ClaimId, ClaimStatus, ContentDomainId, Deadline, ObjectId, ObjectRevision, ParticipantId,
    RequestEpoch, RequestId, RequestKey, RootCommandId, SessionId, TenantId, TimerId,
    ValidationKind, ValidationMode, ValidationPhase,
};

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const RESPONDENT: ParticipantId = ParticipantId::from_u128(2);

pub(crate) fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(11),
        session: SessionId::from_u128(12),
    }
}
fn context() -> NativeContext {
    NativeContext {
        principal: Principal::Actor(ISSUER),
        logical_time: 1,
    }
}
fn binding(id: u128) -> Binding {
    Binding {
        ledger: ledger(),
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}
fn key(id: u128) -> RequestKey {
    RequestKey {
        principal: ISSUER,
        epoch: RequestEpoch(1),
        id: RequestId::from_u128(id),
    }
}
fn native_limits() -> NativeLimits {
    NativeLimits {
        range: RangeConfig {
            page_entries: 4,
            max_batch_entries: 128,
            ..RangeConfig::default()
        },
        plan_nodes: 32,
        plan_edges: 65_536,
        preparation_bytes: 1024 * 1024,
        evaluations_per_claim: 32,
        ..NativeLimits::default()
    }
}
pub(crate) fn limits() -> NativeSessionLimits {
    let declaration = validation::Limits {
        handlers: 32,
        attempts: 64,
        slot_bytes: 4096,
    };
    NativeSessionLimits {
        recovery: recovery::Limits {
            native: native_limits(),
            acceptance: aggregation::Limits {
                max_slots: 256,
                max_checks: 4096,
                max_results: 8192,
                max_updates: 8192,
            },
            artifact: artifact_descriptor::Limits {
                kind_bytes: 1024,
                metadata_bytes: 65_536,
                inline_bytes: 1024 * 1024,
                inputs: 256,
                visibility_labels: 256,
                visibility_label_bytes: 4096,
                construction_bytes: 4 * 1024 * 1024,
            },
            claim: claim_descriptor::Limits {
                description_bytes: 65_536,
                relations: 4096,
                scopes: 256,
                scope_key_bytes: 4096,
                requirements: 4096,
                slots: 256,
                checks: 4096,
                construction_bytes: 4 * 1024 * 1024,
            },
            declaration,
            validation: validation_descriptor::Limits {
                declaration,
                description_bytes: 65_536,
                quality_bar_bytes: 65_536,
                contributors: 256,
                construction_bytes: 4 * 1024 * 1024,
            },
            response: ResponseLimits {
                artifacts: 256,
                diagnostics: 256,
                summary_bytes: 65_536,
                construction_bytes: 4 * 1024 * 1024,
            },
            creation_objects: 4096,
            work: recovery::Work {
                parsing: 1 << 30,
                source: 1 << 30,
                model: 1 << 30,
                lookup: 1 << 30,
            },
        },
        encoding: record::EncodingLimits {
            bytes: 6 << 20,
            visits: 1 << 30,
            rows: 100_000,
        },
        inspection: record::InspectionLimits {
            bytes: 6 << 20,
            visits: 1 << 30,
            rows: 100_000,
            row_bytes: 6 << 20,
        },
        checkpoint: native_checkpoint::Limits::default(),
        frame_bytes: 1 << 20,
        decode_work: input_codec::DecodeWork {
            parse: 1 << 28,
            source: 1 << 28,
            model: 1 << 28,
            acceptance: 1 << 28,
            native: 1 << 28,
        },
        memory_bytes: 128 << 20,
        completion_reserve_bytes: 16 << 20,
        content_domain: ContentDomainId::from_u128(1),
        disk_headroom_bytes: 1 << 20,
    }
}
fn definition(binding: Binding) -> validation::Declaration {
    let claim_id = u128::from_be_bytes(binding.object.0);
    validation::Declaration::new(
        Principal::Actor(ISSUER),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(claim_id.checked_add(10_000).unwrap()),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer: ISSUER,
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
fn proposal(id: u128) -> Proposal {
    let binding = binding(id);
    Proposal {
        definition: ClaimDefinition {
            binding,
            issuer: ISSUER,
            subject: RESPONDENT,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1)).unwrap(),
            acceptance: aggregation::AcceptancePolicy::new(
                binding,
                ISSUER,
                &[],
                &[definition(binding)],
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
fn create(request: u128, id: u128) -> NativeInput {
    let proposals = vec![proposal(id)];
    let declarations = proposals
        .iter()
        .map(|p| definition(p.definition.binding))
        .collect();
    NativeInput {
        request: key(request),
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    }
}
fn post(request: u128, expected: Binding) -> NativeInput {
    NativeInput {
        request: key(request),
        command: NativeCommand::Post { expected },
    }
}
pub(crate) fn store(directory: &std::path::Path) -> ContentStore {
    ContentStore::open(
        directory,
        StoreLimits {
            max_content_bytes: 2 * 1024 * 1024,
            max_staging_bytes: 4 * 1024 * 1024,
            max_uploads: 8,
            chunk_bytes: 4096,
            max_manifest_bytes: 128 * 1024,
        },
    )
    .unwrap()
}
pub(crate) fn config() -> NodeConfig {
    NodeConfig::single(1, [21; 16], [22; 16])
}
pub(crate) fn open_dir(
    dir: &std::path::Path,
    parent: &MemoryBudget,
) -> NativeSession<BuiltinNativeSchemas> {
    open_with(dir, parent, limits()).unwrap()
}
pub(crate) fn open_with(
    dir: &std::path::Path,
    parent: &MemoryBudget,
    limits: NativeSessionLimits,
) -> Result<NativeSession<BuiltinNativeSchemas>, NativeSessionError> {
    std::fs::create_dir_all(dir.join("content")).unwrap();
    let opened = NativeSession::open(
        dir.join("wal"),
        config(),
        ledger(),
        RangeId(1),
        NativeContentProfile::ProjectionOnly,
        limits,
        parent,
        store(&dir.join("content")),
        BuiltinNativeSchemas,
    )?;
    assert!(opened.initial.consensus.messages.is_empty());
    Ok(opened.session)
}
/// Hold every byte a budget can still lend, on both lanes, so the next real
/// reservation is refused until the holds drop.
pub(crate) fn exhaust(budget: &MemoryBudget) -> Vec<focal_memory::Reservation> {
    let mut held = Vec::new();
    for lane in [BudgetLane::Ordinary, BudgetLane::Completion] {
        let mut size = 1usize << 26;
        while size >= 256 {
            match budget.reserve(BudgetKind::Query, lane, size) {
                Ok(reservation) => held.push(reservation),
                Err(_) => size /= 2,
            }
        }
    }
    held
}
pub(crate) fn lead(session: &mut NativeSession<BuiltinNativeSchemas>) {
    session.campaign().unwrap();
    for _ in 0..64 {
        let events = session.poll().unwrap();
        assert!(
            events.consensus.messages.is_empty(),
            "single voter sends no messages"
        );
        if session.is_authoritative() {
            return;
        }
        session.tick().unwrap();
    }
    panic!("single voter did not become authoritative");
}
fn commit(
    session: &mut NativeSession<BuiltinNativeSchemas>,
    input: NativeInput,
    step: &str,
) -> NativeOutcome {
    let request = input.request;
    let submission = session
        .propose(context(), input)
        .unwrap_or_else(|error| panic!("{step}: {error:?}"));
    let NativeSubmission::Pending { candidate, outcome } = submission else {
        panic!("fresh request must be pending")
    };
    for _ in 0..16 {
        let events = session
            .poll()
            .unwrap_or_else(|error| panic!("{step} poll: {error:?}"));
        if let Some(commit) = events
            .committed
            .iter()
            .find(|commit| commit.outcome == outcome)
        {
            assert!(commit.raft_index > 0 && commit.raft_term > 0);
            assert_eq!(session.outcome(request).unwrap(), Some(outcome));
            let _ = candidate;
            return outcome;
        }
    }
    panic!("{step}: candidate never committed");
}
fn claim_status(session: &NativeSession<BuiltinNativeSchemas>, id: u128) -> (ClaimStatus, Binding) {
    let claim = session
        .committed_core()
        .unwrap()
        .native_claim(ClaimId::from_u128(id))
        .unwrap();
    (claim.status(), claim.binding())
}

#[test]
fn one_node_create_post_checkpoint_restart_and_exact_retry_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let created;
    let posted;
    let expected;
    {
        let mut session = open_dir(dir.path(), &parent);
        assert_eq!(
            NativeSession::<BuiltinNativeSchemas>::decoder_hash(),
            native_checkpoint::format_hash()
        );
        lead(&mut session);
        // An empty genesis root at the applied no-op prefix is a legitimate checkpoint.
        session.begin_checkpoint().unwrap();
        let _ = session.poll().unwrap();
        assert!(!session.checkpoint_pending());
        created = commit(&mut session, create(1, 100), "first create");
        assert_eq!(session.sequence().unwrap(), SessionSeq(1));
        let (status, generated) = claim_status(&session, 100);
        expected = generated;
        assert_eq!(status, ClaimStatus::Generated);
        // Exact retry of a committed request returns the original outcome without a candidate.
        assert_eq!(
            session.propose(context(), create(1, 100)).unwrap(),
            NativeSubmission::Committed(created)
        );
        assert_eq!(session.pending_count(), 0);
        session.begin_checkpoint().unwrap();
        assert!(session.checkpoint_pending());
        let _ = session.poll().unwrap();
        assert!(!session.checkpoint_pending());
        posted = commit(&mut session, post(2, expected), "post after checkpoint");
        assert_eq!(session.sequence().unwrap(), SessionSeq(2));
        assert_eq!(claim_status(&session, 100).0, ClaimStatus::Posted);
    }
    // Reopen: checkpoint restores the created claim, the tail replays the post.
    let mut session = open_dir(dir.path(), &parent);
    assert_eq!(session.sequence().unwrap(), SessionSeq(2));
    assert_eq!(claim_status(&session, 100).0, ClaimStatus::Posted);
    assert_eq!(session.outcome(key(1)).unwrap(), Some(created));
    assert_eq!(session.outcome(key(2)).unwrap(), Some(posted));
    assert!(!session.is_authoritative());
    assert!(matches!(
        session.propose(context(), create(3, 101)),
        Err(NativeSessionError::NotReady { .. })
    ));
    // A new term admits new work and still answers the old exact retries.
    lead(&mut session);
    assert_eq!(
        session.propose(context(), create(1, 100)).unwrap(),
        NativeSubmission::Committed(created)
    );
    // The exact original intent (creation binding) is answered; a substituted binding conflicts.
    assert_eq!(
        session.propose(context(), post(2, expected)).unwrap(),
        NativeSubmission::Committed(posted)
    );
    assert!(matches!(
        session.propose(context(), post(2, claim_status(&session, 100).1)),
        Err(NativeSessionError::Owner(NativeOwnerError::Native(
            NativeError::RequestConflict
        )))
    ));
    let third = commit(&mut session, create(3, 101), "create after reopen");
    assert_eq!(third.sequence, SessionSeq(3));
    assert_eq!(claim_status(&session, 101).0, ClaimStatus::Generated);
    session.begin_checkpoint().unwrap();
    let _ = session.poll().unwrap();
    drop(session);
    let session = open_dir(dir.path(), &parent);
    assert_eq!(session.sequence().unwrap(), SessionSeq(3));
    assert_eq!(claim_status(&session, 101).0, ClaimStatus::Generated);
    assert_eq!(session.outcome(key(3)).unwrap(), Some(third));
}

#[test]
fn checkpoint_refuses_foreign_identity_and_missing_native_prefix_before_touching_state() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut session = open_dir(dir.path(), &parent);
    // Nothing applied yet: no checkpoint can describe a prefix.
    assert!(matches!(
        session.begin_checkpoint(),
        Err(NativeSessionError::Consensus(
            ConsensusError::CheckpointIndex
        ))
    ));
    lead(&mut session);
    let _ = commit(&mut session, create(1, 100), "create before checkpoint");
    session.begin_checkpoint().unwrap();
    // A second request while the first is unresolved is a persistence refusal, not a failure.
    assert!(matches!(
        session.begin_checkpoint(),
        Err(NativeSessionError::Consensus(
            ConsensusError::PersistencePending
        ))
    ));
    let _ = session.poll().unwrap();
    assert!(session.is_authoritative());
    // A snapshot for another physical group is refused fail-closed at inspection.
    let core = session.committed_core().unwrap();
    let configuration = session.status();
    let foreign = native_checkpoint::Metadata {
        cluster: [9; 16],
        group: [22; 16],
        applied_raft: 1,
        applied_term: 1,
        configuration_index: 0,
        recording_range: Some(RangeId(1)),
        recording_term: 1,
        records_floor: 0,
        activation_index: 1,
        activation: native_checkpoint::Activation {
            decoder: native_checkpoint::format_hash(),
            durable_floor: native_checkpoint::format_hash(),
            genesis: native_checkpoint::genesis(
                [9; 16],
                [22; 16],
                ledger(),
                NativeContentProfile::ProjectionOnly,
                native_checkpoint::format_hash(),
            ),
        },
        ancillary: native_checkpoint::AncillaryProfile::NativeOnlyV1,
    };
    let membership = focal_consensus::MembershipConfiguration {
        voters: configuration.voters.clone(),
        learners: Vec::new(),
        voters_outgoing: Vec::new(),
        learners_next: Vec::new(),
        auto_leave: false,
    };
    let plan =
        native_checkpoint::EncodingPlan::prepare(core, foreign, &membership, limits().checkpoint)
            .unwrap();
    let encoded = plan.encode_in(&parent).unwrap();
    let snapshot = focal_consensus::AppliedSnapshot {
        index: 1,
        term: 1,
        data: encoded.bytes().to_vec(),
        configuration: membership,
    };
    assert!(matches!(
        session.restore_snapshot_for_test(&snapshot),
        Err(NativeSessionError::Corrupt)
    ));
    assert_eq!(session.sequence().unwrap(), SessionSeq(1));
    assert!(session.is_authoritative());
}

fn frame(input: &NativeInput) -> Vec<u8> {
    let source = input_codec::InputFrame::Request {
        ledger: ledger(),
        profile: NativeContentProfile::ProjectionOnly,
        input,
    };
    let plan = input_codec::EncodingPlan::prepare(
        source,
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

#[test]
fn borrowed_frames_share_identity_with_typed_input_and_timer_namespaces_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut session = open_dir(dir.path(), &parent);
    lead(&mut session);
    let input = create(7, 700);
    let bytes = frame(&input);
    let NativeSubmission::Pending { outcome, .. } =
        session.propose_native_frame(context(), &bytes).unwrap()
    else {
        panic!("fresh frame")
    };
    // The identical frame is the same pending candidate; the typed input is too.
    assert_eq!(
        session.propose_native_frame(context(), &bytes).unwrap(),
        NativeSubmission::Pending {
            candidate: session.oldest_candidate_for_test().unwrap(),
            outcome
        }
    );
    assert_eq!(
        session.propose(context(), create(7, 700)).unwrap(),
        NativeSubmission::Pending {
            candidate: session.oldest_candidate_for_test().unwrap(),
            outcome
        }
    );
    for _ in 0..8 {
        if session.outcome(key(7)).unwrap().is_some() {
            break;
        }
        let _ = session.poll().unwrap();
    }
    assert_eq!(session.outcome(key(7)).unwrap(), Some(outcome));
    assert_eq!(
        session.propose_native_frame(context(), &bytes).unwrap(),
        NativeSubmission::Committed(outcome)
    );
    // A frame authored for another principal cannot be admitted under this context.
    let other = NativeContext {
        principal: Principal::Actor(RESPONDENT),
        logical_time: 2,
    };
    assert!(matches!(
        session.propose_native_frame(other, &bytes),
        Err(NativeSessionError::Owner(_))
    ));
    // A trusted timer namespace in participant bytes is refused before any admission.
    let timer = input_codec::InputFrame::ClaimDeadline {
        ledger: ledger(),
        profile: NativeContentProfile::ProjectionOnly,
        input: focal_core::native::NativeClaimDeadlineInput {
            claim: ClaimId::from_u128(700),
            deadline: Deadline {
                timer: TimerId::from_u128(9),
                generation: 1,
                at: 5,
            },
        },
    };
    let plan = input_codec::EncodingPlan::prepare(
        timer,
        input_codec::EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 28,
        },
    )
    .unwrap();
    let mut timer_bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut timer_bytes).unwrap();
    assert!(matches!(
        session.propose_native_frame(context(), &timer_bytes),
        Err(NativeSessionError::Owner(_))
    ));
    assert_eq!(session.pending_count(), 0);
    // Correlated read barriers return the caller's correlation at an applied prefix.
    session.read_index(ReadCorrelation([3; 16])).unwrap();
    let mut seen = None;
    for _ in 0..8 {
        let events = session.poll().unwrap();
        if let Some(boundary) = events.read_boundaries.first() {
            seen = Some(*boundary);
            break;
        }
    }
    let boundary = seen.expect("read barrier delivered");
    assert_eq!(boundary.correlation, ReadCorrelation([3; 16]));
    assert_eq!(boundary.native_sequence, SessionSeq(1));
    assert!(session.read_at_least(boundary).is_ok());
}

#[test]
fn pending_queue_bound_refuses_the_next_fresh_candidate_until_a_slot_frees() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut bounded = limits();
    bounded.recovery.native.pending = 1;
    let mut session = open_with(dir.path(), &parent, bounded).unwrap();
    lead(&mut session);
    let NativeSubmission::Pending { outcome: first, .. } =
        session.propose(context(), create(1, 1)).unwrap()
    else {
        panic!("fresh")
    };
    let before = session.budget_for_test().stats();
    let refused = session.propose(context(), create(2, 2)).unwrap_err();
    assert_eq!(refused.class(), FailureClass::Retryable, "{refused:?}");
    assert_eq!(
        session.pending_count(),
        1,
        "the refused candidate holds no slot"
    );
    assert_eq!(session.budget_for_test().stats(), before, "and no memory");
    assert_eq!(session.outcome(key(2)).unwrap(), None);
    // The queued candidate is still the same ticket; its exact retry is pending.
    assert!(matches!(
        session.propose(context(), create(1, 1)).unwrap(),
        NativeSubmission::Pending { outcome, .. } if outcome == first
    ));
    let mut committed = false;
    for _ in 0..16 {
        let events = session.poll().unwrap();
        if events
            .committed
            .iter()
            .any(|commit| commit.outcome == first)
        {
            committed = true;
            break;
        }
    }
    assert!(committed);
    assert_eq!(session.pending_count(), 0);
    let second = commit(&mut session, create(2, 2), "second after a slot freed");
    assert_eq!(session.outcome(key(2)).unwrap(), Some(second));
    assert_eq!(session.sequence().unwrap(), SessionSeq(2));
}

#[test]
fn delivery_under_session_memory_pressure_is_retained_and_completes_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut session = open_dir(dir.path(), &parent);
    lead(&mut session);
    let NativeSubmission::Pending { outcome, .. } =
        session.propose(context(), create(1, 1)).unwrap()
    else {
        panic!("fresh")
    };
    let held = exhaust(session.budget_for_test());
    assert!(!held.is_empty());
    let mut refusals = 0;
    for _ in 0..4 {
        match session.poll() {
            Err(error) => {
                assert_eq!(error.class(), FailureClass::Retryable, "{error:?}");
                refusals += 1;
            }
            Ok(events) => assert!(
                events.committed.is_empty(),
                "nothing applies under pressure"
            ),
        }
    }
    assert!(refusals > 0, "the delivery output could not be funded");
    assert_eq!(
        session.pending_count(),
        1,
        "the candidate is retained, not discarded"
    );
    assert_eq!(session.outcome(key(1)).unwrap(), None);
    assert!(!session.failed());
    drop(held);
    let mut applied = 0;
    for _ in 0..16 {
        let events = session.poll().unwrap();
        applied += events
            .committed
            .iter()
            .filter(|commit| commit.outcome == outcome)
            .count();
    }
    assert_eq!(applied, 1, "the retained delivery completes exactly once");
    assert_eq!(session.outcome(key(1)).unwrap(), Some(outcome));
    assert_eq!(session.sequence().unwrap(), SessionSeq(1));
    assert_eq!(session.pending_count(), 0);
}

#[test]
fn opening_under_a_budget_too_small_for_reconstruction_is_refused_without_state() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let mut tiny = limits();
    tiny.memory_bytes = 4096;
    tiny.completion_reserve_bytes = 1024;
    let error = match open_with(dir.path(), &parent, tiny) {
        Err(error) => error,
        Ok(_) => panic!("a 4 KiB session budget cannot fund reconstruction"),
    };
    assert_eq!(error.class(), FailureClass::Retryable, "{error:?}");
    assert_eq!(
        parent.stats().used,
        0,
        "a refused open leaves nothing charged"
    );
    // The same directory opens normally once memory is available.
    let mut session = open_dir(dir.path(), &parent);
    lead(&mut session);
    commit(&mut session, create(1, 1), "create after refused open");
}

#[test]
fn disk_headroom_watermark_refuses_fresh_admission_but_answers_exact_retries() {
    let dir = tempfile::tempdir().unwrap();
    let parent = MemoryBudget::new(256 << 20, 64 << 20).unwrap();
    let created = {
        let mut session = open_dir(dir.path(), &parent);
        lead(&mut session);
        commit(&mut session, create(1, 1), "create")
    };
    let mut unreachable = limits();
    unreachable.disk_headroom_bytes = u64::MAX;
    let mut session = open_with(dir.path(), &parent, unreachable).unwrap();
    lead(&mut session);
    assert_eq!(
        session.propose(context(), create(1, 1)).unwrap(),
        NativeSubmission::Committed(created),
        "committed work needs no disk"
    );
    let before = session.budget_for_test().stats();
    assert!(matches!(
        session.propose(context(), create(2, 2)),
        Err(NativeSessionError::Capacity)
    ));
    assert_eq!(session.pending_count(), 0);
    assert_eq!(session.budget_for_test().stats(), before);
    assert_eq!(session.outcome(key(2)).unwrap(), None);
    assert!(!session.failed());
    assert_eq!(session.sequence().unwrap(), SessionSeq(1));
    drop(session);
    // A reachable watermark on the same log admits the same fresh request.
    let mut session = open_dir(dir.path(), &parent);
    lead(&mut session);
    commit(&mut session, create(2, 2), "create with headroom");
    assert_eq!(session.sequence().unwrap(), SessionSeq(2));
}
