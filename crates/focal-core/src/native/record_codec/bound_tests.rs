//! Proof that the future record bound dominates the codec for every recorded
//! row family, measured on real mutations of complete workflows at authored
//! maxima, and that each completion promise dominates the report it funds.
//! `buffer::future_quote` is the single function under test; every changed
//! key is checked individually against its own heap charge.
use super::*;
use crate::native::completion_envelope::CompletionUse;
use crate::native::fixtures as fx;
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{MemoryBudget, RangeConfig, RangeWriteLimits};
use focal_model::lifecycle::claim_descriptor::{self, ClaimDescriptor, ClaimSpec, ScopeSpec};
use focal_model::lifecycle::evidence::{EvidenceFailure, Parent};
use focal_model::lifecycle::validation_descriptor::{self, ValidationDescriptor, ValidationSpec};
use focal_model::lifecycle::{Principal, aggregation, graph, scope, validation};
use focal_model::{
    ActionType, ArtifactId, ClaimId, ClaimStatus, Confidence, ContentDomainId, Deadline, ObjectId,
    ObjectKind, ObjectRef, OccurrenceId, OutcomeKind, ParticipantId, Relation, RelationKind,
    RelationTarget, RequestKey, RootCommandId, ScopeKind, SessionId, TenantId, TestamentId,
    TimerId, ValidationId, ValidationKind, ValidationMode, ValidationPhase, VerdictValue,
    WaitPredicate,
};

const PARTIES: fx::Parties = fx::Parties::numbered(71);
const DELIVERY: u128 = 300;
const SLOT: u128 = 301;
const DOMAIN: ContentDomainId = ContentDomainId::from_u128(1);
const FAMILIES: [RowFamily; 31] = [
    RowFamily::Index,
    RowFamily::IncomingHead,
    RowFamily::IncomingLink,
    RowFamily::Monitor,
    RowFamily::MonitorHead,
    RowFamily::MonitorLink,
    RowFamily::MissingResult,
    RowFamily::Meta,
    RowFamily::Claim,
    RowFamily::Definition,
    RowFamily::Evaluation,
    RowFamily::Artifact,
    RowFamily::ArtifactIdentity,
    RowFamily::Accepted,
    RowFamily::DeliveryResult,
    RowFamily::Receipt,
    RowFamily::Cycle,
    RowFamily::RetiredCycleHead,
    RowFamily::RetiredCycle,
    RowFamily::Work,
    RowFamily::WorkSlot,
    RowFamily::Diagnostic,
    RowFamily::Response,
    RowFamily::ResultTestament,
    RowFamily::ClaimResultTestament,
    RowFamily::Outcome,
    RowFamily::Event,
    RowFamily::ClaimContent,
    RowFamily::ClaimIdentity,
    RowFamily::DefinitionIdentity,
    RowFamily::CreationResult,
];

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(31),
        session: SessionId::from_u128(32),
    }
}
fn encoding() -> EncodingLimits {
    EncodingLimits {
        bytes: 32 << 20,
        visits: 1 << 32,
        rows: 100_000,
    }
}
fn limits() -> NativeLimits {
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
fn budget() -> MemoryBudget {
    // Five concurrently begun work reports each hold their conservative page
    // envelope, which prices every changed key (index rows included) as a
    // possible page copy.
    MemoryBudget::new(256 << 20, 16 << 20).unwrap()
}
fn declarations(claim: u128) -> Vec<validation::Declaration> {
    vec![
        fx::delivery_declaration(ledger(), PARTIES, claim, claim * 1000 + DELIVERY, 1000).unwrap(),
        fx::slot_declaration(
            ledger(),
            PARTIES,
            claim,
            claim * 1000 + SLOT,
            1,
            0,
            ValidationMode::Required,
            false,
            1000,
        )
        .unwrap(),
    ]
}
fn slots(claim: u128) -> Vec<fx::Slot> {
    vec![
        fx::Slot {
            slot: 0,
            missing_declaration_index: 20,
            mode: ValidationMode::Required,
            checks: vec![aggregation::CheckPolicy {
                declaration_index: 1,
                validation: ValidationId::from_u128(claim * 1000 + SLOT),
                mode: ValidationMode::Required,
            }],
        },
        fx::Slot {
            slot: 1,
            missing_declaration_index: 21,
            mode: ValidationMode::Required,
            checks: vec![],
        },
    ]
}

/// Observed shape of one recorded mutation: what the bound must dominate.
#[derive(Debug, Clone, Copy)]
struct Shape {
    rows: usize,
    heap: usize,
}
impl Shape {
    fn limits(self) -> RangeWriteLimits {
        RangeWriteLimits {
            changed_keys: self.rows,
            deleted_keys: 0,
            deleted_heap: 0,
            incoming_heap: self.heap,
            input_capacity: self.rows,
        }
    }
    fn within(self, promised: RangeWriteLimits) -> bool {
        self.rows <= promised.changed_keys && self.heap <= promised.incoming_heap
    }
}

struct Lab {
    owner: NativeOwner,
    store: ContentStore,
    _dir: tempfile::TempDir,
    clock: u64,
    serial: u128,
    seen: Vec<RowFamily>,
    rows_checked: usize,
}
impl Lab {
    fn new(profile: NativeContentProfile) -> Self {
        let core = match profile {
            NativeContentProfile::ProjectionOnly => {
                Core::new_native(ledger(), RangeId(7), limits(), budget()).unwrap()
            }
            NativeContentProfile::AuthoredV1 => {
                Core::new_native_authored(ledger(), RangeId(8), limits(), budget()).unwrap()
            }
        };
        let dir = tempfile::tempdir().unwrap();
        let store = ContentStore::open(
            dir.path(),
            StoreLimits {
                max_content_bytes: 2 * 1024 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 4096,
                max_manifest_bytes: 128 * 1024,
            },
        )
        .unwrap();
        Self {
            owner: NativeOwner::with_record_buffers(core, &BuiltinNativeSchemas, encoding())
                .unwrap(),
            store,
            _dir: dir,
            clock: 0,
            serial: 0,
            seen: Vec::new(),
            rows_checked: 0,
        }
    }
    fn next(&mut self, actor: ParticipantId) -> RequestKey {
        self.serial += 1;
        fx::request(actor, 1, self.serial)
    }
    fn view(&self) -> NativeView<'_> {
        self.owner.committed()
    }
    fn claim(&self, id: u128) -> Binding {
        self.view().claim(ClaimId::from_u128(id)).unwrap().binding()
    }
    fn status(&self, id: u128) -> ClaimStatus {
        self.view().claim(ClaimId::from_u128(id)).unwrap().status()
    }
    fn parent(&self, id: u128) -> Parent {
        Parent::from_claim(self.view().claim(ClaimId::from_u128(id)).unwrap()).unwrap()
    }
    fn response(&self, id: u128) -> Binding {
        self.view()
            .response_record(TestamentId::from_u128(id))
            .unwrap()
            .response()
            .identity()
            .binding
    }
    /// Admit, prove the bound on the candidate, publish; returns the shape.
    fn run(&mut self, actor: ParticipantId, input: NativeInput, step: &str) -> Shape {
        self.clock += 1;
        let staged = self
            .owner
            .prepare_with_custody(
                fx::context(actor, self.clock),
                input,
                &mut self.store,
                DOMAIN,
                &BuiltinNativeSchemas,
            )
            .unwrap_or_else(|error| panic!("{step}: {error:?}"));
        let NativeStaging::Prepared { candidate, .. } = staged else {
            panic!("{step}: expected a fresh candidate");
        };
        let shape = self.check(candidate, step);
        self.owner.publish_after_durable(candidate).unwrap();
        shape
    }
    fn run_deadline(&mut self, input: NativeDeadlineInput, step: &str) -> Shape {
        self.clock = self.clock.max(input.deadline.at + 1);
        let staged = self
            .owner
            .prepare_evaluation_deadline(input, self.clock)
            .unwrap_or_else(|error| panic!("{step}: {error:?}"));
        let NativeStaging::Prepared { candidate, .. } = staged else {
            panic!("{step}: expected a fresh timer candidate");
        };
        let shape = self.check(candidate, step);
        self.owner.publish_after_durable(candidate).unwrap();
        shape
    }
    /// Every changed key individually, then the whole frame, against the bound
    /// evaluated at the mutation's own shape.
    fn check(&mut self, candidate: NativeCandidate, step: &str) -> Shape {
        let quote = self
            .owner
            .encode_candidate(candidate, encoding())
            .unwrap()
            .quote();
        let prepared = self.owner.prepared_candidate(candidate).unwrap();
        let ledger = prepared.outcome().ledger;
        let mut shape = Shape { rows: 0, heap: 0 };
        for (key, deleted) in prepared.writes.entries() {
            shape.rows += 1;
            let family = fixed::family(key).unwrap();
            let mut fixed_part = CountingSink::new(usize::MAX, usize::MAX);
            write_u8(&mut fixed_part, u8::from(!deleted)).unwrap();
            fixed::key(&mut fixed_part, key).unwrap();
            if deleted {
                write_count(&mut fixed_part, 0).unwrap();
                assert!(
                    fixed_part.len() <= row_fixed_bytes(),
                    "{step}: deleted {family:?} exceeds the fixed row allowance"
                );
                continue;
            }
            let entry = prepared
                .range
                .entries()
                .find(|entry| entry.key == key)
                .unwrap_or_else(|| panic!("{step}: written {family:?} row is present"));
            let mut body = CountingSink::new(usize::MAX, usize::MAX);
            rows::value(&mut body, &entry.value, ledger).unwrap();
            write_count(&mut fixed_part, body.len()).unwrap();
            let encoded = fixed_part.len() + body.len();
            let allowed = row_fixed_bytes() + HEAP_EXPANSION * entry.heap_bytes;
            assert!(
                encoded <= allowed,
                "{step}: {family:?} encodes {encoded} bytes from {} heap bytes; allowed {allowed}",
                entry.heap_bytes
            );
            shape.heap += entry.heap_bytes;
            if !self.seen.contains(&family) {
                self.seen.push(family);
            }
        }
        let bound = future_record_quote(shape.limits(), encoding()).unwrap();
        assert!(
            quote.bytes <= bound.bytes,
            "{step}: frame {} bytes exceeds bound {} for {shape:?}",
            quote.bytes,
            bound.bytes
        );
        assert!(
            quote.visits <= bound.visits,
            "{step}: frame {} visits exceeds bound {} for {shape:?}",
            quote.visits,
            bound.visits
        );
        assert_eq!(quote.rows, shape.rows);
        self.rows_checked += shape.rows;
        shape
    }
    /// The shape the completion book promised for this evaluation's report.
    fn promised(&self, key: EvaluationKey, usage: CompletionUse) -> RangeWriteLimits {
        let envelope = *self
            .owner
            .book_for_test()
            .envelope_for_test(key)
            .unwrap_or_else(|| panic!("no completion promise for {key:?}"));
        envelope.report_storage(usage).unwrap().limits()
    }
}

fn metadata() -> Vec<u8> {
    let mut bytes = b"{\"notes\":\"".to_vec();
    bytes.resize(1022, b'm');
    bytes.extend_from_slice(b"\"}");
    bytes
}
fn labels() -> Vec<String> {
    (0..16).map(|index| format!("label{index:02}")).collect()
}
/// Inputs name existing ledger objects; the first claim exists before any
/// artifact is produced.
fn inputs() -> Vec<ObjectRef> {
    vec![ObjectRef {
        ledger: ledger(),
        kind: ObjectKind::Claim,
        id: ObjectId::from_u128(1),
    }]
}

/// Two full cycles: successful testimony with the largest authored artifact
/// descriptors, failed testimony with diagnostics, evaluator reports, derived
/// acceptance, monitors, deadlines, adoption, audit testaments and children.
fn projection_workflow(lab: &mut Lab) {
    let metadata = metadata();
    let labels = labels();
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    let inputs = inputs();
    let shape = fx::ArtifactShape {
        metadata: &metadata,
        inputs: &inputs,
        visibility: &label_refs,
    };
    let summary = "s".repeat(8192);

    // Claim 1: complete work, validated, satisfied.
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::creation(ledger(), PARTIES, request, 1, declarations(1), &slots(1)).unwrap(),
        "create 1",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(1));
    lab.run(PARTIES.issuer, fx::post(request, expected), "post 1");
    let (request, expected) = (lab.next(PARTIES.subject), lab.claim(1));
    lab.run(
        PARTIES.subject,
        fx::acquire_receipt(request, expected, 701),
        "receipt 1",
    );
    assert_eq!(lab.status(1), ClaimStatus::Received);
    let parent = lab.parent(1);
    let (first, first_slot) =
        fx::work_artifact_shaped(ledger(), 801, &parent, 0, fx::PROOF, shape).unwrap();
    let (second, second_slot) =
        fx::work_artifact_shaped(ledger(), 802, &parent, 1, fx::PROOF, shape).unwrap();
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(1));
    lab.run(
        PARTIES.subject,
        fx::submit_work(request, claim, 0, first),
        "work 801",
    );
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(1));
    lab.run(
        PARTIES.subject,
        fx::submit_work(request, claim, 1, second),
        "work 802",
    );
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(1));
    lab.run(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            10,
            &summary,
            Confidence::Committed,
            OutcomeKind::Complete,
            vec![first_slot, second_slot],
            vec![],
        ),
        "close 10",
    );
    let (request, claim, response) = (lab.next(PARTIES.subject), lab.claim(1), lab.response(10));
    lab.run(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post 10",
    );
    let (request, claim, response) = (lab.next(PARTIES.issuer), lab.claim(1), lab.response(10));
    lab.run(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive 10",
    );
    let key = fx::work_key(1, 1000 + SLOT, 10, 0, ArtifactId::from_u128(801), 1);
    let expected = lab.view().evaluation(key).unwrap().binding();
    let (request, claim) = (lab.next(PARTIES.evaluator), lab.claim(1));
    lab.run(
        PARTIES.evaluator,
        fx::begin_work(request, claim, key, expected),
        "begin work",
    );
    let promised = lab.promised(key, CompletionUse::Regular);
    let report = {
        let view = lab.view();
        let state = view.evaluation(key).unwrap();
        let definition = view.definition(key.validation).unwrap();
        fx::report_work_shaped(
            ledger(),
            fx::request(PARTIES.evaluator, 1, 5001),
            851,
            lab.claim(1),
            key,
            state,
            definition,
            VerdictValue::Pass,
            fx::PROOF,
            shape,
        )
        .unwrap()
    };
    let actual = lab.run(PARTIES.evaluator, report, "report work");
    assert!(
        actual.within(promised),
        "the completion promise {promised:?} must dominate the actual report {actual:?}"
    );
    assert_eq!(lab.status(1), ClaimStatus::Satisfied);
    // Audit testament over the satisfied claim, generated then posted.
    let (request, claim) = (lab.next(PARTIES.issuer), lab.claim(1));
    lab.run(
        PARTIES.issuer,
        fx::generate_result_testament(request, claim, 1900),
        "generate result testament",
    );
    let expected = lab
        .view()
        .result_testament(TestamentId::from_u128(1900))
        .unwrap()
        .generated_binding();
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::post_result_testament(request, expected),
        "post result testament",
    );

    // Claim 2: failed work with a diagnostic, whole-work entry, adoption.
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::creation(ledger(), PARTIES, request, 2, declarations(2), &slots(2)).unwrap(),
        "create 2",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(2));
    lab.run(PARTIES.issuer, fx::post(request, expected), "post 2");
    let (request, expected) = (lab.next(PARTIES.subject), lab.claim(2));
    lab.run(
        PARTIES.subject,
        fx::acquire_receipt(request, expected, 702),
        "receipt 2",
    );
    let parent = lab.parent(2);
    let (diagnostic, diagnostic_ref) = fx::diagnostic_artifact_shaped(
        ledger(),
        821,
        &parent,
        EvidenceFailure::Work,
        fx::WORK_DIAGNOSTIC,
        shape,
    )
    .unwrap();
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(2));
    lab.run(
        PARTIES.subject,
        fx::submit_diagnostic(request, claim, EvidenceFailure::Work, diagnostic),
        "diagnostic 821",
    );
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(2));
    lab.run(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            20,
            &summary,
            Confidence::Tentative,
            OutcomeKind::Failed,
            vec![],
            vec![diagnostic_ref],
        ),
        "close 20",
    );
    let (request, claim, response) = (lab.next(PARTIES.subject), lab.claim(2), lab.response(20));
    lab.run(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post 20",
    );
    let (request, claim, response) = (lab.next(PARTIES.issuer), lab.claim(2), lab.response(20));
    lab.run(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive 20",
    );
    let (request, claim, response) = (lab.next(PARTIES.issuer), lab.claim(2), lab.response(20));
    lab.run(
        PARTIES.issuer,
        fx::enter_whole_work(request, claim, response),
        "enter whole work",
    );
    assert_eq!(lab.status(2), ClaimStatus::ValidationIncomplete);

    // Claim 3: received, then adopted by a replacement holder, and a monitor
    // waiting on claim 1 registered while it was still open.
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::creation(ledger(), PARTIES, request, 3, declarations(3), &slots(3)).unwrap(),
        "create 3",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(3));
    lab.run(
        PARTIES.issuer,
        fx::register_monitor(
            request,
            expected,
            None,
            3100,
            vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
            Deadline {
                timer: TimerId::from_u128(3100),
                generation: 1,
                at: 5000,
            },
        ),
        "register monitor",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(3));
    lab.run(PARTIES.issuer, fx::post(request, expected), "post 3");
    let (request, expected) = (lab.next(PARTIES.subject), lab.claim(3));
    lab.run(
        PARTIES.subject,
        fx::acquire_receipt(request, expected, 703),
        "receipt 3",
    );
    // Unfinished work under the first holder is retired by the adoption.
    let parent = lab.parent(3);
    let (unfinished, _) = fx::work_artifact(ledger(), 803, &parent, 0, fx::PROOF).unwrap();
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(3));
    lab.run(
        PARTIES.subject,
        fx::submit_work(request, claim, 0, unfinished),
        "work 803",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(3));
    lab.run(
        PARTIES.issuer,
        fx::adopt_receipt(request, expected, parent.receipt, 704, PARTIES.quality),
        "adopt 3",
    );
    // Claim 4 is caused by claim 3 and owned under it.
    let (request, parent_binding, receipt) = (
        lab.next(PARTIES.issuer),
        lab.claim(3),
        lab.parent(3).receipt,
    );
    lab.run(
        PARTIES.issuer,
        fx::child_creation(
            ledger(),
            PARTIES,
            request,
            4,
            parent_binding,
            Some(receipt),
            declarations(4),
            &slots(4),
        )
        .unwrap(),
        "create child 4",
    );
    // Claim 6 depends on claim 3 and awaits claim 4: derived incoming links.
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::creation_with_graph(
            ledger(),
            PARTIES,
            request,
            6,
            &[
                graph::Obligation {
                    kind: graph::Kind::DependsOn,
                    target: ClaimId::from_u128(3),
                },
                graph::Obligation {
                    kind: graph::Kind::Awaits,
                    target: ClaimId::from_u128(4),
                },
            ],
            declarations(6),
            &slots(6),
        )
        .unwrap(),
        "create dependent 6",
    );
    // Claim 5: the whole-work check is begun and then misses its deadline.
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        fx::creation(ledger(), PARTIES, request, 5, declarations(5), &slots(5)).unwrap(),
        "create 5",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(5));
    lab.run(PARTIES.issuer, fx::post(request, expected), "post 5");
    let (request, expected) = (lab.next(PARTIES.subject), lab.claim(5));
    lab.run(
        PARTIES.subject,
        fx::acquire_receipt(request, expected, 705),
        "receipt 5",
    );
    let parent = lab.parent(5);
    let (work, work_slot) = fx::work_artifact(ledger(), 805, &parent, 0, fx::PROOF).unwrap();
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(5));
    lab.run(
        PARTIES.subject,
        fx::submit_work(request, claim, 0, work),
        "work 805",
    );
    let (request, claim) = (lab.next(PARTIES.subject), lab.claim(5));
    lab.run(
        PARTIES.subject,
        fx::close_response(
            ledger(),
            request,
            claim,
            50,
            "partial",
            Confidence::Tentative,
            OutcomeKind::Complete,
            vec![work_slot],
            vec![],
        ),
        "close 50",
    );
    let (request, claim, response) = (lab.next(PARTIES.subject), lab.claim(5), lab.response(50));
    lab.run(
        PARTIES.subject,
        fx::post_response(request, claim, response),
        "post 50",
    );
    let (request, claim, response) = (lab.next(PARTIES.issuer), lab.claim(5), lab.response(50));
    lab.run(
        PARTIES.issuer,
        fx::receive_response(request, claim, response),
        "receive 50",
    );
    let key = fx::work_key(5, 5000 + SLOT, 50, 0, ArtifactId::from_u128(805), 1);
    let expected = lab.view().evaluation(key).unwrap().binding();
    let (request, claim) = (lab.next(PARTIES.evaluator), lab.claim(5));
    lab.run(
        PARTIES.evaluator,
        fx::begin_work(request, claim, key, expected),
        "begin work 5",
    );
    let deadline = fx::evaluation_deadline(&lab.view(), key).unwrap();
    lab.run_deadline(deadline, "evaluation deadline 5");
}

fn authored_declaration(id: u128, claim: u128) -> ValidationDescriptor {
    let contributors = [PARTIES.issuer];
    let description = "d".repeat(1024);
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(PARTIES.issuer),
        ValidationSpec {
            ledger: ledger(),
            id: ValidationId::from_u128(id),
            schema: 1,
            claim: ClaimId::from_u128(claim),
            issuer: PARTIES.issuer,
            declaration_index: 0,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(id),
                generation: 1,
                at: 1000,
            },
            description: &description,
            // A delivery requirement states no quality bar; that is policy, not size.
            quality_bar: None,
            contributed_by: &contributors,
            policy_revision: 1,
        },
        validation_descriptor::Limits {
            declaration: validation::Limits {
                handlers: 4,
                attempts: 8,
                slot_bytes: 64,
            },
            description_bytes: 1024,
            quality_bar_bytes: 1024,
            contributors: 4,
            construction_bytes: 65_536,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}
fn authored_proposal(id: u128, validation: u128) -> NativeAuthoredProposal {
    let declaration = authored_declaration(validation, id);
    let pins = [focal_model::RequirementRef {
        id: ValidationId::from_u128(validation),
        specification: declaration.specification_hash(),
    }];
    let mut relations = vec![
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(PARTIES.issuer),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(PARTIES.subject),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(900)),
        },
    ];
    relations.sort();
    let description = "c".repeat(4096);
    let keys: Vec<String> = (0..4).map(|index| format!("{index}/").repeat(31)).collect();
    let scopes: Vec<ScopeSpec<'_>> = keys
        .iter()
        .map(|key| ScopeSpec {
            kind: ScopeKind::File,
            key,
        })
        .collect();
    let plan = ClaimDescriptor::prepare(
        ClaimSpec {
            ledger: ledger(),
            id: ClaimId::from_u128(id),
            schema: 1,
            occurrence: OccurrenceId::from_u128(id),
            description: &description,
            relations: &relations,
            scopes: &scopes,
            requirements: &pins,
            slots: &[],
            deadline: None,
            policy: None,
        },
        claim_descriptor::Limits {
            description_bytes: 4096,
            relations: 32,
            scopes: 4,
            scope_key_bytes: 64,
            requirements: 8,
            slots: 8,
            checks: 8,
            construction_bytes: 65_536,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    NativeAuthoredProposal {
        content: plan.build(charge).unwrap(),
        declarations: vec![declaration],
        max_responses: 4,
        scope_limits: scope::ScopeLimits {
            scopes: 4,
            roots: 16,
            children: 8,
        },
        owner: None,
    }
}
fn authored_workflow(lab: &mut Lab) {
    let request = lab.next(PARTIES.issuer);
    lab.run(
        PARTIES.issuer,
        NativeInput {
            request,
            command: NativeCommand::CreateAuthored {
                claims: vec![authored_proposal(11, 11300)],
            },
        },
        "authored create",
    );
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(11));
    lab.run(PARTIES.issuer, fx::post(request, expected), "authored post");
    let (request, expected) = (lab.next(PARTIES.issuer), lab.claim(11));
    lab.run(
        PARTIES.issuer,
        fx::cancel(request, expected),
        "authored cancel",
    );
}

#[test]
fn every_row_family_and_every_completion_promise_stay_within_the_future_record_bound() {
    let mut projection = Lab::new(NativeContentProfile::ProjectionOnly);
    projection_workflow(&mut projection);
    let mut authored = Lab::new(NativeContentProfile::AuthoredV1);
    authored_workflow(&mut authored);
    let mut seen = projection.seen.clone();
    seen.extend(authored.seen.iter().copied());
    let missing: Vec<RowFamily> = FAMILIES
        .iter()
        .copied()
        .filter(|family| !seen.contains(family))
        .collect();
    assert!(
        missing.is_empty(),
        "row families never recorded by the workflows: {missing:?}"
    );
    assert!(projection.rows_checked + authored.rows_checked > 200);
}

#[test]
fn the_bound_is_monotone_and_refuses_shapes_beyond_the_encoding_limits() {
    let small = RangeWriteLimits {
        changed_keys: 9,
        deleted_keys: 0,
        deleted_heap: 0,
        incoming_heap: 4096,
        input_capacity: 9,
    };
    let base = future_record_quote(small, encoding()).unwrap();
    assert_eq!(base.rows, 9);
    assert_eq!(
        base.bytes,
        header_fixed_bytes() + 9 * row_fixed_bytes() + HEAP_EXPANSION * 4096
    );
    for (keys, heap) in [(10, 4096), (9, 4097), (64, 1 << 20)] {
        let larger = future_record_quote(
            RangeWriteLimits {
                changed_keys: keys,
                deleted_keys: 0,
                deleted_heap: 0,
                incoming_heap: heap,
                input_capacity: keys,
            },
            encoding(),
        )
        .unwrap();
        assert!(larger.bytes > base.bytes && larger.visits > base.visits);
    }
    let tight = EncodingLimits {
        bytes: base.bytes - 1,
        ..encoding()
    };
    assert!(matches!(
        future_record_quote(small, tight),
        Err(CodecError::Capacity)
    ));
    let exact = EncodingLimits {
        bytes: base.bytes,
        visits: base.visits,
        rows: 9,
    };
    assert_eq!(future_record_quote(small, exact).unwrap(), base);
    assert!(future_record_quote(small, EncodingLimits { rows: 8, ..exact }).is_err());
    assert!(
        future_record_quote(
            RangeWriteLimits {
                changed_keys: usize::MAX,
                ..small
            },
            encoding()
        )
        .is_err()
    );
}
