use super::*;
use crate::native::{report_tests as f, *};
use bytes::{CountingSink, Cursor, SliceSink};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_model::lifecycle::{
    aggregation,
    artifact_descriptor::{ArtifactSpec, PayloadSpec, WorkProvenance, WorkRole},
    evidence,
};
use focal_model::{
    ArtifactRef, ContentDomainId, ObjectRevision, ReceiptId, ValidationMode, VerdictValue,
};

macro_rules! encoded {
    ($writer:path, $value:expr) => {{
        let value = $value;
        let mut count = CountingSink::new(1024 * 1024, usize::MAX);
        $writer(&mut count, value).unwrap();
        let (length, visits) = (count.len(), count.visits_used());
        let mut bytes = vec![0xa5; length];
        let mut sink = SliceSink::new(&mut bytes, visits);
        $writer(&mut sink, value).unwrap();
        assert_eq!((sink.len(), sink.visits_used()), (length, visits));
        sink.finish().unwrap();
        let mut insufficient = CountingSink::new(length, visits - 1);
        assert_eq!($writer(&mut insufficient, value), Err(Error::Capacity));
        let mut insufficient = CountingSink::new(length - 1, visits);
        assert_eq!($writer(&mut insufficient, value), Err(Error::Capacity));
        bytes
    }};
}
fn position_bytes(sequence: SessionSeq, ordinal: u32) -> Vec<u8> {
    let mut bytes = sequence.0.to_le_bytes().to_vec();
    bytes.extend_from_slice(&ordinal.to_le_bytes());
    bytes
}
fn assert_binding(cursor: &mut Cursor<'_>, binding: Binding) {
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.ledger.tenant.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.ledger.session.0);
    assert_eq!(cursor.fixed::<16>().unwrap(), binding.object.0);
    assert_eq!(cursor.fixed::<32>().unwrap(), binding.content.0);
    assert_eq!(cursor.u64().unwrap(), binding.revision.0);
}
struct Fixture {
    core: Core<NativeState>,
    store: ContentStore,
    _directory: tempfile::TempDir,
    serial: u128,
}

pub(in crate::native::record_codec) fn recovery_fixture(
    stage: u8,
) -> (Core<NativeState>, ContentStore, tempfile::TempDir) {
    recovery_fixture_with_checks(stage, true)
}
pub(in crate::native::record_codec) fn recovery_fixture_with_checks(
    stage: u8,
    checked: bool,
) -> (Core<NativeState>, ContentStore, tempfile::TempDir) {
    let mut fixture = Fixture::with_checks(checked);
    fixture.failed_response();
    if stage >= 1 {
        fixture.apply(
            f::SUBJECT,
            NativeCommand::PostResponse {
                claim: fixture.claim(),
                expected: fixture.response(),
            },
        );
    }
    if stage >= 2 {
        fixture.apply(
            f::ISSUER,
            NativeCommand::ReceiveResponse {
                claim: fixture.claim(),
                expected: fixture.response(),
            },
        );
    }
    if stage >= 3 {
        fixture.apply(
            f::ISSUER,
            NativeCommand::EnterWholeWork {
                claim: fixture.claim(),
                expected: fixture.response(),
            },
        );
    }
    (fixture.core, fixture.store, fixture._directory)
}
impl Fixture {
    fn new() -> Self {
        Self::with_checks(false)
    }
    fn with_checks(checked: bool) -> Self {
        let mut core = f::core();
        let mut input = f::creation(1, 1, &[], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut input.command
        else {
            panic!("create")
        };
        let checks = [1u32, 2].map(|slot| aggregation::CheckPolicy {
            declaration_index: 30 + slot,
            validation: ValidationId::from_u128(300 + u128::from(slot)),
            mode: ValidationMode::Required,
        });
        if checked {
            for slot in [1u32, 2] {
                let handler = focal_model::HandlerRef {
                    id: focal_model::ValidatorId::from_u128(901),
                    version: ContentHash([9; 32]),
                    agentic: false,
                };
                declarations.push(
                    validation::Declaration::new(
                        Principal::Actor(f::ISSUER),
                        validation::DeclarationSpec {
                            binding: f::binding(300 + u128::from(slot)),
                            claim: ClaimId::from_u128(1),
                            issuer: f::ISSUER,
                            declaration_index: 30 + slot,
                            kind: focal_model::ValidationKind::Inspection,
                            phase: focal_model::ValidationPhase::WholeWork,
                            mode: ValidationMode::Required,
                            target: validation::TargetDeclaration::WholeWorkSlot {
                                index: slot,
                                name: if slot == 1 { "first" } else { "second" },
                            },
                            program: validation::Program::Programmatic {
                                check: validation::PhasePolicy {
                                    evaluator: f::EVALUATOR,
                                    definition: ContentHash([8; 32]),
                                    required_policy: None,
                                    handlers: &[validation::HandlerPolicy {
                                        handler: &handler,
                                        attempts: 1,
                                        proof_schema: focal_evidence::test_report_schema(),
                                        diagnostic_schema: focal_evidence::error_report_schema(),
                                    }],
                                },
                                quality: None,
                            },
                            deadline: focal_model::Deadline {
                                timer: focal_model::TimerId::from_u128(910 + u128::from(slot)),
                                generation: 1,
                                at: 1000,
                            },
                        },
                        validation::Limits {
                            handlers: 1,
                            attempts: 1,
                            slot_bytes: 32,
                        },
                    )
                    .unwrap(),
                );
            }
        }
        let slots: Vec<_> = (0..3)
            .map(|slot| aggregation::SlotPolicy {
                slot,
                missing_declaration_index: 20 + slot,
                mode: ValidationMode::Required,
                checks: if checked && slot != 0 {
                    std::slice::from_ref(&checks[(slot - 1) as usize])
                } else {
                    &[]
                },
            })
            .collect();
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            f::binding(1),
            f::ISSUER,
            &slots,
            declarations,
            aggregation::Limits {
                max_slots: 8,
                max_checks: 16,
                max_results: 32,
                max_updates: 32,
            },
        )
        .unwrap();
        f::publish(&mut core, 10, input);
        f::publish(&mut core, 20, f::post(2, f::binding(1)));
        let directory = tempfile::tempdir().unwrap();
        let store = ContentStore::open(
            directory.path(),
            StoreLimits {
                max_content_bytes: 2 * 1024 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 17,
                max_manifest_bytes: 128 * 1024,
            },
        )
        .unwrap();
        let mut value = Self {
            core,
            store,
            _directory: directory,
            serial: 30,
        };
        value.apply(
            f::SUBJECT,
            NativeCommand::AcquireReceipt {
                expected: value.claim(),
                receipt: ReceiptId::from_u128(701),
            },
        );
        value
    }
    fn claim(&self) -> Binding {
        self.core
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .binding()
    }
    fn response(&self) -> Binding {
        self.core
            .native_response(TestamentId::from_u128(900))
            .unwrap()
            .identity()
            .binding
    }
    fn apply(&mut self, actor: ParticipantId, command: NativeCommand) -> NativeOutcome {
        self.serial += 1;
        let input = NativeInput {
            request: f::request(actor, self.serial),
            command,
        };
        let descriptor = match &input.command {
            NativeCommand::SubmitWork { artifact, .. }
            | NativeCommand::SubmitDiagnostic { artifact, .. } => artifact.get(),
            _ => None,
        };
        let token = descriptor.map(|descriptor| {
            self.store
                .verify_native_artifact(
                    input.request,
                    descriptor,
                    ContentDomainId::from_u128(93),
                    &self.core.state.budget,
                    &BuiltinNativeSchemas,
                )
                .unwrap()
        });
        let prepared = f::prepared(self.core.prepare_native_evidenced(
            f::context(actor, self.serial as u64),
            input,
            &[],
            token.as_ref(),
        ));
        self.core.publish_native(prepared).unwrap()
    }
    fn artifact(&self, id: u128, role: WorkRole) -> NativeArtifactInput {
        let parent =
            evidence::Parent::from_claim(self.core.native_claim(ClaimId::from_u128(1)).unwrap())
                .unwrap();
        let diagnostic = matches!(role, WorkRole::Diagnostic { .. });
        NativeArtifactInput::new(f::descriptor(ArtifactSpec {
            ledger: parent.ledger,
            id: ArtifactId::from_u128(id),
            schema: 1,
            kind: if diagnostic { "error" } else { "test-report" },
            schema_hash: if diagnostic {
                focal_evidence::error_report_schema()
            } else {
                focal_evidence::test_report_schema()
            },
            metadata: b"{}",
            payload: PayloadSpec::Inline(if diagnostic {
                br#"{"code":"failed","message":"The requested work failed."}"#
            } else {
                br#"{"passed":3,"failed":0,"skipped":0}"#
            }),
            producer: parent.holder,
            receipt: Some(parent.receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role,
            }),
            inputs: &[],
            visibility: &[],
        }))
        .unwrap()
    }
    fn submit(&mut self, id: u128, role: WorkRole) -> ArtifactRef {
        let artifact = self.artifact(id, role);
        let descriptor = artifact.get().unwrap();
        let reference = ArtifactRef {
            id: descriptor.id(),
            hash: descriptor.content_hash(),
        };
        let command = match role {
            WorkRole::Output { slot } => NativeCommand::SubmitWork {
                claim: self.claim(),
                slot,
                artifact,
            },
            WorkRole::Diagnostic { reason } => NativeCommand::SubmitDiagnostic {
                claim: self.claim(),
                reason,
                artifact,
            },
            _ => panic!("fixture role"),
        };
        self.apply(f::SUBJECT, command);
        reference
    }
    fn failed_response(&mut self) {
        let output = self.submit(801, WorkRole::Output { slot: 0 });
        let production = self.submit(
            802,
            WorkRole::Diagnostic {
                reason: evidence::EvidenceFailure::Production,
            },
        );
        self.apply(
            f::SUBJECT,
            NativeCommand::FailWorkProduction {
                claim: self.claim(),
                slot: 1,
                diagnostic: production,
            },
        );
        let failure = self.submit(
            803,
            WorkRole::Diagnostic {
                reason: evidence::EvidenceFailure::Work,
            },
        );
        self.apply(
            f::SUBJECT,
            NativeCommand::CloseResponse {
                claim: self.claim(),
                response: f::binding(900),
                report: NativeResponseInput {
                    summary: "Respondent résumé: partial output; failures retained.".into(),
                    confidence: Confidence::Tentative,
                    outcome: OutcomeKind::Failed,
                    manifest: vec![evidence::SlotBinding {
                        slot: 0,
                        artifact: output,
                    }],
                    diagnostics: vec![production, failure],
                },
            },
        );
    }
}

#[test]
fn actual_work_error_rows_keep_links_original_artifacts_and_recovery_tree_address() {
    let mut fixture = Fixture::new();
    fixture.failed_response();
    let Some(Row::Artifact(row)) = fixture
        .core
        .state
        .rows
        .get(&Key::Artifact(ArtifactId::from_u128(803)))
    else {
        panic!("artifact")
    };
    let encoded = encoded!(artifact, row);
    let actual = row.get().unwrap();
    let mut measure = CountingSink::new(1024 * 1024, usize::MAX);
    descriptors::artifact(&mut measure, actual.descriptor()).unwrap();
    let tail = encoded.get(measure.len()..).unwrap();
    let mut cursor = Cursor::new(tail, tail.len(), usize::MAX).unwrap();
    let pointer = actual.custody().payload();
    assert_eq!(cursor.fixed::<16>().unwrap(), pointer.domain.0);
    assert_eq!(cursor.fixed::<32>().unwrap(), pointer.root.0);
    assert_eq!(cursor.u64().unwrap(), pointer.length);
    assert_eq!(cursor.u16().unwrap(), 2); // Evidence content, explicit stable tag.
    assert_eq!(cursor.u64().unwrap(), actual.custody().local_revision());
    cursor.finish().unwrap();
    let Some(Row::Diagnostic(row)) = fixture
        .core
        .state
        .rows
        .get(&Key::Diagnostic(ArtifactId::from_u128(803)))
    else {
        panic!("diagnostic")
    };
    let bytes = encoded!(diagnostic, row);
    assert!(bytes.ends_with(&[&[1][..], &ArtifactId::from_u128(802).0].concat()));
    let Some(Row::Work(row)) = fixture
        .core
        .state
        .rows
        .get(&Key::Work(ArtifactId::from_u128(802)))
    else {
        panic!("work")
    };
    let bytes = encoded!(work, row);
    let source = row.get().unwrap();
    let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
    assert_binding(&mut cursor, source.state.binding());
    assert_eq!(cursor.fixed::<16>().unwrap(), ClaimId::from_u128(1).0);
    assert_eq!(cursor.u32().unwrap(), 1);
    assert_eq!(cursor.u32().unwrap(), source.state.cycle());
    assert_eq!(cursor.fixed::<16>().unwrap(), f::SUBJECT.0);
    assert_eq!(
        cursor.fixed::<16>().unwrap(),
        source.state.receipt().receipt.0
    );
    assert_eq!(cursor.u64().unwrap(), source.state.receipt().epoch);
    assert_eq!(cursor.u8().unwrap(), 1); // GenerationFailed.
    assert_eq!(cursor.u8().unwrap(), 0); // Never attached.
    assert_eq!(cursor.u8().unwrap(), 1); // Real diagnostic.
    assert_eq!(cursor.u8().unwrap(), 1); // Production.
    assert_eq!(cursor.fixed::<16>().unwrap(), ArtifactId::from_u128(802).0);
    assert_eq!(
        cursor.fixed::<32>().unwrap(),
        source.state.reference().hash.0
    );
    assert_eq!(cursor.u8().unwrap(), 0); // No aggregate terminal cut.
    assert_eq!(cursor.u8().unwrap(), 1);
    assert_eq!(cursor.fixed::<16>().unwrap(), ArtifactId::from_u128(801).0);
    cursor.finish().unwrap();
}

#[test]
fn response_and_pure_results_preserve_original_publications_through_terminal_missing_work() {
    let mut fixture = Fixture::new();
    fixture.failed_response();
    let original = fixture.response();
    for stage in 0..3 {
        let Some(Row::Response(row)) = fixture
            .core
            .state
            .rows
            .get(&Key::Response(TestamentId::from_u128(900)))
        else {
            panic!("response")
        };
        let bytes = encoded!(response, row);
        let record = row.record().unwrap();
        assert_eq!(record.generated(), original);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert_binding(&mut cursor, record.response().identity().binding);
        assert_eq!(cursor.u64().unwrap(), original.revision.0);
        let copied = row.copy().unwrap();
        assert_eq!(encoded!(response, &copied), bytes);
        if stage == 0 {
            fixture.apply(
                f::SUBJECT,
                NativeCommand::PostResponse {
                    claim: fixture.claim(),
                    expected: fixture.response(),
                },
            );
        } else if stage == 1 {
            let outcome = fixture.apply(
                f::ISSUER,
                NativeCommand::ReceiveResponse {
                    claim: fixture.claim(),
                    expected: fixture.response(),
                },
            );
            let key = (0..outcome.events)
                .find_map(|ordinal| {
                    match fixture
                        .core
                        .native_event(outcome.sequence, ordinal)
                        .unwrap()
                        .fact
                    {
                        NativeFact::Delivery { key } => Some(key),
                        _ => None,
                    }
                })
                .unwrap();
            let Some(Row::DeliveryResult(row)) =
                fixture.core.state.rows.get(&Key::DeliveryResult(key))
            else {
                panic!("delivery")
            };
            let bytes = encoded!(delivery, row);
            let value = row.get().unwrap();
            assert!(bytes.ends_with(&position_bytes(value.sequence(), value.ordinal())));
            assert_eq!(value.ordinal(), 2);
        }
    }
    let outcome = fixture.apply(
        f::ISSUER,
        NativeCommand::EnterWholeWork {
            claim: fixture.claim(),
            expected: fixture.response(),
        },
    );
    let Some(Row::Response(row)) = fixture
        .core
        .state
        .rows
        .get(&Key::Response(TestamentId::from_u128(900)))
    else {
        panic!("response")
    };
    let bytes = encoded!(response, row);
    assert_eq!(row.record().unwrap().generated(), original);
    assert_eq!(
        row.get().unwrap().state(),
        evidence::ResponseState::ValidationIncomplete
    );
    let entered = row.record().unwrap().entered().unwrap();
    assert_eq!(entered.sequence, outcome.sequence);
    assert!(
        bytes.ends_with(&[&[1][..], &position_bytes(entered.sequence, entered.ordinal)].concat())
    );
    let keys: Vec<_> = (0..outcome.events)
        .filter_map(|ordinal| {
            match fixture
                .core
                .native_event(outcome.sequence, ordinal)
                .unwrap()
                .fact
            {
                NativeFact::Missing { key } => Some(key),
                _ => None,
            }
        })
        .collect();
    // Missing required presence is an aggregate cause. With no WholeWork
    // validation declarations it must not fabricate separate missing results.
    assert!(keys.is_empty());
}

#[test]
fn actual_peer_report_result_keeps_attempt_artifact_and_accepted_ordinal() {
    let mut core = f::running(&[(ValidationMode::Required, false)]);
    let mut custody = f::Custody::new();
    let input = f::report_for(
        &core,
        None,
        500,
        1,
        VerdictValue::Error,
        f::descriptor(f::artifact_spec(501, f::EVALUATOR, VerdictValue::Error)),
    );
    let token = f::verified(&mut custody, &input);
    let prepared = f::report(&core, input, &[], &token);
    let result = prepared
        .evaluation(f::key(1))
        .unwrap()
        .last_result()
        .unwrap();
    core.publish_native(prepared).unwrap();
    let Some(Row::Accepted(row)) = core
        .state
        .rows
        .get(&Key::Accepted(NativeResultKey::of(result)))
    else {
        panic!("accepted")
    };
    let bytes = encoded!(accepted, row);
    let value = row.get().unwrap();
    assert_eq!(value.result().verdict(), VerdictValue::Error);
    assert_eq!(value.ordinal(), 2);
    assert!(bytes.ends_with(&position_bytes(value.sequence(), 2)));
    let proof_suffix = [
        &value.artifact().reference().id.0[..],
        &value.artifact().reference().hash.0,
        &value.artifact().producer().0,
        &position_bytes(value.sequence(), 2),
    ]
    .concat();
    assert!(bytes.ends_with(&proof_suffix));
}

#[test]
fn original_generation_revision_is_recorded_before_delivery_and_survives_owned_copies() {
    let parent = evidence::Parent {
        ledger: f::binding(1).ledger,
        claim: ClaimId::from_u128(1),
        issuer: f::ISSUER,
        holder: f::SUBJECT,
        receipt: ReceiptFence {
            receipt: ReceiptId::from_u128(2),
            epoch: 3,
        },
        status: focal_model::ClaimStatus::Received,
        local_complete: false,
        latest_response: None,
        next_cycle: 1,
    };
    let original = Binding {
        revision: ObjectRevision(7),
        ..f::binding(700)
    };
    let report = Response::close(
        evidence::ResponseIdentity {
            binding: original,
            claim: parent.claim,
            receipt: parent.receipt,
            cycle: 1,
            prior: None,
        },
        &parent,
        Principal::Actor(parent.holder),
        &[],
        &[],
        evidence::CloseReport {
            summary: "No outputs were requested.",
            confidence: Confidence::Committed,
            outcome: OutcomeKind::Complete,
            diagnostics: &[],
            limits: evidence::ResponseLimits {
                artifacts: 0,
                diagnostics: 0,
                summary_bytes: 128,
                construction_bytes: 4096,
            },
        },
    )
    .unwrap()
    .response;
    let generated = OwnedResponse::new(report).unwrap();
    let report = generated.get().unwrap();
    let posted = generated
        .transition(
            report
                .plan_post(
                    &report.identity().binding,
                    &parent,
                    Principal::Actor(parent.holder),
                )
                .unwrap(),
            PublicationPosition {
                sequence: SessionSeq(8),
                ordinal: 4,
            },
        )
        .unwrap();
    let report = posted.get().unwrap();
    let received = posted
        .transition(
            report
                .plan_receive(
                    &report.identity().binding,
                    &parent,
                    Principal::Actor(parent.issuer),
                )
                .unwrap(),
            PublicationPosition {
                sequence: SessionSeq(9),
                ordinal: 6,
            },
        )
        .unwrap();
    for row in [&generated, &posted, &received] {
        assert_eq!(row.record().unwrap().generated(), original);
        let bytes = encoded!(response, row);
        let mut cursor = Cursor::new(&bytes, bytes.len(), usize::MAX).unwrap();
        assert_binding(&mut cursor, row.get().unwrap().identity().binding);
        assert_eq!(cursor.u64().unwrap(), 7);
        assert_eq!(encoded!(response, &row.copy().unwrap()), bytes);
    }
    assert_eq!(
        received.get().unwrap().identity().binding.revision,
        ObjectRevision(9)
    );
    assert_eq!(
        received.record().unwrap().received(),
        Some(PublicationPosition {
            sequence: SessionSeq(9),
            ordinal: 6
        })
    );
}

fn authored_definition() -> focal_model::lifecycle::validation_descriptor::ValidationDescriptor {
    use focal_model::lifecycle::validation_descriptor::{
        Limits, ValidationDescriptor, ValidationSpec,
    };
    let plan = ValidationDescriptor::prepare(
        Principal::Actor(f::ISSUER),
        ValidationSpec {
            ledger: f::binding(1).ledger,
            id: ValidationId::from_u128(710),
            schema: 1,
            claim: ClaimId::from_u128(700),
            issuer: f::ISSUER,
            declaration_index: 1,
            kind: focal_model::ValidationKind::Receipt,
            phase: focal_model::ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: focal_model::Deadline {
                timer: focal_model::TimerId::from_u128(711),
                generation: 2,
                at: 1000,
            },
            description: "Record the claimant's actual receipt.",
            quality_bar: None,
            contributed_by: &[f::ISSUER],
            policy_revision: 3,
        },
        Limits {
            declaration: validation::Limits {
                handlers: 1,
                attempts: 1,
                slot_bytes: 16,
            },
            description_bytes: 128,
            quality_bar_bytes: 128,
            contributors: 4,
            construction_bytes: 8192,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}
fn authored_content(
    pin: focal_model::RequirementRef,
) -> focal_model::lifecycle::claim_descriptor::ClaimDescriptor {
    use focal_model::lifecycle::claim_descriptor::{ClaimDescriptor, ClaimSpec, Limits, ScopeSpec};
    use focal_model::{Relation, RelationKind, RelationTarget};
    let mut relations = [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(f::ISSUER),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(f::SUBJECT),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(focal_model::ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Object(focal_model::ObjectRef::claim(
                f::binding(1).ledger,
                ClaimId::from_u128(500),
            )),
        },
    ];
    relations.sort();
    let requirements = [pin];
    let plan = ClaimDescriptor::prepare(
        ClaimSpec {
            ledger: f::binding(1).ledger,
            id: ClaimId::from_u128(700),
            schema: 1,
            occurrence: focal_model::OccurrenceId::from_u128(701),
            description: "Retain the original requested work and scope.",
            relations: &relations,
            scopes: &[ScopeSpec {
                kind: focal_model::ScopeKind::File,
                key: "src/lib.rs",
            }],
            requirements: &requirements,
            slots: &[],
            deadline: None,
            policy: None,
        },
        Limits {
            description_bytes: 128,
            relations: 8,
            scopes: 4,
            scope_key_bytes: 64,
            requirements: 4,
            slots: 4,
            checks: 4,
            construction_bytes: 16384,
        },
    )
    .unwrap();
    let charge = plan.construction_charge();
    plan.build(charge).unwrap()
}

#[test]
fn authored_and_legacy_definitions_keep_distinct_bodies_and_original_claim_profile() {
    let descriptor = authored_definition();
    let pin = focal_model::RequirementRef {
        id: ValidationId::from_u128(710),
        specification: descriptor.specification_hash(),
    };
    let body = encoded!(descriptors::validation, &descriptor);
    let authored = OwnedDeclaration::new_authored(descriptor).unwrap();
    let bytes = encoded!(definition, &authored);
    assert_eq!(bytes[0], 1);
    assert_eq!(&bytes[1..], &body);
    let legacy = authored
        .get()
        .unwrap()
        .try_copy(authored.get().unwrap().retained_bytes().unwrap())
        .unwrap();
    let legacy = OwnedDeclaration::new(legacy).unwrap();
    let bytes = encoded!(definition, &legacy);
    assert_eq!(bytes[0], 0);
    assert_eq!(
        &bytes[1..],
        &encoded!(descriptors::declaration, legacy.get().unwrap())
    );
    let descriptor = authored_content(pin);
    let body = encoded!(descriptors::claim, &descriptor);
    let owner = focal_model::lifecycle::creation::Owner {
        expected: f::binding(500),
        receipt: Some(ReceiptFence {
            receipt: ReceiptId::from_u128(501),
            epoch: 6,
        }),
    };
    let profile = scope::ScopeLimits {
        scopes: 4,
        roots: 9,
        children: 11,
    };
    let row = OwnedClaimContent::new(descriptor, 7, profile, Some(owner)).unwrap();
    let bytes = encoded!(claim_content, &row);
    assert_eq!(&bytes[..body.len()], &body);
    let mut cursor = Cursor::new(&bytes[body.len()..], bytes.len(), usize::MAX).unwrap();
    assert_eq!(cursor.u32().unwrap(), 7);
    assert_eq!(cursor.u32().unwrap(), 4);
    assert_eq!(cursor.u32().unwrap(), 9);
    assert_eq!(cursor.u32().unwrap(), 11);
    assert_eq!(cursor.u8().unwrap(), 1);
    assert_binding(&mut cursor, owner.expected);
    assert_eq!(cursor.u8().unwrap(), 1);
    assert_eq!(
        cursor.fixed::<16>().unwrap(),
        owner.receipt.unwrap().receipt.0
    );
    assert_eq!(cursor.u64().unwrap(), 6);
    cursor.finish().unwrap();
    assert_eq!(encoded!(claim_content, &row.copy().unwrap()), bytes);
}

#[test]
fn creation_mapping_vector_retains_order_family_schema_requested_and_resolved_ids() {
    use crate::native::creation_result::{NativeCreatedObject, NativeCreationResult};
    let entries = vec![
        NativeCreatedObject {
            ordinal: 0,
            family: NativeCreatedFamily::Claim,
            schema: 1,
            content: ContentHash([3; 32]),
            requested: focal_model::ObjectId::from_u128(10),
            resolved: focal_model::ObjectId::from_u128(20),
        },
        NativeCreatedObject {
            ordinal: 1,
            family: NativeCreatedFamily::Validation,
            schema: 1,
            content: ContentHash([4; 32]),
            requested: focal_model::ObjectId::from_u128(10),
            resolved: focal_model::ObjectId::from_u128(20),
        },
    ];
    let mut expected = 2u32.to_le_bytes().to_vec();
    for (index, value) in entries.iter().enumerate() {
        expected.extend_from_slice(&(index as u32).to_le_bytes());
        expected.push(index as u8);
        expected.extend_from_slice(&1u16.to_le_bytes());
        expected.extend_from_slice(&value.content.0);
        expected.extend_from_slice(&value.requested.0);
        expected.extend_from_slice(&value.resolved.0);
    }
    let value = NativeCreationResult::from_owned(entries, 2, 3, usize::MAX).unwrap();
    let row = OwnedCreationResult::new(value).unwrap();
    assert_eq!(encoded!(creation, &row), expected);
}

#[test]
fn declared_missing_results_encode_their_actual_original_publications() {
    let mut fixture = Fixture::with_checks(true);
    fixture.failed_response();
    fixture.apply(
        f::SUBJECT,
        NativeCommand::PostResponse {
            claim: fixture.claim(),
            expected: fixture.response(),
        },
    );
    fixture.apply(
        f::ISSUER,
        NativeCommand::ReceiveResponse {
            claim: fixture.claim(),
            expected: fixture.response(),
        },
    );
    fixture.apply(
        f::ISSUER,
        NativeCommand::EnterWholeWork {
            claim: fixture.claim(),
            expected: fixture.response(),
        },
    );
    let mut preserved = Vec::new();
    for slot in [1u32, 2] {
        let key = EvaluationKey {
            claim: ClaimId::from_u128(1),
            validation: ValidationId::from_u128(300 + u128::from(slot)),
            target: EvaluationTarget::MissingSlot {
                response: TestamentId::from_u128(900),
                slot,
            },
            generation: 1,
        };
        let result = fixture
            .core
            .native_evaluation(key)
            .unwrap()
            .last_result()
            .unwrap();
        let key = NativeResultKey::of(result);
        let Some(Row::MissingResult(row)) = fixture.core.state.rows.get(&Key::MissingResult(key))
        else {
            panic!("missing result")
        };
        let bytes = encoded!(missing, row);
        let value = row.get().unwrap();
        assert_eq!(value.result().phase(), validation::Phase::MissingTarget);
        assert_eq!(
            fixture
                .core
                .native_event(value.sequence(), value.ordinal())
                .unwrap()
                .fact,
            NativeFact::Missing { key }
        );
        assert!(bytes.ends_with(&position_bytes(value.sequence(), value.ordinal())));
        preserved.push((key, bytes, value.sequence(), value.ordinal()));
    }
    // Advance the real ledger without touching this response or its results.
    fixture.apply(f::ISSUER, f::creation(600, 2, &[], None).command);
    for (key, bytes, sequence, ordinal) in preserved {
        let Some(Row::MissingResult(row)) = fixture.core.state.rows.get(&Key::MissingResult(key))
        else {
            panic!("retained result")
        };
        assert_eq!(encoded!(missing, row), bytes);
        assert_eq!(
            (row.get().unwrap().sequence(), row.get().unwrap().ordinal()),
            (sequence, ordinal)
        );
    }
}
