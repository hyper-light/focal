use super::report_tests::{ISSUER, SUBJECT, binding, context, creation, descriptor, request};
use super::*;
#[path = "admission_late_growth_tests.rs"]
mod admission_late_growth_tests;
#[path = "delivery_tests.rs"]
mod delivery_tests;
#[path = "work_failure_tests.rs"]
mod failure_tests;
#[path = "increment_tests.rs"]
mod increment_tests;
#[path = "projection_owner_tests.rs"]
mod projection_owner_tests;
#[path = "projection_work_tests.rs"]
mod projection_work_tests;
#[path = "respondent_owner_tests.rs"]
mod respondent_owner_tests;
#[path = "respondent_state_tests.rs"]
mod respondent_state_tests;
#[path = "response_position_tests.rs"]
mod response_position_tests;
#[path = "whole_work_entry_tests.rs"]
mod whole_work_entry_tests;
#[path = "work_check_tests.rs"]
mod work_check_tests;
use evidence::SlotBinding;
use focal_evidence::{
    BuiltinNativeSchemas, ContentStore, StoreLimits, error_report_schema, test_report_schema,
};
use focal_model::lifecycle::{
    aggregation,
    artifact_descriptor::{ArtifactSpec, PayloadSpec, WorkProvenance, WorkRole},
};
use focal_model::{ArtifactRef, Confidence, ContentDomainId, OutcomeKind, ValidationMode};

struct Fixture {
    owner: NativeOwner,
    store: ContentStore,
    _directory: tempfile::TempDir,
    serial: u128,
}
impl Fixture {
    fn new() -> Self {
        Self::with_limits(NativeLimits {
            plan_nodes: 16,
            // Multiple responses share one allowance for all projection
            // lookups, including nested work cursors and staged-row searches.
            plan_edges: 1024,
            preparation_bytes: 1024 * 1024,
            evaluations_per_claim: 16,
            range: RangeConfig {
                max_batch_entries: 128,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        })
    }
    fn with_limits(limits: NativeLimits) -> Self {
        Self::with_observe(limits, false)
    }
    fn with_observe(limits: NativeLimits, observe: bool) -> Self {
        let core = Core::new_native(
            binding(1).ledger,
            RangeId(781),
            limits,
            MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
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
        let mut fixture = Self {
            owner: NativeOwner::new(core).unwrap(),
            store,
            _directory: directory,
            serial: 10,
        };
        let requirements = [(ValidationMode::Observe, false)];
        let mut initial = creation(1, 1, if observe { &requirements } else { &[] }, None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut initial.command
        else {
            panic!("create")
        };
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[slot_policy(0), slot_policy(1)],
            declarations,
            aggregation::Limits {
                max_slots: 8,
                max_checks: 16,
                max_results: 32,
                max_updates: 32,
            },
        )
        .unwrap();
        fixture.commit(ISSUER, initial.command);
        fixture.commit(
            ISSUER,
            NativeCommand::Post {
                expected: fixture.claim(),
            },
        );
        if observe {
            let key = super::report_tests::key(1);
            let expected = fixture.owner.effective().evaluation(key).unwrap().binding();
            fixture.commit(
                super::report_tests::EVALUATOR,
                NativeCommand::BeginAdmission {
                    claim: fixture.claim(),
                    key,
                    expected,
                },
            );
        }
        fixture.commit(
            SUBJECT,
            NativeCommand::AcquireReceipt {
                expected: fixture.claim(),
                receipt: ReceiptId::from_u128(701),
            },
        );
        fixture
    }
    fn claim(&self) -> Binding {
        self.owner
            .effective()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .binding()
    }
    fn parent(&self) -> evidence::Parent {
        evidence::Parent::from_claim(self.owner.effective().claim(ClaimId::from_u128(1)).unwrap())
            .unwrap()
    }
    fn input(&mut self, actor: ParticipantId, command: NativeCommand) -> NativeInput {
        self.serial += 1;
        NativeInput {
            request: request(actor, self.serial),
            command,
        }
    }
    fn stage(
        &mut self,
        actor: ParticipantId,
        command: NativeCommand,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let input = self.input(actor, command);
        self.owner.prepare_with_custody(
            context(actor, self.serial as u64),
            input,
            &mut self.store,
            ContentDomainId::from_u128(93),
            &BuiltinNativeSchemas,
        )
    }
    fn commit(&mut self, actor: ParticipantId, command: NativeCommand) -> NativeOutcome {
        let NativeStaging::Prepared { candidate, .. } = self.stage(actor, command).unwrap() else {
            panic!("fresh")
        };
        self.owner.publish_after_durable(candidate).unwrap()
    }
    fn artifact(&self, id: u128, role: WorkRole) -> NativeArtifactInput {
        let parent = self.parent();
        let diagnostic = matches!(role, WorkRole::Diagnostic { .. });
        NativeArtifactInput::new(descriptor(ArtifactSpec {
            ledger: parent.ledger,
            id: ArtifactId::from_u128(id),
            schema: 1,
            kind: if diagnostic { "error" } else { "test-report" },
            schema_hash: if diagnostic {
                error_report_schema()
            } else {
                test_report_schema()
            },
            metadata: b"{}",
            payload: PayloadSpec::Inline(if diagnostic {
                br#"{"code":"work_failed","message":"The requested tests could not pass."}"#
            } else {
                br#"{"passed":3,"failed":0,"skipped":0}"#
            }),
            producer: SUBJECT,
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
    fn work(&mut self, id: u128, slot: u32) -> SlotBinding {
        let artifact = self.artifact(id, WorkRole::Output { slot });
        let binding = artifact.get().unwrap().binding();
        self.commit(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim: self.claim(),
                slot,
                artifact,
            },
        );
        SlotBinding {
            slot,
            artifact: ArtifactRef {
                id: ArtifactId(binding.object.0),
                hash: binding.content,
            },
        }
    }
    fn diagnostic(&mut self, id: u128) -> ArtifactRef {
        let artifact = self.artifact(
            id,
            WorkRole::Diagnostic {
                reason: EvidenceFailure::Work,
            },
        );
        let binding = artifact.get().unwrap().binding();
        self.commit(
            SUBJECT,
            NativeCommand::SubmitDiagnostic {
                claim: self.claim(),
                reason: EvidenceFailure::Work,
                artifact,
            },
        );
        ArtifactRef {
            id: ArtifactId(binding.object.0),
            hash: binding.content,
        }
    }
    fn close(
        &self,
        id: u128,
        outcome: OutcomeKind,
        manifest: Vec<SlotBinding>,
        diagnostics: Vec<ArtifactRef>,
    ) -> NativeCommand {
        NativeCommand::CloseResponse {
            claim: self.claim(),
            response: binding(id),
            report: NativeResponseInput {
                summary: "Respondent finished this attempt.".into(),
                confidence: Confidence::Committed,
                outcome,
                manifest,
                diagnostics,
            },
        }
    }
    fn response(&self, id: u128) -> Binding {
        self.owner
            .effective()
            .response(TestamentId::from_u128(id))
            .unwrap()
            .identity()
            .binding
    }
}
fn slot_policy(slot: u32) -> aggregation::SlotPolicy<'static> {
    aggregation::SlotPolicy {
        slot,
        missing_declaration_index: 20 + slot,
        mode: ValidationMode::Required,
        checks: &[],
    }
}

pub(super) fn registry_override_fixture() -> (ClaimState, RegistrationSet) {
    let mut fixture = Fixture::new();
    let first = fixture.work(801, 0);
    let second = fixture.work(802, 1);
    fixture.commit(
        SUBJECT,
        fixture.close(900, OutcomeKind::Complete, vec![first, second], vec![]),
    );
    fixture.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
    fixture.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: fixture.claim(),
            expected: fixture.response(900),
        },
    );
    let view = fixture.owner.effective();
    let claim = view.claim(ClaimId::from_u128(1)).unwrap();
    let registry = view.registrations(ClaimId::from_u128(1)).unwrap();
    (
        claim.try_copy(claim.retained_bytes().unwrap()).unwrap(),
        registry
            .try_copy(registry.retained_bytes().unwrap())
            .unwrap(),
    )
}

#[test]
fn respondent_closes_actual_evidence_then_posts_and_claimant_receives_separately() {
    let mut f = Fixture::new();
    assert_eq!(f.parent().next_cycle, 1);
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    let b = f.work(802, 1);
    let a = f.work(801, 0);
    let original = f
        .owner
        .committed()
        .work(a.artifact.id)
        .unwrap()
        .state
        .binding();
    f.commit(
        ISSUER,
        NativeCommand::ReceiveWork {
            claim: f.claim(),
            expected: original,
        },
    );
    let read = f.owner.pin(0, 100).unwrap();
    let outcome = f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![a, b], vec![]),
    );
    assert_eq!(outcome.responses, 1);
    let response = f
        .owner
        .committed()
        .response(TestamentId::from_u128(900))
        .unwrap();
    assert_eq!(response.state(), ResponseState::Generated);
    assert_eq!(response.manifest(), &[a, b]);
    assert_eq!(response.reported_outcome(), OutcomeKind::Complete);
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::TestamentGenerated
    );
    for entry in [a, b] {
        assert_eq!(
            f.owner
                .committed()
                .work(entry.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Attached
        );
    }
    assert_eq!(
        read.with_work(a.artifact.id, 0, |row| row.state.state())
            .unwrap(),
        Some(WorkArtifactState::Received)
    );
    assert_eq!(
        read.with_work(b.artifact.id, 0, |row| row.state.state())
            .unwrap(),
        Some(WorkArtifactState::Generated)
    );
    assert!(
        read.with_response(TestamentId::from_u128(900), 0, |row| row.state())
            .unwrap()
            .is_none()
    );
    assert!(
        f.stage(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: f.claim(),
                expected: f.response(900)
            }
        )
        .is_err()
    );
    assert!(
        f.stage(
            ISSUER,
            NativeCommand::PostResponse {
                claim: f.claim(),
                expected: f.response(900)
            }
        )
        .is_err()
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Posted
    );
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::ReceiveResponse {
                claim: f.claim(),
                expected: f.response(900)
            }
        )
        .is_err()
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Received
    );
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::TestamentAcknowledged
    );
    assert!(
        !f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .local_complete()
    );
}

#[test]
fn every_unsuccessful_outcome_needs_its_real_diagnostic_and_remains_receivable() {
    for reported in [
        OutcomeKind::Partial,
        OutcomeKind::Refused,
        OutcomeKind::Impossible,
        OutcomeKind::Interrupted,
        OutcomeKind::Failed,
    ] {
        let mut f = Fixture::new();
        assert!(
            f.stage(SUBJECT, f.close(900, reported, vec![], vec![]))
                .is_err()
        );
        let diagnostic = f.diagnostic(800);
        assert!(
            f.stage(SUBJECT, f.close(900, reported, vec![], vec![]))
                .is_err()
        );
        f.commit(SUBJECT, f.close(900, reported, vec![], vec![diagnostic]));
        f.commit(
            SUBJECT,
            NativeCommand::PostResponse {
                claim: f.claim(),
                expected: f.response(900),
            },
        );
        f.commit(
            ISSUER,
            NativeCommand::ReceiveResponse {
                claim: f.claim(),
                expected: f.response(900),
            },
        );
        let response = f
            .owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap();
        assert!(response.manifest().is_empty());
        assert_eq!(response.diagnostics()[0].artifact(), diagnostic);
        assert_eq!(response.reported_outcome(), reported);
        assert_eq!(response.state(), ResponseState::Received);
        assert!(f.owner.committed().work(diagnostic.id).is_none());
    }
}

#[test]
fn owner_membership_rejects_partial_duplicate_foreign_and_reordered_manifests() {
    let mut f = Fixture::new();
    let a = f.work(801, 0);
    let b = f.work(802, 1);
    let diagnostic = f.diagnostic(803);
    let prefix = f.owner.committed().sequence();
    for manifest in [
        vec![a],
        vec![a, a],
        vec![b, a],
        vec![
            SlotBinding {
                slot: 0,
                artifact: diagnostic,
            },
            b,
        ],
    ] {
        assert!(
            f.stage(
                SUBJECT,
                f.close(900, OutcomeKind::Complete, manifest, vec![diagnostic])
            )
            .is_err()
        );
        assert_eq!(f.owner.committed().sequence(), prefix);
        assert_eq!(
            f.owner
                .committed()
                .work(a.artifact.id)
                .unwrap()
                .state
                .state(),
            WorkArtifactState::Generated
        );
    }
    assert!(
        f.stage(
            ISSUER,
            f.close(900, OutcomeKind::Complete, vec![a, b], vec![diagnostic])
        )
        .is_err()
    );
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![a, b], vec![diagnostic]),
    );
}

#[test]
fn repeated_output_and_errors_have_distinct_cycle_provenance_and_immutable_prior_responses() {
    let mut f = Fixture::new();
    let a = f.work(801, 0);
    let first = f.diagnostic(802);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![a], vec![first]),
    );
    let next_a = f.work(803, 0);
    let next_error = f.diagnostic(804);
    assert_ne!(a.artifact.hash, next_a.artifact.hash);
    assert_ne!(first.hash, next_error.hash);
    assert!(
        f.stage(
            SUBJECT,
            f.close(901, OutcomeKind::Partial, vec![a], vec![next_error])
        )
        .is_err()
    );
    assert!(
        f.stage(
            SUBJECT,
            f.close(901, OutcomeKind::Partial, vec![next_a], vec![first])
        )
        .is_err()
    );
    f.commit(
        SUBJECT,
        f.close(901, OutcomeKind::Partial, vec![next_a], vec![next_error]),
    );
    let view = f.owner.committed();
    let previous = view.response(TestamentId::from_u128(900)).unwrap();
    let next = view.response(TestamentId::from_u128(901)).unwrap();
    assert_eq!(previous.identity().cycle, 1);
    assert_eq!(previous.state(), ResponseState::Generated);
    assert_eq!(next.identity().cycle, 2);
    assert_eq!(next.identity().prior, Some(TestamentId::from_u128(900)));
    assert_eq!(previous.manifest(), &[a]);
}

#[test]
fn pending_close_is_atomic_discardable_and_retry_conflicts_bind_full_report() {
    let mut f = Fixture::new();
    let a = f.work(801, 0);
    let base = f.claim();
    let input = f.input(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![a], vec![]),
    );
    let key = input.request;
    let NativeStaging::Prepared { candidate, outcome } =
        f.owner.prepare(context(SUBJECT, 90), input, None).unwrap()
    else {
        panic!("candidate")
    };
    assert!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .is_none()
    );
    assert_eq!(
        f.owner
            .effective()
            .work(a.artifact.id)
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Attached
    );
    let retry = || NativeInput {
        request: key,
        command: NativeCommand::CloseResponse {
            claim: base,
            response: binding(900),
            report: NativeResponseInput {
                summary: "Respondent finished this attempt.".into(),
                confidence: Confidence::Committed,
                outcome: OutcomeKind::Complete,
                manifest: vec![a],
                diagnostics: vec![],
            },
        },
    };
    assert_eq!(
        f.owner.prepare(context(SUBJECT, 1), retry(), None).unwrap(),
        NativeStaging::Existing {
            outcome,
            candidate: Some(candidate)
        }
    );
    let mut changed = retry();
    let NativeCommand::CloseResponse { report, .. } = &mut changed.command else {
        panic!("close")
    };
    report.summary.push('!');
    assert!(matches!(
        f.owner.prepare(context(SUBJECT, 1), changed, None),
        Err(NativeOwnerError::Native(NativeError::RequestConflict))
    ));
    assert_eq!(f.owner.discard_from(candidate).unwrap(), 1);
    assert_eq!(
        f.owner
            .effective()
            .work(a.artifact.id)
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Generated
    );
    assert_eq!(f.claim(), base);
    let NativeStaging::Prepared { candidate, .. } = f
        .owner
        .prepare(context(SUBJECT, 90), retry(), None)
        .unwrap()
    else {
        panic!("candidate")
    };
    f.owner.publish_after_durable(candidate).unwrap();
    assert!(matches!(
        f.owner.prepare(context(SUBJECT, 0), retry(), None).unwrap(),
        NativeStaging::Existing {
            candidate: None,
            ..
        }
    ));
}

#[test]
fn attachment_wins_observation_race_and_cancellation_preserves_generated_report() {
    let mut f = Fixture::new();
    let a = f.work(801, 0);
    let artifact = f
        .owner
        .committed()
        .work(a.artifact.id)
        .unwrap()
        .state
        .binding();
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![a], vec![]),
    );
    assert!(
        f.stage(
            ISSUER,
            NativeCommand::ReceiveWork {
                claim: f.claim(),
                expected: artifact
            }
        )
        .is_err()
    );
    let response = f.response(900);
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::PostResponse {
                claim: f.claim(),
                expected: response
            }
        )
        .is_err()
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Generated
    );
    assert_eq!(
        f.owner
            .committed()
            .work(a.artifact.id)
            .unwrap()
            .state
            .attachment(),
        Some(TestamentId::from_u128(900))
    );
}

#[test]
fn claimant_can_observe_unattached_work_and_posted_response_after_claim_cancellation() {
    let mut f = Fixture::new();
    let a = f.work(801, 0);
    let diagnostic = f.diagnostic(802);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![a], vec![diagnostic]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let later = f.work(803, 0);
    let work = f
        .owner
        .committed()
        .work(later.artifact.id)
        .unwrap()
        .state
        .binding();
    let response = f.response(900);
    f.commit(
        ISSUER,
        NativeCommand::Cancel {
            expected: f.claim(),
        },
    );
    let terminal = f.claim();
    let cut = f
        .owner
        .committed()
        .claim(ClaimId::from_u128(1))
        .unwrap()
        .terminal_cut();
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveWork {
            claim: terminal,
            expected: work,
        },
    );
    assert_eq!(outcome.changed, 0);
    let outcome = f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: terminal,
            expected: response,
        },
    );
    assert_eq!(
        (outcome.changed, outcome.responses, outcome.events),
        (0, 1, 1)
    );
    assert_eq!(f.claim(), terminal);
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .terminal_cut(),
        cut
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Received
    );
    assert_eq!(
        f.owner
            .committed()
            .work(later.artifact.id)
            .unwrap()
            .state
            .state(),
        WorkArtifactState::Received
    );
}

#[test]
fn output_bound_preserves_room_for_diagnostics_and_atomic_closure() {
    // Ten rows fit one attachment's close and the pure Receipt publication. A
    // second independently admitted output would make the cycle uncloseable.
    // Bound the promised report payload and diagnostic set independently of
    // the ten-row publication limit being exercised here.
    let mut f = Fixture::with_limits(NativeLimits {
        range: RangeConfig {
            max_batch_entries: 10,
            page_bytes: 4096,
            max_entry_bytes: 64 * 1024,
            ..RangeConfig::default()
        },
        plan_nodes: 4,
        plan_edges: 64,
        preparation_bytes: 1024 * 1024,
        diagnostics_per_cycle: 1,
        response_summary_bytes: 256,
        ..NativeLimits::default()
    });
    let a = f.work(801, 0);
    let artifact = f.artifact(802, WorkRole::Output { slot: 1 });
    assert!(
        f.stage(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim: f.claim(),
                slot: 1,
                artifact
            }
        )
        .is_err()
    );
    let diagnostic = f.diagnostic(803);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Partial, vec![a], vec![diagnostic]),
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Generated
    );
}

#[test]
fn malformed_actor_provenance_and_duplicate_slot_are_rejected_before_schema_or_custody() {
    struct RefuseSchemas;
    impl focal_evidence::NativeSchemaVerifier for RefuseSchemas {
        fn maximum_bytes(
            &self,
            _: ContentHash,
        ) -> Result<usize, focal_evidence::BuiltinSchemaError> {
            panic!("unauthorized content reached schema lookup")
        }
        fn verify(
            &self,
            _: ContentHash,
            _: &[u8],
        ) -> Result<(), focal_evidence::BuiltinSchemaError> {
            panic!("unauthorized content reached verifier")
        }
    }
    let mut f = Fixture::new();
    let artifact = f.artifact(801, WorkRole::Output { slot: 0 });
    let input = f.input(
        ISSUER,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot: 0,
            artifact,
        },
    );
    assert!(
        f.owner
            .prepare_with_custody(
                context(ISSUER, 90),
                input,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &RefuseSchemas
            )
            .is_err()
    );
    f.work(801, 0);
    let artifact = f.artifact(802, WorkRole::Output { slot: 0 });
    let input = f.input(
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot: 0,
            artifact,
        },
    );
    assert!(
        f.owner
            .prepare_with_custody(
                context(SUBJECT, 90),
                input,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &RefuseSchemas
            )
            .is_err()
    );
    let artifact = f.artifact(803, WorkRole::Output { slot: 1 });
    let input = f.input(
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: f.claim(),
            slot: 0,
            artifact,
        },
    );
    assert!(
        f.owner
            .prepare_with_custody(
                context(SUBJECT, 90),
                input,
                &mut f.store,
                ContentDomainId::from_u128(93),
                &RefuseSchemas
            )
            .is_err()
    );
}

#[test]
fn live_admission_grant_survives_actual_response_history_and_accepts_late_result() {
    use super::report_tests::{EVALUATOR, artifact_spec, key};
    use focal_model::VerdictValue;
    let mut f = Fixture::with_observe(
        NativeLimits {
            preparation_bytes: 1024 * 1024,
            plan_nodes: 16,
            plan_edges: 256,
            range: RangeConfig {
                max_batch_entries: 128,
                page_entries: 4,
                ..RangeConfig::default()
            },
            ..NativeLimits::default()
        },
        true,
    );
    let first = f.work(801, 0);
    f.commit(
        SUBJECT,
        f.close(900, OutcomeKind::Complete, vec![first], vec![]),
    );
    let second = f.work(802, 0);
    f.commit(
        SUBJECT,
        f.close(901, OutcomeKind::Complete, vec![second], vec![]),
    );
    f.commit(
        SUBJECT,
        NativeCommand::PostResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    f.commit(
        ISSUER,
        NativeCommand::ReceiveResponse {
            claim: f.claim(),
            expected: f.response(900),
        },
    );
    let view = f.owner.committed();
    let state = view.evaluation(key(1)).unwrap();
    let definition = view.definition(key(1).validation).unwrap();
    let attempt = state.bind(definition).unwrap().current_attempt().unwrap();
    let descriptor = descriptor(artifact_spec(803, EVALUATOR, VerdictValue::Pass))
        .with_result_provenance(
            focal_model::lifecycle::artifact_descriptor::ResultProvenance {
                claim: key(1).claim,
                validation: key(1).validation,
                target: state.target(),
                generation: state.generation(),
                attempt,
                value: VerdictValue::Pass,
            },
        )
        .unwrap();
    let report = validation::Report {
        generation: state.generation(),
        attempt,
        value: VerdictValue::Pass,
        evidence: ArtifactRef {
            id: descriptor.id(),
            hash: descriptor.content_hash(),
        },
    };
    let command = NativeCommand::ReportAdmission {
        claim: f.claim(),
        key: key(1),
        expected: state.binding(),
        report,
        artifact: NativeArtifactInput::new(descriptor).unwrap(),
    };
    f.commit(EVALUATOR, command);
    assert_eq!(
        f.owner.committed().evaluation(key(1)).unwrap().state(),
        validation::State::Validated
    );
    assert_eq!(
        f.owner
            .committed()
            .claim(ClaimId::from_u128(1))
            .unwrap()
            .response_count(),
        2
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(900))
            .unwrap()
            .state(),
        ResponseState::Received
    );
    assert_eq!(
        f.owner
            .committed()
            .response(TestamentId::from_u128(901))
            .unwrap()
            .state(),
        ResponseState::Generated
    );
}
