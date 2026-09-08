use super::*;
use crate::native::report_tests::{self as helpers, ISSUER, SUBJECT, binding};
use focal_evidence::{BuiltinNativeSchemas, ContentStore, StoreLimits};
use focal_memory::{BudgetLane, Change, Entry};
use focal_model::lifecycle::artifact_descriptor::{WorkProvenance, WorkRole};
use focal_model::{ArtifactRef, Confidence, ContentDomainId, OutcomeKind, VerdictValue};

#[path = "retired_cycle_tests.rs"]
mod retired_cycle_tests;

struct Rows {
    core: Core<NativeState>,
    store: ContentStore,
    _directory: tempfile::TempDir,
    serial: u128,
}
impl Rows {
    fn new(max_responses: u32) -> Self {
        let mut core = Core::new_native(
            binding(1).ledger,
            RangeId(1901),
            NativeLimits {
                plan_nodes: 16,
                plan_edges: 256,
                preparation_bytes: 1024 * 1024,
                evaluations_per_claim: 16,
                range: RangeConfig {
                    page_entries: 1,
                    max_batch_entries: 128,
                    ..RangeConfig::default()
                },
                ..NativeLimits::default()
            },
            MemoryBudget::new(128 * 1024 * 1024, 16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let mut initial = helpers::creation(1, 1, &[], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut initial.command
        else {
            panic!("create")
        };
        claims[0].definition.max_responses = max_responses;
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[slot_policy(0), slot_policy(1)],
            declarations,
            aggregation::Limits {
                max_slots: 2,
                max_checks: 4,
                max_results: 16,
                max_updates: 16,
            },
        )
        .unwrap();
        helpers::publish(&mut core, 1, initial);
        helpers::publish(&mut core, 2, helpers::post(2, binding(1)));
        let directory = tempfile::tempdir().unwrap();
        let store = ContentStore::open(
            directory.path(),
            StoreLimits {
                max_content_bytes: 1024 * 1024,
                max_staging_bytes: 2 * 1024 * 1024,
                max_uploads: 8,
                chunk_bytes: 17,
                max_manifest_bytes: 65536,
            },
        )
        .unwrap();
        let mut rows = Self {
            core,
            store,
            _directory: directory,
            serial: 10,
        };
        rows.send(
            SUBJECT,
            NativeCommand::AcquireReceipt {
                expected: rows.claim(),
                receipt: ReceiptId::from_u128(701),
            },
        );
        rows
    }
    fn claim(&self) -> Binding {
        self.core
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .binding()
    }
    fn parent(&self) -> evidence::Parent {
        evidence::Parent::from_claim(self.core.native_claim(ClaimId::from_u128(1)).unwrap())
            .unwrap()
    }
    fn view(&self) -> View<'_> {
        View {
            state: &self.core.state,
            tail: None,
        }
    }
    fn prepare(&mut self, actor: ParticipantId, command: NativeCommand) -> NativePrepared {
        self.serial += 1;
        let input = NativeInput {
            request: helpers::request(actor, self.serial),
            command,
        };
        let descriptor = match &input.command {
            NativeCommand::SubmitWork { artifact, .. }
            | NativeCommand::SubmitDiagnostic { artifact, .. }
            | NativeCommand::RejectWork { artifact, .. } => Some(artifact.get().unwrap()),
            _ => None,
        };
        let evidence = descriptor.map(|descriptor| {
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
        helpers::prepared(self.core.prepare_native_evidenced(
            helpers::context(actor, self.serial as u64),
            input,
            &[],
            evidence.as_ref(),
        ))
    }
    fn send(&mut self, actor: ParticipantId, command: NativeCommand) -> NativeOutcome {
        let prepared = self.prepare(actor, command);
        self.core.publish_native(prepared).unwrap()
    }
    fn artifact(&self, id: u128, role: WorkRole, actor: ParticipantId) -> NativeArtifactInput {
        let error = !matches!(role, WorkRole::Output { .. });
        let mut spec = helpers::artifact_spec(
            id,
            actor,
            if error {
                VerdictValue::Incomplete
            } else {
                VerdictValue::Pass
            },
        );
        spec.receipt = Some(self.parent().receipt);
        spec.work = Some(WorkProvenance {
            claim: self.parent().claim,
            cycle: self.parent().next_cycle,
            role,
        });
        spec.visibility = &[];
        NativeArtifactInput::new(helpers::descriptor(spec)).unwrap()
    }
    fn output(&mut self, id: u128, slot: u32) -> ArtifactRef {
        let artifact = self.artifact(id, WorkRole::Output { slot }, SUBJECT);
        let reference = ArtifactRef {
            id: artifact.get().unwrap().id(),
            hash: artifact.get().unwrap().content_hash(),
        };
        self.send(
            SUBJECT,
            NativeCommand::SubmitWork {
                claim: self.claim(),
                slot,
                artifact,
            },
        );
        reference
    }
    fn close(
        &mut self,
        id: u128,
        manifest: Vec<evidence::SlotBinding>,
        diagnostics: Vec<ArtifactRef>,
    ) {
        self.send(
            SUBJECT,
            NativeCommand::CloseResponse {
                claim: self.claim(),
                response: binding(id),
                report: NativeResponseInput {
                    summary: "Actual respondent report for work traversal.".into(),
                    confidence: Confidence::Committed,
                    outcome: if diagnostics.is_empty() {
                        OutcomeKind::Complete
                    } else {
                        OutcomeKind::Failed
                    },
                    manifest,
                    diagnostics,
                },
            },
        );
    }
    fn collect(&self) -> Vec<(ArtifactId, WorkArtifactState, u32)> {
        let view = self.view();
        super::super::projection_work::works(&view, ClaimId::from_u128(1), self.core.limits)
            .map(|row| {
                let row = row.unwrap();
                (row.reference().id, row.state(), row.cycle())
            })
            .collect()
    }
    /// Test-only corrupted index publication; each leaf has exactly one row, so
    /// replacing this entry cannot require an unrelated row copier.
    fn replace(&mut self, key: Key, row: Row, heap: usize) {
        let next = self
            .core
            .state
            .rows
            .prepare_batch_with(
                self.core.state.rows.prefix() + 1,
                vec![Change::Put(Entry::new(key, row, heap))],
                BudgetLane::Completion,
                |_| panic!("single-row replacement must not copy another value"),
            )
            .unwrap();
        self.core.state.rows.publish(next).unwrap();
    }
}

#[test]
fn work_iteration_includes_pending_open_and_reverse_closed_cycles_at_authored_limit() {
    let mut rows = Rows::new(2);
    assert!(rows.collect().is_empty());
    let first = rows.output(800, 0);
    let expected = rows.core.native_work(first.id).unwrap().state.binding();
    rows.send(
        ISSUER,
        NativeCommand::ReceiveWork {
            claim: rows.claim(),
            expected,
        },
    );
    let second = rows.output(801, 1);
    assert_eq!(
        rows.collect(),
        vec![
            (second.id, WorkArtifactState::Generated, 1),
            (first.id, WorkArtifactState::Received, 1)
        ]
    );
    rows.close(
        900,
        vec![
            evidence::SlotBinding {
                slot: 0,
                artifact: first,
            },
            evidence::SlotBinding {
                slot: 1,
                artifact: second,
            },
        ],
        vec![],
    );
    let artifact = rows.artifact(802, WorkRole::Output { slot: 0 }, SUBJECT);
    let pending = rows.prepare(
        SUBJECT,
        NativeCommand::SubmitWork {
            claim: rows.claim(),
            slot: 0,
            artifact,
        },
    );
    let before = rows.core.state.budget.stats();
    let view = View {
        state: &rows.core.state,
        tail: Some(&pending),
    };
    let found: Vec<_> =
        super::super::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits)
            .map(|row| row.unwrap().reference().id)
            .collect();
    assert_eq!(found, vec![ArtifactId::from_u128(802), second.id, first.id]);
    assert_eq!(rows.core.state.budget.stats(), before);
    assert_eq!(rows.collect().len(), 2);
    rows.core.publish_native(pending).unwrap();
    let third = rows
        .core
        .native_work(ArtifactId::from_u128(802))
        .unwrap()
        .state
        .reference();
    rows.close(
        901,
        vec![evidence::SlotBinding {
            slot: 0,
            artifact: third,
        }],
        vec![],
    );
    assert_eq!(
        rows.collect(),
        vec![
            (third.id, WorkArtifactState::Attached, 2),
            (second.id, WorkArtifactState::Attached, 1),
            (first.id, WorkArtifactState::Attached, 1)
        ]
    );
}

#[test]
fn production_and_claimant_receipt_failures_remain_distinct_work_members() {
    let mut rows = Rows::new(2);
    let output = rows.output(800, 0);
    let rejection = rows.artifact(
        810,
        WorkRole::ReceiptRejection {
            artifact: output,
            reason: EvidenceFailure::Structure,
        },
        ISSUER,
    );
    rows.send(
        ISSUER,
        NativeCommand::RejectWork {
            claim: rows.claim(),
            expected: rows.core.native_work(output.id).unwrap().state.binding(),
            reason: EvidenceFailure::Structure,
            artifact: rejection,
        },
    );
    let diagnostic = rows.artifact(
        811,
        WorkRole::Diagnostic {
            reason: EvidenceFailure::Production,
        },
        SUBJECT,
    );
    let reference = ArtifactRef {
        id: diagnostic.get().unwrap().id(),
        hash: diagnostic.get().unwrap().content_hash(),
    };
    rows.send(
        SUBJECT,
        NativeCommand::SubmitDiagnostic {
            claim: rows.claim(),
            reason: EvidenceFailure::Production,
            artifact: diagnostic,
        },
    );
    rows.send(
        SUBJECT,
        NativeCommand::FailWorkProduction {
            claim: rows.claim(),
            slot: 1,
            diagnostic: reference,
        },
    );
    let expected = vec![
        (reference.id, WorkArtifactState::GenerationFailed, 1),
        (output.id, WorkArtifactState::ReceiptFailed, 1),
    ];
    assert_eq!(rows.collect(), expected);
    rows.close(900, vec![], vec![reference]);
    assert_eq!(rows.collect(), expected);
    assert_eq!(
        rows.core
            .native_response(TestamentId::from_u128(900))
            .unwrap()
            .failed_work()
            .len(),
        2
    );
}

#[test]
fn iteration_prices_both_link_walks_and_caps_empty_cycle_traversal() {
    let mut rows = Rows::new(2);
    rows.output(800, 0);
    let view = rows.view();
    // Claim, retired-cycle header, open-cycle row, first link walk and the
    // seven source/provenance lookups for the yielded work cost eleven visits.
    for (visits, allowed) in [(10, false), (11, true)] {
        let mut limits = rows.core.limits;
        limits.plan_edges = visits;
        let mut cursor = super::super::projection_work::works(&view, ClaimId::from_u128(1), limits);
        assert_eq!(cursor.next().unwrap().is_ok(), allowed);
        assert!(cursor.next().is_none());
    }
    let mut limits = rows.core.limits;
    limits.work_artifacts_per_cycle = 0;
    assert!(
        super::super::projection_work::works(&view, ClaimId::from_u128(1), limits)
            .next()
            .unwrap()
            .is_err()
    );
    let mut empty = Rows::new(2);
    empty.close(900, vec![], vec![]);
    empty.close(901, vec![], vec![]);
    let view = empty.view();
    // Two headers and two response/cycle pairs; there is no current open cycle
    // after reaching the authored response limit.
    for (visits, allowed) in [(5, false), (6, true)] {
        let mut limits = empty.core.limits;
        limits.plan_edges = visits;
        let mut cursor = super::super::projection_work::works(&view, ClaimId::from_u128(1), limits);
        if allowed {
            assert!(cursor.next().is_none());
        } else {
            assert!(cursor.next().unwrap().is_err());
            assert!(cursor.next().is_none());
        }
    }
}

#[test]
fn malformed_cycle_links_are_refused_once_before_any_duplicate_is_yielded() {
    for malformed in 0..5 {
        let mut rows = Rows::new(2);
        let first = rows.output(800, 0);
        let second = rows.output(801, 1);
        let key = NativeCycleKey::of(&rows.parent());
        if malformed < 3 {
            let Some(Row::Cycle(cycle)) = rows.view().get(Key::Cycle(key)) else {
                panic!("cycle")
            };
            let mut cycle = *cycle;
            match malformed {
                0 => cycle.work_count = 1,
                1 => cycle.work_count = 3,
                _ => cycle.work_head = Some(ArtifactId::from_u128(999)),
            }
            rows.replace(Key::Cycle(key), Row::Cycle(cycle), 0);
        } else {
            let id = if malformed == 3 { first.id } else { second.id };
            let mut work = *rows.core.native_work(id).unwrap();
            work.next = Some(if malformed == 3 { second.id } else { id });
            let work = OwnedWork::new(work).unwrap();
            let heap = work.heap_charge().unwrap();
            rows.replace(Key::Work(id), Row::Work(work), heap);
        }
        let view = rows.view();
        let mut cursor =
            super::super::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits);
        assert!(cursor.next().unwrap().is_err());
        assert!(cursor.next().is_none());
        assert!(cursor.next().is_none());
    }
}

#[test]
fn wrong_slot_identity_and_closed_cycle_response_links_cannot_hide_work() {
    for malformed in 0..3 {
        let mut rows = Rows::new(2);
        let work = rows.output(800, 0);
        let key = NativeCycleKey::of(&rows.parent());
        rows.close(
            900,
            vec![evidence::SlotBinding {
                slot: 0,
                artifact: work,
            }],
            vec![],
        );
        match malformed {
            0 => rows.replace(
                Key::WorkSlot(key, 0),
                Row::WorkSlot(ArtifactId::from_u128(999)),
                0,
            ),
            1 => rows.replace(
                Key::ArtifactIdentity(work.hash),
                Row::ArtifactIdentity(ArtifactId::from_u128(999)),
                0,
            ),
            _ => {
                let Some(Row::Cycle(cycle)) = rows.view().get(Key::Cycle(key)) else {
                    panic!("cycle")
                };
                let mut cycle = *cycle;
                cycle.response = Some(TestamentId::from_u128(999));
                rows.replace(Key::Cycle(key), Row::Cycle(cycle), 0);
            }
        }
        let view = rows.view();
        let mut cursor =
            super::super::projection_work::works(&view, ClaimId::from_u128(1), rows.core.limits);
        assert!(cursor.next().unwrap().is_err());
        assert!(cursor.next().is_none());
    }
}

#[test]
fn independent_cursors_consume_one_shared_allowance_without_allocating() {
    let mut rows = Rows::new(2);
    rows.output(800, 0);
    let before = rows.core.native_budget();
    let view = rows.view();
    let shared = super::super::projection_visits::Visits::new(22);
    for remaining in [11, 0] {
        let found: Vec<_> = super::super::projection_work::works_with_budget(
            &view,
            ClaimId::from_u128(1),
            rows.core.limits,
            Some(&shared),
        )
        .collect::<Result<_, _>>()
        .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(shared.remaining(), remaining);
    }
    let mut refused = super::super::projection_work::works_with_budget(
        &view,
        ClaimId::from_u128(1),
        rows.core.limits,
        Some(&shared),
    );
    assert_eq!(refused.next(), Some(Err(ContractError::Capacity)));
    assert!(refused.next().is_none());
    assert_eq!(shared.check(), Err(ContractError::Capacity));
    assert_eq!(shared.charge(0), Err(ContractError::Capacity));
    assert_eq!(rows.core.native_budget(), before);
}

#[test]
fn interleaved_partial_cursors_cannot_restore_an_exhausted_projection_allowance() {
    let mut rows = Rows::new(2);
    rows.output(800, 0);
    rows.output(801, 1);
    let view = rows.view();
    let shared = super::super::projection_visits::Visits::new(18);
    let mut first = super::super::projection_work::works_with_budget(
        &view,
        ClaimId::from_u128(1),
        rows.core.limits,
        Some(&shared),
    );
    assert!(first.next().unwrap().is_ok());
    assert_eq!(shared.remaining(), 6);
    let mut second = super::super::projection_work::works_with_budget(
        &view,
        ClaimId::from_u128(1),
        rows.core.limits,
        Some(&shared),
    );
    assert_eq!(second.next(), Some(Err(ContractError::Capacity)));
    assert_eq!(first.next(), Some(Err(ContractError::Capacity)));
    assert!(first.next().is_none());
    assert!(second.next().is_none());
}

#[test]
fn projection_lookup_exhaustion_is_capacity_and_never_runs_the_acceptance_callback() {
    let mut rows = Rows::new(2);
    rows.output(800, 0);
    let before = rows.core.native_budget();
    let view = rows.view();
    super::super::projection::with_projection(
        &view,
        ClaimId::from_u128(1),
        rows.core.limits,
        &rows.core.state.budget,
        |_| (),
    )
    .unwrap();
    let limits = NativeLimits {
        plan_edges: 13,
        ..rows.core.limits
    };
    let called = std::cell::Cell::new(false);
    let result = super::super::projection::with_projection(
        &view,
        ClaimId::from_u128(1),
        limits,
        &rows.core.state.budget,
        |_| called.set(true),
    );
    assert!(matches!(
        result,
        Err(NativeError::Contract(ContractError::Capacity))
    ));
    assert!(!called.get());
    assert_eq!(rows.core.native_budget(), before);
    assert_eq!(
        rows.core
            .native_claim(ClaimId::from_u128(1))
            .unwrap()
            .status(),
        ClaimStatus::Received
    );
}

#[test]
fn provisional_overlay_comparisons_share_the_same_lookup_allowance() {
    use focal_model::lifecycle::aggregation::WholeWorkView;
    let mut rows = Rows::new(2);
    let work = rows.output(800, 0);
    let view = rows.view();
    let mut extras = crate::native::prepare::Extras::new(2, usize::MAX).unwrap();
    extras
        .push(crate::native::prepare::Extra {
            key: Key::Meta,
            row: Row::Meta(view.meta()),
            heap: 0,
            fact: None,
        })
        .unwrap();
    let owned = OwnedWork::new(*rows.core.native_work(work.id).unwrap()).unwrap();
    let heap = owned.heap_charge().unwrap();
    extras
        .push(crate::native::prepare::Extra {
            key: Key::Work(work.id),
            row: Row::Work(owned),
            heap,
            fact: None,
        })
        .unwrap();
    for (limit, found) in [(2, false), (3, true)] {
        let visits = super::super::projection_visits::Visits::new(limit);
        let projection = super::super::projection::ProjectionRows {
            view: &view,
            claim: ClaimId::from_u128(1),
            limits: rows.core.limits,
            staged: Some(&extras),
            sequence: view.prefix(),
            visits: &visits,
        };
        assert_eq!(projection.work(work.id).is_some(), found);
        assert_eq!(visits.check().is_ok(), found);
    }
}

fn measured_projection(rows: &Rows, quote: NativeProjectionQuote) -> usize {
    use focal_model::lifecycle::aggregation::{ProjectionLimits, prepare_projection};
    let view = rows.view();
    let claim = rows.core.native_claim(ClaimId::from_u128(1)).unwrap();
    let registry = view
        .owned_claim(ClaimId::from_u128(1))
        .unwrap()
        .registrations()
        .unwrap();
    let mut extras = crate::native::prepare::Extras::new(quote.overlay_rows(), usize::MAX).unwrap();
    // An irrelevant provisional row is enough to exercise a failed overlay
    // search on every direct lookup. It never substitutes a source fact.
    if quote.overlay_rows() != 0 {
        extras
            .push(crate::native::prepare::Extra {
                key: Key::Meta,
                row: Row::Meta(view.meta()),
                heap: 0,
                fact: None,
            })
            .unwrap();
    }
    let visits = super::super::projection_visits::Visits::new(quote.lookup_visits());
    visits.charge(2).unwrap();
    let model = quote.model();
    let projection_rows = super::super::projection::ProjectionRows {
        view: &view,
        claim: ClaimId::from_u128(1),
        limits: NativeLimits {
            plan_edges: quote.lookup_visits(),
            ..rows.core.limits
        },
        staged: Some(&extras),
        sequence: view.prefix(),
        visits: &visits,
    };
    let shape = model.shape();
    let plan = prepare_projection(
        claim,
        registry,
        &projection_rows,
        ProjectionLimits {
            responses: shape.responses,
            works: shape.works,
            slots: shape.responses * claim.acceptance().slot_count(),
            evaluations: shape.evaluations,
            declarations: claim.acceptance().declarations().len(),
            visits: model.inspection_visits().max(model.reduction_visits()),
            bytes: model.construction_charge(),
        },
    )
    .unwrap();
    assert!(plan.construction_charge() <= model.construction_charge());
    let projected = plan.build().unwrap();
    visits.check().unwrap();
    assert!(matches!(
        projected.claim_decision().outcome(),
        aggregation::AggregateOutcome::Pending
    ));
    quote.lookup_visits() - visits.remaining()
}

#[test]
fn one_future_native_quote_covers_later_open_work_and_full_closed_response_growth() {
    use focal_model::lifecycle::aggregation::ProjectionShape;
    let mut rows = Rows::new(4);
    let shape = ProjectionShape {
        responses: 4,
        works: 8,
        evaluations: 4,
    };
    let limits = NativeLimits {
        plan_edges: 8192,
        ..rows.core.limits
    };
    let quote = NativeProjectionQuote::derive(
        limits,
        rows.core.native_claim(ClaimId::from_u128(1)).unwrap(),
        shape,
        1,
    )
    .unwrap();
    let initial = measured_projection(&rows, quote);
    let mut maximum = initial;
    for cycle in 0..4 {
        let first = rows.output(800 + cycle * 2, 0);
        let second = rows.output(801 + cycle * 2, 1);
        maximum = maximum.max(measured_projection(&rows, quote));
        rows.close(
            900 + cycle,
            vec![
                evidence::SlotBinding {
                    slot: 0,
                    artifact: first,
                },
                evidence::SlotBinding {
                    slot: 1,
                    artifact: second,
                },
            ],
            vec![],
        );
        maximum = maximum.max(measured_projection(&rows, quote));
    }
    assert!(maximum > initial);
    assert!(maximum <= quote.lookup_visits());
}

#[test]
fn native_quote_refuses_lookup_overlay_and_arithmetic_overflow_before_admission() {
    use focal_model::lifecycle::aggregation::ProjectionShape;
    let rows = Rows::new(4);
    let claim = rows.core.native_claim(ClaimId::from_u128(1)).unwrap();
    let shape = ProjectionShape {
        responses: 4,
        works: 8,
        evaluations: 4,
    };
    let limits = NativeLimits {
        plan_edges: 8192,
        ..rows.core.limits
    };
    let quote = NativeProjectionQuote::derive(limits, claim, shape, 1).unwrap();
    let maximum = quote
        .model()
        .inspection_visits()
        .max(quote.model().reduction_visits())
        .max(quote.lookup_visits());
    NativeProjectionQuote::derive(
        NativeLimits {
            plan_edges: maximum,
            ..limits
        },
        claim,
        shape,
        1,
    )
    .unwrap();
    assert!(
        NativeProjectionQuote::derive(
            NativeLimits {
                plan_edges: maximum - 1,
                ..limits
            },
            claim,
            shape,
            1
        )
        .is_err()
    );
    assert!(
        NativeProjectionQuote::derive(limits, claim, shape, limits.range.max_batch_entries + 1)
            .is_err()
    );
    assert!(
        NativeProjectionQuote::derive(
            NativeLimits {
                range: RangeConfig {
                    max_batch_entries: usize::MAX,
                    ..limits.range
                },
                ..limits
            },
            claim,
            shape,
            usize::MAX
        )
        .is_err()
    );
    assert!(
        NativeProjectionQuote::derive(
            limits,
            claim,
            ProjectionShape {
                works: usize::MAX,
                ..shape
            },
            1
        )
        .is_err()
    );
}
