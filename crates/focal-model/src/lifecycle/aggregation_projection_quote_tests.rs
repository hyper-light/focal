use super::*;

fn loose() -> ProjectionLimits {
    ProjectionLimits {
        visits: usize::MAX,
        bytes: usize::MAX,
        ..limits()
    }
}

fn measured<T>(action: impl FnOnce() -> T) -> (T, usize) {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            MEASURED_VISITS.with(|counter| counter.set(self.0));
        }
    }
    let _restore = Restore(MEASURED_VISITS.with(|counter| counter.replace(Some(0))));
    let value = action();
    (
        value,
        MEASURED_VISITS.with(|counter| counter.get().unwrap()),
    )
}

fn fits(fixture: &Fixture, quote: ProjectionQuote) -> AggregateOutcome {
    let shape = quote.shape();
    let (plan, inspected) = measured(|| {
        prepare_projection(
            &fixture.claim,
            &fixture.registry,
            fixture,
            ProjectionLimits {
                responses: shape.responses,
                works: shape.works,
                slots: shape.responses * fixture.claim.acceptance().slot_count(),
                evaluations: shape.evaluations,
                declarations: fixture.definitions.len(),
                visits: quote.inspection_visits(),
                bytes: quote.construction_charge(),
            },
        )
        .unwrap()
    });
    assert!(inspected <= quote.inspection_visits());
    assert!(plan.construction_charge() <= quote.construction_charge());
    let mut plan = plan;
    plan.limits.visits = quote.reduction_visits();
    let (projection, reduced) = measured(|| plan.build().unwrap());
    assert!(reduced <= quote.reduction_visits());
    assert!(projection.construction_charge() <= quote.construction_charge());
    projection.claim_decision().outcome()
}

fn missing_results(fixture: &mut Fixture, response: usize, sequence: u64) {
    let aggregate = ClaimAggregation::new(&fixture.claim, policy_limits()).unwrap();
    for index in 0..fixture.evaluations.len() {
        let state = fixture.evaluations[index];
        if !matches!(state.target(), Target::MissingSlot { response: id, .. }
            if id.object == fixture.responses[response].response.identity().binding.object)
        {
            continue;
        }
        let definition = fixture
            .definitions
            .iter()
            .find(|definition| definition.binding().object == state.binding().object)
            .unwrap();
        let evaluation = state.bind(definition).unwrap();
        let transition = evaluation
            .settle_missing(
                Principal::Actor(ISSUER),
                &evaluation.binding(),
                &fixture.claim,
                &fixture.responses[response].response,
                &aggregate.decision(),
            )
            .unwrap();
        fixture.evaluations[index] = transition.next.into_state();
        if let Some(result) = transition.result {
            fixture.results.push((
                result,
                position(sequence, u32::try_from(index + 20).unwrap()),
            ));
        }
    }
}

fn apply_terminal_outcomes(fixture: &mut Fixture) {
    let projection = fixture.projection();
    let mut work = Vec::new();
    let mut response = Vec::new();
    for (index, row) in fixture.responses.iter().enumerate() {
        let id = TestamentId(row.response.identity().binding.object.0);
        let Some(decision) = projection.response_decision(id) else {
            continue;
        };
        for (index, source) in fixture.works.iter().enumerate() {
            if source.attachment() == Some(id) {
                work.push((
                    index,
                    source.apply_decision(&source.binding(), &decision).unwrap(),
                ));
            }
        }
        if let Some(transition) = row
            .response
            .plan_decision(&row.response.identity().binding, &decision)
            .unwrap()
        {
            response.push((index, transition));
        }
    }
    let mut claim = fixture
        .claim
        .try_copy(fixture.claim.copy_charge().unwrap())
        .unwrap();
    claim
        .apply_aggregate(&claim.binding(), &projection.claim_decision())
        .unwrap();
    drop(projection);
    for (index, next) in work {
        fixture.works[index] = next;
    }
    for (index, transition) in response {
        fixture.responses[index].response.apply(transition).unwrap();
    }
    fixture.claim = claim;
}

#[test]
fn empty_future_handles_zero_triangles_and_quotes_without_allocation() {
    let fixture = Fixture::new(&[], false);
    let shape = ProjectionShape {
        responses: 0,
        works: 0,
        evaluations: 0,
    };
    memory::fail_after(0, || {
        let quote = quote_projection(&fixture.claim, shape, loose()).unwrap();
        assert_eq!(quote.shape(), shape);
        assert_eq!(quote.inspection_visits(), 5); // d=1, s=k=r=w=e=0.
        assert_eq!(quote.reduction_visits(), 1);
        assert_eq!(
            quote.construction_charge(),
            size_of::<WholeWorkProjection<'_>>()
        );
        assert_eq!(memory::remaining_allocations(), Some(0));
        assert_eq!(fits(&fixture, quote), AggregateOutcome::Pending);
        assert_eq!(memory::remaining_allocations(), Some(0));
    });
}

#[test]
fn dimensions_bytes_and_each_visit_counter_are_checked_before_allocation() {
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
    fixture.receive(300, &[(0, 400)], 2);
    let shape = ProjectionShape {
        responses: 2,
        works: 2,
        evaluations: 4,
    };
    let quote = quote_projection(&fixture.claim, shape, loose()).unwrap();
    let exact = ProjectionLimits {
        responses: 2,
        works: 2,
        slots: 2,
        evaluations: 4,
        declarations: 2,
        visits: quote.inspection_visits().max(quote.reduction_visits()),
        bytes: quote.construction_charge(),
    };
    assert_eq!(
        quote_projection(&fixture.claim, shape, exact).unwrap(),
        quote
    );
    for invalid in [
        ProjectionLimits {
            responses: 1,
            ..exact
        },
        ProjectionLimits { works: 1, ..exact },
        ProjectionLimits { slots: 1, ..exact },
        ProjectionLimits {
            evaluations: 3,
            ..exact
        },
        ProjectionLimits {
            declarations: 1,
            ..exact
        },
        ProjectionLimits {
            bytes: exact.bytes - 1,
            ..exact
        },
        ProjectionLimits {
            visits: quote.inspection_visits() - 1,
            ..exact
        },
        ProjectionLimits {
            visits: quote.reduction_visits() - 1,
            ..exact
        },
    ] {
        memory::fail_after(0, || {
            assert_eq!(
                quote_projection(&fixture.claim, shape, invalid),
                Err(ContractError::Capacity)
            );
            assert_eq!(memory::remaining_allocations(), Some(0));
        });
    }
    for invalid in [
        ProjectionShape {
            responses: 0,
            ..shape
        },
        ProjectionShape {
            responses: 9,
            ..shape
        },
        ProjectionShape {
            evaluations: usize::MAX,
            ..shape
        },
        ProjectionShape {
            works: usize::MAX,
            ..shape
        },
    ] {
        assert_eq!(
            quote_projection(
                &fixture.claim,
                invalid,
                ProjectionLimits {
                    responses: usize::MAX,
                    works: usize::MAX,
                    slots: usize::MAX,
                    evaluations: usize::MAX,
                    declarations: usize::MAX,
                    ..loose()
                }
            ),
            Err(ContractError::Capacity)
        );
    }
}

#[test]
fn invalid_dimensions_and_zero_response_base_cost_refuse_before_policy_traversal() {
    let fixture = Fixture::new(&[(ValidationMode::Required, 0); 3], false);
    let shape = ProjectionShape {
        responses: 0,
        works: 0,
        evaluations: 0,
    };
    memory::fail_after(0, || {
        // Scalar dimensions are checked before any policy visit is consumed.
        let (result, visited) = measured(|| {
            quote_projection(
                &fixture.claim,
                ProjectionShape {
                    responses: 9,
                    ..shape
                },
                loose(),
            )
        });
        assert_eq!(result, Err(ContractError::Capacity));
        assert_eq!(visited, 0);
        let (result, visited) = measured(|| {
            quote_projection(
                &fixture.claim,
                shape,
                ProjectionLimits {
                    declarations: 0,
                    ..loose()
                },
            )
        });
        assert_eq!(result, Err(ContractError::Capacity));
        assert_eq!(visited, 0);
        // r*s is zero, but the source's d+s preflight still refuses before the
        // loop over slots. Only the single declaration debit has succeeded.
        let (result, visited) = measured(|| {
            quote_projection(
                &fixture.claim,
                shape,
                ProjectionLimits {
                    slots: 0,
                    visits: 3,
                    ..loose()
                },
            )
        });
        assert_eq!(result, Err(ContractError::Capacity));
        assert_eq!(visited, 1);
        assert_eq!(memory::remaining_allocations(), Some(0));
    });
}

#[test]
fn zero_check_and_missing_cells_use_the_same_eleven_buffer_bound() {
    for present in [false, true] {
        let mut fixture = Fixture::new(&[(ValidationMode::Required, 0)], false);
        let quote = quote_projection(
            &fixture.claim,
            ProjectionShape {
                responses: 1,
                works: 1,
                evaluations: 1,
            },
            loose(),
        )
        .unwrap();
        let response = fixture.receive(300, if present { &[(0, 400)] } else { &[] }, 2);
        assert_eq!(fits(&fixture, quote), AggregateOutcome::Pending);
        fixture.enter(response, 3);
        let outcome = fits(&fixture, quote);
        assert!(matches!(
            (present, outcome),
            (true, AggregateOutcome::LocalComplete { .. }) | (false, AggregateOutcome::Blocked(_))
        ));
        let actual =
            prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits()).unwrap();
        assert_eq!(actual.construction_charge(), quote.construction_charge());
        for site in 0..11 {
            memory::fail_after(site, || {
                assert!(
                    prepare_projection(&fixture.claim, &fixture.registry, &fixture, limits())
                        .unwrap()
                        .build()
                        .is_err()
                );
                assert_eq!(memory::remaining_allocations(), Some(0));
            });
        }
        apply_terminal_outcomes(&mut fixture);
        assert_eq!(fits(&fixture, quote), outcome);
    }
}

#[test]
fn a_quote_taken_before_responses_covers_growth_missing_results_and_terminal_rows() {
    let mut fixture = Fixture::new(
        &[
            (ValidationMode::Required, 1),
            (ValidationMode::Required, 0),
            (ValidationMode::Observe, 1),
        ],
        false,
    );
    let quote = quote_projection(
        &fixture.claim,
        ProjectionShape {
            responses: 3,
            works: 6,
            evaluations: 9,
        },
        loose(),
    )
    .unwrap();
    fits(&fixture, quote);
    let first = fixture.receive(300, &[(0, 400), (1, 401)], 2);
    fits(&fixture, quote);
    let second = fixture.receive(301, &[(1, 402), (2, 403)], 3);
    fits(&fixture, quote);
    let third = fixture.receive(302, &[(0, 404), (1, 405)], 4);
    fits(&fixture, quote);
    fixture.enter(first, 5);
    missing_results(&mut fixture, first, 5);
    fits(&fixture, quote);
    fixture.begin(first, 1);
    fixture.enter(third, 6);
    missing_results(&mut fixture, third, 6);
    fits(&fixture, quote);
    fixture.enter(second, 7);
    missing_results(&mut fixture, second, 7);
    fits(&fixture, quote);
    fixture.report(first, 1, VerdictValue::Error, position(8, 1));
    let retry = fixture.evaluation_index(first, 1);
    assert!(
        !fixture.evaluations[retry]
            .last_result()
            .unwrap()
            .is_terminal()
    );
    fits(&fixture, quote);
    fixture.report(first, 1, VerdictValue::Fail, position(9, 1));
    let outcome = fits(&fixture, quote);
    assert!(matches!(outcome, AggregateOutcome::Blocked(_)));
    apply_terminal_outcomes(&mut fixture);
    assert_eq!(fits(&fixture, quote), outcome);
}

#[test]
fn alternate_coverage_and_all_last_result_branches_fit_one_future_quote() {
    for (pass, fail) in [(6, 7), (7, 6), (6, 6)] {
        let mut fixture = Fixture::new(&[(ValidationMode::Required, 2)], false);
        let quote = quote_projection(
            &fixture.claim,
            ProjectionShape {
                responses: 2,
                works: 2,
                evaluations: 6,
            },
            loose(),
        )
        .unwrap();
        let first = fixture.receive(300, &[(0, 400)], 2);
        let second = fixture.receive(301, &[(0, 401)], 3);
        fixture.enter(first, 4);
        fixture.enter(second, 5);
        for response in [first, second] {
            for check in [1, 2] {
                fixture.begin(response, check);
                fits(&fixture, quote);
            }
        }
        fixture.report(first, 1, VerdictValue::Pass, position(pass, 1));
        fits(&fixture, quote);
        fixture.report(second, 2, VerdictValue::Pass, position(pass, 2));
        assert_eq!(fits(&fixture, quote), AggregateOutcome::Pending);
        fixture.report(second, 1, VerdictValue::Fail, position(fail, 3));
        fits(&fixture, quote);
        fixture.report(first, 2, VerdictValue::Pass, position(pass, 4));
        let outcome = fits(&fixture, quote);
        assert!(matches!(outcome, AggregateOutcome::LocalComplete { .. }) == (pass <= fail));
        fixture.evaluations.reverse();
        fixture.results.reverse();
        assert_eq!(fits(&fixture, quote), outcome);
        apply_terminal_outcomes(&mut fixture);
        assert_eq!(fits(&fixture, quote), outcome);
    }
}

#[test]
fn actual_failed_work_and_failure_testimony_fit_before_and_after_missing_settlement() {
    use crate::lifecycle::artifact_descriptor::{
        ArtifactDescriptor, ArtifactSpec, Limits as DescriptorLimits, PayloadSpec, WorkProvenance,
        WorkRole,
    };
    let mut fixture = Fixture::new(&[(ValidationMode::Required, 1)], false);
    let quote = quote_projection(
        &fixture.claim,
        ProjectionShape {
            responses: 1,
            works: 1,
            evaluations: 2,
        },
        loose(),
    )
    .unwrap();
    let parent = evidence::Parent::from_claim(&fixture.claim).unwrap();
    let descriptor = ArtifactDescriptor::prepare(
        ArtifactSpec {
            ledger: parent.ledger,
            id: ArtifactId::from_u128(500),
            schema: 1,
            kind: "error",
            schema_hash: ContentHash([8; 32]),
            metadata: b"{}",
            payload: PayloadSpec::Inline(
                br#"{"code":"tool_unavailable","message":"The requested tool could not run."}"#,
            ),
            producer: parent.holder,
            receipt: Some(parent.receipt),
            result: None,
            work: Some(WorkProvenance {
                claim: parent.claim,
                cycle: parent.next_cycle,
                role: WorkRole::Diagnostic {
                    reason: evidence::EvidenceFailure::Production,
                },
            }),
            inputs: &[],
            visibility: &[],
        },
        DescriptorLimits {
            kind_bytes: 64,
            metadata_bytes: 128,
            inline_bytes: 1024,
            inputs: 0,
            visibility_labels: 0,
            visibility_label_bytes: 0,
            construction_bytes: 8192,
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let reference = ArtifactRef {
        id: descriptor.id(),
        hash: descriptor.content_hash(),
    };
    let evidence = EvidenceAttestation {
        descriptor_hash: descriptor.content_hash(),
        custody_revision: 1,
        durable: true,
        schema_valid: true,
    };
    let failure = evidence::Diagnostic {
        reason: evidence::EvidenceFailure::Production,
        artifact: reference,
    };
    let diagnostic = evidence::ResponseDiagnostic::record_native(
        &parent,
        Principal::Actor(parent.holder),
        parent.receipt,
        failure,
        &descriptor,
        &evidence,
    )
    .unwrap();
    let work = WorkArtifact::generation_failed(
        Binding {
            content: reference.hash,
            ..binding(500)
        },
        &parent,
        Principal::Actor(parent.holder),
        0,
        parent.receipt,
        failure,
        &evidence,
    )
    .unwrap();
    fixture.works.push(work);
    assert_eq!(fits(&fixture, quote), AggregateOutcome::Pending);
    fixture.works.clear();
    let response =
        fixture.receive_report_with_work(300, &[], 2, OutcomeKind::Failed, &[diagnostic], &[work]);
    assert_eq!(fixture.responses[response].response.failed_work().len(), 1);
    assert!(fixture.responses[response].response.manifest().is_empty());
    assert_eq!(fits(&fixture, quote), AggregateOutcome::Pending);
    fixture.enter(response, 3);
    missing_results(&mut fixture, response, 3);
    let outcome = fits(&fixture, quote);
    assert!(matches!(outcome, AggregateOutcome::Blocked(_)));
    apply_terminal_outcomes(&mut fixture);
    assert_eq!(fits(&fixture, quote), outcome);
    assert_eq!(
        fixture.works[0].state(),
        evidence::WorkArtifactState::GenerationFailed
    );
}
