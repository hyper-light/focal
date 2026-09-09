//! Static closure shape bound. This prevents accepting a cycle whose immutable
//! membership can never fit one response transaction. It does not reserve future
//! RAM, durable space, response rows, or mandatory diagnostic capacity.
use super::prepare::{add, within};
use super::prepare_budget::ConstructionBudget;
use super::{ClaimState, NativeError, NativeLimits, NativeOperation, RegistrationSet};

/// Full immutable target capacity: Admission once, every Delivery/slot check per
/// authored response, and every Increment check per possible slot and response.
/// No owner cap silently reduces the respondent's authored response allowance.
pub(super) fn required_registrations(
    claim: &ClaimState,
    limits: NativeLimits,
) -> Result<usize, NativeError> {
    use focal_model::lifecycle::aggregation::ObligationTarget;
    let declarations = claim.acceptance().declarations();
    within(declarations.len(), limits.plan_edges)?;
    let mut admission = 0;
    let mut per_response = 0;
    for declaration in declarations {
        match declaration.target() {
            ObligationTarget::Admission => admission = add(admission, 1)?,
            ObligationTarget::Delivery | ObligationTarget::Slot(_) => {
                per_response = add(per_response, 1)?;
            }
            ObligationTarget::Increment => {
                per_response = add(per_response, claim.acceptance().slot_count())?;
            }
        }
    }
    let responses = usize::try_from(claim.max_responses())
        .map_err(|_| NativeError::Capacity("response registration count"))?;
    add(
        admission,
        responses
            .checked_mul(per_response)
            .ok_or(NativeError::Capacity("response registration count"))?,
    )
}

pub(super) fn check_registration_capacity(
    claim: &ClaimState,
    registry: &RegistrationSet,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    registry.check(claim)?;
    let required = required_registrations(claim, limits)?;
    within(required, registry.max_rows())?;
    within(required, limits.evaluations_per_claim)?;
    within(registry.rows().len(), required)
}

/// Closed response cycles already occupy the immutable baseline. Retired open
/// cycles add independent old-receipt Increment targets, including slots later
/// reused by the replacement holder; none of those registrations is discarded.
pub(super) fn required_with_retired(
    claim: &ClaimState,
    limits: NativeLimits,
    retired_work: usize,
) -> Result<usize, NativeError> {
    within(retired_work, limits.plan_edges)?;
    let increments = super::increments::count(claim, limits)?;
    add(
        required_registrations(claim, limits)?,
        retired_work
            .checked_mul(increments)
            .ok_or(NativeError::Capacity("retired increment registrations"))?,
    )
}

pub(super) fn check_registration_capacity_in(
    view: &super::View<'_>,
    claim: &ClaimState,
    registry: &RegistrationSet,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    registry.check(claim)?;
    let retired =
        super::retired_cycles::head(view, super::ClaimId(claim.binding().object.0), limits)?;
    let required = required_with_retired(claim, limits, retired.work_count)?;
    within(required, registry.max_rows())?;
    within(required, limits.evaluations_per_claim)?;
    within(registry.rows().len(), required)
}

/// View-free conservative retained-registry bound for a completion contract.
/// Old-receipt Increment rows include every abandoned output registration. Some
/// may also belong to closed responses already covered by the baseline; keeping
/// that overlap is safe and avoids dropping any original audit membership.
pub(super) fn retained_registration_bound(
    claim: &ClaimState,
    registry: &RegistrationSet,
    limits: NativeLimits,
) -> Result<usize, NativeError> {
    within(registry.rows().len(), limits.plan_edges)?;
    let current = claim.receipt().map(|receipt| receipt.fence);
    let mut previous = 0usize;
    for row in registry.rows() {
        if matches!(
            row.target(),
            focal_model::lifecycle::validation::Target::Increment { .. }
        ) && row.receipt() != current
        {
            previous = add(previous, 1)?;
        }
    }
    add(required_registrations(claim, limits)?, previous)
}

pub(super) fn delivery_count(
    claim: &ClaimState,
    limits: NativeLimits,
) -> Result<usize, NativeError> {
    let declarations = claim.acceptance().declarations();
    if declarations.len() > limits.plan_edges {
        return Err(NativeError::Capacity("delivery declaration visits"));
    }
    Ok(declarations
        .iter()
        .filter(|row| {
            row.target() == focal_model::lifecycle::aggregation::ObligationTarget::Delivery
        })
        .count())
}

/// Pure Receipt adds an evaluation/result and two facts; every WholeWork check
/// adds Ready and its fact. Include both complete cohorts before responsibility.
pub(super) fn check_receipt_shape(
    delivery: usize,
    work: usize,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    let entries = add(
        delivery
            .checked_mul(2)
            .ok_or(NativeError::Capacity("delivery cohort"))?,
        work,
    )?;
    // Each Ready work check registers its due timer; a delivery evaluation
    // registers and settles in the same publication, and released monitors
    // are graph consequences funded with the graph.
    ConstructionBudget::for_operation(NativeOperation::ReceiveResponse, limits)?.check_counts(
        1,
        add(entries, 1)?,
        add(entries, 2)?,
        add(
            crate::native::index_rows::STATUS_ROWS,
            crate::native::index_rows::timer_rows(0, work, 0)?,
        )?,
    )
}

/// An output and I Ready checks publish with complete registration membership.
/// With checks: artifact/identity/work/slot/cycle + I checks + parent + I+3 facts,
/// then the artifact's index rows for its `inputs` inputs and, with checks,
/// the parent's status move (doc 22 §7). A future submission is checked with
/// the smallest artifact: one citing more inputs is refused at its own
/// admission, never promised in advance.
pub(super) fn check_increment_shape(
    count: usize,
    inputs: usize,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    if inputs > crate::native::index_rows::input_bound(limits) {
        return Err(NativeError::Capacity("artifact inputs"));
    }
    let (claims, events) = if count == 0 {
        (0, 2)
    } else {
        (1, add(count, 3)?)
    };
    // Each registered increment evaluation gains its due timer.
    ConstructionBudget::for_operation(NativeOperation::SubmitWork, limits)?.check_counts(
        claims,
        add(count, 5)?,
        events,
        add(
            crate::native::index_rows::artifact_rows(inputs)?,
            add(add(claims, claims)?, count)?,
        )?,
    )
}

/// Closing n work rows writes n attachment replacements, one cycle, one response,
/// one claim, n+2 events, Meta and outcome: 2*n+7 changes in all. Diagnostics are
/// retained inside the response; they do not add independent closing changes.
pub(super) fn work_limit(limits: NativeLimits) -> Result<usize, NativeError> {
    let budget = ConstructionBudget::for_operation(NativeOperation::CloseResponse, limits)?;
    let refusal = || NativeError::Capacity("response closure transaction");
    if budget.max_claim_rows == 0 {
        return Err(refusal());
    }
    // Closing writes seven fixed rows and the claim's two status index rows
    // beside the two rows each closed attachment adds (doc 22 §7).
    let changes = budget
        .max_changes
        .checked_sub(7)
        .and_then(|changes| changes.checked_sub(crate::native::index_rows::STATUS_ROWS))
        .ok_or_else(refusal)?
        / 2;
    let extras = budget.extras_count.checked_sub(2).ok_or_else(refusal)?;
    let events = budget.max_events.checked_sub(2).ok_or_else(refusal)?;
    Ok(limits
        .work_artifacts_per_cycle
        .min(limits.plan_edges)
        .min(changes)
        .min(extras)
        .min(events))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(batch: usize) -> NativeLimits {
        let mut limits = NativeLimits::default();
        limits.range.max_batch_entries = batch;
        limits
    }

    #[test]
    fn exact_close_row_bound_covers_empty_odd_and_even_batches() {
        for (batch, expected) in [
            (9, 0),
            (10, 0),
            (11, 1),
            (12, 1),
            (129, 60),
            (130, 60),
            (131, 61),
        ] {
            let limits = limits(batch);
            let actual = work_limit(limits).unwrap();
            assert_eq!(actual, expected);
            let budget =
                ConstructionBudget::for_operation(NativeOperation::CloseResponse, limits).unwrap();
            let status = crate::native::index_rows::STATUS_ROWS;
            assert!(
                budget
                    .check_counts(1, actual + 2, actual + 2, status)
                    .is_ok()
            );
            assert!(
                budget
                    .check_counts(1, actual + 3, actual + 3, status)
                    .is_err()
            );
        }
    }

    #[test]
    fn configured_membership_and_visit_caps_only_reduce_the_closeable_cohort() {
        let mut configured = limits(129);
        configured.work_artifacts_per_cycle = 17;
        assert_eq!(work_limit(configured).unwrap(), 17);
        configured.plan_edges = 9;
        assert_eq!(work_limit(configured).unwrap(), 9);
        configured.plan_edges = 0;
        assert_eq!(work_limit(configured).unwrap(), 0);
        configured.plan_edges = 90;
        configured.work_artifacts_per_cycle = 0;
        assert_eq!(work_limit(configured).unwrap(), 0);
    }

    #[test]
    fn impossible_closure_shape_and_overflow_refuse_before_any_cycle_admission() {
        for batch in 0..9 {
            assert!(work_limit(limits(batch)).is_err());
        }
        let mut configured = limits(9);
        configured.plan_nodes = 0;
        assert!(work_limit(configured).is_err());
        assert!(work_limit(limits(usize::MAX)).is_err());
        let mut configured = limits(9);
        configured.preparation_bytes = usize::MAX;
        assert!(work_limit(configured).is_err());
    }

    #[test]
    fn first_receipt_requires_the_full_authored_delivery_capacity() {
        use crate::native::report_tests::running;
        use focal_model::ClaimId;
        let core = running(&[]);
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap();
        assert_eq!(claim.max_responses(), 4);
        assert_eq!(required_registrations(claim, core.limits).unwrap(), 4);
        let one = RegistrationSet::new(claim, 1, size_of::<RegistrationSet>()).unwrap();
        assert!(check_registration_capacity(claim, &one, core.limits).is_err());
        let exact = RegistrationSet::new(claim, 4, size_of::<RegistrationSet>()).unwrap();
        assert!(check_registration_capacity(claim, &exact, core.limits).is_ok());
        let mut reduced = core.limits;
        reduced.evaluations_per_claim = 3;
        assert!(check_registration_capacity(claim, &exact, reduced).is_err());
        assert_eq!(claim.max_responses(), 4);
        assert_eq!(claim.response_count(), 0);
        assert!(claim.receipt().is_none());
    }

    #[test]
    fn increment_cohort_must_fit_with_work_and_membership_in_one_submission() {
        // The artifact's index rows (producer, kind, schema and one per
        // input) join every shape; a Ready cohort also moves the parent's
        // status key and registers one due timer per check (doc 22 §7).
        for inputs in [0, 1, 16] {
            let artifact_index = crate::native::index_rows::artifact_rows(inputs).unwrap();
            for count in 0..16 {
                let batch = if count == 0 {
                    9 + artifact_index
                } else {
                    3 * count + 11 + artifact_index + crate::native::index_rows::STATUS_ROWS
                };
                assert!(check_increment_shape(count, inputs, limits(batch)).is_ok());
                assert!(check_increment_shape(count, inputs, limits(batch - 1)).is_err());
            }
        }
        assert!(check_increment_shape(usize::MAX, 0, limits(128)).is_err());
        assert!(check_increment_shape(0, 17, limits(128)).is_err());
        let mut no_parent = limits(32);
        no_parent.plan_nodes = 0;
        assert!(check_increment_shape(1, 0, no_parent).is_err());
        assert!(check_increment_shape(0, 0, no_parent).is_ok());
    }

    #[test]
    fn receipt_prices_both_complete_cohorts_at_the_exact_batch_boundary() {
        for delivery in 0..8 {
            for work in 0..8 {
                // One changed claim/response, their facts, Meta and outcome,
                // then the claim's two status index rows; Delivery adds four
                // rows and each Ready work check adds two plus its due timer.
                let batch = 4 * delivery + 3 * work + 6 + crate::native::index_rows::STATUS_ROWS;
                assert!(check_receipt_shape(delivery, work, limits(batch)).is_ok());
                assert!(check_receipt_shape(delivery, work, limits(batch - 1)).is_err());
            }
        }
        assert!(check_receipt_shape(usize::MAX, 0, limits(128)).is_err());
        assert!(check_receipt_shape(0, usize::MAX, limits(128)).is_err());
        assert!(check_receipt_shape(usize::MAX / 2, 2, limits(128)).is_err());
        let mut no_parent = limits(128);
        no_parent.plan_nodes = 0;
        assert!(check_receipt_shape(1, 1, no_parent).is_err());
    }

    #[test]
    fn already_materialized_admission_rows_are_counted_once_not_per_response() {
        use crate::native::report_tests::running;
        use crate::native::{Key, Row};
        use focal_model::{ClaimId, ValidationMode};
        let core = running(&[
            (ValidationMode::Observe, false),
            (ValidationMode::Observe, false),
        ]);
        let id = ClaimId::from_u128(1);
        let claim = core.native_claim(id).unwrap();
        let Some(Row::Claim(owned)) = core.state.rows.get(&Key::Claim(id)) else {
            panic!("actual claim row");
        };
        let registry = owned.registrations().unwrap();
        assert_eq!(registry.rows().len(), 2);
        assert_eq!(required_registrations(claim, core.limits).unwrap(), 6);
        assert!(check_registration_capacity(claim, registry, core.limits).is_ok());
        let mut reduced = core.limits;
        reduced.evaluations_per_claim = 5;
        assert!(check_registration_capacity(claim, registry, reduced).is_err());
        reduced = core.limits;
        reduced.plan_edges = 2;
        assert!(required_registrations(claim, reduced).is_err());
    }

    #[test]
    fn slot_and_increment_declarations_price_every_authored_cycle_and_slot() {
        use crate::native::NativeCommand;
        use crate::native::report_tests::{EVALUATOR, ISSUER, binding, core, creation, publish};
        use focal_model::lifecycle::{Principal, aggregation, validation};
        use focal_model::{
            ClaimId, ContentHash, Deadline, HandlerRef, TimerId, ValidationId, ValidationKind,
            ValidationMode, ValidationPhase, ValidatorId,
        };
        let mut core = core();
        let mut input = creation(1, 1, &[(ValidationMode::Observe, false)], None);
        let NativeCommand::Create {
            claims,
            declarations,
        } = &mut input.command
        else {
            panic!("actual creation intent");
        };
        let handler = HandlerRef {
            id: ValidatorId::from_u128(800),
            version: ContentHash([3; 32]),
            agentic: false,
        };
        let steps = [validation::HandlerPolicy {
            handler: &handler,
            attempts: 1,
            proof_schema: ContentHash([4; 32]),
            diagnostic_schema: ContentHash([5; 32]),
        }];
        for (index, phase, target) in [
            (
                2,
                ValidationPhase::WholeWork,
                validation::TargetDeclaration::WholeWorkSlot {
                    index: 0,
                    name: "first",
                },
            ),
            (
                3,
                ValidationPhase::Increment,
                validation::TargetDeclaration::Increment,
            ),
        ] {
            declarations.push(
                validation::Declaration::new(
                    Principal::Actor(ISSUER),
                    validation::DeclarationSpec {
                        binding: binding(100 + u128::from(index)),
                        claim: ClaimId::from_u128(1),
                        issuer: ISSUER,
                        declaration_index: index,
                        kind: ValidationKind::Test,
                        phase,
                        mode: ValidationMode::Observe,
                        target,
                        program: validation::Program::Programmatic {
                            check: validation::PhasePolicy {
                                evaluator: EVALUATOR,
                                definition: ContentHash([6; 32]),
                                handlers: &steps,
                                required_policy: None,
                            },
                            quality: None,
                        },
                        deadline: Deadline {
                            timer: TimerId::from_u128(100 + u128::from(index)),
                            generation: 1,
                            at: 1000,
                        },
                    },
                    validation::Limits {
                        handlers: 4,
                        attempts: 8,
                        slot_bytes: 64,
                    },
                )
                .unwrap(),
            );
        }
        let checks = [aggregation::CheckPolicy {
            declaration_index: 2,
            validation: ValidationId::from_u128(102),
            mode: ValidationMode::Observe,
        }];
        claims[0].definition.acceptance = aggregation::AcceptancePolicy::new(
            binding(1),
            ISSUER,
            &[
                aggregation::SlotPolicy {
                    slot: 0,
                    missing_declaration_index: 10,
                    mode: ValidationMode::Required,
                    checks: &checks,
                },
                aggregation::SlotPolicy {
                    slot: 1,
                    missing_declaration_index: 11,
                    mode: ValidationMode::Required,
                    checks: &[],
                },
            ],
            declarations,
            aggregation::Limits {
                max_slots: 8,
                max_checks: 16,
                max_results: 32,
                max_updates: 32,
            },
        )
        .unwrap();
        publish(&mut core, 10, input);
        let claim = core.native_claim(ClaimId::from_u128(1)).unwrap();
        // One Admission + four cycles * (one Receipt + one slot check + two
        // Increment targets). Missing-slot presence does not invent declarations.
        assert_eq!(required_registrations(claim, core.limits).unwrap(), 17);
        let mut limits = core.limits;
        limits.plan_edges = 3;
        assert!(required_registrations(claim, limits).is_err());
    }
}
