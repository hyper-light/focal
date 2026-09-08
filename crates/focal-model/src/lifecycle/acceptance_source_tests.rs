use super::*;
use crate::lifecycle::validation as v;
use crate::{ObjectId, TenantId, ValidationKind, ValidationPhase};
use std::cell::Cell;
use v::tests::{ISSUER, binding, programmatic};

fn limits() -> Limits {
    Limits {
        max_slots: 8,
        max_checks: 16,
        max_results: 16,
        max_updates: 8,
    }
}
fn specification(
    id: u128,
    index: u32,
    target: TargetDeclaration<'static>,
) -> v::DeclarationSpec<'static> {
    let mut value = v::tests::specification(ValidationMode::Required, programmatic(false));
    value.binding = binding(id);
    value.declaration_index = index;
    value.target = target;
    value.phase = match target {
        TargetDeclaration::Admission => ValidationPhase::Admission,
        TargetDeclaration::Increment => ValidationPhase::Increment,
        _ => ValidationPhase::WholeWork,
    };
    if target == TargetDeclaration::Delivery {
        value.kind = ValidationKind::Receipt;
        value.program = v::Program::Delivery;
    }
    value
}
fn declaration(value: v::DeclarationSpec<'_>) -> Declaration {
    Declaration::new(Principal::Actor(value.issuer), value, v::tests::limits()).unwrap()
}
fn declarations() -> Vec<Declaration> {
    vec![
        declaration(specification(
            203,
            7,
            TargetDeclaration::WholeWorkSlot {
                index: 4,
                name: "output",
            },
        )),
        declaration(specification(201, 2, TargetDeclaration::Delivery)),
        declaration(specification(202, 5, TargetDeclaration::Admission)),
        declaration(specification(204, 9, TargetDeclaration::Increment)),
    ]
}
fn checked(values: &[Declaration]) -> Vec<CheckedDeclaration> {
    values
        .iter()
        .map(Declaration::checked_declaration)
        .collect()
}
const CHECK: CheckPolicy = CheckPolicy {
    declaration_index: 7,
    validation: ValidationId::from_u128(203),
    mode: ValidationMode::Required,
};
fn slots(checks: &[CheckPolicy]) -> [SlotPolicy<'_>; 3] {
    [
        SlotPolicy {
            slot: 4,
            missing_declaration_index: 6,
            mode: ValidationMode::Required,
            checks,
        },
        SlotPolicy {
            slot: 8,
            missing_declaration_index: 12,
            mode: ValidationMode::Required,
            checks: &[],
        },
        SlotPolicy {
            slot: 99,
            missing_declaration_index: 1,
            mode: ValidationMode::Observe,
            checks: &[],
        },
    ]
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Fault {
    #[default]
    None,
    Missing,
    Extra,
    Error,
}

struct Values<'a, T> {
    values: &'a [T],
    fault: Fault,
    error: ContractError,
    index: usize,
}
impl<T: Copy> Iterator for Values<'_, T> {
    type Item = Result<T, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        if self.fault == Fault::Error && index == 0 {
            return Some(Err(self.error));
        }
        if self.fault == Fault::Missing && index + 1 == self.values.len() {
            return None;
        }
        if self.fault == Fault::Extra && index == self.values.len() {
            return self.values.first().copied().map(Ok);
        }
        self.values.get(index).copied().map(Ok)
    }
}

struct Slot<'a> {
    policy: SlotPolicy<'a>,
    fault: Fault,
}
impl ClaimSlotSource for Slot<'_> {
    type Checks<'s>
        = Values<'s, CheckPolicy>
    where
        Self: 's;
    fn fields(&self) -> ClaimSlotFields {
        ClaimSlotFields {
            slot: self.policy.slot,
            missing_declaration_index: self.policy.missing_declaration_index,
            mode: self.policy.mode,
            checks: self.policy.checks.len(),
        }
    }
    fn checks(&self) -> Self::Checks<'_> {
        Values {
            values: self.policy.checks,
            fault: if self.policy.checks.is_empty() {
                Fault::None
            } else {
                self.fault
            },
            error: ContractError::MissingEvidence,
            index: 0,
        }
    }
}
struct SlotValues<'a> {
    values: Values<'a, SlotPolicy<'a>>,
    check_fault: Fault,
}
impl<'a> Iterator for SlotValues<'a> {
    type Item = Result<Slot<'a>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().map(|value| {
            value.map(|policy| Slot {
                policy,
                fault: self.check_fault,
            })
        })
    }
}

#[derive(Debug)]
struct Source<'a> {
    slots: &'a [SlotPolicy<'a>],
    declarations: &'a [CheckedDeclaration],
    alternate_slots: &'a [SlotPolicy<'a>],
    alternate_declarations: &'a [CheckedDeclaration],
    slot_scans: Cell<usize>,
    declaration_scans: Cell<usize>,
    switch_slots_at: Cell<Option<usize>>,
    switch_declarations_at: Cell<Option<usize>>,
    slot_count: Cell<Option<usize>>,
    declaration_count: Cell<Option<usize>>,
    slot_fault: Cell<Fault>,
    declaration_fault: Cell<Fault>,
    check_fault: Cell<Fault>,
}
impl<'a> Source<'a> {
    fn new(slots: &'a [SlotPolicy<'a>], declarations: &'a [CheckedDeclaration]) -> Self {
        Self {
            slots,
            declarations,
            alternate_slots: slots,
            alternate_declarations: declarations,
            slot_scans: Cell::new(0),
            declaration_scans: Cell::new(0),
            switch_slots_at: Cell::new(None),
            switch_declarations_at: Cell::new(None),
            slot_count: Cell::new(None),
            declaration_count: Cell::new(None),
            slot_fault: Cell::new(Fault::None),
            declaration_fault: Cell::new(Fault::None),
            check_fault: Cell::new(Fault::None),
        }
    }
    fn reset_scans(&self) {
        self.slot_scans.set(0);
        self.declaration_scans.set(0);
    }
    fn passes(&self) -> AcceptanceSourcePasses {
        AcceptanceSourcePasses {
            declarations: self.declaration_scans.get(),
            slots: self.slot_scans.get(),
        }
    }
}
impl<'a> AcceptanceSource for Source<'a> {
    type Slot<'s>
        = Slot<'a>
    where
        Self: 's;
    type Slots<'s>
        = SlotValues<'a>
    where
        Self: 's;
    type Declarations<'s>
        = Values<'a, CheckedDeclaration>
    where
        Self: 's;
    fn slot_count(&self) -> usize {
        self.slot_count.get().unwrap_or(self.slots.len())
    }
    fn declaration_count(&self) -> usize {
        self.declaration_count
            .get()
            .unwrap_or(self.declarations.len())
    }
    fn slots(&self) -> Self::Slots<'_> {
        let scan = self.slot_scans.get() + 1;
        self.slot_scans.set(scan);
        let values = if self.switch_slots_at.get().is_some_and(|at| scan >= at) {
            self.alternate_slots
        } else {
            self.slots
        };
        SlotValues {
            values: Values {
                values,
                fault: self.slot_fault.get(),
                error: ContractError::StaleReceipt,
                index: 0,
            },
            check_fault: self.check_fault.get(),
        }
    }
    fn declarations(&self) -> Self::Declarations<'_> {
        let scan = self.declaration_scans.get() + 1;
        self.declaration_scans.set(scan);
        let values = if self
            .switch_declarations_at
            .get()
            .is_some_and(|at| scan >= at)
        {
            self.alternate_declarations
        } else {
            self.declarations
        };
        Values {
            values,
            fault: self.declaration_fault.get(),
            error: ContractError::StaleEvaluation,
            index: 0,
        }
    }
}
fn prepare<'s, 'a>(source: &'s Source<'a>) -> AcceptanceSourcePlan<'s, Source<'a>> {
    AcceptancePolicy::prepare_source(binding(200), ISSUER, source, limits(), usize::MAX).unwrap()
}

#[test]
fn unordered_checked_declarations_match_existing_policy_and_exact_complete_scan_counts() {
    let mut declarations = declarations();
    let checks = [CHECK];
    let slots = slots(&checks);
    let reference =
        AcceptancePolicy::new(binding(200), ISSUER, &slots, &declarations, limits()).unwrap();
    for _ in 0..declarations.len() {
        let tokens = checked(&declarations);
        let source = Source::new(&slots, &tokens);
        let plan = bytes::fail_after(0, || prepare(&source));
        let expected_inspection = AcceptanceSourcePasses {
            declarations: 2 + 2 * 4 + 1,
            slots: 3 + 4 + 3 + 1,
        };
        let expected_build = AcceptanceSourcePasses {
            declarations: 2 + 3 * 4 + 1,
            slots: 4 + 4 + 3 + 1,
        };
        assert_eq!(plan.inspection_passes(), expected_inspection);
        assert_eq!(source.passes(), expected_inspection);
        assert_eq!(plan.build_passes(), expected_build);
        assert_eq!(plan.intent_fingerprint(), reference.intent_fingerprint());
        assert_eq!(
            plan.construction_heap_bytes(),
            reference.copy_heap_bytes().unwrap()
        );
        assert_eq!(
            plan.construction_heap_allocations(),
            reference.copy_heap_allocations().unwrap()
        );
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        source.reset_scans();
        let policy = plan.build(charge, visits).unwrap();
        assert_eq!(source.passes(), expected_build);
        assert_eq!(policy, reference);
        let actual: Vec<_> = policy.slots().collect();
        assert!(actual[1].checks.is_empty());
        assert_eq!(actual[1].mode, ValidationMode::Required);
        assert!(actual[2].checks.is_empty());
        assert_eq!(actual[2].mode, ValidationMode::Observe);
        declarations.rotate_left(1);
    }
}

#[test]
fn exact_visit_byte_and_allocation_limits_refuse_before_copy_and_retry_identically() {
    let declarations = declarations();
    let tokens = checked(&declarations);
    let checks = [CHECK];
    let slots = slots(&checks);
    let source = Source::new(&slots, &tokens);
    let quote = prepare(&source);
    let inspection = quote.inspection_visits();
    let visits = quote.build_visits();
    let charge = quote.construction_charge();
    let allocations = quote.construction_heap_allocations();
    let intent = quote.intent_fingerprint();
    assert_eq!(allocations, 3);
    assert_eq!(
        charge,
        size_of::<AcceptancePolicy>() + quote.construction_heap_bytes()
    );
    bytes::fail_after(0, || {
        assert!(
            AcceptancePolicy::prepare_source(binding(200), ISSUER, &source, limits(), inspection)
                .is_ok()
        );
        assert!(matches!(
            AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                inspection - 1
            ),
            Err(ContractError::Capacity)
        ));
    });
    for (max_bytes, max_visits) in [(charge - 1, visits), (charge, visits - 1)] {
        let plan = prepare(&source);
        source.reset_scans();
        bytes::fail_after(allocations, || {
            assert!(matches!(
                plan.build(max_bytes, max_visits),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(allocations));
        });
        assert_eq!(
            source.passes(),
            AcceptanceSourcePasses {
                declarations: 0,
                slots: 0
            }
        );
    }
    let original = quote.build(charge, visits).unwrap();
    let original_slots = original.slots.as_ptr();
    for after in 0..allocations {
        let plan = bytes::fail_after(0, || prepare(&source));
        bytes::fail_after(after, || {
            assert!(
                matches!(plan.build(charge, visits), Err(ContractError::Capacity)),
                "allocation {after}"
            );
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        assert_eq!(original.slots.as_ptr(), original_slots);
        assert_eq!(original.intent_fingerprint(), intent);
        let retry =
            bytes::fail_after(allocations, || prepare(&source).build(charge, visits)).unwrap();
        assert_eq!(retry, original);
        assert_eq!(retry.retained_bytes().unwrap(), charge);
        assert_ne!(retry.slots.as_ptr(), original_slots);
    }
}

#[test]
fn complete_checked_parent_delivery_identity_and_slot_correspondence_are_required() {
    let declarations = declarations();
    let tokens = checked(&declarations);
    let checks = [CHECK];
    let slots = slots(&checks);
    let refuse = |slots: &[SlotPolicy<'_>], declarations: &[CheckedDeclaration]| {
        let source = Source::new(slots, declarations);
        assert!(matches!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            )),
            Err(ContractError::InvalidPolicy)
        ));
    };
    let original = specification(204, 9, TargetDeclaration::Increment);
    let wrong_parent = v::DeclarationSpec {
        claim: ClaimId::from_u128(999),
        ..original
    };
    let wrong_issuer = v::DeclarationSpec {
        issuer: ParticipantId::from_u128(999),
        ..original
    };
    let wrong_ledger = v::DeclarationSpec {
        binding: Binding {
            ledger: crate::LedgerId {
                tenant: TenantId::from_u128(999),
                ..original.binding.ledger
            },
            ..original.binding
        },
        ..original
    };
    let duplicate_index = v::DeclarationSpec {
        declaration_index: 5,
        ..original
    };
    let duplicate_id = v::DeclarationSpec {
        binding: binding(201),
        ..original
    };
    for changed in [
        wrong_parent,
        wrong_issuer,
        wrong_ledger,
        duplicate_index,
        duplicate_id,
    ] {
        let mut altered = tokens.clone();
        altered[3] = declaration(changed).checked_declaration();
        refuse(&slots, &altered);
    }
    refuse(&slots, &tokens[1..]); // The slot check has no declaration.
    let no_delivery = [tokens[0], tokens[2], tokens[3]];
    refuse(&slots, &no_delivery);
    let mut unmapped = tokens.clone();
    unmapped[0] = declaration(specification(
        203,
        7,
        TargetDeclaration::WholeWorkSlot {
            index: 5,
            name: "other",
        },
    ))
    .checked_declaration();
    refuse(&slots, &unmapped);
    for check in [
        CheckPolicy {
            validation: ValidationId::from_u128(999),
            ..CHECK
        },
        CheckPolicy {
            declaration_index: 8,
            ..CHECK
        },
        CheckPolicy {
            mode: ValidationMode::Observe,
            ..CHECK
        },
        CheckPolicy {
            validation: ValidationId::from_u128(202),
            declaration_index: 5,
            ..CHECK
        },
    ] {
        let altered_checks = [check];
        let altered = [
            SlotPolicy {
                checks: &altered_checks,
                ..slots[0]
            },
            slots[1],
            slots[2],
        ];
        refuse(&altered, &tokens);
    }
    for altered in [
        [
            SlotPolicy {
                missing_declaration_index: 7,
                ..slots[0]
            },
            slots[1],
            slots[2],
        ],
        [
            slots[0],
            SlotPolicy {
                missing_declaration_index: 1,
                ..slots[1]
            },
            slots[2],
        ],
        [slots[1], slots[0], slots[2]],
        [
            slots[0],
            SlotPolicy {
                checks: &checks,
                ..slots[1]
            },
            slots[2],
        ],
        [
            SlotPolicy {
                checks: &[],
                ..slots[0]
            },
            slots[1],
            slots[2],
        ],
    ] {
        refuse(&altered, &tokens);
    }
    let source = Source::new(&slots, &tokens);
    for (claim, issuer) in [
        (
            Binding {
                object: ObjectId::from_u128(0),
                ..binding(200)
            },
            ISSUER,
        ),
        (binding(200), ParticipantId::from_u128(0)),
    ] {
        assert!(matches!(
            AcceptancePolicy::prepare_source(claim, issuer, &source, limits(), usize::MAX),
            Err(ContractError::InvalidPolicy)
        ));
    }
}

#[test]
fn declared_counts_terminal_probes_and_iterator_errors_are_exact() {
    let declarations = declarations();
    let tokens = checked(&declarations);
    let checks = [CHECK];
    let slots = slots(&checks);
    let source = Source::new(&slots, &tokens);
    for (fault, expected) in [
        (Fault::Missing, ContractError::InvalidPolicy),
        (Fault::Extra, ContractError::InvalidPolicy),
        (Fault::Error, ContractError::StaleReceipt),
    ] {
        source.slot_fault.set(fault);
        assert_eq!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            ))
            .unwrap_err(),
            expected
        );
    }
    source.slot_fault.set(Fault::None);
    for (fault, expected) in [
        (Fault::Missing, ContractError::InvalidPolicy),
        (Fault::Extra, ContractError::InvalidPolicy),
        (Fault::Error, ContractError::StaleEvaluation),
    ] {
        source.declaration_fault.set(fault);
        assert_eq!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            ))
            .unwrap_err(),
            expected
        );
    }
    source.declaration_fault.set(Fault::None);
    for (fault, expected) in [
        (Fault::Missing, ContractError::InvalidPolicy),
        (Fault::Extra, ContractError::InvalidPolicy),
        (Fault::Error, ContractError::MissingEvidence),
    ] {
        source.check_fault.set(fault);
        assert_eq!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            ))
            .unwrap_err(),
            expected
        );
    }
    source.check_fault.set(Fault::None);
    for (cell, count) in [
        (&source.slot_count, 0),
        (&source.slot_count, 2),
        (&source.slot_count, 4),
        (&source.declaration_count, 0),
        (&source.declaration_count, 3),
        (&source.declaration_count, 5),
    ] {
        cell.set(Some(count));
        assert!(matches!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            )),
            Err(ContractError::InvalidPolicy)
        ));
        cell.set(None);
    }
    source.slot_count.set(Some(usize::MAX));
    source.reset_scans();
    assert!(matches!(
        AcceptancePolicy::prepare_source(binding(200), ISSUER, &source, limits(), usize::MAX),
        Err(ContractError::Capacity)
    ));
    assert_eq!(
        source.passes(),
        AcceptanceSourcePasses {
            declarations: 0,
            slots: 0
        }
    );
    source.slot_count.set(None);
    source.declaration_count.set(Some(usize::MAX));
    assert!(matches!(
        AcceptancePolicy::prepare_source(binding(200), ISSUER, &source, limits(), usize::MAX),
        Err(ContractError::InvalidPolicy)
    ));
}

#[test]
fn valid_alternate_proofs_and_slots_cannot_replace_any_inspection_or_copy_pass() {
    let declarations = declarations();
    let tokens = checked(&declarations);
    let checks = [CHECK];
    let slots = slots(&checks);
    let reference =
        AcceptancePolicy::new(binding(200), ISSUER, &slots, &declarations, limits()).unwrap();
    let mut alternate = tokens.clone();
    let mut changed_spec = specification(204, 9, TargetDeclaration::Increment);
    changed_spec.program = programmatic(true);
    alternate[3] = declaration(changed_spec).checked_declaration();
    let altered_slots = [
        slots[0],
        SlotPolicy {
            mode: ValidationMode::Observe,
            ..slots[1]
        },
        slots[2],
    ];
    // Both replacements are independently valid complete acceptance inputs.
    let changed_proofs = Source::new(&slots, &alternate);
    let changed_slots = Source::new(&altered_slots, &tokens);
    let _ = prepare(&changed_proofs);
    let _ = prepare(&changed_slots);
    let source = Source {
        alternate_slots: &altered_slots,
        alternate_declarations: &alternate,
        ..Source::new(&slots, &tokens)
    };
    for declarations in [true, false] {
        source.reset_scans();
        let switch = if declarations {
            &source.switch_declarations_at
        } else {
            &source.switch_slots_at
        };
        switch.set(Some(2));
        assert!(matches!(
            bytes::fail_after(0, || AcceptancePolicy::prepare_source(
                binding(200),
                ISSUER,
                &source,
                limits(),
                usize::MAX
            )),
            Err(ContractError::ContentConflict)
        ));
        switch.set(None);
    }
    for declarations in [true, false] {
        source.reset_scans();
        let plan = prepare(&source);
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        let inspection = plan.inspection_passes();
        let switch = if declarations {
            &source.switch_declarations_at
        } else {
            &source.switch_slots_at
        };
        source.reset_scans();
        switch.set(Some(if declarations {
            inspection.declarations + 1
        } else {
            inspection.slots + 1
        }));
        assert!(matches!(
            plan.build(charge, visits),
            Err(ContractError::ContentConflict)
        ));
        switch.set(None);
        source.reset_scans();
        let retry = prepare(&source).build(charge, visits).unwrap();
        assert_eq!(retry, reference);
    }
}
