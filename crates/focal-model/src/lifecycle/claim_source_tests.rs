use super::*;
use crate::{RootCommandId, SessionId, TenantId, ValidationId};
use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Relation,
    Scope,
    Requirement,
    Slot,
    Check,
}
impl Kind {
    fn index(self) -> usize {
        match self {
            Self::Relation => 0,
            Self::Scope => 1,
            Self::Requirement => 2,
            Self::Slot => 3,
            Self::Check => 4,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    Short(Kind),
    Extra(Kind),
    Error(Kind),
    EndError(Kind),
    Count(Kind, usize),
    Drift(Kind),
    OwnId,
}

trait Value: Clone {
    const KIND: Kind;
    fn changed(&self) -> Self;
}
impl Value for Relation {
    const KIND: Kind = Kind::Relation;
    fn changed(&self) -> Self {
        let mut value = self.clone();
        if value.kind == RelationKind::Subject {
            value.target = RelationTarget::Participant(ParticipantId::from_u128(77));
        }
        value
    }
}
impl Value for ScopeSpec<'_> {
    const KIND: Kind = Kind::Scope;
    fn changed(&self) -> Self {
        Self {
            key: "lib",
            ..*self
        }
    }
}
impl Value for RequirementRef {
    const KIND: Kind = Kind::Requirement;
    fn changed(&self) -> Self {
        Self {
            specification: ContentHash([13; 32]),
            ..*self
        }
    }
}
impl Value for SlotPolicy<'_> {
    const KIND: Kind = Kind::Slot;
    fn changed(&self) -> Self {
        Self {
            mode: ValidationMode::Observe,
            ..*self
        }
    }
}
impl Value for CheckPolicy {
    const KIND: Kind = Kind::Check;
    fn changed(&self) -> Self {
        Self {
            mode: ValidationMode::Observe,
            ..*self
        }
    }
}

struct Source<'a> {
    spec: ClaimSpec<'a>,
    fault: Cell<Fault>,
    walks: [Cell<usize>; 5],
}
impl<'a> Source<'a> {
    fn new(spec: ClaimSpec<'a>) -> Self {
        Self {
            spec,
            fault: Cell::new(Fault::None),
            walks: std::array::from_fn(|_| Cell::new(0)),
        }
    }
    fn count(&self, kind: Kind, actual: usize) -> usize {
        match self.fault.get() {
            Fault::Count(target, count) if target == kind => count,
            _ => actual,
        }
    }
    fn stream<'s, T: Value>(&'s self, values: &'a [T]) -> Stream<'s, 'a, T> {
        let count = &self.walks[T::KIND.index()];
        count.set(count.get() + 1);
        Stream {
            source: self,
            values,
            index: 0,
        }
    }
    fn reset_walks(&self) {
        for value in &self.walks {
            value.set(0);
        }
    }
}
struct Stream<'s, 'a, T> {
    source: &'s Source<'a>,
    values: &'a [T],
    index: usize,
}
impl<T: Value> Iterator for Stream<'_, '_, T> {
    type Item = Result<T, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        let fault = self.source.fault.get();
        let index = self.index;
        self.index += 1;
        if fault == Fault::Error(T::KIND) && index == 0 {
            return Some(Err(ContractError::WrongLedger));
        }
        if fault == Fault::Short(T::KIND) && index >= self.values.len().saturating_sub(1) {
            return None;
        }
        if fault == Fault::EndError(T::KIND) && index == self.values.len() {
            return Some(Err(ContractError::WrongLedger));
        }
        let value = if fault == Fault::Extra(T::KIND) && index == self.values.len() {
            self.values.last()
        } else {
            self.values.get(index)
        }?;
        Some(Ok(if fault == Fault::Drift(T::KIND) {
            value.changed()
        } else {
            value.clone()
        }))
    }
}
struct Slot<'s, 'a> {
    source: &'s Source<'a>,
    policy: SlotPolicy<'a>,
}
impl<'a> ClaimSlotSource for Slot<'_, 'a> {
    type Checks<'s>
        = Stream<'s, 'a, CheckPolicy>
    where
        Self: 's;
    fn fields(&self) -> ClaimSlotFields {
        ClaimSlotFields {
            slot: self.policy.slot,
            missing_declaration_index: self.policy.missing_declaration_index,
            mode: self.policy.mode,
            checks: self.source.count(Kind::Check, self.policy.checks.len()),
        }
    }
    fn checks(&self) -> Self::Checks<'_> {
        self.source.stream(self.policy.checks)
    }
}
struct Slots<'s, 'a>(Stream<'s, 'a, SlotPolicy<'a>>);
impl<'s, 'a> Iterator for Slots<'s, 'a> {
    type Item = Result<Slot<'s, 'a>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|value| {
            value.map(|policy| Slot {
                source: self.0.source,
                policy,
            })
        })
    }
}
impl<'a> ClaimSource<'a> for Source<'a> {
    type Relations<'s>
        = Stream<'s, 'a, Relation>
    where
        Self: 's;
    type Scopes<'s>
        = Stream<'s, 'a, ScopeSpec<'a>>
    where
        Self: 's;
    type Requirements<'s>
        = Stream<'s, 'a, RequirementRef>
    where
        Self: 's;
    type Slot<'s>
        = Slot<'s, 'a>
    where
        Self: 's;
    type Slots<'s>
        = Slots<'s, 'a>
    where
        Self: 's;
    fn fields(&self) -> ClaimFields<'a> {
        let mut value = self.spec.fields();
        if self.fault.get() == Fault::OwnId {
            value.id = ClaimId::from_u128(99);
        }
        value
    }
    fn relation_count(&self) -> usize {
        self.count(Kind::Relation, self.spec.relations.len())
    }
    fn scope_count(&self) -> usize {
        self.count(Kind::Scope, self.spec.scopes.len())
    }
    fn requirement_count(&self) -> usize {
        self.count(Kind::Requirement, self.spec.requirements.len())
    }
    fn slot_count(&self) -> usize {
        self.count(Kind::Slot, self.spec.slots.len())
    }
    fn relations(&self) -> Self::Relations<'_> {
        self.stream(self.spec.relations)
    }
    fn scopes(&self) -> Self::Scopes<'_> {
        self.stream(self.spec.scopes)
    }
    fn requirements(&self) -> Self::Requirements<'_> {
        self.stream(self.spec.requirements)
    }
    fn slots(&self) -> Self::Slots<'_> {
        Slots(self.stream(self.spec.slots))
    }
}

const PINS: [RequirementRef; 2] = [
    RequirementRef {
        id: ValidationId(8u128.to_be_bytes()),
        specification: ContentHash([9; 32]),
    },
    RequirementRef {
        id: ValidationId(9u128.to_be_bytes()),
        specification: ContentHash([10; 32]),
    },
];
const CHECKS: [CheckPolicy; 2] = [
    CheckPolicy {
        declaration_index: 1,
        validation: PINS[0].id,
        mode: ValidationMode::Required,
    },
    CheckPolicy {
        declaration_index: 2,
        validation: PINS[1].id,
        mode: ValidationMode::Required,
    },
];
const SLOTS: [SlotPolicy<'static>; 2] = [
    SlotPolicy {
        slot: 0,
        missing_declaration_index: 3,
        mode: ValidationMode::Required,
        checks: &CHECKS,
    },
    SlotPolicy {
        slot: 1,
        missing_declaration_index: 4,
        mode: ValidationMode::Required,
        checks: &[],
    },
];
fn relations() -> [Relation; 4] {
    [
        Relation {
            kind: RelationKind::Issuer,
            target: RelationTarget::Participant(ParticipantId::from_u128(5)),
        },
        Relation {
            kind: RelationKind::Subject,
            target: RelationTarget::Participant(ParticipantId::from_u128(6)),
        },
        Relation {
            kind: RelationKind::ClaimAction,
            target: RelationTarget::Action(ActionType::Work),
        },
        Relation {
            kind: RelationKind::CausedBy,
            target: RelationTarget::Root(RootCommandId::from_u128(7)),
        },
    ]
}
fn spec(relations: &[Relation]) -> ClaimSpec<'_> {
    ClaimSpec {
        ledger: LedgerId {
            tenant: TenantId::from_u128(1),
            session: SessionId::from_u128(2),
        },
        id: ClaimId::from_u128(3),
        schema: 1,
        occurrence: OccurrenceId::from_u128(4),
        description: "prove café",
        relations,
        scopes: &[ScopeSpec {
            kind: ScopeKind::File,
            key: "src",
        }],
        requirements: &PINS,
        slots: &SLOTS,
        deadline: None,
    }
}
fn limits() -> Limits {
    Limits {
        description_bytes: 128,
        relations: 16,
        scopes: 8,
        scope_key_bytes: 128,
        requirements: 16,
        slots: 8,
        checks: 16,
        construction_bytes: 16_384,
    }
}
fn prepare<'s, 'a>(source: &'s Source<'a>) -> ClaimSourcePlan<'s, 'a, Source<'a>> {
    bytes::fail_after(0, || {
        ClaimSourcePlan::prepare(source, limits(), usize::MAX).unwrap()
    })
}

#[test]
fn typed_and_sequential_nested_sources_share_identity_roles_and_exact_final_buffers() {
    let relations = relations();
    let spec = spec(&relations);
    let source = Source::new(spec);
    let plan = prepare(&source);
    assert_eq!(plan.issuer(), ParticipantId::from_u128(5));
    assert_eq!(plan.subject(), ParticipantId::from_u128(6));
    assert_eq!(plan.action(), ActionType::Work);
    assert_eq!(plan.cause(), &Cause::Root(RootCommandId::from_u128(7)));
    plan.check_postable().unwrap();
    let bytes = plan.construction_charge();
    let visits = plan.build_visits();
    let allocations = plan.construction_heap_allocations();
    assert_eq!(allocations, 7);
    let expected = ClaimDescriptor::prepare(spec, limits())
        .unwrap()
        .build(bytes)
        .unwrap();
    assert_eq!(plan.content_hash(), expected.content_hash());
    assert_eq!(plan.intent_fingerprint(), expected.intent_fingerprint());
    source.reset_walks();
    let actual = plan.build(bytes, visits).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.retained_bytes().unwrap(), bytes);
    assert_eq!(actual.heap_allocations().unwrap(), allocations);
    assert_eq!(source.walks.each_ref().map(Cell::get), [1, 1, 1, 1, 2]);
    assert_eq!(actual.slots().nth(1).unwrap().checks.len(), 0);
}

#[test]
fn all_outer_and_nested_cardinalities_and_terminal_errors_are_checked_without_allocation() {
    let relations = relations();
    let source = Source::new(spec(&relations));
    for kind in [
        Kind::Relation,
        Kind::Scope,
        Kind::Requirement,
        Kind::Slot,
        Kind::Check,
    ] {
        for fault in [
            Fault::Short(kind),
            Fault::Extra(kind),
            Fault::Error(kind),
            Fault::EndError(kind),
            Fault::Count(kind, 0),
        ] {
            source.fault.set(fault);
            let result = bytes::fail_after(0, || {
                ClaimSourcePlan::prepare(&source, limits(), usize::MAX)
            });
            assert!(result.is_err(), "{fault:?}");
        }
    }
    source.fault.set(Fault::None);
    prepare(&source);
}

#[test]
fn build_revalidates_every_actual_owned_collection_and_own_id_before_returning() {
    let relations = relations();
    let source = Source::new(spec(&relations));
    for fault in [
        Fault::OwnId,
        Fault::Drift(Kind::Relation),
        Fault::Drift(Kind::Scope),
        Fault::Drift(Kind::Requirement),
        Fault::Drift(Kind::Slot),
        Fault::Drift(Kind::Check),
        Fault::Short(Kind::Check),
        Fault::Extra(Kind::Slot),
        Fault::Error(Kind::Scope),
        Fault::EndError(Kind::Requirement),
    ] {
        let plan = prepare(&source);
        let (bytes, visits) = (plan.construction_charge(), plan.build_visits());
        source.fault.set(fault);
        assert!(plan.build(bytes, visits).is_err(), "{fault:?}");
        source.fault.set(Fault::None);
        let retry = prepare(&source);
        let (bytes, visits) = (retry.construction_charge(), retry.build_visits());
        retry.build(bytes, visits).unwrap();
    }
}

#[test]
fn preparation_and_build_enforce_visits_bytes_and_each_fallible_final_allocation() {
    let relations = relations();
    let source = Source::new(spec(&relations));
    let plan = prepare(&source);
    let (charge, inspection, build, allocations) = (
        plan.construction_charge(),
        plan.inspection_visits(),
        plan.build_visits(),
        plan.construction_heap_allocations(),
    );
    assert!(matches!(
        ClaimSourcePlan::prepare(&source, limits(), inspection - 1),
        Err(ContractError::Capacity)
    ));
    ClaimSourcePlan::prepare(&source, limits(), inspection).unwrap();
    bytes::fail_after(0, || {
        assert!(matches!(
            prepare(&source).build(charge - 1, build),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            prepare(&source).build(charge, build - 1),
            Err(ContractError::Capacity)
        ));
    });
    for allowance in 0..allocations {
        assert!(bytes::fail_after(allowance, || prepare(&source).build(charge, build)).is_err());
        prepare(&source).build(charge, build).unwrap();
    }
    let descriptor =
        bytes::fail_after(allocations, || prepare(&source).build(charge, build)).unwrap();
    assert_eq!(descriptor.retained_bytes().unwrap(), charge);
    source.fault.set(Fault::Count(Kind::Relation, usize::MAX));
    assert!(matches!(
        bytes::fail_after(0, || ClaimSourcePlan::prepare(
            &source,
            Limits {
                relations: usize::MAX,
                ..limits()
            },
            usize::MAX
        )),
        Err(ContractError::Capacity)
    ));
}

#[test]
fn shared_policy_rules_reject_collisions_unpinned_checks_and_duplicate_ordered_pins() {
    let relations = relations();
    let base = spec(&relations);
    let collisions = [
        SlotPolicy {
            missing_declaration_index: 1,
            ..SLOTS[0]
        },
        SLOTS[1],
    ];
    let unpinned = [CheckPolicy {
        validation: ValidationId::from_u128(77),
        ..CHECKS[0]
    }];
    let wrong_slots = [SlotPolicy {
        checks: &unpinned,
        ..SLOTS[0]
    }];
    let duplicate_pins = [PINS[0], PINS[0]];
    for invalid in [
        ClaimSpec {
            slots: &collisions,
            ..base
        },
        ClaimSpec {
            slots: &wrong_slots,
            ..base
        },
        ClaimSpec {
            requirements: &duplicate_pins,
            ..base
        },
    ] {
        let source = Source::new(invalid);
        let typed = ClaimDescriptor::prepare(invalid, limits()).unwrap_err();
        let sequential = ClaimSourcePlan::prepare(&source, limits(), usize::MAX)
            .err()
            .unwrap();
        assert_eq!(typed, sequential);
    }
}
