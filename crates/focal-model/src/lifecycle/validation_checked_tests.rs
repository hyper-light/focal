use super::*;
use crate::lifecycle::{memory as allocation, validation_descriptor as authored};
use crate::{HandlerRef, ObjectId, ObjectRevision, SessionId, TenantId, TimerId};
use std::cell::Cell;

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId([2; 16]),
    version: ContentHash([3; 32]),
    agentic: false,
};
const ROWS: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &HANDLER,
    attempts: 2,
    proof_schema: ContentHash([4; 32]),
    diagnostic_schema: ContentHash([5; 32]),
}];
const CONTRIBUTORS: [ParticipantId; 1] = [ParticipantId([6; 16])];

fn spec() -> authored::ValidationSpec<'static> {
    authored::ValidationSpec {
        ledger: LedgerId {
            tenant: TenantId([7; 16]),
            session: SessionId([8; 16]),
        },
        id: ValidationId([9; 16]),
        schema: 1,
        claim: ClaimId([10; 16]),
        issuer: ISSUER,
        declaration_index: 11,
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::WholeWorkSlot {
            index: 12,
            name: "résultat",
        },
        program: Program::Programmatic {
            check: PhasePolicy {
                evaluator: ParticipantId([13; 16]),
                definition: ContentHash([14; 32]),
                handlers: &ROWS,
                required_policy: Some(ContentHash([15; 32])),
            },
            quality: None,
        },
        deadline: Deadline {
            timer: TimerId([16; 16]),
            generation: 17,
            at: 18,
        },
        description: "Inspect the actual evidence.",
        quality_bar: None,
        contributed_by: &CONTRIBUTORS,
        policy_revision: 19,
    }
}
fn limits() -> authored::Limits {
    authored::Limits {
        declaration: Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 32,
        },
        description_bytes: 128,
        quality_bar_bytes: 128,
        contributors: 4,
        construction_bytes: 4096,
    }
}
fn declaration_spec(value: authored::ValidationSpec<'_>) -> DeclarationSpec<'_> {
    DeclarationSpec {
        binding: Binding {
            ledger: value.ledger,
            object: ObjectId(value.id.0),
            content: ContentHash([20; 32]),
            revision: ObjectRevision(1),
        },
        claim: value.claim,
        issuer: value.issuer,
        declaration_index: value.declaration_index,
        kind: value.kind,
        phase: value.phase,
        mode: value.mode,
        target: value.target,
        program: value.program,
        deadline: value.deadline,
    }
}

struct CountedDeclaration {
    spec: DeclarationSpec<'static>,
    accesses: Cell<usize>,
}
impl PolicySource for CountedDeclaration {
    type Handlers<'s>
        = <DeclarationSpec<'static> as PolicySource>::Handlers<'s>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        self.accesses.set(self.accesses.get() + 1);
        self.spec.handlers(phase)
    }
}
impl DeclarationSource<'static> for CountedDeclaration {
    fn fields(&self) -> DeclarationFields<'static> {
        self.accesses.set(self.accesses.get() + 1);
        self.spec.fields()
    }
}

#[test]
fn owned_and_prepared_metadata_match_all_target_families_and_retain_hidden_policy_identity() {
    let base = declaration_spec(spec());
    let counted = CountedDeclaration {
        spec: base,
        accesses: Cell::new(0),
    };
    let cached = Declaration::prepare_source(
        Principal::Actor(ISSUER),
        &counted,
        limits().declaration,
        usize::MAX,
    )
    .unwrap();
    assert_eq!(counted.accesses.get(), 3);
    counted.accesses.set(0);
    let _ = allocation::fail_after(0, || cached.checked_declaration());
    assert_eq!(counted.accesses.get(), 0);
    for (target, phase, kind, program, expected) in [
        (
            base.target,
            ValidationPhase::WholeWork,
            ValidationKind::Inspection,
            base.program,
            CheckedDeclarationTarget::Slot(12),
        ),
        (
            TargetDeclaration::Delivery,
            ValidationPhase::WholeWork,
            ValidationKind::Receipt,
            Program::Delivery,
            CheckedDeclarationTarget::Delivery,
        ),
        (
            TargetDeclaration::Admission,
            ValidationPhase::Admission,
            ValidationKind::Inspection,
            base.program,
            CheckedDeclarationTarget::Admission,
        ),
        (
            TargetDeclaration::Increment,
            ValidationPhase::Increment,
            ValidationKind::Inspection,
            base.program,
            CheckedDeclarationTarget::Increment,
        ),
    ] {
        let value = DeclarationSpec {
            target,
            phase,
            kind,
            program,
            ..base
        };
        let plan = allocation::fail_after(0, || {
            Declaration::prepare_source(
                Principal::Actor(ISSUER),
                &value,
                limits().declaration,
                usize::MAX,
            )
        })
        .unwrap();
        let metadata = allocation::fail_after(0, || plan.checked_declaration());
        assert_eq!(metadata.binding(), value.binding);
        assert_eq!(metadata.claim(), value.claim);
        assert_eq!(metadata.issuer(), value.issuer);
        assert_eq!(metadata.declaration_index(), value.declaration_index);
        assert_eq!(metadata.kind(), value.kind);
        assert_eq!(metadata.declared_phase(), value.phase);
        assert_eq!(metadata.mode(), value.mode);
        assert_eq!(metadata.target(), expected);
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        let owned = plan.build(charge, visits).unwrap();
        assert_eq!(
            metadata,
            allocation::fail_after(0, || owned.checked_declaration())
        );
        assert_eq!(metadata.definition_stamp(), owned.definition_stamp());
    }
    let original = Declaration::new(Principal::Actor(ISSUER), base, limits().declaration)
        .unwrap()
        .checked_declaration();
    let renamed = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            target: TargetDeclaration::WholeWorkSlot {
                index: 12,
                name: "renamed",
            },
            ..base
        },
        limits().declaration,
    )
    .unwrap()
    .checked_declaration();
    assert_eq!(renamed.binding(), original.binding());
    assert_eq!(renamed.target(), original.target());
    assert_ne!(renamed, original);
    let changed_handler = HandlerRef {
        version: ContentHash([99; 32]),
        ..HANDLER
    };
    let changed_rows = [HandlerPolicy {
        handler: &changed_handler,
        ..ROWS[0]
    }];
    let Program::Programmatic { check, .. } = base.program else {
        panic!("programmatic fixture");
    };
    let changed = Declaration::new(
        Principal::Actor(ISSUER),
        DeclarationSpec {
            program: Program::Programmatic {
                check: PhasePolicy {
                    handlers: &changed_rows,
                    ..check
                },
                quality: None,
            },
            ..base
        },
        limits().declaration,
    )
    .unwrap()
    .checked_declaration();
    assert_eq!(changed.binding(), original.binding());
    assert_eq!(changed.target(), original.target());
    assert_ne!(changed.definition_stamp(), original.definition_stamp());
}

#[derive(Debug, Clone, Copy, Default)]
enum Change {
    #[default]
    None,
    Id,
    Claim,
    Name,
    Description,
    Version,
    Contributor,
    ZeroProof,
    Missing,
    Extra,
}
#[derive(Debug, Default)]
struct Source {
    change: Cell<Change>,
    during_handler: Cell<bool>,
    fields_reads: Cell<usize>,
    handler_factories: Cell<usize>,
    contributor_factories: Cell<usize>,
}
impl Source {
    fn reset_counts(&self) {
        self.fields_reads.set(0);
        self.handler_factories.set(0);
        self.contributor_factories.set(0);
    }
}
struct Handlers<'a> {
    source: &'a Source,
    phase: PolicyPhase,
    index: usize,
}
impl Iterator for Handlers<'_> {
    type Item = Result<HandlerValue, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.phase == PolicyPhase::Quality {
            return None;
        }
        if self.source.during_handler.replace(false) {
            self.source.change.set(Change::Version);
        }
        let index = self.index;
        self.index += 1;
        let change = self.source.change.get();
        if matches!(change, Change::Missing) {
            return None;
        }
        if index == 1 && matches!(change, Change::Extra) {
            return Some(Ok(ROWS[0].into()));
        }
        let mut value: HandlerValue = (*ROWS.get(index)?).into();
        match change {
            Change::Version => value.version = ContentHash([99; 32]),
            Change::ZeroProof => value.proof_schema = ContentHash([0; 32]),
            _ => {}
        }
        Some(Ok(value))
    }
}
impl PolicySource for Source {
    type Handlers<'s>
        = Handlers<'s>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        self.handler_factories.set(self.handler_factories.get() + 1);
        Handlers {
            source: self,
            phase,
            index: 0,
        }
    }
}
impl authored::ValidationSource<'static> for Source {
    type Contributors<'s>
        = std::iter::Once<Result<ParticipantId, ContractError>>
    where
        Self: 's;
    fn fields(&self) -> authored::ValidationFields<'static> {
        self.fields_reads.set(self.fields_reads.get() + 1);
        let mut fields = authored::ValidationSource::fields(&spec());
        match self.change.get() {
            Change::Id => fields.id = ValidationId([99; 16]),
            Change::Claim => fields.claim = ClaimId([99; 16]),
            Change::Name => {
                fields.target = TargetDeclaration::WholeWorkSlot {
                    index: 12,
                    name: "renamed",
                }
            }
            Change::Description => fields.description = "Inspect the actual evidencf.",
            _ => {}
        }
        fields
    }
    fn contributor_count(&self) -> usize {
        1
    }
    fn contributors(&self) -> Self::Contributors<'_> {
        self.contributor_factories
            .set(self.contributor_factories.get() + 1);
        std::iter::once(Ok(if matches!(self.change.get(), Change::Contributor) {
            ParticipantId([99; 16])
        } else {
            CONTRIBUTORS[0]
        }))
    }
}

#[test]
fn authored_metadata_reinspection_has_exact_visits_no_allocation_and_the_final_derived_stamp() {
    let metadata = {
        let source = Source::default();
        let plan = authored::ValidationDescriptor::prepare_source(
            Principal::Actor(ISSUER),
            &source,
            limits(),
            usize::MAX,
        )
        .unwrap();
        let quote = plan.checked_declaration_visits();
        assert!(quote > plan.inspection_visits());
        source.reset_counts();
        assert!(matches!(
            allocation::fail_after(0, || plan.checked_declaration(quote - 1)),
            Err(ContractError::Capacity)
        ));
        assert_eq!(source.fields_reads.get(), 0);
        assert_eq!(source.handler_factories.get(), 0);
        assert_eq!(source.contributor_factories.get(), 0);
        let (metadata, used) =
            allocation::fail_after(0, || plan.checked_declaration(quote)).unwrap();
        assert_eq!(used, quote);
        assert_eq!(source.fields_reads.get(), 1);
        assert_eq!(source.handler_factories.get(), 2);
        assert_eq!(source.contributor_factories.get(), 1);
        assert_eq!(metadata.binding().content, plan.content_hash());
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        let descriptor = plan.build(charge, visits).unwrap();
        assert_eq!(metadata, descriptor.declaration().checked_declaration());
        assert_eq!(
            metadata.definition_stamp(),
            descriptor.declaration().definition_stamp()
        );
        metadata
    };
    // The metadata remains usable after the source, plan and owned body drop.
    let copied = metadata;
    assert_eq!(copied, metadata);
    assert_eq!(copied.target(), CheckedDeclarationTarget::Slot(12));
}

#[test]
fn authored_metadata_refuses_drift_and_invalid_values_in_the_same_stamped_pass() {
    let source = Source::default();
    let plan = authored::ValidationDescriptor::prepare_source(
        Principal::Actor(ISSUER),
        &source,
        limits(),
        usize::MAX,
    )
    .unwrap();
    let quote = plan.checked_declaration_visits();
    let expected = plan.checked_declaration(quote).unwrap().0;
    for change in [
        Change::Id,
        Change::Claim,
        Change::Name,
        Change::Description,
        Change::Version,
        Change::Contributor,
    ] {
        source.change.set(change);
        assert!(
            matches!(
                allocation::fail_after(0, || plan.checked_declaration(quote)),
                Err(ContractError::ContentConflict)
            ),
            "{change:?}"
        );
        source.change.set(Change::None);
        assert_eq!(plan.checked_declaration(quote).unwrap().0, expected);
    }
    for change in [Change::ZeroProof, Change::Missing, Change::Extra] {
        source.change.set(change);
        assert!(
            matches!(
                allocation::fail_after(0, || plan.checked_declaration(quote)),
                Err(ContractError::InvalidPolicy)
            ),
            "{change:?}"
        );
        source.change.set(Change::None);
    }
    source.during_handler.set(true);
    assert!(matches!(
        allocation::fail_after(0, || plan.checked_declaration(quote)),
        Err(ContractError::ContentConflict)
    ));
    source.change.set(Change::None);
    assert_eq!(plan.checked_declaration(quote).unwrap().0, expected);
}
