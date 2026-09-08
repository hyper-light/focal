use super::*;
use crate::{HandlerRef, SessionId, TenantId, TimerId, ValidatorId};
use std::cell::Cell;
use validation::{HandlerPolicy, HandlerValue, PhasePolicy, Program, TargetDeclaration};

const ISSUER: ParticipantId = ParticipantId([1; 16]);
const HANDLER: HandlerRef = HandlerRef {
    id: ValidatorId([2; 16]),
    version: ContentHash([3; 32]),
    agentic: false,
};
const AGENT: HandlerRef = HandlerRef {
    id: ValidatorId([4; 16]),
    version: ContentHash([5; 32]),
    agentic: true,
};
const CHECK: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &HANDLER,
    attempts: 2,
    proof_schema: ContentHash([6; 32]),
    diagnostic_schema: ContentHash([7; 32]),
}];
const QUALITY: [HandlerPolicy<'static>; 1] = [HandlerPolicy {
    handler: &AGENT,
    attempts: 3,
    proof_schema: ContentHash([8; 32]),
    diagnostic_schema: ContentHash([9; 32]),
}];
const CONTRIBUTORS: [ParticipantId; 2] = [ParticipantId([10; 16]), ParticipantId([11; 16])];

fn spec() -> ValidationSpec<'static> {
    ValidationSpec {
        ledger: LedgerId {
            tenant: TenantId([12; 16]),
            session: SessionId([13; 16]),
        },
        id: ValidationId([14; 16]),
        schema: 1,
        claim: ClaimId([15; 16]),
        issuer: ISSUER,
        declaration_index: 16,
        kind: ValidationKind::Inspection,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        target: TargetDeclaration::WholeWorkSlot {
            index: 17,
            name: "result",
        },
        program: Program::Programmatic {
            check: PhasePolicy {
                evaluator: ParticipantId([18; 16]),
                definition: ContentHash([19; 32]),
                handlers: &CHECK,
                required_policy: Some(ContentHash([20; 32])),
            },
            quality: Some(PhasePolicy {
                evaluator: ParticipantId([21; 16]),
                definition: ContentHash([22; 32]),
                handlers: &QUALITY,
                required_policy: Some(ContentHash([23; 32])),
            }),
        },
        deadline: Deadline {
            timer: TimerId([24; 16]),
            generation: 25,
            at: 26,
        },
        description: "Check the exact result. é",
        quality_bar: Some("Explain the proof."),
        contributed_by: &CONTRIBUTORS,
        policy_revision: 27,
    }
}
fn limits() -> Limits {
    Limits {
        declaration: validation::Limits {
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Change {
    #[default]
    None,
    Id,
    Description,
    Version,
    ZeroVersion,
    ZeroProof,
    ZeroDiagnostic,
    ZeroDefinition,
    ZeroRequired,
    ZeroAttempts,
    WrongAgent,
    MissingHandler,
    ExtraHandler,
    HandlerError,
    Contributor,
    DuplicateContributor,
    ZeroContributor,
    MissingContributor,
    ExtraContributor,
    ContributorError,
}

#[derive(Debug)]
struct Source {
    spec: ValidationSpec<'static>,
    change: Cell<Change>,
    count: Cell<Option<usize>>,
    checks: Cell<usize>,
    contributor_passes: Cell<usize>,
    on_check: Cell<Option<(usize, Change)>>,
    on_contributors: Cell<Option<(usize, Change)>>,
}
impl Source {
    fn new() -> Self {
        Self {
            spec: spec(),
            change: Cell::new(Change::None),
            count: Cell::new(None),
            checks: Cell::new(0),
            contributor_passes: Cell::new(0),
            on_check: Cell::new(None),
            on_contributors: Cell::new(None),
        }
    }
    fn handlers_for(&self, phase: PolicyPhase) -> &'static [HandlerPolicy<'static>] {
        match (self.spec.program, phase) {
            (
                Program::Programmatic { check, .. } | Program::Agentic { check },
                PolicyPhase::Check,
            ) => check.handlers,
            (
                Program::Programmatic {
                    quality: Some(quality),
                    ..
                },
                PolicyPhase::Quality,
            ) => quality.handlers,
            _ => &[],
        }
    }
    fn reset(&self) {
        self.change.set(Change::None);
        self.count.set(None);
        self.checks.set(0);
        self.contributor_passes.set(0);
        self.on_check.set(None);
        self.on_contributors.set(None);
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
        let index = self.index;
        self.index += 1;
        let change = self.source.change.get();
        let rows = self.source.handlers_for(self.phase);
        if self.phase == PolicyPhase::Check {
            if change == Change::MissingHandler && index == 0 {
                return None;
            }
            if change == Change::HandlerError && index == 0 {
                return Some(Err(ContractError::StaleEvaluation));
            }
            if change == Change::ExtraHandler && index == rows.len() {
                return Some(Ok(CHECK[0].into()));
            }
        }
        let mut value: HandlerValue = (*rows.get(index)?).into();
        if self.phase == PolicyPhase::Check {
            match change {
                Change::Version => value.version = ContentHash([99; 32]),
                Change::ZeroVersion => value.version = ContentHash([0; 32]),
                Change::ZeroProof => value.proof_schema = ContentHash([0; 32]),
                Change::ZeroDiagnostic => value.diagnostic_schema = ContentHash([0; 32]),
                Change::ZeroAttempts => value.attempts = 0,
                Change::WrongAgent => value.agentic = true,
                _ => {}
            }
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
        if phase == PolicyPhase::Check {
            let pass = self.checks.get() + 1;
            self.checks.set(pass);
            if let Some((at, change)) = self.on_check.get()
                && pass == at
            {
                self.change.set(change);
            }
        }
        Handlers {
            source: self,
            phase,
            index: 0,
        }
    }
}
struct Contributors<'a> {
    source: &'a Source,
    index: usize,
}
impl Iterator for Contributors<'_> {
    type Item = Result<ParticipantId, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        let change = self.source.change.get();
        if change == Change::MissingContributor && index == 1 {
            return None;
        }
        if change == Change::ContributorError && index == 0 {
            return Some(Err(ContractError::StaleReceipt));
        }
        if change == Change::ExtraContributor && index == self.source.spec.contributed_by.len() {
            return Some(Ok(ParticipantId([12; 16])));
        }
        let mut value = *self.source.spec.contributed_by.get(index)?;
        match (change, index) {
            (Change::Contributor, 1) => value = ParticipantId([12; 16]),
            (Change::DuplicateContributor, 1) => value = CONTRIBUTORS[0],
            (Change::ZeroContributor, 0) => value = ParticipantId([0; 16]),
            _ => {}
        }
        Some(Ok(value))
    }
}
impl ValidationSource<'static> for Source {
    type Contributors<'s>
        = Contributors<'s>
    where
        Self: 's;
    fn fields(&self) -> ValidationFields<'static> {
        let mut fields = self.spec.fields();
        match self.change.get() {
            Change::Id => fields.id = ValidationId([99; 16]),
            Change::Description => fields.description = "Check the exact result. a",
            Change::ZeroDefinition | Change::ZeroRequired => {
                if let ProgramFields::Programmatic { ref mut check, .. } = fields.program {
                    if self.change.get() == Change::ZeroDefinition {
                        check.definition = ContentHash([0; 32]);
                    } else {
                        check.required_policy = Some(ContentHash([0; 32]));
                    }
                }
            }
            _ => {}
        }
        fields
    }
    fn contributor_count(&self) -> usize {
        self.count.get().unwrap_or(self.spec.contributed_by.len())
    }
    fn contributors(&self) -> Self::Contributors<'_> {
        let pass = self.contributor_passes.get() + 1;
        self.contributor_passes.set(pass);
        if let Some((at, change)) = self.on_contributors.get()
            && pass == at
        {
            self.change.set(change);
        }
        Contributors {
            source: self,
            index: 0,
        }
    }
}

fn prepare(source: &Source) -> ValidationSourcePlan<'_, 'static, Source> {
    ValidationDescriptor::prepare_source(Principal::Actor(ISSUER), source, limits(), usize::MAX)
        .unwrap()
}
fn assert_same(left: &ValidationDescriptor, right: &ValidationDescriptor) {
    let left_source = OwnedSource {
        descriptor: left,
        declaration: left.declaration(),
    };
    let right_source = OwnedSource {
        descriptor: right,
        declaration: right.declaration(),
    };
    assert_eq!(left_source.fields(), right_source.fields());
    assert_eq!(left.contributed_by(), right.contributed_by());
    assert_eq!(left.content_hash(), right.content_hash());
    assert_eq!(left.specification_hash(), right.specification_hash());
    assert_eq!(left.intent_fingerprint(), right.intent_fingerprint());
    assert_eq!(
        left.declaration().intent_fingerprint(),
        right.declaration().intent_fingerprint()
    );
    for phase in [PolicyPhase::Check, PolicyPhase::Quality] {
        assert_eq!(
            left_source.handlers(phase).collect::<Vec<_>>(),
            right_source.handlers(phase).collect::<Vec<_>>()
        );
    }
}

#[test]
fn checked_source_matches_slice_identity_and_exact_quotes_without_scratch_allocation() {
    let source = Source::new();
    let original_plan =
        ValidationDescriptor::prepare(Principal::Actor(ISSUER), source.spec, limits()).unwrap();
    let plan = bytes::fail_after(0, || prepare(&source));
    assert_eq!(plan.fields(), source.spec.fields());
    assert_eq!(plan.content_hash(), original_plan.content_hash());
    assert_eq!(
        plan.specification_hash(),
        original_plan.specification_hash()
    );
    assert_eq!(
        plan.intent_fingerprint(),
        original_plan.intent_fingerprint()
    );
    assert_eq!(
        plan.construction_charge(),
        original_plan.construction_charge()
    );
    assert_eq!(
        plan.construction_heap_bytes(),
        original_plan.construction_heap_bytes()
    );
    assert_eq!(plan.construction_heap_allocations(), 6);
    assert_eq!(plan.attempt_bound(), 5);
    let charge = plan.construction_charge();
    let inspection = plan.inspection_visits();
    let visits = plan.build_visits();
    let original = original_plan.build(charge).unwrap();
    bytes::fail_after(0, || {
        assert!(
            ValidationDescriptor::prepare_source(
                Principal::Actor(ISSUER),
                &source,
                limits(),
                inspection
            )
            .is_ok()
        );
        assert!(matches!(
            ValidationDescriptor::prepare_source(
                Principal::Actor(ISSUER),
                &source,
                limits(),
                inspection - 1
            ),
            Err(ContractError::Capacity)
        ));
        assert!(matches!(
            ValidationDescriptor::prepare_source(
                Principal::Actor(ISSUER),
                &source,
                Limits {
                    construction_bytes: charge - 1,
                    ..limits()
                },
                inspection
            ),
            Err(ContractError::Capacity)
        ));
    });
    for (bytes_limit, visits_limit) in [(charge - 1, visits), (charge, visits - 1)] {
        let plan = prepare(&source);
        source.checks.set(0);
        source.contributor_passes.set(0);
        bytes::fail_after(6, || {
            assert!(matches!(
                plan.build(bytes_limit, visits_limit),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(6));
        });
        assert_eq!(source.checks.get(), 0);
        assert_eq!(source.contributor_passes.get(), 0);
    }
    let built = bytes::fail_after(6, || plan.build(charge, visits)).unwrap();
    assert_same(&built, &original);
    assert_eq!(built.retained_bytes().unwrap(), charge);
    assert_ne!(
        built.description().as_ptr(),
        source.spec.description.as_ptr()
    );
    assert_ne!(
        built.contributed_by().as_ptr(),
        source.spec.contributed_by.as_ptr()
    );
    for after in 0..6 {
        let plan = bytes::fail_after(0, || prepare(&source));
        bytes::fail_after(after, || {
            assert!(matches!(
                plan.build(charge, visits),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        let retry = bytes::fail_after(6, || prepare(&source).build(charge, visits)).unwrap();
        assert_same(&retry, &original);
    }
}

#[test]
fn exact_fallible_streams_and_stronger_authored_pins_are_checked_in_one_pass() {
    let source = Source::new();
    for (change, error) in [
        (Change::ZeroVersion, ContractError::InvalidPolicy),
        (Change::ZeroProof, ContractError::InvalidPolicy),
        (Change::ZeroDiagnostic, ContractError::InvalidPolicy),
        (Change::ZeroDefinition, ContractError::InvalidPolicy),
        (Change::ZeroRequired, ContractError::InvalidPolicy),
        (Change::ZeroAttempts, ContractError::InvalidPolicy),
        (Change::WrongAgent, ContractError::InvalidPolicy),
        (Change::MissingHandler, ContractError::InvalidPolicy),
        (Change::ExtraHandler, ContractError::InvalidPolicy),
        (Change::HandlerError, ContractError::StaleEvaluation),
        (Change::DuplicateContributor, ContractError::InvalidPolicy),
        (Change::ZeroContributor, ContractError::InvalidPolicy),
        (Change::MissingContributor, ContractError::InvalidManifest),
        (Change::ExtraContributor, ContractError::InvalidManifest),
        (Change::ContributorError, ContractError::StaleReceipt),
    ] {
        source.change.set(change);
        bytes::fail_after(0, || {
            assert_eq!(
                ValidationDescriptor::prepare_source(
                    Principal::Actor(ISSUER),
                    &source,
                    limits(),
                    usize::MAX
                )
                .unwrap_err(),
                error,
                "{change:?}"
            );
        });
    }
    source.reset();
    for count in [0, 1, 3, usize::MAX] {
        source.count.set(Some(count));
        let error = if count == usize::MAX {
            ContractError::Capacity
        } else {
            ContractError::InvalidManifest
        };
        assert_eq!(
            bytes::fail_after(0, || ValidationDescriptor::prepare_source(
                Principal::Actor(ISSUER),
                &source,
                limits(),
                usize::MAX
            ))
            .unwrap_err(),
            error
        );
    }
}

#[test]
fn changed_scalar_handler_and_contributor_values_cannot_reuse_the_original_quote() {
    let source = Source::new();
    for change in [
        Change::Id,
        Change::Description,
        Change::Version,
        Change::Contributor,
    ] {
        let plan = prepare(&source);
        let intent = plan.intent_fingerprint();
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        source.change.set(change);
        assert!(
            matches!(
                plan.build(charge, visits),
                Err(ContractError::ContentConflict)
            ),
            "{change:?}"
        );
        source.reset();
        let retry = prepare(&source).build(charge, visits).unwrap();
        assert_eq!(retry.intent_fingerprint(), intent);
    }
}

#[test]
fn values_changed_during_copy_are_checked_again_as_an_owned_descriptor() {
    let source = Source::new();
    for (change, error) in [
        (Change::Version, ContractError::ContentConflict),
        (Change::ZeroVersion, ContractError::InvalidPolicy),
        (Change::ZeroProof, ContractError::InvalidPolicy),
        (Change::ZeroDiagnostic, ContractError::InvalidPolicy),
    ] {
        let plan = prepare(&source);
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        source.checks.set(0);
        // The authored inspection sees the original stream. Both subsequent
        // lower declaration passes see a matching changed stream, so the final
        // descriptor check must still bind it to the authored quote and pins.
        source.on_check.set(Some((2, change)));
        assert_eq!(plan.build(charge, visits).unwrap_err(), error, "{change:?}");
        assert_eq!(source.checks.get(), 3);
        source.reset();
        assert!(prepare(&source).build(charge, visits).is_ok());
    }
    for (change, error) in [
        (Change::Contributor, ContractError::ContentConflict),
        (Change::DuplicateContributor, ContractError::InvalidPolicy),
    ] {
        let plan = prepare(&source);
        let charge = plan.construction_charge();
        let visits = plan.build_visits();
        source.contributor_passes.set(0);
        source.on_contributors.set(Some((2, change)));
        assert_eq!(plan.build(charge, visits).unwrap_err(), error);
        assert_eq!(source.contributor_passes.get(), 2);
        source.reset();
        assert!(prepare(&source).build(charge, visits).is_ok());
    }
}
