use super::*;
use crate::lifecycle::validation::tests as fixture;
use std::cell::Cell;

#[derive(Debug)]
struct Stream<'a> {
    fields: Cell<DeclarationFields<'a>>,
    check: Vec<Result<HandlerValue, ContractError>>,
    quality: Vec<Result<HandlerValue, ContractError>>,
    replacement: Cell<Option<HandlerValue>>,
    truncate: Cell<Option<usize>>,
    extra: Cell<bool>,
    error: Cell<bool>,
    factories: Cell<usize>,
    reads: Cell<usize>,
}
impl<'a> Stream<'a> {
    fn new(spec: DeclarationSpec<'a>) -> Self {
        Self {
            fields: Cell::new(spec.fields()),
            check: spec.handlers(PolicyPhase::Check).collect(),
            quality: spec.handlers(PolicyPhase::Quality).collect(),
            replacement: Cell::new(None),
            truncate: Cell::new(None),
            extra: Cell::new(false),
            error: Cell::new(false),
            factories: Cell::new(0),
            reads: Cell::new(0),
        }
    }
    fn prepare(&self, visits: usize) -> Result<DeclarationSourcePlan<'_, 'a, Self>, ContractError> {
        Declaration::prepare_source(
            Principal::Actor(fixture::ISSUER),
            self,
            fixture::limits(),
            visits,
        )
    }
}
struct Values<'s, 'a> {
    source: &'s Stream<'a>,
    phase: PolicyPhase,
    position: usize,
}
impl Iterator for Values<'_, '_> {
    type Item = Result<HandlerValue, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.source.reads.set(self.source.reads.get() + 1);
        let position = self.position;
        self.position += 1;
        let values = match self.phase {
            PolicyPhase::Check => &self.source.check,
            PolicyPhase::Quality => &self.source.quality,
        };
        if self.phase == PolicyPhase::Check {
            if self
                .source
                .truncate
                .get()
                .is_some_and(|count| position >= count)
            {
                return None;
            }
            if position == 0 {
                if self.source.error.get() {
                    return Some(Err(ContractError::MissingEvidence));
                }
                if let Some(value) = self.source.replacement.get() {
                    return Some(Ok(value));
                }
            }
            if position == values.len() && self.source.extra.get() {
                return values.first().copied();
            }
        }
        values.get(position).copied()
    }
}
impl<'a> PolicySource for Stream<'a> {
    type Handlers<'s>
        = Values<'s, 'a>
    where
        Self: 's;
    fn handlers(&self, phase: PolicyPhase) -> Self::Handlers<'_> {
        self.factories.set(self.factories.get() + 1);
        Values {
            source: self,
            phase,
            position: 0,
        }
    }
}
impl<'a> DeclarationSource<'a> for Stream<'a> {
    fn fields(&self) -> DeclarationFields<'a> {
        self.fields.get()
    }
}

// Independent frozen native stamp preimage. This deliberately does not call the
// new policy visitor, so parity cannot succeed by sharing an omitted field.
fn old_intent(spec: DeclarationSpec<'_>) -> ContentHash {
    fn field(hash: &mut blake3::Hasher, value: &[u8]) {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value);
    }
    fn phase(hash: &mut blake3::Hasher, value: PhasePolicy<'_>) {
        field(hash, &value.evaluator.0);
        field(hash, &value.definition.0);
        match value.required_policy {
            None => field(hash, b"no-policy"),
            Some(pin) => {
                field(hash, b"policy");
                field(hash, &pin.0);
            }
        }
        field(hash, &(value.handlers.len() as u64).to_be_bytes());
        for step in value.handlers {
            field(hash, &step.handler.id.0);
            field(hash, &step.handler.version.0);
            field(hash, &[u8::from(step.handler.agentic)]);
            field(hash, &step.attempts.to_be_bytes());
            field(hash, &step.proof_schema.0);
            field(hash, &step.diagnostic_schema.0);
        }
    }
    let mut hash = blake3::Hasher::new_derive_key("focal native validation definition binding");
    for value in [
        spec.binding.ledger.tenant.0,
        spec.binding.ledger.session.0,
        spec.binding.object.0,
    ] {
        field(&mut hash, &value);
    }
    field(&mut hash, &spec.binding.content.0);
    field(&mut hash, &spec.binding.revision.0.to_be_bytes());
    field(&mut hash, &spec.claim.0);
    field(&mut hash, &spec.issuer.0);
    field(&mut hash, &spec.declaration_index.to_be_bytes());
    field(&mut hash, &spec.kind.code().to_be_bytes());
    field(&mut hash, &spec.phase.code().to_be_bytes());
    field(&mut hash, &spec.mode.code().to_be_bytes());
    field(&mut hash, &spec.deadline.timer.0);
    field(&mut hash, &spec.deadline.generation.to_be_bytes());
    field(&mut hash, &spec.deadline.at.to_be_bytes());
    match spec.target {
        TargetDeclaration::WholeWorkSlot { index, name } => {
            field(&mut hash, b"whole-work-slot");
            field(&mut hash, &index.to_be_bytes());
            field(&mut hash, name.as_bytes());
        }
        TargetDeclaration::Delivery => field(&mut hash, b"delivery"),
        TargetDeclaration::Admission => field(&mut hash, b"admission"),
        TargetDeclaration::Increment => field(&mut hash, b"increment"),
    }
    let attempts = match spec.program {
        Program::Delivery => {
            field(&mut hash, b"delivery");
            0
        }
        Program::Programmatic { check, quality } => {
            field(&mut hash, b"programmatic");
            phase(&mut hash, check);
            match quality {
                Some(value) => {
                    field(&mut hash, b"quality");
                    phase(&mut hash, value);
                }
                None => field(&mut hash, b"no-quality"),
            }
            check
                .handlers
                .iter()
                .chain(quality.into_iter().flat_map(|p| p.handlers))
                .map(|v| v.attempts)
                .sum::<u32>()
        }
        Program::Agentic { check } => {
            field(&mut hash, b"agentic");
            phase(&mut hash, check);
            check.handlers.iter().map(|v| v.attempts).sum::<u32>()
        }
    };
    let mut intent = blake3::Hasher::new_derive_key("focal/native/validation-intent/1");
    intent.update(hash.finalize().as_bytes());
    intent.update(&attempts.to_be_bytes());
    ContentHash(*intent.finalize().as_bytes())
}

#[test]
fn every_program_target_and_mode_preserves_frozen_native_identity_without_scratch() {
    let Program::Programmatic { check, quality } = fixture::programmatic(true) else {
        panic!()
    };
    let check = PhasePolicy {
        required_policy: Some(ContentHash([91; 32])),
        ..check
    };
    let quality = PhasePolicy {
        required_policy: Some(ContentHash([92; 32])),
        ..quality.unwrap()
    };
    let programs = [
        Program::Programmatic {
            check,
            quality: Some(quality),
        },
        Program::Programmatic {
            check,
            quality: None,
        },
        Program::Agentic { check: quality },
    ];
    for program in programs {
        for mode in [ValidationMode::Required, ValidationMode::Observe] {
            for (target, phase) in [
                (
                    TargetDeclaration::WholeWorkSlot {
                        index: 9,
                        name: "résultat",
                    },
                    ValidationPhase::WholeWork,
                ),
                (TargetDeclaration::Admission, ValidationPhase::Admission),
                (TargetDeclaration::Increment, ValidationPhase::Increment),
            ] {
                let spec = DeclarationSpec {
                    target,
                    phase,
                    ..fixture::specification(mode, program)
                };
                parity(spec);
            }
        }
    }
    parity(DeclarationSpec {
        kind: ValidationKind::Receipt,
        target: TargetDeclaration::Delivery,
        ..fixture::specification(ValidationMode::Required, Program::Delivery)
    });
}
fn parity(spec: DeclarationSpec<'_>) {
    let source = Stream::new(spec);
    let plan = bytes::fail_after(0, || source.prepare(usize::MAX).unwrap());
    let identity = old_intent(spec);
    assert_eq!(plan.intent_fingerprint(), identity);
    assert_eq!(source.factories.get(), 2);
    let charge = plan.construction_charge();
    let allocations = plan.construction_heap_allocations();
    let visits = plan.build_visits();
    let built = bytes::fail_after(allocations, || plan.build(charge, visits).unwrap());
    assert_eq!(
        source.factories.get(),
        4,
        "owned validation must not reread raw streams"
    );
    assert_eq!(built.intent_fingerprint(), identity);
    assert_eq!(built.binding(), spec.binding);
    assert_eq!(built.target(), spec.target);
    assert_eq!(built.deadline(), spec.deadline);
    assert_eq!(built.retained_bytes().unwrap(), charge);
    let slice = Declaration::prepare(Principal::Actor(spec.issuer), spec, fixture::limits())
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(built.definition_stamp(), slice.definition_stamp());
    let borrowed = &built;
    let copied = Declaration::prepare_source(
        Principal::Actor(spec.issuer),
        &borrowed,
        fixture::limits(),
        usize::MAX,
    )
    .unwrap();
    assert_eq!(copied.intent_fingerprint(), identity);
}

#[test]
fn exact_visit_and_byte_quotes_refuse_before_allocation_and_retry_exactly() {
    let source = Stream::new(fixture::specification(
        ValidationMode::Required,
        fixture::programmatic(true),
    ));
    let plan = source.prepare(usize::MAX).unwrap();
    let inspection = plan.inspection_visits();
    let construction = plan.construction_charge();
    let build = plan.build_visits();
    let identity = plan.intent_fingerprint();
    assert!(matches!(
        source.prepare(inspection - 1),
        Err(ContractError::Capacity)
    ));
    assert_eq!(
        source.prepare(inspection).unwrap().inspection_visits(),
        inspection
    );
    for (bytes_limit, visits) in [(construction - 1, build), (construction, build - 1)] {
        bytes::fail_after(3, || {
            assert!(matches!(
                source
                    .prepare(inspection)
                    .unwrap()
                    .build(bytes_limit, visits),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(3));
        });
    }
    for allowed in 0..3 {
        bytes::fail_after(allowed, || {
            assert!(matches!(
                source
                    .prepare(inspection)
                    .unwrap()
                    .build(construction, build),
                Err(ContractError::Capacity)
            ));
            assert_eq!(bytes::remaining_allocations(), Some(0));
        });
        let retry = source
            .prepare(inspection)
            .unwrap()
            .build(construction, build)
            .unwrap();
        assert_eq!(retry.intent_fingerprint(), identity);
    }
}

#[test]
fn source_cardinality_errors_and_invalid_actual_handlers_cannot_supply_a_plan() {
    let source = Stream::new(fixture::specification(
        ValidationMode::Required,
        fixture::programmatic(false),
    ));
    source.truncate.set(Some(1));
    assert!(matches!(
        source.prepare(usize::MAX),
        Err(ContractError::InvalidPolicy)
    ));
    source.truncate.set(None);
    source.extra.set(true);
    assert!(matches!(
        source.prepare(usize::MAX),
        Err(ContractError::InvalidPolicy)
    ));
    source.extra.set(false);
    source.error.set(true);
    assert!(matches!(
        source.prepare(usize::MAX),
        Err(ContractError::MissingEvidence)
    ));
    source.error.set(false);
    let original = source.check[0].unwrap();
    for value in [
        HandlerValue {
            attempts: 0,
            ..original
        },
        HandlerValue {
            agentic: true,
            ..original
        },
        HandlerValue {
            id: ValidatorId::from_u128(0),
            ..original
        },
    ] {
        source.replacement.set(Some(value));
        assert!(matches!(
            source.prepare(usize::MAX),
            Err(ContractError::InvalidPolicy)
        ));
    }
    source.replacement.set(None);
    let mut hidden = Stream::new(fixture::specification(
        ValidationMode::Required,
        fixture::programmatic(false),
    ));
    hidden.quality.push(Ok(HandlerValue {
        agentic: true,
        ..original
    }));
    assert!(matches!(
        hidden.prepare(usize::MAX),
        Err(ContractError::InvalidPolicy)
    ));
}

#[test]
fn changed_valid_body_is_rejected_and_failed_owned_candidate_can_be_retried() {
    let source = Stream::new(fixture::specification(
        ValidationMode::Required,
        fixture::programmatic(true),
    ));
    let prepared = source.prepare(usize::MAX).unwrap();
    let charge = prepared.construction_charge();
    let visits = prepared.build_visits();
    let original = source.check[0].unwrap();
    source.replacement.set(Some(HandlerValue {
        diagnostic_schema: ContentHash([99; 32]),
        ..original
    }));
    assert!(matches!(
        prepared.build(charge, visits),
        Err(ContractError::ContentConflict)
    ));
    source.replacement.set(None);
    let prepared = source.prepare(usize::MAX).unwrap();
    let fields = source.fields.get();
    source.fields.set(DeclarationFields {
        deadline: Deadline {
            at: fields.deadline.at + 1,
            ..fields.deadline
        },
        ..fields
    });
    assert!(matches!(
        prepared.build(charge, visits),
        Err(ContractError::ContentConflict)
    ));
    source.fields.set(fields);
    let prepared = source.prepare(usize::MAX).unwrap();
    source.extra.set(true);
    assert!(matches!(
        prepared.build(charge, visits),
        Err(ContractError::InvalidPolicy)
    ));
    source.extra.set(false);
    let retry = source
        .prepare(usize::MAX)
        .unwrap()
        .build(charge, visits)
        .unwrap();
    assert_eq!(
        retry.intent_fingerprint(),
        old_intent(fixture::specification(
            ValidationMode::Required,
            fixture::programmatic(true)
        ))
    );
}

#[test]
fn legacy_external_zero_pins_remain_unchanged_and_counts_refuse_before_iteration() {
    let source = Stream::new(fixture::specification(
        ValidationMode::Required,
        fixture::programmatic(false),
    ));
    let value = source.check[0].unwrap();
    source.replacement.set(Some(HandlerValue {
        version: ContentHash([0; 32]),
        proof_schema: ContentHash([0; 32]),
        diagnostic_schema: ContentHash([0; 32]),
        ..value
    }));
    let fields = source.fields.get();
    let ProgramFields::Programmatic { check, quality } = fields.program else {
        panic!()
    };
    source.fields.set(DeclarationFields {
        program: ProgramFields::Programmatic {
            check: PhaseFields {
                definition: ContentHash([0; 32]),
                required_policy: Some(ContentHash([0; 32])),
                ..check
            },
            quality,
        },
        ..fields
    });
    let plan = source.prepare(usize::MAX).unwrap();
    let charge = plan.construction_charge();
    let visits = plan.build_visits();
    assert!(plan.build(charge, visits).is_ok());
    source.factories.set(0);
    source.fields.set(DeclarationFields {
        program: ProgramFields::Programmatic {
            check: PhaseFields {
                handlers: usize::MAX,
                ..check
            },
            quality,
        },
        ..fields
    });
    assert!(matches!(
        source.prepare(usize::MAX),
        Err(ContractError::Capacity)
    ));
    assert_eq!(source.factories.get(), 0);
}
