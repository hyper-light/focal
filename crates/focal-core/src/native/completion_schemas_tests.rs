use super::*;
use crate::native::report_tests as fixture;
use focal_evidence::{BuiltinNativeSchemas, BuiltinSchemaError};
use focal_model::{
    ClaimId, Deadline, HandlerRef, TimerId, ValidationKind, ValidationMode, ValidationPhase,
    ValidatorId, lifecycle::Principal,
};
use std::cell::Cell;

fn hash(value: u8) -> ContentHash {
    ContentHash([value; 32])
}

struct Registry {
    maximum: Cell<usize>,
    per_schema: Cell<bool>,
    missing: Cell<Option<ContentHash>>,
    queries: Cell<usize>,
    verifications: Cell<usize>,
}
impl Registry {
    fn new(maximum: usize) -> Self {
        Self {
            maximum: Cell::new(maximum),
            per_schema: Cell::new(false),
            missing: Cell::new(None),
            queries: Cell::new(0),
            verifications: Cell::new(0),
        }
    }
}
impl NativeSchemaVerifier for Registry {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
        self.queries.set(self.queries.get() + 1);
        if self.missing.get() == Some(schema) {
            Err(BuiltinSchemaError::Unsupported)
        } else {
            Ok(self.maximum.get()
                + if self.per_schema.get() {
                    usize::from(schema.0[0])
                } else {
                    0
                })
        }
    }
    fn verify(&self, _schema: ContentHash, _bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
        self.verifications.set(self.verifications.get() + 1);
        Ok(())
    }
}

fn declaration(
    schemas: &[(ContentHash, ContentHash)],
    quality: Option<(ContentHash, ContentHash)>,
    attempts: u32,
) -> validation::Declaration {
    let handler = HandlerRef {
        id: ValidatorId::from_u128(801),
        version: hash(81),
        agentic: false,
    };
    let agent = HandlerRef {
        id: ValidatorId::from_u128(802),
        version: hash(82),
        agentic: true,
    };
    let handlers: Vec<_> = schemas
        .iter()
        .map(
            |&(proof_schema, diagnostic_schema)| validation::HandlerPolicy {
                handler: &handler,
                attempts,
                proof_schema,
                diagnostic_schema,
            },
        )
        .collect();
    let quality_steps: Vec<_> = quality
        .into_iter()
        .map(
            |(proof_schema, diagnostic_schema)| validation::HandlerPolicy {
                handler: &agent,
                attempts: 1,
                proof_schema,
                diagnostic_schema,
            },
        )
        .collect();
    validation::Declaration::new(
        Principal::Actor(fixture::ISSUER),
        validation::DeclarationSpec {
            binding: fixture::binding(101),
            claim: ClaimId::from_u128(1),
            issuer: fixture::ISSUER,
            declaration_index: 1,
            kind: ValidationKind::Test,
            phase: ValidationPhase::Admission,
            mode: ValidationMode::Observe,
            target: validation::TargetDeclaration::Admission,
            program: validation::Program::Programmatic {
                check: validation::PhasePolicy {
                    evaluator: fixture::EVALUATOR,
                    definition: hash(83),
                    handlers: &handlers,
                    required_policy: None,
                },
                quality: quality.map(|_| validation::PhasePolicy {
                    evaluator: fixture::QUALITY,
                    definition: hash(84),
                    handlers: &quality_steps,
                    required_policy: None,
                }),
            },
            deadline: Deadline {
                timer: TimerId::from_u128(803),
                generation: 1,
                at: 1000,
            },
        },
        validation::Limits {
            handlers: 16,
            attempts: u32::MAX,
            slot_bytes: 64,
        },
    )
    .unwrap()
}

#[test]
fn full_fallback_quality_schema_set_is_sorted_unique_and_keeps_spare_capacity_charged() {
    let declaration = declaration(
        &[(hash(4), hash(1)), (hash(2), hash(1))],
        Some((hash(3), hash(1))),
        1,
    );
    let registry = Registry::new(512);
    registry.per_schema.set(true);
    let count = declaration.evidence_schemas().count();
    assert_eq!(count, 6);
    let bytes = crate::native::prepare::array::<NativeVerificationBudget>(count).unwrap();
    let source = MemoryBudget::new(bytes, 0).unwrap();
    let set = SchemaSet::new(&declaration, &source, &registry, count).unwrap();
    assert_eq!(
        set.budgets
            .iter()
            .map(|quote| quote.schema())
            .collect::<Vec<_>>(),
        vec![hash(1), hash(2), hash(3), hash(4)]
    );
    assert_eq!(registry.queries.get(), 2 * count);
    assert_eq!(registry.verifications.get(), 0);
    assert_eq!(set.budgets.capacity(), count);
    assert_eq!(set.retained_bytes(), bytes);
    assert_eq!(source.stats().used, bytes);
    assert_eq!(source.stats().ordinary_used, bytes);
    assert_eq!(source.stats().by_kind[BudgetKind::Index as usize], bytes);
    let quote = NativeVerificationBudget::for_schema(hash(4), &registry).unwrap();
    assert_eq!(set.workspace_bytes(), quote.peak_bytes());
    assert_eq!(set.custody_bytes(), quote.retained_bytes());
    for schema in [hash(1), hash(2), hash(3), hash(4)] {
        assert_eq!(set.budget_for(schema, &registry).unwrap().schema(), schema);
    }
    assert_eq!(source.stats().used, bytes);
    drop(set);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn lookup_rechecks_pinned_limits_and_rejects_undeclared_hash_without_registry_work() {
    let declaration = declaration(&[(hash(1), hash(2))], None, 1);
    let source = MemoryBudget::new(4096, 0).unwrap();
    let registry = Registry::new(64);
    let set = SchemaSet::new(&declaration, &source, &registry, 2).unwrap();
    let retained = source.stats();
    let queries = registry.queries.get();
    assert!(matches!(
        set.budget_for(hash(9), &registry),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    assert_eq!(registry.queries.get(), queries);
    for maximum in [63, 65] {
        registry.maximum.set(maximum);
        assert!(matches!(
            set.budget_for(hash(1), &registry),
            Err(NativeError::Evidence(
                NativeEvidenceError::VerificationBudgetChanged
            ))
        ));
        assert_eq!(source.stats(), retained);
    }
    registry.maximum.set(64);
    registry.missing.set(Some(hash(2)));
    assert!(matches!(
        set.budget_for(hash(2), &registry),
        Err(NativeError::Evidence(NativeEvidenceError::Schema(
            BuiltinSchemaError::Unsupported
        )))
    ));
    registry.missing.set(None);
    assert_eq!(
        set.budget_for(hash(2), &registry).unwrap().maximum_bytes(),
        64
    );
    assert_eq!(registry.verifications.get(), 0);
    assert_eq!(source.stats(), retained);
}

#[test]
fn invalid_contracts_refuse_before_seeking_schema_buffer_memory() {
    let unfunded = MemoryBudget::new(1, 1).unwrap();
    let declaration = declaration(&[(hash(1), hash(2))], None, 1);
    assert!(matches!(
        SchemaSet::new(&declaration, &unfunded, &BuiltinNativeSchemas, 2),
        Err(NativeError::Evidence(NativeEvidenceError::Schema(
            BuiltinSchemaError::Unsupported
        )))
    ));
    let registry = Registry::new(1024 * 1024 + 1);
    assert!(matches!(
        SchemaSet::new(&declaration, &unfunded, &registry, 2),
        Err(NativeError::Evidence(NativeEvidenceError::Content(
            focal_evidence::ContentError::Capacity
        )))
    ));
    let zero = self::declaration(&[(hash(0), hash(1))], None, 1);
    assert!(matches!(
        SchemaSet::new(&zero, &unfunded, &registry, 2),
        Err(NativeError::Evidence(NativeEvidenceError::Contract(
            ContractError::InvalidManifest
        )))
    ));
    assert_eq!(unfunded.stats().used, 0);
    assert_eq!(registry.verifications.get(), 0);
}

#[test]
fn delivery_and_visit_overflow_refuse_without_expanding_attempt_counts() {
    let source = MemoryBudget::new(4096, 0).unwrap();
    let registry = Registry::new(0);
    let input = fixture::creation(1, 1, &[], None);
    let crate::native::NativeCommand::Create { declarations, .. } = input.command else {
        panic!("creation fixture");
    };
    assert!(matches!(
        SchemaSet::new(&declarations[0], &source, &registry, 0),
        Err(NativeError::Contract(ContractError::InvalidPolicy))
    ));
    let declaration = declaration(&[(hash(1), hash(2))], None, u32::MAX);
    assert!(matches!(
        SchemaSet::new(&declaration, &source, &registry, 1),
        Err(NativeError::Capacity("completion schema visits"))
    ));
    assert_eq!(source.stats().used, 0);
    assert_eq!(registry.queries.get(), 1);
    let set = SchemaSet::new(&declaration, &source, &registry, 2).unwrap();
    assert_eq!(set.budgets.len(), 2);
    assert_eq!(
        set.budget_for(hash(1), &registry).unwrap().maximum_bytes(),
        0
    );
    assert!(set.workspace_bytes() > set.custody_bytes());
}

#[test]
fn allocation_failure_and_inconsistent_duplicate_contracts_refund_the_complete_buffer() {
    let declaration = declaration(&[(hash(1), hash(1))], None, 1);
    let bytes = crate::native::prepare::array::<NativeVerificationBudget>(2).unwrap();
    let insufficient = MemoryBudget::new(bytes - 1, 0).unwrap();
    assert!(matches!(
        SchemaSet::new(&declaration, &insufficient, &Registry::new(32), 2),
        Err(NativeError::Memory(MemoryError::Capacity { .. }))
    ));
    assert_eq!(insufficient.stats().used, 0);
    struct Inconsistent(Cell<usize>);
    impl NativeSchemaVerifier for Inconsistent {
        fn maximum_bytes(&self, _schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
            let count = self.0.get() + 1;
            self.0.set(count);
            Ok(if count == 4 { 64 } else { 32 })
        }
        fn verify(&self, _schema: ContentHash, _bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
            panic!("schema capture must not parse payloads");
        }
    }
    let exact = MemoryBudget::new(bytes, 0).unwrap();
    let registry = Inconsistent(Cell::new(0));
    assert!(matches!(
        SchemaSet::new(&declaration, &exact, &registry, 2),
        Err(NativeError::Evidence(
            NativeEvidenceError::VerificationBudgetChanged
        ))
    ));
    assert_eq!(registry.0.get(), 4);
    assert_eq!(exact.stats().used, 0);
}

#[test]
fn ordinary_schema_ownership_uses_held_credit_and_outlives_the_elastic_controller() {
    let root = MemoryBudget::new(1_000_000, 100_000).unwrap();
    let declaration = declaration(&[(hash(1), hash(2))], None, 1);
    let bytes = crate::native::prepare::array::<NativeVerificationBudget>(2).unwrap();
    let pool = root
        .elastic_funded_child(BudgetLane::Ordinary, bytes, bytes)
        .unwrap();
    let backing = root.stats();
    let pressure = root
        .reserve(
            BudgetKind::Query,
            BudgetLane::Completion,
            root.limit() - backing.used,
        )
        .unwrap();
    let registry = Registry::new(32);
    let set = SchemaSet::new(&declaration, pool.budget(), &registry, 2).unwrap();
    assert_eq!(root.stats().used, root.limit());
    assert_eq!(pool.available(), 0);
    set.budget_for(hash(1), &registry).unwrap();
    drop(pool);
    assert_eq!(root.stats().used, root.limit());
    drop(set);
    assert_eq!(root.stats().used, pressure.bytes());
    drop(pressure);
    assert_eq!(root.stats().used, 0);
    let completion = root
        .elastic_funded_child(BudgetLane::Completion, bytes, bytes)
        .unwrap();
    let before = root.stats();
    assert!(SchemaSet::new(&declaration, completion.budget(), &registry, 2).is_err());
    assert_eq!(root.stats(), before);
    assert_eq!(completion.available(), bytes);
}
