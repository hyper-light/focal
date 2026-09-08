use super::*;
use crate::native::report_tests as f;
use focal_evidence::NativeLocalCustody;
use focal_model::lifecycle::artifact_descriptor::{ArtifactDescriptor, ContentPointer};

struct Empty;
impl Objects for Empty {
    fn ledger(&self) -> LedgerId {
        f::binding(1).ledger
    }
    fn prefix(&self) -> SessionSeq {
        SessionSeq(1)
    }
    fn get(&self, _key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
        debit(meter, 1)?;
        Ok(None)
    }
    fn claim_dependency(
        &self,
        _id: ClaimId,
        meter: &Meter,
    ) -> Result<ClaimDependency<'_>, NativeError> {
        debit(meter, 1)?;
        Err(NativeError::Capacity("original lookup refusal"))
    }
    fn artifact_origin(
        &self,
        _id: ArtifactId,
        meter: &Meter,
    ) -> Result<ArtifactOrigin, NativeError> {
        debit(meter, 1)?;
        Err(invalid())
    }
}
impl evidence::Custody for Empty {
    fn recover(
        &self,
        _request: RequestKey,
        _descriptor: &ArtifactDescriptor,
        _pointer: ContentPointer,
        _local_revision: u64,
    ) -> Result<NativeLocalCustody, NativeError> {
        Err(invalid())
    }
}
fn limits() -> Limits {
    let declaration = validation::Limits {
        handlers: 16,
        attempts: 16,
        slot_bytes: 1024,
    };
    Limits {
        native: NativeLimits::default(),
        acceptance: aggregation::Limits {
            max_slots: 16,
            max_checks: 16,
            max_results: 32,
            max_updates: 32,
        },
        artifact: artifact_descriptor::Limits {
            kind_bytes: 1024,
            metadata_bytes: 4096,
            inline_bytes: 4096,
            inputs: 16,
            visibility_labels: 16,
            visibility_label_bytes: 1024,
            construction_bytes: 65_536,
        },
        claim: claim_descriptor::Limits {
            description_bytes: 4096,
            relations: 16,
            scopes: 16,
            scope_key_bytes: 1024,
            requirements: 16,
            slots: 16,
            checks: 16,
            construction_bytes: 65_536,
        },
        declaration,
        validation: validation_descriptor::Limits {
            declaration,
            description_bytes: 4096,
            quality_bar_bytes: 4096,
            contributors: 16,
            construction_bytes: 65_536,
        },
        response: ResponseLimits {
            artifacts: 16,
            diagnostics: 16,
            summary_bytes: 4096,
            construction_bytes: 65_536,
        },
        creation_objects: 32,
    }
}
fn wire(key: Key, row: &Row) -> Vec<u8> {
    let mut body = CountingSink::new(usize::MAX, usize::MAX);
    rows::value(&mut body, row, f::binding(1).ledger).unwrap();
    let write = |sink: &mut dyn bytes::Sink| -> Result<(), CodecError> {
        // Sized adapter avoids allocating a body buffer between measurement and
        // enclosing framing; this test intentionally exercises actual writers.
        struct Forward<'a>(&'a mut dyn bytes::Sink);
        impl bytes::Sink for Forward<'_> {
            fn write(&mut self, value: &[u8]) -> Result<(), CodecError> {
                self.0.write(value)
            }
            fn visit(&mut self, visits: usize) -> Result<(), CodecError> {
                self.0.visit(visits)
            }
        }
        let mut sink = Forward(sink);
        write_u8(&mut sink, 1)?;
        fixed::key(&mut sink, key)?;
        write_count(&mut sink, body.len())?;
        rows::value(&mut sink, row, f::binding(1).ledger)
    };
    let mut size = CountingSink::new(usize::MAX, usize::MAX);
    write(&mut size).unwrap();
    let mut value = vec![0; size.len()];
    let mut sink = SliceSink::new(&mut value, size.visits_used());
    write(&mut sink).unwrap();
    sink.finish().unwrap();
    value
}
#[test]
fn fixed_dispatch_preserves_present_empty_link_and_exhausts_one_shared_parse_budget() {
    let key = Key::MonitorLink(ClaimId::from_u128(1), focal_model::MonitorId::from_u128(2));
    let bytes = wire(key, &Row::MonitorLink(None));
    let row = RecordRows::new(&bytes, 1, bytes.len(), usize::MAX)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let budget = MemoryBudget::new(65_536, 0).unwrap();
    let parsing = Meter::new(10_000);
    let source = Meter::new(10_000);
    let model = Meter::new(10_000);
    let lookup = Meter::new(10_000);
    let context = Context {
        objects: &Empty,
        custody: &Empty,
        workspace: &budget,
        workspace_lane: BudgetLane::Ordinary,
        parsing: &parsing,
        source: &source,
        model: &model,
        lookup: &lookup,
        limits: limits(),
    };
    let quote = prepare(&row, &context).unwrap();
    assert_eq!(
        quote,
        Quote {
            heap_bytes: 0,
            workspace_bytes: 0
        }
    );
    let used = 10_000 - parsing.remaining();
    assert!(used > 1);
    with_build(&row, &context, quote, 0, |value, heap| {
        assert!(matches!(value, Row::MonitorLink(None)));
        assert_eq!(heap, 0);
        Ok(())
    })
    .unwrap();
    assert_eq!(10_000 - parsing.remaining(), used * 2);
    assert_eq!(source.remaining(), 10_000);
    assert_eq!(lookup.remaining(), 10_000);
    assert_eq!(budget.stats().used, 0);
    let parsing = Meter::new(used - 1);
    let context = Context {
        parsing: &parsing,
        ..context
    };
    assert!(prepare(&row, &context).is_err());
    assert!(parsing.remaining() < used - 1);
}
#[test]
fn failed_native_parse_keeps_its_original_cause_and_cannot_reset_consumed_work() {
    let meter = Meter::new(8);
    let result: Result<(), NativeError> = parse(&[1, 2], &meter, |cursor| {
        cursor.u8().map_err(evidence::codec)?;
        Err(NativeError::Capacity("precise decoder refusal"))
    });
    assert!(matches!(
        result,
        Err(NativeError::Capacity("precise decoder refusal"))
    ));
    assert_eq!(meter.remaining(), 6);
    assert!(
        parse(&[1, 2], &meter, |cursor| cursor
            .u8()
            .map_err(evidence::codec))
        .is_err()
    );
    assert_eq!(meter.remaining(), 4);
}
#[test]
fn model_adapter_preserves_native_lookup_failure_without_heap_or_borrow_panics() {
    struct Refusal;
    impl Objects for Refusal {
        fn ledger(&self) -> LedgerId {
            f::binding(1).ledger
        }
        fn prefix(&self) -> SessionSeq {
            SessionSeq(1)
        }
        fn get(&self, _key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
            debit(meter, 1)?;
            Err(NativeError::Capacity("precise restored-store refusal"))
        }
        fn claim_dependency(
            &self,
            _id: ClaimId,
            _meter: &Meter,
        ) -> Result<ClaimDependency<'_>, NativeError> {
            Err(invalid())
        }
        fn artifact_origin(
            &self,
            _id: ArtifactId,
            _meter: &Meter,
        ) -> Result<ArtifactOrigin, NativeError> {
            Err(invalid())
        }
    }
    let meter = Meter::new(1024);
    let access = Access::new(&Refusal, &meter);
    let result = read_claim::Objects::declaration(&access, focal_model::ValidationId::from_u128(1));
    assert!(matches!(result, Err(ContractError::Capacity)));
    let result: Result<(), NativeError> = access.finish(Err(ContractError::InvalidManifest.into()));
    assert!(matches!(
        result,
        Err(NativeError::Capacity("precise restored-store refusal"))
    ));
    assert_eq!(meter.remaining(), 1024 - 257);
}

/// A changed body is deliberately unavailable through get until its full row
/// has been built. Immutable claim dependencies must still prefer that body.
struct ClaimOverlay<'a> {
    base: &'a Core<NativeState>,
    changed: Option<(ClaimId, Option<&'a [u8]>)>,
    definitions: Option<&'a Core<NativeState>>,
}
impl Objects for ClaimOverlay<'_> {
    fn ledger(&self) -> LedgerId {
        self.base.state.ledger
    }
    fn prefix(&self) -> SessionSeq {
        self.base.native_sequence()
    }
    fn get(&self, key: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
        debit(meter, 1)?;
        if matches!(self.changed, Some((id, _)) if key == Key::Claim(id)) {
            return Ok(None);
        }
        let source = if matches!(key, Key::Definition(_)) {
            self.definitions.unwrap_or(self.base)
        } else {
            self.base
        };
        Ok(source.state.rows.get(&key))
    }
    fn claim_dependency(
        &self,
        id: ClaimId,
        meter: &Meter,
    ) -> Result<ClaimDependency<'_>, NativeError> {
        debit(meter, 1)?;
        if let Some((changed, body)) = self.changed
            && changed == id
        {
            return body
                .map(ClaimDependency::Raw)
                .ok_or(ContractError::MissingEvidence.into());
        }
        self.base
            .native_claim(id)
            .map(ClaimDependency::Retained)
            .ok_or(ContractError::MissingEvidence.into())
    }
    fn artifact_origin(
        &self,
        _id: ArtifactId,
        meter: &Meter,
    ) -> Result<ArtifactOrigin, NativeError> {
        debit(meter, 1)?;
        Err(invalid())
    }
}
fn encoded(bytes: &[u8]) -> EncodedRow<'_> {
    RecordRows::new(bytes, 1, bytes.len(), usize::MAX)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}

#[test]
fn unchanged_policy_and_issuer_borrow_actual_base_without_workspace_or_reconstruction() {
    let core = f::running(&[(focal_model::ValidationMode::Required, true)]);
    let claim_id = ClaimId::from_u128(1);
    let claim = core.native_claim(claim_id).unwrap();
    let objects = ClaimOverlay {
        base: &core,
        changed: None,
        definitions: None,
    };
    let budget = MemoryBudget::new(1, 0).unwrap();
    let held = budget
        .reserve(BudgetKind::Recovery, BudgetLane::Ordinary, 1)
        .unwrap();
    let parsing = Meter::new(0);
    let source = Meter::new(0);
    let model = Meter::new(0);
    let lookup = Meter::new(10_000);
    let context = Context {
        objects: &objects,
        custody: &Empty,
        workspace: &budget,
        workspace_lane: BudgetLane::Ordinary,
        parsing: &parsing,
        source: &source,
        model: &model,
        lookup: &lookup,
        limits: limits(),
    };
    let access = Access::new(&objects, &lookup);
    with_policy(claim_id, &context, &access, |policy, workspace| {
        assert!(std::ptr::eq(policy, claim.acceptance()));
        assert_eq!(workspace, 0);
        assert_eq!(budget.stats().used, 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(
        claim_issuer(claim_id, &access, &parsing).unwrap(),
        claim.issuer()
    );
    assert_eq!(lookup.remaining(), 10_000 - 2 * 257);
    assert_eq!(parsing.remaining(), 0);
    assert_eq!(source.remaining(), 0);
    assert_eq!(model.remaining(), 0);
    drop(held);
    assert_eq!(budget.stats().used, 0);

    let one_short = Meter::new(256);
    let access = Access::new(&objects, &one_short);
    assert!(with_policy(claim_id, &context, &access, |_, _| Ok(())).is_err());
    assert_eq!(one_short.remaining(), 0);
}

#[test]
fn declaration_dispatch_uses_retained_claim_issuer_without_a_checkpoint_body() {
    let core = f::running(&[(focal_model::ValidationMode::Required, true)]);
    let key = Key::Definition(focal_model::ValidationId::from_u128(101));
    let bytes = wire(key, core.state.rows.get(&key).unwrap());
    let row = encoded(&bytes);
    let objects = ClaimOverlay {
        base: &core,
        changed: None,
        definitions: None,
    };
    let budget = MemoryBudget::new(1, 0).unwrap();
    let held = budget
        .reserve(BudgetKind::Recovery, BudgetLane::Ordinary, 1)
        .unwrap();
    let parsing = Meter::new(10_000_000);
    let source = Meter::new(10_000_000);
    let model = Meter::new(10_000_000);
    let lookup = Meter::new(10_000_000);
    let context = Context {
        objects: &objects,
        custody: &Empty,
        workspace: &budget,
        workspace_lane: BudgetLane::Ordinary,
        parsing: &parsing,
        source: &source,
        model: &model,
        lookup: &lookup,
        limits: limits(),
    };
    let quote = prepare(&row, &context).unwrap();
    assert_eq!(quote.workspace_bytes, 0);
    let retained = MemoryBudget::new(quote.heap_bytes, 0).unwrap();
    let _retained = retained
        .reserve(BudgetKind::Recovery, BudgetLane::Ordinary, quote.heap_bytes)
        .unwrap();
    with_build(&row, &context, quote, quote.heap_bytes, |value, actual| {
        assert!(actual <= quote.heap_bytes);
        assert_eq!(wire(key, &value), bytes);
        assert_eq!(budget.stats().used, 1);
        Ok(())
    })
    .unwrap();
    drop(held);
    assert_eq!(budget.stats().used, 0);
}

#[test]
fn changed_claim_body_precedes_base_and_its_scoped_policy_refunds_workspace() {
    let base = f::running(&[]);
    let changed = f::running(&[(focal_model::ValidationMode::Required, true)]);
    let id = ClaimId::from_u128(1);
    let bytes = wire(
        Key::Claim(id),
        changed.state.rows.get(&Key::Claim(id)).unwrap(),
    );
    let body = encoded(&bytes).body();
    let objects = ClaimOverlay {
        base: &base,
        changed: Some((id, Some(body))),
        definitions: Some(&changed),
    };
    let budget = MemoryBudget::new(65_536, 0).unwrap();
    let parsing = Meter::new(10_000_000);
    let source = Meter::new(10_000_000);
    let model = Meter::new(10_000_000);
    let lookup = Meter::new(10_000_000);
    let context = Context {
        objects: &objects,
        custody: &Empty,
        workspace: &budget,
        workspace_lane: BudgetLane::Ordinary,
        parsing: &parsing,
        source: &source,
        model: &model,
        lookup: &lookup,
        limits: limits(),
    };
    let access = Access::new(&objects, &lookup);
    assert!(objects.get(Key::Claim(id), &lookup).unwrap().is_none());
    with_policy(id, &context, &access, |policy, workspace| {
        assert!(!std::ptr::eq(
            policy,
            base.native_claim(id).unwrap().acceptance()
        ));
        assert_eq!(
            policy.declarations(),
            changed
                .native_claim(id)
                .unwrap()
                .acceptance()
                .declarations()
        );
        assert_ne!(
            policy.declarations(),
            base.native_claim(id).unwrap().acceptance().declarations()
        );
        assert!(workspace > 0);
        assert_eq!(budget.stats().used, workspace);
        Ok(())
    })
    .unwrap();
    assert_eq!(budget.stats().used, 0);
    let result: Result<(), NativeError> = with_policy(id, &context, &access, |_, _| {
        Err(NativeError::Capacity("scoped consumer refusal"))
    });
    assert!(matches!(
        result,
        Err(NativeError::Capacity("scoped consumer refusal"))
    ));
    assert_eq!(budget.stats().used, 0);

    let tiny = MemoryBudget::new(1, 0).unwrap();
    let context = Context {
        workspace: &tiny,
        ..context
    };
    assert!(with_policy(id, &context, &access, |_, _| Ok(())).is_err());
    assert_eq!(tiny.stats().used, 0);
}

#[test]
fn deleted_malformed_or_substituted_claim_dependency_cannot_use_the_old_policy() {
    let core = f::running(&[]);
    let id = ClaimId::from_u128(1);
    let bytes = wire(
        Key::Claim(id),
        core.state.rows.get(&Key::Claim(id)).unwrap(),
    );
    let mut wrong_issuer = encoded(&bytes).body().to_vec();
    // Full binding is 88 bytes; the next 16 bytes are the claimant. Its
    // acceptance issuer remains unchanged, so this raw body must be rejected.
    wrong_issuer[88..104].copy_from_slice(&f::SUBJECT.0);
    let budget = MemoryBudget::new(65_536, 0).unwrap();
    for body in [None, Some(&[][..]), Some(wrong_issuer.as_slice())] {
        let objects = ClaimOverlay {
            base: &core,
            changed: Some((id, body)),
            definitions: None,
        };
        let parsing = Meter::new(10_000_000);
        let source = Meter::new(10_000_000);
        let model = Meter::new(10_000_000);
        let lookup = Meter::new(10_000_000);
        let context = Context {
            objects: &objects,
            custody: &Empty,
            workspace: &budget,
            workspace_lane: BudgetLane::Ordinary,
            parsing: &parsing,
            source: &source,
            model: &model,
            lookup: &lookup,
            limits: limits(),
        };
        let access = Access::new(&objects, &lookup);
        assert!(with_policy(id, &context, &access, |_, _| Ok(())).is_err());
        assert!(claim_issuer(id, &access, &parsing).is_err());
        assert_eq!(budget.stats().used, 0);
    }
    struct Substitution<'a>(&'a ClaimState);
    impl Objects for Substitution<'_> {
        fn ledger(&self) -> LedgerId {
            self.0.binding().ledger
        }
        fn prefix(&self) -> SessionSeq {
            SessionSeq(1)
        }
        fn get(&self, _: Key, meter: &Meter) -> Result<Option<&Row>, NativeError> {
            debit(meter, 1)?;
            Ok(None)
        }
        fn claim_dependency(
            &self,
            _: ClaimId,
            meter: &Meter,
        ) -> Result<ClaimDependency<'_>, NativeError> {
            debit(meter, 1)?;
            Ok(ClaimDependency::Retained(self.0))
        }
        fn artifact_origin(
            &self,
            _: ArtifactId,
            meter: &Meter,
        ) -> Result<ArtifactOrigin, NativeError> {
            debit(meter, 1)?;
            Err(invalid())
        }
    }
    let objects = Substitution(core.native_claim(id).unwrap());
    let meter = Meter::new(10_000);
    let access = Access::new(&objects, &meter);
    assert!(access.claim_dependency(ClaimId::from_u128(2)).is_err());
}
