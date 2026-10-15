use super::*;
use crate::native::completion_envelope::descriptor_limits;
use crate::native::report_tests as fixture;
use focal_evidence::{BuiltinNativeSchemas, test_report_schema};
use focal_model::lifecycle::{creation::Owner, succession::Lineage};
use focal_model::{Cause, ValidationMode, VerdictValue};

fn source() -> MemoryBudget {
    MemoryBudget::new(usize::MAX / 16, 0).unwrap()
}

fn parent(core: &Core<NativeState>) -> (&ClaimState, &RegistrationSet) {
    let Some(Row::Claim(row)) = core.state.row(&Key::Claim(fixture::key(1).claim)) else {
        panic!("actual claim row")
    };
    (row.claim().unwrap(), row.registrations().unwrap())
}

fn install(
    book: &mut CompletionBook,
    core: &Core<NativeState>,
    source: &MemoryBudget,
    index: u32,
) -> Journal {
    let key = fixture::key(index);
    let (parent, registry) = parent(core);
    let declaration = core.native_definition(key.validation).unwrap();
    let schemas = SchemaSet::new(declaration, source, &BuiltinNativeSchemas, 16).unwrap();
    let envelope = CompletionEnvelope::derive(
        &core.state.rows,
        core.limits,
        parent,
        registry,
        declaration,
        descriptor_limits(core.limits, parent, registry).unwrap(),
    )
    .unwrap();
    let ordinal = registry
        .rows()
        .iter()
        .position(|row| transactions::key_for_registered(key.claim, *row) == key)
        .unwrap();
    book.install_begin(
        key,
        core.native_evaluation(key).unwrap().binding(),
        envelope,
        schemas,
        ordinal,
    )
    .unwrap()
}

fn advance(book: &mut CompletionBook, index: u32, terminal: bool) -> Journal {
    let key = fixture::key(index);
    let before = book.grant(key).unwrap().credit.binding;
    book.advance(
        key,
        before,
        before.next().unwrap(),
        terminal,
        CompletionUse::Regular,
    )
    .unwrap()
}

fn children(core: &Core<NativeState>, count: u128) -> NativeInput {
    let expected = core.native_claim(fixture::key(1).claim).unwrap().binding();
    let mut claims = Vec::new();
    let mut declarations = Vec::new();
    for id in 2..count + 2 {
        let NativeCommand::Create {
            claims: mut next,
            declarations: mut definitions,
        } = fixture::creation(1000 + id, id, &[], None).command
        else {
            panic!("creation")
        };
        next[0].definition.lineage = Lineage::new(
            next[0].definition.binding,
            Cause::Claim(fixture::key(1).claim),
            &[],
            0,
        )
        .unwrap();
        next[0].owner = Some(Owner {
            expected,
            receipt: None,
        });
        claims.append(&mut next);
        declarations.append(&mut definitions);
    }
    NativeInput {
        request: fixture::request(fixture::ISSUER, 1000),
        command: NativeCommand::Create {
            claims,
            declarations,
        },
    }
}

#[test]
fn retiring_a_grant_and_rolling_back_later_reports_preserves_the_older_commit() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 3]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for index in [1, 2, 3] {
        let journal = install(&mut book, &core, &source, index);
        book.commit(journal).unwrap();
    }
    let older = advance(&mut book, 1, true);
    let retiring = advance(&mut book, 2, true);
    let younger = advance(&mut book, 3, false);
    book.commit(older).unwrap();
    assert!(book.entries.get(fixture::key(1)).is_none());
    book.rollback(younger).unwrap();
    book.rollback(retiring).unwrap();
    assert!(book.entries.get(fixture::key(1)).is_none());
    assert_eq!(book.remaining_reports(fixture::key(2)), Some(2));
    assert_eq!(book.remaining_reports(fixture::key(3)), Some(2));
    for index in [2, 3] {
        let journal = advance(&mut book, index, true);
        book.commit(journal).unwrap();
    }
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn wrong_pinned_registration_ordinal_refuses_report_and_changed_parent_without_spending_credit() {
    let core = fixture::running(&[(ValidationMode::Observe, false); 2]);
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for index in 1..=2 {
        let journal = install(&mut book, &core, &source, index);
        book.commit(journal).unwrap();
    }
    let changed = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 40),
        children(&core, 1),
        &[],
    ));
    let input = fixture::report_for(
        &core,
        None,
        2000,
        1,
        VerdictValue::Pass,
        fixture::descriptor(fixture::artifact_spec(
            2001,
            fixture::EVALUATOR,
            VerdictValue::Pass,
        )),
    );
    let NativeCommand::ReportAdmission {
        artifact, expected, ..
    } = &input.command
    else {
        panic!("report")
    };
    let (parent, registry) = parent(&core);
    let key = fixture::key(1);
    let original = book.grant(key).unwrap().registration_index;
    let other = book.grant(fixture::key(2)).unwrap().registration_index;
    assert_ne!(original, other);
    book.report_contract(
        key,
        *expected,
        parent,
        registry,
        artifact,
        test_report_schema(),
        &BuiltinNativeSchemas,
    )
    .unwrap();
    let changed_view = View::committed(&core.state).with_own(&changed).unwrap();
    book.check_parents(&changed_view, &changed).unwrap();
    book.entries
        .update(key, |grant| grant.registration_index = other)
        .unwrap();
    let budget = source.stats();
    assert!(matches!(
        book.report_contract(
            key,
            *expected,
            parent,
            registry,
            artifact,
            test_report_schema(),
            &BuiltinNativeSchemas
        ),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    assert!(matches!(
        book.check_parents(&changed_view, &changed),
        Err(NativeError::Contract(ContractError::InvalidTarget))
    ));
    assert_eq!(book.remaining_reports(key), Some(2));
    assert_eq!(source.stats(), budget);
    book.entries
        .update(key, |grant| grant.registration_index = original)
        .unwrap();
    book.report_contract(
        key,
        *expected,
        parent,
        registry,
        artifact,
        test_report_schema(),
        &BuiltinNativeSchemas,
    )
    .unwrap();
    book.check_parents(&changed_view, &changed).unwrap();
    drop(book);
    assert_eq!(source.stats().used, 0);
}

#[test]
fn child_registration_batch_checks_the_final_parent_cohort_once() {
    let mut core = fixture::core();
    // This larger real cohort must pass the existing full Admission projection
    // bound before the test reaches completion-book parent traversal.
    core.limits.plan_edges = 2048;
    fixture::publish(
        &mut core,
        10,
        fixture::creation(1, 1, &[(ValidationMode::Observe, false); 8], None),
    );
    fixture::publish(&mut core, 20, fixture::post(2, fixture::binding(1)));
    for index in 1..=8 {
        let claim = core
            .native_claim(fixture::key(index).claim)
            .unwrap()
            .binding();
        let evaluation = core
            .native_evaluation(fixture::key(index))
            .unwrap()
            .binding();
        fixture::publish(
            &mut core,
            30,
            fixture::begin(10 + u128::from(index), claim, index, evaluation),
        );
    }
    let source = source();
    let mut book = CompletionBook::new(&source, core.limits).unwrap();
    for index in 1..=8 {
        let journal = install(&mut book, &core, &source, index);
        book.commit(journal).unwrap();
    }
    let one = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 40),
        children(&core, 1),
        &[],
    ));
    let many = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 40),
        children(&core, 4),
        &[],
    ));
    let registrations: Vec<_> = (0..many.outcome().events)
        .filter_map(
            |ordinal| match CompletionBook::event(&many, ordinal, None).unwrap().fact {
                NativeFact::Claim(event) if event.kind == NativeEventKind::ChildRegistered => {
                    Some(event)
                }
                _ => None,
            },
        )
        .collect();
    assert_eq!(registrations.len(), 4);
    assert_eq!(
        registrations
            .iter()
            .filter(|event| event.after
                == core
                    .native_chain(&[&many])
                    .unwrap()
                    .claim(fixture::key(1).claim)
                    .unwrap()
                    .binding())
            .count(),
        1
    );
    let budget = source.stats();
    book.entries.reset_visits();
    book.check_parents(&View::committed(&core.state).with_own(&one).unwrap(), &one)
        .unwrap();
    let one_visits = book.entries.visits();
    book.entries.reset_visits();
    book.check_parents(
        &View::committed(&core.state).with_own(&many).unwrap(),
        &many,
    )
    .unwrap();
    let many_visits = book.entries.visits();
    book.entries.reset_visits();
    let after = EvaluationKey {
        claim: ClaimId::from_u128(2),
        validation: ValidationId::from_u128(0),
        target: EvaluationTarget::Admission,
        generation: 0,
    };
    assert_eq!(book.entries.iter_from(after).count(), 0);
    let empty_visits = book.entries.visits();
    assert!(
        many_visits <= one_visits + 3 * empty_visits + 4,
        "one parent: {one_visits}; four child facts: {many_visits}; empty cohort: {empty_visits}"
    );
    assert_eq!(source.stats(), budget);
    assert_eq!(book.totals.live, 8);
    assert_eq!(book.totals.reports, 16);
    drop(one);
    core.publish_native(many).unwrap();
    assert_eq!(
        core.native_claim(fixture::key(1).claim)
            .unwrap()
            .scopes()
            .children()
            .len(),
        4
    );
    for index in 1..=8 {
        assert_eq!(book.remaining_reports(fixture::key(index)), Some(2));
    }
    drop(book);
    assert_eq!(source.stats().used, 0);
}
