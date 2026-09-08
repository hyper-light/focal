//! Actual authored creation, lifecycle advancement and fresh-request content
//! reuse through the recorded-successor boundary, including its empty journal.
use super::*;

#[test]
fn authored_creation_post_and_zero_event_reuse_replay_into_fresh_incarnations() {
    let directory = tempfile::tempdir().unwrap();
    let store = checkpoint::store(directory.path());
    let (mut original, [creation, posting, reuse]) = authored::replay_fixture();
    let mut recovered = restored(&original, 13_001, &store);
    let source = original.state.rows.id();
    let claim = ClaimId::from_u128(1);
    let validation = ValidationId::from_u128(1);
    let creation_request = creation.request;
    let reuse_request = reuse.request;
    assert_ne!(creation_request, reuse_request);

    let created = apply(&mut original, &mut recovered, 10, creation, &store);
    let created_header = inspect(&created).header();
    assert_eq!(created_header.profile, NativeContentProfile::AuthoredV1);
    assert_eq!(
        (
            created_header.outcome.created,
            created_header.outcome.definitions
        ),
        (1, 1)
    );
    let claim_body = recovered.native_claim_content(claim).unwrap();
    let claim_hash = claim_body.content_hash();
    let claim_pointer = claim_body.description().as_ptr();
    let definition = recovered.native_validation_descriptor(validation).unwrap();
    let definition_hash = definition.content_hash();
    let definition_pointer = definition.description().as_ptr();
    let specification = definition.specification_hash();
    assert_eq!(claim_body.requirements()[0].specification, specification);
    assert_eq!(claim_body.requirements()[0].id, validation);
    assert!(
        matches!(recovered.state.rows.get(&Key::ClaimIdentity(claim_body.schema(), claim_hash)), Some(Row::ClaimIdentity(id)) if *id == claim)
    );
    assert!(
        matches!(recovered.state.rows.get(&Key::DefinitionIdentity(definition.schema(), definition_hash)), Some(Row::DefinitionIdentity(id)) if *id == validation)
    );

    apply(&mut original, &mut recovered, 20, posting, &store);
    let posted = recovered.native_claim(claim).unwrap().binding();
    assert_eq!(
        recovered.native_claim(claim).unwrap().status(),
        ClaimStatus::Posted
    );
    assert_eq!(
        recovered
            .native_claim_content(claim)
            .unwrap()
            .description()
            .as_ptr(),
        claim_pointer
    );
    assert_eq!(
        recovered
            .native_validation_descriptor(validation)
            .unwrap()
            .description()
            .as_ptr(),
        definition_pointer
    );
    // A second independent incarnation starts at the posted checkpoint. The
    // exact same immutable reuse record must extend either validated base.
    let mut checkpoint_base = restored(&original, 13_002, &store);
    let original_event_count = match recovered.state.rows.get(&Key::Meta).unwrap() {
        Row::Meta(meta) => meta.events,
        _ => panic!("missing metadata"),
    };
    let before = checkpoint::encode(&recovered);
    let before_budget = recovered.native_budget();
    let prepared = prepare_input(&original, 30, reuse);
    let reused = encode_mutation(&prepared);
    let record = inspect(&reused);
    let outcome = record.header().outcome;
    assert_eq!(outcome.sequence, SessionSeq(3));
    assert_eq!(
        (
            outcome.created,
            outcome.changed,
            outcome.definitions,
            outcome.evaluations,
            outcome.artifacts,
            outcome.responses,
            outcome.results,
            outcome.result_testaments,
            outcome.events
        ),
        (0, 0, 0, 0, 0, 0, 0, 0, 0)
    );
    let keys = record
        .rows(1_000_000)
        .unwrap()
        .map(|row| row.unwrap().key)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&Key::Meta));
    assert!(keys.contains(&Key::Outcome(NativeInvocation::from(reuse_request))));
    assert!(keys.contains(&Key::CreationResult(NativeInvocation::from(reuse_request))));
    assert!(!keys.iter().any(|key| matches!(key, Key::Event(..))));

    let discarded = replay(&recovered, &reused, source, &store).unwrap();
    assert_eq!(discarded.outcome(), outcome);
    assert_eq!(recovered.native_sequence(), SessionSeq(2));
    drop(discarded);
    assert_eq!(recovered.native_budget(), before_budget);
    assert_eq!(checkpoint::encode(&recovered), before);
    let replayed = replay(&recovered, &reused, source, &store).unwrap();
    let replayed_checkpoint = replay(&checkpoint_base, &reused, source, &store).unwrap();
    original.publish_native(prepared).unwrap();
    recovered.publish_native(replayed).unwrap();
    checkpoint_base.publish_native(replayed_checkpoint).unwrap();
    checkpoint::compare(&original, &recovered);
    checkpoint::compare(&original, &checkpoint_base);
    assert_eq!(recovered.native_claim(claim).unwrap().binding(), posted);
    assert_eq!(
        recovered
            .native_claim_content(claim)
            .unwrap()
            .content_hash(),
        claim_hash
    );
    assert_eq!(
        recovered
            .native_validation_descriptor(validation)
            .unwrap()
            .content_hash(),
        definition_hash
    );
    assert_eq!(
        recovered
            .native_claim_content(claim)
            .unwrap()
            .description()
            .as_ptr(),
        claim_pointer
    );
    assert_eq!(
        recovered
            .native_validation_descriptor(validation)
            .unwrap()
            .description()
            .as_ptr(),
        definition_pointer
    );
    assert_eq!(recovered.native_sequence(), SessionSeq(3));
    assert!(recovered.native_event(outcome.sequence, 0).is_none());
    match recovered.state.rows.get(&Key::Meta).unwrap() {
        Row::Meta(meta) => assert_eq!(meta.events, original_event_count),
        _ => panic!("missing metadata"),
    }
    let original_mapping = recovered
        .native_creation_result(creation_request)
        .unwrap()
        .entries();
    let reused_mapping = recovered
        .native_creation_result(reuse_request)
        .unwrap()
        .entries();
    assert_eq!(original_mapping, reused_mapping);
    assert_eq!(reused_mapping.len(), 2);
    assert_eq!(reused_mapping[0].family, NativeCreatedFamily::Claim);
    assert_eq!(reused_mapping[1].family, NativeCreatedFamily::Validation);
    assert!(replay(&recovered, &reused, source, &store).is_err());
    assert_eq!(recovered.native_sequence(), SessionSeq(3));
}
