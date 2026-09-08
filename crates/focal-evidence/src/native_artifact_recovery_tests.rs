use super::*;

#[test]
fn recovery_reads_existing_inline_and_pointer_trees_without_resealing() {
    let root = tempfile::tempdir().unwrap();
    let mut current = store(root.path());
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let pool = budget();
    let sealed = current
        .verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas)
        .unwrap();
    let expected = sealed.custody().payload();
    drop(sealed);
    drop(current);
    let current = store(root.path());
    let quote = NativeVerificationBudget::for_schema(artifact.schema_hash(), &BuiltinNativeSchemas)
        .unwrap();
    let exact = MemoryBudget::new(quote.peak_bytes(), quote.peak_bytes()).unwrap();
    let recovered = current
        .recover_native_artifact(
            request(),
            &artifact,
            expected,
            1,
            &exact,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    recovered.check(request(), &artifact).unwrap();
    assert_eq!(recovered.custody().payload(), expected);
    assert_eq!(exact.stats().used, quote.retained_bytes());
    drop(recovered);
    assert_eq!(exact.stats().used, 0);
    let pointer_artifact = descriptor(specification(PayloadSpec::Content(expected)));
    let pointer = current
        .recover_native_artifact(
            request(),
            &pointer_artifact,
            expected,
            1,
            &exact,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    pointer.check(request(), &pointer_artifact).unwrap();
    assert!(pointer.check(request(), &artifact).is_err());
    drop(pointer);
    let missing_root = tempfile::tempdir().unwrap();
    let missing = store(missing_root.path());
    assert!(
        missing
            .recover_native_artifact(
                request(),
                &artifact,
                expected,
                1,
                &exact,
                &BuiltinNativeSchemas
            )
            .is_err()
    );
    assert_eq!(exact.stats().used, 0);
    assert!(
        missing
            .read_bytes(&reference(expected), REPORT.len())
            .is_err()
    );
}

#[test]
fn recovery_refuses_wrong_bytes_revision_request_schema_and_unfunded_reads() {
    let root = tempfile::tempdir().unwrap();
    let mut current = store(root.path());
    let pool = budget();
    let artifact = descriptor(specification(PayloadSpec::Inline(REPORT)));
    let sealed = current
        .verify_native_artifact(request(), &artifact, domain(), &pool, &BuiltinNativeSchemas)
        .unwrap();
    let expected = sealed.custody().payload();
    drop(sealed);
    let quote = NativeVerificationBudget::for_schema(artifact.schema_hash(), &BuiltinNativeSchemas)
        .unwrap();
    let short = MemoryBudget::new(quote.peak_bytes() - 1, quote.peak_bytes() - 1).unwrap();
    assert!(matches!(
        current.recover_native_artifact(
            request(),
            &artifact,
            expected,
            1,
            &short,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Memory(_))
    ));
    assert_eq!(short.stats().used, 0);
    for revision in [0, 2, u64::MAX] {
        assert!(
            current
                .recover_native_artifact(
                    request(),
                    &artifact,
                    expected,
                    revision,
                    &pool,
                    &BuiltinNativeSchemas
                )
                .is_err()
        );
    }
    let wrong = RequestKey {
        principal: ParticipantId::from_u128(99),
        ..request()
    };
    assert!(matches!(
        current.recover_native_artifact(
            wrong,
            &artifact,
            expected,
            1,
            &pool,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::WrongRequest)
    ));
    let mut changed = REPORT.to_vec();
    changed[10] = if changed[10] == b'x' { b'y' } else { b'x' };
    let wrong_bytes = descriptor(specification(PayloadSpec::Inline(&changed)));
    assert!(
        current
            .recover_native_artifact(
                request(),
                &wrong_bytes,
                expected,
                1,
                &pool,
                &BuiltinNativeSchemas
            )
            .is_err()
    );
    let invalid = current
        .seal_native_inline(domain(), b"not a schema result")
        .unwrap();
    let invalid_artifact = descriptor(specification(PayloadSpec::Content(pointer(&invalid))));
    assert!(matches!(
        current.recover_native_artifact(
            request(),
            &invalid_artifact,
            pointer(&invalid),
            1,
            &pool,
            &BuiltinNativeSchemas
        ),
        Err(NativeEvidenceError::Schema(_))
    ));
    assert_eq!(pool.stats().used, 0);
    let recovered = current
        .recover_native_artifact(
            request(),
            &artifact,
            expected,
            1,
            &pool,
            &BuiltinNativeSchemas,
        )
        .unwrap();
    recovered.check(request(), &artifact).unwrap();
}
