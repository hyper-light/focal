use super::*;
use crate::native::report_tests as fixture;
use focal_evidence::BuiltinNativeSchemas;
use focal_memory::{BudgetKind, BudgetLane, Change, Entry};
use focal_model::{ObjectRevision, ValidationMode, VerdictValue};

fn limits() -> EncodingLimits {
    EncodingLimits {
        bytes: 32 << 20,
        visits: 100_000_000,
        rows: 100_000,
    }
}
fn inspection(bytes: usize) -> InspectionLimits {
    InspectionLimits {
        bytes,
        visits: 100_000_000,
        rows: 100_000,
        row_bytes: 32 << 20,
    }
}
pub(super) fn encode(prepared: &NativePrepared) -> Vec<u8> {
    let plan = EncodingPlan::prepare(prepared, limits()).unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    assert_eq!(plan.write_into(&mut bytes).unwrap(), plan.quote().hash);
    bytes
}
fn created(core: &Core<NativeState>) -> NativePrepared {
    fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 1),
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ))
}
#[test]
fn real_records_preserve_exact_mutations_and_do_not_serialize_unchanged_rows() {
    let mut core = fixture::core();
    let prepared = created(&core);
    let budget = core.native_budget();
    let bytes = encode(&prepared);
    assert_eq!(core.native_budget(), budget);
    assert_eq!(core.native_sequence(), SessionSeq(0));
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    assert_eq!(record.header().ledger, fixture::binding(1).ledger);
    assert_eq!(record.header().range, prepared.range.id());
    assert_eq!(record.header().outcome, prepared.outcome());
    assert_eq!(record.header().base, SessionSeq(0));
    assert_eq!(record.quote().rows, prepared.range.len());
    let rows = record
        .rows(100_000_000)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), prepared.mutation_count());
    assert!(rows.iter().any(|row| row.family() == RowFamily::Definition));
    assert!(
        rows.iter()
            .all(|row| !row.deleted() && !row.body().is_empty())
    );
    // A decoder can carry borrowed bytes into a row plan after the temporary
    // EncodedRow is consumed; only the original record buffer must remain live.
    let bodies: Vec<&[u8]> = record
        .rows(100_000_000)
        .unwrap()
        .map(|row| row.unwrap().body())
        .collect();
    assert_eq!(bodies.len(), prepared.mutation_count());
    assert_eq!(
        rows.iter().map(|row| row.key).collect::<Vec<_>>(),
        prepared
            .writes
            .entries()
            .map(|(key, _)| key)
            .collect::<Vec<_>>()
    );
    drop(rows);
    core.publish_native(prepared).unwrap();
    let posted = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 2),
        fixture::post(2, fixture::binding(1)),
        &[],
    ));
    let bytes = encode(&posted);
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    assert!(record.quote().rows < posted.range.len());
    assert!(
        record
            .rows(100_000_000)
            .unwrap()
            .all(|row| row.unwrap().family() != RowFamily::Definition)
    );
    // A failed publication retains an identical record, ready to propose/retry.
    let mut foreign = fixture::core();
    let refused = foreign.publish_native(posted).unwrap_err().prepared;
    assert_eq!(encode(&refused), bytes);
    core.publish_native(refused).unwrap();
}

#[test]
fn exact_byte_work_and_count_quotes_bound_every_pass_without_partial_wrong_size_writes() {
    let core = fixture::core();
    let prepared = created(&core);
    let plan = EncodingPlan::prepare(&prepared, limits()).unwrap();
    let q = plan.quote();
    for short in [
        EncodingLimits {
            bytes: q.bytes - 1,
            ..limits()
        },
        EncodingLimits {
            visits: q.visits - 1,
            ..limits()
        },
        EncodingLimits {
            rows: q.rows - 1,
            ..limits()
        },
    ] {
        assert!(matches!(
            EncodingPlan::prepare(&prepared, short),
            Err(CodecError::Capacity)
        ));
    }
    let exact = EncodingPlan::prepare(
        &prepared,
        EncodingLimits {
            bytes: q.bytes,
            visits: q.visits,
            rows: q.rows,
        },
    )
    .unwrap();
    assert_eq!(exact.quote(), q);
    for length in [q.bytes - 1, q.bytes + 1] {
        let mut output = vec![0xa5; length];
        assert_eq!(exact.write_into(&mut output), Err(CodecError::Capacity));
        assert!(output.iter().all(|byte| *byte == 0xa5));
    }
    let bytes = encode(&prepared);
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    let iq = record.quote();
    for short in [
        InspectionLimits {
            bytes: bytes.len() - 1,
            ..inspection(bytes.len())
        },
        InspectionLimits {
            visits: iq.visits - 1,
            ..inspection(bytes.len())
        },
        InspectionLimits {
            rows: iq.rows - 1,
            ..inspection(bytes.len())
        },
        InspectionLimits {
            row_bytes: 0,
            ..inspection(bytes.len())
        },
    ] {
        assert!(matches!(
            StructuralRecord::inspect(&bytes, short),
            Err(CodecError::Capacity)
        ));
    }
    assert!(
        StructuralRecord::inspect(
            &bytes,
            InspectionLimits {
                visits: iq.visits,
                ..inspection(bytes.len())
            }
        )
        .is_ok()
    );
    let mut scan = record.rows(100_000_000).unwrap();
    for row in scan.by_ref() {
        row.unwrap();
    }
    let visits = scan.visits_used();
    assert!(record.rows(visits).unwrap().all(|row| row.is_ok()));
    let mut short = record.rows(visits - 1).unwrap();
    assert!(short.by_ref().any(|row| row.is_err()));
    assert!(short.next().is_none());
}

fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let (payload, checksum) = bytes.split_at_mut(at);
    let mut hash = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hash.update(payload);
    checksum.copy_from_slice(hash.finalize().as_bytes());
}

#[test]
fn corrupted_truncated_wrong_profile_and_outcome_records_refuse_before_import() {
    let core = fixture::core();
    let bytes = encode(&created(&core));
    for length in 0..bytes.len() {
        assert!(StructuralRecord::inspect(&bytes[..length], inspection(bytes.len())).is_err());
    }
    // Every byte is protected, including mutation bodies, metadata and trailer.
    for index in 0..bytes.len() {
        let mut corrupt = bytes.clone();
        corrupt[index] ^= 0x80;
        assert!(StructuralRecord::inspect(&corrupt, inspection(corrupt.len())).is_err());
    }
    for (offset, replacement) in [(0, 0), (8, 2), (10, 2)] {
        let mut corrupt = bytes.clone();
        corrupt[offset] = replacement;
        checksum(&mut corrupt);
        assert!(StructuralRecord::inspect(&corrupt, inspection(corrupt.len())).is_err());
    }
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    let outcome_body = record
        .rows(100_000_000)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.family() == RowFamily::Outcome)
        .unwrap();
    let mut corrupt = bytes.clone();
    let at = corrupt
        .windows(outcome_body.body().len())
        .rposition(|window| window == outcome_body.body())
        .unwrap();
    corrupt[at] ^= 1;
    checksum(&mut corrupt);
    assert!(matches!(
        StructuralRecord::inspect(&corrupt, inspection(corrupt.len())),
        Err(CodecError::InvalidTag("record outcome"))
    ));
    // A self-consistent checksum is not semantic validation or authentication.
    // Changing a non-frame body can pass inspection, but grants no import API.
    let claim = record
        .rows(100_000_000)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| row.family() == RowFamily::Claim)
        .unwrap();
    let at = bytes
        .windows(claim.body().len())
        .position(|window| window == claim.body())
        .unwrap();
    let mut untrusted = bytes.clone();
    untrusted[at] ^= 1;
    checksum(&mut untrusted);
    assert!(StructuralRecord::inspect(&untrusted, inspection(untrusted.len())).is_ok());
}

#[test]
fn held_report_records_include_original_result_artifact_and_event_without_new_owner_funding() {
    let core = fixture::running(&[(ValidationMode::Required, false)]);
    let source = core.state.budget.clone();
    let report = fixture::report_for(
        &core,
        None,
        993,
        1,
        VerdictValue::Pass,
        fixture::descriptor(fixture::artifact_spec(
            9993,
            fixture::EVALUATOR,
            VerdictValue::Pass,
        )),
    );
    let mut custody = fixture::Custody::new();
    let proof = fixture::verified(&mut custody, &report);
    let mut owner = NativeOwner::new(core).unwrap();
    let _pressure = source
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            source.limit() - source.stats().used,
        )
        .unwrap()
        .commit();
    let before = owner.budget_stats();
    let NativeStaging::Prepared { candidate, .. } = owner
        .prepare_evidenced_with_schemas(
            fixture::context(fixture::EVALUATOR, 100),
            report,
            Some(&proof),
            &BuiltinNativeSchemas,
        )
        .unwrap()
    else {
        panic!("new report");
    };
    let prepared = owner.prepared_candidate(candidate).unwrap();
    let funded = owner.budget_stats();
    // Test-owned output stands in for a separately precharged WAL buffer. The
    // encoder itself cannot reserve ordinary credit under this full pressure.
    let bytes = encode(prepared);
    assert_eq!(owner.budget_stats(), funded);
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    let families = record
        .rows(100_000_000)
        .unwrap()
        .map(|row| row.unwrap().family())
        .collect::<Vec<_>>();
    for family in [
        RowFamily::Accepted,
        RowFamily::Artifact,
        RowFamily::ArtifactIdentity,
        RowFamily::Evaluation,
        RowFamily::Event,
        RowFamily::Outcome,
        RowFamily::Meta,
    ] {
        assert!(families.contains(&family), "{family:?}");
    }
    // Passing Admission records its evaluation; posting the claim is a distinct
    // later action, so this mutation must not rewrite the unchanged claim row.
    assert!(!families.contains(&RowFamily::Claim));
    owner.discard_from(candidate).unwrap();
    assert_eq!(owner.budget_stats(), before);
}

#[test]
fn explicit_deletion_and_present_link_tombstone_have_distinct_records() {
    let mut core = fixture::core();
    let removed = Key::ArtifactIdentity(ContentHash([55; 32]));
    let inserted = core
        .state
        .rows
        .prepare_batch_with(
            1,
            vec![Change::Put(Entry::new(
                removed,
                Row::ArtifactIdentity(ArtifactId::from_u128(99)),
                0,
            ))],
            BudgetLane::Ordinary,
            prepare::copy,
        )
        .unwrap();
    core.state.rows.publish(inserted).unwrap();
    let initial = created(&core);
    let outcome = initial.outcome;
    let mut changes = initial
        .range
        .entries()
        .filter(|entry| entry.key != removed)
        .map(|entry| {
            Change::Put(Entry::new(
                entry.key,
                prepare::copy(&entry.value).unwrap(),
                entry.heap_bytes,
            ))
        })
        .collect::<Vec<_>>();
    let tombstone = Key::MonitorLink(ClaimId::from_u128(1), MonitorId::from_u128(22));
    changes.push(Change::Delete(removed));
    changes.push(Change::Put(Entry::new(
        tombstone,
        Row::MonitorLink(None),
        0,
    )));
    let plan = core
        .state
        .rows
        .plan_batch(
            initial.range.prefix(),
            changes,
            BudgetLane::Ordinary,
            usize::MAX,
        )
        .unwrap();
    let count = plan.changes().len();
    let allowance = mutation::bytes(count).unwrap();
    let funding = core
        .state
        .budget
        .reserve(BudgetKind::Pending, BudgetLane::Ordinary, allowance)
        .unwrap()
        .commit();
    let writes = mutation::WriteSet::capture(
        initial.content_profile(),
        plan.changes(),
        allowance,
        funding,
    )
    .unwrap();
    let prepared = NativePrepared {
        range: plan.build_with(prepare::copy).unwrap(),
        outcome,
        writes,
    };
    let bytes = encode(&prepared);
    let record = StructuralRecord::inspect(&bytes, inspection(bytes.len())).unwrap();
    let rows = record
        .rows(100_000_000)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let deleted = rows.iter().find(|row| row.key == removed).unwrap();
    assert!(deleted.deleted());
    assert!(deleted.body().is_empty());
    let present = rows.iter().find(|row| row.key == tombstone).unwrap();
    assert!(!present.deleted());
    assert_eq!(present.body(), &[0]);
}

#[test]
fn all_key_families_and_invocation_namespaces_roundtrip_without_collisions() {
    let claim = ClaimId::from_u128(1);
    let monitor = MonitorId::from_u128(2);
    let evaluation = EvaluationKey {
        claim,
        validation: ValidationId::from_u128(3),
        target: EvaluationTarget::Admission,
        generation: u64::MAX,
    };
    let result = NativeResultKey {
        evaluation,
        revision: ObjectRevision(u64::MAX),
    };
    let cycle = NativeCycleKey {
        claim,
        receipt: ReceiptId::from_u128(4),
        epoch: u64::MAX,
        cycle: u32::MAX,
    };
    let artifact = ArtifactId::from_u128(5);
    let response = TestamentId::from_u128(6);
    let request = fixture::request(fixture::ISSUER, 2).into();
    let hash = ContentHash([0xab; 32]);
    let keys = [
        Key::IncomingHead(claim),
        Key::IncomingLink(claim, claim),
        Key::Monitor(monitor),
        Key::MonitorHead(claim),
        Key::MonitorLink(claim, monitor),
        Key::MissingResult(result),
        Key::Meta,
        Key::Claim(claim),
        Key::Definition(evaluation.validation),
        Key::Evaluation(evaluation),
        Key::Artifact(artifact),
        Key::ArtifactIdentity(hash),
        Key::Accepted(result),
        Key::DeliveryResult(result),
        Key::Receipt(cycle.receipt),
        Key::Cycle(cycle),
        Key::RetiredCycleHead(claim),
        Key::RetiredCycle(cycle),
        Key::Work(artifact),
        Key::WorkSlot(cycle, u32::MAX),
        Key::Diagnostic(artifact),
        Key::Response(response),
        Key::ResultTestament(response),
        Key::ClaimResultTestament(claim),
        Key::Outcome(request),
        Key::Event(SessionSeq(u64::MAX), u32::MAX),
        Key::ClaimContent(claim),
        Key::ClaimIdentity(u16::MAX, hash),
        Key::DefinitionIdentity(u16::MAX, hash),
        Key::CreationResult(request),
    ];
    let mut encodings = std::collections::BTreeSet::new();
    for (tag, key) in keys.into_iter().enumerate() {
        let mut sink = CountingSink::new(1024, 4096);
        fixed::key(&mut sink, key).unwrap();
        let mut bytes = vec![0; sink.len()];
        fixed::key(&mut SliceSink::new(&mut bytes, sink.visits_used()), key).unwrap();
        assert_eq!(usize::from(bytes[0]), tag);
        let mut cursor = bytes::Cursor::new(&bytes, 1024, 4096).unwrap();
        assert_eq!(fixed::read_key(&mut cursor).unwrap(), key);
        cursor.finish().unwrap();
        assert!(encodings.insert(bytes));
    }
    for target in [
        EvaluationTarget::Admission,
        EvaluationTarget::Increment { artifact },
        EvaluationTarget::Work {
            response,
            slot: u32::MAX,
            artifact,
        },
        EvaluationTarget::MissingSlot {
            response,
            slot: u32::MAX,
        },
        EvaluationTarget::Delivery { response },
    ] {
        let key = EvaluationKey {
            target,
            ..evaluation
        };
        for invocation in [
            request,
            NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
                evaluation: key,
                timer: TimerId::from_u128(7),
                generation: u64::MAX,
            }),
            NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
                claim,
                timer: TimerId::from_u128(7),
                generation: u64::MAX,
            }),
            NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
                claim,
                monitor,
                timer: TimerId::from_u128(7),
                generation: u64::MAX,
            }),
        ] {
            let mut sink = CountingSink::new(1024, 4096);
            fixed::invocation(&mut sink, invocation).unwrap();
            let mut bytes = vec![0; sink.len()];
            fixed::invocation(
                &mut SliceSink::new(&mut bytes, sink.visits_used()),
                invocation,
            )
            .unwrap();
            let mut cursor = bytes::Cursor::new(&bytes, 1024, 4096).unwrap();
            assert_eq!(fixed::read_invocation(&mut cursor).unwrap(), invocation);
            cursor.finish().unwrap();
        }
    }
    assert!(fixed::key(&mut CountingSink::new(1024, 4096), Key::End).is_err());
    assert!(fixed::read_key(&mut bytes::Cursor::new(&[30], 1, 100).unwrap()).is_err());
}
