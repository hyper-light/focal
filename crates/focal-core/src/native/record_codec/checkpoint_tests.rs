use super::*;
use crate::native::prepare::ALLOCATION as ALLOCATOR_OVERHEAD;
use crate::native::report_tests as fixture;
use focal_memory::{BudgetKind, BudgetLane, Change, Entry};
use focal_model::ValidationMode;

// The version 6 header ends with the layout: a member count, the layout
// epoch, then one member's identity and its unbounded start.
const ROW_START: usize = 104;
const PREFIX_AT: usize = 59;
const COUNT_AT: usize = 67;

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
fn encode(core: &Core<NativeState>) -> Vec<u8> {
    let plan = EncodingPlan::prepare(core, limits()).unwrap();
    let mut output = vec![0; plan.quote().bytes];
    assert_eq!(plan.write_into(&mut output).unwrap(), plan.quote().hash);
    output
}
fn populated() -> Core<NativeState> {
    let mut core = fixture::core();
    let created = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 1),
        fixture::creation(1, 1, &[(ValidationMode::Required, false)], None),
        &[],
    ));
    core.publish_native(created).unwrap();
    core
}
fn checksum(bytes: &mut [u8]) {
    let at = bytes.len() - 32;
    let (payload, trailer) = bytes.split_at_mut(at);
    let mut hasher = blake3::Hasher::new_derive_key(HASH_DOMAIN);
    hasher.update(payload);
    trailer.copy_from_slice(hasher.finalize().as_bytes());
}
fn spans(bytes: &[u8]) -> Vec<(Key, std::ops::Range<usize>)> {
    let end = bytes.len() - 32;
    let mut cursor = Cursor::new(&bytes[ROW_START..end], end, usize::MAX).unwrap();
    let mut spans = Vec::new();
    while cursor.remaining() != 0 {
        let start = ROW_START + cursor.offset();
        let row = inspect::read_row(&mut cursor, usize::MAX).unwrap();
        spans.push((row.key, start..ROW_START + cursor.offset()));
    }
    spans
}
fn replace_row(bytes: &[u8], range: std::ops::Range<usize>, replacement: &[u8]) -> Vec<u8> {
    let mut changed = bytes[..range.start].to_vec();
    changed.extend_from_slice(replacement);
    changed.extend_from_slice(&bytes[range.end..]);
    checksum(&mut changed);
    changed
}

#[test]
fn complete_checkpoint_preserves_unchanged_rows_and_all_retained_publication_history() {
    let mut core = populated();
    let original = encode(&core);
    let initial = StructuralCheckpoint::inspect(&original, inspection(original.len())).unwrap();
    let initial_rows = initial
        .rows(usize::MAX)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.key, row.body())
        })
        .collect::<Vec<_>>();
    let prepared = fixture::prepared(core.prepare_native(
        fixture::context(fixture::ISSUER, 2),
        fixture::post(2, fixture::binding(1)),
        &[],
    ));
    let mutation_bytes = super::super::tests::encode(&prepared);
    let mutation =
        StructuralRecord::inspect(&mutation_bytes, inspection(mutation_bytes.len())).unwrap();
    assert!(
        mutation
            .rows(usize::MAX)
            .unwrap()
            .all(|row| row.unwrap().family() != RowFamily::Definition)
    );
    core.publish_native(prepared).unwrap();
    let before = core.native_budget();
    let bytes = encode(&core);
    let checkpoint = StructuralCheckpoint::inspect(&bytes, inspection(bytes.len())).unwrap();
    assert_eq!(core.native_budget(), before);
    assert_eq!(checkpoint.header().ledger, core.state.ledger);
    assert_eq!(
        checkpoint.header().profile,
        NativeContentProfile::ProjectionOnly
    );
    assert_eq!(checkpoint.header().range, core.state.rows.id());
    assert_eq!(checkpoint.header().prefix, SessionSeq(2));
    assert_eq!(checkpoint.header().rows, core.state.rows.len() as u64);
    assert_eq!(checkpoint.quote().rows, core.state.rows.len());
    assert!(checkpoint.quote().rows > mutation.quote().rows);
    let rows = checkpoint
        .rows(usize::MAX)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.key).collect::<Vec<_>>(),
        core.state
            .rows
            .entries()
            .map(|entry| entry.key)
            .collect::<Vec<_>>()
    );
    assert!(
        rows.iter()
            .all(|row| !row.deleted() && !row.body().is_empty())
    );
    for (key, original_body) in initial_rows {
        if matches!(key, Key::Definition(_) | Key::Outcome(_) | Key::Event(..)) {
            assert_eq!(
                rows.iter().find(|row| row.key == key).unwrap().body(),
                original_body
            );
        }
    }
    assert_eq!(
        rows.iter()
            .filter(|row| row.family() == RowFamily::Outcome)
            .count(),
        2
    );
    // The formats and hash domains cannot be used interchangeably.
    assert!(StructuralRecord::inspect(&bytes, inspection(bytes.len())).is_err());
    assert!(
        StructuralCheckpoint::inspect(&mutation_bytes, inspection(mutation_bytes.len())).is_err()
    );
}

#[test]
fn empty_genesis_retains_both_profiles_without_fabricating_metadata_or_outcomes() {
    for authored in [false, true] {
        let base = fixture::core();
        let core = if authored {
            Core::new_native_authored(
                base.state.ledger,
                RangeId(1711),
                base.limits,
                base.state.budget.clone(),
            )
            .unwrap()
        } else {
            base
        };
        let budget = core.native_budget();
        let plan = EncodingPlan::prepare(
            &core,
            EncodingLimits {
                rows: 0,
                ..limits()
            },
        )
        .unwrap();
        assert_eq!(plan.quote().rows, 0);
        assert_eq!(plan.quote().bytes, ROW_START + 32);
        let mut bytes = vec![0; plan.quote().bytes];
        plan.write_into(&mut bytes).unwrap();
        let checkpoint = StructuralCheckpoint::inspect(
            &bytes,
            InspectionLimits {
                rows: 0,
                row_bytes: 0,
                ..inspection(bytes.len())
            },
        )
        .unwrap();
        assert_eq!(checkpoint.header().prefix, SessionSeq(0));
        assert_eq!(checkpoint.header().rows, 0);
        assert_eq!(
            checkpoint.header().profile,
            if authored {
                NativeContentProfile::AuthoredV1
            } else {
                NativeContentProfile::ProjectionOnly
            }
        );
        let mut empty_short = checkpoint.rows(0).unwrap();
        assert!(matches!(
            empty_short.next(),
            Some(Err(CodecError::Capacity))
        ));
        assert!(empty_short.next().is_none());
        let mut empty = checkpoint.rows(1).unwrap();
        assert!(empty.next().is_none());
        assert_eq!(empty.visits_used(), 1);
        assert_eq!(core.native_budget(), budget);
        bytes[PREFIX_AT..PREFIX_AT + 8].copy_from_slice(&1u64.to_le_bytes());
        checksum(&mut bytes);
        assert!(matches!(
            StructuralCheckpoint::inspect(&bytes, inspection(bytes.len())),
            Err(CodecError::InvalidTag("checkpoint frame"))
        ));
    }
}

#[test]
fn exact_encoding_inspection_and_scan_budgets_refuse_one_short_and_wrong_output_sizes() {
    let core = populated();
    let plan = EncodingPlan::prepare(&core, limits()).unwrap();
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
            EncodingPlan::prepare(&core, short),
            Err(CodecError::Capacity)
        ));
    }
    let exact = EncodingPlan::prepare(
        &core,
        EncodingLimits {
            bytes: q.bytes,
            visits: q.visits,
            rows: q.rows,
        },
    )
    .unwrap();
    assert_eq!(exact.quote(), q);
    for size in [q.bytes - 1, q.bytes + 1] {
        let mut bytes = vec![0xa5; size];
        assert_eq!(exact.write_into(&mut bytes), Err(CodecError::Capacity));
        assert!(bytes.iter().all(|byte| *byte == 0xa5));
    }
    let bytes = encode(&core);
    let inspected = StructuralCheckpoint::inspect(&bytes, inspection(bytes.len())).unwrap();
    let iq = inspected.quote();
    let max_body = inspected
        .rows(usize::MAX)
        .unwrap()
        .map(|row| row.unwrap().body().len())
        .max()
        .unwrap();
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
            row_bytes: max_body - 1,
            ..inspection(bytes.len())
        },
    ] {
        assert!(matches!(
            StructuralCheckpoint::inspect(&bytes, short),
            Err(CodecError::Capacity)
        ));
    }
    assert!(
        StructuralCheckpoint::inspect(
            &bytes,
            InspectionLimits {
                bytes: bytes.len(),
                visits: iq.visits,
                rows: iq.rows,
                row_bytes: max_body,
            }
        )
        .is_ok()
    );
    let mut scan = inspected.rows(usize::MAX).unwrap();
    for row in scan.by_ref() {
        row.unwrap();
    }
    let visits = scan.visits_used();
    assert!(inspected.rows(visits).unwrap().all(|row| row.is_ok()));
    let mut short = inspected.rows(visits - 1).unwrap();
    assert!(short.by_ref().any(|row| row.is_err()));
    assert!(short.next().is_none());
    // Body borrows survive their temporary EncodedRow wrappers.
    let bodies = inspected
        .rows(visits)
        .unwrap()
        .map(|row| row.unwrap().body())
        .collect::<Vec<_>>();
    assert_eq!(bodies.len(), iq.rows);
}

#[test]
fn corrupted_headers_checksums_counts_and_every_truncation_refuse_without_import() {
    let core = populated();
    let bytes = encode(&core);
    for len in 0..bytes.len() {
        assert!(StructuralCheckpoint::inspect(&bytes[..len], inspection(bytes.len())).is_err());
    }
    for index in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[index] ^= 0x80;
        assert!(StructuralCheckpoint::inspect(&changed, inspection(changed.len())).is_err());
    }
    for (index, replacement) in [(0, 0), (8, 0), (10, 2)] {
        let mut changed = bytes.clone();
        changed[index] = replacement;
        checksum(&mut changed);
        assert!(StructuralCheckpoint::inspect(&changed, inspection(changed.len())).is_err());
    }
    for range in [11..43, PREFIX_AT..PREFIX_AT + 8, COUNT_AT..COUNT_AT + 8] {
        let mut changed = bytes.clone();
        changed[range].fill(0);
        checksum(&mut changed);
        assert!(matches!(
            StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
            Err(CodecError::InvalidTag("checkpoint frame"))
        ));
    }
    let mut changed = bytes.clone();
    changed[COUNT_AT..COUNT_AT + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    checksum(&mut changed);
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::Capacity)
    ));
    // A full-root count uses all eight bytes; it is not decoded as a u32 or
    // silently truncated to one before a row scan starts.
    #[cfg(target_pointer_width = "64")]
    {
        changed[COUNT_AT..COUNT_AT + 8].copy_from_slice(&(u64::from(u32::MAX) + 1).to_le_bytes());
        checksum(&mut changed);
        assert!(matches!(
            StructuralCheckpoint::inspect(
                &changed,
                InspectionLimits {
                    rows: usize::MAX,
                    visits: usize::MAX,
                    ..inspection(changed.len())
                }
            ),
            Err(CodecError::Truncated)
        ));
    }
    let mut changed = bytes.clone();
    let at = changed.len() - 32;
    changed.insert(at, 0);
    checksum(&mut changed);
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::TrailingBytes)
    ));
    let mut changed = bytes.clone();
    let end = changed.len() - 32;
    let mut wrong = blake3::Hasher::new_derive_key(super::super::HASH_DOMAIN);
    wrong.update(&changed[..end]);
    changed[end..].copy_from_slice(wrong.finalize().as_bytes());
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::InvalidTag("checkpoint checksum"))
    ));
}

#[test]
fn structural_checkpoint_rejects_deletions_duplicates_and_missing_accounting_rows_but_not_opaque_semantics()
 {
    let core = populated();
    let bytes = encode(&core);
    let spans = spans(&bytes);
    let (key, range) = spans.last().unwrap();
    let mut sizing = CountingSink::new(usize::MAX, usize::MAX);
    write_u8(&mut sizing, 0).unwrap();
    fixed::key(&mut sizing, *key).unwrap();
    write_count(&mut sizing, 0).unwrap();
    let mut deletion = vec![0; sizing.len()];
    let mut output = SliceSink::new(&mut deletion, usize::MAX);
    write_u8(&mut output, 0).unwrap();
    fixed::key(&mut output, *key).unwrap();
    write_count(&mut output, 0).unwrap();
    output.finish().unwrap();
    let changed = replace_row(&bytes, range.clone(), &deletion);
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::InvalidTag("checkpoint row"))
    ));
    deletion[0] = 1;
    let changed = replace_row(&bytes, range.clone(), &deletion);
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::InvalidTag("mutation body length"))
    ));
    let changed = replace_row(&bytes, spans[1].1.clone(), &bytes[spans[0].1.clone()]);
    assert!(matches!(
        StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
        Err(CodecError::InvalidTag("checkpoint row"))
    ));
    for remove_meta in [false, true] {
        let (_, range) = spans
            .iter()
            .find(|(key, _)| {
                if remove_meta {
                    *key == Key::Meta
                } else {
                    matches!(key, Key::Outcome(_))
                }
            })
            .unwrap();
        let mut changed = replace_row(&bytes, range.clone(), &[]);
        changed[COUNT_AT..COUNT_AT + 8].copy_from_slice(&((spans.len() - 1) as u64).to_le_bytes());
        checksum(&mut changed);
        assert!(matches!(
            StructuralCheckpoint::inspect(&changed, inspection(changed.len())),
            Err(CodecError::InvalidTag("checkpoint accounting rows"))
        ));
    }
    // An integrity-correct body with an impossible claims count stays opaque.
    // This explicitly prevents treating structural success as native acceptance.
    let (_, range) = spans.iter().find(|(key, _)| *key == Key::Meta).unwrap();
    let mut changed = bytes.clone();
    let mut cursor = Cursor::new(&bytes[range.clone()], range.len(), usize::MAX).unwrap();
    cursor.u8().unwrap();
    fixed::read_key(&mut cursor).unwrap();
    cursor.count(usize::MAX).unwrap();
    let body_start = range.start + cursor.offset();
    changed[body_start..body_start + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    checksum(&mut changed);
    assert!(StructuralCheckpoint::inspect(&changed, inspection(changed.len())).is_ok());
}

#[test]
fn encoding_refuses_non_genesis_empty_roots_and_missing_meta_or_outcome() {
    for row in [
        None,
        Some(Row::Meta(Meta::default())),
        Some(Row::Outcome(NativeOutcome {
            ledger: fixture::binding(1).ledger,
            invocation: fixture::request(fixture::ISSUER, 1).into(),
            sequence: SessionSeq(1),
            logical_time: 1,
            operation: NativeOperation::Post,
            intent: ContentHash([0; 32]),
            created: 0,
            changed: 0,
            definitions: 0,
            evaluations: 0,
            artifacts: 0,
            results: 0,
            receipts: 0,
            responses: 0,
            result_testaments: 0,
            events: 0,
        })),
    ] {
        let mut core = fixture::core();
        let initial = if row.is_none() { 1 } else { 0 };
        core.state.rows = crate::native::ranges::NativeRanges::single_from_store(
            RangeStore::new_partitioned(
                RangeId(1712),
                initial,
                core.limits.range,
                core.state.budget.clone(),
                page_partition,
            )
            .unwrap(),
        )
        .unwrap();
        if let Some(row) = row {
            let key = match &row {
                Row::Outcome(v) => Key::Outcome(v.invocation),
                _ => Key::Meta,
            };
            let prepared = core
                .state
                .rows
                .prepare_batch_with(
                    1,
                    vec![Change::Put(Entry::new(key, row, 0))],
                    BudgetLane::Completion,
                    |_| panic!("empty root cannot copy any old row"),
                )
                .unwrap();
            core.state.rows.publish(prepared).unwrap();
        }
        assert!(matches!(
            EncodingPlan::prepare(&core, limits()),
            Err(CodecError::InvalidTag(_))
        ));
    }
}

#[test]
fn caller_funded_output_encodes_under_total_ram_pressure_without_borrowing_new_capacity() {
    let core = populated();
    let budget = &core.state.budget;
    let before = budget.stats();
    let quote = EncodingPlan::prepare(&core, limits()).unwrap().quote();
    assert_eq!(budget.stats(), before);
    let allocation = budget
        .reserve(
            BudgetKind::Pending,
            BudgetLane::Completion,
            quote.bytes + ALLOCATOR_OVERHEAD,
        )
        .unwrap()
        .commit();
    let mut output = Vec::new();
    output.try_reserve_exact(quote.bytes).unwrap();
    assert_eq!(output.capacity(), quote.bytes);
    output.resize(quote.bytes, 0);
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let pressured = budget.stats();
    assert!(matches!(
        budget.reserve(BudgetKind::Pending, BudgetLane::Completion, 1),
        Err(MemoryError::Capacity { .. })
    ));
    let plan = EncodingPlan::prepare(&core, limits()).unwrap();
    assert_eq!(plan.quote(), quote);
    plan.write_into(&mut output).unwrap();
    let checkpoint = StructuralCheckpoint::inspect(&output, inspection(output.len())).unwrap();
    assert_eq!(checkpoint.header().prefix, core.native_sequence());
    assert!(checkpoint.rows(usize::MAX).unwrap().all(|row| row.is_ok()));
    assert_eq!(budget.stats(), pressured);
    drop(output);
    drop(allocation);
    drop(pressure);
    assert_eq!(budget.stats(), before);
}

#[test]
fn streaming_output_matches_exact_slice_bytes_hash_and_refuses_before_unfunded_callbacks() {
    let core = populated();
    let expected = encode(&core);
    let before = core.native_budget();
    let plan = EncodingPlan::prepare(&core, limits()).unwrap();
    let mut output = Vec::with_capacity(expected.len());
    let mut calls = 0;
    let hash = plan
        .write_with(|bytes| {
            calls += 1;
            output.extend_from_slice(bytes);
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
    assert_eq!(hash, plan.quote().hash);
    assert_eq!(output, expected);
    assert!(calls > 10);
    assert_eq!(core.native_budget(), before);
    // Corrupted private plans prove that codec limits are debited before any
    // callback. Public callers only receive immutable measured plans.
    for quote in [
        EncodingQuote {
            bytes: 0,
            ..plan.quote()
        },
        EncodingQuote {
            visits: 0,
            ..plan.quote()
        },
    ] {
        let refused = EncodingPlan { core: &core, quote }.write_with(
            |_| -> Result<(), std::convert::Infallible> {
                panic!("insufficient codec allowance must refuse before output")
            },
        );
        assert_eq!(refused, Err(WriteError::Codec(CodecError::Capacity)));
    }
    let failure = plan
        .write_with(|_| Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)))
        .unwrap_err();
    assert_eq!(
        std::error::Error::source(&failure)
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(
        failure
            .to_string()
            .starts_with("checkpoint output failed: ")
    );
    let failure = WriteError::<std::io::Error>::Codec(CodecError::Capacity);
    assert_eq!(
        std::error::Error::source(&failure)
            .unwrap()
            .downcast_ref::<CodecError>(),
        Some(&CodecError::Capacity)
    );
    assert!(
        failure
            .to_string()
            .starts_with("checkpoint encoding failed: ")
    );
}

#[test]
fn fixed_reusable_stream_buffer_flushes_large_chunks_under_total_ram_pressure() {
    let core = populated();
    let expected = encode(&core);
    let budget = &core.state.budget;
    let before = budget.stats();
    let allocation = budget
        .reserve(BudgetKind::Pending, BudgetLane::Completion, 31)
        .unwrap();
    let mut buffer = [0u8; 31];
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let pressured = budget.stats();
    let plan = EncodingPlan::prepare(&core, limits()).unwrap();
    let mut consumed = 0;
    let mut buffered = 0;
    let mut flushes = 0;
    let mut split_large = false;
    let mut sink_hash = blake3::Hasher::new();
    let hash = plan
        .write_with(|mut bytes| {
            split_large |= bytes.len() > buffer.len();
            while !bytes.is_empty() {
                let copied = (buffer.len() - buffered).min(bytes.len());
                buffer[buffered..buffered + copied].copy_from_slice(&bytes[..copied]);
                buffered += copied;
                bytes = &bytes[copied..];
                if buffered == buffer.len() {
                    sink_hash.update(&buffer);
                    consumed += buffered;
                    buffered = 0;
                    flushes += 1;
                }
            }
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap();
    // Final flushing belongs to the caller, separately from encoding success.
    if buffered != 0 {
        sink_hash.update(&buffer[..buffered]);
        consumed += buffered;
    }
    assert!(split_large);
    assert!(flushes > 1);
    assert_eq!(consumed, expected.len());
    assert_eq!(sink_hash.finalize(), blake3::hash(&expected));
    assert_eq!(hash, plan.quote().hash);
    assert_eq!(budget.stats(), pressured);
    drop(pressure);
    drop(allocation);
    assert_eq!(budget.stats(), before);
}

#[test]
fn streaming_preserves_early_middle_and_trailer_output_errors_and_allows_exact_retry() {
    #[derive(Debug)]
    struct OutputFailure {
        message: String,
    }

    let core = populated();
    let expected = encode(&core);
    let budget = &core.state.budget;
    let before = budget.stats();
    let plan = EncodingPlan::prepare(&core, limits()).unwrap();
    let mut total_calls = 0;
    plan.write_with(|_| {
        total_calls += 1;
        Ok::<(), OutputFailure>(())
    })
    .unwrap();
    let pressure = budget
        .reserve(
            BudgetKind::Payload,
            BudgetLane::Completion,
            budget.stats().limit - budget.stats().used,
        )
        .unwrap();
    let pressured = budget.stats();
    for fail_at in [1, total_calls / 2, total_calls] {
        let mut error = Some(OutputFailure {
            message: format!("output failed at {fail_at}"),
        });
        let original_pointer = error.as_ref().unwrap().message.as_ptr();
        let mut calls = 0;
        let mut written = 0;
        let result = plan.write_with(|bytes| {
            calls += 1;
            assert_eq!(bytes, &expected[written..written + bytes.len()]);
            if calls == fail_at {
                // A writer is allowed to have emitted part of this final chunk
                // when it reports failure; no further callback may occur.
                written += bytes.len() / 2;
                return Err(error.take().unwrap());
            }
            written += bytes.len();
            Ok(())
        });
        match result {
            Err(WriteError::Output(error)) => {
                assert_eq!(error.message.as_ptr(), original_pointer);
                assert_eq!(error.message, format!("output failed at {fail_at}"));
            }
            other => panic!("expected original output error, got {other:?}"),
        }
        assert_eq!(calls, fail_at);
        assert!(written < expected.len());
        assert_eq!(budget.stats(), pressured);
        assert_eq!(core.native_sequence(), SessionSeq(1));
        // Retry starts from a fresh sink position; it never resumes a torn
        // output prefix using the completed plan's hash as a durability token.
        let mut written = 0;
        let hash = plan
            .write_with(|bytes| {
                assert_eq!(bytes, &expected[written..written + bytes.len()]);
                written += bytes.len();
                Ok::<(), OutputFailure>(())
            })
            .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(hash, plan.quote().hash);
        assert_eq!(budget.stats(), pressured);
    }
    drop(pressure);
    assert_eq!(budget.stats(), before);
}
