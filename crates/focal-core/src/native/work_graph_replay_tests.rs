//! Actual testimony and validation precede local completion; a later correction
//! settles its monitor. The new successor is a disjoint capture seed.
use super::*;
use crate::native::record_codec::{self as codec, recovery, replay};
use focal_model::lifecycle::{claim::ClaimTerminalCut, succession};
use focal_model::{Cause, Deadline, MonitorId, ObjectRef, RootCommandId, TimerId};

fn encode(candidate: &NativePrepared) -> Vec<u8> {
    let plan = codec::EncodingPlan::prepare(
        candidate,
        codec::EncodingLimits {
            bytes: 32 << 20,
            visits: 1_000_000_000,
            rows: 100_000,
        },
    )
    .unwrap();
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes).unwrap();
    bytes
}
fn inspect(bytes: &[u8]) -> codec::StructuralRecord<'_> {
    codec::StructuralRecord::inspect(
        bytes,
        codec::InspectionLimits {
            bytes: bytes.len(),
            visits: 1_000_000_000,
            rows: 100_000,
            row_bytes: 32 << 20,
        },
    )
    .unwrap()
}

#[test]
fn locally_complete_claim_releases_against_full_disjoint_correction_capture_on_replay() {
    let mut f = checked_slot_fixture_with_visits(ValidationMode::Required, 2 * 1024 * 1024);
    f.commit(ISSUER, reports::creation(2, 2, &[], None).command);
    f.commit(
        ISSUER,
        NativeCommand::RegisterMonitor {
            expected: f.claim(),
            receipt: f
                .owner
                .effective()
                .claim(CLAIM)
                .unwrap()
                .receipt()
                .map(|row| row.fence),
            id: MonitorId::from_u128(90_001),
            roots: vec![WaitPredicate::Terminal(ClaimId::from_u128(2))],
            deadline: Deadline {
                timer: TimerId::from_u128(90_001),
                generation: 1,
                at: 10_000,
            },
        },
    );
    complete_response(&mut f, 900, 801);
    let key = EvaluationKey {
        claim: CLAIM,
        validation: ValidationId::from_u128(301),
        target: EvaluationTarget::Work {
            response: RESPONSE,
            slot: 0,
            artifact: ArtifactId::from_u128(801),
        },
        generation: 1,
    };
    let expected = f.owner.effective().evaluation(key).unwrap().binding();
    f.commit(
        EVALUATOR,
        NativeCommand::BeginWork {
            claim: f.claim(),
            key,
            expected,
        },
    );
    let (actor, command) = report(&f, key, 90_002, VerdictValue::Pass);
    f.commit(actor, command);
    assert!(f.owner.committed().claim(CLAIM).unwrap().local_complete());
    assert_eq!(
        f.owner.committed().claim(CLAIM).unwrap().status(),
        ClaimStatus::Validating
    );
    let core = f.owner.core_for_test();
    let checkpoint = recovery::tests::encode(core);
    let limits = recovery::tests::limits(core.limits);
    let source = core.state.rows.id();
    let mut restored = recovery::restore(
        &recovery::tests::inspect(&checkpoint),
        RangeId(90_003),
        limits,
        recovery::tests::budget(),
        &f.store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    let restored_checkpoint = recovery::tests::encode(&restored);
    let NativeCommand::Create {
        mut claims,
        declarations,
    } = reports::creation(3, 3, &[], None).command
    else {
        panic!("create")
    };
    claims[0].definition.lineage = succession::Lineage::new(
        binding(3),
        Cause::Root(RootCommandId::from_u128(3)),
        &[succession::Correction {
            kind: succession::CorrectionKind::Supersedes,
            predecessor: ObjectRef::claim(binding(3).ledger, ClaimId::from_u128(2)),
        }],
        1,
    )
    .unwrap();
    let NativeStaging::Prepared { candidate, outcome } = f
        .stage(
            ISSUER,
            NativeCommand::Create {
                claims,
                declarations,
            },
        )
        .unwrap()
    else {
        panic!("correction")
    };
    let prepared = f.owner.prepared_candidate(candidate).unwrap();
    assert_eq!(
        prepared.claim(CLAIM).unwrap().status(),
        ClaimStatus::Satisfied
    );
    let Some(ClaimTerminalCut::Explicit(cut)) = prepared.claim(CLAIM).unwrap().terminal_cut()
    else {
        panic!("release cut")
    };
    let bytes = encode(prepared);
    // The incomplete component has identical satisfaction truth but a different
    // release fingerprint. The immutable disjoint successor must be included.
    let mut bindings: Vec<Binding> = Vec::new();
    let mut boundary = None;
    for ordinal in 0..outcome.events {
        let event = f
            .owner
            .effective()
            .event(outcome.sequence, ordinal)
            .unwrap();
        if let NativeFact::Claim(value) = event.fact {
            if value.kind == NativeEventKind::Satisfied {
                boundary = Some(value.graph.unwrap().before_ordinal);
                break;
            }
            if value.after.object.0 != ClaimId::from_u128(3).0 {
                if let Some(existing) = bindings
                    .iter_mut()
                    .find(|binding| binding.object == value.after.object)
                {
                    *existing = value.after;
                } else {
                    bindings.push(value.after);
                }
            }
        }
    }
    assert!(boundary.is_some());
    assert_eq!(bindings.len(), 2);
    bindings.sort_unstable_by_key(|binding| binding.object);
    let mut digest = blake3::Hasher::new();
    digest.update(b"focal.lifecycle.graph-release.v1\0");
    for binding in bindings {
        digest.update(&binding.ledger.tenant.0);
        digest.update(&binding.ledger.session.0);
        digest.update(&binding.object.0);
        digest.update(&binding.content.0);
        digest.update(&binding.revision.0.to_le_bytes());
    }
    let incomplete = *digest.finalize().as_bytes();
    assert_ne!(incomplete, cut.cause.0);
    let mut corrupted = bytes.clone();
    let record = inspect(&bytes);
    let row = record
        .rows(usize::MAX)
        .unwrap()
        .map(Result::unwrap)
        .find(|row| {
            row.family() == codec::RowFamily::Claim
                && row.body().get(32..48) == Some(CLAIM.0.as_slice())
        })
        .unwrap();
    let relative = row
        .body()
        .windows(32)
        .position(|window| window == cut.cause.0)
        .unwrap();
    let offset = row.body().as_ptr() as usize - bytes.as_ptr() as usize + relative;
    corrupted[offset..offset + 32].copy_from_slice(&incomplete);
    let end = corrupted.len() - 32;
    let mut checksum = blake3::Hasher::new_derive_key("focal.native.record.v2");
    checksum.update(&corrupted[..end]);
    corrupted[end..].copy_from_slice(checksum.finalize().as_bytes());
    let original_budget = restored.native_budget();
    assert!(
        replay::prepare(
            &restored,
            &inspect(&corrupted),
            source,
            recovery::tests::limits(restored.limits),
            &f.store,
            &BuiltinNativeSchemas
        )
        .is_err()
    );
    assert_eq!(restored.native_budget(), original_budget);
    assert_eq!(recovery::tests::encode(&restored), restored_checkpoint);
    let replayed = replay::prepare(
        &restored,
        &record,
        source,
        recovery::tests::limits(restored.limits),
        &f.store,
        &BuiltinNativeSchemas,
    )
    .unwrap();
    restored.publish_native(replayed).unwrap();
    f.owner.publish_after_durable(candidate).unwrap();
    recovery::tests::compare(f.owner.core_for_test(), &restored);
}
