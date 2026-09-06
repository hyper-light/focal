use super::*;
use crate::{ArtifactId, SessionId, TenantId, TimerId, ValidatorId};
use serde::de::DeserializeOwned;
use std::fmt::Debug;

fn receipt() -> Receipt {
    Receipt {
        fence: ReceiptFence {
            receipt: crate::ReceiptId([1; 16]),
            epoch: 300,
        },
        holder: ParticipantId([2; 16]),
        acquired: SessionSeq(128),
    }
}

fn run_id() -> ValidationRunId {
    ValidationRunId {
        validation: ValidationId([3; 16]),
        target_hash: ContentHash([4; 32]),
        phase: ValidationPhase::Increment,
        epoch: 128,
    }
}

fn artifact() -> ArtifactRef {
    ArtifactRef {
        id: ArtifactId([5; 16]),
        hash: ContentHash([6; 32]),
    }
}

fn verdict() -> VerdictRecord {
    VerdictRecord {
        run: run_id(),
        evaluator: ParticipantId([7; 16]),
        handler: HandlerRef {
            id: ValidatorId([8; 16]),
            version: ContentHash([9; 32]),
            agentic: true,
        },
        attempt: 16_384,
        manifest: ContentHash([10; 32]),
        value: VerdictValue::Error,
        evidence: vec![artifact()],
    }
}

fn claim() -> ClaimLifecycle {
    ClaimLifecycle {
        status: ClaimStatus::ValidationFailed,
        revision: ObjectRevision(257),
        created: SessionSeq(1),
        history: vec![
            StatusFact {
                status: ClaimStatus::Generated,
                sequence: SessionSeq(1),
            },
            StatusFact {
                status: ClaimStatus::ValidationFailed,
                sequence: SessionSeq(300),
            },
        ],
        receipt: Some(receipt()),
        evidence_set: Some(EvidenceSetId([11; 16])),
        testament: Some(TestamentId([12; 16])),
        local_complete: true,
        released: true,
        terminal_witness: Some(ClaimId([13; 16])),
    }
}

fn empty_claim() -> ClaimLifecycle {
    ClaimLifecycle {
        status: ClaimStatus::Generated,
        revision: ObjectRevision(0),
        created: SessionSeq(0),
        history: vec![],
        receipt: None,
        evidence_set: None,
        testament: None,
        local_complete: false,
        released: false,
        terminal_witness: None,
    }
}

fn run() -> ValidationRun {
    ValidationRun {
        id: run_id(),
        claim: ClaimId([14; 16]),
        evaluator: ParticipantId([7; 16]),
        manifest: ContentHash([10; 32]),
        handler_index: 16_384,
        quality_phase: true,
        attempts: vec![verdict()],
        final_verdict: Some(VerdictValue::Error),
    }
}

fn evidence() -> EvidenceSet {
    EvidenceSet {
        id: EvidenceSetId([11; 16]),
        claim: ClaimId([14; 16]),
        receipt: receipt().fence,
        artifacts: vec![artifact()],
        closed: true,
    }
}

fn monitor() -> Monitor {
    Monitor {
        id: MonitorId([15; 16]),
        owner: ClaimId([14; 16]),
        roots: BTreeSet::from([
            WaitPredicate::Released(ClaimId([16; 16])),
            WaitPredicate::Satisfied(ClaimId([17; 16])),
            WaitPredicate::Terminal(ClaimId([18; 16])),
        ]),
        deadline: Deadline {
            timer: TimerId([19; 16]),
            // Historically representable; a decoder must not run today's
            // monitor builder, which requires a nonzero generation.
            generation: 0,
            at: u64::MAX,
        },
        registered: SessionSeq(0),
        released: Some(SessionSeq(0)),
    }
}

fn key() -> RequestKey {
    RequestKey {
        principal: ParticipantId([20; 16]),
        epoch: RequestEpoch(u64::MAX),
        id: RequestId([21; 16]),
    }
}

fn mutation(outcome: CommandResult) -> MutationReceipt {
    MutationReceipt {
        ledger: LedgerId {
            tenant: TenantId([22; 16]),
            session: SessionId([23; 16]),
        },
        key: key(),
        sequence: SessionSeq(16_384),
        command_hash: ContentHash([24; 32]),
        outcome,
    }
}

fn results() -> Vec<CommandResult> {
    vec![
        CommandResult::EpochAdmitted(RequestEpoch(128)),
        CommandResult::EpochFloorAdvanced(RequestEpoch(u64::MAX)),
        CommandResult::Generated(vec![ClaimId([25; 16]), ClaimId([26; 16])]),
        CommandResult::Existing(vec![]),
        CommandResult::Claim {
            claim: ClaimId([14; 16]),
            status: ClaimStatus::ValidationFailed,
        },
        CommandResult::Receipt {
            claim: ClaimId([14; 16]),
            fence: receipt().fence,
        },
        CommandResult::EvidenceSet(EvidenceSetId([11; 16])),
        CommandResult::Artifact(artifact()),
        CommandResult::Testament(TestamentId([12; 16])),
        CommandResult::Validation(run_id()),
        CommandResult::Monitor(MonitorId([15; 16])),
        CommandResult::Noop,
    ]
}

fn bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn assert_v1<T>(value: T, original_hex: &str)
where
    T: V1 + Serialize + DeserializeOwned + PartialEq + Debug,
{
    let original = bytes(original_hex);
    assert_eq!(postcard::to_allocvec(&value).unwrap(), original);
    assert_eq!(postcard::to_allocvec(&Ref(&value)).unwrap(), original);
    assert_eq!(postcard::from_bytes::<T>(&original).unwrap(), value);
    let (decoded, remainder) = postcard::take_from_bytes::<Value<T>>(&original).unwrap();
    assert!(remainder.is_empty());
    assert_eq!(decoded.0, value);
    for cut in 0..original.len() {
        assert!(postcard::from_bytes::<Value<T>>(&original[..cut]).is_err());
    }
}

// Captured on 2026-09-06 through the original live model Serialize derives,
// before these nested codecs were integrated into checkpoint persistence. The
// literal bytes pin historical field order independently of both round trips.
const ORIGINAL_RECEIPT: &str =
    "01010101010101010101010101010101ac02020202020202020202020202020202028001";
const ORIGINAL_STATUS_FACT: &str = "14ffffffffffffffffff01";
const ORIGINAL_CLAIM: &str = concat!(
    "0d8102010201010dac020101010101010101010101010101010101ac0202020202020202020202020202020202800101",
    "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b010c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0101010d0d0d0d0d0d0d0d0d0d0d0d",
    "0d0d0d0d",
);
const ORIGINAL_EMPTY_CLAIM: &str = "01000000000000000000";
const ORIGINAL_TESTAMENT_NONE: &str = "800100";
const ORIGINAL_TESTAMENT_SOME: &str = "800101ac02";
const ORIGINAL_ARTIFACT_LIFECYCLE: &str = "00ffffffffffffffffff01";
const ORIGINAL_VALIDATION_LIFECYCLE: &str = "ac0200";
const ORIGINAL_RUN_ID: &str = concat!(
    "030303030303030303030303030303030404040404040404040404040404040404040404040404040404040404040404",
    "028001",
);
const ORIGINAL_VERDICT: &str = concat!(
    "030303030303030303030303030303030404040404040404040404040404040404040404040404040404040404040404",
    "028001070707070707070707070707070707070808080808080808080808080808080809090909090909090909090909",
    "09090909090909090909090909090909090909018080010a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
    "0a0a0a0a0a0a0a0401050505050505050505050505050505050606060606060606060606060606060606060606060606",
    "060606060606060606",
);
const ORIGINAL_RUN: &str = concat!(
    "030303030303030303030303030303030404040404040404040404040404040404040404040404040404040404040404",
    "0280010e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e070707070707070707070707070707070a0a0a0a0a0a0a0a0a0a0a0a0a",
    "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a8080010101030303030303030303030303030303030404040404040404",
    "040404040404040404040404040404040404040404040404028001070707070707070707070707070707070808080808",
    "08080808080808080808080909090909090909090909090909090909090909090909090909090909090909018080010a",
    "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0401050505050505050505050505050505",
    "0506060606060606060606060606060606060606060606060606060606060606060104",
);
const ORIGINAL_PENDING_RUN: &str = concat!(
    "030303030303030303030303030303030404040404040404040404040404040404040404040404040404040404040404",
    "0280010e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e070707070707070707070707070707070a0a0a0a0a0a0a0a0a0a0a0a0a",
    "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a808001000000",
);
const ORIGINAL_EVIDENCE: &str = concat!(
    "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e01010101010101010101010101010101",
    "ac0201050505050505050505050505050505050606060606060606060606060606060606060606060606060606060606",
    "06060601",
);
const ORIGINAL_OPEN_EVIDENCE: &str = concat!(
    "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e01010101010101010101010101010101",
    "ac020000",
);
const ORIGINAL_MONITOR: &str = concat!(
    "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e03001111111111111111111111111111",
    "111101121212121212121212121212121212120210101010101010101010101010101010131313131313131313131313",
    "1313131300ffffffffffffffffff01000100",
);
const ORIGINAL_WAITING_MONITOR: &str = concat!(
    "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e00131313131313131313131313131313",
    "1300ffffffffffffffffff010000",
);
const ORIGINAL_KEY: &str =
    "14141414141414141414141414141414ffffffffffffffffff0115151515151515151515151515151515";
const ORIGINAL_MUTATION: &str = concat!(
    "161616161616161616161616161616161717171717171717171717171717171714141414141414141414141414141414",
    "ffffffffffffffffff011515151515151515151515151515151580800118181818181818181818181818181818181818",
    "18181818181818181818181818050e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e01010101010101010101010101010101ac02",
);
const ORIGINAL_RESULT_0: &str = "008001";
const ORIGINAL_RESULT_1: &str = "01ffffffffffffffffff01";
const ORIGINAL_RESULT_2: &str =
    "0202191919191919191919191919191919191a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";
const ORIGINAL_RESULT_3: &str = "0300";
const ORIGINAL_RESULT_4: &str = "040e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0d";
const ORIGINAL_RESULT_5: &str =
    "050e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e01010101010101010101010101010101ac02";
const ORIGINAL_RESULT_6: &str = "060b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";
const ORIGINAL_RESULT_7: &str = concat!(
    "070505050505050505050505050505050506060606060606060606060606060606060606060606060606060606060606",
    "06",
);
const ORIGINAL_RESULT_8: &str = "080c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c";
const ORIGINAL_RESULT_9: &str = concat!(
    "090303030303030303030303030303030304040404040404040404040404040404040404040404040404040404040404",
    "04028001",
);
const ORIGINAL_RESULT_10: &str = "0a0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";
const ORIGINAL_RESULT_11: &str = "0b";
const ORIGINAL_PREDICATE_0: &str = "001b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b";
const ORIGINAL_PREDICATE_1: &str = "011c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c";
const ORIGINAL_PREDICATE_2: &str = "021d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d";
const ORIGINAL_RECEIPT_PREFIX: &str = concat!(
    "161616161616161616161616161616161717171717171717171717171717171714141414141414141414141414141414",
    "ffffffffffffffffff011515151515151515151515151515151580800118181818181818181818181818181818181818",
    "18181818181818181818181818",
);

#[test]
fn lifecycle_rows_preserve_original_optional_fields_and_historical_values() {
    assert_v1(receipt(), ORIGINAL_RECEIPT);
    assert_v1(
        StatusFact {
            status: ClaimStatus::Deadlocked,
            sequence: SessionSeq(u64::MAX),
        },
        ORIGINAL_STATUS_FACT,
    );
    // This representable historical combination is deliberately not admitted as
    // newly authored state; decoding must preserve it rather than normalize it.
    assert_v1(claim(), ORIGINAL_CLAIM);
    assert_v1(empty_claim(), ORIGINAL_EMPTY_CLAIM);
    assert_v1(
        TestamentLifecycle {
            created: SessionSeq(128),
            acknowledged: None,
        },
        ORIGINAL_TESTAMENT_NONE,
    );
    assert_v1(
        TestamentLifecycle {
            created: SessionSeq(128),
            acknowledged: Some(SessionSeq(300)),
        },
        ORIGINAL_TESTAMENT_SOME,
    );
    assert_v1(
        ArtifactLifecycle {
            created: SessionSeq(0),
            custody_revision: u64::MAX,
        },
        ORIGINAL_ARTIFACT_LIFECYCLE,
    );
    assert_v1(
        ValidationLifecycle {
            created: SessionSeq(300),
            latest_epoch: 0,
        },
        ORIGINAL_VALIDATION_LIFECYCLE,
    );
}

#[test]
fn evaluation_rows_preserve_exact_target_attempt_proof_and_pending_state() {
    assert_v1(run_id(), ORIGINAL_RUN_ID);
    assert_v1(verdict(), ORIGINAL_VERDICT);
    assert_v1(run(), ORIGINAL_RUN);
    let mut pending = run();
    pending.attempts.clear();
    pending.final_verdict = None;
    pending.quality_phase = false;
    assert_v1(pending, ORIGINAL_PENDING_RUN);
    assert_v1(evidence(), ORIGINAL_EVIDENCE);
    let mut open = evidence();
    open.artifacts.clear();
    open.closed = false;
    assert_v1(open, ORIGINAL_OPEN_EVIDENCE);
}

#[test]
fn monitors_preserve_wait_ordinals_and_zero_generation_history() {
    for (predicate, original) in [
        (
            WaitPredicate::Satisfied(ClaimId([27; 16])),
            ORIGINAL_PREDICATE_0,
        ),
        (
            WaitPredicate::Terminal(ClaimId([28; 16])),
            ORIGINAL_PREDICATE_1,
        ),
        (
            WaitPredicate::Released(ClaimId([29; 16])),
            ORIGINAL_PREDICATE_2,
        ),
    ] {
        assert_v1(predicate, original);
    }
    assert_v1(monitor(), ORIGINAL_MONITOR);
    let mut waiting = monitor();
    waiting.roots.clear();
    waiting.released = None;
    assert_v1(waiting, ORIGINAL_WAITING_MONITOR);
    assert!(postcard::from_bytes::<Value<WaitPredicate>>(&[3]).is_err());
    assert!(postcard::from_bytes::<Value<WaitPredicate>>(&[128]).is_err());
}

#[test]
fn all_twelve_results_and_retained_receipts_keep_the_original_bytes() {
    assert_v1(key(), ORIGINAL_KEY);
    assert_v1(
        mutation(CommandResult::Receipt {
            claim: ClaimId([14; 16]),
            fence: receipt().fence,
        }),
        ORIGINAL_MUTATION,
    );
    let originals = [
        ORIGINAL_RESULT_0,
        ORIGINAL_RESULT_1,
        ORIGINAL_RESULT_2,
        ORIGINAL_RESULT_3,
        ORIGINAL_RESULT_4,
        ORIGINAL_RESULT_5,
        ORIGINAL_RESULT_6,
        ORIGINAL_RESULT_7,
        ORIGINAL_RESULT_8,
        ORIGINAL_RESULT_9,
        ORIGINAL_RESULT_10,
        ORIGINAL_RESULT_11,
    ];
    let results = results();
    assert_eq!(results.len(), originals.len());
    for (result, original) in results.into_iter().zip(originals) {
        assert_v1(result.clone(), original);
        assert_v1(
            mutation(result),
            &format!("{ORIGINAL_RECEIPT_PREFIX}{original}"),
        );
    }
    assert!(postcard::from_bytes::<Value<CommandResult>>(&[12]).is_err());
    assert!(postcard::from_bytes::<Value<CommandResult>>(&[128]).is_err());
}
