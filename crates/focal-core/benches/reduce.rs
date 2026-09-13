// Dependency-free bench: a plain `harness = false` binary, no criterion (the
// workspace's deny.toml forbids unmaintained/unvetted deps). It reports the
// per-mutation reduce cost so a regression is visible; a measurement tool, not
// a pass/fail test.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Performance bench for the domain reduce path — `Core::prepare` +
//! `Core::apply_serial`, the serial oracle every mutation passes through (F05;
//! P-gate, R11 §5). After negotiating a request epoch (as any session does), it
//! admits and applies native claim creations. The fixture mirrors the in-crate
//! `new_claim` builder; if it were malformed, `prepare` would refuse it and this
//! bench would panic rather than report a misleading number.
use focal_core::Core;
use focal_model::{
    ActionType, AuthenticatedInput, AuthorityContext, Cause, ClaimContent, ClaimId, Command,
    LedgerId, Limits, NewClaim, NewValidation, OccurrenceId, ParticipantId, Relation, RelationKind,
    RelationTarget, RequestEpoch, RequestId, RequirementRef, RootCommandId, SCHEMA_MAJOR,
    SessionId, SessionSeq, TenantId, ValidationContent, ValidationId, ValidationKind,
    ValidationMode, ValidationPhase,
};
use std::collections::BTreeSet;
use std::time::Instant;

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const WORKER: ParticipantId = ParticipantId::from_u128(2);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
/// Claim request/object ids start here, clear of the negotiation ids above.
const CLAIM_BASE: u128 = 1000;

fn ledger() -> LedgerId {
    LedgerId {
        tenant: TenantId::from_u128(10),
        session: SessionId::from_u128(20),
    }
}

fn new_claim(id: u128) -> NewClaim {
    let cid = ClaimId::from_u128(id);
    let vid = ValidationId::from_u128(id + 100_000);
    let v = ValidationContent {
        ledger: ledger(),
        schema: SCHEMA_MAJOR,
        claim: cid,
        kind: ValidationKind::Receipt,
        phase: ValidationPhase::WholeWork,
        mode: ValidationMode::Required,
        description: "acknowledged receipt".into(),
        quality_bar: None,
        evaluator: EVALUATOR,
        handlers: Vec::new(),
        evidence_schemas: BTreeSet::new(),
        contributed_by: BTreeSet::from([ISSUER]),
        policy_revision: 1,
    };
    NewClaim {
        id: cid,
        content: ClaimContent {
            ledger: ledger(),
            schema: SCHEMA_MAJOR,
            occurrence: OccurrenceId::from_u128(id),
            description: "return evidence".into(),
            relations: BTreeSet::from([
                Relation {
                    kind: RelationKind::Issuer,
                    target: RelationTarget::Participant(ISSUER),
                },
                Relation {
                    kind: RelationKind::Subject,
                    target: RelationTarget::Participant(WORKER),
                },
                Relation {
                    kind: RelationKind::ClaimAction,
                    target: RelationTarget::Action(ActionType::Work),
                },
                Relation {
                    kind: RelationKind::CausedBy,
                    target: RelationTarget::Root(RootCommandId::from_u128(30)),
                },
            ]),
            scopes: BTreeSet::new(),
            requirements: vec![RequirementRef {
                id: vid,
                specification: v.specification_hash().unwrap(),
            }],
            deadline: None,
        },
        validations: vec![NewValidation {
            id: vid,
            content: v,
        }],
    }
}

fn input(n: u128, principal: ParticipantId, command: Command) -> AuthenticatedInput {
    AuthenticatedInput {
        ledger: ledger(),
        principal,
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(n),
        expected_revision: None,
        authority: AuthorityContext {
            runtime: true,
            cause: Cause::Root(RootCommandId::from_u128(30)),
            policy_revision: 1,
            logical_time: 1000,
            evidence: Vec::new(),
        },
        command,
    }
}

fn apply_one(core: &mut Core, n: u128, principal: ParticipantId, command: Command) {
    let prepared = core
        .prepare(&input(n, principal, command))
        .unwrap_or_else(|outcome| panic!("prepare refused (request {n}): {outcome:?}"));
    let seq = SessionSeq(core.sequence().0 + 1);
    core.apply_serial(seq, prepared).unwrap();
}

/// A core with request epoch 1 negotiated for each participant, ready to admit
/// their mutations — the state every session reaches before its first write.
fn negotiated_core() -> Core {
    let mut core = Core::new(ledger(), Limits::default());
    for (i, principal) in [ISSUER, WORKER, EVALUATOR].into_iter().enumerate() {
        apply_one(
            &mut core,
            (i + 1) as u128,
            principal,
            Command::NegotiateEpoch {
                epoch: RequestEpoch(1),
            },
        );
    }
    core
}

fn generate(i: u64) -> (u128, Command) {
    let id = CLAIM_BASE + i as u128;
    (
        id,
        Command::GenerateClaim {
            claim: new_claim(id),
        },
    )
}

fn main() {
    let claims: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    println!("focal-core domain reduce bench ({claims} claim creations)\n");

    // prepare only: admission + validation cost, on a negotiated core. `prepare`
    // is immutable, so the same base state answers each call; a refusal panics
    // rather than mismeasuring.
    {
        let core = negotiated_core();
        let (id0, cmd0) = generate(0);
        if let Err(outcome) = core.prepare(&input(id0, ISSUER, cmd0)) {
            panic!("reduce fixture rejected by prepare: {outcome:?}");
        }
        let start = Instant::now();
        for i in 0..claims {
            let (id, command) = generate(i);
            let prepared = core.prepare(&input(id, ISSUER, command)).unwrap();
            std::hint::black_box(&prepared);
        }
        let elapsed = start.elapsed();
        println!(
            "prepare GenerateClaim              {:8.1} ns/op  {:>11.0} ops/s",
            elapsed.as_nanos() as f64 / claims as f64,
            claims as f64 / elapsed.as_secs_f64()
        );
    }

    // apply_serial: the reduce onto committed state, which grows as claims
    // accumulate. Timed (apply only, excluding prepare) over the first and
    // second half of the run so the one-session scaling is visible: if the
    // second half is dearer per op, apply cost rises with session size — the
    // ceiling the plan measures rather than hides.
    {
        let mut core = negotiated_core();
        let half = claims / 2;
        let apply_range = |core: &mut Core, from: u64, to: u64| -> std::time::Duration {
            let mut total = std::time::Duration::ZERO;
            for i in from..to {
                let (id, command) = generate(i);
                let prepared = core.prepare(&input(id, ISSUER, command)).unwrap();
                let seq = SessionSeq(core.sequence().0 + 1);
                let start = Instant::now();
                let result = core.apply_serial(seq, prepared).unwrap();
                total += start.elapsed();
                std::hint::black_box(&result);
            }
            total
        };
        let low = apply_range(&mut core, 0, half);
        let high = apply_range(&mut core, half, claims);
        println!(
            "apply GenerateClaim, first {half:>6}  {:10.1} ns/op",
            low.as_nanos() as f64 / half.max(1) as f64
        );
        println!(
            "apply GenerateClaim, next  {:>6}  {:10.1} ns/op  (session already holds {half} claims)",
            claims - half,
            high.as_nanos() as f64 / (claims - half).max(1) as f64
        );
    }
}
