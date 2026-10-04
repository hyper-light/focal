// Dependency-free allocation-count bench: a plain `harness = false` binary
// with a counting global allocator (focal-memory/benches/support/alloc_count.rs).
// It reports heap allocations, reallocations, bytes and peak growth per
// reduce operation, which are stable under machine load; no wall-clock.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::disallowed_macros,
    clippy::cast_precision_loss,
    clippy::arithmetic_side_effects
)]
//! Allocation counts for the domain reduce (`Core::prepare` + `Core::apply`,
//! the same fixture as benches/reduce.rs). Inputs are built before the gate
//! opens; `prepare` is measured alone, and `apply` is measured with its
//! `prepare` outside the gate, over the first and second half of the run so
//! any growth of the per-op allocation count with session size is visible.
#[path = "../../focal-memory/benches/support/alloc_count.rs"]
mod alloc_count;

use alloc_count::Meter;
use focal_core::Core;
use focal_model::{
    ActionType, AuthenticatedInput, AuthorityContext, Cause, ClaimContent, ClaimId, Command,
    LedgerId, Limits, NewClaim, NewValidation, OccurrenceId, ParticipantId, Relation, RelationKind,
    RelationTarget, RequestEpoch, RequestId, RequirementRef, RootCommandId, SCHEMA_MAJOR,
    SessionId, SessionSeq, TenantId, ValidationContent, ValidationId, ValidationKind,
    ValidationMode, ValidationPhase,
};
use std::collections::BTreeSet;

const ISSUER: ParticipantId = ParticipantId::from_u128(1);
const WORKER: ParticipantId = ParticipantId::from_u128(2);
const EVALUATOR: ParticipantId = ParticipantId::from_u128(3);
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
    core.apply(seq, prepared).unwrap();
}

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

fn generate(i: u64) -> AuthenticatedInput {
    let id = CLAIM_BASE + i as u128;
    input(
        id,
        ISSUER,
        Command::GenerateClaim {
            claim: new_claim(id),
        },
    )
}

fn main() {
    alloc_count::configure(31, 1, 80_000);
    let claims: u64 = std::env::var("FOCAL_BENCH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000);
    println!("focal-core domain reduce allocation counts ({claims} claim creations)\n");
    println!("{}", alloc_count::header());
    let inputs: Vec<AuthenticatedInput> = (0..claims).map(generate).collect();
    let mut phases = Vec::new();

    {
        let core = negotiated_core();
        alloc_count::reset_sites();
        let mut meter = Meter::start("prepare GenerateClaim (base state)");
        for input in &inputs {
            let before = meter.open();
            let prepared = core.prepare(input).unwrap();
            std::hint::black_box(&prepared);
            drop(prepared);
            meter.close(before);
        }
        phases.push(meter.finish());
        println!("\ntop sites: prepare GenerateClaim");
        print!("{}", alloc_count::sites_report(10));
    }

    {
        let mut core = negotiated_core();
        let half = (claims / 2) as usize;
        for (label, range) in [
            ("apply GenerateClaim, first half", 0..half),
            ("apply GenerateClaim, second half", half..claims as usize),
        ] {
            alloc_count::reset_sites();
            let mut meter = Meter::start(label);
            for input in inputs.get(range).unwrap_or(&[]) {
                let prepared = core.prepare(input).unwrap();
                let seq = SessionSeq(core.sequence().0 + 1);
                let before = meter.open();
                let result = core.apply(seq, prepared).unwrap();
                std::hint::black_box(&result);
                meter.close(before);
                drop(result);
            }
            phases.push(meter.finish());
            println!("\ntop sites: {label}");
            print!("{}", alloc_count::sites_report(12));
        }
    }

    println!();
    println!("{}", alloc_count::header());
    for phase in &phases {
        println!("{}", alloc_count::row(phase));
    }
}
