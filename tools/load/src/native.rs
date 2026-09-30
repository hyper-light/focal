//! Native claim-creation frame construction, shaped as in the node's own native
//! host tests. Each `create_envelope` is one structural native Create the
//! client submits (a `projection_only` ledger's form; an `authored_v1` ledger
//! takes compiled documents, see `authored.rs`). A fixture that cannot be
//! built is the tool's defect, returned as [`FixtureError`], never a number.
//!
//! This module is also included by path from `benches/allocs.rs`, so it names
//! nothing else of this crate.
use focal_core::native::{NativeCommand, NativeInput, input_codec};
use focal_ledger::NativeContentProfile;
use focal_model::lifecycle::{
    Binding, Principal, aggregation, claim::ClaimDefinition, creation::Proposal, graph, scope,
    succession::Lineage, validation,
};
use focal_model::*;
use focal_wire::{NATIVE_PROTOCOL_VERSION, Operation, RequestEnvelope};

/// The most bytes one encoded request frame may take.
const FRAME_BYTES: usize = 1 << 20;
/// The most encoder visits one frame may take.
const FRAME_VISITS: usize = 1 << 28;

/// A request fixture that could not be built: what failed, and how.
#[derive(Debug)]
pub struct FixtureError(pub String);

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for FixtureError {}

fn fixture<E: std::fmt::Debug>(what: &'static str) -> impl FnOnce(E) -> FixtureError {
    move |error| FixtureError(format!("{what}: {error:?}"))
}

fn native_binding(ledger: LedgerId, id: u128) -> Binding {
    Binding {
        ledger,
        object: ObjectId::from_u128(id),
        content: ContentHash([7; 32]),
        revision: ObjectRevision(1),
    }
}

fn definition(
    issuer: ParticipantId,
    binding: Binding,
) -> Result<validation::Declaration, FixtureError> {
    let claim_id = u128::from_be_bytes(binding.object.0);
    let object = claim_id
        .checked_add(10_000)
        .ok_or_else(|| FixtureError("declaration object id: past the id space".into()))?;
    validation::Declaration::new(
        Principal::Actor(issuer),
        validation::DeclarationSpec {
            binding: Binding {
                object: ObjectId::from_u128(object),
                ..binding
            },
            claim: ClaimId(binding.object.0),
            issuer,
            declaration_index: 900,
            kind: ValidationKind::Receipt,
            phase: ValidationPhase::WholeWork,
            mode: ValidationMode::Required,
            target: validation::TargetDeclaration::Delivery,
            program: validation::Program::Delivery,
            deadline: Deadline {
                timer: TimerId::from_u128(1),
                generation: 1,
                at: 4_102_444_800_000,
            },
        },
        validation::Limits {
            handlers: 4,
            attempts: 8,
            slot_bytes: 64,
        },
    )
    .map_err(fixture("validation declaration"))
}

fn proposal(
    ledger: LedgerId,
    issuer: ParticipantId,
    subject: ParticipantId,
    id: u128,
) -> Result<Proposal, FixtureError> {
    let binding = native_binding(ledger, id);
    let declaration = definition(issuer, binding)?;
    Ok(Proposal {
        definition: ClaimDefinition {
            binding,
            issuer,
            subject,
            deadline: None,
            max_responses: 4,
            created: SessionSeq(999),
            graph: graph::Declaration::empty(),
            lineage: Lineage::root(binding, RootCommandId::from_u128(1))
                .map_err(fixture("lineage"))?,
            acceptance: aggregation::AcceptancePolicy::new(
                binding,
                issuer,
                &[],
                &[declaration],
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 32,
                    max_updates: 8,
                },
            )
            .map_err(fixture("acceptance policy"))?,
            scope_limits: scope::ScopeLimits {
                scopes: 8,
                roots: 32,
                children: 16,
            },
        },
        owner: None,
    })
}

fn create(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    request: RequestKey,
    id: u128,
) -> Result<NativeInput, FixtureError> {
    let proposals = vec![proposal(ledger, issuer, worker, id)?];
    let mut declarations = Vec::with_capacity(proposals.len());
    for proposal in &proposals {
        declarations.push(definition(issuer, proposal.definition.binding)?);
    }
    Ok(NativeInput {
        request,
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    })
}

fn frame(
    ledger: LedgerId,
    profile: NativeContentProfile,
    input: &NativeInput,
) -> Result<Vec<u8>, FixtureError> {
    let plan = input_codec::EncodingPlan::prepare(
        input_codec::InputFrame::Request {
            ledger,
            profile,
            input,
        },
        input_codec::EncodingLimits {
            bytes: FRAME_BYTES,
            visits: FRAME_VISITS,
        },
    )
    .map_err(fixture("frame plan"))?;
    let mut bytes = vec![0; plan.quote().bytes];
    plan.write_into(&mut bytes)
        .map_err(fixture("frame write"))?;
    Ok(bytes)
}

fn envelope(
    ledger: LedgerId,
    profile: NativeContentProfile,
    input: &NativeInput,
) -> Result<RequestEnvelope, FixtureError> {
    Ok(RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: input.request.epoch,
        request_id: input.request.id,
        operation: Operation::Native {
            frame: frame(ledger, profile, input)?,
        },
    })
}

/// One structural native Create request envelope, sent as `request` (the
/// principal, its generation and the request id).
pub fn create_envelope(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    profile: NativeContentProfile,
    request: RequestKey,
    id: u128,
) -> Result<RequestEnvelope, FixtureError> {
    let input = create(ledger, issuer, worker, request, id)?;
    envelope(ledger, profile, &input)
}

/// The client protocol's floor advance (the audit's F12): `request`, sent in
/// its own generation, asks the owner to close this principal's generations
/// below `minimum`. Shared by both profiles.
pub fn advance_envelope(
    ledger: LedgerId,
    profile: NativeContentProfile,
    request: RequestKey,
    minimum: RequestEpoch,
) -> Result<RequestEnvelope, FixtureError> {
    let input = NativeInput {
        request,
        command: NativeCommand::AdvanceEpochFloor { minimum },
    };
    envelope(ledger, profile, &input)
}
