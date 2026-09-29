//! Native claim-creation frame construction, shaped as in the node's own native
//! host tests. Each `create_envelope` is one native Create the client submits.
use focal_core::native::{NativeCommand, NativeInput, input_codec};
use focal_ledger::NativeContentProfile;
use focal_model::lifecycle::{
    Binding, Principal, aggregation, claim::ClaimDefinition, creation::Proposal, graph, scope,
    succession::Lineage, validation,
};
use focal_model::*;
use focal_wire::{NATIVE_PROTOCOL_VERSION, Operation, RequestEnvelope};
use std::fmt;

/// A frame the fixed shapes here could not build: the model refused a
/// declaration or a policy, or the codec its encoding. None is expected of
/// these constants; each is reported rather than unwound on.
#[derive(Debug)]
pub struct FrameError {
    stage: &'static str,
    error: String,
}
impl FrameError {
    fn at(stage: &'static str, error: impl fmt::Display) -> Self {
        Self {
            stage,
            error: error.to_string(),
        }
    }
}
impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.stage, self.error)
    }
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
) -> Result<validation::Declaration, FrameError> {
    let claim_id = u128::from_be_bytes(binding.object.0);
    let object = claim_id
        .checked_add(10_000)
        .ok_or_else(|| FrameError::at("declaration id", "arithmetic overflow"))?;
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
    .map_err(|error| FrameError::at("declaration", error))
}

fn proposal(
    ledger: LedgerId,
    issuer: ParticipantId,
    subject: ParticipantId,
    id: u128,
) -> Result<Proposal, FrameError> {
    let binding = native_binding(ledger, id);
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
                .map_err(|error| FrameError::at("lineage", error))?,
            acceptance: aggregation::AcceptancePolicy::new(
                binding,
                issuer,
                &[],
                &[definition(issuer, binding)?],
                aggregation::Limits {
                    max_slots: 8,
                    max_checks: 16,
                    max_results: 32,
                    max_updates: 8,
                },
            )
            .map_err(|error| FrameError::at("acceptance policy", error))?,
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
    request: u128,
    id: u128,
) -> Result<NativeInput, FrameError> {
    let proposals = vec![proposal(ledger, issuer, worker, id)?];
    let declarations = proposals
        .iter()
        .map(|p| definition(issuer, p.definition.binding))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(NativeInput {
        request: RequestKey {
            principal: issuer,
            epoch: RequestEpoch(1),
            id: RequestId::from_u128(request),
        },
        command: NativeCommand::Create {
            claims: proposals,
            declarations,
        },
    })
}

fn frame(ledger: LedgerId, input: &NativeInput) -> Result<Vec<u8>, FrameError> {
    let plan = input_codec::EncodingPlan::prepare(
        input_codec::InputFrame::Request {
            ledger,
            profile: NativeContentProfile::ProjectionOnly,
            input,
        },
        input_codec::EncodingLimits {
            bytes: 1 << 20,
            visits: 1 << 28,
        },
    )
    .map_err(|error| FrameError::at("encoding plan", error))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(plan.quote().bytes)
        .map_err(|error| FrameError::at("frame buffer", error))?;
    bytes.resize(plan.quote().bytes, 0);
    plan.write_into(&mut bytes)
        .map_err(|error| FrameError::at("frame encode", error))?;
    Ok(bytes)
}

/// One native Create request envelope.
pub fn create_envelope(
    ledger: LedgerId,
    issuer: ParticipantId,
    worker: ParticipantId,
    request: u128,
    id: u128,
) -> Result<RequestEnvelope, FrameError> {
    let input = create(ledger, issuer, worker, request, id)?;
    Ok(RequestEnvelope {
        protocol: NATIVE_PROTOCOL_VERSION,
        ledger,
        route_epoch: RouteEpoch(1),
        request_epoch: RequestEpoch(1),
        request_id: RequestId::from_u128(request),
        operation: Operation::Native {
            frame: frame(ledger, &input)?,
        },
    })
}
