//! Owned native definitions and their checked construction plan. The private
//! stamp only binds a detached in-memory evaluation to the same semantic inputs;
//! it is not a content identity, persisted hash, or successor wire allocation.
use super::*;

#[path = "validation_definition_memory.rs"]
mod memory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::lifecycle) struct DefinitionStamp([u8; 32]);
impl DefinitionStamp {
    pub(in crate::lifecycle) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    #[cfg(test)]
    pub(in crate::lifecycle) fn fixture() -> Self {
        Self([0; 32])
    }
}

#[derive(Debug)]
pub(super) struct OwnedHandlerPolicy {
    pub(super) handler: HandlerRef,
    pub(super) attempts: u32,
    pub(super) proof_schema: ContentHash,
    pub(super) diagnostic_schema: ContentHash,
}
#[derive(Debug)]
pub(super) struct OwnedPhasePolicy {
    pub(super) evaluator: ParticipantId,
    pub(super) definition: ContentHash,
    pub(super) handlers: Vec<OwnedHandlerPolicy>,
    pub(super) required_policy: Option<ContentHash>,
}
impl OwnedPhasePolicy {
    fn build(policy: PhasePolicy<'_>) -> Result<Self, ContractError> {
        let mut handlers = Vec::new();
        handlers
            .try_reserve_exact(policy.handlers.len())
            .map_err(|_| ContractError::Capacity)?;
        for step in policy.handlers {
            handlers.push(OwnedHandlerPolicy {
                handler: HandlerRef {
                    id: step.handler.id,
                    version: step.handler.version,
                    agentic: step.handler.agentic,
                },
                attempts: step.attempts,
                proof_schema: step.proof_schema,
                diagnostic_schema: step.diagnostic_schema,
            });
        }
        Ok(Self {
            evaluator: policy.evaluator,
            definition: policy.definition,
            handlers,
            required_policy: policy.required_policy,
        })
    }
    fn retained_bytes(&self) -> Result<usize, ContractError> {
        self.handlers
            .capacity()
            .checked_mul(std::mem::size_of::<OwnedHandlerPolicy>())
            .ok_or(ContractError::Capacity)
    }
}
#[derive(Debug)]
pub(super) enum OwnedProgram {
    Delivery,
    Programmatic {
        check: OwnedPhasePolicy,
        quality: Option<OwnedPhasePolicy>,
    },
    Agentic {
        check: OwnedPhasePolicy,
    },
}
impl OwnedProgram {
    fn build(program: Program<'_>) -> Result<Self, ContractError> {
        Ok(match program {
            Program::Delivery => Self::Delivery,
            Program::Programmatic { check, quality } => Self::Programmatic {
                check: OwnedPhasePolicy::build(check)?,
                quality: quality.map(OwnedPhasePolicy::build).transpose()?,
            },
            Program::Agentic { check } => Self::Agentic {
                check: OwnedPhasePolicy::build(check)?,
            },
        })
    }
    fn retained_bytes(&self) -> Result<usize, ContractError> {
        match self {
            Self::Delivery => Ok(0),
            Self::Programmatic { check, quality } => add(
                check.retained_bytes()?,
                quality
                    .as_ref()
                    .map(OwnedPhasePolicy::retained_bytes)
                    .transpose()?
                    .unwrap_or(0),
            ),
            Self::Agentic { check } => check.retained_bytes(),
        }
    }
}
#[derive(Debug)]
enum OwnedTarget {
    WholeWorkSlot { index: u32, name: String },
    Delivery,
    Admission,
    Increment,
}
impl OwnedTarget {
    fn build(target: TargetDeclaration<'_>) -> Result<Self, ContractError> {
        Ok(match target {
            TargetDeclaration::WholeWorkSlot { index, name } => {
                let mut owned = String::new();
                owned
                    .try_reserve_exact(name.len())
                    .map_err(|_| ContractError::Capacity)?;
                owned.push_str(name);
                Self::WholeWorkSlot { index, name: owned }
            }
            TargetDeclaration::Delivery => Self::Delivery,
            TargetDeclaration::Admission => Self::Admission,
            TargetDeclaration::Increment => Self::Increment,
        })
    }
    fn view(&self) -> TargetDeclaration<'_> {
        match self {
            Self::WholeWorkSlot { index, name } => TargetDeclaration::WholeWorkSlot {
                index: *index,
                name,
            },
            Self::Delivery => TargetDeclaration::Delivery,
            Self::Admission => TargetDeclaration::Admission,
            Self::Increment => TargetDeclaration::Increment,
        }
    }
    fn retained_bytes(&self) -> usize {
        match self {
            Self::WholeWorkSlot { name, .. } => name.capacity(),
            _ => 0,
        }
    }
}
#[derive(Debug)]
pub(super) struct OwnedSpec {
    pub(super) binding: Binding,
    pub(super) claim: ClaimId,
    pub(super) issuer: ParticipantId,
    pub(super) declaration_index: u32,
    kind: ValidationKind,
    pub(super) phase: ValidationPhase,
    pub(super) mode: ValidationMode,
    target: OwnedTarget,
    pub(super) program: OwnedProgram,
    pub(super) deadline: Deadline,
}

/// Immutable admitted definition; construction input lifetimes end at `build`.
/// No handler, target, deadline or policy replacement API is exposed.
#[derive(Debug)]
pub struct Declaration {
    pub(super) spec: OwnedSpec,
    pub(super) attempts: u32,
    stamp: DefinitionStamp,
}

/// Fully checked borrowed construction input. No owned buffers are allocated
/// until `build`; future owners can reserve the charge before that call.
#[derive(Debug)]
pub struct DeclarationPlan<'a> {
    spec: DeclarationSpec<'a>,
    attempts: u32,
    charge: usize,
}
impl DeclarationPlan<'_> {
    /// Native row plus requested exact buffer capacities. Allocator metadata is
    /// excluded; `retained_bytes` reports actual buffer capacities after building.
    pub fn construction_charge(&self) -> usize {
        self.charge
    }
    pub fn build(self) -> Result<Declaration, ContractError> {
        let spec = self.spec;
        let stamp = definition_stamp(spec)?;
        Ok(Declaration {
            spec: OwnedSpec {
                binding: spec.binding,
                claim: spec.claim,
                issuer: spec.issuer,
                declaration_index: spec.declaration_index,
                kind: spec.kind,
                phase: spec.phase,
                mode: spec.mode,
                target: OwnedTarget::build(spec.target)?,
                program: OwnedProgram::build(spec.program)?,
                deadline: spec.deadline,
            },
            attempts: self.attempts,
            stamp,
        })
    }
}
impl Declaration {
    pub fn new(
        principal: Principal,
        spec: DeclarationSpec<'_>,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        Self::prepare(principal, spec, limits)?.build()
    }
    pub fn prepare<'a>(
        principal: Principal,
        spec: DeclarationSpec<'a>,
        limits: Limits,
    ) -> Result<DeclarationPlan<'a>, ContractError> {
        principal.require_actor(spec.issuer)?;
        if spec.claim.is_zero()
            || spec.issuer.is_zero()
            || spec.binding.object.is_zero()
            || spec.binding.ledger.tenant.is_zero()
            || spec.binding.ledger.session.is_zero()
            || spec.deadline.timer.is_zero()
            || spec.deadline.generation == 0
        {
            return Err(ContractError::InvalidPolicy);
        }
        let target_phase = match spec.target {
            TargetDeclaration::WholeWorkSlot { name, .. } => {
                if name.is_empty() || name.len() > limits.slot_bytes {
                    return Err(ContractError::InvalidTarget);
                }
                ValidationPhase::WholeWork
            }
            TargetDeclaration::Delivery => ValidationPhase::WholeWork,
            TargetDeclaration::Admission => ValidationPhase::Admission,
            TargetDeclaration::Increment => ValidationPhase::Increment,
        };
        if spec.phase != target_phase {
            return Err(ContractError::InvalidTarget);
        }
        let attempts = match spec.program {
            Program::Delivery => {
                if spec.kind != ValidationKind::Receipt
                    || spec.mode != ValidationMode::Required
                    || spec.target != TargetDeclaration::Delivery
                {
                    return Err(ContractError::InvalidPolicy);
                }
                0
            }
            Program::Programmatic { check, quality } => {
                if spec.kind == ValidationKind::Receipt
                    || spec.target == TargetDeclaration::Delivery
                {
                    return Err(ContractError::InvalidPolicy);
                }
                let first = validate_policy(check, false, limits)?;
                let second = quality
                    .map(|policy| validate_policy(policy, true, limits))
                    .transpose()?
                    .unwrap_or(0);
                first.checked_add(second).ok_or(ContractError::Capacity)?
            }
            Program::Agentic { check } => {
                if spec.kind == ValidationKind::Receipt
                    || spec.target == TargetDeclaration::Delivery
                {
                    return Err(ContractError::InvalidPolicy);
                }
                validate_policy(check, true, limits)?
            }
        };
        if attempts > limits.attempts {
            return Err(ContractError::Capacity);
        }
        let slots = match spec.target {
            TargetDeclaration::WholeWorkSlot { name, .. } => name.len(),
            _ => 0,
        };
        let handlers = match spec.program {
            Program::Delivery => 0,
            Program::Programmatic { check, quality } => add(
                check.handlers.len(),
                quality.map(|p| p.handlers.len()).unwrap_or(0),
            )?,
            Program::Agentic { check } => check.handlers.len(),
        };
        let buffers = handlers
            .checked_mul(std::mem::size_of::<OwnedHandlerPolicy>())
            .ok_or(ContractError::Capacity)?;
        let charge = add(add(std::mem::size_of::<Self>(), slots)?, buffers)?;
        Ok(DeclarationPlan {
            spec,
            attempts,
            charge,
        })
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        add(
            add(
                std::mem::size_of::<Self>(),
                self.spec.target.retained_bytes(),
            )?,
            self.spec.program.retained_bytes()?,
        )
    }
    pub(in crate::lifecycle) fn definition_stamp(&self) -> DefinitionStamp {
        self.stamp
    }
    pub fn binding(&self) -> Binding {
        self.spec.binding
    }
    pub fn claim(&self) -> ClaimId {
        self.spec.claim
    }
    pub fn issuer(&self) -> ParticipantId {
        self.spec.issuer
    }
    pub fn declaration_index(&self) -> u32 {
        self.spec.declaration_index
    }
    pub fn kind(&self) -> ValidationKind {
        self.spec.kind
    }
    pub fn declared_phase(&self) -> ValidationPhase {
        self.spec.phase
    }
    pub fn mode(&self) -> ValidationMode {
        self.spec.mode
    }
    pub fn target(&self) -> TargetDeclaration<'_> {
        self.spec.target.view()
    }
    pub fn attempt_bound(&self) -> u32 {
        self.attempts
    }
    /// Every declared external-result schema, in handler order: proof followed
    /// by diagnostic for each check/fallback, then each quality/fallback. Pure
    /// delivery has none. Repeated schemas remain repeated; an owner may perform
    /// its own bounded deduplication when pinning verification contracts.
    pub fn evidence_schemas(&self) -> impl Iterator<Item = ContentHash> + '_ {
        let (check, quality): (&[OwnedHandlerPolicy], &[OwnedHandlerPolicy]) =
            match &self.spec.program {
                OwnedProgram::Delivery => (&[], &[]),
                OwnedProgram::Programmatic { check, quality } => (
                    &check.handlers,
                    quality
                        .as_ref()
                        .map_or(&[], |quality| quality.handlers.as_slice()),
                ),
                OwnedProgram::Agentic { check } => (&check.handlers, &[]),
            };
        check
            .iter()
            .chain(quality)
            .flat_map(|step| [step.proof_schema, step.diagnostic_schema])
    }
    pub(super) fn policy(&self, phase: Phase) -> Result<&OwnedPhasePolicy, ContractError> {
        match (&self.spec.program, phase) {
            (OwnedProgram::Programmatic { check, .. }, Phase::Programmatic)
            | (OwnedProgram::Agentic { check }, Phase::Quality)
            | (
                OwnedProgram::Programmatic {
                    quality: Some(check),
                    ..
                },
                Phase::Quality,
            ) => Ok(check),
            _ => Err(ContractError::InvalidTransition),
        }
    }
    pub(super) fn first_phase(&self) -> Phase {
        match &self.spec.program {
            OwnedProgram::Delivery => Phase::Delivery,
            OwnedProgram::Programmatic { .. } => Phase::Programmatic,
            OwnedProgram::Agentic { .. } => Phase::Quality,
        }
    }
}
fn add(left: usize, right: usize) -> Result<usize, ContractError> {
    left.checked_add(right).ok_or(ContractError::Capacity)
}

/// Every chunk carries its length; explicit variant names and ordered handler
/// counts disambiguate shape. This native cache key is never serialized or
/// advertised. A future canonical content format must define its own identity.
fn definition_stamp(spec: DeclarationSpec<'_>) -> Result<DefinitionStamp, ContractError> {
    struct Stamp(blake3::Hasher);
    impl Stamp {
        fn field(&mut self, bytes: &[u8]) -> Result<(), ContractError> {
            let len = u64::try_from(bytes.len()).map_err(|_| ContractError::Capacity)?;
            self.0.update(&len.to_be_bytes());
            self.0.update(bytes);
            Ok(())
        }
        fn policy(&mut self, policy: PhasePolicy<'_>) -> Result<(), ContractError> {
            self.field(&policy.evaluator.0)?;
            self.field(&policy.definition.0)?;
            match policy.required_policy {
                None => self.field(b"no-policy")?,
                Some(hash) => {
                    self.field(b"policy")?;
                    self.field(&hash.0)?;
                }
            }
            self.field(
                &u64::try_from(policy.handlers.len())
                    .map_err(|_| ContractError::Capacity)?
                    .to_be_bytes(),
            )?;
            for step in policy.handlers {
                self.field(&step.handler.id.0)?;
                self.field(&step.handler.version.0)?;
                self.field(&[u8::from(step.handler.agentic)])?;
                self.field(&step.attempts.to_be_bytes())?;
                self.field(&step.proof_schema.0)?;
                self.field(&step.diagnostic_schema.0)?;
            }
            Ok(())
        }
    }
    let mut stamp = Stamp(blake3::Hasher::new_derive_key(
        "focal native validation definition binding",
    ));
    stamp.field(&spec.binding.ledger.tenant.0)?;
    stamp.field(&spec.binding.ledger.session.0)?;
    stamp.field(&spec.binding.object.0)?;
    stamp.field(&spec.binding.content.0)?;
    stamp.field(&spec.binding.revision.0.to_be_bytes())?;
    stamp.field(&spec.claim.0)?;
    stamp.field(&spec.issuer.0)?;
    stamp.field(&spec.declaration_index.to_be_bytes())?;
    stamp.field(&spec.kind.code().to_be_bytes())?;
    stamp.field(&spec.phase.code().to_be_bytes())?;
    stamp.field(&spec.mode.code().to_be_bytes())?;
    stamp.field(&spec.deadline.timer.0)?;
    stamp.field(&spec.deadline.generation.to_be_bytes())?;
    stamp.field(&spec.deadline.at.to_be_bytes())?;
    match spec.target {
        TargetDeclaration::WholeWorkSlot { index, name } => {
            stamp.field(b"whole-work-slot")?;
            stamp.field(&index.to_be_bytes())?;
            stamp.field(name.as_bytes())?;
        }
        TargetDeclaration::Delivery => stamp.field(b"delivery")?,
        TargetDeclaration::Admission => stamp.field(b"admission")?,
        TargetDeclaration::Increment => stamp.field(b"increment")?,
    }
    match spec.program {
        Program::Delivery => stamp.field(b"delivery")?,
        Program::Programmatic { check, quality } => {
            stamp.field(b"programmatic")?;
            stamp.policy(check)?;
            match quality {
                None => stamp.field(b"no-quality")?,
                Some(policy) => {
                    stamp.field(b"quality")?;
                    stamp.policy(policy)?;
                }
            }
        }
        Program::Agentic { check } => {
            stamp.field(b"agentic")?;
            stamp.policy(check)?;
        }
    }
    Ok(DefinitionStamp(*stamp.0.finalize().as_bytes()))
}
