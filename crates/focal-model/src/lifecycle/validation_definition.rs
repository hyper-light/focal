//! Owned native definitions and their checked construction plan. The private
//! stamp only binds a detached in-memory evaluation to the same semantic inputs;
//! it is not a content identity, persisted hash, or successor wire allocation.
use super::*;

#[cfg(test)]
#[path = "validation_definition_allocation_tests.rs"]
mod allocation_tests;
#[path = "validation_definition_memory.rs"]
mod memory;
#[path = "validation_definition_source.rs"]
mod source;
#[cfg(test)]
#[path = "validation_definition_view_tests.rs"]
mod view_tests;
pub use source::{
    DeclarationFields, DeclarationHandlers, DeclarationSource, DeclarationSourcePlan, HandlerValue,
    PhaseFields, PolicyPhase, PolicySource, ProgramFields,
};
pub(in crate::lifecycle) use source::{PolicyEvent, check_declaration_fields, visit_program};

fn construction_buffer<T>(count: usize) -> Result<Vec<T>, ContractError> {
    let values = crate::lifecycle::memory::reserve::<T>(count)?;
    // Reconcile the provisional allocation before constructing its elements or
    // allocating the next buffer. The checked plan quotes exact capacities.
    if values.capacity() != count {
        return Err(ContractError::Capacity);
    }
    Ok(values)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::lifecycle) struct DefinitionStamp([u8; 32]);
impl DefinitionStamp {
    pub(in crate::lifecycle) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    fn intent_fingerprint(self, attempts: u32) -> ContentHash {
        let mut hash = blake3::Hasher::new_derive_key("focal/native/validation-intent/1");
        hash.update(self.as_bytes());
        hash.update(&attempts.to_be_bytes());
        ContentHash(*hash.finalize().as_bytes())
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

/// Allocation-free view of a retained phase and its ordered fallback handlers.
#[derive(Debug, Clone, Copy)]
pub struct PhasePolicyView<'a> {
    policy: &'a OwnedPhasePolicy,
}
impl<'a> PhasePolicyView<'a> {
    pub fn evaluator(self) -> ParticipantId {
        self.policy.evaluator
    }
    pub fn definition(self) -> ContentHash {
        self.policy.definition
    }
    pub fn required_policy(self) -> Option<ContentHash> {
        self.policy.required_policy
    }
    /// Each value borrows the original immutable handler reference. No handler
    /// array or owned definition is copied to expose the authored policy.
    pub fn handlers(
        self,
    ) -> impl ExactSizeIterator<Item = HandlerPolicy<'a>> + DoubleEndedIterator + 'a {
        self.policy.handlers.iter().map(|step| HandlerPolicy {
            handler: &step.handler,
            attempts: step.attempts,
            proof_schema: step.proof_schema,
            diagnostic_schema: step.diagnostic_schema,
        })
    }
}

/// Complete borrowed program policy, including optional quality and fallbacks.
#[derive(Debug, Clone, Copy)]
pub enum ProgramView<'a> {
    Delivery,
    Programmatic {
        check: PhasePolicyView<'a>,
        quality: Option<PhasePolicyView<'a>>,
    },
    Agentic {
        check: PhasePolicyView<'a>,
    },
}
impl OwnedPhasePolicy {
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
                let mut bytes = construction_buffer(name.len())?;
                bytes.extend_from_slice(name.as_bytes());
                // Consumes the checked byte buffer without a second allocation;
                // the borrowed str already supplies valid UTF-8.
                let owned = String::from_utf8(bytes).map_err(|_| ContractError::InvalidManifest)?;
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
    principal: Principal,
    limits: Limits,
    shape: source::Shape,
}
impl DeclarationPlan<'_> {
    /// Native row plus exact requested buffer capacities, excluding allocator metadata.
    pub fn construction_charge(&self) -> usize {
        self.shape.charge
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.shape.stamp.intent_fingerprint(self.shape.attempts)
    }
    pub fn build(self) -> Result<Declaration, ContractError> {
        source::build(
            &self.spec,
            self.principal,
            self.limits,
            self.shape,
            self.shape.charge,
            self.shape.build_visits,
        )
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
        let (_, shape) = source::inspect(principal, &spec, limits, usize::MAX)?;
        Ok(DeclarationPlan {
            spec,
            principal,
            limits,
            shape,
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
    /// Original authored deadline; reading it does not authorize a timer firing.
    pub fn deadline(&self) -> Deadline {
        self.spec.deadline
    }
    /// Complete immutable program view, borrowing every retained handler.
    pub fn program(&self) -> ProgramView<'_> {
        match &self.spec.program {
            OwnedProgram::Delivery => ProgramView::Delivery,
            OwnedProgram::Programmatic { check, quality } => ProgramView::Programmatic {
                check: PhasePolicyView { policy: check },
                quality: quality.as_ref().map(|policy| PhasePolicyView { policy }),
            },
            OwnedProgram::Agentic { check } => ProgramView::Agentic {
                check: PhasePolicyView { policy: check },
            },
        }
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

pub(in crate::lifecycle) struct Stamp(blake3::Hasher);
impl Stamp {
    fn field(&mut self, bytes: &[u8]) -> Result<(), ContractError> {
        let len = u64::try_from(bytes.len()).map_err(|_| ContractError::Capacity)?;
        self.0.update(&len.to_be_bytes());
        self.0.update(bytes);
        Ok(())
    }
}

pub(in crate::lifecycle) fn stamp_begin(
    fields: DeclarationFields<'_>,
) -> Result<Stamp, ContractError> {
    let mut stamp = Stamp(blake3::Hasher::new_derive_key(
        "focal native validation definition binding",
    ));
    stamp.field(&fields.binding.ledger.tenant.0)?;
    stamp.field(&fields.binding.ledger.session.0)?;
    stamp.field(&fields.binding.object.0)?;
    stamp.field(&fields.binding.content.0)?;
    stamp.field(&fields.binding.revision.0.to_be_bytes())?;
    stamp.field(&fields.claim.0)?;
    stamp.field(&fields.issuer.0)?;
    stamp.field(&fields.declaration_index.to_be_bytes())?;
    stamp.field(&fields.kind.code().to_be_bytes())?;
    stamp.field(&fields.phase.code().to_be_bytes())?;
    stamp.field(&fields.mode.code().to_be_bytes())?;
    stamp.field(&fields.deadline.timer.0)?;
    stamp.field(&fields.deadline.generation.to_be_bytes())?;
    stamp.field(&fields.deadline.at.to_be_bytes())?;
    match fields.target {
        TargetDeclaration::WholeWorkSlot { index, name } => {
            stamp.field(b"whole-work-slot")?;
            stamp.field(&index.to_be_bytes())?;
            stamp.field(name.as_bytes())?;
        }
        TargetDeclaration::Delivery => stamp.field(b"delivery")?,
        TargetDeclaration::Admission => stamp.field(b"admission")?,
        TargetDeclaration::Increment => stamp.field(b"increment")?,
    }
    Ok(stamp)
}

impl Stamp {
    pub(in crate::lifecycle) fn event(&mut self, event: PolicyEvent) -> Result<(), ContractError> {
        match event {
            PolicyEvent::Program(program) => self.field(match program {
                ProgramFields::Delivery => b"delivery",
                ProgramFields::Programmatic { .. } => b"programmatic",
                ProgramFields::Agentic { .. } => b"agentic",
            }),
            PolicyEvent::Quality(present) => {
                self.field(if present { b"quality" } else { b"no-quality" })
            }
            PolicyEvent::Phase(fields) => {
                self.field(&fields.evaluator.0)?;
                self.field(&fields.definition.0)?;
                match fields.required_policy {
                    None => self.field(b"no-policy")?,
                    Some(hash) => {
                        self.field(b"policy")?;
                        self.field(&hash.0)?;
                    }
                }
                self.field(
                    &u64::try_from(fields.handlers)
                        .map_err(|_| ContractError::Capacity)?
                        .to_be_bytes(),
                )
            }
            PolicyEvent::Handler(_, value) => {
                self.field(&value.id.0)?;
                self.field(&value.version.0)?;
                self.field(&[u8::from(value.agentic)])?;
                self.field(&value.attempts.to_be_bytes())?;
                self.field(&value.proof_schema.0)?;
                self.field(&value.diagnostic_schema.0)
            }
        }
    }

    pub(in crate::lifecycle) fn finish(self) -> DefinitionStamp {
        DefinitionStamp(*self.0.finalize().as_bytes())
    }
}
