//! One streaming preimage for checked borrowed and owned authored bodies.
use super::*;
use validation::{PolicyEvent, ProgramFields, TargetDeclaration};

pub(super) struct Stream {
    content: blake3::Hasher,
    specification: blake3::Hasher,
}

impl Stream {
    fn update(&mut self, bytes: &[u8]) {
        self.content.update(bytes);
        self.specification.update(bytes);
    }

    fn count(&mut self, value: usize) -> Result<(), ContractError> {
        self.update(
            &u64::try_from(value)
                .map_err(|_| ContractError::Capacity)?
                .to_be_bytes(),
        );
        Ok(())
    }

    fn text(&mut self, value: &str) -> Result<(), ContractError> {
        self.count(value.len())?;
        self.update(value.as_bytes());
        Ok(())
    }

    pub(super) fn new(fields: ValidationFields<'_>) -> Result<Self, ContractError> {
        let mut value = Self {
            content: blake3::Hasher::new(),
            specification: blake3::Hasher::new(),
        };
        value.content.update(b"focal/native/validation-content/1");
        value
            .specification
            .update(b"focal/native/requirement-specification/1");
        value.update(&fields.schema.to_be_bytes());
        value.update(&fields.ledger.tenant.0);
        value.update(&fields.ledger.session.0);
        value.update(&crate::ObjectKind::Validation.code().to_be_bytes());
        value.content.update(&fields.claim.0);
        value.update(&fields.issuer.0);
        value.update(&fields.declaration_index.to_be_bytes());
        value.update(&fields.kind.code().to_be_bytes());
        value.update(&fields.phase.code().to_be_bytes());
        value.update(&fields.mode.code().to_be_bytes());
        match fields.target {
            TargetDeclaration::WholeWorkSlot { index, name } => {
                value.update(&[0]);
                value.update(&index.to_be_bytes());
                value.text(name)?;
            }
            TargetDeclaration::Delivery => value.update(&[1]),
            TargetDeclaration::Admission => value.update(&[2]),
            TargetDeclaration::Increment => value.update(&[3]),
        }
        Ok(value)
    }

    /// Called by the shared policy validator with the exact checked values.
    pub(super) fn policy(&mut self, event: PolicyEvent) -> Result<(), ContractError> {
        match event {
            PolicyEvent::Program(program) => self.update(&[match program {
                ProgramFields::Delivery => 0,
                ProgramFields::Programmatic { .. } => 1,
                ProgramFields::Agentic { .. } => 2,
            }]),
            PolicyEvent::Phase(phase) => {
                if phase.definition.0 == [0; 32]
                    || phase
                        .required_policy
                        .is_some_and(|value| value.0 == [0; 32])
                {
                    return Err(ContractError::InvalidPolicy);
                }
                self.update(&phase.evaluator.0);
                self.update(&phase.definition.0);
                match phase.required_policy {
                    Some(required) => {
                        self.update(&[1]);
                        self.update(&required.0);
                    }
                    None => self.update(&[0]),
                }
                self.count(phase.handlers)?;
            }
            PolicyEvent::Handler(_, step) => {
                if step.version.0 == [0; 32]
                    || step.proof_schema.0 == [0; 32]
                    || step.diagnostic_schema.0 == [0; 32]
                {
                    return Err(ContractError::InvalidPolicy);
                }
                self.update(&step.id.0);
                self.update(&step.version.0);
                self.update(&[u8::from(step.agentic)]);
                self.update(&step.attempts.to_be_bytes());
                self.update(&step.proof_schema.0);
                self.update(&step.diagnostic_schema.0);
            }
            PolicyEvent::Quality(present) => self.update(&[u8::from(present)]),
        }
        Ok(())
    }

    pub(super) fn authored(
        &mut self,
        fields: ValidationFields<'_>,
        contributors: usize,
    ) -> Result<(), ContractError> {
        self.update(&fields.deadline.timer.0);
        self.update(&fields.deadline.generation.to_be_bytes());
        self.update(&fields.deadline.at.to_be_bytes());
        self.text(fields.description)?;
        match fields.quality_bar {
            Some(quality) => {
                self.update(&[1]);
                self.text(quality)?;
            }
            None => self.update(&[0]),
        }
        self.count(contributors)
    }

    pub(super) fn contributor(&mut self, participant: ParticipantId) {
        self.update(&participant.0);
    }

    pub(super) fn finish(mut self, revision: u64) -> (ContentHash, ContentHash) {
        self.update(&revision.to_be_bytes());
        (
            ContentHash(*self.content.finalize().as_bytes()),
            ContentHash(*self.specification.finalize().as_bytes()),
        )
    }
}

pub(super) fn intent(binding: Binding) -> ContentHash {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/validation-descriptor-intent/1");
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    ContentHash(*hash.finalize().as_bytes())
}
