//! Compact copies preserve the admitted native definition and semantic stamp.
//! The owner reserves the complete charge before entering; these methods do not
//! readmit policies, acquire another permit or promise any persisted encoding.
use super::*;
use crate::lifecycle::memory as bytes;

impl OwnedPhasePolicy {
    fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<OwnedHandlerPolicy>(self.handlers.len())
    }

    fn copy_owned(&self) -> Result<Self, ContractError> {
        let mut handlers = bytes::reserve(self.handlers.len())?;
        for step in &self.handlers {
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
            evaluator: self.evaluator,
            definition: self.definition,
            handlers,
            required_policy: self.required_policy,
        })
    }
}

impl OwnedProgram {
    fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        match self {
            Self::Delivery => Ok(0),
            Self::Programmatic { check, quality } => bytes::add(
                check.copy_heap_bytes()?,
                quality
                    .as_ref()
                    .map(OwnedPhasePolicy::copy_heap_bytes)
                    .transpose()?
                    .unwrap_or(0),
            ),
            Self::Agentic { check } => check.copy_heap_bytes(),
        }
    }

    fn allocations(&self, retained: bool) -> Result<usize, ContractError> {
        fn phase(policy: &OwnedPhasePolicy, retained: bool) -> usize {
            bytes::allocation::<OwnedHandlerPolicy>(if retained {
                policy.handlers.capacity()
            } else {
                policy.handlers.len()
            })
        }
        match self {
            Self::Delivery => Ok(0),
            Self::Programmatic { check, quality } => bytes::add(
                phase(check, retained),
                quality.as_ref().map_or(0, |value| phase(value, retained)),
            ),
            Self::Agentic { check } => Ok(phase(check, retained)),
        }
    }

    fn copy_owned(&self) -> Result<Self, ContractError> {
        Ok(match self {
            Self::Delivery => Self::Delivery,
            Self::Programmatic { check, quality } => Self::Programmatic {
                check: check.copy_owned()?,
                quality: quality
                    .as_ref()
                    .map(OwnedPhasePolicy::copy_owned)
                    .transpose()?,
            },
            Self::Agentic { check } => Self::Agentic {
                check: check.copy_owned()?,
            },
        })
    }
}

impl OwnedTarget {
    fn copy_heap_bytes(&self) -> usize {
        match self {
            Self::WholeWorkSlot { name, .. } => name.len(),
            Self::Delivery | Self::Admission | Self::Increment => 0,
        }
    }

    fn copy_owned(&self) -> Result<Self, ContractError> {
        Ok(match self {
            Self::WholeWorkSlot { index, name } => Self::WholeWorkSlot {
                index: *index,
                // UTF-8 validation consumes the allocated byte Vec without a
                // second allocation; safe Rust String supplies valid bytes.
                name: String::from_utf8(bytes::copy(name.as_bytes())?)
                    .map_err(|_| ContractError::InvalidManifest)?,
            },
            Self::Delivery => Self::Delivery,
            Self::Admission => Self::Admission,
            Self::Increment => Self::Increment,
        })
    }
}

impl Declaration {
    /// Dynamic requested capacities, excluding inline row and allocator overhead.
    pub fn copy_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.spec.target.copy_heap_bytes(),
            self.spec.program.copy_heap_bytes()?,
        )
    }

    /// Actual owned String/Vec capacities, excluding inline row and metadata.
    pub fn retained_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::add(
            self.spec.target.retained_bytes(),
            self.spec.program.retained_bytes()?,
        )
    }

    pub fn copy_heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::allocation::<u8>(self.spec.target.copy_heap_bytes()),
            self.spec.program.allocations(false)?,
        )
    }

    pub fn heap_allocations(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::allocation::<u8>(self.spec.target.retained_bytes()),
            self.spec.program.allocations(true)?,
        )
    }

    /// Inline declaration plus every requested compact-copy buffer. Compute this
    /// and allocator bookkeeping before allocating any part of the new row.
    pub fn copy_charge(&self) -> Result<usize, ContractError> {
        bytes::total::<Self>(self.copy_heap_bytes()?)
    }

    /// Copy without applying current admission rules. The owner provides a
    /// reservation or a precharged touched-page row; provisional buffers are
    /// dropped on any failure and actual returned capacities must fit the bound.
    pub fn try_copy(&self, max_bytes: usize) -> Result<Self, ContractError> {
        bytes::fits(self.copy_charge()?, max_bytes)?;
        let copied = Self {
            spec: OwnedSpec {
                binding: self.spec.binding,
                claim: self.spec.claim,
                issuer: self.spec.issuer,
                declaration_index: self.spec.declaration_index,
                kind: self.spec.kind,
                phase: self.spec.phase,
                mode: self.spec.mode,
                target: self.spec.target.copy_owned()?,
                program: self.spec.program.copy_owned()?,
                deadline: self.spec.deadline,
            },
            attempts: self.attempts,
            stamp: self.stamp,
        };
        bytes::fits(copied.retained_bytes()?, max_bytes)?;
        Ok(copied)
    }

    /// Allocation-free native request identity. The private stamp already binds
    /// every admitted field, ordered handler and schema; include the retained
    /// attempt bound too. This is not a wire hash or durable schema commitment.
    pub fn intent_fingerprint(&self) -> ContentHash {
        let mut hash = blake3::Hasher::new_derive_key("focal/native/validation-intent/1");
        hash.update(self.stamp.as_bytes());
        hash.update(&self.attempts.to_be_bytes());
        ContentHash(*hash.finalize().as_bytes())
    }
}

impl DeclarationPlan<'_> {
    /// Requested buffer storage, excluding the inline declaration and metadata.
    pub fn construction_heap_bytes(&self) -> Result<usize, ContractError> {
        self.construction_charge()
            .checked_sub(size_of::<Declaration>())
            .ok_or(ContractError::Capacity)
    }

    /// Nonempty allocation count for owner allocator bookkeeping before build.
    pub fn construction_heap_allocations(&self) -> Result<usize, ContractError> {
        let target = match self.spec.target {
            TargetDeclaration::WholeWorkSlot { name, .. } => bytes::allocation::<u8>(name.len()),
            TargetDeclaration::Delivery
            | TargetDeclaration::Admission
            | TargetDeclaration::Increment => 0,
        };
        let program = match self.spec.program {
            Program::Delivery => 0,
            Program::Programmatic { check, quality } => bytes::add(
                bytes::allocation::<OwnedHandlerPolicy>(check.handlers.len()),
                quality.map_or(0, |policy| {
                    bytes::allocation::<OwnedHandlerPolicy>(policy.handlers.len())
                }),
            )?,
            Program::Agentic { check } => {
                bytes::allocation::<OwnedHandlerPolicy>(check.handlers.len())
            }
        };
        bytes::add(target, program)
    }
}

#[cfg(test)]
#[path = "validation_definition_memory_tests.rs"]
mod tests;
