use super::*;
use std::collections::BTreeSet;

fn text(value: &str, required: bool) -> Result<(), InputError> {
    if value.len() > 16 * 1024 {
        return Err(InputError::Capacity);
    }
    if (required && value.trim().is_empty()) || value.contains('\0') {
        return Err(InputError::Invalid("empty or NUL-containing text"));
    }
    Ok(())
}
fn count(value: usize, limit: usize) -> Result<(), InputError> {
    if value > limit {
        Err(InputError::Capacity)
    } else {
        Ok(())
    }
}
fn allocated(value: Option<String>, ids: &mut impl IdGenerator) -> Result<[u8; 16], InputError> {
    match value {
        Some(value) => parse_id(&value),
        None => {
            let id = ids.next_id()?;
            if id.iter().all(|byte| *byte == 0) {
                return Err(InputError::Identity);
            }
            Ok(id)
        }
    }
}
fn bounded<T: serde::Serialize>(value: &T) -> Result<(), InputError> {
    let size = postcard::experimental::serialized_size(value).map_err(|_| InputError::Capacity)?;
    count(size, MAX_INPUT_BYTES)
}
impl ClaimDocument {
    pub fn build(
        self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<Command, InputError> {
        context.validate()?;
        count(self.validations.len(), 64)?;
        count(self.scopes.len(), 256)?;
        count(self.relations.len(), 252)?;
        bounded(&self)?;
        text(&self.description, true)?;
        let id = ClaimId(allocated(self.id, ids)?);
        let occurrence = OccurrenceId(allocated(self.occurrence, ids)?);
        let subject = resolve_participant(&self.target, context)?;
        let action = parse_action(&self.action)?;
        if subject == context.actor && action != ActionType::Handoff {
            return Err(InputError::Invalid("self targeting requires handoff"));
        }
        let mut relations = BTreeSet::from([
            Relation {
                kind: RelationKind::Issuer,
                target: RelationTarget::Participant(context.actor),
            },
            Relation {
                kind: RelationKind::Subject,
                target: RelationTarget::Participant(subject),
            },
            Relation {
                kind: RelationKind::ClaimAction,
                target: RelationTarget::Action(action),
            },
            Relation {
                kind: RelationKind::CausedBy,
                target: RelationTarget::Root(context.root),
            },
        ]);
        for relation in self.relations {
            let kind = parse_relation(&relation.kind)?;
            if !matches!(
                kind,
                RelationKind::DependsOn
                    | RelationKind::Awaits
                    | RelationKind::Supersedes
                    | RelationKind::Amends
                    | RelationKind::Refines
                    | RelationKind::ConflictsWith
                    | RelationKind::DerivedFrom
                    | RelationKind::Reviews
            ) {
                return Err(InputError::Invalid(
                    "relation is server-owned or unsupported for claims",
                ));
            }
            let target = ClaimId(parse_id(&relation.target)?);
            if target == id && matches!(kind, RelationKind::Supersedes | RelationKind::Amends) {
                return Err(InputError::Invalid("self lineage"));
            }
            if !relations.insert(Relation {
                kind,
                target: RelationTarget::Object(ObjectRef::claim(context.ledger, target)),
            }) {
                return Err(InputError::Invalid("duplicate relation"));
            }
        }
        let mut scopes = BTreeSet::new();
        for scope in self.scopes {
            text(&scope.key, true)?;
            if scope.key.trim() != scope.key {
                return Err(InputError::Invalid("scope key is not normalized"));
            }
            if !scopes.insert(Scope {
                kind: parse_scope_kind(&scope.kind)?,
                key: scope.key,
            }) {
                return Err(InputError::Invalid("duplicate scope"));
            }
        }
        let mut seen = BTreeSet::from([id.0]);
        let mut validations = Vec::new();
        let mut requirements = Vec::new();
        for definition in self.validations {
            let validation = definition.build(context, id, ids)?;
            if !seen.insert(validation.id.0) {
                return Err(InputError::Invalid("duplicate object identity"));
            }
            requirements.push(RequirementRef {
                id: validation.id,
                specification: validation
                    .content
                    .specification_hash()
                    .map_err(|_| InputError::Capacity)?,
            });
            validations.push(validation);
        }
        if !validations.iter().any(|v| {
            v.content.kind == ValidationKind::Receipt
                && v.content.phase == ValidationPhase::WholeWork
                && v.content.mode == ValidationMode::Required
        }) {
            return Err(InputError::Invalid(
                "required whole_work receipt validation is mandatory",
            ));
        }
        let deadline = self.deadline.map(DeadlineDocument::build).transpose()?;
        let command = Command::GenerateClaim {
            claim: NewClaim {
                id,
                validations,
                content: ClaimContent {
                    ledger: context.ledger,
                    schema: SCHEMA_MAJOR,
                    occurrence,
                    description: self.description,
                    relations,
                    scopes,
                    requirements,
                    deadline,
                },
            },
        };
        bounded(&command)?;
        Ok(command)
    }
}
impl ValidationDocument {
    pub fn build(
        self,
        context: &BuildContext,
        claim: ClaimId,
        ids: &mut impl IdGenerator,
    ) -> Result<NewValidation, InputError> {
        context.validate()?;
        if claim.is_zero() {
            return Err(InputError::Invalid("zero claim"));
        }
        count(self.handlers.len(), 64)?;
        count(self.evidence_schemas.len(), 256)?;
        count(self.contributed_by.len(), 256)?;
        bounded(&self)?;
        text(&self.description, true)?;
        let kind = parse_validation_kind(&self.kind)?;
        let phase = parse_validation_phase(&self.phase)?;
        let mode = parse_validation_mode(&self.mode)?;
        let evaluator = resolve_participant(&self.evaluator, context)?;
        let policy_revision = self.policy_revision.unwrap_or(context.policy_revision);
        if policy_revision != context.policy_revision {
            return Err(InputError::Invalid("policy revision differs from context"));
        }
        let mut handlers = Vec::new();
        let mut seen = BTreeSet::new();
        for handler in self.handlers {
            let value = handler.build()?;
            if !seen.insert((value.id, value.version)) {
                return Err(InputError::Invalid("duplicate pinned handler"));
            }
            handlers.push(value);
        }
        if kind != ValidationKind::Receipt && handlers.is_empty() {
            return Err(InputError::Invalid(
                "non-receipt validation needs a pinned handler",
            ));
        }
        if kind == ValidationKind::Receipt && phase != ValidationPhase::WholeWork {
            return Err(InputError::Invalid("receipt validation is whole_work only"));
        }
        let agentic = handlers.iter().filter(|handler| handler.agentic).count();
        if agentic > 1 || (agentic == 1 && handlers.last().is_none_or(|handler| !handler.agentic)) {
            return Err(InputError::Invalid(
                "agentic handler must be the single final handler",
            ));
        }
        if let Some(quality) = &self.quality_bar {
            text(quality, true)?;
            if agentic != 1 || handlers.first().is_none_or(|handler| handler.agentic) {
                return Err(InputError::Invalid(
                    "quality bar needs programmatic then agentic handlers",
                ));
            }
        }
        let mut evidence_schemas = BTreeSet::new();
        for hash in self.evidence_schemas {
            if !evidence_schemas.insert(parse_hash(&hash)?) {
                return Err(InputError::Invalid("duplicate evidence schema"));
            }
        }
        let mut contributed_by = BTreeSet::new();
        for participant in self.contributed_by {
            if !contributed_by.insert(resolve_participant(&participant, context)?) {
                return Err(InputError::Invalid("duplicate contributor"));
            }
        }
        Ok(NewValidation {
            id: ValidationId(allocated(self.id, ids)?),
            content: ValidationContent {
                ledger: context.ledger,
                schema: SCHEMA_MAJOR,
                claim,
                kind,
                phase,
                mode,
                description: self.description,
                quality_bar: self.quality_bar,
                evaluator,
                handlers,
                evidence_schemas,
                contributed_by,
                policy_revision,
            },
        })
    }
}
impl HandlerDocument {
    pub fn build(self) -> Result<HandlerRef, InputError> {
        Ok(HandlerRef {
            id: ValidatorId(parse_id(&self.id)?),
            version: parse_hash(&self.version)?,
            agentic: self.agentic,
        })
    }
}
impl DeadlineDocument {
    pub fn build(self) -> Result<Deadline, InputError> {
        if self.generation == 0 || self.at == 0 {
            return Err(InputError::Invalid("deadline fence"));
        }
        Ok(Deadline {
            timer: TimerId(parse_id(&self.timer)?),
            generation: self.generation,
            at: self.at,
        })
    }
}
impl ReceiptDocument {
    pub fn build(&self) -> Result<ReceiptFence, InputError> {
        if self.epoch == 0 {
            return Err(InputError::Invalid("zero receipt epoch"));
        }
        Ok(ReceiptFence {
            receipt: ReceiptId(parse_id(&self.id)?),
            epoch: self.epoch,
        })
    }
}
impl TestamentDocument {
    pub fn build(
        self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<Command, InputError> {
        context.validate()?;
        count(self.manifest.len(), 256)?;
        bounded(&self)?;
        text(&self.summary, true)?;
        let mut manifest = Vec::new();
        let mut seen = BTreeSet::new();
        for reference in self.manifest {
            let id = ArtifactId(parse_id(&reference.id)?);
            if !seen.insert(id) {
                return Err(InputError::Invalid("duplicate manifest artifact"));
            }
            manifest.push(ArtifactRef {
                id,
                hash: parse_hash(&reference.hash)?,
            });
        }
        Ok(Command::CloseTestament {
            claim: ClaimId(parse_id(&self.claim)?),
            receipt: self.receipt.build()?,
            testament: TestamentId(allocated(self.id, ids)?),
            evidence_set: EvidenceSetId(parse_id(&self.evidence_set)?),
            manifest,
            summary: self.summary,
            confidence: parse_confidence(&self.confidence)?,
            outcome: parse_outcome(&self.outcome)?,
        })
    }
}

impl ArtifactDocument {
    pub fn build(
        self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<Command, InputError> {
        context.validate()?;
        count(self.inputs.len(), 256)?;
        count(self.visibility.len(), 256)?;
        count(self.metadata.len(), 16 * 1024)?;
        bounded(&self)?;
        if self.kind.is_empty()
            || self.kind.len() > 128
            || !self.kind.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_./-".contains(&byte)
            })
        {
            return Err(InputError::Invalid("artifact kind"));
        }
        let receipt = self.receipt.build()?;
        let payload = self.payload.build()?;
        let mut inputs = BTreeSet::new();
        for input in self.inputs {
            let reference = ObjectRef {
                ledger: context.ledger,
                kind: parse_object_kind(&input.kind)?,
                id: ObjectId(parse_id(&input.id)?),
            };
            if !inputs.insert(reference) {
                return Err(InputError::Invalid("duplicate artifact input"));
            }
        }
        let mut visibility = BTreeSet::new();
        for value in self.visibility {
            text(&value, true)?;
            if !visibility.insert(value) {
                return Err(InputError::Invalid("duplicate visibility"));
            }
        }
        Ok(Command::AttachArtifact {
            claim: ClaimId(parse_id(&self.claim)?),
            receipt,
            evidence_set: EvidenceSetId(parse_id(&self.evidence_set)?),
            artifact: NewArtifact {
                id: ArtifactId(allocated(self.id, ids)?),
                content: ArtifactContent {
                    ledger: context.ledger,
                    schema: SCHEMA_MAJOR,
                    kind: self.kind,
                    schema_hash: parse_hash(&self.schema_hash)?,
                    metadata: self.metadata,
                    payload,
                    producer: context.actor,
                    receipt: Some(receipt),
                    inputs,
                    visibility,
                },
            },
        })
    }
}
impl PayloadDocument {
    pub fn build(self) -> Result<ArtifactPayload, InputError> {
        match self {
            Self::Inline { bytes } => {
                count(bytes.len(), 16 * 1024)?;
                Ok(ArtifactPayload::Inline(bytes))
            }
            Self::Text { text: value } => {
                count(value.len(), 16 * 1024)?;
                Ok(ArtifactPayload::Inline(value.into_bytes()))
            }
            Self::Content { reference } => Ok(ArtifactPayload::Content(reference.build()?)),
        }
    }
}
impl ContentReferenceDocument {
    pub fn build(self) -> Result<ContentRef, InputError> {
        Ok(ContentRef {
            domain: ContentDomainId(parse_id(&self.domain)?),
            root: parse_hash(&self.root)?,
            length: self.length,
            class: parse_content_class(&self.class)?,
        })
    }
}
