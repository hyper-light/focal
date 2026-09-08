use super::*;
use crate::native::{
    input_codec::{
        artifact::{ArtifactBodyInput, ArtifactBodyPlan},
        creation_content::{
            BodyConstructionQuote, ClaimBodyInput, ClaimBodyPlan, DeclarationBodyInput,
            DeclarationBodyPlan, ValidationBodyInput, ValidationBodyPlan,
        },
    },
    owned::{OwnedClaimContent, OwnedDeclaration},
    result_owned::{NativeArtifact, OwnedArtifact},
};
use focal_evidence::NativeLocalCustody;
use focal_model::lifecycle::{
    Principal,
    artifact_descriptor::{self as artifact_model, ContentPointer},
    claim_descriptor,
    creation::Owner,
    scope::ScopeLimits,
    validation, validation_descriptor,
};
use focal_model::{ContentClass, ContentDomainId, ContentHash, ParticipantId, RequestKey};

/// Recovery authority is obtained from the real local ContentStore. The
/// implementation must fund/read/verify the complete tree and schema, and bind
/// the resulting token to the original authenticated artifact request. Encoded
/// addresses and local revisions cannot satisfy this interface on their own.
pub(in crate::native::record_codec) trait Custody {
    fn recover(
        &self,
        request: RequestKey,
        descriptor: &ArtifactDescriptor,
        pointer: ContentPointer,
        local_revision: u64,
    ) -> Result<NativeLocalCustody, NativeError>;
}
fn body_quote(
    body: BodyConstructionQuote,
    inline: usize,
    container: usize,
    native_build_visits: usize,
) -> Result<Quote, NativeError> {
    let nested = body
        .bytes
        .checked_sub(inline)
        .ok_or(NativeError::Capacity("descriptor heap quote"))?;
    Ok(Quote {
        heap_bytes: add(container, nested)?,
        allocations: add(body.allocations, 1)?,
        model_inspection_visits: add(body.model_inspection_visits, 128)?,
        model_build_visits: add(body.model_build_visits, native_build_visits)?,
        source_inspection_visits: body.source_inspection_visits,
        source_build_visits: body.source_build_visits,
    })
}
fn content_pointer(c: &mut Cursor<'_>) -> Result<ContentPointer, Error> {
    Ok(ContentPointer {
        domain: ContentDomainId(c.fixed()?),
        root: ContentHash(c.fixed()?),
        length: c.u64()?,
        class: match c.u16()? {
            1 => ContentClass::Document,
            2 => ContentClass::Evidence,
            3 => ContentClass::Checkpoint,
            _ => return Err(Error::InvalidTag("content class")),
        },
    })
}
pub(in crate::native::record_codec) struct ArtifactInput<'a> {
    body: ArtifactBodyInput<'a>,
    pointer: ContentPointer,
    local_revision: u64,
}
pub(in crate::native::record_codec) struct ArtifactPlan<'s, 'a> {
    body: ArtifactBodyPlan<'s, 'a>,
    pointer: ContentPointer,
    local_revision: u64,
    request: RequestKey,
    quote: Quote,
}
pub(in crate::native::record_codec) fn artifact<'a>(
    cursor: &mut Cursor<'a>,
) -> Result<ArtifactInput<'a>, NativeError> {
    Ok(ArtifactInput {
        body: ArtifactBodyInput::read(cursor).map_err(decode)?,
        pointer: content_pointer(cursor).map_err(codec)?,
        local_revision: cursor.u64().map_err(codec)?,
    })
}
impl<'a> ArtifactInput<'a> {
    pub(in crate::native::record_codec) fn fields(&self) -> artifact_model::ArtifactFields<'_> {
        self.body.fields()
    }
    pub(in crate::native::record_codec) fn prepare(
        &mut self,
        request: RequestKey,
        limits: artifact_model::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ArtifactPlan<'_, 'a>, NativeError> {
        let remaining = max_model_visits
            .checked_sub(128)
            .ok_or(NativeError::Capacity("artifact recovery work"))?;
        if self.local_revision == 0
            || self.pointer.domain.is_zero()
            || self.pointer.root.0 == [0; 32]
        {
            return Err(ContractError::MissingEvidence.into());
        }
        let ownership_visits = self.body.ownership_visits().map_err(decode)?;
        let body = self
            .body
            .prepare(limits, remaining, max_source_visits)
            .map_err(decode)?;
        let quote = body_quote(
            body.quote(),
            size_of::<ArtifactDescriptor>(),
            OwnedArtifact::container_charge(),
            ownership_visits,
        )?;
        Ok(ArtifactPlan {
            body,
            pointer: self.pointer,
            local_revision: self.local_revision,
            request,
            quote,
        })
    }
}
impl ArtifactPlan<'_, '_> {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
        custody: &impl Custody,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_model_visits)?;
        let body_quote = self.body.quote();
        let descriptor = self
            .body
            .build(body_quote.bytes, body_quote.model_build_visits)
            .map_err(decode)?;
        let token =
            custody.recover(self.request, &descriptor, self.pointer, self.local_revision)?;
        let value = NativeArtifact::recover(
            descriptor,
            token,
            self.request,
            self.pointer,
            self.local_revision,
        )?;
        let owned = OwnedArtifact::new(value)?;
        let actual = owned.heap_charge()?;
        finish(Row::Artifact(owned), actual, self.quote, max_bytes)
    }
}

pub(in crate::native::record_codec) enum DefinitionInput<'a> {
    Legacy(DeclarationBodyInput<'a>),
    Authored(ValidationBodyInput<'a>),
}
enum DefinitionBodyPlan<'s, 'a> {
    Legacy(DeclarationBodyPlan<'s, 'a>),
    Authored(ValidationBodyPlan<'s, 'a>),
}
pub(in crate::native::record_codec) struct DefinitionPlan<'s, 'a> {
    body: DefinitionBodyPlan<'s, 'a>,
    quote: Quote,
}
pub(in crate::native::record_codec) fn definition<'a>(
    cursor: &mut Cursor<'a>,
) -> Result<DefinitionInput<'a>, NativeError> {
    match cursor.u8().map_err(codec)? {
        0 => Ok(DefinitionInput::Legacy(
            DeclarationBodyInput::read(cursor).map_err(decode)?,
        )),
        1 => Ok(DefinitionInput::Authored(
            ValidationBodyInput::read(cursor).map_err(decode)?,
        )),
        _ => Err(ContractError::InvalidManifest.into()),
    }
}
impl<'a> DefinitionInput<'a> {
    pub(in crate::native::record_codec) fn claim(&self) -> ClaimId {
        match self {
            Self::Legacy(v) => v.fields().claim,
            Self::Authored(v) => v.fields().claim,
        }
    }
    pub(in crate::native::record_codec) fn issuer(&self) -> ParticipantId {
        match self {
            Self::Legacy(v) => v.fields().issuer,
            Self::Authored(v) => v.fields().issuer,
        }
    }
    /// `issuer` is the original claimant identity established by the importer's
    /// recorded definition/claim correspondence, not a synthetic ingress actor.
    pub(in crate::native::record_codec) fn prepare(
        &mut self,
        issuer: ParticipantId,
        legacy_limits: validation::Limits,
        authored_limits: validation_descriptor::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<DefinitionPlan<'_, 'a>, NativeError> {
        let remaining = max_model_visits
            .checked_sub(128)
            .ok_or(NativeError::Capacity("definition recovery work"))?;
        if issuer != self.issuer() {
            return Err(ContractError::WrongActor.into());
        }
        let (body, quote) = match self {
            Self::Legacy(input) => {
                let plan = input
                    .prepare(
                        Principal::Actor(issuer),
                        legacy_limits,
                        remaining,
                        max_source_visits,
                    )
                    .map_err(decode)?;
                let quote = body_quote(
                    plan.quote(),
                    size_of::<Declaration>(),
                    OwnedDeclaration::container_charge(),
                    128,
                )?;
                (DefinitionBodyPlan::Legacy(plan), quote)
            }
            Self::Authored(input) => {
                let plan = input
                    .prepare(
                        Principal::Actor(issuer),
                        authored_limits,
                        remaining,
                        max_source_visits,
                    )
                    .map_err(decode)?;
                let quote = body_quote(
                    plan.quote(),
                    size_of::<validation_descriptor::ValidationDescriptor>(),
                    OwnedDeclaration::authored_container_charge(),
                    128,
                )?;
                (DefinitionBodyPlan::Authored(plan), quote)
            }
        };
        Ok(DefinitionPlan { body, quote })
    }
}
impl DefinitionPlan<'_, '_> {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_model_visits)?;
        let owned = match self.body {
            DefinitionBodyPlan::Legacy(plan) => {
                let quote = plan.quote();
                OwnedDeclaration::new(
                    plan.build(quote.bytes, quote.model_build_visits)
                        .map_err(decode)?,
                )?
            }
            DefinitionBodyPlan::Authored(plan) => {
                let quote = plan.quote();
                OwnedDeclaration::new_authored(
                    plan.build(quote.bytes, quote.model_build_visits)
                        .map_err(decode)?,
                )?
            }
        };
        let actual = owned.heap_charge()?;
        finish(Row::Definition(owned), actual, self.quote, max_bytes)
    }
}

pub(in crate::native::record_codec) struct ClaimContentInput<'a> {
    body: ClaimBodyInput<'a>,
    max_responses: u32,
    scope_limits: ScopeLimits,
    owner: Option<Owner>,
}
pub(in crate::native::record_codec) struct ClaimContentPlan<'s, 'a> {
    body: ClaimBodyPlan<'s, 'a>,
    max_responses: u32,
    scope_limits: ScopeLimits,
    owner: Option<Owner>,
    quote: Quote,
}
pub(in crate::native::record_codec) fn claim_content<'a>(
    cursor: &mut Cursor<'a>,
    max_source_visits: usize,
) -> Result<ClaimContentInput<'a>, NativeError> {
    Ok(ClaimContentInput {
        body: ClaimBodyInput::read(cursor, max_source_visits).map_err(decode)?,
        max_responses: cursor.u32().map_err(codec)?,
        scope_limits: fields::scope_limits(cursor).map_err(codec)?,
        owner: fields::owner(cursor).map_err(codec)?,
    })
}
impl<'a> ClaimContentInput<'a> {
    pub(in crate::native::record_codec) fn parse_source_visits(&self) -> usize {
        self.body.parse_quote().source_visits
    }
    pub(in crate::native::record_codec) fn prepare(
        &mut self,
        limits: claim_descriptor::Limits,
        max_model_visits: usize,
        max_source_visits: usize,
    ) -> Result<ClaimContentPlan<'_, 'a>, NativeError> {
        let remaining = max_model_visits
            .checked_sub(128)
            .ok_or(NativeError::Capacity("claim content recovery work"))?;
        if self.max_responses == 0 {
            return Err(ContractError::InvalidPolicy.into());
        }
        let ownership_visits = self.body.ownership_visits().map_err(decode)?;
        let body = self
            .body
            .prepare(limits, remaining, max_source_visits)
            .map_err(decode)?;
        if let Some(owner) = self.owner {
            let fields = body.fields();
            if owner.expected.ledger != fields.ledger
                || owner.expected.object.is_zero()
                || owner.expected.object.0 == fields.id.0
                || owner.expected.content.0 == [0; 32]
                || owner.expected.revision.0 == 0
                || owner
                    .receipt
                    .is_some_and(|receipt| receipt.receipt.is_zero() || receipt.epoch == 0)
            {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        let quote = body_quote(
            body.quote(),
            size_of::<claim_descriptor::ClaimDescriptor>(),
            OwnedClaimContent::container_charge(),
            ownership_visits,
        )?;
        Ok(ClaimContentPlan {
            body,
            max_responses: self.max_responses,
            scope_limits: self.scope_limits,
            owner: self.owner,
            quote,
        })
    }
}
impl ClaimContentPlan<'_, '_> {
    pub(in crate::native::record_codec) fn quote(&self) -> Quote {
        self.quote
    }
    pub(in crate::native::record_codec) fn build(
        self,
        max_bytes: usize,
        max_model_visits: usize,
    ) -> Result<Row, NativeError> {
        fits(self.quote.heap_bytes, max_bytes)?;
        fits(self.quote.model_build_visits, max_model_visits)?;
        let quote = self.body.quote();
        let value = self
            .body
            .build(quote.bytes, quote.model_build_visits)
            .map_err(decode)?;
        let owned =
            OwnedClaimContent::new(value, self.max_responses, self.scope_limits, self.owner)?;
        let actual = owned.heap_charge()?;
        finish(Row::ClaimContent(owned), actual, self.quote, max_bytes)
    }
}
