//! Full AuthoredV1 creation frames with local cohort proof before construction.
//! Authentication, state-dependent retries/authority, held memory and publication
//! remain owner responsibilities. No supplied hash replaces a descriptor body.
use super::bytes::Cursor;
use super::claim_source::{ClaimView, SlotView};
use super::source_bytes::Values;
use super::*;
use crate::native::authored as native_authored;
use claim::ClaimSource;
use focal_model::lifecycle::{
    aggregation, claim_descriptor as claim, creation, validation,
    validation_descriptor as descriptor,
};
use focal_model::{ObjectId, ObjectRevision};

#[path = "authored_creation_check.rs"]
mod checks;
#[cfg(test)]
#[path = "authored_creation_tests.rs"]
mod tests;
#[path = "authored_creation_budget.rs"]
mod work;
pub use work::AuthoredCreationWork;
use work::{Budget, Group, Reader, add, multiply, subtract};

#[derive(Debug, Clone, Copy)]
pub struct AuthoredCreationLimits {
    pub claim: claim::Limits,
    pub validation: descriptor::Limits,
    pub acceptance: aggregation::Limits,
    pub work: AuthoredCreationWork,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoredCreationQuote {
    /// Final input heap, including every vector allocation and bookkeeping.
    pub bytes: usize,
    pub allocations: usize,
    pub claims: usize,
    pub declarations: usize,
    pub preparation: AuthoredCreationWork,
    pub construction: AuthoredCreationWork,
}
#[derive(Debug)]
pub struct AuthoredFramePlan<'a> {
    bytes: &'a [u8],
    header: InputHeader,
    native: NativeLimits,
    limits: AuthoredCreationLimits,
    quote: AuthoredCreationQuote,
    intent: ContentHash,
    final_inspection: usize,
}

// Cover scalar guards, counts/quotes, request/group hashes and final buffer
// reconciliation. Descriptor byte/model work has its own separate counters.
const FIXED_WORK: usize = 4096;
const DECLARATION_WORK: usize = 1024;

fn array<T>(count: usize) -> Result<usize, DecodeError> {
    Ok(super::super::prepare::array::<T>(count)?)
}
fn reserve<T>(count: usize) -> Result<Vec<T>, DecodeError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| CodecError::Allocation)?;
    if size_of::<T>() != 0 && values.capacity() != count {
        return Err(CodecError::Capacity.into());
    }
    Ok(values)
}
fn push<T>(values: &mut Vec<T>, value: T) -> Result<(), DecodeError> {
    if values.len() == values.capacity() {
        return Err(CodecError::Capacity.into());
    }
    values.push(value);
    Ok(())
}
fn body_heap<T>(quote: BodyConstructionQuote) -> Result<usize, DecodeError> {
    subtract(quote.bytes, size_of::<T>())
}
fn body_work(quote: BodyConstructionQuote) -> AuthoredCreationWork {
    AuthoredCreationWork {
        source: quote.source_build_visits,
        descriptor: quote.model_build_visits,
        ..AuthoredCreationWork::default()
    }
}
fn prepare_claim<'s, 'a>(
    input: &'s mut ClaimBodyInput<'a>,
    budget: &Budget,
    limits: AuthoredCreationLimits,
) -> Result<ClaimBodyPlan<'s, 'a>, DecodeError> {
    let plan = input.prepare_for_frame(
        limits.claim,
        budget.descriptor.remaining(),
        budget.source.remaining(),
    )?;
    budget.prepared(plan.quote())?;
    Ok(plan)
}
fn prepare_validation<'s, 'a>(
    input: &'s mut ValidationBodyInput<'a>,
    budget: &Budget,
    principal: Principal,
    limits: AuthoredCreationLimits,
) -> Result<ValidationBodyPlan<'s, 'a>, DecodeError> {
    let plan = input.prepare_for_frame(
        principal,
        limits.validation,
        budget.descriptor.remaining(),
        budget.source.remaining(),
    )?;
    budget.prepared(plan.quote())?;
    Ok(plan)
}
fn binding(fields: claim::ClaimFields<'_>, content: ContentHash) -> Binding {
    Binding {
        ledger: fields.ledger,
        object: ObjectId(fields.id.0),
        content,
        revision: ObjectRevision(1),
    }
}
fn request(header: InputHeader) -> Result<RequestKey, DecodeError> {
    header
        .request
        .ok_or(CodecError::InvalidTag("actor request").into())
}
fn finish_intent(header: InputHeader, batch: blake3::Hasher) -> Result<ContentHash, DecodeError> {
    let mut hash = super::super::intent::request_hasher(header.ledger, request(header)?);
    hash.update(&[27]);
    hash.update(batch.finalize().as_bytes());
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

impl<'a> StructuralInput<'a> {
    pub fn prepare_authored_creation(
        &self,
        native: NativeLimits,
        limits: AuthoredCreationLimits,
    ) -> Result<Option<AuthoredFramePlan<'a>>, DecodeError> {
        let header = self.header();
        if header.kind != (FrameKind::Request { command: 27 }) {
            return Ok(None);
        }
        let budget = Budget::new(limits.work);
        budget.native.charge(FIXED_WORK)?;
        let (mut reader, count) = Reader::start(self.bytes(), &budget, native.plan_nodes)?;
        let mut construction = limits.work.subtract(budget.remaining())?;
        let mut bytes = array::<NativeAuthoredProposal>(count)?;
        let mut allocations = 1usize;
        let mut declarations = 0usize;
        let mut final_inspection = FIXED_WORK;
        let mut batch = native_authored::fingerprint_begin(count)?;
        let principal = Principal::Actor(request(header)?.principal);
        for _ in 0..count {
            let before = budget.remaining();
            budget.native.charge(FIXED_WORK)?;
            let mut group = reader.group(native.definitions)?;
            // bound_input scans actual claim scopes/slots once for capacities
            // and once for allocator counts; fixed hash work cannot cover an
            // unbounded number of those rows.
            final_inspection = add(
                final_inspection,
                add(
                    FIXED_WORK,
                    multiply(
                        2,
                        add(
                            group.claim.source().scope_count(),
                            group.claim.source().slot_count(),
                        )?,
                    )?,
                )?,
            )?;
            final_inspection = add(final_inspection, multiply(group.count, DECLARATION_WORK)?)?;
            if group.count != group.claim.source().requirement_count() {
                return Err(ContractError::InvalidPolicy.into());
            }
            declarations = add(declarations, group.count)?;
            if add(count, declarations)? > native.range.max_batch_entries {
                return Err(CodecError::Capacity.into());
            }
            let content = prepare_claim(&mut group.claim, &budget, limits)?;
            let issuer = content.issuer();
            principal.require_actor(issuer)?;
            let claim = binding(content.fields(), content.content_hash());
            if claim.ledger != header.ledger {
                return Err(ContractError::WrongLedger.into());
            }
            if group.max_responses == 0 {
                return Err(ContractError::Capacity.into());
            }
            scope::Registry::new(claim, group.scope_limits)?;
            let content_quote = content.quote();
            bytes = add(bytes, body_heap::<claim::ClaimDescriptor>(content_quote)?)?;
            allocations = add(allocations, content_quote.allocations)?;
            construction = construction.add(body_work(content_quote))?;
            native_authored::fingerprint_claim(
                &mut batch,
                content.intent_fingerprint(),
                group.count,
            )?;
            // End the plan's exclusive body borrow before cohort iterators use it.
            bytes = add(
                bytes,
                array::<descriptor::ValidationDescriptor>(group.count)?,
            )?;
            allocations = add(allocations, usize::from(group.count != 0))?;
            let mut rows = Reader {
                tail: group.declarations,
                budget: &budget,
            };
            for _ in 0..group.count {
                budget.native.charge(DECLARATION_WORK)?;
                let mut input = rows.validation()?;
                let descriptor = prepare_validation(&mut input, &budget, principal, limits)?;
                let quote = descriptor.quote();
                bytes = add(bytes, body_heap::<descriptor::ValidationDescriptor>(quote)?)?;
                allocations = add(allocations, quote.allocations)?;
                construction = construction.add(body_work(quote))?;
                native_authored::fingerprint_declaration(
                    &mut batch,
                    descriptor.intent_fingerprint(),
                );
            }
            rows.finish()?;
            native_authored::fingerprint_profile(
                &mut batch,
                group.max_responses,
                group.scope_limits,
                group.owner,
            )?;
            if bytes > native.preparation_bytes {
                return Err(CodecError::Capacity.into());
            }
            construction = construction.add(before.subtract(budget.remaining())?)?;
            checks::cohort(&group, claim, issuer, principal, limits, &budget)?;
        }
        reader.finish()?;
        checks::batch_ids(self.bytes(), count, native, &budget)?;
        // Build performs the same complete scalar/hash pass, then verifies the
        // final owned native input using the existing full-intent function.
        construction.native = add(construction.native, final_inspection)?;
        budget.native.charge(FIXED_WORK)?;
        let intent = finish_intent(header, batch)?;
        let preparation = limits.work.subtract(budget.remaining())?;
        Ok(Some(AuthoredFramePlan {
            bytes: self.bytes(),
            header,
            native,
            limits,
            quote: AuthoredCreationQuote {
                bytes,
                allocations,
                claims: count,
                declarations,
                preparation,
                construction,
            },
            intent,
            final_inspection,
        }))
    }
}
impl AuthoredFramePlan<'_> {
    pub fn header(&self) -> InputHeader {
        self.header
    }
    pub fn quote(&self) -> AuthoredCreationQuote {
        self.quote
    }
    pub fn intent_fingerprint(&self) -> ContentHash {
        self.intent
    }
    pub fn build(
        self,
        max_bytes: usize,
        work: AuthoredCreationWork,
    ) -> Result<NativeInput, DecodeError> {
        if self.quote.bytes > max_bytes {
            return Err(CodecError::Capacity.into());
        }
        self.quote.construction.fits(work)?;
        let budget = Budget::new(self.quote.construction);
        budget.native.charge(FIXED_WORK)?;
        let (mut reader, count) = Reader::start(self.bytes, &budget, self.native.plan_nodes)?;
        if count != self.quote.claims {
            return Err(ContractError::ContentConflict.into());
        }
        let principal = Principal::Actor(request(self.header)?.principal);
        let mut claims = reserve(count)?;
        let mut batch = native_authored::fingerprint_begin(count)?;
        for _ in 0..count {
            budget.native.charge(FIXED_WORK)?;
            let mut group = reader.group(self.native.definitions)?;
            let content = prepare_claim(&mut group.claim, &budget, self.limits)?;
            let quote = content.quote();
            native_authored::fingerprint_claim(
                &mut batch,
                content.intent_fingerprint(),
                group.count,
            )?;
            let content = content.build(quote.bytes, quote.model_build_visits)?;
            budget.built(quote)?;
            let mut declarations = reserve(group.count)?;
            let mut rows = Reader {
                tail: group.declarations,
                budget: &budget,
            };
            for _ in 0..group.count {
                budget.native.charge(DECLARATION_WORK)?;
                let mut input = rows.validation()?;
                let descriptor = prepare_validation(&mut input, &budget, principal, self.limits)?;
                let quote = descriptor.quote();
                native_authored::fingerprint_declaration(
                    &mut batch,
                    descriptor.intent_fingerprint(),
                );
                let descriptor = descriptor.build(quote.bytes, quote.model_build_visits)?;
                budget.built(quote)?;
                push(&mut declarations, descriptor)?;
            }
            rows.finish()?;
            native_authored::fingerprint_profile(
                &mut batch,
                group.max_responses,
                group.scope_limits,
                group.owner,
            )?;
            push(
                &mut claims,
                NativeAuthoredProposal {
                    content,
                    declarations,
                    max_responses: group.max_responses,
                    scope_limits: group.scope_limits,
                    owner: group.owner,
                },
            )?;
        }
        reader.finish()?;
        budget.native.charge(self.final_inspection)?;
        if finish_intent(self.header, batch)? != self.intent {
            return Err(ContractError::ContentConflict.into());
        }
        native_authored::bound_input(
            &claims,
            claims.capacity(),
            NativeLimits {
                preparation_bytes: self.quote.bytes,
                ..self.native
            },
        )?;
        let input = NativeInput {
            request: request(self.header)?,
            command: NativeCommand::CreateAuthored { claims },
        };
        if super::super::intent::fingerprint(self.header.ledger, &input)? != self.intent {
            return Err(ContractError::ContentConflict.into());
        }
        // Every body builder reconciles actual capacities; outer vectors demand
        // exact requested capacity. Native bound_input applies the same full charge.
        Ok(input)
    }
}
