//! Fallible indirection for large native rows. Private singleton Vecs provide
//! owned storage without infallible allocation, unsafe code or shared pointers.
//! Entry already charges the inline Vec header; these charges cover allocated
//! elements, nested heaps and allocator metadata.
use super::input_codec::bytes::{CountingSink, Cursor, Error as CodecError, SliceSink};
use super::record_codec::{EventBindings, decode_event, encode_event};
use super::{ContractError, LedgerId, NativeError, NativeEvent, NativeFact};
use focal_memory::MemoryError;
use focal_model::lifecycle::Binding;
use focal_model::lifecycle::{
    aggregation::RegistrationSet,
    claim::ClaimState,
    claim_descriptor::ClaimDescriptor,
    creation::Owner,
    scope::ScopeLimits,
    validation::{Declaration, EvaluationState},
    validation_descriptor::ValidationDescriptor,
};
use focal_model::{ClaimId, ContentHash, ObjectId, ObjectRevision};

const ALLOCATION: usize = 4 * size_of::<usize>();
const CLAIM_CONTAINER: usize = size_of::<ClaimRow>() + ALLOCATION;
const DECLARATION_CONTAINER: usize = size_of::<Declaration>() + ALLOCATION;
const AUTHORED_DECLARATION_CONTAINER: usize = size_of::<ValidationDescriptor>() + ALLOCATION;
/// The bytes before a content row's descriptor: the content's hash, and the descriptor's length
/// (u32, little-endian).
const CONTENT_HEAD: usize = 36;
/// Codec work a content row's own bytes take to write or read, beyond one a byte.
const CONTENT_VISITS: usize = 64;
const EVALUATION_CONTAINER: usize = size_of::<EvaluationState>() + ALLOCATION;
/// The widest retained event's encoding with its ledger implied (every fact
/// field is fixed-width, so the bound is finite; `owned_tests` proves each
/// fact's widest form within it), and the codec visits that many bytes take.
const EVENT_BYTES: usize = 504;
const EVENT_VISITS: usize = 4 * EVENT_BYTES;
const EVENT_CONTAINER: usize = EVENT_BYTES + ALLOCATION;

#[derive(Debug)]
struct ClaimRow {
    state: ClaimState,
    registrations: RegistrationSet,
}
#[derive(Debug)]
pub(super) struct OwnedClaim(Vec<ClaimRow>);
/// A definition in one allocation of exactly its body, held in the row itself: a boxed slice of
/// one is narrow enough that no box around the row is needed (doc 31 M3).
#[derive(Debug)]
pub(super) struct OwnedDeclaration(DeclarationStorage);
#[derive(Debug)]
enum DeclarationStorage {
    Legacy(Box<[Declaration]>),
    Authored(Box<[ValidationDescriptor]>),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClaimContentProfile {
    pub(super) max_responses: u32,
    pub(super) scope_limits: ScopeLimits,
    pub(super) owner: Option<Owner>,
}
/// A claim's content as its row holds it (doc 31 M3, stage 1): one allocation, exactly its own
/// length, of the content's hash, the descriptor's record encoding and the creation profile's, so
/// a live claim holds the few hundred bytes of the encoding where the decoded descriptor took a
/// kilobyte of nested heap. What admission reads on its common paths (the hash, the ledger, id and
/// schema, the profile) is read in place; the body is decoded only where a check needs it
/// ([`OwnedClaimContent::body`]), charged while it is held. A record and a checkpoint write the
/// row's encoding as it is held, and a page's copy-on-write copies bytes.
#[derive(Debug)]
pub(super) struct OwnedClaimContent(Vec<u8>);
/// A claim's descriptor decoded from its content row, charged while it is held.
#[derive(Debug)]
pub struct HeldClaimBody {
    descriptor: ClaimDescriptor,
    _charge: focal_memory::Allocation,
}
impl PartialEq for HeldClaimBody {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor == other.descriptor
    }
}
impl std::ops::Deref for HeldClaimBody {
    type Target = ClaimDescriptor;
    fn deref(&self) -> &ClaimDescriptor {
        &self.descriptor
    }
}
#[derive(Debug)]
pub(super) struct OwnedEvaluation(Vec<EvaluationState>);
#[derive(Debug)]
/// A retained event as its record encoding with the ledger implied by the
/// range, allocated at exactly its own length.
pub(super) struct OwnedEvent(Vec<u8>);

fn add(left: usize, right: usize) -> Result<usize, MemoryError> {
    left.checked_add(right)
        .ok_or(MemoryError::CounterExhausted("native row heap charge"))
}
fn container_heap<T>(capacity: usize) -> Result<usize, MemoryError> {
    add(
        capacity
            .checked_mul(size_of::<T>())
            .ok_or(MemoryError::CounterExhausted("native row capacity"))?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}
fn within(actual: usize, allowance: usize) -> Result<(), MemoryError> {
    if actual > allowance {
        Err(MemoryError::Capacity {
            requested: actual,
            available: allowance,
        })
    } else {
        Ok(())
    }
}
/// Caller holds the requested element charge before allocation. Refuse larger
/// reported capacity instead of performing a hidden shrink or reallocation.
fn singleton<T>(value: T, allowance: usize) -> Result<Vec<T>, MemoryError> {
    within(container_heap::<T>(1)?, allowance)?;
    let mut rows = Vec::new();
    #[cfg(test)]
    let requested = tests::allocation_capacity(1)?;
    #[cfg(not(test))]
    let requested = 1;
    rows.try_reserve_exact(requested)
        .map_err(|_| MemoryError::AllocationFailed)?;
    within(container_heap::<T>(rows.capacity())?, allowance)?;
    rows.push(value);
    Ok(rows)
}
/// `rows` as a boxed slice in the buffer they fill (a full buffer converts without moving).
fn exact<T>(rows: Vec<T>) -> Result<Box<[T]>, MemoryError> {
    if rows.len() != rows.capacity() {
        return Err(MemoryError::AllocationFailed);
    }
    Ok(rows.into_boxed_slice())
}
fn get<T>(rows: &[T]) -> Option<&T> {
    match rows {
        [row] => Some(row),
        _ => None,
    }
}
fn nested_heap(bytes: usize, allocations: usize) -> Result<usize, MemoryError> {
    add(
        bytes,
        allocations
            .checked_mul(ALLOCATION)
            .ok_or(MemoryError::CounterExhausted("native row allocator charge"))?,
    )
}
fn claim_heap(claim: &ClaimState) -> Result<usize, MemoryError> {
    nested_heap(
        claim
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        claim
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}
fn registrations_heap(registrations: &RegistrationSet) -> Result<usize, MemoryError> {
    nested_heap(
        registrations
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        registrations
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}
fn declaration_heap(declaration: &Declaration) -> Result<usize, MemoryError> {
    nested_heap(
        declaration
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        declaration
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}

fn authored_declaration_heap(descriptor: &ValidationDescriptor) -> Result<usize, MemoryError> {
    nested_heap(
        descriptor
            .retained_heap_bytes()
            .map_err(|_| MemoryError::AllocationFailed)?,
        descriptor
            .heap_allocations()
            .map_err(|_| MemoryError::AllocationFailed)?,
    )
}

impl OwnedClaim {
    /// Additional reservation for moving an already charged claim and set into
    /// one container. Their existing nested buffers move unchanged.
    pub(super) const fn container_charge() -> usize {
        CLAIM_CONTAINER
    }
    pub(super) fn new(
        state: ClaimState,
        registrations: RegistrationSet,
    ) -> Result<Self, MemoryError> {
        add(
            Self::container_charge(),
            add(claim_heap(&state)?, registrations_heap(&registrations)?)?,
        )?;
        Ok(Self(singleton(
            ClaimRow {
                state,
                registrations,
            },
            Self::container_charge(),
        )?))
    }
    pub(super) fn claim(&self) -> Option<&ClaimState> {
        get(&self.0).map(|row| &row.state)
    }
    pub(super) fn registrations(&self) -> Option<&RegistrationSet> {
        get(&self.0).map(|row| &row.registrations)
    }
    /// Native-owner access during private preparation. The owner precharges
    /// growth and publishes state and registration changes atomically.
    #[cfg(test)]
    pub(super) fn parts_mut(&mut self) -> Option<(&mut ClaimState, &mut RegistrationSet)> {
        match self.0.as_mut_slice() {
            [row] => Some((&mut row.state, &mut row.registrations)),
            _ => None,
        }
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        let row = get(&self.0).ok_or(MemoryError::MissingKey)?;
        add(
            container_heap::<ClaimRow>(self.0.capacity())?,
            add(
                claim_heap(&row.state)?,
                registrations_heap(&row.registrations)?,
            )?,
        )
    }
    /// Storage precharges the complete old row before copying a retained neighbor.
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let row = get(&self.0).ok_or(MemoryError::MissingKey)?;
        let state = row
            .state
            .try_copy(
                row.state
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        let registrations = row
            .registrations
            .try_copy(
                row.registrations
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?,
            )
            .map_err(|_| MemoryError::AllocationFailed)?;
        let copied = Self::new(state, registrations)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedDeclaration {
    pub(super) const fn container_charge() -> usize {
        DECLARATION_CONTAINER
    }
    pub(super) fn new(declaration: Declaration) -> Result<Self, MemoryError> {
        add(Self::container_charge(), declaration_heap(&declaration)?)?;
        Ok(Self(DeclarationStorage::Legacy(exact(singleton(
            declaration,
            Self::container_charge(),
        )?)?)))
    }
    pub(super) const fn authored_container_charge() -> usize {
        AUTHORED_DECLARATION_CONTAINER
    }
    /// Move the complete descriptor, including its declaration's existing
    /// handler buffers, into one fallible container without another copy.
    pub(super) fn new_authored(descriptor: ValidationDescriptor) -> Result<Self, MemoryError> {
        add(
            Self::authored_container_charge(),
            authored_declaration_heap(&descriptor)?,
        )?;
        Ok(Self(DeclarationStorage::Authored(exact(singleton(
            descriptor,
            Self::authored_container_charge(),
        )?)?)))
    }
    pub(super) fn get(&self) -> Option<&Declaration> {
        match &self.0 {
            DeclarationStorage::Legacy(rows) => get(rows),
            DeclarationStorage::Authored(rows) => get(rows).map(ValidationDescriptor::declaration),
        }
    }
    pub(super) fn descriptor(&self) -> Option<&ValidationDescriptor> {
        match &self.0 {
            DeclarationStorage::Legacy(_) => None,
            DeclarationStorage::Authored(rows) => get(rows),
        }
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        match &self.0 {
            DeclarationStorage::Legacy(rows) => add(
                container_heap::<Declaration>(rows.len())?,
                declaration_heap(get(rows).ok_or(MemoryError::MissingKey)?)?,
            ),
            DeclarationStorage::Authored(rows) => add(
                container_heap::<ValidationDescriptor>(rows.len())?,
                authored_declaration_heap(get(rows).ok_or(MemoryError::MissingKey)?)?,
            ),
        }
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = match &self.0 {
            DeclarationStorage::Legacy(rows) => {
                let declaration = get(rows).ok_or(MemoryError::MissingKey)?;
                let charge = declaration
                    .retained_bytes()
                    .map_err(|_| MemoryError::AllocationFailed)?;
                Self::new(
                    declaration
                        .try_copy(charge)
                        .map_err(|_| MemoryError::AllocationFailed)?,
                )?
            }
            DeclarationStorage::Authored(rows) => {
                let descriptor = get(rows).ok_or(MemoryError::MissingKey)?;
                let charge = descriptor
                    .copy_charge()
                    .map_err(|_| MemoryError::AllocationFailed)?;
                Self::new_authored(
                    descriptor
                        .try_copy(charge)
                        .map_err(|_| MemoryError::AllocationFailed)?,
                )?
            }
        };
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedClaimContent {
    /// Hold `descriptor` with its creation profile, as its encoding.
    pub(super) fn new(
        descriptor: ClaimDescriptor,
        max_responses: u32,
        scope_limits: ScopeLimits,
        owner: Option<Owner>,
    ) -> Result<Self, MemoryError> {
        let profile = ClaimContentProfile {
            max_responses,
            scope_limits,
            owner,
        };
        let body = Self::body_len(&descriptor)?;
        Self::write(body, descriptor.content_hash(), &profile, |sink| {
            super::input_codec::descriptors::claim(sink, &descriptor)
        })
    }
    /// Hold a descriptor's encoding `body`, as a record carries it once replay has checked it,
    /// with its content's hash and its creation profile.
    pub(super) fn from_encoded(
        body: &[u8],
        content_hash: ContentHash,
        profile: &ClaimContentProfile,
    ) -> Result<Self, MemoryError> {
        Self::write(body.len(), content_hash, profile, |sink| {
            super::input_codec::bytes::write_raw(sink, body)
        })
    }
    /// Exactly what [`Self::new`] charges for `descriptor` under `profile`, without allocating:
    /// what admission charges before it is built.
    pub(super) fn charge_for(
        descriptor: &ClaimDescriptor,
        profile: &ClaimContentProfile,
    ) -> Result<usize, MemoryError> {
        Self::charge_for_body(Self::body_len(descriptor)?, profile)
    }
    /// Exactly what a row of a `body`-byte descriptor encoding charges under `profile`: what a
    /// recovery plan quotes before it is built.
    pub(super) fn charge_for_body(
        body: usize,
        profile: &ClaimContentProfile,
    ) -> Result<usize, MemoryError> {
        container_heap::<u8>(Self::held_len(body, profile)?)
    }
    fn body_len(descriptor: &ClaimDescriptor) -> Result<usize, MemoryError> {
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        super::input_codec::descriptors::claim(&mut count, descriptor)
            .map_err(|_| MemoryError::AllocationFailed)?;
        Ok(count.len())
    }
    fn held_len(body: usize, profile: &ClaimContentProfile) -> Result<usize, MemoryError> {
        let mut count = CountingSink::new(usize::MAX, usize::MAX);
        super::record_codec::content_profile(&mut count, profile)
            .map_err(|_| MemoryError::AllocationFailed)?;
        add(add(CONTENT_HEAD, body)?, count.len())
    }
    fn write(
        body: usize,
        content_hash: ContentHash,
        profile: &ClaimContentProfile,
        write_body: impl FnOnce(&mut SliceSink<'_>) -> Result<(), CodecError>,
    ) -> Result<Self, MemoryError> {
        let len = Self::held_len(body, profile)?;
        let charge = container_heap::<u8>(len)?;
        let mut bytes = Vec::new();
        #[cfg(test)]
        let requested = tests::allocation_capacity(len)?;
        #[cfg(not(test))]
        let requested = len;
        bytes
            .try_reserve_exact(requested)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(container_heap::<u8>(bytes.capacity())?, charge)?;
        // Within the reserved capacity: no reallocation.
        bytes.resize(len, 0);
        let visits = add(add(len, len)?, CONTENT_VISITS)?;
        let mut sink = SliceSink::new(&mut bytes, visits);
        let written = super::input_codec::bytes::write_raw(&mut sink, &content_hash.0)
            .and_then(|()| {
                super::input_codec::bytes::write_u32(
                    &mut sink,
                    u32::try_from(body).map_err(|_| CodecError::Capacity)?,
                )
            })
            .and_then(|()| write_body(&mut sink))
            .and_then(|()| super::record_codec::content_profile(&mut sink, profile))
            .and_then(|()| sink.finish());
        written.map_err(|_| MemoryError::AllocationFailed)?;
        Ok(Self(bytes))
    }
    /// The row's encoding as a record and a checkpoint write it: the descriptor, then the profile.
    pub(super) fn encoded(&self) -> &[u8] {
        self.0.get(CONTENT_HEAD..).unwrap_or(&[])
    }
    /// The descriptor's encoding.
    fn body_bytes(&self) -> Option<&[u8]> {
        let length = self.0.get(32..CONTENT_HEAD)?;
        let length = usize::try_from(u32::from_le_bytes(length.try_into().ok()?)).ok()?;
        self.0.get(CONTENT_HEAD..CONTENT_HEAD.checked_add(length)?)
    }
    /// The content's hash.
    pub(super) fn content_hash(&self) -> Option<ContentHash> {
        Some(ContentHash(self.0.get(..32)?.try_into().ok()?))
    }
    /// The descriptor's ledger, id and schema, read in place.
    fn head(&self) -> Option<(LedgerId, ClaimId, u16)> {
        let body = self.body_bytes()?;
        let mut cursor = Cursor::new(body, body.len(), CONTENT_VISITS).ok()?;
        let ledger = LedgerId {
            tenant: focal_model::TenantId(cursor.fixed().ok()?),
            session: focal_model::SessionId(cursor.fixed().ok()?),
        };
        let id = ClaimId(cursor.fixed().ok()?);
        let schema = cursor.u16().ok()?;
        Some((ledger, id, schema))
    }
    /// The descriptor's schema.
    pub(super) fn schema(&self) -> Option<u16> {
        self.head().map(|(_, _, schema)| schema)
    }
    /// The content's binding: its ledger, its claim, its hash, at its one revision.
    pub(super) fn binding(&self) -> Option<Binding> {
        let (ledger, id, _) = self.head()?;
        Some(Binding {
            ledger,
            object: ObjectId(id.0),
            content: self.content_hash()?,
            revision: ObjectRevision(1),
        })
    }
    /// The content's intent fingerprint, as the descriptor's.
    pub(super) fn intent_fingerprint(&self) -> Option<ContentHash> {
        let (_, id, _) = self.head()?;
        Some(focal_model::lifecycle::claim_descriptor::intent_of(
            id,
            self.content_hash()?,
        ))
    }
    pub(super) fn profile(&self) -> Option<ClaimContentProfile> {
        let body = self.body_bytes()?.len();
        let profile = self.0.get(CONTENT_HEAD.checked_add(body)?..)?;
        let visits = Self::read_visits(profile.len()).ok()?;
        let mut cursor = Cursor::new(profile, profile.len(), visits).ok()?;
        let read = super::record_codec::read_content_profile(&mut cursor).ok()?;
        cursor.finish().ok()?;
        Some(read)
    }
    /// The codec work reading a `len`-byte encoding takes, every collection in it walked a few
    /// times over.
    fn read_visits(len: usize) -> Result<usize, MemoryError> {
        add(
            len.checked_mul(4)
                .ok_or(MemoryError::CounterExhausted("content visits"))?,
            CONTENT_VISITS,
        )
    }
    /// The descriptor as a view over its encoding: its fields, roles, scopes, relations and
    /// requirements read in place, nothing built and nothing allocated.
    pub(super) fn view(&self) -> Result<super::input_codec::ClaimBodyInput<'_>, NativeError> {
        let body = self.body_bytes().ok_or(ContractError::InvalidPolicy)?;
        let visits = Self::read_visits(body.len())?;
        super::input_codec::ClaimBodyInput::inspect(
            body,
            super::input_codec::BodyInspectionLimits {
                bytes: body.len(),
                parse_visits: visits,
                source_visits: visits,
            },
        )
        .map_err(|_| ContractError::InvalidPolicy.into())
    }
    /// The descriptor, decoded from its encoding within `limits`, its heap charged to `budget`
    /// in `lane` before it is built and while it is held. The encoding was checked when the row
    /// was written; what bounds this decode is the encoding's own length.
    pub(super) fn body(
        &self,
        budget: &focal_memory::MemoryBudget,
        lane: focal_memory::BudgetLane,
    ) -> Result<HeldClaimBody, NativeError> {
        let body = self.body_bytes().ok_or(ContractError::InvalidPolicy)?;
        let len = body.len();
        // Bounds of a body that was checked when it was written, each from its own length: the
        // model checks an element at a few hundred visits at most (its fixed fields 512, a
        // relation 256, a slot or check 128) and the source walks each nested one at a few dozen,
        // while every element takes several bytes; a decoded body is no more than tens of times
        // its encoding.
        let visits = len
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .ok_or(MemoryError::CounterExhausted("content visits"))?;
        let mut input = super::input_codec::ClaimBodyInput::inspect(
            body,
            super::input_codec::BodyInspectionLimits {
                bytes: len,
                parse_visits: visits,
                source_visits: visits,
            },
        )
        .map_err(|_| ContractError::InvalidPolicy)?;
        let limits = focal_model::lifecycle::claim_descriptor::Limits {
            description_bytes: len,
            relations: len,
            scopes: len,
            scope_key_bytes: len,
            requirements: len,
            slots: len,
            checks: len,
            construction_bytes: visits,
        };
        let model = visits;
        let plan = input
            .prepare(limits, model, visits)
            .map_err(|_| ContractError::InvalidPolicy)?;
        let quote = plan.quote();
        let charge = budget
            .reserve(focal_memory::BudgetKind::Query, lane, quote.bytes)?
            .commit();
        let descriptor = plan
            .build(quote.bytes, quote.model_build_visits)
            .map_err(|_| ContractError::InvalidPolicy)?;
        Ok(HeldClaimBody {
            descriptor,
            _charge: charge,
        })
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        if self.0.len() < CONTENT_HEAD {
            return Err(MemoryError::MissingKey);
        }
        container_heap::<u8>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let mut bytes = Vec::new();
        #[cfg(test)]
        let requested = tests::allocation_capacity(self.0.len())?;
        #[cfg(not(test))]
        let requested = self.0.len();
        bytes
            .try_reserve_exact(requested)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(container_heap::<u8>(bytes.capacity())?, old)?;
        bytes.extend_from_slice(&self.0);
        Ok(Self(bytes))
    }
}

impl OwnedEvaluation {
    pub(super) const fn container_charge() -> usize {
        EVALUATION_CONTAINER
    }
    pub(super) fn new(evaluation: EvaluationState) -> Result<Self, MemoryError> {
        Ok(Self(singleton(evaluation, Self::container_charge())?))
    }
    pub(super) fn get(&self) -> Option<&EvaluationState> {
        get(&self.0)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        self.get().ok_or(MemoryError::MissingKey)?;
        container_heap::<EvaluationState>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let copied = Self::new(*self.get().ok_or(MemoryError::MissingKey)?)?;
        within(copied.heap_charge()?, old)?;
        Ok(copied)
    }
}

impl OwnedEvent {
    /// The most one event row can charge: what a write envelope reserves.
    pub(super) const fn container_charge() -> usize {
        EVENT_CONTAINER
    }
    /// Hold `event` under `ledger`. Every binding it names must be under that
    /// ledger, and a claim fact's graph capture must name an earlier ordinal.
    pub(super) fn new(event: NativeEvent, ledger: LedgerId) -> Result<Self, NativeError> {
        if let NativeFact::Claim(row) = event.fact {
            row.check_graph_capture(event.ordinal)?;
        }
        if !super::history::under(event, ledger) {
            return Err(ContractError::WrongLedger.into());
        }
        let bindings = EventBindings::Implied(ledger);
        let len = Self::held_len(event, bindings)?;
        within(container_heap::<u8>(len)?, EVENT_CONTAINER)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(container_heap::<u8>(bytes.capacity())?, EVENT_CONTAINER)?;
        // Within the reserved capacity: no reallocation.
        bytes.resize(len, 0);
        let mut sink = SliceSink::new(&mut bytes, EVENT_VISITS);
        encode_event(&mut sink, event, bindings).map_err(event_codec)?;
        sink.finish().map_err(event_codec)?;
        Ok(Self(bytes))
    }
    /// Exactly what [`Self::new`] charges for `event` under `ledger`, without
    /// allocating: what a recovery plan quotes before building the row.
    pub(super) fn charge_for(event: NativeEvent, ledger: LedgerId) -> Result<usize, NativeError> {
        let len = Self::held_len(event, EventBindings::Implied(ledger))?;
        Ok(container_heap::<u8>(len)?)
    }
    fn held_len(event: NativeEvent, bindings: EventBindings) -> Result<usize, NativeError> {
        let mut count = CountingSink::new(EVENT_BYTES, EVENT_VISITS);
        encode_event(&mut count, event, bindings).map_err(event_codec)?;
        Ok(count.len())
    }
    /// The event, its bindings under `ledger`; `None` if the bytes do not
    /// decode to exactly one event.
    pub(super) fn get(&self, ledger: LedgerId) -> Option<NativeEvent> {
        let mut cursor = Cursor::new(&self.0, EVENT_BYTES, EVENT_VISITS).ok()?;
        let event = decode_event(&mut cursor, EventBindings::Implied(ledger)).ok()?;
        cursor.finish().ok()?;
        Some(event)
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        if self.0.is_empty() {
            return Err(MemoryError::MissingKey);
        }
        container_heap::<u8>(self.0.capacity())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        let old = self.heap_charge()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.0.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(container_heap::<u8>(bytes.capacity())?, old)?;
        bytes.extend_from_slice(&self.0);
        Ok(Self(bytes))
    }
}
fn event_codec(error: CodecError) -> NativeError {
    match error {
        CodecError::Capacity => NativeError::Capacity("event row"),
        CodecError::Allocation => NativeError::Memory(MemoryError::AllocationFailed),
        _ => NativeError::Contract(ContractError::InvalidManifest),
    }
}

#[cfg(test)]
#[path = "owned_tests.rs"]
mod tests;

/// Frozen legacy bytes an import retains verbatim (23 §5.2). They are never
/// decoded into a native lifecycle row; readers decode them with the frozen
/// legacy codec on demand.
#[derive(Debug)]
pub(super) struct OwnedLegacy(Vec<u8>);
impl OwnedLegacy {
    pub(super) fn charge(len: usize) -> Result<usize, MemoryError> {
        len.checked_add(ALLOCATION)
            .ok_or(MemoryError::AllocationFailed)
    }
    pub(super) fn new(bytes: &[u8]) -> Result<Self, MemoryError> {
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(bytes.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        if owned.capacity() != bytes.len() {
            return Err(MemoryError::AllocationFailed);
        }
        owned.extend_from_slice(bytes);
        Ok(Self(owned))
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.0
    }
    pub(super) fn heap_charge(&self) -> Result<usize, MemoryError> {
        Self::charge(self.0.len())
    }
    pub(super) fn copy(&self) -> Result<Self, MemoryError> {
        Self::new(&self.0)
    }
}
