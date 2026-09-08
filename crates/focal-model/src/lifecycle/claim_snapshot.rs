//! Checked restoration of complete claim history, without replaying authority.
//! The immutable definition is already owned and checked under caller-held
//! accounting; build moves its graph, lineage and acceptance buffers unchanged.
//! Response stamps come from the actual restored Responses. The importing
//! native record/checkpoint must authenticate original bodies, events, complete
//! cross-row membership and original cuts; an intrinsic plan is not that proof.
use super::*;
use crate::lifecycle::evidence::{Response, ResponseState};
use crate::lifecycle::memory as bytes;
use crate::lifecycle::scope::snapshot::{Hash, Work, complete, identity, overhead, reserve};
#[cfg(test)]
#[path = "claim_snapshot_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimTerminalSnapshotV1 {
    Explicit(ClaimCut),
    Required(aggregation::TerminalCutSnapshotV1),
    Graph(graph::TerminalCutSnapshotV1),
}
impl ClaimTerminalCut {
    pub fn snapshot_v1(self) -> ClaimTerminalSnapshotV1 {
        match self {
            Self::Explicit(cut) => ClaimTerminalSnapshotV1::Explicit(cut),
            Self::Required(cut) => ClaimTerminalSnapshotV1::Required(cut.snapshot_v1()),
            Self::Graph(cut) => ClaimTerminalSnapshotV1::Graph(cut.snapshot_v1()),
        }
    }
}
impl ClaimTerminalSnapshotV1 {
    fn hydrate(self) -> Result<ClaimTerminalCut, ContractError> {
        match self {
            Self::Explicit(cut) => {
                cut.check()?;
                Ok(ClaimTerminalCut::Explicit(cut))
            }
            Self::Required(cut) => Ok(ClaimTerminalCut::Required(
                aggregation::TerminalCut::hydrate_v1(cut)?,
            )),
            Self::Graph(cut) => Ok(ClaimTerminalCut::Graph(graph::TerminalCut::hydrate_v1(
                cut,
            )?)),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimSnapshotV1 {
    pub binding: Binding,
    pub issuer: ParticipantId,
    pub subject: ParticipantId,
    pub created: SessionSeq,
    pub status: ClaimStatus,
    pub receipt: Option<ReceiptEntitlement>,
    pub responses: usize,
    pub max_responses: u32,
    pub deadline: Option<Deadline>,
    pub local_complete: bool,
    pub local_sealed_at: Option<SessionSeq>,
    pub terminal_cut: Option<ClaimTerminalSnapshotV1>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimResponseSnapshotV1 {
    pub link: ResponseLink,
    pub posted: bool,
    pub received: bool,
}
#[derive(Debug, Clone, Copy)]
pub struct ClaimResponseValue<'a> {
    pub history: ClaimResponseSnapshotV1,
    pub response: &'a Response,
}
pub trait ClaimResponseSource {
    type Responses<'a>: Iterator<Item = Result<ClaimResponseValue<'a>, ContractError>>
    where
        Self: 'a;
    fn responses(&self) -> Self::Responses<'_>;
}
impl ClaimResponseSource for [ClaimResponseValue<'_>] {
    type Responses<'a>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, ClaimResponseValue<'a>>>,
        fn(ClaimResponseValue<'a>) -> Result<ClaimResponseValue<'a>, ContractError>,
    >
    where
        Self: 'a;
    fn responses(&self) -> Self::Responses<'_> {
        self.iter().copied().map(Ok)
    }
}

pub struct ClaimHydrationPlan<
    'a,
    R: ClaimResponseSource + ?Sized,
    S: scope::RegistrySnapshotSource + ?Sized,
> {
    definition: ClaimDefinition,
    fields: ClaimSnapshotV1,
    responses: &'a R,
    scopes: scope::RegistryHydrationPlan<'a, S>,
    terminal: Option<ClaimTerminalCut>,
    response_hash: ContentHash,
    response_visits: usize,
    inspection_visits: usize,
    heap: usize,
    allocations: usize,
}
impl ClaimState {
    /// Exact additional response-history allocation for a recorded row shape.
    /// This is a funding quote only; prepare_hydration_v1 validates every value
    /// against the actual retained Responses after the complete row is funded.
    pub fn hydration_response_heap_charge_v1(count: usize) -> Result<usize, ContractError> {
        bytes::add(
            bytes::array::<ResponseRecord>(count)?,
            overhead(usize::from(count != 0))?,
        )
    }
    pub fn snapshot_v1(&self) -> ClaimSnapshotV1 {
        ClaimSnapshotV1 {
            binding: self.binding,
            issuer: self.issuer,
            subject: self.subject,
            created: self.created,
            status: self.status,
            receipt: self.receipt,
            responses: self.responses.len(),
            max_responses: self.max_responses,
            deadline: self.deadline,
            local_complete: self.local_complete,
            local_sealed_at: self.local_sealed_at,
            terminal_cut: self.terminal_cut.map(ClaimTerminalCut::snapshot_v1),
        }
    }
    pub fn response_snapshots_v1(
        &self,
    ) -> impl ExactSizeIterator<Item = ClaimResponseSnapshotV1> + '_ {
        self.responses.iter().map(|row| ClaimResponseSnapshotV1 {
            link: row.link,
            posted: row.posted,
            received: row.received,
        })
    }
    /// `definition` and all source rows are already retained under the caller's
    /// reservation. This plan makes no new allocation; on refusal it drops the
    /// moved definition. No source is changed. No guessed actor or report stamp
    /// is used to reconstruct history.
    pub fn prepare_hydration_v1<
        'a,
        R: ClaimResponseSource + ?Sized,
        S: scope::RegistrySnapshotSource + ?Sized,
    >(
        definition: ClaimDefinition,
        fields: ClaimSnapshotV1,
        responses: &'a R,
        scopes: scope::RegistryHydrationPlan<'a, S>,
        max_visits: usize,
    ) -> Result<ClaimHydrationPlan<'a, R, S>, ContractError> {
        let mut work = Work::new(max_visits);
        work.charge(128)?;
        identity(fields.binding, definition.binding)?;
        if fields.issuer != definition.issuer
            || fields.subject != definition.subject
            || fields.created != definition.created
            || fields.max_responses != definition.max_responses
            || fields.deadline != definition.deadline
        {
            return Err(ContractError::ContentConflict);
        }
        if fields.issuer.is_zero()
            || fields.subject.is_zero()
            || fields.created.0 == 0
            || fields.binding.object.is_zero()
            || fields.binding.ledger.tenant.is_zero()
            || fields.binding.ledger.session.is_zero()
            || fields
                .deadline
                .is_some_and(|deadline| deadline.timer.is_zero() || deadline.generation == 0)
        {
            return Err(ContractError::InvalidTarget);
        }
        if fields.max_responses == 0
            || fields.responses
                > usize::try_from(fields.max_responses).map_err(|_| ContractError::Capacity)?
        {
            return Err(ContractError::Capacity);
        }
        definition.lineage.check_binding(&definition.binding)?;
        definition
            .acceptance
            .check(definition.binding, definition.issuer)?;
        scopes.check_context(fields.binding, fields.created, definition.scope_limits)?;
        scopes.fields().owner.check(&definition.binding)?;
        let terminal = fields
            .terminal_cut
            .map(ClaimTerminalSnapshotV1::hydrate)
            .transpose()?;
        scalar_state(fields, terminal)?;
        let response_visits = response_quote(fields.responses)?;
        work.charge(response_visits)?;
        let response_hash = inspect_responses(fields, responses, response_visits, |_| Ok(()))?;
        // Only acceptance has nested buffers here. All immutable arrays already
        // exist; this traversal measures actual capacities without copying them.
        work.charge(bytes::add(
            definition
                .acceptance
                .slot_count()
                .checked_mul(2)
                .ok_or(ContractError::Capacity)?,
            8,
        )?)?;
        let heap = bytes::add(
            bytes::add(
                bytes::add(
                    definition.graph.retained_heap_bytes()?,
                    definition.lineage.retained_heap_bytes()?,
                )?,
                definition.acceptance.retained_heap_bytes()?,
            )?,
            bytes::add(
                scopes.construction_heap_bytes(),
                bytes::array::<ResponseRecord>(fields.responses)?,
            )?,
        )?;
        let allocations = bytes::add(
            bytes::add(
                bytes::add(
                    definition.graph.heap_allocations()?,
                    definition.lineage.heap_allocations()?,
                )?,
                definition.acceptance.heap_allocations()?,
            )?,
            bytes::add(
                scopes.construction_heap_allocations(),
                usize::from(fields.responses != 0),
            )?,
        )?;
        Ok(ClaimHydrationPlan {
            definition,
            fields,
            responses,
            scopes,
            terminal,
            response_hash,
            response_visits,
            inspection_visits: work.used(),
            heap,
            allocations,
        })
    }
}
impl<R: ClaimResponseSource + ?Sized, S: scope::RegistrySnapshotSource + ?Sized>
    ClaimHydrationPlan<'_, R, S>
{
    pub fn fields(&self) -> ClaimSnapshotV1 {
        self.fields
    }
    pub fn inspection_visits(&self) -> usize {
        self.inspection_visits
    }
    pub fn construction_heap_bytes(&self) -> usize {
        self.heap
    }
    pub fn construction_heap_allocations(&self) -> usize {
        self.allocations
    }
    pub fn construction_charge(&self) -> Result<usize, ContractError> {
        complete::<ClaimState>(self.heap, self.allocations)
    }
    pub fn build_visits(&self) -> Result<usize, ContractError> {
        bytes::add(
            bytes::add(
                self.scopes.build_visits()?,
                self.response_visits
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
            )?,
            bytes::add(
                self.definition
                    .acceptance
                    .slot_count()
                    .checked_mul(2)
                    .ok_or(ContractError::Capacity)?,
                self.scopes
                    .fields()
                    .scopes
                    .checked_mul(3)
                    .and_then(|count| count.checked_add(32))
                    .ok_or(ContractError::Capacity)?,
            )?,
        )
    }
    pub fn build(self, max_bytes: usize, max_visits: usize) -> Result<ClaimState, ContractError> {
        bytes::fits(self.construction_charge()?, max_bytes)?;
        bytes::fits(self.build_visits()?, max_visits)?;
        let scope_charge = self.scopes.construction_charge()?;
        let scope_visits = self.scopes.build_visits()?;
        let scopes = self.scopes.build(scope_charge, scope_visits)?;
        check_scopes(self.fields, self.terminal, &scopes)?;
        let mut remaining = bytes::add(
            bytes::array::<ResponseRecord>(self.fields.responses)?,
            overhead(usize::from(self.fields.responses != 0))?,
        )?;
        let mut responses = reserve(self.fields.responses, &mut remaining)?;
        let actual = inspect_responses(self.fields, self.responses, self.response_visits, |row| {
            responses.push(row);
            Ok(())
        })?;
        if actual != self.response_hash {
            return Err(ContractError::ContentConflict);
        }
        // A generic repeatable source can select different values in a nested
        // duplicate scan. Recheck the actual final array, without callbacks.
        check_records(self.fields, &responses, self.response_visits)?;
        let definition = self.definition;
        let restored = ClaimState {
            binding: self.fields.binding,
            issuer: self.fields.issuer,
            subject: self.fields.subject,
            created: self.fields.created,
            graph: definition.graph,
            lineage: definition.lineage,
            acceptance: definition.acceptance,
            scopes,
            status: self.fields.status,
            receipt: self.fields.receipt,
            responses,
            max_responses: self.fields.max_responses,
            deadline: self.fields.deadline,
            local_complete: self.fields.local_complete,
            local_sealed_at: self.fields.local_sealed_at,
            terminal_cut: self.terminal,
        };
        bytes::fits(
            complete::<ClaimState>(
                restored.retained_heap_bytes()?,
                restored.heap_allocations()?,
            )?,
            max_bytes,
        )?;
        Ok(restored)
    }
}
fn terminal_sequence(value: ClaimTerminalCut) -> SessionSeq {
    match value {
        ClaimTerminalCut::Explicit(cut) => cut.position,
        ClaimTerminalCut::Required(cut) => cut.sequence(),
        ClaimTerminalCut::Graph(cut) => cut.sequence(),
    }
}
fn scalar_state(
    value: ClaimSnapshotV1,
    terminal_cut: Option<ClaimTerminalCut>,
) -> Result<(), ContractError> {
    if value.receipt.is_some_and(|receipt| {
        receipt.holder.is_zero() || receipt.fence.receipt.is_zero() || receipt.fence.epoch == 0
    }) {
        return Err(ContractError::StaleReceipt);
    }
    if terminal(value.status) != terminal_cut.is_some()
        || (value.local_complete || terminal_cut.is_some()) != value.local_sealed_at.is_some()
    {
        return Err(ContractError::InvalidTransition);
    }
    if let Some(sealed) = value.local_sealed_at {
        if sealed < value.created {
            return Err(ContractError::InvalidCut);
        }
        if let Some(cut) = terminal_cut
            && terminal_sequence(cut) < sealed
        {
            return Err(ContractError::InvalidCut);
        }
    }
    match value.status {
        ClaimStatus::Generated
        | ClaimStatus::Posted
        | ClaimStatus::PostFailed
        | ClaimStatus::ReceiptFailed => {
            if value.receipt.is_some() || value.responses != 0 || value.local_complete {
                return Err(ContractError::InvalidTransition);
            }
        }
        ClaimStatus::Received | ClaimStatus::Progressed => {
            if value.receipt.is_none() || value.responses != 0 || value.local_complete {
                return Err(ContractError::InvalidTransition);
            }
        }
        ClaimStatus::TestamentGenerated
        | ClaimStatus::TestamentAcknowledged
        | ClaimStatus::Validating
        | ClaimStatus::Satisfied
        | ClaimStatus::ValidationIncomplete
        | ClaimStatus::ValidationFailed
        | ClaimStatus::ValidationErrored => {
            if value.receipt.is_none() || value.responses == 0 {
                return Err(ContractError::InvalidTransition);
            }
        }
        ClaimStatus::TestamentGenerationFailed => return Err(ContractError::InvalidTransition),
        ClaimStatus::Cancelled
        | ClaimStatus::Expired
        | ClaimStatus::Revoked
        | ClaimStatus::Superseded
        | ClaimStatus::DependencyFailed
        | ClaimStatus::Deadlocked => {}
    }
    if value.local_complete
        && !matches!(
            value.status,
            ClaimStatus::Validating
                | ClaimStatus::Satisfied
                | ClaimStatus::Cancelled
                | ClaimStatus::Expired
                | ClaimStatus::Revoked
                | ClaimStatus::Superseded
                | ClaimStatus::DependencyFailed
                | ClaimStatus::Deadlocked
        )
    {
        return Err(ContractError::InvalidTransition);
    }
    if value.status == ClaimStatus::Satisfied && !value.local_complete {
        return Err(ContractError::InvalidTransition);
    }
    if let Some(cut) = terminal_cut {
        let valid = match cut {
            ClaimTerminalCut::Explicit(_) => matches!(
                value.status,
                ClaimStatus::Satisfied
                    | ClaimStatus::PostFailed
                    | ClaimStatus::ReceiptFailed
                    | ClaimStatus::Cancelled
                    | ClaimStatus::Expired
                    | ClaimStatus::Revoked
                    | ClaimStatus::Superseded
            ),
            ClaimTerminalCut::Required(cut) => {
                if value.local_complete {
                    return Err(ContractError::InvalidTransition);
                }
                match cut.cause().key().target {
                    aggregation::CauseTarget::Admission => value.status == ClaimStatus::PostFailed,
                    aggregation::CauseTarget::Increment { .. }
                    | aggregation::CauseTarget::Response(_) => {
                        value.status
                            == match cut.cause().kind() {
                                aggregation::BlockingKind::Incomplete => {
                                    ClaimStatus::ValidationIncomplete
                                }
                                aggregation::BlockingKind::Failed => ClaimStatus::ValidationFailed,
                                aggregation::BlockingKind::Errored => {
                                    ClaimStatus::ValidationErrored
                                }
                            }
                    }
                }
            }
            ClaimTerminalCut::Graph(cut) => {
                if cut.origin().binding().ledger != value.binding.ledger {
                    return Err(ContractError::WrongLedger);
                }
                value.status
                    == match cut.kind() {
                        graph::FailureKind::DependencyFailed => ClaimStatus::DependencyFailed,
                        graph::FailureKind::Deadlocked => ClaimStatus::Deadlocked,
                    }
            }
        };
        if !valid {
            return Err(ContractError::InvalidTransition);
        }
    }
    Ok(())
}
fn check_scopes(
    value: ClaimSnapshotV1,
    terminal: Option<ClaimTerminalCut>,
    scopes: &scope::Registry,
) -> Result<(), ContractError> {
    for row in scopes.iter() {
        if let Some(cancelled) = row.cancellation()
            && terminal.map(terminal_sequence) != Some(cancelled.terminal)
        {
            return Err(ContractError::InvalidCut);
        }
    }
    if let Some(released) = scopes.release_cut() {
        let cut = terminal.ok_or(ContractError::InvalidTransition)?;
        if released.position < terminal_sequence(cut) {
            return Err(ContractError::InvalidCut);
        }
    }
    if value.status == ClaimStatus::Generated && value.receipt.is_some() {
        return Err(ContractError::InvalidTransition);
    }
    Ok(())
}
fn response_quote(count: usize) -> Result<usize, ContractError> {
    bytes::add(
        64,
        bytes::add(
            count.checked_mul(50).ok_or(ContractError::Capacity)?,
            count.checked_mul(count).ok_or(ContractError::Capacity)?,
        )?,
    )
}
fn inspect_responses<R: ClaimResponseSource + ?Sized>(
    fields: ClaimSnapshotV1,
    source: &R,
    limit: usize,
    mut emit: impl FnMut(ResponseRecord) -> Result<(), ContractError>,
) -> Result<ContentHash, ContractError> {
    let mut work = Work::new(limit);
    work.charge(32)?;
    let mut hash = Hash::new("focal model claim responses snapshot plan 1");
    hash.count(fields.responses)?;
    let mut rows = source.responses();
    let mut previous = None;
    let mut any_received = false;
    let mut current_received = false;
    let mut cause_received = response_cut_target(fields).is_none();
    for index in 0..fields.responses {
        work.charge(50)?;
        let value = rows.next().ok_or(ContractError::InvalidManifest)??;
        let row = response_record(fields, value)?;
        record_order(fields, index, previous, row)?;
        let mut earlier = source.responses();
        for _ in 0..index {
            work.charge(1)?;
            let old = earlier.next().ok_or(ContractError::InvalidManifest)??;
            if old.history.link.testament == row.link.testament {
                return Err(ContractError::InvalidTarget);
            }
        }
        previous = Some(row.link);
        any_received |= row.received;
        current_received |= row.received
            && fields
                .receipt
                .is_some_and(|receipt| receipt.fence == row.link.receipt);
        cause_received |= response_cut_matches(fields, row);
        hash_response(&mut hash, row);
        hash.binding(value.response.identity().binding);
        hash.u8(response_state(value.response.state()));
        emit(row)?;
    }
    work.charge(1)?;
    if rows.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    if !cause_received {
        return Err(ContractError::InvalidTarget);
    }
    received_state(fields, any_received, current_received)?;
    Ok(hash.finish())
}
fn response_record(
    fields: ClaimSnapshotV1,
    value: ClaimResponseValue<'_>,
) -> Result<ResponseRecord, ContractError> {
    let identity = value.response.identity();
    if identity.binding.ledger != fields.binding.ledger
        || identity.claim.0 != fields.binding.object.0
    {
        return Err(ContractError::InvalidTarget);
    }
    let link = ResponseLink {
        testament: TestamentId(identity.binding.object.0),
        content: identity.binding.content,
        receipt: identity.receipt,
        cycle: identity.cycle,
        prior: identity.prior,
    };
    if value.history.link != link {
        return Err(ContractError::ContentConflict);
    }
    let posted = value.response.state() != ResponseState::Generated;
    let received = value.response.state().delivered();
    if value.history.posted != posted
        || value.history.received && !received
        || received && !value.history.received && !terminal(fields.status) && !fields.local_complete
    {
        return Err(ContractError::InvalidTransition);
    }
    Ok(ResponseRecord {
        link,
        stamp: value.response.report_stamp(),
        posted: value.history.posted,
        received: value.history.received,
    })
}
fn record_order(
    fields: ClaimSnapshotV1,
    index: usize,
    previous: Option<ResponseLink>,
    row: ResponseRecord,
) -> Result<(), ContractError> {
    let cycle = u32::try_from(index)
        .map_err(|_| ContractError::Capacity)?
        .checked_add(1)
        .ok_or(ContractError::Capacity)?;
    let receipt = fields.receipt.ok_or(ContractError::StaleReceipt)?;
    if row.link.testament.is_zero()
        || row.link.cycle != cycle
        || row.link.prior != previous.map(|link| link.testament)
        || row.received && !row.posted
    {
        return Err(ContractError::InvalidTarget);
    }
    let fence = row.link.receipt;
    if fence.receipt.is_zero()
        || fence.epoch == 0
        || fence.epoch > receipt.fence.epoch
        || fence.epoch == receipt.fence.epoch && fence != receipt.fence
        || previous.is_some_and(|old| {
            old.receipt.epoch > fence.epoch
                || old.receipt.epoch == fence.epoch && old.receipt != fence
        })
    {
        return Err(ContractError::StaleReceipt);
    }
    Ok(())
}
fn response_cut_target(fields: ClaimSnapshotV1) -> Option<TestamentId> {
    match fields.terminal_cut {
        Some(ClaimTerminalSnapshotV1::Required(cut)) => match cut.cause.key.target {
            aggregation::CauseTarget::Response(id) => Some(id),
            aggregation::CauseTarget::Admission | aggregation::CauseTarget::Increment { .. } => {
                None
            }
        },
        _ => None,
    }
}
fn response_cut_matches(fields: ClaimSnapshotV1, row: ResponseRecord) -> bool {
    response_cut_target(fields) == Some(row.link.testament)
        && row.received
        && fields
            .receipt
            .is_some_and(|receipt| receipt.fence == row.link.receipt)
}
fn received_state(fields: ClaimSnapshotV1, any: bool, current: bool) -> Result<(), ContractError> {
    if matches!(
        fields.status,
        ClaimStatus::TestamentAcknowledged
            | ClaimStatus::Validating
            | ClaimStatus::Satisfied
            | ClaimStatus::ValidationIncomplete
            | ClaimStatus::ValidationFailed
            | ClaimStatus::ValidationErrored
    ) && !any
        || fields.status == ClaimStatus::TestamentGenerated && any
        || fields.local_complete && !current
    {
        return Err(ContractError::InvalidTransition);
    }
    Ok(())
}
fn check_records(
    fields: ClaimSnapshotV1,
    rows: &[ResponseRecord],
    limit: usize,
) -> Result<(), ContractError> {
    let mut work = Work::new(limit);
    let mut previous = None;
    let mut any = false;
    let mut current = false;
    let mut cause_received = response_cut_target(fields).is_none();
    for (index, row) in rows.iter().copied().enumerate() {
        work.charge(33)?;
        record_order(fields, index, previous, row)?;
        for old in rows.iter().take(index) {
            work.charge(1)?;
            if old.link.testament == row.link.testament {
                return Err(ContractError::InvalidTarget);
            }
        }
        cause_received |= response_cut_matches(fields, row);
        previous = Some(row.link);
        any |= row.received;
        current |= row.received
            && fields
                .receipt
                .is_some_and(|receipt| receipt.fence == row.link.receipt);
    }
    if !cause_received {
        return Err(ContractError::InvalidTarget);
    }
    received_state(fields, any, current)
}
fn hash_response(hash: &mut Hash, row: ResponseRecord) {
    hash.raw(&row.link.testament.0);
    hash.raw(&row.link.content.0);
    hash.raw(&row.link.receipt.receipt.0);
    hash.u64(row.link.receipt.epoch);
    hash.u64(u64::from(row.link.cycle));
    if let Some(prior) = row.link.prior {
        hash.u8(1);
        hash.raw(&prior.0);
    } else {
        hash.u8(0);
    }
    hash.u8(u8::from(row.posted));
    hash.u8(u8::from(row.received));
    hash.raw(row.stamp.as_bytes());
}
fn response_state(state: ResponseState) -> u8 {
    match state {
        ResponseState::Generated => 0,
        ResponseState::Posted => 1,
        ResponseState::Received => 2,
        ResponseState::Validating => 3,
        ResponseState::Validated => 4,
        ResponseState::ValidationIncomplete => 5,
        ResponseState::ValidationFailed => 6,
        ResponseState::ValidationErrored => 7,
    }
}
