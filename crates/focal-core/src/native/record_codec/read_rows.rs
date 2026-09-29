//! Allocation-free decoding of fixed retained rows and exact event construction.
//! These checks establish intrinsic identity/shape only. Complete root counts,
//! index membership and contiguous publication history remain importer work.
use super::*;
use bytes::Cursor;
use read_fields as f;

#[cfg(test)]
#[path = "read_rows_tests.rs"]
mod tests;

const FIXED_CHECK_VISITS: usize = 1024;
const EVENT_CHECK_VISITS: usize = 4096;
const EVENT_BUILD_VISITS: usize = 256;

fn codec(error: CodecError) -> NativeError {
    read_source::model_error(error).into()
}
fn invalid() -> NativeError {
    ContractError::InvalidManifest.into()
}
fn count(c: &mut Cursor<'_>) -> Result<usize, CodecError> {
    let count = c.u64()?;
    c.visit(1)?;
    usize::try_from(count).map_err(|_| CodecError::Capacity)
}
fn optional_id(c: &mut Cursor<'_>) -> Result<Option<[u8; 16]>, CodecError> {
    f::optional(c, |c| c.fixed())
}
fn cycle(c: &mut Cursor<'_>) -> Result<NativeCycleKey, CodecError> {
    Ok(NativeCycleKey {
        claim: ClaimId(c.fixed()?),
        receipt: ReceiptId(c.fixed()?),
        epoch: c.u64()?,
        cycle: c.u32()?,
    })
}
fn optional_cycle(c: &mut Cursor<'_>) -> Result<Option<NativeCycleKey>, CodecError> {
    f::optional(c, cycle)
}

/// Unsupported/heap-bearing families return None without reading any bytes.
/// Returned rows own no heap. The enclosing decoder checks exact body exhaustion
/// and incorporates cursor work into its cumulative preparation allowance.
pub(super) fn read_fixed(
    key: Key,
    cursor: &mut Cursor<'_>,
    ledger: LedgerId,
) -> Result<Option<Row>, NativeError> {
    let row = match decode_fixed(key, cursor).map_err(codec)? {
        Some(row) => row,
        None => return Ok(None),
    };
    cursor.visit(FIXED_CHECK_VISITS).map_err(codec)?;
    if ledger.tenant.is_zero() || ledger.session.is_zero() {
        return Err(ContractError::WrongLedger.into());
    }
    check_fixed(key, &row, ledger)?;
    Ok(Some(row))
}

fn decode_fixed(key: Key, c: &mut Cursor<'_>) -> Result<Option<Row>, CodecError> {
    Ok(Some(match key {
        Key::IncomingHead(_) => Row::IncomingHead(incoming_graph::IncomingHead {
            head: optional_id(c)?.map(ClaimId),
            count: count(c)?,
        }),
        Key::IncomingLink(..) => Row::IncomingLink(incoming_graph::IncomingLink {
            next: optional_id(c)?.map(ClaimId),
        }),
        Key::Monitor(_) => Row::Monitor(monitor_index::MonitorAllocation {
            owner: f::binding(c)?,
            registered: f::sequence(c)?,
            deadline: f::deadline(c)?,
        }),
        Key::MonitorHead(_) => Row::MonitorHead(monitor_index::MonitorHead {
            head: optional_id(c)?.map(MonitorId),
            count: count(c)?,
        }),
        Key::MonitorLink(..) => Row::MonitorLink(f::optional(c, |c| {
            Ok(monitor_index::MonitorLink {
                owner: ClaimId(c.fixed()?),
                registered: f::sequence(c)?,
                stamp: f::sequence(c)?,
                previous: optional_id(c)?.map(MonitorId),
                next: optional_id(c)?.map(MonitorId),
            })
        })?),
        Key::Meta => Row::Meta(Meta {
            claims: count(c)?,
            outcomes: count(c)?,
            events: count(c)?,
            definitions: count(c)?,
            evaluations: count(c)?,
            artifacts: count(c)?,
            results: count(c)?,
            receipts: count(c)?,
            responses: count(c)?,
            result_testaments: count(c)?,
            monitors: count(c)?,
            monitor_links: count(c)?,
            creation_results: count(c)?,
            legacy: count(c)?,
            logical_time: c.u64()?,
        }),
        Key::ArtifactIdentity(_) => Row::ArtifactIdentity(ArtifactId(c.fixed()?)),
        Key::Receipt(_) => Row::Receipt(NativeReceipt {
            claim: ClaimId(c.fixed()?),
            fence: f::receipt(c)?,
            holder: f::participant(c)?,
            acquired: f::sequence(c)?,
        }),
        Key::Cycle(_) => Row::Cycle(NativeCycle {
            work_head: optional_id(c)?.map(ArtifactId),
            work_count: count(c)?,
            diagnostic_head: optional_id(c)?.map(ArtifactId),
            diagnostic_count: count(c)?,
            response: optional_id(c)?.map(TestamentId),
        }),
        Key::RetiredCycleHead(_) => Row::RetiredCycleHead(RetiredCycleHead {
            head: optional_cycle(c)?,
            count: count(c)?,
            work_count: count(c)?,
        }),
        Key::Retired(_) => Row::Retired(RetiredClaim {
            bundle: read_fields::hash(c)?,
            bytes: c.u64()?,
            through: SessionSeq(c.u64()?),
            binding: read_fields::binding(c)?,
            status: read_fields::claim_status(c)?,
            retired_at: SessionSeq(c.u64()?),
            events: c.u32()?,
        }),
        Key::RetiredCycle(_) => Row::RetiredCycle(RetiredCycle {
            holder: f::participant(c)?,
            next: optional_cycle(c)?,
        }),
        Key::WorkSlot(..) => Row::WorkSlot(ArtifactId(c.fixed()?)),
        Key::ClaimResultTestament(_) => Row::ClaimResultTestament(TestamentId(c.fixed()?)),
        Key::Outcome(_) => Row::Outcome(fixed::read_outcome(c)?),
        Key::ClaimIdentity(..) => Row::ClaimIdentity(ClaimId(c.fixed()?)),
        Key::DefinitionIdentity(..) => Row::DefinitionIdentity(ValidationId(c.fixed()?)),
        Key::ByIssuer(..)
        | Key::BySubject(..)
        | Key::ByStatus(..)
        | Key::ByAction(..)
        | Key::ByScope(..)
        | Key::ByRelation(..)
        | Key::ByProducer(..)
        | Key::ByArtifactKind(..)
        | Key::BySchema(..)
        | Key::ArtifactInput(..)
        | Key::ByEvaluator(..)
        | Key::ByVerdict(..)
        | Key::ByCreated(..)
        | Key::DueTimer(..)
        | Key::ByObject(..) => {
            if c.u8()? != 1 {
                return Err(CodecError::InvalidTag("index row"));
            }
            Row::Index
        }
        _ => return Ok(None),
    }))
}

fn optional_nonzero(value: Option<[u8; 16]>) -> bool {
    value.is_none_or(|id| id != [0; 16])
}
fn chain(head: Option<[u8; 16]>, count: usize) -> bool {
    optional_nonzero(head) && head.is_some() == (count != 0)
}
fn valid_cycle(key: NativeCycleKey) -> bool {
    !key.claim.is_zero() && !key.receipt.is_zero() && key.epoch != 0 && key.cycle != 0
}
fn valid_receipt(value: ReceiptFence) -> bool {
    !value.receipt.is_zero() && value.epoch != 0
}
fn valid_deadline(value: Deadline) -> bool {
    !value.timer.is_zero() && value.generation != 0
}
fn binding(value: Binding, ledger: LedgerId) -> Result<(), NativeError> {
    if value.ledger != ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if value.object.is_zero() || value.content.0 == [0; 32] || value.revision.0 == 0 {
        return Err(invalid());
    }
    Ok(())
}
fn bindings(before: Option<Binding>, after: Binding, ledger: LedgerId) -> Result<(), NativeError> {
    binding(after, ledger)?;
    if let Some(before) = before {
        binding(before, ledger)?;
        if before.object != after.object || before.content != after.content {
            return Err(invalid());
        }
    }
    Ok(())
}
fn evaluation(key: EvaluationKey) -> bool {
    !key.claim.is_zero()
        && !key.validation.is_zero()
        && key.generation != 0
        && match key.target {
            EvaluationTarget::Admission => true,
            EvaluationTarget::Increment { artifact } => !artifact.is_zero(),
            EvaluationTarget::Work {
                response, artifact, ..
            } => !response.is_zero() && !artifact.is_zero(),
            EvaluationTarget::MissingSlot { response, .. }
            | EvaluationTarget::Delivery { response } => !response.is_zero(),
        }
}
fn invocation(value: NativeInvocation) -> bool {
    match value {
        NativeInvocation::Request(key) => {
            !key.principal.is_zero() && !key.id.is_zero() && key.epoch.0 != 0
        }
        NativeInvocation::EvaluationDeadline(key) => {
            evaluation(key.evaluation) && !key.timer.is_zero() && key.generation != 0
        }
        NativeInvocation::ClaimDeadline(key) => {
            !key.claim.is_zero() && !key.timer.is_zero() && key.generation != 0
        }
        NativeInvocation::MonitorDeadline(key) => {
            !key.claim.is_zero()
                && !key.monitor.is_zero()
                && !key.timer.is_zero()
                && key.generation != 0
        }
        NativeInvocation::Import => true,
        NativeInvocation::Retirement(root) => !root.is_zero(),
    }
}

pub(super) fn check_fixed(key: Key, row: &Row, ledger: LedgerId) -> Result<(), NativeError> {
    let valid = match (key, row) {
        (Key::IncomingHead(target), Row::IncomingHead(row)) => {
            !target.is_zero() && chain(row.head.map(|id| id.0), row.count)
        }
        (Key::IncomingLink(target, dependent), Row::IncomingLink(row)) => {
            !target.is_zero()
                && !dependent.is_zero()
                && optional_nonzero(row.next.map(|id| id.0))
                && row.next != Some(dependent)
        }
        (Key::Monitor(id), Row::Monitor(row)) => {
            binding(row.owner, ledger)?;
            !id.is_zero() && row.registered.0 != 0 && valid_deadline(row.deadline)
        }
        (Key::MonitorHead(target), Row::MonitorHead(row)) => {
            !target.is_zero() && chain(row.head.map(|id| id.0), row.count)
        }
        (Key::MonitorLink(target, id), Row::MonitorLink(row)) => {
            !target.is_zero()
                && !id.is_zero()
                && row.is_none_or(|row| {
                    !row.owner.is_zero()
                        && row.registered.0 != 0
                        && row.stamp >= row.registered
                        && optional_nonzero(row.previous.map(|id| id.0))
                        && optional_nonzero(row.next.map(|id| id.0))
                        && row.previous != Some(id)
                        && row.next != Some(id)
                        && (row.previous.is_none() || row.previous != row.next)
                })
        }
        (Key::Meta, Row::Meta(_)) => true,
        (Key::ArtifactIdentity(hash), Row::ArtifactIdentity(id)) => {
            hash.0 != [0; 32] && !id.is_zero()
        }
        (Key::Receipt(id), Row::Receipt(row)) => {
            !id.is_zero()
                && row.fence.receipt == id
                && valid_receipt(row.fence)
                && !row.claim.is_zero()
                && !row.holder.is_zero()
                && row.acquired.0 != 0
        }
        (Key::Cycle(key), Row::Cycle(row)) => {
            valid_cycle(key)
                && chain(row.work_head.map(|id| id.0), row.work_count)
                && chain(row.diagnostic_head.map(|id| id.0), row.diagnostic_count)
                && optional_nonzero(row.response.map(|id| id.0))
        }
        (Key::Retired(claim), Row::Retired(row)) => {
            !claim.is_zero()
                && row.binding.object.0 == claim.0
                && row.binding.ledger == ledger
                && row.binding.revision.0 != 0
                && row.bundle.0 != [0; 32]
                && row.bytes != 0
                && row.through.0 != 0
                && row.retired_at > row.through
                && row.status.is_terminal()
        }
        (Key::RetiredCycleHead(claim), Row::RetiredCycleHead(row)) => {
            !claim.is_zero()
                && row.head.is_some() == (row.count != 0)
                && (row.count != 0 || row.work_count == 0)
                && row
                    .head
                    .is_none_or(|key| valid_cycle(key) && key.claim == claim)
        }
        (Key::RetiredCycle(key), Row::RetiredCycle(row)) => {
            valid_cycle(key)
                && !row.holder.is_zero()
                && row.next.is_none_or(|next| {
                    valid_cycle(next)
                        && next.claim == key.claim
                        && next.epoch < key.epoch
                        && next.cycle <= key.cycle
                })
        }
        (Key::WorkSlot(key, _), Row::WorkSlot(id)) => valid_cycle(key) && !id.is_zero(),
        (Key::ClaimResultTestament(claim), Row::ClaimResultTestament(id)) => {
            !claim.is_zero() && !id.is_zero()
        }
        (Key::Outcome(key), Row::Outcome(row)) => {
            if row.ledger != ledger {
                return Err(ContractError::WrongLedger.into());
            }
            let namespace = match key {
                NativeInvocation::Request(_) => !matches!(
                    row.operation,
                    NativeOperation::EvaluationDeadline
                        | NativeOperation::ClaimDeadline
                        | NativeOperation::MonitorDeadline
                        | NativeOperation::Import
                        | NativeOperation::Retire
                ),
                NativeInvocation::EvaluationDeadline(_) => {
                    row.operation == NativeOperation::EvaluationDeadline
                }
                NativeInvocation::ClaimDeadline(_) => {
                    row.operation == NativeOperation::ClaimDeadline
                }
                NativeInvocation::MonitorDeadline(_) => {
                    row.operation == NativeOperation::MonitorDeadline
                }
                NativeInvocation::Import => {
                    row.operation == NativeOperation::Import && row.sequence == SessionSeq(1)
                }
                NativeInvocation::Retirement(_) => {
                    row.operation == NativeOperation::Retire && row.events == 0
                }
            };
            key == row.invocation
                && invocation(key)
                && row.sequence.0 != 0
                && row.intent.0 != [0; 32]
                && namespace
        }
        (Key::ClaimIdentity(schema, hash), Row::ClaimIdentity(id)) => {
            schema != 0 && hash.0 != [0; 32] && !id.is_zero()
        }
        (Key::DefinitionIdentity(schema, hash), Row::DefinitionIdentity(id)) => {
            schema != 0 && hash.0 != [0; 32] && !id.is_zero()
        }
        (Key::ByIssuer(participant, claim) | Key::BySubject(participant, claim), Row::Index) => {
            !participant.is_zero() && !claim.is_zero()
        }
        (Key::ByStatus(code, claim), Row::Index) => {
            ClaimStatus::from_code(code).is_some() && !claim.is_zero()
        }
        (Key::ByAction(code, claim), Row::Index) => {
            focal_model::ActionType::from_code(code).is_some() && !claim.is_zero()
        }
        (Key::ByScope(kind, hash, claim), Row::Index) => {
            focal_model::ScopeKind::from_code(kind).is_some()
                && hash.0 != [0; 32]
                && !claim.is_zero()
        }
        (Key::ByRelation(kind, target, claim), Row::Index) => {
            focal_model::RelationKind::from_code(kind).is_some()
                && !target.is_zero()
                && !claim.is_zero()
                && target != claim
        }
        (Key::ByProducer(participant, artifact), Row::Index) => {
            !participant.is_zero() && !artifact.is_zero()
        }
        (Key::ByArtifactKind(hash, artifact) | Key::BySchema(hash, artifact), Row::Index) => {
            hash.0 != [0; 32] && !artifact.is_zero()
        }
        (Key::ArtifactInput(input, artifact), Row::Index) => {
            !input.is_zero() && !artifact.is_zero()
        }
        (Key::ByEvaluator(participant, validation), Row::Index) => {
            !participant.is_zero() && !validation.is_zero()
        }
        (Key::ByVerdict(code, key), Row::Index) => {
            focal_model::VerdictValue::from_code(code).is_some() && evaluation(key.evaluation)
        }
        (Key::ByCreated(family, sequence, object), Row::Index) => {
            focal_model::ObjectKind::from_code(family).is_some()
                && sequence.0 != 0
                && !object.is_zero()
        }
        (Key::ByObject(family, object), Row::Index) => {
            matches!(
                focal_model::ObjectKind::from_code(family),
                Some(
                    focal_model::ObjectKind::Claim
                        | focal_model::ObjectKind::Artifact
                        | focal_model::ObjectKind::Validation
                )
            ) && !object.is_zero()
        }
        (Key::DueTimer(at, target), Row::Index) => {
            at != 0
                && match target {
                    TimerTarget::Claim(claim) => !claim.is_zero(),
                    TimerTarget::Evaluation(key) => evaluation(key),
                    TimerTarget::Monitor(claim, monitor) => !claim.is_zero() && !monitor.is_zero(),
                }
        }
        _ => false,
    };
    if valid { Ok(()) } else { Err(invalid()) }
}

/// Event body parsing, intrinsic checks and compact packing are allocation-free.
/// The plan carries the exact original publication fact, without reexecuting it.
pub(super) struct EventPlan {
    event: history::StoredEvent,
}

impl EventPlan {
    pub(super) fn read(
        key: Key,
        cursor: &mut Cursor<'_>,
        ledger: LedgerId,
    ) -> Result<Self, NativeError> {
        let Key::Event(sequence, ordinal) = key else {
            return Err(invalid());
        };
        let event = read_events::event(cursor).map_err(codec)?;
        cursor.visit(EVENT_CHECK_VISITS).map_err(codec)?;
        if sequence.0 == 0
            || event.sequence != sequence
            || event.ordinal != ordinal
            || ledger.tenant.is_zero()
            || ledger.session.is_zero()
            || !invocation(event.invocation)
        {
            return Err(invalid());
        }
        check_event(event, ledger)?;
        let packed = history::StoredEvent::pack(event)?;
        if packed.expand(ledger) != event {
            return Err(ContractError::WrongLedger.into());
        }
        Ok(Self { event: packed })
    }
    pub(super) const fn heap_bytes(&self) -> usize {
        OwnedEvent::container_charge()
    }
    pub(super) const fn build_visits(&self) -> usize {
        EVENT_BUILD_VISITS
    }
    pub(super) fn build(
        self,
        allowance: usize,
        max_visits: usize,
    ) -> Result<(Row, usize), NativeError> {
        if self.heap_bytes() > allowance || self.build_visits() > max_visits {
            return Err(ContractError::Capacity.into());
        }
        let row = OwnedEvent::new(self.event)?;
        let actual = row.heap_charge()?;
        if actual > allowance {
            return Err(ContractError::Capacity.into());
        }
        Ok((Row::Event(row), actual))
    }
}

fn check_event(event: NativeEvent, ledger: LedgerId) -> Result<(), NativeError> {
    let valid = match event.fact {
        NativeFact::ResultTestament {
            claim,
            before,
            after,
            ..
        }
        | NativeFact::Work {
            claim,
            before,
            after,
            ..
        }
        | NativeFact::Response {
            claim,
            before,
            after,
            ..
        } => {
            bindings(before, after, ledger)?;
            !claim.is_zero()
        }
        NativeFact::Missing { key }
        | NativeFact::Delivery { key }
        | NativeFact::Accepted { key } => evaluation(key.evaluation) && key.revision.0 != 0,
        NativeFact::Registrations { claim } => {
            binding(claim, ledger)?;
            true
        }
        NativeFact::Diagnostic {
            claim,
            binding: value,
            ..
        } => {
            binding(value, ledger)?;
            !claim.is_zero()
        }
        NativeFact::Receipt {
            claim,
            fence,
            holder,
        } => {
            binding(claim, ledger)?;
            valid_receipt(fence) && !holder.is_zero()
        }
        NativeFact::ReceiptAdopted {
            claim,
            previous,
            replacement,
            cause,
        } => {
            binding(claim, ledger)?;
            valid_receipt(previous.fence)
                && valid_receipt(replacement.fence)
                && !previous.holder.is_zero()
                && !replacement.holder.is_zero()
                && cause.0 != [0; 32]
        }
        NativeFact::Artifact { binding: value } => {
            binding(value, ledger)?;
            true
        }
        NativeFact::Claim(row) => {
            bindings(row.before, row.after, ledger)?;
            if let Some(child) = row.owned_child {
                binding(child, ledger)?;
            }
            // Monitor cut authority and exact revision history are checked by
            // the complete importer, not reconstructed from this state tag.
            !matches!(row.kind, NativeEventKind::Monitor(value) if value.id().is_zero())
        }
        NativeFact::Definition {
            binding: value,
            claim,
            intent,
            ..
        } => {
            binding(value, ledger)?;
            !claim.is_zero() && intent.0 != [0; 32]
        }
        NativeFact::Evaluation {
            key,
            before,
            after,
            attempt,
            ..
        } => {
            bindings(before, after, ledger)?;
            evaluation(key)
                && after.object.0 == key.validation.0
                && attempt.is_none_or(|attempt| {
                    !attempt.handler.is_zero()
                        && attempt.version.0 != [0; 32]
                        && !attempt.evaluator.is_zero()
                        && attempt.definition.0 != [0; 32]
                })
        }
    };
    if valid { Ok(()) } else { Err(invalid()) }
}
