//! Explicit persisted key, invocation and outcome tags. Rust enum layout is not
//! part of this format. Every variable target has a bounded fixed-width shape.
use super::*;
use bytes::{Cursor, Error, Sink, write_raw as raw, write_u8, write_u16, write_u32, write_u64};
use focal_model::{ObjectRevision, RequestEpoch, RequestId, SessionId, TenantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFamily {
    IncomingHead,
    IncomingLink,
    Monitor,
    MonitorHead,
    MonitorLink,
    MissingResult,
    Meta,
    Claim,
    Definition,
    Evaluation,
    Artifact,
    ArtifactIdentity,
    Accepted,
    DeliveryResult,
    Receipt,
    Cycle,
    RetiredCycleHead,
    RetiredCycle,
    Work,
    WorkSlot,
    Diagnostic,
    Response,
    ResultTestament,
    ClaimResultTestament,
    Outcome,
    Event,
    ClaimContent,
    ClaimIdentity,
    DefinitionIdentity,
    CreationResult,
    LegacyTestament,
    LegacyEvidenceSet,
    LegacyRun,
    LegacyDefinition,
    Index,
}

pub(super) fn family(value: Key) -> Result<RowFamily, Error> {
    Ok(match value {
        Key::IncomingHead(_) => RowFamily::IncomingHead,
        Key::IncomingLink(..) => RowFamily::IncomingLink,
        Key::Monitor(_) => RowFamily::Monitor,
        Key::MonitorHead(_) => RowFamily::MonitorHead,
        Key::MonitorLink(..) => RowFamily::MonitorLink,
        Key::MissingResult(_) => RowFamily::MissingResult,
        Key::Meta => RowFamily::Meta,
        Key::Claim(_) => RowFamily::Claim,
        Key::Definition(_) => RowFamily::Definition,
        Key::Evaluation(_) => RowFamily::Evaluation,
        Key::Artifact(_) => RowFamily::Artifact,
        Key::ArtifactIdentity(_) => RowFamily::ArtifactIdentity,
        Key::Accepted(_) => RowFamily::Accepted,
        Key::DeliveryResult(_) => RowFamily::DeliveryResult,
        Key::Receipt(_) => RowFamily::Receipt,
        Key::Cycle(_) => RowFamily::Cycle,
        Key::RetiredCycleHead(_) => RowFamily::RetiredCycleHead,
        Key::RetiredCycle(_) => RowFamily::RetiredCycle,
        Key::Work(_) => RowFamily::Work,
        Key::WorkSlot(..) => RowFamily::WorkSlot,
        Key::Diagnostic(_) => RowFamily::Diagnostic,
        Key::Response(_) => RowFamily::Response,
        Key::ResultTestament(_) => RowFamily::ResultTestament,
        Key::ClaimResultTestament(_) => RowFamily::ClaimResultTestament,
        Key::Outcome(_) => RowFamily::Outcome,
        Key::Event(..) => RowFamily::Event,
        Key::ClaimContent(_) => RowFamily::ClaimContent,
        Key::ClaimIdentity(..) => RowFamily::ClaimIdentity,
        Key::DefinitionIdentity(..) => RowFamily::DefinitionIdentity,
        Key::CreationResult(_) => RowFamily::CreationResult,
        Key::LegacyTestament(_) => RowFamily::LegacyTestament,
        Key::LegacyEvidenceSet(_) => RowFamily::LegacyEvidenceSet,
        Key::LegacyRun(..) => RowFamily::LegacyRun,
        Key::LegacyDefinition(_) => RowFamily::LegacyDefinition,
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
        | Key::DueTimer(..) => RowFamily::Index,
        Key::End => return Err(Error::InvalidTag("sentinel key")),
    })
}

pub(super) fn key(s: &mut impl Sink, key: Key) -> Result<(), Error> {
    let tag = match key {
        Key::IncomingHead(_) => 0,
        Key::IncomingLink(..) => 1,
        Key::Monitor(_) => 2,
        Key::MonitorHead(_) => 3,
        Key::MonitorLink(..) => 4,
        Key::MissingResult(_) => 5,
        Key::Meta => 6,
        Key::Claim(_) => 7,
        Key::Definition(_) => 8,
        Key::Evaluation(_) => 9,
        Key::Artifact(_) => 10,
        Key::ArtifactIdentity(_) => 11,
        Key::Accepted(_) => 12,
        Key::DeliveryResult(_) => 13,
        Key::Receipt(_) => 14,
        Key::Cycle(_) => 15,
        Key::RetiredCycleHead(_) => 16,
        Key::RetiredCycle(_) => 17,
        Key::Work(_) => 18,
        Key::WorkSlot(..) => 19,
        Key::Diagnostic(_) => 20,
        Key::Response(_) => 21,
        Key::ResultTestament(_) => 22,
        Key::ClaimResultTestament(_) => 23,
        Key::Outcome(_) => 24,
        Key::Event(..) => 25,
        Key::ClaimContent(_) => 26,
        Key::ClaimIdentity(..) => 27,
        Key::DefinitionIdentity(..) => 28,
        Key::CreationResult(_) => 29,
        Key::LegacyTestament(_) => 30,
        Key::LegacyEvidenceSet(_) => 31,
        Key::LegacyRun(..) => 32,
        Key::LegacyDefinition(_) => 33,
        Key::ByIssuer(..) => 34,
        Key::BySubject(..) => 35,
        Key::ByStatus(..) => 36,
        Key::ByAction(..) => 37,
        Key::ByScope(..) => 38,
        Key::ByRelation(..) => 39,
        Key::ByProducer(..) => 40,
        Key::ByArtifactKind(..) => 41,
        Key::BySchema(..) => 42,
        Key::ArtifactInput(..) => 43,
        Key::ByEvaluator(..) => 44,
        Key::ByVerdict(..) => 45,
        Key::ByCreated(..) => 46,
        Key::DueTimer(..) => 47,
        Key::End => return Err(Error::InvalidTag("sentinel key")),
    };
    write_u8(s, tag)?;
    match key {
        Key::IncomingHead(id)
        | Key::MonitorHead(id)
        | Key::Claim(id)
        | Key::RetiredCycleHead(id)
        | Key::ClaimResultTestament(id)
        | Key::ClaimContent(id) => raw(s, &id.0),
        Key::IncomingLink(a, b) => {
            raw(s, &a.0)?;
            raw(s, &b.0)
        }
        Key::Monitor(id) => raw(s, &id.0),
        Key::MonitorLink(a, b) => {
            raw(s, &a.0)?;
            raw(s, &b.0)
        }
        Key::MissingResult(k) | Key::Accepted(k) | Key::DeliveryResult(k) => result_key(s, k),
        Key::Meta => Ok(()),
        Key::Definition(id) => raw(s, &id.0),
        Key::Evaluation(k) => types::evaluation(s, k),
        Key::Artifact(id) | Key::Work(id) | Key::Diagnostic(id) => raw(s, &id.0),
        Key::ArtifactIdentity(hash) => raw(s, &hash.0),
        Key::Receipt(id) => raw(s, &id.0),
        Key::Cycle(k) | Key::RetiredCycle(k) => cycle(s, k),
        Key::WorkSlot(k, slot) => {
            cycle(s, k)?;
            write_u32(s, slot)
        }
        Key::Response(id) | Key::ResultTestament(id) => raw(s, &id.0),
        Key::Outcome(k) | Key::CreationResult(k) => invocation(s, k),
        Key::Event(seq, ordinal) => {
            write_u64(s, seq.0)?;
            write_u32(s, ordinal)
        }
        Key::ClaimIdentity(schema, hash) | Key::DefinitionIdentity(schema, hash) => {
            write_u16(s, schema)?;
            raw(s, &hash.0)
        }
        Key::LegacyTestament(id) => raw(s, &id.0),
        Key::LegacyEvidenceSet(id) => raw(s, &id.0),
        Key::LegacyRun(id, ordinal) => {
            raw(s, &id.0)?;
            write_u32(s, ordinal)
        }
        Key::LegacyDefinition(id) => raw(s, &id.0),
        Key::ByIssuer(participant, claim) | Key::BySubject(participant, claim) => {
            raw(s, &participant.0)?;
            raw(s, &claim.0)
        }
        Key::ByStatus(code, claim) | Key::ByAction(code, claim) => {
            write_u16(s, code)?;
            raw(s, &claim.0)
        }
        Key::ByScope(kind, hash, claim) => {
            write_u16(s, kind)?;
            raw(s, &hash.0)?;
            raw(s, &claim.0)
        }
        Key::ByRelation(kind, target, claim) => {
            write_u16(s, kind)?;
            raw(s, &target.0)?;
            raw(s, &claim.0)
        }
        Key::ByProducer(participant, artifact) => {
            raw(s, &participant.0)?;
            raw(s, &artifact.0)
        }
        Key::ByArtifactKind(hash, artifact) | Key::BySchema(hash, artifact) => {
            raw(s, &hash.0)?;
            raw(s, &artifact.0)
        }
        Key::ArtifactInput(input, artifact) => {
            raw(s, &input.0)?;
            raw(s, &artifact.0)
        }
        Key::ByEvaluator(participant, validation) => {
            raw(s, &participant.0)?;
            raw(s, &validation.0)
        }
        Key::ByVerdict(code, k) => {
            write_u16(s, code)?;
            result_key(s, k)
        }
        Key::ByCreated(family, sequence, object) => {
            write_u16(s, family)?;
            write_u64(s, sequence.0)?;
            raw(s, &object.0)
        }
        Key::DueTimer(at, target) => {
            write_u64(s, at)?;
            match target {
                TimerTarget::Claim(claim) => {
                    write_u8(s, 0)?;
                    raw(s, &claim.0)
                }
                TimerTarget::Evaluation(k) => {
                    write_u8(s, 1)?;
                    types::evaluation(s, k)
                }
                TimerTarget::Monitor(claim, monitor) => {
                    write_u8(s, 2)?;
                    raw(s, &claim.0)?;
                    raw(s, &monitor.0)
                }
            }
        }
        Key::End => Err(Error::InvalidTag("sentinel key")),
    }
}
pub(super) fn cycle(s: &mut impl Sink, k: NativeCycleKey) -> Result<(), Error> {
    raw(s, &k.claim.0)?;
    raw(s, &k.receipt.0)?;
    write_u64(s, k.epoch)?;
    write_u32(s, k.cycle)
}
pub(super) fn result_key(s: &mut impl Sink, k: NativeResultKey) -> Result<(), Error> {
    types::evaluation(s, k.evaluation)?;
    write_u64(s, k.revision.0)
}
pub(super) fn invocation(s: &mut impl Sink, v: NativeInvocation) -> Result<(), Error> {
    match v {
        NativeInvocation::Request(k) => {
            write_u8(s, 0)?;
            raw(s, &k.principal.0)?;
            write_u64(s, k.epoch.0)?;
            raw(s, &k.id.0)
        }
        NativeInvocation::EvaluationDeadline(k) => {
            write_u8(s, 1)?;
            types::evaluation(s, k.evaluation)?;
            raw(s, &k.timer.0)?;
            write_u64(s, k.generation)
        }
        NativeInvocation::ClaimDeadline(k) => {
            write_u8(s, 2)?;
            raw(s, &k.claim.0)?;
            raw(s, &k.timer.0)?;
            write_u64(s, k.generation)
        }
        NativeInvocation::MonitorDeadline(k) => {
            write_u8(s, 3)?;
            raw(s, &k.claim.0)?;
            raw(s, &k.monitor.0)?;
            raw(s, &k.timer.0)?;
            write_u64(s, k.generation)
        }
        NativeInvocation::Import => write_u8(s, 4),
    }
}
pub(super) fn outcome(s: &mut impl Sink, v: NativeOutcome) -> Result<(), Error> {
    types::ledger(s, v.ledger)?;
    invocation(s, v.invocation)?;
    write_u64(s, v.sequence.0)?;
    write_u64(s, v.logical_time)?;
    write_u8(s, operation(v.operation))?;
    raw(s, &v.intent.0)?;
    s.visit(11)?;
    for count in [
        v.created,
        v.changed,
        v.definitions,
        v.evaluations,
        v.artifacts,
        v.results,
        v.receipts,
        v.responses,
        v.result_testaments,
        v.events,
    ] {
        write_u32(s, count)?;
    }
    Ok(())
}
fn operation(v: NativeOperation) -> u8 {
    match v {
        NativeOperation::RegisterMonitor => 0,
        NativeOperation::RebindMonitor => 1,
        NativeOperation::CancelMonitor => 2,
        NativeOperation::MonitorDeadline => 3,
        NativeOperation::ReleaseScope => 4,
        NativeOperation::GenerateResultTestament => 5,
        NativeOperation::PostResultTestament => 6,
        NativeOperation::EnterWholeWork => 7,
        NativeOperation::SealIncrementTargets => 8,
        NativeOperation::BeginIncrement => 9,
        NativeOperation::ReportIncrement => 10,
        NativeOperation::FailWorkProduction => 11,
        NativeOperation::RejectWork => 12,
        NativeOperation::SubmitWork => 13,
        NativeOperation::SubmitDiagnostic => 14,
        NativeOperation::ReceiveWork => 15,
        NativeOperation::CloseResponse => 16,
        NativeOperation::PostResponse => 17,
        NativeOperation::ReceiveResponse => 18,
        NativeOperation::AcquireReceipt => 19,
        NativeOperation::AdoptReceipt => 20,
        NativeOperation::Create => 21,
        NativeOperation::Cancel => 22,
        NativeOperation::Post => 23,
        NativeOperation::BeginAdmission => 24,
        NativeOperation::ReportAdmission => 25,
        NativeOperation::BeginWork => 26,
        NativeOperation::ReportWork => 27,
        NativeOperation::EvaluationDeadline => 28,
        NativeOperation::ClaimDeadline => 29,
        NativeOperation::Import => 30,
    }
}
fn read_operation(c: &mut Cursor<'_>) -> Result<NativeOperation, Error> {
    Ok(match c.u8()? {
        0 => NativeOperation::RegisterMonitor,
        1 => NativeOperation::RebindMonitor,
        2 => NativeOperation::CancelMonitor,
        3 => NativeOperation::MonitorDeadline,
        4 => NativeOperation::ReleaseScope,
        5 => NativeOperation::GenerateResultTestament,
        6 => NativeOperation::PostResultTestament,
        7 => NativeOperation::EnterWholeWork,
        8 => NativeOperation::SealIncrementTargets,
        9 => NativeOperation::BeginIncrement,
        10 => NativeOperation::ReportIncrement,
        11 => NativeOperation::FailWorkProduction,
        12 => NativeOperation::RejectWork,
        13 => NativeOperation::SubmitWork,
        14 => NativeOperation::SubmitDiagnostic,
        15 => NativeOperation::ReceiveWork,
        16 => NativeOperation::CloseResponse,
        17 => NativeOperation::PostResponse,
        18 => NativeOperation::ReceiveResponse,
        19 => NativeOperation::AcquireReceipt,
        20 => NativeOperation::AdoptReceipt,
        21 => NativeOperation::Create,
        22 => NativeOperation::Cancel,
        23 => NativeOperation::Post,
        24 => NativeOperation::BeginAdmission,
        25 => NativeOperation::ReportAdmission,
        26 => NativeOperation::BeginWork,
        27 => NativeOperation::ReportWork,
        28 => NativeOperation::EvaluationDeadline,
        29 => NativeOperation::ClaimDeadline,
        30 => NativeOperation::Import,
        _ => return Err(Error::InvalidTag("operation")),
    })
}
pub(super) fn read_ledger(c: &mut Cursor<'_>) -> Result<LedgerId, Error> {
    Ok(LedgerId {
        tenant: TenantId(c.fixed()?),
        session: SessionId(c.fixed()?),
    })
}
pub(super) fn read_evaluation(c: &mut Cursor<'_>) -> Result<EvaluationKey, Error> {
    let claim = ClaimId(c.fixed()?);
    let validation = ValidationId(c.fixed()?);
    let generation = c.u64()?;
    let target = match c.u8()? {
        0 => EvaluationTarget::Admission,
        1 => EvaluationTarget::Increment {
            artifact: ArtifactId(c.fixed()?),
        },
        2 => EvaluationTarget::Work {
            response: TestamentId(c.fixed()?),
            slot: c.u32()?,
            artifact: ArtifactId(c.fixed()?),
        },
        3 => EvaluationTarget::MissingSlot {
            response: TestamentId(c.fixed()?),
            slot: c.u32()?,
        },
        4 => EvaluationTarget::Delivery {
            response: TestamentId(c.fixed()?),
        },
        _ => return Err(Error::InvalidTag("evaluation target")),
    };
    Ok(EvaluationKey {
        claim,
        validation,
        target,
        generation,
    })
}
pub(super) fn read_invocation(c: &mut Cursor<'_>) -> Result<NativeInvocation, Error> {
    Ok(match c.u8()? {
        0 => NativeInvocation::Request(RequestKey {
            principal: ParticipantId(c.fixed()?),
            epoch: RequestEpoch(c.u64()?),
            id: RequestId(c.fixed()?),
        }),
        1 => NativeInvocation::EvaluationDeadline(NativeDeadlineKey {
            evaluation: read_evaluation(c)?,
            timer: TimerId(c.fixed()?),
            generation: c.u64()?,
        }),
        2 => NativeInvocation::ClaimDeadline(NativeClaimDeadlineKey {
            claim: ClaimId(c.fixed()?),
            timer: TimerId(c.fixed()?),
            generation: c.u64()?,
        }),
        3 => NativeInvocation::MonitorDeadline(NativeMonitorDeadlineKey {
            claim: ClaimId(c.fixed()?),
            monitor: MonitorId(c.fixed()?),
            timer: TimerId(c.fixed()?),
            generation: c.u64()?,
        }),
        4 => NativeInvocation::Import,
        _ => return Err(Error::InvalidTag("invocation namespace")),
    })
}
fn read_cycle(c: &mut Cursor<'_>) -> Result<NativeCycleKey, Error> {
    Ok(NativeCycleKey {
        claim: ClaimId(c.fixed()?),
        receipt: ReceiptId(c.fixed()?),
        epoch: c.u64()?,
        cycle: c.u32()?,
    })
}
fn read_result(c: &mut Cursor<'_>) -> Result<NativeResultKey, Error> {
    Ok(NativeResultKey {
        evaluation: read_evaluation(c)?,
        revision: ObjectRevision(c.u64()?),
    })
}
pub(super) fn read_key(c: &mut Cursor<'_>) -> Result<Key, Error> {
    Ok(match c.u8()? {
        0 => Key::IncomingHead(ClaimId(c.fixed()?)),
        1 => Key::IncomingLink(ClaimId(c.fixed()?), ClaimId(c.fixed()?)),
        2 => Key::Monitor(MonitorId(c.fixed()?)),
        3 => Key::MonitorHead(ClaimId(c.fixed()?)),
        4 => Key::MonitorLink(ClaimId(c.fixed()?), MonitorId(c.fixed()?)),
        5 => Key::MissingResult(read_result(c)?),
        6 => Key::Meta,
        7 => Key::Claim(ClaimId(c.fixed()?)),
        8 => Key::Definition(ValidationId(c.fixed()?)),
        9 => Key::Evaluation(read_evaluation(c)?),
        10 => Key::Artifact(ArtifactId(c.fixed()?)),
        11 => Key::ArtifactIdentity(ContentHash(c.fixed()?)),
        12 => Key::Accepted(read_result(c)?),
        13 => Key::DeliveryResult(read_result(c)?),
        14 => Key::Receipt(ReceiptId(c.fixed()?)),
        15 => Key::Cycle(read_cycle(c)?),
        16 => Key::RetiredCycleHead(ClaimId(c.fixed()?)),
        17 => Key::RetiredCycle(read_cycle(c)?),
        18 => Key::Work(ArtifactId(c.fixed()?)),
        19 => Key::WorkSlot(read_cycle(c)?, c.u32()?),
        20 => Key::Diagnostic(ArtifactId(c.fixed()?)),
        21 => Key::Response(TestamentId(c.fixed()?)),
        22 => Key::ResultTestament(TestamentId(c.fixed()?)),
        23 => Key::ClaimResultTestament(ClaimId(c.fixed()?)),
        24 => Key::Outcome(read_invocation(c)?),
        25 => Key::Event(SessionSeq(c.u64()?), c.u32()?),
        26 => Key::ClaimContent(ClaimId(c.fixed()?)),
        27 => Key::ClaimIdentity(c.u16()?, ContentHash(c.fixed()?)),
        28 => Key::DefinitionIdentity(c.u16()?, ContentHash(c.fixed()?)),
        29 => Key::CreationResult(read_invocation(c)?),
        30 => Key::LegacyTestament(TestamentId(c.fixed()?)),
        31 => Key::LegacyEvidenceSet(focal_model::EvidenceSetId(c.fixed()?)),
        32 => Key::LegacyRun(ValidationId(c.fixed()?), c.u32()?),
        33 => Key::LegacyDefinition(ValidationId(c.fixed()?)),
        34 => Key::ByIssuer(ParticipantId(c.fixed()?), ClaimId(c.fixed()?)),
        35 => Key::BySubject(ParticipantId(c.fixed()?), ClaimId(c.fixed()?)),
        36 => Key::ByStatus(c.u16()?, ClaimId(c.fixed()?)),
        37 => Key::ByAction(c.u16()?, ClaimId(c.fixed()?)),
        38 => Key::ByScope(c.u16()?, ContentHash(c.fixed()?), ClaimId(c.fixed()?)),
        39 => Key::ByRelation(c.u16()?, ClaimId(c.fixed()?), ClaimId(c.fixed()?)),
        40 => Key::ByProducer(ParticipantId(c.fixed()?), ArtifactId(c.fixed()?)),
        41 => Key::ByArtifactKind(ContentHash(c.fixed()?), ArtifactId(c.fixed()?)),
        42 => Key::BySchema(ContentHash(c.fixed()?), ArtifactId(c.fixed()?)),
        43 => Key::ArtifactInput(focal_model::ObjectId(c.fixed()?), ArtifactId(c.fixed()?)),
        44 => Key::ByEvaluator(ParticipantId(c.fixed()?), ValidationId(c.fixed()?)),
        45 => Key::ByVerdict(c.u16()?, read_result(c)?),
        46 => Key::ByCreated(
            c.u16()?,
            SessionSeq(c.u64()?),
            focal_model::ObjectId(c.fixed()?),
        ),
        47 => {
            let at = c.u64()?;
            let target = match c.u8()? {
                0 => TimerTarget::Claim(ClaimId(c.fixed()?)),
                1 => TimerTarget::Evaluation(read_evaluation(c)?),
                2 => TimerTarget::Monitor(ClaimId(c.fixed()?), MonitorId(c.fixed()?)),
                _ => return Err(Error::InvalidTag("timer target")),
            };
            Key::DueTimer(at, target)
        }
        _ => return Err(Error::InvalidTag("row family")),
    })
}
pub(super) fn read_outcome(c: &mut Cursor<'_>) -> Result<NativeOutcome, Error> {
    Ok(NativeOutcome {
        ledger: read_ledger(c)?,
        invocation: read_invocation(c)?,
        sequence: SessionSeq(c.u64()?),
        logical_time: c.u64()?,
        operation: read_operation(c)?,
        intent: ContentHash(c.fixed()?),
        created: c.u32()?,
        changed: c.u32()?,
        definitions: c.u32()?,
        evaluations: c.u32()?,
        artifacts: c.u32()?,
        results: c.u32()?,
        receipts: c.u32()?,
        responses: c.u32()?,
        result_testaments: c.u32()?,
        events: c.u32()?,
    })
}
