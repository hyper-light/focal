//! Complete borrowed evidence/definition row bodies for native record version 1.
//! Process-local semantic stamps and custody capabilities are never serialized.
//! Artifact custody records contain only the durable tree address and observed
//! local revision: recovery must independently read/verify that tree and schemas
//! before reconstructing a local capability. These bytes confer no authority.
use super::{bytes, descriptors, lifecycle_fields as fields, types};
use crate::native::{
    creation_result::{NativeCreatedFamily, OwnedCreationResult},
    delivery_owned::OwnedDeliveryResult,
    missing_owned::OwnedMissingResult,
    owned::{OwnedClaimContent, OwnedDeclaration},
    response_owned::OwnedResponse,
    result_owned::{OwnedAccepted, OwnedArtifact},
    work_owned::{OwnedDiagnostic, OwnedWork},
};
use bytes::{
    Error, Sink, write_count as count, write_raw as raw, write_text as text, write_u8, write_u16,
    write_u32, write_u64,
};
use focal_model::lifecycle::{
    aggregation::PublicationPosition,
    artifact_descriptor::ContentPointer,
    evidence::{
        Diagnostic, FailedWorkSnapshotV1, ResponseDiagnosticSnapshotV1, ResponseSnapshotSource,
        ResponseTerminalSnapshotV1, WorkTerminalSnapshotV1,
    },
};
use focal_model::{ArtifactId, Confidence, ContentClass, OutcomeKind};

#[cfg(test)]
#[path = "evidence_tests.rs"]
pub(super) mod tests;

fn invalid(_: focal_model::lifecycle::ContractError) -> Error {
    Error::InvalidTag("retained evidence row")
}
fn next(sink: &mut impl Sink, value: Option<ArtifactId>) -> Result<(), Error> {
    fields::optional(sink, value, |sink, value| raw(sink, &value.0))
}
fn diagnostic_ref(sink: &mut impl Sink, value: Diagnostic) -> Result<(), Error> {
    types::failure(sink, value.reason)?;
    types::artifact_ref(sink, value.artifact)
}
fn content_pointer(sink: &mut impl Sink, value: ContentPointer) -> Result<(), Error> {
    raw(sink, &value.domain.0)?;
    raw(sink, &value.root.0)?;
    write_u64(sink, value.length)?;
    write_u16(
        sink,
        match value.class {
            ContentClass::Document => 1,
            ContentClass::Evidence => 2,
            ContentClass::Checkpoint => 3,
        },
    )
}

pub(super) fn artifact(sink: &mut impl Sink, owned: &OwnedArtifact) -> Result<(), Error> {
    sink.visit(1)?;
    let value = owned.get().ok_or(Error::InvalidTag("artifact row"))?;
    descriptors::artifact(sink, value.descriptor())?;
    // A content address and revision are recovery inputs, never transferable
    // proof. In particular, NativeLocalCustody's request-bound token is absent.
    content_pointer(sink, value.custody().payload())?;
    write_u64(sink, value.custody().local_revision())
}

pub(super) fn work(sink: &mut impl Sink, owned: &OwnedWork) -> Result<(), Error> {
    // One checked singleton lookup and at most 24 scalar snapshot fields,
    // including conversion of the largest retained terminal cause.
    sink.visit(25)?;
    let value = owned.get().ok_or(Error::InvalidTag("work row"))?;
    let row = value.state.snapshot_v1().map_err(invalid)?;
    types::binding(sink, row.binding)?;
    raw(sink, &row.claim.0)?;
    write_u32(sink, row.slot)?;
    write_u32(sink, row.cycle)?;
    raw(sink, &row.producer.0)?;
    types::receipt(sink, row.receipt)?;
    fields::work_state(sink, row.state)?;
    fields::optional_binding(sink, row.attachment)?;
    fields::optional(sink, row.diagnostic, diagnostic_ref)?;
    fields::optional(sink, row.terminal, work_terminal)?;
    next(sink, value.next)
}
fn work_terminal(sink: &mut impl Sink, value: WorkTerminalSnapshotV1) -> Result<(), Error> {
    match value {
        WorkTerminalSnapshotV1::Passed { sequence } => {
            write_u8(sink, 0)?;
            write_u64(sink, sequence.0)
        }
        WorkTerminalSnapshotV1::Blocked { sequence, cause } => {
            write_u8(sink, 1)?;
            write_u64(sink, sequence.0)?;
            fields::blocking_cause(sink, cause)
        }
    }
}
fn failed_work(sink: &mut impl Sink, value: FailedWorkSnapshotV1) -> Result<(), Error> {
    types::binding(sink, value.binding)?;
    write_u32(sink, value.slot)?;
    fields::work_state(sink, value.state)?;
    diagnostic_ref(sink, value.diagnostic)
}
fn response_diagnostic(
    sink: &mut impl Sink,
    value: ResponseDiagnosticSnapshotV1,
) -> Result<(), Error> {
    types::ledger(sink, value.ledger)?;
    raw(sink, &value.claim.0)?;
    types::receipt(sink, value.receipt)?;
    write_u32(sink, value.cycle)?;
    raw(sink, &value.producer.0)?;
    diagnostic_ref(sink, value.diagnostic)
}
pub(super) fn diagnostic(sink: &mut impl Sink, owned: &OwnedDiagnostic) -> Result<(), Error> {
    sink.visit(8)?;
    let value = owned.get().ok_or(Error::InvalidTag("diagnostic row"))?;
    response_diagnostic(sink, value.diagnostic.snapshot_v1())?;
    next(sink, value.next)
}

pub(super) fn response(sink: &mut impl Sink, owned: &OwnedResponse) -> Result<(), Error> {
    sink.visit(1)?;
    let value = owned.record().ok_or(Error::InvalidTag("response row"))?;
    let visits = value.response().snapshot_visits().map_err(invalid)?;
    // The model's shared allowance covers the original report-stamp check,
    // including every summary byte and retained failed-work/diagnostic record.
    sink.visit(visits)?;
    let snapshot = value
        .response()
        .snapshot_v1(value.generated(), visits)
        .map_err(invalid)?;
    let row = snapshot.fields().map_err(invalid)?;
    types::binding(sink, row.identity.binding)?;
    // Immutable identity is shared with the current binding. This revision was
    // captured from the actual Generated row, never inferred from a state tag.
    write_u64(sink, row.generated.revision.0)?;
    raw(sink, &row.identity.claim.0)?;
    types::receipt(sink, row.identity.receipt)?;
    write_u32(sink, row.identity.cycle)?;
    fields::optional(sink, row.identity.prior, |sink, id| raw(sink, &id.0))?;
    raw(sink, &row.respondent.0)?;
    fields::response_state(sink, row.state)?;
    text(sink, row.summary)?;
    write_u8(
        sink,
        match row.confidence {
            Confidence::Hint => 0,
            Confidence::Tentative => 1,
            Confidence::Committed => 2,
            Confidence::Consensus => 3,
        },
    )?;
    write_u8(
        sink,
        match row.outcome {
            OutcomeKind::Complete => 0,
            OutcomeKind::Partial => 1,
            OutcomeKind::Refused => 2,
            OutcomeKind::Impossible => 3,
            OutcomeKind::Interrupted => 4,
            OutcomeKind::Failed => 5,
        },
    )?;
    count(sink, row.manifest_count)?;
    for index in 0..row.manifest_count {
        sink.visit(1)?;
        let entry = snapshot.manifest(index).map_err(invalid)?;
        write_u32(sink, entry.slot)?;
        types::artifact_ref(sink, entry.artifact)?;
    }
    count(sink, row.failed_work_count)?;
    for index in 0..row.failed_work_count {
        sink.visit(1)?;
        failed_work(sink, snapshot.failed_work(index).map_err(invalid)?)?;
    }
    count(sink, row.diagnostic_count)?;
    for index in 0..row.diagnostic_count {
        sink.visit(1)?;
        response_diagnostic(sink, snapshot.diagnostic(index).map_err(invalid)?)?;
    }
    fields::optional(sink, row.terminal, response_terminal)?;
    fields::optional_position(sink, value.received())?;
    fields::optional_position(sink, value.entered())
}
fn response_terminal(sink: &mut impl Sink, value: ResponseTerminalSnapshotV1) -> Result<(), Error> {
    match value {
        ResponseTerminalSnapshotV1::Validated { sequence } => {
            write_u8(sink, 0)?;
            write_u64(sink, sequence.0)
        }
        ResponseTerminalSnapshotV1::Blocked(cut) => {
            write_u8(sink, 1)?;
            fields::terminal(sink, cut)
        }
    }
}

pub(super) fn accepted(sink: &mut impl Sink, owned: &OwnedAccepted) -> Result<(), Error> {
    sink.visit(1)?;
    let value = owned.get().ok_or(Error::InvalidTag("accepted row"))?;
    fields::accepted_result(sink, value.result())?;
    types::attempt(sink, value.attempt())?;
    types::artifact_ref(sink, value.artifact().reference())?;
    raw(sink, &value.artifact().producer().0)?;
    fields::position(
        sink,
        PublicationPosition {
            sequence: value.sequence(),
            ordinal: value.ordinal(),
        },
    )
}
pub(super) fn delivery(sink: &mut impl Sink, owned: &OwnedDeliveryResult) -> Result<(), Error> {
    sink.visit(1)?;
    let value = owned
        .get()
        .ok_or(Error::InvalidTag("delivery result row"))?;
    fields::accepted_result(sink, value.result())?;
    fields::position(
        sink,
        PublicationPosition {
            sequence: value.sequence(),
            ordinal: value.ordinal(),
        },
    )
}
pub(super) fn missing(sink: &mut impl Sink, owned: &OwnedMissingResult) -> Result<(), Error> {
    sink.visit(1)?;
    let value = owned.get().ok_or(Error::InvalidTag("missing result row"))?;
    fields::accepted_result(sink, value.result())?;
    fields::position(
        sink,
        PublicationPosition {
            sequence: value.sequence(),
            ordinal: value.ordinal(),
        },
    )
}
pub(super) fn definition(sink: &mut impl Sink, owned: &OwnedDeclaration) -> Result<(), Error> {
    sink.visit(2)?;
    let value = owned.get().ok_or(Error::InvalidTag("definition row"))?;
    match owned.descriptor() {
        None => {
            write_u8(sink, 0)?;
            descriptors::declaration(sink, value)
        }
        Some(descriptor) => {
            write_u8(sink, 1)?;
            descriptors::validation(sink, descriptor)
        }
    }
}
pub(super) fn claim_content(sink: &mut impl Sink, owned: &OwnedClaimContent) -> Result<(), Error> {
    sink.visit(2)?;
    let value = owned.get().ok_or(Error::InvalidTag("claim content row"))?;
    let profile = owned
        .profile()
        .ok_or(Error::InvalidTag("claim content profile"))?;
    descriptors::claim(sink, value)?;
    write_u32(sink, profile.max_responses)?;
    types::scope_limits(sink, profile.scope_limits)?;
    types::owner(sink, profile.owner)
}
pub(super) fn creation(sink: &mut impl Sink, owned: &OwnedCreationResult) -> Result<(), Error> {
    sink.visit(1)?;
    let entries = owned.get().entries();
    sink.visit(entries.len().checked_add(1).ok_or(Error::Capacity)?)?;
    count(sink, entries.len())?;
    for entry in entries {
        write_u32(sink, entry.ordinal)?;
        write_u8(
            sink,
            match entry.family {
                NativeCreatedFamily::Claim => 0,
                NativeCreatedFamily::Validation => 1,
            },
        )?;
        write_u16(sink, entry.schema)?;
        raw(sink, &entry.content.0)?;
        raw(sink, &entry.requested.0)?;
        raw(sink, &entry.resolved.0)?;
    }
    Ok(())
}
