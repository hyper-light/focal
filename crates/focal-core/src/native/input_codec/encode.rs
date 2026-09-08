use super::bytes::{
    Error, Sink, write_count, write_raw, write_text, write_u8, write_u16, write_u32, write_u64,
};
use super::*;
use focal_model::{Confidence, OutcomeKind};

pub(super) fn tag(command: &NativeCommand) -> u8 {
    match command {
        NativeCommand::Create { .. } => 0,
        NativeCommand::Cancel { .. } => 1,
        NativeCommand::Post { .. } => 2,
        NativeCommand::BeginAdmission { .. } => 3,
        NativeCommand::ReportAdmission { .. } => 4,
        NativeCommand::AcquireReceipt { .. } => 5,
        NativeCommand::SubmitWork { .. } => 6,
        NativeCommand::SubmitDiagnostic { .. } => 7,
        NativeCommand::ReceiveWork { .. } => 8,
        NativeCommand::CloseResponse { .. } => 9,
        NativeCommand::PostResponse { .. } => 10,
        NativeCommand::ReceiveResponse { .. } => 11,
        NativeCommand::FailWorkProduction { .. } => 12,
        NativeCommand::RejectWork { .. } => 13,
        NativeCommand::BeginIncrement { .. } => 14,
        NativeCommand::ReportIncrement { .. } => 15,
        NativeCommand::SealIncrementTargets { .. } => 16,
        NativeCommand::EnterWholeWork { .. } => 17,
        NativeCommand::BeginWork { .. } => 18,
        NativeCommand::ReportWork { .. } => 19,
        NativeCommand::GenerateResultTestament { .. } => 20,
        NativeCommand::PostResultTestament { .. } => 21,
        NativeCommand::AdoptReceipt { .. } => 22,
        NativeCommand::ReleaseScope { .. } => 23,
        NativeCommand::RegisterMonitor { .. } => 24,
        NativeCommand::RebindMonitor { .. } => 25,
        NativeCommand::CancelMonitor { .. } => 26,
        NativeCommand::CreateAuthored { .. } => 27,
    }
}

pub(super) fn frame(sink: &mut impl Sink, source: InputFrame<'_>) -> Result<(), Error> {
    let (ledger, profile, namespace) = match source {
        InputFrame::Request {
            ledger, profile, ..
        } => (ledger, profile, 0),
        InputFrame::EvaluationDeadline {
            ledger, profile, ..
        } => (ledger, profile, 1),
        InputFrame::ClaimDeadline {
            ledger, profile, ..
        } => (ledger, profile, 2),
        InputFrame::MonitorDeadline {
            ledger, profile, ..
        } => (ledger, profile, 3),
    };
    write_raw(sink, &MAGIC)?;
    write_u16(sink, VERSION)?;
    write_u8(
        sink,
        match profile {
            NativeContentProfile::ProjectionOnly => 0,
            NativeContentProfile::AuthoredV1 => 1,
        },
    )?;
    write_u8(sink, namespace)?;
    types::ledger(sink, ledger)?;
    match source {
        InputFrame::Request { input, .. } => {
            if super::super::authored::check_profile(profile, &input.command).is_err() {
                return Err(Error::InvalidTag("creation profile"));
            }
            write_raw(sink, &input.request.principal.0)?;
            write_u64(sink, input.request.epoch.0)?;
            write_raw(sink, &input.request.id.0)?;
            write_u8(sink, tag(&input.command))?;
            command(sink, &input.command)
        }
        InputFrame::EvaluationDeadline { input, .. } => {
            types::evaluation(sink, input.evaluation)?;
            types::deadline(sink, input.deadline)
        }
        InputFrame::ClaimDeadline { input, .. } => {
            write_raw(sink, &input.claim.0)?;
            types::deadline(sink, input.deadline)
        }
        InputFrame::MonitorDeadline { input, .. } => {
            write_raw(sink, &input.claim.0)?;
            write_raw(sink, &input.monitor.0)?;
            types::deadline(sink, input.deadline)
        }
    }
}

fn artifact(sink: &mut impl Sink, value: &NativeArtifactInput) -> Result<(), Error> {
    descriptors::artifact(
        sink,
        value.get().ok_or(Error::InvalidTag("artifact input"))?,
    )
}

fn command(sink: &mut impl Sink, value: &NativeCommand) -> Result<(), Error> {
    match value {
        NativeCommand::Cancel { expected }
        | NativeCommand::Post { expected }
        | NativeCommand::PostResultTestament { expected }
        | NativeCommand::ReleaseScope { expected } => types::binding(sink, *expected),
        NativeCommand::SealIncrementTargets { claim } => types::binding(sink, *claim),
        NativeCommand::BeginAdmission {
            claim,
            key,
            expected,
        }
        | NativeCommand::BeginIncrement {
            claim,
            key,
            expected,
        }
        | NativeCommand::BeginWork {
            claim,
            key,
            expected,
        } => {
            types::binding(sink, *claim)?;
            types::evaluation(sink, *key)?;
            types::binding(sink, *expected)
        }
        NativeCommand::ReportAdmission {
            claim,
            key,
            expected,
            report,
            artifact: evidence,
        }
        | NativeCommand::ReportIncrement {
            claim,
            key,
            expected,
            report,
            artifact: evidence,
        }
        | NativeCommand::ReportWork {
            claim,
            key,
            expected,
            report,
            artifact: evidence,
        } => {
            types::binding(sink, *claim)?;
            types::evaluation(sink, *key)?;
            types::binding(sink, *expected)?;
            types::report(sink, *report)?;
            artifact(sink, evidence)
        }
        NativeCommand::AcquireReceipt { expected, receipt } => {
            types::binding(sink, *expected)?;
            write_raw(sink, &receipt.0)
        }
        NativeCommand::SubmitWork {
            claim,
            slot,
            artifact: evidence,
        } => {
            types::binding(sink, *claim)?;
            write_u32(sink, *slot)?;
            artifact(sink, evidence)
        }
        NativeCommand::SubmitDiagnostic {
            claim,
            reason,
            artifact: evidence,
        } => {
            types::binding(sink, *claim)?;
            types::failure(sink, *reason)?;
            artifact(sink, evidence)
        }
        NativeCommand::ReceiveWork { claim, expected }
        | NativeCommand::PostResponse { claim, expected }
        | NativeCommand::ReceiveResponse { claim, expected }
        | NativeCommand::EnterWholeWork { claim, expected } => {
            types::binding(sink, *claim)?;
            types::binding(sink, *expected)
        }
        NativeCommand::CloseResponse {
            claim,
            response,
            report,
        } => {
            types::binding(sink, *claim)?;
            types::binding(sink, *response)?;
            write_text(sink, &report.summary)?;
            write_u8(
                sink,
                match report.confidence {
                    Confidence::Hint => 0,
                    Confidence::Tentative => 1,
                    Confidence::Committed => 2,
                    Confidence::Consensus => 3,
                },
            )?;
            write_u8(
                sink,
                match report.outcome {
                    OutcomeKind::Complete => 0,
                    OutcomeKind::Partial => 1,
                    OutcomeKind::Refused => 2,
                    OutcomeKind::Impossible => 3,
                    OutcomeKind::Interrupted => 4,
                    OutcomeKind::Failed => 5,
                },
            )?;
            write_count(sink, report.manifest.len())?;
            for slot in &report.manifest {
                write_u32(sink, slot.slot)?;
                types::artifact_ref(sink, slot.artifact)?;
            }
            write_count(sink, report.diagnostics.len())?;
            for diagnostic in &report.diagnostics {
                types::artifact_ref(sink, *diagnostic)?;
            }
            Ok(())
        }
        NativeCommand::FailWorkProduction {
            claim,
            slot,
            diagnostic,
        } => {
            types::binding(sink, *claim)?;
            write_u32(sink, *slot)?;
            types::artifact_ref(sink, *diagnostic)
        }
        NativeCommand::RejectWork {
            claim,
            expected,
            reason,
            artifact: evidence,
        } => {
            types::binding(sink, *claim)?;
            types::binding(sink, *expected)?;
            types::failure(sink, *reason)?;
            artifact(sink, evidence)
        }
        NativeCommand::GenerateResultTestament { claim, id } => {
            types::binding(sink, *claim)?;
            write_raw(sink, &id.0)
        }
        NativeCommand::AdoptReceipt {
            expected,
            previous,
            receipt,
            holder,
        } => {
            types::binding(sink, *expected)?;
            types::receipt(sink, *previous)?;
            write_raw(sink, &receipt.0)?;
            write_raw(sink, &holder.0)
        }
        NativeCommand::RegisterMonitor {
            expected,
            receipt,
            id,
            roots,
            deadline,
        } => {
            types::binding(sink, *expected)?;
            types::optional_receipt(sink, *receipt)?;
            write_raw(sink, &id.0)?;
            write_count(sink, roots.len())?;
            for root in roots {
                let (tag, id) = match root {
                    WaitPredicate::Satisfied(id) => (0, id),
                    WaitPredicate::Terminal(id) => (1, id),
                    WaitPredicate::Released(id) => (2, id),
                };
                write_u8(sink, tag)?;
                write_raw(sink, &id.0)?;
            }
            types::deadline(sink, *deadline)
        }
        NativeCommand::RebindMonitor {
            expected,
            receipt,
            id,
            predecessor,
            successor,
        } => {
            types::binding(sink, *expected)?;
            types::optional_receipt(sink, *receipt)?;
            write_raw(sink, &id.0)?;
            types::binding(sink, *predecessor)?;
            types::binding(sink, *successor)
        }
        NativeCommand::CancelMonitor {
            expected,
            receipt,
            id,
        } => {
            types::binding(sink, *expected)?;
            types::optional_receipt(sink, *receipt)?;
            write_raw(sink, &id.0)
        }
        NativeCommand::Create {
            claims,
            declarations,
        } => {
            write_count(sink, claims.len())?;
            for claim in claims {
                // Acceptance summaries are derived on decode from the complete
                // actual global cohort below. Refuse a different source policy
                // instead of silently normalizing it while encoding.
                let n = claim.definition.acceptance.declarations().len();
                let m = declarations.len();
                let visits = m
                    .checked_mul(n)
                    .and_then(|v| v.checked_mul(2))
                    .and_then(|v| v.checked_add(m))
                    .and_then(|v| v.checked_add(n))
                    .ok_or(Error::Capacity)?;
                sink.visit(visits)?;
                claim
                    .definition
                    .acceptance
                    .check_declarations(declarations.iter().filter(|declaration| {
                        declaration.claim().0 == claim.definition.binding.object.0
                    }))
                    .map_err(|_| Error::InvalidTag("acceptance correspondence"))?;
                descriptors::projection(sink, claim)?;
            }
            write_count(sink, declarations.len())?;
            for declaration in declarations {
                descriptors::declaration(sink, declaration)?;
            }
            Ok(())
        }
        NativeCommand::CreateAuthored { claims } => {
            write_count(sink, claims.len())?;
            for claim in claims {
                descriptors::claim(sink, &claim.content)?;
                write_count(sink, claim.declarations.len())?;
                for descriptor in &claim.declarations {
                    descriptors::validation(sink, descriptor)?;
                }
                write_u32(sink, claim.max_responses)?;
                types::scope_limits(sink, claim.scope_limits)?;
                types::owner(sink, claim.owner)?;
            }
            Ok(())
        }
    }
}
