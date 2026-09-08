//! Complete retained values; a present empty link is not a deleted key.
use super::*;
use bytes::{Error, Sink, write_raw as raw, write_u8, write_u64};

pub(super) fn family(key: Key, row: &Row) -> Result<(), Error> {
    mutation::check_family(key, row).map_err(|_| Error::InvalidTag("row/key family"))
}
fn scalar_count(s: &mut impl Sink, value: usize) -> Result<(), Error> {
    write_u64(s, u64::try_from(value).map_err(|_| Error::Capacity)?)
}
fn optional_id(s: &mut impl Sink, id: Option<[u8; 16]>) -> Result<(), Error> {
    match id {
        None => write_u8(s, 0),
        Some(id) => {
            write_u8(s, 1)?;
            raw(s, &id)
        }
    }
}
fn optional_cycle(s: &mut impl Sink, value: Option<NativeCycleKey>) -> Result<(), Error> {
    match value {
        None => write_u8(s, 0),
        Some(value) => {
            write_u8(s, 1)?;
            fixed::cycle(s, value)
        }
    }
}
pub(super) fn value(s: &mut impl Sink, row: &Row, ledger: LedgerId) -> Result<(), Error> {
    // Row body schemas version independently from both input and V1 envelopes.
    // No body uses serde enum order or process-local allocation capacities.
    match row {
        Row::IncomingHead(v) => {
            optional_id(s, v.head.map(|id| id.0))?;
            scalar_count(s, v.count)
        }
        Row::IncomingLink(v) => optional_id(s, v.next.map(|id| id.0)),
        Row::Monitor(v) => {
            types::binding(s, v.owner)?;
            write_u64(s, v.registered.0)?;
            types::deadline(s, v.deadline)
        }
        Row::MonitorHead(v) => {
            optional_id(s, v.head.map(|id| id.0))?;
            scalar_count(s, v.count)
        }
        Row::MonitorLink(v) => match v {
            None => write_u8(s, 0),
            Some(v) => {
                write_u8(s, 1)?;
                raw(s, &v.owner.0)?;
                write_u64(s, v.registered.0)?;
                write_u64(s, v.stamp.0)?;
                optional_id(s, v.previous.map(|id| id.0))?;
                optional_id(s, v.next.map(|id| id.0))
            }
        },
        Row::MissingResult(v) => evidence::missing(s, v),
        Row::Meta(v) => {
            // Cumulative ledger counters are u64, never host-size integers or
            // u32 collection lengths. The decoder must checked-convert them.
            s.visit(14)?;
            for value in [
                v.claims,
                v.outcomes,
                v.events,
                v.definitions,
                v.evaluations,
                v.artifacts,
                v.results,
                v.receipts,
                v.responses,
                v.result_testaments,
                v.monitors,
                v.monitor_links,
                v.creation_results,
            ] {
                write_u64(s, u64::try_from(value).map_err(|_| Error::Capacity)?)?;
            }
            write_u64(s, v.logical_time)
        }
        Row::Claim(v) => lifecycle::claim(s, v),
        Row::Definition(v) => evidence::definition(s, v),
        Row::Evaluation(v) => lifecycle::evaluation(s, v),
        Row::Artifact(v) => evidence::artifact(s, v),
        Row::ArtifactIdentity(id) | Row::WorkSlot(id) => raw(s, &id.0),
        Row::Accepted(v) => evidence::accepted(s, v),
        Row::DeliveryResult(v) => evidence::delivery(s, v),
        Row::Receipt(v) => {
            raw(s, &v.claim.0)?;
            types::receipt(s, v.fence)?;
            raw(s, &v.holder.0)?;
            write_u64(s, v.acquired.0)
        }
        Row::Cycle(v) => {
            optional_id(s, v.work_head.map(|id| id.0))?;
            scalar_count(s, v.work_count)?;
            optional_id(s, v.diagnostic_head.map(|id| id.0))?;
            scalar_count(s, v.diagnostic_count)?;
            optional_id(s, v.response.map(|id| id.0))
        }
        Row::RetiredCycleHead(v) => {
            optional_cycle(s, v.head)?;
            scalar_count(s, v.count)?;
            scalar_count(s, v.work_count)
        }
        Row::RetiredCycle(v) => {
            raw(s, &v.holder.0)?;
            optional_cycle(s, v.next)
        }
        Row::Work(v) => evidence::work(s, v),
        Row::Diagnostic(v) => evidence::diagnostic(s, v),
        Row::Response(v) => evidence::response(s, v),
        Row::ResultTestament(v) => lifecycle::result_testament(s, v),
        Row::ClaimResultTestament(id) => raw(s, &id.0),
        Row::Outcome(v) => fixed::outcome(s, *v),
        Row::Event(v) => events::event(
            s,
            v.get()
                .ok_or(Error::InvalidTag("event row"))?
                .expand(ledger),
        ),
        Row::ClaimContent(v) => evidence::claim_content(s, v),
        Row::ClaimIdentity(id) => raw(s, &id.0),
        Row::DefinitionIdentity(id) => raw(s, &id.0),
        Row::CreationResult(v) => evidence::creation(s, v),
    }
}
