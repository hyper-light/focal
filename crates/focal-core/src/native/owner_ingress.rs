//! Admission of complete borrowed input plans. The owner authenticates identity
//! and the effective prefix, selects an actual held entitlement when eligible,
//! and retains its input allocation through custody and candidate construction.
use super::*;
use crate::native::artifact_intent::{ArtifactCommand, ReportKind};
use crate::native::input_codec::{
    DecodeError, DecodedPlan, DecodedRequest, FrameKind, InputHeader, NativeDecodeLimits,
    StructuralInput,
};
use focal_memory::Reservation;

#[cfg(test)]
#[path = "owner_ingress_tests.rs"]
mod tests;

/// The command of a request frame, from its header.
fn command(bytes: &[u8]) -> Option<u8> {
    match StructuralInput::peek_header(bytes).ok()?.kind {
        FrameKind::Request { command } => Some(command),
        _ => None,
    }
}

fn check_frame_header(
    context: NativeContext,
    ledger: LedgerId,
    profile: NativeContentProfile,
    header: InputHeader,
) -> Result<(), DecodeError> {
    if header.ledger != ledger {
        return Err(ContractError::WrongLedger.into());
    }
    if header.profile != profile {
        return Err(ContractError::InvalidPolicy.into());
    }
    let request = header.request.ok_or(ContractError::WrongActor)?;
    context.principal.require_actor(request.principal)?;
    if request.principal.is_zero() || request.id.is_zero() || request.epoch.0 == 0 {
        return Err(ContractError::InvalidTarget.into());
    }
    Ok(())
}

/// After this many refusals of a [`Shortfall`]'s requests from their headers,
/// one is decoded and prepared after all, to be refused or admitted on its
/// merits: a zero window's probe (RFC 9293 §3.8.6.1). A request that needs
/// less than the one the memory refused, its frame as large, is so never
/// refused for good while the headroom stays as it is. Each probe the memory
/// refuses again doubles the interval, as a zero window's probes back off, up
/// to [`SHORTFALL_PROBE_MAX`]: measured at the bound, a probe's preparation
/// was three quarters of what refusing at one in 32 cost.
const SHORTFALL_PROBE: u32 = 32;
const SHORTFALL_PROBE_MAX: u32 = 1024;

/// The commands whose shortfalls an owner keeps at once: a refusal of one
/// command never undoes another's, as a single shortfall did where creations
/// and posts were both refused. A command takes its own slot, an empty one,
/// or the one its tag picks.
pub(super) const SHORTFALLS: usize = 8;

/// A fresh request the owner's budget refused once its frame was decoded and
/// its candidate built: its command, its frame's size, the lane its
/// reservations took, and the least it needed, the headroom it found and the
/// part of a reservation that headroom lacked. It retained nothing, so while
/// the lane's headroom stays below that, another fresh request of its
/// command, its frame at least as large, needs more than there is and is
/// refused from its header, as a zero window refuses data. At the memory's
/// bound each refusal had cost a preparation, 19 µs of a 23 µs claim creation
/// in the isolated harness: a creation's construction and publication take
/// 2.3 MB of headroom for the 7 KB its claim retains, and only its last
/// reservation was refused.
#[derive(Debug, Clone, Copy)]
pub(super) struct Shortfall {
    command: u8,
    frame_bytes: usize,
    lane: BudgetLane,
    need: usize,
    /// Refused from their headers since the last probe, and how many make
    /// the next.
    refused: u32,
    probe: u32,
}

/// What the owner's budget showed before a frame was prepared: the refusals
/// it had met and each lane's headroom.
#[derive(Clone, Copy)]
struct Before {
    refusals: u64,
    ordinary: usize,
    completion: usize,
}

impl NativeOwner {
    /// Refuses, from its header alone, a request its frame's decoding would
    /// end in refusing: a fresh request (its key has no outcome, committed or
    /// pending) while the owner's memory funds no more pending candidates, or
    /// while it stays short of what a like request was refused for
    /// ([`Shortfall`]). The owner refused it only after planning its frame
    /// (every validation body in it inspected), and the memory only after
    /// building its candidate, so at either bound each refusal cost that work.
    /// A header that does not read, that names another ledger, profile or
    /// actor, a faulted owner and a retry of a request with an outcome are
    /// decoded as before, which says why they are refused or answers them.
    fn refuse_when_full(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
    ) -> Result<(), NativeOwnerError> {
        if self.faulted {
            return Ok(());
        }
        let queue_full = self.reserve_pending().is_err();
        if !queue_full && self.shortfalls.iter().all(Option::is_none) {
            return Ok(());
        }
        let Ok(header) = StructuralInput::peek_header(bytes) else {
            return Ok(());
        };
        let state = &self.core.state;
        if check_frame_header(context, state.ledger, state.profile, header).is_err() {
            return Ok(());
        }
        let Some(request) = header.request else {
            return Ok(());
        };
        let view = View::over(
            state,
            Some(super::pending_index::Chain::indexed(
                &self.index,
                &self.pending,
                u64::MAX,
            )),
        );
        if view.has_outcome(request.into()) {
            return Ok(());
        }
        if queue_full {
            return Err(NativeError::Capacity("pending candidates").into());
        }
        match self.short_of_memory(header.kind, bytes.len()) {
            Some(error) => Err(error.into()),
            None => Ok(()),
        }
    }

    /// The refusal a fresh request of `kind`, its frame `frame_bytes` long,
    /// meets while the owner's memory stays short of the last request of its
    /// command it refused, if any. Memory come back since ends that
    /// shortfall.
    fn short_of_memory(&mut self, kind: FrameKind, frame_bytes: usize) -> Option<MemoryError> {
        let FrameKind::Request { command } = kind else {
            return None;
        };
        let slot = self
            .shortfalls
            .iter_mut()
            .find(|slot| slot.is_some_and(|shortfall| shortfall.command == command))?;
        let shortfall = (*slot)?;
        let headroom = self.core.state.budget.headroom(shortfall.lane);
        if headroom >= shortfall.need {
            *slot = None;
            return None;
        }
        if frame_bytes < shortfall.frame_bytes {
            return None;
        }
        let refused = shortfall.refused.saturating_add(1);
        if refused >= shortfall.probe {
            *slot = Some(Shortfall {
                refused: 0,
                ..shortfall
            });
            return None;
        }
        *slot = Some(Shortfall {
            refused,
            ..shortfall
        });
        Some(MemoryError::Capacity {
            requested: shortfall.need,
            available: headroom,
        })
    }

    /// The slot for `command`'s shortfall: its own, an empty one, or the one
    /// its tag picks.
    fn shortfall_slot(&mut self, command: u8) -> Option<&mut Option<Shortfall>> {
        let at = self
            .shortfalls
            .iter()
            .position(|slot| slot.is_some_and(|shortfall| shortfall.command == command))
            .or_else(|| self.shortfalls.iter().position(Option::is_none))
            .unwrap_or_else(|| usize::from(command).checked_rem(SHORTFALLS).unwrap_or(0));
        self.shortfalls.get_mut(at)
    }

    /// Prepares a frame by `prepare` where [`Self::refuse_when_full`] does not
    /// refuse it. A fresh request the owner's budget then refuses leaves its
    /// [`Shortfall`]; one of its command, its frame as large, prepared ends it.
    fn admit_frame(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        prepare: impl FnOnce(&mut Self) -> Result<NativeStaging, NativeOwnerError>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.refuse_when_full(context, bytes)?;
        let budget = &self.core.state.budget;
        let before = Before {
            refusals: budget.refusals().count,
            ordinary: budget.headroom(BudgetLane::Ordinary),
            completion: budget.headroom(BudgetLane::Completion),
        };
        let result = prepare(self);
        match &result {
            Ok(NativeStaging::Prepared { .. }) => {
                if let Some(command) = command(bytes)
                    && let Some(slot) = self.shortfalls.iter_mut().find(|slot| {
                        slot.is_some_and(|shortfall| {
                            shortfall.command == command && bytes.len() >= shortfall.frame_bytes
                        })
                    })
                {
                    *slot = None;
                }
            }
            Ok(NativeStaging::Existing { .. }) => {}
            Err(error) => self.note_shortfall(bytes, before, error),
        }
        result
    }

    /// Leaves the [`Shortfall`] of a fresh request whose preparation the
    /// owner's own budget refused: the refusal its error reports is the one
    /// the budget has counted since `before`, not another bound that reports
    /// capacity, and the headroom it found is its lane's.
    fn note_shortfall(&mut self, bytes: &[u8], before: Before, error: &NativeOwnerError) {
        let NativeOwnerError::Native(NativeError::Memory(MemoryError::Capacity {
            requested,
            available,
        })) = error
        else {
            return;
        };
        let refused = self.core.state.budget.refusals();
        if refused.count == before.refusals
            || refused.requested != *requested
            || refused.available != *available
        {
            return;
        }
        let Some(command) = command(bytes) else {
            return;
        };
        // A fresh request is admitted in the ordinary or completion lane, never the committed.
        let headroom = match refused.lane {
            BudgetLane::Ordinary => before.ordinary,
            BudgetLane::Completion | BudgetLane::Committed => before.completion,
        };
        let Some(slot) = self.shortfall_slot(command) else {
            return;
        };
        // Refused again while short: the next probe waits twice as long.
        let probe = match *slot {
            Some(shortfall) if shortfall.command == command => {
                shortfall.probe.saturating_mul(2).min(SHORTFALL_PROBE_MAX)
            }
            _ => SHORTFALL_PROBE,
        };
        *slot = Some(Shortfall {
            command,
            frame_bytes: bytes.len(),
            lane: refused.lane,
            need: headroom.saturating_add(requested.saturating_sub(*available)),
            refused: 0,
            probe,
        });
    }

    /// Decode a complete borrowed actor frame with cumulative input work limits.
    /// The caller retains its charged receive buffer until this method returns;
    /// this synchronous API neither receives bytes nor acknowledges durability.
    pub fn prepare_frame(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        evidence: Option<&VerifiedNativeArtifact>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.admit_frame(context, bytes, |owner| {
            let ledger = owner.core.state.ledger;
            let profile = owner.core.state.profile;
            limits.with_header_check(
                owner.core.limits,
                bytes,
                |header| check_frame_header(context, ledger, profile, header),
                |input, quote| {
                    owner.prepare_decoded(context, input, quote.construction.total()?, evidence)
                },
            )?
        })
    }

    #[allow(clippy::too_many_arguments)] // Same custody boundary as decoded ingress.
    pub fn prepare_frame_with_custody(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        store: &mut ContentStore,
        inline_domain: ContentDomainId,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.admit_frame(context, bytes, |owner| {
            let ledger = owner.core.state.ledger;
            let profile = owner.core.state.profile;
            limits.with_header_check(
                owner.core.limits,
                bytes,
                |header| check_frame_header(context, ledger, profile, header),
                |input, quote| {
                    owner.prepare_decoded_with_custody(
                        context,
                        input,
                        quote.construction.total()?,
                        store,
                        inline_domain,
                        schemas,
                    )
                },
            )?
        })
    }

    pub fn prepare_frame_evidenced_with_schemas(
        &mut self,
        context: NativeContext,
        bytes: &[u8],
        limits: NativeDecodeLimits,
        evidence: Option<&VerifiedNativeArtifact>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.admit_frame(context, bytes, |owner| {
            let ledger = owner.core.state.ledger;
            let profile = owner.core.state.profile;
            limits.with_header_check(
                owner.core.limits,
                bytes,
                |header| check_frame_header(context, ledger, profile, header),
                |input, quote| {
                    owner.prepare_decoded_evidenced_with_schemas(
                        context,
                        input,
                        quote.construction.total()?,
                        evidence,
                        schemas,
                    )
                },
            )?
        })
    }

    /// Stage a complete decoded actor command. Variable fields stay borrowed
    /// until this owner reserves construction capacity. Exact retries require
    /// neither input allocation nor a remaining construction-work allowance.
    /// Parsing and source/model preparation use the codec's separate limits.
    pub fn prepare_decoded(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(
            context,
            input,
            max_build_visits,
            evidence,
            None,
            &BuiltinNativeSchemas,
        )
    }

    /// Verify external evidence with the same owner-selected capacity used for
    /// input and candidate construction. This performs no participant execution.
    #[allow(clippy::too_many_arguments)] // Mirrors typed custody ingress plus construction allowance.
    pub fn prepare_decoded_with_custody(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        store: &mut ContentStore,
        inline_domain: ContentDomainId,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(
            context,
            input,
            max_build_visits,
            None,
            Some((store, inline_domain)),
            schemas,
        )
    }

    pub fn prepare_decoded_evidenced_with_schemas(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        self.prepare_decoded_using(context, input, max_build_visits, evidence, None, schemas)
    }

    fn prepare_decoded_using(
        &mut self,
        context: NativeContext,
        input: DecodedRequest<'_, '_>,
        max_build_visits: usize,
        evidence: Option<&VerifiedNativeArtifact>,
        custody: Option<(&mut ContentStore, ContentDomainId)>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<NativeStaging, NativeOwnerError> {
        let header = input.header();
        if header.ledger != self.core.state.ledger {
            return Err(NativeError::Contract(ContractError::WrongLedger).into());
        }
        if header.profile != self.core.state.profile {
            return Err(NativeError::Contract(ContractError::InvalidPolicy).into());
        }
        let request = header
            .request
            .ok_or(ContractError::WrongActor)
            .map_err(NativeError::from)?;
        // Room for the candidate, made before its chain is borrowed (`prepare_using`).
        let room = self.reserve_pending().is_ok();
        let checked = self.core.check_request_identity_chain(
            context,
            request,
            input.intent_fingerprint(),
            Some(super::pending_index::Chain::indexed(
                &self.index,
                &self.pending,
                u64::MAX,
            )),
        )?;
        let view = match checked {
            prepare::RequestCheck::Existing { outcome, committed } => {
                let candidate = if committed {
                    None
                } else {
                    Some(
                        self.pending
                            .iter()
                            .find(|row| row.prepared.outcome() == outcome)
                            .ok_or(NativeOwnerError::UnknownCandidate)?
                            .candidate,
                    )
                };
                return Ok(NativeStaging::Existing { outcome, candidate });
            }
            prepare::RequestCheck::Fresh { view, .. } => view,
        };
        if self.faulted {
            return Err(NativeError::Capacity("completion owner requires reconstruction").into());
        }
        if let Err(error) = self.book.check_health() {
            self.faulted = true;
            return Err(error.into());
        }
        self.next_serial
            .checked_add(1)
            .ok_or(MemoryError::CounterExhausted("native candidate serial"))?;
        if !room {
            return Err(NativeError::Capacity("pending candidates").into());
        }
        input.check_build(self.core.limits.preparation_bytes, max_build_visits)?;
        let reservation = self.reserve_decoded(context, &input, &view, schemas)?;
        // Authorization may replay a borrowed source. It cannot spend the
        // remaining construction passes and then allocate an incomplete input.
        input.check_build(self.core.limits.preparation_bytes, max_build_visits)?;
        let owned = input.build()?;
        // This guard outlives the consumed input and every custody/build path.
        // Each retained candidate page carries its own separately priced debit.
        let _input_allocation = reservation.commit();
        self.prepare_using(
            OwnerInput::Request(context, owned),
            evidence,
            custody,
            schemas,
        )
    }

    fn reserve_decoded(
        &self,
        context: NativeContext,
        input: &DecodedRequest<'_, '_>,
        view: &View<'_>,
        schemas: &impl NativeSchemaVerifier,
    ) -> Result<Reservation, NativeError> {
        let bytes = input.construction_bytes();
        match &input.plan {
            DecodedPlan::Artifact(plan) => match plan.command() {
                ArtifactCommand::Report {
                    kind,
                    claim,
                    key,
                    expected,
                    report,
                } => {
                    let descriptor = plan.descriptor();
                    let (registered, authorization) = match kind {
                        ReportKind::Admission => super::super::admission_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                        ReportKind::Increment => super::super::increment_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                        ReportKind::Work => super::super::work_authority::report_view(
                            view,
                            context,
                            claim,
                            key,
                            expected,
                            report,
                            descriptor,
                            self.core.limits,
                        )?,
                    };
                    // The report is checked against its evaluation's contract, and charged
                    // to the state's budget on the completion lane.
                    self.book.report_contract_view(
                        key,
                        registered.state.binding(),
                        registered.parent,
                        registered.registry,
                        descriptor,
                        authorization.schema(),
                        schemas,
                    )?;
                    return Ok(self.core.state.budget.reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
                ArtifactCommand::SubmitDiagnostic { claim, reason } => {
                    if respondent::select_diagnostic(
                        view,
                        context,
                        claim,
                        reason,
                        plan.descriptor(),
                        &self.book,
                        schemas,
                        self.core.limits,
                    )?
                    .is_some()
                    {
                        return Ok(self.core.state.budget.reserve(
                            BudgetKind::Pending,
                            BudgetLane::Completion,
                            bytes,
                        )?);
                    }
                }
                ArtifactCommand::SubmitWork { .. } | ArtifactCommand::RejectWork { .. } => {}
            },
            DecodedPlan::Response(plan) => {
                if respondent::select_response(
                    view,
                    context,
                    plan.claim(),
                    plan.response(),
                    plan.body(),
                    &self.book,
                    schemas,
                    self.core.limits,
                )?
                .is_some()
                {
                    return Ok(self.core.state.budget.reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
            }
            DecodedPlan::Fixed { input, .. } => {
                if let NativeCommand::PostResponse { claim, expected } = input.command
                    && respondent::select_post(
                        view,
                        context,
                        claim,
                        expected,
                        &self.book,
                        schemas,
                        self.core.limits,
                    )?
                    .is_some()
                {
                    return Ok(self.core.state.budget.reserve(
                        BudgetKind::Pending,
                        BudgetLane::Completion,
                        bytes,
                    )?);
                }
            }
            DecodedPlan::Monitor(_) | DecodedPlan::Projection(_) | DecodedPlan::Authored(_) => {}
        }
        Ok(self
            .core
            .state
            .budget
            .reserve(BudgetKind::Pending, BudgetLane::Ordinary, bytes)?)
    }
}
