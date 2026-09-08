//! Bounded actual-owner projection over a recorded successor. Only the affected
//! claim's response/current/retired cycle indices are walked; no ledger scan or
//! reconstructed participant execution is involved.
use super::read_validate_aggregate::{ProjectionSource, validate_source};
use super::replay_validate::{Overlay, ReplayRead, add, invalid, require};
use super::*;
use focal_model::lifecycle::aggregation::{
    self, AdmissionOutcome, AdmissionView, PublicationPosition, PublishedAdmissionResult,
    PublishedResponse, PublishedResult, RegisteredEvaluation, WholeWorkView,
};
use focal_model::lifecycle::evidence::WorkArtifact;
use std::cell::Cell;

fn model(error: NativeError) -> ContractError {
    match error {
        NativeError::Contract(error) => error,
        NativeError::Memory(_) | NativeError::Capacity(_) => ContractError::Capacity,
        _ => ContractError::InvalidManifest,
    }
}
struct Rows<'a, 'r, 'bytes, O> {
    read: &'a ReplayRead<'r, 'bytes, O>,
    claim: ClaimId,
    error: Cell<Option<ContractError>>,
}
impl<O: Overlay> Rows<'_, '_, '_, O> {
    fn get(&self, key: Key) -> Option<&Row> {
        if self.error.get().is_some() {
            return None;
        }
        match self.read.get(key) {
            Ok(value) => value,
            Err(error) => {
                self.error.set(Some(model(error)));
                None
            }
        }
    }
}
impl<O: Overlay> ProjectionSource for Rows<'_, '_, '_, O> {
    fn limits(&self) -> NativeLimits {
        self.read.limits
    }
    fn budget(&self) -> &MemoryBudget {
        self.read.budget
    }
    fn charge(&self, visits: usize) -> Result<(), NativeError> {
        self.read.charge(visits)
    }
    fn check(&self) -> Result<(), NativeError> {
        self.error.get().map_or(Ok(()), |error| Err(error.into()))
    }
}
impl<O: Overlay> WholeWorkView for Rows<'_, '_, '_, O> {
    fn prefix(&self) -> SessionSeq {
        self.read.outcome.sequence
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        as_definition(self.get(Key::Definition(id)))
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&validation::EvaluationState> {
        as_evaluation(self.get(Key::Evaluation(transactions::key_for_registered(
            self.claim, registered,
        ))))
    }
    fn accepted(&self, result: &validation::AcceptedResult) -> Option<PublishedResult<'_>> {
        let key = NativeResultKey::of(*result);
        let (result, sequence, ordinal) = match result.phase() {
            validation::Phase::Delivery => {
                let value = response_reads::as_delivery(self.get(Key::DeliveryResult(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
            validation::Phase::MissingTarget => {
                let value = response_reads::as_missing(self.get(Key::MissingResult(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
            validation::Phase::Programmatic | validation::Phase::Quality => {
                let value = as_result(self.get(Key::Accepted(key)))?;
                (value.result_ref(), value.sequence(), value.ordinal())
            }
        };
        Some(PublishedResult {
            result,
            position: PublicationPosition { sequence, ordinal },
        })
    }
    fn response(&self, id: TestamentId) -> Option<PublishedResponse<'_>> {
        let value = response_reads::as_response_record(self.get(Key::Response(id)))?;
        Some(PublishedResponse {
            response: value.response(),
            received: value.received(),
            entered: value.entered(),
        })
    }
    fn work(&self, id: ArtifactId) -> Option<&WorkArtifact> {
        as_work(self.get(Key::Work(id))).map(|value| &value.state)
    }
    fn works(&self, id: ClaimId) -> impl Iterator<Item = Result<&WorkArtifact, ContractError>> {
        let initial = (|| {
            self.read.charge(256)?;
            require(id == self.claim)?;
            let claim = self.read.claim(id)?;
            let count =
                u32::try_from(claim.response_count()).map_err(|_| ContractError::Capacity)?;
            require(
                count <= claim.max_responses() && (count != 0) == claim.latest_response().is_some(),
            )?;
            let retired = match self.read.get(Key::RetiredCycleHead(id))? {
                None => RetiredCycleHead {
                    head: None,
                    count: 0,
                    work_count: 0,
                },
                Some(Row::RetiredCycleHead(value)) => *value,
                _ => return Err(invalid()),
            };
            require((retired.count == 0) == retired.head.is_none())?;
            if retired.count > self.read.limits.plan_edges
                || retired.work_count > self.read.limits.plan_edges
            {
                return Err(ContractError::Capacity.into());
            }
            let open = match claim.receipt() {
                Some(receipt) if count < claim.max_responses() => Some((
                    NativeCycleKey {
                        claim: id,
                        receipt: receipt.fence.receipt,
                        epoch: receipt.fence.epoch,
                        cycle: count.checked_add(1).ok_or(ContractError::Capacity)?,
                    },
                    receipt.holder,
                )),
                _ => None,
            };
            Ok((claim, count, retired, open))
        })();
        let (mut initial, mut failed) = match initial {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(model(error))),
        };
        let mut loaded = false;
        let mut open = None;
        let mut next_response = None;
        let mut responses = 0u32;
        let mut retired = None;
        let mut retired_count = 0usize;
        let mut retired_work = 0usize;
        let mut retired_epoch = 0u64;
        let mut retired_cycle = 0u32;
        let mut cycle: Option<(NativeCycleKey, ParticipantId)> = None;
        let mut next_work = None;
        let mut work_remaining = 0usize;
        let mut total_work = 0usize;
        let mut finished = false;
        std::iter::from_fn(move || {
            if finished {
                return None;
            }
            if let Some(error) = failed.take() {
                finished = true;
                self.error.set(Some(error));
                return Some(Err(error));
            }
            let result = (|| {
                if !loaded {
                    let (claim, count, head, current) = initial.take().ok_or_else(invalid)?;
                    loaded = true;
                    open = current;
                    responses = count;
                    next_response = claim.latest_response().map(|value| value.testament);
                    retired = head.head;
                    retired_count = head.count;
                    retired_work = head.work_count;
                    retired_epoch = claim.receipt().map_or(0, |receipt| receipt.fence.epoch);
                    retired_cycle = if count == claim.max_responses() {
                        count
                    } else {
                        count.checked_add(1).ok_or(ContractError::Capacity)?
                    };
                }
                loop {
                    self.read.charge(256)?;
                    if let Some(artifact) = next_work {
                        work_remaining = work_remaining.checked_sub(1).ok_or_else(invalid)?;
                        total_work = add(total_work, 1)?;
                        if total_work > self.read.limits.plan_edges {
                            return Err(ContractError::Capacity.into());
                        }
                        let (key, holder) = cycle.ok_or_else(invalid)?;
                        let value = as_work(Some(self.read.require(Key::Work(artifact))?))
                            .ok_or_else(invalid)?;
                        let work = &value.state;
                        require(
                            work.reference().id == artifact
                                && work.claim() == id
                                && work.producer() == holder
                                && work.receipt()
                                    == (ReceiptFence {
                                        receipt: key.receipt,
                                        epoch: key.epoch,
                                    })
                                && work.cycle() == key.cycle,
                        )?;
                        next_work = value.next;
                        require((work_remaining == 0) == next_work.is_none())?;
                        return Ok(Some(work));
                    }
                    require(work_remaining == 0)?;
                    let (key, holder, expected_response, optional, retired_member) =
                        if let Some((key, holder)) = open.take() {
                            (key, holder, None, true, false)
                        } else if retired_count != 0 {
                            let key = retired.ok_or_else(invalid)?;
                            require(
                                key.claim == id
                                    && key.epoch != 0
                                    && key.epoch < retired_epoch
                                    && key.cycle != 0
                                    && key.cycle <= retired_cycle,
                            )?;
                            let Row::RetiredCycle(link) =
                                self.read.require(Key::RetiredCycle(key))?
                            else {
                                return Err(invalid());
                            };
                            retired_count = retired_count.checked_sub(1).ok_or_else(invalid)?;
                            retired = link.next;
                            retired_epoch = key.epoch;
                            retired_cycle = key.cycle;
                            require((retired_count == 0) == retired.is_none())?;
                            (key, link.holder, None, false, true)
                        } else if responses != 0 {
                            require(retired.is_none() && retired_work == 0)?;
                            let response_id = next_response.ok_or_else(invalid)?;
                            let value = response_reads::as_response_record(Some(
                                self.read.require(Key::Response(response_id))?,
                            ))
                            .ok_or_else(invalid)?;
                            let response = value.response();
                            let identity = response.identity();
                            require(
                                identity.claim == id
                                    && identity.binding.object.0 == response_id.0
                                    && identity.cycle == responses,
                            )?;
                            responses = responses.checked_sub(1).ok_or_else(invalid)?;
                            next_response = identity.prior;
                            (
                                NativeCycleKey {
                                    claim: id,
                                    receipt: identity.receipt.receipt,
                                    epoch: identity.receipt.epoch,
                                    cycle: identity.cycle,
                                },
                                response.respondent(),
                                Some(response_id),
                                false,
                                false,
                            )
                        } else {
                            require(
                                next_response.is_none() && retired.is_none() && retired_work == 0,
                            )?;
                            return Ok(None);
                        };
                    let value = match self.read.get(Key::Cycle(key))? {
                        None if optional => continue,
                        Some(Row::Cycle(value)) => *value,
                        _ => return Err(invalid()),
                    };
                    if value.work_count > self.read.limits.work_artifacts_per_cycle
                        || value.diagnostic_count > self.read.limits.diagnostics_per_cycle
                    {
                        return Err(ContractError::Capacity.into());
                    }
                    require(
                        value.response == expected_response
                            && (value.work_count == 0) == value.work_head.is_none()
                            && (value.diagnostic_count == 0) == value.diagnostic_head.is_none(),
                    )?;
                    let Row::Receipt(receipt) = self.read.require(Key::Receipt(key.receipt))?
                    else {
                        return Err(invalid());
                    };
                    require(
                        receipt.claim == id
                            && receipt.holder == holder
                            && receipt.fence
                                == (ReceiptFence {
                                    receipt: key.receipt,
                                    epoch: key.epoch,
                                }),
                    )?;
                    if retired_member {
                        retired_work = retired_work
                            .checked_sub(value.work_count)
                            .ok_or_else(invalid)?;
                    }
                    cycle = Some((key, holder));
                    next_work = value.work_head;
                    work_remaining = value.work_count;
                }
            })();
            match result {
                Ok(Some(work)) => Some(Ok(work)),
                Ok(None) => {
                    finished = true;
                    None
                }
                Err(error) => {
                    finished = true;
                    let error = model(error);
                    self.error.set(Some(error));
                    Some(Err(error))
                }
            }
        })
    }
}

impl<O: Overlay> AdmissionView for Rows<'_, '_, '_, O> {
    fn prefix(&self) -> SessionSeq {
        WholeWorkView::prefix(self)
    }
    fn declaration(&self, id: ValidationId) -> Option<&validation::Declaration> {
        WholeWorkView::declaration(self, id)
    }
    fn evaluation(&self, registered: RegisteredEvaluation) -> Option<&validation::EvaluationState> {
        WholeWorkView::evaluation(self, registered)
    }
    fn accepted(
        &self,
        result: &validation::AcceptedResult,
    ) -> Option<PublishedAdmissionResult<'_>> {
        let value = as_result(self.get(Key::Accepted(NativeResultKey::of(*result))))?;
        Some(PublishedAdmissionResult {
            result: value.result_ref(),
            sequence: value.sequence(),
            ordinal: value.ordinal(),
        })
    }
}

fn admission<O: Overlay>(
    claim: &ClaimState,
    registry: &RegistrationSet,
    rows: &Rows<'_, '_, '_, O>,
) -> Result<(), NativeError> {
    use focal_model::lifecycle::claim::ClaimTerminalCut;
    let read = rows.read;
    let mut fields = add(
        add(
            claim.acceptance().declarations().len(),
            registry.rows().len(),
        )?,
        1,
    )?;
    let mut slots = claim.acceptance().slots();
    loop {
        read.charge(1)?;
        let Some(slot) = slots.next() else {
            break;
        };
        fields = add(fields, add(slot.checks.len(), 1)?)?;
    }
    read.charge(fields.checked_mul(1024).ok_or(ContractError::Capacity)?)?;
    let visits = aggregation::admission_completion_visits(claim, registry)?;
    // Reserve the model's own allowance before callbacks spend lookup work.
    read.charge(visits)?;
    let decision = aggregation::project_admission(
        claim,
        registry,
        rows,
        aggregation::AdmissionLimits {
            declarations: read.limits.definitions,
            evaluations: read.limits.evaluations_per_claim,
            visits,
        },
    );
    rows.check()?;
    match decision?.outcome() {
        AdmissionOutcome::Passed => (),
        AdmissionOutcome::Pending => require(claim.receipt().is_none())?,
        AdmissionOutcome::Blocked(required) => {
            require(claim.receipt().is_none())?;
            let cut = claim.terminal_cut().ok_or_else(invalid)?;
            let position = match cut {
                ClaimTerminalCut::Explicit(cut) => cut.position,
                ClaimTerminalCut::Required(cut) => cut.sequence(),
                ClaimTerminalCut::Graph(cut) => cut.sequence(),
            };
            require(position <= required.sequence())?;
            if matches!(cut, ClaimTerminalCut::Required(_)) {
                require(cut == ClaimTerminalCut::Required(required))?;
            }
        }
    }
    Ok(())
}

pub(super) fn validate<O: Overlay>(
    read: &ReplayRead<'_, '_, O>,
    claim: &OwnedClaim,
) -> Result<(), NativeError> {
    let state = claim.claim().ok_or_else(invalid)?;
    let registry = claim.registrations().ok_or_else(invalid)?;
    let rows = Rows {
        read,
        claim: ClaimId(state.binding().object.0),
        error: Cell::new(None),
    };
    admission(state, registry, &rows)?;
    validate_source(state, registry, &rows)
}
