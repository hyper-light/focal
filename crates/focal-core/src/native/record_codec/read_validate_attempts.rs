//! Scalar proof of the complete retained attempt chain. This checks recorded
//! transitions against the immutable declaration; it never reruns participant
//! authorization, owner readiness, a clock, a handler or an agent.
use super::read_validate::{ValidationRead, invalid};
use super::*;
use focal_model::lifecycle::validation::{
    AcceptedResult, Attempt, AuthorityFence, Declaration, EvaluationState, FenceReason,
    HandlerPolicy, Phase, PhasePolicyView, ProgramView, State, Suppression, Target,
};
use focal_model::{ArtifactRef, ValidationMode, VerdictValue};

#[cfg(test)]
#[path = "read_validate_attempts_tests.rs"]
mod tests;

pub(super) trait Read {
    fn charge(&self, visits: usize) -> Result<(), NativeError>;
    fn schema(&self, artifact: ArtifactRef) -> Result<ContentHash, NativeError>;
}
impl Read for ValidationRead<'_, '_> {
    fn charge(&self, visits: usize) -> Result<(), NativeError> {
        ValidationRead::charge(self, visits)
    }
    fn schema(&self, reference: ArtifactRef) -> Result<ContentHash, NativeError> {
        self.charge(64)?;
        let artifact = self.artifact(reference.id)?.descriptor();
        if artifact.id() != reference.id || artifact.content_hash() != reference.hash {
            return Err(invalid());
        }
        Ok(artifact.schema_hash())
    }
}
#[derive(Clone, Copy)]
enum Suppressed {
    None,
    Begin,
    Missing,
    Cohort,
}
/// Only fixed scalar history plus a borrow of the real immutable policy. Each
/// report names its original attempt, while an event's phase describes the next
/// cursor, including a successful programmatic-to-quality transition.
pub(super) struct AttemptCursor<'a> {
    declaration: &'a Declaration,
    materialized: bool,
    state: State,
    phase: Phase,
    handler: usize,
    handler_attempt: u32,
    attempt: u32,
    begun: bool,
    suppressed: Suppressed,
    retained_suppression: Option<Suppression>,
    missing: bool,
    sealed: bool,
    retained_seal: Option<ContentHash>,
    fence: Option<AuthorityFence>,
    programmatic: Option<ArtifactRef>,
    last_result: Option<AcceptedResult>,
}
fn require(value: bool) -> Result<(), NativeError> {
    if value { Ok(()) } else { Err(invalid()) }
}
fn add(value: usize, extra: usize) -> Result<usize, NativeError> {
    value
        .checked_add(extra)
        .ok_or(NativeError::Capacity("attempt history work"))
}
impl<'a> AttemptCursor<'a> {
    pub(super) fn new(declaration: &'a Declaration) -> Self {
        let phase = match declaration.program() {
            ProgramView::Delivery => Phase::Delivery,
            ProgramView::Programmatic { .. } => Phase::Programmatic,
            ProgramView::Agentic { .. } => Phase::Quality,
        };
        Self {
            declaration,
            materialized: false,
            state: State::Ready,
            phase,
            handler: 0,
            handler_attempt: 0,
            attempt: 0,
            begun: false,
            suppressed: Suppressed::None,
            retained_suppression: None,
            missing: false,
            sealed: false,
            retained_seal: None,
            fence: None,
            programmatic: None,
            last_result: None,
        }
    }
    /// Continue from an actual row in the already validated predecessor root.
    /// The caller proves unchanged evaluation identity and publication linkage;
    /// this is not a substitute for full checkpoint/history validation. Binding
    /// verifies the original semantic declaration stamp without rescanning it.
    pub(super) fn resume(
        declaration: &'a Declaration,
        value: &EvaluationState,
        read: &impl Read,
    ) -> Result<Self, NativeError> {
        read.charge(1024)?;
        value.bind(declaration)?;
        let snapshot = value.snapshot_v1()?;
        require(
            snapshot.binding.ledger == declaration.binding().ledger
                && snapshot.binding.object == declaration.binding().object
                && snapshot.binding.content == declaration.binding().content,
        )?;
        let mut cursor = Self::new(declaration);
        let initial_phase = cursor.phase;
        cursor.materialized = true;
        cursor.state = snapshot.state;
        cursor.phase = snapshot.phase;
        cursor.handler = usize::try_from(snapshot.handler)
            .map_err(|_| NativeError::Capacity("attempt predecessor handler"))?;
        cursor.handler_attempt = snapshot.handler_attempt;
        cursor.attempt = snapshot.attempt;
        cursor.begun = snapshot.begun;
        cursor.suppressed = match snapshot.suppression {
            None => Suppressed::None,
            Some(Suppression::MissingTarget) => Suppressed::Missing,
            Some(Suppression::CohortSealed(cause)) if snapshot.sealed == Some(cause) => {
                Suppressed::Cohort
            }
            Some(Suppression::ParentFailure(_))
            | Some(Suppression::ArtifactFailure(_))
            | Some(Suppression::CohortSealed(_)) => Suppressed::Begin,
        };
        cursor.retained_suppression = snapshot.suppression;
        cursor.missing = matches!(snapshot.target, Target::MissingSlot { .. });
        cursor.sealed = snapshot.sealed.is_some();
        cursor.retained_seal = snapshot.sealed;
        cursor.fence = snapshot.fence;
        cursor.programmatic = snapshot.programmatic_evidence;
        cursor.last_result = value.last_result();
        if cursor.begun {
            require(snapshot.suppression.is_none() && !cursor.missing)?;
            cursor.step(read)?;
            require(
                cursor.state.is_terminal()
                    || matches!(
                        (cursor.phase, cursor.state),
                        (Phase::Programmatic, State::Validating)
                            | (Phase::Quality, State::ValidatingQualityBar)
                    ),
            )?;
        } else {
            require(
                cursor.phase == initial_phase
                    && cursor.handler == 0
                    && cursor.handler_attempt == 0
                    && cursor.attempt == 0
                    && cursor.programmatic.is_none(),
            )?;
        }
        if let Some(result) = cursor.last_result {
            cursor.result_identity(result)?;
            require(
                result.target() == snapshot.target
                    && result.generation() == snapshot.generation
                    && result.receipt() == snapshot.receipt
                    && result.resulting_state() == snapshot.state
                    && result.programmatic_evidence() == snapshot.programmatic_evidence,
            )?;
        }
        Ok(cursor)
    }
    fn policy(&self) -> Result<PhasePolicyView<'a>, NativeError> {
        match (self.declaration.program(), self.phase) {
            (ProgramView::Programmatic { check, .. }, Phase::Programmatic)
            | (
                ProgramView::Programmatic {
                    quality: Some(check),
                    ..
                },
                Phase::Quality,
            )
            | (ProgramView::Agentic { check }, Phase::Quality) => Ok(check),
            _ => Err(invalid()),
        }
    }
    fn step(&self, read: &impl Read) -> Result<(Attempt, HandlerPolicy<'a>, usize), NativeError> {
        let policy = self.policy()?;
        let mut handlers = policy.handlers();
        let count = handlers.len();
        require(self.handler < count)?;
        // The current implementation indexes a slice iterator. Charge even a
        // linear implementation before nth so policy exposure stays bounded.
        read.charge(add(self.handler, 2)?)?;
        let step = handlers.nth(self.handler).ok_or_else(invalid)?;
        require(
            self.handler_attempt < step.attempts && self.attempt < self.declaration.attempt_bound(),
        )?;
        Ok((
            Attempt {
                phase: self.phase,
                index: self.attempt,
                handler: step.handler.id,
                version: step.handler.version,
                evaluator: policy.evaluator(),
                definition: policy.definition(),
            },
            step,
            count,
        ))
    }
    fn current(&self, read: &impl Read) -> Result<Option<Attempt>, NativeError> {
        if self.begun && !self.state.is_terminal() {
            self.step(read).map(|(attempt, _, _)| Some(attempt))
        } else {
            Ok(None)
        }
    }
    fn advance(&mut self) -> Result<(), NativeError> {
        let next = self
            .attempt
            .checked_add(1)
            .ok_or(NativeError::Capacity("attempt history counter"))?;
        require(next < self.declaration.attempt_bound())?;
        self.attempt = next;
        Ok(())
    }
    fn result_identity(&self, result: AcceptedResult) -> Result<(), NativeError> {
        require(
            result.ledger() == self.declaration.binding().ledger
                && result.claim() == self.declaration.claim()
                && result.validation().0 == self.declaration.binding().object.0
                && result.declaration_index() == self.declaration.declaration_index()
                && result.mode() == self.declaration.mode(),
        )
    }
    #[allow(clippy::too_many_arguments)] // One exact retained event and its result, not a participant API.
    pub(super) fn event(
        &mut self,
        kind: NativeEvaluationEventKind,
        attempt: Option<Attempt>,
        state: State,
        phase: Phase,
        fence: Option<AuthorityFence>,
        result: Option<AcceptedResult>,
        read: &impl Read,
    ) -> Result<(), NativeError> {
        read.charge(512)?;
        if kind == NativeEvaluationEventKind::Materialized {
            require(
                !self.materialized
                    && state == State::Ready
                    && phase == self.phase
                    && attempt.is_none()
                    && fence.is_none()
                    && result.is_none(),
            )?;
            self.materialized = true;
            return Ok(());
        }
        require(self.materialized)?;
        match kind {
            NativeEvaluationEventKind::Begun => {
                require(
                    self.state == State::Ready
                        && !self.begun
                        && !self.sealed
                        && self.fence.is_none()
                        && matches!(self.suppressed, Suppressed::None)
                        && self.phase != Phase::Delivery
                        && phase == self.phase
                        && fence.is_none()
                        && result.is_none(),
                )?;
                if state == State::Ready {
                    require(attempt.is_none())?;
                    self.suppressed = Suppressed::Begin;
                } else {
                    let (expected, _, _) = self.step(read)?;
                    let expected_state = match self.phase {
                        Phase::Programmatic => State::Validating,
                        Phase::Quality => State::ValidatingQualityBar,
                        _ => return Err(invalid()),
                    };
                    require(attempt == Some(expected) && state == expected_state)?;
                    self.begun = true;
                    self.state = state;
                }
            }
            NativeEvaluationEventKind::Reported => {
                require(
                    self.begun
                        && !self.state.is_terminal()
                        && self.fence.is_none()
                        && matches!(self.suppressed, Suppressed::None)
                        && fence.is_none(),
                )?;
                let result = result.ok_or_else(invalid)?;
                let (expected, step, count) = self.step(read)?;
                self.result_identity(result)?;
                require(
                    attempt == Some(expected)
                        && result.phase() == self.phase
                        && result.attempt() == Some(self.attempt)
                        && result.reporter() == Some(expected.evaluator),
                )?;
                let evidence = result.evidence().ok_or_else(invalid)?;
                let schema = match result.verdict() {
                    VerdictValue::Pass | VerdictValue::Fail => step.proof_schema,
                    VerdictValue::Incomplete | VerdictValue::Error => step.diagnostic_schema,
                };
                require(read.schema(evidence)? == schema)?;
                match result.verdict() {
                    VerdictValue::Pass => {
                        if self.phase == Phase::Programmatic {
                            self.programmatic = Some(evidence);
                            if matches!(
                                self.declaration.program(),
                                ProgramView::Programmatic {
                                    quality: Some(_),
                                    ..
                                }
                            ) {
                                self.phase = Phase::Quality;
                                self.handler = 0;
                                self.handler_attempt = 0;
                                self.advance()?;
                                self.state = State::ValidatingQualityBar;
                            } else {
                                self.state = State::Validated;
                            }
                        } else {
                            self.state = State::Validated;
                        }
                    }
                    VerdictValue::Fail => {
                        self.state = match (self.phase, self.declaration.mode()) {
                            (Phase::Programmatic, ValidationMode::Required) => {
                                State::ValidationFailed
                            }
                            (Phase::Programmatic, ValidationMode::Observe) => {
                                State::ValidationFailedNotRequired
                            }
                            (Phase::Quality, ValidationMode::Required) => {
                                State::QualityBarValidationFailed
                            }
                            (Phase::Quality, ValidationMode::Observe) => {
                                State::QualityBarValidationFailedNotRequired
                            }
                            _ => return Err(invalid()),
                        };
                    }
                    VerdictValue::Incomplete => self.state = State::ValidationIncomplete,
                    VerdictValue::Error => {
                        let local = self
                            .handler_attempt
                            .checked_add(1)
                            .ok_or(NativeError::Capacity("handler history counter"))?;
                        if local < step.attempts {
                            self.handler_attempt = local;
                            self.advance()?;
                        } else {
                            let handler = add(self.handler, 1)?;
                            if handler < count {
                                self.handler = handler;
                                self.handler_attempt = 0;
                                self.advance()?;
                            } else {
                                self.state = match self.declaration.mode() {
                                    ValidationMode::Required => State::Errored,
                                    ValidationMode::Observe => State::ErroredNotRequired,
                                };
                            }
                        }
                    }
                }
                require(
                    state == self.state
                        && phase == self.phase
                        && result.resulting_state() == self.state
                        && result.programmatic_evidence() == self.programmatic,
                )?;
                self.last_result = Some(result);
            }
            NativeEvaluationEventKind::MissingTarget => {
                require(
                    self.state == State::Ready
                        && !self.begun
                        && !self.sealed
                        && self.fence.is_none()
                        && matches!(self.suppressed, Suppressed::None)
                        && phase == Phase::MissingTarget
                        && attempt.is_none()
                        && fence.is_none()
                        && self.phase != Phase::Delivery,
                )?;
                self.missing = true;
                match self.declaration.mode() {
                    ValidationMode::Required => {
                        let result = result.ok_or_else(invalid)?;
                        self.result_identity(result)?;
                        require(
                            result.phase() == Phase::MissingTarget
                                && result.verdict() == VerdictValue::Incomplete
                                && result.attempt().is_none()
                                && result.evidence().is_none()
                                && result.reporter().is_none()
                                && result.programmatic_evidence().is_none()
                                && result.resulting_state() == State::ValidationIncomplete
                                && state == State::ValidationIncomplete,
                        )?;
                        self.state = state;
                        self.last_result = Some(result);
                    }
                    ValidationMode::Observe => {
                        require(result.is_none() && state == State::Ready)?;
                        self.suppressed = Suppressed::Missing;
                    }
                }
            }
            NativeEvaluationEventKind::AuthorityFenced => {
                require(
                    !self.state.is_terminal()
                        && self.fence.is_none()
                        && state == self.state
                        && phase == self.phase
                        && result.is_none()
                        && attempt == self.current(read)?,
                )?;
                let next = fence.ok_or_else(invalid)?;
                if let FenceReason::Deadline(deadline) = next.reason {
                    require(deadline == self.declaration.deadline())?;
                }
                self.fence = Some(next);
            }
            NativeEvaluationEventKind::Sealed => {
                require(
                    !self.state.is_terminal()
                        && !self.sealed
                        && state == self.state
                        && phase == self.phase
                        && fence == self.fence
                        && result.is_none()
                        && attempt == self.current(read)?,
                )?;
                self.sealed = true;
                if !self.begun && matches!(self.suppressed, Suppressed::None) {
                    self.suppressed = Suppressed::Cohort;
                }
            }
            NativeEvaluationEventKind::Materialized => return Err(invalid()),
        }
        Ok(())
    }
    /// Pure delivery publishes a DeliveryResult without an EvaluationReported
    /// event. Its exact position/revision is checked by the enclosing history.
    pub(super) fn delivery(
        &mut self,
        result: AcceptedResult,
        read: &impl Read,
    ) -> Result<(), NativeError> {
        read.charge(256)?;
        self.result_identity(result)?;
        require(
            self.materialized
                && self.state == State::Ready
                && self.phase == Phase::Delivery
                && !self.begun
                && !self.sealed
                && self.fence.is_none()
                && matches!(self.suppressed, Suppressed::None)
                && self.last_result.is_none()
                && result.phase() == Phase::Delivery
                && result.verdict() == VerdictValue::Pass
                && result.resulting_state() == State::Validated
                && result.attempt().is_none()
                && result.evidence().is_none()
                && result.reporter().is_none()
                && result.programmatic_evidence().is_none(),
        )?;
        self.state = State::Validated;
        self.last_result = Some(result);
        Ok(())
    }
    pub(super) fn finish(
        &self,
        value: &EvaluationState,
        read: &impl Read,
    ) -> Result<(), NativeError> {
        read.charge(512)?;
        let snapshot = value.snapshot_v1()?;
        require(
            self.materialized
                && snapshot.state == self.state
                && snapshot.phase == self.phase
                && snapshot.handler
                    == u64::try_from(self.handler)
                        .map_err(|_| NativeError::Capacity("handler index"))?
                && snapshot.handler_attempt == self.handler_attempt
                && snapshot.attempt == self.attempt
                && snapshot.begun == self.begun
                && snapshot.sealed.is_some() == self.sealed
                && self
                    .retained_seal
                    .is_none_or(|cause| snapshot.sealed == Some(cause))
                && self
                    .retained_suppression
                    .is_none_or(|cause| snapshot.suppression == Some(cause))
                && snapshot.fence == self.fence
                && snapshot.programmatic_evidence == self.programmatic
                && value.last_result() == self.last_result,
        )?;
        if self.missing {
            require(matches!(snapshot.target, Target::MissingSlot { .. }))?;
        }
        match (self.suppressed, snapshot.suppression) {
            (Suppressed::None, None)
            | (Suppressed::Begin, Some(_))
            | (Suppressed::Missing, Some(Suppression::MissingTarget)) => (),
            (Suppressed::Cohort, Some(Suppression::CohortSealed(cause)))
                if Some(cause) == snapshot.sealed => {}
            _ => return Err(invalid()),
        }
        Ok(())
    }
}
