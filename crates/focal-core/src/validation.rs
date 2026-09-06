//! Durable pinned validation scheduling and error-only fallback.
use crate::reduce::Reducer;
use crate::*;

impl Reducer<'_> {
    pub fn schedule_phase(
        &mut self,
        claim: ClaimId,
        phase: ValidationPhase,
        target: ContentHash,
        manifest: ContentHash,
    ) -> Result<(), DomainOutcome> {
        let mut ids = Vec::new();
        for (id, _) in self.state.validations.iter().filter(|(_, v)| {
            v.content().claim == claim
                && v.content().phase == phase
                && v.content().kind != ValidationKind::Receipt
        }) {
            self.state.scratch(1)?;
            ids.push(*id);
        }
        for id in ids {
            self.schedule(id, target, manifest)?;
        }
        Ok(())
    }
    pub fn schedule(
        &mut self,
        id: ValidationId,
        target: ContentHash,
        manifest: ContentHash,
    ) -> Result<ValidationRunId, DomainOutcome> {
        let v = self
            .state
            .validations
            .get_owned(&id)?
            .ok_or_else(|| refuse(ErrorCode::UnknownObject, "unknown validation"))?;
        if self
            .state
            .runs
            .values()
            .any(|r| r.id.validation == id && r.id.target_hash == target)
        {
            return Err(refuse(
                ErrorCode::ConflictingVerdict,
                "target already scheduled",
            ));
        }
        let epoch = v
            .lifecycle()
            .latest_epoch
            .checked_add(1)
            .ok_or_else(|| refuse(ErrorCode::Capacity, "validation epoch exhausted"))?;
        let run_id = ValidationRunId {
            validation: id,
            target_hash: target,
            phase: v.content().phase,
            epoch,
        };
        let first = v
            .content()
            .handlers
            .first()
            .ok_or_else(|| refuse(ErrorCode::InvalidHandler, "validation handler absent"))?;
        let run = ValidationRun {
            id: run_id,
            claim: v.content().claim,
            evaluator: v.content().evaluator,
            manifest,
            handler_index: 0,
            quality_phase: first.agentic && v.content().quality_bar.is_some(),
            attempts: Vec::new(),
            final_verdict: None,
        };
        self.state.runs.insert(run_id, run);
        self.state.validations.insert(
            id,
            v.with_lifecycle(ValidationLifecycle {
                created: v.lifecycle().created,
                latest_epoch: epoch,
            }),
        );
        self.emit(
            Some(v.content().claim),
            LifecycleAction::ValidationScheduled,
            DeltaFact::ValidationScheduled(run_id),
        )?;
        self.effect(EffectIntent::ExecuteValidation {
            run: run_id,
            handler: first.clone(),
            evaluator: v.content().evaluator,
            attempt: 0,
            manifest,
            quality_phase: first.agentic && v.content().quality_bar.is_some(),
        })?;
        Ok(run_id)
    }
    pub fn receipt_passes(
        &mut self,
        claim: ClaimId,
        target: ContentHash,
        manifest: ContentHash,
    ) -> Result<(), DomainOutcome> {
        let mut ids = Vec::new();
        for (id, _) in self.state.validations.iter().filter(|(_, v)| {
            v.content().claim == claim && v.content().kind == ValidationKind::Receipt
        }) {
            self.state.scratch(1)?;
            ids.push(*id);
        }
        for id in ids {
            let v = self.state.validations.get_owned(&id)?.ok_or_else(|| {
                refuse(ErrorCode::InvalidManifest, "scheduled validation missing")
            })?;
            if self
                .state
                .runs
                .keys()
                .any(|r| r.validation == id && r.target_hash == target)
            {
                continue;
            }
            let epoch = v
                .lifecycle()
                .latest_epoch
                .checked_add(1)
                .ok_or_else(|| refuse(ErrorCode::Capacity, "validation epoch exhausted"))?;
            let run_id = ValidationRunId {
                validation: id,
                target_hash: target,
                phase: ValidationPhase::WholeWork,
                epoch,
            };
            let verdict = VerdictRecord {
                run: run_id,
                evaluator: v.content().evaluator,
                handler: HandlerRef {
                    id: ValidatorId::from_u128(1),
                    version: ContentHash(*blake3::hash(b"focal.builtin.receipt.v1").as_bytes()),
                    agentic: false,
                },
                attempt: 0,
                manifest,
                value: VerdictValue::Pass,
                evidence: Vec::new(),
            };
            self.state.runs.insert(
                run_id,
                ValidationRun {
                    id: run_id,
                    claim,
                    evaluator: v.content().evaluator,
                    manifest,
                    handler_index: 0,
                    quality_phase: false,
                    attempts: vec![verdict.clone()],
                    final_verdict: Some(VerdictValue::Pass),
                },
            );
            self.state.validations.insert(
                id,
                v.with_lifecycle(ValidationLifecycle {
                    created: v.lifecycle().created,
                    latest_epoch: epoch,
                }),
            );
            self.emit(
                Some(claim),
                LifecycleAction::ValidationVerdict,
                DeltaFact::Verdict(verdict),
            )?;
        }
        Ok(())
    }
    pub fn phase_complete(&self, claim: ClaimId, phase: ValidationPhase) -> bool {
        self.state
            .validations
            .iter()
            .filter(|(_, v)| {
                v.content().claim == claim
                    && v.content().phase == phase
                    && v.content().mode == ValidationMode::Required
            })
            .all(|(id, _)| {
                let mut runs = self
                    .state
                    .runs
                    .values()
                    .filter(|r| r.id.validation == *id)
                    .peekable();
                runs.peek().is_some() && runs.all(|r| r.final_verdict.is_some())
            })
    }
    pub fn phase_passed(&self, claim: ClaimId, phase: ValidationPhase) -> bool {
        self.state
            .validations
            .iter()
            .filter(|(_, v)| {
                v.content().claim == claim
                    && v.content().phase == phase
                    && v.content().mode == ValidationMode::Required
            })
            .all(|(id, _)| {
                let mut runs = self
                    .state
                    .runs
                    .values()
                    .filter(|r| r.id.validation == *id)
                    .peekable();
                runs.peek().is_some() && runs.all(|r| r.final_verdict == Some(VerdictValue::Pass))
            })
    }
    pub fn aggregate(&self, claim: ClaimId) -> Option<VerdictValue> {
        let mut worst = VerdictValue::Pass;
        let mut any = false;
        for (id, v) in &self.state.validations {
            if v.content().claim != claim
                || v.content().mode != ValidationMode::Required
                || v.content().phase == ValidationPhase::Admission
            {
                continue;
            }
            let mut runs = self
                .state
                .runs
                .values()
                .filter(|r| r.id.validation == *id)
                .peekable();
            runs.peek()?;
            any = true;
            for r in runs {
                let result = r.final_verdict?;
                if result.severity() > worst.severity() {
                    worst = result
                }
            }
        }
        any.then_some(worst)
    }
    pub fn verdict(&mut self, verdict: &VerdictRecord) -> Result<CommandResult, DomainOutcome> {
        let mut run = self
            .state
            .runs
            .get_owned(&verdict.run)?
            .ok_or_else(|| refuse(ErrorCode::StaleEvaluator, "unknown validation run epoch"))?;
        let v = self
            .state
            .validations
            .get_owned(&run.id.validation)?
            .ok_or_else(|| refuse(ErrorCode::UnknownObject, "validation requirement"))?;
        let c = self.claim(run.claim)?;
        if verdict.evaluator != self.input.principal || run.evaluator != self.input.principal {
            return Err(refuse(
                ErrorCode::WrongActor,
                "only designated evaluator may submit verdict",
            ));
        }
        if matches!(
            c.lifecycle().status,
            ClaimStatus::Cancelled
                | ClaimStatus::Revoked
                | ClaimStatus::Superseded
                | ClaimStatus::Expired
                | ClaimStatus::Deadlocked
                | ClaimStatus::DependencyFailed
        ) {
            return Err(refuse(
                ErrorCode::StaleEvaluator,
                "execution fenced by terminal control",
            ));
        }
        if let Some(old) = run.attempts.iter().find(|v| v.attempt == verdict.attempt) {
            return if old == verdict {
                Ok(CommandResult::Validation(run.id))
            } else {
                Err(refuse(
                    ErrorCode::ConflictingVerdict,
                    "attempt already has a different immutable verdict",
                ))
            };
        }
        if run.final_verdict.is_some() {
            return Err(refuse(
                ErrorCode::ConflictingVerdict,
                "run already has terminal verdict",
            ));
        }
        if c.lifecycle().status.is_terminal() && v.content().mode == ValidationMode::Required {
            return Err(DomainOutcome::Inform {
                claim: Some(run.claim),
                reason: InformReason::Terminal(c.lifecycle().status),
            });
        }
        let valid_state = match run.id.phase {
            ValidationPhase::Admission => c.lifecycle().status == ClaimStatus::Posted,
            ValidationPhase::Increment => matches!(
                c.lifecycle().status,
                ClaimStatus::Received
                    | ClaimStatus::Progressed
                    | ClaimStatus::TestamentGenerated
                    | ClaimStatus::TestamentAcknowledged
                    | ClaimStatus::Validating
            ),
            ValidationPhase::WholeWork => c.lifecycle().status == ClaimStatus::Validating,
        };
        if !(valid_state
            || c.lifecycle().status.is_terminal() && v.content().mode == ValidationMode::Observe)
        {
            return Err(refuse(
                ErrorCode::InvalidTransition,
                "verdict phase is not active",
            ));
        }
        let current = v
            .content()
            .handlers
            .get(run.handler_index as usize)
            .ok_or_else(|| refuse(ErrorCode::InvalidHandler, "handler index"))?;
        if verdict.handler != *current
            || verdict.attempt
                != u32::try_from(run.attempts.len())
                    .map_err(|_| refuse(ErrorCode::Capacity, "validation attempt exhausted"))?
            || verdict.manifest != run.manifest
        {
            return Err(refuse(
                ErrorCode::StaleEvaluator,
                "handler, attempt or evidence fence mismatch",
            ));
        }
        self.verify_artifacts(&verdict.evidence)?;
        if !v.content().evidence_schemas.is_empty() {
            let mut actual = BTreeSet::new();
            let mut diagnostic = false;
            for reference in &verdict.evidence {
                let artifact = self.state.artifacts.get(&reference.id).ok_or_else(|| {
                    refuse(ErrorCode::InvalidManifest, "verdict artifact missing")
                })?;
                self.state.scratch(1)?;
                actual.insert(artifact.content().schema_hash);
                diagnostic |= artifact.content().kind == "error";
            }
            let unavailable = matches!(
                verdict.value,
                VerdictValue::Incomplete | VerdictValue::Error
            ) && diagnostic;
            if !v.content().evidence_schemas.is_subset(&actual) && !unavailable {
                return Err(refuse(
                    ErrorCode::InvalidManifest,
                    "verdict evidence lacks required schema",
                ));
            }
        }
        run.attempts.push(verdict.clone());
        let following_handler = (run.handler_index as usize)
            .checked_add(1)
            .ok_or_else(|| refuse(ErrorCode::InvalidHandler, "handler index exhausted"))?;
        let next = match verdict.value {
            VerdictValue::Error if !run.quality_phase => v
                .content()
                .handlers
                .iter()
                .enumerate()
                .skip(following_handler)
                .find(|(_, handler)| v.content().quality_bar.is_none() || !handler.agentic)
                .map(|(index, _)| index),
            VerdictValue::Pass if !run.quality_phase && v.content().quality_bar.is_some() => v
                .content()
                .handlers
                .iter()
                .enumerate()
                .skip(following_handler)
                .find(|(_, h)| h.agentic)
                .map(|(i, _)| {
                    run.quality_phase = true;
                    i
                }),
            _ => None,
        };
        if let Some(next) = next {
            let handler = v
                .content()
                .handlers
                .get(next)
                .ok_or_else(|| refuse(ErrorCode::InvalidHandler, "next handler missing"))?;
            run.handler_index = u32::try_from(next)
                .map_err(|_| refuse(ErrorCode::InvalidHandler, "handler index exhausted"))?;
            run.quality_phase = handler.agentic && v.content().quality_bar.is_some();
            self.effect(EffectIntent::ExecuteValidation {
                run: run.id,
                handler: handler.clone(),
                evaluator: run.evaluator,
                attempt: u32::try_from(run.attempts.len())
                    .map_err(|_| refuse(ErrorCode::Capacity, "validation attempt exhausted"))?,
                manifest: run.manifest,
                quality_phase: run.quality_phase,
            })?;
        } else {
            run.final_verdict = Some(verdict.value)
        }
        self.state.runs.insert(run.id, run.clone());
        self.emit(
            Some(run.claim),
            LifecycleAction::ValidationVerdict,
            DeltaFact::Verdict(verdict.clone()),
        )?;
        if run.id.phase == ValidationPhase::Admission
            && v.content().mode == ValidationMode::Required
            && self.phase_complete(run.claim, ValidationPhase::Admission)
            && !self.phase_passed(run.claim, ValidationPhase::Admission)
        {
            self.status(run.claim, ClaimStatus::PostFailed, None)?
        }
        Ok(CommandResult::Validation(run.id))
    }
}
