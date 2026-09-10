//! Trusted monitor timers have a disjoint exact identity. Resolve only after
//! checking the actual pending prefix; expensive graph work awaits reservation.
use super::*;

impl Core<NativeState> {
    pub(in crate::native) fn check_monitor_deadline_chain<'a, 'p: 'a>(
        &'a self,
        input: NativeMonitorDeadlineInput,
        logical_time: u64,
        mut pending: impl DoubleEndedIterator<Item = &'p NativePrepared> + ExactSizeIterator + Clone,
    ) -> Result<Checked<'a>, NativeError> {
        let pending_count = pending.len();
        if pending_count > self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        self.state
            .rows
            .validate_chain(pending.clone().map(|item| &item.fragments))?;
        let intent = intent::monitor_deadline_fingerprint(self.state.ledger, input)?;
        let view = View {
            state: &self.state,
            tail: pending.next_back(),
        };
        if let Some(outcome) = as_outcome(view.get(Key::Outcome(input.key().into()))) {
            return if outcome.intent == intent {
                Ok(Checked::Existing {
                    outcome,
                    committed: outcome.sequence <= self.native_sequence(),
                })
            } else {
                Err(NativeError::RequestConflict)
            };
        }
        if pending_count == self.limits.pending {
            return Err(NativeError::Capacity("pending candidates"));
        }
        let mut meta = view.meta();
        if logical_time < meta.logical_time {
            return Err(ContractError::InvalidCut.into());
        }
        meta.logical_time = logical_time;
        meta.outcomes = add(meta.outcomes, 1)?;
        within(meta.outcomes, self.limits.outcomes)?;
        let sequence = SessionSeq(
            view.prefix()
                .0
                .checked_add(1)
                .ok_or(NativeError::Capacity("sequence"))?,
        );
        let cut = ClaimCut {
            position: sequence,
            cause: intent,
        };
        let resolved = crate::native::monitor_deadlines::resolve(&view, input, logical_time)?;
        Ok(Checked::Fresh(Fresh {
            dispatch: Dispatch::MonitorDeadline {
                input,
                logical_time,
                resolved,
            },
            view,
            meta,
            sequence,
            cut,
            intent,
            operation: NativeOperation::MonitorDeadline,
            lane: BudgetLane::Completion,
            limits: self.limits,
        }))
    }
}
