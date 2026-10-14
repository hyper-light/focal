//! Trusted monitor timers have a disjoint exact identity. Resolve only after
//! checking the actual pending prefix; expensive graph work awaits reservation.
use super::*;

impl Core<NativeState> {
    pub(in crate::native) fn check_monitor_deadline_chain<'a, 'p: 'a>(
        &'a self,
        input: NativeMonitorDeadlineInput,
        logical_time: u64,
        pending: Option<pending_index::Chain<'p>>,
    ) -> Result<Checked<'a>, NativeError> {
        self.state.rows.validate_chain(
            pending
                .and_then(|chain| chain.head())
                .map(|item| &item.fragments)
                .into_iter(),
        )?;
        let intent = intent::monitor_deadline_fingerprint(self.state.ledger, input)?;
        let view = View::over(&self.state, pending);
        if let Some(outcome) = view.outcome(input.key().into())? {
            return if outcome.intent == intent {
                Ok(Checked::Existing {
                    outcome,
                    committed: outcome.sequence <= self.native_sequence(),
                })
            } else {
                Err(NativeError::RequestConflict)
            };
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
