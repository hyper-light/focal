//! Protect held graph promises before accepting any effective candidate prefix.
//! Every known native writer emits an owning-claim fact. Creation additionally
//! exposes each immutable dependency and lineage parent: those endpoints find
//! protected components even when their Claim rows themselves are unchanged.
use super::*;
use focal_model::Cause;

fn owner(fact: NativeFact) -> Option<ClaimId> {
    match fact {
        NativeFact::Claim(event) => Some(ClaimId(event.after.object.0)),
        NativeFact::Receipt { claim, .. }
        | NativeFact::ReceiptAdopted { claim, .. }
        | NativeFact::Registrations { claim } => Some(ClaimId(claim.object.0)),
        NativeFact::Work { claim, .. }
        | NativeFact::Diagnostic { claim, .. }
        | NativeFact::Response { claim, .. }
        | NativeFact::Definition { claim, .. } => Some(claim),
        NativeFact::Evaluation { key, .. } => Some(key.claim),
        NativeFact::Missing { key }
        | NativeFact::Delivery { key }
        | NativeFact::Accepted { key } => Some(key.evaluation.claim),
        // Native artifact writers always include a Work, Diagnostic or Accepted
        // fact. Immutable artifact identity alone changes no projection shape.
        NativeFact::Artifact { .. } => None,
        // Claimant audit closure does not alter response membership, graph
        // topology or the acceptance projection protected by graph grants.
        NativeFact::ResultTestament { .. } => None,
    }
}

impl CompletionBook {
    fn visit_graph_claim(
        &self,
        id: ClaimId,
        visit: &mut impl FnMut(&Grant) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        let mut remaining = mul(self.limits.plan_nodes, self.limits.evaluations_per_claim)?;
        for key in self.protections.affected(id) {
            remaining = remaining
                .checked_sub(1)
                .ok_or(NativeError::Capacity("protected graph evaluation cohort"))?;
            let grant = self.grant(key)?;
            if grant.credit.remaining_reports == 0 {
                continue;
            }
            if !grant.envelope.has_graph()
                || !grant
                    .members
                    .as_ref()
                    .is_some_and(|members| members.contains(id))
            {
                return Err(ContractError::InvalidManifest.into());
            }
            visit(grant)?;
        }
        self.check_health()
    }

    fn visit_affected_graph(
        &self,
        view: &View<'_>,
        prepared: &NativePrepared,
        mut visit: impl FnMut(&Grant) -> Result<(), NativeError>,
    ) -> Result<(), NativeError> {
        let mut previous = None;
        for ordinal in 0..prepared.outcome.events {
            let fact = Self::event(prepared, ordinal)?.fact;
            if let Some(id) = owner(fact)
                && previous != Some(id)
            {
                self.visit_graph_claim(id, &mut visit)?;
                previous = Some(id);
            }
            if let NativeFact::Claim(event) = fact
                && matches!(
                    event.kind,
                    NativeEventKind::Monitor(
                        NativeMonitorEvent::Registered { .. } | NativeMonitorEvent::Rebound { .. }
                    )
                )
            {
                let NativeEventKind::Monitor(change) = event.kind else {
                    return Err(ContractError::InvalidManifest.into());
                };
                let claim = view
                    .claim(ClaimId(event.after.object.0))
                    .ok_or(ContractError::InvalidTarget)?;
                let scope = claim
                    .scopes()
                    .monitor(change.id())
                    .ok_or(ContractError::InvalidTarget)?;
                within(scope.roots().len(), self.limits.plan_edges)?;
                for root in scope.roots() {
                    let id = match *root {
                        WaitPredicate::Satisfied(id)
                        | WaitPredicate::Terminal(id)
                        | WaitPredicate::Released(id) => id,
                    };
                    self.visit_graph_claim(id, &mut visit)?;
                }
            }
            if let NativeFact::Claim(event) = fact
                && event.before.is_none()
            {
                let created = view
                    .claim(ClaimId(event.after.object.0))
                    .ok_or(ContractError::InvalidTarget)?;
                within(created.graph().obligations().len(), self.limits.plan_edges)?;
                for obligation in created.graph().obligations() {
                    self.visit_graph_claim(obligation.target, &mut visit)?;
                }
                if let Cause::Claim(parent) = *created.lineage().cause() {
                    self.visit_graph_claim(parent, &mut visit)?;
                }
            }
        }
        Ok(())
    }

    /// Checks only directly indexed affected grants. Each graph member was
    /// captured from that grant's complete original closure before Begin, so a
    /// new incoming edge cannot hide behind an unchanged funded-parent row.
    /// No allocation or scan depends on unrelated ledger or grant occupancy.
    pub(in crate::native) fn check_graph_growth(
        &self,
        view: &View<'_>,
        prepared: &NativePrepared,
    ) -> Result<(), NativeError> {
        self.check_health()?;
        if self.totals.graphs == 0 {
            return Ok(());
        }
        // Reset the entire affected cohort before reading any mark. A refusal
        // may leave temporary marks, but the next call cannot reuse them. No
        // counter, pending journal or persistent mutation is needed for dedup.
        self.visit_affected_graph(view, prepared, |grant| {
            grant.growth_checked.set(false);
            Ok(())
        })?;
        self.visit_affected_graph(view, prepared, |grant| {
            if grant.growth_checked.replace(true) {
                return Ok(());
            }
            let bytes = grant.envelope.graph_check_bytes();
            let reservation =
                self.source()
                    .reserve(BudgetKind::Pending, BudgetLane::Completion, bytes)?;
            let mut scratch = prepare::Scratch {
                used: 0,
                max: bytes,
            };
            let members = grant
                .members
                .as_ref()
                .ok_or(ContractError::InvalidManifest)?;
            grant.envelope.check_graph_with_members(
                view,
                self.limits,
                members.ids(),
                &mut scratch,
            )?;
            drop(reservation);
            Ok(())
        })?;
        self.check_health()
    }
}
