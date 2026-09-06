//! One bounded evidence export on the existing session owner. Only the shared
//! WAL thread waits on fsync; the grouped session worker polls exact receipts.
use super::*;
use focal_ledger::DurableEvidenceSnapshot;
use futures_util::FutureExt;

pub(super) struct EvidenceCall {
    ttl: u64,
    deadline: Instant,
    response: oneshot::Sender<Result<DurableEvidenceSnapshot, LedgerError>>,
}
pub(super) struct PendingEvidenceCall {
    call: EvidenceCall,
    started: bool,
    charge: Allocation,
}
impl PendingEvidenceCall {
    pub(super) fn finish(self, result: Result<DurableEvidenceSnapshot, LedgerError>) {
        let Self { call, charge, .. } = self;
        // The snapshot carries independent owned checkpoint/placement permits.
        drop(charge);
        let _ = call.response.send(result);
    }
}
impl ReplicaHost {
    /// Trusted local export, absent from the wire API. The owner chooses its
    /// monotone capture clock; the caller supplies only a bounded lease duration.
    /// A timeout cancels interest, never an already admitted checkpoint rewrite.
    pub async fn checkpoint_evidence(
        &self,
        ttl: Duration,
    ) -> Result<DurableEvidenceSnapshot, LedgerError> {
        if ttl.is_zero() || ttl > Duration::from_secs(30) {
            return Err(LedgerError::Capacity);
        }
        tokio::runtime::Handle::try_current().map_err(|_| LedgerError::Failed)?;
        std::panic::catch_unwind(|| drop(tokio::time::sleep(Duration::ZERO)))
            .map_err(|_| LedgerError::Failed)?;
        let ttl = u64::try_from(ttl.as_millis()).map_err(|_| LedgerError::Capacity)?;
        if ttl == 0 {
            return Err(LedgerError::Capacity);
        }
        let deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or(LedgerError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 4096)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Evidence(
                Box::new(EvidenceCall {
                    ttl,
                    deadline,
                    response,
                }),
                charge,
            ))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(self.request_timeout, receive).await
        })
        .catch_unwind()
        .await
        .map_err(|_| LedgerError::OutcomeUnknown)?
        .map_err(|_| LedgerError::OutcomeUnknown)?
        .map_err(|_| LedgerError::OutcomeUnknown)?
    }
}
impl Owner {
    pub(super) fn accept_evidence(
        &mut self,
        call: EvidenceCall,
        charge: Allocation,
    ) -> Result<(), LedgerError> {
        if self.evidence.is_some() || self.stopping.is_some() || call.response.is_closed() {
            PendingEvidenceCall {
                call,
                started: false,
                charge,
            }
            .finish(Err(LedgerError::Capacity));
            return Ok(());
        }
        if !self.nonblocking {
            // The compatibility owner serves only this one session. The shared
            // worker always takes the staged path below, including checkpoint IO.
            let now = self
                .views
                .advance(&mut self.session)
                .map_err(|_| LedgerError::Failed)?;
            let result = self.session.checkpoint_evidence(now, call.ttl);
            PendingEvidenceCall {
                call,
                started: true,
                charge,
            }
            .finish(result);
            return Ok(());
        }
        self.evidence = Some(PendingEvidenceCall {
            call,
            started: false,
            charge,
        });
        self.progress_evidence()
    }
    pub(super) fn progress_evidence(&mut self) -> Result<(), LedgerError> {
        let Some(mut pending) = self.evidence.take() else {
            return Ok(());
        };
        if self.stopping.is_some()
            || pending.call.response.is_closed()
            || Instant::now() >= pending.call.deadline
        {
            self.session.cancel_checkpoint_evidence()?;
            pending.finish(Err(LedgerError::OutcomeUnknown));
            return Ok(());
        }
        #[cfg(test)]
        let prepared_now = !pending.started;
        if !pending.started {
            if self.session.persistence_pending()
                || self.session.has_ready()
                || self.session.pending_count() != 0
            {
                self.evidence = Some(pending);
                return Ok(());
            }
            let now = self
                .views
                .advance(&mut self.session)
                .map_err(|_| LedgerError::Failed)?;
            if let Err(error) = self
                .session
                .begin_checkpoint_evidence(now, pending.call.ttl)
            {
                pending.finish(Err(error));
                return Ok(());
            }
            pending.started = true;
        }
        match self.session.try_finish_checkpoint_evidence() {
            Ok(None) => {
                #[cfg(test)]
                if prepared_now {
                    self.observe_checkpoint_pending();
                }
                self.evidence = Some(pending);
            }
            Ok(Some(snapshot)) => pending.finish(Ok(snapshot)),
            Err(error) => {
                let failed = matches!(error, LedgerError::Consensus(_));
                self.session.cancel_checkpoint_evidence()?;
                pending.finish(Err(error));
                if failed {
                    return Err(LedgerError::Failed);
                }
            }
        }
        Ok(())
    }
}
