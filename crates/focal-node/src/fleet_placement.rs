//! Trusted placement intents cross the same owner queue and durability fence as
//! domain work. Only durable apply followed by ReadIndex releases a witness.
use super::*;
pub use focal_ledger::{CommittedPlacement, SessionPlacementRequest};

pub struct PlacementReply {
    witness: CommittedPlacement,
    _charge: Allocation,
}
impl PlacementReply {
    pub fn witness(&self) -> &CommittedPlacement {
        &self.witness
    }
    /// The ledger witness has its own allocation permit; moving it into the
    /// control authority owner preserves that permit after this reply is gone.
    pub fn into_witness(self) -> CommittedPlacement {
        self.witness
    }
}
pub(super) struct PlacementCall {
    request: SessionPlacementRequest,
    response: oneshot::Sender<Result<PlacementReply, LedgerError>>,
    /// Answer only from the committed record this replica applied; never
    /// propose. Any replica, leader or follower, can witness.
    witness_only: bool,
}
pub(super) struct PendingPlacementCall {
    call: PlacementCall,
    context: Option<Vec<u8>>,
    term: u64,
    deadline: Instant,
    charge: Allocation,
}
impl PendingPlacementCall {
    pub(super) fn finish(self, result: Result<CommittedPlacement, LedgerError>) {
        let Self {
            call,
            context,
            charge,
            ..
        } = self;
        let PlacementCall {
            request, response, ..
        } = call;
        drop(request);
        drop(context);
        let _ = response.send(result.map(|witness| PlacementReply {
            witness,
            _charge: charge,
        }));
    }
}
impl ReplicaHost {
    /// Trusted in-process placement authority only. Node/Actor wire credentials
    /// do not expose this operation. Preserve the exact request on an unknown
    /// outcome; only the session's retained placement receipts are retryable.
    pub async fn propose_placement(
        &self,
        request: SessionPlacementRequest,
    ) -> Result<PlacementReply, LedgerError> {
        self.placement_call(request, false).await
    }
    /// The committed record answering `request` on this replica, whether it
    /// leads or follows; `NotReady` while the record is not yet applied here.
    pub async fn placement_witness(
        &self,
        request: SessionPlacementRequest,
    ) -> Result<PlacementReply, LedgerError> {
        self.placement_call(request, true).await
    }
    async fn placement_call(
        &self,
        request: SessionPlacementRequest,
        witness_only: bool,
    ) -> Result<PlacementReply, LedgerError> {
        request.validate()?;
        let encoded = postcard::experimental::serialized_size(&request)?;
        if encoded > 64 * 1024 {
            return Err(LedgerError::Capacity);
        }
        let bytes = encoded
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(192 * 1024))
            .ok_or(LedgerError::Capacity)?;
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, bytes)?
            .commit();
        let (response, receive) = oneshot::channel();
        self.sender
            .try_send(Work::Placement(
                Box::new(PlacementCall {
                    request,
                    response,
                    witness_only,
                }),
                charge,
            ))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
}
impl Owner {
    pub(super) fn accept_placement(&mut self, call: PlacementCall, charge: Allocation) {
        if call.witness_only {
            let result = match self.session.placement_witness(&call.request) {
                Ok(Some(witness)) => Ok(PlacementReply {
                    witness,
                    _charge: charge,
                }),
                Ok(None) => Err(LedgerError::NotReady {
                    leader: self.session.status().leader_id,
                }),
                Err(error) => Err(error),
            };
            drop(call.request);
            let _ = call.response.send(result);
            return;
        }
        let result = (|| {
            if self.placement.is_some() || self.stopping.is_some() {
                return Err(LedgerError::Capacity);
            }
            let deadline = Instant::now()
                .checked_add(self.config.request_timeout)
                .ok_or(LedgerError::Capacity)?;
            self.session.propose_placement(&call.request)?;
            Ok(deadline)
        })();
        match result {
            Ok(deadline) => {
                self.placement = Some(PendingPlacementCall {
                    call,
                    context: None,
                    term: self.session.status().term,
                    deadline,
                    charge,
                })
            }
            Err(error) => {
                drop(call.request);
                let _ = call.response.send(Err(error));
                drop(charge);
            }
        }
    }
    pub(super) fn expire_placement(&mut self) {
        if self.placement.as_ref().is_some_and(|pending| {
            pending.call.response.is_closed() || Instant::now() >= pending.deadline
        }) && let Some(pending) = self.placement.take()
        {
            pending.finish(Err(LedgerError::OutcomeUnknown));
        }
    }
    pub(super) fn resolve_placement(&mut self, events: &SessionEvents) -> Result<(), LedgerError> {
        let Some(mut pending) = self.placement.take() else {
            return Ok(());
        };
        let status = self.session.status();
        if pending.call.response.is_closed()
            || pending.term != status.term
            || status.role != StateRole::Leader
            || Instant::now() >= pending.deadline
        {
            pending.finish(Err(LedgerError::OutcomeUnknown));
            return Ok(());
        }
        let ready = match self.session.placement_receipt(&pending.call.request) {
            Ok(receipt) => receipt.is_some(),
            Err(error) => {
                pending.finish(Err(error));
                return Ok(());
            }
        };
        if let Some(context) = &pending.context {
            if events
                .read_barriers
                .iter()
                .any(|(observed, _)| observed == context)
            {
                let result = if ready {
                    self.session
                        .placement_witness(&pending.call.request)
                        .and_then(|witness| witness.ok_or(LedgerError::PlacementConflict))
                } else {
                    Err(LedgerError::PlacementConflict)
                };
                pending.finish(result);
                return Ok(());
            }
        } else if ready {
            self.nonce = self.nonce.checked_add(1).ok_or(LedgerError::Capacity)?;
            let mut context = b"focal.placement.read.v1\0".to_vec();
            context.extend_from_slice(&self.nonce.to_be_bytes());
            match self.session.read_index(context.clone()) {
                Ok(()) => pending.context = Some(context),
                Err(
                    LedgerError::Capacity
                    | LedgerError::Consensus(focal_consensus::ConsensusError::Capacity),
                ) => {}
                Err(error) => {
                    pending.finish(Err(error));
                    return Ok(());
                }
            }
        }
        self.placement = Some(pending);
        Ok(())
    }
}
