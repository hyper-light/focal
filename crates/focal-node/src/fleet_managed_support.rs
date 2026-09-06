use super::*;

pub struct ManagedSupportReply {
    fact: ManagedFormatSupport,
    route: RouteEpoch,
    targets: [Option<u64>; 4],
    _charge: Allocation,
}
impl ManagedSupportReply {
    pub fn fact(&self) -> &ManagedFormatSupport {
        &self.fact
    }
    pub fn route_epoch(&self) -> RouteEpoch {
        self.route
    }
    pub fn targets(&self) -> impl Iterator<Item = u64> + '_ {
        self.targets.iter().flatten().copied()
    }
}
pub(super) struct SupportCall {
    received: Option<(u64, ManagedFormatSupport)>,
    response: oneshot::Sender<Result<ManagedSupportReply, LedgerError>>,
}
impl ReplicaHost {
    /// Trusted in-process composition only. The wire probe itself is Node-only;
    /// recording a received fact requires the transport-verified peer node ID.
    pub async fn managed_support(&self) -> Result<ManagedSupportReply, LedgerError> {
        self.managed_support_call(None).await
    }
    pub async fn record_managed_support(
        &self,
        peer_node: u64,
        fact: ManagedFormatSupport,
    ) -> Result<ManagedSupportReply, LedgerError> {
        if fact.node != peer_node || peer_node == 0 {
            return Err(LedgerError::Capacity);
        }
        self.managed_support_call(Some((peer_node, fact))).await
    }
    async fn managed_support_call(
        &self,
        received: Option<(u64, ManagedFormatSupport)>,
    ) -> Result<ManagedSupportReply, LedgerError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(LedgerError::Failed);
        }
        let charge = self
            .budget
            .reserve(BudgetKind::Control, BudgetLane::Completion, 192 * 1024)?
            .commit();
        if let Some((_, fact)) = &received {
            let total = fact
                .voters
                .len()
                .checked_add(fact.voters_outgoing.len())
                .and_then(|n| n.checked_add(fact.learners.len()))
                .and_then(|n| n.checked_add(fact.learners_next.len()))
                .ok_or(LedgerError::Capacity)?;
            if total > 4096 {
                return Err(LedgerError::Capacity);
            }
        }
        let (send, receive) = oneshot::channel();
        self.sender
            .try_send(Work::ManagedSupport(
                Box::new(SupportCall {
                    received,
                    response: send,
                }),
                charge,
            ))
            .map_err(|error| match error {
                HostQueueError::Full => LedgerError::Capacity,
                HostQueueError::Disconnected => LedgerError::Failed,
            })?;
        // The normal bounded owner queue has no wire/actor side authority. If
        // the waiter is dropped, the queued fact and permit remain owner-owned.
        receive.await.map_err(|_| LedgerError::OutcomeUnknown)?
    }
}
impl Owner {
    pub(super) fn accept_managed_support(&mut self, call: SupportCall, charge: Allocation) {
        let SupportCall { received, response } = call;
        let result = (|| {
            if !self.session.managed_support_demanded() {
                return Err(LedgerError::Managed(
                    focal_ledger::ManagedError::Unsupported,
                ));
            }
            if let Some((node, fact)) = received {
                self.session.record_managed_support(node, fact)?;
            }
            self.session.managed_support()
        })()
        .map(|fact| {
            let candidate = self
                .memberships
                .iter()
                .filter(|pending| !pending.proposed && !pending.call.response.is_closed())
                .find_map(|pending| {
                    pending
                        .call
                        .request
                        .as_ref()
                        .and_then(|request| match request.change {
                            focal_consensus::MembershipChange::AddLearner { node }
                            | focal_consensus::MembershipChange::Promote { node } => Some(node),
                            _ => None,
                        })
                });
            let mut targets = [None; 4];
            if let Some(candidate) =
                candidate.filter(|node| self.session.needs_managed_support(*node))
                && let Some(slot) = targets.first_mut()
            {
                *slot = Some(candidate);
            }
            for slot in 0..4 {
                if targets.get(slot).is_some_and(Option::is_some) {
                    continue;
                }
                let eligible = || {
                    fact.voters
                        .iter()
                        .chain(&fact.voters_outgoing)
                        .copied()
                        .filter(|node| {
                            !self.session.managed_protocol_active()
                                && self.session.needs_managed_support(*node)
                                && !targets.contains(&Some(*node))
                        })
                };
                let target = eligible()
                    .filter(|node| *node > self.support_cursor)
                    .min()
                    .or_else(|| eligible().min());
                let Some(node) = target else { break };
                self.support_cursor = node;
                if let Some(slot) = targets.get_mut(slot) {
                    *slot = Some(node);
                }
            }
            ManagedSupportReply {
                fact,
                route: self.config.route_epoch,
                targets,
                _charge: charge,
            }
        });
        let _ = response.send(result);
    }
}

pub(super) struct DeferredManaged {
    work: Work,
    deadline: Instant,
}
impl Owner {
    /// Return false while this exact owned input is waiting for the local floor
    /// or all current voter promises. No application proposal exists yet.
    pub(super) fn managed_gate(&mut self, work: &Work) -> Result<bool, LedgerError> {
        if let Work::Request(request, ..) = work
            && matches!(
                request.verified.request().operation,
                Operation::Managed { .. }
            )
        {
            let (key, family, intent) = match managed_request_identity(request.verified.request()) {
                Ok(identity) => identity,
                Err(_) => return Ok(true),
            };
            match self.session.managed_receipt(&key, intent, family) {
                Ok(Some(_)) => return Ok(true),
                Err(LedgerError::Managed(
                    focal_ledger::ManagedError::InvalidIdentity
                    | focal_ledger::ManagedError::NotRegistered
                    | focal_ledger::ManagedError::Conflict
                    | focal_ledger::ManagedError::Closed { .. }
                    | focal_ledger::ManagedError::Retired { .. },
                )) => return Ok(true),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
        if let Work::Request(request, ..) = work
            && let Operation::RequestStreamControl { cluster, command } =
                &request.verified.request().operation
        {
            match self.session.request_stream_receipt_parts(
                *cluster,
                request.verified.peer().principal(),
                request.verified.request().request_id,
                command,
            ) {
                Ok(Some(_)) => return Ok(true),
                Err(LedgerError::Managed(focal_ledger::ManagedError::Conflict)) => return Ok(true),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
        let all = match work {
            Work::Request(request, ..) => match &request.verified.request().operation {
                Operation::Managed { .. } | Operation::RequestStreamControl { .. } => {
                    let cluster = match &request.verified.request().operation {
                        Operation::Managed { key, .. } => key.stream.cluster,
                        Operation::RequestStreamControl { cluster, .. } => *cluster,
                        _ => [0; 16],
                    };
                    if cluster != self.session.cluster_id()
                        || request.verified.request().ledger != self.session.ledger()
                        || !self.serves_route(request.verified.request().route_epoch)
                        || !self.session.is_authoritative()
                    {
                        return Ok(true);
                    }
                    true
                }
                Operation::ManagedSupport { group } => {
                    let PeerRole::Node { node_id } = request.verified.peer().role() else {
                        return Ok(true);
                    };
                    let status = self.session.status();
                    if request.verified.request().ledger != self.session.ledger()
                        || *group != self.session.group_id()
                        || request.verified.request().route_epoch != self.config.route_epoch
                        || !status.voters.contains(&node_id) && !status.learners.contains(&node_id)
                    {
                        return Ok(true);
                    }
                    false
                }
                _ => return Ok(true),
            },
            Work::ManagedSupport(..) if self.session.managed_support_demanded() => false,
            _ => return Ok(true),
        };
        match self.session.begin_managed_support() {
            Ok(()) => {}
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::PersistencePending)) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
        let fact = match self.session.managed_support() {
            Ok(fact) => fact,
            Err(LedgerError::Consensus(focal_consensus::ConsensusError::PersistencePending)) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        Ok(!all
            || self.session.managed_protocol_active()
            || !fact
                .voters
                .iter()
                .chain(&fact.voters_outgoing)
                .any(|node| self.session.needs_managed_support(*node)))
    }
    pub(super) fn defer_managed(&mut self, work: Work) -> Result<(), LedgerError> {
        if self
            .deferred_managed
            .len()
            .saturating_add(self.pending.len())
            >= self.config.pending_clients
        {
            reject_managed_work(work, LedgerError::Capacity);
            return Ok(());
        }
        let Some(deadline) = Instant::now().checked_add(self.config.request_timeout) else {
            reject_managed_work(work, LedgerError::Capacity);
            return Ok(());
        };
        if self.deferred_managed.len() == self.deferred_managed.capacity() {
            let target = self
                .deferred_managed
                .len()
                .checked_add(1)
                .and_then(|n| n.max(8).checked_next_power_of_two());
            let backing = target
                .and_then(|target| target.checked_mul(size_of::<DeferredManaged>()))
                .and_then(|bytes| {
                    self.budget
                        .reserve(BudgetKind::Pending, grouped::lane(&work), bytes)
                        .ok()
                });
            let (Some(target), Some(backing)) = (target, backing) else {
                reject_managed_work(work, LedgerError::Capacity);
                return Ok(());
            };
            if self
                .deferred_managed
                .try_reserve_exact(target.saturating_sub(self.deferred_managed.len()))
                .is_err()
            {
                reject_managed_work(work, LedgerError::Capacity);
                return Ok(());
            }
            self.deferred_backing = Some(backing.commit());
        }
        self.deferred_managed
            .push_back(DeferredManaged { work, deadline });
        if !self.nonblocking {
            self.drain()?;
        }
        Ok(())
    }
    pub(super) fn progress_managed(&mut self) -> Result<(), LedgerError> {
        let count = self.deferred_managed.len();
        for _ in 0..count {
            let Some(pending) = self.deferred_managed.pop_front() else {
                break;
            };
            let cancelled = match &pending.work {
                Work::Request(_, response, _) => response.is_closed(),
                Work::ManagedSupport(call, _) => call.response.is_closed(),
                _ => false,
            };
            if cancelled || self.stopping.is_some() || Instant::now() >= pending.deadline {
                reject_managed_work(pending.work, LedgerError::OutcomeUnknown);
                continue;
            }
            if self.session.persistence_pending() {
                self.deferred_managed.push_back(pending);
                continue;
            }
            match self.managed_gate(&pending.work) {
                Ok(true) => {
                    let before = self.pending.len();
                    self.accept(pending.work)?;
                    if self.pending.len() > before
                        && let Some(accepted) = self.pending.back_mut()
                    {
                        accepted.deadline = accepted.deadline.min(pending.deadline);
                    }
                }
                Ok(false) => self.deferred_managed.push_back(pending),
                Err(error) => reject_managed_work(pending.work, error),
            }
        }
        if self.deferred_managed.is_empty() {
            self.deferred_managed = VecDeque::new();
            self.deferred_backing = None;
        }
        Ok(())
    }
    pub(super) fn close_managed(&mut self) {
        while let Some(pending) = self.deferred_managed.pop_front() {
            reject_managed_work(pending.work, LedgerError::OutcomeUnknown);
        }
        self.deferred_managed = VecDeque::new();
        self.deferred_backing = None;
    }
}
pub(super) fn reject_managed_work(work: Work, error: LedgerError) {
    match work {
        Work::Request(request, response, charge) => {
            let header = request
                .verified
                .request()
                .reply(Response::Error(access(error)));
            drop(request);
            let _ = response.send(finish_response(header, charge));
        }
        Work::ManagedSupport(call, charge) => {
            let SupportCall { received, response } = *call;
            drop(received);
            let _ = response.send(Err(error));
            drop(charge);
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "fleet_managed_tests.rs"]
mod tests;
