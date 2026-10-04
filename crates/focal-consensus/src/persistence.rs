//! One outstanding durable Ready per group. The pinned RawNode::ready contract
//! forbids mutation before advance; other groups remain independently runnable.
//!
//! What a `Ready` gives to persist — a term, a vote, entries, a snapshot,
//! what the member approved by itself — is durable before the member
//! answers for it. The commit index is not among them: it is the group's
//! volatile state (Ongaro's thesis, figure 3.1), re-derived after a restart
//! from the leader or from the first entry a new leader commits, so a
//! `Ready` that moves nothing else is not waited for, and a commit that moves
//! once a write is durable waits for no write of its own. It is kept with
//! the stored hard state and written with the group's next record; a
//! member that alone decides writes it with the very entries it commits;
//! and one no record has carried for a whole period of its owner — the
//! group has gone quiet — is written then, and when the member is let go,
//! one such write in flight at a time and no one waiting for it. A group
//! that keeps writing never writes a commit for itself: a write made the
//! moment the commit moved would hold the disk the next entry needs. So a
//! member that stops finds in its log what it applied, but for what a cut
//! within its owner's period took; that tail it is told of again, by its
//! group.
//!
//! One kind of entry is not applied on a commit the log does not hold: a
//! change of membership. Whoever is told that a change committed may act on
//! it where no log records it — stop the member it removed — and a member
//! that then restarted without the commit would count that member again,
//! and wait for it for good: two voters of which one was removed and
//! stopped leave one that cannot elect itself. So the commit that covers a
//! change is written, and waited for, before the change is applied.
//!
//! And one kind of group applies nothing on such a commit
//! (`apply_on_written_commit`): the group whose state its members act on
//! when they next start, before the group has told them anything — who is
//! enrolled and who was revoked, the level below which a binary does not
//! serve, where a ledger is placed. A member of it that stopped within its
//! owner's period would start again without what it had applied, and act
//! against it: serve below a fence it had honoured, admit a peer it had
//! refused. What a ledger applied is served only through its group — a read
//! by a barrier, a write by a leader that committed in its term — so a
//! ledger's member that starts behind what it applied says nothing its
//! group has not told it again.
use super::*;
use focal_log::WalAppend;
use hyper_raft::{LightReady, Ready, proto};
use storage::PreparedUpdate;

pub(super) struct PendingDrain {
    events: NodeEvents,
    delivered: u64,
    phase: Phase,
}
enum Phase {
    Start,
    Ready(Box<ReadyPhase>),
    Light(Box<LightPhase>),
}
/// A commit that covers what is not applied before the log holds it (a
/// change of membership; anything, in a group that applies on a written
/// commit), being written before what it committed is applied.
struct LightPhase {
    light: LightReady,
    hard_state: HardState,
    record: Option<Record>,
    receipt: Option<WalAppend>,
    outcome: Option<Result<(), focal_log::LogError>>,
}
// Only an outstanding Ready allocates this owned slot. Its size and payload
// copies are covered by the pre-reserved staging permit, keeping idle nodes small.
struct ReadyPhase {
    ready: Ready,
    prepared: PreparedUpdate,
    records: Vec<Record>,
    receipt: Option<WalAppend>,
    /// What the receipt answered, when an owner waited for it
    /// (`wait_persisted`) before the drain that takes it.
    outcome: Option<Result<(), focal_log::LogError>>,
    /// The hard state this write states, when it states one: the `Ready`'s
    /// own, or the stored one with a commit the log has yet to hold.
    hard_state: Option<HardState>,
    /// The commit a member that alone decides wrote with its entries: what
    /// the core says once they are durable.
    sole_commit: Option<u64>,
    /// The entries the `Ready` gave to apply, when the log does not hold
    /// their commit and they wait for it (`fenced`): applied once this
    /// write, which states it, is durable.
    held: Vec<Entry>,
}
impl DurableNode {
    pub fn shared_wal(&self) -> SharedWal {
        self.wal.shared_wal()
    }
    /// True from Ready acquisition until its full output prefix is released.
    /// Mutations return PersistencePending in this state; no Raft input is lost.
    pub fn persistence_pending(&self) -> bool {
        self.persistence.is_some() || self.checkpoint.is_some() || self.decoder_write.is_some()
    }
    /// Read-only owner wake predicate, including a retained WAL receipt and
    /// newly queued Ready work that has not yet started persistence.
    pub fn has_ready(&self) -> bool {
        self.persistence.is_some()
            || self.checkpoint.is_some()
            || self.decoder_write.is_some()
            || self.raw.has_ready()
            || self.recovered_events.is_some()
            || self.recovered_snapshot.is_some()
    }

    /// Queue/poll one group's durability without waiting for the physical writer.
    /// None retains the exact Ready, preparation, output and accounting permit.
    /// The caller can poll other groups on the same owner before trying again,
    /// and sends what may be sent meanwhile (`sendable`).
    pub fn try_drain(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        self.poll_drain(false)
    }

    /// Synchronous compatibility path over the same persistence state machine.
    /// Waits on each exact receipt without a timer, spin loop, or runtime bridge.
    /// An owner that sends for a leader does not wait here: it polls
    /// (`try_drain`), sends what may be sent (`sendable`) and waits for the
    /// write (`wait_persisted`).
    pub fn drain(&mut self) -> Result<NodeEvents, ConsensusError> {
        self.poll_drain(true)?
            .ok_or(ConsensusError::PersistencePending)
    }

    /// The messages gathered so far that may be sent while this group's
    /// write is in flight: a leader's, which its members persist for
    /// themselves (Ongaro's thesis §10.2.1) — its own write and theirs then
    /// overlap, and the entry commits when the later of them is durable —
    /// and those of a write this drain already saw durable. None of them
    /// answers for the write in flight: a follower's acknowledgement and a
    /// vote are given once what they answer for is durable, by the drain
    /// that follows. Nothing else is said early: the events of a drain are
    /// given whole, with nothing still to persist, as its owners take them.
    /// A snapshot is left for that drain too: its owner answers for what
    /// became of it (`report_snapshot`), which the member takes only once
    /// its write is done.
    pub fn sendable(&mut self) -> Result<Option<NodeEvents>, ConsensusError> {
        self.check()?;
        let Some(pending) = self.persistence.as_mut() else {
            return Ok(None);
        };
        let snapshot = MessageType::MsgSnapshot;
        let count = pending
            .events
            .messages
            .iter()
            .filter(|message| message.msg_type != snapshot)
            .count();
        if count == 0 {
            return Ok(None);
        }
        let mut messages = Vec::new();
        messages
            .try_reserve_exact(count)
            .map_err(|_| ConsensusError::Capacity)?;
        messages.extend(
            pending
                .events
                .messages
                .extract_if(.., |message| message.msg_type != snapshot),
        );
        let mut early = NodeEvents {
            messages,
            applied_index: self.delivered_index,
            ..Default::default()
        };
        let charge = memory::events_bytes(&early).and_then(|bytes| {
            self.active_allocation
                .as_mut()
                .ok_or(ConsensusError::Failed)?
                .split_off(bytes)
                .map_err(|_| ConsensusError::Capacity)
        });
        match charge {
            Ok(charge) => {
                early.allocation = Some(charge);
                Ok(Some(early))
            }
            Err(error) => {
                // Nothing left: the messages wait for the drain that gives
                // everything.
                if let Some(pending) = self.persistence.as_mut() {
                    pending.events.messages.append(&mut early.messages);
                }
                Err(error)
            }
        }
    }

    /// What this group applies, its members act on when they next start,
    /// before the group has told them anything: so nothing is applied, and
    /// nothing said to have committed, on a commit the log does not hold
    /// (the module's header). A member that stopped then knows, opened
    /// again, all it had acted on. Set once, by the group's owner, before
    /// it drains: what a member replays at opening its log holds already.
    pub fn apply_on_written_commit(&mut self) {
        self.written_commit = true;
    }

    /// An owner of many groups is told when a write of this one is
    /// answered, instead of asking at intervals: `signal` makes, for each
    /// write, what the log's writer calls then, on its own thread — a wake
    /// for the owner, which drains the group after. The wake says a drain
    /// has something to take, nothing more; a write answered without it
    /// (none was set, the wake was lost) is found by the owner's next poll.
    pub fn notify_persisted(&mut self, signal: Option<PersistedSignal>) {
        self.persisted = signal;
    }

    /// Whether the log will tell this group's owner when what the group
    /// waits for is answered: a signal is set (`notify_persisted`), and
    /// every write the group waits for — a `Ready`'s, a commit's, a
    /// checkpoint's, a decoder floor's — was taken by the log, which calls
    /// the signal as it answers. An owner so told need not ask at
    /// intervals: its own period is the bound on a signal that was lost. A
    /// write the log had no room for tells no one, and its owner asks
    /// again.
    pub fn wakes_owner(&self) -> bool {
        if self.persisted.is_none() || !self.persistence_pending() {
            return false;
        }
        let drain = match self.persistence.as_ref().map(|pending| &pending.phase) {
            None => true,
            Some(Phase::Ready(work)) => work.receipt.is_some(),
            Some(Phase::Light(work)) => work.receipt.is_some(),
            Some(Phase::Start) => false,
        };
        drain
            && self
                .checkpoint
                .as_ref()
                .is_none_or(|pending| pending.taken())
            && self
                .decoder_write
                .as_ref()
                .is_none_or(|pending| pending.taken())
    }

    /// Waits for the write this group has in flight, when it has one: an
    /// owner on its own thread, with nothing else to do for the group,
    /// waits here and drains after. False when there is no write to wait
    /// for — none is out, or the log had no room to take it and is asked
    /// again by the next drain.
    pub fn wait_persisted(&mut self) -> Result<bool, ConsensusError> {
        self.check()?;
        let (outcome, receipt) = match self.persistence.as_mut().map(|pending| &mut pending.phase) {
            Some(Phase::Ready(work)) => (&mut work.outcome, work.receipt.as_mut()),
            Some(Phase::Light(work)) => (&mut work.outcome, work.receipt.as_mut()),
            _ => return Ok(false),
        };
        if outcome.is_some() {
            return Ok(true);
        }
        let Some(ticket) = receipt else {
            return Ok(false);
        };
        *outcome = Some(ticket.wait_blocking().map(|_| ()));
        Ok(true)
    }

    fn poll_drain(&mut self, blocking: bool) -> Result<Option<NodeEvents>, ConsensusError> {
        self.check()?;
        if self.decoder_write.is_some() && !self.poll_decoder_floor(blocking)? {
            return Ok(None);
        }
        if self.checkpoint.is_some() {
            return Err(ConsensusError::PersistencePending);
        }
        if self.persistence.is_none() {
            // The members the committed changes about to be applied add,
            // whose progress this drain makes (`Tracker::apply`).
            let delivered = self.delivered_index;
            let tracker = self.raw.raft.tracker();
            let joining = self
                .raw
                .raft
                .log()
                .unstable()
                .entries()
                .iter()
                .rev()
                .take_while(|entry| entry.index > delivered)
                .chain(
                    self.raw
                        .store()
                        .entries
                        .iter()
                        .rev()
                        .take_while(|entry| entry.index > delivered),
                )
                .filter(|entry| proto::changes_configuration(entry))
                .fold(0usize, |added, entry| {
                    added.saturating_add(memory::members_added(entry, tracker))
                })
                .min(hyper_raft::MAX_MEMBERS);
            let bytes = memory::staging_bytes(&self.raw, &self.config, 0, joining)?;
            // Nothing has been taken from Raft yet: a refused staging reservation
            // leaves the replica exactly as it was, so the caller retries once
            // memory returns instead of losing the node to a transient shortage.
            self.active_allocation = Some(memory::reserve(
                &self.budget,
                BudgetKind::Pending,
                BudgetLane::Completion,
                bytes,
            )?);
            let events = self.recovered_events.take().unwrap_or_else(|| NodeEvents {
                snapshot: self.recovered_snapshot.take(),
                allocation: self.recovered_allocation.take(),
                ..Default::default()
            });
            self.persistence = Some(PendingDrain {
                events,
                delivered: self.delivered_index,
                phase: Phase::Start,
            });
        }
        let result = catch_unwind(AssertUnwindSafe(|| self.drain_progress(blocking)));
        match result {
            Ok(Ok(events)) => Ok(events),
            Ok(Err(error)) => {
                self.failed = true;
                Err(error)
            }
            Err(_) => {
                self.failed = true;
                Err(ConsensusError::DependencyFailure)
            }
        }
    }

    /// The commit the entries of `ready` reach once they are durable, when
    /// this member alone decides: it leads, it is the one voter of a
    /// configuration that is not joint, and the last of the entries is of
    /// its term. No other member's answer is waited for, so the commit is
    /// true exactly when the write that holds the entries is durable, and
    /// the same write states it. Asked once the entries the `Ready` gave to
    /// apply are applied: a change among them is in force when the core
    /// counts what is durable.
    fn sole_commit(&self, ready: &Ready) -> Option<u64> {
        let raft = &self.raw.raft;
        let last = ready.entries().last()?;
        (ready.snapshot().is_none()
            && raft.state() == StateRole::Leader
            && raft.tracker().is_singleton()
            && raft.tracker().configuration().votes(raft.id())
            && last.term == raft.term())
        .then_some(last.index)
    }
    /// The events gathered so far leave under the one charge they carry,
    /// and say how far they deliver.
    fn hand_over(
        &mut self,
        events: &mut NodeEvents,
        delivered: u64,
    ) -> Result<NodeEvents, ConsensusError> {
        events.applied_index = delivered;
        let bytes = memory::events_bytes(events)?;
        // What the events carry already — recovered at opening and charged
        // then, or delivered by the drain that built them — is not charged
        // again: the staging pays for what this drain added, and the one
        // charge the events leave with is exact.
        let charge = match events.allocation.take() {
            Some(mut carried) if carried.bytes() >= bytes => {
                carried
                    .shrink_to(bytes)
                    .map_err(|_| ConsensusError::Capacity)?;
                carried
            }
            carried => {
                let have = carried.as_ref().map_or(0, Allocation::bytes);
                let mut grown = self
                    .active_allocation
                    .as_mut()
                    .ok_or(ConsensusError::Failed)?
                    .split_off(bytes.saturating_sub(have))
                    .map_err(|_| ConsensusError::Capacity)?;
                if let Some(mut carried) = carried {
                    grown
                        .absorb(&mut carried)
                        .map_err(|_| ConsensusError::Failed)?;
                }
                grown
            }
        };
        events.allocation = Some(charge);
        self.delivered_index = delivered;
        Ok(std::mem::take(events))
    }
    fn drain_progress(&mut self, blocking: bool) -> Result<Option<NodeEvents>, ConsensusError> {
        let mut pending = self.persistence.take().ok_or(ConsensusError::Failed)?;
        loop {
            match pending.phase {
                Phase::Start => {
                    if !self.raw.has_ready() {
                        let events = self.hand_over(&mut pending.events, pending.delivered)?;
                        let retained = memory::raw_bytes(&self.raw)?;
                        let mut allocation = self
                            .active_allocation
                            .take()
                            .ok_or(ConsensusError::Failed)?;
                        if allocation.shrink_to(retained).is_err() {
                            self.active_allocation = Some(allocation);
                            return Err(ConsensusError::Capacity);
                        }
                        self.raw_allocation = Some(allocation);
                        return Ok(Some(events));
                    }
                    let mut ready = self.raw.ready()?;
                    // What a `Ready` gives to apply is committed and durable
                    // here already (`Ready::committed_entries`): it waits
                    // for nothing this `Ready` writes — but for a change of
                    // membership whose commit the log does not hold, which
                    // waits for the write that states it.
                    let committed = ready.take_committed_entries();
                    let held = if self.fenced(&committed) {
                        committed
                    } else {
                        self.apply_entries(committed, &mut pending.events, &mut pending.delivered)?;
                        Vec::new()
                    };
                    if !ready.must_sync() && held.is_empty() {
                        // Nothing but the commit moved, if that: there is no
                        // write to wait for. The commit is kept with the
                        // stored hard state and rides the group's next
                        // record.
                        if let Some(hs) = ready.hard_state() {
                            let stored = &self.raw.store().hard_state;
                            if hs.term != stored.term
                                || hs.vote != stored.vote
                                || hs.commit < stored.commit
                            {
                                return Err(ConsensusError::Corruption(
                                    "a term or a vote in a ready that asks for no write",
                                ));
                            }
                            self.raw.store_mut().hard_state = *hs;
                            self.commit_unwritten = true;
                        }
                        Self::release(&mut ready, &mut pending.events);
                        let light = self.raw.advance_append(ready)?;
                        pending.phase =
                            self.after_advance(light, &mut pending.events, &mut pending.delivered)?;
                        continue;
                    }
                    // What a leader sends its members persist for
                    // themselves: it waits for nothing this `Ready` writes,
                    // and an owner may send it while the write is in flight
                    // (`sendable`).
                    pending.events.messages.extend(ready.take_messages());
                    // No commit is foretold across a change of membership
                    // that is still to be applied: who decides is not
                    // settled until it is.
                    let sole_commit = if held.is_empty() {
                        self.sole_commit(&ready)
                    } else {
                        None
                    };
                    let hard_state = match (ready.hard_state(), sole_commit) {
                        (Some(hs), sole) => {
                            let mut hs = *hs;
                            hs.commit = hs.commit.max(sole.unwrap_or(0));
                            Some(hs)
                        }
                        (None, Some(commit)) => {
                            let mut hs = self.raw.store().hard_state;
                            hs.commit = hs.commit.max(commit);
                            Some(hs)
                        }
                        (None, None) if self.commit_unwritten || !held.is_empty() => {
                            Some(self.raw.store().hard_state)
                        }
                        (None, None) => None,
                    };
                    let mut records = Vec::new();
                    // The record count is known: entries plus an optional snapshot
                    // and hard state. Reserve once so the ready cycle never grows.
                    records
                        .try_reserve_exact(
                            ready
                                .entries()
                                .len()
                                .saturating_add(ready.proposals().len())
                                .saturating_add(2),
                        )
                        .map_err(|_| ConsensusError::Capacity)?;
                    if let Some(snapshot) = ready.snapshot() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::Snapshot,
                            proto::snapshot_index(snapshot),
                            proto::snapshot_term(snapshot),
                            snapshot,
                        )?);
                    }
                    for entry in ready.entries() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::Entry,
                            entry.index,
                            entry.term,
                            entry,
                        )?);
                    }
                    // What the member approved by itself is durable before it
                    // says that it holds it (27 §4.4).
                    for proposal in ready.proposals() {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::Proposal,
                            proposal.index,
                            proposal.term,
                            proposal,
                        )?);
                    }
                    // After the entries: a commit names entries the log
                    // holds by then, as it is read back.
                    if let Some(hs) = &hard_state {
                        records.push(proto_record(
                            self.config.group_id,
                            RecordKind::HardState,
                            hs.commit,
                            hs.term,
                            hs,
                        )?);
                    }
                    self.wal.validate_append(&records)?;
                    let prepared = self.raw.store_mut().prepare_with(
                        ready.entries(),
                        ready.snapshot(),
                        ready.proposals(),
                    )?;
                    pending.phase = Phase::Ready(Box::new(ReadyPhase {
                        ready,
                        prepared,
                        records,
                        receipt: None,
                        outcome: None,
                        hard_state,
                        sole_commit,
                        held,
                    }));
                }
                Phase::Ready(mut work) => {
                    if !work.records.is_empty() && work.receipt.is_none() {
                        let persisted = self.persisted.as_ref().map(|signal| signal());
                        match self.wal.append_async_notified(
                            &work.records,
                            BudgetLane::Completion,
                            persisted,
                        ) {
                            Ok(receipt) => {
                                work.records.clear();
                                work.receipt = Some(receipt);
                                if !blocking {
                                    pending.phase = Phase::Ready(work);
                                    self.persistence = Some(pending);
                                    return Ok(None);
                                }
                            }
                            Err(focal_log::LogError::Capacity) => {
                                pending.phase = Phase::Ready(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    if let Some(ticket) = work.receipt.as_mut() {
                        let completed = match work.outcome.take() {
                            Some(outcome) => Some(outcome),
                            None if blocking => Some(ticket.wait_blocking().map(|_| ())),
                            None => ticket.try_complete().map(|result| result.map(|_| ())),
                        };
                        match completed {
                            None => {
                                pending.phase = Phase::Ready(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Some(result) => {
                                result?;
                            }
                        }
                    }
                    let ReadyPhase {
                        mut ready,
                        prepared,
                        hard_state,
                        sole_commit,
                        held,
                        ..
                    } = *work;
                    self.raw.store_mut().publish(prepared)?;
                    if let Some(snapshot) = ready.snapshot() {
                        let index = proto::snapshot_index(snapshot);
                        pending.delivered = index;
                        pending.events.committed.clear();
                        pending.events.membership.clear();
                        pending.events.snapshot = Some(snapshot_event(snapshot));
                        self.commit_durable = self.commit_durable.max(index);
                    }
                    if let Some(hs) = &hard_state {
                        self.commit_durable = self.commit_durable.max(hs.commit);
                    }
                    // What waited for the commit this write stated.
                    self.apply_entries(held, &mut pending.events, &mut pending.delivered)?;
                    Self::release(&mut ready, &mut pending.events);
                    let light = self.raw.advance_append(ready)?;
                    if let Some(hs) = hard_state {
                        // The log holds the commit this write stated.
                        self.raw.store_mut().hard_state = hs;
                        self.commit_unwritten = false;
                    }
                    // A member that alone decides wrote the commit of its
                    // entries with them: the core says the same now that
                    // they are durable, or the log states what is not so.
                    if sole_commit.is_some_and(|commit| self.raw.raft.hard_state().commit < commit)
                    {
                        return Err(ConsensusError::Corruption(
                            "a sole voter's entries are durable and not committed",
                        ));
                    }
                    pending.phase =
                        self.after_advance(light, &mut pending.events, &mut pending.delivered)?;
                }
                Phase::Light(mut work) => {
                    if let Some(encoded) = work.record.as_ref() {
                        let persisted = self.persisted.as_ref().map(|signal| signal());
                        match self.wal.append_async_notified(
                            std::slice::from_ref(encoded),
                            BudgetLane::Completion,
                            persisted,
                        ) {
                            Ok(ticket) => {
                                work.record = None;
                                work.receipt = Some(ticket);
                                if !blocking {
                                    pending.phase = Phase::Light(work);
                                    self.persistence = Some(pending);
                                    return Ok(None);
                                }
                            }
                            Err(focal_log::LogError::Capacity) => {
                                pending.phase = Phase::Light(work);
                                self.persistence = Some(pending);
                                return Ok(None);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let ticket = work.receipt.as_mut().ok_or(ConsensusError::Failed)?;
                    let completed = match work.outcome.take() {
                        Some(outcome) => Some(outcome),
                        None if blocking => Some(ticket.wait_blocking().map(|_| ())),
                        None => ticket.try_complete().map(|result| result.map(|_| ())),
                    };
                    match completed {
                        None => {
                            pending.phase = Phase::Light(work);
                            self.persistence = Some(pending);
                            return Ok(None);
                        }
                        Some(result) => {
                            result?;
                        }
                    }
                    self.commit_durable = self.commit_durable.max(work.hard_state.commit);
                    self.raw.store_mut().hard_state = work.hard_state;
                    self.commit_unwritten = false;
                    self.finish_light(work.light, &mut pending.events, &mut pending.delivered)?;
                    pending.phase = Phase::Start;
                }
            }
        }
    }
    /// Whether what is given to apply waits for its commit to be in the
    /// log: a change of membership past the commit the log holds, and
    /// anything past it in a group whose members act on what they applied
    /// before the group tells them again (the module's header).
    fn fenced(&self, entries: &[Entry]) -> bool {
        entries.iter().any(|entry| {
            entry.index > self.commit_durable
                && (self.written_commit || proto::changes_configuration(entry))
        })
    }
    /// What follows `advance_append`: what the `Ready` committed is given
    /// at once, unless a change of membership is among it and the log does
    /// not hold its commit — then the commit is written first.
    fn after_advance(
        &mut self,
        light: LightReady,
        events: &mut NodeEvents,
        delivered: &mut u64,
    ) -> Result<Phase, ConsensusError> {
        if !self.fenced(light.committed_entries()) {
            self.finish_light(light, events, delivered)?;
            return Ok(Phase::Start);
        }
        let mut hard_state = self.raw.store().hard_state;
        hard_state.commit = hard_state
            .commit
            .max(light.commit_index().unwrap_or(0))
            .max(self.raw.raft.hard_state().commit);
        let record = proto_record(
            self.config.group_id,
            RecordKind::HardState,
            hard_state.commit,
            hard_state.term,
            &hard_state,
        )?;
        self.wal.validate_append(std::slice::from_ref(&record))?;
        Ok(Phase::Light(Box::new(LightPhase {
            light,
            hard_state,
            record: Some(record),
            receipt: None,
            outcome: None,
        })))
    }
    /// The owner's period has passed. A commit the log did not hold a
    /// period ago and does not hold now — no record of the group has
    /// carried it: the group is quiet — is written, behind what it
    /// released (the module's header).
    pub(super) fn settle_commit(&mut self) -> Result<(), ConsensusError> {
        let commit = self.raw.store().hard_state.commit;
        if !self.commit_unwritten {
            self.commit_waiting = None;
            return self.write_commit_behind();
        }
        if self.commit_waiting != Some(commit) {
            self.commit_waiting = Some(commit);
            return Ok(());
        }
        self.commit_waiting = None;
        self.write_commit_behind()
    }
    /// A commit the log does not hold is written behind what it released,
    /// and waited for by no one: one such write is in flight at a time, the
    /// log orders it before whatever the group writes next, and a log with
    /// no room for it now leaves it to the next record. A write that failed
    /// stops the member, as any of its writes does.
    pub(super) fn write_commit_behind(&mut self) -> Result<(), ConsensusError> {
        if let Some((receipt, commit)) = self.commit_write.as_mut() {
            match receipt.try_complete() {
                None => return Ok(()),
                Some(Ok(_)) => {
                    self.commit_durable = self.commit_durable.max(*commit);
                    self.commit_write = None;
                }
                Some(Err(error)) => {
                    self.commit_write = None;
                    return Err(error.into());
                }
            }
        }
        if !self.commit_unwritten {
            return Ok(());
        }
        let hard_state = self.raw.store().hard_state;
        let record = proto_record(
            self.config.group_id,
            RecordKind::HardState,
            hard_state.commit,
            hard_state.term,
            &hard_state,
        )?;
        let records = std::slice::from_ref(&record);
        let queued = self
            .wal
            .validate_append(records)
            .and_then(|()| self.wal.append_async_in(records, BudgetLane::Completion));
        match queued {
            Ok(receipt) => {
                self.commit_write = Some((receipt, hard_state.commit));
                self.commit_unwritten = false;
                Ok(())
            }
            Err(focal_log::LogError::Capacity) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    /// What a `Ready` gives to say once what it persists is durable, or at
    /// once when it persists nothing: its messages of either kind, the
    /// reads it confirms and what was displaced.
    fn release(ready: &mut Ready, events: &mut NodeEvents) {
        events.displaced.extend(
            ready
                .take_displaced()
                .into_iter()
                .map(|entry| CommittedEntry {
                    index: entry.index,
                    term: entry.term,
                    data: entry.data,
                }),
        );
        events.messages.extend(ready.take_messages());
        events.messages.extend(ready.take_persisted_messages());
        events.read_states.extend(
            ready
                .take_read_states()
                .into_iter()
                .map(|read| ReadBarrier {
                    index: read.index,
                    context: read.request_ctx,
                }),
        );
    }
    /// What follows a `Ready` that is durable: the messages it left, the
    /// entries it committed, and the commit, which waits for no write (the
    /// module's header): it is kept with the stored hard state until the
    /// group's next record carries it.
    fn finish_light(
        &mut self,
        mut light: LightReady,
        events: &mut NodeEvents,
        delivered: &mut u64,
    ) -> Result<(), ConsensusError> {
        if let Some(commit) = light.commit_index()
            && commit > self.raw.store().hard_state.commit
        {
            self.raw.store_mut().hard_state.commit = commit;
            self.commit_unwritten = true;
        }
        events.messages.extend(light.take_messages());
        self.apply_entries(light.take_committed_entries(), events, delivered)?;
        self.raw.advance_apply_to(*delivered)?;
        Ok(())
    }
}
