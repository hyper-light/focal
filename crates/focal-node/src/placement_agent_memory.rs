//! What the placement agent holds, charged as it holds it. Its tables are
//! charged by their entries (`focal_memory::map_bytes`): before a pass grows
//! them, the most that pass's observation can add (`fund_pass`), and after
//! every pass and job, what they hold then (`settle`). The agent prepaid a
//! fixed 32 MiB for its life instead, a third of a 128 MB container's
//! envelope: no node of such a cluster had room to seat a session's second
//! copy (each reported 47.8 MB free against the 50.3 MB a 48 MiB session
//! asks).
use super::*;
use focal_directory::{RegionId, SessionDescriptor};
use focal_memory::{map_bytes, map_entry_bytes};

/// What one session's rows in the agent's tables hold at most: its custody
/// scope, announced and installed, its fence (a row, its residency and its
/// members' regions), its install record (a row and its member sets), its
/// last verification and leader hint, and the balancers' observations of it
/// and of each range of its group.
fn session_rows(descriptor: &SessionDescriptor) -> Option<usize> {
    let members = |spec: &focal_directory::PlacementSpec| {
        spec.placement
            .voters
            .len()
            .checked_add(spec.placement.content_copies.len())
    };
    let pending = match &descriptor.pending {
        Some(plan) => members(&plan.desired)?,
        None => 0,
    };
    let members = members(&descriptor.active)?.checked_add(pending)?;
    let residency = descriptor.active.policy.residency.len().max(
        descriptor
            .pending
            .as_ref()
            .map_or(0, |plan| plan.desired.policy.residency.len()),
    );
    let ranges = focal_ranges::RangeLimits::default().max_ranges;
    [
        map_entry_bytes::<LedgerId, CustodyScope>()?.checked_mul(2),
        map_entry_bytes::<LedgerId, crate::placement_executor::ResidencyFence>(),
        map_bytes::<RegionId, ()>(residency),
        map_bytes::<u64, RegionId>(members),
        map_entry_bytes::<LedgerId, InstalledCopy>(),
        map_bytes::<u64, ()>(members)?.checked_mul(2),
        members.checked_mul(size_of::<u64>()),
        map_entry_bytes::<LedgerId, (ContentHash, u64)>(),
        map_entry_bytes::<LedgerId, u64>(),
        // The leader and home balancers, one observation each.
        crate::leader_balancer::LeaderBalancer::row_bytes()?.checked_mul(2),
        // The range balancer: over and under, a row for each range.
        map_entry_bytes::<(LedgerId, focal_memory::RangeId), u8>()?
            .checked_mul(2)?
            .checked_mul(ranges),
    ]
    .into_iter()
    .try_fold(0usize, |total, part| total.checked_add(part?))
}

impl PlacementAgent {
    /// What the agent's own tables hold now, by their entries: the hosted
    /// copies' custody, fences and install records, the per-node,
    /// per-partition and per-session tables of its passes, the partitions
    /// kept for the operator's view (each as its owner charges it), its
    /// journals and its queued requests.
    pub(super) fn held_bytes(&self) -> Option<usize> {
        let mut parts: Vec<Option<usize>> = vec![
            map_bytes::<LedgerId, CustodyScope>(self.custody.len()),
            map_bytes::<LedgerId, crate::placement_executor::ResidencyFence>(self.fences.len()),
            map_bytes::<u64, RegionId>(self.node_regions.len()),
            map_bytes::<LedgerId, CustodyScope>(self.pending_custody.len()),
            map_bytes::<focal_directory::PartitionId, (i64, u64)>(self.last_load.len()),
            map_bytes::<LedgerId, u64>(self.session_leaders.len()),
            self.last_observed
                .capacity()
                .checked_mul(size_of::<(Delegation, PartitionCheckpoint)>()),
            map_bytes::<u64, u64>(self.member_facts.len()),
            map_bytes::<LedgerId, (ContentHash, u64)>(self.verified_active.len()),
            map_bytes::<LedgerId, (focal_directory::DurabilityIntent, Vec<(PlanReply, bool)>)>(
                self.plan_requests.len(),
            ),
            self.move_requests
                .capacity()
                .checked_mul(size_of::<crate::placement_control::MoveRangeJob>()),
            self.balancer.held_bytes(),
            self.leader_balancer.held_bytes(),
            self.home_balancer.held_bytes(),
            self.leading.as_ref().map_or(Some(0), |leading| {
                map_bytes::<u64, u64>(leading.counts.len())
            }),
        ];
        parts.extend(self.fences.values().map(|fence| fence.bytes().ok()));
        parts.extend(
            self.last_observed
                .iter()
                .map(|(_, checkpoint)| checkpoint.charged_bytes().ok()),
        );
        parts.extend(self.plan_requests.values().map(|(_, waiters)| {
            waiters
                .capacity()
                .checked_mul(size_of::<(PlanReply, bool)>())
        }));
        if let Some(journals) = &self.journals {
            parts.push(journals.root.held_bytes());
            parts.push(map_bytes::<
                focal_directory::PartitionId,
                crate::placement_journal::IntentJournal,
            >(journals.partitions.len()));
            parts.extend(
                journals
                    .partitions
                    .values()
                    .map(|journal| journal.held_bytes()),
            );
        }
        if let Some(installs) = &self.installs {
            parts.push(map_bytes::<LedgerId, InstalledCopy>(
                installs.record.installed.len(),
            ));
            parts.extend(installs.record.installed.values().map(|copy| {
                map_bytes::<u64, ()>(copy.voters.len())?
                    .checked_add(map_bytes::<u64, ()>(copy.copies.len())?)?
                    .checked_add(
                        copy.bootstrap_voters
                            .capacity()
                            .checked_mul(size_of::<u64>())?,
                    )
            }));
        }
        parts
            .into_iter()
            .try_fold(0usize, |total, part| total.checked_add(part?))
    }

    /// The most a pass over `observed` adds to the agent's tables: the
    /// partitions it keeps for the operator's view, a row in every per-node
    /// table for each node they list, every session's rows
    /// (`session_rows`), and a row in every per-partition table for each.
    pub(super) fn pass_bytes(observed: &[Observed]) -> Option<usize> {
        let node_rows = map_entry_bytes::<u64, RegionId>()?
            .checked_add(map_entry_bytes::<u64, u64>()?.checked_mul(2)?)?;
        let partition_rows = map_entry_bytes::<focal_directory::PartitionId, (i64, u64)>()?
            .checked_add(map_entry_bytes::<
                focal_directory::PartitionId,
                crate::placement_journal::IntentJournal,
            >()?)?;
        let mut bytes = observed
            .len()
            .checked_mul(size_of::<(Delegation, PartitionCheckpoint)>())?;
        for (_, _, snapshot, _) in observed {
            let ControlBootstrap::Partition { directory } = &snapshot.state else {
                continue;
            };
            bytes = bytes
                .checked_add(directory.charged_bytes().ok()?)?
                .checked_add(directory.nodes.len().checked_mul(node_rows)?)?
                .checked_add(partition_rows)?;
            for descriptor in directory.sessions.values() {
                bytes = bytes.checked_add(session_rows(descriptor)?)?;
            }
        }
        Some(bytes)
    }

    /// Fund a pass over `observed` before it grows anything: what the agent
    /// holds and the most the pass can add. A refusal is `Capacity`, the pass
    /// unmade, and nothing grown.
    pub(super) fn fund_pass(&mut self, observed: &[Observed]) -> Result<(), AgentError> {
        let wanted = self
            .held_bytes()
            .and_then(|held| held.checked_add(Self::pass_bytes(observed)?))
            .ok_or(AgentError::Capacity)?;
        self.charge_to(wanted)
    }

    /// The charge for the agent's tables made `wanted` bytes: grown by a new
    /// reservation, or given back down to it.
    fn charge_to(&mut self, wanted: usize) -> Result<(), AgentError> {
        let held = self.tables.bytes();
        if wanted <= held {
            return self
                .tables
                .shrink_to(wanted)
                .map_err(|_| AgentError::Capacity);
        }
        let mut more = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                wanted.checked_sub(held).ok_or(AgentError::Capacity)?,
            )
            .map_err(|_| AgentError::Capacity)?
            .commit();
        self.tables
            .absorb(&mut more)
            .map_err(|_| AgentError::Capacity)
    }

    /// The charge settled on what the agent's tables hold now, after a pass or
    /// a job. Their growth was funded before it (`fund_pass`), so this gives
    /// back what was not used; growth past it, which a pass's bound rules
    /// out, is charged here when the budget takes it.
    pub(super) fn settle(&mut self) {
        if let Some(held) = self.held_bytes() {
            let _ = self.charge_to(held);
        }
    }
}
