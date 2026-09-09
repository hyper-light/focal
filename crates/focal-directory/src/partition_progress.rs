//! Per-copy assignment progress, bounded refusals and the measured guarantee of
//! one session's placement. These are deterministic projections of committed
//! partition state: no clock, transport or planner runs here.
use crate::*;
use focal_model::SessionSeq;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// What a node holds for a session under one placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum AssignmentRole {
    Voter,
    Materializer,
    ContentCopy,
}

/// The ladder one assignment climbs. `Failed` is off the ladder: it is entered
/// only through a recorded refusal and left only by a new attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssignmentPhase {
    Assigned,
    Installed,
    CaughtUp,
    CustodyVerified,
    Promoted,
    Active,
    Draining,
    Retired,
    Failed,
}
impl AssignmentPhase {
    /// Ladder position; `Failed` has none.
    pub fn rank(self) -> Option<u8> {
        match self {
            Self::Assigned => Some(0),
            Self::Installed => Some(1),
            Self::CaughtUp => Some(2),
            Self::CustodyVerified => Some(3),
            Self::Promoted => Some(4),
            Self::Active => Some(5),
            Self::Draining => Some(6),
            Self::Retired => Some(7),
            Self::Failed => None,
        }
    }
    pub fn at_least(self, other: Self) -> bool {
        match (self.rank(), other.rank()) {
            (Some(mine), Some(theirs)) => mine >= theirs,
            _ => false,
        }
    }
}

/// Why an assignment or a plan could not proceed. Codes name the fact the
/// controller observed; they never carry free text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefusalCode {
    NoPlacement,
    NodeCapacity,
    DiskCapacity,
    CustodyUnavailable,
    LearnerBehind,
    Residency,
    Quorum,
    Expired,
    Unreachable,
}
impl RefusalCode {
    /// A retryable refusal may be answered by another attempt on the same
    /// node; the others need a different plan or a fleet change.
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::NodeCapacity
                | Self::DiskCapacity
                | Self::CustodyUnavailable
                | Self::LearnerBehind
                | Self::Expired
                | Self::Unreachable
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub operation: OperationId,
    pub code: RefusalCode,
    /// None: the plan itself was refused (no node was assigned).
    pub node: Option<u64>,
    pub attempt: u32,
    /// The controller's authenticated decision time; diagnostic only.
    pub at: i64,
}

/// Committed progress of one node's assignment. Progress is monotone within an
/// attempt; a new attempt may restart the ladder after a refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentProgress {
    pub node: u64,
    pub node_generation: u64,
    pub roles: BTreeSet<AssignmentRole>,
    pub phase: AssignmentPhase,
    pub attempt: u32,
    /// The session prefix the copy has applied or verified.
    pub through: SessionSeq,
    /// The placement epoch whose content set this copy verified; zero below
    /// `CustodyVerified`.
    pub custody_epoch: u64,
    pub refusal: Option<RefusalCode>,
}
impl AssignmentProgress {
    pub fn assigned(node: u64, node_generation: u64, roles: BTreeSet<AssignmentRole>) -> Self {
        Self {
            node,
            node_generation,
            roles,
            phase: AssignmentPhase::Assigned,
            attempt: 1,
            through: SessionSeq(0),
            custody_epoch: 0,
            refusal: None,
        }
    }
}

/// Roles one node holds under a placement.
pub fn roles_of(placement: &Placement, node: u64) -> BTreeSet<AssignmentRole> {
    let mut roles = BTreeSet::new();
    if placement.voters.contains_key(&node) {
        roles.insert(AssignmentRole::Voter);
    }
    if placement.materializers.contains_key(&node) {
        roles.insert(AssignmentRole::Materializer);
    }
    if placement.content_copies.contains_key(&node) {
        roles.insert(AssignmentRole::ContentCopy);
    }
    roles
}

/// The assignment phase a copy needs before the placement may activate.
pub fn required_phase(roles: &BTreeSet<AssignmentRole>) -> AssignmentPhase {
    if roles.contains(&AssignmentRole::Voter) {
        AssignmentPhase::Promoted
    } else {
        AssignmentPhase::CustodyVerified
    }
}

/// The plan phase implied by committed progress once preparation began.
pub(crate) fn derive_phase(plan: &PendingPlacement) -> PlacementPhase {
    if plan.phase == PlacementPhase::Planned {
        return PlacementPhase::Planned;
    }
    if plan.barrier.is_some() {
        return PlacementPhase::Cutover;
    }
    let mut lowest = AssignmentPhase::Promoted;
    for progress in plan.progress.values() {
        match progress.phase {
            AssignmentPhase::Failed => return PlacementPhase::Failed,
            phase if !phase.at_least(lowest) => lowest = phase,
            _ => {}
        }
    }
    match lowest {
        AssignmentPhase::Assigned => PlacementPhase::Preparing,
        AssignmentPhase::Installed => PlacementPhase::Catchup,
        AssignmentPhase::CaughtUp => PlacementPhase::Custody,
        _ => PlacementPhase::Promoting,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockReason {
    MissingNode,
    StaleNode,
    IneligibleNode,
    /// The fleet's detector declared the member dead.
    DeadNode,
    UnknownDomain,
    Assignment(AssignmentPhase),
    Refused(RefusalCode),
    AwaitingCutover,
    AwaitingActivation,
    Draining,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blocker {
    pub node: Option<u64>,
    pub reason: BlockReason,
}

/// What a session promises, what its active placement measurably provides
/// against the current node registry, and what stands between the two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuaranteeReport {
    pub desired: DurabilityIntent,
    /// None when a promised failure domain cannot be evaluated for a member.
    pub achieved: Option<DurabilityIntent>,
    pub blocked_by: Vec<Blocker>,
    pub phase: Option<PlacementPhase>,
}

const MAX_BLOCKERS: usize = 256;

/// Measure the active placement against the node registry and report the
/// pending plan's outstanding work. Bounded by the placement's member count.
pub fn effective_guarantee(
    session: &SessionDescriptor,
    nodes: &BTreeMap<u64, NodeRecord>,
) -> Result<GuaranteeReport, DirectoryError> {
    let desired = session
        .pending
        .as_ref()
        .map_or(session.active.policy.durability, |plan| {
            plan.desired.policy.durability
        });
    let mut blocked_by = Vec::new();
    blocked_by
        .try_reserve_exact(MAX_BLOCKERS)
        .map_err(|_| DirectoryError::Memory(focal_memory::MemoryError::AllocationFailed))?;
    let mut push = |blocker: Blocker| -> Result<(), DirectoryError> {
        if blocked_by.len() >= MAX_BLOCKERS {
            return Err(DirectoryError::Capacity);
        }
        blocked_by.push(blocker);
        Ok(())
    };
    let class = session.active.policy.durability.survive;
    let mut evaluable = true;
    let mut tolerated = u16::MAX;
    for (members, quorum) in [
        (&session.active.placement.voters, true),
        (&session.active.placement.materializers, false),
        (&session.active.placement.content_copies, false),
    ] {
        let mut domains = BTreeMap::new();
        let mut alive = 0_usize;
        for (id, generation) in members {
            let Some(node) = nodes.get(id) else {
                push(Blocker {
                    node: Some(*id),
                    reason: BlockReason::MissingNode,
                })?;
                continue;
            };
            if node.enrollment.generation != *generation {
                push(Blocker {
                    node: Some(*id),
                    reason: BlockReason::StaleNode,
                })?;
                continue;
            }
            if !node.enrollment.eligible {
                push(Blocker {
                    node: Some(*id),
                    reason: BlockReason::IneligibleNode,
                })?;
                continue;
            }
            if !node.is_alive() {
                push(Blocker {
                    node: Some(*id),
                    reason: BlockReason::DeadNode,
                })?;
                continue;
            }
            let Ok(domain) = placement::failure_domain(&node.enrollment, class) else {
                evaluable = false;
                push(Blocker {
                    node: Some(*id),
                    reason: BlockReason::UnknownDomain,
                })?;
                continue;
            };
            alive = add(alive, 1)?;
            let count = domains.entry(domain).or_insert(0_usize);
            *count = add(*count, 1)?;
        }
        let mut counts: Vec<usize> = Vec::new();
        counts
            .try_reserve_exact(domains.len())
            .map_err(|_| DirectoryError::Memory(focal_memory::MemoryError::AllocationFailed))?;
        counts.extend(domains.into_values());
        counts.sort_unstable_by(|a, b| b.cmp(a));
        let needed = if quorum {
            members.len().saturating_div(2).saturating_add(1)
        } else {
            1
        };
        // The largest f whose worst-case loss (the f fullest domains) still
        // leaves `needed` members: a linear scan over at most `members` domains.
        let mut survivable = 0_u16;
        let mut remaining = alive;
        for count in counts {
            let after = remaining.saturating_sub(count);
            if after < needed {
                break;
            }
            remaining = after;
            survivable = survivable.saturating_add(1);
        }
        if alive < needed {
            survivable = 0;
        }
        tolerated = tolerated.min(survivable);
    }
    if tolerated == u16::MAX {
        tolerated = 0;
    }
    let achieved = evaluable.then_some(DurabilityIntent {
        survive: class,
        max_failures: tolerated,
    });
    if let Some(plan) = &session.pending {
        match plan.phase {
            PlacementPhase::Planned => push(Blocker {
                node: None,
                reason: BlockReason::AwaitingActivation,
            })?,
            PlacementPhase::Cutover => {
                for progress in plan.progress.values() {
                    if plan
                        .barrier
                        .as_ref()
                        .is_some_and(|barrier| progress.through < barrier.sequence)
                    {
                        push(Blocker {
                            node: Some(progress.node),
                            reason: BlockReason::Assignment(progress.phase),
                        })?;
                    }
                }
                push(Blocker {
                    node: None,
                    reason: BlockReason::AwaitingActivation,
                })?;
            }
            _ => {
                let mut complete = true;
                for progress in plan.progress.values() {
                    if let Some(code) = progress
                        .refusal
                        .filter(|_| progress.phase == AssignmentPhase::Failed)
                    {
                        complete = false;
                        push(Blocker {
                            node: Some(progress.node),
                            reason: BlockReason::Refused(code),
                        })?;
                    } else if !progress.phase.at_least(required_phase(&progress.roles)) {
                        complete = false;
                        push(Blocker {
                            node: Some(progress.node),
                            reason: BlockReason::Assignment(progress.phase),
                        })?;
                    }
                }
                if complete {
                    push(Blocker {
                        node: None,
                        reason: BlockReason::AwaitingCutover,
                    })?;
                }
            }
        }
    }
    for retiring in session.retiring.values() {
        push(Blocker {
            node: Some(retiring.node),
            reason: BlockReason::Draining,
        })?;
    }
    for refusal in &session.refusals {
        if refusal.node.is_none() && session.pending.is_none() {
            push(Blocker {
                node: None,
                reason: BlockReason::Refused(refusal.code),
            })?;
        }
    }
    blocked_by.shrink_to_fit();
    Ok(GuaranteeReport {
        desired,
        achieved,
        blocked_by,
        phase: session.pending.as_ref().map(|plan| plan.phase),
    })
}

pub(crate) fn progress_charge(progress: &AssignmentProgress) -> Result<usize, DirectoryError> {
    add(
        tree_row::<(u64, AssignmentProgress)>(),
        mul(progress.roles.len().max(1), tree_row::<AssignmentRole>())?,
    )
}
