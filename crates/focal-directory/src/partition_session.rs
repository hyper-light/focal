//! The per-session placement state machine applied inside partition
//! preparation. Every rule here is deterministic over committed state and the
//! bounded evidence the caller's verifier already authenticated.
use crate::*;
use focal_model::{LedgerId, RouteEpoch};
use std::collections::BTreeMap;

pub(crate) fn change_charge(
    state: &PartitionCheckpoint,
    ledger: LedgerId,
    change: &SessionChange,
) -> Result<usize, DirectoryError> {
    let session = state.sessions.get(&ledger);
    Ok(match change {
        SessionChange::Plan {
            desired,
            observations,
            ..
        } => add(
            placement::spec_charge(desired)?,
            mul(observations.len(), tree_row::<(u64, u64)>())?,
        )?,
        SessionChange::BeginPreparation { .. } => {
            let members = session
                .and_then(|session| session.pending.as_ref())
                .map_or(0, |plan| plan.desired.placement.nodes().len());
            mul(
                members,
                add(
                    tree_row::<(u64, AssignmentProgress)>(),
                    mul(3, tree_row::<AssignmentRole>())?,
                )?,
            )?
        }
        SessionChange::Ready { .. } => tree_row::<(u64, ReplicaReady)>(),
        SessionChange::Progress { progress, .. } => partition_progress::progress_charge(progress)?,
        SessionChange::Refuse { .. } => add(size_of::<Refusal>(), ALLOCATOR_OVERHEAD)?,
        SessionChange::Activate { .. } => {
            let members = session.map_or(0, |session| session.active.placement.nodes().len());
            mul(
                members,
                add(
                    tree_row::<(u64, AssignmentProgress)>(),
                    mul(3, tree_row::<AssignmentRole>())?,
                )?,
            )?
        }
        SessionChange::Cutover { .. }
        | SessionChange::Abort { .. }
        | SessionChange::Drain { .. }
        | SessionChange::Retire { .. } => 0,
    })
}

pub(crate) fn apply_session(
    session: &mut SessionDescriptor,
    change: &SessionChange,
    nodes: &BTreeMap<u64, NodeRecord>,
    config: PartitionConfig,
    verifier: &impl AuthorityVerifier,
) -> Result<(), DirectoryError> {
    match change {
        SessionChange::Plan {
            operation,
            desired,
            observations,
        } => {
            if let Some(pending) = &session.pending {
                return if pending.operation == *operation
                    && pending.desired == *desired
                    && pending.observations == *observations
                {
                    Ok(())
                } else {
                    Err(DirectoryError::Phase)
                };
            }
            if session.authority.operation == *operation {
                return Err(DirectoryError::WrongOperation);
            }
            verify_placement(desired, nodes, config.max_members)?;
            for (node, report) in observations {
                if desired.placement.generation(*node).is_none() {
                    return Err(DirectoryError::Invalid("observation outside placement"));
                }
                if nodes
                    .get(node)
                    .and_then(|record| record.load)
                    .is_none_or(|load| load.report < *report)
                {
                    return Err(DirectoryError::StaleNode);
                }
            }
            session.pending = Some(PendingPlacement {
                operation: *operation,
                next_route: RouteEpoch(
                    session
                        .route_epoch
                        .0
                        .checked_add(1)
                        .ok_or(DirectoryError::CounterExhausted)?,
                ),
                next_membership: session
                    .membership_epoch
                    .checked_add(u64::from(
                        desired.placement.voters != session.active.placement.voters,
                    ))
                    .ok_or(DirectoryError::CounterExhausted)?,
                next_placement: session
                    .placement_epoch
                    .checked_add(1)
                    .ok_or(DirectoryError::CounterExhausted)?,
                desired: desired.clone(),
                phase: PlacementPhase::Planned,
                ready: BTreeMap::new(),
                barrier: None,
                observations: observations.clone(),
                progress: BTreeMap::new(),
            });
        }
        SessionChange::BeginPreparation { operation } => {
            let plan = pending(session, *operation)?;
            if plan.phase != PlacementPhase::Planned {
                return Ok(());
            }
            let placement = &plan.desired.placement;
            let mut progress = BTreeMap::new();
            for node in placement.nodes() {
                let generation = placement.generation(node).ok_or(DirectoryError::Missing)?;
                progress.insert(
                    node,
                    AssignmentProgress::assigned(node, generation, roles_of(placement, node)),
                );
            }
            plan.progress = progress;
            plan.phase = PlacementPhase::Preparing;
            plan.phase = partition_progress::derive_phase(plan);
        }
        SessionChange::Ready { ready } => {
            if ready.ledger != session.ledger {
                return Err(DirectoryError::OutsideNamespace);
            }
            let plan = pending(session, ready.operation)?;
            if plan.phase == PlacementPhase::Planned {
                return Err(DirectoryError::Phase);
            }
            if ready.route_epoch != plan.next_route
                || plan.desired.placement.generation(ready.node) != Some(ready.node_generation)
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if nodes
                .get(&ready.node)
                .is_none_or(|node| node.enrollment.generation != ready.node_generation)
            {
                return Err(DirectoryError::StaleNode);
            }
            if plan
                .ready
                .get(&ready.node)
                .is_some_and(|old| old.through > ready.through)
            {
                return Err(DirectoryError::StaleEpoch);
            }
            if !types::nonzero_hash(ready.attestation) || !types::nonzero_hash(ready.custody) {
                return Err(DirectoryError::Custody);
            }
            let entry = plan
                .progress
                .get(&ready.node)
                .ok_or(DirectoryError::Phase)?;
            if entry.phase == AssignmentPhase::Failed {
                return Err(DirectoryError::Phase);
            }
            verifier.verify_replica_ready(ready)?;
            let custody_epoch = plan.next_placement;
            let entry = plan
                .progress
                .get_mut(&ready.node)
                .ok_or(DirectoryError::Phase)?;
            if !entry.phase.at_least(AssignmentPhase::CustodyVerified) {
                entry.phase = AssignmentPhase::CustodyVerified;
            }
            entry.through = entry.through.max(ready.through);
            entry.custody_epoch = custody_epoch;
            plan.ready.insert(ready.node, ready.clone());
            plan.phase = partition_progress::derive_phase(plan);
        }
        SessionChange::Progress {
            operation,
            progress,
        } => {
            let plan = pending(session, *operation)?;
            if plan.phase == PlacementPhase::Planned {
                return Err(DirectoryError::Phase);
            }
            validate_reported_progress(plan, progress)?;
            let entry = plan
                .progress
                .get_mut(&progress.node)
                .ok_or(DirectoryError::StaleEpoch)?;
            if progress.attempt < entry.attempt {
                return Err(DirectoryError::StaleEpoch);
            }
            if progress.attempt == entry.attempt {
                if entry.phase == AssignmentPhase::Failed {
                    return Err(DirectoryError::Phase);
                }
                let (Some(new), Some(old)) = (progress.phase.rank(), entry.phase.rank()) else {
                    return Err(DirectoryError::Phase);
                };
                if new < old || progress.through < entry.through {
                    return Err(DirectoryError::StaleEpoch);
                }
                if new == old && progress.through == entry.through {
                    return Ok(());
                }
            }
            *entry = progress.clone();
            plan.phase = partition_progress::derive_phase(plan);
        }
        SessionChange::Refuse { operation, refusal } => {
            if refusal.operation != *operation {
                return Err(DirectoryError::WrongOperation);
            }
            if session.refusals.iter().any(|old| old == refusal) {
                return Ok(());
            }
            match refusal.node {
                Some(node) => {
                    let plan = pending(session, *operation)?;
                    if plan.phase == PlacementPhase::Planned || plan.barrier.is_some() {
                        return Err(DirectoryError::Phase);
                    }
                    let entry = plan
                        .progress
                        .get_mut(&node)
                        .ok_or(DirectoryError::StaleEpoch)?;
                    if refusal.attempt != entry.attempt {
                        return Err(DirectoryError::StaleEpoch);
                    }
                    if entry.phase == AssignmentPhase::Failed {
                        return Err(DirectoryError::Phase);
                    }
                    entry.phase = AssignmentPhase::Failed;
                    entry.refusal = Some(refusal.code);
                    plan.phase = partition_progress::derive_phase(plan);
                }
                None => {
                    if session
                        .pending
                        .as_ref()
                        .is_some_and(|plan| plan.operation == *operation)
                        || session.authority.operation == *operation
                    {
                        return Err(DirectoryError::WrongOperation);
                    }
                }
            }
            record_refusal(session, refusal, config)?;
        }
        SessionChange::Cutover {
            operation,
            authority,
        } => {
            validate_transition_fence(session, *operation, authority, SessionFenceKind::Cutover)?;
            if authority.sequence < session.authority.sequence
                || authority.index <= session.authority.index
                || authority.term < session.authority.term
            {
                return Err(DirectoryError::StaleEpoch);
            }
            let plan = session.pending.as_ref().ok_or(DirectoryError::Phase)?;
            if plan.phase == PlacementPhase::Planned {
                return Err(DirectoryError::Phase);
            }
            if plan
                .barrier
                .as_ref()
                .is_some_and(|previous| previous != authority)
            {
                return Err(DirectoryError::CompareFailed);
            }
            if plan.barrier.is_some() {
                return Ok(());
            }
            for progress in plan.progress.values() {
                if progress.phase == AssignmentPhase::Failed
                    || (progress.roles.contains(&AssignmentRole::Voter)
                        && !progress.phase.at_least(AssignmentPhase::Promoted))
                {
                    return Err(DirectoryError::NotReady);
                }
            }
            verifier.verify_session_fence(authority)?;
            let plan = pending(session, *operation)?;
            plan.barrier = Some(authority.clone());
            plan.phase = partition_progress::derive_phase(plan);
        }
        SessionChange::Activate {
            operation,
            authority,
        } => {
            if session.pending.is_none() && session.authority == *authority {
                return Ok(());
            }
            validate_transition_fence(session, *operation, authority, SessionFenceKind::Activated)?;
            let plan = session.pending.as_ref().ok_or(DirectoryError::Phase)?;
            let barrier = plan.barrier.as_ref().ok_or(DirectoryError::NotReady)?;
            if authority.sequence < barrier.sequence
                || authority.index <= barrier.index
                || authority.term < barrier.term
            {
                return Err(DirectoryError::StaleEpoch);
            }
            verify_placement(&plan.desired, nodes, config.max_members)?;
            for node in plan.desired.placement.nodes() {
                let ready = plan.ready.get(&node).ok_or(DirectoryError::NotReady)?;
                let progress = plan.progress.get(&node).ok_or(DirectoryError::NotReady)?;
                if ready.through < barrier.sequence
                    || !progress.phase.at_least(required_phase(&progress.roles))
                    || progress.custody_epoch != plan.next_placement
                {
                    return Err(DirectoryError::NotReady);
                }
            }
            verifier.verify_session_fence(authority)?;
            let cut = barrier.sequence;
            let plan = session.pending.take().ok_or(DirectoryError::Phase)?;
            let previous = std::mem::replace(&mut session.active, plan.desired);
            let previous_epoch = session.placement_epoch;
            session.route_epoch = plan.next_route;
            session.membership_epoch = authority.membership_epoch;
            session.placement_epoch = plan.next_placement;
            session.authority = authority.clone();
            let kept = session.active.placement.nodes();
            session.retiring.retain(|node, _| !kept.contains(node));
            for node in previous.placement.nodes() {
                if kept.contains(&node) || session.retiring.contains_key(&node) {
                    continue;
                }
                let generation = previous
                    .placement
                    .generation(node)
                    .ok_or(DirectoryError::Missing)?;
                session.retiring.insert(
                    node,
                    AssignmentProgress {
                        node,
                        node_generation: generation,
                        roles: roles_of(&previous.placement, node),
                        phase: AssignmentPhase::Active,
                        attempt: 1,
                        through: cut,
                        custody_epoch: previous_epoch,
                        refusal: None,
                    },
                );
            }
        }
        SessionChange::Abort { operation } => {
            if pending(session, *operation)?.barrier.is_some() {
                return Err(DirectoryError::Phase);
            }
            session.pending = None;
        }
        SessionChange::Drain { operation, node } => {
            if session.authority.operation != *operation {
                return Err(DirectoryError::WrongOperation);
            }
            let copy = session
                .retiring
                .get_mut(node)
                .ok_or(DirectoryError::Missing)?;
            copy.phase = AssignmentPhase::Draining;
        }
        SessionChange::Retire { operation, node } => {
            if session.authority.operation != *operation {
                return Err(DirectoryError::WrongOperation);
            }
            if let Some(copy) = session.retiring.get(node) {
                if copy.phase != AssignmentPhase::Draining {
                    return Err(DirectoryError::Phase);
                }
                session.retiring.remove(node);
            }
        }
    }
    Ok(())
}

fn record_refusal(
    session: &mut SessionDescriptor,
    refusal: &Refusal,
    config: PartitionConfig,
) -> Result<(), DirectoryError> {
    if config.max_refusals == 0 {
        return Ok(());
    }
    if session.refusals.len() >= config.max_refusals {
        session.refusals.remove(0);
    }
    session
        .refusals
        .try_reserve_exact(1)
        .map_err(|_| DirectoryError::Memory(focal_memory::MemoryError::AllocationFailed))?;
    session.refusals.push(refusal.clone());
    Ok(())
}

/// A reported progress row must describe the assignment it claims to advance.
fn validate_reported_progress(
    plan: &PendingPlacement,
    progress: &AssignmentProgress,
) -> Result<(), DirectoryError> {
    let placement = &plan.desired.placement;
    if placement.generation(progress.node) != Some(progress.node_generation) {
        return Err(DirectoryError::StaleEpoch);
    }
    if progress.roles != roles_of(placement, progress.node) || progress.attempt == 0 {
        return Err(DirectoryError::Invalid("assignment roles or attempt"));
    }
    match progress.phase {
        AssignmentPhase::Failed
        | AssignmentPhase::Active
        | AssignmentPhase::Draining
        | AssignmentPhase::Retired => return Err(DirectoryError::Phase),
        AssignmentPhase::Promoted if !progress.roles.contains(&AssignmentRole::Voter) => {
            return Err(DirectoryError::Phase);
        }
        _ => {}
    }
    if progress.refusal.is_some() {
        return Err(DirectoryError::Phase);
    }
    let verified = progress.phase.at_least(AssignmentPhase::CustodyVerified);
    if verified && !plan.ready.contains_key(&progress.node) {
        return Err(DirectoryError::NotReady);
    }
    if progress.custody_epoch != if verified { plan.next_placement } else { 0 } {
        return Err(DirectoryError::StaleEpoch);
    }
    Ok(())
}

/// Checkpoint-level consistency of a plan's progress rows.
pub(crate) fn validate_progress(plan: &PendingPlacement) -> Result<(), DirectoryError> {
    let placement = &plan.desired.placement;
    for (node, progress) in &plan.progress {
        if *node != progress.node
            || placement.generation(*node) != Some(progress.node_generation)
            || progress.roles != roles_of(placement, *node)
            || progress.attempt == 0
        {
            return Err(DirectoryError::StaleEpoch);
        }
        match progress.phase {
            AssignmentPhase::Active | AssignmentPhase::Draining | AssignmentPhase::Retired => {
                return Err(DirectoryError::Phase);
            }
            AssignmentPhase::Failed if progress.refusal.is_none() => {
                return Err(DirectoryError::Phase);
            }
            AssignmentPhase::Promoted if !progress.roles.contains(&AssignmentRole::Voter) => {
                return Err(DirectoryError::Phase);
            }
            _ => {}
        }
        if progress.phase != AssignmentPhase::Failed && progress.refusal.is_some() {
            return Err(DirectoryError::Phase);
        }
        let verified = progress.phase.at_least(AssignmentPhase::CustodyVerified);
        if verified && !plan.ready.contains_key(node) {
            return Err(DirectoryError::NotReady);
        }
        if progress.custody_epoch != if verified { plan.next_placement } else { 0 } {
            return Err(DirectoryError::StaleEpoch);
        }
    }
    if plan.barrier.is_some() {
        for progress in plan.progress.values() {
            if progress.phase == AssignmentPhase::Failed
                || (progress.roles.contains(&AssignmentRole::Voter)
                    && !progress.phase.at_least(AssignmentPhase::Promoted))
            {
                return Err(DirectoryError::NotReady);
            }
        }
    }
    Ok(())
}

fn pending(
    session: &mut SessionDescriptor,
    operation: OperationId,
) -> Result<&mut PendingPlacement, DirectoryError> {
    let plan = session.pending.as_mut().ok_or(DirectoryError::Phase)?;
    if plan.operation != operation {
        return Err(DirectoryError::WrongOperation);
    }
    Ok(plan)
}
pub(crate) fn validate_transition_fence(
    session: &SessionDescriptor,
    operation: OperationId,
    fence: &SessionFence,
    kind: SessionFenceKind,
) -> Result<(), DirectoryError> {
    let pending = session.pending.as_ref().ok_or(DirectoryError::Phase)?;
    if pending.operation != operation || fence.operation != operation {
        return Err(DirectoryError::WrongOperation);
    }
    if fence.kind != kind
        || fence.ledger != session.ledger
        || fence.log_group != session.log_group
        || fence.from_route != session.route_epoch
        || fence.to_route != pending.next_route
        // The plan knows only how many voter-set changes it needs at least;
        // the fence carries the group's actual epoch at signing.
        || fence.membership_epoch < pending.next_membership
        || fence.placement_epoch != pending.next_placement
    {
        return Err(DirectoryError::StaleEpoch);
    }
    validate_fence(fence, &pending.desired)
}
pub(crate) fn validate_fence(
    fence: &SessionFence,
    spec: &PlacementSpec,
) -> Result<(), DirectoryError> {
    if fence.index.0 == 0 || fence.term.0 == 0 || !types::nonzero_hash(fence.record_hash) {
        return Err(DirectoryError::UnverifiedAuthority);
    }
    if fence.placement_digest != placement_digest(spec)? {
        return Err(DirectoryError::CompareFailed);
    }
    Ok(())
}
