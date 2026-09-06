use crate::access::GraphRead;
use crate::*;
/// Brute-force least fixed point, permanently retained as a correctness oracle.
pub fn least_fixpoint(state: &State) -> BTreeSet<ClaimId> {
    tracked_fixpoint(state)
}
pub(crate) fn tracked_fixpoint(state: &impl GraphRead) -> BTreeSet<ClaimId> {
    let mut satisfied = BTreeSet::new();
    for (id, claim) in state.claims() {
        if claim.lifecycle().status == ClaimStatus::Satisfied {
            if state.scratch(1).is_err() {
                return satisfied;
            }
            satisfied.insert(*id);
        }
    }
    loop {
        let mut changed = false;
        for (id, c) in state.claims() {
            if satisfied.contains(id)
                || c.lifecycle().status.is_terminal()
                || !c.lifecycle().local_complete
            {
                continue;
            }
            if c.content()
                .dependencies(RelationKind::DependsOn)
                .all(|d| satisfied.contains(&d))
                && c.content().dependencies(RelationKind::Awaits).all(|d| {
                    satisfied.contains(&d)
                        || state
                            .claim(&d)
                            .is_some_and(|x| x.lifecycle().status.is_terminal())
                })
                && state
                    .monitors()
                    .filter(|m| m.owner == *id && m.released.is_none())
                    .all(|m| {
                        m.roots.iter().all(|p| match p {
                            WaitPredicate::Satisfied(c) => satisfied.contains(c),
                            WaitPredicate::Terminal(c) => {
                                satisfied.contains(c) || predicate(state, *p)
                            }
                            _ => predicate(state, *p),
                        })
                    })
            {
                if state.scratch(1).is_err() {
                    return satisfied;
                }
                satisfied.insert(*id);
                changed = true;
            }
        }
        if !changed {
            return satisfied;
        }
    }
}
pub(crate) fn predicate(state: &impl GraphRead, p: WaitPredicate) -> bool {
    match p {
        WaitPredicate::Satisfied(id) => state
            .claim(&id)
            .is_some_and(|c| c.lifecycle().status == ClaimStatus::Satisfied),
        WaitPredicate::Terminal(id) => state
            .claim(&id)
            .is_some_and(|c| c.lifecycle().status.is_terminal()),
        WaitPredicate::Released(id) => state.claim(&id).is_some_and(|c| c.lifecycle().released),
    }
}
pub(crate) fn check_acyclic(
    state: &impl GraphRead,
    kinds: &[RelationKind],
    limit: usize,
) -> Result<(), DomainOutcome> {
    let mut done = BTreeSet::new();
    let mut visits = 0usize;
    for start in state.claims().map(|(id, _)| id) {
        if done.contains(start) {
            continue;
        }
        state.scratch(1)?;
        let mut stack = vec![(*start, false)];
        let mut path = BTreeSet::new();
        while let Some((id, exit)) = stack.pop() {
            visits = visits
                .checked_add(1)
                .ok_or_else(|| refuse(ErrorCode::Capacity, "lineage traversal counter"))?;
            if visits > limit {
                return Err(refuse(ErrorCode::Capacity, "lineage traversal budget"));
            }
            if exit {
                path.remove(&id);
                state.scratch(1)?;
                done.insert(id);
                continue;
            }
            if path.contains(&id) {
                return Err(refuse(
                    ErrorCode::InvalidRelation,
                    "causation or correction lineage cycle",
                ));
            }
            if done.contains(&id) {
                continue;
            }
            state.scratch(2)?;
            path.insert(id);
            stack.push((id, true));
            let c = state
                .claim(&id)
                .ok_or_else(|| refuse(ErrorCode::InvalidRelation, "graph endpoint is missing"))?;
            for kind in kinds {
                for d in c.content().dependencies(*kind) {
                    state.scratch(1)?;
                    stack.push((d, false));
                }
            }
        }
    }
    Ok(())
}
fn neighbors(state: &impl GraphRead, id: ClaimId) -> Result<Vec<ClaimId>, DomainOutcome> {
    let c = state
        .claim(&id)
        .ok_or_else(|| refuse(ErrorCode::InvalidRelation, "graph endpoint is missing"))?;
    let mut result = Vec::new();
    for dependency in c
        .content()
        .dependencies(RelationKind::DependsOn)
        .chain(c.content().dependencies(RelationKind::Awaits))
        .filter(|id| {
            state
                .claim(id)
                .is_some_and(|claim| claim.lifecycle().status.is_active())
        })
    {
        state.scratch(1)?;
        result.push(dependency);
    }
    for m in state
        .monitors()
        .filter(|m| m.owner == id && m.released.is_none())
    {
        for root in &m.roots {
            if !predicate(state, *root) {
                state.scratch(1)?;
                result.push(match root {
                    WaitPredicate::Satisfied(id)
                    | WaitPredicate::Terminal(id)
                    | WaitPredicate::Released(id) => *id,
                })
            }
        }
    }
    result.sort();
    result.dedup();
    Ok(result)
}
fn reachable(
    state: &impl GraphRead,
    start: ClaimId,
    limit: &mut usize,
) -> Result<BTreeSet<ClaimId>, DomainOutcome> {
    let mut seen = BTreeSet::new();
    state.scratch(1)?;
    let mut stack = vec![start];
    while let Some(id) = stack.pop() {
        if seen.contains(&id) {
            continue;
        }
        state.scratch(1)?;
        seen.insert(id);
        if *limit == 0 {
            return Err(refuse(ErrorCode::Capacity, "SCC traversal budget"));
        }
        *limit = limit
            .checked_sub(1)
            .ok_or_else(|| refuse(ErrorCode::Capacity, "SCC traversal budget"))?;
        let neighbors = neighbors(state, id)?;
        state.scratch(neighbors.len())?;
        stack.extend(neighbors);
    }
    Ok(seen)
}
/// Brute-force mutual reachability reference; bounded by explicit edge visits.
pub(crate) fn cycle_containing(
    state: &impl GraphRead,
    start: ClaimId,
    mut limit: usize,
) -> Result<Option<BTreeSet<ClaimId>>, DomainOutcome> {
    let reachable_from = reachable(state, start, &mut limit)?;
    let mut component = BTreeSet::new();
    for node in reachable_from {
        if reachable(state, node, &mut limit)?.contains(&start) {
            state.scratch(1)?;
            component.insert(node);
        }
    }
    if component.len() > 1 || neighbors(state, start)?.contains(&start) {
        Ok(Some(component))
    } else {
        Ok(None)
    }
}
