//! Derived reverse membership for immutable DependsOn/Awaits declarations.
//! Creation publishes links and their final heads with the actual claim rows.
//! Iteration checks the complete finite chain before exposing any dependent.
use super::prepare::{Extra, Extras, Scratch};
use super::*;
use focal_model::lifecycle::graph::{Kind, Obligation};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct IncomingHead {
    pub head: Option<ClaimId>,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IncomingLink {
    pub next: Option<ClaimId>,
}

struct Visits(usize);
impl Visits {
    fn take(&mut self, amount: usize) -> Result<(), ContractError> {
        self.0 = self.0.checked_sub(amount).ok_or(ContractError::Capacity)?;
        Ok(())
    }
}

fn identity(claim: &ClaimState, id: ClaimId, view: &View<'_>) -> Result<(), ContractError> {
    if claim.binding().ledger != view.ledger() {
        return Err(ContractError::WrongLedger);
    }
    if claim.binding().object.0 != id.0 || id.is_zero() || claim.binding().revision.0 == 0 {
        return Err(ContractError::InvalidTarget);
    }
    Ok(())
}

fn dependent<'b>(
    view: &View<'b>,
    id: ClaimId,
    target: ClaimId,
    visits: &mut Visits,
) -> Result<(&'b ClaimState, IncomingLink), ContractError> {
    visits.take(2)?;
    let claim = as_claim(view.get(Key::Claim(id))).ok_or(ContractError::MissingEvidence)?;
    identity(claim, id, view)?;
    if claim.created().0 == 0 || claim.created() > view.prefix() {
        return Err(ContractError::InvalidCut);
    }
    declares(claim, target, visits)?;
    let Some(Row::IncomingLink(link)) = view.get(Key::IncomingLink(target, id)) else {
        return Err(ContractError::InvalidManifest);
    };
    Ok((claim, *link))
}

fn declares(claim: &ClaimState, target: ClaimId, visits: &mut Visits) -> Result<(), ContractError> {
    let obligations = claim.graph().obligations();
    // The private immutable declaration already guarantees sorted unique keys.
    // Charge both bounded binary searches, including the unsuccessful kind.
    let depth = match obligations.len().checked_ilog2() {
        Some(depth) => depth.checked_add(1).ok_or(ContractError::Capacity)?,
        None => 0,
    };
    visits.take(
        usize::try_from(depth)
            .map_err(|_| ContractError::Capacity)?
            .checked_mul(2)
            .ok_or(ContractError::Capacity)?,
    )?;
    if obligations
        .binary_search(&Obligation {
            kind: Kind::DependsOn,
            target,
        })
        .is_ok()
        || obligations
            .binary_search(&Obligation {
                kind: Kind::Awaits,
                target,
            })
            .is_ok()
    {
        Ok(())
    } else {
        Err(ContractError::InvalidTarget)
    }
}

fn head(view: &View<'_>, target: ClaimId) -> Result<IncomingHead, ContractError> {
    match view.get(Key::IncomingHead(target)) {
        None => Ok(IncomingHead::default()),
        Some(Row::IncomingHead(head)) if head.head.is_some() == (head.count != 0) => Ok(*head),
        _ => Err(ContractError::InvalidManifest),
    }
}

fn check_chain(
    view: &View<'_>,
    target: ClaimId,
    head: IncomingHead,
    limits: NativeLimits,
    visits: &mut Visits,
) -> Result<(), ContractError> {
    if head.count > limits.plan_nodes || head.count > limits.plan_edges {
        return Err(ContractError::Capacity);
    }
    let mut next = head.head;
    for _ in 0..head.count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        let before = visits.0;
        let (_, link) = dependent(view, id, target, visits)?;
        // A consumer may read every validated row again. Reserve that work now
        // so a valid chain cannot exhaust its visit allowance halfway through.
        visits.take(
            before
                .checked_sub(visits.0)
                .ok_or(ContractError::Capacity)?,
        )?;
        next = link.next;
    }
    if next.is_some() {
        Err(ContractError::InvalidManifest)
    } else {
        Ok(())
    }
}

struct Incoming<'a, 'b> {
    view: &'a View<'b>,
    target: ClaimId,
    next: Option<ClaimId>,
    remaining: usize,
    error: Option<ContractError>,
}

/// The source borrow pins the same committed or pending effective owner prefix
/// throughout discovery. No target filter or caller-authored membership is used.
pub(super) fn incoming<'a, 'b>(
    view: &'a View<'b>,
    target: ClaimId,
    limits: NativeLimits,
) -> impl Iterator<Item = Result<&'b ClaimState, ContractError>> + 'a {
    let checked = (|| {
        let mut visits = Visits(limits.plan_edges);
        visits.take(2)?;
        let claim = view.claim(target).ok_or(ContractError::InvalidTarget)?;
        identity(claim, target, view)?;
        if claim.created().0 == 0 || claim.created() > view.prefix() {
            return Err(ContractError::InvalidCut);
        }
        let head = head(view, target)?;
        check_chain(view, target, head, limits, &mut visits)?;
        Ok(head)
    })();
    match checked {
        Ok(head) => Incoming {
            view,
            target,
            next: head.head,
            remaining: head.count,
            error: None,
        },
        Err(error) => Incoming {
            view,
            target,
            next: None,
            remaining: 0,
            error: Some(error),
        },
    }
}

impl<'b> Iterator for Incoming<'_, 'b> {
    type Item = Result<&'b ClaimState, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        if self.remaining == 0 {
            return None;
        }
        let result = (|| {
            let id = self.next.ok_or(ContractError::InvalidManifest)?;
            let Some(Row::IncomingLink(link)) = self.view.get(Key::IncomingLink(self.target, id))
            else {
                return Err(ContractError::InvalidManifest);
            };
            let claim =
                as_claim(self.view.get(Key::Claim(id))).ok_or(ContractError::MissingEvidence)?;
            self.remaining = self
                .remaining
                .checked_sub(1)
                .ok_or(ContractError::InvalidManifest)?;
            self.next = link.next;
            Ok(claim)
        })();
        if result.is_err() {
            self.remaining = 0;
            self.next = None;
        }
        Some(result)
    }
}

struct HeadPlan {
    target: ClaimId,
    first: usize,
    end: usize,
    original: Option<ClaimId>,
    result: IncomingHead,
}

fn staged_claim<'a>(
    rows: &'a [ClaimState],
    index: &[(ClaimId, usize)],
    id: ClaimId,
) -> Option<&'a ClaimState> {
    index
        .binary_search_by_key(&id, |row| row.0)
        .ok()
        .and_then(|position| index.get(position))
        .and_then(|(_, position)| rows.get(*position))
}

/// Stage only new declarations. Modified parents/predecessors keep their exact
/// immutable graph and existing index membership. All same-target additions are
/// combined before emitting Extras, so a batch never emits duplicate head keys.
pub(super) fn stage_created(
    rows: &[ClaimState],
    view: &View<'_>,
    extras: &mut Extras,
    scratch: &mut Scratch,
    limits: NativeLimits,
) -> Result<(), NativeError> {
    if rows.len() > limits.plan_nodes {
        return Err(ContractError::Capacity.into());
    }
    let mut visits = Visits(limits.plan_edges);
    let mut index = scratch.reserve::<(ClaimId, usize)>(rows.len())?;
    let mut edge_count = 0usize;
    let created = SessionSeq(
        view.prefix()
            .0
            .checked_add(1)
            .ok_or(ContractError::InvalidCut)?,
    );
    for (position, row) in rows.iter().enumerate() {
        visits.take(1)?;
        let id = ClaimId(row.binding().object.0);
        identity(row, id, view)?;
        index.push((id, position));
        match view.get(Key::Claim(id)) {
            Some(Row::Claim(old)) => {
                let old = old.claim().ok_or(ContractError::MissingEvidence)?;
                visits.take(row.graph().obligations().len())?;
                if old.graph() != row.graph() {
                    return Err(ContractError::InvalidPolicy.into());
                }
            }
            None => {
                if row.created() != created {
                    return Err(ContractError::InvalidCut.into());
                }
                edge_count = edge_count
                    .checked_add(row.graph().obligations().len())
                    .ok_or(ContractError::Capacity)?;
                if edge_count > limits.plan_edges {
                    return Err(ContractError::Capacity.into());
                }
            }
            Some(_) => return Err(ContractError::InvalidTarget.into()),
        }
    }
    index.sort_unstable_by_key(|row| row.0);
    if index
        .windows(2)
        .any(|pair| matches!(pair, [a, b] if a.0 == b.0))
    {
        return Err(ContractError::InvalidManifest.into());
    }
    let mut edges = scratch.reserve::<(ClaimId, ClaimId)>(edge_count)?;
    for row in rows {
        let id = ClaimId(row.binding().object.0);
        if view.get(Key::Claim(id)).is_some() {
            continue;
        }
        for obligation in row.graph().obligations() {
            visits.take(1)?;
            let target = staged_claim(rows, &index, obligation.target)
                .or_else(|| view.claim(obligation.target))
                .ok_or(ContractError::InvalidTarget)?;
            identity(target, obligation.target, view)?;
            if target.created().0 == 0 || target.created() > created {
                return Err(ContractError::InvalidCut.into());
            }
            edges.push((obligation.target, id));
        }
    }
    edges.sort_unstable();
    edges.dedup();
    let mut heads = scratch.reserve::<HeadPlan>(edges.len())?;
    let mut first = 0usize;
    while let Some((target, _)) = edges.get(first).copied() {
        if heads.len() >= limits.plan_nodes {
            return Err(ContractError::Capacity.into());
        }
        visits.take(2)?;
        let original = head(view, target)?;
        if view.claim(target).is_none() && original != IncomingHead::default() {
            return Err(ContractError::InvalidManifest.into());
        }
        check_chain(view, target, original, limits, &mut visits)?;
        let mut result = original;
        let mut end = first;
        while let Some((found, dependent)) = edges.get(end).copied() {
            if found != target {
                break;
            }
            visits.take(1)?;
            if view.get(Key::IncomingLink(target, dependent)).is_some() {
                return Err(ContractError::InvalidManifest.into());
            }
            let row = staged_claim(rows, &index, dependent).ok_or(ContractError::InvalidTarget)?;
            let before = visits.0;
            declares(row, target, &mut visits)?;
            let checks = before
                .checked_sub(visits.0)
                .ok_or(ContractError::Capacity)?;
            // Include future two-pass source/link reads for each new member.
            visits.take(checks.checked_add(4).ok_or(ContractError::Capacity)?)?;
            result.count = result.count.checked_add(1).ok_or(ContractError::Capacity)?;
            if result.count > limits.plan_nodes {
                return Err(ContractError::Capacity.into());
            }
            result.head = Some(dependent);
            end = end.checked_add(1).ok_or(ContractError::Capacity)?;
        }
        heads.push(HeadPlan {
            target,
            first,
            end,
            original: original.head,
            result,
        });
        first = end;
    }
    // Index keys cannot collide with declarations/fences already staged by
    // Create. Validate that boundary before the first derived-index insertion.
    for extra in &extras.rows {
        visits.take(1)?;
        if matches!(extra.key, Key::IncomingHead(_) | Key::IncomingLink(_, _)) {
            return Err(ContractError::InvalidManifest.into());
        }
    }
    for head in heads {
        let mut next = head.original;
        for (_, dependent) in edges
            .get(head.first..head.end)
            .ok_or(ContractError::InvalidManifest)?
        {
            extras.push(Extra {
                key: Key::IncomingLink(head.target, *dependent),
                row: Row::IncomingLink(IncomingLink { next }),
                heap: 0,
                fact: None,
            })?;
            next = Some(*dependent);
        }
        extras.push(Extra {
            key: Key::IncomingHead(head.target),
            row: Row::IncomingHead(head.result),
            heap: 0,
            fact: None,
        })?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "incoming_graph_tests.rs"]
mod tests;
