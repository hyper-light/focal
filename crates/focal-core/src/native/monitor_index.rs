//! Retained monitor identities and direct, active reverse subscriptions.
//! Tombstones preserve global link allocation accounting; only live links occur
//! in a target's doubly linked chain. No graph closure or scope is invented here.
use super::prepare::{Extra, Extras, Scratch};
use super::*;
use focal_model::lifecycle::scope::{self, MonitorDisposition, Scope};
use focal_model::{Deadline, MonitorId, WaitPredicate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MonitorAllocation {
    pub owner: Binding,
    pub registered: SessionSeq,
    pub deadline: Deadline,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct MonitorHead {
    pub head: Option<MonitorId>,
    pub count: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MonitorLink {
    pub owner: ClaimId,
    pub registered: SessionSeq,
    pub stamp: SessionSeq,
    pub previous: Option<MonitorId>,
    pub next: Option<MonitorId>,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct MonitorChanges {
    pub monitors: usize,
    pub links: usize,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexBound {
    pub extra_rows: usize,
    pub temporary_bytes: usize,
    pub visits: usize,
    pub incoming_heap: usize,
}
pub(super) struct Subscriber<'a> {
    pub owner: &'a ClaimState,
    pub monitor: &'a Scope,
}
struct Visits(usize);
impl Visits {
    fn take(&mut self, n: usize) -> Result<(), ContractError> {
        self.0 = self.0.checked_sub(n).ok_or(ContractError::Capacity)?;
        Ok(())
    }
}
fn add(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_add(b).ok_or(ContractError::Capacity)
}
fn mul(a: usize, b: usize) -> Result<usize, ContractError> {
    a.checked_mul(b).ok_or(ContractError::Capacity)
}
fn target(root: WaitPredicate) -> ClaimId {
    match root {
        WaitPredicate::Satisfied(id)
        | WaitPredicate::Terminal(id)
        | WaitPredicate::Released(id) => id,
    }
}
fn stamp(scope: &Scope) -> SessionSeq {
    scope
        .last_rebinding()
        .map_or(scope.registered(), |change| change.cut.position)
}
fn same_owner(
    owner: &ClaimState,
    original: Binding,
    ledger: LedgerId,
) -> Result<(), ContractError> {
    let actual = owner.binding();
    if actual.ledger != ledger || original.ledger != ledger {
        return Err(ContractError::WrongLedger);
    }
    if actual.object != original.object
        || actual.content != original.content
        || original.object.is_zero()
        || original.revision.0 == 0
        || actual.revision < original.revision
    {
        return Err(ContractError::InvalidTarget);
    }
    Ok(())
}
fn monitor<'a>(
    owner: &'a ClaimState,
    id: MonitorId,
    visits: &mut Visits,
) -> Result<&'a Scope, ContractError> {
    let depth = owner
        .scopes()
        .limits()
        .scopes
        .checked_ilog2()
        .map(|n| n.checked_add(1).ok_or(ContractError::Capacity))
        .transpose()?
        .unwrap_or(0);
    visits.take(add(
        usize::try_from(depth).map_err(|_| ContractError::Capacity)?,
        1,
    )?)?;
    owner
        .scopes()
        .monitor(id)
        .ok_or(ContractError::InvalidTarget)
}
fn contains(scope: &Scope, id: ClaimId, visits: &mut Visits) -> Result<bool, ContractError> {
    visits.take(scope.roots().len())?;
    Ok(scope.roots().iter().any(|root| target(*root) == id))
}
/// Index reads use the candidate overlay before the immutable source. Charging
/// its admitted maximum also bounds Extras' duplicate-key scans during writes.
struct Read<'a, 'b> {
    view: &'a View<'b>,
    extras: &'a [Extra],
    owner: Option<&'a ClaimState>,
    owners: &'a [ClaimState],
    overlay: usize,
}
impl Read<'_, '_> {
    fn get(&self, key: Key, visits: &mut Visits) -> Result<Option<&Row>, ContractError> {
        visits.take(add(self.overlay, 1)?)?;
        Ok(self
            .extras
            .iter()
            .find(|extra| extra.key == key)
            .map(|extra| &extra.row)
            .or_else(|| self.view.get(key)))
    }
    fn head(&self, target: ClaimId, visits: &mut Visits) -> Result<MonitorHead, ContractError> {
        match self.get(Key::MonitorHead(target), visits)? {
            None => Ok(MonitorHead::default()),
            Some(Row::MonitorHead(head)) if head.head.is_some() == (head.count != 0) => Ok(*head),
            _ => Err(ContractError::InvalidManifest),
        }
    }
    fn link(
        &self,
        target: ClaimId,
        id: MonitorId,
        visits: &mut Visits,
    ) -> Result<MonitorLink, ContractError> {
        if id.is_zero() {
            return Err(ContractError::InvalidTarget);
        }
        match self.get(Key::MonitorLink(target, id), visits)? {
            Some(Row::MonitorLink(Some(link))) => Ok(*link),
            _ => Err(ContractError::InvalidManifest),
        }
    }
    fn subscriber(
        &self,
        target: ClaimId,
        id: MonitorId,
        visits: &mut Visits,
    ) -> Result<(Subscriber<'_>, MonitorLink), ContractError> {
        let link = self.link(target, id, visits)?;
        let Some(Row::Monitor(allocation)) = self.get(Key::Monitor(id), visits)? else {
            return Err(ContractError::MissingEvidence);
        };
        visits.take(add(self.owners.len(), 1)?)?;
        let owner = match self
            .owner
            .filter(|owner| owner.binding().object.0 == link.owner.0)
        {
            Some(owner) => owner,
            None => self
                .owners
                .iter()
                .find(|owner| owner.binding().object.0 == link.owner.0)
                .or_else(|| self.view.claim(link.owner))
                .ok_or(ContractError::MissingEvidence)?,
        };
        same_owner(owner, allocation.owner, self.view.ledger())?;
        let publication = if self.extras.is_empty() {
            self.view.prefix()
        } else {
            SessionSeq(
                self.view
                    .prefix()
                    .0
                    .checked_add(1)
                    .ok_or(ContractError::InvalidCut)?,
            )
        };
        if owner.created().0 == 0
            || owner.created() > self.view.prefix()
            || link.owner.0 != allocation.owner.object.0
            || link.registered != allocation.registered
            || allocation.registered.0 == 0
            || allocation.registered > publication
        {
            return Err(ContractError::InvalidCut);
        }
        let scope = monitor(owner, id, visits)?;
        if !scope.active()
            || scope.registered() != allocation.registered
            || scope.deadline() != allocation.deadline
            || stamp(scope) != link.stamp
            || !contains(scope, target, visits)?
        {
            return Err(ContractError::InvalidManifest);
        }
        Ok((
            Subscriber {
                owner,
                monitor: scope,
            },
            link,
        ))
    }
}
fn chain(
    read: &Read<'_, '_>,
    target: ClaimId,
    limits: NativeLimits,
    visits: &mut Visits,
    reread: bool,
    required: Option<MonitorId>,
) -> Result<MonitorHead, ContractError> {
    visits.take(1)?;
    let claim = read
        .view
        .claim(target)
        .ok_or(ContractError::InvalidTarget)?;
    same_owner(claim, claim.binding(), read.view.ledger())?;
    if claim.created().0 == 0 || claim.created() > read.view.prefix() {
        return Err(ContractError::InvalidCut);
    }
    let head = read.head(target, visits)?;
    if head.count > limits.monitors
        || head.count > limits.monitor_links
        || head.count > limits.plan_edges
    {
        return Err(ContractError::Capacity);
    }
    let mut next = head.head;
    let mut previous = None;
    let mut found = required.is_none();
    for _ in 0..head.count {
        let id = next.ok_or(ContractError::InvalidManifest)?;
        found |= required == Some(id);
        let before = visits.0;
        let (_, link) = read.subscriber(target, id, visits)?;
        if link.previous != previous || link.next == Some(id) {
            return Err(ContractError::InvalidManifest);
        }
        if reread {
            visits.take(
                before
                    .checked_sub(visits.0)
                    .ok_or(ContractError::Capacity)?,
            )?;
        }
        previous = Some(id);
        next = link.next;
    }
    if next.is_some() || !found {
        return Err(ContractError::InvalidManifest);
    }
    Ok(head)
}
struct Subscribers<'a, 'b> {
    view: &'a View<'b>,
    target: ClaimId,
    next: Option<MonitorId>,
    remaining: usize,
    error: Option<ContractError>,
}
/// Validate the entire chain before yielding; the same immutable source borrow
/// pins both passes. The allowance prices validation and every consumer reread.
pub(super) fn subscribers<'a, 'b>(
    view: &'a View<'b>,
    target: ClaimId,
    limits: NativeLimits,
) -> impl Iterator<Item = Result<Subscriber<'b>, ContractError>> + 'a {
    let read = Read {
        view,
        extras: &[],
        owner: None,
        owners: &[],
        overlay: 0,
    };
    let checked = chain(
        &read,
        target,
        limits,
        &mut Visits(limits.plan_edges),
        true,
        None,
    );
    match checked {
        Ok(head) => Subscribers {
            view,
            target,
            next: head.head,
            remaining: head.count,
            error: None,
        },
        Err(error) => Subscribers {
            view,
            target,
            next: None,
            remaining: 0,
            error: Some(error),
        },
    }
}
impl<'b> Iterator for Subscribers<'_, 'b> {
    type Item = Result<Subscriber<'b>, ContractError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        if self.remaining == 0 {
            return None;
        }
        let result = (|| {
            let id = self.next.ok_or(ContractError::InvalidManifest)?;
            let Some(Row::MonitorLink(Some(link))) =
                self.view.get(Key::MonitorLink(self.target, id))
            else {
                return Err(ContractError::InvalidManifest);
            };
            let owner = as_claim(self.view.get(Key::Claim(link.owner)))
                .ok_or(ContractError::MissingEvidence)?;
            let monitor = owner
                .scopes()
                .monitor(id)
                .ok_or(ContractError::MissingEvidence)?;
            self.next = link.next;
            self.remaining = self
                .remaining
                .checked_sub(1)
                .ok_or(ContractError::InvalidManifest)?;
            Ok(Subscriber { owner, monitor })
        })();
        if result.is_err() {
            self.remaining = 0;
            self.next = None;
        }
        Some(result)
    }
}
/// Charge both complete passes without allocating or exposing partial members.
#[cfg(test)]
fn check_subscribers(
    view: &View<'_>,
    target: ClaimId,
    limits: NativeLimits,
) -> Result<usize, ContractError> {
    let mut visits = Visits(limits.plan_edges);
    chain(
        &Read {
            view,
            extras: &[],
            owner: None,
            owners: &[],
            overlay: 0,
        },
        target,
        limits,
        &mut visits,
        true,
        None,
    )?;
    limits
        .plan_edges
        .checked_sub(visits.0)
        .ok_or(ContractError::Capacity)
}
fn unique(
    roots: &[WaitPredicate],
    index: usize,
    visits: &mut Visits,
) -> Result<bool, ContractError> {
    let root = roots.get(index).ok_or(ContractError::InvalidTarget)?;
    visits.take(add(index, 1)?)?;
    Ok(!roots
        .get(..index)
        .ok_or(ContractError::InvalidTarget)?
        .iter()
        .any(|prior| target(*prior) == target(*root)))
}
fn put(
    extras: &mut Extras,
    key: Key,
    row: Row,
    visits: &mut Visits,
    overlay: usize,
) -> Result<(), NativeError> {
    // Both lookup and Extras::push's duplicate scan are covered before mutation.
    visits.take(mul(add(overlay, 1)?, 2)?)?;
    if let Some(existing) = extras.rows.iter_mut().find(|extra| extra.key == key) {
        if existing.heap != 0 || existing.fact.is_some() {
            return Err(ContractError::InvalidManifest.into());
        }
        existing.row = row;
        Ok(())
    } else {
        extras.push(Extra {
            key,
            row,
            heap: 0,
            fact: None,
        })
    }
}
fn check_delta(
    before: &ClaimState,
    next: &ClaimState,
    event: scope::Event,
    view: &View<'_>,
    visits: &mut Visits,
) -> Result<MonitorId, ContractError> {
    before.binding().next()?.check(&next.binding())?;
    same_owner(before, before.binding(), view.ledger())?;
    let (id, cut) = match event {
        scope::Event::Registered { id, cut } | scope::Event::MonitorReleased { id, cut } => {
            (id, cut)
        }
        scope::Event::Rebound { id, change } => (id, change.cut),
        scope::Event::MonitorCancelled { id, cancellation } => (id, cancellation.cut),
        _ => return Err(ContractError::InvalidTransition),
    };
    if id.is_zero()
        || cut.cause == ContentHash([0; 32])
        || cut.position.0
            != view
                .prefix()
                .0
                .checked_add(1)
                .ok_or(ContractError::InvalidCut)?
    {
        return Err(ContractError::InvalidCut);
    }
    let a = before.scopes();
    let b = next.scopes();
    visits.take(add(a.children().len(), b.children().len())?)?;
    if a.limits() != b.limits()
        || a.children() != b.children()
        || a.release_cut() != b.release_cut()
    {
        return Err(ContractError::InvalidManifest);
    }
    let mut old = a.iter().filter(|scope| scope.id() != id);
    let mut new = b.iter().filter(|scope| scope.id() != id);
    loop {
        visits.take(2)?;
        match (old.next(), new.next()) {
            (None, None) => break,
            (Some(a), Some(b)) => {
                visits.take(add(a.roots().len(), b.roots().len())?)?;
                if a != b {
                    return Err(ContractError::InvalidManifest);
                }
            }
            _ => return Err(ContractError::InvalidManifest),
        }
    }
    let old = a.monitor(id);
    let new = monitor(next, id, visits)?;
    match event {
        scope::Event::Registered { cut, .. } => {
            if old.is_some()
                || !new.active()
                || new.registered() != cut.position
                || new.last_rebinding().is_some()
                || new.roots().is_empty()
                || new.deadline().timer.is_zero()
                || new.deadline().generation == 0
            {
                return Err(ContractError::InvalidManifest);
            }
        }
        _ => {
            let old = old.ok_or(ContractError::InvalidTarget)?;
            if !old.active()
                || old.registered() != new.registered()
                || old.deadline() != new.deadline()
            {
                return Err(ContractError::InvalidManifest);
            }
            match event {
                scope::Event::Rebound { change, .. } => {
                    if !new.active()
                        || new.last_rebinding() != Some(change)
                        || change.predecessor == change.successor
                        || !contains(old, change.predecessor, visits)?
                    {
                        return Err(ContractError::InvalidManifest);
                    }
                    visits.take(mul(
                        mul(add(old.roots().len(), 1)?, add(new.roots().len(), 1)?)?,
                        2,
                    )?)?;
                    let mapped = |root| match root {
                        WaitPredicate::Satisfied(id) if id == change.predecessor => {
                            WaitPredicate::Satisfied(change.successor)
                        }
                        WaitPredicate::Terminal(id) if id == change.predecessor => {
                            WaitPredicate::Terminal(change.successor)
                        }
                        WaitPredicate::Released(id) if id == change.predecessor => {
                            WaitPredicate::Released(change.successor)
                        }
                        root => root,
                    };
                    if old
                        .roots()
                        .iter()
                        .any(|root| !new.roots().contains(&mapped(*root)))
                        || new
                            .roots()
                            .iter()
                            .any(|root| !old.roots().iter().any(|prior| mapped(*prior) == *root))
                    {
                        return Err(ContractError::InvalidManifest);
                    }
                }
                scope::Event::MonitorReleased { cut, .. } => {
                    visits.take(add(old.roots().len(), new.roots().len())?)?;
                    if new.disposition() != Some(MonitorDisposition::Released(cut))
                        || old.roots() != new.roots()
                        || old.last_rebinding() != new.last_rebinding()
                    {
                        return Err(ContractError::InvalidManifest);
                    }
                }
                scope::Event::MonitorCancelled { cancellation, .. } => {
                    visits.take(add(old.roots().len(), new.roots().len())?)?;
                    if new.disposition() != Some(MonitorDisposition::Cancelled(cancellation))
                        || old.roots() != new.roots()
                        || old.last_rebinding() != new.last_rebinding()
                    {
                        return Err(ContractError::InvalidManifest);
                    }
                }
                _ => return Err(ContractError::InvalidTransition),
            }
        }
    }
    Ok(id)
}
fn read<'a, 'b>(
    view: &'a View<'b>,
    extras: &'a Extras,
    owner: &'a ClaimState,
    limits: NativeLimits,
) -> Read<'a, 'b> {
    Read {
        view,
        extras: &extras.rows,
        owner: Some(owner),
        owners: &[],
        overlay: limits.range.max_batch_entries,
    }
}
fn unlink(
    view: &View<'_>,
    owner: &ClaimState,
    target: ClaimId,
    id: MonitorId,
    limits: NativeLimits,
    extras: &mut Extras,
    visits: &mut Visits,
) -> Result<(), NativeError> {
    let (mut head, link, previous, next) = {
        let read = read(view, extras, owner, limits);
        let head = read.head(target, visits)?;
        let link = read.link(target, id, visits)?;
        if link.owner.0 != owner.binding().object.0
            || (link.previous.is_none() && head.head != Some(id))
        {
            return Err(ContractError::InvalidManifest.into());
        }
        let previous = link
            .previous
            .map(|previous| read.link(target, previous, visits))
            .transpose()?;
        let next = link
            .next
            .map(|next| read.link(target, next, visits))
            .transpose()?;
        if previous.is_some_and(|previous| previous.next != Some(id))
            || next.is_some_and(|next| next.previous != Some(id))
        {
            return Err(ContractError::InvalidManifest.into());
        }
        (head, link, previous, next)
    };
    head.count = head
        .count
        .checked_sub(1)
        .ok_or(ContractError::InvalidManifest)?;
    if link.previous.is_none() {
        head.head = link.next;
    }
    if head.head.is_some() != (head.count != 0) {
        return Err(ContractError::InvalidManifest.into());
    }
    let overlay = limits.range.max_batch_entries;
    if let (Some(id), Some(mut previous)) = (link.previous, previous) {
        previous.next = link.next;
        put(
            extras,
            Key::MonitorLink(target, id),
            Row::MonitorLink(Some(previous)),
            visits,
            overlay,
        )?;
    }
    if let (Some(id), Some(mut next)) = (link.next, next) {
        next.previous = link.previous;
        put(
            extras,
            Key::MonitorLink(target, id),
            Row::MonitorLink(Some(next)),
            visits,
            overlay,
        )?;
    }
    put(
        extras,
        Key::MonitorLink(target, id),
        Row::MonitorLink(None),
        visits,
        overlay,
    )?;
    put(
        extras,
        Key::MonitorHead(target),
        Row::MonitorHead(head),
        visits,
        overlay,
    )
}
fn insert(
    view: &View<'_>,
    owner: &ClaimState,
    scope: &Scope,
    target: ClaimId,
    limits: NativeLimits,
    extras: &mut Extras,
    visits: &mut Visits,
) -> Result<bool, NativeError> {
    let (mut head, old, fresh) = {
        let read = read(view, extras, owner, limits);
        let fresh = match read.get(Key::MonitorLink(target, scope.id()), visits)? {
            None => true,
            Some(Row::MonitorLink(None)) => false,
            _ => return Err(ContractError::InvalidManifest.into()),
        };
        let head = read.head(target, visits)?;
        let old = head
            .head
            .map(|id| read.link(target, id, visits))
            .transpose()?;
        if old.is_some_and(|old| old.previous.is_some()) {
            return Err(ContractError::InvalidManifest.into());
        }
        (head, old, fresh)
    };
    head.count = add(head.count, 1)?;
    if head.count > limits.monitors || head.count > limits.monitor_links {
        return Err(ContractError::Capacity.into());
    }
    let link = MonitorLink {
        owner: ClaimId(owner.binding().object.0),
        registered: scope.registered(),
        stamp: stamp(scope),
        previous: None,
        next: head.head,
    };
    let overlay = limits.range.max_batch_entries;
    if let (Some(id), Some(mut old)) = (head.head, old) {
        old.previous = Some(scope.id());
        put(
            extras,
            Key::MonitorLink(target, id),
            Row::MonitorLink(Some(old)),
            visits,
            overlay,
        )?;
    }
    head.head = Some(scope.id());
    put(
        extras,
        Key::MonitorLink(target, scope.id()),
        Row::MonitorLink(Some(link)),
        visits,
        overlay,
    )?;
    put(
        extras,
        Key::MonitorHead(target),
        Row::MonitorHead(head),
        visits,
        overlay,
    )?;
    Ok(fresh)
}
/// Stage one actual checked scope transition. The caller retains its model
/// capability and records the corresponding claim fact. Private Extras may be
/// discarded wholesale on any refusal; no persistent state is changed here.
pub(super) fn stage(
    view: &View<'_>,
    before: &ClaimState,
    next: &ClaimState,
    event: scope::Event,
    limits: NativeLimits,
    extras: &mut Extras,
    _scratch: &mut Scratch,
) -> Result<MonitorChanges, NativeError> {
    if extras.rows.len() > limits.range.max_batch_entries {
        return Err(ContractError::Capacity.into());
    }
    let mut visits = Visits(limits.plan_edges);
    let id = check_delta(before, next, event, view, &mut visits)?;
    let old = before.scopes().monitor(id);
    let new = next
        .scopes()
        .monitor(id)
        .ok_or(ContractError::InvalidTarget)?;
    let mut changes = MonitorChanges::default();
    let overlay = limits.range.max_batch_entries;
    match read(view, extras, before, limits).get(Key::Monitor(id), &mut visits)? {
        None if matches!(event, scope::Event::Registered { .. }) => {
            changes.monitors = 1;
            if add(view.meta().monitors, 1)? > limits.monitors {
                return Err(ContractError::Capacity.into());
            }
        }
        Some(Row::Monitor(allocation)) if !matches!(event, scope::Event::Registered { .. }) => {
            same_owner(before, allocation.owner, view.ledger())?;
            if allocation.registered != new.registered() || allocation.deadline != new.deadline() {
                return Err(ContractError::InvalidManifest.into());
            }
        }
        _ => return Err(ContractError::InvalidTarget.into()),
    }
    // Every affected old chain is validated before its links are edited. Source
    // overrides retain same-candidate earlier dispositions on this owner.
    if let Some(old) = old {
        for (index, root) in old.roots().iter().enumerate() {
            if !unique(old.roots(), index, &mut visits)? {
                continue;
            }
            let target = target(*root);
            chain(
                &read(view, extras, before, limits),
                target,
                limits,
                &mut visits,
                false,
                Some(id),
            )?;
            let link = read(view, extras, before, limits).link(target, id, &mut visits)?;
            if link.owner.0 != before.binding().object.0
                || link.registered != old.registered()
                || link.stamp != stamp(old)
            {
                return Err(ContractError::InvalidManifest.into());
            }
            if !new.active() || !contains(new, target, &mut visits)? {
                unlink(view, before, target, id, limits, extras, &mut visits)?;
            } else {
                put(
                    extras,
                    Key::MonitorLink(target, id),
                    Row::MonitorLink(Some(MonitorLink {
                        stamp: stamp(new),
                        ..link
                    })),
                    &mut visits,
                    overlay,
                )?;
            }
        }
    }
    if new.active() {
        for (index, root) in new.roots().iter().enumerate() {
            if !unique(new.roots(), index, &mut visits)? {
                continue;
            }
            let target = target(*root);
            if old
                .map(|old| contains(old, target, &mut visits))
                .transpose()?
                .unwrap_or(false)
            {
                continue;
            }
            chain(
                &read(view, extras, before, limits),
                target,
                limits,
                &mut visits,
                false,
                None,
            )?;
            if insert(view, before, new, target, limits, extras, &mut visits)? {
                changes.links = add(changes.links, 1)?;
            }
        }
    }
    if add(view.meta().monitor_links, changes.links)? > limits.monitor_links {
        return Err(ContractError::Capacity.into());
    }
    if changes.monitors != 0 {
        put(
            extras,
            Key::Monitor(id),
            Row::Monitor(MonitorAllocation {
                owner: before.binding(),
                registered: new.registered(),
                deadline: new.deadline(),
            }),
            &mut visits,
            overlay,
        )?;
    }
    Ok(changes)
}
/// Conservative release/cancellation staging bound. It includes complete
/// affected-chain validation and worst-case overlay/key scans, including two
/// neighbors per distinct root. Source discovery is charged by its own caller.
pub(super) fn release_bound(
    view: &View<'_>,
    owner: &ClaimState,
    scope: &Scope,
    limits: NativeLimits,
) -> Result<IndexBound, ContractError> {
    if !scope.active() || owner.scopes().monitor(scope.id()) != Some(scope) {
        return Err(ContractError::InvalidTarget);
    }
    let mut visits = Visits(limits.plan_edges);
    let overlay = limits.range.max_batch_entries;
    // Upper bound check_delta's complete unchanged-registry comparison and the
    // two named-scope lookups; actual child buffers are retained and unchanged.
    let mut registry = add(mul(owner.scopes().children().len(), 2)?, 32)?;
    for row in owner.scopes().iter() {
        registry = add(registry, add(mul(row.roots().len(), 2)?, 4)?)?;
    }
    let depth = owner
        .scopes()
        .limits()
        .scopes
        .checked_ilog2()
        .map(|n| n.checked_add(1).ok_or(ContractError::Capacity))
        .transpose()?
        .unwrap_or(0);
    visits.take(add(
        registry,
        mul(
            usize::try_from(depth).map_err(|_| ContractError::Capacity)?,
            2,
        )?,
    )?)?;
    let mut roots = 0usize;
    let read = Read {
        view,
        extras: &[],
        owner: None,
        owners: &[],
        overlay,
    };
    for (index, root) in scope.roots().iter().enumerate() {
        if !unique(scope.roots(), index, &mut visits)? {
            continue;
        }
        roots = add(roots, 1)?;
        chain(&read, target(*root), limits, &mut visits, false, None)?;
        visits.take(subscriber_visits(owner, scope)?)?;
        // Current link, head, neighbors, four writes and source allocation;
        // reserve more than the 14 actual per-target overlay scans.
        visits.take(mul(add(overlay, 1)?, 20)?)?;
    }
    Ok(IndexBound {
        extra_rows: mul(roots, 4)?,
        temporary_bytes: 0,
        visits: limits
            .plan_edges
            .checked_sub(visits.0)
            .ok_or(ContractError::Capacity)?,
        incoming_heap: 0,
    })
}

#[cfg(test)]
#[path = "monitor_index_tests.rs"]
mod tests;

/// Per yielded subscriber, including both validated and consumer passes. Empty
/// target setup costs two more visits. This composes with a shared graph budget.
pub(super) fn subscriber_visits(owner: &ClaimState, scope: &Scope) -> Result<usize, ContractError> {
    let depth = owner
        .scopes()
        .limits()
        .scopes
        .checked_ilog2()
        .map(|n| n.checked_add(1).ok_or(ContractError::Capacity))
        .transpose()?
        .unwrap_or(0);
    mul(
        add(
            add(
                4,
                usize::try_from(depth).map_err(|_| ContractError::Capacity)?,
            )?,
            scope.roots().len(),
        )?,
        2,
    )
}
#[path = "monitor_index_journal.rs"]
mod journal;
pub(super) use journal::{check_journal, replay_bytes};

/// Validate a forward-declared original membership against the complete reverse
/// chain. This also rejects an orphaned active scope when a target head is empty.
/// Proposed scope replacements use their private transition capability instead.
pub(super) fn check_member(
    view: &View<'_>,
    owner: &ClaimState,
    scope: &Scope,
    target: ClaimId,
    limits: NativeLimits,
) -> Result<usize, ContractError> {
    let mut visits = Visits(limits.plan_edges);
    visits.take(1)?;
    let actual = view
        .claim(ClaimId(owner.binding().object.0))
        .ok_or(ContractError::InvalidTarget)?;
    owner.binding().check(&actual.binding())?;
    let original = monitor(actual, scope.id(), &mut visits)?;
    visits.take(add(original.roots().len(), scope.roots().len())?)?;
    if original != scope || !scope.active() || !contains(scope, target, &mut visits)? {
        return Err(ContractError::InvalidManifest);
    }
    let read = Read {
        view,
        extras: &[],
        owner: None,
        owners: &[],
        overlay: 0,
    };
    chain(&read, target, limits, &mut visits, false, Some(scope.id()))?;
    let link = read.link(target, scope.id(), &mut visits)?;
    if link.owner.0 != owner.binding().object.0 {
        return Err(ContractError::InvalidManifest);
    }
    limits
        .plan_edges
        .checked_sub(visits.0)
        .ok_or(ContractError::Capacity)
}
