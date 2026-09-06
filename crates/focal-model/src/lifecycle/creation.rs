//! Mandatory native creation over the owner's complete effective prefix.
//!
//! The owner reserves `Limits::bytes` before preparation and publishes every
//! returned replacement together. The lookup is a trusted owner interface, never
//! a transport-supplied list of allegedly complete ancestors. Core also binds its
//! candidate to the exact base/predecessor root, including negative observations.
use super::claim::{ClaimCut, ClaimDefinition, ClaimState};
use super::memory as bytes;
use super::succession::{CorrectionKind, Lineage};
use super::{Binding, ContractError, Principal, scope};
use crate::{Cause, ClaimId, ContentHash, LedgerId, ObjectId, ReceiptFence, SessionSeq};

pub trait EffectiveClaims {
    fn ledger(&self) -> LedgerId;
    fn prefix(&self) -> SessionSeq;
    fn claim(&self, id: ClaimId) -> Option<&ClaimState>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owner {
    pub expected: Binding,
    pub receipt: Option<ReceiptFence>,
}

#[derive(Debug)]
pub struct Proposal {
    pub definition: ClaimDefinition,
    /// Mandatory for an owned child, forbidden for a root. The fence is supplied
    /// by the request, not inferred from the current receipt during admission.
    pub owner: Option<Owner>,
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub nodes: usize,
    pub edge_visits: usize,
    pub bytes: usize,
}

/// Complete native request identity. This is deliberately separate from the V1
/// command codec and from any future persisted native format. Creation revision
/// and sequence are assigned by the publishing owner and are not request fields.
pub fn intent_fingerprint(proposals: &[Proposal]) -> Result<ContentHash, ContractError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"focal/native/creation-intent/1");
    hash_count(&mut hash, proposals.len())?;
    for proposal in proposals {
        let definition = &proposal.definition;
        hash_binding(&mut hash, definition.binding, false);
        hash.update(&definition.issuer.0);
        hash.update(&definition.subject.0);
        match definition.deadline {
            Some(deadline) => {
                hash.update(&[1]);
                hash.update(&deadline.timer.0);
                hash.update(&deadline.generation.to_be_bytes());
                hash.update(&deadline.at.to_be_bytes());
            }
            None => {
                hash.update(&[0]);
            }
        }
        hash.update(&definition.max_responses.to_be_bytes());
        hash_count(&mut hash, definition.graph.obligations().len())?;
        for obligation in definition.graph.obligations() {
            hash.update(&[match obligation.kind {
                super::graph::Kind::DependsOn => 0,
                super::graph::Kind::Awaits => 1,
            }]);
            hash.update(&obligation.target.0);
        }
        hash_binding(&mut hash, definition.lineage.binding(), false);
        match definition.lineage.cause() {
            Cause::Root(id) => {
                hash.update(&[0]);
                hash.update(&id.0);
            }
            Cause::Claim(id) => {
                hash.update(&[1]);
                hash.update(&id.0);
            }
        }
        hash_count(&mut hash, definition.lineage.corrections().len())?;
        for correction in definition.lineage.corrections() {
            hash.update(&[match correction.kind {
                CorrectionKind::Supersedes => 0,
                CorrectionKind::Amends => 1,
            }]);
            // Lineage construction already restricts predecessors to Claim.
            hash.update(&correction.predecessor.ledger.tenant.0);
            hash.update(&correction.predecessor.ledger.session.0);
            hash.update(&correction.predecessor.id.0);
        }
        hash.update(&definition.acceptance.intent_fingerprint().0);
        hash_count(&mut hash, definition.scope_limits.scopes)?;
        hash_count(&mut hash, definition.scope_limits.roots)?;
        hash_count(&mut hash, definition.scope_limits.children)?;
        match proposal.owner {
            Some(owner) => {
                hash.update(&[1]);
                hash_binding(&mut hash, owner.expected, true);
                match owner.receipt {
                    Some(receipt) => {
                        hash.update(&[1]);
                        hash.update(&receipt.receipt.0);
                        hash.update(&receipt.epoch.to_be_bytes());
                    }
                    None => {
                        hash.update(&[0]);
                    }
                }
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    Ok(ContentHash(*hash.finalize().as_bytes()))
}

fn hash_binding(hash: &mut blake3::Hasher, binding: Binding, revision: bool) {
    hash.update(&binding.ledger.tenant.0);
    hash.update(&binding.ledger.session.0);
    hash.update(&binding.object.0);
    hash.update(&binding.content.0);
    if revision {
        hash.update(&binding.revision.0.to_be_bytes());
    }
}
fn hash_count(hash: &mut blake3::Hasher, count: usize) -> Result<(), ContractError> {
    hash.update(
        &u64::try_from(count)
            .map_err(|_| ContractError::Capacity)?
            .to_be_bytes(),
    );
    Ok(())
}

#[derive(Debug)]
pub struct CreationPlan {
    rows: Vec<ClaimState>,
    reads: Vec<Binding>,
    absent: Vec<ClaimId>,
    cut: ClaimCut,
}

/// A predecessor control fact produced only by an actual checked creation plan.
/// Compound child registration may also advance the replacement's revision.
#[derive(Debug, Clone, Copy)]
pub struct Supersession {
    previous: Binding,
    replacement: Binding,
    cut: ClaimCut,
}
impl Supersession {
    pub fn previous(&self) -> Binding {
        self.previous
    }
    pub fn replacement(&self) -> Binding {
        self.replacement
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
    pub(super) fn check(&self, previous: &ClaimState) -> Result<(), ContractError> {
        previous.binding().check(&self.previous)?;
        if previous.status().is_terminal() || self.replacement.revision <= self.previous.revision {
            return Err(ContractError::InvalidTransition);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Spec {
    binding: Binding,
    owner: Option<Owner>,
}
struct Node<'a> {
    row: &'a ClaimState,
    color: u8,
}

const ALLOCATION: usize = 4 * std::mem::size_of::<usize>();

fn vector_charge<T>(capacity: usize) -> Result<usize, ContractError> {
    bytes::add(
        bytes::array::<T>(capacity)?,
        if capacity == 0 { 0 } else { ALLOCATION },
    )
}

fn reserve_accounted<T>(
    requested: usize,
    used: &mut usize,
    limit: usize,
) -> Result<Vec<T>, ContractError> {
    #[cfg(test)]
    let allocation_request = requested
        .checked_add(test_overage())
        .ok_or(ContractError::Capacity)?;
    #[cfg(not(test))]
    let allocation_request = requested;
    let values = bytes::reserve(allocation_request)?;
    // The complete requested envelope was charged before the first allocation.
    // Reconcile allocator-reported capacity against that envelope immediately,
    // while this provisional vector can still be dropped without any install.
    *used = used
        .checked_sub(vector_charge::<T>(requested)?)
        .ok_or(ContractError::Capacity)?;
    charge(used, vector_charge::<T>(values.capacity())?, limit)?;
    Ok(values)
}

/// Conservative fixed-capacity scratch and inline row buffers. The definition
/// heaps and copied effective rows are charged separately before allocation.
pub fn scratch_bytes(limits: Limits) -> Result<usize, ContractError> {
    let mut charge = std::mem::size_of::<CreationPlan>();
    for value in [
        vector_charge::<ClaimState>(limits.nodes)?,
        vector_charge::<Node<'_>>(limits.nodes)?,
        vector_charge::<(ObjectId, usize)>(limits.nodes)?,
        vector_charge::<(usize, usize)>(limits.nodes)?,
        vector_charge::<Spec>(limits.nodes)?,
        vector_charge::<Binding>(limits.nodes)?,
        vector_charge::<ClaimId>(limits.nodes)?,
    ] {
        charge = bytes::add(charge, value)?;
    }
    Ok(charge)
}

fn heap_charge(row: &ClaimState) -> Result<usize, ContractError> {
    bytes::add(
        row.retained_heap_bytes()?,
        row.heap_allocations()?
            .checked_mul(ALLOCATION)
            .ok_or(ContractError::Capacity)?,
    )
}

fn definition_heap(definition: &ClaimDefinition) -> Result<usize, ContractError> {
    let mut charge = 0usize;
    for (heap, allocations) in [
        (
            definition.graph.retained_heap_bytes()?,
            definition.graph.heap_allocations()?,
        ),
        (
            definition.lineage.retained_heap_bytes()?,
            definition.lineage.heap_allocations()?,
        ),
        (
            definition.acceptance.retained_heap_bytes()?,
            definition.acceptance.heap_allocations()?,
        ),
    ] {
        charge = bytes::add(charge, heap)?;
        charge = bytes::add(
            charge,
            allocations
                .checked_mul(ALLOCATION)
                .ok_or(ContractError::Capacity)?,
        )?;
    }
    Ok(charge)
}

fn charge(used: &mut usize, value: usize, limit: usize) -> Result<(), ContractError> {
    *used = bytes::add(*used, value)?;
    bytes::fits(*used, limit)
}

fn index(rows: &[ClaimState], id: ObjectId) -> Result<usize, usize> {
    rows.binary_search_by_key(&id, |row| row.binding().object)
}

fn target(lineage: &Lineage, edge: usize) -> Option<ClaimId> {
    let offset = match lineage.cause() {
        Cause::Claim(id) if edge == 0 => return Some(*id),
        Cause::Claim(_) => edge.checked_sub(1)?,
        Cause::Root(_) => edge,
    };
    lineage
        .corrections()
        .get(offset)
        .map(|relation| ClaimId(relation.predecessor.id.0))
}

fn effective_row(
    effective: &impl EffectiveClaims,
    id: ClaimId,
) -> Result<&ClaimState, ContractError> {
    let row = effective.claim(id).ok_or(ContractError::InvalidTarget)?;
    if row.binding().object.0 != id.0 {
        return Err(ContractError::WrongObject);
    }
    if row.binding().ledger != effective.ledger() {
        return Err(ContractError::WrongLedger);
    }
    if row.created().0 == 0
        || row.created() > effective.prefix()
        || row
            .local_sealed_at()
            .is_some_and(|cut| cut > effective.prefix())
        || row
            .scopes()
            .release_cut()
            .is_some_and(|cut| cut.position > effective.prefix())
    {
        return Err(ContractError::InvalidCut);
    }
    Ok(row)
}

/// One iterative DFS visits every proposed component. A sorted auxiliary index
/// provides binary lookup; insertion shifts are bounded by `nodes`. No recursive
/// stack, unbounded hash table, or second mutable object authority is introduced.
fn lineage_reads(
    rows: &[ClaimState],
    effective: &impl EffectiveClaims,
    limits: Limits,
    used: &mut usize,
) -> Result<Vec<Binding>, ContractError> {
    let mut nodes = reserve_accounted::<Node<'_>>(limits.nodes, used, limits.bytes)?;
    let mut ids = reserve_accounted::<(ObjectId, usize)>(limits.nodes, used, limits.bytes)?;
    let mut stack = reserve_accounted::<(usize, usize)>(limits.nodes, used, limits.bytes)?;
    for row in rows {
        ids.push((row.binding().object, nodes.len()));
        nodes.push(Node { row, color: 0 });
    }
    let mut visits = 0usize;
    for start in 0..rows.len() {
        if nodes.get(start).ok_or(ContractError::InvalidTarget)?.color != 0 {
            continue;
        }
        nodes
            .get_mut(start)
            .ok_or(ContractError::InvalidTarget)?
            .color = 1;
        stack.push((start, 0));
        while let Some((node, edge)) = stack.last().copied() {
            let source = nodes.get(node).ok_or(ContractError::InvalidTarget)?.row;
            let Some(next_id) = target(source.lineage(), edge) else {
                nodes
                    .get_mut(node)
                    .ok_or(ContractError::InvalidTarget)?
                    .color = 2;
                stack.pop();
                continue;
            };
            visits = visits.checked_add(1).ok_or(ContractError::Capacity)?;
            if visits > limits.edge_visits {
                return Err(ContractError::Capacity);
            }
            stack.last_mut().ok_or(ContractError::InvalidTarget)?.1 =
                edge.checked_add(1).ok_or(ContractError::Capacity)?;
            let id = ObjectId(next_id.0);
            let next = match ids.binary_search_by_key(&id, |value| value.0) {
                Ok(position) => ids.get(position).ok_or(ContractError::InvalidTarget)?.1,
                Err(position) => {
                    if nodes.len() == limits.nodes {
                        return Err(ContractError::Capacity);
                    }
                    let row = effective_row(effective, next_id)?;
                    let next = nodes.len();
                    nodes.push(Node { row, color: 0 });
                    ids.insert(position, (id, next));
                    next
                }
            };
            let destination = nodes.get(next).ok_or(ContractError::InvalidTarget)?.row;
            if destination.created() > source.created() {
                return Err(ContractError::InvalidCut);
            }
            // Existing native rows sharing a creation prefix came from the same
            // prior atomic publication. New rows at this cut exist only in rows.
            if let Cause::Claim(parent) = source.lineage().cause()
                && edge == 0
                && source.created() <= effective.prefix()
            {
                let owned = destination
                    .scopes()
                    .children()
                    .iter()
                    .find(|child| child.id().0 == source.binding().object.0)
                    .ok_or(ContractError::InvalidManifest)?;
                if parent.0 != destination.binding().object.0
                    || owned.registered() != source.created()
                {
                    return Err(ContractError::InvalidManifest);
                }
                Binding {
                    revision: owned.binding().revision,
                    ..source.binding()
                }
                .check(&owned.binding())?;
            }
            match nodes.get(next).ok_or(ContractError::InvalidTarget)?.color {
                0 => {
                    nodes
                        .get_mut(next)
                        .ok_or(ContractError::InvalidTarget)?
                        .color = 1;
                    stack.push((next, 0));
                }
                1 => return Err(ContractError::InvalidTarget),
                2 => {}
                _ => return Err(ContractError::InvalidTarget),
            }
        }
    }
    let mut reads = reserve_accounted(limits.nodes, used, limits.bytes)?;
    for node in nodes {
        if node.row.created() <= effective.prefix() {
            reads.push(node.row.binding());
        }
    }
    reads.sort_unstable_by_key(|binding| binding.object);
    Ok(reads)
}

impl CreationPlan {
    pub fn prepare(
        principal: Principal,
        proposals: Vec<Proposal>,
        effective: &impl EffectiveClaims,
        cut: ClaimCut,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        if proposals.is_empty() || proposals.len() > limits.nodes {
            return Err(ContractError::Capacity);
        }
        if effective.prefix().0.checked_add(1) != Some(cut.position.0) {
            return Err(ContractError::InvalidCut);
        }
        let mut used = scratch_bytes(limits)?;
        charge(
            &mut used,
            vector_charge::<Proposal>(proposals.capacity())?,
            limits.bytes,
        )?;
        for proposal in &proposals {
            charge(
                &mut used,
                definition_heap(&proposal.definition)?,
                limits.bytes,
            )?;
        }
        let mut rows = reserve_accounted(limits.nodes, &mut used, limits.bytes)?;
        let mut specs = reserve_accounted(limits.nodes, &mut used, limits.bytes)?;
        let mut absent = reserve_accounted(limits.nodes, &mut used, limits.bytes)?;
        for proposal in proposals {
            let definition = proposal.definition;
            if definition.binding.ledger != effective.ledger() {
                return Err(ContractError::WrongLedger);
            }
            if definition.created != cut.position || definition.binding.revision.0 != 1 {
                return Err(ContractError::InvalidCut);
            }
            let id = ClaimId(definition.binding.object.0);
            if effective.claim(id).is_some() {
                return Err(ContractError::InvalidTarget);
            }
            match (definition.lineage.cause(), proposal.owner) {
                (Cause::Root(_), None) => {}
                (Cause::Claim(parent), Some(owner)) if parent.0 == owner.expected.object.0 => {}
                _ => return Err(ContractError::InvalidTarget),
            }
            specs.push(Spec {
                binding: definition.binding,
                owner: proposal.owner,
            });
            absent.push(id);
            rows.push(ClaimState::generate_defined(principal, definition)?);
        }
        rows.sort_unstable_by_key(|row| row.binding().object);
        specs.sort_unstable_by_key(|row| row.binding.object);
        absent.sort_unstable();
        if absent
            .windows(2)
            .any(|pair| matches!(pair, [a,b] if a == b))
        {
            return Err(ContractError::InvalidTarget);
        }
        let reads = lineage_reads(&rows, effective, limits, &mut used)?;
        // Verify every request fence against the initial effective/proposed row
        // before any private replacement advances its revision.
        for spec in &specs {
            if let Some(owner) = spec.owner {
                let parent = match index(&rows, owner.expected.object) {
                    Ok(position) => rows.get(position).ok_or(ContractError::InvalidTarget)?,
                    Err(_) => effective_row(effective, ClaimId(owner.expected.object.0))?,
                };
                parent.binding().check(&owner.expected)?;
                if parent.receipt().map(|value| value.fence) != owner.receipt {
                    return Err(ContractError::StaleReceipt);
                }
                if principal != Principal::Actor(parent.issuer()) {
                    principal.require_actor(
                        parent.receipt().ok_or(ContractError::StaleReceipt)?.holder,
                    )?;
                }
            }
        }
        for spec in &specs {
            if let Some(owner) = spec.owner {
                Self::copy_effective(
                    &mut rows,
                    owner.expected.object,
                    effective,
                    &mut used,
                    limits,
                )?;
                let parent = index(&rows, owner.expected.object)
                    .map_err(|_| ContractError::InvalidTarget)?;
                let child =
                    index(&rows, spec.binding.object).map_err(|_| ContractError::InvalidTarget)?;
                let (parent, child) = two_rows(&mut rows, parent, child)?;
                // Registry preparation temporarily holds its old and replacement
                // buffers. Charge a full replacement plus the new owned-child
                // slot and its two pinned reads before entering that allocator.
                let transient = scope_copy_charge(parent)?;
                bytes::fits(bytes::add(used, transient)?, limits.bytes)?;
                let old_heap = heap_charge(parent)?;
                let expected = parent.binding();
                let transition =
                    scope::Registry::prepare_child(parent, child, &expected, owner.receipt, cut)?;
                #[cfg(test)]
                let transition = test_grow_transition(transition)?;
                bytes::fits(
                    bytes::add(used, transition.allocation_charge()?)?,
                    limits.bytes,
                )?;
                parent.apply_scope(&expected, transition, &[child])?;
                // Installation drops the old registry and temporary read set.
                // Keep only the installed heap charged for the next child.
                used = used.checked_sub(old_heap).ok_or(ContractError::Capacity)?;
                charge(&mut used, heap_charge(parent)?, limits.bytes)?;
            }
        }
        // Every declared Supersedes relation has its consequence in this same
        // plan. Amends is an immutable link and never changes its predecessor.
        for spec in &specs {
            let mut relation = 0usize;
            loop {
                let successor = rows
                    .get(
                        index(&rows, spec.binding.object)
                            .map_err(|_| ContractError::InvalidTarget)?,
                    )
                    .ok_or(ContractError::InvalidTarget)?;
                let Some(correction) = successor.lineage().corrections().get(relation).copied()
                else {
                    break;
                };
                relation = relation.checked_add(1).ok_or(ContractError::Capacity)?;
                if correction.kind != CorrectionKind::Supersedes {
                    continue;
                }
                let predecessor = effective_row(effective, ClaimId(correction.predecessor.id.0))?;
                principal.require_actor(predecessor.issuer())?;
                if predecessor.subject() != successor.subject()
                    || predecessor.issuer() != successor.issuer()
                {
                    return Err(ContractError::InvalidTarget);
                }
                if predecessor.created() >= cut.position {
                    return Err(ContractError::InvalidCut);
                }
                if predecessor.status().is_terminal() {
                    continue;
                }
                Self::copy_effective(
                    &mut rows,
                    correction.predecessor.id,
                    effective,
                    &mut used,
                    limits,
                )?;
                let predecessor_index = index(&rows, correction.predecessor.id)
                    .map_err(|_| ContractError::InvalidTarget)?;
                let predecessor = rows
                    .get_mut(predecessor_index)
                    .ok_or(ContractError::InvalidTarget)?;
                let expected = predecessor.binding();
                predecessor.supersede_verified(&expected, cut)?;
            }
        }
        let mut actual = std::mem::size_of::<Self>();
        for charge in [
            vector_charge::<ClaimState>(rows.capacity())?,
            vector_charge::<Spec>(specs.capacity())?,
            vector_charge::<Binding>(reads.capacity())?,
            vector_charge::<ClaimId>(absent.capacity())?,
        ] {
            actual = bytes::add(actual, charge)?;
        }
        bytes::fits(actual, limits.bytes)?;
        for row in &rows {
            charge(&mut actual, heap_charge(row)?, limits.bytes)?;
        }
        Ok(Self {
            rows,
            reads,
            absent,
            cut,
        })
    }

    fn copy_effective(
        rows: &mut Vec<ClaimState>,
        id: ObjectId,
        effective: &impl EffectiveClaims,
        used: &mut usize,
        limits: Limits,
    ) -> Result<(), ContractError> {
        let Err(position) = index(rows, id) else {
            return Ok(());
        };
        if rows.len() == limits.nodes {
            return Err(ContractError::Capacity);
        }
        let original = effective_row(effective, ClaimId(id.0))?;
        let overhead = original
            .copy_heap_allocations()?
            .checked_mul(ALLOCATION)
            .ok_or(ContractError::Capacity)?;
        charge(
            used,
            bytes::add(original.copy_heap_bytes()?, overhead)?,
            limits.bytes,
        )?;
        let copy = original.try_copy(original.copy_charge()?)?;
        rows.insert(position, copy);
        Ok(())
    }

    pub fn rows(&self) -> &[ClaimState] {
        &self.rows
    }
    pub fn reads(&self) -> &[Binding] {
        &self.reads
    }
    pub fn absent(&self) -> &[ClaimId] {
        &self.absent
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
    pub fn supersession(&self, previous: Binding) -> Result<Option<Supersession>, ContractError> {
        self.reads
            .iter()
            .find(|read| read.object == previous.object)
            .ok_or(ContractError::InvalidTarget)?
            .check(&previous)?;
        let Some(replacement) = self
            .rows
            .iter()
            .find(|row| row.binding().object == previous.object)
        else {
            return Ok(None);
        };
        if replacement.status() != crate::ClaimStatus::Superseded {
            return Ok(None);
        }
        if replacement.terminal_cut() != Some(super::claim::ClaimTerminalCut::Explicit(self.cut)) {
            return Err(ContractError::InvalidCut);
        }
        Ok(Some(Supersession {
            previous,
            replacement: replacement.binding(),
            cut: self.cut,
        }))
    }
    pub fn into_rows(self) -> Vec<ClaimState> {
        self.rows
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        let mut charge = std::mem::size_of::<Self>();
        for allocation in [
            vector_charge::<ClaimState>(self.rows.capacity())?,
            vector_charge::<Binding>(self.reads.capacity())?,
            vector_charge::<ClaimId>(self.absent.capacity())?,
        ] {
            charge = bytes::add(charge, allocation)?;
        }
        for row in &self.rows {
            charge = bytes::add(charge, heap_charge(row)?)?;
        }
        Ok(charge)
    }
}

fn scope_copy_charge(parent: &ClaimState) -> Result<usize, ContractError> {
    let registry = parent.scopes();
    let mut charge = bytes::add(
        std::mem::size_of::<scope::Transition>(),
        registry.copy_heap_bytes()?,
    )?;
    charge = bytes::add(charge, std::mem::size_of::<scope::OwnedChild>())?;
    charge = bytes::add(charge, vector_charge::<Binding>(2)?)?;
    bytes::add(
        charge,
        bytes::add(registry.copy_heap_allocations()?, 1)?
            .checked_mul(ALLOCATION)
            .ok_or(ContractError::Capacity)?,
    )
}

#[cfg(test)]
std::thread_local! {
    static RESERVATION_OVERAGE: std::cell::Cell<Option<(usize, usize)>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn test_overage() -> usize {
    RESERVATION_OVERAGE.with(|state| match state.get() {
        Some((0, extra)) => {
            state.set(None);
            extra
        }
        Some((remaining, extra)) => {
            state.set(Some((remaining - 1, extra)));
            0
        }
        None => 0,
    })
}

#[cfg(test)]
fn test_grow_transition(
    mut transition: scope::Transition,
) -> Result<scope::Transition, ContractError> {
    transition.test_extra_read_capacity(test_overage())?;
    Ok(transition)
}

#[cfg(test)]
fn with_reservation_overage<T>(after: usize, extra: usize, action: impl FnOnce() -> T) -> T {
    struct Restore(Option<(usize, usize)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            RESERVATION_OVERAGE.with(|state| state.set(self.0));
        }
    }
    let _restore = Restore(RESERVATION_OVERAGE.with(|state| state.replace(Some((after, extra)))));
    action()
}

fn two_rows(
    rows: &mut [ClaimState],
    parent: usize,
    child: usize,
) -> Result<(&mut ClaimState, &ClaimState), ContractError> {
    if parent == child {
        return Err(ContractError::InvalidTarget);
    }
    if parent < child {
        let (left, right) = rows
            .split_at_mut_checked(child)
            .ok_or(ContractError::InvalidTarget)?;
        Ok((
            left.get_mut(parent).ok_or(ContractError::InvalidTarget)?,
            right.first().ok_or(ContractError::InvalidTarget)?,
        ))
    } else {
        let (left, right) = rows
            .split_at_mut_checked(parent)
            .ok_or(ContractError::InvalidTarget)?;
        Ok((
            right.first_mut().ok_or(ContractError::InvalidTarget)?,
            left.get(child).ok_or(ContractError::InvalidTarget)?,
        ))
    }
}

#[cfg(test)]
#[path = "creation_tests.rs"]
mod tests;
