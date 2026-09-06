//! Complete owned-tree cancellation over the sole owner's effective claims.
//! Plans borrow immutable source rows. The publishing owner reserves the plan's
//! byte bound, copies changed rows, checks the effective prefix, and publishes
//! every replacement and outcome atomically. These tokens release no scopes.
use super::claim::{ClaimCut, ClaimState, ClaimTerminalCut};
use super::creation::EffectiveClaims;
use super::{Binding, ContractError, Principal};
use crate::{Cause, ClaimId, LedgerId, SessionSeq};

const ALLOCATION: usize = 4 * size_of::<usize>();

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub nodes: usize,
    pub edge_visits: usize,
    pub bytes: usize,
}

/// Private derived authority for one actual row of a verified ownership tree.
/// Terminal rows remain observations: their original cuts and seals survive.
#[derive(Debug)]
pub struct Cancellation<'a> {
    original: &'a ClaimState,
    next: Option<Binding>,
    cut: ClaimCut,
}
impl Cancellation<'_> {
    pub fn claim(&self) -> &ClaimState {
        self.original
    }
    pub fn binding(&self) -> Binding {
        self.original.binding()
    }
    pub fn next_binding(&self) -> Option<Binding> {
        self.next
    }
    pub fn changes_state(&self) -> bool {
        self.next.is_some()
    }
    pub fn cut(&self) -> ClaimCut {
        self.cut
    }
    pub(super) fn check(&self, current: &ClaimState) -> Result<(), ContractError> {
        current.binding().check(&self.original.binding())?;
        if current != self.original {
            return Err(ContractError::ContentConflict);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CancellationPlan<'a> {
    ledger: LedgerId,
    prefix: SessionSeq,
    root: Binding,
    rows: Vec<Cancellation<'a>>,
}
impl<'a> CancellationPlan<'a> {
    /// Conservative total plan charge, including its retained vector capacity.
    /// The caller holds this reservation before invoking `prepare`.
    pub fn construction_charge(limits: Limits) -> Result<usize, ContractError> {
        if limits.nodes == 0 {
            return Err(ContractError::Capacity);
        }
        let charge = limits
            .nodes
            .checked_mul(size_of::<Cancellation<'a>>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(ALLOCATION))
            .ok_or(ContractError::Capacity)?;
        if charge > limits.bytes {
            return Err(ContractError::Capacity);
        }
        Ok(charge)
    }

    pub fn prepare(
        view: &'a impl EffectiveClaims,
        expected_root: Binding,
        principal: Principal,
        cut: ClaimCut,
        limits: Limits,
    ) -> Result<Self, ContractError> {
        Self::construction_charge(limits)?;
        if view.ledger() != expected_root.ledger {
            return Err(ContractError::WrongLedger);
        }
        if view.prefix().0.checked_add(1) != Some(cut.position.0) || cut.position.0 == 0 {
            return Err(ContractError::InvalidCut);
        }
        let root_id = ClaimId(expected_root.object.0);
        let root = view.claim(root_id).ok_or(ContractError::InvalidTarget)?;
        root.binding().check(&expected_root)?;
        principal.require_actor(root.issuer())?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(limits.nodes)
            .map_err(|_| ContractError::Capacity)?;
        let mut plan = Self {
            ledger: view.ledger(),
            prefix: view.prefix(),
            root: expected_root,
            rows,
        };
        if plan.retained_bytes()? > limits.bytes {
            return Err(ContractError::Capacity);
        }
        plan.push(root, cut, limits)?;
        let mut cursor = 0usize;
        let mut visits = 0usize;
        while cursor < plan.rows.len() {
            let parent = plan
                .rows
                .get(cursor)
                .ok_or(ContractError::InvalidTarget)?
                .original;
            let parent_id = ClaimId(parent.binding().object.0);
            let mut previous = None;
            for owned in parent.scopes().children() {
                visits = visits.checked_add(1).ok_or(ContractError::Capacity)?;
                if visits > limits.edge_visits {
                    return Err(ContractError::Capacity);
                }
                if previous.is_some_and(|id| id >= owned.id()) || owned.id() == root_id {
                    return Err(ContractError::InvalidTarget);
                }
                previous = Some(owned.id());
                let child = view.claim(owned.id()).ok_or(ContractError::InvalidTarget)?;
                let actual = child.binding();
                Binding {
                    revision: owned.binding().revision,
                    ..actual
                }
                .check(&owned.binding())?;
                if actual.revision < owned.binding().revision {
                    return Err(ContractError::StaleRevision);
                }
                if child.lineage().cause() != &Cause::Claim(parent_id) {
                    return Err(ContractError::InvalidTarget);
                }
                if owned.registered() != child.created() || child.created() < parent.created() {
                    return Err(ContractError::InvalidCut);
                }
                // Each child's immutable Cause names exactly this parent and
                // each parent's registry is strictly unique. Therefore any
                // reachable cycle must reenter the selected root (rejected
                // above); no separate allocated visited set is required.
                plan.push(child, cut, limits)?;
            }
            cursor = cursor.checked_add(1).ok_or(ContractError::Capacity)?;
        }
        Ok(plan)
    }

    fn push(
        &mut self,
        claim: &'a ClaimState,
        cut: ClaimCut,
        limits: Limits,
    ) -> Result<(), ContractError> {
        if self.rows.len() >= limits.nodes {
            return Err(ContractError::Capacity);
        }
        if claim.binding().ledger != self.ledger {
            return Err(ContractError::WrongLedger);
        }
        if claim.created() > self.prefix
            || claim
                .local_sealed_at()
                .is_some_and(|position| position > self.prefix)
        {
            return Err(ContractError::InvalidCut);
        }
        if let Some(terminal) = claim.terminal_cut() {
            let position = match terminal {
                ClaimTerminalCut::Explicit(value) => value.position,
                ClaimTerminalCut::Required(value) => value.sequence(),
                ClaimTerminalCut::Graph(value) => value.sequence(),
            };
            if position > self.prefix {
                return Err(ContractError::InvalidCut);
            }
        }
        self.rows.push(Cancellation {
            original: claim,
            next: claim.cancellation_binding(cut)?,
            cut,
        });
        Ok(())
    }

    pub fn root(&self) -> Binding {
        self.root
    }
    pub fn prefix(&self) -> SessionSeq {
        self.prefix
    }
    pub fn transitions(&self) -> &[Cancellation<'a>] {
        &self.rows
    }
    pub fn retained_bytes(&self) -> Result<usize, ContractError> {
        self.rows
            .capacity()
            .checked_mul(size_of::<Cancellation<'a>>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(ALLOCATION))
            .ok_or(ContractError::Capacity)
    }

    /// Recheck every immutable source row and the effective prefix before any
    /// owner publication; newly registered pending children invalidate the plan.
    pub fn check(&self, view: &impl EffectiveClaims) -> Result<(), ContractError> {
        if view.ledger() != self.ledger {
            return Err(ContractError::WrongLedger);
        }
        if view.prefix() != self.prefix {
            return Err(ContractError::StaleRevision);
        }
        for transition in &self.rows {
            let current = view
                .claim(ClaimId(transition.binding().object.0))
                .ok_or(ContractError::InvalidTarget)?;
            transition.check(current)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "ownership_tests.rs"]
mod tests;
