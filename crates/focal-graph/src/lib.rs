#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::unreachable,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        clippy::disallowed_macros
    )
)]
//! Atomic paged domain objects and derived indexes. The serial Core remains an oracle.
mod projection;
mod read;
#[cfg(test)]
mod tests;
mod validation_results;
use focal_core::{CoreView, RowPatch, State};
use focal_memory::*;
use focal_model::*;
pub use projection::reference_charge;
pub use read::*;
use serde::{Deserialize, Serialize};
pub use validation_results::ValidationResultCandidate;

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("memory: {0}")]
    Memory(#[from] MemoryError),
    #[error("codec: {0}")]
    Codec(#[from] postcard::Error),
    #[error("graph session or prefix differs from domain state")]
    Prefix,
    #[error("derived index differs from canonical domain state")]
    IndexMismatch,
    #[error("graph counter overflow")]
    Overflow,
}

/// Canonical relation edges and schema-defined family associations remain distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GraphRelation {
    Authored(RelationKind),
    Requirement,
    TestamentOf,
    Evidence,
    ArtifactInput,
    ValidationOf,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum GraphKey {
    Object(ObjectKind, ObjectId),
    ByClaim(ClaimId, ObjectKind, ObjectId),
    Identity(ObjectKind, ContentHash),
    Forward(ObjectRef, RelationTarget, GraphRelation),
    Reverse(RelationTarget, ObjectRef, GraphRelation),
    Lifecycle(ClaimStatus, ClaimId),
    Deadline(u64, ClaimId, TimerId, u64),
    Required(ClaimId, ValidationPhase, ValidationId),
    End,
    /// Appended rows; the previous End sentinel still terminates old indexes.
    ValidationResult(ValidationId, ValidationResultPosition),
    ValidationResultsEnd,
}
impl GraphKey {
    pub fn object(reference: ObjectRef) -> Self {
        Self::Object(reference.kind, reference.id)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphObject {
    Claim(Claim),
    Testament(Testament),
    Validation(Validation),
    Artifact(Artifact),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source: ObjectRef,
    pub target: RelationTarget,
    pub relation: GraphRelation,
    pub introduced: SessionSeq,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphValue {
    Object(Box<GraphObject>),
    Reference(ObjectRef),
    Edge(GraphEdge),
    ValidationResult(Box<ValidationResult>),
}

#[derive(Debug, Clone, Copy)]
pub struct GraphConfig {
    pub range: RangeConfig,
}
impl Default for GraphConfig {
    fn default() -> Self {
        Self {
            range: RangeConfig {
                max_batch_entries: 65_536,
                ..RangeConfig::default()
            },
        }
    }
}

pub struct PreparedGraph {
    range: PreparedRange<GraphKey, GraphValue>,
}
impl PreparedGraph {
    pub fn base(&self) -> SessionSeq {
        SessionSeq(self.range.base_prefix())
    }
    pub fn sequence(&self) -> SessionSeq {
        SessionSeq(self.range.prefix())
    }
}

pub struct GraphStore {
    ledger: LedgerId,
    rows: RangeStore<GraphKey, GraphValue>,
    budget: MemoryBudget,
    config: GraphConfig,
}
impl GraphStore {
    pub fn from_state(
        state: &State,
        incarnation: RangeId,
        config: GraphConfig,
        budget: MemoryBudget,
    ) -> Result<Self, GraphError> {
        let bytes = reference_charge(state)?
            .checked_mul(2)
            .ok_or(GraphError::Overflow)?;
        let _staging = budget.reserve(BudgetKind::Recovery, BudgetLane::Completion, bytes)?;
        let entries = projection::project(state)?;
        let rows = RangeStore::from_entries(
            incarnation,
            state.sequence.0,
            config.range,
            budget.clone(),
            entries.into_values(),
        )?;
        Ok(Self {
            ledger: state.ledger,
            rows,
            budget,
            config,
        })
    }
    pub fn ledger(&self) -> LedgerId {
        self.ledger
    }
    pub fn sequence(&self) -> SessionSeq {
        SessionSeq(self.rows.prefix())
    }
    pub fn stats(&self) -> RangeStats {
        self.rows.stats()
    }
    pub fn memory_stats(&self) -> BudgetStats {
        self.budget.stats()
    }
    pub fn budget(&self) -> MemoryBudget {
        self.budget.clone()
    }
    pub fn config(&self) -> GraphConfig {
        self.config
    }
    pub fn get(&self, reference: ObjectRef) -> Result<Option<&GraphObject>, GraphError> {
        if reference.ledger != self.ledger {
            return Err(GraphError::Prefix);
        }
        Ok(match self.rows.get(&GraphKey::object(reference)) {
            Some(GraphValue::Object(object)) => Some(object.as_ref()),
            _ => None,
        })
    }
    /// Only changed canonical records/associations are expanded into index writes.
    /// Comparing the reference maps is still O(session state); page writes are COW.
    pub fn prepare_transition(
        &self,
        previous: &State,
        next: &State,
        after: Option<&PreparedGraph>,
        lane: BudgetLane,
    ) -> Result<PreparedGraph, GraphError> {
        let base = after.map_or(self.sequence(), PreparedGraph::sequence);
        if previous.ledger != self.ledger
            || next.ledger != self.ledger
            || previous.sequence != base
            || next.sequence.0 != base.0.checked_add(1).ok_or(GraphError::Overflow)?
        {
            return Err(GraphError::Prefix);
        }
        // Accounts the temporary write map before any cloned rows are constructed.
        let _staging = self.budget.reserve(
            BudgetKind::Pending,
            lane,
            reference_charge(next)?
                .checked_mul(2)
                .ok_or(GraphError::Overflow)?,
        )?;
        let changes = projection::changes(previous, next)?;
        let range = match after {
            Some(after) => self
                .rows
                .prepare_after(&after.range, next.sequence.0, changes, lane)?,
            None => self.rows.prepare_batch(next.sequence.0, changes, lane)?,
        };
        Ok(PreparedGraph { range })
    }
    /// Expand exactly the reducer's owned row updates into object/index pages.
    /// The immutable before view includes earlier pending or epoch row versions.
    pub fn prepare_patch(
        &self,
        before: CoreView<'_>,
        patch: RowPatch<'_>,
        after: Option<&PreparedGraph>,
        lane: BudgetLane,
    ) -> Result<PreparedGraph, GraphError> {
        let base = after.map_or(self.sequence(), PreparedGraph::sequence);
        if before.ledger() != self.ledger
            || patch.ledger() != self.ledger
            || before.sequence() != base
            || patch.base() != base
            || base.0.checked_add(1) != Some(patch.sequence().0)
        {
            return Err(GraphError::Prefix);
        }
        let _staging = self.budget.reserve(
            BudgetKind::Pending,
            lane,
            projection::patch_charge(before, patch)?,
        )?;
        let changes = projection::patch_changes(before, patch)?;
        let range = match after {
            Some(after) => {
                self.rows
                    .prepare_after(&after.range, patch.sequence().0, changes, lane)?
            }
            None => self.rows.prepare_batch(patch.sequence().0, changes, lane)?,
        };
        Ok(PreparedGraph { range })
    }
    /// Validate every root/provenance link before any domain or graph mutation.
    pub fn validate_publication<'a>(
        &'a self,
        prepared: impl IntoIterator<Item = &'a PreparedGraph>,
    ) -> Result<(), GraphError> {
        self.rows
            .validate_chain(prepared.into_iter().map(|graph| &graph.range))?;
        Ok(())
    }
    /// One allocation-free root swap publishes every object/index at a common prefix.
    pub fn publish(&mut self, prepared: PreparedGraph) -> Result<(), GraphError> {
        self.rows.publish(prepared.range)?;
        Ok(())
    }
    pub fn snapshot(&mut self, now: u64, ttl: u64) -> Result<GraphSnapshot, GraphError> {
        let lease = self.rows.pin(now, ttl)?;
        Ok(GraphSnapshot::new(
            self.ledger,
            lease,
            self.budget.clone(),
            self.config.range,
        ))
    }
    pub fn release_snapshot(&mut self, snapshot: &GraphSnapshot) -> Result<(), GraphError> {
        self.rows.release(snapshot.lease())?;
        Ok(())
    }
    pub fn advance_clock(&mut self, now: u64) -> Result<usize, GraphError> {
        Ok(self.rows.advance_clock(now)?)
    }
    pub fn audit(&self, state: &State) -> Result<(), GraphError> {
        if state.ledger != self.ledger || state.sequence != self.sequence() {
            return Err(GraphError::Prefix);
        }
        let _staging = self.budget.reserve(
            BudgetKind::Recovery,
            BudgetLane::Completion,
            reference_charge(state)?
                .checked_mul(2)
                .ok_or(GraphError::Overflow)?,
        )?;
        let expected = projection::project(state)?;
        if expected.len() != self.rows.len()
            || self
                .rows
                .entries()
                .any(|entry| expected.get(&entry.key) != Some(entry))
        {
            return Err(GraphError::IndexMismatch);
        }
        Ok(())
    }
}
