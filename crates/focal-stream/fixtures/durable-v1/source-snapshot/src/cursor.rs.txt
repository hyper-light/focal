use crate::StreamError;
use focal_model::{ClaimId, ContentHash, Delta, DeltaId, LedgerId, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConsumerId(pub [u8; 16]);
impl ConsumerId {
    pub const fn from_u128(value: u128) -> Self {
        Self(value.to_be_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConsumerKey {
    pub ledger: LedgerId,
    pub consumer: ConsumerId,
}

/// Within one sequence every Delta ordinal precedes Resolved. A resolved
/// position acknowledges *all* matching deltas through this complete prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PositionOffset {
    Delta(u32),
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position {
    pub ledger: LedgerId,
    pub sequence: SessionSeq,
    pub offset: PositionOffset,
}

impl Position {
    pub const fn origin(ledger: LedgerId) -> Self {
        Self::resolved(ledger, SessionSeq(0))
    }
    pub const fn resolved(ledger: LedgerId, sequence: SessionSeq) -> Self {
        Self {
            ledger,
            sequence,
            offset: PositionOffset::Resolved,
        }
    }
    pub const fn after_delta(id: DeltaId) -> Self {
        Self {
            ledger: id.ledger,
            sequence: id.sequence,
            offset: PositionOffset::Delta(id.ordinal),
        }
    }
    pub fn validate(self, ledger: LedgerId, published: SessionSeq) -> Result<(), StreamError> {
        if self.ledger != ledger {
            return Err(StreamError::WrongLedger);
        }
        if self.sequence > published {
            return Err(StreamError::CursorAhead);
        }
        if self.sequence.0 == 0 && self.offset != PositionOffset::Resolved {
            return Err(StreamError::Invalid("sequence zero has no delta ordinals"));
        }
        Ok(())
    }
    /// Complete sequences eligible for retirement while preserving this exact
    /// ordinal. Part of sequence S never authorizes retiring all of S.
    pub fn retention_prefix(self) -> SessionSeq {
        match self.offset {
            PositionOffset::Resolved => self.sequence,
            PositionOffset::Delta(_) => SessionSeq(self.sequence.0.saturating_sub(1)),
        }
    }
    pub fn ensure_same_ledger(self, other: Self) -> Result<(), StreamError> {
        if self.ledger != other.ledger {
            Err(StreamError::WrongLedger)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorToken {
    pub key: ConsumerKey,
    pub generation: u64,
    /// Hash of authenticated authority/query scope supplied by trusted ingress.
    pub scope: ContentHash,
    pub position: Position,
}

impl CursorToken {
    pub fn same_stream(self, other: Self) -> Result<(), StreamError> {
        if self.key.ledger != self.position.ledger
            || other.key.ledger != other.position.ledger
            || self.key.ledger != other.key.ledger
            || self.position.ledger != other.position.ledger
        {
            return Err(StreamError::WrongLedger);
        }
        if self.key != other.key {
            return Err(StreamError::WrongConsumer);
        }
        if self.generation != other.generation {
            return Err(StreamError::WrongGeneration);
        }
        if self.scope != other.scope {
            return Err(StreamError::WrongScope);
        }
        Ok(())
    }
    pub(crate) fn at(self, position: Position) -> Self {
        Self { position, ..self }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaFilter {
    All,
    Claims(BTreeSet<ClaimId>),
}

impl DeltaFilter {
    pub fn matches(&self, delta: &Delta) -> bool {
        match self {
            Self::All => true,
            Self::Claims(claims) => delta.claim.is_some_and(|claim| claims.contains(&claim)),
        }
    }
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::All => 0,
            Self::Claims(claims) => claims.len(),
        }
    }
    pub(crate) fn charge(&self) -> Result<usize, StreamError> {
        crate::mul(
            self.len(),
            crate::add(
                crate::mul(16, crate::add(size_of::<ClaimId>(), size_of::<usize>())?)?,
                crate::ALLOCATOR_OVERHEAD,
            )?,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResyncReason {
    HistoryExpired,
    LeaseExpired,
    SlowConsumer,
    SnapshotExpired,
    ExplicitReset,
}
