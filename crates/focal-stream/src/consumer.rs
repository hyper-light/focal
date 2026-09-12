use crate::{CursorToken, Position, StreamError, StreamEvent};
use serde::{Deserialize, Serialize};

/// Persist this beside the consumer's effects/projection in the same local
/// transaction. A server acknowledgment comes only after that local commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerCheckpoint {
    pub schema: u16,
    pub cursor: CursorToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerDecision {
    Duplicate,
    ApplyDelta,
    AdvanceResolved,
}

#[derive(Debug, Clone, Copy)]
pub struct PreparedConsumerAdvance {
    base: ConsumerCheckpoint,
    next: ConsumerCheckpoint,
    pub decision: ConsumerDecision,
}
impl PreparedConsumerAdvance {
    pub fn checkpoint(&self) -> ConsumerCheckpoint {
        self.next
    }
}

impl ConsumerCheckpoint {
    pub fn new(cursor: CursorToken) -> Result<Self, StreamError> {
        if cursor.key.ledger != cursor.position.ledger {
            return Err(StreamError::WrongLedger);
        }
        if cursor.generation == 0 {
            return Err(StreamError::WrongGeneration);
        }
        Ok(Self { schema: 1, cursor })
    }

    /// A snapshot reset is a projection replacement with a new generation;
    /// callers must not replay irreversible effects while installing that seed.
    pub fn from_installed_seed(cursor: CursorToken) -> Result<Self, StreamError> {
        if !matches!(cursor.position.offset, crate::PositionOffset::Resolved) {
            return Err(StreamError::Invalid(
                "installed seed requires resolved prefix",
            ));
        }
        Self::new(cursor)
    }

    pub fn prepare(&self, event: &StreamEvent) -> Result<PreparedConsumerAdvance, StreamError> {
        if self.schema != 1 {
            return Err(StreamError::Invalid("unknown consumer checkpoint schema"));
        }
        let (cursor, decision, delta_id) = match event {
            StreamEvent::Delta { cursor, delta } => {
                if delta.schema != 1 || delta.id.sequence.0 == 0 {
                    return Err(StreamError::SourceViolation(
                        "invalid delta schema or sequence",
                    ));
                }
                if cursor.position != Position::after_delta(delta.id) {
                    return Err(StreamError::SourceViolation(
                        "event cursor does not identify delta",
                    ));
                }
                (*cursor, ConsumerDecision::ApplyDelta, Some(delta.id))
            }
            StreamEvent::Resolved { cursor } => {
                if !matches!(cursor.position.offset, crate::PositionOffset::Resolved) {
                    return Err(StreamError::SourceViolation(
                        "resolved event carries partial position",
                    ));
                }
                (*cursor, ConsumerDecision::AdvanceResolved, None)
            }
            StreamEvent::Resync { cursor, reason, .. } => {
                self.cursor.same_stream(*cursor)?;
                return Err(StreamError::ResyncRequired(*reason));
            }
        };
        self.cursor.same_stream(cursor)?;
        if cursor.position <= self.cursor.position {
            return Ok(PreparedConsumerAdvance {
                base: *self,
                next: *self,
                decision: ConsumerDecision::Duplicate,
            });
        }
        // A forward delta must be contiguous within its sequence, mirroring the
        // server's validate_delta: without this a skipped ordinal (5,1) -> (5,3)
        // silently corrupts the local projection with the missing effect, and a
        // new sequence must begin at ordinal 0. Applied only to non-duplicate
        // deltas (duplicates were short-circuited above).
        if let Some(id) = delta_id {
            let previous = self.cursor.position;
            if id.sequence == previous.sequence {
                let crate::PositionOffset::Delta(ordinal) = previous.offset else {
                    return Err(StreamError::SourceViolation(
                        "delta after resolved transaction",
                    ));
                };
                if ordinal.checked_add(1) != Some(id.ordinal) {
                    return Err(StreamError::SourceViolation("delta ordinal gap"));
                }
            } else if id.ordinal != 0 {
                return Err(StreamError::SourceViolation(
                    "transaction starts after ordinal zero",
                ));
            }
        }
        Ok(PreparedConsumerAdvance {
            base: *self,
            next: Self { schema: 1, cursor },
            decision,
        })
    }

    /// Publish only after effects and checkpoint were atomically persisted by
    /// the embedding consumer. Dropping a prepared advance leaves state intact.
    pub fn publish(&mut self, prepared: PreparedConsumerAdvance) -> Result<(), StreamError> {
        if *self != prepared.base {
            return Err(StreamError::StalePreparation);
        }
        *self = prepared.next;
        Ok(())
    }
}
