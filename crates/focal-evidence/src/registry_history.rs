//! Schema identity across upgrades (REMAINING §13 instruction 3, R8.3): a
//! schema is an immutable descriptor named by its hash; the registry records
//! when each entered service, which schema superseded it, and the byte bound
//! a report under it may reach. A superseded schema keeps its identity and
//! bound, so a report that cited it resolves exactly as it did; only a
//! schema's *current* successor is offered to new work. The registry is
//! bounded and charged to the budget it is built in; verification itself is
//! the built-in contract of the two shipped schemas (`schemas.rs`), and any
//! other registered descriptor answers its bound but is refused verification
//! as unsupported rather than guessed at.
use crate::{BuiltinSchemaError, NativeSchemaVerifier, RegistryError};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, MemoryError};
use focal_model::{ContentHash, SessionSeq};
use std::collections::BTreeMap;

/// The most bytes one schema descriptor may hold.
pub const MAX_SCHEMA_DESCRIPTOR_BYTES: usize = 4096;
const RECORD_OVERHEAD: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRecord {
    pub descriptor: Vec<u8>,
    pub maximum_bytes: usize,
    pub introduced_at: SessionSeq,
    pub superseded_by: Option<ContentHash>,
    pub superseded_at: Option<SessionSeq>,
}

pub struct SchemaRegistry {
    entries: BTreeMap<ContentHash, SchemaRecord>,
    capacity: usize,
    _allocation: Option<Allocation>,
}
impl SchemaRegistry {
    /// An empty registry of at most `capacity` schemas.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity,
            _allocation: None,
        }
    }
    /// A registry whose whole capacity is charged to `budget` up front, so
    /// no registration allocates past what was admitted.
    pub fn new_in(capacity: usize, budget: &MemoryBudget) -> Result<Self, MemoryError> {
        let bytes = capacity
            .checked_mul(MAX_SCHEMA_DESCRIPTOR_BYTES.saturating_add(RECORD_OVERHEAD))
            .ok_or(MemoryError::AllocationFailed)?;
        let allocation = budget
            .reserve(BudgetKind::Control, BudgetLane::Ordinary, bytes)?
            .commit();
        Ok(Self {
            entries: BTreeMap::new(),
            capacity,
            _allocation: Some(allocation),
        })
    }
    /// A registry holding the two built-in schemas from the start of history.
    pub fn with_builtins(capacity: usize) -> Result<Self, RegistryError> {
        let mut registry = Self::new(capacity);
        registry.register(
            crate::TEST_REPORT_SCHEMA,
            crate::TEST_REPORT_MAX_BYTES,
            SessionSeq(0),
        )?;
        registry.register(
            crate::ERROR_REPORT_SCHEMA,
            crate::ERROR_REPORT_MAX_BYTES,
            SessionSeq(0),
        )?;
        Ok(registry)
    }
    /// Register a descriptor as of `introduced_at`; its hash is its identity.
    /// The same descriptor and bound again is idempotent; the same
    /// descriptor with another bound conflicts.
    pub fn register(
        &mut self,
        descriptor: &[u8],
        maximum_bytes: usize,
        introduced_at: SessionSeq,
    ) -> Result<ContentHash, RegistryError> {
        if descriptor.is_empty()
            || descriptor.len() > MAX_SCHEMA_DESCRIPTOR_BYTES
            || maximum_bytes == 0
        {
            return Err(RegistryError::Contract);
        }
        let hash = ContentHash(*blake3::hash(descriptor).as_bytes());
        if let Some(existing) = self.entries.get(&hash) {
            return if existing.maximum_bytes == maximum_bytes {
                Ok(hash)
            } else {
                Err(RegistryError::Conflict)
            };
        }
        if self.entries.len() >= self.capacity {
            return Err(RegistryError::Capacity);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(descriptor.len())
            .map_err(|_| RegistryError::Capacity)?;
        bytes.extend_from_slice(descriptor);
        self.entries.insert(
            hash,
            SchemaRecord {
                descriptor: bytes,
                maximum_bytes,
                introduced_at,
                superseded_by: None,
                superseded_at: None,
            },
        );
        Ok(hash)
    }
    /// Record that `successor` supersedes `old` as of `at`. Both must be
    /// registered, the successor must not be `old` or lead back to it, and
    /// an old schema is superseded once: the same successor again is
    /// idempotent, another conflicts.
    pub fn supersede(
        &mut self,
        old: ContentHash,
        successor: ContentHash,
        at: SessionSeq,
    ) -> Result<(), RegistryError> {
        if old == successor || !self.entries.contains_key(&successor) {
            return Err(RegistryError::Contract);
        }
        if self.current(successor)? == old {
            return Err(RegistryError::Conflict);
        }
        let record = self
            .entries
            .get_mut(&old)
            .ok_or(RegistryError::Unavailable)?;
        match record.superseded_by {
            Some(existing) if existing == successor => Ok(()),
            Some(_) => Err(RegistryError::Conflict),
            None if at < record.introduced_at => Err(RegistryError::Conflict),
            None => {
                record.superseded_by = Some(successor);
                record.superseded_at = Some(at);
                Ok(())
            }
        }
    }
    /// A schema's record, superseded or not: its historical identity.
    pub fn record(&self, schema: ContentHash) -> Option<&SchemaRecord> {
        self.entries.get(&schema)
    }
    /// The schema new work should cite for `schema`: the end of its
    /// supersession chain, itself when never superseded.
    pub fn current(&self, schema: ContentHash) -> Result<ContentHash, RegistryError> {
        let mut at = schema;
        for _ in 0..=self.capacity {
            match self.entries.get(&at) {
                None => return Err(RegistryError::Unavailable),
                Some(record) => match record.superseded_by {
                    None => return Ok(at),
                    Some(next) => at = next,
                },
            }
        }
        Err(RegistryError::Conflict)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
impl NativeSchemaVerifier for SchemaRegistry {
    fn maximum_bytes(&self, schema: ContentHash) -> Result<usize, BuiltinSchemaError> {
        self.entries
            .get(&schema)
            .map(|record| record.maximum_bytes)
            .ok_or(BuiltinSchemaError::Unsupported)
    }
    fn verify(&self, schema: ContentHash, bytes: &[u8]) -> Result<(), BuiltinSchemaError> {
        let record = self
            .entries
            .get(&schema)
            .ok_or(BuiltinSchemaError::Unsupported)?;
        if bytes.len() > record.maximum_bytes {
            return Err(BuiltinSchemaError::Capacity);
        }
        crate::verify_builtin_schema(schema, bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TestReportValidator, Validator, error_report_schema, test_report_schema};

    #[test]
    fn schemas_keep_their_identity_and_bound_across_supersession() {
        let mut registry = SchemaRegistry::with_builtins(8).unwrap();
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry.maximum_bytes(test_report_schema()).unwrap(),
            crate::TEST_REPORT_MAX_BYTES
        );
        // A newer test-report contract with a larger bound.
        let v2 = registry
            .register(
                b"focal.test_report.v2:{passed:u64,failed:u64,skipped:u64,flaky:u64}",
                2 << 20,
                SessionSeq(40),
            )
            .unwrap();
        assert_eq!(
            registry.register(
                b"focal.test_report.v2:{passed:u64,failed:u64,skipped:u64,flaky:u64}",
                2 << 20,
                SessionSeq(41)
            ),
            Ok(v2)
        );
        assert_eq!(
            registry.register(
                b"focal.test_report.v2:{passed:u64,failed:u64,skipped:u64,flaky:u64}",
                1,
                SessionSeq(41)
            ),
            Err(RegistryError::Conflict)
        );
        registry
            .supersede(test_report_schema(), v2, SessionSeq(42))
            .unwrap();
        registry
            .supersede(test_report_schema(), v2, SessionSeq(42))
            .unwrap();
        assert_eq!(
            registry.supersede(test_report_schema(), error_report_schema(), SessionSeq(43)),
            Err(RegistryError::Conflict)
        );
        // The old identity still resolves with its original bound; new work
        // is pointed at the successor; the chain never loops.
        let old = registry.record(test_report_schema()).unwrap();
        assert_eq!(old.maximum_bytes, crate::TEST_REPORT_MAX_BYTES);
        assert_eq!(old.superseded_by, Some(v2));
        assert_eq!(old.superseded_at, Some(SessionSeq(42)));
        assert_eq!(registry.current(test_report_schema()).unwrap(), v2);
        assert_eq!(registry.current(v2).unwrap(), v2);
        assert_eq!(
            registry.supersede(v2, test_report_schema(), SessionSeq(44)),
            Err(RegistryError::Conflict)
        );
        assert_eq!(
            registry.supersede(v2, v2, SessionSeq(44)),
            Err(RegistryError::Contract)
        );
        assert_eq!(
            registry.supersede(v2, ContentHash([9; 32]), SessionSeq(44)),
            Err(RegistryError::Contract)
        );
        assert_eq!(
            registry.current(ContentHash([9; 32])),
            Err(RegistryError::Unavailable)
        );
        // Verification stays the built-in contract; a registered descriptor
        // without one answers its bound and refuses verification honestly.
        assert!(
            registry
                .verify(
                    test_report_schema(),
                    br#"{"passed":1,"failed":0,"skipped":0}"#
                )
                .is_ok()
        );
        assert_eq!(registry.maximum_bytes(v2).unwrap(), 2 << 20);
        assert_eq!(
            registry.verify(v2, b"{}"),
            Err(BuiltinSchemaError::Unsupported)
        );
        assert_eq!(
            registry.verify(
                test_report_schema(),
                &vec![b' '; crate::TEST_REPORT_MAX_BYTES + 1]
            ),
            Err(BuiltinSchemaError::Capacity)
        );
        // Bounds: capacity, descriptor size, an empty descriptor, a zero bound.
        let mut small = SchemaRegistry::new(1);
        small.register(b"a", 1, SessionSeq(0)).unwrap();
        assert_eq!(
            small.register(b"b", 1, SessionSeq(0)),
            Err(RegistryError::Capacity)
        );
        assert_eq!(
            small.register(&[b'x'; MAX_SCHEMA_DESCRIPTOR_BYTES + 1], 1, SessionSeq(0)),
            Err(RegistryError::Contract)
        );
        assert_eq!(
            small.register(b"", 1, SessionSeq(0)),
            Err(RegistryError::Contract)
        );
        assert_eq!(
            small.register(b"c", 0, SessionSeq(0)),
            Err(RegistryError::Contract)
        );
        // A charged registry returns its bytes when dropped.
        let budget = MemoryBudget::new(1 << 20, 1 << 18).unwrap();
        {
            let charged = SchemaRegistry::new_in(4, &budget).unwrap();
            assert!(budget.stats().used >= 4 * MAX_SCHEMA_DESCRIPTOR_BYTES);
            drop(charged);
        }
        assert_eq!(budget.stats().used, 0);
        assert!(SchemaRegistry::new_in(usize::MAX, &budget).is_err());
    }

    #[test]
    fn a_retired_validator_keeps_its_identity_and_runs_nothing_new() {
        let handler = focal_model::HandlerRef {
            id: focal_model::ValidatorId::from_u128(5),
            version: ContentHash([5; 32]),
            agentic: false,
        };
        let registration = crate::Registration {
            handler: handler.clone(),
            evidence_schema: test_report_schema(),
            max_evidence_bytes: 4096,
        };
        let mut registry = crate::Registry::new(2);
        registry
            .register_at(
                registration.clone(),
                Box::new(TestReportValidator),
                SessionSeq(10),
            )
            .unwrap();
        assert_eq!(
            registry.lifetime(&handler),
            Some(crate::Lifetime {
                introduced_at: SessionSeq(10),
                retired_at: None
            })
        );
        let report = br#"{"passed":1,"failed":0,"skipped":0}"#;
        assert!(
            registry
                .execute(&handler, test_report_schema(), report, None)
                .is_ok()
        );
        // Retiring before its introduction is a contradiction.
        assert_eq!(
            registry.retire(&handler, SessionSeq(9)),
            Err(RegistryError::Conflict)
        );
        registry.retire(&handler, SessionSeq(20)).unwrap();
        registry.retire(&handler, SessionSeq(20)).unwrap();
        assert_eq!(
            registry.retire(&handler, SessionSeq(21)),
            Err(RegistryError::Conflict)
        );
        assert_eq!(
            registry.execute(&handler, test_report_schema(), report, None),
            Err(RegistryError::Retired { at: 20 })
        );
        assert_eq!(registry.lookup(&handler), Some(&registration));
        assert_eq!(
            registry.lifetime(&handler).and_then(|l| l.retired_at),
            Some(SessionSeq(20))
        );
        // The identity stays immutable: the same registration is idempotent
        // after retirement, a different one conflicts, an unknown one is
        // unavailable, and a mismatched handler is a contract error.
        assert!(
            registry
                .register(registration.clone(), Box::new(TestReportValidator))
                .is_ok()
        );
        let mut other = registration.clone();
        other.max_evidence_bytes = 1;
        assert_eq!(
            registry.register(other, Box::new(TestReportValidator)),
            Err(RegistryError::Conflict)
        );
        let unknown = focal_model::HandlerRef {
            id: focal_model::ValidatorId::from_u128(6),
            ..handler.clone()
        };
        assert_eq!(
            registry.retire(&unknown, SessionSeq(30)),
            Err(RegistryError::Unavailable)
        );
        assert!(registry.lookup(&unknown).is_none());
        let agentic = focal_model::HandlerRef {
            agentic: true,
            ..handler.clone()
        };
        assert_eq!(
            registry.retire(&agentic, SessionSeq(30)),
            Err(RegistryError::Contract)
        );
        struct Never;
        impl Validator for Never {
            fn evaluate(
                &self,
                _: &[u8],
                _: Option<&str>,
            ) -> Result<crate::Evaluation, RegistryError> {
                Err(RegistryError::Execution("never".into()))
            }
        }
        let second = crate::Registration {
            handler: focal_model::HandlerRef {
                version: ContentHash([6; 32]),
                ..handler.clone()
            },
            evidence_schema: test_report_schema(),
            max_evidence_bytes: 4096,
        };
        registry
            .register_at(second, Box::new(Never), SessionSeq(20))
            .unwrap();
        assert_eq!(
            registry.register_at(
                crate::Registration {
                    handler: focal_model::HandlerRef {
                        version: ContentHash([7; 32]),
                        ..handler
                    },
                    evidence_schema: test_report_schema(),
                    max_evidence_bytes: 1,
                },
                Box::new(Never),
                SessionSeq(21)
            ),
            Err(RegistryError::Capacity)
        );
    }
}
