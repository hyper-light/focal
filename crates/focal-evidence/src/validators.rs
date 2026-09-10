use focal_model::{ContentHash, HandlerRef, SessionSeq, ValidatorId, VerdictValue};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const TEST_REPORT_SCHEMA: &[u8] =
    br#"focal.test_report.v1:{passed:u64,failed:u64,skipped:u64};deny_unknown_fields"#;
pub fn test_report_schema() -> ContentHash {
    ContentHash(*blake3::hash(TEST_REPORT_SCHEMA).as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evaluation {
    pub value: VerdictValue,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("validator registration limit reached")]
    Capacity,
    #[error("validator version has conflicting immutable registration")]
    Conflict,
    #[error("the pinned validator implementation is unavailable")]
    Unavailable,
    #[error("validator evidence schema/capability/size mismatch")]
    Contract,
    #[error("validator execution error: {0}")]
    Execution(String),
    /// The version keeps its identity but runs nothing new since `at`.
    #[error("validator version retired at session sequence {at}")]
    Retired { at: u64 },
}
/// When a version entered service and, once retired, when it left it. A
/// retired version keeps its registration (a report that cites it still
/// resolves to the same schema and bound) but evaluates nothing new.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifetime {
    pub introduced_at: SessionSeq,
    pub retired_at: Option<SessionSeq>,
}

pub trait Validator: Send + Sync {
    fn evaluate(
        &self,
        evidence: &[u8],
        quality_bar: Option<&str>,
    ) -> Result<Evaluation, RegistryError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub handler: HandlerRef,
    pub evidence_schema: ContentHash,
    pub max_evidence_bytes: usize,
}

struct Entry {
    registration: Registration,
    lifetime: Lifetime,
    implementation: Box<dyn Validator>,
}
pub struct Registry {
    entries: BTreeMap<(ValidatorId, ContentHash), Entry>,
    capacity: usize,
}

impl Registry {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity,
        }
    }
    /// Register a version as present from the start of history.
    pub fn register(
        &mut self,
        registration: Registration,
        implementation: Box<dyn Validator>,
    ) -> Result<(), RegistryError> {
        self.register_at(registration, implementation, SessionSeq(0))
    }
    /// Register a version that entered service at `introduced_at`. A version
    /// is immutable: the same registration again is idempotent (whatever
    /// its lifetime), a different one under the same identity conflicts.
    pub fn register_at(
        &mut self,
        registration: Registration,
        implementation: Box<dyn Validator>,
        introduced_at: SessionSeq,
    ) -> Result<(), RegistryError> {
        let key = (registration.handler.id, registration.handler.version);
        if let Some(existing) = self.entries.get(&key) {
            return if existing.registration == registration {
                Ok(())
            } else {
                Err(RegistryError::Conflict)
            };
        }
        if self.entries.len() >= self.capacity {
            return Err(RegistryError::Capacity);
        }
        self.entries.insert(
            key,
            Entry {
                registration,
                lifetime: Lifetime {
                    introduced_at,
                    retired_at: None,
                },
                implementation,
            },
        );
        Ok(())
    }
    /// Retire a version at `at`: it keeps its identity for every report that
    /// cites it and evaluates nothing new. Retiring twice at the same
    /// sequence is idempotent; at another sequence it conflicts, since a
    /// retirement is a recorded fact.
    pub fn retire(&mut self, handler: &HandlerRef, at: SessionSeq) -> Result<(), RegistryError> {
        let entry = self
            .entries
            .get_mut(&(handler.id, handler.version))
            .ok_or(RegistryError::Unavailable)?;
        if entry.registration.handler != *handler {
            return Err(RegistryError::Contract);
        }
        match entry.lifetime.retired_at {
            Some(retired) if retired == at => Ok(()),
            Some(_) => Err(RegistryError::Conflict),
            None if at < entry.lifetime.introduced_at => Err(RegistryError::Conflict),
            None => {
                entry.lifetime.retired_at = Some(at);
                Ok(())
            }
        }
    }
    /// The immutable registration of a version, retired or not.
    pub fn lookup(&self, handler: &HandlerRef) -> Option<&Registration> {
        self.entries
            .get(&(handler.id, handler.version))
            .map(|entry| &entry.registration)
            .filter(|registration| registration.handler == *handler)
    }
    pub fn lifetime(&self, handler: &HandlerRef) -> Option<Lifetime> {
        self.entries
            .get(&(handler.id, handler.version))
            .filter(|entry| entry.registration.handler == *handler)
            .map(|entry| entry.lifetime)
    }
    pub fn execute(
        &self,
        handler: &HandlerRef,
        schema: ContentHash,
        evidence: &[u8],
        quality_bar: Option<&str>,
    ) -> Result<Evaluation, RegistryError> {
        let entry = self
            .entries
            .get(&(handler.id, handler.version))
            .ok_or(RegistryError::Unavailable)?;
        if entry.registration.handler != *handler
            || entry.registration.evidence_schema != schema
            || evidence.len() > entry.registration.max_evidence_bytes
            || (quality_bar.is_some() && !handler.agentic)
        {
            return Err(RegistryError::Contract);
        }
        if let Some(at) = entry.lifetime.retired_at {
            return Err(RegistryError::Retired { at: at.0 });
        }
        entry.implementation.evaluate(evidence, quality_bar)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestReport {
    pub passed: u64,
    pub failed: u64,
    pub skipped: u64,
}
pub struct TestReportValidator;
impl Validator for TestReportValidator {
    fn evaluate(
        &self,
        evidence: &[u8],
        _quality_bar: Option<&str>,
    ) -> Result<Evaluation, RegistryError> {
        let report: TestReport = match serde_json::from_slice(evidence) {
            Ok(value) => value,
            Err(_) => {
                return Ok(Evaluation {
                    value: VerdictValue::Fail,
                    reason: "evidence does not match the pinned test-report schema".into(),
                });
            }
        };
        let (value, reason) = if report.failed > 0 {
            (VerdictValue::Fail, "the test report contains failures")
        } else if report.passed == 0 {
            (
                VerdictValue::Incomplete,
                "no passing executed test is evidenced",
            )
        } else {
            (VerdictValue::Pass, "all reported executed tests passed")
        };
        Ok(Evaluation {
            value,
            reason: reason.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reports_distinguish_negative_missing_and_passing_evidence() {
        let v = TestReportValidator;
        for (input, value) in [
            (
                br#"{"passed":1,"failed":0,"skipped":0}"#.as_slice(),
                VerdictValue::Pass,
            ),
            (
                br#"{"passed":0,"failed":0,"skipped":9}"#,
                VerdictValue::Incomplete,
            ),
            (
                br#"{"passed":100,"failed":1,"skipped":0}"#,
                VerdictValue::Fail,
            ),
            (
                br#"{"passed":1,"failed":0,"skipped":0,"ignore_failures":true}"#,
                VerdictValue::Fail,
            ),
        ] {
            assert_eq!(v.evaluate(input, None).unwrap().value, value);
        }
    }
    #[test]
    fn pinned_registry_rejects_substitution_and_unsupported_judgment() {
        let h = HandlerRef {
            id: ValidatorId::from_u128(1),
            version: ContentHash([2; 32]),
            agentic: false,
        };
        let registration = Registration {
            handler: h.clone(),
            evidence_schema: test_report_schema(),
            max_evidence_bytes: 256,
        };
        let mut r = Registry::new(1);
        r.register(registration.clone(), Box::new(TestReportValidator))
            .unwrap();
        let mut conflict = registration;
        conflict.max_evidence_bytes = 1;
        assert_eq!(
            r.register(conflict, Box::new(TestReportValidator)),
            Err(RegistryError::Conflict)
        );
        assert_eq!(
            r.execute(&h, test_report_schema(), b"{}", Some("good design")),
            Err(RegistryError::Contract)
        );
    }
}
