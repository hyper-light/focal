use focal_model::{ContentHash, HandlerRef, ValidatorId, VerdictValue};
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

pub struct Registry {
    entries: BTreeMap<(ValidatorId, ContentHash), (Registration, Box<dyn Validator>)>,
    capacity: usize,
}

impl Registry {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            capacity,
        }
    }
    pub fn register(
        &mut self,
        registration: Registration,
        implementation: Box<dyn Validator>,
    ) -> Result<(), RegistryError> {
        let key = (registration.handler.id, registration.handler.version);
        if let Some((existing, _)) = self.entries.get(&key) {
            return if existing == &registration {
                Ok(())
            } else {
                Err(RegistryError::Conflict)
            };
        }
        if self.entries.len() >= self.capacity {
            return Err(RegistryError::Capacity);
        }
        self.entries.insert(key, (registration, implementation));
        Ok(())
    }
    pub fn execute(
        &self,
        handler: &HandlerRef,
        schema: ContentHash,
        evidence: &[u8],
        quality_bar: Option<&str>,
    ) -> Result<Evaluation, RegistryError> {
        let (registration, validator) = self
            .entries
            .get(&(handler.id, handler.version))
            .ok_or(RegistryError::Unavailable)?;
        if registration.handler != *handler
            || registration.evidence_schema != schema
            || evidence.len() > registration.max_evidence_bytes
            || (quality_bar.is_some() && !handler.agentic)
        {
            return Err(RegistryError::Contract);
        }
        validator.evaluate(evidence, quality_bar)
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
