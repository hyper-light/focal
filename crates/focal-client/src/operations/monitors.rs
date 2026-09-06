//! Authored durable wait predicates. Registration records a monitor; it neither
//! launches work nor turns an elapsed client wait into a ledger timer input.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorRegisterDocument {
    #[serde(default)]
    pub monitor: Option<String>,
    pub owner: String,
    pub roots: Vec<WaitPredicateDocument>,
    pub deadline: DeadlineDocument,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "predicate", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitPredicateDocument {
    Satisfied { claim: String },
    Terminal { claim: String },
    Released { claim: String },
}

impl WaitPredicateDocument {
    fn build(&self) -> Result<WaitPredicate, InputError> {
        Ok(match self {
            Self::Satisfied { claim } => WaitPredicate::Satisfied(ClaimId(parse_id(claim)?)),
            Self::Terminal { claim } => WaitPredicate::Terminal(ClaimId(parse_id(claim)?)),
            Self::Released { claim } => WaitPredicate::Released(ClaimId(parse_id(claim)?)),
        })
    }
}

impl MonitorRegisterDocument {
    pub fn build(
        self,
        context: &BuildContext,
        ids: &mut impl IdGenerator,
    ) -> Result<Command, InputError> {
        context.validate()?;
        if self.roots.is_empty() || self.roots.len() > 256 {
            return Err(InputError::Invalid("monitor requires 1..256 wait roots"));
        }
        let owner = ClaimId(parse_id(&self.owner)?);
        if self.deadline.generation == 0 || self.deadline.at == 0 {
            return Err(InputError::Invalid(
                "monitor requires an explicit positive deadline",
            ));
        }
        let deadline = Deadline {
            timer: TimerId(parse_id(&self.deadline.timer)?),
            generation: self.deadline.generation,
            at: self.deadline.at,
        };
        let mut roots = BTreeSet::new();
        for root in self.roots {
            if !roots.insert(root.build()?) {
                return Err(InputError::Invalid("duplicate monitor wait predicate"));
            }
        }
        Ok(Command::RegisterMonitor {
            monitor: MonitorId(allocated(self.monitor, ids)?),
            owner,
            roots,
            deadline,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorGetDocument {
    pub id: String,
}
impl MonitorGetDocument {
    pub fn build(self) -> Result<MonitorId, InputError> {
        Ok(MonitorId(parse_id(&self.id)?))
    }
}

#[cfg(test)]
#[path = "monitor_tests.rs"]
mod tests;
