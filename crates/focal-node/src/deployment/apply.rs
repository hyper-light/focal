//! Applying a plan: preflight against the current observation, one journaled
//! step per change, resumable and exact on repetition.
use super::{
    DeploymentError, GuaranteeLevel, hex, now_ms,
    observe::observe,
    plan::{Change, DeploymentPlan, Observation, ObservedSession},
    survive_code,
};
use crate::{
    cluster_admin::ClusterAdmin,
    config::policy::{self, PolicyRevision},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// `FCLAPLY1`: magic then the postcard journal.
pub const JOURNAL_MAGIC: &[u8; 8] = b"FCLAPLY1";
pub const JOURNAL_SCHEMA: u16 = 1;
pub const MAX_JOURNAL_BYTES: usize = 1024 * 1024;
/// Journals `status` lists at most.
pub const MAX_LISTED: usize = 64;
const POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Phase {
    /// The step is next; nothing was sent.
    Prepared,
    /// The change was accepted (policy written, placement request journaled).
    Committed,
    /// The directory reflects the change (the plan is under way or done).
    Verified,
    /// The change's effect is observed complete.
    Complete,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub index: u32,
    pub phase: Phase,
    /// The operation the placement request denoted, once committed.
    pub operation: Option<[u8; 16]>,
    pub updated_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    InProgress,
    Complete,
    /// The plan was found stale at `step`; it is never resumed.
    Stale {
        step: u32,
        subject: String,
        field: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Journal {
    pub schema: u16,
    pub plan_id: [u8; 16],
    pub plan_hash: [u8; 32],
    pub started_ms: u64,
    pub steps: Vec<Step>,
    pub outcome: Outcome,
}
impl Journal {
    pub fn new(plan: &DeploymentPlan, now: u64) -> Result<Self, DeploymentError> {
        Ok(Self {
            schema: JOURNAL_SCHEMA,
            plan_id: plan.plan_id,
            plan_hash: plan.hash()?,
            started_ms: now,
            steps: Vec::new(),
            outcome: Outcome::InProgress,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>, DeploymentError> {
        let body = postcard::to_stdvec(self)
            .map_err(|error| DeploymentError::Encoding(error.to_string()))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(JOURNAL_MAGIC.len().saturating_add(body.len()))
            .map_err(|_| DeploymentError::Capacity)?;
        bytes.extend_from_slice(JOURNAL_MAGIC);
        bytes.extend_from_slice(&body);
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(DeploymentError::Capacity);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DeploymentError> {
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(DeploymentError::Capacity);
        }
        let body = bytes
            .strip_prefix(JOURNAL_MAGIC.as_slice())
            .ok_or(DeploymentError::Corrupt("not an apply journal"))?;
        let (journal, tail): (Self, &[u8]) = postcard::take_from_bytes(body)
            .map_err(|_| DeploymentError::Corrupt("apply journal does not decode"))?;
        if !tail.is_empty() || journal.schema != JOURNAL_SCHEMA {
            return Err(DeploymentError::Corrupt("apply journal schema"));
        }
        Ok(journal)
    }
    pub fn phase(&self, index: u32) -> Option<Phase> {
        self.steps
            .iter()
            .find(|step| step.index == index)
            .map(|step| step.phase)
    }
    pub fn operation(&self, index: u32) -> Option<[u8; 16]> {
        self.steps
            .iter()
            .find(|step| step.index == index)
            .and_then(|step| step.operation)
    }
    /// Record a step's phase; phases only advance.
    pub fn record(
        &mut self,
        index: u32,
        phase: Phase,
        operation: Option<[u8; 16]>,
        now: u64,
    ) -> Result<(), DeploymentError> {
        if let Some(step) = self.steps.iter_mut().find(|step| step.index == index) {
            if phase < step.phase {
                return Err(DeploymentError::Corrupt(
                    "apply journal would regress a step",
                ));
            }
            step.phase = phase;
            if operation.is_some() {
                step.operation = operation;
            }
            step.updated_ms = now;
            return Ok(());
        }
        self.steps
            .try_reserve_exact(1)
            .map_err(|_| DeploymentError::Capacity)?;
        self.steps.push(Step {
            index,
            phase,
            operation,
            updated_ms: now,
        });
        Ok(())
    }
}

/// The current facts a step is checked against.
pub struct Current {
    pub policy_revision: u64,
    pub sessions: BTreeMap<([u8; 16], [u8; 16]), ObservedSession>,
}
impl Current {
    pub fn of(observation: &Observation) -> Self {
        Self {
            policy_revision: observation.committed.revision.0,
            sessions: observation
                .sessions
                .iter()
                .map(|session| ((session.tenant, session.session), session.clone()))
                .collect(),
        }
    }
}
fn stale(subject: String, field: &'static str) -> DeploymentError {
    DeploymentError::Stale { subject, field }
}
fn session_name(tenant: &[u8; 16], session: &[u8; 16]) -> String {
    format!("session {}/{}", hex(tenant), hex(session))
}
/// Check every step that has not been committed yet against the current
/// facts: the policy revision the plan observed and each session's epochs.
/// Nothing is sent when this fails.
pub fn preflight(
    plan: &DeploymentPlan,
    journal: &Journal,
    current: &Current,
) -> Result<(), DeploymentError> {
    if let Outcome::Stale { subject, field, .. } = &journal.outcome {
        return Err(DeploymentError::Stale {
            subject: subject.clone(),
            field: match field.as_str() {
                "committed_revision" => "committed_revision",
                "route_epoch" => "route_epoch",
                "membership_epoch" => "membership_epoch",
                "placement_epoch" => "placement_epoch",
                "operation" => "operation",
                _ => "presence",
            },
        });
    }
    for (index, change) in (0u32..).zip(&plan.body.changes) {
        if journal
            .phase(index)
            .is_some_and(|phase| phase >= Phase::Committed)
        {
            continue;
        }
        match change {
            Change::CommitPolicy { from_revision, .. } => {
                if current.policy_revision != *from_revision {
                    return Err(stale("policy".into(), "committed_revision"));
                }
            }
            Change::PlanSession {
                tenant,
                session,
                expected,
                ..
            } => {
                let name = session_name(tenant, session);
                let observed = current
                    .sessions
                    .get(&(*tenant, *session))
                    .ok_or_else(|| stale(name.clone(), "presence"))?;
                if observed.epochs.route != expected.route {
                    return Err(stale(name, "route_epoch"));
                }
                if observed.epochs.membership != expected.membership {
                    return Err(stale(name, "membership_epoch"));
                }
                if observed.epochs.placement != expected.placement {
                    return Err(stale(name, "placement_epoch"));
                }
            }
            Change::NoChange { .. } => {}
        }
    }
    Ok(())
}
/// The phase a committed placement request has reached by observation.
pub fn session_progress(
    observed: Option<&ObservedSession>,
    operation: [u8; 16],
    durability: GuaranteeLevel,
    expected_placement_epoch: u64,
) -> Phase {
    let Some(observed) = observed else {
        return Phase::Committed;
    };
    let achieved = observed
        .achieved
        .is_some_and(|achieved| achieved.covers(durability));
    if observed.pending.is_none() && achieved {
        return Phase::Complete;
    }
    if observed.pending == Some(operation)
        || achieved
        || observed.epochs.placement > expected_placement_epoch
    {
        return Phase::Verified;
    }
    Phase::Committed
}

pub fn journal_dir(root: &Path, plan_id: &[u8; 16]) -> PathBuf {
    root.join("cluster").join("apply").join(hex(plan_id))
}
fn write_journal(dir: &Path, journal: &Journal) -> Result<(), DeploymentError> {
    crate::embedded::durable_dir(dir)?;
    crate::embedded::atomic_file(&dir.join("JOURNAL"), &journal.encode()?)?;
    Ok(())
}
fn read_file(path: &Path, max: usize) -> Result<Option<Vec<u8>>, DeploymentError> {
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = usize::try_from(file.metadata()?.len()).map_err(|_| DeploymentError::Capacity)?;
    if length > max {
        return Err(DeploymentError::Capacity);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| DeploymentError::Capacity)?;
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}
/// Load a plan's journal, or start one and keep the plan beside it.
fn open_journal(root: &Path, plan: &DeploymentPlan, now: u64) -> Result<Journal, DeploymentError> {
    let dir = journal_dir(root, &plan.plan_id);
    match read_file(&dir.join("JOURNAL"), MAX_JOURNAL_BYTES)? {
        Some(bytes) => {
            let journal = Journal::decode(&bytes)?;
            if journal.plan_id != plan.plan_id || journal.plan_hash != plan.hash()? {
                return Err(DeploymentError::Corrupt("apply journal names another plan"));
            }
            Ok(journal)
        }
        None => {
            let journal = Journal::new(plan, now)?;
            crate::embedded::durable_dir(&dir)?;
            crate::embedded::atomic_file(&dir.join("PLAN"), &plan.encode()?)?;
            write_journal(&dir, &journal)?;
            Ok(journal)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StepReport {
    pub index: u32,
    pub change: super::plan::ChangeView,
    pub phase: Phase,
    pub operation: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApplyReport {
    pub kind: &'static str,
    pub plan_id: String,
    pub outcome: Outcome,
    pub steps: Vec<StepReport>,
    pub guarantee: super::plan::GuaranteeView,
    pub journal: String,
}
fn report(
    kind: &'static str,
    root: &Path,
    plan: &DeploymentPlan,
    journal: &Journal,
) -> ApplyReport {
    ApplyReport {
        kind,
        plan_id: hex(&plan.plan_id),
        outcome: journal.outcome.clone(),
        steps: (0u32..)
            .zip(&plan.body.changes)
            .map(|(index, change)| StepReport {
                index,
                change: super::plan::ChangeView::of(change),
                phase: journal.phase(index).unwrap_or(Phase::Prepared),
                operation: journal.operation(index).map(|operation| hex(&operation)),
            })
            .collect(),
        guarantee: plan.view().guarantee,
        journal: journal_dir(root, &plan.plan_id).display().to_string(),
    }
}

/// Apply `plan` on the node `admin` administers, resuming the journal when
/// one exists, and wait up to `wait` for the placement changes to complete.
pub async fn apply(
    admin: &ClusterAdmin,
    network: bool,
    plan: &DeploymentPlan,
    wait: Duration,
) -> Result<ApplyReport, DeploymentError> {
    let root = admin.root();
    if plan.body.deployment.cluster != admin.identity().cluster {
        return Err(DeploymentError::WrongDeployment);
    }
    if !plan.body.blocked.is_empty() {
        return Err(DeploymentError::Blocked(plan.body.blocked.len()));
    }
    let observation = observe(admin, network).await?;
    // The journal exists only once the plan passed preflight at least once,
    // so a stale plan leaves nothing behind.
    let existing = match read_file(
        &journal_dir(root, &plan.plan_id).join("JOURNAL"),
        MAX_JOURNAL_BYTES,
    )? {
        Some(bytes) => Some(Journal::decode(&bytes)?),
        None => None,
    };
    let fresh = Journal::new(plan, 0)?;
    preflight(
        plan,
        existing.as_ref().unwrap_or(&fresh),
        &Current::of(&observation),
    )?;
    let now = now_ms()?;
    let mut journal = open_journal(root, plan, now)?;
    let dir = journal_dir(root, &plan.plan_id);
    let mut current = Current::of(&observation);
    let deadline = Instant::now().checked_add(wait);
    for (index, change) in (0u32..).zip(&plan.body.changes) {
        if journal.phase(index) == Some(Phase::Complete) {
            continue;
        }
        match change {
            Change::CommitPolicy {
                from_revision,
                to_revision,
            } => {
                if journal
                    .phase(index)
                    .is_none_or(|phase| phase < Phase::Committed)
                {
                    journal.record(index, Phase::Prepared, None, now_ms()?)?;
                    write_journal(&dir, &journal)?;
                    policy::commit(
                        root,
                        &plan.body.requested,
                        PolicyRevision(*from_revision),
                        crate::embedded::atomic_file,
                    )?;
                    journal.record(index, Phase::Committed, None, now_ms()?)?;
                    write_journal(&dir, &journal)?;
                }
                let committed = policy::read_committed(root)?
                    .ok_or(crate::config::ConfigError::PolicyMissing)?;
                if committed.revision.0 != *to_revision || committed.hash != plan.body.policy_hash {
                    journal.outcome = Outcome::Stale {
                        step: index,
                        subject: "policy".into(),
                        field: "committed_revision".into(),
                    };
                    write_journal(&dir, &journal)?;
                    return Err(stale("policy".into(), "committed_revision"));
                }
                current.policy_revision = committed.revision.0;
                journal.record(index, Phase::Complete, None, now_ms()?)?;
                write_journal(&dir, &journal)?;
            }
            Change::PlanSession {
                tenant,
                session,
                durability,
                operation,
                expected,
                ..
            } => {
                if journal
                    .phase(index)
                    .is_none_or(|phase| phase < Phase::Committed)
                {
                    journal.record(index, Phase::Prepared, None, now_ms()?)?;
                    write_journal(&dir, &journal)?;
                    let reply = admin
                        .plan_session_reply(
                            *tenant,
                            *session,
                            survive_code(durability.survive),
                            durability.max_failures,
                            false,
                        )
                        .await?;
                    if reply.operation != *operation {
                        let name = session_name(tenant, session);
                        journal.outcome = Outcome::Stale {
                            step: index,
                            subject: name.clone(),
                            field: "operation".into(),
                        };
                        write_journal(&dir, &journal)?;
                        return Err(stale(name, "operation"));
                    }
                    journal.record(index, Phase::Committed, Some(*operation), now_ms()?)?;
                    write_journal(&dir, &journal)?;
                }
                loop {
                    let progress = session_progress(
                        current.sessions.get(&(*tenant, *session)),
                        *operation,
                        *durability,
                        expected.placement,
                    );
                    let recorded = journal.phase(index).unwrap_or(Phase::Committed);
                    if progress > recorded {
                        journal.record(index, progress, None, now_ms()?)?;
                        write_journal(&dir, &journal)?;
                    }
                    if progress == Phase::Complete
                        || deadline.is_none_or(|deadline| Instant::now() >= deadline)
                    {
                        break;
                    }
                    tokio::time::sleep(POLL).await;
                    current = Current::of(&observe(admin, network).await?);
                }
            }
            Change::NoChange { .. } => {
                journal.record(index, Phase::Complete, None, now_ms()?)?;
                write_journal(&dir, &journal)?;
            }
        }
        if journal.phase(index) != Some(Phase::Complete) {
            // Later steps still run: each placement request is independent
            // and exact; this one stays journaled as under way.
            continue;
        }
    }
    let complete = (0u32..)
        .zip(&plan.body.changes)
        .all(|(index, _)| journal.phase(index) == Some(Phase::Complete));
    journal.outcome = if complete {
        Outcome::Complete
    } else {
        Outcome::InProgress
    };
    write_journal(&dir, &journal)?;
    Ok(report("deployment_applied", root, plan, &journal))
}

/// Re-evaluate every journaled plan (or the one named) against the current
/// observation, advancing steps whose effect is now visible.
pub async fn status(
    admin: &ClusterAdmin,
    network: bool,
    plan_id: Option<[u8; 16]>,
) -> Result<Vec<ApplyReport>, DeploymentError> {
    let root = admin.root();
    let base = root.join("cluster").join("apply");
    let mut ids = Vec::new();
    match plan_id {
        Some(id) => ids.push(id),
        None => {
            if let Ok(entries) = std::fs::read_dir(&base) {
                for entry in entries {
                    let entry = entry?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    let Ok(id) = focal_client::input::parse_id(name) else {
                        continue;
                    };
                    if ids.len() >= MAX_LISTED {
                        break;
                    }
                    ids.try_reserve_exact(1)
                        .map_err(|_| DeploymentError::Capacity)?;
                    ids.push(id);
                }
            }
            ids.sort_unstable();
        }
    }
    let mut reports = Vec::new();
    reports
        .try_reserve_exact(ids.len())
        .map_err(|_| DeploymentError::Capacity)?;
    let mut observation = None;
    for id in ids {
        let dir = journal_dir(root, &id);
        let Some(plan_bytes) = read_file(&dir.join("PLAN"), super::plan::MAX_PLAN_BYTES)? else {
            if plan_id.is_some() {
                return Err(DeploymentError::Corrupt("no journal for this plan"));
            }
            continue;
        };
        let plan = DeploymentPlan::decode(&plan_bytes)?;
        let journal_bytes = read_file(&dir.join("JOURNAL"), MAX_JOURNAL_BYTES)?.ok_or(
            DeploymentError::Corrupt("journal file missing beside its plan"),
        )?;
        let mut journal = Journal::decode(&journal_bytes)?;
        if journal.plan_id != id || plan.plan_id != id {
            return Err(DeploymentError::Corrupt("journal names another plan"));
        }
        if journal.outcome == Outcome::InProgress {
            if observation.is_none() {
                observation = Some(observe(admin, network).await?);
            }
            let current = observation.as_ref().map(Current::of);
            let mut changed = false;
            for (index, change) in (0u32..).zip(&plan.body.changes) {
                let Change::PlanSession {
                    tenant,
                    session,
                    durability,
                    operation,
                    expected,
                    ..
                } = change
                else {
                    continue;
                };
                let recorded = journal.phase(index).unwrap_or(Phase::Prepared);
                if recorded < Phase::Committed || recorded == Phase::Complete {
                    continue;
                }
                let progress = session_progress(
                    current
                        .as_ref()
                        .and_then(|current| current.sessions.get(&(*tenant, *session))),
                    *operation,
                    *durability,
                    expected.placement,
                );
                if progress > recorded {
                    journal.record(index, progress, None, now_ms()?)?;
                    changed = true;
                }
            }
            let complete = (0u32..)
                .zip(&plan.body.changes)
                .all(|(index, _)| journal.phase(index) == Some(Phase::Complete));
            if complete {
                journal.outcome = Outcome::Complete;
                changed = true;
            }
            if changed {
                write_journal(&dir, &journal)?;
            }
        }
        reports.push(report("deployment_status", root, &plan, &journal));
    }
    Ok(reports)
}
