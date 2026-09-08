//! Restores complete retained native membership from actual declaration and
//! independent evaluation rows. Native history proves that no registration was
//! omitted and that original materialization/seal events occurred. These plans
//! do not materialize a new run, reopen a cohort or replay participant authority.
use super::*;
use crate::lifecycle::scope::snapshot::{Hash, Work, complete, identity, overhead, reserve};
use crate::lifecycle::validation::{Declaration, EvaluationState};
use crate::{ReceiptFence, ValidationMode};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationSnapshotV1 {
    pub claim: Binding,
    pub rows: usize,
    pub max_rows: usize,
    pub sealed: bool,
    pub increments_sealed: bool,
    pub sealed_at: Option<SessionSeq>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationMemberSnapshotV1 {
    pub binding: Binding,
    pub target: Target,
    pub generation: u64,
    pub receipt: Option<ReceiptFence>,
    pub declaration_index: u32,
    pub mode: ValidationMode,
}
#[derive(Debug, Clone, Copy)]
pub struct RegistrationValue<'a> {
    pub member: RegistrationMemberSnapshotV1,
    pub declaration: &'a Declaration,
    pub evaluation: EvaluationState,
}
pub trait RegistrationSnapshotSource {
    type Rows<'a>: Iterator<Item = Result<RegistrationValue<'a>, ContractError>>
    where
        Self: 'a;
    fn rows(&self) -> Self::Rows<'_>;
}
impl RegistrationSnapshotSource for [RegistrationValue<'_>] {
    type Rows<'a>
        = std::iter::Map<
        std::iter::Copied<std::slice::Iter<'a, RegistrationValue<'a>>>,
        fn(RegistrationValue<'a>) -> Result<RegistrationValue<'a>, ContractError>,
    >
    where
        Self: 'a;
    fn rows(&self) -> Self::Rows<'_> {
        self.iter().copied().map(Ok)
    }
}
pub struct RegistrationHydrationPlan<'a, S: RegistrationSnapshotSource + ?Sized> {
    claim: &'a ClaimState,
    fields: RegistrationSnapshotV1,
    source: &'a S,
    policy: ContentHash,
    fingerprint: ContentHash,
    visits: usize,
}
impl RegistrationSet {
    /// Exact retained member allocation for precharging a detached claim row.
    /// A quote carries no registration authority or semantic validation.
    pub fn hydration_heap_charge_v1(count: usize) -> Result<usize, ContractError> {
        bytes::add(
            bytes::array::<RegisteredEvaluation>(count)?,
            overhead(usize::from(count != 0))?,
        )
    }
    pub fn snapshot_v1(&self) -> RegistrationSnapshotV1 {
        RegistrationSnapshotV1 {
            claim: self.claim,
            rows: self.rows.len(),
            max_rows: self.max_rows,
            sealed: self.sealed,
            increments_sealed: self.increments_sealed,
            sealed_at: self.sealed_at,
        }
    }
    pub fn member_snapshots_v1(
        &self,
    ) -> impl ExactSizeIterator<Item = RegistrationMemberSnapshotV1> + '_ {
        self.rows.iter().map(|row| RegistrationMemberSnapshotV1 {
            binding: row.binding(),
            target: row.target(),
            generation: row.generation(),
            receipt: row.receipt(),
            declaration_index: row.declaration_index(),
            mode: row.mode(),
        })
    }
    pub fn prepare_hydration_v1<'a, S: RegistrationSnapshotSource + ?Sized>(
        claim: &'a ClaimState,
        fields: RegistrationSnapshotV1,
        source: &'a S,
        max_rows: usize,
        max_visits: usize,
    ) -> Result<RegistrationHydrationPlan<'a, S>, ContractError> {
        let mut work = Work::new(max_visits);
        work.charge(64)?;
        identity(claim.binding(), fields.claim)?;
        claim.acceptance().check(claim.binding(), claim.issuer())?;
        if fields.max_rows == 0 || fields.max_rows > max_rows || fields.rows > fields.max_rows {
            return Err(ContractError::Capacity);
        }
        if fields.sealed && !fields.increments_sealed
            || fields.sealed_at.is_some() && !fields.sealed
        {
            return Err(ContractError::InvalidTransition);
        }
        if let Some(sequence) = fields.sealed_at
            && (sequence.0 == 0
                || sequence < claim.created()
                || claim.local_sealed_at() != Some(sequence)
                || !claim.local_complete() && !claim.is_terminal())
        {
            return Err(ContractError::InvalidCut);
        }
        // An unstamped legacy seal is preserved as such; it cannot manufacture
        // NativeSealedTargets audit authority after restoration.
        let mut policy_visits = bytes::add(
            8,
            claim
                .acceptance()
                .declarations()
                .len()
                .checked_mul(16)
                .ok_or(ContractError::Capacity)?,
        )?;
        for slot in claim.acceptance().slots() {
            work.charge(1)?;
            policy_visits = bytes::add(
                policy_visits,
                bytes::add(
                    8,
                    slot.checks
                        .len()
                        .checked_mul(4)
                        .ok_or(ContractError::Capacity)?,
                )?,
            )?;
        }
        work.charge(policy_visits)?;
        let policy = claim.acceptance().intent_fingerprint();
        let visits = row_quote(fields.rows, claim.acceptance().declarations().len())?;
        work.charge(visits)?;
        let fingerprint = inspect(claim, fields, source, visits, |_| Ok(()))?;
        Ok(RegistrationHydrationPlan {
            claim,
            fields,
            source,
            policy,
            fingerprint,
            visits: work.used(),
        })
    }
}
impl<S: RegistrationSnapshotSource + ?Sized> RegistrationHydrationPlan<'_, S> {
    pub fn fields(&self) -> RegistrationSnapshotV1 {
        self.fields
    }
    pub fn inspection_visits(&self) -> usize {
        self.visits
    }
    pub fn construction_heap_bytes(&self) -> Result<usize, ContractError> {
        bytes::array::<RegisteredEvaluation>(self.fields.rows)
    }
    pub fn construction_heap_allocations(&self) -> usize {
        usize::from(self.fields.rows != 0)
    }
    pub fn construction_charge(&self) -> Result<usize, ContractError> {
        complete::<RegistrationSet>(
            self.construction_heap_bytes()?,
            self.construction_heap_allocations(),
        )
    }
    pub fn build_visits(&self) -> Result<usize, ContractError> {
        self.visits.checked_mul(2).ok_or(ContractError::Capacity)
    }
    pub fn build(
        self,
        max_bytes: usize,
        max_visits: usize,
    ) -> Result<RegistrationSet, ContractError> {
        bytes::fits(self.construction_charge()?, max_bytes)?;
        bytes::fits(self.build_visits()?, max_visits)?;
        let mut remaining = bytes::add(
            self.construction_heap_bytes()?,
            overhead(self.construction_heap_allocations())?,
        )?;
        let mut rows = reserve(self.fields.rows, &mut remaining)?;
        let actual = inspect(self.claim, self.fields, self.source, self.visits, |row| {
            rows.push(row);
            Ok(())
        })?;
        if actual != self.fingerprint {
            return Err(ContractError::ContentConflict);
        }
        let mut work = Work::new(self.visits);
        for (index, row) in rows.iter().enumerate() {
            work.charge(1)?;
            for old in rows.iter().take(index) {
                work.charge(1)?;
                duplicate(*old, *row)?;
            }
        }
        let restored = RegistrationSet {
            claim: self.fields.claim,
            policy: self.policy,
            rows,
            max_rows: self.fields.max_rows,
            sealed: self.fields.sealed,
            increments_sealed: self.fields.increments_sealed,
            sealed_at: self.fields.sealed_at,
        };
        bytes::fits(restored.allocation_charge()?, max_bytes)?;
        Ok(restored)
    }
}
fn row_quote(rows: usize, declarations: usize) -> Result<usize, ContractError> {
    bytes::add(
        32,
        bytes::add(
            rows.checked_mul(bytes::add(declarations, 64)?)
                .ok_or(ContractError::Capacity)?,
            rows.checked_mul(rows).ok_or(ContractError::Capacity)?,
        )?,
    )
}
fn inspect<S: RegistrationSnapshotSource + ?Sized>(
    claim: &ClaimState,
    fields: RegistrationSnapshotV1,
    source: &S,
    max_visits: usize,
    mut emit: impl FnMut(RegisteredEvaluation) -> Result<(), ContractError>,
) -> Result<ContentHash, ContractError> {
    let mut work = Work::new(max_visits);
    work.charge(16)?;
    let mut hash = Hash::new("focal model registration snapshot plan 1");
    hash.binding(fields.claim);
    hash.count(fields.rows)?;
    hash.count(fields.max_rows)?;
    hash.u8(u8::from(fields.sealed));
    hash.u8(u8::from(fields.increments_sealed));
    if let Some(sequence) = fields.sealed_at {
        hash.u8(1);
        hash.u64(sequence.0);
    } else {
        hash.u8(0);
    }
    let mut values = source.rows();
    for index in 0..fields.rows {
        work.charge(bytes::add(claim.acceptance().declarations().len(), 64)?)?;
        let value = values.next().ok_or(ContractError::InvalidManifest)??;
        claim.acceptance().check_declaration(value.declaration)?;
        let row = RegisteredEvaluation::hydrate_member_v1(
            value.member,
            value.declaration,
            value.evaluation,
        )?;
        let mut prior = source.rows();
        for _ in 0..index {
            work.charge(1)?;
            let old = prior.next().ok_or(ContractError::InvalidManifest)??;
            if old.member.binding.object == row.binding().object
                && same_target(old.member.target, row.target())
            {
                return Err(ContractError::StaleEvaluation);
            }
        }
        hash.binding(row.binding());
        hash_target(&mut hash, row.target());
        hash.u64(row.generation());
        hash_receipt(&mut hash, row.receipt());
        hash.u64(u64::from(row.declaration_index()));
        hash.u8(match row.mode() {
            ValidationMode::Required => 0,
            ValidationMode::Observe => 1,
        });
        hash.raw(row.definition_stamp().as_bytes());
        hash.binding(value.evaluation.binding());
        emit(row)?;
    }
    work.charge(1)?;
    if values.next().is_some() {
        return Err(ContractError::InvalidManifest);
    }
    Ok(hash.finish())
}
fn duplicate(left: RegisteredEvaluation, right: RegisteredEvaluation) -> Result<(), ContractError> {
    if left.binding().object == right.binding().object && same_target(left.target(), right.target())
    {
        Err(ContractError::StaleEvaluation)
    } else {
        Ok(())
    }
}
fn hash_receipt(hash: &mut Hash, value: Option<ReceiptFence>) {
    if let Some(receipt) = value {
        hash.u8(1);
        hash.raw(&receipt.receipt.0);
        hash.u64(receipt.epoch);
    } else {
        hash.u8(0);
    }
}
fn hash_target(hash: &mut Hash, value: Target) {
    match value {
        Target::Artifact {
            response,
            slot,
            artifact,
        } => {
            hash.u8(0);
            hash.binding(response);
            hash.u64(u64::from(slot));
            hash.binding(artifact);
        }
        Target::MissingSlot { response, slot } => {
            hash.u8(1);
            hash.binding(response);
            hash.u64(u64::from(slot));
        }
        Target::Delivery { response } => {
            hash.u8(2);
            hash.binding(response);
        }
        Target::Admission { claim } => {
            hash.u8(3);
            hash.binding(claim);
        }
        Target::Increment { claim, artifact } => {
            hash.u8(4);
            hash.binding(claim);
            hash.binding(artifact);
        }
    }
}
