//! Exact write-set ownership for a prepared native mutation. The canonical
//! storage plan supplies keys; final values remain in its one candidate root.
//! This is not a WAL encoding, an imported root or an activation promise.
use super::*;
use focal_memory::{Allocation, Change, RangeWriteEnvelope};
use prepare::{add, array, within};

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MutationKey {
    key: Key,
    deleted: bool,
}

/// Buffers precede their funding owner so destruction remains charged.
#[derive(Debug)]
pub(super) struct WriteSet {
    keys: Vec<MutationKey>,
    profile: NativeContentProfile,
    funding: Option<Allocation>,
}

pub(super) fn bytes(count: usize) -> Result<usize, NativeError> {
    array::<MutationKey>(count)
}

/// Pending write-set storage is retained independently for each promised action.
/// The range's own retained charge covers only pages and the persistent root.
pub(super) fn retained(range: RangeWriteEnvelope) -> Result<usize, NativeError> {
    add(
        range.additional_retained_bytes(),
        bytes(range.limits().changed_keys)?,
    )
}

impl WriteSet {
    pub(super) fn capture(
        profile: NativeContentProfile,
        changes: &[Change<Key, Row>],
        allowance: usize,
        funding: Allocation,
    ) -> Result<Self, NativeError> {
        let required = bytes(changes.len())?;
        within(required, allowance)?;
        if changes.is_empty() || funding.bytes() != required {
            return Err(ContractError::InvalidManifest.into());
        }
        // The complete actual charge is held before allocation. Passing this
        // owner by value avoids a second accounting handle or uncharged handoff.
        #[cfg(test)]
        fail_capture()?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(changes.len())
            .map_err(|_| MemoryError::AllocationFailed)?;
        within(bytes(keys.capacity())?, required)?;
        let mut previous = None;
        for change in changes {
            let key = *change.key();
            if previous.is_some_and(|last| last >= key) || key == Key::End {
                return Err(ContractError::InvalidManifest.into());
            }
            let deleted = match change {
                Change::Put(entry) => {
                    check_family(key, &entry.value)?;
                    false
                }
                Change::Delete(_) => true,
            };
            if keys.len() == keys.capacity() {
                return Err(ContractError::Capacity.into());
            }
            keys.push(MutationKey { key, deleted });
            previous = Some(key);
        }
        Ok(Self {
            keys,
            profile,
            funding: Some(funding),
        })
    }

    /// Reconcile the exact captured plan with the constructed candidate. This
    /// touches only changed keys, including a future explicit deletion. It does
    /// not reconstruct the write set by scanning or diffing full ledger roots.
    pub(super) fn check(&self, range: &PreparedRange<Key, Row>) -> Result<(), NativeError> {
        if self.keys.is_empty()
            || self.funding.as_ref().map(Allocation::bytes) != Some(bytes(self.keys.capacity())?)
        {
            return Err(ContractError::InvalidManifest.into());
        }
        for item in &self.keys {
            match (item.deleted, range.get(&item.key)) {
                (true, None) => {}
                (false, Some(row)) => check_family(item.key, row)?,
                _ => return Err(ContractError::InvalidManifest.into()),
            }
        }
        Ok(())
    }

    pub(super) fn entries(&self) -> impl ExactSizeIterator<Item = (Key, bool)> + '_ {
        self.keys.iter().map(|item| (item.key, item.deleted))
    }

    pub(super) fn len(&self) -> usize {
        self.keys.len()
    }
    pub(super) fn heap_bytes(&self) -> usize {
        self.funding.as_ref().map_or(0, Allocation::bytes)
    }
    pub(super) fn profile(&self) -> NativeContentProfile {
        self.profile
    }

    /// Existing corruption fixtures deliberately replace candidate rows outside
    /// the real constructor. They carry no exportable recorded mutation.
    #[cfg(test)]
    pub(super) fn unrecorded() -> Self {
        Self {
            keys: Vec::new(),
            profile: NativeContentProfile::ProjectionOnly,
            funding: None,
        }
    }
}

pub(super) fn check_family(key: Key, row: &Row) -> Result<(), NativeError> {
    let valid = matches!(
        (key, row),
        (Key::IncomingHead(_), Row::IncomingHead(_))
            | (Key::IncomingLink(..), Row::IncomingLink(_))
            | (Key::Monitor(_), Row::Monitor(_))
            | (Key::MonitorHead(_), Row::MonitorHead(_))
            | (Key::MonitorLink(..), Row::MonitorLink(_))
            | (Key::MissingResult(_), Row::MissingResult(_))
            | (Key::Meta, Row::Meta(_))
            | (Key::Claim(_), Row::Claim(_))
            | (Key::Definition(_), Row::Definition(_))
            | (Key::Evaluation(_), Row::Evaluation(_))
            | (Key::Artifact(_), Row::Artifact(_))
            | (Key::ArtifactIdentity(_), Row::ArtifactIdentity(_))
            | (Key::Accepted(_), Row::Accepted(_))
            | (Key::DeliveryResult(_), Row::DeliveryResult(_))
            | (Key::Receipt(_), Row::Receipt(_))
            | (Key::Cycle(_), Row::Cycle(_))
            | (Key::RetiredCycleHead(_), Row::RetiredCycleHead(_))
            | (Key::RetiredCycle(_), Row::RetiredCycle(_))
            | (Key::Work(_), Row::Work(_))
            | (Key::WorkSlot(..), Row::WorkSlot(_))
            | (Key::Diagnostic(_), Row::Diagnostic(_))
            | (Key::Response(_), Row::Response(_))
            | (Key::ResultTestament(_), Row::ResultTestament(_))
            | (Key::ClaimResultTestament(_), Row::ClaimResultTestament(_))
            | (Key::Outcome(_), Row::Outcome(_))
            | (Key::Event(..), Row::Event(_))
            | (Key::ClaimContent(_), Row::ClaimContent(_))
            | (Key::ClaimIdentity(..), Row::ClaimIdentity(_))
            | (Key::DefinitionIdentity(..), Row::DefinitionIdentity(_))
            | (Key::CreationResult(_), Row::CreationResult(_))
    );
    if valid {
        Ok(())
    } else {
        Err(ContractError::InvalidManifest.into())
    }
}

#[cfg(test)]
thread_local! { static FAIL_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
fn fail_capture() -> Result<(), NativeError> {
    if FAIL_CAPTURE.with(|flag| flag.replace(false)) {
        Err(MemoryError::AllocationFailed.into())
    } else {
        Ok(())
    }
}
