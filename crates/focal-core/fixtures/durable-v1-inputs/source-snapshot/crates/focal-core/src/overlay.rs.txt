//! Owned per-entry row updates; earlier versions stay immutable until epoch audit.
use crate::access::{Recorder, TableKey};
use crate::*;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RowWrites {
    pub(crate) claims: BTreeMap<ClaimId, Claim>,
    pub(crate) validations: BTreeMap<ValidationId, Validation>,
    pub(crate) artifacts: BTreeMap<ArtifactId, Artifact>,
    pub(crate) testaments: BTreeMap<TestamentId, Testament>,
    pub(crate) evidence_sets: BTreeMap<EvidenceSetId, EvidenceSet>,
    pub(crate) runs: BTreeMap<ValidationRunId, ValidationRun>,
    pub(crate) monitors: BTreeMap<MonitorId, Monitor>,
    pub(crate) identities: BTreeMap<(ObjectKind, ContentHash), ObjectId>,
    pub(crate) epochs: BTreeMap<ParticipantId, EpochWindow>,
    pub(crate) receipts: BTreeMap<RequestKey, MutationReceipt>,
    pub(crate) structural: BTreeSet<StateTable>,
    pub(crate) added: BTreeMap<StateTable, usize>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RowVersion {
    pub(crate) sequence: SessionSeq,
    pub(crate) rows: RowWrites,
}
impl RowWrites {
    pub(crate) fn publish(self, state: &mut State) {
        state.claims.extend(self.claims);
        state.validations.extend(self.validations);
        state.artifacts.extend(self.artifacts);
        state.testaments.extend(self.testaments);
        state.evidence_sets.extend(self.evidence_sets);
        state.runs.extend(self.runs);
        state.monitors.extend(self.monitors);
        state.identities.extend(self.identities);
        state.epochs.extend(self.epochs);
        state.receipts.extend(self.receipts);
    }
}

pub(crate) enum Backing<'a, K, V> {
    Read(&'a BTreeMap<K, V>),
    Write(&'a mut BTreeMap<K, V>),
    Overlay {
        base: &'a BTreeMap<K, V>,
        prior: &'a [Option<RowVersion>],
        select: fn(&RowWrites) -> &BTreeMap<K, V>,
        own: BTreeMap<K, V>,
        sequence: SessionSeq,
        added: usize,
    },
}
impl<K: TableKey, V: Clone + Serialize> Backing<'_, K, V> {
    pub(crate) fn get(&self, key: &K, base_version: SessionSeq) -> Option<(&K, &V, SessionSeq)> {
        match self {
            Self::Read(map) => map.get_key_value(key).map(|(k, v)| (k, v, base_version)),
            Self::Write(map) => map.get_key_value(key).map(|(k, v)| (k, v, base_version)),
            Self::Overlay {
                base,
                prior,
                select,
                own,
                sequence,
                ..
            } => {
                if let Some((k, v)) = own.get_key_value(key) {
                    return Some((k, v, *sequence));
                }
                for version in prior.iter().rev().flatten() {
                    if let Some((k, v)) = select(&version.rows).get_key_value(key) {
                        return Some((k, v, version.sequence));
                    }
                }
                base.get_key_value(key).map(|(k, v)| (k, v, base_version))
            }
        }
    }
    pub(crate) fn watermark(&self, base_version: SessionSeq, count: bool) -> SessionSeq {
        match self {
            Self::Read(_) | Self::Write(_) => base_version,
            Self::Overlay {
                prior,
                select,
                own,
                sequence,
                added,
                ..
            } => {
                if if count { *added != 0 } else { !own.is_empty() } {
                    return *sequence;
                }
                prior
                    .iter()
                    .rev()
                    .flatten()
                    .find(|version| {
                        if count {
                            version.rows.structural.contains(&K::TABLE)
                        } else {
                            !select(&version.rows).is_empty()
                        }
                    })
                    .map_or(base_version, |version| version.sequence)
            }
        }
    }
    pub(crate) fn next(
        &self,
        after: Option<K>,
        base_version: SessionSeq,
    ) -> Option<(&K, &V, SessionSeq)> {
        let first = |map: &BTreeMap<K, V>| {
            map.range((after.map_or(Unbounded, Excluded), Unbounded))
                .next()
                .map(|(key, _)| *key)
        };
        let key = match self {
            Self::Read(map) => first(map),
            Self::Write(map) => first(map),
            Self::Overlay {
                base,
                prior,
                select,
                own,
                ..
            } => std::iter::once(first(base))
                .chain(
                    prior
                        .iter()
                        .flatten()
                        .map(|version| first(select(&version.rows))),
                )
                .chain(std::iter::once(first(own)))
                .flatten()
                .min(),
        }?;
        self.get(&key, base_version)
    }
    pub(crate) fn len(&self) -> Option<usize> {
        match self {
            Self::Read(map) => Some(map.len()),
            Self::Write(map) => Some(map.len()),
            Self::Overlay {
                base, prior, added, ..
            } => prior
                .iter()
                .flatten()
                .try_fold(base.len(), |sum, version| {
                    sum.checked_add(version.rows.added.get(&K::TABLE).copied().unwrap_or(0))
                })
                .and_then(|sum| sum.checked_add(*added)),
        }
    }
    pub(crate) fn native_iter(&self) -> Option<std::collections::btree_map::Iter<'_, K, V>> {
        match self {
            Self::Read(map) => Some(map.iter()),
            Self::Write(map) => Some(map.iter()),
            Self::Overlay { .. } => None,
        }
    }
    pub(crate) fn insert(&mut self, key: K, value: V, recorder: &Recorder) {
        let existed = self.get(&key, recorder.base()).is_some();
        match self {
            Self::Read(_) => recorder.invalidate(),
            Self::Write(map) => {
                map.insert(key, value);
            }
            Self::Overlay { own, added, .. } => {
                if recorder.reserve_value(&value) {
                    if !existed {
                        let Some(next) = added.checked_add(1) else {
                            recorder.invalidate();
                            return;
                        };
                        *added = next;
                    }
                    own.insert(key, value);
                }
            }
        }
    }
    pub(crate) fn get_mut(&mut self, key: &K, recorder: &Recorder) -> Option<&mut V> {
        if let Self::Overlay { own, .. } = self
            && !own.contains_key(key)
        {
            let (_, value, _) = self.get(key, recorder.base())?;
            if !recorder.reserve_value(value) {
                return None;
            }
            let value = value.clone();
            if let Self::Overlay { own, .. } = self {
                own.insert(*key, value);
            }
        }
        match self {
            Self::Read(_) => {
                recorder.invalidate();
                None
            }
            Self::Write(map) => map.get_mut(key),
            Self::Overlay { own, .. } => own.get_mut(key),
        }
    }
    pub(crate) fn into_writes(self) -> Option<(BTreeMap<K, V>, usize)> {
        match self {
            Self::Overlay { own, added, .. } => Some((own, added)),
            _ => None,
        }
    }
}
