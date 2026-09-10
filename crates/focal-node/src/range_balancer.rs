//! Automatic layout decisions (25 §8): a member that stays past twice the
//! target for several observations is split near its middle; two adjacent
//! members that both stay under a quarter of it are merged. Observations
//! are per member and reset whenever the condition lapses, so a burst
//! never reshapes a group; every decision is one session-log record the
//! authority proposes, and a refusal is simply observed again.
use focal_memory::RangeId;
use focal_model::LedgerId;
use std::collections::BTreeMap;

/// The operator's target rows per member; a campaign lowers it so a small
/// group reshapes.
pub const TARGET_ENTRIES_ENV: &str = "FOCAL_RANGE_TARGET_ENTRIES";
/// The most `(ledger, member)` observations kept; older ones lapse.
const MAX_OBSERVATIONS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BalancerConfig {
    /// Rows a member should hold.
    pub target_entries: usize,
    /// A member past `split_factor × target` is split.
    pub split_factor: usize,
    /// Two adjacent members under `target / merge_divisor` are merged.
    pub merge_divisor: usize,
    /// Consecutive observations a condition must hold before a decision.
    pub observations: u8,
}
impl Default for BalancerConfig {
    fn default() -> Self {
        Self::standard()
    }
}
impl BalancerConfig {
    pub const fn standard() -> Self {
        Self {
            target_entries: 250_000,
            split_factor: 2,
            merge_divisor: 4,
            observations: 3,
        }
    }
    /// The standard configuration under the operator's target, if set.
    pub fn from_env() -> Self {
        let mut config = Self::standard();
        if let Some(value) = std::env::var_os(TARGET_ENTRIES_ENV)
            && let Some(target) = value
                .to_str()
                .and_then(|text| text.trim().parse::<usize>().ok())
            && target > 0
        {
            config.target_entries = target;
        }
        config
    }
    fn split_above(&self) -> usize {
        self.target_entries.saturating_mul(self.split_factor)
    }
    fn merge_below(&self) -> usize {
        self.target_entries
            .checked_div(self.merge_divisor)
            .unwrap_or(self.target_entries)
    }
}

/// One member's load as the balancer sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemberLoad {
    pub id: RangeId,
    pub entries: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Split this member near its middle.
    Split { member: RangeId },
    /// Merge this member with its successor.
    Merge { left: RangeId },
}

/// Per-member observation counters across passes.
#[derive(Debug, Default)]
pub struct Balancer {
    config: BalancerConfig,
    over: BTreeMap<(LedgerId, RangeId), u8>,
    under: BTreeMap<(LedgerId, RangeId), u8>,
}
impl Balancer {
    pub fn new(config: BalancerConfig) -> Self {
        Self {
            config,
            over: BTreeMap::new(),
            under: BTreeMap::new(),
        }
    }
    pub fn config(&self) -> BalancerConfig {
        self.config
    }
    /// Observe one group's members in key order and decide, at most one
    /// change: a split of the first member past the bound for enough
    /// observations (while the group can grow), else a merge of the first
    /// adjacent pair both under the bound for enough observations.
    pub fn observe(
        &mut self,
        ledger: LedgerId,
        members: &[MemberLoad],
        max_members: usize,
    ) -> Option<Decision> {
        let config = self.config;
        // Members that left the group take their observations with them.
        self.over.retain(|(owner, id), _| {
            *owner != ledger || members.iter().any(|member| member.id == *id)
        });
        self.under.retain(|(owner, id), _| {
            *owner != ledger || members.iter().any(|member| member.id == *id)
        });
        for member in members {
            let key = (ledger, member.id);
            if member.entries > config.split_above() {
                bump(&mut self.over, key);
            } else {
                self.over.remove(&key);
            }
            if member.entries < config.merge_below() {
                bump(&mut self.under, key);
            } else {
                self.under.remove(&key);
            }
        }
        if members.len() < max_members
            && let Some(member) = members.iter().find(|member| {
                self.over
                    .get(&(ledger, member.id))
                    .is_some_and(|count| *count >= config.observations)
            })
        {
            self.over.remove(&(ledger, member.id));
            return Some(Decision::Split { member: member.id });
        }
        if let Some(pair) = members.windows(2).find(|pair| {
            pair.iter().all(|member| {
                self.under
                    .get(&(ledger, member.id))
                    .is_some_and(|count| *count >= config.observations)
            })
        }) && let [left, right] = pair
        {
            self.under.remove(&(ledger, left.id));
            self.under.remove(&(ledger, right.id));
            return Some(Decision::Merge { left: left.id });
        }
        None
    }
}
fn bump(counts: &mut BTreeMap<(LedgerId, RangeId), u8>, key: (LedgerId, RangeId)) {
    if !counts.contains_key(&key) && counts.len() >= MAX_OBSERVATIONS {
        return;
    }
    let count = counts.entry(key).or_insert(0);
    *count = count.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> LedgerId {
        LedgerId {
            tenant: focal_model::TenantId::from_u128(1),
            session: focal_model::SessionId::from_u128(2),
        }
    }
    fn config() -> BalancerConfig {
        BalancerConfig {
            target_entries: 100,
            split_factor: 2,
            merge_divisor: 4,
            observations: 3,
        }
    }
    fn load(id: u128, entries: usize) -> MemberLoad {
        MemberLoad {
            id: RangeId::from_u128(id),
            entries,
        }
    }

    #[test]
    fn a_split_needs_consecutive_observations_and_a_dip_resets_them() {
        let mut balancer = Balancer::new(config());
        let big = [load(1, 250)];
        assert_eq!(balancer.observe(ledger(), &big, 64), None);
        assert_eq!(balancer.observe(ledger(), &big, 64), None);
        // A dip below the bound resets the count.
        assert_eq!(balancer.observe(ledger(), &[load(1, 150)], 64), None);
        assert_eq!(balancer.observe(ledger(), &big, 64), None);
        assert_eq!(balancer.observe(ledger(), &big, 64), None);
        assert_eq!(
            balancer.observe(ledger(), &big, 64),
            Some(Decision::Split {
                member: RangeId::from_u128(1)
            })
        );
        // The decision consumed the observations; the next needs three more.
        assert_eq!(balancer.observe(ledger(), &big, 64), None);
        // A group at its member bound never splits.
        for _ in 0..3 {
            assert_eq!(balancer.observe(ledger(), &big, 1), None);
        }
    }

    #[test]
    fn a_merge_needs_both_adjacent_members_small_for_every_observation() {
        let mut balancer = Balancer::new(config());
        let small = [load(1, 10), load(2, 20), load(3, 500)];
        assert_eq!(balancer.observe(ledger(), &small, 64), None);
        assert_eq!(balancer.observe(ledger(), &small, 64), None);
        // The third member is past the split bound at the same time: a split
        // takes precedence over a merge.
        assert_eq!(
            balancer.observe(ledger(), &small, 64),
            Some(Decision::Split {
                member: RangeId::from_u128(3)
            })
        );
        // With the split consumed, the small pair merges on this pass.
        assert_eq!(
            balancer.observe(ledger(), &small, 64),
            Some(Decision::Merge {
                left: RangeId::from_u128(1)
            })
        );
        // Members not adjacent, or one of them healthy, never merge.
        let apart = [load(1, 10), load(2, 80), load(3, 10)];
        for _ in 0..4 {
            assert_eq!(balancer.observe(ledger(), &apart, 64), None);
        }
        // A member that left takes its observations with it.
        let fresh = [load(4, 10), load(5, 10)];
        assert_eq!(balancer.observe(ledger(), &fresh, 64), None);
        assert_eq!(balancer.observe(ledger(), &fresh, 64), None);
        assert_eq!(
            balancer.observe(ledger(), &fresh, 64),
            Some(Decision::Merge {
                left: RangeId::from_u128(4)
            })
        );
    }
}
