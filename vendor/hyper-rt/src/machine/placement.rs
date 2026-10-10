//! Where a daemon's shards run (§4.3 "Shards = performance cores"; D-9 thread-per-core; §4.1 "cores and
//! classes"): the one rule the runtime places its shard threads by (`slates-rt`'s `shard_cores`) and the
//! wake probe places its pair by ([`crate::machine::wake`]), so the probe measures the placement production runs
//! under.
//!
//! **How many.** Every core of the fastest class the process may run on, as many as its CPU budget lets
//! run at once, less the cores the consumer reserves for its threads that are not shards (its device
//! issuers, its blocking pool, stdio); at least one. slates kept exactly one, for control and the OS; here
//! the count is the consumer's, from the CPU those threads are measured to take (docs/runtime.md §10.3).
//! slates' worked example, with one reserved: six "Super" cores and no quota give five shards; a two-CPU
//! quota gives one.
//!
//! **Fixed to cores only when they are the process's own.** A shard is fixed to a core — the
//! thread-per-core model's cache and wake locality — only when the process's CPU budget buys every core it
//! may run on at once ([`crate::machine::facts::CpuBudget::covers`]): no quota (a machine, or a cpuset given to the
//! daemon alone), or a quota that covers the cpuset, as Kubernetes' static CPU manager gives a Guaranteed
//! pod with integer CPUs exclusive cores ("Only containers that are both part of a Guaranteed pod and have
//! integer CPU requests are assigned exclusive CPUs" [B: Kubernetes, "Control CPU Management Policies on
//! the Node"]). A quota below the cpuset is a share of time on a pool the process shares with every other
//! tenant: fixed to cores, every daemon under such a quota picks the same ones — each derives them from
//! the same facts — and waits for them while the rest of the pool idles. So there the operating system
//! places the shards, as it places everything else in the pool.
//!
//! Measured on 2026-09-29 in the KIND lane (a Docker VM of 18 virtual CPUs): eight daemons under a
//! two-CPU quota each fixed its one shard to CPU 1, having derived their cores from a count the quota
//! lowered. The VM was 90 % idle while each shard ran 12 % of the time and waited for its CPU 80 %
//! (`cpu.pressure` some = full = 79–81 %, no quota throttling), and the anchors killed the daemons for
//! late heartbeats every minute or two
//! (docs/bugs/2026-09-29-every-daemon-under-a-cpu-quota-pinned-its-shard-to-cpu-1.md).
//!
//! Deployed thread-per-core runtimes draw the same line by hand: Seastar fixes threads to cores by default
//! and asks an operator to pass `overprovisioned` — "Run in an overprovisioned environment (such as docker
//! or a laptop). Equivalent to: idle_poll_time_us = 0, smp_options::thread_affinity = 0, poll_aio = 0"
//! [C: seastar include/seastar/core/reactor_config.hh, fetched 2026-09-29]. slates derives the line from
//! the facts instead: no switch, no hand-set value (R3, R8).
//!
//! Two daemons given one cpuset with no quota between them still share its cores. That is the one shape
//! the facts cannot show, and it is an operator's to avoid: one daemon per machine, or a cpuset or a quota
//! per daemon.

use crate::derived;
use crate::machine::derived::Derived;
use crate::machine::facts::{CoreFacts, CpuBudget};

/// Where a process's shards run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    /// How many shards run.
    pub shards: Derived<u16>,
    /// The cores they are fixed to, when the process owns its cores; `None` when the OS places every shard.
    pub fixed: Option<FixedCores>,
}

/// The cores of a placement that fixes its shards.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedCores {
    /// The cores reserved for the consumer's threads that are not shards: the fastest class's first, as
    /// many as it reserves. The wake probe's waker runs on the first.
    pub reserved: Vec<u32>,
    /// The core each shard is fixed to, in shard order: the class's other cores, or its first core, shared,
    /// when the reservation leaves none.
    pub shards: Vec<u32>,
}

impl Placement {
    /// The placement for a process that may run on `cores` under `budget` (the machine facts'
    /// [`crate::machine::facts::Facts::cores`] and [`crate::machine::facts::Facts::cpu_budget`]), keeping
    /// `reserved` cores for the consumer's threads that are not shards.
    pub fn of(cores: &[CoreFacts], budget: Option<CpuBudget>, reserved: u16) -> Placement {
        let best = cores.iter().map(|core| core.level).min();
        let mut class: Vec<u32> = cores
            .iter()
            .filter(|core| Some(core.level) == best)
            .map(|core| core.id)
            .collect();
        class.sort_unstable();
        let at_once = budget.map_or(u64::MAX, |budget| budget.whole_cpus());
        let usable = u64::try_from(class.len()).unwrap_or(u64::MAX).min(at_once);
        let count = u16::try_from(usable.saturating_sub(u64::from(reserved)))
            .unwrap_or(u16::MAX)
            .max(1);
        let shards = derived!(
            count,
            "fastest-class cores the CPU budget runs at once, less the consumer's reserved cores, at least one",
            ["cores.class", "cores.level", "cpu_budget", "reserved_cores"]
        );
        let owned = budget.is_none_or(|budget| budget.covers(cores.len()));
        let fixed = owned
            .then(|| fixed_cores(&class, usize::from(reserved)))
            .flatten();
        Placement { shards, fixed }
    }
}

/// The reserved cores and the shard cores of a class the process owns: its first `reserved` cores kept,
/// each other one a shard's; when the reservation leaves no core, the one shard shares the class's first.
/// `None` for an empty class.
fn fixed_cores(class: &[u32], reserved: usize) -> Option<FixedCores> {
    let first = *class.first()?;
    let kept = reserved.min(class.len().saturating_sub(1));
    let (reserved, shards) = class.split_at(kept);
    Some(FixedCores {
        reserved: reserved.to_vec(),
        shards: if shards.is_empty() {
            vec![first]
        } else {
            shards.to_vec()
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::facts::CoreClass;

    fn core(id: u32, level: u32) -> CoreFacts {
        CoreFacts {
            id,
            class: CoreClass::Unknown,
            level,
            numa: 0,
            l2_bytes: 0,
        }
    }

    fn same_class(ids: impl IntoIterator<Item = u32>) -> Vec<CoreFacts> {
        ids.into_iter().map(|id| core(id, 0)).collect()
    }

    fn budget(cpus: u64) -> Option<CpuBudget> {
        Some(CpuBudget {
            quota_us: cpus * 100_000,
            period_us: 100_000,
        })
    }

    /// §4.3's worked example, by the facts it names: a whole machine with no quota and six fastest cores
    /// beside twelve slower ones fixes five shards to the fastest class's other cores and keeps its first
    /// for control; the slower class is never used.
    #[test]
    fn a_machine_with_no_quota_fixes_a_shard_to_each_fastest_core_but_one() {
        let mut cores: Vec<CoreFacts> = (0..6).map(|id| core(id, 0)).collect();
        cores.extend((6..18).map(|id| core(id, 1)));
        let placement = Placement::of(&cores, None, 1);
        assert_eq!(placement.shards.get(), 5);
        assert_eq!(
            placement.fixed,
            Some(FixedCores {
                reserved: vec![0],
                shards: vec![1, 2, 3, 4, 5]
            })
        );
    }

    /// The failure the KIND lane measured, as facts: a pod of 18 CPUs under a two-CPU quota runs one shard
    /// and fixes it nowhere — the pool is shared in time, so the scheduler places it.
    #[test]
    fn a_quota_below_the_cpuset_runs_what_it_buys_and_fixes_nothing() {
        let placement = Placement::of(&same_class(0..18), budget(2), 1);
        assert_eq!(placement.shards.get(), 1);
        assert_eq!(placement.fixed, None);
        let wider = Placement::of(&same_class(0..18), budget(8), 1);
        assert_eq!(wider.shards.get(), 7);
        assert_eq!(wider.fixed, None);
    }

    /// A cpuset the quota covers is the process's own (Kubernetes' static CPU manager): its shards are fixed
    /// to its cores by their ids, which need not begin at 0.
    #[test]
    fn an_owned_cpuset_fixes_shards_to_its_own_ids() {
        let placement = Placement::of(&same_class([2, 3]), budget(2), 1);
        assert_eq!(placement.shards.get(), 1);
        assert_eq!(
            placement.fixed,
            Some(FixedCores {
                reserved: vec![2],
                shards: vec![3]
            })
        );
        let unbounded = Placement::of(&same_class([9, 4, 7]), None, 1);
        assert_eq!(
            unbounded.fixed,
            Some(FixedCores {
                reserved: vec![4],
                shards: vec![7, 9]
            })
        );
    }

    /// A one-core set shares its core between control and the one shard; no core at all (a refused query)
    /// runs one shard the OS places.
    #[test]
    fn one_core_is_shared_and_no_core_fixes_nothing() {
        let one = Placement::of(&same_class([5]), None, 1);
        assert_eq!(one.shards.get(), 1);
        assert_eq!(
            one.fixed,
            Some(FixedCores {
                reserved: vec![],
                shards: vec![5]
            })
        );
        let none = Placement::of(&[], None, 1);
        assert_eq!(none.shards.get(), 1);
        assert_eq!(none.fixed, None);
    }

    /// A quota's fraction of a CPU buys no extra shard, and a quota below one CPU still runs one; the quota
    /// is compared with the whole cpuset (every class), since every core of it is shared when it falls short.
    #[test]
    fn fractions_round_down_and_the_whole_cpuset_decides_ownership() {
        let fraction = Some(CpuBudget {
            quota_us: 250_000,
            period_us: 100_000,
        });
        let placement = Placement::of(&same_class(0..8), fraction, 1);
        assert_eq!(placement.shards.get(), 1);
        assert_eq!(placement.fixed, None);
        let tiny = Some(CpuBudget {
            quota_us: 50_000,
            period_us: 100_000,
        });
        assert_eq!(Placement::of(&same_class(0..4), tiny, 1).shards.get(), 1);
        // Four fast cores and four slow ones under a four-CPU quota: the quota buys the fast class but not
        // the cpuset, so the pool is shared and nothing is fixed.
        let mut mixed: Vec<CoreFacts> = (0..4).map(|id| core(id, 0)).collect();
        mixed.extend((4..8).map(|id| core(id, 1)));
        let shared = Placement::of(&mixed, budget(4), 1);
        assert_eq!(shared.shards.get(), 3);
        assert_eq!(shared.fixed, None);
    }

    /// The oracle over every small shape: for cpusets drawn from sixteen ids in two classes and every quota
    /// from none to past the set, the placement (1) never fixes a shard outside the process's set or to the
    /// slower class, (2) fixes shards only when the quota buys the whole set, (3) runs at least one shard
    /// and no more than the budget and the fastest class allow, and (4) fixes each shard to a different
    /// core unless the class has one. Two daemons deriving from one shared pool therefore never both fix a
    /// shard in it: a pool shared in time fixes nothing.
    #[test]
    fn no_placement_fixes_a_shard_outside_what_the_process_owns() {
        /// Shape: the id space the oracle draws cpusets from (masks stepped through all 2^16, so sets with
        /// gaps, offsets and both classes; small enough to run in milliseconds).
        const IDS: u32 = 16;
        for mask in (1u32..(1 << IDS)).step_by(97) {
            let cores: Vec<CoreFacts> = (0..IDS)
                .filter(|id| mask & (1 << id) != 0)
                .map(|id| core(id, u32::from(id % 3 == 2)))
                .collect();
            for quota in [
                None,
                budget(1),
                budget(2),
                budget(5),
                budget(u64::from(IDS) + 1),
            ] {
                for reserved in 0..3 {
                    check_placement(&cores, quota, reserved);
                }
            }
        }
    }

    /// One shape of the oracle: the design's count, and cores fixed only on an owned set.
    fn check_placement(cores: &[CoreFacts], quota: Option<CpuBudget>, reserved: u16) {
        let placement = Placement::of(cores, quota, reserved);
        let shards = usize::from(placement.shards.get());
        let best = cores.iter().map(|c| c.level).min();
        let class = cores.iter().filter(|c| Some(c.level) == best).count();
        // The design's count: the fastest class's cores the budget runs at once, less the reserved, at least one.
        let buys = quota.map_or(usize::MAX, |b| usize::try_from(b.whole_cpus()).unwrap());
        assert_eq!(
            shards,
            class.min(buys).saturating_sub(usize::from(reserved)).max(1),
            "{cores:?} {quota:?}"
        );
        let owned = quota.is_none_or(|b| b.covers(cores.len()));
        match &placement.fixed {
            None => assert!(!owned || cores.is_empty(), "{cores:?} {quota:?}"),
            Some(fixed) => {
                assert!(owned, "fixed on a shared pool: {cores:?} {quota:?}");
                check_fixed(cores, fixed, shards);
            }
        }
    }

    /// A fixed placement's cores: every one the process's own and of its fastest class, one per shard, none
    /// shared with control unless the class has one core.
    fn check_fixed(cores: &[CoreFacts], fixed: &FixedCores, shards: usize) {
        let best = cores.iter().map(|c| c.level).min();
        let level_of = |id: u32| cores.iter().find(|c| c.id == id).map(|c| c.level);
        for id in fixed.shards.iter().chain(&fixed.reserved) {
            assert_eq!(
                level_of(*id),
                best,
                "{id} is not the process's fastest: {cores:?}"
            );
        }
        let mut distinct = fixed.shards.clone();
        distinct.dedup();
        assert_eq!(distinct.len(), fixed.shards.len(), "{fixed:?}");
        assert!(
            fixed
                .shards
                .iter()
                .all(|shard| !fixed.reserved.contains(shard)),
            "{fixed:?}"
        );
        assert_eq!(fixed.shards.len(), shards, "{fixed:?}");
    }
}
