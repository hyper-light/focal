//! What the runtime measures of the machine and derives its configuration from (docs/runtime.md §10):
//! the facts it places shards by, the null system call's cost and the wake latency. slates' profile
//! measures far more (faults, memcpy, hashing, codecs, lock capacity) to size its storage; a runtime
//! needs these three, and a short command needs them without paying the probes (§10.2's stored record).

use std::time::Duration;

use crate::derived;
use crate::machine::bench::Measurement;
use crate::machine::derived::Derived;
use crate::machine::error::MachineError;
use crate::machine::facts::Facts;
use crate::machine::placement::Placement;
use crate::machine::probes;
use crate::machine::wake::{self, WakeLatency};

/// The machine as the runtime reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Calibration {
    /// The fixed facts: cores, CPU budget, page, power.
    pub facts: Facts,
    /// The null system call.
    pub syscall: Measurement,
    /// The wake latency: the expected cost of parking ([`crate::machine::wake`]).
    pub wake: WakeLatency,
}

/// What the consumer states that the machine cannot (docs/runtime.md §3.3, §3.6, §10.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Cores kept off the shards for the consumer's threads that are not shards: from the CPU those threads
    /// are measured to take, rounded up to whole cores, or the count of them that run continuously.
    pub reserved_cores: u16,
    /// How late a timer may fire, nanoseconds; a machine whose `tick + wake p99` exceeds it is refused.
    /// `None` when the consumer has stated none (every lateness is accepted and reported).
    pub lateness_tolerance_ns: Option<u64>,
    /// The consumer's latency objective for one loop step, nanoseconds, when tighter than the mean wake.
    pub latency_objective_ns: Option<u64>,
}

/// The runtime's constants, each with its formula (docs/runtime.md §10.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Constants {
    /// How long an idle shard spins before parking, and the step quantum: the expected cost of parking, the
    /// 2-competitive spin-then-park threshold [A: Karlin, Li, Manasse, Owicki, SOSP'91].
    pub spin_ns: Derived<u64>,
    /// The timing wheel's tick.
    pub tick_ns: Derived<u64>,
    /// The latency budget the loop's batch is sized against.
    pub batch_budget_ns: Derived<u64>,
    /// The control queue's depth.
    pub control_entries: Derived<u64>,
    /// Where the shards run.
    pub placement: Placement,
}

/// A configuration the machine and the consumer's policy cannot meet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unmet {
    /// The lateness a timer can reach on this machine: `tick + wake p99`, nanoseconds.
    pub lateness_ns: u64,
    /// The consumer's tolerance it exceeds.
    pub tolerance_ns: u64,
}

impl Calibration {
    /// Measures the machine: the facts, then the system-call and wake probes, each within `budget`.
    /// Refused `MeasurementTimeout` when the wake probe could not measure at all within its bound.
    pub fn measure(budget: Duration, reserved_cores: u16) -> Result<Calibration, MachineError> {
        let facts = Facts::query();
        let syscall = probes::syscall(budget);
        let placement = Placement::of(&facts.cores, facts.cpu_budget, reserved_cores);
        let wake = wake::wake(budget, &placement)?;
        Ok(Calibration {
            facts,
            syscall,
            wake,
        })
    }

    /// The constants under the consumer's `policy`, or the lateness the machine cannot keep.
    pub fn constants(&self, policy: &Policy) -> Result<Constants, Unmet> {
        let wake_mean = self.wake.mean_ns.max(1);
        let wake_p99 = self.wake.p99_ns.max(1);
        let syscall = self.syscall.median_ns().max(1);
        let lateness_ns = wake_mean.saturating_add(wake_p99);
        if let Some(tolerance_ns) = policy.lateness_tolerance_ns
            && lateness_ns > tolerance_ns
        {
            return Err(Unmet {
                lateness_ns,
                tolerance_ns,
            });
        }
        let batch_budget = policy
            .latency_objective_ns
            .map_or(wake_mean, |objective| objective.min(wake_mean));
        Ok(Constants {
            spin_ns: derived!(
                wake_mean,
                "wake.mean (the expected cost of parking: the 2-competitive spin-then-park threshold)",
                ["wake.mean_ns"]
            ),
            tick_ns: derived!(
                wake_mean,
                "wake.mean (a parked shard cannot fire a timer sooner than it wakes; lateness = tick + wake.p99)",
                ["wake.mean_ns"]
            ),
            batch_budget_ns: derived!(
                batch_budget,
                "min(wake.mean, the consumer's latency objective) (a batch delays a woken task no longer than a park would)",
                ["wake.mean_ns", "policy.latency_objective_ns"]
            ),
            control_entries: derived!(
                wake_p99
                    .checked_div(syscall)
                    .unwrap_or(1)
                    .max(1)
                    .next_power_of_two(),
                "Little's law at the overflow target: one message per syscall.median for a wake at its p99, rounded up to a power of two",
                ["wake.p99_ns", "syscall.median"]
            ),
            placement: Placement::of(
                &self.facts.cores,
                self.facts.cpu_budget,
                policy.reserved_cores,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quick() -> Calibration {
        Calibration::measure(Duration::from_millis(40), 1).expect("the machine calibrates")
    }

    #[test]
    fn a_calibration_measures_a_wake_and_a_syscall() {
        let calibration = quick();
        assert!(calibration.wake.mean_ns > 0);
        assert!(calibration.syscall.median_ns() > 0);
    }

    #[test]
    fn the_constants_follow_the_wake_and_a_tolerance_below_the_lateness_is_refused() {
        let calibration = quick();
        let open = Policy {
            reserved_cores: 1,
            lateness_tolerance_ns: None,
            latency_objective_ns: None,
        };
        let constants = calibration.constants(&open).unwrap();
        assert_eq!(constants.tick_ns.get(), calibration.wake.mean_ns.max(1));
        assert_eq!(constants.spin_ns.get(), calibration.wake.mean_ns.max(1));
        assert!(constants.control_entries.get().is_power_of_two());
        let tight = Policy {
            lateness_tolerance_ns: Some(1),
            ..open
        };
        let unmet = calibration.constants(&tight).unwrap_err();
        assert_eq!(unmet.tolerance_ns, 1);
        assert!(unmet.lateness_ns > 1);
        let objective = Policy {
            latency_objective_ns: Some(1),
            ..open
        };
        assert_eq!(
            calibration
                .constants(&objective)
                .unwrap()
                .batch_budget_ns
                .get(),
            1
        );
    }
}
