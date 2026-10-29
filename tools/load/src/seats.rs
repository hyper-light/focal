//! How a run divides its callers, its lifecycles and its agents among the ledgers it drives
//! (`WorkloadShape::ledgers`). Ledger `k` of `L` takes callers `k`, `k + L`, …, agents `k`,
//! `k + L`, …, and every `L`th of the lifecycles, those at places `k`, `k + L`, … of the run's one
//! schedule, so each ledger is offered the same share of the rate whatever its callers. Within a
//! ledger its callers interleave as a run's callers always have: seat `j` of `C` takes the
//! ledger's lifecycles `j`, `j + C`, …, and a lifecycle's agents are found around the ledger's own
//! ring by that place. One ledger is the run as it was: caller `j` of `C` at places `j`, `j + C`,
//! ….
use crate::error::LoadError;

/// `total` split evenly over `parts`; the first `total % parts` parts get one more.
pub fn share(total: u64, parts: u16, index: u16) -> Result<u64, LoadError> {
    let parts = u64::from(parts);
    let each = total
        .checked_div(parts)
        .ok_or(LoadError::Bound("zero workers"))?;
    let extra = total
        .checked_rem(parts)
        .ok_or(LoadError::Bound("zero workers"))?;
    Ok(if u64::from(index) < extra {
        each.saturating_add(1)
    } else {
        each
    })
}

/// Where a caller sits: the ledger it drives, of how many, and its seat among that ledger's
/// callers, of how many.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seat {
    pub ledger: u16,
    pub ledgers: u16,
    pub seat: u16,
    pub seats: u16,
}

impl Seat {
    /// Caller `index` of `callers`, the callers divided among `ledgers`: ledger `index mod
    /// ledgers`, seat `index div ledgers`. Every ledger takes a caller at least.
    pub fn of(index: u16, callers: u16, ledgers: u16) -> Result<Self, LoadError> {
        if index >= callers || ledgers > callers {
            return Err(LoadError::Bound("callers"));
        }
        let ledger = index
            .checked_rem(ledgers)
            .ok_or(LoadError::Bound("zero ledgers"))?;
        let seat = index
            .checked_div(ledgers)
            .ok_or(LoadError::Bound("zero ledgers"))?;
        let seats = u16::try_from(share(u64::from(callers), ledgers, ledger)?)
            .map_err(|_| LoadError::Bound("callers"))?;
        Ok(Self {
            ledger,
            ledgers,
            seat,
            seats,
        })
    }
    /// The lifecycles the caller starts: its seat's share of its ledger's share of `claims`.
    pub fn lifecycles(&self, claims: u64) -> Result<u64, LoadError> {
        share(
            share(claims, self.ledgers, self.ledger)?,
            self.seats,
            self.seat,
        )
    }
    /// The caller's lifecycle `i`'s place among its ledger's: its agents are found around the
    /// ledger's ring by it.
    pub fn local(&self, i: u64) -> Result<u64, LoadError> {
        i.checked_mul(u64::from(self.seats))
            .and_then(|place| place.checked_add(u64::from(self.seat)))
            .ok_or(LoadError::Bound("lifecycle places"))
    }
    /// The caller's lifecycle `i`'s place on the run's one schedule: its ledger's places, the
    /// ledgers' interleaved.
    pub fn global(&self, i: u64) -> Result<u64, LoadError> {
        self.local(i)?
            .checked_mul(u64::from(self.ledgers))
            .and_then(|place| place.checked_add(u64::from(self.ledger)))
            .ok_or(LoadError::Bound("schedule"))
    }
}

/// Where an agent sits: the ledger it is a principal on, and its index among that ledger's
/// agents, its place around the ledger's ring and among the ledger's homes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentSeat {
    pub ledger: u16,
    pub index: u16,
}

/// Agent `agent`, the agents divided among `ledgers`: ledger `agent mod ledgers`, index `agent div
/// ledgers`.
pub fn agent_seat(agent: u16, ledgers: u16) -> Result<AgentSeat, LoadError> {
    Ok(AgentSeat {
        ledger: agent
            .checked_rem(ledgers)
            .ok_or(LoadError::Bound("zero ledgers"))?,
        index: agent
            .checked_div(ledgers)
            .ok_or(LoadError::Bound("zero ledgers"))?,
    })
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects
    )
)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Every place on the schedule is taken once, by the ledger it falls to: each ledger takes
    /// every `L`th place and its share of the lifecycles, however unevenly the callers divide, and
    /// within each ledger its callers' places cover its lifecycles once each.
    #[test]
    fn the_schedule_is_divided_among_the_ledgers_place_by_place() {
        for (claims, callers, ledgers) in [
            (1u64, 1u16, 1u16),
            (10, 4, 1),
            (33, 4, 2),
            (10, 4, 3),
            (100, 16, 11),
            (7, 11, 11),
            (64, 64, 64),
        ] {
            let mut global = Vec::new();
            let mut local: BTreeMap<u16, Vec<u64>> = BTreeMap::new();
            for index in 0..callers {
                let seat = Seat::of(index, callers, ledgers).unwrap();
                assert_eq!(seat.ledger, index % ledgers);
                for i in 0..seat.lifecycles(claims).unwrap() {
                    let place = seat.global(i).unwrap();
                    assert_eq!(place % u64::from(ledgers), u64::from(seat.ledger));
                    global.push(place);
                    local
                        .entry(seat.ledger)
                        .or_default()
                        .push(seat.local(i).unwrap());
                }
            }
            global.sort_unstable();
            assert_eq!(
                global,
                (0..claims).collect::<Vec<_>>(),
                "{claims} claims, {callers} callers, {ledgers} ledgers"
            );
            for ledger in 0..ledgers {
                let mut places = local.remove(&ledger).unwrap_or_default();
                places.sort_unstable();
                let lifecycles = share(claims, ledgers, ledger).unwrap();
                assert_eq!(places, (0..lifecycles).collect::<Vec<_>>());
            }
        }
    }

    /// One ledger seats its callers as a run always has: caller `j` of `C` at places `j`,
    /// `j + C`, ….
    #[test]
    fn one_ledger_is_the_run_as_it_was() {
        let seat = Seat::of(3, 16, 1).unwrap();
        assert_eq!(
            seat,
            Seat {
                ledger: 0,
                ledgers: 1,
                seat: 3,
                seats: 16
            }
        );
        assert_eq!(seat.global(2).unwrap(), 2 * 16 + 3);
        assert_eq!(seat.local(2).unwrap(), seat.global(2).unwrap());
        assert_eq!(seat.lifecycles(100).unwrap(), share(100, 16, 3).unwrap());
    }

    /// Callers and agents divide among the ledgers alike, the first ledgers one more where they
    /// do not divide evenly; more ledgers than callers leaves a ledger undriven, refused.
    #[test]
    fn callers_and_agents_divide_among_the_ledgers() {
        let seats: Vec<_> = (0..16)
            .map(|index| Seat::of(index, 16, 11).unwrap())
            .collect();
        for ledger in 0..11u16 {
            let its: Vec<_> = seats.iter().filter(|seat| seat.ledger == ledger).collect();
            assert_eq!(its.len(), if ledger < 5 { 2 } else { 1 });
            assert!(its.iter().all(|seat| usize::from(seat.seats) == its.len()));
            let mut indexes: Vec<_> = its.iter().map(|seat| seat.seat).collect();
            indexes.sort_unstable();
            assert_eq!(indexes, (0..its.len() as u16).collect::<Vec<_>>());
        }
        assert!(Seat::of(0, 2, 3).is_err());
        assert!(Seat::of(2, 2, 1).is_err());
        let agents: Vec<_> = (0..8).map(|agent| agent_seat(agent, 3).unwrap()).collect();
        assert_eq!(
            agents
                .iter()
                .map(|seat| (seat.ledger, seat.index))
                .collect::<Vec<_>>(),
            vec![
                (0, 0),
                (1, 0),
                (2, 0),
                (0, 1),
                (1, 1),
                (2, 1),
                (0, 2),
                (1, 2)
            ]
        );
        for ledger in 0..3 {
            let count = agents.iter().filter(|seat| seat.ledger == ledger).count();
            assert_eq!(count as u64, share(8, 3, ledger).unwrap());
        }
    }
}
