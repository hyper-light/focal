//! Admission of connections by identity (mantle note 32 T9–T11; focal `admission.rs`, 27 §3.1 P5,
//! the audit's F20), owned by the endpoint: no shared handle, no lock.
//!
//! A connection is admitted twice. Before its handshake it takes one of a bounded number of
//! **pending** places, which authenticated connections never use, so handshakes that never
//! authenticate cannot take the capacity of peers that have. Once its certificate names a peer it is
//! charged to that **identity**:
//!
//! - an identity holds at most `per_identity` connections; one past the bound replaces the one of
//!   that identity used longest ago among those that are **idle**, which is closed. A peer that
//!   restarted or moved leaves a connection behind until its idle timeout; refusing the newcomer
//!   would make it wait that out. Only an idle connection is replaced: none of its exchanges under
//!   way, and nothing begun, ended or carried on it for a quiet period (the endpoint's serve
//!   period). A connection carrying an exchange is alive by that exchange, and closing it made the
//!   exchange's outcome unknown to its owner; one between exchanges is about to carry the next.
//!   With none idle the newcomer is refused ([`Refusal::Busy`]): an identity using every
//!   connection it may hold is told so, where replacing made its own callers close each other's
//!   connections (focal, 2026-10-09: 64 callers of one principal against a bound of 16 replaced
//!   27,765 connections in 30 s, and a third of their writes ended unknown);
//! - the connections held in all are bounded by `connections`, met after the replacement rule,
//!   never before it: an identity at its own bound reaches its replacement however full the
//!   endpoint is (F20). A full endpoint gives a connection to an identity under its fair share
//!   (the connections among the identities holding them, the newcomer counted) from the identity
//!   holding the most, when that one holds more than the share and has an idle connection:
//!   max-min fairness, no identity's share grows at the cost of one holding less (Bertsekas and
//!   Gallager, *Data Networks*, §6.5.2). Without one, only the connection that would be one more
//!   is refused;
//! - the identities are bounded, and an identity past its own bound displaces only itself.
//!
//! Every refusal is typed and counted. focal's fixed bounds (4 per node, 16 per participant) are
//! excluded (note 32 T9): every bound here is the owner's configuration.

use std::time::{Duration, Instant};

use crate::{PeerId, Refusal};

/// The bounds of admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Inbound handshakes in progress.
    pub pending: usize,
    /// Identities holding a connection.
    pub identities: usize,
    /// Connections held in all, once authenticated, and dials in progress.
    pub connections: usize,
    /// Connections one identity holds.
    pub per_identity: usize,
}

impl AdmissionLimits {
    fn valid(&self) -> bool {
        self.pending > 0 && self.identities > 0 && self.connections > 0 && self.per_identity > 0
    }
}

/// What admission holds, and what it refused.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AdmissionStats {
    /// Handshakes in progress.
    pub pending: usize,
    /// Identities holding connections.
    pub identities: usize,
    /// Connections held.
    pub connections: usize,
    /// Exchanges under way on the connections held: what keeps a connection from being replaced.
    pub serving: usize,
    /// Connections admitted so far.
    pub admitted: u64,
    /// Connections that replaced one of their identity's, or took an idle one of the identity
    /// holding the most from a full endpoint.
    pub replaced: u64,
    /// Handshakes refused a pending place.
    pub refused_pending: u64,
    /// Connections refused for the identity bound.
    pub refused_identities: u64,
    /// Connections refused for the connection bound.
    pub refused_connections: u64,
    /// Connections refused for an identity at its bound with none of its connections idle.
    pub refused_busy: u64,
    /// Handshakes whose certificate named no peer.
    pub refused_identity: u64,
}

/// A connection, as admission knows it: the endpoint's key for it.
pub(crate) type ConnectionKey = usize;

/// One connection an identity holds.
#[derive(Debug)]
struct Held {
    key: ConnectionKey,
    /// When it last began an exchange, by the endpoint's counter: its place among its identity's
    /// connections (an answer that ends late does not make it the more recently used).
    used: u64,
    /// Its exchanges under way.
    serving: usize,
    /// When it last began or ended an exchange or carried a frame: its quiet counts from here.
    moved: Instant,
}

#[derive(Debug)]
struct Identity {
    peer: PeerId,
    held: Vec<Held>,
}

/// One endpoint's admission.
#[derive(Debug)]
pub(crate) struct Admission {
    limits: AdmissionLimits,
    /// How long a connection must have moved nothing to be idle: the endpoint's serve period.
    quiet: Duration,
    /// Sorted by peer.
    identities: Vec<Identity>,
    stats: AdmissionStats,
}

fn bump(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}

impl Held {
    fn idle(&self, now: Instant, quiet: Duration) -> bool {
        self.serving == 0 && now.saturating_duration_since(self.moved) >= quiet
    }
}

impl Admission {
    /// Admission within `limits`, a connection idle once nothing has moved on it for `quiet`.
    pub(crate) fn new(limits: AdmissionLimits, quiet: Duration) -> Result<Self, Refusal> {
        if !limits.valid() {
            return Err(Refusal::Configuration);
        }
        Ok(Self {
            limits,
            quiet,
            identities: Vec::new(),
            stats: AdmissionStats::default(),
        })
    }
    pub(crate) fn limits(&self) -> AdmissionLimits {
        self.limits
    }
    pub(crate) fn stats(&self) -> AdmissionStats {
        AdmissionStats {
            identities: self.identities.len(),
            ..self.stats
        }
    }
    /// A place for one inbound handshake, held until it authenticates or ends.
    pub(crate) fn begin(&mut self) -> Result<(), Refusal> {
        if self.stats.pending >= self.limits.pending {
            bump(&mut self.stats.refused_pending);
            return Err(Refusal::Pending);
        }
        self.stats.pending = self.stats.pending.saturating_add(1);
        Ok(())
    }
    /// An inbound handshake's place given back.
    pub(crate) fn end_pending(&mut self) {
        self.stats.pending = self.stats.pending.saturating_sub(1);
    }
    /// Whether one more connection may be dialed: dials count against the connection bound.
    pub(crate) fn may_dial(&self, dialing: usize) -> Result<(), Refusal> {
        if self.stats.connections.saturating_add(dialing) >= self.limits.connections {
            return Err(Refusal::Connections);
        }
        Ok(())
    }
    /// A certificate named no peer.
    pub(crate) fn unidentified(&mut self) {
        bump(&mut self.stats.refused_identity);
    }
    /// Charge `connection` to `peer`, admitted at `tick` and `now`. Returns the connection this
    /// one replaces, its identity's or the one a full endpoint took from the identity holding the
    /// most, which the caller closes.
    pub(crate) fn admit(
        &mut self,
        peer: PeerId,
        connection: ConnectionKey,
        tick: u64,
        now: Instant,
    ) -> Result<Option<ConnectionKey>, Refusal> {
        let found = self
            .identities
            .binary_search_by_key(&peer, |identity| identity.peer);
        let held = found
            .ok()
            .and_then(|at| self.identities.get(at))
            .map_or(0, |identity| identity.held.len());
        if held == 0 && found.is_err() && self.identities.len() >= self.limits.identities {
            bump(&mut self.stats.refused_identities);
            return Err(Refusal::Identities);
        }
        let quiet = self.quiet;
        let fresh = Held {
            key: connection,
            used: tick,
            serving: 0,
            moved: now,
        };
        // The replacement rule first: an identity at its bound takes its own place back however
        // full the endpoint is, from the connection it used longest ago of those that are idle.
        if held >= self.limits.per_identity {
            let identity = found
                .ok()
                .and_then(|at| self.identities.get_mut(at))
                .ok_or(Refusal::Identities)?;
            let Some(at) = least_used_idle(&identity.held, now, quiet) else {
                bump(&mut self.stats.refused_busy);
                return Err(Refusal::Busy);
            };
            let replaced =
                std::mem::replace(identity.held.get_mut(at).ok_or(Refusal::Identities)?, fresh);
            bump(&mut self.stats.replaced);
            bump(&mut self.stats.admitted);
            return Ok(Some(replaced.key));
        }
        // A full endpoint: a connection from the identity holding the most, over the share.
        let mut taken = None;
        if self.stats.connections >= self.limits.connections {
            let Some((victim, at)) = self.fair_victim(peer, held, found.is_err(), now) else {
                bump(&mut self.stats.refused_connections);
                return Err(Refusal::Connections);
            };
            let identity = self
                .identities
                .get_mut(victim)
                .ok_or(Refusal::Connections)?;
            if at >= identity.held.len() {
                return Err(Refusal::Connections);
            }
            taken = Some(identity.held.remove(at).key);
            if identity.held.is_empty() {
                self.identities.remove(victim);
            }
        }
        let at = match self
            .identities
            .binary_search_by_key(&peer, |identity| identity.peer)
        {
            Ok(at) => at,
            Err(at) => {
                let identity = Identity {
                    peer,
                    held: Vec::with_capacity(self.limits.per_identity),
                };
                self.identities.insert(at, identity);
                at
            }
        };
        let identity = self.identities.get_mut(at).ok_or(Refusal::Identities)?;
        identity.held.push(fresh);
        if taken.is_some() {
            bump(&mut self.stats.replaced);
        } else {
            self.stats.connections = self.stats.connections.saturating_add(1);
        }
        bump(&mut self.stats.admitted);
        Ok(taken)
    }
    /// The connection a full endpoint gives `peer`, which holds `held` (`new` when it is not yet
    /// an identity here), while it is under its fair share: an idle one of the identity holding
    /// the most, the one it used longest ago, when that identity holds more than the share. As the
    /// identity's index and the connection's. An identity at its share takes nothing, so two
    /// identities never trade a connection back and forth.
    fn fair_victim(
        &self,
        peer: PeerId,
        held: usize,
        new: bool,
        now: Instant,
    ) -> Option<(usize, usize)> {
        let identities = self.identities.len().saturating_add(usize::from(new));
        let share = self
            .limits
            .connections
            .checked_div(identities.max(1))
            .unwrap_or(0);
        if held >= share {
            return None;
        }
        let (victim, identity) = self
            .identities
            .iter()
            .enumerate()
            .filter(|(_, identity)| identity.peer != peer)
            .max_by_key(|(_, identity)| identity.held.len())?;
        if identity.held.len() <= share {
            return None;
        }
        let at = least_used_idle(&identity.held, now, self.quiet)?;
        Some((victim, at))
    }
    /// The connections charged to `peer`.
    pub(crate) fn connections_of(&self, peer: PeerId) -> impl Iterator<Item = ConnectionKey> + '_ {
        self.identities
            .binary_search_by_key(&peer, |identity| identity.peer)
            .ok()
            .and_then(|at| self.identities.get(at))
            .into_iter()
            .flat_map(|identity| identity.held.iter().map(|held| held.key))
    }
    fn held_mut(&mut self, peer: PeerId, connection: ConnectionKey) -> Option<&mut Held> {
        let at = self
            .identities
            .binary_search_by_key(&peer, |identity| identity.peer)
            .ok()?;
        self.identities
            .get_mut(at)?
            .held
            .iter_mut()
            .find(|held| held.key == connection)
    }
    /// Since when `connection` of `peer` has moved nothing: none of its exchanges under way, and
    /// nothing begun, ended or carried on it since the instant returned. `None` while an exchange
    /// is under way, or for a connection not charged here. It is idle once that is the quiet
    /// period ago, the rule replacement keeps.
    pub(crate) fn quiet_since(&self, peer: PeerId, connection: ConnectionKey) -> Option<Instant> {
        self.identities
            .binary_search_by_key(&peer, |identity| identity.peer)
            .ok()
            .and_then(|at| self.identities.get(at))
            .and_then(|identity| identity.held.iter().find(|held| held.key == connection))
            .filter(|held| held.serving == 0)
            .map(|held| held.moved)
    }
    /// The quiet period after which a connection that moved nothing is idle.
    pub(crate) fn quiet(&self) -> Duration {
        self.quiet
    }
    /// `connection` of `peer` began an exchange at `tick` and `now`: it is the identity's most
    /// recently used, and the exchange keeps it from being replaced until it ends.
    pub(crate) fn begun(
        &mut self,
        peer: PeerId,
        connection: ConnectionKey,
        tick: u64,
        now: Instant,
    ) {
        let Some(held) = self.held_mut(peer, connection) else {
            return;
        };
        held.serving = held.serving.saturating_add(1);
        held.used = tick;
        held.moved = now;
        self.stats.serving = self.stats.serving.saturating_add(1);
    }
    /// An exchange `connection` of `peer` carried ended at `now`.
    pub(crate) fn ended(&mut self, peer: PeerId, connection: ConnectionKey, now: Instant) {
        let Some(held) = self.held_mut(peer, connection) else {
            return;
        };
        let was = held.serving;
        held.serving = held.serving.saturating_sub(1);
        held.moved = now;
        if was > 0 {
            self.stats.serving = self.stats.serving.saturating_sub(1);
        }
    }
    /// `connection` of `peer` carried a frame at `now`: its quiet starts again.
    pub(crate) fn carried(&mut self, peer: PeerId, connection: ConnectionKey, now: Instant) {
        if let Some(held) = self.held_mut(peer, connection) {
            held.moved = now;
        }
    }
    /// `connection` of `peer` ended: its charge is given back.
    pub(crate) fn release(&mut self, peer: PeerId, connection: ConnectionKey) {
        let Ok(at) = self
            .identities
            .binary_search_by_key(&peer, |identity| identity.peer)
        else {
            return;
        };
        let Some(identity) = self.identities.get_mut(at) else {
            return;
        };
        let Some(position) = identity.held.iter().position(|held| held.key == connection) else {
            return;
        };
        let held = identity.held.remove(position);
        self.stats.serving = self.stats.serving.saturating_sub(held.serving);
        self.stats.connections = self.stats.connections.saturating_sub(1);
        if identity.held.is_empty() {
            self.identities.remove(at);
        }
    }
}

/// Of `held`, the idle connection used longest ago.
fn least_used_idle(held: &[Held], now: Instant, quiet: Duration) -> Option<usize> {
    held.iter()
        .enumerate()
        .filter(|(_, held)| held.idle(now, quiet))
        .min_by_key(|(_, held)| held.used)
        .map(|(at, _)| at)
}

#[cfg(test)]
mod tests {
    //! focal `tests.rs` `mod admission`, on the admission alone; the same rules over real
    //! connections are in `tests/exchange.rs`.
    use super::*;

    /// The quiet period of the tests' admissions.
    const QUIET: Duration = Duration::from_secs(2);

    fn bounds() -> AdmissionLimits {
        AdmissionLimits {
            pending: 8,
            identities: 8,
            connections: 16,
            per_identity: 2,
        }
    }

    /// An instant `seconds` after the tests' origin.
    fn at(origin: Instant, seconds: u64) -> Instant {
        origin + Duration::from_secs(seconds)
    }

    #[test]
    fn pending_places_are_bounded_and_given_back() {
        assert_eq!(
            Admission::new(
                AdmissionLimits {
                    pending: 0,
                    ..bounds()
                },
                QUIET
            )
            .err(),
            Some(Refusal::Configuration)
        );
        let mut admission = Admission::new(
            AdmissionLimits {
                pending: 2,
                ..bounds()
            },
            QUIET,
        )
        .unwrap();
        admission.begin().unwrap();
        admission.begin().unwrap();
        assert_eq!(admission.begin(), Err(Refusal::Pending));
        assert_eq!(
            (admission.stats().pending, admission.stats().refused_pending),
            (2, 1)
        );
        admission.end_pending();
        admission.begin().unwrap();
        assert_eq!(admission.begin(), Err(Refusal::Pending));
        admission.end_pending();
        admission.end_pending();
        assert_eq!(
            (admission.stats().pending, admission.stats().connections),
            (0, 0)
        );
    }

    /// The audit's F20: with the endpoint full, an identity at its own bound still reaches its
    /// replacement; only a connection that would be one more is refused.
    #[test]
    fn a_full_endpoint_still_replaces_an_identity_s_own_connection() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(
            AdmissionLimits {
                connections: 3,
                ..bounds()
            },
            QUIET,
        )
        .unwrap();
        assert_eq!(admission.admit(1, 10, 1, origin), Ok(None));
        assert_eq!(admission.admit(1, 11, 2, origin), Ok(None));
        assert_eq!(admission.admit(2, 20, 3, origin), Ok(None));
        // Identity 2 holds no more than its share of the full endpoint: one more is refused.
        assert_eq!(
            admission.admit(2, 21, 4, at(origin, 10)),
            Err(Refusal::Connections)
        );
        assert_eq!(admission.stats().refused_connections, 1);
        // Identity 1 at its bound replaces the idle connection it used least, though the endpoint
        // is full.
        admission.begun(1, 10, 5, at(origin, 10));
        admission.ended(1, 10, at(origin, 10));
        assert_eq!(admission.admit(1, 12, 6, at(origin, 20)), Ok(Some(11)));
        let stats = admission.stats();
        assert_eq!(
            (stats.connections, stats.replaced, stats.admitted),
            (3, 1, 4)
        );
    }

    #[test]
    fn an_identity_past_its_bound_loses_the_idle_connection_it_used_least() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(bounds(), QUIET).unwrap();
        admission.admit(1, 10, 1, origin).unwrap();
        admission.admit(1, 11, 2, origin).unwrap();
        // The first is used after the second was opened: the second is now the one used least.
        admission.begun(1, 10, 3, origin);
        admission.ended(1, 10, origin);
        assert_eq!(admission.admit(1, 12, 4, at(origin, 3)), Ok(Some(11)));
        // Clients that leave without closing, each after the last's quiet period: one after
        // another is always admitted, each replacing what the last left behind.
        for round in 0..40u64 {
            let replaced = admission
                .admit(
                    1,
                    100 + usize::try_from(round).unwrap(),
                    10 + round,
                    at(origin, 6 + 3 * round),
                )
                .unwrap();
            assert!(replaced.is_some(), "round {round}");
        }
        let stats = admission.stats();
        assert_eq!(
            (stats.identities, stats.connections, stats.replaced),
            (1, 2, 41)
        );
    }

    /// focal 2026-10-09: replacing whichever connection was used least closed connections whose
    /// exchanges were under way. A connection carrying an exchange, or quiet for less than the
    /// period, is never replaced; with none idle, the newcomer is refused.
    #[test]
    fn a_connection_carrying_an_exchange_or_used_within_the_quiet_period_is_never_replaced() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(bounds(), QUIET).unwrap();
        admission.admit(1, 10, 1, origin).unwrap();
        admission.admit(1, 11, 2, origin).unwrap();
        // Connection 10 carries an exchange; connection 11 began one a moment ago.
        admission.begun(1, 10, 3, origin);
        admission.begun(1, 11, 4, at(origin, 1));
        admission.ended(1, 11, at(origin, 1));
        assert_eq!(admission.admit(1, 12, 5, at(origin, 2)), Err(Refusal::Busy));
        assert_eq!(admission.stats().refused_busy, 1);
        assert_eq!(admission.stats().serving, 1);
        // Long after, connection 10 still carries its exchange: only 11, quiet now, is replaced.
        assert_eq!(admission.admit(1, 12, 6, at(origin, 60)), Ok(Some(11)));
        // 12 carries frames, 10 its exchange: neither is idle.
        admission.carried(1, 12, at(origin, 61));
        assert_eq!(
            admission.admit(1, 13, 7, at(origin, 62)),
            Err(Refusal::Busy)
        );
        // 10's exchange ends; it goes once quiet for the period, while 12, carrying on, stays.
        admission.ended(1, 10, at(origin, 62));
        assert_eq!(admission.stats().serving, 0);
        admission.carried(1, 12, at(origin, 63));
        assert_eq!(
            admission.admit(1, 13, 8, at(origin, 63)),
            Err(Refusal::Busy)
        );
        admission.carried(1, 12, at(origin, 64));
        assert_eq!(admission.admit(1, 13, 9, at(origin, 64)), Ok(Some(10)));
        // A frame carried keeps a connection from being idle as an exchange does.
        admission.carried(1, 12, at(origin, 65));
        admission.carried(1, 13, at(origin, 65));
        assert_eq!(
            admission.admit(1, 14, 10, at(origin, 66)),
            Err(Refusal::Busy)
        );
        assert_eq!(admission.admit(1, 14, 11, at(origin, 67)), Ok(Some(12)));
        let stats = admission.stats();
        assert_eq!(
            (stats.connections, stats.replaced, stats.refused_busy),
            (2, 3, 4)
        );
    }

    /// Max-min fairness: alone an identity may hold what its bound allows; a full endpoint gives
    /// a newcomer an idle connection of the identity holding the most, never of one at or under
    /// the newcomer's share, and never one carrying an exchange.
    #[test]
    fn a_full_endpoint_gives_a_newcomer_an_idle_connection_of_the_identity_holding_the_most() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(
            AdmissionLimits {
                connections: 4,
                per_identity: 4,
                ..bounds()
            },
            QUIET,
        )
        .unwrap();
        for key in 0..4 {
            admission
                .admit(1, key, u64::try_from(key).unwrap(), origin)
                .unwrap();
        }
        // Every connection of identity 1 is busy or freshly used: identity 2 is refused.
        assert_eq!(admission.admit(2, 20, 5, origin), Err(Refusal::Connections));
        // A quiet period later, identity 2 takes the one identity 1 used longest ago, but one
        // carrying an exchange is never taken.
        admission.begun(1, 0, 6, origin);
        assert_eq!(admission.admit(2, 20, 7, at(origin, 3)), Ok(Some(1)));
        assert_eq!(admission.admit(3, 30, 8, at(origin, 3)), Ok(Some(2)));
        // Four identities on four connections: the share is one; identity 1 holds two and gives
        // its idle one. A fifth identity's share of four connections is none: it takes nothing.
        assert_eq!(admission.admit(4, 40, 9, at(origin, 3)), Ok(Some(3)));
        assert_eq!(
            admission.admit(5, 50, 10, at(origin, 3)),
            Err(Refusal::Connections)
        );
        let stats = admission.stats();
        assert_eq!(
            (stats.identities, stats.connections, stats.replaced),
            (4, 4, 3)
        );
    }

    #[test]
    fn identities_are_bounded_and_one_identity_cannot_take_another_s_place() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(
            AdmissionLimits {
                identities: 2,
                ..bounds()
            },
            QUIET,
        )
        .unwrap();
        for connection in 0..5 {
            admission
                .admit(
                    1,
                    connection,
                    u64::try_from(connection).unwrap(),
                    at(origin, 3 * u64::try_from(connection).unwrap()),
                )
                .unwrap();
        }
        admission.admit(2, 50, 9, at(origin, 20)).unwrap();
        let stats = admission.stats();
        assert_eq!(
            (stats.identities, stats.replaced, stats.connections),
            (2, 3, 3)
        );
        assert_eq!(
            admission.admit(3, 60, 10, at(origin, 20)),
            Err(Refusal::Identities)
        );
        assert_eq!(admission.stats().refused_identities, 1);
        // An identity that leaves frees its place.
        admission.release(2, 50);
        assert_eq!(admission.stats().identities, 1);
        assert_eq!(admission.admit(3, 60, 11, at(origin, 20)), Ok(None));
        assert_eq!(admission.stats().connections, 3);
    }

    #[test]
    fn dials_count_against_the_connection_bound() {
        let mut admission = Admission::new(
            AdmissionLimits {
                connections: 2,
                ..bounds()
            },
            QUIET,
        )
        .unwrap();
        admission.may_dial(1).unwrap();
        assert_eq!(admission.may_dial(2), Err(Refusal::Connections));
        admission
            .admit(1, 1, 1, hyper_sim::Anchor::new().instant(0).unwrap())
            .unwrap();
        assert_eq!(admission.may_dial(1), Err(Refusal::Connections));
    }

    #[test]
    fn a_released_connection_gives_back_its_exchanges_under_way() {
        let origin = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut admission = Admission::new(bounds(), QUIET).unwrap();
        admission.admit(1, 10, 1, origin).unwrap();
        admission.begun(1, 10, 2, origin);
        admission.begun(1, 10, 3, origin);
        assert_eq!(admission.stats().serving, 2);
        admission.release(1, 10);
        let stats = admission.stats();
        assert_eq!(
            (stats.serving, stats.connections, stats.identities),
            (0, 0, 0)
        );
        // An exchange ending on a connection already released counts nothing.
        admission.ended(1, 10, origin);
        assert_eq!(admission.stats().serving, 0);
    }
}
