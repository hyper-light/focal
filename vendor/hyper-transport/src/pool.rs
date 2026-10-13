//! The exchanges pooled connections share ([`crate::StreamCredit::Pooled`], docs/transport.md
//! §4g): admission credit as Breakwater's (Cho et al., "Overload Control for µs-scale RPCs with
//! Breakwater", OSDI 2020), carried by QUIC's bidirectional stream limit (RFC 9000 §4.6).
//!
//! The owner sizes the pool: the exchanges its pooled peers may have open at once, all
//! connections' together, which it sets to what it can take without queueing past its own bound.
//! A connection is granted streams as its peer is blocked at its grant (STREAMS_BLOCKED, RFC 9000
//! §4.6), first come first served. A peer then holds back what the endpoint cannot take, where it
//! sent it to be refused: a refusal costs the endpoint about what an answer does (its packets, its
//! decryption, its reading and its reply), and a request held at the peer costs it nothing.
//!
//! - **Granting.** A connection that joins while the pool holds credit no connection is granted
//!   is raised to its fair share at once (the pool over the connections in it, one stream at
//!   least), so a peer that is not contended for waits no round trip to say it wants more. While
//!   the pool holds such credit, the connection that has waited longest is raised to its fair
//!   share, or by one when it holds that already.
//! - **Giving back.** QUIC takes back no stream it granted: a grant lowered falls as the peer's
//!   streams end. When an exchange its peer opened ends, a connection gives one stream back if the
//!   pool is overspent (the owner shrank it, or connections hold their handshake's grant past it).
//!   While another connection waits, it gives one back if it holds more than a fair share (max-min
//!   fairness among the connections that want streams, Bertsekas and Gallager, *Data Networks*,
//!   §6.5.2), and all it holds if no exchange of its peer's is left open: credit goes with demand,
//!   as Breakwater issues credit only to clients with requests to send, and an idle peer that
//!   wants a stream again says so (STREAMS_BLOCKED) and waits its turn.
//! - **Taking back from the idle.** QUIC takes back no stream it announced, so a connection that
//!   holds credit and opens nothing would keep it from every other forever: it gives back only as
//!   its exchanges end. A connection with no exchange of its peer's open is queued as idle, once
//!   (as it joins, and as its last exchange ends with credit kept). While another waits and nothing
//!   is spare, the endpoint takes back what an idle connection holds, in the pool's account: its
//!   peer may still open the streams it was told of, so the pool is overcommitted by them, as
//!   Breakwater overcommits its credit to idle clients and keeps its queue by dropping what an
//!   overload sends past it (the owner's door).
//! - **Bounds.** A connection waits once and is queued as idle once at most, so each queue holds
//!   no more than the pooled connections; its entries name a connection by its key and serial,
//!   and one gone, or one busy again, is passed when reached.

use std::collections::VecDeque;

/// A pooled connection's part of the pool, kept with the connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Share {
    /// The connection is in the pool: its peer's role is pooled and it is up.
    pub(crate) pooled: bool,
    /// The streams its peer may hold open at once: what the pool granted it.
    pub(crate) granted: u32,
    /// It waits in the pool's queue for more.
    pub(crate) wanting: bool,
    /// It is in the pool's queue of idle connections.
    pub(crate) idle: bool,
}

/// The credit pooled connections share.
#[derive(Debug)]
pub(crate) struct Pool {
    /// What the owner sized it to.
    size: u64,
    /// The pooled connections' grants, summed.
    granted: u64,
    /// The pooled connections.
    connections: u64,
    /// The connections whose peers are blocked at their grants, longest waiting first: each by its
    /// key and serial, once. One that left keeps its place until it is reached and passed.
    wanting: VecDeque<(usize, u64)>,
    /// The connections waiting that have not left: the queue less those that left.
    waiting: u64,
    /// Connections that held credit with no exchange of their peer's open, by key and serial,
    /// once each: what the pool takes back for those waiting when nothing is spare.
    idle: VecDeque<(usize, u64)>,
    /// Streams taken back from idle connections, counted.
    reclaimed: u64,
    /// Streams granted to waiting connections, counted.
    grants: u64,
    /// Streams connections gave back, counted.
    given_back: u64,
}

/// What the pool holds and has done, for the owner's statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// What the owner sized it to.
    pub size: u64,
    /// The pooled connections' grants, summed: past `size` while connections hold more than it
    /// (each holds its handshake's grant, and grants above a shrunken size fall as streams end).
    pub granted: u64,
    /// The pooled connections.
    pub connections: u64,
    /// Connections waiting for streams.
    pub wanting: u64,
    /// Streams granted to waiting connections, counted.
    pub grants: u64,
    /// Streams connections gave back, counted.
    pub given_back: u64,
    /// Streams taken back from idle connections for those waiting, counted: the pool's
    /// overcommitment, as their peers may still open the streams they were told of.
    pub reclaimed: u64,
}

impl Pool {
    /// A pool of `size` streams, none granted.
    pub(crate) fn new(size: u64) -> Self {
        Self {
            size,
            granted: 0,
            connections: 0,
            wanting: VecDeque::new(),
            waiting: 0,
            idle: VecDeque::new(),
            reclaimed: 0,
            grants: 0,
            given_back: 0,
        }
    }

    /// The owner sizes the pool anew: credit it gained goes to the connections waiting, and credit
    /// it lost comes back as their exchanges end.
    pub(crate) fn resize(&mut self, size: u64) {
        self.size = size;
    }

    /// What the pool holds and has done.
    pub(crate) fn stats(&self) -> PoolStats {
        PoolStats {
            size: self.size,
            granted: self.granted,
            connections: self.connections,
            wanting: self.waiting,
            grants: self.grants,
            given_back: self.given_back,
            reclaimed: self.reclaimed,
        }
    }

    /// Credit no connection is granted.
    fn spare(&self) -> u64 {
        self.size.saturating_sub(self.granted)
    }

    /// A connection's fair share: the pool over the pooled connections, rounded up, one stream at
    /// least.
    fn fair(&self) -> u64 {
        self.size.div_ceil(self.connections.max(1)).max(1)
    }

    /// A connection joins the pool holding `granted`, its handshake's grant, idle until its peer
    /// opens an exchange: `key` and `serial` name it.
    pub(crate) fn join(&mut self, share: &mut Share, granted: u32, key: usize, serial: u64) {
        if share.pooled {
            return;
        }
        *share = Share {
            pooled: true,
            granted,
            wanting: false,
            idle: false,
        };
        self.granted = self.granted.saturating_add(u64::from(granted));
        self.connections = self.connections.saturating_add(1);
        self.went_idle(share, key, serial);
    }

    /// The connection holds credit with no exchange of its peer's open: it is queued as idle,
    /// once.
    fn went_idle(&mut self, share: &mut Share, key: usize, serial: u64) {
        if share.granted > 0 && !share.idle {
            share.idle = true;
            self.idle.push_back((key, serial));
        }
    }

    /// The idle connection queued longest, by key and serial, while another waits and nothing is
    /// spare: taken from the queue. The caller takes back what it holds ([`Pool::reclaim`]) if it
    /// is still pooled and still has no exchange of its peer's open.
    pub(crate) fn next_idle(&mut self) -> Option<(usize, u64)> {
        if self.spare() > 0 || self.waiting == 0 {
            return None;
        }
        self.idle.pop_front()
    }

    /// Takes back what the idle connection holds, in the pool's account, `open` of its peer's
    /// exchanges open: its new grant, none, if it is idle still. One busy again keeps its grant.
    pub(crate) fn reclaim(&mut self, share: &mut Share, open: u32) -> Option<u32> {
        share.idle = false;
        // One busy again keeps its grant, and one that waits for more is granted, not taken from.
        if !share.pooled || open > 0 || share.granted == 0 || share.wanting {
            return None;
        }
        let back = u64::from(share.granted);
        share.granted = 0;
        self.granted = self.granted.saturating_sub(back);
        self.reclaimed = self.reclaimed.saturating_add(back);
        Some(0)
    }

    /// A connection just joined is granted its fair share at once from what is spare, within
    /// `most`: its new grant, if it gained any. Its peer then waits no round trip to say it wants
    /// more; under contention it holds its handshake's grant and asks as any does.
    pub(crate) fn welcome(&mut self, share: &mut Share, most: u32) -> Option<u32> {
        if !share.pooled {
            return None;
        }
        let held = u64::from(share.granted);
        let raise = self
            .fair()
            .saturating_sub(held)
            .min(self.spare())
            .min(u64::from(most).saturating_sub(held));
        if raise == 0 {
            return None;
        }
        share.granted = share
            .granted
            .saturating_add(u32::try_from(raise).unwrap_or(u32::MAX));
        self.granted = self.granted.saturating_add(raise);
        self.grants = self.grants.saturating_add(raise);
        Some(share.granted)
    }

    /// A connection leaves the pool: what it held is the pool's again. Its place in the queue, if
    /// it had one, is skipped when reached.
    pub(crate) fn leave(&mut self, share: &mut Share) {
        if !share.pooled {
            return;
        }
        self.granted = self.granted.saturating_sub(u64::from(share.granted));
        self.connections = self.connections.saturating_sub(1);
        if share.wanting {
            self.waiting = self.waiting.saturating_sub(1);
        }
        *share = Share::default();
    }

    /// The connection's peer is blocked at its grant: it waits its turn, once.
    pub(crate) fn wants(&mut self, share: &mut Share, key: usize, serial: u64) {
        if !share.pooled || share.wanting {
            return;
        }
        share.wanting = true;
        self.wanting.push_back((key, serial));
        self.waiting = self.waiting.saturating_add(1);
    }

    /// The connection that has waited longest, by key and serial, while the pool has credit to
    /// grant: taken from the queue. The caller grants it ([`Pool::grant`]) if it is still pooled
    /// and its peer still blocked, and otherwise lets it go ([`Pool::passed`]).
    pub(crate) fn next(&mut self) -> Option<(usize, u64)> {
        if self.spare() == 0 {
            return None;
        }
        self.wanting.pop_front()
    }

    /// The connection taken from the queue no longer waits: its peer is not blocked now.
    pub(crate) fn passed(&mut self, share: &mut Share) {
        if std::mem::replace(&mut share.wanting, false) {
            self.waiting = self.waiting.saturating_sub(1);
        }
    }

    /// Grants the waiting connection its fair share, or one stream more if it holds that already,
    /// from what is spare and within `most`, the streams one connection may hold: its new grant.
    pub(crate) fn grant(&mut self, share: &mut Share, most: u32) -> u32 {
        self.passed(share);
        if !share.pooled {
            return share.granted;
        }
        let held = u64::from(share.granted);
        let raise = self
            .fair()
            .saturating_sub(held)
            .max(1)
            .min(self.spare())
            .min(u64::from(most).saturating_sub(held));
        let raise32 = u32::try_from(raise).unwrap_or(u32::MAX);
        share.granted = share.granted.saturating_add(raise32);
        self.granted = self.granted.saturating_add(raise);
        self.grants = self.grants.saturating_add(raise);
        share.granted
    }

    /// An exchange the connection's peer opened has ended, `open` of its peer's still open, the
    /// connection named by `key` and `serial`: its new grant if it gives streams back. It gives one
    /// back when the pool is overspent; while another connection waits, one when it holds more
    /// than a fair share, and all it holds when none of its peer's is open. Its own wait is no
    /// other's: a lone connection that wants more keeps what it holds. One left holding credit with
    /// none open is queued as idle.
    pub(crate) fn ended(
        &mut self,
        share: &mut Share,
        open: u32,
        key: usize,
        serial: u64,
    ) -> Option<u32> {
        if !share.pooled || share.granted == 0 {
            return None;
        }
        let waited = self.waiting > u64::from(share.wanting);
        let keep = if waited && open == 0 {
            0
        } else if self.granted > self.size || (waited && u64::from(share.granted) > self.fair()) {
            share.granted.saturating_sub(1)
        } else {
            share.granted
        };
        let back = share.granted.saturating_sub(keep);
        share.granted = keep;
        self.granted = self.granted.saturating_sub(u64::from(back));
        self.given_back = self.given_back.saturating_add(u64::from(back));
        if open == 0 {
            self.went_idle(share, key, serial);
        }
        (back > 0).then_some(keep)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// A pool of `size` with `count` connections joined holding `initial` each.
    fn pool(size: u64, count: usize, initial: u32) -> (Pool, Vec<Share>) {
        let mut pool = Pool::new(size);
        let mut shares = vec![Share::default(); count];
        for (key, share) in shares.iter_mut().enumerate() {
            pool.join(share, initial, key, 0);
        }
        (pool, shares)
    }

    /// Grants every waiting connection it can, as the endpoint does each turn.
    fn grant_all(pool: &mut Pool, shares: &mut [Share], most: u32) {
        while let Some((key, _)) = pool.next() {
            pool.grant(&mut shares[key], most);
        }
    }

    /// A lone connection that wants more is raised to the whole pool at once, never past what one
    /// connection may hold.
    #[test]
    fn a_lone_waiting_connection_is_raised_to_the_pool() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 8);
        assert_eq!(pool.stats().granted, 8);
        let (mut pool, mut shares) = self::pool(8, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 4);
        assert_eq!(shares[0].granted, 4, "within what one connection may hold");
    }

    /// Two connections that both want streams converge to equal shares: the one granted first
    /// gives back what it holds past its fair share as its exchanges end, and the other is granted
    /// it. The pool is never overspent by a grant.
    #[test]
    fn waiting_connections_converge_to_fair_shares() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 8);
        // A second connection comes, holding its handshake's grant, and wants more.
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        pool.wants(&mut shares[1], 1, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(
            shares[1].granted, 1,
            "nothing is spare: the pool is overspent by one"
        );
        // The first one's exchanges end, one at a time; each turn grants what came back.
        for _ in 0..16 {
            // Busy: every stream it holds but the one that ended carries an exchange.
            let open = shares[0].granted.saturating_sub(1);
            let _ = pool.ended(&mut shares[0], open, 0, 0);
            if !shares[1].wanting && shares[1].granted < 4 {
                pool.wants(&mut shares[1], 1, 0);
            }
            grant_all(&mut pool, &mut shares, 16);
            assert!(
                pool.stats().granted <= 9,
                "only the handshake's grant past the pool"
            );
        }
        assert_eq!((shares[0].granted, shares[1].granted), (4, 4));
        assert_eq!(pool.stats().granted, 8);
    }

    /// A pool the owner shrinks is overspent until the connections' exchanges end, and then holds
    /// its new size; each gives back one stream an exchange.
    #[test]
    fn a_shrunken_pool_is_given_back_as_exchanges_end() {
        let (mut pool, mut shares) = pool(8, 2, 1);
        for (key, share) in shares.iter_mut().enumerate() {
            pool.wants(share, key, 0);
        }
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!((shares[0].granted, shares[1].granted), (4, 4));
        pool.resize(2);
        for _ in 0..3 {
            for share in shares.iter_mut() {
                let open = share.granted.saturating_sub(1);
                assert!(pool.ended(share, open, 0, 0).is_some());
            }
        }
        assert_eq!((shares[0].granted, shares[1].granted), (1, 1));
        assert_eq!(
            pool.ended(&mut shares[0], 0, 0, 0),
            None,
            "the pool holds its size"
        );
        assert_eq!(pool.stats().granted, 2);
    }

    /// More connections than the pool holds: each holds its handshake's grant, given back as the
    /// pool is overspent; a connection whose peer has no exchange open gives what it holds to one
    /// that waits, at once, however small its share, so the streams open at once come back to the
    /// pool's size and go where they are wanted.
    #[test]
    fn connections_past_the_pool_give_their_grants_back_to_those_waiting() {
        let (mut pool, mut shares) = pool(2, 4, 1);
        assert_eq!(pool.stats().granted, 4, "each holds its handshake's grant");
        for key in [0, 1] {
            assert_eq!(pool.ended(&mut shares[key], 0, key, 0), Some(0));
        }
        assert_eq!(pool.stats().granted, 2);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 0, "nothing is spare");
        // Connection 2 still has an exchange open: it keeps its one stream, its fair share.
        assert_eq!(pool.ended(&mut shares[2], 1, 2, 0), None);
        // Its last one ends: idle, it gives its stream to the one waiting.
        assert_eq!(pool.ended(&mut shares[2], 0, 2, 0), Some(0));
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 1);
        assert_eq!(pool.stats().granted, 2);
    }

    /// A connection whose peer alone wants more keeps what it holds as its exchanges end: its own
    /// wait is no other's, and giving its streams back to grant them to itself again would leave
    /// it out of them until QUIC freed the streams it held.
    #[test]
    fn a_lone_waiting_connection_keeps_its_grant() {
        let (mut pool, mut shares) = pool(3, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 3);
        pool.wants(&mut shares[0], 0, 0);
        assert_eq!(pool.ended(&mut shares[0], 0, 0, 0), None);
        assert_eq!(shares[0].granted, 3);
    }

    /// A busy connection holding its fair share keeps it while another waits, so no connection's
    /// open exchanges are cut under its fair share for another's want.
    #[test]
    fn a_busy_connection_at_its_fair_share_keeps_it() {
        let (mut pool, mut shares) = pool(4, 2, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 2, "its fair share, the pool over two");
        pool.wants(&mut shares[1], 1, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[1].granted, 2);
        pool.wants(&mut shares[1], 1, 0);
        assert_eq!(pool.ended(&mut shares[0], 1, 0, 0), None);
        assert_eq!(shares[0].granted, 2);
    }

    /// A connection that joins an uncontended pool is granted its fair share at once; one that
    /// joins a pool with nothing spare keeps its handshake's grant.
    #[test]
    fn a_connection_joining_is_welcomed_with_what_is_spare() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        assert_eq!(pool.welcome(&mut shares[0], 16), Some(8));
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        assert_eq!(pool.welcome(&mut shares[1], 16), None, "nothing spare");
        assert_eq!(shares[1].granted, 1);
        assert_eq!(pool.stats().granted, 9);
    }

    /// Grants every waiting connection it can, taking back what idle connections hold for them
    /// when nothing is spare, as the endpoint does each turn: `open` is each connection's
    /// exchanges open.
    fn turn(pool: &mut Pool, shares: &mut [Share], most: u32, open: &[u32]) {
        loop {
            if let Some((key, _)) = pool.next() {
                pool.grant(&mut shares[key], most);
                continue;
            }
            let Some((key, _)) = pool.next_idle() else {
                break;
            };
            let _ = pool.reclaim(&mut shares[key], open[key]);
        }
    }

    /// An idle connection's credit is taken back for one that waits when nothing is spare, though
    /// it opened nothing to end: its peer may still open what it was told of (the overcommitment
    /// counted), and the waiting one is granted its fair share. One waiting is never taken from,
    /// and one busy again keeps its grant.
    #[test]
    fn an_idle_connections_credit_is_taken_back_for_one_waiting() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        assert_eq!(pool.welcome(&mut shares[0], 16), Some(8));
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        pool.wants(&mut shares[1], 1, 0);
        turn(&mut pool, &mut shares, 16, &[0, 0]);
        assert_eq!((shares[0].granted, shares[1].granted), (0, 4));
        assert_eq!(pool.stats().reclaimed, 8);
        // A third joins and waits; the second, busy now, keeps its grant though it was queued idle.
        shares.push(Share::default());
        pool.join(&mut shares[2], 1, 2, 0);
        pool.resize(5);
        pool.wants(&mut shares[2], 2, 0);
        turn(&mut pool, &mut shares, 16, &[0, 3, 0]);
        assert_eq!(shares[1].granted, 4, "busy again: kept");
        assert_eq!(
            shares[2].granted, 1,
            "nothing spare and none idle to take from"
        );
    }

    /// A connection waits once however often its peer says it is blocked, and one that left is
    /// skipped when its turn comes; leaving gives its grant back.
    #[test]
    fn a_connection_waits_once_and_one_that_left_is_passed() {
        let (mut pool, mut shares) = pool(4, 2, 1);
        pool.wants(&mut shares[0], 0, 7);
        pool.wants(&mut shares[0], 0, 7);
        pool.wants(&mut shares[1], 1, 9);
        assert_eq!(pool.stats().wanting, 2);
        pool.leave(&mut shares[0]);
        assert_eq!(pool.stats().granted, 1);
        assert_eq!(pool.stats().wanting, 1, "the one that left waits no longer");
        assert_eq!(pool.next(), Some((0, 7)));
        pool.passed(&mut shares[0]);
        assert_eq!(pool.next(), Some((1, 9)));
        assert_eq!(pool.grant(&mut shares[1], 16), 4);
        assert_eq!(pool.next(), None);
    }
}
