//! The exchanges pooled connections share ([`crate::StreamCredit::Pooled`], docs/transport.md
//! §4g): admission credit as Breakwater's (Cho et al., "Overload Control for µs-scale RPCs with
//! Breakwater", OSDI 2020), carried by QUIC's bidirectional stream limit (RFC 9000 §4.6).
//!
//! The owner sizes the pool: the exchanges its pooled peers may have open at once past the one
//! each keeps ([`KEPT`]), all connections' together, which it sets to what it can take without
//! queueing past its own bound.
//! A connection is granted streams as its peer is blocked at its grant (STREAMS_BLOCKED, RFC 9000
//! §4.6), first come first served. A peer then holds back what the endpoint cannot take, where it
//! sent it to be refused: a refusal costs the endpoint about what an answer does (its packets, its
//! decryption, its reading and its reply), and a request held at the peer costs it nothing.
//!
//! - **Granting.** A connection that joins while the pool holds credit no connection is granted
//!   is raised to its fair share at once (the stream it keeps and the pool over the connections in
//!   it), so a peer that is not contended for waits no round trip to say it wants more. While
//!   the pool holds such credit, the connection that has waited longest is raised to its fair
//!   share, or by one when it holds that already.
//! - **Giving back.** QUIC takes back no stream it granted: a grant lowered falls as the peer's
//!   streams end. When an exchange its peer opened ends, a connection gives one stream back if the
//!   pool is overspent (the owner shrank it, or connections hold their handshake's grant past it).
//!   While another connection waits, it gives one back if it holds more than a fair share (max-min
//!   fairness among the connections that want streams, Bertsekas and Gallager, *Data Networks*,
//!   §6.5.2), and all it holds but [`KEPT`] if no exchange of its peer's is left open.
//! - **What a connection keeps.** A connection gives back neither as an exchange ends nor as it is
//!   taken back from idle its last [`KEPT`] stream: Breakwater's demand speculation (§3.2.2), where
//!   each client may hold its latest demand and at least one credit more, and a credit goes to a
//!   client in the response the server is about to send it. A peer that sends a request at a time
//!   then sends its next with the stream its last reply gave back, and is never made to say it is
//!   blocked (STREAMS_BLOCKED) and wait for a grant (MAX_STREAMS) between its requests: two
//!   packets more each way a request, which Breakwater's demand synchronization (clients telling
//!   each change of demand, the server answering with explicit credit) was measured to cost, its
//!   packets received and sent doubled at and below capacity (§5, Figure 8). The kept streams are
//!   outside the pool's account: its size is the credit granted past them, so the pool is
//!   overcommitted by one stream a connection, as Breakwater's is (Coc at least one), and the
//!   owner's door holds what an overload sends past it, as Breakwater's AQM does. Counted inside
//!   it, more connections than the pool's size left nothing spare ever, and a peer with two
//!   exchanges to ask at once waited for its first to end (focal's work mode, 502 connections past
//!   a pool of 142: STREAMS_BLOCKED received rose from 0.30 a write to 1.33 at 50 claims a second).
//! - **Taking back from the idle.** QUIC takes back no stream it announced, so a connection that
//!   holds credit past what it keeps and opens nothing would keep it from every other forever: it
//!   gives back only as its exchanges end. A connection with no exchange of its peer's open and
//!   more than it keeps is queued as idle, once (as it joins, and as its last exchange ends with
//!   credit kept). While another waits and nothing is spare, the endpoint takes back what an idle
//!   connection holds past what it keeps, in the pool's account: its peer may still open the
//!   streams it was told of, so the pool is overcommitted by them too, as Breakwater overcommits
//!   its credit to idle clients and keeps its queue by dropping what an overload sends past it (the
//!   owner's door).
//! - **Parking.** An exchange that waits on an event its owner awaits, not on the endpoint's
//!   service (a long poll: [`crate::Classes::waits`]), holds no credit while it waits: it is parked
//!   as its head arrives, and its connection is granted a stream of its own for it, so its peer
//!   may still open what the pool grants it. Kafka holds a fetch that waits for data in its
//!   purgatory, apart from the request handlers that serve (`DelayedFetch`); Breakwater issues
//!   credit for the server's work, which a waiting read does none of. A parked exchange is not
//!   one of its peer's open ones in the pool's accounts, so a connection that holds only parked
//!   exchanges is idle, its credit reclaimed for others. Parked at most up to the streams one
//!   connection may hold, its grant and its parked together: past that an exchange is served in
//!   the pool's account like any other. Its stream goes as it ends, granted to no other.
//! - **Bounds.** A connection waits once and is queued as idle once at most, so each queue holds
//!   no more than the pooled connections; its entries name a connection by its key and serial,
//!   and one gone, or one busy again, is passed when reached. The streams the pooled connections
//!   hold at once are at most the pool and [`KEPT`] each, as their handshakes' grants and shrinks
//!   of the pool fall back to it.

use std::collections::VecDeque;

/// The streams a pooled connection keeps however its pool is contended: one, the least credit
/// Breakwater's demand speculation leaves a client (Cho et al., OSDI 2020, §3.2.2:
/// `Coc = max((Ctotal − Cissued) / nc, 1)` unused credits past its latest demand), so a peer that
/// sends a request at a time sends each with the stream its last reply gave back. None of a pool
/// sized to nothing ([`Pool::kept`]): its owner takes nothing until it sizes the pool again.
pub(crate) const KEPT: u32 = 1;

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
    /// Exchanges of its peer's parked: each holds a stream of its own, apart from the grant.
    pub(crate) parked: u32,
}

impl Share {
    /// The streams its peer may hold open at once: its grant and its parked exchanges' own.
    pub(crate) fn limit(&self) -> u32 {
        self.granted.saturating_add(self.parked)
    }
}

/// The credit pooled connections share.
#[derive(Debug)]
pub(crate) struct Pool {
    /// What the owner sized it to.
    size: u64,
    /// The pooled connections' grants, summed.
    granted: u64,
    /// The pooled connections holding at least the stream each keeps ([`KEPT`]): what the
    /// connections hold past the pool's size, one stream each, while it is sized.
    holding: u64,
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
    /// Exchanges parked now, all connections' together.
    parked: u64,
    /// Exchanges parked, counted; and those not parked for want of room in their connection.
    parks: u64,
    parks_refused: u64,
}

/// What the pool holds and has done, for the owner's statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// What the owner sized it to.
    pub size: u64,
    /// The pooled connections' grants, summed: past `size` by the stream each connection keeps
    /// ([`KEPT`]), and past that while connections hold more than it (grants above a shrunken size
    /// fall as streams end).
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
    /// Exchanges parked now, holding no credit of the pool's ([`crate::Classes::waits`]).
    pub parked: u64,
    /// Exchanges parked, counted.
    pub parks: u64,
    /// Exchanges that would have parked, their connection holding as many streams as one may:
    /// served in the pool's account instead.
    pub parks_refused: u64,
}

impl Pool {
    /// A pool of `size` streams, none granted.
    pub(crate) fn new(size: u64) -> Self {
        Self {
            size,
            granted: 0,
            holding: 0,
            connections: 0,
            wanting: VecDeque::new(),
            waiting: 0,
            idle: VecDeque::new(),
            reclaimed: 0,
            grants: 0,
            given_back: 0,
            parked: 0,
            parks: 0,
            parks_refused: 0,
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
            parked: self.parked,
            parks: self.parks,
            parks_refused: self.parks_refused,
        }
    }

    /// The streams a connection keeps of this pool ([`KEPT`]): none of one sized to nothing.
    fn kept(&self) -> u32 {
        if self.size == 0 { 0 } else { KEPT }
    }

    /// The streams the connections keep past the pool's size: one each holding one, while it is
    /// sized.
    fn floors(&self) -> u64 {
        self.holding.saturating_mul(u64::from(self.kept()))
    }

    /// Credit no connection is granted: the pool's size past the streams its connections keep.
    fn spare(&self) -> u64 {
        self.size
            .saturating_add(self.floors())
            .saturating_sub(self.granted)
    }

    /// Whether the connections hold more past the streams they keep than the pool's size: the owner
    /// shrank it, or handshake grants exceed it.
    fn overspent(&self) -> bool {
        self.granted > self.size.saturating_add(self.floors())
    }

    /// A connection's fair share: the stream it keeps and the pool over the pooled connections,
    /// rounded up, one stream at least.
    fn fair(&self) -> u64 {
        u64::from(self.kept())
            .saturating_add(self.size.div_ceil(self.connections.max(1)))
            .max(1)
    }

    /// `share`'s grant set to `granted`, the pool's sums with it.
    fn regrant(&mut self, share: &mut Share, granted: u32) {
        let before = share.granted;
        self.granted = self
            .granted
            .saturating_sub(u64::from(before))
            .saturating_add(u64::from(granted));
        match (before >= KEPT, granted >= KEPT) {
            (false, true) => self.holding = self.holding.saturating_add(1),
            (true, false) => self.holding = self.holding.saturating_sub(1),
            _ => {}
        }
        share.granted = granted;
    }

    /// A connection joins the pool holding `granted`, its handshake's grant, idle until its peer
    /// opens an exchange: `key` and `serial` name it.
    pub(crate) fn join(&mut self, share: &mut Share, granted: u32, key: usize, serial: u64) {
        if share.pooled {
            return;
        }
        *share = Share {
            pooled: true,
            granted: 0,
            wanting: false,
            idle: false,
            parked: 0,
        };
        self.regrant(share, granted);
        self.connections = self.connections.saturating_add(1);
        self.went_idle(share, key, serial);
    }

    /// The connection holds credit past what it keeps with no exchange of its peer's open: it is
    /// queued as idle, once.
    fn went_idle(&mut self, share: &mut Share, key: usize, serial: u64) {
        if share.granted > self.kept() && !share.idle {
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

    /// Takes back what the idle connection holds past what it keeps ([`Pool::kept`]), in the pool's
    /// account, `open` of its peer's exchanges open: its new grant, if it is idle still and held
    /// more. One busy again keeps its grant.
    pub(crate) fn reclaim(&mut self, share: &mut Share, open: u32) -> Option<u32> {
        share.idle = false;
        // One busy again keeps its grant, and one that waits for more is granted, not taken from.
        let kept = self.kept();
        if !share.pooled || open > 0 || share.granted <= kept || share.wanting {
            return None;
        }
        let back = u64::from(share.granted.saturating_sub(kept));
        self.regrant(share, kept);
        self.reclaimed = self.reclaimed.saturating_add(back);
        Some(kept)
    }

    /// A connection just joined, named by `key` and `serial`, is granted its fair share at once
    /// from what is spare, within `most`: its new grant, if it gained any. Its peer then waits no
    /// round trip to say it wants more; under contention it holds its handshake's grant and asks as
    /// any does. Raised past what it keeps with nothing of its peer's open yet, it is idle.
    pub(crate) fn welcome(
        &mut self,
        share: &mut Share,
        most: u32,
        key: usize,
        serial: u64,
    ) -> Option<u32> {
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
        let raised = share
            .granted
            .saturating_add(u32::try_from(raise).unwrap_or(u32::MAX));
        self.regrant(share, raised);
        self.grants = self.grants.saturating_add(raise);
        self.went_idle(share, key, serial);
        Some(share.granted)
    }

    /// A connection leaves the pool: what it held is the pool's again. Its place in the queue, if
    /// it had one, is skipped when reached.
    pub(crate) fn leave(&mut self, share: &mut Share) {
        if !share.pooled {
            return;
        }
        self.regrant(share, 0);
        self.parked = self.parked.saturating_sub(u64::from(share.parked));
        self.connections = self.connections.saturating_sub(1);
        if share.wanting {
            self.waiting = self.waiting.saturating_sub(1);
        }
        *share = Share::default();
    }

    /// An exchange its peer opened waits on its owner's event: parked, it holds a stream of its own
    /// and none of the pool's, if the connection holds fewer than `most` streams, its grant and its
    /// parked together. `open` of its peer's exchanges are open past it, the connection named by
    /// `key` and `serial`: one left with none open in the pool's account is queued as idle. The
    /// connection's new stream limit, if parked.
    pub(crate) fn park(
        &mut self,
        share: &mut Share,
        most: u32,
        open: u32,
        key: usize,
        serial: u64,
    ) -> Option<u32> {
        if !share.pooled {
            return None;
        }
        if share.limit() >= most {
            self.parks_refused = self.parks_refused.saturating_add(1);
            return None;
        }
        share.parked = share.parked.saturating_add(1);
        self.parked = self.parked.saturating_add(1);
        self.parks = self.parks.saturating_add(1);
        if open == 0 {
            self.went_idle(share, key, serial);
        }
        Some(share.limit())
    }

    /// A parked exchange ended: its stream goes with it, granted to no other. The connection's new
    /// stream limit.
    pub(crate) fn unpark(&mut self, share: &mut Share) -> Option<u32> {
        if !share.pooled || share.parked == 0 {
            return None;
        }
        share.parked = share.parked.saturating_sub(1);
        self.parked = self.parked.saturating_sub(1);
        Some(share.limit())
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
        let raised = share.granted.saturating_add(raise32);
        self.regrant(share, raised);
        self.grants = self.grants.saturating_add(raise);
        share.granted
    }

    /// An exchange the connection's peer opened has ended, `open` of its peer's still open, the
    /// connection named by `key` and `serial`: its new grant if it gives streams back. It gives one
    /// back when the pool is overspent; while another connection waits, one when it holds more
    /// than a fair share, and all it holds when none of its peer's is open; never what it keeps
    /// ([`Pool::kept`]), which goes back to its peer with the reply. Its own wait is no other's: a lone
    /// connection that wants more keeps what it holds. One left holding credit past what it keeps
    /// with none open is queued as idle.
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
        let kept = share.granted.min(self.kept());
        let keep = if waited && open == 0 {
            kept
        } else if self.overspent() || (waited && u64::from(share.granted) > self.fair()) {
            share.granted.saturating_sub(1).max(kept)
        } else {
            share.granted
        };
        let back = share.granted.saturating_sub(keep);
        self.regrant(share, keep);
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

    /// A lone connection that wants more is raised to the whole pool at once, past the stream it
    /// keeps, never past what one connection may hold.
    #[test]
    fn a_lone_waiting_connection_is_raised_to_the_pool() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 9, "the pool and the stream it keeps");
        assert_eq!(pool.stats().granted, 9);
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
        assert_eq!(shares[0].granted, 9);
        // A second connection comes, holding its handshake's grant, and wants more.
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        pool.wants(&mut shares[1], 1, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(
            shares[1].granted, 1,
            "nothing is spare: the pool is granted past the streams kept"
        );
        // The first one's exchanges end, one at a time; each turn grants what came back.
        for _ in 0..16 {
            // Busy: every stream it holds but the one that ended carries an exchange.
            let open = shares[0].granted.saturating_sub(1);
            let _ = pool.ended(&mut shares[0], open, 0, 0);
            if !shares[1].wanting && shares[1].granted < 5 {
                pool.wants(&mut shares[1], 1, 0);
            }
            grant_all(&mut pool, &mut shares, 16);
            assert!(
                pool.stats().granted <= 10,
                "only the streams kept past the pool"
            );
        }
        assert_eq!((shares[0].granted, shares[1].granted), (5, 5));
        assert_eq!(pool.stats().granted, 10);
    }

    /// A pool the owner shrinks is overspent until the connections' exchanges end, and then holds
    /// its new size past the streams kept; each gives back one stream an exchange.
    #[test]
    fn a_shrunken_pool_is_given_back_as_exchanges_end() {
        let (mut pool, mut shares) = pool(8, 2, 1);
        for (key, share) in shares.iter_mut().enumerate() {
            pool.wants(share, key, 0);
        }
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!((shares[0].granted, shares[1].granted), (5, 5));
        pool.resize(2);
        for _ in 0..3 {
            for share in shares.iter_mut() {
                let open = share.granted.saturating_sub(1);
                assert!(pool.ended(share, open, 0, 0).is_some());
            }
        }
        assert_eq!((shares[0].granted, shares[1].granted), (2, 2));
        assert_eq!(
            pool.ended(&mut shares[0], 0, 0, 0),
            None,
            "the pool holds its size past the streams kept"
        );
        assert_eq!(pool.stats().granted, 4);
    }

    /// More connections than the pool holds streams: each keeps the stream it holds, outside the
    /// pool's account (`KEPT`, the least credit Breakwater leaves a client), so its peer's next
    /// request goes with the stream its last reply gave back and never waits for a grant. The
    /// pool's streams go past them by demand: two of four connections that want more are granted
    /// one each, the others wait, and one whose last exchange ends while another waits gives back
    /// what it holds past its stream, to be granted to that one.
    #[test]
    fn connections_past_the_pool_keep_one_stream_each() {
        let (mut pool, mut shares) = pool(2, 4, 1);
        assert_eq!(pool.stats().granted, 4, "each holds its handshake's grant");
        for (key, share) in shares.iter_mut().enumerate() {
            pool.wants(share, key, 0);
        }
        grant_all(&mut pool, &mut shares, 16);
        let granted: Vec<u32> = shares.iter().map(|share| share.granted).collect();
        assert_eq!(
            granted,
            [2, 2, 1, 1],
            "the pool's two past the streams kept"
        );
        // The others still wait: one granted ends its last exchange, gives back its second
        // stream and keeps its first, and a waiting one is granted it.
        assert_eq!(pool.ended(&mut shares[0], 0, 0, 0), Some(1));
        grant_all(&mut pool, &mut shares, 16);
        let granted: Vec<u32> = shares.iter().map(|share| share.granted).collect();
        assert_eq!(granted, [1, 2, 2, 1]);
        // One holding only its stream keeps it however many wait.
        assert_eq!(pool.ended(&mut shares[3], 0, 3, 0), None);
        assert_eq!(shares[3].granted, 1);
        assert_eq!(pool.stats().granted, 6);
    }

    /// A pool sized to nothing keeps nothing for its connections: what they hold goes back as
    /// their exchanges end, and its owner takes nothing until it sizes the pool again.
    #[test]
    fn a_pool_sized_to_nothing_keeps_nothing() {
        let (mut pool, mut shares) = pool(0, 2, 1);
        assert_eq!(pool.ended(&mut shares[0], 0, 0, 0), Some(0));
        pool.wants(&mut shares[1], 1, 0);
        assert_eq!(pool.ended(&mut shares[1], 0, 1, 0), Some(0));
        assert_eq!(pool.stats().granted, 0);
    }

    /// A connection whose peer alone wants more keeps what it holds as its exchanges end: its own
    /// wait is no other's, and giving its streams back to grant them to itself again would leave
    /// it out of them until QUIC freed the streams it held.
    #[test]
    fn a_lone_waiting_connection_keeps_its_grant() {
        let (mut pool, mut shares) = pool(3, 1, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[0].granted, 4);
        pool.wants(&mut shares[0], 0, 0);
        assert_eq!(pool.ended(&mut shares[0], 0, 0, 0), None);
        assert_eq!(shares[0].granted, 4);
    }

    /// A busy connection holding its fair share keeps it while another waits, so no connection's
    /// open exchanges are cut under its fair share for another's want.
    #[test]
    fn a_busy_connection_at_its_fair_share_keeps_it() {
        let (mut pool, mut shares) = pool(4, 2, 1);
        pool.wants(&mut shares[0], 0, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(
            shares[0].granted, 3,
            "its fair share: its stream and the pool over two"
        );
        pool.wants(&mut shares[1], 1, 0);
        grant_all(&mut pool, &mut shares, 16);
        assert_eq!(shares[1].granted, 3);
        pool.wants(&mut shares[1], 1, 0);
        assert_eq!(pool.ended(&mut shares[0], 1, 0, 0), None);
        assert_eq!(shares[0].granted, 3);
    }

    /// A connection that joins an uncontended pool is granted its fair share at once; one that
    /// joins a pool with nothing spare keeps its handshake's grant.
    #[test]
    fn a_connection_joining_is_welcomed_with_what_is_spare() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        assert_eq!(pool.welcome(&mut shares[0], 16, 0, 0), Some(9));
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        assert_eq!(
            pool.welcome(&mut shares[1], 16, 1, 0),
            None,
            "nothing spare"
        );
        assert_eq!(shares[1].granted, 1);
        assert_eq!(pool.stats().granted, 10);
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

    /// An idle connection's credit past the stream it keeps is taken back for one that waits when
    /// nothing is spare, though it opened nothing to end: its peer may still open what it was told
    /// of (the overcommitment counted), and the waiting one is granted its fair share. One waiting
    /// is never taken from, and one busy again keeps its grant.
    #[test]
    fn an_idle_connections_credit_is_taken_back_for_one_waiting() {
        let (mut pool, mut shares) = pool(8, 1, 1);
        assert_eq!(pool.welcome(&mut shares[0], 16, 0, 0), Some(9));
        shares.push(Share::default());
        pool.join(&mut shares[1], 1, 1, 0);
        pool.wants(&mut shares[1], 1, 0);
        turn(&mut pool, &mut shares, 16, &[0, 0]);
        assert_eq!(
            (shares[0].granted, shares[1].granted),
            (1, 5),
            "taken back to the stream it keeps"
        );
        assert_eq!(pool.stats().reclaimed, 8);
        // A third joins and waits; the second, busy now, keeps its grant though it was queued idle.
        shares.push(Share::default());
        pool.join(&mut shares[2], 1, 2, 0);
        pool.resize(4);
        pool.wants(&mut shares[2], 2, 0);
        turn(&mut pool, &mut shares, 16, &[0, 3, 0]);
        assert_eq!(shares[1].granted, 5, "busy again: kept");
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
        assert_eq!(pool.grant(&mut shares[1], 16), 5);
        assert_eq!(pool.next(), None);
    }

    /// A parked exchange holds a stream of its own beside the grant, within what one connection
    /// may hold, and is no open one in the pool's accounts: a connection whose only open exchange
    /// is parked is idle, and its grant past the stream it keeps is taken back for one waiting,
    /// the parked stream kept. Ended, the parked stream goes with it.
    #[test]
    fn a_parked_exchange_holds_a_stream_of_its_own_and_none_of_the_pools() {
        let (mut pool, mut shares) = pool(1, 2, 1);
        assert_eq!(pool.welcome(&mut shares[0], 4, 0, 0), Some(2));
        // Connection 0's peer parks a wait: its limit rises past its grant, the pool's untouched.
        assert_eq!(pool.park(&mut shares[0], 4, 0, 0, 0), Some(3));
        assert_eq!((shares[0].granted, shares[0].parked), (2, 1));
        let stats = pool.stats();
        assert_eq!((stats.granted, stats.parked, stats.parks), (3, 1, 1));
        // Connection 1 waits with nothing spare: connection 0, idle in the pool's accounts though
        // its parked exchange is open, gives back its grant past the stream it keeps, and keeps
        // the parked stream.
        pool.wants(&mut shares[1], 1, 0);
        assert_eq!(pool.next(), None, "nothing spare");
        let idle = pool.next_idle();
        assert_eq!(idle, Some((0, 0)));
        assert_eq!(pool.reclaim(&mut shares[0], 0), Some(1));
        assert_eq!((shares[0].granted, shares[0].limit()), (1, 2));
        assert_eq!(pool.next(), Some((1, 0)));
        assert_eq!(pool.grant(&mut shares[1], 4), 2);
        // The parked exchange ends: its stream goes, granted to no other.
        assert_eq!(pool.unpark(&mut shares[0]), Some(1));
        assert_eq!(pool.unpark(&mut shares[0]), None, "nothing parked");
        let stats = pool.stats();
        assert_eq!((stats.granted, stats.parked), (3, 0));
    }

    /// A connection that holds as many streams as one may parks nothing more: the exchange is
    /// served in the pool's account, counted; and one that leaves takes its parked count with it.
    #[test]
    fn parking_stops_at_the_streams_one_connection_may_hold() {
        let (mut pool, mut shares) = pool(8, 1, 2);
        assert_eq!(pool.park(&mut shares[0], 3, 1, 0, 0), Some(3));
        assert_eq!(pool.park(&mut shares[0], 3, 1, 0, 0), None);
        let stats = pool.stats();
        assert_eq!((stats.parked, stats.parks, stats.parks_refused), (1, 1, 1));
        pool.leave(&mut shares[0]);
        assert_eq!(pool.stats().parked, 0);
        let mut outside = Share::default();
        assert_eq!(
            pool.park(&mut outside, 3, 0, 1, 0),
            None,
            "a role outside the pool"
        );
    }
}
