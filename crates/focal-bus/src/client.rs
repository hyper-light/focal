//! A bus client: publishes batches, subscribes to streams, and takes what the node delivers.
//!
//! Every delivery is checked whole against its producer's CRC before it is handed on: what a node,
//! a disk or a copy between them changed is refused here, end to end, whatever the transport's own
//! integrity said about the last hop. A gap notice is handed on as it is: the subscriber learns
//! exactly which offsets it missed.
//!
//! A client of a part's group follows its leader (doc 34 P2, P7): it sends to the node it knows
//! leads and takes a leader a node names only where its term is past the one it knew (KIP-951's
//! rule). It asks another node of the group when it cannot hear the one it sends to: its connection
//! closed, a dial to it failed, or its path degrading (hyper-transport's `Degrading`: nothing
//! acknowledged for four probe timeouts, quiche's rule). A Kafka producer asks another broker for
//! metadata on a disconnect or a request timeout; TCP hands it a dead broker's RST, where QUIC has
//! nothing from a host whose endpoint is gone and the client would wait for its idle timeout (two
//! minutes, [`crate::limits::IDLE_TIMEOUT`]). A degrading path is a reason to look elsewhere and
//! nothing more, as Chromium's session takes it: a busy node's loop can hold its acknowledgements
//! past four probe timeouts of a loopback or a LAN (169 ms measured on a node just restarted, where
//! four probe timeouts were about 104 ms), and its connection, its subscriptions with it, is kept
//! until the node is heard again (`Progressing`), when it is followed again, or the group names
//! another leader at a newer term, when it is given up, as a Kafka client disconnects a broker whose
//! request timed out. The node asked answers as any node that does not lead: with the leader it
//! knows, or it leads and acknowledges. One that names no leader, or names the leader the client
//! cannot hear at the term the client knows (the group has not yet noticed), is asked again, the
//! next node in turn, after a back-off: Kafka's producer's after a retriable error, doubled to its
//! bound and drawn within a fifth of it (KIP-580, [`crate::limits::ASK_BACKOFF_FIRST`]), once for
//! each round of asking, however many batches the node redirected in it; a leader the client has no
//! connection to
//! is dialed beside the asking, and followed again once it connects. A leader named at a newer term
//! is followed and, where its connection went, dialed afresh: the group that elected it is fresher
//! evidence than a connection the client saw go (a restarted node resets what its last run held).
//!
//! The caller is handed [`Delivery::Rerouted`] whenever the node it sends to changes, and sends that
//! node what it owes, while the client asks ([`Client::asking`]) only its oldest batch: the asked
//! node's answer names the leader or acknowledges, and the rest waits for the node that leads. The
//! part recognises a batch it took (doc 34 P6). It is handed
//! [`Delivery::Lost`] for each node whose connection ended, and subscribes again, from where it is,
//! wherever it held a subscription there: a subscription lives on its connection, and only on it.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use focal_memory::MemoryBudget;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{
    Budget, Classes, Config, Directory, Endpoint, Epoch, Event, ExchangeId, PeerId, Progress,
    Refusal,
};

use crate::BusError;
use crate::batch::{Batch, BatchError};
use crate::budget::BusBudget;
use crate::directory::BusDirectory;
use crate::limits;
use crate::pki;
use crate::wire::{
    self, Ack, Bus, Gap, Kind, LANE, Leader, Moved, Redirect, Reply, Role, Subscribe, Subscribed,
};

/// The endpoint of client identity `index` of the local directory `local` (`focal start bus
/// --pki`), its nodes 1 to `nodes` enrolled as the nodes it dials, under the client's bounds for a
/// group of that many (`limits::client_of`), its bytes charged to `budget`.
pub fn endpoint(
    local: &pki::Local,
    index: usize,
    nodes: usize,
    budget: MemoryBudget,
    now: Instant,
) -> Result<Endpoint<Bus, BusBudget, BusDirectory>, BusError> {
    let identity = local
        .clients
        .get(index)
        .ok_or(BusError::Configuration("no such client identity"))?;
    let mut directory = BusDirectory::new(nodes);
    for n in 1..=nodes {
        let node = u64::try_from(n).map_err(|_| BusError::Configuration("a node's number"))?;
        let certificate = &local
            .node(node)
            .ok_or(BusError::Configuration(
                "no identity for a node of the group",
            ))?
            .certificate;
        directory.enroll(certificate, node, Role::Node, &pki::node_name(node))?;
    }
    let credentials = Credentials {
        chain: vec![CertificateDer::from(identity.certificate.clone())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key.clone())),
        roots: vec![CertificateDer::from(local.root.clone())],
    };
    let config = Config {
        tls: Tls::mutual(&credentials, false)?,
        role: Role::Client,
        limits: limits::client_of(nodes)?,
        congestion: Default::default(),
    };
    Ok(Endpoint::new(
        config,
        BusBudget::new(budget),
        directory,
        now,
    )?)
}

/// What a client is handed.
#[derive(Debug)]
pub enum Delivery<'a> {
    /// A stored batch, its CRC checked.
    Batch(Batch<'a>),
    /// Offsets of a stream the node let go before this client took them.
    Gap(Gap),
    /// A subscription the node ended, for it stopped leading the stream's part (doc 34 P7):
    /// subscribe again where the client now sends ([`Client::node`]), from the offset it gives; every
    /// offset before it was delivered.
    Moved(Moved),
    /// A subscription's start: the stream's retained window when it was made.
    Subscribed {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// The reply.
        subscribed: Subscribed,
    },
    /// A subscribe the node did not take, for it does not lead the stream's part: the leader it
    /// knows (doc 34 P7).
    SubscribeRedirected {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// The leader the node knows.
        leader: Leader,
    },
    /// A publish its part's group committed (doc 34 P5).
    Acked(Ack),
    /// A publish the node did not take, for it does not lead the batch's part, and the leader it
    /// knows (doc 34 P2).
    Redirected(Redirect),
    /// The node the client sends to changed: it follows another leader, asks another node about the
    /// one it cannot hear, or follows its leader again. Send that node what is owed, once
    /// [`Client::ready`]: every batch not acknowledged.
    Rerouted {
        /// The node sends go to now.
        node: PeerId,
    },
    /// A delivery that did not pass its producer's CRC, or a frame that was no delivery.
    Refused(BatchError),
    /// A subscribe the node or the transport refused, and why.
    Unsubscribed {
        /// The exchange that asked.
        exchange: ExchangeId,
        /// Why.
        refusal: Refusal,
    },
    /// The connection to `node` ended, given up by the client or by either side, or a dial to the
    /// node of a lone client failed: nothing more comes over it, and every subscription it carried
    /// is gone, to be made again from where it is. Why, when either side said. A client of a group
    /// that sent to it asks another node ([`Delivery::Rerouted`]).
    Lost {
        /// The node.
        node: PeerId,
        /// Why, when either side said.
        refusal: Option<Refusal>,
    },
}

/// What a client last heard of a node of its group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hearing {
    /// Nothing amiss: connected, or not yet dialed.
    Heard,
    /// Its connection is open and nothing sent on it has been acknowledged for four probe timeouts
    /// (`Event::Degrading`): the endpoint says when it is heard again (`Event::Progressing`) or
    /// gives the connection up (`Event::Closed`).
    Degrading,
    /// It has no connection: its connection ended, or a dial to it failed. Only a dial tells more.
    Lost,
}

/// A node of a part's group as its client knows it.
#[derive(Clone, Copy, Debug)]
struct Member {
    node: PeerId,
    address: SocketAddr,
    heard: Hearing,
    /// The epoch of its connection the client was last told is up, while it is.
    connected: Option<Epoch>,
}

/// A client of one node, or of a part's group.
#[derive(Debug)]
pub struct Client {
    /// The group's nodes, in the order the client asks them; none for a client of a lone node,
    /// which asks no other.
    members: Vec<Member>,
    /// The node the client follows as its part's leader: the one a node last named, or the first.
    leader: PeerId,
    /// The term it was named at; zero before a node named one.
    term: u64,
    /// The node asked while the leader is not heard.
    asking: Option<PeerId>,
    /// Sends wait until then: the node asked named no leader the client could take.
    held_until: Option<Instant>,
    /// What the next wait is drawn about.
    backoff: Duration,
    /// The state each wait's draw comes from (SplitMix64), seeded from the operating system for a
    /// client of a group, so no two clients wait alike.
    draws: u64,
}

impl Client {
    /// A client of the node the directory names `node`, which the caller dials.
    pub const fn new(node: PeerId) -> Self {
        Self {
            members: Vec::new(),
            leader: node,
            term: 0,
            asking: None,
            held_until: None,
            backoff: limits::ASK_BACKOFF_FIRST,
            draws: 0,
        }
    }

    /// A client of a part's group: every node of it and its address, in the order the client asks
    /// them. It follows the first until a node names the leader; [`Client::dial`] dials it.
    pub fn group(nodes: &[(PeerId, SocketAddr)]) -> Result<Self, BusError> {
        let (first, _) = nodes
            .first()
            .ok_or(BusError::Configuration("a group of no nodes"))?;
        let mut client = Self::new(*first);
        let mut seed = [0; 8];
        getrandom::fill(&mut seed)
            .map_err(|_| BusError::Configuration("the operating system's random bytes"))?;
        client.draws = u64::from_le_bytes(seed);
        client.members = nodes
            .iter()
            .map(|&(node, address)| Member {
                node,
                address,
                heard: Hearing::Heard,
                connected: None,
            })
            .collect();
        Ok(client)
    }

    /// The node publishes and subscribes go to: the one asked while the leader is not heard,
    /// otherwise the leader.
    pub fn node(&self) -> PeerId {
        self.asking.unwrap_or(self.leader)
    }

    /// The leader the client follows, and the term it was named at (zero before a node named one).
    pub const fn leader(&self) -> (PeerId, u64) {
        (self.leader, self.term)
    }

    /// Whether the client is asking a node about a leader it cannot hear, rather than sending to the
    /// node it knows leads: send the asked node only the oldest batch owed, whose answer names the
    /// leader or acknowledges it.
    pub const fn asking(&self) -> bool {
        self.asking.is_some()
    }

    /// Whether sends may go now; otherwise they wait until [`Client::held_until`].
    pub fn ready(&self, now: Instant) -> bool {
        self.held_until.is_none_or(|until| now >= until)
    }

    /// Until when sends wait, if they do.
    pub const fn held_until(&self) -> Option<Instant> {
        self.held_until
    }

    /// Dials the node sends go to, where the client knows its address (a client of a group); one
    /// connected or being dialed is left as it is.
    pub fn dial<B, D>(
        &self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
    ) -> Result<(), Refusal>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        match self.member(self.node()) {
            Some(member) => endpoint.connect(now, member.node, member.address),
            None => Ok(()),
        }
    }

    /// Queues `batch` for the node sends go to; `LaneFull` while the lane has no room, after which
    /// the endpoint says `LaneRoom`, and `NotConnected` while the node has no connection, which
    /// [`Client::dial`] makes.
    pub fn publish<B, D>(
        &self,
        endpoint: &mut Endpoint<Bus, B, D>,
        batch: &[u8],
    ) -> Result<(), Refusal>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        endpoint.send_frame(self.node(), LANE, Kind::Publish, batch)
    }

    /// Asks the node sends go to for `stream` from `from`, judged at `period`.
    pub fn subscribe<B, D>(
        &self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        stream: u64,
        from: wire::From,
        period: Duration,
    ) -> Result<ExchangeId, Refusal>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let head = Subscribe { stream, from }.encode();
        endpoint.open(
            now,
            self.node(),
            Kind::Subscribe,
            &head,
            None,
            Progress::new(period)?,
        )
    }

    /// Acts on one of the endpoint's events at `now`, handing what it delivers to `take`.
    pub fn on_event<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        event: Event<Bus>,
        now: Instant,
        mut take: impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        match event {
            Event::Frame {
                kind: Kind::Deliver,
                frame,
                ..
            } => {
                let limit = usize::try_from(wire::BATCH_BOUND).unwrap_or(usize::MAX);
                // The transport checked the batch's CRC where the wire says it carries one.
                let parse = if Bus::embedded_checksum(Kind::Deliver).is_some() {
                    Batch::parse_verified
                } else {
                    Batch::parse
                };
                match parse(frame.bytes(), limit) {
                    Ok(batch) => take(Delivery::Batch(batch)),
                    Err(refused) => take(Delivery::Refused(refused)),
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Gap,
                frame,
                ..
            } => {
                match Gap::decode(frame.bytes()) {
                    Ok(gap) => take(Delivery::Gap(gap)),
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
                endpoint.release(frame);
            }
            Event::Frame {
                kind: Kind::Moved,
                frame,
                ..
            } => {
                let moved = Moved::decode(frame.bytes());
                endpoint.release(frame);
                match moved {
                    Ok(moved) => {
                        let leader = moved.leader;
                        take(Delivery::Moved(moved));
                        self.follow(endpoint, now, leader, &mut take);
                    }
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
            }
            Event::Frame {
                peer,
                kind: Kind::Ack,
                frame,
                ..
            } => {
                let ack = Ack::decode(frame.bytes());
                endpoint.release(frame);
                match ack {
                    Ok(ack) => {
                        // A node that acknowledges a publish leads its part: the node asked
                        // leads, though no node named it.
                        if self.asking == Some(peer) {
                            self.leave(endpoint, now, peer);
                            self.leader = peer;
                            self.asking = None;
                            self.held_until = None;
                            self.backoff = limits::ASK_BACKOFF_FIRST;
                        }
                        take(Delivery::Acked(ack));
                    }
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
            }
            Event::Frame {
                kind: Kind::Redirect,
                frame,
                ..
            } => {
                let redirect = Redirect::decode(frame.bytes());
                endpoint.release(frame);
                match redirect {
                    Ok(redirect) => {
                        let leader = redirect.leader;
                        take(Delivery::Redirected(redirect));
                        self.follow(endpoint, now, leader, &mut take);
                    }
                    Err(_) => take(Delivery::Refused(BatchError::Short)),
                }
            }
            Event::Frame { frame, .. } => endpoint.release(frame),
            Event::Reply { exchange, .. } => {
                let reply = endpoint.head(exchange).map(Reply::decode);
                endpoint.end(exchange);
                match reply {
                    Some(Ok(Reply::Subscribed(subscribed))) => take(Delivery::Subscribed {
                        exchange,
                        subscribed,
                    }),
                    Some(Ok(Reply::Redirected(leader))) => {
                        take(Delivery::SubscribeRedirected { exchange, leader });
                        self.follow(endpoint, now, leader, &mut take);
                    }
                    _ => {}
                }
            }
            Event::Refused {
                exchange, refusal, ..
            } => take(Delivery::Unsubscribed { exchange, refusal }),
            Event::Connected { peer, epoch, .. } => self.connected(peer, epoch, &mut take),
            Event::Closed {
                peer,
                epoch,
                refusal,
            } => self.gone(endpoint, now, peer, Some(epoch), refusal, &mut take),
            Event::Unreachable { peer, refusal } => {
                self.gone(endpoint, now, peer, None, refusal, &mut take);
            }
            Event::Degrading { peer, .. } => {
                self.unheard(endpoint, now, peer, Hearing::Degrading, &mut take);
            }
            Event::Progressing { peer, .. } => self.heard(peer, &mut take),
            Event::Request { .. }
            | Event::BodyReady { .. }
            | Event::Writable { .. }
            | Event::LaneRoom { .. } => {}
        }
    }

    fn member(&self, node: PeerId) -> Option<&Member> {
        self.members.iter().find(|member| member.node == node)
    }

    fn member_mut(&mut self, node: PeerId) -> Option<&mut Member> {
        self.members.iter_mut().find(|member| member.node == node)
    }

    /// Sends wait a back-off from `now`, drawn within a fifth of it and no longer than its bound,
    /// and the next back-off doubles to its bound (Kafka's `ExponentialBackoff`, KIP-580).
    fn hold(&mut self, now: Instant) {
        let span = limits::ASK_BACKOFF_JITTER_PERMILLE
            .saturating_mul(2)
            .saturating_add(1);
        let factor = (1_000_u64.saturating_sub(limits::ASK_BACKOFF_JITTER_PERMILLE))
            .saturating_add(self.draw().checked_rem(span).unwrap_or(0));
        let nanos = u64::try_from(self.backoff.as_nanos()).unwrap_or(u64::MAX);
        let wait =
            Duration::from_nanos(nanos.checked_div(1_000).unwrap_or(0).saturating_mul(factor))
                .min(limits::ASK_BACKOFF_MOST);
        self.held_until = now.checked_add(wait);
        self.backoff = self.backoff.saturating_mul(2).min(limits::ASK_BACKOFF_MOST);
    }

    /// The next draw of the client's waits (SplitMix64: Steele, Lea and Flood, "Fast Splittable
    /// Pseudorandom Number Generators", OOPSLA 2014).
    fn draw(&mut self) -> u64 {
        self.draws = self.draws.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.draws;
        mixed = (mixed ^ mixed.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ mixed.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ mixed.wrapping_shr(31)
    }

    /// The node after `after` in the group's order, other than the leader, that the client heard
    /// nothing amiss of; failing that, the first other node in that order: the next to ask. None in
    /// a group of one. A node outside the group asks from the first.
    fn next_after(&self, after: PeerId) -> Option<PeerId> {
        let count = self.members.len();
        let at = self
            .members
            .iter()
            .position(|member| member.node == after)
            .unwrap_or(count.saturating_sub(1));
        let others = || {
            (1..=count)
                .filter_map(move |step| {
                    at.checked_add(step)
                        .and_then(|index| index.checked_rem(count))
                        .and_then(|index| self.members.get(index))
                })
                .filter(|member| member.node != self.leader)
        };
        others()
            .find(|member| member.heard == Hearing::Heard)
            .or_else(|| others().next())
            .map(|member| member.node)
    }

    /// Asks the node after `after` about the leader the client cannot hear, dialing it: a node whose
    /// dial the endpoint refuses (every seat of its peer table held), which no event will tell of,
    /// is passed for the next; once every node was, sends wait a back-off.
    fn ask<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        after: PeerId,
        take: &mut impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let mut from = after;
        // Bounded: each pass tries another node of the group.
        for _ in 0..self.members.len() {
            let Some(next) = self.next_after(from) else {
                return;
            };
            self.asking = Some(next);
            if self.dial(endpoint, now).is_ok() {
                take(Delivery::Rerouted { node: next });
                return;
            }
            if let Some(member) = self.member_mut(next) {
                member.heard = Hearing::Lost;
            }
            from = next;
        }
        self.hold(now);
        take(Delivery::Rerouted { node: self.node() });
    }

    /// Takes the leader a node named where its term is past the one the client knows (KIP-951): the
    /// leader it leaves, where its path is degrading, no longer leads, and its connection is given
    /// up (its close told as any other, `Event::Closed`); the new one, where its connection went, is
    /// dialed afresh, the group that elected it fresher evidence than the connection that went, and
    /// where its path is degrading the client goes on asking until it is heard. A node that names
    /// none, or names the leader the client cannot hear at the term it knows, speaks for a group
    /// that has not yet elected another: the next node is asked after a back-off, and the leader,
    /// where it has no connection, dialed beside the asking. A client of a lone node has no group to
    /// follow: its caller dials, and is handed what the node said.
    fn follow<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        named: Leader,
        take: &mut impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if self.members.is_empty() {
            return;
        }
        match named.node {
            Some(node) if named.term > self.term => {
                let before = self.node();
                self.leave(endpoint, now, node);
                self.leader = node;
                self.term = named.term;
                self.asking = None;
                self.held_until = None;
                self.backoff = limits::ASK_BACKOFF_FIRST;
                let heard = self
                    .member(node)
                    .map_or(Hearing::Heard, |member| member.heard);
                if heard == Hearing::Degrading {
                    self.hold(now);
                    self.ask(endpoint, now, node, take);
                    return;
                }
                if let Some(member) = self.member_mut(node) {
                    member.heard = Hearing::Heard;
                }
                // A dial refused here is made again when a send finds no connection.
                let _ = self.dial(endpoint, now);
                if self.node() != before {
                    take(Delivery::Rerouted { node });
                }
            }
            // A redirect of a round of asking the client already holds for, one of the batches the
            // node redirected in it, steps nothing again.
            _ if !self.ready(now) => {}
            _ if self.asking.is_some() => {
                if named.node == Some(self.leader)
                    && let Some(member) = self.member(self.leader)
                    && member.heard == Hearing::Lost
                {
                    let _ = endpoint.connect(now, member.node, member.address);
                }
                self.hold(now);
                if let Some(asked) = self.asking {
                    self.ask(endpoint, now, asked, take);
                }
            }
            // The node followed knows no leader: an election is under way there.
            None => self.hold(now),
            Some(_) => {}
        }
    }

    /// The client follows `next` as its part's leader from now: the leader it leaves, where its path
    /// is degrading, no longer leads, and its connection is given up, what it carried with it (its
    /// close told as any other, `Event::Closed`).
    fn leave<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, now: Instant, next: PeerId)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let left = self.leader;
        if left == next {
            return;
        }
        if let Some(member) = self.member_mut(left)
            && member.heard == Hearing::Degrading
        {
            member.heard = Hearing::Lost;
            endpoint.disconnect(now, left);
        }
    }

    /// `peer`'s connection of `epoch` is up: a connection told up before under another epoch went
    /// unseen (the endpoint tells a peer's latest state), and is told lost. It is heard.
    fn connected(&mut self, peer: PeerId, epoch: Epoch, take: &mut impl FnMut(Delivery<'_>)) {
        let Some(member) = self.member_mut(peer) else {
            return;
        };
        let went = member.connected.is_some_and(|known| known != epoch);
        member.connected = Some(epoch);
        if went {
            take(Delivery::Lost {
                node: peer,
                refusal: None,
            });
        }
        self.heard(peer, take);
    }

    /// `peer` is heard: connected, or its path making progress again. Where it is the leader the
    /// client asked about, it is followed again.
    fn heard(&mut self, peer: PeerId, take: &mut impl FnMut(Delivery<'_>)) {
        let Some(member) = self.member_mut(peer) else {
            return;
        };
        member.heard = Hearing::Heard;
        if peer == self.leader && self.asking.is_some() {
            self.asking = None;
            self.held_until = None;
            self.backoff = limits::ASK_BACKOFF_FIRST;
            take(Delivery::Rerouted { node: peer });
        }
    }

    /// `peer`'s connection of `epoch` ended, or a dial to it failed (no epoch): told lost where a
    /// connection to it was up or could have carried what was asked of it, and asked about as
    /// [`Client::unheard`] says. A lone node's client is told of the node it sends to.
    fn gone<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        peer: PeerId,
        epoch: Option<Epoch>,
        refusal: Option<Refusal>,
        take: &mut impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let Some(member) = self.member_mut(peer) else {
            if peer == self.node() {
                take(Delivery::Lost {
                    node: peer,
                    refusal,
                });
            }
            return;
        };
        // A close is of the peer's newest connection, the one its exchanges and lanes are on;
        // a failed dial follows a connection only where one was told up.
        let carried = match epoch {
            Some(epoch) => member.connected.is_none_or(|known| known == epoch),
            None => member.connected.is_some(),
        };
        member.connected = None;
        if carried {
            take(Delivery::Lost {
                node: peer,
                refusal,
            });
        }
        self.unheard(endpoint, now, peer, Hearing::Lost, take);
    }

    /// `peer` cannot be heard, as `heard` says: its path degrading, its connection ended or given
    /// up, or a dial to it failed. Where it is the leader the client asks the next node at once;
    /// where it is the node asked, the one after it, after a back-off.
    fn unheard<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        now: Instant,
        peer: PeerId,
        heard: Hearing,
        take: &mut impl FnMut(Delivery<'_>),
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let Some(member) = self.member_mut(peer) else {
            return;
        };
        member.heard = heard;
        match self.asking {
            Some(asked) if asked == peer => {
                self.hold(now);
                self.ask(endpoint, now, peer, take);
            }
            None if peer == self.leader => self.ask(endpoint, now, peer, take),
            _ => {}
        }
    }
}

impl From<Refusal> for BusError {
    fn from(refusal: Refusal) -> Self {
        Self::Transport(refusal)
    }
}
