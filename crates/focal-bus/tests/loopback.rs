//! A bus node, a publisher and a subscriber over real UDP sockets on loopback, their three
//! endpoints driven by one task on one hyper-rt shard: every published batch reaches the
//! subscriber in order with its producer's CRC intact; a subscriber that stops reading holds the
//! publisher feeding its stream (the node holds the publisher's lane), so nothing is let go from
//! under it, and once it reads again it takes every batch, in order, none missed; the node keeps
//! what its part's memory holds, letting the oldest batches go once it holds no more; a
//! subscription that starts below the window's floor is told exactly which offsets it missed; and
//! what the node holds of the budget goes back. Every bound is the bus's own (`focal_bus::limits`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::{BusBudget, Headroom};
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::IO;
use focal_bus::node::{Node, NodeConfig};
use focal_bus::wire::{self, Bus, Class, Role};
use focal_memory::MemoryBudget;
use focal_timing::ProgressDeadline;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Budget, Config, Endpoint, Event, Lane, Refusal, Reservation};

type Drv = Driver<Bus, Starving, BusDirectory>;

/// The bus's budget, refusing the next delivery frames it is told to: how a node whose memory is
/// full looks to its deliveries. It refuses publish frames while told to, its memory as free as
/// ever: how a frame that waits for anything but memory looks to the node. It refuses nothing
/// until told.
struct Starving {
    inner: BusBudget,
    refusing: Refusing,
}

/// What a [`Starving`] budget refuses, set from outside it.
#[derive(Clone, Default)]
struct Refusing {
    /// Delivery frames still to refuse.
    deliver: Rc<Cell<u32>>,
    /// Whether publish frames are refused.
    publish: Rc<Cell<bool>>,
}

impl Budget<Class> for Starving {
    fn reserve(&mut self, bytes: u64, lane: Lane<Class>) -> Result<Reservation, Refusal> {
        let deliver = &self.refusing.deliver;
        if matches!(lane, Lane::Class(Class::Deliver)) && deliver.get() > 0 {
            deliver.set(deliver.get() - 1);
            return Err(Refusal::Budget);
        }
        if matches!(lane, Lane::Class(Class::Publish)) && self.refusing.publish.get() {
            return Err(Refusal::Budget);
        }
        self.inner.reserve(bytes, lane)
    }

    fn release(&mut self, reservation: Reservation) {
        self.inner.release(reservation);
    }
}

impl Headroom for Starving {
    fn headroom(&self) -> u64 {
        self.inner.headroom()
    }
}

const NODE: u64 = 1;
const PUBLISHER: u64 = 2;
const SUBSCRIBER: u64 = 3;
const STREAM: u64 = 77;
/// Batches published while the subscriber reads.
const IN_ORDER: u32 = 1_000;
/// How far a subscriber that keeps up is behind the publisher, at most: well inside the node's
/// window, so it is never left behind.
const KEEPS_UP: u32 = 200;
/// A wait fails only when nothing it waits on moved for this long: a wedge, not slowness.
const FROZEN: Duration = Duration::from_secs(30);
/// The segments the node's part has memory for, of the least size a segment may be (one batch of
/// the largest): what the window holds before it lets the oldest batches go.
const PART_SEGMENTS: usize = 3;

struct Pki {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    root: Vec<u8>,
}

impl Pki {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let root = params.self_signed(&key).unwrap().der().to_vec();
        Self {
            issuer: rcgen::Issuer::new(params, key),
            root,
        }
    }

    fn issue(&self, name: &str) -> (Vec<u8>, Vec<u8>) {
        let params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        (certificate.der().to_vec(), key.serialize_der())
    }

    fn credentials(&self, own: &(Vec<u8>, Vec<u8>)) -> Credentials {
        Credentials {
            chain: vec![CertificateDer::from(own.0.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(own.1.clone())),
            roots: vec![CertificateDer::from(self.root.clone())],
        }
    }
}

fn runtime() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

fn endpoint(
    pki: &Pki,
    own: &(Vec<u8>, Vec<u8>),
    peers: &[(&[u8], u64, Role, &str)],
    role: Role,
    listen: bool,
    budget: &MemoryBudget,
    refusing: &Refusing,
) -> Endpoint<Bus, Starving, BusDirectory> {
    let mut directory = BusDirectory::new(8);
    for (certificate, peer, role, name) in peers {
        directory.enroll(certificate, *peer, *role, name).unwrap();
    }
    let config = Config {
        tls: Tls::mutual(&pki.credentials(own), listen).unwrap(),
        role,
        limits: match role {
            Role::Node => focal_bus::limits::node(2, 0).unwrap(),
            Role::Client => focal_bus::limits::client().unwrap(),
        },
        congestion: Default::default(),
    };
    Endpoint::new(
        config,
        Starving {
            inner: BusBudget::new(budget.clone()),
            refusing: refusing.clone(),
        },
        directory,
        Instant::now(),
    )
    .unwrap()
}

/// The next event of any driver polled, as (whose, event); `None` for none within a tick. Each
/// call starts with the next driver in turn, so none waits behind another that is always busy. A
/// subscriber left out has stopped reading: what is sent to it waits in its socket, unacknowledged.
async fn next(
    turn: usize,
    node: &mut Drv,
    publisher: &mut Drv,
    mut subscriber: Option<&mut Drv>,
) -> Option<(u64, Event<Bus>)> {
    let mut tick = pin!(hyper_rt::futures::sleep(1_000_000));
    poll_fn(|context| {
        for at in 0..3 {
            let whose = [NODE, PUBLISHER, SUBSCRIBER][(turn + at) % 3];
            let driver = match whose {
                NODE => Some(&mut *node),
                PUBLISHER => Some(&mut *publisher),
                _ => subscriber.as_deref_mut(),
            };
            if let Some(driver) = driver
                && let Poll::Ready(event) = driver.poll_event(context)
            {
                return Poll::Ready(Some((whose, event.unwrap())));
            }
        }
        if tick.as_mut().poll(context).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

fn batch(builder: &mut BatchBuilder, sequence: u32) -> Vec<u8> {
    builder.begin(&Producing {
        stream: STREAM,
        producer: PUBLISHER,
        sequence,
        flags: 0,
        timestamp: 1_792_022_400_000_000,
    });
    builder
        .push(1_792_022_400_000_000, &u64::from(sequence).to_le_bytes())
        .unwrap();
    builder.finish().unwrap().to_vec()
}

/// What the subscriber was handed, in the order handed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Item {
    Batch { base: u64, end: u64, sequence: u64 },
    Gap { from: u64, to: u64 },
}

#[derive(Default)]
struct Seen {
    connected: u32,
    subscribed: bool,
    /// Each subscription's reply: the stream's window when it was made, and where it starts.
    replies: Vec<wire::Subscribed>,
    order: Vec<Item>,
    refused: u64,
}

impl Seen {
    /// The offset the subscriber has accounted for: delivered, or told it missed.
    fn reached(&self) -> u64 {
        match self.order.last() {
            Some(Item::Batch { end, .. }) => *end,
            Some(Item::Gap { to, .. }) => *to,
            None => 0,
        }
    }
}

struct Harness {
    node_driver: Drv,
    node: Node,
    publisher: Drv,
    subscriber: Drv,
    publishing: Client,
    subscribing: Client,
    builder: BatchBuilder,
    sent: u32,
    seen: Seen,
    events: u64,
    turn: usize,
    wait: ProgressDeadline,
    /// What the node's budget refuses (`Starving`).
    refusing: Refusing,
    /// The node's transport's reservations given back as the harness last looked.
    returns: u64,
}

impl Harness {
    /// Handles the next event of any endpoint, the subscriber's only while it `reads`.
    async fn step(&mut self, reads: bool) {
        let subscriber = reads.then_some(&mut self.subscriber);
        self.turn = self.turn.wrapping_add(1);
        let Some((which, event)) = next(
            self.turn,
            &mut self.node_driver,
            &mut self.publisher,
            subscriber,
        )
        .await
        else {
            // No event within a tick: a delivery that found no memory is asked again once the
            // node's transport gave memory back, as the node's own loop asks (`serve`).
            let returns = self.node_driver.endpoint().budget().inner.stats().returns;
            if self.node.starving() && returns != self.returns {
                self.returns = returns;
                self.node.relieve_transport(self.node_driver.endpoint());
            }
            return;
        };
        self.events += 1;
        if which == NODE {
            self.node.on_event(self.node_driver.endpoint(), event);
            while let Some(event) = self.node_driver.endpoint().poll_event() {
                self.node.on_event(self.node_driver.endpoint(), event);
            }
            self.returns = self.node_driver.endpoint().budget().inner.stats().returns;
            self.node.relieve_transport(self.node_driver.endpoint());
            return;
        }
        let (driver, client) = if which == PUBLISHER {
            (&mut self.publisher, &mut self.publishing)
        } else {
            (&mut self.subscriber, &mut self.subscribing)
        };
        let seen = &mut self.seen;
        let mut handle = |event: Event<Bus>, driver: &mut Drv| {
            if let Event::Connected { .. } = &event {
                seen.connected += 1;
            }
            client.on_event(driver.endpoint(), event, |delivery| match delivery {
                Delivery::Batch(batch) => seen.order.push(Item::Batch {
                    base: batch.base_offset(),
                    end: batch.end_offset(),
                    sequence: batch.sequence().into(),
                }),
                Delivery::Gap(gap) => seen.order.push(Item::Gap {
                    from: gap.from,
                    to: gap.to,
                }),
                Delivery::Subscribed { subscribed, .. } => {
                    seen.subscribed = true;
                    seen.replies.push(subscribed);
                }
                Delivery::Refused(_) => seen.refused += 1,
                Delivery::Unsubscribed { refusal, .. } => {
                    panic!("a subscribe refused: {refusal:?}")
                }
                Delivery::Lost(why) => panic!("the node's connection ended: {why:?}"),
                // A node that runs no group leads every stream it holds, and acknowledges nothing:
                // its class is at most once.
                Delivery::SubscribeRedirected { leader, .. } => {
                    panic!("a lone node redirected a subscribe to {leader:?}")
                }
                Delivery::Acked(ack) => panic!("a lone node acknowledged {ack:?}"),
                Delivery::Redirected(redirect) => {
                    panic!("a lone node redirected a publish: {redirect:?}")
                }
            });
        };
        handle(event, driver);
        while let Some(event) = driver.endpoint().poll_event() {
            handle(event, driver);
        }
    }

    /// Publishes batches up to `until` while the publisher's lane has room.
    fn publish(&mut self, until: u32) {
        while self.sent < until {
            let bytes = batch(&mut self.builder, self.sent);
            match self.publishing.publish(self.publisher.endpoint(), &bytes) {
                Ok(()) => self.sent += 1,
                Err(Refusal::LaneFull) => return,
                Err(other) => panic!("publish refused: {other:?}"),
            }
        }
    }

    /// Fails the test once nothing moved for the frozen window.
    fn moved(&mut self, what: &str) {
        if let Err(spent) = self.wait.check(&[self.events]) {
            panic!(
                "the bus stopped while {what}: {spent}; sent {}, the subscriber reached {}",
                self.sent,
                self.seen.reached()
            );
        }
    }

    /// The stream's retained window at the node: (floor, next).
    fn bounds(&self) -> (u64, u64) {
        self.node.part().bounds(STREAM).unwrap_or((0, 0))
    }
}

impl Harness {
    /// A node, a publisher and a subscriber of the node's budgets and config over loopback, both
    /// clients connected and the subscriber subscribed from the stream's tail.
    async fn connect(
        pki: &Pki,
        node_budget: &MemoryBudget,
        client_budget: &MemoryBudget,
        part_budget: &MemoryBudget,
        config: NodeConfig,
    ) -> Self {
        let node_id = pki.issue("bus-node");
        let publisher_id = pki.issue("bus-publisher");
        let subscriber_id = pki.issue("bus-subscriber");
        let refusing = Refusing::default();
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let node_endpoint = endpoint(
            pki,
            &node_id,
            &[
                (&publisher_id.0, PUBLISHER, Role::Client, "bus-publisher"),
                (&subscriber_id.0, SUBSCRIBER, Role::Client, "bus-subscriber"),
            ],
            Role::Node,
            true,
            node_budget,
            &refusing,
        );
        let node_driver: Drv = Driver::bind(node_endpoint, loopback, IO).unwrap();
        let node_addr = node_driver.local_addr().unwrap();
        let directory = [(&node_id.0[..], NODE, Role::Node, "bus-node")];
        let client = |own: &(Vec<u8>, Vec<u8>)| {
            let endpoint = endpoint(
                pki,
                own,
                &directory,
                Role::Client,
                false,
                client_budget,
                &Refusing::default(),
            );
            let driver: Drv = Driver::bind(endpoint, loopback, IO).unwrap();
            driver
        };
        let mut publisher = client(&publisher_id);
        let mut subscriber = client(&subscriber_id);
        let now = publisher.clock().unwrap();
        publisher.endpoint().connect(now, NODE, node_addr).unwrap();
        let now = subscriber.clock().unwrap();
        subscriber.endpoint().connect(now, NODE, node_addr).unwrap();
        let mut bus = Harness {
            node_driver,
            node: Node::new(config, part_budget.clone()).unwrap(),
            publisher,
            subscriber,
            publishing: Client::new(NODE),
            subscribing: Client::new(NODE),
            builder: BatchBuilder::new(),
            sent: 0,
            seen: Seen::default(),
            events: 0,
            turn: 0,
            wait: ProgressDeadline::begin(&[0], u64::MAX, FROZEN),
            refusing,
            returns: 0,
        };
        while bus.seen.connected < 2 {
            bus.step(true).await;
            bus.moved("connecting");
        }
        let now = bus.subscriber.clock().unwrap();
        bus.subscribing
            .subscribe(
                bus.subscriber.endpoint(),
                now,
                STREAM,
                wire::From::Tail,
                focal_bus::limits::SERVE_PERIOD,
            )
            .unwrap();
        while !bus.seen.subscribed {
            bus.step(true).await;
            bus.moved("subscribing");
        }
        bus
    }
}

#[test]
fn every_publish_reaches_the_subscriber_in_order_and_a_stalled_subscriber_holds_its_publisher() {
    let pki = Pki::new();
    let node_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let client_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let segment = usize::try_from(wire::BATCH_BOUND).unwrap();
    let part_budget = node_budget.child(PART_SEGMENTS * segment, 0).unwrap();
    let config = NodeConfig {
        segment_bytes: segment,
        streams: 64,
        subscriptions: 16,
        producers: 64,
    };
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    let report = runtime
        .block_on({
            let node_budget = node_budget.clone();
            async move {
                let mut bus =
                    Harness::connect(&pki, &node_budget, &client_budget, &part_budget, config)
                        .await;

                // 1,000 batches to a subscriber that keeps up: the publisher holds back while its
                // lane is full or it is KEEPS_UP ahead of what the subscriber took.
                while bus.seen.reached() < u64::from(IN_ORDER) {
                    let ahead = u32::try_from(bus.seen.reached()).unwrap() + KEEPS_UP;
                    bus.publish(IN_ORDER.min(ahead));
                    bus.step(true).await;
                    bus.moved("delivering in order");
                }
                for (at, item) in bus.seen.order.iter().enumerate() {
                    let at = at as u64;
                    assert_eq!(
                        *item,
                        Item::Batch {
                            base: at,
                            end: at + 1,
                            sequence: at
                        },
                        "in order, each once"
                    );
                }

                // The subscriber stops reading. Its lane fills (about a congestion window, the lane
                // being lossless), and the node holds the publisher feeding its stream: the
                // publisher's own lane fills and stays full, the node takes nothing more, and no
                // offset is let go from under the subscriber.
                let published = bus.node.stats().published;
                let mut still = 0;
                let mut last = published;
                for _ in 0..100_000 {
                    bus.publish(u32::MAX);
                    bus.step(false).await;
                    bus.moved("the subscriber stalled");
                    let now = bus.node.stats().published;
                    if bus.node.stats().held > 0 && now == last {
                        still += 1;
                        // Held: a run of turns in which the node took nothing more.
                        if still >= 200 {
                            break;
                        }
                    } else {
                        still = 0;
                    }
                    last = now;
                }
                assert!(bus.node.stats().held > 0, "the node held the publisher");
                assert!(still >= 200, "the publisher was held still");
                assert_eq!(
                    bus.node.stats().gaps,
                    0,
                    "nothing let go from under the subscriber"
                );
                let held_at = bus.sent;

                // The subscriber reads again: it takes everything published, in order, none
                // missed, and the publisher goes on.
                let end = u64::from(held_at) + 1_000;
                while bus.seen.reached() < end {
                    bus.publish(u32::try_from(end).unwrap());
                    bus.step(true).await;
                    bus.moved("the subscriber catches up");
                }
                // The node keeps what its part's memory holds. The subscriber reading along, the
                // publisher goes on until the window lets the stream's oldest batches go: the
                // subscriber had taken them, and misses none.
                while bus.bounds().0 == 0 {
                    let ahead = u32::try_from(bus.seen.reached()).unwrap() + KEEPS_UP;
                    bus.publish(ahead);
                    bus.step(true).await;
                    bus.moved("filling the window");
                }
                let end = u64::from(bus.sent);
                while bus.seen.reached() < end {
                    bus.step(true).await;
                    bus.moved("the subscriber takes the rest");
                }
                let (floor, next) = bus.bounds();
                assert_eq!(next, end, "the node appended every batch published");
                assert!(
                    bus.node.pool().held() <= PART_SEGMENTS,
                    "the window held no more than its memory"
                );
                // The segment the window let go was the next one's: taken again warm, nothing
                // allocated past what the memory holds and nothing returned to the system, where a
                // segment given back was allocated anew and faulted in page by page.
                let stats = bus.node.stats();
                assert!(
                    stats.segments_allocated <= u64::try_from(PART_SEGMENTS).unwrap(),
                    "segments allocated past the memory: {stats:?}"
                );
                assert_eq!(
                    stats.segments_released, 0,
                    "a segment went back to the system and was allocated anew: {stats:?}"
                );
                for (at, item) in bus.seen.order.iter().enumerate() {
                    let at = at as u64;
                    assert_eq!(
                        *item,
                        Item::Batch {
                            base: at,
                            end: at + 1,
                            sequence: at
                        },
                        "every batch, in order, none missed"
                    );
                }
                assert_eq!(bus.node.stats().gaps, 0);
                assert_eq!(
                    bus.seen.refused, 0,
                    "every delivery passed its producer's CRC"
                );

                // A subscription that asks from where the window has let go: its reply says where
                // it starts, the window's floor, so it knows exactly which offsets it missed, and
                // it takes the window from there, in order.
                assert!(floor > 0, "the window let the stream's first batches go");
                let taken = bus.seen.order.len();
                let now = bus.subscriber.clock().unwrap();
                bus.subscribing
                    .subscribe(
                        bus.subscriber.endpoint(),
                        now,
                        STREAM,
                        wire::From::Offset(0),
                        focal_bus::limits::SERVE_PERIOD,
                    )
                    .unwrap();
                let want = usize::try_from(next - floor).unwrap();
                while bus.seen.replies.len() < 2 || bus.seen.order.len() < taken + want {
                    bus.step(true).await;
                    bus.moved("the late subscription takes the window");
                }
                let reply = bus.seen.replies[1];
                assert_eq!((reply.floor, reply.next, reply.start), (floor, next, floor));
                for (at, item) in bus.seen.order[taken..].iter().enumerate() {
                    let at = floor + at as u64;
                    assert_eq!(
                        *item,
                        Item::Batch {
                            base: at,
                            end: at + 1,
                            sequence: at
                        },
                        "the window from its floor, in order"
                    );
                }
                format!(
                    "{IN_ORDER} delivered in order; stalled, the publisher was held at {held_at} \
                     batches, then all {end} delivered in order, none missed (window floor {floor})"
                )
            }
        })
        .unwrap();
    eprintln!("{report}");
    drop(runtime);
    assert_eq!(node_budget.stats().used, 0, "the node gave every byte back");
}

/// A node with no memory for a delivery's frame keeps its subscriber. The window gives up its
/// oldest batches for the frame, the subscriber told what it missed; with nothing left to give, the
/// subscriber waits and is served once the transport gives memory back. Taken as the subscriber's
/// going, the refusal let the subscription go in silence (the pubsub matrix of 2026-10-16 lost 1.1
/// million messages so at 100,000 a second).
#[test]
fn a_delivery_the_node_has_no_memory_for_waits_and_its_subscriber_stays() {
    /// Batches published, the node's memory refusing deliveries partway.
    const PUBLISHED: u32 = 100;
    /// Batches delivered before the node's memory refuses.
    const BEFORE: u64 = 20;
    /// Delivery frames refused in a row: the first met by the window's relief, the rest by waiting.
    const REFUSED: u32 = 3;
    let pki = Pki::new();
    let node_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let client_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let segment = usize::try_from(wire::BATCH_BOUND).unwrap();
    let part_budget = node_budget.child(PART_SEGMENTS * segment, 0).unwrap();
    let config = NodeConfig {
        segment_bytes: segment,
        streams: 64,
        subscriptions: 16,
        producers: 64,
    };
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let mut bus =
                Harness::connect(&pki, &node_budget, &client_budget, &part_budget, config).await;
            // The first batches published and delivered whole: what the node sends after this, it
            // reserves after the refusals are set.
            let before = u32::try_from(BEFORE).unwrap();
            while bus.seen.reached() < BEFORE {
                bus.publish(before);
                bus.step(true).await;
                bus.moved("delivering before the refusals");
            }
            bus.refusing.deliver.set(REFUSED);
            while bus.seen.reached() < u64::from(PUBLISHED) {
                bus.publish(PUBLISHED.min(u32::try_from(bus.seen.reached()).unwrap() + KEEPS_UP));
                bus.step(true).await;
                bus.moved("delivering through the refusals");
            }
            let stats = bus.node.stats();
            assert_eq!(bus.refusing.deliver.get(), 0, "every refusal was met");
            assert_eq!(stats.dropped, 0, "no subscription was let go");
            assert!(stats.starved > 0, "a delivery waited for memory: {stats:?}");
            // Each item goes on where the last ended: a batch delivered, or offsets told missed.
            let mut at = 0;
            for item in &bus.seen.order {
                match *item {
                    Item::Batch { base, end, .. } => {
                        assert_eq!(base, at, "in order: {:?}", bus.seen.order);
                        at = end;
                    }
                    Item::Gap { from, to } => {
                        assert_eq!(from, at, "a gap told where it began: {:?}", bus.seen.order);
                        at = to;
                    }
                }
            }
            assert_eq!(at, u64::from(PUBLISHED));
        })
        .unwrap();
}

/// A frame the transport waits to take while the node's memory could hold it is given nothing
/// from the window: refused for something memory cannot meet, or its lane held by the node (its
/// stream's subscribers taking no more), it would take every batch the window retains, those its
/// subscribers are owed among them, and wait on all the same. Given the window whatever it waited
/// for, a held publisher's frame let the whole window go while the node held 16 MB of its 1.5 GiB,
/// and the subscriber it was held for was told a gap (the bare-metal pubsub run of 2026-10-16).
#[test]
fn a_frame_that_waits_for_anything_but_memory_takes_nothing_from_the_window() {
    /// Batches published and delivered before the frame waits: the window retains them all.
    const BEFORE: u32 = 50;
    let pki = Pki::new();
    let node_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let client_budget = MemoryBudget::new(256 << 20, 0).unwrap();
    let segment = usize::try_from(wire::BATCH_BOUND).unwrap();
    let part_budget = node_budget.child(PART_SEGMENTS * segment, 0).unwrap();
    let config = NodeConfig {
        segment_bytes: segment,
        streams: 64,
        subscriptions: 16,
        producers: 64,
    };
    let mut runtime = LocalRuntime::new(&runtime()).unwrap();
    runtime
        .block_on(async move {
            let mut bus =
                Harness::connect(&pki, &node_budget, &client_budget, &part_budget, config).await;
            while bus.seen.reached() < u64::from(BEFORE) {
                bus.publish(BEFORE);
                bus.step(true).await;
                bus.moved("delivering before the frame waits");
            }
            let window = (0, u64::from(BEFORE));
            assert_eq!(bus.bounds(), window, "the window retains every batch");

            // The node's transport is refused the next publish frame's bytes, its memory as free
            // as ever: the frame waits, its lane unread, and the node is asked to relieve it at
            // every turn.
            bus.refusing.publish.set(true);
            bus.publish(BEFORE + 1);
            while bus.node_driver.endpoint().frames_reserving() == 0 {
                bus.step(true).await;
                bus.moved("the publish frame comes to wait");
            }
            bus.node.relieve_transport(bus.node_driver.endpoint());
            assert_eq!(
                bus.bounds(),
                window,
                "a refusal memory cannot meet took the window"
            );

            // The node holds the publisher's lane, as it does once its stream's subscribers take no
            // more, and the refusal ends: the frame waits for its lane now.
            bus.node_driver
                .endpoint()
                .hold_lane(PUBLISHER, wire::LANE)
                .unwrap();
            bus.refusing.publish.set(false);
            assert_eq!(
                bus.node_driver.endpoint().frames_reserving(),
                0,
                "a held lane's frame waits for its lane, not for memory"
            );
            bus.node.relieve_transport(bus.node_driver.endpoint());
            assert_eq!(bus.bounds(), window, "a held frame took the window");
            assert_eq!(
                bus.node.stats().published,
                u64::from(BEFORE),
                "a held lane is not read"
            );

            // Let go, the lane is read on: its frame is taken, appended and delivered, and the
            // subscriber has every batch, in order, none missed.
            bus.node_driver
                .endpoint()
                .release_lane(PUBLISHER, wire::LANE)
                .unwrap();
            while bus.seen.reached() < u64::from(BEFORE + 1) {
                bus.step(true).await;
                bus.moved("the held frame delivered");
            }
            for (at, item) in bus.seen.order.iter().enumerate() {
                let at = at as u64;
                assert_eq!(
                    *item,
                    Item::Batch {
                        base: at,
                        end: at + 1,
                        sequence: at
                    },
                    "every batch, in order, none missed"
                );
            }
            assert_eq!(bus.bounds(), (0, u64::from(BEFORE + 1)));
            assert_eq!(bus.node.stats().gaps, 0);
        })
        .unwrap();
}
