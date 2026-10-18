//! The focal event bus's side of `pubsub`: a publisher and a subscriber, each one of the node's
//! client identities (its local directory, `focal start bus --pki`), each driven on a hyper-rt
//! shard of a thread of its own, as two applications would run them, the generator's two CPUs
//! theirs as tokio's two workers are the other clients'. The schedule, the stamp and the histogram
//! are the other systems' (`pubsub`): a batch of one record a message, the record the payload,
//! stamped as it is published and timed from then to its delivery. Every bound is the bus's own
//! (`focal_bus::limits`).
//!
//! The two threads tell each other, by one-shots that wake the shard waiting on them, when the
//! subscription stands (its instant is the run's start, the schedule's zero), how many messages the
//! schedule published once it closed, and when the subscriber is done; the publisher drives its
//! connection until then, so what its lane still holds when the schedule closes is sent. Nothing is
//! waited for on a clock of the harness's: setup ends when the node answers or the transport says
//! it cannot be reached, and a thread that fails drops its one-shot, which tells the other. The
//! publisher keeps the schedule as the other systems' does (`pubsub::SPIN`): it sleeps to the
//! spin's width before a message is due and spins the rest, taking its connection's events between
//! looks.
use crate::deliver::stamp;
use crate::pubsub::{Lag, Path, Received, SPIN};
use crate::{CompareError, Tally};
use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::BusBudget;
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::{IDLE_TIMEOUT, IO, SERVE_PERIOD};
use focal_bus::pki;
use focal_bus::wire::{self, Bus, Role};
use focal_memory::MemoryBudget;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::{OneshotReceiver, OneshotSender, SyncError, oneshot};
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, Refusal};
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::{Duration, Instant};

type Drv = Driver<Bus, BusBudget, BusDirectory>;

/// Format: the node's peer in every client's directory.
const NODE: u64 = 1;
/// Format: the stream every run publishes to.
const STREAM: u64 = 1;
/// Format: the client identities a run uses, the publisher's (the first) and the subscriber's.
const CLIENTS: usize = 2;
/// Format: the publisher's identity, and the producer its batches name.
const PUBLISHER: usize = 0;
/// Format: the subscriber's identity.
const SUBSCRIBER: usize = 1;

/// One `pubsub` run's terms on the bus.
pub struct Run {
    pub node: SocketAddr,
    pub pki: PathBuf,
    pub rate: u32,
    pub warmup: Duration,
    pub total: Duration,
    pub payloads: Vec<Vec<u8>>,
}

fn fail(what: impl std::fmt::Display) -> CompareError {
    CompareError::Target(format!("bus: {what}"))
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Client `index`'s endpoint, its bytes charged to `budget`.
fn endpoint(
    local: &pki::Local,
    index: usize,
    budget: &MemoryBudget,
) -> Result<Endpoint<Bus, BusBudget, BusDirectory>, CompareError> {
    let identity = local
        .clients
        .get(index)
        .ok_or_else(|| fail("no such client identity"))?;
    let mut directory = BusDirectory::new(1);
    directory
        .enroll(
            &local
                .node(NODE)
                .ok_or_else(|| fail("no node identity"))?
                .certificate,
            NODE,
            Role::Node,
            &pki::node_name(NODE),
        )
        .map_err(fail)?;
    let credentials = Credentials {
        chain: vec![CertificateDer::from(identity.certificate.clone())],
        key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key.clone())),
        roots: vec![CertificateDer::from(local.root.clone())],
    };
    let tls = Tls::mutual(&credentials, false).map_err(|error| fail(format!("{error:?}")))?;
    Endpoint::new(
        Config {
            tls,
            role: Role::Client,
            limits: focal_bus::limits::client().map_err(fail)?,
            congestion: Default::default(),
        },
        BusBudget::new(budget.clone()),
        directory,
        Instant::now(),
    )
    .map_err(|error| fail(format!("{error:?}")))
}

/// `endpoint`'s driver, bound on the current shard and dialing the node.
fn dial(
    endpoint: Endpoint<Bus, BusBudget, BusDirectory>,
    node: SocketAddr,
) -> Result<Drv, CompareError> {
    let any: SocketAddr = "0.0.0.0:0".parse().map_err(|_| fail("an address"))?;
    let mut driver: Drv = Driver::bind(endpoint, any, IO).map_err(fail)?;
    let now = driver.clock().map_err(fail)?;
    driver
        .endpoint()
        .connect(now, NODE, node)
        .map_err(|error| fail(format!("{error:?}")))?;
    Ok(driver)
}

/// What woke a client's thread.
enum Woke<T> {
    /// The driver's event.
    Event(Event<Bus>),
    /// The other thread's word, or its going without one.
    Told(Result<T, SyncError<()>>),
    /// The deadline passed, or, spinning, the shard was yielded to once.
    Due,
}

/// When a wait ends of itself.
#[derive(Clone, Copy)]
enum Until {
    /// After one yield to the shard, which reads its inputs (the sockets among them) first: a spin.
    Yield,
    /// After this many nanoseconds.
    Nanos(u64),
    /// Only on an event or the other thread's word: the transport bounds the one (its idle
    /// timeout ends a silent connection) and the other thread's own bounded run the other.
    Told,
}

/// Waits for the driver's next event, for `told` if it is still to come, or until `until`.
async fn wait<T>(
    driver: &mut Drv,
    told: Option<&mut OneshotReceiver<T>>,
    until: Until,
) -> Result<Woke<T>, CompareError> {
    let mut told = told;
    let mut timer = pin!(match until {
        Until::Nanos(ns) => Some(hyper_rt::futures::sleep(ns.max(1))),
        Until::Yield | Until::Told => None,
    });
    let mut yielded = false;
    poll_fn(|context| {
        if let Poll::Ready(event) = driver.poll_event(context) {
            return Poll::Ready(event.map(Woke::Event));
        }
        if let Some(told) = told.as_deref_mut()
            && let Poll::Ready(word) = Pin::new(told).poll(context)
        {
            return Poll::Ready(Ok(Woke::Told(word)));
        }
        match until {
            Until::Yield if yielded => return Poll::Ready(Ok(Woke::Due)),
            Until::Yield => {
                yielded = true;
                context.waker().wake_by_ref();
            }
            Until::Nanos(_) => {
                if let Some(timer) = timer.as_mut().as_pin_mut()
                    && timer.poll(context).is_ready()
                {
                    return Poll::Ready(Ok(Woke::Due));
                }
            }
            Until::Told => {}
        }
        Poll::Pending
    })
    .await
    .map_err(fail)
}

/// Runs the schedule and returns what the publisher sent, what the subscriber received, and how
/// far the publishes fell behind the schedule.
pub fn run(run: Run) -> Result<(Tally, Received, Lag), CompareError> {
    let Run {
        node,
        pki: directory,
        rate,
        warmup,
        total,
        payloads,
    } = run;
    let local = pki::load_or_make(&directory, 1, CLIENTS).map_err(fail)?;
    // Both clients' endpoints are charged within this process's envelope, as a node's are; a machine
    // whose memory cannot be read is refused rather than guessed at.
    let envelope = focal_platform::memory_envelope()
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| fail("the machine's memory could not be read"))?;
    let budget = MemoryBudget::new(envelope, 0).map_err(|error| fail(format!("{error:?}")))?;
    let runtime = focal_rt::local_config();
    let (started, start) = oneshot::<Instant>().map_err(fail)?;
    let (close, closed) = oneshot::<u64>().map_err(fail)?;
    let (finish, finished) = oneshot::<()>().map_err(fail)?;
    let schedule = Schedule {
        rate,
        warmup,
        total,
        payloads,
    };
    std::thread::scope(|scope| {
        let local = &local;
        let budget = &budget;
        let (subscribing_runtime, publishing_runtime) = (runtime.clone(), runtime);
        let subscribing = scope.spawn(move || {
            subscriber(
                subscribing_runtime,
                local,
                budget,
                node,
                warmup,
                Told {
                    started,
                    closed,
                    finish,
                },
            )
        });
        let publishing = scope.spawn(move || {
            publisher(
                publishing_runtime,
                local,
                budget,
                node,
                schedule,
                Tells {
                    start,
                    close,
                    finished,
                },
            )
        });
        let received = subscribing
            .join()
            .map_err(|_| fail("the subscriber's thread unwound"))?;
        let published = publishing
            .join()
            .map_err(|_| fail("the publisher's thread unwound"))?;
        let (tally, lag) = published?;
        Ok((tally, received?, lag))
    })
}

/// The subscriber's side of the one-shots: it says when the subscription stood and when it is
/// done, and is told how many messages the schedule published.
struct Told {
    started: OneshotSender<Instant>,
    closed: OneshotReceiver<u64>,
    finish: OneshotSender<()>,
}

/// The publisher's side.
struct Tells {
    start: OneshotReceiver<Instant>,
    close: OneshotSender<u64>,
    finished: OneshotReceiver<()>,
}

/// The subscriber's thread: subscribes from the stream's tail, says when the subscription stands,
/// and takes every delivery until all the schedule published arrived. The node keeps every batch
/// for a live subscriber (its lane is lossless), so the drain waits for all of them, and gives up
/// only as the transport gives up on a silent peer, once nothing has arrived for its idle timeout:
/// what the node refused (its memory full) never comes.
fn subscriber(
    runtime: RuntimeConfig,
    local: &pki::Local,
    budget: &MemoryBudget,
    node: SocketAddr,
    warmup: Duration,
    told: Told,
) -> Result<Received, CompareError> {
    let endpoint = endpoint(local, SUBSCRIBER, budget)?;
    let Told {
        started,
        mut closed,
        finish,
    } = told;
    let mut runtime = LocalRuntime::new(&runtime).map_err(fail)?;
    runtime
        .block_on(async move {
            let mut driver = dial(endpoint, node)?;
            let mut client = Client::new(NODE);
            let mut received = Received::new()?;
            let mut started = Some(started);
            let mut asked = false;
            let mut start: Option<Instant> = None;
            let mut close: Option<(Instant, u64)> = None;
            let mut last = 0_u64;
            let mut heard = Instant::now();
            loop {
                if let Some((at, published)) = close
                    && (received.received >= published || heard.elapsed() >= IDLE_TIMEOUT)
                {
                    received.finish(at);
                    received.path = driver
                        .endpoint()
                        .connection_stats(NODE)
                        .map(|stats| Path::of(&stats));
                    let _ = finish.send(());
                    return Ok(received);
                }
                let woke = match close {
                    Some(_) => {
                        let silent = IDLE_TIMEOUT.saturating_sub(heard.elapsed());
                        wait::<u64>(&mut driver, None, Until::Nanos(nanos(silent))).await?
                    }
                    None => wait(&mut driver, Some(&mut closed), Until::Told).await?,
                };
                let event = match woke {
                    Woke::Event(event) => event,
                    Woke::Told(Ok(published)) => {
                        close = Some((Instant::now(), published));
                        continue;
                    }
                    Woke::Told(Err(_)) => {
                        return Err(fail("the publisher stopped before the schedule closed"));
                    }
                    Woke::Due => continue,
                };
                let mut outcome: Result<(), CompareError> = Ok(());
                let mut connected = false;
                let mut subscribed = false;
                let mut take = |event: Event<Bus>, driver: &mut Drv| {
                    if let Event::Connected { .. } = &event {
                        connected = true;
                    }
                    client.on_event(driver.endpoint(), event, |delivery| match delivery {
                        Delivery::Batch(batch) => {
                            let arrived = Instant::now();
                            for record in batch.records().flatten() {
                                if let Some(start) = start
                                    && let Err(error) = received.record(
                                        start,
                                        warmup,
                                        record.payload,
                                        arrived,
                                        &mut last,
                                    )
                                {
                                    outcome = Err(error);
                                }
                            }
                        }
                        Delivery::Subscribed { .. } => subscribed = true,
                        Delivery::Gap(gap) => {
                            received.missed_told = received
                                .missed_told
                                .saturating_add(gap.to.saturating_sub(gap.from));
                        }
                        Delivery::Refused(error) => {
                            received.corrupt = received.corrupt.saturating_add(1);
                            received
                                .first_error
                                .get_or_insert_with(|| error.to_string());
                        }
                        Delivery::Unsubscribed { refusal, .. } => {
                            outcome = Err(fail(format!("the subscribe was refused: {refusal:?}")));
                        }
                        Delivery::Lost(why) => {
                            outcome = Err(fail(format!("the node's connection ended: {why:?}")));
                        }
                        // The lone node this mode runs leads every stream and acknowledges
                        // nothing; a redirect or an acknowledgement here is a node of a group.
                        Delivery::SubscribeRedirected { leader, .. } => {
                            outcome =
                                Err(fail(format!("the subscribe was redirected to {leader:?}")));
                        }
                        Delivery::Redirected(redirect) => {
                            outcome = Err(fail(format!("a publish was redirected: {redirect:?}")));
                        }
                        Delivery::Acked(_) => {}
                    });
                };
                take(event, &mut driver);
                while let Some(event) = driver.endpoint().poll_event() {
                    take(event, &mut driver);
                }
                outcome?;
                heard = Instant::now();
                if connected && !asked {
                    asked = true;
                    let now = driver.clock().map_err(fail)?;
                    client
                        .subscribe(
                            driver.endpoint(),
                            now,
                            STREAM,
                            wire::From::Tail,
                            SERVE_PERIOD,
                        )
                        .map_err(|error| fail(format!("{error:?}")))?;
                }
                if subscribed && start.is_none() {
                    let now = Instant::now();
                    start = Some(now);
                    started
                        .take()
                        .ok_or_else(|| fail("the subscription stood twice"))?
                        .send(now)
                        .map_err(|_| fail("the publisher stopped before the subscription stood"))?;
                }
            }
        })
        .map_err(fail)?
}

/// Sends the open batch, if any, and counts its records by what the lane said: each queued, or
/// each failed; whether the batch left. A batch the lane has no room for stays, whole, to be sent
/// again once it has (the endpoint says `LaneRoom`): the generator refuses nothing on a full lane,
/// it waits, as the other systems' clients do.
fn send(
    builder: &mut BatchBuilder,
    held: &mut Vec<(u64, u64, bool)>,
    client: &Client,
    driver: &mut Drv,
    tally: &mut Tally,
    lag: &mut Lag,
) -> Result<bool, CompareError> {
    if held.is_empty() {
        return Ok(true);
    }
    let bytes = builder.finish().map_err(fail)?;
    let outcome = client.publish(driver.endpoint(), bytes);
    if outcome == Err(Refusal::LaneFull) {
        if tally.held_since.is_none() {
            tally.held_since = Some(Instant::now());
            tally.holds = tally.holds.saturating_add(1);
        }
        return Ok(false);
    }
    if let Some(since) = tally.held_since.take() {
        let held = nanos(since.elapsed());
        tally.held_ns = tally.held_ns.saturating_add(held);
        tally.held_most_ns = tally.held_most_ns.max(held);
    }
    for (intended_ns, published_ns, measured) in held.drain(..) {
        match &outcome {
            Ok(()) => {
                tally.sent = tally.sent.saturating_add(1);
                tally.produced = tally.produced.saturating_add(1);
                if measured {
                    lag.record(intended_ns, published_ns)?;
                }
            }
            Err(error) => {
                tally.failed = tally.failed.saturating_add(1);
                tally
                    .first_error
                    .get_or_insert_with(|| format!("{error:?}"));
            }
        }
    }
    Ok(true)
}

/// What the publisher's schedule offers.
struct Schedule {
    rate: u32,
    warmup: Duration,
    total: Duration,
    payloads: Vec<Vec<u8>>,
}

/// The publisher's thread: once the subscription stands, publishes each message as it falls due,
/// stamped as it is published; every message due is sent, or held while the lane has no room
/// (the generator queues nothing, the other systems' rule). It drives its connection until the
/// subscriber is done.
fn publisher(
    runtime: RuntimeConfig,
    local: &pki::Local,
    budget: &MemoryBudget,
    node: SocketAddr,
    schedule: Schedule,
    tells: Tells,
) -> Result<(Tally, Lag), CompareError> {
    let endpoint = endpoint(local, PUBLISHER, budget)?;
    let interval = Duration::from_secs(1)
        .checked_div(schedule.rate)
        .ok_or_else(|| fail("rate"))?;
    let Tells {
        mut start,
        close,
        mut finished,
    } = tells;
    let producer = pki::Local::client_peer_of(1, PUBLISHER).ok_or_else(|| fail("the producer"))?;
    let mut runtime = LocalRuntime::new(&runtime).map_err(fail)?;
    runtime
        .block_on(async move {
            let mut driver = dial(endpoint, node)?;
            let mut client = Client::new(NODE);
            let mut tally = Tally::default();
            let mut lag = Lag::new()?;
            let mut builder = BatchBuilder::new();
            // The batch's records, each (intended, published, measured), counted once the lane
            // says what became of the batch; at most a batch's records.
            let mut held: Vec<(u64, u64, bool)> = Vec::new();
            // The message being stamped, its bytes copied from the corpus.
            let mut scratch: Vec<u8> = Vec::new();
            // Messages numbered so far: each one's sequence in its stamp.
            let mut numbered = 0_u64;
            let mut close = Some(close);
            let batch_bound =
                usize::try_from(wire::BATCH_BOUND).map_err(|_| fail("batch bound"))?;
            let mut began: Option<Instant> = None;
            let mut next_due = Duration::ZERO;
            let mut closed_at: Option<Instant> = None;
            let spin_ns = nanos(SPIN);
            loop {
                // A turn publishes at most one batch, then the connection is driven: a batch the
                // lane had no room for goes first, and while it waits nothing more is taken from the
                // schedule (the publisher is held, as the other systems' clients hold theirs, and
                // the schedule's lag says so). Otherwise the messages due go as one batch, as the
                // other clients gather what is published between two of their writes: at a low rate
                // a batch of one, more as more fall due at once, none held for another.
                if let Some(began) = began {
                    let free = held.is_empty()
                        || send(
                            &mut builder,
                            &mut held,
                            &client,
                            &mut driver,
                            &mut tally,
                            &mut lag,
                        )?;
                    while free && closed_at.is_none() {
                        if !began
                            .checked_add(next_due)
                            .is_some_and(|due| due <= Instant::now())
                        {
                            break;
                        }
                        let intended = next_due;
                        // The schedule's time over, what it still holds was never offered.
                        if intended < schedule.total && began.elapsed() > schedule.total {
                            tally.behind =
                                crate::pubsub::behind(schedule.total, intended, interval);
                            closed_at = Some(Instant::now());
                            break;
                        }
                        if intended >= schedule.total {
                            closed_at = Some(Instant::now());
                            break;
                        }
                        let index = usize::try_from(numbered)
                            .unwrap_or(0)
                            .checked_rem(schedule.payloads.len())
                            .unwrap_or(0);
                        let corpus = schedule.payloads.get(index).map_or(&[][..], Vec::as_slice);
                        // A message the batch has no room for waits for the next turn's.
                        let full = builder
                            .len()
                            .saturating_add(BatchBuilder::record_bytes(corpus.len()))
                            > batch_bound
                            || builder.count() == u16::MAX;
                        if !held.is_empty() && full {
                            break;
                        }
                        next_due = next_due
                            .checked_add(interval)
                            .ok_or_else(|| fail("schedule"))?;
                        let measured = intended >= schedule.warmup;
                        scratch.clear();
                        scratch.extend_from_slice(corpus);
                        let intended_ns = nanos(intended);
                        let published_ns = nanos(began.elapsed());
                        numbered = numbered.saturating_add(1);
                        stamp(&mut scratch, published_ns, numbered)?;
                        if held.is_empty() {
                            builder.begin(&Producing {
                                stream: STREAM,
                                producer,
                                sequence: u32::try_from(numbered).unwrap_or(u32::MAX),
                                flags: 0,
                                timestamp: published_ns / 1_000,
                            });
                        }
                        builder.push(published_ns / 1_000, &scratch).map_err(fail)?;
                        held.push((intended_ns, published_ns, measured));
                    }
                    if free && !held.is_empty() {
                        send(
                            &mut builder,
                            &mut held,
                            &client,
                            &mut driver,
                            &mut tally,
                            &mut lag,
                        )?;
                    }
                    // The schedule closed and every batch it made has left: the subscriber is told
                    // how many to wait for.
                    if closed_at.is_some()
                        && held.is_empty()
                        && let Some(close) = close.take()
                    {
                        close.send(tally.produced).map_err(|_| {
                            fail("the subscriber stopped before the schedule closed")
                        })?;
                    }
                }
                // Before the subscription stands, the subscriber's word; while the schedule runs,
                // the next message's time (slept to the spin's width before it, spun after); once
                // the subscriber was told the total, its word that it is done.
                let event = match (began, close.is_some()) {
                    (None, _) => match wait(&mut driver, Some(&mut start), Until::Told).await? {
                        Woke::Told(Ok(at)) => {
                            began = Some(at);
                            continue;
                        }
                        Woke::Told(Err(_)) => {
                            return Err(fail("the subscriber stopped before subscribing"));
                        }
                        Woke::Event(event) => Some(event),
                        Woke::Due => None,
                    },
                    (Some(began), true) => {
                        let until = if closed_at.is_some() || !held.is_empty() {
                            // A batch waits for the lane's room, which the endpoint says
                            // (`LaneRoom`): the publisher is held until then, whatever the
                            // schedule says, as the other systems' clients are.
                            Until::Told
                        } else {
                            let due = began.checked_add(next_due).unwrap_or(began);
                            let left = nanos(due.saturating_duration_since(Instant::now()));
                            if left <= spin_ns {
                                Until::Yield
                            } else {
                                Until::Nanos(left.saturating_sub(spin_ns))
                            }
                        };
                        match wait::<()>(&mut driver, None, until).await? {
                            Woke::Event(event) => Some(event),
                            Woke::Told(_) | Woke::Due => None,
                        }
                    }
                    (Some(_), false) => {
                        match wait(&mut driver, Some(&mut finished), Until::Told).await? {
                            Woke::Told(_) => {
                                lag.path = driver
                                    .endpoint()
                                    .connection_stats(NODE)
                                    .map(|stats| Path::of(&stats));
                                return Ok((tally, lag));
                            }
                            Woke::Event(event) => Some(event),
                            Woke::Due => None,
                        }
                    }
                };
                if let Some(event) = event {
                    let mut lost = None;
                    let mut take = |delivery: Delivery<'_>| {
                        if let Delivery::Lost(why) = delivery {
                            lost = Some(why);
                        }
                    };
                    client.on_event(driver.endpoint(), event, &mut take);
                    while let Some(event) = driver.endpoint().poll_event() {
                        client.on_event(driver.endpoint(), event, &mut take);
                    }
                    if let Some(why) = lost {
                        return Err(fail(format!("the node's connection ended: {why:?}")));
                    }
                }
            }
        })
        .map_err(fail)?
}
