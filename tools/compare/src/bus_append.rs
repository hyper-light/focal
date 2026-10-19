//! The focal event bus's side of `append` (doc 34, steps 3b.5 and 3c): a part replicated by a group
//! of nodes, and a publisher on the open-loop schedule, each record timed from its intended send to
//! its batch's acknowledgement: persisted once a majority's disks hold it, or, where the group states
//! its fates, replicated once a majority holds it across them (doc 33 §4.4a): the durable replicated append
//! the other systems are held to (Kafka `acks=all` with `flush.messages=1`, JetStream `sync: always`,
//! Redis `appendfsync always` with `WAITAOF`).
//!
//! The publisher dials the first node and follows each redirect to the leader a node names, taking
//! a leader only where its term is past what it knew (KIP-951's rule); a batch redirected, refused
//! out of its session's order, not acknowledged when the leader's connection ends, or unanswered for a
//! serve period, is sent again to the leader it then knows, and the part recognises a batch it took.
//! It publishes on a session of its own, begun at sequence zero (Kafka's producer takes a fresh
//! producer id), and sends in its session's order: a batch waiting to go again holds every batch
//! after it, which the part would refuse past the gap (doc 34 P6). It gathers the records due each
//! turn into one batch, as the
//! other systems' clients gather what is published between two of their writes: at a low rate a
//! batch of one, more as more fall due at once. A record due while the records in flight are at
//! `inflight` is counted refused by the generator, never queued in it, as the other systems' are.
//! At a rate of zero there is no schedule: each turn sends as many records as the window has room
//! for, each timed from its send (a closed loop, the other systems' too).
//! Every bound is the bus's own (`focal_bus::limits`).

use crate::{CompareError, HIGHEST_NS, Tally, schedule};
use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::BusBudget;
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::{IO, SERVE_PERIOD};
use focal_bus::pki;
use focal_bus::wire::{self, Ack, Bus, Durability, Role};
use focal_memory::MemoryBudget;
use hdrhistogram::Histogram;
use hyper_rt::runtime::LocalRuntime;
use hyper_rt_transport::Driver;
use hyper_transport::tls::{CertificateDer, Credentials, PrivateKeyDer, PrivatePkcs8KeyDer, Tls};
use hyper_transport::{Config, Endpoint, Event, Refusal};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

type Drv = Driver<Bus, BusBudget, BusDirectory>;

/// Format: the stream every run publishes to.
const STREAM: u64 = 1;
/// Format: the publisher's client identity.
const PUBLISHER: usize = 0;
/// Cited: Kafka's client's first backoff after a retriable error, `retry.backoff.ms` (100 ms).
const RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Cited: the most it backs off, `retry.backoff.max.ms` (1,000 ms, KIP-580).
const RETRY_BACKOFF_MAX: Duration = Duration::from_millis(1_000);

/// One `append` run's terms on the bus.
pub struct Run {
    /// Each node's address, node 1 first.
    pub nodes: Vec<SocketAddr>,
    pub pki: PathBuf,
    /// Records a second on the schedule; zero, a closed loop.
    pub rate: u32,
    pub warmup: Duration,
    pub total: Duration,
    pub payload: usize,
    pub inflight: usize,
}

fn fail(what: impl std::fmt::Display) -> CompareError {
    CompareError::Target(format!("bus: {what}"))
}

/// A batch in flight: its bytes, kept to send again, and each record's intended send and whether
/// it is measured.
struct Flying {
    bytes: Vec<u8>,
    records: Vec<(Instant, bool)>,
}

/// A session of the publisher's own: sixty-four random bits from the operating system, so no other
/// run of it names the same one (doc 34 P6); never zero.
fn session() -> Result<u64, CompareError> {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).map_err(|error| fail(format!("the session's bits: {error}")))?;
    Ok(u64::from_le_bytes(bytes).max(1))
}

/// Runs the schedule; each acknowledged record's latency in the histogram.
pub fn run(run: Run) -> Result<(Histogram<u64>, Tally), CompareError> {
    let nodes = run.nodes.len();
    let local = pki::load_or_make(&run.pki, nodes, 2).map_err(fail)?;
    let envelope = focal_platform::memory_envelope()
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| fail("the machine's memory could not be read"))?;
    let budget = MemoryBudget::new(envelope, 0).map_err(|error| fail(format!("{error:?}")))?;
    let identity = local
        .clients
        .get(PUBLISHER)
        .ok_or_else(|| fail("no client identity"))?;
    let mut directory = BusDirectory::new(nodes);
    for n in 1..=nodes {
        let n = u64::try_from(n).map_err(fail)?;
        let node = local.node(n).ok_or_else(|| fail("a node's identity"))?;
        directory
            .enroll(&node.certificate, n, Role::Node, &pki::node_name(n))
            .map_err(fail)?;
    }
    let tls = Tls::mutual(
        &Credentials {
            chain: vec![CertificateDer::from(identity.certificate.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key.clone())),
            roots: vec![CertificateDer::from(local.root.clone())],
        },
        false,
    )
    .map_err(|error| fail(format!("{error:?}")))?;
    let endpoint = Endpoint::new(
        Config {
            tls,
            role: Role::Client,
            limits: focal_bus::limits::client_of(nodes).map_err(fail)?,
            congestion: Default::default(),
        },
        BusBudget::new(budget),
        directory,
        Instant::now(),
    )
    .map_err(|error| fail(format!("{error:?}")))?;
    let producer = session()?;
    let mut runtime = LocalRuntime::new(&focal_rt::local_config()).map_err(fail)?;
    runtime
        .block_on(async move { publish(run, endpoint, producer).await })
        .map_err(fail)?
}

async fn publish(
    run: Run,
    endpoint: Endpoint<Bus, BusBudget, BusDirectory>,
    producer: u64,
) -> Result<(Histogram<u64>, Tally), CompareError> {
    let any: SocketAddr = "0.0.0.0:0".parse().map_err(fail)?;
    let mut driver: Drv = Driver::bind(endpoint, any, IO).map_err(fail)?;
    let address = |node: u64| -> Result<SocketAddr, CompareError> {
        usize::try_from(node)
            .ok()
            .and_then(|n| n.checked_sub(1))
            .and_then(|at| run.nodes.get(at))
            .copied()
            .ok_or_else(|| fail("a node the client does not know"))
    };
    let mut leader = 1_u64;
    let mut term = 0_u64;
    let now = driver.clock().map_err(fail)?;
    driver
        .endpoint()
        .connect(now, leader, address(leader)?)
        .map_err(|error| fail(format!("{error:?}")))?;
    let mut histogram = Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
        .map_err(|error| fail(error.to_string()))?;
    let mut tally = Tally::default();
    // One record due each `interval`; none (a rate of zero), a closed loop.
    let interval = match run.rate {
        0 => None,
        rate => Some(
            Duration::from_secs(1)
                .checked_div(rate)
                .filter(|interval| !interval.is_zero())
                .ok_or_else(|| fail("rate"))?,
        ),
    };
    let payload = vec![0x5a_u8; run.payload];
    let batch_bound = usize::try_from(wire::BATCH_BOUND).map_err(fail)?;
    let mut builder = BatchBuilder::new();
    // Every batch made and not acknowledged, by its first record's sequence; and those to send, again
    // or for the first time, to the leader the client now knows, in their session's order.
    let mut flying: BTreeMap<u32, Flying> = BTreeMap::new();
    let mut pending: BTreeSet<u32> = BTreeSet::new();
    // A redirect that names no leader (an election under way) holds what goes again until a node
    // names one or the backoff passes, as Kafka's client backs off a retriable error: from
    // `retry.backoff.ms`, 100 ms, doubling to `retry.backoff.max.ms`, 1,000 ms (KIP-580's defaults).
    let mut held_until: Option<Instant> = None;
    let mut backoff = RETRY_BACKOFF;
    let mut in_flight = 0_usize;
    let mut sequence = 0_u32;
    let began = Instant::now();
    let mut next_due = Duration::ZERO;
    let mut closed: Option<Instant> = None;
    let mut last_answer = began;
    loop {
        let open = match interval {
            Some(_) => next_due < run.total,
            None => began.elapsed() < run.total,
        };
        if !open && closed.is_none() {
            closed = Some(Instant::now());
        }
        if !open && flying.is_empty() {
            break;
        }
        // A run that heard no acknowledgement for a whole serve period, the transport's own
        // judgement of a silent peer, ends: what is still in flight failed. Before its end, all of
        // it goes again, once a period.
        if last_answer.elapsed() > SERVE_PERIOD && closed.is_none() {
            pending.extend(flying.keys().copied());
            last_answer = Instant::now();
        }
        if last_answer.elapsed() > SERVE_PERIOD && closed.is_some() {
            for batch in std::mem::take(&mut flying).into_values() {
                for (_, measured) in batch.records {
                    if measured {
                        tally.failed = tally.failed.saturating_add(1);
                        tally.first_error.get_or_insert_with(|| {
                            "no acknowledgement for a serve period".to_owned()
                        });
                    }
                }
            }
            break;
        }
        // The records this turn sends, gathered into one batch: on the schedule those due by now
        // while the window has room, and once it is full every one due counted refused at once; in a
        // closed loop as many as the window has room for, each sent now. A record the batch has no
        // room for waits for the next turn's.
        let mut records: Vec<(Instant, bool)> = Vec::new();
        let first = sequence;
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(began);
        let room = run.inflight.saturating_sub(in_flight);
        // Bounded: each pass takes a record, or ends the turn.
        for taken in 0..=room {
            if !open || (interval.is_some() && (next_due > elapsed || next_due >= run.total)) {
                break;
            }
            if taken == room {
                if let Some(interval) = interval {
                    let skipped =
                        schedule::overdue(next_due, elapsed, interval, run.warmup, run.total)?;
                    tally.refused = tally.refused.saturating_add(skipped.measured);
                    next_due = skipped.next;
                }
                break;
            }
            if records.is_empty() {
                builder.begin(&Producing {
                    stream: STREAM,
                    producer,
                    sequence: first,
                    flags: 0,
                    timestamp: 0,
                });
            }
            if builder
                .len()
                .saturating_add(BatchBuilder::record_bytes(payload.len()))
                > batch_bound
                || builder.count() == u16::MAX
            {
                break;
            }
            let (intended, measured) = match interval {
                None => (now, elapsed >= run.warmup),
                Some(interval) => {
                    let intended = began
                        .checked_add(next_due)
                        .ok_or_else(|| fail("schedule"))?;
                    let measured = next_due >= run.warmup;
                    next_due = next_due
                        .checked_add(interval)
                        .ok_or_else(|| fail("schedule"))?;
                    (intended, measured)
                }
            };
            builder.push(0, &payload).map_err(fail)?;
            records.push((intended, measured));
        }
        if !records.is_empty() {
            let bytes = builder.finish().map_err(fail)?.to_vec();
            let count = u32::try_from(records.len()).map_err(fail)?;
            sequence = sequence
                .checked_add(count)
                .ok_or_else(|| fail("sequences"))?;
            tally.sent = tally.sent.saturating_add(u64::from(count));
            in_flight = in_flight.saturating_add(records.len());
            flying.insert(first, Flying { bytes, records });
            pending.insert(first);
        }
        // What waits goes to the leader known now, in order, once no backoff holds it: a lane with
        // no room, or a connection not made yet, keeps it and everything after it for the next turn.
        let client = Client::new(leader);
        let backing_off = held_until.is_some_and(|until| Instant::now() < until);
        if !backing_off {
            // Bounded: each turn takes one batch out of `pending`, or stops.
            for _ in 0..pending.len() {
                let Some(&first) = pending.first() else {
                    break;
                };
                let Some(batch) = flying.get(&first) else {
                    pending.remove(&first);
                    continue;
                };
                match client.publish(driver.endpoint(), &batch.bytes) {
                    Ok(()) => {
                        pending.remove(&first);
                    }
                    Err(Refusal::LaneFull | Refusal::NotConnected) => break,
                    Err(error) => return Err(fail(format!("publish refused: {error:?}"))),
                }
            }
        }
        // The connection's events until the next record is due, a tick at most; in a closed loop,
        // none while the window has room.
        let wait_ns = match interval {
            Some(_) => began
                .checked_add(next_due)
                .map(|due| due.saturating_duration_since(Instant::now()))
                .unwrap_or_default(),
            None if open && in_flight < run.inflight => Duration::ZERO,
            None => Duration::MAX,
        }
        .min(Duration::from_millis(1));
        let mut tick = pin!(hyper_rt::futures::sleep(
            u64::try_from(wait_ns.as_nanos()).unwrap_or(1).max(1)
        ));
        let event = poll_fn(|context| {
            if let Poll::Ready(event) = driver.poll_event(context) {
                return Poll::Ready(Some(event));
            }
            if tick.as_mut().poll(context).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await;
        let mut events: Vec<Event<Bus>> = Vec::new();
        if let Some(event) = event {
            events.push(event.map_err(fail)?);
        }
        while let Some(event) = driver.endpoint().poll_event() {
            events.push(event);
        }
        for event in events {
            let mut client = Client::new(leader);
            let mut acked: Vec<Ack> = Vec::new();
            let mut redirected: Vec<wire::Redirect> = Vec::new();
            let mut lost = false;
            client.on_event(driver.endpoint(), event, |delivery| match delivery {
                Delivery::Acked(ack) => acked.push(ack),
                Delivery::Redirected(redirect) => redirected.push(redirect),
                Delivery::Lost(_) => lost = true,
                _ => {}
            });
            let now = Instant::now();
            for ack in acked {
                if ack.class == Ack::OUT_OF_ORDER {
                    // Past a gap in the session: everything from the sequence the part waits for
                    // goes again, in order. A sequence below every batch in flight was acknowledged:
                    // the part no longer holds the session as it was.
                    let lowest = flying.keys().next().map(|first| u64::from(*first));
                    if lowest.is_some_and(|lowest| ack.base < lowest) {
                        return Err(fail(format!(
                            "the part waits for sequence {} of a session acknowledged past it",
                            ack.base
                        )));
                    }
                    let from = u32::try_from(ack.base).map_err(fail)?;
                    pending.extend(flying.range(from..).map(|(first, _)| *first));
                    continue;
                }
                pending.remove(&ack.sequence);
                let Some(batch) = flying.remove(&ack.sequence) else {
                    continue;
                };
                last_answer = now;
                in_flight = in_flight.saturating_sub(batch.records.len());
                // Replicated (a majority across the group's fates, followers in memory under
                // their durable promises) or persisted (a majority's disks) is a durable replicated
                // append; anything else (a refusal) failed.
                let class = Durability::of(ack.class);
                let durable = matches!(class, Some(Durability::Replicated | Durability::Persisted));
                for (intended, measured) in batch.records {
                    if durable {
                        tally.produced = tally.produced.saturating_add(1);
                        match class {
                            Some(Durability::Replicated) => {
                                tally.replicated = tally.replicated.saturating_add(1);
                            }
                            _ => tally.persisted = tally.persisted.saturating_add(1),
                        }
                    }
                    if measured {
                        let result = if durable {
                            Ok(())
                        } else {
                            Err(format!("class {}", ack.class))
                        };
                        tally.record(
                            &mut histogram,
                            now.saturating_duration_since(intended),
                            result,
                        )?;
                    }
                }
            }
            for redirect in redirected {
                match redirect.leader.node {
                    Some(node) if redirect.leader.term > term => {
                        leader = node;
                        term = redirect.leader.term;
                        let _ = driver.endpoint().connect(now, leader, address(leader)?);
                        // A leader named: what waits goes at once.
                        held_until = None;
                        backoff = RETRY_BACKOFF;
                    }
                    Some(_) => {}
                    None => {
                        if held_until.is_none_or(|until| now >= until) {
                            held_until = now.checked_add(backoff);
                            backoff = backoff.saturating_mul(2).min(RETRY_BACKOFF_MAX);
                        }
                    }
                }
                if flying.contains_key(&redirect.sequence) {
                    pending.insert(redirect.sequence);
                }
            }
            if lost {
                // The leader's connection ended: what it did not acknowledge goes again, to the
                // next node, which redirects to the leader it knows.
                let count = u64::try_from(run.nodes.len()).unwrap_or(1).max(1);
                leader = leader.checked_rem(count).unwrap_or(0).saturating_add(1);
                let _ = driver.endpoint().connect(now, leader, address(leader)?);
                pending.extend(flying.keys().copied());
            }
        }
    }
    tally.drain = closed.map_or(Duration::ZERO, |closed| {
        last_answer.saturating_duration_since(closed)
    });
    Ok((histogram, tally))
}
