//! The `pubsub` mode: a publisher's message to a subscriber placed first, the bus's ephemeral class
//! against NATS core, Redis Pub/Sub and Kafka, one node each. NATS and Redis as shipped; Kafka as a
//! latency-minded user sets it: no linger before a send (Kafka's default before KIP-1030 made it
//! 5 ms), the consumer's fetch answered as soon as a byte is there (`fetch.min.bytes` 1, its
//! default), a topic of one partition on its one broker.
//!
//! A publisher sends on the open-loop schedule; a subscriber placed before the first publish takes
//! every message delivered. Each message is stamped as it is handed to its client (the stamp in its
//! first 16 bytes, on the generator's own clock, which publisher and subscriber share) and timed
//! from then to its arrival: what the client, the network and the node took. How far each publish
//! fell behind its intended time is measured apart ([`Lag`]), so a schedule the generator could not
//! keep is seen, and no generator timer is charged to a system. What never arrives is counted, not
//! timed: at most once loses messages under pressure by design, and the report says how many.
//!
//! Every system's clients run alike: the publisher on a thread of its own and the subscriber on
//! another, each with a runtime of its own (tokio's current-thread runtime here, a hyper-rt shard
//! for the bus, `bus.rs`), and the publisher keeps the schedule to the microsecond: it sleeps to
//! [`SPIN`] before a message is due and spins the rest, so no timer's granularity releases messages
//! in bursts (tokio's wheel rounds a deadline up to its next millisecond: fifty messages at once at
//! 50,000 a second). The threads tell each other by one-shots, each awaited, when the subscription
//! stands (the run's start), how many messages the schedule published once it closed, and when the
//! subscriber is done; a thread that fails drops its one-shot, which tells the other.
//!
//! Nothing is waited for on a clock of the harness's. A client's setup ends as its own client says,
//! and each subscriber's drain ends on a fact of its system's: NATS's subscriber flushes (a PING
//! the server answers after everything it wrote to the subscriber before it, the publisher's own
//! flush having made every publish reach the server first), and takes what had arrived by then;
//! Redis's takes until an empty message the publisher sends after every publish was answered,
//! which Redis delivers after them on the one connection, or until Redis disconnects it; Kafka's
//! takes every record Kafka acknowledged, none of which it loses.
use crate::deliver::{self, HEADER};
use crate::{CompareError, HIGHEST_NS, System, Tally};
use futures_util::{FutureExt, StreamExt};
use hdrhistogram::Histogram;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

/// The subject, channel or topic every run publishes on.
pub const SUBJECT: &str = "focal-compare-pubsub";
/// Derived: how long before a message is due its publisher stops sleeping and spins: two of
/// tokio's timer ticks (its wheel's millisecond), so the sleep's rounding never makes it late.
pub const SPIN: Duration = Duration::from_millis(2);

/// What a subscriber measured.
pub struct Received {
    /// Messages published after the warm-up, from their publish to their arrival.
    pub histogram: Histogram<u64>,
    /// Messages received, the warm-up's included.
    pub received: u64,
    /// Messages whose stamp could not be read.
    pub unreadable: u64,
    /// Messages received out of their publisher's order.
    pub reordered: u64,
    /// Messages the node told the subscriber it missed (the bus's gap notices).
    pub missed_told: u64,
    /// Deliveries refused for failing their producer's check, and the first one's reason.
    pub corrupt: u64,
    pub first_error: Option<String>,
    /// From the schedule's close to the subscriber's last arrival or the drain's end.
    pub drain: Duration,
    /// From the schedule's close to the last arrival (zero if everything arrived before it): the
    /// span a delivered rate is counted over, without a drain waited out for messages lost.
    pub tail: Duration,
    /// When the last message arrived.
    pub last_arrival: Option<Instant>,
    /// The subscriber's connection as its QUIC stack counted it (the bus's clients only).
    pub path: Option<Path>,
}

impl Received {
    pub fn new() -> Result<Self, CompareError> {
        Ok(Self {
            histogram: Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
                .map_err(|error| CompareError::Histogram(error.to_string()))?,
            received: 0,
            unreadable: 0,
            reordered: 0,
            missed_told: 0,
            corrupt: 0,
            first_error: None,
            drain: Duration::ZERO,
            tail: Duration::ZERO,
            last_arrival: None,
            path: None,
        })
    }

    /// One message arrived at `arrived`, stamped by a run that began at `start`.
    pub fn record(
        &mut self,
        start: Instant,
        warmup: Duration,
        payload: &[u8],
        arrived: Instant,
        last_sequence: &mut u64,
    ) -> Result<(), CompareError> {
        self.received = self.received.saturating_add(1);
        self.last_arrival = Some(arrived);
        let (Some(time), Some(sequence)) = (
            payload
                .first_chunk::<8>()
                .map(|time| u64::from_be_bytes(*time)),
            payload
                .get(8..HEADER)
                .and_then(|order| <[u8; 8]>::try_from(order).ok())
                .map(u64::from_be_bytes),
        ) else {
            self.unreadable = self.unreadable.saturating_add(1);
            return Ok(());
        };
        if sequence <= *last_sequence {
            self.reordered = self.reordered.saturating_add(1);
        }
        *last_sequence = sequence;
        let published = Duration::from_nanos(time);
        if published < warmup {
            return Ok(());
        }
        let latency = arrived
            .saturating_duration_since(start)
            .saturating_sub(published);
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.histogram
            .record(nanos.clamp(1_000, HIGHEST_NS))
            .map_err(|error| CompareError::Histogram(error.to_string()))
    }

    /// The subscriber is done, the schedule having closed at `closed_at`: the drain it waited
    /// and the tail its arrivals took.
    pub fn finish(&mut self, closed_at: Instant) {
        self.drain = closed_at.elapsed();
        self.tail = self.last_arrival.map_or(Duration::ZERO, |last| {
            last.saturating_duration_since(closed_at)
        });
    }
}

/// How far behind its intended time each measured message was handed to its client: the
/// generator's own schedule, apart from what the system took.
pub struct Lag {
    pub histogram: Histogram<u64>,
    /// The publisher's connection as its QUIC stack counted it (the bus's clients only).
    pub path: Option<Path>,
}

/// A bus client's connection to the node, as its QUIC stack counted it over the run: what a run's
/// tail is traced to (losses, the congestion window, a peer's window that blocked it).
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Path {
    pub rtt_us: f64,
    pub min_rtt_us: f64,
    pub cwnd: u64,
    pub congestion_events: u64,
    pub lost_packets: u64,
    pub sent_packets: u64,
    /// Times it told the node the node's connection window blocked it (DATA_BLOCKED sent).
    pub data_blocked: u64,
    /// Times a stream's window blocked it (STREAM_DATA_BLOCKED sent).
    pub stream_data_blocked: u64,
    /// Window updates the node sent it (MAX_DATA, MAX_STREAM_DATA received).
    pub max_data_rx: u64,
    pub max_stream_data_rx: u64,
    /// Times the node said this side's window blocked it (DATA_BLOCKED received).
    pub data_blocked_rx: u64,
    /// Datagrams, bytes and send or receive calls each way, as the connection counted them.
    pub datagrams_tx: u64,
    pub bytes_tx: u64,
    pub ios_tx: u64,
    pub datagrams_rx: u64,
    pub bytes_rx: u64,
    pub ios_rx: u64,
    /// ACK and STREAM frames each way: what acknowledging costs beside what it acknowledges.
    pub acks_tx: u64,
    pub acks_rx: u64,
    pub stream_tx: u64,
    pub stream_rx: u64,
    /// ACK_FREQUENCY frames each way (draft-ietf-quic-ack-frequency).
    pub ack_frequency_tx: u64,
    pub ack_frequency_rx: u64,
}

impl Path {
    pub fn of(stats: &hyper_quic::ConnectionStats) -> Self {
        let micros = |duration: Duration| duration.as_secs_f64() * 1e6;
        Self {
            rtt_us: micros(stats.path.rtt),
            min_rtt_us: micros(stats.path.min_rtt),
            cwnd: stats.path.cwnd,
            congestion_events: stats.path.congestion_events,
            lost_packets: stats.path.lost_packets,
            sent_packets: stats.path.sent_packets,
            data_blocked: stats.frame_tx.data_blocked,
            stream_data_blocked: stats.frame_tx.stream_data_blocked,
            max_data_rx: stats.frame_rx.max_data,
            max_stream_data_rx: stats.frame_rx.max_stream_data,
            data_blocked_rx: stats.frame_rx.data_blocked,
            datagrams_tx: stats.udp_tx.datagrams,
            bytes_tx: stats.udp_tx.bytes,
            ios_tx: stats.udp_tx.ios,
            datagrams_rx: stats.udp_rx.datagrams,
            bytes_rx: stats.udp_rx.bytes,
            ios_rx: stats.udp_rx.ios,
            acks_tx: stats.frame_tx.acks,
            acks_rx: stats.frame_rx.acks,
            stream_tx: stats.frame_tx.stream,
            stream_rx: stats.frame_rx.stream,
            ack_frequency_tx: stats.frame_tx.ack_frequency,
            ack_frequency_rx: stats.frame_rx.ack_frequency,
        }
    }
}

impl Lag {
    pub fn new() -> Result<Self, CompareError> {
        Ok(Self {
            histogram: Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
                .map_err(|error| CompareError::Histogram(error.to_string()))?,
            path: None,
        })
    }

    /// A message intended at `intended_ns` was published at `published_ns`, both since the start.
    pub fn record(&mut self, intended_ns: u64, published_ns: u64) -> Result<(), CompareError> {
        self.histogram
            .record(
                published_ns
                    .saturating_sub(intended_ns)
                    .clamp(1_000, HIGHEST_NS),
            )
            .map_err(|error| CompareError::Histogram(error.to_string()))
    }
}

/// One `pubsub` run's terms on a system whose clients run on tokio.
pub struct Run {
    pub system: System,
    pub endpoint: String,
    pub rate: u32,
    pub warmup: Duration,
    pub total: Duration,
    pub payloads: Vec<Vec<u8>>,
    pub inflight: usize,
}

/// Resolves at `due` to within a few microseconds: it sleeps until [`SPIN`] before it, then yields
/// round the runtime's other tasks (the client's own, the publishes in flight) until it passes.
pub async fn until(due: Instant) {
    if let Some(early) = due.checked_sub(SPIN)
        && Instant::now() < early
    {
        tokio::time::sleep_until(early.into()).await;
    }
    while Instant::now() < due {
        tokio::task::yield_now().await;
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, CompareError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| CompareError::Target(error.to_string()))
}

/// Runs the schedule and returns what the publisher sent, what the subscriber received, and how far
/// the publishes fell behind the schedule.
pub fn run(run: Run) -> Result<(Tally, Received, Lag), CompareError> {
    let (started, start) = oneshot::channel::<Instant>();
    let (close, closed) = oneshot::channel::<u64>();
    let (finish, finished) = oneshot::channel::<()>();
    let Run {
        system,
        endpoint,
        rate,
        warmup,
        total,
        payloads,
        inflight,
    } = run;
    let interval = Duration::from_secs(1)
        .checked_div(rate)
        .ok_or(CompareError::Argument("rate"))?;
    std::thread::scope(|scope| {
        let endpoint = endpoint.as_str();
        let subscribing = scope.spawn(move || {
            runtime()?.block_on(async move {
                let subscriber = Subscriber::connect(system, endpoint).await?;
                let start = Instant::now();
                started
                    .send(start)
                    .map_err(|_| CompareError::Target("the publisher stopped".into()))?;
                let received = subscriber.run(start, warmup, closed).await;
                let _ = finish.send(());
                received
            })
        });
        let payloads = &payloads;
        let publishing = scope.spawn(move || {
            runtime()?.block_on(async move {
                let publisher = Publisher::connect(system, endpoint).await?;
                let schedule = Schedule {
                    interval,
                    warmup,
                    total,
                    payloads,
                    inflight,
                };
                let published = publish(publisher, &schedule, start, close).await;
                // The subscriber takes what is still on its way, and says when it is done (or goes,
                // which says as much): its drain ends on its system's fact, not a clock.
                let _ = finished.await;
                published
            })
        });
        let received = subscribing
            .join()
            .map_err(|_| CompareError::Target("the subscriber's thread unwound".into()))?;
        let published = publishing
            .join()
            .map_err(|_| CompareError::Target("the publisher's thread unwound".into()))?;
        let (tally, lag) = published?;
        Ok((tally, received?, lag))
    })
}

/// What a publisher's schedule offers.
struct Schedule<'a> {
    interval: Duration,
    warmup: Duration,
    total: Duration,
    payloads: &'a [Vec<u8>],
    inflight: usize,
}

/// Publishes on the schedule once the subscription stands, each message stamped as it is handed to
/// the client; says how many it published once the schedule closed and every publish in flight
/// was answered.
async fn publish(
    publisher: Publisher,
    schedule: &Schedule<'_>,
    start: oneshot::Receiver<Instant>,
    close: oneshot::Sender<u64>,
) -> Result<(Tally, Lag), CompareError> {
    // The subscriber's setup is its client's to bound; one that fails drops its one-shot.
    let began = start
        .await
        .map_err(|_| CompareError::Target("the subscriber stopped before subscribing".into()))?;
    let mut tally = Tally::default();
    let mut lag = Lag::new()?;
    // Redis's publishes, each waiting for its reply; the others' clients take a message into
    // their own queues at once and are published inline.
    let mut flying: JoinSet<Result<(u64, u64), String>> = JoinSet::new();
    let mut next = Duration::ZERO;
    let mut numbered = 0_u64;
    let mut scratch: Vec<u8> = Vec::new();
    let answered = |joined: Result<Result<(u64, u64), String>, tokio::task::JoinError>,
                    tally: &mut Tally,
                    lag: &mut Lag|
     -> Result<(), CompareError> {
        match joined.map_err(|error| CompareError::Target(error.to_string()))? {
            Ok((intended_ns, published_ns)) => {
                tally.produced = tally.produced.saturating_add(1);
                if Duration::from_nanos(intended_ns) >= schedule.warmup {
                    lag.record(intended_ns, published_ns)?;
                }
            }
            Err(error) => {
                tally.failed = tally.failed.saturating_add(1);
                tally.first_error.get_or_insert(error);
            }
        }
        Ok(())
    };
    while next < schedule.total {
        while let Some(joined) = flying.try_join_next() {
            answered(joined, &mut tally, &mut lag)?;
        }
        // The schedule's time is over: what it still holds was never offered.
        if began.elapsed() > schedule.total {
            tally.behind = behind(schedule.total, next, schedule.interval);
            break;
        }
        let intended = next;
        next = next
            .checked_add(schedule.interval)
            .ok_or(CompareError::Argument("schedule"))?;
        until(began.checked_add(intended).unwrap_or(began)).await;
        let measured = intended >= schedule.warmup;
        let index = usize::try_from(numbered)
            .unwrap_or(0)
            .checked_rem(schedule.payloads.len())
            .unwrap_or(0);
        let corpus = schedule.payloads.get(index).map_or(&[][..], Vec::as_slice);
        let intended_ns = u64::try_from(intended.as_nanos()).unwrap_or(u64::MAX);
        numbered = numbered.saturating_add(1);
        match &publisher {
            // The client takes the message into its queue and its connection's task writes what
            // the queue holds when it next runs, between this loop's yields.
            Publisher::Nats(client) => {
                let mut payload = corpus.to_vec();
                let published_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
                deliver::stamp(&mut payload, published_ns, numbered)?;
                tally.sent = tally.sent.saturating_add(1);
                match client.publish(SUBJECT, payload.into()).await {
                    Ok(()) => {
                        tally.produced = tally.produced.saturating_add(1);
                        if measured {
                            lag.record(intended_ns, published_ns)?;
                        }
                    }
                    Err(error) => {
                        tally.failed = tally.failed.saturating_add(1);
                        tally.first_error.get_or_insert(error.to_string());
                    }
                }
            }
            // librdkafka copies the message into its queue, its own threads batch and send it; a
            // full queue holds the publisher, which offers the message again until it is taken
            // (librdkafka's `QueueFull` contract), as the bus's full lane holds its publisher. The
            // queue frees as messages are delivered or fail, each within `message.timeout.ms`.
            Publisher::Kafka(producer) => {
                scratch.clear();
                scratch.extend_from_slice(corpus);
                let published_ns = u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
                deliver::stamp(&mut scratch, published_ns, numbered)?;
                let mut record = rdkafka::producer::BaseRecord::<(), [u8]>::to(SUBJECT)
                    .payload(scratch.as_slice());
                let taken = loop {
                    match producer.send(record) {
                        Ok(()) => break Ok(()),
                        Err((
                            rdkafka::error::KafkaError::MessageProduction(
                                rdkafka::types::RDKafkaErrorCode::QueueFull,
                            ),
                            again,
                        )) => {
                            record = again;
                            tokio::task::yield_now().await;
                        }
                        Err((error, _)) => break Err(error),
                    }
                };
                match taken {
                    Ok(()) => {
                        tally.sent = tally.sent.saturating_add(1);
                        tally.produced = tally.produced.saturating_add(1);
                        if measured {
                            lag.record(intended_ns, published_ns)?;
                        }
                    }
                    Err(error) => {
                        tally.failed = tally.failed.saturating_add(1);
                        tally.first_error.get_or_insert(error.to_string());
                    }
                }
            }
            Publisher::Redis(connection) => {
                if flying.len() >= schedule.inflight {
                    if measured {
                        tally.refused = tally.refused.saturating_add(1);
                    }
                    continue;
                }
                tally.sent = tally.sent.saturating_add(1);
                let mut payload = corpus.to_vec();
                let mut connection = connection.clone();
                let sequence = numbered;
                flying.spawn(async move {
                    let published_ns =
                        u64::try_from(began.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    deliver::stamp(&mut payload, published_ns, sequence)
                        .map_err(|error| error.to_string())?;
                    redis::cmd("PUBLISH")
                        .arg(SUBJECT)
                        .arg(payload)
                        .query_async::<i64>(&mut connection)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok((intended_ns, published_ns))
                });
            }
        }
    }
    while let Some(joined) = flying.join_next().await {
        answered(joined, &mut tally, &mut lag)?;
    }
    // What the subscriber is to wait for: every message the system took, less those it said it
    // failed.
    let mut delivered = tally.produced;
    match &publisher {
        // What librdkafka still holds is delivered or failed before the schedule is said closed,
        // each within `message.timeout.ms`; a record Kafka acknowledged is never lost.
        Publisher::Kafka(producer) => {
            use rdkafka::producer::Producer as _;
            let _ = producer.flush(rdkafka::util::Timeout::Never);
            let (failed, first) = producer.context().failed();
            tally.failed = tally.failed.saturating_add(failed);
            delivered = delivered.saturating_sub(failed);
            if let Some(first) = first {
                tally.first_error.get_or_insert(first);
            }
        }
        // Every publish reached the server before the subscriber's flush asks it what it wrote.
        Publisher::Nats(client) => {
            client
                .flush()
                .await
                .map_err(|error| CompareError::Target(error.to_string()))?;
        }
        // Every publish was answered: the end of the run, an empty message, follows them all on
        // the subscriber's connection.
        Publisher::Redis(connection) => {
            let mut connection = connection.clone();
            redis::cmd("PUBLISH")
                .arg(SUBJECT)
                .arg(&[] as &[u8])
                .query_async::<i64>(&mut connection)
                .await
                .map_err(|error| CompareError::Target(error.to_string()))?;
        }
    }
    close.send(delivered).map_err(|_| {
        CompareError::Target("the subscriber stopped before the schedule closed".into())
    })?;
    Ok((tally, lag))
}

/// The messages a schedule of `interval` still holds at `next`, its time `total` over.
pub fn behind(total: Duration, next: Duration, interval: Duration) -> u64 {
    let left = total.saturating_sub(next).as_nanos();
    u64::try_from(left.checked_div(interval.as_nanos()).unwrap_or(0)).unwrap_or(u64::MAX)
}

/// What Kafka's producer reported it failed to deliver, from its delivery callback on its own
/// poll thread.
#[derive(Default)]
pub struct Deliveries {
    failed: std::sync::atomic::AtomicU64,
    first: std::sync::OnceLock<String>,
}

impl Deliveries {
    /// The deliveries failed, and the first one's reason.
    fn failed(&self) -> (u64, Option<String>) {
        (
            self.failed.load(std::sync::atomic::Ordering::Acquire),
            self.first.get().cloned(),
        )
    }
}

impl rdkafka::ClientContext for Deliveries {}

impl rdkafka::producer::ProducerContext for Deliveries {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &rdkafka::message::DeliveryResult<'_>, _: ()) {
        if let Err((error, _)) = result {
            self.failed
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            let _ = self.first.set(error.to_string());
        }
    }
}

/// A publisher of one system: its client.
pub enum Publisher {
    Nats(async_nats::Client),
    Redis(redis::aio::MultiplexedConnection),
    Kafka(rdkafka::producer::ThreadedProducer<Deliveries>),
}

impl Publisher {
    /// Connects a publisher of `system`.
    pub async fn connect(system: System, endpoint: &str) -> Result<Self, CompareError> {
        let fail = |error: String| CompareError::Target(error);
        match system {
            System::Nats => async_nats::connect(format!("nats://{endpoint}").as_str())
                .await
                .map(Self::Nats)
                .map_err(|error| fail(error.to_string())),
            System::Redis => redis::Client::open(format!("redis://{endpoint}"))
                .map_err(|error| fail(error.to_string()))?
                .get_multiplexed_async_connection()
                .await
                .map(Self::Redis)
                .map_err(|error| fail(error.to_string())),
            System::Kafka => rdkafka::ClientConfig::new()
                .set("bootstrap.servers", endpoint)
                // A latency-minded producer sends at once (Kafka 4's default lingers 5 ms).
                .set("linger.ms", "0")
                .set("message.timeout.ms", (HIGHEST_NS / 1_000_000).to_string())
                .create_with_context(Deliveries::default())
                .map(Self::Kafka)
                .map_err(|error| fail(error.to_string())),
            System::FocalBus => Err(CompareError::Argument(
                "focal-bus's clients run on their own threads (bus.rs)",
            )),
        }
    }
}

/// A subscriber of one system.
pub enum Subscriber {
    Nats {
        _client: async_nats::Client,
        subscription: async_nats::Subscriber,
    },
    Redis(redis::aio::PubSub),
    Kafka(Box<rdkafka::consumer::StreamConsumer>),
}

impl Subscriber {
    /// Connects a subscriber of `system`, subscribed before this returns, so it takes the first
    /// publish. Kafka's topic is made here, before anything is published to it.
    pub async fn connect(system: System, endpoint: &str) -> Result<Self, CompareError> {
        let fail = |error: String| CompareError::Target(error);
        match system {
            System::Nats => {
                let listener = async_nats::connect(format!("nats://{endpoint}").as_str())
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                let subscription = listener
                    .subscribe(SUBJECT)
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                // The subscription reaches the server before the first publish is sent.
                listener
                    .flush()
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Nats {
                    _client: listener,
                    subscription,
                })
            }
            System::Redis => {
                let client = redis::Client::open(format!("redis://{endpoint}"))
                    .map_err(|error| fail(error.to_string()))?;
                let mut pubsub = client
                    .get_async_pubsub()
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                pubsub
                    .subscribe(SUBJECT)
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Redis(pubsub))
            }
            System::Kafka => {
                use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
                use rdkafka::client::DefaultClientContext;
                use rdkafka::consumer::Consumer;
                let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
                    .set("bootstrap.servers", endpoint)
                    .create()
                    .map_err(|error| fail(error.to_string()))?;
                let topic = NewTopic::new(SUBJECT, 1, TopicReplication::Fixed(1));
                // A topic that exists already is the topic to use; the request is bounded by
                // librdkafka's own timeouts.
                let _ = admin.create_topics([&topic], &AdminOptions::new()).await;
                let consumer: rdkafka::consumer::StreamConsumer = rdkafka::ClientConfig::new()
                    .set("bootstrap.servers", endpoint)
                    .set("group.id", "focal-compare-pubsub")
                    .set("enable.auto.commit", "false")
                    // The topic is the run's own: from its start, nothing published is missed.
                    .set("auto.offset.reset", "earliest")
                    .set("fetch.min.bytes", "1")
                    .create()
                    .map_err(|error| fail(error.to_string()))?;
                consumer
                    .subscribe(&[SUBJECT])
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Kafka(Box::new(consumer)))
            }
            System::FocalBus => Err(CompareError::Argument(
                "focal-bus's clients run on their own threads (bus.rs)",
            )),
        }
    }

    /// Takes messages until the system's fact says the run's are all in (the module's header).
    pub async fn run(
        self,
        start: Instant,
        warmup: Duration,
        closed: oneshot::Receiver<u64>,
    ) -> Result<Received, CompareError> {
        let mut received = Received::new()?;
        let mut last = 0_u64;
        let mut closed = closed;
        let stopped =
            || CompareError::Target("the publisher stopped before the schedule closed".into());
        match self {
            Self::Nats {
                _client: client,
                mut subscription,
            } => {
                let published = loop {
                    tokio::select! {
                        message = subscription.next() => match message {
                            Some(message) => received.record(
                                start,
                                warmup,
                                &message.payload,
                                Instant::now(),
                                &mut last,
                            )?,
                            None => return Err(CompareError::Target("the subscription ended".into())),
                        },
                        published = &mut closed => break published.map_err(|_| stopped())?,
                    }
                };
                let closed_at = Instant::now();
                // The server answers the flush after everything it wrote to this connection before
                // it: what has not arrived by then, it dropped.
                if received.received < published {
                    client
                        .flush()
                        .await
                        .map_err(|error| CompareError::Target(error.to_string()))?;
                    while let Some(Some(message)) = subscription.next().now_or_never() {
                        received.record(
                            start,
                            warmup,
                            &message.payload,
                            Instant::now(),
                            &mut last,
                        )?;
                    }
                }
                received.finish(closed_at);
                Ok(received)
            }
            Self::Kafka(consumer) => {
                use rdkafka::Message;
                let mut close: Option<(Instant, u64)> = None;
                loop {
                    if let Some((closed_at, published)) = close
                        && received.received >= published
                    {
                        received.finish(closed_at);
                        return Ok(received);
                    }
                    tokio::select! {
                        message = consumer.recv() => {
                            let message = message.map_err(|error| CompareError::Target(error.to_string()))?;
                            received.record(
                                start,
                                warmup,
                                message.payload().unwrap_or(&[]),
                                Instant::now(),
                                &mut last,
                            )?;
                        }
                        published = &mut closed, if close.is_none() => {
                            close = Some((Instant::now(), published.map_err(|_| stopped())?));
                        }
                    }
                }
            }
            Self::Redis(pubsub) => {
                let mut messages = pubsub.into_on_message();
                let mut closed_at: Option<Instant> = None;
                loop {
                    tokio::select! {
                        message = messages.next() => {
                            let Some(message) = message else {
                                // Redis disconnected the subscriber (its output buffer's limit):
                                // nothing more comes.
                                received.finish(closed_at.unwrap_or_else(Instant::now));
                                return Ok(received);
                            };
                            let payload = message.get_payload_bytes();
                            // The run's end, which every message published arrived before.
                            if payload.is_empty() {
                                let at = match closed_at {
                                    Some(at) => at,
                                    None => {
                                        (&mut closed).await.map_err(|_| stopped())?;
                                        Instant::now()
                                    }
                                };
                                received.finish(at);
                                return Ok(received);
                            }
                            received.record(start, warmup, payload, Instant::now(), &mut last)?;
                        }
                        published = &mut closed, if closed_at.is_none() => {
                            published.map_err(|_| stopped())?;
                            closed_at = Some(Instant::now());
                        }
                    }
                }
            }
        }
    }
}
