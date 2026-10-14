//! The `deliver` mode's consumers: each system's own way to take a message and acknowledge it
//! durably, as an application working a queue does, measured from the message's intended send
//! time to its acknowledgement confirmed. Kafka: a consumer group, each poll's records committed
//! synchronously (the commit replicated as the records are). NATS JetStream: a durable pull
//! consumer with explicit acknowledgement, each message acknowledged with the server's
//! confirmation (`double_ack`). Redis Streams: a consumer group read with `XREADGROUP`, each
//! read's entries acknowledged with `XACK`, made durable locally and at both replicas
//! (`WAITAOF`) when the run is durable. Every record carries its intended send time and sequence
//! in its first [`HEADER`] bytes; the clock is the generator's own, so producer and consumer
//! share it.
use crate::target::NAME;
use crate::{CompareError, HIGHEST_NS, System};
use hdrhistogram::Histogram;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// The bytes at the head of every deliver-mode record: its intended send time after the run's
/// start in nanoseconds, then its sequence, each big-endian.
pub const HEADER: usize = 16;
/// The records one acknowledgement covers at most: what one poll or read takes.
const BATCH: usize = 1024;
/// Acknowledgements under way at once (NATS acknowledges a message at a time).
const ACKS_IN_FLIGHT: usize = 1024;
/// How long a consumer waits for a record before it looks whether the run ended.
const IDLE: Duration = Duration::from_millis(50);
/// How long a consumer may take, once the schedule closed, to acknowledge what was produced.
const DRAIN: Duration = Duration::from_secs(120);
/// The consumer group's (or durable consumer's) name.
const GROUP: &str = "focal-compare";

/// Writes a record's stamp at the head of `payload`.
pub fn stamp(payload: &mut [u8], intended_ns: u64, sequence: u64) -> Result<(), CompareError> {
    let (time, rest) = payload
        .split_first_chunk_mut::<8>()
        .ok_or(CompareError::Argument(
            "deliver needs a payload of 16 bytes or more",
        ))?;
    let (order, _) = rest
        .split_first_chunk_mut::<8>()
        .ok_or(CompareError::Argument(
            "deliver needs a payload of 16 bytes or more",
        ))?;
    *time = intended_ns.to_be_bytes();
    *order = sequence.to_be_bytes();
    Ok(())
}

/// A record's intended send time after the run's start, from its stamp.
fn intended(bytes: &[u8]) -> Option<u64> {
    bytes
        .first_chunk::<8>()
        .map(|time| u64::from_be_bytes(*time))
}

/// What a consumer measured.
pub struct Delivered {
    /// Records acknowledged whose intended send time falls after the warm-up, from that time to
    /// the acknowledgement confirmed.
    pub histogram: Histogram<u64>,
    /// Records acknowledged, the warm-up's included.
    pub acknowledged: u64,
    /// Acknowledgements refused or failed, and records whose stamp could not be read.
    pub failed: u64,
    pub first_error: Option<String>,
}

impl Delivered {
    fn new() -> Result<Self, CompareError> {
        Ok(Self {
            histogram: Histogram::<u64>::new_with_bounds(1_000, HIGHEST_NS, 3)
                .map_err(|error| CompareError::Histogram(error.to_string()))?,
            acknowledged: 0,
            failed: 0,
            first_error: None,
        })
    }
    /// One record acknowledged at `acked`, intended `intended_ns` after `start`.
    fn record(
        &mut self,
        start: Instant,
        warmup: Duration,
        intended_ns: u64,
        acked: Instant,
    ) -> Result<(), CompareError> {
        self.acknowledged = self.acknowledged.saturating_add(1);
        let intended = Duration::from_nanos(intended_ns);
        if intended < warmup {
            return Ok(());
        }
        let latency = acked
            .saturating_duration_since(start)
            .saturating_sub(intended);
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        self.histogram
            .record(nanos.clamp(1_000, HIGHEST_NS))
            .map_err(|error| CompareError::Histogram(error.to_string()))
    }
    fn fail(&mut self, error: String) {
        self.failed = self.failed.saturating_add(1);
        self.first_error.get_or_insert(error);
    }
}

/// When a consumer stops: the records the producers had acknowledged once the schedule closed
/// (`None` while it runs), all of which it acknowledges, or the drain's bound runs out.
pub struct Stop {
    produced: watch::Receiver<Option<u64>>,
    closed_at: Option<Instant>,
}

impl Stop {
    pub fn new(produced: watch::Receiver<Option<u64>>) -> Self {
        Self {
            produced,
            closed_at: None,
        }
    }
    fn reached(&mut self, delivered: &Delivered) -> bool {
        let Some(total) = *self.produced.borrow() else {
            return false;
        };
        let closed = *self.closed_at.get_or_insert_with(Instant::now);
        delivered.acknowledged >= total || closed.elapsed() >= DRAIN
    }
}

/// One system's consumer, connected and positioned before the producers start.
pub enum Consumer {
    Kafka(Box<rdkafka::consumer::StreamConsumer>),
    Nats(Box<async_nats::jetstream::consumer::PullConsumer>),
    Redis {
        connection: redis::aio::MultiplexedConnection,
        durable: bool,
    },
}

impl Consumer {
    pub async fn connect(
        system: System,
        durable: bool,
        endpoints: &[String],
        target: &crate::target::Target,
    ) -> Result<Self, CompareError> {
        let fail = |error: String| CompareError::Target(error);
        match (system, target) {
            (System::Kafka, _) => {
                use rdkafka::consumer::Consumer as _;
                let consumer: rdkafka::consumer::StreamConsumer = rdkafka::ClientConfig::new()
                    .set("bootstrap.servers", endpoints.join(","))
                    .set("group.id", GROUP)
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", "earliest")
                    // A fetch is answered as soon as a record is there (`fetch.min.bytes` 1),
                    // and waits for one at most this long.
                    .set("fetch.wait.max.ms", "10")
                    .create()
                    .map_err(|error| fail(error.to_string()))?;
                consumer
                    .subscribe(&[NAME])
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Kafka(Box::new(consumer)))
            }
            (System::Nats, crate::target::Target::Nats(context)) => {
                use async_nats::jetstream::consumer::{AckPolicy, pull};
                let stream = context
                    .get_stream(NAME)
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                let consumer = stream
                    .get_or_create_consumer(
                        GROUP,
                        pull::Config {
                            durable_name: Some(GROUP.into()),
                            ack_policy: AckPolicy::Explicit,
                            max_ack_pending: i64::try_from(ACKS_IN_FLIGHT)
                                .map_err(|_| CompareError::Argument("acks in flight"))?,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Nats(Box::new(consumer)))
            }
            (System::Redis, crate::target::Target::Redis { connections, .. }) => {
                let mut connection = connections
                    .first()
                    .cloned()
                    .ok_or(CompareError::Argument("no endpoint"))?;
                // The group reads the stream from its start; a group that exists already is
                // the group to read with.
                let created: Result<(), redis::RedisError> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(NAME)
                    .arg(GROUP)
                    .arg("0")
                    .arg("MKSTREAM")
                    .query_async(&mut connection)
                    .await;
                if let Err(error) = created
                    && error.code() != Some("BUSYGROUP")
                {
                    return Err(fail(error.to_string()));
                }
                Ok(Self::Redis {
                    connection,
                    durable,
                })
            }
            _ => Err(CompareError::Argument(
                "deliver: the target is not the system's",
            )),
        }
    }

    /// Takes and acknowledges records until `stop` says the run ended and every record produced
    /// was acknowledged, or its drain ran out.
    pub async fn run(
        self,
        start: Instant,
        warmup: Duration,
        mut stop: Stop,
    ) -> Result<Delivered, CompareError> {
        let mut delivered = Delivered::new()?;
        match self {
            Self::Kafka(consumer) => {
                kafka(&consumer, start, warmup, &mut stop, &mut delivered).await?;
            }
            Self::Nats(consumer) => {
                nats(&consumer, start, warmup, &mut stop, &mut delivered).await?;
            }
            Self::Redis {
                mut connection,
                durable,
            } => {
                redis_streams(
                    &mut connection,
                    durable,
                    start,
                    warmup,
                    &mut stop,
                    &mut delivered,
                )
                .await?;
            }
        }
        Ok(delivered)
    }
}

/// Each poll's records (what is there now, up to [`BATCH`]) committed synchronously, the commit
/// confirmed as their acknowledgement.
async fn kafka(
    consumer: &rdkafka::consumer::StreamConsumer,
    start: Instant,
    warmup: Duration,
    stop: &mut Stop,
    delivered: &mut Delivered,
) -> Result<(), CompareError> {
    use rdkafka::Message as _;
    use rdkafka::consumer::{CommitMode, Consumer as _};
    let mut batch: Vec<u64> = Vec::new();
    batch
        .try_reserve_exact(BATCH)
        .map_err(|_| CompareError::Argument("deliver batch"))?;
    while !stop.reached(delivered) {
        batch.clear();
        let mut last = None;
        let mut wait = IDLE;
        while batch.len() < BATCH {
            let message = match tokio::time::timeout(wait, consumer.recv()).await {
                Ok(Ok(message)) => message,
                Ok(Err(error)) => {
                    delivered.fail(error.to_string());
                    break;
                }
                Err(_) => break,
            };
            // Once a record came, the rest of this poll is what is there already.
            wait = Duration::ZERO;
            last = Some((message.partition(), message.offset()));
            match message.payload().and_then(intended) {
                Some(intended) => batch.push(intended),
                None => delivered.fail("a record without its stamp".into()),
            }
        }
        let Some((partition, offset)) = last else {
            continue;
        };
        let mut list = rdkafka::TopicPartitionList::new();
        list.add_partition_offset(
            NAME,
            partition,
            rdkafka::Offset::Offset(offset.saturating_add(1)),
        )
        .map_err(|error| CompareError::Target(error.to_string()))?;
        // The commit blocks its thread until the coordinator answers: off the runtime's workers.
        let committed = tokio::task::block_in_place(|| consumer.commit(&list, CommitMode::Sync));
        let acked = Instant::now();
        match committed {
            Ok(()) => {
                for intended in &batch {
                    delivered.record(start, warmup, *intended, acked)?;
                }
            }
            Err(error) => delivered.fail(error.to_string()),
        }
    }
    Ok(())
}

/// Each message acknowledged with the server's confirmation, up to [`ACKS_IN_FLIGHT`] at once.
async fn nats(
    consumer: &async_nats::jetstream::consumer::PullConsumer,
    start: Instant,
    warmup: Duration,
    stop: &mut Stop,
    delivered: &mut Delivered,
) -> Result<(), CompareError> {
    use futures_util::StreamExt as _;
    let mut messages = consumer
        .messages()
        .await
        .map_err(|error| CompareError::Target(error.to_string()))?;
    let mut acking = tokio::task::JoinSet::new();
    loop {
        if acking.is_empty() && stop.reached(delivered) {
            return Ok(());
        }
        tokio::select! {
            next = tokio::time::timeout(IDLE, messages.next()), if acking.len() < ACKS_IN_FLIGHT => {
                match next {
                    Ok(Some(Ok(message))) => match intended(&message.payload) {
                        Some(intended) => {
                            acking.spawn(async move {
                                let acked = message.double_ack().await;
                                (intended, Instant::now(), acked.map_err(|error| error.to_string()))
                            });
                        }
                        None => delivered.fail("a message without its stamp".into()),
                    },
                    Ok(Some(Err(error))) => delivered.fail(error.to_string()),
                    // The stream of messages ended, or nothing came within the wait: look again.
                    Ok(None) | Err(_) => {}
                }
            }
            Some(joined) = acking.join_next(), if !acking.is_empty() => {
                match joined {
                    Ok((intended, acked, Ok(()))) => delivered.record(start, warmup, intended, acked)?,
                    Ok((_, _, Err(error))) => delivered.fail(error),
                    Err(error) => delivered.fail(error.to_string()),
                }
            }
        }
    }
}

/// Each read's entries (up to [`BATCH`]) acknowledged with `XACK`, made durable locally and at
/// both replicas before the acknowledgement counts when the run is durable.
async fn redis_streams(
    connection: &mut redis::aio::MultiplexedConnection,
    durable: bool,
    start: Instant,
    warmup: Duration,
    stop: &mut Stop,
    delivered: &mut Delivered,
) -> Result<(), CompareError> {
    let block = u64::try_from(IDLE.as_millis()).unwrap_or(50);
    let mut ids: Vec<String> = Vec::new();
    let mut stamps: Vec<u64> = Vec::new();
    ids.try_reserve_exact(BATCH)
        .map_err(|_| CompareError::Argument("deliver batch"))?;
    stamps
        .try_reserve_exact(BATCH)
        .map_err(|_| CompareError::Argument("deliver batch"))?;
    while !stop.reached(delivered) {
        let reply: Result<redis::Value, redis::RedisError> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg("generator")
            .arg("COUNT")
            .arg(BATCH)
            .arg("BLOCK")
            .arg(block)
            .arg("STREAMS")
            .arg(NAME)
            .arg(">")
            .query_async(connection)
            .await;
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) => {
                delivered.fail(error.to_string());
                continue;
            }
        };
        ids.clear();
        stamps.clear();
        entries(&reply, &mut ids, &mut stamps, delivered);
        if ids.is_empty() {
            continue;
        }
        let mut pipe = redis::pipe();
        pipe.cmd("XACK")
            .arg(NAME)
            .arg(GROUP)
            .arg(ids.as_slice())
            .ignore();
        let acknowledged: Result<(), String> = if durable {
            pipe.cmd("WAITAOF").arg(1).arg(2).arg(0);
            match pipe.query_async::<((u64, u64),)>(connection).await {
                Ok(((local, replicas),)) if local >= 1 && replicas >= 2 => Ok(()),
                Ok(((local, replicas),)) => Err(format!(
                    "WAITAOF answered {local} local, {replicas} replicas"
                )),
                Err(error) => Err(error.to_string()),
            }
        } else {
            pipe.query_async::<()>(connection)
                .await
                .map_err(|error| error.to_string())
        };
        let acked = Instant::now();
        match acknowledged {
            Ok(()) => {
                for intended in &stamps {
                    delivered.record(start, warmup, *intended, acked)?;
                }
            }
            Err(error) => delivered.fail(error),
        }
    }
    Ok(())
}

/// The entries of one `XREADGROUP` reply: `[[stream, [[id, [field, value, ...]], ...]]]`, or nil
/// when nothing came within the block. Each entry's id and the stamp of its `d` field.
fn entries(
    reply: &redis::Value,
    ids: &mut Vec<String>,
    stamps: &mut Vec<u64>,
    delivered: &mut Delivered,
) {
    let redis::Value::Array(streams) = reply else {
        return;
    };
    for stream in streams {
        let redis::Value::Array(parts) = stream else {
            continue;
        };
        let Some(redis::Value::Array(records)) = parts.get(1) else {
            continue;
        };
        for record in records {
            let redis::Value::Array(fields) = record else {
                continue;
            };
            let (Some(redis::Value::BulkString(id)), Some(redis::Value::Array(values))) =
                (fields.first(), fields.get(1))
            else {
                delivered.fail("an entry not of the stream's shape".into());
                continue;
            };
            let stamp = values.get(1).and_then(|value| match value {
                redis::Value::BulkString(bytes) => intended(bytes),
                _ => None,
            });
            match (String::from_utf8(id.clone()), stamp) {
                (Ok(id), Some(stamp)) if ids.len() < BATCH => {
                    ids.push(id);
                    stamps.push(stamp);
                }
                _ => delivered.fail("an entry without its id or stamp".into()),
            }
        }
    }
}
