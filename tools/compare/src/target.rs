//! One durable replicated append per system, at the durability the plan
//! names: the system as shipped, or (`durable`) acknowledged only once on disk
//! at a quorum.
use crate::{CompareError, System};
use std::time::Duration;

/// The stream, topic or key space every run writes.
const NAME: &str = "focal-compare";
/// How long setup (creating the stream or topic) may take.
const SETUP: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub enum Target {
    Nats(async_nats::jetstream::Context),
    Kafka(rdkafka::producer::FutureProducer),
    Redis {
        /// One connection a lane: WAITAOF blocks the connection it is sent
        /// on until the replicas answer, so a durable client spreads its
        /// writes over connections, as applications at volume do.
        connections: Vec<redis::aio::MultiplexedConnection>,
        durable: bool,
    },
    /// One request's share of a Redis pool: the connection its sequence
    /// picks.
    RedisLane {
        connection: redis::aio::MultiplexedConnection,
        durable: bool,
    },
}

impl Target {
    /// `lanes`: the connections a Redis run spreads its writes over, a
    /// parameter of the experiment that every report records.
    pub async fn connect(
        system: System,
        durable: bool,
        endpoints: &[String],
        lanes: usize,
    ) -> Result<Self, CompareError> {
        let fail = |error: String| CompareError::Target(error);
        match system {
            System::Nats => {
                // Durability is the servers' `sync_interval` (always, or the
                // shipped 2 min); the client awaits the stream's ack.
                let servers = endpoints
                    .iter()
                    .map(|endpoint| format!("nats://{endpoint}").parse::<async_nats::ServerAddr>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| fail(error.to_string()))?;
                let client = async_nats::connect(servers)
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                // The client's own ack deadline is the histogram's highest
                // value: a slow ack is a latency to record, not a failure the
                // client's default deadline would turn it into.
                let context = async_nats::jetstream::context::ContextBuilder::new()
                    .timeout(Duration::from_nanos(crate::HIGHEST_NS))
                    .build(client);
                let config = async_nats::jetstream::stream::Config {
                    name: NAME.into(),
                    subjects: vec![NAME.into()],
                    num_replicas: 3,
                    storage: async_nats::jetstream::stream::StorageType::File,
                    ..Default::default()
                };
                tokio::time::timeout(SETUP, context.get_or_create_stream(config))
                    .await
                    .map_err(|_| fail("stream setup timed out".into()))?
                    .map_err(|error| fail(error.to_string()))?;
                Ok(Self::Nats(context))
            }
            System::Kafka => {
                use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
                use rdkafka::client::DefaultClientContext;
                let servers = endpoints.join(",");
                let mut config = rdkafka::ClientConfig::new();
                config
                    .set("bootstrap.servers", &servers)
                    // Every in-sync replica before the answer, and no
                    // duplicate from a retry (Kafka's own safe-producer
                    // defaults since 3.0); the brokers'
                    // `log.flush.interval.messages` decides the fsync.
                    .set("acks", "all")
                    .set("enable.idempotence", "true")
                    // The client's own deadline is the histogram's highest
                    // value, so a slow acknowledgement is recorded.
                    .set(
                        "message.timeout.ms",
                        crate::HIGHEST_NS
                            .checked_div(1_000_000)
                            .unwrap_or(0)
                            .to_string(),
                    );
                let admin: AdminClient<DefaultClientContext> =
                    config.create().map_err(|error| fail(error.to_string()))?;
                let topic = NewTopic::new(NAME, 1, TopicReplication::Fixed(3))
                    .set("min.insync.replicas", "2");
                // A topic that exists already is the topic to use.
                let _ = tokio::time::timeout(
                    SETUP,
                    admin.create_topics([&topic], &AdminOptions::new()),
                )
                .await;
                let producer: rdkafka::producer::FutureProducer =
                    config.create().map_err(|error| fail(error.to_string()))?;
                Ok(Self::Kafka(producer))
            }
            System::Redis => {
                let primary = endpoints
                    .first()
                    .ok_or(CompareError::Argument("no endpoint"))?;
                let client = redis::Client::open(format!("redis://{primary}"))
                    .map_err(|error| fail(error.to_string()))?;
                // As with NATS, the client's own deadline is the histogram's
                // highest value, so a slow WAITAOF is recorded, not failed.
                let config = redis::AsyncConnectionConfig::new()
                    .set_response_timeout(Some(Duration::from_nanos(crate::HIGHEST_NS)));
                let connection = client
                    .get_multiplexed_async_connection_with_config(&config)
                    .await
                    .map_err(|error| fail(error.to_string()))?;
                // Measure only a replicated primary: WAITAOF on a primary whose
                // replicas have not finished their sync waits on them, not on
                // its writes (the plan's three nodes, both replicas connected).
                let mut connection = connection;
                let deadline = tokio::time::Instant::now()
                    .checked_add(SETUP)
                    .ok_or(CompareError::Argument("setup deadline"))?;
                loop {
                    let info: String = redis::cmd("INFO")
                        .arg("replication")
                        .query_async(&mut connection)
                        .await
                        .map_err(|error| fail(error.to_string()))?;
                    if info.lines().any(|line| line.trim() == "connected_slaves:2")
                        && info.matches("state=online").count() == 2
                    {
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(fail(format!(
                            "the primary's replicas did not come online within {SETUP:?}"
                        )));
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                let mut connections = Vec::new();
                connections
                    .try_reserve_exact(lanes)
                    .map_err(|error| fail(error.to_string()))?;
                connections.push(connection);
                while connections.len() < lanes {
                    connections.push(
                        client
                            .get_multiplexed_async_connection_with_config(&config)
                            .await
                            .map_err(|error| fail(error.to_string()))?,
                    );
                }
                Ok(Self::Redis {
                    connections,
                    durable,
                })
            }
        }
    }

    /// What one request takes with it: the whole target, or for Redis the
    /// one connection its sequence picks.
    pub fn lane(&self, sequence: u64) -> Self {
        match self {
            Self::Redis {
                connections,
                durable,
            } => {
                let index = usize::try_from(sequence).unwrap_or(0) % connections.len().max(1);
                match connections.get(index) {
                    Some(connection) => Self::RedisLane {
                        connection: connection.clone(),
                        durable: *durable,
                    },
                    None => self.clone(),
                }
            }
            other => other.clone(),
        }
    }

    /// One record of `payload`, acknowledged as the target's durability says.
    pub async fn append(self, sequence: u64, payload: Vec<u8>) -> Result<(), String> {
        match self {
            Self::Redis { .. } => Err("a request takes its lane, never the pool".into()),
            Self::Nats(context) => context
                .publish(NAME, payload.into())
                .await
                .map_err(|error| error.to_string())?
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Self::Kafka(producer) => {
                let key = sequence.to_be_bytes();
                let record = rdkafka::producer::FutureRecord::to(NAME)
                    .key(&key[..])
                    .payload(&payload[..]);
                producer
                    .send(record, rdkafka::util::Timeout::Never)
                    .await
                    .map(|_| ())
                    .map_err(|(error, _)| error.to_string())
            }
            Self::RedisLane {
                mut connection,
                durable,
            } => {
                let key = format!("{NAME}:{sequence}");
                if durable {
                    // On disk locally and at both replicas before the answer
                    // (`appendfsync always` on every node; Redis 7.2's WAITAOF).
                    // A pipeline answers one value a command not ignored:
                    // WAITAOF's pair, inside a one-element array.
                    let ((local, replicas),): ((u64, u64),) = redis::pipe()
                        .cmd("SET")
                        .arg(&key)
                        .arg(payload)
                        .ignore()
                        .cmd("WAITAOF")
                        .arg(1)
                        .arg(2)
                        .arg(0)
                        .query_async(&mut connection)
                        .await
                        .map_err(|error| error.to_string())?;
                    if local < 1 || replicas < 2 {
                        return Err(format!(
                            "WAITAOF answered {local} local, {replicas} replicas"
                        ));
                    }
                    Ok(())
                } else {
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(payload)
                        .query_async::<()>(&mut connection)
                        .await
                        .map_err(|error| error.to_string())
                }
            }
        }
    }
}
